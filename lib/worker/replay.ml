(* Copyright 2026 Kevin Hoffman

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. *)

(* The workflow replay engine: a deterministic cooperative scheduler over
   Effect.Deep that drives a workflow body from its recorded history
   ([Replay_state]). We re-run the body from the top on each activation. The body
   parks on each operation it awaits (activity, timer, child); a scheduler then walks
   the history log in order, waking the parked fiber as each resolution arrives and
   delivering signals and updates at their history positions. Operations still parked
   after the log is drained emit their schedule command, and the activation ends.

   With a single fiber this reproduces the older single-cursor walk exactly; the ready
   queue and per-seq waiter tables are what generalize it to many fibers. This is the
   deterministic replay model, minus persisting continuations across polls. *)

open Replay_state

(* how an outstanding operation reacts when its scope is cancelled: the cancel command
   to send, whether to send it (Abandon does not), and whether to raise Canceled in the
   awaiter at once. When [raise_on_cancel] is false (Wait_cancellation_completed
   activity, or a child with wait_for_cancellation), the awaiter stays parked for the
   server's resolution instead. *)
type cancel_spec = {
  cmd : Coresdk.wf_command;
  emit_on_cancel : bool;
  raise_on_cancel : bool;
}

(* [queries] are the (query_id, query_type, arguments) of any QueryWorkflow jobs, and
   [updates] the (protocol_instance_id, run_validator) of any DoUpdate jobs, on this
   activation (whose Update events apply_job already appended to the log, in job
   order). When [query_mode] is set (a pure-query activation), the body is replayed
   read-only: it rebuilds state from history but suppresses every workflow-advancing
   command. Queries are answered, and updates validated/run and responded to, from the
   handlers the body registers. [task_queue] is the worker's default queue, used for
   scheduled activities and as the fallback for child workflows that don't name one. *)
let run_workflow (wf : Workflow.reg) (state : run_state) ~task_queue:default_tq
    ~run_id ~can_suggested ~history_length ~query_mode ~queries ~updates :
    Coresdk.wf_command list =
  let commands = ref [] in
  (* a query replay is read-only: it rebuilds state from history but must emit no
     workflow-advancing command. [emit] centralizes that guard for the incremental
     commands; the terminal ones (complete/fail/continue-as-new) guard inline. *)
  let emit cmd = if not query_mode then commands := cmd :: !commands in
  let act_seq = ref 0 and timer_seq = ref 0 and child_seq = ref 0 in
  (* Signals: handlers the body registers (rebuilt each re-run), plus a per-name
     buffer for signals the log walk passed before a handler existed. Matches Temporal,
     which buffers until a handler registers; normally handlers register at the top of
     the body, before any checkpoint, so nothing is ever buffered. *)
  let module Signals = struct
    let handlers : (string, Codec.payload -> unit) Hashtbl.t = Hashtbl.create 8
    let buffered : (string, Codec.payload Queue.t) Hashtbl.t = Hashtbl.create 4

    let deliver name payload =
      match Hashtbl.find_opt handlers name with
      | Some h -> h payload
      | None ->
        let q =
          match Hashtbl.find_opt buffered name with
          | Some q -> q
          | None ->
            let q = Queue.create () in
            Hashtbl.replace buffered name q;
            q
        in
        Queue.add payload q

    (* on registration, drain any signals buffered for this name, in arrival order *)
    let register name handler =
      Hashtbl.replace handlers name handler;
      match Hashtbl.find_opt buffered name with
      | Some q ->
        Queue.iter handler q;
        Hashtbl.remove buffered name
      | None -> ()
  end in
  (* Queries: handlers the body registers (rebuilt each re-run), answered after the
     replay reaches its frontier. A missing or raising handler fails just the query,
     not the workflow. *)
  let module Queries = struct
    let handlers : (string, Codec.payload -> Codec.payload) Hashtbl.t =
      Hashtbl.create 8

    let register name handler = Hashtbl.replace handlers name handler

    let responses () =
      List.map
        (fun (query_id, query_type, arguments) ->
          let result =
            match Hashtbl.find_opt handlers query_type with
            | None ->
              Coresdk.Query_failed
                (Printf.sprintf "no query handler registered for %S" query_type)
            | Some h -> (
              let arg =
                match arguments with
                | p :: _ -> p
                | [] -> Codec.to_payload Codec.unit ()
              in
              try Coresdk.Query_succeeded (h arg)
              with e -> Coresdk.Query_failed (Printexc.to_string e))
          in
          Coresdk.Respond_to_query { query_id; result })
        queries
  end in
  (* Updates: handlers the body registers (name -> validator?, handler) and one
     recorded [outcome] per delivered update. Delivering runs the validator (first
     delivery only, i.e. pid in [validate_pids]) then the handler; the outcome then
     drives the UpdateResponse(s) emitted after the replay. *)
  let module Updates = struct
    type outcome =
      | Rejected of string (* validator rejected, or no handler registered *)
      | Accepted_completed of Codec.payload
      | Accepted_failed of string (* accepted, then the handler raised *)

    let handlers :
        (string, (Codec.payload -> unit) option * (Codec.payload -> Codec.payload))
        Hashtbl.t =
      Hashtbl.create 4

    let validate_pids : (string, unit) Hashtbl.t = Hashtbl.create 4

    let () =
      List.iter
        (fun (pid, run_validator) ->
          if run_validator then Hashtbl.replace validate_pids pid ())
        updates

    let outcomes : (string, outcome) Hashtbl.t = Hashtbl.create 4

    let register name validator handler =
      Hashtbl.replace handlers name (validator, handler)

    let deliver pid name input =
      match Hashtbl.find_opt handlers name with
      | None ->
        Hashtbl.replace outcomes pid
          (Rejected (Printf.sprintf "no update handler registered for %S" name))
      | Some (validator, handler) ->
        (* validate only on first delivery; a raising validator rejects and the
           handler never runs *)
        let rejection =
          if Hashtbl.mem validate_pids pid then
            match validator with
            | Some v -> ( try v input; None with e -> Some (Printexc.to_string e))
            | None -> None
          else None
        in
        let outcome =
          match rejection with
          | Some msg -> Rejected msg
          | None -> (
            (* a handler that raises after acceptance fails the update, not the run *)
            try Accepted_completed (handler input)
            with e -> Accepted_failed (Printexc.to_string e))
        in
        Hashtbl.replace outcomes pid outcome

    let response pid outcome =
      Coresdk.Update_response { protocol_instance_id = pid; outcome }

    (* accepted then completed on success; accepted then rejected if the handler
       raised after acceptance; a lone rejected if the validator (or a missing
       handler) rejected before acceptance. *)
    let responses () =
      List.concat_map
        (fun (pid, _run_validator) ->
          match Hashtbl.find_opt outcomes pid with
          | Some (Accepted_completed result) ->
            [ response pid Coresdk.Update_accepted;
              response pid (Coresdk.Update_completed result) ]
          | Some (Accepted_failed msg) ->
            [ response pid Coresdk.Update_accepted;
              response pid (Coresdk.Update_rejected msg) ]
          | Some (Rejected msg) -> [ response pid (Coresdk.Update_rejected msg) ]
          | None -> [ response pid (Coresdk.Update_rejected "update rejected") ])
        updates

    (* a rejected update was never admitted to history, so drop its event to keep the
       log consistent with what core replays after an eviction. *)
    let drop_rejected_events () =
      List.iter
        (fun (pid, _) ->
          match Hashtbl.find_opt outcomes pid with
          | Some (Accepted_completed _ | Accepted_failed _) -> ()
          | Some (Rejected _) | None ->
            state.events_rev <-
              List.filter
                (function
                  | Update u -> u.protocol_instance_id <> pid
                  | _ -> true)
                state.events_rev)
        updates
  end in
  let open Effect.Deep in
  (* emit each operation's command exactly once. The [issued] set lives in run_state,
     so a re-run within the same cached run does not re-issue an outstanding operation;
     a query replay neither emits nor records. *)
  let emit_once key cmd =
    if (not query_mode) && not (Hashtbl.mem state.issued key) then (
      Hashtbl.replace state.issued key ();
      emit cmd)
  in
  (* ---- the scheduler --------------------------------------------------- *)
  (* per-operation waiters keyed by seq. A matching resolution wakes the waiter; while
     it stays in the table the operation is unresolved. A child awaits in two stages,
     so it moves from the start table to the completion table when it starts. *)
  let act_waiters : (int, resolution -> unit) Hashtbl.t = Hashtbl.create 8 in
  let timer_waiters : (int, unit -> unit) Hashtbl.t = Hashtbl.create 8 in
  let child_start_waiters : (int, child_start -> unit) Hashtbl.t = Hashtbl.create 8 in
  let child_result_waiters : (int, resolution -> unit) Hashtbl.t = Hashtbl.create 8 in
  (* fibers blocked on a predicate, re-checked after each event *)
  let cond_waiters : ((unit -> bool) * (unit, unit) continuation) list ref = ref [] in
  (* runnable resumes, drained FIFO so command-issue order is deterministic *)
  let ready : (unit -> unit) Queue.t = Queue.create () in
  let run_ready () = while not (Queue.is_empty ready) do (Queue.pop ready) () done in
  (* after each event, wake any fiber whose predicate now holds *)
  let wake_conditions () =
    let woken, still = List.partition (fun (p, _) -> p ()) !cond_waiters in
    cond_waiters := still;
    List.iter (fun (_, k) -> Queue.push (fun () -> continue k ()) ready) woken
  in
  (* ---- cancellation scopes --------------------------------------------- *)
  (* scope 0 is the root. A non-detached child inherits its parent's cancellation; a
     detached one does not (it has no recorded parent). *)
  let scope_seq = ref 0 in
  let scope_parent : (int, int) Hashtbl.t = Hashtbl.create 8 in
  let cancelled_scopes : (int, unit) Hashtbl.t = Hashtbl.create 8 in
  let rec is_cancelled sc =
    Hashtbl.mem cancelled_scopes sc
    ||
    match Hashtbl.find_opt scope_parent sc with
    | Some p -> is_cancelled p
    | None -> false
  in
  (* every started activity/timer/child is cancellable until it resolves. [op_scope]
     records the scope it ran under and [op_cancel] its [cancel_spec] (what to emit and
     whether to raise at once). [cancel_hook], present once a raise-on-cancel op is
     awaited, discontinues the parked fiber with Canceled. Keys are "act:seq" /
     "timer:seq" / "child:seq". *)
  let op_scope : (string, int) Hashtbl.t = Hashtbl.create 8 in
  let op_cancel : (string, cancel_spec) Hashtbl.t = Hashtbl.create 8 in
  let cancel_hook : (string, unit -> unit) Hashtbl.t = Hashtbl.create 8 in
  let track_op key scope ~cmd ~emit ~raise_now =
    Hashtbl.replace op_scope key scope;
    Hashtbl.replace op_cancel key
      { cmd; emit_on_cancel = emit; raise_on_cancel = raise_now }
  in
  let untrack_op key =
    Hashtbl.remove op_cancel key;
    Hashtbl.remove cancel_hook key
  in
  let do_cancel sc =
    Hashtbl.replace cancelled_scopes sc ();
    (* copy: cancel_hook entries are removed while iterating *)
    Hashtbl.iter
      (fun key spec ->
        if is_cancelled (Hashtbl.find op_scope key) then (
          if spec.emit_on_cancel then emit_once ("cancel-" ^ key) spec.cmd;
          (* raise now, or leave the awaiter parked for the server's resolution *)
          if spec.raise_on_cancel then
            match Hashtbl.find_opt cancel_hook key with
            | Some h -> Hashtbl.remove cancel_hook key; h ()
            | None -> ()))
      (Hashtbl.copy op_cancel)
  in
  let arg =
    match state.init_arg with
    | Some p -> p
    | None -> Codec.to_payload Codec.unit ()
  in
  let dummy = Codec.to_payload Codec.unit () in
  (* spawned fibers: each has a seq-keyed outcome and one waiter for whoever awaits
     it. The result value itself rides in the future's own cell (Workflow.spawn); the
     scheduler only tracks done-or-failed. *)
  let fiber_seq = ref 0 in
  let fiber_outcome : (int, (unit, exn) result) Hashtbl.t = Hashtbl.create 8 in
  let fiber_waiters : (int, (unit, exn) result -> unit) Hashtbl.t = Hashtbl.create 8 in
  let resolve_fiber id r =
    Hashtbl.replace fiber_outcome id r;
    match Hashtbl.find_opt fiber_waiters id with
    | Some w -> Hashtbl.remove fiber_waiters id; Queue.push (fun () -> w r) ready
    | None -> ()
  in
  let root_output = ref None in
  (* the root fiber completing closes the workflow; any other fiber raising also fails
     it. Catch exceptions in the body (try/with) to compensate and continue (saga). *)
  let root_done = function
    | Ok () ->
      if not query_mode then
        commands := [ Coresdk.Complete_workflow_execution !root_output ]
    | Error (Workflow.Canceled reason) ->
      (* the root scope was cancelled and the body did not deny it: close as canceled *)
      if query_mode then
        Eio.traceln "[wf] workflow canceled during query replay: %s" reason
      else (
        Eio.traceln "[wf] workflow canceled: %s" reason;
        commands := [ Coresdk.Cancel_workflow_execution ])
    | Error exn ->
      let msg = Printexc.to_string exn in
      if query_mode then
        Eio.traceln "[wf] workflow raised during query replay: %s" msg
      else (
        Eio.traceln "[wf] failing workflow execution: %s" msg;
        commands := [ Coresdk.Fail_workflow_execution msg ])
  in
  (* run one fiber (the root body, or a spawned thunk) under the shared handler.
     [on_done] gets Ok () on normal completion, Error on an uncaught raise. *)
  let rec run_fiber (on_done : (unit, exn) result -> unit) (thunk : unit -> unit) =
    match_with thunk ()
    {
      retc = (fun () -> on_done (Ok ()));
      exnc = (fun exn -> on_done (Error exn));
      effc =
        (fun (type a) (eff : a Effect.t) ->
          match eff with
          | Workflow.Start_activity_effect
              { scope; activity_type; arg; start_to_close; max_attempts; cancel_type } ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr act_seq;
                let s = !act_seq in
                let key = Printf.sprintf "act:%d" s in
                emit_once key
                  (Coresdk.Schedule_activity
                     {
                       seq = s;
                       activity_id = string_of_int s;
                       activity_type;
                       task_queue = default_tq;
                       arguments = [ arg ];
                       start_to_close;
                       max_attempts;
                       cancellation_type = cancel_type;
                     });
                (* Abandon (2) sends no cancel command; Wait_cancellation_completed (1)
                   sends it but waits for the resolution rather than raising at once. *)
                track_op key scope
                  ~cmd:(Coresdk.Request_cancel_activity { seq = s })
                  ~emit:(cancel_type <> 2) ~raise_now:(cancel_type <> 1);
                continue k (Workflow.Op_activity s))
          | Workflow.Start_timer_effect { scope; start_to_fire } ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr timer_seq;
                let s = !timer_seq in
                let key = Printf.sprintf "timer:%d" s in
                emit_once key (Coresdk.Start_timer { seq = s; start_to_fire });
                track_op key scope ~cmd:(Coresdk.Cancel_timer { seq = s })
                  ~emit:true ~raise_now:true;
                continue k (Workflow.Op_timer s))
          | Workflow.Start_child_effect
              { scope; workflow_id; workflow_type; input; task_queue;
                parent_close_policy; execution_timeout; run_timeout;
                wait_for_cancellation } ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr child_seq;
                let s = !child_seq in
                let key = Printf.sprintf "child:%d" s in
                let wid =
                  match workflow_id with
                  | Some id -> id
                  | None -> Printf.sprintf "%s/%d" state.wf_id s
                in
                let tq = match task_queue with Some q -> q | None -> default_tq in
                emit_once key
                  (Coresdk.Start_child_workflow_execution
                     {
                       seq = s;
                       namespace = "";
                       workflow_id = wid;
                       workflow_type;
                       task_queue = tq;
                       input = [ input ];
                       execution_timeout;
                       run_timeout;
                       parent_close_policy;
                     });
                (* wait_for_cancellation sends the cancel but waits for the child's own
                   resolution instead of raising Canceled at once *)
                track_op key scope
                  ~cmd:
                    (Coresdk.Cancel_child_workflow_execution
                       { child_workflow_seq = s; reason = "" })
                  ~emit:true ~raise_now:(not wait_for_cancellation);
                continue k (Workflow.Op_child s))
          | Workflow.Await_effect op ->
            Some
              (fun (k : (a, unit) continuation) ->
                (* park on [op]; on resolution resume with the payload, or discontinue
                   with a labelled failure. A cancellable op (activity/timer/child)
                   raises Canceled if its scope is already cancelled, or later if a
                   cancel reaches it while parked. A child awaits in two stages. *)
                let ok p = continue k p
                and fail label msg = discontinue k (Failure (label ^ msg))
                and canceled msg = discontinue k (Workflow.Canceled msg) in
                let park_cancellable key ~remove_waiter ~register_waiter =
                  (* a raise-on-cancel op raises Canceled at once if its scope is already
                     cancelled, and installs a hook so a later cancel reaches it while
                     parked. A wait-on-cancel op always parks and lets the server's
                     resolution wake it. *)
                  let raise_on_cancel =
                    match Hashtbl.find_opt op_cancel key with
                    | Some s -> s.raise_on_cancel
                    | None -> true
                  in
                  if raise_on_cancel && is_cancelled (Hashtbl.find op_scope key) then
                    discontinue k (Workflow.Canceled "scope canceled")
                  else (
                    register_waiter ();
                    if raise_on_cancel then
                      Hashtbl.replace cancel_hook key (fun () ->
                          remove_waiter ();
                          Queue.push
                            (fun () ->
                              discontinue k (Workflow.Canceled "scope canceled"))
                            ready))
                in
                match op with
                | Workflow.Op_activity s ->
                  let key = Printf.sprintf "act:%d" s in
                  park_cancellable key
                    ~remove_waiter:(fun () -> Hashtbl.remove act_waiters s)
                    ~register_waiter:(fun () ->
                      Hashtbl.replace act_waiters s (fun r ->
                          Hashtbl.remove act_waiters s;
                          untrack_op key;
                          match r with
                          | R_ok p -> ok p
                          | R_cancelled msg -> canceled msg
                          | R_fail msg -> fail "activity failed: " msg))
                | Workflow.Op_timer s ->
                  let key = Printf.sprintf "timer:%d" s in
                  park_cancellable key
                    ~remove_waiter:(fun () -> Hashtbl.remove timer_waiters s)
                    ~register_waiter:(fun () ->
                      Hashtbl.replace timer_waiters s (fun () ->
                          Hashtbl.remove timer_waiters s;
                          untrack_op key;
                          ok (Codec.to_payload Codec.unit ())))
                | Workflow.Op_child s ->
                  let key = Printf.sprintf "child:%d" s in
                  park_cancellable key
                    ~remove_waiter:(fun () ->
                      Hashtbl.remove child_start_waiters s;
                      Hashtbl.remove child_result_waiters s)
                    ~register_waiter:(fun () ->
                      Hashtbl.replace child_start_waiters s (fun cs ->
                          Hashtbl.remove child_start_waiters s;
                          match cs with
                          | Child_start_fail msg ->
                            untrack_op key;
                            fail "child workflow start failed: " msg
                          | Child_run _run_id ->
                            Hashtbl.replace child_result_waiters s (fun r ->
                                Hashtbl.remove child_result_waiters s;
                                untrack_op key;
                                match r with
                                | R_ok p -> ok p
                                | R_cancelled msg -> canceled msg
                                | R_fail msg -> fail "child workflow failed: " msg)))
                | Workflow.Op_fiber id ->
                  let wake = function
                    | Ok () -> ok dummy
                    | Error exn -> discontinue k exn
                  in
                  (match Hashtbl.find_opt fiber_outcome id with
                   | Some r -> wake r
                   | None -> Hashtbl.replace fiber_waiters id wake))
          | Workflow.Await_any_effect ops ->
            Some
              (fun (k : (a, unit) continuation) ->
                (* park on every op; the first to resolve wakes the fiber with its
                   index and payload, and the rest become no-ops *)
                let resolved = ref false in
                let wake idx r =
                  if not !resolved then (
                    resolved := true;
                    match r with
                    | R_ok p -> continue k (idx, p)
                    | R_cancelled msg -> discontinue k (Workflow.Canceled msg)
                    | R_fail msg ->
                      discontinue k (Failure ("awaited operation failed: " ^ msg)))
                in
                List.iteri
                  (fun idx op ->
                    match op with
                    | Workflow.Op_activity s ->
                      Hashtbl.replace act_waiters s (fun r ->
                          Hashtbl.remove act_waiters s;
                          wake idx r)
                    | Workflow.Op_timer s ->
                      Hashtbl.replace timer_waiters s (fun () ->
                          Hashtbl.remove timer_waiters s;
                          wake idx (R_ok (Codec.to_payload Codec.unit ())))
                    | Workflow.Op_child s ->
                      Hashtbl.replace child_start_waiters s (fun cs ->
                          Hashtbl.remove child_start_waiters s;
                          match cs with
                          | Child_start_fail msg ->
                            wake idx (R_fail ("child workflow start failed: " ^ msg))
                          | Child_run _run_id ->
                            Hashtbl.replace child_result_waiters s (fun r ->
                                Hashtbl.remove child_result_waiters s;
                                wake idx r))
                    | Workflow.Op_fiber id ->
                      let wake_f = function
                        | Ok () -> wake idx (R_ok dummy)
                        | Error exn -> wake idx (R_fail (Printexc.to_string exn))
                      in
                      (match Hashtbl.find_opt fiber_outcome id with
                       | Some r -> wake_f r
                       | None -> Hashtbl.replace fiber_waiters id wake_f))
                  ops)
          | Workflow.Spawn_effect thunk ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr fiber_seq;
                let id = !fiber_seq in
                (* the child runs when the ready queue next drains; the parent
                   continues immediately *)
                Queue.push (fun () -> run_fiber (resolve_fiber id) thunk) ready;
                continue k (Workflow.Op_fiber id))
          | Workflow.New_scope_effect { parent; detached } ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr scope_seq;
                let sid = !scope_seq in
                (* a non-detached scope records its parent so cancellation inherits;
                   a detached one does not *)
                if not detached then Hashtbl.replace scope_parent sid parent;
                continue k sid)
          | Workflow.Cancel_scope_effect sc ->
            Some (fun (k : (a, unit) continuation) -> do_cancel sc; continue k ())
          | Workflow.Cancel_requested_effect sc ->
            Some (fun (k : (a, unit) continuation) -> continue k (is_cancelled sc))
          | Workflow.Continue_as_new_effect new_arg ->
            Some
              (fun (_ : (a, unit) continuation) ->
                (* terminal: end this run, start a fresh one; drop the k. A read-only
                   query replay never emits this. *)
                if not query_mode then
                  commands :=
                    [ Coresdk.Continue_as_new { arguments = [ new_arg ] } ])
          | Workflow.Register_signal_handler_effect (name, handler) ->
            Some
              (fun (k : (a, unit) continuation) ->
                Signals.register name handler;
                continue k ())
          | Workflow.Register_query_handler_effect (name, handler) ->
            Some
              (fun (k : (a, unit) continuation) ->
                Queries.register name handler;
                continue k ())
          | Workflow.Register_update_handler_effect (name, validator, handler) ->
            Some
              (fun (k : (a, unit) continuation) ->
                Updates.register name validator handler;
                continue k ())
          | Workflow.Wait_condition_effect pred ->
            Some
              (fun (k : (a, unit) continuation) ->
                (* true now: resume inline. false: park on the predicate for the log
                   walk to re-check after each event. *)
                if pred () then continue k ()
                else cond_waiters := (pred, k) :: !cond_waiters)
          | _ -> None);
    }
  in
  run_fiber root_done
    (fun () ->
      root_output :=
        Some
          (wf.Workflow.body ~task_queue:default_tq ~workflow_id:state.wf_id ~run_id
             ~can_suggested ~history_length arg));
  run_ready ();
  (* drive the accumulated history log in order. A resolution wakes its waiter (via
     the ready queue, so resumes run FIFO); a signal or update delivers to its
     handlers; after each event any predicate that now holds is woken. *)
  List.iter
    (fun ev ->
      (match ev with
       | Activity_resolved (seq, r) -> (
         match Hashtbl.find_opt act_waiters seq with
         | Some w -> Queue.push (fun () -> w r) ready
         | None -> ())
       | Timer_fired seq -> (
         match Hashtbl.find_opt timer_waiters seq with
         | Some w -> Queue.push (fun () -> w ()) ready
         | None -> ())
       | Child_started (seq, cs) -> (
         match Hashtbl.find_opt child_start_waiters seq with
         | Some w -> Queue.push (fun () -> w cs) ready
         | None -> ())
       | Child_resolved (seq, r) -> (
         match Hashtbl.find_opt child_result_waiters seq with
         | Some w -> Queue.push (fun () -> w r) ready
         | None -> ())
       | Signal (name, payload) -> Signals.deliver name payload
       | Update { protocol_instance_id; name; input } ->
         Updates.deliver protocol_instance_id name input);
      run_ready ();
      wake_conditions ();
      run_ready ())
    (List.rev state.events_rev);
  (* answer queries and emit update responses from the handlers/outcomes the replay
     built, then drop any rejected update's event so the log stays consistent with
     what core replays after an eviction. *)
  let query_commands = Queries.responses () in
  let update_commands = Updates.responses () in
  Updates.drop_rejected_events ();
  List.rev !commands @ query_commands @ update_commands
