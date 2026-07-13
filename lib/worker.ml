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

(* A worker: polls a task queue and serves registered workflows and activities.
   This is the runtime — the effect handler that drives workflow bodies and the
   two poll loops. Re-exported as Temporal.Worker. *)

type t = {
  client : Client.t;
  worker : Temporal_ffi.worker;
  task_queue : string;
  workflows : Workflow.reg list;
  activities : Activity.reg list;
}

let create (client : Client.t) ~task_queue =
  let worker = Temporal_ffi.worker_new client.Client.conn task_queue in
  { client; worker; task_queue; workflows = []; activities = [] }

(* Pipe-friendly registration: the phantom types are erased in here, so the [reg]
   existential never appears in user code. *)
let register_workflow (w : (_, _) Workflow.t) (t : t) : t =
  let r = Workflow.reg w in
  Eio.traceln "registered workflow: %s" r.Workflow.name;
  { t with workflows = r :: t.workflows }

let register_activity (a : (_, _) Activity.t) (t : t) : t =
  let r = Activity.reg a in
  Eio.traceln "registered activity: %s" r.Activity.name;
  { t with activities = r :: t.activities }

(* ---- workflow execution ------------------------------------------------ *)
(* We keep a small amount of per-run state (the init argument + a history-ordered
   log of the external events seen so far) and re-run the workflow body from the
   top on each activation. A cursor walks that log in order: execute_activity /
   sleep consume the next matching resolution and resume the body; if the demanded
   resolution has not arrived yet we emit the command and suspend (drop the
   continuation). This is the deterministic replay model, minus persisting
   continuations across polls. *)

type resolution = R_ok of Codec.payload | R_fail of string

(* External events the server delivers, kept in a single history-ordered log
   rather than per-kind maps. Order is irrelevant for activities/timers (the body
   demands them in a fixed sequence), but it is load-bearing for signals, which
   must be delivered relative to the resolutions around them — so this log is the
   foundation that makes ordered signal delivery possible. *)
type event =
  | Activity_resolved of int * resolution (* seq, result *)
  | Timer_fired of int (* seq *)
  | Signal of string * Codec.payload (* name, encoded arg *)
  | Update of {
      protocol_instance_id : string; (* correlates the UpdateResponse(s) *)
      name : string;
      input : Codec.payload;
    }
    (* an admitted update: order-sensitive like a signal, delivered by the same
       cursor walk. Kept in the log so its state mutation replays; the accept/
       reject decision and the response are per-activation, not stored here. *)

type run_state = {
  mutable wf_name : string;
  mutable init_arg : Codec.payload option;
  mutable events_rev : event list; (* history order, newest first (cons to append) *)
}

let runs : (string, run_state) Hashtbl.t = Hashtbl.create 16

let get_run run_id =
  match Hashtbl.find_opt runs run_id with
  | Some s -> s
  | None ->
    let s = { wf_name = ""; init_arg = None; events_rev = [] } in
    Hashtbl.replace runs run_id s;
    s

(* [queries] are the (query_id, query_type, arguments) of any QueryWorkflow jobs,
   and [updates] the (protocol_instance_id, run_validator) of any DoUpdate jobs, on
   this activation (whose Update events apply_job already appended to the log, in
   job order). When [query_mode] is set (a pure-query activation), the body is
   replayed read-only: it rebuilds state from history but suppresses every
   workflow-advancing command, so it neither re-schedules the frontier's pending
   effect nor re-completes an already-finished run. Queries are answered, and
   updates validated/run and responded to, from the handlers the body registers. *)
let run_workflow (t : t) (wf : Workflow.reg) (state : run_state) ~can_suggested
    ~history_length ~query_mode ~queries ~updates : Coresdk.wf_command list =
  let commands = ref [] in
  let act_seq = ref 0 in
  let timer_seq = ref 0 in
  (* the replay cursor: remaining events in history order. Peek the next event;
     advance only when the body consumes it. (When signals land, advancing past a
     signal entry is exactly where its handler will fire.) *)
  let cursor = ref (List.rev state.events_rev) in
  let peek () = match !cursor with ev :: _ -> Some ev | [] -> None in
  let advance () = match !cursor with _ :: rest -> cursor := rest | [] -> () in
  (* signal handlers registered by the body on this run (rebuilt each re-run). *)
  let signal_handlers : (string, Codec.payload -> unit) Hashtbl.t =
    Hashtbl.create 8
  in
  (* query handlers registered by the body on this run (rebuilt each re-run),
     invoked after the replay reaches its frontier to answer QueryWorkflow jobs. *)
  let query_handlers : (string, Codec.payload -> Codec.payload) Hashtbl.t =
    Hashtbl.create 8
  in
  (* signals the cursor passed before their handler was registered, held per name
     in arrival order and drained when on_signal registers a matching handler.
     Matches Temporal, which buffers signals until a handler exists. In the normal
     case handlers are registered at the top of the body, before any checkpoint, so
     nothing is ever buffered. *)
  let buffered_signals : (string, Codec.payload Queue.t) Hashtbl.t =
    Hashtbl.create 4
  in
  let buffer_signal name payload =
    let q =
      match Hashtbl.find_opt buffered_signals name with
      | Some q -> q
      | None ->
        let q = Queue.create () in
        Hashtbl.replace buffered_signals name q;
        q
    in
    Queue.add payload q
  in
  (* update handlers registered by the body on this run: name -> (validator?,
     handler). Delivering an update runs the validator (only on first delivery,
     i.e. pid in validate_pids) then the handler, recording the per-update outcome
     below so the UpdateResponse(s) can be emitted after the replay. *)
  let update_handlers :
      (string, (Codec.payload -> unit) option * (Codec.payload -> Codec.payload))
      Hashtbl.t =
    Hashtbl.create 4
  in
  let validate_pids : (string, unit) Hashtbl.t = Hashtbl.create 4 in
  List.iter
    (fun (pid, run_validator) ->
      if run_validator then Hashtbl.replace validate_pids pid ())
    updates;
  let update_accepted : (string, unit) Hashtbl.t = Hashtbl.create 4 in
  let update_results : (string, Codec.payload) Hashtbl.t = Hashtbl.create 4 in
  let update_failed : (string, string) Hashtbl.t = Hashtbl.create 4 in
  let deliver_update pid name input =
    match Hashtbl.find_opt update_handlers name with
    | None ->
      Hashtbl.replace update_failed pid
        (Printf.sprintf "no update handler registered for %S" name)
    | Some (validator, handler) ->
      (* validate only on first delivery; a raising validator rejects, and the
         handler never runs. Otherwise the update is accepted. *)
      let rejected =
        Hashtbl.mem validate_pids pid
        &&
        match validator with
        | Some v -> (
          try
            v input;
            false
          with e ->
            Hashtbl.replace update_failed pid (Printexc.to_string e);
            true)
        | None -> false
      in
      if not rejected then (
        Hashtbl.replace update_accepted pid ();
        (* a handler that raises after acceptance fails the update, not the run *)
        try Hashtbl.replace update_results pid (handler input)
        with e -> Hashtbl.replace update_failed pid (Printexc.to_string e))
  in
  (* deliver signals and updates at the cursor head, advancing past each, until the
     head is a resolution or the cursor is empty — applying each exactly where it
     sits relative to the resolutions around it. A signal whose handler isn't
     registered yet is buffered, not dropped. *)
  let rec deliver_pending () =
    match !cursor with
    | Signal (name, payload) :: rest ->
      cursor := rest;
      (match Hashtbl.find_opt signal_handlers name with
       | Some h -> h payload
       | None -> buffer_signal name payload);
      deliver_pending ()
    | Update { protocol_instance_id; name; input } :: rest ->
      cursor := rest;
      deliver_update protocol_instance_id name input;
      deliver_pending ()
    | _ -> ()
  in
  let arg =
    match state.init_arg with
    | Some p -> p
    | None -> Codec.to_payload Codec.unit ()
  in
  let open Effect.Deep in
  match_with
    (fun () ->
      wf.Workflow.body ~task_queue:t.task_queue ~can_suggested ~history_length arg)
    ()
    {
      retc =
        (fun (output : Codec.payload) ->
          (* a read-only query replay must not (re-)complete the workflow *)
          if not query_mode then
            commands := [ Coresdk.Complete_workflow_execution (Some output) ]);
      exnc =
        (fun exn ->
          (* an uncaught exception in the workflow body fails the execution;
             catch it in the body (try/with) to compensate and continue (saga) *)
          let msg = Printexc.to_string exn in
          if query_mode then
            Eio.traceln "[wf] workflow raised during query replay: %s" msg
          else (
            Eio.traceln "[wf] failing workflow execution: %s" msg;
            commands := [ Coresdk.Fail_workflow_execution msg ]));
      effc =
        (fun (type a) (eff : a Effect.t) ->
          match eff with
          | Workflow.Schedule_activity_effect
              { activity_type; arg; start_to_close; max_attempts } ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr act_seq;
                let s = !act_seq in
                deliver_pending ();
                match peek () with
                | Some (Activity_resolved (s', R_ok payload)) when s' = s ->
                  advance ();
                  continue k payload
                | Some (Activity_resolved (s', R_fail msg)) when s' = s ->
                  advance ();
                  discontinue k (Failure ("activity failed: " ^ msg))
                | _ ->
                  (* not resolved yet: schedule it and suspend this run. A query
                     replay is read-only — suppress the command and just suspend at
                     the frontier. *)
                  if not query_mode then
                    commands :=
                      Coresdk.Schedule_activity
                        {
                          seq = s;
                          activity_id = string_of_int s;
                          activity_type;
                          task_queue = t.task_queue;
                          arguments = [ arg ];
                          start_to_close;
                          max_attempts;
                        }
                      :: !commands)
          | Workflow.Start_timer_effect { start_to_fire } ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr timer_seq;
                let s = !timer_seq in
                deliver_pending ();
                match peek () with
                | Some (Timer_fired s') when s' = s -> advance (); continue k ()
                | _ ->
                  if not query_mode then
                    commands :=
                      Coresdk.Start_timer { seq = s; start_to_fire } :: !commands)
          | Workflow.Continue_as_new_effect new_arg ->
            Some
              (fun (_ : (a, unit) continuation) ->
                (* terminal: end this run, start a fresh one; drop the k. A query
                   replay is read-only, so it never emits this. *)
                if not query_mode then
                  commands :=
                    [ Coresdk.Continue_as_new { arguments = [ new_arg ] } ])
          | Workflow.Register_signal_handler_effect (name, handler) ->
            Some
              (fun (k : (a, unit) continuation) ->
                Hashtbl.replace signal_handlers name handler;
                (* deliver any signals that arrived before this handler existed,
                   in arrival order, then discard the buffer for this name *)
                (match Hashtbl.find_opt buffered_signals name with
                 | Some q ->
                   Queue.iter handler q;
                   Hashtbl.remove buffered_signals name
                 | None -> ());
                continue k ())
          | Workflow.Register_query_handler_effect (name, handler) ->
            Some
              (fun (k : (a, unit) continuation) ->
                Hashtbl.replace query_handlers name handler;
                continue k ())
          | Workflow.Register_update_handler_effect (name, validator, handler) ->
            Some
              (fun (k : (a, unit) continuation) ->
                Hashtbl.replace update_handlers name (validator, handler);
                continue k ())
          | Workflow.Wait_condition_effect pred ->
            Some
              (fun (k : (a, unit) continuation) ->
                deliver_pending ();
                if pred () then continue k ()
                else () (* condition false: suspend, emit no command *))
          | _ -> None);
    };
  (* flush any signals/updates still at the frontier that no checkpoint reached
     (e.g. the body completed without a trailing wait_condition) so an arrived
     update always gets a response. Already-delivered events were advanced past, so
     this only fires stragglers. *)
  deliver_pending ();
  (* answer each query on this activation from the handlers the body registered as
     it replayed to its frontier. A missing or raising handler answers with a
     failure rather than failing the workflow. *)
  let query_commands =
    List.map
      (fun (query_id, query_type, arguments) ->
        let result =
          match Hashtbl.find_opt query_handlers query_type with
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
  in
  (* emit the UpdateResponse(s) for updates that arrived this activation: accepted
     then completed on success; accepted then rejected if the handler raised after
     acceptance; a lone rejected if the validator rejected. *)
  let update_response pid outcome =
    Coresdk.Update_response { protocol_instance_id = pid; outcome }
  in
  let update_commands =
    List.concat_map
      (fun (pid, _run_validator) ->
        if Hashtbl.mem update_accepted pid then
          let accepted = update_response pid Coresdk.Update_accepted in
          match Hashtbl.find_opt update_results pid with
          | Some result ->
            [ accepted; update_response pid (Coresdk.Update_completed result) ]
          | None ->
            let msg =
              match Hashtbl.find_opt update_failed pid with
              | Some m -> m
              | None -> "update handler did not complete"
            in
            [ accepted; update_response pid (Coresdk.Update_rejected msg) ]
        else
          let msg =
            match Hashtbl.find_opt update_failed pid with
            | Some m -> m
            | None -> "update rejected"
          in
          [ update_response pid (Coresdk.Update_rejected msg) ])
      updates
  in
  (* a rejected update was never admitted to history, so drop its event to keep the
     log consistent with what core replays after an eviction. *)
  List.iter
    (fun (pid, _) ->
      if not (Hashtbl.mem update_accepted pid) then
        state.events_rev <-
          List.filter
            (function
              | Update u -> u.protocol_instance_id <> pid
              | _ -> true)
            state.events_rev)
    updates;
  List.rev !commands @ query_commands @ update_commands

let apply_job (state : run_state) = function
  | Coresdk.Initialize_workflow { workflow_type; arguments } ->
    state.wf_name <- workflow_type;
    state.init_arg <- (match arguments with p :: _ -> Some p | [] -> None)
  | Coresdk.Resolve_activity { seq; result } ->
    let r =
      match result with
      | Coresdk.Completed p ->
        R_ok (match p with Some x -> x | None -> Codec.to_payload Codec.unit ())
      | Coresdk.Failed msg -> R_fail msg
      | Coresdk.Other_resolution -> R_fail "unknown activity resolution"
    in
    state.events_rev <- Activity_resolved (seq, r) :: state.events_rev
  | Coresdk.Fire_timer { seq } ->
    state.events_rev <- Timer_fired seq :: state.events_rev
  | Coresdk.Signal_workflow { signal_name; input } ->
    let p = match input with p :: _ -> p | [] -> Codec.to_payload Codec.unit () in
    state.events_rev <- Signal (signal_name, p) :: state.events_rev
  | Coresdk.Query_workflow _ ->
    (* Queries don't advance the workflow, so they are not appended to the event
       log. Answering a query (re-run the body in read-only mode and emit
       Respond_to_query) is handled separately from history application and is
       the next runtime increment; for now the job is decoded but not served. *)
    ()
  | Coresdk.Do_update { protocol_instance_id; name; input; run_validator = _ } ->
    (* admit the update to the log in job order, like a signal; the validator gate
       and the UpdateResponse are handled per-activation in run_workflow, which
       drops the event again if the validator rejects it. *)
    let p = match input with p :: _ -> p | [] -> Codec.to_payload Codec.unit () in
    state.events_rev <-
      Update { protocol_instance_id; name; input = p } :: state.events_rev
  | Coresdk.Remove_from_cache | Coresdk.Other -> ()

let workflow_loop (t : t) =
  let runtime = t.client.Client.runtime and worker = t.worker in
  let rec loop () =
    match Temporal_ffi.poll_workflow_activation runtime worker with
    | Error e -> Eio.traceln "[wf] poll loop stopped: %s" e
    | Ok bytes ->
      let a = Coresdk.decode_wf_activation bytes in
      let evict =
        List.exists
          (function Coresdk.Remove_from_cache -> true | _ -> false)
          a.Coresdk.jobs
      in
      let state = get_run a.Coresdk.run_id in
      List.iter (apply_job state) a.Coresdk.jobs;
      (* QueryWorkflow jobs are answered by re-running the body to its frontier;
         pull them out, and note whether this activation carries nothing but
         queries — if so the replay must be read-only (see run_workflow). *)
      let queries =
        List.filter_map
          (function
            | Coresdk.Query_workflow { query_id; query_type; arguments } ->
              Some (query_id, query_type, arguments)
            | _ -> None)
          a.Coresdk.jobs
      in
      let query_mode =
        queries <> []
        && not
             (List.exists
                (function
                  | Coresdk.Query_workflow _ | Coresdk.Remove_from_cache
                  | Coresdk.Other ->
                    false
                  | _ -> true)
                a.Coresdk.jobs)
      in
      (* DoUpdate jobs, as (protocol_instance_id, run_validator); their Update
         events were appended by apply_job above. run_validator is true only on
         first delivery. *)
      let updates =
        List.filter_map
          (function
            | Coresdk.Do_update { protocol_instance_id; run_validator; _ } ->
              Some (protocol_instance_id, run_validator)
            | _ -> None)
          a.Coresdk.jobs
      in
      let commands =
        if evict then (
          Hashtbl.remove runs a.Coresdk.run_id;
          [])
        else
          match
            List.find_opt
              (fun (w : Workflow.reg) -> w.Workflow.name = state.wf_name)
              t.workflows
          with
          | Some wf ->
            run_workflow t wf state
              ~can_suggested:a.Coresdk.continue_as_new_suggested
              ~history_length:a.Coresdk.history_length ~query_mode ~queries ~updates
          | None ->
            Eio.traceln "[wf] no workflow registered as %S" state.wf_name;
            []
      in
      Eio.traceln "[wf] run_id=%s jobs=%d -> %d command(s)" a.Coresdk.run_id
        (List.length a.Coresdk.jobs) (List.length commands);
      (match
         Temporal_ffi.complete_workflow_activation runtime worker
           ~completion:
             (Coresdk.encode_wf_completion ~run_id:a.Coresdk.run_id ~commands)
       with
       | Ok () -> ()
       | Error e -> Eio.traceln "[wf] complete error: %s" e);
      loop ()
  in
  loop ()

(* ---- activity execution ------------------------------------------------ *)
let activity_loop (t : t) =
  let runtime = t.client.Client.runtime and worker = t.worker in
  let rec loop () =
    match Temporal_ffi.poll_activity_task runtime worker with
    | Error e -> Eio.traceln "[act] poll loop stopped: %s" e
    | Ok bytes -> (
      let task = Coresdk.decode_activity_task bytes in
      match task.Coresdk.start with
      | None -> loop ()
      | Some { activity_type; input } ->
        let result =
          match
            List.find_opt
              (fun (a : Activity.reg) -> a.Activity.name = activity_type)
              t.activities
          with
          | None ->
            Coresdk.Act_failed ("no activity registered: " ^ activity_type)
          | Some act -> (
            try
              let arg =
                match input with
                | p :: _ -> p
                | [] -> Codec.to_payload Codec.unit ()
              in
              Coresdk.Act_completed (Some (act.Activity.run_payload arg))
            with e -> Coresdk.Act_failed (Printexc.to_string e))
        in
        Eio.traceln "[act] ran %s" activity_type;
        (match
           Temporal_ffi.complete_activity_task runtime worker
             ~completion:
               (Coresdk.encode_activity_completion
                  ~task_token:task.Coresdk.task_token ~result)
         with
         | Ok () -> ()
         | Error e -> Eio.traceln "[act] complete error: %s" e);
        loop ())
  in
  loop ()

let run (t : t) =
  Eio.traceln "worker polling task-queue '%s' (%d workflows, %d activities)"
    t.task_queue (List.length t.workflows) (List.length t.activities);
  Eio.Fiber.both
    (fun () -> t.client.Client.spawn_domain (fun () -> workflow_loop t))
    (fun () -> t.client.Client.spawn_domain (fun () -> activity_loop t))
