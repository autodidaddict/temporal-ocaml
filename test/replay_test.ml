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

(* Deterministic replay tests. These reach the internal modules directly (the
   public API in temporal.mli is sealed), so the engine can be driven without the
   FFI or a live server. This is the harness Phase 0 of the ADR-0004 plan calls for:
   feed synthetic activation jobs, assert the emitted commands. *)

module Codec = Temporal__Codec
module Coresdk = Temporal__Coresdk
module Pb = Temporal__Pb
module Activity = Temporal__Activity
module Signal = Temporal__Signal
module Query = Temporal__Query
module Update = Temporal__Update
module Workflow = Temporal__Workflow
module Replay = Temporal__Replay
module Replay_state = Temporal__Replay_state

let failures = ref 0

let check name cond =
  if cond then Printf.printf "ok   %s\n" name
  else (
    Printf.printf "FAIL %s\n" name;
    incr failures)

(* ---- wire: the new cancellation encoders -------------------------------- *)

(* (field, inner bytes) of a single-field WorkflowCommand message *)
let top_field bytes =
  let r = Pb.Reader.create bytes in
  let f, _w = Pb.Reader.key r in
  (f, Pb.Reader.bytes r)

(* value of field 1 (a varint) in a message, or -1 if absent *)
let field1_varint bytes =
  let r = Pb.Reader.create bytes in
  let v = ref (-1) in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 0 -> v := Pb.Reader.varint r
    | _, w -> Pb.Reader.skip r w
  done;
  !v

let () =
  let f, inner = top_field (Coresdk.encode_command (Coresdk.Request_cancel_activity { seq = 3 })) in
  check "encode RequestCancelActivity -> field 4, seq 3" (f = 4 && field1_varint inner = 3);
  let f, inner = top_field (Coresdk.encode_command (Coresdk.Cancel_timer { seq = 7 })) in
  check "encode CancelTimer -> field 5, seq 7" (f = 5 && field1_varint inner = 7);
  let f, _ = top_field (Coresdk.encode_command Coresdk.Cancel_workflow_execution) in
  check "encode CancelWorkflowExecution -> field 9" (f = 9);
  let f, inner =
    top_field
      (Coresdk.encode_command
         (Coresdk.Cancel_child_workflow_execution { child_workflow_seq = 2; reason = "r" }))
  in
  check "encode CancelChildWorkflowExecution -> field 12, seq 2" (f = 12 && field1_varint inner = 2)

(* value of [field] (a varint) in a message, or -1 if absent *)
let field_varint field bytes =
  let r = Pb.Reader.create bytes in
  let v = ref (-1) in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | f, 0 when f = field -> v := Pb.Reader.varint r
    | _, w -> Pb.Reader.skip r w
  done;
  !v

let () =
  let sched ct =
    Coresdk.encode_command
      (Coresdk.Schedule_activity
         { seq = 1;
           activity_id = "1";
           activity_type = "a";
           task_queue = "tq";
           arguments = [];
           start_to_close = 10.;
           max_attempts = 0;
           cancellation_type = ct;
         })
  in
  let _, inner = top_field (sched 2) in
  check "encode ScheduleActivity cancellation_type=2 -> field 13 = 2" (field_varint 13 inner = 2);
  let _, inner = top_field (sched 0) in
  check "encode ScheduleActivity default cancellation_type -> field 13 omitted"
    (field_varint 13 inner = -1)

(* ---- wire: the new decoders --------------------------------------------- *)

let () =
  (* WorkflowActivationJob { cancel_workflow=6 { reason=1 } } *)
  let cw =
    let w = Pb.Writer.create () in
    Pb.Writer.bytes w 1 "user stop";
    Pb.Writer.contents w
  in
  let job =
    let w = Pb.Writer.create () in
    Pb.Writer.bytes w 6 cw;
    Pb.Writer.contents w
  in
  check "decode CancelWorkflow job"
    (match Coresdk.decode_wf_job job with
     | Coresdk.Cancel_workflow { reason = "user stop" } -> true
     | _ -> false)

let () =
  (* ResolveActivity { seq=1; result=ActivityResolution { cancelled=3 {
       Cancellation { failure=1 = Failure { message=1 } } } } } *)
  let failure =
    let w = Pb.Writer.create () in
    Pb.Writer.bytes w 1 "act boom";
    Pb.Writer.contents w
  in
  let cancellation =
    let w = Pb.Writer.create () in
    Pb.Writer.bytes w 1 failure;
    Pb.Writer.contents w
  in
  let act_res =
    let w = Pb.Writer.create () in
    Pb.Writer.bytes w 3 cancellation;
    Pb.Writer.contents w
  in
  let ra =
    let w = Pb.Writer.create () in
    Pb.Writer.int w 1 5;
    Pb.Writer.bytes w 2 act_res;
    Pb.Writer.contents w
  in
  let job =
    let w = Pb.Writer.create () in
    Pb.Writer.bytes w 8 ra;
    Pb.Writer.contents w
  in
  check "decode ResolveActivity cancelled -> Cancelled msg"
    (match Coresdk.decode_wf_job job with
     | Coresdk.Resolve_activity { seq = 5; result = Coresdk.Cancelled "act boom" } -> true
     | _ -> false)

(* ---- replay harness: characterization tests ----------------------------- *)

(* an echo activity and a workflow that runs it once and returns the result *)
let echo =
  Activity.define ~name:"echo" ~input:Codec.string ~output:Codec.string (fun s -> s)

let echo_wf =
  Workflow.reg
    (Workflow.define ~name:"EchoW" ~input:Codec.string ~output:Codec.string
       (fun ctx s -> Workflow.execute_activity ctx echo s))

let init_job ~workflow_type ~workflow_id args =
  Coresdk.Initialize_workflow { workflow_type; workflow_id; arguments = args }

(* run one activation of [reg] against the accumulated [state] *)
let activation reg state ~run_id ~history_length =
  Replay.run_workflow reg state ~task_queue:"test-tq" ~run_id ~can_suggested:false
    ~history_length ~query_mode:false ~queries:[] ~updates:[]

let () =
  let run_id = "wf-echo" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st
    (init_job ~workflow_type:"EchoW" ~workflow_id:run_id [ Codec.to_payload Codec.string "hi" ]);
  let cmds1 = activation echo_wf st ~run_id ~history_length:1 in
  check "activity: schedules on init"
    (match cmds1 with
     | [ Coresdk.Schedule_activity { seq = 1; activity_type = "echo"; _ } ] -> true
     | _ -> false);
  Replay_state.apply_job st
    (Coresdk.Resolve_activity
       { seq = 1; result = Coresdk.Completed (Some (Codec.to_payload Codec.string "hi")) });
  let cmds2 = activation echo_wf st ~run_id ~history_length:2 in
  check "activity: completes on resolve with the result"
    (match cmds2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "hi"
     | _ -> false)

let unit_arg = Codec.to_payload Codec.unit ()
let approve = Signal.define ~name:"approve" Codec.unit
let set_x = Signal.define ~name:"setx" Codec.int
let status_q = Query.define ~name:"status" ~input:Codec.unit ~output:Codec.string
let deposit = Update.define ~name:"deposit" ~input:Codec.int ~output:Codec.int

(* timer: sleep starts a durable timer, FireTimer completes the run *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"TimerW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           Workflow.sleep ctx 5.;
           "done"))
  in
  let run_id = "wf-timer" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"TimerW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "timer: starts timer on init"
    (match c1 with
     | [ Coresdk.Start_timer { seq = 1; start_to_fire } ] -> start_to_fire = 5.
     | _ -> false);
  Replay_state.apply_job st (Coresdk.Fire_timer { seq = 1 });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "timer: completes on fire"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "done"
     | _ -> false)

(* signal + wait_condition: block until a signal flips the condition *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"ApproveW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let decided = ref false in
           Workflow.on_signal ctx approve (fun () -> decided := true);
           Workflow.wait_condition ctx (fun () -> !decided);
           "approved"))
  in
  let run_id = "wf-approve" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"ApproveW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "signal: blocks on wait_condition (no commands)" (c1 = []);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "approve"; input = [ unit_arg ] });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "signal: completes once the condition holds"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "approved"
     | _ -> false)

(* ADR-0002 history-ordered delivery: a signal sitting between two activity
   resolutions is delivered at the second activity's checkpoint, so synchronous code
   reading state between the two sees the pre-signal value. *)
let () =
  let act = Activity.define ~name:"a" ~input:Codec.unit ~output:Codec.string (fun () -> "a") in
  let wf =
    Workflow.reg
      (Workflow.define ~name:"OrderW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let x = ref 0 in
           Workflow.on_signal ctx set_x (fun v -> x := v);
           let _ = Workflow.execute_activity ctx act () in
           let seen = !x in
           let _ = Workflow.execute_activity ctx act () in
           Printf.sprintf "seen=%d final=%d" seen !x))
  in
  let run_id = "wf-order" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"OrderW" ~workflow_id:run_id [ unit_arg ]);
  let a_ok = Coresdk.Completed (Some (Codec.to_payload Codec.string "a")) in
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 1; result = a_ok });
  Replay_state.apply_job st
    (Coresdk.Signal_workflow { signal_name = "setx"; input = [ Codec.to_payload Codec.int 12 ] });
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 2; result = a_ok });
  let c = activation wf st ~run_id ~history_length:4 in
  check "signal ordering: delivered at the 2nd checkpoint, not before"
    (match c with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "seen=0 final=12"
     | _ -> false)

(* query mode: a query-only activation replays read-only and answers via the handler *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"QueryW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let decided = ref None in
           Workflow.on_signal ctx approve (fun () -> decided := Some "yes");
           Workflow.on_query ctx status_q (fun () ->
               match !decided with None -> "pending" | Some x -> x);
           Workflow.wait_condition ctx (fun () -> !decided <> None);
           Option.get !decided))
  in
  let run_id = "wf-query" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"QueryW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wf st ~run_id ~history_length:1 in
  let c =
    Replay.run_workflow wf st ~task_queue:"test-tq" ~run_id ~can_suggested:false
      ~history_length:1 ~query_mode:true ~queries:[ ("q1", "status", []) ] ~updates:[]
  in
  check "query: answers pending with no advancing commands"
    (match c with
     | [ Coresdk.Respond_to_query { query_id = "q1"; result = Coresdk.Query_succeeded p } ] ->
       Codec.of_payload Codec.string p = "pending"
     | _ -> false)

(* update: an accepted deposit returns the new balance; a validator rejects a
   non-positive one, mutating nothing *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"AcctW" ~input:Codec.int ~output:Codec.int
         (fun ctx opening ->
           let bal = ref opening in
           Workflow.on_update ctx deposit
             ~validate:(fun a -> if a <= 0 then failwith "must be positive")
             (fun a ->
               bal := !bal + a;
               !bal);
           Workflow.wait_condition ctx (fun () -> false);
           !bal))
  in
  let run_id = "wf-acct" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st
    (init_job ~workflow_type:"AcctW" ~workflow_id:run_id [ Codec.to_payload Codec.int 100 ]);
  let _ = activation wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st
    (Coresdk.Do_update
       { protocol_instance_id = "u1"; name = "deposit"; input = [ Codec.to_payload Codec.int 50 ];
         run_validator = true });
  let c1 =
    Replay.run_workflow wf st ~task_queue:"test-tq" ~run_id ~can_suggested:false
      ~history_length:2 ~query_mode:false ~queries:[] ~updates:[ ("u1", true) ]
  in
  check "update: accepted then completed with new balance"
    (match c1 with
     | [ Coresdk.Update_response { protocol_instance_id = "u1"; outcome = Coresdk.Update_accepted };
         Coresdk.Update_response { protocol_instance_id = "u1"; outcome = Coresdk.Update_completed p } ]
       -> Codec.of_payload Codec.int p = 150
     | _ -> false);
  Replay_state.apply_job st
    (Coresdk.Do_update
       { protocol_instance_id = "u2"; name = "deposit"; input = [ Codec.to_payload Codec.int (-5) ];
         run_validator = true });
  let c2 =
    Replay.run_workflow wf st ~task_queue:"test-tq" ~run_id ~can_suggested:false
      ~history_length:3 ~query_mode:false ~queries:[] ~updates:[ ("u2", true) ]
  in
  check "update: validator rejects a non-positive deposit"
    (match c2 with
     | [ Coresdk.Update_response { protocol_instance_id = "u2"; outcome = Coresdk.Update_rejected _ } ]
       -> true
     | _ -> false)

(* continue-as-new: a positive counter starts a fresh run with the decremented arg *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"CanW" ~input:Codec.int ~output:Codec.string
         (fun ctx n -> if n > 0 then Workflow.continue_as_new ctx (n - 1) else "done"))
  in
  let run_id = "wf-can" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"CanW" ~workflow_id:run_id [ Codec.to_payload Codec.int 1 ]);
  let c = activation wf st ~run_id ~history_length:1 in
  check "continue-as-new: emits Continue_as_new with the decremented arg"
    (match c with
     | [ Coresdk.Continue_as_new { arguments = [ p ] } ] -> Codec.of_payload Codec.int p = 0
     | _ -> false)

let echo_act = Activity.define ~name:"echo" ~input:Codec.string ~output:Codec.string (fun s -> s)

(* fan-out: two activities started eagerly, then await_all *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"FanW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let a = Workflow.start_activity ctx echo_act "A" in
           let b = Workflow.start_activity ctx echo_act "B" in
           String.concat "+" (Workflow.await_all ctx [ a; b ])))
  in
  let run_id = "wf-fan" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"FanW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "fan-out: schedules both activities eagerly, in order"
    (match c1 with
     | [ Coresdk.Schedule_activity { seq = 1; _ }; Coresdk.Schedule_activity { seq = 2; _ } ] ->
       true
     | _ -> false);
  let ok s = Coresdk.Completed (Some (Codec.to_payload Codec.string s)) in
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 1; result = ok "A" });
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 2; result = ok "B" });
  let c2 = activation wf st ~run_id ~history_length:3 in
  check "fan-out: completes with both results, nothing re-scheduled"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "A+B"
     | _ -> false)

(* emit-once: a signal arriving while an activity is outstanding must not re-schedule *)
let () =
  let ping = Signal.define ~name:"ping" Codec.unit in
  let wf =
    Workflow.reg
      (Workflow.define ~name:"OnceW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           Workflow.on_signal ctx ping (fun () -> ());
           Workflow.execute_activity ctx echo_act "x"))
  in
  let run_id = "wf-once" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"OnceW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "emit-once: schedules on init"
    (match c1 with [ Coresdk.Schedule_activity { seq = 1; _ } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "ping"; input = [ unit_arg ] });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "emit-once: no re-schedule while the activity is outstanding" (c2 = [])

(* await_any: the first future to resolve wins *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"RaceW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let a = Workflow.start_activity ctx echo_act "A" in
           let b = Workflow.start_activity ctx echo_act "B" in
           Workflow.await_any ctx [ a; b ]))
  in
  let run_id = "wf-race" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"RaceW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st
    (Coresdk.Resolve_activity
       { seq = 2; result = Coresdk.Completed (Some (Codec.to_payload Codec.string "B")) });
  let c = activation wf st ~run_id ~history_length:2 in
  check "await_any: the first (and only) to resolve wins"
    (match c with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "B"
     | _ -> false)

(* spawn: two independent fibers, each running an activity, awaited separately *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"SpawnW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let a = Workflow.spawn ctx (fun () -> Workflow.execute_activity ctx echo_act "A") in
           let b = Workflow.spawn ctx (fun () -> Workflow.execute_activity ctx echo_act "B") in
           let ra = Workflow.await ctx a in
           let rb = Workflow.await ctx b in
           ra ^ "+" ^ rb))
  in
  let run_id = "wf-spawn" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"SpawnW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "spawn: both fibers schedule their activities eagerly"
    (match c1 with
     | [ Coresdk.Schedule_activity { seq = 1; _ }; Coresdk.Schedule_activity { seq = 2; _ } ] ->
       true
     | _ -> false);
  let ok s = Coresdk.Completed (Some (Codec.to_payload Codec.string s)) in
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 1; result = ok "A" });
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 2; result = ok "B" });
  let c2 = activation wf st ~run_id ~history_length:3 in
  check "spawn: completes with both fibers' results"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "A+B"
     | _ -> false)

(* cancellation: canceling a scope emits the cancel command for an activity started
   under it. The cancel and the workflow's completion fall in different activations,
   so the Request_cancel is observable (a terminal command replaces the list). *)
let () =
  let go = Signal.define ~name:"go" Codec.unit in
  let finish = Signal.define ~name:"finish" Codec.unit in
  let wf =
    Workflow.reg
      (Workflow.define ~name:"CancelReqW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let started = ref false and done_ = ref false in
           Workflow.on_signal ctx go (fun () -> started := true);
           Workflow.on_signal ctx finish (fun () -> done_ := true);
           Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
               let _a = Workflow.start_activity ctx' echo_act "A" in
               Workflow.wait_condition ctx (fun () -> !started);
               cancel ();
               Workflow.wait_condition ctx (fun () -> !done_);
               "done")))
  in
  let run_id = "wf-cancel-req" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"CancelReqW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "cancel: schedules the activity, then blocks"
    (match c1 with [ Coresdk.Schedule_activity { seq = 1; _ } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "go"; input = [ unit_arg ] });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "cancel: canceling the scope requests activity cancellation"
    (match c2 with [ Coresdk.Request_cancel_activity { seq = 1 } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "finish"; input = [ unit_arg ] });
  let c3 = activation wf st ~run_id ~history_length:3 in
  check "cancel: completes once released"
    (match c3 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "done"
     | _ -> false)

(* cancellation: awaiting an operation whose scope is already cancelled raises Canceled
   at the await point. Canceled is an ordinary exception, so the body catches it and
   completes normally (a denied cancel). *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"CancelCatchW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
               let a = Workflow.start_activity ctx' echo_act "A" in
               cancel ();
               try Workflow.await ctx' a with Workflow.Canceled _ -> "denied")))
  in
  let run_id = "wf-cancel-catch" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st
    (init_job ~workflow_type:"CancelCatchW" ~workflow_id:run_id [ unit_arg ]);
  let c = activation wf st ~run_id ~history_length:1 in
  check "cancel: await under a cancelled scope raises Canceled, caught and denied"
    (match c with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "denied"
     | _ -> false)

(* cancellation: canceling one scope leaves a sibling scope's operation untouched. The
   activity started under the live scope is scheduled (not cancelled) and its await
   resolves normally. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"SiblingScopeW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let a =
             Workflow.with_cancel_scope ctx (fun ctx_a ~cancel:_ ->
                 Workflow.start_activity ctx_a echo_act "A")
           in
           Workflow.with_cancel_scope ctx (fun _ctx_b ~cancel -> cancel ());
           Workflow.await ctx a))
  in
  let run_id = "wf-sibling-scope" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st
    (init_job ~workflow_type:"SiblingScopeW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "cancel: a sibling scope's cancel emits nothing for the live scope's activity"
    (match c1 with [ Coresdk.Schedule_activity { seq = 1; _ } ] -> true | _ -> false);
  Replay_state.apply_job st
    (Coresdk.Resolve_activity
       { seq = 1; result = Coresdk.Completed (Some (Codec.to_payload Codec.string "A")) });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "cancel: the live scope's activity resolves normally"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "A"
     | _ -> false)

(* cancellation: is_cancel_requested reflects the scope's state before and after a
   cancel, for cooperative checks *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"CancelProbeW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
               let before = Workflow.is_cancel_requested ctx' in
               cancel ();
               let after = Workflow.is_cancel_requested ctx' in
               Printf.sprintf "before=%b after=%b" before after)))
  in
  let run_id = "wf-cancel-probe" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st
    (init_job ~workflow_type:"CancelProbeW" ~workflow_id:run_id [ unit_arg ]);
  let c = activation wf st ~run_id ~history_length:1 in
  check "cancel: is_cancel_requested flips false -> true across a cancel"
    (match c with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "before=false after=true"
     | _ -> false)

(* Phase 5: with_timeout, detached, wait_condition_timeout. A release signal parks the
   root after the interesting cancel, so the intermediate command is observable (a
   terminal command otherwise replaces the list). *)
let release_sig = Signal.define ~name:"release" Codec.unit
let go_sig = Signal.define ~name:"go" Codec.unit

(* with_timeout: one body-scope activity raced against a deadline timer. Deadline first
   cancels the activity; body first cancels the timer. Same workflow, two histories. *)
let timeout_wf =
  Workflow.reg
    (Workflow.define ~name:"TimeoutW" ~input:Codec.unit ~output:Codec.string
       (fun ctx () ->
         let released = ref false in
         Workflow.on_signal ctx release_sig (fun () -> released := true);
         let outcome =
           match
             Workflow.with_timeout ctx 30. (fun c -> Workflow.execute_activity c echo_act "A")
           with
           | r -> r
           | exception Workflow.Canceled _ -> "timed-out"
         in
         Workflow.wait_condition ctx (fun () -> !released);
         outcome))

let () =
  let run_id = "wf-timeout-fire" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"TimeoutW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation timeout_wf st ~run_id ~history_length:1 in
  check "with_timeout: schedules the body activity and a deadline timer"
    (match c1 with
     | [ Coresdk.Schedule_activity { seq = 1; _ };
         Coresdk.Start_timer { seq = 1; start_to_fire } ] ->
       start_to_fire = 30.
     | _ -> false);
  Replay_state.apply_job st (Coresdk.Fire_timer { seq = 1 });
  let c2 = activation timeout_wf st ~run_id ~history_length:2 in
  check "with_timeout: deadline fires, cancels the body activity"
    (match c2 with [ Coresdk.Request_cancel_activity { seq = 1 } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "release"; input = [ unit_arg ] });
  let c3 = activation timeout_wf st ~run_id ~history_length:3 in
  check "with_timeout: body observes Canceled from the deadline"
    (match c3 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "timed-out"
     | _ -> false)

let () =
  let run_id = "wf-timeout-ok" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"TimeoutW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation timeout_wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st
    (Coresdk.Resolve_activity
       { seq = 1; result = Coresdk.Completed (Some (Codec.to_payload Codec.string "A")) });
  let c2 = activation timeout_wf st ~run_id ~history_length:2 in
  check "with_timeout: body finishes first, cancels the deadline timer"
    (match c2 with [ Coresdk.Cancel_timer { seq = 1 } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "release"; input = [ unit_arg ] });
  let c3 = activation timeout_wf st ~run_id ~history_length:3 in
  check "with_timeout: returns the body result when it beats the deadline"
    (match c3 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "A"
     | _ -> false)

(* detached: an activity started in a detached child scope is not reached when an
   ancestor scope is cancelled, so its await resolves normally. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"DetachedW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let go = ref false in
           Workflow.on_signal ctx go_sig (fun () -> go := true);
           let d =
             Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
                 let d = Workflow.detached ctx' (fun cd -> Workflow.start_activity cd echo_act "D") in
                 Workflow.wait_condition ctx (fun () -> !go);
                 cancel ();
                 d)
           in
           Workflow.await ctx d))
  in
  let run_id = "wf-detached" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"DetachedW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "detached: schedules the detached activity"
    (match c1 with [ Coresdk.Schedule_activity { seq = 1; _ } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "go"; input = [ unit_arg ] });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "detached: canceling the ancestor scope emits nothing for the detached op" (c2 = []);
  Replay_state.apply_job st
    (Coresdk.Resolve_activity
       { seq = 1; result = Coresdk.Completed (Some (Codec.to_payload Codec.string "D")) });
  let c3 = activation wf st ~run_id ~history_length:3 in
  check "detached: the detached activity resolves normally, unaffected by the cancel"
    (match c3 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "D"
     | _ -> false)

(* wait_condition_timeout: predicate met before the deadline returns true and cancels
   the timer; the deadline firing first returns false. Same workflow, two histories. *)
let wct_wf =
  Workflow.reg
    (Workflow.define ~name:"WctW" ~input:Codec.unit ~output:Codec.string
       (fun ctx () ->
         let ok = ref false and released = ref false in
         Workflow.on_signal ctx go_sig (fun () -> ok := true);
         Workflow.on_signal ctx release_sig (fun () -> released := true);
         let met = Workflow.wait_condition_timeout ctx ~timeout:30. (fun () -> !ok) in
         Workflow.wait_condition ctx (fun () -> !released);
         Printf.sprintf "met=%b" met))

let () =
  let run_id = "wf-wct-met" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"WctW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wct_wf st ~run_id ~history_length:1 in
  check "wait_condition_timeout: starts the deadline timer"
    (match c1 with [ Coresdk.Start_timer { seq = 1; start_to_fire } ] -> start_to_fire = 30. | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "go"; input = [ unit_arg ] });
  let c2 = activation wct_wf st ~run_id ~history_length:2 in
  check "wait_condition_timeout: predicate met cancels the deadline timer"
    (match c2 with [ Coresdk.Cancel_timer { seq = 1 } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "release"; input = [ unit_arg ] });
  let c3 = activation wct_wf st ~run_id ~history_length:3 in
  check "wait_condition_timeout: returns true when the predicate held in time"
    (match c3 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "met=true"
     | _ -> false)

let () =
  let run_id = "wf-wct-timeout" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"WctW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wct_wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st (Coresdk.Fire_timer { seq = 1 });
  let c2 = activation wct_wf st ~run_id ~history_length:2 in
  check "wait_condition_timeout: deadline firing emits no further command" (c2 = []);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "release"; input = [ unit_arg ] });
  let c3 = activation wct_wf st ~run_id ~history_length:3 in
  check "wait_condition_timeout: returns false when the deadline fired first"
    (match c3 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "met=false"
     | _ -> false)

(* Phase 6: per-operation cancellation. Activity cancel types, timer local cancel,
   distinct Canceled on a server-cancelled resolution, and child wait_for_cancellation. *)

(* Abandon: the scheduled command carries cancellation_type=2, and cancelling the scope
   emits no cancel command (unlike Try_cancel). *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"AbandonW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let go = ref false and released = ref false in
           Workflow.on_signal ctx go_sig (fun () -> go := true);
           Workflow.on_signal ctx release_sig (fun () -> released := true);
           Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
               let _a =
                 Workflow.start_activity ~cancel_type:Workflow.Abandon ctx' echo_act "A"
               in
               Workflow.wait_condition ctx (fun () -> !go);
               cancel ();
               Workflow.wait_condition ctx (fun () -> !released);
               "done")))
  in
  let run_id = "wf-abandon" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"AbandonW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "abandon: schedules with cancellation_type=2 (ABANDON)"
    (match c1 with
     | [ Coresdk.Schedule_activity { seq = 1; cancellation_type = 2; _ } ] -> true
     | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "go"; input = [ unit_arg ] });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "abandon: canceling the scope emits no cancel command" (c2 = [])

(* Wait_cancellation_completed: cancelling requests activity cancellation but does not
   raise at once; the awaiter waits for the server's resolution. A Cancelled resolution
   then raises Canceled; a Completed one returns the real result. *)
let wcc_wf =
  Workflow.reg
    (Workflow.define ~name:"WccW" ~input:Codec.unit ~output:Codec.string
       (fun ctx () ->
         Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
             let a =
               Workflow.start_activity ~cancel_type:Workflow.Wait_cancellation_completed
                 ctx' echo_act "A"
             in
             cancel ();
             try Workflow.await ctx' a with Workflow.Canceled _ -> "cancelled")))

let () =
  let run_id = "wf-wcc-cancelled" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"WccW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wcc_wf st ~run_id ~history_length:1 in
  check "wait-cancellation-completed: requests cancellation but keeps waiting"
    (match c1 with
     | [ Coresdk.Schedule_activity { seq = 1; cancellation_type = 1; _ };
         Coresdk.Request_cancel_activity { seq = 1 } ] ->
       true
     | _ -> false);
  Replay_state.apply_job st
    (Coresdk.Resolve_activity { seq = 1; result = Coresdk.Cancelled "stopped" });
  let c2 = activation wcc_wf st ~run_id ~history_length:2 in
  check "wait-cancellation-completed: a Cancelled resolution raises Canceled"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "cancelled"
     | _ -> false)

let () =
  let run_id = "wf-wcc-ignored" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"WccW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wcc_wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st
    (Coresdk.Resolve_activity
       { seq = 1; result = Coresdk.Completed (Some (Codec.to_payload Codec.string "A")) });
  let c2 = activation wcc_wf st ~run_id ~history_length:2 in
  check "wait-cancellation-completed: an ignored cancel returns the real result"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "A"
     | _ -> false)

(* timer local cancel: cancelling a scope with an awaited timer emits CancelTimer and
   raises Canceled in the body awaiting it. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"TimerCancelW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let go = ref false and released = ref false in
           Workflow.on_signal ctx go_sig (fun () -> go := true);
           Workflow.on_signal ctx release_sig (fun () -> released := true);
           let outcome =
             Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
                 let _ =
                   Workflow.spawn ctx (fun () ->
                       Workflow.wait_condition ctx (fun () -> !go);
                       cancel ())
                 in
                 try
                   Workflow.sleep ctx' 60.;
                   "slept"
                 with Workflow.Canceled _ -> "timer-cancelled")
           in
           Workflow.wait_condition ctx (fun () -> !released);
           outcome))
  in
  let run_id = "wf-timer-cancel" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"TimerCancelW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "timer cancel: starts the timer"
    (match c1 with [ Coresdk.Start_timer { seq = 1; _ } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "go"; input = [ unit_arg ] });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "timer cancel: canceling the scope cancels the timer"
    (match c2 with [ Coresdk.Cancel_timer { seq = 1 } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Signal_workflow { signal_name = "release"; input = [ unit_arg ] });
  let c3 = activation wf st ~run_id ~history_length:3 in
  check "timer cancel: the body observes Canceled from the cancelled timer"
    (match c3 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "timer-cancelled"
     | _ -> false)

(* server-cancelled activity: a ResolveActivity carrying Cancelled raises Canceled at
   the await, distinct from a generic failure. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"ActCancelledW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           try
             let _ = Workflow.execute_activity ctx echo_act "A" in
             "completed"
           with
           | Workflow.Canceled _ -> "canceled"
           | Failure _ -> "failed"))
  in
  let run_id = "wf-act-cancelled" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"ActCancelledW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st
    (Coresdk.Resolve_activity { seq = 1; result = Coresdk.Cancelled "server stopped it" });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "server-cancelled activity: raises Canceled, not a generic failure"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "canceled"
     | _ -> false)

let child_wf =
  Workflow.define ~name:"ChildW" ~input:Codec.string ~output:Codec.string (fun _ s -> s)

(* server-cancelled child: a ResolveChildWorkflowExecution carrying Cancelled raises a
   distinct Canceled at the await. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"ChildCancelledW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           try
             let _ = Workflow.execute_child_workflow ctx child_wf "x" in
             "completed"
           with
           | Workflow.Canceled _ -> "canceled"
           | Failure _ -> "failed"))
  in
  let run_id = "wf-child-cancelled" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"ChildCancelledW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st
    (Coresdk.Resolve_child_workflow_execution_start
       { seq = 1; outcome = Coresdk.Child_start_succeeded "run-x" });
  Replay_state.apply_job st
    (Coresdk.Resolve_child_workflow_execution { seq = 1; result = Coresdk.Child_cancelled "stopped" });
  let c2 = activation wf st ~run_id ~history_length:3 in
  check "server-cancelled child: raises Canceled, not a generic failure"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "canceled"
     | _ -> false)

(* child wait_for_cancellation: cancelling requests the child's cancellation but does
   not raise until the child actually resolves. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"ChildWaitCancelW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
               let c =
                 Workflow.start_child_workflow ~wait_for_cancellation:true ctx' child_wf "x"
               in
               cancel ();
               try
                 let _ = Workflow.await ctx' c in
                 "completed"
               with Workflow.Canceled _ -> "child-cancelled")))
  in
  let run_id = "wf-child-wait-cancel" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st
    (init_job ~workflow_type:"ChildWaitCancelW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "child wait_for_cancellation: requests cancellation but keeps waiting"
    (match c1 with
     | [ Coresdk.Start_child_workflow_execution { seq = 1; _ };
         Coresdk.Cancel_child_workflow_execution { child_workflow_seq = 1; _ } ] ->
       true
     | _ -> false);
  Replay_state.apply_job st
    (Coresdk.Resolve_child_workflow_execution_start
       { seq = 1; outcome = Coresdk.Child_start_succeeded "run-x" });
  Replay_state.apply_job st
    (Coresdk.Resolve_child_workflow_execution { seq = 1; result = Coresdk.Child_cancelled "stopped" });
  let c2 = activation wf st ~run_id ~history_length:3 in
  check "child wait_for_cancellation: raises Canceled once the child resolves"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "child-cancelled"
     | _ -> false)

(* Phase 7: workflow-level cancellation. A CancelWorkflow job is delivered as an
   order-sensitive history event that cancels the root scope. *)

(* cancel while blocked on an activity: the body does not catch Canceled, so it escapes
   the main body and the workflow closes as CancelWorkflowExecution. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"CancelBlockedW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () -> Workflow.execute_activity ctx echo_act "A"))
  in
  let run_id = "wf-cancel-blocked" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"CancelBlockedW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "workflow cancel: schedules the activity"
    (match c1 with [ Coresdk.Schedule_activity { seq = 1; _ } ] -> true | _ -> false);
  Replay_state.apply_job st (Coresdk.Cancel_workflow { reason = "" });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "workflow cancel: uncaught Canceled closes as CancelWorkflowExecution"
    (match c2 with [ Coresdk.Cancel_workflow_execution ] -> true | _ -> false)

(* cancel while blocked on an activity, caught: the body may catch Canceled and
   complete normally, denying the cancel. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"CancelDenyW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           try Workflow.execute_activity ctx echo_act "A"
           with Workflow.Canceled _ -> "denied"))
  in
  let run_id = "wf-cancel-deny" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"CancelDenyW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st (Coresdk.Cancel_workflow { reason = "" });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "workflow cancel: the body may catch Canceled and complete normally"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "denied"
     | _ -> false)

(* cancel while blocked on a wait_condition: the wait is interrupted with Canceled
   rather than hanging (the signal that would release it is never coming). *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"CancelWaitCondW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let approved = ref false in
           Workflow.on_signal ctx approve (fun () -> approved := true);
           Workflow.wait_condition ctx (fun () -> !approved);
           "approved"))
  in
  let run_id = "wf-cancel-waitcond" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"CancelWaitCondW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "workflow cancel: blocks on wait_condition (no commands)" (c1 = []);
  Replay_state.apply_job st (Coresdk.Cancel_workflow { reason = "" });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "workflow cancel: interrupts a wait_condition rather than hanging"
    (match c2 with [ Coresdk.Cancel_workflow_execution ] -> true | _ -> false)

(* order-sensitivity: a cancel ordered before an activity resolution preempts it, so
   the (caught) Canceled wins and the resolution is ignored. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"CancelOrderW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           try Workflow.execute_activity ctx echo_act "A"
           with Workflow.Canceled _ -> "canceled"))
  in
  let run_id = "wf-cancel-order" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"CancelOrderW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st (Coresdk.Cancel_workflow { reason = "" });
  Replay_state.apply_job st
    (Coresdk.Resolve_activity
       { seq = 1; result = Coresdk.Completed (Some (Codec.to_payload Codec.string "A")) });
  let c2 = activation wf st ~run_id ~history_length:3 in
  check "workflow cancel: a cancel ordered before a resolution preempts it"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "canceled"
     | _ -> false)

(* await_any cancellation: a scope cancel interrupts a fiber parked in await_any,
   raising Canceled at the await point. Uncaught, it closes the run as canceled. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"RaceCancelW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let a = Workflow.start_activity ctx echo_act "A" in
           let b = Workflow.start_activity ctx echo_act "B" in
           Workflow.await_any ctx [ a; b ]))
  in
  let run_id = "wf-race-cancel" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"RaceCancelW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "await_any cancel: schedules both raced activities"
    (match c1 with
     | [ Coresdk.Schedule_activity { seq = 1; _ }; Coresdk.Schedule_activity { seq = 2; _ } ] -> true
     | _ -> false);
  Replay_state.apply_job st (Coresdk.Cancel_workflow { reason = "" });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "await_any cancel: an interrupted await_any escapes as CancelWorkflowExecution"
    (match c2 with [ Coresdk.Cancel_workflow_execution ] -> true | _ -> false)

(* await_any cancellation, caught: the body may catch the Canceled and complete. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"RaceCancelCatchW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let a = Workflow.start_activity ctx echo_act "A" in
           let b = Workflow.start_activity ctx echo_act "B" in
           try Workflow.await_any ctx [ a; b ] with Workflow.Canceled _ -> "raced-canceled"))
  in
  let run_id = "wf-race-cancel-catch" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st
    (init_job ~workflow_type:"RaceCancelCatchW" ~workflow_id:run_id [ unit_arg ]);
  let _ = activation wf st ~run_id ~history_length:1 in
  Replay_state.apply_job st (Coresdk.Cancel_workflow { reason = "" });
  let c2 = activation wf st ~run_id ~history_length:2 in
  check "await_any cancel: the body may catch Canceled from an interrupted await_any"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "raced-canceled"
     | _ -> false)

(* await_any under an already-cancelled scope raises Canceled at once (the upfront
   check, mirroring a single await). *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"RacePreCancelW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           Workflow.with_cancel_scope ctx (fun ctx' ~cancel ->
               let a = Workflow.start_activity ctx' echo_act "A" in
               let b = Workflow.start_activity ctx' echo_act "B" in
               cancel ();
               try Workflow.await_any ctx' [ a; b ] with Workflow.Canceled _ -> "pre-canceled")))
  in
  let run_id = "wf-race-precancel" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"RacePreCancelW" ~workflow_id:run_id [ unit_arg ]);
  let c = activation wf st ~run_id ~history_length:1 in
  check "await_any cancel: awaiting under an already-cancelled scope raises Canceled"
    (match c with
     | [ Coresdk.Complete_workflow_execution (Some p) ] ->
       Codec.of_payload Codec.string p = "pre-canceled"
     | _ -> false)

(* out-of-order fan-in: await_all awaits its futures in order, but the server may
   resolve them in any order. A resolution that arrives before its await is buffered,
   not dropped, so the workflow still completes. Here the three activities resolve in
   reverse (seq 3, 2, 1) while the body awaits seq 1 first. *)
let () =
  let wf =
    Workflow.reg
      (Workflow.define ~name:"FanReverseW" ~input:Codec.unit ~output:Codec.string
         (fun ctx () ->
           let a = Workflow.start_activity ctx echo_act "A" in
           let b = Workflow.start_activity ctx echo_act "B" in
           let c = Workflow.start_activity ctx echo_act "C" in
           String.concat "+" (Workflow.await_all ctx [ a; b; c ])))
  in
  let run_id = "wf-fan-reverse" in
  let st = Replay_state.get_run run_id in
  Replay_state.apply_job st (init_job ~workflow_type:"FanReverseW" ~workflow_id:run_id [ unit_arg ]);
  let c1 = activation wf st ~run_id ~history_length:1 in
  check "out-of-order fan-in: schedules all three eagerly"
    (match c1 with
     | [ Coresdk.Schedule_activity { seq = 1; _ };
         Coresdk.Schedule_activity { seq = 2; _ };
         Coresdk.Schedule_activity { seq = 3; _ } ] ->
       true
     | _ -> false);
  let ok s = Coresdk.Completed (Some (Codec.to_payload Codec.string s)) in
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 3; result = ok "C" });
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 2; result = ok "B" });
  Replay_state.apply_job st (Coresdk.Resolve_activity { seq = 1; result = ok "A" });
  let c2 = activation wf st ~run_id ~history_length:4 in
  check "out-of-order fan-in: completes despite reverse resolution order"
    (match c2 with
     | [ Coresdk.Complete_workflow_execution (Some p) ] -> Codec.of_payload Codec.string p = "A+B+C"
     | _ -> false)

let () =
  if !failures > 0 then (
    Printf.printf "%d replay test(s) failed\n" !failures;
    exit 1)
  else Printf.printf "all replay tests passed\n"
