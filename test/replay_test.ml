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

let () =
  if !failures > 0 then (
    Printf.printf "%d replay test(s) failed\n" !failures;
    exit 1)
  else Printf.printf "all replay tests passed\n"
