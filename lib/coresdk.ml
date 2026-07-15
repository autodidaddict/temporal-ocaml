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

(* The sdk-core message layer: decode the activations the server sends us, encode
   the commands/completions we send back. Pb-based, no codegen. Field numbers are
   from temporal/sdk/core/*.proto and temporal/api/*.

   Only the jobs/commands the worker needs are modelled; everything else decodes
   to [Other] and is skipped. *)

type payload = Codec.payload

(* ---- decode: WorkflowActivation --------------------------------------- *)

type activity_resolution =
  | Completed of payload option (* ActivityResolution.completed = Success.result *)
  | Failed of string (* ActivityResolution.failed = Failure.message *)
  | Cancelled of string (* ActivityResolution.cancelled = Cancellation.failure *)
  | Other_resolution

(* Outcome of a child's first resolution (it was created, or could not be). *)
type child_start_outcome =
  | Child_start_succeeded of string (* the child's run_id *)
  | Child_start_failed of string (* e.g. the workflow id already exists *)
  | Child_start_cancelled of string
  | Child_start_other

(* Outcome of a child's completion resolution (child_workflow.ChildWorkflowResult). *)
type child_result =
  | Child_completed of payload option
  | Child_failed of string
  | Child_cancelled of string
  | Child_result_other

type wf_job =
  | Initialize_workflow of {
      workflow_type : string;
      workflow_id : string;
      arguments : payload list;
    }
  | Resolve_activity of { seq : int; result : activity_resolution }
  | Fire_timer of { seq : int }
  | Signal_workflow of { signal_name : string; input : payload list }
  | Query_workflow of {
      query_id : string; (* correlates the RespondToQuery we send back *)
      query_type : string; (* the query name; routes to a handler *)
      arguments : payload list;
    }
  | Do_update of {
      protocol_instance_id : string; (* correlates the UpdateResponse(s) we send *)
      name : string; (* the update handler name *)
      input : payload list;
      run_validator : bool; (* true on first delivery; false on replay *)
    }
  | Resolve_child_workflow_execution_start of {
      seq : int; (* matches the StartChildWorkflowExecution command's seq *)
      outcome : child_start_outcome;
    }
  | Resolve_child_workflow_execution of { seq : int; result : child_result }
  | Cancel_workflow of { reason : string }
  | Remove_from_cache
  | Other

type wf_activation = {
  run_id : string;
  jobs : wf_job list;
  continue_as_new_suggested : bool;
  history_length : int;
}

let decode_payloads_field r acc = Codec.decode_payload (Pb.Reader.bytes r) :: acc

(* temporal.api.failure.v1.Failure { message=1; cause=4 }. An activity failure
   arrives wrapped: the outer message is a generic "Activity task failed" and the
   user's real error sits in the cause chain, so return the deepest message. *)
let rec failure_message s =
  let r = Pb.Reader.create s in
  let msg = ref "" and cause = ref None in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> msg := Pb.Reader.bytes r
    | 4, 2 -> cause := Some (Pb.Reader.bytes r)
    | _, w -> Pb.Reader.skip r w
  done;
  match !cause with
  | Some c -> ( match failure_message c with "" -> !msg | deeper -> deeper)
  | None -> !msg

(* activity_result.ActivityResolution { oneof { Success completed=1; Failure failed=2 } }
   activity_result.Success { Payload result = 1 } *)
let decode_activity_resolution s =
  let r = Pb.Reader.create s in
  let res = ref Other_resolution in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 ->
      let succ = Pb.Reader.create (Pb.Reader.bytes r) in
      let p = ref None in
      while not (Pb.Reader.at_end succ) do
        match Pb.Reader.key succ with
        | 1, 2 -> p := Some (Codec.decode_payload (Pb.Reader.bytes succ))
        | _, w -> Pb.Reader.skip succ w
      done;
      res := Completed !p
    | 2, 2 ->
      (* activity_result.Failure { failure=1 } wraps a temporal...Failure; the
         user's error is in its cause chain, so take the deepest message. *)
      let wrapper = Pb.Reader.create (Pb.Reader.bytes r) in
      let msg = ref "" in
      while not (Pb.Reader.at_end wrapper) do
        match Pb.Reader.key wrapper with
        | 1, 2 -> msg := failure_message (Pb.Reader.bytes wrapper)
        | _, w -> Pb.Reader.skip wrapper w
      done;
      res := Failed !msg
    | 3, 2 ->
      (* ActivityResolution.cancelled = Cancellation { failure=1 }, wrapping a
         temporal...Failure (CanceledFailureInfo); take the deepest message. *)
      let c = Pb.Reader.create (Pb.Reader.bytes r) in
      let msg = ref "" in
      while not (Pb.Reader.at_end c) do
        match Pb.Reader.key c with
        | 1, 2 -> msg := failure_message (Pb.Reader.bytes c)
        | _, w -> Pb.Reader.skip c w
      done;
      res := Cancelled !msg
    | _, w -> Pb.Reader.skip r w
  done;
  !res

(* InitializeWorkflow { workflow_type=1; workflow_id=2; arguments=3 (repeated
   Payload) } *)
let decode_initialize s =
  let r = Pb.Reader.create s in
  let wt = ref "" and wid = ref "" and args = ref [] in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> wt := Pb.Reader.bytes r
    | 2, 2 -> wid := Pb.Reader.bytes r
    | 3, 2 -> args := decode_payloads_field r !args
    | _, w -> Pb.Reader.skip r w
  done;
  Initialize_workflow
    { workflow_type = !wt; workflow_id = !wid; arguments = List.rev !args }

(* ResolveActivity { seq=1 (uint32); result=2 (ActivityResolution) } *)
let decode_resolve_activity s =
  let r = Pb.Reader.create s in
  let seq = ref 0 and result = ref Other_resolution in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 0 -> seq := Pb.Reader.varint r
    | 2, 2 -> result := decode_activity_resolution (Pb.Reader.bytes r)
    | _, w -> Pb.Reader.skip r w
  done;
  Resolve_activity { seq = !seq; result = !result }

(* FireTimer { seq=1 (uint32) } *)
let decode_fire_timer s =
  let r = Pb.Reader.create s in
  let seq = ref 0 in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 0 -> seq := Pb.Reader.varint r
    | _, w -> Pb.Reader.skip r w
  done;
  Fire_timer { seq = !seq }

(* SignalWorkflow { signal_name=1; input=2 (repeated Payload) } *)
let decode_signal_workflow s =
  let r = Pb.Reader.create s in
  let name = ref "" and input = ref [] in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> name := Pb.Reader.bytes r
    | 2, 2 -> input := decode_payloads_field r !input
    | _, w -> Pb.Reader.skip r w
  done;
  Signal_workflow { signal_name = !name; input = List.rev !input }

(* QueryWorkflow { query_id=1; query_type=2; arguments=3 (repeated Payload) }.
   query_id correlates the RespondToQuery command we send back; query_type is the
   query name the workflow body routes to a registered handler. *)
let decode_query_workflow s =
  let r = Pb.Reader.create s in
  let query_id = ref "" and query_type = ref "" and args = ref [] in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> query_id := Pb.Reader.bytes r
    | 2, 2 -> query_type := Pb.Reader.bytes r
    | 3, 2 -> args := decode_payloads_field r !args
    | _, w -> Pb.Reader.skip r w
  done;
  Query_workflow
    { query_id = !query_id; query_type = !query_type; arguments = List.rev !args }

(* DoUpdate { id=1; protocol_instance_id=2; name=3; input=4 (repeated Payload);
   run_validator=7 (bool) }. protocol_instance_id correlates the UpdateResponse(s)
   we send; run_validator is true only on first delivery (core doesn't re-run the
   validator on replay). id and headers/meta are not needed by the worker. *)
let decode_do_update s =
  let r = Pb.Reader.create s in
  let pid = ref "" and name = ref "" and input = ref [] and run_validator = ref false in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 2, 2 -> pid := Pb.Reader.bytes r
    | 3, 2 -> name := Pb.Reader.bytes r
    | 4, 2 -> input := decode_payloads_field r !input
    | 7, 0 -> run_validator := Pb.Reader.varint r <> 0
    | _, w -> Pb.Reader.skip r w
  done;
  Do_update
    { protocol_instance_id = !pid;
      name = !name;
      input = List.rev !input;
      run_validator = !run_validator;
    }

(* ResolveChildWorkflowExecutionStart { seq=1; oneof {
     ResolveChildWorkflowExecutionStartSuccess succeeded=2 { run_id=1 };
     ResolveChildWorkflowExecutionStartFailure failed=3 { workflow_id=1;
       workflow_type=2; StartChildWorkflowExecutionFailedCause cause=3 };
     ResolveChildWorkflowExecutionStartCancelled cancelled=4 { failure=1 } } } *)
let decode_resolve_child_start s =
  let r = Pb.Reader.create s in
  let seq = ref 0 and outcome = ref Child_start_other in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 0 -> seq := Pb.Reader.varint r
    | 2, 2 ->
      let succ = Pb.Reader.create (Pb.Reader.bytes r) in
      let run_id = ref "" in
      while not (Pb.Reader.at_end succ) do
        match Pb.Reader.key succ with
        | 1, 2 -> run_id := Pb.Reader.bytes succ
        | _, w -> Pb.Reader.skip succ w
      done;
      outcome := Child_start_succeeded !run_id
    | 3, 2 ->
      let f = Pb.Reader.create (Pb.Reader.bytes r) in
      let wid = ref "" and cause = ref 0 in
      while not (Pb.Reader.at_end f) do
        match Pb.Reader.key f with
        | 1, 2 -> wid := Pb.Reader.bytes f
        | 3, 0 -> cause := Pb.Reader.varint f
        | _, w -> Pb.Reader.skip f w
      done;
      (* cause 1 = WORKFLOW_ALREADY_EXISTS (the only defined cause) *)
      outcome :=
        Child_start_failed
          (if !cause = 1 then Printf.sprintf "child workflow id %S already exists" !wid
           else Printf.sprintf "child workflow start failed (cause %d)" !cause)
    | 4, 2 ->
      let c = Pb.Reader.create (Pb.Reader.bytes r) in
      let msg = ref "" in
      while not (Pb.Reader.at_end c) do
        match Pb.Reader.key c with
        | 1, 2 -> msg := failure_message (Pb.Reader.bytes c)
        | _, w -> Pb.Reader.skip c w
      done;
      outcome := Child_start_cancelled !msg
    | _, w -> Pb.Reader.skip r w
  done;
  Resolve_child_workflow_execution_start { seq = !seq; outcome = !outcome }

(* child_workflow.ChildWorkflowResult { oneof { Success completed=1 { result=1 };
   Failure failed=2 { failure=1 }; Cancellation cancelled=3 { failure=1 } } } *)
let decode_child_result s =
  let r = Pb.Reader.create s in
  let res = ref Child_result_other in
  let deepest_failure b =
    let f = Pb.Reader.create b in
    let msg = ref "" in
    while not (Pb.Reader.at_end f) do
      match Pb.Reader.key f with
      | 1, 2 -> msg := failure_message (Pb.Reader.bytes f)
      | _, w -> Pb.Reader.skip f w
    done;
    !msg
  in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 ->
      let succ = Pb.Reader.create (Pb.Reader.bytes r) in
      let p = ref None in
      while not (Pb.Reader.at_end succ) do
        match Pb.Reader.key succ with
        | 1, 2 -> p := Some (Codec.decode_payload (Pb.Reader.bytes succ))
        | _, w -> Pb.Reader.skip succ w
      done;
      res := Child_completed !p
    | 2, 2 -> res := Child_failed (deepest_failure (Pb.Reader.bytes r))
    | 3, 2 -> res := Child_cancelled (deepest_failure (Pb.Reader.bytes r))
    | _, w -> Pb.Reader.skip r w
  done;
  !res

(* ResolveChildWorkflowExecution { seq=1; ChildWorkflowResult result=2 } *)
let decode_resolve_child s =
  let r = Pb.Reader.create s in
  let seq = ref 0 and result = ref Child_result_other in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 0 -> seq := Pb.Reader.varint r
    | 2, 2 -> result := decode_child_result (Pb.Reader.bytes r)
    | _, w -> Pb.Reader.skip r w
  done;
  Resolve_child_workflow_execution { seq = !seq; result = !result }

(* CancelWorkflow { reason=1 (string); details omitted } *)
let decode_cancel_workflow s =
  let r = Pb.Reader.create s in
  let reason = ref "" in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> reason := Pb.Reader.bytes r
    | _, w -> Pb.Reader.skip r w
  done;
  Cancel_workflow { reason = !reason }

(* WorkflowActivationJob { oneof { initialize_workflow=1; fire_timer=2;
   query_workflow=5; cancel_workflow=6; signal_workflow=7; resolve_activity=8;
   resolve_child_workflow_execution_start=10; resolve_child_workflow_execution=11;
   do_update=14; remove_from_cache=50 } } *)
let decode_wf_job s =
  let r = Pb.Reader.create s in
  let job = ref Other in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> job := decode_initialize (Pb.Reader.bytes r)
    | 2, 2 -> job := decode_fire_timer (Pb.Reader.bytes r)
    | 5, 2 -> job := decode_query_workflow (Pb.Reader.bytes r)
    | 6, 2 -> job := decode_cancel_workflow (Pb.Reader.bytes r)
    | 7, 2 -> job := decode_signal_workflow (Pb.Reader.bytes r)
    | 8, 2 -> job := decode_resolve_activity (Pb.Reader.bytes r)
    | 10, 2 -> job := decode_resolve_child_start (Pb.Reader.bytes r)
    | 11, 2 -> job := decode_resolve_child (Pb.Reader.bytes r)
    | 14, 2 -> job := decode_do_update (Pb.Reader.bytes r)
    | 50, 2 ->
      ignore (Pb.Reader.bytes r);
      job := Remove_from_cache (* remove_from_cache = 50 *)
    | _, w -> Pb.Reader.skip r w
  done;
  !job

(* WorkflowActivation { run_id=1; history_length=4 (uint32); jobs=5 (repeated);
   continue_as_new_suggested=8 (bool) } *)
let decode_wf_activation s =
  let r = Pb.Reader.create s in
  let run_id = ref "" and jobs = ref [] in
  let history_length = ref 0 and can_suggested = ref false in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> run_id := Pb.Reader.bytes r
    | 4, 0 -> history_length := Pb.Reader.varint r
    | 5, 2 -> jobs := decode_wf_job (Pb.Reader.bytes r) :: !jobs
    | 8, 0 -> can_suggested := Pb.Reader.varint r <> 0
    | _, w -> Pb.Reader.skip r w
  done;
  { run_id = !run_id;
    jobs = List.rev !jobs;
    continue_as_new_suggested = !can_suggested;
    history_length = !history_length;
  }

(* ---- encode: WorkflowActivationCompletion ----------------------------- *)

(* A query answer: the encoded handler result, or a failure message when the
   handler raised or no handler was registered for the query type. *)
type query_result = Query_succeeded of payload | Query_failed of string

(* One UpdateResponse. A successful update emits two of these in order — accepted,
   then completed; a validator rejection (or a handler that raised after
   acceptance) emits a rejected. *)
type update_outcome =
  | Update_accepted (* validator passed / not asked to validate *)
  | Update_rejected of string (* validator failed, or handler raised *)
  | Update_completed of payload (* handler result *)

type wf_command =
  | Schedule_activity of {
      seq : int;
      activity_id : string;
      activity_type : string;
      task_queue : string;
      arguments : payload list;
      start_to_close : float; (* seconds *)
      max_attempts : int; (* 0 = unlimited (server default); 1 disables retries *)
      cancellation_type : int; (* ActivityCancellationType: 0=TRY_CANCEL 1=WAIT_CANCELLATION_COMPLETED 2=ABANDON *)
    }
  | Start_timer of { seq : int; start_to_fire : float (* seconds *) }
  | Complete_workflow_execution of payload option
  | Fail_workflow_execution of string
  | Continue_as_new of { arguments : payload list }
  | Respond_to_query of { query_id : string; result : query_result }
  | Update_response of { protocol_instance_id : string; outcome : update_outcome }
  | Start_child_workflow_execution of {
      seq : int;
      namespace : string; (* "" to let core default to the parent's namespace *)
      workflow_id : string;
      workflow_type : string;
      task_queue : string;
      input : payload list;
      execution_timeout : float; (* seconds; 0. to omit *)
      run_timeout : float; (* seconds; 0. to omit *)
      parent_close_policy : int; (* ParentClosePolicy: 1=TERMINATE 2=ABANDON 3=REQUEST_CANCEL *)
    }
  | Request_cancel_activity of { seq : int } (* cancel a scheduled activity by its seq *)
  | Cancel_timer of { seq : int } (* cancel a started timer by its seq *)
  | Cancel_workflow_execution (* close this workflow as canceled *)
  | Cancel_child_workflow_execution of {
      child_workflow_seq : int; (* the StartChildWorkflowExecution seq *)
      reason : string;
    }

(* google.protobuf.Duration { int64 seconds=1; int32 nanos=2 } *)
let encode_duration seconds =
  let secs = int_of_float seconds in
  let nanos = int_of_float ((seconds -. float_of_int secs) *. 1e9) in
  let w = Pb.Writer.create () in
  if secs <> 0 then Pb.Writer.int w 1 secs;
  if nanos <> 0 then Pb.Writer.int w 2 nanos;
  Pb.Writer.contents w

let encode_payload_field w field p = Pb.Writer.bytes w field (Codec.encode_payload p)

(* temporal.api.failure.v1.Failure { message=1; source=2; application_failure_info=5 }.
   The application_failure_info { type=1 } marks it an application failure so core
   can evaluate activity retryability — without it, a failed activity never
   reaches the server and falls through to a start_to_close timeout. Richer
   structured failures (cause chains, payload details) are future work per
   ADR-0001. *)
let encode_failure msg =
  let info = Pb.Writer.create () in
  Pb.Writer.bytes info 1 "ApplicationFailure" (* ApplicationFailureInfo.type = 1 *);
  let w = Pb.Writer.create () in
  Pb.Writer.bytes w 1 msg;
  Pb.Writer.bytes w 2 "OCamlSDK";
  Pb.Writer.bytes w 5 (Pb.Writer.contents info) (* Failure.application_failure_info = 5 *);
  Pb.Writer.contents w

(* temporal.api.common.v1.RetryPolicy { maximum_attempts=4 (int32) }.
   maximum_attempts: 1 disables retries; 0 (default) is unlimited (bounded by
   timeouts). Emitted only when the caller set a cap. *)
let encode_retry_policy max_attempts =
  let w = Pb.Writer.create () in
  Pb.Writer.int w 4 max_attempts;
  Pb.Writer.contents w

(* -> a single WorkflowCommand *)
let encode_command = function
  | Complete_workflow_execution result ->
    let cwe = Pb.Writer.create () in
    (match result with Some p -> encode_payload_field cwe 1 p | None -> ());
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 6 (Pb.Writer.contents cwe);
    (* WorkflowCommand.complete_workflow_execution = 6 *)
    Pb.Writer.contents cmd
  | Schedule_activity a ->
    let sa = Pb.Writer.create () in
    Pb.Writer.int sa 1 a.seq;
    Pb.Writer.bytes sa 2 a.activity_id;
    Pb.Writer.bytes sa 3 a.activity_type;
    Pb.Writer.bytes sa 5 a.task_queue;
    List.iter (encode_payload_field sa 7) a.arguments;
    Pb.Writer.bytes sa 10 (encode_duration a.start_to_close);
    if a.max_attempts > 0 then
      Pb.Writer.bytes sa 12 (encode_retry_policy a.max_attempts);
    (* omitted when TRY_CANCEL (0), which is core's default, keeping the common case
       byte-identical to before the field existed *)
    if a.cancellation_type <> 0 then Pb.Writer.int sa 13 a.cancellation_type;
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 2 (Pb.Writer.contents sa);
    (* WorkflowCommand.schedule_activity = 2 *)
    Pb.Writer.contents cmd
  | Start_timer tm ->
    let st = Pb.Writer.create () in
    Pb.Writer.int st 1 tm.seq;
    Pb.Writer.bytes st 2 (encode_duration tm.start_to_fire);
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 1 (Pb.Writer.contents st);
    (* WorkflowCommand.start_timer = 1 *)
    Pb.Writer.contents cmd
  | Fail_workflow_execution msg ->
    let fwe = Pb.Writer.create () in
    Pb.Writer.bytes fwe 1 (encode_failure msg);
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 7 (Pb.Writer.contents fwe);
    (* WorkflowCommand.fail_workflow_execution = 7 *)
    Pb.Writer.contents cmd
  | Continue_as_new c ->
    let can = Pb.Writer.create () in
    List.iter (encode_payload_field can 3) c.arguments;
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 8 (Pb.Writer.contents can);
    (* WorkflowCommand.continue_as_new_workflow_execution = 8;
       ContinueAsNewWorkflowExecution.arguments = 3 *)
    Pb.Writer.contents cmd
  | Respond_to_query { query_id; result } ->
    (* QueryResult { query_id=1;
         oneof { QuerySuccess succeeded=2 { Payload response=1 };
                 temporal.api.failure.v1.Failure failed=3 } } *)
    let qr = Pb.Writer.create () in
    Pb.Writer.bytes qr 1 query_id;
    (match result with
     | Query_succeeded response ->
       let succ = Pb.Writer.create () in
       encode_payload_field succ 1 response;
       Pb.Writer.bytes qr 2 (Pb.Writer.contents succ)
     | Query_failed msg -> Pb.Writer.bytes qr 3 (encode_failure msg));
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 3 (Pb.Writer.contents qr);
    (* WorkflowCommand.respond_to_query = 3 *)
    Pb.Writer.contents cmd
  | Update_response { protocol_instance_id; outcome } ->
    (* UpdateResponse { protocol_instance_id=1;
         oneof { google.protobuf.Empty accepted=2;
                 temporal.api.failure.v1.Failure rejected=3;
                 temporal.api.common.v1.Payload completed=4 } } *)
    let ur = Pb.Writer.create () in
    Pb.Writer.bytes ur 1 protocol_instance_id;
    (match outcome with
     | Update_accepted -> Pb.Writer.bytes ur 2 "" (* Empty: a zero-length message *)
     | Update_rejected msg -> Pb.Writer.bytes ur 3 (encode_failure msg)
     | Update_completed p -> encode_payload_field ur 4 p);
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 20 (Pb.Writer.contents ur);
    (* WorkflowCommand.update_response = 20 *)
    Pb.Writer.contents cmd
  | Start_child_workflow_execution c ->
    (* StartChildWorkflowExecution { seq=1; namespace=2; workflow_id=3;
         workflow_type=4; task_queue=5; input=6 (repeated Payload);
         workflow_execution_timeout=7; workflow_run_timeout=8 (Duration);
         parent_close_policy=10 (enum) } *)
    let sc = Pb.Writer.create () in
    Pb.Writer.int sc 1 c.seq;
    if c.namespace <> "" then Pb.Writer.bytes sc 2 c.namespace;
    Pb.Writer.bytes sc 3 c.workflow_id;
    Pb.Writer.bytes sc 4 c.workflow_type;
    Pb.Writer.bytes sc 5 c.task_queue;
    List.iter (encode_payload_field sc 6) c.input;
    if c.execution_timeout > 0. then
      Pb.Writer.bytes sc 7 (encode_duration c.execution_timeout);
    if c.run_timeout > 0. then
      Pb.Writer.bytes sc 8 (encode_duration c.run_timeout);
    if c.parent_close_policy > 0 then Pb.Writer.int sc 10 c.parent_close_policy;
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 11 (Pb.Writer.contents sc);
    (* WorkflowCommand.start_child_workflow_execution = 11 *)
    Pb.Writer.contents cmd
  | Request_cancel_activity { seq } ->
    (* RequestCancelActivity { seq=1 } *)
    let rca = Pb.Writer.create () in
    Pb.Writer.int rca 1 seq;
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 4 (Pb.Writer.contents rca);
    (* WorkflowCommand.request_cancel_activity = 4 *)
    Pb.Writer.contents cmd
  | Cancel_timer { seq } ->
    (* CancelTimer { seq=1 } *)
    let ct = Pb.Writer.create () in
    Pb.Writer.int ct 1 seq;
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 5 (Pb.Writer.contents ct);
    (* WorkflowCommand.cancel_timer = 5 *)
    Pb.Writer.contents cmd
  | Cancel_workflow_execution ->
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 9 "";
    (* WorkflowCommand.cancel_workflow_execution = 9, empty message *)
    Pb.Writer.contents cmd
  | Cancel_child_workflow_execution { child_workflow_seq; reason } ->
    (* CancelChildWorkflowExecution { child_workflow_seq=1; reason=2 } *)
    let cc = Pb.Writer.create () in
    Pb.Writer.int cc 1 child_workflow_seq;
    if reason <> "" then Pb.Writer.bytes cc 2 reason;
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 12 (Pb.Writer.contents cc);
    (* WorkflowCommand.cancel_child_workflow_execution = 12 *)
    Pb.Writer.contents cmd

(* WorkflowActivationCompletion { run_id=1; successful=2 = Success{ commands=1 } } *)
let encode_wf_completion ~run_id ~commands =
  let succ = Pb.Writer.create () in
  List.iter (fun c -> Pb.Writer.bytes succ 1 (encode_command c)) commands;
  let w = Pb.Writer.create () in
  Pb.Writer.bytes w 1 run_id;
  Pb.Writer.bytes w 2 (Pb.Writer.contents succ);
  Pb.Writer.contents w

(* ---- decode: ActivityTask / encode: ActivityTaskCompletion (for #4) --- *)

type activity_start = { activity_type : string; input : payload list }
type activity_task = { task_token : string; start : activity_start option }

(* activity_task.Start { activity_type=5; input=7 (repeated Payload) } *)
let decode_activity_start s =
  let r = Pb.Reader.create s in
  let at = ref "" and input = ref [] in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 5, 2 -> at := Pb.Reader.bytes r
    | 7, 2 -> input := decode_payloads_field r !input
    | _, w -> Pb.Reader.skip r w
  done;
  { activity_type = !at; input = List.rev !input }

(* ActivityTask { task_token=1 (bytes); oneof { Start start=3 } } *)
let decode_activity_task s =
  let r = Pb.Reader.create s in
  let token = ref "" and start = ref None in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> token := Pb.Reader.bytes r
    | 3, 2 -> start := Some (decode_activity_start (Pb.Reader.bytes r))
    | _, w -> Pb.Reader.skip r w
  done;
  { task_token = !token; start = !start }

type activity_outcome = Act_completed of payload option | Act_failed of string

(* ActivityTaskCompletion { task_token=1; result=2 = ActivityExecutionResult }
   ActivityExecutionResult { oneof { Success completed=1; Failure failed=2 } } *)
let encode_activity_completion ~task_token ~result =
  let exec = Pb.Writer.create () in
  (match result with
   | Act_completed p ->
     let succ = Pb.Writer.create () in
     (match p with Some pl -> encode_payload_field succ 1 pl | None -> ());
     Pb.Writer.bytes exec 1 (Pb.Writer.contents succ)
   | Act_failed msg ->
     (* ActivityExecutionResult.failed = activity_result.Failure { failure=1 },
        wrapping temporal.api.failure.v1.Failure { message=1 } *)
     let wrapper = Pb.Writer.create () in
     Pb.Writer.bytes wrapper 1 (encode_failure msg);
     Pb.Writer.bytes exec 2 (Pb.Writer.contents wrapper));
  let w = Pb.Writer.create () in
  Pb.Writer.bytes w 1 task_token;
  Pb.Writer.bytes w 2 (Pb.Writer.contents exec);
  Pb.Writer.contents w
