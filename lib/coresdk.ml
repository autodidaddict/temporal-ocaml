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
  | Other_resolution

type wf_job =
  | Initialize_workflow of { workflow_type : string; arguments : payload list }
  | Resolve_activity of { seq : int; result : activity_resolution }
  | Other

type wf_activation = { run_id : string; jobs : wf_job list }

let decode_payloads_field r acc = Codec.decode_payload (Pb.Reader.bytes r) :: acc

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
      let fail = Pb.Reader.create (Pb.Reader.bytes r) in
      let msg = ref "" in
      while not (Pb.Reader.at_end fail) do
        match Pb.Reader.key fail with
        | 1, 2 -> msg := Pb.Reader.bytes fail (* Failure.message = 1 *)
        | _, w -> Pb.Reader.skip fail w
      done;
      res := Failed !msg
    | _, w -> Pb.Reader.skip r w
  done;
  !res

(* InitializeWorkflow { workflow_type=1; arguments=3 (repeated Payload) } *)
let decode_initialize s =
  let r = Pb.Reader.create s in
  let wt = ref "" and args = ref [] in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> wt := Pb.Reader.bytes r
    | 3, 2 -> args := decode_payloads_field r !args
    | _, w -> Pb.Reader.skip r w
  done;
  Initialize_workflow { workflow_type = !wt; arguments = List.rev !args }

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

(* WorkflowActivationJob { oneof { initialize_workflow=1; resolve_activity=8; … } } *)
let decode_wf_job s =
  let r = Pb.Reader.create s in
  let job = ref Other in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> job := decode_initialize (Pb.Reader.bytes r)
    | 8, 2 -> job := decode_resolve_activity (Pb.Reader.bytes r)
    | _, w -> Pb.Reader.skip r w
  done;
  !job

(* WorkflowActivation { run_id=1; jobs=5 (repeated WorkflowActivationJob) } *)
let decode_wf_activation s =
  let r = Pb.Reader.create s in
  let run_id = ref "" and jobs = ref [] in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 -> run_id := Pb.Reader.bytes r
    | 5, 2 -> jobs := decode_wf_job (Pb.Reader.bytes r) :: !jobs
    | _, w -> Pb.Reader.skip r w
  done;
  { run_id = !run_id; jobs = List.rev !jobs }

(* ---- encode: WorkflowActivationCompletion ----------------------------- *)

type wf_command =
  | Schedule_activity of {
      seq : int;
      activity_id : string;
      activity_type : string;
      task_queue : string;
      arguments : payload list;
      start_to_close : float; (* seconds *)
    }
  | Complete_workflow_execution of payload option

(* google.protobuf.Duration { int64 seconds=1; int32 nanos=2 } *)
let encode_duration seconds =
  let secs = int_of_float seconds in
  let nanos = int_of_float ((seconds -. float_of_int secs) *. 1e9) in
  let w = Pb.Writer.create () in
  if secs <> 0 then Pb.Writer.int w 1 secs;
  if nanos <> 0 then Pb.Writer.int w 2 nanos;
  Pb.Writer.contents w

let encode_payload_field w field p = Pb.Writer.bytes w field (Codec.encode_payload p)

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
    let cmd = Pb.Writer.create () in
    Pb.Writer.bytes cmd 2 (Pb.Writer.contents sa);
    (* WorkflowCommand.schedule_activity = 2 *)
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
     let fail = Pb.Writer.create () in
     Pb.Writer.bytes fail 1 msg;
     Pb.Writer.bytes exec 2 (Pb.Writer.contents fail));
  let w = Pb.Writer.create () in
  Pb.Writer.bytes w 1 task_token;
  Pb.Writer.bytes w 2 (Pb.Writer.contents exec);
  Pb.Writer.contents w
