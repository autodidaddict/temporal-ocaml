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

(* Per-run replay state and history application. Each run keeps a small amount of
   state — the init argument plus a history-ordered log of the external events seen
   so far — which [apply_job] appends to as activation jobs arrive. The replay engine
   ([Replay.run_workflow]) walks that log to drive the workflow body. *)

type resolution = R_ok of Codec.payload | R_fail of string

(* A child's first (start) resolution: it was created with this run id, or it
   could not be created. Its completion reuses [resolution] above. *)
type child_start = Child_run of string | Child_start_fail of string

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
  | Child_started of int * child_start (* seq, start outcome *)
  | Child_resolved of int * resolution (* seq, completion outcome *)

type run_state = {
  mutable wf_name : string;
  mutable wf_id : string; (* this execution's workflow id, from InitializeWorkflow *)
  mutable init_arg : Codec.payload option;
  mutable events_rev : event list; (* history order, newest first (cons to append) *)
}

let runs : (string, run_state) Hashtbl.t = Hashtbl.create 16

let get_run run_id =
  match Hashtbl.find_opt runs run_id with
  | Some s -> s
  | None ->
    let s = { wf_name = ""; wf_id = ""; init_arg = None; events_rev = [] } in
    Hashtbl.replace runs run_id s;
    s

(* drop a run's cached state on eviction (a Remove_from_cache job). *)
let forget run_id = Hashtbl.remove runs run_id

(* apply one activation job to [state]: initialize, or append the external event it
   carries to the history log. Query jobs don't advance the workflow, so they are
   not logged. *)
let apply_job (state : run_state) = function
  | Coresdk.Initialize_workflow { workflow_type; workflow_id; arguments } ->
    state.wf_name <- workflow_type;
    state.wf_id <- workflow_id;
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
       log; they are answered separately by re-running the body in read-only mode
       (see Replay.run_workflow). *)
    ()
  | Coresdk.Resolve_child_workflow_execution_start { seq; outcome } ->
    let cs =
      match outcome with
      | Coresdk.Child_start_succeeded run_id -> Child_run run_id
      | Coresdk.Child_start_failed msg -> Child_start_fail msg
      | Coresdk.Child_start_cancelled msg -> Child_start_fail ("cancelled: " ^ msg)
      | Coresdk.Child_start_other -> Child_start_fail "unknown child start outcome"
    in
    state.events_rev <- Child_started (seq, cs) :: state.events_rev
  | Coresdk.Resolve_child_workflow_execution { seq; result } ->
    let r =
      match result with
      | Coresdk.Child_completed p ->
        R_ok (match p with Some x -> x | None -> Codec.to_payload Codec.unit ())
      | Coresdk.Child_failed msg -> R_fail msg
      | Coresdk.Child_cancelled msg -> R_fail ("cancelled: " ^ msg)
      | Coresdk.Child_result_other -> R_fail "unknown child result"
    in
    state.events_rev <- Child_resolved (seq, r) :: state.events_rev
  | Coresdk.Do_update { protocol_instance_id; name; input; run_validator = _ } ->
    (* admit the update to the log in job order, like a signal; the validator gate
       and the UpdateResponse are handled per-activation in Replay.run_workflow,
       which drops the event again if the validator rejects it. *)
    let p = match input with p :: _ -> p | [] -> Codec.to_payload Codec.unit () in
    state.events_rev <-
      Update { protocol_instance_id; name; input = p } :: state.events_rev
  | Coresdk.Remove_from_cache | Coresdk.Other -> ()
