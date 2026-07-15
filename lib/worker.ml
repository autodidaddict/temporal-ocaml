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
   This is the runtime driver — construction, registration, and the two poll loops.
   The workflow loop feeds each activation through [Replay_state] (history) and
   [Replay.run_workflow] (the effect handler); the activity loop runs registered
   activities. Re-exported as Temporal.Worker. *)

open Replay_state

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
         queries — if so the replay must be read-only (see Replay.run_workflow). *)
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
          forget a.Coresdk.run_id;
          [])
        else
          match
            List.find_opt
              (fun (w : Workflow.reg) -> w.Workflow.name = state.wf_name)
              t.workflows
          with
          | Some wf ->
            Replay.run_workflow wf state ~task_queue:t.task_queue
              ~run_id:a.Coresdk.run_id
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
