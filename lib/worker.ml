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
(* We keep a small amount of per-run state (the init argument + the activity
   results seen so far) and re-run the workflow body from the top on each
   activation. execute_activity performs an effect: if the k-th activity is
   already resolved we resume the body with its result; otherwise we emit a
   Schedule_activity command and suspend (drop the continuation). This is the
   deterministic replay model, minus persisting continuations across polls. *)

type resolution = R_ok of Codec.payload | R_fail of string

type run_state = {
  mutable wf_name : string;
  mutable init_arg : Codec.payload option;
  resolutions : (int, resolution) Hashtbl.t; (* activity seq -> result *)
  fired_timers : (int, unit) Hashtbl.t; (* timer seq -> fired *)
}

let runs : (string, run_state) Hashtbl.t = Hashtbl.create 16

let get_run run_id =
  match Hashtbl.find_opt runs run_id with
  | Some s -> s
  | None ->
    let s =
      { wf_name = "";
        init_arg = None;
        resolutions = Hashtbl.create 8;
        fired_timers = Hashtbl.create 8;
      }
    in
    Hashtbl.replace runs run_id s;
    s

let run_workflow (t : t) (wf : Workflow.reg) (state : run_state) :
    Coresdk.wf_command list =
  let commands = ref [] in
  let act_seq = ref 0 in
  let timer_seq = ref 0 in
  let ctx = Workflow.{ task_queue = t.task_queue } in
  let arg =
    match state.init_arg with
    | Some p -> p
    | None -> Codec.to_payload Codec.unit ()
  in
  let open Effect.Deep in
  match_with
    (fun () -> wf.Workflow.body ctx arg)
    ()
    {
      retc =
        (fun (output : Codec.payload) ->
          commands := [ Coresdk.Complete_workflow_execution (Some output) ]);
      exnc =
        (fun exn ->
          Eio.traceln "[wf] body raised: %s" (Printexc.to_string exn);
          commands := []);
      effc =
        (fun (type a) (eff : a Effect.t) ->
          match eff with
          | Workflow.Schedule_activity_effect
              { activity_type; arg; start_to_close } ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr act_seq;
                let s = !act_seq in
                match Hashtbl.find_opt state.resolutions s with
                | Some (R_ok payload) -> continue k payload
                | Some (R_fail msg) ->
                  discontinue k (Failure ("activity failed: " ^ msg))
                | None ->
                  (* not resolved yet: schedule it and suspend this run *)
                  commands :=
                    Coresdk.Schedule_activity
                      {
                        seq = s;
                        activity_id = string_of_int s;
                        activity_type;
                        task_queue = t.task_queue;
                        arguments = [ arg ];
                        start_to_close;
                      }
                    :: !commands)
          | Workflow.Start_timer_effect { start_to_fire } ->
            Some
              (fun (k : (a, unit) continuation) ->
                incr timer_seq;
                let s = !timer_seq in
                if Hashtbl.mem state.fired_timers s then continue k ()
                else
                  commands :=
                    Coresdk.Start_timer { seq = s; start_to_fire } :: !commands)
          | _ -> None);
    };
  List.rev !commands

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
    Hashtbl.replace state.resolutions seq r
  | Coresdk.Fire_timer { seq } -> Hashtbl.replace state.fired_timers seq ()
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
          | Some wf -> run_workflow t wf state
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
