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

(* OCaml Temporal worker SDK (scaffold), over the real temporalio-sdk-core via a
   static C FFI.

   Milestone 1: you can define workflows + activities, register them, and launch
   a worker that connects and polls. The worker completes received workflow
   tasks (real `CompleteWorkflowExecution` command). Workflow/activity *bodies*
   are defined but not yet driven by the runtime — that is the effect-handler
   milestone (M2). *)

(* The C/Rust FFI boundary lives entirely in the [temporal_ffi] library. *)

(* Payload codecs (lib/codec.ml), re-exported as Temporal.Codec. *)
module Codec = Codec

(* The sdk-core message layer (decode activations / encode commands) is in
   lib/coresdk.ml — module Coresdk. *)

(* ---- Developer-facing API --------------------------------------------- *)

module Activity = struct
  type ('i, 'o) t = { name : string; run : 'i -> 'o }

  let define ~name run = { name; run }

  type reg = Reg : (_, _) t -> reg

  let reg t = Reg t
  let name (Reg t) = t.name
end

module Workflow = struct
  (* M1: the context is opaque. In M2 it carries the effect-based scheduler so
     `execute_activity` suspends the workflow fiber into a ScheduleActivity
     command and resumes it when core delivers the ResolveActivity job. *)
  type ctx = unit

  type ('i, 'o) t = { name : string; run : ctx -> 'i -> 'o }

  let define ~name run = { name; run }

  let execute_activity (_ : ctx) (a : ('i, 'o) Activity.t) (_ : 'i) : 'o =
    failwith
      (Printf.sprintf
         "Workflow.execute_activity %S: workflow bodies are not driven yet \
          (milestone 2: effect handler)"
         a.Activity.name)

  type reg = Reg : (_, _) t -> reg

  let reg t = Reg t
  let name (Reg t) = t.name
end

module Client = struct
  type t = {
    runtime : Temporal_ffi.runtime;
    conn : Temporal_ffi.client;
    spawn_domain : (unit -> unit) -> unit; (* run on a fresh Eio domain *)
  }

  let connect env ~target =
    let dm = Eio.Stdenv.domain_mgr env in
    let spawn_domain (f : unit -> unit) : unit = Eio.Domain_manager.run dm f in
    let runtime = Temporal_ffi.runtime_new () in
    match Temporal_ffi.connect runtime ~target with
    | Ok conn -> { runtime; conn; spawn_domain }
    | Error e -> failwith ("Temporal.Client.connect: " ^ e)
end

module Worker = struct
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

  (* Pipe-friendly registration: the phantom types are erased in here, so the
     [reg] existential never appears in user code. *)
  let register_workflow (w : (_, _) Workflow.t) (t : t) : t =
    let r = Workflow.reg w in
    Eio.traceln "registered workflow: %s" (Workflow.name r);
    { t with workflows = r :: t.workflows }

  let register_activity (a : (_, _) Activity.t) (t : t) : t =
    let r = Activity.reg a in
    Eio.traceln "registered activity: %s" (Activity.name r);
    { t with activities = r :: t.activities }

  (* Complete each workflow task. On the initial activation we emit a real
     CompleteWorkflowExecution command so the workflow finishes (Completed in
     the UI); otherwise we complete with no commands. *)
  let workflow_loop runtime worker =
    let rec loop () =
      match Temporal_ffi.poll_workflow_activation runtime worker with
      | Error e -> Eio.traceln "[wf] poll loop stopped: %s" e
      | Ok bytes ->
        let a = Coresdk.decode_wf_activation bytes in
        let has_init =
          List.exists
            (function Coresdk.Initialize_workflow _ -> true | _ -> false)
            a.Coresdk.jobs
        in
        (* Behaviour unchanged for now: complete the workflow on its first
           activation. The effect handler (next increment) will drive the body
           and emit real Schedule_activity / Complete_workflow_execution. *)
        let commands =
          if has_init then [ Coresdk.Complete_workflow_execution None ] else []
        in
        Eio.traceln "[wf] activation run_id=%s jobs=%d init=%b -> %d command(s)"
          a.Coresdk.run_id (List.length a.Coresdk.jobs) has_init
          (List.length commands);
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

  let activity_loop runtime worker =
    let rec loop () =
      match Temporal_ffi.poll_activity_task runtime worker with
      | Error e -> Eio.traceln "[act] poll loop stopped: %s" e
      | Ok task ->
        Eio.traceln
          "[act] activity task received (bytes=%d) — execution arrives in M2"
          (String.length task);
        loop ()
    in
    loop ()

  let run (t : t) =
    Eio.traceln "worker polling task-queue '%s' (%d workflows, %d activities)"
      t.task_queue (List.length t.workflows) (List.length t.activities);
    let runtime = t.client.Client.runtime and worker = t.worker in
    Eio.Fiber.both
      (fun () ->
        t.client.Client.spawn_domain (fun () -> workflow_loop runtime worker))
      (fun () ->
        t.client.Client.spawn_domain (fun () -> activity_loop runtime worker))
end
