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

(* ---- Developer-facing API + runtime ----------------------------------- *)

(* execute_activity performs this effect; the worker's handler either resolves it
   (activity already completed, replayed from history) or emits a
   Schedule_activity command and suspends the workflow. *)
type _ Effect.t +=
  | Schedule_activity_effect : {
      activity_type : string;
      arg : Codec.payload;
      start_to_close : float;
    }
      -> Codec.payload Effect.t

module Activity = struct
  type ('i, 'o) t = {
    name : string;
    input : 'i Codec.t;
    output : 'o Codec.t;
    run : 'i -> 'o;
  }

  let define ~name ~input ~output run = { name; input; output; run }

  (* registered form, erased to a payload -> payload handler *)
  type reg = { name : string; run_payload : Codec.payload -> Codec.payload }

  let reg (t : (_, _) t) =
    { name = t.name;
      run_payload =
        (fun p -> Codec.to_payload t.output (t.run (Codec.of_payload t.input p))) }
end

module Workflow = struct
  type ctx = { task_queue : string }

  type ('i, 'o) t = {
    name : string;
    input : 'i Codec.t;
    output : 'o Codec.t;
    run : ctx -> 'i -> 'o;
  }

  let define ~name ~input ~output run = { name; input; output; run }

  let execute_activity ?(start_to_close = 10.0) (_ : ctx)
      (a : ('i, 'o) Activity.t) (input : 'i) : 'o =
    let arg = Codec.to_payload a.Activity.input input in
    let result =
      Effect.perform
        (Schedule_activity_effect
           { activity_type = a.Activity.name; arg; start_to_close })
    in
    Codec.of_payload a.Activity.output result

  (* registered form: run the body on the decoded init arg, return output payload *)
  type reg = { name : string; body : ctx -> Codec.payload -> Codec.payload }

  let reg (t : (_, _) t) =
    { name = t.name;
      body =
        (fun ctx p ->
          Codec.to_payload t.output (t.run ctx (Codec.of_payload t.input p))) }
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
    Eio.traceln "registered workflow: %s" r.Workflow.name;
    { t with workflows = r :: t.workflows }

  let register_activity (a : (_, _) Activity.t) (t : t) : t =
    let r = Activity.reg a in
    Eio.traceln "registered activity: %s" r.Activity.name;
    { t with activities = r :: t.activities }

  (* ---- workflow execution ---------------------------------------------- *)
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
  }

  let runs : (string, run_state) Hashtbl.t = Hashtbl.create 16

  let get_run run_id =
    match Hashtbl.find_opt runs run_id with
    | Some s -> s
    | None ->
      let s =
        { wf_name = ""; init_arg = None; resolutions = Hashtbl.create 8 }
      in
      Hashtbl.replace runs run_id s;
      s

  let run_workflow (t : t) (wf : Workflow.reg) (state : run_state) :
      Coresdk.wf_command list =
    let commands = ref [] in
    let seq = ref 0 in
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
            | Schedule_activity_effect { activity_type; arg; start_to_close } ->
              Some
                (fun (k : (a, unit) continuation) ->
                  incr seq;
                  let s = !seq in
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

  (* ---- activity execution ---------------------------------------------- *)
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
end
