(* OCaml Temporal worker SDK (scaffold), over the real temporalio-sdk-core via a
   static C FFI.

   Milestone 1: you can define workflows + activities, register them, and launch
   a worker that connects and polls. The worker completes received workflow
   tasks (real `CompleteWorkflowExecution` command). Workflow/activity *bodies*
   are defined but not yet driven by the runtime — that is the effect-handler
   milestone (M2). *)

(* The C/Rust FFI boundary lives entirely in the [temporal_ffi] library. *)

(* ---- Coresdk: small dependency-free protobuf codec -------------------- *)
(* Deliberately hand-rolled: no codegen, no ppx, no runtime library, no system
   protobuf — so consumers of this library inherit none of that. Covers exactly
   the coresdk messages the worker loop touches; field numbers verified against
   temporal/sdk/core/*.proto. Grow this (or revisit codegen) as more message
   types are needed. *)
module Coresdk = struct
  let read_varint s pos =
    let shift = ref 0 and result = ref 0 and continue = ref true in
    while !continue do
      let b = Char.code (String.get s !pos) in
      incr pos;
      result := !result lor ((b land 0x7f) lsl !shift);
      shift := !shift + 7;
      if b < 0x80 then continue := false
    done;
    !result

  let add_varint buf n =
    let n = ref n and continue = ref true in
    while !continue do
      let b = !n land 0x7f in
      n := !n lsr 7;
      if !n = 0 then (
        Buffer.add_char buf (Char.chr b);
        continue := false)
      else Buffer.add_char buf (Char.chr (b lor 0x80))
    done

  (* append a length-delimited field (wire type 2) *)
  let add_field buf field payload =
    add_varint buf ((field lsl 3) lor 2);
    add_varint buf (String.length payload);
    Buffer.add_string buf payload

  (* does the message in s[off, off+len) contain field number [target]? *)
  let msg_has_field s off len target =
    let pos = ref off and stop = off + len and found = ref false in
    (try
       while !pos < stop do
         let key = read_varint s pos in
         let field = key lsr 3 and wire = key land 0x7 in
         if field = target then found := true;
         match wire with
         | 0 -> ignore (read_varint s pos)
         | 1 -> pos := !pos + 8
         | 5 -> pos := !pos + 4
         | 2 ->
           let l = read_varint s pos in
           pos := !pos + l
         | _ -> raise Exit
       done
     with Exit -> ());
    !found

  type activation = {
    run_id : string;
    num_jobs : int;
    has_initialize : bool; (* an InitializeWorkflow job is present *)
  }

  (* WorkflowActivation { run_id=1; jobs=5 (repeated WorkflowActivationJob) }
     WorkflowActivationJob { initialize_workflow=1; ... } *)
  let decode_activation s =
    let n = String.length s in
    let pos = ref 0 in
    let run_id = ref "" and jobs = ref 0 and init = ref false in
    (try
       while !pos < n do
         let key = read_varint s pos in
         let field = key lsr 3 and wire = key land 0x7 in
         match wire with
         | 0 -> ignore (read_varint s pos)
         | 1 -> pos := !pos + 8
         | 5 -> pos := !pos + 4
         | 2 ->
           let len = read_varint s pos in
           let off = !pos in
           if field = 1 then run_id := String.sub s off len
           else if field = 5 then (
             incr jobs;
             if msg_has_field s off len 1 then init := true);
           pos := off + len
         | _ -> raise Exit
       done
     with Exit -> ());
    { run_id = !run_id; num_jobs = !jobs; has_initialize = !init }

  (* WorkflowCommand variants we can emit (workflow_commands.proto). *)
  type command = Complete_workflow_execution

  let encode_command = function
    | Complete_workflow_execution ->
      (* WorkflowCommand { complete_workflow_execution=6 = CompleteWorkflowExecution{} } *)
      let b = Buffer.create 8 in
      add_field b 6 "";
      (* empty CompleteWorkflowExecution (no result Payload) *)
      Buffer.contents b

  (* WorkflowActivationCompletion { run_id=1; successful=2 = Success{ commands=1 } } *)
  let encode_completion ~run_id ~commands =
    let success = Buffer.create 32 in
    List.iter (fun c -> add_field success 1 (encode_command c)) commands;
    let b = Buffer.create 64 in
    add_field b 1 run_id;
    add_field b 2 (Buffer.contents success);
    Buffer.contents b
end

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

  let create (client : Client.t) ~task_queue ~workflows ~activities =
    List.iter
      (fun w -> Eio.traceln "registered workflow: %s" (Workflow.name w))
      workflows;
    List.iter
      (fun a -> Eio.traceln "registered activity: %s" (Activity.name a))
      activities;
    let worker = Temporal_ffi.worker_new client.Client.conn task_queue in
    { client; worker; task_queue; workflows; activities }

  (* Complete each workflow task. On the initial activation we emit a real
     CompleteWorkflowExecution command so the workflow finishes (Completed in
     the UI); otherwise we complete with no commands. *)
  let workflow_loop runtime worker =
    let rec loop () =
      match Temporal_ffi.poll_workflow_activation runtime worker with
      | Error e -> Eio.traceln "[wf] poll loop stopped: %s" e
      | Ok bytes ->
        let a = Coresdk.decode_activation bytes in
        let commands =
          if a.Coresdk.has_initialize then [ Coresdk.Complete_workflow_execution ]
          else []
        in
        Eio.traceln "[wf] activation run_id=%s jobs=%d init=%b -> %d command(s)"
          a.Coresdk.run_id a.Coresdk.num_jobs a.Coresdk.has_initialize
          (List.length commands);
        (match
           Temporal_ffi.complete_workflow_activation runtime worker
             ~completion:
               (Coresdk.encode_completion ~run_id:a.Coresdk.run_id ~commands)
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
