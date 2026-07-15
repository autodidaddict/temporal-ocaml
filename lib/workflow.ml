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

(* A workflow: deterministic orchestration of activities. Re-exported as
   Temporal.Workflow. *)

type 'input ctx = {
  task_queue : string;
  workflow_id : string; (* this execution's workflow id (stable across replay) *)
  run_id : string; (* this run's id (a fresh one per continue-as-new) *)
  continue_as_new_suggested : bool; (* this activation *)
  history_length : int; (* events in this run's history so far *)
  encode_input : 'input -> Codec.payload; (* the workflow's own input codec *)
  scope : int; (* current cancellation scope id; 0 is the root scope *)
}

type ('i, 'o) t = {
  name : string;
  input : 'i Codec.t;
  output : 'o Codec.t;
  run : 'i ctx -> 'i -> 'o;
}

let define ~name ~input ~output run = { name; input; output; run }

(* An operation the workflow has started and can await: an activity, a timer, a
   child workflow, or a spawned fiber, keyed by its per-kind sequence number. *)
type op = Op_activity of int | Op_timer of int | Op_child of int | Op_fiber of int

(* What happens to a running child when the parent closes. *)
type parent_close_policy = Terminate | Abandon | Request_cancel

(* Raised at an await point inside a cancelled scope, and by an await whose operation
   is already cancelled. An ordinary exception: catch it to compensate, then re-raise
   to propagate the cancel or return normally to deny it. *)
exception Canceled of string

(* start_* emit their command eagerly (once) and return the operation's handle
   without blocking; await parks the fiber on a handle until the operation resolves.
   Each start carries the [scope] it runs under, so cancelling that scope reaches it.
   The worker (replay.ml) performs the emission and the parking; these effects live
   here with their performers. *)
type _ Effect.t +=
  | Start_activity_effect : {
      scope : int;
      activity_type : string;
      arg : Codec.payload;
      start_to_close : float;
      max_attempts : int;
    }
      -> op Effect.t
  | Start_timer_effect : { scope : int; start_to_fire : float } -> op Effect.t
  | Start_child_effect : {
      scope : int;
      workflow_id : string option;
      workflow_type : string;
      input : Codec.payload;
      task_queue : string option;
      parent_close_policy : int; (* 1=TERMINATE 2=ABANDON 3=REQUEST_CANCEL *)
      execution_timeout : float;
      run_timeout : float;
    }
      -> op Effect.t
  | Await_effect : op -> Codec.payload Effect.t
  | Await_any_effect : op list -> (int * Codec.payload) Effect.t
  | Spawn_effect : (unit -> unit) -> op Effect.t
  (* cancellation scopes: create a child scope, cancel one, or check a scope *)
  | New_scope_effect : { parent : int; detached : bool } -> int Effect.t
  | Cancel_scope_effect : int -> unit Effect.t
  | Cancel_requested_effect : int -> bool Effect.t

(* a handle to a not-yet-ready result: the pending operation plus how to decode it *)
type 'a future = { op : op; decode : Codec.payload -> 'a }

let await (_ : _ ctx) (f : 'a future) : 'a =
  f.decode (Effect.perform (Await_effect f.op))

(* await every future. The operations run concurrently, since each was started
   eagerly; this only waits for them all. *)
let await_all ctx (fs : 'a future list) : 'a list = List.map (await ctx) fs

(* the first future to resolve; the losers keep running (Promise.race semantics) *)
let await_any (_ : _ ctx) (fs : 'a future list) : 'a =
  let idx, payload =
    Effect.perform (Await_any_effect (List.map (fun f -> f.op) fs))
  in
  (List.nth fs idx).decode payload

(* spawn a concurrent fiber running [f], scheduled cooperatively with the rest of the
   workflow; returns a future for its result. The result value rides in [cell], which
   the fiber sets on completion and the future's decode reads. *)
let spawn (_ : _ ctx) (f : unit -> 'a) : 'a future =
  let cell = ref None in
  let op = Effect.perform (Spawn_effect (fun () -> cell := Some (f ()))) in
  { op; decode = (fun _ -> match !cell with Some v -> v | None -> raise Not_found) }

let start_activity ?(start_to_close = 10.0) ?(max_attempts = 0) (ctx : _ ctx)
    (a : ('i, 'o) Activity.t) (input : 'i) : 'o future =
  let arg = Codec.to_payload a.Activity.input input in
  let op =
    Effect.perform
      (Start_activity_effect
         { scope = ctx.scope; activity_type = a.Activity.name; arg; start_to_close;
           max_attempts })
  in
  { op; decode = Codec.of_payload a.Activity.output }

(* execute_activity is start-and-await: schedule [activity], wait for it, and return
   the result (raising at the call site if it failed) *)
let execute_activity ?start_to_close ?max_attempts ctx a input =
  await ctx (start_activity ?start_to_close ?max_attempts ctx a input)

let start_timer (ctx : _ ctx) (seconds : float) : unit future =
  let op =
    Effect.perform (Start_timer_effect { scope = ctx.scope; start_to_fire = seconds })
  in
  { op; decode = (fun _ -> ()) }

(* sleep is start-and-await over a durable timer *)
let sleep ctx (seconds : float) : unit = await ctx (start_timer ctx seconds)

let start_child_workflow ?workflow_id ?task_queue ?(parent_close_policy = Terminate)
    ?(execution_timeout = 0.) ?(run_timeout = 0.) (ctx : _ ctx) (w : ('i, 'o) t)
    (input : 'i) : 'o future =
  let policy =
    match parent_close_policy with
    | Terminate -> 1
    | Abandon -> 2
    | Request_cancel -> 3
  in
  let arg = Codec.to_payload w.input input in
  let op =
    Effect.perform
      (Start_child_effect
         { scope = ctx.scope;
           workflow_id;
           workflow_type = w.name;
           input = arg;
           task_queue;
           parent_close_policy = policy;
           execution_timeout;
           run_timeout;
         })
  in
  { op; decode = Codec.of_payload w.output }

(* execute_child_workflow is start-and-await: start the child and wait for it *)
let execute_child_workflow ?workflow_id ?task_queue ?parent_close_policy
    ?execution_timeout ?run_timeout ctx w input =
  await ctx
    (start_child_workflow ?workflow_id ?task_queue ?parent_close_policy
       ?execution_timeout ?run_timeout ctx w input)

(* run [f] in a fresh cancellable child scope. [cancel ()] cancels every operation
   started under the child ctx, raising Canceled in fibers awaiting them. *)
let with_cancel_scope (ctx : 'i ctx) (f : 'i ctx -> cancel:(unit -> unit) -> 'a) : 'a =
  let sid = Effect.perform (New_scope_effect { parent = ctx.scope; detached = false }) in
  let cancel () = Effect.perform (Cancel_scope_effect sid) in
  f { ctx with scope = sid } ~cancel

(* run [f] in a detached child scope. Ancestor cancellation does not reach a detached
   scope, so post-cancel cleanup started under it runs to completion. *)
let detached (ctx : 'i ctx) (f : 'i ctx -> 'a) : 'a =
  let sid = Effect.perform (New_scope_effect { parent = ctx.scope; detached = true }) in
  f { ctx with scope = sid }

(* run [f] in a fresh child scope that auto-cancels after [seconds]. A watcher fiber
   sleeps the deadline in a sibling scope; when it fires it cancels the body scope,
   raising Canceled in [f]. When [f] finishes first, its deadline timer is cancelled.
   Canceled from a fired deadline propagates out, for the caller to handle. *)
let with_timeout (ctx : 'i ctx) (seconds : float) (f : 'i ctx -> 'a) : 'a =
  let body_scope =
    Effect.perform (New_scope_effect { parent = ctx.scope; detached = false })
  in
  let timer_scope =
    Effect.perform (New_scope_effect { parent = ctx.scope; detached = false })
  in
  let ctx_timer = { ctx with scope = timer_scope } in
  let (_ : unit future) =
    spawn ctx_timer (fun () ->
        sleep ctx_timer seconds;
        Effect.perform (Cancel_scope_effect body_scope))
  in
  Fun.protect
    ~finally:(fun () -> Effect.perform (Cancel_scope_effect timer_scope))
    (fun () -> f { ctx with scope = body_scope })

(* whether the current scope has been asked to cancel; for cooperative checks *)
let is_cancel_requested (ctx : _ ctx) : bool =
  Effect.perform (Cancel_requested_effect ctx.scope)

(* continue_as_new ends this run and atomically starts a fresh one (same workflow
   id, new run id, empty history) with new input; the handler emits the terminal
   command and never resumes, so this does not return. *)
type _ Effect.t += Continue_as_new_effect : Codec.payload -> unit Effect.t

let continue_as_new (ctx : 'i ctx) (new_input : 'i) : 'a =
  Effect.perform (Continue_as_new_effect (ctx.encode_input new_input));
  assert false (* the handler drops this continuation; execution never resumes *)

(* core suggests continue-as-new once history grows past the worker's threshold;
   both signals are deterministic on replay, so branching on them is safe. *)
let continue_as_new_suggested (ctx : _ ctx) : bool = ctx.continue_as_new_suggested
let history_length (ctx : _ ctx) : int = ctx.history_length

(* this execution's ids. Both are stable across replay (the run id changes only on
   continue-as-new, which starts a new run), so they are safe to read and to derive
   deterministic values from — e.g. a child workflow id. *)
let workflow_id (ctx : _ ctx) : string = ctx.workflow_id
let run_id (ctx : _ ctx) : string = ctx.run_id

(* on_signal registers a handler (the signal's decoder composed with the user's
   callback); the worker fires it as the replay cursor passes a matching signal
   event. wait_condition blocks the body until [pred] holds, re-checked after each
   activation delivers new events. *)
type _ Effect.t +=
  | Register_signal_handler_effect :
      string * (Codec.payload -> unit)
      -> unit Effect.t
  | Wait_condition_effect : (unit -> bool) -> unit Effect.t

let on_signal (_ : _ ctx) (s : 'a Signal.t) (handler : 'a -> unit) : unit =
  Effect.perform
    (Register_signal_handler_effect
       (s.Signal.name, fun p -> handler (Codec.of_payload s.Signal.codec p)))

let wait_condition (_ : _ ctx) (pred : unit -> bool) : unit =
  Effect.perform (Wait_condition_effect pred)

(* block until [pred] holds or [timeout] seconds elapse; return whether it held in
   time. A watcher fiber sleeps the deadline in a child scope and flips a flag the
   predicate reads; when [pred] wins first the deadline timer is cancelled. *)
let wait_condition_timeout (ctx : _ ctx) ~(timeout : float) (pred : unit -> bool) :
    bool =
  if pred () then true
  else begin
    let timer_scope =
      Effect.perform (New_scope_effect { parent = ctx.scope; detached = false })
    in
    let ctx_timer = { ctx with scope = timer_scope } in
    let timed_out = ref false in
    let (_ : unit future) =
      spawn ctx_timer (fun () ->
          sleep ctx_timer timeout;
          timed_out := true)
    in
    wait_condition ctx (fun () -> pred () || !timed_out);
    let met = pred () in
    Effect.perform (Cancel_scope_effect timer_scope);
    met
  end

(* on_query registers a read-only handler (the query's decoder and encoder wrapped
   around the user's callback). The worker collects these while replaying the body
   to its frontier, then invokes the one matching an incoming QueryWorkflow job.
   The handler is a plain [payload -> payload]; performing any workflow effect from
   inside it is unhandled (query answering runs outside the effect handler), which
   is exactly the read-only guarantee. *)
type _ Effect.t +=
  | Register_query_handler_effect :
      string * (Codec.payload -> Codec.payload)
      -> unit Effect.t

let on_query (_ : _ ctx) (q : ('a, 'b) Query.t) (handler : 'a -> 'b) : unit =
  Effect.perform
    (Register_query_handler_effect
       ( q.Query.name,
         fun p ->
           Codec.to_payload q.Query.output (handler (Codec.of_payload q.Query.input p))
       ))

(* on_update registers a handler (and optional validator) for an update. The
   worker runs the validator (if [validate] is given and core asks for it) to
   accept or reject, then fires the handler as the replay cursor passes the
   admitted update — mutating state like a signal and producing a result like a
   query. Both are erased to payload functions: a validator raises to reject. *)
type _ Effect.t +=
  | Register_update_handler_effect :
      string
      * (Codec.payload -> unit) option (* validator: raise to reject *)
      * (Codec.payload -> Codec.payload) (* handler: mutate state, return result *)
      -> unit Effect.t

let on_update (_ : _ ctx) (u : ('a, 'b) Update.t) ?(validate : ('a -> unit) option)
    (handle : 'a -> 'b) : unit =
  let decode = Codec.of_payload u.Update.input in
  let validator = Option.map (fun v p -> v (decode p)) validate in
  let handler p = Codec.to_payload u.Update.output (handle (decode p)) in
  Effect.perform
    (Register_update_handler_effect (u.Update.name, validator, handler))

(* registered form: builds the typed ctx (carrying the workflow's own input
   encoder) from the per-activation runtime info, then runs the body. *)
type reg = {
  name : string;
  body :
    task_queue:string ->
    workflow_id:string ->
    run_id:string ->
    can_suggested:bool ->
    history_length:int ->
    Codec.payload ->
    Codec.payload;
}

let reg (t : (_, _) t) =
  { name = t.name;
    body =
      (fun ~task_queue ~workflow_id ~run_id ~can_suggested ~history_length p ->
        let ctx =
          { task_queue;
            workflow_id;
            run_id;
            continue_as_new_suggested = can_suggested;
            history_length;
            encode_input = Codec.to_payload t.input;
            scope = 0;
          }
        in
        Codec.to_payload t.output (t.run ctx (Codec.of_payload t.input p))) }
