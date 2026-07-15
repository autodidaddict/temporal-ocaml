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

(** OCaml SDK for Temporal.

    Define activities and workflows, register them on a worker, and run it. The
    transport underneath is an implementation detail and is not part of this
    interface. *)

(** Serialization for values that cross the Temporal boundary. A codec maps an
    OCaml value to/from a Payload (JSON by default, for cross-SDK interop). Users
    supply the JSON conversion for their own types — by hand or via a ppx in
    their own app; the SDK depends on neither. *)
module Codec : sig
  type 'a t

  val json :
    encode:('a -> Yojson.Safe.t) -> decode:(Yojson.Safe.t -> 'a) -> 'a t

  val string : string t
  val int : int t
  val bool : bool t
  val float : float t
  val unit : unit t

  val list : 'a t -> 'a list t
  val option : 'a t -> 'a option t
  val pair : 'a t -> 'b t -> ('a * 'b) t

  val to_bytes : 'a t -> 'a -> string
  (** Serialize a value to its Payload wire bytes. *)

  val of_bytes : 'a t -> string -> 'a
end

(** An activity: a plain function from input to output. *)
module Activity : sig
  type ('input, 'output) t

  val define :
    name:string ->
    input:'input Codec.t ->
    output:'output Codec.t ->
    ('input -> 'output) ->
    ('input, 'output) t
  (** [define ~name ~input ~output f] declares an activity called [name], with
      codecs for its argument and result. *)
end

(** A signal: a named, typed message delivered into a running workflow. *)
module Signal : sig
  type 'a t

  val define : name:string -> 'a Codec.t -> 'a t
  (** [define ~name codec] declares a signal named [name] carrying an ['a]
      argument. *)
end

(** A query: a named, typed, read-only request answered from a running (or
    closed) workflow's current state. *)
module Query : sig
  type ('input, 'output) t

  val define :
    name:string ->
    input:'input Codec.t ->
    output:'output Codec.t ->
    ('input, 'output) t
  (** [define ~name ~input ~output] declares a query named [name] taking an
      ['input] argument and returning an ['output]. Use [Codec.unit] for the
      common no-argument query. *)
end

(** An update: a request that both mutates workflow state (like a signal) and
    returns a value (like a query), optionally gated by a validator. *)
module Update : sig
  type ('input, 'output) t

  val define :
    name:string ->
    input:'input Codec.t ->
    output:'output Codec.t ->
    ('input, 'output) t
  (** [define ~name ~input ~output] declares an update named [name] taking an
      ['input] argument and returning an ['output]. *)
end

(** A workflow: deterministic orchestration of activities. *)
module Workflow : sig
  type 'input ctx
  (** The context threaded through a workflow body, parameterized by the
      workflow's own input type so {!continue_as_new} can re-encode it. *)

  type ('input, 'output) t

  type 'a future
  (** A handle to the not-yet-ready result of a started operation (activity, timer,
      or child workflow). Produced by a [start_*], consumed by {!await}. *)

  val define :
    name:string ->
    input:'input Codec.t ->
    output:'output Codec.t ->
    ('input ctx -> 'input -> 'output) ->
    ('input, 'output) t
  (** [define ~name ~input ~output f] declares a workflow called [name]. *)

  type activity_cancel_type =
    | Try_cancel  (** request cancellation and raise {!Canceled} at once (default) *)
    | Wait_cancellation_completed
        (** request cancellation, then raise {!Canceled} only once the activity's own
            resolution arrives. This needs activity heartbeating, which does not exist
            yet, so today it mostly affects an activity that has not started. *)
    | Abandon  (** raise {!Canceled} at once and send no cancellation request *)

  val execute_activity :
    ?start_to_close:float ->
    ?max_attempts:int ->
    ?cancel_type:activity_cancel_type ->
    _ ctx ->
    ('i, 'o) Activity.t ->
    'i ->
    'o
  (** [execute_activity ctx activity input] schedules [activity], waits for it,
      and returns its result. [start_to_close] is the activity timeout in
      seconds (default 10). [max_attempts] caps retries: 1 disables retries, 0
      (the default) leaves it unlimited (bounded by the timeouts). An activity
      that exhausts its attempts resolves as a failure, which raises in the
      workflow body at the call site. [cancel_type] selects how a cancel of the
      enclosing scope reaches the activity (default {!Try_cancel}). *)

  type parent_close_policy =
    | Terminate  (** terminate the child when the parent closes (default) *)
    | Abandon  (** leave the child running independently *)
    | Request_cancel  (** request cancellation of the child *)

  val execute_child_workflow :
    ?workflow_id:string ->
    ?task_queue:string ->
    ?parent_close_policy:parent_close_policy ->
    ?execution_timeout:float ->
    ?run_timeout:float ->
    ?wait_for_cancellation:bool ->
    _ ctx ->
    ('i, 'o) t ->
    'i ->
    'o
  (** [execute_child_workflow ctx child input] starts [child] (referred to by its
      own workflow definition) as a child workflow, waits for it, and returns its
      result. [workflow_id] defaults to a deterministic id derived from the
      parent's id; [task_queue] defaults to the parent's; [parent_close_policy]
      defaults to [Terminate]. A child that fails raises at the call site like a
      failed activity, and a cancelled child raises {!Canceled}. If
      [wait_for_cancellation] is set, cancelling the enclosing scope requests the
      child's cancellation and then waits for the child to actually end before
      raising, rather than raising at once (default [false]). *)

  val sleep : _ ctx -> float -> unit
  (** [sleep ctx seconds] durably suspends the workflow for [seconds] via a
      Temporal timer — it survives worker restarts and is deterministic on
      replay, unlike a wall-clock [Unix.sleep]. *)

  val start_activity :
    ?start_to_close:float ->
    ?max_attempts:int ->
    ?cancel_type:activity_cancel_type ->
    _ ctx ->
    ('i, 'o) Activity.t ->
    'i ->
    'o future
  (** [start_activity ctx activity input] schedules [activity] and returns a future
      immediately; the activity runs while the workflow continues.
      {!execute_activity} is [await ctx (start_activity ...)]. *)

  val start_timer : _ ctx -> float -> unit future
  (** [start_timer ctx seconds] starts a durable timer and returns a future that
      resolves when it fires. {!sleep} is [await ctx (start_timer ...)]. *)

  val start_child_workflow :
    ?workflow_id:string ->
    ?task_queue:string ->
    ?parent_close_policy:parent_close_policy ->
    ?execution_timeout:float ->
    ?run_timeout:float ->
    ?wait_for_cancellation:bool ->
    _ ctx ->
    ('i, 'o) t ->
    'i ->
    'o future
  (** [start_child_workflow ctx child input] starts [child] and returns a future for
      its result. {!execute_child_workflow} is [await ctx (start_child_workflow ...)]. *)

  val await : _ ctx -> 'a future -> 'a
  (** [await ctx f] blocks the workflow until [f] resolves and returns its value,
      raising at the call site if the operation failed. *)

  val await_all : _ ctx -> 'a future list -> 'a list
  (** [await_all ctx fs] waits for every future in [fs]. Because each was started
      eagerly the operations run concurrently; this only waits for them all and
      returns the results in order. Start several activities, then [await_all], for
      fan-out and fan-in. *)

  val await_any : _ ctx -> 'a future list -> 'a
  (** [await_any ctx fs] returns the result of the first future to resolve. The
      losers keep running (Promise.race semantics); cancelling them awaits the
      cancellation-scope work. *)

  val spawn : _ ctx -> (unit -> 'a) -> 'a future
  (** [spawn ctx f] runs [f] as a concurrent fiber, scheduled cooperatively with the
      rest of the workflow, and returns a future for its result. Use it for
      independent concurrent control flow; to fan out operations, [start_*] plus
      {!await_all} is simpler. *)

  exception Canceled of string
  (** Raised at an await point inside a cancelled scope. Catch it to compensate and
      re-raise to propagate the cancel, or return normally to deny it. *)

  val with_cancel_scope : 'i ctx -> ('i ctx -> cancel:(unit -> unit) -> 'a) -> 'a
  (** [with_cancel_scope ctx (fun ctx' ~cancel -> ...)] runs the body in a fresh
      cancellable child scope. [cancel ()] cancels every operation started under
      [ctx'], emitting its cancel command and raising {!Canceled} in fibers awaiting
      it; an await under an already-cancelled scope raises at once. *)

  val with_timeout : 'i ctx -> float -> ('i ctx -> 'a) -> 'a
  (** [with_timeout ctx seconds f] runs [f] in a child scope that auto-cancels after
      [seconds]. If the deadline fires first, the operations [f] is awaiting are
      cancelled and {!Canceled} is raised in [f], propagating out unless [f] catches
      it. If [f] finishes first, the deadline timer is cancelled. *)

  val detached : 'i ctx -> ('i ctx -> 'a) -> 'a
  (** [detached ctx f] runs [f] in a detached child scope. Cancellation of an ancestor
      scope does not reach a detached scope, so cleanup started under it (for example
      a compensating activity after a cancel) runs to completion. *)

  val is_cancel_requested : _ ctx -> bool
  (** whether the current scope has been asked to cancel, for cooperative checks *)

  val on_signal : _ ctx -> 'a Signal.t -> ('a -> unit) -> unit
  (** [on_signal ctx signal handler] runs [handler] whenever [signal] is
      received. The handler runs synchronously and typically mutates state the
      body observes via {!wait_condition}. *)

  val on_query : _ ctx -> ('a, 'b) Query.t -> ('a -> 'b) -> unit
  (** [on_query ctx query handler] registers [handler] to answer [query]. The
      handler must be read-only: it may inspect body state but must not mutate
      it, run activities, sleep, or continue-as-new. It runs when a query
      arrives, after the body has replayed to its current frontier, and its
      result is returned to the caller. A query with no registered handler
      answers with an error. *)

  val on_update :
    _ ctx -> ('a, 'b) Update.t -> ?validate:('a -> unit) -> ('a -> 'b) -> unit
  (** [on_update ctx update ?validate handle] registers [handle] for [update].
      [handle] runs when the update is delivered — it may mutate body state
      (which a subsequent {!wait_condition} observes) and returns a result sent
      back to the caller. If [validate] is given it runs first, on the update's
      first delivery only, and rejects the update by raising: a rejected update
      is not admitted, mutates nothing, and returns the failure to the caller.
      Like signal and query handlers, both run synchronously — they must not run
      activities, sleep, or continue-as-new. *)

  val wait_condition : _ ctx -> (unit -> bool) -> unit
  (** [wait_condition ctx pred] blocks the workflow until [pred ()] holds,
      re-checked after each activation delivers new signals, timers, or activity
      results. *)

  val wait_condition_timeout : _ ctx -> timeout:float -> (unit -> bool) -> bool
  (** [wait_condition_timeout ctx ~timeout pred] blocks like {!wait_condition} but
      gives up after [timeout] seconds. It returns [true] if [pred ()] held in time
      and [false] if the timeout fired first. *)

  val continue_as_new : 'input ctx -> 'input -> 'a
  (** [continue_as_new ctx input] ends the current run and atomically starts a
      fresh run of the same workflow — same workflow id, new run id, empty
      history — with [input]. It does not return. Use it to bound history growth
      in long-running or looping workflows. *)

  val continue_as_new_suggested : _ ctx -> bool
  (** Whether core suggests continuing-as-new now (this run's history has grown
      past the worker's threshold). Deterministic on replay, so safe to branch
      on. *)

  val history_length : _ ctx -> int
  (** Number of events in this run's history so far. Deterministic on replay. *)

  val workflow_id : _ ctx -> string
  (** This execution's workflow id. Stable across replay and across
      continue-as-new, so it is safe to branch on or to derive deterministic
      values from (e.g. a child workflow id). *)

  val run_id : _ ctx -> string
  (** This run's id. Stable across replay; a fresh run id is assigned on each
      continue-as-new. Deterministic on replay. *)
end

(** A connection to a Temporal server. *)
module Client : sig
  type t

  val connect :
    < domain_mgr : _ Eio.Domain_manager.t ; .. > -> target:string -> t
  (** [connect env ~target] connects to the Temporal server at [target], e.g.
      ["http://localhost:7233"]. *)
end

(** A worker: polls a task queue and serves registered workflows and
    activities. *)
module Worker : sig
  type t

  val create : Client.t -> task_queue:string -> t
  (** [create client ~task_queue] builds an empty worker bound to [task_queue].
      Register with {!register_workflow} / {!register_activity}. *)

  val register_workflow : (_, _) Workflow.t -> t -> t
  (** [worker |> register_workflow w] registers workflow [w]. *)

  val register_activity : (_, _) Activity.t -> t -> t
  (** [worker |> register_activity a] registers activity [a]. *)

  val run : t -> unit
  (** Start polling. Blocks until the process is stopped. *)
end
