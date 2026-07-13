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

  val define :
    name:string ->
    input:'input Codec.t ->
    output:'output Codec.t ->
    ('input ctx -> 'input -> 'output) ->
    ('input, 'output) t
  (** [define ~name ~input ~output f] declares a workflow called [name]. *)

  val execute_activity :
    ?start_to_close:float ->
    ?max_attempts:int ->
    _ ctx ->
    ('i, 'o) Activity.t ->
    'i ->
    'o
  (** [execute_activity ctx activity input] schedules [activity], waits for it,
      and returns its result. [start_to_close] is the activity timeout in
      seconds (default 10). [max_attempts] caps retries: 1 disables retries, 0
      (the default) leaves it unlimited (bounded by the timeouts). An activity
      that exhausts its attempts resolves as a failure, which raises in the
      workflow body at the call site. *)

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
    _ ctx ->
    ('i, 'o) t ->
    'i ->
    'o
  (** [execute_child_workflow ctx child input] starts [child] (referred to by its
      own workflow definition) as a child workflow, waits for it, and returns its
      result. [workflow_id] defaults to a deterministic id derived from the
      parent's id; [task_queue] defaults to the parent's; [parent_close_policy]
      defaults to [Terminate]. A child that fails or is cancelled raises at the
      call site, like a failed activity. *)

  val sleep : _ ctx -> float -> unit
  (** [sleep ctx seconds] durably suspends the workflow for [seconds] via a
      Temporal timer — it survives worker restarts and is deterministic on
      replay, unlike a wall-clock [Unix.sleep]. *)

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
