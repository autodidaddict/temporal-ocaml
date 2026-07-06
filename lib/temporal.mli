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

  val define : name:string -> ('input -> 'output) -> ('input, 'output) t
  (** [define ~name f] declares an activity called [name], implemented by [f]. *)

  type reg
  (** A registered activity with its input/output types erased, for
      {!Worker.create}. *)

  val reg : (_, _) t -> reg
end

(** A workflow: deterministic orchestration of activities. *)
module Workflow : sig
  type ctx
  (** The context threaded through a workflow body. *)

  type ('input, 'output) t

  val define :
    name:string -> (ctx -> 'input -> 'output) -> ('input, 'output) t
  (** [define ~name f] declares a workflow called [name], implemented by [f]. *)

  val execute_activity : ctx -> ('i, 'o) Activity.t -> 'i -> 'o
  (** [execute_activity ctx activity input] runs [activity] and returns its
      result. *)

  type reg
  (** A registered workflow with its input/output types erased, for
      {!Worker.create}. *)

  val reg : (_, _) t -> reg
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

  val create :
    Client.t ->
    task_queue:string ->
    workflows:Workflow.reg list ->
    activities:Activity.reg list ->
    t
  (** [create client ~task_queue ~workflows ~activities] builds a worker bound
      to [task_queue]. *)

  val run : t -> unit
  (** Start polling. Blocks until the process is stopped. *)
end
