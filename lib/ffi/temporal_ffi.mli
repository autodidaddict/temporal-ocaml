(** Internal FFI to the Temporal sdk-core bridge.

    Not part of the SDK's public surface: opaque handles plus direct-style
    operations, with the raw [external] bindings and callback plumbing hidden. *)

type runtime
type client
type worker

val runtime_new : unit -> runtime
val runtime_free : runtime -> unit

val connect : runtime -> target:string -> (client, string) result
val client_free : client -> unit

val worker_new : client -> string -> worker
val worker_free : worker -> unit

val poll_workflow_activation : runtime -> worker -> (string, string) result

val complete_workflow_activation :
  runtime -> worker -> completion:string -> (unit, string) result

val poll_activity_task : runtime -> worker -> (string, string) result

val complete_activity_task :
  runtime -> worker -> completion:string -> (unit, string) result
