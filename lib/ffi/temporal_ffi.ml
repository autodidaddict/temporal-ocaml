(* Raw FFI to the Temporal sdk-core C bridge. Internal to the SDK. The interface
   (temporal_ffi.mli) exposes only opaque handles and direct-style operations —
   the [external] declarations and callback plumbing stay private. *)

type runtime
type client
type worker

external runtime_new : unit -> runtime = "ocaml_temporal_runtime_new"
external runtime_free : runtime -> unit = "ocaml_temporal_runtime_free"

external client_connect :
  runtime -> string -> (client option -> string option -> unit) -> unit
  = "ocaml_temporal_client_connect"

external client_free : client -> unit = "ocaml_temporal_client_free"
external worker_new : client -> string -> worker = "ocaml_temporal_worker_new"
external worker_free : worker -> unit = "ocaml_temporal_worker_free"

external poll_wf :
  runtime -> worker -> (string option -> string option -> unit) -> unit
  = "ocaml_temporal_worker_poll"

external complete_wf :
  runtime -> worker -> string -> (string option -> unit) -> unit
  = "ocaml_temporal_worker_complete"

external poll_act :
  runtime -> worker -> (string option -> string option -> unit) -> unit
  = "ocaml_temporal_activity_poll"

external complete_act :
  runtime -> worker -> string -> (string option -> unit) -> unit
  = "ocaml_temporal_activity_complete"

(* The bridge fires its completion callback synchronously (see stubs.c), so this
   resolves before returning. *)
let await (f : ('r -> unit) -> unit) : 'r =
  let cell = ref None in
  f (fun r -> cell := Some r);
  match !cell with
  | Some r -> r
  | None -> failwith "temporal_ffi: completion callback did not fire"

let connect runtime ~target : (client, string) result =
  await (fun k ->
      client_connect runtime target (fun c e ->
          k
            (match (c, e) with
             | Some c, _ -> Ok c
             | None, Some m -> Error m
             | None, None -> Error "connect: unknown error")))

let poll_workflow_activation runtime worker : (string, string) result =
  await (fun k ->
      poll_wf runtime worker (fun a e ->
          k
            (match (a, e) with
             | Some a, _ -> Ok a
             | None, Some m -> Error m
             | None, None -> Error "poll workflow: unknown error")))

let complete_workflow_activation runtime worker ~completion :
    (unit, string) result =
  await (fun k ->
      complete_wf runtime worker completion (fun e ->
          k (match e with None -> Ok () | Some m -> Error m)))

let poll_activity_task runtime worker : (string, string) result =
  await (fun k ->
      poll_act runtime worker (fun t e ->
          k
            (match (t, e) with
             | Some t, _ -> Ok t
             | None, Some m -> Error m
             | None, None -> Error "poll activity: unknown error")))

let complete_activity_task runtime worker ~completion : (unit, string) result =
  await (fun k ->
      complete_act runtime worker completion (fun e ->
          k (match e with None -> Ok () | Some m -> Error m)))
