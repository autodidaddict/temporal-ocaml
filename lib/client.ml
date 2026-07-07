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

(* A connection to a Temporal server: the FFI runtime + client handle, plus a way
   to run work on a fresh Eio domain (the FFI blocks its thread with block_on).
   Re-exported as Temporal.Client. *)

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
