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

(* Crate root for the OCaml Temporal worker SDK, over the real temporalio-sdk-core
   via a static C FFI. This module just re-exports the categorized public modules;
   each implementation lives in its own file:

     - Codec    (codec.ml)    payload serialization
     - Activity (activity.ml) activity definitions
     - Workflow (workflow.ml) workflow definitions + execute_activity
     - Client   (client.ml)   server connection
     - Worker   (worker.ml)   the runtime: effect handler + poll loops

   Internal modules — the sdk-core message layer (coresdk.ml / Coresdk), the
   protobuf helpers (pb.ml / Pb), and the C/Rust FFI (Temporal_ffi) — are
   deliberately NOT re-exported, so they stay out of Temporal.* and the generated
   docs. temporal.mli seals the public surface below. *)

module Codec = Codec
module Activity = Activity
module Signal = Signal
module Workflow = Workflow
module Client = Client
module Worker = Worker
