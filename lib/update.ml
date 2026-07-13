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

(* An update: a named request delivered into a running workflow that both mutates
   state (like a signal) and returns a value (like a query), and may be gated by a
   validator that can reject it before it is admitted. The name routes it to a
   handler; the codecs type its argument and result. Re-exported as
   Temporal.Update. *)

type ('i, 'o) t = { name : string; input : 'i Codec.t; output : 'o Codec.t }

let define ~name ~input ~output = { name; input; output }
