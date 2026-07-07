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

(* An activity: a plain OCaml function plus codecs for its input and output.
   Re-exported as Temporal.Activity. *)

type ('i, 'o) t = {
  name : string;
  input : 'i Codec.t;
  output : 'o Codec.t;
  run : 'i -> 'o;
}

let define ~name ~input ~output run = { name; input; output; run }

(* registered form, erased to a payload -> payload handler *)
type reg = { name : string; run_payload : Codec.payload -> Codec.payload }

let reg (t : (_, _) t) =
  { name = t.name;
    run_payload =
      (fun p -> Codec.to_payload t.output (t.run (Codec.of_payload t.input p))) }
