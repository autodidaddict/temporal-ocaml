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

(* Payload codecs — see docs/adr/0001-payload-codecs.md.

   A ['a Codec.t] encodes/decodes an OCaml value to/from a Temporal Payload
   (temporal.api.common.v1.Payload { map<string,bytes> metadata = 1;
   bytes data = 2 }). The default is JSON (encoding: json/plain), so we stay
   interoperable with the Go/Python/TS SDKs; users supply the JSON conversion
   for their own types (by hand or via a ppx in their own app). *)

(* ---- Payload (temporal.api.common.v1.Payload) ------------------------- *)

type payload = { metadata : (string * string) list; data : string }

let encode_payload (p : payload) : string =
  let w = Pb.Writer.create () in
  List.iter
    (fun (k, v) ->
      (* map<string,bytes> entry: message { key = 1; value = 2 } *)
      let entry = Pb.Writer.create () in
      Pb.Writer.bytes entry 1 k;
      Pb.Writer.bytes entry 2 v;
      Pb.Writer.bytes w 1 (Pb.Writer.contents entry))
    p.metadata;
  Pb.Writer.bytes w 2 p.data;
  Pb.Writer.contents w

let decode_payload (s : string) : payload =
  let r = Pb.Reader.create s in
  let metadata = ref [] and data = ref "" in
  while not (Pb.Reader.at_end r) do
    match Pb.Reader.key r with
    | 1, 2 ->
      let entry = Pb.Reader.create (Pb.Reader.bytes r) in
      let k = ref "" and v = ref "" in
      while not (Pb.Reader.at_end entry) do
        match Pb.Reader.key entry with
        | 1, 2 -> k := Pb.Reader.bytes entry
        | 2, 2 -> v := Pb.Reader.bytes entry
        | _, w -> Pb.Reader.skip entry w
      done;
      metadata := (!k, !v) :: !metadata
    | 2, 2 -> data := Pb.Reader.bytes r
    | _, w -> Pb.Reader.skip r w
  done;
  { metadata = List.rev !metadata; data = !data }

(* ---- the codec -------------------------------------------------------- *)

type 'a t = { to_json : 'a -> Yojson.Safe.t; of_json : Yojson.Safe.t -> 'a }

exception Decode_error of string

let type_error what j =
  raise
    (Decode_error
       (Printf.sprintf "codec: expected %s, got %s" what (Yojson.Safe.to_string j)))

let to_payload (t : 'a t) (x : 'a) : payload =
  { metadata = [ ("encoding", "json/plain") ]; data = Yojson.Safe.to_string (t.to_json x) }

let of_payload (t : 'a t) (p : payload) : 'a =
  t.of_json (Yojson.Safe.from_string p.data)

(* value <-> serialized Payload bytes (what crosses the FFI) *)
let to_bytes t x = encode_payload (to_payload t x)
let of_bytes t s = of_payload t (decode_payload s)

let json ~encode ~decode = { to_json = encode; of_json = decode }

let string =
  { to_json = (fun s -> `String s);
    of_json = (function `String s -> s | j -> type_error "string" j) }

let int =
  { to_json = (fun i -> `Int i);
    of_json = (function `Int i -> i | j -> type_error "int" j) }

let bool =
  { to_json = (fun b -> `Bool b);
    of_json = (function `Bool b -> b | j -> type_error "bool" j) }

let float =
  { to_json = (fun f -> `Float f);
    of_json =
      (function `Float f -> f | `Int i -> float_of_int i | j -> type_error "float" j) }

let unit = { to_json = (fun () -> `Null); of_json = (fun _ -> ()) }

let list (t : 'a t) : 'a list t =
  { to_json = (fun xs -> `List (List.map t.to_json xs));
    of_json = (function `List l -> List.map t.of_json l | j -> type_error "list" j) }

let option (t : 'a t) : 'a option t =
  { to_json = (function Some x -> t.to_json x | None -> `Null);
    of_json = (function `Null -> None | j -> Some (t.of_json j)) }

let pair (a : 'a t) (b : 'b t) : ('a * 'b) t =
  { to_json = (fun (x, y) -> `List [ a.to_json x; b.to_json y ]);
    of_json =
      (function `List [ x; y ] -> (a.of_json x, b.of_json y) | j -> type_error "pair" j) }
