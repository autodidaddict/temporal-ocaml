open Temporal

(* a user-defined type with a hand-written codec *)
type item = { sku : string; qty : int }

let item_codec =
  Codec.json
    ~encode:(fun i -> `Assoc [ ("sku", `String i.sku); ("qty", `Int i.qty) ])
    ~decode:(fun j ->
      let open Yojson.Safe.Util in
      { sku = j |> member "sku" |> to_string; qty = j |> member "qty" |> to_int })

let failures = ref 0

let check name cond =
  if cond then Printf.printf "ok   %s\n" name
  else (
    Printf.printf "FAIL %s\n" name;
    incr failures)

(* value -> Payload bytes -> value must be identity *)
let roundtrip name codec v = check name (Codec.of_bytes codec (Codec.to_bytes codec v) = v)

let () =
  roundtrip "string" Codec.string "hello, order-42";
  roundtrip "int" Codec.int 4200;
  roundtrip "bool" Codec.bool true;
  roundtrip "unit" Codec.unit ();
  roundtrip "list int" Codec.(list int) [ 1; 2; 3 ];
  roundtrip "option some" Codec.(option string) (Some "x");
  roundtrip "option none" Codec.(option string) None;
  roundtrip "pair" Codec.(pair string int) ("ch_order-1", 4200);
  roundtrip "record (hand-written json codec)" item_codec { sku = "SKU-1"; qty = 3 };
  roundtrip "list of records" (Codec.list item_codec)
    [ { sku = "a"; qty = 1 }; { sku = "b"; qty = 2 } ];
  if !failures > 0 then (
    Printf.printf "%d codec test(s) failed\n" !failures;
    exit 1)
  else Printf.printf "all codec round-trips passed\n"
