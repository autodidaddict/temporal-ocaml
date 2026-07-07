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

(* Domain model for the e-commerce order-fulfillment example. Each domain type
   gets its own module (a [type t], its [codec], and any helpers) — the idiomatic
   OCaml convention, and tidier than a flat pile of [order_codec] /
   [line_item_codec] names. Codecs are hand-written JSON; an app could derive them
   with a ppx in its own deps, keeping the SDK ppx-free. *)

open Temporal

module Line_item = struct
  type t = { sku : string; qty : int; unit_price : int (* cents *) }

  let to_json (i : t) =
    `Assoc
      [ ("sku", `String i.sku);
        ("qty", `Int i.qty);
        ("unit_price", `Int i.unit_price) ]

  let of_json j : t =
    let open Yojson.Safe.Util in
    { sku = j |> member "sku" |> to_string;
      qty = j |> member "qty" |> to_int;
      unit_price = j |> member "unit_price" |> to_int }

  let codec : t Codec.t = Codec.json ~encode:to_json ~decode:of_json
end

module Order = struct
  type t = {
    order_id : string;
    customer : string;
    ship_to : string;
    items : Line_item.t list;
  }

  let total_cents (items : Line_item.t list) =
    List.fold_left (fun acc (i : Line_item.t) -> acc + (i.qty * i.unit_price)) 0 items

  let codec : t Codec.t =
    Codec.json
      ~encode:(fun (o : t) ->
        `Assoc
          [ ("order_id", `String o.order_id);
            ("customer", `String o.customer);
            ("ship_to", `String o.ship_to);
            ("items", `List (List.map Line_item.to_json o.items)) ])
      ~decode:(fun j ->
        let open Yojson.Safe.Util in
        { order_id = j |> member "order_id" |> to_string;
          customer = j |> member "customer" |> to_string;
          ship_to = j |> member "ship_to" |> to_string;
          items = j |> member "items" |> to_list |> List.map Line_item.of_json })
end

module Return_request = struct
  type t = {
    return_order : string;
    return_charge : string;
    return_amount : int;
    return_items : Line_item.t list;
  }

  let codec : t Codec.t =
    Codec.json
      ~encode:(fun (r : t) ->
        `Assoc
          [ ("return_order", `String r.return_order);
            ("return_charge", `String r.return_charge);
            ("return_amount", `Int r.return_amount);
            ("return_items", `List (List.map Line_item.to_json r.return_items)) ])
      ~decode:(fun j ->
        let open Yojson.Safe.Util in
        { return_order = j |> member "return_order" |> to_string;
          return_charge = j |> member "return_charge" |> to_string;
          return_amount = j |> member "return_amount" |> to_int;
          return_items =
            j |> member "return_items" |> to_list |> List.map Line_item.of_json })
end
