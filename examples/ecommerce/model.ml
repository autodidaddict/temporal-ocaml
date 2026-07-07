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

(* Domain model for the e-commerce order-fulfillment example. *)

open Temporal

type line_item = { sku : string; qty : int; unit_price : int (* cents *) }

type order = {
  order_id : string;
  customer : string;
  ship_to : string;
  items : line_item list;
}

type return_request = {
  return_order : string;
  return_charge : string;
  return_amount : int;
  return_items : line_item list;
}

let total_cents items =
  List.fold_left (fun acc (i : line_item) -> acc + (i.qty * i.unit_price)) 0 items

(* ---- codecs: hand-written JSON (a ppx in the app could generate these) - *)

let line_item_to_json i =
  `Assoc
    [ ("sku", `String i.sku);
      ("qty", `Int i.qty);
      ("unit_price", `Int i.unit_price) ]

let line_item_of_json j =
  let open Yojson.Safe.Util in
  {
    sku = j |> member "sku" |> to_string;
    qty = j |> member "qty" |> to_int;
    unit_price = j |> member "unit_price" |> to_int;
  }

let line_item_codec : line_item Codec.t =
  Codec.json ~encode:line_item_to_json ~decode:line_item_of_json

let order_codec : order Codec.t =
  Codec.json
    ~encode:(fun o ->
      `Assoc
        [ ("order_id", `String o.order_id);
          ("customer", `String o.customer);
          ("ship_to", `String o.ship_to);
          ("items", `List (List.map line_item_to_json o.items)) ])
    ~decode:(fun j ->
      let open Yojson.Safe.Util in
      {
        order_id = j |> member "order_id" |> to_string;
        customer = j |> member "customer" |> to_string;
        ship_to = j |> member "ship_to" |> to_string;
        items = j |> member "items" |> to_list |> List.map line_item_of_json;
      })

let return_request_codec : return_request Codec.t =
  Codec.json
    ~encode:(fun r ->
      `Assoc
        [ ("return_order", `String r.return_order);
          ("return_charge", `String r.return_charge);
          ("return_amount", `Int r.return_amount);
          ("return_items", `List (List.map line_item_to_json r.return_items)) ])
    ~decode:(fun j ->
      let open Yojson.Safe.Util in
      {
        return_order = j |> member "return_order" |> to_string;
        return_charge = j |> member "return_charge" |> to_string;
        return_amount = j |> member "return_amount" |> to_int;
        return_items =
          j |> member "return_items" |> to_list |> List.map line_item_of_json;
      })
