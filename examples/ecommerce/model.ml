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
