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

(* Activities: plain OCaml functions. In a real app these do the I/O (charge a
   card, call the warehouse, hit a carrier API); here they are simple stubs. *)

open Temporal
open Model

let validate_order =
  Activity.define ~name:"validate_order" ~input:Order.codec ~output:Codec.int
    (fun (o : Order.t) ->
      if o.items = [] then
        failwith
          (Printf.sprintf "order %s (%s) has no line items" o.order_id o.customer);
      Order.total_cents o.items)

let charge_payment =
  Activity.define ~name:"charge_payment"
    ~input:Codec.(pair string int)
    ~output:Codec.string
    (fun (order_id, amount) -> Printf.sprintf "ch_%s_%d" order_id amount)

let reserve_inventory =
  Activity.define ~name:"reserve_inventory"
    ~input:(Codec.list Line_item.codec) ~output:Codec.string
    (fun (items : Line_item.t list) ->
      "rsv_" ^ String.concat "-" (List.map (fun (i : Line_item.t) -> i.sku) items))

let pick_and_pack =
  Activity.define ~name:"pick_and_pack"
    ~input:(Codec.list Line_item.codec) ~output:Codec.string
    (fun (items : Line_item.t list) ->
      Printf.sprintf "pkg_%d_items" (List.length items))

let dispatch_carrier =
  Activity.define ~name:"dispatch_carrier"
    ~input:Codec.(pair string string)
    ~output:Codec.string
    (fun ((package_id : string), (_ship_to : string)) -> "1Z_" ^ package_id)

let confirm_delivery =
  Activity.define ~name:"confirm_delivery" ~input:Codec.string ~output:Codec.string
    (fun tracking -> "delivered:" ^ tracking)

let authorize_return =
  Activity.define ~name:"authorize_return" ~input:Codec.string ~output:Codec.string
    (fun order_id -> "rma_" ^ order_id)

let restock_inventory =
  Activity.define ~name:"restock_inventory"
    ~input:(Codec.list Line_item.codec) ~output:Codec.int
    (fun (items : Line_item.t list) -> List.length items)

let refund_payment =
  Activity.define ~name:"refund_payment"
    ~input:Codec.(pair string int)
    ~output:Codec.string
    (fun ((charge_id : string), (_amount : int)) -> "rf_" ^ charge_id)
