(* Activities: plain OCaml functions. In a real app these do the I/O (charge a
   card, call the warehouse, hit a carrier API); here they are simple stubs. *)

open Temporal
open Model

let validate_order =
  Activity.define ~name:"validate_order" (fun (o : order) ->
      if o.items = [] then
        failwith
          (Printf.sprintf "order %s (%s) has no line items" o.order_id o.customer);
      total_cents o.items)

let charge_payment =
  Activity.define ~name:"charge_payment" (fun (order_id, amount) ->
      Printf.sprintf "ch_%s_%d" order_id amount)

let reserve_inventory =
  Activity.define ~name:"reserve_inventory" (fun (items : line_item list) ->
      "rsv_" ^ String.concat "-" (List.map (fun (i : line_item) -> i.sku) items))

let request_shipment =
  Activity.define ~name:"request_shipment"
    (fun ((order_id : string), (_ship_to : string)) -> "shp_" ^ order_id)

let pick_and_pack =
  Activity.define ~name:"pick_and_pack" (fun (items : line_item list) ->
      Printf.sprintf "pkg_%d_items" (List.length items))

let dispatch_carrier =
  Activity.define ~name:"dispatch_carrier"
    (fun ((package_id : string), (_ship_to : string)) -> "1Z_" ^ package_id)

let confirm_delivery =
  Activity.define ~name:"confirm_delivery" (fun tracking -> "delivered:" ^ tracking)

let authorize_return =
  Activity.define ~name:"authorize_return" (fun order_id -> "rma_" ^ order_id)

let restock_inventory =
  Activity.define ~name:"restock_inventory" (fun (items : line_item list) ->
      List.length items)

let refund_payment =
  Activity.define ~name:"refund_payment"
    (fun ((charge_id : string), (_amount : int)) -> "rf_" ^ charge_id)
