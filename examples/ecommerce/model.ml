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
