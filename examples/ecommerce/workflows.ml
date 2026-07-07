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

(* Workflows: deterministic orchestration of activities. Each one sequences a few
   activities via [execute_activity]. Interactions that need SDK primitives we
   don't have yet are marked "later:". *)

open Temporal
open Model

let order_workflow =
  Workflow.define ~name:"OrderWorkflow" ~input:order_codec ~output:Codec.string
    (fun ctx (o : order) ->
      (* validation is deterministic — a bad order won't pass on retry, so fail
         fast (max_attempts:1) rather than retrying the default unlimited times *)
      let total =
        Workflow.execute_activity ~max_attempts:1 ctx Activities.validate_order o
      in
      let charge =
        Workflow.execute_activity ctx Activities.charge_payment (o.order_id, total)
      in
      (* brief settle window before fulfilment — a durable Temporal timer, not a
         wall-clock sleep, so it survives restarts and replays deterministically *)
      Workflow.sleep ctx 1.0;
      (* later: on failure below, compensate with refund_payment (saga) *)
      let reservation =
        Workflow.execute_activity ctx Activities.reserve_inventory o.items
      in
      (* later: start ShipmentWorkflow as a child instead of an activity *)
      let shipment =
        Workflow.execute_activity ctx Activities.request_shipment
          (o.order_id, o.ship_to)
      in
      Printf.sprintf "order %s: charged %s, reserved %s, shipment %s" o.order_id
        charge reservation shipment)

let shipment_workflow =
  Workflow.define ~name:"ShipmentWorkflow"
    ~input:Codec.(pair (list line_item_codec) string)
    ~output:Codec.string
    (fun ctx ((items, ship_to) : line_item list * string) ->
      let package = Workflow.execute_activity ctx Activities.pick_and_pack items in
      let tracking =
        Workflow.execute_activity ctx Activities.dispatch_carrier (package, ship_to)
      in
      (* later: poll delivery on a timer instead of a single confirm *)
      Workflow.execute_activity ctx Activities.confirm_delivery tracking)

let return_workflow =
  Workflow.define ~name:"ReturnWorkflow" ~input:return_request_codec
    ~output:Codec.string
    (fun ctx (r : return_request) ->
      let rma =
        Workflow.execute_activity ctx Activities.authorize_return r.return_order
      in
      let restocked =
        Workflow.execute_activity ctx Activities.restock_inventory r.return_items
      in
      let refund =
        Workflow.execute_activity ctx Activities.refund_payment
          (r.return_charge, r.return_amount)
      in
      Printf.sprintf "%s: restocked %d item(s), refunded %s" rma restocked refund)
