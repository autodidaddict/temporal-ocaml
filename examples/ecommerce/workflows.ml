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

(* Open Workflow so the workflow-body operations (execute_activity, sleep,
   on_signal, ...) read unqualified. We still write [Workflow.define] at each
   declaration to make plain what's being defined. *)
open Workflow

let order_workflow =
  Workflow.define ~name:"OrderWorkflow" ~input:Order.codec ~output:Codec.string
  @@ fun ctx (o : Order.t) ->
  (* validation is deterministic — a bad order won't pass on retry, so fail fast
     (max_attempts:1) rather than retrying the default unlimited times *)
  let total = execute_activity ~max_attempts:1 ctx Activities.validate_order o in
  let charge = execute_activity ctx Activities.charge_payment (o.order_id, total) in
  (* brief settle window before fulfilment — a durable Temporal timer, not a
     wall-clock sleep, so it survives restarts and replays deterministically *)
  sleep ctx 1.0;
  (* later: on failure below, compensate with refund_payment (saga) *)
  let reservation = execute_activity ctx Activities.reserve_inventory o.items in
  (* later: start ShipmentWorkflow as a child instead of an activity *)
  let shipment =
    execute_activity ctx Activities.request_shipment (o.order_id, o.ship_to)
  in
  Printf.sprintf "order %s: charged %s, reserved %s, shipment %s" o.order_id charge
    reservation shipment

let shipment_workflow =
  Workflow.define ~name:"ShipmentWorkflow"
    ~input:Codec.(pair (list Line_item.codec) string)
    ~output:Codec.string
  @@ fun ctx ((items, ship_to) : Line_item.t list * string) ->
  (* a clean linear hand-off: pack, dispatch, then confirm the tracking number *)
  let package = execute_activity ctx Activities.pick_and_pack items in
  (* later: poll delivery on a timer instead of a single confirm *)
  execute_activity ctx Activities.dispatch_carrier (package, ship_to)
  |> execute_activity ctx Activities.confirm_delivery

let return_workflow =
  Workflow.define ~name:"ReturnWorkflow" ~input:Return_request.codec
    ~output:Codec.string
  @@ fun ctx (r : Return_request.t) ->
  let rma = execute_activity ctx Activities.authorize_return r.return_order in
  let restocked = execute_activity ctx Activities.restock_inventory r.return_items in
  let refund =
    execute_activity ctx Activities.refund_payment (r.return_charge, r.return_amount)
  in
  Printf.sprintf "%s: restocked %d item(s), refunded %s" rma restocked refund

(* Demonstrates continue-as-new: each run does its unit of work and then starts a
   fresh run with decremented state, keeping history bounded. A real long-running
   workflow would loop on [continue_as_new_suggested] instead of a fixed count. *)
let countdown_workflow =
  Workflow.define ~name:"CountdownWorkflow" ~input:Codec.int ~output:Codec.string
  @@ fun ctx (n : int) ->
  if n > 0 then continue_as_new ctx (n - 1)
  else
    Printf.sprintf "countdown finished (suggested=%b, history=%d)"
      (continue_as_new_suggested ctx) (history_length ctx)

(* Demonstrates signals + wait_condition: the workflow blocks until it receives an
   approve or reject signal, then completes with the decision. reject carries a
   typed argument (the reason string). *)
let approve = Signal.define ~name:"approve" Codec.unit
let reject = Signal.define ~name:"reject" Codec.string

let approval_workflow =
  Workflow.define ~name:"ApprovalWorkflow" ~input:Codec.string ~output:Codec.string
  @@ fun ctx (subject : string) ->
  let decision = ref None in
  on_signal ctx approve (fun () -> decision := Some "approved");
  on_signal ctx reject (fun reason -> decision := Some ("rejected: " ^ reason));
  wait_condition ctx (fun () -> !decision <> None);
  Printf.sprintf "%s -> %s" subject (Option.get !decision)
