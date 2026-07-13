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

(* Worker entry point for the e-commerce order-fulfillment example.

   Structure (mirrors the other Temporal SDKs' examples):
     model.ml      — domain types
     activities.ml — activity implementations (plain functions; do I/O in real life)
     workflows.ml  — workflow definitions (deterministic orchestration)
     main.ml       — connect, register, run

   Run:
     temporal server start-dev
     make run
     temporal workflow start --task-queue ecommerce \
       --type OrderWorkflow --workflow-id order-1
*)

open Temporal

let env_or name default = try Sys.getenv name with Not_found -> default

let () =
  Eio_main.run @@ fun env ->
  let target = env_or "TEMPORAL_TARGET" "http://localhost:7233" in
  let task_queue = env_or "TEMPORAL_TASK_QUEUE" "ecommerce" in
  Eio.traceln "connecting to %s ..." target;
  let client = Client.connect env ~target in
  let worker =
    let open Worker in
    let open Activities in
    let open Workflows in
    create client ~task_queue
    |> register_activity validate_order
    |> register_activity charge_payment
    |> register_activity reserve_inventory
    |> register_activity request_shipment
    |> register_activity pick_and_pack
    |> register_activity dispatch_carrier
    |> register_activity confirm_delivery
    |> register_activity authorize_return
    |> register_activity restock_inventory
    |> register_activity refund_payment
    |> register_workflow order_workflow
    |> register_workflow shipment_workflow
    |> register_workflow return_workflow
    |> register_workflow countdown_workflow
    |> register_workflow approval_workflow
    |> register_workflow buffered_signal_workflow
    |> register_workflow account_workflow
  in
  Worker.run worker
