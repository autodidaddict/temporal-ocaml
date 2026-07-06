(* Worker entry point for the e-commerce order-fulfillment example.

   Structure (mirrors the other Temporal SDKs' examples):
     model.ml      — domain types
     activities.ml — activity implementations (plain functions; do I/O in real life)
     workflows.ml  — workflow definitions (deterministic orchestration)
     main.ml       — connect, register, run

   M1 caveat: the runtime doesn't drive workflow bodies yet (execute_activity
   raises), so running this registers everything, connects, and completes the
   workflow tasks it receives.

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
    Worker.create client ~task_queue
      ~workflows:
        [ Workflow.reg Workflows.order_workflow;
          Workflow.reg Workflows.shipment_workflow;
          Workflow.reg Workflows.return_workflow ]
      ~activities:
        [ Activity.reg Activities.validate_order;
          Activity.reg Activities.charge_payment;
          Activity.reg Activities.reserve_inventory;
          Activity.reg Activities.request_shipment;
          Activity.reg Activities.pick_and_pack;
          Activity.reg Activities.dispatch_carrier;
          Activity.reg Activities.confirm_delivery;
          Activity.reg Activities.authorize_return;
          Activity.reg Activities.restock_inventory;
          Activity.reg Activities.refund_payment ]
  in
  Worker.run worker
