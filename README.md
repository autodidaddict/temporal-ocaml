# temporal-ocaml (scaffold)

An experiment in building a **Temporal worker SDK for OCaml**, on top of the
real `temporalio-sdk-core` (Rust) via a static C FFI, with an Eio-native front
end.

**Milestone 1:** define workflows + activities, register them, launch the binary
→ it connects to Temporal and starts polling task queues, and **completes the
workflow tasks it receives**. Verified end-to-end against `temporal server
start-dev`: two workflows started via the CLI both reach `Status COMPLETED`.

```
registered workflow: OrderWorkflow … ShipmentWorkflow … ReturnWorkflow
registered activity: validate_order … charge_payment … reserve_inventory … (10 total)
worker polling task-queue 'ecommerce' (3 workflows, 10 activities)
[wf] activation run_id=019f38cd-… jobs=1 init=true -> 1 command(s)   # CompleteWorkflowExecution
  order-1  -> Status COMPLETED
  return-1 -> Status COMPLETED
```

The workflow/activity **bodies** are defined with real types and read the way the
finished SDK will, but the runtime does not drive them yet — that is the
effect-handler milestone (M2). So in M1 the example "runs a worker and waits",
and the worker completes tasks with a trivial (empty-result) completion.

## The developer API (see `examples/ecommerce/` for the full e-commerce example)

```ocaml
open Temporal

(* activities are plain functions *)
let validate_order =
  Activity.define ~name:"validate_order" (fun (o : order) -> total_cents o.items)
let charge_payment =
  Activity.define ~name:"charge_payment" (fun (order_id, amount) ->
      Printf.sprintf "ch_%s_%d" order_id amount)

(* workflows orchestrate activities *)
let order_workflow =
  Workflow.define ~name:"OrderWorkflow" (fun ctx (o : order) ->
      let total = Workflow.execute_activity ctx validate_order o in
      let charge = Workflow.execute_activity ctx charge_payment (o.order_id, total) in
      let shipment = Workflow.execute_activity ctx request_shipment (o.order_id, o.ship_to) in
      Printf.sprintf "order %s: charged %s, shipment %s" o.order_id charge shipment)

let () =
  Eio_main.run @@ fun env ->
  let client = Client.connect env ~target:"http://localhost:7233" in
  let worker =
    Worker.create client ~task_queue:"ecommerce"
      ~workflows:[ Workflow.reg order_workflow; Workflow.reg shipment_workflow;
                   Workflow.reg return_workflow ]
      ~activities:[ Activity.reg validate_order; Activity.reg charge_payment;
                    (* … 8 more … *) ]
  in
  Worker.run worker      (* polls until Ctrl-C *)
```

## Architecture

sdk-core owns the protocol, the **replay engine**, and all networking. It does
**not** run your workflow code: it hands your process `WorkflowActivation`s and
takes back `WorkflowActivationCompletion`s (commands). The OCaml side owns
execution.

```
 Temporal server ⇄ gRPC ⇄ sdk-core (Rust static .a)  ⇄ bytes ⇄  OCaml (this repo)
                          replay · state machines ·            decode activation →
                          determinism · networking             emit commands
```

| Path | What |
|------|------|
| `rust/temporal_bridge/` | `staticlib` crate: small C ABI delegating to real `temporalio-sdk-core`. |
| `lib/ffi/` | The FFI boundary (`temporal_ffi` lib): raw bindings + C stubs + the static archive. Exposes only opaque handles and direct-style ops; the `external`s are private. |
| `lib/temporal.ml` (+ `.mli`) | The public SDK — `Activity`/`Workflow`/`Client`/`Worker` + the `Coresdk` codec. No C/Rust here. |
| `examples/ecommerce/` | Example worker (e-commerce order fulfillment): 3 workflows, 10 activities. |
| `scripts/build-bridge.sh` | Builds + stages the Rust staticlib. |
| `.github/workflows/` | `ci.yml`, `release.yml` (per-platform prebuilt artifacts). |

## Build & run

cargo is **decoupled from dune** on purpose (running it inside a dune rule
recompiled the whole sdk-core graph into `_build/` every time). `build-bridge.sh`
builds the `.a` once and stages it; dune links it.

```sh
make build                     # scripts/build-bridge.sh (cargo) + dune build examples/ecommerce/main.exe
temporal server start-dev      # terminal 1
make run                       # terminal 2 — the worker
temporal workflow start --task-queue ecommerce \
  --type OrderWorkflow --workflow-id order-1   # terminal 3
```

Requirements: Rust toolchain, `protoc` (sdk-core compiles protobufs at build
time), OCaml 5.1+ and Eio. No ppx, no protobuf runtime, no compiler pin for
consumers. First build compiles ~300 Rust crates; the static archive is ~108 MB,
linking a ~49 MB native binary. We target `examples/ecommerce/main.exe` (native) because we ship
only the static `.a`, not a shared `dll`.

## Protobuf codec (`Coresdk`)

The bytes crossing the FFI are `coresdk` protobufs. `Coresdk` in `lib/temporal.ml`
is a small, **dependency-free** hand-rolled codec (varint reader/writer, ~90
lines): decode `WorkflowActivation` (`run_id` + detect `InitializeWorkflow`) and
encode `WorkflowActivationCompletion` with a real `CompleteWorkflowExecution`
command. Verified wire-correct against a live server (workflows reach
`COMPLETED`).

This is a deliberate choice for a **library**: no ppx, no protobuf runtime, no
`libprotobuf-dev`, no compiler pin — consumers inherit only Eio + a Rust build.
(Generated types via `ocaml-protoc-plugin` were evaluated and rejected here: the
runtime drags in a ppx dependency and a system `libprotobuf-dev`, and caps the
compiler at OCaml ≤ 5.2 — complexity not worth forcing on consumers for the
handful of messages the worker touches.) Grow `Coresdk` as more message types are
needed, or revisit codegen if the surface gets large.

## Roadmap

1. **Grow `Coresdk`** — decode more activation jobs / encode more commands
   (`ScheduleActivity`, `StartTimer`, `FailWorkflowExecution`) as the worker
   needs them, keeping it dependency-free.
2. **The effect handler** — resume workflow fibers from activations; translate
   `execute_activity`/`sleep`/`await_signal` effects into commands. This drives
   the workflow bodies that M1 only defines.
3. **Activity execution** — the activity poll loop exists; wire it to registered
   activity functions on an Eio domain pool.
4. **Signals, queries, updates** — the interactive layer (`cancel_order`,
   `order_status`, validated `add_line_item`), once the effect handler lands.
5. Replace per-domain `block_on` with Tokio-spawn + the `caml_c_thread_register`
   handoff (see `lib/ffi/stubs.c`) for production.

## Distribution

sdk-core makes the build heavy (Rust + `protoc`, ~108 MB archive, a per-platform
matrix). The fix is what Temporal's own Python/TS/Ruby/.NET SDKs do on top of the
same core: **precompile per `(os, arch)` in CI and ship prebuilt artifacts**
(`release.yml`) so end users never need a Rust toolchain.
