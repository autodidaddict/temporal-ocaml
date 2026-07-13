# 3. Child workflows

- Status: Proposed
- Date: 2026-07-13

## Context

A workflow can start *another* workflow as a **child**: a separate execution, with
its own workflow id, run id, and history, whose lifecycle is tied to the parent.
Children are how Temporal composes large workflows out of smaller ones — the
canonical example being an order workflow that delegates fulfilment to a shipment
workflow. The e-commerce example already anticipates this: `OrderWorkflow` runs
shipment as an *activity* with a standing `(* later: start ShipmentWorkflow as a
child instead of an activity *)` note, and `ShipmentWorkflow` is already defined
and registered.

A child differs from an activity in ways that matter to the design:

- **It is a workflow.** It has typed input and output, a deterministic body,
  signals, queries, updates, timers, and its own activities — everything a
  top-level workflow has. It is not a plain function call.
- **It has a two-stage lifecycle.** Starting a child resolves *twice*: first a
  **start** resolution (the child was created — here is its run id — or it could
  not be created, e.g. the id already exists), then later a **completion**
  resolution (the child finished: succeeded with a result, failed, or was
  cancelled). An activity resolves exactly once. This second resolution point is
  the central new mechanic.
- **Its fate is bound to the parent.** When the parent closes, a **parent-close
  policy** decides whether the child is terminated, abandoned, or asked to cancel.

This must all fit the runtime the SDK already has (ADR-0001, ADR-0002):

1. **The effect-handler re-run model.** The SDK does not persist continuations
   across workflow tasks. It re-runs the body from the top on each activation,
   resolving effects (`execute_activity`, `sleep`, signal/update delivery) from an
   accumulated, history-ordered event log via a replay cursor. Child workflows
   must live inside that model.
2. **No ppx.** Definitions are plain typed values and the API stays codec-typed
   (ADR-0001). A child must be referred to type-safely without code generation.
3. **Determinism / replay-safety.** A child's workflow id, and the interleaving of
   its two resolutions with the rest of history, must replay identically forever.

### What the other SDKs do

The dominant SDKs converge on two entry points:

- **execute-child** (`executeChildWorkflow` / `workflow.execute_child_workflow` /
  Go's `ExecuteChildWorkflow(...).Get(...)`) — start the child *and* await its
  result in one call. This is the overwhelmingly common shape.
- **start-child + handle** (`startChild` → a handle) — start the child, get a
  handle carrying its run id once it has started, and separately await the result,
  **signal** it, or **cancel** it. This enables fire-and-forget children and
  parent→child signalling.

Both reference the child by its typed definition (a workflow type/name plus
input/output types), which is exactly what our `Workflow.t` already is.

## Decision

Add child workflows in two layers, mirroring how activities and updates were
staged. Adopt **`execute_child_workflow` (start + await) as the primary and first
API**, referring to the child by its typed `Workflow.t`. Design the **handle-based
`start_child_workflow`** and **`signal_child`** as the granular form but **defer**
them, along with **cancellation**, to follow-up work.

### API — reference the child by its `Workflow.t` (no ppx)

A child is named and typed by the very value used to define and register it, so
the parent gets the child's codecs for free and the compiler checks the argument
and result types — the same trick `execute_activity` uses with `Activity.t`:

```ocaml
(* in the Workflow module *)
type parent_close_policy = Terminate | Abandon | Request_cancel

val execute_child_workflow :
  ?workflow_id:string ->                    (* default: deterministic, see below *)
  ?task_queue:string ->                     (* default: the parent's task queue *)
  ?parent_close_policy:parent_close_policy ->(* default: Terminate *)
  ?execution_timeout:float ->
  ?run_timeout:float ->
  _ ctx ->
  ('i, 'o) t ->                             (* the child's own typed definition *)
  'i ->
  'o
```

`OrderWorkflow` then reads, replacing the activity and its `later:` note:

```ocaml
let shipment =
  execute_child_workflow ctx shipment_workflow (o.items, o.ship_to)
```

`execute_child_workflow` emits a start command, awaits the child's *start* and then
its *completion*, and returns the decoded output — or raises at the call site if
the child fails (see Failures). It is the exact analogue of `execute_activity`: a
single blocking call whose two-stage nature is hidden inside the effect.

The child's `Workflow.t` supplies only its **name and codecs**. The child need not
be registered on the *same* worker — it runs wherever its task queue is polled.
The parent references the definition purely for type-safety and naming.

### Execution semantics — two resolutions over the existing cursor

The runtime already resolves `execute_activity` by: emit a `Schedule_activity`
command with a per-type sequence number `s`; on each re-run, if the log holds
`Activity_resolved (s, _)` at the cursor, consume it and resume with the result,
otherwise (first time) emit the command and suspend. Child workflows use the same
shape, but demand **two** log entries for one call.

Extend the event log with two seq-keyed events (the existing `resolution =
R_ok of payload | R_fail of string` is reused for completion):

```ocaml
type child_start = Child_ok of string (* run_id *) | Child_start_failed of string
type event =
  | ...
  | Child_started  of int * child_start   (* seq, start outcome  *)
  | Child_resolved of int * resolution    (* seq, completion outcome *)
```

`execute_child_workflow` (sequence `s`) then, on each re-run, delivers any pending
signals/updates at the cursor (as every effect already does) and:

1. **Neither resolution present** → emit `StartChildWorkflowExecution { seq = s;
   workflow_id; workflow_type; input; task_queue; parent_close_policy; … }` and
   suspend. (Emitted once, exactly like `Schedule_activity`.)
2. **`Child_started (s, Child_start_failed msg)`** → consume it and raise at the
   call site (the child could not be created, e.g. the id already exists).
3. **`Child_started (s, Child_ok run_id)` but no completion yet** → consume the
   start event and suspend: the child is running. The start command is *not*
   re-emitted (the start event's presence records that it was already sent).
4. **`Child_resolved (s, R_ok payload)`** → consume it and resume with the decoded
   result.
5. **`Child_resolved (s, R_fail msg)`** → consume it and raise at the call site.

Because both resolutions are keyed by `s`, they are order-independent with respect
to the body's fixed demand sequence — the same property that lets activity
resolutions live in the history-ordered log without special ordering. Signals and
updates that arrive *between* a child's start and completion are delivered by the
existing `deliver_pending` at each checkpoint, so their interleaving with the
child's resolutions replays deterministically (the guarantee ADR-0002 established).

The runtime already uses **independent per-command-type sequence counters**
(`act_seq`, `timer_seq`) — an activity with `seq = 1` and a timer with `seq = 1`
coexist because core matches a resolution to its command within a job type, as the
`OrderWorkflow` smoke path (activities + a timer) demonstrates. Child workflows add
a `child_seq` in the same spirit; `ResolveChildWorkflowExecutionStart` and
`ResolveChildWorkflowExecution` both carry that seq.

### Determinism — the child's workflow id

`StartChildWorkflowExecution.workflow_id` is set by *lang*, not the server, so it
must be chosen **deterministically** or replay will diverge. Two cases:

- **Explicit** `?workflow_id` — the caller supplies a stable id (the usual choice
  when the child id is meaningful, e.g. `"shipment-" ^ order_id`).
- **Default** — a deterministic id derived from the parent's workflow id and the
  child sequence number (e.g. `"<parent_workflow_id>/<child_seq>"`), so it is
  identical on every replay and unique per child.

The default requires the **parent's workflow id to be available in `ctx`**, which
it is not today (`ctx` carries the task queue, continue-as-new signals, and the
input encoder, but not the workflow/run id). Threading the parent workflow id
through `Initialize_workflow` into `ctx` is part of implementing this — see Open
questions.

### Parent-close policy and options

`?parent_close_policy` maps to `child_workflow.ParentClosePolicy`:

- `Terminate` → `PARENT_CLOSE_POLICY_TERMINATE` (**default**, matching Temporal) —
  the child is terminated when the parent closes.
- `Abandon` → `PARENT_CLOSE_POLICY_ABANDON` — the child keeps running independently.
- `Request_cancel` → `PARENT_CLOSE_POLICY_REQUEST_CANCEL` — the child is asked to
  cancel.

`?execution_timeout` / `?run_timeout` map to the corresponding `Duration` fields
(encoded like the activity `start_to_close` already is). `workflow_id_reuse_policy`,
`retry_policy`, `cron_schedule`, memo, search attributes, and `versioning_intent`
are left at their server/core defaults for the first cut and can be surfaced later
without changing the shape.

### Failures

A child that does not succeed **raises at the call site**, exactly as a failed
activity does, so a parent can wrap `execute_child_workflow` in `try/with` to
compensate (saga). Mapping from `ChildWorkflowResult`:

- `completed { result }` → resume with the decoded `'o`.
- `failed { failure }` → raise; the message is the deepest cause in the failure
  chain, reusing the existing `failure_message` extraction (as activity failures
  already do).
- `cancelled { failure }` → for the first cut, treated as a failure (raises).
  Modelling cancellation as a distinct, catchable outcome belongs with the
  cancellation work (deferred).

A **start** failure (`ResolveChildWorkflowExecutionStartFailure`, cause
`WORKFLOW_ALREADY_EXISTS`) likewise raises. This connects to ADR-0001's failure
taxonomy: the wrapper is a `ChildWorkflowExecutionFailureInfo`, produced by
sdk-core and read on the lang side; richer structured mapping (typed details,
chained cause) rides on the same seam ADR-0001 left open.

### Handle-based API and signalling (deferred)

The granular form is designed but not in the first cut:

```ocaml
type ('i, 'o) child_handle
val start_child_workflow : … -> _ ctx -> ('i,'o) t -> 'i -> ('i,'o) child_handle
  (* awaits the start resolution; returns a handle carrying the child's run id *)
val child_result   : _ ctx -> ('i,'o) child_handle -> 'o     (* awaits completion *)
val child_workflow_id : ('i,'o) child_handle -> string
val child_run_id      : ('i,'o) child_handle -> string
val signal_child   : _ ctx -> ('i,'o) child_handle -> 'a Signal.t -> 'a -> unit
```

`start_child_workflow` awaits only the *start* resolution (steps 1–3 above) and
returns a handle; `child_result` awaits the *completion* (steps 4–5). This splits
the single `execute_child_workflow` effect into two, over the same two log events —
so `execute_child_workflow` is definable as `start` then `child_result`, and can be
introduced first without blocking the handle form. `signal_child` needs an
additional command (`SignalExternalWorkflowExecution`) and its resolution job, and
is deferred with it.

### Wire additions (coresdk)

Two new commands and two new activation jobs, field numbers pinned against the
vendored `workflow_commands.proto` / `workflow_activation.proto`
(`temporalio-common` / sdk-core), the same way timers, continue-as-new, queries,
and updates were:

- **Encode** `StartChildWorkflowExecution` — `WorkflowCommand.start_child_workflow_execution = 11`;
  fields `seq=1`, `namespace=2`, `workflow_id=3`, `workflow_type=4`, `task_queue=5`,
  `input=6` (repeated Payload), timeouts `7/8/9` (Duration), `parent_close_policy=10`.
  (`CancelChildWorkflowExecution = 12` is deferred with cancellation.)
- **Decode** `ResolveChildWorkflowExecutionStart` —
  `WorkflowActivationJob.resolve_child_workflow_execution_start = 10`; `seq=1`, and
  a status oneof `succeeded=2 { run_id=1 }` / `failed=3 { workflow_id=1,
  workflow_type=2, cause=3 }` / `cancelled=4 { failure=1 }`.
- **Decode** `ResolveChildWorkflowExecution` —
  `WorkflowActivationJob.resolve_child_workflow_execution = 11`; `seq=1`, `result=2`
  = `ChildWorkflowResult { completed=1 { result=1 }, failed=2 { failure=1 },
  cancelled=3 { failure=1 } }`.

## Consequences

- **+** Composition: large workflows decompose into smaller registered workflows,
  and the `OrderWorkflow` → `ShipmentWorkflow` `later:` note is realised with a
  one-line, type-checked call.
- **+** No ppx and full type-safety: a child is referenced by its `Workflow.t`, so
  input/output codecs and names come for free — the same ergonomics as
  `execute_activity`.
- **+** Reuses the runtime: the two resolutions are just two more seq-keyed events
  in the existing history-ordered log, consumed by the existing cursor; signals and
  updates interleave deterministically via the machinery ADR-0002 already built.
- **−** The two-stage lifecycle is genuinely new: one call consumes two log
  entries, so `run_workflow`'s effect handler grows a case that is more intricate
  than the single-resolution activity path.
- **−** Determinism now depends on a lang-generated child id, which forces the
  parent workflow id into `ctx` — a small but real change to context plumbing.
- **−** Scope is deliberately partial: cancellation, parent→child signalling, and
  the handle API are designed here but deferred, so the first cut cannot signal or
  cancel a child, and a cancelled child surfaces as a plain failure.

## Alternatives considered

- **Model a child as an activity-like single resolution**, ignoring the start
  event. Rejected: the start resolution carries the child's run id and the
  already-exists failure, and start-vs-completion is exactly what distinguishes a
  child; collapsing them would drop the run id (needed for the handle API and
  signalling) and mishandle start failures.
- **A separate `Child.define` distinct from `Workflow.define`.** Rejected:
  redundant. A child *is* a workflow; reusing `Workflow.t` gives the codecs and
  name with zero new surface, and the same value can be run as a top-level workflow
  or as a child.
- **Handle-only API** (`start_child_workflow` + `child_result`, no `execute_`
  convenience). Rejected as the primary: the overwhelmingly common case is
  start-and-await, and forcing every caller to thread a handle is boilerplate. The
  handle form is retained as the deferred granular API, with `execute_` as sugar
  over it.
- **ppx-generated child stubs** (mirroring typed proxies in some SDKs). Rejected by
  the project's dependency stance, as in ADR-0001.

## Open questions (for implementation)

- **Parent workflow id in `ctx`.** Confirm `Initialize_workflow` exposes the parent
  workflow id (and run id) and thread them through so the default child id is
  deterministic; decide the exact default id scheme.
- **`child_seq` allocation** relative to `act_seq` / `timer_seq` — confirm a
  private counter suffices and matches core's per-job-type seq matching for the two
  child resolution jobs.
- **Cancelled children.** Whether the first cut raises a *distinguishable*
  cancellation exception (so a parent can tell cancel from failure) even before
  general cancellation lands.
- **Result codec on failure/cancel.** `ChildWorkflowResult.cancelled` carries a
  `Failure`; decide how much of its structured detail to surface now vs. on the
  ADR-0001 failure-converter seam.
- **`parent_close_policy = Abandon` and worker lifetime** — document the
  replay/eviction implications of an abandoned child outliving its parent's cache
  entry.
- **Duplicate-id / reuse policy** surfacing (`workflow_id_reuse_policy`) once a
  concrete need appears.
