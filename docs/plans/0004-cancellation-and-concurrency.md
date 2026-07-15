# Implementation plan: cancellation and in-workflow concurrency

Implements [ADR-0004](../adr/0004-cancellation-and-concurrency.md). The code in
`main` is the source of truth; this plan is the intended sequence, and it will shift
as phases land.

## Guiding principles

- **Concurrency before cancellation**, matching the ADR's dependency order.
- **Every phase merges green on its own** and keeps existing workflows working.
- **Build the replay test harness before touching the engine.** There is no way to
  unit-test `run_workflow` today (only `test_codec.ml`), and the ADR's whole value is
  determinism. The harness lets every later phase be test-driven and lets us encode
  the ADR's scenarios (ADR-0002 signal ordering, emit-once, cancellation replay) as
  assertions.
- **Spike the "confirm against sdk-core" open questions early**, before code depends
  on them.

## Decisions

- **Plain `int` seqs, no phantom types.** The three counters (`act_seq`, `timer_seq`,
  `child_seq`) stay plain `int`, scoped by their event constructors. A phantom `Seq.t`
  buys little here and would complicate the scheduler's waiter tables. Revisit only if
  a later phase passes bare seqs around widely enough to warrant it.

## Phases

### Phase 0 - Test harness and wire plumbing

No behavior change. Foundation for everything after.

- Build a deterministic harness that drives `run_workflow` with synthetic activation
  jobs and asserts the emitted commands. Encode current behavior as characterization
  tests: activity sequence, timer, ADR-0002 signal ordering, query mode, update
  accept and reject.
- Add the new variants to `coresdk.ml`, encode and decode only, nothing emitted yet:
  decode `CancelWorkflow = 6` and `ResolveActivity` status `Cancelled = 3`; add
  encoders for `RequestCancelActivity = 4`, `CancelTimer = 5`,
  `CancelWorkflowExecution = 9`, `CancelChildWorkflowExecution = 12`; add
  `ScheduleActivity.cancellation_type = 13` (default `TryCancel`). Round-trip tests.
- Spike via `livetest.sh`: does a cancelled timer produce no `FireTimer` job? does
  sdk-core tolerate a re-emitted command for an outstanding operation? These answer
  two open questions.

### Phase 1 - Scheduler skeleton (single fiber)

Behavior-preserving refactor.

- Rewrite `run_workflow`'s core from the single-cursor walk into a cooperative
  scheduler over `Effect.Deep`: a ready queue of fibers, an event-driven log walk,
  per-type waiter tables keyed by seq. With one fiber it must reproduce Phase 0's
  tests exactly.
- Prototype the deterministic scheduler in isolation first, then wire it in. This is
  the highest-risk phase.
- Pins the "within-activation scheduling order" open question.

### Phase 2 - Futures and fan-out

- Add `type 'a future`, `start_activity` / `start_child_workflow` / `start_timer`,
  `await`, `await_all`, `await_any`. Redefine `execute_activity` / `sleep` /
  `execute_child_workflow` as `await (start_*)`.
- `start_*` emits its command eagerly and does not block, so starting several then
  `await_all` gives real parallel activities without `spawn`.
- Implement emit-once (an issued-seq set in `run_state`), now that several operations
  can be outstanding at once.
- Decide `await_any` loser semantics (open question).

### Phase 3 - spawn

- Add `spawn : _ ctx -> (unit -> 'a) -> 'a future` for concurrent control flow, a
  fiber running its own sequential await steps. Finalizes the multi-fiber scheduler
  and its determinism (fiber order gives stable seq assignment).

### Phase 4 - Cancellation scopes (core)

- Thread a `scope` through `ctx`; scope tree with deterministic scope ids;
  `exception Canceled of string`.
- `with_cancel_scope ctx (fun ctx' ~cancel -> ...)`: `cancel ()` marks the scope and
  its non-detached descendants cancelled, emits cancel commands for outstanding
  operations in the subtree, and raises `Canceled` in parked fibers.
- Each operation records its `ctx.scope` so the scheduler can cancel by subtree.
  Resolves the scope-identity open question.

### Phase 5 - with_timeout, detached, wait_condition_timeout

- `with_timeout` is a scope plus an internal timer that auto-cancels. `detached` is a
  scope that ancestor cancellation does not reach. `wait_condition_timeout` becomes
  clean here, since it can cancel the losing timer. Decide the single-`?timeout` entry
  point versus the two-function split (open question).

### Phase 6 - Activity, timer, and child cancellation specifics

- Activity `?cancel_type` behaviors: `Try_cancel` and `Abandon` now,
  `Wait_cancellation_completed` limited until heartbeating exists (documented). Timer
  local-cancel resolution. Child `CancelChildWorkflowExecution` raises a distinct
  `Canceled`; add `?wait_for_cancellation`.

### Phase 7 - Workflow-level cancellation

- Deliver `CancelWorkflow` as an order-sensitive history event that cancels the root
  scope. Uncaught `Canceled` from the main body emits `CancelWorkflowExecution`; the
  `exnc` branch distinguishes it. Add `is_cancel_requested`.

### Phase 8 - Integration and finalize

- Drive real scenarios against a dev server: cancel-with-cleanup, activity timeout,
  parallel fan-out. Resolve the remaining "confirm against sdk-core" questions. Add a
  concurrency and saga example to `examples/ecommerce`. Flip the ADR to Accepted and
  fold in the resolved open questions.

## Open questions carried from the ADR

Each is resolved by the phase noted:

- Within-activation scheduling order (Phase 1).
- Emit-once for a single outstanding operation (Phase 0 spike, Phase 2).
- Timer cancel resolution (Phase 0 spike, Phase 6).
- `Canceled` and the failure taxonomy (Phase 4 onward).
- `await_any` and losers (Phase 2).
- Scope identity across replay (Phase 4).
- Heartbeating dependency for `Wait_cancellation_completed` (Phase 6, deferred).
- `wait_condition_timeout` naming and return (Phase 5).
