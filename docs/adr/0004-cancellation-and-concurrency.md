# 4. Cancellation and in-workflow concurrency

- Status: Proposed
- Date: 2026-07-15

## Context

The ability to cancel a running workflow is a key feature of Temporal workflows. For
this SDK to maintain parity with the official Temporal SDKs, we need to support
cancellation as well.

Workflow cancellation specifically refers to a _request_ for the workflow to stop 
gracefully. The workflow receives the cancellation request and chooses how to respond. 
It can run cleanup such as refunds or compensations, or it can decline and complete
 normally on its own schedule. 
 
Termination is the forced alternative. It stops workflow execution immediately
without the user code in the workflow being notified. Every official SDK models 
cancellation cooperatively like this. Python catches `asyncio.CancelledError` and
can _"perform some cleanup … or … deny the cancellation entirely"_. Go closes 
`ctx.Done()` and blocking calls return `*CanceledError`. TypeScript raises
`CancelledFailure` and Java raises `CanceledFailure` at await points. Cancellation is the third workflow interaction
type after signals and queries.

Cancellation requires asynchronous execution. The SDK as it stands at
the time of writing of this ADR runs a single logical thread. `execute_activity`, 
`execute_child_workflow`, and `sleep` each block the body until resolution. Currently, a blocked
section of code can't observe a request to abandon the operation it is
waiting on, and it can't express "wait for a signal or a timeout, whichever comes
first." 

Cancellation has meaning only when operations are in flight concurrently and 
some external stimulus can interrupt them. Since we can't have cancellation without
concurrent operation, this ADR covers both as a single architecture. 
This ADR defines a deterministic concurrency model first, backed by the replay log
 and _not_ futures from other libraries like `Eio` or `Lwt`. Cancellation is then
 layered on top of this feature.

This document supersedes the part of ADR-0002 that kept concurrency out of scope.
That ADR rejected the
channel model partly to keep the body "deterministic, not concurrent," and closed by
noting a limitation it left open: _"within a single activation the exact ordering of a
handler firing between two synchronous statements is coarser than a true coroutine
scheduler … Documented, not solved."_ 

A cooperative scheduler keeps the body deterministic while making it concurrent
and resolves that limitation.

### Prior Art in other Language SDKs

Every SDK exposes cooperative, single-threaded concurrency driven by a 
deterministic scheduler:

- **Go**
    * `workflow.Go(ctx, f)` spawns a coroutine; `workflow.ExecuteActivity(...)`
  returns a `Future`
    * `future.Get(ctx, &out)` awaits
    * `workflow.NewSelector(ctx)` with `.AddFuture/.AddReceive/.AddDefault/.Select(ctx)` races heterogeneous
  awaitables.
- **TypeScript**
    * Activities and child workflows return `Promise`s, combined with `Promise.all`
  and `Promise.race`
    * `sleep(ms)`
    * `condition(fn)` and `condition(fn, timeout)` (the
  latter returns a `boolean`: `true` if met before the timeout).
- **Java**
  * `Async.function/procedure` returns a `Promise`, combined with `Promise.allOf` and `Promise.anyOf`
  * `Workflow.await(supplier)` and `Workflow.await(timeout, supplier)` (returns `false` if timed out).
- **Python**
  * Durable asyncio, with `asyncio.gather` / `wait` / `as_completed` over
  activity awaitables
  * `workflow.wait_condition(fn, timeout=…)` (returns `None`,
  raises `asyncio.TimeoutError` on timeout).

The scheduler must be deterministic. Go's dispatcher _"executes coroutines one by one
in deterministic order until all of them are completed or blocked"_
(`Dispatcher.ExecuteUntilAllBlocked`), and command sequence numbers come from a
single monotonic counter advanced in that fixed order. 

For this SDK that means the workflow body can't use OCaml's own concurrency or nondeterministic primitives. That rules out Eio fibers and promises, `Domain.spawn`, `Thread.create` for concurrency, `Eio.Time`
 and Unix wall-clock time, `Random`, `Sys.getenv`, and randomized hashtables for nondeterministic values. 
 Concurrency goes through the proposed `spawn` and `await` values, timers through `Workflow.sleep`, and IO or any nondeterministic
  value through an `activity`. The workflow or activity's `ctx` carries no `Eio` environment, so these are reachable 
  only if user code captures them from an enclosing scope.

Throughout other SDKs, **Cancellation** uses a tree of **cancellation scopes** whose root is the workflow
itself:

- **Scopes** - Cancelling a scope propagates to every activity, timer, and
  child workflow started within it.
- **Cleanup after cancel** runs in a detached scope that ancestor cancellation does
  not reach: Go `workflow.NewDisconnectedContext`, TS `CancellationScope.nonCancellable`,
  Java `newDetachedCancellationScope`, Python `asyncio.shield`.
- **Activity cancellation type** governs how the workflow-side call resolves when an
  activity is cancelled:
  - `TRY_CANCEL`: send the cancel request and resolve the call immediately with a
    cancellation error.
  - `WAIT_CANCELLATION_COMPLETED`: send the request and resolve only after the
    activity acknowledges. The activity must heartbeat, with a heartbeat timeout, to
    receive the request.
  - `ABANDON`: never send a request; resolve immediately.

  Defaults diverge across SDKs. sdk-core's proto default and the Java and Python
  defaults are `TRY_CANCEL` while Go's `ActivityOptions.WaitForCancellation` defaults to
  `false`, which behaves like `TRY_CANCEL`, and Go has no `ABANDON`. 
  TypeScript
  defaults to `WAIT_CANCELLATION_COMPLETED`.
- **Timers and child workflows** resolve their await as a cancellation error when
  cancelled (Go `Sleep` returns `*CanceledError`; TS `sleep` throws `CancelledFailure`).
  `ParentClosePolicy`, already in the SDK, governs a separate case: what happens to a
  child when the parent closes.

### Constraints (unchanged from ADR-0001/0002/0003)

1. **No ppx** - The API stays codec-typed plain values.
2. **The re-run model** - The SDK does not persist continuations across workflow
   tasks. It re-runs the body from the top each activation, resolving effects from a
   history-ordered event log via a replay cursor. Concurrency and cancellation live inside that model.
3. **Determinism and replay-safety** - Every interleaving, cancellation point, and
   command sequence number replays identically forever.
4. **Preserve ADR-0002 delivery** - History-ordered signal and update delivery must still hold across every fiber the scheduler runs.

## Decision

The decision of this ADR is to add the following in two layers above the existing runtime:

1. **Deterministic in-workflow concurrency** - A `future` type, non-blocking `start_*`
   forms, `await` / `await_all` / `await_any`, `spawn` (`async` in other languages), and a `wait_condition`
   timeout, driven by a small deterministic cooperative scheduler that generalizes ADR-0002's single replay cursor.
2. **Cancellation** - A tree of cancellation scopes threaded through `ctx`, with
   `with_cancel_scope` / `with_timeout` / `detached`, a catchable `Canceled` exception
   raised at await points, and the wire commands to cancel activities, timers, and
   child workflows.

The existing blocking API (`execute_activity`, `execute_child_workflow`, `sleep`)
stays unchanged to consumers. Internally, each becomes an `await` of the corresponding `start_*`.

### Part 1 - Deterministic In-Workflow Concurrency
Concurrency primitives that are also the required primitives to support cancellations.

#### API
While this API is defined in the ADR, changes may be required when it comes time to implement. As always, the code in `main` is the source of truth.

```ocaml
type 'a future

(* non-blocking: emit the start command (once) and return immediately *)
val start_activity :
  ?start_to_close:float -> ?max_attempts:int -> ?cancel_type:activity_cancel_type ->
  _ ctx -> ('i, 'o) Activity.t -> 'i -> 'o future
val start_child_workflow :  (* same options as execute_child_workflow *)
  … -> _ ctx -> ('i, 'o) t -> 'i -> 'o future
val start_timer : _ ctx -> float -> unit future      (* a cancellable sleep *)

val await     : _ ctx -> 'a future -> 'a             (* block this fiber until resolved *)
val await_all : _ ctx -> 'a future list -> 'a list   (* all resolve; Promise.all / allOf *)
val await_any : _ ctx -> 'a future list -> 'a        (* first resolves; Promise.race / anyOf *)

val spawn : _ ctx -> (unit -> 'a) -> 'a future       (* spawn a fiber; workflow.Go / Async.function *)

(* existing, unchanged; ~timeout returns whether the predicate held before the
   deadline (like TS condition / Java await), built on start_timer + a scope cancel *)
val wait_condition         : _ ctx -> (unit -> bool) -> unit
val wait_condition_timeout : _ ctx -> timeout:float -> (unit -> bool) -> bool
```

`execute_activity ctx a x` becomes `await ctx (start_activity ctx a x)`, and the
other blocking forms likewise. Fan-out and fan-in read naturally:

```ocaml
let a = start_activity ctx charge order in
let b = start_activity ctx reserve items in
let charge, reservation = match await_all ctx [ a; b ] with [ c; r ] -> c, r | _ -> assert false
```

`await_any` returns the first future to resolve. The losers keep running unless the
surrounding scope is cancelled (Part 2), which matches `Promise.race` semantics.
`await_any` races futures of a single type, since it takes a `'a future list`. The
common mixed race is an operation against a timeout, and `with_timeout` (Part 2)
already covers it. Go handles a general race over differently-typed futures with a
`Selector`. This ADR would need an equivalent `select` for that, and defers it (see
Open questions).

#### Runtime migrates from a single cursor to a cooperative dispatcher
ADR-0002 walks a single cursor over the history log, and the workflow body advances it. Each `execute_activity` or `sleep` in the body asks for the next matching resolution, moving the cursor one step. Concurrency replaces that single walker with a scheduler that drives the log for many fibers at once.

- **Fibers** - The main body is the root fiber. `spawn` registers a new fiber (an
  effectful thunk) with the scheduler. All fibers run under the same `Effect.Deep`[^effect_deep]
  handler.
- **A deterministic ready queue** - The scheduler holds runnable fibers FIFO in
  creation order (root first, then spawn order) and runs them one at a time, each
  until it blocks or completes. This is Go's `ExecuteUntilAllBlocked` behavior. A fiber blocks
  by `await`ing an unresolved operation or by hitting a false `wait_condition`. The
  scheduler parks it on that seq or condition and retains its continuation for the
  duration of this activation only.
- **An event-driven log walk** - The scheduler processes the accumulated event log in
  history order. A resolution event (`Activity_resolved`, `Timer_fired`, or a child
  resolution with seq K) wakes the fiber parked on K and continues it with the
  result. A `Signal` or `Update` event fires its handler at that history position,
  which may flip a `wait_condition` and wake its fiber. After each event the scheduler
  runs all runnable fibers until they block again.
- **Emit-once** - The current runtime re-emits an operation's start command on any re-run where its resolution is not yet in the log. With a single operation outstanding that is at worst benign (see Open questions). Once concurrency allows several operations outstanding at once, re-emitting them is incorrect. So the scheduler records issued seqs in run_state and emits each start exactly once: a re-run triggered by an unrelated job, such as a signal arriving while three activities are outstanding, does not re-issue them.

With a single fiber and no `spawn`, this reduces to ADR-0002's cursor walk. One
fiber makes one blocking demand at a time, and signals arrive as the walk passes
them. Concurrency generalizes that behavior and leaves the single-fiber semantics
unchanged.

Across activations nothing changes. Continuations are still dropped when the activation ends,
and the whole body, all fibers included, is re-derived from the top on the next
activation. Parked continuations live only within an activation, to interleave fibers
as resolutions arrive.

#### Determinism

Replay-safety rests on two facts, both drawn from the official SDKs:

1. **Fibers run in a fixed order** - The ready queue is FIFO in deterministic creation
   order, and each fiber runs to its next block point. `spawn`s are themselves
   deterministic body code. The order in which fibers issue commands is therefore
   identical on every replay.
2. **Sequence numbers follow that order** - The runtime advances the existing per-type
   counters (`act_seq`, `timer_seq`, `child_seq` from ADR-0003) as commands are
   issued, in the deterministic scheduler order.

Rather than reuse `Eio`'s scheduler, the SDK builds its own atop the effect handler.

`Eio`'s scheduler is wall-clock and non-deterministic and would break replay, the same reason ADR-0002 declined to back signals with `Eio.Stream`. The resulting concurrency is deterministic and driven by history.

### Part 2 - Cancellation
Part 2 layers cancellation onto the concurrency model from Part 1. A cancellation comes from a client (`CancelWorkflow`), a `with_timeout` deadline, or an explicit `cancel ()` in the body, and reaches the workflow as a catchable `Canceled` exception the body may handle or ignore.

#### API
The following is a suggested API for cancellation. This is the recommended API at the time of ADR creation. Refer to what's in `main` as the single source of truth for implementation.

```ocaml
exception Canceled of string   (* reason; raised at an await point inside a cancelled scope *)

type activity_cancel_type = Try_cancel | Wait_cancellation_completed | Abandon
   (* default: Try_cancel *)

val with_cancel_scope : 'i ctx -> ('i ctx -> cancel:(unit -> unit) -> 'a) -> 'a
   (* run [f] in a fresh cancellable child scope. [cancel ()] cancels every operation
      started under [ctx'], raising [Canceled] in fibers awaiting them. *)

val with_timeout : 'i ctx -> float -> ('i ctx -> 'a) -> 'a
   (* run [f] in a scope that auto-cancels after [seconds] *)

val detached : 'i ctx -> ('i ctx -> 'a) -> 'a
   (* run [f] detached: ancestor cancellation does not propagate in, so post-cancel
      cleanup completes. *)

val is_cancel_requested : _ ctx -> bool
   (* whether the current scope was asked to cancel (TS cancelRequested / Java
      isCancelRequested); for cooperative checks between awaits. *)
```

And so an activity with a deadline, plus its cleanup:

```ocaml
match
  with_timeout ctx 30.0 (fun ctx -> execute_activity ctx charge_payment order)
with
| charge -> charge
| exception Canceled _ ->
  detached ctx (fun ctx -> execute_activity ctx refund_payment order);  (* survives the cancel *)
  raise (Canceled "charge timed out")
```

Scopes thread through `ctx`. `ctx` gains a `scope` field, and `with_cancel_scope` /
`with_timeout` / `detached` produce a child `ctx` carrying a fresh scope node. Every
operation records the `ctx.scope` it was started under. This is the same
context-plumbing extension ADR-0003 anticipated for the parent workflow id.

#### Semantics

- **Scope tree** - Scopes form a tree whose root scope is the workflow itself. A
  non-detached scope is cancelled when it is cancelled directly or when an ancestor is
  cancelled. A detached scope is cancelled only when cancelled directly.
- **Cancelling a scope** (via `cancel ()`, a `with_timeout` firing, or the workflow
  being cancelled) does two things. For every outstanding operation started under the
  scope subtree, the runtime emits its cancel command once (see below). And it raises
  `Canceled` at the await point of any fiber parked on one of those operations. A
  fiber that `await`s under an already-cancelled scope raises `Canceled` immediately.
- **Cooperative** - `Canceled` is an ordinary exception. Catch it to compensate and
  re-raise, which propagates the cancel, or catch it and complete normally, which
  denies the cancel. Every SDK guarantees this cleanup-or-deny behavior.
- **Recorded in replay-safe history** - The cancellation trigger is always a
  deterministic event: a body call to `cancel ()`, a `Timer_fired` for a
  `with_timeout`, or the `CancelWorkflow` job. Which scopes are cancelled, and where
  each fiber raises `Canceled`, is a pure function of history.

#### Activity cancellation

`start_activity` and `execute_activity` gain `?cancel_type`, defaulting to
`Try_cancel`. That matches the sdk-core proto default and the Go, Java, and Python
defaults; TypeScript's `Wait_cancellation_completed` default is the outlier. The
option sets `ScheduleActivity.cancellation_type`. When the enclosing scope is
cancelled:

- **`Try_cancel`** - emit `RequestCancelActivity` and resolve the await with
  `Canceled` immediately.
- **`Wait_cancellation_completed`** - emit `RequestCancelActivity`, then resolve with
  `Canceled` only when the activity's `ResolveActivity` arrives (as `Cancelled`, or as
  its real outcome if the activity ignored the request). This depends on activity
  heartbeating, which the SDK does not have yet, since activities are currently pure
  functions. Until heartbeating exists, a cancel request likely reaches only an
  activity that has not yet started (confirm against sdk-core).
- **`Abandon`** - resolve with `Canceled` immediately and emit no cancel command.

#### Timer and child-workflow cancellation

- **Timer** - Cancelling a scope that has an outstanding `start_timer` or `sleep`
  (seq K) emits `CancelTimer { seq = K }`. sdk-core does not deliver a `FireTimer` for
  a cancelled timer, so the runtime resolves timer K locally as cancelled and raises
  `Canceled` in its awaiter. The runtime records this local resolution so the
  deterministic re-run reproduces it. (Confirm against sdk-core's timer state machine
  that no timer resolution job is sent on cancel.)
- **Child workflow** - Emit `CancelChildWorkflowExecution { child_workflow_seq = K }`.
  The child's completion returns `ChildWorkflowResult.cancelled`, already decoded as
  `Child_cancelled` in `replay_state` and currently flattened to a failure. This ADR
  raises a distinct `Canceled` for it, which answers ADR-0003's open question about
  whether a cancelled child raises a distinguishable exception. A
  `?wait_for_cancellation` option, from Go's `ChildWorkflowOptions.WaitForCancellation`,
  governs whether `await` waits for the child to actually end; it defaults to `false`.
- **`ParentClosePolicy` stays unchanged** - It governs a child when its parent closes,
  a separate case from the explicit cancellation of a running child described above.

#### Workflow-level cancellation

Decode the `CancelWorkflow` activation job. On arrival, the runtime cancels the root
scope, which cascades by the rules above. The main body observes it as `Canceled`
raised at its current await. If the body catches it and completes normally, the
workflow completes normally. If `Canceled` propagates out of the main body, the
runtime emits `CancelWorkflowExecution` and the workflow closes as canceled.
Concretely, `run_workflow`'s `exnc` branch sends `CancelWorkflowExecution` for an
escaping `Canceled` and `Fail_workflow_execution` for any other exception, as it does
today.

### Wire additions (coresdk)

Field tags pinned against the vendored prost-generated protos
(`coresdk.workflow_commands` / `coresdk.workflow_activation` /
`coresdk.activity_result`):

**Decode**

- `CancelWorkflow`: `WorkflowActivationJob.cancel_workflow = 6`, field `reason = 1`
  (string). Cancels the root scope.
- Extend `ResolveActivity` (`= 8`, already decoded) to handle
  `ActivityResolution.result.cancelled = 3` (`Cancellation { failure = 1 }`). Today
  only `completed = 1` and `failed = 2` are mapped, so a cancelled activity falls
  through to `R_fail "unknown activity resolution"`.
- `ResolveChildWorkflowExecution.cancelled = 3` is already decoded as `Child_cancelled`,
  and it now raises `Canceled`.

**Encode**

- `RequestCancelActivity`: `WorkflowCommand.request_cancel_activity = 4`, field
  `seq = 1`.
- `CancelTimer`: `WorkflowCommand.cancel_timer = 5`, field `seq = 1`.
- `CancelWorkflowExecution`: `WorkflowCommand.cancel_workflow_execution = 9`, empty
  message.
- `CancelChildWorkflowExecution`: `WorkflowCommand.cancel_child_workflow_execution = 12`,
  field `child_workflow_seq = 1` (plus `reason = 2`).
- Extend `ScheduleActivity` with `cancellation_type = 13` (`ActivityCancellationType`:
  `TryCancel = 0`, `WaitCancellationCompleted = 1`, `Abandon = 2`).

## Consequences

- **👍** Cancellation lands with the full cooperative model users expect from other
  SDKs: cleanup, deny, detached cleanup, timeouts, and a catchable `Canceled` that
  distinguishes a cancelled child or activity from a failed one.
- **👍** In-workflow concurrency unblocks a large class of real workflows: parallel
  fan-out and fan-in, racing, and "signal or timeout" via `wait_condition_timeout` and
  `with_timeout`. The blocking API cannot express any of these.
- **👍** The cooperative scheduler resolves ADR-0002's documented coarse-ordering
  limitation by defining handler and fiber interleaving precisely.
- **👍** The design reuses the existing seq-keyed event log and per-type counters. With
  one fiber the scheduler behaves identically to today's cursor walk.
- **👎** This is a large change to the runtime. The single-cursor walk becomes a fiber
  scheduler that holds multiple continuations within an activation and drives the log
  as an event loop. `run_workflow` grows materially.
- **👎** It revisits ADR-0002's "the body is not concurrent" stance. The body is now
  concurrent, though deterministically so, which is a more complex mental model for
  users and maintainers even with replay-safety preserved.
- **👎** `Wait_cancellation_completed` is only partly useful until activity
  heartbeating exists. The ADR documents this and defaults to `Try_cancel`.
- **👎** Emit-once now requires tracking issued seqs in `run_state`, a small but real
  addition to the per-run state the runtime keeps.

## Alternatives considered

- **Poll-only cancellation, no concurrency** - Deliver `CancelWorkflow` as a flag the
  body checks via `is_cancel_requested`, with no scopes and no interruption of
  in-flight operations. _Rejected_: it cannot cancel a running activity, timer, or
  child, cannot express timeouts, and diverges from every SDK's cooperative cancellation
  model. The `is_cancel_requested` flag survives as one affordance inside the scope
  model.
- **Persisted continuations (sticky resume)** - Rather than re-run the body from the
  top, keep each workflow instance in memory and resume its continuations across
  activations, as the real SDKs' runtimes do. _Rejected_: it abandons the project's defining
  re-run model (ADR-0001, 0002, 0003) for a much larger runtime. The cooperative
  scheduler achieves concurrency within the re-run model, holding continuations only
  within a single activation.
- **Eio fibers for the scheduler** - _Rejected_: Eio's scheduler is non-deterministic
  and wall-clock driven, and workflow scheduling must be a pure function of history.
  The SDK builds a deterministic dispatcher over its own effect handler, the same
  conclusion ADR-0002 reached about Eio channels.
- **Forced (non-cooperative) cancellation** - _Rejected_: Temporal cancellation is
  cooperative by definition. A forced kill is termination, a server-side action with
  no workflow-side API.
- **`select`/`Selector` as the primary racing API** (Go-style) - _Deferred_ rather than
  rejected. `await_any` covers same-typed future races and `with_timeout` covers an
  operation against a timeout, so a typed heterogeneous `select` can be added later
  over the same futures without reshaping the model.

## Open questions (for implementation)

- **Within-activation scheduling order** - The precise, documented rule for which fiber
  runs next and when the log advances, so the interleaving is pinned. This is the
  analogue of ADR-0002's "which signals are delivered at which checkpoint" question.
- **Emit-once for a single outstanding operation** - Confirm whether today's
  single-fiber path already re-emits a start command on a signal-triggered re-run, and
  whether sdk-core tolerates it, independent of the concurrency change.
- **Timer cancel resolution** - Confirm sdk-core sends no activation job for a
  cancelled timer, so the local resolution is the correct model.
- **`Canceled` and the failure taxonomy** - `Canceled` is the first concrete member of
  ADR-0001's deferred `Failure` taxonomy (`CanceledFailureInfo`). Decide how much
  structured detail, such as reason and cause chain, to carry now versus later when
  the ADR-0001 failure converter lands.
- **`await_any` and losers** - Whether to keep pure `Promise.race` semantics, where
  losers run on, or offer a structured `race` that cancels the losing branch in a
  child scope.
- **Scope identity across replay** - Confirm a deterministic scope id, such as a scope
  counter like the operation seqs, suffices, and that `with_timeout`'s internal timer
  seq interleaves cleanly with body timers.
- **Heartbeating** - `Wait_cancellation_completed` and activity-side cancellation
  observation depend on the activity heartbeat and context, which do not exist yet.
  Sequence that work relative to this ADR.
- **`wait_condition_timeout` naming and return** - Confirm the two-function split
  (`wait_condition : … -> unit`, `wait_condition_timeout : … -> bool`) over a single
  `?timeout` entry point. This matches Java's two `await` overloads rather than
  TypeScript's single one.

## References

Official SDK behavior treated as source of truth:

- **Cancellation concepts** - <https://docs.temporal.io/evaluate/development-production-features/interrupt-workflow>, <https://docs.temporal.io/parent-close-policy>
- **Go** - <https://docs.temporal.io/develop/go/cancellation>, <https://docs.temporal.io/develop/go/workflows/selectors>, <https://docs.temporal.io/develop/go/go-sdk-multithreading>; source `internal/internal_workflow.go`, `internal/context.go`, `internal/activity.go`
- **TypeScript** - <https://docs.temporal.io/develop/typescript/workflows/cancellation-scopes>, <https://typescript.temporal.io/api/classes/workflow.CancellationScope>; source `packages/common/src/activity-options.ts`
- **Java** - <https://docs.temporal.io/develop/java/cancellation>; source `temporal-sdk/.../workflow/CancellationScope.java`, `activity/ActivityOptions.java`, `activity/ActivityCancellationType.java`
- **Python** - <https://docs.temporal.io/develop/python/cancellation>; source `temporalio/workflow/_activities.py`, `_context.py`

## Footnotes

[^effect_deep]: `Effect.Deep` is a part of the OCaml standard `Effect` library added as a language feature with OCaml 5.0
