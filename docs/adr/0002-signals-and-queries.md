# 2. Signals and queries

- Status: Proposed
- Date: 2026-07-07

## Context

Workflows so far are closed systems: an input goes in, activities and timers run,
a result (which can be positive, a failure, or continue-as-new) comes out. **Signals** and **queries**
are the means through which the outside world interacts with workflows.

- A **signal** is asynchronous input delivered into a *running* workflow (e.g.
  `cancelOrder`, `addItem`). It has no return value, may mutate workflow state, and is
  recorded in history. Signals are semantically similar to messages delivered 
  to actors in an actor system or Elixir GenServers via mailbox.
- A **query** is a synchronous, **read-only** inspection of a running (or even
  closed) workflow's state (e.g. `orderStatus`). It must not mutate state, run
  activities, or emit commands, and it is *not* recorded in history. A query
  has _consistent_ and _safe_ access to state at the time the query handler
  runs.

Three constraints shape the design:

1. **No ppx** The dominant SDK ergonomics rely on code generation — Python's
   `@workflow.signal` / `@workflow.query` decorators, the modern Rust SDK's
   `#[signal]` / `#[query]` attribute macros. Ppx has been ruled out (see
   [ADR-0001](0001-payload-codecs.md)), so that's a last-ditch option if nothing
   else works.
   
   Both of those SDKs, however, expose a *manual* form of signal handling underneath (Rust's
   `SignalDefinition` / `QueryDefinition` traits on marker structs), and that
   manual form is a natural fit with the codegen-free OCaml ergonomics we want
   in this project.
2. **The effect-handler runtime** The SDK don't persist continuations
   across workflow tasks. Instead, it re-runs the workflow body from the beginning on each
   activation, resolving effects from accumulated history. Any signal/query
   design must live inside that re-run model.
3. **Determinism / replay-safety must be preserved** Reading signal-mutated
   state (`if !x = 12 then …`) has to replay identically forever.

### What the other SDKs do

They other SDKs seem to have converged toward a newer pattern. Python uses decorators,
TypeScript uses `defineSignal` and `setHandler`. These SDKs as well as Java all
adopt the same 3 design options:

- A **typed signal/query definition**
- A **handler** that runs when the signal arrives and mutates workflow state
- **`wait_condition(predicate-over-state)`** for the body to block until state
  satisfies a condition

The older **channel/stream** model used by Go's `workflow.GetSignalChannel` + selector, 
and the original Rust `make_signal_channel` is falling out of favor (at least as far as
searches indicate). The current Rust SDK replaced this with `SignalDefinition` + 
`ctx.wait_condition(|state| …)`.

In every one of these models, signal **reception is asynchronous**. The
body never has to pause to *receive* a signal. The handler fires on its own
whenever the signal arrives (even mid-activity). The body separately chooses when
to *react* to the resulting state. Reception and reaction are decoupled.

Because order is preserved, any examination of workflow state in the main worklow
body will be consistent with respect to activity and timer executions.

## Decision

Adopt the **handler + `wait_condition`** model (below, "Option A") as the
primary and only signal API, with **history-ordered delivery** as the execution
semantics. Handlers are synchronous state mutators. Queries run in a read-only
replay. Handling Temporal _update_ messages is sketched out but deferred and not in scope.

### API — Typed Definitions + Explicit Registration (Option A)

Signals and queries become **first-class, codec-typed values**, exactly like
`Activity.t` and `Workflow.t`, and the body registers handlers explicitly:

```ocaml
module Signal : sig
  type 'a t
  val define : name:string -> 'a Codec.t -> 'a t
end

module Query : sig
  type ('input, 'output) t
  val define :
    name:string -> input:'input Codec.t -> output:'output Codec.t ->
    ('input, 'output) t
  (* a [unit]-input convenience is provided for the common no-arg query *)
end

(* in the Workflow module, all taking [_ ctx] (they ignore the input type): *)
val on_signal      : _ ctx -> 'a Signal.t -> ('a -> unit) -> unit
val on_query       : _ ctx -> ('a, 'b) Query.t -> ('a -> 'b) -> unit
val wait_condition : _ ctx -> (unit -> bool) -> unit
```

A workflow reads:

```ocaml
let cancel_order = Signal.define ~name:"cancelOrder" Codec.unit
let add_item     = Signal.define ~name:"addItem" item_codec
let order_status = Query.define  ~name:"orderStatus" ~input:Codec.unit ~output:Order.codec

let order_workflow =
  Workflow.define ~name:"OrderWorkflow" ~input:Order.codec ~output:Codec.string
    (fun ctx (o : order) ->
      let items = ref o.items and cancelled = ref false in
      Workflow.on_signal ctx cancel_order (fun () -> cancelled := true);
      Workflow.on_signal ctx add_item     (fun it -> items := it :: !items);
      Workflow.on_query  ctx order_status (fun () ->
        if !cancelled then "cancelled"
        else Printf.sprintf "%d items" (List.length !items));
      Workflow.wait_condition ctx (fun () -> !cancelled || ready !items);
      if !cancelled then "order cancelled" else fulfil ctx !items)
```

Handlers are ordinary closures over body-local `ref`s. `wait_condition` is one
more effect that works just like the runtime already has: _evaluate the predicate; if it holds, resume; if not, suspend the run (drop the
continuation) and re-evaluate on the next activation._

### Why Option A, and not the channel model (Option B)

Option B would hand the body an explicit stream to pull from:

```ocaml
let chan = Workflow.signal_channel ctx add_item in
let rec drain acc = match Workflow.receive ctx chan with it -> drain (it :: acc) in ...
```

It was rejected as the primary API for these reasons:

- **Ergonomics / state-machine fit.** The overwhelmingly common workflow shape is
  “fold signals into state, then act on aggregate state”: *wait until cancelled,
  or 3 items collected, or a deadline passes*. `wait_condition (fun () -> …)`
  expresses that in one line and keeps the body's sequential control flow
  readable — do an activity, then wait for a condition, then another activity.
  Channels force you to hand-roll the aggregation and structure the code around
  explicit `receive` points; a multi-signal condition becomes an awkward select
  over several channels.
- **Async reception for free.** With handlers, reception is always live — a signal
  arriving mid-activity is captured by its handler regardless of where the body
  is. The channel model also buffers, but the body must *remember to drain* every
  channel, and an un-received signal sits unconsumed.
- **Fit with the effect runtime.** `wait_condition` is a single suspend/resume
  effect — the machinery model B already runs for `execute_activity`/`sleep`. A
  channel `receive` is *also* an effect, but it additionally needs a per-channel
  cursor **and the same history-ordered delivery underneath** (below). So channels
  are strictly *more* machinery for *less* ergonomic payoff.
- **No leverage from Eio channels.** One might reach for `Eio.Stream` to back a
  channel API — but the workflow body does **not** run on Eio fibers. It is the
  deterministic re-run under our own effect handler. A channel API would be a
  bespoke cursor over our event log, not a use of Eio's concurrency primitives —
  zero ecosystem gain, and it muddies the “the body is deterministic, not
  concurrent” mental model.
- **Familiarity.** Python, TypeScript, Java, and the current Rust SDK all lead
  with handlers + `wait_condition`. Matching the dominant model lowers the
  learning curve for anyone arriving from another Temporal SDK.
- **The hard part is identical either way.** History-ordered delivery (below) is
  required for *both* models — a channel does not make determinism easier. Given
  that, the more ergonomic surface is the decision.

A channel-style `signal_channel` remains *possible later* as a thin facade
over the same event log, if a genuine streaming use-case appears. 

### Execution semantics — history-ordered delivery (the core)

How history-ordered delivery works is subtle and it's very easy for our 
6-months-in-the-future selves to forget, so it's documented explicitly here.

**The requirement.** Reading signal-mutated state MUST be _replay-deterministic_.
In real Temporal it is: the interleaving of handler execution and body progress
is a pure function of history (signals sit at fixed positions relative to activity
completions and timer fires), and replay reproduces it exactly. So a body that
does `if !x = 12 then …` is safe. The branch is frozen into history the first
time and reproduced on every replay. The real non-determinism hazards are
wall-clock time, RNG, and iteration order.

**Why the naive approach is wrong.** Accumulating signals in a set and applying them all
through handlers at the start of each replay is a tempting, but dangerous, shortcut.

Consider:

```ocaml
let a = execute_activity ctx act_A input in                 (* seq 1 *)
if !x = 12 then execute_activity ctx act_B … else execute_activity ctx act_C …
```

With a handler `set_x : x := 12`. Suppose history is: 

A completes (task 2) → the body reaches the `if` with `x = 0` → takes the **else** → schedules **C** (now in
history); *then* `set_x(12)` arrives (task 3).

- Real replay reaches the `if` at its history position before the signal so
  `x = 0`, schedules C, matches history. ✅
- “Apply all signals up front” re-runs task 3, applies `set_x` first (`x = 12`),
  replays A's resolution, reaches the `if` → `x = 12` → schedules **B**. History
  says C → **non-determinism error, workflow fails.** ❌ (we should be thankful that Temporal
  actively generates non-determinism errors, otherwise this stuff would sneak into production
  and corrupt our data while no one's looking)

Eagerly applying signals reorders them ahead of resolutions that preceded them in history.

**The model: an ordered event log with a replay cursor.** Today `run_state` holds
an unordered `seq → resolution` map. This is fine for activities because activity
resolutions are keyed by `seq` and order-independent. Signals are **order-sensitive
relative to everything else**, so `run_state` instead holds a single history-ordered log
of external events:

```ocaml
type event =
  | Activity_resolved of int * resolution   (* keyed by seq *)
  | Timer_fired       of int
  | Signal            of string * payload   (* name, encoded arg *)
type run_state = { ...; mutable events : event list (* append-only, history order *) }
```

On each re-run a **cursor** walks `events`:

* An `execute_activity` (seq k) / `sleep` (timer k) effect advances the cursor to
  the next matching `Activity_resolved (k, _)` / `Timer_fired k`, **delivering any
  `Signal` entries it passes** to their registered handlers, then resumes the body
  with the result (or, if none is found, emits the schedule command and suspends)
* A `wait_condition` checkpoint advances the cursor to the current frontier, 
  delivering any not-yet-delivered `Signal` entries. Then it evaluates the
  predicate, resuming if true suspending otherwise.
* **Synchronous code between checkpoints sees a stable snapshot**: it observes
  only the signals delivered up to the last effect/checkpoint. The cursor is *not*
  advanced by a bare `if !x = …`

This delivers each signal at the point the body would have observed it
originally, so `if !x = 12` replays identically. Walking through the failure case
above: `execute_activity A (seq 1)` finds `Activity_resolved (1, _)` with no
`Signal` before it, so `x` is still `0` at the `if`; the `Signal(set_x)` sits
*after* that resolution in the log and is only delivered at the next checkpoint —
after the `if`. Branch preserved. ✅

**Handlers are synchronous** (`payload -> unit`): the async is entirely in
*delivery*, not in the handler doing its own `await`/activities. This keeps replay
simple and matches the common case in every SDK. (Python/TS permit async handlers;
we deliberately do not, at least initially.)

**Buffering.** If the cursor reaches a `Signal` whose handler is not yet
registered (a handler registered *after* a checkpoint, which is unconventional),
the signal is buffered and delivered when `on_signal` registers a handler for that
name — matching Temporal, which buffers signals until a handler exists. In the
normal case handlers are registered at the top of the body, before any checkpoint,
so buffering never triggers.

### Queries (Read-only Replays)

A `QueryWorkflow` job arrives on an activation (possibly one with no other jobs,
even on an already-completed workflow). It's important to maintain the mental
model that queries aren't function calls but they are queue-delivered jobs.

Queries do not advance the workflow, so:

1. Re-run the body to its current suspension frontier (rebuilding state from the
   event log and registering the `on_query` handlers) in a **query mode** that
   emits **no** workflow-advancing commands and rejects any *new* command-emitting
   effect (an `execute_activity`/`sleep`/`continue_as_new` that is not already
   resolved by history is a programming error in a query path)
2. Invoke the registered query handler with the decoded argument
3. Emit a single `RespondToQuery` command carrying the encoded result.

Because the body is deterministic and replayed only to the same frontier, no new
history is produced. A query with no registered handler answers with an error.

### `wait_condition`

`wait_condition ctx pred` evaluates `pred ()` against current (replayed-so-far)
state. If true, the body proceeds. If false, the run suspends (drop the
continuation) with no new commands and resumes on the next activation, where fresh
signals/timers/resolutions may flip the predicate. A re-run that reaches neither a
resolvable effect nor a true condition simply produces no commands for that task —
the workflow idles until more input arrives. (A body that can *never* make progress
is a bug; detecting that “nothing advanced” case is an open question below.)

### Wire Additions (coresdk)

Decode two new `WorkflowActivationJob` variants — `SignalWorkflow` and
`QueryWorkflow` — and encode one new `WorkflowCommand`, `RespondToQuery`. Field
numbers to be confirmed against `workflow_activation.proto` /
`workflow_commands.proto`, the same way timers, continue-as-new, and the failure
wrapper were pinned down.

### Updates (deferred)

Temporal **updates** are signals that return a value and may be gated by a
validator. They fit the same shape (`Update.define ~name ~input ~output`,
`on_update ctx upd ~validate ~handle`) and reuse the event log (an update is an
order-sensitive event like a signal, with a response like a query). Updates are
mentioned here but their definition and design is deferred until the code matching
this ADR has settled on `main`.

## Consequences

- **+** No ppx: definitions are plain values and handlers are plain closures —
  the manual form both Python and Rust expose underneath, native to OCaml.
- **+** Reception is asynchronous and always-live; the body keeps full control of
  sequencing, so state-machine workflows are natural.
- **+** `wait_condition` reuses the existing suspend/re-run effect machinery — no
  new concurrency model, and the body stays deterministic.
- **+** History-ordered delivery makes branching on signal state replay-safe, and
  read-only query mode keeps queries from corrupting state or history.
- **−** `run_state` moves from an unordered `seq → resolution` map to an ordered
  event log. This is a real change to the runtime's core data structure, and the
  cursor/frontier logic is more intricate than the current `Hashtbl.find_opt`.
- **−** Synchronous-only handlers mean an async handler pattern (a handler that
  itself awaits an activity) is not expressible; workflows must move that work into
  the body, reacting to state a synchronous handler set. Accepted for the first
  cut.
- **−** The fine-grained interleaving still has an edge: within a single
  activation the exact ordering of a handler firing *between two synchronous
  statements* is coarser than a true coroutine scheduler. For “handler accumulates,
  body reacts to aggregate state” (essentially all signal workflows) it is
  equivalent; a workflow depending on a handler firing at a precise mid-body point
  would see coarser ordering. Documented, not solved.

## Alternatives considered

- **Option B — channel/stream** (`signal_channel` + `receive`). Rejected as the
  primary API for the reasons in the dedicated section above: worse ergonomics for
  aggregate-state conditions, more runtime machinery for less payoff, no leverage
  from Eio's channels (the body is not fiber-scheduled), and it does not simplify
  the determinism problem. Kept open as a possible thin convenience later.
- **Option C — functional fold** (`define_reactive ~init ~on_signal ~decide`). A
  pure reducer over the signal stream. Rejected: it cannot express “do an activity,
  then wait for a signal, then do another activity” — it fights `execute_activity`
  / `sleep` living inside the body, which is the whole point of a workflow.
- **ppx-driven decorators/macros** (mirroring Python/Rust directly). Rejected by
  the project's dependency stance — the same objection as ADR-0001.
- **Runtime-managed state instead of body-local refs.** Handlers mutate a
  runtime-held store rather than closures over `ref`s. Rejected: less ergonomic and
  no real benefit, since the event log already gives deterministic rebuild of
  ref-based state on each re-run.

## Open questions (for the implementing ADR)

- The precise within-activation frontier: exactly which signals are delivered at a
  `wait_condition` vs. at an effect, when multiple signals and resolutions share
  one activation.
- How query mode **enforces** read-only — a distinct query effect handler that
  rejects command-emitting effects, vs. documentation plus convention.
- Signal buffering semantics when a handler is (re-)registered late, and whether a
  signal with no handler is dropped, buffered indefinitely, or supported via a
  dynamic/catch-all handler.
- Handler exceptions: does a throwing signal handler fail the task, fail the
  workflow, or get logged and skipped? (Other SDKs mostly fail the task.)
- Detecting a workflow task that made **no** progress (no effect resolved, no
  condition satisfied) so an unsatisfiable `wait_condition` doesn't silently idle.
- Updates: validators, and whether they share the event log or need their own
  ordering.
