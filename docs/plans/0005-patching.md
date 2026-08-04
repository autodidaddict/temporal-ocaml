# Implementation plan: patching

Implements [ADR-0005](../adr/0005-patching.md). The code in `main` is the source of
truth; this plan is the intended sequence, and it will shift as phases land.

## Guiding principles

- **Confirm the sdk-core behavior the design rests on before writing the runtime.**
  Two of the ADR's open questions decide the shape of the answer rule. Both are cheap
  to settle with a live server and expensive to discover after the fact.
- **Every phase merges green on its own** and keeps existing workflows working.
- **The sticky-answer rule gets tests before it gets trusted.** Getting it wrong
  changes a running execution's branch without failing, so it cannot be caught by
  watching for errors. `replay_test.ml` already drives synthetic activations, which is
  exactly the harness this needs.
- **Nothing touches the scheduler.** `patched` returns without blocking, so ADR-0004's
  dispatcher, waiter tables, and scope tree are untouched. A phase that finds itself
  editing the scheduler has gone wrong.

## Decisions

- **One answer table, not two sets.** ADR-0005's first draft described notified
  identifiers and denied identifiers separately. A single
  `patches : (string, bool) Hashtbl.t` in `run_state` covers both, since a
  `NotifyHasPatch` job simply writes `true` before the body runs. The ADR text follows
  this shape.
- **No new `issued` keys.** The answer table already means an identifier reaches the
  emit path at most once per run, and sdk-core drops duplicate `SetPatchMarker` commands
  through its own `encountered_patch_markers` table. Adding a `"patch:<id>"` key to the
  ADR-0004 emit-once set would be redundant.
- **`patched` is an effect.** `ctx` carries plain values rebuilt per activation and
  cannot reach `run_state`, which only `replay.ml` holds. The check follows every other
  workflow operation and performs an effect, with the handler continuing immediately
  rather than parking the fiber.
- **`deprecate_patch` returns unit.** sdk-core allows the call in every history state,
  so there is no answer to return or record.

## Phases

### Phase 0 - Wire plumbing and the sdk-core spikes

Landed. No behavior change. Settles the two open questions the answer rule depends on.

- Added to `coresdk.ml`, encode and decode only, nothing emitted or consumed yet:
  decode `NotifyHasPatch = 9` (field `patch_id = 1`) and
  `WorkflowActivation.is_replaying = 3`; an encoder for `SetPatchMarker = 10` (fields
  `patch_id = 1`, `deprecated = 2`).
- Two consumers had to account for the new job variant. `apply_job` ignores it until
  phase 2. `worker.ml` classifies it as non-mutating when deciding `query_mode`, since
  it answers a patch check rather than advancing the body, and leaving it out would have
  turned a query-only activation into a read-write replay.
- The round-trip tests went into `replay_test.ml`, not `test_codec.ml` as this plan
  first said. `test_codec.ml` exercises the public `Codec` API through `Temporal`, and
  the wire layer is only reachable as `Temporal__Coresdk`, which `replay_test.ml`
  already opens for the ADR-0004 encoders.
- Both spikes ran against a dev server with a worker restarted mid-execution, using
  `ApprovalWorkflow` because it parks on `wait_condition` and can be left open. Results
  are recorded in the ADR's open questions. A from-scratch replay does carry
  `is_replaying = true` for the replayed history and `false` for the new work that
  follows, and emitting a marker while replaying against a history with no marker is
  accepted rather than reported as a mismatch.
- The spikes needed throwaway instrumentation in `worker.ml` and `replay.ml`, reverted
  once they answered. Note for anyone repeating them: after killing a worker, the next
  workflow task goes to the dead worker's sticky queue and only reaches a fresh worker
  once that times out, which takes longer than an obvious wait.

### Phase 1 - Thread `is_replaying` through the runtime

Behavior-preserving.

- `decode_wf_activation` returns `is_replaying`, and `workflow_loop` passes it into
  `Replay.run_workflow` the way it already passes `can_suggested` and `history_length`.
- Existing characterization tests must pass unchanged. Nothing reads the new argument
  yet.
- Useful past this ADR. Replay-aware logging needs the same field.

### Phase 2 - `patched` and `deprecate_patch`

- `replay_state` gains `patches : (string, bool) Hashtbl.t`, rebuilt empty on eviction
  like `issued`.
- `apply_job` handles the `Notify_has_patch` job by writing `true` for its identifier.
  The job carries no ordering significance, unlike a signal, so it does not join the
  event log.
- Two effects in `workflow.ml`, `Patched_effect of string -> bool` and
  `Deprecate_patch_effect of string -> unit`, with handlers in `replay.ml` that consult
  the table, fall back to `is_replaying`, record the answer, and continue immediately.
- Marker emission goes through the existing `emit`, so `query_mode` suppresses it and a
  read-only query replay adds nothing to history.
- Public API in `temporal.mli` with the four-step lifecycle summarized in the docstring,
  since that is where a developer will look for it.

### Phase 3 - Replay tests for the sticky answer

The behavior that fails silently, so it gets its own phase rather than riding along
with Phase 2.

- An execution that replays with no marker answers `false`, and then answers `false`
  again on a following activation carrying new work with `is_replaying = false`. This is
  the case the whole design exists for.
- An execution whose activation carries `NotifyHasPatch` answers `true` on that
  activation and on every re-run after it.
- A first pass with no marker and no replay emits exactly one `SetPatchMarker` and
  answers `true`, and a re-run triggered by an unrelated signal emits none.
- Eviction drops the table, and the full replay that follows re-derives the same
  answers in the same order.
- A query-mode replay of a patched body emits no marker and still answers consistently.
- `deprecate_patch` emits the marker with `deprecated` set and is accepted in every one
  of the above histories.

### Phase 4 - Integration and finalize

- Drive the ADR's developer flow against a dev server through `livetest.sh`: start
  executions under an unpatched body, deploy a patched body against the same task queue,
  and assert that the older executions complete on the original branch while new ones
  take the patched one.
- Add a patched workflow to `examples/ecommerce`, with the retired form in a comment so
  the four-step lifecycle is visible in code.
- Fold the spike results into the ADR, resolve the open questions, and flip it to
  Accepted.

## Open questions carried from the ADR

Each is resolved by the phase noted:

- Whether a `false` answer should still emit `SetPatchMarker` (Phase 0). Answered:
  harmless, so phase 2's emit path needs no guard beyond the answer-`true` branch.
- Post-eviction replay and `is_replaying` (Phase 0, covered again in Phase 3).
  Answered: a from-scratch replay reports `is_replaying = true` for the replayed
  history, so the recorded-answer rule holds.
- Where a `false` answer is memoized (Phase 2). Holding it in `run_state` ties it to the
  run's cache entry, which is only safe if no activation sequence evicts a run and then
  delivers new work without replaying the body first.
- Reporting a stale patch (deferred). Depends on the metrics work the SDK does not have.
