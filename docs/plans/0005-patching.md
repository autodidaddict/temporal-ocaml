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
- **No new `issued` keys, and no emit-once for markers at all.** The marker is emitted
  on every run that takes the patched branch, because sdk-core treats a marker in
  history with no matching command as a workflow that does not support that version.
  sdk-core drops the duplicates through its own `encountered_patch_markers` table.
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

Landed. Behavior-preserving.

- `workflow_loop` passes the decoded `is_replaying` into `Replay.run_workflow` the way
  it already passes `can_suggested` and `history_length`.
- `run_workflow` takes it as `~is_replaying:_`, so the label is in the signature for
  phase 2 without tripping the unused-variable warning.
- The `activation` helper in `replay_test.ml` takes `?(is_replaying = false)`, which
  leaves every existing test unchanged and gives phase 3 the `true` case it needs. The
  same test count passes before and after, which is the point of the phase.
- Useful past this ADR. Replay-aware logging needs the same field.

### Phase 2 - `patched` and `deprecate_patch`

Landed.

- `replay_state` gained `patches : (string, bool) Hashtbl.t`, rebuilt empty on eviction
  like `issued`. `apply_job` writes `true` for a `Notify_has_patch` job. The job carries
  no ordering significance, unlike a signal, so it does not join the event log.
- Two effects in `workflow.ml`, `Patched_effect` and `Deprecate_patch_effect`, with
  handlers in `replay.ml` that answer from the table, fall back to `is_replaying`,
  record the answer, and continue immediately.
- The recorded answer controls the answer alone. The marker is emitted on every run
  that takes the patched branch, not only the run that first decided it. A marker in an
  execution's history with no matching command from us is what sdk-core reports as a
  workflow that does not support this version, so suppressing the command on later
  re-runs would have been a live break. sdk-core drops the duplicates through its own
  `encountered_patch_markers` table, which the phase 0 spike observed directly.
- Markers ride in a list of their own rather than through `emit`. A terminal command
  replaces `commands` outright, so a body that records a marker and then completes in
  the same activation would otherwise lose it. Making the terminal command cons onto the
  list instead was tried first and broke eight cancellation tests: discarding pending
  operation commands when a run closes is deliberate. Keeping markers separate also
  fixes their position, so they lead the completion on the recording run and on every
  replay after it. `query_mode` still suppresses them.
- Public API in `temporal.mli`, with the retirement steps in the docstring since that is
  where a developer will look for them.
- Four tests cover the mechanism: a first pass, a replay with no marker, a
  `NotifyHasPatch` answering true while replaying, and `deprecate_patch`. Phase 3 covers
  the sequences.

### Phase 3 - Replay tests for the sticky answer

Landed. The behavior that fails silently, so it got its own phase rather than riding
along with phase 2.

The tests drive a body that decides its branch and then parks on a signal, so the
answer can be observed across more than one activation.

- An execution that replays with no marker answers `false` and answers `false` again on
  the following activation carrying new work. This is the case the whole design exists
  for.
- The same holds when the re-run is triggered by a signal that has nothing to do with
  the patch.
- An execution whose activation carries `NotifyHasPatch` answers `true` even while
  replaying.
- A first pass records the marker, and the activation that completes the run records it
  again. This corrects what this plan first said, which was that a later re-run emits
  none. Emitting again is required, not tolerated: see the phase 2 notes.
- Eviction drops the table, and the full replay that follows re-derives the same answer.
- A query-mode replay of a patched body answers from the branch it took and records no
  marker.
- `deprecate_patch` is recorded against a history that holds a marker and against one
  that does not.
- The suite was checked against a mutation rather than assumed to bite. Replacing the
  recorded answer with a bare `not is_replaying` fails five of these, including both
  cases the design exists for.

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
