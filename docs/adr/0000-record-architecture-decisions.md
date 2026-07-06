# 0. Record architecture decisions

- Status: Accepted
- Date: 2026-07-06

## Context

This SDK involves a number of consequential design choices — the FFI boundary,
how much sits in OCaml versus sdk-core, serialization, the workflow execution
model — where the *reasoning* matters as much as the outcome. Future
contributors (and future us) need to know not just what was decided but why, and
what was rejected, so decisions aren't silently re-litigated or accidentally
reversed.

## Decision

We will keep Architecture Decision Records, in the lightweight format described
by Michael Nygard.

- ADRs live in `docs/adr/` as `NNNN-title.md`, numbered sequentially from 0000.
- Each ADR has: a title, a status, the context, the decision, and its
  consequences (and, where useful, the alternatives considered).
- Statuses: `Proposed`, `Accepted`, `Rejected`, `Deprecated`, `Superseded by
  ADR-XXXX`.
- ADRs are immutable once `Accepted`. To change a decision, write a new ADR that
  supersedes the old one; mark the old one `Superseded`. Small edits (typos,
  clarifications) are fine.

## Consequences

- The rationale behind significant choices is discoverable and durable.
- A small, deliberate overhead per architectural decision — which is the point:
  it applies only to decisions worth recording, not day-to-day changes.
- The numbered, append-only history doubles as a design changelog for the SDK.
