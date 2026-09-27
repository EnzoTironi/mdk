---
title: Account history recovery
updated: 2026-09-27
status: Proposed design (v2). Replaces the 23 recovery design, ledger and qualification notes.
---

# Account history recovery

Tracks #1945 (outcome), #1947 (bounded acquisition) and #1948 (assurance). The durable
owner from #1946 stays. This document covers what changes, what is deleted, and how we
will know it worked.

## Goal and scorecard

Recovery must be correct, responsive, cheap on bandwidth, and it must finish.

The scorecard is the #1945 large-account workload: 36 groups, about 11,000 events, one
51-member group, two relays, one genuinely missing older commit, and live traffic
throughout. It runs under **production policy**, against v0.10.4 (what iOS build 41
shipped) and against the new code.

| Measure | Target |
| --- | --- |
| Status and send latency while recovery runs | under 500 ms |
| A live message becomes visible while recovery runs | within 2 s |
| Queue overflow caused by history we already hold | zero re-download |
| Missing commit that some relay still has | recovered in one paced attempt |
| Steady state once caught up | no full-history download; comparison traffic only |
| Loss obligations | each one completes, or parks visibly after a fixed budget |

The simulator comes first; Jeff validates on a phone.

## Decisions (agreed with Jeff, 2026-09-27)

1. **Completion is tiered.**
   - Known-event loss completes when that exact event is durably stored, or has a durable
     terminal disposition.
   - Unknown-scope loss completes when every *required* relay finished an untruncated NIP-77
     comparison over the frozen window, and every difference was admitted or terminally
     disposed.
   - Otherwise, after **3 completed attempts that make no progress**, the obligation parks.
     It shows "history may be incomplete" and offers an explicit deep repair. There are no
     further automatic retries.
   - Attempts that fail only because relays were unreachable do not count toward the budget.
2. **The queue keeps what it drops.** Overflowed deliveries are stored durably (bytes), within
   a cap.
3. **Required relays are the group relays we operate.** These are the whitenoise.chat
   relays, and NIP-77 is a hard requirement for group relays from now on. Any other relay
   is read on a best-effort budget and never blocks completion.
4. Both implementation steps land before the next MarmotKit release.

## Design

### 1. Overflow becomes a durable tail, not a loss

Today, when the 1,024-slot account queue is full, the router holds the whole
`TransportDelivery` (ID, payload, route) and keeps only a count. Everything else in the
current design follows from that choice: loss of unknown scope, broad replay, a completion
nobody can certify, and the wait behind the queued backlog.

New behavior, with each tier falling back to the next:

```
router ─► account queue (1,024, memory) ─► worker ingest
   │ full
   ▼
durable spill (encrypted DB, capped: 16 MiB / 8,192 events) ─► worker ingest, bounded turns
   │ full
   ▼
dropped IDs (capped: 4,096) ─► known-event obligations ─► exact-ID fetch
   │ full
   ▼
unknown-scope loss (today's behavior) ─► comparison obligation
```

- The spill writer extends the already-approved off-worker loss writer. It writes spill rows
  and loss evidence only, never engine or receipt state. The hand-off from the router is a
  bounded in-memory buffer, so the router never blocks.
- A spilled event counts as retained for cursor safety once its write is durable. The live
  cursor never passes an event that is neither admitted nor durably spilled.
- The worker admits spilled events after the in-memory queue drains, a few per turn. Order
  is not guaranteed; the engine already handles reordered input (deferral and retained
  input), and a test will pin this.
- In the incident, overflow came from replaying history the device already had. Under this
  design that costs no network at all: the spilled events are admitted as duplicates and
  dropped cheaply.

### 2. Recovery never touches live subscriptions

Recovery stops calling `require_fresh_activation`, `activate_transport(since)` and the
group re-subscribe. Live subscriptions follow the cursor and change only on route changes
or reconnects, both owned by the relay plane. Broad unfloored replay is removed from
automatic recovery.

### 3. One execution path for every cause

| Cause | Source of work |
| --- | --- |
| Spill over its cap, released receipts | Known event IDs |
| SDK notification loss, spill-and-ID overflow, epoch gap, cold-start incremental history, explicit repair | NIP-77 comparison of the affected routes over the retained-inventory window (explicit repair may use a wider window) |

Every cause runs the same job:

1. **Select and freeze** (worker, short turn). The owner picks due obligations and coalesces
   those with compatible routes and window. It freezes the plan (routes, required relays,
   window, revisions) and reserves the retry cost before any I/O. This part exists today.
2. **Acquire** (off the worker, holding one of the two process-wide credits).
   Request-local NIP-77 comparison plus bounded exact-ID fetch, reusing #2031/#2037. The
   task returns an owned batch plus a per-relay outcome: complete, truncated, failed or
   unsupported.
3. **Admit** (worker, bounded turns). A few events per turn go straight into the normal
   ingest path, never through the live queue. The worker yields between turns, so commands
   and live input interleave. The admission loop from `bounded_recovery.rs` is the starting
   point.
4. **Settle** (worker, short turn). Checkpoint, then complete, retry or park according to
   the tier rules, using the existing revision checks. Old attempts still cannot clear newer
   demand.

The worker never awaits the network. There is one recovery job per account.

Rules kept from the current design: complete coverage with a still-stuck engine means no
replay; the blocked reason is recorded; escalation happens after three distinct
observations.

NIP-77 cost scales with the difference, not the set size, so comparing the whole retained
window is cheap once we are caught up.

## What gets deleted

- `client/sync/comparison_job.rs`: per-trigger offload, its eligibility rules, and the
  online epoch-gap job.
- The inline broad executor inside `execute_recovery_grant`: activation, broad replay, and
  drain-based completion (`DrainVerdict` mapping). The same goes for
  `recover_delivery_overflow*`, the QueueLoss control-token deferral, and the
  `queue_reconciled_event` → `handle_reconciled_event` path back into the live queue.
- Conservative mode (`RecoveryExecutorMode`). It is internal to `marmot-app` and not in the
  bindings.
- Three recovery job slots and their yield flags in the worker loop, replaced by one.
- The approved exception for unbounded retention of unresolved-loss rows. Loss records are
  now capped at every tier.
- The 23 recovery docs, replaced by this one.
- Most of the 16 real-relay qualification test files. They are replaced by the tests below.

Each PR reports exact before/after line counts. The goal is a large net reduction across
the recovery modules, not a rewrite that adds a second system alongside the current one.

## Kept

- The durable owner, obligations, retry pacing across restarts, and revision checks
  (simplified where they become unnecessary).
- The process credit pool.
- Serialized admission on the worker.
- Per-account SDK clients (#2009) and directory isolation (#2005).
- Worker-startup isolation (#1999).
- Epoch-stall detector facts.
- Post-join maintenance subscriptions. These are unchanged here and revisited later: they
  are also a full-history request.

## Storage

- Forward-only migration 0096 adds the spill table: event ID unique, route hint, delivery
  blob, received time, size.
- Dropped IDs become ordinary known-event obligations; the table for those already exists.
  They get their first production producer, since today only tests create them.
- Existing pending QueueLoss, notification-loss, epoch-gap, incremental and explicit rows
  run on the new path as unknown-scope comparisons.
- Tables that no code reads any more are dropped in a later migration, once their rows have
  been converted.

## Tests

- **Deterministic:**
  - spill caps and degradation;
  - cursor safety while spill writes are pending;
  - reordered admission, for example a spilled commit followed by live messages;
  - completion tiers and parking;
  - revision checks.
- **Real-relay, a handful:**
  - overflow of known history, which must produce zero network requests;
  - a missing commit fetched through comparison on two relays;
  - status, send and live delivery while a network request is held;
  - restart mid-recovery.
- **Scorecard:** nightly, production policy, measuring the table above. It tracks bytes by
  kind (novel, duplicate, control), attempts, and time to useful progress.
- **CI policy:** no reruns to get green. A flaky test gets a root cause.

## Delivery

| Step | Contents | Merge policy |
| --- | --- | --- |
| 0 (tonight) | Fix the production-policy nightly; close #2060 with an evidence summary; slim the docs to this file; scorecard harness and a baseline on current master | Merge after self-review |
| 1 | Spill and dropped-ID tiers, direct worker admission for recovered events, known-event producer | Built and self-reviewed tonight; merged after we talk |
| 2 | One execution path for every cause, removal of activation and broad replay, tier completion, parking and status, deletions | Tomorrow onward |

## Risks and open items

- Spill write latency under a burst, and cursor safety while writes are pending (covered by
  tests).
- Whether the fork reports a per-relay NIP-77 outcome with a truncation flag. If not, that
  needs a small fork change.
- **Ask:** raise the NIP-77 match-set cap on the whitenoise relays to at least the
  inventory cap (16,384 per route), so comparisons are never truncated.
- Recovery audit event meanings change. The audit-v5 agents pick this up after step 2.
- NSE behavior needs device validation. The spill makes short extension runs safer, because
  nothing is lost if one ends mid-drain.
