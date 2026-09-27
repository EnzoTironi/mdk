---
title: Account history recovery
updated: 2026-09-27
status: Design (v2), being implemented. Replaces the 22 recovery design, ledger and qualification notes.
---

# Account history recovery

Tracks #1945 (outcome), #1947 (bounded acquisition) and #1948 (assurance). The durable
owner from #1946 stays. This document covers what changes, what is deleted, and how we
will know it worked.

The notes this replaces, including the #1946 ownership design, its integration ledger and
the 13395cc1 checkpoint evidence, remain in git history at
[`9489bb091`](https://github.com/marmot-protocol/mdk/tree/9489bb091/docs/marmot-architecture/further-context).

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
durable spill (encrypted DB, capped: 16 MiB / 8,192 events) ─► worker ingest, alternating with live
   │ full
   ▼
unknown-scope loss (today's behavior) ─► comparison obligation
```

Recording dropped IDs as a middle tier, turning them into known-event obligations for
exact-ID fetch, is deferred. The spill is sized so that tier is rarely reached, and
NIP-77 comparison covers what remains.

- The spill writer extends the already-approved off-worker loss writer. It writes spill rows
  and loss evidence only, never engine or receipt state. The hand-off from the router is a
  bounded in-memory buffer (4 MiB and 4,096 deliveries), so the router never blocks.
- A delivery the account has already seen, and whose receipt was not released for
  redelivery, is discarded before it uses spill capacity. Replaying history the device
  already holds therefore fills neither the spill nor the network.
- A delivery that misses the hand-off or spill limits becomes queue loss, as today: the
  cursor stays fenced until recovery settles that loss, and live input keeps flowing.
- A spilled event counts as retained for cursor safety once its write is durable. The live
  cursor never passes an event that is neither admitted, durably spilled, nor covered by
  pending queue loss.
- While spilled rows remain, the worker alternates them with live deliveries, and yields
  between them, so neither a busy live queue nor a large spill starves the other. Order is
  not guaranteed; the engine already handles reordered input (deferral and retained input).
- A row is removed only once its event is in the seen index. A row whose ingest left no
  durable trace is retried with a doubling delay, from one minute up to an hour. After 8
  attempts it is removed and recorded as queue loss in the same transaction, so recovery
  keeps an obligation.
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
| Released receipts | Known event IDs |
| Spill overflow, SDK notification loss, epoch gap, cold-start incremental history, explicit repair | NIP-77 comparison of the affected routes over the retained-inventory window (explicit repair may use a wider window) |

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
replay; the blocked reason is recorded; the existing one-shot wedge report still escalates
after three distinct local observations and starts no acquisition. That report is separate
from decision 1's budget, which parks an obligation after three passes without progress.

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
- The 22 recovery notes, replaced by this one. The bounded-acquisition interface contract
  stays in its own document.
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

- Forward-only migration 0096 adds the spill table: event ID unique, payload, metadata
  blob with a format version, size, retry attempts and retry time.
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

| Step | Contents | Status |
| --- | --- | --- |
| 0 | Restore the production-policy nightly (#2064); close #2060; slim the docs to this file; add a scorecard harness with a baseline | Nightly and #2060 done; docs in review; scorecard in progress |
| 1 | Durable spill of queue overflow, admitted through the live ingest path (#2065) | In review |
| 2 | One execution path for every cause, removal of activation and broad replay, tier completion, parking and status, deletions | Not started |

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
