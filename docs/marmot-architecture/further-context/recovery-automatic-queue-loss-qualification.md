---
title: Automatic account QueueLoss recovery qualification
created: 2026-09-26
updated: 2026-09-27
status: Focused local qualification
---

# Automatic account QueueLoss recovery qualification

The account worker now keeps an eligible singleton QueueLoss grant while its
immutable SDK reconciliation request and bounded event queue run in the shared
recovery job. Direct overflow Receive, ordinary Receive, and Maintenance use
the same frozen owner grant. Only plans within four routes and four endpoints
per route offload; an ineligible plan executes that same grant inline. When the
shared credit pool is empty, an eligible QueueLoss shape defers before retry
reservation. Activation, admission, drain, checkpoint, and loss acknowledgment
remain with the account owner. Shutdown aborts and reaps owned network/queue
tasks; the loss guard preserves unacknowledged delivery loss. An owned
direct-overflow completion resumes the original Receive tail after
reporting its real summary: pending projection updates, visible post-join
history, subscription refresh, convergence scheduling, audit/push work, and
the existing error/reconnect path. A completed overflow also runs the
post-replay Receive arm for newly pending recovery debt; incomplete and
deferred outcomes retain their debt for a later seam.

The focused fixture uses two local relays, the real Nostr SDK transport,
SQLCipher account storage, and valid MLS history. Bob fills Alice's 1,024
ordinary-delivery slots, then publishes a valid group-profile commit as the
first omitted event and two dependent custom messages. The router records a
real drop and durable typed QueueLoss evidence. A test-only worker pause
stabilizes the pre-burst boundary; it does not select a grant. The fixture
observes the naturally selected QueueLoss attempt and its persisted target
route/time bounds. It holds the exact-ID acquisition for the missing commit
at the local relay, then checks a status read within 300 ms, a healthy-route
send within 2 s, and a separate healthy live delivery within 2 s. The hold
releases immediately after concurrent probes, before scope inspection.

Both local paths passed: a naturally selected Maintenance attempt and a
Receive attempt reached by one additional valid healthy-route delivery once
the ordinary queue had capacity and no network job was active. That condition
does not establish that an earlier grant had completed. At the hold, the
selected network job and adapter request remained active through all probes.
After release, that attempt returned and queued the target event; no later
grant was selected before the missing commit was durably retained, Alice
advanced one MLS epoch, and both dependent plaintext messages appeared. The
durable QueueLoss marker and
demand remained pending because the SDK's EOSE and aggregate comparison do
not certify exhaustive admission. The fixture asserts the loss stays honest.

The account worker now makes one cooperative scheduler handoff after a claimed
delivery completes its durable ingest and Receive continuation. A forced
direct-overflow Receive continuation runs first. The handoff is outside the
biased select, so a ready delivery queue does not immediately win another
Receive in that worker poll. This deliberately paces the worker; it makes no
guarantee about I/O latency, timers, or throughput under other loads.

SDK reconciliation separately marks a **clean bounded suffix** only when a
finite pass limit left remote-only IDs unattempted after clean comparisons and
exact-ID requests. A missing or failed endpoint, request-policy failure,
claimed ID with no returned event, wrong ID, completed byte-limit rejection,
or intrinsically oversized cached event cannot supply that evidence. Inline
and owned paths carry at most 16 IDs absent from the frozen route inventory,
and only after the full returned batch reaches the delivery queue. After the
drain, the owner requires an event retained in the selected route and time
scope, a matching grant revision fence, and no refusal or overflow. Only then
may a standalone QueueLoss obligation remain paced `Retry` on an unfinished
pass. The checkpoint outcome remains `Unknown` or `BudgetExhausted`; endpoint
coverage and admission flags remain false. Cursor movement or queue submission
alone does not qualify, and repeated adversarial backdated novelty has no
finite-lifetime bound from this rule.

The cleaned composition passed one local offline Linux arm64 run of the
original 500-event Receive fixture in 67.71 seconds: QueueLoss Receive
attempts 5–8 proceeded through normal owner pacing; the held attempt 8
requested and queued the missing commit, and the fixture verified durable
retention, the MLS epoch advance, both dependent plaintext messages, and the
existing status/send/live response bounds. The marker and demand remained
pending as the fixture requires. Earlier local handoff-only and diagnostic
runs failed, and the previously observed GitHub x86_64 failures remain open;
this local result does not classify or close them. Temporary CPU, scheduler,
target-rank, and cursor probes used during diagnosis were removed from the
review patch.

The follow-up adds two narrowly typed reasons to continue an unfinished
standalone QueueLoss investigation. After successful NEG comparison, a valid
remote-only prefix interrupted solely by an exact-ID `Deadline` may propose
up to 16 returned candidate IDs. It does not become a clean suffix: the owner
requires full queue handoff, a selected scope, a stable fence, no refusal or
unsupported route, and actual post-drain retention before paced `Retry`.
Separately, a completed route with an actual NEG `Timeout`, only timed-out
failed endpoints, and zero remote IDs and returned events may report that its
selected, admitted, required route was unavailable. This permits paced
`Retry` without a retained candidate only after the same owner guards; lookup
failure, other NEG errors, and an outer route timeout do not qualify. The
ordinary comparison remains a transient failure and stored endpoint coverage
remains `Unknown`, nonexhaustive, and admission-incomplete. Existing durable
backoff is 15/30/60/120/240/300 seconds, capped at 300 seconds without an
attempt-count cutoff for transient unavailability. A later completed empty
investigation without either typed reason parks at `NeedsDeepRepair`.

Focused SDK classifier tests use constructed `ErrorKind` outcomes. The
deadline-prefix test constructs a `Deadline` from an endpoint returned by a
real acquisition; it is not an end-to-end timed acquisition proof. Inline and
owned handoff and owner checkpoint tests pass, including refusal, unsupported,
stale-fence, unrelated-route, pacing, and later-empty exclusions. The SDK's
two-second route pass and the separate ten-second account job are cooperative
deadlines, not guaranteed wall-time, CPU, or byte bounds.

One composed offline Linux arm64 Receive run failed its strict fixture check
after 68.30 seconds. Receive attempt 7 entered the held target query with an
active job and request, but the request and reconciliation phase ended during
the concurrent probes; healthy live observation missed its two-second bound.
The intended new Receive stimulus was not sent. The target route returned 14
events with an exact-request `Deadline`; a separate inbox route had a typed
NEG timeout. Useful retention, epoch advance, and both dependent plaintexts
occurred later, but one later grant prevents attribution to the held grant.
The full log is preserved locally at
`/Volumes/Worktrees/codex/fdad/mdk-2060-linux-probe/logs/receive-typed-continuation-linux.log`.

A single diagnostic-only follow-up passed in 68.70 seconds. The natural
Receive stimulus was sent, and attempt 8 held the target query from fixture
time 19,860 ms. Bob's healthy live publish started at 19,861 ms and completed
successfully at 19,907 ms; Alice's plaintext appeared at 19,949 ms. The same
job and request remained active through the probes, no later grant was
selected, and that grant returned and queued the target event. Durable target
retention, MLS epoch advance, both dependent plaintexts, and the still-pending
QueueLoss marker and demand satisfied the fixture. The full log is preserved
at `/Volumes/Worktrees/codex/fdad/mdk-2060-linux-probe/logs/receive-live-boundaries-linux.log`.
This pass is a nonreproduction of the preceding failure, not its cause or fix.
Its target-route comparisons before the hold were clean request-cap prefixes;
it does not integrate-qualify either new typed continuation.

The exact `62555076` CI still failed 40 x86_64 attempts across ten heads.
Rust jobs 1 and 2 failed the Receive and Maintenance fixture variants;
Rust jobs 3–5 passed. The
Receive fixture saw hundreds of ordinary deliveries queued at 45 seconds, so
its zero-queue stimulus could not run. The target route returned only a small
prefix of a large remote-only set in the first attempts and timed out during
NEG in later attempts. Bob published healthy-route traffic, but Alice did not
observe its plaintext within two seconds. The target remained missing. Capped
owner-decision witnesses do not establish why the terminal attempt parked.

One test-only cost probe ran the unchanged Receive fixture once on offline
Linux arm64. It passed in 68.08 seconds: the queue reached zero before the
19,345 ms held target query, the healthy live message appeared, and the same
grant returned and queued the target without a later grant. In the roughly
19.4-second measured window, Alice's reconciliation-inventory calls totaled
804,212 microseconds across 1,075 calls; scoped-admission checks totaled
48,984 microseconds across 1,026 calls. The 286 direct Receive recovery
follow-ups totaled 32,298 microseconds of elapsed time, including suspension.
The direct Receive counters observed 335 dequeued deliveries, 49 duplicate
skips, and 286 completed ingests; they exclude deliveries drained by online
recovery. The runtime telemetry spans are shared across the runtime, nested,
and rounded down to milliseconds per completion. Their sums cannot be added
or subtracted to partition wall time, and completions may straddle the probe
boundaries. The arm64 run did not reproduce the x86_64 backlog, so it does not
identify the x86 cost. Inventory/projection transaction consolidation remains
an unproven production hypothesis pending x86 measurement. The local log is
preserved at `/Volumes/Worktrees/codex/fdad/mdk-2060-linux-probe/logs/receive-loss-cost-linux.log`.
The open CI blocker remains unresolved; this PR remains draft.

This is a focused local outcome, not proof for every account route shape,
continuous traffic, relays without comparison support, process restart, or
device delivery. The SDK's per-route negotiation and acquisition deadlines,
the outer 10-second network quantum, and existing drain clocks were unchanged.
The fixture gates the SDK's exact-ID acquisition and records the active client
attempt, per-route comparison counts, and remaining outer deadline. A selected
scope alone does not establish that an exact-ID request was issued or that the
missing event was admitted. Failed relay comparisons and unattempted suffixes
remain partial coverage with durable debt; they are not successful recovery.
