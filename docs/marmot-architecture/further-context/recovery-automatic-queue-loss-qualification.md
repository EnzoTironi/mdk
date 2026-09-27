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

The exact `62555076` CI added four failures, reaching 40 recorded x86_64
failed attempts across ten heads. Rust jobs 1 and 2 failed the Receive and
Maintenance fixture variants; Rust jobs 3–5 passed. The Receive fixture saw
hundreds of ordinary deliveries queued at 45 seconds, so its zero-queue
stimulus could not run. The target route returned only a small prefix of a
large remote-only set in the first attempts and timed out during NEG in later
attempts. Bob published healthy-route traffic, but Alice did not observe its
plaintext within two seconds. In both jobs' second tries, useful target
retention, MLS epoch advance, and dependent plaintexts appeared only after
five later grants; the held attempt had lost active acquisition, so the strict
same-grant assertion still failed. Capped owner-decision witnesses did not
establish why the terminal attempt parked.

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

Fresh `b667903c5` CI added four more failures, reaching 44 recorded x86_64
failed attempts across eleven heads; Rust jobs 3–5 passed. In Rust job 1's
Receive second try, the target route handed seven candidate events to the
adapter, but 277 ordinary deliveries remained queued at 45 seconds. The owner
recorded `Complete` with no candidate in durable reconciliation inventory and
parked at `NoRetainedCandidate`; the target, epoch advance, and plaintexts were
still absent at the fixture's useful-state gate. Adapter handoff proves neither
account-worker consumption nor durable retention, and the exact FIFO position
of those seven events was not witnessed. The Receive stimulus could not run
while the account queue remained nonempty. The Maintenance first try in Rust
job 2 eventually recovered useful state after five later grants but failed the
same-grant active-acquisition assertion; its second try remained incomplete.
Healthy live delivery also missed the strict bound in the failed backlog cases.

The account-owned online drain now tracks at most the already bounded remote-only
candidate IDs from its immutable network result. A successful adapter handoff
records which candidate IDs were submitted, while the account worker records
their actual consumption after a duplicate check or successful ingest. EOSE
can still complete an empty or fully consumed fence. When a submitted candidate
remains unconsumed, EOSE cannot end the attempt: the unchanged execution
quantum yields an incomplete verdict, and only an obligation whose selected
route and time scope contain that candidate remains paced `Retry`. Consumption
does not assert retention; refused, released, malformed, stale, failed-handoff,
zero-delivery, and overflow paths keep their existing owner checks. The change
adds no durable ledger, public API, deadline, or workload adjustment. Focused
default and feature tests cover real adapter queue handoff, EOSE before account
consumption, consumption before the queue result is collected, failed and
zero-delivery handoffs, owner pacing, and preserved empty-fence completion.

One rebuilt offline Linux arm64 library test ran the original 500-event Receive
fixture once on this correction and passed in 69.23 seconds. Its queue reached
zero at 20,645 ms, the held target query began at 20,957 ms, and Alice observed
the healthy live event at 21,046 ms. Active attempt 8 returned and queued the
target without a later grant; durable retention, epoch advance, both dependent
plaintexts, and the still-pending loss marker and demand satisfied the fixture.
The full log is at
`/Volumes/Worktrees/codex/fdad/mdk-2060-linux-probe/logs/receive-candidate-fence-linux.log`.
The preceding uncorrected arm64 cost run also passed, so this result does not
establish that the candidate fence fixes the x86_64 backlog or live-response
failures. The 44 failed x86_64 attempts and open CI blocker remain unresolved;
this PR remains draft.

Exact `2829401da` CI added four failed x86_64 attempts, reaching 48 across
12 heads. Rust jobs 1–3 passed; both tries in job 4's Receive fixture and job
5's unstimulated fixture missed the held target-acquisition gate at 45 seconds.
The Receive tries still had 318 and 353 ordinary deliveries queued, so the
zero-queue ordinary Receive stimulus was not sent. The target was eventually
returned and queued after later grants, while the loss marker remained pending;
that useful progress does not satisfy the fixture's same-grant active-acquisition
or healthy-live-observation proof. Some route attempts timed out during NEG;
others returned bounded event prefixes without reaching the held target.
Runtime-wide ingest and storage transaction timings are nested, shared across
accounts, and do not assign the backlog to directory work. Queue-depth change
does not count novel successful ingests when arrivals and replays continue.

This diagnostic change extends only the fixture-armed, Alice-owned test
cost probe to time directory enrichment in this path. The inclusive
`display_names` span covers `display_names_for_account_ids`, excluding sender
extraction and its later warning/fallback; the inclusive `remember_sender`
span covers `remember_directory_message_sender`. Nested stages time existing
account catalog enumeration, cached-handle acquisition, profile/entry queries,
hydration and merge/equality work, and actual write attempts. The `queries`
count is measured query operations, including a cache-entry scan that can
perform multiple cache reads; it is not a SQL statement count. Write attempts
are not confirmed writes. Ordinary error returns are counted, panics are not.
Parent and nested durations are inclusive and cannot be added to partition
wall time. No extra storage read, task, await, production cache, or policy
change is part of this diagnostic. Local default and test-policy-overrides
attribution, directory freshness, and sender tests pass; new-head x86_64 CI
has not yet run. A production directory optimization remains unproven, and
the draft PR's recovery blocker remains open.

Exact `8834a386a` CI added four failed x86_64 attempts, reaching 52 failed
attempts across 13 heads. Rust jobs 2–4 passed. Both tries in job 5's Receive
fixture and job 1's unstimulated fixture failed: the Receive first try reached
the target gate at 16,858 ms, late in its 15,113–17,133 ms SDK route pass,
with 719 ordinary deliveries queued. Bob's healthy live publish succeeded at
16,914–17,023 ms, but Alice did not observe that plaintext within two seconds;
the request and job were gone at release. The Receive second try still had 358
deliveries queued at the 45-second gate, and the other three failed attempts
never reached the held target gate. Useful target retention, epoch advance,
and both dependent plaintexts appeared eventually in all four attempts. That
later progress does not establish active acquisition or live delivery during
the held grant. Directory work measured only about 0.62–1.51 seconds inclusive
per window, with zero write attempts; it does not explain the missing live
observation or justify a production optimization.

A further fixture-only, default-disabled witness now records the healthy group
event's existing path through router arrival, per-account queue admission
(accepted, full, closed, or no account), dequeue, duplicate skip, and entry and
return from the existing ingest/projection call. It is armed only for Alice's
group-hint candidates during the timed probe and retains at most 16 transport
IDs privately. After stopping the witness and releasing the hold, the fixture
maps Bob's sent inner message ID to its locally published outer transport ID
and selects that exact event's stage aggregates. Each stage retains its first
timestamp and count, so repeated deliveries cannot erase an earlier accepted
or successful observation. Stages do not identify which copy advanced to the
next stage; an ingest call returning `Ok` does not certify a visible row or
durable checkpoint. The overflow count is dropped stage observations, not
distinct IDs. A missing sender mapping, unmatched candidate, or absent stage
is unknown rather than proof of relay loss. The witness makes no extra storage
read, synchronous health call, task, or sleep before the timed probe; it does
not alter production routing, queue policy, fixture timing, or assertions.
Focused default and feature tests exercise actual router admission/dequeue,
repeated-ID accepted/full evidence, separate ingest outcomes, the candidate
cap, and stopping. New-head x86_64 CI has not yet run, and the recovery
blocker remains open.

This is a focused local outcome, not proof for every account route shape,
continuous traffic, relays without comparison support, process restart, or
device delivery. The SDK's per-route negotiation and acquisition deadlines,
the outer 10-second network quantum, and existing drain clocks were unchanged.
The fixture gates the SDK's exact-ID acquisition and records the active client
attempt, per-route comparison counts, and remaining outer deadline. A selected
scope alone does not establish that an exact-ID request was issued or that the
missing event was admitted. Failed relay comparisons and unattempted suffixes
remain partial coverage with durable debt; they are not successful recovery.

The account delivery queue now admits at most 1,024 ordinary items plus one
reserved overflow control position. The first omission records its position
with the account-local loss authority; when a durable marker is configured,
its writer activates that position only after persistence or storage closure.
The single account receiver alternates its FIFO head with the earliest
different, locally validated group before that control position, without
overtaking any event in the same group. Validation requires the exact locally
installed group hint,
transport route, and endpoint. Unknown, ambiguous, changed, and retired route
keys remain FIFO barriers. An unchanged sanitized group sync preserves valid
keys across its await; changed maps invalidate before the await. Activation
still invalidates conservatively. Adapter lifecycle calls serialize on their
shared lock, and queue identity and retirement prevent a stale adapter or
completion from installing scheduling authority in a replacement queue.
Forwarder recovery retires a pending or ready control position atomically
with signal cancellation, so accepted items behind it drain and a late marker
writer cannot recreate it. The pending loss evidence remains authoritative.

The current local revision also lets a qualified different-group delivery
cross a **ready** overflow control only while the exact current queue marker
is durable and no later omission has invalidated that fact. Every crossed
control must be disjoint from the candidate group. Pending controls, stale
queue generations or marker tokens, notification loss, changed routing, and
same-group deliveries still block crossing. The control retains its loss
authority and its direct Receive behavior. A real adapter regression checks
the qualified dequeue and composes its consumed control with the existing
Receive selection; it does not run the account worker or establish strict
recovery on x86_64.

The focused default and `test-policy-overrides` relay-plane suites each passed
102 tests; both all-target `marmot-app` Clippy checks, formatting, and diff
checks passed. Tests cover three-group progress and per-group order, overflow
position and capacity, unknown/ambiguous/rebound FIFO, unchanged versus
changed route sync, cancellation and close, marker completion on both sides of
forwarder recovery, and stale adapter lookup/completion around replacement.
The original, unchanged Receive fixture failed one local 43.26-second run:
its ordinary stimulus was never sent because none of 277 checks saw an empty
queue; the exact healthy event was accepted at 12,178 ms but dequeued at
14,088 ms. A later single local 43.08-second run passed every strict fixture
assertion. In that run one of 308 stimulus checks saw an empty, idle queue at
13,607 ms; the active Receive attempt 8 entered the held target query at
13,723 ms, and the healthy event was accepted and dequeued at 13,753 ms,
ingested successfully at 13,761 ms, and observed by Alice at 13,771 ms.
The target was returned and queued by that grant with no later grant; the
durable QueueLoss marker and demand correctly remained pending. The passing
run verifies the empty-queue timing path, not healthy cross-group service
ahead of the original backlog: at that earlier head, post-loss ordinary
arrivals sat behind the reserved control. Neither local run classifies the 56
recorded x86_64 failures across 14 prior heads; fresh-head CI and the open
review blocker remain separate gates. No control promotion, recovery contract
change, or fixture assertion change is included.

The current follow-up defers a sole automatically eligible QueueLoss before
retry reservation while its exact control remains in the current adapter's
queue. A consumed control supplies a one-shot hint for that Receive call only;
the adapter identity, generation, and marker are checked again before owner
selection. The owner matches the selected demand's ID, revision, cause, and
marker against the final post-rearm eligible fence. Conservative mode checks
the full eligible fence before truncating to one winner. Parked history debt
retains its ticket and eligibility; any other eligible demand, pending
comparison, live caller, or still-pending maintenance observation bypasses
the defer. Explicit, Startup, and required-ID selection also bypass it. This
hint grants no coverage or completion authority and changes no durable retry
state. A composed LocalRelay/AppClient test retains real parked bootstrap debt,
persists a queue omission, drains the admitted prefix, and passes the actual
consumed control through production selection and Receive-continuation helpers.
That helper composition does not itself run the worker loop or a natural
Maintenance tick.

One **unchanged-original** local macOS Receive run failed in 42.80 seconds
(`/tmp/mdk-2060-eligible-control-receive.log`). Its observer never saw the
old simultaneous empty-queue/no-job condition, so it never published the
ordinary Receive stimulus. A Maintenance QueueLoss grant instead held the
target request and returned and queued the missing event; the separate healthy
event was exactly correlated through queue acceptance, dequeue, ingest, and
Alice's plaintext observation. Useful progress under that Maintenance grant
does not satisfy the fixture's strict Receive-seam assertion. The original
failed log remains separate evidence.

The fixture now publishes that same single valid ordinary wake at the first
observed positive queue depth below the existing 1,024-item capacity while no
network job is active. It records the sampled backlog and correlates the
wake's sender inner ID to its published outer transport ID before reading the
bounded router/queue/ingest witness. The strict Receive, active-phase,
response-time, durable target, epoch, plaintext, and pending-debt assertions
are unchanged. The **revised-stimulus** local macOS run passed in 56.79 seconds
(`/tmp/mdk-2060-eligible-control-receive-revised.log`): the send sampled
depth 1,023; the exact wake was accepted at 97 ms, dequeued at 13,938 ms,
and ingested at 13,947 ms. Receive attempt 5 held the target query, returned
and queued the missing event without a later grant, and passed the useful
state and concurrent status/send/live bounds. The separate healthy event was
accepted and dequeued at 14,075 ms, ingested at 14,083 ms, and observed by
Alice at 14,088 ms. The QueueLoss marker and demand remained pending. Queue
depth is an advisory sample, and neither log timestamps the exact control
pop. Four later failed x86_64 attempts at head `8cbd4c7e` bring the recorded
total to 64 across 16 heads. The original 42.80-second failure and revised
56.79-second local pass remain separate evidence. Neither the new queue
selection nor the local pass closes strict recovery or the open review blocker.

A test-only Alice probe now reports three disjoint **completed** direct Receive
intervals: claimed delivery to ingest return, ingest return to Receive tail
completion, and tail completion to the next claimed delivery. A lock-held stop
freezes totals and reports any unfinished parent interval at that boundary.
Runtime ingest is a nested child; it can complete inside a parent later
excluded or unfinished, so subtracting its aggregate from a completed parent
aggregate would not give exact exclusive cost. Existing follow-up counters
also include cycles later excluded for background handoff or joined-group
work, and are not a matched subset of completed second intervals. The third
interval includes waiting, duplicate filtering, overflow, and other worker
arms; it is not a pure scheduler or CPU measure. Errors, reconnect, teardown,
injected discard, background handoff, and joined-group tails are excluded or
invalidated explicitly.

At head `05fa4924`, the stimulated Receive fixture and the nonstimulated
Maintenance-required fixture each failed strict active acquisition on its
first x86_64 attempt and configured retry: four failures total. All four had
a healthy live delivery accepted, dequeued, ingested, and observed by Alice;
both Receive wakes were ingested.
The owner deferred 723–786 selections per run, selected no grant during the
hold, and later made useful target/epoch/plaintext progress under a later
grant while retaining pending debt. These four failures bring the record to
68 x86_64 attempts across 17 heads. The separate online epoch-gap test failed
its first attempt at a conjunction of network-result and queue-route evidence,
then passed its configured retry; that assertion does not identify which leg
failed or establish a cause related to this revision.

The four Alice probe windows lasted 45.17–45.44 seconds. Completed direct
claim-to-ingest spans totaled 43.34–43.70 seconds, with nested runtime ingest
at 15.57–16.40 seconds; completed tails totaled 1.27–1.51 seconds and
between-claim spans 0.32–0.47 seconds. Counts matched in each run, all
handoff/join/error exclusions were zero, and only a short between-claim span
straddled stop. For these matched completed spans, the elapsed residual
outside runtime ingest is 27.02–27.91 seconds; it does not identify a storage
or projection suboperation. Runtime-wide transaction totals contain nested
work and cannot be subtracted as an exclusive partition.

At head `15b9e444`, the same two fixtures again failed strict active
acquisition on their first x86_64 attempts and configured retries: four more
failures, bringing the record to 72 across 18 heads. Each run retained healthy
live accepted/dequeued/ingested/plaintext evidence and later useful
target/epoch/plaintext progress, while leaving queue debt pending. The owner
selected no grant during the hold and deferred 710–742 selections per run;
both Receive wakes were accepted, dequeued, and ingested. The separate
`05fa4924` epoch-gap first-fail/retry-pass cause remains unresolved;
`15b9e444` passing on its first attempt does not establish a fix.

The test-only Alice secure-prune measurement at `15b9e444` completed for
710–742 matching direct messages per run, without error or unfinished calls.
Its inclusive elapsed total was 0.282–0.302 seconds, about 0.65–0.70% of the
matching 43.37–43.52-second completed claim-to-ingest parent total. This does
not support a production no-expiry fast path as the dominant cost.

At head `c908b323`, the stimulated Receive fixture failed strict active
acquisition on both x86_64 attempts despite accepted/dequeued/ingested wakes,
healthy live delivery, and later useful target/epoch/plaintext progress. The
owner selected no grant during the hold and deferred 728 and 758 selections;
debt remained pending. These two failures bring the QueueLoss record to 74
failed x86_64 attempts across 19 heads. The Maintenance-required fixture
passed its first attempt in 92.957 seconds; that positive result does not
identify why the Receive acquisition failed. Separately, the startup-gap
acceptance fixture failed its first attempt at the active-job assertion after
15.544 seconds, then passed its retry in 17.711 seconds. The held relay
handler is a different lifetime from the comparison task witness, so the
startup failure's cause remains unresolved.

The `c908b323` test-only Alice measurement timed the existing
`record_account_app_event_at` call for 728 and 758 matched completed direct
messages. Its inclusive elapsed totals were 16.815 and 16.849 seconds,
about 39% of the respective 43.314-second and 43.394-second completed
claim-to-ingest parents. The call covers account/storage setup, atomic
raw-event, timeline, and chat-list work, and hydration of the returned
update. This total alone does not isolate SQL, transaction commit, or CPU
time. The next test-only
split records account/storage setup, transaction-call entry to actual closure
entry, source/timeline call, inclusive chat-list refresh with nested
presentation hydration, and closure exit to transaction return. The entry
envelope includes owner wait and begin; the return envelope includes
commit/rollback, callbacks, and owner release, and must not be called pure
commit or fsync. Each boundary reports completed count/total/max, result
errors, and unfinished work at the frozen stop; closure-not-entered is counted
separately. Completed children may belong to excluded or unfinished parents,
so comparisons need matched counts and boundaries, not additive subtraction.
The probe does not claim to count panics. Fresh x86_64 evidence for the split
is pending. The PR remains draft without merge, device qualification,
default activation, or release.

Exact `c3ae98f5` CI added four failed x86_64 QueueLoss attempts, reaching 78
across 20 heads. The Receive and Maintenance-required fixtures each failed
strict active acquisition on their first attempt and configured retry; Rust
jobs 3–5 passed. Each failing attempt selected no grant during the hold and
deferred 690–742 selections. Status, send, and healthy live probes passed,
and target retention, epoch advance, and dependent plaintext appeared under
a later grant while debt remained pending. Matching Alice direct measurements
put `record_account_app_event_at` at 16.62–16.87 seconds inclusive within
43.33–43.41 seconds of completed claim-to-ingest time. The nested source and
timeline call took 8.20–8.42 seconds; chat-list refresh took 7.22–7.30
seconds inclusive, including 0.89–1.00 seconds of hydration. The closure-exit
to transaction-return envelope took 1.11–1.27 seconds. These measurements
do not isolate SQL, CPU, fsync, or an exclusive wall-time partition.

The current storage follow-up skips encrypted-media reference replacement
only for a brand-new app event that cannot create media references under the
existing chat component parser. Every existing message ID still takes the
original replacement path, including non-media replays, so prior references
can be retired. There is no new SQL or transaction split. A focused SQLite
trace checks the skipped plain, custom-kind, and malformed media-tag cases
and the retained replay and media paths. Existing encrypted-media tests pass
17/17, including prune and reopen cases; an injected `BEFORE DELETE` failure
checks that earlier app-event and timeline tag updates roll back and that
the reference and retirement state remain unchanged. A successful same-ID
media-to-plain transition retires the reference, and a conflicting replay
retains the original source epoch. Focused app source-epoch tests pass with
default and `test-policy-overrides` features; storage and app all-target
Clippy checks pass in both configurations. The next `ea842568` CI provides
post-change x86_64 evidence below; it does not establish a controlled speedup
or strict recovery qualification.

Exact `ea842568` CI added four more failed x86_64 QueueLoss attempts, reaching
82 across 21 heads. Receive and Maintenance-required each failed strict active
acquisition twice; Rust jobs 3–5 passed. During the held probe, all four had
no selected grant, active NEG, or network job and deferred 718–753 selections.
Status, send, and healthy live probes passed, and useful target retention,
epoch advance, and dependent plaintext appeared under a later grant with debt
still pending. The Receive wakes were accepted, dequeued, and ingested. The
matched Alice source/timeline calls totaled 6.10–8.38 seconds, inclusive
chat-list refresh 5.04–7.49 seconds, and closure-exit to transaction-return
envelopes 1.11–4.27 seconds across 718–753 completed calls per run. Those
boundaries do not isolate SQL, CPU, fsync, or exclusive wall time. A prior
Maintenance pass at `c908b323` and first-pass startup and online epoch-gap
tests on this head do not explain these failures or resolve earlier causes.

The next narrow storage change skips modifier-edge replacement only when the
app event is brand-new and its kind cannot create modifier edges. All existing
message IDs still take the original replacement path, including a replay that
changes from a modifier to a non-modifier; fresh modifiers still insert edges.
The existing app-event and chat-list transaction and projection paths remain.
A connection-local SQL trace confirms no edge delete for new custom-kind and
chat events, and confirms the delete for an existing non-modifier replay and
the delete and insert for a new modifier. Existing retarget, modifier-to-chat,
and prune regressions pass. A test-triggered target timeline failure fires
only after it sees the newly inserted edge; the enclosing transaction rolls
back that edge and the app-event insert while leaving the target timeline row
unchanged. Focused app projection tests, formatting, and default and feature
all-target storage/app Clippy checks pass. This one avoided no-op delete has
no measured contribution to the source/timeline envelope, and it does not
account for chat-list work or the unassigned claim-to-ingest time. No x86_64
strict fixture result exists for this change yet; speedup and strict recovery
qualification remain unmeasured. The PR stays draft.

Exact `57614779` CI then added four more failed x86_64 QueueLoss attempts,
bringing the record to **86 across 22 heads**. Receive and
Maintenance-required each failed the unchanged active-acquisition assertion
on the first attempt and configured retry; Rust jobs 3–5 passed. All four
deferred 468–743 selections and held no selected grant, active NEG, network
job, or request during the probe. Status, send, and healthy live delivery
passed. Both Receive wakes were accepted, dequeued, and ingested. One later
grant returned and queued the target; durable retention, epoch advance, and
both dependent plaintext messages followed with QueueLoss debt still pending.
That later progress does not satisfy the same-held-grant assertion. Earlier
startup and online epoch-gap failure causes remain unresolved despite their
first-pass successes on this head. The modified storage path has no controlled
speedup result.

The next test-only Alice direct-cycle child times the existing
`refresh_group_routes()` call after ingest and before the projection
checkpoint. It adds no route read or state change. The call scans every
projected group and can retire terminal or prior routes even when the current
delivery did not set `routes_dirty`; the existing error and subscription
behavior stay intact. The probe reports completed count, inclusive elapsed
total and maximum, result errors, and an unfinished call at the frozen stop.
A completed child can belong to an excluded or unfinished parent, so its
aggregate is not an exclusive partition of claim-to-ingest time. At
`57614779`, matching Receive parents spent 43.512 and 43.299 seconds, with
runtime ingest 15.749 and 15.790 seconds, app projection 17.085 and 16.698
seconds, and secure prune about 0.296 seconds per run; the remaining time
contains several operations and does not yet identify route refresh cost.
The new measurement has no x86_64 result yet. Strict recovery, same-held-grant
progress, and platform qualification remain open; the PR stays draft.

Exact `565338902` Core CI added three failed x86_64 QueueLoss attempts, for
**89 across 23 heads**. The stimulated Receive fixture failed twice at strict
same-held-grant selection; the Maintenance-required fixture failed first and
passed its retry. Failed holds selected no grant, NEG comparison, or network
job and deferred 733–825 selections. Concurrent status, send, and live delivery
passed, while useful target, epoch, and plaintext progress appeared only under
a later grant with marker and debt pending. The Maintenance retry is a positive
result for that attempt, not a cause or consistency closure. Route refresh
took 0.726–1.308 seconds inclusive within 43.265–44.494-second completed
claim-to-ingest intervals, so it is not the dominant measured child. The
separate scheduled post-convergence comparison test failed its first attempt
at the attempt-serial assertion on line 451 and passed its retry; attribution
and cause remain unresolved.

The next local storage candidate adds migration 0096 with three partial
activity indexes and constrains chat-list preview and accepted high-water
candidate walks to the static kind superset: chat, poll, and group-system.
The full existing activity predicate still decides group-system eligibility
from tags and group state, so many ineligible kind-1210 rows can still be
scanned. The rank, pending fallback, and insertion-order high water remain;
the existing general indexes, including migration 0062's preview index, are
retained. In a file-backed SQLCipher test with
1,024 newer custom-kind events, preview selection used 38,121 SQLite VM steps
before this change and 233 after; accepted high-water selection used 17,442
and 34 steps respectively. With no older eligible chat, the same comparison
was 38,002 to 79 preview steps and 17,427 to 20 high-water steps. These are
query-work counts for that fixture, not elapsed latency, a general bound for
group-system rows, or proof of a 45-second recovery outcome.

The new tests compare selected preview and high water against the original
SQL on each database snapshot, and compare incremental and rebuild rows across
same-ID kind replay, pending and failed sends, poll invalidation, and dynamic
group-system eligibility. A forced chat-row write failure proves rollback of
source, timeline, and chat row inside a storage-level outer transaction; it
does not execute a `MarmotApp` integration failure. App `custom_intent` tests
cover API and intent behavior in default and `test-policy-overrides` modes.
Migration interruption rolls back index creation and preserves populated
rows; upgrade and encrypted reopen pass locally. Migration 0096 initially
scans existing rows to build its indexes, and eligible writes maintain three
additional indexes. Open-time migration duration, index storage, and write
costs remain unmeasured. Existing schema guards cause binaries that know only
through migration 0095 to refuse an upgraded database; no downgrade
compatibility is claimed. Strict x86_64 recovery, device, and platform
qualification remain open; PR #2060 stays draft.

Exact-head Core CI for signed `276203dcb99b283d2d2e81cb29dc155b4a3d285b`
failed both strict QueueLoss fixtures twice, bringing the preserved x86_64
record to **93 failed attempts across 24 heads**. The two Receive attempts
held a real target acquisition on a Maintenance grant despite accepted,
dequeued, successfully ingested ordinary wakes; the fixture required Receive.
The two Maintenance-required attempts also failed strict active acquisition,
though all four eventually recovered useful target, epoch, and plaintext
state. The five full Rust logs and retry classifications remain in the
[exact-head PR evidence](https://github.com/marmot-protocol/mdk/pull/2060#issuecomment-5857916019).
Positive eventual recovery and prior first-pass results do not close those
failed assertions.

The next local candidate lets a qualified different-group ordinary delivery
cross only its current durable, ready, disjoint QueueLoss control. The
adapter returns a queue-identity/generation/token receipt with the delivery;
the successful ordinary Receive may retain its original control deferral
proof across unchanged prefix deliveries. The exact control must complete and
run its original Receive tail once before a completed hint is eligible. In the
biased worker select, this one-shot continuation now precedes a simultaneously
due Maintenance tick, while shutdown, completions, commands, ordinary Receive,
due convergence, and bounded admission keep their earlier positions. A caller
or mutation invalidates the hint; snapshot reads do not. Dispatch takes the
hint before awaiting, and the owner rechecks the original proof against the
current eligible demand, retry serial, and loss/route/inventory fence before
reservation. Once dispatched, stale receipt, no-credit, or generic Deferred
selection spends the opportunity and leaves normal scheduling and durable
debt intact. Earlier due work can keep the hint waiting until the next turn. The
periodic tick body, policy budgets, schema, fixture stimulus, and assertions
are unchanged.

One unchanged full local macOS Receive run of this candidate failed in 32.31
test seconds (Cargo exit 101; 33.087 seconds including its wrapper). The exact
ordinary wake was accepted at 120 ms, dequeued at 125 ms, and ingested at 133
ms, but the sole QueueLoss grant used Maintenance attempt 5. Its full log is
`/tmp/mdk-2060-deferred-control-receive-20260927T182448Z.log`. That run had
no hint-lifecycle witness: it does not prove whether the crossing receipt,
proof, completed control, or ready deferred branch existed. The existing
fixture-owned, default-disabled Alice probe now freezes fixed aggregate counts
at the diagnostic stop for actual receipt return, hint arm/retention/discard,
control completion, and one-shot dispatch/result. It records no identities;
`retained` counts present-to-present transitions, not proof of identity
equality. Focused readiness, probe freeze/isolation, and real-adapter owner
composition tests pass in default and feature builds. The composed test does
not exercise simultaneous readiness in the full worker.

One subsequent **unchanged** full local macOS Receive run passed in 46.56
test seconds (Cargo exit 0; wrapper exit 0). Its complete separate log is
`/tmp/mdk-2060-arbitration-receive-20260927T190328Z.log`. The sole selected
grant was Receive attempt 5 with one QueueLoss obligation. The lifecycle
snapshot recorded one returned crossing, one arm, 1,015 present-to-present
retentions, one exact-control completion, one dispatch attempt, and one job;
all discard and stale categories were zero. The active target job and request
each remained at one through entry and release with about 9.91 and 9.86
seconds left on the deadline. That grant returned and queued the missing
commit; no later grant ran. The fixture passed durable retention, MLS epoch
advance, both dependent plaintexts, and status/send/live bounds while the
QueueLoss marker and demand remained pending without exhaustive admission
evidence. This qualifies the current candidate in one local macOS run. The
earlier failed run's hint readiness remains unknown, and this result does not
qualify x86_64, explain all previous failures, or authorize merge, device
activation, or release. PR #2060 remains draft for fresh exact-head CI review.
