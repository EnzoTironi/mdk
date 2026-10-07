---
title: "Storage Format v2"
created: 2026-08-13
updated: 2026-09-08
tags: [marmot, storage, sqlite, migration, encoding]
status: current
---

# Storage Format v2

This document owns MDK's local SQLCipher storage-format contract. It is not a
Marmot wire format and does not constrain other Marmot implementations. The
canonical protocol specifies which state must be reproducible; this document
specifies how `storage-sqlite` currently persists that state.

## Released transport receipts

The current account schema contains a bounded, account-local release journal. The engine calls `MessageStorage::release_message_for_replay`
when resource policy releases retained raw transport bytes. A backend without
host receipt bookkeeping may use the trait's delete-only default. A backend
that advertises possession or suppresses delivery must atomically revoke those
receipts or persist the evidence needed to revoke them after restart.

The app's receipt stores are advisory transport indexes used for duplicate checks
and bounded reconciliation queries. Engine row absence alone cannot replace
their possession answer: older databases did not backfill every accepted wrapper
into the engine's transport markers. The journal records explicit releases while
preserving these existing query paths; deriving every receipt from engine state
would also need a migration-compatible mapping for historical transport ids.

For app-owned databases, SQLCipher atomically deletes the raw row, removes its
reconciliation inventory and persisted seen entry, and records the id and owning group in
`cgka_released_transport_receipts`. Ordinary message deletion does not create
replay work. The journal retains no payload or secret, cascades when the group
is deleted, and refuses a new release before deleting bytes when 8,192 pending
entries already exist. It never evicts outstanding replay evidence to admit a
new journal entry. App ownership means a durable `account_state` row, established
by MarmotApp before engine hydration. The check shares the release transaction
and does not require an existing receipt: the app may hold only an unsaved
in-memory seen entry. A standalone SQLite engine database without app ownership
uses ordinary deletion and accumulates no journal or app backfill work, so the
journal bound is not a lifetime limit on standalone engine releases. If an
app-owned journal fills, the release error aborts that group's current deferred
sweep, including retries of unrelated rows. Recovery resumes after the app
consumes the journal; retaining the raw bytes and replay evidence takes priority
over progress while the journal is full.

`AppClient::transport_receipts` is the account-owned synchronization boundary.
Its `SynchronizedTransportReceipts` view requires an exclusive client borrow and
owns access to duplicate membership, reconciliation inventory, and pending seen
checkpoint entries. The raw membership index exposes no production lookup API.
The SDK drain passes the same view from its duplicate check into engine admission,
so no intervening engine step or second pre-ingest journal query is needed.
Direct ingestion creates its own view; post-ingest access synchronizes again
because that engine step may have released more input. Account open restores
pending backfill intents through this boundary, including when the journal is
empty. Low-level storage reads and other read-only app APIs retain their existing
behavior; this view is for the owning account client, not concurrent observers.

The app consumes release evidence on account open, before receipt checkpoints,
reconciliation and duplicate shortcuts, after engine ingest, and when observing
engine effects. The empty-journal path remains a bounded existence query, without
rebuilding the seen ring/index or loading all backfill intents. Consumption
again removes both durable receipts (including an intervening stale checkpoint),
arms the existing durable group backfill intent, and acknowledges the journal
in one transaction. The caller then clears its in-memory seen ring/index
synchronously, before any await or fallible operation. Process death before
consumption leaves the journal; death after acknowledgment leaves clean disk
receipts and durable backfill. Neither case relies on delivery of the engine's
in-memory `TransportObjectResourceRefused` event. Completing an older backfill
must retain durable intents for groups rearmed into pending or queued work,
including a new release at the same epoch. The next execution owns their cleanup;
otherwise an old completion can erase the restart recovery edge before that
execution starts. Exact-ID replay still passes through the ordinary authentication
and engine deduplication paths.

This prevents future release/receipt mismatches. It cannot reconstruct releases
that happened before this journal existed; bounded repair of historical receipt
claims is tracked in [#1724](https://github.com/marmot-protocol/mdk/issues/1724).
Missing historical wrapper markers alone do not prove a release, so current storage does not clear existing receipt history indiscriminately.

## Versioning model

Account/session admission is one exact current schema with marker `1 / current_account_schema_v1` in
`cgka_schema_migrations`. A fresh installation and its typed seeds commit atomically; current reopen validates
shape and marker without rewriting domain rows. Historical numbered account chains, future markers, partial
and foreign schemas are refused. The account-device owner performs any explicitly bound coordinated disposable
reset outside the installer. Shared and directory stores retain their independent schema contracts.

Each independently decoded opaque artifact retains its own version. Binary envelopes carry artifact-specific
magic and fixed-width network-order versions. Supported old artifact versions remain readable at their documented
fallback boundaries; writers emit the newest version and malformed/unsupported artifacts fail closed. Golden
byte fixtures pin these blob contracts. Replacing the account installation chain does not change them.

## Binary grammar

MDK-local binary artifacts reuse the Marmot binary grammar for consistency:

- fields appear in the documented order;
- fixed-width integers use network byte order;
- variable byte strings use the shortest canonical QUIC-varint byte-length
  prefix;
- list fields are one QUIC-length-prefixed byte vector containing concatenated
  item encodings;
- option fields use a one-byte discriminator (`0` absent, `1` present);
- decoders reject non-minimal length prefixes, unknown discriminants, truncated
  values, invalid UTF-8 for string fields, and trailing bytes.

Using the same grammar is an MDK implementation convention. It does not make
these storage artifacts Marmot protocol values.

## `cgka_messages` authority model

The current account baseline stores one normalized message-row representation. The scalar
`id`, `group_id`, `epoch`, and `state` columns are authoritative; `payload` is a required
BLOB containing the exact `MessageRecord.payload` bytes, and `deferred_peel` is optional JSON
`DeferredPeelLifecycle` metadata. There is no duplicate message-record blob or row-format discriminator.

State updates change only `state` and clear `deferred_peel` when leaving `PeelDeferred`;
they do not read, decode, copy, or rewrite `payload`. Historical format-1 message schemas
are outside the current baseline and require the separately enumerated coordinated reset.
There is no compatibility decoder, promotion API, or host promotion schedule. Independent
payload, snapshot, and wire protocol versions keep their existing contracts.

Reclaiming freed SQLCipher pages is an explicit maintenance operation; opening
an account never runs `VACUUM` implicitly.

## Processed transport identities

The current `cgka_processed_transport_ids` table separates exact-wrapper replay
evidence from the bounded `cgka_ingress_dedup` exceptional-input cache. An id
enters `cgka_processed_transport_ids` only after a group wrapper peels and its
content is durably admitted. The content row and processed transport id are one
transaction when first admitted, so ingest cannot report a marker-write error
after committing only the canonical row.

Processed transport ids are retained for the lifetime of their owning protocol
group and cascade when that group is deleted. They are not subject to the
4,096-row account-device FIFO used for malformed or otherwise unassociated
terminal ingress. This keeps exact retained-history replay restart-safe without
letting attacker-controlled, unpeelable wrapper ids create unbounded durable
state.

## `StoredMessagePayload` v2

New `MessageRecord.payload` values use this envelope:

```text
opaque magic[5] = "MDKMP";
uint16 version = 2;
uint8 variant;
PayloadVariant body;
```

Variant values are stable:

| Value | Body |
| --- | --- |
| `0` | one `TransportMessage`, `RawTransport` |
| `1` | one `TransportMessage`, `OutboundWelcome` |
| `2` | one `TransportMessage`, `OpenMlsWire` |
| `3` | exact `TransportMessage`, OpenMLS `TransportMessage`, optional own-commit stamp, optional own-application stamp |
| `4` | one `TransportMessage`, required own-commit stamp |

A stored `TransportMessage` contains, in order:

```text
opaque id<V>;
opaque payload<V>;
uint64 timestamp;
MessageId causal_dependencies<V>;
opaque source_utf8<V>;
uint8 envelope_variant; // 0 group message, 1 welcome
opaque envelope_id<V>;  // transport group id or recipient id
```

Each `MessageId` inside `causal_dependencies` is itself a length-prefixed opaque
value. An own-commit stamp contains the length-prefixed committer id, a one-byte
priority (`0` privileged, `1` ordinary), a vector of length-prefixed UTF-8
proposal references, optional checkpoint id, and optional resulting epoch
authenticator. An own-application stamp contains the sender id and source epoch
authenticator.

Each field is limited to at most `2^30` bytes and each decoded list to at most
`2^20` items. These are defensive local resource bounds, not protocol limits.

The decoder falls back from a missing `MDKMP` magic to the previous tagged JSON
envelope and then the oldest bare-`TransportMessage` JSON shape. A value that
starts with `MDKMP` never falls back after a binary decoding failure.

## Snapshot v2

Retained-anchor snapshots no longer copy the message ledger or queued outbound
work. Their 32-invitee capture cost measured roughly 1.23 ms. Full rollback
snapshots still include the live message ledger and measured roughly 11.0 ms,
so that path justified a binary format.

Snapshot and group-state checkpoint v2 start with `MDKS` and network-order
`uint16` version `2`. A full rollback snapshot then contains length-delimited
group JSON, normalized message rows, queued outbound rows, member capabilities,
optional convergence-policy and validation bytes, and raw OpenMLS key/value
rows. A state-scoped snapshot intentionally marks the message and queue fields
absent, so restoring it leaves the live message ledger, pending application
events, and outbound queue untouched. A branch-addressed checkpoint is also
canonical-state-only and additionally omits convergence policy. Message ids,
group ids, payloads, and OpenMLS fields remain raw bytes rather than JSON number
arrays. The exact encoded length is computed before allocating the zeroizing
output buffer; the encoder cannot grow it. Temporary list bodies that can
contain secret material also use zeroizing ownership.

Temporary replay and branch probes use a canonical-state-only restore even when
their retained anchor predates state-scoped capture and contains full message
and queue images. This keeps legacy anchors readable without letting a probe
rewrite live input or outbound-work rows.

A blob without `MDKS` is decoded as the corresponding unversioned JSON v1
snapshot or checkpoint. A blob with `MDKS` never falls back following a v2
error. Full snapshots, state-scoped snapshots, and branch-addressed checkpoints
use the envelope. Decoders preserve the scope distinction: checkpoints reject
rollback-only message, queue, or convergence-policy fields rather than silently
accepting a full snapshot as canonical-state-only.

## Decision evidence

Measured on 2026-08-13 in the release profile on the same local machine and
Criterion harness used to identify the original regression:

| Measurement | Current result |
| --- | --- |
| `create_group/32 invitees` | 15.62–15.73 ms; the pre-v2 current-head baseline was about 28.09 ms |
| retained-anchor capture, empty group | 177–185 us |
| retained-anchor capture, 32 invitees | 1.23–1.29 ms |
| full rollback snapshot, 32 invitees, JSON | 10.95–11.03 ms |
| full rollback snapshot, 32 invitees, v2 | 3.62–4.40 ms |
| representative 16,727-byte Welcome, JSON payload envelope | 67,254 bytes |
| representative 16,727-byte Welcome, explicit MDK v2 envelope | 16,821 bytes |
| MDK v2 encode / decode | about 686 ns / 335 ns |
| JSON encode / decode | about 40.4 us / 114.4 us |

A Postcard 1.1.3 spike produced 16,810 bytes and encoded in about 6.42 us, but
could not deserialize the existing internally tagged `StoredMessagePayload`
Serde shape (`WontImplement`). Supporting it would therefore require a second
Postcard-specific DTO/schema in addition to the explicit compatibility
contract. The explicit MDK format is smaller than JSON, faster than both tested
encoders, already decodes the production type, and has stable golden bytes, so
Postcard is not adopted.

The group-state-only retained-anchor capture is roughly 1.25 ms at the
32-invitee fixture and the headline create benchmark is below the 20 ms target.
Snapshot v2 is justified by the separately measured full rollback path, not by
the obsolete pre-#1406 claim that retained anchors still contain messages.

## Operational policy

- Fresh account/session databases install the direct current schema in one immediate transaction; current reopen
  preserves the marker, presentation epoch and committed domain data. Historical schemas require a separately
  enumerated, reversible coordinated disposable account-device reset. The installer never migrates or repairs them.
- Current message writes and cold reopen preserve normalized payload and deferred metadata exactly; historical
  message schemas are refused rather than decoded or promoted.
- SQLCipher key presentation, compatibility, memory security, PRAGMAs, default profile and native dependencies remain
  unchanged. Opening never runs implicit VACUUM or resets a database.
- Complete native schema/typed-seed parity, marker/refusal/rollback and owned before/after-commit process-death tests
  live under crates/storage-sqlite/src/account_schema. Existing blob golden bytes and current recovery, stall,
  attachment and bounded query tests retain their contracts.

## SQLCipher key presentation and KDF work factor (decision record, mdk#1439)

Every `storage-sqlite` database is keyed with 256-bit key material derived by
HKDF over the account secret and a stable per-database salt
(`crates/marmot-app/src/sqlcipher.rs`). The derived bytes are hex-encoded and
presented to SQLCipher as a *passphrase*. With `cipher_compatibility = 4`
pinned and no `cipher_kdf_iterations` override anywhere in the workspace,
SQLCipher then runs PBKDF2-HMAC-SHA512 with 256,000 iterations on every keyed
open. That KDF exists to resist dictionary attacks on human passphrases; the
HKDF-derived input is already uniform high-entropy key material, so the
stretching adds no effective security on this path — its cost is pure latency
(~50–500 ms per derivation depending on device class).

**Decision (2026-08-14): keep passphrase presentation and the compat-4 KDF
work factor unchanged.** Do not adopt raw `x'…'` key presentation or a reduced
`cipher_kdf_iterations` as part of the mdk#1439 performance work. The measured
win ships without touching on-disk cipher parameters: the in-process v2-open
verdict cache makes each existing-database open pay the KDF once instead of
twice, and the remaining open-count reduction is tracked by mdk#1436.

Rationale for rejecting the key-presentation change in this context:

1. **It is a storage-format change, not a tuning change.** Raw-key presentation
   or a reduced iteration count changes the on-disk cipher parameters of every
   existing database. That requires a numbered, crash-safe `PRAGMA rekey`
   migration with the same marker/sidecar discipline as the salt migration,
   plus a rollback plan — and a reduced iteration count additionally becomes a
   de-facto open parameter that must be pinned before keying on *every* open
   path (account storage, hardened cache opens, probe/rekey opens, and external
   tooling).
2. **It requires explicit security sign-off.** Raw presentation is sound only
   because every presented key is HKDF-derived 256-bit uniform material; that
   invariant must be enforced by construction (a typed raw-key variant of
   `SqlCipherKey` that no user-supplied passphrase can ever reach) before
   adoption. A lowered `cipher_kdf_iterations` would instead weaken brute-force
   resistance for any future low-entropy passphrase that reaches the same
   pragma path.
3. **The trade is nil in both directions today, so measure first.** With the
   verdict cache, per-open KDF count is observable in the fleet via
   `app_sqlcipher_migration_probe_runs`/`app_sqlcipher_migration_probe_skips`
   and the `app_account_open`/`app_account_session_open` duration buckets
   (`telemetry.md`). If those numbers still show open-latency pain after
   mdk#1436 lands, the accept path below can be evaluated on evidence rather
   than stacked silently into a performance fix.

Accept path, if revisited: a versioned rekey migration guarded by a durable
marker (mirroring the salt migration's crash windows), no change to hardening
semantics (`cipher_compatibility` pin, `cipher_memory_security`,
`secure_delete`, `synchronous=FULL`), a downgrade story (rekey back to
passphrase presentation), typed raw-key construction in `storage-sqlite`, and
updated forensic/tooling docs (raw-keyed databases need raw-key open syntax in
the `sqlcipher` CLI).
