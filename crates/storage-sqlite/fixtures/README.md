# Current account/session baseline evidence

Numbered historical account-upgrade fixtures and their writers are retired. The direct current installer refuses those
old applied schemas. `src/account_schema/reference.json` records the actual native final schema and typed seeds, with
only historical ledger rows removed and the proven presentation epoch represented by a fresh-random rule.
`src/account_schema/tests.rs` pins complete schema/seed/security parity, one fresh marker, unchanged reopen, refusal,
transaction failures and owned process-death boundaries. Artifact-local format-1/format-2 message, payload, snapshot
and checkpoint contracts remain covered by current domain tests; no old database installation path is required.
