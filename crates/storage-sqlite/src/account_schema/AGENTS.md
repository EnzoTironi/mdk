# Current account/session schema

../account_schema.rs installs current.sql in one immediate transaction. shape.rs freezes the exact current schema.
Preserve reference.json unchanged as the native-derived 314-object oracle after the previously explicit marker/ledger/epoch deltas.
tests.rs applies only the exact cgka_messages delta: remove record/storage_format, require a typed BLOB payload,
and remove idx_cgka_messages_legacy_order, yielding 313 explicit objects. Every other schema object, typed seed,
foreign key, index, view, trigger and all seven AUTOINCREMENT sequence rows remain exact.
Current normalized message payloads, including empty BLOBs, survive public reads and exact cold reopen;
invalid missing/null/non-BLOB payloads must fail without writes. There is no historical record promotion path.
Do not add numbered account migrations, compatibility aliases, repair/backfill paths or implicit resets.
query_work_tests.rs retains current bounded-query and populated-reopen assertions; obsolete promotion work is removed.
Shared-store and directory-cache schemas remain independent. Root alone owns any exact manifested disposable account-device reset.
