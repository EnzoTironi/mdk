//! One current account/session schema. Historical account databases require an external reset.
use crate::SqliteResultExt;
#[cfg(feature = "diagnostic-account-open-witness")]
use crate::connection::{
    DiagnosticMigrationStep, DiagnosticOpenBoundary, DiagnosticOpenEvent, DiagnosticOpenObserver,
};
use cgka_traits::storage::{StorageError, StorageResult};
use rusqlite::{Connection, TransactionBehavior, params};
#[cfg(test)]
mod query_work_tests;
mod shape;
#[cfg(test)]
mod tests;

const VERSION: i64 = 1;
const NAME: &str = "current_account_schema_v1";
const SQL: &str = include_str!("account_schema/current.sql");

pub(crate) fn install(
    connection: &mut Connection,
    #[cfg(feature = "diagnostic-account-open-witness")] observer: Option<
        DiagnosticOpenObserver<'_>,
    >,
) -> StorageResult<usize> {
    #[cfg(feature = "diagnostic-account-open-witness")]
    let boundary = DiagnosticOpenBoundary::enter(
        observer,
        DiagnosticOpenEvent::Migration {
            version: VERSION,
            step: DiagnosticMigrationStep::Begin,
        },
    );
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .storage()?;
    #[cfg(feature = "diagnostic-account-open-witness")]
    boundary.returned();

    #[cfg(feature = "diagnostic-account-open-witness")]
    let boundary = DiagnosticOpenBoundary::enter(
        observer,
        DiagnosticOpenEvent::Migration {
            version: VERSION,
            step: DiagnosticMigrationStep::Apply,
        },
    );
    let objects: i64 = tx
        .query_row("SELECT COUNT(*) FROM main.sqlite_schema", [], |row| {
            row.get(0)
        })
        .storage()?;
    let applied = if objects == 0 {
        tx.execute_batch(SQL).storage()?;
        #[cfg(test)]
        test_boundary("apply")?;
        validate_shape(&tx)?;
        1
    } else {
        validate_shape(&tx)?;
        let markers = {
            let mut query = tx.prepare("SELECT version, name, applied_at_unix_seconds FROM cgka_schema_migrations ORDER BY version LIMIT 2").storage()?;
            let rows = query
                .query_map([], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                })
                .storage()?;
            rows.collect::<rusqlite::Result<Vec<_>>>().storage()?
        };
        if let Some((version, _, _)) = markers.iter().find(|(version, _, _)| *version > VERSION) {
            return Err(StorageError::UnsupportedSchemaVersion {
                found: *version,
                latest_supported: VERSION,
            });
        }
        if markers.len() != 1 || markers[0].0 != VERSION || markers[0].1 != NAME || markers[0].2 < 0
        {
            return Err(StorageError::Backend(
                "account schema marker is not the current baseline".into(),
            ));
        }
        0
    };
    #[cfg(feature = "diagnostic-account-open-witness")]
    boundary.returned();

    if applied == 1 {
        #[cfg(feature = "diagnostic-account-open-witness")]
        let boundary = DiagnosticOpenBoundary::enter(
            observer,
            DiagnosticOpenEvent::Migration {
                version: VERSION,
                step: DiagnosticMigrationStep::LedgerInsert,
            },
        );
        tx.execute("INSERT INTO cgka_schema_migrations(version, name, applied_at_unix_seconds) VALUES (?1, ?2, CAST(strftime('%s', 'now') AS INTEGER))", params![VERSION, NAME]).storage()?;
        #[cfg(test)]
        test_boundary("ledger")?;
        #[cfg(feature = "diagnostic-account-open-witness")]
        boundary.returned();
    }
    #[cfg(feature = "diagnostic-account-open-witness")]
    let boundary = DiagnosticOpenBoundary::enter(
        observer,
        DiagnosticOpenEvent::Migration {
            version: VERSION,
            step: DiagnosticMigrationStep::Commit,
        },
    );
    #[cfg(test)]
    test_boundary("before_commit")?;
    tx.commit().storage()?;
    #[cfg(feature = "diagnostic-account-open-witness")]
    boundary.returned();
    #[cfg(test)]
    test_boundary("after_commit")?;
    Ok(applied)
}

fn validate_shape(connection: &Connection) -> StorageResult<()> {
    let actual = {
        let mut query = connection.prepare("SELECT type, name, tbl_name, sql FROM main.sqlite_schema WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name LIMIT ?1").storage()?;
        let rows = query
            .query_map([shape::OBJECTS.len() as i64 + 1], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })
            .storage()?;
        rows.collect::<rusqlite::Result<Vec<_>>>().storage()?
    };
    if actual.len() != shape::OBJECTS.len()
        || actual.iter().zip(shape::OBJECTS).any(|(actual, expected)| {
            (
                actual.0.as_str(),
                actual.1.as_str(),
                actual.2.as_str(),
                actual.3.as_str(),
            ) != *expected
        })
    {
        return Err(StorageError::Backend(
            "account schema shape differs from the current baseline".into(),
        ));
    }
    let sequence_sql: String = connection
        .query_row(
            "SELECT sql FROM main.sqlite_schema WHERE type='table' AND name='sqlite_sequence'",
            [],
            |row| row.get(0),
        )
        .storage()?;
    if sequence_sql != "CREATE TABLE sqlite_sequence(name,seq)" {
        return Err(StorageError::Backend(
            "account sequence schema differs from the current baseline".into(),
        ));
    }
    for (table, key) in [
        ("account_recovery_comparison", "singleton"),
        ("account_recovery_state", "singleton"),
        ("attachment_partial_usage", "id"),
        ("attachment_retention_usage", "id"),
        ("avatar_cache_meta", "id"),
        ("cgka_maintenance_settings", "singleton"),
        ("chat_list_navigation_meta", "id"),
        ("chat_list_projection_meta", "id"),
        ("chat_presentation_checkpoint", "id"),
        ("chat_presentation_meta", "id"),
        ("content_report_backfill", "singleton"),
        ("message_draft_revision_clock", "id"),
        ("outgoing_attachment_recovery_cursor", "id"),
        ("user_block_list", "id"),
    ] {
        let valid: bool = connection
            .query_row(
                &format!("SELECT COUNT(*) = 1 AND MIN(\"{key}\") = 1 FROM \"{table}\""),
                [],
                |row| row.get(0),
            )
            .storage()?;
        if !valid {
            return Err(StorageError::Backend(format!(
                "current account singleton is missing or malformed: {table}"
            )));
        }
    }
    for (table, predicate) in [
        (
            "account_recovery_comparison",
            r#"typeof("singleton") = 'integer' AND typeof("plan_format") = 'integer' AND typeof("plan_payload") IN ('blob','null') AND typeof("last_outcome") IN ('integer','null') AND typeof("revision") = 'integer' AND typeof("settled_revision") = 'integer' AND typeof("request_key") IN ('blob','null') AND typeof("requested_at_ms") = 'integer' AND typeof("requested_until_seconds") = 'integer' AND typeof("blocked_route_revision") IN ('integer','null') AND typeof("blocked_capability_key") IN ('blob','null') AND typeof("attempt_serial") = 'integer' AND typeof("frozen_revision") = 'integer'"#,
        ),
        (
            "account_recovery_state",
            r#"typeof("singleton") = 'integer' AND typeof("next_engine_observation") = 'integer' AND typeof("next_stall_sample") = 'integer' AND typeof("next_attempt") = 'integer' AND typeof("loss_revision") = 'integer' AND typeof("route_revision") = 'integer' AND typeof("inventory_revision") = 'integer' AND typeof("retry_ordinal") = 'integer' AND typeof("retry_recorded_at_ms") = 'integer' AND typeof("retry_delay_ms") = 'integer' AND typeof("retry_not_before_ms") = 'integer' AND typeof("route_snapshot") IN ('blob','null')"#,
        ),
        (
            "attachment_partial_usage",
            r#"typeof("id") = 'integer' AND typeof("reserved_bytes") = 'integer'"#,
        ),
        (
            "attachment_retention_usage",
            r#"typeof("id") = 'integer' AND typeof("byte_count") = 'integer'"#,
        ),
        (
            "avatar_cache_meta",
            r#"typeof("id") = 'integer' AND typeof("access_seq") = 'integer'"#,
        ),
        (
            "cgka_maintenance_settings",
            r#"typeof("singleton") = 'integer' AND typeof("periodic_policy") = 'integer'"#,
        ),
        (
            "chat_list_navigation_meta",
            r#"typeof("id") = 'integer' AND typeof("revision") = 'integer' AND typeof("pin_rewrite_in_progress") = 'integer'"#,
        ),
        (
            "chat_list_projection_meta",
            r#"typeof("id") = 'integer' AND typeof("mention_counts_version") = 'integer'"#,
        ),
        (
            "chat_presentation_checkpoint",
            r#"typeof("id") = 'integer' AND typeof("generation") = 'integer' AND typeof("state") IN ('blob','null')"#,
        ),
        (
            "chat_presentation_meta",
            r#"typeof("id") = 'integer' AND typeof("store_epoch") = 'blob' AND typeof("revision") = 'integer'"#,
        ),
        (
            "content_report_backfill",
            r#"typeof("singleton") = 'integer' AND typeof("after_order") = 'integer' AND typeof("through_order") = 'integer'"#,
        ),
        (
            "message_draft_revision_clock",
            r#"typeof("id") = 'integer' AND typeof("revision") = 'integer'"#,
        ),
        (
            "outgoing_attachment_recovery_cursor",
            r#"typeof("id") = 'integer' AND typeof("group_id_hex") = 'text' AND typeof("message_id_hex") = 'text' AND typeof("upload_token") = 'blob'"#,
        ),
        (
            "user_block_list",
            r#"typeof("id") = 'integer' AND typeof("revision") = 'integer' AND typeof("event_id") = 'text' AND typeof("event_created_at") = 'integer' AND typeof("public_tags") = 'text' AND typeof("private_tags") = 'text' AND typeof("unreadable_event_id") = 'text' AND typeof("unreadable_event_created_at") = 'integer'"#,
        ),
    ] {
        let valid: bool = connection
            .query_row(&format!("SELECT {predicate} FROM \"{table}\""), [], |row| {
                row.get(0)
            })
            .storage()?;
        if !valid {
            return Err(StorageError::Backend(format!(
                "current account singleton has invalid value types: {table}"
            )));
        }
    }
    let epoch_valid: bool = connection.query_row("SELECT typeof(store_epoch)='blob' AND length(store_epoch)=16 FROM chat_presentation_meta WHERE id=1", [], |row| row.get(0)).storage()?;
    if !epoch_valid {
        return Err(StorageError::Backend(
            "current account presentation epoch is malformed".into(),
        ));
    }
    let sequence_names = [
        "account_delivery_spill",
        "app_events",
        "cgka_ingress_dedup",
        "cgka_messages",
        "cgka_outbound_fanout",
        "cgka_queued_outbound",
        "local_message_submissions",
    ];
    let sequences = {
        let mut query = connection
            .prepare("SELECT name, seq FROM sqlite_sequence LIMIT 8")
            .storage()?;
        let rows = query
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })
            .storage()?;
        rows.collect::<rusqlite::Result<Vec<_>>>().storage()?
    };
    let mut seen = std::collections::BTreeSet::new();
    for (name, sequence) in sequences {
        if sequence < 0 || !sequence_names.contains(&name.as_str()) || !seen.insert(name) {
            return Err(StorageError::Backend(
                "current account sequence is foreign, malformed or duplicated".into(),
            ));
        }
    }
    if !seen.contains("cgka_messages") || !seen.contains("cgka_queued_outbound") {
        return Err(StorageError::Backend(
            "current account required sequence seeds are missing".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
thread_local! { static FAILURE: std::cell::Cell<Option<&'static str>> = const { std::cell::Cell::new(None) }; }
#[cfg(test)]
fn test_boundary(step: &'static str) -> StorageResult<()> {
    if FAILURE.with(|failure| failure.get() == Some(step)) {
        return Err(StorageError::Backend(
            "injected account schema setup failure".into(),
        ));
    }
    if std::env::var("MDK_ACCOUNT_SCHEMA_CRASH_STEP").as_deref() == Ok(step) {
        let ready =
            std::env::var_os("MDK_ACCOUNT_SCHEMA_CRASH_READY").expect("owned boundary ready path");
        std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(ready)
            .expect("create owned boundary ready file");
        loop {
            std::thread::park();
        }
    }
    Ok(())
}
