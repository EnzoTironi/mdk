//! Keep non-activity custom events out of the ordered chat-list candidate walks.
use crate::SqliteResultExt;
use cgka_traits::storage::StorageResult;
use rusqlite::Transaction;

pub(crate) fn apply(tx: &Transaction<'_>) -> StorageResult<()> {
    // The kind set is the static superset of chat-list activity: chat (9),
    // poll (1068), and group-system (1210). The query still applies the full
    // tag and group-profile predicate to decide which system rows qualify.
    tx.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_message_timeline_chat_activity_preview
             ON message_timeline (
                 group_id_hex,
                 CASE
                     WHEN direction = 'sent'
                      AND invalidation_status = 'local_publish_failed' THEN 0
                     WHEN direction = 'sent'
                      AND source_message_id_hex IS NULL
                      AND invalidation_status IS NULL THEN 2
                     ELSE 1
                 END DESC,
                 timeline_order_class DESC,
                 timeline_order_primary DESC,
                 timeline_order_phase DESC,
                 timeline_order_at DESC,
                 message_id_hex DESC
             ) WHERE kind IN (9, 1068, 1210);
         CREATE INDEX IF NOT EXISTS idx_app_events_chat_activity_order
             ON app_events (group_id_hex, insert_order DESC)
             WHERE kind IN (9, 1068, 1210);
         CREATE INDEX IF NOT EXISTS idx_app_events_accepted_chat_activity_order
             ON app_events (group_id_hex, insert_order DESC)
             WHERE (direction != 'sent' OR source_message_id_hex IS NOT NULL)
               AND kind IN (9, 1068, 1210);",
    )
    .storage()
}
