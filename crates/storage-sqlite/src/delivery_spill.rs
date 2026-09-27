//! Durable overflow tail of the bounded in-memory account delivery queue.
//!
//! When the account queue is full, the relay router hands deliveries to this
//! table instead of dropping them. The account worker later admits each row
//! through the ordinary ingest path and removes it afterwards, so a spilled
//! delivery stays durable until ingest has seen it.
use crate::connection::{CachedSql, retry_on_busy};
use crate::{SqliteAccountStorage, SqliteResultExt, i64_to_u64, u64_to_i64};
use cgka_traits::TransportDelivery;
use cgka_traits::storage::StorageResult;
use rusqlite::{Connection, params};

/// Upper bounds on the spill table. A delivery that would exceed either one is
/// reported [`DeliverySpillDisposition::Full`] and left to the loss path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeliverySpillLimits {
    pub max_rows: u64,
    pub max_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliverySpillDisposition {
    /// Durably held for later admission, including a copy that was already
    /// spilled.
    Stored,
    /// Already durably seen and not released for redelivery, so there is
    /// nothing to keep.
    AlreadySeen,
    /// No room within the limits. The caller must treat the delivery as lost.
    Full,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SpilledDelivery {
    pub seq: i64,
    pub delivery: TransportDelivery,
}

/// Artifact-local version of the `metadata` encoding.
const SPILL_FORMAT: i64 = 1;

/// The payload keeps its own column. JSON would inflate raw ciphertext about
/// threefold, so only the small remaining metadata is serialized.
struct EncodedDelivery {
    event_id: Vec<u8>,
    payload: Vec<u8>,
    metadata: Vec<u8>,
    bytes: u64,
}

impl EncodedDelivery {
    fn encode(mut delivery: TransportDelivery) -> StorageResult<Self> {
        let payload = std::mem::take(&mut delivery.message.payload);
        let metadata = crate::codec::serialize(&delivery)?;
        Ok(Self {
            event_id: delivery.message.id.into_bytes(),
            bytes: (payload.len() + metadata.len()) as u64,
            payload,
            metadata,
        })
    }
}

fn already_seen(conn: &Connection, event_id: &[u8]) -> StorageResult<bool> {
    // Release journals the ID before the app's seen ring is rewritten, so a
    // journaled receipt still needs redelivery even while `seen_events` lists it.
    conn.query_row_cached(
        "SELECT EXISTS(SELECT 1 FROM seen_events WHERE event_id=?1)
            AND NOT EXISTS(SELECT 1 FROM cgka_released_transport_receipts WHERE id=?2)",
        params![hex::encode(event_id), event_id],
        |row| row.get(0),
    )
    .storage()
}

impl SqliteAccountStorage {
    /// Spill deliveries in order and report one disposition per input.
    pub fn spill_account_deliveries(
        &self,
        deliveries: Vec<TransportDelivery>,
        limits: DeliverySpillLimits,
        now_secs: u64,
    ) -> StorageResult<Vec<DeliverySpillDisposition>> {
        let encoded = deliveries
            .into_iter()
            .map(EncodedDelivery::encode)
            .collect::<StorageResult<Vec<_>>>()?;
        let spilled_at = u64_to_i64(now_secs)?;
        retry_on_busy(|| {
            self.connection.with_transaction(|| {
                let conn = self.lock()?;
                let (rows, bytes): (i64, i64) = conn
                    .query_row_cached(
                        "SELECT COUNT(*), COALESCE(SUM(bytes), 0) FROM account_delivery_spill",
                        [],
                        |row| Ok((row.get(0)?, row.get(1)?)),
                    )
                    .storage()?;
                let (mut rows, mut bytes) = (i64_to_u64(rows)?, i64_to_u64(bytes)?);
                let mut dispositions = Vec::with_capacity(encoded.len());
                for delivery in &encoded {
                    if already_seen(&conn, &delivery.event_id)? {
                        dispositions.push(DeliverySpillDisposition::AlreadySeen);
                        continue;
                    }
                    if rows >= limits.max_rows
                        || bytes.saturating_add(delivery.bytes) > limits.max_bytes
                    {
                        dispositions.push(DeliverySpillDisposition::Full);
                        continue;
                    }
                    let inserted = conn
                        .execute_cached(
                            "INSERT OR IGNORE INTO account_delivery_spill
                                (event_id, payload, metadata, format, bytes, spilled_at)
                             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                            params![
                                delivery.event_id,
                                delivery.payload,
                                delivery.metadata,
                                SPILL_FORMAT,
                                u64_to_i64(delivery.bytes)?,
                                spilled_at
                            ],
                        )
                        .storage()?;
                    if inserted > 0 {
                        rows += 1;
                        bytes = bytes.saturating_add(delivery.bytes);
                    }
                    dispositions.push(DeliverySpillDisposition::Stored);
                }
                Ok(dispositions)
            })
        })
    }

    /// Oldest spilled deliveries first. A row whose metadata no longer decodes
    /// is removed rather than blocking every later row.
    pub fn spilled_account_deliveries(&self, limit: usize) -> StorageResult<Vec<SpilledDelivery>> {
        let conn = self.lock()?;
        let rows = conn
            .prepare_cached(
                "SELECT seq, payload, metadata FROM account_delivery_spill ORDER BY seq LIMIT ?1",
            )
            .storage()?
            .query_map([u64_to_i64(limit as u64)?], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .storage()?
            .collect::<Result<Vec<_>, _>>()
            .storage()?;
        let mut spilled = Vec::with_capacity(rows.len());
        for (seq, payload, metadata) in rows {
            match crate::codec::deserialize::<TransportDelivery>(&metadata) {
                Ok(mut delivery) => {
                    delivery.message.payload = payload;
                    spilled.push(SpilledDelivery { seq, delivery });
                }
                Err(_) => {
                    conn.execute_cached("DELETE FROM account_delivery_spill WHERE seq=?1", [seq])
                        .storage()?;
                }
            }
        }
        Ok(spilled)
    }

    pub fn remove_spilled_account_delivery(&self, seq: i64) -> StorageResult<()> {
        self.lock()?
            .execute_cached("DELETE FROM account_delivery_spill WHERE seq=?1", [seq])
            .storage()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::test_support::{gid, sample_group};
    use cgka_traits::storage::GroupStorage;
    use cgka_traits::transport::{TransportEnvelope, TransportMessage, TransportSource};
    use cgka_traits::{MemberId, MessageId, TransportDeliveryPlane, TransportDeliverySource};

    const LIMITS: DeliverySpillLimits = DeliverySpillLimits {
        max_rows: 3,
        max_bytes: u64::MAX,
    };

    fn delivery(id: u8) -> TransportDelivery {
        TransportDelivery {
            account_id: MemberId::new(vec![0xAA; 32]),
            group_id_hint: None,
            message: TransportMessage {
                id: MessageId::new(vec![id; 32]),
                payload: vec![id; 64],
                timestamp: cgka_traits::transport::Timestamp(u64::from(id)),
                causal_deps: Vec::new(),
                source: TransportSource("nostr".to_owned()),
                envelope: TransportEnvelope::GroupMessage {
                    transport_group_id: vec![0x42; 32],
                },
            },
            received_at: cgka_traits::transport::Timestamp(7),
            source: TransportDeliverySource {
                transport: TransportSource("nostr".to_owned()),
                plane: TransportDeliveryPlane::Group,
                endpoint: None,
                subscription_id: Some("live".to_owned()),
                wire: None,
            },
        }
    }

    fn mark_seen(store: &SqliteAccountStorage, id: u8) {
        store
            .lock()
            .unwrap()
            .execute_cached(
                "INSERT INTO seen_events(event_id, seen_at) VALUES (?1, 1)",
                [hex::encode([id; 32])],
            )
            .unwrap();
    }

    #[test]
    fn spill_round_trips_in_order_and_removes_rows() {
        let store = SqliteAccountStorage::in_memory().unwrap();
        let outcome = store
            .spill_account_deliveries(vec![delivery(2), delivery(1)], LIMITS, 9)
            .unwrap();
        assert_eq!(outcome, vec![DeliverySpillDisposition::Stored; 2]);

        let spilled = store.spilled_account_deliveries(10).unwrap();
        let deliveries: Vec<_> = spilled.iter().map(|row| row.delivery.clone()).collect();
        assert_eq!(deliveries, vec![delivery(2), delivery(1)]);

        store
            .remove_spilled_account_delivery(spilled[0].seq)
            .unwrap();
        let remaining = store.spilled_account_deliveries(10).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].delivery, delivery(1));
    }

    #[test]
    fn spill_skips_seen_events_but_keeps_released_ones() {
        let store = SqliteAccountStorage::in_memory().unwrap();
        mark_seen(&store, 1);
        mark_seen(&store, 2);
        let group = gid(1);
        store.put_group(&sample_group(group.clone(), 0, 2)).unwrap();
        store
            .lock()
            .unwrap()
            .execute_cached(
                "INSERT INTO cgka_released_transport_receipts(id, group_id, epoch) VALUES (?1, ?2, 0)",
                params![vec![2_u8; 32], group.as_slice()],
            )
            .unwrap();

        let outcome = store
            .spill_account_deliveries(vec![delivery(1), delivery(2), delivery(3)], LIMITS, 9)
            .unwrap();
        assert_eq!(
            outcome,
            vec![
                DeliverySpillDisposition::AlreadySeen,
                DeliverySpillDisposition::Stored,
                DeliverySpillDisposition::Stored,
            ]
        );
    }

    #[test]
    fn spill_reports_full_at_row_and_byte_limits_and_dedups_copies() {
        let store = SqliteAccountStorage::in_memory().unwrap();
        let outcome = store
            .spill_account_deliveries(
                vec![
                    delivery(1),
                    delivery(1),
                    delivery(2),
                    delivery(3),
                    delivery(4),
                ],
                LIMITS,
                9,
            )
            .unwrap();
        use DeliverySpillDisposition::{Full, Stored};
        assert_eq!(outcome, vec![Stored, Stored, Stored, Stored, Full]);
        assert_eq!(store.spilled_account_deliveries(10).unwrap().len(), 3);

        let tight = DeliverySpillLimits {
            max_rows: u64::MAX,
            max_bytes: 0,
        };
        let empty = SqliteAccountStorage::in_memory().unwrap();
        assert_eq!(
            empty
                .spill_account_deliveries(vec![delivery(5)], tight, 9)
                .unwrap(),
            vec![Full]
        );
    }

    #[test]
    fn undecodable_rows_are_dropped_instead_of_blocking_the_spill() {
        let store = SqliteAccountStorage::in_memory().unwrap();
        store
            .spill_account_deliveries(vec![delivery(1), delivery(2)], LIMITS, 9)
            .unwrap();
        store
            .lock()
            .unwrap()
            .execute_cached(
                "UPDATE account_delivery_spill SET metadata=x'00' WHERE event_id=?1",
                [vec![1_u8; 32]],
            )
            .unwrap();
        let spilled = store.spilled_account_deliveries(10).unwrap();
        assert_eq!(spilled.len(), 1);
        assert_eq!(spilled[0].delivery, delivery(2));
        assert_eq!(store.spilled_account_deliveries(10).unwrap().len(), 1);
    }
}
