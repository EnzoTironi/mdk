//! Account-worker side of the durable delivery spill.
//!
//! Spilled rows are read back in small batches and alternated with live
//! deliveries, so neither backlog starves the other. A row is removed only
//! once its event is in the seen index, the same test dedup uses, so a newer
//! live delivery can move the cursor past a spilled one without losing it.
//! Rows whose ingest left no durable trace are retried later with backoff.

use std::collections::VecDeque;

use cgka_traits::TransportDelivery;
use storage_sqlite::{SpilledDelivery, SpilledDeliveryRetry};

use super::AppClient;
use crate::AppError;
use crate::relay_plane::AccountDeliveryReceive;
use crate::unix_now_seconds;

const SPILL_READ_BATCH: usize = 32;
/// About three hours of doubling retries before an unadmittable row goes.
const SPILL_MAX_ATTEMPTS: u32 = 8;

pub(crate) struct DeliverySpillReader {
    buffered: VecDeque<SpilledDelivery>,
    /// Due rows may exist beyond `buffered`. Starts true, so rows left by an
    /// earlier process are admitted.
    maybe_rows: bool,
    /// Earliest retry time of a deferred row.
    next_retry_at: Option<u64>,
    live_turn: bool,
    /// Row whose delivery is with ingest, and that delivery's event ID.
    in_flight: Option<(i64, String)>,
}

impl Default for DeliverySpillReader {
    fn default() -> Self {
        Self {
            buffered: VecDeque::new(),
            maybe_rows: true,
            next_retry_at: None,
            live_turn: false,
            in_flight: None,
        }
    }
}

impl DeliverySpillReader {
    pub(super) fn pending(&mut self) -> bool {
        if self
            .next_retry_at
            .is_some_and(|at| at <= unix_now_seconds())
        {
            self.next_retry_at = None;
            self.maybe_rows = true;
        }
        !self.buffered.is_empty() || self.maybe_rows
    }
}

impl AppClient {
    /// A delivery that is ready without waiting: while spilled rows remain,
    /// alternate a queued live delivery with the oldest spilled one.
    pub(super) fn take_ready_delivery(
        &mut self,
    ) -> Result<Option<AccountDeliveryReceive>, AppError> {
        if !self.delivery_spill.pending() {
            return Ok(None);
        }
        self.delivery_spill.live_turn = !self.delivery_spill.live_turn;
        if self.delivery_spill.live_turn
            && let Some(received) = self.adapter.try_receive_account_delivery()
        {
            return Ok(Some(received));
        }
        Ok(self
            .next_spilled_delivery()?
            .map(|delivery| AccountDeliveryReceive::Delivery(Box::new(delivery))))
    }

    fn next_spilled_delivery(&mut self) -> Result<Option<TransportDelivery>, AppError> {
        if self.delivery_spill.buffered.is_empty() && self.delivery_spill.maybe_rows {
            let storage = self.app.account_storage(&self.state.label)?;
            let batch = storage.spilled_account_deliveries(SPILL_READ_BATCH, unix_now_seconds())?;
            if batch.discarded > 0 {
                // Undecodable rows are gone and their content is unknown.
                // Record them as queue loss so recovery keeps an obligation.
                let token = rand::RngCore::next_u64(&mut rand::rngs::OsRng) & i64::MAX as u64;
                storage.record_account_recovery_loss(
                    &self.state.label,
                    storage_sqlite::RecoveryLossCause::Queue,
                    token,
                    batch.discarded,
                    unix_now_seconds(),
                )?;
                storage.synchronize_account_delivery_loss(&self.state.label)?;
                tracing::warn!(
                    target: "marmot_app::client::delivery_spill",
                    method = "next_spilled_delivery",
                    error_kind = "undecodable_spill_rows",
                    discarded = batch.discarded,
                    "removed undecodable spilled deliveries and recorded them as queue loss",
                );
            }
            self.delivery_spill.maybe_rows = batch.more;
            self.delivery_spill.next_retry_at = batch.next_retry_at;
            self.delivery_spill.buffered.extend(batch.deliveries);
        }
        Ok(self.delivery_spill.buffered.pop_front().map(|row| {
            let event_id = hex::encode(row.delivery.message.id.as_slice());
            self.delivery_spill.in_flight = Some((row.seq, event_id));
            row.delivery
        }))
    }

    pub(super) fn note_spill_ready(&mut self) {
        self.delivery_spill.maybe_rows = true;
    }

    /// Settle the row handed to ingest or dedup. It is removed once its event
    /// is in the seen index; otherwise it stays for a later retry. A failed
    /// removal leaves a duplicate that the next read deduplicates.
    pub(super) fn settle_spilled_delivery(&mut self) {
        let Some((seq, event_id)) = self.delivery_spill.in_flight.take() else {
            return;
        };
        let admitted = self
            .transport_receipts()
            .is_ok_and(|receipts| receipts.contains(&event_id));
        let settled = self
            .app
            .account_storage(&self.state.label)
            .and_then(|storage| {
                if admitted {
                    storage.remove_spilled_account_delivery(seq)?;
                    return Ok(());
                }
                let now = unix_now_seconds();
                match storage.defer_spilled_account_delivery(seq, now, SPILL_MAX_ATTEMPTS)? {
                    SpilledDeliveryRetry::Deferred { not_before } => {
                        let next = self.delivery_spill.next_retry_at.get_or_insert(not_before);
                        *next = (*next).min(not_before);
                    }
                    SpilledDeliveryRetry::Abandoned => tracing::warn!(
                        target: "marmot_app::client::delivery_spill",
                        method = "settle_spilled_delivery",
                        error_kind = "spill_row_abandoned",
                        "spilled delivery was never admitted; removed after its last retry",
                    ),
                }
                Ok(())
            });
        if settled.is_err() {
            tracing::warn!(
                target: "marmot_app::client::delivery_spill",
                method = "settle_spilled_delivery",
                error_kind = "spill_row_settlement_failed",
                "spilled delivery row kept after ingest; it will be read again",
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use cgka_traits::transport::{Timestamp, TransportEnvelope, TransportMessage, TransportSource};
    use cgka_traits::{
        MemberId, MessageId, TransportDelivery, TransportDeliveryPlane, TransportDeliverySource,
    };
    use marmot_account::AccountHome;

    use crate::relay_plane::{ACCOUNT_DELIVERY_SPILL_LIMITS, MarmotRelayPlane};
    use crate::tests::{ScriptedPushRelayClient, client_on_app_relay_plane};
    use crate::{MarmotApp, unix_now_seconds};

    fn delivery(id: u8) -> TransportDelivery {
        TransportDelivery {
            account_id: MemberId::new(vec![0xAA; 32]),
            group_id_hint: None,
            message: TransportMessage {
                id: MessageId::new(vec![id; 32]),
                payload: vec![id; 16],
                timestamp: Timestamp(1),
                causal_deps: Vec::new(),
                source: TransportSource("nostr".to_owned()),
                envelope: TransportEnvelope::GroupMessage {
                    transport_group_id: vec![0x42; 32],
                },
            },
            received_at: Timestamp(1),
            source: TransportDeliverySource {
                transport: TransportSource("nostr".to_owned()),
                plane: TransportDeliveryPlane::Group,
                endpoint: None,
                subscription_id: None,
                wire: None,
            },
        }
    }

    #[tokio::test]
    async fn spilled_row_stays_until_its_event_is_seen() {
        let dir = tempfile::tempdir().unwrap();
        AccountHome::open(dir.path())
            .create_account("alice")
            .unwrap();
        let relay = Arc::new(ScriptedPushRelayClient::default());
        let mut app = MarmotApp::with_relay(dir.path(), "wss://relay.example")
            .with_test_relay_client(relay.clone());
        app.relay_plane =
            MarmotRelayPlane::new_with_loopback(Some(Duration::from_secs(120)), relay, true);
        let mut client = client_on_app_relay_plane(&app, "alice").await;
        let storage = app.account_storage("alice").unwrap();
        let now = unix_now_seconds();
        storage
            .spill_account_deliveries(&[delivery(7)], ACCOUNT_DELIVERY_SPILL_LIMITS, now)
            .unwrap();

        // Ingest failed or kept no trace: the event never reached the seen
        // index, so the row stays and is deferred rather than deleted.
        client.delivery_spill.maybe_rows = true;
        assert_eq!(client.next_spilled_delivery().unwrap(), Some(delivery(7)));
        client.settle_spilled_delivery();
        assert!(
            storage
                .spilled_account_deliveries(10, now)
                .unwrap()
                .deliveries
                .is_empty(),
            "a deferred row is not due yet"
        );
        let kept = storage
            .spilled_account_deliveries(10, now + 24 * 60 * 60)
            .unwrap()
            .deliveries;
        assert_eq!(kept.len(), 1, "an unadmitted row is kept");

        // Once the event is seen, settling removes the row.
        client.delivery_spill.in_flight = Some((kept[0].seq, hex::encode([7_u8; 32])));
        client.remember_seen_event(hex::encode([7_u8; 32]));
        client.settle_spilled_delivery();
        assert!(
            storage
                .spilled_account_deliveries(10, now + 24 * 60 * 60)
                .unwrap()
                .deliveries
                .is_empty()
        );
    }
}
