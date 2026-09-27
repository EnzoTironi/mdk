//! Durable overflow tail for a full account delivery queue.
//!
//! The shared router must never wait on one account's slow consumer. When an
//! account queue is full, the router hands the delivery to that account's
//! spill instead of dropping it. One writer task per account stores hand-offs
//! in the account database; the account worker later admits spilled rows
//! through its ordinary ingest path. A delivery is lost, and becomes a
//! queue-loss generation, only when the hand-off or the durable spill is full.

use std::collections::VecDeque;
use std::sync::Arc;

use cgka_traits::TransportDelivery;
use storage_sqlite::{DeliverySpillDisposition, DeliverySpillLimits};
use tokio::sync::Notify;

use super::{AccountDeliveryRecoveryMarkerError, AccountDeliveryRoute, omit_account_delivery};

pub(crate) type AccountDeliverySpillStore = Arc<
    dyn Fn(
            Vec<TransportDelivery>,
        ) -> Result<Vec<DeliverySpillDisposition>, AccountDeliveryRecoveryMarkerError>
        + Send
        + Sync
        + 'static,
>;

/// Durable spill bounds for one account database.
pub(crate) const ACCOUNT_DELIVERY_SPILL_LIMITS: DeliverySpillLimits = DeliverySpillLimits {
    max_rows: 8_192,
    max_bytes: 16 * 1024 * 1024,
};
/// Payload bytes the router may hand off before the writer catches up.
const SPILL_HANDOFF_MAX_BYTES: usize = 4 * 1024 * 1024;
const SPILL_WRITE_BATCH: usize = 128;

pub(super) struct AccountDeliverySpill {
    store: AccountDeliverySpillStore,
    handoff: std::sync::Mutex<Handoff>,
    /// Wakes the account worker after rows become durable.
    ready: Arc<Notify>,
}

#[derive(Default)]
struct Handoff {
    items: VecDeque<TransportDelivery>,
    bytes: usize,
    writing: bool,
}

impl AccountDeliverySpill {
    pub(super) fn new(store: AccountDeliverySpillStore, ready: Arc<Notify>) -> Arc<Self> {
        Arc::new(Self {
            store,
            handoff: std::sync::Mutex::new(Handoff::default()),
            ready,
        })
    }

    /// Accept a delivery the full queue cannot take, without blocking the
    /// router. Returns false when the hand-off itself is full. Until the
    /// writer settles it, an accepted delivery fences the account's transport
    /// cursor.
    pub(super) fn offer(
        self: &Arc<Self>,
        delivery: TransportDelivery,
        route: &AccountDeliveryRoute,
    ) -> bool {
        let size = delivery.message.payload.len();
        let mut handoff = self.handoff.lock().unwrap_or_else(|p| p.into_inner());
        if handoff.bytes.saturating_add(size) > SPILL_HANDOFF_MAX_BYTES {
            return false;
        }
        route.overflow.begin_spill();
        handoff.bytes += size;
        handoff.items.push_back(delivery);
        if !handoff.writing {
            handoff.writing = true;
            tokio::spawn(self.clone().write(route.clone()));
        }
        true
    }

    async fn write(self: Arc<Self>, route: AccountDeliveryRoute) {
        loop {
            let batch: Vec<_> = {
                let mut handoff = self.handoff.lock().unwrap_or_else(|p| p.into_inner());
                if handoff.items.is_empty() {
                    handoff.writing = false;
                    return;
                }
                let take = handoff.items.len().min(SPILL_WRITE_BATCH);
                let batch: Vec<_> = handoff.items.drain(..take).collect();
                let bytes: usize = batch.iter().map(|d| d.message.payload.len()).sum();
                handoff.bytes = handoff.bytes.saturating_sub(bytes);
                batch
            };
            let count = batch.len() as u64;
            let store = self.store.clone();
            let dispositions = tokio::task::spawn_blocking(move || store(batch))
                .await
                .ok()
                .and_then(Result::ok)
                .unwrap_or_default();
            let stored = count_of(&dispositions, DeliverySpillDisposition::Stored);
            let seen = count_of(&dispositions, DeliverySpillDisposition::AlreadySeen);
            // Record loss before releasing the spill fence, so the cursor
            // stays fenced throughout.
            for _ in stored + seen..count {
                omit_account_delivery(&route);
            }
            route.overflow.finish_spill(count, stored, seen);
            if stored > 0 {
                self.ready.notify_one();
            }
        }
    }
}

fn count_of(dispositions: &[DeliverySpillDisposition], wanted: DeliverySpillDisposition) -> u64 {
    dispositions.iter().filter(|d| **d == wanted).count() as u64
}
