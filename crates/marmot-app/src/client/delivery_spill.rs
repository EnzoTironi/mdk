//! Account-worker side of the durable delivery spill.
//!
//! Spilled rows are read back in small batches and alternated with live
//! deliveries, so neither backlog starves the other. Each row is removed only
//! after ingest has seen its delivery. A newer live delivery can therefore be
//! admitted first and move the cursor past a spilled one without losing it.

use std::collections::VecDeque;

use cgka_traits::TransportDelivery;
use storage_sqlite::SpilledDelivery;

use super::AppClient;
use crate::AppError;
use crate::relay_plane::AccountDeliveryReceive;

const SPILL_READ_BATCH: usize = 32;

pub(crate) struct DeliverySpillReader {
    buffered: VecDeque<SpilledDelivery>,
    /// Durable rows may exist beyond `buffered`. Starts true, so rows left by
    /// an earlier process are admitted.
    maybe_rows: bool,
    live_turn: bool,
    /// Row whose delivery is currently with ingest.
    in_flight: Option<i64>,
}

impl Default for DeliverySpillReader {
    fn default() -> Self {
        Self {
            buffered: VecDeque::new(),
            maybe_rows: true,
            live_turn: false,
            in_flight: None,
        }
    }
}

impl DeliverySpillReader {
    pub(super) fn pending(&self) -> bool {
        !self.buffered.is_empty() || self.maybe_rows
    }
}

impl AppClient {
    /// A delivery that is ready without waiting: while spilled rows remain,
    /// alternate a queued live delivery with the oldest spilled one.
    pub(super) fn take_ready_delivery(
        &mut self,
    ) -> Result<Option<AccountDeliveryReceive>, AppError> {
        let spill = &mut self.delivery_spill;
        if !spill.pending() {
            return Ok(None);
        }
        spill.live_turn = !spill.live_turn;
        if spill.live_turn
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
            let rows = self
                .app
                .account_storage(&self.state.label)?
                .spilled_account_deliveries(SPILL_READ_BATCH)?;
            self.delivery_spill.maybe_rows = rows.len() == SPILL_READ_BATCH;
            self.delivery_spill.buffered.extend(rows);
        }
        Ok(self.delivery_spill.buffered.pop_front().map(|row| {
            self.delivery_spill.in_flight = Some(row.seq);
            row.delivery
        }))
    }

    pub(super) fn note_spill_ready(&mut self) {
        self.delivery_spill.maybe_rows = true;
    }

    /// Ingest (or dedup) has seen the spilled delivery; drop its row. A failed
    /// removal leaves a duplicate that the next read dedups again.
    pub(super) fn release_spilled_delivery(&mut self) {
        let Some(seq) = self.delivery_spill.in_flight.take() else {
            return;
        };
        let removed = self
            .app
            .account_storage(&self.state.label)
            .and_then(|storage| Ok(storage.remove_spilled_account_delivery(seq)?));
        if removed.is_err() {
            tracing::warn!(
                target: "marmot_app::client::delivery_spill",
                method = "release_spilled_delivery",
                error_kind = "spill_row_retained",
                "spilled delivery row retained after ingest; it will be deduplicated on the next read",
            );
        }
    }
}
