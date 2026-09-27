//! One bounded account queue shared by live routing, recovery admission, and
//! the reserved overflow control writer. The account adapter is its only
//! consumer; its owner still performs every engine ingest serially.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use cgka_traits::transport::TransportEnvelope;
use cgka_traits::{GroupId, TransportDelivery, TransportDeliveryPlane, TransportGroupSubscription};
use tokio::sync::Notify;

use super::{
    ACCOUNT_DELIVERY_BUFFER, AccountDeliveryEvent, AccountDeliveryOverflow,
    AccountDeliveryOverflowInner, AccountDeliveryOverflowState,
};

const QUEUE_CAPACITY: usize = ACCOUNT_DELIVERY_BUFFER + 1;

pub(super) struct AccountDeliverySender {
    inner: Arc<QueueInner>,
}

pub(super) struct AccountDeliveryReceiver {
    inner: Arc<QueueInner>,
}

struct QueueInner {
    state: Mutex<QueueState>,
    available: Notify,
    space: Notify,
}

struct QueueState {
    items: VecDeque<QueueItem>,
    senders: usize,
    receiver_alive: bool,
    routes: Vec<TransportGroupSubscription>,
    route_generation: u64,
    routes_retired: bool,
    alternate_turn: bool,
}

enum QueueItem {
    Delivery {
        delivery: Box<TransportDelivery>,
        group: Option<(GroupId, u64)>,
    },
    /// The position is reserved atomically with the first omission. The
    /// The writer makes this consumable after persistence or closure. Crossing
    /// also requires the *current* durable marker, checked at pop time.
    Overflow {
        generation: u64,
        marker_token: u64,
        group: Option<(GroupId, u64)>,
        ready: bool,
    },
}

#[derive(Debug)]
pub(super) enum TrySendError<T> {
    Full(T),
    Closed(T),
    MissingReservation(T),
}

pub(super) enum RouteAdmission {
    Accepted,
    Omitted {
        queue_depth: usize,
        signal_generation: Option<u64>,
    },
    Closed,
}

impl AccountDeliverySender {
    pub(super) fn channel() -> (Self, AccountDeliveryReceiver) {
        let inner = Arc::new(QueueInner {
            state: Mutex::new(QueueState {
                items: VecDeque::with_capacity(QUEUE_CAPACITY),
                senders: 1,
                receiver_alive: true,
                routes: Vec::new(),
                route_generation: 0,
                routes_retired: false,
                alternate_turn: false,
            }),
            available: Notify::new(),
            space: Notify::new(),
        });
        (
            Self {
                inner: inner.clone(),
            },
            AccountDeliveryReceiver { inner },
        )
    }

    pub(super) fn closed() -> Self {
        let (sender, receiver) = Self::channel();
        drop(receiver);
        sender
    }

    pub(super) fn max_capacity(&self) -> usize {
        QUEUE_CAPACITY
    }

    /// The adapter keeps its receiver alive, so this allocation address is a
    /// stable identity for its lifetime without retaining another sender.
    pub(super) fn identity(&self) -> usize {
        Arc::as_ptr(&self.inner) as usize
    }

    pub(super) fn capacity(&self) -> usize {
        QUEUE_CAPACITY - self.inner.state.lock().unwrap().items.len()
    }

    /// Read the live control reservation under queue -> overflow lock order.
    /// A notification signal alone does not establish a queue position.
    pub(super) fn queued_overflow_marker_token(
        &self,
        overflow: &AccountDeliveryOverflowState,
    ) -> Option<u64> {
        let queue = self.inner.state.lock().unwrap();
        if !queue.receiver_alive || queue.routes_retired {
            return None;
        }
        let loss = overflow
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        (loss.pending
            && loss.signal_queued
            && loss.dropped > 0
            && queue.items.iter().any(|item| {
                matches!(item, QueueItem::Overflow { generation, .. } if *generation == loss.generation)
            }))
        .then_some(loss.marker_token)
    }

    /// Inspect the same live queue identity after a control was consumed.
    /// There is no longer a reservation to find, so match its generation and
    /// marker against the still-pending process-local loss authority.
    pub(super) fn consumed_control_still_current(
        &self,
        overflow: &AccountDeliveryOverflowState,
        generation: u64,
        marker_token: u64,
    ) -> bool {
        let queue = self.inner.state.lock().unwrap();
        if !queue.receiver_alive || queue.routes_retired {
            return false;
        }
        let loss = overflow
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loss.pending && loss.generation == generation && loss.marker_token == marker_token
    }

    /// Keep the historical consume operation for old receivers, but only a
    /// receiver belonging to the current nonretired queue may yield a hint.
    pub(super) fn consume_control(
        &self,
        overflow: &AccountDeliveryOverflowState,
        generation: u64,
    ) -> AccountDeliveryOverflow {
        let queue = self.inner.state.lock().unwrap();
        let current = queue.receiver_alive && !queue.routes_retired;
        let mut observed = overflow.consume_signal(generation);
        observed.consumed_current_control &= current;
        observed
    }

    /// A route mutation invalidates all previously admitted scheduling keys.
    /// The events remain in the FIFO and are still handed to the engine.
    pub(super) fn invalidate_routes(&self) {
        let mut state = self.inner.state.lock().unwrap();
        if state.routes_retired {
            return;
        }
        state.route_generation = state.route_generation.wrapping_add(1);
        state.routes.clear();
    }

    /// The old receiver may drain its accepted events, but completion of an
    /// operation against its former route must never revalidate their keys.
    pub(super) fn retire_routes(&self) {
        let mut state = self.inner.state.lock().unwrap();
        state.route_generation = state.route_generation.wrapping_add(1);
        state.routes.clear();
        state.routes_retired = true;
    }

    /// An unchanged sanitized map keeps already-admitted local keys valid.
    /// Changed maps invalidate before the adapter awaits the external sync.
    pub(super) fn invalidate_routes_if_changed(&self, next: &[TransportGroupSubscription]) {
        let mut state = self.inner.state.lock().unwrap();
        if !state.routes_retired && state.routes != next {
            state.route_generation = state.route_generation.wrapping_add(1);
            state.routes.clear();
        }
    }

    /// Forwarder recovery retires the old queue's control position while
    /// preserving accepted deliveries on both sides of it. Serialize with
    /// marker activation under the queue lock so a late writer cannot put a
    /// control record back into the retired queue.
    pub(super) fn cancel_overflow_signal(
        &self,
        overflow: &AccountDeliveryOverflowState,
        generation: u64,
    ) {
        let mut state = self.inner.state.lock().unwrap();
        overflow.cancel_signal(generation);
        if let Some(index) = state.items.iter().position(|item| {
            matches!(item, QueueItem::Overflow { generation: queued, .. } if *queued == generation)
        }) {
            state.items.remove(index);
            self.inner.available.notify_waiters();
            self.inner.space.notify_waiters();
        }
    }

    pub(super) fn install_routes(&self, routes: Vec<TransportGroupSubscription>) {
        let mut state = self.inner.state.lock().unwrap();
        if !state.routes_retired && state.routes != routes {
            state.route_generation = state.route_generation.wrapping_add(1);
            state.routes = routes;
        }
    }

    /// Hold queue -> overflow locks in this order. No overflow method holds
    /// its lock while acquiring this queue: the marker writer releases the
    /// overflow lock before activating the reserved control position.
    pub(super) fn route_delivery(
        &self,
        delivery: TransportDelivery,
        overflow: &AccountDeliveryOverflowState,
    ) -> RouteAdmission {
        let mut state = self.inner.state.lock().unwrap();
        if !state.receiver_alive {
            return RouteAdmission::Closed;
        }
        let queue_depth = state.items.len();
        overflow.observe_queue_depth(queue_depth);
        if queue_depth >= ACCOUNT_DELIVERY_BUFFER {
            let omitted_group = state.scheduling_group(&delivery);
            let signal_generation = overflow.record_drop(queue_depth);
            let loss = overflow
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let current = loss.pending && loss.signal_queued && loss.dropped > 0;
            if let Some(generation) = signal_generation {
                assert!(state.items.len() < QUEUE_CAPACITY);
                state.items.push_back(QueueItem::Overflow {
                    generation,
                    marker_token: loss.marker_token,
                    group: (current && loss.generation == generation)
                        .then_some(omitted_group)
                        .flatten(),
                    ready: false,
                });
                self.inner.available.notify_one();
            } else if let Some(QueueItem::Overflow { group, .. }) =
                state.items.iter_mut().rev().find(|item| {
                    matches!(item, QueueItem::Overflow {
                        generation,
                        marker_token,
                        ..
                    } if current && *generation == loss.generation && *marker_token == loss.marker_token)
                })
                && *group != omitted_group
            {
                // An unknown or mixed omission stays unknown for this control.
                *group = None;
            }
            return RouteAdmission::Omitted {
                queue_depth,
                signal_generation,
            };
        }
        let group = state.scheduling_group(&delivery);
        state.items.push_back(QueueItem::Delivery {
            delivery: Box::new(delivery),
            group,
        });
        overflow.observe_queue_depth(state.items.len());
        self.inner.available.notify_one();
        RouteAdmission::Accepted
    }

    pub(super) fn try_send(
        &self,
        event: AccountDeliveryEvent,
    ) -> Result<(), TrySendError<AccountDeliveryEvent>> {
        let mut state = self.inner.state.lock().unwrap();
        if !state.receiver_alive {
            return Err(TrySendError::Closed(event));
        }
        match event {
            AccountDeliveryEvent::Overflow { generation } => {
                if let Some(QueueItem::Overflow { ready, .. }) = state.items.iter_mut().find(
                    |item| matches!(item, QueueItem::Overflow { generation: queued, .. } if *queued == generation),
                ) {
                    *ready = true;
                    self.inner.available.notify_one();
                    return Ok(());
                }
                return Err(TrySendError::MissingReservation(
                    AccountDeliveryEvent::Overflow { generation },
                ));
            }
            AccountDeliveryEvent::Delivery(delivery) => {
                if state.items.len() >= ACCOUNT_DELIVERY_BUFFER {
                    return Err(TrySendError::Full(AccountDeliveryEvent::Delivery(delivery)));
                }
                let group = state.scheduling_group(&delivery);
                state
                    .items
                    .push_back(QueueItem::Delivery { delivery, group });
            }
        }
        self.inner.available.notify_one();
        Ok(())
    }

    #[cfg(all(test, feature = "test-policy-overrides"))]
    pub(super) async fn send(
        &self,
        mut event: AccountDeliveryEvent,
    ) -> Result<(), TrySendError<AccountDeliveryEvent>> {
        loop {
            let notified = self.inner.space.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.try_send(event) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Closed(event)) => return Err(TrySendError::Closed(event)),
                Err(TrySendError::MissingReservation(event)) => {
                    return Err(TrySendError::MissingReservation(event));
                }
                Err(TrySendError::Full(returned)) => event = returned,
            }
            notified.await;
        }
    }
}

impl Clone for AccountDeliverySender {
    fn clone(&self) -> Self {
        self.inner.state.lock().unwrap().senders += 1;
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl Drop for AccountDeliverySender {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.senders -= 1;
        if state.senders == 0 {
            self.inner.available.notify_waiters();
        }
    }
}

impl AccountDeliveryReceiver {
    #[cfg(test)]
    pub(super) fn try_recv(
        &mut self,
        overflow: &AccountDeliveryOverflowState,
    ) -> Option<AccountDeliveryEvent> {
        let event = self.inner.state.lock().unwrap().pop(overflow);
        if event.is_some() {
            self.inner.space.notify_waiters();
        }
        event
    }

    pub(super) async fn recv(
        &mut self,
        overflow: &AccountDeliveryOverflowState,
    ) -> Option<AccountDeliveryEvent> {
        loop {
            let notified = self.inner.available.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.inner.state.lock().unwrap();
                if let Some(event) = state.pop(overflow) {
                    self.inner.space.notify_waiters();
                    return Some(event);
                }
                if state.senders == 0 {
                    return None;
                }
            }
            notified.await;
        }
    }
}

impl Drop for AccountDeliveryReceiver {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap();
        state.receiver_alive = false;
        state.items.clear();
        drop(state);
        self.inner.space.notify_waiters();
    }
}

impl QueueState {
    fn scheduling_group(&self, delivery: &TransportDelivery) -> Option<(GroupId, u64)> {
        if delivery.source.plane != TransportDeliveryPlane::Group {
            return None;
        }
        let TransportEnvelope::GroupMessage { transport_group_id } = &delivery.message.envelope
        else {
            return None;
        };
        let hinted = delivery.group_id_hint.as_ref()?;
        let endpoint = delivery.source.endpoint.as_ref()?;
        let mut matching = self.routes.iter().filter(|route| {
            route.transport_group_id == *transport_group_id && route.endpoints.contains(endpoint)
        });
        let first = matching.next()?;
        if matching.any(|route| route.group_id != first.group_id) || &first.group_id != hinted {
            return None;
        }
        Some((hinted.clone(), self.route_generation))
    }

    fn alternate_index(&self, loss: &AccountDeliveryOverflowInner) -> Option<usize> {
        if !self.alternate_turn || self.routes_retired {
            return None;
        }
        let head_group = match self.items.front()? {
            QueueItem::Delivery {
                group: Some((group, generation)),
                ..
            } if *generation == self.route_generation => group,
            control @ QueueItem::Overflow { .. } => self.crossable_control(control, loss)?,
            _ => return None,
        };
        let mut crossed = Vec::new();
        if matches!(self.items.front(), Some(QueueItem::Overflow { .. })) {
            crossed.push(head_group);
        }
        for (index, item) in self.items.iter().enumerate().skip(1) {
            match item {
                QueueItem::Delivery {
                    group: Some((group, generation)),
                    ..
                } if *generation == self.route_generation => {
                    if group != head_group && crossed.iter().all(|omitted| *omitted != group) {
                        return Some(index);
                    }
                }
                control @ QueueItem::Overflow { .. } => {
                    crossed.push(self.crossable_control(control, loss)?);
                }
                _ => return None,
            }
        }
        None
    }

    fn crossable_control<'a>(
        &self,
        control: &'a QueueItem,
        loss: &AccountDeliveryOverflowInner,
    ) -> Option<&'a GroupId> {
        let QueueItem::Overflow {
            generation,
            marker_token,
            group: Some((group, route_generation)),
            ready: true,
        } = control
        else {
            return None;
        };
        (self.receiver_alive
            && !self.routes_retired
            && *route_generation == self.route_generation
            && loss.pending
            && loss.signal_queued
            && loss.generation == *generation
            && loss.marker_token == *marker_token
            && loss.dropped > 0
            && loss.notification_losses == 0
            && loss.marker_durable
            && !loss.marker_closed
            && !loss.marker_in_progress)
            .then_some(group)
    }

    fn pop(&mut self, overflow: &AccountDeliveryOverflowState) -> Option<AccountDeliveryEvent> {
        let head = self.items.front()?;
        if matches!(head, QueueItem::Overflow { ready: false, .. }) {
            return None;
        }
        let current_group = matches!(
            head,
            QueueItem::Delivery {
                group: Some((_, generation)),
                ..
            } if *generation == self.route_generation
        );
        // Hold the overflow snapshot through selection and removal. A later
        // omission clears marker_durable under this lock; it cannot race a
        // cross-control pop after we have checked the marker.
        let loss = self.alternate_turn.then(|| {
            overflow
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
        });
        let alternate = loss
            .as_ref()
            .and_then(|current| self.alternate_index(current));
        let item = self.items.remove(alternate.unwrap_or(0))?;
        drop(loss);
        self.alternate_turn = alternate.is_none() && current_group;
        match item {
            QueueItem::Delivery { delivery, .. } => Some(AccountDeliveryEvent::Delivery(delivery)),
            QueueItem::Overflow {
                generation,
                ready: true,
                ..
            } => {
                self.alternate_turn = false;
                Some(AccountDeliveryEvent::Overflow { generation })
            }
            QueueItem::Overflow { ready: false, .. } => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cgka_traits::transport::{Timestamp, TransportEnvelope, TransportMessage, TransportSource};
    use cgka_traits::{
        MemberId, MessageId, TransportDeliveryPlane, TransportDeliverySource, TransportEndpoint,
    };
    use tokio::time::timeout;

    use super::super::{
        AccountDeliveryRecoveryMarker, AccountDeliveryRecoveryMarkerError, persist_queue_loss,
    };
    use super::*;

    fn account() -> MemberId {
        MemberId::new(vec![0xA1; 32])
    }

    fn endpoint() -> TransportEndpoint {
        TransportEndpoint("wss://relay.example".into())
    }

    fn route(group: u8, transport_route: u8) -> TransportGroupSubscription {
        TransportGroupSubscription {
            group_id: GroupId::new(vec![group; 16]),
            transport_group_id: vec![transport_route; 32],
            endpoints: vec![endpoint()],
        }
    }

    fn delivery(group: u8, transport_route: u8, id: u8) -> TransportDelivery {
        TransportDelivery {
            account_id: account(),
            group_id_hint: Some(GroupId::new(vec![group; 16])),
            message: TransportMessage {
                id: MessageId::new(vec![id; 32]),
                payload: vec![id],
                timestamp: Timestamp(1),
                causal_deps: Vec::new(),
                source: TransportSource("nostr".into()),
                envelope: TransportEnvelope::GroupMessage {
                    transport_group_id: vec![transport_route; 32],
                },
            },
            received_at: Timestamp(1),
            source: TransportDeliverySource {
                transport: TransportSource("nostr".into()),
                plane: TransportDeliveryPlane::Group,
                endpoint: Some(endpoint()),
                subscription_id: None,
                wire: None,
            },
        }
    }

    fn pop_id(
        receiver: &mut AccountDeliveryReceiver,
        overflow: &AccountDeliveryOverflowState,
    ) -> u8 {
        match receiver.try_recv(overflow).expect("queued delivery") {
            AccountDeliveryEvent::Delivery(delivery) => delivery.message.payload[0],
            AccountDeliveryEvent::Overflow { .. } => panic!("unexpected control"),
        }
    }

    fn loss_queue() -> (
        AccountDeliverySender,
        AccountDeliveryReceiver,
        Arc<AccountDeliveryOverflowState>,
        u64,
    ) {
        let (sender, receiver) = AccountDeliverySender::channel();
        sender.install_routes(vec![route(1, 11), route(2, 22), route(3, 33)]);
        let overflow = Arc::new(AccountDeliveryOverflowState::default());
        for _ in 0..ACCOUNT_DELIVERY_BUFFER {
            assert!(matches!(
                sender.route_delivery(delivery(1, 11, 1), &overflow),
                RouteAdmission::Accepted
            ));
        }
        let RouteAdmission::Omitted {
            signal_generation: Some(generation),
            ..
        } = sender.route_delivery(delivery(1, 11, 2), &overflow)
        else {
            panic!("first group A omission reserves its control");
        };
        (sender, receiver, overflow, generation)
    }

    async fn wait_control_ready(sender: &AccountDeliverySender, generation: u64) {
        timeout(Duration::from_secs(2), async {
            loop {
                let ready = sender.inner.state.lock().unwrap().items.iter().any(|item| {
                    matches!(item, QueueItem::Overflow {
                        generation: queued,
                        ready: true,
                        ..
                    } if *queued == generation)
                });
                if ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("marker writer must activate the reserved control");
    }

    #[tokio::test]
    async fn real_forwarder_crosses_only_a_current_durable_marker() {
        for closed in [false, true] {
            let (sender, mut receiver, overflow, generation) = loss_queue();
            let marker: AccountDeliveryRecoveryMarker = Arc::new(move |_, _| {
                if closed {
                    Err(AccountDeliveryRecoveryMarkerError::Closed)
                } else {
                    Ok(())
                }
            });
            persist_queue_loss(&sender, &overflow, marker, Some(generation));
            wait_control_ready(&sender, generation).await;
            {
                let loss = overflow.inner.lock().unwrap();
                assert_eq!(loss.marker_durable, !closed);
                assert_eq!(loss.marker_closed, closed);
            }
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
            assert!(matches!(
                sender.route_delivery(delivery(2, 22, 3), &overflow),
                RouteAdmission::Accepted
            ));
            if closed {
                for _ in 2..ACCOUNT_DELIVERY_BUFFER {
                    assert_eq!(pop_id(&mut receiver, &overflow), 1);
                }
                assert!(matches!(
                    receiver.try_recv(&overflow),
                    Some(AccountDeliveryEvent::Overflow { generation: observed })
                        if observed == generation
                ));
                assert_eq!(pop_id(&mut receiver, &overflow), 3);
            } else {
                assert_eq!(
                    pop_id(&mut receiver, &overflow),
                    3,
                    "an independent live group gets the existing alternate turn"
                );
                assert_eq!(pop_id(&mut receiver, &overflow), 1);
            }
        }
    }

    #[tokio::test]
    async fn later_drop_stops_crossing_while_the_ready_marker_is_rewritten() {
        let (sender, mut receiver, overflow, generation) = loss_queue();
        let marker: AccountDeliveryRecoveryMarker = Arc::new(|_, _| Ok(()));
        persist_queue_loss(&sender, &overflow, marker, Some(generation));
        wait_control_ready(&sender, generation).await;
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
        assert!(matches!(
            sender.route_delivery(delivery(2, 22, 3), &overflow),
            RouteAdmission::Accepted
        ));
        assert!(matches!(
            sender.route_delivery(delivery(1, 11, 4), &overflow),
            RouteAdmission::Omitted {
                signal_generation: None,
                ..
            }
        ));
        assert!(!overflow.inner.lock().unwrap().marker_durable);
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let entered = Mutex::new(Some(entered_tx));
        let release = Mutex::new(Some(release_rx));
        let marker: AccountDeliveryRecoveryMarker = Arc::new(move |_, count| {
            assert_eq!(count, 2);
            entered.lock().unwrap().take().unwrap().send(()).unwrap();
            release
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .blocking_recv()
                .unwrap();
            Ok(())
        });
        persist_queue_loss(&sender, &overflow, marker, None);
        timeout(Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            pop_id(&mut receiver, &overflow),
            1,
            "ready from the earlier count cannot authorize crossing"
        );
        release_tx.send(()).unwrap();
        timeout(Duration::from_secs(2), async {
            while !overflow.inner.lock().unwrap().marker_durable {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(pop_id(&mut receiver, &overflow), 3);
    }

    #[test]
    fn ready_control_with_later_mixed_or_unknown_omission_stays_a_barrier() {
        for omitted in [
            delivery(2, 22, 4),
            {
                let mut unknown = delivery(1, 11, 4);
                unknown.group_id_hint = None;
                unknown
            },
            {
                let mut inbox = delivery(1, 11, 4);
                inbox.source.plane = TransportDeliveryPlane::AccountInbox;
                inbox
            },
        ] {
            let (sender, mut receiver, overflow, generation) = loss_queue();
            sender
                .try_send(AccountDeliveryEvent::Overflow { generation })
                .unwrap();
            overflow.inner.lock().unwrap().marker_durable = true;
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
            assert!(matches!(
                sender.route_delivery(delivery(3, 33, 3), &overflow),
                RouteAdmission::Accepted
            ));
            assert!(matches!(
                sender.route_delivery(omitted, &overflow),
                RouteAdmission::Omitted { .. }
            ));
            assert!(matches!(
                sender.route_delivery(delivery(1, 11, 5), &overflow),
                RouteAdmission::Omitted { .. }
            ));
            // Model the writer finishing the increased count. Readiness did
            // not encode the new omitted group, and Unknown is irreversible.
            overflow.inner.lock().unwrap().marker_durable = true;
            assert!(matches!(
                sender
                    .inner
                    .state
                    .lock()
                    .unwrap()
                    .items
                    .iter()
                    .find(|item| { matches!(item, QueueItem::Overflow { .. }) }),
                Some(QueueItem::Overflow { group: None, .. })
            ));
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
        }
    }

    #[test]
    fn crossed_groups_keep_fifo_and_the_existing_one_for_one_turns() {
        let (sender, mut receiver, overflow, generation) = loss_queue();
        sender
            .try_send(AccountDeliveryEvent::Overflow { generation })
            .unwrap();
        overflow.inner.lock().unwrap().marker_durable = true;
        for _ in 0..4 {
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
        }
        for (group, route, id) in [(2, 22, 3), (2, 22, 4), (3, 33, 5)] {
            assert!(matches!(
                sender.route_delivery(delivery(group, route, id), &overflow),
                RouteAdmission::Accepted
            ));
        }
        assert_eq!(
            (0..5)
                .map(|_| pop_id(&mut receiver, &overflow))
                .collect::<Vec<_>>(),
            [3, 1, 4, 1, 5]
        );
        assert!(sender.queued_overflow_marker_token(&overflow).is_some());
    }

    #[test]
    fn every_crossed_control_must_be_disjoint_and_current() {
        let (sender, mut receiver, overflow, generation) = loss_queue();
        sender
            .try_send(AccountDeliveryEvent::Overflow { generation })
            .unwrap();
        let token = {
            let mut loss = overflow.inner.lock().unwrap();
            loss.marker_durable = true;
            loss.marker_token
        };
        for _ in 0..4 {
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
        }
        // A second in-queue control cannot occur with the current single
        // signal, but the scanner must reject a candidate matching ANY
        // crossed control if that representation ever occurs.
        sender
            .inner
            .state
            .lock()
            .unwrap()
            .items
            .push_back(QueueItem::Overflow {
                generation,
                marker_token: token,
                group: Some((GroupId::new(vec![2; 16]), 1)),
                ready: true,
            });
        assert!(matches!(
            sender.route_delivery(delivery(2, 22, 3), &overflow),
            RouteAdmission::Accepted
        ));
        assert!(matches!(
            sender.route_delivery(delivery(3, 33, 4), &overflow),
            RouteAdmission::Accepted
        ));
        assert_eq!(pop_id(&mut receiver, &overflow), 4);
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
        // A stale writer's identity cannot authorize another crossing.
        if let Some(QueueItem::Overflow { marker_token, .. }) = sender
            .inner
            .state
            .lock()
            .unwrap()
            .items
            .iter_mut()
            .find(|item| matches!(item, QueueItem::Overflow { .. }))
        {
            *marker_token ^= 1;
        }
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
    }

    #[test]
    fn changed_or_retired_routes_close_the_crossing_path() {
        for retire in [false, true] {
            let (sender, mut receiver, overflow, generation) = loss_queue();
            sender
                .try_send(AccountDeliveryEvent::Overflow { generation })
                .unwrap();
            overflow.inner.lock().unwrap().marker_durable = true;
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
            assert!(matches!(
                sender.route_delivery(delivery(2, 22, 3), &overflow),
                RouteAdmission::Accepted
            ));
            if retire {
                sender.retire_routes();
            } else {
                sender.invalidate_routes();
                sender.install_routes(vec![route(1, 11), route(2, 22), route(3, 33)]);
            }
            for _ in 2..ACCOUNT_DELIVERY_BUFFER {
                assert_eq!(pop_id(&mut receiver, &overflow), 1);
            }
            assert!(matches!(
                receiver.try_recv(&overflow),
                Some(AccountDeliveryEvent::Overflow { generation: observed })
                    if observed == generation
            ));
            assert_eq!(pop_id(&mut receiver, &overflow), 3);
        }
    }

    #[test]
    fn exact_queue_reservation_excludes_notification_cancel_and_retirement() {
        let (sender, mut receiver) = AccountDeliverySender::channel();
        let overflow = AccountDeliveryOverflowState::default();
        let notification_generation = overflow.record_notification_loss().unwrap();
        assert_eq!(sender.queued_overflow_marker_token(&overflow), None);
        overflow.cancel_signal(notification_generation);
        for _ in 0..ACCOUNT_DELIVERY_BUFFER {
            sender.route_delivery(delivery(1, 11, 1), &overflow);
        }
        let RouteAdmission::Omitted {
            signal_generation: Some(generation),
            ..
        } = sender.route_delivery(delivery(1, 11, 2), &overflow)
        else {
            panic!("first omitted delivery reserves a control");
        };
        let token = overflow.pending_snapshot().unwrap().marker_token;
        assert_eq!(sender.queued_overflow_marker_token(&overflow), Some(token));
        sender
            .try_send(AccountDeliveryEvent::Overflow { generation })
            .unwrap();
        assert_eq!(sender.queued_overflow_marker_token(&overflow), Some(token));
        for _ in 0..ACCOUNT_DELIVERY_BUFFER {
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
        }
        assert!(matches!(
            receiver.try_recv(&overflow),
            Some(AccountDeliveryEvent::Overflow { generation: observed }) if observed == generation
        ));
        assert!(overflow.consume_signal(generation).consumed_current_control);
        assert_eq!(sender.queued_overflow_marker_token(&overflow), None);
        // The same pending generation can later acquire a notification signal
        // with dropped > 0, but that signal has no queue reservation.
        let notification_again = overflow.record_notification_loss().unwrap();
        assert_eq!(sender.queued_overflow_marker_token(&overflow), None);
        overflow.cancel_signal(notification_again);
        for _ in 0..ACCOUNT_DELIVERY_BUFFER {
            sender.route_delivery(delivery(1, 11, 3), &overflow);
        }
        let RouteAdmission::Omitted {
            signal_generation: Some(next),
            ..
        } = sender.route_delivery(delivery(1, 11, 4), &overflow)
        else {
            panic!("later loss reserves another control in the same generation");
        };
        assert_eq!(next, generation);
        assert_eq!(sender.queued_overflow_marker_token(&overflow), Some(token));
        sender.cancel_overflow_signal(&overflow, next);
        assert_eq!(sender.queued_overflow_marker_token(&overflow), None);
        sender.retire_routes();
        assert_eq!(sender.queued_overflow_marker_token(&overflow), None);
    }

    #[test]
    fn three_groups_progress_with_continuing_hot_group_and_keep_each_group_order() {
        let (sender, mut receiver) = AccountDeliverySender::channel();
        sender.install_routes(vec![route(1, 11), route(2, 22), route(3, 33)]);
        let overflow = AccountDeliveryOverflowState::default();
        for id in 1..=30 {
            assert!(matches!(
                sender.route_delivery(delivery(1, 11, id), &overflow),
                RouteAdmission::Accepted
            ));
        }
        for id in 101..=110 {
            sender.route_delivery(delivery(2, 22, id), &overflow);
            sender.route_delivery(delivery(3, 33, id + 20), &overflow);
        }
        let mut result = Vec::new();
        for id in 31..=50 {
            result.push(pop_id(&mut receiver, &overflow));
            sender.route_delivery(delivery(1, 11, id), &overflow);
        }
        assert!(
            result
                .iter()
                .position(|id| (101..=110).contains(id))
                .unwrap()
                < 4
        );
        assert!(
            result
                .iter()
                .position(|id| (121..=130).contains(id))
                .unwrap()
                < 6
        );
        assert!(
            result
                .iter()
                .filter(|&&id| (101..=110).contains(&id))
                .count()
                >= 3
        );
        assert!(
            result
                .iter()
                .filter(|&&id| (121..=130).contains(&id))
                .count()
                >= 3
        );
        while receiver.try_recv(&overflow).is_some() {}
        // A second finite queue verifies exact per-group sequence after
        // alternation, independently of how many hot arrivals continued.
        let (sender, mut receiver) = AccountDeliverySender::channel();
        sender.install_routes(vec![route(1, 11), route(2, 22), route(3, 33)]);
        for id in 1..=30 {
            sender.route_delivery(delivery(1, 11, id), &overflow);
        }
        for id in 101..=110 {
            sender.route_delivery(delivery(2, 22, id), &overflow);
            sender.route_delivery(delivery(3, 33, id + 20), &overflow);
        }
        let mut observed = Vec::new();
        while let Some(AccountDeliveryEvent::Delivery(delivery)) = receiver.try_recv(&overflow) {
            observed.push(delivery.message.payload[0]);
        }
        for expected in [
            (1..=30).collect::<Vec<_>>(),
            (101..=110).collect::<Vec<_>>(),
            (121..=130).collect::<Vec<_>>(),
        ] {
            assert_eq!(
                observed
                    .iter()
                    .copied()
                    .filter(|id| expected.contains(id))
                    .collect::<Vec<_>>(),
                expected
            );
        }
    }

    #[tokio::test]
    async fn pending_overflow_position_blocks_later_group_even_after_capacity_reopens() {
        let (sender, mut receiver) = AccountDeliverySender::channel();
        sender.install_routes(vec![route(1, 11), route(2, 22)]);
        let overflow = AccountDeliveryOverflowState::default();
        for _ in 0..ACCOUNT_DELIVERY_BUFFER {
            assert!(matches!(
                sender.route_delivery(delivery(1, 11, 1), &overflow),
                RouteAdmission::Accepted
            ));
        }
        let RouteAdmission::Omitted {
            signal_generation: Some(generation),
            ..
        } = sender.route_delivery(delivery(1, 11, 2), &overflow)
        else {
            panic!("first omission must reserve its control position");
        };
        assert_eq!(sender.capacity(), 0);
        assert!(matches!(
            sender.route_delivery(delivery(2, 22, 3), &overflow),
            RouteAdmission::Omitted {
                signal_generation: None,
                ..
            }
        ));
        for _ in 0..2 {
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
        }
        assert!(matches!(
            sender.route_delivery(delivery(2, 22, 4), &overflow),
            RouteAdmission::Accepted
        ));
        for _ in 2..ACCOUNT_DELIVERY_BUFFER {
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
        }
        assert!(
            receiver.try_recv(&overflow).is_none(),
            "pending control is a barrier"
        );
        assert!(
            timeout(Duration::from_millis(10), receiver.recv(&overflow))
                .await
                .is_err()
        );
        sender
            .try_send(AccountDeliveryEvent::Overflow { generation })
            .unwrap();
        assert!(matches!(
            receiver.recv(&overflow).await,
            Some(AccountDeliveryEvent::Overflow { generation: observed }) if observed == generation
        ));
        assert_eq!(overflow.consume_signal(generation).dropped, 2);
        assert_eq!(pop_id(&mut receiver, &overflow), 4);
        assert!(receiver.try_recv(&overflow).is_none());
    }

    #[test]
    fn unknown_ambiguous_and_rebound_routes_remain_fifo_barriers() {
        let (sender, mut receiver) = AccountDeliverySender::channel();
        let overflow = AccountDeliveryOverflowState::default();
        sender.install_routes(vec![route(1, 11), route(2, 22)]);
        sender.route_delivery(delivery(1, 11, 1), &overflow);
        sender.route_delivery(delivery(1, 11, 2), &overflow);
        sender.invalidate_routes();
        sender.install_routes(vec![route(1, 11), route(2, 22)]);
        sender.route_delivery(delivery(2, 22, 3), &overflow);
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
        assert_eq!(pop_id(&mut receiver, &overflow), 2);
        assert_eq!(pop_id(&mut receiver, &overflow), 3);
        sender.install_routes(vec![route(1, 11), route(2, 11)]);
        sender.route_delivery(delivery(1, 11, 4), &overflow);
        sender.route_delivery(delivery(2, 11, 5), &overflow);
        assert_eq!(pop_id(&mut receiver, &overflow), 4);
        assert_eq!(pop_id(&mut receiver, &overflow), 5);
        sender.invalidate_routes();
        sender.route_delivery(delivery(1, 11, 6), &overflow);
        sender.route_delivery(delivery(2, 22, 7), &overflow);
        assert_eq!(pop_id(&mut receiver, &overflow), 6);
        assert_eq!(pop_id(&mut receiver, &overflow), 7);
    }

    #[test]
    fn unchanged_sync_gap_keeps_keys_but_changed_or_retired_maps_do_not() {
        let (sender, mut receiver) = AccountDeliverySender::channel();
        let overflow = AccountDeliveryOverflowState::default();
        let routes = vec![route(1, 11), route(2, 22)];
        sender.install_routes(routes.clone());
        sender.route_delivery(delivery(1, 11, 1), &overflow);
        sender.route_delivery(delivery(1, 11, 2), &overflow);
        sender.invalidate_routes_if_changed(&routes);
        sender.route_delivery(delivery(2, 22, 3), &overflow);
        sender.install_routes(routes.clone());
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
        assert_eq!(pop_id(&mut receiver, &overflow), 3);
        assert_eq!(pop_id(&mut receiver, &overflow), 2);

        sender.route_delivery(delivery(1, 11, 4), &overflow);
        sender.route_delivery(delivery(1, 11, 5), &overflow);
        let changed = vec![route(1, 12), route(2, 22)];
        sender.invalidate_routes_if_changed(&changed);
        // A failed or cancelled sync does not revalidate old admissions.
        sender.route_delivery(delivery(2, 22, 6), &overflow);
        assert_eq!(pop_id(&mut receiver, &overflow), 4);
        assert_eq!(pop_id(&mut receiver, &overflow), 5);
        assert_eq!(pop_id(&mut receiver, &overflow), 6);

        sender.install_routes(changed.clone());
        sender.route_delivery(delivery(1, 12, 7), &overflow);
        sender.route_delivery(delivery(1, 12, 8), &overflow);
        sender.retire_routes();
        sender.install_routes(changed);
        sender.route_delivery(delivery(2, 22, 9), &overflow);
        assert_eq!(pop_id(&mut receiver, &overflow), 7);
        assert_eq!(pop_id(&mut receiver, &overflow), 8);
        assert_eq!(pop_id(&mut receiver, &overflow), 9);
    }

    #[tokio::test]
    async fn receive_cancellation_and_sender_receiver_close_keep_exact_items() {
        let (sender, mut receiver) = AccountDeliverySender::channel();
        let overflow = AccountDeliveryOverflowState::default();
        assert!(
            timeout(Duration::from_millis(10), receiver.recv(&overflow))
                .await
                .is_err()
        );
        sender.route_delivery(delivery(1, 11, 1), &overflow);
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
        let clone = sender.clone();
        drop(sender);
        assert!(
            timeout(Duration::from_millis(10), receiver.recv(&overflow))
                .await
                .is_err()
        );
        clone.route_delivery(delivery(1, 11, 2), &overflow);
        drop(clone);
        assert_eq!(pop_id(&mut receiver, &overflow), 2);
        assert!(receiver.recv(&overflow).await.is_none());
        let (sender, receiver) = AccountDeliverySender::channel();
        drop(receiver);
        assert!(matches!(
            sender.route_delivery(delivery(1, 11, 3), &overflow),
            RouteAdmission::Closed
        ));
    }

    #[tokio::test]
    async fn cancelling_reserved_control_drains_accepted_suffix_and_rejects_late_marker() {
        let (sender, mut receiver) = AccountDeliverySender::channel();
        let overflow = AccountDeliveryOverflowState::default();
        for _ in 0..ACCOUNT_DELIVERY_BUFFER {
            assert!(matches!(
                sender.route_delivery(delivery(1, 11, 1), &overflow),
                RouteAdmission::Accepted
            ));
        }
        let RouteAdmission::Omitted {
            signal_generation: Some(generation),
            ..
        } = sender.route_delivery(delivery(1, 11, 2), &overflow)
        else {
            panic!("first omission reserves control");
        };
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
        assert_eq!(pop_id(&mut receiver, &overflow), 1);
        assert!(matches!(
            sender.route_delivery(delivery(2, 22, 3), &overflow),
            RouteAdmission::Accepted
        ));
        sender.cancel_overflow_signal(&overflow, generation);
        assert!(matches!(
            sender.try_send(AccountDeliveryEvent::Overflow { generation }),
            Err(TrySendError::MissingReservation(_))
        ));
        drop(sender);
        for _ in 2..ACCOUNT_DELIVERY_BUFFER {
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
        }
        assert_eq!(pop_id(&mut receiver, &overflow), 3);
        assert!(
            timeout(Duration::from_secs(1), receiver.recv(&overflow))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(overflow.pending_snapshot().unwrap().dropped, 1);
    }

    #[tokio::test]
    async fn cancelling_already_activated_control_keeps_loss_pending_and_drains_queue() {
        let (sender, mut receiver) = AccountDeliverySender::channel();
        let overflow = AccountDeliveryOverflowState::default();
        for _ in 0..ACCOUNT_DELIVERY_BUFFER {
            sender.route_delivery(delivery(1, 11, 1), &overflow);
        }
        let RouteAdmission::Omitted {
            signal_generation: Some(generation),
            ..
        } = sender.route_delivery(delivery(1, 11, 2), &overflow)
        else {
            panic!("first omission reserves control");
        };
        sender
            .try_send(AccountDeliveryEvent::Overflow { generation })
            .unwrap();
        sender.cancel_overflow_signal(&overflow, generation);
        drop(sender);
        for _ in 0..ACCOUNT_DELIVERY_BUFFER {
            assert_eq!(pop_id(&mut receiver, &overflow), 1);
        }
        assert!(receiver.recv(&overflow).await.is_none());
        assert_eq!(overflow.pending_snapshot().unwrap().dropped, 1);
    }
}
