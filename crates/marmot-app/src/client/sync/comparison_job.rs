//! Immutable comparison I/O for a worker-owned recovery grant. The task has no
//! account storage, engine, session, or event-queue authority.

use super::*;
#[cfg(test)]
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::OwnedSemaphorePermit;
use tokio::task::JoinHandle;
use transport_nostr_adapter::{NostrReconciliationProgress, SubscriptionAttempt};

/// The bounded off-worker shape. A larger route retains the existing inline
/// executor with its complete endpoint set.
pub(crate) const MAX_COMPARISON_ENDPOINTS_PER_ROUTE: usize = 4;

#[cfg(test)]
#[derive(Clone, Default)]
pub(crate) struct TestComparisonActivityWitness {
    pub(crate) attempt_serial: Arc<AtomicU64>,
    pub(crate) active_jobs: Arc<AtomicUsize>,
    pub(crate) active_requests: Arc<AtomicUsize>,
    #[cfg(feature = "test-policy-overrides")]
    pub(crate) startup_branch: Arc<Mutex<Option<TestStartupBranchWitness>>>,
    pub(crate) network_deadline: Arc<Mutex<Option<tokio::time::Instant>>>,
    pub(crate) target_event_id: Arc<Mutex<Option<String>>>,
    #[cfg(feature = "test-policy-overrides")]
    pub(crate) target_route: Arc<Mutex<Option<TransportReconciliationRoute>>>,
    #[cfg(feature = "test-policy-overrides")]
    pub(crate) route_outcomes: Arc<Mutex<Vec<TestComparisonRouteOutcome>>>,
    #[cfg(feature = "test-policy-overrides")]
    pub(crate) target_decisions: Arc<Mutex<TestComparisonDecisionWindow>>,
    #[cfg(feature = "test-policy-overrides")]
    pub(crate) diagnostic_origin: Arc<Mutex<Option<std::time::Instant>>>,
    pub(crate) returned_events: Arc<AtomicUsize>,
    pub(crate) matching_events: Arc<AtomicUsize>,
    pub(crate) matching_queued_deliveries: Arc<AtomicUsize>,
    pub(crate) panic_after_queue_submission: Arc<std::sync::atomic::AtomicBool>,
}

#[cfg(all(test, feature = "test-policy-overrides"))]
#[derive(Clone, Debug)]
pub(crate) struct TestStartupBranchWitness {
    pub(crate) branch: &'static str,
    pub(crate) eligibility: &'static str,
    pub(crate) credit_available: bool,
    pub(crate) attempt_serial: Option<u64>,
    pub(crate) plan_items: usize,
    pub(crate) non_incremental_items: usize,
}

#[cfg(all(test, feature = "test-policy-overrides"))]
impl TestComparisonActivityWitness {
    pub(crate) fn startup_branch_for_target<F>(
        target: Option<&Self>,
        make: F,
    ) -> Option<TestStartupBranchWitness>
    where
        F: FnOnce() -> TestStartupBranchWitness,
    {
        target.map(|_| make())
    }

    pub(crate) fn record_startup_branch(&self, branch: TestStartupBranchWitness) {
        *self.startup_branch.lock().unwrap() = Some(branch);
    }

    pub(crate) fn startup_failure_snapshot(
        &self,
    ) -> (usize, usize, u64, Option<TestStartupBranchWitness>) {
        (
            self.active_jobs.load(Ordering::SeqCst),
            self.active_requests.load(Ordering::SeqCst),
            self.attempt_serial.load(Ordering::SeqCst),
            self.startup_branch.lock().unwrap().clone(),
        )
    }
}

#[cfg(all(test, feature = "test-policy-overrides"))]
#[derive(Clone, Debug)]
pub(crate) struct TestComparisonDecision {
    pub(crate) attempt_serial: u64,
    pub(crate) comparison_diagnostics: Option<transport_nostr_adapter::NostrComparisonDiagnostics>,
    pub(crate) clean_suffix: Option<bool>,
    pub(crate) candidate_count: Option<usize>,
    pub(crate) full_queue_handoff: Option<bool>,
    pub(crate) handed_off_candidates: Option<usize>,
}

#[cfg(all(test, feature = "test-policy-overrides"))]
#[derive(Clone, Debug, Default)]
pub(crate) struct TestComparisonDecisionWindow {
    pub(crate) records: Vec<TestComparisonDecision>,
    pub(crate) dropped: usize,
}

#[cfg(all(test, feature = "test-policy-overrides"))]
impl TestComparisonDecisionWindow {
    fn record(&mut self, record: TestComparisonDecision) {
        if self.records.len() < 2 {
            self.records.push(record);
        } else {
            self.dropped += 1;
        }
    }
}

#[cfg(all(test, feature = "test-policy-overrides"))]
#[derive(Clone, Debug)]
pub(crate) struct TestComparisonRouteOutcome {
    pub(crate) attempt_serial: u64,
    pub(crate) target_route: bool,
    pub(crate) inbox: bool,
    pub(crate) kind: &'static str,
    pub(crate) relays_succeeded: usize,
    pub(crate) relays_failed: usize,
    pub(crate) remote_items: usize,
    pub(crate) received_items: usize,
    pub(crate) comparison_diagnostics: Option<transport_nostr_adapter::NostrComparisonDiagnostics>,
    pub(crate) started_ms: Option<u64>,
    pub(crate) finished_ms: Option<u64>,
}

#[cfg(all(test, feature = "test-policy-overrides"))]
fn diagnostic_elapsed_ms(
    origin: Option<std::time::Instant>,
    at: std::time::Instant,
) -> Option<u64> {
    origin.map(|origin| {
        at.saturating_duration_since(origin)
            .as_millis()
            .min(u64::MAX as u128) as u64
    })
}

#[cfg(test)]
struct ActiveCounter(Arc<AtomicUsize>);

#[cfg(test)]
impl ActiveCounter {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::SeqCst);
        Self(counter)
    }
}

#[cfg(test)]
impl Drop for ActiveCounter {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

struct MemoryProgress {
    cursor: Mutex<Option<[u8; 32]>>,
}

impl NostrReconciliationProgress for MemoryProgress {
    fn load_cursor(&self) -> Result<Option<[u8; 32]>, cgka_traits::TransportAdapterError> {
        Ok(*self.cursor.lock().expect("comparison progress mutex"))
    }

    fn save_cursor(
        &self,
        cursor: Option<[u8; 32]>,
    ) -> Result<(), cgka_traits::TransportAdapterError> {
        *self.cursor.lock().expect("comparison progress mutex") = cursor;
        Ok(())
    }
}

struct FrozenRoute {
    inventory: super::super::recovery::FrozenRecoveryInventory,
    initial_cursor: Option<[u8; 32]>,
}

// The feature-gated SDK decision fields slightly enlarge the owned summary;
// boxing the route result would also change the production handoff.
#[cfg_attr(feature = "test-policy-overrides", allow(clippy::large_enum_variant))]
pub(crate) enum ComparisonRouteWorkResult {
    Skipped,
    TimedOut,
    Returned(OwnedComparisonResult),
}

pub(crate) struct ComparisonRouteResult {
    #[cfg(all(test, feature = "test-policy-overrides"))]
    attempt_serial: u64,
    route: TransportReconciliationRoute,
    initial_cursor: Option<[u8; 32]>,
    cursor: Option<[u8; 32]>,
    result: ComparisonRouteWorkResult,
    continuation_evidence: Option<PositiveContinuationEvidence>,
}

pub(crate) struct ComparisonNetworkResult {
    routes: Vec<ComparisonRouteResult>,
}

impl ComparisonNetworkResult {
    pub(super) fn continuation_candidate_ids(&self) -> HashSet<[u8; 32]> {
        self.routes
            .iter()
            .filter_map(|route| route.continuation_evidence.as_ref())
            .flat_map(|evidence| evidence.candidate_ids.iter().copied())
            .collect()
    }
}

pub(crate) struct RouteSubmission {
    pub(super) route: TransportReconciliationRoute,
    initial_cursor: Option<[u8; 32]>,
    cursor: Option<[u8; 32]>,
    cursor_safe: bool,
    outcome: storage_sqlite::RecoveryComparisonOutcome,
    pub(super) continuation_evidence: Option<PositiveContinuationEvidence>,
    // A successful adapter handoff is not account consumption. Retain only
    // the bounded evidence IDs for which the adapter reported a delivery.
    pub(super) queued_candidates: Vec<([u8; 32], u64)>,
    pub(super) unavailable_route: Option<TransportReconciliationRoute>,
    #[cfg(test)]
    attempted: usize,
    #[cfg(test)]
    delivered: usize,
}

#[cfg(test)]
impl RouteSubmission {
    pub(crate) fn delivery_counts_for_test(&self) -> (usize, usize) {
        (self.attempted, self.delivered)
    }
}

/// The queue producer owns no account engine or storage. The worker can drain
/// the same adapter queue while this bounded producer waits for capacity.
pub(crate) struct EpochGapQueueJob {
    credit: Option<Arc<OwnedSemaphorePermit>>,
    handle: JoinHandle<Vec<RouteSubmission>>,
}

impl Drop for EpochGapQueueJob {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl EpochGapQueueJob {
    pub(crate) fn start(
        client: &AppClient,
        network: ComparisonNetworkResult,
        credit: OwnedSemaphorePermit,
        #[cfg(test)] witness: Option<TestComparisonActivityWitness>,
    ) -> Self {
        let adapter = client.adapter.clone();
        let credit = Arc::new(credit);
        let task_credit = credit.clone();
        let handle = tokio::spawn(async move {
            let _credit = task_credit;
            let deadline = tokio::time::Instant::now() + TRANSPORT_RECONCILIATION_QUANTUM;
            let mut submitted = Vec::with_capacity(network.routes.len());
            for route in network.routes {
                submitted.push(
                    submit_reconciliation_route(
                        &adapter,
                        route,
                        deadline,
                        #[cfg(test)]
                        witness.as_ref(),
                    )
                    .await,
                );
            }
            #[cfg(test)]
            if witness
                .as_ref()
                .is_some_and(|witness| witness.panic_after_queue_submission.load(Ordering::SeqCst))
            {
                panic!("injected online recovery queue panic after submission");
            }
            submitted
        });
        Self {
            credit: Some(credit),
            handle,
        }
    }

    pub(crate) async fn wait(&mut self) -> Result<Vec<RouteSubmission>, tokio::task::JoinError> {
        (&mut self.handle).await
    }

    pub(crate) async fn abort_and_wait(mut self) -> Arc<OwnedSemaphorePermit> {
        self.handle.abort();
        let _ = (&mut self.handle).await;
        self.credit.take().expect("queue job owns credit")
    }

    pub(crate) fn into_credit(mut self) -> Arc<OwnedSemaphorePermit> {
        self.credit.take().expect("queue job owns credit")
    }
}

#[cfg(test)]
impl ComparisonNetworkResult {
    pub(crate) fn outcome_kinds_for_test(&self) -> Vec<&'static str> {
        self.routes
            .iter()
            .map(|route| match &route.result {
                ComparisonRouteWorkResult::Skipped => "route_skipped",
                ComparisonRouteWorkResult::TimedOut => "route_timed_out",
                ComparisonRouteWorkResult::Returned(Ok(None)) => "route_unsupported",
                ComparisonRouteWorkResult::Returned(Err(_)) => "route_error",
                ComparisonRouteWorkResult::Returned(Ok(Some((summary, events)))) => {
                    if summary.relays_failed > 0 {
                        "route_relay_failed"
                    } else if events.is_empty() {
                        "route_no_events"
                    } else {
                        "route_events"
                    }
                }
            })
            .collect()
    }
}

/// Dropping the worker's handle cancels I/O and drops all unadmitted events.
pub(crate) struct ComparisonNetworkJob {
    handle: JoinHandle<(OwnedSemaphorePermit, ComparisonNetworkResult)>,
}

impl Drop for ComparisonNetworkJob {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl ComparisonNetworkJob {
    /// Cancel the request and wait until its future has dropped the shared credit.
    pub(crate) async fn abort_and_wait(mut self) {
        self.handle.abort();
        let _ = (&mut self.handle).await;
    }

    pub(crate) fn start(
        client: &AppClient,
        grant: &AttemptGrant,
        credit: OwnedSemaphorePermit,
        #[cfg(test)] witness: Option<TestComparisonActivityWitness>,
    ) -> Result<Self, AppError> {
        let storage = client.app.account_storage(&client.state.label)?;
        let routes = grant
            .inventory
            .iter()
            .map(|inventory| {
                Ok(FrozenRoute {
                    inventory: inventory.clone(),
                    initial_cursor: storage
                        .transport_reconciliation_replay_cursor(&inventory.route)?,
                })
            })
            .collect::<Result<Vec<_>, AppError>>()?;
        let adapter = client.adapter.clone();
        #[cfg(test)]
        let attempt_serial = grant.reservation.attempt_serial;
        let task = async move {
            #[cfg(test)]
            let _job_active = witness.as_ref().map(|witness| {
                witness
                    .attempt_serial
                    .store(attempt_serial, Ordering::SeqCst);
                ActiveCounter::new(witness.active_jobs.clone())
            });
            let deadline = tokio::time::Instant::now() + TRANSPORT_RECONCILIATION_QUANTUM;
            #[cfg(test)]
            if let Some(witness) = &witness {
                *witness.network_deadline.lock().unwrap() = Some(deadline);
            }
            let mut results = Vec::with_capacity(routes.len());
            for frozen in routes {
                let FrozenRoute {
                    inventory,
                    initial_cursor,
                } = frozen;
                #[cfg(all(test, feature = "test-policy-overrides"))]
                let started_at = std::time::Instant::now();
                if tokio::time::Instant::now() >= deadline {
                    #[cfg(all(test, feature = "test-policy-overrides"))]
                    if let Some(witness) = &witness {
                        let origin = *witness.diagnostic_origin.lock().unwrap();
                        let target_route = witness
                            .target_route
                            .lock()
                            .unwrap()
                            .as_ref()
                            .is_some_and(|target| target == &inventory.route);
                        witness
                            .route_outcomes
                            .lock()
                            .unwrap()
                            .push(TestComparisonRouteOutcome {
                                attempt_serial,
                                target_route,
                                inbox: matches!(
                                    &inventory.route,
                                    TransportReconciliationRoute::Inbox
                                ),
                                kind: "skipped",
                                relays_succeeded: 0,
                                relays_failed: 0,
                                remote_items: 0,
                                received_items: 0,
                                comparison_diagnostics: None,
                                started_ms: diagnostic_elapsed_ms(origin, started_at),
                                finished_ms: diagnostic_elapsed_ms(
                                    origin,
                                    std::time::Instant::now(),
                                ),
                            });
                    }
                    results.push(ComparisonRouteResult {
                        #[cfg(all(test, feature = "test-policy-overrides"))]
                        attempt_serial,
                        route: inventory.route,
                        initial_cursor,
                        cursor: initial_cursor,
                        result: ComparisonRouteWorkResult::Skipped,
                        continuation_evidence: None,
                    });
                    continue;
                }
                let progress = Arc::new(MemoryProgress {
                    cursor: Mutex::new(initial_cursor),
                });
                let run = async {
                    #[cfg(test)]
                    let _request_active = witness
                        .as_ref()
                        .map(|witness| ActiveCounter::new(witness.active_requests.clone()));
                    match inventory.work {
                        TransportReconciliationWork::Inbox(endpoints) => {
                            adapter
                                .reconcile_inbox_history(
                                    endpoints,
                                    &inventory.items,
                                    inventory.since,
                                    inventory.until,
                                    progress.as_ref(),
                                )
                                .await
                        }
                        TransportReconciliationWork::Group(group) => {
                            adapter
                                .reconcile_group_history(
                                    group,
                                    &inventory.items,
                                    inventory.since,
                                    inventory.until,
                                    progress.as_ref(),
                                )
                                .await
                        }
                    }
                };
                let result = match tokio::time::timeout_at(deadline, run).await {
                    Ok(value) => ComparisonRouteWorkResult::Returned(value),
                    Err(_) => ComparisonRouteWorkResult::TimedOut,
                };
                #[cfg(test)]
                if let Some(witness) = &witness
                    && let ComparisonRouteWorkResult::Returned(Ok(Some((_, events)))) = &result
                {
                    witness
                        .returned_events
                        .fetch_add(events.len(), Ordering::SeqCst);
                    if let Some(target) = witness.target_event_id.lock().unwrap().as_ref() {
                        witness.matching_events.fetch_add(
                            events
                                .iter()
                                .filter(|event| event.event.id.eq_ignore_ascii_case(target))
                                .count(),
                            Ordering::SeqCst,
                        );
                    }
                }
                let cursor = *progress.cursor.lock().expect("comparison progress mutex");
                let continuation_evidence = match &result {
                    ComparisonRouteWorkResult::Returned(Ok(Some((summary, events)))) => {
                        positive_continuation_evidence(
                            &inventory.route,
                            &inventory.items,
                            summary,
                            events,
                        )
                    }
                    _ => None,
                };
                #[cfg(all(test, feature = "test-policy-overrides"))]
                if let Some(witness) = &witness {
                    let finished_at = std::time::Instant::now();
                    let origin = *witness.diagnostic_origin.lock().unwrap();
                    let (
                        kind,
                        relays_succeeded,
                        relays_failed,
                        remote_items,
                        received_items,
                        comparison_diagnostics,
                    ) = match &result {
                        ComparisonRouteWorkResult::Skipped => ("skipped", 0, 0, 0, 0, None),
                        ComparisonRouteWorkResult::TimedOut => ("timed_out", 0, 0, 0, 0, None),
                        ComparisonRouteWorkResult::Returned(Ok(None)) => {
                            ("unsupported", 0, 0, 0, 0, None)
                        }
                        ComparisonRouteWorkResult::Returned(Err(_)) => ("error", 0, 0, 0, 0, None),
                        ComparisonRouteWorkResult::Returned(Ok(Some((summary, _)))) => (
                            "returned",
                            summary.relays_succeeded,
                            summary.relays_failed,
                            summary.remote_items,
                            summary.received_items,
                            Some(summary.comparison_diagnostics.clone()),
                        ),
                    };
                    let target_route = witness
                        .target_route
                        .lock()
                        .unwrap()
                        .as_ref()
                        .is_some_and(|target| target == &inventory.route);
                    if target_route {
                        let (comparison_diagnostics, clean_suffix, candidate_count) =
                            if let ComparisonRouteWorkResult::Returned(Ok(Some((
                                summary,
                                events,
                            )))) = &result
                            {
                                let frozen = inventory
                                    .items
                                    .iter()
                                    .map(|item| hex::encode(item.event_id))
                                    .collect::<HashSet<_>>();
                                (
                                    Some(summary.comparison_diagnostics.clone()),
                                    Some(summary.clean_bounded_suffix),
                                    Some(
                                        events
                                            .iter()
                                            .filter(|event| !frozen.contains(&event.event.id))
                                            .take(16)
                                            .count(),
                                    ),
                                )
                            } else {
                                (None, None, None)
                            };
                        witness
                            .target_decisions
                            .lock()
                            .unwrap()
                            .record(TestComparisonDecision {
                                attempt_serial,
                                comparison_diagnostics,
                                clean_suffix,
                                candidate_count,
                                full_queue_handoff: None,
                                handed_off_candidates: None,
                            });
                    }
                    witness
                        .route_outcomes
                        .lock()
                        .unwrap()
                        .push(TestComparisonRouteOutcome {
                            attempt_serial,
                            target_route,
                            inbox: matches!(&inventory.route, TransportReconciliationRoute::Inbox),
                            kind,
                            relays_succeeded,
                            relays_failed,
                            remote_items,
                            received_items,
                            comparison_diagnostics,
                            started_ms: diagnostic_elapsed_ms(origin, started_at),
                            finished_ms: diagnostic_elapsed_ms(origin, finished_at),
                        });
                }
                results.push(ComparisonRouteResult {
                    #[cfg(all(test, feature = "test-policy-overrides"))]
                    attempt_serial,
                    route: inventory.route,
                    initial_cursor,
                    cursor,
                    result,
                    continuation_evidence,
                });
            }
            (credit, ComparisonNetworkResult { routes: results })
        };
        let handle = tokio::spawn(task);
        Ok(Self { handle })
    }

    pub(crate) async fn wait(
        &mut self,
    ) -> Result<(OwnedSemaphorePermit, ComparisonNetworkResult), tokio::task::JoinError> {
        (&mut self.handle).await
    }
}

impl AppClient {
    /// Capacity preflight before the owner spends a retry reservation. This is
    /// deliberately narrower than the post-selection grant check below.
    pub(crate) fn epoch_gap_only_waiting_for_credit(&self) -> Result<bool, AppError> {
        let storage = self.app.account_storage(&self.state.label)?;
        let fence = storage.recovery_eligible_revision_fence(false)?;
        if fence.obligations.len() != 1
            || storage.recovery_comparison()?.pending()
            || self.delivery_loss_blocks_cursor()
            || !storage
                .recovery_loss_snapshot(
                    &self.state.label,
                    storage_sqlite::RecoveryLossCause::Queue,
                )?
                .is_empty()
            || !storage
                .recovery_loss_snapshot(
                    &self.state.label,
                    storage_sqlite::RecoveryLossCause::NotificationConsumer,
                )?
                .is_empty()
        {
            return Ok(false);
        }
        // A zero-credit preflight may defer a grant only when its visible
        // routing shape fits the off-worker cap. Otherwise preserve the
        // inline executor even while another account holds both credits.
        let routes = self.routing.snapshot();
        let visible_routes =
            routes.group_routes.len() + usize::from(!routes.local_inbox_endpoints.is_empty());
        if visible_routes == 0
            || visible_routes > TRANSPORT_RECONCILIATION_MAX_ROUTES_PER_PASS
            || routes.local_inbox_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
            || routes
                .group_routes
                .iter()
                .any(|route| route.endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE)
        {
            return Ok(false);
        }
        let selected = fence.obligations[0];
        if storage
            .recovery_scope_snapshots(selected.0)?
            .iter()
            .any(|scope| {
                scope.plan.required_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                    || scope.plan.admitted_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
            })
        {
            return Ok(false);
        }
        Ok(storage.pending_recovery_demands()?.iter().any(|demand| {
            demand.ticket.id == selected.0
                && demand.ticket.revision == selected.1
                && demand.cause == storage_sqlite::RecoveryCause::EpochGap
        }))
    }

    /// Avoid spending a QueueLoss reservation while the only bounded worker
    /// continuation is waiting for shared capacity. The frozen grant is still
    /// checked after a permit becomes available.
    pub(crate) fn queue_loss_only_waiting_for_credit(&self) -> Result<bool, AppError> {
        let storage = self.app.account_storage(&self.state.label)?;
        let fence = storage.recovery_eligible_revision_fence(false)?;
        if fence.obligations.len() != 1 || storage.recovery_comparison()?.pending() {
            return Ok(false);
        }
        let routes = self.routing.snapshot();
        let visible_routes =
            routes.group_routes.len() + usize::from(!routes.local_inbox_endpoints.is_empty());
        if visible_routes == 0
            || visible_routes > TRANSPORT_RECONCILIATION_MAX_ROUTES_PER_PASS
            || routes.local_inbox_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
            || routes
                .group_routes
                .iter()
                .any(|route| route.endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE)
            || storage
                .recovery_scope_snapshots(fence.obligations[0].0)?
                .iter()
                .any(|scope| {
                    scope.plan.required_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                        || scope.plan.admitted_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                })
        {
            return Ok(false);
        }
        Ok(storage.pending_recovery_demands()?.iter().any(|demand| {
            demand.ticket.id == fence.obligations[0].0
                && demand.ticket.revision == fence.obligations[0].1
                && demand.cause == storage_sqlite::RecoveryCause::QueueLoss
        }))
    }

    /// The bounded steady-state continuation for one EpochGap or QueueLoss
    /// owner grant. Comparison-slot completion remains a separate path.
    pub(crate) fn online_recovery_offload_eligible(
        &self,
        grant: &AttemptGrant,
    ) -> Result<bool, AppError> {
        let Some(plan) = grant.plan() else {
            return Ok(false);
        };
        let cause = plan.first().map(|obligation| obligation.cause);
        let eligible_seam = match cause {
            Some(storage_sqlite::RecoveryCause::EpochGap) => {
                grant.seam == marmot_forensics::EpochBackfillExecutionSeam::Receive
            }
            Some(storage_sqlite::RecoveryCause::QueueLoss) => matches!(
                grant.seam,
                marmot_forensics::EpochBackfillExecutionSeam::Receive
                    | marmot_forensics::EpochBackfillExecutionSeam::Maintenance
            ),
            _ => false,
        };
        if !eligible_seam
            || grant.comparison_revision.is_some()
            || plan.len() != 1
            || grant.fence.obligations.len() != 1
            || grant.fence.obligations[0].0 != plan[0].id
            || (cause == Some(storage_sqlite::RecoveryCause::EpochGap)
                && self.delivery_loss_blocks_cursor())
            || grant.inventory.is_empty()
            || grant.inventory.len() > TRANSPORT_RECONCILIATION_MAX_ROUTES_PER_PASS
            || plan[0].scopes.iter().any(|scope| {
                scope.goal.admitted_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                    || scope.goal.required_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
            })
            || grant.inventory.iter().any(|route| match &route.work {
                TransportReconciliationWork::Inbox(endpoints) => {
                    endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                }
                TransportReconciliationWork::Group(group) => {
                    group.endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                }
            })
        {
            return Ok(false);
        }
        let storage = self.app.account_storage(&self.state.label)?;
        Ok(storage.pending_recovery_demands()?.iter().any(|demand| {
            demand.ticket.id == grant.fence.obligations[0].0
                && demand.ticket.revision == grant.fence.obligations[0].1
                && Some(demand.cause) == cause
        }))
    }

    /// An advisory preflight used only when the shared credit pool is empty.
    /// Independent debt still reaches the legacy executor. The actual frozen
    /// grant is checked again after authorization when a credit exists.
    pub(crate) fn comparison_only_waiting_for_credit(&self) -> Result<bool, AppError> {
        let storage = self.app.account_storage(&self.state.label)?;
        if !storage.recovery_comparison()?.pending()
            || storage
                .pending_recovery_demands()?
                .iter()
                .any(|demand| demand.cause != storage_sqlite::RecoveryCause::IncrementalHistory)
            || !storage
                .recovery_loss_snapshot(
                    &self.state.label,
                    storage_sqlite::RecoveryLossCause::Queue,
                )?
                .is_empty()
            || !storage
                .recovery_loss_snapshot(
                    &self.state.label,
                    storage_sqlite::RecoveryLossCause::NotificationConsumer,
                )?
                .is_empty()
        {
            return Ok(false);
        }
        let routes = self.routing.snapshot();
        Ok(
            routes.local_inbox_endpoints.len() <= MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                && routes
                    .group_routes
                    .iter()
                    .all(|route| route.endpoints.len() <= MAX_COMPARISON_ENDPOINTS_PER_ROUTE),
        )
    }

    /// The sole eligible automatic selection is the comparison and, when
    /// present, its own IncrementalHistory obligation. The frozen grant is the
    /// authority; an earlier pending-demand probe never decides this.
    pub(crate) fn comparison_offload_eligible(
        &self,
        grant: &AttemptGrant,
    ) -> Result<bool, AppError> {
        if grant.comparison_revision.is_none()
            || grant.inventory.len() > TRANSPORT_RECONCILIATION_MAX_ROUTES_PER_PASS
            || grant.comparison_plan.as_ref().is_none_or(|plan| {
                plan.routes.len() > TRANSPORT_RECONCILIATION_MAX_ROUTES_PER_PASS
                    || plan.routes.iter().any(|route| {
                        route.admitted_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                            || route.required_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                    })
            })
            || grant.inventory.iter().any(|route| match &route.work {
                TransportReconciliationWork::Inbox(endpoints) => {
                    endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                }
                TransportReconciliationWork::Group(group) => {
                    group.endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                }
            })
        {
            return Ok(false);
        }
        let Some(plan) = grant.plan() else {
            return Ok(false);
        };
        if plan.len() > 1
            || plan.iter().any(|item| {
                item.cause != storage_sqlite::RecoveryCause::IncrementalHistory
                    || item.scopes.iter().any(|scope| {
                        scope.goal.admitted_endpoints.len() > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                            || scope.goal.required_endpoints.len()
                                > MAX_COMPARISON_ENDPOINTS_PER_ROUTE
                    })
            })
        {
            return Ok(false);
        }
        let selected = plan.iter().map(|item| item.id).collect::<Vec<_>>();
        if selected.len() != grant.fence.obligations.len()
            || !selected.iter().all(|id| {
                grant
                    .fence
                    .obligations
                    .iter()
                    .any(|(candidate, _)| candidate == id)
            })
        {
            return Ok(false);
        }
        let storage = self.app.account_storage(&self.state.label)?;
        let pending = storage.pending_recovery_demands()?;
        Ok(grant.fence.obligations.iter().all(|(id, revision)| {
            pending.iter().any(|demand| {
                demand.ticket.id == *id
                    && demand.ticket.revision == *revision
                    && demand.cause == storage_sqlite::RecoveryCause::IncrementalHistory
            })
        }))
    }

    pub(crate) async fn activate_comparison_grant(
        &mut self,
        grant: &AttemptGrant,
        telemetry: Option<&AppPerformanceTelemetry>,
    ) -> Result<SubscriptionAttempt, AppError> {
        let mut activation = EpochBackfillActivationOutcome::Failed;
        self.activate_recovery_grant_inner(grant, telemetry, &mut activation)
            .await
            .map_err(|failure| failure.source)?;
        self.adapter
            .account_subscription_attempt()
            .await
            .ok_or_else(|| {
                cgka_traits::TransportAdapterError::Subscription(
                    "activated comparison subscription disappeared".into(),
                )
                .into()
            })
    }

    pub(crate) async fn finish_comparison_grant(
        &mut self,
        grant: AttemptGrant,
        attempt: SubscriptionAttempt,
        network: ComparisonNetworkResult,
    ) -> Result<EpochBackfillRunOutcome, AppError> {
        let storage = self.app.account_storage(&self.state.label)?;
        storage.synchronize_account_delivery_loss(&self.state.label)?;
        drop(self.transport_receipts()?);
        self.observe_recovery_route_policy()?;
        let current = storage.recovery_revision_fence()?;
        let slot = storage.recovery_comparison()?;
        let stable = current.loss_revision == grant.fence.loss_revision
            && current.route_revision == grant.fence.route_revision
            && current.inventory_revision == grant.fence.inventory_revision
            && grant
                .fence
                .obligations
                .iter()
                .all(|selected| current.obligations.contains(selected))
            && slot.pending()
            && Some(slot.revision) == grant.comparison_revision
            && slot.attempt_serial == grant.reservation.attempt_serial
            && slot.frozen_revision == slot.revision
            && self.adapter.account_subscription_attempt().await == Some(attempt);
        if !stable {
            // The network task never wrote to SQLCipher or the delivery queue.
            // A changed grant or activation discards every proposed byte and
            // leaves durable comparison and coverage debt for the next owner.
            return Ok(EpochBackfillRunOutcome::Deferred);
        }
        // Acquisition may have consumed its entire quantum before the worker
        // joins. Give delivery of the already owned batch a separate bounded
        // admission window.
        let admission_deadline = tokio::time::Instant::now() + TRANSPORT_RECONCILIATION_QUANTUM;
        let mut outcomes = Vec::with_capacity(network.routes.len());
        for route in network.routes {
            outcomes.push(
                self.admit_comparison_route(&storage, route, admission_deadline)
                    .await?,
            );
        }
        let mut counts = DrainCounts::default();
        let mut verdict = None;
        let summary = self
            .complete_recovery_grant_inner(
                &grant,
                None,
                &mut counts,
                &mut verdict,
                (outcomes, Vec::new(), Vec::new()),
            )
            .await
            .map_err(|failure| {
                self.pending_failed_sync_summary
                    .merge(failure.partial_summary);
                failure.source
            })?;
        Ok(EpochBackfillRunOutcome::Incomplete(summary))
    }

    async fn admit_comparison_route(
        &self,
        storage: &storage_sqlite::SqliteAccountStorage,
        route: ComparisonRouteResult,
        admission_deadline: tokio::time::Instant,
    ) -> Result<
        (
            TransportReconciliationRoute,
            storage_sqlite::RecoveryComparisonOutcome,
        ),
        AppError,
    > {
        let submission = submit_reconciliation_route(
            &self.adapter,
            route,
            admission_deadline,
            #[cfg(test)]
            None,
        )
        .await;
        self.persist_reconciliation_submission(storage, submission)
    }

    pub(crate) fn persist_reconciliation_submission(
        &self,
        storage: &storage_sqlite::SqliteAccountStorage,
        submission: RouteSubmission,
    ) -> Result<
        (
            TransportReconciliationRoute,
            storage_sqlite::RecoveryComparisonOutcome,
        ),
        AppError,
    > {
        // A failed or timed-out queue step leaves an unqueued suffix whose
        // IDs cannot be mapped back to individual cursor positions.
        if submission.cursor_safe && submission.cursor != submission.initial_cursor {
            storage.advance_transport_reconciliation_replay_cursor(
                &submission.route,
                submission.cursor,
            )?;
        }
        Ok((submission.route, submission.outcome))
    }
}

async fn submit_reconciliation_route(
    adapter: &crate::relay_plane::MarmotRelayPlaneAccountAdapter,
    route: ComparisonRouteResult,
    admission_deadline: tokio::time::Instant,
    #[cfg(test)] witness: Option<&TestComparisonActivityWitness>,
) -> RouteSubmission {
    let mut cursor_safe = true;
    #[cfg(test)]
    let mut attempted = 0usize;
    #[cfg(test)]
    let mut delivered = 0usize;
    let mut fully_submitted = false;
    let mut unavailable = false;
    let mut queued_candidates = Vec::new();
    let outcome = match route.result {
        ComparisonRouteWorkResult::Skipped => {
            storage_sqlite::RecoveryComparisonOutcome::ServicedPartial
        }
        ComparisonRouteWorkResult::TimedOut => {
            storage_sqlite::RecoveryComparisonOutcome::TransientFailure
        }
        ComparisonRouteWorkResult::Returned(Ok(None)) => {
            storage_sqlite::RecoveryComparisonOutcome::Unsupported
        }
        ComparisonRouteWorkResult::Returned(Err(_)) => {
            storage_sqlite::RecoveryComparisonOutcome::TransientFailure
        }
        ComparisonRouteWorkResult::Returned(Ok(Some((summary, events)))) => {
            let mut submitted = true;
            for event in events {
                let candidate_id = route.continuation_evidence.as_ref().and_then(|evidence| {
                    let id = hex::decode(&event.event.id).ok()?;
                    let id = <[u8; 32]>::try_from(id.as_slice()).ok()?;
                    evidence
                        .candidate_ids
                        .contains(&id)
                        .then_some((id, event.event.created_at))
                });
                #[cfg(test)]
                {
                    attempted += 1;
                }
                #[cfg(test)]
                let matches_target = witness.is_some_and(|witness| {
                    witness
                        .target_event_id
                        .lock()
                        .unwrap()
                        .as_ref()
                        .is_some_and(|target| event.event.id.eq_ignore_ascii_case(target))
                });
                let queue = async {
                    #[cfg(test)]
                    if let Ok(Some(action)) = TEST_COMPARISON_QUEUE_ACTIONS
                        .try_with(|actions| actions.borrow_mut().pop_front())
                    {
                        match action {
                            TestComparisonQueueAction::Fail => {
                                return Err(cgka_traits::TransportAdapterError::Subscription(
                                    "injected comparison queue failure".into(),
                                ));
                            }
                            TestComparisonQueueAction::Block => {
                                std::future::pending::<()>().await;
                            }
                        }
                    }
                    adapter.queue_reconciled_event(event).await
                };
                match tokio::time::timeout_at(admission_deadline, queue).await {
                    Ok(Ok(route_count)) => {
                        if route_count > 0
                            && let Some(id) = candidate_id
                        {
                            queued_candidates.push(id);
                        }
                        #[cfg(test)]
                        {
                            delivered += route_count;
                        }
                        #[cfg(test)]
                        if matches_target && let Some(witness) = witness {
                            witness
                                .matching_queued_deliveries
                                .fetch_add(route_count, Ordering::SeqCst);
                        }
                        #[cfg(not(test))]
                        let _ = route_count;
                    }
                    _ => {
                        submitted = false;
                        break;
                    }
                }
            }
            if !submitted || summary.relays_failed > 0 {
                cursor_safe = submitted;
                fully_submitted = submitted;
                unavailable = submitted && summary.neg_timeout_unavailable;
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure
            } else {
                fully_submitted = true;
                storage_sqlite::RecoveryComparisonOutcome::ServicedUnknown
            }
        }
    };
    #[cfg(all(test, feature = "test-policy-overrides"))]
    if let Some(witness) = witness
        && witness.target_route.lock().unwrap().as_ref() == Some(&route.route)
    {
        let mut window = witness.target_decisions.lock().unwrap();
        if let Some(record) = window
            .records
            .iter_mut()
            .find(|record| record.attempt_serial == route.attempt_serial)
        {
            record.full_queue_handoff = Some(fully_submitted);
            record.handed_off_candidates = Some(if fully_submitted {
                route
                    .continuation_evidence
                    .as_ref()
                    .map_or(0, |evidence| evidence.candidate_ids.len())
            } else {
                0
            });
        }
    }
    let unavailable_route = unavailable.then(|| route.route.clone());
    RouteSubmission {
        route: route.route,
        initial_cursor: route.initial_cursor,
        cursor: route.cursor,
        cursor_safe,
        outcome,
        continuation_evidence: fully_submitted
            .then_some(route.continuation_evidence)
            .flatten(),
        queued_candidates,
        unavailable_route,
        #[cfg(test)]
        attempted,
        #[cfg(test)]
        delivered,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{
        ScriptedEosePump, ScriptedPushRelayClient, client_on_app_relay_plane, every_subscription,
        scripted_eose_pump,
    };
    use cgka_traits::GroupStorage;
    use nostr_sdk::prelude::{EventBuilder, FinalizeEvent, Keys, Kind, Tag};
    use std::cell::RefCell;
    use std::future::Future as _;
    use std::sync::Arc;
    use std::task::{Context, Poll, Waker};

    #[cfg(feature = "test-policy-overrides")]
    #[test]
    fn startup_branch_capture_is_target_gated_and_failure_snapshot_is_frozen() {
        let constructed = std::cell::Cell::new(false);
        let disabled = TestComparisonActivityWitness::startup_branch_for_target(None, || {
            constructed.set(true);
            unreachable!("disabled target must not inspect the plan")
        });
        assert!(disabled.is_none());
        assert!(!constructed.get());

        let witness = TestComparisonActivityWitness::default();
        let mut branch =
            TestComparisonActivityWitness::startup_branch_for_target(Some(&witness), || {
                TestStartupBranchWitness {
                    branch: "not_selected",
                    eligibility: "excluded",
                    credit_available: true,
                    attempt_serial: Some(7),
                    plan_items: 2,
                    non_incremental_items: 1,
                }
            })
            .unwrap();
        branch.branch = "inline";
        witness.record_startup_branch(branch);
        witness.active_jobs.store(0, Ordering::SeqCst);
        witness.active_requests.store(1, Ordering::SeqCst);
        witness.attempt_serial.store(7, Ordering::SeqCst);
        let (jobs, requests, attempt, frozen_branch) = witness.startup_failure_snapshot();

        witness.active_jobs.store(1, Ordering::SeqCst);
        witness.record_startup_branch(TestStartupBranchWitness {
            branch: "offload_network_created",
            eligibility: "eligible",
            credit_available: false,
            attempt_serial: Some(8),
            plan_items: 1,
            non_incremental_items: 0,
        });
        let later_sql: Result<u64, ()> = Err(());
        assert!(later_sql.is_err());
        assert_eq!((jobs, requests, attempt), (0, 1, 7));
        let frozen_branch = frozen_branch.unwrap();
        assert_eq!(frozen_branch.branch, "inline");
        assert_eq!(frozen_branch.eligibility, "excluded");
        assert!(frozen_branch.credit_available);
        assert_eq!(frozen_branch.attempt_serial, Some(7));
        assert_eq!(
            (
                frozen_branch.plan_items,
                frozen_branch.non_incremental_items
            ),
            (2, 1)
        );
    }

    fn candidate_for_route(route: [u8; 32]) -> transport_nostr_adapter::NostrRelayEvent {
        let signed = EventBuilder::new(Kind::MlsGroupMessage, "queue boundary")
            .tags([Tag::custom("h", [hex::encode(route)])])
            .finalize(&Keys::generate())
            .unwrap();
        transport_nostr_adapter::NostrRelayEvent {
            endpoint: cgka_traits::TransportEndpoint("wss://relay.example".into()),
            subscription_id: None,
            event: transport_nostr_peeler::NostrTransportEvent::from_nostr_event(&signed).unwrap(),
        }
    }

    fn candidate() -> transport_nostr_adapter::NostrRelayEvent {
        candidate_for_route([7; 32])
    }

    #[test]
    fn clean_suffix_candidates_exclude_frozen_ids_and_are_bounded() {
        let route = TransportReconciliationRoute::Group([7; 32]);
        let events = (0..20).map(|_| candidate()).collect::<Vec<_>>();
        let frozen_id: [u8; 32] = hex::decode(&events[0].event.id)
            .unwrap()
            .try_into()
            .unwrap();
        let frozen = [transport_nostr_adapter::NostrReconciliationItem {
            event_id: frozen_id,
            created_at: 0,
        }];
        let summary = transport_nostr_adapter::NostrReconciliationSummary {
            relays_failed: 1,
            clean_bounded_suffix: true,
            ..Default::default()
        };
        let evidence = positive_continuation_evidence(&route, &frozen, &summary, &events).unwrap();
        assert_eq!(evidence.kind, ContinuationEvidenceKind::CleanSuffix);
        assert_eq!(evidence.candidate_ids.len(), 16);
        assert!(!evidence.candidate_ids.contains(&frozen_id));
        let interrupted = transport_nostr_adapter::NostrReconciliationSummary {
            clean_bounded_suffix: false,
            deadline_interrupted_prefix: true,
            ..summary.clone()
        };
        let evidence =
            positive_continuation_evidence(&route, &frozen, &interrupted, &events).unwrap();
        assert_eq!(
            evidence.kind,
            ContinuationEvidenceKind::DeadlineInterruptedPrefix
        );
        assert_eq!(evidence.candidate_ids.len(), 16);
        assert!(
            positive_continuation_evidence(
                &route,
                &frozen,
                &transport_nostr_adapter::NostrReconciliationSummary {
                    clean_bounded_suffix: false,
                    ..summary
                },
                &events,
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn owned_continuation_evidence_requires_full_queue_handoff() {
        let fixture = fixture().await;
        let group = fixture
            .client
            .app
            .group("alice", &hex::encode(fixture.group_id.as_slice()))
            .unwrap()
            .unwrap();
        let route_id: [u8; 32] = hex::decode(group.nostr_routing.nostr_group_id_hex)
            .unwrap()
            .try_into()
            .unwrap();
        let route = TransportReconciliationRoute::Group(route_id);
        let event = candidate_for_route(route_id);
        for kind in [
            ContinuationEvidenceKind::CleanSuffix,
            ContinuationEvidenceKind::DeadlineInterruptedPrefix,
        ] {
            let summary = transport_nostr_adapter::NostrReconciliationSummary {
                relays_failed: 1,
                clean_bounded_suffix: kind == ContinuationEvidenceKind::CleanSuffix,
                deadline_interrupted_prefix: kind
                    == ContinuationEvidenceKind::DeadlineInterruptedPrefix,
                ..Default::default()
            };
            let make_result = || ComparisonRouteResult {
                #[cfg(feature = "test-policy-overrides")]
                attempt_serial: 0,
                route: route.clone(),
                initial_cursor: None,
                cursor: Some([8; 32]),
                continuation_evidence: positive_continuation_evidence(
                    &route,
                    &[],
                    &summary,
                    std::slice::from_ref(&event),
                ),
                result: ComparisonRouteWorkResult::Returned(Ok(Some((
                    summary.clone(),
                    vec![event.clone()],
                )))),
            };
            #[cfg(feature = "test-policy-overrides")]
            let witness = {
                let witness = TestComparisonActivityWitness::default();
                *witness.target_route.lock().unwrap() = Some(route.clone());
                witness
                    .target_decisions
                    .lock()
                    .unwrap()
                    .record(TestComparisonDecision {
                        attempt_serial: 0,
                        comparison_diagnostics: None,
                        clean_suffix: Some(kind == ContinuationEvidenceKind::CleanSuffix),
                        candidate_count: Some(1),
                        full_queue_handoff: None,
                        handed_off_candidates: None,
                    });
                witness
            };
            let admitted = submit_reconciliation_route(
                &fixture.client.adapter,
                make_result(),
                tokio::time::Instant::now() + Duration::from_secs(5),
                #[cfg(feature = "test-policy-overrides")]
                Some(&witness),
                #[cfg(not(feature = "test-policy-overrides"))]
                None,
            )
            .await;
            assert_eq!(
                admitted
                    .continuation_evidence
                    .as_ref()
                    .map(|evidence| evidence.kind),
                Some(kind)
            );
            assert_eq!(admitted.queued_candidates.len(), 1);
            #[cfg(feature = "test-policy-overrides")]
            {
                let record = witness.target_decisions.lock().unwrap().records[0].clone();
                assert_eq!(record.full_queue_handoff, Some(true));
                assert_eq!(record.handed_off_candidates, Some(1));
                *witness.target_decisions.lock().unwrap() = TestComparisonDecisionWindow::default();
                witness
                    .target_decisions
                    .lock()
                    .unwrap()
                    .record(TestComparisonDecision {
                        full_queue_handoff: None,
                        handed_off_candidates: None,
                        ..record
                    });
            }
            let refused = TEST_COMPARISON_QUEUE_ACTIONS
                .scope(
                    RefCell::new([TestComparisonQueueAction::Fail].into()),
                    submit_reconciliation_route(
                        &fixture.client.adapter,
                        make_result(),
                        tokio::time::Instant::now() + Duration::from_secs(5),
                        #[cfg(feature = "test-policy-overrides")]
                        Some(&witness),
                        #[cfg(not(feature = "test-policy-overrides"))]
                        None,
                    ),
                )
                .await;
            assert!(refused.continuation_evidence.is_none());
            assert!(refused.queued_candidates.is_empty());
            assert!(!refused.cursor_safe);
            #[cfg(feature = "test-policy-overrides")]
            {
                let record = witness.target_decisions.lock().unwrap().records[0].clone();
                assert_eq!(record.full_queue_handoff, Some(false));
                assert_eq!(record.handed_off_candidates, Some(0));
            }
        }
    }

    #[tokio::test]
    async fn online_candidate_fence_keeps_consumption_before_queue_completion() {
        let mut fixture = fixture_with_group_relays_and_comparison(None, false).await;
        arm_test_queue_loss(&mut fixture).await;
        fixture
            .client
            .recovery_owner
            .test_advance_clock(Duration::from_secs(300));
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Receive)
            .unwrap()
            .unwrap();
        let route = grant
            .plan()
            .unwrap()
            .iter()
            .find(|obligation| obligation.cause == storage_sqlite::RecoveryCause::QueueLoss)
            .unwrap()
            .scopes
            .iter()
            .find(|scope| scope.goal.route_kind == 1)
            .map(|scope| {
                TransportReconciliationRoute::Group(scope.goal.transport_group_id.unwrap())
            })
            .unwrap();
        let TransportReconciliationRoute::Group(route_id) = route else {
            unreachable!()
        };
        let event = candidate_for_route(route_id);
        let id: [u8; 32] = hex::decode(&event.event.id).unwrap().try_into().unwrap();
        let unrouted = candidate_for_route([0xee; 32]);
        let unrouted_id: [u8; 32] = hex::decode(&unrouted.event.id).unwrap().try_into().unwrap();
        let returned = vec![event, unrouted];
        let failed_event = candidate_for_route([0xdd; 32]);
        let failed_id: [u8; 32] = hex::decode(&failed_event.event.id)
            .unwrap()
            .try_into()
            .unwrap();
        let summary = transport_nostr_adapter::NostrReconciliationSummary {
            relays_succeeded: 1,
            clean_bounded_suffix: true,
            ..Default::default()
        };
        let network = ComparisonNetworkResult {
            routes: vec![
                ComparisonRouteResult {
                    #[cfg(feature = "test-policy-overrides")]
                    attempt_serial: 0,
                    route: TransportReconciliationRoute::Group(route_id),
                    initial_cursor: None,
                    cursor: None,
                    continuation_evidence: positive_continuation_evidence(
                        &TransportReconciliationRoute::Group(route_id),
                        &[],
                        &summary,
                        &returned,
                    ),
                    result: ComparisonRouteWorkResult::Returned(Ok(Some((summary, returned)))),
                },
                ComparisonRouteResult {
                    #[cfg(feature = "test-policy-overrides")]
                    attempt_serial: 0,
                    route: TransportReconciliationRoute::Inbox,
                    initial_cursor: None,
                    cursor: None,
                    continuation_evidence: positive_continuation_evidence(
                        &TransportReconciliationRoute::Inbox,
                        &[],
                        &transport_nostr_adapter::NostrReconciliationSummary {
                            clean_bounded_suffix: true,
                            ..Default::default()
                        },
                        std::slice::from_ref(&failed_event),
                    ),
                    result: ComparisonRouteWorkResult::Returned(Ok(Some((
                        transport_nostr_adapter::NostrReconciliationSummary {
                            clean_bounded_suffix: true,
                            ..Default::default()
                        },
                        vec![failed_event],
                    )))),
                },
            ],
        };
        let mut recovery = fixture.client.begin_online_epoch_gap(grant).await.unwrap();
        fixture
            .client
            .online_epoch_gap_start_drain(&mut recovery, &network);
        assert!(
            recovery
                .drain
                .as_ref()
                .unwrap()
                .candidate_fence
                .expected
                .contains(&id)
        );
        assert!(
            recovery
                .drain
                .as_ref()
                .unwrap()
                .candidate_fence
                .expected
                .contains(&unrouted_id)
        );
        assert!(
            recovery
                .drain
                .as_ref()
                .unwrap()
                .candidate_fence
                .expected
                .contains(&failed_id)
        );
        let mut routes = network.routes.into_iter();
        let submission = submit_reconciliation_route(
            &fixture.client.adapter,
            routes.next().unwrap(),
            tokio::time::Instant::now() + Duration::from_secs(5),
            None,
        )
        .await;
        assert_eq!(submission.queued_candidates.len(), 1);
        assert_eq!(submission.queued_candidates[0].0, id);
        let failed = TEST_COMPARISON_QUEUE_ACTIONS
            .scope(
                RefCell::new([TestComparisonQueueAction::Fail].into()),
                submit_reconciliation_route(
                    &fixture.client.adapter,
                    routes.next().unwrap(),
                    tokio::time::Instant::now() + Duration::from_secs(5),
                    None,
                ),
            )
            .await;
        assert!(failed.continuation_evidence.is_none());
        assert!(failed.queued_candidates.is_empty());

        // The producer has finished, but the worker has not collected its
        // result: drain with admission_complete=false first.
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                assert!(
                    fixture
                        .client
                        .online_epoch_gap_drain_slice(&mut recovery, false)
                        .await
                        .unwrap()
                        .is_none()
                );
                if recovery
                    .drain
                    .as_ref()
                    .unwrap()
                    .candidate_fence
                    .observed
                    .contains(&id)
                {
                    break;
                }
            }
        })
        .await
        .unwrap();
        fixture
            .client
            .online_epoch_gap_submissions_ready(&mut recovery, &[submission, failed]);
        assert!(recovery.drain.as_ref().unwrap().candidate_fence.complete());
        assert!(
            recovery
                .drain
                .as_ref()
                .unwrap()
                .candidate_fence
                .pending_candidates()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn owned_neg_timeout_is_distinct_from_outer_route_timeout() {
        let fixture = fixture().await;
        let route = TransportReconciliationRoute::Inbox;
        let make_result = |result| ComparisonRouteResult {
            #[cfg(feature = "test-policy-overrides")]
            attempt_serial: 0,
            route: route.clone(),
            initial_cursor: None,
            cursor: None,
            result,
            continuation_evidence: None,
        };
        let unavailable = submit_reconciliation_route(
            &fixture.client.adapter,
            make_result(ComparisonRouteWorkResult::Returned(Ok(Some((
                transport_nostr_adapter::NostrReconciliationSummary {
                    relays_failed: 1,
                    neg_timeout_unavailable: true,
                    ..Default::default()
                },
                Vec::new(),
            ))))),
            tokio::time::Instant::now() + Duration::from_secs(5),
            None,
        )
        .await;
        assert!(unavailable.outcome == storage_sqlite::RecoveryComparisonOutcome::TransientFailure);
        assert_eq!(unavailable.unavailable_route, Some(route.clone()));
        assert!(unavailable.continuation_evidence.is_none());
        let failed_handoff = TEST_COMPARISON_QUEUE_ACTIONS
            .scope(
                RefCell::new([TestComparisonQueueAction::Fail].into()),
                submit_reconciliation_route(
                    &fixture.client.adapter,
                    make_result(ComparisonRouteWorkResult::Returned(Ok(Some((
                        transport_nostr_adapter::NostrReconciliationSummary {
                            relays_failed: 1,
                            neg_timeout_unavailable: true,
                            ..Default::default()
                        },
                        vec![candidate()],
                    ))))),
                    tokio::time::Instant::now() + Duration::from_secs(5),
                    None,
                ),
            )
            .await;
        assert!(failed_handoff.unavailable_route.is_none());
        let outer_timeout = submit_reconciliation_route(
            &fixture.client.adapter,
            make_result(ComparisonRouteWorkResult::TimedOut),
            tokio::time::Instant::now() + Duration::from_secs(5),
            None,
        )
        .await;
        assert!(outer_timeout.unavailable_route.is_none());
    }

    #[cfg(feature = "test-policy-overrides")]
    #[test]
    fn target_decision_window_caps_records_and_keeps_unknown_handoff() {
        let mut window = TestComparisonDecisionWindow::default();
        for serial in 1..=3 {
            window.record(TestComparisonDecision {
                attempt_serial: serial,
                comparison_diagnostics: None,
                clean_suffix: None,
                candidate_count: None,
                full_queue_handoff: None,
                handed_off_candidates: None,
            });
        }
        assert_eq!(
            window
                .records
                .iter()
                .map(|record| record.attempt_serial)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert_eq!(window.dropped, 1);
        assert!(
            window
                .records
                .iter()
                .all(|record| record.comparison_diagnostics.is_none()
                    && record.clean_suffix.is_none()
                    && record.candidate_count.is_none()
                    && record.full_queue_handoff.is_none()
                    && record.handed_off_candidates.is_none())
        );
    }

    #[tokio::test]
    async fn inline_continuation_evidence_requires_full_queue_handoff() {
        for (queue_failure, kind) in [false, true].into_iter().flat_map(|failure| {
            [
                ContinuationEvidenceKind::CleanSuffix,
                ContinuationEvidenceKind::DeadlineInterruptedPrefix,
            ]
            .into_iter()
            .map(move |kind| (failure, kind))
        }) {
            let mut fixture = fixture().await;
            let grant = fixture
                .client
                .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
                .unwrap()
                .unwrap();
            fixture.client.test_comparison_results = Some(
                grant
                    .inventory
                    .iter()
                    .map(|inventory| {
                        let events = match inventory.route {
                            TransportReconciliationRoute::Group(id) => {
                                vec![candidate_for_route(id)]
                            }
                            TransportReconciliationRoute::Inbox => Vec::new(),
                        };
                        Ok(Some((
                            transport_nostr_adapter::NostrReconciliationSummary {
                                relays_failed: usize::from(!events.is_empty()),
                                clean_bounded_suffix: !events.is_empty()
                                    && kind == ContinuationEvidenceKind::CleanSuffix,
                                deadline_interrupted_prefix: !events.is_empty()
                                    && kind == ContinuationEvidenceKind::DeadlineInterruptedPrefix,
                                ..Default::default()
                            },
                            events,
                        )))
                    })
                    .collect(),
            );
            let run = fixture.client.reconcile_transport_history(&grant.inventory);
            let (outcomes, evidence, unavailable) = if queue_failure {
                TEST_COMPARISON_QUEUE_ACTIONS
                    .scope(RefCell::new([TestComparisonQueueAction::Fail].into()), run)
                    .await
                    .unwrap()
            } else {
                run.await.unwrap()
            };
            assert!(outcomes.iter().any(|(_, outcome)| *outcome
                == storage_sqlite::RecoveryComparisonOutcome::TransientFailure));
            assert_eq!(evidence.len(), usize::from(!queue_failure));
            assert!(unavailable.is_empty());
            if let Some(evidence) = evidence.first() {
                assert_eq!(evidence.kind, kind);
            }
        }
    }

    #[tokio::test]
    async fn inline_neg_timeout_propagates_only_a_completed_typed_result() {
        for queue_failure in [false, true] {
            let mut fixture = fixture().await;
            let grant = fixture
                .client
                .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
                .unwrap()
                .unwrap();
            fixture.client.test_comparison_results = Some(
                grant
                    .inventory
                    .iter()
                    .map(|inventory| {
                        // A real timeout marker has no events. The injected
                        // inconsistent result also checks the handoff gate.
                        let events = match (&inventory.route, queue_failure) {
                            (TransportReconciliationRoute::Group(id), true) => {
                                vec![candidate_for_route(*id)]
                            }
                            _ => Vec::new(),
                        };
                        Ok(Some((
                            transport_nostr_adapter::NostrReconciliationSummary {
                                relays_failed: usize::from(matches!(
                                    inventory.route,
                                    TransportReconciliationRoute::Group(_)
                                )),
                                neg_timeout_unavailable: matches!(
                                    inventory.route,
                                    TransportReconciliationRoute::Group(_)
                                ),
                                ..Default::default()
                            },
                            events,
                        )))
                    })
                    .collect(),
            );
            let run = fixture.client.reconcile_transport_history(&grant.inventory);
            let (outcomes, evidence, unavailable) = if queue_failure {
                TEST_COMPARISON_QUEUE_ACTIONS
                    .scope(RefCell::new([TestComparisonQueueAction::Fail].into()), run)
                    .await
                    .unwrap()
            } else {
                run.await.unwrap()
            };
            assert!(evidence.is_empty());
            assert_eq!(unavailable.len(), usize::from(!queue_failure));
            if !queue_failure {
                assert!(matches!(
                    unavailable[0],
                    TransportReconciliationRoute::Group(_)
                ));
                assert!(outcomes.iter().any(|(route, outcome)| {
                    route == &unavailable[0]
                        && *outcome == storage_sqlite::RecoveryComparisonOutcome::TransientFailure
                }));
            }
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_request_keeps_credit_until_network_future_is_dropped() {
        let capacity = Arc::new(tokio::sync::Semaphore::new(1));
        let credit = capacity.clone().try_acquire_owned().unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let waiting = entered.notified();
        tokio::pin!(waiting);
        waiting.as_mut().enable();
        let handle = tokio::spawn({
            let entered = entered.clone();
            async move {
                entered.notify_one();
                std::future::pending::<()>().await;
                (credit, ComparisonNetworkResult { routes: Vec::new() })
            }
        });
        let job = ComparisonNetworkJob { handle };
        waiting.await;
        drop(job);
        // abort() has only requested cancellation. The task still owns the
        // permit until Tokio drops its future on the next scheduler turn.
        assert_eq!(capacity.available_permits(), 0);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while capacity.available_permits() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled task releases its own permit");
        assert_eq!(capacity.available_permits(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn abort_and_wait_reaps_network_future_before_returning() {
        let capacity = Arc::new(tokio::sync::Semaphore::new(1));
        let credit = capacity.clone().try_acquire_owned().unwrap();
        let entered = Arc::new(tokio::sync::Notify::new());
        let waiting = entered.notified();
        tokio::pin!(waiting);
        waiting.as_mut().enable();
        let handle = tokio::spawn({
            let entered = entered.clone();
            async move {
                entered.notify_one();
                std::future::pending::<()>().await;
                (credit, ComparisonNetworkResult { routes: Vec::new() })
            }
        });
        let job = ComparisonNetworkJob { handle };
        waiting.await;
        assert_eq!(capacity.available_permits(), 0);
        job.abort_and_wait().await;
        assert_eq!(capacity.available_permits(), 1);
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        _pump: ScriptedEosePump,
        client: AppClient,
        storage: storage_sqlite::SqliteAccountStorage,
        group_id: GroupId,
    }

    async fn fixture() -> Fixture {
        fixture_with_group_relays(None).await
    }

    async fn fixture_with_group_relays(relays: Option<Vec<String>>) -> Fixture {
        fixture_with_group_relays_and_comparison(relays, true).await
    }

    async fn fixture_with_group_relays_and_comparison(
        relays: Option<Vec<String>>,
        comparison: bool,
    ) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        crate::AccountHome::open(dir.path())
            .create_account("alice")
            .unwrap();
        let relay = Arc::new(ScriptedPushRelayClient::default());
        let app = crate::MarmotApp::with_relay(dir.path(), "wss://relay.example")
            .with_test_relay_client(relay.clone());
        let pump = scripted_eose_pump(app.relay_plane.clone(), relay, every_subscription);
        let mut client = client_on_app_relay_plane(&app, "alice").await;
        let group_id = client
            .create_group_with_options(
                "comparison offload",
                &[],
                crate::AppCreateGroupOptions {
                    relays,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        if comparison {
            client.request_bounded_comparison().unwrap();
        }
        let storage = app.account_storage("alice").unwrap();
        Fixture {
            _dir: dir,
            _pump: pump,
            client,
            storage,
            group_id,
        }
    }

    async fn arm_test_epoch_gap(fixture: &mut Fixture) {
        // Settle the create-group history demand through the original
        // executor, leaving its NeedsDeepRepair debt ineligible as in the
        // online worker fixture. The next selection then belongs to one gap.
        let baseline = fixture
            .client
            .authorize_account_recovery(
                None,
                marmot_forensics::EpochBackfillExecutionSeam::Maintenance,
            )
            .unwrap()
            .unwrap();
        fixture
            .client
            .execute_pending_epoch_backfill_grant(baseline)
            .await
            .unwrap();
        let epoch = fixture
            .client
            .group_mls_state(&fixture.group_id)
            .unwrap()
            .epoch;
        fixture
            .storage
            .arm_epoch_backfill_intents(&[storage_sqlite::StoredEpochBackfillIntent {
                group_id_hex: hex::encode(fixture.group_id.as_slice()),
                stalled_epoch: epoch,
            }])
            .unwrap();
    }

    async fn arm_test_queue_loss(fixture: &mut Fixture) {
        let baseline = fixture
            .client
            .authorize_account_recovery(
                None,
                marmot_forensics::EpochBackfillExecutionSeam::Maintenance,
            )
            .unwrap()
            .unwrap();
        fixture
            .client
            .execute_pending_epoch_backfill_grant(baseline)
            .await
            .unwrap();
        fixture
            .storage
            .mark_account_delivery_recovery("alice", 7, 1)
            .unwrap();
        fixture
            .storage
            .synchronize_account_delivery_loss("alice")
            .unwrap();
        fixture.client.delivery_overflow_recovery_pending = true;
        fixture.client.delivery_overflow_recovery_marker_token = Some(7);
    }

    #[tokio::test]
    async fn queue_loss_zero_credit_preflight_and_same_grant_inline_fallback() {
        let mut eligible = fixture_with_group_relays_and_comparison(None, false).await;
        arm_test_queue_loss(&mut eligible).await;
        let serial = eligible
            .storage
            .recovery_retry_state()
            .unwrap()
            .attempt_serial;
        assert!(
            eligible
                .client
                .queue_loss_only_waiting_for_credit()
                .unwrap()
        );
        assert_eq!(
            eligible
                .storage
                .recovery_retry_state()
                .unwrap()
                .attempt_serial,
            serial
        );
        eligible
            .client
            .recovery_owner
            .test_advance_clock(Duration::from_secs(300));
        let grant = eligible
            .client
            .authorize_account_recovery(None, marmot_forensics::EpochBackfillExecutionSeam::Receive)
            .unwrap()
            .unwrap();
        assert!(
            eligible
                .client
                .online_recovery_offload_eligible(&grant)
                .unwrap()
        );

        let relays = (0..5)
            .map(|index| format!("wss://relay-{index}.example"))
            .collect();
        let mut over_cap = fixture_with_group_relays_and_comparison(Some(relays), false).await;
        arm_test_queue_loss(&mut over_cap).await;
        assert!(
            !over_cap
                .client
                .queue_loss_only_waiting_for_credit()
                .unwrap()
        );
        over_cap
            .client
            .recovery_owner
            .test_advance_clock(Duration::from_secs(300));
        let grant = over_cap
            .client
            .authorize_account_recovery(None, marmot_forensics::EpochBackfillExecutionSeam::Receive)
            .unwrap()
            .unwrap();
        assert!(
            !over_cap
                .client
                .online_recovery_offload_eligible(&grant)
                .unwrap()
        );
        let serial = grant.reservation.attempt_serial;
        over_cap
            .client
            .execute_pending_epoch_backfill_grant(grant)
            .await
            .unwrap();
        assert_eq!(
            over_cap
                .storage
                .recovery_retry_state()
                .unwrap()
                .attempt_serial,
            serial,
            "the ineligible shape executes the selected grant without another reservation"
        );
        assert!(
            over_cap
                .storage
                .account_delivery_recovery("alice")
                .unwrap()
                .is_some(),
            "an unfloored attempt without a coverage certificate retains the marker"
        );
    }

    #[tokio::test]
    async fn epoch_gap_zero_credit_preflight_preserves_inline_fallback() {
        let mut eligible = fixture_with_group_relays_and_comparison(None, false).await;
        arm_test_epoch_gap(&mut eligible).await;
        let serial = eligible
            .storage
            .recovery_retry_state()
            .unwrap()
            .attempt_serial;
        assert!(eligible.client.epoch_gap_only_waiting_for_credit().unwrap());
        assert_eq!(
            eligible
                .storage
                .recovery_retry_state()
                .unwrap()
                .attempt_serial,
            serial
        );
        eligible
            .client
            .recovery_owner
            .test_advance_clock(Duration::from_secs(300));
        let maintenance = eligible
            .client
            .authorize_account_recovery(
                None,
                marmot_forensics::EpochBackfillExecutionSeam::Maintenance,
            )
            .unwrap()
            .unwrap();
        assert!(
            !eligible
                .client
                .online_recovery_offload_eligible(&maintenance)
                .unwrap()
        );

        let relays = (0..5)
            .map(|index| format!("wss://relay-{index}.example"))
            .collect();
        let mut over_cap = fixture_with_group_relays_and_comparison(Some(relays), false).await;
        arm_test_epoch_gap(&mut over_cap).await;
        assert!(!over_cap.client.epoch_gap_only_waiting_for_credit().unwrap());
        over_cap
            .client
            .recovery_owner
            .test_advance_clock(Duration::from_secs(300));
        let grant = over_cap
            .client
            .authorize_account_recovery(None, marmot_forensics::EpochBackfillExecutionSeam::Receive)
            .unwrap()
            .unwrap();
        assert!(
            !over_cap
                .client
                .online_recovery_offload_eligible(&grant)
                .unwrap()
        );
        let serial = grant.reservation.attempt_serial;
        over_cap
            .client
            .execute_pending_epoch_backfill_grant(grant)
            .await
            .unwrap();
        assert_eq!(
            over_cap
                .storage
                .recovery_retry_state()
                .unwrap()
                .attempt_serial,
            serial
        );
    }

    fn network_result(
        route: TransportReconciliationRoute,
        cursor: Option<[u8; 32]>,
    ) -> ComparisonNetworkResult {
        ComparisonNetworkResult {
            routes: vec![ComparisonRouteResult {
                #[cfg(feature = "test-policy-overrides")]
                attempt_serial: 0,
                route,
                initial_cursor: None,
                cursor,
                result: ComparisonRouteWorkResult::Returned(Ok(Some((
                    transport_nostr_adapter::NostrReconciliationSummary {
                        relays_succeeded: 1,
                        ..Default::default()
                    },
                    Vec::new(),
                )))),
                continuation_evidence: None,
            }],
        }
    }

    #[tokio::test]
    async fn comparison_more_than_four_endpoints_keeps_inline_grant() {
        let relays = (0..5)
            .map(|index| format!("wss://relay-{index}.example"))
            .collect();
        let mut fixture = fixture_with_group_relays(Some(relays)).await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        assert!(grant.inventory.iter().any(|route| match &route.work {
            TransportReconciliationWork::Group(group) => group.endpoints.len() == 5,
            TransportReconciliationWork::Inbox(_) => false,
        }));
        assert!(!fixture.client.comparison_offload_eligible(&grant).unwrap());
        let serial = grant.reservation.attempt_serial;
        fixture
            .client
            .execute_pending_epoch_backfill_grant(grant)
            .await
            .unwrap();
        assert_eq!(
            fixture
                .storage
                .recovery_retry_state()
                .unwrap()
                .attempt_serial,
            serial,
            "inline fallback executes the selected grant without a second authorization"
        );
    }

    #[tokio::test]
    async fn comparison_stale_result_preserves_cursor_and_debt() {
        let mut fixture = fixture().await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        assert!(fixture.client.comparison_offload_eligible(&grant).unwrap());
        let route = grant.inventory.first().unwrap().route.clone();
        let attempt = fixture
            .client
            .activate_comparison_grant(&grant, None)
            .await
            .unwrap();
        let activated_subscription = fixture.client.adapter.account_subscription_attempt().await;
        let new_goals = fixture
            .client
            .comparison_route_goals(unix_now_seconds())
            .unwrap();
        fixture
            .storage
            .join_recovery_comparison(
                &[9; 16],
                crate::client::recovery::wall_now_ms().unwrap(),
                &new_goals,
            )
            .unwrap();
        let result = fixture
            .client
            .finish_comparison_grant(grant, attempt, network_result(route.clone(), Some([8; 32])))
            .await
            .unwrap();
        assert!(matches!(result, EpochBackfillRunOutcome::Deferred));
        assert_eq!(
            fixture
                .storage
                .transport_reconciliation_replay_cursor(&route)
                .unwrap(),
            None
        );
        assert!(fixture.storage.recovery_comparison().unwrap().pending());
        fixture
            .client
            .finish_deferred_comparison_sync()
            .await
            .unwrap();
        assert_eq!(
            fixture.client.adapter.account_subscription_attempt().await,
            activated_subscription,
            "a stale startup comparison drains its live activation without rebuilding subscriptions"
        );
    }

    #[tokio::test]
    async fn comparison_inventory_change_rejects_owned_result() {
        let mut fixture = fixture().await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        let route = grant
            .inventory
            .iter()
            .find_map(|item| match item.route {
                TransportReconciliationRoute::Group(id) => Some(id),
                TransportReconciliationRoute::Inbox => None,
            })
            .unwrap();
        let attempt = fixture
            .client
            .activate_comparison_grant(&grant, None)
            .await
            .unwrap();
        let before = fixture
            .storage
            .recovery_revision_fence()
            .unwrap()
            .inventory_revision;
        fixture
            .storage
            .record_transport_reconciliation_item(
                &TransportReconciliationRoute::Group(route),
                &TransportReconciliationItem {
                    event_id: [5; 32],
                    created_at: unix_now_seconds(),
                },
            )
            .unwrap();
        fixture
            .storage
            .delete_transport_group_route(&route)
            .unwrap();
        assert!(
            fixture
                .storage
                .recovery_revision_fence()
                .unwrap()
                .inventory_revision
                > before
        );
        let result = fixture
            .client
            .finish_comparison_grant(
                grant,
                attempt,
                network_result(TransportReconciliationRoute::Inbox, Some([8; 32])),
            )
            .await
            .unwrap();
        assert!(matches!(result, EpochBackfillRunOutcome::Deferred));
        assert_eq!(
            fixture
                .storage
                .transport_reconciliation_replay_cursor(&TransportReconciliationRoute::Inbox)
                .unwrap(),
            None
        );
        assert!(fixture.storage.recovery_comparison().unwrap().pending());
    }

    #[tokio::test]
    async fn comparison_subscription_change_rejects_owned_result() {
        let mut fixture = fixture().await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        let route = grant.inventory.first().unwrap().route.clone();
        let attempt = fixture
            .client
            .activate_comparison_grant(&grant, None)
            .await
            .unwrap();
        fixture.client.adapter.require_fresh_activation().await;
        fixture
            .client
            .runtime
            .activate_transport(None)
            .await
            .unwrap();
        assert_ne!(
            fixture.client.adapter.account_subscription_attempt().await,
            Some(attempt)
        );
        let result = fixture
            .client
            .finish_comparison_grant(grant, attempt, network_result(route.clone(), Some([8; 32])))
            .await
            .unwrap();
        assert!(matches!(result, EpochBackfillRunOutcome::Deferred));
        assert_eq!(
            fixture
                .storage
                .transport_reconciliation_replay_cursor(&route)
                .unwrap(),
            None
        );
        assert!(fixture.storage.recovery_comparison().unwrap().pending());
    }

    #[tokio::test]
    async fn comparison_worker_join_persists_advisory_cursor_before_settlement() {
        let mut fixture = fixture().await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        assert!(fixture.client.comparison_offload_eligible(&grant).unwrap());
        let route = grant.inventory.first().unwrap().route.clone();
        let attempt = fixture
            .client
            .activate_comparison_grant(&grant, None)
            .await
            .unwrap();
        let result = fixture
            .client
            .finish_comparison_grant(grant, attempt, network_result(route.clone(), Some([8; 32])))
            .await
            .unwrap();
        assert!(matches!(result, EpochBackfillRunOutcome::Incomplete(_)));
        assert_eq!(
            fixture
                .storage
                .transport_reconciliation_replay_cursor(&route)
                .unwrap(),
            Some([8; 32])
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn comparison_join_admits_owned_event_after_real_delivery_backpressure() {
        let mut fixture = fixture().await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        let (route, group_route) = grant
            .inventory
            .iter()
            .find_map(|item| match item.route {
                TransportReconciliationRoute::Group(id) => Some((item.route.clone(), id)),
                TransportReconciliationRoute::Inbox => None,
            })
            .expect("fixture has a selected group route");
        let attempt = fixture
            .client
            .activate_comparison_grant(&grant, None)
            .await
            .unwrap();
        let event = candidate_for_route(group_route);
        // Saturate the shared transport queue with another account's route.
        // Its account queue can hold this prefix; Alice's queue stays empty
        // until the comparison result is admitted and owner-drained.
        crate::AccountHome::open(fixture._dir.path())
            .create_account("bob")
            .unwrap();
        let mut bob = client_on_app_relay_plane(&fixture.client.app, "bob").await;
        let bob_group = bob.create_group("backpressure source", &[]).await.unwrap();
        let bob_record = fixture
            .client
            .app
            .group("bob", &hex::encode(bob_group))
            .unwrap()
            .unwrap();
        let bob_route: [u8; 32] = hex::decode(bob_record.nostr_routing.nostr_group_id_hex)
            .unwrap()
            .try_into()
            .unwrap();
        let bob_event = candidate_for_route(bob_route);
        let adapter = bob.adapter.clone();
        let mut context = Context::from_waker(Waker::noop());
        tokio::task::unconstrained(async {
            for _ in 0..1024 {
                assert_eq!(
                    adapter
                        .queue_reconciled_event(bob_event.clone())
                        .await
                        .unwrap(),
                    1
                );
            }
            // Keep Tokio's cooperative yield from running the router during
            // the fill. The next real adapter send must pend on its buffer.
            let mut blocked = Box::pin(adapter.queue_reconciled_event(bob_event.clone()));
            assert!(matches!(blocked.as_mut().poll(&mut context), Poll::Pending));
        })
        .await;

        let mut network = network_result(route.clone(), Some([8; 32]));
        network.routes[0].result = ComparisonRouteWorkResult::Returned(Ok(Some((
            transport_nostr_adapter::NostrReconciliationSummary {
                relays_succeeded: 1,
                ..Default::default()
            },
            vec![event],
        ))));
        let mut finish = Box::pin(
            fixture
                .client
                .finish_comparison_grant(grant, attempt, network),
        );
        assert!(matches!(finish.as_mut().poll(&mut context), Poll::Pending));
        let result = tokio::time::timeout(Duration::from_secs(15), finish)
            .await
            .expect("the owner drains after its real queue send pends")
            .unwrap();
        assert!(matches!(result, EpochBackfillRunOutcome::Incomplete(_)));
        assert_eq!(
            fixture
                .storage
                .transport_reconciliation_replay_cursor(&route)
                .unwrap(),
            Some([8; 32]),
        );
        assert!(fixture.client.adapter.pending_delivery_overflow().is_none());
        assert!(bob.adapter.pending_delivery_overflow().is_none());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                fixture.client.adapter.receive_account_delivery()
            )
            .await
            .is_err(),
            "owner continuation drained Alice's admitted event"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_online_quantum_keeps_receiving_pending_queue_admission() {
        let mut fixture = fixture().await;
        let group = fixture
            .client
            .app
            .group("alice", &hex::encode(fixture.group_id.as_slice()))
            .unwrap()
            .unwrap();
        let route: [u8; 32] = hex::decode(group.nostr_routing.nostr_group_id_hex)
            .unwrap()
            .try_into()
            .unwrap();
        let candidate = candidate_for_route(route);
        tokio::time::timeout(Duration::from_secs(2), async {
            while !fixture
                .client
                .adapter
                .account_subscription_eose()
                .await
                .complete()
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the scripted relay completes EOSE before queue admission");

        // Fill the real shared adapter channel without scheduling its router.
        // The selected account queue is empty; the next send must initially
        // pend behind another account's routed deliveries.
        crate::AccountHome::open(fixture._dir.path())
            .create_account("bob")
            .unwrap();
        let mut bob = client_on_app_relay_plane(&fixture.client.app, "bob").await;
        let bob_group = bob.create_group("backpressure source", &[]).await.unwrap();
        let bob_record = fixture
            .client
            .app
            .group("bob", &hex::encode(bob_group))
            .unwrap()
            .unwrap();
        let bob_route: [u8; 32] = hex::decode(bob_record.nostr_routing.nostr_group_id_hex)
            .unwrap()
            .try_into()
            .unwrap();
        let bob_event = candidate_for_route(bob_route);
        let bob_adapter = bob.adapter.clone();
        let mut context = Context::from_waker(Waker::noop());
        tokio::task::unconstrained(async {
            for _ in 0..1024 {
                assert_eq!(
                    bob_adapter
                        .queue_reconciled_event(bob_event.clone())
                        .await
                        .unwrap(),
                    1
                );
            }
        })
        .await;
        let adapter = fixture.client.adapter.clone();
        let mut queued = Box::pin(adapter.queue_reconciled_event(candidate));
        assert!(matches!(queued.as_mut().poll(&mut context), Poll::Pending));

        let mut state = RecoveryDrainState::new(
            DrainCompletion::EndOfStoredEvents {
                silence_budget: Duration::from_secs(5),
                execution_quantum: Duration::from_secs(5),
            },
            None,
        );
        state.drain_started = Instant::now() - Duration::from_secs(6);
        let mut counts = DrainCounts::default();
        let (queued_result, drain_result) = tokio::join!(
            queued.as_mut(),
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    assert!(
                        fixture
                            .client
                            .drain_sdk_relay_slice(
                                &mut state,
                                &mut counts,
                                Some(ONLINE_EPOCH_GAP_DRAIN_SLICE),
                                false,
                            )
                            .await
                            .unwrap()
                            .is_none(),
                        "pending queue admission cannot end the owner drain"
                    );
                    if counts.deliveries + counts.skipped > 0 {
                        break;
                    }
                }
            })
        );
        drain_result.expect("the owner keeps receiving after its quantum expires");
        assert_eq!(queued_result.unwrap(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn comparison_join_timeout_keeps_cursor_and_retry_debt_after_real_queue_block() {
        let mut fixture = fixture().await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        let (route, group_route) = grant
            .inventory
            .iter()
            .find_map(|item| match item.route {
                TransportReconciliationRoute::Group(id) => Some((item.route.clone(), id)),
                TransportReconciliationRoute::Inbox => None,
            })
            .expect("fixture has a selected group route");
        fixture
            .client
            .activate_comparison_grant(&grant, None)
            .await
            .unwrap();
        let event = candidate_for_route(group_route);
        let adapter = fixture.client.adapter.clone();
        let router_pause = fixture.client.app.relay_plane.pause_router_for_test().await;
        let mut context = Context::from_waker(Waker::noop());
        tokio::task::unconstrained(async {
            for _ in 0..1024 {
                assert_eq!(
                    adapter.queue_reconciled_event(event.clone()).await.unwrap(),
                    1
                );
            }
            let mut blocked = Box::pin(adapter.queue_reconciled_event(event.clone()));
            assert!(matches!(blocked.as_mut().poll(&mut context), Poll::Pending));
        })
        .await;
        let mut network = network_result(route.clone(), Some([8; 32]));
        network.routes[0].result = ComparisonRouteWorkResult::Returned(Ok(Some((
            transport_nostr_adapter::NostrReconciliationSummary {
                relays_succeeded: 1,
                ..Default::default()
            },
            vec![event],
        ))));
        let route_result = network.routes.remove(0);
        let admission = fixture.client.admit_comparison_route(
            &fixture.storage,
            route_result,
            tokio::time::Instant::now() - Duration::from_millis(1),
        );
        // The elapsed admission deadline meets the same genuinely pending
        // production send while the test holds only the router task. Tokio's
        // real timeout fires before that task is restarted.
        let (route_key, outcome) = tokio::time::timeout(Duration::from_secs(1), admission)
            .await
            .expect("elapsed admission timeout fires on the real queue send")
            .unwrap();
        assert!(matches!(
            outcome,
            storage_sqlite::RecoveryComparisonOutcome::TransientFailure
        ));
        assert_eq!(
            fixture
                .storage
                .transport_reconciliation_replay_cursor(&route)
                .unwrap(),
            None
        );
        drop(router_pause);
        let mut counts = DrainCounts::default();
        let mut verdict = None;
        tokio::time::timeout(
            Duration::from_secs(15),
            fixture.client.complete_recovery_grant_inner(
                &grant,
                None,
                &mut counts,
                &mut verdict,
                (vec![(route_key, outcome)], Vec::new(), Vec::new()),
            ),
        )
        .await
        .expect("owner drains and checkpoints after timed-out admission")
        .unwrap();
        let slot = fixture.storage.recovery_comparison().unwrap();
        assert!(slot.pending());
        assert!(!slot.plan.unwrap().retry_routes.is_empty());
        assert!(fixture.client.adapter.pending_delivery_overflow().is_none());
    }

    #[tokio::test]
    async fn comparison_failed_queue_keeps_prepass_cursor_for_unqueued_suffix() {
        let mut fixture = fixture().await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        let route = grant.inventory.first().unwrap().route.clone();
        let attempt = fixture
            .client
            .activate_comparison_grant(&grant, None)
            .await
            .unwrap();
        let mut network = network_result(route.clone(), Some([8; 32]));
        network.routes[0].result = ComparisonRouteWorkResult::Returned(Ok(Some((
            transport_nostr_adapter::NostrReconciliationSummary {
                relays_succeeded: 1,
                ..Default::default()
            },
            vec![candidate()],
        ))));
        let result = TEST_COMPARISON_QUEUE_ACTIONS
            .scope(
                RefCell::new([TestComparisonQueueAction::Fail].into()),
                async {
                    fixture
                        .client
                        .finish_comparison_grant(grant, attempt, network)
                        .await
                },
            )
            .await
            .unwrap();
        assert!(matches!(result, EpochBackfillRunOutcome::Incomplete(_)));
        assert_eq!(
            fixture
                .storage
                .transport_reconciliation_replay_cursor(&route)
                .unwrap(),
            None,
        );
        assert!(fixture.storage.recovery_comparison().unwrap().pending());
    }

    #[tokio::test]
    async fn comparison_partial_pass_rotates_cursor_and_keeps_retry_debt() {
        let mut fixture = fixture().await;
        let grant = fixture
            .client
            .authorize_account_recovery(None, EpochBackfillExecutionSeam::Maintenance)
            .unwrap()
            .unwrap();
        let route = grant.inventory.first().unwrap().route.clone();
        let attempt = fixture
            .client
            .activate_comparison_grant(&grant, None)
            .await
            .unwrap();
        let mut network = network_result(route.clone(), Some([8; 32]));
        network.routes[0].result = ComparisonRouteWorkResult::Returned(Ok(Some((
            transport_nostr_adapter::NostrReconciliationSummary {
                relays_failed: 1,
                ..Default::default()
            },
            Vec::new(),
        ))));
        fixture
            .client
            .finish_comparison_grant(grant, attempt, network)
            .await
            .unwrap();
        assert_eq!(
            fixture
                .storage
                .transport_reconciliation_replay_cursor(&route)
                .unwrap(),
            Some([8; 32])
        );
        let slot = fixture.storage.recovery_comparison().unwrap();
        assert!(slot.pending());
        assert!(!slot.plan.unwrap().retry_routes.is_empty());
    }

    #[tokio::test]
    async fn queue_loss_clean_suffix_requires_post_drain_retention_for_retry() {
        for (retained, end, route_outcome, stale_fence, has_candidate, kind) in [
            (
                false,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure,
                false,
                true,
                ContinuationEvidenceKind::CleanSuffix,
            ),
            (
                true,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure,
                false,
                true,
                ContinuationEvidenceKind::CleanSuffix,
            ),
            (
                true,
                DrainVerdict::NoProgressQuantumYield,
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure,
                false,
                true,
                ContinuationEvidenceKind::CleanSuffix,
            ),
            (
                true,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::Unsupported,
                false,
                true,
                ContinuationEvidenceKind::CleanSuffix,
            ),
            (
                true,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure,
                true,
                true,
                ContinuationEvidenceKind::CleanSuffix,
            ),
            (
                true,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure,
                false,
                false,
                ContinuationEvidenceKind::CleanSuffix,
            ),
            (
                true,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure,
                false,
                true,
                ContinuationEvidenceKind::DeadlineInterruptedPrefix,
            ),
            (
                false,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure,
                false,
                true,
                ContinuationEvidenceKind::DeadlineInterruptedPrefix,
            ),
            (
                true,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::Unsupported,
                false,
                true,
                ContinuationEvidenceKind::DeadlineInterruptedPrefix,
            ),
            (
                true,
                DrainVerdict::Complete,
                storage_sqlite::RecoveryComparisonOutcome::TransientFailure,
                true,
                true,
                ContinuationEvidenceKind::DeadlineInterruptedPrefix,
            ),
        ] {
            let mut fixture = fixture_with_group_relays_and_comparison(None, false).await;
            arm_test_queue_loss(&mut fixture).await;
            #[cfg(feature = "test-policy-overrides")]
            let owner_witness = {
                let witness = super::super::TestRecoveryPhaseWitness::default();
                witness.arm_queue_loss();
                witness.set_target_group_id(fixture.group_id.clone());
                fixture.client.test_recovery_phase_witness = Some(witness.clone());
                witness
            };
            fixture
                .client
                .recovery_owner
                .test_advance_clock(Duration::from_secs(300));
            let mut grant = fixture
                .client
                .authorize_account_recovery(None, EpochBackfillExecutionSeam::Receive)
                .unwrap()
                .unwrap();
            assert!(grant.comparison_revision.is_none());
            let obligation = grant
                .plan()
                .unwrap()
                .iter()
                .find(|obligation| obligation.cause == storage_sqlite::RecoveryCause::QueueLoss)
                .unwrap();
            let scope = obligation
                .scopes
                .iter()
                .find(|scope| scope.goal.route_kind == 1)
                .unwrap();
            #[cfg(feature = "test-policy-overrides")]
            owner_witness.set_target_transport_route(scope.goal.transport_group_id.unwrap());
            let route = TransportReconciliationRoute::Group(scope.goal.transport_group_id.unwrap());
            let id = [0xa5; 32];
            if retained {
                fixture
                    .storage
                    .record_transport_reconciliation_item(
                        &route,
                        &TransportReconciliationItem {
                            event_id: id,
                            created_at: scope.goal.until_seconds,
                        },
                    )
                    .unwrap();
            }
            if stale_fence {
                grant.fence.inventory_revision += 1;
            }
            let mut counts = DrainCounts::default();
            let mut verdict = None;
            fixture
                .client
                .finish_recovery_grant_after_drain(
                    &grant,
                    &mut counts,
                    &mut verdict,
                    (
                        vec![(route.clone(), route_outcome)],
                        vec![PositiveContinuationEvidence {
                            route,
                            candidate_ids: if has_candidate { vec![id] } else { Vec::new() },
                            kind,
                        }],
                        Vec::new(),
                    ),
                    (SyncSummary::default(), end),
                    &[],
                )
                .await
                .unwrap();
            #[cfg(feature = "test-policy-overrides")]
            {
                use super::super::TestQueueLossExclusion as Exclusion;
                let (records, dropped) = owner_witness.queue_loss_decisions();
                assert_eq!(dropped, 0);
                assert_eq!(records.len(), 1);
                let record = records[0];
                assert_eq!(record.attempt_serial, grant.reservation.attempt_serial);
                assert_eq!(record.verdict, end);
                assert_eq!(
                    record.fence_stable,
                    if stale_fence {
                        Some(false)
                    } else if end == DrainVerdict::Complete
                        && route_outcome != storage_sqlite::RecoveryComparisonOutcome::Unsupported
                    {
                        Some(true)
                    } else {
                        None
                    }
                );
                assert_eq!(
                    record.retained_any,
                    if stale_fence
                        || end != DrainVerdict::Complete
                        || route_outcome == storage_sqlite::RecoveryComparisonOutcome::Unsupported
                        || !has_candidate
                    {
                        None
                    } else {
                        Some(retained)
                    }
                );
                assert_eq!(
                    record.first_exclusion,
                    if stale_fence {
                        Some(Exclusion::FenceChanged)
                    } else if end != DrainVerdict::Complete {
                        Some(Exclusion::DrainVerdict)
                    } else if route_outcome
                        == storage_sqlite::RecoveryComparisonOutcome::Unsupported
                    {
                        Some(Exclusion::ComparisonRoute)
                    } else if retained && has_candidate {
                        None
                    } else {
                        Some(Exclusion::NoRetainedCandidate)
                    }
                );
                assert_eq!(
                    record.proposed,
                    if !stale_fence
                        && retained
                        && has_candidate
                        && end == DrainVerdict::Complete
                        && route_outcome != storage_sqlite::RecoveryComparisonOutcome::Unsupported
                    {
                        storage_sqlite::RecoveryEligibility::Retry
                    } else {
                        storage_sqlite::RecoveryEligibility::NeedsDeepRepair
                    }
                );
                assert!(record.checkpoint_result.is_some());
            }
            let demand = fixture
                .storage
                .pending_recovery_demands()
                .unwrap()
                .into_iter()
                .find(|demand| demand.cause == storage_sqlite::RecoveryCause::QueueLoss)
                .unwrap();
            assert_eq!(
                demand.eligibility,
                if stale_fence {
                    storage_sqlite::RecoveryEligibility::Ready
                } else if retained
                    && has_candidate
                    && end == DrainVerdict::Complete
                    && route_outcome != storage_sqlite::RecoveryComparisonOutcome::Unsupported
                {
                    storage_sqlite::RecoveryEligibility::Retry
                } else {
                    storage_sqlite::RecoveryEligibility::NeedsDeepRepair
                }
            );
            if demand.eligibility == storage_sqlite::RecoveryEligibility::Retry {
                let before = fixture.storage.recovery_retry_state().unwrap();
                drop(grant);
                assert!(
                    fixture
                        .client
                        .authorize_account_recovery(None, EpochBackfillExecutionSeam::Receive,)
                        .unwrap()
                        .is_none()
                );
                assert_eq!(fixture.storage.recovery_retry_state().unwrap(), before);
            }
        }
    }

    #[tokio::test]
    async fn queue_loss_owner_keeps_only_unconsumed_local_handoff_retryable() {
        use storage_sqlite::{
            RecoveryComparisonOutcome as Comparison, RecoveryEligibility as Eligibility,
        };

        for (local_pending, in_scope, refused, expected) in [
            (true, true, false, Eligibility::Retry),
            (false, true, false, Eligibility::NeedsDeepRepair),
            (true, false, false, Eligibility::NeedsDeepRepair),
            (true, true, true, Eligibility::WaitingCapacity),
        ] {
            let mut fixture = fixture_with_group_relays_and_comparison(None, false).await;
            arm_test_queue_loss(&mut fixture).await;
            fixture
                .client
                .recovery_owner
                .test_advance_clock(Duration::from_secs(300));
            let grant = fixture
                .client
                .authorize_account_recovery(None, EpochBackfillExecutionSeam::Receive)
                .unwrap()
                .unwrap();
            let (route, created_at) = grant
                .plan()
                .unwrap()
                .iter()
                .find(|obligation| obligation.cause == storage_sqlite::RecoveryCause::QueueLoss)
                .unwrap()
                .scopes
                .iter()
                .find(|scope| scope.goal.route_kind == 1)
                .map(|scope| {
                    (
                        TransportReconciliationRoute::Group(scope.goal.transport_group_id.unwrap()),
                        scope.goal.until_seconds,
                    )
                })
                .unwrap();
            let pending = PendingLocalCandidate {
                route: route.clone(),
                created_at: created_at + u64::from(!in_scope),
            };
            let mut counts = DrainCounts {
                refused: u64::from(refused),
                ..DrainCounts::default()
            };
            let mut drain_verdict = None;
            fixture
                .client
                .finish_recovery_grant_after_drain(
                    &grant,
                    &mut counts,
                    &mut drain_verdict,
                    (
                        vec![(route.clone(), Comparison::TransientFailure)],
                        Vec::new(),
                        Vec::new(),
                    ),
                    (SyncSummary::default(), DrainVerdict::NoProgressQuantumYield),
                    if local_pending {
                        std::slice::from_ref(&pending)
                    } else {
                        &[]
                    },
                )
                .await
                .unwrap();
            let demand = fixture
                .storage
                .pending_recovery_demands()
                .unwrap()
                .into_iter()
                .find(|demand| demand.cause == storage_sqlite::RecoveryCause::QueueLoss)
                .unwrap();
            assert_eq!(demand.eligibility, expected);
        }
    }

    #[cfg(feature = "test-policy-overrides")]
    #[tokio::test]
    async fn queue_loss_typed_neg_timeout_retries_only_selected_unavailable_route() {
        use super::super::TestQueueLossExclusion as Exclusion;
        use storage_sqlite::{
            RecoveryComparisonOutcome as Comparison, RecoveryEligibility as Eligibility,
        };

        for (route_outcome, stale_fence, refused, unrelated, expected, exclusion) in [
            (
                Comparison::TransientFailure,
                false,
                false,
                false,
                Eligibility::Retry,
                None,
            ),
            (
                Comparison::TransientFailure,
                true,
                false,
                false,
                Eligibility::Ready,
                Some(Exclusion::FenceChanged),
            ),
            (
                Comparison::TransientFailure,
                false,
                true,
                false,
                Eligibility::WaitingCapacity,
                Some(Exclusion::Refused),
            ),
            (
                Comparison::Unsupported,
                false,
                false,
                false,
                Eligibility::NeedsDeepRepair,
                Some(Exclusion::ComparisonRoute),
            ),
            (
                Comparison::TransientFailure,
                false,
                false,
                true,
                Eligibility::NeedsDeepRepair,
                Some(Exclusion::ComparisonRoute),
            ),
            (
                Comparison::ServicedUnknown,
                false,
                false,
                false,
                Eligibility::NeedsDeepRepair,
                Some(Exclusion::NoRetainedCandidate),
            ),
        ] {
            let mut fixture = fixture_with_group_relays_and_comparison(None, false).await;
            arm_test_queue_loss(&mut fixture).await;
            let witness = super::super::TestRecoveryPhaseWitness::default();
            witness.arm_queue_loss();
            witness.set_target_group_id(fixture.group_id.clone());
            fixture.client.test_recovery_phase_witness = Some(witness.clone());
            fixture
                .client
                .recovery_owner
                .test_advance_clock(Duration::from_secs(300));
            let mut grant = fixture
                .client
                .authorize_account_recovery(None, EpochBackfillExecutionSeam::Receive)
                .unwrap()
                .unwrap();
            let obligation = grant
                .plan()
                .unwrap()
                .iter()
                .find(|obligation| obligation.cause == storage_sqlite::RecoveryCause::QueueLoss)
                .unwrap();
            let scope = obligation
                .scopes
                .iter()
                .find(|scope| scope.goal.route_kind == 1)
                .unwrap();
            let route = TransportReconciliationRoute::Group(scope.goal.transport_group_id.unwrap());
            witness.set_target_transport_route(scope.goal.transport_group_id.unwrap());
            let obligation_id = obligation.id;
            let reserved_retry = fixture.storage.recovery_retry_state().unwrap();
            if stale_fence {
                grant.fence.inventory_revision += 1;
            }
            let mut counts = DrainCounts::default();
            if refused {
                counts.refused = 1;
            }
            let mut verdict = None;
            fixture
                .client
                .finish_recovery_grant_after_drain(
                    &grant,
                    &mut counts,
                    &mut verdict,
                    (
                        vec![(route.clone(), route_outcome)],
                        Vec::new(),
                        vec![if unrelated {
                            TransportReconciliationRoute::Group([0xee; 32])
                        } else {
                            route.clone()
                        }],
                    ),
                    (SyncSummary::default(), DrainVerdict::Complete),
                    &[],
                )
                .await
                .unwrap();
            let (records, dropped) = witness.queue_loss_decisions();
            assert_eq!(dropped, 0);
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].first_exclusion, exclusion);
            assert_eq!(
                records[0].proposed,
                if stale_fence {
                    Eligibility::NeedsDeepRepair
                } else {
                    expected
                }
            );
            let demand = fixture
                .storage
                .pending_recovery_demands()
                .unwrap()
                .into_iter()
                .find(|demand| demand.cause == storage_sqlite::RecoveryCause::QueueLoss)
                .unwrap();
            assert_eq!(demand.eligibility, expected);
            if expected == Eligibility::Retry {
                assert_eq!(
                    fixture.storage.recovery_retry_state().unwrap().ordinal,
                    reserved_retry.ordinal,
                    "a timeout must not reset the account-wide retry ordinal"
                );
                let scopes = fixture
                    .storage
                    .recovery_scope_snapshots(obligation_id)
                    .unwrap();
                let selected = scopes
                    .iter()
                    .find(|snapshot| snapshot.plan.route_kind == 1)
                    .unwrap();
                assert!(!selected.checkpoints.is_empty());
                assert!(selected.checkpoints.iter().all(|checkpoint| {
                    checkpoint.outcome == storage_sqlite::RecoveryScopeOutcome::Unknown
                        && !checkpoint.exhaustive
                        && !checkpoint.admission_complete
                }));
                let retry = fixture.storage.recovery_retry_state().unwrap();
                drop(grant);
                assert!(
                    fixture
                        .client
                        .authorize_account_recovery(None, EpochBackfillExecutionSeam::Receive,)
                        .unwrap()
                        .is_none()
                );
                assert_eq!(fixture.storage.recovery_retry_state().unwrap(), retry);
                fixture
                    .client
                    .recovery_owner
                    .test_advance_clock(Duration::from_secs(300));
                let due = fixture
                    .client
                    .authorize_account_recovery(None, EpochBackfillExecutionSeam::Receive)
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    fixture.storage.recovery_retry_state().unwrap().ordinal,
                    retry.ordinal + 1
                );
                let mut counts = DrainCounts::default();
                let mut verdict = None;
                fixture
                    .client
                    .finish_recovery_grant_after_drain(
                        &due,
                        &mut counts,
                        &mut verdict,
                        (
                            vec![(route, Comparison::ServicedUnknown)],
                            Vec::new(),
                            Vec::new(),
                        ),
                        (SyncSummary::default(), DrainVerdict::Complete),
                        &[],
                    )
                    .await
                    .unwrap();
                let demand = fixture
                    .storage
                    .pending_recovery_demands()
                    .unwrap()
                    .into_iter()
                    .find(|demand| demand.cause == storage_sqlite::RecoveryCause::QueueLoss)
                    .unwrap();
                assert_eq!(demand.eligibility, Eligibility::NeedsDeepRepair);
            }
        }
    }
}
