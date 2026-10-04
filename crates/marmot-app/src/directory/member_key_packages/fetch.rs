//! Bounded per-endpoint acquisition for invitations. Operational relay route limits remain unchanged.
use cgka_traits::TransportEndpoint;
use futures::{StreamExt, stream};
use nostr_sdk::prelude::RelayUrl;
use std::collections::{BTreeMap, BTreeSet};

use crate::MarmotApp;
use crate::relay_plane::{DirectoryEventQuery, DirectoryFetchOutcome};
use transport_nostr_adapter::KIND_MARMOT_KEY_PACKAGE;

// Search beyond one operational route without unbounded work from an untrusted relay list.
pub(super) const MAX_DISCOVERY_RELAYS: usize = 64;
const CONCURRENT_RELAY_REQUESTS: usize = 16;

#[derive(Default)]
pub(super) struct MemberDirectoryOutcome {
    pub(super) records: Vec<crate::relay_plane::DirectoryRelayEventRecord>,
    pub(super) complete: bool,
}

impl MemberDirectoryOutcome {
    pub(super) fn directory_outcome(self) -> DirectoryFetchOutcome {
        DirectoryFetchOutcome {
            records: self.records,
            complete: self.complete,
        }
    }
}

// RelayUrl equality handles equivalent trailing slash, host case and default-port forms.
// Keep the first safe endpoint spelling without rewriting the signed relay-list projection.
pub(super) fn unique_safe_endpoints(
    app: &MarmotApp,
    endpoints: Vec<TransportEndpoint>,
    context: &str,
) -> Vec<TransportEndpoint> {
    let mut seen = BTreeSet::new();
    app.retain_safe_discovered_endpoints(endpoints, context)
        .into_iter()
        .filter(|endpoint| RelayUrl::parse(endpoint.as_str()).is_ok_and(|url| seen.insert(url)))
        .collect()
}

pub(super) async fn fetch_member_directory(
    app: &MarmotApp,
    mut endpoints: Vec<TransportEndpoint>,
    queries: Vec<DirectoryEventQuery>,
    network_deadline: tokio::time::Instant,
    max_relays: usize,
) -> MemberDirectoryOutcome {
    endpoints = unique_safe_endpoints(app, endpoints, "member directory acquisition");
    let budget_exceeded = endpoints.len() > max_relays;
    let mut result = MemberDirectoryOutcome {
        complete: !endpoints.is_empty() && !budget_exceeded,
        ..Default::default()
    };
    // Optional diagnostics use separate REQs: a relay rejecting kind443 must
    // neither hide a current package nor invalidate a completed current search.
    let (required_queries, evidence_queries): (Vec<_>, Vec<_>) = queries
        .iter()
        .cloned()
        .partition(|query| !query.evidence_only);
    result.complete &= !required_queries.is_empty();
    let query_groups = [(required_queries, true), (evidence_queries, false)]
        .into_iter()
        .filter(|(queries, _)| !queries.is_empty())
        .collect::<Vec<_>>();
    for (queries, required) in query_groups {
        // Current publications already supply either a usable key or a typed
        // validation verdict. Avoid optional probes delaying those results.
        if !required
            && result
                .records
                .iter()
                .any(|record| record.event.kind == KIND_MARMOT_KEY_PACKAGE)
        {
            break;
        }
        let mut requests =
            stream::iter(endpoints.iter().take(max_relays).cloned().map(|endpoint| {
                let queries = queries.clone();
                async move {
                    // Retain completed receivers without starting queued work after cutoff.
                    if tokio::time::Instant::now() >= network_deadline {
                        return None;
                    }
                    Some(
                        tokio::time::timeout_at(network_deadline, async move {
                            app.relay_plane
                                .fetch_directory_events_with_completion(vec![endpoint], queries)
                                .await
                        })
                        .await,
                    )
                }
            }))
            .buffer_unordered(CONCURRENT_RELAY_REQUESTS);
        loop {
            match requests.next().await {
                Some(Some(Ok(Ok(outcome)))) => {
                    let saturated =
                        queries
                            .iter()
                            .filter(|query| !query.evidence_only)
                            .any(|query| {
                                let mut by_endpoint = BTreeMap::<_, BTreeSet<_>>::new();
                                for record in outcome
                                    .records
                                    .iter()
                                    .filter(|record| record.event.kind == query.kind)
                                {
                                    for endpoint in &record.endpoints {
                                        by_endpoint
                                            .entry(endpoint)
                                            .or_default()
                                            .insert(&record.event.id);
                                    }
                                }
                                by_endpoint.values().any(|ids| ids.len() >= query.limit)
                            });
                    if required {
                        result.complete &= outcome.complete && !saturated;
                    }
                    result.records.extend(outcome.records);
                }
                Some(_) if required => result.complete = false,
                Some(_) => {}
                None => break,
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use transport_nostr_adapter::KIND_MARMOT_KEY_PACKAGE;

    #[tokio::test]
    async fn member_directory_keeps_all_ready_results_at_the_deadline() {
        let (_directory, app, accounts, _fetcher) =
            crate::tests::member_resolution_fixture(1, false).await;
        tokio::time::pause();
        for count in [3, CONCURRENT_RELAY_REQUESTS + 1] {
            let before = app
                .relay_plane
                .relay_health()
                .await
                .directory_completed_fetches;
            let expected = count.min(CONCURRENT_RELAY_REQUESTS);
            let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
            let request = fetch_member_directory(
                &app,
                (0..count)
                    .map(|index| TransportEndpoint(format!("wss://relay-{index}.example")))
                    .collect(),
                vec![DirectoryEventQuery {
                    kind: KIND_MARMOT_KEY_PACKAGE,
                    authors: vec![accounts[0].account_id_hex.clone()],
                    limit: 12,
                    evidence_only: false,
                }],
                deadline,
                MAX_DISCOVERY_RELAYS,
            );
            futures::pin_mut!(request);
            assert!(futures::poll!(request.as_mut()).is_pending());
            // Let every started directory owner deliver its result, then
            // simulate a delayed consumer poll without a relay-timer race.
            for _ in 0..100 {
                if app
                    .relay_plane
                    .relay_health()
                    .await
                    .directory_completed_fetches
                    == before + expected
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            assert_eq!(
                app.relay_plane
                    .relay_health()
                    .await
                    .directory_completed_fetches,
                before + expected
            );
            tokio::time::advance(Duration::from_secs(1)).await;
            let outcome = request.await;

            assert_eq!(outcome.complete, count == expected);
            assert_eq!(outcome.records.len(), expected);
            assert_eq!(
                app.relay_plane
                    .relay_health()
                    .await
                    .directory_inflight_fetches,
                0
            );
            assert!(tokio::time::Instant::now() >= deadline);
            assert!(tokio::time::Instant::now() <= deadline + Duration::from_millis(10));
        }
    }

    #[tokio::test]
    async fn member_directory_stalled_requests_end_at_the_same_absolute_deadline() {
        let (_directory, app, accounts, fetcher) =
            crate::tests::member_resolution_fixture(1, false).await;
        let (_entered, _release) = fetcher.hold_fetches();
        tokio::time::pause();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(1);
        let outcome = fetch_member_directory(
            &app,
            vec![TransportEndpoint("wss://first.example".into())],
            vec![DirectoryEventQuery {
                kind: KIND_MARMOT_KEY_PACKAGE,
                authors: vec![accounts[0].account_id_hex.clone()],
                limit: 12,
                evidence_only: false,
            }],
            deadline,
            MAX_DISCOVERY_RELAYS,
        )
        .await;

        assert!(!outcome.complete);
        assert!(outcome.records.is_empty());
        assert!(tokio::time::Instant::now() >= deadline);
        assert!(tokio::time::Instant::now() <= deadline + Duration::from_millis(10));
    }
}

#[cfg(test)]
mod metadata_failure_tests {
    #[test]
    fn member_directory_invalid_metadata_keeps_a_typed_recipient_failure() {
        let (result, outcome) = super::super::validate_current_member_key_package(
            "synthetic-member",
            crate::KeyPackage::new(vec![0]),
        );
        assert_eq!(outcome, "invalid");
        assert!(
            matches!(result, Err(crate::AppError::MemberInvalidKeyPackage(ref account)) if account == "synthetic-member")
        );
    }
}
