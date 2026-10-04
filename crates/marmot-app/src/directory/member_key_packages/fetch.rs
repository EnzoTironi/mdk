//! Bounded per-endpoint acquisition for invitations. Operational relay route limits remain unchanged.
use cgka_traits::TransportEndpoint;
use futures::{StreamExt, stream};
use nostr_sdk::prelude::RelayUrl;
use std::collections::{BTreeMap, BTreeSet};

use crate::MarmotApp;
use crate::relay_plane::{DirectoryEventQuery, DirectoryFetchOutcome};

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
    // Per-endpoint completion lets fast relays release capacity while slow/dead relays
    // remain bounded. Each request still passes through the operational safety chokepoint.
    let mut requests = stream::iter(endpoints.into_iter().take(max_relays).map(|endpoint| {
        let queries = queries.clone();
        async move {
            app.relay_plane
                .fetch_directory_events_with_completion(vec![endpoint], queries)
                .await
        }
    }))
    .buffer_unordered(CONCURRENT_RELAY_REQUESTS);
    loop {
        if tokio::time::Instant::now() >= network_deadline {
            result.complete = false;
            break;
        }
        let acquired = tokio::time::timeout_at(network_deadline, requests.next()).await;
        match acquired {
            Ok(Some(Ok(outcome))) => {
                // EOSE with a saturated bounded query cannot establish absence or a complete version inventory.
                let saturated = queries
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
                result.complete &= outcome.complete && !saturated;
                result.records.extend(outcome.records);
            }
            Ok(Some(Err(_))) => result.complete = false,
            Ok(None) => break,
            Err(_) => {
                result.complete = false;
                break;
            }
        }
    }
    result
}
