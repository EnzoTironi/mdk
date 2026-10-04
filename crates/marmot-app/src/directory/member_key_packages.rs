//! Set-oriented group-member KeyPackage resolution.
//!
//! The create and invite paths know the whole requested roster before they
//! mutate MLS state. Resolve that set as a set: canonicalize aliases, collapse
//! duplicate account ids, reuse discovery routes, and fetch current KeyPackages
//! from relays before every invitation. Cached packages are discovery hints, not
//! evidence that a recipient still owns the private bundle. Unsupported batch
//! queries fall back to bounded single-author relay requests.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cgka_engine::key_package::key_package_metadata;
use cgka_traits::TransportEndpoint;
use cgka_traits::engine::KeyPackage;
use cgka_traits::group::ProtocolProfile;
use futures::{StreamExt, stream};
use nostr_sdk::prelude::PublicKey;
use transport_nostr_adapter::{
    KIND_MARMOT_INBOX_RELAY_LIST, KIND_MARMOT_KEY_PACKAGE, KIND_NIP65_RELAY_LIST,
};

mod fetch;

fn member_relay_list_failure(app: &MarmotApp, target: &MemberTarget, error: AppError) -> AppError {
    match error {
        AppError::RelayDirectory(_) => {
            let safe = unique_safe_endpoints(
                app,
                target
                    .relay_lists
                    .nip65
                    .relays
                    .iter()
                    .cloned()
                    .map(TransportEndpoint)
                    .collect(),
                "member discovery diagnosis",
            );
            if safe.is_empty()
                && app
                    .retain_safe_discovered_endpoints(
                        app.directory_source_relays(&[]),
                        "member configured discovery diagnosis",
                    )
                    .is_empty()
            {
                AppError::MemberNoUsableDiscoveryRelays(target.account_id_hex.clone())
            } else if safe.len() > fetch::MAX_DISCOVERY_RELAYS {
                AppError::MemberRelayBudgetExceeded(target.account_id_hex.clone())
            } else {
                AppError::MemberDiscoveryIncomplete(target.account_id_hex.clone())
            }
        }
        error => error,
    }
}

fn records_have_future_packages(
    account: &str,
    records: &[crate::relay_plane::DirectoryRelayEventRecord],
    freshness: crate::DirectoryFreshness,
) -> bool {
    records.iter().any(|record| {
        record.event.pubkey == account
            && record.event.kind == KIND_MARMOT_KEY_PACKAGE
            && !freshness.accepts(record)
    })
}

use fetch::{fetch_member_directory, unique_safe_endpoints};

use crate::key_package_records::{
    fresh_relay_list_status_from_records, merge_relay_list_status, obsolete_key_package_observed,
    preferred_member_key_package_from_records as preferred_fresh_key_package_from_records,
};
use crate::relay_plane::{DirectoryEventQuery, DirectoryFetchOutcome};
use crate::{AccountRelayListStatus, AppError, FetchedKeyPackage, MarmotApp};

/// Maximum authors placed in one Nostr directory filter.
pub(crate) const MEMBER_RESOLUTION_AUTHORS_PER_QUERY: usize = 32;
/// Concurrent endpoint-group/chunk requests within one resolution pass.
const MEMBER_RESOLUTION_RELAY_CONCURRENCY: usize = 4;
/// Existing per-member fallback concurrency, retained for incompatible relays.
const MEMBER_RESOLUTION_FALLBACK_CONCURRENCY: usize = 8;
/// One complete set resolution is bounded even when several relay sets stall.
// Three network stages, each with a bounded single-author retry, can each
// consume a 5s connection budget plus a 3s fetch budget. Keep enough time for
// those stages and local validation while retaining one overall deadline.
const MEMBER_RESOLUTION_DEADLINE: Duration = Duration::from_secs(50);
const KEY_PACKAGE_EVENTS_PER_AUTHOR: usize = 12;
const RELAY_LIST_EVENTS_PER_AUTHOR: usize = 4;
const MEMBER_PREWARM_CACHE_LIMIT: usize = 256;
const MEMBER_PREWARM_CACHE_TTL: Duration = Duration::from_secs(5 * 60);

struct MemberResolutionContext<'a> {
    purpose: MemberResolutionPurpose,
    requirements: Option<&'a cgka_engine::key_package::KeyPackageRequirements>,
    network_deadline: tokio::time::Instant,
    relay_list_completed: HashSet<usize>,
    observed_packages: Vec<BTreeMap<String, crate::relay_plane::DirectoryRelayEventRecord>>,
}

#[derive(Clone)]
struct MemberKeyPackagePrewarmEntry {
    relay_lists: AccountRelayListStatus,
    inserted_at: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn composition_prewarm_cache_is_bounded_and_expires_entries() {
        let mut cache = MemberKeyPackagePrewarmCache::default();
        for index in 0..=MEMBER_PREWARM_CACHE_LIMIT {
            cache.insert(format!("{index:064x}"), AccountRelayListStatus::empty());
        }
        assert_eq!(cache.entries.len(), MEMBER_PREWARM_CACHE_LIMIT);
        assert!(cache.get(&format!("{:064x}", 0)).is_none());

        let newest = format!("{:064x}", MEMBER_PREWARM_CACHE_LIMIT);
        cache.entries.get_mut(&newest).unwrap().inserted_at =
            Instant::now() - MEMBER_PREWARM_CACHE_TTL - Duration::from_secs(1);
        assert!(cache.get(&newest).is_none());
        assert!(!cache.order.contains(&newest));
    }

    #[tokio::test]
    async fn composition_prewarm_route_reuse_preserves_original_freshness_deadline() {
        let (_directory, app, accounts, _fetcher) =
            crate::tests::member_resolution_fixture(1, false).await;
        let id = &accounts[0].account_id_hex;
        app.prewarm_group_member_key_packages(&[id.as_str()])
            .await
            .unwrap();
        let original = Instant::now() - Duration::from_secs(240);
        app.member_key_package_prewarm_cache
            .lock()
            .unwrap()
            .entries
            .get_mut(id)
            .unwrap()
            .inserted_at = original;
        for _ in 0..3 {
            app.prewarm_group_member_key_packages(&[id.as_str()])
                .await
                .unwrap();
            app.resolve_member_key_packages(&[id.as_str()])
                .await
                .unwrap();
        }
        let mut cache = app.member_key_package_prewarm_cache.lock().unwrap();
        assert_eq!(cache.entries.get(id).unwrap().inserted_at, original);
        cache.entries.get_mut(id).unwrap().inserted_at =
            Instant::now() - MEMBER_PREWARM_CACHE_TTL - Duration::from_secs(1);
        assert!(cache.get(id).is_none());
    }
}

#[derive(Default)]
pub(crate) struct MemberKeyPackagePrewarmCache {
    entries: HashMap<String, MemberKeyPackagePrewarmEntry>,
    order: VecDeque<String>,
}

impl MemberKeyPackagePrewarmCache {
    fn get(&mut self, account_id_hex: &str) -> Option<AccountRelayListStatus> {
        self.remove_expired();
        self.entries
            .get(account_id_hex)
            .map(|entry| entry.relay_lists.clone())
    }

    /// Insert freshly discovered routes, never a package-refresh result.
    fn insert(&mut self, account_id_hex: String, relay_lists: AccountRelayListStatus) {
        self.remove_expired();
        self.order.retain(|existing| existing != &account_id_hex);
        self.order.push_back(account_id_hex.clone());
        self.entries.insert(
            account_id_hex,
            MemberKeyPackagePrewarmEntry {
                relay_lists,
                inserted_at: Instant::now(),
            },
        );
        while self.entries.len() > MEMBER_PREWARM_CACHE_LIMIT {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }

    fn remove_expired(&mut self) {
        let now = Instant::now();
        self.entries.retain(|_, entry| {
            now.saturating_duration_since(entry.inserted_at) <= MEMBER_PREWARM_CACHE_TTL
        });
        self.order
            .retain(|account_id| self.entries.contains_key(account_id));
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MemberResolutionPurpose {
    Commit,
    CommitFresh,
    Prewarm,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MemberKeyPackagePrewarmSummary {
    /// Number of member references supplied by the host.
    pub requested_members: u64,
    /// Canonical account ids after aliases and duplicates are collapsed.
    pub unique_members: u64,
    /// Retained for API compatibility; always zero because packages must be
    /// fetched from relays. Discovery routes may still be reused.
    pub reused_members: u64,
    /// Packages that required relay resolution during this call.
    pub network_resolved_members: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MemberKeyPackageResolutionStats {
    pub(crate) requested_members: usize,
    pub(crate) unique_members: usize,
}

impl From<MemberKeyPackageResolutionStats> for MemberKeyPackagePrewarmSummary {
    fn from(stats: MemberKeyPackageResolutionStats) -> Self {
        Self {
            requested_members: stats.requested_members as u64,
            unique_members: stats.unique_members as u64,
            reused_members: 0,
            network_resolved_members: stats.unique_members as u64,
        }
    }
}

#[derive(Clone)]
struct MemberTarget {
    account_id_hex: String,
    relay_lists: AccountRelayListStatus,
    cached_relay_lists: AccountRelayListStatus,
    relay_records: Vec<crate::relay_plane::DirectoryRelayEventRecord>,
}

#[derive(Default)]
struct RelayListResolution {
    completed: HashSet<usize>,
    errors: Vec<(usize, AppError)>,
}

#[allow(dead_code)]
fn member_resolution_future_is_send(app: &MarmotApp) {
    fn assert_send<T: Send>(_: T) {}
    assert_send(app.resolve_member_key_packages(&[]));
}

pub(crate) struct ResolvedMemberKeyPackages {
    pub(crate) key_packages: Vec<KeyPackage>,
    pub(crate) stats: MemberKeyPackageResolutionStats,
}

impl MarmotApp {
    /// Resolve a roster as one deterministic set.
    ///
    /// Account aliases are canonicalized and duplicate account ids collapse to
    /// their first input position. Every package is fetched from relays; cached
    /// packages are never a fallback if that lookup fails. On error, the error
    /// belonging to the first unresolved canonical member is returned even if
    /// later relay work completed earlier.
    pub async fn resolve_member_key_packages(
        &self,
        member_refs: &[&str],
    ) -> Result<Vec<KeyPackage>, AppError> {
        let member_refs = member_refs
            .iter()
            .map(|member_ref| (*member_ref).to_owned())
            .collect::<Vec<_>>();
        Ok(self
            .resolve_member_key_packages_with_stats(member_refs)
            .await?
            .key_packages)
    }

    pub(crate) async fn resolve_compatible_member_key_packages(
        &self,
        member_refs: Vec<String>,
        requirements: &cgka_engine::key_package::KeyPackageRequirements,
        purpose: MemberResolutionPurpose,
    ) -> Result<ResolvedMemberKeyPackages, AppError> {
        self.resolve_member_key_packages_for_purpose(member_refs, purpose, Some(requirements))
            .await
    }

    /// Prewarm group composition without reserving or consuming any package.
    /// Success reports relay/metadata readiness only; final Create/Invite
    /// re-fetches packages and enforces the engine's membership policy.
    ///
    /// The roster must also resolve a safe Marmot inbox route for every member;
    /// missing routes return [`AppError::MissingMemberInboxRoute`]. Successfully
    /// discovered routes remain cached even when another member fails readiness.
    /// A malformed current slot publication suppresses older packages here too;
    /// return an error rather than report readiness from superseded material.
    /// Every call fetches packages for a fresh readiness signal; hosts should
    /// debounce composition changes. A later create call can reuse discovery routes,
    /// but fetches KeyPackages again because prewarmed material may have been
    /// consumed in the meantime.
    pub async fn prewarm_group_member_key_packages(
        &self,
        member_refs: &[&str],
    ) -> Result<MemberKeyPackagePrewarmSummary, AppError> {
        let member_refs = member_refs
            .iter()
            .map(|member_ref| (*member_ref).to_owned())
            .collect::<Vec<_>>();
        self.resolve_member_key_packages_for_purpose(
            member_refs,
            MemberResolutionPurpose::Prewarm,
            None,
        )
        .await
        .map(|resolved| resolved.stats.into())
    }

    pub(crate) async fn resolve_member_key_packages_with_stats(
        &self,
        member_refs: Vec<String>,
    ) -> Result<ResolvedMemberKeyPackages, AppError> {
        self.resolve_member_key_packages_for_purpose(
            member_refs,
            MemberResolutionPurpose::Commit,
            None,
        )
        .await
    }

    async fn resolve_member_key_packages_for_purpose(
        &self,
        member_refs: Vec<String>,
        purpose: MemberResolutionPurpose,
        requirements: Option<&cgka_engine::key_package::KeyPackageRequirements>,
    ) -> Result<ResolvedMemberKeyPackages, AppError> {
        let observation = self.product_analytics.begin(
            crate::ProductFamily::KeyPackage,
            "lookup",
            crate::ProductUnit::Action,
        );
        // Reserve local validation/classification time after bounded network work.
        let network_deadline =
            tokio::time::Instant::now() + MEMBER_RESOLUTION_DEADLINE - Duration::from_secs(2);
        let result = match tokio::time::timeout(
            MEMBER_RESOLUTION_DEADLINE,
            self.resolve_member_key_packages_inner(
                &member_refs,
                purpose,
                requirements,
                network_deadline,
            ),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(AppError::MemberDiscoveryTimeout),
        };
        if let Some(observation) = observation {
            observation.finish(match &result {
                Ok(_) => "usable",
                Err(AppError::MissingKeyPackage(_)) => "absent",
                Err(
                    AppError::InvalidKeyPackageEvent(_)
                    | AppError::MemberInvalidKeyPackage(_)
                    | AppError::MemberInvalidKeyPackageLifetime(_)
                    | AppError::MemberIncompatibleKeyPackage(_)
                    | AppError::ObsoleteKeyPackage(_),
                ) => "invalid",
                Err(_) => "unavailable",
            });
        }
        result
    }

    async fn resolve_member_key_packages_inner(
        &self,
        member_refs: &[String],
        purpose: MemberResolutionPurpose,
        requirements: Option<&cgka_engine::key_package::KeyPackageRequirements>,
        network_deadline: tokio::time::Instant,
    ) -> Result<ResolvedMemberKeyPackages, AppError> {
        let directory_observation = self
            .product_analytics
            .begin(
                crate::ProductFamily::Directory,
                "key_package",
                crate::ProductUnit::Attempt,
            )
            .map(crate::ProductObservation::counts_only);
        let mut seen = HashSet::new();
        let mut targets = Vec::new();
        for member_ref in member_refs {
            let local = self.account_home().account(member_ref).ok();
            let account_id_hex = match &local {
                Some(account) => account.account_id_hex.clone(),
                None => PublicKey::parse(member_ref)
                    .map_err(|_| AppError::InvalidPublicKey)?
                    .to_hex(),
            };
            if !seen.insert(account_id_hex.clone()) {
                continue;
            }
            let relay_lists = self
                .directory_entry_for_account_id(&account_id_hex)?
                .map(|entry| entry.relay_lists)
                .unwrap_or_else(AccountRelayListStatus::empty);
            targets.push(MemberTarget {
                account_id_hex,
                relay_lists: relay_lists.clone(),
                cached_relay_lists: relay_lists,
                relay_records: Vec::new(),
            });
        }

        let mut outcomes = (0..targets.len())
            .map(|_| None)
            .collect::<Vec<Option<Result<KeyPackage, AppError>>>>();
        // Even a valid, unexpired cached package may have been consumed on
        // another device. Every purpose, including composition prewarm, must
        // fetch current relay publications; only discovery routes are reused.
        let mut reused_prewarmed_routes = HashSet::new();
        for (index, target) in targets.iter_mut().enumerate() {
            // Recovery deliberately refreshes routes as well as packages.
            if purpose == MemberResolutionPurpose::CommitFresh {
                continue;
            }
            let prefetched = self
                .member_key_package_prewarm_cache
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&target.account_id_hex);
            if let Some(relay_lists) = prefetched {
                target.relay_lists =
                    merge_relay_list_status(target.relay_lists.clone(), relay_lists);
                target.cached_relay_lists = target.relay_lists.clone();
                if !target.relay_lists.nip65.relays.is_empty()
                    && !self
                        .retain_safe_discovered_endpoints(
                            target
                                .relay_lists
                                .inbox
                                .relays
                                .iter()
                                .cloned()
                                .map(TransportEndpoint)
                                .collect(),
                            "member prewarm inbox readiness",
                        )
                        .is_empty()
                {
                    reused_prewarmed_routes.insert(index);
                }
            }
        }

        // Durable projections have unknown observation age and need a bounded
        // refresh. Process-local prewarm entries have an enforced TTL and were
        // already resolved through the outbox path during composition.
        let relay_list_unresolved = (0..targets.len())
            .filter(|index| !reused_prewarmed_routes.contains(index))
            .collect::<Vec<_>>();
        let relay_list_resolution = self
            .resolve_missing_relay_lists(&mut targets, &relay_list_unresolved, network_deadline)
            .await;
        for (index, error) in relay_list_resolution.errors {
            outcomes[index] = Some(Err(member_relay_list_failure(self, &targets[index], error)));
        }
        let key_package_unresolved = (0..targets.len())
            .filter(|index| outcomes[*index].is_none())
            .collect::<Vec<_>>();
        let mut discovery = MemberResolutionContext {
            purpose,
            requirements,
            network_deadline,
            relay_list_completed: relay_list_resolution.completed,
            observed_packages: (0..targets.len()).map(|_| BTreeMap::new()).collect(),
        };
        self.resolve_missing_key_packages(
            &targets,
            &key_package_unresolved,
            &mut outcomes,
            &mut discovery,
        )
        .await;
        let stale_hints = key_package_unresolved
            .iter()
            .copied()
            .filter(|index| {
                reused_prewarmed_routes.contains(index) && matches!(outcomes[*index], Some(Err(_)))
            })
            .collect::<Vec<_>>();
        if !stale_hints.is_empty() {
            {
                let mut cache = self
                    .member_key_package_prewarm_cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for index in &stale_hints {
                    let account = &targets[*index].account_id_hex;
                    cache.entries.remove(account);
                    cache.order.retain(|id| id != account);
                }
            }
            let refreshed = self
                .resolve_missing_relay_lists(&mut targets, &stale_hints, network_deadline)
                .await;
            for index in &stale_hints {
                outcomes[*index] = None;
                discovery.relay_list_completed.remove(index);
            }
            discovery.relay_list_completed.extend(refreshed.completed);
            for (index, error) in refreshed.errors {
                outcomes[index] =
                    Some(Err(member_relay_list_failure(self, &targets[index], error)));
            }
            let retry = stale_hints
                .into_iter()
                .filter(|index| outcomes[*index].is_none())
                .collect::<Vec<_>>();
            self.resolve_missing_key_packages(&targets, &retry, &mut outcomes, &mut discovery)
                .await;
        }

        if let Some(observation) = directory_observation {
            for outcome in &outcomes {
                let outcome = match outcome {
                    Some(Ok(_)) => "success",
                    Some(Err(AppError::MissingKeyPackage(_))) | None => "empty",
                    Some(Err(_)) => "failure",
                };
                observation.directory_sample(outcome, "network", 1);
            }
        }
        match purpose {
            MemberResolutionPurpose::Commit | MemberResolutionPurpose::CommitFresh => {
                for target in &targets {
                    if !target.relay_lists.nip65.relays.is_empty()
                        || !target.relay_lists.inbox.relays.is_empty()
                        || target.relay_lists.nip65.created_at > 0
                        || target.relay_lists.inbox.created_at > 0
                    {
                        self.remember_directory_relay_lists(
                            &target.account_id_hex,
                            &target.relay_lists,
                        )?;
                    }
                }
            }
            MemberResolutionPurpose::Prewarm => {
                let mut cache = self
                    .member_key_package_prewarm_cache
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                for (index, target) in targets.iter().enumerate() {
                    // Only completed discovery renews the route deadline.
                    // A usable package can arrive after an incomplete metadata
                    // hop retained an older durable inbox/outbox projection.
                    if discovery.relay_list_completed.contains(&index)
                        && matches!(outcomes[index], Some(Ok(_)))
                    {
                        cache.insert(target.account_id_hex.clone(), target.relay_lists.clone());
                    }
                }
            }
        }

        let mut key_packages = Vec::with_capacity(targets.len());
        for (index, outcome) in outcomes.into_iter().enumerate() {
            match outcome.unwrap_or_else(|| {
                Err(AppError::MissingKeyPackage(
                    targets[index].account_id_hex.clone(),
                ))
            }) {
                Ok(key_package) => {
                    let safe_inbox = unique_safe_endpoints(
                        self,
                        targets[index]
                            .relay_lists
                            .inbox
                            .relays
                            .iter()
                            .cloned()
                            .map(TransportEndpoint)
                            .collect(),
                        "member invite inbox readiness",
                    );
                    if safe_inbox.is_empty() {
                        return Err(AppError::MissingMemberInboxRoute(
                            targets[index].account_id_hex.clone(),
                        ));
                    }
                    key_packages.push(key_package);
                }
                Err(error) => return Err(error),
            }
        }
        Ok(ResolvedMemberKeyPackages {
            key_packages,
            stats: MemberKeyPackageResolutionStats {
                requested_members: member_refs.len(),
                unique_members: targets.len(),
            },
        })
    }

    fn accept_fetched_key_package(
        &self,
        purpose: MemberResolutionPurpose,
        fetched: FetchedKeyPackage,
    ) -> Result<KeyPackage, AppError> {
        let key_package = self.validate_member_key_package_current(
            &fetched.account_id_hex,
            fetched.key_package.clone(),
        )?;
        match purpose {
            MemberResolutionPurpose::Commit | MemberResolutionPurpose::CommitFresh => {
                self.remember_directory_key_package(&fetched)?
            }
            MemberResolutionPurpose::Prewarm => {}
        }
        Ok(key_package)
    }

    fn validate_member_key_package_current(
        &self,
        account_id_hex: &str,
        key_package: KeyPackage,
    ) -> Result<KeyPackage, AppError> {
        let observation = self.product_analytics.begin(
            crate::ProductFamily::KeyPackage,
            "lookup",
            crate::ProductUnit::Attempt,
        );
        let (result, outcome) = validate_current_member_key_package(account_id_hex, key_package);
        if let Some(observation) = observation {
            observation.count(outcome, crate::ProductUnit::Attempt, 1);
            observation.discard();
        }
        result
    }

    fn relay_lists_from_records(
        &self,
        target: &MemberTarget,
        records: Vec<crate::relay_plane::DirectoryRelayEventRecord>,
        endpoints: &[TransportEndpoint],
    ) -> AccountRelayListStatus {
        let mut status = fresh_relay_list_status_from_records(
            &target.account_id_hex,
            records,
            self.directory_freshness(),
        )
        .value;
        if status.bootstrap_relays.is_empty() {
            status.bootstrap_relays = endpoints
                .iter()
                .map(|endpoint| endpoint.0.clone())
                .collect();
        }
        merge_relay_list_status(target.cached_relay_lists.clone(), status)
    }

    /// Run one metadata hop with identical batching, partial-record retention,
    /// and bounded per-author fallback on both discovery and advertised relays.
    /// Completion belongs to this hop only; callers reconcile hops separately.
    async fn resolve_relay_list_hop(
        &self,
        targets: &mut [MemberTarget],
        groups: Vec<(Vec<TransportEndpoint>, Vec<usize>)>,
        network_deadline: tokio::time::Instant,
    ) -> HashSet<usize> {
        let specs = groups
            .into_iter()
            .flat_map(|(endpoints, indices)| {
                indices
                    .chunks(MEMBER_RESOLUTION_AUTHORS_PER_QUERY)
                    .map(|chunk| {
                        let members = chunk
                            .iter()
                            .map(|index| (*index, targets[*index].account_id_hex.clone()))
                            .collect::<Vec<_>>();
                        (endpoints.clone(), members)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let app = self.clone();
        let work = specs.into_iter().map(move |(endpoints, members)| {
            let app = app.clone();
            async move {
                let authors = members.iter().map(|(_, id)| id.clone()).collect::<Vec<_>>();
                let queries = [KIND_NIP65_RELAY_LIST, KIND_MARMOT_INBOX_RELAY_LIST]
                    .into_iter()
                    .map(|kind| {
                        DirectoryEventQuery::new(
                            kind,
                            authors.clone(),
                            authors.len() * RELAY_LIST_EVENTS_PER_AUTHOR,
                        )
                    })
                    .collect();
                let result = fetch_member_directory(
                    &app,
                    endpoints.clone(),
                    queries,
                    network_deadline,
                    fetch::MAX_DISCOVERY_RELAYS,
                )
                .await;
                let outcome = result.directory_outcome();
                if members.len() == 1 || outcome.complete {
                    return members
                        .into_iter()
                        .map(|(index, _)| (index, endpoints.clone(), outcome.clone()))
                        .collect::<Vec<_>>();
                }
                // CLOSED and other incomplete outcomes need the same fallback
                // as transport errors. Retain positive batch records even when
                // the single-author retry also fails.
                stream::iter(members.into_iter().map(|(index, account_id)| {
                    let app = app.clone();
                    let endpoints = endpoints.clone();
                    let mut records = outcome
                        .records
                        .iter()
                        .filter(|record| record.event.pubkey == account_id)
                        .cloned()
                        .collect::<Vec<_>>();
                    async move {
                        let queries = [KIND_NIP65_RELAY_LIST, KIND_MARMOT_INBOX_RELAY_LIST]
                            .into_iter()
                            .map(|kind| {
                                DirectoryEventQuery::new(
                                    kind,
                                    vec![account_id.clone()],
                                    RELAY_LIST_EVENTS_PER_AUTHOR,
                                )
                            })
                            .collect();
                        let retry = fetch_member_directory(
                            &app,
                            endpoints.clone(),
                            queries,
                            network_deadline,
                            fetch::MAX_DISCOVERY_RELAYS,
                        )
                        .await;
                        records.extend(retry.records);
                        (
                            index,
                            endpoints,
                            DirectoryFetchOutcome {
                                records,
                                complete: retry.complete,
                            },
                        )
                    }
                }))
                .buffered(MEMBER_RESOLUTION_FALLBACK_CONCURRENCY)
                .collect::<Vec<_>>()
                .await
            }
        });
        let results = stream::iter(work)
            .buffered(MEMBER_RESOLUTION_RELAY_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        let mut completed = HashSet::new();
        for (index, endpoints, outcome) in results.into_iter().flatten() {
            let account_id = targets[index].account_id_hex.clone();
            targets[index].relay_records.extend(
                outcome
                    .records
                    .into_iter()
                    .filter(|record| record.event.pubkey == account_id),
            );
            targets[index].relay_lists = self.relay_lists_from_records(
                &targets[index],
                targets[index].relay_records.clone(),
                &endpoints,
            );
            if outcome.complete
                && !fresh_relay_list_status_from_records(
                    &account_id,
                    targets[index].relay_records.clone(),
                    self.directory_freshness(),
                )
                .rejected_future
            {
                completed.insert(index);
            }
        }
        completed
    }

    async fn resolve_missing_relay_lists(
        &self,
        targets: &mut [MemberTarget],
        unresolved: &[usize],
        network_deadline: tokio::time::Instant,
    ) -> RelayListResolution {
        let needs_discovery = unresolved.to_vec();
        if needs_discovery.is_empty() {
            return RelayListResolution::default();
        }
        let endpoints = self.directory_source_relays(&[]);
        if endpoints.is_empty() {
            return RelayListResolution::default();
        }
        let completed_discovery = self
            .resolve_relay_list_hop(
                targets,
                vec![(endpoints.clone(), needs_discovery.clone())],
                network_deadline,
            )
            .await;
        let incomplete_discovery = needs_discovery
            .iter()
            .copied()
            .filter(|index| !completed_discovery.contains(index))
            .collect::<HashSet<_>>();

        // Group the same safe outbox sets before querying. This remains pure:
        // only commit-purpose resolution persists the observed metadata.
        let mut by_outboxes = BTreeMap::<Vec<TransportEndpoint>, Vec<usize>>::new();
        for index in needs_discovery.iter().copied() {
            let outbox_endpoints = unique_safe_endpoints(
                self,
                targets[index]
                    .relay_lists
                    .nip65
                    .relays
                    .iter()
                    .cloned()
                    .map(TransportEndpoint)
                    .collect(),
                "member inbox outbox discovery",
            );
            let mut outbox_endpoints = outbox_endpoints
                .into_iter()
                .filter(|endpoint| {
                    !completed_discovery.contains(&index) || !endpoints.contains(endpoint)
                })
                .collect::<Vec<_>>();
            outbox_endpoints.sort();
            if !outbox_endpoints.is_empty() {
                by_outboxes.entry(outbox_endpoints).or_default().push(index);
            }
        }
        let attempted_outbox = by_outboxes
            .values()
            .flatten()
            .copied()
            .collect::<HashSet<_>>();
        let completed_outbox = self
            .resolve_relay_list_hop(targets, by_outboxes.into_iter().collect(), network_deadline)
            .await;
        let completed = completed_discovery
            .iter()
            .copied()
            .filter(|index| !attempted_outbox.contains(index) || completed_outbox.contains(index))
            .collect();
        let errors = needs_discovery
            .into_iter()
            .filter(|index| {
                let has_inbox = !self
                    .retain_safe_discovered_endpoints(
                        targets[*index]
                            .relay_lists
                            .inbox
                            .relays
                            .iter()
                            .cloned()
                            .map(TransportEndpoint)
                            .collect(),
                        "member invite inbox completion",
                    )
                    .is_empty();
                !has_inbox
                    && (incomplete_discovery.contains(index)
                        || (attempted_outbox.contains(index) && !completed_outbox.contains(index)))
            })
            .map(|index| {
                // The known list may be a stale cache or partial snapshot, so a
                // retry can still find usable relays; name it without claiming
                // a verdict. Completed lookups report MissingMemberInboxRoute.
                let message = if targets[index].relay_lists.inbox.relays.is_empty() {
                    "relay-list absence was not authoritatively established"
                } else {
                    "known member inbox relay list has no usable relays and its refresh did not complete"
                };
                (index, AppError::RelayDirectory(message.to_owned()))
            })
            .collect();
        RelayListResolution { completed, errors }
    }

    async fn resolve_missing_key_packages(
        &self,
        targets: &[MemberTarget],
        unresolved: &[usize],
        outcomes: &mut [Option<Result<KeyPackage, AppError>>],
        discovery: &mut MemberResolutionContext<'_>,
    ) {
        let purpose = discovery.purpose;
        let requirements = discovery.requirements;
        let network_deadline = discovery.network_deadline;
        let mut defaults = unique_safe_endpoints(
            self,
            self.directory_source_relays(&[]),
            "member fallback discovery",
        );
        let default_budget_exceeded = defaults.len() > fetch::MAX_DISCOVERY_RELAYS;
        defaults.truncate(fetch::MAX_DISCOVERY_RELAYS);
        let mut by_endpoints = BTreeMap::<Vec<TransportEndpoint>, Vec<usize>>::new();
        for index in unresolved.iter().copied() {
            let mut endpoints = unique_safe_endpoints(
                self,
                targets[index]
                    .relay_lists
                    .nip65
                    .relays
                    .iter()
                    .cloned()
                    .map(TransportEndpoint)
                    .collect(),
                "member KeyPackage batch fetch",
            );
            if endpoints.is_empty() {
                endpoints.clone_from(&defaults);
            }
            endpoints.sort();
            endpoints.dedup();
            if endpoints.is_empty() {
                outcomes[index] = Some(Err(AppError::MemberNoUsableDiscoveryRelays(
                    targets[index].account_id_hex.clone(),
                )));
            } else {
                by_endpoints.entry(endpoints).or_default().push(index);
            }
        }

        let request_specs = by_endpoints
            .into_iter()
            .flat_map(|(endpoints, indices)| {
                indices
                    .chunks(MEMBER_RESOLUTION_AUTHORS_PER_QUERY)
                    .map(|chunk| (endpoints.clone(), chunk.to_vec()))
                    .collect::<Vec<_>>()
            })
            .map(|(endpoints, indices)| {
                let authors = indices
                    .iter()
                    .map(|index| targets[*index].account_id_hex.clone())
                    .collect::<Vec<_>>();
                let queries = [KIND_MARMOT_KEY_PACKAGE, 443]
                    .into_iter()
                    .map(|kind| {
                        let query = DirectoryEventQuery::new(
                            kind,
                            authors.clone(),
                            authors.len() * KEY_PACKAGE_EVENTS_PER_AUTHOR,
                        );
                        if kind == 443 {
                            query.evidence_only()
                        } else {
                            query
                        }
                    })
                    .collect();
                (indices, endpoints, queries)
            })
            .collect::<Vec<_>>();
        let app = self.clone();
        let requests = request_specs
            .into_iter()
            .map(move |(indices, endpoints, queries)| {
                let app = app.clone();
                async move {
                    let outcome = fetch_member_directory(
                        &app,
                        endpoints,
                        queries,
                        network_deadline,
                        fetch::MAX_DISCOVERY_RELAYS,
                    )
                    .await;
                    (indices, outcome)
                }
            });
        let batches = stream::iter(requests)
            .buffered(MEMBER_RESOLUTION_RELAY_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;

        let mut fallback = Vec::new();
        let mut fallback_records = BTreeMap::new();
        let mut completed_single_author = HashSet::new();
        for (indices, outcome) in batches {
            if indices.len() == 1 && outcome.complete {
                completed_single_author.insert(indices[0]);
            }
            let multiple_authors = indices.len() > 1;
            let mut records_by_author = BTreeMap::<_, Vec<_>>::new();
            for record in outcome.records {
                records_by_author
                    .entry(record.event.pubkey.clone())
                    .or_default()
                    .push(record);
            }
            for index in indices {
                let account_id = &targets[index].account_id_hex;
                for record in records_by_author.remove(account_id).unwrap_or_default() {
                    discovery.observed_packages[index]
                        .entry(record.event.id.clone())
                        .or_insert(record);
                }
                let account_records = discovery.observed_packages[index]
                    .values()
                    .cloned()
                    .collect::<Vec<_>>();
                let selected = preferred_fresh_key_package_from_records(
                    account_id,
                    &account_records,
                    self.directory_freshness(),
                    requirements,
                )
                .and_then(|selection| {
                    selection
                        .value
                        .ok_or_else(|| AppError::MissingKeyPackage(account_id.clone()))
                });
                match selected {
                    Ok(selected)
                        if purpose == MemberResolutionPurpose::Prewarm
                            || !multiple_authors
                            || selected.priority == 2 =>
                    {
                        let mut fetched = selected.fetched;
                        fetched.relay_lists = targets[index].relay_lists.clone();
                        outcomes[index] = Some(self.accept_fetched_key_package(purpose, fetched));
                    }
                    // Bounded multi-author results can hide an older preferred
                    // slot even when they contain a valid lower-priority one.
                    // Relays may cap below our requested limit, so even a
                    // short batch does not prove completeness. Prewarm only
                    // needs existence, while commits seek the preferred slot.
                    // A priority-2 winner already has the best tier in a
                    // newest-first prefix. This is a bounded preference search,
                    // not proof of relay completeness: neither path can detect
                    // a newer publication omitted from a non-prefix response.
                    // Keep observed slot replacements when combining the refetch.
                    _ => {
                        fallback.push(index);
                        fallback_records.insert(index, account_records);
                    }
                }
            }
        }

        fallback.sort_unstable();
        fallback.dedup();
        let fallback_specs = fallback
            .into_iter()
            .map(|index| {
                let target = targets[index].clone();
                let mut endpoints = unique_safe_endpoints(
                    self,
                    target
                        .relay_lists
                        .nip65
                        .relays
                        .iter()
                        .cloned()
                        .map(TransportEndpoint)
                        .collect(),
                    "member KeyPackage fallback fetch",
                );
                let recipient_budget_exceeded = endpoints.len() > fetch::MAX_DISCOVERY_RELAYS;
                let complete_primary = completed_single_author.contains(&index);
                let already_searched = if endpoints.is_empty() {
                    &defaults
                } else {
                    &endpoints
                };
                let mut retry_endpoints = defaults
                    .iter()
                    .filter(|endpoint| !complete_primary || !already_searched.contains(endpoint))
                    .cloned()
                    .collect::<Vec<_>>();
                if !complete_primary {
                    retry_endpoints.append(&mut endpoints);
                }
                (
                    index,
                    target,
                    retry_endpoints,
                    fallback_records.remove(&index).unwrap_or_default(),
                    recipient_budget_exceeded,
                )
            })
            .collect::<Vec<_>>();
        let discovery_completed = &discovery.relay_list_completed;
        let defaults_count = defaults.len();
        let app = self.clone();
        let work = fallback_specs.into_iter().map(
            move |(index, target, endpoints, observed_records, recipient_budget_exceeded)| {
                let app = app.clone();
                async move {
                    let queries = [KIND_MARMOT_KEY_PACKAGE, 443]
                        .into_iter()
                        .map(|kind| {
                            let query = DirectoryEventQuery::new(
                                kind,
                                vec![target.account_id_hex.clone()],
                                KEY_PACKAGE_EVENTS_PER_AUTHOR,
                            );
                            if kind == 443 {
                                query.evidence_only()
                            } else {
                                query
                            }
                        })
                        .collect();
                    let outcome = if endpoints.is_empty() {
                        fetch::MemberDirectoryOutcome {
                            complete: true,
                            ..Default::default()
                        }
                    } else {
                        fetch_member_directory(
                            &app,
                            endpoints,
                            queries,
                            network_deadline,
                            fetch::MAX_DISCOVERY_RELAYS + defaults_count,
                        )
                        .await
                    };
                    let complete = outcome.complete && !default_budget_exceeded;
                    let budget_exceeded = recipient_budget_exceeded;
                    let mut records = outcome.records;
                    // This supplementary lookup must not discard a
                    // valid package observed in this call's batch.
                    // Never load a previously cached package here.
                    records.extend(observed_records);
                    let complete = complete
                        && !records_have_future_packages(
                            &target.account_id_hex,
                            &records,
                            app.directory_freshness(),
                        );
                    let metadata_complete = discovery_completed.contains(&index);
                    let result = async {
                        let selected = preferred_fresh_key_package_from_records(
                            &target.account_id_hex,
                            &records,
                            app.directory_freshness(),
                            requirements,
                        );
                        let missing = || {
                            if budget_exceeded {
                                AppError::MemberRelayBudgetExceeded(target.account_id_hex.clone())
                            } else if !complete || !metadata_complete {
                                AppError::MemberDiscoveryIncomplete(target.account_id_hex.clone())
                            } else if obsolete_key_package_observed(
                                &target.account_id_hex,
                                &records,
                                app.directory_freshness(),
                            ) {
                                AppError::ObsoleteKeyPackage(target.account_id_hex.clone())
                            } else {
                                AppError::MissingKeyPackage(target.account_id_hex.clone())
                            }
                        };
                        let mut fetched = match selected {
                            Ok(selection) => selection.value.ok_or_else(missing)?.fetched,
                            Err(AppError::ObsoleteKeyPackage(_)) => return Err(missing()),
                            Err(AppError::InvalidKeyPackageEvent(_)) => {
                                return Err(AppError::MemberInvalidKeyPackage(
                                    target.account_id_hex.clone(),
                                ));
                            }
                            Err(AppError::Session(cgka_session::SessionError::Engine(
                                cgka_traits::EngineError::MissingRequiredCapabilities { .. },
                            ))) => {
                                return Err(AppError::MemberIncompatibleKeyPackage(
                                    target.account_id_hex.clone(),
                                ));
                            }
                            Err(error) => return Err(error),
                        };
                        fetched.relay_lists = target.relay_lists;
                        Ok::<_, AppError>(fetched)
                    }
                    .await;
                    (index, result, records)
                }
            },
        );
        let fallback_results = stream::iter(work)
            .buffered(MEMBER_RESOLUTION_FALLBACK_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        for (index, result, records) in fallback_results {
            for record in records {
                discovery.observed_packages[index]
                    .entry(record.event.id.clone())
                    .or_insert(record);
            }
            outcomes[index] =
                Some(result.and_then(|fetched| self.accept_fetched_key_package(purpose, fetched)));
        }
    }
}

// The closed outcome accompanies the result without storing mutable flags in a closure.
fn validate_current_member_key_package(
    account_id_hex: &str,
    key_package: KeyPackage,
) -> (Result<KeyPackage, AppError>, &'static str) {
    let metadata = match key_package_metadata(&key_package) {
        Ok(metadata) => metadata,
        Err(error) => {
            return (
                Err(match error {
                    cgka_traits::EngineError::InvalidKeyPackageLifetime { .. } => {
                        AppError::MemberInvalidKeyPackageLifetime(account_id_hex.to_owned())
                    }
                    _ => AppError::MemberInvalidKeyPackage(account_id_hex.to_owned()),
                }),
                "invalid",
            );
        }
    };
    if metadata.protocol_profile != ProtocolProfile::Current
        || metadata.credential_identity_hex != account_id_hex
    {
        return (
            Err(AppError::MemberInvalidKeyPackage(account_id_hex.to_owned())),
            "invalid",
        );
    }
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if now < metadata.not_before || now > metadata.not_after {
        return (
            Err(AppError::MemberInvalidKeyPackageLifetime(
                account_id_hex.to_owned(),
            )),
            if now > metadata.not_after {
                "expired"
            } else {
                "unavailable"
            },
        );
    }
    (Ok(key_package), "usable")
}
