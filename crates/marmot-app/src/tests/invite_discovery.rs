use super::*;

fn advertised_relays(account: &AccountSummary, count: usize) -> NostrTransportEvent {
    NostrTransportEvent::new_unsigned(
        account.account_id_hex.clone(),
        KIND_NIP65_RELAY_LIST,
        (0..count)
            .map(|index| {
                vec![
                    "r".into(),
                    format!("wss://relay-{index:03}.example"),
                    "write".into(),
                ]
            })
            .collect(),
        String::new(),
    )
}

#[tokio::test]
async fn invite_discovery_counts_distinct_normalized_relays_instead_of_url_aliases() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let mut routes = advertised_relays(account, 40);
    let aliases = routes
        .tags
        .iter()
        .map(|tag| vec!["r".into(), format!("{}/", tag[1]), "write".into()])
        .collect::<Vec<_>>();
    routes.tags.extend(aliases);
    let inbox = fetcher
        .events
        .lock()
        .unwrap()
        .iter()
        .find(|event| event.kind == KIND_MARMOT_INBOX_RELAY_LIST)
        .unwrap()
        .clone();
    fetcher
        .events_by_endpoint
        .lock()
        .unwrap()
        .insert("wss://directory.example".into(), vec![routes, inbox]);
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(
        matches!(error, AppError::MissingKeyPackage(_)),
        "{}",
        error.privacy_safe_kind()
    );
}

#[tokio::test]
async fn invite_discovery_preserves_explicit_development_loopback_policy() {
    let (_dir, mut app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    app.config.directory_relay_urls = vec!["ws://127.0.0.1:7777".into()];
    app.relay_plane = MarmotRelayPlane::new_with_directory_fetcher_for_test(
        Some(Duration::from_secs(120)),
        Arc::new(ScriptedPushRelayClient::default()),
        fetcher.clone(),
        true,
    );
    app.resolve_member_key_packages(&[accounts[0].account_id_hex.as_str()])
        .await
        .unwrap();
    assert!(fetcher.requests.lock().unwrap().iter().any(|request| {
        request
            .endpoints
            .iter()
            .any(|endpoint| endpoint.0.starts_with("ws://127.0.0.1:7777"))
    }));
}

#[tokio::test]
async fn invite_discovery_reports_no_usable_source_without_dialing_unsafe_configuration() {
    let (_dir, mut app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    app.config.directory_relay_urls = vec!["ws://unsafe.example".into()];
    let error = app
        .resolve_member_key_packages(&[accounts[0].account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::MemberNoUsableDiscoveryRelays(_)));
    assert!(fetcher.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn invite_discovery_refreshes_stale_prewarm_routes_before_reporting_failure() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    app.prewarm_group_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap();
    let events = fetcher.events.lock().unwrap().clone();
    let mut routes = events
        .iter()
        .find(|event| event.kind == KIND_NIP65_RELAY_LIST)
        .unwrap()
        .clone();
    routes.created_at += 1;
    routes.tags = vec![vec!["r".into(), "wss://new-route.example".into()]];
    let inbox = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_INBOX_RELAY_LIST)
        .unwrap()
        .clone();
    let package = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_KEY_PACKAGE)
        .unwrap()
        .clone();
    fetcher.events_by_endpoint.lock().unwrap().extend([
        ("wss://directory.example".into(), vec![routes, inbox]),
        ("wss://new-route.example".into(), vec![package]),
    ]);
    app.resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap();
    assert!(fetcher.requests.lock().unwrap().iter().any(|request| {
        request
            .endpoints
            .iter()
            .any(|endpoint| endpoint.0 == "wss://new-route.example")
            && request
                .queries
                .iter()
                .any(|query| query.kind == KIND_MARMOT_KEY_PACKAGE)
    }));
}

#[tokio::test]
async fn invite_discovery_keeps_a_newer_invalid_slot_replacement_across_route_refresh() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    app.prewarm_group_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap();
    let events = fetcher.events.lock().unwrap().clone();
    let mut routes = events
        .iter()
        .find(|event| event.kind == KIND_NIP65_RELAY_LIST)
        .unwrap()
        .clone();
    routes.created_at += 1;
    routes.tags = vec![vec!["r".into(), "wss://new-route.example".into()]];
    let inbox = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_INBOX_RELAY_LIST)
        .unwrap()
        .clone();
    let package = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_KEY_PACKAGE)
        .unwrap()
        .clone();
    let mut replacement = package.clone();
    replacement.created_at += 1;
    replacement.id = "ff".repeat(32);
    replacement.content = "not base64".into();
    fetcher.events_by_endpoint.lock().unwrap().extend([
        ("wss://directory.example".into(), vec![routes, inbox]),
        ("wss://shared.example".into(), vec![replacement]),
        ("wss://new-route.example".into(), vec![package]),
    ]);
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::MemberInvalidKeyPackage(_)));
}

#[tokio::test]
async fn invite_discovery_does_not_certify_absence_after_incomplete_profile_metadata() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    fetcher
        .events
        .lock()
        .unwrap()
        .retain(|event| event.kind != KIND_MARMOT_KEY_PACKAGE);
    *fetcher.incomplete_endpoint.lock().unwrap() = Some("wss://directory.example".into());
    let error = app
        .resolve_member_key_packages(&[accounts[0].account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::MemberDiscoveryIncomplete(_)));
}

#[tokio::test]
async fn invite_discovery_searches_beyond_the_first_sixteen_relays() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let events = fetcher.events.lock().unwrap().clone();
    let inbox = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_INBOX_RELAY_LIST)
        .unwrap()
        .clone();
    let package = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_KEY_PACKAGE)
        .unwrap()
        .clone();
    let mut routes = advertised_relays(account, 32);
    // Unsafe and duplicate entries do not poison the usable list or consume distinct work.
    routes
        .tags
        .push(vec!["r".into(), "ws://unsafe.example".into()]);
    routes.tags.push(routes.tags[0].clone());
    fetcher.events_by_endpoint.lock().unwrap().extend([
        ("wss://directory.example".into(), vec![routes, inbox]),
        ("wss://relay-031.example".into(), vec![package]),
    ]);
    app.resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap();
    let requests = fetcher.requests.lock().unwrap();
    assert!(requests.iter().all(|request| request.endpoints.len() <= 16));
    assert!(requests.iter().any(|request| {
        request
            .endpoints
            .iter()
            .any(|endpoint| endpoint.0 == "wss://relay-031.example")
            && request
                .queries
                .iter()
                .any(|query| query.kind == KIND_MARMOT_KEY_PACKAGE)
    }));
    assert!(requests.iter().all(|request| {
        request
            .endpoints
            .iter()
            .all(|endpoint| !endpoint.0.starts_with("ws://"))
    }));
}

#[tokio::test]
async fn invite_discovery_retries_configured_relays_after_a_complete_outbox_miss() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let events = fetcher.events.lock().unwrap().clone();
    fetcher
        .events_by_endpoint
        .lock()
        .unwrap()
        .insert("wss://directory.example".into(), events);
    app.resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap();
    let requests = fetcher.requests.lock().unwrap();
    assert!(requests.iter().any(|request| {
        request
            .endpoints
            .iter()
            .any(|endpoint| endpoint.0 == "wss://directory.example")
            && request
                .queries
                .iter()
                .any(|query| query.kind == KIND_MARMOT_KEY_PACKAGE)
    }));
}

#[tokio::test]
async fn invite_discovery_distinguishes_incomplete_acquisition_from_complete_missing_packages() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    fetcher
        .events
        .lock()
        .unwrap()
        .retain(|event| event.kind != KIND_MARMOT_KEY_PACKAGE);
    *fetcher.incomplete_endpoint.lock().unwrap() = Some("wss://shared.example".into());
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(
        matches!(error, AppError::MemberDiscoveryIncomplete(ref id) if id == &account.account_id_hex)
    );
    *fetcher.incomplete_endpoint.lock().unwrap() = None;
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::MissingKeyPackage(ref id) if id == &account.account_id_hex));
}

#[tokio::test]
async fn invite_discovery_reports_a_relay_budget_without_rejecting_a_positive_package() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let events = fetcher.events.lock().unwrap().clone();
    let inbox = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_INBOX_RELAY_LIST)
        .unwrap()
        .clone();
    let package = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_KEY_PACKAGE)
        .unwrap()
        .clone();
    fetcher.events_by_endpoint.lock().unwrap().insert(
        "wss://directory.example".into(),
        vec![advertised_relays(account, 70), inbox],
    );
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(
        matches!(error, AppError::MemberRelayBudgetExceeded(ref id) if id == &account.account_id_hex)
    );
    fetcher
        .events_by_endpoint
        .lock()
        .unwrap()
        .insert("wss://relay-031.example".into(), vec![package]);
    app.resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap();
    assert!(
        fetcher
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|request| request.endpoints.len() <= 16)
    );
}

#[tokio::test]
async fn invite_discovery_classifies_verified_legacy_without_admitting_it_or_trusting_tags() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let legacy = fresh_key_package_for_account(&app, account, true).await;
    let mut event = member_resolution_key_package_event(account, legacy);
    event.kind = 443;
    fetcher
        .events
        .lock()
        .unwrap()
        .retain(|event| event.kind != KIND_MARMOT_KEY_PACKAGE);
    fetcher.events.lock().unwrap().push(event.clone());
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::ObsoleteKeyPackage(ref id) if id == &account.account_id_hex));
    // A client label and declared legacy tags cannot turn malformed bytes into v1 evidence.
    fetcher.events.lock().unwrap().last_mut().unwrap().content = "not a package".into();
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::MissingKeyPackage(_)));
    // A valid current candidate always succeeds alongside observed legacy publications.
    let current = fresh_key_package_for_account(&app, account, false).await;
    fetcher
        .events
        .lock()
        .unwrap()
        .extend([event, member_resolution_key_package_event(account, current)]);
    app.resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap();
}

#[tokio::test]
async fn invite_discovery_does_not_require_an_outbox_list_when_configured_fallback_works() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    fetcher
        .events
        .lock()
        .unwrap()
        .retain(|event| event.kind != KIND_NIP65_RELAY_LIST);
    app.resolve_member_key_packages(&[accounts[0].account_id_hex.as_str()])
        .await
        .unwrap();
}

#[tokio::test]
async fn invite_discovery_preserves_observed_replacement_across_failed_fallback() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let events = fetcher.events.lock().unwrap().clone();
    let current = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_KEY_PACKAGE)
        .unwrap()
        .clone();
    let mut invalid = current.clone();
    invalid.created_at += 1;
    invalid.id = "ff".repeat(32);
    invalid.content = "not base64".into();
    let routes = advertised_relays(account, 32);
    let inbox = events
        .iter()
        .find(|event| event.kind == KIND_MARMOT_INBOX_RELAY_LIST)
        .unwrap()
        .clone();
    fetcher.events_by_endpoint.lock().unwrap().extend([
        (
            "wss://directory.example".into(),
            vec![routes, inbox, current.clone()],
        ),
        ("wss://relay-000.example".into(), vec![current]),
        ("wss://relay-031.example".into(), vec![invalid]),
    ]);
    *fetcher.failing_key_package_endpoint.lock().unwrap() = Some("wss://directory.example".into());
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(
        matches!(error, AppError::MemberInvalidKeyPackage(ref id) if id == &account.account_id_hex)
    );
}

#[tokio::test]
async fn invite_discovery_slow_32_relay_search_finishes_with_recipient_guidance_before_outer_deadline()
 {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let inbox = fetcher
        .events
        .lock()
        .unwrap()
        .iter()
        .find(|event| event.kind == KIND_MARMOT_INBOX_RELAY_LIST)
        .unwrap()
        .clone();
    fetcher.events_by_endpoint.lock().unwrap().insert(
        "wss://directory.example".into(),
        vec![advertised_relays(account, 32), inbox],
    );
    *fetcher.fetch_delay.lock().unwrap() = Some(Duration::from_secs(8));
    *fetcher.incomplete_endpoint.lock().unwrap() = Some("wss://relay-031.example".into());
    tokio::time::pause();
    let start = tokio::time::Instant::now();
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(
        matches!(error, AppError::MemberDiscoveryIncomplete(ref id) if id == &account.account_id_hex)
    );
    assert!(start.elapsed() < Duration::from_secs(50));
}

#[tokio::test]
async fn invite_discovery_64_relay_list_does_not_charge_the_configured_fallback_to_the_recipient() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let inbox = fetcher
        .events
        .lock()
        .unwrap()
        .iter()
        .find(|event| event.kind == KIND_MARMOT_INBOX_RELAY_LIST)
        .unwrap()
        .clone();
    fetcher.events_by_endpoint.lock().unwrap().insert(
        "wss://directory.example".into(),
        vec![advertised_relays(account, 64), inbox],
    );
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(
        matches!(error, AppError::MissingKeyPackage(_)),
        "configured fallback does not make64 overbudget: {error:?}"
    );
}

#[tokio::test]
async fn invite_discovery_saturated_legacy_history_does_not_obscure_a_completed_current_search() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let legacy = fresh_key_package_for_account(&app, account, true).await;
    let mut event = member_resolution_key_package_event(account, legacy);
    event.kind = 443;
    fetcher
        .events
        .lock()
        .unwrap()
        .retain(|event| event.kind != KIND_MARMOT_KEY_PACKAGE);
    for index in 0..12 {
        let mut historical = event.clone();
        historical.id = format!("{index:064x}");
        fetcher.events.lock().unwrap().push(historical);
    }
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::ObsoleteKeyPackage(_)));
}

#[tokio::test]
async fn invite_discovery_legacy_probe_rejection_cannot_hide_current_keys_or_absence() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    fetcher
        .reject_legacy_queries
        .store(true, std::sync::atomic::Ordering::SeqCst);
    app.resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap();
    let requests = fetcher.requests.lock().unwrap().clone();
    assert!(
        requests
            .iter()
            .all(|r| r.queries.iter().all(|q| q.kind != 443))
    );
    assert!(
        requests
            .iter()
            .all(|r| !(r.queries.iter().any(|q| q.kind == 443)
                && r.queries.iter().any(|q| q.kind == KIND_MARMOT_KEY_PACKAGE)))
    );
    fetcher
        .events
        .lock()
        .unwrap()
        .retain(|event| event.kind != KIND_MARMOT_KEY_PACKAGE);
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::MissingKeyPackage(ref id) if id == &account.account_id_hex));
    assert!(
        fetcher
            .requests
            .lock()
            .unwrap()
            .iter()
            .any(|r| r.queries.iter().any(|q| q.kind == 443))
    );
}

#[tokio::test]
async fn invite_discovery_future_legacy_evidence_cannot_downgrade_current_absence() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let legacy = fresh_key_package_for_account(&app, account, true).await;
    let mut event = member_resolution_key_package_event(account, legacy);
    event.kind = 443;
    event.created_at += 86400;
    fetcher
        .events
        .lock()
        .unwrap()
        .retain(|event| event.kind != KIND_MARMOT_KEY_PACKAGE);
    fetcher.events.lock().unwrap().push(event);
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::MissingKeyPackage(ref id) if id == &account.account_id_hex));
}

#[tokio::test]
async fn invite_discovery_classifies_hex_legacy_evidence_without_admitting_it() {
    let (_dir, app, accounts, fetcher) = member_resolution_fixture(1, false).await;
    let account = &accounts[0];
    let legacy = fresh_key_package_for_account(&app, account, true).await;
    let content = hex::encode(legacy.bytes());
    let mut event = member_resolution_key_package_event(account, legacy);
    event.kind = 443;
    event.content = content;
    event
        .tags
        .retain(|tag| tag.first().map(String::as_str) != Some("encoding"));
    event.tags.push(vec!["encoding".into(), "hex".into()]);
    fetcher
        .events
        .lock()
        .unwrap()
        .retain(|event| event.kind != KIND_MARMOT_KEY_PACKAGE);
    fetcher.events.lock().unwrap().push(event);
    let error = app
        .resolve_member_key_packages(&[account.account_id_hex.as_str()])
        .await
        .unwrap_err();
    assert!(matches!(error, AppError::ObsoleteKeyPackage(ref id) if id == &account.account_id_hex));
}
