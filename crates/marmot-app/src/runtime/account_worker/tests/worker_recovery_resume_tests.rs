//! The owned comparison result stays outside account admission until the
//! serialized worker admits it through ordinary ingest, never through the
//! live delivery queue.

use super::*;
use cgka_traits::{TransportEndpoint, TransportGroupSubscription};
use nostr_relay_builder::MockRelay;
use std::sync::Mutex;
use transport_nostr_adapter::NostrReconciliationProgress;

#[derive(Default)]
struct ComparisonCursor(Mutex<Option<[u8; 32]>>);

impl NostrReconciliationProgress for ComparisonCursor {
    fn load_cursor(&self) -> Result<Option<[u8; 32]>, cgka_traits::TransportAdapterError> {
        Ok(*self.0.lock().unwrap())
    }

    fn save_cursor(
        &self,
        cursor: Option<[u8; 32]>,
    ) -> Result<(), cgka_traits::TransportAdapterError> {
        *self.0.lock().unwrap() = cursor;
        Ok(())
    }
}

#[tokio::test]
async fn comparison_result_waits_for_worker_submission_before_durable_admission() {
    let _serial = BOUNDED_WORKER_FIXTURE_LOCK.lock().await;
    let relay = MockRelay::run().await.unwrap();
    let url = relay.url().await.to_string();
    let dir = tempfile::tempdir().unwrap();
    let home = AccountHome::open(dir.path());
    let alice = home.create_account("alice").unwrap();
    let bob = home.create_account("bob").unwrap();
    let app = MarmotApp::with_relay_and_config(
        dir.path(),
        url.clone(),
        crate::MarmotAppConfig::default().with_allow_loopback_relay_endpoints(true),
    );
    crate::tests::remember_test_member_inbox(&app, &bob.account_id_hex, &url);
    let runtime = super::super::super::MarmotAppRuntime::new(app.clone());
    runtime.reconcile_accounts().await.unwrap();
    runtime.publish_key_package(&bob.label).await.unwrap();
    let group = runtime
        .create_group_with_options(
            &alice.label,
            "owned comparison result",
            std::slice::from_ref(&bob.account_id_hex),
            AppCreateGroupOptions {
                relays: Some(vec![url.clone()]),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(10), async {
        loop {
            runtime.catch_up_accounts().await.unwrap();
            if app
                .group(&bob.label, &hex::encode(&group))
                .unwrap()
                .is_some()
            {
                break;
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("peer joins before the missing message is published");
    runtime
        .accounts()
        .workers
        .lock()
        .await
        .remove(&alice.account_id_hex)
        .unwrap()
        .shutdown()
        .await;
    runtime
        .send_message(&bob.label, &group, b"comparison-owned body".to_vec())
        .await
        .unwrap();

    let mut client = app.client(&alice.label).await.unwrap();
    // Register ordinary route state and wait for its startup replay to finish
    // before dropping the queued delivery. Reconciliation must reacquire its
    // bytes despite the first SDK sighting.
    client.prepare_transport().await.unwrap();
    timeout(Duration::from_secs(10), async {
        while !client.adapter.account_subscription_eose().await.complete() {
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("startup replay reaches EOSE before checking comparison ownership");
    while matches!(
        timeout(Duration::from_millis(100), client.receive_next_delivery()).await,
        Ok(Ok(_))
    ) {}
    let record = app
        .group(&alice.label, &hex::encode(&group))
        .unwrap()
        .unwrap();
    let route: [u8; 32] = hex::decode(record.nostr_routing.nostr_group_id_hex)
        .unwrap()
        .try_into()
        .unwrap();
    let storage = app.account_storage(&alice.label).unwrap();
    let inventory = storage
        .transport_reconciliation_inventory(
            &storage_sqlite::TransportReconciliationRoute::Group(route),
            crate::unix_now_seconds(),
        )
        .unwrap();
    let local_items = inventory
        .items
        .iter()
        .map(|item| transport_nostr_adapter::NostrReconciliationItem {
            event_id: item.event_id,
            created_at: item.created_at,
        })
        .collect::<Vec<_>>();
    let subscription = TransportGroupSubscription {
        group_id: group.clone(),
        transport_group_id: route.to_vec(),
        endpoints: vec![TransportEndpoint(url)],
        retained_since: None,
    };
    let (summary, mut events) = client
        .adapter
        .reconcile_group_history(
            subscription.clone(),
            &local_items,
            inventory.since,
            crate::unix_now_seconds(),
            &ComparisonCursor::default(),
        )
        .await
        .unwrap()
        .expect("SDK reconciliation backend is configured");
    assert_eq!(summary.relays_succeeded, 1);
    assert_eq!(summary.relays_failed, 0);
    assert!(
        !events.is_empty(),
        "the missing encrypted event is returned"
    );
    let event = events.remove(0);
    let event_id: [u8; 32] = hex::decode(&event.event.id).unwrap().try_into().unwrap();
    let route = storage_sqlite::TransportReconciliationRoute::Group(route);
    assert!(
        !storage
            .retained_recovery_event(&route, &event_id, None, crate::unix_now_seconds())
            .unwrap()
    );
    assert!(
        timeout(Duration::from_millis(100), client.receive_next_delivery())
            .await
            .is_err(),
        "an owned comparison result must not enter the ordinary delivery queue"
    );
    let (_, repeated) = client
        .adapter
        .reconcile_group_history(
            subscription.clone(),
            &local_items,
            inventory.since,
            crate::unix_now_seconds(),
            &ComparisonCursor::default(),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(
        repeated
            .iter()
            .any(|candidate| candidate.event.id == hex::encode(event_id)),
        "a comparison result not admitted by the worker remains eligible"
    );
    assert!(
        !storage
            .retained_recovery_event(&route, &event_id, None, crate::unix_now_seconds())
            .unwrap()
    );

    let deliveries = client.adapter.recovered_deliveries(event).await.unwrap();
    assert_eq!(deliveries.len(), 1, "the event routes to this account once");
    for delivery in deliveries {
        client.admit_recovered_delivery(delivery).await.unwrap();
    }
    assert!(
        storage
            .retained_recovery_event(&route, &event_id, None, crate::unix_now_seconds())
            .unwrap()
    );
    assert!(
        timeout(Duration::from_millis(100), client.receive_next_delivery())
            .await
            .is_err(),
        "worker admission never queues the event for live delivery"
    );
    drop(client);
    runtime.shutdown_and_close().await.unwrap();
}

#[tokio::test]
async fn closed_epoch_pin_recovers_before_the_next_queued_read_and_send() {
    let _serial = BOUNDED_WORKER_FIXTURE_LOCK.lock().await;
    let dir = tempfile::tempdir().unwrap();
    let account = AccountHome::open(dir.path())
        .create_account("alice")
        .unwrap();
    let relay = Arc::new(ScriptedPushRelayClient::default());
    let app = MarmotApp::with_relay(dir.path(), "wss://relay.example")
        .with_test_relay_client(relay.clone());
    let mut setup = app.client("alice").await.unwrap();
    let group_id = setup.create_group("worker epoch pin", &[]).await.unwrap();
    let current_epoch = setup.group_mls_state(&group_id).unwrap().epoch;
    drop(setup);

    let runtime = super::super::super::MarmotAppRuntime::new(app.clone());
    runtime.reconcile_accounts().await.unwrap();
    runtime.catch_up_accounts().await.unwrap();
    let commands = runtime.accounts().worker_commands("alice").await.unwrap();
    let subscriptions_before = relay.subscription_count();
    let group_message_attempts_before = relay.group_message_attempt_count();
    let reference = MediaAttachmentReference {
        locators: vec![crate::MediaLocator {
            kind: "blossom-v1".into(),
            value: format!("https://media.example/{}.bin", hex::encode([0x44_u8; 32])),
        }],
        ciphertext_sha256: hex::encode([0x44_u8; 32]),
        plaintext_sha256: hex::encode([0x12_u8; 32]),
        nonce_hex: hex::encode([0x23_u8; 12]),
        file_name: "epoch.png".into(),
        media_type: "image/png".into(),
        version: "encrypted-media-v2".into(),
        source_epoch: current_epoch + 1,
        dim: None,
        thumbhash: None,
    };
    let (respond, refused) = oneshot::channel();
    commands
        .try_send(AccountWorkerCommand::SendAppEvent {
            enqueued_at: Instant::now(),
            group_id: group_id.clone(),
            intent: AppMessageIntent::Media {
                message_tags: vec![],
                attachments: vec![reference],
                caption: Some("refused epoch reference".into()),
            },
            respond,
        })
        .unwrap();
    let (respond, read) = oneshot::channel();
    commands
        .try_send(AccountWorkerCommand::GroupMlsState {
            group_id: group_id.clone(),
            respond,
        })
        .unwrap();
    let (respond, sent) = oneshot::channel();
    commands
        .try_send(AccountWorkerCommand::SendMessage {
            enqueued_at: Instant::now(),
            queued: None,
            group_id: group_id.clone(),
            payload: b"valid after epoch refusal".to_vec(),
            respond,
        })
        .unwrap();

    let error = timeout(Duration::from_secs(10), refused)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(matches!(error, AppError::MediaReferenceStaleEpoch {
        source_epoch, current_epoch: observed,
    } if source_epoch == current_epoch + 1 && observed == current_epoch));
    let state = timeout(Duration::from_secs(10), read)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(state.epoch, current_epoch);
    let summary = timeout(Duration::from_secs(10), sent)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(summary.published, 1);
    assert_eq!(
        relay.group_message_attempt_count(),
        group_message_attempts_before + 1
    );
    assert!(
        relay.subscription_count() > subscriptions_before,
        "the closed owner must retire its SDK context and reactivate the replacement"
    );
    let messages = app.messages("alice").unwrap();
    let valid = messages
        .iter()
        .filter(|message| message.plaintext == "valid after epoch refusal" && !message.invalidated)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(valid.len(), 1);
    let refused_rows = messages
        .iter()
        .filter(|message| message.plaintext == "refused epoch reference")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(refused_rows.len(), 1);
    assert!(refused_rows[0].invalidated);

    runtime
        .accounts()
        .workers
        .lock()
        .await
        .remove(&account.account_id_hex)
        .unwrap()
        .shutdown()
        .await;
    let reopened = app.client("alice").await.unwrap();
    assert!(!reopened.runtime.session().is_closed());
    assert_eq!(
        reopened.group_mls_state(&group_id).unwrap().epoch,
        current_epoch
    );
    let retained = app
        .messages("alice")
        .unwrap()
        .into_iter()
        .filter(|message| message.plaintext == "valid after epoch refusal" && !message.invalidated)
        .collect::<Vec<_>>();
    assert_eq!(retained, valid);
    let retained_refusal = app
        .messages("alice")
        .unwrap()
        .into_iter()
        .filter(|message| message.plaintext == "refused epoch reference")
        .collect::<Vec<_>>();
    assert_eq!(retained_refusal, refused_rows);
    assert_eq!(
        relay.group_message_attempt_count(),
        group_message_attempts_before + 1
    );
    drop(reopened);
    runtime.shutdown().await;
}
