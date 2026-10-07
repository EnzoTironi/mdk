//! Durable queued-intent ownership of prepared artifacts.

use crate::engine::Engine;
use crate::openmls_projection::{OpenMlsContentKind, decode_openmls_wire_projection};
use cgka_traits::engine::{SendIntent, SendResult};
use cgka_traits::engine_state::PendingStateRef;
use cgka_traits::error::EngineError;
use cgka_traits::message::StoredMessagePayload;
use cgka_traits::storage::{
    QueuedIntentPreparation, QueuedOutboundIntent, RegeneratedArtifact, StorageProvider,
};
use cgka_traits::types::{GroupId, MessageId};
use std::collections::HashSet;

pub(crate) enum CheckedQueuedBinding {
    Message {
        intent_id: MessageId,
        message_id: MessageId,
    },
    PendingCommit {
        intent_id: MessageId,
        origin_message_id: MessageId,
        pending: PendingStateRef,
    },
}

impl<S: StorageProvider> Engine<S> {
    /// The host's outer transaction includes preparation, this binding and
    /// frozen fanout staging before any returned artifact can leave the process.
    pub(crate) fn bind_regenerated_queued_artifact(
        &mut self,
        record: &QueuedOutboundIntent,
        result: &SendResult,
    ) -> Result<(), EngineError> {
        if !matches!(record.preparation, QueuedIntentPreparation::Unprepared {}) {
            return Err(EngineError::QueuedIntentRecoveryFailed);
        }
        let artifact = match result {
            SendResult::ApplicationMessage { msg, group_id, .. } => {
                if group_id != &record.group_id {
                    return Err(EngineError::QueuedIntentRecoveryFailed);
                }
                RegeneratedArtifact::Message {
                    message_id: msg.id.clone(),
                }
            }
            SendResult::Proposal { msg } => RegeneratedArtifact::Message {
                message_id: msg.id.clone(),
            },
            SendResult::GroupEvolution { msg, .. } => RegeneratedArtifact::PendingCommit {
                origin_message_id: msg.id.clone(),
            },
            SendResult::NoChange { group_id } if group_id == &record.group_id => {
                self.storage.delete_queued_outbound_intent(&record.id)?;
                return Ok(());
            }
            SendResult::DisbandRequested { request } if request.group_id == record.group_id => {
                self.storage.delete_queued_outbound_intent(&record.id)?;
                return Ok(());
            }
            _ => return Err(EngineError::QueuedIntentRecoveryFailed),
        };
        let mut bound = record.clone();
        bound.preparation = QueuedIntentPreparation::BoundArtifact { artifact };
        self.storage.put_queued_outbound_intent(&bound)?;
        match result {
            SendResult::ApplicationMessage { msg, .. } | SendResult::Proposal { msg } => {
                self.queued_intent_by_message
                    .insert(msg.id.clone(), (record.group_id.clone(), record.id.clone()));
            }
            SendResult::GroupEvolution { pending, .. } => {
                self.queued_intent_by_pending
                    .insert(*pending, (record.group_id.clone(), record.id.clone()));
            }
            _ => {}
        }
        Ok(())
    }

    /// Validate every binding before changing either map. Missing or mismatched
    /// authority blocks hydration/drain; it never turns a bound row unprepared.
    pub(crate) fn restore_queued_artifact_bindings(
        &mut self,
        group_id: &GroupId,
    ) -> Result<(), EngineError> {
        let bindings = self.checked_queued_artifact_bindings(group_id)?;
        let mut messages = Vec::new();
        let mut pending = Vec::new();
        for binding in bindings {
            match binding {
                CheckedQueuedBinding::PendingCommit {
                    intent_id,
                    origin_message_id,
                    pending: reference,
                } => {
                    if self.pending_origin_commits.get(&reference) != Some(&origin_message_id)
                        || self.epoch_manager.group_for_pending(reference).as_ref()
                            != Some(group_id)
                    {
                        return Err(EngineError::QueuedIntentRecoveryFailed);
                    }
                    pending.push((reference, (group_id.clone(), intent_id)));
                }
                CheckedQueuedBinding::Message {
                    intent_id,
                    message_id,
                } => messages.push((message_id, (group_id.clone(), intent_id))),
            }
        }
        self.queued_intent_by_message
            .retain(|_, (group, _)| group != group_id);
        self.queued_intent_by_pending
            .retain(|_, (group, _)| group != group_id);
        self.queued_intent_by_message.extend(messages);
        self.queued_intent_by_pending.extend(pending);
        Ok(())
    }

    /// Read-only preflight precedes pending-commit recovery: a missing fanout
    /// must not send a bound commit through the unowned-commit rollback path.
    pub(crate) fn checked_queued_artifact_bindings(
        &self,
        group_id: &GroupId,
    ) -> Result<Vec<CheckedQueuedBinding>, EngineError> {
        let records = self.storage.list_queued_outbound_intents(group_id)?;
        let fanouts = self.storage.list_outbound_fanouts_for_group(group_id)?;
        let mut bindings = Vec::new();
        let mut artifacts = HashSet::new();
        for record in records {
            if &record.group_id != group_id
                || super::message_processor::send_intent_group_id(&record.intent) != group_id
            {
                return Err(EngineError::QueuedIntentRecoveryFailed);
            }
            let (message_id, expected_kind, is_pending) = match &record.preparation {
                QueuedIntentPreparation::Unprepared {} => continue,
                QueuedIntentPreparation::BoundArtifact {
                    artifact: RegeneratedArtifact::Message { message_id },
                } => {
                    let expected = match &record.intent {
                        SendIntent::AppMessage { .. } => OpenMlsContentKind::Application,
                        SendIntent::Leave { .. } => OpenMlsContentKind::Proposal,
                        _ => return Err(EngineError::QueuedIntentRecoveryFailed),
                    };
                    (message_id, expected, false)
                }
                QueuedIntentPreparation::BoundArtifact {
                    artifact: RegeneratedArtifact::PendingCommit { origin_message_id },
                } => {
                    if matches!(
                        record.intent,
                        SendIntent::AppMessage { .. } | SendIntent::Leave { .. }
                    ) {
                        return Err(EngineError::QueuedIntentRecoveryFailed);
                    }
                    (origin_message_id, OpenMlsContentKind::Commit, true)
                }
            };
            if !artifacts.insert(message_id.clone()) {
                return Err(EngineError::QueuedIntentRecoveryFailed);
            }
            let stored = self
                .storage
                .get_message(message_id)
                .map_err(|_| EngineError::QueuedIntentRecoveryFailed)?;
            let (_, projection) = decode_openmls_wire_projection(&stored.payload)
                .ok_or(EngineError::QueuedIntentRecoveryFailed)?;
            let payload = StoredMessagePayload::decode(&stored.payload)
                .map_err(|_| EngineError::QueuedIntentRecoveryFailed)?;
            let exact = payload
                .as_exact_transport()
                .ok_or(EngineError::QueuedIntentRecoveryFailed)?;
            if &stored.group_id != group_id
                || &stored.id != message_id
                || projection.kind != expected_kind
                || projection.source_epoch != Some(stored.epoch.0)
                || &exact.id != message_id
            {
                return Err(EngineError::QueuedIntentRecoveryFailed);
            }
            let mut matching = fanouts.iter().filter(|fanout| {
                fanout.group_id() == Some(group_id)
                    && if is_pending {
                        fanout.pending_origin_message_id() == Some(message_id)
                    } else {
                        fanout.message_id() == message_id
                    }
            });
            let fanout = matching
                .next()
                .ok_or(EngineError::QueuedIntentRecoveryFailed)?;
            if matching.next().is_some()
                || &fanout.request().message != exact
                || is_pending != fanout.pending_ref().is_some()
            {
                return Err(EngineError::QueuedIntentRecoveryFailed);
            }
            if fanout.request().account_id != self.identity.self_id().clone() {
                return Err(EngineError::QueuedIntentRecoveryFailed);
            }
            if is_pending
                && fanout.pending_kind() != Some(cgka_traits::FanoutPendingKind::GroupEvolution)
            {
                return Err(EngineError::QueuedIntentRecoveryFailed);
            }
            match &record.intent {
                SendIntent::AppMessage { payload, .. } => {
                    let event = cgka_traits::app_event::MarmotAppEvent::decode(payload)
                        .map_err(|_| EngineError::QueuedIntentRecoveryFailed)?;
                    let context = fanout
                        .application_message()
                        .ok_or(EngineError::QueuedIntentRecoveryFailed)?;
                    if context.app_event_id != event.id
                        || &context.group_id != group_id
                        || context.source_epoch != stored.epoch
                    {
                        return Err(EngineError::QueuedIntentRecoveryFailed);
                    }
                }
                _ if fanout.application_message().is_some() => {
                    return Err(EngineError::QueuedIntentRecoveryFailed);
                }
                _ => {}
            }
            if let Some(pending) = fanout.pending_ref() {
                bindings.push(CheckedQueuedBinding::PendingCommit {
                    intent_id: record.id,
                    origin_message_id: message_id.clone(),
                    pending,
                });
            } else {
                bindings.push(CheckedQueuedBinding::Message {
                    intent_id: record.id,
                    message_id: message_id.clone(),
                });
            }
        }
        Ok(bindings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cgka_traits::app_event::{MARMOT_APP_EVENT_KIND_CHAT, MarmotAppEvent};
    use cgka_traits::engine::{CgkaEngine, CreateGroupRequest};
    use cgka_traits::storage::{
        MessageStorage, OutboundFanoutStorage, OutboundIntentStorage, StorageError,
    };
    use cgka_traits::transport::TransportEnvelope;
    use cgka_traits::{
        OutboundApplicationMessage, OutboundFanout, TransportEndpoint, TransportEndpointFailure,
        TransportEndpointFailureKind, TransportPublishRequest, TransportPublishTarget,
    };
    use storage_sqlite::SqliteAccountStorage;

    fn engine(storage: &SqliteAccountStorage) -> Engine<SqliteAccountStorage> {
        crate::distributed_convergence::tests::current_test_engine_with_storage(storage.clone())
    }

    async fn group(engine: &mut Engine<SqliteAccountStorage>) -> GroupId {
        let (group, created) = engine
            .create_group(CreateGroupRequest {
                name: "queued artifact".into(),
                description: String::new(),
                members: vec![],
                required_features: vec![],
                app_components: vec![],
                initial_admins: vec![],
            })
            .await
            .unwrap();
        assert!(matches!(created, SendResult::FoundingGroupCreated { .. }));
        group
    }

    async fn queue_text(engine: &mut Engine<SqliteAccountStorage>, group: &GroupId) -> MessageId {
        let payload = MarmotAppEvent::new(
            hex::encode(engine.self_id().as_slice()),
            1_700_000_000,
            MARMOT_APP_EVENT_KIND_CHAT,
            vec![],
            "durable queued text",
        )
        .encode()
        .unwrap();
        let SendResult::Queued { intent_id, .. } = engine
            .queue_app_message(group.clone(), payload)
            .await
            .unwrap()
        else {
            panic!("queued text");
        };
        intent_id
    }

    fn stage(
        engine: &Engine<SqliteAccountStorage>,
        result: &SendResult,
        group: &GroupId,
    ) -> OutboundFanout {
        let (msg, pending) = match result {
            SendResult::ApplicationMessage { msg, .. } | SendResult::Proposal { msg } => {
                (msg, None)
            }
            SendResult::GroupEvolution { msg, pending, .. } => (msg, Some(*pending)),
            _ => panic!("publishable artifact"),
        };
        let TransportEnvelope::GroupMessage { transport_group_id } = &msg.envelope else {
            panic!("group route");
        };
        let mut fanout = OutboundFanout::stage(
            TransportPublishRequest {
                account_id: engine.self_id().clone(),
                message: msg.clone(),
                target: TransportPublishTarget::Group {
                    group_id: group.clone(),
                    transport_group_id: transport_group_id.clone(),
                    endpoints: vec![TransportEndpoint::from("wss://synthetic.example")],
                },
                required_acks: 1,
            },
            pending,
            Some(group.clone()),
            1_700_000_000_000,
        )
        .unwrap();
        if let SendResult::ApplicationMessage {
            app_event_id,
            source_epoch,
            retention,
            authority,
            ..
        } = result
        {
            fanout
                .set_application_message(OutboundApplicationMessage {
                    group_id: group.clone(),
                    app_event_id: app_event_id.clone(),
                    source_epoch: *source_epoch,
                    retention: *retention,
                    authority: *authority,
                })
                .unwrap();
        }
        fanout
    }

    fn prepare(
        storage: &SqliteAccountStorage,
        engine: &mut Engine<SqliteAccountStorage>,
        group: &GroupId,
    ) -> Vec<SendResult> {
        storage
            .with_transaction(|storage| {
                let results = futures::executor::block_on(
                    engine.converge_and_drain_queued_outbound_intents(group, 1_700_000_001_000),
                )?;
                for result in &results {
                    storage.put_outbound_fanout(&stage(engine, result, group))?;
                }
                Ok::<_, EngineError>(results)
            })
            .unwrap()
    }

    #[tokio::test]
    async fn application_binding_and_ambiguous_exact_fanout_survive_restart() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        let intent = queue_text(&mut original, &group).await;
        let results = prepare(&storage, &mut original, &group);
        let [SendResult::ApplicationMessage { msg, .. }] = results.as_slice() else {
            panic!("one application");
        };
        let exact = msg.clone();
        let mut fanout = storage.outbound_fanout(&exact.id).unwrap().unwrap();
        fanout
            .mark_attempt_started_at(0, 1_700_000_001_100)
            .unwrap();
        storage.put_outbound_fanout(&fanout).unwrap();
        fanout
            .record_target_failure(
                0,
                TransportEndpointFailure {
                    endpoint: fanout.request().target.endpoints()[0].clone(),
                    reason: "acknowledgment unknown".into(),
                    kind: TransportEndpointFailureKind::PossiblyExposed,
                    rejection_category: None,
                },
            )
            .unwrap();
        storage.put_outbound_fanout(&fanout).unwrap();
        drop(original);
        let mut reopened = engine(&storage);
        reopened.hydrate_all_stored_groups().unwrap();
        assert_eq!(
            reopened.regenerated_queued_intent_for_message(&exact.id),
            Some((group.clone(), intent.clone()))
        );
        assert!(
            reopened
                .converge_and_drain_queued_outbound_intents(&group, 1_700_000_002_000)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage
                .outbound_fanout(&exact.id)
                .unwrap()
                .unwrap()
                .request()
                .message,
            exact
        );
        assert_eq!(
            storage.list_queued_outbound_intents(&group).unwrap().len(),
            1
        );
        let mut acknowledged = storage.outbound_fanout(&exact.id).unwrap().unwrap();
        acknowledged
            .mark_attempt_started_at(0, 1_700_000_002_100)
            .unwrap();
        storage.put_outbound_fanout(&acknowledged).unwrap();
        acknowledged.mark_target_accepted(0).unwrap();
        storage.put_outbound_fanout(&acknowledged).unwrap();
        reopened.confirm_queued_outbound_intent(&intent).unwrap();
        assert!(
            storage
                .list_queued_outbound_intents(&group)
                .unwrap()
                .is_empty()
        );
        queue_text(&mut reopened, &group).await;
        assert_eq!(prepare(&storage, &mut reopened, &group).len(), 1);
    }

    #[tokio::test]
    async fn pending_binding_restores_existing_origin_reference_and_ack_retires_row() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        let row = original
            .prepare_queued_outbound_intent(
                group.clone(),
                SendIntent::SelfUpdate {
                    group_id: group.clone(),
                },
                0,
            )
            .unwrap();
        storage.put_queued_outbound_intent(&row).unwrap();
        let results = prepare(&storage, &mut original, &group);
        let [SendResult::GroupEvolution { msg, pending, .. }] = results.as_slice() else {
            panic!("one evolution");
        };
        let origin = msg.id.clone();
        let pending = *pending;
        drop(original);
        let mut reopened = engine(&storage);
        reopened.hydrate_all_stored_groups().unwrap();
        assert_eq!(
            reopened.pending_origin_message_id(pending).unwrap(),
            origin.clone()
        );
        assert_eq!(
            reopened.queued_intent_by_pending.get(&pending),
            Some(&(group.clone(), row.id))
        );
        let mut fanout = storage.outbound_fanout(&origin).unwrap().unwrap();
        fanout
            .mark_attempt_started_at(0, 1_700_000_001_100)
            .unwrap();
        storage.put_outbound_fanout(&fanout).unwrap();
        fanout.mark_target_accepted(0).unwrap();
        storage.put_outbound_fanout(&fanout).unwrap();
        reopened
            .confirm_published_fanout(pending, &mut fanout)
            .await
            .unwrap();
        assert!(
            storage
                .list_queued_outbound_intents(&group)
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn missing_fanout_blocks_restart_without_erasing_or_regenerating_binding() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        queue_text(&mut original, &group).await;
        let results = prepare(&storage, &mut original, &group);
        let [SendResult::ApplicationMessage { msg, .. }] = results.as_slice() else {
            panic!("one application");
        };
        let bound = storage.list_queued_outbound_intents(&group).unwrap();
        storage.delete_outbound_fanout(&msg.id).unwrap();
        drop(original);
        let mut reopened = engine(&storage);
        reopened.hydrate_all_stored_groups().unwrap();
        assert!(
            reopened
                .converge_and_drain_queued_outbound_intents(&group, 1_700_000_002_000)
                .await
                .is_err()
        );
        assert_eq!(storage.list_queued_outbound_intents(&group).unwrap(), bound);
    }

    #[tokio::test]
    async fn outer_rollback_restores_unprepared_row_and_reopen_can_prepare() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        queue_text(&mut original, &group).await;
        let before = storage.list_queued_outbound_intents(&group).unwrap();
        let result = storage.with_transaction(|storage| {
            let results = futures::executor::block_on(
                original.converge_and_drain_queued_outbound_intents(&group, 1_700_000_001_000),
            )?;
            assert_eq!(results.len(), 1);
            for result in &results {
                storage.put_outbound_fanout(&stage(&original, result, &group))?;
            }
            Err::<(), EngineError>(
                StorageError::Backend("synthetic enclosing rollback".into()).into(),
            )
        });
        assert!(result.is_err());
        drop(original);
        assert_eq!(
            storage.list_queued_outbound_intents(&group).unwrap(),
            before
        );
        assert!(
            storage
                .list_outbound_fanouts_for_group(&group)
                .unwrap()
                .is_empty()
        );
        let mut reopened = engine(&storage);
        reopened.hydrate_all_stored_groups().unwrap();
        assert_eq!(prepare(&storage, &mut reopened, &group).len(), 1);
    }

    #[tokio::test]
    async fn bound_artifact_kind_group_and_duplicate_ownership_are_checked() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        queue_text(&mut original, &group).await;
        let results = prepare(&storage, &mut original, &group);
        let [SendResult::ApplicationMessage { msg, .. }] = results.as_slice() else {
            panic!("one application");
        };
        let bound = storage
            .list_queued_outbound_intents(&group)
            .unwrap()
            .remove(0);
        let mut mismatched_kind = bound.clone();
        mismatched_kind.intent = SendIntent::SelfUpdate {
            group_id: group.clone(),
        };
        storage
            .put_queued_outbound_intent(&mismatched_kind)
            .unwrap();
        assert!(matches!(
            original.restore_queued_artifact_bindings(&group),
            Err(EngineError::QueuedIntentRecoveryFailed)
        ));
        assert_eq!(
            storage.list_queued_outbound_intents(&group).unwrap(),
            vec![mismatched_kind]
        );
        storage.put_queued_outbound_intent(&bound).unwrap();
        let mut mismatched_group = bound.clone();
        mismatched_group.intent = SendIntent::SelfUpdate {
            group_id: GroupId::new(vec![9; 32]),
        };
        storage
            .put_queued_outbound_intent(&mismatched_group)
            .unwrap();
        assert!(matches!(
            original.restore_queued_artifact_bindings(&group),
            Err(EngineError::QueuedIntentRecoveryFailed)
        ));
        storage.put_queued_outbound_intent(&bound).unwrap();
        let mut duplicate = bound.clone();
        duplicate.id = MessageId::new(vec![8; 32]);
        storage.put_queued_outbound_intent(&duplicate).unwrap();
        assert!(matches!(
            original.restore_queued_artifact_bindings(&group),
            Err(EngineError::QueuedIntentRecoveryFailed)
        ));
        assert_eq!(
            storage.list_queued_outbound_intents(&group).unwrap().len(),
            2
        );
        assert_eq!(
            storage
                .outbound_fanout(&msg.id)
                .unwrap()
                .unwrap()
                .request()
                .message,
            *msg
        );
        storage
            .delete_queued_outbound_intent(&duplicate.id)
            .unwrap();
        original.restore_queued_artifact_bindings(&group).unwrap();
    }

    #[tokio::test]
    async fn started_ambiguous_and_acknowledged_attempts_cannot_reissue() {
        for outcome in ["started", "ambiguous", "accepted"] {
            let storage = SqliteAccountStorage::in_memory().unwrap();
            let mut original = engine(&storage);
            let group = group(&mut original).await;
            let intent = queue_text(&mut original, &group).await;
            let results = prepare(&storage, &mut original, &group);
            let [SendResult::ApplicationMessage { msg, .. }] = results.as_slice() else {
                panic!("one application");
            };
            let mut fanout = storage.outbound_fanout(&msg.id).unwrap().unwrap();
            fanout
                .mark_attempt_started_at(0, 1_700_000_001_100)
                .unwrap();
            storage.put_outbound_fanout(&fanout).unwrap();
            match outcome {
                "ambiguous" => {
                    fanout
                        .record_target_failure(
                            0,
                            TransportEndpointFailure {
                                endpoint: fanout.request().target.endpoints()[0].clone(),
                                reason: "unknown acknowledgment".into(),
                                kind: TransportEndpointFailureKind::PossiblyExposed,
                                rejection_category: None,
                            },
                        )
                        .unwrap();
                }
                "accepted" => {
                    fanout.mark_target_accepted(0).unwrap();
                }
                _ => {}
            }
            storage.put_outbound_fanout(&fanout).unwrap();
            let before = storage.list_queued_outbound_intents(&group).unwrap();
            drop(original);
            let mut reopened = engine(&storage);
            reopened.hydrate_all_stored_groups().unwrap();
            assert!(
                matches!(
                    reopened.retry_queued_outbound_intent(&group, &intent),
                    Err(EngineError::QueuedIntentReissueRefused)
                ),
                "{outcome}"
            );
            assert_eq!(
                storage.list_queued_outbound_intents(&group).unwrap(),
                before
            );
            assert_eq!(storage.outbound_fanout(&msg.id).unwrap().unwrap(), fanout);
        }
    }

    #[tokio::test]
    async fn proven_unexposed_reissue_retires_only_old_artifact_and_prepares_once() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        let intent = queue_text(&mut original, &group).await;
        let results = prepare(&storage, &mut original, &group);
        let [SendResult::ApplicationMessage { msg, .. }] = results.as_slice() else {
            panic!("one application");
        };
        let old_id = msg.id.clone();
        let old_row = storage
            .list_queued_outbound_intents(&group)
            .unwrap()
            .remove(0);
        let mut fanout = storage.outbound_fanout(&old_id).unwrap().unwrap();
        fanout
            .mark_attempt_started_at(0, 1_700_000_001_100)
            .unwrap();
        storage.put_outbound_fanout(&fanout).unwrap();
        fanout
            .record_target_failure(
                0,
                TransportEndpointFailure {
                    endpoint: fanout.request().target.endpoints()[0].clone(),
                    reason: "proved no exposure".into(),
                    kind: TransportEndpointFailureKind::NotExposed,
                    rejection_category: None,
                },
            )
            .unwrap();
        storage.put_outbound_fanout(&fanout).unwrap();
        original
            .retry_queued_outbound_intent(&group, &intent)
            .unwrap();
        let reset = storage
            .list_queued_outbound_intents(&group)
            .unwrap()
            .remove(0);
        assert_eq!(reset.preparation, QueuedIntentPreparation::Unprepared {});
        assert_eq!(reset.id, old_row.id);
        assert_eq!(reset.intent, old_row.intent);
        assert!(storage.outbound_fanout(&old_id).unwrap().is_none());
        drop(original);
        let mut reopened = engine(&storage);
        reopened.hydrate_all_stored_groups().unwrap();
        let regenerated = prepare(&storage, &mut reopened, &group);
        let [SendResult::ApplicationMessage { msg, .. }] = regenerated.as_slice() else {
            panic!("one new application");
        };
        assert_ne!(msg.id, old_id);
        assert!(
            reopened
                .converge_and_drain_queued_outbound_intents(&group, 1_700_000_002_000)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            storage
                .list_outbound_fanouts_for_group(&group)
                .unwrap()
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn pending_ambiguity_refuses_rollback_and_proven_unexposed_rearms_durably() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        let row = original
            .prepare_queued_outbound_intent(
                group.clone(),
                SendIntent::SelfUpdate {
                    group_id: group.clone(),
                },
                0,
            )
            .unwrap();
        storage.put_queued_outbound_intent(&row).unwrap();
        let results = prepare(&storage, &mut original, &group);
        let [SendResult::GroupEvolution { msg, pending, .. }] = results.as_slice() else {
            panic!("one evolution");
        };
        let origin = msg.id.clone();
        let pending = *pending;
        let before = storage.list_queued_outbound_intents(&group).unwrap();
        let mut attempt = storage.outbound_fanout(&origin).unwrap().unwrap();
        attempt
            .mark_attempt_started_at(0, 1_700_000_001_100)
            .unwrap();
        storage.put_outbound_fanout(&attempt).unwrap();
        assert!(matches!(
            original.publish_failed(pending).await,
            Err(EngineError::QueuedIntentReissueRefused)
        ));
        assert_eq!(
            storage.list_queued_outbound_intents(&group).unwrap(),
            before
        );
        assert_eq!(original.pending_origin_message_id(pending).unwrap(), origin);
        attempt
            .record_target_failure(
                0,
                TransportEndpointFailure {
                    endpoint: attempt.request().target.endpoints()[0].clone(),
                    reason: "proved no exposure".into(),
                    kind: TransportEndpointFailureKind::NotExposed,
                    rejection_category: None,
                },
            )
            .unwrap();
        storage.put_outbound_fanout(&attempt).unwrap();
        original.publish_failed(pending).await.unwrap();
        assert_eq!(
            storage.list_queued_outbound_intents(&group).unwrap()[0].preparation,
            QueuedIntentPreparation::Unprepared {}
        );
        assert!(matches!(
            storage
                .outbound_fanout(&origin)
                .unwrap()
                .unwrap()
                .mls_state(),
            cgka_traits::FanoutMlsState::RolledBack
        ));
        assert!(original.pending_origin_message_id(pending).is_err());
        drop(original);
        let mut reopened = engine(&storage);
        reopened.hydrate_all_stored_groups().unwrap();
        let regenerated = prepare(&storage, &mut reopened, &group);
        let [SendResult::GroupEvolution { msg, .. }] = regenerated.as_slice() else {
            panic!("one authorized evolution");
        };
        assert_ne!(msg.id, origin);
    }

    #[tokio::test]
    async fn encrypted_connection_reopen_retains_started_artifact_and_exact_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queued.sqlite");
        let key = storage_sqlite::SqlCipherKey::new("synthetic queued-artifact test key").unwrap();
        let storage = SqliteAccountStorage::open_encrypted(&path, &key).unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        let intent = queue_text(&mut original, &group).await;
        let results = prepare(&storage, &mut original, &group);
        let [SendResult::ApplicationMessage { msg, .. }] = results.as_slice() else {
            panic!("one application");
        };
        let exact = msg.clone();
        let mut fanout = storage.outbound_fanout(&exact.id).unwrap().unwrap();
        fanout
            .mark_attempt_started_at(0, 1_700_000_001_100)
            .unwrap();
        storage.put_outbound_fanout(&fanout).unwrap();
        let bound = storage.list_queued_outbound_intents(&group).unwrap();
        drop(original);
        storage.close().unwrap();
        drop(storage);
        let reopened_storage = SqliteAccountStorage::open_encrypted(&path, &key).unwrap();
        let mut reopened = engine(&reopened_storage);
        reopened.hydrate_all_stored_groups().unwrap();
        assert_eq!(
            reopened.regenerated_queued_intent_for_message(&exact.id),
            Some((group.clone(), intent))
        );
        assert!(
            reopened
                .converge_and_drain_queued_outbound_intents(&group, 1_700_000_002_000)
                .await
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            reopened_storage
                .list_queued_outbound_intents(&group)
                .unwrap(),
            bound
        );
        assert_eq!(
            reopened_storage
                .outbound_fanout(&exact.id)
                .unwrap()
                .unwrap(),
            fanout
        );
        drop(reopened);
        reopened_storage.close().unwrap();
    }

    #[tokio::test]
    async fn enclosing_rollback_restores_pending_binding_and_protocol_authority() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        let row = original
            .prepare_queued_outbound_intent(
                group.clone(),
                SendIntent::SelfUpdate {
                    group_id: group.clone(),
                },
                0,
            )
            .unwrap();
        storage.put_queued_outbound_intent(&row).unwrap();
        let results = prepare(&storage, &mut original, &group);
        let [SendResult::GroupEvolution { msg, pending, .. }] = results.as_slice() else {
            panic!("one evolution");
        };
        let origin = msg.id.clone();
        let pending = *pending;
        let mut unexposed = storage.outbound_fanout(&origin).unwrap().unwrap();
        unexposed
            .mark_attempt_started_at(0, 1_700_000_001_100)
            .unwrap();
        storage.put_outbound_fanout(&unexposed).unwrap();
        unexposed
            .record_target_failure(
                0,
                TransportEndpointFailure {
                    endpoint: unexposed.request().target.endpoints()[0].clone(),
                    reason: "no external send".into(),
                    kind: TransportEndpointFailureKind::NotExposed,
                    rejection_category: None,
                },
            )
            .unwrap();
        storage.put_outbound_fanout(&unexposed).unwrap();
        let before = storage.list_queued_outbound_intents(&group).unwrap();
        let result = storage.with_transaction(|_| {
            futures::executor::block_on(original.publish_failed(pending))?;
            Err::<(), EngineError>(StorageError::Backend("synthetic outer rollback".into()).into())
        });
        assert!(result.is_err());
        drop(original);
        assert_eq!(
            storage.list_queued_outbound_intents(&group).unwrap(),
            before
        );
        assert_eq!(
            storage.outbound_fanout(&origin).unwrap().unwrap(),
            unexposed
        );
        let mut reopened = engine(&storage);
        reopened.hydrate_all_stored_groups().unwrap();
        assert_eq!(reopened.pending_origin_message_id(pending).unwrap(), origin);
        assert_eq!(
            reopened.queued_intent_by_pending.get(&pending),
            Some(&(group, row.id))
        );
    }
    #[tokio::test]
    async fn pending_binding_refuses_a_mirror_epoch_that_disagrees_with_wire() {
        let storage = SqliteAccountStorage::in_memory().unwrap();
        let mut original = engine(&storage);
        let group = group(&mut original).await;
        let row = original
            .prepare_queued_outbound_intent(
                group.clone(),
                SendIntent::SelfUpdate {
                    group_id: group.clone(),
                },
                0,
            )
            .unwrap();
        storage.put_queued_outbound_intent(&row).unwrap();
        let prepared = prepare(&storage, &mut original, &group);
        let [SendResult::GroupEvolution { msg, pending, .. }] = prepared.as_slice() else {
            panic!("one pending commit");
        };
        let before = storage.list_queued_outbound_intents(&group).unwrap();
        let exact_fanout = storage.outbound_fanout(&msg.id).unwrap().unwrap();
        let mut stored = storage.get_message(&msg.id).unwrap();
        stored.epoch = cgka_traits::types::EpochId(stored.epoch.0 + 1);
        storage.put_message(&stored).unwrap();
        assert!(matches!(
            original.checked_queued_artifact_bindings(&group),
            Err(EngineError::QueuedIntentRecoveryFailed)
        ));
        assert_eq!(
            original.pending_origin_message_id(*pending).unwrap(),
            msg.id
        );
        assert_eq!(
            storage.list_queued_outbound_intents(&group).unwrap(),
            before
        );
        assert_eq!(
            storage.outbound_fanout(&msg.id).unwrap().unwrap(),
            exact_fanout
        );
    }
}
