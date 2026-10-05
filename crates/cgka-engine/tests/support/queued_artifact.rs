/// Host staging required before another queued drain or an external send.
/// These engine fixtures use a pass-through peeler and one synthetic endpoint;
/// they do not qualify Nostr interop. Production wraps preparation and staging
/// in its enclosing storage transaction before releasing artifacts.
pub fn stage_queued_results<S: cgka_traits::storage::StorageProvider>(
    engine: &dyn cgka_traits::engine::CgkaEngine,
    storage: &S,
    group: &cgka_traits::types::GroupId,
    results: &[cgka_traits::engine::SendResult],
) {
    use cgka_traits::engine::SendResult;
    use cgka_traits::transport::TransportEnvelope;
    use cgka_traits::{
        OutboundApplicationMessage, OutboundFanout, TransportEndpoint, TransportPublishRequest,
        TransportPublishTarget,
    };
    for result in results {
        let (message, pending, welcomes) = match result {
            SendResult::ApplicationMessage { msg, .. } | SendResult::Proposal { msg } => {
                (msg, None, vec![])
            }
            SendResult::GroupEvolution {
                msg,
                pending,
                welcomes,
            } => (msg, Some(*pending), welcomes.clone()),
            _ => continue,
        };
        let TransportEnvelope::GroupMessage { transport_group_id } = &message.envelope else {
            panic!("group artifact");
        };
        let mut fanout = OutboundFanout::stage_with_post_confirmation_welcomes(
            TransportPublishRequest {
                account_id: engine.self_id(),
                message: message.clone(),
                target: TransportPublishTarget::Group {
                    group_id: group.clone(),
                    transport_group_id: transport_group_id.clone(),
                    endpoints: vec![TransportEndpoint::from("synthetic://engine-fixture")],
                },
                required_acks: 1,
            },
            pending,
            Some(group.clone()),
            0,
            pending.map(|_| message.id.clone()),
            pending.map(|_| cgka_traits::FanoutPendingKind::GroupEvolution),
            welcomes,
        )
        .unwrap();
        if let SendResult::ApplicationMessage {
            group_id,
            app_event_id,
            source_epoch,
            retention,
            authority,
            ..
        } = result
        {
            fanout
                .set_application_message(OutboundApplicationMessage {
                    group_id: group_id.clone(),
                    app_event_id: app_event_id.clone(),
                    source_epoch: *source_epoch,
                    retention: *retention,
                    authority: *authority,
                })
                .unwrap();
        }
        storage.put_outbound_fanout(&fanout).unwrap();
    }
}

pub fn mark_queued_results_unexposed<S: cgka_traits::storage::StorageProvider>(
    storage: &S,
    results: &[cgka_traits::engine::SendResult],
) {
    use cgka_traits::engine::SendResult;
    use cgka_traits::{TransportEndpointFailure, TransportEndpointFailureKind};
    for result in results {
        let message = match result {
            SendResult::ApplicationMessage { msg, .. }
            | SendResult::Proposal { msg }
            | SendResult::GroupEvolution { msg, .. } => msg,
            _ => continue,
        };
        let mut fanout = storage.outbound_fanout(&message.id).unwrap().unwrap();
        assert!(!fanout.possible_exposure());
        assert_eq!(fanout.outcome().accepted_targets, 0);
        for index in 0..fanout.request().target.endpoints().len() {
            fanout.mark_attempt_started(index).unwrap();
            storage.put_outbound_fanout(&fanout).unwrap();
            fanout
                .record_target_failure(
                    index,
                    TransportEndpointFailure {
                        endpoint: fanout.request().target.endpoints()[index].clone(),
                        reason: "fixture performed no external send".into(),
                        kind: TransportEndpointFailureKind::NotExposed,
                        rejection_category: None,
                    },
                )
                .unwrap();
            storage.put_outbound_fanout(&fanout).unwrap();
        }
    }
}
