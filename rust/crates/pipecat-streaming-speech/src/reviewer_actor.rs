// Included by cartesia_turns::eager_actor_tests.
fn reviewer_actor_adopt(
    turn: &NativeEagerTurn,
    prediction: &NativeEagerPrediction,
) -> (
    pipecat_core::NativeEagerConsumer,
    pipecat_core::SpeculationOwner,
) {
    let consumer = pipecat_core::NativeEagerConsumer::new();
    assert!(turn.claim_start(&consumer));
    let owner = pipecat_core::SpeculationOwner::try_adopt_source_prediction(prediction, &consumer)
        .expect("fixture prediction admission");
    (consumer, owner)
}

fn reviewer_actor_context() -> pipecat_core::LlmContextData {
    pipecat_core::LlmContextData {
        messages: vec![],
        tools: pipecat_core::ToolSet::Functions(vec![]),
        tool_choice: None,
    }
}

fn reviewer_actor_confirm(
    source: &Arc<EagerSession>,
    turn: &NativeEagerTurn,
    consumer: &pipecat_core::NativeEagerConsumer,
    owner: &pipecat_core::SpeculationOwner,
) -> bool {
    source
        .offer_final(turn, payload(true))
        .ok()
        .flatten()
        .is_some_and(|commit| {
            commit.claim_final(consumer)
                && commit.claim_stop(consumer)
                && owner.confirm_source_with_context(&commit, consumer, reviewer_actor_context())
                && owner.capability().release_after_boundary()
        })
}

#[tokio::test]
async fn reviewer_provider_fault_revokes_only_faulting_live_socket() {
    let (failed_source, failed_turn, failed_prediction) = ready_source();
    let (healthy_source, healthy_turn, healthy_prediction) = ready_source();
    let (failed_consumer, failed_owner) = reviewer_actor_adopt(&failed_turn, &failed_prediction);
    let (healthy_consumer, healthy_owner) =
        reviewer_actor_adopt(&healthy_turn, &healthy_prediction);
    let failed_commit = failed_source
        .offer_final(&failed_turn, payload(true))
        .unwrap()
        .unwrap();
    let (close_entered, release_close) = failed_source.hold_actor_close();
    let mut failed = ActorPeer::connect(failed_source).await;
    let mut healthy = ActorPeer::connect(healthy_source.clone()).await;

    failed
        .send(json!({"type":"turn.update","transcript":"occupy one raw slot"}))
        .await;
    let queue_barrier = failed.parsed_barrier().await;
    let queued = failed.session.events.rx.len();
    failed
        .send(json!({"type":"error","message":"isolated provider fault"}))
        .await;
    within(close_entered).await.unwrap();

    let exact_retirement = !failed_turn.is_live()
        && !failed_prediction.is_live()
        && !failed_commit.claim_final(&failed_consumer)
        && !failed_owner.capability().admits_completion();
    let healthy_live = healthy_turn.is_live() && healthy_prediction.is_live();
    let healthy_transport = healthy.parsed_barrier().await;
    let healthy_published = reviewer_actor_confirm(
        &healthy_source,
        &healthy_turn,
        &healthy_consumer,
        &healthy_owner,
    );
    let failed_cleanup_pending = !failed.session.events.task.is_finished();

    let _ = release_close.send(());
    let failed_result = failed.join().await;
    healthy.session.cancel();
    let healthy_result = healthy.join().await;
    drop(failed);
    drop(healthy);

    assert!(queue_barrier && queued == 1 && failed_cleanup_pending);
    assert!(exact_retirement);
    assert!(healthy_live && healthy_transport && healthy_published);
    assert_eq!(
        failed_result,
        Err(StreamingError::Provider { status: None })
    );
    assert_eq!(healthy_result, Err(StreamingError::Cancelled));
}

#[tokio::test]
async fn reviewer_abandoned_successful_close_preserves_other_socket_publication() {
    let (abandoned_source, abandoned_turn, abandoned_prediction) = ready_source();
    let (healthy_source, healthy_turn, healthy_prediction) = ready_source();
    let (_, abandoned_owner) = reviewer_actor_adopt(&abandoned_turn, &abandoned_prediction);
    let (healthy_consumer, healthy_owner) =
        reviewer_actor_adopt(&healthy_turn, &healthy_prediction);
    let (close_entered, release_close) = abandoned_source.hold_actor_close();
    let mut abandoned = ActorPeer::connect(abandoned_source.clone()).await;
    let mut healthy = ActorPeer::connect(healthy_source.clone()).await;

    abandoned_source.prepare_end();
    let close_command = abandoned.finish_command().await;
    abandoned.close_peer().await;
    within(close_entered).await.unwrap();
    let held_after_success = abandoned_turn.is_live()
        && abandoned_prediction.is_live()
        && !abandoned.session.events.task.is_finished();
    abandoned.session.events.task.abort();
    let abandoned_result = abandoned.join().await;
    let _ = release_close.send(());

    let only_abandoned_revoked = !abandoned_turn.is_live()
        && !abandoned_prediction.is_live()
        && !abandoned_owner.capability().admits_completion()
        && healthy_turn.is_live()
        && healthy_prediction.is_live();
    let healthy_transport = healthy.parsed_barrier().await;
    let healthy_published = reviewer_actor_confirm(
        &healthy_source,
        &healthy_turn,
        &healthy_consumer,
        &healthy_owner,
    );
    healthy.session.cancel();
    let healthy_result = healthy.join().await;
    drop(abandoned);
    drop(healthy);

    assert!(close_command && held_after_success);
    assert_eq!(abandoned_result, Err(StreamingError::Transport));
    assert!(only_abandoned_revoked && healthy_transport && healthy_published);
    assert_eq!(healthy_result, Err(StreamingError::Cancelled));
}
