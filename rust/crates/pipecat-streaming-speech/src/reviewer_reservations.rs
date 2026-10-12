// Included by cartesia_turns_processor::eager_reservation_tests.
use futures_util::{
    SinkExt as ReviewerReservationSinkExt, StreamExt as ReviewerReservationStreamExt,
};
use tokio::net::{
    TcpListener as ReviewerReservationListener, TcpStream as ReviewerReservationStream,
};
use tokio_tungstenite::{
    accept_async as reviewer_reservation_accept,
    tungstenite::Message as ReviewerReservationMessage,
    WebSocketStream as ReviewerReservationSocket,
};

async fn reviewer_reservation_within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(std::time::Duration::from_secs(3), future)
        .await
        .expect("reviewer reservation deadline")
}

async fn reviewer_reservation_listener(
    processor: &CartesiaTurnsProcessor,
) -> ReviewerReservationListener {
    let listener = ReviewerReservationListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let mut config = processor.config_snapshot().unwrap();
    config.endpoint = format!("ws://{}/turns", listener.local_addr().unwrap());
    config.limits.connect_timeout = std::time::Duration::from_secs(2);
    config.limits.close_timeout = std::time::Duration::from_millis(100);
    processor.state.lock().unwrap().client = CartesiaTurnsClient::new(config).unwrap();
    listener
}

async fn reviewer_reservation_peer(
    listener: &ReviewerReservationListener,
    acknowledge: bool,
) -> ReviewerReservationSocket<ReviewerReservationStream> {
    let (stream, _) = listener.accept().await.unwrap();
    let mut peer = reviewer_reservation_accept(stream).await.unwrap();
    if acknowledge {
        peer.send(ReviewerReservationMessage::Text(
            serde_json::json!({"type":"connected"}).to_string().into(),
        ))
        .await
        .unwrap();
    } else {
        // A Pong proves the client has entered its provider-readiness loop;
        // completing only the server's upgrade would not prove that boundary.
        let marker = b"connected acknowledgement withheld".to_vec();
        peer.send(ReviewerReservationMessage::Ping(marker.clone().into()))
            .await
            .unwrap();
        match peer.next().await {
            Some(Ok(ReviewerReservationMessage::Pong(bytes)))
                if bytes.as_ref() == marker.as_slice() => {}
            other => panic!("expected readiness-loop Pong, received {other:?}"),
        }
    }
    peer
}

fn reviewer_reservation_prediction(
    source: &Arc<EagerSession>,
) -> (
    pipecat_core::NativeEagerTurn,
    pipecat_core::NativeEagerPrediction,
    NativeEagerConsumer,
    SpeculationOwner,
) {
    assert!(source.claim_start());
    let turn = source.begin_start().unwrap().unwrap();
    assert!(source.mark_ready(&turn));
    let prediction = source
        .predict(
            &turn,
            FrameData::EagerTranscription(TranscriptionFrame {
                text: "reservation publication".into(),
                user_id: "reviewer".into(),
                timestamp: "fixed".into(),
                ..Default::default()
            }),
        )
        .unwrap()
        .unwrap();
    let consumer = NativeEagerConsumer::new();
    assert!(turn.claim_start(&consumer));
    let owner = SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap();
    (turn, prediction, consumer, owner)
}

async fn reviewer_reservation_ping(
    peer: &mut ReviewerReservationSocket<ReviewerReservationStream>,
) -> bool {
    let marker = b"current reservation still admitted".to_vec();
    reviewer_reservation_within(peer.send(ReviewerReservationMessage::Ping(marker.clone().into())))
        .await
        .unwrap();
    matches!(reviewer_reservation_within(peer.next()).await,
        Some(Ok(ReviewerReservationMessage::Pong(bytes))) if bytes.as_ref() == marker.as_slice())
}

struct ReviewerReservationWake(AtomicU64);
impl Wake for ReviewerReservationWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn reviewer_polled_connection_drop_clears_busy_and_allows_real_retry() {
    let (processor, worker, ctx) = fixture(true).await;
    let listener = reviewer_reservation_listener(&processor).await;
    let setup = processor.setup_gate.lock().await;
    let reservation = processor
        .reserve_initial(ctx.configuration(), &ctx, ctx.health().generation())
        .unwrap();
    let old_cancel = reservation.cancel.clone();
    let old_barrier = reservation.barrier_cancel.clone();
    let mut connecting = Box::pin(processor.connect(&ctx, false, reservation, setup));
    let held_peer = reviewer_reservation_within(async {
        tokio::select! {
            result = &mut connecting => panic!("connection ended before held acknowledgement: {result:?}"),
            peer = reviewer_reservation_peer(&listener, false) => peer,
        }
    }).await;
    let old_source = processor.state.lock().unwrap().eager.clone().unwrap();
    let (old_turn, old_prediction, _, old_owner) = reviewer_reservation_prediction(&old_source);
    let capability = old_owner.capability();
    let mut watch = capability.watch();
    let mut changed = Box::pin(watch.changed());
    let probe = Arc::new(ReviewerReservationWake(AtomicU64::new(0)));
    let waker = Waker::from(probe.clone());
    let watcher_pending = changed
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending();

    drop(connecting);
    let cleared = {
        let state = processor.state.lock().unwrap();
        !state.connecting
            && state.cancel.is_none()
            && state.barrier_cancel.is_none()
            && state.sender.is_none()
            && state.eager.is_none()
    };
    let old_retired = !old_turn.is_live()
        && !old_prediction.is_live()
        && !capability.admits_completion()
        && old_cancel.is_cancelled()
        && old_barrier.is_cancelled();
    let notified = probe.0.load(Ordering::Acquire) > 0;
    let locks_released = processor.setup_gate.try_lock().is_ok()
        && processor.emission.try_lock().is_ok()
        && processor.state.try_lock().is_ok();
    drop(changed);
    drop(held_peer);

    let (retry, mut peer) = reviewer_reservation_within(async {
        tokio::join!(
            processor.setup(ctx.configuration(), &ctx),
            reviewer_reservation_peer(&listener, true),
        )
    })
    .await;
    let (current_source, current_ready) = {
        let state = processor.state.lock().unwrap();
        (
            state.eager.clone().unwrap(),
            !state.connecting
                && state
                    .sender
                    .as_ref()
                    .is_some_and(|sender| !sender.is_closed()),
        )
    };
    let real_retry_admitted = reviewer_reservation_ping(&mut peer).await;
    let reader_drop = current_source.watch_reader_drop();
    let actor_return = current_source.watch_actor_return();
    let cleanup = processor.cancel_current(false);
    let actor_result = reviewer_reservation_within(actor_return).await;
    let reader_result = reviewer_reservation_within(reader_drop).await;
    drop(peer);
    reviewer_reservation_within(finish(worker)).await;

    assert!(watcher_pending && cleared && old_retired && notified && locks_released);
    assert!(retry.is_ok() && current_ready && real_retry_admitted);
    assert!(cleanup.is_ok());
    assert_eq!(actor_result, Ok(false));
    assert_eq!(reader_result, Ok(false));
}

#[tokio::test]
async fn reviewer_stale_polled_connection_drop_preserves_connected_replacement() {
    let (processor, worker, ctx) = fixture(true).await;
    let listener = reviewer_reservation_listener(&processor).await;
    let setup = processor.setup_gate.lock().await;
    let reservation = processor
        .reserve_initial(ctx.configuration(), &ctx, ctx.health().generation())
        .unwrap();
    let old_cancel = reservation.cancel.clone();
    let mut stale = Box::pin(processor.connect(&ctx, false, reservation, setup));
    let held_peer = reviewer_reservation_within(async {
        tokio::select! {
            result = &mut stale => panic!("connection ended before held acknowledgement: {result:?}"),
            peer = reviewer_reservation_peer(&listener, false) => peer,
        }
    }).await;
    let old_source = processor.state.lock().unwrap().eager.clone().unwrap();
    let (old_turn, old_prediction, _, old_owner) = reviewer_reservation_prediction(&old_source);
    let retired_old = processor.cancel_current(false);

    let (replacement, mut peer) = reviewer_reservation_within(async {
        tokio::join!(
            processor.setup(ctx.configuration(), &ctx),
            reviewer_reservation_peer(&listener, true),
        )
    })
    .await;
    let (current_source, current_serial, current_cancel) = {
        let state = processor.state.lock().unwrap();
        (
            state.eager.clone().unwrap(),
            state.serial,
            state.cancel.clone().unwrap(),
        )
    };
    let (current_turn, current_prediction, consumer, owner) =
        reviewer_reservation_prediction(&current_source);

    // The old future still owns both real handshake and reservation guards.
    // Its destruction occurs only after the replacement socket is fully ready.
    drop(stale);
    drop(held_peer);
    let preserved = {
        let state = processor.state.lock().unwrap();
        state.serial == current_serial
            && !state.connecting
            && !current_cancel.is_cancelled()
            && state
                .sender
                .as_ref()
                .is_some_and(|sender| !sender.is_closed())
            && state
                .eager
                .as_ref()
                .is_some_and(|source| Arc::ptr_eq(source, &current_source))
    };
    let source_isolated = old_cancel.is_cancelled()
        && !old_turn.is_live()
        && !old_prediction.is_live()
        && !old_owner.capability().admits_completion()
        && current_turn.is_live()
        && current_prediction.is_live();
    let transport_isolated = reviewer_reservation_ping(&mut peer).await;
    let publication_isolated = current_source
        .offer_final(
            &current_turn,
            FrameData::Transcription(TranscriptionFrame {
                text: "reservation publication".into(),
                user_id: "reviewer".into(),
                timestamp: "fixed".into(),
                finalized: true,
                ..Default::default()
            }),
        )
        .ok()
        .flatten()
        .is_some_and(|commit| {
            commit.claim_final(&consumer)
                && commit.claim_stop(&consumer)
                && owner.confirm_source_with_context(
                    &commit,
                    &consumer,
                    pipecat_core::LlmContextData {
                        messages: vec![],
                        tools: pipecat_core::ToolSet::Functions(vec![]),
                        tool_choice: None,
                    },
                )
                && owner.capability().release_after_boundary()
        });
    let reader_drop = current_source.watch_reader_drop();
    let actor_return = current_source.watch_actor_return();
    let cleanup = processor.cancel_current(false);
    let actor_result = reviewer_reservation_within(actor_return).await;
    let reader_result = reviewer_reservation_within(reader_drop).await;
    drop(peer);
    reviewer_reservation_within(finish(worker)).await;

    assert!(retired_old.is_ok() && replacement.is_ok());
    assert!(preserved && source_isolated && transport_isolated && publication_isolated);
    assert!(cleanup.is_ok());
    assert_eq!(actor_result, Ok(false));
    assert_eq!(reader_result, Ok(false));
}
