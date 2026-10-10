//! Conversation-turn boundaries, deadlines, loss, and pipeline ownership.
use pipecat_core::*;
use pipecat_observers::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

fn event(sequence: u64, kind: &'static str, at: u64) -> ObservationEvent {
    ObservationEvent {
        sequence,
        dropped_before: 0,
        observation: Arc::new(PipelineObservation::Push {
            source: ObservedProcessor {
                runtime_id: 1,
                index: 0,
                name: "turn-fixture".into(),
            },
            destination: None,
            frame: ObservedFrame {
                token: None,
                id: sequence,
                kind,
                interruptible: true,
                vad_stop_secs: None,
                metrics: vec![],
                omitted_metrics: 0,
            },
            direction: Direction::Downstream,
            first_push: true,
            timestamp_ns: at,
        }),
    }
}
async fn send(observer: &TurnTrackingObserver, seq: u64, kind: &'static str, at: u64) {
    observer.on_event(event(seq, kind, at)).await;
}
fn started(number: u64, at: u64) -> TurnTrackingEvent {
    TurnTrackingEvent::Started {
        boundary: None,
        turn_number: number,
        timestamp_ns: at,
        numbering_complete: true,
    }
}
fn ended(
    number: u64,
    begin: u64,
    at: u64,
    interrupted: bool,
    reason: TurnEndReason,
) -> TurnTrackingEvent {
    TurnTrackingEvent::Ended {
        boundary: None,
        turn_number: number,
        started_ns: begin,
        timestamp_ns: at,
        duration_seconds: (at - begin) as f64 / 1e9,
        was_interrupted: interrupted,
        reason,
        numbering_complete: true,
    }
}
fn drain(receiver: &mut broadcast::Receiver<TurnTrackingEvent>) -> Vec<TurnTrackingEvent> {
    let mut out = vec![];
    loop {
        match receiver.try_recv() {
            Ok(event) => out.push(event),
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                return out
            }
            Err(error) => panic!("unexpected loss: {error}"),
        }
    }
}
async fn advance(observer: &TurnTrackingObserver, duration: Duration) {
    tokio::task::yield_now().await;
    tokio::time::advance(duration).await;
    observer.on_timeout().await;
    tokio::task::yield_now().await;
}
#[tokio::test(start_paused = true)]
async fn normal_conversation_starts_at_pipeline_start_and_has_two_complete_turns() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    for (seq, kind, at) in [
        (0, "StartFrame", 0),
        (1, "UserStartedSpeakingFrame", 100),
        (2, "UserStoppedSpeakingFrame", 200),
        (3, "BotStartedSpeakingFrame", 300),
        (4, "BotStoppedSpeakingFrame", 400),
        (5, "UserStartedSpeakingFrame", 500),
        (6, "UserStoppedSpeakingFrame", 600),
        (7, "BotStartedSpeakingFrame", 700),
        (8, "BotStoppedSpeakingFrame", 800),
    ] {
        send(&observer, seq, kind, at).await;
    }
    advance(&observer, Duration::from_millis(2499)).await;
    assert_eq!(
        drain(&mut rx),
        vec![
            started(1, 0),
            ended(1, 0, 500, false, TurnEndReason::UserStarted),
            started(2, 500)
        ]
    );
    advance(&observer, Duration::from_millis(1)).await;
    assert_eq!(
        drain(&mut rx),
        vec![ended(2, 500, 800, false, TurnEndReason::BotStoppedTimeout)]
    );
}
#[tokio::test]
async fn repeated_user_speech_before_bot_stays_in_the_same_turn() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    for seq in 1..10 {
        send(&observer, seq, "UserStartedSpeakingFrame", seq).await;
    }
    assert_eq!(drain(&mut rx), vec![started(1, 0)]);
    assert_eq!(observer.stats().active_turn, Some(1));
}
#[tokio::test(start_paused = true)]
async fn bot_resume_cancels_old_deadline_and_latest_stop_is_the_duration_boundary() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    send(&observer, 1, "BotStartedSpeakingFrame", 10).await;
    send(&observer, 2, "BotStoppedSpeakingFrame", 20).await;
    advance(&observer, Duration::from_secs(2)).await;
    send(&observer, 3, "BotStartedSpeakingFrame", 30).await;
    advance(&observer, Duration::from_secs(3)).await;
    assert_eq!(drain(&mut rx), vec![started(1, 0)]);
    send(&observer, 4, "BotStoppedSpeakingFrame", 40).await;
    advance(&observer, Duration::from_millis(2500)).await;
    assert_eq!(
        drain(&mut rx),
        vec![ended(1, 0, 40, false, TurnEndReason::BotStoppedTimeout)]
    );
}
#[tokio::test(start_paused = true)]
async fn interruption_rolls_over_and_late_bot_stop_cannot_end_the_new_turn() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    send(&observer, 1, "BotStartedSpeakingFrame", 10).await;
    send(&observer, 2, "UserStartedSpeakingFrame", 20).await;
    send(&observer, 3, "BotStoppedSpeakingFrame", 30).await;
    advance(&observer, Duration::from_secs(5)).await;
    assert_eq!(
        drain(&mut rx),
        vec![
            started(1, 0),
            ended(1, 0, 20, true, TurnEndReason::UserStarted),
            started(2, 20)
        ]
    );
    assert_eq!(observer.stats().active_turn, Some(2));
    assert!(!observer.stats().timer_pending);
}
#[tokio::test(start_paused = true)]
async fn end_cancel_and_native_stop_flush_once_and_cancel_pending_deadline() {
    for (kind, reason) in [
        ("EndFrame", TurnEndReason::End),
        ("CancelFrame", TurnEndReason::Cancel),
        ("StopFrame", TurnEndReason::Stop),
    ] {
        let observer = TurnTrackingObserver::new();
        let mut rx = observer.subscribe();
        send(&observer, 0, "StartFrame", 0).await;
        send(&observer, 1, "BotStartedSpeakingFrame", 1).await;
        send(&observer, 2, "BotStoppedSpeakingFrame", 2).await;
        send(&observer, 3, kind, 3).await;
        send(&observer, 4, kind, 4).await;
        advance(&observer, Duration::from_secs(10)).await;
        assert_eq!(
            drain(&mut rx),
            vec![started(1, 0), ended(1, 0, 3, true, reason)]
        );
        assert!(observer.stats().closed);
        assert!(!observer.stats().timer_pending);
    }
}
#[tokio::test]
async fn sequence_gaps_and_clock_regressions_invalidate_instead_of_making_up_duration() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 10).await;
    send(&observer, 2, "UserStartedSpeakingFrame", 20).await;
    send(&observer, 3, "BotStartedSpeakingFrame", 15).await;
    send(&observer, 3, "UserStartedSpeakingFrame", 40).await;
    send(&observer, 4, "EndFrame", 5).await;
    let records = drain(&mut rx);
    assert_eq!(records[0], started(1, 10));
    assert!(records.iter().any(|e| matches!(
        e,
        TurnTrackingEvent::Invalidated {
            reason: TurnInvalidationReason::SequenceGap,
            ..
        }
    )));
    assert!(records.iter().any(|e| matches!(
        e,
        TurnTrackingEvent::Invalidated {
            reason: TurnInvalidationReason::NonMonotonicTimestamp,
            ..
        }
    )));
    assert!(!records
        .iter()
        .any(|e| matches!(e, TurnTrackingEvent::Ended { .. })));
    assert!(observer.stats().closed);
}
#[tokio::test(start_paused = true)]
async fn dropped_input_invalidates_pending_turn_and_reports_inexact_future_numbering() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    send(&observer, 1, "BotStartedSpeakingFrame", 1).await;
    send(&observer, 2, "BotStoppedSpeakingFrame", 2).await;
    let mut loss = event(4, "BotStoppedSpeakingFrame", 4);
    loss.dropped_before = 1;
    observer.on_event(loss).await;
    advance(&observer, Duration::from_secs(10)).await;
    let mut fresh = event(5, "UserStartedSpeakingFrame", 5);
    fresh.dropped_before = 1;
    observer.on_event(fresh).await;
    assert_eq!(
        drain(&mut rx),
        vec![
            started(1, 0),
            TurnTrackingEvent::ObservationLoss {
                dropped_total: 1,
                invalidated_turn: Some(1)
            },
            TurnTrackingEvent::Started {
                boundary: None,
                turn_number: 2,
                timestamp_ns: 5,
                numbering_complete: false
            }
        ]
    );
    assert_eq!(observer.stats().observations_dropped, 1);
}
#[tokio::test(start_paused = true)]
async fn duplicate_broadcast_speech_transitions_do_not_duplicate_turns_or_extend_timer() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    send(&observer, 1, "StartFrame", 1).await;
    send(&observer, 2, "BotStartedSpeakingFrame", 2).await;
    send(&observer, 3, "BotStoppedSpeakingFrame", 3).await;
    let deadline = observer.next_deadline();
    advance(&observer, Duration::from_secs(1)).await;
    send(&observer, 4, "BotStoppedSpeakingFrame", 4).await;
    assert_eq!(observer.next_deadline(), deadline);
    advance(&observer, Duration::from_millis(1500)).await;
    assert_eq!(
        drain(&mut rx),
        vec![
            started(1, 0),
            ended(1, 0, 3, false, TurnEndReason::BotStoppedTimeout)
        ]
    );
}
#[tokio::test]
async fn first_push_filters_repeated_hops_but_upstream_first_push_is_valid() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    let mut first = event(0, "StartFrame", 0);
    if let PipelineObservation::Push { direction, .. } = Arc::make_mut(&mut first.observation) {
        *direction = Direction::Upstream;
    }
    observer.on_event(first).await;
    let mut repeat = event(1, "UserStartedSpeakingFrame", 1);
    if let PipelineObservation::Push { first_push, .. } = Arc::make_mut(&mut repeat.observation) {
        *first_push = false;
    }
    observer.on_event(repeat).await;
    assert_eq!(drain(&mut rx), vec![started(1, 0)]);
}
#[tokio::test]
async fn bounded_broadcast_reports_lag_independently_and_no_subscribers_is_counted() {
    assert!(TurnTrackingObserver::with_config(TurnTrackingConfig {
        event_capacity: 0,
        ..Default::default()
    })
    .is_err());
    let observer = TurnTrackingObserver::with_config(TurnTrackingConfig {
        event_capacity: 1,
        ..Default::default()
    })
    .unwrap();
    send(&observer, 0, "StartFrame", 0).await;
    assert_eq!(observer.stats().events_without_receivers, 1);
    let mut rx = observer.subscribe();
    send(&observer, 1, "BotStartedSpeakingFrame", 1).await;
    send(&observer, 2, "UserStartedSpeakingFrame", 2).await;
    assert!(matches!(
        rx.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(_))
    ));
    assert_eq!(observer.stats().events_emitted, 3);
}
#[tokio::test(start_paused = true)]
async fn cleanup_cancels_pending_deadline_without_inventing_end() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    send(&observer, 1, "BotStartedSpeakingFrame", 1).await;
    send(&observer, 2, "BotStoppedSpeakingFrame", 2).await;
    observer.cleanup().await;
    observer.cleanup().await;
    advance(&observer, Duration::from_secs(5)).await;
    assert_eq!(drain(&mut rx), vec![started(1, 0)]);
    assert!(observer.stats().closed);
    assert!(!observer.stats().timer_pending);
}
#[tokio::test]
async fn dropping_observer_with_pending_timer_releases_it_and_closes_receivers() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    send(&observer, 1, "BotStartedSpeakingFrame", 1).await;
    send(&observer, 2, "BotStoppedSpeakingFrame", 2).await;
    drop(observer);
    assert_eq!(drain(&mut rx), vec![started(1, 0)]);
    assert!(matches!(
        rx.try_recv(),
        Err(broadcast::error::TryRecvError::Closed)
    ));
}
#[tokio::test(start_paused = true)]
async fn exact_deadline_resume_reconciles_under_one_state_lock() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    send(&observer, 1, "BotStartedSpeakingFrame", 1).await;
    send(&observer, 2, "BotStoppedSpeakingFrame", 2).await;
    tokio::time::advance(Duration::from_millis(2500)).await;
    send(&observer, 3, "BotStartedSpeakingFrame", 3).await;
    observer.on_timeout().await;
    assert_eq!(drain(&mut rx), vec![started(1, 0)]);
    send(&observer, 4, "BotStoppedSpeakingFrame", 4).await;
    advance(&observer, Duration::from_millis(2500)).await;
    assert_eq!(
        drain(&mut rx),
        vec![ended(1, 0, 4, false, TurnEndReason::BotStoppedTimeout)]
    );
}
#[tokio::test(start_paused = true)]
async fn repeated_timers_and_reentrant_consumer_keep_one_deadline() {
    let observer = TurnTrackingObserver::new();
    let mut rx = observer.subscribe();
    send(&observer, 0, "StartFrame", 0).await;
    let _ = rx.recv().await.unwrap();
    assert_eq!(observer.stats().active_turn, Some(1));
    send(&observer, 1, "BotStartedSpeakingFrame", 1).await;
    send(&observer, 2, "BotStoppedSpeakingFrame", 2).await;
    advance(&observer, Duration::from_millis(2500)).await;
    observer.on_timeout().await;
    observer.on_timeout().await;
    assert_eq!(
        drain(&mut rx),
        vec![ended(1, 0, 2, false, TurnEndReason::BotStoppedTimeout)]
    );
    assert!(observer.next_deadline().is_none());
}
#[tokio::test]
async fn registration_is_single_use_and_worker_shutdown_cancels_retained_tracker() {
    let observer = Arc::new(TurnTrackingObserver::new());
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    observer
        .register(&mut pipeline, ObserverOptions::default())
        .unwrap();
    assert!(observer
        .register(&mut pipeline, ObserverOptions::default())
        .is_err());
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    worker.cancel("done").await.unwrap();
    worker.collect().await.unwrap();
    assert!(observer.stats().closed);
}
#[tokio::test(start_paused = true)]
async fn registered_pipeline_dispatches_default_timeout_with_original_stop_timestamp() {
    let observer = Arc::new(TurnTrackingObserver::new());
    let mut receiver = observer.subscribe();
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    observer
        .register(&mut pipeline, ObserverOptions::default())
        .unwrap();
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    let TurnTrackingEvent::Started {
        timestamp_ns: begin,
        ..
    } = receiver.recv().await.unwrap()
    else {
        panic!("start")
    };
    tokio::time::advance(Duration::from_secs(1)).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::BotStartedSpeaking),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.recv().await.unwrap();
    tokio::time::advance(Duration::from_secs(2)).await;
    let stop = Frame::new(FrameData::BotStoppedSpeaking);
    let token = stop.observation_token();
    worker
        .sender()
        .send(stop, Direction::Downstream)
        .await
        .unwrap();
    worker.recv().await.unwrap();
    let record = receiver.recv().await.unwrap();
    let mut expected = ended(
        1,
        begin,
        begin + 3_000_000_000,
        false,
        TurnEndReason::BotStoppedTimeout,
    );
    if let TurnTrackingEvent::Ended { boundary, .. } = &mut expected {
        *boundary = Some(token);
    }
    assert_eq!(record, expected);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    assert!(observer.stats().closed);
}
