//! Source observer behavior, bounded state, loss and lifecycle contracts.
use pipecat_core::*;
use pipecat_observers::*;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;

fn event(sequence: u64, at: u64) -> ObservationEvent {
    ObservationEvent {
        sequence,
        dropped_before: 0,
        observation: Arc::new(PipelineObservation::Push {
            source: ObservedProcessor {
                runtime_id: 1,
                index: 0,
                name: "source".into(),
            },
            destination: None,
            frame: ObservedFrame {
                id: sequence,
                token: None,
                kind: "Fixture",
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
fn terminal_event(sequence: u64, kind: &'static str) -> ObservationEvent {
    let mut event = event(sequence, sequence);
    if let PipelineObservation::Push { frame, .. } = Arc::make_mut(&mut event.observation) {
        frame.kind = kind;
    }
    event
}
fn call(phase: FunctionCallPhase, id: Option<&str>) -> CapturedFunctionCall {
    CapturedFunctionCall {
        phase,
        function_name: id.map_or(Captured::NotRequested, |_| Captured::Present("f".into())),
        tool_call_id: id.map_or(Captured::NotRequested, |id| Captured::Present(id.into())),
        group_id: Captured::NotRequested,
        blocking: None,
        arguments: Captured::NotRequested,
        result: Captured::NotRequested,
        error: Captured::NotRequested,
    }
}
fn capture(call: CapturedFunctionCall) -> CapturedPush {
    CapturedPush {
        broadcast: false,
        payload: CapturedPayload::Functions(CapturedFunctionCalls {
            calls: vec![call],
            omitted_calls: 0,
        }),
    }
}
async fn push_call(
    observer: &FunctionCallObserver,
    seq: u64,
    at: u64,
    phase: FunctionCallPhase,
    id: Option<&str>,
) {
    observer
        .on_captured_event(event(seq, at), capture(call(phase, id)))
        .await;
}
async fn speech(
    observer: &SpeakingObserver,
    seq: u64,
    at: u64,
    kind: SpeechEventKind,
    offset: f64,
) {
    observer
        .on_captured_event(
            event(seq, at),
            CapturedPush {
                broadcast: false,
                payload: CapturedPayload::Speech(CapturedSpeech {
                    kind,
                    confirmation_secs: offset,
                }),
            },
        )
        .await;
}
fn drain<T: Clone>(rx: &mut broadcast::Receiver<SourceEvent<T>>) -> Vec<SourceEvent<T>> {
    let mut events = vec![];
    loop {
        match rx.try_recv() {
            Ok(event) => events.push(event),
            Err(broadcast::error::TryRecvError::Empty | broadcast::error::TryRecvError::Closed) => {
                return events
            }
            Err(error) => panic!("unexpected output loss: {error}"),
        }
    }
}
fn records<T>(events: Vec<SourceEvent<T>>) -> Vec<T> {
    events
        .into_iter()
        .filter_map(|event| {
            if let SourceEvent::Record(record) = event {
                Some(record)
            } else {
                None
            }
        })
        .collect()
}
#[tokio::test]
async fn independent_pending_and_running_calls_settle_only_their_own_timing() {
    use FunctionCallPhase::*;
    let observer = FunctionCallObserver::new();
    let mut rx = observer.subscribe();
    for (seq, at, phase, id) in [
        (0, 10, Started, "a"),
        (1, 20, Started, "b"),
        (2, 30, InProgress, "a"),
        (3, 40, InProgress, "b"),
        (4, 50, Failed, "b"),
        (5, 60, Completed, "a"),
        (6, 70, Cancelled, "unknown"),
    ] {
        push_call(&observer, seq, at, phase, Some(id)).await;
    }
    let events = records(drain(&mut rx));
    assert_eq!(events[2].started_ns, Some(10));
    assert_eq!(events[3].started_ns, Some(20));
    assert_eq!(events[4].in_progress_ns, Some(40));
    assert_eq!(events[5].in_progress_ns, Some(30));
    assert_eq!(events[6].in_progress_ns, None);
    assert_eq!(observer.active_calls(), 0);
}
#[tokio::test]
async fn no_identity_capture_means_no_retained_or_invented_timing() {
    let observer = FunctionCallObserver::new();
    let mut rx = observer.subscribe();
    for (seq, phase) in [
        FunctionCallPhase::Started,
        FunctionCallPhase::InProgress,
        FunctionCallPhase::Completed,
    ]
    .into_iter()
    .enumerate()
    {
        push_call(&observer, seq as u64, seq as u64, phase, None).await;
    }
    assert_eq!(observer.active_calls(), 0);
    assert!(records(drain(&mut rx))
        .iter()
        .all(|event| event.started_ns.is_none() && event.in_progress_ns.is_none()));
}
#[tokio::test]
async fn input_loss_sequence_gaps_and_backwards_clocks_discard_call_correlation() {
    let observer = FunctionCallObserver::new();
    let mut rx = observer.subscribe();
    push_call(&observer, 0, 10, FunctionCallPhase::InProgress, Some("a")).await;
    let mut lost = event(2, 20);
    lost.dropped_before = 1;
    observer
        .on_captured_event(lost, capture(call(FunctionCallPhase::Completed, Some("a"))))
        .await;
    let events = drain(&mut rx);
    assert!(matches!(
        events[1],
        SourceEvent::ObservationLoss { dropped_total: 1 }
    ));
    assert_eq!(records(events)[1].in_progress_ns, None);
    let mut start = event(3, 30);
    start.dropped_before = 1;
    observer
        .on_captured_event(
            start,
            capture(call(FunctionCallPhase::InProgress, Some("b"))),
        )
        .await;
    let mut gap = event(5, 40);
    gap.dropped_before = 1;
    observer
        .on_captured_event(gap, capture(call(FunctionCallPhase::Completed, Some("b"))))
        .await;
    let events = drain(&mut rx);
    assert!(events.iter().any(|event| matches!(
        event,
        SourceEvent::Invalidated {
            reason: SourceInvalidation::SequenceGap
        }
    )));
    assert_eq!(records(events)[1].in_progress_ns, None);
    let mut backward = event(6, 5);
    backward.dropped_before = 1;
    observer
        .on_captured_event(
            backward,
            capture(call(FunctionCallPhase::Started, Some("c"))),
        )
        .await;
    assert_eq!(
        drain(&mut rx),
        vec![SourceEvent::Invalidated {
            reason: SourceInvalidation::NonMonotonicTimestamp
        }]
    );
    assert_eq!(observer.active_calls(), 0);
}
#[tokio::test]
async fn stale_events_after_a_loss_marker_cannot_reestablish_state() {
    let observer = FunctionCallObserver::new();
    let mut rx = observer.subscribe();
    let mut lost = event(0, 1);
    lost.dropped_before = 4;
    observer.on_event(lost).await;
    observer
        .on_captured_event(
            event(1, 2),
            capture(call(FunctionCallPhase::InProgress, Some("a"))),
        )
        .await;
    assert_eq!(observer.active_calls(), 0);
    assert_eq!(
        drain(&mut rx),
        vec![SourceEvent::ObservationLoss { dropped_total: 4 }]
    );
}
#[tokio::test]
async fn whole_truncated_batches_are_ineligible_for_correlation() {
    let observer = FunctionCallObserver::new();
    let mut rx = observer.subscribe();
    push_call(&observer, 0, 10, FunctionCallPhase::InProgress, Some("old")).await;
    let mut malformed = call(FunctionCallPhase::Started, Some("partial"));
    malformed.tool_call_id = Captured::Omitted(CaptureOmission::FieldBytes);
    observer
        .on_captured_event(
            event(1, 20),
            CapturedPush {
                broadcast: false,
                payload: CapturedPayload::Functions(CapturedFunctionCalls {
                    calls: vec![call(FunctionCallPhase::Started, Some("new")), malformed],
                    omitted_calls: 0,
                }),
            },
        )
        .await;
    assert_eq!(observer.active_calls(), 0);
    observer
        .on_captured_event(
            event(2, 30),
            CapturedPush {
                broadcast: false,
                payload: CapturedPayload::Functions(CapturedFunctionCalls {
                    calls: vec![call(FunctionCallPhase::Started, Some("new"))],
                    omitted_calls: 2,
                }),
            },
        )
        .await;
    assert_eq!(observer.active_calls(), 0);
    push_call(&observer, 3, 40, FunctionCallPhase::InProgress, Some("new")).await;
    let events = drain(&mut rx);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                SourceEvent::Invalidated {
                    reason: SourceInvalidation::CaptureTruncated
                }
            ))
            .count(),
        2
    );
    assert_eq!(records(events).last().unwrap().started_ns, None);
}
#[tokio::test]
async fn active_call_count_and_exact_id_bytes_are_bounded() {
    let observer = FunctionCallObserver::with_config(FunctionCallObserverConfig {
        max_active_calls: 1,
        max_correlation_bytes: 3,
        ..Default::default()
    })
    .unwrap();
    let mut rx = observer.subscribe();
    push_call(&observer, 0, 1, FunctionCallPhase::InProgress, Some("a")).await;
    push_call(&observer, 1, 2, FunctionCallPhase::InProgress, Some("b")).await;
    assert_eq!(observer.active_calls(), 1);
    push_call(&observer, 2, 3, FunctionCallPhase::Completed, Some("a")).await;
    push_call(&observer, 3, 4, FunctionCallPhase::Started, Some("long")).await;
    assert_eq!(observer.active_calls(), 0);
    let events = drain(&mut rx);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                SourceEvent::Invalidated {
                    reason: SourceInvalidation::StateCapacity
                }
            ))
            .count(),
        2
    );
    assert_eq!(records(events)[2].in_progress_ns, None);
}
#[tokio::test]
async fn acoustic_semantic_bot_and_interruption_layers_remain_distinct() {
    use SpeechEventKind::*;
    let observer = SpeakingObserver::new();
    let mut rx = observer.subscribe();
    for (seq, at, kind, offset) in [
        (0, 1_000_000_000, UserSpeechStarted, 0.2),
        (1, 1_100_000_000, UserTurnStarted, 0.0),
        (2, 2_000_000_000, UserSpeechStopped, 0.3),
        (3, 2_100_000_000, UserTurnStopped, 0.0),
        (4, 2_200_000_000, BotSpeechStarted, 0.0),
        (5, 2_300_000_000, Interruption, 0.0),
        (6, 2_400_000_000, BotSpeechStopped, 0.0),
    ] {
        speech(&observer, seq, at, kind, offset).await;
    }
    let events = records(drain(&mut rx));
    assert_eq!(events[0].timestamp_ns, 800_000_000);
    assert_eq!(events[2].timestamp_ns, 1_700_000_000);
    assert_eq!(events[2].started_ns, Some(800_000_000));
    assert_eq!(events[3].started_ns, Some(1_100_000_000));
    assert_eq!(events[5].started_ns, None);
    assert_eq!(events[6].started_ns, Some(2_200_000_000));
}
#[tokio::test]
async fn repeated_open_replaces_only_its_layer_and_missing_open_stays_absent() {
    use SpeechEventKind::*;
    let observer = SpeakingObserver::new();
    let mut rx = observer.subscribe();
    for (seq, kind) in [
        UserTurnStarted,
        BotSpeechStarted,
        UserTurnStarted,
        UserTurnStopped,
        BotSpeechStopped,
        UserSpeechStopped,
    ]
    .into_iter()
    .enumerate()
    {
        speech(&observer, seq as u64, 100 + seq as u64, kind, 0.0).await;
    }
    let events = records(drain(&mut rx));
    assert_eq!(events[3].started_ns, Some(102));
    assert_eq!(events[4].started_ns, Some(101));
    assert_eq!(events[5].started_ns, None);
}
#[tokio::test]
async fn invalid_vad_offsets_and_lost_speech_boundaries_do_not_make_intervals() {
    use SpeechEventKind::*;
    let observer = SpeakingObserver::new();
    let mut rx = observer.subscribe();
    speech(&observer, 0, 100, UserTurnStarted, 0.0).await;
    for (seq, offset) in [(1, -1.0), (2, f64::NAN), (3, f64::INFINITY), (4, 1.0)] {
        speech(&observer, seq, 100 + seq, UserSpeechStarted, offset).await;
    }
    speech(&observer, 5, 200, UserTurnStopped, 0.0).await;
    let events = drain(&mut rx);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(
                event,
                SourceEvent::Invalidated {
                    reason: SourceInvalidation::InvalidVadOffset
                }
            ))
            .count(),
        4
    );
    assert_eq!(records(events).last().unwrap().started_ns, None);
    speech(&observer, 6, 300, BotSpeechStarted, 0.0).await;
    let mut lost = event(8, 400);
    lost.dropped_before = 1;
    observer
        .on_captured_event(
            lost,
            CapturedPush {
                broadcast: false,
                payload: CapturedPayload::Speech(CapturedSpeech {
                    kind: BotSpeechStopped,
                    confirmation_secs: 0.0,
                }),
            },
        )
        .await;
    assert_eq!(records(drain(&mut rx)).last().unwrap().started_ns, None);
}
#[tokio::test]
async fn forwarded_and_upstream_broadcast_moments_are_ignored_but_plain_upstream_is_allowed() {
    let observer = FunctionCallObserver::new();
    let mut rx = observer.subscribe();
    let mut up = event(0, 1);
    if let PipelineObservation::Push { direction, .. } = Arc::make_mut(&mut up.observation) {
        *direction = Direction::Upstream;
    }
    let mut broadcast = capture(call(FunctionCallPhase::Started, Some("a")));
    broadcast.broadcast = true;
    observer.on_captured_event(up.clone(), broadcast).await;
    up.sequence = 1;
    observer
        .on_captured_event(up, capture(call(FunctionCallPhase::Started, Some("b"))))
        .await;
    let mut repeated = event(2, 2);
    if let PipelineObservation::Push { first_push, .. } = Arc::make_mut(&mut repeated.observation) {
        *first_push = false;
    }
    observer
        .on_captured_event(
            repeated,
            capture(call(FunctionCallPhase::Completed, Some("b"))),
        )
        .await;
    assert_eq!(records(drain(&mut rx)).len(), 1);
    assert_eq!(observer.active_calls(), 1);
    let speaking = SpeakingObserver::new();
    let mut rx = speaking.subscribe();
    let mut up = event(0, 1);
    if let PipelineObservation::Push { direction, .. } = Arc::make_mut(&mut up.observation) {
        *direction = Direction::Upstream;
    }
    speaking
        .on_captured_event(
            up,
            CapturedPush {
                broadcast: true,
                payload: CapturedPayload::Speech(CapturedSpeech {
                    kind: SpeechEventKind::Interruption,
                    confirmation_secs: 0.0,
                }),
            },
        )
        .await;
    assert!(drain(&mut rx).is_empty());
}
#[tokio::test]
async fn terminal_cancel_and_cleanup_close_streams_without_fabricated_outcomes() {
    for kind in ["EndFrame", "CancelFrame", "StopFrame"] {
        let observer = FunctionCallObserver::new();
        let mut rx = observer.subscribe();
        push_call(&observer, 0, 1, FunctionCallPhase::Started, Some("pending")).await;
        observer.on_event(terminal_event(1, kind)).await;
        assert_eq!(records(drain(&mut rx)).len(), 1);
        assert_eq!(observer.active_calls(), 0);
        assert!(observer.stats().closed);
        assert_eq!(rx.try_recv(), Err(broadcast::error::TryRecvError::Closed));
        observer.cancel();
        observer.cleanup().await;
        assert_eq!(observer.stats().events_emitted, 1);
    }
    let speaking = SpeakingObserver::new();
    let mut rx = speaking.subscribe();
    speech(&speaking, 0, 1, SpeechEventKind::BotSpeechStarted, 0.0).await;
    speaking.cancel();
    speaking.cleanup().await;
    assert_eq!(records(drain(&mut rx)).len(), 1);
    assert!(speaking.stats().closed);
    let errors = ErrorObserver::new();
    let mut rx = errors.subscribe();
    errors.cancel();
    errors.cleanup().await;
    assert_eq!(rx.try_recv(), Err(broadcast::error::TryRecvError::Closed));
}
#[tokio::test]
async fn slow_subscribers_lag_independently_and_unsubscribed_records_are_counted() {
    let observer = SpeakingObserver::with_capacity(1).unwrap();
    speech(&observer, 0, 1, SpeechEventKind::BotSpeechStarted, 0.0).await;
    assert_eq!(observer.stats().events_without_receivers, 1);
    let mut slow = observer.subscribe();
    let mut fast = observer.subscribe();
    for seq in 1..=3 {
        speech(
            &observer,
            seq,
            seq + 1,
            SpeechEventKind::BotSpeechStarted,
            0.0,
        )
        .await;
        assert!(matches!(fast.try_recv(), Ok(SourceEvent::Record(_))));
    }
    assert_eq!(
        slow.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(2))
    );
    assert!(matches!(slow.try_recv(), Ok(SourceEvent::Record(_))));
}
fn started_frame(id: &str) -> Frame {
    Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
        function_name: "weather".into(),
        tool_call_id: id.into(),
        arguments: json!({"city":"secret-city"}),
    }]))
}
fn progress_frame(id: &str) -> Frame {
    Frame::new(FrameData::FunctionCallInProgress(
        FunctionCallInProgressFrame {
            function_name: "weather".into(),
            tool_call_id: id.into(),
            arguments: json!({"city":"secret-city"}),
            cancel_on_interruption: false,
            group_id: Some("private-group".into()),
        },
    ))
}
fn result_frame(id: &str, is_final: bool, error: Option<&str>) -> Frame {
    Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
        function_name: "weather".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
        result: json!({"answer":"secret-result"}),
        run_llm: None,
        is_final,
        error: error.map(str::to_owned),
    }))
}
#[tokio::test]
async fn live_pipeline_preserves_source_phases_finality_and_independent_privacy_choices() {
    let private = Arc::new(
        FunctionCallObserver::with_config(FunctionCallObserverConfig {
            capture: FunctionCaptureOptions {
                identity: true,
                arguments: true,
                results: true,
                errors: true,
            },
            ..Default::default()
        })
        .unwrap(),
    );
    let safe = Arc::new(FunctionCallObserver::new());
    let mut private_rx = private.subscribe();
    let mut safe_rx = safe.subscribe();
    let mut pipeline = Pipeline::new(vec![
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
    ]);
    private
        .register(&mut pipeline, ObserverOptions::default())
        .unwrap();
    safe.register(&mut pipeline, ObserverOptions::default())
        .unwrap();
    assert!(private
        .register(&mut pipeline, ObserverOptions::default())
        .is_err());
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    for frame in [
        started_frame("a"),
        progress_frame("a"),
        result_frame("a", false, None),
        result_frame("a", true, None),
        result_frame("b", true, Some("private-failure")),
        Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "weather".into(),
            tool_call_id: "c".into(),
            run_llm: true,
        })),
        Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "weather".into(),
            tool_call_id: "d".into(),
            run_llm: false,
        })),
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        worker.recv().await.unwrap();
    }
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    let captured = records(drain(&mut private_rx));
    let safe_events = records(drain(&mut safe_rx));
    use FunctionCallPhase::*;
    assert_eq!(
        captured
            .iter()
            .map(|event| event.call.phase)
            .collect::<Vec<_>>(),
        vec![Started, InProgress, Completed, Failed, TimedOut, Cancelled]
    );
    assert_eq!(captured[1].started_ns, Some(captured[0].timestamp_ns));
    assert_eq!(captured[2].in_progress_ns, Some(captured[1].timestamp_ns));
    assert_eq!(captured[1].call.blocking, Some(false));
    assert_eq!(
        captured[1].call.group_id.value().unwrap().as_deref(),
        Some("private-group")
    );
    assert!(captured[0].call.arguments.value().is_some());
    assert!(captured[2].call.result.value().is_some());
    assert_eq!(
        captured[3].call.error.value().unwrap().as_deref(),
        Some("private-failure")
    );
    assert_eq!(captured[3].call.result, Captured::NotRequested);
    assert_eq!(safe_events.len(), captured.len());
    for event in safe_events {
        assert_eq!(event.call.tool_call_id, Captured::NotRequested);
        assert_eq!(event.call.function_name, Captured::NotRequested);
        assert_eq!(event.call.arguments, Captured::NotRequested);
        assert_eq!(event.call.result, Captured::NotRequested);
        assert_eq!(event.call.error, Captured::NotRequested);
        assert_eq!(event.started_ns, None);
        assert_eq!(event.in_progress_ns, None);
    }
    assert!(private.stats().closed);
    assert_eq!(private.active_calls(), 0);
}

#[tokio::test]
async fn error_records_are_stateless_first_push_events_with_optional_bounded_messages() {
    let observer = ErrorObserver::new();
    let mut rx = observer.subscribe();
    let captured = CapturedPush {
        broadcast: false,
        payload: CapturedPayload::Error(CapturedError {
            processor: ObservedProcessor {
                runtime_id: 7,
                index: 2,
                name: "origin".into(),
            },
            processor_usable: false,
            category: ErrorCategory::Authentication,
            fatal: false,
            message: Captured::Omitted(CaptureOmission::FieldBytes),
        }),
    };
    observer
        .on_captured_event(event(0, 10), captured.clone())
        .await;
    let mut repeated = event(1, 20);
    if let PipelineObservation::Push { first_push, .. } = Arc::make_mut(&mut repeated.observation) {
        *first_push = false;
    }
    observer.on_captured_event(repeated, captured.clone()).await;
    observer.on_captured_event(event(2, 30), captured).await;
    let records = records(drain(&mut rx));
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].timestamp_ns, 10);
    assert_eq!(records[1].timestamp_ns, 30);
    assert!(!records[0].processor_usable);
    assert_eq!(
        records[0].message,
        Captured::Omitted(CaptureOmission::FieldBytes)
    );
    assert_eq!(records[0].category, ErrorCategory::Authentication);
}

#[tokio::test]
async fn dropping_a_live_worker_owns_and_closes_all_source_observers() {
    let errors = Arc::new(ErrorObserver::new());
    let speech = Arc::new(SpeakingObserver::new());
    let calls = Arc::new(FunctionCallObserver::new());
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    errors
        .register(&mut pipeline, ObserverOptions::default())
        .unwrap();
    speech
        .register(&mut pipeline, ObserverOptions::default())
        .unwrap();
    calls
        .register(&mut pipeline, ObserverOptions::default())
        .unwrap();
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    drop(worker);
    tokio::time::timeout(Duration::from_secs(3), async {
        while !(errors.stats().closed && speech.stats().closed && calls.stats().closed) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn final_queue_loss_is_exposed_even_when_no_later_marker_is_delivered() {
    let calls = Arc::new(FunctionCallObserver::new());
    let mut rx = calls.subscribe();
    let mut pipeline = Pipeline::new(vec![Arc::new(IdentityProcessor)]);
    let handle = calls
        .register(
            &mut pipeline,
            ObserverOptions {
                capacity: 1,
                ..Default::default()
            },
        )
        .unwrap();
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    assert!(handle.stats().dropped > 0);
    assert_eq!(calls.stats().observations_dropped, handle.stats().dropped);
    assert!(drain(&mut rx).iter().any(|event| matches!(event, SourceEvent::ObservationLoss { dropped_total } if *dropped_total == handle.stats().dropped)));
    assert!(calls.stats().closed);
}
