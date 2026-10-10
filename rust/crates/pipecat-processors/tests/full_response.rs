//! Native pipeline coverage of the statically inspected response-aggregator contract.
use pipecat_core::*;
use pipecat_processors::*;
use std::{
    future::pending,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, Notify};

fn recorder() -> (FullResponseCallback, mpsc::Receiver<FullResponseCompletion>) {
    let (send, receive) = mpsc::channel(64);
    let callback: FullResponseCallback = Arc::new(move |event| {
        let send = send.clone();
        Box::pin(async move {
            send.send(event)
                .await
                .map_err(|_| ProcessorError::new("test receiver closed"))
        })
    });
    (callback, receive)
}
async fn start_with(
    config: FullResponseAggregatorConfig,
    callback: FullResponseCallback,
    pipeline: PipelineConfig,
) -> (Arc<LlmFullResponseAggregator>, PipelineWorker) {
    let aggregator = Arc::new(LlmFullResponseAggregator::new(config, callback).unwrap());
    let mut worker = Pipeline::new(vec![aggregator.clone()])
        .start(pipeline)
        .await
        .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    (aggregator, worker)
}
async fn start() -> (
    Arc<LlmFullResponseAggregator>,
    PipelineWorker,
    mpsc::Receiver<FullResponseCompletion>,
) {
    let (callback, receive) = recorder();
    let (aggregator, worker) = start_with(Default::default(), callback, Default::default()).await;
    (aggregator, worker, receive)
}
async fn pass(worker: &mut PipelineWorker, frame: Frame, direction: Direction) {
    let tag = LocalFrameTag::new();
    let mut frame = frame;
    tag.mark(&mut frame).unwrap();
    frame
        .metadata
        .insert("sentinel".into(), serde_json::json!(["keep", 7]));
    frame.pts = Some(1234);
    frame.transport_source = Some("source".into());
    frame.transport_destination = Some("destination".into());
    let expected = serde_json::to_value(&frame).unwrap();
    let id = frame.id;
    worker.sender().send(frame, direction).await.unwrap();
    loop {
        let actual = tokio::time::timeout(Duration::from_secs(2), worker.recv())
            .await
            .unwrap()
            .unwrap();
        if actual.frame.id == id {
            assert_eq!(actual.direction, direction);
            assert_eq!(serde_json::to_value(&actual.frame).unwrap(), expected);
            assert!(tag.contains(&actual.frame));
            return;
        }
        assert!(
            matches!(actual.frame.data, FrameData::Error(_)),
            "unexpected frame: {:?}",
            actual.frame.data
        );
    }
}
async fn down(worker: &mut PipelineWorker, data: FrameData) {
    pass(worker, Frame::new(data), Direction::Downstream).await;
}
async fn completion(
    receive: &mut mpsc::Receiver<FullResponseCompletion>,
) -> FullResponseCompletion {
    tokio::time::timeout(Duration::from_secs(2), receive.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn end(worker: PipelineWorker) -> WorkerReport {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    report
}
async fn wait_idle(aggregator: &LlmFullResponseAggregator) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while aggregator.stats().in_flight_callbacks != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
fn text(text: &str) -> FrameData {
    FrameData::LlmText(TextFrame::llm(text))
}

#[test]
fn configuration_rejects_zero_excessive_bounds_and_unbounded_deadlines() {
    for config in [
        FullResponseAggregatorConfig {
            max_response_bytes: 0,
            ..Default::default()
        },
        FullResponseAggregatorConfig {
            max_response_bytes: 16 * 1024 * 1024 + 1,
            ..Default::default()
        },
        FullResponseAggregatorConfig {
            max_in_flight_callbacks: 0,
            ..Default::default()
        },
        FullResponseAggregatorConfig {
            max_in_flight_callbacks: 1025,
            ..Default::default()
        },
        FullResponseAggregatorConfig {
            max_in_flight_bytes: 0,
            ..Default::default()
        },
        FullResponseAggregatorConfig {
            max_in_flight_bytes: 64 * 1024 * 1024 + 1,
            ..Default::default()
        },
        FullResponseAggregatorConfig {
            callback_timeout: Duration::ZERO,
            ..Default::default()
        },
        FullResponseAggregatorConfig {
            callback_timeout: Duration::from_nanos(1),
            ..Default::default()
        },
        FullResponseAggregatorConfig {
            callback_timeout: Duration::MAX,
            ..Default::default()
        },
    ] {
        assert!(LlmFullResponseAggregator::new(config, recorder().0).is_err());
    }
}

#[tokio::test]
async fn exact_utf8_whitespace_repeated_starts_and_all_boundary_forms() {
    let (aggregator, mut worker, mut receive) = start().await;
    down(&mut worker, text("ignored before start")).await;
    down(&mut worker, FrameData::LlmResponseStart).await;
    down(&mut worker, text("  你")).await;
    down(
        &mut worker,
        FrameData::LlmFullResponseStart(Default::default()),
    )
    .await;
    let mut payload = TextFrame::llm("好\n");
    payload.append_to_context = false;
    payload.skip_tts = Some(true);
    payload.includes_inter_frame_spaces = false;
    down(&mut worker, FrameData::LlmText(payload)).await;
    down(
        &mut worker,
        FrameData::VisionFullResponseStart(Default::default()),
    )
    .await;
    down(&mut worker, FrameData::VisionText(TextFrame::llm("🙂 \t"))).await;
    down(
        &mut worker,
        FrameData::VisionFullResponseEnd(Default::default()),
    )
    .await;
    let first = completion(&mut receive).await;
    assert_eq!(
        (first.text.as_str(), first.completed),
        ("  你好\n🙂 \t", true)
    );
    assert_eq!((first.sequence, first.session_id), (1, 1));
    down(
        &mut worker,
        FrameData::LlmFullResponseEnd(Default::default()),
    )
    .await;
    assert_eq!(completion(&mut receive).await.text, "");
    down(&mut worker, FrameData::LlmResponseEnd).await;
    assert_eq!(completion(&mut receive).await.text, "");
    assert!(end(worker).await.errors.is_empty());
    assert_eq!(aggregator.stats().succeeded, 3);
    assert_eq!(aggregator.stats().buffered_bytes, 0);
    assert_eq!(aggregator.stats().in_flight_bytes, 0);
}

#[tokio::test]
async fn every_empty_end_and_interruption_is_a_distinct_completion() {
    let (_, mut worker, mut receive) = start().await;
    down(&mut worker, FrameData::Interruption).await;
    let first = completion(&mut receive).await;
    assert_eq!((first.text.as_str(), first.completed), ("", false));
    down(&mut worker, FrameData::LlmResponseStart).await;
    down(&mut worker, text("partial ")).await;
    down(&mut worker, FrameData::Interruption).await;
    let second = completion(&mut receive).await;
    assert_eq!(
        (second.text.as_str(), second.completed),
        ("partial ", false)
    );
    down(&mut worker, text("ignored after interruption")).await;
    let frame = Frame::new(FrameData::LlmResponseEnd);
    for index in 3..=4 {
        pass(&mut worker, frame.clone(), Direction::Downstream).await;
        let event = completion(&mut receive).await;
        assert_eq!((event.text.as_str(), event.completed), ("", true));
        assert_eq!(event.sequence, index);
        assert_eq!(event.trigger_frame_id, frame.id);
    }
    assert!(end(worker).await.errors.is_empty());
}

#[tokio::test]
async fn upstream_frames_keep_envelopes_and_only_generated_text_is_collected() {
    let (_, mut worker, mut receive) = start().await;
    let frames = [
        FrameData::LlmResponseStart,
        FrameData::Text(TextFrame::new("plain")),
        FrameData::InputText(TextFrame::new("typed")),
        FrameData::LlmThoughtText(TextFrame::new("thought")),
        FrameData::Transcription(TranscriptionFrame {
            text: "transcript".into(),
            ..Default::default()
        }),
        FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new("spoken"),
            aggregated_by: "sentence".into(),
            context_id: None,
            raw_text: None,
            will_be_spoken: true,
        }),
        text("[[complete]] exact"),
        FrameData::LlmResponseEnd,
    ];
    for data in frames {
        pass(&mut worker, Frame::new(data), Direction::Upstream).await;
    }
    let event = completion(&mut receive).await;
    assert_eq!(event.text, "[[complete]] exact");
    assert_eq!(event.direction, Direction::Upstream);
    assert!(end(worker).await.errors.is_empty());
}

#[tokio::test]
async fn lifecycle_terminals_do_not_synthesize_completions_and_stop_allows_reuse() {
    let (callback, mut receive) = recorder();
    let aggregator =
        Arc::new(LlmFullResponseAggregator::new(Default::default(), callback).unwrap());
    for terminal in [
        FrameData::Stop,
        FrameData::End(Default::default()),
        FrameData::EndDetailed(ReasonFrame {
            reason: Some("end".into()),
        }),
        FrameData::Cancel(Default::default()),
        FrameData::CancelDetailed(ReasonFrame {
            reason: Some("cancel".into()),
        }),
    ] {
        let mut worker = Pipeline::new(vec![aggregator.clone()])
            .start(Default::default())
            .await
            .unwrap();
        worker.recv().await.unwrap();
        down(&mut worker, FrameData::LlmResponseStart).await;
        down(&mut worker, text("unfinished")).await;
        pass(&mut worker, Frame::new(terminal), Direction::Downstream).await;
        tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(receive.try_recv().is_err());
        assert_eq!(aggregator.stats().buffered_bytes, 0);
    }
    assert_eq!(aggregator.stats().boundaries, 0);
    aggregator.cleanup().await.unwrap();
    aggregator.cleanup().await.unwrap();
}

#[tokio::test]
async fn oversized_response_is_discarded_whole_and_recovers_only_after_boundary() {
    let (callback, mut receive) = recorder();
    let (aggregator, mut worker) = start_with(
        FullResponseAggregatorConfig {
            max_response_bytes: 4,
            ..Default::default()
        },
        callback,
        Default::default(),
    )
    .await;
    down(&mut worker, FrameData::LlmResponseStart).await;
    down(&mut worker, text("éé")).await;
    assert_eq!(aggregator.stats().buffered_bytes, 4);
    down(&mut worker, text("x")).await;
    assert_eq!(aggregator.stats().buffered_bytes, 0);
    down(&mut worker, FrameData::LlmResponseStart).await;
    for _ in 0..16 {
        down(&mut worker, text(&"x".repeat(4096))).await;
    }
    down(&mut worker, FrameData::LlmResponseEnd).await;
    assert!(receive.try_recv().is_err());
    down(&mut worker, FrameData::LlmResponseStart).await;
    down(&mut worker, text("okay")).await;
    down(&mut worker, FrameData::LlmResponseEnd).await;
    assert_eq!(completion(&mut receive).await.text, "okay");
    let report = end(worker).await;
    assert_eq!(report.total_errors, 2);
    assert_eq!(aggregator.stats().response_overflows, 1);
    assert_eq!(aggregator.stats().discarded_responses, 1);
    assert_eq!(aggregator.stats().succeeded, 1);
}

#[tokio::test]
async fn count_and_byte_backpressure_reject_boundaries_without_blocking_forwarding() {
    for (max_callbacks, max_bytes) in [(1, 100), (4, 3)] {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let callback: FullResponseCallback = {
            let entered = entered.clone();
            let release = release.clone();
            Arc::new(move |_| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let (aggregator, mut worker) = start_with(
            FullResponseAggregatorConfig {
                max_in_flight_callbacks: max_callbacks,
                max_in_flight_bytes: max_bytes,
                ..Default::default()
            },
            callback,
            Default::default(),
        )
        .await;
        down(&mut worker, FrameData::LlmResponseStart).await;
        down(&mut worker, text("abc")).await;
        down(&mut worker, FrameData::LlmResponseEnd).await;
        entered.notified().await;
        down(&mut worker, FrameData::LlmResponseStart).await;
        down(&mut worker, text("d")).await;
        down(&mut worker, FrameData::LlmResponseEnd).await;
        assert_eq!(aggregator.stats().in_flight_callbacks, 1);
        assert_eq!(aggregator.stats().in_flight_bytes, 3);
        assert_eq!(aggregator.stats().rejected, 1);
        assert_eq!(aggregator.stats().buffered_bytes, 0);
        release.notify_one();
        assert_eq!(end(worker).await.total_errors, 1);
        assert_eq!(aggregator.stats().succeeded, 1);
    }
}

#[tokio::test]
async fn callback_factory_poll_errors_panics_and_deadlines_are_payload_free() {
    for case in 0..4 {
        let callback: FullResponseCallback = Arc::new(move |_| {
            if case == 0 {
                panic!("private factory payload");
            }
            Box::pin(async move {
                match case {
                    1 => panic!("private poll payload"),
                    2 => Err(ProcessorError::fatal("private error payload")),
                    _ => pending::<Result<(), ProcessorError>>().await,
                }
            })
        });
        let (aggregator, mut worker) = start_with(
            FullResponseAggregatorConfig {
                callback_timeout: Duration::from_millis(20),
                ..Default::default()
            },
            callback,
            Default::default(),
        )
        .await;
        down(&mut worker, FrameData::LlmResponseEnd).await;
        let report = end(worker).await;
        assert_eq!(report.total_errors, 1);
        assert!(!format!("{:?}", report.errors).contains("private"));
        assert!(!report.errors[0].fatal);
        let stats = aggregator.stats();
        assert_eq!(stats.callback_panics, u64::from(case <= 1));
        assert_eq!(stats.callback_errors, u64::from(case == 2));
        assert_eq!(stats.timed_out, u64::from(case == 3));
        assert_eq!(stats.in_flight_callbacks, 0);
    }
}

struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn cancel_cleanup_and_owner_drop_cancel_owned_futures_and_release_budgets() {
    for action in 0..3 {
        let drops = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(Notify::new());
        let callback: FullResponseCallback = {
            let drops = drops.clone();
            let entered = entered.clone();
            Arc::new(move |_| {
                let probe = DropProbe(drops.clone());
                let entered = entered.clone();
                Box::pin(async move {
                    let _probe = probe;
                    entered.notify_one();
                    pending().await
                })
            })
        };
        let (aggregator, mut worker) =
            start_with(Default::default(), callback, Default::default()).await;
        down(&mut worker, FrameData::LlmResponseStart).await;
        down(&mut worker, text("retained")).await;
        down(&mut worker, FrameData::LlmResponseEnd).await;
        entered.notified().await;
        match action {
            0 => {
                worker.cancel("cancel").await.unwrap();
                worker.collect().await.unwrap();
            }
            1 => {
                aggregator.cleanup().await.unwrap();
                end(worker).await;
            }
            _ => drop(worker),
        }
        wait_idle(&aggregator).await;
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(aggregator.stats().cancelled, 1);
        assert_eq!(aggregator.stats().in_flight_bytes, 0);
        aggregator.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn interruption_survives_old_callback_while_new_response_keeps_its_snapshot() {
    let (send, mut receive) = mpsc::channel(8);
    let release = Arc::new(Notify::new());
    let first_entered = Arc::new(Notify::new());
    let callback: FullResponseCallback = {
        let release = release.clone();
        let first_entered = first_entered.clone();
        Arc::new(move |event| {
            let send = send.clone();
            let release = release.clone();
            let first_entered = first_entered.clone();
            Box::pin(async move {
                if event.sequence == 1 {
                    first_entered.notify_one();
                    release.notified().await;
                }
                send.send(event).await.unwrap();
                Ok(())
            })
        })
    };
    let (aggregator, mut worker) =
        start_with(Default::default(), callback, Default::default()).await;
    down(&mut worker, FrameData::LlmResponseStart).await;
    down(&mut worker, text("old ")).await;
    down(&mut worker, FrameData::Interruption).await;
    first_entered.notified().await;
    down(&mut worker, FrameData::LlmResponseStart).await;
    down(&mut worker, text("new")).await;
    down(&mut worker, FrameData::LlmResponseEnd).await;
    let new = completion(&mut receive).await;
    assert_eq!(
        (new.text.as_str(), new.completed, new.sequence),
        ("new", true, 2)
    );
    release.notify_one();
    let old = completion(&mut receive).await;
    assert_eq!(
        (old.text.as_str(), old.completed, old.sequence),
        ("old ", false, 1)
    );
    assert!(end(worker).await.errors.is_empty());
    assert_eq!(aggregator.stats().succeeded, 2);
}

#[tokio::test]
async fn concurrent_interruption_and_end_settle_separate_boundaries_without_duplicates() {
    let (aggregator, mut worker, mut receive) = start().await;
    down(&mut worker, FrameData::LlmResponseStart).await;
    down(&mut worker, text("race")).await;
    let mut end_frame = Frame::new(FrameData::LlmResponseEnd);
    end_frame.interruptible = false;
    let end_id = end_frame.id;
    let interruption = Frame::interruption();
    let interruption_id = interruption.id;
    let sender = worker.sender();
    sender.send(end_frame, Direction::Downstream).await.unwrap();
    sender
        .send(interruption, Direction::Downstream)
        .await
        .unwrap();
    let mut events = [
        completion(&mut receive).await,
        completion(&mut receive).await,
    ];
    events.sort_by_key(|event| event.sequence);
    assert_eq!(
        events
            .iter()
            .map(|event| event.text.as_str())
            .collect::<String>(),
        "race"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.trigger_frame_id == end_id && event.completed)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| event.trigger_frame_id == interruption_id && !event.completed)
            .count(),
        1
    );
    assert!(end(worker).await.errors.is_empty());
    assert_eq!(aggregator.stats().succeeded, 2);
}

#[tokio::test]
async fn end_and_stop_wait_for_already_admitted_callback_without_creating_another() {
    for stop in [false, true] {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let callback: FullResponseCallback = {
            let entered = entered.clone();
            let release = release.clone();
            Arc::new(move |_| {
                let entered = entered.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(())
                })
            })
        };
        let (aggregator, mut worker) =
            start_with(Default::default(), callback, Default::default()).await;
        down(&mut worker, FrameData::LlmResponseEnd).await;
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        let terminal = if stop {
            FrameData::Stop
        } else {
            FrameData::End(Default::default())
        };
        worker
            .sender()
            .send(Frame::new(terminal), Direction::Downstream)
            .await
            .unwrap();
        tokio::task::yield_now().await;
        assert!(worker.try_recv().is_err());
        assert_eq!(aggregator.stats().in_flight_callbacks, 1);
        release.notify_one();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            report.state,
            if stop {
                WorkerState::Stopped
            } else {
                WorkerState::Ended
            }
        );
        assert!(frames.iter().any(|frame| frame.frame.is_terminal()));
        assert_eq!(aggregator.stats().succeeded, 1);
        assert_eq!(aggregator.stats().boundaries, 1);
        assert_eq!(aggregator.stats().in_flight_callbacks, 0);
    }
}

#[tokio::test]
async fn runtime_task_limit_preserves_original_boundary_before_reported_failure() {
    let callback: FullResponseCallback = Arc::new(|_| Box::pin(pending()));
    let (aggregator, mut worker) = start_with(
        Default::default(),
        callback,
        PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        },
    )
    .await;
    down(&mut worker, FrameData::LlmResponseEnd).await;
    down(&mut worker, FrameData::LlmResponseEnd).await;
    let result = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap();
    assert!(result.is_err());
    assert_eq!(aggregator.stats().rejected, 1);
    assert_eq!(aggregator.stats().cancelled, 1);
    assert_eq!(aggregator.stats().in_flight_callbacks, 0);
}

#[tokio::test]
async fn full_output_queue_fails_explicitly_and_releases_admitted_callback() {
    let callback: FullResponseCallback = Arc::new(|_| Box::pin(pending()));
    let (aggregator, worker) = start_with(
        Default::default(),
        callback,
        PipelineConfig {
            output_capacity: 1,
            ..Default::default()
        },
    )
    .await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmResponseStart),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !worker.has_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(worker.collect().await.is_err());
    assert_eq!(aggregator.stats().boundaries, 1);
    assert_eq!(aggregator.stats().cancelled, 1);
    assert_eq!(aggregator.stats().in_flight_callbacks, 0);
}

struct CleanupAtBoundary(Arc<LlmFullResponseAggregator>);
#[async_trait]
impl FrameProcessor for CleanupAtBoundary {
    fn name(&self) -> &str {
        "CleanupAtBoundary"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let boundary = matches!(frame.data, FrameData::LlmResponseEnd);
        self.0.process(frame, direction, ctx).await?;
        if boundary {
            self.0.cleanup().await?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cleanup().await
    }
}
#[tokio::test(flavor = "current_thread")]
async fn cleanup_before_first_poll_does_not_call_user_code_or_leak_reservation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let callback: FullResponseCallback = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(()) })
        })
    };
    let aggregator =
        Arc::new(LlmFullResponseAggregator::new(Default::default(), callback).unwrap());
    let mut worker = Pipeline::new(vec![Arc::new(CleanupAtBoundary(aggregator.clone()))])
        .start(Default::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    down(&mut worker, FrameData::LlmResponseEnd).await;
    assert!(end(worker).await.errors.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(aggregator.stats().cancelled, 1);
    assert_eq!(aggregator.stats().in_flight_callbacks, 0);
}

#[tokio::test]
async fn callback_budget_charges_spare_string_capacity_and_empty_events_charge_count() {
    let callback: FullResponseCallback = Arc::new(|_| Box::pin(pending()));
    let (aggregator, mut worker) = start_with(
        FullResponseAggregatorConfig {
            max_response_bytes: 8,
            max_in_flight_bytes: 4,
            max_in_flight_callbacks: 1,
            ..Default::default()
        },
        callback,
        Default::default(),
    )
    .await;
    down(&mut worker, FrameData::LlmResponseStart).await;
    down(&mut worker, text("abc")).await;
    down(&mut worker, text("d")).await;
    assert_eq!(aggregator.stats().buffered_bytes, 4);
    assert_eq!(aggregator.stats().buffered_capacity, 6);
    down(&mut worker, FrameData::LlmResponseEnd).await;
    assert_eq!(aggregator.stats().rejected, 1);
    down(&mut worker, FrameData::LlmResponseEnd).await;
    assert_eq!(aggregator.stats().in_flight_callbacks, 1);
    assert_eq!(aggregator.stats().in_flight_bytes, 0);
    for _ in 0..8 {
        down(&mut worker, FrameData::LlmResponseEnd).await;
    }
    assert_eq!(aggregator.stats().rejected, 9);
    worker.cancel("done").await.unwrap();
    worker.collect().await.unwrap();
    assert_eq!(aggregator.stats().in_flight_callbacks, 0);
}

#[tokio::test]
async fn sequential_sessions_get_distinct_identity_and_do_not_reuse_unfinished_text() {
    let (callback, mut receive) = recorder();
    let aggregator =
        Arc::new(LlmFullResponseAggregator::new(Default::default(), callback).unwrap());
    for session in 1..=2 {
        let mut worker = Pipeline::new(vec![aggregator.clone()])
            .start(Default::default())
            .await
            .unwrap();
        worker.recv().await.unwrap();
        down(&mut worker, FrameData::LlmResponseStart).await;
        down(&mut worker, text("same")).await;
        down(&mut worker, FrameData::LlmResponseEnd).await;
        let event = completion(&mut receive).await;
        assert_eq!(event.session_id, session);
        assert_eq!(event.sequence, session);
        assert_eq!(event.text, "same");
        down(&mut worker, FrameData::LlmResponseStart).await;
        down(&mut worker, text("must not leak")).await;
        assert!(end(worker).await.errors.is_empty());
    }
}

struct PanickingDestructor;
impl std::future::Future for PanickingDestructor {
    type Output = Result<(), ProcessorError>;
    fn poll(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::task::Poll::Pending
    }
}
impl Drop for PanickingDestructor {
    fn drop(&mut self) {
        panic!("callback destructor panic");
    }
}
#[tokio::test]
async fn unsupported_panicking_destructor_reaches_core_failure_but_releases_reservation() {
    let callback: FullResponseCallback = Arc::new(|_| Box::pin(PanickingDestructor));
    let (aggregator, mut worker) = start_with(
        FullResponseAggregatorConfig {
            callback_timeout: Duration::from_millis(20),
            ..Default::default()
        },
        callback,
        Default::default(),
    )
    .await;
    down(&mut worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(aggregator.stats().in_flight_callbacks, 0);
    assert_eq!(aggregator.stats().pending_registrations, 0);
    assert_eq!(aggregator.stats().cancelled, 1);
}

#[tokio::test]
async fn shorter_runtime_shutdown_deadline_cancels_callback_instead_of_claiming_a_drain() {
    let callback: FullResponseCallback = Arc::new(|_| Box::pin(pending()));
    let (aggregator, mut worker) = start_with(
        FullResponseAggregatorConfig {
            callback_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        callback,
        PipelineConfig {
            shutdown_timeout: Duration::from_millis(20),
            ..Default::default()
        },
    )
    .await;
    down(&mut worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap();
    assert!(matches!(result, Err(PipelineError::ShutdownTimeout)));
    assert_eq!(aggregator.stats().cancelled, 1);
    assert_eq!(aggregator.stats().succeeded, 0);
    assert_eq!(aggregator.stats().in_flight_callbacks, 0);
}
