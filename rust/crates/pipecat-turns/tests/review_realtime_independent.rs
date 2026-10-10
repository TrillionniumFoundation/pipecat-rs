use pipecat_core::*;
use pipecat_services::{Content, Context, Role};
use pipecat_turns::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct ProviderProbe {
    frames: Mutex<Vec<DirectedFrame>>,
    current: Mutex<Option<ProcessorContext>>,
}
#[async_trait]
impl FrameProcessor for ProviderProbe {
    fn name(&self) -> &str {
        "state-only-provider-probe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.current.lock().unwrap() = Some(ctx.clone());
        if matches!(
            frame.data,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
        ) {
            self.frames
                .lock()
                .unwrap()
                .push(DirectedFrame::new(frame.clone(), direction));
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.current.lock().unwrap().take();
        Ok(())
    }
}
fn pair(mode: RealtimeServiceMode) -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        mode,
    )
    .unwrap()
}
fn transcript(text: &str) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    })
}
async fn receive(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker.recv().await.expect("output");
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("{error:?}");
            }
            if wanted(&frame.frame.data) {
                return frame;
            }
        }
    })
    .await
    .expect("output deadline")
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("barrier".into()),
        }),
    )
    .await;
    receive(worker, |data| matches!(data, FrameData::Flush(_))).await;
}
async fn start(pair: &LLMContextAggregatorPair) -> (PipelineWorker, Arc<ProviderProbe>) {
    start_with_limit(pair, 1).await
}
async fn start_with_limit(
    pair: &LLMContextAggregatorPair,
    limit: usize,
) -> (PipelineWorker, Arc<ProviderProbe>) {
    let probe = Arc::new(ProviderProbe::default());
    let mut worker = Pipeline::new(vec![pair.user(), probe.clone(), pair.assistant()])
        .start(PipelineConfig {
            max_background_tasks: limit,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, FrameData::is_start).await;
    (worker, probe)
}
fn history(pair: &LLMContextAggregatorPair) -> Vec<(Role, String)> {
    pair.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|m| {
            (
                m.role,
                match m.content {
                    Some(Content::Text(text)) => text,
                    _ => String::new(),
                },
            )
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn review_newer_flush_invalidates_held_older_snapshot() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start(&pair).await;
    send(&worker, transcript("first")).await;
    send(&worker, FrameData::LlmResponseStart).await;
    barrier(&mut worker).await;
    let older = pair.user().advance(Duration::from_secs(1)).unwrap();
    assert_eq!(older.actions.len(), 1);
    send(&worker, transcript("late")).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    barrier(&mut worker).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(probe.frames.lock().unwrap().len(), 1);
    let current = probe.current.lock().unwrap().as_ref().unwrap().clone();
    older.emit(&current).await.unwrap();
    barrier(&mut worker).await;
    assert_eq!(
        probe.frames.lock().unwrap().len(),
        1,
        "older snapshot must not overwrite a newer committed user snapshot"
    );
    assert_eq!(
        history(&pair),
        [(Role::User, "first".into()), (Role::User, "late".into())]
    );
    worker.end().await.unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[test]
fn review_projection_failure_then_playback_retry_does_not_duplicate_receipt() {
    use pipecat_services::{ContentPart, Message};
    let invalid = Context::new(vec![Message {
        role: Role::System,
        content: Some(Content::Parts(vec![ContentPart::InlineData {
            data: "AA==".into(),
            mime_type: "image/png".into(),
        }])),
        name: None,
        tool_calls: vec![],
        tool_call_id: None,
    }]);
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        invalid,
        Default::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    pair.user()
        .process_at(
            Frame::new(transcript("user")),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    pair.assistant()
        .process_at(
            Frame::new(FrameData::LlmResponseStart),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    pair.assistant().begin_playback("receipt").unwrap();
    let parts = [AggregatedTextFrame {
        context_id: Some("receipt".into()),
        text: TextFrame::new("heard"),
        aggregated_by: "review".into(),
        raw_text: None,
        will_be_spoken: true,
    }];
    assert!(pair
        .assistant()
        .confirm_playback("receipt", &parts)
        .is_err());
    pair.context().replace(Context::default()).unwrap();
    pair.assistant()
        .confirm_playback("receipt", &parts)
        .unwrap();
    assert_eq!(
        history(&pair),
        [
            (Role::User, "user".into()),
            (Role::Assistant, "heard".into())
        ]
    );
}

#[tokio::test(start_paused = true)]
async fn review_explicit_run_remains_unmarked_after_trailing_write() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start(&pair).await;
    send(&worker, transcript("spoken input")).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    barrier(&mut worker).await;
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert_eq!(probe.frames.lock().unwrap().len(), 1);
    let mut run = Frame::new(FrameData::LlmRun);
    run.metadata
        .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), true.into());
    assert!(!run.is_realtime_context_update());
    worker
        .sender()
        .send(run, Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    let recorded = probe.frames.lock().unwrap().clone();
    assert_eq!(recorded.len(), 2);
    assert!(recorded[0].frame.is_realtime_context_update());
    assert!(!recorded[1].frame.is_realtime_context_update());
    assert_eq!(recorded[1].direction, Direction::Downstream);
    assert_eq!(history(&pair), [(Role::User, "spoken input".into())]);
    worker.end().await.unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[test]
fn review_partial_custom_config_vetoes_all_recommendations_and_interruptions() {
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                enable_interruptions: false,
                start_strategies: Some(
                    UserTurnStartStrategies::new(vec![
                        UserTurnStartStrategy::external().with_interruptions(true)
                    ])
                    .unwrap(),
                ),
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
        RealtimeServiceMode::Auto,
    )
    .unwrap();
    let metadata = FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "review".into(),
            user_turn_strategies: Some(UserTurnStrategies {
                start: Some(vec![UserTurnStrategy::Eager {
                    speculation_timeout: f64::NAN,
                }]),
                stop: Some(vec![UserTurnStrategy::Eager {
                    speculation_timeout: f64::NAN,
                }]),
            }),
        },
        is_realtime_service: true,
    });
    pair.user()
        .process_at(Frame::new(metadata), Direction::Upstream, Duration::ZERO)
        .unwrap();
    assert!(pair.user().realtime_service_enabled().unwrap());
    let output = pair
        .user()
        .process_at(
            Frame::new(FrameData::ProposedUserStartedSpeaking),
            Direction::Upstream,
            Duration::ZERO,
        )
        .unwrap();
    assert!(output.events.contains(&AggregationEvent::UserTurnStarted));
    assert!(!output
        .actions
        .iter()
        .any(|a| matches!(a, AggregationAction::Interrupt)));
}

#[test]
fn review_utf8_exact_bound_and_rejected_tail_are_transactional() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let exact = "é".repeat(512 * 1024);
    pair.user()
        .process_at(
            Frame::new(transcript(&exact)),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    let rejected = pair.user().process_at(
        Frame::new(transcript("x")),
        Direction::Downstream,
        Duration::ZERO,
    );
    assert!(rejected.unwrap_err().message.contains("1048576"));
    pair.assistant()
        .process_at(
            Frame::new(FrameData::LlmResponseEnd),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    assert_eq!(history(&pair), [(Role::User, exact)]);
}

#[tokio::test(start_paused = true)]
async fn review_cleanup_revokes_retained_effect_without_emission() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start(&pair).await;
    send(&worker, transcript("accepted")).await;
    send(&worker, FrameData::LlmResponseStart).await;
    barrier(&mut worker).await;
    let retained = pair.user().advance(Duration::from_secs(1)).unwrap();
    let current = probe.current.lock().unwrap().as_ref().unwrap().clone();
    worker.end().await.unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
    retained.emit(&current).await.unwrap();
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
    assert!(probe.frames.lock().unwrap().is_empty());
    assert_eq!(history(&pair), [(Role::User, "accepted".into())]);
}

#[derive(Default)]
struct BlockOneFrame {
    entered: tokio::sync::Notify,
    released: tokio::sync::Notify,
    current: Mutex<Option<ProcessorContext>>,
}
#[async_trait]
impl FrameProcessor for BlockOneFrame {
    fn name(&self) -> &str {
        "review-block-one"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.current.lock().unwrap() = Some(ctx.clone());
        if matches!(&frame.data, FrameData::Text(text) if text.text == "block") {
            self.entered.notify_one();
            self.released.notified().await;
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.current.lock().unwrap().take();
        Ok(())
    }
}
#[tokio::test(start_paused = true)]
async fn review_backpressure_preserves_history_without_duplicate_retry() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let blocker = Arc::new(BlockOneFrame::default());
    let mut worker = Pipeline::new(vec![pair.user(), blocker.clone(), pair.assistant()])
        .start(PipelineConfig {
            queue_capacity: 4,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, FrameData::is_start).await;
    send(&worker, transcript("truthful history")).await;
    send(&worker, FrameData::LlmResponseStart).await;
    barrier(&mut worker).await;
    let pending = pair.user().advance(Duration::from_secs(1)).unwrap();
    assert_eq!(pending.actions.len(), 1);
    send(&worker, FrameData::Text(TextFrame::new("block"))).await;
    blocker.entered.notified().await;
    let current = blocker.current.lock().unwrap().as_ref().unwrap().clone();
    for index in 0..4 {
        current
            .queue(Frame::text(format!("fill-{index}")), Direction::Downstream)
            .await
            .unwrap();
    }
    assert!(pending
        .emit(&current)
        .await
        .unwrap_err()
        .message
        .to_lowercase()
        .contains("full"));
    assert_eq!(history(&pair), [(Role::User, "truthful history".into())]);
    blocker.released.notify_one();
    assert!(matches!(
        worker.join().await.unwrap_err(),
        PipelineError::Backpressure(_)
    ));
    assert!(pair
        .user()
        .advance(Duration::from_secs(2))
        .unwrap()
        .actions
        .is_empty());
    assert_eq!(history(&pair).len(), 1);
}
