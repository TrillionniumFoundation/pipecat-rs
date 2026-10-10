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
async fn deferred_flush_uses_user_route_without_any_new_user_frame() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start(&pair).await;
    send(&worker, transcript("late transcript")).await;
    send(&worker, FrameData::LlmResponseStart).await;
    barrier(&mut worker).await;
    assert!(history(&pair).is_empty());
    tokio::time::advance(Duration::from_secs(1)).await;
    receive(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    let recorded = probe.frames.lock().unwrap().clone();
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].direction, Direction::Downstream);
    assert_eq!(
        recorded[0].frame.metadata.get("realtime_context_update"),
        Some(&true.into())
    );
    assert_eq!(history(&pair), [(Role::User, "late transcript".into())]);
    worker.end().await.unwrap();
    let report = worker.join().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty());
}
#[tokio::test(start_paused = true)]
async fn end_failsafe_sends_state_from_user_boundary_before_assistant_snapshot() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start(&pair).await;
    send(&worker, transcript("question")).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("answer"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    barrier(&mut worker).await;
    // A returned routed snapshot re-enters the pipeline ahead of the provider;
    // wait for that independently of the assistant's original input barrier.
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        history(&pair),
        [
            (Role::User, "question".into()),
            (Role::Assistant, "answer".into())
        ]
    );
    assert_eq!(probe.frames.lock().unwrap().len(), 1);
    assert!(probe.frames.lock().unwrap()[0]
        .frame
        .metadata
        .contains_key("realtime_context_update"));
    worker.end().await.unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
}
#[tokio::test(start_paused = true)]
async fn one_owned_supervisor_handles_repeated_handoff_supersession() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start(&pair).await;
    for _ in 0..100 {
        send(&worker, transcript("word")).await;
        send(&worker, FrameData::LlmResponseStart).await;
        barrier(&mut worker).await;
    }
    tokio::time::advance(Duration::from_secs(1)).await;
    receive(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(probe.frames.lock().unwrap().len(), 1);
    assert_eq!(history(&pair)[0].1.split_whitespace().count(), 100);
    worker.end().await.unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
}
#[tokio::test(start_paused = true)]
async fn terminal_cleanup_does_not_wait_for_ttfs_or_retain_user_runtime() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
    ] {
        let pair = pair(RealtimeServiceMode::Enabled);
        let (mut worker, _probe) = start(&pair).await;
        send(&worker, transcript("accepted final")).await;
        send(&worker, FrameData::LlmResponseStart).await;
        barrier(&mut worker).await;
        let started = tokio::time::Instant::now();
        send(&worker, terminal).await;
        let report = tokio::time::timeout(Duration::from_millis(100), worker.join())
            .await
            .expect("no pending handoff task")
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(started.elapsed() < Duration::from_millis(100));
        assert_eq!(history(&pair), [(Role::User, "accepted final".into())]);
        assert_eq!(pair.user().deadline().unwrap(), None);
        tokio::time::advance(Duration::from_secs(100)).await;
        assert_eq!(history(&pair).len(), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn interrupted_response_preserves_real_user_history_and_new_turn_still_flushes() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start(&pair).await;
    send(&worker, transcript("first")).await;
    send(&worker, FrameData::LlmResponseStart).await;
    barrier(&mut worker).await;
    send(&worker, FrameData::Interruption).await;
    receive(&mut worker, |data| matches!(data, FrameData::Interruption)).await;
    send(&worker, transcript("second")).await;
    send(&worker, FrameData::LlmResponseStart).await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    receive(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(
        history(&pair),
        [(Role::User, "first".into()), (Role::User, "second".into())]
    );
    assert!(probe
        .frames
        .lock()
        .unwrap()
        .iter()
        .all(|frame| frame.direction == Direction::Downstream));
    worker.end().await.unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
}
#[tokio::test(start_paused = true)]
async fn typed_input_is_an_explicit_request_without_state_only_marker() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start(&pair).await;
    let mut typed = Frame::new(FrameData::InputText(TextFrame::new("typed request")));
    typed
        .metadata
        .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), true.into());
    assert!(!typed.is_realtime_context_update());
    worker
        .sender()
        .send(typed, Direction::Downstream)
        .await
        .unwrap();
    receive(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(probe.frames.lock().unwrap().len(), 1);
    assert!(!probe.frames.lock().unwrap()[0]
        .frame
        .metadata
        .contains_key("realtime_context_update"));
    worker.end().await.unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[tokio::test(start_paused = true)]
async fn retained_handoff_effect_keeps_causal_fence_under_surviving_supervisor() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let (mut worker, probe) = start_with_limit(&pair, 2).await;
    send(&worker, transcript("real final")).await;
    let mut start = Frame::new(FrameData::LlmResponseStart);
    start
        .metadata
        .insert("context_id".into(), "same-optional-id".into());
    worker
        .sender()
        .send(start.clone(), Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    // Explicit deterministic advance obtains the effect before its runtime
    // timer elapses, allowing interruption between history commit and routing.
    let pending = pair.user().advance(Duration::from_secs(1)).unwrap();
    assert_eq!(pending.actions.len(), 1);
    let mut durable = pending.clone();
    let AggregationAction::RealtimeUserContext(update) = &mut durable.actions[0] else {
        panic!("route");
    };
    update.frame.interruptible = false;
    send(&worker, FrameData::Interruption).await;
    receive(&mut worker, |data| matches!(data, FrameData::Interruption)).await;
    barrier(&mut worker).await;
    let current = probe.current.lock().unwrap().as_ref().unwrap().clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let surviving = durable.clone();
    current
        .clone()
        .spawn("stale-handoff-control", false, async move {
            pending.emit(&current).await?;
            surviving.emit(&current).await?;
            tx.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    rx.await.unwrap();
    receive(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(
        probe.frames.lock().unwrap().len(),
        1,
        "only explicit uninterruptible survives raw causal invalidation"
    );
    assert_eq!(history(&pair), [(Role::User, "real final".into())]);
    // The same optional ID does not revive an effect superseded by a new start.
    send(&worker, transcript("new final")).await;
    worker
        .sender()
        .send(start.duplicate(), Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    let current = probe.current.lock().unwrap().as_ref().unwrap().clone();
    durable.emit(&current).await.unwrap();
    barrier(&mut worker).await;
    assert_eq!(probe.frames.lock().unwrap().len(), 1);
    worker.end().await.unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
    assert!(probe.current.lock().unwrap().is_none());
    assert_eq!(history(&pair).len(), 2);
}

#[tokio::test(start_paused = true)]
async fn missing_or_misordered_realtime_pair_fails_startup_closed() {
    for reversed in [false, true] {
        let pair = pair(RealtimeServiceMode::Enabled);
        let processors: Vec<Arc<dyn FrameProcessor>> = if reversed {
            vec![pair.assistant(), pair.user()]
        } else {
            vec![pair.assistant()]
        };
        let result = Pipeline::new(processors)
            .start(PipelineConfig {
                startup_timeout: Duration::from_millis(50),
                ..Default::default()
            })
            .await;
        let error = match result {
            Ok(mut worker) => {
                worker.cancel("invalid topology").await.unwrap();
                let _ = worker.join().await;
                panic!("invalid realtime topology started")
            }
            Err(error) => error,
        };
        assert!(
            !matches!(error, PipelineError::StartupTimeout),
            "missing pair must fail directly rather than hide behind a startup timeout"
        );
    }
}
