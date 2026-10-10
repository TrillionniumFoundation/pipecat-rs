use pipecat_core::*;
use pipecat_services::Context;
use pipecat_turns::*;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

// ObserverHub retains its clock until the Runtime itself is released, including
// after observer shutdown. No observer task or test context retains this clock.
struct RuntimeClock;
impl ObserverClock for RuntimeClock {
    fn now_ns(&self) -> u64 {
        0
    }
}
fn pair() -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap()
}
fn tracked_pipeline(processors: Vec<Arc<dyn FrameProcessor>>) -> (Pipeline, Weak<RuntimeClock>) {
    let clock = Arc::new(RuntimeClock);
    let weak = Arc::downgrade(&clock);
    (Pipeline::new(processors).with_observer_clock(clock), weak)
}
async fn released(clock: &Weak<RuntimeClock>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while clock.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("runtime retained a route, handoff cause, or owned task after shutdown");
}
async fn arm(pair: &LLMContextAggregatorPair, worker: &mut PipelineWorker) {
    for data in [
        FrameData::Transcription(TranscriptionFrame {
            text: "accepted".into(),
            finalized: true,
            ..Default::default()
        }),
        FrameData::LlmResponseStart,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: None,
        }),
    ] {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let f = worker.recv().await.expect("startup and barrier output");
            assert!(!matches!(f.frame.data, FrameData::Error(_)));
            if matches!(f.frame.data, FrameData::Flush(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(pair.user().deadline().unwrap().is_some());
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
}
#[tokio::test]
async fn terminal_cleanup_releases_runtime_while_pair_stays_alive() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
    ] {
        eprintln!("checking terminal: {terminal:?}");
        let pair = pair();
        let (pipeline, weak) = tracked_pipeline(vec![
            pair.user(),
            Arc::new(IdentityProcessor),
            pair.assistant(),
        ]);
        let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
        arm(&pair, &mut worker).await;
        worker
            .sender()
            .send(Frame::new(terminal), Direction::Downstream)
            .await
            .unwrap();
        assert!(worker.join().await.unwrap().errors.is_empty());
        drop(worker);
        released(&weak).await;
        assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    }
}
#[tokio::test]
async fn dropped_worker_releases_armed_handoff_and_runtime_with_pair_retained() {
    let pair = pair();
    let (pipeline, weak) = tracked_pipeline(vec![
        pair.user(),
        Arc::new(IdentityProcessor),
        pair.assistant(),
    ]);
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    arm(&pair, &mut worker).await;
    drop(worker);
    released(&weak).await;
    assert!(pair.user().deadline().unwrap().is_none());
}
#[tokio::test]
async fn dropping_pair_and_armed_worker_releases_both_processors_and_runtime() {
    let pair = pair();
    let weak_user = Arc::downgrade(&pair.user());
    let weak_assistant = Arc::downgrade(&pair.assistant());
    let (pipeline, weak) = tracked_pipeline(vec![
        pair.user(),
        Arc::new(IdentityProcessor),
        pair.assistant(),
    ]);
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    arm(&pair, &mut worker).await;
    drop(pair);
    drop(worker);
    released(&weak).await;
    assert!(weak_user.upgrade().is_none());
    assert!(weak_assistant.upgrade().is_none());
}
struct FailStart;
#[async_trait]
impl FrameProcessor for FailStart {
    fn name(&self) -> &str {
        "fail-after-user-binding"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            return Err(ProcessorError::fatal("independent startup failure"));
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn startup_failure_after_user_binding_releases_runtime_with_pair_retained() {
    let pair = pair();
    let (pipeline, weak) =
        tracked_pipeline(vec![pair.user(), Arc::new(FailStart), pair.assistant()]);
    assert!(pipeline.start(PipelineConfig::default()).await.is_err());
    released(&weak).await;
    assert!(pair.user().deadline().unwrap().is_none());
}
#[tokio::test]
async fn rejected_topology_releases_runtime_with_pair_retained() {
    for reverse in [false, true] {
        let pair = pair();
        let processors: Vec<Arc<dyn FrameProcessor>> = if reverse {
            vec![pair.assistant(), pair.user()]
        } else {
            vec![pair.assistant()]
        };
        let (pipeline, weak) = tracked_pipeline(processors);
        assert!(pipeline.start(PipelineConfig::default()).await.is_err());
        released(&weak).await;
    }
}

async fn transcript_barrier(worker: &mut PipelineWorker) {
    for data in [
        FrameData::Transcription(TranscriptionFrame {
            text: "cascade accepted".into(),
            finalized: true,
            ..Default::default()
        }),
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: None,
        }),
    ] {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    loop {
        let frame = worker.recv().await.expect("cascade barrier");
        assert!(!matches!(frame.frame.data, FrameData::Error(_)));
        if matches!(frame.frame.data, FrameData::Flush(_)) {
            break;
        }
    }
}
#[tokio::test]
async fn standalone_cascade_stop_releases_runtime_with_user_retained() {
    let user = Arc::new(LLMUserAggregator::new(
        SharedContext::default(),
        Default::default(),
    ));
    let (pipeline, weak) = tracked_pipeline(vec![user.clone()]);
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    transcript_barrier(&mut worker).await;
    worker
        .sender()
        .send(Frame::new(FrameData::Stop), Direction::Downstream)
        .await
        .unwrap();
    assert!(worker.join().await.unwrap().errors.is_empty());
    drop(worker);
    released(&weak).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
}
#[tokio::test]
async fn paired_cascade_user_without_assistant_stop_releases_runtime() {
    for mode in [RealtimeServiceMode::Auto, RealtimeServiceMode::Disabled] {
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            Default::default(),
            Default::default(),
            mode,
        )
        .unwrap();
        let (pipeline, weak) = tracked_pipeline(vec![pair.user()]);
        let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
        transcript_barrier(&mut worker).await;
        worker
            .sender()
            .send(Frame::new(FrameData::Stop), Direction::Downstream)
            .await
            .unwrap();
        assert!(worker.join().await.unwrap().errors.is_empty());
        drop(worker);
        released(&weak).await;
        assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    }
}

#[tokio::test]
async fn stopped_pair_and_standalone_cannot_rebind_to_a_new_runtime() {
    for paired in [false, true] {
        let pair = paired.then(pair);
        let user = pair.as_ref().map_or_else(
            || {
                Arc::new(LLMUserAggregator::new(
                    SharedContext::default(),
                    Default::default(),
                ))
            },
            LLMContextAggregatorPair::user,
        );
        let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![user];
        if let Some(pair) = &pair {
            processors.push(pair.assistant());
        }
        let (pipeline, old_clock) = tracked_pipeline(processors.clone());
        let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::Stop), Direction::Downstream)
            .await
            .unwrap();
        assert!(worker.join().await.unwrap().errors.is_empty());
        drop(worker);
        released(&old_clock).await;
        let (pipeline, new_clock) = tracked_pipeline(processors);
        let error = match pipeline
            .start(PipelineConfig {
                startup_timeout: Duration::from_millis(50),
                ..Default::default()
            })
            .await
        {
            Ok(mut worker) => {
                worker.cancel("unexpected restart").await.unwrap();
                let _ = worker.join().await;
                panic!("ended aggregator restarted")
            }
            Err(error) => error,
        };
        assert!(
            !matches!(error, PipelineError::StartupTimeout),
            "ended session must reject before timeout"
        );
        released(&new_clock).await;
    }
}

#[derive(Default)]
struct EffectRoute {
    current: Mutex<Option<ProcessorContext>>,
    blocked: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl FrameProcessor for EffectRoute {
    fn name(&self) -> &str {
        "retained-effect-route"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Flush(_)) {
            *self.current.lock().unwrap() = Some(ctx.clone());
        }
        if matches!(&frame.data, FrameData::Text(text) if text.text == "block") {
            *self.current.lock().unwrap() = Some(ctx.clone());
            self.blocked.notify_one();
            self.release.notified().await;
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.current.lock().unwrap().take();
        Ok(())
    }
}
#[tokio::test]
async fn retained_timed_action_clones_release_runtime_after_each_terminal() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
    ] {
        for emit_successfully in [false, true] {
            let pair = pair();
            let route = Arc::new(EffectRoute::default());
            let (pipeline, weak) =
                tracked_pipeline(vec![pair.user(), route.clone(), pair.assistant()]);
            let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
            arm(&pair, &mut worker).await;
            let retained = pair
                .user()
                .advance(pair.user().deadline().unwrap().unwrap())
                .unwrap();
            let clone = retained.clone();
            assert_eq!(retained.actions.len(), 1);
            let current = route.current.lock().unwrap().take().unwrap();
            if emit_successfully {
                retained.clone().emit(&current).await.unwrap();
            }
            drop(current);
            worker
                .sender()
                .send(Frame::new(terminal.clone()), Direction::Downstream)
                .await
                .unwrap();
            assert!(worker.join().await.unwrap().errors.is_empty());
            drop(worker);
            released(&weak).await;
            // Keep both inspectable clones alive across the release assertion.
            assert_eq!(retained.actions.len(), 1);
            assert_eq!(clone.actions.len(), 1);
            assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
        }
    }
}
#[tokio::test]
async fn retained_timed_action_clones_release_runtime_after_failed_emission() {
    let pair = pair();
    let route = Arc::new(EffectRoute::default());
    let (pipeline, weak) = tracked_pipeline(vec![pair.user(), route.clone(), pair.assistant()]);
    let mut worker = pipeline
        .start(PipelineConfig {
            queue_capacity: 4,
            ..Default::default()
        })
        .await
        .unwrap();
    arm(&pair, &mut worker).await;
    let retained = pair
        .user()
        .advance(pair.user().deadline().unwrap().unwrap())
        .unwrap();
    let clone = retained.clone();
    route.current.lock().unwrap().take();
    worker
        .sender()
        .send(Frame::text("block"), Direction::Downstream)
        .await
        .unwrap();
    route.blocked.notified().await;
    let current = route.current.lock().unwrap().take().unwrap();
    for index in 0..4 {
        current
            .queue(Frame::text(format!("fill-{index}")), Direction::Downstream)
            .await
            .unwrap();
    }
    assert!(retained
        .clone()
        .emit(&current)
        .await
        .unwrap_err()
        .message
        .to_lowercase()
        .contains("full"));
    drop(current);
    route.release.notify_one();
    assert!(matches!(
        worker.join().await.unwrap_err(),
        PipelineError::Backpressure(_)
    ));
    drop(worker);
    released(&weak).await;
    assert_eq!(retained.actions.len(), 1);
    assert_eq!(clone.actions.len(), 1);
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
}
