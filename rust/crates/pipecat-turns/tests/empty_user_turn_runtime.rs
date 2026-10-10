//! Empty recovery uses the existing owned supervisor and context admission path.
use pipecat_core::*;
use pipecat_services::Context;
use pipecat_turns::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}
fn config() -> UserAggregatorConfig {
    UserAggregatorConfig {
        turn: UserTurnConfig {
            speech_timeout: ms(100),
            stop_timeout: ms(200),
            ..Default::default()
        },
        empty_user_turn: Some(EmptyUserTurnConfig {
            idle_prompt: Some("Please repeat".into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn vad(start: bool) -> Frame {
    let value = VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    };
    Frame::new(if start {
        FrameData::VadUserStartedSpeaking(value)
    } else {
        FrameData::VadUserStoppedSpeaking(value)
    })
}
struct Inferences(Arc<AtomicUsize>);
#[async_trait]
impl FrameProcessor for Inferences {
    fn name(&self) -> &str {
        "Inferences"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
        ) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        ctx.push(frame, direction).await
    }
}
async fn recv(worker: &mut PipelineWorker, wanted: impl Fn(&FrameData) -> bool) -> DirectedFrame {
    tokio::time::timeout(ms(1000), async {
        loop {
            let frame = worker.recv().await.expect("open pipeline");
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("{error:?}");
            }
            if frame.direction == Direction::Downstream && wanted(&frame.frame.data) {
                return frame;
            }
        }
    })
    .await
    .expect("frame deadline")
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        Frame::new(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("barrier".into()),
        })),
    )
    .await;
    recv(worker, |data| matches!(data, FrameData::Flush(_))).await;
}
async fn boot() -> (LLMContextAggregatorPair, Arc<AtomicUsize>, PipelineWorker) {
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        config(),
        AssistantAggregatorConfig::default(),
    );
    let count = Arc::new(AtomicUsize::new(0));
    let mut worker = Pipeline::new(vec![
        pair.user(),
        Arc::new(Inferences(count.clone())),
        pair.assistant(),
    ])
    .start(PipelineConfig {
        max_background_tasks: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    recv(&mut worker, FrameData::is_start).await;
    (pair, count, worker)
}
async fn empty(worker: &mut PipelineWorker) {
    send(worker, vad(true)).await;
    barrier(worker).await;
    send(worker, vad(false)).await;
    barrier(worker).await;
    tokio::time::advance(ms(200)).await;
    recv(worker, |data| {
        matches!(data, FrameData::UserStoppedSpeaking)
    })
    .await;
    barrier(worker).await;
}

#[tokio::test(start_paused = true)]
async fn repeated_empty_turns_reuse_one_task_and_bound_actual_pipeline_requests() {
    let (pair, count, mut worker) = boot().await;
    empty(&mut worker).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    for _ in 0..32 {
        empty(&mut worker).await;
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    send(
        &worker,
        Frame::new(FrameData::Transcription(TranscriptionFrame {
            text: "real words".into(),
            finalized: true,
            ..Default::default()
        })),
    )
    .await;
    barrier(&mut worker).await;
    tokio::time::advance(ms(100)).await;
    recv(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    empty(&mut worker).await;
    assert_eq!(count.load(Ordering::SeqCst), 3);
    worker.end().await.unwrap();
    let report = worker.join().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.state, WorkerState::Ended);
}

#[tokio::test(start_paused = true)]
async fn end_stop_cancel_and_drop_never_emit_recovery_after_shutdown() {
    for terminal in ["end", "stop", "cancel", "drop"] {
        let (pair, count, mut worker) = boot().await;
        send(&worker, vad(true)).await;
        barrier(&mut worker).await;
        send(&worker, vad(false)).await;
        barrier(&mut worker).await;
        match terminal {
            "end" => worker.end().await.unwrap(),
            "stop" => send(&worker, Frame::new(FrameData::Stop)).await,
            "cancel" => worker.cancel("cancel empty turn").await.unwrap(),
            "drop" => {
                drop(worker);
                tokio::time::advance(ms(10_000)).await;
                tokio::task::yield_now().await;
                assert_eq!(count.load(Ordering::SeqCst), 0);
                assert!(pair.context().snapshot().unwrap().messages.is_empty());
                continue;
            }
            _ => unreachable!(),
        }
        let report = worker.join().await.unwrap();
        assert!(report.errors.is_empty(), "{terminal}: {:?}", report.errors);
        assert_eq!(
            report.state,
            if terminal == "cancel" {
                WorkerState::Cancelled
            } else if terminal == "stop" {
                WorkerState::Stopped
            } else {
                WorkerState::Ended
            }
        );
        tokio::time::advance(ms(10_000)).await;
        assert_eq!(count.load(Ordering::SeqCst), 0, "{terminal}");
        assert!(
            pair.context().snapshot().unwrap().messages.is_empty(),
            "{terminal}"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn pending_recovery_interruption_does_not_reset_the_loop_bound() {
    let (pair, count, mut worker) = boot().await;
    empty(&mut worker).await;
    send(&worker, Frame::interruption()).await;
    barrier(&mut worker).await;
    empty(&mut worker).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    worker.cancel("cancel pending recovery").await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Cancelled);
}
