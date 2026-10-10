//! Executable notification/completion route with the native task supervisor.
use pipecat_core::*;
use pipecat_services::{Content, Context, Role};
use pipecat_turns::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn config() -> UserAggregatorConfig {
    UserAggregatorConfig {
        turn: UserTurnConfig {
            enable_interruptions: false,
            stop_timeout: ms(10000),
            stop_strategies: Some(
                UserTurnStopStrategies::new(vec![
                    UserTurnStopStrategy::speech_timeout(ms(100), true).deferred(),
                    UserTurnStopStrategy::external_completion(),
                ])
                .unwrap(),
            ),
            ..Default::default()
        },
        ..Default::default()
    }
}
fn transcript(text: &str) -> Frame {
    Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    }))
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn recv_until(
    worker: &mut PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker.recv().await.expect("pipeline output");
            if let FrameData::Error(e) = &frame.frame.data {
                panic!("{e:?}");
            }
            if predicate(&frame.frame.data) {
                return frame;
            }
        }
    })
    .await
    .expect("frame deadline")
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
    recv_until(worker, |d| matches!(d, FrameData::Flush(_))).await;
}
struct ResponseCounter(Arc<AtomicUsize>);
#[async_trait]
impl FrameProcessor for ResponseCounter {
    fn name(&self) -> &str {
        "ResponseCounter"
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
        ) && direction == Direction::Downstream
        {
            self.0.fetch_add(1, Ordering::SeqCst);
            for data in [
                FrameData::LlmResponseStart,
                FrameData::LlmText(TextFrame::llm("accepted answer")),
                FrameData::LlmResponseEnd,
            ] {
                ctx.push(Frame::new(data), direction).await?;
            }
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(start_paused = true)]
async fn external_notification_holds_shared_history_and_downstream_inference_until_completion() {
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        config(),
        AssistantAggregatorConfig::default(),
    );
    let user = pair.user();
    let calls = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let observed = Arc::downgrade(&user);
    user.on_user_turn_inference_requested(move |r| {
        assert!(
            observed
                .upgrade()
                .unwrap()
                .context()
                .snapshot()?
                .messages
                .is_empty(),
            "callback is outside the context lock"
        );
        tx.send(r)
            .map_err(|_| ProcessorError::new("closed notification receiver"))
    })
    .unwrap();
    let mut worker = Pipeline::new(vec![
        user.clone(),
        Arc::new(ResponseCounter(calls.clone())),
        pair.assistant(),
    ])
    .start(PipelineConfig {
        max_background_tasks: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    recv_until(&mut worker, |d| d.is_start()).await;
    send(&worker, transcript("hello")).await;
    barrier(&mut worker).await;
    tokio::time::advance(ms(100)).await;
    let request = rx.recv().await.unwrap();
    assert_eq!(request.text, "hello");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    barrier(&mut worker).await;
    assert!(rx.try_recv().is_err());
    send(&worker, request.completion_frame()).await;
    recv_until(&mut worker, |d| matches!(d, FrameData::LlmContext(c) if c.messages.last().is_some_and(|m| m.content=="accepted answer"))).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let snapshot = user.context().snapshot().unwrap();
    assert_eq!(snapshot.messages.len(), 2);
    assert_eq!(snapshot.messages[0].role, Role::User);
    assert_eq!(
        snapshot.messages[1].content,
        Some(Content::Text("accepted answer".into()))
    );
    send(&worker, request.completion_frame()).await;
    barrier(&mut worker).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
#[tokio::test(start_paused = true)]
async fn many_revisions_share_one_supervisor_and_cancel_stale_ids() {
    let user = Arc::new(LLMUserAggregator::new(SharedContext::default(), config()));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    user.on_user_turn_inference_requested(move |r| {
        tx.send(r).map_err(|_| ProcessorError::new("closed"))
    })
    .unwrap();
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    recv_until(&mut worker, |d| d.is_start()).await;
    for i in 0..100 {
        send(&worker, transcript(&format!("part{i}"))).await;
        barrier(&mut worker).await;
        tokio::time::advance(ms(100)).await;
        let request = rx.recv().await.unwrap();
        send(
            &worker,
            Frame::new(FrameData::InterimTranscription(TranscriptionFrame {
                text: "pending".into(),
                ..Default::default()
            })),
        )
        .await;
        barrier(&mut worker).await;
        send(&worker, request.completion_frame()).await;
        barrier(&mut worker).await;
        assert!(user.context().snapshot().unwrap().messages.is_empty());
    }
    worker.cancel("done").await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Cancelled);
    assert_eq!(user.pending_inference_request().unwrap(), None);
    assert_eq!(user.deadline().unwrap(), None);
}
#[tokio::test(start_paused = true)]
async fn end_and_stop_cancel_pending_deadlines_before_task_drain() {
    for stop in [false, true] {
        let user = Arc::new(LLMUserAggregator::new(SharedContext::default(), config()));
        let notifications = Arc::new(AtomicUsize::new(0));
        let count = notifications.clone();
        user.on_user_turn_inference_requested(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
        let mut worker = Pipeline::new(vec![user.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv_until(&mut worker, |d| d.is_start()).await;
        send(&worker, transcript("final")).await;
        barrier(&mut worker).await;
        let start = tokio::time::Instant::now();
        if stop {
            send(&worker, Frame::new(FrameData::Stop)).await;
        } else {
            worker.end().await.unwrap();
        }
        let report = worker.join().await.unwrap();
        assert!(matches!(
            report.state,
            WorkerState::Ended | WorkerState::Stopped
        ));
        assert!(start.elapsed() < ms(100));
        assert_eq!(notifications.load(Ordering::SeqCst), 0);
        assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn detector_deadline_does_not_fire_after_resume_until_fresh_stop() {
    let user = Arc::new(LLMUserAggregator::new(SharedContext::default(), config()));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    user.on_user_turn_inference_requested(move |r| {
        tx.send(r).map_err(|_| ProcessorError::new("closed"))
    })
    .unwrap();
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    recv_until(&mut worker, |d| d.is_start()).await;
    send(&worker, transcript("hello")).await;
    barrier(&mut worker).await;
    send(
        &worker,
        Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            timestamp: 0.0,
            duration_secs: 0.2,
        })),
    )
    .await;
    barrier(&mut worker).await;
    tokio::time::advance(ms(20000)).await;
    barrier(&mut worker).await;
    assert!(rx.try_recv().is_err());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(
        &worker,
        Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            timestamp: 0.0,
            duration_secs: 0.2,
        })),
    )
    .await;
    barrier(&mut worker).await;
    tokio::time::advance(ms(100)).await;
    assert_eq!(rx.recv().await.unwrap().text, "hello");
    worker.cancel("done").await.unwrap();
    worker.join().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn notification_failure_preserves_other_handlers_and_watchdog() {
    let user = Arc::new(LLMUserAggregator::new(SharedContext::default(), config()));
    user.on_user_turn_inference_requested(|_| {
        Err(ProcessorError::new("classifier notification unavailable"))
    })
    .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    user.on_user_turn_inference_requested(move |request| {
        tx.send(request).map_err(|_| ProcessorError::new("closed"))
    })
    .unwrap();
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    recv_until(&mut worker, |d| d.is_start()).await;
    send(&worker, transcript("accepted")).await;
    barrier(&mut worker).await;
    tokio::time::advance(ms(100)).await;
    let request = rx.recv().await.unwrap();
    assert_eq!(request.text, "accepted");
    let error = worker.recv().await.unwrap();
    assert!(
        matches!(error.frame.data, FrameData::Error(ref e) if !e.fatal && e.error.contains("notification unavailable"))
    );
    tokio::time::advance(ms(10000)).await;
    recv_until(&mut worker, |d| matches!(d, FrameData::LlmContext(_))).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
