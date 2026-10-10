//! Runtime lifecycle and deterministic-clock checks for the processor adapters.
use pipecat_core::*;
use pipecat_services::{Content, Context};
use pipecat_turns::*;
use std::sync::Arc;
use std::time::Duration;

fn transcription(text: &str) -> Frame {
    Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    }))
}
fn configured() -> Arc<LLMUserAggregator> {
    Arc::new(LLMUserAggregator::new(
        SharedContext::new(Context::default()),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                enable_interruptions: false,
                speech_timeout: Duration::from_secs(60),
                stop_timeout: Duration::from_secs(90),
                ..Default::default()
            },
            ..Default::default()
        },
    ))
}
async fn recv_until(
    worker: &mut PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    loop {
        let next = tokio::time::timeout(Duration::from_secs(2), worker.recv())
            .await
            .expect("frame deadline")
            .expect("worker output");
        if predicate(&next.frame.data) {
            return next;
        }
    }
}
async fn barrier(worker: &mut PipelineWorker) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::Flush(FlushFrame {
                returning: false,
                origin: Some("test-barrier".into()),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    recv_until(worker, |data| matches!(data, FrameData::Flush(_))).await;
}
#[tokio::test(start_paused = true)]
async fn timer_emits_a_context_once_without_further_input() {
    let user = configured();
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    worker
        .sender()
        .send(transcription("hello"), Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    tokio::time::advance(Duration::from_secs(60)).await;
    let out = recv_until(&mut worker, |data| matches!(data, FrameData::LlmContext(_))).await;
    let FrameData::LlmContext(context) = out.frame.data else {
        unreachable!()
    };
    assert_eq!(context.messages[0].content, "hello");
    tokio::time::advance(Duration::from_secs(300)).await;
    barrier(&mut worker).await;
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
#[tokio::test(start_paused = true)]
async fn end_cancels_silence_timer_before_drain_and_flushes_immediately() {
    let user = configured();
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    worker
        .sender()
        .send(transcription("pending final"), Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    let ended_at = tokio::time::Instant::now();
    worker.end().await.unwrap();
    let report = tokio::time::timeout(Duration::from_millis(100), worker.join())
        .await
        .expect("End must not wait for the 60-second timer")
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(ended_at.elapsed() < Duration::from_millis(100));
    assert_eq!(
        user.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("pending final".into()))
    );
}
#[tokio::test(start_paused = true)]
async fn cancel_cleans_timer_and_never_commits_an_interim_hypothesis() {
    let user = configured();
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::InterimTranscription(TranscriptionFrame {
                text: "unconfirmed".into(),
                ..Default::default()
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    barrier(&mut worker).await;
    worker.cancel("test").await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Cancelled);
    assert!(user.context().snapshot().unwrap().messages.is_empty());
}
#[tokio::test(start_paused = true)]
async fn repeated_finals_reuse_one_owned_timer_at_small_task_limit() {
    let user = configured();
    let config = PipelineConfig {
        max_background_tasks: 1,
        ..Default::default()
    };
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(config)
        .await
        .unwrap();
    recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    for _ in 0..200 {
        worker
            .sender()
            .send(transcription("word"), Direction::Downstream)
            .await
            .unwrap();
        barrier(&mut worker).await;
    }
    worker.end().await.unwrap();
    let report = worker.join().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    let Some(Content::Text(text)) = &user.context().snapshot().unwrap().messages[0].content else {
        panic!("expected text")
    };
    assert_eq!(text.split_whitespace().count(), 200);
}
#[tokio::test(start_paused = true)]
async fn initially_muted_start_reaches_downstream_before_mute_signal() {
    let user = Arc::new(LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            initially_muted: true,
            ..Default::default()
        },
    ));
    let mut worker = Pipeline::new(vec![user])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let first = worker.recv().await.unwrap();
    assert!(matches!(first.frame.data, FrameData::Start(_)));
    let mute = worker.recv().await.unwrap();
    assert!(matches!(mute.frame.data, FrameData::UserMuteStarted));
    worker.end().await.unwrap();
    worker.join().await.unwrap();
}
