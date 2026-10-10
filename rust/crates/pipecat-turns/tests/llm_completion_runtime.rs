//! Native pipeline/assistant integration and owned deadline lifecycle.
use pipecat_core::*;
use pipecat_services::{Content, Role};
use pipecat_turns::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Notify, Semaphore};

async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn receive(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let output = worker.recv().await.expect("pipeline output");
            if wanted(&output.frame.data) {
                return output;
            }
            assert!(
                !matches!(output.frame.data, FrameData::Error(_)),
                "unexpected error: {:?}",
                output.frame.data
            );
        }
    })
    .await
    .expect("output deadline")
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("completion-test".into()),
        }),
    )
    .await;
    receive(worker, |data| matches!(data, FrameData::Flush(_))).await;
}
async fn start(processors: Vec<Arc<dyn FrameProcessor>>) -> PipelineWorker {
    let mut worker = Pipeline::new(processors)
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, FrameData::is_start).await;
    worker
}
async fn response(worker: &PipelineWorker, text: &str) {
    send(worker, FrameData::LlmResponseStart).await;
    send(worker, FrameData::LlmText(TextFrame::llm(text))).await;
    send(worker, FrameData::LlmResponseEnd).await;
}
async fn finish(mut worker: PipelineWorker) {
    worker.end().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(1), worker.join())
        .await
        .expect("owned timer must not delay End")
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
}
fn marker() -> Arc<LLMTurnCompletionProcessor> {
    Arc::new(LLMTurnCompletionProcessor::new(Default::default()).unwrap())
}
fn history(context: &SharedContext) -> Vec<(Role, String)> {
    context
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|m| {
            let Some(Content::Text(text)) = m.content else {
                panic!("text history");
            };
            (m.role, text)
        })
        .collect()
}

#[tokio::test(start_paused = true)]
async fn complete_and_incomplete_markers_have_source_assistant_context_shape() {
    let shared = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut worker = start(vec![marker(), assistant]).await;
    response(&worker, "● Hello there").await;
    barrier(&mut worker).await;
    assert_eq!(
        history(&shared),
        [(Role::Assistant, "● Hello there".into())]
    );
    send(&worker, FrameData::UserStartedSpeaking).await;
    response(&worker, "◐ hidden words").await;
    barrier(&mut worker).await;
    assert_eq!(
        history(&shared),
        [
            (Role::Assistant, "● Hello there".into()),
            (Role::Assistant, "◐".into())
        ]
    );
    finish(worker).await;
}

#[tokio::test(start_paused = true)]
async fn short_and_long_deadlines_append_exactly_one_developer_retry_and_run_context() {
    for (symbol, delay, phrase) in [("◐", 5, "paused briefly"), ("○", 10, "quiet for a while")]
    {
        let shared = SharedContext::default();
        let assistant = Arc::new(LLMAssistantAggregator::new(
            shared.clone(),
            Default::default(),
        ));
        let mut worker = start(vec![marker(), assistant]).await;
        response(&worker, symbol).await;
        barrier(&mut worker).await;
        tokio::time::advance(Duration::from_secs(delay - 1)).await;
        barrier(&mut worker).await;
        assert_eq!(history(&shared).len(), 1);
        tokio::time::advance(Duration::from_secs(1)).await;
        let context = receive(&mut worker, |f| {
            matches!(
                f,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .await;
        assert_eq!(context.direction, Direction::Upstream);
        let messages = history(&shared);
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[1].0, Role::Developer);
        assert!(messages[1].1.contains(phrase));
        response(&worker, symbol).await;
        barrier(&mut worker).await;
        tokio::time::advance(Duration::from_secs(100)).await;
        barrier(&mut worker).await;
        assert_eq!(
            history(&shared).len(),
            3,
            "bounded retry budget must survive new response"
        );
        finish(worker).await;
    }
}

#[tokio::test(start_paused = true)]
async fn repeated_rearming_uses_one_supervisor_and_no_stale_prompt() {
    let mut worker = start(vec![marker()]).await;
    for _ in 0..200 {
        response(&worker, "◐").await;
        barrier(&mut worker).await;
    }
    send(
        &worker,
        FrameData::VadUserStartedSpeakingDetailed(Default::default()),
    )
    .await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    barrier(&mut worker).await;
    response(&worker, "● stale while speaking").await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    barrier(&mut worker).await;
    send(
        &worker,
        FrameData::VadUserStoppedSpeakingDetailed(Default::default()),
    )
    .await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    receive(&mut worker, |f| {
        matches!(f, FrameData::LlmMessagesTransform(_))
    })
    .await;
    finish(worker).await;
}

#[tokio::test(start_paused = true)]
async fn all_terminal_variants_cancel_timer_without_waiting_for_deadline() {
    for (terminal, expected) in [
        (
            FrameData::End(EndFrame { reason: None }),
            WorkerState::Ended,
        ),
        (
            FrameData::EndDetailed(Default::default()),
            WorkerState::Ended,
        ),
        (FrameData::Stop, WorkerState::Stopped),
        (
            FrameData::Cancel(CancelFrame { reason: None }),
            WorkerState::Cancelled,
        ),
        (
            FrameData::CancelDetailed(Default::default()),
            WorkerState::Cancelled,
        ),
    ] {
        let mut worker = start(vec![marker()]).await;
        response(&worker, "○").await;
        barrier(&mut worker).await;
        let before = tokio::time::Instant::now();
        send(&worker, terminal).await;
        let report = tokio::time::timeout(Duration::from_secs(1), worker.join())
            .await
            .expect("terminal must cancel owned supervisor")
            .unwrap();
        assert_eq!(report.state, expected);
        assert!(before.elapsed() < Duration::from_secs(1));
    }
}

async fn pending_transform(worker: &mut PipelineWorker) -> LocalMessageTransform {
    response(worker, "◐").await;
    barrier(worker).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let frame = receive(worker, |f| matches!(f, FrameData::LlmMessagesTransform(_))).await;
    let FrameData::LlmMessagesTransform(frame) = frame.frame.data else {
        unreachable!()
    };
    assert_eq!(frame.run_llm, Some(true));
    frame.transform
}

#[tokio::test(start_paused = true)]
async fn queued_retry_capability_rejects_replay_newer_response_speech_and_dead_owner() {
    let owner = marker();
    let mut worker = start(vec![owner.clone()]).await;
    let transform = pending_transform(&mut worker).await;
    let applied = transform.apply(vec![]).unwrap();
    assert_eq!(applied.len(), 1);
    assert!(transform.apply(vec![]).is_err());
    finish(worker).await;
    drop(owner);
    assert!(transform.apply(vec![]).is_err());

    for boundary in [
        FrameData::LlmResponseStart,
        FrameData::VadUserStartedSpeakingDetailed(Default::default()),
        FrameData::Interruption,
    ] {
        let mut worker = start(vec![marker()]).await;
        let transform = pending_transform(&mut worker).await;
        send(&worker, boundary).await;
        barrier(&mut worker).await;
        assert!(
            transform.apply(vec![]).is_err(),
            "queued retry must be checked at context consumption"
        );
        finish(worker).await;
    }
    let owner = marker();
    let mut worker = start(vec![owner.clone()]).await;
    let transform = pending_transform(&mut worker).await;
    finish(worker).await;
    drop(owner);
    assert!(transform.apply(vec![]).is_err());
}

struct RetryGate {
    arrived: Notify,
    release: Semaphore,
    speech_seen: Notify,
}
#[async_trait]
impl FrameProcessor for RetryGate {
    fn name(&self) -> &str {
        "RetryGate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmMessagesTransform(_)) {
            self.arrived.notify_one();
            self.release
                .acquire()
                .await
                .expect("gate remains open")
                .forget();
        }
        if matches!(frame.data, FrameData::VadUserStartedSpeakingDetailed(_)) {
            self.speech_seen.notify_one();
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(start_paused = true)]
async fn delayed_retry_cannot_mutate_assistant_context_after_user_resumes() {
    let shared = SharedContext::default();
    let gate = Arc::new(RetryGate {
        arrived: Notify::new(),
        release: Semaphore::new(0),
        speech_seen: Notify::new(),
    });
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut worker = start(vec![marker(), gate.clone(), assistant]).await;
    response(&worker, "◐").await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    gate.arrived.notified().await;
    send(
        &worker,
        FrameData::VadUserStartedSpeakingDetailed(Default::default()),
    )
    .await;
    gate.speech_seen.notified().await;
    gate.release.add_permits(1);
    let error = receive(&mut worker, |f| matches!(f, FrameData::Error(_))).await;
    assert!(
        matches!(&error.frame.data, FrameData::Error(e) if !e.fatal && e.error.contains("stale"))
    );
    barrier(&mut worker).await;
    assert_eq!(history(&shared), [(Role::Assistant, "◐".into())]);
    finish(worker).await;
}

#[tokio::test(start_paused = true)]
async fn stalled_incomplete_response_fails_explicitly_and_joins() {
    let mut worker = start(vec![marker()]).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("◐"))).await;
    barrier(&mut worker).await;
    tokio::time::advance(Duration::from_secs(5)).await;
    let result = tokio::time::timeout(Duration::from_secs(1), worker.join())
        .await
        .expect("stalled deadline terminates");
    assert!(
        result.is_err(),
        "stalled response cannot masquerade as success"
    );
}
