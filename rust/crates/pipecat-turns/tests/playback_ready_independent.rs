//! Independent readiness ownership probes; source-only until a frozen gate run.
use pipecat_core::*;
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator, SharedContext,
};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct CaptureFence(Mutex<Option<InvocationFence>>);
#[async_trait]
impl FrameProcessor for CaptureFence {
    fn name(&self) -> &str {
        "independent-readiness-fence"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.0.lock().unwrap() = Some(ctx.invocation_fence()?);
        }
        ctx.push(frame, direction).await
    }
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn drain(worker: &mut PipelineWorker) {
    let frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("independent-readiness-registry".into()),
        returning: false,
    }));
    let id = frame.id;
    send(worker, frame).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if frame.frame.id == id {
                return;
            }
        }
        panic!("pipeline ended before readiness registry fence");
    })
    .await
    .unwrap();
}
async fn fixture() -> (Arc<LLMAssistantAggregator>, PipelineWorker, InvocationFence) {
    let assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let capture = Arc::new(CaptureFence::default());
    let mut worker = Pipeline::new(vec![capture.clone(), assistant.clone()])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, Frame::text("capture invocation only")).await;
    drain(&mut worker).await;
    let cause = capture.0.lock().unwrap().clone().unwrap();
    (assistant, worker, cause)
}
async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty());
}

#[tokio::test]
async fn queued_success_rechecks_original_scope_when_wait_first_polls() {
    let (assistant, mut worker, cause) = fixture().await;
    let frame = Frame::new(FrameData::LlmResponseStart);
    let ready = assistant
        .playback_readiness(&frame, cause.clone())
        .unwrap()
        .unwrap();
    send(&worker, frame).await;
    drain(&mut worker).await; // Successful actual assistant invocation has resolved readiness.
    send(&worker, Frame::interruption()).await;
    drain(&mut worker).await;
    assert!(!cause.is_current());
    assert!(
        tokio::time::timeout(Duration::from_secs(1), ready.wait())
            .await
            .unwrap()
            .is_err(),
        "an unpolled queued success must not renew a cancelled original scope"
    );
    finish(worker).await;
}

#[tokio::test]
async fn dropping_old_resolved_ticket_cannot_retire_a_new_same_identity_registration() {
    let (assistant, mut worker, cause) = fixture().await;
    let frame = Frame::new(FrameData::LlmResponseStart);
    let old = assistant
        .playback_readiness(&frame, cause.clone())
        .unwrap()
        .unwrap();
    send(&worker, frame.clone()).await;
    drain(&mut worker).await;
    // An implementation may reject all re-registration of consumed identities.
    // If it permits a new pending registration, old ownership cannot retire it.
    if let Ok(Some(new)) = assistant.playback_readiness(&frame, cause) {
        drop(old);
        let wait = new.wait();
        tokio::pin!(wait);
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut wait)
                .await
                .is_err(),
            "dropping a resolved old ticket removed the replacement reservation"
        );
        send(&worker, frame).await;
        drain(&mut worker).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(1), &mut wait)
                .await
                .unwrap()
                .is_err(),
            "replayed native Start must be ignored/rejected, not acknowledged"
        );
    } else {
        drop(old);
    }
    finish(worker).await;
}

#[tokio::test]
async fn unpolled_ticket_drop_releases_capacity_and_native_clones_are_single_registration() {
    let (assistant, worker, cause) = fixture().await;
    let frames: Vec<_> = (0..17)
        .map(|_| Frame::new(FrameData::LlmResponseStart))
        .collect();
    let mut tickets = Vec::new();
    for frame in &frames[..16] {
        tickets.push(
            assistant
                .playback_readiness(frame, cause.clone())
                .unwrap()
                .unwrap(),
        );
    }
    assert!(assistant
        .playback_readiness(&frames[0].clone(), cause.clone())
        .is_err());
    assert!(assistant
        .playback_readiness(&frames[16], cause.clone())
        .is_err());
    drop(tickets.pop());
    let replacement = assistant
        .playback_readiness(&frames[16], cause)
        .unwrap()
        .unwrap();
    // None of these frames reaches the assistant. Terminal cleanup must resolve
    // every pending registration without an auxiliary task or a retained frame.
    finish(worker).await;
    tickets.push(replacement);
    for ticket in tickets {
        assert!(tokio::time::timeout(Duration::from_secs(1), ticket.wait())
            .await
            .unwrap()
            .is_err());
    }
}
