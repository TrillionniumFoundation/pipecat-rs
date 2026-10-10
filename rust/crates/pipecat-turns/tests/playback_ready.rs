use pipecat_core::*;
use pipecat_turns::*;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
const LIMIT: Duration = Duration::from_secs(2);
#[derive(Default)]
struct Capture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "readiness-origin"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.0.lock().unwrap() = Some(ctx.clone());
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.lock().unwrap().take();
        Ok(())
    }
}
async fn barrier(worker: &mut PipelineWorker) {
    let frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("readiness-test".into()),
        returning: false,
    }));
    let id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(LIMIT, async {
        while let Some(frame) = worker.recv().await {
            if frame.frame.id == id {
                return;
            }
        }
        panic!("closed before barrier");
    })
    .await
    .unwrap();
}
async fn fixture() -> (
    Arc<LLMAssistantAggregator>,
    PipelineWorker,
    ProcessorContext,
) {
    let assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone(), assistant.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("capture"), Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    let ctx = capture.0.lock().unwrap().clone().unwrap();
    (assistant, worker, ctx)
}
#[tokio::test]
async fn reservations_are_bounded_unique_and_released_without_polling() {
    let (assistant, worker, ctx) = fixture().await;
    let frames: Vec<_> = (0..17)
        .map(|_| Frame::new(FrameData::LlmResponseStart))
        .collect();
    let mut tickets = Vec::new();
    for frame in &frames[..16] {
        tickets.push(
            assistant
                .playback_readiness(frame, ctx.invocation_fence().unwrap())
                .unwrap()
                .unwrap(),
        );
    }
    assert!(assistant
        .playback_readiness(&frames[0], ctx.invocation_fence().unwrap())
        .is_err());
    assert!(assistant
        .playback_readiness(&frames[16], ctx.invocation_fence().unwrap())
        .is_err());
    drop(tickets.pop());
    let next = assistant
        .playback_readiness(&frames[16], ctx.invocation_fence().unwrap())
        .unwrap()
        .unwrap();
    drop(next);
    drop(tickets);
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}
#[tokio::test]
async fn a_direct_reducer_and_public_id_copy_cannot_certify_native_readiness() {
    let (assistant, mut worker, ctx) = fixture().await;
    let frame = Frame::new(FrameData::LlmResponseStart);
    let ticket = assistant
        .playback_readiness(&frame, ctx.invocation_fence().unwrap())
        .unwrap()
        .unwrap();
    assistant
        .process_frame(frame.clone(), Direction::Downstream)
        .unwrap();
    let mut copy = Frame::new(FrameData::LlmResponseStart);
    copy.id = frame.id;
    worker
        .sender()
        .send(copy, Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), ticket.wait())
            .await
            .is_err()
    );
    // Replaying the actual identity is ignored by the reducer and rejected,
    // even though process_at_inner itself returns Ok for a seen frame.
    let replay = assistant
        .playback_readiness(&frame, ctx.invocation_fence().unwrap())
        .unwrap()
        .unwrap();
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    assert!(tokio::time::timeout(LIMIT, replay.wait())
        .await
        .unwrap()
        .is_err());
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
#[tokio::test]
async fn ready_result_rechecks_original_scope_before_first_poll() {
    let (assistant, mut worker, ctx) = fixture().await;
    let frame = Frame::new(FrameData::LlmResponseStart);
    let ticket = assistant
        .playback_readiness(&frame, ctx.invocation_fence().unwrap())
        .unwrap()
        .unwrap();
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    assert!(ticket.wait().await.is_err());
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
#[tokio::test]
async fn purged_boundary_and_owner_cleanup_release_waiters() {
    let (assistant, worker, ctx) = fixture().await;
    let frame = Frame::new(FrameData::LlmResponseStart);
    let ticket = assistant
        .playback_readiness(&frame, ctx.invocation_fence().unwrap())
        .unwrap()
        .unwrap();
    drop(frame);
    assert!(tokio::time::timeout(LIMIT, ticket.wait())
        .await
        .unwrap()
        .is_err());
    let frame = Frame::new(FrameData::LlmResponseStart);
    let ticket = assistant
        .playback_readiness(&frame, ctx.invocation_fence().unwrap())
        .unwrap()
        .unwrap();
    drop(worker);
    assert!(tokio::time::timeout(LIMIT, ticket.wait())
        .await
        .unwrap()
        .is_err());
    assistant.cleanup().await.unwrap();
    assert!(assistant
        .playback_readiness(&frame, ctx.invocation_fence().unwrap())
        .is_err());
}
#[tokio::test]
async fn failed_outward_tts_start_admission_does_not_release_readiness() {
    let (_origin_assistant, origin, ctx) = fixture().await;
    let assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let mut worker = Pipeline::new(vec![assistant.clone()])
        .start(PipelineConfig {
            output_capacity: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Start(_)
    ));
    worker
        .sender()
        .send(
            Frame::new(FrameData::Flush(FlushFrame {
                origin: None,
                returning: false,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let frame = Frame::new(FrameData::TtsStarted(TtsStartedFrame {
        context_id: Some("full".into()),
        append_to_context: true,
    }));
    let ready = assistant
        .playback_readiness(&frame, ctx.invocation_fence().unwrap())
        .unwrap()
        .unwrap();
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    assert!(tokio::time::timeout(LIMIT, ready.wait())
        .await
        .unwrap()
        .is_err());
    assert!(matches!(
        tokio::time::timeout(LIMIT, worker.join()).await.unwrap(),
        Err(PipelineError::Backpressure(_))
    ));
    origin.end().await.unwrap();
    origin.collect().await.unwrap();
}
#[tokio::test]
async fn a_closed_reducer_cannot_certify_an_ignored_start() {
    let (assistant, worker, ctx) = fixture().await;
    assistant
        .process_frame(
            Frame::new(FrameData::End(Default::default())),
            Direction::Downstream,
        )
        .unwrap();
    let frame = Frame::new(FrameData::LlmResponseStart);
    let ready = assistant
        .playback_readiness(&frame, ctx.invocation_fence().unwrap())
        .unwrap()
        .unwrap();
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    assert!(tokio::time::timeout(LIMIT, ready.wait())
        .await
        .unwrap()
        .is_err());
    // This fixture deliberately ends the reducer and discards its effects out
    // of band, so it cannot forward a second pipeline terminal. Owner-drop is
    // the supported cancellation path; release captured owners and verify join
    // cleanup releases the processor instead of asserting ordinary End drainage.
    let weak = Arc::downgrade(&assistant);
    drop(worker);
    drop(ctx);
    drop(assistant);
    tokio::time::timeout(LIMIT, async {
        while weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn terminal_reducer_failure_closes_tickets_before_cleanup_and_preserves_committed_history() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
    ] {
        let (assistant, worker, ctx) = fixture().await;
        assistant.begin_playback("committed").unwrap();
        assistant
            .confirm_playback(
                "committed",
                &[AggregatedTextFrame {
                    text: TextFrame::new("Already heard"),
                    context_id: Some("committed".into()),
                    raw_text: None,
                    aggregated_by: "test".into(),
                    will_be_spoken: true,
                }],
            )
            .unwrap();
        let before = assistant.context().frame_data().unwrap();
        let pending = Frame::new(FrameData::LlmResponseStart);
        let ready = assistant
            .playback_readiness(&pending, ctx.invocation_fence().unwrap())
            .unwrap()
            .unwrap();
        let mut invalid_terminal = Frame::new(FrameData::LlmResponseStart);
        invalid_terminal
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(
                    NativeLlmContinuation::next_response_id().unwrap(),
                    LlmContinuationProvider::Anthropic,
                )
                .unwrap(),
            ))
            .unwrap();
        // Public payload mutation must be rejected by native-carrier validation.
        // Call the real processor entry with a live invocation and do not run
        // cleanup first: closure must precede the injected reducer error.
        invalid_terminal.data = terminal;
        assert!(assistant
            .process(invalid_terminal, Direction::Downstream, &ctx)
            .await
            .is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(100), ready.wait())
                .await
                .unwrap()
                .is_err()
        );
        assert!(assistant
            .playback_readiness(&pending, ctx.invocation_fence().unwrap())
            .is_err());
        assert_eq!(assistant.context().frame_data().unwrap(), before);
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
    }
}
