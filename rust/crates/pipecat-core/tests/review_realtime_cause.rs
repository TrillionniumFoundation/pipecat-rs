//! Independent public-API admission, sealing, and non-owning runtime regression tests.
use pipecat_core::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn snapshot(text: &str) -> Frame {
    Frame::new(FrameData::LlmContext(LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, text)],
        ..Default::default()
    }))
}
fn cause(frame: &Frame) -> Arc<NativeRealtimeContextCause> {
    frame
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .clone()
}
#[test]
fn review_failed_attachment_preserves_sequence_and_clone_consumption_is_local() {
    let producer = NativeRealtimeContextProducer::default();
    for (mut frame, intent) in [
        (
            snapshot("range"),
            RealtimeContextIntent::Append { start: 1, end: 0 },
        ),
        (
            snapshot("range"),
            RealtimeContextIntent::Append { start: 0, end: 2 },
        ),
        (
            snapshot(&"x".repeat(1024 * 1024)),
            RealtimeContextIntent::Run,
        ),
        (Frame::text("wrong type"), RealtimeContextIntent::Run),
    ] {
        let before = frame.clone();
        assert!(producer.attach(&mut frame, intent, true).is_err());
        assert!(frame.native_realtime_context_cause().unwrap().is_none());
        assert_eq!(frame, before);
    }
    let mut first = snapshot("private context content");
    producer
        .attach(
            &mut first,
            RealtimeContextIntent::Append { start: 0, end: 1 },
            false,
        )
        .unwrap();
    let retained = first.clone();
    let original = cause(&first);
    assert_eq!(original.sequence(), 1);
    assert!(!original.run_llm());
    assert!(original.inference_fence().is_none());
    assert!(!format!("{original:?}").contains("private context content"));
    assert!(serde_json::to_value(&retained).is_err());
    assert!(retained.require_no_native_llm_continuation().is_err());
    assert!(Arc::ptr_eq(
        &first.take_native_realtime_context_cause().unwrap().unwrap(),
        &original
    ));
    assert!(serde_json::to_value(&first).is_ok());
    assert!(Arc::ptr_eq(&cause(&retained), &original));
    let restored: Frame = serde_json::from_value(serde_json::to_value(first).unwrap()).unwrap();
    assert!(restored.native_realtime_context_cause().unwrap().is_none());
}
#[test]
fn review_check_without_commit_and_rejected_sequences_do_not_advance_cursor() {
    let producer = NativeRealtimeContextProducer::default();
    let frames: Vec<_> = (0..3)
        .map(|_| {
            let mut frame = snapshot("identical explicit text remains distinct");
            producer
                .attach(&mut frame, RealtimeContextIntent::Run, true)
                .unwrap();
            frame
        })
        .collect();
    let mut admission = RealtimeContextAdmission::default();
    assert!(admission.check(&cause(&frames[1])).is_err());
    assert!(admission.check(&cause(&frames[0])).unwrap());
    assert!(admission.check(&cause(&frames[0])).unwrap());
    admission.commit(cause(&frames[0]));
    assert!(!admission.check(&cause(&frames[0])).unwrap());
    assert!(admission.check(&cause(&frames[2])).is_err());
    let mut alien = snapshot("identical explicit text remains distinct");
    NativeRealtimeContextProducer::default()
        .attach(&mut alien, RealtimeContextIntent::Run, true)
        .unwrap();
    assert!(admission.check(&cause(&alien)).is_err());
    assert!(admission.check(&cause(&frames[1])).unwrap());
    admission.commit(cause(&frames[1]));
    assert!(admission.check(&cause(&frames[0])).is_err());
    assert!(admission.check(&cause(&frames[2])).unwrap());
}
#[test]
fn review_direct_handler_seal_does_not_retain_callback_after_frame_drop() {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let mut frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![],
            tools: ToolSet::Direct(vec![DirectFunction {
                schema: ToolDefinition {
                    name: "lookup".into(),
                    description: String::new(),
                    parameters: serde_json::json!({"type":"object"}),
                },
                handler: LocalToolFunction::new(move |value| {
                    let resource = resource.clone();
                    async move {
                        let _held = resource;
                        Ok(value)
                    }
                }),
            }]),
            tool_choice: None,
        },
        speculation: false,
    }));
    NativeRealtimeContextProducer::default()
        .attach(&mut frame, RealtimeContextIntent::Run, false)
        .unwrap();
    let retained_cause = cause(&frame);
    drop(frame);
    assert!(
        weak.upgrade().is_none(),
        "cause digest retained a local callback or its resource"
    );
    assert!(!retained_cause.run_llm());
}

#[derive(Default)]
struct BindAndEmit {
    captured: Mutex<Vec<Frame>>,
}
#[async_trait]
impl FrameProcessor for BindAndEmit {
    fn name(&self) -> &str {
        "independent-cause-binding"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmContext(_)) {
            frame.bind_realtime_context_invocation(ctx)?;
            self.captured.lock().unwrap().push(frame.clone());
        }
        if matches!(&frame.data, FrameData::Text(text) if text.text == "try stale output") {
            let fence = cause(&self.captured.lock().unwrap()[0])
                .inference_fence()
                .unwrap()
                .clone();
            ctx.try_push_with_invocation_fence(
                Frame::text("stale output leaked"),
                direction,
                &fence,
            )?;
        }
        ctx.push(frame, direction).await
    }
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn review_rebinding_same_carrier_never_refreshes_original_fence_or_retains_processor() {
    let processor = Arc::new(BindAndEmit::default());
    let weak = Arc::downgrade(&processor);
    let producer = NativeRealtimeContextProducer::default();
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    let mut frame = snapshot("retained original invocation");
    producer
        .attach(&mut frame, RealtimeContextIntent::Run, true)
        .unwrap();
    worker
        .sender()
        .send(frame.clone(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::LlmContext(_)
    ));
    let original = cause(&frame);
    assert!(original.inference_fence().unwrap().is_current());
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    assert!(!original.inference_fence().unwrap().is_current());
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::LlmContext(_)
    ));
    assert!(!original.inference_fence().unwrap().is_current());
    assert!(original
        .inference_fence()
        .unwrap()
        .with_current(|| panic!("stale effect"))
        .is_none());
    worker
        .sender()
        .send(Frame::text("try stale output"), Direction::Downstream)
        .await
        .unwrap();
    assert!(
        matches!(next(&mut worker).await.frame.data, FrameData::Text(text) if text.text=="try stale output")
    );
    let mut fresh = snapshot("fresh invocation");
    producer
        .attach(&mut fresh, RealtimeContextIntent::Run, true)
        .unwrap();
    worker
        .sender()
        .send(fresh.clone(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::LlmContext(_)
    ));
    let fresh_cause = cause(&fresh);
    assert!(fresh_cause.inference_fence().unwrap().is_current());
    worker
        .sender()
        .send(Frame::new(FrameData::Stop), Direction::Downstream)
        .await
        .unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!fresh_cause.inference_fence().unwrap().is_current());
    drop(processor);
    assert!(
        weak.upgrade().is_none(),
        "retained causes owned the stopped pipeline or its processor"
    );
}
