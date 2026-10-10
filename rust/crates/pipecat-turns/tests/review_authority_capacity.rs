//! Accepted signed calls cannot lose authority while still awaiting settlement.
use pipecat_core::*;
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Capture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "IndependentSignedCapacityCapture"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.0.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}
fn attached(data: FrameData, history: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(Arc::new(history))
        .unwrap();
    frame
}
#[tokio::test]
async fn independent_accepted_signed_declarations_do_not_evict_unsettled_authority() {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let calls: Vec<_> = (0..513)
        .map(|id| FunctionCall {
            function_name: "effect".into(),
            tool_call_id: format!("signed-pending-{id}"),
            arguments: json!({}),
        })
        .collect();
    assistant
        .process_frame(
            attached(
                FrameData::LlmResponseStart,
                NativeLlmContinuation::start(6001, LlmContinuationProvider::Anthropic).unwrap(),
            ),
            Direction::Downstream,
        )
        .unwrap();
    let history = NativeLlmContinuation::new(
        6001,
        LlmContinuationProvider::Anthropic,
        calls
            .iter()
            .cloned()
            .map(LlmContinuationBlock::ToolCall)
            .collect(),
    )
    .unwrap();
    assistant
        .process_frame(
            attached(FrameData::FunctionCallsStarted(calls.clone()), history),
            Direction::Downstream,
        )
        .expect(
            "valid bounded signed declarations should be admitted before their native settlement",
        );
    assistant
        .process_frame(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .unwrap();
    ctx.tool_outcome_publisher(calls[0].clone(), true)
        .unwrap()
        .complete(
            Ok(json!("committed")),
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
    let frame = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = worker.recv().await.unwrap().frame;
            if frame.native_tool_outcome().unwrap().is_some() {
                break frame;
            }
        }
    })
    .await
    .unwrap();
    assistant
        .process_frame(frame, Direction::Downstream)
        .expect("accepted signed identity was evicted before its terminal receipt arrived");
    assert!(context
        .snapshot_rich()
        .unwrap()
        .messages
        .iter()
        .any(
            |message| message.tool_call_id.as_deref() == Some("signed-pending-0")
                && message.content == MessageContent::Text("\"committed\"".into())
        ));
    drop(ctx);
    drop(capture);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
