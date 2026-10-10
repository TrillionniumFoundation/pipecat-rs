//! Shared assistant alone determines whether a native terminal creates new intent.
use pipecat_core::*;
use pipecat_services::Context;
use pipecat_turns::*;
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
        "NativeSchedulingPublisher"
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
fn call(id: &str) -> Arc<FunctionCall> {
    Arc::new(FunctionCall {
        function_name: "work".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    })
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
fn causes(output: AggregationOutput) -> Vec<Arc<NativeRealtimeContextCause>> {
    output
        .actions
        .into_iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame) if frame.direction == Direction::Upstream => frame
                .frame
                .native_realtime_context_cause()
                .unwrap()
                .cloned(),
            _ => None,
        })
        .collect()
}
fn declare(assistant: &LLMAssistantAggregator, batch: &Arc<NativeToolCallBatch>) {
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(
        batch
            .members()
            .iter()
            .map(|(call, _)| (**call).clone())
            .collect(),
    ));
    frame.set_native_tool_batch(batch.clone()).unwrap();
    assistant
        .process_frame(frame, Direction::Downstream)
        .unwrap();
    assistant
        .process_frame(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .unwrap();
}
async fn terminal(
    worker: &mut PipelineWorker,
    ctx: &ProcessorContext,
    batch: &Arc<NativeToolCallBatch>,
    id: &str,
    run: bool,
) -> Frame {
    let (call, policy) = batch
        .members()
        .iter()
        .find(|(call, _)| call.tool_call_id == id)
        .unwrap();
    ctx.tool_outcome_publisher(call.clone(), *policy)
        .unwrap()
        .with_batch(batch.clone())
        .unwrap()
        .cancel(
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::TimedOut,
            run,
        );
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = worker.recv().await.unwrap().frame;
            if frame.native_tool_outcome().unwrap().is_some() {
                break frame;
            }
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn false_native_cancellation_releases_only_current_preexisting_owed_request_once() {
    // No owed request; live owed request; interruption clears owed request.
    for mode in 0..3 {
        let capture = Arc::new(Capture::default());
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(Default::default())
            .await
            .unwrap();
        let ctx = capture.0.lock().unwrap().take().unwrap();
        let pair = pair();
        let assistant = pair.assistant();
        let first =
            NativeToolCallBatch::new("first".into(), vec![(call("a"), false), (call("b"), false)])
                .unwrap();
        let other = NativeToolCallBatch::new("other".into(), vec![(call("c"), false)]).unwrap();
        declare(&assistant, &first);
        declare(&assistant, &other);
        if mode != 0 {
            assert!(causes(
                assistant
                    .process_frame(Frame::new(FrameData::LlmRun), Direction::Downstream)
                    .unwrap()
            )
            .is_empty());
        }
        if mode == 2 {
            assistant
                .process_frame(Frame::interruption(), Direction::Downstream)
                .unwrap();
        }
        for (batch, id, last) in [
            (&first, "a", false),
            (&first, "b", false),
            (&other, "c", true),
        ] {
            let frame = terminal(&mut worker, &ctx, batch, id, false).await;
            let replay = frame.clone();
            let runs = causes(
                assistant
                    .process_frame(frame, Direction::Downstream)
                    .unwrap(),
            );
            assert_eq!(runs.len(), usize::from(mode == 1 && last));
            if let Some(cause) = runs.first() {
                assert_eq!(cause.intent(), RealtimeContextIntent::AssistantContinuation);
                assert_eq!(cause.assistant_native_outcome_id(), None);
            }
            assert!(causes(
                assistant
                    .process_frame(replay, Direction::Downstream)
                    .unwrap()
            )
            .is_empty());
        }
        worker.end().await.unwrap();
        assert!(worker.collect().await.unwrap().1.errors.is_empty());
    }
}
#[tokio::test]
async fn false_cancellation_preserves_selected_eligible_group_reason_and_clears_it_once() {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let pair = pair();
    let assistant = pair.assistant();
    let batch =
        NativeToolCallBatch::new("group".into(), vec![(call("a"), false), (call("b"), false)])
            .unwrap();
    declare(&assistant, &batch);
    let first = terminal(&mut worker, &ctx, &batch, "a", true).await;
    let selected = first.native_tool_outcome().unwrap().unwrap().id();
    assert!(causes(
        assistant
            .process_frame(first, Direction::Downstream)
            .unwrap()
    )
    .is_empty());
    let second = terminal(&mut worker, &ctx, &batch, "b", false).await;
    let runs = causes(
        assistant
            .process_frame(second.clone(), Direction::Downstream)
            .unwrap(),
    );
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].assistant_native_outcome_id(), Some(selected));
    assert!(causes(
        assistant
            .process_frame(second, Direction::Downstream)
            .unwrap()
    )
    .is_empty());
    let new = causes(
        assistant
            .process_frame(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .unwrap(),
    );
    assert_eq!(new.len(), 1);
    assert_eq!(new[0].assistant_native_outcome_id(), None);
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}
