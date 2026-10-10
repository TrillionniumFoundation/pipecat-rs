use pipecat_core::*;
use pipecat_processors::tools::*;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
fn call(id: &str) -> FunctionCall {
    FunctionCall {
        function_name: "effect".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    }
}
fn signed(id: u64, calls: Vec<FunctionCall>, bytes: usize) -> Frame {
    let mut blocks = vec![
        LlmContinuationBlock::Thinking {
            text: "private".into(),
            signature: "opaque".into(),
        },
        LlmContinuationBlock::Text("x".repeat(bytes)),
    ];
    blocks.extend(calls.iter().cloned().map(LlmContinuationBlock::ToolCall));
    let history =
        NativeLlmContinuation::new(id, LlmContinuationProvider::Anthropic, blocks).unwrap();
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(calls));
    frame
        .set_native_llm_continuation(Arc::new(history))
        .unwrap();
    frame
}
async fn run(frames: Vec<Frame>) -> (usize, Vec<DirectedFrame>, WorkerReport) {
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut tools = ToolExecutor::new(8, 128, 4096).unwrap();
    tools
        .register(
            "effect",
            RegisteredTool::new(Arc::new(move |_| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(json!({}))
                })
            })),
        )
        .unwrap();
    let (out, report) = tokio::time::timeout(Duration::from_secs(5), async {
        let worker = Pipeline::new(vec![Arc::new(tools)])
            .start(Default::default())
            .await
            .unwrap();
        for frame in frames {
            worker
                .sender()
                .send(frame, Direction::Downstream)
                .await
                .unwrap();
        }
        worker.end().await.unwrap();
        worker.collect().await.unwrap()
    })
    .await
    .unwrap();
    (effects.load(Ordering::SeqCst), out, report)
}
#[tokio::test]
async fn duplicates_forward_completion_without_effect_and_mixed_rejection_is_atomic() {
    let first = signed(1, vec![call("a")], 0);
    let duplicate = first.duplicate();
    let mixed = signed(2, vec![call("a"), call("b")], 0);
    let corrected = signed(2, vec![call("b")], 0);
    let (mutated_calls, mut invalid) = (vec![call("changed")], signed(3, vec![call("c")], 0));
    invalid.data = FrameData::FunctionCallsStarted(mutated_calls);
    let (effects, out, report) = run(vec![first, duplicate, mixed, corrected, invalid]).await;
    assert_eq!(effects, 2);
    assert_eq!(report.errors.len(), 2);
    assert_eq!(
        out.iter()
            .filter(|f| matches!(f.frame.data, FrameData::FunctionCallsStarted(_)))
            .count(),
        3
    );
}
#[tokio::test]
async fn aggregate_carrier_budget_rejection_precedes_effects_and_identity_mutation() {
    let first = signed(10, vec![call("large-a")], 4 * 1024 * 1024);
    let second = signed(11, vec![call("large-b")], 4 * 1024 * 1024);
    let corrected = signed(11, vec![call("large-b")], 0);
    let replay = corrected.duplicate();
    let (effects, _, report) = run(vec![first, second, corrected, replay]).await;
    assert_eq!(effects, 2);
    assert_eq!(report.errors.len(), 1);
}
