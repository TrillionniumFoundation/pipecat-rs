//! Independent native terminal reconstruction with declarations withheld.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::Context;
use pipecat_turns::*;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, Notify};

const WAIT: Duration = Duration::from_secs(3);
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("native history probe stalled")
}
fn call(id: &str) -> FunctionCall {
    FunctionCall {
        function_name: id.into(),
        tool_call_id: id.into(),
        arguments: json!({"identity":id}),
    }
}
fn run_count(output: &AggregationOutput) -> usize {
    output.actions.iter().filter(|action|matches!(action,AggregationAction::Push(frame) if frame.direction==Direction::Upstream && matches!(frame.frame.data,FrameData::LlmContext(_)))).count()
}
fn apply(assistant: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    assistant
        .process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}
struct Capture {
    terminals: mpsc::Sender<Frame>,
    declarations: Mutex<Vec<Frame>>,
}
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "IndependentWithholdToolDeclarations"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if matches!(
                frame.data,
                FrameData::FunctionCallsStarted(_) | FrameData::FunctionCallInProgress(_)
            ) {
                self.declarations.lock().unwrap().push(frame);
                return Ok(());
            }
            if frame.native_tool_outcome()?.is_some() {
                self.terminals.send(frame).await.unwrap();
                return Ok(());
            }
        }
        ctx.push(frame, direction).await
    }
}
async fn mixed_batch(first_cancel: bool, interruption: bool, spoken: bool) {
    let first_release = Arc::new(Notify::new());
    let second_release = Arc::new(Notify::new());
    let first_entered = Arc::new(Notify::new());
    let second_entered = Arc::new(Notify::new());
    let mut executor = ToolExecutor::new(2, 16, 4096).unwrap();
    for (id, cancel_on_interruption, release, entered) in [
        (
            "synchronous",
            true,
            first_release.clone(),
            first_entered.clone(),
        ),
        (
            "asynchronous",
            false,
            second_release.clone(),
            second_entered.clone(),
        ),
    ] {
        let mut tool = RegisteredTool::new(Arc::new(move |call| {
            let release = release.clone();
            let entered = entered.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(json!({"settled":call.tool_call_id}))
            })
        }));
        tool.cancel_on_interruption = cancel_on_interruption;
        executor.register(id, tool).unwrap();
    }
    let journal = executor.outcomes();
    let (tx, mut rx) = mpsc::channel(8);
    let capture = Arc::new(Capture {
        terminals: tx,
        declarations: Mutex::new(vec![]),
    });
    let worker = Pipeline::new(vec![Arc::new(executor), capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![
                call("synchronous"),
                call("asynchronous"),
            ])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    bounded(first_entered.notified()).await;
    bounded(second_entered.notified()).await;
    if first_cancel {
        worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: "synchronous".into(),
                    tool_call_id: "synchronous".into(),
                    run_llm: true,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
    } else {
        first_release.notify_one();
    }
    let first = bounded(rx.recv()).await.unwrap();
    assert_eq!(
        first
            .native_tool_outcome()
            .unwrap()
            .unwrap()
            .call()
            .tool_call_id,
        "synchronous"
    );
    assert_eq!(
        first.native_tool_outcome().unwrap().unwrap().effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    let context = SharedContext::new(Context::default());
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: if spoken {
                AssistantTextMode::Spoken
            } else {
                AssistantTextMode::Text
            },
        },
    );
    // No declaration frame reaches the assistant before the terminal.
    let first_copy = first.duplicate();
    let output = assistant
        .process_frame(first, Direction::Downstream)
        .unwrap();
    assert_eq!(
        run_count(&output),
        0,
        "first terminal must reconstruct surviving siblings before deciding inference"
    );
    assert!(
        assistant.has_function_calls_in_progress().unwrap(),
        "missing declarations must not hide live async sibling"
    );
    let snapshot = context.snapshot_rich().unwrap();
    assert_eq!(
        snapshot
            .messages
            .iter()
            .flat_map(|m| &m.tool_calls)
            .filter(|c| c.tool_call_id == "synchronous")
            .count(),
        1,
        "native settlement reconstructs one call declaration"
    );
    assert!(
        snapshot
            .messages
            .iter()
            .any(|m| m.tool_call_id.as_deref() == Some("synchronous")
                && m.content
                    == MessageContent::Text(if first_cancel {
                        "CANCELLED".into()
                    } else {
                        json!({"settled":"synchronous"}).to_string()
                    })),
        "first terminal must be reflected truthfully in history"
    );
    if interruption {
        apply(&assistant, FrameData::Interruption);
    }
    second_release.notify_one();
    let second = bounded(rx.recv()).await.unwrap();
    let second_copy = second.duplicate();
    let output = assistant
        .process_frame(second, Direction::Downstream)
        .unwrap();
    assert_eq!(
        run_count(&output),
        1,
        "last sibling must release one inference instead of stalling forever"
    );
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    let settled = context.snapshot_rich().unwrap();
    assert_eq!(
        run_count(
            &assistant
                .process_frame(first_copy, Direction::Downstream)
                .unwrap()
        ),
        0
    );
    assert_eq!(
        run_count(
            &assistant
                .process_frame(second_copy, Direction::Downstream)
                .unwrap()
        ),
        0
    );
    for frame in std::mem::take(&mut *capture.declarations.lock().unwrap()) {
        assert_eq!(
            run_count(
                &assistant
                    .process_frame(frame, Direction::Downstream)
                    .unwrap()
            ),
            0,
            "late declaration cannot revive inference"
        );
    }
    assert_eq!(
        context.snapshot_rich().unwrap(),
        settled,
        "duplicate terminals and delayed declarations cannot rewrite native history"
    );
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    assert_eq!(journal.snapshot().total_terminal, 2);
    assert!(journal.snapshot().active.is_empty());
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test]
async fn independent_lost_declarations_success_waits_for_async_sibling() {
    mixed_batch(false, false, false).await;
}
#[tokio::test]
async fn independent_lost_declarations_cancellation_waits_for_async_sibling() {
    mixed_batch(true, false, false).await;
}
#[tokio::test]
async fn independent_lost_declarations_async_sibling_survives_interruption() {
    mixed_batch(false, true, false).await;
}
#[tokio::test]
async fn independent_spoken_lost_declarations_preserve_native_terminals() {
    mixed_batch(false, false, true).await;
}
