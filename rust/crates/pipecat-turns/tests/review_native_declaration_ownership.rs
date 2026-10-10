//! A native active declaration survives unrelated legacy cancellation churn.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_turns::{AggregationAction, LLMAssistantAggregator, SharedContext};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, Notify};
struct Capture(mpsc::Sender<Frame>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "IndependentNativeDeclarationCapture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && (matches!(
                frame.data,
                FrameData::FunctionCallsStarted(_) | FrameData::FunctionCallInProgress(_)
            ) || frame.native_tool_outcome()?.is_some())
        {
            self.0.send(frame).await.unwrap();
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
async fn receive(rx: &mut mpsc::Receiver<Frame>) -> Frame {
    tokio::time::timeout(Duration::from_secs(3), rx.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn ownership(keep_started: bool, keep_policy: bool, consume: bool) {
    let release = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let (r, e) = (release.clone(), entered.clone());
    let mut executor = ToolExecutor::new(1, 16, 4096).unwrap();
    executor
        .register(
            "native",
            RegisteredTool::new(Arc::new(move |_| {
                let (r, e) = (r.clone(), e.clone());
                Box::pin(async move {
                    e.notify_one();
                    r.notified().await;
                    Ok(json!("native won"))
                })
            })),
        )
        .unwrap();
    let journal = executor.outcomes();
    let (tx, mut rx) = mpsc::channel(8);
    let worker = Pipeline::new(vec![Arc::new(executor), Arc::new(Capture(tx))])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "native".into(),
                tool_call_id: "native-active".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    let declarations = [receive(&mut rx).await, receive(&mut rx).await];
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let mut delayed = vec![];
    for mut frame in declarations {
        assert!(frame.native_tool_batch().unwrap().is_some());
        assert!(serde_json::to_vec(&frame).is_err());
        if consume {
            frame.take_native_tool_batch().unwrap().unwrap();
        }
        let keep = if matches!(frame.data, FrameData::FunctionCallsStarted(_)) {
            keep_started
        } else {
            keep_policy
        };
        if keep {
            assistant
                .process_frame(frame, Direction::Downstream)
                .unwrap();
        } else {
            delayed.push(frame);
        }
    }
    for id in 0..1100 {
        assistant
            .process_frame(
                Frame::new(FrameData::FunctionCallInProgress(
                    FunctionCallInProgressFrame {
                        function_name: "legacy".into(),
                        tool_call_id: format!("legacy-{id}"),
                        arguments: json!({}),
                        cancel_on_interruption: true,
                        group_id: None,
                    },
                )),
                Direction::Downstream,
            )
            .unwrap();
        assistant
            .process_frame(Frame::interruption(), Direction::Downstream)
            .unwrap();
    }
    if !consume && (keep_started || keep_policy) {
        assert!(
            assistant.has_function_calls_in_progress().unwrap(),
            "native active ownership evicted by legacy churn"
        );
    }
    release.notify_one();
    let result = receive(&mut rx).await;
    let duplicate = result.duplicate();
    let output = assistant.process_frame(result, Direction::Downstream);
    if consume {
        assert!(
            output.is_err(),
            "explicitly stripped legacy declaration must not silently regain evicted ownership"
        );
        assert!(context.snapshot_rich().unwrap().messages.iter().any(|m| m
            .tool_call_id
            .as_deref()
            == Some("native-active")
            && m.content == MessageContent::Text("CANCELLED".into())));
    } else {
        let output = output.expect("native active declaration was lost");
        assert_eq!(output.actions.iter().filter(|a|matches!(a,AggregationAction::Push(f) if f.direction==Direction::Upstream && matches!(f.frame.data,FrameData::LlmContext(_)))).count(),1);
        let snapshot = context.snapshot_rich().unwrap();
        assert_eq!(
            snapshot
                .messages
                .iter()
                .flat_map(|m| &m.tool_calls)
                .filter(|c| c.tool_call_id == "native-active")
                .count(),
            1
        );
        assert!(snapshot
            .messages
            .iter()
            .any(|m| m.tool_call_id.as_deref() == Some("native-active")
                && m.content == MessageContent::Text("\"native won\"".into())));
        assistant
            .process_frame(duplicate, Direction::Downstream)
            .unwrap();
        for frame in delayed {
            let output = assistant
                .process_frame(frame, Direction::Downstream)
                .unwrap();
            assert!(!output.actions.iter().any(|a|matches!(a,AggregationAction::Push(f) if f.direction==Direction::Upstream && matches!(f.frame.data,FrameData::LlmContext(_)))));
        }
        assert_eq!(context.snapshot_rich().unwrap(), snapshot);
        assert!(!assistant.has_function_calls_in_progress().unwrap());
    }
    assert_eq!(journal.snapshot().total_terminal, 1);
    assert!(journal.snapshot().active.is_empty());
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test]
async fn independent_started_only_ownership_survives_legacy_churn() {
    ownership(true, false, false).await;
}
#[tokio::test]
async fn independent_policy_only_ownership_survives_legacy_churn() {
    ownership(false, true, false).await;
}
#[tokio::test]
async fn independent_terminal_first_restores_after_both_declarations_delayed() {
    ownership(false, false, false).await;
}
#[tokio::test]
async fn independent_explicitly_consumed_declarations_forfeit_native_retention() {
    ownership(true, true, true).await;
}

#[tokio::test]
async fn independent_native_terminal_ignores_late_declarations_after_legacy_hint_eviction() {
    let mut executor = ToolExecutor::new(1, 16, 4096).unwrap();
    executor
        .register(
            "native",
            RegisteredTool::new(Arc::new(|_| {
                Box::pin(async { Ok(json!("sealed completed value")) })
            })),
        )
        .unwrap();
    let (tx, mut rx) = mpsc::channel(8);
    let worker = Pipeline::new(vec![Arc::new(executor), Arc::new(Capture(tx))])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "native".into(),
                tool_call_id: "native-closed".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let mut delayed = Vec::new();
    let terminal = loop {
        let frame = receive(&mut rx).await;
        if frame.native_tool_outcome().unwrap().is_some() {
            break frame;
        }
        delayed.push(frame);
    };
    while delayed.len() < 2 {
        delayed.push(receive(&mut rx).await);
    }
    let duplicate = terminal.duplicate();
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let output = assistant
        .process_frame(terminal, Direction::Downstream)
        .unwrap();
    assert_eq!(
        output
            .actions
            .iter()
            .filter(|a| matches!(a, AggregationAction::Push(f)
        if f.direction == Direction::Upstream && matches!(f.frame.data, FrameData::LlmContext(_))))
            .count(),
        1
    );
    for id in 0..600 {
        assistant
            .process_frame(
                Frame::new(FrameData::FunctionCallInProgress(
                    FunctionCallInProgressFrame {
                        function_name: "legacy".into(),
                        tool_call_id: format!("later-legacy-{id}"),
                        arguments: json!({}),
                        cancel_on_interruption: true,
                        group_id: None,
                    },
                )),
                Direction::Downstream,
            )
            .unwrap();
        assistant
            .process_frame(Frame::interruption(), Direction::Downstream)
            .unwrap();
    }
    let before = context.snapshot_rich().unwrap();
    for frame in delayed.into_iter().chain(std::iter::once(duplicate)) {
        let output = assistant
            .process_frame(frame, Direction::Downstream)
            .expect("retained native terminal marker must outlive legacy completed hints");
        assert!(!output.actions.iter().any(|a| matches!(a, AggregationAction::Push(f)
            if f.direction == Direction::Upstream && matches!(f.frame.data, FrameData::LlmContext(_)))));
        assert_eq!(
            context.snapshot_rich().unwrap(),
            before,
            "late declaration changed committed history"
        );
        assert!(
            !assistant.has_function_calls_in_progress().unwrap(),
            "late declaration resurrected a completed native call"
        );
    }
    let output = assistant
        .process_frame(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .unwrap();
    assert_eq!(
        output
            .actions
            .iter()
            .filter(|a| matches!(a, AggregationAction::Push(f)
        if f.direction == Direction::Upstream && matches!(f.frame.data, FrameData::LlmContext(_))))
            .count(),
        1,
        "late Started left a phantom LLM-active inference blocker"
    );
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
