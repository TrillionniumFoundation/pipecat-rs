//! Publication identity and default-consumer resource alignment.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

#[tokio::test]
async fn largest_portable_single_batch_reservation_settles_in_default_assistant() {
    let mut function = FunctionCall {
        function_name: "bounded".into(),
        tool_call_id: "boundary".into(),
        arguments: json!({"payload":""}),
    };
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![]));
    let group = format!("tool-batch-{}", u64::MAX);
    let overhead = NativeToolCallBatch::measure(&group, &[(&function, true)]).unwrap()
        + NativeToolCallBatch::policy_bytes(&function, true, Some(&group)).unwrap()
        + 2 * function.tool_call_id.len();
    let bytes = (ToolExecutor::MAX_OUTCOME_PAYLOAD_BYTES - overhead) / 2;
    function.arguments = json!({"payload":"x".repeat(bytes)});
    let measured = NativeToolCallBatch::measure(&group, &[(&function, true)]).unwrap()
        + NativeToolCallBatch::policy_bytes(&function, true, Some(&group)).unwrap()
        + 2 * function.tool_call_id.len();
    assert!(measured <= ToolExecutor::MAX_OUTCOME_PAYLOAD_BYTES);
    assert!(measured + 2 > ToolExecutor::MAX_OUTCOME_PAYLOAD_BYTES);
    frame.data = FrameData::FunctionCallsStarted(vec![function]);
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut executor = ToolExecutor::new(1, 16, 8 * 1024 * 1024).unwrap();
    executor
        .register(
            "bounded",
            RegisteredTool::new(Arc::new(move |_| {
                let count = count.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(json!("done"))
                })
            })),
        )
        .unwrap();
    let context = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        context.clone(),
        Default::default(),
    ));
    let worker = Pipeline::new(vec![Arc::new(executor), assistant.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if context
                .snapshot_rich()
                .unwrap()
                .messages
                .iter()
                .any(|message| {
                    message.tool_call_id.as_deref() == Some("boundary")
                        && message.content == MessageContent::Text("\"done\"".into())
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

#[derive(Default)]
struct Capture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "PublicationIdentityCapture"
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
#[tokio::test]
async fn concurrent_duplicate_publications_commit_once_and_emit_no_native_frame() {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    ctx.tool_outcome_publisher(
        FunctionCall {
            function_name: "one".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
        },
        true,
    )
    .unwrap()
    .complete(
        Ok(json!(42)),
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::CallbackReturned,
    );
    let frame = loop {
        let frame = worker.recv().await.unwrap().frame;
        if frame.native_tool_outcome().unwrap().is_some() {
            break frame;
        }
    };
    let original_id = frame.native_tool_outcome().unwrap().unwrap().id();
    let context = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        context.clone(),
        Default::default(),
    ));
    let barrier = Arc::new(std::sync::Barrier::new(16));
    let mut threads = vec![];
    for _ in 0..16 {
        let assistant = assistant.clone();
        let barrier = barrier.clone();
        let mut duplicate = frame.duplicate();
        duplicate.id = u64::MAX;
        assert_eq!(
            duplicate.native_tool_outcome().unwrap().unwrap().id(),
            original_id
        );
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            let output = assistant
                .process_frame(duplicate, Direction::Downstream)
                .unwrap();
            output
                .actions
                .iter()
                .map(|action| {
                    if let pipecat_turns::AggregationAction::Push(frame) = action {
                        assert!(frame.frame.native_tool_outcome().unwrap().is_none());
                        serde_json::to_vec(&frame.frame).unwrap();
                        usize::from(
                            frame.direction == Direction::Upstream
                                && matches!(frame.frame.data, FrameData::LlmContext(_)),
                        )
                    } else {
                        0
                    }
                })
                .sum::<usize>()
        }));
    }
    assert_eq!(
        threads
            .into_iter()
            .map(|thread| thread.join().unwrap())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        context
            .snapshot_rich()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.role == ChatRole::Tool)
            .count(),
        1
    );
    drop(ctx);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}

#[tokio::test]
async fn native_settlement_waits_for_real_spoken_receipt_and_context_contains_both() {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    ctx.tool_outcome_publisher(
        FunctionCall {
            function_name: "receipt".into(),
            tool_call_id: "spoken-tool".into(),
            arguments: json!({}),
        },
        true,
    )
    .unwrap()
    .complete(
        Ok(json!(42)),
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::CallbackReturned,
    );
    let frame = loop {
        let frame = worker.recv().await.unwrap().frame;
        if frame.native_tool_outcome().unwrap().is_some() {
            break frame;
        }
    };
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        pipecat_turns::AssistantAggregatorConfig {
            text_mode: pipecat_turns::AssistantTextMode::Spoken,
        },
    );
    assistant.begin_playback("played").unwrap();
    let count = |output: &pipecat_turns::AggregationOutput| {
        output.actions.iter().filter(|action| matches!(action, pipecat_turns::AggregationAction::Push(frame) if frame.direction == Direction::Upstream && matches!(frame.frame.data, FrameData::LlmContext(_)))).count()
    };
    assert_eq!(
        count(
            &assistant
                .process_frame(frame, Direction::Downstream)
                .unwrap()
        ),
        0
    );
    let delivered = assistant
        .confirm_playback(
            "played",
            &[AggregatedTextFrame {
                text: TextFrame::new("heard"),
                aggregated_by: "receipt".into(),
                context_id: Some("played".into()),
                raw_text: None,
                will_be_spoken: true,
            }],
        )
        .unwrap();
    assert_eq!(count(&delivered), 1);
    let messages = context.snapshot_rich().unwrap().messages;
    assert!(messages
        .iter()
        .any(|message| message.role == ChatRole::Assistant
            && message.content == MessageContent::Text("heard".into())));
    assert!(messages.iter().any(
        |message| message.tool_call_id.as_deref() == Some("spoken-tool")
            && message.content == MessageContent::Text("42".into())
    ));
    assert_eq!(
        count(&assistant.confirm_playback("played", &[]).unwrap()),
        0
    );
    drop(ctx);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}

#[tokio::test]
async fn upstream_raw_cancel_reaches_executor_before_history_settlement() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let notify = entered.clone();
    let mut executor = ToolExecutor::new(1, 16, 4096).unwrap();
    executor
        .register(
            "wait",
            RegisteredTool::new(Arc::new(move |_| {
                let notify = notify.clone();
                Box::pin(async move {
                    notify.notify_one();
                    std::future::pending().await
                })
            })),
        )
        .unwrap();
    let journal = executor.outcomes();
    let context = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        context.clone(),
        Default::default(),
    ));
    let worker = Pipeline::new(vec![Arc::new(executor), assistant.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "wait".into(),
                tool_call_id: "cancel-upstream".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "wait".into(),
                tool_call_id: "cancel-upstream".into(),
                run_llm: true,
            })),
            Direction::Upstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if context.snapshot_rich().unwrap().messages.iter().any(|m| {
                m.tool_call_id.as_deref() == Some("cancel-upstream")
                    && m.content == MessageContent::Text("CANCELLED".into())
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let snapshot = journal.snapshot();
    assert_eq!(snapshot.total_terminal, 1);
    assert_eq!(
        snapshot.terminal[0].outcome.state(),
        ToolCallTerminalState::Cancelled
    );
    assert_eq!(
        snapshot.terminal[0].outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
