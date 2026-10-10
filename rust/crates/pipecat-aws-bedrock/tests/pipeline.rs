mod support;
use pipecat_aws_bedrock::*;
use pipecat_core::*;
use pipecat_services::{
    Content, ContentPart, Context, InferenceOptions, LlmInference, Message, Role,
};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{io::AsyncReadExt, net::TcpListener};
async fn send(w: &PipelineWorker, data: FrameData) {
    w.sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn drain(w: &mut PipelineWorker) -> Vec<DirectedFrame> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut frames = vec![];
        while let Some(f) = w.recv().await {
            frames.push(f);
        }
        let r = w.join().await.unwrap();
        assert!(r.errors.is_empty(), "{:?}", r.errors);
        frames
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn real_pipeline_text_native_history_metrics_and_end_drain() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p = Arc::new(BedrockProcessor::new(
        BedrockClient::new(support::config(&l)).unwrap(),
        support::request(),
    ));
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        support::read_request(&mut s).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        support::respond(
            &mut s,
            "application/vnd.amazon.eventstream",
            &support::text_response("hello"),
            3,
        )
        .await;
    });
    let mut config = PipelineConfig::default();
    config.start.enable_metrics = true;
    config.start.enable_usage_metrics = true;
    let mut w = Pipeline::new(vec![p]).start(config).await.unwrap();
    send(&w, FrameData::LlmRun).await;
    w.end().await.unwrap();
    let frames = drain(&mut w).await;
    let order: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::LlmResponseStart => Some("start"),
            FrameData::LlmText(t) => {
                assert_eq!(t.text, "hello");
                Some("text")
            }
            FrameData::LlmResponseEnd => {
                assert!(f.frame.native_llm_continuation().unwrap().is_some());
                Some("response-end")
            }
            FrameData::End(_) => Some("end"),
            _ => None,
        })
        .collect();
    assert_eq!(order, ["start", "text", "response-end", "end"]);
    assert!(frames.iter().any(|f|matches!(&f.frame.data,FrameData::Metrics(m) if m.iter().any(|m|matches!(m,Metric::LlmUsage{total_tokens:10,..})))));
    server.await.unwrap();
}
#[tokio::test]
async fn inference_trait_calls_actual_converse_and_never_changes_snapshot() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p = BedrockProcessor::new(
        BedrockClient::new(support::config(&l)).unwrap(),
        support::request(),
    );
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let r = support::read_request(&mut s).await;
        support::respond(
            &mut s,
            "application/json",
            br#"{"output":{"message":{"content":[{"text":"answer"}]}}}"#,
            10,
        )
        .await;
        r
    });
    let context = LlmContextData {
        messages: vec![ChatMessage::new(ChatRole::User, "outside").into()],
        tools: ToolSet::NotGiven,
        tool_choice: None,
    };
    let mut options = InferenceOptions::default();
    options.settings.max_tokens = Setting::Value(10);
    assert_eq!(
        p.run_inference(context, options).await.unwrap(),
        Some("answer".into())
    );
    let r = server.await.unwrap();
    assert!(r.route.ends_with("/converse HTTP/1.1"));
    assert_eq!(r.json()["inferenceConfig"]["maxTokens"], 10);
    assert_eq!(r.json()["messages"][0]["content"][0]["text"], "outside");
    assert_eq!(
        p.request_snapshot().unwrap().context.messages[0].content,
        Some(Content::Text("Hello".into()))
    );
}
#[test]
fn settings_commit_atomically_and_target_name_is_canonical() {
    let p = BedrockProcessor::new(
        BedrockClient::new(BedrockConfig::new(support::credentials(), "us-east-1")).unwrap(),
        support::request(),
    );
    assert_eq!(p.name(), "AWSBedrockLLMService");
    let mut d = LlmSettings {
        temperature: Setting::Value(9.0),
        ..Default::default()
    };
    d.base.model = Setting::Value("changed".into());
    assert!(p.update_settings(&d).is_err());
    assert_eq!(
        p.request_snapshot().unwrap().model,
        "us.amazon.nova-lite-v1:0"
    );
    assert!(matches!(
        p.request_snapshot().unwrap().settings.base.model,
        Setting::NotGiven
    ));
    d.temperature = Setting::Null;
    p.update_settings(&d).unwrap();
    assert_eq!(
        p.request_snapshot().unwrap().settings.base.model,
        Setting::Value("changed".into())
    );
}
#[tokio::test]
async fn interruption_aborts_stalled_request_and_a_new_request_can_complete() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let p = Arc::new(BedrockProcessor::new(
        BedrockClient::new(support::config(&l)).unwrap(),
        support::request(),
    ));
    let (tx, rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = l.accept().await.unwrap();
        support::read_request(&mut first).await;
        tx.send(()).unwrap();
        let mut b = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), first.read(&mut b))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        let (mut second, _) = l.accept().await.unwrap();
        support::read_request(&mut second).await;
        support::respond(
            &mut second,
            "application/vnd.amazon.eventstream",
            &support::text_response("fresh"),
            100,
        )
        .await;
    });
    let mut w = Pipeline::new(vec![p])
        .start(Default::default())
        .await
        .unwrap();
    send(&w, FrameData::LlmRun).await;
    rx.await.unwrap();
    send(&w, FrameData::Interruption).await;
    send(&w, FrameData::LlmRun).await;
    w.end().await.unwrap();
    let frames = drain(&mut w).await;
    assert_eq!(
        frames
            .iter()
            .filter_map(|f| match &f.frame.data {
                FrameData::LlmText(t) => Some(t.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>(),
        ["fresh"]
    );
    server.await.unwrap();
}
fn attached(data: FrameData, history: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(Arc::new(history))
        .unwrap();
    frame
}
#[test]
fn assistant_aggregator_retains_bedrock_signatures_in_both_context_forms() {
    let shared = SharedContext::new(Context::new(vec![Message::text(Role::User, "q")]));
    let a = LLMAssistantAggregator::new(shared.clone(), Default::default());
    let provider = LlmContinuationProvider::AwsBedrock;
    a.process_frame(
        attached(
            FrameData::LlmResponseStart,
            NativeLlmContinuation::start(717, provider).unwrap(),
        ),
        Direction::Downstream,
    )
    .unwrap();
    a.process_frame(
        Frame::new(FrameData::LlmText(TextFrame::llm("answer"))),
        Direction::Downstream,
    )
    .unwrap();
    let blocks = vec![
        LlmContinuationBlock::BedrockReasoning {
            text: "private".into(),
            signature: Some("exact+/==".into()),
        },
        LlmContinuationBlock::BedrockRedacted(vec![0, 1, 255]),
        LlmContinuationBlock::Text("answer".into()),
    ];
    a.process_frame(
        attached(
            FrameData::LlmResponseEnd,
            NativeLlmContinuation::new(717, provider, blocks).unwrap(),
        ),
        Direction::Downstream,
    )
    .unwrap();
    let snapshot = shared.snapshot().unwrap();
    let rich = pipecat_services::frame_context::context(&shared.snapshot_rich().unwrap()).unwrap();
    assert_eq!(snapshot, rich);
    let req = CompletionRequest::new("fixture", snapshot);
    let body = adapter::prepare(&req, true).unwrap().body;
    assert_eq!(
        body["messages"][1]["content"],
        json!([{"reasoningContent":{"reasoningText":{"text":"private","signature":"exact+/=="}}},{"reasoningContent":{"redactedContent":"AAH/"}},{"text":"answer"}])
    );
    assert!(
        matches!(rich.messages[1].content,Some(Content::Parts(ref p)) if matches!(p[0],ContentPart::BedrockReasoning{..}))
    );
}
#[test]
fn native_carriers_reject_wrong_provider_oversize_and_wire_serialization() {
    let block = LlmContinuationBlock::BedrockReasoning {
        text: "private".into(),
        signature: Some("SECRET".into()),
    };
    assert!(
        NativeLlmContinuation::new(1, LlmContinuationProvider::Anthropic, vec![block.clone()])
            .is_err()
    );
    let history =
        NativeLlmContinuation::new(1, LlmContinuationProvider::AwsBedrock, vec![block]).unwrap();
    assert!(!format!("{history:?}").contains("SECRET"));
    let frame = attached(FrameData::LlmResponseEnd, history);
    assert!(serde_json::to_value(frame).is_err());
    assert!(NativeLlmContinuation::new(
        2,
        LlmContinuationProvider::AwsBedrock,
        vec![LlmContinuationBlock::BedrockReasoning {
            text: String::new(),
            signature: Some("x".repeat(NativeLlmContinuation::MAX_SIGNATURE_BYTES + 1))
        }]
    )
    .is_err());
}
#[test]
fn shared_ids_preserve_history_across_provider_switches_in_one_aggregator() {
    fn contains_bedrock(first: u64, second: u64) -> bool {
        let context = SharedContext::new(Context::new(vec![Message::text(Role::User, "q")]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        for (id, provider, blocks) in [
            (
                first,
                LlmContinuationProvider::Anthropic,
                vec![LlmContinuationBlock::Thinking {
                    text: "one".into(),
                    signature: "one-signature".into(),
                }],
            ),
            (
                second,
                LlmContinuationProvider::AwsBedrock,
                vec![LlmContinuationBlock::BedrockReasoning {
                    text: "two".into(),
                    signature: Some("two-signature".into()),
                }],
            ),
        ] {
            assistant
                .process_frame(
                    attached(
                        FrameData::LlmResponseStart,
                        NativeLlmContinuation::start(id, provider).unwrap(),
                    ),
                    Direction::Downstream,
                )
                .unwrap();
            assistant
                .process_frame(
                    attached(
                        FrameData::LlmResponseEnd,
                        NativeLlmContinuation::new(id, provider, blocks).unwrap(),
                    ),
                    Direction::Downstream,
                )
                .unwrap();
        }
        context.snapshot().unwrap().messages.iter().any(|m|matches!(&m.content,Some(Content::Parts(parts)) if parts.iter().any(|p|matches!(p,ContentPart::BedrockReasoning{..}))))
    }
    // Separate provider-local counters previously both started at one.
    assert!(!contains_bedrock(90001, 90001));
    let first = NativeLlmContinuation::next_response_id().unwrap();
    let second = NativeLlmContinuation::next_response_id().unwrap();
    assert_ne!(first, second);
    assert!(contains_bedrock(first, second));
}
#[tokio::test]
async fn signed_tool_cycle_executes_once_and_replays_native_blocks_on_the_next_wire_request() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = support::config(&listener);
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        support::read_request(&mut first).await;
        let records=[
            support::record("messageStart",json!({"role":"assistant"})),
            support::record("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"text":"private plan"}}})),
            support::record("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"signature":"exact-signed-plan"}}})),
            support::record("contentBlockStop",json!({"contentBlockIndex":0})),
            support::record("contentBlockStart",json!({"contentBlockIndex":1,"start":{"toolUse":{"toolUseId":"bedrock-call","name":"echo"}}})),
            support::record("contentBlockDelta",json!({"contentBlockIndex":1,"delta":{"toolUse":{"input":"{\"value\":7}"}}})),
            support::record("contentBlockStop",json!({"contentBlockIndex":1})),
            support::record("messageStop",json!({"stopReason":"tool_use"})),
        ].concat();
        support::respond(
            &mut first,
            "application/vnd.amazon.eventstream",
            &records,
            7,
        )
        .await;
        let (mut second, _) = listener.accept().await.unwrap();
        let request = support::read_request(&mut second).await;
        support::respond(
            &mut second,
            "application/vnd.amazon.eventstream",
            &support::text_response("done"),
            11,
        )
        .await;
        request
    });
    let mut request = support::request();
    request.context.tools = Some(vec![pipecat_services::ToolDefinition {
        name: "echo".into(),
        description: "echo".into(),
        parameters: json!({"type":"object","properties":{"value":{"type":"integer"}}}),
    }]);
    let shared = SharedContext::new(request.context.clone());
    let count = Arc::new(AtomicUsize::new(0));
    let mut executor = ToolExecutor::new(2, 32, 4096).unwrap();
    let effect = count.clone();
    executor
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |call| {
                let effect = effect.clone();
                Box::pin(async move {
                    effect.fetch_add(1, Ordering::SeqCst);
                    Ok(call.arguments)
                })
            })),
        )
        .unwrap();
    let mut worker = Pipeline::new(vec![
        Arc::new(BedrockProcessor::new(
            BedrockClient::new(config).unwrap(),
            request,
        )),
        Arc::new(executor),
        Arc::new(LLMAssistantAggregator::new(shared, Default::default())),
    ])
    .start(Default::default())
    .await
    .unwrap();
    send(&worker, FrameData::LlmRun).await;
    let second = tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(
        second.json()["messages"][1]["content"],
        json!([
            {"reasoningContent":{"reasoningText":{"text":"private plan","signature":"exact-signed-plan"}}},
            {"toolUse":{"toolUseId":"bedrock-call","name":"echo","input":{"value":7}}}
        ])
    );
    assert_eq!(
        second.json()["messages"][2]["content"],
        json!([{"toolResult":{"toolUseId":"bedrock-call","content":[{"json":{"value":7}}]}}])
    );
    worker.end().await.unwrap();
    drain(&mut worker).await;
}
#[tokio::test]
async fn filtered_tool_blocks_never_reach_actual_registered_handlers() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for stop in [
        "guardrail_intervened",
        "content_filtered",
        "malformed_tool_use",
        "max_tokens",
        "unknown",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BedrockClient::new(support::config(&listener)).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            let records=[support::record("messageStart",json!({"role":"assistant"})),support::record("contentBlockStart",json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"unsafe-call","name":"effect"}}})),support::record("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"toolUse":{"input":"{}"}}})),support::record("contentBlockStop",json!({"contentBlockIndex":0})),support::record("messageStop",json!({"stopReason":stop}))].concat();
            support::respond(
                &mut socket,
                "application/vnd.amazon.eventstream",
                &records,
                13,
            )
            .await;
        });
        let count = Arc::new(AtomicUsize::new(0));
        let effect = count.clone();
        let mut executor = ToolExecutor::new(1, 8, 4096).unwrap();
        executor
            .register(
                "effect",
                RegisteredTool::new(Arc::new(move |_| {
                    let effect = effect.clone();
                    Box::pin(async move {
                        effect.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({"performed":true}))
                    })
                })),
            )
            .unwrap();
        let mut request = support::request();
        request.context.tools = Some(vec![pipecat_services::ToolDefinition {
            name: "effect".into(),
            description: "effect".into(),
            parameters: json!({"type":"object"}),
        }]);
        let mut worker = Pipeline::new(vec![
            Arc::new(BedrockProcessor::new(client, request)),
            Arc::new(executor),
        ])
        .start(Default::default())
        .await
        .unwrap();
        send(&worker, FrameData::LlmRun).await;
        worker.end().await.unwrap();
        let mut failed = false;
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(output) = worker.recv().await {
                assert!(!matches!(
                    output.frame.data,
                    FrameData::FunctionCallsStarted(_)
                ));
                failed |= matches!(&output.frame.data, FrameData::LlmResponseEnd)
                    && output.frame.metadata.get("failed") == Some(&json!(true));
            }
            worker.join().await.unwrap();
        })
        .await
        .unwrap();
        assert!(failed, "missing failed response boundary for {stop}");
        assert_eq!(count.load(Ordering::SeqCst), 0, "side effect after {stop}");
        server.await.unwrap();
    }
}
#[tokio::test]
async fn broader_executor_registry_cannot_bypass_immutable_request_declarations_or_forced_choice() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use std::sync::atomic::{AtomicUsize, Ordering};
    for forced in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = BedrockClient::new(support::config(&listener)).unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            support::read_request(&mut socket).await;
            let records=[support::record("messageStart",json!({"role":"assistant"})),support::record("contentBlockStart",json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"hidden-call","name":"hidden_effect"}}})),support::record("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"toolUse":{"input":"{}"}}})),support::record("contentBlockStop",json!({"contentBlockIndex":0})),support::record("messageStop",json!({"stopReason":"tool_use"}))].concat();
            support::respond(
                &mut socket,
                "application/vnd.amazon.eventstream",
                &records,
                17,
            )
            .await;
        });
        let count = Arc::new(AtomicUsize::new(0));
        let effect = count.clone();
        let mut executor = ToolExecutor::new(1, 8, 4096).unwrap();
        executor
            .register(
                "hidden_effect",
                RegisteredTool::new(Arc::new(move |_| {
                    let effect = effect.clone();
                    Box::pin(async move {
                        effect.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({}))
                    })
                })),
            )
            .unwrap();
        let mut request = support::request();
        request.context.tools = Some(vec![pipecat_services::ToolDefinition {
            name: "visible".into(),
            description: "visible".into(),
            parameters: json!({"type":"object"}),
        }]);
        if forced {
            request
                .context
                .tools
                .as_mut()
                .unwrap()
                .push(pipecat_services::ToolDefinition {
                    name: "hidden_effect".into(),
                    description: "not forced".into(),
                    parameters: json!({"type":"object"}),
                });
            request.context.tool_choice =
                Some(pipecat_services::ToolChoice::Function("visible".into()));
        }
        let mut worker = Pipeline::new(vec![
            Arc::new(BedrockProcessor::new(client, request)),
            Arc::new(executor),
        ])
        .start(Default::default())
        .await
        .unwrap();
        send(&worker, FrameData::LlmRun).await;
        worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(output) = worker.recv().await {
                assert!(!matches!(
                    output.frame.data,
                    FrameData::FunctionCallsStarted(_)
                ));
            }
            worker.join().await.unwrap();
        })
        .await
        .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 0);
        server.await.unwrap();
    }
}
