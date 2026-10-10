//! Reconstructed real TCP/SSE native continuation regressions; no live providers.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
async fn request(socket: &mut TcpStream) -> Value {
    let mut data = vec![];
    let mut buf = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&buf[..n]);
        if let Some(p) = data.windows(4).position(|v| v == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let length: usize = std::str::from_utf8(&data[..end])
        .unwrap()
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse().unwrap())
        })
        .unwrap();
    while data.len() < end + length {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&buf[..n]);
    }
    serde_json::from_slice(&data[end..end + length]).unwrap()
}
async fn respond(socket: &mut TcpStream, events: &[Value]) {
    let body: String = events.iter().map(|v| format!("data: {v}\n\n")).collect();
    socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
    for bytes in body.as_bytes().chunks(11) {
        socket.write_all(bytes).await.unwrap();
        tokio::task::yield_now().await;
    }
}
fn llm(endpoint: String, context: Context) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::Anthropic, "synthetic-local-fixture-key");
    config.endpoint = endpoint;
    config.request_timeout = Duration::from_secs(5);
    Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", context),
    ))
}
fn executor(effects: Arc<AtomicUsize>) -> Arc<ToolExecutor> {
    let mut tools = ToolExecutor::new(3, 128, 4096).unwrap();
    tools
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |call| {
                let effects = effects.clone();
                Box::pin(async move {
                    effects.fetch_add(1, Ordering::SeqCst);
                    Ok(call.arguments)
                })
            })),
        )
        .unwrap();
    Arc::new(tools)
}
#[tokio::test]
async fn signed_blocks_survive_native_tool_results_into_second_request() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../pipecat-services/tests/fixtures/anthropic_signed_history.json"
    ))
    .unwrap();
    let events = fixture["events"].as_array().unwrap().clone();
    let expected = fixture["expected_content"].clone();
    let expected_results: Vec<_> = expected.as_array().unwrap().iter()
        .filter(|block| block["type"] == "tool_use")
        .map(|block| json!({"type":"tool_result","tool_use_id":block["id"],"content":[{"type":"text","text":block["input"].to_string()}]}))
        .collect();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        request(&mut first).await;
        respond(&mut first, &events).await;
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        let captured = request(&mut second).await;
        respond(
            &mut second,
            &[
                json!({"type":"message_start","message":{}}),
                json!({"type":"message_stop"}),
            ],
        )
        .await;
        captured
    });
    let context = Context::new(vec![Message::text(Role::User, "question")]);
    let shared = SharedContext::new(context.clone());
    let effects = Arc::new(AtomicUsize::new(0));
    let assistant = Arc::new(LLMAssistantAggregator::new(shared, Default::default()));
    let mut worker = Pipeline::new(vec![
        llm(endpoint, context),
        executor(effects.clone()),
        assistant,
    ])
    .start(Default::default())
    .await
    .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    let body = tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(body["messages"][1]["content"], expected);
    assert_eq!(effects.load(Ordering::SeqCst), expected_results.len());
    assert_eq!(body["messages"].as_array().unwrap().len(), 3);
    assert_eq!(
        body["messages"][2],
        json!({"role":"user","content":expected_results})
    );
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
struct ThoughtProgress {
    count: AtomicUsize,
    expected: usize,
    ready: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl PipelineObserver for ThoughtProgress {
    async fn on_event(&self, event: ObservationEvent) {
        if matches!(&*event.observation,PipelineObservation::Process{processor,frame,..}if processor.name.as_ref()=="LLMAssistantAggregator"&&frame.kind=="LLMThoughtTextFrame")
            && self.count.fetch_add(1, Ordering::SeqCst) + 1 == self.expected
        {
            self.ready.add_permits(1);
        }
    }
}
#[tokio::test]
async fn cancelling_first_partial_signature_preserves_no_invented_history() {
    cancellation(false).await;
}
#[tokio::test]
async fn cancelling_later_signature_retains_closed_signature_and_heard_prefix() {
    cancellation(true).await;
}
async fn cancellation(closed: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let (socket_closed_tx, socket_closed_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        request(&mut first).await;
        first.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n").await.unwrap();
        let mut events = vec![json!({"type":"message_start","message":{}})];
        if closed {
            events.extend([
 json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"closed"}}),json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"complete-first"}}),json!({"type":"content_block_stop","index":0}),
 json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"heard prefix"}}),json!({"type":"content_block_stop","index":1}),
 json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"never-run","name":"echo","input":{}}}),json!({"type":"content_block_stop","index":2})]);
        }
        let i = if closed { 3 } else { 0 };
        events.extend([json!({"type":"content_block_start","index":i,"content_block":{"type":"thinking","thinking":"partial"}}),json!({"type":"content_block_delta","index":i,"delta":{"type":"signature_delta","signature":"unfinished"}})]);
        let body: String = events.iter().map(|v| format!("data: {v}\n\n")).collect();
        first
            .write_all(format!("{:x}\r\n{}\r\n", body.len(), body).as_bytes())
            .await
            .unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), first.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        socket_closed_tx.send(()).unwrap();
        let (mut second, _) = listener.accept().await.unwrap();
        let captured = request(&mut second).await;
        respond(
            &mut second,
            &[
                json!({"type":"message_start","message":{}}),
                json!({"type":"message_stop"}),
            ],
        )
        .await;
        captured
    });
    let context = Context::new(vec![Message::text(Role::User, "question")]);
    let shared = SharedContext::new(context.clone());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let ready = Arc::new(tokio::sync::Semaphore::new(0));
    let effects = Arc::new(AtomicUsize::new(0));
    let mut worker = Pipeline::new(vec![
        llm(endpoint, context),
        executor(effects.clone()),
        assistant,
    ])
    .with_observer(
        Arc::new(ThoughtProgress {
            count: AtomicUsize::new(0),
            expected: if closed { 2 } else { 1 },
            ready: ready.clone(),
        }),
        ObserverOptions::default(),
    )
    .start(Default::default())
    .await
    .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), ready.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), socket_closed_rx)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(
                worker.recv().await.unwrap().frame.data,
                FrameData::Interruption
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(shared.frame_data().unwrap()),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let body = tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(!body.to_string().contains("unfinished"));
    assert!(!body.to_string().contains("never-run"));
    if closed {
        assert_eq!(
            body["messages"][1]["content"],
            json!([{"type":"thinking","thinking":"closed","signature":"complete-first"},{"type":"text","text":"heard prefix"}])
        );
    } else {
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
    }
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

#[tokio::test]
async fn rejected_overtaking_completion_cancels_and_next_http_request_is_clean() {
    let context = Context::new(vec![Message::text(Role::User, "question")]);
    let shared = SharedContext::new(context);
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let id = u64::MAX - 1;
    let provider = LlmContinuationProvider::Anthropic;
    let attach = |data, history| {
        let mut frame = Frame::new(data);
        frame
            .set_native_llm_continuation(Arc::new(history))
            .unwrap();
        frame
    };
    let process = |frame| assistant.process_frame(frame, Direction::Downstream);
    process(attach(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(id, provider).unwrap(),
    ))
    .unwrap();
    let mut blocks = vec![
        LlmContinuationBlock::Thinking {
            text: "valid thought".into(),
            signature: "valid-signature".into(),
        },
        LlmContinuationBlock::Text("heard prefix".into()),
    ];
    process(attach(
        FrameData::LlmContinuationCheckpoint,
        NativeLlmContinuation::checkpoint(id, provider, blocks.clone()).unwrap(),
    ))
    .unwrap();
    process(Frame::new(FrameData::LlmText(TextFrame::llm(
        "heard prefix",
    ))))
    .unwrap();
    process(Frame::new(FrameData::LlmAssistantPushAggregation)).unwrap();
    let before = shared.snapshot_rich().unwrap();
    let call = FunctionCall {
        function_name: "echo".into(),
        tool_call_id: "never-admitted".into(),
        arguments: json!({}),
    };
    blocks.extend([
        LlmContinuationBlock::Text("original tail".into()),
        LlmContinuationBlock::ToolCall(call.clone()),
    ]);
    process(attach(
        FrameData::FunctionCallsStarted(vec![call]),
        NativeLlmContinuation::new(id, provider, blocks).unwrap(),
    ))
    .unwrap();
    process(attach(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(id, provider).unwrap(),
    ))
    .unwrap();
    process(Frame::new(FrameData::LlmText(TextFrame::llm(
        "transformed invalid tail",
    ))))
    .unwrap();
    assert!(process(Frame::new(FrameData::LlmResponseEnd)).is_err());
    assert_eq!(shared.snapshot_rich().unwrap(), before);
    process(Frame::interruption()).unwrap();
    assert_eq!(shared.snapshot_rich().unwrap(), before);
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let body = request(&mut socket).await;
        respond(
            &mut socket,
            &[
                json!({"type":"message_start","message":{}}),
                json!({"type":"message_stop"}),
            ],
        )
        .await;
        body
    });
    let effects = Arc::new(AtomicUsize::new(0));
    let mut worker = Pipeline::new(vec![
        llm(endpoint, shared.snapshot().unwrap()),
        executor(effects.clone()),
        assistant,
    ])
    .start(Default::default())
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(shared.frame_data().unwrap()),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let body = tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        body["messages"][1]["content"],
        json!([{"type":"thinking","thinking":"valid thought","signature":"valid-signature"},{"type":"text","text":"heard prefix"}])
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
