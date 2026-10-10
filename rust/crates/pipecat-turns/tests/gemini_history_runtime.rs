//! Native Gemini HTTP/SSE and real ToolExecutor continuation, without an SDK.
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
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn request(socket: &mut TcpStream) -> Value {
    let mut data = vec![];
    let mut buffer = [0u8; 4096];
    let header = loop {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        data.extend_from_slice(&buffer[..count]);
        if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let length = std::str::from_utf8(&data[..header])
        .unwrap()
        .lines()
        .find_map(|line| {
            line.to_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while data.len() < header + length {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        data.extend_from_slice(&buffer[..count]);
    }
    serde_json::from_slice(&data[header..header + length]).unwrap()
}
async fn respond(socket: &mut TcpStream, events: &[Value]) {
    let body: String = events
        .iter()
        .map(|value| format!("data: {value}\n\n"))
        .collect();
    socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
    for bytes in body.as_bytes().chunks(11) {
        socket.write_all(bytes).await.unwrap();
        tokio::task::yield_now().await;
    }
}
fn event(parts: Value) -> Value {
    json!({"candidates":[{"index":0,"content":{"role":"model","parts":parts}}]})
}
fn completed(text: &str) -> Value {
    json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":text}]},"finishReason":"STOP"}]})
}
fn llm(endpoint: String, context: Context) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::Gemini, "local-fixture-key");
    config.endpoint = endpoint;
    config.request_timeout = Duration::from_secs(5);
    Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("gemini-3-fixture", context),
    ))
}
fn tools(effects: &Arc<AtomicUsize>) -> Arc<ToolExecutor> {
    let effect_count = effects.clone();
    let mut tools = ToolExecutor::new(3, 128, 4096).unwrap();
    tools
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |call| {
                let effects = effect_count.clone();
                Box::pin(async move {
                    effects.fetch_add(1, Ordering::SeqCst);
                    Ok(call.arguments)
                })
            })),
        )
        .unwrap();
    Arc::new(tools)
}
struct AudibleText(Arc<Mutex<Vec<String>>>);
#[async_trait]
impl FrameProcessor for AudibleText {
    fn name(&self) -> &str {
        "AudibleText"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::LlmText(text) = &frame.data {
            self.0.lock().unwrap().push(text.text.clone());
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn exact_gemini_parts_and_parallel_tools_survive_second_http_request() {
    continuation_scenario(false).await;
}
#[tokio::test]
async fn fresh_signed_sequential_step_preserves_both_previous_results() {
    continuation_scenario(true).await;
}
async fn continuation_scenario(sequential: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/models/gemini-3-fixture:streamGenerateContent?alt=sse",
        listener.local_addr().unwrap()
    );
    let first = vec![
        event(json!([{"text":"same","thoughtSignature":"text-first+/=="}])),
        event(json!([{"text":"private thought","thought":true,"thoughtSignature":"thought+/=="}])),
        event(json!([{"text":"same","thoughtSignature":"text-second+/=="}])),
        event(json!([{"text":"","thoughtSignature":"empty-tail+/=="}])),
        event(json!([
            {"functionCall":{"id":"tool-a","name":"echo","args":{"n":1}},"thoughtSignature":"parallel+/=="},
            {"functionCall":{"id":"tool-b","name":"echo","args":{"n":2}}},
        ])),
        json!({"candidates":[{"index":0,"finishReason":"STOP"}]}),
    ];
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let original = request(&mut socket).await;
        respond(&mut socket, &first).await;
        drop(socket);
        let (mut socket, _) = listener.accept().await.unwrap();
        let continuation = request(&mut socket).await;
        let mut continuations = vec![continuation];
        if sequential {
            respond(&mut socket, &[
                event(json!([{"functionCall":{"id":"tool-c","name":"echo","args":{"n":3}},"thoughtSignature":"sequential+/=="}])),
                json!({"candidates":[{"index":0,"finishReason":"STOP"}]}),
            ]).await;
            drop(socket);
            let (mut third, _) = listener.accept().await.unwrap();
            continuations.push(request(&mut third).await);
            respond(&mut third, &[completed("done")]).await;
            return (original, continuations);
        }
        respond(&mut socket, &[completed("done")]).await;
        (original, continuations)
    });
    let context = Context::new(vec![Message::text(Role::User, "fixture question")]);
    let shared = SharedContext::new(context.clone());
    let effects = Arc::new(AtomicUsize::new(0));
    let audible = Arc::new(Mutex::new(vec![]));
    let assistant = Arc::new(LLMAssistantAggregator::new(shared, Default::default()));
    let mut worker = Pipeline::new(vec![
        llm(endpoint, context),
        Arc::new(AudibleText(audible.clone())),
        tools(&effects),
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
    let (original, continuations) = tokio::time::timeout(Duration::from_secs(8), peer)
        .await
        .expect("tool results must trigger second native request")
        .unwrap();
    assert_eq!(
        effects.load(Ordering::SeqCst),
        if sequential { 3 } else { 2 }
    );
    assert_eq!(
        original["contents"],
        json!([{"role":"user","parts":[{"text":"fixture question"}]}])
    );
    let mut expected = json!([
        {"role":"user","parts":[{"text":"fixture question"}]},
        {"role":"model","parts":[
            {"text":"same","thoughtSignature":"text-first+/=="},
            {"text":"private thought","thought":true,"thoughtSignature":"thought+/=="},
            {"text":"same","thoughtSignature":"text-second+/=="},
            {"text":"","thoughtSignature":"empty-tail+/=="},
            {"functionCall":{"id":"tool-a","name":"echo","args":{"n":1}},"thoughtSignature":"parallel+/=="},
            {"functionCall":{"id":"tool-b","name":"echo","args":{"n":2}}},
        ]},
        {"role":"user","parts":[
            {"functionResponse":{"id":"tool-a","name":"echo","response":{"n":1}}},
            {"functionResponse":{"id":"tool-b","name":"echo","response":{"n":2}}},
        ]},
    ]).as_array().unwrap().clone();
    assert_eq!(continuations[0]["contents"], json!(expected));
    if sequential {
        expected.extend([
            json!({"role":"model","parts":[{"functionCall":{"id":"tool-c","name":"echo","args":{"n":3}},"thoughtSignature":"sequential+/=="}]}),
            json!({"role":"user","parts":[{"functionResponse":{"id":"tool-c","name":"echo","response":{"n":3}}}]}),
        ]);
        assert_eq!(continuations[1]["contents"], json!(expected));
    }
    assert!(!serde_json::to_string(&continuations)
        .unwrap()
        .contains("skip_thought_signature_validator"));
    assert!(!audible.lock().unwrap().join("").contains("private thought"));
    assert!(audible.lock().unwrap().join("").starts_with("samesame"));
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

struct TextProgress {
    count: AtomicUsize,
    expected: usize,
    ready: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl PipelineObserver for TextProgress {
    async fn on_event(&self, event: ObservationEvent) {
        if matches!(&*event.observation, PipelineObservation::Process { processor, frame, .. }
            if processor.name.as_ref() == "LLMAssistantAggregator" && frame.kind == "LLMTextFrame")
            && self.count.fetch_add(1, Ordering::SeqCst) + 1 == self.expected
        {
            self.ready.add_permits(1);
        }
    }
}
#[tokio::test]
async fn cancellation_during_partial_signature_event_executes_no_tools() {
    cancellation_scenario(false).await;
}
#[tokio::test]
async fn cancellation_during_later_partial_signature_preserves_completed_prefix() {
    cancellation_scenario(true).await;
}
async fn cancellation_scenario(closed_prefix: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/gemini", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n").await.unwrap();
        let mut events = vec![];
        if closed_prefix {
            events.push(event(json!([{"text":"closed thought","thought":true,"thoughtSignature":"complete-first-signature"}])));
        }
        events.push(event(
            json!([{"functionCall":{"id":"never-run","name":"echo","args":{"n":0}}}]),
        ));
        events.push(event(json!([{"text":"heard prefix"}])));
        let mut body: String = events
            .iter()
            .map(|value| format!("data: {value}\n\n"))
            .collect();
        body.push_str("data: {\"candidates\":[{\"index\":0,\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"\",\"thoughtSignature\":\"unfinished-signature");
        socket
            .write_all(format!("{:x}\r\n{}\r\n", body.len(), body).as_bytes())
            .await
            .unwrap();
        ready_tx.send(()).unwrap();
        let mut byte = [0u8];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        closed_tx.send(()).unwrap();
        let (mut next, _) = listener.accept().await.unwrap();
        let captured = request(&mut next).await;
        respond(&mut next, &[completed("next")]).await;
        captured
    });
    let context = Context::new(vec![Message::text(Role::User, "question")]);
    let shared = SharedContext::new(context.clone());
    let effects = Arc::new(AtomicUsize::new(0));
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let progress = Arc::new(tokio::sync::Semaphore::new(0));
    let mut worker = Pipeline::new(vec![llm(endpoint, context), tools(&effects), assistant])
        .with_observer(
            Arc::new(TextProgress {
                count: AtomicUsize::new(0),
                expected: 1,
                ready: progress.clone(),
            }),
            Default::default(),
        )
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), ready_rx)
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), progress.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
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
    closed_rx.await.unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    worker
        .sender()
        .send(
            Frame::new(shared.frame_data().unwrap()),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let next = tokio::time::timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
    let mut expected = vec![json!({"role":"user","parts":[{"text":"question"}]})];
    if closed_prefix {
        expected.push(json!({"role":"model","parts":[{"text":"closed thought","thought":true,"thoughtSignature":"complete-first-signature"},{"text":"heard prefix"}]}));
    } else {
        expected.push(json!({"role":"model","parts":[{"text":"heard prefix"}]}));
    }
    assert_eq!(next["contents"], json!(expected));
    assert!(!next.to_string().contains("unfinished-signature"));
    assert!(!next.to_string().contains("never-run"));
    assert!(!next.to_string().contains("partial thought"));
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

#[tokio::test]
async fn interrupted_signed_part_reuse_fails_before_http_or_tool_effects() {
    let shared = SharedContext::new(Context::new(vec![Message::text(Role::User, "question")]));
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let process = |frame| {
        assistant
            .process_frame(frame, Direction::Downstream)
            .unwrap()
    };
    let attach = |data, history| {
        let mut frame = Frame::new(data);
        frame
            .set_native_llm_continuation(Arc::new(history))
            .unwrap();
        frame
    };
    process(attach(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(500, LlmContinuationProvider::Gemini).unwrap(),
    ));
    process(Frame::new(FrameData::TtsStarted(TtsStartedFrame {
        context_id: Some("partial".into()),
        append_to_context: true,
    })));
    assistant.begin_playback("partial").unwrap();
    process(attach(
        FrameData::LlmResponseEnd,
        NativeLlmContinuation::new(
            500,
            LlmContinuationProvider::Gemini,
            vec![LlmContinuationBlock::GeminiText {
                text: "heard unheard".into(),
                thought: false,
                signature: Some("complete-signature".into()),
            }],
        )
        .unwrap(),
    ));
    assistant
        .confirm_playback(
            "partial",
            &[AggregatedTextFrame {
                text: TextFrame::llm("heard"),
                raw_text: Some("heard".into()),
                context_id: Some("partial".into()),
                aggregated_by: "fixture".into(),
                will_be_spoken: true,
            }],
        )
        .unwrap();
    process(Frame::interruption());
    let saved = serde_json::to_string(&shared.snapshot_rich().unwrap()).unwrap();
    assert!(saved.contains("heard"));
    assert!(!saved.contains("unheard"));
    assert!(!saved.contains("complete-signature"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/gemini", listener.local_addr().unwrap());
    let effects = Arc::new(AtomicUsize::new(0));
    let mut worker = Pipeline::new(vec![
        llm(endpoint, Context::default()),
        tools(&effects),
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
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let FrameData::Error(error) = worker.recv().await.unwrap().frame.data {
                assert!(!format!("{error:?}").contains("complete-signature"));
                break;
            }
        }
    })
    .await
    .expect("invalid Gemini replay must report an error");
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

#[tokio::test]
async fn native_gemini3_tool_without_signed_anchor_has_zero_effects() {
    missing_anchor_scenario(0).await;
}
#[tokio::test]
async fn later_signature_cannot_authorize_an_earlier_unsigned_native_tool() {
    missing_anchor_scenario(1).await;
}
#[tokio::test]
async fn signed_text_cannot_authorize_an_unsigned_native_tool() {
    missing_anchor_scenario(2).await;
}
async fn missing_anchor_scenario(variant: usize) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/gemini", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        request(&mut socket).await;
        let mut parts =
            vec![json!({"functionCall":{"id":"unsigned-native","name":"echo","args":{"n":1}}})];
        if variant == 1 {
            parts.push(json!({"functionCall":{"id":"later-signed","name":"echo","args":{"n":2}},"thoughtSignature":"later-signature"}));
        }
        if variant == 2 {
            parts.insert(0, json!({"text":"private thought","thought":true,"thoughtSignature":"text-signature"}));
        }
        respond(
            &mut socket,
            &[
                event(json!(parts)),
                json!({"candidates":[{"index":0,"finishReason":"STOP"}]}),
            ],
        )
        .await;
        listener
    });
    let context = Context::new(vec![Message::text(Role::User, "question")]);
    let shared = SharedContext::new(context.clone());
    let effects = Arc::new(AtomicUsize::new(0));
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut worker = Pipeline::new(vec![llm(endpoint, context), tools(&effects), assistant])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(worker.recv().await.unwrap().frame.data, FrameData::Error(_)) {
                break;
            }
        }
    })
    .await
    .expect("native unsigned tool history must fail closed");
    let listener = peer.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(shared
        .snapshot()
        .unwrap()
        .messages
        .iter()
        .all(|m| m.tool_calls.is_empty()));
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
