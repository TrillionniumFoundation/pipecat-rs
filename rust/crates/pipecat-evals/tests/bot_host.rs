//! Real loopback WebSocket -> native pipeline -> HTTP/SSE -> Rust tools.
#![cfg(feature = "native-bot-example")]
#[path = "support/ports.rs"]
mod fixture_ports;
#[path = "../examples/support/text_app.rs"]
mod text_app;
use pipecat_core::*;
use pipecat_evals::{
    bot_host::*, serializer, Cancellation, EvalSession, ScenarioFile, SessionOptions,
};
use pipecat_protocol::rtvi::Envelope;
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
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
    sync::mpsc,
};

fn config() -> EvalBotHostConfig {
    EvalBotHostConfig {
        startup_timeout: Duration::from_secs(2),
        session_timeout: Duration::from_secs(5),
        host_timeout: Duration::from_secs(10),
        shutdown_timeout: Duration::from_secs(2),
        transport: SessionConfig {
            close_timeout: Duration::from_millis(50),
            write_timeout: Duration::from_millis(200),
            ..EvalBotHostConfig::default().transport
        },
        pipeline: PipelineConfig {
            shutdown_timeout: Duration::from_millis(300),
            ..EvalBotHostConfig::default().pipeline
        },
        ..Default::default()
    }
}
fn options() -> SessionOptions {
    SessionOptions {
        handshake_timeout: Duration::from_secs(2),
        default_timeout: Duration::from_secs(2),
        max_duration: Duration::from_secs(5),
        transport: SessionConfig {
            close_timeout: Duration::from_millis(50),
            ..Default::default()
        },
        ..Default::default()
    }
}
async fn request(socket: &mut TcpStream) -> Value {
    let mut data = vec![];
    let mut buf = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&buf[..n]);
        assert!(data.len() < 1024 * 1024);
        if let Some(index) = data.windows(4).position(|v| v == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let length: usize = std::str::from_utf8(&data[..end])
        .unwrap()
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse().unwrap())
        })
        .unwrap();
    while data.len() < end + length {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&buf[..n]);
    }
    serde_json::from_slice(&data[end..end + length]).unwrap()
}
fn text_response(text: &str) -> String {
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":text},"index":0}]}),
        json!({"choices":[{"delta":{},"finish_reason":"stop","index":0}]})
    )
}
fn tool_response() -> String {
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"sum-call","type":"function","function":{"name":"add","arguments":"{\"a\":2,\"b\":3}"}}]},"index":0}]}),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls","index":0}]})
    )
}
async fn response(socket: &mut TcpStream, body: &str) {
    socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
}
async fn fixture(
    replies: Vec<String>,
) -> (String, mpsc::Receiver<Value>, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (tx, rx) = mpsc::channel(16);
    let task = tokio::spawn(async move {
        for reply in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = request(&mut socket).await;
            tx.send(body).await.unwrap();
            response(&mut socket, &reply).await;
        }
    });
    (endpoint, rx, task)
}
async fn host(
    bot: impl EvalBotFactory + 'static,
    config: EvalBotHostConfig,
) -> (
    String,
    Cancellation,
    tokio::task::JoinHandle<Result<EvalBotHostReport, BotHostError>>,
) {
    let host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    let url = format!("ws://{}", host.local_addr().unwrap());
    let cancel = Cancellation::new();
    let cloned = cancel.clone();
    let task = tokio::spawn(async move { host.serve(&bot, cloned).await });
    (url, cancel, task)
}
async fn send(socket: &WebSocketSession, envelope: Envelope) {
    socket
        .sender()
        .send(Packet::Text(envelope.encode().unwrap()))
        .await
        .unwrap();
}
async fn next(socket: &mut WebSocketSession) -> Envelope {
    let packet = tokio::time::timeout(Duration::from_secs(3), socket.recv())
        .await
        .expect("wire deadline")
        .expect("socket closed");
    match packet {
        Packet::Text(text) => Envelope::parse(text.as_bytes()).unwrap(),
        Packet::Binary(_) => panic!("binary output"),
    }
}
async fn connect(url: &str) -> WebSocketSession {
    let mut socket = WebSocketSession::connect(
        url,
        SessionConfig {
            close_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    send(
        &socket,
        serializer::message(1729, "client-ready", json!({"version":"2.2.0"})),
    )
    .await;
    let ready = next(&mut socket).await;
    assert_eq!(ready.message_type, "bot-ready");
    assert_eq!(ready.id.as_deref(), Some("1729"));
    assert_eq!(
        ready.data.as_ref().unwrap()["capabilities"]["context_mode"],
        "generated-text"
    );
    socket
}
async fn text(socket: &WebSocketSession, value: &str) {
    send(
        socket,
        serializer::message(
            8,
            "send-text",
            json!({"content":value,"options":{"audio_response":false}}),
        ),
    )
    .await;
}
async fn stop(socket: WebSocketSession) {
    send(
        &socket,
        serializer::control(99, serializer::EVAL_CANCEL, json!({})),
    )
    .await;
    let _ = socket.finish().await;
}
async fn settle() {
    tokio::time::timeout(Duration::from_secs(3), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("task owner leaked");
}

#[tokio::test]
async fn eval_session_runs_two_turns_real_tool_continuation_and_negative_assertion() {
    let (endpoint, mut requests, server) = fixture(vec![
        tool_response(),
        text_response("The native sum is 5."),
        text_response("I remember the native sum is 5."),
    ])
    .await;
    let bot = text_app::TextBot::new(endpoint, "synthetic".into()).unwrap();
    let calls = bot.tool_calls.clone();
    let (url, cancel, task) = host(bot, config()).await;
    let scenario = ScenarioFile::from_value(json!({"name":"real-native-two-turn","context":[{"role":"system","content":"seed-sentinel"}],"turns":[{"user":"add two and three","expect":[{"event":"function_call","calls":[{"name":"add","args":{"a":2,"b":3}}]},{"event":"response","text_contains":"native sum is 5"}]},{"user":"remember that result","expect":[{"event":"response","text_contains":"I remember"},{"event":"bot_started_speaking","absent":true,"within_ms":70}]}]})).unwrap().scenarios.remove(0);
    let result = EvalSession::new(scenario, &url, options())
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert!(result.passed, "{result:?}");
    assert!(!result.events_seen.iter().any(|e| matches!(
        e["type"].as_str(),
        Some("bot_started_speaking" | "bot_stopped_speaking")
    )));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let first = requests.recv().await.unwrap();
    assert_eq!(first["messages"][0]["content"], "seed-sentinel");
    assert_eq!(first["messages"].as_array().unwrap().len(), 2);
    assert_eq!(first["tools"][0]["function"]["name"], "add");
    let continuation = requests.recv().await.unwrap();
    let messages = continuation["messages"].as_array().unwrap();
    let tool = messages
        .iter()
        .find(|m| m["role"] == "tool")
        .expect("native tool result missing");
    assert_eq!(tool["tool_call_id"], "sum-call");
    let value: Value = serde_json::from_str(tool["content"].as_str().unwrap()).unwrap();
    assert_eq!(value, json!({"sum":5,"session_invocation":1}));
    let second = requests.recv().await.unwrap();
    assert!(second["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["role"] == "assistant" && m["content"] == "The native sum is 5."));
    server.await.unwrap();
    cancel.cancel();
    task.await.unwrap().unwrap();
    settle().await;
}

#[tokio::test]
async fn seed_is_quiet_and_readiness_is_correlated() {
    let (endpoint, mut requests, server) = fixture(vec![text_response("model-only answer")]).await;
    let (url, _, task) = host(
        text_app::TextBot::new(endpoint, "synthetic".into()).unwrap(),
        config(),
    )
    .await;
    let mut socket = connect(&url).await;
    send(
        &socket,
        serializer::control(
            2,
            serializer::EVAL_CONTEXT,
            json!({"messages":[{"role":"system","content":"quiet seed"}]}),
        ),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert!(
        requests.try_recv().is_err(),
        "seed triggered an HTTP inference"
    );
    text(&socket, "question").await;
    let captured = tokio::time::timeout(Duration::from_secs(2), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(captured["messages"][0]["content"], "quiet seed");
    let mut generated = String::new();
    loop {
        let event = next(&mut socket).await;
        if event.message_type == "bot-llm-text" {
            generated.push_str(event.data.as_ref().unwrap()["text"].as_str().unwrap());
        }
        if event.message_type == "bot-llm-stopped" {
            break;
        }
    }
    assert_eq!(generated, "model-only answer");
    stop(socket).await;
    assert_eq!(
        task.await.unwrap().unwrap().last_exit,
        Some(EvalBotExit::EvalCancel)
    );
    server.await.unwrap();
    settle().await;
}

#[tokio::test]
async fn reconnect_builds_fresh_context_and_tool_state() {
    let (endpoint, mut requests, server) = fixture(vec![
        tool_response(),
        text_response("first result"),
        tool_response(),
        text_response("second result"),
    ])
    .await;
    let bot = text_app::TextBot::new(endpoint, "synthetic".into()).unwrap();
    let calls = bot.tool_calls.clone();
    let (url, cancel, task) = host(bot, config()).await;
    for index in 0..2 {
        let scenario = ScenarioFile::from_value(json!({"name":"fresh","turns":[{"user":format!("scenario-{index}"),"expect":[{"event":"response","text_contains":if index == 0 {"first result"} else {"second result"}}]}]})).unwrap().scenarios.remove(0);
        let result = EvalSession::new(scenario, &url, options())
            .unwrap()
            .run(Cancellation::new())
            .await;
        assert!(result.passed, "{result:?}");
        let initial = requests.recv().await.unwrap();
        assert_eq!(initial["messages"].as_array().unwrap().len(), 2);
        assert_eq!(
            initial["messages"][1]["content"],
            format!("scenario-{index}")
        );
        let continuation = requests.recv().await.unwrap();
        let tool = continuation["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "tool")
            .unwrap();
        let value: Value = serde_json::from_str(tool["content"].as_str().unwrap()).unwrap();
        assert_eq!(value["session_invocation"], 1);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    server.await.unwrap();
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap().sessions, 2);
    settle().await;
}

#[tokio::test]
async fn blocked_model_cancel_closes_http_socket_and_reaps_owners() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        started_tx.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), stream.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let (url, _, task) = host(
        text_app::TextBot::new(endpoint, "synthetic".into()).unwrap(),
        config(),
    )
    .await;
    let socket = connect(&url).await;
    text(&socket, "block inference").await;
    started_rx.await.unwrap();
    stop(socket).await;
    task.await.unwrap().unwrap();
    server.await.unwrap();
    settle().await;
}

struct TtsGuard {
    starts: Arc<AtomicUsize>,
    texts: Arc<AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for TtsGuard {
    fn name(&self) -> &str {
        "RejectUnskippedSynthesis"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::LlmFullResponseStart(start) => {
                assert_eq!(start.skip_tts, Some(true));
                self.starts.fetch_add(1, Ordering::SeqCst);
            }
            FrameData::LlmResponseStart => {
                assert_eq!(frame.metadata.get("skip_tts"), Some(&Value::Bool(true)));
                self.starts.fetch_add(1, Ordering::SeqCst);
            }
            FrameData::LlmText(text) => {
                assert_eq!(text.skip_tts, Some(true));
                self.texts.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
}
struct IntroBot {
    bot: text_app::TextBot,
    starts: Arc<AtomicUsize>,
    texts: Arc<AtomicUsize>,
}
#[async_trait]
impl EvalBotFactory for IntroBot {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        let mut plan = self.bot.create(settings).await?;
        plan.processors.insert(
            2,
            Arc::new(TtsGuard {
                starts: self.starts.clone(),
                texts: self.texts.clone(),
            }),
        );
        Ok(plan)
    }
}
#[tokio::test]
async fn introduction_is_configured_before_first_output_and_no_speech_is_invented() {
    let (endpoint, mut requests, server) =
        fixture(vec![text_response("Hello from the real model.")]).await;
    let mut bot = text_app::TextBot::new(endpoint, "synthetic".into()).unwrap();
    bot.introduce = true;
    let starts = Arc::new(AtomicUsize::new(0));
    let texts = Arc::new(AtomicUsize::new(0));
    let (url, _, task) = host(
        IntroBot {
            bot,
            starts: starts.clone(),
            texts: texts.clone(),
        },
        config(),
    )
    .await;
    let mut socket = connect(&url).await;
    loop {
        let event = next(&mut socket).await;
        assert!(!matches!(
            event.message_type.as_str(),
            "bot-started-speaking" | "bot-stopped-speaking" | "bot-output"
        ));
        if event.message_type == "bot-llm-stopped" {
            break;
        }
    }
    assert_eq!(starts.load(Ordering::SeqCst), 1);
    assert_eq!(texts.load(Ordering::SeqCst), 1);
    assert!(requests.recv().await.unwrap()["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["content"] == "Please introduce yourself."));
    stop(socket).await;
    task.await.unwrap().unwrap();
    server.await.unwrap();
    settle().await;
}

#[tokio::test]
async fn wrong_bind_and_binary_input_are_rejected_and_trigger_disconnect_exits() {
    for ip in ["0.0.0.0:0", "[::]:0", "192.0.2.1:0"] {
        assert!(matches!(
            EvalBotHost::bind(ip.parse().unwrap(), config()).await,
            Err(BotHostError::Invalid(_))
        ));
    }
    let (url, _, task) = host(
        text_app::TextBot::new(
            "http://127.0.0.1:1/v1/chat/completions".into(),
            "unused".into(),
        )
        .unwrap(),
        config(),
    )
    .await;
    let socket = connect(&format!("{url}/?trigger_disconnect=true")).await;
    socket.finish().await.unwrap();
    assert_eq!(
        task.await.unwrap().unwrap().last_exit,
        Some(EvalBotExit::TriggerDisconnect)
    );
    let (url, _, task) = host(
        text_app::TextBot::new(
            "http://127.0.0.1:1/v1/chat/completions".into(),
            "unused".into(),
        )
        .unwrap(),
        config(),
    )
    .await;
    let mut socket = connect(&url).await;
    socket
        .sender()
        .send(Packet::Binary(vec![1, 2, 3].into()))
        .await
        .unwrap();
    // Output already delivered before Drop can remain in the client queue;
    // draining to EOF proves the owner closed without inventing new output.
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(packet) = socket.recv().await {
            let Packet::Text(text) = packet else {
                panic!("binary output");
            };
            let event = Envelope::parse(text.as_bytes()).unwrap();
            assert!(
                matches!(
                    event.message_type.as_str(),
                    "bot-llm-started" | "bot-interrupted" | "user-llm-text"
                ),
                "unexpected output after Drop: {event:?}"
            );
        }
    })
    .await
    .unwrap();
    assert!(matches!(task.await.unwrap(), Err(BotHostError::Invalid(_))));
    drop(socket);
    settle().await;
}

#[tokio::test]
async fn eval_session_external_cancellation_stops_real_bot_when_requested() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        started_tx.send(()).unwrap();
        let mut b = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), stream.read(&mut b))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let (url, _, task) = host(
        text_app::TextBot::new(endpoint, "synthetic".into()).unwrap(),
        config(),
    )
    .await;
    let scenario = ScenarioFile::from_value(json!({"name":"cancel","turns":[{"user":"wait","expect":[{"event":"response","text_contains":"never"}]}]})).unwrap().scenarios.remove(0);
    let cancel = Cancellation::new();
    let cloned = cancel.clone();
    let mut opts = options();
    opts.stop_bot = true;
    let session = tokio::spawn(async move {
        EvalSession::new(scenario, url, opts)
            .unwrap()
            .run(cloned)
            .await
    });
    started_rx.await.unwrap();
    cancel.cancel();
    let result = session.await.unwrap();
    assert!(!result.passed);
    assert!(result.failures.iter().any(|f| f.kind == "cancelled"));
    assert_eq!(
        task.await.unwrap().unwrap().last_exit,
        Some(EvalBotExit::EvalCancel)
    );
    server.await.unwrap();
    settle().await;
}

struct EmitObserverFrames;
#[async_trait]
impl FrameProcessor for EmitObserverFrames {
    fn name(&self) -> &str {
        "ObserverProbeSource"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmMessagesAppend(_)) {
            for data in [
                FrameData::LlmResponseStart,
                FrameData::VadUserStartedSpeakingDetailed(Default::default()),
                FrameData::LlmMarkerResponse(LlmMarkerResponseFrame {
                    raw: "complete: yes".into(),
                    marker: Some("complete".into()),
                    kind: Some("complete".into()),
                    markers: vec!["complete".into()],
                }),
                FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
                    function_name: "probe".into(),
                    tool_call_id: "probe-call".into(),
                    arguments: json!({"private_argument":"visible-only-at-full"}),
                    cancel_on_interruption: true,
                    group_id: None,
                }),
                FrameData::LlmResponseEnd,
            ] {
                ctx.push(Frame::new(data), Direction::Downstream).await?;
            }
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
struct ProbeFactory;
#[async_trait]
impl EvalBotFactory for ProbeFactory {
    async fn create(&self, _: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        Ok(EvalBotPipeline {
            context: Default::default(),
            processors: vec![Arc::new(EmitObserverFrames)],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
#[tokio::test]
async fn configure_changes_real_observer_name_arguments_vad_and_markers() {
    for (level, enabled) in [("none", false), ("name", false), ("full", true)] {
        let (url, _, task) = host(ProbeFactory, config()).await;
        let mut socket = connect(&url).await;
        send(&socket, serializer::control(3, serializer::EVAL_CONFIGURE, json!({"function_call_report_level":{"*":level},"vad_user_speaking":enabled,"llm_markers":enabled}))).await;
        text(&socket, "observe").await;
        let mut events = vec![];
        loop {
            let event = next(&mut socket).await;
            let done = event.message_type == "bot-llm-stopped";
            events.push(event);
            if done {
                break;
            }
        }
        assert_eq!(
            events
                .iter()
                .any(|e| e.message_type == "vad-user-started-speaking"),
            enabled
        );
        assert_eq!(
            events.iter().any(|e| e.message_type == "bot-llm-marker"),
            enabled
        );
        let call = events
            .iter()
            .find(|e| e.message_type == "llm-function-call-in-progress")
            .unwrap()
            .data
            .as_ref()
            .unwrap();
        assert_eq!(
            call.get("function_name").is_some_and(Value::is_string),
            level != "none"
        );
        assert_eq!(
            call.get("arguments").is_some_and(Value::is_object),
            level == "full"
        );
        if level == "full" {
            assert_eq!(
                call["arguments"]["private_argument"],
                "visible-only-at-full"
            );
        }
        stop(socket).await;
        task.await.unwrap().unwrap();
        settle().await;
    }
}

struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct BlockingTool {
    bot: text_app::TextBot,
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicUsize>,
}
#[async_trait]
impl EvalBotFactory for BlockingTool {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        let mut plan = self.bot.create(settings).await?;
        let mut executor = pipecat_processors::tools::ToolExecutor::new(1, 8, 4096)?;
        let entered = self.entered.clone();
        let dropped = self.dropped.clone();
        executor.register(
            "add",
            pipecat_processors::tools::RegisteredTool::new(Arc::new(move |_| {
                let entered = entered.clone();
                let dropped = dropped.clone();
                Box::pin(async move {
                    let _guard = DropCount(dropped);
                    entered.notify_one();
                    std::future::pending().await
                })
            })),
        )?;
        plan.processors[2] = Arc::new(executor);
        Ok(plan)
    }
}
#[tokio::test]
async fn blocked_native_tool_cancellation_drops_callback_and_all_owners() {
    let (endpoint, mut requests, server) = fixture(vec![tool_response()]).await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let (url, _, task) = host(
        BlockingTool {
            bot: text_app::TextBot::new(endpoint, "synthetic".into()).unwrap(),
            entered: entered.clone(),
            dropped: dropped.clone(),
        },
        config(),
    )
    .await;
    let socket = connect(&url).await;
    text(&socket, "add").await;
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    requests.recv().await.unwrap();
    stop(socket).await;
    task.await.unwrap().unwrap();
    server.await.unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    settle().await;
}

#[tokio::test]
async fn dropping_host_future_cancels_model_and_closes_listener() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        request(&mut stream).await;
        started_tx.send(()).unwrap();
        let mut b = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), stream.read(&mut b))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let (url, _, task) = host(
        text_app::TextBot::new(endpoint, "synthetic".into()).unwrap(),
        config(),
    )
    .await;
    let mut socket = connect(&url).await;
    text(&socket, "block").await;
    started_rx.await.unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    // Output already delivered before Drop can remain in the client queue;
    // draining to EOF proves the owner closed without inventing new output.
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(packet) = socket.recv().await {
            let Packet::Text(text) = packet else {
                panic!("binary output");
            };
            let event = Envelope::parse(text.as_bytes()).unwrap();
            assert!(
                matches!(
                    event.message_type.as_str(),
                    "bot-llm-started" | "bot-interrupted" | "user-llm-text"
                ),
                "unexpected output after Drop: {event:?}"
            );
        }
    })
    .await
    .unwrap();
    drop(socket);
    server.await.unwrap();
    assert!(WebSocketSession::connect(
        &url,
        SessionConfig {
            connect_timeout: Duration::from_millis(100),
            ..Default::default()
        }
    )
    .await
    .is_err());
    settle().await;
}

#[tokio::test]
async fn existing_release_manifest_starts_and_reaps_the_compiled_real_bot() {
    use pipecat_evals::{
        process::ProcessOptions,
        release::{run_release, ReleaseManifest},
    };
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!(
        "native-text-bot-release-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir(&dir).unwrap();
    let port = fixture_ports::port();
    let (endpoint, mut requests, server) = fixture(vec![
        tool_response(),
        text_response("Native release result 5."),
    ])
    .await;
    std::fs::write(dir.join("scenario.yaml"), json!({"name":"native-release","turns":[{"user":"sum","expect":[{"event":"response","text_contains":"Native release result 5"}]}]}).to_string()).unwrap();
    std::fs::write(dir.join("manifest.yaml"), json!({"native_version":1,"concurrency":1,"base_port":port,"scenarios_dir":".","suite":[{"bot":env!("CARGO_BIN_EXE_native-text-bot"),"args":["--port","{port}","--llm-url",endpoint,"--model","synthetic"],"scenarios":["scenario.yaml"]}]}).to_string()).unwrap();
    let manifest = ReleaseManifest::load(dir.join("manifest.yaml")).unwrap();
    let result = run_release(
        manifest,
        options(),
        ProcessOptions {
            startup_timeout: Duration::from_secs(3),
            shutdown_grace: Duration::from_secs(2),
            output_drain_timeout: Duration::from_secs(1),
            ..Default::default()
        },
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(result.runs.len(), 1);
    let process = &result.runs[0].hosted.process;
    assert!(
        process.reaped && process.success && !process.forced,
        "{process:?}"
    );
    #[cfg(target_os = "linux")]
    assert!(!std::path::Path::new(&format!("/proc/{}", process.pid.unwrap())).exists());
    requests.recv().await.unwrap();
    let second = requests.recv().await.unwrap();
    assert!(second["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["role"] == "tool"));
    server.await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
    settle().await;
}

struct BlockSetup {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicUsize>,
    cleaned: Arc<AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for BlockSetup {
    fn name(&self) -> &str {
        "BlockedSetup"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        let _guard = DropCount(self.dropped.clone());
        self.entered.notify_one();
        std::future::pending().await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct SetupFactory {
    entered: Arc<tokio::sync::Notify>,
    dropped: Arc<AtomicUsize>,
    cleaned: Arc<AtomicUsize>,
}
#[async_trait]
impl EvalBotFactory for SetupFactory {
    async fn create(&self, _: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        Ok(EvalBotPipeline {
            context: Default::default(),
            processors: vec![Arc::new(BlockSetup {
                entered: self.entered.clone(),
                dropped: self.dropped.clone(),
                cleaned: self.cleaned.clone(),
            })],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
#[tokio::test]
async fn disconnect_during_processor_setup_waits_for_owned_cleanup() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (url, _, task) = host(
        SetupFactory {
            entered: entered.clone(),
            dropped: dropped.clone(),
            cleaned: cleaned.clone(),
        },
        config(),
    )
    .await;
    let socket = WebSocketSession::connect(
        &format!("{url}/?trigger_disconnect=true"),
        SessionConfig {
            close_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    entered.notified().await;
    socket.finish().await.unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    settle().await;
}

struct Burst;
#[async_trait]
impl FrameProcessor for Burst {
    fn name(&self) -> &str {
        "Burst"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmMessagesAppend(_)) {
            for _ in 0..512 {
                ctx.push(
                    Frame::new(FrameData::LlmText(TextFrame::new("bounded"))),
                    Direction::Downstream,
                )
                .await?;
                // Exercise the cumulative meter, not unrelated instantaneous queues.
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
struct BurstFactory;
#[async_trait]
impl EvalBotFactory for BurstFactory {
    async fn create(&self, _: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        Ok(EvalBotPipeline {
            context: Default::default(),
            processors: vec![Arc::new(Burst)],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
#[tokio::test]
async fn cumulative_processed_frame_budget_fails_bounded_and_reaps() {
    let mut limits = config();
    limits.max_processed_frames = 80;
    let (url, _, task) = host(BurstFactory, limits).await;
    let socket = connect(&url).await;
    text(&socket, "burst").await;
    let result = tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
    let error = result.expect_err("burst must hit the cumulative frame meter");
    assert!(
        error
            .to_string()
            .contains("eval processed frame budget exceeded"),
        "{error:?}"
    );
    drop(socket);
    settle().await;
}

struct WireBurst;
#[async_trait]
impl FrameProcessor for WireBurst {
    fn name(&self) -> &str {
        "BlockedWireOutputProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmMessagesAppend(_)) {
            for _ in 0..256 {
                let data = json!({"label":"rtvi-ai","type":"server-message","data":{"text":"x".repeat(120 * 1024)}});
                ctx.push(
                    Frame::new(FrameData::OutputTransportMessageUrgent(data)),
                    Direction::Downstream,
                )
                .await?;
            }
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
struct WireBurstFactory;
#[async_trait]
impl EvalBotFactory for WireBurstFactory {
    async fn create(&self, _: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        Ok(EvalBotPipeline {
            context: Default::default(),
            processors: vec![Arc::new(WireBurst)],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
#[tokio::test]
async fn blocked_output_reports_the_actual_queue_or_write_failure_and_reaps() {
    let mut limits = config();
    limits.pipeline.output_capacity = 8;
    limits.pipeline.queue_capacity = 512;
    limits.pipeline.system_queue_capacity = 512;
    limits.transport.queue_capacity = 2;
    limits.max_processed_bytes = 64 * 1024 * 1024;
    let (url, _, task) = host(WireBurstFactory, limits).await;
    let mut socket = WebSocketSession::connect(
        &url,
        SessionConfig {
            queue_capacity: 1,
            max_message_bytes: 128 * 1024,
            close_timeout: Duration::from_millis(50),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    send(
        &socket,
        serializer::message(1729, "client-ready", json!({"version":"2.2.0"})),
    )
    .await;
    assert_eq!(next(&mut socket).await.message_type, "bot-ready");
    text(&socket, "fill output").await;
    // Client queue and TCP receive buffers eventually fill; never consume output.
    let result = tokio::time::timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap();
    let error = result.expect_err("undrained output must fail within its bound");
    assert!(
        matches!(
            &error,
            BotHostError::Timeout("event output")
                | BotHostError::Transport(pipecat_transports::TransportError::Timeout("write"))
        ) || error.to_string().contains("worker output queue is full"),
        "{error:?}"
    );
    drop(socket);
    settle().await;
}

#[tokio::test]
async fn full_processor_input_lane_is_reported_and_reaped() {
    let mut limits = config();
    limits.max_processed_bytes = 64 * 1024 * 1024;
    // Default small input lanes must reject this synchronous burst before it
    // can masquerade as a model or output timeout.
    let (url, _, task) = host(WireBurstFactory, limits).await;
    let socket = connect(&url).await;
    text(&socket, "fill input lane").await;
    let error = tokio::time::timeout(Duration::from_secs(4), task)
        .await
        .unwrap()
        .unwrap()
        .expect_err("input lane must saturate");
    assert!(
        matches!(&error, BotHostError::Pipeline(PipelineError::Backpressure(reason)) if reason.contains("input lane is full")),
        "{error:?}"
    );
    drop(socket);
    settle().await;
}

struct EndWithOutput;
#[async_trait]
impl FrameProcessor for EndWithOutput {
    fn name(&self) -> &str {
        "QueuedTerminalOutputProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmMessagesAppend(_)) {
            for data in [
                FrameData::LlmResponseStart,
                FrameData::LlmText(TextFrame::new("admitted before End")),
                FrameData::LlmResponseEnd,
            ] {
                ctx.push(Frame::new(data), Direction::Downstream).await?;
            }
            ctx.push(
                Frame::new(FrameData::EndWorker(ReasonFrame::default())),
                Direction::Downstream,
            )
            .await
        } else {
            ctx.push(frame, direction).await
        }
    }
}
struct EndWithOutputFactory;
#[async_trait]
impl EvalBotFactory for EndWithOutputFactory {
    async fn create(&self, _: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        Ok(EvalBotPipeline {
            context: Default::default(),
            processors: vec![Arc::new(EndWithOutput)],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
#[tokio::test]
async fn graceful_end_flushes_admitted_generated_events_before_session_closes() {
    let (url, cancel, task) = host(EndWithOutputFactory, config()).await;
    let mut socket = connect(&url).await;
    text(&socket, "end").await;
    let mut kinds = vec![];
    let mut generated = String::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(Packet::Text(text)) = socket.recv().await {
            let envelope = Envelope::parse(text.as_bytes()).unwrap();
            if envelope.message_type == "bot-llm-text" {
                generated.push_str(envelope.data.as_ref().unwrap()["text"].as_str().unwrap());
            }
            kinds.push(envelope.message_type);
        }
    })
    .await
    .unwrap();
    assert_eq!(generated, "admitted before End");
    assert!(kinds.iter().any(|kind| kind == "bot-llm-started"));
    assert!(kinds.iter().any(|kind| kind == "bot-llm-stopped"));
    cancel.cancel();
    task.await.unwrap().unwrap();
    drop(socket);
    settle().await;
}

#[test]
fn native_example_requires_explicit_literal_loopback_endpoint() {
    for endpoint in [
        "https://127.0.0.1/v1/chat/completions",
        "http://localhost:1234/v1/chat/completions",
        "http://192.0.2.1/v1/chat/completions",
        "http://user:secret@127.0.0.1/v1/chat/completions",
        "http://127.0.0.1/v1/chat/completions?q=1",
        "http://127.0.0.1/v1/chat/completions#fragment",
    ] {
        assert!(
            text_app::TextBot::new(endpoint.into(), "unused".into()).is_err(),
            "{endpoint}"
        );
    }
    assert!(text_app::TextBot::new(
        "http://127.0.0.1:1234/v1/chat/completions".into(),
        "unused".into()
    )
    .is_ok());
    assert!(text_app::TextBot::new(
        "http://[::1]:1234/v1/chat/completions".into(),
        "unused".into()
    )
    .is_ok());
}

#[tokio::test]
async fn native_example_does_not_follow_http_redirects() {
    let target = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let redirect = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        redirect.local_addr().unwrap()
    );
    let location = format!(
        "http://{}/must-not-be-visited",
        target.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let (mut stream, _) = redirect.accept().await.unwrap();
        request(&mut stream).await;
        stream.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: {location}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    });
    let (url, _, task) = host(
        text_app::TextBot::new(endpoint, "synthetic".into()).unwrap(),
        config(),
    )
    .await;
    let mut socket = connect(&url).await;
    text(&socket, "must not redirect").await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while socket.recv().await.is_some() {}
    })
    .await
    .unwrap();
    assert!(task.await.unwrap().is_err());
    server.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), target.accept())
            .await
            .is_err()
    );
    drop(target);
    drop(socket);
    settle().await;
}

#[tokio::test]
async fn host_rejects_browser_origin_foreign_authority_bad_paths_and_queries() {
    let (url, _, task) = host(ProbeFactory, config()).await;
    let address = url.trim_start_matches("ws://");
    for (path, host_header, origin) in [
        ("/", address, "Origin: http://127.0.0.1\r\n"),
        ("/", "localhost:7860", ""),
        ("/other", address, ""),
        ("/?skip_tts=false", address, ""),
        ("/?skip_tts=true&skip_tts=true", address, ""),
        ("/?capture_bot_audio=true", address, ""),
        ("/?trigger_disconnect=maybe", address, ""),
        ("http://127.0.0.1/", address, ""),
    ] {
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream.write_all(format!("GET {path} HTTP/1.1\r\nHost: {host_header}\r\n{origin}Upgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").as_bytes()).await.unwrap();
        let mut bytes = [0; 1024];
        let n = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut bytes))
            .await
            .unwrap()
            .unwrap();
        let response = String::from_utf8_lossy(&bytes[..n]);
        assert!(response.starts_with("HTTP/1.1 403"), "{path} {response}");
    }
    let socket = connect(&url).await;
    stop(socket).await;
    let report = task.await.unwrap().unwrap();
    assert_eq!(report.rejected_handshakes, 8);
    assert_eq!(report.sessions, 1);
    settle().await;
}

struct FirstToolBlocks {
    first: AtomicUsize,
    blocking: BlockingTool,
}
#[async_trait]
impl EvalBotFactory for FirstToolBlocks {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if self.first.fetch_add(1, Ordering::SeqCst) == 0 {
            self.blocking.create(settings).await
        } else {
            self.blocking.bot.create(settings).await
        }
    }
}
#[tokio::test]
async fn disconnect_during_tool_is_reaped_before_fresh_real_scenario() {
    let (endpoint, mut requests, server) = fixture(vec![
        tool_response(),
        tool_response(),
        text_response("Fresh scenario result 5."),
    ])
    .await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let bot = text_app::TextBot::new(endpoint, "synthetic".into()).unwrap();
    let calls = bot.tool_calls.clone();
    let (url, cancel, task) = host(
        FirstToolBlocks {
            first: AtomicUsize::new(0),
            blocking: BlockingTool {
                bot,
                entered: entered.clone(),
                dropped: dropped.clone(),
            },
        },
        config(),
    )
    .await;
    let first = connect(&url).await;
    text(&first, "old blocked scenario").await;
    entered.notified().await;
    first.cancel().await.unwrap();
    requests.recv().await.unwrap();
    // Busy clients are explicitly rejected. Wait for the previous native tool
    // owner to be dropped before attempting the next scenario.
    tokio::time::timeout(Duration::from_secs(2), async {
        while dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let scenario = ScenarioFile::from_value(json!({"name":"fresh-after-block","turns":[{"user":"new scenario","expect":[{"event":"response","text_contains":"Fresh scenario result 5","text_excludes":"old blocked"}]}]})).unwrap().scenarios.remove(0);
    let result = EvalSession::new(scenario, &url, options())
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert!(result.passed, "{result:?}");
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let fresh = requests.recv().await.unwrap();
    assert_eq!(fresh["messages"].as_array().unwrap().len(), 2);
    assert_eq!(fresh["messages"][1]["content"], "new scenario");
    requests.recv().await.unwrap();
    server.await.unwrap();
    cancel.cancel();
    task.await.unwrap().unwrap();
    settle().await;
}
