//! Failed Gemini completions preserve closed text prefixes but admit no tools.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider, Role,
};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
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
    socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
    for bytes in body.as_bytes().chunks(11) {
        socket.write_all(bytes).await.unwrap();
        tokio::task::yield_now().await;
    }
}
fn event(parts: Value) -> Value {
    json!({"candidates":[{"index":0,"content":{"role":"model","parts":parts}}]})
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
struct TerminalProbe(Arc<tokio::sync::Semaphore>);
#[async_trait]
impl FrameProcessor for TerminalProbe {
    fn name(&self) -> &str {
        "TerminalProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        assert!(!matches!(frame.data, FrameData::FunctionCallsStarted(_)));
        let terminal = matches!(frame.data, FrameData::LlmResponseEnd);
        if terminal {
            assert_eq!(frame.metadata.get("failed"), Some(&json!(true)));
            assert!(frame.native_llm_continuation().unwrap().is_none());
        }
        ctx.push(frame, direction).await?;
        if terminal {
            self.0.add_permits(1);
        }
        Ok(())
    }
}
#[tokio::test]
async fn unsuccessful_gemini_finish_reason_must_not_execute_signed_tools() {
    for reason in [
        Some("SAFETY"),
        Some("MAX_TOKENS"),
        Some("MALFORMED_FUNCTION_CALL"),
        Some("RECITATION"),
        Some("UNKNOWN"),
        None,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/gemini", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            request(&mut socket).await;
            let mut events = vec![
                event(json!([{"text":"heard","thoughtSignature":"retained-prefix"}])),
                event(
                    json!([{"functionCall":{"id":"unsafe","name":"echo","args":{}},"thoughtSignature":"signature"}]),
                ),
            ];
            if let Some(reason) = reason {
                events.push(json!({"candidates":[{"finishReason":reason}]}));
            }
            respond(&mut socket, &events).await;
        });
        let effects = Arc::new(AtomicUsize::new(0));
        let shared = SharedContext::new(Context::default());
        let terminal = Arc::new(tokio::sync::Semaphore::new(0));
        let mut worker = Pipeline::new(vec![
            llm(endpoint, Context::default()),
            Arc::new(TerminalProbe(terminal.clone())),
            tools(&effects),
            Arc::new(LLMAssistantAggregator::new(
                shared.clone(),
                Default::default(),
            )),
        ])
        .start(Default::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), terminal.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        peer.await.unwrap();
        worker.end().await.unwrap();
        worker.join().await.unwrap();
        assert_eq!(
            effects.load(Ordering::SeqCst),
            0,
            "finish reason {reason:?} must not admit tools"
        );
        let history = shared.snapshot().unwrap();
        assert!(history
            .messages
            .iter()
            .all(|message| message.tool_calls.is_empty() && message.role != Role::Tool));
        let wire = pipecat_services::adapters::request_body(
            Provider::Gemini,
            &CompletionRequest::new("gemini-3-fixture", history),
        )
        .unwrap();
        assert_eq!(
            wire["contents"][0]["parts"],
            json!([{"text":"heard","thoughtSignature":"retained-prefix"}])
        );
    }
}
