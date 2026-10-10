//! Real loopback HTTP/SSE checks for a valid prefix followed by stream failure.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
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
    let mut bytes = Vec::new();
    let mut buffer = [0u8; 4096];
    let head = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(at) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let length: usize = std::str::from_utf8(&bytes[..head])
        .unwrap()
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < head + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[head..head + length]).unwrap()
}
async fn respond(socket: &mut TcpStream, events: &[Value]) {
    let body: String = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
    for chunk in body.as_bytes().chunks(7) {
        socket.write_all(chunk).await.unwrap();
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn review_failed_later_block_keeps_exact_signed_prefix_for_next_http_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        request(&mut first).await;
        respond(&mut first, &[
            json!({"type":"message_start","message":{}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"first\n思考"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"valid+/"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signature=="}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"delivered"}}),
            json!({"type":"content_block_stop","index":1}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"unadmitted","name":"effect","input":{}}}),
            json!({"type":"content_block_stop","index":2}),
            json!({"type":"content_block_start","index":3,"content_block":{"type":"thinking","thinking":"partial"}}),
            json!({"type":"content_block_delta","index":3,"delta":{"type":"signature_delta","signature":"partial-signature"}}),
            // A protocol error after an independently completed signed block.
            json!({"type":"content_block_delta","index":3,"delta":{"type":"thinking_delta","thinking":"illegal after signature"}}),
        ]).await;
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
    let original = Context::new(vec![Message::text(Role::User, "question")]);
    let shared = SharedContext::new(original.clone());
    let mut config = ClientConfig::new(Provider::Anthropic, "synthetic-local-fixture-key");
    config.endpoint = endpoint;
    config.request_timeout = Duration::from_secs(5);
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", original),
    ));
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
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
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut worker = Pipeline::new(vec![llm, Arc::new(tools), assistant])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut saw_error = false;
        let mut saw_context = false;
        loop {
            let output = worker.recv().await.unwrap();
            saw_error |= matches!(output.frame.data, FrameData::Error(_));
            if matches!(
                output.frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            ) {
                saw_context = true;
            }
            if saw_error && saw_context {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    worker
        .sender()
        .send(
            Frame::new(shared.frame_data().unwrap()),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let next = tokio::time::timeout(Duration::from_secs(5), peer)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next["messages"][1]["content"],
        json!([
            {"type":"thinking","thinking":"first\n思考","signature":"valid+/signature=="},
            {"type":"text","text":"delivered"}
        ])
    );
    let serialized = next.to_string();
    assert!(!serialized.contains("partial-signature"));
    assert!(!serialized.contains("unadmitted"));
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
