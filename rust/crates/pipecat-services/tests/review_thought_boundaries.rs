//! Provider block boundaries must remain distinct from native history checkpoints.
use pipecat_core::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

async fn trace(events: Vec<Value>) -> Vec<String> {
    trace_with_checkpoints(events)
        .await
        .into_iter()
        .filter(|entry| entry != "checkpoint")
        .collect()
}
async fn trace_with_checkpoints(events: Vec<Value>) -> Vec<String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
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
        let body: String = events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect();
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
        socket.write_all(body.as_bytes()).await.unwrap();
    });
    let mut config = ClientConfig::new(Provider::Anthropic, "synthetic-local-fixture-key");
    config.endpoint = endpoint;
    config.request_timeout = Duration::from_secs(5);
    let context = Context::new(vec![Message::text(Role::User, "question")]);
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", context),
    ));
    let mut worker = Pipeline::new(vec![llm])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    let trace = tokio::time::timeout(Duration::from_secs(5), async {
        let mut trace = Vec::new();
        loop {
            match worker.recv().await.unwrap().frame.data {
                FrameData::LlmThoughtStart | FrameData::LlmThoughtStartDetailed(_) => {
                    trace.push("start".into())
                }
                FrameData::LlmThoughtText(text) => trace.push(format!("text:{}", text.text)),
                FrameData::LlmThoughtEnd | FrameData::LlmThoughtEndDetailed(_) => {
                    trace.push("end".into())
                }
                FrameData::LlmContinuationCheckpoint => trace.push("checkpoint".into()),
                FrameData::LlmResponseEnd => break trace,
                FrameData::Error(error) => panic!("valid block fixture failed: {}", error.error),
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    peer.await.unwrap();
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    trace
}
fn begin(index: u32, kind: &str, text: &str) -> Value {
    match kind {
        "thinking" => {
            json!({"type":"content_block_start","index":index,"content_block":{"type":"thinking","thinking":text}})
        }
        "text" => {
            json!({"type":"content_block_start","index":index,"content_block":{"type":"text","text":text}})
        }
        _ => unreachable!(),
    }
}
fn sign(index: u32) -> Value {
    json!({"type":"content_block_delta","index":index,"delta":{"type":"signature_delta","signature":format!("signature-{index}")}})
}
fn close(index: u32) -> Value {
    json!({"type":"content_block_stop","index":index})
}

#[tokio::test]
async fn review_later_text_and_tool_checkpoints_do_not_emit_extra_thought_ends() {
    let actual = trace(vec![
        json!({"type":"message_start","message":{}}),
        begin(0, "thinking", "first"), sign(0), close(0),
        begin(1, "text", "visible"), close(1),
        json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"call","name":"effect","input":{}}}), close(2),
        begin(3, "thinking", "second"), sign(3), close(3),
        json!({"type":"message_stop"}),
    ]).await;
    assert_eq!(
        actual,
        ["start", "text:first", "end", "start", "text:second", "end"]
    );
}

#[tokio::test]
async fn review_delayed_earlier_prefix_cannot_end_the_later_active_thought() {
    let actual = trace(vec![
        json!({"type":"message_start","message":{}}),
        begin(0, "text", "lead"),
        begin(1, "thinking", "first"), sign(1), close(1),
        begin(2, "thinking", "second"),
        close(0),
        json!({"type":"content_block_delta","index":2,"delta":{"type":"thinking_delta","thinking":" tail"}}),
        sign(2), close(2),
        json!({"type":"message_stop"}),
    ]).await;
    assert_eq!(
        actual,
        [
            "start",
            "text:first",
            "end",
            "start",
            "text:second",
            "text: tail",
            "end"
        ]
    );
}

#[tokio::test]
async fn review_signed_empty_thought_still_has_balanced_real_boundaries() {
    let actual = trace(vec![
        json!({"type":"message_start","message":{}}),
        begin(0, "thinking", ""),
        sign(0),
        close(0),
        json!({"type":"message_stop"}),
    ])
    .await;
    assert_eq!(actual, ["start", "end"]);
}

#[tokio::test]
async fn review_closed_signature_is_checkpointed_before_its_public_end_boundary() {
    let actual = trace_with_checkpoints(vec![
        json!({"type":"message_start","message":{}}),
        begin(0, "thinking", "closed"),
        sign(0),
        close(0),
        json!({"type":"message_stop"}),
    ])
    .await;
    assert_eq!(actual, ["start", "text:closed", "checkpoint", "end"]);
}
