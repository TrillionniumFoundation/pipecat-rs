//! Adversarial service lifecycle tests. All HTTP traffic stays on loopback.
use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, LlmContextFrame, Pipeline, PipelineConfig,
    WorkerState,
};
use pipecat_services::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut chunk = [0; 4096];
    let header_end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < header_end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_completion_tasks_preserve_input_order() {
    const REQUESTS: usize = 32;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut inputs = vec![];
        for _ in 0..REQUESTS {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_request(&mut socket).await;
            let input = body["messages"][0]["content"].as_str().unwrap().to_string();
            inputs.push(input.clone());
            let event = json!({"choices":[{"delta":{"content":input},"finish_reason":"stop"}]});
            let response = format!("data: {event}\n\ndata: [DONE]\n\n");
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        }
        inputs
    });
    let processor = LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("synthetic", Context::default()),
    );
    let mut worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    for n in 0..REQUESTS {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmContext(LlmContextFrame {
                    messages: vec![ChatMessage::new(ChatRole::User, n.to_string())],
                    ..Default::default()
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let mut text = vec![];
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::LlmText(value) = frame.frame.data {
                text.push(value.text);
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    let requests = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    let expected: Vec<_> = (0..REQUESTS).map(|n| n.to_string()).collect();
    assert_eq!(
        requests, expected,
        "serialization must follow input order, not task scheduling order"
    );
    assert_eq!(text, expected);
}

#[tokio::test]
async fn validation_error_marks_terminal_response_as_failed() {
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = "http://127.0.0.1:1/unused".into();
    let processor = LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("", Context::default()),
    );
    let mut worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut error = false;
    let mut terminal = None;
    while let Some(frame) = worker.recv().await {
        match frame.frame.data {
            FrameData::Error(_) => error = true,
            FrameData::LlmResponseEnd => terminal = Some(frame.frame.metadata),
            _ => {}
        }
    }
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    assert!(error);
    assert_eq!(
        terminal.unwrap().get("failed"),
        Some(&json!(true)),
        "all failure paths must identify terminal failure consistently"
    );
}

#[tokio::test]
async fn interruption_discards_queued_completions_before_accepting_fresh_context() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let initial = read_request(&mut first).await;
        assert_eq!(initial["messages"][0]["content"], "old-active");
        first.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let delta = format!(
            "data: {}\n\n",
            json!({"choices":[{"delta":{"content":"initial"}}]})
        );
        first
            .write_all(format!("{:x}\r\n{delta}\r\n", delta.len()).as_bytes())
            .await
            .unwrap();
        let mut byte = [0; 1];
        assert_eq!(first.read(&mut byte).await.unwrap(), 0);
        closed_tx.send(()).unwrap();
        let (mut second, _) = listener.accept().await.unwrap();
        let request = read_request(&mut second).await;
        let response = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"delta":{"content":"fresh-answer"},"finish_reason":"stop"}]})
        );
        second.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        request
    });
    let processor = LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("synthetic", Context::default()),
    );
    let mut worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    for prompt in ["old-active", "old-queued"] {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmContext(LlmContextFrame {
                    messages: vec![ChatMessage::new(ChatRole::User, prompt)],
                    ..Default::default()
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::LlmText(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), closed_rx)
        .await
        .unwrap()
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(LlmContextFrame {
                messages: vec![ChatMessage::new(ChatRole::User, "fresh")],
                ..Default::default()
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut answers = vec![];
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::LlmText(value) = frame.frame.data {
                answers.push(value.text);
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    assert_eq!(answers, ["fresh-answer"]);
    let request = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(request["messages"][0]["content"], "fresh");
}

#[tokio::test]
async fn request_timeout_closes_http_even_when_consumer_channel_is_full() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    config.channel_capacity = 1;
    config.request_timeout = Duration::from_millis(100);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let delta = format!(
            "data: {}\n\n",
            json!({"choices":[{"delta":{"content":"blocked-token"}}]})
        );
        socket
            .write_all(format!("{:x}\r\n{delta}\r\n", delta.len()).as_bytes())
            .await
            .unwrap();
        let mut byte = [0; 1];
        socket.read(&mut byte).await.unwrap()
    });
    let mut stream = LlmClient::new(config)
        .unwrap()
        .stream(CompletionRequest::new(
            "synthetic",
            Context::new(vec![Message::text(Role::User, "Hello")]),
        ))
        .unwrap();
    // Started occupies the only event slot; do not drain until the socket closes.
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(matches!(
        stream.next().await,
        Some(Ok(StreamEvent::Started))
    ));
    assert!(matches!(
        stream.next().await,
        Some(Err(ServiceError::Timeout))
    ));
    assert!(matches!(
        stream.next().await,
        Some(Ok(StreamEvent::Finished { .. }))
    ));
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn upstream_context_and_run_trigger_downstream_completions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut inputs = vec![];
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            inputs.push(read_request(&mut socket).await);
            let response = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"delta":{"content":"continued"},"finish_reason":"stop"}]})
            );
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes()).await.unwrap();
        }
        inputs
    });
    let processor = LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new(
            "synthetic",
            Context::new(vec![Message::text(Role::User, "old")]),
        ),
    );
    let mut worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(LlmContextFrame {
                messages: vec![ChatMessage::new(ChatRole::User, "tool-result-continuation")],
                ..Default::default()
            })),
            Direction::Upstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Upstream)
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Upstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut outputs = vec![];
    let mut passed_context = false;
    let mut forwarded_control = false;
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::LlmText(text) => outputs.push((frame.direction, text.text)),
                FrameData::LlmContext(_) | FrameData::LlmRun => passed_context = true,
                FrameData::UserStoppedSpeaking => {
                    forwarded_control = frame.direction == Direction::Upstream
                }
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    if outputs.len() != 2 {
        server.abort();
    }
    assert_eq!(
        outputs,
        vec![(Direction::Downstream, "continued".into()); 2]
    );
    assert!(!passed_context);
    assert!(forwarded_control);
    let inputs = server.await.unwrap();
    assert!(inputs
        .iter()
        .all(|body| body["messages"][0]["content"] == "tool-result-continuation"));
}
