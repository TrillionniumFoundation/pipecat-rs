//! Independent loopback eager ordering and health-ownership regressions.
use pipecat_core::{self as core, *};
use pipecat_services::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

#[derive(Default)]
struct ToolPushes(std::sync::atomic::AtomicUsize);
#[async_trait]
impl PipelineObserver for ToolPushes {
    async fn on_event(&self, event: ObservationEvent) {
        if matches!(&*event.observation, PipelineObservation::Push { frame, first_push: true, .. } if frame.kind == "FunctionCallsStartedFrame")
        {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut chunk = [0; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    serde_json::from_slice(&bytes[end..end + length]).unwrap()
}
fn client(listener: &TcpListener) -> LlmClient {
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    config.request_timeout = Duration::from_secs(30);
    LlmClient::new(config).unwrap()
}
fn context(text: &str) -> LlmContextData {
    LlmContextData {
        messages: vec![ChatMessage::new(ChatRole::User, text).into()],
        tools: ToolSet::NotGiven,
        tool_choice: None,
    }
}
fn inference(owner: &SpeculationOwner) -> Frame {
    let mut frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: context("prediction"),
        speculation: true,
    }));
    frame.set_native_speculation(owner.capability()).unwrap();
    frame
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn recv(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn shutdown(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
}

#[tokio::test]
async fn review_early_confirm_does_not_release_system_tool_before_stop_boundary() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm = Arc::new(LlmProcessor::new(
        client(&listener),
        CompletionRequest::new("synthetic", Context::default()),
    ));
    let owner = SpeculationOwner::new();
    assert!(owner.confirm_with_context(context("Final text!")));
    let (sent, response_sent) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        assert_eq!(request["messages"][0]["content"], "Final text!");
        let body = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"review-tool","type":"function","function":{"name":"effect","arguments":"{}"}}]},"finish_reason":"tool_calls"}]})
        );
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        sent.send(()).unwrap();
    });
    let observer = Arc::new(ToolPushes::default());
    let mut worker = Pipeline::new(vec![llm])
        .with_observer(observer.clone(), ObserverOptions::default())
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, inference(&owner)).await;
    tokio::time::timeout(Duration::from_secs(2), response_sent)
        .await
        .unwrap()
        .unwrap();
    let leaked = tokio::time::timeout(Duration::from_millis(120), async {
        loop {
            let frame = worker.recv().await.unwrap().frame;
            if matches!(
                frame.data,
                FrameData::FunctionCallsStarted(_)
                    | FrameData::LlmText(_)
                    | FrameData::LlmResponseStart
                    | FrameData::LlmResponseEnd
            ) {
                return frame;
            }
        }
    })
    .await;
    assert!(
        leaked.is_err(),
        "response semantic frame escaped before stop boundary: {leaked:?}"
    );
    assert_eq!(
        observer.0.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "tool must be held before the first pipeline push observation"
    );
    let mut boundary = Frame::new(FrameData::UserStoppedSpeaking);
    boundary.set_native_speculation(owner.capability()).unwrap();
    send(&worker, boundary).await;
    let mut ordered = vec![];
    loop {
        let frame = recv(&mut worker).await.frame;
        match frame.data {
            FrameData::UserStoppedSpeaking => ordered.push("stop"),
            FrameData::LlmResponseStart => ordered.push("start"),
            FrameData::FunctionCallsStarted(_) => ordered.push("tool"),
            FrameData::LlmResponseEnd => {
                ordered.push("end");
                break;
            }
            _ => {}
        }
    }
    assert_eq!(ordered.first(), Some(&"stop"));
    assert!(ordered.contains(&"start") && ordered.contains(&"tool"));
    shutdown(worker).await;
    assert_eq!(
        observer.0.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "released tool is observed exactly once"
    );
    server.await.unwrap();
}

#[tokio::test]
async fn review_health_generation_change_drops_stalled_speculative_socket_promptly() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm = Arc::new(LlmProcessor::new(
        client(&listener),
        CompletionRequest::new("synthetic", Context::default()),
    ));
    let owner = SpeculationOwner::new();
    let (ready, request_started) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        ready.send(()).unwrap();
        let mut byte = [0];
        socket.read(&mut byte).await.unwrap()
    });
    let worker = Pipeline::new(vec![llm.clone()])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, inference(&owner)).await;
    tokio::time::timeout(Duration::from_secs(2), request_started)
        .await
        .unwrap()
        .unwrap();
    let health = core::FrameProcessor::health(llm.as_ref()).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect(
                "health change must cancel idle provider ownership without requiring another event"
            )
            .unwrap(),
        0
    );
    assert_eq!(owner.capability().status(), SpeculationStatus::Cancelled);
    shutdown(worker).await;
}

#[tokio::test]
async fn review_byte_overflow_withdraws_but_preserves_uninterruptible_order() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm = Arc::new(
        LlmProcessor::new(
            client(&listener),
            CompletionRequest::new("synthetic", Context::default()),
        )
        .with_speculation_limits(SpeculationLimits {
            max_frames: 64,
            max_bytes: 4096,
        })
        .unwrap(),
    );
    let (ready, request_started) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        ready.send(()).unwrap();
        let mut byte = [0];
        socket.read(&mut byte).await.unwrap()
    });
    let owner = SpeculationOwner::new();
    let worker = Pipeline::new(vec![llm.clone()])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, inference(&owner)).await;
    tokio::time::timeout(Duration::from_secs(2), request_started)
        .await
        .unwrap()
        .unwrap();
    send(
        &worker,
        Frame::text("survivor-first").with_interruptible(false),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while llm.speculation_snapshot().await.buffered_frames < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut huge = Frame::text("survivor-overflow").with_interruptible(false);
    huge.metadata
        .insert("budgeted".into(), "x".repeat(8192).into());
    send(&worker, huge).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        while owner.capability().status() != SpeculationStatus::Cancelled {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(llm.speculation_snapshot().await.buffered_frames, 0);
    assert_eq!(llm.speculation_snapshot().await.buffered_bytes, 0);
    assert!(!owner.confirm(), "overflow withdrawal is terminal");
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let survived: Vec<_> = frames
        .into_iter()
        .filter_map(|f| {
            if let FrameData::Text(t) = f.frame.data {
                Some(t.text)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(survived, ["survivor-first", "survivor-overflow"]);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap(),
        0
    );
}
