//! Adversarial loopback test for input/remote-playback acknowledgement ordering.
use pipecat_cli::{
    config::{Options, ProviderConfig},
    run_session_with_input_processors, wire,
};
use pipecat_core::*;
use pipecat_protocol::protobuf::{self as pb, frame::Kind};
use pipecat_services::Role;
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
};

struct DelayedInput(Arc<Notify>);
#[async_trait]
impl FrameProcessor for DelayedInput {
    fn name(&self) -> &str {
        "DelayedInput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::InputText(_)) {
            self.0.notify_one();
            tokio::time::sleep(Duration::from_secs(5)).await;
        }
        ctx.push(frame, direction).await
    }
}
async fn http_request(socket: &mut TcpStream) {
    let mut bytes = vec![];
    let mut buffer = [0; 2048];
    let (end, length) = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0 && bytes.len() < 1024 * 1024);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(at) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            let end = at + 4;
            let header = std::str::from_utf8(&bytes[..end])
                .unwrap()
                .to_ascii_lowercase();
            let length: usize = header
                .lines()
                .find_map(|line| {
                    line.strip_prefix("content-length:")
                        .map(|n| n.trim().parse().unwrap())
                })
                .unwrap();
            break (end, length);
        }
    };
    while bytes.len() < end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
}
async fn next_control(client: &mut WebSocketSession, kind: &str) -> Value {
    loop {
        let Packet::Binary(bytes) = client.recv().await.expect("client closed") else {
            continue;
        };
        if let Some(Kind::Message(message)) = pb::Frame::parse(&bytes).unwrap().frame {
            let control: Value = serde_json::from_str(&message.data).unwrap();
            if control["type"] == kind {
                return control;
            }
        }
    }
}

#[tokio::test]
async fn typed_input_invalidates_old_playback_before_pipeline_can_process_it() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let dg = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let llm = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let agent = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", agent.local_addr().unwrap());
        let config = ProviderConfig {
            deepgram_key: "synthetic".into(), openai_key: "synthetic".into(), cartesia_key: "synthetic".into(),
            deepgram_endpoint: format!("ws://{}/listen", dg.local_addr().unwrap()),
            openai_endpoint: format!("http://{}/chat", llm.local_addr().unwrap()),
            cartesia_endpoint: format!("ws://{}/tts", tts.local_addr().unwrap()),
            model: "test".into(), voice_id: "test".into(), system_prompt: "test".into(), input_rate: 16000, output_rate: 24000,
        };
        let dg_task = tokio::spawn(async move {
            let (socket, _) = dg.accept().await.unwrap();
            let mut session = WebSocketSession::accept(socket, SessionConfig::default()).await.unwrap();
            while session.recv().await.is_some() {}
        });
        let llm_task = tokio::spawn(async move {
            let (mut socket, _) = llm.accept().await.unwrap();
            http_request(&mut socket).await;
            let response = format!("data: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"index":0,"delta":{"content":"Unplayed greeting"},"finish_reason":"stop"}]}));
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response).as_bytes()).await.unwrap();
        });
        let tts_task = tokio::spawn(async move {
            let (socket, _) = tts.accept().await.unwrap();
            let mut session = WebSocketSession::accept(socket, SessionConfig::default()).await.unwrap();
            while let Some(Packet::Text(text)) = session.recv().await {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["continue"] == false {
                    let id = &value["context_id"];
                    for reply in [
                        json!({"type":"chunk", "context_id":id, "data":"AAABAA=="}),
                        json!({"type":"timestamps", "context_id":id, "word_timestamps":{"words":["Unplayed","greeting"],"start":[0.0,0.00003],"end":[0.00003,0.00006]}}),
                        json!({"type":"done", "context_id":id}),
                    ] { session.sender().send(Packet::Text(reply.to_string())).await.unwrap(); }
                    session.finish().await.unwrap();
                    return;
                }
            }
        });
        let delayed = Arc::new(Notify::new());
        let gate = delayed.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = agent.accept().await.unwrap();
            run_session_with_input_processors(socket, &Options { introduce: true, ..Default::default() }, &config, vec![Arc::new(DelayedInput(gate))]).await.unwrap()
        });
        let mut client = WebSocketSession::connect(&url, SessionConfig::default()).await.unwrap();
        let ready = next_control(&mut client, "playback-ready").await;
        client.sender().send(wire::packet(Kind::Text(pb::TextFrame { id: 10, name: "typed".into(), text: "Stop talking".into() })).unwrap()).await.unwrap();
        delayed.notified().await;
        client.sender().send(wire::message(json!({"type":"playback-complete", "context_id":ready["context_id"]})).unwrap()).await.unwrap();
        let acknowledged = tokio::time::timeout(Duration::from_millis(100), next_control(&mut client, "playback-acknowledged")).await.is_ok();
        client.cancel().await.unwrap();
        let report = server.await.unwrap();
        dg_task.abort();
        llm_task.await.unwrap();
        tts_task.await.unwrap();
        assert!(!acknowledged, "an ACK for interrupted audio must be rejected at the client input boundary");
        assert!(!report.context.messages.iter().any(|message| message.role == Role::Assistant), "unplayed greeting was committed after typed input interrupted it");
    }).await.expect("bounded loopback audit");
}
