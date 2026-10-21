//! Literal-loopback-only fixtures through the production typed constructor.
#![allow(dead_code)]
use pipecat_browser_client::{
    client_ready, protocol,
    text_entry::{TextEntry, TextPayload},
    Transport,
};
use pipecat_client_server::server::{serve_with_api, ServerConfig};
use pipecat_runner::{
    NativeRunner, OpenAiRealtimeVoiceConfig, Options, RunnerConfig, SessionSummary,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
pub const WAIT: Duration = Duration::from_secs(5);
pub struct Provider {
    pub config: OpenAiRealtimeVoiceConfig,
    pub events: mpsc::Sender<Value>,
    pub commands: mpsc::Receiver<Value>,
    task: Option<JoinHandle<()>>,
}
impl Provider {
    pub async fn new(auto_ready: bool) -> Self {
        Self::sessions(auto_ready, 1).await
    }
    pub async fn sessions(auto_ready: bool, sessions: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = OpenAiRealtimeVoiceConfig::new("dummy-realtime-key");
        config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        config.limits.connect_timeout = Duration::from_millis(800);
        let (events, mut inputs) = mpsc::channel::<Value>(32);
        let (output, commands) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            for _ in 0..sessions {
                let (stream, _) = listener.accept().await.unwrap();
                let mut socket = WebSocketSession::accept_with_request_policy(
                    stream,
                    SessionConfig::default(),
                    |request| {
                        request
                            .headers()
                            .get("authorization")
                            .is_some_and(|v| v == "Bearer dummy-realtime-key")
                            && !request.headers().contains_key("openai-beta")
                            && request.uri().path() == "/v1/realtime"
                    },
                )
                .await
                .unwrap();
                let init = read_command(&mut socket).await;
                assert_eq!(init["type"], "session.update");
                output.send(init).await.unwrap();
                if auto_ready {
                    assert!(send_event(&socket, ready()).await);
                }
                loop {
                    tokio::select! {
                        value = inputs.recv() => match value {
                            Some(value) if value["fixture_close"] == true => break,
                            Some(value) => { if !send_event(&socket, value).await { break; } },
                            None => break,
                        },
                        packet = socket.recv() => match packet {
                            Some(Packet::Text(text)) => { if output.send(serde_json::from_str(&text).unwrap()).await.is_err() { break; } }
                            Some(_) => panic!("provider expects JSON"),
                            None => break,
                        }
                    }
                }
                let _ = socket.finish().await;
            }
        });
        Self {
            config,
            events,
            commands,
            task: Some(task),
        }
    }
    pub async fn command(&mut self) -> Value {
        timeout(WAIT, self.commands.recv())
            .await
            .unwrap()
            .expect("provider command")
    }
    pub async fn inference(&mut self) -> Value {
        let mut item = None;
        let mut updates = 0;
        for _ in 0..3 {
            let command = self.command().await;
            match command["type"].as_str() {
                Some("session.update") => {
                    updates += 1;
                    assert_eq!(updates, 1);
                    assert!(command["session"]["tools"]
                        .as_array()
                        .is_none_or(Vec::is_empty));
                }
                Some("conversation.item.create") => {
                    assert!(item.replace(command).is_none(), "duplicate item");
                }
                Some("response.create") => return item.expect("response follows one durable item"),
                _ => panic!("unexpected inference command: {command}"),
            }
        }
        panic!("missing response.create")
    }
    pub async fn event(&self, value: Value) {
        self.events.send(value).await.unwrap();
    }
    pub async fn quiet(&mut self) {
        assert!(
            timeout(Duration::from_millis(100), self.commands.recv())
                .await
                .is_err(),
            "unexpected provider command"
        );
    }
    pub async fn joined(&mut self) {
        timeout(WAIT, self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap();
    }
}
impl Drop for Provider {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
async fn read_command(socket: &mut WebSocketSession) -> Value {
    match timeout(WAIT, socket.recv()).await.unwrap().unwrap() {
        Packet::Text(text) => serde_json::from_str(&text).unwrap(),
        _ => panic!("provider JSON required"),
    }
}
async fn send_event(socket: &WebSocketSession, mut value: Value) -> bool {
    value["event_id"] = json!("local-fixture-event");
    socket
        .sender()
        .send(Packet::Text(value.to_string()))
        .await
        .is_ok()
}
pub fn ready() -> Value {
    json!({"type":"session.updated","session":{"type":"realtime","id":"local-session"}})
}
pub struct Host {
    pub runner: Arc<NativeRunner>,
    pub reports: broadcast::Receiver<SessionSummary>,
    pub origin: String,
    http: reqwest::Client,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}
impl Host {
    pub async fn new(config: OpenAiRealtimeVoiceConfig) -> Self {
        Self::configured(config, false).await
    }
    pub async fn configured(config: OpenAiRealtimeVoiceConfig, introduce: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let runner = Arc::new(
            NativeRunner::new_openai_realtime(
                RunnerConfig {
                    allowed_origins: vec![origin.clone()],
                    max_sessions: 1,
                    connect_timeout: Duration::from_secs(2),
                    max_session_duration: Duration::from_secs(20),
                    options: Options {
                        introduce,
                        idle_timeout: Duration::from_secs(10),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                config,
            )
            .unwrap(),
        );
        let reports = runner.subscribe();
        let (stop, done) = oneshot::channel();
        let api = runner.clone();
        let server_config = ServerConfig {
            allowed_origins: vec![origin.clone()],
            ..Default::default()
        };
        let task = tokio::spawn(async move {
            serve_with_api(listener, api, server_config, async {
                let _ = done.await;
            })
            .await
            .unwrap();
        });
        Self {
            runner,
            reports,
            origin,
            http: reqwest::Client::builder()
                .no_proxy()
                .timeout(WAIT)
                .build()
                .unwrap(),
            stop: Some(stop),
            task: Some(task),
        }
    }
    pub async fn start(&self) -> Value {
        let response = self
            .http
            .post(format!("{}/api/start", self.origin))
            .header("origin", &self.origin)
            .json(&json!({"transport":"websocket"}))
            .send()
            .await
            .unwrap();
        assert!(response.status().is_success());
        response.json().await.unwrap()
    }
    pub async fn browser(&self, start: &Value, id: u64) -> Browser {
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = start["wsUrl"]
            .as_str()
            .unwrap()
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("origin", self.origin.parse().unwrap());
        let socket = WebSocketSession::connect_with_request(request, SessionConfig::default())
            .await
            .unwrap();
        let browser = Browser { socket, id };
        browser.message(client_ready(id)).await;
        browser
    }
    pub async fn report(&mut self) -> SessionSummary {
        timeout(WAIT, self.reports.recv()).await.unwrap().unwrap()
    }
    pub async fn close(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        timeout(WAIT, self.runner.shutdown()).await.unwrap();
        if let Some(task) = self.task.take() {
            timeout(WAIT, task).await.unwrap().unwrap();
        }
        assert_eq!(self.runner.active_sessions(), 0);
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
pub struct Browser {
    pub socket: WebSocketSession,
    pub id: u64,
}
pub enum Incoming {
    Json(Value),
    Audio(Vec<u8>),
    Interrupted,
}
impl Browser {
    pub async fn message(&self, value: Value) {
        self.socket
            .sender()
            .send(Packet::Binary(protocol::encode_message(&value).into()))
            .await
            .unwrap();
    }
    pub async fn recv(&mut self) -> Incoming {
        let Packet::Binary(bytes) = timeout(WAIT, self.socket.recv())
            .await
            .unwrap()
            .expect("browser output")
        else {
            panic!("protobuf output required")
        };
        match protocol::decode(&bytes).unwrap().body.unwrap() {
            protocol::frame::Body::Message(value) => {
                Incoming::Json(serde_json::from_str(&value.data).unwrap())
            }
            protocol::frame::Body::Audio(audio) => {
                assert_eq!((audio.sample_rate, audio.num_channels), (24000, 1));
                Incoming::Audio(audio.audio)
            }
            protocol::frame::Body::Interruption(_) => Incoming::Interrupted,
            _ => panic!("unexpected browser body"),
        }
    }
    pub async fn ready(&mut self) -> Value {
        loop {
            if let Incoming::Json(value) = self.recv().await {
                if value["type"] == "bot-ready" {
                    assert_eq!(value["id"], self.id.to_string());
                    assert_eq!(value["data"]["version"], "2.2.0");
                    return value;
                }
            }
        }
    }
    pub async fn text(&self, text: &str) {
        let mut entry = TextEntry::new(self.id);
        assert!(entry.accept_ready(&json!({"label":"rtvi-ai","type":"bot-ready","id":self.id.to_string(),"data":{"version":"2.2.0"}})));
        let TextPayload::WebSocket(bytes) = entry
            .prepare(text, Transport::WebSocket, 0)
            .unwrap()
            .payload
        else {
            unreachable!()
        };
        self.socket
            .sender()
            .send(Packet::Binary(bytes.into()))
            .await
            .unwrap();
    }
    pub async fn audio(&self, samples: &[f32], rate: u32) {
        self.socket
            .sender()
            .send(Packet::Binary(protocol::encode_audio(samples, rate).into()))
            .await
            .unwrap();
    }
}
pub fn created(id: &str) -> Value {
    json!({"type":"response.created","response":{"id":id,"status":"in_progress","output":[]}})
}
pub fn transcript(id: &str, text: &str) -> Value {
    json!({"type":"response.output_audio_transcript.delta","response_id":id,"item_id":format!("item-{id}"),"output_index":0,"content_index":0,"delta":text})
}
pub fn done(id: &str) -> Value {
    json!({"type":"response.done","response":{"id":id,"status":"completed","output":[]}})
}
pub async fn response(provider: &Provider, id: &str, text: &str, pcm: &[u8]) {
    use base64::Engine;
    provider.event(created(id)).await;
    provider.event(transcript(id, text)).await;
    provider.event(json!({"type":"response.output_audio.delta","response_id":id,"item_id":format!("item-{id}"),"output_index":0,"content_index":0,"delta":base64::engine::general_purpose::STANDARD.encode(pcm)})).await;
    provider.event(json!({"type":"response.output_audio.done","response_id":id,"item_id":format!("item-{id}"),"output_index":0,"content_index":0})).await;
    provider.event(done(id)).await;
}
