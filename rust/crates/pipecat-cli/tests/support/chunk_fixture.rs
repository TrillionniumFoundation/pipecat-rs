//! Real loopback providers and a cleanup-first public CLI session fixture.
//! A synthetic client reports playback; no physical speaker claim is made.
#![allow(dead_code)]
use pipecat_cli::{
    config::{Options, ProviderConfig},
    run_session, run_session_with_input_processors, wire, SessionReport,
};
use pipecat_protocol::protobuf::{self as pb, frame::Kind};
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
    sync::watch,
    task::JoinHandle,
};

pub type TestResult<T> = Result<T, String>;
pub const USER: &str = "hello native agent";
pub const ASSISTANT: &str = "Hello from Rust!";
pub const CHUNK_BYTES: usize = 1920; // 40 ms, 24 kHz, mono PCM16.
pub const TAIL_BYTES: usize = 314;
pub const IO_BUDGET: Duration = Duration::from_secs(3);
pub const JOIN_UNVERIFIED: &str = "v2 pipeline cleanup deadline exceeded; join is unverified";

pub fn pcm() -> Vec<u8> {
    (0..(CHUNK_BYTES * 2 + TAIL_BYTES) / 2)
        .flat_map(|i| ((i % 30_000 + 1) as i16).to_le_bytes())
        .collect()
}
fn error(e: impl std::fmt::Display) -> String {
    e.to_string()
}
pub fn config() -> SessionConfig {
    SessionConfig {
        queue_capacity: 8,
        connect_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_secs(2),
        close_timeout: Duration::from_millis(200),
        ..Default::default()
    }
}
pub async fn bounded<T>(future: impl std::future::Future<Output = TestResult<T>>) -> TestResult<T> {
    tokio::time::timeout(IO_BUDGET, future)
        .await
        .map_err(|_| "loopback operation deadline".to_owned())?
}
pub async fn send(client: &WebSocketSession, value: Value) -> TestResult<()> {
    let packet = wire::message(value).map_err(error)?;
    bounded(async { client.sender().send(packet).await.map_err(error) }).await
}
pub async fn next(client: &mut WebSocketSession) -> TestResult<Kind> {
    bounded(async {
        let packet = client
            .recv()
            .await
            .ok_or_else(|| "peer closed".to_owned())?;
        let Packet::Binary(bytes) = packet else {
            return Err("server emitted non-protobuf packet".into());
        };
        pb::Frame::parse(&bytes)
            .map_err(error)?
            .frame
            .ok_or_else(|| "empty protobuf packet".into())
    })
    .await
}
pub fn control(kind: Kind) -> TestResult<Value> {
    let Kind::Message(message) = kind else {
        return Err(format!("expected control, got {kind:?}"));
    };
    serde_json::from_str(&message.data).map_err(error)
}
pub async fn start_user(client: &WebSocketSession) -> TestResult<()> {
    let packet = wire::packet(Kind::Audio(pb::AudioRawFrame {
        id: 1,
        name: "InputAudioRawFrame#1".into(),
        audio: vec![0; 1024],
        sample_rate: 16000,
        num_channels: 1,
        pts: Some(0),
    }))
    .map_err(error)?;
    bounded(async { client.sender().send(packet).await.map_err(error) }).await
}

pub struct Fixture {
    pub client: Option<WebSocketSession>,
    pub provider_contacts: Arc<AtomicUsize>,
    pub server: Option<JoinHandle<TestResult<SessionReport>>>,
    provider_stop: watch::Sender<bool>,
    tts_release: watch::Sender<bool>,
    pub tts_done_sent: Arc<AtomicUsize>,
    provider_tasks: Vec<JoinHandle<TestResult<()>>>,
}
pub struct Closed {
    pub server: TestResult<SessionReport>,
    pub cleanup_errors: Vec<String>,
    pub contacts: usize,
    pub provider_exits: Vec<String>,
}
impl Fixture {
    pub async fn start(options: Options) -> TestResult<Self> {
        Self::start_gated(options, false).await
    }
    pub async fn start_gated(options: Options, hold_tts_done: bool) -> TestResult<Self> {
        Self::start_inner(options, hold_tts_done, vec![], false).await
    }
    pub async fn start_with_processors(
        options: Options,
        processors: Vec<Arc<dyn pipecat_core::FrameProcessor>>,
    ) -> TestResult<Self> {
        Self::start_inner(options, false, processors, false).await
    }
    pub async fn start_invalid_provider(options: Options) -> TestResult<Self> {
        Self::start_inner(options, false, vec![], true).await
    }
    async fn start_inner(
        options: Options,
        hold_tts_done: bool,
        processors: Vec<Arc<dyn pipecat_core::FrameProcessor>>,
        invalid_provider: bool,
    ) -> TestResult<Self> {
        let dg = TcpListener::bind("127.0.0.1:0").await.map_err(error)?;
        let llm = TcpListener::bind("127.0.0.1:0").await.map_err(error)?;
        let tts = TcpListener::bind("127.0.0.1:0").await.map_err(error)?;
        let agent = TcpListener::bind("127.0.0.1:0").await.map_err(error)?;
        let url = format!("ws://{}", agent.local_addr().map_err(error)?);
        let providers = ProviderConfig {
            deepgram_key: "synthetic-only".into(),
            openai_key: "synthetic-only".into(),
            cartesia_key: "synthetic-only".into(),
            deepgram_endpoint: format!("ws://{}/listen", dg.local_addr().map_err(error)?),
            openai_endpoint: format!("http://{}/chat", llm.local_addr().map_err(error)?),
            cartesia_endpoint: format!("ws://{}/tts", tts.local_addr().map_err(error)?),
            model: "synthetic".into(),
            voice_id: "synthetic".into(),
            system_prompt: "Synthetic protocol test".into(),
            input_rate: if invalid_provider { 0 } else { 16000 },
            output_rate: 24000,
        };
        let contacts = Arc::new(AtomicUsize::new(0));
        let (stop, stop_rx) = watch::channel(false);
        let (tts_release, tts_released) = watch::channel(!hold_tts_done);
        let tts_done_sent = Arc::new(AtomicUsize::new(0));
        let tasks = vec![
            tokio::spawn(deepgram(dg, stop_rx.clone(), contacts.clone())),
            tokio::spawn(openai(
                llm,
                stop_rx.clone(),
                contacts.clone(),
                options.introduce,
            )),
            tokio::spawn(cartesia(
                tts,
                stop_rx,
                contacts.clone(),
                tts_released,
                tts_done_sent.clone(),
            )),
        ];
        let server = tokio::spawn(async move {
            let (stream, _) = agent.accept().await.map_err(error)?;
            let result = if processors.is_empty() {
                run_session(stream, &options, &providers).await
            } else {
                run_session_with_input_processors(stream, &options, &providers, processors).await
            };
            result.map_err(error)
        });
        let mut fixture = Self {
            client: None,
            provider_contacts: contacts,
            server: Some(server),
            provider_stop: stop,
            tts_release,
            tts_done_sent,
            provider_tasks: tasks,
        };
        match WebSocketSession::connect(&url, config()).await {
            Ok(client) => {
                fixture.client = Some(client);
                Ok(fixture)
            }
            Err(e) => {
                let message = format!("client connect: {e}");
                let cleanup = fixture.close().await;
                Err(format!("{message}; cleanup={:?}", cleanup.cleanup_errors))
            }
        }
    }
    pub fn release_tts_done(&self) {
        let _ = self.tts_release.send(true);
    }
    pub fn client(&mut self) -> TestResult<&mut WebSocketSession> {
        self.client
            .as_mut()
            .ok_or_else(|| "client already closed".into())
    }
    /// Retire the real client first, then await the session owner and each provider.
    /// A deadline fallback abort is always joined and retained as failed evidence.
    pub async fn close(self) -> Closed {
        self.close_with_peer_abort(false).await
    }
    pub async fn close_after_abort(self) -> Closed {
        self.close_with_peer_abort(true).await
    }
    async fn close_with_peer_abort(mut self, expected_peer_abort: bool) -> Closed {
        let mut errors = Vec::new();
        let mut provider_exits = Vec::new();
        if let Some(client) = self.client.take() {
            match client.cancel().await {
                Ok(()) => eprintln!("client terminal outcome: joined normally"),
                Err(error) => {
                    let outcome = peer_exit(error);
                    eprintln!("client terminal outcome: joined with {outcome}");
                    if !(expected_peer_abort && outcome.starts_with("expected peer shutdown: ")) {
                        errors.push(format!("client join: {outcome}"));
                    }
                }
            }
        }
        let server = if let Some(mut task) = self.server.take() {
            match tokio::time::timeout(Duration::from_secs(12), &mut task).await {
                Ok(Ok(result)) => result,
                Ok(Err(e)) => Err(format!("server join: {e}")),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    errors.push("server owner required abort after 12 s; abort joined".into());
                    Err("server cleanup deadline".into())
                }
            }
        } else {
            Err("missing server owner".into())
        };
        if server
            .as_ref()
            .is_err_and(|error| error.contains(JOIN_UNVERIFIED))
        {
            errors.push("ordinary session returned an unverified pipeline join".into());
        }
        let _ = self.provider_stop.send(true);
        for (index, mut task) in self.provider_tasks.drain(..).enumerate() {
            match tokio::time::timeout(Duration::from_secs(3), &mut task).await {
                Ok(Ok(Ok(()))) => provider_exits.push(format!("provider {index}: joined normally")),
                Ok(Ok(Err(e))) => {
                    provider_exits.push(format!("provider {index}: joined with {e}"));
                    if !(expected_peer_abort && e.starts_with("expected peer shutdown: ")) {
                        errors.push(format!("provider {index}: {e}"));
                    }
                }
                Ok(Err(e)) => errors.push(format!("provider {index} join: {e}")),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    errors.push(format!("provider {index} required abort; abort joined"));
                }
            }
        }
        eprintln!("provider terminal outcomes (expected_peer_abort={expected_peer_abort}): {provider_exits:?}");
        Closed {
            server,
            cleanup_errors: errors,
            contacts: self.provider_contacts.load(Ordering::SeqCst),
            provider_exits,
        }
    }
    /// Wait for server-initiated close while continuing to read its real socket.
    pub async fn wait_closed(&mut self) -> TestResult<Vec<Kind>> {
        bounded(async {
            let mut packets = Vec::new();
            let client = self.client()?;
            while let Some(packet) = client.recv().await {
                let Packet::Binary(bytes) = packet else {
                    return Err("non-protobuf server packet".into());
                };
                let kind = pb::Frame::parse(&bytes)
                    .map_err(error)?
                    .frame
                    .ok_or_else(|| "missing frame".to_owned())?;
                packets.push(kind);
                if packets.len() > 64 {
                    return Err("unbounded terminal output".into());
                }
            }
            Ok(packets)
        })
        .await
    }
}
async fn cancelled(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}
async fn accept(
    listener: TcpListener,
    stop: &mut watch::Receiver<bool>,
    contacts: &AtomicUsize,
) -> TestResult<Option<TcpStream>> {
    tokio::select! { biased; _ = cancelled(stop) => Ok(None), accepted = listener.accept() => {
        contacts.fetch_add(1, Ordering::SeqCst); accepted.map(|a| Some(a.0)).map_err(error)
    }}
}
async fn deepgram(
    listener: TcpListener,
    mut stop: watch::Receiver<bool>,
    contacts: Arc<AtomicUsize>,
) -> TestResult<()> {
    let Some(stream) = accept(listener, &mut stop, &contacts).await? else {
        return Ok(());
    };
    let mut session = WebSocketSession::accept(stream, config())
        .await
        .map_err(error)?;
    let result: TestResult<()> = async {
        let mut transcribed = false;
        loop {
            let packet = tokio::select! { biased; _ = cancelled(&mut stop) => return Ok(()), packet = session.recv() => packet };
            match packet {
                None => return Ok(()),
                Some(Packet::Binary(audio)) => {
                    if transcribed || audio.as_ref() != [0; 1024] { return Err("STT input PCM differs".into()); }
                    transcribed = true;
                    session.sender().send(Packet::Text(json!({"type":"Results","is_final":true,"speech_final":true,"start":0.0,"duration":0.032,"channel":{"alternatives":[{"transcript":USER,"confidence":1.0,"words":[]}]}}).to_string())).await.map_err(error)?;
                }
                Some(Packet::Text(text)) => {
                    let value: Value = serde_json::from_str(&text).map_err(error)?;
                    if value["type"] == "CloseStream" { return Ok(()); }
                    if value["type"] == "Finalize" {
                        session.sender().send(Packet::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"speech_final":true,"channel":{"alternatives":[{"transcript":""}]}}).to_string())).await.map_err(error)?;
                    }
                }
            }
        }
    }.await;
    let closed = session.cancel().await.map_err(peer_exit);
    result.and(closed)
}
async fn openai(
    listener: TcpListener,
    mut stop: watch::Receiver<bool>,
    contacts: Arc<AtomicUsize>,
    introduction: bool,
) -> TestResult<()> {
    let Some(mut socket) = accept(listener, &mut stop, &contacts).await? else {
        return Ok(());
    };
    let result: TestResult<()> = async {
        let body = tokio::select! { biased; _ = cancelled(&mut stop) => return Ok(()), body = read_http_json(&mut socket) => body? };
        let (role, text) = if introduction { ("developer", "Please introduce yourself to the user.") } else { ("user", USER) };
        if body["stream"] != true || !body["messages"].as_array().is_some_and(|messages| messages.iter().any(|m| m["role"] == role && m["content"] == text)) { return Err("LLM did not receive the expected actual context".into()); }
        let response = format!("data: {}\n\ndata: {}\n\ndata: [DONE]\n\n", json!({"choices":[{"index":0,"delta":{"content":ASSISTANT}}]}), json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}));
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", response.len(), response).as_bytes()).await.map_err(error)?;
        socket.flush().await.map_err(error)
    }.await;
    let _ = socket.shutdown().await;
    result
}
async fn cartesia(
    listener: TcpListener,
    mut stop: watch::Receiver<bool>,
    contacts: Arc<AtomicUsize>,
    mut released: watch::Receiver<bool>,
    done_sent: Arc<AtomicUsize>,
) -> TestResult<()> {
    let Some(stream) = accept(listener, &mut stop, &contacts).await? else {
        return Ok(());
    };
    let mut session = WebSocketSession::accept(stream, config())
        .await
        .map_err(error)?;
    let result: TestResult<()> = async {
        let mut transcript = String::new();
        let mut context = None;
        loop {
            let packet = tokio::select! { biased; _ = cancelled(&mut stop) => return Ok(()), packet = session.recv() => packet };
            let Some(packet) = packet else { return Ok(()); };
            let Packet::Text(text) = packet else { return Err("TTS request was not JSON".into()); };
            let value: Value = serde_json::from_str(&text).map_err(error)?;
            if value.get("cancel").is_some() { continue; }
            let id = value["context_id"].as_str().ok_or_else(|| "TTS request has no context".to_owned())?;
            if context.as_deref().is_some_and(|old| old != id) { return Err("TTS response changed context".into()); }
            context = Some(id.to_owned());
            transcript.push_str(value["transcript"].as_str().ok_or_else(|| "missing transcript".to_owned())?);
            if value["continue"] == false {
                if transcript != ASSISTANT || value["output_format"]["sample_rate"] != 24000 { return Err("TTS synthesis request differs".into()); }
                session.sender().send(Packet::Text(json!({"type":"chunk","context_id":id,"data":base64(&pcm())}).to_string())).await.map_err(error)?;
                while !*released.borrow_and_update() {
                    tokio::select! { biased; _ = cancelled(&mut stop) => return Ok(()), changed = released.changed() => if changed.is_err() { return Err("TTS completion gate lost its owner".into()); } }
                }
                for reply in [
                    json!({"type":"timestamps","context_id":id,"word_timestamps":{"words":["Hello","from","Rust!"],"start":[0.0,0.02,0.04],"end":[0.02,0.04,0.08]}}),
                    json!({"type":"done","context_id":id}),
                ] { session.sender().send(Packet::Text(reply.to_string())).await.map_err(error)?; }
                done_sent.fetch_add(1, Ordering::SeqCst);
                // Keep the peer owned until the actual client/session closes; no detached task.
                loop { tokio::select! { biased; _ = cancelled(&mut stop) => return Ok(()), packet = session.recv() => if packet.is_none() { return Ok(()); } } }
            }
        }
    }.await;
    let closed = session.cancel().await.map_err(peer_exit);
    result.and(closed)
}
async fn read_http_json(socket: &mut TcpStream) -> TestResult<Value> {
    let mut bytes = Vec::new();
    let mut buffer = [0; 2048];
    let end = loop {
        let n = socket.read(&mut buffer).await.map_err(error)?;
        if n == 0 || bytes.len() + n > 1024 * 1024 {
            return Err("invalid HTTP request".into());
        }
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).map_err(error)?;
    let len = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|s| s.trim().parse::<usize>().ok())
        })
        .filter(|n| *n < 1024 * 1024)
        .ok_or_else(|| "missing HTTP length".to_owned())?;
    while bytes.len() < end + len {
        let n = socket.read(&mut buffer).await.map_err(error)?;
        if n == 0 {
            return Err("truncated HTTP request".into());
        }
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[end..end + len]).map_err(error)
}
fn base64(bytes: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for group in bytes.chunks(3) {
        let a = group[0] as usize;
        let b = group.get(1).copied().unwrap_or(0) as usize;
        let c = group.get(2).copied().unwrap_or(0) as usize;
        result.push(CHARS[a >> 2] as char);
        result.push(CHARS[((a & 3) << 4) | (b >> 4)] as char);
        result.push(if group.len() > 1 {
            CHARS[((b & 15) << 2) | (c >> 6)] as char
        } else {
            '='
        });
        result.push(if group.len() > 2 {
            CHARS[c & 63] as char
        } else {
            '='
        });
    }
    result
}

// A negative protocol test may intentionally reset a provider connection.
// Keep that terminal outcome as evidence; only these exact transport-close
// classes may be expected, never invalid provider content or a join deadline.
pub fn peer_exit(error: pipecat_transports::TransportError) -> String {
    let text = error.to_string();
    // Inspect the typed underlying I/O cause. Arbitrary errors with similar
    // wording, provider-content failures and join/time budgets stay failures.
    let reset = match &error {
        pipecat_transports::TransportError::WebSocket(websocket) => {
            std::error::Error::source(websocket)
                .and_then(|cause| cause.downcast_ref::<std::io::Error>())
                .is_some_and(|cause| cause.kind() == std::io::ErrorKind::ConnectionReset)
        }
        _ => false,
    };
    let expected = reset
        || matches!(error, pipecat_transports::TransportError::Closed)
        || matches!(
            text.as_str(),
            "WebSocket: WebSocket protocol error: Connection reset without closing handshake"
                | "WebSocket: Connection closed normally"
                | "WebSocket: Trying to work with closed connection"
        );
    if expected {
        format!("expected peer shutdown: {text}")
    } else {
        text
    }
}
