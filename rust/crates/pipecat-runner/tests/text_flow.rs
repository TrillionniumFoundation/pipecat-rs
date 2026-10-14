//! Real native transport/provider boundaries. Browser DOM execution is separate.
#[allow(dead_code)]
#[path = "support/mod.rs"]
mod common;
#[allow(dead_code)] // Shared child fixture has exit-only cases in its other harness.
#[path = "support/browser_host.rs"]
mod host;
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_browser_client::{
    client_ready, protocol,
    text_entry::{TextEntry, TextPayload},
    websocket_endpoint, InputMode, Transport,
};
use pipecat_client_server::server::{serve_with_api, ServerConfig};
use pipecat_runner::{NativeRunner, Options, ProviderConfig, RunnerConfig};
use pipecat_services::Content;
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
#[cfg(feature = "webrtc")]
use pipecat_webrtc::{
    AudioCodec, RTCSessionDescription, SmallWebRtcConfig, SmallWebRtcConnection, TransportEvent,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{broadcast, oneshot},
    task::JoinHandle,
};

type Outcome<T = ()> = Result<T, String>;
const TYPED: &str = "  你好，世界！🦀\n\"literal\" <script> & ";
macro_rules! get {
    ($value:expr) => {
        $value.map_err(|error| format!("{error:?}"))?
    };
}
macro_rules! check {
    ($condition:expr, $message:expr) => {
        if !$condition {
            return Err($message.into());
        }
    };
}
fn pcm() -> Vec<u8> {
    (0..1440)
        .flat_map(|i| {
            ((i as f64 * std::f64::consts::TAU * 440. / 24000.)
                .sin()
                .mul_add(8000., 0.) as i16)
                .to_le_bytes()
        })
        .collect()
}
async fn stt(listener: &TcpListener) -> Outcome {
    let (stream, _) = get!(listener.accept().await);
    let mut socket = get!(WebSocketSession::accept(stream, SessionConfig::default()).await);
    let observation: Outcome = async {
        while let Some(packet) = socket.recv().await {
            let Packet::Text(text) = packet else { return Err("typed-only fixture received microphone PCM".into()); };
            let value: Value = get!(serde_json::from_str(&text));
            if value["type"] == "CloseStream" { return Ok(()); }
            if value["type"] == "Finalize" {
                get!(socket.sender().send(Packet::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"speech_final":true,"channel":{"alternatives":[{"transcript":""}]}}).to_string())).await);
            }
        }
        Ok(())
    }.await;
    let cleanup = socket.finish().await;
    eprintln!("text STT observation={observation:?}; cleanup={cleanup:?}");
    observation?;
    get!(cleanup);
    Ok(())
}
async fn tts(listener: &TcpListener) -> Outcome {
    let (stream, _) = get!(listener.accept().await);
    let mut socket = get!(WebSocketSession::accept(stream, SessionConfig::default()).await);
    let observation: Outcome = async {
        let mut transcript = String::new();
        while let Some(packet) = socket.recv().await {
            let Packet::Text(text) = packet else {
                return Err("TTS nontext input".into());
            };
            let value: Value = get!(serde_json::from_str(&text));
            transcript.push_str(
                value["transcript"]
                    .as_str()
                    .ok_or("TTS transcript absent")?,
            );
            if value["continue"] == false {
                check!(transcript == "Hello from Rust!", "unexpected TTS content");
                check!(
                    value["output_format"]["sample_rate"] == 24000,
                    "TTS sample rate"
                );
                let id = &value["context_id"];
                get!(
                    socket
                        .sender()
                        .send(Packet::Text(
                            json!({"type":"chunk","context_id":id,"data":STANDARD.encode(pcm())})
                                .to_string()
                        ))
                        .await
                );
                get!(
                    socket
                        .sender()
                        .send(Packet::Text(
                            json!({"type":"done","context_id":id}).to_string()
                        ))
                        .await
                );
                return Ok(());
            }
        }
        Err("TTS closed before complete text".into())
    }
    .await;
    let cleanup = socket.finish().await;
    eprintln!("text TTS observation={observation:?}; cleanup={cleanup:?}");
    observation?;
    get!(cleanup);
    Ok(())
}

enum Peer {
    Ws(WebSocketSession),
    #[cfg(feature = "webrtc")]
    Rtc(SmallWebRtcConnection),
}
enum Incoming {
    Message(Value),
    Pcm(Vec<u8>),
    Interruption,
}
impl Peer {
    async fn send(&self, message: &Value) -> Outcome {
        match self {
            Self::Ws(socket) => get!(
                socket
                    .sender()
                    .send(Packet::Binary(protocol::encode_message(message).into()))
                    .await
            ),
            #[cfg(feature = "webrtc")]
            Self::Rtc(peer) => get!(peer.send_app_message(message).await),
        }
        Ok(())
    }
    async fn text(&self, payload: TextPayload) -> Outcome {
        match (self, payload) {
            (Self::Ws(socket), TextPayload::WebSocket(bytes)) => {
                get!(socket.sender().send(Packet::Binary(bytes.into())).await)
            }
            #[cfg(feature = "webrtc")]
            (Self::Rtc(peer), TextPayload::WebRtc(text)) => get!(
                peer.send_app_message(&get!(serde_json::from_str::<Value>(&text)))
                    .await
            ),
            _ => return Err("wrong text transport encoding".into()),
        }
        Ok(())
    }
    async fn recv(&mut self) -> Outcome<Incoming> {
        match self {
            Self::Ws(socket) => {
                let Packet::Binary(bytes) = socket.recv().await.ok_or("WebSocket closed early")?
                else {
                    return Err("non-protobuf browser output".into());
                };
                match get!(protocol::decode(&bytes))
                    .body
                    .ok_or("missing protobuf body")?
                {
                    protocol::frame::Body::Message(message) => {
                        Ok(Incoming::Message(get!(serde_json::from_str(&message.data))))
                    }
                    protocol::frame::Body::Audio(audio) => {
                        check!(
                            audio.sample_rate == 24000 && audio.num_channels == 1,
                            "WebSocket PCM format"
                        );
                        Ok(Incoming::Pcm(audio.audio))
                    }
                    protocol::frame::Body::Interruption(_) => Ok(Incoming::Interruption),
                    _ => Err("unexpected browser output frame".into()),
                }
            }
            #[cfg(feature = "webrtc")]
            Self::Rtc(peer) => loop {
                match peer.recv_event().await.ok_or("WebRTC closed early")? {
                    TransportEvent::AppMessage(value) => return Ok(Incoming::Message(value)),
                    TransportEvent::Audio(audio) => {
                        check!(
                            audio.codec == AudioCodec::Pcmu
                                && audio.frame.sample_rate == 8000
                                && audio.frame.num_frames() == 160,
                            "WebRTC PCM packet format"
                        );
                        return Ok(Incoming::Pcm(audio.frame.audio));
                    }
                    TransportEvent::Error(error) => return Err(error),
                    _ => {}
                }
            },
        }
    }
    async fn close(self) -> Outcome {
        match self {
            Self::Ws(socket) => get!(socket.finish().await),
            #[cfg(feature = "webrtc")]
            Self::Rtc(peer) => get!(peer.close().await),
        }
        Ok(())
    }
}
struct Fixture {
    runner: Arc<NativeRunner>,
    reports: broadcast::Receiver<pipecat_runner::SessionSummary>,
    origin: String,
    peer: Option<Peer>,
    providers: Vec<JoinHandle<Outcome>>,
    server: JoinHandle<Outcome>,
    stop: Option<oneshot::Sender<()>>,
    endpoints: Vec<String>,
}
impl Fixture {
    async fn new(mode: Transport, sessions: usize) -> Outcome<Self> {
        // Before the first spawn, owned listeners/runners drop normally on setup failure.
        let dg = get!(TcpListener::bind("127.0.0.1:0").await);
        let llm = get!(TcpListener::bind("127.0.0.1:0").await);
        let cartesia = get!(TcpListener::bind("127.0.0.1:0").await);
        let host = get!(TcpListener::bind("127.0.0.1:0").await);
        let origin = format!("http://{}", get!(host.local_addr()));
        let config = RunnerConfig {
            allowed_origins: vec![origin.clone()],
            options: Options {
                introduce: false,
                ..Default::default()
            },
            ..Default::default()
        };
        let providers = ProviderConfig {
            deepgram_key: "synthetic-only".into(),
            openai_key: "synthetic-only".into(),
            cartesia_key: "synthetic-only".into(),
            deepgram_endpoint: format!("ws://{}/stt", get!(dg.local_addr())),
            openai_endpoint: format!("http://{}/llm", get!(llm.local_addr())),
            cartesia_endpoint: format!("ws://{}/tts", get!(cartesia.local_addr())),
            model: "synthetic".into(),
            voice_id: "synthetic".into(),
            system_prompt: "test".into(),
            input_rate: 16000,
            output_rate: 24000,
        };
        let runner = match mode {
            Transport::WebSocket => get!(NativeRunner::new(config, providers)),
            #[cfg(feature = "webrtc")]
            Transport::SmallWebRtc => get!(NativeRunner::new_with_webrtc(
                config,
                providers,
                pipecat_runner::WebRtcRunnerConfig {
                    codec: AudioCodec::Pcmu,
                    localhost_only: true,
                    ..Default::default()
                }
            )),
            _ => return Err("unsupported fixture mode".into()),
        };
        let runner = Arc::new(runner);
        let reports = runner.subscribe();
        let (stop, stopped) = oneshot::channel();
        let api = runner.clone();
        let server_origin = origin.clone();
        let server = tokio::spawn(async move {
            get!(
                serve_with_api(
                    host,
                    api,
                    ServerConfig {
                        allowed_origins: vec![server_origin],
                        ..Default::default()
                    },
                    async {
                        let _ = stopped.await;
                    }
                )
                .await
            );
            Ok(())
        });
        let providers = vec![
            tokio::spawn(async move {
                for _ in 0..sessions {
                    stt(&dg).await?;
                }
                Ok(())
            }),
            tokio::spawn(async move {
                for _ in 0..sessions {
                    get!(common::openai_expected(&llm, false, TYPED).await);
                }
                Ok(())
            }),
            tokio::spawn(async move {
                for _ in 0..sessions {
                    tts(&cartesia).await?;
                }
                Ok(())
            }),
        ];
        Ok(Self {
            runner,
            reports,
            origin,
            peer: None,
            providers,
            server,
            stop: Some(stop),
            endpoints: vec![],
        })
    }
    async fn observe(&mut self, mode: Transport, epoch: u64, disconnect: bool) -> Outcome {
        let http = reqwest::Client::new();
        let start = get!(url::Url::parse(&format!("{}/api/start", self.origin)));
        let denied = get!(
            http.post(start.clone())
                .header("Origin", "https://untrusted.example")
                .json(&mode.start_body())
                .send()
                .await
        );
        check!(
            denied.status() == 403 && self.runner.active_sessions() == 0,
            "origin protection"
        );
        let response: Value = get!(
            get!(get!(
                http.post(start.clone())
                    .header("Origin", &self.origin)
                    .json(&mode.start_body())
                    .send()
                    .await
            )
            .error_for_status())
            .json()
            .await
        );
        check!(
            response["contextMode"] == "generated-text"
                && response["playbackAcknowledgements"] == false,
            "RTVI mode changed"
        );
        match mode {
            Transport::WebSocket => {
                use tokio_tungstenite::tungstenite::client::IntoClientRequest;
                let url = get!(websocket_endpoint(&start, &response));
                let mut request = get!(url.as_str().into_client_request());
                request
                    .headers_mut()
                    .insert("Origin", get!(self.origin.parse()));
                self.peer = Some(Peer::Ws(get!(
                    WebSocketSession::connect_with_request(request, SessionConfig::default()).await
                )));
            }
            #[cfg(feature = "webrtc")]
            Transport::SmallWebRtc => {
                self.peer = Some(Peer::Rtc(get!(
                    SmallWebRtcConnection::new_receive_only(SmallWebRtcConfig {
                        audio_codec: Some(AudioCodec::Pcmu),
                        localhost_only: true,
                        data_channel_label: "chat".into(),
                        ..Default::default()
                    })
                    .await
                )));
                let Some(Peer::Rtc(peer)) = self.peer.as_mut() else {
                    return Err("missing owned native peer".into());
                };
                let offer = get!(peer.create_offer().await);
                let offer_url = get!(pipecat_browser_client::offer_endpoint(
                    &start,
                    response["sessionId"]
                        .as_str()
                        .ok_or("missing session capability")?
                ));
                let answer: Value = get!(
                    get!(get!(
                        http.post(offer_url)
                            .header("Origin", &self.origin)
                            .json(&json!({"sdp":offer.sdp,"type":"offer"}))
                            .send()
                            .await
                    )
                    .error_for_status())
                    .json()
                    .await
                );
                get!(
                    peer.accept_answer(get!(RTCSessionDescription::answer(
                        answer["sdp"].as_str().ok_or("missing SDP answer")?.into()
                    )))
                    .await
                );
                get!(peer.wait_connected(Duration::from_secs(5)).await);
                get!(peer.wait_data_channel(Duration::from_secs(5)).await);
            }
            _ => return Err("unsupported fixture mode".into()),
        }
        let peer = self.peer.as_mut().ok_or("missing owned peer")?;
        let mut entry = TextEntry::new(epoch);
        check!(
            entry.prepare(TYPED, mode, 0).is_err(),
            "early text accepted"
        );
        peer.send(&client_ready(epoch)).await?;
        loop {
            if let Incoming::Message(ready) = peer.recv().await? {
                if ready["type"] == "bot-ready" {
                    check!(entry.accept_ready(&ready), "mismatched actual bot-ready");
                    break;
                }
            }
        }
        let request = entry.prepare(TYPED, mode, 0)?;
        check!(
            request.id == format!("text-{epoch}-1"),
            "replayed request ID"
        );
        peer.text(request.payload).await?;
        let (mut text, mut audio, mut stopped, mut interrupted) =
            (String::new(), vec![], false, false);
        let expected_bytes = if mode == Transport::WebSocket {
            pcm().len()
        } else {
            960
        };
        while text != "Hello from Rust!" || audio.len() < expected_bytes || !stopped {
            match peer.recv().await? {
                Incoming::Message(message) => {
                    check!(
                        message["type"] != "user-transcription",
                        "typed text fabricated an STT transcript"
                    );
                    if message["type"] == "bot-output" {
                        check!(
                            message["data"]["spoken"] == false,
                            "generated text claimed spoken"
                        );
                        text = message["data"]["text"]
                            .as_str()
                            .ok_or("missing bot text")?
                            .into();
                    }
                    if message["type"] == "bot-tts-stopped" {
                        stopped = true;
                    }
                }
                Incoming::Pcm(bytes) => audio.extend(bytes),
                Incoming::Interruption => interrupted = true,
            }
        }
        if mode == Transport::WebSocket {
            check!(
                interrupted && audio == pcm(),
                "exact PCM or interruption missing"
            );
        } else {
            check!(
                audio.len() == 960
                    && audio
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .any(|s| i16::from_le_bytes([s[0], s[1]]).abs() > 100),
                "decoded native WebRTC audio missing"
            );
        }
        #[cfg(feature = "webrtc")]
        if let Peer::Rtc(rtc) = peer {
            let silent = pipecat_core::AudioRawFrame::new(vec![0; 320], 8_000, 1)
                .map_err(|e| format!("{e:?}"))?;
            check!(
                matches!(
                    rtc.send_audio(silent.clone()).await,
                    Err(pipecat_webrtc::Error::AudioDisabled)
                ),
                "runner client audio send accepted"
            );
            check!(
                matches!(
                    rtc.try_send_audio(silent),
                    Err(pipecat_webrtc::Error::AudioDisabled)
                ),
                "runner client try audio send accepted"
            );
            let stats = rtc.stats();
            eprintln!("runner receive-only client transport counters={stats:?}");
            check!(
                stats.audio_packets_sent == 0 && stats.audio_packets_received > 0,
                "runner receive-only media counters"
            );
        }
        entry.close();
        check!(
            entry.prepare("late", mode, 0).is_err(),
            "post-close text accepted"
        );
        if disconnect {
            let closed = self
                .peer
                .take()
                .ok_or("missing closing peer")?
                .close()
                .await;
            self.endpoints
                .push(format!("explicit disconnect: {closed:?}"));
            closed?;
        } else {
            peer.send(&json!({"label":"rtvi-ai","id":"bye","type":"disconnect-bot"}))
                .await?;
        }
        let report = get!(self.reports.recv().await);
        check!(
            report.successful && report.pipeline_errors == 0,
            "runner cleanup/report failed"
        );
        check!(
            report.generated_context.messages.len() == 2,
            "wrong generated context length"
        );
        check!(
            report.generated_context.messages[0].content == Some(Content::Text(TYPED.into())),
            "exact typed user context missing"
        );
        check!(
            report.generated_context.messages[1].content
                == Some(Content::Text("Hello from Rust!".into())),
            "exact assistant context missing"
        );
        check!(
            self.runner.active_sessions() == 0,
            "session remained admitted"
        );
        if let Some(peer) = self.peer.take() {
            let closed = peer.close().await;
            self.endpoints.push(format!("ended peer: {closed:?}"));
            closed?;
        }
        Ok(())
    }
    async fn cleanup(mut self) -> Vec<String> {
        let mut failures = vec![];
        if let Some(peer) = self.peer.take() {
            let result = tokio::time::timeout(Duration::from_secs(5), peer.close()).await;
            self.endpoints.push(format!("cleanup peer: {result:?}"));
            if !matches!(result, Ok(Ok(()))) {
                failures.push(format!("peer cleanup: {result:?}"));
            }
        }
        if tokio::time::timeout(Duration::from_secs(8), self.runner.shutdown())
            .await
            .is_err()
        {
            failures.push("runner shutdown timeout; join unverified".into());
        }
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        for (index, mut task) in self.providers.into_iter().chain([self.server]).enumerate() {
            match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
                Ok(result) => {
                    self.endpoints.push(format!("task {index}: {result:?}"));
                    if !matches!(result, Ok(Ok(()))) {
                        failures.push(format!("task {index}: {result:?}"));
                    }
                }
                Err(_) => {
                    task.abort();
                    let joined = task.await;
                    failures.push(format!(
                        "task {index} deadline; fallback abort joined: {joined:?}"
                    ));
                }
            }
        }
        eprintln!(
            "text fixture endpoint outcomes: {:?}; cleanup failures: {failures:?}",
            self.endpoints
        );
        failures
    }
}
async fn run(mode: Transport, sessions: usize) {
    let mut fixture = Fixture::new(mode, sessions)
        .await
        .expect("fixture setup before owners spawned");
    let observation = tokio::time::timeout(Duration::from_secs(35), async {
        for index in 0..sessions {
            fixture
                .observe(mode, 23 + index as u64 * 2, index == 1)
                .await?;
        }
        Ok::<(), String>(())
    })
    .await;
    let cleanup = fixture.cleanup().await;
    eprintln!("text flow mode={mode:?}; observation={observation:?}; cleanup={cleanup:?}");
    assert!(cleanup.is_empty(), "cleanup failed: {cleanup:?}");
    assert!(
        matches!(observation, Ok(Ok(()))),
        "observation failed: {observation:?}"
    );
}
#[tokio::test]
async fn actual_websocket_text_entry_exact_context_fresh_session_and_disconnect() {
    // Consume actual child HTTP configuration, then exercise the native runner
    // with injected loopback providers. No browser or binary provider override.
    let config = host::hosted(&["--text-input"])
        .await
        .expect("owned actual child HTTP probe")
        .into_config()
        .expect("actual hosted configuration and child shutdown");
    assert_eq!(config.input_mode, InputMode::Text);
    assert!(!config.input_mode.captures_microphone());
    run(config.transports[0], 2).await;
}
#[cfg(feature = "webrtc")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_native_webrtc_text_entry_and_generated_reply() {
    let config = host::hosted(&["--text-input", "--webrtc"])
        .await
        .expect("owned actual child HTTP probe")
        .into_config()
        .expect("actual hosted receive-only configuration and child shutdown");
    assert_eq!(config.input_mode, InputMode::Text);
    assert!(!config.input_mode.captures_microphone());
    let mode = *config
        .transports
        .iter()
        .find(|mode| **mode == Transport::SmallWebRtc)
        .expect("actual host must advertise RTC");
    run(mode, 2).await;
}
