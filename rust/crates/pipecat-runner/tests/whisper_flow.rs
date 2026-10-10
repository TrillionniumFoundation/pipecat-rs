//! Offline browser-to-native-Whisper coverage. Models and speech are explicitly
//! provisioned by the caller; only the synthetic LLM and TTS use network I/O.
#![cfg(feature = "whisper")]

#[allow(dead_code)]
#[path = "support/mod.rs"]
mod common;

use pipecat_browser_client::{client_ready, protocol, transcript, websocket_endpoint};
use pipecat_client_server::server::{serve_with_api, ServerConfig};
use pipecat_runner::{
    LocalWhisperConfig, NativeRunner, Options, ProviderConfig, RunnerConfig, SttProvider,
    WhisperSegmentation,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_whisper::DecodingStrategy;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

const ASSISTANT: &str = "Hello from Rust!";

fn whisper(vad: bool) -> LocalWhisperConfig {
    let path = std::env::var_os("PIPECAT_WHISPER_MODEL")
        .expect("set PIPECAT_WHISPER_MODEL to an explicit local GGML model");
    let mut config = LocalWhisperConfig::new(path);
    config.model.inference_threads = 2;
    config.settings.strategy = DecodingStrategy::Greedy { best_of: 1 };
    config.segmentation = if vad {
        WhisperSegmentation::Vad
    } else {
        WhisperSegmentation::FixedWindow(Duration::from_secs(12))
    };
    config
}

fn browser_audio() -> Vec<f32> {
    let path = std::env::var_os("PIPECAT_WHISPER_FIXTURE")
        .expect("set PIPECAT_WHISPER_FIXTURE to the upstream JFK PCM16 WAV");
    let bytes = std::fs::read(path).unwrap();
    let wav = pipecat_audio::read_pcm16_wav(&bytes).unwrap();
    assert_eq!(wav.format.sample_rate(), 16_000);
    assert_eq!(wav.format.channels(), 1);
    // Browser capture is 48 kHz. Repetition preserves the original speech while
    // exercising the runner's streaming normalization to Whisper's 16 kHz.
    let mut samples: Vec<f32> = wav
        .pcm
        .as_chunks::<2>()
        .0
        .iter()
        .flat_map(|sample| {
            let sample = i16::from_le_bytes([sample[0], sample[1]]) as f32 / 32768.;
            [sample; 3]
        })
        .collect();
    assert!(samples.len() <= 48_000 * 12);
    // Extra silence crosses the complete-window boundary despite resampler
    // latency; HTTP stop cancels the remaining partial window after the reply.
    samples.resize(48_000 * 12 + 4_800, 0.);
    samples
}

fn verify_jfk(text: &str) {
    let text = text.to_ascii_lowercase();
    for phrase in [
        "ask not what your country can do for you",
        "ask what you can do for your country",
    ] {
        assert!(text.contains(phrase), "actual native transcript: {text}");
    }
}

fn contains_jfk(text: &str) -> bool {
    let text = text.to_ascii_lowercase();
    text.contains("ask not what your country can do for you")
        && text.contains("ask what you can do for your country")
}

fn user_context(body: &Value) -> String {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "user")
        .map(|message| message["content"].as_str().unwrap())
        .collect::<Vec<_>>()
        .join(" ")
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL and PIPECAT_WHISPER_FIXTURE; real native inference"]
async fn browser_pcm_reaches_real_whisper_then_loopback_llm_tts_and_browser_audio() {
    browser_flow(false).await;
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL, PIPECAT_WHISPER_FIXTURE, PIPECAT_WHISPER_VAD_MODEL and PIPECAT_WHISPER_ONNX_RUNTIME"]
async fn paced_browser_pcm_uses_native_silero_and_whisper_then_llm_tts_and_browser_audio() {
    browser_flow(true).await;
}

async fn browser_flow(vad: bool) {
    tokio::time::timeout(Duration::from_secs(120), async {
        let turns = if vad { 1 } else { 2 };
        let vad_paths = vad.then(|| pipecat_cli::config::VadPaths {
            model: std::env::var_os("PIPECAT_WHISPER_VAD_MODEL")
                .expect("set PIPECAT_WHISPER_VAD_MODEL to an explicit local Silero ONNX model")
                .into(),
            runtime: std::env::var_os("PIPECAT_WHISPER_ONNX_RUNTIME")
                .expect("set PIPECAT_WHISPER_ONNX_RUNTIME to an explicit ONNX Runtime library")
                .into(),
        });
        let llm = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", host.local_addr().unwrap());
        let providers = ProviderConfig {
            deepgram_key: String::new(),
            deepgram_endpoint: "ws://127.0.0.1:0/unused-deepgram".into(),
            openai_key: "synthetic-only".into(),
            cartesia_key: "synthetic-only".into(),
            openai_endpoint: format!("http://{}/v1/chat/completions", llm.local_addr().unwrap()),
            cartesia_endpoint: format!("ws://{}/tts/websocket", tts.local_addr().unwrap()),
            model: "synthetic-model".into(),
            voice_id: "synthetic-voice".into(),
            system_prompt: "Be brief".into(),
            input_rate: 16_000,
            output_rate: 24_000,
        };
        let runner = Arc::new(
            NativeRunner::new(
                RunnerConfig {
                    stt: SttProvider::Whisper(whisper(vad)),
                    allowed_origins: vec![origin.clone()],
                    options: Options {
                        introduce: false,
                        vad: vad_paths,
                        turn_timeout: if vad { Duration::from_secs(3) } else { Duration::from_millis(20) },
                        ..Default::default()
                    },
                    ..Default::default()
                },
                providers,
            )
            .unwrap(),
        );
        assert_eq!(runner.active_local_stt_workers(), 0);
        let mut reports = runner.subscribe();
        let (stop, stopped) = oneshot::channel();
        let server = tokio::spawn(serve_with_api(
            host,
            runner.clone(),
            ServerConfig {
                allowed_origins: vec![origin.clone()],
                ..Default::default()
            },
            async {
                let _ = stopped.await;
            },
        ));
        let (requests, mut captured_requests) = mpsc::channel(16);
        let openai = tokio::spawn(async move {
            for turn in 0..16 {
                let (mut socket, _) = llm.accept().await.unwrap();
                let body = read_http_json(&mut socket).await;
                assert_eq!(body["stream"], true);
                let messages = body["messages"].as_array().unwrap();
                let users: Vec<_> = messages.iter().filter(|m| m["role"] == "user").collect();
                assert_eq!(users.len(), turn + 1);
                if !vad {
                    for user in users {
                        verify_jfk(user["content"].as_str().unwrap());
                    }
                }
                let assistants: Vec<_> = messages
                    .iter()
                    .filter(|m| m["role"] == "assistant")
                    .collect();
                assert_eq!(assistants.len(), turn);
                assert!(assistants.iter().all(|message| message["content"] == ASSISTANT));
                let final_request = if vad { contains_jfk(&user_context(&body)) } else { turn + 1 == turns };
                requests.send(body).await.unwrap();
                let response = format!(
                    "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                    json!({"choices":[{"index":0,"delta":{"content":"Hello "}}]}),
                    json!({"choices":[{"index":0,"delta":{"content":"from Rust!"},"finish_reason":"stop"}]}),
                );
                socket
                    .write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}", response.len()).as_bytes())
                    .await
                    .unwrap();
                socket.shutdown().await.unwrap();
                if final_request {
                    return turn + 1;
                }
            }
            panic!("too many LLM requests for the bounded speech fixture");
        });
        let (stop_tts, mut tts_stopped) = oneshot::channel();
        let cartesia = tokio::spawn(async move {
            let mut responses = 0;
            loop {
                tokio::select! {
                    _ = &mut tts_stopped => return responses,
                    result = common::cartesia(&tts) => {
                        result.unwrap();
                        responses += 1;
                    },
                }
            }
        });
        let http = reqwest::Client::new();
        let start = format!("{origin}/api/start");
        let response: Value = http
            .post(&start)
            .header("Origin", &origin)
            .json(&json!({"transport":"websocket"}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(runner.active_local_stt_workers(), 0);
        let url = websocket_endpoint(&url::Url::parse(&start).unwrap(), &response).unwrap();
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = url.as_str().into_client_request().unwrap();
        request.headers_mut().insert("Origin", origin.parse().unwrap());
        let mut client = WebSocketSession::connect_with_request(request, SessionConfig::default())
            .await
            .unwrap();
        client
            .sender()
            .send(Packet::Binary(protocol::encode_message(&client_ready(23)).into()))
            .await
            .unwrap();
        let ready = next_message(&mut client).await;
        assert_eq!(ready["type"], "bot-ready");
        assert_eq!(ready["id"], "23");
        assert_eq!(runner.active_local_stt_workers(), 1);
        let input = browser_audio();
        for turn in 0..turns {
            let sender = client.sender();
            let input = input.clone();
            let send_audio = tokio::spawn(async move {
                let began = tokio::time::Instant::now();
                let mut sent = 0;
                for samples in input.chunks(if vad { 1_536 } else { 16_000 }) {
                    let bytes = protocol::encode_audio(samples, 48_000);
                    assert!(bytes.len() <= 64 * 1024);
                    sender.send(Packet::Binary(bytes.into())).await.unwrap();
                    sent += samples.len();
                    if vad {
                        tokio::time::sleep_until(began + Duration::from_secs_f64(sent as f64 / 48_000.)).await;
                    }
                }
            });
            let mut user = String::new();
            let mut bot = None;
            let mut audio = Vec::new();
            let mut tts_stopped = 0;
            let mut requests_seen = 0;
            let mut final_request_seen = false;
            loop {
                while let Ok(request) = captured_requests.try_recv() {
                    requests_seen += 1;
                    final_request_seen = contains_jfk(&user_context(&request));
                    if !vad {
                        assert_eq!(request["messages"].as_array().unwrap().iter().filter(|m| m["role"] == "user").count(), turn + 1);
                    }
                }
                if contains_jfk(&user) && final_request_seen && bot.as_deref() == Some(ASSISTANT)
                    && tts_stopped == requests_seen && audio.len() == requests_seen * 8 {
                    break;
                }
                let Packet::Binary(bytes) = client.recv().await.expect("browser output") else {
                    panic!("browser output must use binary protobuf")
                };
                match protocol::decode(&bytes).unwrap().body.unwrap() {
                    protocol::frame::Body::Audio(pcm) => {
                        assert_eq!(pcm.sample_rate, 24_000);
                        assert_eq!(pcm.num_channels, 1);
                        audio.extend(pcm.audio);
                    }
                    protocol::frame::Body::Message(message) => {
                        let message: Value = serde_json::from_str(&message.data).unwrap();
                        assert_ne!(message["type"], "bot-started-speaking");
                        assert_ne!(message["type"], "bot-stopped-speaking");
                        if let Some(line) = transcript(&message) {
                            match line.role {
                                "user" => {
                                    user.push(' ');
                                    user.push_str(&line.text);
                                },
                                "bot" => bot = Some(line.text),
                                _ => panic!("unexpected transcript role"),
                            }
                        }
                        if message["type"] == "bot-output" {
                            assert_eq!(message["data"]["spoken"], false);
                        }
                        if message["type"] == "bot-tts-stopped" {
                            tts_stopped += 1;
                        }
                    }
                    _ => {}
                }
            }
            send_audio.await.unwrap();
            verify_jfk(&user);
            assert!(requests_seen > 0);
            assert_eq!(audio, [0, 0, 1, 0, 255, 255, 2, 0].repeat(requests_seen));
        }
        let requests_seen = openai.await.unwrap();
        assert_eq!(runner.local_stt_fixed_windows_submitted(), if vad { 0 } else { turns });
        http
            .post(format!("{origin}/api/sessions/{}/stop", response["sessionId"].as_str().unwrap()))
            .header("Origin", &origin)
            .json(&json!({}))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap();
        while client.recv().await.is_some() {}
        let _ = client.finish().await;
        let report = reports.recv().await.unwrap();
        assert!(report.successful, "session failed: {report:?}");
        assert_eq!(report.pipeline_errors, 0);
        assert_eq!(report.generated_context.messages.len(), requests_seen * 2);
        assert_eq!(runner.active_sessions(), 0);
        assert_eq!(runner.active_local_stt_workers(), 0);
        let _ = stop_tts.send(());
        assert_eq!(cartesia.await.unwrap(), requests_seen);
        runner.shutdown().await;
        assert_eq!(runner.active_local_stt_workers(), 0);
        let _ = stop.send(());
        server.await.unwrap().unwrap();
    })
    .await
    .expect("real Whisper browser flow timed out");
}

async fn next_message(client: &mut WebSocketSession) -> Value {
    loop {
        let Packet::Binary(bytes) = client.recv().await.expect("browser message") else {
            panic!("browser output must use binary protobuf")
        };
        if let Some(protocol::frame::Body::Message(message)) =
            protocol::decode(&bytes).unwrap().body
        {
            return serde_json::from_str(&message.data).unwrap();
        }
    }
}

async fn read_http_json(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(
            n > 0 && bytes.len() + n <= 1024 * 1024,
            "invalid HTTP request"
        );
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(pos) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).unwrap();
    let len = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .and_then(|length| length.trim().parse::<usize>().ok())
        })
        .filter(|length| *length < 1024 * 1024)
        .expect("bounded HTTP content-length");
    while bytes.len() < end + len {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0, "truncated HTTP request");
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[end..end + len]).unwrap()
}
