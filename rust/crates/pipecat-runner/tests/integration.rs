#[path = "support/mod.rs"]
mod common;
use pipecat_browser_client::{client_ready, protocol, transcript, websocket_endpoint};
use pipecat_client_server::server::{serve_with_api, BotApi, ServerConfig};
use pipecat_runner::{
    NativeRunner, Options, ProviderConfig, RunnerConfig, RunnerError, SessionFailureStage,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};

#[derive(Clone, Copy)]
enum Ending {
    Graceful,
    InvalidProtobuf,
    ProviderFailure,
}
#[tokio::test]
async fn actual_http_start_browser_codec_and_synthetic_provider_pipeline() {
    exercise(Ending::Graceful).await;
}
#[tokio::test]
async fn failure_after_generation_retains_context_and_original_transport_error() {
    exercise(Ending::InvalidProtobuf).await;
}
#[tokio::test]
async fn provider_error_retains_generated_context_and_actual_pipeline_error_count() {
    exercise(Ending::ProviderFailure).await;
}
async fn exercise(ending: Ending) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let dg = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let llm = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", host.local_addr().unwrap());
        let providers = ProviderConfig {
            deepgram_key: "synthetic-only".into(),
            openai_key: "synthetic-only".into(),
            cartesia_key: "synthetic-only".into(),
            deepgram_endpoint: format!("ws://{}/v1/listen", dg.local_addr().unwrap()),
            openai_endpoint: format!("http://{}/v1/chat/completions", llm.local_addr().unwrap()),
            cartesia_endpoint: format!("ws://{}/tts/websocket", tts.local_addr().unwrap()),
            model: "synthetic-model".into(),
            voice_id: "synthetic-voice".into(),
            system_prompt: "Be brief".into(),
            input_rate: 16000,
            output_rate: 24000,
        };
        let runner = Arc::new(
            NativeRunner::new(
                RunnerConfig {
                    allowed_origins: vec![origin.clone()],
                    allow_missing_origin: false,
                    options: Options {
                        introduce: false,
                        turn_timeout: Duration::from_millis(20),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                providers,
            )
            .unwrap(),
        );
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
        let deepgram = tokio::spawn(async move {
            for _ in 0..2 {
                common::deepgram(&dg, matches!(ending, Ending::Graceful)).await?;
            }
            Ok::<_, pipecat_cli::AgentError>(())
        });
        let openai = tokio::spawn(async move {
            for _ in 0..2 {
                common::openai(&llm, false).await?;
                if matches!(ending, Ending::ProviderFailure) {
                    let (mut peer, _) = llm.accept().await?;
                    let mut request = Vec::new();
                    while !request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                        let mut buffer = [0; 1024];
                        let count = peer.read(&mut buffer).await?;
                        if count == 0 || request.len() + count > 16 * 1024 {
                            return Err(pipecat_cli::AgentError::Smoke("missing second LLM request"));
                        }
                        request.extend_from_slice(&buffer[..count]);
                    }
                    peer.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await?;
                    peer.shutdown().await?;
                }
            }
            Ok::<_, pipecat_cli::AgentError>(())
        });
        let cartesia = tokio::spawn(async move {
            for _ in 0..2 {
                common::cartesia(&tts).await?;
            }
            Ok::<_, pipecat_cli::AgentError>(())
        });
        let http = reqwest::Client::new();
        let start = format!("{origin}/api/start");
        let rejected = http
            .post(&start)
            .header("Origin", "https://untrusted.example")
            .json(&json!({"transport":"websocket"}))
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), 403);
        assert_eq!(runner.active_sessions(), 0);
        for _ in 0..2 {
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
            assert_eq!(response["contextMode"], "generated-text");
            assert_eq!(response["playbackAcknowledgements"], false);
            let url = websocket_endpoint(&url::Url::parse(&start).unwrap(), &response).unwrap();
            use tokio_tungstenite::tungstenite::client::IntoClientRequest;
            let mut request = url.as_str().into_client_request().unwrap();
            request
                .headers_mut()
                .insert("Origin", origin.parse().unwrap());
            let mut client =
                WebSocketSession::connect_with_request(request, SessionConfig::default())
                    .await
                    .unwrap();
            client
                .sender()
                .send(Packet::Binary(
                    protocol::encode_message(&client_ready(23)).into(),
                ))
                .await
                .unwrap();
            let ready = next_message(&mut client).await;
            assert_eq!(ready["type"], "bot-ready");
            assert_eq!(ready["id"], "23");
            assert_eq!(ready["data"]["version"], "2.2.0");
            // Keep the original audio smoke path. Failure cases use typed
            // input so cancellation need not flush an unfinished capture tail.
            let initial_input = if matches!(ending, Ending::Graceful) {
                protocol::encode_audio(&vec![0.; 1536], 48000)
            } else {
                protocol::encode_message(
                    &json!({"id":"first-turn","label":"rtvi-ai","type":"send-text","data":{"content":"hello native agent","options":{"run_immediately":true,"audio_response":true}}}),
                )
            };
            client
                .sender()
                .send(Packet::Binary(initial_input.into()))
                .await
                .unwrap();
            let mut user = if matches!(ending, Ending::Graceful) {
                None
            } else {
                Some("hello native agent".to_string())
            };
            let mut bot = None;
            let mut audio = Vec::new();
            let mut tts_stopped = false;
            let mut bot_output_count = 0;
            while user.is_none()
                || bot.as_deref() != Some("Hello from Rust!")
                || audio.len() != 8
                || !tts_stopped
            {
                let packet = client.recv().await.expect("runner output");
                let Packet::Binary(bytes) = packet else {
                    panic!("browser expects binary protobuf")
                };
                match protocol::decode(&bytes).unwrap().body.unwrap() {
                    protocol::frame::Body::Audio(pcm) => {
                        assert_eq!(pcm.sample_rate, 24000);
                        assert_eq!(pcm.num_channels, 1);
                        audio.extend(pcm.audio);
                    }
                    protocol::frame::Body::Message(message) => {
                        let message: Value = serde_json::from_str(&message.data).unwrap();
                        assert_ne!(message["type"], "bot-started-speaking");
                        assert_ne!(message["type"], "bot-stopped-speaking");
                        if message["type"] == "bot-output" {
                            assert_eq!(message["data"]["spoken"], false);
                            assert!(message["data"].get("spoken_status").is_none());
                            bot_output_count += 1;
                        }
                        if let Some(line) = transcript(&message) {
                            match line.role {
                                "user" => user = Some(line.text),
                                "bot" => bot = Some(line.text),
                                _ => panic!(),
                            }
                        }
                        if message["type"] == "bot-tts-stopped" {
                            tts_stopped = true;
                        }
                    }
                    _ => {}
                }
            }
            assert_eq!(user.as_deref(), Some("hello native agent"));
            assert_eq!(audio, [0, 0, 1, 0, 255, 255, 2, 0]);
            assert!(bot_output_count >= 1);
            let terminal_input = match ending {
                Ending::Graceful => protocol::encode_message(
                    &json!({"id":"bye","label":"rtvi-ai","type":"disconnect-bot"}),
                ),
                Ending::InvalidProtobuf => vec![0xff],
                Ending::ProviderFailure => protocol::encode_message(
                    &json!({"id":"second-turn","label":"rtvi-ai","type":"send-text","data":{"content":"fail this request","options":{"run_immediately":true,"audio_response":true}}}),
                ),
            };
            client
                .sender()
                .send(Packet::Binary(terminal_input.into()))
                .await
                .unwrap();
            while client.recv().await.is_some() {}
            let _ = client.finish().await;
            let report = reports.recv().await.unwrap();
            assert!(
                report.generated_context_observed,
                "lost actual context snapshot"
            );
            assert_eq!(report.observed_pipeline_errors, Some(report.pipeline_errors));
            match ending {
                Ending::Graceful => {
                    assert!(report.successful, "session failed: {:?}", report.failure);
                    assert_eq!(report.pipeline_errors, 0);
                    assert!(report.failure.is_none());
                    assert_eq!(report.generated_context.messages.len(), 2);
                }
                Ending::InvalidProtobuf => {
                    assert!(!report.successful);
                    assert_eq!(report.pipeline_errors, 0);
                    let failure = report.failure.as_ref().expect("missing typed failure");
                    assert_eq!(failure.stage, SessionFailureStage::Runtime);
                    assert!(matches!(&*failure.error, RunnerError::Protobuf(_)));
                    assert_eq!(report.generated_context.messages.len(), 2);
                }
                Ending::ProviderFailure => {
                    assert!(!report.successful);
                    assert!(
                        report.pipeline_errors > 0,
                        "lost joined worker error count"
                    );
                    let worker = report
                        .worker_report
                        .as_ref()
                        .expect("lost joined worker report");
                    assert_eq!(worker.total_errors, report.pipeline_errors);
                    assert!(!worker.errors.is_empty(), "lost original processor errors");
                    let failure = report.failure.as_ref().expect("missing typed failure");
                    assert_eq!(failure.stage, SessionFailureStage::Runtime);
                    assert!(matches!(&*failure.error, RunnerError::Provider));
                    assert!(report.generated_context.messages.len() >= 2);
                }
            }
            assert_eq!(
                report.generated_context.messages[1].content,
                Some(pipecat_services::Content::Text("Hello from Rust!".into()))
            );
            assert_eq!(runner.active_sessions(), 0);
        }
        deepgram.await.unwrap().unwrap();
        openai.await.unwrap().unwrap();
        cartesia.await.unwrap().unwrap();
        runner.shutdown().await;
        let _ = stop.send(());
        server.await.unwrap().unwrap();
    })
    .await
    .unwrap();
}
async fn next_message(client: &mut WebSocketSession) -> Value {
    loop {
        let Packet::Binary(bytes) = client.recv().await.unwrap() else {
            panic!()
        };
        if let Some(protocol::frame::Body::Message(message)) =
            protocol::decode(&bytes).unwrap().body
        {
            return serde_json::from_str(&message.data).unwrap();
        }
    }
}

#[test]
fn original_source_wire_fixtures_cross_both_rust_codecs() {
    let bytes = include_bytes!("fixtures/client-ready.pb");
    let expected: Value =
        serde_json::from_slice(include_bytes!("fixtures/client-ready.json")).unwrap();
    let protocol::frame::Body::Message(message) = protocol::decode(bytes).unwrap().body.unwrap()
    else {
        panic!()
    };
    assert_eq!(
        serde_json::from_str::<Value>(&message.data).unwrap(),
        expected
    );
    let native = pipecat_runner::wire::decode(Packet::Binary(bytes.to_vec().into())).unwrap();
    let pipecat_core::FrameData::InputTransportMessage(value) = native.data else {
        panic!()
    };
    assert_eq!(value, expected);
    let bytes = include_bytes!("fixtures/audio-48000.pb");
    let protocol::frame::Body::Audio(audio) = protocol::decode(bytes).unwrap().body.unwrap() else {
        panic!()
    };
    assert_eq!(audio.sample_rate, 48000);
    assert_eq!(audio.audio, [0, 0, 1, 0, 255, 255, 2, 0]);
    let native = pipecat_runner::wire::decode(Packet::Binary(bytes.to_vec().into())).unwrap();
    let pipecat_core::FrameData::InputAudio(audio) = native.data else {
        panic!()
    };
    assert_eq!(audio.sample_rate, 48000);
}

#[tokio::test]
async fn connection_failure_reports_unobserved_context_and_error_count() {
    let runner = NativeRunner::new(
        RunnerConfig {
            connect_timeout: Duration::from_millis(30),
            ..Default::default()
        },
        ProviderConfig {
            deepgram_key: "synthetic-only".into(),
            openai_key: "synthetic-only".into(),
            cartesia_key: "synthetic-only".into(),
            deepgram_endpoint: "ws://127.0.0.1:9/stt".into(),
            openai_endpoint: "http://127.0.0.1:9/llm".into(),
            cartesia_endpoint: "ws://127.0.0.1:9/tts".into(),
            model: "synthetic".into(),
            voice_id: "synthetic".into(),
            system_prompt: "fixture".into(),
            input_rate: 16000,
            output_rate: 24000,
        },
    )
    .unwrap();
    let mut reports = runner.subscribe();
    let started = runner
        .start(&json!({"transport":"websocket"}))
        .await
        .unwrap();
    // Intentionally never connect: no pipeline worker or provider is created.
    let report = tokio::time::timeout(Duration::from_secs(3), reports.recv()).await;
    runner.shutdown().await;
    let report = report.unwrap().unwrap();
    assert_eq!(report.session_id, started["sessionId"].as_str().unwrap());
    assert!(!report.successful);
    assert!(!report.generated_context_observed);
    assert!(report.generated_context.messages.is_empty());
    assert_eq!(report.observed_pipeline_errors, None);
    let failure = report.failure.expect("missing connection failure");
    assert_eq!(failure.stage, SessionFailureStage::Connection);
    assert!(matches!(&*failure.error, RunnerError::Deadline));
}
