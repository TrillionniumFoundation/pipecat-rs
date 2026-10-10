#[path = "support/mod.rs"]
mod common;
use pipecat_browser_client::{client_ready, protocol, transcript, websocket_endpoint};
use pipecat_client_server::server::{serve_with_api, ServerConfig};
use pipecat_runner::{NativeRunner, Options, ProviderConfig, RunnerConfig};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot};

#[tokio::test]
async fn actual_http_start_browser_codec_and_synthetic_provider_pipeline() {
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
                common::deepgram(&dg, true).await?;
            }
            Ok::<_, pipecat_cli::AgentError>(())
        });
        let openai = tokio::spawn(async move {
            for _ in 0..2 {
                common::openai(&llm, false).await?;
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
            // A browser's actual 48 kHz capture is converted to configured 16 kHz.
            client
                .sender()
                .send(Packet::Binary(
                    protocol::encode_audio(&vec![0.; 1536], 48000).into(),
                ))
                .await
                .unwrap();
            let mut user = None;
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
            client
                .sender()
                .send(Packet::Binary(
                    protocol::encode_message(
                        &json!({"id":"bye","label":"rtvi-ai","type":"disconnect-bot"}),
                    )
                    .into(),
                ))
                .await
                .unwrap();
            while client.recv().await.is_some() {}
            let _ = client.finish().await;
            let report = reports.recv().await.unwrap();
            assert!(report.successful, "session failed");
            assert_eq!(report.pipeline_errors, 0);
            assert_eq!(report.generated_context.messages.len(), 2);
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
