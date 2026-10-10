#![cfg(feature = "webrtc")]
#[path = "support/mod.rs"]
#[allow(dead_code)]
mod common;
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_browser_client::{client_ready, offer_endpoint, transcript, Transport};
use pipecat_client_server::server::{serve_with_api, ServerConfig};
use pipecat_core::AudioRawFrame;
use pipecat_runner::{NativeRunner, Options, ProviderConfig, RunnerConfig, WebRtcRunnerConfig};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_webrtc::{
    AudioCodec, RTCSessionDescription, SmallWebRtcConfig, SmallWebRtcConnection, TransportEvent,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::oneshot};

fn pcm(rate: u32, samples: usize) -> Vec<u8> {
    (0..samples)
        .flat_map(|i| {
            ((i as f64 * std::f64::consts::TAU * 440.0 / rate as f64)
                .sin()
                .mul_add(8000.0, 0.0) as i16)
                .to_le_bytes()
        })
        .collect()
}
async fn stt(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut socket =
        WebSocketSession::accept_with_request_policy(stream, SessionConfig::default(), |request| {
            let url = url::Url::parse(&format!("http://localhost{}", request.uri())).unwrap();
            url.query_pairs()
                .any(|(key, value)| key == "sample_rate" && value == "16000")
        })
        .await
        .unwrap();
    let mut received = false;
    let mut drained_tail = false;
    while let Some(packet) = socket.recv().await {
        match packet {
            Packet::Binary(audio) => {
                if received {
                    assert!(!drained_tail, "duplicate terminal microphone tail");
                    assert_eq!(
                        audio.len(),
                        68,
                        "graceful end must complete all 320 normalized samples"
                    );
                    drained_tail = true;
                    continue;
                }
                assert_eq!(
                    audio.len(),
                    572,
                    "48 kHz Opus capture must be resampled to 16 kHz"
                );
                assert!(audio
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .any(|s| i16::from_le_bytes([s[0], s[1]]).abs() > 100));
                received = true;
                socket.sender().send(Packet::Text(json!({"type":"Results","is_final":true,"speech_final":true,"start":0.0,"duration":0.02,"channel":{"alternatives":[{"transcript":"hello native agent","confidence":1.0,"words":[]}]}}).to_string())).await.unwrap();
            }
            Packet::Text(text) => {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["type"] == "CloseStream" {
                    break;
                }
                if value["type"] == "Finalize" {
                    socket.sender().send(Packet::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"speech_final":true,"channel":{"alternatives":[{"transcript":""}]}}).to_string())).await.unwrap();
                }
            }
        }
    }
    assert!(received && drained_tail);
    let _ = socket.finish().await;
}
async fn tts(listener: TcpListener) {
    let (stream, _) = listener.accept().await.unwrap();
    let mut socket = WebSocketSession::accept(stream, SessionConfig::default())
        .await
        .unwrap();
    let mut text = String::new();
    while let Some(Packet::Text(message)) = socket.recv().await {
        let value: Value = serde_json::from_str(&message).unwrap();
        text.push_str(value["transcript"].as_str().unwrap());
        if value["continue"] == false {
            assert_eq!(text, "Hello from Rust!");
            assert_eq!(value["output_format"]["sample_rate"], 24000);
            let id = &value["context_id"];
            let bytes = pcm(24000, 1440); // 60 ms, split across codec boundaries.
            for chunk in [&bytes[..222], &bytes[222..940], &bytes[940..]] {
                socket
                    .sender()
                    .send(Packet::Text(
                        json!({"type":"chunk","context_id":id,"data":STANDARD.encode(chunk)})
                            .to_string(),
                    ))
                    .await
                    .unwrap();
            }
            socket
                .sender()
                .send(Packet::Text(
                    json!({"type":"done","context_id":id}).to_string(),
                ))
                .await
                .unwrap();
            let _ = socket.finish().await;
            return;
        }
    }
    panic!("TTS ended before text completed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_smallwebrtc_offer_opus_rtvi_and_synthetic_voice_pipeline() {
    tokio::time::timeout(Duration::from_secs(35), async {
        let dg = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let llm = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cartesia = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", host.local_addr().unwrap());
        let providers = ProviderConfig {
            deepgram_key: "synthetic-only".into(),
            openai_key: "synthetic-only".into(),
            cartesia_key: "synthetic-only".into(),
            deepgram_endpoint: format!("ws://{}/v1/listen", dg.local_addr().unwrap()),
            openai_endpoint: format!("http://{}/v1/chat/completions", llm.local_addr().unwrap()),
            cartesia_endpoint: format!("ws://{}/tts/websocket", cartesia.local_addr().unwrap()),
            model: "synthetic".into(),
            voice_id: "synthetic".into(),
            system_prompt: "Be brief".into(),
            input_rate: 16000,
            output_rate: 24000,
        };
        let runner = Arc::new(
            NativeRunner::new_with_webrtc(
                RunnerConfig {
                    allowed_origins: vec![origin.clone()],
                    max_sessions: 2,
                    options: Options {
                        introduce: false,
                        turn_timeout: Duration::from_millis(20),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                providers,
                WebRtcRunnerConfig {
                    localhost_only: true,
                    ..Default::default()
                },
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
        let deepgram = tokio::spawn(stt(dg));
        let openai = tokio::spawn(async move {
            common::openai(&llm, false).await.unwrap();
        });
        let synthesis = tokio::spawn(tts(cartesia));
        let http = reqwest::Client::new();
        let start = url::Url::parse(&format!("{origin}/api/start")).unwrap();
        // Use the browser crate's exact source-compatible start body and URL builder.
        let body = Transport::SmallWebRtc.start_body();
        let response: Value = http
            .post(start.clone())
            .header("Origin", &origin)
            .json(&body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response["iceConfig"]["iceServers"], json!([]));
        assert_eq!(response["contextMode"], "generated-text");
        assert_eq!(response["playbackAcknowledgements"], false);
        let id = response["sessionId"].as_str().unwrap();
        assert_eq!(id.len(), 64);
        let offer_url = offer_endpoint(&start, id).unwrap();
        let other: Value = http
            .post(start.clone())
            .header("Origin", &origin)
            .json(&body)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let full = http
            .post(start.clone())
            .header("Origin", &origin)
            .json(&body)
            .send()
            .await
            .unwrap();
        assert_eq!(full.status(), 429);
        let peer = SmallWebRtcConnection::new(SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: Some(AudioCodec::Opus),
            data_channel_label: "chat".into(),
            ..Default::default()
        })
        .await
        .unwrap();
        let offer = peer.create_offer().await.unwrap();
        let offer_body = json!({"sdp":offer.sdp,"type":"offer"});
        for denied in [None, Some("https://untrusted.example")] {
            let mut request = http.post(offer_url.clone()).json(&offer_body);
            if let Some(origin) = denied {
                request = request.header("Origin", origin);
            }
            assert_eq!(request.send().await.unwrap().status(), 403);
        }
        let wrong_capability = offer_endpoint(&start, "incorrect-capability").unwrap();
        assert_eq!(
            http.post(wrong_capability)
                .header("Origin", &origin)
                .json(&offer_body)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        let answer: Value = http
            .post(offer_url.clone())
            .header("Origin", &origin)
            .json(&offer_body)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(answer["type"], "answer");
        assert!(answer["sdp"].as_str().unwrap().contains("opus/48000/2"));
        assert_ne!(answer["pc_id"], response["sessionId"]);
        peer.accept_answer(
            RTCSessionDescription::answer(answer["sdp"].as_str().unwrap().into()).unwrap(),
        )
        .await
        .unwrap();
        peer.wait_connected(Duration::from_secs(5)).await.unwrap();
        peer.wait_data_channel(Duration::from_secs(5))
            .await
            .unwrap();
        let patch = json!({"pc_id":answer["pc_id"],"candidates":[]});
        assert_eq!(
            http.patch(offer_url.clone())
                .header("Origin", &origin)
                .json(&patch)
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        let wrong_peer = json!({"pc_id":"another-peer","candidates":[]});
        assert_eq!(
            http.patch(offer_url.clone())
                .header("Origin", &origin)
                .json(&wrong_peer)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        let other_offer = offer_endpoint(&start, other["sessionId"].as_str().unwrap()).unwrap();
        assert_eq!(
            http.patch(other_offer)
                .header("Origin", &origin)
                .json(&patch)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        assert_eq!(
            http.post(offer_url.clone())
                .header("Origin", &origin)
                .json(&offer_body)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        let other_stop = format!(
            "{origin}/api/sessions/{}/stop",
            other["sessionId"].as_str().unwrap()
        );
        assert_eq!(
            http.post(other_stop)
                .header("Origin", &origin)
                .json(&json!({}))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            reports.recv().await.unwrap().session_id,
            other["sessionId"].as_str().unwrap()
        );
        peer.send_keepalive().await.unwrap();
        peer.send_app_message(&client_ready(29)).await.unwrap();
        loop {
            match peer.recv_event().await.unwrap() {
                TransportEvent::AppMessage(value) if value["type"] == "bot-ready" => {
                    assert_eq!(value["id"], "29");
                    assert_eq!(value["data"]["version"], "2.2.0");
                    break;
                }
                TransportEvent::Error(error) => panic!("{error}"),
                _ => {}
            }
        }
        peer.send_keepalive().await.unwrap();
        peer.send_audio(AudioRawFrame::new(pcm(48000, 960), 48000, 1).unwrap())
            .await
            .unwrap();
        let mut user = None;
        let mut bot = None;
        let mut audio_packets = 0;
        let mut stopped = false;
        while user.is_none()
            || bot.as_deref() != Some("Hello from Rust!")
            || audio_packets < 3
            || !stopped
        {
            match peer
                .recv_event()
                .await
                .expect("runner connection remains open")
            {
                TransportEvent::AppMessage(value) => {
                    assert_ne!(value["type"], "bot-started-speaking");
                    assert_ne!(value["type"], "bot-stopped-speaking");
                    if value["type"] == "bot-output" {
                        assert_eq!(value["data"]["spoken"], false);
                    }
                    if value["type"] == "bot-tts-stopped" {
                        stopped = true;
                    }
                    if let Some(line) = transcript(&value) {
                        match line.role {
                            "user" => user = Some(line.text),
                            "bot" => bot = Some(line.text),
                            _ => {}
                        }
                    }
                }
                TransportEvent::Audio(audio) => {
                    assert_eq!(audio.codec, AudioCodec::Opus);
                    assert_eq!(audio.frame.sample_rate, 48000);
                    assert_eq!(audio.frame.num_frames(), 960);
                    assert!(audio
                        .frame
                        .audio
                        .as_chunks::<2>()
                        .0
                        .iter()
                        .any(|s| i16::from_le_bytes([s[0], s[1]]).abs() > 100));
                    audio_packets += 1;
                }
                TransportEvent::Error(error) => panic!("{error}"),
                _ => {}
            }
        }
        assert_eq!(user.as_deref(), Some("hello native agent"));
        assert_eq!(audio_packets, 3);
        peer.send_app_message(&json!({"label":"rtvi-ai","id":"bye","type":"disconnect-bot"}))
            .await
            .unwrap();
        let report = reports.recv().await.unwrap();
        assert_eq!(report.session_id, id);
        assert!(report.successful);
        assert_eq!(report.pipeline_errors, 0);
        assert_eq!(report.generated_context.messages.len(), 2);
        assert_eq!(runner.active_sessions(), 0);
        assert_eq!(
            http.patch(offer_url)
                .header("Origin", &origin)
                .json(&patch)
                .send()
                .await
                .unwrap()
                .status(),
            400
        );
        peer.close().await.unwrap();
        deepgram.await.unwrap();
        openai.await.unwrap();
        synthesis.await.unwrap();
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        runner.shutdown().await;
    })
    .await
    .unwrap();
}
