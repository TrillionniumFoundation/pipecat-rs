#![cfg(feature = "webrtc")]
use pipecat_client_server::{server::BotApi, ProxyError, SessionMethod};
use pipecat_runner::{NativeRunner, ProviderConfig, RunnerConfig, WebRtcRunnerConfig};
use pipecat_webrtc::{SmallWebRtcConfig, SmallWebRtcConnection};
use serde_json::json;
use std::time::Duration;
use tokio::net::TcpListener;

fn providers(address: std::net::SocketAddr) -> ProviderConfig {
    ProviderConfig {
        deepgram_key: "synthetic-only".into(),
        openai_key: "synthetic-only".into(),
        cartesia_key: "synthetic-only".into(),
        deepgram_endpoint: format!("ws://{address}/stt"),
        openai_endpoint: format!("http://{address}/llm"),
        cartesia_endpoint: format!("ws://{address}/tts"),
        model: "synthetic".into(),
        voice_id: "synthetic".into(),
        system_prompt: "test".into(),
        input_rate: 16000,
        output_rate: 24000,
    }
}
#[tokio::test]
async fn unused_and_invalid_offers_expire_without_connecting_providers() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new_with_webrtc(
            RunnerConfig {
                max_sessions: 1,
                connect_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            providers(provider.local_addr().unwrap()),
            WebRtcRunnerConfig {
                localhost_only: true,
                ..Default::default()
            },
        )
        .unwrap();
        let mut reports = runner.subscribe();
        for invalid_offer in [false, true] {
            let first = runner.start(&json!({"transport":"webrtc"})).await.unwrap();
            let id = first["sessionId"].as_str().unwrap();
            assert!(matches!(
                runner.start(&json!({"transport":"websocket"})).await,
                Err(ProxyError::Busy)
            ));
            if invalid_offer {
                let request = json!({"sdp":"malformed","type":"offer"});
                assert!(matches!(
                    runner
                        .session(SessionMethod::Post, id, &["api", "offer"], &request)
                        .await,
                    Err(ProxyError::InvalidRequest)
                ));
                assert_eq!(runner.active_sessions(), 1);
            }
            let report = reports.recv().await.unwrap();
            assert_eq!(report.session_id, id);
            assert!(!report.successful);
            assert_eq!(runner.active_sessions(), 0);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(30), provider.accept())
                .await
                .is_err()
        );
        runner.shutdown().await;
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn shutdown_closes_an_answered_but_unconnected_peer() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new_with_webrtc(
            RunnerConfig::default(),
            providers(provider.local_addr().unwrap()),
            WebRtcRunnerConfig {
                localhost_only: true,
                ..Default::default()
            },
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let started = runner
            .start(&json!({"transport":"smallwebrtc"}))
            .await
            .unwrap();
        let id = started["sessionId"].as_str().unwrap();
        let peer = SmallWebRtcConnection::new(SmallWebRtcConfig {
            localhost_only: true,
            ..Default::default()
        })
        .await
        .unwrap();
        let offer = peer.create_offer().await.unwrap();
        let answer = runner
            .session(
                SessionMethod::Post,
                id,
                &["api", "offer"],
                &json!({"sdp":offer.sdp,"type":"offer"}),
            )
            .await
            .unwrap();
        assert_eq!(answer["type"], "answer");
        // No answer is installed, so ICE cannot reach a usable peer pair.
        runner.shutdown().await;
        assert_eq!(runner.active_sessions(), 0);
        assert_eq!(reports.recv().await.unwrap().session_id, id);
        assert!(matches!(
            runner.start(&json!({"transport":"webrtc"})).await,
            Err(ProxyError::Closed)
        ));
        assert!(matches!(
            runner
                .session(
                    SessionMethod::Patch,
                    id,
                    &["api", "offer"],
                    &json!({"pc_id":answer["pc_id"],"candidates":[]})
                )
                .await,
            Err(ProxyError::Path)
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), provider.accept())
                .await
                .is_err()
        );
        peer.close().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn default_runner_remains_websocket_only_and_webrtc_rejects_provider_overrides() {
    let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = provider.local_addr().unwrap();
    let runner = NativeRunner::new(RunnerConfig::default(), providers(address)).unwrap();
    assert!(matches!(
        runner.start(&json!({"transport":"webrtc"})).await,
        Err(ProxyError::InvalidRequest)
    ));
    assert!(matches!(
        runner.start(&json!({"transport":"smallwebrtc"})).await,
        Err(ProxyError::InvalidRequest)
    ));
    runner.shutdown().await;
    let runner = NativeRunner::new_with_webrtc(
        RunnerConfig::default(),
        providers(address),
        WebRtcRunnerConfig::default(),
    )
    .unwrap();
    for request in [
        json!({"transport":"webrtc","body":{"openai_endpoint":"http://attacker.example"}}),
        json!({"transport":"webrtc","createDailyRoom":true}),
        json!({"transport":"webrtc","enableDefaultIceServers":"true"}),
        json!({"transport":"webrtc","iceServers":[]}),
    ] {
        assert!(matches!(
            runner.start(&request).await,
            Err(ProxyError::InvalidRequest)
        ));
    }
    assert_eq!(runner.active_sessions(), 0);
    runner.shutdown().await;
}

#[tokio::test]
async fn two_owned_peers_reject_cross_session_patches_and_stop_independently() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new_with_webrtc(
            RunnerConfig {
                max_sessions: 2,
                ..Default::default()
            },
            providers(provider.local_addr().unwrap()),
            WebRtcRunnerConfig {
                localhost_only: true,
                ..Default::default()
            },
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let mut sessions = Vec::new();
        let mut peers = Vec::new();
        for _ in 0..2 {
            let response = runner.start(&json!({"transport":"webrtc"})).await.unwrap();
            let id = response["sessionId"].as_str().unwrap().to_owned();
            let peer = SmallWebRtcConnection::new(SmallWebRtcConfig {
                localhost_only: true,
                ..Default::default()
            })
            .await
            .unwrap();
            let offer = peer.create_offer().await.unwrap();
            let answer = runner
                .session(
                    SessionMethod::Post,
                    &id,
                    &["api", "offer"],
                    &json!({"sdp":offer.sdp,"type":"offer"}),
                )
                .await
                .unwrap();
            sessions.push((id, answer["pc_id"].as_str().unwrap().to_owned()));
            peers.push(peer);
        }
        for index in 0..2 {
            let (id, own_pc) = &sessions[index];
            assert_eq!(
                runner
                    .session(
                        SessionMethod::Patch,
                        id,
                        &["api", "offer"],
                        &json!({"pc_id":own_pc,"candidates":[]})
                    )
                    .await
                    .unwrap(),
                json!({"status":"success"})
            );
            assert!(matches!(
                runner
                    .session(
                        SessionMethod::Patch,
                        id,
                        &["api", "offer"],
                        &json!({"pc_id":sessions[1-index].1,"candidates":[]})
                    )
                    .await,
                Err(ProxyError::Path)
            ));
            assert!(matches!(
                runner
                    .session(
                        SessionMethod::Post,
                        id,
                        &["api", "offer"],
                        &json!({"sdp":"reoffer","type":"offer","pc_id":own_pc,"restart_pc":true})
                    )
                    .await,
                Err(ProxyError::InvalidRequest)
            ));
        }
        runner
            .session(SessionMethod::Post, &sessions[0].0, &["stop"], &json!({}))
            .await
            .unwrap();
        assert_eq!(reports.recv().await.unwrap().session_id, sessions[0].0);
        assert_eq!(runner.active_sessions(), 1);
        assert!(runner
            .session(
                SessionMethod::Patch,
                &sessions[1].0,
                &["api", "offer"],
                &json!({"pc_id":sessions[1].1,"candidates":[]})
            )
            .await
            .is_ok());
        runner.shutdown().await;
        assert_eq!(runner.active_sessions(), 0);
        for peer in peers {
            peer.close().await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(30), provider.accept())
                .await
                .is_err()
        );
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn continuous_rtp_cannot_extend_the_session_lifetime() {
    use pipecat_core::AudioRawFrame;
    use pipecat_transports::{SessionConfig, WebSocketSession};
    use std::sync::Arc;
    tokio::time::timeout(Duration::from_secs(8), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = provider.local_addr().unwrap();
        let provider_task = tokio::spawn(async move {
            let (stream, _) = provider.accept().await.unwrap();
            let mut socket = WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap();
            while socket.recv().await.is_some() {}
            let _ = socket.finish().await;
        });
        let runner = NativeRunner::new_with_webrtc(
            RunnerConfig {
                connect_timeout: Duration::from_secs(3),
                max_session_duration: Duration::from_millis(500),
                options: pipecat_runner::Options {
                    introduce: false,
                    ..Default::default()
                },
                ..Default::default()
            },
            providers(address),
            WebRtcRunnerConfig {
                localhost_only: true,
                ..Default::default()
            },
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let response = runner.start(&json!({"transport":"webrtc"})).await.unwrap();
        let id = response["sessionId"].as_str().unwrap();
        let peer = Arc::new(
            SmallWebRtcConnection::new(SmallWebRtcConfig {
                localhost_only: true,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let offer = peer.create_offer().await.unwrap();
        let answer = runner
            .session(
                SessionMethod::Post,
                id,
                &["api", "offer"],
                &json!({"sdp":offer.sdp,"type":"offer"}),
            )
            .await
            .unwrap();
        peer.accept_answer(
            pipecat_webrtc::RTCSessionDescription::answer(answer["sdp"].as_str().unwrap().into())
                .unwrap(),
        )
        .await
        .unwrap();
        peer.wait_data_channel(Duration::from_secs(3))
            .await
            .unwrap();
        peer.send_app_message(&pipecat_browser_client::client_ready(1))
            .await
            .unwrap();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let sending_peer = peer.clone();
        let sender = tokio::spawn(async move {
            let audio = AudioRawFrame::new(vec![0; 1920], 48000, 1).unwrap();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    result = sending_peer.send_audio(audio.clone()) => if result.is_err() {break;},
                }
            }
        });
        let report = reports.recv().await.unwrap();
        assert_eq!(report.session_id, id);
        assert!(!report.successful);
        assert_eq!(runner.active_sessions(), 0);
        let _ = stop.send(());
        sender.await.unwrap();
        peer.close().await.unwrap();
        runner.shutdown().await;
        provider_task.await.unwrap();
    })
    .await
    .unwrap();
}
