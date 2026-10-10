//! Offline lifecycle coverage for sessions owning native Whisper workers.
#![cfg(feature = "whisper")]

use pipecat_browser_client::{client_ready, protocol};
use pipecat_client_server::{server::BotApi, SessionMethod};
use pipecat_runner::{
    LocalWhisperConfig, NativeRunner, Options, ProviderConfig, RunnerConfig, SttProvider,
    WhisperSegmentation,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_whisper::DecodingStrategy;
use serde_json::{json, Value};
use std::{net::SocketAddr, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

fn config() -> RunnerConfig {
    let path = std::env::var_os("PIPECAT_WHISPER_MODEL")
        .expect("set PIPECAT_WHISPER_MODEL to an explicit local GGML model");
    let mut whisper = LocalWhisperConfig::new(path);
    whisper.model.inference_threads = 2;
    whisper.model.queue_capacity = 1;
    whisper.settings.strategy = DecodingStrategy::BeamSearch { beam_size: 8 };
    whisper.segmentation = WhisperSegmentation::FixedWindow(Duration::from_secs(30));
    RunnerConfig {
        stt: SttProvider::Whisper(whisper),
        max_sessions: 1,
        allow_missing_origin: true,
        options: Options {
            introduce: false,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn providers(address: SocketAddr) -> ProviderConfig {
    ProviderConfig {
        deepgram_key: String::new(),
        deepgram_endpoint: "ws://127.0.0.1:0/unused-deepgram".into(),
        openai_key: "synthetic-only".into(),
        cartesia_key: "synthetic-only".into(),
        openai_endpoint: format!("http://{address}/llm"),
        cartesia_endpoint: format!("ws://{address}/tts"),
        model: "synthetic-model".into(),
        voice_id: "synthetic-voice".into(),
        system_prompt: "Be brief".into(),
        input_rate: 16_000,
        output_rate: 24_000,
    }
}

fn address(response: &Value) -> SocketAddr {
    let url = url::Url::parse(response["wsUrl"].as_str().unwrap()).unwrap();
    format!("{}:{}", url.host_str().unwrap(), url.port().unwrap())
        .parse()
        .unwrap()
}

fn long_speech_window() -> Vec<f32> {
    let path = std::env::var_os("PIPECAT_WHISPER_FIXTURE")
        .expect("set PIPECAT_WHISPER_FIXTURE to the upstream JFK PCM16 WAV");
    let bytes = std::fs::read(path).unwrap();
    let wav = pipecat_audio::read_pcm16_wav(&bytes).unwrap();
    assert_eq!(wav.format.sample_rate(), 16_000);
    assert_eq!(wav.format.channels(), 1);
    let speech: Vec<f32> = wav
        .pcm
        .as_chunks::<2>()
        .0
        .iter()
        .map(|sample| i16::from_le_bytes([sample[0], sample[1]]) as f32 / 32768.)
        .collect();
    assert!(!speech.is_empty());
    let mut window: Vec<f32> = speech.into_iter().cycle().take(16_000 * 30).collect();
    window.resize(16_000 * 30 + 1_600, 0.);
    window
}

async fn ready_client(response: &Value) -> WebSocketSession {
    let mut client = WebSocketSession::connect(
        response["wsUrl"].as_str().unwrap(),
        SessionConfig::default(),
    )
    .await
    .unwrap();
    client
        .sender()
        .send(Packet::Binary(
            protocol::encode_message(&client_ready(1)).into(),
        ))
        .await
        .unwrap();
    loop {
        let Packet::Binary(bytes) = client.recv().await.expect("bot-ready") else {
            panic!("browser output must use binary protobuf")
        };
        if let Some(protocol::frame::Body::Message(message)) =
            protocol::decode(&bytes).unwrap().body
        {
            let message: Value = serde_json::from_str(&message.data).unwrap();
            if message["type"] == "bot-ready" {
                return client;
            }
        }
    }
}

#[tokio::test]
async fn invalid_model_setup_releases_the_session_and_reuses_admission() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut local = LocalWhisperConfig::new(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
        local.segmentation = WhisperSegmentation::FixedWindow(Duration::from_secs(1));
        let runner = NativeRunner::new(
            RunnerConfig {
                stt: SttProvider::Whisper(local),
                max_sessions: 1,
                allow_missing_origin: true,
                ..Default::default()
            },
            providers(provider.local_addr().unwrap()),
        )
        .unwrap();
        let mut reports = runner.subscribe();
        for _ in 0..2 {
            let response = runner
                .start(&json!({"transport":"websocket"}))
                .await
                .unwrap();
            let mut client = WebSocketSession::connect(
                response["wsUrl"].as_str().unwrap(),
                SessionConfig::default(),
            )
            .await
            .unwrap();
            while client.recv().await.is_some() {}
            let _ = client.finish().await;
            let report = reports.recv().await.unwrap();
            assert!(!report.successful);
            assert_eq!(report.session_id, response["sessionId"].as_str().unwrap());
            assert_eq!(runner.active_sessions(), 0);
            assert_eq!(runner.active_local_stt_workers(), 0);
            assert!(TcpStream::connect(address(&response)).await.is_err());
        }
        runner.shutdown().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), provider.accept())
                .await
                .is_err()
        );
    })
    .await
    .expect("failed native initialization did not roll back");
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL; two independent real native model owners"]
async fn stopping_one_native_session_joins_only_its_owner_and_reuses_the_slot() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new(
            RunnerConfig {
                max_sessions: 2,
                ..config()
            },
            providers(provider.local_addr().unwrap()),
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let first = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        let mut first_client = ready_client(&first).await;
        let second = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        let mut second_client = ready_client(&second).await;
        assert_eq!(runner.active_local_stt_workers(), 2);
        runner
            .session(
                SessionMethod::Post,
                first["sessionId"].as_str().unwrap(),
                &["stop"],
                &json!({}),
            )
            .await
            .unwrap();
        while first_client.recv().await.is_some() {}
        let _ = first_client.finish().await;
        let report = reports.recv().await.unwrap();
        assert!(report.successful);
        assert_eq!(report.session_id, first["sessionId"].as_str().unwrap());
        assert_eq!(runner.active_sessions(), 1);
        assert_eq!(runner.active_local_stt_workers(), 1);
        // A new start must be admitted while the second model remains owned.
        let third = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        assert_eq!(runner.active_sessions(), 2);
        assert_eq!(runner.active_local_stt_workers(), 1);
        runner.shutdown().await;
        assert_eq!(runner.active_sessions(), 0);
        assert_eq!(runner.active_local_stt_workers(), 0);
        while second_client.recv().await.is_some() {}
        let _ = second_client.finish().await;
        assert!(TcpStream::connect(address(&third)).await.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), provider.accept())
                .await
                .is_err()
        );
    })
    .await
    .expect("native session ownership was not isolated");
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL; rollback after loading an actual native model"]
async fn later_initialization_failure_joins_the_native_model_before_releasing_admission() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        for empty_llm_key in [true, false] {
            let mut hosted = providers(provider.local_addr().unwrap());
            if empty_llm_key {
                hosted.openai_key.clear();
            } else {
                hosted.cartesia_key.clear();
            }
            let runner = NativeRunner::new(config(), hosted).unwrap();
            let mut reports = runner.subscribe();
            let response = runner
                .start(&json!({"transport":"websocket"}))
                .await
                .unwrap();
            let mut client = WebSocketSession::connect(
                response["wsUrl"].as_str().unwrap(),
                SessionConfig::default(),
            )
            .await
            .unwrap();
            while client.recv().await.is_some() {}
            let _ = client.finish().await;
            let report = reports.recv().await.unwrap();
            assert!(!report.successful);
            assert_eq!(runner.active_local_stt_workers(), 0);
            assert_eq!(runner.active_sessions(), 0);
            assert!(TcpStream::connect(address(&response)).await.is_err());
            let _reused = runner
                .start(&json!({"transport":"websocket"}))
                .await
                .unwrap();
            runner.shutdown().await;
            assert_eq!(runner.active_local_stt_workers(), 0);
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), provider.accept())
                .await
                .is_err()
        );
    })
    .await
    .expect("later setup failure retained a native model");
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL; authorized client never sends client-ready"]
async fn authorized_but_unready_client_expires_and_joins_its_native_owner() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new(
            RunnerConfig {
                connect_timeout: Duration::from_millis(200),
                ..config()
            },
            providers(provider.local_addr().unwrap()),
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let response = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        let mut client = WebSocketSession::connect(
            response["wsUrl"].as_str().unwrap(),
            SessionConfig::default(),
        )
        .await
        .unwrap();
        while runner.active_local_stt_workers() == 0 {
            tokio::task::yield_now().await;
        }
        assert_eq!(runner.active_local_stt_workers(), 1);
        while client.recv().await.is_some() {}
        let _ = client.finish().await;
        let report = reports.recv().await.unwrap();
        assert!(!report.successful);
        assert_eq!(runner.active_local_stt_workers(), 0);
        assert_eq!(runner.active_sessions(), 0);
        assert!(TcpStream::connect(address(&response)).await.is_err());
        let _reused = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        runner.shutdown().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), provider.accept())
                .await
                .is_err()
        );
    })
    .await
    .expect("unready authorized client retained a native model");
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL; no network model downloads"]
async fn rejected_browser_and_unused_session_expiry_never_start_native_workers() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new(
            RunnerConfig {
                allow_missing_origin: false,
                allowed_origins: vec!["http://127.0.0.1:7860".into()],
                connect_timeout: Duration::from_millis(250),
                ..config()
            },
            providers(provider.local_addr().unwrap()),
        )
        .unwrap();
        let mut reports = runner.subscribe();
        let first = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        assert_eq!(runner.active_local_stt_workers(), 0);
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let mut request = first["wsUrl"]
            .as_str()
            .unwrap()
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("Origin", "https://untrusted.example".parse().unwrap());
        assert!(
            WebSocketSession::connect_with_request(request, SessionConfig::default())
                .await
                .is_err()
        );
        assert_eq!(runner.active_local_stt_workers(), 0);
        let report = reports.recv().await.unwrap();
        assert!(!report.successful);
        assert_eq!(report.session_id, first["sessionId"].as_str().unwrap());
        assert_eq!(runner.active_sessions(), 0);
        assert_eq!(runner.active_local_stt_workers(), 0);
        assert!(TcpStream::connect(address(&first)).await.is_err());
        let second = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        assert_ne!(first["sessionId"], second["sessionId"]);
        runner.shutdown().await;
        assert_eq!(runner.active_sessions(), 0);
        assert_eq!(runner.active_local_stt_workers(), 0);
        assert!(TcpStream::connect(address(&second)).await.is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), provider.accept())
                .await
                .is_err()
        );
    })
    .await
    .expect("unused Whisper session expiry timed out");
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL and PIPECAT_WHISPER_FIXTURE; real native inference"]
async fn stop_cancels_native_audio_work_and_joins_the_owned_worker() {
    native_audio_lifecycle(false).await;
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL and PIPECAT_WHISPER_FIXTURE; real native inference"]
async fn idle_expiry_cancels_native_audio_work_and_joins_the_owned_worker() {
    native_audio_lifecycle(true).await;
}

#[tokio::test]
#[ignore = "requires PIPECAT_WHISPER_MODEL and PIPECAT_WHISPER_FIXTURE; real native inference"]
async fn vad_pre_roll_does_not_replay_a_previously_submitted_utterance() {
    use pipecat_core::{
        AudioRawFrame, Direction, Frame, FrameData, Pipeline, PipelineConfig, PipelineWorker,
        TextFrame, VadSpeakingFrame,
    };
    use pipecat_whisper::{WhisperProcessorOptions, WhisperService, WhisperSttProcessor};
    use std::sync::Arc;

    async fn barrier(worker: &mut PipelineWorker, label: &str) {
        worker
            .sender()
            .send(
                Frame::new(FrameData::Text(TextFrame::new(label))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            let frame = worker.recv().await.expect("audio ordering barrier");
            assert!(frame.frame.data.as_error_frame().is_none(), "{frame:?}");
            if matches!(&frame.frame.data, FrameData::Text(text) if text.text == label) {
                break;
            }
        }
    }

    tokio::time::timeout(Duration::from_secs(90), async {
        let model = std::env::var_os("PIPECAT_WHISPER_MODEL")
            .expect("set PIPECAT_WHISPER_MODEL to an explicit local GGML model");
        let fixture = std::env::var_os("PIPECAT_WHISPER_FIXTURE")
            .expect("set PIPECAT_WHISPER_FIXTURE to the upstream JFK PCM16 WAV");
        let bytes = std::fs::read(fixture).unwrap();
        let wav = pipecat_audio::read_pcm16_wav(&bytes).unwrap();
        assert_eq!(wav.format.sample_rate(), 16_000);
        assert_eq!(wav.format.channels(), 1);
        let mut local = LocalWhisperConfig::new(model);
        assert_eq!(local.pre_roll_duration, Duration::from_secs(3));
        local.model.inference_threads = 2;
        local.settings.strategy = DecodingStrategy::Greedy { best_of: 1 };
        let processor = Arc::new(
            WhisperSttProcessor::new(
                WhisperService::load(local.model).await.unwrap(),
                WhisperProcessorOptions {
                    settings: local.settings,
                    pre_roll_duration: local.pre_roll_duration,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let mut worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let sender = worker.sender();
        for turn in 0..2 {
            if turn == 1 {
                sender
                    .send(
                        Frame::new(FrameData::InputAudio(
                            AudioRawFrame::new(vec![0; 16_000 * 2 * 2], 16_000, 1).unwrap(),
                        )),
                        Direction::Downstream,
                    )
                    .await
                    .unwrap();
                barrier(&mut worker, "quiet audio consumed").await;
            }
            sender
                .send(
                    Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                        duration_secs: 0.2,
                        timestamp: 0.0,
                    })),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            for pcm in wav.pcm.chunks(32_000) {
                sender
                    .send(
                        Frame::new(FrameData::InputAudio(
                            AudioRawFrame::new(pcm.to_vec(), 16_000, 1).unwrap(),
                        )),
                        Direction::Downstream,
                    )
                    .await
                    .unwrap();
            }
            // VAD stop is a priority frame; observe a normal-lane barrier so it
            // cannot overtake audio that belongs to this utterance.
            barrier(&mut worker, "speech audio consumed").await;
            sender
                .send(
                    Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                        duration_secs: 0.2,
                        timestamp: 0.0,
                    })),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            let transcript = loop {
                let frame = worker.recv().await.expect("real native transcript");
                assert!(frame.frame.data.as_error_frame().is_none(), "{frame:?}");
                if let FrameData::Transcription(transcript) = frame.frame.data {
                    break transcript;
                }
            };
            let text = transcript.text.to_ascii_lowercase();
            assert!(text.contains("ask not what your country can do for you"), "{text}");
            assert!(text.contains("ask what you can do for your country"), "{text}");
            let duration = transcript.result.unwrap()["audio_duration_seconds"]
                .as_f64()
                .unwrap();
            let expected = wav.pcm.len() as f64 / 32_000. + 0.5
                + if turn == 1 { 2.0 } else { 0.0 };
            assert!(
                (duration - expected).abs() < 1. / 16_000.,
                "utterance {turn}: decoded {duration}s, expected {expected}s; pre-roll replayed old audio"
            );
        }
        worker.end().await.unwrap();
        let (remaining, report) = worker.collect().await.unwrap();
        assert_eq!(report.total_errors, 0, "{report:?}");
        assert!(!remaining.iter().any(|frame| matches!(frame.frame.data, FrameData::Transcription(_))));
        assert!(!processor.is_running());
    })
    .await
    .expect("native Whisper pre-roll isolation timed out");
}

async fn native_audio_lifecycle(expire: bool) {
    tokio::time::timeout(Duration::from_secs(45), async {
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_address = provider.local_addr().unwrap();
        let (stop_provider, mut provider_stopped) = oneshot::channel();
        // Keep any fast-completing inference's downstream request pending, so
        // the only session exit is the stop/deadline under test.
        let provider_task = tokio::spawn(async move {
            let mut connections = Vec::new();
            loop {
                tokio::select! {
                    _ = &mut provider_stopped => break,
                    accepted = provider.accept() => connections.push(accepted.unwrap().0),
                }
            }
        });
        let mut config = config();
        if expire {
            config.options.idle_timeout = Duration::from_millis(700);
        }
        let runner = NativeRunner::new(config, providers(provider_address)).unwrap();
        let mut reports = runner.subscribe();
        let response = runner
            .start(&json!({"transport":"websocket"}))
            .await
            .unwrap();
        assert_eq!(runner.active_local_stt_workers(), 0);
        let mut client = ready_client(&response).await;
        assert_eq!(runner.active_local_stt_workers(), 1);
        for samples in long_speech_window().chunks(16_000) {
            let bytes = protocol::encode_audio(samples, 16_000);
            assert!(bytes.len() <= 64 * 1024);
            client
                .sender()
                .send(Packet::Binary(bytes.into()))
                .await
                .unwrap();
        }
        let input_sent = tokio::time::Instant::now();
        // Submission is observed rather than inferred from elapsed wall time.
        tokio::time::timeout(Duration::from_secs(10), async {
            while runner.local_stt_fixed_windows_submitted() == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("browser audio never reached native Whisper submission");
        assert_eq!(runner.local_stt_fixed_windows_submitted(), 1);
        assert_eq!(runner.active_local_stt_workers(), 1);
        if !expire {
            runner
                .session(
                    SessionMethod::Post,
                    response["sessionId"].as_str().unwrap(),
                    &["stop"],
                    &json!({}),
                )
                .await
                .unwrap();
        }
        while client.recv().await.is_some() {}
        let _ = client.finish().await;
        let report = reports.recv().await.unwrap();
        if expire {
            assert!(
                input_sent.elapsed() >= Duration::from_millis(600),
                "session failed before its 700 ms idle deadline: {report:?}"
            );
        }
        assert_eq!(report.successful, !expire, "session report: {report:?}");
        assert_eq!(report.pipeline_errors, 0);
        assert_eq!(runner.active_sessions(), 0);
        assert_eq!(runner.active_local_stt_workers(), 0);
        assert!(TcpStream::connect(address(&response)).await.is_err());
        runner.shutdown().await;
        assert_eq!(runner.active_local_stt_workers(), 0);
        let _ = stop_provider.send(());
        provider_task.await.unwrap();
    })
    .await
    .expect("native Whisper lifecycle cleanup timed out");
}
