mod support;
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_core::AudioRawFrame;
use pipecat_gradium::*;
use serde_json::json;
use std::time::Duration;
use support::*;
#[tokio::test]
async fn exact_setup_audio_flush_trailing_text_accounting_and_eos() {
    let mut peer = support::peer().await;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"setup","model_name":"default","input_format":"pcm_16000","json_config":{"language":"en","delay_in_frames":12}})
    );
    sender.try_send_audio(pcm(2560)).unwrap();
    let message = wire(&mut peer).await;
    assert_eq!(
        message,
        json!({"type":"audio","audio":STANDARD.encode(vec![0;2560])})
    );
    wait(|| sender.statistics().sent_wire_bytes == 2560).await;
    assert_eq!(
        sender.statistics(),
        GradiumStatistics {
            accepted_input_bytes: 2560,
            sent_wire_bytes: 2560,
            confirmed_wire_bytes: 0
        }
    );
    sender.try_flush().unwrap();
    assert_eq!(sender.try_flush(), Err(GradiumError::Busy));
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"flush","flush_id":"1"})
    );
    emit(&peer, json!({"type":"text","text":"hello"})).await;
    assert!(
        matches!(next(&mut session).await,GradiumSttEvent::Transcript(t) if t.text=="hello"&&!t.is_final)
    );
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    assert_eq!(
        next(&mut session).await,
        GradiumSttEvent::Flushed {
            flush_id: 1,
            confirmed_wire_bytes: 2560
        }
    );
    emit(&peer, json!({"type":"text","text":"world"})).await;
    assert!(
        matches!(next(&mut session).await,GradiumSttEvent::Transcript(t) if t.text=="hello world"&&!t.is_final)
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), session.next())
            .await
            .is_err()
    );
    assert!(
        matches!(next(&mut session).await,GradiumSttEvent::Transcript(t) if t.text=="hello world"&&t.is_final&&(t.audio_seconds-0.08).abs()<1e-9)
    );
    assert_eq!(sender.statistics().confirmed_wire_bytes, 2560);
    assert_eq!(
        finish(&mut session).await,
        vec![GradiumSttEvent::EndOfStream {
            confirmed_wire_bytes: 2560
        }]
    );
    assert_eq!(wire(&mut peer).await, json!({"type":"end_of_stream"}));
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn arbitrary_odd_byte_splits_resample_exactly_and_preserve_tail() {
    for rate in [4000, 8000, 11025, 16000, 22050, 24000, 44100, 48000] {
        let mut peer = support::peer().await;
        peer.config.sample_rate = rate;
        let mut session = GradiumSttClient::new(peer.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let sender = session.sender();
        let setup = wire(&mut peer).await;
        let wire_rate = peer.config.wire_sample_rate();
        assert_eq!(setup["input_format"], format!("pcm_{wire_rate}"));
        let input: Vec<u8> = (0..rate / 5)
            .flat_map(|i| (((i as f64 / 17.0).sin() * 15000.0) as i16).to_le_bytes())
            .collect();
        for chunk in input.chunks(777) {
            sender
                .try_send_audio(AudioRawFrame {
                    audio: chunk.to_vec(),
                    sample_rate: rate,
                    num_channels: 1,
                    context_id: None,
                })
                .unwrap()
        }
        let events = finish(&mut session).await;
        assert!(matches!(
            events.last(),
            Some(GradiumSttEvent::EndOfStream { .. })
        ));
        let mut output = Vec::new();
        let chunk_bytes = (wire_rate * 80 / 1000 * 2) as usize;
        let mut lengths = Vec::new();
        loop {
            let msg = wire(&mut peer).await;
            if msg["type"] == "end_of_stream" {
                break;
            }
            let bytes = STANDARD.decode(msg["audio"].as_str().unwrap()).unwrap();
            lengths.push(bytes.len());
            output.extend(bytes)
        }
        assert_eq!(
            output,
            pipecat_audio::resample_pcm16(&input, rate, wire_rate, 1).unwrap()
        );
        assert!(lengths
            .iter()
            .take(lengths.len() - 1)
            .all(|n| *n == chunk_bytes));
        assert_eq!(output.len() % 2, 0);
        bounded(peer.task).await.unwrap();
    }
}
#[tokio::test]
async fn four_phase_detection_exact_threshold_closest_horizon_and_cooldown() {
    let mut peer = support::peer().await;
    peer.config.enable_turn_detection = true;
    peer.config.settings.turn.post_flush_cooldown_frames = 2;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    wire(&mut peer).await;
    emit(&peer, step(0.0)).await;
    emit(&peer, step(0.5)).await;
    emit(&peer,json!({"type":"step","vad":[{"horizon_s":1.0,"inactivity_prob":1.0},{"horizon_s":2.9,"inactivity_prob":0.1}]})).await;
    assert_eq!(next(&mut session).await, GradiumSttEvent::TurnStarted);
    emit(&peer, step(0.5)).await;
    assert_eq!(next(&mut session).await, GradiumSttEvent::TurnStopped);
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"flush","flush_id":"1"})
    );
    emit(&peer, step(0.0)).await;
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    assert!(matches!(
        next(&mut session).await,
        GradiumSttEvent::Flushed { .. }
    ));
    tokio::time::sleep(Duration::from_millis(120)).await;
    emit(&peer, step(0.0)).await;
    emit(&peer, step(0.0)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), session.next())
            .await
            .is_err()
    );
    emit(&peer, step(0.0)).await;
    assert_eq!(next(&mut session).await, GradiumSttEvent::TurnStarted);
    finish(&mut session).await;
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn malformed_early_late_duplicate_and_provider_errors_are_terminal_and_redacted() {
    for message in [
        json!({"type":"ready"}),
        json!({"type":"end_of_stream"}),
        json!({"type":"flushed","flush_id":"1"}),
        json!({"type":"step","vad":[{"horizon_s":3,"inactivity_prob":1.1}]}),
        json!({"type":"text","text":"private","stream_id":1}),
        json!({"type":"error","message":"synthetic-not-secret private provider text"}),
    ] {
        let peer = support::peer().await;
        let mut session = GradiumSttClient::new(peer.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        emit(&peer, message).await;
        let error = bounded(session.next()).await.unwrap().unwrap_err();
        assert!(!error.to_string().contains("private"));
        assert!(!format!("{error:?}").contains("synthetic-not-secret"));
        assert!(session.next().await.is_none());
        bounded(peer.task).await.unwrap();
    }
    for id in [json!(1), json!("01"), json!("2")] {
        let mut peer = support::peer().await;
        let mut session = GradiumSttClient::new(peer.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        wire(&mut peer).await;
        session.sender().try_flush().unwrap();
        wire(&mut peer).await;
        emit(&peer, json!({"type":"flushed","flush_id":id})).await;
        assert!(bounded(session.next()).await.unwrap().is_err());
        bounded(peer.task).await.unwrap();
    }
    let mut peer = support::peer().await;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    wire(&mut peer).await;
    session.sender().try_flush().unwrap();
    wire(&mut peer).await;
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    next(&mut session).await;
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    assert!(bounded(session.next()).await.unwrap().is_err());
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn deadlines_partial_samples_output_backpressure_and_drop_are_bounded() {
    let mut peer = peer_ready(None).await;
    peer.config.limits.connect_timeout = Duration::from_millis(40);
    let error = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .err()
        .unwrap();
    assert_eq!(error, GradiumError::Timeout("setup"));
    bounded(peer.task).await.unwrap();
    let mut peer = support::peer().await;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    wire(&mut peer).await;
    session.sender().try_flush().unwrap();
    wire(&mut peer).await;
    assert_eq!(
        bounded(session.next()).await.unwrap(),
        Err(GradiumError::Timeout("flush acknowledgment"))
    );
    bounded(peer.task).await.unwrap();
    let peer = support::peer().await;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().try_send_audio(pcm(1)).unwrap();
    assert_eq!(
        session.sender().finish().await,
        Err(GradiumError::PartialSample)
    );
    assert_eq!(
        session.next().await.unwrap(),
        Err(GradiumError::PartialSample)
    );
    bounded(peer.task).await.unwrap();
    let mut peer = support::peer().await;
    peer.config.limits.queue_capacity = 1;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    for text in ["one", "two"] {
        emit(&peer, json!({"type":"text","text":text})).await
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        bounded(session.next()).await.unwrap(),
        Err(GradiumError::Backpressure)
    );
    bounded(peer.task).await.unwrap();
    let peer = support::peer().await;
    let session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    drop(session);
    bounded(peer.task).await.unwrap();
    wait(|| sender.is_closed()).await;
}
#[tokio::test]
async fn settings_validation_is_atomic_and_new_sessions_get_new_generations() {
    let mut peer = support::peer().await;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    wire(&mut peer).await;
    let sender = session.sender();
    let old = sender.config_snapshot().unwrap();
    assert!(sender
        .merge_settings(
            json!({"eot_threshold":0.8,"delay_in_frames":9})
                .as_object()
                .unwrap()
        )
        .is_err());
    assert_eq!(sender.config_snapshot().unwrap().settings, old.settings);
    assert_eq!(
        sender.merge_settings(json!({"model":"different"}).as_object().unwrap()),
        Err(GradiumError::RequiresRestart)
    );
    sender
        .merge_settings(
            json!({"eot_threshold":0.7,"post_flush_cooldown_frames":0})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        sender
            .config_snapshot()
            .unwrap()
            .settings
            .turn
            .eot_threshold,
        0.7
    );
    let generation = session.generation();
    finish(&mut session).await;
    bounded(peer.task).await.unwrap();
    let peer = support::peer().await;
    let session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert!(session.generation() > generation);
    drop(session);
    bounded(peer.task).await.unwrap();
}
#[test]
fn geometry_settings_endpoints_and_debug_are_constrained() {
    let c = GradiumSttConfig::new("synthetic-not-secret");
    assert!(!format!("{c:?}").contains("synthetic-not-secret"));
    for encoding in ["wav", "opus", "pcm_16000"] {
        let mut c = c.clone();
        c.encoding = encoding.into();
        assert!(GradiumSttClient::new(c).is_err())
    }
    for channels in [0, 2, 8] {
        let mut c = c.clone();
        c.channels = channels;
        assert!(GradiumSttClient::new(c).is_err())
    }
    for endpoint in [
        "ws://api.gradium.ai/asr",
        "wss://127.0.0.1/asr",
        "wss://user@api.gradium.ai/asr",
        "wss://api.gradium.ai/asr?key=secret",
        "wss://api.gradium.ai/asr#secret",
        "file:///etc/passwd",
    ] {
        let mut c = c.clone();
        c.endpoint = endpoint.into();
        assert!(GradiumSttClient::new(c).is_err())
    }
    let mut c = c;
    c.allow_loopback = true;
    c.endpoint = "ws://localhost:123/asr".into();
    assert!(GradiumSttClient::new(c).is_err());
}
#[tokio::test]
async fn cumulative_input_output_control_and_session_deadlines_are_enforced() {
    let mut peer = support::peer().await;
    peer.config.limits.max_session_input_bytes = 4;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender.try_send_audio(pcm(4)).unwrap();
    assert_eq!(sender.try_send_audio(pcm(2)), Err(GradiumError::TooLarge));
    assert_eq!(sender.statistics().accepted_input_bytes, 4);
    finish(&mut session).await;
    bounded(peer.task).await.unwrap();
    let mut peer = support::peer().await;
    peer.config.limits.max_session_messages = 2;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    for _ in 0..3 {
        peer.outgoing
            .send(tokio_tungstenite::tungstenite::Message::Pong(
                Vec::new().into(),
            ))
            .await
            .unwrap()
    }
    assert_eq!(
        bounded(session.next()).await.unwrap(),
        Err(GradiumError::TooLarge)
    );
    bounded(peer.task).await.unwrap();
    let mut peer = support::peer().await;
    peer.config.limits.max_session_output_bytes = 3;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    peer.outgoing
        .send(tokio_tungstenite::tungstenite::Message::Pong(
            vec![1; 4].into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        bounded(session.next()).await.unwrap(),
        Err(GradiumError::TooLarge)
    );
    bounded(peer.task).await.unwrap();
    let mut peer = support::peer().await;
    peer.config.limits.session_timeout = Duration::from_millis(50);
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert_eq!(
        bounded(session.next()).await.unwrap(),
        Err(GradiumError::Timeout("session"))
    );
    bounded(peer.task).await.unwrap();
    let mut peer = peer_mode(Some(json!({"type":"ready"})), false).await;
    peer.config.limits.finish_timeout = Duration::from_millis(50);
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert!(matches!(
        session.sender().finish().await,
        Err(GradiumError::Timeout("finish"))
    ));
    assert!(session.next().await.unwrap().is_err());
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn ordinary_flush_does_not_reset_resampler_or_round_tiny_segments() {
    let mut peer = support::peer().await;
    peer.config.sample_rate = 48000;
    peer.config.limits.queue_capacity = 128;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    wire(&mut peer).await;
    let mut input = Vec::new();
    for id in 1..=3 {
        let sample = 1234i16.to_le_bytes().to_vec();
        input.extend_from_slice(&sample);
        session
            .sender()
            .try_send_audio(AudioRawFrame {
                audio: sample,
                sample_rate: 48000,
                num_channels: 1,
                context_id: None,
            })
            .unwrap();
        session.sender().try_flush().unwrap();
        assert_eq!(
            wire(&mut peer).await,
            json!({"type":"flush","flush_id":id.to_string()})
        );
        emit(&peer, json!({"type":"flushed","flush_id":id.to_string()})).await;
        assert_eq!(
            next(&mut session).await,
            GradiumSttEvent::Flushed {
                flush_id: id,
                confirmed_wire_bytes: 0
            }
        );
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    finish(&mut session).await;
    let audio = wire(&mut peer).await;
    let bytes = STANDARD.decode(audio["audio"].as_str().unwrap()).unwrap();
    assert_eq!(
        bytes,
        pipecat_audio::resample_pcm16(&input, 48000, 24000, 1).unwrap()
    );
    assert_eq!(bytes.len(), 4);
    assert_eq!(wire(&mut peer).await, json!({"type":"end_of_stream"}));
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn control_traffic_does_not_starve_audio_or_terminal_and_cancel_interrupts_setup() {
    let mut peer = support::peer().await;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    wire(&mut peer).await;
    for _ in 0..100 {
        peer.outgoing
            .send(tokio_tungstenite::tungstenite::Message::Pong(
                Vec::new().into(),
            ))
            .await
            .unwrap()
    }
    session.sender().try_send_audio(pcm(2560)).unwrap();
    assert_eq!(wire(&mut peer).await["type"], "audio");
    finish(&mut session).await;
    assert_eq!(wire(&mut peer).await, json!({"type":"end_of_stream"}));
    bounded(peer.task).await.unwrap();
    let peer = peer_ready(None).await;
    let client = GradiumSttClient::new(peer.config.clone()).unwrap();
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let pending = tokio::spawn(async move { client.connect_with_cancel(token).await });
    tokio::time::sleep(Duration::from_millis(15)).await;
    cancel.cancel();
    assert!(matches!(
        bounded(pending).await.unwrap(),
        Err(GradiumError::Cancelled)
    ));
    bounded(peer.task).await.unwrap();
}
