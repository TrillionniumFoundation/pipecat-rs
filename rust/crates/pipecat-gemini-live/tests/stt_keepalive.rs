//! Credential-free real loopback WebSocket contracts, not provider qualification.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    time::{timeout, Instant},
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

type Ws = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(3);
const IDLE: Duration = Duration::from_millis(160);
fn policy() -> SttKeepalivePolicy {
    SttKeepalivePolicy {
        idle_timeout: IDLE,
        check_interval: Duration::from_millis(20),
    }
}
fn config() -> GeminiConfig {
    let mut config = GeminiConfig::transcription("synthetic-key", "fixture-model");
    config.limits.finish_timeout = Duration::from_millis(300);
    config
}
async fn recv(ws: &mut Ws) -> Message {
    timeout(WAIT, ws.next())
        .await
        .expect("peer timed out")
        .expect("peer EOF")
        .expect("peer error")
}
async fn send(ws: &mut Ws, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn silent(ws: &mut Ws, duration: Duration) {
    assert!(
        timeout(duration, ws.next()).await.is_err(),
        "unexpected wire input"
    );
}
async fn fixture(
    mut config: GeminiConfig,
    enabled: bool,
    delay_setup: bool,
) -> (GeminiSession, Ws) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let mut client = GeminiClient::new(config).unwrap();
    if enabled {
        client = client.with_stt_keepalive(policy()).unwrap();
    }
    let connecting = tokio::spawn(async move { client.connect().await });
    let (stream, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    assert!(
        serde_json::from_str::<Value>(recv(&mut ws).await.to_text().unwrap())
            .unwrap()
            .get("setup")
            .is_some()
    );
    if delay_setup {
        silent(&mut ws, IDLE * 2).await;
    }
    send(&mut ws, json!({"setupComplete":{}})).await;
    let session = timeout(WAIT, connecting).await.unwrap().unwrap().unwrap();
    (session, ws)
}
async fn audio(ws: &mut Ws, rate: u32, bytes: &[u8]) {
    let message = recv(ws).await;
    let wire: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(
        wire["realtimeInput"]["audio"]["mimeType"],
        format!("audio/pcm;rate={rate}")
    );
    assert_eq!(
        STANDARD
            .decode(wire["realtimeInput"]["audio"]["data"].as_str().unwrap())
            .unwrap(),
        bytes
    );
    assert_eq!(wire.as_object().unwrap().len(), 1);
}
async fn idle_finish(mut session: GeminiSession, ws: &mut Ws) {
    session.sender().finish().await.unwrap();
    assert!(
        matches!(recv(ws).await, Message::Close(_)),
        "idle finish emitted fake stream end"
    );
    assert!(
        timeout(WAIT, session.next_event()).await.unwrap().is_none(),
        "invented event or terminal error"
    );
    session.shutdown().await.unwrap();
}

#[test]
fn policy_is_explicit_stt_only_and_bounded_without_config_literal_changes() {
    assert_eq!(
        SttKeepalivePolicy::new(IDLE).check_interval,
        Duration::from_secs(5)
    );
    assert!(matches!(
        GeminiClient::new(GeminiConfig::new("key", "model"))
            .unwrap()
            .with_stt_keepalive(policy()),
        Err(GeminiError::Unsupported(_))
    ));
    for duration in [Duration::ZERO, Duration::MAX, Duration::from_secs(86_401)] {
        for p in [
            SttKeepalivePolicy {
                idle_timeout: duration,
                ..policy()
            },
            SttKeepalivePolicy {
                check_interval: duration,
                ..policy()
            },
        ] {
            assert!(matches!(
                GeminiClient::new(config()).unwrap().with_stt_keepalive(p),
                Err(GeminiError::Invalid(_))
            ));
        }
    }
    let mut small = config();
    small.limits.max_audio_bytes = 3198;
    assert!(matches!(
        GeminiClient::new(small)
            .unwrap()
            .with_stt_keepalive(policy()),
        Err(GeminiError::TooLarge)
    ));
    let mut small = config();
    small.limits.max_message_bytes = 3200;
    small.limits.max_audio_bytes = 3200;
    assert!(matches!(
        GeminiClient::new(small)
            .unwrap()
            .with_stt_keepalive(policy()),
        Err(GeminiError::TooLarge)
    ));
    let mut exact = config();
    exact.limits.max_audio_bytes = 3200;
    assert!(GeminiClient::new(exact)
        .unwrap()
        .with_stt_keepalive(policy())
        .is_ok());
}

#[tokio::test]
async fn default_off_emits_no_idle_audio() {
    let (session, mut ws) = fixture(config(), false, false).await;
    silent(&mut ws, IDLE * 2).await;
    idle_finish(session, &mut ws).await;
}

#[tokio::test]
async fn delayed_setup_then_exact_silence_at_all_supported_rate_edges() {
    for rate in [8_000, 16_000, 44_101, 96_000] {
        let mut config = config();
        config.input_sample_rate = rate;
        let (session, mut ws) = fixture(config, true, true).await;
        silent(&mut ws, IDLE / 2).await;
        audio(&mut ws, rate, &vec![0; (rate / 10) as usize * 2]).await;
        idle_finish(session, &mut ws).await;
    }
}

#[tokio::test]
async fn successful_silence_resets_idle_without_catchup_bursts_or_events() {
    let (mut session, mut ws) = fixture(config(), true, false).await;
    audio(&mut ws, 16_000, &vec![0; 3200]).await;
    let first = Instant::now();
    silent(&mut ws, IDLE / 2).await;
    audio(&mut ws, 16_000, &vec![0; 3200]).await;
    assert!(first.elapsed() >= IDLE * 3 / 4);
    assert!(timeout(Duration::from_millis(20), session.next_event())
        .await
        .is_err());
    idle_finish(session, &mut ws).await;
}

#[tokio::test]
async fn real_audio_resets_idle_and_silence_preserves_origin_and_real_finish() {
    let (mut session, mut ws) = fixture(config(), true, false).await;
    silent(&mut ws, IDLE / 2).await;
    let origin = Origin {
        frame_id: Some(42),
        context_id: Some("real-user".into()),
    };
    session
        .sender()
        .try_send(Action::Audio(vec![1, 2, 3, 4]), origin.clone())
        .unwrap();
    audio(&mut ws, 16_000, &[1, 2, 3, 4]).await;
    silent(&mut ws, IDLE * 3 / 4).await;
    audio(&mut ws, 16_000, &vec![0; 3200]).await;
    session.sender().finish().await.unwrap();
    let value: Value = serde_json::from_str(recv(&mut ws).await.to_text().unwrap()).unwrap();
    assert_eq!(value, json!({"realtimeInput":{"audioStreamEnd":true}}));
    silent(&mut ws, Duration::from_millis(40)).await;
    send(
        &mut ws,
        json!({"serverContent":{"inputTranscription":{"text":"real"},"turnComplete":true}}),
    )
    .await;
    let mut transcript = false;
    while let Some(event) = timeout(WAIT, session.next_event()).await.unwrap() {
        let event = event.unwrap();
        if matches!(event.event, Event::InputTranscription { .. }) {
            assert_eq!(event.origin, Origin::default());
            transcript = true;
        }
        if matches!(event.event, Event::TurnComplete) {
            assert_eq!(event.origin, origin);
        }
        assert!(!matches!(event.event, Event::Usage(_)), "synthetic usage");
    }
    assert!(transcript);
    assert!(matches!(recv(&mut ws).await, Message::Close(_)));
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn idle_finish_and_cancel_join_actor_without_late_audio() {
    let (session, mut ws) = fixture(config(), true, false).await;
    // No yield between admission and cancel; both are visible before actor work.
    session.sender().finish().await.unwrap();
    timeout(WAIT, session.shutdown()).await.unwrap().unwrap();
    assert!(matches!(
        timeout(WAIT, ws.next()).await.unwrap(),
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
    let (session, mut ws) = fixture(config(), true, false).await;
    audio(&mut ws, 16_000, &vec![0; 3200]).await;
    timeout(WAIT, session.shutdown()).await.unwrap().unwrap();
    assert!(matches!(
        timeout(WAIT, ws.next()).await.unwrap(),
        None | Some(Err(_)) | Some(Ok(Message::Close(_)))
    ));
}

#[tokio::test]
async fn receive_resumption_waits_for_replacement_setup_and_resets_idle() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config();
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let client = GeminiClient::new(config)
        .unwrap()
        .with_stt_keepalive(policy())
        .unwrap()
        .with_session_resumption(ResumptionPolicy::default())
        .unwrap();
    let connecting = tokio::spawn(async move { client.connect().await });
    let (stream, _) = listener.accept().await.unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    recv(&mut ws).await;
    send(&mut ws, json!({"setupComplete":{}})).await;
    let mut session = connecting.await.unwrap().unwrap();
    audio(&mut ws, 16_000, &vec![0; 3200]).await;
    send(
        &mut ws,
        json!({"sessionResumptionUpdate":{"resumable":true,"newHandle":"opaque-handle"}}),
    )
    .await;
    // A provider usage event is an ordered observation barrier for the handle.
    send(&mut ws, json!({"usageMetadata":{"totalTokenCount":7}})).await;
    loop {
        if matches!(
            session.next_event().await.unwrap().unwrap().event,
            Event::Usage(_)
        ) {
            break;
        }
    }
    drop(ws);
    let (stream, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
    let setup: Value = serde_json::from_str(recv(&mut ws).await.to_text().unwrap()).unwrap();
    assert_eq!(
        setup["setup"]["sessionResumption"]["handle"],
        "opaque-handle"
    );
    silent(&mut ws, IDLE * 2).await;
    send(&mut ws, json!({"setupComplete":{}})).await;
    silent(&mut ws, IDLE / 2).await;
    audio(&mut ws, 16_000, &vec![0; 3200]).await;
    idle_finish(session, &mut ws).await;
    assert!(timeout(Duration::from_millis(40), listener.accept())
        .await
        .is_err());
}

#[tokio::test]
async fn processor_mute_does_not_postpone_idle_or_synthesize_frames_and_unmute_sends_audio() {
    use pipecat_core::*;
    use std::sync::Arc;
    for passthrough in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config();
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let client = GeminiClient::new(config)
            .unwrap()
            .with_stt_keepalive(policy())
            .unwrap();
        let processor =
            Arc::new(GeminiLiveProcessor::new(client).with_audio_passthrough(passthrough));
        let starting = tokio::spawn(async move {
            Pipeline::new(vec![processor])
                .start(PipelineConfig::default())
                .await
        });
        let (stream, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        recv(&mut ws).await;
        send(&mut ws, json!({"setupComplete":{}})).await;
        let mut worker = starting.await.unwrap().unwrap();
        let mut seen = Vec::new();
        loop {
            let out = timeout(WAIT, worker.recv()).await.unwrap().unwrap();
            let start = out.frame.data.is_start();
            seen.push(out.frame);
            if start {
                break;
            }
        }
        worker
            .sender()
            .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
            .await
            .unwrap();
        // A same-lane text fence witnesses processing, rather than admission only.
        let fence = Frame::text("muted-ready");
        let fence_id = fence.id;
        worker
            .sender()
            .send(fence, Direction::Downstream)
            .await
            .unwrap();
        loop {
            let out = timeout(WAIT, worker.recv()).await.unwrap().unwrap();
            let done = out.frame.id == fence_id;
            seen.push(out.frame);
            if done {
                break;
            }
        }
        silent(&mut ws, IDLE / 2).await;
        let muted = Frame::new(FrameData::UserAudio(UserAudioRawFrame {
            audio: AudioRawFrame::new(vec![1, 2, 3, 4], 16_000, 1).unwrap(),
            user_id: "muted-speaker".into(),
        }));
        let muted_id = muted.id;
        worker
            .sender()
            .send(muted, Direction::Downstream)
            .await
            .unwrap();
        let fence = Frame::text("muted-audio-processed");
        let fence_id = fence.id;
        worker
            .sender()
            .send(fence, Direction::Downstream)
            .await
            .unwrap();
        loop {
            let out = timeout(WAIT, worker.recv()).await.unwrap().unwrap();
            let done = out.frame.id == fence_id;
            seen.push(out.frame);
            if done {
                break;
            }
        }
        // If muted audio reset the clock, this bounded observation would expire.
        timeout(IDLE * 3 / 4, audio(&mut ws, 16_000, &vec![0; 3200]))
            .await
            .unwrap();
        assert_eq!(seen.iter().any(|frame| frame.id == muted_id), passthrough);
        worker
            .sender()
            .send(Frame::new(FrameData::SttMute(false)), Direction::Downstream)
            .await
            .unwrap();
        let fence = Frame::text("unmuted-ready");
        let fence_id = fence.id;
        worker
            .sender()
            .send(fence, Direction::Downstream)
            .await
            .unwrap();
        loop {
            let out = timeout(WAIT, worker.recv()).await.unwrap().unwrap();
            let done = out.frame.id == fence_id;
            seen.push(out.frame);
            if done {
                break;
            }
        }
        let real = Frame::new(FrameData::UserAudio(UserAudioRawFrame {
            audio: AudioRawFrame::new(vec![5, 6, 7, 8], 16_000, 1).unwrap(),
            user_id: "real-speaker".into(),
        }));
        let real_id = real.id;
        worker
            .sender()
            .send(real, Direction::Downstream)
            .await
            .unwrap();
        audio(&mut ws, 16_000, &[5, 6, 7, 8]).await;
        silent(&mut ws, IDLE / 2).await;
        send(&mut ws, json!({"serverContent":{"inputTranscription":{"text":"real words"},"turnComplete":true}})).await;
        loop {
            let out = timeout(WAIT, worker.recv()).await.unwrap().unwrap();
            let done = if let FrameData::Transcription(t) = &out.frame.data {
                assert_eq!(t.user_id, "real-speaker");
                true
            } else {
                false
            };
            seen.push(out.frame);
            if done {
                break;
            }
        }
        worker.end().await.unwrap();
        assert!(matches!(recv(&mut ws).await, Message::Close(_)));
        while let Some(out) = timeout(WAIT, worker.recv()).await.unwrap() {
            seen.push(out.frame);
        }
        let report = timeout(WAIT, worker.join()).await.unwrap().unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 0);
        for frame in &seen {
            assert!(!matches!(
                frame.data,
                FrameData::SttMute(_) | FrameData::Metrics(_)
            ));
            if matches!(
                frame.data,
                FrameData::InputAudio(_) | FrameData::UserAudio(_)
            ) {
                assert!(
                    passthrough && [muted_id, real_id].contains(&frame.id),
                    "synthetic audio frame"
                );
            }
        }
    }
}

#[tokio::test]
async fn finish_and_cancel_during_replacement_setup_win_without_silence() {
    for cancel in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config();
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let client = GeminiClient::new(config)
            .unwrap()
            .with_stt_keepalive(policy())
            .unwrap()
            .with_session_resumption(ResumptionPolicy::default())
            .unwrap();
        let connecting = tokio::spawn(async move { client.connect().await });
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        recv(&mut ws).await;
        send(&mut ws, json!({"setupComplete":{}})).await;
        let mut session = connecting.await.unwrap().unwrap();
        send(
            &mut ws,
            json!({"sessionResumptionUpdate":{"resumable":true,"newHandle":"handle"}}),
        )
        .await;
        send(&mut ws, json!({"usageMetadata":{"totalTokenCount":7}})).await;
        loop {
            if matches!(
                session.next_event().await.unwrap().unwrap().event,
                Event::Usage(_)
            ) {
                break;
            }
        }
        drop(ws);
        let (stream, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        recv(&mut ws).await;
        silent(&mut ws, IDLE * 2).await;
        if cancel {
            timeout(WAIT, session.shutdown()).await.unwrap().unwrap();
        } else {
            session.sender().finish().await.unwrap();
            assert_eq!(
                timeout(WAIT, session.next_event()).await.unwrap(),
                Some(Err(GeminiError::Closed))
            );
            session.shutdown().await.unwrap();
        }
        assert!(matches!(
            timeout(WAIT, ws.next()).await.unwrap(),
            None | Some(Err(_))
        ));
        assert!(timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err());
    }
}
