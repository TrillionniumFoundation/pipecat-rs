// Independent native review: pinned Python af9f9da9d7afddaa8811c0e0251b32566c980bdf,
// stt.py:440-465 (future settings), 467-487 (End/Cancel), 686-695 (ACK accounting).
// Official wire: https://docs.gladia.io/api-reference/v2/live/websocket
// Address policy: https://www.iana.org/assignments/iana-ipv6-special-registry
mod support;
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gladia::*;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use support::*;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[test]
fn review_rejects_special_ipv6_before_transmitting_credentials() {
    for host in [
        "2001:2::1",
        "2001::1",
        "2001:10::1",
        "2002:7f00:1::1",
        "3fff::1",
        "3fff:fff::1",
        "::ffff:127.0.0.1",
        "64:ff9b::7f00:1",
    ] {
        let mut config = GladiaSttConfig::new("synthetic-review-key");
        config.endpoint = format!("https://[{host}]/v2/live");
        assert_eq!(
            config.validate(),
            Err(GladiaError::Endpoint),
            "REST special address {host}"
        );
        config.endpoint = "https://api.gladia.io/v2/live".into();
        config.allowed_session_origins = vec![format!("wss://[{host}]")];
        assert_eq!(
            config.validate(),
            Err(GladiaError::Endpoint),
            "WS special address {host}"
        );
    }
    for host in ["2606:4700:4700::1111", "2001:4860:4860::8888"] {
        let mut config = GladiaSttConfig::new("synthetic-review-key");
        config.endpoint = format!("https://[{host}]/v2/live");
        config.allowed_session_origins = vec![format!("wss://[{host}]")];
        assert!(config.validate().is_ok(), "ordinary global address {host}");
    }
}

#[tokio::test]
async fn review_ack_disabled_settings_cannot_disable_budget_retirement() {
    let peer = peer(1).await;
    let mut invalid = peer.config.clone();
    invalid.settings.messages_config = Some(GladiaMessagesConfig {
        receive_acknowledgments: Some(false),
        ..Default::default()
    });
    assert!(
        GladiaSttClient::new(invalid).is_err(),
        "ACK-dependent budget cannot support explicitly disabled ACKs"
    );
    let session = GladiaSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    let accepted = sender.config_snapshot().unwrap();
    assert!(sender
        .merge_settings(
            json!({"messages_config":{"receive_acknowledgments":false}})
                .as_object()
                .unwrap()
        )
        .is_err());
    assert_eq!(sender.config_snapshot().unwrap(), accepted);
    session.cancel();
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_real_wire_ack_union_cannot_retire_a_gap_or_half_stereo_frame() {
    let mut peer = peer(1).await;
    peer.config.channels = 2;
    peer.config.limits.max_audio_chunk_bytes = 16;
    peer.config.limits.max_buffered_audio_bytes = 32;
    let mut session = GladiaSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    let audio = || AudioRawFrame::new(vec![7; 16], 16000, 2).unwrap();
    for _ in 0..2 {
        sender.try_send_audio(audio()).unwrap();
        next_wire(&mut peer).await;
    }
    assert_eq!(
        sender.try_send_audio(audio()),
        Err(GladiaError::Backpressure)
    );
    for (start, end, prefix, buffer) in [
        (16, 31, 0, 32),
        (0, 7, 8, 32),
        (16, 31, 8, 32),
        (4, 15, 32, 0),
        (0, 7, 32, 0),
        (0, 31, 32, 0),
    ] {
        emit(&peer,json!({"type":"audio_chunk","session_id":"fixture-0","acknowledged":true,"data":{"byte_range":[start,end]}})).await;
        let GladiaSttEvent::Acknowledgment(a) = next(&mut session).await else {
            panic!("expected ACK")
        };
        assert_eq!(
            (a.acknowledged_bytes, a.buffered_bytes, a.submitted_bytes),
            (prefix, buffer, 32)
        );
        assert_eq!(a.generation, session.generation());
        if buffer == 32 {
            assert_eq!(
                sender.try_send_audio(audio()),
                Err(GladiaError::Backpressure)
            );
        }
    }
    sender.try_send_audio(audio()).unwrap();
    next_wire(&mut peer).await;
    sender.try_send_audio(audio()).unwrap();
    next_wire(&mut peer).await;
    for range in [
        json!([32, 64]),
        json!([34, 63]),
        json!([32, 61]),
        json!([64, 63]),
        json!([-1, 63]),
        json!([32]),
        json!([32, u64::MAX]),
    ] {
        emit(
            &peer,
            json!({"type":"audio_chunk","acknowledged":true,"data":{"byte_range":range}}),
        )
        .await;
        assert!(matches!(next(&mut session).await, GladiaSttEvent::Error(_)));
        assert_eq!(
            sender.try_send_audio(audio()),
            Err(GladiaError::Backpressure)
        );
    }
    emit(&peer,json!({"type":"audio_chunk","session_id":"fixture-previous","acknowledged":true,"data":{"byte_range":[32,63]}})).await;
    assert!(matches!(next(&mut session).await, GladiaSttEvent::Error(_)));
    assert_eq!(
        sender.try_send_audio(audio()),
        Err(GladiaError::Backpressure)
    );
    emit(
        &peer,
        json!({"type":"audio_chunk","acknowledged":true,"data":{"byte_range":[32,63]}}),
    )
    .await;
    assert!(
        matches!(next(&mut session).await,GladiaSttEvent::Acknowledgment(a) if a.acknowledged_bytes==64&&a.buffered_bytes==0)
    );
    sender.try_send_audio(audio()).unwrap();
    next_wire(&mut peer).await;
    sender.finish().await.unwrap();
    assert_eq!(next_wire(&mut peer).await, json!({"type":"stop_recording"}));
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_returned_origin_cannot_transmit_token_to_unlisted_local_peer() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let approved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let attacker = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GladiaSttConfig::new("synthetic-review-rest-key");
    config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
    config.allowed_session_origins = vec![format!("ws://{}", approved.local_addr().unwrap())];
    config.allow_loopback = true;
    config.limits.connect_timeout = Duration::from_millis(200);
    let destination = format!(
        "ws://{}/stolen?token=synthetic-review-session-token",
        attacker.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = http.accept().await.unwrap();
        let (headers, _) = request(&mut socket).await;
        assert!(headers.contains("synthetic-review-rest-key"));
        respond(
            &mut socket,
            201,
            &json!({"id":"origin-fixture","url":destination}).to_string(),
        )
        .await;
    });
    let error = GladiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .err()
        .unwrap();
    assert_eq!(error, GladiaError::Endpoint);
    assert!(!format!("{error:?}").contains("synthetic-review-session-token"));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), attacker.accept())
            .await
            .is_err(),
        "returned token leaked to unapproved origin"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), approved.accept())
            .await
            .is_err()
    );
    bounded(server).await.unwrap();
}

#[derive(Default)]
struct ReviewRecorder(Mutex<Vec<Frame>>);
#[async_trait]
impl FrameProcessor for ReviewRecorder {
    fn name(&self) -> &str {
        "GladiaIndependentRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_settings_accepted_during_setup_survive_and_metadata_uses_active_snapshot() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws.local_addr().unwrap();
    let mut config = GladiaSttConfig::new("synthetic-review-key");
    config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
    config.allowed_session_origins = vec![format!("ws://{ws_addr}")];
    config.allow_loopback = true;
    config.limits.close_timeout = Duration::from_millis(100);
    let (seen, mut requests) = mpsc::channel::<Value>(2);
    let (release, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut release_rx = Some(release_rx);
        for n in 0..2 {
            let (mut socket, _) = http.accept().await.unwrap();
            let (_, body) = request(&mut socket).await;
            seen.send(body).await.unwrap();
            if n == 0 {
                release_rx.take().unwrap().await.unwrap();
            }
            respond(&mut socket,201,&json!({"id":format!("held-{n}"),"url":format!("ws://{ws_addr}/v2/live?token=synthetic")}).to_string()).await;
            let mut socket = accept_async(ws.accept().await.unwrap().0).await.unwrap();
            while let Some(message) = socket.next().await {
                match message {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Ping(p)) => socket.send(Message::Pong(p)).await.unwrap(),
                    _ => (),
                }
            }
        }
    });
    let processor = Arc::new(GladiaSttProcessor::new(
        GladiaSttClient::new(config).unwrap(),
    ));
    let recorder = Arc::new(ReviewRecorder::default());
    let starting = tokio::spawn({
        let processor = processor.clone();
        let recorder = recorder.clone();
        async move {
            Pipeline::new(vec![processor, recorder])
                .start(Default::default())
                .await
        }
    });
    assert_eq!(
        bounded(requests.recv()).await.unwrap()["model"],
        "solaria-1"
    );
    processor
        .merge_settings(
            json!({"model":"accepted-during-setup","enable_vad":true})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        processor.config_snapshot().unwrap().settings.model,
        "accepted-during-setup"
    );
    release.send(()).unwrap();
    let first = bounded(starting).await.unwrap().unwrap();
    assert_eq!(
        processor.sender().unwrap().active_settings().model,
        "solaria-1"
    );
    assert!(!processor.sender().unwrap().active_settings().enable_vad);
    assert_eq!(
        processor.config_snapshot().unwrap().settings.model,
        "accepted-during-setup",
        "a successful future-only settings update must not be lost when setup finishes"
    );
    wait(|| {
        recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|f| matches!(&f.data, FrameData::SttMetadata(_)))
    })
    .await;
    assert!(
        recorder.0.lock().unwrap().iter().any(
            |f| matches!(&f.data,FrameData::SttMetadata(m) if m.base.user_turn_strategies.is_none())
        ),
        "first metadata must follow active false VAD, not the queued future true value"
    );
    first.end().await.unwrap();
    bounded(first.collect()).await.unwrap();
    let second = Pipeline::new(vec![processor.clone()])
        .start(Default::default())
        .await
        .unwrap();
    assert_eq!(
        bounded(requests.recv()).await.unwrap()["model"],
        "accepted-during-setup"
    );
    assert!(processor.sender().unwrap().active_settings().enable_vad);
    second.end().await.unwrap();
    bounded(second.collect()).await.unwrap();
    bounded(server).await.unwrap();
}
