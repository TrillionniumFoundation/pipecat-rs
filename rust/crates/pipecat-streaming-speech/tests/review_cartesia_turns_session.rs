//! Independent public-session tests; expected bytes and protocol strings are literal.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, Pipeline,
    ProcessorContext, ProcessorError, ProcessorHealth, ServiceSettings, Setting, StartFrame,
    SttSettings,
};
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};

type Socket = WebSocketStream<TcpStream>;
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("independent session deadline")
}
fn config(listener: &TcpListener) -> CartesiaTurnsConfig {
    let mut c = CartesiaTurnsConfig::new("independent-synthetic-secret");
    c.endpoint = format!(
        "ws://{}/stt/turns/websocket",
        listener.local_addr().unwrap()
    );
    c.limits.connect_timeout = Duration::from_secs(2);
    c.limits.write_timeout = Duration::from_millis(400);
    c.limits.close_timeout = Duration::from_millis(100);
    c
}
async fn read(socket: &mut Socket) -> Message {
    bounded(socket.next())
        .await
        .expect("peer remains open")
        .expect("peer message")
}
async fn send(socket: &mut Socket, literal: &'static str) {
    bounded(socket.send(Message::Text(literal.into())))
        .await
        .unwrap();
}
fn delta(value: Value) -> SttSettings {
    SttSettings {
        base: ServiceSettings {
            extra: serde_json::from_value(value).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    }
}
fn pcm(bytes: usize) -> AudioRawFrame {
    AudioRawFrame::new(vec![0x31; bytes], 16000, 1).unwrap()
}

#[tokio::test]
async fn independent_http_upgrade_is_not_readiness_and_cancel_retires_held_ack() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = CartesiaTurnsClient::new(config(&listener)).unwrap();
    let (pong, pong_seen) = oneshot::channel();
    let (retired, retired_seen) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = bounded(listener.accept()).await.unwrap();
        let mut socket = bounded(accept_async(tcp)).await.unwrap();
        socket.send(Message::Ping(vec![0x24].into())).await.unwrap();
        assert_eq!(read(&mut socket).await, Message::Pong(vec![0x24].into()));
        pong.send(()).unwrap();
        // Any PCM/JSON command before cancellation is a readiness violation.
        assert!(matches!(read(&mut socket).await, Message::Close(_)));
        let _ = socket.flush().await;
        retired.send(()).unwrap();
    });
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let connecting = tokio::spawn(async move { client.connect_with_cancel(token).await });
    bounded(pong_seen).await.unwrap();
    assert!(
        !connecting.is_finished(),
        "HTTP upgrade must not admit a session before connected"
    );
    cancel.cancel();
    assert!(matches!(
        bounded(connecting).await.unwrap(),
        Err(StreamingError::Cancelled)
    ));
    bounded(retired_seen).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn independent_turns_query_has_no_live_language_or_keyterm_model_filter() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config(&listener);
    c.settings.model = "future model".into();
    c.settings.turn_start_threshold = Some(0.5);
    c.settings.turn_eager_end_threshold = Some(0.6);
    c.settings.turn_end_threshold = Some(0.05);
    c.settings.turn_end_timeout_ms = Some(11200);
    c.settings.keyterm = vec!["\u{1c} Ink 2\u{1f}".into(), "a+b/*&中".into()];
    let server = tokio::spawn(async move {
        let (tcp, _) = bounded(listener.accept()).await.unwrap();
        let mut socket = bounded(accept_hdr_async(tcp, |r: &Request, response: Response| {
            assert_eq!(r.uri().path_and_query().unwrap().as_str(), "/stt/turns/websocket?model=future%20model&encoding=pcm_s16le&sample_rate=16000&turn_start_threshold=0.5&turn_eager_end_threshold=0.6&turn_end_threshold=0.05&turn_end_timeout_ms=11200&keyterm=Ink%202&keyterm=a%2Bb%2F%2A%26%E4%B8%AD");
            assert_eq!(r.headers()["cartesia-version"], "2026-03-01");
            assert_eq!(r.headers()["x-api-key"], "independent-synthetic-secret");
            assert!(r.headers().get("authorization").is_none());
            Ok(response)
        })).await.unwrap();
        send(
            &mut socket,
            r#"{"type":"connected","request_id":"literal-id","future":19}"#,
        )
        .await;
        assert_eq!(
            read(&mut socket).await,
            Message::Binary(vec![0x31; 320].into())
        );
        assert_eq!(
            read(&mut socket).await.to_text().unwrap(),
            r#"{"type":"close"}"#
        );
        send(
            &mut socket,
            r#"{"type":"turn.end","transcript":"Drained final.","future":23}"#,
        )
        .await;
        socket.close(None).await.unwrap();
    });
    let client = CartesiaTurnsClient::new(c).unwrap();
    assert!(!format!("{client:?}").contains("independent-synthetic-secret"));
    let mut session = bounded(client.connect()).await.unwrap();
    assert_eq!(session.request_id(), Some("literal-id"));
    assert_eq!(session.connected_result()["future"], 19);
    let sender = session.sender();
    for asynchronous in [false, true] {
        let mut oversized_allocation = Vec::with_capacity(32769);
        oversized_allocation.extend_from_slice(&[0x31, 0x31]);
        assert!(oversized_allocation.capacity() > 32768);
        let frame = AudioRawFrame::new(oversized_allocation, 16000, 1).unwrap();
        let rejected = if asynchronous {
            sender.send_audio(frame).await
        } else {
            sender.try_send_audio(frame)
        };
        assert_eq!(
            rejected,
            Err(StreamingError::TooLarge),
            "retained allocation, not only payload length, must fit each input slot"
        );
    }
    sender.send_audio(pcm(320)).await.unwrap();
    sender.finish().await.unwrap();
    assert!(sender.try_send_audio(pcm(2)).is_err());
    match bounded(session.next()).await.unwrap().unwrap() {
        CartesiaTurnsEvent::End {
            transcript,
            audio_seconds,
            result,
        } => {
            assert_eq!(transcript, "Drained final.");
            assert_eq!(audio_seconds, 0.01);
            assert_eq!(result["future"], 23);
        }
        other => panic!("expected committed drain: {other:?}"),
    }
    assert!(bounded(session.next()).await.is_none());
    assert!(bounded(session.next()).await.is_none());
    bounded(server).await.unwrap();
}

#[test]
fn independent_settings_null_omission_first_overflow_and_unsupported_compound_are_atomic() {
    let settings = CartesiaTurnsSettings {
        turn_start_threshold: Some(0.8),
        keyterm: vec!["kept".into()],
        ..Default::default()
    };
    assert_eq!(settings.merge(&SttSettings::default()).unwrap(), settings);
    let cleared = settings
        .merge(&delta(json!({"turn_start_threshold":null,"keyterm":null})))
        .unwrap();
    assert_eq!(cleared.turn_start_threshold, None);
    assert!(cleared.keyterm.is_empty());
    let unsupported = SttSettings {
        language: Setting::Value("fr".into()),
        base: ServiceSettings {
            extra: [("keyterm".into(), json!(["must not apply"]))].into(),
            ..Default::default()
        },
    };
    assert!(settings.merge(&unsupported).is_err());
    assert_eq!(settings.keyterm, ["kept"]);
    assert_eq!(settings.turn_start_threshold, Some(0.8));
    for invalid in [
        json!({"turn_start_threshold":0.49}),
        json!({"turn_end_threshold":0.51}),
        json!({"turn_end_timeout_ms":11201}),
        json!({"turn_end_timeout_ms":640.0}),
        json!({"keyterm":[null]}),
        json!({"enable_eager_end_of_turn":true}),
    ] {
        assert!(settings.merge(&delta(invalid)).is_err());
    }
    let mut terms = CartesiaTurnsSettings {
        model: "future".into(),
        keyterm: vec!["汉".repeat(1199), "overflow".into(), "z".into()],
        ..Default::default()
    };
    assert_eq!(terms.prepared_keyterms().terms, vec!["汉".repeat(1199)]);
    assert_eq!(terms.prepared_keyterms().dropped, 2);
    terms.keyterm = vec!["\u{85}\u{1c} same \u{1f}".into(); 101];
    assert_eq!(
        terms.prepared_keyterms().terms,
        vec!["same".to_owned(); 100]
    );
    assert!(!terms.prepared_keyterms().ignored_model);
}

#[tokio::test]
async fn independent_long_chunk_watchdog_counts_actual_silent_samples_and_empty_end_keeps_usage() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config(&listener);
    c.watchdog.minimum_timeout = Duration::from_millis(50);
    let (measured, measurement) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = bounded(accept_async(bounded(listener.accept()).await.unwrap().0))
            .await
            .unwrap();
        send(&mut socket, r#"{"type":"connected"}"#).await;
        assert_eq!(
            read(&mut socket).await,
            Message::Binary(vec![0x31; 6400].into())
        );
        let real_seen = tokio::time::Instant::now();
        socket
            .feed(Message::Text(r#"{"type":"turn.start"}"#.into()))
            .await
            .unwrap();
        socket
            .feed(Message::Text(
                r#"{"type":"turn.eager_end","transcript":"pending"}"#.into(),
            ))
            .await
            .unwrap();
        socket
            .feed(Message::Text(r#"{"type":"turn.resume"}"#.into()))
            .await
            .unwrap();
        socket.flush().await.unwrap();
        let silence = read(&mut socket).await;
        assert!(silence.is_binary());
        assert!(
            real_seen.elapsed() >= Duration::from_millis(400),
            "twice the real 200ms chunk must dominate the 50ms minimum"
        );
        let bytes = silence.into_data();
        assert!(bytes.len() >= 12800);
        assert!(bytes.iter().all(|v| *v == 0));
        measured.send(bytes.len()).unwrap();
        socket
            .feed(Message::Text(
                r#"{"type":"turn.end","transcript":""}"#.into(),
            ))
            .await
            .unwrap();
        socket
            .feed(Message::Text(
                r#"{"type":"turn.end","transcript":"Includes actual silence."}"#.into(),
            ))
            .await
            .unwrap();
        socket.flush().await.unwrap();
        assert_eq!(
            read(&mut socket).await.to_text().unwrap(),
            r#"{"type":"close"}"#
        );
        socket.close(None).await.unwrap();
    });
    let mut session = bounded(CartesiaTurnsClient::new(c).unwrap().connect())
        .await
        .unwrap();
    session.sender().send_audio(pcm(6400)).await.unwrap();
    let silent_bytes = bounded(measurement).await.unwrap();
    let mut empty_seen = false;
    loop {
        match bounded(session.next()).await.unwrap().unwrap() {
            CartesiaTurnsEvent::End {
                transcript,
                audio_seconds,
                ..
            } if transcript.is_empty() => {
                empty_seen = true;
                assert_eq!(audio_seconds, 0.0);
            }
            CartesiaTurnsEvent::End {
                transcript,
                audio_seconds,
                ..
            } => {
                assert!(empty_seen);
                assert_eq!(transcript, "Includes actual silence.");
                assert_eq!(audio_seconds, (6400 + silent_bytes) as f64 / 32000.0);
                break;
            }
            CartesiaTurnsEvent::Usage { .. } => panic!("usage should belong to committed end"),
            _ => {}
        }
    }
    session.sender().finish().await.unwrap();
    assert!(
        bounded(session.next()).await.is_none(),
        "usage must not be double-reported at graceful close"
    );
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn independent_malformed_or_provider_error_never_exposes_wire_secrets() {
    for literal in [
        r#"{"type":"error","message":"private provider transcript","error_code":"independent-synthetic-secret"}"#,
        r#"{"type":"turn.update","transcript":{"secret":"private provider transcript"}}"#,
        "[]",
        "not json",
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c = config(&listener);
        let server = tokio::spawn(async move {
            let mut socket = bounded(accept_async(bounded(listener.accept()).await.unwrap().0))
                .await
                .unwrap();
            send(&mut socket, r#"{"type":"connected"}"#).await;
            send(&mut socket, literal).await;
            let _ = bounded(socket.next()).await;
        });
        let mut session = bounded(CartesiaTurnsClient::new(c).unwrap().connect())
            .await
            .unwrap();
        let error = bounded(session.next()).await.unwrap().unwrap_err();
        let display = format!("{error:?} {error}");
        assert!(!display.contains("private provider transcript"));
        assert!(!display.contains("independent-synthetic-secret"));
        assert!(bounded(session.next()).await.is_none());
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn independent_health_restoration_during_connected_wait_rejects_late_readiness() {
    for same_value in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let processor = Arc::new(CartesiaTurnsProcessor::new(
            CartesiaTurnsClient::new(config(&listener)).unwrap(),
        ));
        let health = processor.health().unwrap();
        let (in_ready, ready_seen) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut socket = bounded(accept_async(bounded(listener.accept()).await.unwrap().0))
                .await
                .unwrap();
            socket.send(Message::Ping(vec![0x42].into())).await.unwrap();
            assert_eq!(read(&mut socket).await, Message::Pong(vec![0x42].into()));
            in_ready.send(()).unwrap();
            bounded(released).await.unwrap();
            // Release a stale success and an authentication failure together.
            // Either write may lose the race to cancellation; neither may revive the owner.
            let _ = socket
                .feed(Message::Text(
                    r#"{"type":"connected","request_id":"stale"}"#.into(),
                ))
                .await;
            let _ = socket
                .feed(Message::Text(
                    r#"{"type":"error","message":"stale auth secret"}"#.into(),
                ))
                .await;
            let _ = socket.flush().await;
            match bounded(socket.next()).await {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {}
                Some(Ok(message)) => {
                    panic!("retired handshake transmitted unexpected work: {message:?}")
                }
            }
        });
        let owned = processor.clone();
        let start =
            tokio::spawn(async move { Pipeline::new(vec![owned]).start(Default::default()).await });
        bounded(ready_seen).await.unwrap();
        if !same_value {
            health.set_usable(false);
        }
        health.set_usable(true);
        let restored = health.generation();
        release.send(()).unwrap();
        let result = bounded(start).await.unwrap();
        // Pipeline lifecycle setup may complete dormant to suppress retired
        // errors. That must not imply usable socket readiness or new work.
        if let Ok(worker) = result {
            processor
                .update_settings(&delta(json!({"keyterm":["dormant after retirement"]})))
                .unwrap();
            worker.cancel("retired setup cleanup").await.unwrap();
            let (frames, report) = bounded(worker.collect()).await.unwrap();
            assert!(
                report.errors.is_empty(),
                "retired setup leaked an old error: {:?}",
                report.errors
            );
            assert!(!frames.iter().any(|f| matches!(
                f.frame.data,
                pipecat_core::FrameData::Transcription(_)
                    | pipecat_core::FrameData::Metrics(_)
                    | pipecat_core::FrameData::ProposedUserStartedSpeaking
                    | pipecat_core::FrameData::ProposedUserStoppedSpeaking
                    | pipecat_core::FrameData::Error(_)
            )));
        }
        assert!(
            health.is_usable(),
            "late old authentication failure must not poison restored health"
        );
        assert_eq!(health.generation(), restored);
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn independent_full_event_queue_cancel_and_drop_bypass_capacity() {
    for drop_consumer in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut c = config(&listener);
        c.limits.queue_capacity = 1;
        let (filled, queue_filled) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut socket = bounded(accept_async(bounded(listener.accept()).await.unwrap().0))
                .await
                .unwrap();
            send(&mut socket, r#"{"type":"connected"}"#).await;
            socket
                .feed(Message::Text(
                    r#"{"type":"turn.update","transcript":"queued first"}"#.into(),
                ))
                .await
                .unwrap();
            socket.feed(Message::Ping(vec![0x64].into())).await.unwrap();
            socket.flush().await.unwrap();
            // Receiving this Pong proves the first event has entered the sole
            // output slot. The consumer has not called next().
            assert_eq!(read(&mut socket).await, Message::Pong(vec![0x64].into()));
            socket
                .feed(Message::Text(
                    r#"{"type":"turn.end","transcript":"must not escape"}"#.into(),
                ))
                .await
                .unwrap();
            socket.feed(Message::Ping(vec![0x65].into())).await.unwrap();
            socket.flush().await.unwrap();
            filled.send(()).unwrap();
            // With the first slot occupied, processing the second event blocks
            // before the following Ping. Cancellation must bypass that wait.
            assert!(matches!(read(&mut socket).await, Message::Close(_)));
            let _ = socket.flush().await;
        });
        let mut session = bounded(CartesiaTurnsClient::new(c).unwrap().connect())
            .await
            .unwrap();
        let sender = session.sender();
        bounded(queue_filled).await.unwrap();
        if drop_consumer {
            drop(session);
        } else {
            session.cancel();
            assert_eq!(
                bounded(session.next()).await,
                Some(Err(StreamingError::Cancelled))
            );
            assert!(bounded(session.next()).await.is_none());
        }
        bounded(server).await.unwrap();
        assert!(sender.is_closed());
        assert!(sender.try_send_audio(pcm(2)).is_err());
    }
}

struct OwnerCapture {
    processor: Arc<CartesiaTurnsProcessor>,
    owner: Arc<Mutex<Option<ProcessorContext>>>,
}
#[async_trait]
impl FrameProcessor for OwnerCapture {
    fn name(&self) -> &str {
        "IndependentTurnsOwner"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.processor.health()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.owner.lock().unwrap() = Some(ctx.clone());
        self.processor.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.processor.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.processor.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.processor.cleanup().await
    }
}

#[tokio::test]
async fn independent_unhealthy_unmute_preserves_retry_then_requires_fresh_connected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let processor = Arc::new(CartesiaTurnsProcessor::new(
        CartesiaTurnsClient::new(config(&listener)).unwrap(),
    ));
    let (old_closed, old_closed_seen) = oneshot::channel();
    let (new_waiting, new_waiting_seen) = oneshot::channel();
    let (release_ack, ack_released) = oneshot::channel();
    let (received, received_audio) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut old = bounded(accept_async(bounded(listener.accept()).await.unwrap().0))
            .await
            .unwrap();
        send(&mut old, r#"{"type":"connected"}"#).await;
        assert!(matches!(read(&mut old).await, Message::Close(_)));
        let _ = old.flush().await;
        old_closed.send(()).unwrap();
        let mut fresh = bounded(accept_async(bounded(listener.accept()).await.unwrap().0))
            .await
            .unwrap();
        fresh.send(Message::Ping(vec![0x73].into())).await.unwrap();
        assert_eq!(read(&mut fresh).await, Message::Pong(vec![0x73].into()));
        new_waiting.send(()).unwrap();
        bounded(ack_released).await.unwrap();
        send(
            &mut fresh,
            r#"{"type":"connected","request_id":"new after health"}"#,
        )
        .await;
        assert_eq!(
            read(&mut fresh).await,
            Message::Binary(vec![0x31; 320].into())
        );
        received.send(()).unwrap();
        assert_eq!(
            read(&mut fresh).await.to_text().unwrap(),
            r#"{"type":"close"}"#
        );
        fresh.close(None).await.unwrap();
    });
    let captured = Arc::new(Mutex::new(None));
    let worker = bounded(
        Pipeline::new(vec![Arc::new(OwnerCapture {
            processor: processor.clone(),
            owner: captured.clone(),
        })])
        .start(Default::default()),
    )
    .await
    .unwrap();
    let owner = captured.lock().unwrap().clone().unwrap();
    processor
        .process(
            Frame::new(FrameData::SttMute(true)),
            Direction::Downstream,
            &owner,
        )
        .await
        .unwrap();
    bounded(old_closed_seen).await.unwrap();
    let health = processor.health().unwrap();
    health.set_usable(false);
    assert!(processor
        .process(
            Frame::new(FrameData::SttMute(false)),
            Direction::Downstream,
            &owner
        )
        .await
        .is_err());
    health.set_usable(true);
    let unmute = {
        let processor = processor.clone();
        let owner = owner.clone();
        tokio::spawn(async move {
            processor
                .process(
                    Frame::new(FrameData::SttMute(false)),
                    Direction::Downstream,
                    &owner,
                )
                .await
        })
    };
    bounded(new_waiting_seen).await.unwrap();
    assert!(
        !unmute.is_finished(),
        "unmute must wait for the fresh application acknowledgement"
    );
    release_ack.send(()).unwrap();
    bounded(unmute).await.unwrap().unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(pcm(320))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    bounded(received_audio).await.unwrap();
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(server).await.unwrap();
}
