use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU16, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
};

struct Connection {
    request: Request,
    wire: mpsc::Receiver<Message>,
    emit: mpsc::Sender<Message>,
}
struct Peer {
    config: CartesiaTurnsConfig,
    connections: mpsc::Receiver<Connection>,
    task: JoinHandle<()>,
    reject_next: Arc<AtomicU16>,
    hold_next: Arc<AtomicBool>,
    holds: mpsc::Receiver<tokio::sync::oneshot::Sender<()>>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
// tungstenite's callback ABI requires an unboxed HTTP error response.
#[allow(clippy::result_large_err)]
async fn peer() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaTurnsConfig::new("private-test-key");
    config.endpoint = format!(
        "ws://{}/stt/turns/websocket",
        listener.local_addr().unwrap()
    );
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.write_timeout = Duration::from_secs(1);
    config.limits.response_timeout = Duration::from_secs(1);
    let (tx, connections) = mpsc::channel(16);
    let reject_next = Arc::new(AtomicU16::new(0));
    let reject = reject_next.clone();
    let hold_next = Arc::new(AtomicBool::new(false));
    let hold = hold_next.clone();
    let (hold_tx, holds) = mpsc::channel(16);
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { break; }; let tx = tx.clone(); let status = reject.swap(0, Ordering::SeqCst); let paused = hold.swap(false, Ordering::SeqCst); let hold_tx = hold_tx.clone();
                    tasks.spawn(async move {
                        if paused {
                            let (ready, release) = tokio::sync::oneshot::channel();
                            if hold_tx.send(ready).await.is_err() { return; }
                            if release.await.is_err() { return; }
                        }
                        let request = Arc::new(Mutex::new(None)); let capture = request.clone();
                        let result = accept_hdr_async(stream, move |r: &Request, response: Response| { *capture.lock().unwrap() = Some(r.clone()); if status > 0 { return Err(tokio_tungstenite::tungstenite::http::Response::builder().status(status).body(Some("private-test-key secret reason".into())).unwrap()); } Ok(response) }).await;
                        let Ok(mut socket) = result else { return; };
                        let request = request.lock().unwrap().take().unwrap();
                        let (wire_tx, wire) = mpsc::channel(1024); let (emit, mut events) = mpsc::channel(1024);
                        if tx.send(Connection { request, wire, emit }).await.is_err() { return; }
                        loop {
                            tokio::select! {
                                next = socket.next() => match next {
                                    Some(Ok(message)) => { let closed = matches!(message, Message::Close(_)); if wire_tx.send(message).await.is_err() || closed { let _ = socket.flush().await; break; } },
                                    _ => break,
                                },
                                event = events.recv() => match event { Some(event) => { if socket.send(event).await.is_err() { break; } }, None => break },
                            }
                        }
                    });
                },
                _ = tasks.join_next(), if !tasks.is_empty() => (),
            }
        }
    });
    Peer {
        config,
        connections,
        task,
        reject_next,
        hold_next,
        holds,
    }
}
async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("test deadline")
}
async fn connection(peer: &mut Peer) -> Connection {
    within(peer.connections.recv()).await.unwrap()
}
async fn wire(c: &mut Connection) -> Message {
    within(c.wire.recv()).await.unwrap()
}
async fn emit(c: &Connection, value: serde_json::Value) {
    c.emit
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
fn pcm(bytes: usize) -> AudioRawFrame {
    AudioRawFrame::new(vec![1; bytes], 16000, 1).unwrap()
}
fn delta(value: serde_json::Value) -> SttSettings {
    SttSettings {
        base: ServiceSettings {
            extra: serde_json::from_value(value).unwrap(),
            ..Default::default()
        },
        ..Default::default()
    }
}
async fn connected(p: &mut Peer) -> (CartesiaTurnsSession, Connection) {
    let client = CartesiaTurnsClient::new(p.config.clone()).unwrap();
    let task = tokio::spawn(async move { client.connect().await });
    let c = connection(p).await;
    emit(&c, json!({"type":"connected","request_id":"request-1"})).await;
    (within(task).await.unwrap().unwrap(), c)
}
async fn event(session: &mut CartesiaTurnsSession) -> CartesiaTurnsEvent {
    within(session.next()).await.unwrap().unwrap()
}
#[tokio::test]
async fn exact_handshake_ack_readiness_and_no_pcm_before_ack() {
    let mut p = peer().await;
    p.config.settings.keyterm = vec!["Ink 2".into(), "a+b/~*?&中".into()];
    p.config.settings.turn_end_threshold = Some(0.05);
    let client = CartesiaTurnsClient::new(p.config.clone()).unwrap();
    let task = tokio::spawn(async move { client.connect().await });
    let mut c = connection(&mut p).await;
    assert_eq!(c.request.uri().path_and_query().unwrap().as_str(), "/stt/turns/websocket?model=ink-2&encoding=pcm_s16le&sample_rate=16000&turn_end_threshold=0.05&keyterm=Ink%202&keyterm=a%2Bb%2F~%2A%3F%26%E4%B8%AD");
    assert_eq!(c.request.headers()["x-api-key"], "private-test-key");
    assert_eq!(c.request.headers()["cartesia-version"], "2026-03-01");
    assert!(c.request.headers().get("authorization").is_none());
    // Ping completion is a network barrier: HTTP upgrade alone did not admit PCM.
    c.emit.send(Message::Ping(vec![7].into())).await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Pong(vec![7].into()));
    assert!(!task.is_finished());
    assert!(c.wire.try_recv().is_err());
    emit(&c, json!({"type":"future"})).await;
    emit(
        &c,
        json!({"type":"connected","request_id":"ready","extra":17}),
    )
    .await;
    let session = within(task).await.unwrap().unwrap();
    assert_eq!(session.request_id(), Some("ready"));
    assert_eq!(session.connected_result()["extra"], 17);
    session.cancel();
}
#[tokio::test]
async fn ack_timeout_cancellation_and_upgrade_hold_are_bounded() {
    let mut p = peer().await;
    p.config.limits.connect_timeout = Duration::from_millis(150);
    let client = CartesiaTurnsClient::new(p.config.clone()).unwrap();
    let task = tokio::spawn(async move { client.connect().await });
    let mut c = connection(&mut p).await;
    assert!(matches!(
        within(task).await.unwrap(),
        Err(StreamingError::Timeout("Turns connected acknowledgement"))
    ));
    assert!(matches!(wire(&mut c).await, Message::Close(_)));
    let client = CartesiaTurnsClient::new(p.config.clone()).unwrap();
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let task = tokio::spawn(async move { client.connect_with_cancel(token).await });
    let mut c = connection(&mut p).await;
    c.emit.send(Message::Ping(vec![9].into())).await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Pong(vec![9].into()));
    cancel.cancel();
    assert!(matches!(
        within(task).await.unwrap(),
        Err(StreamingError::Cancelled)
    ));
    assert!(matches!(wire(&mut c).await, Message::Close(_)));
    p.hold_next.store(true, Ordering::SeqCst);
    let client = CartesiaTurnsClient::new(p.config.clone()).unwrap();
    let cancel = CancellationToken::new();
    let token = cancel.clone();
    let task = tokio::spawn(async move { client.connect_with_cancel(token).await });
    let release = within(p.holds.recv()).await.unwrap();
    cancel.cancel();
    assert!(matches!(
        within(task).await.unwrap(),
        Err(StreamingError::Cancelled)
    ));
    let _ = release.send(());
}
#[tokio::test]
async fn provider_failure_pre_ready_and_active_close_are_not_success() {
    let p = peer().await;
    p.reject_next.store(401, Ordering::SeqCst);
    let error = CartesiaTurnsClient::new(p.config.clone())
        .unwrap()
        .connect()
        .await
        .err()
        .unwrap();
    assert_eq!(error, StreamingError::Http(401));
    assert!(!format!("{error:?}").contains("private-test-key"));
    for message in [
        json!({"type":"error","message":"private-test-key transcript"}),
        json!({"type":"turn.start","transcript":"secret"}),
    ] {
        let mut p = peer().await;
        let client = CartesiaTurnsClient::new(p.config.clone()).unwrap();
        let task = tokio::spawn(async move { client.connect().await });
        let c = connection(&mut p).await;
        emit(&c, message).await;
        let error = within(task).await.unwrap().err().unwrap();
        assert!(!format!("{error}").contains("private-test-key"));
    }
    let mut p = peer().await;
    let (mut session, c) = connected(&mut p).await;
    c.emit.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::Closed))
    );
    assert!(session.sender().is_closed());
    assert!(within(session.next()).await.is_none());
}
#[tokio::test]
async fn binary_fifo_cumulative_events_usage_json_close_and_trailing_flush() {
    let mut p = peer().await;
    let (mut session, mut c) = connected(&mut p).await;
    let sender = session.sender();
    sender.send_audio(pcm(3200)).await.unwrap();
    sender.send_audio(pcm(1600)).await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Binary(vec![1; 3200].into()));
    assert_eq!(wire(&mut c).await, Message::Binary(vec![1; 1600].into()));
    for (kind, text) in [
        ("turn.start", "not final"),
        ("turn.update", "hello"),
        ("turn.update", "hello world"),
        ("turn.eager_end", "prediction"),
        ("turn.resume", "ignored"),
        ("turn.end", "Hello world."),
    ] {
        emit(
            &c,
            json!({"type":kind,"transcript":text,"is_final":true,"extra":3}),
        )
        .await;
    }
    assert!(
        matches!(event(&mut session).await,CartesiaTurnsEvent::Start{transcript,..} if transcript=="not final")
    );
    assert!(
        matches!(event(&mut session).await,CartesiaTurnsEvent::Update{transcript,..} if transcript=="hello")
    );
    assert!(
        matches!(event(&mut session).await,CartesiaTurnsEvent::Update{transcript,..} if transcript=="hello world")
    );
    assert!(matches!(
        event(&mut session).await,
        CartesiaTurnsEvent::EagerEnd { .. }
    ));
    assert!(matches!(
        event(&mut session).await,
        CartesiaTurnsEvent::Resume { .. }
    ));
    assert!(
        matches!(event(&mut session).await,CartesiaTurnsEvent::End{transcript,audio_seconds,result} if transcript=="Hello world." && audio_seconds==0.15 && result["extra"]==3)
    );
    sender.send_audio(pcm(1600)).await.unwrap();
    assert!(matches!(wire(&mut c).await, Message::Binary(_)));
    sender.finish().await.unwrap();
    assert_eq!(
        wire(&mut c).await,
        Message::Text("{\"type\":\"close\"}".into())
    );
    assert_eq!(sender.try_send_audio(pcm(2)), Err(StreamingError::Closed));
    emit(&c, json!({"type":"turn.start","transcript":"late"})).await;
    emit(&c, json!({"type":"turn.update","transcript":"late"})).await;
    emit(&c, json!({"type":"turn.end","transcript":""})).await;
    assert!(
        matches!(event(&mut session).await,CartesiaTurnsEvent::End{transcript,audio_seconds:0.0,..} if transcript.is_empty())
    );
    c.emit.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        event(&mut session).await,
        CartesiaTurnsEvent::Usage {
            audio_seconds: 0.05
        }
    );
    assert!(within(session.next()).await.is_none());
}
#[tokio::test]
async fn close_drain_timeout_has_no_invented_final_or_restart() {
    let mut p = peer().await;
    p.config.limits.response_timeout = Duration::from_millis(100);
    let (mut session, mut c) = connected(&mut p).await;
    session.sender().finish().await.unwrap();
    assert_eq!(
        wire(&mut c).await,
        Message::Text("{\"type\":\"close\"}".into())
    );
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::Timeout("Turns close drain")))
    );
    assert!(p.connections.try_recv().is_err());
}
#[tokio::test]
async fn input_format_and_empty_pcm_are_bounded() {
    let mut p = peer().await;
    let (session, mut c) = connected(&mut p).await;
    let sender = session.sender();
    assert!(sender
        .try_send_audio(AudioRawFrame {
            audio: vec![1],
            sample_rate: 16000,
            num_channels: 1,
            context_id: None
        })
        .is_err());
    assert!(sender
        .try_send_audio(AudioRawFrame::new(vec![0; 4], 8000, 1).unwrap())
        .is_err());
    assert!(sender
        .try_send_audio(AudioRawFrame::new(vec![0; 4], 16000, 2).unwrap())
        .is_err());
    assert_eq!(
        sender.try_send_audio(pcm(32770)),
        Err(StreamingError::TooLarge)
    );
    sender.send_audio(pcm(0)).await.unwrap();
    sender.finish().await.unwrap();
    assert_eq!(
        wire(&mut c).await,
        Message::Text("{\"type\":\"close\"}".into())
    );
    session.cancel();
}
#[tokio::test]
async fn silence_requires_start_and_real_audio_and_is_counted() {
    let mut p = peer().await;
    p.config.watchdog.minimum_timeout = Duration::from_millis(20);
    let (mut session, mut c) = connected(&mut p).await;
    emit(&c, json!({"type":"turn.start"})).await;
    assert!(matches!(
        event(&mut session).await,
        CartesiaTurnsEvent::Start { .. }
    ));
    c.emit.send(Message::Ping(vec![2].into())).await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Pong(vec![2].into()));
    assert!(c.wire.try_recv().is_err());
    session.sender().send_audio(pcm(320)).await.unwrap();
    assert!(matches!(wire(&mut c).await, Message::Binary(_)));
    emit(&c, json!({"type":"turn.eager_end","transcript":"preview"})).await;
    event(&mut session).await;
    emit(&c, json!({"type":"turn.resume"})).await;
    event(&mut session).await;
    let Message::Binary(silence) = wire(&mut c).await else {
        panic!()
    };
    assert!(silence.iter().all(|b| *b == 0));
    assert!(silence.len() >= 3840);
    emit(&c, json!({"type":"turn.end","transcript":"done"})).await;
    let CartesiaTurnsEvent::End { audio_seconds, .. } = event(&mut session).await else {
        panic!()
    };
    assert_eq!(audio_seconds, (160 + silence.len() / 2) as f64 / 16000.0);
    session.sender().finish().await.unwrap();
    assert_eq!(
        wire(&mut c).await,
        Message::Text("{\"type\":\"close\"}".into())
    );
    c.emit.send(Message::Close(None)).await.unwrap();
    assert!(within(session.next()).await.is_none());
}
#[tokio::test]
async fn silence_budget_and_full_event_queue_cancel_drop_are_bounded() {
    let mut p = peer().await;
    p.config.watchdog.minimum_timeout = Duration::from_millis(1);
    p.config.watchdog.max_catch_up = Duration::from_millis(150);
    p.config.watchdog.max_silence = Duration::from_millis(150);
    let (mut session, mut c) = connected(&mut p).await;
    session.sender().send_audio(pcm(32)).await.unwrap();
    wire(&mut c).await;
    emit(&c, json!({"type":"turn.start"})).await;
    event(&mut session).await;
    assert!(matches!(wire(&mut c).await, Message::Binary(_)));
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::Timeout(
            "Turns watchdog silence budget"
        )))
    );
    let mut p = peer().await;
    p.config.limits.queue_capacity = 1;
    let (mut session, mut c) = connected(&mut p).await;
    for _ in 0..8 {
        emit(&c, json!({"type":"turn.update","transcript":"held"})).await;
    }
    let sender = session.sender();
    session.cancel();
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::Cancelled))
    );
    assert!(matches!(wire(&mut c).await, Message::Close(_)));
    assert!(sender.is_closed());
    let (session, mut c) = connected(&mut p).await;
    let sender = session.sender();
    drop(session);
    assert!(matches!(wire(&mut c).await, Message::Close(_)));
    assert!(sender.is_closed());
}
#[test]
fn dormant_settings_do_not_restore_health_or_mutate_on_failure() {
    let processor = CartesiaTurnsProcessor::new(
        CartesiaTurnsClient::new(CartesiaTurnsConfig::new("key")).unwrap(),
    );
    let health = processor.health().unwrap();
    health.set_usable(false);
    let generation = health.generation();
    let before = processor.config_snapshot().unwrap();
    assert!(processor
        .update_settings(&delta(json!({"keyterm":["new"],"language":"en"})))
        .is_err());
    assert_eq!(processor.config_snapshot().unwrap(), before);
    processor
        .update_settings(&delta(json!({"keyterm":["new"],"turn_end_threshold":0.2})))
        .unwrap();
    assert_eq!(health.generation(), generation);
    assert!(!health.is_usable());
    assert!(processor
        .update_settings(&delta(json!({"keyterm":[" new "]})))
        .is_err());
}

#[tokio::test]
async fn sustained_pcm_does_not_starve_provider_control_or_errors() {
    let mut p = peer().await;
    let (mut session, c) = connected(&mut p).await;
    let sender = session.sender();
    let producer = tokio::spawn(async move {
        for _ in 0..512 {
            if sender.send_audio(pcm(2)).await.is_err() {
                break;
            }
        }
    });
    emit(
        &c,
        json!({"type":"turn.update","transcript":"control under load"}),
    )
    .await;
    assert!(
        matches!(event(&mut session).await,CartesiaTurnsEvent::Update{transcript,..} if transcript=="control under load")
    );
    emit(&c, json!({"type":"error","message":"secret"})).await;
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::Provider { status: None }))
    );
    within(producer).await.unwrap();
}

#[tokio::test]
async fn raw_pcm_spare_capacity_is_rejected_before_queue_admission() {
    let mut p = peer().await;
    p.config.limits.queue_capacity = 1;
    let (mut session, mut c) = connected(&mut p).await;
    let sender = session.sender();
    let oversized = || {
        let mut bytes = Vec::with_capacity(32769);
        bytes.extend_from_slice(&[0xcc, 0xdd]);
        assert_eq!(bytes.len(), 2);
        assert!(bytes.capacity() > 32768);
        AudioRawFrame::new(bytes, 16000, 1).unwrap()
    };
    assert_eq!(
        sender.try_send_audio(oversized()),
        Err(StreamingError::TooLarge)
    );
    assert_eq!(
        sender.send_audio(oversized()).await,
        Err(StreamingError::TooLarge)
    );
    // Neither rejection took the only command slot, before the actor can run.
    sender.try_send_audio(pcm(2)).unwrap();
    sender.finish().await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Binary(vec![1; 2].into()));
    assert_eq!(
        wire(&mut c).await,
        Message::Text("{\"type\":\"close\"}".into())
    );
    c.emit.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        event(&mut session).await,
        CartesiaTurnsEvent::Usage {
            audio_seconds: 1.0 / 16000.0
        }
    );
    assert!(within(session.next()).await.is_none());
}
