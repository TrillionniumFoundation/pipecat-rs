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
    config: CartesiaSttConfig,
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
    let mut config = CartesiaSttConfig::new("private-test-key");
    config.endpoint = format!("ws://{}/stt/websocket", listener.local_addr().unwrap());
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
async fn drain(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    within(async {
        let mut frames = vec![];
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        frames
    })
    .await
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    within(worker.recv()).await.unwrap()
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}

#[test]
fn defaults_keyterms_limits_unicode_and_settings_are_exact() {
    let config = CartesiaSttConfig::new("secret");
    assert_eq!(config.endpoint, "wss://api.cartesia.ai/stt/websocket");
    assert_eq!(config.settings.model, "ink-whisper");
    assert_eq!(config.settings.language, "en");
    assert_eq!(CARTESIA_STT_VERSION, "2026-03-01");
    assert_eq!(config.keepalive_timeout, Duration::from_secs(120));
    assert_eq!(config.keepalive_interval, Duration::from_secs(30));
    let mut settings = CartesiaSttSettings {
        model: "ink-preview-future".into(),
        keyterm: vec![
            "\u{1c}  A \u{1f}".into(),
            " ".into(),
            "中".repeat(1199),
            "overflow".into(),
            "tail".into(),
        ],
        ..Default::default()
    };
    let prepared = settings.prepared_keyterms();
    assert_eq!(prepared.terms, vec!["A".to_string(), "中".repeat(1199)]);
    assert_eq!(prepared.dropped, 2);
    settings.keyterm = (0..150).map(|n| format!("term{n}")).collect();
    assert_eq!(settings.prepared_keyterms().terms.len(), 100);
    settings.model = "ink-whisper".into();
    assert!(settings.prepared_keyterms().ignored_model);
    assert!(settings.prepared_keyterms().terms.is_empty());
    assert!(settings.merge(&delta(json!({"unknown":true}))).is_err());
    assert!(settings.merge(&delta(json!({"language":null}))).is_err());
    assert!(settings.merge(&delta(json!({"keyterm":[1]}))).is_err());
    assert!(settings
        .merge(&delta(json!({"keyterm":null})))
        .unwrap()
        .keyterm
        .is_empty());
    assert!(!format!("{config:?}").contains("secret"));
}
#[tokio::test]
async fn exact_handshake_repeated_rfc3986_query_and_auth() {
    let mut p = peer().await;
    p.config.settings.model = "ink-2".into();
    p.config.settings.keyterm = vec!["Ink 2".into(), "a+b/~*?&中".into()];
    let session = CartesiaSttClient::new(p.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let c = connection(&mut p).await;
    assert_eq!(c.request.uri().path_and_query().unwrap().as_str(),"/stt/websocket?model=ink-2&language=en&encoding=pcm_s16le&sample_rate=16000&keyterm=Ink%202&keyterm=a%2Bb%2F~%2A%3F%26%E4%B8%AD");
    assert_eq!(c.request.headers()["x-api-key"], "private-test-key");
    assert_eq!(c.request.headers()["cartesia-version"], "2026-03-01");
    assert!(c.request.headers().get("authorization").is_none());
    session.cancel();
}
#[tokio::test]
async fn transcript_usage_finalize_done_and_terminal_admission() {
    let mut p = peer().await;
    let mut session = CartesiaSttClient::new(p.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut c = connection(&mut p).await;
    let sender = session.sender();
    sender.send_audio(pcm(3200)).await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Binary(vec![1; 3200].into()));
    emit(
        &c,
        json!({"type":"transcript","text":"pre","is_final":false,"language":"en"}),
    )
    .await;
    let CartesiaSttEvent::Transcript(t) = within(session.next()).await.unwrap().unwrap() else {
        panic!()
    };
    assert!(!t.is_final);
    assert_eq!(t.audio_seconds, 0.0);
    sender.finalize().await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Text("finalize".into()));
    emit(&c, json!({"type":"transcript","text":"","is_final":true})).await;
    emit(
        &c,
        json!({"type":"transcript","text":"final","is_final":true,"language":"en","extra":7}),
    )
    .await;
    let CartesiaSttEvent::Transcript(t) = within(session.next()).await.unwrap().unwrap() else {
        panic!()
    };
    assert!(t.is_final);
    assert_eq!(t.audio_seconds, 0.1);
    assert_eq!(t.result["extra"], 7);
    sender.send_audio(pcm(1600)).await.unwrap();
    assert!(matches!(wire(&mut c).await, Message::Binary(_)));
    sender.finish().await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Text("done".into()));
    assert_eq!(sender.try_finalize(), Err(StreamingError::Closed));
    assert_eq!(sender.try_send_audio(pcm(2)), Err(StreamingError::Closed));
    c.emit.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        within(session.next()).await,
        Some(Ok(CartesiaSttEvent::Usage {
            audio_seconds: 0.05
        }))
    );
    assert_eq!(within(session.next()).await, None);
}
#[tokio::test]
async fn native_duration_excludes_silence_keepalive_and_empty_finals() {
    let mut p = peer().await;
    p.config.keepalive_timeout = Duration::from_millis(20);
    p.config.keepalive_interval = Duration::from_millis(10);
    let mut session = CartesiaSttClient::new(p.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut c = connection(&mut p).await;
    assert_eq!(wire(&mut c).await, Message::Binary(vec![0; 3200].into()));
    emit(
        &c,
        json!({"type":"transcript","text":"silence","is_final":true}),
    )
    .await;
    let CartesiaSttEvent::Transcript(t) = within(session.next()).await.unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(t.audio_seconds, 0.0);
    session.cancel();
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::Cancelled))
    );
    assert_eq!(session.next().await, None);
}
#[tokio::test]
async fn protocol_error_is_redacted_and_does_not_reconnect() {
    let mut p = peer().await;
    let mut session = CartesiaSttClient::new(p.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let c = connection(&mut p).await;
    emit(
        &c,
        json!({"type":"error","message":"private-test-key secret transcript","status_code":401}),
    )
    .await;
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::Provider { status: Some(401) }))
    );
    assert_eq!(session.next().await, None);
    assert!(p.connections.try_recv().is_err());
}
#[tokio::test]
async fn malformed_messages_fail_closed() {
    for message in [
        Message::Text("{private-test-key".into()),
        Message::Text(json!({"type":"transcript","text":4}).to_string().into()),
        Message::Text(
            json!({"type":"transcript","text":"x","is_final":1})
                .to_string()
                .into(),
        ),
        Message::Binary(vec![1, 2].into()),
    ] {
        let mut p = peer().await;
        let mut session = CartesiaSttClient::new(p.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let c = connection(&mut p).await;
        c.emit.send(message).await.unwrap();
        let error = within(session.next()).await.unwrap().unwrap_err();
        assert!(matches!(error, StreamingError::Protocol(_)));
        assert!(!error.to_string().contains("private-test-key"));
    }
}
#[tokio::test]
async fn finish_timeout_and_unexpected_close_are_terminal() {
    let mut p = peer().await;
    p.config.limits.response_timeout = Duration::from_millis(30);
    let mut session = CartesiaSttClient::new(p.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut c = connection(&mut p).await;
    session.sender().finish().await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Text("done".into()));
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::Timeout("Cartesia final response")))
    );
    let mut second = CartesiaSttClient::new(p.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let c = connection(&mut p).await;
    c.emit.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        within(second.next()).await,
        Some(Err(StreamingError::Closed))
    );
    assert!(p.connections.try_recv().is_err());
}
#[tokio::test]
async fn cancellation_and_drop_bypass_full_output_and_sender_clones() {
    for drop_session in [false, true] {
        let mut p = peer().await;
        p.config.limits.queue_capacity = 1;
        let mut session = CartesiaSttClient::new(p.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut c = connection(&mut p).await;
        let sender = session.sender();
        for _ in 0..5 {
            emit(&c, json!({"type":"transcript","text":"backlog"})).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        if drop_session {
            drop(session);
        } else {
            session.cancel();
            assert_eq!(
                within(session.next()).await,
                Some(Err(StreamingError::Cancelled))
            );
            assert_eq!(session.next().await, None);
        }
        assert!(matches!(wire(&mut c).await, Message::Close(_)));
        assert!(sender.is_closed());
        assert_eq!(
            sender.try_send_audio(pcm(2)),
            Err(StreamingError::Cancelled)
        );
    }
}
#[tokio::test]
async fn queue_backpressure_format_and_wire_size_are_bounded() {
    let mut p = peer().await;
    p.config.limits.queue_capacity = 1;
    p.config.limits.max_message_bytes = 3200;
    let mut session = CartesiaSttClient::new(p.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let c = connection(&mut p).await;
    let sender = session.sender();
    assert!(matches!(
        sender.try_send_audio(AudioRawFrame::new(vec![0; 4], 8000, 1).unwrap()),
        Err(StreamingError::Invalid(_))
    ));
    assert_eq!(
        sender.try_send_audio(pcm(3202)),
        Err(StreamingError::TooLarge)
    );
    let mut full = false;
    for _ in 0..100 {
        if sender.try_send_audio(pcm(2)) == Err(StreamingError::Backpressure) {
            full = true;
            break;
        }
    }
    assert!(full);
    c.emit
        .send(Message::Text("x".repeat(3300).into()))
        .await
        .unwrap();
    assert_eq!(
        within(session.next()).await,
        Some(Err(StreamingError::TooLarge))
    );
}
#[test]
fn unsafe_endpoints_headers_and_impossible_keepalive_are_rejected() {
    for endpoint in [
        "ws://example.com/stt/websocket",
        "wss://user:pass@example.com/stt",
        "wss://example.com/stt?token=secret",
        "https://example.com/stt",
    ] {
        let mut config = CartesiaSttConfig::new("key");
        config.endpoint = endpoint.into();
        assert!(CartesiaSttClient::new(config).is_err());
    }
    let mut config = CartesiaSttConfig::new("key\r\nX: injected");
    assert!(CartesiaSttClient::new(config.clone()).is_err());
    config.api_key = "key".into();
    config.limits.max_message_bytes = 1;
    assert!(CartesiaSttClient::new(config).is_err());
}
#[test]
fn dormant_settings_are_atomic_and_never_restore_health() {
    let proc =
        CartesiaSttProcessor::new(CartesiaSttClient::new(CartesiaSttConfig::new("key")).unwrap());
    let health = proc.health().unwrap();
    health.set_usable(false);
    let before = health.generation();
    let snapshot = proc.config_snapshot().unwrap();
    assert!(proc
        .update_settings(&delta(json!({"model":"ink-2","unknown":1})))
        .is_err());
    assert_eq!(proc.config_snapshot().unwrap(), snapshot);
    assert!(proc
        .update_settings(&delta(json!({"keyterm":["ignored"]})))
        .is_err());
    assert_eq!(health.generation(), before);
    proc.update_settings(&delta(json!({"model":"ink-2","keyterm":["Cartesia"]})))
        .unwrap();
    assert_eq!(proc.config_snapshot().unwrap().settings.model, "ink-2");
    assert_eq!(health.generation(), before);
    proc.update_settings(&SttSettings::default()).unwrap();
    assert_eq!(health.generation(), before);
}
#[derive(Clone)]
struct Owner {
    proc: Arc<CartesiaSttProcessor>,
    ctx: Arc<Mutex<Option<ProcessorContext>>>,
    invocation: Arc<Mutex<Option<ProcessorContext>>>,
}
#[async_trait]
impl FrameProcessor for Owner {
    fn name(&self) -> &str {
        "CartesiaSTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.proc.health()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.ctx.lock().unwrap() = Some(ctx.clone());
        self.proc.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(_)) {
            *self.invocation.lock().unwrap() = Some(ctx.clone());
        }
        self.proc.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.proc.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.proc.cleanup().await
    }
}
async fn pipeline(p: &Peer) -> (Arc<CartesiaSttProcessor>, Owner, PipelineWorker) {
    let proc = Arc::new(
        CartesiaSttProcessor::new(CartesiaSttClient::new(p.config.clone()).unwrap())
            .with_user_id("speaker")
            .unwrap(),
    );
    let owner = Owner {
        proc: proc.clone(),
        ctx: Arc::default(),
        invocation: Arc::default(),
    };
    let worker = Pipeline::new(vec![Arc::new(owner.clone())])
        .start(PipelineConfig {
            start: StartFrame {
                enable_usage_metrics: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    (proc, owner, worker)
}
#[tokio::test]
async fn pipeline_emits_usage_before_final_metadata_vad_and_end() {
    let mut p = peer().await;
    let (_, _, mut worker) = pipeline(&p).await;
    let mut c = connection(&mut p).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::SttMetadata(SttMetadataFrame {
            ttfs_p99_latency: 0.81,
            ..
        })
    ));
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Start(_)
    ));
    send(&worker, FrameData::InputAudio(pcm(3200))).await;
    assert!(matches!(wire(&mut c).await, Message::Binary(_)));
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::InputAudio(_)
    ));
    emit(
        &c,
        json!({"type":"transcript","text":"interim","is_final":false}),
    )
    .await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::InterimTranscription(_)
    ));
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.1,
                timestamp: 1.0,
            })),
            Direction::Upstream,
        )
        .await
        .unwrap();
    assert_eq!(wire(&mut c).await, Message::Text("finalize".into()));
    next(&mut worker).await;
    emit(
        &c,
        json!({"type":"transcript","text":"hello","is_final":true,"language":"en"}),
    )
    .await;
    let FrameData::Metrics(metrics) = next(&mut worker).await.frame.data else {
        panic!()
    };
    assert!(matches!(&metrics[0],Metric::SttUsage{audio_seconds,..} if *audio_seconds==0.1));
    let FrameData::Transcription(t) = next(&mut worker).await.frame.data else {
        panic!()
    };
    assert_eq!(t.user_id, "speaker");
    assert!(!t.finalized);
    assert_eq!(t.language.as_deref(), Some("en"));
    assert!(t.timestamp.ends_with("+00:00"));
    worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut c).await, Message::Text("done".into()));
    c.emit.send(Message::Close(None)).await.unwrap();
    let frames = drain(&mut worker).await;
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::End(_))));
    assert!(worker.join().await.unwrap().errors.is_empty());
}
#[tokio::test]
async fn active_noop_rejection_and_explicit_restart_preserve_admission() {
    let mut p = peer().await;
    let (proc, owner, mut worker) = pipeline(&p).await;
    let mut old = connection(&mut p).await;
    next(&mut worker).await;
    next(&mut worker).await;
    let health = proc.health().unwrap();
    let before = health.generation();
    let config = proc.config_snapshot().unwrap();
    proc.update_settings(&SttSettings::default()).unwrap();
    assert!(proc
        .update_settings(&delta(json!({"model":"ink-2"})))
        .is_err());
    assert!(proc
        .update_settings(&delta(json!({"keyterm":["ineffective"]})))
        .is_err());
    assert_eq!(proc.config_snapshot().unwrap(), config);
    assert_eq!(health.generation(), before);
    assert!(p.connections.try_recv().is_err());
    let ctx = owner.ctx.lock().unwrap().clone().unwrap();
    assert!(proc
        .restart_with_settings(&delta(json!({"model":null})), &ctx)
        .await
        .is_err());
    assert_eq!(proc.config_snapshot().unwrap(), config);
    send(&worker, FrameData::InputAudio(pcm(3200))).await;
    assert!(matches!(wire(&mut old).await, Message::Binary(_)));
    next(&mut worker).await;
    proc.restart_with_settings(
        &delta(json!({"model":"ink-2","keyterm":["new phrase"]})),
        &ctx,
    )
    .await
    .unwrap();
    let mut new = connection(&mut p).await;
    assert!(new
        .request
        .uri()
        .query()
        .unwrap()
        .contains("keyterm=new%20phrase"));
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    assert!(new.wire.try_recv().is_err());
    emit(
        &new,
        json!({"type":"transcript","text":"new","is_final":true}),
    )
    .await;
    let FrameData::Transcription(t) = next(&mut worker).await.frame.data else {
        panic!()
    };
    assert_eq!(t.text, "new"); // no old audio usage was replayed
    worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut new).await, Message::Text("done".into()));
    new.emit.send(Message::Close(None)).await.unwrap();
    drain(&mut worker).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
    assert!(proc
        .restart_with_settings(&SttSettings::default(), &ctx)
        .await
        .is_err());
}
#[tokio::test]
async fn health_restoration_fences_old_auth_error_and_transcript() {
    let mut p = peer().await;
    let (proc, owner, mut worker) = pipeline(&p).await;
    let old = connection(&mut p).await;
    next(&mut worker).await;
    next(&mut worker).await;
    let health = proc.health().unwrap();
    let before = health.generation();
    health.set_usable(true);
    assert_ne!(health.generation(), before);
    emit(
        &old,
        json!({"type":"error","status_code":401,"message":"stale"}),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(health.is_usable());
    let ctx = owner.ctx.lock().unwrap().clone().unwrap();
    proc.restart_with_settings(&SttSettings::default(), &ctx)
        .await
        .unwrap();
    let mut new = connection(&mut p).await;
    emit(&new, json!({"type":"transcript","text":"current"})).await;
    let FrameData::InterimTranscription(t) = next(&mut worker).await.frame.data else {
        panic!()
    };
    assert_eq!(t.text, "current");
    assert!(health.is_usable());
    worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut new).await, Message::Text("done".into()));
    new.emit.send(Message::Close(None)).await.unwrap();
    drain(&mut worker).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}
#[tokio::test]
async fn mute_retires_old_socket_and_unmute_opens_fresh_without_pcm_replay() {
    let mut p = peer().await;
    let (_, _, mut worker) = pipeline(&p).await;
    let mut old = connection(&mut p).await;
    next(&mut worker).await;
    next(&mut worker).await;
    send(&worker, FrameData::SttMute(true)).await;
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::SttMute(true)
    ));
    send(&worker, FrameData::InputAudio(pcm(3200))).await;
    next(&mut worker).await;
    send(&worker, FrameData::SttMute(false)).await;
    let mut new = connection(&mut p).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::SttMute(false)
    ));
    assert!(new.wire.try_recv().is_err());
    worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut new).await, Message::Text("done".into()));
    new.emit.send(Message::Close(None)).await.unwrap();
    drain(&mut worker).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[tokio::test]
async fn restart_rejects_foreign_and_stale_context_without_retiring_owner() {
    let mut p = peer().await;
    let (proc, owner, mut worker) = pipeline(&p).await;
    let mut live = connection(&mut p).await;
    next(&mut worker).await;
    next(&mut worker).await;
    let mut foreign_peer = peer().await;
    let (_, foreign_owner, mut foreign_worker) = pipeline(&foreign_peer).await;
    let mut foreign_wire = connection(&mut foreign_peer).await;
    next(&mut foreign_worker).await;
    next(&mut foreign_worker).await;
    let foreign = foreign_owner.ctx.lock().unwrap().clone().unwrap();
    let before = proc.config_snapshot().unwrap();
    let health = proc.health().unwrap().generation();
    assert!(
        proc.restart_with_settings(&delta(json!({"model":"ink-2"})), &foreign)
            .await
            .is_err(),
        "foreign context must not retire the live owner"
    );
    assert_eq!(proc.config_snapshot().unwrap(), before);
    assert_eq!(proc.health().unwrap().generation(), health);
    assert!(p.connections.try_recv().is_err());
    send(&worker, FrameData::Text(TextFrame::new("capture"))).await;
    next(&mut worker).await;
    let stale = owner.invocation.lock().unwrap().clone().unwrap();
    send(&worker, FrameData::Interruption).await;
    next(&mut worker).await;
    assert!(!stale.is_current());
    assert!(proc
        .restart_with_settings(&delta(json!({"model":"ink-2"})), &stale)
        .await
        .is_err());
    assert_eq!(proc.config_snapshot().unwrap(), before);
    send(&worker, FrameData::InputAudio(pcm(2))).await;
    assert!(matches!(wire(&mut live).await, Message::Binary(_)));
    next(&mut worker).await;
    worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut live).await, Message::Text("done".into()));
    live.emit.send(Message::Close(None)).await.unwrap();
    drain(&mut worker).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
    foreign_worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut foreign_wire).await, Message::Text("done".into()));
    foreign_wire.emit.send(Message::Close(None)).await.unwrap();
    drain(&mut foreign_worker).await;
    assert!(foreign_worker.join().await.unwrap().errors.is_empty());
}
#[tokio::test]
async fn replacement_failure_returns_error_and_requires_new_explicit_attempt() {
    let mut p = peer().await;
    let (proc, owner, mut worker) = pipeline(&p).await;
    let mut old = connection(&mut p).await;
    next(&mut worker).await;
    next(&mut worker).await;
    let ctx = owner.ctx.lock().unwrap().clone().unwrap();
    p.reject_next.store(503, Ordering::SeqCst);
    let error = proc
        .restart_with_settings(&delta(json!({"model":"ink-2"})), &ctx)
        .await
        .unwrap_err();
    assert!(error.message.contains("503"));
    assert!(!error.message.contains("private-test-key"));
    assert!(proc.health().unwrap().is_usable());
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Error(ErrorFrame {
            category: ErrorCategory::Server,
            ..
        })
    ));
    send(&worker, FrameData::InputAudio(pcm(3200))).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Error(_)
    ));
    assert!(p.connections.try_recv().is_err());
    proc.restart_with_settings(&SttSettings::default(), &ctx)
        .await
        .unwrap();
    let mut new = connection(&mut p).await;
    assert!(new.wire.try_recv().is_err());
    worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut new).await, Message::Text("done".into()));
    new.emit.send(Message::Close(None)).await.unwrap();
    drain(&mut worker).await;
    assert_eq!(worker.join().await.unwrap().errors.len(), 2);
}
#[tokio::test]
async fn handshake_auth_failure_disables_health_but_503_does_not() {
    for (status, usable) in [(401, false), (503, true)] {
        let p = peer().await;
        p.reject_next.store(status, Ordering::SeqCst);
        let proc = Arc::new(CartesiaSttProcessor::new(
            CartesiaSttClient::new(p.config.clone()).unwrap(),
        ));
        let health = proc.health().unwrap();
        let result = Pipeline::new(vec![proc])
            .start(PipelineConfig::default())
            .await;
        let mut worker = result.unwrap();
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::Error(_)
        ));
        assert_eq!(health.is_usable(), usable);
        worker.cancel("cleanup").await.unwrap();
        drain(&mut worker).await;
        let _ = worker.join().await;
    }
}

#[tokio::test]
async fn restart_while_muted_is_rejected_without_configuration_change() {
    let mut p = peer().await;
    let (proc, owner, mut worker) = pipeline(&p).await;
    let mut old = connection(&mut p).await;
    next(&mut worker).await;
    next(&mut worker).await;
    send(&worker, FrameData::SttMute(true)).await;
    next(&mut worker).await;
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    let ctx = owner.ctx.lock().unwrap().clone().unwrap();
    let before = proc.config_snapshot().unwrap();
    let health = proc.health().unwrap().generation();
    assert!(proc
        .restart_with_settings(&delta(json!({"model":"ink-2"})), &ctx)
        .await
        .is_err());
    assert_eq!(proc.config_snapshot().unwrap(), before);
    assert_eq!(proc.health().unwrap().generation(), health);
    assert!(p.connections.try_recv().is_err());
    worker.sender().end().await.unwrap();
    drain(&mut worker).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}
#[tokio::test]
async fn end_and_cancel_abort_pending_replacement_handshake_promptly() {
    for cancel in [false, true] {
        let mut p = peer().await;
        p.config.limits.connect_timeout = Duration::from_secs(3);
        let (proc, owner, mut worker) = pipeline(&p).await;
        let mut old = connection(&mut p).await;
        next(&mut worker).await;
        next(&mut worker).await;
        let ctx = owner.ctx.lock().unwrap().clone().unwrap();
        p.hold_next.store(true, Ordering::SeqCst);
        let restart_proc = proc.clone();
        let restart_ctx = ctx.clone();
        let task = tokio::spawn(async move {
            restart_proc
                .restart_with_settings(&delta(json!({"model":"ink-2"})), &restart_ctx)
                .await
        });
        let held = within(p.holds.recv()).await.unwrap();
        assert!(matches!(wire(&mut old).await, Message::Close(_)));
        if cancel {
            worker.cancel("cancel pending handshake").await.unwrap();
        } else {
            worker.sender().end().await.unwrap();
        }
        let result = tokio::time::timeout(Duration::from_millis(300), task)
            .await
            .expect("terminal did not cancel pending handshake")
            .unwrap();
        assert!(result.is_err());
        drop(held);
        drain(&mut worker).await;
        let report = worker.join().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(proc
            .restart_with_settings(&SttSettings::default(), &ctx)
            .await
            .is_err());
    }
}
#[tokio::test]
async fn stop_drains_trailing_duration_and_cancel_discards_it() {
    for cancel in [false, true] {
        let mut p = peer().await;
        let (_, _, mut worker) = pipeline(&p).await;
        let mut c = connection(&mut p).await;
        next(&mut worker).await;
        next(&mut worker).await;
        send(&worker, FrameData::InputAudio(pcm(3200))).await;
        assert!(matches!(wire(&mut c).await, Message::Binary(_)));
        next(&mut worker).await;
        if cancel {
            worker.cancel("drop pending recognition").await.unwrap();
            assert!(matches!(wire(&mut c).await, Message::Close(_)));
        } else {
            send(&worker, FrameData::Stop).await;
            assert_eq!(wire(&mut c).await, Message::Text("done".into()));
            c.emit.send(Message::Close(None)).await.unwrap();
        }
        let frames = drain(&mut worker).await;
        let usage = frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::Metrics(_)))
            .count();
        assert_eq!(usage, if cancel { 0 } else { 1 });
        assert!(worker.join().await.unwrap().errors.is_empty());
    }
}

#[tokio::test]
async fn restoration_and_interruption_retire_pending_handshake_and_clear_connecting() {
    for restore in [false, true] {
        let mut p = peer().await;
        p.config.limits.connect_timeout = Duration::from_secs(3);
        let (proc, owner, mut worker) = pipeline(&p).await;
        let mut old = connection(&mut p).await;
        next(&mut worker).await;
        next(&mut worker).await;
        send(&worker, FrameData::Text(TextFrame::new("capture"))).await;
        next(&mut worker).await;
        let ctx = if restore {
            owner.ctx.lock().unwrap().clone().unwrap()
        } else {
            owner.invocation.lock().unwrap().clone().unwrap()
        };
        p.hold_next.store(true, Ordering::SeqCst);
        let restart_proc = proc.clone();
        let task = tokio::spawn(async move {
            restart_proc
                .restart_with_settings(&delta(json!({"model":"ink-2"})), &ctx)
                .await
        });
        let held = within(p.holds.recv()).await.unwrap();
        assert!(matches!(wire(&mut old).await, Message::Close(_)));
        if restore {
            proc.health().unwrap().set_usable(true);
        } else {
            send(&worker, FrameData::Interruption).await;
            next(&mut worker).await;
        }
        assert!(tokio::time::timeout(Duration::from_millis(300), task)
            .await
            .expect("retired handshake did not cancel")
            .unwrap()
            .is_err());
        drop(held);
        // A failed/retired connect is dormant, not permanently marked connecting.
        proc.update_settings(&delta(json!({"language":"fr"})))
            .unwrap();
        assert_eq!(proc.config_snapshot().unwrap().settings.language, "fr");
        let live = owner.ctx.lock().unwrap().clone().unwrap();
        proc.restart_with_settings(&SttSettings::default(), &live)
            .await
            .unwrap();
        let mut fresh = connection(&mut p).await;
        assert!(fresh.wire.try_recv().is_err());
        worker.sender().end().await.unwrap();
        assert_eq!(wire(&mut fresh).await, Message::Text("done".into()));
        fresh.emit.send(Message::Close(None)).await.unwrap();
        drain(&mut worker).await;
        assert!(worker.join().await.unwrap().errors.is_empty());
    }
}

#[test]
fn extreme_limits_fail_validation_instead_of_panicking_or_allocating() {
    let mut config = CartesiaSttConfig::new("key");
    config.limits.queue_capacity = usize::MAX;
    assert!(CartesiaSttClient::new(config).is_err());
    let mut config = CartesiaSttConfig::new("key");
    config.limits.response_timeout = Duration::MAX;
    assert!(CartesiaSttClient::new(config).is_err());
    let mut config = CartesiaSttConfig::new("key");
    config.limits.max_message_bytes = usize::MAX;
    assert!(CartesiaSttClient::new(config).is_err());
}
#[tokio::test]
async fn surviving_background_task_cannot_reuse_stale_restart_invocation() {
    let mut p = peer().await;
    let (proc, owner, mut worker) = pipeline(&p).await;
    let mut wire_peer = connection(&mut p).await;
    next(&mut worker).await;
    next(&mut worker).await;
    send(&worker, FrameData::Text(TextFrame::new("capture"))).await;
    next(&mut worker).await;
    let stale = owner.invocation.lock().unwrap().clone().unwrap();
    send(&worker, FrameData::Interruption).await;
    next(&mut worker).await;
    assert!(!stale.invocation_fence().unwrap().is_current());
    let live = owner.ctx.lock().unwrap().clone().unwrap();
    let request = proc.clone();
    let before = proc.config_snapshot().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();
    live.spawn("attempt-stale-restart", false, async move {
        assert!(
            stale.is_current(),
            "test requires the noninterruptible task-local override"
        );
        tx.send(
            request
                .restart_with_settings(&delta(json!({"model":"ink-2"})), &stale)
                .await
                .is_err(),
        )
        .unwrap();
        Ok(())
    })
    .unwrap();
    assert!(within(rx).await.unwrap());
    assert_eq!(proc.config_snapshot().unwrap(), before);
    assert!(p.connections.try_recv().is_err());
    worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut wire_peer).await, Message::Text("done".into()));
    wire_peer.emit.send(Message::Close(None)).await.unwrap();
    drain(&mut worker).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}
#[tokio::test]
async fn same_true_restoration_retires_an_idle_socket_without_keepalive_traffic() {
    let mut p = peer().await;
    let (proc, owner, mut worker) = pipeline(&p).await;
    let mut old = connection(&mut p).await;
    next(&mut worker).await;
    next(&mut worker).await;
    proc.health().unwrap().set_usable(true);
    let result = tokio::time::timeout(Duration::from_secs(2), old.wire.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, Message::Close(_)));
    proc.update_settings(&delta(json!({"model":"ink-2"})))
        .unwrap();
    let ctx = owner.ctx.lock().unwrap().clone().unwrap();
    proc.restart_with_settings(&SttSettings::default(), &ctx)
        .await
        .unwrap();
    let mut current = connection(&mut p).await;
    worker.sender().end().await.unwrap();
    assert_eq!(wire(&mut current).await, Message::Text("done".into()));
    current.emit.send(Message::Close(None)).await.unwrap();
    drain(&mut worker).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}
