//! Processor ownership and committed-turn controls with a literal loopback peer.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
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
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
#[allow(clippy::result_large_err)]
async fn peer() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaTurnsConfig::new("processor-test-key");
    config.endpoint = format!(
        "ws://{}/stt/turns/websocket",
        listener.local_addr().unwrap()
    );
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.write_timeout = Duration::from_secs(1);
    config.limits.response_timeout = Duration::from_secs(1);
    config.limits.close_timeout = Duration::from_millis(50);
    let (tx, connections) = mpsc::channel(16);
    let task = tokio::spawn(async move {
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let Ok((stream, _)) = accepted else { return; };
                    let tx = tx.clone();
                    tasks.spawn(async move {
                        let request = Arc::new(Mutex::new(None));
                        let capture = request.clone();
                        let Ok(mut socket) = accept_hdr_async(stream, move |r: &Request, response: Response| {
                            *capture.lock().unwrap() = Some(r.clone());
                            Ok(response)
                        }).await else { return; };
                        let request = request.lock().unwrap().take().unwrap();
                        let (wire_tx, wire) = mpsc::channel(64);
                        let (emit, mut events) = mpsc::channel(64);
                        if tx.send(Connection { request, wire, emit }).await.is_err() { return; }
                        loop {
                            tokio::select! {
                                item = socket.next() => match item {
                                    Some(Ok(message)) => {
                                        let closed = matches!(message, Message::Close(_));
                                        if wire_tx.send(message).await.is_err() || closed {
                                            let _ = socket.flush().await;
                                            return;
                                        }
                                    },
                                    _ => return,
                                },
                                item = events.recv() => match item {
                                    Some(message) => if socket.send(message).await.is_err() { return; },
                                    None => return,
                                },
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
    }
}
async fn within<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("processor test deadline")
}
async fn connection(peer: &mut Peer) -> Connection {
    within(peer.connections.recv()).await.unwrap()
}
async fn wire(c: &mut Connection) -> Message {
    within(c.wire.recv()).await.unwrap()
}
async fn emit(c: &Connection, value: Value) {
    c.emit
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
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
fn pcm(bytes: usize) -> AudioRawFrame {
    AudioRawFrame::new(vec![1; bytes], 16000, 1).unwrap()
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
async fn through_marker(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let marker = Frame::text("processor-barrier");
    let id = marker.id;
    worker
        .sender()
        .send(marker, Direction::Downstream)
        .await
        .unwrap();
    let mut frames = vec![];
    loop {
        let frame = next(worker).await;
        let done = frame.frame.id == id;
        frames.push(frame);
        if done {
            return frames;
        }
    }
}
async fn initial(worker: &mut PipelineWorker, interrupts: bool) {
    assert!(matches!(next(worker).await.frame.data, FrameData::Start(_)));
    let down = next(worker).await;
    let up = next(worker).await;
    assert_eq!(down.direction, Direction::Downstream);
    assert_eq!(up.direction, Direction::Upstream);
    assert_ne!(down.frame.id, up.frame.id);
    assert_eq!(down.frame.broadcast_sibling_id, Some(up.frame.id));
    assert_eq!(up.frame.broadcast_sibling_id, Some(down.frame.id));
    for frame in [down, up] {
        let FrameData::SttMetadata(metadata) = frame.frame.data else {
            panic!("metadata missing");
        };
        assert_eq!(metadata.ttfs_p99_latency, 0.0);
        assert_eq!(metadata.base.service_name, "CartesiaTurnsSTTService");
        assert_eq!(
            metadata.base.user_turn_strategies,
            Some(UserTurnStrategies {
                start: Some(vec![UserTurnStrategy::External {
                    enable_interruptions: interrupts
                }]),
                stop: Some(vec![UserTurnStrategy::External {
                    enable_interruptions: interrupts
                }]),
            })
        );
    }
}

#[derive(Clone)]
struct Owner {
    proc: Arc<CartesiaTurnsProcessor>,
    ctx: Arc<Mutex<Option<ProcessorContext>>>,
    invocation: Arc<Mutex<Option<ProcessorContext>>>,
}
#[async_trait]
impl FrameProcessor for Owner {
    fn name(&self) -> &str {
        "CartesiaTurnsSTTService"
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
async fn pipeline(
    p: &mut Peer,
    passthrough: bool,
    interrupts: bool,
) -> (
    Arc<CartesiaTurnsProcessor>,
    Owner,
    PipelineWorker,
    Connection,
) {
    let proc = Arc::new(
        CartesiaTurnsProcessor::new(CartesiaTurnsClient::new(p.config.clone()).unwrap())
            .with_user_id("speaker")
            .unwrap()
            .with_audio_passthrough(passthrough)
            .with_should_interrupt(interrupts),
    );
    let owner = Owner {
        proc: proc.clone(),
        ctx: Arc::default(),
        invocation: Arc::default(),
    };
    let owned = owner.clone();
    let task = tokio::spawn(async move {
        Pipeline::new(vec![Arc::new(owned)])
            .start(PipelineConfig {
                start: StartFrame {
                    enable_usage_metrics: true,
                    ..Default::default()
                },
                ..Default::default()
            })
            .await
            .unwrap()
    });
    let c = connection(p).await;
    emit(
        &c,
        json!({"type":"connected","request_id":"processor-session"}),
    )
    .await;
    (proc, owner, within(task).await.unwrap(), c)
}
async fn finish(worker: &mut PipelineWorker, c: &mut Connection) -> Vec<DirectedFrame> {
    worker.sender().end().await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(wire(c).await.to_text().unwrap()).unwrap(),
        json!({"type":"close"})
    );
    c.emit.send(Message::Close(None)).await.unwrap();
    drain(worker).await
}

#[tokio::test]
async fn metadata_and_committed_turn_order_preserve_raw_result_and_siblings() {
    let mut p = peer().await;
    let (_, _, mut worker, mut c) = pipeline(&mut p, true, false).await;
    initial(&mut worker, false).await;
    send(&worker, FrameData::InputAudio(pcm(3200))).await;
    assert_eq!(wire(&mut c).await.into_data().to_vec(), vec![1; 3200]);
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::InputAudio(_)
    ));
    for event in [
        json!({"type":"turn.start","transcript":"not a final"}),
        json!({"type":"turn.update","transcript":"hello"}),
        json!({"type":"turn.update","transcript":"hello world","is_final":true}),
        json!({"type":"turn.eager_end","transcript":"not committed"}),
        json!({"type":"turn.resume"}),
        json!({"type":"turn.end","transcript":"Hello world.","language":"fr","extra":17}),
    ] {
        emit(&c, event).await;
    }
    let mut frames = vec![];
    let mut stops = 0;
    while stops < 2 {
        let frame = next(&mut worker).await;
        if matches!(frame.frame.data, FrameData::ProposedUserStoppedSpeaking) {
            stops += 1;
        }
        frames.push(frame);
    }
    let starts: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::ProposedUserStartedSpeaking))
        .collect();
    assert_eq!(starts.len(), 2);
    assert!(starts.iter().all(|f| f.frame.is_system()));
    assert_eq!(
        starts[0].frame.broadcast_sibling_id,
        Some(starts[1].frame.id)
    );
    let interims: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::InterimTranscription(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(interims, ["hello", "hello world"]);
    let finals: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::Transcription(t) => Some(t),
            _ => None,
        })
        .collect();
    assert_eq!(finals.len(), 1);
    assert_eq!(finals[0].text, "Hello world.");
    assert_eq!(finals[0].language.as_deref(), Some("en"));
    assert_eq!(finals[0].user_id, "speaker");
    assert!(finals[0].finalized);
    assert!(finals[0].timestamp.ends_with("+00:00"));
    assert_eq!(finals[0].result.as_ref().unwrap()["extra"], 17);
    let usage = frames.iter().position(|f| matches!(&f.frame.data, FrameData::Metrics(m) if matches!(&m[0], Metric::SttUsage { audio_seconds, .. } if *audio_seconds == 0.1))).unwrap();
    let final_index = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::Transcription(_)))
        .unwrap();
    let stop_index = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::ProposedUserStoppedSpeaking))
        .unwrap();
    assert!(usage < final_index && final_index < stop_index);
    let stops: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::ProposedUserStoppedSpeaking))
        .collect();
    assert!(stops.iter().all(|f| !f.frame.is_system()));
    assert_eq!(stops[0].frame.broadcast_sibling_id, Some(stops[1].frame.id));
    assert!(finish(&mut worker, &mut c)
        .await
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::End(_))));
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[tokio::test]
async fn empty_end_emits_only_stop_and_graceful_drain_emits_final_without_new_start() {
    let mut p = peer().await;
    let (_, _, mut worker, mut c) = pipeline(&mut p, false, true).await;
    initial(&mut worker, true).await;
    send(&worker, FrameData::InputAudio(pcm(320))).await;
    assert!(matches!(wire(&mut c).await, Message::Binary(_)));
    emit(&c, json!({"type":"turn.end","transcript":""})).await;
    for _ in 0..2 {
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::ProposedUserStoppedSpeaking
        ));
    }
    worker.sender().end().await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(wire(&mut c).await.to_text().unwrap()).unwrap(),
        json!({"type":"close"})
    );
    for event in [
        json!({"type":"turn.start","transcript":"late start"}),
        json!({"type":"turn.update","transcript":"late update"}),
        json!({"type":"turn.eager_end","transcript":"late eager"}),
        json!({"type":"turn.resume"}),
        json!({"type":"turn.end","transcript":"drained final"}),
    ] {
        emit(&c, event).await;
    }
    c.emit.send(Message::Close(None)).await.unwrap();
    let frames = drain(&mut worker).await;
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::ProposedUserStartedSpeaking | FrameData::InterimTranscription(_)
    )));
    let usage = frames.iter().position(|f| matches!(&f.frame.data, FrameData::Metrics(m) if matches!(&m[0], Metric::SttUsage { audio_seconds, .. } if *audio_seconds == 0.01))).unwrap();
    let final_index = frames.iter().position(|f| matches!(&f.frame.data, FrameData::Transcription(t) if t.text == "drained final" && t.finalized)).unwrap();
    let terminal = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::End(_)))
        .unwrap();
    assert!(usage < final_index && final_index < terminal);
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[tokio::test]
async fn invalid_and_noop_deltas_preserve_audio_and_replacement_waits_for_ack() {
    let mut p = peer().await;
    let (proc, owner, mut worker, mut old) = pipeline(&mut p, false, true).await;
    initial(&mut worker, true).await;
    let ctx = owner.ctx.lock().unwrap().clone().unwrap();
    let before = proc.config_snapshot().unwrap();
    let health = proc.health().unwrap().generation();
    for invalid in [
        json!({"model":"new","keyterm":["new"]}),
        json!({"language":"en"}),
        json!({"turn_end_threshold":0.0}),
        json!({"keyterm":["new"],"unknown":1}),
    ] {
        assert!(proc.update_settings(&delta(invalid.clone())).is_err());
        assert!(proc
            .restart_with_settings(&delta(invalid), &ctx)
            .await
            .is_err());
        assert_eq!(proc.config_snapshot().unwrap(), before);
        assert_eq!(proc.health().unwrap().generation(), health);
    }
    proc.update_settings(&SttSettings::default()).unwrap();
    proc.restart_with_settings(&SttSettings::default(), &ctx)
        .await
        .unwrap();
    assert!(proc
        .update_settings(&delta(json!({"keyterm":["new"]})))
        .is_err());
    send(&worker, FrameData::InputAudio(pcm(640))).await;
    assert_eq!(wire(&mut old).await.into_data().to_vec(), vec![1; 640]);
    assert!(p.connections.try_recv().is_err());
    let replacement = {
        let proc = proc.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            proc.restart_with_settings(
                &delta(json!({"keyterm":["new phrase"],"turn_end_timeout_ms":640})),
                &ctx,
            )
            .await
        })
    };
    let mut new = connection(&mut p).await;
    assert!(new
        .request
        .uri()
        .query()
        .unwrap()
        .contains("keyterm=new%20phrase"));
    assert!(new
        .request
        .uri()
        .query()
        .unwrap()
        .contains("turn_end_timeout_ms=640"));
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    new.emit.send(Message::Ping(vec![4].into())).await.unwrap();
    assert_eq!(wire(&mut new).await, Message::Pong(vec![4].into()));
    assert!(!replacement.is_finished());
    assert!(new.wire.try_recv().is_err());
    emit(&new, json!({"type":"connected"})).await;
    within(replacement).await.unwrap().unwrap();
    emit(&new, json!({"type":"turn.end","transcript":"replacement"})).await;
    assert!(
        matches!(
            next(&mut worker).await.frame.data,
            FrameData::Transcription(_)
        ),
        "old audio usage must not replay"
    );
    for _ in 0..2 {
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::ProposedUserStoppedSpeaking
        ));
    }
    finish(&mut worker, &mut new).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
    assert!(proc
        .restart_with_settings(&delta(json!({"keyterm":["third"]})), &ctx)
        .await
        .is_err());
}

#[tokio::test]
async fn interruption_preserves_listening_but_stale_and_foreign_invocations_cannot_restart_or_submit_audio(
) {
    let mut p = peer().await;
    let (proc, owner, mut worker, mut c) = pipeline(&mut p, false, true).await;
    initial(&mut worker, true).await;
    let mut foreign_peer = peer().await;
    let (_, foreign_owner, mut foreign_worker, mut foreign_c) =
        pipeline(&mut foreign_peer, false, true).await;
    initial(&mut foreign_worker, true).await;
    let foreign = foreign_owner.ctx.lock().unwrap().clone().unwrap();
    assert!(proc
        .restart_with_settings(&delta(json!({"keyterm":["foreign"]})), &foreign)
        .await
        .is_err());
    through_marker(&mut worker).await;
    let stale = owner.invocation.lock().unwrap().clone().unwrap();
    send(&worker, FrameData::Interruption).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    assert!(proc
        .process(
            Frame::new(FrameData::InputAudio(pcm(160))),
            Direction::Downstream,
            &stale
        )
        .await
        .is_err());
    assert!(proc
        .restart_with_settings(&delta(json!({"keyterm":["stale"]})), &stale)
        .await
        .is_err());
    let (answer, result) = tokio::sync::oneshot::channel();
    let retained = stale.clone();
    let owned = proc.clone();
    stale
        .spawn("surviving-stale-owner", false, async move {
            let outcome = owned
                .restart_with_settings(&delta(json!({"keyterm":["laundered"]})), &retained)
                .await;
            let _ = answer.send(outcome.is_err());
            Ok(())
        })
        .unwrap();
    assert!(within(result).await.unwrap());
    send(&worker, FrameData::InputAudio(pcm(320))).await;
    assert_eq!(wire(&mut c).await.into_data().to_vec(), vec![1; 320]);
    emit(
        &c,
        json!({"type":"turn.end","transcript":"still listening"}),
    )
    .await;
    let mut final_seen = false;
    for _ in 0..4 {
        final_seen |= matches!(&next(&mut worker).await.frame.data, FrameData::Transcription(t) if t.text == "still listening");
    }
    assert!(final_seen);
    assert!(p.connections.try_recv().is_err());
    finish(&mut worker, &mut c).await;
    finish(&mut foreign_worker, &mut foreign_c).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
    assert!(foreign_worker.join().await.unwrap().errors.is_empty());
}

#[tokio::test]
async fn mute_retires_socket_and_audio_routes_without_finalize_or_replay() {
    let mut p = peer().await;
    let (_, _, mut worker, mut old) = pipeline(&mut p, false, true).await;
    initial(&mut worker, true).await;
    send(&worker, FrameData::SttMute(true)).await;
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::SttMute(true)
    ));
    send(&worker, FrameData::InputAudio(pcm(320))).await;
    // SttMute is system priority. Establish that normal audio was consumed
    // while muted before unmute is allowed to overtake the normal lane.
    through_marker(&mut worker).await;
    send(&worker, FrameData::SttMute(false)).await;
    let mut new = connection(&mut p).await;
    emit(&new, json!({"type":"connected"})).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::SttMute(false)
    ));
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(pcm(640))),
            Direction::Upstream,
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::InputAudio(_)
    ));
    send(&worker, FrameData::OutputAudio(pcm(640))).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::OutputAudio(_)
    ));
    send(
        &worker,
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.1,
            timestamp: 1.0,
        }),
    )
    .await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::VadUserStoppedSpeaking(_)
    ));
    new.emit.send(Message::Ping(vec![9].into())).await.unwrap();
    assert_eq!(
        wire(&mut new).await,
        Message::Pong(vec![9].into()),
        "no replay, output audio or finalize may reach provider"
    );
    send(
        &worker,
        FrameData::UserAudio(UserAudioRawFrame {
            audio: pcm(320),
            user_id: "speaker".into(),
        }),
    )
    .await;
    assert_eq!(wire(&mut new).await.into_data().to_vec(), vec![1; 320]);
    finish(&mut worker, &mut new).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[tokio::test]
async fn same_value_health_restoration_fences_events_pcm_and_noop_restart() {
    let mut p = peer().await;
    let (proc, owner, mut worker, mut old) = pipeline(&mut p, false, true).await;
    initial(&mut worker, true).await;
    let health = proc.health().unwrap();
    let generation = health.generation();
    health.set_usable(true);
    assert_ne!(health.generation(), generation);
    // Peer traffic prompts immediate guard evaluation rather than a timer wait.
    emit(&old, json!({"type":"turn.end","transcript":"retired"})).await;
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    let ctx = owner.ctx.lock().unwrap().clone().unwrap();
    proc.restart_with_settings(&SttSettings::default(), &ctx)
        .await
        .unwrap();
    assert!(p.connections.try_recv().is_err());
    let frames = through_marker(&mut worker).await;
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::Transcription(_)
            | FrameData::Metrics(_)
            | FrameData::ProposedUserStoppedSpeaking
            | FrameData::Error(_)
    )));
    let replacement = {
        let proc = proc.clone();
        let ctx = ctx.clone();
        tokio::spawn(async move {
            proc.restart_with_settings(&delta(json!({"keyterm":["recovered"]})), &ctx)
                .await
        })
    };
    let mut new = connection(&mut p).await;
    emit(&new, json!({"type":"connected"})).await;
    within(replacement).await.unwrap().unwrap();
    send(&worker, FrameData::InputAudio(pcm(320))).await;
    assert!(matches!(wire(&mut new).await, Message::Binary(_)));
    finish(&mut worker, &mut new).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[tokio::test]
async fn requested_metadata_has_fresh_siblings_and_preserves_request_route() {
    let mut p = peer().await;
    let (_, _, mut worker, mut c) = pipeline(&mut p, false, false).await;
    initial(&mut worker, false).await;
    let request = Frame::new(FrameData::ServiceSwitcherRequestMetadata(
        ProcessorReference::Name("CartesiaTurnsSTTService".into()),
    ));
    let request_id = request.id;
    worker
        .sender()
        .send(request, Direction::Upstream)
        .await
        .unwrap();
    let down = next(&mut worker).await;
    let up = next(&mut worker).await;
    assert!(matches!(down.frame.data, FrameData::SttMetadata(_)));
    assert!(matches!(up.frame.data, FrameData::SttMetadata(_)));
    assert_ne!(down.frame.id, up.frame.id);
    assert_eq!(down.frame.broadcast_sibling_id, Some(up.frame.id));
    let forwarded = next(&mut worker).await;
    assert_eq!(forwarded.frame.id, request_id);
    assert_eq!(forwarded.direction, Direction::Upstream);
    finish(&mut worker, &mut c).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}

struct NoopObserver;
#[async_trait]
impl PipelineObserver for NoopObserver {
    async fn on_event(&self, _: ObservationEvent) {}
}
struct RestoringClock {
    health: ProcessorHealth,
    remaining: AtomicU64,
    ticks: AtomicU64,
}
impl ObserverClock for RestoringClock {
    fn now_ns(&self) -> u64 {
        if self
            .remaining
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
                (n > 0).then(|| n - 1)
            })
            == Ok(1)
        {
            self.health.set_usable(true);
        }
        self.ticks.fetch_add(1, Ordering::SeqCst)
    }
}

#[tokio::test]
async fn observer_restoration_at_usage_final_or_stop_suppresses_every_later_segment() {
    for restore_at in 1..=3 {
        let mut p = peer().await;
        let proc = Arc::new(
            CartesiaTurnsProcessor::new(CartesiaTurnsClient::new(p.config.clone()).unwrap())
                .with_audio_passthrough(false),
        );
        let health = proc.health().unwrap();
        let clock = Arc::new(RestoringClock {
            health: health.clone(),
            remaining: AtomicU64::new(0),
            ticks: AtomicU64::new(0),
        });
        let pipeline = Pipeline::new(vec![proc])
            .with_observer(Arc::new(NoopObserver), ObserverOptions::default())
            .with_observer_clock(clock.clone());
        let task = tokio::spawn(async move {
            pipeline
                .start(PipelineConfig {
                    start: StartFrame {
                        enable_usage_metrics: true,
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
                .unwrap()
        });
        let mut c = connection(&mut p).await;
        emit(&c, json!({"type":"connected"})).await;
        let mut worker = within(task).await.unwrap();
        initial(&mut worker, true).await;
        send(&worker, FrameData::InputAudio(pcm(320))).await;
        assert!(matches!(wire(&mut c).await, Message::Binary(_)));
        through_marker(&mut worker).await;
        let before = health.generation();
        // On this single-processor/current-thread worker, the next three clock
        // invocations are usage, final and downstream stop publication. No
        // await separates those emissions. The clock changes health inside the
        // selected admission, and every subsequent segment must be fenced.
        clock.remaining.store(restore_at, Ordering::SeqCst);
        emit(
            &c,
            json!({"type":"turn.end","transcript":"partially published"}),
        )
        .await;
        assert!(matches!(wire(&mut c).await, Message::Close(_)));
        let frames = through_marker(&mut worker).await;
        assert_eq!(clock.remaining.load(Ordering::SeqCst), 0);
        assert_ne!(health.generation(), before);
        assert!(health.is_usable());
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::Metrics(_)))
                .count(),
            1
        );
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::Transcription(_)))
                .count(),
            usize::from(restore_at >= 2)
        );
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::ProposedUserStoppedSpeaking))
                .count(),
            usize::from(restore_at >= 3)
        );
        assert!(!frames.iter().any(|f| f.direction == Direction::Upstream
            && matches!(f.frame.data, FrameData::ProposedUserStoppedSpeaking)));
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::Error(_))));
        worker.sender().end().await.unwrap();
        drain(&mut worker).await;
        assert!(worker.join().await.unwrap().errors.is_empty());
    }
}

async fn closed_without_pcm(c: &mut Connection) {
    within(async {
        while let Some(message) = c.wire.recv().await {
            assert!(
                !matches!(message, Message::Binary(_) | Message::Text(_)),
                "retired readiness must never admit PCM or commands"
            );
        }
    })
    .await;
}

#[tokio::test]
async fn same_value_health_restoration_retires_held_ack_and_returns_cancelled_to_owner() {
    let mut p = peer().await;
    let (proc, owner, mut worker, mut old) = pipeline(&mut p, false, true).await;
    initial(&mut worker, true).await;
    let ctx = owner.ctx.lock().unwrap().clone().unwrap();
    let replacement = {
        let proc = proc.clone();
        tokio::spawn(async move {
            proc.restart_with_settings(&delta(json!({"keyterm":["held"]})), &ctx)
                .await
        })
    };
    let mut held = connection(&mut p).await;
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    held.emit.send(Message::Ping(vec![6].into())).await.unwrap();
    assert_eq!(wire(&mut held).await, Message::Pong(vec![6].into()));
    assert!(!replacement.is_finished());
    proc.health().unwrap().set_usable(true);
    emit(
        &held,
        json!({"type":"error","message":"retired provider secret"}),
    )
    .await;
    let error = within(replacement).await.unwrap().unwrap_err();
    assert_eq!(error.message, StreamingError::Cancelled.to_string());
    closed_without_pcm(&mut held).await;
    let frames = through_marker(&mut worker).await;
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::Error(_)
            | FrameData::Transcription(_)
            | FrameData::Metrics(_)
            | FrameData::ProposedUserStartedSpeaking
            | FrameData::ProposedUserStoppedSpeaking
    )));
    // Failed readiness is dormant, not permanently stuck in connecting.
    proc.update_settings(&delta(json!({"keyterm":["dormant"]})))
        .unwrap();
    assert_eq!(
        proc.config_snapshot().unwrap().settings.keyterm,
        ["dormant"]
    );
    assert!(p.connections.try_recv().is_err());
    worker.sender().end().await.unwrap();
    drain(&mut worker).await;
    assert!(worker.join().await.unwrap().errors.is_empty());
}

#[tokio::test]
async fn unmute_reports_current_provider_error_once_and_suppresses_retired_error() {
    for restore in [false, true] {
        let mut p = peer().await;
        let (proc, _, mut worker, mut old) = pipeline(&mut p, false, true).await;
        initial(&mut worker, true).await;
        send(&worker, FrameData::SttMute(true)).await;
        assert!(matches!(wire(&mut old).await, Message::Close(_)));
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::SttMute(true)
        ));
        send(&worker, FrameData::SttMute(false)).await;
        let mut held = connection(&mut p).await;
        held.emit.send(Message::Ping(vec![8].into())).await.unwrap();
        assert_eq!(wire(&mut held).await, Message::Pong(vec![8].into()));
        if restore {
            proc.health().unwrap().set_usable(true);
        }
        emit(&held, json!({"type":"error","message":"provider secret"})).await;
        closed_without_pcm(&mut held).await;
        // System and normal loops are concurrent: socket cleanup alone does
        // not prove unmute has finished reporting or forwarding its outcome.
        let outcome = next(&mut worker).await;
        if restore {
            assert!(matches!(outcome.frame.data, FrameData::SttMute(false)));
        } else {
            assert!(matches!(outcome.frame.data, FrameData::Error(_)));
        }
        let mut frames = vec![outcome];
        frames.extend(through_marker(&mut worker).await);
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                .count(),
            usize::from(!restore)
        );
        for frame in &frames {
            if let FrameData::Error(error) = &frame.frame.data {
                assert!(!error.error.contains("provider secret"));
            }
        }
        assert!(proc.health().unwrap().is_usable());
        worker.sender().end().await.unwrap();
        drain(&mut worker).await;
        assert_eq!(
            worker.join().await.unwrap().errors.len(),
            usize::from(!restore)
        );
    }
}

#[tokio::test]
async fn unhealthy_unmute_preserves_retry_until_fresh_connected_acknowledgement() {
    let mut p = peer().await;
    let (proc, _, mut worker, mut old) = pipeline(&mut p, false, true).await;
    initial(&mut worker, true).await;
    send(&worker, FrameData::SttMute(true)).await;
    assert!(matches!(wire(&mut old).await, Message::Close(_)));
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::SttMute(true)
    ));
    let health = proc.health().unwrap();
    health.set_usable(false);
    let unusable = health.generation();
    send(&worker, FrameData::SttMute(false)).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Error(_)
    ));
    assert!(!health.is_usable());
    assert_eq!(health.generation(), unusable);
    through_marker(&mut worker).await;
    assert!(p.connections.try_recv().is_err());
    health.set_usable(true);
    through_marker(&mut worker).await;
    assert!(
        p.connections.try_recv().is_err(),
        "health restoration must not reconnect"
    );
    send(&worker, FrameData::SttMute(false)).await;
    let mut new = connection(&mut p).await;
    new.emit.send(Message::Ping(vec![7].into())).await.unwrap();
    assert_eq!(wire(&mut new).await, Message::Pong(vec![7].into()));
    assert!(
        worker.try_recv().is_err(),
        "unmute may not finish at HTTP upgrade"
    );
    emit(
        &new,
        json!({"type":"connected","request_id":"healthy-retry"}),
    )
    .await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::SttMute(false)
    ));
    send(&worker, FrameData::InputAudio(pcm(320))).await;
    assert_eq!(wire(&mut new).await, Message::Binary(vec![1; 320].into()));
    finish(&mut worker, &mut new).await;
    assert_eq!(worker.join().await.unwrap().errors.len(), 1);
}
