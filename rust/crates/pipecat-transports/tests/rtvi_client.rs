//! Real loopback coverage for the deliberately narrow, single-run RTVI client.
//! Queue admission is not evidence of socket delivery: outbound assertions below
//! are made only from messages actually read by the remote WebSocket peer.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_transports::rtvi_client::{RtviClientFailure, RtviClientStatus};
use pipecat_transports::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::tungstenite::Message;

const DEADLINE: Duration = Duration::from_secs(3);
const QUIET: Duration = Duration::from_millis(60);

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("operation deadline")
}

// Borrow the handle so a timeout cannot silently detach a live server. An
// observed timeout is a test failure only after cancellation has been joined.
async fn join_server<T>(task: &mut JoinHandle<T>) -> Result<T, &'static str> {
    match tokio::time::timeout(DEADLINE, &mut *task).await {
        Ok(result) => result.map_err(|_| "server task failed"),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("server join deadline")
        }
    }
}

struct Peer {
    endpoint: String,
    send: mpsc::Sender<Message>,
    received: mpsc::Receiver<Message>,
    task: Option<JoinHandle<bool>>,
}
impl Peer {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}/rtvi", listener.local_addr().unwrap());
        let (send, mut commands) = mpsc::channel(256);
        let (observed, received) = mpsc::channel(256);
        let task = tokio::spawn(async move {
            // A failed test cannot leave a detached listener/socket task forever.
            tokio::time::timeout(Duration::from_secs(12), async move {
                let Ok((socket, _)) = listener.accept().await else { return false };
                let Ok(mut ws) = tokio_tungstenite::accept_async(socket).await else { return false };
                loop {
                    tokio::select! {
                        command = commands.recv() => match command {
                            Some(message) => {
                                if ws.send(message).await.is_err() { return true; }
                            }
                            None => { let _ = ws.close(None).await; return false; }
                        },
                        message = ws.next() => match message {
                            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                                let _ = ws.close(None).await;
                                return true;
                            }
                            Some(Ok(message)) => { if observed.send(message).await.is_err() { return false; } }
                        }
                    }
                }
            }).await.unwrap_or(false)
        });
        Self {
            endpoint,
            send,
            received,
            task: Some(task),
        }
    }
    async fn wire(&self, value: Value) {
        bounded(self.send.send(Message::Text(value.to_string().into())))
            .await
            .unwrap();
    }
    async fn observed(&mut self) -> Option<Value> {
        bounded(async {
            loop {
                match self.received.recv().await? {
                    Message::Text(text) => return serde_json::from_str(&text).ok(),
                    _ => continue,
                }
            }
        })
        .await
    }
    async fn finish(mut self) -> bool {
        // Keep command sender alive until the peer has observed actual closure.
        let mut task = self.task.take().expect("peer server handle");
        join_server(&mut task).await.unwrap_or(false)
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            // Bootstrap/assertion-panic fallback only. Drop requests cancellation;
            // it cannot synchronously join. Normal and observed failure paths
            // call finish(), which explicitly joins before assertions.
            task.abort();
        }
    }
}
fn event(kind: &str, data: Value) -> Value {
    json!({"label":"rtvi-ai", "type":kind, "data":data})
}
fn ready() -> Value {
    json!({"label":"rtvi-ai", "type":"bot-ready", "id":"ready", "data":{"version":"2.2.0"}})
}
fn command() -> Frame {
    Frame::new(FrameData::OutputTransportMessage(json!({
        "label":"rtvi-ai", "type":"send-text", "id":"command-1", "data":{"text":"hello"}
    })))
}
async fn start(client: Arc<RtviClientProcessor>) -> PipelineWorker {
    bounded(Pipeline::new(vec![client]).start(PipelineConfig {
        shutdown_timeout: Duration::from_secs(1),
        ..Default::default()
    }))
    .await
    .unwrap()
}
async fn finish(worker: &mut PipelineWorker) -> Result<WorkerReport, PipelineError> {
    let _ = bounded(worker.cancel("test finished")).await;
    bounded(worker.join()).await
}
async fn llm_frames(worker: &mut PipelineWorker, count: usize) -> Vec<FrameData> {
    bounded(async {
        let mut frames = Vec::new();
        while frames.len() < count {
            let Some(item) = worker.recv().await else {
                break;
            };
            if !matches!(item.frame.data, FrameData::Start(_)) {
                frames.push(item.frame.data);
            }
        }
        frames
    })
    .await
}
fn client(peer: &Peer) -> Arc<RtviClientProcessor> {
    Arc::new(RtviClientProcessor::new(RtviClientConfig::new(&peer.endpoint)).unwrap())
}
async fn wait_closed(control: &RtviClientControl) -> bool {
    tokio::time::timeout(DEADLINE, async {
        while !control.is_closed() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok()
}
async fn wait_ready(control: &RtviClientControl) -> bool {
    tokio::time::timeout(DEADLINE, async {
        while !control.is_ready() && !control.is_closed() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        control.is_ready()
    })
    .await
    .unwrap_or(false)
}

#[tokio::test]
async fn real_pipeline_handshake_whitelist_fifo_and_bidirectional_command() {
    let mut peer = Peer::new().await;
    let client = client(&peer);
    let control = client.control();
    let mut worker = start(client).await;
    let start = bounded(worker.recv()).await;
    let handshake = peer.observed().await;
    peer.wire(json!({"label":"wrong", "type":"bot-ready", "id":"bad", "data":{"version":"2.2.0"}}))
        .await;
    peer.wire(event("bot-llm-text", json!({"text":"before-ready"})))
        .await;
    tokio::time::sleep(QUIET).await;
    let prematurely_ready = control.is_ready();
    let premature_frame = worker.try_recv().ok();
    // Python profile intentionally accepts label/type-only bot-ready.
    peer.wire(event("bot-ready", json!({}))).await;
    let became_ready = wait_ready(&control).await;
    peer.wire(ready()).await;
    // Invalid JSON, wrong labels, valid but out-of-profile events, and remote
    // lifecycle/control requests must never become native pipeline authority.
    bounded(peer.send.send(Message::Text("{invalid".into())))
        .await
        .unwrap();
    for value in [
        event("unknown", json!({})),
        event("interruption", json!({})),
        event("cancel", json!({})),
        event("bot-interrupted", Value::Null),
        event(
            "llm-function-call-in-progress",
            json!({"function_name":"danger", "tool_call_id":"x", "arguments":{}}),
        ),
        event(
            "bot-tts-audio",
            json!({"audio":"AAAA", "sample_rate":16000, "num_channels":1}),
        ),
        event(
            "user-transcription",
            json!({"text":"not admitted", "user_id":"u", "timestamp":"now", "final":true}),
        ),
        event("bot-llm-marker", json!({"text":"not admitted"})),
        json!({"label":"wrong", "type":"bot-llm-text", "data":{"text":"wrong-label"}}),
    ] {
        peer.wire(value).await;
    }
    peer.wire(event("bot-llm-started", Value::Null)).await;
    peer.wire(event("bot-llm-text", json!({"text":"first"})))
        .await;
    peer.wire(event("bot-llm-text", json!({"text":"second"})))
        .await;
    peer.wire(event("bot-llm-stopped", Value::Null)).await;
    let frames = llm_frames(&mut worker, 4).await;
    let command_sent = bounded(worker.sender().send(command(), Direction::Downstream)).await;
    let wire_command = peer.observed().await;
    let plain = Frame::text("serializer-none");
    let plain_id = plain.id;
    let _ = bounded(worker.sender().send(plain, Direction::Downstream)).await;
    let passthrough = bounded(worker.recv()).await;
    let unsupported = Frame::new(FrameData::OutputTransportMessage(json!({
        "label": "another-protocol", "data": "x".repeat(20_000)
    })));
    let unsupported_id = unsupported.id;
    let _ = bounded(worker.sender().send(unsupported, Direction::Downstream)).await;
    let serializer_none = bounded(worker.recv()).await;
    let unexpected_wire = tokio::time::timeout(QUIET, peer.received.recv())
        .await
        .ok()
        .flatten();
    let still_ready = control.is_ready();
    let report = finish(&mut worker).await;
    let socket_closed = peer.finish().await;
    assert!(matches!(
        start.map(|f| f.frame.data),
        Some(FrameData::Start(_))
    ));
    let handshake = handshake.expect("client-ready wire message");
    assert_eq!(handshake["label"], "rtvi-ai");
    assert_eq!(handshake["type"], "client-ready");
    assert_eq!(handshake["data"]["version"], "2.2.0");
    assert!(handshake["id"].as_str().is_some_and(|s| !s.is_empty()));
    assert!(handshake["data"]["about"].is_object());
    assert!(!prematurely_ready && premature_frame.is_none());
    assert!(became_ready && still_ready);
    assert!(
        matches!(frames.as_slice(), [FrameData::LlmResponseStart, FrameData::LlmText(a), FrameData::LlmText(b), FrameData::LlmResponseEnd] if a.text == "first" && b.text == "second")
    );
    assert!(command_sent.is_ok());
    assert_eq!(wire_command.unwrap()["id"], "command-1");
    assert_eq!(passthrough.unwrap().frame.id, plain_id);
    assert_eq!(serializer_none.unwrap().frame.id, unsupported_id);
    assert!(
        unexpected_wire.is_none(),
        "serializer None emitted a wire message"
    );
    assert!(report.is_ok() && socket_closed && control.is_closed());
}

#[tokio::test]
async fn handshake_timeout_and_remote_close_close_control_without_reconnect() {
    for remote_close in [false, true] {
        let mut peer = Peer::new().await;
        let mut config = RtviClientConfig::new(&peer.endpoint);
        config.handshake_timeout = Duration::from_millis(100);
        let client = Arc::new(RtviClientProcessor::new(config).unwrap());
        let control = client.control();
        let mut worker = start(client).await;
        let _ = peer.observed().await;
        if remote_close {
            bounded(peer.send.send(Message::Close(None))).await.unwrap();
        }
        let closed = wait_closed(&control).await;
        let status = control.status();
        let report = finish(&mut worker).await;
        let socket_closed = peer.finish().await;
        assert!(closed && socket_closed && report.is_ok());
        if !remote_close {
            assert_eq!(
                status,
                RtviClientStatus::Failed(RtviClientFailure::HandshakeTimeout)
            );
        } else {
            assert_eq!(status, RtviClientStatus::Closed);
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum Terminal {
    End,
    Stop,
    Cancel,
    Interruption,
    OwnerDrop,
}
#[tokio::test]
async fn lifecycle_closes_held_bot_handshake_and_active_receive_window() {
    for ready_first in [false, true] {
        for terminal in [
            Terminal::End,
            Terminal::Stop,
            Terminal::Cancel,
            Terminal::Interruption,
            Terminal::OwnerDrop,
        ] {
            let mut peer = Peer::new().await;
            let client = client(&peer);
            let control = client.control();
            let weak = Arc::downgrade(&client);
            let mut worker = start(client).await;
            let _ = peer.observed().await;
            if ready_first {
                peer.wire(ready()).await;
                let _ = wait_ready(&control).await;
            }
            let mut joined = None;
            match terminal {
                Terminal::End => {
                    let _ = bounded(worker.end()).await;
                }
                Terminal::Stop => {
                    let _ = bounded(
                        worker
                            .sender()
                            .send(Frame::new(FrameData::Stop), Direction::Downstream),
                    )
                    .await;
                }
                Terminal::Cancel => {
                    let _ = bounded(worker.cancel("terminal test")).await;
                }
                Terminal::Interruption => {
                    let _ = bounded(
                        worker
                            .sender()
                            .send(Frame::interruption(), Direction::Downstream),
                    )
                    .await;
                    let _ = wait_closed(&control).await;
                    // This late remote frame must not be rebound to the new epoch.
                    let _ = peer
                        .send
                        .send(Message::Text(
                            event("bot-llm-text", json!({"text":"late"}))
                                .to_string()
                                .into(),
                        ))
                        .await;
                    let _ = bounded(worker.cancel("after interruption")).await;
                }
                Terminal::OwnerDrop => {}
            }
            if !matches!(terminal, Terminal::OwnerDrop) {
                joined = Some(bounded(worker.join()).await);
            }
            let mut late = false;
            while let Ok(frame) = worker.try_recv() {
                late |= matches!(frame.frame.data, FrameData::LlmText(ref t) if t.text == "late");
            }
            drop(worker);
            let closed = wait_closed(&control).await;
            let socket_closed = peer.finish().await;
            let released = tokio::time::timeout(DEADLINE, async {
                while weak.upgrade().is_some() {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .is_ok();
            assert!(
                closed && socket_closed && released && !late,
                "{terminal:?}, ready={ready_first}"
            );
            if let Some(report) = joined {
                assert!(report.is_ok(), "{terminal:?}: {report:?}");
            }
        }
    }
}

#[tokio::test]
async fn fresh_instance_isolated_from_old_control_and_setup_is_single_use() {
    let mut first = Peer::new().await;
    let old_client = client(&first);
    let old_control = old_client.control();
    let mut old_worker = start(old_client.clone()).await;
    let _ = first.observed().await;
    let old_report = finish(&mut old_worker).await;
    let first_closed = first.finish().await;
    drop(old_worker);
    let reuse = bounded(Pipeline::new(vec![old_client]).start(PipelineConfig::default())).await;
    // Core reports nonfatal setup errors on the returned worker; start()
    // need not fail merely because a processor rejected setup.
    let (reuse_rejected, reuse_diagnostic) = match reuse {
        Err(error) => {
            let rejected = matches!(&error, PipelineError::Execution(message) if message.contains("single-use"));
            (rejected, format!("start: {error:?}"))
        }
        Ok(mut reused) => {
            let errors = reused.errors();
            let rejected = errors
                .iter()
                .any(|error| error.error.contains("single-use"));
            let joined = finish(&mut reused).await;
            (rejected, format!("errors: {errors:?}; join: {joined:?}"))
        }
    };
    let mut peer = Peer::new().await;
    let new_client = client(&peer);
    let new_control = new_client.control();
    let mut worker = start(new_client).await;
    let _ = peer.observed().await;
    peer.wire(ready()).await;
    let new_ready = wait_ready(&new_control).await;
    old_control.close();
    peer.wire(event("bot-llm-text", json!({"text":"fresh"})))
        .await;
    let frames = llm_frames(&mut worker, 1).await;
    let remained_ready = new_control.is_ready();
    let report = finish(&mut worker).await;
    let socket_closed = peer.finish().await;
    assert!(old_report.is_ok(), "original worker join: {old_report:?}");
    assert!(first_closed, "original peer did not observe socket closure");
    assert!(
        reuse_rejected,
        "single-use setup was not rejected: {reuse_diagnostic}"
    );
    assert!(new_ready, "fresh control never became ready");
    assert!(remained_ready, "old control closed the fresh run");
    assert!(report.is_ok(), "fresh worker join: {report:?}");
    assert!(socket_closed, "fresh peer did not observe socket closure");
    assert!(matches!(frames.as_slice(), [FrameData::LlmText(t)] if t.text == "fresh"));
}

#[tokio::test]
async fn lifecycle_interrupts_http_upgrade_without_waiting_for_connect_timeout() {
    use tokio::io::AsyncReadExt;
    for terminal in [
        Terminal::End,
        Terminal::Stop,
        Terminal::Cancel,
        Terminal::Interruption,
        Terminal::OwnerDrop,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (accepted, accepted_rx) = tokio::sync::oneshot::channel();
        let mut server = tokio::spawn(async move {
            tokio::time::timeout(Duration::from_secs(8), async {
                let (mut socket, _) = listener.accept().await.unwrap();
                let _ = accepted.send(());
                let mut bytes = [0; 4096];
                loop {
                    match socket.read(&mut bytes).await {
                        Ok(0) | Err(_) => break true,
                        Ok(_) => {}
                    }
                }
            })
            .await
            .unwrap_or(false)
        });
        let mut config = RtviClientConfig::new(endpoint);
        config.session.connect_timeout = Duration::from_secs(30);
        let client = Arc::new(RtviClientProcessor::new(config).unwrap());
        let control = client.control();
        let mut worker = start(client).await;
        let accepted = bounded(accepted_rx).await;
        match terminal {
            Terminal::End => {
                let _ = bounded(worker.end()).await;
            }
            Terminal::Stop => {
                let _ = bounded(
                    worker
                        .sender()
                        .send(Frame::new(FrameData::Stop), Direction::Downstream),
                )
                .await;
            }
            Terminal::Cancel => {
                let _ = bounded(worker.cancel("held upgrade")).await;
            }
            Terminal::Interruption => {
                let _ = bounded(
                    worker
                        .sender()
                        .send(Frame::interruption(), Direction::Downstream),
                )
                .await;
                let _ = wait_closed(&control).await;
                let _ = bounded(worker.cancel("after held upgrade interruption")).await;
            }
            Terminal::OwnerDrop => {}
        }
        let joined = if matches!(terminal, Terminal::OwnerDrop) {
            None
        } else {
            Some(bounded(worker.join()).await)
        };
        drop(worker);
        let closed = wait_closed(&control).await;
        let socket_closed = join_server(&mut server).await.unwrap();
        assert!(accepted.is_ok() && closed && socket_closed, "{terminal:?}");
        if let Some(report) = joined {
            assert!(report.is_ok(), "{terminal:?}: {report:?}");
        }
    }
}

struct HoldCollector {
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait]
impl FrameProcessor for HoldCollector {
    fn name(&self) -> &str {
        "HeldRtviCollector"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmText(_)) {
            self.entered.notify_one();
            let permit = self.release.acquire().await.unwrap();
            permit.forget();
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn held_downstream_queue_fails_fast_and_joins_owned_socket() {
    let mut peer = Peer::new().await;
    let client = client(&peer);
    let control = client.control();
    let collector = Arc::new(HoldCollector {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
    });
    let mut worker = bounded(Pipeline::new(vec![client, collector.clone()]).start(
        PipelineConfig {
            queue_capacity: 1,
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    ))
    .await
    .unwrap();
    let _ = peer.observed().await;
    peer.wire(ready()).await;
    let became_ready = wait_ready(&control).await;
    peer.wire(event("bot-llm-text", json!({"text":"held"})))
        .await;
    bounded(collector.entered.notified()).await;
    for i in 0..8 {
        peer.wire(event("bot-llm-text", json!({"text":format!("queued-{i}")})))
            .await;
    }
    let closed = wait_closed(&control).await;
    let status = control.status();
    collector.release.add_permits(16);
    let report = finish(&mut worker).await;
    let socket_closed = peer.finish().await;
    assert!(became_ready, "client never became ready");
    assert!(
        closed,
        "downstream saturation did not close control: {status:?}"
    );
    assert!(socket_closed, "peer did not observe socket closure");
    // Core fails the entire worker synchronously on adjacent-lane overflow.
    assert!(
        matches!(&report, Err(PipelineError::Backpressure(message)) if message.contains("processor 1 input lane is full")),
        "expected fatal downstream queue backpressure, got {report:?}"
    );
    assert_eq!(
        status,
        RtviClientStatus::Failed(RtviClientFailure::Pipeline)
    );
}

#[tokio::test]
async fn native_history_is_preserved_locally_but_cannot_be_forged_into_wire_command() {
    let mut peer = Peer::new().await;
    let client = client(&peer);
    let control = client.control();
    let mut worker = start(client).await;
    let _ = bounded(worker.recv()).await;
    let _ = peer.observed().await;
    // Native frames are local pass-through even before the bot handshake.
    let history = Arc::new(
        NativeLlmContinuation::new(
            81,
            LlmContinuationProvider::Anthropic,
            vec![LlmContinuationBlock::Thinking {
                text: "private".into(),
                signature: "signature".into(),
            }],
        )
        .unwrap(),
    );
    let mut native = Frame::new(FrameData::LlmResponseEnd);
    native.set_native_llm_continuation(history.clone()).unwrap();
    let mut forged = native.clone();
    forged.data = command().data;
    let sent_native = bounded(worker.sender().send(native, Direction::Downstream)).await;
    let preserved = bounded(worker.recv()).await;
    peer.wire(ready()).await;
    let became_ready = wait_ready(&control).await;
    let _ = bounded(worker.sender().send(forged, Direction::Downstream)).await;
    let no_wire = tokio::time::timeout(QUIET, peer.received.recv())
        .await
        .is_err();
    // A valid command is a barrier: it proves the forged command was processed.
    let _ = bounded(worker.sender().send(command(), Direction::Downstream)).await;
    let wire = peer.observed().await;
    let errors = worker.total_errors();
    let report = finish(&mut worker).await;
    let socket_closed = peer.finish().await;
    assert!(sent_native.is_ok() && became_ready && no_wire && errors > 0);
    let preserved = preserved.unwrap().frame;
    assert!(Arc::ptr_eq(
        preserved.native_llm_continuation().unwrap().unwrap(),
        &history
    ));
    assert_eq!(wire.unwrap()["id"], "command-1");
    assert!(report.is_ok() && socket_closed);
}

struct OccupiedTaskSlot(Arc<RtviClientProcessor>);
#[async_trait]
impl FrameProcessor for OccupiedTaskSlot {
    fn name(&self) -> &str {
        "OccupiedRtviTaskSlot"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.spawn("occupy-only-slot", false, std::future::pending())
            .map(|_| ())?;
        self.0.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.0.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cleanup().await
    }
}
#[tokio::test]
async fn rejected_background_task_admission_does_not_open_or_retain_session() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let client = Arc::new(RtviClientProcessor::new(RtviClientConfig::new(endpoint)).unwrap());
    let control = client.control();
    let weak = Arc::downgrade(&client);
    let mut server = tokio::spawn(async move {
        // Even a rejected startup has an owned, bounded listener task to join.
        tokio::time::timeout(Duration::from_millis(200), listener.accept())
            .await
            .is_ok()
    });
    let started = bounded(
        Pipeline::new(vec![Arc::new(OccupiedTaskSlot(client))]).start(PipelineConfig {
            max_background_tasks: 1,
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        }),
    )
    .await;
    // Start delivery and fatal task-admission failure may race. In either
    // case, the terminal result must preserve the precise fatal error.
    let outcome = match started {
        Err(error) => Err(error),
        Ok(mut worker) => bounded(worker.join()).await,
    };
    let closed = wait_closed(&control).await;
    let connected = join_server(&mut server).await.unwrap();
    let released = tokio::time::timeout(DEADLINE, async {
        while weak.upgrade().is_some() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok();
    assert!(
        matches!(&outcome, Err(PipelineError::Execution(message)) if message == "background task limit reached"),
        "expected fatal task admission rejection, got {outcome:?}"
    );
    assert!(
        closed,
        "control remained open after rejected task admission"
    );
    assert!(!connected, "rejected task admission opened a socket");
    assert!(released, "rejected task retained processor");
}

#[tokio::test]
async fn pre_ready_command_rejected_and_audio_is_local_only() {
    let mut peer = Peer::new().await;
    let client = client(&peer);
    let control = client.control();
    let mut worker = start(client).await;
    let _ = bounded(worker.recv()).await;
    let _ = peer.observed().await;
    let audio = Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(vec![0, 0], 16000, 1).unwrap(),
    ));
    let audio_id = audio.id;
    let _ = bounded(worker.sender().send(audio, Direction::Downstream)).await;
    let local_audio = bounded(worker.recv()).await;
    let _ = bounded(worker.sender().send(command(), Direction::Downstream)).await;
    let error_seen = tokio::time::timeout(DEADLINE, async {
        while worker.total_errors() == 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .is_ok();
    let not_ready = !control.is_ready();
    let report = finish(&mut worker).await;
    let mut messages = Vec::new();
    while let Ok(message) = peer.received.try_recv() {
        messages.push(message);
    }
    let socket_closed = peer.finish().await;
    assert_eq!(local_audio.unwrap().frame.id, audio_id);
    assert!(error_seen && not_ready && messages.is_empty() && report.is_ok() && socket_closed);
}

#[tokio::test]
async fn peer_not_reading_and_byte_admission_failure_still_close_and_join() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (release, held) = tokio::sync::oneshot::channel();
    let mut server = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(8), async {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(socket).await.unwrap();
            ws.send(Message::Text(ready().to_string().into()))
                .await
                .unwrap();
            // Deliberately do not read until the pipeline has joined. This tests
            // cleanup with a non-reading peer, not unprovable OS saturation.
            let _ = held.await;
            let mut text = Vec::new();
            let closed = loop {
                match ws.next().await {
                    Some(Ok(Message::Text(value))) => text.push(value.to_string()),
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break true,
                    _ => {}
                }
            };
            (closed, text)
        })
        .await
        .unwrap_or((false, vec![]))
    });
    let mut config = RtviClientConfig::new(endpoint);
    config.outbox_capacity = 1;
    config.outbox_max_bytes = 16; // Smaller than the exact serialized command.
    let client = Arc::new(RtviClientProcessor::new(config).unwrap());
    let control = client.control();
    let mut worker = start(client).await;
    let ready = wait_ready(&control).await;
    let _ = bounded(worker.sender().send(command(), Direction::Downstream)).await;
    let closed = wait_closed(&control).await;
    let status = control.status();
    let report = finish(&mut worker).await;
    let _ = release.send(());
    let (socket_closed, messages) = join_server(&mut server).await.unwrap();
    assert!(ready && closed && socket_closed && report.is_ok());
    assert_eq!(
        status,
        RtviClientStatus::Failed(RtviClientFailure::Backpressure)
    );
    assert!(messages
        .iter()
        .all(|s| serde_json::from_str::<Value>(s).unwrap()["type"] == "client-ready"));
    assert!(
        messages.len() <= 1,
        "duplicate handshake or unexpected outbound command"
    );
}

#[tokio::test]
async fn upstream_transport_passthrough_urgent_wire_and_ambiguous_routing_rejection() {
    let mut peer = Peer::new().await;
    let client = client(&peer);
    let control = client.control();
    let mut worker = start(client).await;
    let _ = bounded(worker.recv()).await;
    let _ = peer.observed().await;
    peer.wire(ready()).await;
    let became_ready = wait_ready(&control).await;
    let mut upstream = command();
    upstream.transport_destination = Some("local-destination".into());
    let upstream_id = upstream.id;
    let _ = bounded(worker.sender().send(upstream, Direction::Upstream)).await;
    let local = bounded(worker.recv()).await;
    let mut urgent = command();
    let FrameData::OutputTransportMessage(value) = urgent.data else {
        unreachable!()
    };
    urgent.data = FrameData::OutputTransportMessageUrgent(value);
    let _ = bounded(worker.sender().send(urgent, Direction::Upstream)).await;
    let urgent_wire = peer.observed().await;
    let before = worker.total_errors();
    for mode in 0..3 {
        let mut invalid = command();
        match mode {
            0 => invalid.transport_destination = Some("unsupported".into()),
            1 => invalid.pts = Some(0),
            _ => {
                invalid.set_pipeline_pts(0);
                invalid.pts = None;
            }
        }
        let _ = bounded(worker.sender().send(invalid, Direction::Downstream)).await;
    }
    // A regular local frame is a processing barrier for the three rejects.
    let _ = bounded(
        worker
            .sender()
            .send(Frame::text("barrier"), Direction::Downstream),
    )
    .await;
    let _ = bounded(async {
        while let Some(item) = worker.recv().await {
            if matches!(item.frame.data, FrameData::Text(ref t) if t.text == "barrier") {
                break;
            }
        }
    })
    .await;
    let rejected = worker.total_errors().saturating_sub(before);
    let no_other_wire = tokio::time::timeout(QUIET, peer.received.recv())
        .await
        .is_err();
    let report = finish(&mut worker).await;
    let socket_closed = peer.finish().await;
    let local = local.unwrap();
    assert_eq!(local.direction, Direction::Upstream);
    assert_eq!(local.frame.id, upstream_id);
    assert_eq!(
        local.frame.transport_destination.as_deref(),
        Some("local-destination")
    );
    assert!(became_ready && urgent_wire.unwrap()["id"] == "command-1");
    assert_eq!(rejected, 3);
    assert!(no_other_wire && socket_closed && report.is_ok());
}

/// Harness ownership regression, not an additional product network capability.
#[tokio::test]
async fn peer_fixture_timeout_cancels_joins_and_releases_listener() {
    let peer = Peer::new().await;
    let endpoint = url::Url::parse(&peer.endpoint).unwrap();
    let address = (endpoint.host_str().unwrap(), endpoint.port().unwrap());
    // Never connect: finish must take its timeout branch, cancel and join the
    // server, and return a failure observation rather than detach its task.
    let observed_close = peer.finish().await;
    let rebound = TcpListener::bind(address).await;
    assert!(
        !observed_close,
        "an unconnected peer cannot observe closure"
    );
    assert!(rebound.is_ok(), "timed-out fixture retained its listener");
}
