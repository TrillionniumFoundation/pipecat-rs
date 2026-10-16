//! Correlated cancel-race recovery over real local WebSockets and the default
//! paired User -> realtime service -> receipt tap -> Assistant pipeline.
//! Public sender acknowledgements prove actor admission; Ping/Pong proves socket
//! consumption only. Provider frame receipts and final joined traces are separate.
#[path = "manual_preroll_runtime.rs"]
mod existing_manual_preroll;

use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_realtime::*;
use pipecat_services::Context;
use pipecat_turns::{LLMContextAggregatorPair, UserAggregatorConfig, UserTurnConfig};
use serde_json::{json, Value};
use std::{collections::VecDeque, fmt::Display, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);
const MAX_RECORDS: usize = 256;
const FATAL_MESSAGE: &str = "realtime provider rejected session state: synthetic cancel race";
type Check<T> = Result<T, String>;
fn error(e: impl Display) -> String {
    e.to_string()
}

enum PeerAction {
    Event(Value),
    Ping(u64),
    Close,
}
enum Wire {
    Json(Value),
    Pong(u64),
}
struct Peer {
    actions: mpsc::Sender<PeerAction>,
    wire: mpsc::Receiver<Wire>,
    task: JoinHandle<Check<()>>,
    records: Vec<Value>,
    pending: VecDeque<Value>,
    next_ping: u64,
}
impl Peer {
    async fn new(ready: bool, server_vad: bool) -> (Self, RealtimeConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("synthetic-cancel-race-loopback");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        config.limits.operation_timeout = Duration::from_secs(2);
        config.limits.response_timeout = WAIT;
        config.limits.close_timeout = Duration::from_secs(1);
        if server_vad {
            config
                .session
                .audio
                .as_mut()
                .unwrap()
                .input
                .as_mut()
                .unwrap()
                .turn_detection = Some(TurnDetection::ServerVad {
                threshold: 0.5,
                prefix_padding_ms: 300,
                silence_duration_ms: 500,
                create_response: true,
                interrupt_response: true,
            });
        }
        let (actions, mut events) = mpsc::channel::<PeerAction>(64);
        let (wire_tx, wire) = mpsc::channel::<Wire>(64);
        let task = tokio::spawn(async move {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(error)?
                .map_err(error)?;
            let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
                .max_message_size(Some(1024 * 1024))
                .max_frame_size(Some(1024 * 1024));
            let mut ws = timeout(
                WAIT,
                tokio_tungstenite::accept_async_with_config(tcp, Some(ws_config)),
            )
            .await
            .map_err(error)?
            .map_err(error)?;
            let first = timeout(WAIT, ws.next())
                .await
                .map_err(error)?
                .ok_or("no session update")?
                .map_err(error)?;
            let first: Value =
                serde_json::from_str(first.to_text().map_err(error)?).map_err(error)?;
            wire_tx.try_send(Wire::Json(first)).map_err(error)?;
            if ready {
                timeout(
                    WAIT,
                    ws.send(Message::Text(ready_event().to_string().into())),
                )
                .await
                .map_err(error)?
                .map_err(error)?;
            }
            loop {
                tokio::select! {
                    action = events.recv() => {
                        let message = match action {
                            Some(PeerAction::Event(value)) => Message::Text(value.to_string().into()),
                            Some(PeerAction::Ping(id)) => Message::Ping(id.to_be_bytes().to_vec().into()),
                            Some(PeerAction::Close) | None => { let _ = timeout(WAIT, ws.close(None)).await; return Ok(()); }
                        };
                        timeout(WAIT, ws.send(message)).await.map_err(error)?.map_err(error)?;
                    }
                    message = ws.next() => {
                        match message {
                            Some(Ok(Message::Text(text))) => {
                                if text.len() > 1024 * 1024 { return Err("peer message bound".into()); }
                                wire_tx.try_send(Wire::Json(serde_json::from_str(&text).map_err(error)?)).map_err(error)?;
                            }
                            Some(Ok(Message::Pong(bytes))) => {
                                let bytes: [u8;8] = bytes.as_ref().try_into().map_err(error)?;
                                wire_tx.try_send(Wire::Pong(u64::from_be_bytes(bytes))).map_err(error)?;
                            }
                            Some(Ok(Message::Ping(bytes))) => { timeout(WAIT, ws.send(Message::Pong(bytes))).await.map_err(error)?.map_err(error)?; }
                            Some(Ok(Message::Close(_))) | None => { let _ = timeout(WAIT, ws.close(None)).await; return Ok(()); }
                            Some(Err(e)) => return Err(error(e)),
                            Some(Ok(_)) => return Err("unexpected client binary message".into()),
                        }
                    }
                }
            }
        });
        (
            Self {
                actions,
                wire,
                task,
                records: vec![],
                pending: VecDeque::new(),
                next_ping: 0,
            },
            config,
        )
    }
    async fn action(&self, action: PeerAction) -> Check<()> {
        timeout(WAIT, self.actions.send(action))
            .await
            .map_err(error)?
            .map_err(error)
    }
    async fn record(&mut self) -> Check<Wire> {
        let item = timeout(WAIT, self.wire.recv())
            .await
            .map_err(error)?
            .ok_or("peer wire closed")?;
        if let Wire::Json(value) = &item {
            if self.records.len() >= MAX_RECORDS {
                return Err("wire observation bound".into());
            }
            self.records.push(value.clone());
            // Recording a command is not consuming it. A Ping/Pong fence may
            // encounter the deferred create released by the preceding done.
            self.pending.push_back(value.clone());
        }
        Ok(item)
    }
    async fn command(&mut self, kind: &str) -> Check<Value> {
        timeout(WAIT, async {
            loop {
                if let Some(value) = self.pending.pop_front() {
                    if value["type"] == kind {
                        return Ok(value);
                    }
                    // Context synchronization can update the session before the
                    // requested command; no inference or control command is skipped.
                    if value["type"] != "session.update" {
                        return Err(format!("expected {kind}, received {value}"));
                    }
                } else if matches!(self.record().await?, Wire::Pong(_)) {
                    return Err("unclaimed fixture Pong".into());
                }
            }
        })
        .await
        .map_err(error)?
    }
    async fn fence(&mut self) -> Check<()> {
        self.next_ping += 1;
        let id = self.next_ping;
        self.action(PeerAction::Ping(id)).await?;
        timeout(WAIT, async {
            loop {
                if matches!(self.record().await?, Wire::Pong(got) if got == id) {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(error)?
    }
    async fn quiet(&mut self) -> Check<()> {
        if let Some(value) = self.pending.front() {
            return Err(format!("unconsumed command before quiet window: {value}"));
        }
        match timeout(Duration::from_millis(150), self.record()).await {
            Err(_) => Ok(()),
            Ok(Ok(Wire::Json(value))) => Err(format!(
                "unexpected command while response remains active: {value}"
            )),
            Ok(Ok(Wire::Pong(_))) => Err("unclaimed fixture Pong".into()),
            Ok(Err(e)) => Err(e),
        }
    }
    async fn join(mut self) -> (Vec<Value>, Check<()>) {
        let result = match timeout(WAIT, &mut self.task).await {
            Ok(result) => result.map_err(error).and_then(|v| v),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Err("peer deadline; aborted owner actually joined".into())
            }
        };
        while let Ok(item) = self.wire.try_recv() {
            if let Wire::Json(value) = item {
                if self.records.len() >= MAX_RECORDS {
                    return (self.records, Err("wire observation bound".into()));
                }
                self.records.push(value);
            }
        }
        (self.records, result)
    }
}
fn ready_event() -> Value {
    json!({"event_id":"ready","type":"session.updated","session":{"id":"cancel-race-fixture","type":"realtime"}})
}
fn response_event(id: &str, done: bool) -> Value {
    if done {
        json!({"event_id":"finished","type":"response.done","response":{"id":id,"status":"completed","output":[]}})
    } else {
        json!({"event_id":"created","type":"response.created","response":{"id":id,"status":"in_progress","output":[]}})
    }
}
struct Tap(mpsc::Sender<DirectedFrame>);
#[async_trait]
impl FrameProcessor for Tap {
    fn name(&self) -> &str {
        "CancelRacePostServiceReceipt"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0
            .try_send(DirectedFrame::new(frame.clone(), direction))
            .map_err(|e| ProcessorError::new(e.to_string()))?;
        ctx.push(frame, direction).await
    }
}
struct Rig {
    worker: PipelineWorker,
    peer: Peer,
    processor: Arc<OpenAiRealtimeProcessor>,
    sender: RealtimeSender,
    tapped: mpsc::Receiver<DirectedFrame>,
    frames: Vec<DirectedFrame>,
}
struct Outcome {
    wire: Vec<Value>,
    frames: Vec<DirectedFrame>,
    report: Result<WorkerReport, PipelineError>,
    errors: Vec<ErrorFrame>,
    closed_command: Check<()>,
    peer: Check<()>,
}
impl Rig {
    async fn new(ready: bool, server_vad: bool, explicit: Option<f64>) -> Self {
        let (peer, config) = Peer::new(ready, server_vad).await;
        let mut processor =
            OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap();
        if let Some(seconds) = explicit {
            processor = processor.with_user_audio_preroll_secs(seconds).unwrap();
        }
        let processor = Arc::new(processor);
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            UserAggregatorConfig {
                turn: UserTurnConfig {
                    stop_timeout: Duration::from_secs(30),
                    ..Default::default()
                },
                empty_user_turn: None,
                ..Default::default()
            },
            Default::default(),
        );
        let (tap, tapped) = mpsc::channel(128);
        let result = Pipeline::new(vec![
            pair.user(),
            processor.clone(),
            Arc::new(Tap(tap)),
            pair.assistant(),
        ])
        .start(PipelineConfig {
            start: StartFrame {
                audio_in_sample_rate: 24_000,
                audio_out_sample_rate: 24_000,
                ..Default::default()
            },
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        })
        .await;
        match result {
            Ok(worker) => Self {
                worker,
                peer,
                sender: processor.sender().unwrap(),
                processor,
                tapped,
                frames: vec![],
            },
            Err(e) => {
                let _ = peer.action(PeerAction::Close).await;
                let _ = peer.join().await;
                panic!("pipeline start failed after peer joined: {e}");
            }
        }
    }
    async fn observed(&mut self, wanted: impl Fn(&DirectedFrame) -> bool) -> Check<()> {
        if self.frames.iter().any(&wanted) {
            return Ok(());
        }
        timeout(WAIT, async {
            loop {
                let out = timeout(WAIT, self.tapped.recv())
                    .await
                    .map_err(error)?
                    .ok_or("tap closed")?;
                let matched = wanted(&out);
                if self.frames.len() >= MAX_RECORDS {
                    return Err("frame observation bound".into());
                }
                self.frames.push(out);
                if matched {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(error)?
    }
    async fn frame(&mut self, data: FrameData) -> Check<()> {
        let frame = Frame::new(data);
        let id = frame.id;
        timeout(
            WAIT,
            self.worker.sender().send(frame, Direction::Downstream),
        )
        .await
        .map_err(error)?
        .map_err(error)?;
        self.observed(|f| f.direction == Direction::Downstream && f.frame.id == id)
            .await
    }
    async fn send_only(&self, data: FrameData) -> Check<()> {
        timeout(
            WAIT,
            self.worker
                .sender()
                .send(Frame::new(data), Direction::Downstream),
        )
        .await
        .map_err(error)?
        .map_err(error)
    }
    async fn interrupt(&mut self) -> Check<String> {
        self.frame(FrameData::Interruption).await?;
        self.peer.command("input_audio_buffer.clear").await?;
        let command = self.peer.command("response.cancel").await?;
        self.peer.fence().await?;
        command["event_id"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| "cancel had no client event ID".into())
    }
    async fn run(&mut self) -> Check<()> {
        self.send_only(FrameData::LlmRun).await?;
        self.peer.command("response.create").await?;
        Ok(())
    }
    async fn created(&mut self, id: &str) -> Check<()> {
        self.peer
            .action(PeerAction::Event(response_event(id, false)))
            .await?;
        self.observed(|f| {
            matches!(f.frame.data, FrameData::LlmResponseStart)
                && f.frame.metadata.get("response_id").and_then(Value::as_str) == Some(id)
        })
        .await?;
        self.peer.fence().await
    }
    async fn done(&mut self, id: &str) -> Check<()> {
        self.peer
            .action(PeerAction::Event(response_event(id, true)))
            .await?;
        self.peer.fence().await
    }
    async fn deferred_run(&self) -> Check<()> {
        // This public sender acknowledgement proves the actor processed the
        // request and retained it behind active, rather than merely enqueueing.
        self.processor
            .sender()
            .map_err(error)?
            .send(Action::CreateResponse(ResponseOptions::default()))
            .await
            .map_err(error)
    }
    async fn fatal_receipt(&mut self) -> Check<()> {
        // A fatal error is journaled before runtime.fail closes all queues.
        // Its upstream frame need not survive that closure. Wait for the actual
        // output closure here; finish() joins both owners before assertions.
        timeout(WAIT, async {
            while let Some(out) = self.worker.recv().await {
                if self.frames.len() >= MAX_RECORDS {
                    return Err("frame observation bound".into());
                }
                self.frames.push(out);
            }
            if self
                .worker
                .errors()
                .iter()
                .any(|e| e.fatal && e.error == FATAL_MESSAGE)
            {
                Ok(())
            } else {
                Err("pipeline closed without the expected fatal journal entry".into())
            }
        })
        .await
        .map_err(error)?
    }
    fn collect_tap(&mut self) -> Check<()> {
        while let Ok(frame) = self.tapped.try_recv() {
            if self.frames.len() >= MAX_RECORDS {
                return Err("frame observation bound".into());
            }
            self.frames.push(frame);
        }
        Ok(())
    }
    async fn finish(mut self, cancel: bool) -> Outcome {
        let mut terminal = if self.worker.has_finished() {
            Ok(())
        } else if cancel {
            timeout(WAIT, self.worker.cancel("cancel-race cleanup"))
                .await
                .map_err(error)
                .and_then(|r| r.map_err(error))
        } else {
            timeout(WAIT, self.worker.end())
                .await
                .map_err(error)
                .and_then(|r| r.map_err(error))
        };
        if terminal.is_err() {
            let _ = self.worker.cancel("terminal admission failed").await;
        }
        let report = match timeout(WAIT, self.worker.join()).await {
            Ok(result) => result,
            Err(_) => {
                let _ = self.worker.cancel("join deadline").await;
                terminal = Err("worker join deadline; owner joined after cancellation".into());
                self.worker.join().await
            }
        };
        while let Ok(frame) = self.tapped.try_recv() {
            if self.frames.len() < MAX_RECORDS {
                self.frames.push(frame);
            }
        }
        let errors = self.worker.errors();
        let (wire, peer) = self.peer.join().await;
        let closed_command = if errors.iter().any(|e| e.fatal) {
            match timeout(
                WAIT,
                self.sender
                    .send(Action::CreateResponse(ResponseOptions::default())),
            )
            .await
            {
                Ok(Err(RealtimeError::Closed)) => Ok(()),
                result => Err(format!(
                    "joined fatal session accepted or misclassified a command: {result:?}"
                )),
            }
        } else {
            Ok(())
        };
        // The caller performs every assertion after both owners really joined.
        Outcome {
            wire,
            frames: self.frames,
            report,
            errors,
            closed_command,
            peer: terminal.and(peer),
        }
    }
}
fn provider_error(outer: &str, code: &str, nested: Option<Value>) -> Value {
    let mut event = json!({"event_id":outer,"type":"error","error":{"type":"invalid_request_error","code":code,"message":"synthetic cancel race"}});
    if let Some(id) = nested {
        event["error"]["event_id"] = id;
    }
    event
}
fn response_frames(frames: &[DirectedFrame], start: bool) -> Vec<String> {
    frames
        .iter()
        .filter(|f| {
            if start {
                matches!(f.frame.data, FrameData::LlmResponseStart)
            } else {
                matches!(f.frame.data, FrameData::LlmResponseEnd)
            }
        })
        .filter_map(|f| {
            f.frame
                .metadata
                .get("response_id")?
                .as_str()
                .map(str::to_owned)
        })
        .collect()
}
fn commands(wire: &[Value], kind: &str) -> usize {
    wire.iter().filter(|v| v["type"] == kind).count()
}
fn successful(out: &Outcome) {
    assert!(out.peer.is_ok(), "{:?}", out.peer);
    let report = out.report.as_ref().unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0, "{report:?}");
    assert!(!out
        .frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::Error(_))));
}
fn failed_closed(out: &Outcome) {
    assert!(
        out.peer.is_ok(),
        "peer owner did not finish: {:?}",
        out.peer
    );
    assert!(
        matches!(&out.report, Err(PipelineError::Execution(message)) if message == FATAL_MESSAGE),
        "{:?}",
        out.report
    );
    assert!(
        out.errors
            .iter()
            .any(|e| e.fatal && e.error == FATAL_MESSAGE),
        "{:?}",
        out.errors
    );
    assert!(out.closed_command.is_ok(), "{:?}", out.closed_command);
    assert_eq!(
        commands(&out.wire, "response.create"),
        1,
        "a rejected session cannot create another response"
    );
}

#[tokio::test]
async fn active_cancel_done_and_correlated_error_commute_without_early_inference() {
    for error_first in [false, true] {
        let mut rig = Rig::new(true, false, None).await;
        let work: Check<Vec<String>> = async {
            rig.peer.command("session.update").await?;
            rig.peer.fence().await?;
            rig.run().await?;
            rig.created("old").await?;
            let cancel = rig.interrupt().await?;
            if !error_first {
                rig.done("old").await?;
            }
            rig.peer
                .action(PeerAction::Event(provider_error(
                    "benign",
                    "response_cancel_not_active",
                    Some(json!(cancel)),
                )))
                .await?;
            rig.peer.fence().await?;
            let before_done = if error_first {
                rig.deferred_run().await?;
                rig.peer.quiet().await?;
                rig.collect_tap()?;
                let starts = response_frames(&rig.frames, true);
                rig.done("old").await?;
                rig.peer.command("response.create").await?;
                starts
            } else {
                rig.peer.quiet().await?;
                rig.run().await?;
                vec!["old".into()]
            };
            rig.created("next").await?;
            rig.done("next").await?;
            rig.observed(|f| {
                matches!(f.frame.data, FrameData::LlmResponseEnd)
                    && f.frame.metadata.get("response_id").and_then(Value::as_str) == Some("next")
            })
            .await?;
            rig.peer.quiet().await?;
            Ok(before_done)
        }
        .await;
        let out = rig.finish(work.is_err()).await;
        let before_done = work.unwrap();
        successful(&out);
        assert_eq!(
            before_done,
            ["old"],
            "the error cannot synthesize a response start or release busy"
        );
        assert_eq!(commands(&out.wire, "response.cancel"), 1);
        assert_eq!(commands(&out.wire, "input_audio_buffer.clear"), 1);
        assert_eq!(commands(&out.wire, "response.create"), 2);
        assert_eq!(response_frames(&out.frames, true), ["old", "next"]);
        assert_eq!(response_frames(&out.frames, false), ["next"]);
    }
}

#[tokio::test]
async fn late_old_cancel_error_preserves_new_active_response_and_deferred_intent() {
    let mut rig = Rig::new(true, false, None).await;
    let work: Check<Vec<String>> = async {
        rig.peer.command("session.update").await?;
        rig.peer.fence().await?;
        rig.run().await?;
        rig.created("old").await?;
        let cancel = rig.interrupt().await?;
        rig.done("old").await?;
        rig.run().await?;
        rig.created("new").await?;
        rig.peer
            .action(PeerAction::Event(provider_error(
                "late-benign",
                "response_cancel_not_active",
                Some(json!(cancel)),
            )))
            .await?;
        rig.peer.fence().await?;
        // The additional, explicitly requested response must remain deferred.
        // Error recovery cannot clear the new active owner or call maybe_continue.
        rig.deferred_run().await?;
        rig.peer.quiet().await?;
        rig.collect_tap()?;
        let before_done = response_frames(&rig.frames, true);
        rig.done("new").await?;
        rig.peer.command("response.create").await?;
        rig.created("requested-after-new").await?;
        rig.done("requested-after-new").await?;
        rig.observed(|f| {
            matches!(f.frame.data, FrameData::LlmResponseEnd)
                && f.frame.metadata.get("response_id").and_then(Value::as_str)
                    == Some("requested-after-new")
        })
        .await?;
        rig.peer.quiet().await?;
        Ok(before_done)
    }
    .await;
    let out = rig.finish(work.is_err()).await;
    let before_done = work.unwrap();
    successful(&out);
    assert_eq!(before_done, ["old", "new"]);
    assert_eq!(
        commands(&out.wire, "response.create"),
        3,
        "exactly the three explicitly requested responses"
    );
    assert_eq!(commands(&out.wire, "response.cancel"), 1);
    assert_eq!(
        response_frames(&out.frames, true),
        ["old", "new", "requested-after-new"]
    );
    assert_eq!(
        response_frames(&out.frames, false),
        ["new", "requested-after-new"]
    );
}

#[tokio::test]
async fn missing_unknown_and_outer_only_cancel_error_ids_fail_closed() {
    for variant in ["missing", "null", "unknown", "empty", "outer-only"] {
        let mut rig = Rig::new(true, false, None).await;
        let work: Check<()> = async {
            rig.peer.command("session.update").await?;
            rig.peer.fence().await?;
            rig.run().await?;
            rig.created("old").await?;
            let cancel = rig.interrupt().await?;
            let nested = match variant {
                "missing" => None,
                "null" => Some(Value::Null),
                "empty" => Some(json!("")),
                _ => Some(json!("never-sent-cancel")),
            };
            let outer = if variant == "outer-only" {
                cancel.as_str()
            } else {
                "server-error"
            };
            rig.peer
                .action(PeerAction::Event(provider_error(
                    outer,
                    "response_cancel_not_active",
                    nested,
                )))
                .await?;
            rig.fatal_receipt().await
        }
        .await;
        let out = rig.finish(true).await;
        work.unwrap();
        failed_closed(&out);
    }
}

#[tokio::test]
async fn consumed_cancel_ids_and_wrong_codes_remain_fatal() {
    for variant in [
        "duplicate",
        "invalid_value",
        "conversation_already_has_active_response",
    ] {
        let mut rig = Rig::new(true, false, None).await;
        let work: Check<()> = async {
            rig.peer.command("session.update").await?;
            rig.peer.fence().await?;
            rig.run().await?;
            rig.created("old").await?;
            let cancel = rig.interrupt().await?;
            if variant == "duplicate" {
                rig.peer
                    .action(PeerAction::Event(provider_error(
                        "first-error",
                        "response_cancel_not_active",
                        Some(json!(cancel)),
                    )))
                    .await?;
                rig.peer.fence().await?;
                rig.peer.quiet().await?;
            }
            let code = if variant == "duplicate" {
                "response_cancel_not_active"
            } else {
                variant
            };
            // A new outer server ID does not renew the consumed client receipt.
            rig.peer
                .action(PeerAction::Event(provider_error(
                    "second-error",
                    code,
                    Some(json!(cancel)),
                )))
                .await?;
            rig.fatal_receipt().await
        }
        .await;
        let out = rig.finish(true).await;
        work.unwrap();
        failed_closed(&out);
    }
}

#[tokio::test]
async fn pending_only_cancel_is_fatal_and_standalone_errors_stay_visible() {
    for variant in [
        "pending-cancel",
        "pending-create-cancel-code",
        "pending-create-active-code",
    ] {
        let mut rig = Rig::new(true, false, None).await;
        let work: Check<()> = async {
            rig.peer.command("session.update").await?;
            rig.peer.fence().await?;
            rig.run().await?;
            let create_id = rig
                .peer
                .records
                .iter()
                .rev()
                .find(|v| v["type"] == "response.create")
                .and_then(|v| v["event_id"].as_str())
                .ok_or("missing real pending-create ID")?
                .to_owned();
            // The request reached the wire, but response.created has not established
            // an active owner. Neither this cancel nor the create ID can authorize recovery.
            let cancel = rig.interrupt().await?;
            // A fatal error must not run generic pending-error recovery and emit
            // this already-admitted deferred response before closing the session.
            rig.deferred_run().await?;
            let target = if variant == "pending-cancel" {
                cancel
            } else {
                create_id
            };
            let code = if variant == "pending-create-active-code" {
                "conversation_already_has_active_response"
            } else {
                "response_cancel_not_active"
            };
            rig.peer
                .action(PeerAction::Event(provider_error(
                    "pending-error",
                    code,
                    Some(json!(target)),
                )))
                .await?;
            rig.fatal_receipt().await
        }
        .await;
        let out = rig.finish(true).await;
        work.unwrap();
        failed_closed(&out);
        assert!(response_frames(&out.frames, true).is_empty());
    }

    let (mut peer, config) = Peer::new(true, false).await;
    let connected = RealtimeClient::new(config).unwrap().connect().await;
    let mut session = match connected {
        Ok(session) => session,
        Err(e) => {
            let _ = peer.action(PeerAction::Close).await;
            let _ = peer.join().await;
            panic!("standalone connect failed after peer joined: {e}");
        }
    };
    let sender = session.sender();
    let work: Check<String> = async {
        peer.command("session.update").await?;
        peer.fence().await?;
        sender
            .send(Action::CreateResponse(ResponseOptions::default()))
            .await
            .map_err(error)?;
        peer.command("response.create").await?;
        peer.action(PeerAction::Event(response_event("raw-active", false)))
            .await?;
        timeout(WAIT, async {
            loop {
                let event = session
                    .next_event()
                    .await
                    .ok_or("raw session ended")?
                    .map_err(error)?;
                if matches!(event.event, Event::ResponseCreated { .. }) {
                    return Ok::<(), String>(());
                }
            }
        })
        .await
        .map_err(error)??;
        sender.interrupt(None);
        let cancel = peer.command("response.cancel").await?;
        let id = cancel["event_id"]
            .as_str()
            .ok_or("missing raw cancel ID")?
            .to_owned();
        peer.action(PeerAction::Event(provider_error(
            "raw-error",
            "response_cancel_not_active",
            Some(json!(id)),
        )))
        .await?;
        timeout(WAIT, async {
            loop {
                let event = session
                    .next_event()
                    .await
                    .ok_or("raw session ended before error")?
                    .map_err(error)?;
                if let Event::Error { error } = event.event {
                    return error
                        .event_id
                        .ok_or_else(|| "raw error lost client ID".to_owned());
                }
            }
        })
        .await
        .map_err(error)?
    }
    .await;
    let shutdown = session.shutdown().await;
    let (wire, joined) = peer.join().await;
    let visible_id = work.unwrap();
    shutdown.unwrap();
    joined.unwrap();
    let sent_cancel = wire
        .iter()
        .find(|v| v["type"] == "response.cancel")
        .unwrap();
    assert_eq!(sent_cancel["event_id"], visible_id);
    assert_eq!(commands(&wire, "response.create"), 1);
    assert_eq!(commands(&wire, "input_audio_buffer.clear"), 0);
}
