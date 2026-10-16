//! Native raw camera over an actual default paired pipeline and loopback socket.
//! Pipeline receipts/journal and joined owner traces are independent evidence.
//! Imported regressions retain their original source and lifecycle contracts.
#[path = "cancel_race_runtime.rs"]
mod existing_cancel_race;

use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use jpeg_decoder::{Decoder, PixelFormat};
use pipecat_core::*;
use pipecat_realtime::*;
use pipecat_services::Context;
use pipecat_turns::{LLMContextAggregatorPair, SharedContext};
use serde_json::{json, Value};
use std::{
    fmt::Display,
    io::Cursor,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::{error::ProtocolError, Message};

const WAIT: Duration = Duration::from_secs(5);
const MAX_RECORDS: usize = 256;
type Check<T> = Result<T, String>;
fn error(value: impl Display) -> String {
    value.to_string()
}
fn require(ok: bool, message: &str) -> Check<()> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn ready_event() -> Value {
    json!({"event_id":"camera-ready","type":"session.updated","session":{"id":"camera-session","type":"realtime"}})
}
struct Peer {
    events: mpsc::Sender<Value>,
    wire: mpsc::Receiver<Value>,
    task: JoinHandle<Check<()>>,
    records: Vec<Value>,
    cancelled: Arc<AtomicBool>,
}
impl Peer {
    async fn new(ready: bool, budget: Option<usize>) -> (Self, RealtimeConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("synthetic-camera-loopback");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        config.limits.operation_timeout = Duration::from_secs(2);
        config.limits.close_timeout = Duration::from_secs(1);
        if let Some(budget) = budget {
            config.limits.max_message_bytes = budget;
            config.limits.max_audio_chunk_bytes = budget / 4;
        }
        let (events, mut event_rx) = mpsc::channel::<Value>(16);
        let (wire_tx, wire) = mpsc::channel(64);
        let cancelled = Arc::new(AtomicBool::new(false));
        let was_cancelled = cancelled.clone();
        let task = tokio::spawn(async move {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(error)?
                .map_err(error)?;
            let mut ws = timeout(WAIT, tokio_tungstenite::accept_async(tcp))
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
            require(first["type"] == "session.update", "wrong initial command")?;
            require(first["session"]["type"] == "realtime", "wrong session type")?;
            wire_tx.try_send(first).map_err(error)?;
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
                    event = event_rx.recv() => {
                        let Some(event) = event else {
                            return Err("peer controller closed before client terminal".into());
                        };
                        timeout(WAIT, ws.send(Message::Text(event.to_string().into())))
                            .await.map_err(error)?.map_err(error)?;
                    }
                    message = ws.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            require(text.len() <= 1024 * 1024, "peer message bound")?;
                            wire_tx.try_send(serde_json::from_str(&text).map_err(error)?)
                                .map_err(error)?;
                        }
                        Some(Ok(Message::Ping(payload))) => {
                            timeout(WAIT, ws.send(Message::Pong(payload)))
                                .await.map_err(error)?.map_err(error)?;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) => {
                            match timeout(WAIT, ws.flush()).await.map_err(error)? {
                                Ok(()) | Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) => return Ok(()),
                                Err(e) => return Err(error(e)),
                            }
                        }
                        None if was_cancelled.load(Ordering::SeqCst) => return Ok(()),
                        None => return Err("graceful terminal closed without websocket Close".into()),
                        // Cancellation may abort a socket owner without a close handshake.
                        Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(
                            ProtocolError::ResetWithoutClosingHandshake,
                        ))) if was_cancelled.load(Ordering::SeqCst) => return Ok(()),
                        Some(Err(e)) => return Err(error(e)),
                        other => return Err(format!("unexpected client wire {other:?}")),
                    }
                }
            }
        });
        (
            Self {
                events,
                wire,
                task,
                records: vec![],
                cancelled,
            },
            config,
        )
    }
    async fn ready(&self) -> Check<()> {
        timeout(WAIT, self.events.send(ready_event()))
            .await
            .map_err(error)?
            .map_err(error)
    }
    async fn command(&mut self, kind: &str) -> Check<Value> {
        timeout(WAIT, async {
            loop {
                let value = self.wire.recv().await.ok_or("wire closed")?;
                require(self.records.len() < MAX_RECORDS, "wire observation bound")?;
                self.records.push(value.clone());
                if value["type"] == kind {
                    return Ok(value);
                }
            }
        })
        .await
        .map_err(error)?
    }
    async fn join(mut self) -> (Vec<Value>, Check<()>) {
        let result = match timeout(WAIT, &mut self.task).await {
            Ok(result) => result.map_err(error).and_then(|r| r),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Err("peer deadline; aborted owner was actually joined".into())
            }
        };
        while let Ok(value) = self.wire.try_recv() {
            if self.records.len() == MAX_RECORDS {
                return (self.records, Err("wire observation bound".into()));
            }
            self.records.push(value);
        }
        (self.records, result)
    }
}

struct Tap {
    stage: &'static str,
    receipts: mpsc::Sender<(&'static str, DirectedFrame)>,
}
#[async_trait]
impl FrameProcessor for Tap {
    fn name(&self) -> &str {
        self.stage
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.receipts
            .try_send((self.stage, DirectedFrame::new(frame.clone(), direction)))
            .map_err(|e| ProcessorError::new(e.to_string()))?;
        ctx.push(frame, direction).await
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Terminal {
    End,
    Stop,
    Cancel,
}
struct Rig {
    worker: PipelineWorker,
    peer: Peer,
    processor: Arc<OpenAiRealtimeProcessor>,
    sender: RealtimeSender,
    context: SharedContext,
    receipts: mpsc::Receiver<(&'static str, DirectedFrame)>,
    frames: Vec<(&'static str, DirectedFrame)>,
    terminal_admitted: bool,
}
struct Outcome {
    wire: Vec<Value>,
    frames: Vec<(&'static str, DirectedFrame)>,
    errors: Vec<ErrorFrame>,
    context: Check<LlmContextData>,
    report: Result<WorkerReport, PipelineError>,
    owners: Check<()>,
    sender_closed: Check<()>,
}
impl Rig {
    async fn new(ready: bool, paused: bool, budget: Option<usize>) -> Self {
        Self::with_turn_detection(ready, paused, budget, false).await
    }
    async fn with_turn_detection(
        ready: bool,
        paused: bool,
        budget: Option<usize>,
        server_vad: bool,
    ) -> Self {
        let (peer, mut config) = Peer::new(ready, budget).await;
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
        let mut processor =
            OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap();
        if paused {
            processor = processor.with_video_input_paused(true);
        }
        let processor = Arc::new(processor);
        // Deliberately use the real pair's defaults, including its realtime image
        // rejection. A passive UserImage must never reach that assistant.
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            Default::default(),
            Default::default(),
        );
        let context = pair.context();
        let (receipt_tx, receipts) = mpsc::channel(128);
        let result = Pipeline::new(vec![
            pair.user(),
            processor.clone(),
            Arc::new(Tap {
                stage: "after-service",
                receipts: receipt_tx.clone(),
            }),
            pair.assistant(),
            Arc::new(Tap {
                stage: "after-assistant",
                receipts: receipt_tx,
            }),
        ])
        .start(PipelineConfig {
            start: StartFrame {
                audio_in_sample_rate: 24_000,
                audio_out_sample_rate: 24_000,
                ..Default::default()
            },
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .await;
        match result {
            Ok(worker) => Self {
                worker,
                peer,
                sender: processor.sender().unwrap(),
                processor,
                context,
                receipts,
                frames: vec![],
                terminal_admitted: false,
            },
            Err(e) => {
                let _ = peer.join().await;
                panic!("pipeline start failed after peer owner joined: {e}");
            }
        }
    }
    async fn send(&self, frame: Frame, direction: Direction) -> Check<()> {
        timeout(WAIT, self.worker.sender().send(frame, direction))
            .await
            .map_err(error)?
            .map_err(error)
    }
    async fn receipt(&mut self, stage: &str, id: u64, direction: Direction) -> Check<()> {
        timeout(WAIT, async {
            loop {
                if self
                    .frames
                    .iter()
                    .any(|(s, f)| *s == stage && f.frame.id == id && f.direction == direction)
                {
                    return Ok(());
                }
                let receipt = self.receipts.recv().await.ok_or("receipt tap closed")?;
                require(self.frames.len() < MAX_RECORDS, "frame observation bound")?;
                self.frames.push(receipt);
            }
        })
        .await
        .map_err(error)?
    }
    async fn barrier(&mut self) -> Check<()> {
        // This inert system frame follows InputImage/UserImage in their actual
        // same-priority queue. Its post-assistant receipt establishes pipeline
        // completion even when the earlier image was consumed or rejected.
        let frame = Frame::new(FrameData::OutputTransportMessageUrgent(json!({
            "camera_test_receipt": true
        })));
        let id = frame.id;
        self.send(frame, Direction::Downstream).await?;
        self.receipt("after-assistant", id, Direction::Downstream)
            .await
    }
    async fn image(&mut self, frame: Frame, direction: Direction) -> Check<()> {
        self.send(frame, direction).await?;
        self.barrier().await
    }
    fn journal(&self, expected: u64) -> Check<()> {
        require(
            self.worker.total_errors() == expected,
            "unexpected error count",
        )?;
        require(
            self.worker
                .errors()
                .iter()
                .all(|e| !e.fatal && e.processor.as_deref() == Some("OpenAiRealtimeService")),
            "image rejection escaped service ownership or became fatal",
        )?;
        require(
            self.worker
                .processor_health(1)
                .is_some_and(|h| h.is_usable()),
            "image rejection made the realtime service unusable",
        )
    }
    async fn terminal(&mut self, terminal: Terminal) -> Check<()> {
        if self.worker.has_finished() || self.terminal_admitted {
            return Ok(());
        }
        if terminal == Terminal::Cancel {
            self.peer.cancelled.store(true, Ordering::SeqCst);
        }
        timeout(WAIT, async {
            match terminal {
                Terminal::End => self.worker.end().await,
                Terminal::Stop => {
                    self.worker
                        .sender()
                        .send(Frame::new(FrameData::Stop), Direction::Downstream)
                        .await
                }
                Terminal::Cancel => self.worker.cancel("camera fixture cleanup").await,
            }
        })
        .await
        .map_err(error)?
        .map_err(error)?;
        self.terminal_admitted = true;
        Ok(())
    }
    async fn finish(mut self, terminal: Terminal) -> Outcome {
        let admission = self.terminal(terminal).await;
        if admission.is_err() {
            self.peer.cancelled.store(true, Ordering::SeqCst);
            let _ = self.worker.cancel("camera terminal admission failed").await;
        }
        let mut owners = admission;
        let report = match timeout(WAIT, self.worker.join()).await {
            Ok(result) => result,
            Err(_) => {
                self.peer.cancelled.store(true, Ordering::SeqCst);
                let _ = self.worker.cancel("camera join deadline").await;
                owners = Err("worker join deadline; cancellation owner actually joined".into());
                self.worker.join().await
            }
        };
        while let Ok(frame) = self.receipts.try_recv() {
            if self.frames.len() == MAX_RECORDS {
                owners = Err("frame observation bound".into());
                break;
            }
            self.frames.push(frame);
        }
        let errors = self.worker.errors();
        let context = self.context.snapshot_rich().map_err(error);
        let (wire, peer) = self.peer.join().await;
        let sender_closed = match timeout(WAIT, self.sender.send(Action::CommitAudio)).await {
            Ok(Err(RealtimeError::Closed)) => Ok(()),
            other => Err(format!("joined owner accepted later command: {other:?}")),
        };
        Outcome {
            wire,
            frames: self.frames,
            errors,
            context,
            report,
            owners: owners.and(peer),
            sender_closed,
        }
    }
}
fn rgb(color: Option<[u8; 3]>) -> ImageRawFrame {
    let mut image = Vec::with_capacity(32 * 16 * 3);
    for y in 0..16 {
        for x in 0..32 {
            let pixel = color.unwrap_or(match (x < 16, y < 8) {
                (true, true) => [240, 15, 15],
                (false, true) => [15, 240, 15],
                (true, false) => [15, 15, 240],
                (false, false) => [240, 240, 240],
            });
            image.extend_from_slice(&pixel);
        }
    }
    ImageRawFrame {
        image,
        size: (32, 16),
        format: Some("RGB".into()),
    }
}
fn input() -> Frame {
    Frame::new(FrameData::InputImage(rgb(None)))
}
fn request() -> UserImageRequestFrame {
    UserImageRequestFrame {
        user_id: "camera-owner".into(),
        text: Some("local request association only".into()),
        append_to_context: Some(false),
        video_source: Some("screenVideo".into()),
        function_name: None,
        tool_call_id: None,
        result_callback: None,
    }
}
fn passive() -> UserImageRawFrame {
    UserImageRawFrame {
        image: rgb(Some([15, 15, 240])),
        user_id: "camera-owner".into(),
        text: Some("must not become text context".into()),
        append_to_context: None,
        request: Some(Box::new(request())),
        run_llm: Some(false),
    }
}
fn malformed() -> Frame {
    Frame::new(FrameData::InputImage(ImageRawFrame {
        image: vec![0],
        size: (2, 2),
        format: Some("RGB".into()),
    }))
}
fn camera_items(wire: &[Value]) -> Vec<&Value> {
    wire.iter()
        .filter(|v| {
            v["type"] == "conversation.item.create"
                && v["item"]["content"][0]["type"] == "input_image"
        })
        .collect()
}
fn decode_camera(value: &Value) -> Vec<u8> {
    assert_eq!(value["type"], "conversation.item.create");
    assert_eq!(value["item"]["type"], "message");
    assert_eq!(value["item"]["role"], "user");
    let parts = value["item"]["content"].as_array().unwrap();
    assert_eq!(parts.len(), 1, "camera must not invent accompanying text");
    assert_eq!(parts[0]["type"], "input_image");
    assert_eq!(parts[0]["detail"], "auto");
    assert_eq!(parts[0].as_object().unwrap().len(), 3);
    assert!(value["item"].get("user_id").is_none());
    let uri = parts[0]["image_url"].as_str().unwrap();
    let jpeg = STANDARD
        .decode(uri.strip_prefix("data:image/jpeg;base64,").unwrap())
        .unwrap();
    let mut decoder = Decoder::new(Cursor::new(jpeg));
    let pixels = decoder
        .decode()
        .expect("wire contains an actual decodable JPEG");
    let info = decoder.info().unwrap();
    assert_eq!((info.width, info.height), (32, 16));
    assert_eq!(info.pixel_format, PixelFormat::RGB24);
    assert_eq!(pixels.len(), 32 * 16 * 3);
    pixels
}
fn pixel(pixels: &[u8], x: usize, y: usize, expected: [u8; 3]) {
    let actual = &pixels[(y * 32 + x) * 3..(y * 32 + x + 1) * 3];
    for (actual, expected) in actual.iter().zip(expected) {
        assert!(
            actual.abs_diff(expected) <= 35,
            "lossy pixel {actual} != {expected}"
        );
    }
}
fn assert_outcome(out: &Outcome, terminal: Terminal, errors: u64) {
    assert!(out.owners.is_ok(), "{:?}", out.owners);
    assert!(out.sender_closed.is_ok(), "{:?}", out.sender_closed);
    let report = out.report.as_ref().unwrap();
    assert_eq!(
        report.state,
        match terminal {
            Terminal::End => WorkerState::Ended,
            Terminal::Stop => WorkerState::Stopped,
            Terminal::Cancel => WorkerState::Cancelled,
        }
    );
    assert_eq!(report.total_errors, errors, "{report:?}");
    assert_eq!(out.errors.len() as u64, errors, "{:?}", out.errors);
    assert!(out.errors.iter().all(|e| !e.fatal));
    assert!(
        out.context.as_ref().unwrap().messages.is_empty(),
        "camera polluted paired context"
    );
    assert!(
        out.wire.iter().all(|v| v["type"] != "response.create"),
        "camera must never initiate inference: {:?}",
        out.wire
    );
    assert!(
        out.wire.iter().all(|v| matches!(
            v["type"].as_str(),
            Some(
                "session.update"
                    | "conversation.item.create"
                    | "input_audio_buffer.clear"
                    | "response.cancel"
            )
        )),
        "unexpected camera wire effect: {:?}",
        out.wire
    );
}
fn assert_camera_only(out: &Outcome) {
    assert!(
        out.wire.iter().all(|v| v["type"] == "session.update"
            || (v["type"] == "conversation.item.create"
                && v["item"]["content"][0]["type"] == "input_image")),
        "unexpected non-camera command: {:?}",
        out.wire
    );
}

#[tokio::test]
async fn default_pair_decodes_camera_and_consumes_passive_user_with_shared_throttle() {
    let mut rig = Rig::new(true, false, None).await;
    let mut original = input();
    original.name = "camera-original".into();
    original.set_pipeline_pts(12345);
    original.transport_source = Some("webcam".into());
    original.transport_destination = Some("display".into());
    original
        .metadata
        .insert("capture".into(), json!({"sequence": 7}));
    let tag = LocalFrameTag::new();
    tag.mark(&mut original).unwrap();
    let user = Frame::new(FrameData::UserImage(passive()));
    let later = Frame::new(FrameData::UserImage(passive()));
    let checked = async {
        rig.image(original.clone(), Direction::Downstream).await?;
        rig.image(user.clone(), Direction::Downstream).await?;
        rig.journal(0)?;
        tokio::time::sleep(Duration::from_millis(1050)).await;
        rig.image(later.clone(), Direction::Downstream).await?;
        rig.journal(0)
    }
    .await;
    let out = rig.finish(Terminal::End).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_outcome(&out, Terminal::End, 0);
    assert_camera_only(&out);
    let items = camera_items(&out.wire);
    assert_eq!(
        items.len(),
        2,
        "InputImage and all UserImage sources share 1 Hz"
    );
    let pixels = decode_camera(items[0]);
    pixel(&pixels, 4, 3, [240, 15, 15]);
    pixel(&pixels, 27, 3, [15, 240, 15]);
    pixel(&pixels, 4, 12, [15, 15, 240]);
    pixel(&pixels, 27, 12, [240, 240, 240]);
    pixel(&decode_camera(items[1]), 16, 8, [15, 15, 240]);
    for stage in ["after-service", "after-assistant"] {
        let forwarded = out
            .frames
            .iter()
            .find(|(s, f)| *s == stage && f.frame.id == original.id)
            .unwrap();
        assert_eq!(forwarded.1.frame.data, original.data);
        assert_eq!(forwarded.1.frame.name, original.name);
        assert_eq!(forwarded.1.frame.pts, original.pts);
        assert_eq!(forwarded.1.frame.metadata, original.metadata);
        assert_eq!(
            forwarded.1.frame.transport_source,
            original.transport_source
        );
        assert_eq!(
            forwarded.1.frame.transport_destination,
            original.transport_destination
        );
        assert!(tag.contains(&forwarded.1.frame));
    }
    assert!(
        out.frames
            .iter()
            .all(|(_, f)| f.frame.id != user.id && f.frame.id != later.id),
        "passive UserImage reached realtime-rejecting paired assistant"
    );
}

#[tokio::test]
async fn pause_skips_codec_without_resetting_window_or_retracting_admitted_camera() {
    let mut rig = Rig::new(true, true, None).await;
    let invalid = malformed();
    let checked = async {
        rig.image(invalid.clone(), Direction::Downstream).await?;
        let mut user = passive();
        user.image.image.clear();
        rig.image(
            Frame::new(FrameData::UserImage(user)),
            Direction::Downstream,
        )
        .await?;
        rig.journal(0)?;
        rig.processor.set_video_input_paused(false);
        rig.image(input(), Direction::Downstream).await?;
        rig.processor.set_video_input_paused(true);
        rig.processor.set_video_input_paused(false);
        rig.image(input(), Direction::Downstream).await?;
        rig.processor.set_video_input_paused(true);
        tokio::time::sleep(Duration::from_millis(1050)).await;
        rig.image(malformed(), Direction::Downstream).await?;
        rig.journal(0)
    }
    .await;
    let out = rig.finish(Terminal::End).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_outcome(&out, Terminal::End, 0);
    assert_camera_only(&out);
    assert_eq!(camera_items(&out.wire).len(), 1);
    assert!(out
        .frames
        .iter()
        .any(|(s, f)| *s == "after-assistant" && f.frame.id == invalid.id));

    // Readiness is withheld until after pause: this image is demonstrably
    // queued, so a pause must not retract work already admitted by service.
    let mut rig = Rig::new(false, false, None).await;
    let checked = async {
        rig.image(input(), Direction::Downstream).await?;
        rig.processor.set_video_input_paused(true);
        rig.peer.ready().await?;
        rig.journal(0)
    }
    .await;
    let out = rig.finish(Terminal::End).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_outcome(&out, Terminal::End, 0);
    assert_camera_only(&out);
    assert_eq!(camera_items(&out.wire).len(), 1);
}

#[tokio::test]
async fn malformed_pixels_consume_window_but_rejection_is_recoverable() {
    let mut rig = Rig::new(true, false, None).await;
    let invalid = malformed();
    let checked = async {
        rig.image(invalid.clone(), Direction::Downstream).await?;
        rig.journal(1)?;
        rig.image(input(), Direction::Downstream).await?;
        rig.journal(1)?;
        tokio::time::sleep(Duration::from_millis(1050)).await;
        rig.image(input(), Direction::Downstream).await?;
        rig.journal(1)
    }
    .await;
    let out = rig.finish(Terminal::End).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_outcome(&out, Terminal::End, 1);
    assert_camera_only(&out);
    assert_eq!(camera_items(&out.wire).len(), 1);
    assert!(out.frames.iter().all(|(_, f)| f.frame.id != invalid.id));
}

#[tokio::test]
async fn rich_requests_reject_before_pause_or_throttle_without_callback_or_context_effects() {
    let callbacks = Arc::new(AtomicUsize::new(0));
    for paused in [true, false] {
        let mut rig = Rig::new(true, paused, None).await;
        let checked = async {
            if !paused {
                rig.image(input(), Direction::Downstream).await?;
            }
            for case in 0..6 {
                let mut user = passive();
                match case {
                    0 => user.append_to_context = Some(true),
                    1 => user.run_llm = Some(true),
                    2 => user.request.as_mut().unwrap().append_to_context = Some(true),
                    3 => user.request.as_mut().unwrap().function_name = Some("look".into()),
                    4 => user.request.as_mut().unwrap().tool_call_id = Some("call-camera".into()),
                    _ => {
                        let callbacks = callbacks.clone();
                        user.request.as_mut().unwrap().result_callback =
                            Some(LocalResultCallback::new(move |_| {
                                callbacks.fetch_add(1, Ordering::SeqCst);
                                async { Ok(()) }
                            }));
                    }
                }
                rig.image(
                    Frame::new(FrameData::UserImage(user)),
                    Direction::Downstream,
                )
                .await?;
                rig.journal(case + 1)?;
            }
            Ok::<_, String>(())
        }
        .await;
        let out = rig.finish(Terminal::End).await;
        assert!(checked.is_ok(), "paused={paused}: {checked:?}");
        assert_outcome(&out, Terminal::End, 6);
        assert_camera_only(&out);
        assert_eq!(camera_items(&out.wire).len(), usize::from(!paused));
        assert!(out
            .frames
            .iter()
            .all(|(_, f)| !matches!(f.frame.data, FrameData::UserImage(_))));
    }
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn upstream_input_bypasses_codec_without_spending_downstream_window() {
    let mut rig = Rig::new(true, false, None).await;
    let invalid = malformed();
    let checked = async {
        rig.send(invalid.clone(), Direction::Upstream).await?;
        // The upstream runtime boundary is beyond service and user aggregator,
        // whereas a post-service tap sees upstream input before service does.
        timeout(WAIT, async {
            loop {
                let frame = rig.worker.recv().await.ok_or("upstream boundary closed")?;
                if frame.frame.id == invalid.id && frame.direction == Direction::Upstream {
                    return Ok::<_, String>(());
                }
            }
        })
        .await
        .map_err(error)??;
        rig.image(input(), Direction::Downstream).await?;
        rig.journal(0)
    }
    .await;
    let out = rig.finish(Terminal::End).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_outcome(&out, Terminal::End, 0);
    assert_camera_only(&out);
    assert_eq!(camera_items(&out.wire).len(), 1);
}

#[tokio::test]
async fn unsupported_formats_dimensions_and_exact_wire_budget_do_not_upload() {
    let mut bad = Vec::new();
    for format in [None, Some("RGBX"), Some("image/jpeg"), Some("rgb")] {
        let mut image = rgb(None);
        image.format = format.map(str::to_owned);
        bad.push(image);
    }
    let mut zero = rgb(None);
    zero.size = (0, 16);
    bad.push(zero);
    let mut too_wide = rgb(None);
    too_wide.size = (2049, 1);
    bad.push(too_wide);
    let mut too_many = rgb(None);
    too_many.size = (1024, 1024);
    bad.push(too_many);
    for image in bad {
        let mut rig = Rig::new(true, false, None).await;
        let checked = async {
            rig.image(
                Frame::new(FrameData::InputImage(image)),
                Direction::Downstream,
            )
            .await?;
            rig.journal(1)
        }
        .await;
        let out = rig.finish(Terminal::End).await;
        assert!(checked.is_ok(), "{checked:?}");
        assert_outcome(&out, Terminal::End, 1);
        assert_camera_only(&out);
        assert!(camera_items(&out.wire).is_empty());
    }
    let mut rig = Rig::new(true, false, Some(512)).await;
    let checked = async {
        rig.image(input(), Direction::Downstream).await?;
        rig.journal(1)
    }
    .await;
    let out = rig.finish(Terminal::End).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_outcome(&out, Terminal::End, 1);
    assert_camera_only(&out);
    assert!(camera_items(&out.wire).is_empty());
}

#[tokio::test]
async fn interruption_retires_pre_ready_camera_but_preserves_public_create_item() {
    for server_vad in [false, true] {
        let mut rig = Rig::with_turn_detection(false, false, None, server_vad).await;
        let checked = async {
            rig.image(input(), Direction::Downstream).await?;
            rig.sender
                .try_send(Action::CreateItem {
                    item: ConversationItem::Message {
                        id: "durable-public".into(),
                        role: Role::User,
                        content: vec![ItemContent::InputText {
                            text: "public history survives".into(),
                        }],
                    },
                    previous_item_id: None,
                })
                .map_err(error)?;
            let interruption = Frame::new(FrameData::Interruption);
            let id = interruption.id;
            rig.send(interruption, Direction::Downstream).await?;
            rig.receipt("after-assistant", id, Direction::Downstream)
                .await?;
            rig.peer.ready().await?;
            rig.peer.command("conversation.item.create").await?;
            tokio::time::sleep(Duration::from_millis(1050)).await;
            rig.image(
                Frame::new(FrameData::UserImage(passive())),
                Direction::Downstream,
            )
            .await?;
            rig.journal(0)
        }
        .await;
        let out = rig.finish(Terminal::End).await;
        assert!(checked.is_ok(), "server_vad={server_vad}: {checked:?}");
        assert_outcome(&out, Terminal::End, 0);
        assert_eq!(
            out.wire[0]["session"]["audio"]["input"]["turn_detection"],
            if server_vad {
                json!({
                    "type": "server_vad",
                    "threshold": 0.5,
                    "prefix_padding_ms": 300,
                    "silence_duration_ms": 500,
                    "create_response": true,
                    "interrupt_response": true
                })
            } else {
                Value::Null
            },
            "the actual socket session must exercise the selected actor branch"
        );
        let items = out
            .wire
            .iter()
            .filter(|v| v["type"] == "conversation.item.create")
            .collect::<Vec<_>>();
        assert_eq!(items.len(), 2);
        assert_eq!(items[0]["item"]["id"], "durable-public");
        let cameras = camera_items(&out.wire);
        assert_eq!(cameras.len(), 1);
        pixel(&decode_camera(cameras[0]), 16, 8, [15, 15, 240]);
    }
}

#[tokio::test]
async fn end_and_stop_drain_admitted_camera_before_finish_and_join_socket_owner() {
    for terminal in [Terminal::End, Terminal::Stop] {
        let mut rig = Rig::new(false, false, None).await;
        let checked = async {
            rig.image(input(), Direction::Downstream).await?;
            rig.journal(0)?;
            rig.terminal(terminal).await?;
            rig.peer.ready().await
        }
        .await;
        // Terminal admission is recorded separately from completion: finish()
        // does not enqueue a duplicate terminal and always actually joins.
        let out = rig.finish(terminal).await;
        assert!(checked.is_ok(), "{terminal:?}: {checked:?}");
        assert_outcome(&out, terminal, 0);
        assert_camera_only(&out);
        let items = camera_items(&out.wire);
        assert_eq!(items.len(), 1);
        decode_camera(items[0]);
    }
}

#[tokio::test]
async fn cancel_joins_owners_without_replaying_queued_pre_ready_camera() {
    let mut rig = Rig::new(false, false, None).await;
    let checked = async {
        rig.image(input(), Direction::Downstream).await?;
        rig.journal(0)
    }
    .await;
    let out = rig.finish(Terminal::Cancel).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_outcome(&out, Terminal::Cancel, 0);
    assert_camera_only(&out);
    assert!(camera_items(&out.wire).is_empty());
}
