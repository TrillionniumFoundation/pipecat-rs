//! Paired manual-turn pre-roll over real local WebSockets. A wire append followed
//! by an actor Pong is the local flush/ring barrier; it is not a provider ACK.
//! The imported regressions retain their original helper/lifecycle contracts.
#[path = "review_aggregation.rs"]
mod existing_aggregation;
#[path = "input_audio_family.rs"]
mod existing_input_audio;
#[path = "realtime.rs"]
mod existing_realtime;
#[path = "review_recovery.rs"]
mod existing_recovery;

use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_realtime::*;
use pipecat_services::Context;
use pipecat_turns::{LLMContextAggregatorPair, UserAggregatorConfig, UserTurnConfig};
use serde_json::{json, Value};
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);
const MAX_RECORDS: usize = 256;
const CHUNK: usize = 4096;
type Check<T> = Result<T, String>;
fn error(e: impl Display) -> String {
    e.to_string()
}

enum PeerAction {
    Event(Value),
    Ping(u64),
    InvalidBinary,
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
    next_ping: u64,
}
impl Peer {
    async fn new(ready: bool, server_vad: bool) -> (Self, RealtimeConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("synthetic-preroll-loopback");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        config.limits.operation_timeout = Duration::from_secs(2);
        config.limits.response_timeout = WAIT;
        config.limits.close_timeout = Duration::from_secs(1);
        config.limits.max_audio_chunk_bytes = CHUNK;
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
                            Some(PeerAction::InvalidBinary) => Message::Binary(vec![9,8,7].into()),
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
        }
        Ok(item)
    }
    async fn command(&mut self, kind: &str) -> Check<Value> {
        timeout(WAIT, async {
            loop {
                if let Wire::Json(value) = self.record().await? {
                    if value["type"] == kind {
                        return Ok(value);
                    }
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
    json!({"event_id":"ready","type":"session.updated","session":{"id":"preroll-fixture","type":"realtime"}})
}
fn response_event(id: &str, done: bool) -> Value {
    if done {
        json!({"event_id":"finished","type":"response.done","response":{"id":id,"status":"cancelled","output":[]}})
    } else {
        json!({"event_id":"created","type":"response.created","response":{"id":id,"status":"in_progress","output":[]}})
    }
}
struct Tap(mpsc::Sender<DirectedFrame>);
#[async_trait]
impl FrameProcessor for Tap {
    fn name(&self) -> &str {
        "PrerollPostServiceReceipt"
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
    tapped: mpsc::Receiver<DirectedFrame>,
    frames: Vec<DirectedFrame>,
}
struct Outcome {
    wire: Vec<Value>,
    frames: Vec<DirectedFrame>,
    report: Result<WorkerReport, PipelineError>,
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
    async fn input(&mut self, bytes: &[u8], user_audio: bool) -> Check<()> {
        for part in bytes.chunks(CHUNK) {
            let audio = AudioRawFrame::new(part.to_vec(), 24_000, 1).map_err(error)?;
            let data = if user_audio {
                FrameData::UserAudio(UserAudioRawFrame {
                    audio,
                    user_id: "speaker".into(),
                })
            } else {
                FrameData::InputAudio(audio)
            };
            self.frame(data).await?;
            self.peer.command("input_audio_buffer.append").await?;
        }
        self.peer.fence().await
    }
    async fn interrupt(&mut self) -> Check<()> {
        self.frame(FrameData::Interruption).await?;
        self.peer.fence().await
    }
    async fn resize(&mut self, start_secs: f64) -> Check<()> {
        self.frame(FrameData::SpeechControlParams(SpeechControlParamsFrame {
            vad_params: Some(VadParams {
                start_secs,
                ..Default::default()
            }),
            turn_params: None,
        }))
        .await?;
        self.peer.fence().await
    }
    async fn finish(mut self, cancel: bool) -> Outcome {
        let mut terminal = if self.worker.has_finished() {
            Ok(())
        } else if cancel {
            timeout(WAIT, self.worker.cancel("preroll cleanup"))
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
        let (wire, peer) = self.peer.join().await;
        // The caller performs every assertion after both owners really joined.
        Outcome {
            wire,
            frames: self.frames,
            report,
            peer: terminal.and(peer),
        }
    }
}
fn pcm(samples: usize, seed: u32) -> Vec<u8> {
    (0..samples)
        .flat_map(|i| (((i as u32 * 31 + seed) % 32767) as i16).to_le_bytes())
        .collect()
}
fn pcm_wire(value: &Value) -> Vec<u8> {
    STANDARD.decode(value["audio"].as_str().unwrap()).unwrap()
}
fn replays(wire: &[Value]) -> Vec<Vec<u8>> {
    let mut result: Vec<Vec<u8>> = vec![];
    let mut active = false;
    for value in wire {
        match value["type"].as_str() {
            Some("input_audio_buffer.clear") => {
                result.push(vec![]);
                active = true;
            }
            Some("input_audio_buffer.append") if active => {
                let bytes = pcm_wire(value);
                assert!(!bytes.is_empty() && bytes.len() <= CHUNK && bytes.len().is_multiple_of(2));
                result.last_mut().unwrap().extend(bytes)
            }
            // Ping/Pong is a fixture barrier and isn't retained as a JSON command.
            Some(
                "response.cancel"
                | "conversation.item.truncate"
                | "input_audio_buffer.commit"
                | "response.create",
            ) => active = false,
            _ => {}
        }
    }
    result
}
fn kinds(wire: &[Value]) -> Vec<&str> {
    wire.iter().filter_map(|v| v["type"].as_str()).collect()
}
fn successful(out: &Outcome) {
    assert!(out.peer.is_ok(), "{:?}", out.peer);
    let report = out.report.as_ref().unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0, "{report:?}");
}
fn no_inference(wire: &[Value]) {
    assert!(!kinds(wire).iter().any(|v| matches!(
        *v,
        "input_audio_buffer.commit"
            | "response.create"
            | "response.cancel"
            | "conversation.item.truncate"
    )));
}

#[tokio::test]
async fn paired_default_suffix_uses_resolved_interruption_not_started_alone() {
    for resolved_by_vad in [false, true] {
        let mut rig = Rig::new(true, false, None).await;
        let audio = pcm(18_000, 11);
        let work: Check<(usize, usize)> = async {
            rig.peer.command("session.update").await?;
            rig.peer.fence().await?;
            rig.input(&audio, resolved_by_vad).await?;
            let before = rig.peer.records.len();
            if resolved_by_vad {
                rig.send_only(FrameData::VadUserStartedSpeakingDetailed(Default::default()))
                    .await?;
                rig.observed(|f| matches!(f.frame.data, FrameData::Interruption))
                    .await?;
                rig.observed(|f| matches!(f.frame.data, FrameData::UserStartedSpeaking))
                    .await?;
                rig.peer.fence().await?;
            } else {
                rig.frame(FrameData::UserStartedSpeaking).await?;
                rig.peer.fence().await?;
                let after_started = rig.peer.records.len();
                rig.interrupt().await?;
                return Ok((before, after_started));
            }
            Ok((before, before))
        }
        .await;
        let out = rig.finish(work.is_err()).await;
        let (before, after_started) = work.unwrap();
        successful(&out);
        assert_eq!(before, after_started, "Started alone has no wire effect");
        assert_eq!(
            replays(&out.wire[before..]),
            [audio[audio.len() - 24_000..].to_vec()]
        );
        no_inference(&out.wire);
        if resolved_by_vad {
            assert!(out
                .frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::Interruption)));
        }
    }
}

#[tokio::test]
async fn queued_pre_ready_audio_is_not_promoted_and_current_live_audio_is_once() {
    let mut rig = Rig::new(false, false, None).await;
    let stale = pcm(700, 21);
    let live = pcm(900, 31);
    let work: Check<(usize, usize, usize)> = async {
        rig.peer.command("session.update").await?;
        rig.frame(FrameData::InputAudio(
            AudioRawFrame::new(stale, 24_000, 1).map_err(error)?,
        ))
        .await?;
        // Post-service receipt proves actual sender queue admission, not just
        // pipeline admission. Readiness still prevents all audio socket writes.
        rig.interrupt().await?;
        let before_ready = rig.peer.records.len();
        rig.peer.action(PeerAction::Event(ready_event())).await?;
        rig.peer.fence().await?;
        let after_ready = rig.peer.records.len();
        rig.input(&live, false).await?;
        let after_live = rig.peer.records.len();
        rig.interrupt().await?;
        Ok((before_ready, after_ready, after_live))
    }
    .await;
    let out = rig.finish(work.is_err()).await;
    let (before_ready, after_ready, after_live) = work.unwrap();
    successful(&out);
    let appends: Vec<_> = out
        .wire
        .iter()
        .filter(|v| v["type"] == "input_audio_buffer.append")
        .map(pcm_wire)
        .collect();
    assert_eq!(appends, [live.clone(), live.clone()]);
    assert_eq!(kinds(&out.wire[..before_ready]), ["session.update"]);
    assert_eq!(
        kinds(&out.wire[before_ready..after_ready]),
        ["input_audio_buffer.clear"]
    );
    assert_eq!(
        kinds(&out.wire[after_ready..after_live]),
        ["input_audio_buffer.append"]
    );
    assert_eq!(replays(&out.wire[after_live..]), [live]);
    no_inference(&out.wire);
}

#[tokio::test]
async fn readiness_resize_and_interrupt_preserve_latest_control_without_old_audio() {
    let mut rig = Rig::new(false, false, None).await;
    let live = pcm(13_000, 43);
    let work: Check<(usize, usize)> = async {
        rig.peer.command("session.update").await?;
        rig.frame(FrameData::InputAudio(
            AudioRawFrame::new(pcm(600, 1), 24_000, 1).map_err(error)?,
        ))
        .await?;
        rig.resize(0.1).await?;
        rig.interrupt().await?;
        rig.resize(0.2).await?;
        rig.peer.action(PeerAction::Event(ready_event())).await?;
        rig.peer.fence().await?;
        let after_ready = rig.peer.records.len();
        rig.input(&live, true).await?;
        let after_live = rig.peer.records.len();
        rig.interrupt().await?;
        Ok((after_ready, after_live))
    }
    .await;
    let out = rig.finish(work.is_err()).await;
    let (after_ready, after_live) = work.unwrap();
    successful(&out);
    assert_eq!(
        kinds(&out.wire[..after_ready]),
        ["session.update", "input_audio_buffer.clear"]
    );
    assert_eq!(
        replays(&out.wire[after_live..]),
        [live[live.len() - 24_000..].to_vec()]
    );
    let sent: Vec<u8> = out.wire[after_ready..after_live]
        .iter()
        .filter(|v| v["type"] == "input_audio_buffer.append")
        .flat_map(pcm_wire)
        .collect();
    assert_eq!(sent, live);
    no_inference(&out.wire);
}

#[tokio::test]
async fn dynamic_shrink_and_growth_keep_suffix_without_restoring_evicted_audio() {
    let mut rig = Rig::new(true, false, None).await;
    let audio = pcm(18_000, 47);
    let work: Check<()> = async {
        rig.peer.command("session.update").await?;
        rig.peer.fence().await?;
        rig.input(&audio, false).await?;
        rig.resize(0.1).await?; // 0.1 + 0.3 = 0.4s = 19,200 bytes.
        rig.resize(1.0).await?; // Growth cannot restore the discarded prefix.
        rig.interrupt().await
    }
    .await;
    let out = rig.finish(work.is_err()).await;
    work.unwrap();
    successful(&out);
    assert_eq!(replays(&out.wire), [audio[audio.len() - 19_200..].to_vec()]);
    no_inference(&out.wire);
}

#[tokio::test]
async fn explicit_duration_wins_and_invalid_or_out_of_bound_durations_are_rejected() {
    for value in [
        0.0,
        -0.1,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        5.0001,
        0.000001,
    ] {
        let client = RealtimeClient::new(RealtimeConfig::new("synthetic-validation")).unwrap();
        assert!(OpenAiRealtimeProcessor::new(client)
            .unwrap()
            .with_user_audio_preroll_secs(value)
            .is_err());
    }
    let client = RealtimeClient::new(RealtimeConfig::new("synthetic-validation")).unwrap();
    assert!(OpenAiRealtimeProcessor::new(client)
        .unwrap()
        .with_user_audio_preroll_secs(5.0)
        .is_ok());
    let mut rig = Rig::new(true, false, Some(0.125)).await;
    let audio = pcm(6000, 59);
    let work: Check<()> = async {
        rig.peer.command("session.update").await?;
        rig.peer.fence().await?;
        rig.resize(3.0).await?;
        rig.input(&audio, false).await?;
        rig.interrupt().await
    }
    .await;
    let out = rig.finish(work.is_err()).await;
    work.unwrap();
    successful(&out);
    assert_eq!(replays(&out.wire), [audio[audio.len() - 6000..].to_vec()]);
    no_inference(&out.wire);
}

#[tokio::test]
async fn repeated_interruptions_do_not_reinsert_replay_or_create_inference() {
    let mut rig = Rig::new(true, false, None).await;
    let audio = pcm(1000, 61);
    let work: Check<()> = async {
        rig.peer.command("session.update").await?;
        rig.peer.fence().await?;
        rig.input(&audio, false).await?;
        rig.interrupt().await?;
        rig.interrupt().await
    }
    .await;
    let out = rig.finish(work.is_err()).await;
    work.unwrap();
    successful(&out);
    assert_eq!(replays(&out.wire), [audio.clone(), audio]);
    no_inference(&out.wire);
}

#[tokio::test]
async fn clear_replay_cancel_truncate_order_precedes_next_manual_commit() {
    let mut rig = Rig::new(true, false, None).await;
    let audio = pcm(700, 67);
    let work: Check<usize> = async {
        rig.peer.command("session.update").await?; rig.peer.fence().await?;
        rig.input(&audio, false).await?;
        rig.processor.sender().map_err(error)?.send(Action::CreateResponse(ResponseOptions::default())).await.map_err(error)?;
        rig.peer.command("response.create").await?;
        rig.peer.action(PeerAction::Event(response_event("old", false))).await?;
        rig.observed(|f| matches!(f.frame.data, FrameData::LlmResponseStart)).await?;
        rig.peer.action(PeerAction::Event(json!({"event_id":"audio","type":"response.output_audio.delta","response_id":"old","item_id":"spoken","output_index":0,"content_index":0,"delta":STANDARD.encode(pcm(1200,71))}))).await?;
        rig.observed(|f| matches!(f.frame.data, FrameData::TtsAudio(_))).await?;
        rig.processor.acknowledge_playback(PlaybackPosition { item_id: "spoken".into(), content_index: 0, audio_end_ms: 25 }).map_err(error)?;
        let before = rig.peer.records.len();
        rig.interrupt().await?;
        rig.peer.action(PeerAction::Event(response_event("old", true))).await?;
        rig.peer.fence().await?;
        // Adoption emits no interruption; its paired stop owns commit/create.
        rig.frame(FrameData::UserStartedSpeaking).await?;
        rig.send_only(FrameData::UserStoppedSpeaking).await?;
        rig.peer.command("response.create").await?;
        rig.peer.action(PeerAction::Event(response_event("new", false))).await?;
        rig.peer.action(PeerAction::Event(response_event("new", true))).await?;
        rig.peer.fence().await?;
        Ok(before)
    }.await;
    let out = rig.finish(work.is_err()).await;
    let before = work.unwrap();
    successful(&out);
    assert_eq!(
        kinds(&out.wire[before..]),
        [
            "input_audio_buffer.clear",
            "input_audio_buffer.append",
            "response.cancel",
            "conversation.item.truncate",
            "input_audio_buffer.commit",
            "response.create"
        ]
    );
    assert_eq!(pcm_wire(&out.wire[before + 1]), audio);
    assert_eq!(out.wire[before + 3]["item_id"], "spoken");
    assert_eq!(out.wire[before + 3]["audio_end_ms"], 25);
}

#[tokio::test]
async fn protocol_failure_rejects_later_commit_and_cancel_end_join_actual_owners() {
    for cancel in [false, true] {
        let mut rig = Rig::new(true, false, None).await;
        let work: Check<bool> = async {
            rig.peer.command("session.update").await?;
            rig.peer.fence().await?;
            rig.input(&pcm(600, 79), false).await?;
            rig.peer.action(PeerAction::InvalidBinary).await?;
            // next_event's deterministic protocol failure closes the actor. The
            // pipeline error receipt is observed, not a guessed socket-write failure.
            timeout(WAIT, async {
                loop {
                    let frame = rig
                        .worker
                        .recv()
                        .await
                        .ok_or("pipeline closed before error")?;
                    if matches!(frame.frame.data, FrameData::Error(_)) {
                        return Ok::<(), String>(());
                    }
                }
            })
            .await
            .map_err(error)??;
            Ok(rig
                .processor
                .sender()
                .map_err(error)?
                .try_send(Action::CommitAudio)
                .is_err())
        }
        .await;
        let out = rig.finish(cancel || work.is_err()).await;
        let rejected = work.unwrap();
        assert!(rejected);
        assert!(out.peer.is_ok(), "{:?}", out.peer);
        assert!(
            out.report.as_ref().is_err() || out.report.as_ref().is_ok_and(|r| r.total_errors > 0)
        );
        no_inference(&out.wire);
    }
}

#[tokio::test]
async fn standalone_client_and_paired_server_vad_do_not_gain_clear_or_replay() {
    let audio = pcm(700, 83);
    let mut rig = Rig::new(true, true, None).await;
    let work: Check<()> = async {
        rig.peer.command("session.update").await?;
        rig.peer.fence().await?;
        rig.input(&audio, false).await?;
        rig.resize(1.0).await?;
        rig.interrupt().await
    }
    .await;
    let out = rig.finish(work.is_err()).await;
    work.unwrap();
    successful(&out);
    assert_eq!(
        kinds(&out.wire),
        ["session.update", "input_audio_buffer.append"]
    );

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
    let work: Check<()> = async {
        peer.command("session.update").await?;
        peer.fence().await?;
        sender
            .send(Action::AppendAudio(audio.clone()))
            .await
            .map_err(error)?;
        peer.command("input_audio_buffer.append").await?;
        peer.fence().await?;
        sender.interrupt(None);
        peer.fence().await
    }
    .await;
    let shutdown = session.shutdown().await;
    let (wire, joined) = peer.join().await;
    work.unwrap();
    shutdown.unwrap();
    joined.unwrap();
    assert_eq!(
        kinds(&wire),
        ["session.update", "input_audio_buffer.append"]
    );
}
