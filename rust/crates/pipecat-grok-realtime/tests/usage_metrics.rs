//! Production WebSocket -> Grok processor -> RTVI tap usage projection.
//! Assertions inspect the real pipeline outlet after both owners have joined.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_grok_realtime::*;
use pipecat_rtvi::RtviObserver;
use serde_json::{json, Value};
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);
const MAX_RECORDS: usize = 512;
type Check<T> = Result<T, String>;
fn error(value: impl Display) -> String {
    value.to_string()
}
fn require(condition: bool, message: &str) -> Check<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
#[derive(Clone, Copy, Debug)]
struct Counts(u64, u64, u64);
impl Counts {
    fn usage(self) -> Value {
        json!({"input_tokens":self.0,"output_tokens":self.1,"total_tokens":self.2})
    }
    fn metric(self) -> FrameData {
        FrameData::Metrics(vec![Metric::LlmUsage {
            processor: "GrokRealtimeService".into(),
            prompt_tokens: self.0,
            completion_tokens: self.1,
            total_tokens: self.2,
        }])
    }
    fn rtvi(self) -> Value {
        json!({"label":"rtvi-ai","type":"metrics","data":{"tokens":[{
            "prompt_tokens":self.0,"completion_tokens":self.1,"total_tokens":self.2
        }]}})
    }
}
const BASE: Counts = Counts(7, 5, 99); // Deliberately nonadditive provider total.
fn rich_usage() -> Value {
    let mut value = BASE.usage();
    value["input_token_details"] = json!({
        "text_tokens":13,"cached_tokens":0,"audio_tokens":u64::MAX,
        "cached_tokens_details":{"audio_tokens":3}
    });
    value["output_token_details"] = json!({"text_tokens":17,"audio_tokens":11});
    value["provider_private"] = json!("must not become metrics metadata");
    value
}
fn response(id: &str, status: &str, usage: Option<Value>) -> Value {
    let mut value =
        json!({"id":id,"status":status,"conversation_id":"usage-conversation","output":[]});
    if let Some(usage) = usage {
        value["usage"] = usage;
    }
    value
}
fn created(id: &str) -> Value {
    json!({"event_id":format!("{id}-created"),"type":"response.created","response":response(id,"in_progress",None)})
}
fn done(id: &str, status: &str, top: Option<Value>, nested: Option<Value>) -> Value {
    let mut value = json!({"event_id":format!("{id}-done"),"type":"response.done","response":response(id,status,nested)});
    if let Some(usage) = top {
        value["usage"] = usage;
    }
    value
}

struct Peer {
    events: mpsc::Sender<Value>,
    wire: mpsc::Receiver<Value>,
    task: JoinHandle<Check<()>>,
    records: Vec<Value>,
}
impl Peer {
    async fn new() -> (Self, RealtimeConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("synthetic-usage-loopback");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (events, mut event_rx) = mpsc::channel::<Value>(32);
        let (wire_tx, wire) = mpsc::channel(64);
        let task = tokio::spawn(async move {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(error)?
                .map_err(error)?;
            let mut ws = timeout(WAIT, tokio_tungstenite::accept_async(tcp))
                .await
                .map_err(error)?
                .map_err(error)?;
            // Grok waits for conversation.created before session.update.
            for event in [
                json!({"event_id":"usage-session-created","type":"session.created","session":{"id":"usage-session"}}),
                json!({"event_id":"usage-conversation-created","type":"conversation.created","conversation":{"id":"usage-conversation"}}),
            ] {
                timeout(WAIT, ws.send(Message::Text(event.to_string().into())))
                    .await
                    .map_err(error)?
                    .map_err(error)?;
            }
            let first = timeout(WAIT, ws.next())
                .await
                .map_err(error)?
                .ok_or("missing session update")?
                .map_err(error)?;
            let first: Value =
                serde_json::from_str(first.to_text().map_err(error)?).map_err(error)?;
            require(
                first["type"] == "session.update",
                "first wire command must initialize the session",
            )?;
            wire_tx.try_send(first).map_err(error)?;
            timeout(WAIT, ws.send(Message::Text(json!({"event_id":"usage-ready","type":"session.updated","session":{"id":"usage-session"}}).to_string().into()))).await.map_err(error)?.map_err(error)?;
            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        let Some(event) = event else { return Err("peer controller dropped before client closed".into()) };
                        timeout(WAIT, ws.send(Message::Text(event.to_string().into()))).await.map_err(error)?.map_err(error)?;
                    }
                    message = ws.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            require(text.len() <= 1024 * 1024, "wire message bound")?;
                            wire_tx.try_send(serde_json::from_str(&text).map_err(error)?).map_err(error)?;
                        }
                        Some(Ok(Message::Ping(bytes))) => { ws.send(Message::Pong(bytes)).await.map_err(error)?; }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) => {
                            match timeout(WAIT, ws.flush()).await.map_err(error)? {
                                Ok(()) | Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed) => return Ok(()),
                                Err(error) => return Err(error.to_string()),
                            }
                        }
                        None => return Err("socket ended without client close".into()),
                        Some(Err(error)) => return Err(error.to_string()),
                        other => return Err(format!("unexpected client message: {other:?}")),
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
            },
            config,
        )
    }
    async fn event(&self, event: Value) -> Check<()> {
        timeout(WAIT, self.events.send(event))
            .await
            .map_err(error)?
            .map_err(error)
    }
    async fn command(&mut self, kind: &str) -> Check<Value> {
        timeout(WAIT, async {
            loop {
                let value = self.wire.recv().await.ok_or("peer wire closed")?;
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
            Ok(result) => result.map_err(error).and_then(|result| result),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Err("peer deadline; aborted peer actually joined".into())
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
struct Rig {
    worker: PipelineWorker,
    sender: RealtimeSender,
    peer: Peer,
    frames: Vec<DirectedFrame>,
}
struct Outcome {
    frames: Vec<DirectedFrame>,
    wire: Vec<Value>,
    report: Result<WorkerReport, PipelineError>,
    owners: Check<()>,
}
impl Rig {
    async fn new(capture: Option<bool>, metrics: bool, legacy: bool, audio: bool) -> Self {
        let (peer, config) = Peer::new().await;
        let processor =
            Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        let observer = Arc::new(
            RtviObserver::new(Default::default(), None)
                .unwrap()
                .with_llm_usage_details(legacy)
                .with_llm_audio_usage_details(audio),
        );
        let mut config = PipelineConfig::default();
        config.start.audio_in_sample_rate = 24_000;
        config.start.audio_out_sample_rate = 24_000;
        config.start.enable_metrics = metrics;
        if let Some(capture) = capture {
            config.start.enable_usage_metrics = capture;
        }
        let worker = Pipeline::new(vec![
            processor.clone(),
            Arc::new(observer.tap("usage-rtvi")),
        ])
        .start(config)
        .await;
        let worker = match worker {
            Ok(worker) => worker,
            Err(error) => {
                let (_, joined) = peer.join().await;
                panic!("pipeline startup: {error}; peer: {joined:?}");
            }
        };
        let sender = processor.sender().unwrap();
        Self {
            worker,
            sender,
            peer,
            frames: vec![],
        }
    }
    async fn witness(&mut self, wanted: impl Fn(&DirectedFrame) -> bool) -> Check<()> {
        timeout(WAIT, async {
            loop {
                let frame = self
                    .worker
                    .recv()
                    .await
                    .ok_or("outlet closed before witness")?;
                let matched = wanted(&frame);
                require(self.frames.len() < MAX_RECORDS, "outlet observation bound")?;
                self.frames.push(frame);
                if matched {
                    return Ok(());
                }
            }
        })
        .await
        .map_err(error)?
    }
    async fn begin(&mut self, id: &str) -> Check<()> {
        self.sender
            .send(Action::CreateResponse(Default::default()))
            .await
            .map_err(error)?;
        self.peer.command("response.create").await?;
        self.peer.event(created(id)).await?;
        self.witness(|frame| {
            matches!(frame.frame.data, FrameData::LlmResponseStart)
                && frame.frame.metadata.get("response_id") == Some(&json!(id))
        })
        .await
    }
    async fn complete(
        &mut self,
        id: &str,
        status: &str,
        top: Option<Value>,
        nested: Option<Value>,
    ) -> Check<()> {
        self.begin(id).await?;
        self.peer.event(done(id, status, top, nested)).await?;
        self.witness(|frame| {
            matches!(frame.frame.data, FrameData::LlmResponseEnd)
                && frame.frame.metadata.get("response_id") == Some(&json!(id))
        })
        .await
    }
    async fn finish(mut self, cancel: bool) -> Outcome {
        let mut owners = if self.worker.has_finished() {
            Ok(())
        } else {
            timeout(WAIT, async {
                if cancel {
                    self.worker.cancel("usage fixture terminal").await
                } else {
                    self.worker.end().await
                }
            })
            .await
            .map_err(error)
            .and_then(|result| result.map_err(error))
        };
        if owners.is_err() {
            let _ = self.worker.cancel("usage terminal admission failed").await;
        }
        let report = match timeout(WAIT, self.worker.join()).await {
            Ok(report) => report,
            Err(_) => {
                let _ = self.worker.cancel("usage join deadline").await;
                owners = Err("worker deadline; cancellation owner actually joined".into());
                self.worker.join().await
            }
        };
        while let Ok(frame) = self.worker.try_recv() {
            if self.frames.len() == MAX_RECORDS {
                owners = Err("outlet observation bound".into());
                break;
            }
            self.frames.push(frame);
        }
        let (wire, joined) = self.peer.join().await;
        owners = owners.and(joined);
        match timeout(WAIT, self.sender.send(Action::CommitAudio)).await {
            Ok(Err(RealtimeError::Closed)) => {}
            other => owners = Err(format!("joined owner accepted a later command: {other:?}")),
        }
        Outcome {
            frames: self.frames,
            wire,
            report,
            owners,
        }
    }
}
impl Outcome {
    fn diagnostics(&self) -> String {
        // Records and strings are capped, including failure-path diagnostics.
        let outlet: String = format!("{:?}", self.frames).chars().take(8192).collect();
        let wire: String = format!("{:?}", self.wire).chars().take(4096).collect();
        format!("report={:?}; outlet={outlet}; wire={wire}", self.report)
    }
    fn joined(&self, phase: Check<()>) {
        assert!(
            self.owners.is_ok(),
            "owners: {:?}; {}",
            self.owners,
            self.diagnostics()
        );
        assert!(self.report.is_ok(), "{}", self.diagnostics());
        assert!(phase.is_ok(), "phase: {phase:?}; {}", self.diagnostics());
        assert_eq!(self.report.as_ref().unwrap().dropped_frames, 0);
    }
    fn metrics(&self) -> Vec<&Frame> {
        self.frames
            .iter()
            .filter_map(|frame| {
                if matches!(frame.frame.data, FrameData::Metrics(_)) {
                    assert_eq!(frame.direction, Direction::Downstream);
                    Some(&frame.frame)
                } else {
                    None
                }
            })
            .collect()
    }
    fn rtvi_metrics(&self) -> Vec<Value> {
        self.frames
            .iter()
            .filter_map(|frame| match &frame.frame.data {
                FrameData::OutputTransportMessageUrgent(value)
                | FrameData::OutputTransportMessage(value)
                    if value["type"] == "metrics" =>
                {
                    assert_eq!(frame.direction, Direction::Downstream);
                    Some(value.clone())
                }
                _ => None,
            })
            .collect()
    }
    fn usage(&self, expected: &[(&str, Counts)]) {
        let metrics = self.metrics();
        assert_eq!(metrics.len(), expected.len(), "{}", self.diagnostics());
        for (frame, (id, counts)) in metrics.into_iter().zip(expected) {
            assert_metric(frame, id, *counts);
        }
        // Compare the entire transport envelope, not only its numeric fields.
        assert_eq!(
            self.rtvi_metrics(),
            expected
                .iter()
                .map(|(_, counts)| counts.rtvi())
                .collect::<Vec<_>>()
        );
        assert!(self.frames.iter().all(|frame| {
            !frame.frame.metadata.contains_key("llm_usage")
                && !frame.frame.metadata.contains_key("input_token_details")
                && !frame.frame.metadata.contains_key("output_token_details")
        }));
    }
    fn responses(&self, ids: &[&str]) {
        for id in ids {
            for start in [true, false] {
                assert_eq!(
                    self.frames
                        .iter()
                        .filter(|frame| {
                            frame.direction == Direction::Downstream
                                && frame.frame.metadata.get("response_id") == Some(&json!(id))
                                && if start {
                                    matches!(frame.frame.data, FrameData::LlmResponseStart)
                                } else {
                                    matches!(frame.frame.data, FrameData::LlmResponseEnd)
                                }
                        })
                        .count(),
                    1,
                    "id={id}; start={start}; {}",
                    self.diagnostics()
                );
            }
        }
    }
}
fn assert_metric(frame: &Frame, id: &str, counts: Counts) {
    assert_eq!(frame.data, counts.metric(), "response {id}");
    assert_eq!(frame.metadata.get("response_id"), Some(&json!(id)));
    assert_eq!(
        frame.metadata.get("realtime_event_id"),
        Some(&json!(format!("{id}-done")))
    );
    assert!(
        frame.metadata.keys().all(|key| matches!(
            key.as_str(),
            "realtime_event_id"
                | "realtime_generation"
                | "session_id"
                | "conversation_id"
                | "context_id"
                | "response_id"
        )),
        "unexpected metric metadata: {:?}",
        frame.metadata
    );
}

#[tokio::test]
async fn enabled_usage_is_base_only_for_every_rtvi_detail_switch() {
    for legacy in [false, true] {
        for audio in [false, true] {
            // Latency metrics are independent from usage capture.
            let mut rig = Rig::new(Some(true), false, legacy, audio).await;
            let phase = rig
                .complete("switches", "completed", Some(rich_usage()), None)
                .await;
            let result = rig.finish(phase.is_err()).await;
            result.joined(phase);
            assert!(result.report.as_ref().unwrap().errors.is_empty());
            result.usage(&[("switches", BASE)]);
            result.responses(&["switches"]);
        }
    }
}

#[tokio::test]
async fn default_and_explicit_disabled_usage_preserve_response_lifecycle() {
    for capture in [None, Some(false)] {
        let mut rig = Rig::new(capture, true, true, true).await;
        let phase = async {
            rig.complete("disabled-top", "completed", Some(rich_usage()), None)
                .await?;
            rig.complete("disabled-nested", "completed", None, Some(rich_usage()))
                .await
        }
        .await;
        let result = rig.finish(phase.is_err()).await;
        result.joined(phase);
        assert!(result.report.as_ref().unwrap().errors.is_empty());
        result.usage(&[]);
        result.responses(&["disabled-top", "disabled-nested"]);
    }
}

#[tokio::test]
async fn repeated_positive_zero_absent_and_empty_usage_never_spill_between_responses() {
    let cases = [
        ("rich", Some(rich_usage()), None, Some(BASE)),
        // Select the top-level object first; zero must not fall back to nested usage.
        (
            "zero-shadows",
            Some(Counts(77, 81, 0).usage()),
            Some(BASE.usage()),
            None,
        ),
        ("absent", None, None, None),
        ("empty-shadows", Some(json!({})), Some(BASE.usage()), None),
        (
            "nested",
            None,
            Some(Counts(0, u64::MAX, 1).usage()),
            Some(Counts(0, u64::MAX, 1)),
        ),
        (
            "null-fallback",
            Some(Value::Null),
            Some(Counts(u64::MAX, 0, 2).usage()),
            Some(Counts(u64::MAX, 0, 2)),
        ),
        ("final-absent", None, None, None),
    ];
    let mut rig = Rig::new(Some(true), true, true, true).await;
    let phase = async {
        for (id, top, nested, _) in &cases {
            rig.complete(id, "completed", top.clone(), nested.clone())
                .await?;
        }
        Ok(())
    }
    .await;
    let result = rig.finish(phase.is_err()).await;
    result.joined(phase);
    assert!(result.report.as_ref().unwrap().errors.is_empty());
    result.usage(
        &cases
            .iter()
            .filter_map(|(id, _, _, counts)| counts.map(|counts| (*id, counts)))
            .collect::<Vec<_>>(),
    );
    result.responses(&cases.iter().map(|(id, _, _, _)| *id).collect::<Vec<_>>());
}

#[tokio::test]
async fn failed_and_cancelled_responses_keep_errors_and_ends_across_usage_gates() {
    for capture in [None, Some(false), Some(true)] {
        let cases = [
            ("failed-positive", "failed", BASE),
            ("failed-zero", "failed", Counts(7, 5, 0)),
            ("cancelled-positive", "cancelled", BASE),
            ("cancelled-zero", "cancelled", Counts(7, 5, 0)),
        ];
        let mut rig = Rig::new(capture, true, true, true).await;
        let phase = async {
            for (id, status, counts) in &cases {
                rig.complete(id, status, Some(counts.usage()), Some(BASE.usage()))
                    .await?;
            }
            Ok(())
        }
        .await;
        let result = rig.finish(phase.is_err()).await;
        result.joined(phase);
        let expected = if capture == Some(true) {
            vec![("failed-positive", BASE), ("cancelled-positive", BASE)]
        } else {
            vec![]
        };
        result.usage(&expected);
        result.responses(&cases.iter().map(|(id, _, _)| *id).collect::<Vec<_>>());
        let errors = &result.report.as_ref().unwrap().errors;
        assert_eq!(errors.len(), 2, "{}", result.diagnostics());
        assert!(errors.iter().all(|error| {
            error.processor.as_deref() == Some("GrokRealtimeService")
                && error.error == "realtime response failed"
                && !error.fatal
        }));
        assert_eq!(result.frames.iter().filter(|frame| {
            frame.direction == Direction::Upstream
                && matches!(&frame.frame.data, FrameData::Error(error) if error.error == "realtime response failed")
        }).count(), 2);
    }
}

async fn next_matching(
    session: &mut RealtimeSession,
    wanted: impl Fn(&RealtimeEvent) -> bool,
) -> Check<RealtimeEvent> {
    timeout(WAIT, async {
        for _ in 0..MAX_RECORDS {
            let event = session
                .next_event()
                .await
                .ok_or("raw event stream closed")?
                .map_err(error)?;
            if wanted(&event) {
                return Ok(event);
            }
        }
        Err("raw event observation bound".into())
    })
    .await
    .map_err(error)?
}

#[tokio::test]
async fn standalone_websocket_events_keep_the_public_mapper_unconditional_and_zero_preserving() {
    let cases = [
        ("raw-positive", Some(rich_usage()), None, Some(BASE)),
        (
            "raw-top-zero",
            Some(Counts(7, 5, 0).usage()),
            Some(BASE.usage()),
            Some(Counts(7, 5, 0)),
        ),
        (
            "raw-nested-zero",
            None,
            Some(Counts(0, 0, 0).usage()),
            Some(Counts(0, 0, 0)),
        ),
        (
            "raw-empty-shadows",
            Some(json!({})),
            Some(BASE.usage()),
            None,
        ),
        (
            "raw-null-fallback",
            Some(Value::Null),
            Some(BASE.usage()),
            Some(BASE),
        ),
        ("raw-absent", None, None, None),
    ];
    let (mut peer, config) = Peer::new().await;
    let connected = RealtimeClient::new(config).unwrap().connect().await;
    let mut session = match connected {
        Ok(session) => session,
        Err(error) => {
            let (_, joined) = peer.join().await;
            panic!("raw connect: {error}; peer: {joined:?}");
        }
    };
    let sender = session.sender();
    let mut events = vec![];
    let phase: Check<()> = async {
        next_matching(&mut session, |event| matches!(event.event, Event::SessionUpdated { .. })).await?;
        for (id, top, nested, _) in &cases {
            sender.send(Action::CreateResponse(Default::default())).await.map_err(error)?;
            peer.command("response.create").await?;
            peer.event(created(id)).await?;
            next_matching(&mut session, |event| matches!(&event.event, Event::ResponseCreated { response } if response.id == *id)).await?;
            peer.event(done(id, "completed", top.clone(), nested.clone())).await?;
            // Read the actual typed client event, not a hand-built mapper input.
            events.push(next_matching(&mut session, |event| matches!(&event.event, Event::ResponseDone { response, .. } if response.id == *id)).await?);
        }
        Ok(())
    }.await;
    let shutdown = session.shutdown().await;
    let (wire, peer_joined) = peer.join().await;
    // The session actor and peer have both joined before any contract assertion.
    let diagnostics: String = format!("events={events:?}; wire={wire:?}")
        .chars()
        .take(8192)
        .collect();
    assert!(shutdown.is_ok(), "{shutdown:?}; {diagnostics}");
    assert!(peer_joined.is_ok(), "{peer_joined:?}; {diagnostics}");
    assert!(phase.is_ok(), "{phase:?}; {diagnostics}");
    assert_eq!(events.len(), cases.len());
    assert_eq!(
        sender.send(Action::CommitAudio).await,
        Err(RealtimeError::Closed)
    );
    let mut mapper = RealtimeFrameMapper::default();
    for ((id, _, _, expected), event) in cases.iter().zip(events) {
        // There is deliberately no Start or usage opt-in on this public path.
        let frames = mapper.frames(&event).unwrap();
        let metrics = frames
            .iter()
            .filter(|frame| matches!(frame.data, FrameData::Metrics(_)))
            .collect::<Vec<_>>();
        if let Some(counts) = expected {
            assert_eq!(metrics.len(), 1, "{id}");
            assert_metric(metrics[0], id, *counts);
        } else {
            assert!(metrics.is_empty(), "{id}");
        }
        assert!(frames
            .iter()
            .all(|frame| !frame.metadata.contains_key("llm_usage")));
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.data, FrameData::LlmResponseEnd))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn exact_python_usage_source_vectors_match_real_started_pipeline_outcomes() {
    // This fixture executes selected original Python class/method/statement ASTs,
    // not the full handler, provider SDK, or Python pipeline. The native side
    // consumes identical JSON usage values through real WebSocket events.
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/python_grok_usage_source.json")).unwrap();
    assert_eq!(
        fixture["revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 24);
    // A native Pipeline always runs setup. Do not relabel the Python no-setup
    // case as a default Start; it remains source-oracle-only coverage.
    let excluded = cases
        .iter()
        .filter(|case| case["setup"].is_null())
        .map(|case| case["id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(excluded, vec!["not_setup"]);
    let mut consumed = 0;
    for capture in [None, Some(false), Some(true)] {
        for latency in [false, true] {
            let selected = cases
                .iter()
                .filter(|case| {
                    case["id"] != "not_setup"
                        && case["setup"]["enable_usage_metrics"].as_bool() == capture
                        && case["setup"]["enable_metrics"].as_bool().unwrap_or(false) == latency
                })
                .collect::<Vec<_>>();
            if selected.is_empty() {
                continue;
            }
            let mut expected = vec![];
            let mut ids = vec![];
            for case in &selected {
                let id = case["id"].as_str().unwrap();
                ids.push(id);
                for value in case["expected"].as_array().unwrap() {
                    expected.push((
                        id,
                        Counts(
                            value["prompt_tokens"].as_u64().unwrap(),
                            value["completion_tokens"].as_u64().unwrap(),
                            value["total_tokens"].as_u64().unwrap(),
                        ),
                    ));
                }
            }
            let mut rig = Rig::new(capture, latency, true, true).await;
            let phase = async {
                for case in &selected {
                    let id = case["id"].as_str().unwrap();
                    rig.complete(
                        id,
                        "completed",
                        case["event"].get("usage").cloned(),
                        case["event"]["response"].get("usage").cloned(),
                    )
                    .await
                    .map_err(|error| format!("source case {id}: {error}"))?;
                }
                Ok(())
            }
            .await;
            let result = rig.finish(phase.is_err()).await;
            result.joined(phase);
            assert!(result.report.as_ref().unwrap().errors.is_empty());
            result.usage(&expected);
            result.responses(&ids);
            consumed += selected.len();
        }
    }
    assert_eq!(consumed, 23);
}
