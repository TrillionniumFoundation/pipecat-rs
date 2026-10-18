//! Production WebSocket -> realtime processor -> RTVI tap usage projection.
//! Assertions inspect the real pipeline outlet after both owners have joined.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_realtime::*;
use pipecat_rtvi::RtviObserver;
use serde_json::{json, Value};
use std::{fmt::Display, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

const WAIT: Duration = Duration::from_secs(5);
const MAX_RECORDS: usize = 512;
const DETAILS: [&str; 4] = [
    "cache_read_input_tokens",
    "input_audio_tokens",
    "output_audio_tokens",
    "cache_read_input_audio_tokens",
];
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
fn usage() -> Value {
    // Deliberately inconsistent: provider totals must never be reconstructed.
    json!({"input_tokens":7,"output_tokens":5,"total_tokens":99})
}
fn base() -> Value {
    json!({"prompt_tokens":7,"completion_tokens":5,"total_tokens":99})
}
fn rich_usage() -> Value {
    let mut value = usage();
    value["input_token_details"] = json!({
        "cached_tokens":0,"audio_tokens":u64::MAX,
        "cached_tokens_details":{"audio_tokens":3}
    });
    value["output_token_details"] = json!({"audio_tokens":11});
    value
}
fn rich_metadata() -> Value {
    let mut value = base();
    for (key, count) in DETAILS.into_iter().zip([0, u64::MAX, 11, 3]) {
        value[key] = json!(count);
    }
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
fn done(id: &str, usage: Option<Value>) -> Value {
    json!({"event_id":format!("{id}-done"),"type":"response.done","response":response(id,"completed",usage)})
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
            ws.send(Message::Text(json!({"event_id":"usage-ready","type":"session.updated","session":{"id":"usage-session","type":"realtime"}}).to_string().into())).await.map_err(error)?;
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
    async fn new(capture: Option<bool>, legacy: bool, audio: bool) -> Self {
        let (peer, config) = Peer::new().await;
        let processor =
            Arc::new(OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        let observer = Arc::new(
            RtviObserver::new(Default::default(), None)
                .unwrap()
                .with_llm_usage_details(legacy)
                .with_llm_audio_usage_details(audio),
        );
        let mut config = PipelineConfig::default();
        config.start.audio_in_sample_rate = 24_000;
        config.start.audio_out_sample_rate = 24_000;
        config.start.enable_metrics = true;
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
    async fn complete(&mut self, id: &str, usage: Option<Value>) -> Check<()> {
        self.begin(id).await?;
        self.peer.event(done(id, usage)).await?;
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
    fn joined(&self, phase: Check<()>) {
        assert!(self.owners.is_ok(), "owners: {:?}", self.owners);
        assert!(self.report.is_ok(), "report: {:?}", self.report);
        assert!(phase.is_ok(), "phase: {phase:?}");
    }
    fn healthy(&self, phase: Check<()>) {
        self.joined(phase);
        assert!(
            self.report.as_ref().unwrap().errors.is_empty(),
            "{:?}",
            self.report
        );
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
    fn tokens(&self) -> Vec<Value> {
        self.frames
            .iter()
            .filter_map(|frame| match &frame.frame.data {
                FrameData::OutputTransportMessageUrgent(value)
                | FrameData::OutputTransportMessage(value)
                    if value["type"] == "metrics" =>
                {
                    assert_eq!(frame.direction, Direction::Downstream);
                    Some(value["data"]["tokens"].as_array().unwrap().clone())
                }
                _ => None,
            })
            .flatten()
            .collect()
    }
}
fn assert_metric(frame: &Frame, id: &str, details: Option<&Value>) {
    assert_eq!(
        frame.data,
        FrameData::Metrics(vec![Metric::LlmUsage {
            processor: "OpenAiRealtimeService".into(),
            prompt_tokens: 7,
            completion_tokens: 5,
            total_tokens: 99,
        }])
    );
    assert_eq!(frame.metadata.get("llm_usage"), details);
    assert_eq!(frame.metadata.get("response_id"), Some(&json!(id)));
    assert_eq!(
        frame.metadata.get("realtime_event_id"),
        Some(&json!(format!("{id}-done")))
    );
}

#[tokio::test]
async fn real_processor_and_rtvi_tap_cover_all_four_independent_projection_switches() {
    for legacy in [false, true] {
        for audio in [false, true] {
            let mut rig = Rig::new(Some(true), legacy, audio).await;
            let phase = rig.complete("switches", Some(rich_usage())).await;
            let result = rig.finish(phase.is_err()).await;
            result.healthy(phase);
            let metrics = result.metrics();
            assert_eq!(metrics.len(), 1);
            assert_metric(metrics[0], "switches", Some(&rich_metadata()));
            let mut expected = base();
            for key in DETAILS {
                if (key == DETAILS[0] && legacy) || (key != DETAILS[0] && audio) {
                    expected[key] = rich_metadata()[key].clone();
                }
            }
            assert_eq!(
                result.tokens(),
                vec![expected],
                "legacy={legacy} audio={audio}"
            );
        }
    }
}

#[tokio::test]
async fn missing_null_empty_zero_max_and_unknown_details_do_not_carry_between_packets() {
    let mut cases: Vec<(Value, Option<Value>)> =
        vec![(rich_usage(), Some(rich_metadata())), (usage(), None)];
    for input in [None, Some(Value::Null), Some(json!({}))] {
        for output in [None, Some(Value::Null), Some(json!({}))] {
            let mut raw = usage();
            let mut expected = base();
            if let Some(value) = &input {
                raw["input_token_details"] = value.clone();
                if value.is_object() {
                    expected[DETAILS[0]] = json!(0);
                    expected[DETAILS[1]] = json!(0);
                }
            }
            if let Some(value) = &output {
                raw["output_token_details"] = value.clone();
                if value.is_object() {
                    expected[DETAILS[2]] = json!(0);
                }
            }
            cases.push((raw, (expected != base()).then_some(expected)));
        }
    }
    for nested in [
        Value::Null,
        json!({}),
        json!({"audio_tokens":null}),
        json!({"audio_tokens":0}),
        json!({"audio_tokens":u64::MAX}),
    ] {
        let mut raw = usage();
        raw["input_token_details"] = json!({"cached_tokens_details":nested});
        let mut expected = base();
        expected[DETAILS[0]] = json!(0);
        expected[DETAILS[1]] = json!(0);
        if let Some(object) = nested.as_object() {
            match object.get("audio_tokens") {
                None => expected[DETAILS[3]] = json!(0),
                Some(value) if !value.is_null() => expected[DETAILS[3]] = value.clone(),
                Some(_) => {}
            }
        }
        cases.push((raw, Some(expected)));
    }
    for bad_unknown in [
        json!(false),
        json!(-1),
        json!(1.5),
        json!("not a number"),
        json!([]),
        json!({}),
    ] {
        let mut raw = rich_usage();
        raw["input_token_details"]["cached_tokens_details"]["text_tokens"] = bad_unknown;
        raw["input_token_details"]["cached_tokens_details"]["extra"] =
            json!("private-blob".repeat(4096));
        raw["input_token_details"]["reasoning_tokens"] = json!(500);
        raw["output_token_details"]["cached_tokens_details"] = json!(false);
        raw["provider_private"] = json!("must not leak");
        cases.push((raw, Some(rich_metadata())));
    }
    let mut all_max = usage();
    all_max["input_token_details"] = json!({"cached_tokens":u64::MAX,"audio_tokens":u64::MAX,"cached_tokens_details":{"audio_tokens":u64::MAX}});
    all_max["output_token_details"] = json!({"audio_tokens":u64::MAX});
    let mut expected = base();
    for key in DETAILS {
        expected[key] = json!(u64::MAX);
    }
    cases.push((all_max, Some(expected)));
    cases.push((usage(), None));
    let mut rig = Rig::new(Some(true), true, true).await;
    let phase = async {
        for (index, (raw, _)) in cases.iter().enumerate() {
            rig.complete(&format!("details-{index}"), Some(raw.clone()))
                .await?;
        }
        // A response without usage must not reuse the preceding packet either.
        rig.complete("no-usage", None).await
    }
    .await;
    let result = rig.finish(phase.is_err()).await;
    result.healthy(phase);
    let metrics = result.metrics();
    assert_eq!(metrics.len(), cases.len());
    for (index, ((_, expected), frame)) in cases.iter().zip(metrics).enumerate() {
        assert_metric(frame, &format!("details-{index}"), expected.as_ref());
    }
    assert_eq!(
        result.tokens(),
        cases
            .iter()
            .map(|(_, expected)| expected.clone().unwrap_or_else(base))
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn default_and_explicit_disabled_usage_capture_emit_neither_frames_nor_rtvi_tokens() {
    for capture in [None, Some(false)] {
        let mut rig = Rig::new(capture, true, true).await;
        let phase = rig.complete("disabled", Some(rich_usage())).await;
        let result = rig.finish(phase.is_err()).await;
        result.healthy(phase);
        assert!(result.metrics().is_empty());
        assert!(result.tokens().is_empty());
        assert!(result
            .frames
            .iter()
            .all(|frame| !frame.frame.metadata.contains_key("llm_usage")));
        assert!(result
            .frames
            .iter()
            .any(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd)));
    }
}

#[tokio::test]
async fn malformed_nested_details_reject_before_done_releases_a_deferred_response() {
    let overflow: Value = serde_json::from_str("18446744073709551616").unwrap();
    let malformed = [
        json!([]),
        json!(true),
        json!(-1),
        json!(1.5),
        json!("1"),
        overflow,
        json!({}),
    ];
    let mut cases = Vec::new();
    for value in &malformed {
        let mut raw = rich_usage();
        raw["input_token_details"]["cached_tokens_details"]["audio_tokens"] = value.clone();
        cases.push((raw, "invalid cached token details"));
        if !value.is_object() {
            let mut raw = rich_usage();
            raw["input_token_details"]["cached_tokens_details"] = value.clone();
            cases.push((raw, "invalid cached token details"));
        }
    }
    // Existing top-level typed u64 fields retain their nonnullable contract.
    for (family, field) in [
        ("input_token_details", "cached_tokens"),
        ("input_token_details", "audio_tokens"),
        ("output_token_details", "audio_tokens"),
    ] {
        let mut raw = rich_usage();
        raw[family][field] = Value::Null;
        cases.push((raw, "invalid typed event"));
    }
    for (index, (raw, message)) in cases.into_iter().enumerate() {
        let mut rig = Rig::new(Some(true), true, true).await;
        let phase = async {
            rig.begin("malformed")
                .await
                .map_err(|error| format!("begin active response: {error}"))?;
            // A real accepted deferred create would be written by a wrongly
            // applied response.done before parsing the newly supported details.
            rig.sender
                .send(Action::CreateResponse(Default::default()))
                .await
                .map_err(|error| format!("admit deferred create: {error}"))?;
            rig.peer
                .event(done("malformed", Some(raw)))
                .await
                .map_err(|error| format!("send malformed done: {error}"))?;
            rig.witness(|frame| {
                matches!(&frame.frame.data, FrameData::Error(error)
                if error.error.contains(message) && frame.direction == Direction::Upstream)
            })
            .await
            .map_err(|error| format!("observe upstream malformed error ({message}): {error}"))
        }
        .await;
        let result = rig.finish(true).await;
        // Retain bounded diagnostics only after both owners have been joined.
        let frame_trace: String = format!("{:?}", result.frames).chars().take(8192).collect();
        let wire_trace: String = format!("{:?}", result.wire).chars().take(8192).collect();
        let diagnostics = format!(
            "case {index}; report={:?}; outlet={frame_trace}; wire={wire_trace}",
            result.report
        );
        result.joined(phase.map_err(|error| format!("{error}; {diagnostics}")));
        assert!(result.metrics().is_empty(), "case {index}");
        assert!(result.tokens().is_empty(), "case {index}");
        assert!(
            !result
                .frames
                .iter()
                .any(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd)),
            "case {index}"
        );
        let errors = &result.report.as_ref().unwrap().errors;
        assert_eq!(errors.len(), 1, "case {index}: {errors:?}");
        assert_eq!(
            errors[0].processor.as_deref(),
            Some("OpenAiRealtimeService")
        );
        assert!(
            errors[0].error.contains(message),
            "case {index}: {errors:?}"
        );
        assert_eq!(
            result
                .wire
                .iter()
                .filter(|value| value["type"] == "response.create")
                .count(),
            1,
            "malformed done released deferred create in case {index}: {:?}",
            result.wire
        );
    }
}

#[tokio::test]
async fn interruption_discards_stale_usage_without_contaminating_the_current_response() {
    let mut rig = Rig::new(Some(true), true, true).await;
    let phase = async {
        rig.begin("stale").await?;
        rig.worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .map_err(error)?;
        rig.witness(|frame| matches!(frame.frame.data, FrameData::Interruption))
            .await?;
        rig.peer.command("response.cancel").await?;
        rig.peer.event(done("stale", Some(rich_usage()))).await?;
        rig.complete("current", Some(usage())).await
    }
    .await;
    let result = rig.finish(phase.is_err()).await;
    result.healthy(phase);
    let metrics = result.metrics();
    assert_eq!(metrics.len(), 1);
    assert_metric(metrics[0], "current", None);
    assert_eq!(
        metrics[0].metadata.get("realtime_generation"),
        Some(&json!(1))
    );
    assert_eq!(result.tokens(), vec![base()]);
    assert!(!result.frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::LlmResponseEnd
    ) && frame.frame.metadata.get("response_id")
        == Some(&json!("stale"))));
}

#[tokio::test]
async fn failed_and_cancelled_responses_retain_usage_and_original_terminal_semantics() {
    for status in ["failed", "cancelled"] {
        let mut rig = Rig::new(Some(true), true, true).await;
        let phase = async {
            rig.begin(status).await?;
            let mut event = done(status, Some(rich_usage()));
            event["response"]["status"] = json!(status);
            if status == "failed" {
                event["response"]["status_details"] =
                    json!({"error":{"message":"synthetic response failure"}});
            }
            rig.peer.event(event).await?;
            rig.witness(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
                .await
        }
        .await;
        let result = rig.finish(phase.is_err()).await;
        result.joined(phase);
        let metrics = result.metrics();
        assert_eq!(metrics.len(), 1);
        assert_metric(metrics[0], status, Some(&rich_metadata()));
        assert_eq!(result.tokens(), vec![rich_metadata()]);
        let errors = &result.report.as_ref().unwrap().errors;
        if status == "failed" {
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].error, "synthetic response failure");
            assert!(!errors[0].fatal);
            assert_eq!(
                errors[0].processor.as_deref(),
                Some("OpenAiRealtimeService")
            );
        } else {
            assert!(errors.is_empty());
        }
    }
}

#[tokio::test]
async fn public_next_event_and_raw_mapper_keep_unconditional_unenriched_usage_metrics() {
    // Existing downstream users can still initialize all four public fields.
    let public_details = TokenDetails {
        text_tokens: 1,
        audio_tokens: 2,
        image_tokens: 3,
        cached_tokens: 4,
    };
    assert_eq!(
        serde_json::to_value(public_details).unwrap(),
        json!({"text_tokens":1,"audio_tokens":2,"image_tokens":3,"cached_tokens":4})
    );
    let (mut peer, config) = Peer::new().await;
    let connected = RealtimeClient::new(config).unwrap().connect().await;
    let mut session = match connected {
        Ok(session) => session,
        Err(error) => {
            let (_, joined) = peer.join().await;
            panic!("raw session startup: {error}; peer: {joined:?}");
        }
    };
    let mut frames = Vec::new();
    let phase: Check<()> = async {
        session
            .sender()
            .send(Action::CreateResponse(Default::default()))
            .await
            .map_err(error)?;
        peer.command("response.create").await?;
        peer.event(created("raw")).await?;
        peer.event(done("raw", Some(rich_usage()))).await?;
        let mut mapper = RealtimeFrameMapper::default();
        loop {
            let event = timeout(WAIT, session.next_event())
                .await
                .map_err(error)?
                .ok_or("raw session ended before done")?
                .map_err(error)?;
            let is_done = matches!(event.event, Event::ResponseDone { .. });
            frames.extend(mapper.frames(&event).map_err(error)?);
            require(frames.len() < MAX_RECORDS, "raw mapper observation bound")?;
            if is_done {
                return Ok(());
            }
        }
    }
    .await;
    let shutdown = session.shutdown().await;
    let (_, joined) = peer.join().await;
    assert!(joined.is_ok(), "{joined:?}");
    assert!(shutdown.is_ok(), "{shutdown:?}");
    assert!(phase.is_ok(), "{phase:?}");
    let metrics: Vec<_> = frames
        .iter()
        .filter(|frame| matches!(frame.data, FrameData::Metrics(_)))
        .collect();
    assert_eq!(metrics.len(), 1);
    assert_metric(metrics[0], "raw", None);
}

#[tokio::test]
async fn real_processor_matches_retained_fixed_python_usage_handler_vectors() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/python_realtime_usage_source.json")).unwrap();
    assert_eq!(
        fixture["revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    assert_eq!(
        fixture["source_blobs"]["python-realtime-llm.py"],
        "5e859077a8ee44057adb6d1902709e44e04885cd"
    );
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 8);
    // This retained source requires both detail objects. The separate native
    // compatibility test owns missing/null whole-detail semantics.
    let mut rig = Rig::new(Some(true), true, true).await;
    let phase = async {
        for case in cases {
            let id = case["id"].as_str().ok_or("source vector lacks ID")?;
            rig.complete(id, Some(case["input"].clone())).await?;
        }
        Ok(())
    }
    .await;
    let result = rig.finish(phase.is_err()).await;
    result.healthy(phase);
    let metrics = result.metrics();
    assert_eq!(metrics.len(), cases.len());
    for (case, frame) in cases.iter().zip(metrics) {
        let expected = &case["expected"];
        assert_eq!(
            frame.metadata.get("llm_usage"),
            Some(expected),
            "{}",
            case["id"]
        );
        assert_eq!(
            frame.data,
            FrameData::Metrics(vec![Metric::LlmUsage {
                processor: "OpenAiRealtimeService".into(),
                prompt_tokens: expected["prompt_tokens"].as_u64().unwrap(),
                completion_tokens: expected["completion_tokens"].as_u64().unwrap(),
                total_tokens: expected["total_tokens"].as_u64().unwrap(),
            }]),
            "{}",
            case["id"]
        );
        assert_eq!(frame.metadata.get("response_id"), Some(&case["id"]));
    }
    assert_eq!(
        result.tokens(),
        cases
            .iter()
            .map(|case| case["expected"].clone())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn historical_public_positional_sequences_keep_typed_usage_without_nested_projection() {
    // This is native Serde compatibility, not a Python/provider wire promise.
    // Public field order is TokenDetails[text,audio,image,cached],
    // Usage[total,input,output,input_details,output_details], and
    // Response[id,status,conversation_id,output,usage,status_details].
    let input = json!({"text_tokens":13,"audio_tokens":17,"image_tokens":19,"cached_tokens":23,
        "cached_tokens_details":{"audio_tokens":997}});
    let output = json!({"text_tokens":29,"audio_tokens":31,"image_tokens":37,"cached_tokens":41});
    let mut object_usage = usage();
    object_usage["input_token_details"] = input.clone();
    object_usage["output_token_details"] = output.clone();
    let mut details = done("legacy-details", Some(object_usage.clone()));
    details["response"]["usage"]["input_token_details"] = json!([13, 17, 19, 23]);
    details["response"]["usage"]["output_token_details"] = json!([29, 31, 37, 41]);
    let positional_usage = done("legacy-usage", Some(json!([99, 7, 5, input, output])));
    let mut positional_response = done("legacy-response", None);
    positional_response["response"] = json!([
        "legacy-response",
        "completed",
        "usage-conversation",
        [],
        object_usage,
        null
    ]);
    let mut all_positional = done("legacy-all", None);
    all_positional["response"] = json!([
        "legacy-all",
        "completed",
        "usage-conversation",
        [],
        [99, 7, 5, [13, 17, 19, 23], [29, 31, 37, 41]],
        null
    ]);
    let mut empty_details = done("legacy-empty-details", Some(usage()));
    empty_details["response"]["usage"]["input_token_details"] = json!([]);
    empty_details["response"]["usage"]["output_token_details"] = json!([]);
    let expected = Usage {
        total_tokens: 99,
        input_tokens: 7,
        output_tokens: 5,
        input_token_details: Some(TokenDetails {
            text_tokens: 13,
            audio_tokens: 17,
            image_tokens: 19,
            cached_tokens: 23,
        }),
        output_token_details: Some(TokenDetails {
            text_tokens: 29,
            audio_tokens: 31,
            image_tokens: 37,
            cached_tokens: 41,
        }),
    };
    let empty_expected = Usage {
        input_token_details: Some(TokenDetails::default()),
        output_token_details: Some(TokenDetails::default()),
        ..expected.clone()
    };
    let cases = [
        ("legacy-details", details, expected.clone()),
        ("legacy-usage", positional_usage, expected.clone()),
        ("legacy-response", positional_response, expected.clone()),
        ("legacy-all", all_positional, expected),
        ("legacy-empty-details", empty_details, empty_expected),
    ];
    // Verify the legacy public typed admission directly before opening owners.
    for (id, event, expected) in &cases {
        let typed: Response = serde_json::from_value(event["response"].clone()).unwrap();
        assert_eq!(typed.id, *id);
        assert_eq!(typed.usage.as_ref(), Some(expected));
    }

    let (mut peer, config) = Peer::new().await;
    let connected = RealtimeClient::new(config).unwrap().connect().await;
    let mut session = match connected {
        Ok(session) => session,
        Err(error) => {
            let (_, joined) = peer.join().await;
            panic!("positional raw session startup: {error}; peer: {joined:?}");
        }
    };
    let mut raw_frames = Vec::new();
    let mut raw_usage = Vec::new();
    let phase: Check<()> = async {
        let mut mapper = RealtimeFrameMapper::default();
        for (id, event, _) in &cases {
            session
                .sender()
                .send(Action::CreateResponse(Default::default()))
                .await
                .map_err(error)?;
            peer.command("response.create").await?;
            peer.event(created(id)).await?;
            peer.event(event.clone()).await?;
            loop {
                let event = timeout(WAIT, session.next_event())
                    .await
                    .map_err(error)?
                    .ok_or("positional raw session ended before done")?
                    .map_err(error)?;
                let is_done = if let Event::ResponseDone { response } = &event.event {
                    raw_usage.push((response.id.clone(), response.usage.clone()));
                    true
                } else {
                    false
                };
                raw_frames.extend(mapper.frames(&event).map_err(error)?);
                require(
                    raw_frames.len() < MAX_RECORDS,
                    "positional raw observation bound",
                )?;
                if is_done {
                    break;
                }
            }
        }
        Ok(())
    }
    .await;
    let shutdown = session.shutdown().await;
    let (_, joined) = peer.join().await;
    assert!(joined.is_ok(), "{joined:?}");
    assert!(shutdown.is_ok(), "{shutdown:?}");
    assert!(phase.is_ok(), "{phase:?}");
    assert_eq!(
        raw_usage,
        cases
            .iter()
            .map(|(id, _, expected)| ((*id).to_owned(), Some(expected.clone())))
            .collect::<Vec<_>>()
    );
    assert_eq!(raw_frames.len(), cases.len() * 3);
    for ((id, _, _), frames) in cases.iter().zip(raw_frames.as_chunks::<3>().0) {
        assert!(matches!(frames[0].data, FrameData::LlmResponseStart));
        assert_metric(&frames[1], id, None);
        assert!(matches!(frames[2].data, FrameData::LlmResponseEnd));
    }

    let mut rig = Rig::new(Some(true), true, true).await;
    let phase = async {
        for (id, event, _) in &cases {
            rig.begin(id).await?;
            rig.peer.event(event.clone()).await?;
            rig.witness(|frame| {
                matches!(frame.frame.data, FrameData::LlmResponseEnd)
                    && frame.frame.metadata.get("response_id") == Some(&json!(id))
            })
            .await?;
        }
        Ok(())
    }
    .await;
    let result = rig.finish(phase.is_err()).await;
    result.healthy(phase);
    let metrics = result.metrics();
    assert_eq!(metrics.len(), cases.len());
    let mut expected_tokens = Vec::new();
    for ((id, _, expected), frame) in cases.iter().zip(metrics) {
        let mut metadata = base();
        metadata[DETAILS[0]] = json!(expected.input_token_details.as_ref().unwrap().cached_tokens);
        metadata[DETAILS[1]] = json!(expected.input_token_details.as_ref().unwrap().audio_tokens);
        metadata[DETAILS[2]] = json!(expected.output_token_details.as_ref().unwrap().audio_tokens);
        assert_metric(frame, id, Some(&metadata));
        assert!(metadata.get(DETAILS[3]).is_none());
        expected_tokens.push(metadata);
    }
    assert_eq!(result.tokens(), expected_tokens);
}
