//! Chat Completions audio transcripts are ordinary generated text, never playback.
//! Real paired pipelines, bounded loopback HTTP/SSE, and committed-history receipts.
#[path = "../../pipecat-services/tests/review_nvidia_reasoning.rs"]
mod existing_nvidia;
#[path = "../../pipecat-services/tests/review_processor.rs"]
mod existing_processor;
#[path = "../../pipecat-services/tests/protocol_contracts.rs"]
mod existing_protocol;
#[path = "assistant_stopped_runtime.rs"]
mod existing_stopped;

use pipecat_core::*;
use pipecat_services::{
    protocol::ProtocolDecoder, sse::SseEvent, ClientConfig, CompletionRequest, Content,
    ContentPart, Context, LlmClient, LlmProcessor, Message, Provider, Role, ServiceError,
    StreamEvent,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{
    fmt::Display,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Notify},
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(7);
const MODEL: &str = "synthetic-audio-chat";
const PREFIX: &str = "delivered prefix";
const HIDDEN: &str = "UNDELIVERED-AUDIO-SUFFIX";
const MAX_RECORDS: usize = 512;
type Check<T> = Result<T, String>;
fn error(value: impl Display) -> String {
    value.to_string()
}
fn require(value: bool, message: &str) -> Check<()> {
    if value {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn delta(value: Value) -> Value {
    json!({"choices":[{"index":0,"delta":value}]})
}
fn transcript(value: &str) -> Value {
    delta(
        json!({"audio":{"transcript":value,"id":"ignored-audio-id","data":"ignored-audio-bytes","expires_at":123}}),
    )
}
fn finish_event() -> Value {
    json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})
}
fn body(events: &[Value], done: bool) -> Vec<u8> {
    let mut body = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect::<String>();
    if done {
        body.push_str("data: [DONE]\n\n");
    }
    body.into_bytes()
}
fn initial_context() -> Context {
    let mut message = Message::text(Role::User, "");
    // Synthetic wire payloads only: these tests do not decode input audio.
    message.content = Some(Content::Parts(vec![
        ContentPart::Text {
            text: "transcribe these fixtures".into(),
        },
        ContentPart::InputAudio {
            data: "AQID".into(),
            format: "wav".into(),
        },
        ContentPart::InputAudio {
            data: "BAUG".into(),
            format: "mp3".into(),
        },
    ]));
    Context::new(vec![message])
}
fn initial_messages() -> Value {
    json!([{"role":"user","content":[
        {"type":"text","text":"transcribe these fixtures"},
        {"type":"input_audio","input_audio":{"data":"AQID","format":"wav"}},
        {"type":"input_audio","input_audio":{"data":"BAUG","format":"mp3"}}
    ]}])
}
fn expected_request(messages: Value) -> Value {
    json!({"model":MODEL,"stream":true,"stream_options":{"include_usage":true},
        "modalities":["text","audio"],"audio":{"voice":"alloy","format":"pcm16"},
        "messages":messages})
}
fn history_with(texts: &[&str]) -> Context {
    let mut context = initial_context();
    context.messages.extend(
        texts
            .iter()
            .map(|text| Message::text(Role::Assistant, *text)),
    );
    context
}
fn wire_history(texts: &[&str]) -> Value {
    let mut messages = initial_messages().as_array().unwrap().clone();
    messages.extend(
        texts
            .iter()
            .map(|text| json!({"role":"assistant","content":text})),
    );
    Value::Array(messages)
}

struct Script {
    bytes: Vec<u8>,
    hold_until_client_eof: bool,
    fragment: bool,
}
impl Script {
    fn complete(events: Vec<Value>) -> Self {
        Self {
            bytes: body(&events, true),
            hold_until_client_eof: false,
            fragment: true,
        }
    }
    fn stalled() -> Self {
        let mut bytes = body(&[transcript(PREFIX)], false);
        // A real partial SSE/JSON record has entered the socket but cannot yet
        // produce a transcript. It must never be committed on cancellation.
        bytes.extend_from_slice(
            format!("data: {{\"choices\":[{{\"delta\":{{\"audio\":{{\"transcript\":\"{HIDDEN}")
                .as_bytes(),
        );
        Self {
            bytes,
            hold_until_client_eof: true,
            fragment: true,
        }
    }
}
#[derive(Clone, Debug)]
struct Exchange {
    headers: String,
    body: Value,
}
#[derive(Default)]
struct WireTrace {
    exchanges: Vec<Exchange>,
    client_eofs: usize,
    parked: usize,
    active: usize,
}
async fn read_request(socket: &mut TcpStream) -> Check<Exchange> {
    const LIMIT: usize = 64 * 1024;
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let end = loop {
        let count = socket.read(&mut chunk).await.map_err(error)?;
        require(count != 0, "HTTP closed before headers")?;
        bytes.extend_from_slice(&chunk[..count]);
        require(bytes.len() <= LIMIT, "HTTP request byte bound")?;
        if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).map_err(error)?;
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>())
        })
        .ok_or("HTTP request missing Content-Length")?
        .map_err(error)?;
    require(length <= LIMIT - end, "HTTP body byte bound")?;
    while bytes.len() < end + length {
        let count = socket.read(&mut chunk).await.map_err(error)?;
        require(count != 0, "HTTP closed before body")?;
        bytes.extend_from_slice(&chunk[..count]);
        require(bytes.len() <= LIMIT, "HTTP request byte bound")?;
    }
    Ok(Exchange {
        headers,
        body: serde_json::from_slice(&bytes[end..end + length]).map_err(error)?,
    })
}
async fn respond(
    socket: &mut TcpStream,
    script: &Script,
    trace: &Mutex<WireTrace>,
    changed: &Notify,
) -> Check<bool> {
    let headers = if script.hold_until_client_eof {
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n".to_owned()
    } else {
        format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",script.bytes.len())
    };
    socket.write_all(headers.as_bytes()).await.map_err(error)?;
    if script.hold_until_client_eof {
        socket
            .write_all(format!("{:x}\r\n", script.bytes.len()).as_bytes())
            .await
            .map_err(error)?;
    }
    // Deliberately split UTF-8, JSON and SSE records across real TCP writes.
    let width = if script.fragment {
        7
    } else {
        script.bytes.len().max(1)
    };
    for bytes in script.bytes.chunks(width) {
        socket.write_all(bytes).await.map_err(error)?;
        if script.fragment {
            tokio::task::yield_now().await;
        }
    }
    if script.hold_until_client_eof {
        socket.write_all(b"\r\n").await.map_err(error)?;
        trace.lock().unwrap().parked += 1;
        changed.notify_waiters();
        let mut byte = [0];
        match socket.read(&mut byte).await {
            Ok(0) => Ok(true),
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted
                ) =>
            {
                Ok(true)
            }
            Ok(_) => Err("unexpected pipelined bytes instead of cancellation EOF".into()),
            Err(e) => Err(error(e)),
        }
    } else {
        socket.shutdown().await.map_err(error)?;
        Ok(false)
    }
}
struct Peer {
    trace: Arc<Mutex<WireTrace>>,
    changed: Arc<Notify>,
    stop: Option<oneshot::Sender<()>>,
    task: JoinHandle<Check<()>>,
}
impl Peer {
    async fn new(scripts: Vec<Script>) -> (Self, ClientConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = ClientConfig::new(Provider::OpenAi, "synthetic-audio-transcript");
        config.endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        config.request_timeout = Duration::from_secs(5);
        config.idle_timeout = Duration::from_secs(4);
        let trace = Arc::new(Mutex::new(WireTrace::default()));
        let changed = Arc::new(Notify::new());
        let (stop, mut stopped) = oneshot::channel();
        let captured = trace.clone();
        let notify = changed.clone();
        let task = tokio::spawn(async move {
            let mut index = 0;
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stopped => return Ok(()),
                    accepted = listener.accept() => accepted.map_err(error)?,
                };
                captured.lock().unwrap().active += 1;
                let result = timeout(WAIT, async {
                    let request = read_request(&mut socket).await?;
                    {
                        let mut trace = captured.lock().unwrap();
                        require(trace.exchanges.len() < 16, "HTTP request count bound")?;
                        trace.exchanges.push(request);
                    }
                    notify.notify_waiters();
                    let script = scripts
                        .get(index)
                        .ok_or("unexpected additional HTTP request")?;
                    respond(&mut socket, script, &captured, &notify).await
                })
                .await
                .map_err(error)
                .and_then(|r| r);
                {
                    let mut trace = captured.lock().unwrap();
                    trace.active -= 1;
                    if matches!(result, Ok(true)) {
                        trace.client_eofs += 1;
                    }
                }
                notify.notify_waiters();
                result?;
                index += 1;
            }
        });
        (
            Self {
                trace,
                changed,
                stop: Some(stop),
                task,
            },
            config,
        )
    }
    async fn parked(&self) -> Check<()> {
        timeout(WAIT, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.trace.lock().unwrap().parked > 0 {
                    return Ok(());
                }
                notified.await;
            }
        })
        .await
        .map_err(error)?
    }
    async fn eof(&self) -> Check<()> {
        timeout(WAIT, async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.trace.lock().unwrap().client_eofs > 0 {
                    return Ok(());
                }
                notified.await;
            }
        })
        .await
        .map_err(error)?
    }
    async fn join(mut self) -> (Vec<Exchange>, usize, Check<()>) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        let result = match timeout(WAIT, &mut self.task).await {
            Ok(result) => result.map_err(error).and_then(|r| r),
            Err(_) => {
                self.task.abort();
                let _ = self.task.await;
                Err("HTTP peer deadline; aborted task actually joined".into())
            }
        };
        let trace = self.trace.lock().unwrap();
        (
            trace.exchanges.clone(),
            trace.client_eofs,
            result.and(require(
                trace.active == 0,
                "HTTP owner left an active connection",
            )),
        )
    }
}

#[derive(Clone)]
struct Receipt {
    stage: &'static str,
    frame: DirectedFrame,
}
#[derive(Default)]
struct Frames {
    values: Mutex<Vec<Receipt>>,
    changed: Notify,
}
struct Tap {
    stage: &'static str,
    frames: Arc<Frames>,
    text_receipts: bool,
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
        let prefix = if self.text_receipts && direction == Direction::Downstream {
            match &frame.data {
                FrameData::LlmText(text) => Some(text.text.clone()),
                _ => None,
            }
        } else {
            None
        };
        {
            let mut values = self.frames.values.lock().unwrap();
            if values.len() == MAX_RECORDS {
                return Err(ProcessorError::new("frame observation bound"));
            }
            values.push(Receipt {
                stage: self.stage,
                frame: DirectedFrame::new(frame.clone(), direction),
            });
        }
        self.frames.changed.notify_waiters();
        ctx.push(frame, direction).await?;
        if let Some(prefix) = prefix {
            // Same invocation, direction, fence/generation and Normal lane;
            // no priority promotion. Assistant consumes LlmText, then forwards
            // this marker. Only its receipt proves the prefix was processed.
            let mut marker = Frame::new(FrameData::Data);
            marker
                .metadata
                .insert("audio_transcript_prefix".into(), json!(prefix));
            ctx.push(marker, direction).await?;
        }
        Ok(())
    }
}
struct Rig {
    worker: PipelineWorker,
    peer: Peer,
    shared: SharedContext,
    stopped: AssistantStoppedReceiver,
    completions: Vec<AssistantTurnStopped>,
    checkpoints: Vec<Context>,
    frames: Arc<Frames>,
}
struct Outcome {
    exchanges: Vec<Exchange>,
    client_eofs: usize,
    frames: Vec<Receipt>,
    context: Check<Context>,
    completions: Vec<AssistantTurnStopped>,
    checkpoints: Vec<Context>,
    receiver_close: Option<AssistantStoppedClose>,
    errors: Vec<ErrorFrame>,
    report: Result<WorkerReport, PipelineError>,
    owners: Check<()>,
}
impl Rig {
    async fn new(scripts: Vec<Script>) -> Self {
        let (peer, config) = Peer::new(scripts).await;
        let context = initial_context();
        let pair =
            LLMContextAggregatorPair::new(context.clone(), Default::default(), Default::default());
        let shared = pair.context();
        let stopped = pair
            .assistant()
            .subscribe_stopped(Default::default())
            .unwrap();
        let mut request = CompletionRequest::new(MODEL, context);
        request
            .extra
            .insert("modalities".into(), json!(["text", "audio"]));
        request
            .extra
            .insert("audio".into(), json!({"voice":"alloy","format":"pcm16"}));
        let llm = Arc::new(LlmProcessor::new(LlmClient::new(config).unwrap(), request));
        let frames = Arc::new(Frames::default());
        let result = Pipeline::new(vec![
            pair.user(),
            llm,
            Arc::new(Tap {
                stage: "after-service",
                frames: frames.clone(),
                text_receipts: true,
            }),
            pair.assistant(),
            Arc::new(Tap {
                stage: "after-assistant",
                frames: frames.clone(),
                text_receipts: false,
            }),
        ])
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .await;
        match result {
            Ok(worker) => Self {
                worker,
                peer,
                shared,
                stopped,
                completions: vec![],
                checkpoints: vec![],
                frames,
            },
            Err(e) => {
                let _ = peer.join().await;
                panic!("pipeline start failed after peer joined: {e}");
            }
        }
    }
    async fn send(&self, frame: Frame) -> Check<()> {
        timeout(
            WAIT,
            self.worker.sender().send(frame, Direction::Downstream),
        )
        .await
        .map_err(error)?
        .map_err(error)
    }
    async fn run(&self) -> Check<()> {
        self.send(Frame::new(FrameData::LlmRun)).await
    }
    async fn stopped(&mut self) -> Check<()> {
        let record = timeout(WAIT, self.stopped.recv())
            .await
            .map_err(error)?
            .ok_or("stopped receiver closed early")?;
        self.completions.push(record.completion().as_ref().clone());
        // Publication follows the real context write; this is a committed
        // history receipt, unlike the service's incoming ResponseEnd frame.
        self.checkpoints
            .push(self.shared.snapshot().map_err(error)?);
        Ok(())
    }
    async fn observed(&self, wanted: impl Fn(&Receipt) -> bool) -> Check<()> {
        timeout(WAIT, async {
            loop {
                let notified = self.frames.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.frames.values.lock().unwrap().iter().any(&wanted) {
                    return Ok(());
                }
                notified.await;
            }
        })
        .await
        .map_err(error)?
    }
    async fn prefix(&self) -> Check<()> {
        self.observed(|r| {
            r.stage == "after-assistant"
                && r.frame.direction == Direction::Downstream
                && r.frame.frame.metadata.get("audio_transcript_prefix") == Some(&json!(PREFIX))
        })
        .await?;
        self.peer.parked().await
    }
    async fn interrupt(&self) -> Check<()> {
        let frame = Frame::interruption();
        let id = frame.id;
        self.send(frame).await?;
        self.observed(|r| r.stage == "after-assistant" && r.frame.frame.id == id)
            .await?;
        self.peer.eof().await
    }
    async fn finish(mut self, cancel: bool) -> Outcome {
        let admission = if self.worker.has_finished() {
            Ok(())
        } else {
            timeout(WAIT, async {
                if cancel {
                    self.worker.cancel("audio transcript fixture").await
                } else {
                    self.worker.end().await
                }
            })
            .await
            .map_err(error)
            .and_then(|r| r.map_err(error))
        };
        if admission.is_err() {
            let _ = self
                .worker
                .cancel("audio fixture terminal admission failed")
                .await;
        }
        let mut owners = admission;
        let report = match timeout(WAIT, self.worker.join()).await {
            Ok(result) => result,
            Err(_) => {
                let _ = self.worker.cancel("audio fixture join deadline").await;
                owners = Err("worker deadline; cancelled owner actually joined".into());
                self.worker.join().await
            }
        };
        while let Some(record) = self.stopped.try_recv() {
            self.completions.push(record.completion().as_ref().clone());
        }
        let context = self.shared.snapshot().map_err(error);
        let errors = self.worker.errors();
        let frames = self.frames.values.lock().unwrap().clone();
        let receiver_close = self.stopped.closed_reason();
        let (exchanges, client_eofs, peer) = self.peer.join().await;
        Outcome {
            exchanges,
            client_eofs,
            frames,
            context,
            completions: self.completions,
            checkpoints: self.checkpoints,
            receiver_close,
            errors,
            report,
            owners: owners.and(peer),
        }
    }
}
fn service_frames(out: &Outcome) -> impl Iterator<Item = &Frame> {
    out.frames
        .iter()
        .filter(|r| r.stage == "after-service" && r.frame.direction == Direction::Downstream)
        .map(|r| &r.frame.frame)
}
fn texts(out: &Outcome) -> Vec<String> {
    service_frames(out)
        .filter_map(|frame| match &frame.data {
            FrameData::LlmText(text) => Some(text.text.clone()),
            _ => None,
        })
        .collect()
}
fn assert_owners(out: &Outcome, cancel: bool, errors: u64, requests: usize) {
    assert!(out.owners.is_ok(), "{:?}", out.owners);
    let report = out.report.as_ref().unwrap();
    assert_eq!(
        report.state,
        if cancel {
            WorkerState::Cancelled
        } else {
            WorkerState::Ended
        }
    );
    assert_eq!(report.total_errors, errors, "{report:?}");
    assert_eq!(out.errors.len() as u64, errors, "{:?}", out.errors);
    assert!(out.errors.iter().all(|e| !e.fatal));
    assert_eq!(
        out.receiver_close,
        Some(if cancel {
            AssistantStoppedClose::Cancel
        } else {
            AssistantStoppedClose::End
        })
    );
    assert_eq!(
        out.exchanges.len(),
        requests,
        "unexpected duplicate/inference HTTP request"
    );
    for exchange in &out.exchanges {
        assert_eq!(
            exchange.headers.lines().next(),
            Some("POST /v1/chat/completions HTTP/1.1")
        );
        assert!(exchange
            .headers
            .to_ascii_lowercase()
            .contains("authorization: bearer synthetic-audio-transcript\r\n"));
    }
    assert_eq!(out.exchanges[0].body, expected_request(initial_messages()));
    assert!(
        service_frames(out).all(|f| !matches!(
            f.data,
            FrameData::TtsAudio(_)
                | FrameData::OutputAudio(_)
                | FrameData::TtsStarted(_)
                | FrameData::TtsStopped(_)
                | FrameData::LlmContinuationCheckpoint
        )),
        "transcript invented playback or provider-specific continuation"
    );
    for frame in service_frames(out) {
        if let FrameData::LlmText(text) = &frame.data {
            assert!(text.append_to_context && text.includes_inter_frame_spaces);
            assert_eq!(text.skip_tts, None);
            assert!(!serde_json::to_string(&frame.metadata)
                .unwrap()
                .contains("ignored-audio"));
        }
    }
}
fn assert_completion(out: &Outcome, index: usize, text: &str, interrupted: bool) {
    assert_eq!(out.completions[index].text(), text);
    assert_eq!(out.completions[index].interrupted(), interrupted);
}

#[tokio::test]
async fn fragmented_audio_transcript_commits_exact_paired_history_before_second_http_request() {
    let first = Script::complete(vec![
        transcript("你好"),
        transcript("， streamed "),
        delta(json!({"content":"text wins", "audio":{"transcript":"SUPPRESSED"}})),
        json!({"choices":[{"index":0,"delta":{"audio":{"transcript":"!"}},"finish_reason":"stop"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}),
    ]);
    let mut rig = Rig::new(vec![
        first,
        Script::complete(vec![transcript("next"), finish_event()]),
    ])
    .await;
    let checked = async {
        rig.run().await?;
        rig.stopped().await?;
        rig.run().await?;
        rig.stopped().await
    }
    .await;
    let out = rig.finish(false).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_owners(&out, false, 0, 2);
    const ANSWER: &str = "你好， streamed text wins!";
    assert_eq!(
        texts(&out),
        ["你好", "， streamed ", "text wins", "!", "next"]
    );
    assert_eq!(
        out.checkpoints,
        [history_with(&[ANSWER]), history_with(&[ANSWER, "next"])]
    );
    assert_eq!(
        out.context.as_ref().unwrap(),
        &history_with(&[ANSWER, "next"])
    );
    assert_eq!(
        out.exchanges[1].body,
        expected_request(wire_history(&[ANSWER]))
    );
    assert_eq!(out.completions.len(), 2);
    assert_completion(&out, 0, ANSWER, false);
    assert_completion(&out, 1, "next", false);
    let ends = service_frames(&out)
        .filter(|f| matches!(f.data, FrameData::LlmResponseEnd))
        .collect::<Vec<_>>();
    assert_eq!(ends.len(), 2);
    assert!(ends
        .iter()
        .all(|f| f.metadata.get("failed") == Some(&json!(false))));
    assert!(ends
        .iter()
        .all(|f| f.metadata.get("finish_reason") == Some(&json!("stop"))));
}

fn decode(decoder: &mut ProtocolDecoder, value: Value) -> Result<Vec<StreamEvent>, ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: value.to_string(),
    })
}
fn done(decoder: &mut ProtocolDecoder) -> Result<Vec<StreamEvent>, ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: "[DONE]".into(),
    })
}
#[test]
fn audio_fallback_preserves_existing_tool_content_events_and_rejects_selected_wrong_types() {
    for (payload, mut expected) in [
        (
            json!({"audio":{"transcript":"audio"}}),
            vec![StreamEvent::TextDelta("audio".into())],
        ),
        (
            json!({"content":"text", "audio":{"transcript":"SUPPRESSED"}}),
            vec![StreamEvent::TextDelta("text".into())],
        ),
        (
            json!({"content":"text", "audio":17}),
            vec![StreamEvent::TextDelta("text".into())],
        ),
        (
            json!({"tool_calls":[],"content":"", "audio":{"transcript":"fallback"}}),
            vec![StreamEvent::TextDelta("fallback".into())],
        ),
        (
            json!({"content":null,"audio":{"transcript":" "}}),
            vec![StreamEvent::TextDelta(" ".into())],
        ),
        (json!({"audio":null}), vec![]),
        (json!({"audio":{}}), vec![]),
        (
            json!({"audio":{"transcript":null,"data":"ignored"}}),
            vec![],
        ),
        (
            json!({"audio":{"transcript":"","id":"ignored","expires_at":5}}),
            vec![],
        ),
    ] {
        let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
        expected.insert(0, StreamEvent::ResponseProgress);
        assert_eq!(decode(&mut decoder, delta(payload)).unwrap(), expected);
        assert!(done(&mut decoder).unwrap().is_empty());
    }
    for content in [None, Some("existing content")] {
        let mut payload = json!({"tool_calls":[{"index":0,"id":"call-audio","type":"function","function":{"name":"lookup","arguments":"{}"}}],"audio":{"transcript":"SUPPRESSED"}});
        if let Some(content) = content {
            payload["content"] = json!(content);
        }
        let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
        let events = decode(&mut decoder, delta(payload)).unwrap();
        let mut expected = content
            .map(|text| StreamEvent::TextDelta(text.into()))
            .into_iter()
            .collect::<Vec<_>>();
        expected.insert(0, StreamEvent::ResponseProgress);
        expected.push(StreamEvent::ToolCallDelta {
            index: 0,
            id: Some("call-audio".into()),
            name: Some("lookup".into()),
            arguments: "{}".into(),
        });
        assert_eq!(
            events, expected,
            "existing content+tools co-emission remains intact"
        );
        assert_eq!(
            done(&mut decoder).unwrap(),
            vec![StreamEvent::ToolCall(pipecat_services::ToolCall {
                id: "call-audio".into(),
                name: "lookup".into(),
                arguments: json!({})
            })]
        );
    }
    for audio in [
        json!(false),
        json!(17),
        json!("not-object"),
        json!([]),
        json!({"transcript":false}),
        json!({"transcript":17}),
        json!({"transcript":[]}),
        json!({"transcript":{}}),
    ] {
        let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
        assert!(matches!(
            decode(&mut decoder, delta(json!({"audio":audio}))),
            Err(ServiceError::Protocol(_))
        ));
        assert!(matches!(
            decode(&mut decoder, transcript("must-not-escape-poison")),
            Err(ServiceError::Protocol(_))
        ));
    }
    let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
    assert_eq!(
        decode(&mut decoder, finish_event()).unwrap(),
        vec![StreamEvent::ResponseProgress]
    );
    assert!(
        !decoder.is_done(),
        "finish_reason alone is not the stream completion marker"
    );
    done(&mut decoder).unwrap();
    assert!(decode(&mut decoder, transcript("after-DONE"))
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn malformed_audio_and_premature_eof_commit_only_delivered_prefix_with_failed_end() {
    for malformed in [true, false] {
        let mut events = vec![transcript(PREFIX)];
        if malformed {
            events.push(delta(json!({"audio":{"transcript":7}})));
            events.push(transcript(HIDDEN));
        } else {
            events.push(finish_event());
        }
        let script = Script {
            bytes: body(&events, malformed),
            hold_until_client_eof: false,
            // Place error and ignored suffix in one bounded write: a decoder
            // rejection must not race unrelated later test-server writes.
            fragment: false,
        };
        let mut rig = Rig::new(vec![script]).await;
        let checked = async {
            rig.run().await?;
            rig.stopped().await
        }
        .await;
        let out = rig.finish(false).await;
        assert!(checked.is_ok(), "malformed={malformed}: {checked:?}");
        assert_owners(&out, false, 1, 1);
        assert_eq!(texts(&out), [PREFIX]);
        assert_eq!(out.context.as_ref().unwrap(), &history_with(&[PREFIX]));
        assert_eq!(out.checkpoints, [history_with(&[PREFIX])]);
        assert_eq!(out.completions.len(), 1);
        assert_completion(&out, 0, PREFIX, true);
        assert_eq!(out.errors[0].category, ErrorCategory::Application);
        let expected = if malformed {
            "invalid stream: transcript must be a string or null"
        } else {
            "invalid stream: stream ended before the provider completion marker"
        };
        assert_eq!(out.errors[0].error, expected);
        let ends = service_frames(&out)
            .filter(|f| matches!(f.data, FrameData::LlmResponseEnd))
            .collect::<Vec<_>>();
        assert_eq!(ends.len(), 1);
        assert_eq!(ends[0].metadata.get("failed"), Some(&json!(true)));
    }
}

#[tokio::test]
async fn interruption_retires_unfinished_audio_and_next_http_uses_processed_prefix() {
    let mut rig = Rig::new(vec![
        Script::stalled(),
        Script::complete(vec![transcript("fresh answer"), finish_event()]),
    ])
    .await;
    let checked = async {
        rig.run().await?;
        rig.prefix().await?;
        rig.interrupt().await?;
        rig.stopped().await?;
        rig.run().await?;
        rig.stopped().await
    }
    .await;
    let out = rig.finish(false).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_owners(&out, false, 0, 2);
    assert_eq!(out.client_eofs, 1);
    assert_eq!(texts(&out), [PREFIX, "fresh answer"]);
    assert_eq!(
        out.checkpoints,
        [
            history_with(&[PREFIX]),
            history_with(&[PREFIX, "fresh answer"])
        ]
    );
    assert_eq!(
        out.context.as_ref().unwrap(),
        &history_with(&[PREFIX, "fresh answer"])
    );
    assert_eq!(
        out.exchanges[1].body,
        expected_request(wire_history(&[PREFIX]))
    );
    assert_eq!(out.completions.len(), 2);
    assert_completion(&out, 0, PREFIX, true);
    assert_completion(&out, 1, "fresh answer", false);
    assert_eq!(
        service_frames(&out)
            .filter(|f| matches!(f.data, FrameData::LlmResponseEnd))
            .count(),
        1,
        "retired stream cannot publish a fabricated successful end"
    );
}

#[tokio::test]
async fn cancel_joins_http_owner_and_preserves_only_assistant_processed_audio_prefix() {
    let rig = Rig::new(vec![Script::stalled()]).await;
    let checked = async {
        rig.run().await?;
        rig.prefix().await
    }
    .await;
    let out = rig.finish(true).await;
    assert!(checked.is_ok(), "{checked:?}");
    assert_owners(&out, true, 0, 1);
    assert_eq!(out.client_eofs, 1);
    assert_eq!(texts(&out), [PREFIX]);
    assert_eq!(out.context.as_ref().unwrap(), &history_with(&[PREFIX]));
    assert_eq!(out.completions.len(), 1);
    assert_completion(&out, 0, PREFIX, true);
    assert_eq!(
        service_frames(&out)
            .filter(|f| matches!(f.data, FrameData::LlmResponseEnd))
            .count(),
        0
    );
}
