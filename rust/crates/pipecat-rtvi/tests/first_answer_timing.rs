//! First answer timing through local HTTP/SSE, the real LLM processor, and RTVI.
//! Gates observe accepted pipeline output, never raw provider arrival timestamps.
//! Both every-request and processor-lifetime initial-only timing are qualified.
use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, FunctionCall, LlmContinuationBlock,
    LlmContinuationProvider, MessagesFrame, Metric, NativeLlmContinuation,
    NativeLlmContinuationError, ObservationEvent, ObservedFrame, ObservedProcessor,
    ObserverOptions, Pipeline, PipelineConfig, PipelineObservation, PipelineObserver,
    PipelineWorker, StartFrame,
};
use pipecat_observers::{metrics_stream, Measurement, MetricsObserver, MetricsStream};
use pipecat_rtvi::{Observation, RtviObserver, RtviObserverParams};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use serde_json::{json, Value};
use std::{fmt::Write as _, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

// Retain every source test in the prior 66-test Gemini and 98-test provider
// graphs without copying bodies. The 15 HTTP and 22 protocol tests shared by
// those graphs are loaded once through the provider graph: 127 unique prior
// tests plus the original 11 tests below retain all 138 prior source functions.
// Initial-only policy tests below extend that graph without removing coverage.
#[path = "../../pipecat-services/tests/gemini_first_progress.rs"]
mod gemini_first_progress_regressions;
#[path = "../../pipecat-services/tests/review_gemini_part_order.rs"]
mod gemini_part_order_regressions;
#[path = "../../pipecat-services/tests/gemini_signatures.rs"]
mod gemini_signature_regressions;
#[path = "../../pipecat-services/tests/review_gemini_terminal_boundaries.rs"]
mod gemini_terminal_regressions;
#[path = "provider_first_progress.rs"]
mod provider_ttfb_regressions;
#[path = "../../pipecat-services/tests/runtime_processor.rs"]
mod runtime_processor_regressions;

const WAIT: Duration = Duration::from_secs(10);
const NAME: &str = "first-answer-llm";
type TestResult<T> = Result<T, String>;

fn choice(delta: Value) -> Value {
    json!({"choices":[{"index":0,"delta":delta}]})
}
fn block(index: usize, content: Value) -> Value {
    json!({"type":"content_block_start","index":index,"content_block":content})
}
fn stop_block(index: usize) -> Value {
    json!({"type":"content_block_stop","index":index})
}
fn candidate(parts: Value) -> Value {
    json!({"candidates":[{"index":0,"content":{"parts":parts}}]})
}
fn wire(records: &[Value]) -> Vec<u8> {
    let mut body = String::new();
    for record in records {
        write!(body, "data: {record}\n\n").unwrap();
    }
    body.into_bytes()
}
fn terminal(provider: Provider) -> Vec<u8> {
    match provider {
        Provider::OpenAi => b"data: [DONE]\n\n".to_vec(),
        Provider::Anthropic => wire(&[
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
            json!({"type":"message_stop"}),
        ]),
        Provider::Gemini => wire(&[json!({"candidates":[{"finishReason":"STOP"}]})]),
    }
}
fn config(listener: &TcpListener, provider: Provider) -> ClientConfig {
    let mut config = ClientConfig::new(provider, "synthetic-key");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    config.request_timeout = Duration::from_secs(30);
    config.idle_timeout = Duration::from_secs(20);
    config
}

// The same bounded HTTP framing used by the existing first-progress fixtures.
async fn read_request(socket: &mut TcpStream) -> TestResult<Value> {
    const LIMIT: usize = 1024 * 1024;
    let mut bytes = vec![];
    let mut part = [0; 4096];
    let end = loop {
        let n = socket.read(&mut part).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("EOF before request headers".into());
        }
        bytes.extend_from_slice(&part[..n]);
        if bytes.len() > LIMIT {
            return Err("oversize request".into());
        }
        if let Some(i) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).map_err(|e| e.to_string())?;
    if !headers.starts_with("POST /stream HTTP/1.1\r\n") {
        return Err("unexpected request target".into());
    }
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse::<usize>())
        })
        .ok_or("missing request length")?
        .map_err(|e| e.to_string())?;
    if length > LIMIT.saturating_sub(end) {
        return Err("oversize request body".into());
    }
    while bytes.len() < end + length {
        let n = socket.read(&mut part).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("EOF before request body".into());
        }
        bytes.extend_from_slice(&part[..n]);
    }
    let body: Value =
        serde_json::from_slice(&bytes[end..end + length]).map_err(|e| e.to_string())?;
    if !body.is_object() {
        return Err("non-object inference request".into());
    }
    Ok(body)
}
fn verify_tool_continuation(body: &Value, provider: Provider) -> TestResult<()> {
    let messages = body["messages"]
        .as_array()
        .ok_or("missing tool continuation messages")?;
    if messages.len() != 3 || messages[1]["role"] != "assistant" {
        return Err(format!(
            "expected initial user, tool call, and result: {messages:?}"
        ));
    }
    let matches = match provider {
        Provider::OpenAi => {
            let call = &messages[1]["tool_calls"][0];
            let arguments = call["function"]["arguments"]
                .as_str()
                .ok_or("missing serialized arguments")?;
            let result = messages[2]["content"]
                .as_str()
                .ok_or("missing serialized OpenAI tool result")?;
            let parsed_result = serde_json::from_str::<Value>(result)
                .map_err(|error| format!("invalid OpenAI tool result JSON: {error}"))?;
            call["id"] == "tool-1"
                && call["function"]["name"] == "weather"
                && serde_json::from_str::<Value>(arguments).map_err(|e| e.to_string())?
                    == json!({"city":"Paris"})
                && messages[2]["role"] == "tool"
                && messages[2]["tool_call_id"] == "tool-1"
                && parsed_result == json!({"temperature":18})
                && result == r#"{"temperature":18}"#
        }
        Provider::Anthropic => {
            let call = messages[1]["content"]
                .as_array()
                .and_then(|parts| parts.iter().find(|part| part["type"] == "tool_use"))
                .ok_or("missing Anthropic tool use")?;
            let result = &messages[2]["content"][0];
            let result_text = result["content"][0]["text"]
                .as_str()
                .ok_or("missing serialized Anthropic tool result")?;
            let parsed_result = serde_json::from_str::<Value>(result_text)
                .map_err(|error| format!("invalid Anthropic tool result JSON: {error}"))?;
            call["id"] == "tool-1"
                && call["name"] == "weather"
                && call["input"] == json!({"city":"Paris"})
                && messages[2]["role"] == "user"
                && result["type"] == "tool_result"
                && result["tool_use_id"] == "tool-1"
                && parsed_result == json!({"temperature":18})
                && result_text == r#"{"temperature":18}"#
        }
        Provider::Gemini => {
            return Err("this continuation fixture uses OpenAI and Anthropic".into())
        }
    };
    if matches {
        Ok(())
    } else {
        Err(format!("tool continuation mismatch: {messages:?}"))
    }
}
fn tool_continuation(trace: &Trace) -> TestResult<FrameData> {
    if trace.calls.len() != 1 {
        return Err("tool continuation requires exactly one completed call".into());
    }
    let call = &trace.calls[0];
    if call.function_name != "weather" || call.arguments != json!({"city":"Paris"}) {
        return Err("local fixture tool received incorrect arguments".into());
    }
    let mut assistant = ChatMessage::new(ChatRole::Assistant, "");
    assistant.tool_calls = trace.calls.clone();
    // Execute this deterministic local fixture only after complete arguments
    // reached FunctionCallsStarted; carry its result into the next inference.
    let mut result = ChatMessage::new(ChatRole::Tool, json!({"temperature":18}).to_string());
    result.name = Some(call.function_name.clone());
    result.tool_call_id = Some(call.tool_call_id.clone());
    Ok(FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![assistant, result],
        run_llm: true,
    }))
}
async fn write(socket: &mut TcpStream, bytes: &[u8]) -> TestResult<()> {
    socket.write_all(bytes).await.map_err(|e| e.to_string())
}
async fn header(socket: &mut TcpStream, length: usize) -> TestResult<()> {
    write(
        socket,
        format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {length}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
}
async fn closed(socket: &mut TcpStream) -> TestResult<()> {
    let mut byte = [0];
    match socket.read(&mut byte).await {
        Ok(0) => Ok(()),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) =>
        {
            Ok(())
        }
        other => Err(format!("expected client EOF/reset, got {other:?}")),
    }
}
async fn join_peer(mut peer: JoinHandle<TestResult<()>>) -> TestResult<()> {
    match timeout(WAIT, &mut peer).await {
        Ok(result) => result.map_err(|e| e.to_string())?,
        Err(_) => {
            peer.abort();
            let joined = peer.await;
            Err(format!("peer timeout; aborted and joined: {joined:?}"))
        }
    }
}
fn processor(config: ClientConfig) -> TestResult<Arc<LlmProcessor>> {
    let request = CompletionRequest::new(
        "gemini-2.5-fixture",
        Context::new(vec![Message::text(Role::User, "hello")]),
    );
    Ok(Arc::new(
        LlmProcessor::new(LlmClient::new(config).map_err(|e| e.to_string())?, request)
            .with_name(NAME),
    ))
}
async fn start(config: ClientConfig, metrics: bool) -> TestResult<(PipelineWorker, MetricsStream)> {
    start_processor(processor(config)?, metrics, false).await
}
async fn start_processor(
    processor: Arc<LlmProcessor>,
    metrics: bool,
    initial_only: bool,
) -> TestResult<(PipelineWorker, MetricsStream)> {
    let rtvi = Arc::new(RtviObserver::new(Default::default(), None).map_err(|e| e.to_string())?);
    let (publisher, stream) = metrics_stream(1024).map_err(str::to_owned)?;
    let worker = Pipeline::new(vec![
        processor,
        // Shared taps exercise real forwarding/deduplication, not a fabricated
        // wire record. A native metric must still produce exactly one envelope.
        Arc::new(rtvi.tap("rtvi-first")),
        Arc::new(rtvi.tap("rtvi-forwarded")),
    ])
    .with_observer(
        Arc::new(MetricsObserver::new(publisher)),
        ObserverOptions {
            capacity: 1024,
            observe_every_push: true,
            drain_timeout: WAIT,
        },
    )
    .start(PipelineConfig {
        start: StartFrame {
            enable_metrics: metrics,
            enable_usage_metrics: true,
            report_only_initial_ttfb: initial_only,
            ..Default::default()
        },
        shutdown_timeout: WAIT,
        ..Default::default()
    })
    .await
    .map_err(|e| e.to_string())?;
    Ok((worker, stream))
}
async fn send(worker: &PipelineWorker, data: FrameData) -> TestResult<()> {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .map_err(|e| e.to_string())
}
fn finite_nonnegative(value: f64) -> TestResult<()> {
    if value.is_finite() && value >= 0.0 {
        Ok(())
    } else {
        Err(format!("invalid timing value {value}"))
    }
}
fn breakdown(metric: &Metric) -> TestResult<Value> {
    let Metric::Ttfat {
        processor,
        ttfat,
        ttfb,
        thinking_time,
    } = metric
    else {
        return Err("expected TTFAT metric".into());
    };
    for value in [*ttfat, *ttfb, *thinking_time] {
        finite_nonnegative(value)?;
    }
    if *ttfat < *ttfb || (*ttfat - *ttfb - *thinking_time).abs() > 1e-9 {
        return Err(format!("inconsistent timing breakdown: {metric:?}"));
    }
    Ok(json!({"processor":processor,"ttfat":ttfat,"ttfb":ttfb,"thinking_time":thinking_time}))
}
#[derive(Debug, Default)]
struct Trace {
    ttfb: Vec<f64>,
    ttfat: Vec<Metric>,
    wire_ttfb: Vec<f64>,
    wire_ttfat: Vec<Value>,
    processing: Vec<f64>,
    wire_processing: Vec<f64>,
    usage: Vec<Metric>,
    wire_usage: Vec<Value>,
    starts: usize,
    text: String,
    thought: String,
    calls: Vec<FunctionCall>,
    checkpoints: Vec<u64>,
    errors: Vec<String>,
    order: Vec<&'static str>,
    native_ended: bool,
    wire_ended: bool,
}
impl Trace {
    fn record(&mut self, frame: &Frame) -> TestResult<bool> {
        match &frame.data {
            FrameData::Metrics(metrics) => {
                for metric in metrics {
                    match metric {
                        Metric::Ttfb { processor, seconds } => {
                            finite_nonnegative(*seconds)?;
                            if processor != NAME {
                                return Err("wrong TTFB owner".into());
                            }
                            self.ttfb.push(*seconds);
                            self.order.push("ttfb");
                        }
                        Metric::Ttfat { processor, .. } => {
                            breakdown(metric)?;
                            if processor != NAME {
                                return Err("wrong TTFAT owner".into());
                            }
                            self.ttfat.push(metric.clone());
                            self.order.push("ttfat");
                        }
                        Metric::Processing { processor, seconds } => {
                            finite_nonnegative(*seconds)?;
                            if processor != NAME {
                                return Err("wrong Processing owner".into());
                            }
                            self.processing.push(*seconds);
                        }
                        Metric::LlmUsage { processor, .. } => {
                            if processor != NAME {
                                return Err("wrong Usage owner".into());
                            }
                            self.usage.push(metric.clone());
                        }
                        _ => {}
                    }
                }
            }
            FrameData::OutputTransportMessageUrgent(value) if value["type"] == "metrics" => {
                if let Some(items) = value["data"]["processing"].as_array() {
                    for item in items {
                        if item["processor"] != NAME {
                            return Err("wrong wire Processing owner".into());
                        }
                        let seconds = item["value"].as_f64().ok_or("nonnumeric wire Processing")?;
                        finite_nonnegative(seconds)?;
                        self.wire_processing.push(seconds);
                    }
                }
                if let Some(items) = value["data"]["tokens"].as_array() {
                    self.wire_usage.extend(items.iter().cloned());
                }
                if let Some(items) = value["data"]["ttfb"].as_array() {
                    for item in items {
                        let seconds = item["value"].as_f64().ok_or("nonnumeric wire TTFB")?;
                        finite_nonnegative(seconds)?;
                        self.wire_ttfb.push(seconds);
                    }
                }
                if let Some(items) = value["data"].get("ttfat") {
                    let items = items.as_array().ok_or("wire TTFAT must be an array")?;
                    for item in items {
                        let object = item.as_object().ok_or("wire TTFAT must contain objects")?;
                        if object.len() != 4
                            || item["processor"] != NAME
                            || item.get("value").is_some()
                        {
                            return Err(format!("unexpected wire TTFAT shape: {item}"));
                        }
                        for key in ["ttfat", "ttfb", "thinking_time"] {
                            finite_nonnegative(item[key].as_f64().ok_or("nonnumeric wire TTFAT")?)?;
                        }
                        self.wire_ttfat.push(item.clone());
                        self.order.push("wire-ttfat");
                    }
                }
            }
            FrameData::LlmText(text) => {
                self.text.push_str(&text.text);
                self.order.push("text");
            }
            FrameData::LlmThoughtText(text) => {
                self.thought.push_str(&text.text);
                self.order.push("thought");
            }
            FrameData::FunctionCallsStarted(calls) => {
                self.calls.extend(calls.iter().cloned());
                self.order.push("tool");
            }
            FrameData::LlmContinuationCheckpoint => {
                let continuation = frame
                    .native_llm_continuation()
                    .map_err(|error| error.to_string())?
                    .ok_or("checkpoint lost its native carrier at RTVI boundary")?;
                self.checkpoints.push(continuation.response_id());
                self.order.push("checkpoint");
            }
            FrameData::FunctionCallInProgress(_) => self.order.push("tool-execution"),
            FrameData::Error(error) => self.errors.push(error.error.clone()),
            FrameData::LlmResponseStart => self.starts += 1,
            FrameData::LlmResponseEnd => self.native_ended = true,
            FrameData::OutputTransportMessageUrgent(value)
                if value["type"] == "bot-llm-stopped" =>
            {
                self.wire_ended = true;
            }
            _ => {}
        }
        // Urgent RTVI envelopes may overtake normal frames at the second tap.
        // Both terminal observations form the fence for all prior content.
        Ok(self.native_ended && self.wire_ended)
    }
    fn assert_answer(&self) {
        assert_eq!(self.ttfb.len(), 1, "{self:?}");
        assert_eq!(self.ttfat.len(), 1, "{self:?}");
        assert_eq!(self.wire_ttfb, self.ttfb, "{self:?}");
        assert_eq!(
            self.wire_ttfat,
            vec![breakdown(&self.ttfat[0]).unwrap()],
            "{self:?}"
        );
        let Metric::Ttfat { ttfb, .. } = &self.ttfat[0] else {
            unreachable!()
        };
        assert_eq!(*ttfb, self.ttfb[0]);
        assert!(
            self.order.iter().position(|s| *s == "ttfb")
                < self.order.iter().position(|s| *s == "ttfat")
        );
        assert!(self.errors.is_empty(), "{self:?}");
    }
}

// Cleanup always joins the pipeline owner and peer, even when a gate or an
// assertion predicate fails. Timeouts fail the test, never stand in for a join.
async fn finish(worker: &mut PipelineWorker, cancel: bool, trace: &mut Trace) -> TestResult<()> {
    finish_with_errors(worker, cancel, trace, 0).await
}
async fn finish_with_errors(
    worker: &mut PipelineWorker,
    cancel: bool,
    trace: &mut Trace,
    expected_errors: usize,
) -> TestResult<()> {
    let mut errors = vec![];
    let control = if cancel {
        worker.cancel("TTFAT fixture cleanup").await
    } else {
        worker.end().await
    };
    if let Err(error) = control {
        errors.push(error.to_string());
    }
    let drained = timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            if let Err(error) = trace.record(&frame.frame) {
                errors.push(error);
            }
        }
    })
    .await;
    if drained.is_err() || !errors.is_empty() {
        if let Err(error) = worker.cancel("TTFAT fixture drain failure").await {
            errors.push(error.to_string());
        }
    }
    if drained.is_err() {
        errors.push("owner drain timeout".into());
    }
    match timeout(WAIT, worker.join()).await {
        Ok(Ok(report)) => {
            if report.errors.len() != expected_errors {
                errors.push(format!(
                    "expected {expected_errors} owner errors: {:?}",
                    report.errors
                ));
            }
        }
        Ok(Err(error)) => errors.push(error.to_string()),
        Err(_) => {
            errors.push("owner first join timeout".into());
            let _ = worker.cancel("TTFAT fixture join timeout").await;
            match timeout(WAIT, worker.join()).await {
                Ok(Ok(_)) => {}
                other => errors.push(format!("owner could not be joined: {other:?}")),
            }
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}
fn observed_ttfat(stream: &mut MetricsStream) -> Vec<Metric> {
    assert_eq!(
        stream.stats().dropped,
        0,
        "bounded metrics stream overflowed"
    );
    let mut metrics = vec![];
    while let Ok(measurement) = stream.try_recv() {
        match measurement {
            Measurement::ReportedService {
                metric: metric @ Metric::Ttfat { .. },
                ..
            } => metrics.push(metric),
            Measurement::ObservationLoss { dropped_total } => {
                panic!("observation loss: {dropped_total}")
            }
            Measurement::TruncatedMetrics { omitted, .. } => panic!("truncated metrics: {omitted}"),
            _ => {}
        }
    }
    metrics
}

#[derive(Clone)]
struct Scenario {
    provider: Provider,
    // When present, answer bytes are withheld until a real thought frame has
    // crossed both RTVI taps. No answer timing may have appeared at that point.
    thoughts: Vec<Value>,
    answer: Vec<Value>,
    tail: Vec<Value>,
    gate_answer: bool,
    metrics: bool,
    tool_continuation: bool,
}
impl Scenario {
    fn answer(provider: Provider, answer: Vec<Value>, tail: Vec<Value>) -> Self {
        Self {
            provider,
            thoughts: vec![],
            answer,
            tail,
            gate_answer: true,
            metrics: true,
            tool_continuation: false,
        }
    }
}
async fn response(
    worker: &mut PipelineWorker,
    trace: &mut Trace,
    mut thought_gate: Option<oneshot::Sender<()>>,
    mut answer_gate: Option<oneshot::Sender<()>>,
) -> TestResult<()> {
    loop {
        let frame = worker
            .recv()
            .await
            .ok_or("pipeline EOF before RTVI response end")?;
        let ended = trace.record(&frame.frame)?;
        if !trace.thought.is_empty() && thought_gate.is_some() {
            if !trace.ttfat.is_empty() || !trace.wire_ttfat.is_empty() || !trace.calls.is_empty() {
                return Err(format!(
                    "thought/progress incorrectly counted as answer: {trace:?}"
                ));
            }
            if trace.ttfb.len() != 1 || trace.wire_ttfb.len() != 1 {
                return Err("thought did not preserve existing TTFB ordering".into());
            }
            thought_gate
                .take()
                .unwrap()
                .send(())
                .map_err(|_| "peer dropped thought gate")?;
        }
        if !trace.wire_ttfat.is_empty() && answer_gate.is_some() {
            if !trace.calls.is_empty() || trace.order.contains(&"tool-execution") {
                return Err("tool dispatched before withheld arguments/terminal bytes".into());
            }
            answer_gate
                .take()
                .unwrap()
                .send(())
                .map_err(|_| "peer dropped RTVI metric gate")?;
        }
        if ended {
            if thought_gate.is_some() || answer_gate.is_some() {
                return Err("response ended without its causal gate".into());
            }
            return Ok(());
        }
    }
}
async fn fixture(scenario: Scenario, repeats: usize) -> Vec<Trace> {
    fixture_rounds(vec![scenario; repeats], false).await
}
async fn fixture_rounds(scenarios: Vec<Scenario>, initial_only: bool) -> Vec<Trace> {
    let first = scenarios.first().expect("at least one response");
    assert!(scenarios
        .iter()
        .all(|case| case.provider == first.provider && case.metrics == first.metrics));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener, first.provider);
    let metrics = first.metrics;
    let mut gates = vec![];
    let mut receivers = vec![];
    for _ in &scenarios {
        let (thought_tx, thought_rx) = oneshot::channel();
        let (answer_tx, answer_rx) = oneshot::channel();
        gates.push((thought_tx, answer_tx));
        receivers.push((thought_rx, answer_rx));
    }
    let peer_cases = scenarios.clone();
    let peer = tokio::spawn(async move {
        for (round, (peer_case, (thought_gate, answer_gate))) in
            peer_cases.into_iter().zip(receivers).enumerate()
        {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let body = read_request(&mut socket).await?;
            if round > 0 && peer_case.tool_continuation {
                verify_tool_continuation(&body, peer_case.provider)?;
            }
            let thoughts = wire(&peer_case.thoughts);
            let answer = wire(&peer_case.answer);
            let tail = [wire(&peer_case.tail), terminal(peer_case.provider)].concat();
            header(&mut socket, thoughts.len() + answer.len() + tail.len()).await?;
            write(&mut socket, &thoughts).await?;
            if !peer_case.thoughts.is_empty() && thought_gate.await.is_err() {
                return closed(&mut socket).await;
            }
            write(&mut socket, &answer).await?;
            if peer_case.gate_answer && answer_gate.await.is_err() {
                return closed(&mut socket).await;
            }
            write(&mut socket, &tail).await?;
        }
        Ok(())
    });
    let (mut worker, mut stream) =
        match start_processor(processor(config).unwrap(), metrics, initial_only).await {
            Ok(started) => started,
            Err(error) => {
                drop(gates);
                let peer = join_peer(peer).await;
                panic!("start={error}; peer={peer:?}");
            }
        };
    let mut traces = vec![];
    let outcome = timeout(WAIT, async {
        for (round, (scenario, (thought_gate, answer_gate))) in
            scenarios.into_iter().zip(gates).enumerate()
        {
            let input = if round > 0 && scenario.tool_continuation {
                tool_continuation(traces.last().ok_or("missing first tool round")?)?
            } else {
                FrameData::LlmRun
            };
            send(&worker, input).await?;
            let mut trace = Trace::default();
            response(
                &mut worker,
                &mut trace,
                (!scenario.thoughts.is_empty()).then_some(thought_gate),
                scenario.gate_answer.then_some(answer_gate),
            )
            .await?;
            traces.push(trace);
        }
        Ok::<_, String>(())
    })
    .await;
    let mut cleanup = Trace::default();
    let owner = finish(&mut worker, !matches!(&outcome, Ok(Ok(()))), &mut cleanup).await;
    let peer = join_peer(peer).await;
    assert!(
        matches!(&outcome, Ok(Ok(()))),
        "response={outcome:?}; owner={owner:?}; peer={peer:?}"
    );
    owner.unwrap();
    peer.unwrap();
    assert!(
        cleanup.ttfb.is_empty()
            && cleanup.ttfat.is_empty()
            && cleanup.wire_ttfat.is_empty()
            && cleanup.checkpoints.is_empty(),
        "delayed timing after RTVI terminal: {cleanup:?}"
    );
    assert_eq!(
        observed_ttfat(&mut stream),
        traces
            .iter()
            .flat_map(|trace| trace.ttfat.clone())
            .collect::<Vec<_>>()
    );
    traces
}

#[tokio::test]
async fn thought_then_answer_uses_actual_rtvi_gate_and_resets_each_inference() {
    for provider in [Provider::OpenAi, Provider::Anthropic, Provider::Gemini] {
        let (thoughts, answer, tail) = match provider {
            Provider::OpenAi => (
                vec![
                    choice(json!({"role":"assistant"})),
                    choice(json!({"reasoning_content":"plan"})),
                ],
                vec![choice(json!({"content":"answer"}))],
                vec![choice(json!({"content":" tail"})), choice(json!({}))],
            ),
            Provider::Anthropic => (
                vec![block(
                    0,
                    json!({"type":"thinking","thinking":"plan","signature":"signed"}),
                )],
                vec![
                    stop_block(0),
                    block(1, json!({"type":"text","text":"answer"})),
                ],
                vec![
                    json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":" tail"}}),
                    stop_block(1),
                ],
            ),
            Provider::Gemini => (
                vec![candidate(json!([{"text":"plan","thought":true}]))],
                vec![candidate(json!([{"text":"answer"}]))],
                vec![candidate(json!([{"text":" tail"}]))],
            ),
        };
        let scenario = Scenario {
            provider,
            thoughts,
            answer,
            tail,
            gate_answer: true,
            metrics: true,
            tool_continuation: false,
        };
        for trace in fixture(scenario, 2).await {
            trace.assert_answer();
            assert_eq!(trace.thought, "plan");
            assert_eq!(trace.text, "answer tail");
            assert_eq!(
                trace.checkpoints.len(),
                if provider == Provider::Anthropic {
                    2
                } else {
                    0
                },
                "signed thought checkpoints must cross both taps: {trace:?}"
            );
            assert!(trace.checkpoints.windows(2).all(|ids| ids[0] == ids[1]));
            let Metric::Ttfat { thinking_time, .. } = trace.ttfat[0] else {
                unreachable!()
            };
            assert!(
                thinking_time > 0.0,
                "causal thought-to-answer gate: {trace:?}"
            );
            let position = |event| trace.order.iter().position(|s| *s == event).unwrap();
            assert!(position("thought") < position("ttfat"));
            assert!(position("wire-ttfat") < position("text"));
        }
    }
}

#[tokio::test]
async fn first_partial_tool_unlocks_arguments_but_never_dispatches_early() {
    for provider in [Provider::OpenAi, Provider::Anthropic] {
        let (answer, tail) = match provider {
            Provider::OpenAi => (
                vec![choice(
                    json!({"tool_calls":[{"index":0,"id":"tool-1","type":"function","function":{"name":"weather","arguments":"{\"city\":"}}]}),
                )],
                vec![choice(
                    json!({"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]}),
                )],
            ),
            Provider::Anthropic => (
                vec![
                    block(
                        0,
                        json!({"type":"tool_use","id":"tool-1","name":"weather","input":{}}),
                    ),
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}),
                ],
                vec![
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"Paris\"}"}}),
                    stop_block(0),
                ],
            ),
            Provider::Gemini => unreachable!(),
        };
        let mut scenario = Scenario::answer(provider, answer, tail);
        scenario.tool_continuation = true;
        for trace in fixture(scenario, 2).await {
            trace.assert_answer();
            assert_eq!(
                trace.calls,
                vec![FunctionCall {
                    function_name: "weather".into(),
                    tool_call_id: "tool-1".into(),
                    arguments: json!({"city":"Paris"})
                }]
            );
            assert!(trace.text.is_empty());
            let metric = trace.order.iter().position(|s| *s == "wire-ttfat").unwrap();
            let dispatch = trace.order.iter().position(|s| *s == "tool").unwrap();
            assert!(metric < dispatch);
        }
    }
}

#[tokio::test]
async fn gemini_complete_tool_is_an_answer_without_tool_delta() {
    let scenario = Scenario::answer(
        Provider::Gemini,
        vec![candidate(
            json!([{"functionCall":{"id":"tool-1","name":"weather","args":{"city":"Paris"}},"thoughtSignature":"signed-tool"}]),
        )],
        vec![candidate(json!([{"text":"tail"}]))],
    );
    for trace in fixture(scenario, 2).await {
        trace.assert_answer();
        assert_eq!(trace.calls.len(), 1);
        assert_eq!(trace.calls[0].arguments, json!({"city":"Paris"}));
        assert_eq!(trace.text, "tail");
        assert_eq!(
            trace.checkpoints.len(),
            2,
            "signed Gemini checkpoints must cross both taps: {trace:?}"
        );
        assert_eq!(trace.checkpoints[0], trace.checkpoints[1]);
    }
}

#[tokio::test]
async fn accepted_empty_and_whitespace_text_are_answers_and_emit_only_once() {
    // An explicit Anthropic text delta preserves "". Empty block starts and
    // OpenAI/Gemini empty content are decoder progress, not TextDelta events.
    for text in ["", " \t\n"] {
        let scenario = Scenario::answer(
            Provider::Anthropic,
            vec![
                block(0, json!({"type":"text","text":""})),
                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":text}}),
            ],
            vec![
                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"tail"}}),
                stop_block(0),
            ],
        );
        for trace in fixture(scenario, 2).await {
            trace.assert_answer();
            assert_eq!(trace.text, format!("{text}tail"));
            assert_eq!(trace.order.iter().filter(|s| **s == "text").count(), 2);
        }
    }
    for provider in [Provider::OpenAi, Provider::Gemini] {
        let answer = match provider {
            Provider::OpenAi => choice(json!({"content":" \t"})),
            Provider::Gemini => candidate(json!([{"text":" \t"}])),
            Provider::Anthropic => unreachable!(),
        };
        let trace = fixture(Scenario::answer(provider, vec![answer], vec![]), 1)
            .await
            .remove(0);
        trace.assert_answer();
        assert_eq!(trace.text, " \t");
    }
}

#[tokio::test]
async fn thought_progress_usage_and_empty_provider_content_do_not_create_answer_timing() {
    for (provider, records, expected_ttfb, thought) in [
        (
            Provider::OpenAi,
            vec![choice(json!({"reasoning_content":"plan"}))],
            1,
            "plan",
        ),
        (
            Provider::Anthropic,
            vec![
                block(
                    0,
                    json!({"type":"thinking","thinking":"plan","signature":"signed"}),
                ),
                stop_block(0),
            ],
            1,
            "plan",
        ),
        (
            Provider::Gemini,
            vec![candidate(json!([{"text":"plan","thought":true}]))],
            1,
            "plan",
        ),
        (
            Provider::OpenAi,
            vec![
                choice(json!({"role":"assistant"})),
                choice(json!({})),
                choice(json!({"content":""})),
            ],
            1,
            "",
        ),
        (
            Provider::Anthropic,
            vec![block(0, json!({"type":"text","text":""})), stop_block(0)],
            1,
            "",
        ),
        (
            Provider::Gemini,
            vec![candidate(json!([])), candidate(json!([{"text":""}]))],
            1,
            "",
        ),
        (
            Provider::OpenAi,
            vec![
                json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":0,"total_tokens":1}}),
            ],
            0,
            "",
        ),
        (
            Provider::Anthropic,
            vec![
                json!({"type":"ping"}),
                json!({"type":"message_start","message":{"usage":{"input_tokens":1,"output_tokens":0}}}),
            ],
            0,
            "",
        ),
        // Gemini's terminal candidate itself remains a legacy TTFB trigger.
        (
            Provider::Gemini,
            vec![
                json!({"candidates":[],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":0,"totalTokenCount":1}}),
            ],
            1,
            "",
        ),
    ] {
        let scenario = Scenario {
            provider,
            thoughts: vec![],
            answer: records,
            tail: vec![],
            gate_answer: false,
            metrics: true,
            tool_continuation: false,
        };
        let trace = fixture(scenario, 1).await.remove(0);
        assert_eq!(trace.ttfb.len(), expected_ttfb, "{trace:?}");
        assert!(
            trace.ttfat.is_empty() && trace.wire_ttfat.is_empty(),
            "{trace:?}"
        );
        assert_eq!(trace.thought, thought);
        assert_eq!(
            trace.checkpoints.len(),
            usize::from(provider == Provider::Anthropic && thought == "plan"),
            "signed thought-only checkpoint must be preserved: {trace:?}"
        );
        assert!(
            trace.text.is_empty() && trace.calls.is_empty() && trace.errors.is_empty(),
            "{trace:?}"
        );
    }
}

#[tokio::test]
async fn disabled_service_metrics_preserve_answer_content_without_timing() {
    for provider in [Provider::OpenAi, Provider::Anthropic, Provider::Gemini] {
        let (answer, tail) = match provider {
            Provider::OpenAi => (vec![choice(json!({"content":"answer"}))], vec![]),
            Provider::Anthropic => (
                vec![block(0, json!({"type":"text","text":"answer"}))],
                vec![stop_block(0)],
            ),
            Provider::Gemini => (vec![candidate(json!([{"text":"answer"}]))], vec![]),
        };
        let scenario = Scenario {
            provider,
            thoughts: vec![],
            answer,
            tail,
            gate_answer: false,
            metrics: false,
            tool_continuation: false,
        };
        for trace in fixture(scenario, 2).await {
            assert!(
                trace.ttfb.is_empty()
                    && trace.ttfat.is_empty()
                    && trace.wire_ttfb.is_empty()
                    && trace.wire_ttfat.is_empty(),
                "{trace:?}"
            );
            assert_eq!(trace.text, "answer");
            assert!(trace.errors.is_empty());
        }
    }
}

#[tokio::test]
async fn interruption_cancels_timing_state_and_next_request_starts_fresh() {
    for after_answer in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener, Provider::OpenAi);
        let (closed_tx, closed_rx) = oneshot::channel();
        let (answer_tx, answer_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            header(&mut socket, 1_000_000).await?;
            let delta = if after_answer {
                json!({"content":"old-answer"})
            } else {
                json!({"reasoning_content":"old-plan"})
            };
            write(&mut socket, &wire(&[choice(delta)])).await?;
            closed(&mut socket).await?;
            closed_tx
                .send(())
                .map_err(|_| "owner dropped closed barrier")?;
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            let first = wire(&[choice(json!({"content":"new-answer"}))]);
            let tail = terminal(Provider::OpenAi);
            header(&mut socket, first.len() + tail.len()).await?;
            write(&mut socket, &first).await?;
            if answer_rx.await.is_err() {
                return closed(&mut socket).await;
            }
            write(&mut socket, &tail).await
        });
        let (mut worker, mut stream) = match start(config, true).await {
            Ok(started) => started,
            Err(error) => {
                drop(answer_tx);
                let peer = join_peer(peer).await;
                panic!("start={error}; peer={peer:?}");
            }
        };
        let mut old = Trace::default();
        let mut new = Trace::default();
        let outcome = timeout(WAIT, async {
            send(&worker, FrameData::LlmRun).await?;
            loop {
                let frame = worker
                    .recv()
                    .await
                    .ok_or("EOF before cancellation boundary")?;
                if old.record(&frame.frame)? {
                    return Err("old response ended before interruption".into());
                }
                if if after_answer {
                    !old.wire_ttfat.is_empty()
                } else {
                    !old.thought.is_empty()
                } {
                    break;
                }
            }
            send(&worker, FrameData::Interruption).await?;
            loop {
                let frame = worker
                    .recv()
                    .await
                    .ok_or("EOF before interruption acknowledgement")?;
                old.record(&frame.frame)?;
                if matches!(frame.frame.data, FrameData::Interruption) {
                    break;
                }
            }
            closed_rx
                .await
                .map_err(|_| "peer did not witness cancelled HTTP owner")?;
            send(&worker, FrameData::LlmRun).await?;
            response(&mut worker, &mut new, None, Some(answer_tx)).await
        })
        .await;
        let mut cleanup = Trace::default();
        let owner = finish(&mut worker, !matches!(&outcome, Ok(Ok(()))), &mut cleanup).await;
        let peer = join_peer(peer).await;
        assert!(
            matches!(&outcome, Ok(Ok(()))),
            "response={outcome:?}; owner={owner:?}; peer={peer:?}"
        );
        owner.unwrap();
        peer.unwrap();
        assert_eq!(old.ttfb.len(), 1);
        assert_eq!(old.ttfat.len(), usize::from(after_answer));
        assert_eq!(old.wire_ttfat.len(), usize::from(after_answer));
        new.assert_answer();
        assert_eq!(new.text, "new-answer");
        assert!(
            new.thought.is_empty() && cleanup.ttfat.is_empty() && cleanup.wire_ttfat.is_empty()
        );
        let expected = old.ttfat.into_iter().chain(new.ttfat).collect::<Vec<_>>();
        assert_eq!(observed_ttfat(&mut stream), expected);
    }
}

fn metric(ttfat: f64, ttfb: f64, thinking_time: f64) -> Metric {
    Metric::Ttfat {
        processor: NAME.into(),
        ttfat,
        ttfb,
        thinking_time,
    }
}
#[test]
fn core_serde_and_rtvi_shape_roundtrip_dedupe_and_disabled_contract() {
    let metric = metric(0.75, 0.25, 0.5);
    let serialized =
        json!({"kind":"ttfat","processor":NAME,"ttfat":0.75,"ttfb":0.25,"thinking_time":0.5});
    assert_eq!(serde_json::to_value(&metric).unwrap(), serialized);
    assert_eq!(
        serde_json::from_value::<Metric>(serialized).unwrap(),
        metric
    );
    let mut frame = Frame::new(FrameData::Metrics(vec![metric.clone()]));
    frame
        .metadata
        .insert("private-provider-timestamp".into(), json!(12345));
    let observer = RtviObserver::new(Default::default(), None).unwrap();
    let observation = Observation::new("llm", Direction::Downstream);
    let events = observer.observe(&frame, observation).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        serde_json::to_value(&events[0]).unwrap(),
        json!({"label":"rtvi-ai","type":"metrics","data":{"ttfat":[{"processor":NAME,"ttfat":0.75,"ttfb":0.25,"thinking_time":0.5}]}})
    );
    assert!(observer.observe(&frame, observation).unwrap().is_empty());
    observer.reset();
    assert_eq!(observer.observe(&frame, observation).unwrap().len(), 1);
    let disabled = RtviObserver::new(
        RtviObserverParams {
            metrics_enabled: false,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    assert!(disabled.observe(&frame, observation).unwrap().is_empty());
    let observer = RtviObserver::new(Default::default(), None).unwrap();
    let mut forwarded = observation;
    forwarded.first_push = false;
    assert!(observer.observe(&frame, forwarded).unwrap().is_empty());
    assert_eq!(observer.observe(&frame, observation).unwrap().len(), 1);
}

fn native_checkpoint(provider: LlmContinuationProvider) -> (Frame, Arc<NativeLlmContinuation>) {
    let block = match provider {
        LlmContinuationProvider::Anthropic => LlmContinuationBlock::Thinking {
            text: "private local thought".into(),
            signature: "private-anthropic-signature".into(),
        },
        LlmContinuationProvider::Gemini => LlmContinuationBlock::GeminiText {
            text: "private local thought".into(),
            thought: true,
            signature: Some("private-gemini-signature".into()),
        },
        LlmContinuationProvider::AwsBedrock => unreachable!(),
    };
    let carrier = Arc::new(
        NativeLlmContinuation::checkpoint(
            NativeLlmContinuation::next_response_id().unwrap(),
            provider,
            vec![block],
        )
        .unwrap(),
    );
    let mut frame = Frame::new(FrameData::LlmContinuationCheckpoint);
    frame.set_native_llm_continuation(carrier.clone()).unwrap();
    frame
        .metadata
        .insert("local-marker".into(), json!("preserve me"));
    (frame, carrier)
}

#[test]
fn native_checkpoint_projection_validates_carriers_without_serializing_or_consuming_them() {
    let observer = RtviObserver::new(Default::default(), None).unwrap();
    let observation = Observation::new("llm", Direction::Downstream);
    for provider in [
        LlmContinuationProvider::Anthropic,
        LlmContinuationProvider::Gemini,
    ] {
        let (frame, carrier) = native_checkpoint(provider);
        let original = frame.clone();
        assert!(observer.observe(&frame, observation).unwrap().is_empty());
        assert!(observer.observe(&frame, observation).unwrap().is_empty());
        assert_eq!(frame, original);
        assert!(Arc::ptr_eq(
            frame.native_llm_continuation().unwrap().unwrap(),
            &carrier
        ));
        assert!(
            serde_json::to_value(&frame).is_err(),
            "RTVI observation must not consume native wire protection"
        );
        assert!(
            serde_json::to_value(&frame.data).is_err(),
            "checkpoint FrameData stays nonserializable"
        );

        let missing = Frame::new(FrameData::LlmContinuationCheckpoint);
        assert_eq!(
            observer.observe(&missing, observation).unwrap_err().message,
            NativeLlmContinuationError::MissingCarrier.to_string()
        );

        let mut mutated = Frame::new(FrameData::LlmResponseStart);
        mutated
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(
                    NativeLlmContinuation::next_response_id().unwrap(),
                    provider,
                )
                .unwrap(),
            ))
            .unwrap();
        // A public payload change must not turn a Start carrier into a valid
        // checkpoint merely because the new frame kind has no wire projection.
        mutated.data = FrameData::LlmContinuationCheckpoint;
        assert_eq!(
            observer.observe(&mutated, observation).unwrap_err().message,
            NativeLlmContinuationError::FrameMismatch.to_string()
        );
    }
}

#[tokio::test]
async fn shared_rtvi_taps_preserve_native_checkpoint_frames_tokens_and_arc_identity() {
    let observer = Arc::new(RtviObserver::new(Default::default(), None).unwrap());
    let expected = [
        native_checkpoint(LlmContinuationProvider::Anthropic),
        native_checkpoint(LlmContinuationProvider::Gemini),
    ];
    let mut worker = Pipeline::new(vec![
        Arc::new(observer.tap("checkpoint-first")),
        Arc::new(observer.tap("checkpoint-forwarded")),
    ])
    .start(PipelineConfig {
        shutdown_timeout: WAIT,
        ..Default::default()
    })
    .await
    .unwrap();
    let barrier = Frame::new(FrameData::Data);
    let mut received = vec![];
    let mut wire_events = vec![];
    let outcome = timeout(WAIT, async {
        for (frame, _) in &expected {
            worker
                .sender()
                .send(frame.clone(), Direction::Downstream)
                .await
                .map_err(|error| error.to_string())?;
        }
        worker
            .sender()
            .send(barrier.clone(), Direction::Downstream)
            .await
            .map_err(|error| error.to_string())?;
        loop {
            let directed = worker
                .recv()
                .await
                .ok_or("EOF before checkpoint forwarding barrier")?;
            if directed.frame.id == barrier.id {
                break;
            }
            match &directed.frame.data {
                FrameData::LlmContinuationCheckpoint => {
                    if directed.direction != Direction::Downstream {
                        return Err("checkpoint direction changed".into());
                    }
                    received.push(directed.frame);
                }
                FrameData::OutputTransportMessageUrgent(value) => wire_events.push(value.clone()),
                FrameData::Error(error) => return Err(error.error.clone()),
                _ => {}
            }
        }
        Ok::<_, String>(())
    })
    .await;
    let mut cleanup = Trace::default();
    let owner = finish(&mut worker, !matches!(&outcome, Ok(Ok(()))), &mut cleanup).await;
    assert!(
        matches!(&outcome, Ok(Ok(()))),
        "forwarding={outcome:?}; owner={owner:?}"
    );
    owner.unwrap();
    assert!(
        wire_events.is_empty(),
        "native checkpoints must not produce RTVI envelopes: {wire_events:?}"
    );
    assert!(cleanup.checkpoints.is_empty());
    assert_eq!(received.len(), expected.len());
    for (frame, (original, carrier)) in received.iter().zip(&expected) {
        assert_eq!(
            frame, original,
            "the frame-preserving tap changed a native checkpoint"
        );
        assert_eq!(frame.observation_token(), original.observation_token());
        assert!(Arc::ptr_eq(
            frame.native_llm_continuation().unwrap().unwrap(),
            carrier
        ));
        assert!(serde_json::to_value(frame).is_err());
        assert!(serde_json::to_value(&frame.data).is_err());
    }
}

fn reported_event(metrics: Vec<Metric>, first_push: bool, sequence: u64) -> ObservationEvent {
    ObservationEvent {
        sequence,
        dropped_before: 0,
        observation: Arc::new(PipelineObservation::Push {
            source: ObservedProcessor {
                runtime_id: 1,
                index: 0,
                name: Arc::from(NAME),
            },
            destination: None,
            frame: ObservedFrame {
                id: 42,
                token: None,
                kind: "MetricsFrame",
                interruptible: false,
                vad_stop_secs: None,
                metrics,
                omitted_metrics: 0,
            },
            direction: Direction::Downstream,
            first_push,
            timestamp_ns: 123,
        }),
    }
}
#[tokio::test]
async fn real_metrics_observer_accepts_valid_breakdowns_and_rejects_each_invalid_field() {
    let (publisher, mut stream) = metrics_stream(64).unwrap();
    let observer = MetricsObserver::new(publisher);
    let valid = vec![metric(0.75, 0.25, 0.5), metric(0.0, 0.0, 0.0)];
    let mut input = valid.clone();
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.01] {
        input.extend([
            metric(bad, 0.25, 0.5),
            metric(0.75, bad, 0.5),
            metric(0.75, 0.25, bad),
        ]);
    }
    observer.on_event(reported_event(input, true, 0)).await;
    for expected in &valid {
        assert_eq!(
            stream.try_recv().unwrap(),
            Measurement::ReportedService {
                timestamp_ns: 123,
                metric: expected.clone()
            }
        );
    }
    assert!(stream.try_recv().is_err());
    observer.on_event(reported_event(valid, false, 1)).await;
    assert!(
        stream.try_recv().is_err(),
        "forwarded metrics must not be counted twice"
    );
    assert_eq!(stream.stats().dropped, 0);
}

fn usage_record(provider: Provider) -> Value {
    match provider {
        Provider::OpenAi => {
            json!({"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}})
        }
        Provider::Anthropic => {
            json!({"type":"message_delta","delta":{},"usage":{"input_tokens":2,"output_tokens":3}})
        }
        Provider::Gemini => {
            json!({"candidates":[],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":3,"totalTokenCount":5}})
        }
    }
}
fn text_case(provider: Provider, text: &str) -> Scenario {
    let (answer, mut tail) = match provider {
        Provider::OpenAi => (vec![choice(json!({"content":text}))], vec![]),
        Provider::Anthropic => (
            vec![block(0, json!({"type":"text","text":text}))],
            vec![stop_block(0)],
        ),
        Provider::Gemini => (vec![candidate(json!([{"text":text}]))], vec![]),
    };
    tail.push(usage_record(provider));
    Scenario::answer(provider, answer, tail)
}
fn assert_suppressed(trace: &Trace) {
    assert!(
        trace.ttfb.is_empty()
            && trace.ttfat.is_empty()
            && trace.wire_ttfb.is_empty()
            && trace.wire_ttfat.is_empty(),
        "initial-only timing escaped its lifetime allowance: {trace:?}"
    );
}
fn assert_finished_accounting(trace: &Trace, metrics: bool) {
    assert_eq!(trace.starts, 1, "{trace:?}");
    assert!(trace.native_ended && trace.wire_ended, "{trace:?}");
    assert_eq!(trace.processing.len(), usize::from(metrics), "{trace:?}");
    assert_eq!(trace.wire_processing, trace.processing, "{trace:?}");
    assert_eq!(
        trace.usage,
        vec![Metric::LlmUsage {
            processor: NAME.into(),
            prompt_tokens: 2,
            completion_tokens: 3,
            total_tokens: 5,
        }],
        "{trace:?}"
    );
    assert_eq!(
        trace.wire_usage,
        vec![json!({"prompt_tokens":2,"completion_tokens":3,"total_tokens":5})],
        "{trace:?}"
    );
    assert!(trace.errors.is_empty(), "{trace:?}");
}

#[tokio::test]
async fn initial_only_reports_first_request_and_preserves_later_content_processing_and_usage() {
    for provider in [Provider::OpenAi, Provider::Anthropic, Provider::Gemini] {
        let first = text_case(provider, "answer");
        let mut later = first.clone();
        // A suppressed metric cannot unlock a provider gate. Full terminal
        // receipt, owner join and the observer drain prove its absence instead.
        later.gate_answer = false;
        let traces = fixture_rounds(vec![first, later.clone(), later], true).await;
        traces[0].assert_answer();
        for trace in &traces[1..] {
            assert_suppressed(trace);
        }
        for trace in traces {
            assert_eq!(trace.text, "answer");
            assert_finished_accounting(&trace, true);
        }
    }
}

#[tokio::test]
async fn initial_only_thought_only_request_consumes_the_allowance_without_answer_timing() {
    for provider in [Provider::OpenAi, Provider::Anthropic, Provider::Gemini] {
        let records = match provider {
            Provider::OpenAi => vec![choice(json!({"reasoning_content":"plan"}))],
            Provider::Anthropic => vec![
                block(
                    0,
                    json!({"type":"thinking","thinking":"plan","signature":"signed"}),
                ),
                stop_block(0),
            ],
            Provider::Gemini => vec![candidate(json!([{"text":"plan","thought":true}]))],
        };
        let mut first = Scenario::answer(provider, records, vec![usage_record(provider)]);
        first.gate_answer = false;
        let mut later = text_case(provider, "answer");
        later.gate_answer = false;
        let traces = fixture_rounds(vec![first, later], true).await;
        let thought = &traces[0];
        assert_eq!(thought.ttfb.len(), 1, "{thought:?}");
        assert_eq!(thought.wire_ttfb, thought.ttfb);
        assert!(thought.ttfat.is_empty() && thought.wire_ttfat.is_empty());
        assert_eq!(thought.thought, "plan");
        assert!(thought.text.is_empty() && thought.calls.is_empty());
        assert_eq!(
            thought.checkpoints.len(),
            usize::from(provider == Provider::Anthropic)
        );
        assert_suppressed(&traces[1]);
        assert_eq!(traces[1].text, "answer");
        for trace in traces {
            assert_finished_accounting(&trace, true);
        }
    }
}

#[tokio::test]
async fn initial_only_partial_tool_times_first_round_and_suppresses_verified_tool_followup() {
    for provider in [Provider::OpenAi, Provider::Anthropic] {
        let (answer, mut tail) = match provider {
            Provider::OpenAi => (
                vec![choice(
                    json!({"tool_calls":[{"index":0,"id":"tool-1","type":"function","function":{"name":"weather","arguments":"{\"city\":"}}]}),
                )],
                vec![choice(
                    json!({"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\"}"}}]}),
                )],
            ),
            Provider::Anthropic => (
                vec![
                    block(
                        0,
                        json!({"type":"tool_use","id":"tool-1","name":"weather","input":{}}),
                    ),
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"city\":"}}),
                ],
                vec![
                    json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"Paris\"}"}}),
                    stop_block(0),
                ],
            ),
            Provider::Gemini => unreachable!(),
        };
        tail.push(usage_record(provider));
        let first = Scenario::answer(provider, answer, tail);
        let mut second = text_case(provider, "weather is 18");
        second.gate_answer = false;
        second.tool_continuation = true;
        let traces = fixture_rounds(vec![first, second], true).await;
        traces[0].assert_answer();
        assert_eq!(
            traces[0].calls,
            vec![FunctionCall {
                function_name: "weather".into(),
                tool_call_id: "tool-1".into(),
                arguments: json!({"city":"Paris"}),
            }]
        );
        let position = |event| {
            traces[0]
                .order
                .iter()
                .position(|item| *item == event)
                .unwrap()
        };
        assert!(position("wire-ttfat") < position("tool"));
        assert_suppressed(&traces[1]);
        assert_eq!(traces[1].text, "weather is 18");
        assert!(traces[1].calls.is_empty());
        for trace in traces {
            assert_finished_accounting(&trace, true);
        }
    }
}

#[tokio::test]
async fn initial_only_started_http_failure_consumes_allowance_before_any_provider_progress() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener, Provider::OpenAi);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        read_request(&mut socket).await?;
        write(
            &mut socket,
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await?;
        drop(socket);
        let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        read_request(&mut socket).await?;
        let body = [
            wire(&[
                choice(json!({"content":"recovered"})),
                usage_record(Provider::OpenAi),
            ]),
            terminal(Provider::OpenAi),
        ]
        .concat();
        header(&mut socket, body.len()).await?;
        write(&mut socket, &body).await
    });
    let (mut worker, mut stream) = start_processor(processor(config).unwrap(), true, true)
        .await
        .unwrap();
    let mut failed = Trace::default();
    let mut recovered = Trace::default();
    let outcome = timeout(WAIT, async {
        send(&worker, FrameData::LlmRun).await?;
        response(&mut worker, &mut failed, None, None).await?;
        send(&worker, FrameData::LlmRun).await?;
        response(&mut worker, &mut recovered, None, None).await
    })
    .await;
    let mut cleanup = Trace::default();
    let owner = finish_with_errors(
        &mut worker,
        !matches!(&outcome, Ok(Ok(()))),
        &mut cleanup,
        1,
    )
    .await;
    let peer = join_peer(peer).await;
    assert!(
        matches!(&outcome, Ok(Ok(()))),
        "response={outcome:?}; owner={owner:?}; peer={peer:?}"
    );
    owner.unwrap();
    peer.unwrap();
    assert_suppressed(&failed);
    assert_eq!(failed.starts, 1);
    assert!(failed.native_ended && failed.wire_ended);
    assert_eq!(failed.errors.len(), 1, "{failed:?}");
    assert!(failed.errors[0].contains("503"), "{failed:?}");
    assert_eq!(failed.processing.len(), 1, "{failed:?}");
    assert_eq!(failed.wire_processing, failed.processing);
    assert!(failed.text.is_empty() && failed.usage.is_empty() && failed.wire_usage.is_empty());
    assert_suppressed(&recovered);
    assert_eq!(recovered.text, "recovered");
    assert_finished_accounting(&recovered, true);
    assert_suppressed(&cleanup);
    assert!(observed_ttfat(&mut stream).is_empty());
}

#[tokio::test]
async fn initial_only_started_interruption_never_rearms_before_or_after_first_answer() {
    // Stage zero cancels an accepted HTTP request before any provider progress.
    // The other stages prove an interruption does not rearm thought/answer work.
    for stage in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener, Provider::OpenAi);
        let (accepted_tx, accepted_rx) = oneshot::channel();
        let (closed_tx, closed_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            header(&mut socket, 1_000_000).await?;
            let records = match stage {
                0 => vec![],
                1 => vec![choice(json!({"reasoning_content":"old-plan"}))],
                _ => vec![choice(json!({"content":"old-answer"}))],
            };
            write(&mut socket, &wire(&records)).await?;
            accepted_tx
                .send(())
                .map_err(|_| "owner dropped HTTP acceptance gate")?;
            closed(&mut socket).await?;
            closed_tx
                .send(())
                .map_err(|_| "owner dropped HTTP close gate")?;
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            let body = [
                wire(&[
                    choice(json!({"content":"new-answer"})),
                    usage_record(Provider::OpenAi),
                ]),
                terminal(Provider::OpenAi),
            ]
            .concat();
            header(&mut socket, body.len()).await?;
            write(&mut socket, &body).await
        });
        let (mut worker, mut stream) = start_processor(processor(config).unwrap(), true, true)
            .await
            .unwrap();
        let mut old = Trace::default();
        let mut new = Trace::default();
        let outcome = timeout(WAIT, async {
            send(&worker, FrameData::LlmRun).await?;
            accepted_rx
                .await
                .map_err(|_| "HTTP request never started")?;
            loop {
                let frame = worker
                    .recv()
                    .await
                    .ok_or("EOF before cancellation boundary")?;
                if old.record(&frame.frame)? {
                    return Err("old response ended before interruption".into());
                }
                if match stage {
                    0 => old.starts == 1,
                    1 => !old.thought.is_empty(),
                    _ => !old.wire_ttfat.is_empty() && !old.text.is_empty(),
                } {
                    break;
                }
            }
            send(&worker, FrameData::Interruption).await?;
            loop {
                let frame = worker
                    .recv()
                    .await
                    .ok_or("EOF before interruption acknowledgement")?;
                old.record(&frame.frame)?;
                if matches!(frame.frame.data, FrameData::Interruption) {
                    break;
                }
            }
            closed_rx
                .await
                .map_err(|_| "peer did not witness cancelled HTTP request")?;
            send(&worker, FrameData::LlmRun).await?;
            response(&mut worker, &mut new, None, None).await
        })
        .await;
        let mut cleanup = Trace::default();
        let owner = finish(&mut worker, !matches!(&outcome, Ok(Ok(()))), &mut cleanup).await;
        let peer = join_peer(peer).await;
        assert!(
            matches!(&outcome, Ok(Ok(()))),
            "stage={stage}; response={outcome:?}; owner={owner:?}; peer={peer:?}"
        );
        owner.unwrap();
        peer.unwrap();
        assert_eq!(old.starts, 1);
        assert_eq!(old.ttfb.len(), usize::from(stage > 0), "{old:?}");
        assert_eq!(old.wire_ttfb, old.ttfb);
        assert_eq!(old.ttfat.len(), usize::from(stage == 2), "{old:?}");
        assert_eq!(
            old.wire_ttfat,
            old.ttfat
                .iter()
                .map(|metric| breakdown(metric).unwrap())
                .collect::<Vec<_>>()
        );
        assert!(old.processing.is_empty() && old.wire_processing.is_empty());
        assert!(old.usage.is_empty() && old.wire_usage.is_empty());
        assert!(
            !old.native_ended && !old.wire_ended,
            "cancellation must not synthesize a completion: {old:?}"
        );
        assert!(old.errors.is_empty());
        assert_eq!(old.thought, if stage == 1 { "old-plan" } else { "" });
        assert_eq!(old.text, if stage == 2 { "old-answer" } else { "" });
        assert_suppressed(&new);
        assert_eq!(new.text, "new-answer");
        assert_finished_accounting(&new, true);
        assert_suppressed(&cleanup);
        assert_eq!(observed_ttfat(&mut stream), old.ttfat);
    }
}

#[tokio::test]
async fn initial_only_allowance_survives_end_cancel_and_mode_switches_on_the_same_processor() {
    // Each tuple is (metrics enabled, initial only, cancel after response,
    // expected timing). These are new Pipelines sharing one ordinary processor,
    // not a restart of a closed worker or an unsupported second Start frame.
    let modes = [
        (false, true, false, false),
        (true, false, false, true),
        (true, false, true, true),
        (true, true, true, true),
        (true, false, false, false),
        (false, false, true, false),
        (true, true, false, false),
        // A distinct processor instance retains its own initial allowance.
        (true, true, false, true),
    ];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener, Provider::OpenAi);
    let shared = processor(config.clone()).unwrap();
    let independent = processor(config).unwrap();
    assert!(!Arc::ptr_eq(&shared, &independent));
    let peer = tokio::spawn(async move {
        for _ in modes {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            let body = [
                wire(&[
                    choice(json!({"content":"answer"})),
                    usage_record(Provider::OpenAi),
                ]),
                terminal(Provider::OpenAi),
            ]
            .concat();
            header(&mut socket, body.len()).await?;
            write(&mut socket, &body).await?;
        }
        Ok(())
    });
    let outcome = async {
        let mut traces = vec![];
        for (round, (metrics, initial_only, cancel, _)) in modes.into_iter().enumerate() {
            let llm = if round == modes.len() - 1 {
                independent.clone()
            } else {
                shared.clone()
            };
            let (mut worker, mut stream) = start_processor(llm, metrics, initial_only).await?;
            let mut trace = Trace::default();
            let response_outcome = timeout(WAIT, async {
                send(&worker, FrameData::LlmRun).await?;
                response(&mut worker, &mut trace, None, None).await
            })
            .await;
            let mut cleanup = Trace::default();
            let owner = finish(
                &mut worker,
                cancel || !matches!(&response_outcome, Ok(Ok(()))),
                &mut cleanup,
            )
            .await;
            if !matches!(&response_outcome, Ok(Ok(()))) {
                return Err(format!(
                    "round={round}; response={response_outcome:?}; owner={owner:?}"
                ));
            }
            owner?;
            if !cleanup.ttfb.is_empty()
                || !cleanup.ttfat.is_empty()
                || !cleanup.wire_ttfb.is_empty()
                || !cleanup.wire_ttfat.is_empty()
            {
                return Err(format!("delayed timing after terminal: {cleanup:?}"));
            }
            if observed_ttfat(&mut stream) != trace.ttfat {
                return Err("observer lost or duplicated initial-only timing".into());
            }
            traces.push(trace);
        }
        Ok::<_, String>(traces)
    }
    .await;
    let peer = join_peer(peer).await;
    assert!(outcome.is_ok(), "responses={outcome:?}; peer={peer:?}");
    peer.unwrap();
    for (trace, (metrics, _, _, reports)) in outcome.unwrap().into_iter().zip(modes) {
        if reports {
            trace.assert_answer();
        } else {
            assert_suppressed(&trace);
        }
        assert_eq!(trace.text, "answer");
        assert_finished_accounting(&trace, metrics);
    }
}
