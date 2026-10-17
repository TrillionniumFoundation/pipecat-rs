//! Ordinary Gemini HTTP -> LlmProcessor -> actual TTFB output contracts.
//! Later response bytes remain withheld until the metric opens the peer gate.
use pipecat_core::{
    Direction, Frame, FrameData, FunctionCall, LlmContinuationBlock, Metric, Pipeline,
    PipelineConfig, PipelineWorker,
};
use pipecat_services::*;
use pipecat_services::{
    protocol::ProtocolDecoder,
    sse::{SseDecoder, SseEvent},
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

const WAIT: Duration = Duration::from_secs(10);
type TestResult<T> = Result<T, String>;

fn wire(records: &[Value]) -> Vec<u8> {
    let mut body = String::new();
    for value in records {
        write!(body, "data: {value}\n\n").unwrap();
    }
    // Gemini has no [DONE] marker: a finishReason followed by clean EOF is required.
    body.into_bytes()
}

fn candidate(parts: Value) -> Value {
    json!({"candidates":[{"index":0,"content":{"parts":parts}}]})
}

fn decode(decoder: &mut ProtocolDecoder, value: &Value) -> Result<Vec<StreamEvent>, ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: value.to_string(),
    })
}

fn usage() -> Value {
    json!({"candidates":[],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"thoughtsTokenCount":1,"totalTokenCount":6}})
}

fn tail() -> Vec<Value> {
    vec![
        candidate(json!([])),
        candidate(json!([{"text":"tail"}])),
        json!({"candidates":[{"finishReason":"STOP"}]}),
        usage(),
    ]
}

async fn read_request(socket: &mut TcpStream) -> TestResult<()> {
    let mut bytes = vec![];
    let mut part = [0; 4096];
    let end = loop {
        let n = socket.read(&mut part).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("EOF before request headers".into());
        }
        bytes.extend_from_slice(&part[..n]);
        if bytes.len() > 1024 * 1024 {
            return Err("oversize request".into());
        }
        if let Some(i) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).map_err(|e| e.to_string())?;
    if !headers.starts_with(
        "POST /v1beta/models/gemini-2.5-fixture:streamGenerateContent?alt=sse HTTP/1.1\r\n",
    ) {
        return Err("unexpected Gemini request target".into());
    }
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse::<usize>())
        })
        .ok_or("missing request length")?
        .map_err(|e| e.to_string())?;
    while bytes.len() < end + length {
        let n = socket.read(&mut part).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("EOF before request body".into());
        }
        bytes.extend_from_slice(&part[..n]);
    }
    let body: Value =
        serde_json::from_slice(&bytes[end..end + length]).map_err(|e| e.to_string())?;
    if body["contents"][0]["parts"][0]["text"] != "hello" {
        return Err("unexpected Gemini request content".into());
    }
    Ok(())
}

async fn write(socket: &mut TcpStream, bytes: &[u8]) -> TestResult<()> {
    socket.write_all(bytes).await.map_err(|e| e.to_string())
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
            let result = peer.await;
            Err(format!("peer timeout; aborted and joined: {result:?}"))
        }
    }
}

fn config(listener: &TcpListener) -> ClientConfig {
    let mut config = ClientConfig::new(Provider::Gemini, "synthetic-key");
    config.endpoint = format!(
        "http://{}/v1beta/models/{{model}}:streamGenerateContent?alt=sse",
        listener.local_addr().unwrap()
    );
    config.request_timeout = Duration::from_secs(30);
    config.idle_timeout = Duration::from_secs(20);
    // The aggregate-tool rejection fixture must reach the provider decoder,
    // rather than being rejected first by the lower SSE size limit.
    config.max_event_bytes = 8 * 1024 * 1024;
    config
}

async fn start(config: ClientConfig, metrics: bool) -> TestResult<PipelineWorker> {
    let request = CompletionRequest::new(
        "gemini-2.5-fixture",
        Context::new(vec![Message::text(Role::User, "hello")]),
    );
    Pipeline::new(vec![Arc::new(LlmProcessor::new(
        LlmClient::new(config).map_err(|e| e.to_string())?,
        request,
    ))])
    .start(PipelineConfig {
        start: pipecat_core::StartFrame {
            enable_metrics: metrics,
            enable_usage_metrics: true,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .map_err(|e| e.to_string())
}

async fn run(worker: &PipelineWorker) -> TestResult<()> {
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .map_err(|e| e.to_string())
}

#[derive(Debug, Default)]
struct Trace {
    ttfb: usize,
    text: String,
    thought: String,
    calls: Vec<FunctionCall>,
    history: Vec<LlmContinuationBlock>,
    checkpoints: usize,
    usage: Vec<(u64, u64, u64)>,
    errors: usize,
    error_messages: Vec<String>,
    order: Vec<&'static str>,
}

impl Trace {
    fn record(&mut self, frame: &Frame) -> TestResult<bool> {
        match &frame.data {
            FrameData::Metrics(metrics) => {
                for metric in metrics {
                    match metric {
                        Metric::Ttfb { seconds, .. } => {
                            if !seconds.is_finite() || *seconds < 0.0 {
                                return Err(format!("invalid TTFB {seconds}"));
                            }
                            self.ttfb += 1;
                            self.order.push("ttfb");
                        }
                        Metric::LlmUsage {
                            prompt_tokens,
                            completion_tokens,
                            total_tokens,
                            ..
                        } => self
                            .usage
                            .push((*prompt_tokens, *completion_tokens, *total_tokens)),
                        _ => {}
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
            FrameData::LlmContinuationCheckpoint => {
                self.checkpoints += 1;
                self.order.push("checkpoint");
                frame.native_llm_continuation().map_err(|e| e.to_string())?;
            }
            FrameData::FunctionCallsStarted(calls) => {
                self.calls.extend(calls.iter().cloned());
                self.order.push("tool");
                if let Some(history) = frame.native_llm_continuation().map_err(|e| e.to_string())? {
                    self.history.extend_from_slice(history.blocks());
                }
            }
            FrameData::Error(error) => {
                self.errors += 1;
                self.error_messages.push(error.error.clone());
            }
            FrameData::LlmResponseEnd => {
                if let Some(history) = frame.native_llm_continuation().map_err(|e| e.to_string())? {
                    self.history.extend_from_slice(history.blocks());
                }
                return Ok(true);
            }
            _ => {}
        }
        Ok(false)
    }
}

async fn response(
    worker: &mut PipelineWorker,
    trace: &mut Trace,
    mut gate: Option<oneshot::Sender<()>>,
) -> TestResult<()> {
    loop {
        let frame = worker
            .recv()
            .await
            .ok_or("pipeline EOF before response end")?;
        let end = trace.record(&frame.frame)?;
        if trace.ttfb > 0 {
            if let Some(gate) = gate.take() {
                gate.send(()).map_err(|_| "peer dropped metric gate")?;
            }
        }
        if end {
            if gate.is_some() {
                return Err("response ended without first metric".into());
            }
            return Ok(());
        }
    }
}

// As in the prior provider fixture, cleanup failure never bypasses owner join.
// Timeouts fail the test; they do not count as evidence of termination.
async fn finish(worker: &mut PipelineWorker, cancel: bool, trace: &mut Trace) -> TestResult<()> {
    let control = if cancel {
        worker.cancel("Gemini fixture cleanup").await
    } else {
        worker.end().await
    };
    let mut errors = vec![];
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
        if let Err(error) = worker.cancel("Gemini fixture drain failure").await {
            errors.push(error.to_string());
        }
    }
    if drained.is_err() {
        errors.push("owner drain timeout".into());
    }
    match timeout(WAIT, worker.join()).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => errors.push(error.to_string()),
        Err(_) => {
            errors.push("owner first join timeout".into());
            let _ = worker.cancel("Gemini fixture join timeout").await;
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

async fn fixture(
    first: Vec<u8>,
    rest: Vec<u8>,
    gated: bool,
    metrics: bool,
    repeats: usize,
) -> Vec<Trace> {
    assert!(
        !gated || !rest.is_empty(),
        "gate must withhold response bytes/EOF"
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener);
    let mut gates = vec![];
    let mut receivers = vec![];
    for _ in 0..repeats {
        let (tx, rx) = oneshot::channel();
        gates.push(tx);
        receivers.push(rx);
    }
    let peer = tokio::spawn(async move {
        for gate in receivers {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                first.len() + rest.len()
            );
            write(&mut socket, header.as_bytes()).await?;
            write(&mut socket, &first).await?;
            if gated && gate.await.is_err() {
                return closed(&mut socket).await;
            }
            write(&mut socket, &rest).await?;
        }
        Ok(())
    });
    let mut worker = match start(config, metrics).await {
        Ok(worker) => worker,
        Err(error) => {
            drop(gates);
            let peer = join_peer(peer).await;
            panic!("start={error}; peer={peer:?}");
        }
    };
    let mut results = vec![];
    let outcome = timeout(WAIT, async {
        for gate in gates {
            run(&worker).await?;
            let mut trace = Trace::default();
            response(
                &mut worker,
                &mut trace,
                if gated { Some(gate) } else { None },
            )
            .await?;
            results.push(trace);
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
    assert_eq!(
        cleanup.ttfb, 0,
        "no delayed metric after terminal response: {cleanup:?}"
    );
    results
}

#[tokio::test]
async fn first_accepted_candidate_unlocks_text_once_in_each_fresh_request() {
    let cases = [
        (json!({"candidates":[{}]}), "", "", false, false),
        (candidate(json!([])), "", "", false, false),
        (candidate(json!([{"text":""}])), "", "", false, false),
        (
            candidate(json!([{"text":"lead"}])),
            "lead",
            "",
            false,
            false,
        ),
        (
            candidate(json!([{"text":"plan","thought":true}])),
            "",
            "plan",
            false,
            false,
        ),
        (
            candidate(json!([{"text":"lead","thoughtSignature":"text-signature"}])),
            "lead",
            "",
            false,
            true,
        ),
        (
            candidate(
                json!([{"functionCall":{"id":"a","name":"echo","args":{"n":1}},"thoughtSignature":"tool-signature"}]),
            ),
            "",
            "",
            true,
            true,
        ),
    ];
    for (first, text, thought, tool, signed) in cases {
        for _ in 0..2 {
            let mut decoder = ProtocolDecoder::new(Provider::Gemini);
            for metadata in [json!({}), json!({"candidates":[]}), usage()] {
                assert!(decode(&mut decoder, &metadata).unwrap().is_empty());
            }
            let events = decode(&mut decoder, &first).unwrap();
            assert_eq!(events.first(), Some(&StreamEvent::ResponseProgress));
            assert_eq!(
                events
                    .iter()
                    .filter(|event| matches!(event, StreamEvent::ResponseProgress))
                    .count(),
                1
            );
            assert!(decode(&mut decoder, &candidate(json!([])))
                .unwrap()
                .is_empty());
            assert!(!decoder.is_done());
        }
        let prefix = wire(&[usage(), json!({"candidates":[]}), first]);
        for trace in fixture(prefix, wire(&tail()), true, true, 2).await {
            assert_eq!(trace.ttfb, 1, "{trace:?}");
            assert_eq!(trace.order.first(), Some(&"ttfb"));
            assert_eq!(trace.text, format!("{text}tail"));
            assert_eq!(trace.thought, thought);
            assert_eq!(trace.errors, 0);
            assert_eq!(trace.usage, [(3, 3, 6)]);
            assert_eq!(trace.calls.len(), usize::from(tool));
            assert_eq!(trace.checkpoints > 0, signed);
            if tool {
                assert_eq!(trace.calls[0].tool_call_id, "a");
                assert_eq!(trace.calls[0].function_name, "echo");
                assert_eq!(trace.calls[0].arguments, json!({"n":1}));
                assert!(
                    trace
                        .order
                        .iter()
                        .position(|event| *event == "text")
                        .unwrap()
                        < trace
                            .order
                            .iter()
                            .position(|event| *event == "tool")
                            .unwrap()
                );
                assert!(trace.history.iter().any(|block| matches!(block,
                    LlmContinuationBlock::GeminiToolMetadata { tool_call_id, signature: Some(signature) }
                        if tool_call_id == "a" && signature == "tool-signature")));
            } else if signed {
                assert!(trace.history.iter().any(|block| matches!(block,
                    LlmContinuationBlock::GeminiText { text, thought: false, signature: Some(signature) }
                        if text == "lead" && signature == "text-signature")));
            }
        }
    }
}

#[tokio::test]
async fn finish_only_candidate_unlocks_usage_and_eof_without_waiting_for_text() {
    for reason in ["STOP", "MAX_TOKENS", "SAFETY", "UNKNOWN_FINISH"] {
        // No content or duplicate finish marker is legal after this prefix.
        // The metric must arrive before the peer can send usage and clean EOF.
        for trace in fixture(
            wire(&[json!({"candidates":[{"finishReason":reason}]})]),
            wire(&[usage()]),
            true,
            true,
            2,
        )
        .await
        {
            assert_eq!(trace.ttfb, 1, "{trace:?}");
            assert_eq!(trace.order, ["ttfb"]);
            assert!(trace.text.is_empty());
            assert!(trace.calls.is_empty());
            assert_eq!(trace.errors, 0);
            assert_eq!(trace.usage, [(3, 3, 6)]);
        }
    }
}

#[tokio::test]
async fn disabled_metrics_preserve_text_tools_usage_and_finish_only_responses() {
    for (first, rest, expected_text, expected_tools) in [
        (candidate(json!([])), tail(), "tail", 0),
        (
            candidate(json!([{"functionCall":{"id":"a","name":"echo","args":{}}}])),
            tail(),
            "tail",
            1,
        ),
        (
            json!({"candidates":[{"finishReason":"STOP"}]}),
            vec![usage()],
            "",
            0,
        ),
    ] {
        let traces = fixture(wire(&[first]), wire(&rest), false, false, 1).await;
        let trace = &traces[0];
        assert_eq!(trace.ttfb, 0, "{trace:?}");
        assert_eq!(trace.text, expected_text);
        assert_eq!(trace.calls.len(), expected_tools);
        assert_eq!(trace.errors, 0);
        assert_eq!(trace.usage, [(3, 3, 6)]);
    }
}

#[tokio::test]
async fn metadata_and_empty_candidates_do_not_emit_metric_before_terminal_error() {
    for record in [
        json!({}),
        json!({"candidates":[]}),
        usage(),
        json!({"usageMetadata":{"promptTokenCount":3}}),
    ] {
        let traces = fixture(wire(&[record]), vec![], false, true, 1).await;
        let trace = &traces[0];
        assert_eq!(trace.ttfb, 0, "{trace:?}");
        assert!(trace.text.is_empty());
        assert!(trace.thought.is_empty());
        assert!(trace.calls.is_empty());
        // Metadata alone supplies no finishReason, so EOF fails closed.
        assert_eq!(trace.errors, 1);
    }
}

#[tokio::test]
async fn rejected_first_records_cannot_leak_text_tools_or_ttfb() {
    let large = "x".repeat(2 * 1024 * 1024);
    let aggregate = candidate(json!([
        {"functionCall":{"id":"a","name":"echo","args":{"large":large}}},
        {"functionCall":{"id":"b","name":"echo","args":{"large":large}}}
    ]));
    let records = [
        json!([]),
        json!({"candidates":{}}),
        json!({"candidates":[null]}),
        json!({"candidates":[{},{}]}),
        json!({"candidates":[{"index":1}]}),
        candidate(json!([{"text":"must-not-escape"},{"text":42}])),
        candidate(json!([{"functionCall":{"id":"a","name":"echo","args":{}}},{}])),
        json!({"candidates":[{}],"promptFeedback":{"blockReason":"SAFETY"}}),
        json!({"candidates":[{}],"error":{"status":"SYNTHETIC","message":"rejected"}}),
        aggregate,
    ];
    for first in records
        .into_iter()
        .map(|record| wire(&[record]))
        .chain([b"data: {bad-json}\n\n".to_vec()])
    {
        let aggregate = first.len() > 4 * 1024 * 1024;
        let mut sse = SseDecoder::new(8 * 1024 * 1024);
        let mut events = sse.feed(&first).unwrap();
        assert_eq!(events.len(), 1);
        let mut decoder = ProtocolDecoder::new(Provider::Gemini);
        let error = decoder.decode(events.remove(0)).unwrap_err();
        if aggregate {
            assert!(error
                .to_string()
                .contains("aggregate tool calls exceed size limit"));
        }
        // Public failure poisons the decoder. Its internal once flag is not
        // exposed; these checks prove no result escapes and poison is retained.
        assert!(matches!(decode(&mut decoder, &candidate(json!([]))),
            Err(ServiceError::Protocol(message)) if message.contains("poisoned")));
        assert!(decoder.end_of_stream().is_err());
        let mut fresh = ProtocolDecoder::new(Provider::Gemini);
        assert_eq!(
            decode(&mut fresh, &candidate(json!([]))).unwrap(),
            vec![StreamEvent::ResponseProgress]
        );
        assert!(decode(&mut fresh, &candidate(json!([])))
            .unwrap()
            .is_empty());
        let traces = fixture(first, vec![], false, true, 1).await;
        let trace = &traces[0];
        assert_eq!(trace.ttfb, 0, "{trace:?}");
        assert!(trace.text.is_empty());
        assert!(trace.thought.is_empty());
        assert!(trace.calls.is_empty());
        assert_eq!(trace.checkpoints, 0);
        assert_eq!(trace.errors, 1);
        if aggregate {
            assert!(trace.error_messages[0].contains("aggregate tool calls exceed size limit"));
        }
    }
}

#[tokio::test]
async fn later_provider_error_preserves_exactly_one_observed_metric() {
    let traces = fixture(
        wire(&[candidate(json!([]))]),
        wire(&[json!({"error":{"status":"SYNTHETIC","message":"after progress"}})]),
        true,
        true,
        2,
    )
    .await;
    for trace in traces {
        assert_eq!(trace.ttfb, 1, "{trace:?}");
        assert!(trace.text.is_empty());
        assert!(trace.calls.is_empty());
        assert_eq!(trace.errors, 1);
    }
}

#[tokio::test]
async fn cancellation_before_and_after_actual_progress_joins_owner_and_peer() {
    for after in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener);
        let (ready_tx, ready_rx) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
            read_request(&mut socket).await?;
            write(&mut socket, b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n").await?;
            let first = if after { candidate(json!([])) } else { usage() };
            write(&mut socket, &wire(&[first])).await?;
            let _ = ready_tx.send(());
            closed(&mut socket).await
        });
        let mut worker = match start(config, true).await {
            Ok(worker) => worker,
            Err(error) => {
                let peer = join_peer(peer).await;
                panic!("start={error}; peer={peer:?}");
            }
        };
        let mut trace = Trace::default();
        let outcome = timeout(WAIT, async {
            run(&worker).await?;
            ready_rx
                .await
                .map_err(|_| "peer did not establish response headers")?;
            if after {
                while trace.ttfb == 0 {
                    let frame = worker.recv().await.ok_or("EOF before metric")?;
                    if trace.record(&frame.frame)? {
                        return Err("response ended before metric".into());
                    }
                }
            }
            Ok::<_, String>(())
        })
        .await;
        let owner = finish(&mut worker, true, &mut trace).await;
        let peer = join_peer(peer).await;
        assert!(
            matches!(&outcome, Ok(Ok(()))),
            "outcome={outcome:?}; owner={owner:?}; peer={peer:?}"
        );
        owner.unwrap();
        peer.unwrap();
        assert_eq!(trace.ttfb, usize::from(after), "{trace:?}");
        assert!(trace.text.is_empty());
        assert!(trace.calls.is_empty());
    }
}
