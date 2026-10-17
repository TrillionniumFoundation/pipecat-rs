//! Local HTTP/SSE witnesses for Anthropic cumulative usage replacement.
//! Raw streams prove terminal snapshots; the real pipeline proves Metrics and
//! existing RTVI projections. No remote provider acceptance is asserted.
use pipecat_core::*;
use pipecat_rtvi::RtviObserver;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, FinishReason, LlmClient, LlmProcessor, LlmStream,
    Message as ServiceMessage, Provider, Role as ServiceRole, ServiceError, StreamEvent, Usage,
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

// Shared decoder state, Anthropic signed blocks, real HTTP terminal behavior,
// and the existing RTVI whitelist. Unchanged cache request policy and turns
// history consumers are outside this graph.
#[path = "../../pipecat-services/tests/http_simulated.rs"]
mod http_regressions;
#[path = "../../pipecat-services/tests/protocol_contracts.rs"]
mod protocol_regressions;
#[path = "usage_details.rs"]
mod rtvi_usage_regressions;
#[path = "../../pipecat-services/tests/anthropic_signatures.rs"]
mod signature_regressions;

const WAIT: Duration = Duration::from_secs(5);
const MODEL: &str = "anthropic-usage-fixture";
const MARKER: &str = "usage-decoded-before-cancel";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Success,
    Protocol,
    Cancelled,
}
#[derive(Clone)]
struct Case {
    label: String,
    records: Vec<Value>,
    usage: Option<Usage>,
    outcome: Outcome,
    reason: Option<FinishReason>,
}
fn start(usage: Value) -> Value {
    json!({"type":"message_start","message":{"usage":usage}})
}
fn delta(usage: Value) -> Value {
    json!({"type":"message_delta","usage":usage})
}
fn stop_reason(reason: &str) -> Value {
    json!({"type":"message_delta","delta":{"stop_reason":reason}})
}
fn snapshot(
    prompt: u64,
    output: u64,
    total: u64,
    read: Option<u64>,
    write: Option<u64>,
    reasoning: Option<u64>,
) -> Usage {
    Usage {
        prompt_tokens: prompt,
        completion_tokens: output,
        total_tokens: total,
        cache_read_input_tokens: read,
        cache_creation_input_tokens: write,
        reasoning_tokens: reasoning,
    }
}
fn complete(label: impl Into<String>, mut records: Vec<Value>, usage: Option<Usage>) -> Case {
    records.push(stop_reason("end_turn"));
    records.push(json!({"type":"message_stop"}));
    Case {
        label: label.into(),
        records,
        usage,
        outcome: Outcome::Success,
        reason: Some(FinishReason::Stop),
    }
}
fn full() -> Value {
    json!({"input_tokens":7,"output_tokens":9,"cache_read_input_tokens":11,"cache_creation_input_tokens":13,"output_tokens_details":{"thinking_tokens":17}})
}
fn full_snapshot() -> Usage {
    snapshot(31, 9, 40, Some(11), Some(13), Some(17))
}
fn absent_case() -> Case {
    complete(
        "no snapshot",
        vec![
            json!({"type":"message_start","message":{}}),
            start(Value::Null),
            delta(json!({"unknown":123})),
            delta(Value::Null),
            delta(
                json!({"input_tokens":null,"output_tokens":null,"cache_read_input_tokens":null,"cache_creation_input_tokens":null,"output_tokens_details":null}),
            ),
        ],
        None,
    )
}
fn zero_case() -> Case {
    complete(
        "fresh explicit zeros",
        vec![
            delta(
                json!({"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"output_tokens_details":{"thinking_tokens":0}}),
            ),
            delta(
                json!({"input_tokens":null,"output_tokens":null,"cache_read_input_tokens":null,"cache_creation_input_tokens":null,"output_tokens_details":null}),
            ),
        ],
        Some(snapshot(0, 0, 0, Some(0), Some(0), Some(0))),
    )
}

fn cumulative_cases() -> Vec<Case> {
    let mut cases = vec![
        absent_case(),
        complete(
            "empty start keeps legacy default",
            vec![start(json!({}))],
            Some(Usage::default()),
        ),
        complete(
            "reasoning-only zero establishes snapshot",
            vec![delta(
                json!({"output_tokens_details":{"thinking_tokens":0}}),
            )],
            Some(snapshot(0, 0, 0, None, None, Some(0))),
        ),
    ];
    let mut records = vec![start(full())];
    cases.push(complete(
        "initial gross prompt",
        records.clone(),
        Some(full_snapshot()),
    ));
    for (label, update, expected) in [
        (
            "missing fields retain",
            json!({"unknown":42}),
            full_snapshot(),
        ),
        (
            "null fields retain",
            json!({"input_tokens":null,"output_tokens":null,"cache_read_input_tokens":null,"cache_creation_input_tokens":null,"output_tokens_details":null}),
            full_snapshot(),
        ),
        (
            "input decreases independently",
            json!({"input_tokens":4}),
            snapshot(28, 9, 37, Some(11), Some(13), Some(17)),
        ),
        (
            "output zero replaces",
            json!({"output_tokens":0}),
            snapshot(28, 0, 28, Some(11), Some(13), Some(17)),
        ),
        (
            "cache-read zero replaces",
            json!({"cache_read_input_tokens":0}),
            snapshot(17, 0, 17, Some(0), Some(13), Some(17)),
        ),
        (
            "cache-creation decreases",
            json!({"cache_creation_input_tokens":2}),
            snapshot(6, 0, 6, Some(0), Some(2), Some(17)),
        ),
        (
            "reasoning zero replaces without adding to output",
            json!({"output_tokens_details":{"thinking_tokens":0}}),
            snapshot(6, 0, 6, Some(0), Some(2), Some(0)),
        ),
        (
            "duplicate values do not accumulate",
            json!({"input_tokens":4,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":2,"output_tokens_details":{"thinking_tokens":0}}),
            snapshot(6, 0, 6, Some(0), Some(2), Some(0)),
        ),
    ] {
        records.push(delta(update));
        cases.push(complete(label, records.clone(), Some(expected)));
    }
    // Every prefix is a separate real request, so each intermediate snapshot is
    // observed despite the client's intentionally terminal-only Usage emission.
    let mut records = vec![start(
        json!({"input_tokens":8,"output_tokens":4,"cache_read_input_tokens":u64::MAX,"cache_creation_input_tokens":5,"output_tokens_details":{"thinking_tokens":u64::MAX}}),
    )];
    cases.push(complete(
        "saturated prompt and total",
        records.clone(),
        Some(snapshot(
            u64::MAX,
            4,
            u64::MAX,
            Some(u64::MAX),
            Some(5),
            Some(u64::MAX),
        )),
    ));
    for (label, update, expected) in [
        (
            "cache decrease recovers uncached input after saturation",
            json!({"cache_read_input_tokens":2}),
            snapshot(15, 4, 19, Some(2), Some(5), Some(u64::MAX)),
        ),
        (
            "input zero replaces retained raw input",
            json!({"input_tokens":0}),
            snapshot(7, 4, 11, Some(2), Some(5), Some(u64::MAX)),
        ),
        (
            "output maximum is legal",
            json!({"output_tokens":u64::MAX}),
            snapshot(7, u64::MAX, u64::MAX, Some(2), Some(5), Some(u64::MAX)),
        ),
        (
            "output decrease recovers total",
            json!({"output_tokens":1}),
            snapshot(7, 1, 8, Some(2), Some(5), Some(u64::MAX)),
        ),
        (
            "cache-creation maximum is legal",
            json!({"cache_creation_input_tokens":u64::MAX}),
            snapshot(
                u64::MAX,
                1,
                u64::MAX,
                Some(2),
                Some(u64::MAX),
                Some(u64::MAX),
            ),
        ),
        (
            "cache-creation zero recovers prompt",
            json!({"cache_creation_input_tokens":0}),
            snapshot(2, 1, 3, Some(2), Some(0), Some(u64::MAX)),
        ),
        (
            "raw input maximum is legal",
            json!({"input_tokens":u64::MAX}),
            snapshot(u64::MAX, 1, u64::MAX, Some(2), Some(0), Some(u64::MAX)),
        ),
        (
            "raw input decrease restores finite total",
            json!({"input_tokens":3}),
            snapshot(5, 1, 6, Some(2), Some(0), Some(u64::MAX)),
        ),
    ] {
        records.push(delta(update));
        cases.push(complete(label, records.clone(), Some(expected)));
    }
    // Repeated message_start uses the same replacement rules, not a reset.
    cases.push(complete("later start replaces only present fields", vec![start(full()), start(json!({"input_tokens":2,"cache_read_input_tokens":0,"output_tokens":null,"output_tokens_details":null}))], Some(snapshot(15,9,24,Some(0),Some(13),Some(17)))));
    cases.push(complete(
        "empty later start preserves snapshot",
        vec![start(full()), start(json!({}))],
        Some(full_snapshot()),
    ));
    cases.push(zero_case());
    cases
}
fn invalid_case(label: impl Into<String>, usage: Value, at_start: bool, prior: bool) -> Case {
    let mut records = if prior {
        vec![start(full()), stop_reason("end_turn")]
    } else {
        vec![]
    };
    let mut invalid = if at_start { start(usage) } else { delta(usage) };
    invalid["delta"] = json!({"stop_reason":"max_tokens"});
    records.push(invalid);
    // The HTTP decoder must terminate at the invalid record instead of exposing
    // a later valid record or silently accepting a partial update.
    records.push(json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"after-invalid-must-not-appear"}}));
    Case {
        label: label.into(),
        records,
        usage: prior.then(full_snapshot),
        outcome: Outcome::Protocol,
        reason: prior.then_some(FinishReason::Stop),
    }
}
fn invalid_cases() -> Vec<Case> {
    let mut cases = vec![];
    let bad_numbers = [
        json!(-1),
        json!(1.5),
        json!(1.0),
        json!("1"),
        json!(true),
        json!([]),
        json!({}),
        serde_json::from_str::<Value>("18446744073709551616").unwrap(),
    ];
    for at_start in [false, true] {
        for key in [
            "input_tokens",
            "output_tokens",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "thinking_tokens",
        ] {
            for bad in &bad_numbers {
                // Every invalid record also proposes other valid replacements.
                let mut update = json!({"input_tokens":101,"output_tokens":102,"cache_read_input_tokens":103,"cache_creation_input_tokens":104,"output_tokens_details":{"thinking_tokens":105}});
                if key == "thinking_tokens" {
                    update["output_tokens_details"][key] = bad.clone();
                } else {
                    update[key] = bad.clone();
                }
                cases.push(invalid_case(
                    format!("start={at_start} {key}={bad}"),
                    update,
                    at_start,
                    true,
                ));
            }
        }
        for bad in [
            json!({}),
            json!({"thinking_tokens":null}),
            json!({"unknown":4}),
            json!([]),
            json!([2]),
            json!(false),
            json!(4),
            json!("details"),
        ] {
            let mut update = json!({"input_tokens":101,"output_tokens":102,"cache_read_input_tokens":103,"cache_creation_input_tokens":104});
            update["output_tokens_details"] = bad.clone();
            cases.push(invalid_case(
                format!("start={at_start} details={bad}"),
                update,
                at_start,
                true,
            ));
        }
        for bad in [
            json!([]),
            json!([1, 2, 3, 4, 5]),
            json!(false),
            json!(4),
            json!("usage"),
        ] {
            cases.push(invalid_case(
                format!("start={at_start} usage={bad}"),
                bad,
                at_start,
                true,
            ));
        }
        cases.push(invalid_case(
            format!("start={at_start} invalid before any snapshot"),
            json!({"input_tokens":7,"output_tokens":true}),
            at_start,
            false,
        ));
    }
    cases
}
fn request() -> CompletionRequest {
    CompletionRequest::new(
        MODEL,
        Context::new(vec![ServiceMessage::text(
            ServiceRole::User,
            "synthetic usage request",
        )]),
    )
}
async fn read_request(socket: &mut TcpStream) -> Result<(String, Value), String> {
    const MAX: usize = 32 * 1024;
    let mut bytes = vec![];
    let mut chunk = [0; 2048];
    let end = loop {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed HTTP headers".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > MAX {
            return Err("HTTP request budget".into());
        }
        if let Some(pos) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).map_err(|e| e.to_string())?;
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|n| n.trim().parse::<usize>())
        })
        .ok_or("missing HTTP content length")?
        .map_err(|e| e.to_string())?;
    if length > MAX - end {
        return Err("HTTP body budget".into());
    }
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed HTTP body".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > MAX {
            return Err("HTTP body budget".into());
        }
    }
    let body = serde_json::from_slice(&bytes[end..end + length]).map_err(|e| e.to_string())?;
    Ok((headers, body))
}
#[derive(Debug, Default)]
struct Trace {
    requests: Vec<(String, Value)>,
    cancelled_connections: usize,
}
struct Peer {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<Trace, String>>,
}
impl Peer {
    async fn open(cases: &[Case]) -> Result<(Self, LlmClient), String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let mut config = ClientConfig::new(Provider::Anthropic, "synthetic-usage-key");
        config.endpoint = format!(
            "http://{}/messages",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        config.connect_timeout = Duration::from_secs(2);
        config.idle_timeout = Duration::from_secs(3);
        config.request_timeout = Duration::from_secs(4);
        let client = LlmClient::new(config).map_err(|e| e.to_string())?;
        let cases = cases.to_vec();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut trace = Trace::default();
            for case in cases {
                let accepted = tokio::select! {
                    _=&mut stopped => return Err("peer stopped before all requests".into()),
                    accepted=timeout(WAIT,listener.accept()) => accepted.map_err(|_|"HTTP accept deadline")?.map_err(|e|e.to_string())?,
                };
                let mut socket = accepted.0;
                trace.requests.push(
                    timeout(WAIT, read_request(&mut socket))
                        .await
                        .map_err(|_| "HTTP request deadline")??,
                );
                let mut data = String::new();
                for value in &case.records {
                    write!(
                        &mut data,
                        "event: {}\ndata: {value}\n\n",
                        value["type"].as_str().unwrap_or("message")
                    )
                    .map_err(|error| error.to_string())?;
                }
                let response = if case.outcome == Outcome::Cancelled {
                    format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{}\r\n",data.len(),data)
                } else {
                    format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",data.len(),data)
                };
                timeout(WAIT, socket.write_all(response.as_bytes()))
                    .await
                    .map_err(|_| "HTTP response deadline")?
                    .map_err(|e| e.to_string())?;
                if case.outcome == Outcome::Cancelled {
                    // Keep the SSE response unfinished until actual client
                    // cancellation closes the socket; a write is not a barrier.
                    let mut byte = [0];
                    let closed = tokio::select! {
                        _=&mut stopped => return Err("peer stopped before cancellation close".into()),
                        closed=timeout(WAIT,socket.read(&mut byte)) => closed.map_err(|_|"cancelled HTTP socket deadline")?,
                    };
                    match closed {
                        Ok(0) => trace.cancelled_connections += 1,
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::BrokenPipe
                                    | std::io::ErrorKind::UnexpectedEof
                            ) =>
                        {
                            trace.cancelled_connections += 1
                        }
                        other => return Err(format!("expected cancelled HTTP close: {other:?}")),
                    }
                } else {
                    socket.shutdown().await.map_err(|e| e.to_string())?;
                }
            }
            Ok(trace)
        });
        Ok((Self { stop, task }, client))
    }
    async fn join(mut self) -> Result<Trace, String> {
        // A drained stream and the peer's socket-close observation can race.
        // Normal completion waits for the peer naturally, without a stop signal.
        match timeout(WAIT, &mut self.task).await {
            Ok(result) => result.map_err(|e| e.to_string()).and_then(|r| r),
            Err(error) => {
                let _ = self.stop.send(());
                self.task.abort();
                let _ = self.task.await;
                Err(format!("HTTP peer aborted and joined: {error}"))
            }
        }
    }
}
fn check_trace(trace: &Trace, cases: &[Case]) {
    assert_eq!(trace.requests.len(), cases.len());
    assert_eq!(
        trace.cancelled_connections,
        cases
            .iter()
            .filter(|case| case.outcome == Outcome::Cancelled)
            .count()
    );
    for (headers, body) in &trace.requests {
        let headers = headers.to_ascii_lowercase();
        assert!(headers.starts_with("post /messages http/1.1\r\n"));
        assert!(headers.contains("x-api-key: synthetic-usage-key\r\n"));
        assert!(headers.contains("anthropic-version: 2023-06-01\r\n"));
        assert_eq!(body["model"], MODEL);
        assert_eq!(body["stream"], true);
        assert_eq!(
            body["messages"],
            json!([{"role":"user","content":[{"type":"text","text":"synthetic usage request"}]}])
        );
    }
}
async fn drain(
    stream: &mut LlmStream,
    case: &Case,
) -> Result<Vec<Result<StreamEvent, ServiceError>>, String> {
    let mut events = vec![];
    let mut cancelled = false;
    let observed = timeout(WAIT, async {
        while let Some(event) = stream.next().await {
            if case.outcome == Outcome::Cancelled
                && matches!(&event,Ok(StreamEvent::TextDelta(text)) if text==MARKER)
            {
                stream.cancel();
                cancelled = true;
            }
            events.push(event);
        }
    })
    .await;
    if observed.is_err() {
        stream.cancel();
        let drained = timeout(WAIT, async { while stream.next().await.is_some() {} }).await;
        return Err(format!(
            "{} stream deadline; post-cancel drain={drained:?}",
            case.label
        ));
    }
    if case.outcome == Outcome::Cancelled && !cancelled {
        return Err("cancel marker was never decoded".into());
    }
    // None is the public completion observation for the private HTTP producer.
    // LlmStream exposes no JoinHandle; local peer handles are actually joined.
    Ok(events)
}
async fn raw_cases(cases: &[Case]) -> Vec<Vec<Result<StreamEvent, ServiceError>>> {
    let (peer, client) = Peer::open(cases).await.unwrap();
    let mut results = vec![];
    let mut failure = None;
    for case in cases {
        let mut stream = match client.stream(request()) {
            Ok(stream) => stream,
            Err(error) => {
                failure = Some(error.to_string());
                break;
            }
        };
        match drain(&mut stream, case).await {
            Ok(events) => results.push(events),
            Err(error) => {
                failure = Some(error);
                break;
            }
        }
    }
    let trace = peer.join().await;
    assert!(failure.is_none(), "{failure:?}; peer={trace:?}");
    check_trace(&trace.unwrap(), cases);
    results
}
fn check_raw(case: &Case, events: &[Result<StreamEvent, ServiceError>]) {
    assert!(
        matches!(events.first(), Some(Ok(StreamEvent::Started))),
        "{}: {events:?}",
        case.label
    );
    assert_eq!(
        events.len(),
        2 + usize::from(case.usage.is_some())
            + usize::from(case.outcome != Outcome::Success)
            + 2 * usize::from(case.outcome == Outcome::Cancelled),
        "{}: {events:?}",
        case.label
    );
    let progress: Vec<_> = events
        .iter()
        .enumerate()
        .filter_map(|(i, event)| matches!(event, Ok(StreamEvent::ResponseProgress)).then_some(i))
        .collect();
    assert_eq!(
        progress.len(),
        usize::from(case.outcome == Outcome::Cancelled),
        "{}: {events:?}",
        case.label
    );
    if case.outcome == Outcome::Cancelled {
        let text_index = events
            .iter()
            .position(|event| matches!(event, Ok(StreamEvent::TextDelta(text)) if text == MARKER))
            .expect("cancel marker text");
        assert!(
            progress[0] < text_index,
            "progress must precede marker text: {events:?}"
        );
    }
    let usages: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Ok(StreamEvent::Usage(usage)) => Some(usage),
            _ => None,
        })
        .collect();
    assert_eq!(
        usages,
        case.usage.iter().collect::<Vec<_>>(),
        "{}",
        case.label
    );
    let errors: Vec<_> = events
        .iter()
        .enumerate()
        .filter_map(|(i, event)| event.as_ref().err().map(|error| (i, error)))
        .collect();
    match case.outcome {
        Outcome::Success => assert!(errors.is_empty(), "{}: {events:?}", case.label),
        Outcome::Protocol => {
            assert_eq!(errors.len(), 1, "{}: {events:?}", case.label);
            assert!(
                matches!(errors[0].1, ServiceError::Protocol(_)),
                "{}: {events:?}",
                case.label
            );
        }
        Outcome::Cancelled => {
            assert_eq!(errors.len(), 1, "{}: {events:?}", case.label);
            assert!(matches!(errors[0].1, ServiceError::Cancelled));
        }
    }
    assert!(
        matches!(events.last(),Some(Ok(StreamEvent::Finished{reason})) if reason==&case.reason),
        "{}: {events:?}",
        case.label
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(StreamEvent::Started)))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(StreamEvent::Finished { .. })))
            .count(),
        1
    );
    if let Some((error_index, _)) = errors.first() {
        assert_eq!(
            *error_index + 1,
            events.len() - 1,
            "error must immediately precede Finished"
        );
        if case.usage.is_some() {
            assert!(matches!(
                &events[*error_index - 1],
                Ok(StreamEvent::Usage(_))
            ));
        }
    }
    let texts: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Ok(StreamEvent::TextDelta(text)) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        texts,
        if case.outcome == Outcome::Cancelled {
            vec![MARKER]
        } else {
            vec![]
        },
        "{}",
        case.label
    );
}

#[tokio::test]
async fn cumulative_http_snapshots_replace_null_zero_decreases_and_recover_saturated_totals() {
    let cases = cumulative_cases();
    let results = raw_cases(&cases).await;
    for (case, events) in cases.iter().zip(&results) {
        check_raw(case, events);
    }
}
#[tokio::test]
async fn invalid_http_usage_records_preserve_the_entire_prior_snapshot_and_finish_reason() {
    let cases = invalid_cases();
    let results = raw_cases(&cases).await;
    for (case, events) in cases.iter().zip(&results) {
        check_raw(case, events);
    }
}
#[tokio::test]
async fn cancellation_after_a_decoded_marker_emits_latest_usage_once_and_client_reuse_is_fresh() {
    let case = Case {
        label: "cancel after actual decoded text".into(),
        records: vec![
            start(full()),
            delta(
                json!({"input_tokens":2,"output_tokens":0,"cache_read_input_tokens":0,"output_tokens_details":{"thinking_tokens":0}}),
            ),
            stop_reason("end_turn"),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":MARKER}}),
        ],
        usage: Some(snapshot(15, 0, 15, Some(0), Some(13), Some(0))),
        outcome: Outcome::Cancelled,
        reason: Some(FinishReason::Stop),
    };
    let cases = vec![
        case,
        absent_case(),
        zero_case(),
        complete(
            "fresh input without cache or reasoning",
            vec![delta(json!({"input_tokens":3,"output_tokens":2}))],
            Some(snapshot(3, 2, 5, None, None, None)),
        ),
    ];
    let results = raw_cases(&cases).await;
    for (case, events) in cases.iter().zip(&results) {
        check_raw(case, events);
    }
}

struct PipelineResult {
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
}
async fn pipeline_cases(cases: &[Case], details: bool, usage_enabled: bool) -> PipelineResult {
    let observer = RtviObserver::new(Default::default(), None).unwrap();
    // Exercise the actual untouched constructor for legacy output.
    let observer = Arc::new(if details {
        observer.with_llm_usage_details(true)
    } else {
        observer
    });
    let (peer, client) = Peer::open(cases).await.unwrap();
    let created = Pipeline::new(vec![
        Arc::new(LlmProcessor::new(client, request())),
        Arc::new(observer.tap("anthropic-usage-wire")),
    ])
    .start(PipelineConfig {
        startup_timeout: WAIT,
        shutdown_timeout: Duration::from_secs(3),
        start: StartFrame {
            enable_usage_metrics: usage_enabled,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    let mut worker = match created {
        Ok(worker) => worker,
        Err(error) => {
            let peer = peer.join().await;
            panic!("pipeline start: {error}; peer={peer:?}");
        }
    };
    let mut frames = vec![];
    let observation = timeout(WAIT, async {
        while !frames
            .iter()
            .any(|f: &DirectedFrame| f.frame.data.is_start())
        {
            frames.push(worker.recv().await.ok_or("closed before pipeline Start")?);
        }
        for completed in 1..=cases.len() {
            worker
                .sender()
                .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
                .await
                .map_err(|e| e.to_string())?;
            while frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
                .count()
                < completed
            {
                frames.push(
                    worker
                        .recv()
                        .await
                        .ok_or("closed before LLM response end")?,
                );
            }
        }
        Ok::<_, String>(())
    })
    .await;
    let normal = matches!(observation, Ok(Ok(())));
    let admission = timeout(WAIT, async {
        if normal {
            worker.end().await
        } else {
            worker.cancel("usage observation failure").await
        }
    })
    .await;
    if !matches!(admission, Ok(Ok(()))) {
        let _ = timeout(WAIT, worker.cancel("usage terminal deadline")).await;
    }
    let mut join_timeout = false;
    let joined = match timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        worker.join().await
    })
    .await
    {
        Ok(result) => result,
        Err(_) => {
            join_timeout = true;
            let _ = timeout(WAIT, worker.cancel("usage join deadline")).await;
            worker.join().await
        }
    };
    let trace = peer.join().await;
    assert!(normal, "{observation:?}; peer={trace:?}");
    assert!(matches!(admission, Ok(Ok(()))), "{admission:?}");
    assert!(!join_timeout);
    check_trace(&trace.unwrap(), cases);
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    PipelineResult { frames, report }
}
fn check_pipeline(result: &PipelineResult, cases: &[Case], details: bool, usage_enabled: bool) {
    let errors = cases
        .iter()
        .filter(|case| case.outcome == Outcome::Protocol)
        .count();
    assert_eq!(
        result.report.total_errors, errors as u64,
        "{:?}",
        result.report.errors
    );
    let error_frames: Vec<_> = result
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Error(error) => Some((frame.direction, error)),
            _ => None,
        })
        .collect();
    assert_eq!(error_frames.len(), errors);
    for (direction, error) in error_frames {
        assert_eq!(direction, Direction::Upstream);
        assert_eq!(error.category, ErrorCategory::Application);
        assert!(!error.fatal);
    }
    assert!(
        !result
            .frames
            .iter()
            .any(|frame| matches!(frame.frame.data, FrameData::LlmText(_))),
        "invalid record leaked later text"
    );
    let ends: Vec<_> = result
        .frames
        .iter()
        .filter(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
        .collect();
    assert_eq!(ends.len(), cases.len());
    for (end, case) in ends.iter().zip(cases) {
        assert_eq!(end.direction, Direction::Downstream);
        assert_eq!(
            end.frame.metadata["failed"],
            case.outcome != Outcome::Success
        );
        assert_eq!(
            end.frame.metadata["finish_reason"],
            serde_json::to_value(&case.reason).unwrap()
        );
    }
    let expected: Vec<_> = cases
        .iter()
        .filter_map(|case| {
            if usage_enabled {
                case.usage.as_ref()
            } else {
                None
            }
        })
        .collect();
    let metrics: Vec<_> = result
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Metrics(metrics)
                if metrics
                    .iter()
                    .any(|metric| matches!(metric, Metric::LlmUsage { .. })) =>
            {
                Some((frame, metrics))
            }
            _ => None,
        })
        .collect();
    assert_eq!(metrics.len(), expected.len());
    for ((frame, metrics), usage) in metrics.iter().zip(&expected) {
        assert_eq!(frame.direction, Direction::Downstream);
        assert_eq!(
            metrics.as_slice(),
            &[Metric::LlmUsage {
                processor: "LlmService".into(),
                prompt_tokens: usage.prompt_tokens,
                completion_tokens: usage.completion_tokens,
                total_tokens: usage.total_tokens
            }]
        );
        assert_eq!(
            frame.frame.metadata["llm_usage"],
            serde_json::to_value(usage).unwrap()
        );
    }
    let wire: Vec<_> = result
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::OutputTransportMessageUrgent(value) if value["type"] == "metrics" => {
                Some(value)
            }
            _ => None,
        })
        .collect();
    let expected_wire:Vec<_>=expected.iter().map(|usage|{
        let mut tokens=json!({"prompt_tokens":usage.prompt_tokens,"completion_tokens":usage.completion_tokens,"total_tokens":usage.total_tokens});
        if details {
            for (key,count) in [("cache_read_input_tokens",usage.cache_read_input_tokens),("cache_creation_input_tokens",usage.cache_creation_input_tokens),("reasoning_tokens",usage.reasoning_tokens)] {
                if let Some(count)=count {tokens[key]=json!(count);}
            }
        }
        json!({"label":"rtvi-ai","type":"metrics","data":{"tokens":[tokens]}})
    }).collect();
    assert_eq!(wire, expected_wire.iter().collect::<Vec<_>>());
}
#[tokio::test]
async fn real_http_metrics_and_default_or_opt_in_rtvi_share_exact_latest_gross_counts() {
    let good = complete(
        "pipeline latest snapshot",
        vec![
            start(full()),
            delta(
                json!({"input_tokens":2,"output_tokens":3,"cache_read_input_tokens":0,"cache_creation_input_tokens":4,"output_tokens_details":{"thinking_tokens":5}}),
            ),
            delta(json!({"input_tokens":null,"output_tokens_details":null})),
        ],
        Some(snapshot(6, 3, 9, Some(0), Some(4), Some(5))),
    );
    let invalid = invalid_case(
        "pipeline invalid details preserve prior snapshot",
        json!({"input_tokens":101,"output_tokens":102,"cache_read_input_tokens":103,"cache_creation_input_tokens":104,"output_tokens_details":{}}),
        false,
        true,
    );
    let cases = vec![good, absent_case(), zero_case(), invalid];
    for (details, usage_enabled) in [(false, true), (true, true), (true, false)] {
        let result = pipeline_cases(&cases, details, usage_enabled).await;
        check_pipeline(&result, &cases, details, usage_enabled);
    }
}
