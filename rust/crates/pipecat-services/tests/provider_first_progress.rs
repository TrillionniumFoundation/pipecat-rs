//! First accepted model event, witnessed through HTTP -> LlmProcessor -> Metric.
//! The peer cannot send later content until the actual metric output opens its gate.
use pipecat_core::{Direction, Frame, FrameData, Metric, Pipeline, PipelineConfig, PipelineWorker};
use pipecat_services::*;
use serde_json::{json, Value};
use std::{fmt::Write as _, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(5);
type TestResult<T> = Result<T, String>;
fn wire(records: &[Value], provider: Provider, done: bool) -> Vec<u8> {
    let mut body = String::new();
    for value in records {
        write!(body, "data: {value}\n\n").unwrap();
    }
    if done {
        body.push_str(match provider {
            Provider::OpenAi => "data: [DONE]\n\n",
            Provider::Anthropic => "data: {\"type\":\"message_stop\"}\n\n",
            Provider::Gemini => unreachable!(),
        });
    }
    body.into_bytes()
}
fn choice(delta: Value) -> Value {
    json!({"choices":[{"index":0,"delta":delta}]})
}
fn block(value: Value) -> Value {
    json!({"type":"content_block_start","index":0,"content_block":value})
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
async fn start(config: ClientConfig, metrics: bool) -> TestResult<PipelineWorker> {
    let request = CompletionRequest::new(
        "synthetic-first-event",
        Context::new(vec![Message::text(Role::User, "hello")]),
    );
    Pipeline::new(vec![Arc::new(LlmProcessor::new(
        LlmClient::new(config).map_err(|e| e.to_string())?,
        request,
    ))])
    .start(PipelineConfig {
        start: pipecat_core::StartFrame {
            enable_metrics: metrics,
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
    errors: usize,
    order: Vec<&'static str>,
}
impl Trace {
    fn record(&mut self, data: &FrameData) -> TestResult<bool> {
        match data {
            FrameData::Metrics(metrics) => {
                for metric in metrics {
                    if let Metric::Ttfb { seconds, .. } = metric {
                        if !seconds.is_finite() || *seconds < 0.0 {
                            return Err(format!("invalid TTFB {seconds}"));
                        }
                        self.ttfb += 1;
                        self.order.push("ttfb");
                    }
                }
            }
            FrameData::LlmText(text) => {
                self.text.push_str(&text.text);
                self.order.push("text");
            }
            FrameData::LlmThoughtStart => self.order.push("thought-start"),
            FrameData::LlmThoughtText(_) => self.order.push("thought"),
            FrameData::Error(_) => self.errors += 1,
            FrameData::LlmResponseEnd => return Ok(true),
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
        let end = trace.record(&frame.frame.data)?;
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
// Cleanup errors do not bypass owner join. A timeout is a failure, never a
// substitute for evidence that the owner actually terminated.
async fn finish(worker: &mut PipelineWorker, cancel: bool, trace: &mut Trace) -> TestResult<()> {
    let control = if cancel {
        worker.cancel("fixture cleanup").await
    } else {
        worker.end().await
    };
    let mut errors = vec![];
    if let Err(error) = control {
        errors.push(error.to_string());
    }
    let drained = timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            if let Err(error) = trace.record(&frame.frame.data) {
                errors.push(error);
            }
        }
    })
    .await;
    if drained.is_err() || !errors.is_empty() {
        if let Err(error) = worker.cancel("fixture drain failure").await {
            errors.push(error.to_string());
        }
    }
    if drained.is_err() {
        errors.push("owner drain timeout".into());
    }
    let joined = timeout(WAIT, worker.join()).await;
    match joined {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => errors.push(error.to_string()),
        Err(_) => {
            errors.push("owner first join timeout".into());
            let _ = worker.cancel("fixture join timeout").await;
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
    provider: Provider,
    prefix: Vec<Value>,
    tail: Vec<Value>,
    gated: bool,
    metrics: bool,
    repeats: usize,
) -> Vec<Trace> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(provider, "synthetic-key");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    config.request_timeout = Duration::from_secs(15);
    config.idle_timeout = Duration::from_secs(10);
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
            let first = wire(&prefix, provider, false);
            let rest = wire(&tail, provider, true);
            let header = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", first.len()+rest.len());
            write(&mut socket, header.as_bytes()).await?;
            if gated {
                write(&mut socket, &first).await?;
                if gate.await.is_err() {
                    return closed(&mut socket).await;
                }
                write(&mut socket, &rest).await?;
            } else {
                write(&mut socket, &[first, rest].concat()).await?;
            }
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
fn tail(provider: Provider) -> Vec<Value> {
    match provider {
        Provider::OpenAi => vec![choice(json!({})), choice(json!({"content":"tail"}))],
        Provider::Anthropic => vec![
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"tail"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":""}}),
            json!({"type":"content_block_stop","index":1}),
        ],
        Provider::Gemini => unreachable!(),
    }
}
#[tokio::test]
async fn first_model_event_unlocks_later_content_and_each_response_reports_once() {
    for first in [
        choice(json!({"role":"assistant"})),
        choice(json!({})),
        choice(json!({"content":""})),
        choice(json!({"content":"lead"})),
        choice(json!({"reasoning_content":"plan"})),
        choice(
            json!({"tool_calls":[{"index":0,"id":"a","function":{"name":"f","arguments":"{}"}}]}),
        ),
    ] {
        for trace in fixture(
            Provider::OpenAi,
            vec![first],
            tail(Provider::OpenAi),
            true,
            true,
            2,
        )
        .await
        {
            assert_eq!(trace.ttfb, 1, "{trace:?}");
            assert_eq!(trace.order.first(), Some(&"ttfb"));
            assert!(trace.text.ends_with("tail"));
            assert_eq!(trace.errors, 0);
        }
    }
    for first in [
        block(json!({"type":"text","text":""})),
        block(json!({"type":"text","text":"lead"})),
        block(json!({"type":"thinking","thinking":"","signature":"signed"})),
        block(json!({"type":"tool_use","id":"a","name":"f","input":{}})),
        block(json!({"type":"redacted_thinking","data":"opaque"})),
    ] {
        for trace in fixture(
            Provider::Anthropic,
            vec![first],
            tail(Provider::Anthropic),
            true,
            true,
            2,
        )
        .await
        {
            assert_eq!(trace.ttfb, 1, "{trace:?}");
            assert_eq!(trace.order.first(), Some(&"ttfb"));
            assert!(trace.text.ends_with("tail"));
            assert_eq!(trace.errors, 0);
        }
    }
}
#[tokio::test]
async fn metadata_and_rejected_first_events_have_no_metric_at_terminal_barrier() {
    for (provider, records, failed) in [
        (
            Provider::OpenAi,
            vec![
                json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":0,"total_tokens":1}}),
                json!({"choices":[]}),
            ],
            false,
        ),
        (
            Provider::Anthropic,
            vec![
                json!({"type":"ping"}),
                json!({"type":"message_start","message":{"usage":{"input_tokens":1,"output_tokens":0}}}),
                json!({"type":"message_delta","usage":{"output_tokens":0}}),
            ],
            false,
        ),
        (
            Provider::OpenAi,
            vec![
                json!({"choices":[{"delta":{"content":"must-not-escape"}},{"index":1,"delta":{}}]}),
            ],
            true,
        ),
        (
            Provider::OpenAi,
            vec![json!({"error":{"type":"synthetic","message":"rejected"}})],
            true,
        ),
        (
            Provider::Anthropic,
            vec![block(
                json!({"type":"thinking","thinking":"must-not-escape","signature":42}),
            )],
            true,
        ),
        (
            Provider::Anthropic,
            vec![
                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"bad"}}),
            ],
            true,
        ),
        (
            Provider::Anthropic,
            vec![json!({"type":"error","error":{"type":"synthetic","message":"rejected"}})],
            true,
        ),
    ] {
        let traces = fixture(provider, records, vec![], false, true, 1).await;
        assert_eq!(traces[0].ttfb, 0, "{:?}", traces[0]);
        assert!(traces[0].text.is_empty());
        assert_eq!(traces[0].errors > 0, failed);
    }
}
#[tokio::test]
async fn disabled_metrics_preserve_content_and_prefix_errors_do_not_repeat_metric() {
    for provider in [Provider::OpenAi, Provider::Anthropic] {
        let first = match provider {
            Provider::OpenAi => choice(json!({})),
            _ => block(json!({"type":"text","text":""})),
        };
        let traces = fixture(
            provider,
            vec![first.clone()],
            tail(provider),
            false,
            false,
            1,
        )
        .await;
        assert_eq!(traces[0].ttfb, 0);
        assert_eq!(traces[0].text, "tail");
        assert_eq!(traces[0].errors, 0);
        let error = match provider {
            Provider::OpenAi => json!({"error":{"message":"after progress"}}),
            _ => json!({"type":"error","error":{"type":"synthetic","message":"after progress"}}),
        };
        let traces = fixture(provider, vec![first], vec![error], true, true, 1).await;
        assert_eq!(traces[0].ttfb, 1);
        assert!(traces[0].errors > 0);
    }
}
#[tokio::test]
async fn cancellation_before_and_after_observed_progress_joins_owner_and_peer() {
    for provider in [Provider::OpenAi, Provider::Anthropic] {
        for after in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut config = ClientConfig::new(provider, "synthetic-key");
            config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
            let (ready_tx, ready_rx) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
                read_request(&mut socket).await?;
                write(&mut socket, b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 1000000\r\nConnection: close\r\n\r\n").await?;
                if after {
                    let first = match provider {
                        Provider::OpenAi => choice(json!({})),
                        _ => block(json!({"type":"text","text":""})),
                    };
                    write(&mut socket, &wire(&[first], provider, false)).await?;
                }
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
                        if trace.record(&frame.frame.data)? {
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
        }
    }
}
