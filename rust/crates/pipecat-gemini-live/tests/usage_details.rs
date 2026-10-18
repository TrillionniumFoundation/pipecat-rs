//! Local WebSocket usage details through the production Gemini processor and
//! RTVI tap. Actual outlet frames and completed owners are the witnesses; socket
//! writes, observer entry, and Ping/Pong are not processing acknowledgements.
//! These fixtures make no claim about hosted Google acceptance or model output.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use pipecat_rtvi::RtviObserver;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::{error::ProtocolError, Error as WsError, Message};

// The affected previous graph is retained once: STT routing and its nested
// regressions (66), asynchronous tools (14), and held thinking completion (13).
#[path = "async_tools.rs"]
mod async_tool_regressions;
#[path = "stt_audio_passthrough.rs"]
mod stt_audio_regressions;
#[path = "thinking.rs"]
mod thinking_regressions;

const WAIT: Duration = Duration::from_secs(5);
const REQUEST: &str = "usage-details-request";
const WITNESS: &str = "usage-details-processed-reply";
const NAME: &str = "GeminiLiveProcessor";

#[derive(Clone)]
struct Plan {
    packets: Vec<Value>,
    protocol_error: bool,
    cancel: bool,
}
impl Plan {
    fn normal(packets: Vec<Value>) -> Self {
        Self {
            packets,
            protocol_error: false,
            cancel: false,
        }
    }
}
#[derive(Debug)]
struct Trace {
    setup: Value,
    wire: Vec<Value>,
    closed: bool,
    reset: bool,
}
struct Peer(JoinHandle<Result<Vec<Trace>, String>>);
fn disconnected(error: &WsError) -> bool {
    matches!(
        error,
        WsError::ConnectionClosed
            | WsError::AlreadyClosed
            | WsError::Protocol(ProtocolError::ResetWithoutClosingHandshake)
    ) || matches!(error, WsError::Io(error) if matches!(error.kind(),
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof))
}
impl Peer {
    async fn open(plans: Vec<Plan>) -> Result<(Self, GeminiConfig), String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let mut config = GeminiConfig::new("usage-details-fixture-key", "gemini-3.8-live");
        config.endpoint = format!(
            "ws://{}/live",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        config.limits.connect_timeout = Duration::from_secs(2);
        config.limits.setup_timeout = Duration::from_secs(2);
        config.limits.write_timeout = Duration::from_secs(1);
        config.limits.finish_timeout = Duration::from_secs(1);
        config.session.tools = ["lookup", "blocking"]
            .into_iter()
            .map(|name| FunctionDeclaration {
                name: name.into(),
                description: format!("synthetic {name}"),
                parameters: json!({"type":"object","properties":{}}),
            })
            .collect();
        let peer = tokio::spawn(async move {
            let mut traces = vec![];
            for plan in plans {
                let (tcp, _) = timeout(WAIT, listener.accept())
                    .await
                    .map_err(|_| "accept deadline")?
                    .map_err(|e| e.to_string())?;
                let mut ws = timeout(WAIT, tokio_tungstenite::accept_async(tcp))
                    .await
                    .map_err(|_| "upgrade deadline")?
                    .map_err(|e| e.to_string())?;
                let first = timeout(WAIT, ws.next())
                    .await
                    .map_err(|_| "setup deadline")?
                    .ok_or("no setup")?
                    .map_err(|e| e.to_string())?;
                let setup = serde_json::from_str(first.to_text().map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
                let mut trace = Trace {
                    setup,
                    wire: vec![],
                    closed: false,
                    reset: false,
                };
                timeout(
                    WAIT,
                    ws.send(Message::Text(
                        json!({"setupComplete":{}}).to_string().into(),
                    )),
                )
                .await
                .map_err(|_| "setup reply deadline")?
                .map_err(|e| e.to_string())?;
                let mut sent = false;
                loop {
                    let incoming = timeout(WAIT, ws.next())
                        .await
                        .map_err(|_| "peer read deadline")?;
                    match incoming {
                        Some(Ok(Message::Text(text))) => {
                            let value: Value =
                                serde_json::from_str(&text).map_err(|e| e.to_string())?;
                            let request = value["realtimeInput"]["text"] == REQUEST;
                            trace.wire.push(value);
                            if request {
                                if sent {
                                    return Err("duplicate request".into());
                                }
                                sent = true;
                                for packet in &plan.packets {
                                    timeout(
                                        WAIT,
                                        ws.send(Message::Text(packet.to_string().into())),
                                    )
                                    .await
                                    .map_err(|_| "packet write deadline")?
                                    .map_err(|e| e.to_string())?;
                                }
                                if !plan.protocol_error {
                                    let witness = json!({"serverContent":{"modelTurn":{"parts":[{"text":WITNESS}]},"turnComplete":true}});
                                    timeout(
                                        WAIT,
                                        ws.send(Message::Text(witness.to_string().into())),
                                    )
                                    .await
                                    .map_err(|_| "witness write deadline")?
                                    .map_err(|e| e.to_string())?;
                                }
                            }
                        }
                        Some(Ok(Message::Ping(bytes))) => {
                            timeout(WAIT, ws.send(Message::Pong(bytes)))
                                .await
                                .map_err(|_| "pong deadline")?
                                .map_err(|e| e.to_string())?;
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) => {
                            trace.closed = true;
                            if let Err(error) = timeout(WAIT, ws.flush())
                                .await
                                .map_err(|_| "close deadline")?
                            {
                                if !matches!(error, WsError::ConnectionClosed)
                                    && !((plan.cancel || plan.protocol_error)
                                        && disconnected(&error))
                                {
                                    return Err(error.to_string());
                                }
                            }
                            break;
                        }
                        None => {
                            trace.reset = true;
                            break;
                        }
                        Some(Err(error)) if disconnected(&error) => {
                            trace.reset = true;
                            break;
                        }
                        other => return Err(format!("unexpected peer input: {other:?}")),
                    }
                }
                if !sent {
                    return Err("closed before real input request".into());
                }
                traces.push(trace);
            }
            Ok(traces)
        });
        Ok((Self(peer), config))
    }
    async fn join(mut self) -> Result<Vec<Trace>, String> {
        match timeout(WAIT, &mut self.0).await {
            Ok(result) => result.map_err(|e| e.to_string()).and_then(|result| result),
            Err(error) => {
                self.0.abort();
                let _ = self.0.await;
                Err(format!("peer deadline; aborted and joined: {error}"))
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
enum Ownership {
    #[default]
    Unchanged,
    MismatchedBase,
    TwoLlmMetrics,
}
struct OwnershipProbe(Ownership);
#[async_trait]
impl FrameProcessor for OwnershipProbe {
    fn name(&self) -> &str {
        "UsageOwnershipProbe"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::Metrics(metrics) = &mut frame.data {
            match self.0 {
                Ownership::Unchanged => {}
                Ownership::MismatchedBase => {
                    if let Some(usage) = frame.metadata.get_mut("llm_usage") {
                        usage["prompt_tokens"] = json!(999);
                    }
                }
                Ownership::TwoLlmMetrics => {
                    if let Some(metric) = metrics.first().cloned() {
                        metrics.push(metric);
                    }
                }
            }
        }
        ctx.push(frame, direction).await
    }
}
#[derive(Debug)]
struct Observed {
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
}
async fn next(worker: &mut PipelineWorker, frames: &mut Vec<DirectedFrame>) -> Result<(), String> {
    frames.push(worker.recv().await.ok_or("outlet closed before witness")?);
    Ok(())
}
async fn execute(
    plans: Vec<Plan>,
    details: bool,
    enabled: bool,
    ownership: Ownership,
) -> Vec<Observed> {
    execute_audio(plans, details, false, enabled, ownership).await
}
async fn execute_audio(
    plans: Vec<Plan>,
    details: bool,
    audio: bool,
    enabled: bool,
    ownership: Ownership,
) -> Vec<Observed> {
    let (peer, config) = Peer::open(plans.clone()).await.unwrap();
    let client = match GeminiClient::new(config)
        .and_then(|client| client.with_non_blocking_tools(vec!["lookup".into()]))
    {
        Ok(client) => client,
        Err(error) => {
            let remote = peer.join().await;
            panic!("client configuration: {error}; {remote:?}");
        }
    };
    let processor = Arc::new(GeminiLiveProcessor::new(client));
    let mut observed = vec![];
    let mut failures = vec![];
    for plan in &plans {
        let observer = match RtviObserver::new(Default::default(), None) {
            Ok(observer) => Arc::new(
                observer
                    .with_llm_usage_details(details)
                    .with_llm_audio_usage_details(audio),
            ),
            Err(error) => {
                failures.push(format!("RTVI configuration: {error}"));
                break;
            }
        };
        let created = Pipeline::new(vec![
            processor.clone(),
            Arc::new(OwnershipProbe(ownership)),
            Arc::new(observer.tap("gemini-usage-wire")),
        ])
        .start(PipelineConfig {
            startup_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(3),
            start: StartFrame {
                audio_out_sample_rate: 24_000,
                enable_usage_metrics: enabled,
                ..Default::default()
            },
            ..Default::default()
        })
        .await;
        let mut worker = match created {
            Ok(worker) => worker,
            Err(error) => {
                failures.push(format!("pipeline start: {error}"));
                break;
            }
        };
        let mut frames = vec![];
        let observation = timeout(WAIT, async {
            while !frames.iter().any(|f: &DirectedFrame| f.frame.data.is_start()) { next(&mut worker, &mut frames).await?; }
            let mut request = Frame::new(FrameData::InputText(TextFrame::new(REQUEST)));
            request.metadata.insert("context_id".into(), json!("usage-input-owner"));
            worker.sender().send(request, Direction::Downstream).await.map_err(|e| e.to_string())?;
            let mut witness = false;
            let mut completed = false;
            let expected_metrics = if enabled { plan.packets.iter().filter(|packet| {
                ["promptTokenCount", "responseTokenCount", "totalTokenCount"].iter()
                    .all(|field| packet["usageMetadata"][field].as_u64().is_some())
            }).count() } else { 0 };
            loop {
                next(&mut worker, &mut frames).await?;
                match &frames.last().ok_or("missing output")?.frame.data {
                    FrameData::Error(error) => {
                        if plan.protocol_error { return Ok(()); }
                        return Err(format!("unexpected pipeline error: {error:?}"));
                    }
                    FrameData::LlmText(text) if text.text == WITNESS => witness = true,
                    FrameData::LlmResponseEnd if witness => {
                        if plan.protocol_error { return Err("malformed packet reached its witness".into()); }
                        completed = true;
                    }
                    _ => {}
                }
                // A cancelling run must observe its Metrics and RTVI outputs
                // before cancellation can discard still-queued downstream work.
                let metric_count = frames.iter().filter(|f| matches!(f.frame.data, FrameData::Metrics(_))).count();
                let wire_count = frames.iter().filter(|f| matches!(&f.frame.data, FrameData::OutputTransportMessageUrgent(v) if v["type"] == "metrics")).count();
                if completed && (!plan.cancel || (metric_count == expected_metrics && wire_count == expected_metrics)) { return Ok(()); }
            }
        }).await.map_err(|_| "pipeline observation deadline".to_owned()).and_then(|r| r);
        let normal = observation.is_ok();
        if let Err(error) = observation {
            failures.push(error);
        }
        let cancel = plan.cancel || !normal;
        let admission = timeout(WAIT, async {
            if cancel {
                worker.cancel("usage details cancel").await
            } else {
                worker.end().await
            }
        })
        .await;
        if !matches!(admission, Ok(Ok(()))) {
            failures.push(format!("terminal admission: {admission:?}"));
            let _ = timeout(WAIT, worker.cancel("terminal admission deadline")).await;
        }
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
                failures.push("pipeline drain/join deadline".into());
                let _ = timeout(WAIT, worker.cancel("join deadline")).await;
                worker.join().await
            }
        };
        match joined {
            Ok(report) => observed.push(Observed { frames, report }),
            Err(error) => failures.push(format!("pipeline join: {error}")),
        }
    }
    // Always join the peer before reporting any assertion failure.
    let traces = peer.join().await;
    assert!(failures.is_empty(), "{failures:?}; peer: {traces:?}");
    let traces = traces.unwrap();
    assert_eq!(traces.len(), plans.len());
    assert_eq!(observed.len(), plans.len());
    for ((trace, result), plan) in traces.iter().zip(&observed).zip(&plans) {
        assert_eq!(trace.setup["setup"]["model"], "models/gemini-3.8-live");
        assert_eq!(trace.wire, vec![json!({"realtimeInput":{"text":REQUEST}})]);
        assert!(
            trace.closed || ((plan.protocol_error || plan.cancel) && trace.reset),
            "{trace:?}"
        );
        assert_eq!(
            result.report.state,
            if plan.cancel {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        assert_eq!(
            result.report.total_errors,
            u64::from(plan.protocol_error),
            "{:?}",
            result.report
        );
    }
    observed
}

fn base(prompt: u64, completion: u64, total: u64) -> Value {
    json!({"prompt_tokens":prompt,"completion_tokens":completion,"total_tokens":total})
}
fn counts(prompt: u64, completion: u64, total: u64) -> Value {
    json!({"promptTokenCount":prompt,"responseTokenCount":completion,"totalTokenCount":total})
}
fn packet(usage: Value) -> Value {
    json!({"usageMetadata":usage})
}
fn full_usage() -> Value {
    json!({"promptTokenCount":7,"responseTokenCount":5,"totalTokenCount":900,
        "cachedContentTokenCount":3,"thoughtsTokenCount":2,"toolUsePromptTokenCount":11,
        "promptTokensDetails":[{"modality":"TEXT","tokenCount":999},{"modality":"AUDIO","tokenCount":4}],
        "responseTokensDetails":[{"modality":"AUDIO","tokenCount":6}],
        "cacheTokensDetails":[{"modality":"AUDIO","tokenCount":8}]})
}
fn full_metadata() -> Value {
    json!({"prompt_tokens":7,"completion_tokens":5,"total_tokens":900,
        "cache_read_input_tokens":3,"reasoning_tokens":2,"tool_use_prompt_tokens":11,
        "input_audio_tokens":4,"output_audio_tokens":6,"cache_read_input_audio_tokens":8})
}
fn metrics(observed: &Observed) -> Vec<&Frame> {
    observed
        .frames
        .iter()
        .filter_map(|f| matches!(f.frame.data, FrameData::Metrics(_)).then_some(&f.frame))
        .collect()
}
fn wire(observed: &Observed) -> Vec<Value> {
    observed
        .frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::OutputTransportMessageUrgent(value) if value["type"] == "metrics" => {
                Some(value["data"]["tokens"].clone())
            }
            _ => None,
        })
        .collect()
}
fn assert_usage(observed: &Observed, expected: &[Value]) {
    let actual = metrics(observed);
    assert_eq!(actual.len(), expected.len(), "{actual:?}");
    for (frame, expected) in actual.iter().zip(expected) {
        if expected.as_object().unwrap().len() == 3 {
            assert!(!frame.metadata.contains_key("llm_usage"));
        } else {
            assert_eq!(frame.metadata.get("llm_usage"), Some(expected));
        }
        assert_eq!(
            frame.data,
            FrameData::Metrics(vec![Metric::LlmUsage {
                processor: NAME.into(),
                prompt_tokens: expected["prompt_tokens"].as_u64().unwrap(),
                completion_tokens: expected["completion_tokens"].as_u64().unwrap(),
                total_tokens: expected["total_tokens"].as_u64().unwrap(),
            }])
        );
        assert!(!frame.metadata.contains_key("source_frame_id"));
        assert!(!frame.metadata.contains_key("context_id"));
    }
    assert!(observed
        .frames
        .iter()
        .all(|f| matches!(f.frame.data, FrameData::Metrics(_))
            || !f.frame.metadata.contains_key("llm_usage")));
}

#[tokio::test]
async fn raw_provider_counts_and_all_six_details_reach_metrics_with_legacy_rtvi_unchanged() {
    for details in [false, true] {
        let mut bundled = packet(full_usage());
        bundled["serverContent"] = json!({"modelTurn":{"parts":[{"text":"bundled-content"}]},"generationComplete":true,"turnComplete":true});
        let observed = execute(
            vec![Plan::normal(vec![bundled])],
            details,
            true,
            Ownership::Unchanged,
        )
        .await;
        assert_usage(&observed[0], &[full_metadata()]);
        let mut expected = base(7, 5, 900);
        if details {
            expected["cache_read_input_tokens"] = json!(3);
            expected["reasoning_tokens"] = json!(2);
        }
        assert_eq!(wire(&observed[0]), vec![json!([expected])]);
        assert!(observed[0].frames.iter().any(
            |f| matches!(&f.frame.data, FrameData::LlmText(t) if t.text == "bundled-content")
        ));
    }
}

#[tokio::test]
async fn first_uppercase_audio_wins_including_none_zero_and_nonadditive_u64_maximum() {
    let mut first = counts(1, 2, 3);
    first["cachedContentTokenCount"] = json!(0);
    first["thoughtsTokenCount"] = Value::Null;
    first["toolUsePromptTokenCount"] = json!(0);
    first["promptTokensDetails"] = json!([{"modality":"audio","tokenCount":91},{"modality":"AUDIO","tokenCount":null},{"modality":"AUDIO","tokenCount":92}]);
    first["responseTokensDetails"] =
        json!([{"modality":"AUDIO","tokenCount":0},{"modality":"AUDIO","tokenCount":93}]);
    first["cacheTokensDetails"] =
        json!([{"modality":"AUDIO"},{"modality":"AUDIO","tokenCount":94}]);
    let mut second = counts(u64::MAX, u64::MAX, 0);
    for key in [
        "cachedContentTokenCount",
        "thoughtsTokenCount",
        "toolUsePromptTokenCount",
    ] {
        second[key] = json!(u64::MAX);
    }
    for key in [
        "promptTokensDetails",
        "responseTokensDetails",
        "cacheTokensDetails",
    ] {
        second[key] =
            json!([{"modality":"AUDIO","tokenCount":u64::MAX},{"modality":"AUDIO","tokenCount":1}]);
    }
    let mut third = counts(4, 5, 6);
    third["cachedContentTokenCount"] = Value::Null;
    third["thoughtsTokenCount"] = Value::Null;
    third["toolUsePromptTokenCount"] = Value::Null;
    third["promptTokensDetails"] = Value::Null;
    third["responseTokensDetails"] = json!([]);
    third["cacheTokensDetails"] =
        json!([{"modality":"TEXT","tokenCount":7},{"modality":"audio","tokenCount":8},{}]);
    let observed = execute(
        vec![Plan::normal(vec![
            packet(first),
            packet(second),
            packet(third),
        ])],
        true,
        true,
        Ownership::Unchanged,
    )
    .await;
    let mut expected_first = base(1, 2, 3);
    expected_first["cache_read_input_tokens"] = json!(0);
    expected_first["tool_use_prompt_tokens"] = json!(0);
    expected_first["output_audio_tokens"] = json!(0);
    let mut expected_second = base(u64::MAX, u64::MAX, 0);
    for key in [
        "cache_read_input_tokens",
        "reasoning_tokens",
        "tool_use_prompt_tokens",
        "input_audio_tokens",
        "output_audio_tokens",
        "cache_read_input_audio_tokens",
    ] {
        expected_second[key] = json!(u64::MAX);
    }
    assert_usage(
        &observed[0],
        &[expected_first, expected_second, base(4, 5, 6)],
    );
    let wires = wire(&observed[0]);
    assert_eq!(
        wires[0],
        json!([{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3,"cache_read_input_tokens":0}])
    );
    assert_eq!(
        wires[1],
        json!([{"prompt_tokens":u64::MAX,"completion_tokens":u64::MAX,"total_tokens":0,"cache_read_input_tokens":u64::MAX,"reasoning_tokens":u64::MAX}])
    );
    assert_eq!(wires[2], json!([base(4, 5, 6)]));
}

#[tokio::test]
async fn missing_base_counts_and_adjacent_packets_cannot_borrow_or_leave_details() {
    let mut packets = vec![];
    for field in ["promptTokenCount", "responseTokenCount", "totalTokenCount"] {
        for null in [false, true] {
            let mut incomplete = full_usage();
            if null {
                incomplete[field] = Value::Null;
            } else {
                incomplete.as_object_mut().unwrap().remove(field);
            }
            packets.push(packet(incomplete));
        }
    }
    packets.push(packet(counts(10, 20, 99)));
    packets.push(json!({"serverContent":{"modelTurn":{"parts":[{"text":"no-usage-here"}]}}}));
    packets.push(packet(full_usage()));
    packets.push(packet(counts(30, 40, 12)));
    let observed = execute(
        vec![Plan::normal(packets)],
        true,
        true,
        Ownership::Unchanged,
    )
    .await;
    assert_usage(
        &observed[0],
        &[base(10, 20, 99), full_metadata(), base(30, 40, 12)],
    );
    assert_eq!(
        wire(&observed[0]),
        vec![
            json!([base(10, 20, 99)]),
            json!([{"prompt_tokens":7,"completion_tokens":5,"total_tokens":900,"cache_read_input_tokens":3,"reasoning_tokens":2}]),
            json!([base(30, 40, 12)])
        ]
    );
}

#[tokio::test]
async fn tool_run_splitting_and_cancellation_do_not_shift_usage_details() {
    let mut calls = packet(full_usage());
    calls["toolCall"] = json!({"functionCalls":[{"id":"ordinary-a","name":"blocking","args":{}},{"id":"selected-a","name":"lookup","args":{}},{"id":"ordinary-b","name":"blocking","args":{}}]});
    let mut cancel = packet(counts(13, 14, 15));
    cancel["usageMetadata"]["thoughtsTokenCount"] = json!(0);
    cancel["toolCallCancellation"] = json!({"ids":["ordinary-a","selected-a","ordinary-b"]});
    let observed = execute(
        vec![Plan::normal(vec![
            calls,
            cancel,
            packet(counts(16, 17, 18)),
        ])],
        true,
        true,
        Ownership::Unchanged,
    )
    .await;
    let mut cancelled_metadata = base(13, 14, 15);
    cancelled_metadata["reasoning_tokens"] = json!(0);
    assert_usage(
        &observed[0],
        &[full_metadata(), cancelled_metadata, base(16, 17, 18)],
    );
    let cancellations: Vec<_> = observed[0]
        .frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::FunctionCallCancel(call) => Some(call.tool_call_id.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        cancellations,
        vec!["ordinary-a", "ordinary-b", "selected-a"]
    );
    assert!(observed[0]
        .frames
        .iter()
        .filter(|f| matches!(
            f.frame.data,
            FrameData::FunctionCallInProgress(_)
                | FrameData::FunctionCallCancel(_)
                | FrameData::FunctionCallsStarted(_)
        ))
        .all(|f| !f.frame.metadata.contains_key("llm_usage")));
    assert_eq!(wire(&observed[0]).len(), 3);
}

#[tokio::test]
async fn malformed_known_details_reject_the_whole_packet_before_any_text_or_usage() {
    let bad_counts = [
        json!(-1),
        json!(1.5),
        json!("1"),
        json!(true),
        serde_json::from_str::<Value>("18446744073709551616").unwrap(),
    ];
    // Full-arity sequences can deserialize as serde structs unless the wire
    // boundary explicitly insists on JSON objects.
    let mut cases = vec![json!([7, 5, 12, 0, 0, 0, [], [], []])];
    for field in [
        "promptTokenCount",
        "responseTokenCount",
        "totalTokenCount",
        "cachedContentTokenCount",
        "thoughtsTokenCount",
        "toolUsePromptTokenCount",
    ] {
        for bad in &bad_counts {
            let mut usage = full_usage();
            usage[field] = bad.clone();
            cases.push(usage);
        }
    }
    for field in [
        "promptTokensDetails",
        "responseTokensDetails",
        "cacheTokensDetails",
    ] {
        for bad in [
            json!({}),
            json!(false),
            json!([true]),
            json!([["AUDIO", 1]]),
            json!([{"modality":42,"tokenCount":1}]),
        ] {
            let mut usage = full_usage();
            usage[field] = bad;
            cases.push(usage);
        }
        for bad in &bad_counts {
            let mut usage = full_usage();
            // A selected earlier AUDIO never hides a malformed later row.
            usage[field] =
                json!([{"modality":"AUDIO","tokenCount":0},{"modality":"TEXT","tokenCount":bad}]);
            cases.push(usage);
        }
    }
    let plans = cases
        .into_iter()
        .map(|usage| {
            let mut invalid = packet(usage);
            invalid["serverContent"] =
                json!({"modelTurn":{"parts":[{"text":"must-not-leak"}]},"turnComplete":true});
            Plan {
                packets: vec![invalid],
                protocol_error: true,
                cancel: false,
            }
        })
        .collect();
    let observed = execute(plans, true, true, Ownership::Unchanged).await;
    for result in &observed {
        assert_usage(result, &[]);
        assert!(wire(result).is_empty());
        assert!(!result.frames.iter().any(|f| matches!(
            f.frame.data,
            FrameData::LlmText(_) | FrameData::LlmResponseStart | FrameData::LlmResponseEnd
        )));
        let errors: Vec<_> = result
            .frames
            .iter()
            .filter_map(|f| match &f.frame.data {
                FrameData::Error(error) => Some((f.direction, error)),
                _ => None,
            })
            .collect();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, Direction::Upstream);
        assert_eq!(errors[0].1.category, ErrorCategory::InvalidRequest);
        assert!(!errors[0].1.fatal);
    }
}

#[tokio::test]
async fn disabled_metrics_and_reused_cancelled_processor_do_not_leak_sidecars() {
    let mut cancelled = Plan::normal(vec![packet(full_usage())]);
    cancelled.cancel = true;
    let observed = execute(
        vec![cancelled, Plan::normal(vec![packet(counts(2, 3, 42))])],
        true,
        true,
        Ownership::Unchanged,
    )
    .await;
    assert_usage(&observed[0], &[full_metadata()]);
    assert_usage(&observed[1], &[base(2, 3, 42)]);
    assert_eq!(wire(&observed[1]), vec![json!([base(2, 3, 42)])]);
    let disabled = execute(
        vec![Plan::normal(vec![
            packet(full_usage()),
            packet(counts(9, 8, 7)),
        ])],
        true,
        false,
        Ownership::Unchanged,
    )
    .await;
    assert_usage(&disabled[0], &[]);
    assert!(wire(&disabled[0]).is_empty());
}

#[tokio::test]
async fn real_rtvi_tap_requires_exact_metadata_base_and_single_metric_ownership() {
    for ownership in [Ownership::MismatchedBase, Ownership::TwoLlmMetrics] {
        let observed = execute(
            vec![Plan::normal(vec![packet(full_usage())])],
            true,
            true,
            ownership,
        )
        .await;
        let expected = match ownership {
            Ownership::TwoLlmMetrics => json!([base(7, 5, 900), base(7, 5, 900)]),
            _ => json!([base(7, 5, 900)]),
        };
        assert_eq!(wire(&observed[0]), vec![expected]);
        assert_eq!(metrics(&observed[0]).len(), 1);
    }
}

#[tokio::test]
async fn public_next_event_and_complete_usage_literal_keep_the_legacy_api() {
    let plan = Plan::normal(vec![packet(full_usage()), packet(counts(2, 3, 99))]);
    let (peer, config) = Peer::open(vec![plan]).await.unwrap();
    let client = match GeminiClient::new(config) {
        Ok(client) => client,
        Err(error) => {
            let remote = peer.join().await;
            panic!("client configuration: {error}; {remote:?}");
        }
    };
    let created = client.connect().await;
    let mut session = match created {
        Ok(session) => session,
        Err(error) => {
            let remote = peer.join().await;
            panic!("client connect: {error}; {remote:?}");
        }
    };
    let sent = session
        .sender()
        .try_send(Action::RealtimeText(REQUEST.into()), Origin::default());
    let mut events: Vec<GeminiEvent> = vec![];
    let observation = timeout(WAIT, async {
        sent.map_err(|e| e.to_string())?;
        loop {
            // The explicit public return type guards against exposing the private envelope.
            let next: Option<Result<GeminiEvent, GeminiError>> = session.next_event().await;
            let event = next
                .ok_or("public event stream ended early")?
                .map_err(|e| e.to_string())?;
            let done = matches!(event.event, Event::TurnComplete);
            events.push(event);
            if done {
                return Ok::<_, String>(());
            }
        }
    })
    .await;
    let shutdown = session.shutdown().await;
    let remote = peer.join().await;
    assert!(matches!(observation, Ok(Ok(()))), "{observation:?}");
    assert!(shutdown.is_ok(), "{shutdown:?}");
    let remote = remote.unwrap();
    assert!(remote[0].closed || remote[0].reset);
    let usages: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.event {
            Event::Usage(usage) => Some(usage.clone()),
            _ => None,
        })
        .collect();
    // No struct update syntax: adding a public Usage field breaks this contract.
    let expected = vec![
        Usage {
            prompt_token_count: Some(7),
            response_token_count: Some(5),
            total_token_count: Some(900),
        },
        Usage {
            prompt_token_count: Some(2),
            response_token_count: Some(3),
            total_token_count: Some(99),
        },
    ];
    assert_eq!(usages, expected);
    assert_eq!(serde_json::to_value(&usages[0]).unwrap(), counts(7, 5, 900));
}

#[tokio::test]
async fn audio_usage_opt_in_crosses_real_gemini_socket_and_rtvi_pipeline() {
    for details in [false, true] {
        for audio in [false, true] {
            let observed = execute_audio(
                vec![Plan::normal(vec![packet(full_usage())])],
                details,
                audio,
                true,
                Ownership::Unchanged,
            )
            .await;
            assert_usage(&observed[0], &[full_metadata()]);
            let mut expected = base(7, 5, 900);
            if details {
                expected["cache_read_input_tokens"] = json!(3);
                expected["reasoning_tokens"] = json!(2);
            }
            if audio {
                expected["input_audio_tokens"] = json!(4);
                expected["output_audio_tokens"] = json!(6);
                expected["cache_read_input_audio_tokens"] = json!(8);
            }
            assert_eq!(wire(&observed[0]), vec![json!([expected])]);
        }
    }
}
#[tokio::test]
async fn audio_projection_retains_zero_max_missing_and_never_accumulates_packets() {
    let mut usage = full_usage();
    usage["promptTokensDetails"] =
        json!([{"modality":"AUDIO"},{"modality":"AUDIO","tokenCount":42}]);
    usage["responseTokensDetails"] = json!([{"modality":"AUDIO","tokenCount":0}]);
    usage["cacheTokensDetails"] = json!([{"modality":"AUDIO","tokenCount":u64::MAX}]);
    let observed = execute_audio(
        vec![Plan::normal(vec![packet(usage), packet(counts(1, 2, 3))])],
        false,
        true,
        true,
        Ownership::Unchanged,
    )
    .await;
    let mut expected = base(7, 5, 900);
    assert!(expected.get("input_audio_tokens").is_none());
    expected["output_audio_tokens"] = json!(0);
    expected["cache_read_input_audio_tokens"] = json!(u64::MAX);
    assert_eq!(
        wire(&observed[0]),
        vec![json!([expected]), json!([base(1, 2, 3)])]
    );
}
#[tokio::test]
async fn audio_projection_respects_real_usage_disable_and_ownership_rejection() {
    let observed = execute_audio(
        vec![Plan::normal(vec![packet(full_usage())])],
        true,
        true,
        false,
        Ownership::Unchanged,
    )
    .await;
    assert!(wire(&observed[0]).is_empty());
    assert!(metrics(&observed[0]).is_empty());
    for ownership in [Ownership::MismatchedBase, Ownership::TwoLlmMetrics] {
        let observed = execute_audio(
            vec![Plan::normal(vec![packet(full_usage())])],
            true,
            true,
            true,
            ownership,
        )
        .await;
        let expected = if matches!(ownership, Ownership::TwoLlmMetrics) {
            json!([base(7, 5, 900), base(7, 5, 900)])
        } else {
            json!([base(7, 5, 900)])
        };
        assert_eq!(wire(&observed[0]), vec![expected]);
    }
}
