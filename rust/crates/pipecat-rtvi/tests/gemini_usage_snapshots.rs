//! Actual Gemini HTTP/SSE -> LlmProcessor -> RTVI tap. The shared peer has no
//! tests, so this target does not duplicate the services decoder/HTTP matrix.
#[path = "../../pipecat-services/tests/support/gemini_usage_peer.rs"]
mod peer;
use peer::*;
use pipecat_core::*;
use pipecat_rtvi::RtviObserver;
use pipecat_services::{FinishReason, LlmProcessor, Usage};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::time::timeout;

fn cases() -> Vec<Case> {
    vec![
        complete(
            "null retains full snapshot",
            vec![usage(full()), usage(Value::Null)],
            Some(full_snapshot()),
        ),
        complete(
            "no snapshot invented",
            vec![json!({}), usage(Value::Null)],
            None,
        ),
        complete(
            "empty object resets optional details",
            vec![usage(full()), usage(json!({}))],
            Some(Usage::default()),
        ),
        complete(
            "optional zeros and provider total retained",
            vec![
                usage(full()),
                usage(json!({
            "promptTokenCount":2,"candidatesTokenCount":3,"thoughtsTokenCount":0,
            "cachedContentTokenCount":0,"totalTokenCount":0})),
                usage(Value::Null),
            ],
            Some(snapshot(2, 3, 0, Some(0), Some(0))),
        ),
        Case {
            label: "sparse post-finish replacement".into(),
            records: vec![
                usage(full()),
                finish(),
                usage(json!({"totalTokenCount":999})),
                usage(Value::Null),
            ],
            usage: Some(snapshot(0, 0, 999, None, None)),
            outcome: Outcome::Success,
            reason: Some(FinishReason::Stop),
        },
        Case {
            label: "protocol error retains admitted snapshot".into(),
            records: vec![
                usage(full()),
                usage(Value::Null),
                json!({"candidates":false,"usageMetadata":{"promptTokenCount":999}}),
            ],
            usage: Some(full_snapshot()),
            outcome: Outcome::Protocol,
            reason: None,
        },
        Case {
            label: "idle timeout retains admitted snapshot".into(),
            records: vec![usage(full()), usage(Value::Null), marker()],
            usage: Some(full_snapshot()),
            outcome: Outcome::Timeout,
            reason: None,
        },
    ]
}
struct ResultTrace {
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
}
async fn pipeline_cases(
    cases: &[Case],
    timing: bool,
    usage_enabled: bool,
    details: bool,
) -> ResultTrace {
    let observer = RtviObserver::new(Default::default(), None).unwrap();
    // The untouched constructor exercises legacy serialization. Both detail
    // opt-ins must still omit unreported fields, including every audio count.
    let observer = Arc::new(if details {
        observer
            .with_llm_usage_details(true)
            .with_llm_audio_usage_details(true)
    } else {
        observer
    });
    let (peer, client) = Peer::open(cases).await.unwrap();
    let created = Pipeline::new(vec![
        Arc::new(LlmProcessor::new(client, request())),
        Arc::new(observer.tap("gemini-usage-wire")),
    ])
    .start(PipelineConfig {
        startup_timeout: WAIT,
        shutdown_timeout: Duration::from_secs(3),
        start: StartFrame {
            enable_metrics: timing,
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
            panic!("start={error}; peer={peer:?}");
        }
    };
    let mut frames = vec![];
    let observed = timeout(WAIT, async {
        while !frames
            .iter()
            .any(|frame: &DirectedFrame| frame.frame.data.is_start())
        {
            frames.push(worker.recv().await.ok_or("closed before Start")?);
        }
        for completed in 1..=cases.len() {
            worker
                .sender()
                .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
                .await
                .map_err(|e| e.to_string())?;
            while frames
                .iter()
                .filter(|frame| matches!(frame.frame.data, FrameData::LlmResponseEnd))
                .count()
                < completed
            {
                frames.push(worker.recv().await.ok_or("closed before LlmResponseEnd")?);
            }
        }
        Ok::<_, String>(())
    })
    .await;
    let normal = matches!(observed, Ok(Ok(())));
    let admission = timeout(WAIT, async {
        if normal {
            worker.end().await
        } else {
            worker.cancel("Gemini usage observation failure").await
        }
    })
    .await;
    if !matches!(admission, Ok(Ok(()))) {
        let _ = timeout(WAIT, worker.cancel("Gemini usage terminal deadline")).await;
    }
    let drained = timeout(WAIT, async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
    })
    .await;
    if drained.is_err() {
        let _ = timeout(WAIT, worker.cancel("Gemini usage drain deadline")).await;
    }
    let mut join_timed_out = false;
    let joined = match timeout(WAIT, worker.join()).await {
        Ok(result) => result,
        Err(_) => {
            join_timed_out = true;
            let _ = timeout(WAIT, worker.cancel("Gemini usage owner join deadline")).await;
            // Join is still required after a failed deadline. Never count an
            // abort request or elapsed timeout as successful owner completion.
            worker.join().await
        }
    };
    let socket_trace = peer.join().await;
    assert!(normal, "response={observed:?}; peer={socket_trace:?}");
    assert!(matches!(admission, Ok(Ok(()))), "{admission:?}");
    assert!(drained.is_ok());
    assert!(!join_timed_out);
    check_trace(&socket_trace.unwrap(), cases);
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    ResultTrace { frames, report }
}
fn check_pipeline(
    result: &ResultTrace,
    cases: &[Case],
    timing: bool,
    usage_enabled: bool,
    details: bool,
) {
    let error_count = cases
        .iter()
        .filter(|case| case.outcome != Outcome::Success)
        .count();
    assert_eq!(
        result.report.total_errors, error_count as u64,
        "{:?}",
        result.report.errors
    );
    let errors: Vec<_> = result
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Error(error) => Some((frame.direction, error)),
            _ => None,
        })
        .collect();
    assert_eq!(errors.len(), error_count);
    for (direction, error) in errors {
        assert_eq!(direction, Direction::Upstream);
        assert!(!error.fatal);
    }
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
            case.outcome != Outcome::Success,
            "{}",
            case.label
        );
        assert_eq!(
            end.frame.metadata.get("finish_reason"),
            case.reason
                .as_ref()
                .map(|reason| serde_json::to_value(reason).unwrap())
                .as_ref(),
            "{}",
            case.label
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
        // The public Usage shape remains exactly its existing six fields.
        assert_eq!(
            frame.frame.metadata["llm_usage"],
            serde_json::to_value(usage).unwrap()
        );
        assert_eq!(
            frame.frame.metadata["llm_usage"].as_object().unwrap().len(),
            6
        );
    }
    if !usage_enabled {
        assert!(result
            .frames
            .iter()
            .all(|frame| !frame.frame.metadata.contains_key("llm_usage")));
    }
    let ttfb = result
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Metrics(metrics) => Some(metrics),
            _ => None,
        })
        .flatten()
        .filter(|metric| matches!(metric, Metric::Ttfb { .. }))
        .count();
    assert_eq!(
        ttfb,
        if timing { cases.len() - 1 } else { 0 },
        "metadata-only protocol error cannot create progress"
    );
    let wire: Vec<_> = result
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::OutputTransportMessageUrgent(value)
                if value["type"] == "metrics" && value["data"]["tokens"].is_array() =>
            {
                Some(value)
            }
            _ => None,
        })
        .collect();
    let expected_wire: Vec<_> = expected.iter().map(|usage| {
        let mut tokens = json!({"prompt_tokens":usage.prompt_tokens,"completion_tokens":usage.completion_tokens,"total_tokens":usage.total_tokens});
        if details {
            for (key, count) in [("cache_read_input_tokens", usage.cache_read_input_tokens),
                ("cache_creation_input_tokens", usage.cache_creation_input_tokens), ("reasoning_tokens", usage.reasoning_tokens)] {
                if let Some(count) = count { tokens[key] = json!(count); }
            }
        }
        json!({"label":"rtvi-ai","type":"metrics","data":{"tokens":[tokens]}})
    }).collect();
    assert_eq!(wire, expected_wire.iter().collect::<Vec<_>>());
    let text: Vec<_> = result
        .frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::LlmText(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, [MARKER]);
}

#[tokio::test]
async fn real_http_pipeline_rtvi_preserves_snapshots_for_each_timing_usage_gate_and_detail_mode() {
    let cases = cases();
    // Usage metrics and timing metrics are independent gates; each is tested
    // with both the untouched legacy RTVI constructor and opt-in breakdowns.
    for timing in [false, true] {
        for usage_enabled in [false, true] {
            for details in [false, true] {
                let result = pipeline_cases(&cases, timing, usage_enabled, details).await;
                check_pipeline(&result, &cases, timing, usage_enabled, details);
            }
        }
    }
}
