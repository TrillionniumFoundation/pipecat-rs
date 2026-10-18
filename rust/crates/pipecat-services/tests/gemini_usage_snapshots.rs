//! Ordinary Gemini snapshot lifecycle, independent source arithmetic, and real
//! HTTP/SSE finalization. Synthetic loopback peers are not live-provider proof.
#[path = "support/gemini_usage_peer.rs"]
mod peer;
use peer::*;
use pipecat_services::{
    protocol::ProtocolDecoder, sse::SseEvent, FinishReason, LlmStream, Provider, ServiceError,
    StreamEvent, Usage,
};
use serde_json::{json, Value};
use tokio::time::timeout;

fn decode(decoder: &mut ProtocolDecoder, value: &Value) -> Result<Vec<StreamEvent>, ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: value.to_string(),
    })
}
fn source_fixture() -> Value {
    serde_json::from_str(include_str!(
        "fixtures/python_gemini_stream_usage_source.json"
    ))
    .unwrap()
}
fn source_cases() -> Vec<Case> {
    let source = source_fixture();
    // These complete-count vectors really are equivalent. The retained oracle
    // also documents source/native differences, which must NOT be asserted as
    // native parity: source always sums totals, invents optional zeros, and uses
    // unbounded integers. The AST harness simulates SDK shape, not SDK coercion.
    let ids = [
        "explicit_null_retains",
        "missing_metadata_retains",
        "nullable_candidates_admitted",
        "all_zero_replaces",
    ];
    ids.iter()
        .map(|id| {
            let case = source["cases"]
                .as_array()
                .unwrap()
                .iter()
                .find(|case| case["id"] == *id)
                .unwrap();
            let trace = case["trace"].as_array().unwrap();
            complete(
                *id,
                trace.iter().map(|step| step["record"].clone()).collect(),
                Some(serde_json::from_value(trace.last().unwrap()["usage"].clone()).unwrap()),
            )
        })
        .collect()
}

#[test]
fn unchanged_python_ast_vectors_match_only_the_equivalent_native_contract() {
    let source = source_fixture();
    assert_eq!(
        source["source_blob_sha"],
        "0c11d0bea727ba14775a4d5487d46f991af24b98"
    );
    assert_eq!(
        source["source_sha256"],
        "c23a4915f5f0a9f63aac31b15c4047f57c3fdd06520c8b2cf322a638f509769d"
    );
    assert_eq!(
        source["executed_ast_lines"],
        json!([[741, 753], [755, 755]])
    );
    for case in source_cases() {
        let oracle = source["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| item["id"] == case.label)
            .unwrap();
        let mut decoder = ProtocolDecoder::new(Provider::Gemini);
        for step in oracle["trace"].as_array().unwrap() {
            assert_eq!(step["skip_candidate_output"], true);
            assert!(
                decode(&mut decoder, &step["record"]).unwrap().is_empty(),
                "{}",
                case.label
            );
            let expected: Usage = serde_json::from_value(step["usage"].clone()).unwrap();
            assert_eq!(decoder.usage(), Some(&expected), "{}", case.label);
            assert!(!decoder.is_done());
        }
        assert_eq!(
            decode(&mut decoder, &finish()).unwrap(),
            [StreamEvent::ResponseProgress]
        );
        assert!(decoder.end_of_stream().unwrap().is_empty());
    }
}

#[test]
fn nullable_envelopes_neither_invent_usage_nor_progress_and_preserve_the_latest_snapshot() {
    let mut decoder = ProtocolDecoder::new(Provider::Gemini);
    for record in [
        json!({}),
        json!({"candidates":null}),
        json!({"candidates":[]}),
        usage(Value::Null),
    ] {
        assert!(decode(&mut decoder, &record).unwrap().is_empty());
        assert_eq!(decoder.usage(), None);
        assert!(!decoder.is_done());
    }
    assert!(decode(&mut decoder, &usage(full())).unwrap().is_empty());
    assert_eq!(decoder.usage(), Some(&full_snapshot()));
    for candidates in [None, Some(Value::Null), Some(json!([]))] {
        for metadata in [None, Some(Value::Null)] {
            let mut record = json!({});
            if let Some(candidates) = &candidates {
                record["candidates"] = candidates.clone();
            }
            if let Some(metadata) = metadata {
                record["usageMetadata"] = metadata;
            }
            assert!(decode(&mut decoder, &record).unwrap().is_empty());
            assert_eq!(decoder.usage(), Some(&full_snapshot()));
        }
    }
    assert_eq!(
        decode(&mut decoder, &finish()).unwrap(),
        [StreamEvent::ResponseProgress]
    );
    assert!(!decoder.is_done(), "finishReason still requires clean EOF");
    assert!(decode(&mut decoder, &usage(json!({"promptTokenCount":2})))
        .unwrap()
        .is_empty());
    assert_eq!(decoder.usage(), Some(&snapshot(2, 0, 2, None, None)));
    assert!(decode(&mut decoder, &usage(Value::Null))
        .unwrap()
        .is_empty());
    assert_eq!(decoder.usage(), Some(&snapshot(2, 0, 2, None, None)));
    assert!(decoder.end_of_stream().unwrap().is_empty());
    assert!(decoder.is_done());
}

fn snapshot_cases() -> Vec<Case> {
    let mut cases = source_cases();
    cases.push(complete(
        "no initial usage",
        vec![json!({}), usage(Value::Null), json!({"candidates":[]})],
        None,
    ));
    let zero = json!({"promptTokenCount":0,"toolUsePromptTokenCount":0,"candidatesTokenCount":0,
        "thoughtsTokenCount":0,"cachedContentTokenCount":0,"totalTokenCount":0});
    for (label, metadata, expected) in [
        ("empty object resets", json!({}), Usage::default()),
        (
            "partial prompt replaces whole snapshot",
            json!({"promptTokenCount":2}),
            snapshot(2, 0, 2, None, None),
        ),
        (
            "partial cache zero is measured",
            json!({"cachedContentTokenCount":0}),
            snapshot(0, 0, 0, Some(0), None),
        ),
        (
            "partial reasoning zero is measured",
            json!({"thoughtsTokenCount":0}),
            snapshot(0, 0, 0, None, Some(0)),
        ),
        (
            "explicit zeros replace",
            zero.clone(),
            snapshot(0, 0, 0, Some(0), Some(0)),
        ),
        (
            "repeated snapshot never accumulates",
            full(),
            full_snapshot(),
        ),
        (
            "decreasing values replace",
            json!({"promptTokenCount":2,"toolUsePromptTokenCount":1,
            "candidatesTokenCount":3,"thoughtsTokenCount":4,"cachedContentTokenCount":1,"totalTokenCount":10}),
            snapshot(3, 7, 10, Some(1), Some(4)),
        ),
        (
            "cache remains a prompt subset",
            json!({"promptTokenCount":7,"cachedContentTokenCount":5,"candidatesTokenCount":3}),
            snapshot(7, 3, 10, Some(5), None),
        ),
        (
            "tool prompt is counted once",
            json!({"toolUsePromptTokenCount":4}),
            snapshot(4, 0, 4, None, None),
        ),
        (
            "thought tokens are already completion",
            json!({"candidatesTokenCount":3,"thoughtsTokenCount":4}),
            snapshot(0, 7, 7, None, Some(4)),
        ),
        (
            "provider total mismatch wins",
            json!({"promptTokenCount":4,"candidatesTokenCount":3,"totalTokenCount":999}),
            snapshot(4, 3, 999, None, None),
        ),
        (
            "provider total zero wins",
            json!({"promptTokenCount":4,"candidatesTokenCount":3,"totalTokenCount":0}),
            snapshot(4, 3, 0, None, None),
        ),
        (
            "provider total maximum wins",
            json!({"promptTokenCount":4,"candidatesTokenCount":3,"totalTokenCount":u64::MAX}),
            snapshot(4, 3, u64::MAX, None, None),
        ),
        (
            "sparse provider total wins",
            json!({"totalTokenCount":999}),
            snapshot(0, 0, 999, None, None),
        ),
        (
            "missing total uses sum",
            json!({"promptTokenCount":4,"candidatesTokenCount":3}),
            snapshot(4, 3, 7, None, None),
        ),
        (
            "null total uses sum",
            json!({"promptTokenCount":4,"candidatesTokenCount":3,"totalTokenCount":null}),
            snapshot(4, 3, 7, None, None),
        ),
        (
            "prompt sum saturates",
            json!({"promptTokenCount":u64::MAX,"toolUsePromptTokenCount":1,
            "candidatesTokenCount":2,"thoughtsTokenCount":3}),
            snapshot(u64::MAX, 5, u64::MAX, None, Some(3)),
        ),
        (
            "completion sum saturates",
            json!({"promptTokenCount":2,"candidatesTokenCount":u64::MAX,
            "thoughtsTokenCount":1}),
            snapshot(2, u64::MAX, u64::MAX, None, Some(1)),
        ),
        (
            "fallback total saturates",
            json!({"promptTokenCount":u64::MAX-2,"candidatesTokenCount":3}),
            snapshot(u64::MAX - 2, 3, u64::MAX, None, None),
        ),
        (
            "zero provider total beats saturated details",
            json!({"promptTokenCount":u64::MAX,"toolUsePromptTokenCount":1,
            "candidatesTokenCount":u64::MAX,"thoughtsTokenCount":1,"totalTokenCount":0}),
            snapshot(u64::MAX, u64::MAX, 0, None, Some(1)),
        ),
    ] {
        cases.push(complete(
            label,
            vec![
                usage(full()),
                usage(metadata),
                usage(Value::Null),
                json!({}),
            ],
            Some(expected),
        ));
    }
    cases.push(complete(
        "lower snapshot recovers after overflow",
        vec![
            usage(json!({
        "promptTokenCount":u64::MAX,"toolUsePromptTokenCount":1,"candidatesTokenCount":u64::MAX,
        "thoughtsTokenCount":1,"cachedContentTokenCount":u64::MAX})),
            usage(json!({"promptTokenCount":2,"candidatesTokenCount":3})),
        ],
        Some(snapshot(2, 3, 5, None, None)),
    ));
    cases.push(complete(
        "zero then null does not restore prior counts",
        vec![usage(full()), usage(zero), usage(Value::Null)],
        Some(snapshot(0, 0, 0, Some(0), Some(0))),
    ));
    cases.push(Case {
        label: "nullable usage-only updates after finish".into(),
        records: vec![
            usage(full()),
            finish(),
            usage(json!({"promptTokenCount":2,"totalTokenCount":0})),
            usage(Value::Null),
        ],
        usage: Some(snapshot(2, 0, 0, None, None)),
        outcome: Outcome::Success,
        reason: Some(FinishReason::Stop),
    });
    cases
}

#[test]
fn native_snapshot_arithmetic_and_optional_absence_are_not_python_parity_claims() {
    for case in snapshot_cases() {
        let mut decoder = ProtocolDecoder::new(Provider::Gemini);
        for record in &case.records {
            decode(&mut decoder, record).unwrap();
        }
        assert_eq!(decoder.usage(), case.usage.as_ref(), "{}", case.label);
        assert_eq!(
            decoder.finish_reason(),
            case.reason.as_ref(),
            "{}",
            case.label
        );
        assert!(
            decoder.end_of_stream().unwrap().is_empty(),
            "{}",
            case.label
        );
    }
}

fn error_cases() -> Vec<Case> {
    let mut cases = vec![];
    for prior in [false, true] {
        for bad in [json!({}), json!(false), json!(0), json!("invalid")] {
            let mut records = if prior { vec![usage(full())] } else { vec![] };
            // Envelope admission precedes usage. Keep this existing ordering.
            records.push(json!({"candidates":bad,"usageMetadata":{"promptTokenCount":999}}));
            records.push(marker());
            cases.push(Case {
                label: format!("nonarray={bad}, prior={prior}"),
                records,
                usage: prior.then(full_snapshot),
                outcome: Outcome::Protocol,
                reason: None,
            });
        }
    }
    // Candidate-member validation follows usage parsing. No whole-event rollback
    // is promised: a valid new snapshot survives the candidate's protocol error.
    cases.push(Case {
        label: "usage is admitted before invalid candidate member".into(),
        records: vec![
            usage(full()),
            json!({"usageMetadata":{"promptTokenCount":2},"candidates":[null]}),
            marker(),
        ],
        usage: Some(snapshot(2, 0, 2, None, None)),
        outcome: Outcome::Protocol,
        reason: None,
    });
    cases.push(Case {
        label: "usage is admitted before content-after-finish rejection".into(),
        records: vec![
            usage(full()),
            finish(),
            json!({"usageMetadata":{"totalTokenCount":0},
            "candidates":[{"content":{"parts":[{"text":"must-not-escape"}]}}]}),
            marker(),
        ],
        usage: Some(snapshot(0, 0, 0, None, None)),
        outcome: Outcome::Protocol,
        reason: Some(FinishReason::Stop),
    });
    for prior in [false, true] {
        let mut records = if prior { vec![usage(full())] } else { vec![] };
        records.extend([usage(Value::Null), json!({"candidates":null})]);
        cases.push(Case {
            label: format!("metadata-only EOF, prior={prior}"),
            records,
            usage: prior.then(full_snapshot),
            outcome: Outcome::Protocol,
            reason: None,
        });
    }
    cases
}

#[test]
fn invalid_nonarray_candidates_remain_rejected_and_usage_ordering_is_unchanged() {
    for case in error_cases() {
        let mut decoder = ProtocolDecoder::new(Provider::Gemini);
        let mut rejected = false;
        for record in &case.records {
            if decode(&mut decoder, record).is_err() {
                rejected = true;
                break;
            }
        }
        if !rejected {
            assert!(decoder.end_of_stream().is_err());
        }
        assert_eq!(decoder.usage(), case.usage.as_ref(), "{}", case.label);
        assert_eq!(
            decoder.finish_reason(),
            case.reason.as_ref(),
            "{}",
            case.label
        );
        assert!(
            matches!(decode(&mut decoder, &marker()), Err(ServiceError::Protocol(message)) if message.contains("poisoned"))
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
            // This decoded content is an ACK for all earlier usage/null records.
            // A peer write or elapsed sleep would not establish that ordering.
            if case.outcome == Outcome::Cancelled
                && matches!(&event, Ok(StreamEvent::TextDelta(text)) if text == MARKER)
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
            "{} stream deadline; cancel drain={drained:?}",
            case.label
        ));
    }
    if case.outcome == Outcome::Cancelled && !cancelled {
        return Err("cancel marker never decoded".into());
    }
    // Draining to None observes the private HTTP producer's closed channel.
    // LlmStream exposes no JoinHandle; only the real socket peer is joined here.
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
    assert_eq!(
        events.first().unwrap().as_ref().unwrap(),
        &StreamEvent::Started,
        "{}",
        case.label
    );
    assert_eq!(
        events.last().unwrap().as_ref().unwrap(),
        &StreamEvent::Finished {
            reason: case.reason.clone()
        },
        "{}",
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
    let progress = events
        .iter()
        .filter(|event| matches!(event, Ok(StreamEvent::ResponseProgress)))
        .count();
    let progress_expected =
        case.outcome == Outcome::Success || case.outcome.holds_socket() || case.reason.is_some();
    assert_eq!(
        progress,
        usize::from(progress_expected),
        "{}: {events:?}",
        case.label
    );
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
        "{}: {events:?}",
        case.label
    );
    let errors: Vec<_> = events
        .iter()
        .filter_map(|event| event.as_ref().err())
        .collect();
    assert_eq!(
        errors.len(),
        usize::from(case.outcome != Outcome::Success),
        "{}: {events:?}",
        case.label
    );
    if let Some(error) = errors.first() {
        assert!(
            matches!(
                (case.outcome, error),
                (Outcome::Protocol, ServiceError::Protocol(_))
                    | (Outcome::Cancelled, ServiceError::Cancelled)
                    | (Outcome::Timeout, ServiceError::Timeout)
            ),
            "{}: {error:?}",
            case.label
        );
        assert!(
            events[events.len() - 2].is_err(),
            "{}: {events:?}",
            case.label
        );
    }
    if let Some(expected) = &case.usage {
        let offset = if case.outcome == Outcome::Success {
            2
        } else {
            3
        };
        assert_eq!(
            events[events.len() - offset].as_ref().unwrap(),
            &StreamEvent::Usage(expected.clone()),
            "{}: {events:?}",
            case.label
        );
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
        if case.outcome.holds_socket() {
            vec![MARKER]
        } else {
            vec![]
        },
        "{}",
        case.label
    );
    assert!(!events.iter().any(|event| matches!(
        event,
        Ok(StreamEvent::ToolCall(_) | StreamEvent::ThoughtDelta(_))
    )));
}

#[tokio::test]
async fn real_http_snapshots_replace_instead_of_accumulating_and_keep_native_totals() {
    let cases = snapshot_cases();
    for (case, events) in cases.iter().zip(raw_cases(&cases).await) {
        check_raw(case, &events);
    }
}
#[tokio::test]
async fn real_http_protocol_failure_flushes_the_last_admitted_snapshot_once() {
    let cases = error_cases();
    for (case, events) in cases.iter().zip(raw_cases(&cases).await) {
        check_raw(case, &events);
    }
}
#[tokio::test]
async fn real_http_cancel_and_idle_timeout_flush_once_after_decoded_null_and_reuse_is_fresh() {
    let mut cases = vec![];
    for outcome in [Outcome::Cancelled, Outcome::Timeout] {
        for prior in [false, true] {
            let mut records = if prior {
                vec![
                    usage(full()),
                    usage(
                        json!({"promptTokenCount":2,"cachedContentTokenCount":0,"totalTokenCount":0}),
                    ),
                ]
            } else {
                vec![]
            };
            records.extend([usage(Value::Null), marker()]);
            cases.push(Case {
                label: format!("{outcome:?}, prior={prior}"),
                records,
                usage: prior.then(|| snapshot(2, 0, 0, Some(0), None)),
                outcome,
                reason: None,
            });
            cases.push(complete(
                "fresh request after terminal failure",
                vec![usage(Value::Null)],
                None,
            ));
        }
    }
    for (case, events) in cases.iter().zip(raw_cases(&cases).await) {
        check_raw(case, &events);
    }
}

#[tokio::test]
async fn real_http_nullable_envelopes_preserve_later_text_and_emit_usage_only_at_eof() {
    let case = Case {
        label: "null envelopes preserve later text".into(),
        records: vec![
            usage(full()),
            json!({"usageMetadata":null}),
            json!({"candidates":[],"usageMetadata":null}),
            usage(Value::Null),
            marker(),
            finish(),
            usage(Value::Null),
        ],
        usage: Some(full_snapshot()),
        outcome: Outcome::Success,
        reason: Some(FinishReason::Stop),
    };
    let mut results = raw_cases(&[case]).await;
    let events: Vec<_> = results.remove(0).into_iter().map(Result::unwrap).collect();
    assert_eq!(
        events,
        vec![
            StreamEvent::Started,
            StreamEvent::ResponseProgress,
            StreamEvent::TextDelta(MARKER.into()),
            StreamEvent::Usage(full_snapshot()),
            StreamEvent::Finished {
                reason: Some(FinishReason::Stop)
            }
        ]
    );
}
