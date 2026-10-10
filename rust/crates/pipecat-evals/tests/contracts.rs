use pipecat_evals::{
    events::{EventLimits, EventStream},
    matcher::{ExpectationMatcher, MatchOutcome, PendingCheck},
    serializer::{self, EvalControl},
    EvalError, ScenarioFile,
};
use pipecat_protocol::rtvi::Envelope;
use serde_json::{json, Value};
fn envelope(kind: &str, data: Value) -> Envelope {
    serde_json::from_value(json!({"label":"rtvi-ai","type":kind,"data":data})).unwrap()
}
fn expectation(value: Value) -> pipecat_evals::Expectation {
    ScenarioFile::from_value(json!({"name":"x","turns":[{"expect":[value]}]}))
        .unwrap()
        .scenarios
        .remove(0)
        .turns
        .remove(0)
        .expect
        .remove(0)
}
#[test]
fn source_fixture_normalizes_response_calls_and_observer_configuration() {
    let file = ScenarioFile::load(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/script.yaml"
    ))
    .unwrap();
    let s = &file.scenarios[0];
    assert_eq!(s.name, "native_contract/tools_and_markers");
    assert_eq!(s.turns[0].expect[2].event, "llm_response");
    assert_eq!(s.turns[1].dtmf.as_deref(), Some("012#"));
    assert_eq!(
        s.observer_config(),
        json!({"function_call_report_level":{"*":"full"},"llm_markers":true})
    );
}
#[test]
fn untouched_source_fixture_rejects_unimplemented_judge_provider() {
    let error = ScenarioFile::load(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/source_short_complete.yaml"
    ))
    .unwrap_err();
    assert!(matches!(error, EvalError::Unsupported(_)));
    assert!(error.to_string().contains("judge provider"));
}
#[test]
fn defaults_replace_whole_values_and_names_are_qualified() {
    let file=ScenarioFile::from_value(json!({"name":"f","context":[{"role":"system","content":"old"}],"turns":[],"stop_on_failure":false,"scenarios":[{"name":"a"},{"name":"b","context":[]}]})).unwrap();
    assert_eq!(file.scenarios[0].context.len(), 1);
    assert!(file.scenarios[1].context.is_empty());
    assert!(!file.scenarios[0].stop_on_failure);
    assert_eq!(file.scenarios[1].name, "f/b");
}
#[test]
fn strict_invalid_and_unsupported_features_do_not_become_pass_only_checks() {
    for value in [
        json!({"name":"x","persona":"caller","goal":"book"}),
        json!({"name":"x","turns":[],"user":{"modality":"audio"}}),
        json!({"name":"x","turns":[{"user":"x","audio":"x.wav"}]}),
        json!({"name":"x","turns":[{"image":"x.png"}]}),
        json!({"name":"x","turns":[{"expect":[{"event":"response","eval":"correct"}]}]}),
        json!({"name":"x","turns":[{"expect":[{"event":"tts_response"}]}]}),
        json!({"name":"x","turns":[{"expect":[{"event":"typo"}]}]}),
        json!({"name":"x","turns":[{"expect":[{"event":"response","absent":true,"text_contains":"x"}]}]}),
        json!({"name":"x","turns":[{"expect":[{"event":"response","within_ms":0}]}]}),
        json!({"name":"x","turns":[{"expect":[{"event":"response","marker_first":true}]}]}),
        json!({"name":"x","turns":[{"expect":[{"event":"function_call","calls":[]}]}]}),
        json!({"name":"x","turns":[{"user":"x","dtmf":"1"}]}),
        json!({"name":"x","turns":[{"dtmf":"A"}]}),
        json!({"name":"x","turns":[{"send_after":{"delay_ms":1}}]}),
        json!({"name":"x","turns":[],"scenarios":[{"name":"same"},{"name":"same"}]}),
        json!({"name":"x","turns":[],"stop_on_falure":true}),
    ] {
        assert!(
            ScenarioFile::from_value(value.clone()).is_err(),
            "accepted {value}"
        );
    }
}
#[test]
fn yaml_includes_are_relative_bounded_and_preserve_leading_zero_dtmf() {
    let dir = std::env::temp_dir().join(format!("pipecat-evals-yaml-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("turns.yaml"), "- dtmf: 012\n- dtmf: 123\n").unwrap();
    std::fs::write(
        dir.join("main.yaml"),
        "name: x\nturns: !include turns.yaml\n",
    )
    .unwrap();
    let file = ScenarioFile::load(dir.join("main.yaml")).unwrap();
    assert_eq!(file.scenarios[0].turns[0].dtmf.as_deref(), Some("012"));
    assert_eq!(file.scenarios[0].turns[1].dtmf.as_deref(), Some("123"));
    std::fs::write(
        dir.join("main.yaml"),
        "name: x\nturns: !include main.yaml\n",
    )
    .unwrap();
    assert!(ScenarioFile::load(dir.join("main.yaml"))
        .unwrap_err()
        .to_string()
        .contains("cycle"));
    std::fs::write(
        dir.join("main.yaml"),
        "name: x\nturns: !python/object anything\n",
    )
    .unwrap();
    assert!(ScenarioFile::load(dir.join("main.yaml")).is_err());
    std::fs::remove_dir_all(dir).unwrap();
}
#[test]
fn eval_controls_match_python_serializer_wire() {
    let controls = [
        (
            "eval-context",
            json!({"messages":[{"role":"system","content":"be terse"}]}),
        ),
        (
            "eval-configure",
            json!({"function_call_report_level":{"*":"full"},"vad_user_speaking":true,"llm_markers":true}),
        ),
        ("eval-cancel", json!({})),
        (
            "eval-image",
            json!({"image":"aGVsbG8=","format":"image/png"}),
        ),
    ];
    for (i, (kind, data)) in controls.into_iter().enumerate() {
        let wire = serializer::control(i as u64 + 1, kind, data)
            .encode()
            .unwrap();
        let parsed = serializer::decode_control(wire.as_bytes(), 4096).unwrap();
        match parsed {
            EvalControl::Context(v) => assert_eq!(v[0]["content"], "be terse"),
            EvalControl::Configure(v) => assert_eq!(v.llm_markers, Some(true)),
            EvalControl::Cancel => {}
            EvalControl::Image { bytes, format } => {
                assert_eq!(bytes, b"hello");
                assert_eq!(format, "image/png");
            }
            _ => panic!("control forwarded"),
        }
    }
    assert!(serializer::decode_control(b"not json", 1024).is_err());
    assert!(serializer::decode_control(
        serializer::control(
            1,
            "eval-configure",
            json!({"function_call_report_level":{"*":"administrator"}})
        )
        .encode()
        .unwrap()
        .as_bytes(),
        4096
    )
    .is_err());
}
#[test]
fn event_mapping_interruption_suppresses_stale_text_and_preserves_transcriptions() {
    let mut stream = EventStream::new(EventLimits::default());
    stream
        .ingest(&envelope(
            "user-transcription",
            json!({"text":"partial","final":false}),
        ))
        .unwrap();
    assert!(stream.events_seen.is_empty());
    stream
        .ingest(&envelope(
            "user-transcription",
            json!({"text":"DTMF: 123#","final":true}),
        ))
        .unwrap();
    stream
        .ingest(&envelope("bot-llm-started", json!({})))
        .unwrap();
    stream
        .ingest(&envelope("bot-llm-text", json!({"text":"stale"})))
        .unwrap();
    stream
        .ingest(&envelope("bot-interrupted", json!({})))
        .unwrap();
    stream
        .ingest(&envelope("bot-llm-text", json!({"text":"straggler"})))
        .unwrap();
    stream
        .ingest(&envelope("bot-llm-stopped", json!({})))
        .unwrap();
    assert_eq!(stream.pop().unwrap()["type"], "user_transcription");
    assert_eq!(stream.pop().unwrap()["type"], "bot_interrupted");
    assert!(stream.pop().is_none());
    stream
        .ingest(&envelope("bot-llm-started", json!({})))
        .unwrap();
    stream
        .ingest(&envelope("bot-llm-text", json!({"text":"Hello "})))
        .unwrap();
    stream
        .ingest(&envelope("bot-llm-text", json!({"text":"world"})))
        .unwrap();
    stream
        .ingest(&envelope("bot-llm-stopped", json!({})))
        .unwrap();
    assert_eq!(stream.events_seen.last().unwrap()["text"], "Hello world");
    assert!(stream
        .events_seen
        .last()
        .unwrap()
        .get("started_at")
        .is_some());
}
#[test]
fn event_mapping_function_stopped_image_vad_and_limits() {
    let mut stream = EventStream::new(EventLimits {
        max_events: 3,
        max_event_bytes: 4096,
        max_text_bytes: 4,
    });
    stream
        .ingest(&envelope(
            "llm-function-call-stopped",
            json!({"function_name":"write","tool_call_id":"a","cancelled":true}),
        ))
        .unwrap();
    assert_eq!(
        stream.events_seen[0]["args"],
        json!({"tool_call_id":"a","cancelled":true})
    );
    stream
        .ingest(&envelope(
            "eval-bot-image",
            json!({"width":640,"height":480,"format":"RGB"}),
        ))
        .unwrap();
    assert_eq!(stream.events_seen[1]["width"], 640);
    stream
        .ingest(&envelope("vad-user-stopped-speaking", json!({})))
        .unwrap();
    assert!(matches!(
        stream.ingest(&envelope("bot-started-speaking", json!({}))),
        Err(EvalError::Limit(_))
    ));
    assert!(matches!(
        stream.ingest(&envelope("bot-llm-text", json!({"text":"12345"}))),
        Err(EvalError::Limit(_))
    ));
}
#[test]
fn matcher_calls_arrive_in_any_order_and_corrected_arguments_can_pass() {
    let exp =
        expectation(json!({"event":"function_call","calls":[{"name":"a","args":{"x":1}},"b"]}));
    let mut matcher = ExpectationMatcher::new(1024, 10);
    let mut check = PendingCheck::new(exp);
    for event in [
        json!({"type":"function_call","name":"b","args":{}}),
        json!({"type":"function_call","name":"a","args":{"x":0}}),
    ] {
        assert_eq!(
            matcher.feed(&mut check, event).unwrap(),
            MatchOutcome::Pending
        );
    }
    assert!(
        matches!(matcher.timeout(&check),MatchOutcome::Failed{kind,..} if kind=="function_args_mismatch")
    );
    assert_eq!(
        matcher
            .feed(
                &mut check,
                json!({"type":"function_call","name":"a","args":{"x":1,"extra":true}})
            )
            .unwrap(),
        MatchOutcome::Passed("a, b".into())
    );
    matcher.reset_turn();
    let check = PendingCheck::new(expectation(
        json!({"event":"function_call","name":"absent"}),
    ));
    assert!(
        matches!(matcher.timeout(&check),MatchOutcome::Failed{kind,..} if kind=="missing_function_call")
    );
}
#[test]
fn matcher_whitespace_aggregation_exclusions_absence_and_marker_shape() {
    let mut matcher = ExpectationMatcher::new(1024, 10);
    let mut check = PendingCheck::new(expectation(
        json!({"event":"response","text_contains":"Hello world"}),
    ));
    assert_eq!(
        matcher
            .feed(&mut check, json!({"type":"llm_response","text":"Hello\n"}))
            .unwrap(),
        MatchOutcome::Pending
    );
    assert!(matches!(
        matcher
            .feed(&mut check, json!({"type":"llm_response","text":" world"}))
            .unwrap(),
        MatchOutcome::Passed(_)
    ));
    let mut check = PendingCheck::new(expectation(
        json!({"event":"response","text_excludes":"bad"}),
    ));
    assert!(
        matches!(matcher.feed(&mut check,json!({"type":"llm_response","text":"bad"})).unwrap(),MatchOutcome::Failed{kind,..} if kind=="text_present")
    );
    let mut check = PendingCheck::new(expectation(
        json!({"event":"response","absent":true,"within_ms":1}),
    ));
    assert!(matches!(matcher.timeout(&check), MatchOutcome::Passed(_)));
    assert!(
        matches!(matcher.feed(&mut check,json!({"type":"llm_response","text":"unexpected"})).unwrap(),MatchOutcome::Failed{kind,..} if kind=="unexpected_event")
    );
    let exp = expectation(
        json!({"event":"llm_marker","marker":"incomplete","marker_first":true,"markers":1,"text_after":false}),
    );
    let mut check = PendingCheck::new(exp.clone());
    assert!(matches!(matcher.feed(&mut check,json!({"type":"llm_marker","text":"◐","kind":"short","raw":" ◐ ","markers":["●","◐","○"]})).unwrap(),MatchOutcome::Passed(_)));
    let mut check = PendingCheck::new(exp);
    assert!(
        matches!(matcher.feed(&mut check,json!({"type":"llm_marker","kind":"short","raw":"preamble ◐ text","markers":["●","◐","○"]})).unwrap(),MatchOutcome::Failed{kind,..} if kind=="marker_format")
    );
}

#[test]
fn include_construction_shares_budgets_and_preserves_relative_path_guards() {
    let dir = std::env::temp_dir().join(format!(
        "pipecat-evals-include-budget-{}",
        std::process::id()
    ));
    std::fs::create_dir_all(dir.join("sub")).unwrap();
    let row = vec!["0"; 192].join(", ");
    let copies = vec!["*row"; 192].join(", ");
    std::fs::write(
        dir.join("child.yaml"),
        format!("- role: system\n  content: &row [{row}]\n  repeated: [{copies}]\n"),
    )
    .unwrap();
    // One included tree fits; its repeated inclusion must share the same budget.
    std::fs::write(
        dir.join("main.yaml"),
        "name: x\nturns: []\ncontext: !include child.yaml\n",
    )
    .unwrap();
    assert!(ScenarioFile::load(dir.join("main.yaml")).is_ok());
    std::fs::write(
        dir.join("main.yaml"),
        "name: x\nturns: []\nfirst: !include child.yaml\nsecond: !include child.yaml\n",
    )
    .unwrap();
    assert!(matches!(
        ScenarioFile::load(dir.join("main.yaml")),
        Err(EvalError::Limit("scenario expanded nodes"))
    ));
    // The include's seed starts at the parent's depth, not at a fresh root.
    std::fs::write(
        dir.join("deep.yaml"),
        format!("{}0{}", "[".repeat(40), "]".repeat(40)),
    )
    .unwrap();
    std::fs::write(
        dir.join("main.yaml"),
        format!("{}!include deep.yaml{}", "[".repeat(30), "]".repeat(30)),
    )
    .unwrap();
    assert!(matches!(
        ScenarioFile::load(dir.join("main.yaml")),
        Err(EvalError::Limit("scenario expanded depth"))
    ));
    std::fs::write(dir.join("main.yaml"), "!include sub/../main.yaml\n").unwrap();
    assert!(ScenarioFile::load(dir.join("main.yaml"))
        .unwrap_err()
        .to_string()
        .contains("cycle"));
    for i in 0..9 {
        std::fs::write(
            dir.join(format!("{i}.yaml")),
            if i < 8 {
                format!("!include {}.yaml\n", i + 1)
            } else {
                "name: x\nturns: []\n".into()
            },
        )
        .unwrap();
    }
    assert!(ScenarioFile::load(dir.join("0.yaml"))
        .unwrap_err()
        .to_string()
        .contains("depth exceeds 8"));
    std::fs::write(
        dir.join("main.yaml"),
        format!("!include {}\n", "x".repeat(4097)),
    )
    .unwrap();
    assert!(matches!(
        ScenarioFile::load(dir.join("main.yaml")),
        Err(EvalError::Limit("include path bytes"))
    ));
    std::fs::write(
        dir.join("child.yaml"),
        "name: first\nname: second\nturns: []\n",
    )
    .unwrap();
    std::fs::write(dir.join("main.yaml"), "!include child.yaml\n").unwrap();
    assert!(ScenarioFile::load(dir.join("main.yaml"))
        .unwrap_err()
        .to_string()
        .contains("duplicate"));
    std::fs::write(dir.join("main.yaml"), vec![b' '; 1024 * 1024 + 1]).unwrap();
    assert!(matches!(
        ScenarioFile::load(dir.join("main.yaml")),
        Err(EvalError::Limit("scenario/include bytes"))
    ));
    std::fs::remove_dir_all(dir).unwrap();
}
