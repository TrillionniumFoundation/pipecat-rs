//! Audio usage projection is independent opt-in, with atomic selected-field admission.
use pipecat_core::*;
use pipecat_rtvi::*;
use serde_json::{json, Value};

const AUDIO: [&str; 3] = [
    "input_audio_tokens",
    "output_audio_tokens",
    "cache_read_input_audio_tokens",
];
const LEGACY: [&str; 3] = [
    "cache_read_input_tokens",
    "cache_creation_input_tokens",
    "reasoning_tokens",
];
fn base() -> Value {
    json!({"prompt_tokens": 7,"completion_tokens": 5,"total_tokens": 12})
}
fn metric() -> Metric {
    Metric::LlmUsage {
        processor: "fixture".into(),
        prompt_tokens: 7,
        completion_tokens: 5,
        total_tokens: 12,
    }
}
fn frame(usage: Value) -> Frame {
    let mut frame = Frame::new(FrameData::Metrics(vec![metric()]));
    frame.metadata.insert("llm_usage".into(), usage);
    frame
}
fn observer(legacy: bool, audio: bool) -> RtviObserver {
    RtviObserver::new(Default::default(), None)
        .unwrap()
        .with_llm_usage_details(legacy)
        .with_llm_audio_usage_details(audio)
}
fn wire(observer: &RtviObserver, frame: &Frame) -> Vec<Value> {
    observer
        .observe(frame, Observation::new("fixture", Direction::Downstream))
        .unwrap()
        .into_iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect()
}
fn projected(legacy: bool, audio: bool, usage: Value) -> Value {
    wire(&observer(legacy, audio), &frame(usage))[0]["data"]["tokens"][0].clone()
}
fn complete() -> Value {
    let mut v = base();
    for (key, n) in LEGACY
        .into_iter()
        .chain(AUDIO)
        .zip([0, 3, 2, 0, u64::MAX, 1])
    {
        v[key] = json!(n);
    }
    v
}
#[test]
fn all_four_switch_combinations_preserve_base_and_project_only_selected_fields() {
    for legacy in [false, true] {
        for audio in [false, true] {
            let mut expected = base();
            let usage = complete();
            for (keys, enabled) in [(LEGACY, legacy), (AUDIO, audio)] {
                if enabled {
                    for key in keys {
                        expected[key] = usage[key].clone();
                    }
                }
            }
            assert_eq!(projected(legacy, audio, usage), expected);
        }
    }
    let f = frame(complete());
    assert_eq!(
        wire(&RtviObserver::new(Default::default(), None).unwrap(), &f)[0]["data"]["tokens"],
        json!([base()])
    );
}
#[test]
fn selected_invalid_fields_reject_whole_extension_and_unselected_fields_are_ignored() {
    for key in LEGACY.into_iter().chain(AUDIO) {
        for bad in [
            json!(-1),
            json!(1.5),
            json!("1"),
            json!(true),
            json!([]),
            json!({}),
            serde_json::from_str::<Value>("18446744073709551616").unwrap(),
        ] {
            for legacy in [false, true] {
                for audio in [false, true] {
                    let mut usage = complete();
                    usage[key] = bad.clone();
                    let expected =
                        if (legacy && LEGACY.contains(&key)) || (audio && AUDIO.contains(&key)) {
                            base()
                        } else {
                            projected(legacy, audio, complete())
                        };
                    assert_eq!(
                        projected(legacy, audio, usage),
                        expected,
                        "key={key} legacy={legacy} audio={audio}"
                    );
                }
            }
        }
    }
}
#[test]
fn absent_and_null_audio_fields_omit_but_zero_and_max_are_not_normalized() {
    for key in AUDIO {
        let mut usage = complete();
        usage.as_object_mut().unwrap().remove(key);
        let missing = projected(false, true, usage.clone());
        assert!(missing.get(key).is_none());
        usage[key] = Value::Null;
        assert_eq!(projected(false, true, usage), missing);
    }
    let v = projected(false, true, complete());
    assert_eq!(v["input_audio_tokens"], 0);
    assert_eq!(v["output_audio_tokens"], u64::MAX);
    assert_eq!(v["total_tokens"], 12);
}
#[test]
fn malformed_or_mismatched_base_and_ambiguous_ownership_never_enrich() {
    for key in ["prompt_tokens", "completion_tokens", "total_tokens"] {
        for bad in [json!(999), json!(-1), Value::Null, json!("7"), json!([])] {
            let mut usage = complete();
            usage[key] = bad;
            assert_eq!(projected(true, true, usage), base());
        }
        let mut usage = complete();
        usage.as_object_mut().unwrap().remove(key);
        assert_eq!(projected(true, true, usage), base());
    }
    for bad in [Value::Null, json!([]), json!(7)] {
        assert_eq!(projected(true, true, bad), base());
    }
    let mut f = frame(complete());
    if let FrameData::Metrics(metrics) = &mut f.data {
        metrics.push(metric());
    }
    assert_eq!(
        wire(&observer(true, true), &f)[0]["data"]["tokens"],
        json!([base(), base()])
    );
    let mut f = frame(complete());
    if let FrameData::Metrics(metrics) = &mut f.data {
        metrics.push(Metric::TtsUsage {
            processor: "tts".into(),
            characters: 8,
        });
    }
    let out = wire(&observer(true, true), &f);
    assert_eq!(out[0]["data"]["tokens"], json!([complete()]));
    assert_eq!(out[0]["data"]["characters"][0]["value"], 8);
}
#[test]
fn unknown_tool_and_private_metadata_never_forward_and_frame_stays_unchanged() {
    let mut usage = complete();
    usage["tool_use_prompt_tokens"] = json!(900);
    usage["model"] = json!("private-model");
    usage["unknown"] = json!("x".repeat(100_000));
    let f = frame(usage);
    let original = f.clone();
    let o = observer(true, true);
    assert_eq!(wire(&o, &f)[0]["data"]["tokens"], json!([complete()]));
    assert_eq!(f, original);
    assert!(wire(&o, &f).is_empty());
    o.reset();
    assert_eq!(wire(&o, &f)[0]["data"]["tokens"], json!([complete()]));
}
#[test]
fn original_observation_filters_and_disabled_metrics_remain_effective() {
    let f = frame(complete());
    let o = observer(true, true);
    let mut observation = Observation::new("fixture", Direction::Downstream);
    observation.first_push = false;
    assert!(o.observe(&f, observation).unwrap().is_empty());
    assert_eq!(wire(&o, &f)[0]["data"]["tokens"], json!([complete()]));
    let o = RtviObserver::new(
        RtviObserverParams {
            metrics_enabled: false,
            ..Default::default()
        },
        None,
    )
    .unwrap()
    .with_llm_audio_usage_details(true);
    assert!(wire(&o, &frame(complete())).is_empty());
}

#[test]
fn enriched_wire_still_obeys_final_message_budget() {
    let mut witnessed = false;
    for length in 400..1000 {
        let mut f = frame(complete());
        if let FrameData::Metrics(metrics) = &mut f.data {
            metrics.push(Metric::Processing {
                processor: "x".repeat(length),
                seconds: 0.5,
            });
        }
        let limited = |audio| {
            RtviObserver::new(
                RtviObserverParams {
                    max_message_bytes: 1024,
                    ..Default::default()
                },
                None,
            )
            .unwrap()
            .with_llm_audio_usage_details(audio)
        };
        let legacy = limited(false).observe(&f, Observation::new("fixture", Direction::Downstream));
        let enriched =
            limited(true).observe(&f, Observation::new("fixture", Direction::Downstream));
        if legacy.is_ok() && enriched.is_err() {
            witnessed = true;
            break;
        }
    }
    assert!(
        witnessed,
        "must exercise final enriched output budget, not input rejection"
    );
}

#[test]
fn selected_six_fields_match_retained_fixed_python_single_usage_vectors() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/python_usage_details_source.json")).unwrap();
    assert_eq!(
        fixture["revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    let ids = [
        "optional_absent",
        "optional_explicit_none",
        "all_optional_zero",
        "all_optional_nonzero",
        "single_zero_cache_read_input_tokens",
        "single_zero_cache_creation_input_tokens",
        "single_zero_reasoning_tokens",
        "single_zero_input_audio_tokens",
        "single_zero_output_audio_tokens",
        "single_zero_cache_read_input_audio_tokens",
        "mixed_none_zero_nonzero",
        "all_required_zero_still_one_record",
        "provider_total_is_not_recomputed",
        "full_u64_values_preserved",
    ];
    let cases = fixture["cases"].as_array().unwrap();
    for id in ids {
        let case = cases.iter().find(|case| case["id"] == id).unwrap();
        let usage = case["input"].clone();
        let mut f = Frame::new(FrameData::Metrics(vec![Metric::LlmUsage {
            processor: "fixture".into(),
            prompt_tokens: usage["prompt_tokens"].as_u64().unwrap(),
            completion_tokens: usage["completion_tokens"].as_u64().unwrap(),
            total_tokens: usage["total_tokens"].as_u64().unwrap(),
        }]));
        f.metadata.insert("llm_usage".into(), usage);
        assert_eq!(
            wire(&observer(true, true), &f)[0]["data"]["tokens"],
            case["message"]["data"]["tokens"],
            "{id}"
        );
    }
}
