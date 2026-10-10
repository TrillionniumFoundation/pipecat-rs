//! Replay operations recorded by the unchanged original Python tests.
use pipecat_text::*;
use serde_json::{json, Value};

fn mode(config: &Value) -> AggregationType {
    match config["aggregation_type"].as_str() {
        Some("token") => AggregationType::Token,
        Some("word") => AggregationType::Word,
        _ => AggregationType::Sentence,
    }
}
fn normalize(value: Value) -> Value {
    match value {
        Value::Array(a) => Value::Array(a.into_iter().map(normalize).collect()),
        Value::Object(mut o) => {
            o.remove("full_match");
            Value::Object(o)
        }
        other => other,
    }
}
#[test]
fn original_aggregator_operation_traces() {
    let fixtures: Value =
        serde_json::from_str(include_str!("fixtures/source_traces.json")).unwrap();
    let mut count = 0;
    for trace in fixtures["traces"].as_array().unwrap() {
        let name = trace["test"].as_str().unwrap();
        let config = &trace["config"];
        let language = config["language"].as_str();
        let kind = trace["class"].as_str().unwrap();
        if kind == "MarkdownTextFilter" {
            continue;
        }
        // A source test assigns a private Python field directly. Its native
        // equivalent is tested within the pattern module, not this public API.
        if name.ends_with("test_flush_skips_completed_pair_still_in_buffer") {
            continue;
        }
        let mut pattern = (kind == "PatternPairAggregator")
            .then(|| PatternPairAggregator::new(mode(config), language));
        let mut simple: Box<dyn TextAggregator> = if kind == "SkipTagsAggregator" {
            Box::new(SkipTagsAggregator::new(
                serde_json::from_value(config["tags"].clone()).unwrap(),
                mode(config),
                language,
            ))
        } else {
            Box::new(SimpleTextAggregator::new(mode(config), language))
        };
        for (index, op) in trace["operations"].as_array().unwrap().iter().enumerate() {
            let args = &op["args"];
            let kwargs = &op["kwargs"];
            if op["method"] == "add_pattern" {
                let action = match kwargs["action"].as_str() {
                    Some("keep") => MatchAction::Keep,
                    Some("aggregate") => MatchAction::Aggregate,
                    _ => MatchAction::Remove,
                };
                pattern
                    .as_mut()
                    .unwrap()
                    .add_pattern(
                        kwargs["type"].as_str().unwrap(),
                        kwargs["start_pattern"].as_str().unwrap(),
                        kwargs["end_pattern"].as_str().unwrap(),
                        action,
                    )
                    .unwrap();
                continue;
            }
            let a: &mut dyn TextAggregator = if let Some(p) = pattern.as_mut() {
                p
            } else {
                simple.as_mut()
            };
            let actual = match op["method"].as_str().unwrap() {
                "aggregate" => json!(a.aggregate(args[0].as_str().unwrap())),
                "flush" => json!(a.flush()),
                "reset" => {
                    a.reset();
                    Value::Null
                }
                "handle_interruption" => {
                    a.handle_interruption();
                    Value::Null
                }
                "set_language" => {
                    a.set_language(args[0].as_str());
                    Value::Null
                }
                unknown => panic!("Unhandled {unknown}"),
            };
            assert_eq!(
                normalize(actual),
                normalize(op["result"].clone()),
                "{name} operation {index}: {op}"
            );
            count += 1;
        }
    }
    assert!(
        count >= 365,
        "expected substantial original operation coverage, got {count}"
    );
}
#[test]
fn original_markdown_operation_traces() {
    let fixtures: Value =
        serde_json::from_str(include_str!("fixtures/source_traces.json")).unwrap();
    let mut failures = vec![];
    for trace in fixtures["traces"].as_array().unwrap() {
        if trace["class"] != "MarkdownTextFilter" {
            continue;
        }
        let settings = if trace["config"]["params"].is_null() {
            MarkdownSettings::default()
        } else {
            serde_json::from_value(trace["config"]["params"].clone()).unwrap()
        };
        let mut filter = MarkdownTextFilter::new(settings);
        for op in trace["operations"].as_array().unwrap() {
            let args = &op["args"];
            let actual = match op["method"].as_str().unwrap() {
                "filter" => json!(filter.filter(args[0].as_str().unwrap())),
                "remove_tables" => json!(filter.remove_tables(args[0].as_str().unwrap())),
                "handle_interruption" => {
                    filter.handle_interruption();
                    Value::Null
                }
                "reset_interruption" => {
                    filter.reset_interruption();
                    Value::Null
                }
                "update_settings" => {
                    filter.update_settings(
                        args[0]
                            .as_object()
                            .unwrap()
                            .iter()
                            .map(|(k, v)| (k.as_str(), v.as_bool().unwrap())),
                    );
                    Value::Null
                }
                unknown => panic!("Unhandled {unknown}"),
            };
            if actual != op["result"] {
                failures.push(format!(
                    "{}: {args}\nexpected: {}\nactual: {actual}",
                    trace["test"], op["result"]
                ));
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n\n"));
}

#[test]
fn original_utility_call_traces() {
    use pipecat_text::{
        alnum_utils as a, markup_utils as m, string as s, word_timestamp_utils as w,
    };
    let fixtures: Value =
        serde_json::from_str(include_str!("fixtures/source_traces.json")).unwrap();
    let mut count = 0;
    for call in fixtures["utility_calls"].as_array().unwrap() {
        let args = &call["args"];
        let text = args[0].as_str().unwrap_or("");
        let actual = match call["function"].as_str().unwrap() {
            "strip_markup" => json!(m::strip_markup(text)),
            "strip_complete_markup" => json!(m::strip_complete_markup(text)),
            "raw_offset_after_clean_chars" => json!(m::raw_offset_after_clean_chars(
                text,
                args[1].as_u64().unwrap() as usize
            )),
            "split_markup_runs" => json!(m::split_markup_runs(text)),
            "alnum_only" => json!(a::alnum_only(text)),
            "has_alnum" => json!(a::has_alnum(text)),
            "strip_trailing_punctuation" => json!(a::strip_trailing_punctuation(text)),
            "fold_typography" => json!(a::fold_typography(text)),
            "fold_for_matching" => json!(a::fold_for_matching(text)),
            "advance_by_chars" => json!(a::advance_by_chars(
                text,
                args[1].as_u64().unwrap() as usize,
                args[2].as_u64().unwrap() as usize
            )),
            "advance_by_alnums" => json!(a::advance_by_alnums(
                text,
                args[1].as_u64().unwrap() as usize,
                args[2].as_u64().unwrap() as usize
            )),
            "merge_punct_tokens" => json!(w::merge_punct_tokens(
                &serde_json::from_value::<Vec<(String, f64)>>(args[0].clone()).unwrap()
            )),
            "resolve_sentence_tokenizer_language" => {
                json!(s::resolve_sentence_tokenizer_language(args[0].as_str()))
            }
            "match_endofsentence" => json!(s::match_endofsentence(
                text,
                call["kwargs"]["language"].as_str()
            )),
            "longest_trailing_partial_match" => json!(s::longest_trailing_partial_match(
                text,
                &serde_json::from_value::<Vec<String>>(args[1].clone()).unwrap()
            )),
            other => panic!("Unhandled utility {other}"),
        };
        assert_eq!(actual, call["result"], "{}: {call}", call["test"]);
        count += 1;
    }
    assert!(count >= 190);
}
