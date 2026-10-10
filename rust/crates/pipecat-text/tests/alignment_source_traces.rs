//! Replay unchanged source tests recorded by the developer-only oracle tool.
use pipecat_text::*;
use serde_json::{json, Value};

fn segment(s: &TextSegment) -> Value {
    json!({"original":s.original,"tts":s.tts,"original_start":s.original_start,"original_end":s.original_end})
}
fn nonempty(text: Option<&str>) -> Option<&str> {
    text.map(str::trim).filter(|s| !s.is_empty())
}
#[test]
fn original_alignment_operation_traces() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/alignment_source_traces.json")).unwrap();
    assert_eq!(fixture["source_tests_run"], 302);
    let mut failures = Vec::new();
    let mut operations = 0;
    let mut corrected_projection = 0;
    for trace in fixture["traces"].as_array().unwrap() {
        let name = trace["test"].as_str().unwrap();
        let config = &trace["config"];
        let tts = config["tts_text"].as_str().unwrap();
        let llm = config["llm_text"].as_str();
        let mut generation = 1;
        if trace["class"] == "TextSegmentMap" {
            let mut map = TextSegmentMap::new(
                tts,
                config["original_text"].as_str().unwrap(),
                llm,
                AlignmentLimits::default(),
            )
            .unwrap();
            let segments: Vec<_> = map.segments().iter().map(segment).collect();
            if json!(segments) != trace["segments"] {
                failures.push(format!(
                    "{name}: segment build\nactual={segments:?}\nexpected={}",
                    trace["segments"]
                ));
            }
            for (i, op) in trace["operations"].as_array().unwrap().iter().enumerate() {
                let word = op["args"][0].as_str().unwrap_or("");
                let result = match op["method"].as_str().unwrap() {
                    "advance_word" => {
                        map.advance_word(word).unwrap();
                        Value::Null
                    }
                    "reset" => {
                        map.reset();
                        Value::Null
                    }
                    "word_belongs_current_segment" => {
                        json!(map.word_belongs_current_segment(word).unwrap())
                    }
                    m => panic!("unknown method {m}"),
                };
                let state = json!({"raw_pos":map.raw_pos(),"user_facing_pos":map.user_facing_pos(),"llm_pos":map.llm_pos(),"llm_spoken_pos":map.llm_reported_pos(),"last_overflow":map.last_overflow(),"last_leading_duplicate":map.last_leading_duplicate(),"is_complete":map.is_complete(),"in_transformed_segment":map.in_transformed_segment(),"last_completed_segment":map.last_completed_segment().map(segment)});
                if result != op["result"] || state != op["state"] {
                    failures.push(format!(
                        "{name} op{i} {word:?}\nactual={state}\nexpected={}",
                        op["state"]
                    ));
                }
                operations += 1;
            }
        } else {
            let mut tracker = WordCompletionTracker::new(
                generation,
                tts,
                llm,
                config["user_facing_text"].as_str(),
                AlignmentLimits::default(),
            )
            .unwrap();
            for (i, op) in trace["operations"].as_array().unwrap().iter().enumerate() {
                let word = op["args"][0].as_str().unwrap_or("");
                let result = match op["method"].as_str().unwrap() {
                    "add_word_and_check_complete" => {
                        json!(tracker.add_word(generation, word).unwrap().complete)
                    }
                    "reset" => {
                        generation += 1;
                        tracker.reset(generation).unwrap();
                        Value::Null
                    }
                    "word_belongs_here" => json!(tracker.word_belongs_here(word).unwrap()),
                    m => panic!("unknown method {m}"),
                };
                let last = tracker.last_completion();
                let state = json!({"is_complete":tracker.is_complete(),"word":last.and_then(|r|r.word.as_deref()),"overflow":last.and_then(|r|r.overflow.as_deref()),"llm_consumed":nonempty(last.and_then(|r|r.source_text.as_deref())),"suppress":last.map_or(llm.is_some(),|r|r.suppress_in_context),"accumulated_user":tracker.accumulated_user_facing_text(),"remaining_user":tracker.remaining_user_facing_text(),"accumulated_tts":tracker.accumulated_tts_text(),"remaining_tts":tracker.remaining_tts_text(),"accumulated_llm":tracker.accumulated_llm_text(),"remaining_llm":nonempty(tracker.remaining_llm_text())});
                let mut expected = op["state"].clone();
                // The pinned oracle remains unchanged. Expanded TTS punctuation
                // cannot spend future source characters; see review_alignment.
                if name == "test_word_completion_tracker.TestWordCompletionTrackerTokenChangingReplacements.test_hyphenated_single_token_replacement_is_not_discarded" && i == 1 {
                    assert_eq!(word, "body-pump");
                    assert_eq!(expected["accumulated_llm"], "Try BODYPUMP ");
                    expected["accumulated_llm"] = json!("Try BODYPUMP");
                    corrected_projection += 1;
                }
                if result != op["result"] || state != expected {
                    failures.push(format!(
                        "{name} op{i} {word:?}\nactual={state}\nexpected={}",
                        expected
                    ));
                }
                operations += 1;
            }
        }
    }
    assert!(operations > 1000, "only {operations} operations");
    assert_eq!(corrected_projection, 1);
    assert!(
        failures.is_empty(),
        "{} mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
