//! Replay immutable Python source oracles using only the native public API.
//!
//! The source adapter maps implicit context names to issued native identities.
//! It asserts native stale-generation errors before comparing the source's empty
//! emission, and treats finalize-before-registration as a no-op without creating
//! an identity. Frame-only whitespace trimming does not alter native source spans.
//! All remaining comparisons use the fixture unchanged.
use pipecat_text::{
    AlignmentError, AlignmentEvidence, AlignmentLimits, ContextId, ParallelSentence,
    ParallelSentenceAggregator, SegmentId, SequencerEvent, TokenFrameSequencer, TokenReporting,
    TokenSequencerEvent, TokenSequencerLimits,
};
use serde_json::{json, Value};
use std::collections::HashMap;

fn fixture() -> Value {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/token_source_traces.json")).unwrap();
    assert_eq!(fixture["schema_version"], 1);
    assert_eq!(
        fixture["baseline"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    assert_eq!(fixture["source_tests_run"], 47);
    assert_eq!(fixture["dependencies"]["sentencex"], "1.0.32");
    fixture
}

fn sentence(value: &ParallelSentence) -> Value {
    json!({
        "tts_text": value.tts_text,
        "llm_text": value.llm_text,
        "user_facing_text": value.user_facing_text,
    })
}

fn assert_parallel_state(aggregator: &ParallelSentenceAggregator, expected: &Value, label: &str) {
    let pending = aggregator.pending();
    assert_eq!(
        pending.tts_text, expected["tts"],
        "{label}: pending synthesis"
    );
    assert_eq!(pending.llm_text, expected["llm"], "{label}: pending source");
    assert_eq!(
        pending.user_facing_text, expected["user"],
        "{label}: pending display"
    );
    assert_eq!(
        aggregator.language(),
        expected["language"],
        "{label}: language"
    );
}

#[test]
fn source_parallel_sentence_outputs_buffers_and_language() {
    let fixture = fixture();
    let mut traces = 0;
    let mut operations = 0;
    for trace in fixture["traces"].as_array().unwrap() {
        if trace["class"] != "_ParallelSentenceAggregator" {
            continue;
        }
        let name = trace["test"].as_str().unwrap();
        let mut aggregator = ParallelSentenceAggregator::new(
            trace["config"]["language"].as_str(),
            AlignmentLimits::default(),
        )
        .unwrap();
        assert_parallel_state(&aggregator, &trace["initial_state"], name);
        for (index, operation) in trace["operations"].as_array().unwrap().iter().enumerate() {
            let args = &operation["args"];
            let label = format!("{name} operation {index} {}", operation["method"]);
            let actual = match operation["method"].as_str().unwrap() {
                "aggregate" => json!(aggregator
                    .aggregate(
                        args[0].as_str().unwrap(),
                        args[1].as_str().unwrap(),
                        args[2].as_str().unwrap(),
                    )
                    .unwrap()
                    .iter()
                    .map(sentence)
                    .collect::<Vec<_>>()),
                "flush" => aggregator.flush().as_ref().map_or(Value::Null, sentence),
                "handle_interruption" => {
                    aggregator.interrupt();
                    Value::Null
                }
                "set_language" => {
                    aggregator.set_language(args[0].as_str()).unwrap();
                    Value::Null
                }
                method => panic!("unknown source method {method}"),
            };
            assert_eq!(actual, operation["result"], "{label}: output");
            assert_parallel_state(&aggregator, &operation["state"], &label);
            operations += 1;
        }
        traces += 1;
    }
    assert_eq!((traces, operations), (23, 87));
}

fn argument<'a>(operation: &'a Value, position: usize, keyword: &str) -> &'a Value {
    operation["args"]
        .get(position)
        .unwrap_or(&operation["kwargs"][keyword])
}

fn optional_nonempty(text: &str) -> Option<&str> {
    (!text.is_empty()).then_some(text)
}

/// Map the source's implicit string contexts to explicit, generation-scoped IDs.
struct Replay {
    sequencer: TokenFrameSequencer,
    contexts: HashMap<String, ContextId>,
    names: HashMap<ContextId, String>,
    anchors: HashMap<SegmentId, u64>,
    skipped: HashMap<SegmentId, Value>,
    stale_rejections: usize,
}

impl Replay {
    fn new() -> Self {
        Self {
            sequencer: TokenFrameSequencer::new(1, TokenSequencerLimits::default()).unwrap(),
            contexts: HashMap::new(),
            names: HashMap::new(),
            anchors: HashMap::new(),
            skipped: HashMap::new(),
            stale_rejections: 0,
        }
    }

    fn register_context(&mut self, name: &str, append: bool, language: Option<&str>) -> ContextId {
        if let Some(&id) = self.contexts.get(name) {
            if id.generation() == self.sequencer.generation() {
                self.sequencer.set_language(id, language).unwrap();
                return id;
            }
        }
        let id = self
            .sequencer
            .open_context(append, TokenReporting::WordTimestamps, language)
            .unwrap();
        self.contexts.insert(name.into(), id);
        self.names.insert(id, name.into());
        id
    }

    fn source_emission(
        &mut self,
        context: ContextId,
        result: Result<Vec<TokenSequencerEvent>, AlignmentError>,
    ) -> Vec<TokenSequencerEvent> {
        if context.generation() != self.sequencer.generation() {
            assert_eq!(result, Err(AlignmentError::StaleGeneration));
            self.stale_rejections += 1;
            Vec::new()
        } else {
            result.unwrap()
        }
    }

    fn operation(&mut self, operation: &Value) -> Value {
        let events = match operation["method"].as_str().unwrap() {
            "register_spoken" => {
                let frame = argument(operation, 0, "frame");
                let name = argument(operation, 1, "context_id").as_str().unwrap();
                let tts = argument(operation, 2, "tts_text").as_str().unwrap();
                let append = argument(operation, 3, "append_to_context")
                    .as_bool()
                    .unwrap();
                let language = argument(operation, 6, "language").as_str();
                let id = self.register_context(name, append, language);
                let user = frame["text"].as_str().unwrap();
                let llm = frame["raw_text"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .unwrap_or(user);
                self.sequencer.register_token(id, tts, user, llm).unwrap()
            }
            "register_skipped" => {
                let frame = argument(operation, 0, "frame");
                let name = argument(operation, 1, "context_id").as_str().unwrap();
                let destination = argument(operation, 2, "transport_destination").as_str();
                let context = self.register_context(name, true, None);
                let (segment, events) = self
                    .sequencer
                    .register_skipped(
                        context,
                        frame["text"].as_str().unwrap(),
                        frame["raw_text"].as_str().unwrap_or(""),
                        destination,
                    )
                    .unwrap();
                self.skipped.insert(segment, frame.clone());
                events
            }
            "process_word" => {
                let word = argument(operation, 0, "word").as_str().unwrap();
                let pts = argument(operation, 1, "pts").as_u64().unwrap();
                let name = argument(operation, 2, "context_id").as_str().unwrap();
                let spaces = argument(operation, 3, "includes_inter_frame_spaces")
                    .as_bool()
                    .unwrap_or(false);
                let context = self.contexts[name];
                let result = self.sequencer.process_word(context, word, pts, spaces);
                self.source_emission(context, result)
            }
            "finalize" => {
                let name = argument(operation, 0, "context_id").as_str().unwrap();
                if let Some(&context) = self.contexts.get(name) {
                    let result = self.sequencer.finalize(context);
                    self.source_emission(context, result)
                } else {
                    // No native identity exists before the first registration.
                    Vec::new()
                }
            }
            "force_complete" => {
                let name = argument(operation, 0, "context_id").as_str().unwrap();
                let pts = argument(operation, 1, "last_word_pts").as_u64().unwrap();
                self.sequencer
                    .close_context(self.contexts[name], pts)
                    .unwrap()
            }
            "clear" => {
                self.sequencer.interrupt().unwrap();
                return Value::Null;
            }
            method => panic!("unsupported tracked TOKEN operation {method}"),
        };
        self.frames(events, &operation["result"])
    }

    fn frames(&mut self, events: Vec<TokenSequencerEvent>, expected: &Value) -> Value {
        let mut frames = Vec::new();
        for event in events {
            let value = match event {
                TokenSequencerEvent::Promoted {
                    context,
                    segment,
                    sentence,
                } => {
                    let source_id = expected[frames.len()]["id"].as_u64().unwrap();
                    assert!(self.anchors.insert(segment, source_id).is_none());
                    json!({
                        "class": "AggregatedTextFrame", "pts": null,
                        "transport_destination": null, "text": sentence.user_facing_text,
                        "aggregated_by": "sentence", "context_id": self.names[&context],
                        "raw_text": optional_nonempty(&sentence.llm_text),
                        "will_be_spoken": true, "append_to_context": false,
                        "includes_inter_frame_spaces": false,
                    })
                }
                TokenSequencerEvent::Progress {
                    context,
                    segment,
                    pts_ns,
                    text,
                    accumulated_text,
                    remaining_text,
                    evidence,
                } => {
                    assert!(matches!(
                        evidence,
                        AlignmentEvidence::ProviderReport
                            | AlignmentEvidence::Resynchronized
                            | AlignmentEvidence::UnreportedRemainder
                    ));
                    json!({
                        "class": "AggregatedTextProgressFrame", "pts": pts_ns,
                        "transport_destination": null, "text": text,
                        "aggregated_by": "sentence", "context_id": self.names[&context],
                        "segment_id": self.anchors[&segment],
                        "accumulated_text": accumulated_text, "remaining_text": remaining_text,
                    })
                }
                TokenSequencerEvent::Alignment(SequencerEvent::Word {
                    context,
                    segment,
                    pts_ns,
                    completion,
                    append_to_context,
                    includes_inter_frame_spaces,
                }) => {
                    assert!(matches!(
                        completion.evidence,
                        AlignmentEvidence::ProviderReport | AlignmentEvidence::Resynchronized
                    ));
                    assert!(self.anchors.contains_key(&segment));
                    source_word_frame(
                        &self.names[&context],
                        pts_ns,
                        &completion,
                        append_to_context,
                        includes_inter_frame_spaces,
                    )
                }
                TokenSequencerEvent::Alignment(SequencerEvent::Unreported {
                    context,
                    segment,
                    pts_ns,
                    completion,
                    append_to_context,
                    includes_inter_frame_spaces,
                }) => {
                    assert_eq!(completion.evidence, AlignmentEvidence::UnreportedRemainder);
                    assert!(self.anchors.contains_key(&segment));
                    source_word_frame(
                        &self.names[&context],
                        pts_ns,
                        &completion,
                        append_to_context,
                        includes_inter_frame_spaces,
                    )
                }
                TokenSequencerEvent::Alignment(SequencerEvent::Skipped {
                    context,
                    segment,
                    text,
                    raw_text,
                    transport_destination,
                    pts_ns,
                }) => {
                    let source = &self.skipped[&segment];
                    json!({
                        "class": "AggregatedTextFrame", "pts": pts_ns,
                        "transport_destination": transport_destination, "text": text,
                        "aggregated_by": source["aggregated_by"],
                        "context_id": self.names[&context],
                        "raw_text": optional_nonempty(&raw_text),
                        "will_be_spoken": false, "append_to_context": true,
                        "includes_inter_frame_spaces": source["includes_inter_frame_spaces"],
                    })
                }
                TokenSequencerEvent::Alignment(SequencerEvent::UnmatchedWord { .. }) => {
                    panic!("source trace unexpectedly exposed an unmatched-word diagnostic")
                }
            };
            frames.push(value);
        }
        json!(frames)
    }
}

fn source_word_frame(
    context: &str,
    pts: u64,
    completion: &pipecat_text::WordCompletion,
    append: bool,
    spaces: bool,
) -> Value {
    // The native source span is lossless; the Python frame accessor trims it.
    let raw = completion
        .source_text
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    json!({
        "class": "TTSTextFrame", "pts": pts, "transport_destination": null,
        "text": completion.word.as_deref().unwrap(), "aggregated_by": "word",
        "context_id": context, "raw_text": raw, "will_be_spoken": false,
        "append_to_context": append && !completion.suppress_in_context,
        "includes_inter_frame_spaces": spaces,
    })
}

fn expected_frames(value: &Value) -> Value {
    if value.is_null() {
        return Value::Null;
    }
    let mut value = value.clone();
    for frame in value.as_array_mut().unwrap() {
        // Ephemeral frame IDs differ; stable progress-to-anchor identity is checked.
        frame.as_object_mut().unwrap().remove("id");
    }
    value
}

#[test]
fn source_tracked_token_sequencer_events_and_queue_counts() {
    let fixture = fixture();
    let mut traces = 0;
    let mut operations = 0;
    let mut excluded_non_streaming = 0;
    let mut excluded_implicit_completion = 0;
    let mut stale_rejections = 0;
    let mut failures = Vec::new();
    for trace in fixture["traces"].as_array().unwrap() {
        if trace["class"] != "AggregatedFrameSequencer" {
            continue;
        }
        if trace["config"]["streaming"] != true {
            excluded_non_streaming += 1;
            continue;
        }
        if trace["operations"]
            .as_array()
            .unwrap()
            .iter()
            .any(|operation| {
                operation["method"] == "register_spoken"
                    && argument(operation, 4, "build_tracker") == false
            })
        {
            // Native completion mode requires an explicit synthesis-completion event.
            excluded_implicit_completion += 1;
            continue;
        }
        let name = trace["test"].as_str().unwrap();
        let mut replay = Replay::new();
        for (index, operation) in trace["operations"].as_array().unwrap().iter().enumerate() {
            let label = format!("{name} operation {index} {}", operation["method"]);
            let actual = replay.operation(operation);
            let expected = expected_frames(&operation["result"]);
            if actual != expected {
                failures.push(format!("{label}\nactual={actual}\nexpected={expected}"));
            }
            let state = &operation["state"];
            assert_eq!(
                replay.sequencer.pending_slots(),
                state["slots"].as_array().unwrap().len(),
                "{label}: pending slots"
            );
            assert_eq!(
                replay.sequencer.buffered_words(),
                state["buffered_words"].as_array().unwrap().len(),
                "{label}: buffered words"
            );
            let bytes: usize = state["buffered_words"]
                .as_array()
                .unwrap()
                .iter()
                .map(|word| word["word"].as_str().unwrap().len())
                .sum();
            assert_eq!(
                replay.sequencer.buffered_word_bytes(),
                bytes,
                "{label}: buffered bytes"
            );
            operations += 1;
        }
        stale_rejections += replay.stale_rejections;
        traces += 1;
    }
    assert_eq!(excluded_non_streaming, 3);
    assert_eq!(excluded_implicit_completion, 1);
    assert_eq!(traces, 28);
    assert_eq!(operations, 121);
    assert_eq!(stale_rejections, 4);
    assert!(
        failures.is_empty(),
        "{} event mismatches:\n{}",
        failures.len(),
        failures.join("\n")
    );
}
