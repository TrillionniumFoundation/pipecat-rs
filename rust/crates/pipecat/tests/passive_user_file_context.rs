//! Public PythonJsonCodec passive-file consumer regressions.
//! The exact source fixture is UPSTREAM; no RTVI frame is reinterpreted here.
use pipecat::{
    core::*,
    network_bus::PythonJsonCodec,
    services::{Context, Message, Role},
    turns::{
        AggregationAction, AggregationEvent, AggregationOutput, AssistantStoppedReceiver,
        AssistantTurnStopped, LLMAssistantAggregator, SharedContext,
    },
    workers::MessageData,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

const PDF_BASE64: &str = "JVBERi0xLjQKMSAwIG9iago8PCAvVHlwZSAvQ2F0YWxvZyAvUGFnZXMgMiAwIFIgPj4KZW5kb2JqCjIgMCBvYmoKPDwgL1R5cGUgL1BhZ2VzIC9LaWRzIFszIDAgUl0gL0NvdW50IDEgPj4KZW5kb2JqCjMgMCBvYmoKPDwgL1R5cGUgL1BhZ2UgL1BhcmVudCAyIDAgUiAvTWVkaWFCb3ggWzAgMCA3MiA3Ml0gL1Jlc291cmNlcyA8PCA+PiAvQ29udGVudHMgNCAwIFIgPj4KZW5kb2JqCjQgMCBvYmoKPDwgL0xlbmd0aCAwID4+CnN0cmVhbQoKZW5kc3RyZWFtCmVuZG9iagp4cmVmCjAgNQowMDAwMDAwMDAwIDY1NTM1IGYgCjAwMDAwMDAwMDkgMDAwMDAgbiAKMDAwMDAwMDA1OCAwMDAwMCBuIAowMDAwMDAwMTE1IDAwMDAwIG4gCjAwMDAwMDAyMTcgMDAwMDAgbiAKdHJhaWxlcgo8PCAvU2l6ZSA1IC9Sb290IDEgMCBSID4+CnN0YXJ0eHJlZgoyNjYKJSVFT0YK";

fn source_fixture() -> Value {
    let fixtures: Vec<Value> = serde_json::from_str(include_str!(
        "../../pipecat-network-bus/tests/fixtures/python_frames.json"
    ))
    .unwrap();
    fixtures
        .into_iter()
        .find(|value| {
            value["__data__"]["frame"]["__type__"] == "pipecat.frames.frames.UserFileRawFrame"
        })
        .expect("pinned public UserFileRawFrame fixture")
}
fn decode(value: Value) -> DirectedFrame {
    let envelope = PythonJsonCodec::default()
        .decode(&serde_json::to_vec(&value).unwrap())
        .expect("public codec must decode the source-shaped fixture");
    let MessageData::Frame {
        frame, direction, ..
    } = &envelope.as_bus_message().unwrap().data
    else {
        panic!("source fixture must decode to a frame")
    };
    DirectedFrame::new((**frame).clone(), *direction)
}
fn valid_passive() -> DirectedFrame {
    // A separate source-shaped public codec input isolates the falsey-append
    // decision from invalid PDF bytes and custom options in the exact fixture.
    let mut fixture = source_fixture();
    let fields = &mut fixture["__data__"]["frame"]["__data__"];
    fields["file"] = json!(format!("data:application/pdf;base64,{PDF_BASE64}"));
    fields["format"] = json!("application/pdf");
    fields["append_to_context"] = Value::Null;
    fields["run_llm"] = json!(true);
    fields["custom_options"] = Value::Null;
    decode(fixture)
}
fn consume(
    assistant: &LLMAssistantAggregator,
    frame: Frame,
) -> Result<AggregationOutput, ProcessorError> {
    assistant.process_at(frame, Direction::Downstream, Duration::ZERO)
}
fn text(value: &str) -> Frame {
    Frame::new(FrameData::LlmText(TextFrame::llm(value)))
}
fn silent(output: &AggregationOutput) {
    assert!(
        output.actions.is_empty(),
        "unexpected actions: {:?}",
        output.actions
    );
    assert!(
        output.events.is_empty(),
        "unexpected events: {:?}",
        output.events
    );
}
fn admit(output: &Result<AggregationOutput, ProcessorError>) -> Result<(), ProcessorError> {
    if let Ok(output) = output {
        for action in &output.actions {
            if let AggregationAction::AssistantStopped(action) = action {
                action.admit()?;
            }
        }
    }
    Ok(())
}
fn records(receiver: &mut AssistantStoppedReceiver) -> Vec<Arc<AssistantTurnStopped>> {
    let mut records = vec![];
    while let Some(record) = receiver.try_recv() {
        records.push(record.completion().clone());
    }
    records
}
fn end_effects(output: &AggregationOutput, record: &AssistantTurnStopped, expected: &str) {
    assert_eq!(
        output.events,
        vec![AggregationEvent::AssistantTurnStopped {
            text: expected.into(),
            interrupted: false,
        }]
    );
    assert_eq!(record.text(), expected);
    assert_eq!(record.turn_id(), 1);
    assert!(!record.interrupted());
    assert_eq!(
        output
            .actions
            .iter()
            .filter(|action| { matches!(action, AggregationAction::AssistantStopped(_)) })
            .count(),
        1
    );
    let frames: Vec<_> = output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame) => Some(frame),
            _ => None,
        })
        .collect();
    assert_eq!(frames.len(), 4);
    assert!(matches!(
        frames[0].frame.data,
        FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
    ));
    assert_eq!(frames[0].direction, Direction::Downstream);
    assert!(matches!(
        frames[1].frame.data,
        FrameData::LlmContextAssistantTimestamp { .. }
    ));
    assert_eq!(frames[1].direction, Direction::Downstream);
    for (frame, direction) in frames[2..]
        .iter()
        .zip([Direction::Downstream, Direction::Upstream])
    {
        assert_eq!(frame.direction, direction);
        let FrameData::LlmContextAssistantTurn(turn) = &frame.frame.data else {
            panic!("missing final assistant turn")
        };
        assert_eq!(turn.text, expected);
        assert_eq!(turn.timestamp, record.started_at());
    }
}

#[tokio::test]
async fn exact_upstream_python_fixture_is_silent_and_preserves_existing_history() {
    let produced = decode(source_fixture());
    let direction = produced.direction;
    let data = produced.frame.data.clone();
    let context = SharedContext::new(Context::new(vec![Message::text(Role::User, "existing")]));
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let before = context.snapshot();
    let before_rich = context.snapshot_rich();
    let accepted = assistant.process_at(produced.frame, direction, Duration::ZERO);
    let after = context.snapshot();
    let after_rich = context.snapshot_rich();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    assert_eq!(direction, Direction::Upstream);
    let FrameData::UserFileDetailed(file) = data else {
        panic!("expected actual decoded detailed file")
    };
    assert_eq!(file.append_to_context, Some(false));
    assert_eq!(file.run_llm, Some(false));
    assert_eq!(file.file.format, "RGBA");
    assert_eq!(file.file.file, "data:application/pdf;base64,AAE=");
    assert!(file.custom_options.is_some());
    silent(&accepted.expect("exact public-codec passive file must be silently consumed"));
    assert_eq!(before.unwrap(), after.unwrap());
    assert_eq!(before_rich.unwrap(), after_rich.unwrap());
}

#[tokio::test]
async fn decoded_passive_file_preserves_one_full_text_and_normal_turn_lifecycle() {
    for with_file in [false, true] {
        let produced = valid_passive();
        let direction = produced.direction;
        let data = produced.frame.data.clone();
        let context = SharedContext::new(Context::new(vec![Message::text(Role::User, "existing")]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let before = context.snapshot();
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = consume(&assistant, text("A"));
        let passive =
            with_file.then(|| assistant.process_at(produced.frame, direction, Duration::ZERO));
        let during = context.snapshot();
        let premature = records(&mut receiver);
        let during_stats = receiver.stats();
        let suffix = consume(&assistant, text("B"));
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let admitted = admit(&end);
        let duplicate_end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let again = admit(&duplicate_end);
        let completed = records(&mut receiver);
        let after = context.snapshot();
        let terminal = consume(&assistant, Frame::end());
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        terminal.unwrap();
        assert_eq!(direction, Direction::Upstream);
        let FrameData::UserFileDetailed(file) = data else {
            panic!("expected actual decoded detailed file")
        };
        assert_eq!(file.append_to_context, None);
        assert_eq!(file.run_llm, Some(true));
        assert_eq!(file.file.format, "application/pdf");
        assert_eq!(
            file.file.file,
            format!("data:application/pdf;base64,{PDF_BASE64}")
        );
        assert!(file.custom_options.is_none());
        assert_eq!(
            start.unwrap().events,
            vec![AggregationEvent::AssistantTurnStarted]
        );
        silent(&prefix.unwrap());
        if let Some(passive) = passive {
            silent(
                &passive
                    .expect("passive file in an ordinary open response must preserve held text"),
            );
        }
        assert_eq!(before.unwrap(), during.unwrap());
        assert!(premature.is_empty());
        assert_eq!(during_stats.reserved, 0);
        silent(&suffix.unwrap());
        admitted.unwrap();
        again.unwrap();
        silent(&duplicate_end.unwrap());
        assert_eq!(completed.len(), 1);
        end_effects(&end.unwrap(), &completed[0], "AB");
        assert_eq!(
            after.unwrap().messages,
            vec![
                Message::text(Role::User, "existing"),
                Message::text(Role::Assistant, "AB"),
            ]
        );
    }
}

// Candidate-only finite controls. All preceding baseline bytes are frozen.
fn payload(frame: &mut Frame) -> &mut DetailedUserFileRawFrame {
    let FrameData::UserFileDetailed(file) = &mut frame.data else {
        panic!("not a detailed user file")
    };
    file
}
fn passive_file(append: Option<bool>, run: Option<bool>) -> Frame {
    Frame::new(FrameData::UserFileDetailed(DetailedUserFileRawFrame {
        file: FileRawFrame {
            file: format!("data:application/pdf;base64,{PDF_BASE64}"),
            source_type: FileSourceType::Bytes,
            filename: Some("document.pdf".into()),
            format: "application/pdf".into(),
        },
        user_id: "ordinary-user".into(),
        text: "ignored passive text".into(),
        append_to_context: append,
        custom_options: None,
        run_llm: run,
    }))
}
fn active_file() -> Frame {
    passive_file(Some(true), Some(false))
}

#[tokio::test]
async fn falsey_append_ignores_all_run_values_without_flushing_any_prefix() {
    for append in [None, Some(false)] {
        for run in [None, Some(false), Some(true)] {
            for direction in [Direction::Downstream, Direction::Upstream] {
                for prefix in [None, Some(""), Some(" \n\t "), Some(" A ")] {
                    let context = SharedContext::default();
                    let assistant =
                        LLMAssistantAggregator::new(context.clone(), Default::default());
                    let start = prefix
                        .map(|_| consume(&assistant, Frame::new(FrameData::LlmResponseStart)));
                    let held = prefix.map(|value| consume(&assistant, text(value)));
                    let mut file = passive_file(append, run);
                    payload(&mut file).file.file.clear();
                    payload(&mut file).file.format.clear();
                    payload(&mut file).custom_options = Some(Default::default());
                    let accepted = assistant.process_at(file, direction, Duration::ZERO);
                    let during = context.snapshot();
                    let suffix = prefix.map(|_| consume(&assistant, text("B")));
                    let end =
                        prefix.map(|_| consume(&assistant, Frame::new(FrameData::LlmResponseEnd)));
                    let after = context.snapshot();
                    let terminal = consume(&assistant, Frame::end());
                    let cleaned = assistant.cleanup().await;
                    drop(assistant);
                    cleaned.unwrap();
                    terminal.unwrap();
                    if let Some(start) = start {
                        start.unwrap();
                    }
                    if let Some(held) = held {
                        silent(&held.unwrap());
                    }
                    silent(&accepted.unwrap());
                    assert!(during.unwrap().messages.is_empty());
                    if let Some(suffix) = suffix {
                        silent(&suffix.unwrap());
                    }
                    let expected = prefix.map(|prefix| format!("{prefix}B").trim().to_owned());
                    if let Some(end) = end {
                        assert_eq!(
                            end.unwrap().events,
                            vec![AggregationEvent::AssistantTurnStopped {
                                text: expected.clone().unwrap(),
                                interrupted: false,
                            }]
                        );
                    }
                    assert_eq!(
                        after.unwrap().messages,
                        expected
                            .into_iter()
                            .map(|text| Message::text(Role::Assistant, text))
                            .collect::<Vec<_>>()
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn passive_payload_fields_are_unused_and_do_not_claim_the_active_retry_id() {
    for kind in [
        "empty",
        "unknown-mime",
        "url",
        "bad-base64",
        "wrong-signature",
        "overcap",
        "custom",
    ] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut file = passive_file(Some(false), Some(true));
        let data = payload(&mut file);
        match kind {
            "empty" => {
                data.file.file.clear();
                data.file.format.clear();
            }
            "unknown-mime" => data.file.format = "unrecognized/type".into(),
            "url" => {
                data.file.source_type = FileSourceType::Url;
                data.file.file = "https://example.invalid/never-retrieve".into();
            }
            "bad-base64" => data.file.file = "data:application/pdf;base64,%%%".into(),
            "wrong-signature" => data.file.file = "data:application/pdf;base64,AAAA".into(),
            "overcap" => data.file.file = "x".repeat(11_184_841),
            _ => {
                data.custom_options = Some(
                    [
                        ("function_name".into(), json!("not-authority")),
                        ("tool_call_id".into(), json!("not-owned")),
                        ("result_callback".into(), json!({"unused": true})),
                    ]
                    .into(),
                );
                data.file.filename = Some("\0ignored filename".into());
                data.text = "\0ignored text".into();
            }
        }
        let id = file.id;
        let accepted = consume(&assistant, file.clone());
        let repeated = consume(&assistant, file);
        let before = context.snapshot_rich();
        let mut corrected = active_file();
        corrected.id = id;
        let active = consume(&assistant, corrected.clone());
        let replay = consume(&assistant, corrected);
        let after = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        for output in [accepted, repeated, active, replay] {
            silent(&output.unwrap());
        }
        assert!(before.unwrap().messages.is_empty(), "{kind}");
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 1, "{kind}");
        let MessageContent::Parts(parts) = &after.messages[0].content else {
            panic!("active file")
        };
        assert_eq!(
            parts,
            &vec![
                MessagePart::Text {
                    text: "ignored passive text".into()
                },
                MessagePart::File {
                    data: PDF_BASE64.into(),
                    mime_type: "application/pdf".into(),
                    filename: Some("document.pdf".into())
                },
            ]
        );
    }
}

#[tokio::test]
async fn failed_active_then_passive_then_corrected_active_preserves_validation_and_replay() {
    for invalid in ["bytes", "mime", "options"] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut bad = active_file();
        match invalid {
            "bytes" => payload(&mut bad).file.file.clear(),
            "mime" => payload(&mut bad).file.format = "text/plain".into(),
            _ => payload(&mut bad).custom_options = Some(Default::default()),
        }
        let id = bad.id;
        let rejected = consume(&assistant, bad.clone());
        let mut passive = bad.clone();
        payload(&mut passive).append_to_context = None;
        payload(&mut passive).run_llm = Some(true);
        let ignored = consume(&assistant, passive.clone());
        let before = context.snapshot_rich();
        let mut corrected = active_file();
        corrected.id = id;
        let accepted = consume(&assistant, corrected.clone());
        let after = context.snapshot_rich();
        let invalid_duplicate = consume(&assistant, bad);
        let ignored_after_success = consume(&assistant, passive);
        let replay = consume(&assistant, corrected);
        let final_context = context.snapshot_rich();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        assert!(rejected.is_err(), "{invalid}");
        assert!(
            invalid_duplicate.is_err(),
            "active validation must precede cache"
        );
        for output in [ignored, accepted, ignored_after_success, replay] {
            silent(&output.unwrap());
        }
        assert!(before.unwrap().messages.is_empty());
        let after = after.unwrap();
        assert_eq!(after.messages.len(), 1);
        assert_eq!(after, final_context.unwrap());
    }
}

#[tokio::test]
async fn repeated_passive_id_preserves_spacing_and_full_turn_after_successful_content() {
    for includes_spaces in [false, true] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let active = active_file();
        let admitted_file = consume(&assistant, active.clone());
        let initial = context.snapshot();
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let mut passive = active;
        payload(&mut passive).append_to_context = None;
        payload(&mut passive).file.file.clear();
        let mut outputs = vec![];
        let mut snapshots = vec![];
        for value in ["A", "B", "C"] {
            let mut part = TextFrame::llm(value);
            part.includes_inter_frame_spaces = includes_spaces;
            outputs.push(consume(&assistant, Frame::new(FrameData::LlmText(part))));
            outputs.push(consume(&assistant, passive.clone()));
            snapshots.push(context.snapshot());
        }
        let premature = records(&mut receiver);
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let admitted = admit(&end);
        let completed = records(&mut receiver);
        let after = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        silent(&admitted_file.unwrap());
        start.unwrap();
        for output in outputs {
            silent(&output.unwrap());
        }
        let mut expected = initial.unwrap();
        for snapshot in snapshots {
            assert_eq!(snapshot.unwrap(), expected);
        }
        assert!(premature.is_empty());
        admitted.unwrap();
        assert_eq!(completed.len(), 1);
        let full = if includes_spaces { "ABC" } else { "A B C" };
        end_effects(&end.unwrap(), &completed[0], full);
        expected.messages.push(Message::text(Role::Assistant, full));
        assert_eq!(after.unwrap(), expected);
    }
}

#[tokio::test]
async fn passive_unique_ids_do_not_evict_successful_content_from_bounded_replay_cache() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let active = active_file();
    let accepted = consume(&assistant, active.clone());
    let before = context.snapshot();
    let mut passives = vec![];
    for _ in 0..513 {
        passives.push(consume(&assistant, passive_file(None, None)));
    }
    let replay = consume(&assistant, active);
    let after = context.snapshot();
    let cleaned = assistant.cleanup().await;
    drop(assistant);
    cleaned.unwrap();
    silent(&accepted.unwrap());
    for passive in passives {
        silent(&passive.unwrap());
    }
    silent(&replay.unwrap());
    assert_eq!(before.unwrap(), after.unwrap());
}

#[tokio::test]
async fn passive_admission_does_not_project_native_history_or_flush_held_text() {
    use pipecat::services::{Content, ContentPart};
    for busy in [false, true] {
        let prior = Message {
            role: Role::User,
            content: Some(Content::Parts(vec![ContentPart::InlineData {
                mime_type: "application/pdf".into(),
                data: "JVBERg==".into(),
            }])),
            name: None,
            tool_calls: vec![],
            tool_call_id: None,
        };
        let context = SharedContext::new(Context::new(vec![prior]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let start = busy.then(|| consume(&assistant, Frame::new(FrameData::LlmResponseStart)));
        let held = busy.then(|| consume(&assistant, text("A")));
        let before = context.snapshot();
        let before_projection = context.frame_data();
        let accepted = consume(&assistant, passive_file(None, Some(true)));
        let after = context.snapshot();
        let after_projection = context.frame_data();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        if let Some(start) = start {
            start.unwrap();
        }
        if let Some(held) = held {
            silent(&held.unwrap());
        }
        silent(&accepted.unwrap());
        assert_eq!(before.unwrap(), after.unwrap());
        assert_eq!(
            before_projection.unwrap_err(),
            after_projection.unwrap_err()
        );
    }
}

#[tokio::test]
async fn passive_file_preserves_marker_and_original_job_turn_identity() {
    for with_file in [false, true] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let correlation = assistant.bind_job_generation(73).unwrap();
        let mut receiver = assistant.subscribe_stopped(Default::default()).unwrap();
        let start = consume(&assistant, Frame::new(FrameData::LlmResponseStart));
        let prefix = consume(&assistant, text("A"));
        let marker = consume(
            &assistant,
            Frame::new(FrameData::LlmMarker(LlmMarkerFrame {
                marker: "<DONE>".into(),
                append_to_context_immediately: false,
            })),
        );
        let passive = with_file.then(|| consume(&assistant, passive_file(None, Some(true))));
        let during = context.snapshot();
        let premature = records(&mut receiver);
        let suffix = consume(&assistant, text("B<DONE>"));
        let end = consume(&assistant, Frame::new(FrameData::LlmResponseEnd));
        let admitted = admit(&end);
        let completed = records(&mut receiver);
        let after = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        start.unwrap();
        for output in [prefix, marker, suffix] {
            silent(&output.unwrap());
        }
        if let Some(passive) = passive {
            silent(&passive.unwrap());
        }
        assert!(during.unwrap().messages.is_empty());
        assert!(premature.is_empty());
        admitted.unwrap();
        assert_eq!(completed.len(), 1);
        assert!(correlation.matches(&completed[0]));
        assert_eq!(completed[0].job_generation(), Some(73));
        assert_eq!(
            after.unwrap().messages,
            vec![Message::text(Role::Assistant, "A <DONE> B<DONE>")]
        );
        end_effects(&end.unwrap(), &completed[0], "A  B");
    }
}

#[tokio::test]
async fn passive_files_preserve_all_protected_activity_and_inference_debt_gates() {
    use pipecat::turns::{AssistantAggregatorConfig, AssistantTextMode};
    use serde_json::json;
    for gate in [
        "spoken",
        "tts-only",
        "thought",
        "thought-append",
        "user",
        "bot",
        "awaited",
        "playback",
        "tool",
        "run",
        "tool-run",
    ] {
        for append in [None, Some(false)] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(
                context.clone(),
                AssistantAggregatorConfig {
                    text_mode: if matches!(gate, "spoken" | "playback") {
                        AssistantTextMode::Spoken
                    } else {
                        AssistantTextMode::Text
                    },
                },
            );
            let mut setup = vec![consume(
                &assistant,
                Frame::new(if gate == "tts-only" {
                    FrameData::TtsStarted(Default::default())
                } else {
                    FrameData::LlmResponseStart
                }),
            )];
            let mut prefix = TextFrame::llm("A");
            prefix.skip_tts = Some(true);
            setup.push(consume(&assistant, Frame::new(FrameData::LlmText(prefix))));
            let prelude = match gate {
                "thought" => Some(Frame::new(FrameData::LlmThoughtText(TextFrame::llm(
                    "held thought",
                )))),
                "thought-append" => Some(Frame::new(FrameData::LlmThoughtStartDetailed(
                    LlmThoughtStartFrame {
                        append_to_context: true,
                        llm: Some("fixture".into()),
                    },
                ))),
                "user" => Some(Frame::new(FrameData::UserStartedSpeaking)),
                "bot" => Some(Frame::new(FrameData::BotStartedSpeaking)),
                "awaited" => {
                    let mut f = Frame::new(FrameData::UserStoppedSpeaking);
                    f.metadata
                        .insert("user_context_follows".into(), json!(true));
                    Some(f)
                }
                "tool" | "tool-run" => Some(Frame::new(FrameData::FunctionCallInProgress(
                    FunctionCallInProgressFrame {
                        function_name: "pending".into(),
                        tool_call_id: "pending".into(),
                        arguments: json!({}),
                        cancel_on_interruption: false,
                        group_id: None,
                    },
                ))),
                "run" => Some(Frame::new(FrameData::LlmRun)),
                _ => None,
            };
            if let Some(prelude) = prelude {
                setup.push(consume(&assistant, prelude));
            }
            if gate == "tool-run" {
                setup.push(consume(
                    &assistant,
                    Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
                        function_name: "pending".into(),
                        tool_call_id: "pending".into(),
                        arguments: json!({}),
                        result: json!("done"),
                        run_llm: Some(true),
                        is_final: true,
                        error: None,
                    })),
                ));
            }
            let playback = (gate == "playback").then(|| assistant.begin_playback("pending"));
            let before = context.snapshot_rich();
            let pending_before = assistant.has_function_calls_in_progress();
            let mut rejected = vec![];
            for run in [None, Some(true), Some(false)] {
                let image = passive_file(append, run);
                rejected.push(consume(&assistant, image));
            }
            let after = context.snapshot_rich();
            let pending_after = assistant.has_function_calls_in_progress();
            let debt_end = matches!(gate, "run" | "tool-run")
                .then(|| consume(&assistant, Frame::new(FrameData::LlmResponseEnd)));
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            for result in setup {
                result.unwrap();
            }
            if let Some(playback) = playback {
                playback.unwrap();
            }
            for result in rejected {
                assert!(
                    result.unwrap_err().message.contains("idle assistant"),
                    "{gate}"
                );
            }
            assert_eq!(before.unwrap(), after.unwrap(), "{gate}");
            assert_eq!(pending_before.unwrap(), pending_after.unwrap(), "{gate}");
            if let Some(end) = debt_end {
                let end = end.unwrap();
                assert_eq!(
                    end.events,
                    vec![AggregationEvent::AssistantTurnStopped {
                        text: "A".into(),
                        interrupted: false
                    }]
                );
                assert_eq!(end.actions.iter().filter(|action| matches!(action,
                    AggregationAction::Push(frame) if frame.direction == Direction::Upstream && matches!(frame.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_))
                )).count(), 1, "{gate} debt must remain owed");
            }
        }
    }
}

#[tokio::test]
async fn passive_files_preserve_private_carrier_signed_owner_and_realtime_guards() {
    use pipecat::{
        services::Context,
        turns::{LLMContextAggregatorPair, RealtimeServiceMode},
    };
    use serde_json::json;
    for append in [None, Some(false)] {
        let mut continuation = Frame::new(FrameData::LlmResponseStart);
        continuation
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(994201, LlmContinuationProvider::Gemini).unwrap(),
            ))
            .unwrap();
        let call = FunctionCall {
            function_name: "capture".into(),
            tool_call_id: "owned".into(),
            arguments: json!({}),
        };
        let mut tool = Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()]));
        tool.set_native_tool_batch(
            NativeToolCallBatch::new("owned-group".into(), vec![(Arc::new(call), false)]).unwrap(),
        )
        .unwrap();
        let source = EagerSourceSessionOwner::new(4096).unwrap();
        let mut eager = Frame::new(FrameData::ProposedUserStartedSpeaking);
        eager
            .set_native_eager_source(NativeEagerSource::Start(source.begin_turn().unwrap()))
            .unwrap();
        let owner = SpeculationOwner::new();
        let mut speculation = active_file();
        speculation
            .set_native_speculation(owner.capability())
            .unwrap();
        let producer = NativeRealtimeContextProducer::default();
        let mut realtime = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![],
                tools: ToolSet::NotGiven,
                tool_choice: None,
            },
            speculation: false,
        }));
        producer
            .attach(&mut realtime, RealtimeContextIntent::Run, true)
            .unwrap();
        for mut carrier in [continuation.clone(), tool, eager, speculation, realtime] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let image = passive_file(Some(true), Some(false));
            carrier.id = image.id;
            let admitted = consume(&assistant, image);
            let passive = passive_file(append, None);
            carrier.data = passive.data;
            let before = context.snapshot();
            let rejected = consume(&assistant, carrier);
            let after = context.snapshot();
            let pending = assistant.has_function_calls_in_progress();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            silent(&admitted.unwrap());
            assert!(
                rejected.is_err(),
                "private authority precedes passive handling and cached ID"
            );
            assert_eq!(before.unwrap(), after.unwrap());
            assert!(!pending.unwrap());
        }
        for provider in [
            LlmContinuationProvider::Gemini,
            LlmContinuationProvider::Anthropic,
        ] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let mut start = Frame::new(FrameData::LlmResponseStart);
            start
                .set_native_llm_continuation(Arc::new(
                    NativeLlmContinuation::start(994202, provider).unwrap(),
                ))
                .unwrap();
            let started = consume(&assistant, start);
            let held = consume(&assistant, text("A"));
            let before = context.snapshot();
            let rejected = consume(&assistant, passive_file(append, Some(false)));
            let after = context.snapshot();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            started.unwrap();
            held.unwrap();
            assert!(rejected
                .unwrap_err()
                .message
                .contains("signed continuation"));
            assert_eq!(before.unwrap(), after.unwrap());
        }
        for mode in [RealtimeServiceMode::Enabled, RealtimeServiceMode::Auto] {
            let pair = LLMContextAggregatorPair::with_realtime_mode(
                Context::default(),
                Default::default(),
                Default::default(),
                mode,
            )
            .unwrap();
            let assistant = pair.assistant();
            let announced = (mode == RealtimeServiceMode::Auto).then(|| {
                consume(
                    &assistant,
                    Frame::new(FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
                        base: ServiceMetadataFrame {
                            service_name: "passive-realtime".into(),
                            user_turn_strategies: None,
                        },
                        is_realtime_service: true,
                    })),
                )
            });
            let before = pair.context().snapshot();
            let rejected = consume(&assistant, passive_file(append, None));
            let after = pair.context().snapshot();
            let ac = assistant.cleanup().await;
            let uc = pair.user().cleanup().await;
            drop(assistant);
            drop(pair);
            ac.unwrap();
            uc.unwrap();
            if let Some(announced) = announced {
                announced.unwrap();
            }
            assert!(rejected.unwrap_err().message.contains("realtime"));
            assert_eq!(before.unwrap(), after.unwrap());
        }
    }
}

#[tokio::test]
async fn terminal_passive_files_preserve_ignore_and_common_tool_carrier_order() {
    for terminal in [
        Some(Frame::end()),
        Some(Frame::new(FrameData::Stop)),
        Some(Frame::cancel("fixture")),
        None,
    ] {
        for append in [None, Some(false)] {
            let context = SharedContext::default();
            let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
            let ended = terminal
                .as_ref()
                .map(|frame| consume(&assistant, frame.clone()));
            let early_cleanup = if terminal.is_none() {
                Some(assistant.cleanup().await)
            } else {
                None
            };
            let mut late = passive_file(append, Some(true));
            payload(&mut late).file.file.clear();
            payload(&mut late).custom_options = Some(Default::default());
            let ignored = consume(&assistant, late.clone());
            let mut continuation = Frame::new(FrameData::LlmResponseStart);
            continuation
                .set_native_llm_continuation(Arc::new(
                    NativeLlmContinuation::start(994303, LlmContinuationProvider::Gemini).unwrap(),
                ))
                .unwrap();
            continuation.data = late.data.clone();
            let private_ignored = consume(&assistant, continuation);
            let call = FunctionCall {
                function_name: "capture".into(),
                tool_call_id: "owned".into(),
                arguments: json!({}),
            };
            let mut tool = Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()]));
            tool.set_native_tool_batch(
                NativeToolCallBatch::new("late-group".into(), vec![(Arc::new(call), false)])
                    .unwrap(),
            )
            .unwrap();
            tool.data = late.data;
            let invalid_tool = consume(&assistant, tool);
            let after = context.snapshot_rich();
            let pending = assistant.has_function_calls_in_progress();
            let cleaned = assistant.cleanup().await;
            drop(assistant);
            cleaned.unwrap();
            if let Some(ended) = ended {
                ended.unwrap();
            }
            if let Some(cleaned) = early_cleanup {
                cleaned.unwrap();
            }
            silent(&ignored.unwrap());
            silent(&private_ignored.unwrap());
            assert!(
                invalid_tool.is_err(),
                "tool carrier validation precedes ended suppression"
            );
            assert!(after.unwrap().messages.is_empty());
            assert!(!pending.unwrap());
        }
    }
}

#[tokio::test]
async fn failed_projection_keeps_idle_inference_debt_blocking_passive_files() {
    use pipecat::services::{Content, ContentPart};
    for tool_debt in [false, true] {
        let inline = Message {
            role: Role::User,
            content: Some(Content::Parts(vec![ContentPart::InlineData {
                mime_type: "application/pdf".into(),
                data: "JVBERg==".into(),
            }])),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
        };
        let context = SharedContext::new(Context::new(vec![inline]));
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        let started = tool_debt.then(|| {
            consume(
                &assistant,
                Frame::new(FrameData::FunctionCallInProgress(
                    FunctionCallInProgressFrame {
                        function_name: "owned".into(),
                        tool_call_id: "owed".into(),
                        arguments: json!({}),
                        cancel_on_interruption: false,
                        group_id: None,
                    },
                )),
            )
        });
        let scheduling = if tool_debt {
            consume(
                &assistant,
                Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: "owned".into(),
                    tool_call_id: "owed".into(),
                    run_llm: true,
                })),
            )
        } else {
            consume(&assistant, Frame::new(FrameData::LlmRun))
        };
        let pending = assistant.has_function_calls_in_progress();
        let before = context.snapshot();
        let rejected: Vec<_> = [None, Some(false)]
            .into_iter()
            .map(|append| consume(&assistant, passive_file(append, Some(false))))
            .collect();
        let after = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        if let Some(started) = started {
            started.unwrap();
        }
        assert!(scheduling.unwrap_err().message.contains("InlineData"));
        assert!(
            !pending.unwrap(),
            "debt must survive without a pending tool"
        );
        for rejected in rejected {
            assert!(rejected.unwrap_err().message.contains("idle assistant"));
        }
        assert_eq!(before.unwrap(), after.unwrap());
    }
}

#[tokio::test]
async fn retained_signed_owner_blocks_passive_file_until_playback_retirement() {
    use pipecat::turns::{AssistantAggregatorConfig, AssistantTextMode};
    for append in [None, Some(false)] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(
            context.clone(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Spoken,
            },
        );
        let mut start = Frame::new(FrameData::LlmResponseStart);
        start
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(994311, LlmContinuationProvider::Gemini).unwrap(),
            ))
            .unwrap();
        let started = consume(&assistant, start);
        let playback = assistant.begin_playback("held");
        let held = consume(&assistant, text("signed answer"));
        let mut end = Frame::new(FrameData::LlmResponseEnd);
        end.set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::new(
                994311,
                LlmContinuationProvider::Gemini,
                vec![LlmContinuationBlock::GeminiText {
                    text: "signed answer".into(),
                    thought: false,
                    signature: Some("opaque-signature".into()),
                }],
            )
            .unwrap(),
        ))
        .unwrap();
        let ended = consume(&assistant, end);
        let file = passive_file(append, Some(true));
        let before = context.snapshot();
        let rejected = consume(&assistant, file.clone());
        let unchanged = context.snapshot();
        let receipt = assistant.confirm_playback(
            "held",
            &[AggregatedTextFrame {
                text: TextFrame::llm("signed answer"),
                raw_text: Some("signed answer".into()),
                context_id: Some("held".into()),
                aggregated_by: "fixture".into(),
                will_be_spoken: true,
            }],
        );
        let retired = context.snapshot();
        let accepted = consume(&assistant, file);
        let after = context.snapshot();
        let cleaned = assistant.cleanup().await;
        drop(assistant);
        cleaned.unwrap();
        started.unwrap();
        playback.unwrap();
        held.unwrap();
        ended.unwrap();
        assert!(rejected
            .unwrap_err()
            .message
            .contains("signed continuation"));
        assert_eq!(before.unwrap(), unchanged.unwrap());
        receipt.unwrap();
        silent(&accepted.unwrap());
        let retired = retired.unwrap();
        assert_eq!(retired.messages.len(), 1);
        assert_eq!(retired, after.unwrap());
    }
}
