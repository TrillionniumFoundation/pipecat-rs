//! Source-mapped deterministic assertions, not a Python execution baseline.
use pipecat_core::*;
use pipecat_services::{Content, Context, Message, Role};
use pipecat_turns::*;
use serde_json::json;
use std::time::Duration;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn vad(start: bool) -> FrameData {
    let event = VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    };
    if start {
        FrameData::VadUserStartedSpeaking(event)
    } else {
        FrameData::VadUserStoppedSpeaking(event)
    }
}
fn transcript(text: &str, finalized: bool) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized,
        ..Default::default()
    })
}
fn interim(text: &str) -> FrameData {
    FrameData::InterimTranscription(TranscriptionFrame {
        text: text.into(),
        ..Default::default()
    })
}
fn user() -> LLMUserAggregator {
    LLMUserAggregator::new(SharedContext::default(), UserAggregatorConfig::default())
}
fn user_at(user: &LLMUserAggregator, data: FrameData, time: u64) -> AggregationOutput {
    user.process_at(Frame::new(data), Direction::Downstream, ms(time))
        .unwrap()
}
fn assistant(mode: AssistantTextMode) -> LLMAssistantAggregator {
    LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig { text_mode: mode },
    )
}
fn assistant_frame(assistant: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    assistant
        .process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}
fn history(context: SharedContext) -> Vec<(Role, String)> {
    context
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|message| {
            let text = match message.content {
                Some(Content::Text(text)) => text,
                None => String::new(),
                other => panic!("{other:?}"),
            };
            (message.role, text)
        })
        .collect()
}
fn context_pushes(output: &AggregationOutput, direction: Direction) -> usize {
    output
        .actions
        .iter()
        .filter(|action| {
            matches!(action, AggregationAction::Push(frame)
        if frame.direction == direction && matches!(frame.frame.data, FrameData::LlmContext(_)))
        })
        .count()
}

// tests/test_context_aggregators_universal.py::test_interim_transcription_not_pushed_downstream
#[test]
fn interim_hypotheses_are_replaced_and_never_committed() {
    let user = user();
    let first = user_at(&user, interim("Hel"), 0);
    assert_eq!(first.events, vec![AggregationEvent::UserTurnStarted]);
    assert!(!first
        .actions
        .iter()
        .any(|action| matches!(action, AggregationAction::Push(_))));
    assert!(user_at(&user, interim("Hello"), 5).events.is_empty());
    assert_eq!(user.interim_text().unwrap(), "Hello");
    user_at(&user, transcript("Hello", true), 10);
    assert_eq!(user.interim_text().unwrap(), "");
    let finished = user.advance(ms(610)).unwrap();
    assert_eq!(context_pushes(&finished, Direction::Downstream), 1);
    assert_eq!(history(user.context()), [(Role::User, "Hello".into())]);
    assert!(user.advance(ms(10000)).unwrap().events.is_empty());
}

// speech_timeout_user_turn_stop_strategy.py: independent speech policy and STT safety net.
#[test]
fn finalized_transcript_does_not_bypass_silence_policy() {
    let mut c = UserTurnController::new(UserTurnConfig {
        stt_timeout: ms(2000),
        ..Default::default()
    });
    assert_eq!(c.process(&vad(true), ms(0)).len(), 1);
    c.process(&transcript("hello", true), ms(100));
    assert!(c.advance(ms(5000)).is_empty()); // Still audibly speaking.
    c.process(&vad(false), ms(5100));
    assert!(c.advance(ms(5699)).is_empty());
    assert_eq!(
        c.advance(ms(5700)),
        [UserTurnEvent::Stopped {
            emit_speaking: true,
            reason: TurnStopReason::SpeechTimeout
        }]
    );
}
#[test]
fn unfinalized_final_segment_waits_for_stt_latency_from_vad_stop() {
    let mut c = UserTurnController::new(UserTurnConfig {
        stt_timeout: ms(2000),
        ..Default::default()
    });
    c.process(&vad(true), ms(0));
    c.process(&transcript("hello", false), ms(10));
    c.process(&vad(false), ms(100));
    assert_eq!(c.deadline(), Some(ms(1900)));
    assert!(c.advance(ms(700)).is_empty());
    assert!(c.advance(ms(1899)).is_empty());
    assert_eq!(c.advance(ms(1900)).len(), 1);
}
#[test]
fn late_final_arriving_after_both_waits_commits_once() {
    let user = user();
    user_at(&user, vad(true), 0);
    user_at(&user, vad(false), 100);
    assert!(user.advance(ms(800)).unwrap().events.is_empty());
    let out = user_at(&user, transcript("late tail", true), 1000);
    assert_eq!(context_pushes(&out, Direction::Downstream), 1);
    assert_eq!(history(user.context()), [(Role::User, "late tail".into())]);
}
#[test]
fn interim_after_finalization_restores_stt_wait() {
    let mut c = UserTurnController::new(UserTurnConfig {
        stt_timeout: ms(2000),
        ..Default::default()
    });
    c.process(&vad(true), ms(0));
    c.process(&transcript("first", true), ms(20));
    c.process(&interim("tail"), ms(30));
    c.process(&vad(false), ms(100));
    assert!(c.advance(ms(700)).is_empty());
    assert_eq!(c.deadline(), Some(ms(1900)));
    assert_eq!(c.process(&transcript("tail", true), ms(800)).len(), 1);
}
// tests/test_user_turn_controller.py::test_completion_dropped_while_user_speaking
#[test]
fn resumed_speech_invalidates_a_stale_completion_timer() {
    let mut c = UserTurnController::new(UserTurnConfig::default());
    c.process(&vad(true), ms(0));
    c.process(&transcript("I think", true), ms(10));
    c.process(&vad(false), ms(100));
    assert!(c.process(&vad(true), ms(650)).is_empty());
    assert!(c.advance(ms(10000)).is_empty());
    c.process(&vad(false), ms(10100));
    assert!(c.advance(ms(10699)).is_empty());
    assert_eq!(c.advance(ms(10700)).len(), 1);
}
// tests/test_user_turn_controller.py::test_late_transcription_between_turns_no_premature_stop
#[test]
fn vad_only_strategy_does_not_leak_late_text_into_next_turn() {
    let mut c = UserTurnController::new(UserTurnConfig {
        start_on_transcription: false,
        speech_timeout: ms(0),
        ..Default::default()
    });
    c.process(&vad(true), ms(0));
    c.process(&transcript("turn one", true), ms(1));
    assert_eq!(c.process(&vad(false), ms(10)).len(), 1);
    assert!(c.process(&transcript("late", true), ms(20)).is_empty());
    c.process(&vad(true), ms(30));
    assert!(c.process(&vad(false), ms(40)).is_empty());
    assert!(c.advance(ms(41)).is_empty());
    assert_eq!(c.process(&transcript("turn two", true), ms(50)).len(), 1);
}
#[test]
fn duplicate_frame_and_explicit_transcript_identity_do_not_double_run() {
    let user = user();
    let mut frame = Frame::new(transcript("hello", true));
    frame
        .metadata
        .insert("transcript_id".into(), "segment-1".into());
    user.process_at(frame.clone(), Direction::Downstream, ms(0))
        .unwrap();
    assert!(user
        .process_at(frame.clone(), Direction::Downstream, ms(1))
        .unwrap()
        .actions
        .is_empty());
    assert_eq!(
        context_pushes(&user.advance(ms(600)).unwrap(), Direction::Downstream),
        1
    );
    assert!(user
        .process_at(frame.duplicate(), Direction::Downstream, ms(800))
        .unwrap()
        .actions
        .is_empty());
    user_at(&user, transcript("hello", true), 1000); // Genuine repetition has a new identity.
    user.advance(ms(1600)).unwrap();
    assert_eq!(
        history(user.context()),
        [(Role::User, "hello".into()), (Role::User, "hello".into())]
    );
}
// tests/test_context_aggregators_universal.py::test_user_mute_strategies, test_empty_user_turn.py::test_idle_ignored_by_default
#[test]
fn mute_and_empty_transcripts_cannot_start_inference() {
    let user = user();
    for text in ["", " ", "\n\t"] {
        assert!(user_at(&user, transcript(text, true), 0).actions.is_empty());
    }
    user_at(&user, FrameData::UserMuteStarted, 1);
    for data in [
        vad(true),
        interim("private"),
        transcript("private", true),
        vad(false),
        FrameData::Interruption,
    ] {
        assert!(user_at(&user, data, 2).actions.is_empty());
    }
    assert!(history(user.context()).is_empty());
    user_at(&user, FrameData::UserMuteStopped, 10);
    // Idle noise is unanswered; an interrupted pre-greeting turn is recovered.
    user_at(&user, FrameData::BotStoppedSpeaking, 11);
    user_at(&user, vad(true), 20);
    user_at(&user, vad(false), 30);
    let empty = user.advance(ms(5030)).unwrap();
    assert_eq!(context_pushes(&empty, Direction::Downstream), 0);
    assert_eq!(
        empty.events,
        [AggregationEvent::UserTurnStopped {
            text: String::new(),
            reason: TurnStopReason::Watchdog
        }]
    );
}
// tests/test_context_aggregators_universal.py::test_pending_transcription_emitted_on_end_frame
#[test]
fn end_flushes_pending_final_but_never_interim() {
    let user = user();
    user_at(&user, vad(true), 0);
    user_at(&user, transcript("Hello!", false), 1);
    user_at(&user, interim("unconfirmed"), 2);
    let end = user_at(&user, FrameData::End(EndFrame::default()), 3);
    assert_eq!(context_pushes(&end, Direction::Downstream), 1);
    assert_eq!(history(user.context()), [(Role::User, "Hello!".into())]);
    assert!(user_at(&user, transcript("late", true), 4)
        .actions
        .is_empty());
}
// tests/test_context_aggregators_universal.py::test_real_turn_frames_are_adopted_without_re_emission
#[test]
fn external_real_frames_are_adopted_without_extra_interruptions() {
    let user = user();
    let first = user_at(&user, FrameData::UserStartedSpeaking, 0);
    assert_eq!(first.events, [AggregationEvent::UserTurnStarted]);
    assert_eq!(first.actions.len(), 1); // Just forwards the original speaking frame.
    user_at(&user, transcript("adopted", true), 1);
    let last = user_at(&user, FrameData::UserStoppedSpeaking, 2);
    assert_eq!(context_pushes(&last, Direction::Downstream), 1);
    assert!(!last
        .actions
        .iter()
        .any(|a| matches!(a, AggregationAction::Broadcast(_))));
}

// tests/test_context_aggregators_universal.py::test_multiple_text_mixed_spaces
#[test]
fn assistant_mixed_token_and_stripped_text_golden() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    for (text, spaces) in [
        ("Hello ", true),
        ("Pipecat. ", true),
        ("Here's some", true),
        (" code:", true),
        ("```python\nprint('Hello, World!')\n```", false),
        ("```javascript\nconsole.log('Hello, World!');\n```", false),
        (" And some more: ", true),
        ("```html\n<div>Hello, World!</div>\n```", false),
        ("Hope that ", true),
        ("helps!", true),
    ] {
        let mut frame = TextFrame::new(text);
        frame.includes_inter_frame_spaces = spaces;
        assistant_frame(&assistant, FrameData::Text(frame));
    }
    assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assert_eq!(history(assistant.context())[0].1,
        "Hello Pipecat. Here's some code: ```python\nprint('Hello, World!')\n``` ```javascript\nconsole.log('Hello, World!');\n``` And some more: ```html\n<div>Hello, World!</div>\n``` Hope that helps!");
}
// tests/test_context_aggregators_universal.py::test_interruption, test_multiple_responses
#[test]
fn assistant_interruption_commits_partial_and_ignores_stale_tail() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assistant_frame(&assistant, FrameData::LlmText(TextFrame::llm("Hello ")));
    let interruption = assistant_frame(&assistant, FrameData::Interruption);
    assert!(interruption
        .events
        .contains(&AggregationEvent::AssistantTurnStopped {
            text: "Hello".into(),
            interrupted: true
        }));
    assistant_frame(
        &assistant,
        FrameData::LlmText(TextFrame::llm("unspoken tail")),
    );
    assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assistant_frame(
        &assistant,
        FrameData::LlmText(TextFrame::llm("Hello there!")),
    );
    assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assert_eq!(
        history(assistant.context()),
        [
            (Role::Assistant, "Hello".into()),
            (Role::Assistant, "Hello there!".into())
        ]
    );
}
fn tts_text(text: &str, id: &str, spoken: bool) -> Frame {
    let mut frame = Frame::new(FrameData::TtsText(AggregatedTextFrame {
        text: TextFrame::new(text),
        aggregated_by: "word".into(),
        context_id: Some(id.into()),
        raw_text: None,
        will_be_spoken: true,
    }));
    frame.metadata.insert("spoken".into(), spoken.into());
    frame
}
fn tts_start(assistant: &LLMAssistantAggregator, id: &str) {
    assistant_frame(
        assistant,
        FrameData::TtsStarted(TtsStartedFrame {
            context_id: Some(id.into()),
            append_to_context: true,
        }),
    );
}
fn finish_playback(assistant: &LLMAssistantAggregator) -> AggregationOutput {
    assistant_frame(
        assistant,
        FrameData::Flush(FlushFrame {
            origin: Some("assistant_turn_end".into()),
            returning: false,
        }),
    )
}
// tests/test_context_aggregators_universal.py::test_tts_speak_interruption_records_partial_text
#[test]
fn spoken_mode_requires_playback_ack_and_only_records_heard_prefix() {
    let assistant = assistant(AssistantTextMode::Spoken);
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assistant_frame(
        &assistant,
        FrameData::LlmText(TextFrame::llm("whole generated sentence")),
    );
    tts_start(&assistant, "audio-1");
    assistant
        .process_frame(
            tts_text("whole generated sentence", "audio-1", false),
            Direction::Downstream,
        )
        .unwrap();
    assistant
        .process_frame(tts_text("Let me", "audio-1", true), Direction::Downstream)
        .unwrap();
    assistant
        .process_frame(tts_text("check on", "audio-1", true), Direction::Downstream)
        .unwrap();
    assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assert!(history(assistant.context()).is_empty());
    let out = assistant_frame(&assistant, FrameData::Interruption);
    assert_eq!(
        out.events,
        [AggregationEvent::AssistantTurnStopped {
            text: "Let me check on".into(),
            interrupted: true
        }]
    );
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    tts_start(&assistant, "audio-2");
    assistant
        .process_frame(
            tts_text("stale old tail", "audio-1", true),
            Direction::Downstream,
        )
        .unwrap();
    assistant
        .process_frame(
            tts_text("new answer", "audio-2", true),
            Direction::Downstream,
        )
        .unwrap();
    assistant_frame(&assistant, FrameData::LlmResponseEnd);
    finish_playback(&assistant);
    assert_eq!(
        history(assistant.context()),
        [
            (Role::Assistant, "Let me check on".into()),
            (Role::Assistant, "new answer".into())
        ]
    );
}
#[test]
fn speaking_system_stop_cannot_cut_off_queued_acknowledged_words() {
    let assistant = assistant(AssistantTextMode::Spoken);
    tts_start(&assistant, "audio");
    assistant_frame(&assistant, FrameData::BotStoppedSpeaking); // Overtakes normal-lane words.
    assistant
        .process_frame(tts_text("last word", "audio", true), Direction::Downstream)
        .unwrap();
    assistant_frame(
        &assistant,
        FrameData::TtsStopped(TtsStoppedFrame {
            context_id: Some("audio".into()),
        }),
    );
    assert!(history(assistant.context()).is_empty());
    finish_playback(&assistant);
    assert_eq!(
        history(assistant.context()),
        [(Role::Assistant, "last word".into())]
    );
}
#[test]
fn thoughts_and_append_false_are_never_assistant_context() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assistant_frame(
        &assistant,
        FrameData::LlmThoughtText(TextFrame::llm("private thought")),
    );
    let mut text = TextFrame::new("skip");
    text.append_to_context = false;
    assistant_frame(&assistant, FrameData::Text(text));
    let out = assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assert_eq!(
        out.events,
        [AggregationEvent::AssistantTurnStopped {
            text: String::new(),
            interrupted: false
        }]
    );
    assert!(history(assistant.context()).is_empty());
}
#[test]
fn end_flushes_pending_assistant_text_exactly_once() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assistant_frame(&assistant, FrameData::LlmText(TextFrame::llm("pending")));
    assistant_frame(&assistant, FrameData::End(EndFrame::default()));
    assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assert_eq!(
        history(assistant.context()),
        [(Role::Assistant, "pending".into())]
    );
}
fn call(id: &str, group: Option<&str>, cancel_on_interruption: bool) -> FrameData {
    FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
        function_name: "weather".into(),
        tool_call_id: id.into(),
        arguments: json!({"city":"Paris"}),
        cancel_on_interruption,
        group_id: group.map(str::to_owned),
    })
}
fn result(id: &str, final_result: bool, run_llm: Option<bool>) -> FrameData {
    FrameData::FunctionCallResult(FunctionCallResultFrame {
        function_name: "weather".into(),
        tool_call_id: id.into(),
        arguments: json!({"city":"Paris"}),
        result: json!({"sunny":true}),
        run_llm,
        is_final: final_result,
        error: None,
    })
}
// tests/test_context_aggregators_universal.py::test_function_call, test_parallel_fast_async_function_calls_settle_in_place
#[test]
fn tool_group_commits_results_in_place_and_runs_once_in_reverse_result_order() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, call("one", Some("group"), true));
    assistant_frame(&assistant, call("two", Some("group"), true));
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("two", true, None)),
            Direction::Upstream
        ),
        0
    );
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("one", true, None)),
            Direction::Upstream
        ),
        1
    );
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    let snapshot = assistant.context().snapshot().unwrap();
    snapshot.validate().unwrap();
    assert_eq!(snapshot.messages[1].tool_call_id.as_deref(), Some("one"));
    assert_eq!(snapshot.messages[3].tool_call_id.as_deref(), Some("two"));
    for index in [1, 3] {
        assert_eq!(
            snapshot.messages[index].content,
            Some(Content::Text("{\"sunny\":true}".into()))
        );
    }
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("one", true, None)),
            Direction::Upstream
        ),
        0
    );
}
#[test]
fn result_before_in_progress_or_after_cancel_cannot_fabricate_tool_history() {
    let assistant = assistant(AssistantTextMode::Text);
    let out = assistant_frame(&assistant, result("one", true, None));
    assert_eq!(
        out.events,
        [AggregationEvent::ToolResultIgnored {
            tool_call_id: "one".into()
        }]
    );
    assert!(history(assistant.context()).is_empty());
    assistant_frame(&assistant, call("one", None, true));
    assistant_frame(&assistant, FrameData::Interruption);
    assistant_frame(&assistant, result("one", true, None));
    assert_eq!(history(assistant.context())[1].1, "CANCELLED");
}
// test_async_function_call_after_user_message_is_deferred; test_intermediate_update_keeps_the_call_deferred
#[test]
fn async_tool_intermediate_and_late_final_use_developer_protocol() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, call("one", None, false));
    assistant_frame(&assistant, result("one", false, Some(false)));
    assistant
        .context()
        .append(Message::text(Role::User, "different question"))
        .unwrap();
    assistant_frame(&assistant, FrameData::Interruption); // Async call survives.
    assert!(assistant.has_function_calls_in_progress().unwrap());
    assistant_frame(&assistant, result("one", true, Some(false)));
    let messages = assistant.context().snapshot().unwrap().messages;
    assert_eq!(messages[1].role, Role::Tool);
    let started: serde_json::Value =
        serde_json::from_str(&history(assistant.context())[1].1).unwrap();
    assert_eq!(started["status"], "running");
    let final_result: serde_json::Value =
        serde_json::from_str(&history(assistant.context()).last().unwrap().1).unwrap();
    assert_eq!(messages.last().unwrap().role, Role::Developer);
    assert_eq!(final_result["type"], "async_tool");
    assert_eq!(final_result["status"], "finished");
    assert_eq!(final_result["tool_call_id"], "one");
}
#[test]
fn fast_async_result_settles_original_placeholder() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, call("one", None, false));
    assistant
        .context()
        .append(Message::text(Role::Assistant, "checking"))
        .unwrap();
    assistant_frame(&assistant, result("one", true, Some(false)));
    assert_eq!(history(assistant.context()).len(), 3);
    assert_eq!(
        history(assistant.context())[1],
        (Role::Tool, "{\"sunny\":true}".into())
    );
}
// tests/test_context_aggregators_universal.py::test_run_survives_a_later_result_that_does_not_ask_to_run
#[test]
fn run_requested_by_a_tool_is_sticky_until_bot_finishes() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, FrameData::BotStartedSpeaking);
    assistant_frame(&assistant, call("one", None, true));
    assistant_frame(&assistant, call("two", None, true));
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("one", true, Some(true))),
            Direction::Upstream
        ),
        0
    );
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("two", true, Some(false))),
            Direction::Upstream
        ),
        0
    );
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, FrameData::BotStoppedSpeaking),
            Direction::Upstream
        ),
        1
    );
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, FrameData::BotStoppedSpeaking),
            Direction::Upstream
        ),
        0
    );
}
#[test]
fn multiround_pair_context_is_shared_and_ordered() {
    let pair = LLMContextAggregatorPair::new(
        Context::new(vec![Message::text(Role::System, "Be helpful")]),
        UserAggregatorConfig::default(),
        AssistantAggregatorConfig::default(),
    );
    for (round, (question, answer)) in [("hello", "Hi"), ("again", "Welcome back")]
        .into_iter()
        .enumerate()
    {
        let now = round as u64 * 1000;
        user_at(&pair.user(), transcript(question, true), now);
        let out = pair.user().advance(ms(now + 600)).unwrap();
        assert_eq!(context_pushes(&out, Direction::Downstream), 1);
        assistant_frame(&pair.assistant(), FrameData::LlmResponseStart);
        assistant_frame(
            &pair.assistant(),
            FrameData::LlmText(TextFrame::llm(answer)),
        );
        assistant_frame(&pair.assistant(), FrameData::LlmResponseEnd);
    }
    assert_eq!(
        history(pair.context()),
        [
            (Role::System, "Be helpful".into()),
            (Role::User, "hello".into()),
            (Role::Assistant, "Hi".into()),
            (Role::User, "again".into()),
            (Role::Assistant, "Welcome back".into())
        ]
    );
}

#[test]
fn tool_started_system_frame_overtaking_response_start_does_not_lose_owed_run() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(
        &assistant,
        FrameData::FunctionCallsStarted(vec![FunctionCall {
            function_name: "weather".into(),
            tool_call_id: "one".into(),
            arguments: json!({"city":"Paris"}),
        }]),
    );
    assistant_frame(&assistant, call("one", Some("group"), true));
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("one", true, None)),
            Direction::Upstream
        ),
        0
    );
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, FrameData::LlmResponseEnd),
            Direction::Upstream
        ),
        1
    );
}
#[test]
fn unrelated_run_owed_cannot_run_a_half_completed_tool_group() {
    let assistant = assistant(AssistantTextMode::Text);
    assistant_frame(&assistant, call("one", Some("group"), true));
    assistant_frame(&assistant, call("two", Some("group"), true));
    let append = FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::Developer, "new instruction")],
        run_llm: true,
    });
    assert_eq!(
        context_pushes(&assistant_frame(&assistant, append), Direction::Upstream),
        0
    );
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("one", true, Some(false))),
            Direction::Upstream
        ),
        0
    );
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("two", true, Some(false))),
            Direction::Upstream
        ),
        1
    );
}
#[test]
fn immediate_typed_turn_snapshot_contains_interrupted_assistant_prefix_first() {
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig::default(),
        AssistantAggregatorConfig::default(),
    );
    assistant_frame(&pair.assistant(), FrameData::LlmResponseStart);
    assistant_frame(
        &pair.assistant(),
        FrameData::LlmText(TextFrame::llm("delivered prefix")),
    );
    let typed = user_at(
        &pair.user(),
        FrameData::InputText(TextFrame::new("new question")),
        0,
    );
    let snapshot = typed
        .actions
        .iter()
        .find_map(|action| match action {
            AggregationAction::Push(frame) => match &frame.frame.data {
                FrameData::LlmContext(context) => Some(context),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    assert_eq!(snapshot.messages.len(), 2);
    assert_eq!(snapshot.messages[0].role, ChatRole::Assistant);
    assert_eq!(snapshot.messages[0].content, "delivered prefix");
    assert_eq!(snapshot.messages[1].role, ChatRole::User);
    assert_eq!(snapshot.messages[1].content, "new question");
    assistant_frame(&pair.assistant(), FrameData::Interruption);
    assert_eq!(history(pair.context()).len(), 2);
}
#[test]
fn atomic_remote_ack_is_committed_before_an_immediate_user_interrupt() {
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    assistant_frame(&pair.assistant(), FrameData::LlmResponseStart);
    let FrameData::TtsText(text) = tts_text("already played", "context-1", false).data else {
        unreachable!()
    };
    pair.assistant()
        .confirm_playback("context-1", std::slice::from_ref(&text))
        .unwrap();
    user_at(
        &pair.user(),
        FrameData::InputText(TextFrame::new("next")),
        0,
    );
    assert_eq!(
        history(pair.context()),
        [
            (Role::Assistant, "already played".into()),
            (Role::User, "next".into())
        ]
    );
    let replay = pair
        .assistant()
        .confirm_playback("context-1", &[text])
        .unwrap();
    assert!(replay.actions.is_empty());
    assert_eq!(history(pair.context()).len(), 2);
}
#[test]
fn atomic_confirmation_rejects_mismatched_context_ids_without_any_write() {
    let assistant = assistant(AssistantTextMode::Spoken);
    let FrameData::TtsText(text) = tts_text("untrusted mapping", "a", false).data else {
        unreachable!()
    };
    assert!(assistant.confirm_playback("b", &[text]).is_err());
    assert!(history(assistant.context()).is_empty());
}
#[test]
fn empty_tool_result_values_are_preserved_instead_of_replaced_by_completed() {
    for value in [json!(false), json!(0), json!(""), json!([])] {
        let assistant = assistant(AssistantTextMode::Text);
        assistant_frame(&assistant, call("one", None, true));
        let FrameData::FunctionCallResult(mut result) = result("one", true, Some(false)) else {
            unreachable!()
        };
        result.result = value.clone();
        assistant_frame(&assistant, FrameData::FunctionCallResult(result));
        assert_eq!(history(assistant.context())[1].1, value.to_string());
    }
}
#[test]
fn unsupported_multimodal_context_fails_explicitly() {
    let mut message = Message::text(Role::User, "ignored");
    message.content = Some(Content::Parts(vec![
        pipecat_services::ContentPart::ImageUrl {
            url: "https://example.test/image.png".into(),
        },
    ]));
    assert!(context_to_frame(&Context::new(vec![message]))
        .unwrap_err()
        .message
        .contains("multimodal"));
}
#[test]
fn resolving_external_proposal_waits_for_the_current_interim_to_finalize() {
    let mut c = UserTurnController::new(UserTurnConfig {
        resolve_proposals: true,
        ..Default::default()
    });
    assert_eq!(
        c.process(&FrameData::ProposedUserStartedSpeaking, ms(0))
            .len(),
        1
    );
    c.process(&transcript("first", true), ms(1));
    c.process(&interim("second"), ms(2));
    assert!(c
        .process(&FrameData::ProposedUserStoppedSpeaking, ms(3))
        .is_empty());
    assert_eq!(
        c.process(&transcript("second", true), ms(4)),
        [UserTurnEvent::Stopped {
            emit_speaking: true,
            reason: TurnStopReason::External
        }]
    );
}
#[test]
fn spoken_mode_tool_only_response_does_not_wait_for_nonexistent_audio_ack() {
    let assistant = assistant(AssistantTextMode::Spoken);
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assistant_frame(
        &assistant,
        FrameData::FunctionCallsStarted(vec![FunctionCall {
            function_name: "weather".into(),
            tool_call_id: "one".into(),
            arguments: json!({"city":"Paris"}),
        }]),
    );
    assistant_frame(&assistant, call("one", Some("group"), true));
    assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("one", true, None)),
            Direction::Upstream
        ),
        1
    );
    assert_eq!(history(assistant.context()).len(), 2); // Just call and tool result; no invented assistant prose.
}
#[test]
fn queued_playback_contexts_gate_tool_rerun_until_all_confirmed() {
    let assistant = assistant(AssistantTextMode::Spoken);
    assistant_frame(&assistant, FrameData::LlmResponseStart);
    assistant.begin_playback("first").unwrap();
    assistant.begin_playback("second").unwrap();
    assistant_frame(&assistant, call("tool", None, true));
    assistant_frame(&assistant, FrameData::LlmResponseEnd);
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, result("tool", true, None)),
            Direction::Upstream
        ),
        0
    );
    let FrameData::TtsText(one) = tts_text("First part", "first", false).data else {
        unreachable!()
    };
    let FrameData::TtsText(two) = tts_text("Second part", "second", false).data else {
        unreachable!()
    };
    assert_eq!(
        context_pushes(
            &assistant.confirm_playback("first", &[one]).unwrap(),
            Direction::Upstream
        ),
        0
    );
    assert_eq!(
        context_pushes(
            &assistant_frame(&assistant, FrameData::BotStoppedSpeaking),
            Direction::Upstream
        ),
        0
    );
    assert_eq!(
        context_pushes(
            &assistant.confirm_playback("second", &[two]).unwrap(),
            Direction::Upstream
        ),
        1
    );
    let text = history(assistant.context());
    assert_eq!(text[2].1, "First part");
    assert_eq!(text[3].1, "Second part");
}
