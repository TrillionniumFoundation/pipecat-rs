//! Pinned source-contract replay and native lifecycle regressions.
use pipecat_core::*;
use pipecat_services::{Content, Context, Role};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

const INTERRUPTED: &str = "interrupted, nothing recognized";
const IDLE: &str = "idle, nothing recognized";
fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}
fn config() -> UserAggregatorConfig {
    UserAggregatorConfig {
        turn: UserTurnConfig {
            speech_timeout: ms(100),
            stop_timeout: ms(200),
            ..Default::default()
        },
        user_idle_timeout: ms(200),
        empty_user_turn: Some(EmptyUserTurnConfig {
            interrupted_prompt: Some(INTERRUPTED.into()),
            ..Default::default()
        }),
        ..Default::default()
    }
}
fn vad(start: bool) -> FrameData {
    let value = VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    };
    if start {
        FrameData::VadUserStartedSpeaking(value)
    } else {
        FrameData::VadUserStoppedSpeaking(value)
    }
}
fn transcript(text: &str) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    })
}
fn call(id: &str) -> FunctionCall {
    FunctionCall {
        function_name: "lookup".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    }
}
fn result(id: &str, is_final: bool) -> FunctionCallResultFrame {
    FunctionCallResultFrame {
        function_name: "lookup".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
        result: json!({"answer":42}),
        run_llm: None,
        is_final,
        error: None,
    }
}
fn history(user: &LLMUserAggregator, role: Role) -> Vec<String> {
    user.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter(|message| message.role == role)
        .map(|message| match message.content {
            Some(Content::Text(text)) => text,
            other => panic!("unexpected content {other:?}"),
        })
        .collect()
}
fn contexts(output: &AggregationOutput, direction: Direction) -> usize {
    output.actions.iter().filter(|action| matches!(action,
        AggregationAction::Push(frame) if frame.direction == direction
            && matches!(frame.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_))
    )).count()
}

struct Replay {
    user: Arc<LLMUserAggregator>,
    assistant: Option<Arc<LLMAssistantAggregator>>,
    now: u64,
    inferences: usize,
    tool_continuations: usize,
}
impl Replay {
    fn new(config: UserAggregatorConfig, paired: bool) -> Self {
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            config.clone(),
            AssistantAggregatorConfig::default(),
        );
        Self {
            user: if paired {
                pair.user()
            } else {
                Arc::new(LLMUserAggregator::new(SharedContext::default(), config))
            },
            assistant: paired.then(|| pair.assistant()),
            now: 0,
            inferences: 0,
            tool_continuations: 0,
        }
    }
    fn accept(&mut self, output: AggregationOutput) {
        self.inferences += contexts(&output, Direction::Downstream);
        if let Some(assistant) = &self.assistant {
            for action in output.actions {
                let frame = match action {
                    AggregationAction::Push(frame) if frame.direction == Direction::Downstream => {
                        frame.frame
                    }
                    AggregationAction::Broadcast(frame) => frame,
                    AggregationAction::Interrupt => Frame::interruption(),
                    AggregationAction::InterruptBeforeContext => {
                        let mut frame = Frame::interruption();
                        frame
                            .metadata
                            .insert("user_context_follows".into(), true.into());
                        frame
                    }
                    _ => continue,
                };
                let output = assistant
                    .process_frame(frame, Direction::Downstream)
                    .unwrap();
                self.tool_continuations += contexts(&output, Direction::Upstream);
            }
        }
    }
    fn send_frame(&mut self, frame: Frame) {
        let output = self
            .user
            .process_at(frame, Direction::Downstream, ms(self.now))
            .unwrap();
        self.accept(output);
        self.now += 1;
    }
    fn send(&mut self, data: FrameData) {
        self.send_frame(Frame::new(data));
    }
    fn advance(&mut self, amount: u64) -> AggregationOutput {
        self.now += amount;
        let output = self.user.advance(ms(self.now)).unwrap();
        self.accept(output.clone());
        output
    }
    fn empty(&mut self) -> AggregationOutput {
        self.send(vad(true));
        self.send(vad(false));
        self.advance(300)
    }
    fn transcribed(&mut self, text: &str) {
        self.send(vad(true));
        self.send(transcript(text));
        self.send(vad(false));
        self.advance(150);
    }
    fn bot_speaking(&mut self) {
        self.send(FrameData::LlmRun);
        self.send(FrameData::LlmResponseStart);
        self.send(FrameData::BotStartedSpeaking);
        self.send(FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new("Where would"),
            aggregated_by: "word".into(),
            context_id: None,
            raw_text: None,
            will_be_spoken: true,
        }));
    }
}

#[test]
fn replay_pinned_source_empty_turn_cases() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/empty_user_turn.json")).unwrap();
    assert_eq!(
        fixture["source_revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    assert_eq!(
        fixture["default_prompt"],
        DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT
    );
    for case in fixture["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut config = config();
        if case["default_config"] == true {
            config.empty_user_turn = UserAggregatorConfig::default().empty_user_turn;
        }
        if case["disabled"] == true {
            config.empty_user_turn = None;
        }
        if let Some(prompt) = case["idle_prompt"].as_str() {
            config.empty_user_turn.as_mut().unwrap().idle_prompt = Some(prompt.into());
        }
        let mut replay = Replay::new(config, case["standalone"] != true);
        for step in case["steps"].as_array().unwrap() {
            match step.as_str().unwrap() {
                "bot_speaking" => replay.bot_speaking(),
                "bot_idle" => replay.send(FrameData::BotStoppedSpeaking),
                "response_end" => replay.send(FrameData::LlmResponseEnd),
                "run" => replay.send(FrameData::LlmRun),
                "empty" => {
                    replay.empty();
                }
                "transcribed" => replay.transcribed("Hello!"),
                "tool_pending" => replay.send(FrameData::FunctionCallsStarted(vec![call("one")])),
                other => panic!("unknown fixture action {other}"),
            }
        }
        let expected: Vec<_> = case["prompts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|prompt| {
                match prompt.as_str().unwrap() {
                    "$default" => DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT,
                    other => other,
                }
                .to_owned()
            })
            .collect();
        assert_eq!(history(&replay.user, Role::Developer), expected, "{name}");
        assert_eq!(
            replay.inferences,
            case["inferences"].as_u64().unwrap() as usize,
            "{name}"
        );
        assert_eq!(
            replay.tool_continuations, 0,
            "{name}: duplicate tool inference"
        );
        if case["heard_prefix"] == true {
            let messages = replay.user.context().snapshot().unwrap().messages;
            assert_eq!(messages[messages.len() - 2].role, Role::Assistant);
            assert_eq!(
                messages[messages.len() - 2].content,
                Some(Content::Text("Where would".into()))
            );
        }
    }
}

#[test]
fn source_idle_rearms_only_when_recovery_did_not_run() {
    for enabled in [false, true] {
        let mut cfg = config();
        if !enabled {
            cfg.empty_user_turn = None;
        }
        let mut replay = Replay::new(cfg, true);
        replay.bot_speaking();
        replay.send(vad(true));
        replay.send(FrameData::BotStoppedSpeaking);
        replay.send(vad(false));
        replay.advance(300);
        assert_eq!(replay.user.waiting_for_user().unwrap(), !enabled);
        assert_eq!(replay.user.idle_deadline().unwrap().is_some(), !enabled);
        let later = replay.advance(200);
        assert_eq!(
            later.events.contains(&AggregationEvent::UserTurnIdle),
            !enabled
        );
    }
}

#[test]
fn absent_empty_and_zero_budget_never_recover_or_consume_a_budget() {
    for prompt in [None, Some(String::new())] {
        let mut cfg = config();
        cfg.empty_user_turn.as_mut().unwrap().interrupted_prompt = prompt;
        cfg.empty_user_turn.as_mut().unwrap().idle_prompt = Some(IDLE.into());
        let mut replay = Replay::new(cfg, true);
        replay.empty();
        assert!(history(&replay.user, Role::Developer).is_empty());
        replay.send(FrameData::BotStoppedSpeaking);
        replay.empty();
        assert_eq!(history(&replay.user, Role::Developer), [IDLE]);
    }
    let mut cfg = config();
    cfg.empty_user_turn
        .as_mut()
        .unwrap()
        .max_consecutive_recoveries = 0;
    let mut replay = Replay::new(cfg, true);
    replay.empty();
    assert!(history(&replay.user, Role::Developer).is_empty());
}

#[test]
fn interruptions_disabled_and_adopted_external_starts_use_idle_prompt() {
    for external in [false, true] {
        let mut cfg = config();
        cfg.turn.enable_interruptions = external;
        cfg.empty_user_turn.as_mut().unwrap().idle_prompt = Some(IDLE.into());
        let mut replay = Replay::new(cfg, true);
        replay.send(if external {
            FrameData::UserStartedSpeaking
        } else {
            vad(true)
        });
        replay.send(if external {
            FrameData::UserStoppedSpeaking
        } else {
            vad(false)
        });
        replay.advance(300);
        assert_eq!(history(&replay.user, Role::Developer), [IDLE]);
    }
}

#[test]
fn only_committed_transcripts_reset_budget_not_noise_replays_or_context_edits() {
    let mut cfg = config();
    cfg.empty_user_turn.as_mut().unwrap().idle_prompt = Some(IDLE.into());
    cfg.empty_user_turn
        .as_mut()
        .unwrap()
        .max_consecutive_recoveries = 2;
    let mut replay = Replay::new(cfg, true);
    replay.empty();
    replay.empty();
    let final_frame = Frame::new(transcript("real words"));
    replay.send_frame(final_frame.clone());
    replay.advance(150);
    replay.empty();
    replay.empty();
    assert_eq!(history(&replay.user, Role::Developer).len(), 4);
    replay.send_frame(final_frame);
    replay.send(FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "context injection")],
        run_llm: false,
    }));
    replay.send(FrameData::InterimTranscription(TranscriptionFrame {
        text: "unconfirmed".into(),
        ..Default::default()
    }));
    replay.send(vad(false));
    replay.advance(300);
    replay.send(transcript(" \n\t"));
    replay.empty();
    assert_eq!(history(&replay.user, Role::Developer).len(), 4);
    assert_eq!(
        history(&replay.user, Role::User),
        ["real words", "context injection"]
    );
}

#[test]
fn muted_and_terminal_turns_never_recover_and_stale_timers_stay_closed() {
    for ending in [
        FrameData::UserMuteStarted,
        FrameData::SttMute(true),
        FrameData::End(EndFrame::default()),
        FrameData::EndDetailed(ReasonFrame::default()),
        FrameData::Stop,
        FrameData::Cancel(CancelFrame::default()),
        FrameData::CancelDetailed(ReasonFrame::default()),
    ] {
        let mut replay = Replay::new(config(), true);
        replay.send(vad(true));
        replay.send(ending);
        replay.send(vad(false));
        replay.advance(10_000);
        assert!(history(&replay.user, Role::Developer).is_empty());
        assert_eq!(replay.inferences, 0);
    }
}

#[test]
fn pending_tool_guard_requires_matching_final_result_or_cancel() {
    for cancel in [false, true] {
        let mut cfg = config();
        cfg.empty_user_turn.as_mut().unwrap().idle_prompt = Some(IDLE.into());
        let mut replay = Replay::new(cfg, false);
        replay.send(FrameData::FunctionCallsStarted(vec![call("one")]));
        replay.empty();
        replay.send(FrameData::FunctionCallResult(result("one", false)));
        replay.send(FrameData::FunctionCallResult(result("unknown", true)));
        let mut mismatched = result("one", true);
        mismatched.arguments = json!({"wrong":true});
        replay.send(FrameData::FunctionCallResult(mismatched));
        replay.empty();
        assert!(history(&replay.user, Role::Developer).is_empty());
        assert!(replay.user.function_calls_in_progress().unwrap());
        replay.send(if cancel {
            FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "lookup".into(),
                tool_call_id: "one".into(),
                run_llm: false,
            })
        } else {
            FrameData::FunctionCallResult(result("one", true))
        });
        replay.send(FrameData::FunctionCallsStarted(vec![call("one")]));
        assert!(!replay.user.function_calls_in_progress().unwrap());
        replay.empty();
        assert_eq!(history(&replay.user, Role::Developer).len(), 1);
        assert_eq!(replay.inferences, 1);
    }
}

#[test]
fn recovery_coalesces_completed_background_tool_inference_at_stop_boundary() {
    let mut replay = Replay::new(config(), true);
    replay.send(FrameData::FunctionCallsStarted(vec![call("one")]));
    replay.send(FrameData::FunctionCallInProgress(
        FunctionCallInProgressFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            cancel_on_interruption: false,
            group_id: Some("group".into()),
        },
    ));
    replay.send(vad(true));
    replay.send(FrameData::FunctionCallResult(result("one", true)));
    replay.send(vad(false));
    let stopped = replay.advance(300);
    assert_eq!(replay.inferences, 1);
    assert_eq!(replay.tool_continuations, 0);
    assert_eq!(history(&replay.user, Role::Developer), [INTERRUPTED]);
    let boundary = stopped
        .actions
        .iter()
        .find_map(|action| match action {
            AggregationAction::Broadcast(frame)
                if matches!(frame.data, FrameData::UserStoppedSpeaking) =>
            {
                Some(frame)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(boundary.metadata["user_context_follows"], true);
    let context = replay.user.context().snapshot().unwrap();
    assert!(context
        .messages
        .iter()
        .any(|message| message.role == Role::Tool));
    replay.send(FrameData::LlmResponseStart);
    replay.send(FrameData::LlmResponseEnd);
    assert_eq!(replay.tool_continuations, 0);
}

#[test]
fn realtime_metadata_remains_explicitly_unsupported_and_nonrealtime_keeps_recovery() {
    let mut replay = Replay::new(config(), false);
    let metadata = LlmServiceMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "test".into(),
            user_turn_strategies: None,
        },
        is_realtime_service: true,
    };
    assert!(replay
        .user
        .process_at(
            Frame::new(FrameData::LlmServiceMetadata(metadata.clone())),
            Direction::Downstream,
            ms(0)
        )
        .is_err());
    assert!(replay
        .user
        .context()
        .snapshot()
        .unwrap()
        .messages
        .is_empty());
    replay.send(FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
        is_realtime_service: false,
        ..metadata
    }));
    replay.empty();
    assert_eq!(history(&replay.user, Role::Developer), [INTERRUPTED]);
}
