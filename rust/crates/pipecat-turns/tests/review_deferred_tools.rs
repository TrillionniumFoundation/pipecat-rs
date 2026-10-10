//! Deferred-tool continuation must survive user boundaries with no new inference.
use pipecat_core::*;
use pipecat_services::Context;
use pipecat_turns::*;
use serde_json::json;
use std::time::Duration;

fn process(assistant: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    assistant
        .process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}
fn upstream_contexts(out: &AggregationOutput) -> usize {
    out.actions.iter().filter(|action| matches!(action, AggregationAction::Push(frame) if frame.direction == Direction::Upstream && matches!(frame.frame.data, FrameData::LlmContext(_)))).count()
}
fn forward_user_boundary(pair: &LLMContextAggregatorPair, out: AggregationOutput) -> usize {
    let mut runs = 0;
    for action in out.actions {
        match action {
            AggregationAction::Broadcast(frame) => {
                runs += upstream_contexts(
                    &pair
                        .assistant()
                        .process_frame(frame, Direction::Downstream)
                        .unwrap(),
                )
            }
            AggregationAction::Interrupt => {
                runs += upstream_contexts(&process(&pair.assistant(), FrameData::Interruption))
            }
            AggregationAction::InterruptBeforeContext => {
                let mut interruption = Frame::interruption();
                interruption
                    .metadata
                    .insert("user_context_follows".into(), true.into());
                runs += upstream_contexts(
                    &pair
                        .assistant()
                        .process_frame(interruption, Direction::Downstream)
                        .unwrap(),
                );
            }
            AggregationAction::Push(frame) if frame.direction == Direction::Downstream => {
                runs += upstream_contexts(
                    &pair
                        .assistant()
                        .process_frame(frame.frame, frame.direction)
                        .unwrap(),
                )
            }
            _ => {}
        }
    }
    runs
}
fn async_result_waiting_for_user() -> LLMContextAggregatorPair {
    async_result_waiting_for_user_with_recovery(Some(EmptyUserTurnConfig::default()))
}
fn async_result_waiting_for_user_with_recovery(
    empty_user_turn: Option<EmptyUserTurnConfig>,
) -> LLMContextAggregatorPair {
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig {
            empty_user_turn,
            ..Default::default()
        },
        AssistantAggregatorConfig::default(),
    );
    process(
        &pair.assistant(),
        FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            cancel_on_interruption: false,
            group_id: Some("group".into()),
        }),
    );
    let start = pair
        .user()
        .process_at(
            Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            })),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    assert_eq!(forward_user_boundary(&pair, start), 0);
    let result = process(
        &pair.assistant(),
        FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            result: json!({"answer":42}),
            run_llm: None,
            is_final: true,
            error: None,
        }),
    );
    assert_eq!(
        upstream_contexts(&result),
        0,
        "must defer while user is speaking"
    );
    assert!(!pair.assistant().has_function_calls_in_progress().unwrap());
    pair
}
#[test]
fn empty_user_turn_releases_the_completed_background_tool_continuation() {
    // Without recovery, the completed tool still owns the next inference.
    let pair = async_result_waiting_for_user_with_recovery(None);
    let stop = pair
        .user()
        .process_at(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.2,
                timestamp: 0.1,
            })),
            Direction::Downstream,
            Duration::from_millis(100),
        )
        .unwrap();
    assert_eq!(forward_user_boundary(&pair, stop), 0);
    let empty = pair.user().advance(Duration::from_millis(5100)).unwrap();
    assert!(empty.events.iter().any(|event| matches!(event, AggregationEvent::UserTurnStopped { text, reason: TurnStopReason::Watchdog } if text.is_empty())));
    assert_eq!(forward_user_boundary(&pair, empty), 1, "an empty turn supplies no user context; the deferred tool result must trigger its own continuation");
}
#[test]
fn muting_during_user_speech_releases_the_completed_background_tool_continuation() {
    let pair = async_result_waiting_for_user();
    let mute = pair
        .user()
        .process_at(
            Frame::new(FrameData::UserMuteStarted),
            Direction::Downstream,
            Duration::from_millis(100),
        )
        .unwrap();
    assert_eq!(
        forward_user_boundary(&pair, mute),
        1,
        "muting supplies no user inference; it must not strand a completed async tool result"
    );
}

#[test]
fn interruption_overtaking_an_admitted_placeholder_cannot_resurrect_cancelled_tool() {
    let assistant = LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig::default(),
    );
    let call = FunctionCall {
        function_name: "lookup".into(),
        tool_call_id: "declared".into(),
        arguments: json!({}),
    };
    process(
        &assistant,
        FrameData::FunctionCallsStarted(vec![call.clone()]),
    );
    // In-progress is a noninterruptible normal frame. Interruption is system priority,
    // so this is a valid processing order even when the placeholder was enqueued first.
    process(&assistant, FrameData::Interruption);
    process(
        &assistant,
        FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
            function_name: call.function_name,
            tool_call_id: call.tool_call_id,
            arguments: call.arguments,
            cancel_on_interruption: true,
            group_id: Some("batch".into()),
        }),
    );
    assert!(!assistant.has_function_calls_in_progress().unwrap(), "a cancelled callback has no future result; its queued placeholder must not reopen the group");
}

#[test]
fn paired_user_start_blocks_tool_inference_before_broadcast_delivery() {
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig::default(),
        AssistantAggregatorConfig::default(),
    );
    process(
        &pair.assistant(),
        FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            cancel_on_interruption: false,
            group_id: Some("group".into()),
        }),
    );
    let _not_yet_delivered = pair
        .user()
        .process_at(
            Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            })),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    let result = process(
        &pair.assistant(),
        FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            result: json!(42),
            run_llm: None,
            is_final: true,
            error: None,
        }),
    );
    assert_eq!(upstream_contexts(&result), 0, "the paired assistant must observe user admission atomically, before asynchronous boundary frames arrive");
}

#[test]
fn nonempty_user_turn_supplies_only_its_own_inference_after_tool_completion() {
    let pair = async_result_waiting_for_user();
    let text = pair
        .user()
        .process_at(
            Frame::new(FrameData::Transcription(TranscriptionFrame {
                text: "next question".into(),
                finalized: true,
                ..Default::default()
            })),
            Direction::Downstream,
            Duration::from_millis(10),
        )
        .unwrap();
    assert_eq!(forward_user_boundary(&pair, text), 0);
    let stop = pair
        .user()
        .process_at(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.2,
                timestamp: 0.1,
            })),
            Direction::Downstream,
            Duration::from_millis(100),
        )
        .unwrap();
    assert_eq!(forward_user_boundary(&pair, stop), 0);
    let end = pair.user().advance(Duration::from_millis(700)).unwrap();
    assert_eq!(end.actions.iter().filter(|action| matches!(action, AggregationAction::Push(frame) if frame.direction == Direction::Downstream && matches!(frame.frame.data, FrameData::LlmContext(_)))).count(), 1);
    assert_eq!(
        forward_user_boundary(&pair, end),
        0,
        "the user context already owns continuation; speaking-stop must not run again"
    );
    process(&pair.assistant(), FrameData::LlmResponseStart);
    assert_eq!(upstream_contexts(&process(&pair.assistant(), FrameData::LlmResponseEnd)), 0, "a tool result captured in the user request must not force a redundant post-response inference");
}

#[test]
fn typed_input_replacing_an_empty_voice_turn_supplies_only_one_inference() {
    let pair = async_result_waiting_for_user();
    let typed = pair
        .user()
        .process_at(
            Frame::new(FrameData::InputText(TextFrame::new("next question"))),
            Direction::Downstream,
            Duration::from_millis(100),
        )
        .unwrap();
    assert_eq!(typed.actions.iter().filter(|action| matches!(action, AggregationAction::Push(frame) if frame.direction == Direction::Downstream && matches!(frame.frame.data, FrameData::LlmContext(_)))).count(), 1);
    assert_eq!(
        forward_user_boundary(&pair, typed),
        0,
        "the stop preceding typed context must not release a duplicate tool continuation"
    );
}

fn tool_result_after_user_snapshot(deliver_stop_first: bool) {
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig::default(),
        AssistantAggregatorConfig::default(),
    );
    process(
        &pair.assistant(),
        FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            cancel_on_interruption: false,
            group_id: Some("group".into()),
        }),
    );
    let start = pair
        .user()
        .process_at(
            Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            })),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap();
    forward_user_boundary(&pair, start);
    let text = pair
        .user()
        .process_at(
            Frame::new(FrameData::Transcription(TranscriptionFrame {
                text: "new question".into(),
                finalized: true,
                ..Default::default()
            })),
            Direction::Downstream,
            Duration::from_millis(10),
        )
        .unwrap();
    forward_user_boundary(&pair, text);
    let stop = pair
        .user()
        .process_at(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.2,
                timestamp: 0.1,
            })),
            Direction::Downstream,
            Duration::from_millis(100),
        )
        .unwrap();
    forward_user_boundary(&pair, stop);
    let mut committed = Some(pair.user().advance(Duration::from_millis(700)).unwrap());
    if deliver_stop_first {
        forward_user_boundary(&pair, committed.take().unwrap());
    }
    // The user context is already enqueued to the LLM, but its normal response-start
    // marker can arrive after an independently completing tool callback.
    let result = process(
        &pair.assistant(),
        FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            result: json!(42),
            run_llm: None,
            is_final: true,
            error: None,
        }),
    );
    assert_eq!(upstream_contexts(&result), 0, "a pending user inference must gate tool-triggered inference until its response lifecycle settles");
    if let Some(committed) = committed {
        assert_eq!(forward_user_boundary(&pair, committed), 0);
    }
    process(&pair.assistant(), FrameData::LlmResponseStart);
    let done = process(&pair.assistant(), FrameData::LlmResponseEnd);
    assert_eq!(upstream_contexts(&done), 1, "the completed result arrived after the request snapshot; it still needs one inference when the current response ends");
}

#[test]
fn explicit_cancel_overtaking_placeholder_cannot_resurrect_a_stopped_callback() {
    for cancel_on_interruption in [true, false] {
        let assistant = LLMAssistantAggregator::new(
            SharedContext::default(),
            AssistantAggregatorConfig::default(),
        );
        process(
            &assistant,
            FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "lookup".into(),
                tool_call_id: "declared".into(),
                arguments: json!({}),
            }]),
        );
        process(
            &assistant,
            FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "lookup".into(),
                tool_call_id: "declared".into(),
                run_llm: false,
            }),
        );
        process(
            &assistant,
            FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
                function_name: "lookup".into(),
                tool_call_id: "declared".into(),
                arguments: json!({}),
                cancel_on_interruption,
                group_id: Some("batch".into()),
            }),
        );
        assert!(!assistant.has_function_calls_in_progress().unwrap(), "explicit cancellation stops both interruptible and persistent callbacks, even when its system frame overtakes their normal placeholder");
    }
}

#[test]
fn tool_result_after_nonempty_user_stop_waits_for_the_already_requested_response() {
    tool_result_after_user_snapshot(true);
}
#[test]
fn tool_result_between_user_snapshot_and_boundary_delivery_is_not_lost() {
    tool_result_after_user_snapshot(false);
}
#[test]
fn explicit_interruption_clears_a_cancelled_user_request_gate() {
    let assistant = LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig::default(),
    );
    process(
        &assistant,
        FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            cancel_on_interruption: false,
            group_id: Some("group".into()),
        }),
    );
    let mut stop = Frame::new(FrameData::UserStoppedSpeaking);
    stop.metadata
        .insert("user_context_follows".into(), true.into());
    assistant
        .process_frame(stop, Direction::Downstream)
        .unwrap();
    process(&assistant, FrameData::Interruption);
    let result = process(
        &assistant,
        FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            arguments: json!({}),
            result: json!(42),
            run_llm: None,
            is_final: true,
            error: None,
        }),
    );
    assert_eq!(upstream_contexts(&result), 1, "the interrupted user request will never emit ResponseStart; its gate must not strand a surviving tool result");
}

#[test]
fn automatic_interruption_preserves_post_snapshot_result_for_typed_and_immediate_voice_input() {
    for typed in [true, false] {
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            UserAggregatorConfig {
                turn: UserTurnConfig {
                    speech_timeout: Duration::ZERO,
                    ..Default::default()
                },
                ..Default::default()
            },
            AssistantAggregatorConfig::default(),
        );
        process(
            &pair.assistant(),
            FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
                function_name: "lookup".into(),
                tool_call_id: "one".into(),
                arguments: json!({}),
                cancel_on_interruption: false,
                group_id: Some("group".into()),
            }),
        );
        let input = if typed {
            FrameData::InputText(TextFrame::new("question"))
        } else {
            FrameData::Transcription(TranscriptionFrame {
                text: "question".into(),
                finalized: true,
                ..Default::default()
            })
        };
        let admitted = pair
            .user()
            .process_at(Frame::new(input), Direction::Downstream, Duration::ZERO)
            .unwrap();
        assert!(admitted
            .actions
            .iter()
            .any(|action| matches!(action, AggregationAction::InterruptBeforeContext)));
        let result = process(
            &pair.assistant(),
            FrameData::FunctionCallResult(FunctionCallResultFrame {
                function_name: "lookup".into(),
                tool_call_id: "one".into(),
                arguments: json!({}),
                result: json!(42),
                run_llm: None,
                is_final: true,
                error: None,
            }),
        );
        assert_eq!(upstream_contexts(&result), 0);
        assert_eq!(forward_user_boundary(&pair, admitted), 0);
        process(&pair.assistant(), FrameData::LlmResponseStart);
        let end = process(&pair.assistant(), FrameData::LlmResponseEnd);
        assert_eq!(upstream_contexts(&end), 1, "automatic cancellation of the old response must preserve results newer than the admitted input snapshot");
    }
}

#[tokio::test]
async fn processor_adapter_preserves_the_automatic_interruption_admission_metadata() {
    use std::sync::Arc;
    tokio::time::timeout(Duration::from_secs(2), async {
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            UserAggregatorConfig::default(),
            AssistantAggregatorConfig::default(),
        );
        process(
            &pair.assistant(),
            FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
                function_name: "lookup".into(),
                tool_call_id: "one".into(),
                arguments: json!({}),
                cancel_on_interruption: false,
                group_id: Some("group".into()),
            }),
        );
        let mut worker = Pipeline::new(vec![
            pair.user(),
            Arc::new(IdentityProcessor),
            pair.assistant(),
        ])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputText(TextFrame::new("question"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let mut marked_interruption = false;
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::Interruption) {
                assert_eq!(
                    frame.frame.metadata.get("user_context_follows"),
                    Some(&json!(true))
                );
                marked_interruption = true;
            }
            if matches!(frame.frame.data, FrameData::LlmContext(_)) {
                break;
            }
        }
        assert!(marked_interruption);
        let result = process(
            &pair.assistant(),
            FrameData::FunctionCallResult(FunctionCallResultFrame {
                function_name: "lookup".into(),
                tool_call_id: "one".into(),
                arguments: json!({}),
                result: json!(42),
                run_llm: None,
                is_final: true,
                error: None,
            }),
        );
        assert_eq!(upstream_contexts(&result), 0);
        process(&pair.assistant(), FrameData::LlmResponseStart);
        assert_eq!(
            upstream_contexts(&process(&pair.assistant(), FrameData::LlmResponseEnd)),
            1
        );
        worker.cancel("test complete").await.unwrap();
        assert!(worker.collect().await.unwrap().1.errors.is_empty());
    })
    .await
    .unwrap();
}

#[test]
fn requested_cancel_continuation_waits_for_the_rest_of_its_group() {
    let assistant = LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig::default(),
    );
    for id in ["one", "two"] {
        process(
            &assistant,
            FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
                function_name: "lookup".into(),
                tool_call_id: id.into(),
                arguments: json!({}),
                cancel_on_interruption: true,
                group_id: Some("group".into()),
            }),
        );
    }
    let cancelled = process(
        &assistant,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "lookup".into(),
            tool_call_id: "one".into(),
            run_llm: true,
        }),
    );
    assert_eq!(upstream_contexts(&cancelled), 0);
    let completed = process(
        &assistant,
        FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: "lookup".into(),
            tool_call_id: "two".into(),
            arguments: json!({}),
            result: json!(42),
            run_llm: Some(false),
            is_final: true,
            error: None,
        }),
    );
    assert_eq!(upstream_contexts(&completed), 1, "a cancellation's explicit run request must remain owed while another group member is running");
}
