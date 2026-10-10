//! Universal frame behavior traced to llm_response_universal.py and turn strategies.
use pipecat_core::*;
use pipecat_services::{Content, ContentPart, Context, Message, Role};
use pipecat_turns::*;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn user() -> LLMUserAggregator {
    LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                speech_timeout: Duration::from_millis(100),
                enable_interruptions: false,
                ..Default::default()
            },
            ..Default::default()
        },
    )
}
fn assistant() -> LLMAssistantAggregator {
    LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig::default(),
    )
}
fn final_text(text: &str, spaces: bool, append: bool, finalized: bool) -> Frame {
    Frame::new(FrameData::TranscriptionDetailed(
        DetailedTranscriptionFrame {
            text: TextFrame {
                text: text.into(),
                includes_inter_frame_spaces: spaces,
                append_to_context: append,
                skip_tts: Some(true),
            },
            user_id: "speaker-7".into(),
            timestamp: "2026-10-09T12:00:00Z".into(),
            language: Some("en-US".into()),
            result: Some(json!({"words":[{"word":text,"confidence":0.9}]})),
            finalized,
        },
    ))
}
fn user_at(user: &LLMUserAggregator, frame: Frame, ms: u64) -> AggregationOutput {
    user.process_at(frame, Direction::Downstream, Duration::from_millis(ms))
        .unwrap()
}
fn rich_message(text: &str) -> ContextMessage {
    ContextMessage {
        role: ChatRole::User,
        content: MessageContent::Text(text.into()),
        name: Some("person".into()),
        tool_call_id: None,
        tool_calls: vec![],
        extra: BTreeMap::from([("provider_extra".into(), json!({"precise":false}))]),
    }
}
fn pushes(out: &AggregationOutput) -> Vec<&FrameData> {
    out.actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame) => Some(&frame.frame.data),
            _ => None,
        })
        .collect()
}
fn start() -> Frame {
    Frame::new(FrameData::VadUserStartedSpeakingDetailed(
        VadUserStartedSpeakingFrame {
            start_secs: 0.12,
            timestamp: 0.0,
        },
    ))
}
fn stop() -> Frame {
    Frame::new(FrameData::VadUserStoppedSpeakingDetailed(
        VadUserStoppedSpeakingFrame {
            stop_secs: 0.2,
            timestamp: 0.0,
        },
    ))
}
fn process(assistant: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    assistant
        .process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}

#[test]
fn detailed_transcripts_preserve_spacing_flags_and_entire_original_payload() {
    let user = user();
    user_at(&user, start(), 0);
    let mut first = final_text("Hello ", true, true, false);
    first.metadata.insert("trace".into(), json!({"id":42}));
    first.interruptible = false;
    user_at(&user, first.clone(), 1);
    user_at(&user, final_text("world", true, true, true), 2);
    user_at(&user, stop(), 3);
    assert!(user
        .advance(Duration::from_millis(102))
        .unwrap()
        .events
        .is_empty());
    let out = user.advance(Duration::from_millis(103)).unwrap();
    assert!(out.events.iter().any(
        |event| matches!(event,AggregationEvent::UserTurnStopped{text,..} if text=="Hello world")
    ));
    assert_eq!(user.transcript_frames().unwrap()[0], first);
    assert_eq!(
        user.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("Hello world".into()))
    );
}
#[test]
fn detailed_append_false_closes_turn_without_context_or_leaking_transcript() {
    let user = user();
    user_at(&user, start(), 0);
    user_at(&user, final_text("secret", false, false, true), 1);
    user_at(&user, stop(), 2);
    let out = user.advance(Duration::from_millis(102)).unwrap();
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    assert!(pushes(&out).is_empty());
    assert_eq!(user.transcript_frames().unwrap().len(), 1);
}
#[test]
fn detailed_interim_invalidates_final_and_stt_metadata_drives_actual_budget() {
    let user = user();
    user_at(
        &user,
        Frame::new(FrameData::SttMetadata(SttMetadataFrame {
            base: ServiceMetadataFrame {
                service_name: "test-stt".into(),
                user_turn_strategies: None,
            },
            ttfs_p99_latency: 0.8,
        })),
        0,
    );
    user_at(&user, start(), 0);
    user_at(&user, final_text("first", false, true, true), 1);
    let FrameData::TranscriptionDetailed(text) = final_text("tail", false, true, false).data else {
        unreachable!()
    };
    user_at(
        &user,
        Frame::new(FrameData::InterimTranscriptionDetailed(text)),
        2,
    );
    user_at(&user, stop(), 10);
    assert!(user
        .advance(Duration::from_millis(609))
        .unwrap()
        .events
        .is_empty());
    let out = user.advance(Duration::from_millis(610)).unwrap();
    assert!(out
        .events
        .iter()
        .any(|e| matches!(e,AggregationEvent::UserTurnStopped{text,..} if text=="first")));
    assert_eq!(user.interim_text().unwrap(), "");
}
#[test]
fn muted_rich_frames_are_suppressed_and_replay_is_idempotent() {
    let user = user();
    user_at(&user, Frame::new(FrameData::SttMute(true)), 0);
    user_at(&user, start(), 1);
    user_at(&user, final_text("muted", false, true, true), 2);
    assert!(user.transcript_frames().unwrap().is_empty());
    user_at(&user, Frame::new(FrameData::SttMute(false)), 3);
    let frame = final_text("accepted", false, true, true);
    user_at(&user, frame.clone(), 4);
    user_at(&user, frame, 5);
    user.advance(Duration::from_millis(104)).unwrap();
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    assert_eq!(user.transcript_frames().unwrap().len(), 1);
}
#[test]
fn rich_history_preserves_extensions_executable_tools_and_exact_frames() {
    let tools = ToolSet::Direct(vec![DirectFunction {
        schema: ToolDefinition {
            name: "native".into(),
            description: "local".into(),
            parameters: json!({}),
        },
        handler: LocalToolFunction::new(|_| async { Ok(json!(7)) }),
    }]);
    let context = LlmContextData {
        messages: vec![rich_message("hi")],
        tools,
        tool_choice: Some(ToolChoice::Required),
    };
    let shared = SharedContext::new_rich(context.clone());
    assert_eq!(shared.snapshot_rich().unwrap(), context);
    assert!(shared.snapshot().is_err());
    assert!(shared.frame().is_err());
    let FrameData::LlmContextDetailed(frame) = shared.frame_data().unwrap() else {
        panic!()
    };
    assert_eq!(frame.context, context);
    assert!(!frame.speculation);
}
#[test]
fn native_parts_are_supported_without_losing_image_detail() {
    let original = Context::new(vec![Message {
        role: Role::User,
        content: Some(Content::Parts(vec![ContentPart::ImageUrlDetailed {
            url: "https://example.test/image.png".into(),
            detail: "high".into(),
        }])),
        name: None,
        tool_calls: vec![],
        tool_call_id: None,
    }]);
    let shared = SharedContext::new(original.clone());
    assert_eq!(shared.snapshot().unwrap(), original);
    assert!(shared.frame().is_err());
    let FrameData::LlmContextDetailed(frame) = shared.frame_data().unwrap() else {
        panic!()
    };
    assert!(
        matches!(&frame.context.messages[0].content,MessageContent::Parts(parts) if matches!(&parts[0],MessagePart::Image{detail:Some(detail),..} if detail=="high"))
    );
}
#[test]
fn native_inline_data_is_retained_but_ambiguous_framing_is_rejected() {
    let original = Context::new(vec![Message {
        role: Role::User,
        content: Some(Content::Parts(vec![ContentPart::InlineData {
            data: "abcd".into(),
            mime_type: "image/png".into(),
        }])),
        name: None,
        tool_calls: vec![],
        tool_call_id: None,
    }]);
    let shared = SharedContext::new(original.clone());
    assert_eq!(shared.snapshot().unwrap(), original);
    assert!(shared
        .frame_data()
        .unwrap_err()
        .message
        .contains("InlineData"));
}
#[test]
fn explicit_empty_tools_stay_distinct_from_omission() {
    let omitted = SharedContext::default();
    assert!(matches!(
        omitted.frame_data().unwrap(),
        FrameData::LlmContext(_)
    ));
    let shared = SharedContext::new(Context {
        tools: Some(vec![]),
        ..Default::default()
    });
    assert!(matches!(
        shared.frame_data().unwrap(),
        FrameData::LlmContextDetailed(_)
    ));
    assert!(shared.frame().is_err());
    assert_eq!(shared.snapshot().unwrap().tools, Some(vec![]));
}
#[test]
fn rich_append_transform_and_update_are_lossless_and_none_does_not_run() {
    let user = user();
    let message = rich_message("original");
    let out = user_at(
        &user,
        Frame::new(FrameData::LlmMessagesAppendDetailed(
            DetailedMessagesFrame {
                messages: vec![message.clone()],
                run_llm: None,
            },
        )),
        0,
    );
    assert!(out.actions.is_empty());
    let out = user_at(
        &user,
        Frame::new(FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
            transform: LocalMessageTransform::new(|mut messages| {
                messages[0].name = Some("renamed".into());
                Ok(messages)
            }),
            run_llm: Some(true),
        })),
        1,
    );
    let frames = pushes(&out);
    let FrameData::LlmContextDetailed(frame) = frames[0] else {
        panic!()
    };
    assert_eq!(frame.context.messages[0].extra, message.extra);
    assert_eq!(frame.context.messages[0].name.as_deref(), Some("renamed"));
    user_at(
        &user,
        Frame::new(FrameData::LlmMessagesUpdateDetailed(
            DetailedMessagesFrame {
                messages: vec![],
                run_llm: Some(false),
            },
        )),
        2,
    );
    assert!(user.context().snapshot_rich().unwrap().messages.is_empty());
}
#[test]
fn transform_failure_does_not_mutate_shared_history() {
    let user = user();
    user.context().append_rich(rich_message("keep")).unwrap();
    let old = user.context().snapshot_rich().unwrap();
    assert!(user
        .process_at(
            Frame::new(FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
                transform: LocalMessageTransform::new(|_| Err(ProcessorError::new("rejected"))),
                run_llm: Some(true)
            })),
            Direction::Downstream,
            Duration::ZERO
        )
        .is_err());
    assert_eq!(user.context().snapshot_rich().unwrap(), old);
}
#[test]
fn identity_transform_preserves_distinct_native_occurrences_and_rejects_partial_selection() {
    let messages = vec![
        Message {
            role: Role::User,
            content: Some(Content::Parts(vec![ContentPart::InlineData {
                data: "xx".into(),
                mime_type: "image/png".into(),
            }])),
            name: None,
            tool_calls: vec![],
            tool_call_id: None,
        },
        Message {
            role: Role::User,
            content: Some(Content::Parts(vec![ContentPart::File {
                data: "xx".into(),
                mime_type: "image/png".into(),
                filename: None,
            }])),
            name: None,
            tool_calls: vec![],
            tool_call_id: None,
        },
    ];
    let shared = SharedContext::new(Context::new(messages.clone()));
    let user = LLMUserAggregator::new(shared.clone(), Default::default());
    user_at(
        &user,
        Frame::new(FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
            transform: LocalMessageTransform::new(Ok),
            run_llm: None,
        })),
        0,
    );
    assert_eq!(shared.snapshot().unwrap().messages, messages);
    let failed = user.process_at(
        Frame::new(FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
            transform: LocalMessageTransform::new(|messages| Ok(vec![messages[1].clone()])),
            run_llm: None,
        })),
        Direction::Downstream,
        Duration::ZERO,
    );
    assert!(failed.unwrap_err().message.contains("ambiguously"));
    assert_eq!(shared.snapshot().unwrap().messages, messages);
}
#[test]
fn legacy_update_clears_native_sidecar_and_rich_mode() {
    let shared = SharedContext::new(Context::new(vec![Message::assistant_tool_calls(vec![])]));
    let user = LLMUserAggregator::new(shared.clone(), Default::default());
    user_at(
        &user,
        Frame::new(FrameData::LlmMessagesUpdate(MessagesFrame {
            messages: vec![ChatMessage {
                role: ChatRole::Assistant,
                content: String::new(),
                name: None,
                tool_call_id: None,
                tool_calls: vec![],
            }],
            run_llm: false,
        })),
        0,
    );
    assert_eq!(
        shared.snapshot().unwrap().messages[0].content,
        Some(Content::Text(String::new()))
    );
}
#[test]
fn rich_response_boundaries_commit_and_skip_tts_speech_mode_does_not_wait_for_playback() {
    for mode in [AssistantTextMode::Text, AssistantTextMode::Spoken] {
        let assistant = LLMAssistantAggregator::new(
            SharedContext::default(),
            AssistantAggregatorConfig { text_mode: mode },
        );
        process(
            &assistant,
            FrameData::LlmFullResponseStart(LlmFullResponseFrame {
                skip_tts: Some(true),
            }),
        );
        process(
            &assistant,
            FrameData::LlmText(TextFrame::llm("text-only answer")),
        );
        let out = process(
            &assistant,
            FrameData::LlmFullResponseEnd(LlmFullResponseFrame {
                skip_tts: Some(true),
            }),
        );
        assert!(out.events.iter().any(|event|matches!(event,AggregationEvent::AssistantTurnStopped{text,..} if text=="text-only answer")));
    }
}
fn call() -> FunctionCallInProgressFrame {
    FunctionCallInProgressFrame {
        function_name: "lookup".into(),
        tool_call_id: "call-1".into(),
        arguments: json!({"q":"x"}),
        cancel_on_interruption: true,
        group_id: None,
    }
}
fn result(properties: FunctionCallResultProperties) -> FrameData {
    FrameData::FunctionCallResultDetailed(DetailedFunctionCallResultFrame {
        function_name: "lookup".into(),
        tool_call_id: "call-1".into(),
        arguments: json!({"q":"x"}),
        result: json!(false),
        run_llm: Some(true),
        properties: Some(properties),
        error: None,
    })
}
#[test]
fn rich_tool_result_property_precedence_partial_final_and_callback_are_real() {
    let assistant = assistant();
    process(
        &assistant,
        FrameData::FunctionCallsStartedDetailed(vec![FunctionCallFromLlm {
            function_name: "lookup".into(),
            tool_call_id: "call-1".into(),
            arguments: BTreeMap::from([("q".into(), json!("x"))]),
            context: None,
        }]),
    );
    process(&assistant, FrameData::FunctionCallInProgress(call()));
    process(
        &assistant,
        FrameData::LlmFullResponseStart(Default::default()),
    );
    process(
        &assistant,
        FrameData::LlmFullResponseEnd(Default::default()),
    );
    let out = process(
        &assistant,
        result(FunctionCallResultProperties {
            run_llm: Some(false),
            is_final: false,
            on_context_updated: Some(LocalAsyncCallback::new(|| async { Ok(()) })),
        }),
    );
    assert!(assistant.has_function_calls_in_progress().unwrap());
    assert!(pushes(&out).is_empty());
    assert!(out
        .actions
        .iter()
        .any(|action| matches!(action, AggregationAction::ContextUpdated(_))));
    let out = process(
        &assistant,
        result(FunctionCallResultProperties {
            run_llm: Some(true),
            ..Default::default()
        }),
    );
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    assert_eq!(pushes(&out).len(), 1);
    assert!(assistant.context().snapshot().unwrap().messages.iter().any(
        |message| matches!(&message.content,Some(Content::Text(text)) if text.contains("false"))
    ));
    let ignored = process(
        &assistant,
        result(FunctionCallResultProperties {
            on_context_updated: Some(LocalAsyncCallback::new(|| async { Ok(()) })),
            ..Default::default()
        }),
    );
    assert!(!ignored
        .actions
        .iter()
        .any(|action| matches!(action, AggregationAction::ContextUpdated(_))));
}
#[test]
fn rich_thoughts_retain_provider_and_json_signature_without_becoming_spoken_text() {
    let assistant = assistant();
    process(
        &assistant,
        FrameData::LlmFullResponseStart(Default::default()),
    );
    process(
        &assistant,
        FrameData::LlmThoughtStartDetailed(LlmThoughtStartFrame {
            append_to_context: true,
            llm: Some("native-provider".into()),
        }),
    );
    process(
        &assistant,
        FrameData::LlmThoughtText(TextFrame::llm("reasoned")),
    );
    process(
        &assistant,
        FrameData::LlmThoughtEndDetailed(LlmThoughtEndFrame {
            signature: Some(json!({"signed":[1,2]})),
        }),
    );
    process(&assistant, FrameData::LlmText(TextFrame::llm("answer")));
    let out = process(
        &assistant,
        FrameData::LlmFullResponseEnd(Default::default()),
    );
    let rich = assistant.context().snapshot_rich().unwrap();
    assert_eq!(rich.messages[0].extra["signature"], json!({"signed":[1,2]}));
    assert_eq!(rich.messages[0].extra["llm"], json!("native-provider"));
    assert_eq!(
        rich.messages[1].content,
        MessageContent::Text("answer".into())
    );
    assert!(matches!(pushes(&out)[0], FrameData::LlmContextDetailed(_)));
    assert!(assistant.context().snapshot().is_err());
}
#[tokio::test]
async fn rich_result_callback_executes_after_commit_in_a_native_pipeline() {
    let assistant = Arc::new(assistant());
    let observed = Arc::new(AtomicUsize::new(0));
    let count = observed.clone();
    let context = assistant.context();
    let mut worker = Pipeline::new(vec![assistant])
        .start(Default::default())
        .await
        .unwrap();
    let sender = worker.sender();
    sender
        .send(
            Frame::new(FrameData::FunctionCallInProgress(call())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    sender
        .send(
            Frame::new(result(FunctionCallResultProperties {
                run_llm: Some(false),
                on_context_updated: Some(LocalAsyncCallback::new(move || {
                    let count = count.clone();
                    let context = context.clone();
                    async move {
                        assert_eq!(
                            context.snapshot()?.messages[1].content,
                            Some(Content::Text("false".into()))
                        );
                        count.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                })),
                ..Default::default()
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while observed.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(observed.load(Ordering::SeqCst), 1);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

#[test]
fn native_empty_and_rich_empty_occurrences_do_not_alias_sidecars() {
    let shared = SharedContext::new(Context::new(vec![Message::assistant_tool_calls(vec![])]));
    shared
        .append_rich(ContextMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Text(String::new()),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
            extra: Default::default(),
        })
        .unwrap();
    let snapshot = shared.snapshot().unwrap();
    assert_eq!(snapshot.messages[0].content, None);
    assert_eq!(
        snapshot.messages[1].content,
        Some(Content::Text(String::new()))
    );
    let user = LLMUserAggregator::new(shared.clone(), Default::default());
    let error = user
        .process_at(
            Frame::new(FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
                transform: LocalMessageTransform::new(|messages| Ok(vec![messages[1].clone()])),
                run_llm: None,
            })),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap_err();
    assert!(error.message.contains("ambiguously"));
    assert_eq!(shared.snapshot().unwrap(), snapshot);
}

#[test]
fn rich_append_deferred_during_response_emits_one_full_context_after_end() {
    let assistant = assistant();
    process(
        &assistant,
        FrameData::LlmFullResponseStart(Default::default()),
    );
    process(
        &assistant,
        FrameData::LlmText(TextFrame::llm("in progress")),
    );
    let append = process(
        &assistant,
        FrameData::LlmMessagesAppendDetailed(DetailedMessagesFrame {
            messages: vec![rich_message("new input")],
            run_llm: Some(true),
        }),
    );
    assert!(append.actions.is_empty());
    let out = process(
        &assistant,
        FrameData::LlmFullResponseEnd(Default::default()),
    );
    let upstream: Vec<_> = out
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame) if frame.direction == Direction::Upstream => {
                Some(&frame.frame.data)
            }
            _ => None,
        })
        .collect();
    assert_eq!(upstream.len(), 1);
    let FrameData::LlmContextDetailed(context) = upstream[0] else {
        panic!()
    };
    assert_eq!(context.context.messages.len(), 2);
    assert_eq!(
        context.context.messages[1].content,
        MessageContent::Text("in progress".into())
    );
}

#[tokio::test]
async fn end_cancels_pending_result_callback_instead_of_waiting_forever() {
    let assistant = Arc::new(assistant());
    let begun = Arc::new(AtomicUsize::new(0));
    let ended = Arc::new(AtomicUsize::new(0));
    let began = begun.clone();
    let dropped = ended.clone();
    let mut worker = Pipeline::new(vec![assistant])
        .start(Default::default())
        .await
        .unwrap();
    let sender = worker.sender();
    sender
        .send(
            Frame::new(FrameData::FunctionCallInProgress(call())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    sender
        .send(
            Frame::new(result(FunctionCallResultProperties {
                run_llm: Some(false),
                on_context_updated: Some(LocalAsyncCallback::new(move || {
                    let began = began.clone();
                    let dropped = dropped.clone();
                    async move {
                        struct OnDrop(Arc<AtomicUsize>);
                        impl Drop for OnDrop {
                            fn drop(&mut self) {
                                self.0.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        let _guard = OnDrop(dropped);
                        began.fetch_add(1, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                        Ok(())
                    }
                })),
                ..Default::default()
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while begun.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    worker.end().await.unwrap();
    let report = tokio::time::timeout(Duration::from_secs(1), worker.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(ended.load(Ordering::SeqCst), 1);
}

#[test]
fn invalid_control_metadata_and_unconfigured_source_features_fail_explicitly() {
    let user = user();
    assert!(user
        .process_at(
            Frame::new(FrameData::VadUserStoppedSpeakingDetailed(
                VadUserStoppedSpeakingFrame {
                    stop_secs: f64::NAN,
                    timestamp: 0.0
                }
            )),
            Direction::Downstream,
            Duration::ZERO
        )
        .is_err());
    assert!(user
        .process_at(
            Frame::new(FrameData::ServiceMetadata(ServiceMetadataFrame {
                service_name: "changing".into(),
                user_turn_strategies: Some(UserTurnStrategies {
                    start: Some(vec![]),
                    stop: None
                })
            })),
            Direction::Downstream,
            Duration::ZERO
        )
        .is_err());
    let assistant = assistant();
    assert!(assistant
        .process_frame(
            Frame::new(FrameData::LlmSummarizeContext(None)),
            Direction::Downstream
        )
        .unwrap()
        .actions
        .is_empty());
    assert!(assistant
        .process_frame(
            Frame::new(FrameData::LlmThoughtStartDetailed(LlmThoughtStartFrame {
                append_to_context: true,
                llm: None
            })),
            Direction::Downstream
        )
        .is_err());
}
