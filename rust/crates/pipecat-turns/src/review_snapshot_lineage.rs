//! Independent regression for explicit edit range/snapshot lineage consistency.
//! Include as a child module of assistant.rs so the test can hold its real state
//! lock without adding a production scheduling hook.
use super::*;
use crate::{
    AggregationAction, LLMContextAggregatorPair, RealtimeServiceMode, UserAggregatorConfig,
    UserTurnConfig,
};
use pipecat_core::{
    ChatMessage, DetailedMessagesFrame, LlmContextData, MessagesFrame, RealtimeContextIntent,
    TextFrame,
};
use pipecat_services::{Content, Context};
use std::sync::mpsc;
use std::time::{Duration, Instant};

fn edits() -> Vec<FrameData> {
    let mut text = TextFrame::new("original explicit edit");
    text.append_to_context = true;
    vec![
        FrameData::InputText(text),
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "original explicit edit")],
            run_llm: true,
        }),
        FrameData::LlmMessagesAppendDetailed(DetailedMessagesFrame {
            messages: vec![text_message(ChatRole::User, "original explicit edit")],
            run_llm: Some(true),
        }),
    ]
}

fn pair() -> LLMContextAggregatorPair {
    let user = UserAggregatorConfig {
        turn: UserTurnConfig {
            enable_interruptions: false,
            ..Default::default()
        },
        ..Default::default()
    };
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        user,
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    pair.context()
        .append(Message::text(Role::User, "prefix"))
        .unwrap();
    pair
}

fn is_original_edit(context: &Context) -> bool {
    context.messages.len() == 2
        && matches!(&context.messages[1].content, Some(Content::Text(text)) if text == "original explicit edit")
}

fn run_interleaving(edit: FrameData, replacement: Option<bool>) {
    let pair = pair();
    let assistant = pair.assistant();
    let shared = pair.context();
    // This real lock is exactly what prepare_user_context must acquire after
    // the user has dropped the context lock containing its new append range.
    let mut state = assistant.lock().unwrap();
    state.run_owed = true;
    state.tool_run_owed = true;
    state.cancelled_run_groups.insert("prior owed group".into());
    assert!(!state.awaiting_user_inference);
    let user = pair.user();
    let retry_edit = edit.clone();
    let (done, result) = mpsc::sync_channel(1);
    let worker = std::thread::spawn(move || {
        let output = user.process_at(Frame::new(edit), Direction::Downstream, Duration::ZERO);
        let _ = done.send(output);
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    let edited = loop {
        let snapshot = shared.snapshot().unwrap();
        if is_original_edit(&snapshot) || Instant::now() >= deadline {
            break snapshot;
        }
        std::thread::yield_now();
    };
    // Release before asserting so any fixture failure cannot strand the worker.
    if !is_original_edit(&edited) {
        drop(state);
        let result = result.recv_timeout(Duration::from_secs(5));
        panic!("explicit edit did not reach the held snapshot boundary: {edited:?}; {result:?}");
    }
    assert!(
        result.try_recv().is_err(),
        "snapshot passed the held assistant lock"
    );
    match replacement {
        Some(true) => shared
            .replace_rich(LlmContextData {
                messages: vec![
                    text_message(ChatRole::User, "replacement prefix"),
                    text_message(ChatRole::User, "substituted unrelated content"),
                ],
                tools: pipecat_core::ToolSet::NotGiven,
                tool_choice: None,
            })
            .unwrap(),
        Some(false) => {
            let replacement = Context::new(vec![
                Message::text(Role::User, "replacement prefix"),
                Message::text(Role::User, "substituted unrelated content"),
            ]);
            shared.replace(replacement).unwrap();
        }
        None => shared
            .append(Message::text(Role::Assistant, "concurrent ordinary append"))
            .unwrap(),
    }
    let retained = shared.snapshot().unwrap();
    drop(state);
    let output = result
        .recv_timeout(Duration::from_secs(5))
        .expect("snapshot operation deadlocked");
    worker.join().unwrap();
    assert_eq!(
        shared.snapshot().unwrap(),
        retained,
        "snapshot rolled back or rewrote concurrent history"
    );
    if replacement.is_some() {
        let error = output.expect_err("explicit Append was sealed against replacement content");
        assert!(
            error.fatal,
            "divergent explicit stream was not fatal: {error}"
        );
        assert!(error.message.contains("context"), "{error}");
        let state = assistant.lock().unwrap();
        assert!(
            state.run_owed && state.tool_run_owed,
            "failed snapshot acknowledged owed inference"
        );
        assert!(state.cancelled_run_groups.contains("prior owed group"));
        assert!(
            !state.awaiting_user_inference,
            "failed snapshot marked user inference admitted"
        );
        drop(state);
        for retry in [
            FrameData::LlmRun,
            retry_edit,
            FrameData::InputText(TextFrame::new("later edit")),
        ] {
            assert!(
                pair.user()
                    .process_at(Frame::new(retry), Direction::Downstream, Duration::ZERO)
                    .is_err(),
                "poisoned explicit stream silently resumed"
            );
            assert_eq!(
                shared.snapshot().unwrap(),
                retained,
                "retry mutated replacement history"
            );
        }
        assert!(
            is_original_edit(&edited),
            "original edit was never retained before replacement"
        );
    } else {
        let output = output.unwrap();
        let frames: Vec<_> = output
            .actions
            .iter()
            .filter_map(|action| match action {
                AggregationAction::Push(frame)
                    if frame
                        .frame
                        .native_realtime_context_cause()
                        .unwrap()
                        .is_some() =>
                {
                    Some(&frame.frame)
                }
                _ => None,
            })
            .collect();
        assert_eq!(frames.len(), 1);
        let cause = frames[0].native_realtime_context_cause().unwrap().unwrap();
        assert_eq!(cause.sequence(), 1);
        assert_eq!(
            cause.intent(),
            RealtimeContextIntent::Append { start: 1, end: 2 }
        );
        assert!(cause.run_llm());
        match &frames[0].data {
            FrameData::LlmContext(frame) => {
                assert_eq!(frame.messages.len(), 3);
                assert_eq!(frame.messages[1].content, "original explicit edit");
                assert_eq!(frame.messages[2].content, "concurrent ordinary append");
            }
            FrameData::LlmContextDetailed(frame) => {
                assert_eq!(frame.context.messages.len(), 3);
                assert_eq!(
                    frame.context.messages[1].content,
                    MessageContent::Text("original explicit edit".into())
                );
                assert_eq!(
                    frame.context.messages[2].content,
                    MessageContent::Text("concurrent ordinary append".into())
                );
            }
            other => panic!("unexpected caused frame {other:?}"),
        }
        let state = assistant.lock().unwrap();
        assert!(!state.run_owed && !state.tool_run_owed);
        assert!(state.cancelled_run_groups.is_empty());
        assert!(state.awaiting_user_inference);
    }
}

#[test]
fn review_explicit_append_rejects_replacement_between_edit_and_snapshot_without_acknowledging_inference(
) {
    for edit in edits() {
        for rich in [false, true] {
            run_interleaving(edit.clone(), Some(rich));
        }
    }
}

#[test]
fn review_explicit_append_allows_concurrent_append_and_seals_original_range() {
    for edit in edits() {
        run_interleaving(edit, None);
    }
}
