//! Source-mapped selection, thresholds, manual overrides and correlated replacement.
use pipecat_core::*;
use pipecat_services::{Content, ContentPart, Context, Message, Role};
use pipecat_turns::*;
use serde_json::json;
use std::collections::BTreeMap;

fn message(role: ChatRole, text: &str) -> ContextMessage {
    ContextMessage {
        role,
        content: MessageContent::Text(text.into()),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: BTreeMap::new(),
    }
}
fn history() -> SharedContext {
    SharedContext::new_rich(LlmContextData {
        messages: vec![
            message(ChatRole::System, "instructions"),
            message(ChatRole::User, "first question"),
            message(ChatRole::Assistant, "first answer"),
            message(ChatRole::User, "recent question"),
            message(ChatRole::Assistant, "recent answer"),
        ],
        tools: ToolSet::NotGiven,
        tool_choice: None,
    })
}
fn config(keep: usize) -> ContextSummarizationConfig {
    ContextSummarizationConfig {
        summary: LlmContextSummaryConfig {
            min_messages_after_summary: keep,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn result(request: &LlmContextSummaryRequestFrame) -> LlmContextSummaryResultFrame {
    let range =
        get_messages_to_summarize(&request.context.messages, request.min_messages_to_keep).unwrap();
    LlmContextSummaryResultFrame {
        request_id: request.request_id.clone(),
        summary: "User asked a question; assistant answered".into(),
        last_summarized_index: range.end - 1,
        error: None,
    }
}
fn call(ids: &[&str]) -> ContextMessage {
    ContextMessage {
        tool_calls: ids
            .iter()
            .map(|id| FunctionCall {
                tool_call_id: (*id).into(),
                function_name: "lookup".into(),
                arguments: json!({}),
            })
            .collect(),
        ..message(ChatRole::Assistant, "")
    }
}
fn tool(id: &str, text: &str) -> ContextMessage {
    ContextMessage {
        tool_call_id: Some(id.into()),
        ..message(ChatRole::Tool, text)
    }
}
#[test]
fn selection_preserves_all_leading_systems_but_not_mid_conversation_injections() {
    let messages = vec![
        message(ChatRole::System, "first"),
        message(ChatRole::System, "second"),
        message(ChatRole::User, "old"),
        message(ChatRole::System, "mid"),
        message(ChatRole::User, "latest"),
    ];
    assert_eq!(get_messages_to_summarize(&messages, 1), Some(2..4));
    assert_eq!(get_messages_to_summarize(&messages, 100), None);
    assert_eq!(get_messages_to_summarize(&messages[..2], 0), None);
    assert_eq!(get_messages_to_summarize(&[], 0), None);
}
#[test]
fn selection_requires_complete_tool_groups_inside_selected_prefix() {
    for pending in [
        "IN_PROGRESS",
        r#"{"type":"async_tool","status":"started"}"#,
        r#"{"type":"async_tool","status":"running"}"#,
    ] {
        let messages = vec![
            message(ChatRole::User, "old"),
            call(&["a", "b"]),
            tool("a", "done"),
            tool("b", pending),
            message(ChatRole::User, "tail"),
        ];
        assert_eq!(get_messages_to_summarize(&messages, 1), Some(0..1));
    }
    let messages = vec![
        message(ChatRole::User, "old"),
        call(&["a", "b"]),
        tool("a", "done"),
        tool("b", "done"),
        message(ChatRole::User, "tail"),
    ];
    assert_eq!(get_messages_to_summarize(&messages, 1), Some(0..4));
    assert_eq!(get_messages_to_summarize(&messages, 2), Some(0..1));
}
#[test]
fn fixed_point_moves_back_across_overlapping_tool_groups() {
    let messages = vec![
        message(ChatRole::User, "old"),
        call(&["a"]),
        call(&["b"]),
        tool("a", "done"),
        tool("b", "IN_PROGRESS"),
        message(ChatRole::User, "tail"),
    ];
    assert_eq!(get_messages_to_summarize(&messages, 1), Some(0..1));
}
#[test]
fn asynchronous_completion_and_orphans_are_handled_safely() {
    let mut messages = vec![
        message(ChatRole::User, "old"),
        call(&["a"]),
        tool("a", r#"{"type":"async_tool","status":"running"}"#),
        message(
            ChatRole::Developer,
            r#"{"type":"async_tool","status":"finished","tool_call_id":"a","result":"done"}"#,
        ),
        message(ChatRole::User, "tail"),
    ];
    assert_eq!(get_messages_to_summarize(&messages, 1), Some(0..4));
    assert_eq!(get_messages_to_summarize(&messages, 2), Some(0..1));
    messages[1] = tool("orphan", "result");
    assert_eq!(get_messages_to_summarize(&messages, 1), Some(0..1));
}
#[test]
fn nonadjacent_suffix_results_preserve_their_declarations() {
    let messages = vec![
        message(ChatRole::User, "old"),
        call(&["a"]),
        tool("a", "done"),
        message(ChatRole::User, "new"),
        tool("a", "duplicate or deferred"),
    ];
    assert_eq!(get_messages_to_summarize(&messages, 2), Some(0..1));
}
#[test]
fn manual_default_and_per_request_template_are_user_messages() {
    let context = history();
    let summarizer = LLMContextSummarizer::new(context.clone(), config(2)).unwrap();
    assert!(summarizer.request(None, false).unwrap().is_none());
    let override_config = LlmContextSummaryConfig {
        min_messages_after_summary: 0,
        target_context_tokens: 123,
        summary_message_template: "<summary>{summary}</summary>".into(),
        summarization_prompt: Some("custom prompt".into()),
        summarization_timeout: 2.5,
        ..Default::default()
    };
    let request = summarizer
        .request(Some(&override_config), true)
        .unwrap()
        .unwrap();
    assert_eq!(request.min_messages_to_keep, 0);
    assert_eq!(request.target_context_tokens, 123);
    assert_eq!(request.summarization_prompt, "custom prompt");
    assert_eq!(request.summarization_timeout, Some(2.5));
    assert!(summarizer.request(None, true).unwrap().is_none());
    let event = summarizer.apply(&result(&request)).unwrap().unwrap();
    assert_eq!(
        event,
        SummaryAppliedEvent {
            original_message_count: 5,
            new_message_count: 2,
            summarized_message_count: 4,
            preserved_message_count: 1
        }
    );
    let messages = context.snapshot_rich().unwrap().messages;
    assert_eq!(messages[1].role, ChatRole::User);
    assert!(
        matches!(&messages[1].content,MessageContent::Text(text) if text.starts_with("<summary>"))
    );
}
#[test]
fn auto_thresholds_are_independent_and_exclude_systems_and_current_summary() {
    for token_trigger in [false, true] {
        let context = history();
        let mut settings = config(2);
        settings.auto_trigger = true;
        settings.max_context_tokens = token_trigger.then_some(1);
        settings.max_unsummarized_messages = (!token_trigger).then_some(4);
        let summarizer = LLMContextSummarizer::new(context.clone(), settings).unwrap();
        let request = summarizer.request(None, false).unwrap().unwrap();
        if token_trigger {
            assert_eq!(request.target_context_tokens, 1);
        }
        summarizer.apply(&result(&request)).unwrap().unwrap();
        if !token_trigger {
            assert!(summarizer.request(None, false).unwrap().is_none());
            context.append_rich(message(ChatRole::User, "a")).unwrap();
            assert!(summarizer.request(None, false).unwrap().is_none());
            context
                .append_rich(message(ChatRole::Assistant, "b"))
                .unwrap();
            assert!(summarizer.request(None, false).unwrap().is_some());
        }
    }
}
#[test]
fn token_estimate_includes_images_calls_and_result_overhead() {
    let mut messages = vec![
        message(ChatRole::User, "12345678"),
        call(&["a"]),
        tool("a", "1234"),
    ];
    messages[0].content = MessageContent::Parts(vec![
        MessagePart::Text {
            text: "12345678".into(),
        },
        MessagePart::Image {
            url: "x".into(),
            detail: None,
        },
    ]);
    assert_eq!(estimate_context_tokens(&messages), 544);
}
#[test]
fn concurrent_suffix_and_rich_extensions_survive_exactly() {
    let context = history();
    let mut rich = message(ChatRole::User, "recent");
    rich.content = MessageContent::Parts(vec![MessagePart::Image {
        url: "https://invalid.test/image".into(),
        detail: Some("high".into()),
    }]);
    rich.extra
        .insert("provider".into(), json!({"signature":"opaque"}));
    context.append_rich(rich.clone()).unwrap();
    let summarizer = LLMContextSummarizer::new(context.clone(), config(1)).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    let suffix = message(ChatRole::User, "arrived during inference");
    context.append_rich(suffix.clone()).unwrap();
    let event = summarizer.apply(&result(&request)).unwrap().unwrap();
    let messages = context.snapshot_rich().unwrap().messages;
    assert_eq!(&messages[2..], &[rich, suffix]);
    assert_eq!(event.original_message_count, 7);
    assert_eq!(event.preserved_message_count, 3);
}
#[test]
fn native_originals_are_remapped_by_index_without_equal_projection_confusion() {
    let null = Message {
        role: Role::Assistant,
        content: None,
        tool_calls: vec![],
        tool_call_id: None,
        name: None,
    };
    let empty = Message::text(Role::Assistant, "");
    let rich = Message {
        content: Some(Content::Parts(vec![ContentPart::ImageUrlDetailed {
            url: "https://invalid.test/image".into(),
            detail: "high".into(),
        }])),
        ..Message::text(Role::User, "")
    };
    let context = SharedContext::new(Context::new(vec![
        Message::text(Role::System, "rules"),
        Message::text(Role::User, "compress"),
        null.clone(),
        empty.clone(),
        rich.clone(),
    ]));
    let summarizer = LLMContextSummarizer::new(context.clone(), config(3)).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    summarizer.apply(&result(&request)).unwrap().unwrap();
    assert_eq!(
        &context.snapshot().unwrap().messages[2..],
        &[null, empty, rich]
    );
}
#[test]
fn replacement_even_with_equal_messages_rejects_old_result_and_resets_marker() {
    let context = history();
    let summarizer = LLMContextSummarizer::new(context.clone(), config(2)).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    context
        .replace_rich(context.snapshot_rich().unwrap())
        .unwrap();
    assert!(summarizer.apply(&result(&request)).unwrap().is_none());
    assert!(!summarizer.is_pending());
}
#[test]
fn wrong_identity_does_not_consume_current_request_and_wrong_range_does() {
    let summarizer = LLMContextSummarizer::new(history(), config(2)).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    let mut response = result(&request);
    response.request_id = "stale".into();
    assert!(summarizer.apply(&response).unwrap().is_none());
    assert!(summarizer.is_pending());
    response.request_id = request.request_id;
    response.last_summarized_index += 1;
    assert!(summarizer.apply(&response).is_err());
    assert!(!summarizer.is_pending());
}
#[test]
fn empty_error_and_oversized_results_never_mutate_context_and_allow_retry() {
    for bad in ["", " ", &"x".repeat(1024 * 1024 + 1)] {
        let context = history();
        let before = context.snapshot_rich().unwrap();
        let summarizer = LLMContextSummarizer::new(context.clone(), config(2)).unwrap();
        let request = summarizer.request(None, true).unwrap().unwrap();
        let mut response = result(&request);
        response.summary = bad.into();
        assert!(summarizer.apply(&response).is_err());
        assert_eq!(context.snapshot_rich().unwrap(), before);
        assert!(summarizer.request(None, true).unwrap().is_some());
    }
}
#[test]
fn interrupted_results_are_stale_even_without_a_newer_request() {
    let context = history();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default())
        .with_summarization(config(2))
        .unwrap();
    let out = assistant
        .process_frame(
            Frame::new(FrameData::LlmSummarizeContext(None)),
            Direction::Downstream,
        )
        .unwrap();
    let AggregationAction::Push(frame) = &out.actions[0] else {
        panic!()
    };
    let FrameData::LlmContextSummaryRequest(request) = &frame.frame.data else {
        panic!()
    };
    assistant
        .process_frame(Frame::interruption(), Direction::Downstream)
        .unwrap();
    assert!(assistant
        .process_frame(
            Frame::new(FrameData::LlmContextSummaryResult(result(request))),
            Direction::Downstream
        )
        .unwrap()
        .events
        .is_empty());
    assert_eq!(context.snapshot_rich().unwrap().messages.len(), 5);
}
#[test]
fn aggregator_applies_summary_emits_metrics_and_deduplicates_manual_frames() {
    let context = history();
    let assistant = LLMAssistantAggregator::new(context, Default::default())
        .with_summarization(config(2))
        .unwrap();
    let manual = Frame::new(FrameData::LlmSummarizeContext(None));
    let out = assistant
        .process_frame(manual.clone(), Direction::Downstream)
        .unwrap();
    assert!(assistant
        .process_frame(manual, Direction::Downstream)
        .unwrap()
        .actions
        .is_empty());
    let AggregationAction::Push(frame) = &out.actions[0] else {
        panic!()
    };
    let FrameData::LlmContextSummaryRequest(request) = &frame.frame.data else {
        panic!()
    };
    let out = assistant
        .process_frame(
            Frame::new(FrameData::LlmContextSummaryResult(result(request))),
            Direction::Downstream,
        )
        .unwrap();
    assert!(matches!(
        &out.events[0],
        AggregationEvent::SummaryApplied(SummaryAppliedEvent {
            summarized_message_count: 2,
            ..
        })
    ));
}
#[test]
fn configuration_is_validated_without_launching_io() {
    for timeout in [0.0, -1.0, f64::NAN, f64::INFINITY, 86401.0] {
        let mut settings = config(2);
        settings.summary.summarization_timeout = timeout;
        assert!(LLMContextSummarizer::new(history(), settings).is_err());
    }
    let mut settings = config(2);
    settings.summary.summary_message_template = "missing placeholder".into();
    assert!(LLMContextSummarizer::new(history(), settings).is_err());
    let mut settings = config(2);
    settings.summary.target_context_tokens = 0;
    assert!(LLMContextSummarizer::new(history(), settings).is_err());
    let mut settings = config(2);
    settings.auto_trigger = true;
    settings.max_context_tokens = None;
    settings.max_unsummarized_messages = None;
    assert!(LLMContextSummarizer::new(history(), settings).is_err());
}
#[test]
fn native_inline_data_is_explicitly_preserved_in_summary_transcript() {
    let context = SharedContext::new(Context::new(vec![Message {
        content: Some(Content::Parts(vec![ContentPart::InlineData {
            mime_type: "image/png".into(),
            data: "AAAA".into(),
        }])),
        ..Message::text(Role::User, "")
    }]));
    let summarizer = LLMContextSummarizer::new(context, config(0)).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    assert_eq!(
        request.context.messages[0].extra["pipecat_native_message"]["content"][0]["type"],
        "inline_data"
    );
}
#[tokio::test(start_paused = true)]
async fn expired_result_never_applies_and_manual_retry_gets_a_new_identity() {
    let context = history();
    let mut settings = config(2);
    settings.summary.summarization_timeout = 1.0;
    let summarizer = LLMContextSummarizer::new(context, settings).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    tokio::time::advance(std::time::Duration::from_secs(2)).await;
    assert!(summarizer.apply(&result(&request)).is_err());
    assert_ne!(
        request.request_id,
        summarizer.request(None, true).unwrap().unwrap().request_id
    );
}

#[test]
fn a_new_request_after_external_context_replacement_invalidates_old_pending_work() {
    let context = history();
    let summarizer = LLMContextSummarizer::new(context.clone(), config(2)).unwrap();
    let original = summarizer.request(None, true).unwrap().unwrap();
    context
        .replace_rich(context.snapshot_rich().unwrap())
        .unwrap();
    let fresh = summarizer.request(None, true).unwrap().unwrap();
    assert_ne!(original.request_id, fresh.request_id);
    assert!(summarizer.apply(&result(&original)).unwrap().is_none());
    assert!(summarizer.is_pending());
    assert!(summarizer.apply(&result(&fresh)).unwrap().is_some());
}
