//! Independent regressions for compression boundaries and live-turn isolation.
use pipecat_core::*;
use pipecat_services::{
    presets::{OpenAiPreset, OpenAiPresetConfig},
    ClientConfig, CompletionRequest, Content, ContentPart, Context, LlmClient, Message, Provider,
    Role, ToolCall,
};
use pipecat_turns::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn config(keep: usize) -> ContextSummarizationConfig {
    ContextSummarizationConfig {
        summary: LlmContextSummaryConfig {
            min_messages_after_summary: keep,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn result(request: &LlmContextSummaryRequestFrame, end: usize) -> LlmContextSummaryResultFrame {
    LlmContextSummaryResultFrame {
        request_id: request.request_id.clone(),
        summary: "The earlier discussion is complete.".into(),
        last_summarized_index: end - 1,
        error: None,
    }
}

fn with_part(part: ContentPart) -> Message {
    Message {
        content: Some(Content::Parts(vec![part])),
        ..Message::text(Role::User, "")
    }
}

#[test]
fn appended_result_cannot_outlive_its_summarized_declaration() {
    let context = SharedContext::new(Context::new(vec![
        Message::text(Role::User, "Please look this up"),
        Message::assistant_tool_calls(vec![ToolCall {
            id: "lookup-1".into(),
            name: "lookup".into(),
            arguments: json!({}),
        }]),
        Message::tool_result("lookup-1", "first result"),
        Message::text(Role::Assistant, "I found it"),
        Message::text(Role::User, "Keep this recent turn"),
    ]));
    let summarizer = LLMContextSummarizer::new(context.clone(), config(1)).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    context
        .append(Message::tool_result("lookup-1", "late additional result"))
        .unwrap();
    let before = context.snapshot().unwrap();
    assert!(summarizer.apply(&result(&request, 4)).unwrap().is_none());
    assert_eq!(context.snapshot().unwrap(), before);
    assert!(!summarizer.is_pending());
}

#[test]
fn native_inline_data_in_kept_suffix_survives_summary_exactly() {
    let inline = with_part(ContentPart::InlineData {
        mime_type: "image/png".into(),
        data: "aW1hZ2U=".into(),
    });
    let context = SharedContext::new(Context::new(vec![
        Message::text(Role::System, "Keep the instructions"),
        Message::text(Role::User, "Old question"),
        Message::text(Role::Assistant, "Old answer"),
        inline.clone(),
    ]));
    let summarizer = LLMContextSummarizer::new(context.clone(), config(1)).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    assert!(summarizer.apply(&result(&request, 3)).unwrap().is_some());
    let after = context.snapshot().unwrap();
    assert_eq!(after.messages.len(), 3);
    assert_eq!(
        after.messages[0],
        Message::text(Role::System, "Keep the instructions")
    );
    assert_eq!(after.messages[2], inline);
}

#[test]
fn distinct_native_suffix_messages_with_equal_projections_keep_their_order() {
    let inline = with_part(ContentPart::InlineData {
        mime_type: "image/png".into(),
        data: "aW1hZ2U=".into(),
    });
    let file = with_part(ContentPart::File {
        mime_type: "image/png".into(),
        data: "aW1hZ2U=".into(),
        filename: None,
    });
    assert_eq!(
        pipecat_services::frame_context::to_core_message(&inline),
        pipecat_services::frame_context::to_core_message(&file),
    );
    let context = SharedContext::new(Context::new(vec![
        Message::text(Role::User, "Old question"),
        Message::text(Role::Assistant, "Old answer"),
        inline.clone(),
        file.clone(),
        inline.clone(),
    ]));
    let summarizer = LLMContextSummarizer::new(context.clone(), config(3)).unwrap();
    let request = summarizer.request(None, true).unwrap().unwrap();
    assert!(summarizer.apply(&result(&request, 2)).unwrap().is_some());
    assert_eq!(
        context.snapshot().unwrap().messages[1..],
        [inline.clone(), file, inline]
    );
}

#[test]
fn automatic_summary_admission_error_does_not_discard_the_live_assistant_turn() {
    let context = SharedContext::new(Context::new(vec![
        Message::text(Role::User, "x".repeat(16 * 1024 * 1024 + 1)),
        Message::text(Role::User, "Recent question"),
    ]));
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default())
        .with_summarization(ContextSummarizationConfig {
            auto_trigger: true,
            max_context_tokens: None,
            max_unsummarized_messages: Some(1),
            ..config(1)
        })
        .unwrap();
    let started = assistant
        .process_frame(
            Frame::new(FrameData::LlmResponseStart),
            Direction::Downstream,
        )
        .expect("automatic compression failure must not reject response-start bookkeeping");
    assert!(started.actions.iter().any(|action| matches!(
        action,
        AggregationAction::Push(directed)
            if directed.direction == Direction::Upstream
                && matches!(&directed.frame.data, FrameData::Error(error) if !error.fatal)
    )));
    assistant
        .process_frame(
            Frame::new(FrameData::LlmText(TextFrame::llm("Live answer"))),
            Direction::Downstream,
        )
        .unwrap();
    assistant
        .process_frame(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .unwrap();
    assert_eq!(
        context.snapshot().unwrap().messages.last(),
        Some(&Message::text(Role::Assistant, "Live answer"))
    );
    assert!(!assistant.summarizer().is_pending());
}

#[test]
fn long_overlapping_tool_chain_closes_the_entire_dependency_boundary() {
    fn declaration(index: usize) -> Message {
        Message::assistant_tool_calls(vec![ToolCall {
            id: format!("call-{index}"),
            name: "lookup".into(),
            arguments: json!({}),
        }])
    }
    const CALLS: usize = 10_000;
    let mut messages = vec![
        Message::text(Role::System, "Retain instructions"),
        Message::text(Role::User, "An independent old fact"),
        declaration(0),
    ];
    for index in 1..CALLS {
        messages.push(declaration(index));
        messages.push(Message::tool_result(format!("call-{}", index - 1), "done"));
    }
    messages.push(Message::tool_result(format!("call-{}", CALLS - 1), "done"));
    let rich: Vec<_> = messages
        .iter()
        .map(pipecat_services::frame_context::to_core_message)
        .collect();
    // Every call overlaps the next, so preserving the final result retains all
    // declarations and results, but does not pin the unrelated older fact.
    assert_eq!(get_messages_to_summarize(&rich, 1), Some(1..2));
    assert_eq!(get_messages_to_summarize(&rich, 0), Some(1..rich.len()));
}

#[tokio::test]
async fn provider_template_cannot_override_summary_budget_or_enable_legacy_tools() {
    for source in [
        "extra_body_max_tokens",
        "extra_body_max_completion_tokens",
        "extra_max_completion_tokens",
        "settings_max_completion_tokens",
        "preset_max_completion_tokens",
        "extra_body_functions",
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client_config = ClientConfig::new(Provider::OpenAi, "");
        client_config.api_key = None;
        client_config.endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let mut preset = OpenAiPresetConfig::new(OpenAiPreset::Ollama);
        let mut template = CompletionRequest::new("loopback-summary", Context::default());
        match source {
            "extra_body_max_tokens" => {
                template
                    .extra
                    .insert("extra_body".into(), json!({"max_tokens": 999999}));
            }
            "extra_body_max_completion_tokens" => {
                template.extra.insert(
                    "extra_body".into(),
                    json!({"max_completion_tokens": 999999}),
                );
            }
            "extra_max_completion_tokens" => {
                template
                    .extra
                    .insert("max_completion_tokens".into(), json!(999999));
            }
            "settings_max_completion_tokens" => {
                template
                    .settings
                    .base
                    .extra
                    .insert("max_completion_tokens".into(), json!(999999));
            }
            "preset_max_completion_tokens" => {
                preset.settings.max_completion_tokens = Setting::Value(999999);
            }
            "extra_body_functions" => {
                template.extra.insert(
                    "extra_body".into(),
                    json!({"functions": [{
                        "name": "unexpected_tool", "parameters": {"type": "object"}
                    }]}),
                );
            }
            _ => unreachable!(),
        }
        client_config.openai_preset = Some(preset);
        let llm = SummaryLlm::new(LlmClient::new(client_config).unwrap(), template);
        let context = SharedContext::new(Context::new(vec![
            Message::text(Role::User, "Old context"),
            Message::text(Role::Assistant, "Old answer"),
            Message::text(Role::User, "Recent question"),
        ]));
        let summarizer = LLMContextSummarizer::new(context, config(1)).unwrap();
        let request = summarizer.request(None, true).unwrap().unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), llm.generate(&request))
            .await
            .unwrap_or_else(|_| {
                panic!("{source}: template override must fail before HTTP inference")
            });
        assert!(
            result
                .error
                .as_ref()
                .is_some_and(|error| error.contains("override")),
            "{source}: expected explicit request-isolation failure, got {result:?}"
        );
        assert!(result.summary.is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(10), listener.accept())
                .await
                .is_err(),
            "{source}: rejected template must not start an HTTP request"
        );
    }
}

#[tokio::test(start_paused = true)]
async fn responder_fast_failures_do_not_exhaust_completed_task_capacity() {
    let mut client_config = ClientConfig::new(Provider::OpenAi, "");
    client_config.api_key = None;
    client_config.endpoint = "http://127.0.0.1:9/".into();
    let responder = Arc::new(LLMContextSummaryProcessor::new(SummaryLlm::new(
        LlmClient::new(client_config).unwrap(),
        CompletionRequest::new("unused", Context::default()),
    )));
    let mut worker = Pipeline::new(vec![responder])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    loop {
        let frame = worker.recv().await.unwrap();
        if matches!(frame.frame.data, FrameData::Start(_)) {
            break;
        }
    }
    // Settle the initial maintenance tick. The requests below complete before
    // the next periodic tick and must not depend on it to recover capacity.
    tokio::time::advance(Duration::from_millis(1)).await;
    let began = tokio::time::Instant::now();
    for index in 0..32 {
        let request = LlmContextSummaryRequestFrame {
            request_id: format!("fast-failure-{index}"),
            context: LlmContextData {
                messages: vec![ChatMessage {
                    role: ChatRole::User,
                    content: "Only a preserved recent message".into(),
                    name: None,
                    tool_call_id: None,
                    tool_calls: vec![],
                }
                .into()],
                tools: ToolSet::NotGiven,
                tool_choice: None,
            },
            min_messages_to_keep: 1,
            target_context_tokens: 128,
            summarization_prompt: "Summarize".into(),
            summarization_timeout: Some(1.0),
        };
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmContextSummaryRequest(request.clone())),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(1), worker.recv())
                .await
                .expect("responder result deadline")
                .expect("responder remains usable");
            match frame.frame.data {
                FrameData::Error(error) => {
                    panic!("request {index} must not fail task admission: {error:?}")
                }
                FrameData::LlmContextSummaryResult(result) => {
                    assert_eq!(result.request_id, request.request_id);
                    assert!(result
                        .error
                        .as_ref()
                        .is_some_and(|error| error.contains("no safe messages")));
                    break;
                }
                _ => {}
            }
        }
    }
    assert_eq!(
        began.elapsed(),
        Duration::ZERO,
        "capacity recovery must not wait for a periodic maintenance tick"
    );
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
