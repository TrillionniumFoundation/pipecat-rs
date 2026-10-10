//! Gemini Part provenance survives receipt-backed context reconciliation.
use pipecat_core::*;
use pipecat_services::{
    adapters::request_body, CompletionRequest, Content, ContentPart, Context, Message, Provider,
    Role, ToolCall,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::sync::Arc;

fn process(
    assistant: &LLMAssistantAggregator,
    frame: Frame,
) -> Result<AggregationOutput, ProcessorError> {
    assistant.process_frame(frame, Direction::Downstream)
}
fn attached(data: FrameData, carrier: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(Arc::new(carrier))
        .unwrap();
    frame
}
fn start(id: u64) -> Frame {
    attached(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(id, LlmContinuationProvider::Gemini).unwrap(),
    )
}
fn end(id: u64, blocks: Vec<LlmContinuationBlock>) -> Frame {
    attached(
        FrameData::LlmResponseEnd,
        NativeLlmContinuation::new(id, LlmContinuationProvider::Gemini, blocks).unwrap(),
    )
}
fn checkpoint(id: u64, blocks: Vec<LlmContinuationBlock>) -> Frame {
    attached(
        FrameData::LlmContinuationCheckpoint,
        NativeLlmContinuation::checkpoint(id, LlmContinuationProvider::Gemini, blocks).unwrap(),
    )
}
fn part(text: &str, thought: bool, signature: Option<&str>) -> LlmContinuationBlock {
    LlmContinuationBlock::GeminiText {
        text: text.into(),
        thought,
        signature: signature.map(Into::into),
    }
}
fn text(assistant: &LLMAssistantAggregator, value: &str) {
    process(
        assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm(value))),
    )
    .unwrap();
}
fn history() -> Vec<LlmContinuationBlock> {
    vec![
        part("same", false, Some("first+/==")),
        part("private thought", true, Some("thought+/==")),
        part("same", false, Some("second+/==")),
        part("", false, Some("tail+/==")),
    ]
}
fn expected_parts() -> Value {
    json!([
        {"text":"same","thoughtSignature":"first+/=="},
        {"text":"private thought","thought":true,"thoughtSignature":"thought+/=="},
        {"text":"same","thoughtSignature":"second+/=="},
        {"text":"","thoughtSignature":"tail+/=="},
    ])
}
fn wire(context: &SharedContext) -> Result<Value, pipecat_services::ServiceError> {
    request_body(
        Provider::Gemini,
        &CompletionRequest::new("gemini-3-fixture", context.snapshot().unwrap()),
    )
}
fn parts(context: &SharedContext) -> Vec<ContentPart> {
    context
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .flat_map(|m| match m.content {
            Some(Content::Parts(parts)) => parts,
            _ => vec![],
        })
        .collect()
}
fn receipt(id: &str, value: &str) -> [AggregatedTextFrame; 1] {
    [AggregatedTextFrame {
        text: TextFrame::llm(value),
        raw_text: Some(value.into()),
        context_id: Some(id.into()),
        aggregated_by: "fixture".into(),
        will_be_spoken: true,
    }]
}
fn register(assistant: &LLMAssistantAggregator, id: &str) {
    process(
        assistant,
        Frame::new(FrameData::TtsStarted(TtsStartedFrame {
            context_id: Some(id.into()),
            append_to_context: true,
        })),
    )
    .unwrap();
    assistant.begin_playback(id).unwrap();
}

#[test]
fn repeated_text_and_empty_trailing_signature_round_trip_without_coalescing_parts() {
    let context = SharedContext::new(Context::new(vec![Message::text(Role::User, "question")]));
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    process(&assistant, start(1)).unwrap();
    text(&assistant, "samesame");
    process(&assistant, end(1, history())).unwrap();
    assert_eq!(
        wire(&context).unwrap()["contents"][1]["parts"],
        expected_parts()
    );
    let rich = context.snapshot_rich().unwrap();
    let decoded: LlmContextData =
        serde_json::from_value(serde_json::to_value(&rich).unwrap()).unwrap();
    let converted = pipecat_services::frame_context::context(&decoded).unwrap();
    let request = CompletionRequest::new("gemini-3-fixture", converted);
    assert_eq!(
        request_body(Provider::Gemini, &request).unwrap(),
        wire(&context).unwrap()
    );
    assert!(request_body(Provider::OpenAi, &request).is_err());
    assert!(request_body(Provider::Anthropic, &request).is_err());
    assert!(!format!("{rich:?}").contains("thought+/=="));
}

#[test]
fn ordered_receipts_restore_only_exact_complete_signed_parts() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    process(&assistant, start(2)).unwrap();
    text(&assistant, "samesame");
    for id in ["first", "second"] {
        register(&assistant, id);
    }
    process(&assistant, end(2, history())).unwrap();
    assert!(wire(&context).is_err());
    assert!(assistant
        .confirm_playback("second", &receipt("second", "same"))
        .is_err());
    assistant
        .confirm_playback("first", &receipt("first", "same"))
        .unwrap();
    let current = parts(&context);
    assert!(current.iter().any(|p| matches!(p, ContentPart::GeminiText { text, signature: Some(signature), incomplete: false, .. } if text == "same" && signature == "first+/==")));
    assert!(current.iter().any(|p| matches!(p, ContentPart::GeminiText { text, signature: None, incomplete: true, .. } if text.is_empty())));
    assert!(!current.iter().any(|p| matches!(p, ContentPart::GeminiText { signature: Some(signature), .. } if signature == "second+/==" || signature == "tail+/==")));
    assert!(wire(&context).is_err());
    assistant
        .confirm_playback("second", &receipt("second", "same"))
        .unwrap();
    assert_eq!(
        wire(&context).unwrap()["contents"][0]["parts"],
        expected_parts()
    );
    let before = context.snapshot_rich().unwrap();
    process(&assistant, end(2, history())).unwrap();
    assert_eq!(context.snapshot_rich().unwrap(), before);
}

#[test]
fn partial_signed_text_keeps_heard_bytes_without_signature_and_fails_closed() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    process(&assistant, start(3)).unwrap();
    register(&assistant, "partial");
    process(
        &assistant,
        end(
            3,
            vec![
                part("你好，世界", false, Some("whole-part-signature")),
                part("unheard unsigned tail", false, None),
                part("", false, Some("empty-tail-signature")),
            ],
        ),
    )
    .unwrap();
    assistant
        .confirm_playback("partial", &receipt("partial", "你好"))
        .unwrap();
    process(&assistant, Frame::interruption()).unwrap();
    let current = parts(&context);
    assert!(current.iter().any(|p| matches!(p, ContentPart::GeminiText { text, thought: false, signature: None, incomplete: true } if text == "你好")));
    let saved = serde_json::to_string(&context.snapshot_rich().unwrap()).unwrap();
    for absent in [
        "世界",
        "unheard unsigned tail",
        "whole-part-signature",
        "empty-tail-signature",
    ] {
        assert!(!saved.contains(absent));
    }
    assert!(wire(&context).is_err());
    let before = context.snapshot_rich().unwrap();
    assistant
        .confirm_playback("partial", &receipt("partial", "，世界"))
        .unwrap();
    assert_eq!(
        context.snapshot_rich().unwrap(),
        before,
        "late playback cannot resurrect a signature"
    );
}

#[test]
fn signed_thoughts_do_not_consume_audible_receipts_and_unsigned_unheard_text_is_omitted() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    process(&assistant, start(4)).unwrap();
    register(&assistant, "heard");
    process(
        &assistant,
        end(
            4,
            vec![
                part("private reasoning", true, Some("private-signature")),
                part("heard", false, None),
                part("never heard", false, None),
            ],
        ),
    )
    .unwrap();
    assistant
        .confirm_playback("heard", &receipt("heard", "heard"))
        .unwrap();
    assert_eq!(
        wire(&context).unwrap()["contents"][0]["parts"],
        json!([
            {"text":"private reasoning","thought":true,"thoughtSignature":"private-signature"}, {"text":"heard"},
        ])
    );
}

#[test]
fn playback_before_checkpoint_and_completion_preserves_one_exact_signature() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    process(&assistant, start(5)).unwrap();
    register(&assistant, "early");
    assistant
        .confirm_playback("early", &receipt("early", "same"))
        .unwrap();
    let prefix = vec![part("same", false, Some("first+/=="))];
    process(&assistant, checkpoint(5, prefix.clone())).unwrap();
    process(&assistant, end(5, prefix)).unwrap();
    assert_eq!(
        wire(&context).unwrap()["contents"][0]["parts"],
        json!([{"text":"same","thoughtSignature":"first+/=="}])
    );
}

#[test]
fn checkpoints_are_monotone_and_never_admit_declared_tools() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    process(&assistant, start(6)).unwrap();
    let prefix = vec![part("closed thought", true, Some("closed-signature"))];
    let mut full = prefix.clone();
    full.extend([
        LlmContinuationBlock::ToolCall(FunctionCall {
            function_name: "echo".into(),
            tool_call_id: "not-admitted".into(),
            arguments: json!({}),
        }),
        LlmContinuationBlock::GeminiToolMetadata {
            tool_call_id: "not-admitted".into(),
            signature: Some("tool-signature".into()),
        },
    ]);
    process(&assistant, checkpoint(6, full)).unwrap();
    let before = context.snapshot_rich().unwrap();
    process(&assistant, checkpoint(6, prefix)).unwrap();
    assert_eq!(context.snapshot_rich().unwrap(), before);
    assert!(process(
        &assistant,
        checkpoint(
            6,
            vec![part("closed thought", true, Some("changed-signature"))]
        )
    )
    .is_err());
    process(&assistant, Frame::interruption()).unwrap();
    let body = wire(&context).unwrap().to_string();
    assert!(body.contains("closed-signature"));
    assert!(!body.contains("not-admitted"));
    assert!(!body.contains("tool-signature"));
}

#[test]
fn foreign_tool_calls_use_only_official_sentinel_while_native_unsigned_calls_fail_closed() {
    let call = ToolCall {
        id: "exact-call-id".into(),
        name: "echo".into(),
        arguments: json!({"n":1}),
    };
    let foreign = Message::assistant_tool_calls(vec![call.clone()]);
    let result = Message::tool_result("exact-call-id", r#"{"ok":true}"#);
    let request = |message| {
        CompletionRequest::new(
            "gemini-3-fixture",
            Context::new(vec![message, result.clone()]),
        )
    };
    let body = request_body(Provider::Gemini, &request(foreign.clone())).unwrap();
    assert_eq!(
        body["contents"][0]["parts"][0]["thoughtSignature"],
        "skip_thought_signature_validator"
    );
    assert_eq!(
        body["contents"][0]["parts"][0]["functionCall"]["id"],
        "exact-call-id"
    );
    let mut native = foreign;
    native.content = Some(Content::Parts(vec![ContentPart::GeminiToolMetadata {
        tool_call_id: call.id,
        signature: None,
    }]));
    assert!(request_body(Provider::Gemini, &request(native)).is_err());
}

#[test]
fn transformed_delivery_and_context_replacement_do_not_publish_rejected_signatures() {
    for replace in [false, true] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        process(&assistant, start(7)).unwrap();
        text(&assistant, if replace { "original" } else { "transformed" });
        if replace {
            context.replace(Context::default()).unwrap();
        }
        assert!(process(
            &assistant,
            end(
                7,
                vec![part("original", false, Some("never-admitted-signature"))]
            )
        )
        .is_err());
        assert!(!serde_json::to_string(&context.snapshot_rich().unwrap())
            .unwrap()
            .contains("never-admitted-signature"));
    }
}
