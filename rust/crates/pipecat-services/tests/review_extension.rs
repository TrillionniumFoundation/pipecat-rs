//! Independent regression checks for settings and rich-context boundaries.
use pipecat_core::{self as core, Direction, Frame, FrameData, Pipeline, PipelineConfig, Setting};
use pipecat_services::*;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn identity_transform_preserves_distinct_native_messages_with_equal_core_projection() {
    let mut inline = Message::text(Role::User, "");
    inline.content = Some(Content::Parts(vec![ContentPart::InlineData {
        mime_type: "image/png".into(),
        data: "YQ==".into(),
    }]));
    let mut file = inline.clone();
    file.content = Some(Content::Parts(vec![ContentPart::File {
        mime_type: "image/png".into(),
        data: "YQ==".into(),
        filename: None,
    }]));
    assert_eq!(
        frame_context::to_core_message(&inline),
        frame_context::to_core_message(&file)
    );
    let original = Context::new(vec![inline, file]);
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(ClientConfig::new(Provider::OpenAi, "synthetic")).unwrap(),
        CompletionRequest::new("synthetic", original.clone()),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmMessagesTransform(
                core::LlmMessagesTransformFrame {
                    transform: core::LocalMessageTransform::new(Ok),
                    run_llm: Some(false),
                },
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while worker.recv().await.is_some() {}
    })
    .await
    .unwrap();
    worker.join().await.unwrap();
    assert_eq!(
        processor.request_snapshot().unwrap().context,
        original,
        "an identity transform must preserve each native image/file representation"
    );
}

#[test]
fn gemini_model_update_validates_the_endpoint_before_committing_state() {
    let mut config = ClientConfig::new(Provider::Gemini, "synthetic");
    config.endpoint = "http://127.0.0.1:1/models/{model}:streamGenerateContent?alt=sse".into();
    let processor = LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new(
            "valid-model",
            Context::new(vec![Message::text(Role::User, "hello")]),
        ),
    );
    let before = processor.request_snapshot().unwrap();
    let mut delta = core::LlmSettings::default();
    delta.base.model = Setting::Value("invalid/model".into());
    assert!(
        processor.update_settings(&delta).is_err(),
        "an invalid path model must be rejected before state is committed"
    );
    assert_eq!(
        processor.request_snapshot().unwrap().settings,
        before.settings
    );
}

#[test]
fn gemini_scalar_tool_results_match_python_value_wrapper() {
    use serde_json::json;
    for (text, expected) in [
        ("COMPLETED", json!({"value":"COMPLETED"})),
        ("\"hello\"", json!({"value":"hello"})),
        ("42", json!({"value":42})),
        ("true", json!({"value":true})),
        ("null", json!({"value":null})),
        ("[1,2]", json!({"value":[1,2]})),
        ("{\"ok\":true}", json!({"ok":true})),
    ] {
        let request = CompletionRequest::new(
            "synthetic",
            Context::new(vec![
                Message::assistant_tool_calls(vec![ToolCall {
                    id: "a".into(),
                    name: "f".into(),
                    arguments: json!({}),
                }]),
                Message::tool_result("a", text),
            ]),
        );
        let body = adapters::request_body(Provider::Gemini, &request).unwrap();
        assert_eq!(
            body["contents"][1]["parts"][0]["functionResponse"]["response"], expected,
            "{text}"
        );
    }
}

fn native_variants() -> (Message, Message) {
    let mut inline = Message::text(Role::User, "");
    inline.content = Some(Content::Parts(vec![ContentPart::InlineData {
        mime_type: "image/png".into(),
        data: "YQ==".into(),
    }]));
    let mut file = inline.clone();
    file.content = Some(Content::Parts(vec![ContentPart::File {
        mime_type: "image/png".into(),
        data: "YQ==".into(),
        filename: None,
    }]));
    (inline, file)
}

async fn transform_context(
    original: Context,
    transform: core::LocalMessageTransform,
) -> (Context, usize) {
    transform_context_for(Provider::OpenAi, original, transform).await
}

async fn transform_context_for(
    provider: Provider,
    original: Context,
    transform: core::LocalMessageTransform,
) -> (Context, usize) {
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(ClientConfig::new(provider, "synthetic")).unwrap(),
        CompletionRequest::new("synthetic", original),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmMessagesTransform(
                core::LlmMessagesTransformFrame {
                    transform,
                    run_llm: Some(false),
                },
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut errors = 0;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::Error(_)) {
                errors += 1;
            }
        }
    })
    .await
    .unwrap();
    worker.join().await.unwrap();
    (processor.request_snapshot().unwrap().context, errors)
}

#[tokio::test]
async fn native_transform_preserves_reordered_unique_parts_and_duplicate_multiplicity() {
    let (inline, file) = native_variants();
    let text = Message::text(Role::User, "unique");
    let original = Context::new(vec![
        inline.clone(),
        file.clone(),
        text.clone(),
        file.clone(),
    ]);
    let (result, errors) = transform_context(
        original,
        core::LocalMessageTransform::new(|mut messages| {
            let unique = messages.remove(2);
            messages.insert(0, unique);
            messages.push(core::ChatMessage::new(core::ChatRole::User, "new").into());
            Ok(messages)
        }),
    )
    .await;
    assert_eq!(errors, 0);
    assert_eq!(
        result.messages,
        vec![
            text,
            inline,
            file.clone(),
            file,
            Message::text(Role::User, "new")
        ]
    );
}

#[tokio::test]
async fn ambiguous_partial_filter_is_rejected_without_context_mutation() {
    let (inline, file) = native_variants();
    let original = Context::new(vec![inline, file]);
    let (result, errors) = transform_context(
        original.clone(),
        core::LocalMessageTransform::new(|mut messages| {
            messages.remove(0);
            Ok(messages)
        }),
    )
    .await;
    assert_eq!(errors, 1);
    assert_eq!(result, original);
}

#[tokio::test]
async fn unambiguous_partial_filter_retains_native_file() {
    let (inline, mut file) = native_variants();
    file.name = Some("distinct".into());
    let original = Context::new(vec![inline, file.clone()]);
    let (result, errors) = transform_context(
        original,
        core::LocalMessageTransform::new(|mut messages| {
            messages.remove(0);
            Ok(messages)
        }),
    )
    .await;
    assert_eq!(errors, 0);
    assert_eq!(result.messages, vec![file]);
}

#[tokio::test]
async fn local_self_target_updates_only_the_referenced_same_name_processor() {
    use serde_json::json;
    let new_processor = || {
        Arc::new(LlmProcessor::new(
            LlmClient::new(ClientConfig::new(Provider::OpenAi, "synthetic")).unwrap(),
            CompletionRequest::new("original", Context::default()),
        ))
    };
    let first = new_processor();
    let second = new_processor();
    let mut worker = Pipeline::new(vec![first.clone(), second.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let delta = core::ServiceSettings {
        model: Setting::Value("updated".into()),
        ..Default::default()
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::ServiceUpdateSettings(Box::new(
                core::ServiceUpdateSettingsFrame {
                    delta: Some(delta),
                    service: Some(core::ProcessorReference::Local(
                        core::LocalProcessorReference(second.clone()),
                    )),
                    ..Default::default()
                },
            ))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut leaked = false;
    while let Some(output) = worker.recv().await {
        leaked |= matches!(output.frame.data, FrameData::ServiceUpdateSettings(_));
    }
    worker.join().await.unwrap();
    assert_eq!(
        adapters::request_body(Provider::OpenAi, &first.request_snapshot().unwrap()).unwrap()
            ["model"],
        json!("original")
    );
    assert_eq!(
        adapters::request_body(Provider::OpenAi, &second.request_snapshot().unwrap()).unwrap()
            ["model"],
        json!("updated")
    );
    assert!(!leaked, "the local target must consume its own update");
}

#[test]
fn gemini_tools_follow_source_recursive_schema_adaptations_without_erasing_parameter_names() {
    use serde_json::json;
    let schema = json!({"type":"object", "additionalProperties":false, "x-request-id":"drop",
        "properties": {
            "x-request-id": {"type":["string","null"], "enum":["one","two"], "x-mcp-header":"drop"},
            "additionalProperties": {"type":"integer", "enum":[1,2]},
            "nested": {"type":"array", "items":{"type":"object", "additionalProperties":true,
                "properties":{"flag":{"type":"boolean"}}, "required":["flag"]}},
            "choice": {"anyOf":[{"type":"string", "x-note":"drop"},{"type":"number"}]}
        }, "required":["x-request-id"], "description":"retain"});
    let mut request = CompletionRequest::new(
        "gemini-2.5-flash",
        Context::new(vec![Message::text(Role::User, "hi")]),
    );
    request.context.tools = Some(vec![ToolDefinition {
        name: "f".into(),
        description: "function".into(),
        parameters: schema.clone(),
    }]);
    let body = adapters::request_body(Provider::Gemini, &request).unwrap();
    assert_eq!(
        body["tools"][0]["functionDeclarations"][0]["parameters"],
        json!({"type":"object",
        "properties": {
            "x-request-id": {"anyOf":[{"type":"string"},{"type":"null"}], "enum":["one","two"]},
            "additionalProperties": {"type":"integer"},
            "nested": {"type":"array", "items":{"type":"object", "properties":{"flag":{"type":"boolean"}}, "required":["flag"]}},
            "choice": {"anyOf":[{"type":"string"},{"type":"number"}]}
        }, "required":["x-request-id"], "description":"retain"})
    );
    assert_eq!(
        request.context.tools.as_ref().unwrap()[0].parameters,
        schema
    );
}

#[test]
fn gemini_prefill_fix_matches_source_model_prefix_matrix_and_resolved_settings() {
    use serde_json::json;
    for (model, expected_len) in [
        ("gemini-2.5-flash", 1),
        ("gemini-3-pro-preview", 1),
        ("gemini-3.1-pro-preview", 1),
        ("gemini-pro-latest", 1),
        ("gemini-3.5-flash", 2),
        ("gemini-3.5-flash-lite", 2),
        ("future-model", 2),
    ] {
        let mut request = CompletionRequest::new(
            "unused",
            Context::new(vec![Message::text(Role::Assistant, "partial")]),
        );
        request.settings.base.model = Setting::Value(model.into());
        let body = adapters::request_body(Provider::Gemini, &request).unwrap();
        assert_eq!(
            body["contents"].as_array().unwrap().len(),
            expected_len,
            "{model}"
        );
        if expected_len == 2 {
            assert_eq!(
                body["contents"][1],
                json!({"role":"user","parts":[{"text":"."}]})
            );
        }
        assert_eq!(request.context.messages.len(), 1);
    }
}

#[tokio::test]
async fn signed_thinking_survives_identity_transforms_and_rich_tool_continuations() {
    use serde_json::json;
    let mut assistant = Message::assistant_tool_calls(vec![ToolCall {
        id: "call".into(),
        name: "f".into(),
        arguments: json!({"n":1}),
    }]);
    assistant.content = Some(Content::Parts(vec![
        ContentPart::Thinking {
            text: "saved thought".into(),
            signature: Some("signed-provider-data".into()),
        },
        ContentPart::Text {
            text: "answer".into(),
        },
    ]));
    let original = Context::new(vec![
        assistant,
        Message::tool_result("call", "{\"result\":true}"),
    ]);
    let (result, errors) = transform_context_for(
        Provider::Anthropic,
        original.clone(),
        core::LocalMessageTransform::new(Ok),
    )
    .await;
    assert_eq!(errors, 0);
    assert_eq!(result, original);
    let body = adapters::request_body(
        Provider::Anthropic,
        &CompletionRequest::new("synthetic", result),
    )
    .unwrap();
    assert_eq!(
        body["messages"][0]["content"][0],
        json!({"type":"thinking","thinking":"saved thought","signature":"signed-provider-data"})
    );
    assert_eq!(body["messages"][0]["content"][2]["id"], "call");
    assert_eq!(body["messages"][1]["content"][0]["tool_use_id"], "call");
}

#[test]
fn gemini_schema_adaptation_notes_make_constraint_changes_inspectable() {
    use serde_json::json;
    let source = json!({"type":["integer","null"],"enum":[1,2],"additionalProperties":false,"x-note":"removed","opaque":{"keep":true}});
    let (adapted, notes) = adapters::adapt_gemini_tool_schema(&source);
    assert_eq!(
        adapted,
        json!({"anyOf":[{"type":"integer"},{"type":"null"}],"opaque":{"keep":true}})
    );
    assert_eq!(
        notes,
        vec![
            "converted a union 'type' to 'anyOf'",
            "dropped a non-string 'enum', losing its constraint",
            "dropped unsupported key 'additionalProperties'",
            "dropped unsupported key 'x-note'",
        ]
    );
}

#[test]
fn explicit_empty_gemini_tools_is_an_empty_provider_list() {
    let mut request = CompletionRequest::new("gemini-2.5-flash", Context::default());
    request.context.tools = Some(vec![]);
    assert_eq!(
        adapters::request_body(Provider::Gemini, &request).unwrap()["tools"],
        serde_json::json!([])
    );
    request.context.tools = None;
    assert!(adapters::request_body(Provider::Gemini, &request)
        .unwrap()
        .get("tools")
        .is_none());
}
