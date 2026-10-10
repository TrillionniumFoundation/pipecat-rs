mod support;
use pipecat_aws_bedrock::{adapter, BedrockClient, BedrockConfig};
use pipecat_core::Setting;
use pipecat_services::{Content, ContentPart, Message, Role, ToolCall, ToolChoice, ToolDefinition};
use serde_json::json;
#[test]
fn default_body_is_converse_schema() {
    let req = support::request();
    let p = adapter::prepare(&req, true).unwrap();
    assert_eq!(
        p.body,
        json!({"messages":[{"role":"user","content":[{"text":"Hello"}]}],"additionalModelRequestFields":{}})
    );
    assert_eq!(p.model, "us.amazon.nova-lite-v1:0");
    assert!(!p.noop_tool);
}
#[test]
fn settings_are_sparse_null_clear_and_atomic_validation() {
    let mut req = support::request();
    req.temperature = Some(0.9);
    req.settings.temperature = Setting::Null;
    req.settings.max_tokens = Setting::Value(200);
    req.settings.top_p = Setting::Value(0.8);
    req.settings.base.extra.extend([
        ("stop_sequences".into(), json!(["END"])),
        ("latency".into(), json!("optimized")),
        ("effort".into(), json!("high")),
        (
            "additional_model_request_fields".into(),
            json!({"thinking":{"type":"enabled","budget_tokens":1024}}),
        ),
    ]);
    let p = adapter::prepare(&req, true).unwrap();
    assert_eq!(
        p.body["inferenceConfig"],
        json!({"maxTokens":200,"topP":0.8,"stopSequences":["END"]})
    );
    assert_eq!(p.body["outputConfig"], json!({"effort":"high"}));
    assert_eq!(p.body["performanceConfig"], json!({"latency":"optimized"}));
    req.settings.temperature = Setting::Value(1.1);
    assert!(adapter::prepare(&req, true).is_err());
}
#[test]
fn source_system_override_merge_and_prefill_rules() {
    let mut req = support::request();
    req.context.messages = vec![
        Message::text(Role::System, "old"),
        Message::text(Role::User, ""),
        Message::text(Role::Developer, "middle"),
        Message::text(Role::Assistant, "prefix"),
    ];
    req.system_instruction = Some("new".into());
    req.model = "us.anthropic.claude-sonnet-4-6-v1:0".into();
    let p = adapter::prepare(&req, true).unwrap();
    assert_eq!(p.body["system"], json!([{"text":"new"}]));
    assert_eq!(
        p.body["messages"][0]["content"],
        json!([{"text":"(empty)"},{"text":"middle"}])
    );
    assert_eq!(
        p.body["messages"][2],
        json!({"role":"user","content":[{"text":"."}]})
    );
    req.model = "anthropic.claude-3-sonnet".into();
    assert_eq!(
        adapter::prepare(&req, true).unwrap().body["messages"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
}
#[test]
fn tools_choices_prompt_cache_and_noop_history() {
    let mut req = support::request();
    req.system_instruction = Some("system".into());
    req.settings
        .base
        .extra
        .insert("enable_prompt_caching".into(), json!(true));
    req.context.tools = Some(vec![ToolDefinition {
        name: "lookup".into(),
        description: "Look up".into(),
        parameters: json!({"type":"object","properties":{}}),
    }]);
    req.context.tool_choice = Some(ToolChoice::Function("lookup".into()));
    let p = adapter::prepare(&req, true).unwrap();
    assert_eq!(
        p.body["toolConfig"]["toolChoice"],
        json!({"tool":{"name":"lookup"}})
    );
    assert_eq!(
        p.body["system"][1],
        json!({"cachePoint":{"type":"default"}})
    );
    assert_eq!(
        p.body["toolConfig"]["tools"][1],
        json!({"cachePoint":{"type":"default"}})
    );
    req.context
        .messages
        .push(Message::assistant_tool_calls(vec![ToolCall {
            id: "id1".into(),
            name: "lookup".into(),
            arguments: json!({"q":"x"}),
        }]));
    req.context
        .messages
        .push(Message::tool_result("id1", "{\"ok\":true}"));
    req.context.tools = None;
    let p = adapter::prepare(&req, true).unwrap();
    assert!(p.noop_tool);
    assert_eq!(
        p.body["toolConfig"]["tools"][0]["toolSpec"]["name"],
        "no_operation"
    );
    assert!(p.body["toolConfig"].get("toolChoice").is_none());
    assert_eq!(
        p.body["messages"][2]["content"][0]["toolResult"]["content"],
        json!([{"json":{"ok":true}}])
    );
}
#[test]
fn nonstream_inference_excludes_tools_cache_latency() {
    let mut req = support::request();
    req.context.tools = Some(vec![ToolDefinition {
        name: "f".into(),
        description: "f".into(),
        parameters: json!({}),
    }]);
    req.settings
        .base
        .extra
        .insert("latency".into(), json!("standard"));
    req.settings
        .base
        .extra
        .insert("enable_prompt_caching".into(), json!(true));
    req.system_instruction = Some("sys".into());
    let p = adapter::prepare(&req, false).unwrap();
    assert!(p.body.get("toolConfig").is_none());
    assert!(p.body.get("performanceConfig").is_none());
    assert_eq!(p.body["system"], json!([{"text":"sys"}]));
}
#[test]
fn file_formats_s3_sanitization_order_and_unsupported_audio() {
    let mut req = support::request();
    req.context.messages[0].content = Some(Content::Parts(vec![
        ContentPart::Text {
            text: "read".into(),
        },
        ContentPart::File {
            data: "aGk=".into(),
            mime_type: "application/pdf".into(),
            filename: Some("é notes... (v1).pdf".into()),
        },
    ]));
    let p = adapter::prepare(&req, true).unwrap();
    assert_eq!(
        p.body["messages"][0]["content"][0]["document"],
        json!({"name":"notes (v1) pdf","format":"pdf","source":{"bytes":"aGk="}})
    );
    req.context.messages[0].content = Some(Content::Parts(vec![ContentPart::File {
        data: "s3://bucket/movie.mpg".into(),
        mime_type: "video/mpeg".into(),
        filename: Some("movie.mpg".into()),
    }]));
    assert_eq!(
        adapter::prepare(&req, true).unwrap().body["messages"][0]["content"][0]["video"]["format"],
        "mpg"
    );
    req.context.messages[0].content = Some(Content::Parts(vec![ContentPart::InputAudio {
        data: "aGk=".into(),
        format: "wav".into(),
    }]));
    assert!(adapter::prepare(&req, true).is_err());
}
#[test]
fn signed_reasoning_and_redacted_replay_are_native_and_debug_redacted() {
    let mut req = support::request();
    let mut message = Message::text(Role::Assistant, "");
    message.content = Some(Content::Parts(vec![
        ContentPart::BedrockReasoning {
            text: "SECRET_THOUGHT".into(),
            signature: Some("SECRET_SIGNATURE".into()),
        },
        ContentPart::BedrockRedacted { data: vec![0, 255] },
    ]));
    assert!(!format!("{message:?}").contains("SECRET"));
    req.context.messages.push(message);
    let p = adapter::prepare(&req, true).unwrap();
    let debug = format!("{p:?}");
    assert!(!debug.contains("SECRET"));
    assert!(!debug.contains("AP8="));
    assert_eq!(
        p.body["messages"][1]["content"],
        json!([{"reasoningContent":{"reasoningText":{"text":"SECRET_THOUGHT","signature":"SECRET_SIGNATURE"}}},{"reasoningContent":{"redactedContent":"AP8="}}])
    );
    for provider in [
        pipecat_services::Provider::OpenAi,
        pipecat_services::Provider::Anthropic,
        pipecat_services::Provider::Gemini,
    ] {
        assert!(pipecat_services::adapters::request_body(provider, &req).is_err());
    }
}
#[test]
fn endpoints_and_credentials_reject_leakable_routes() {
    for url in [
        "http://bedrock-runtime.us-east-1.amazonaws.com/",
        "https://evil.example/",
        "http://localhost:9000/",
        "http://127.0.0.1/?secret=1",
        "http://user@127.0.0.1/",
    ] {
        let mut c = BedrockConfig::new(support::credentials(), "us-east-1");
        c.endpoint = url.into();
        assert!(BedrockClient::new(c).is_err());
    }
    let c = BedrockConfig::new(support::credentials(), "cn-north-1");
    assert_eq!(
        c.endpoint,
        "https://bedrock-runtime.cn-north-1.amazonaws.com.cn/"
    );
    assert!(!format!("{c:?}").contains("FAKETOKEN"));
}
#[test]
fn lone_system_and_initial_developer_remain_user_messages() {
    for role in [Role::System, Role::Developer] {
        let mut request = support::request();
        request.context.messages = vec![Message::text(role, "instruction")];
        let prepared = adapter::prepare(&request, true).unwrap();
        assert!(prepared.body.get("system").is_none());
        assert_eq!(
            prepared.body["messages"],
            json!([{"role":"user","content":[{"text":"instruction"}]}])
        );
    }
    let mut request = support::request();
    request
        .context
        .messages
        .insert(0, Message::text(Role::System, "context-system"));
    request.system_instruction = Some(String::new());
    assert_eq!(
        adapter::prepare(&request, true).unwrap().body["system"],
        json!([{"text":"context-system"}])
    );
}
