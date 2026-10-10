//! Golden HTTP JSON contracts derived from the checked-in Python service and
//! adapter methods. These tests make no provider calls and need no SDK/API key.
use pipecat_core::Setting;
use pipecat_services::{presets::*, *};
use serde_json::{json, Value};

fn user() -> Context {
    Context::new(vec![Message::text(Role::User, "Hello")])
}
fn request(preset: OpenAiPreset) -> CompletionRequest {
    OpenAiPresetConfig::new(preset).request(user())
}
fn body(preset: OpenAiPreset, request: &CompletionRequest) -> Value {
    OpenAiPresetConfig::new(preset)
        .request_body(request)
        .unwrap()
}

#[test]
fn all_source_defaults_are_pinned_and_grok_is_an_xai_alias() {
    // Defaults from each service's __init__; these are exact POST URLs formed
    // by the Python OpenAI client's base_url + "chat/completions".
    let cases = [
        (
            OpenAiPreset::OpenAi,
            "gpt-4.1",
            Some("https://api.openai.com/v1/chat/completions"),
            true,
        ),
        (OpenAiPreset::Azure, "gpt-4.1", None, true),
        (
            OpenAiPreset::Baseten,
            "zai-org/GLM-5.3",
            Some("https://inference.baseten.co/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Cerebras,
            "gpt-oss-120b",
            Some("https://api.cerebras.ai/v1/chat/completions"),
            true,
        ),
        (
            OpenAiPreset::Crusoe,
            "openai/gpt-oss-120b",
            Some("https://api.inference.crusoecloud.com/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::DeepSeek,
            "deepseek-flash",
            Some("https://api.deepseek.com/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Fireworks,
            "accounts/fireworks/models/nemotron-3-ultra-nvfp4",
            Some("https://api.fireworks.ai/inference/v1/chat/completions"),
            true,
        ),
        (
            OpenAiPreset::Groq,
            "openai/gpt-oss-120b",
            Some("https://api.groq.com/openai/v1/chat/completions"),
            true,
        ),
        (
            OpenAiPreset::Inception,
            "mercury-2",
            Some("https://api.inceptionlabs.ai/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Mistral,
            "mistral-small-latest",
            Some("https://api.mistral.ai/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Nebius,
            "Qwen/Qwen3-30B-A3B-Instruct-2507",
            Some("https://api.tokenfactory.nebius.com/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Novita,
            "moonshotai/kimi-k2.5",
            Some("https://api.novita.ai/openai/chat/completions"),
            true,
        ),
        (
            OpenAiPreset::Nvidia,
            "nvidia/nemotron-3-super-120b-a12b",
            Some("https://integrate.api.nvidia.com/v1/chat/completions"),
            true,
        ),
        (
            OpenAiPreset::Ollama,
            "llama2",
            Some("http://localhost:11434/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::OpenRouter,
            "openai/gpt-4.1",
            Some("https://openrouter.ai/api/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Perplexity,
            "sonar",
            Some("https://api.perplexity.ai/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Qwen,
            "qwen-plus",
            Some("https://dashscope-intl.aliyuncs.com/compatible-mode/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::SambaNova,
            "Meta-Llama-3.3-70B-Instruct",
            Some("https://api.sambanova.ai/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Sarvam,
            "sarvam-105b",
            Some("https://api.sarvam.ai/v2/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Together,
            "zai-org/GLM-5.2",
            Some("https://api.together.xyz/v1/chat/completions"),
            false,
        ),
        (
            OpenAiPreset::Xai,
            "grok-4.20-non-reasoning",
            Some("https://api.x.ai/v1/chat/completions"),
            true,
        ),
    ];
    assert_eq!(cases.len(), OpenAiPreset::ALL.len());
    for (index, (preset, model, endpoint, developer)) in cases.into_iter().enumerate() {
        assert_eq!(OpenAiPreset::ALL[index], preset);
        assert_eq!(preset.default_model(), model, "{preset:?}");
        assert_eq!(preset.default_endpoint(), endpoint, "{preset:?}");
        assert_eq!(preset.supports_developer_role(), developer, "{preset:?}");
        assert_eq!(request(preset).model, model);
    }
    assert!(include_str!("../../../../src/pipecat/services/grok/llm.py")
        .contains("from pipecat.services.xai.llm import *"));
}

#[test]
fn same_wire_presets_exact_default_request() {
    for preset in [
        OpenAiPreset::OpenAi,
        OpenAiPreset::Baseten,
        OpenAiPreset::Cerebras,
        OpenAiPreset::Crusoe,
        OpenAiPreset::Groq,
        OpenAiPreset::Nebius,
        OpenAiPreset::Novita,
        OpenAiPreset::Ollama,
        OpenAiPreset::OpenRouter,
        OpenAiPreset::Qwen,
        OpenAiPreset::SambaNova,
        OpenAiPreset::Together,
        OpenAiPreset::Xai,
    ] {
        assert_eq!(
            body(preset, &request(preset)),
            json!({
                "model": preset.default_model(), "messages":[{"role":"user","content":"Hello"}],
                "stream":true, "stream_options":{"include_usage":true}
            }),
            "{preset:?}"
        );
    }
}

#[test]
fn all_executable_presets_apply_developer_role_rules_without_mutation() {
    for preset in OpenAiPreset::ALL
        .into_iter()
        .filter(|p| p.unsupported_reason().is_none())
    {
        let request = OpenAiPresetConfig::new(preset)
            .request(Context::new(vec![Message::text(Role::Developer, "Rule")]));
        let expected = if preset.supports_developer_role() {
            "developer"
        } else {
            "user"
        };
        assert_eq!(
            body(preset, &request)["messages"],
            json!([{"role":expected,"content":"Rule"}]),
            "{preset:?}"
        );
        assert_eq!(request.context.messages[0].role, Role::Developer);
    }
}

#[test]
fn generic_sampling_and_distinct_token_settings_match_python() {
    let mut config = OpenAiPresetConfig::new(OpenAiPreset::OpenAi);
    config.settings.max_completion_tokens = Setting::Value(512);
    config.settings.service_tier = Setting::Value("priority".into());
    let mut request = config.request(user());
    request.settings.temperature = Setting::Value(0.5);
    request.settings.max_tokens = Setting::Value(64);
    request.settings.top_p = Setting::Value(0.75);
    request.settings.frequency_penalty = Setting::Value(-0.5);
    request.settings.presence_penalty = Setting::Value(0.25);
    request.settings.seed = Setting::Value(17);
    assert_eq!(
        config.request_body(&request).unwrap(),
        json!({
            "model":"gpt-4.1", "messages":[{"role":"user","content":"Hello"}],
            "stream":true, "stream_options":{"include_usage":true}, "temperature":0.5,
            "max_tokens":64, "max_completion_tokens":512, "top_p":0.75,
            "frequency_penalty":-0.5, "presence_penalty":0.25, "seed":17,
            "service_tier":"priority"
        })
    );
}

#[test]
fn deepseek_golden_thinking_assistant_stamp_and_token_name() {
    // deepseek/llm.py build_chat_completion_params +
    // adapters/services/deepseek_adapter.py _add_reasoning_content.
    let mut request = OpenAiPresetConfig::new(OpenAiPreset::DeepSeek).request(Context::new(vec![
        Message::text(Role::Developer, "Rule"),
        Message::text(Role::User, "Question"),
        Message::text(Role::Assistant, "Answer"),
        Message::text(Role::User, "Again"),
    ]));
    request.max_tokens = Some(128);
    request.temperature = Some(0.25);
    assert_eq!(
        body(OpenAiPreset::DeepSeek, &request),
        json!({
            "model":"deepseek-flash", "messages":[
                {"role":"user","content":"Rule"}, {"role":"user","content":"Question"},
                {"role":"assistant","content":"Answer","reasoning_content":""},
                {"role":"user","content":"Again"}],
            "stream":true,"stream_options":{"include_usage":true},
            "max_tokens":128,"temperature":0.25,"thinking":{"type":"disabled"}
        })
    );
    let mut config = OpenAiPresetConfig::new(OpenAiPreset::DeepSeek);
    config.settings.thinking = Setting::Value(DeepSeekThinking::Enabled);
    assert_eq!(
        config.request_body(&request).unwrap()["thinking"],
        json!({"type":"enabled"})
    );
    config.settings.thinking = Setting::Null;
    assert!(config
        .request_body(&request)
        .unwrap()
        .get("thinking")
        .is_none());
}

#[test]
fn fireworks_golden_omits_stream_usage_and_uses_legacy_token_field() {
    let mut request = request(OpenAiPreset::Fireworks);
    request.settings.max_tokens = Setting::Value(80);
    request.settings.top_p = Setting::Value(0.8);
    assert_eq!(
        body(OpenAiPreset::Fireworks, &request),
        json!({
            "model":"accounts/fireworks/models/nemotron-3-ultra-nvfp4", "messages":[{"role":"user","content":"Hello"}],
            "stream":true,"max_tokens":80,"top_p":0.8
        })
    );
}

#[test]
fn mistral_golden_history_fixes_seed_and_prefix() {
    // MistralLLMAdapter applies tool separators before system-role conversion.
    let mut request = OpenAiPresetConfig::new(OpenAiPreset::Mistral).request(Context::new(vec![
        Message::text(Role::System, "Initial"),
        Message::text(Role::System, "Also initial"),
        Message::text(Role::User, "Hello"),
        Message::text(Role::System, "Late rule"),
        Message::assistant_tool_calls(vec![ToolCall {
            id: "abcdefghi".into(),
            name: "lookup".into(),
            arguments: json!({}),
        }]),
        Message::tool_result("abcdefghi", "42"),
    ]));
    request.settings.seed = Setting::Value(17);
    request.settings.max_tokens = Setting::Value(32);
    assert_eq!(
        body(OpenAiPreset::Mistral, &request),
        json!({
            "model":"mistral-small-latest","stream":true,"random_seed":17,"max_tokens":32,
            "messages":[{"role":"system","content":"Initial"},{"role":"system","content":"Also initial"},
            {"role":"user","content":"Hello"},{"role":"user","content":"Late rule"},
            {"role":"assistant","content":null,"tool_calls":[{"id":"abcdefghi","type":"function","function":{"name":"lookup","arguments":"{}"}}]},
            {"role":"tool","content":"42","tool_call_id":"abcdefghi"},
            {"role":"assistant","content":" ","prefix":true}]
        })
    );
    assert_eq!(request.context.messages.len(), 6);
    request.settings.seed = Setting::Value(0);
    let zero_seed = body(OpenAiPreset::Mistral, &request);
    assert!(zero_seed.get("seed").is_none());
    assert!(zero_seed.get("random_seed").is_none());
}

#[test]
fn mistral_preserves_assistant_after_tool_and_inserts_between_adjacent_tools() {
    let mut context = Context::new(vec![
        Message::assistant_tool_calls(vec![
            ToolCall {
                id: "one".into(),
                name: "first".into(),
                arguments: json!({}),
            },
            ToolCall {
                id: "two".into(),
                name: "second".into(),
                arguments: json!({}),
            },
        ]),
        Message::tool_result("one", "1"),
        Message::tool_result("two", "2"),
        Message::text(Role::Assistant, "Partial"),
    ]);
    let original = context.clone();
    let request = CompletionRequest::new("mistral", context.clone());
    let messages = body(OpenAiPreset::Mistral, &request)["messages"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(messages.len(), 5);
    assert_eq!(messages[2], json!({"role":"assistant","content":" "}));
    assert_eq!(
        messages[4],
        json!({"role":"assistant","content":"Partial","prefix":true})
    );
    assert_eq!(context, original);
    context.messages.push(Message::text(Role::User, "Continue"));
    assert!(body(
        OpenAiPreset::Mistral,
        &CompletionRequest::new("mistral", context)
    )["messages"][4]
        .get("prefix")
        .is_none());
}

#[test]
fn perplexity_golden_merges_late_system_and_drops_trailing_assistant() {
    let mut request =
        OpenAiPresetConfig::new(OpenAiPreset::Perplexity).request(Context::new(vec![
            Message::text(Role::System, "Initial"),
            Message::text(Role::System, "Also initial"),
            Message::text(Role::User, "One"),
            Message::text(Role::Developer, "Two"),
            Message::text(Role::System, "Three"),
            Message::text(Role::Assistant, "Answer"),
            Message::text(Role::User, "Four"),
            Message::text(Role::Assistant, "Discarded partial"),
        ]));
    request.settings.temperature = Setting::Null;
    request.settings.max_tokens = Setting::Value(100);
    assert_eq!(
        body(OpenAiPreset::Perplexity, &request),
        json!({
            "model":"sonar","stream":true,"max_tokens":100,"messages":[
                {"role":"system","content":"Initial"},{"role":"system","content":"Also initial"},
                {"role":"user","content":[{"type":"text","text":"One"},{"type":"text","text":"Two"},{"type":"text","text":"Three"}]},
                {"role":"assistant","content":"Answer"},{"role":"user","content":"Four"}
            ]
        })
    );
    assert_eq!(request.context.messages.len(), 8);
}

#[test]
fn openrouter_gemini_golden_and_provider_preferences_are_final_body_fields() {
    let mut config = OpenAiPresetConfig::new(OpenAiPreset::OpenRouter);
    config.settings.provider = Setting::Value(OpenRouterProviderPreferences {
        order: Some(vec!["first".into(), "second".into()]),
        allow_fallbacks: Some(false),
        require_parameters: Some(true),
        data_collection: Some("deny".into()),
        zdr: Some(true),
        preferred_min_throughput: Some(RoutingThreshold::Number(20.0)),
        extra: [
            ("future_option".into(), json!(false)),
            ("omitted_null".into(), Value::Null),
        ]
        .into_iter()
        .collect(),
        ..Default::default()
    });
    let mut request = CompletionRequest::new(
        "google/GEMINI-test",
        Context::new(vec![
            Message::text(Role::System, "First"),
            Message::text(Role::System, "Second"),
            Message::text(Role::Developer, "Third"),
            Message::text(Role::User, "Hello"),
            Message::text(Role::System, "Fourth"),
        ]),
    );
    assert_eq!(
        config.request_body(&request).unwrap(),
        json!({
            "model":"google/GEMINI-test","stream":true,"stream_options":{"include_usage":true},
            "messages":[{"role":"system","content":"First"},{"role":"user","content":"Second"},
                {"role":"user","content":"Third"},{"role":"user","content":"Hello"},{"role":"user","content":"Fourth"}],
            "provider":{"order":["first","second"],"allow_fallbacks":false,"require_parameters":true,
                "data_collection":"deny","zdr":true,"preferred_min_throughput":20.0,"future_option":false}
        })
    );
    request.model = "other-model".into();
    assert_eq!(
        config.request_body(&request).unwrap()["messages"][1]["role"],
        "system"
    );
    request.extra.insert(
        "extra_body".into(),
        json!({"provider":{"only":["override"]},"custom":false}),
    );
    let out = config.request_body(&request).unwrap();
    assert_eq!(out["provider"], json!({"only":["override"]}));
    assert_eq!(out["custom"], false);
    assert!(out.get("extra_body").is_none());
}

#[test]
fn provider_option_golden_defaults_null_false_and_precedence() {
    let mut inception = OpenAiPresetConfig::new(OpenAiPreset::Inception);
    let mut req = inception.request(user());
    assert_eq!(
        inception.request_body(&req).unwrap(),
        json!({
            "model":"mercury-2","messages":[{"role":"user","content":"Hello"}],
            "stream":true,"stream_options":{"include_usage":true},"realtime":true
        })
    );
    inception.settings.realtime = Setting::Value(false);
    inception.settings.reasoning_effort = Setting::Value(ReasoningEffort::Instant);
    req.extra.insert("reasoning_effort".into(), json!("high"));
    let out = inception.request_body(&req).unwrap();
    assert_eq!(out["realtime"], false);
    assert_eq!(out["reasoning_effort"], "instant");
    inception.settings.realtime = Setting::Null;
    assert!(inception
        .request_body(&req)
        .unwrap()
        .get("realtime")
        .is_none());
    req.extra.insert(
        "extra_body".into(),
        json!({"realtime":false,"reasoning_effort":"low"}),
    );
    let out = inception.request_body(&req).unwrap();
    assert_eq!(out["realtime"], false);
    assert_eq!(out["reasoning_effort"], "low");

    for (preset, expected) in [(OpenAiPreset::Groq, "low"), (OpenAiPreset::Xai, "high")] {
        let mut config = OpenAiPresetConfig::new(preset);
        config.settings.reasoning_effort = Setting::Value(ReasoningEffort::Low);
        let mut req = config.request(user());
        req.extra.insert("reasoning_effort".into(), json!("high"));
        assert_eq!(
            config.request_body(&req).unwrap()["reasoning_effort"],
            expected
        );
    }
    let mut together = OpenAiPresetConfig::new(OpenAiPreset::Together);
    together.settings.reasoning =
        Setting::Value(json!({"enabled":false}).as_object().unwrap().clone());
    assert_eq!(
        together.request_body(&together.request(user())).unwrap()["reasoning"],
        json!({"enabled":false})
    );
}

#[test]
fn source_specific_unsupported_features_fail_explicitly() {
    let config = OpenAiPresetConfig::new(OpenAiPreset::Azure);
    assert!(matches!(
        config.endpoint(),
        Err(ServiceError::Unsupported(_))
    ));
    assert!(matches!(
        config.request_body(&config.request(user())),
        Err(ServiceError::Unsupported(_))
    ));
    assert!(!OpenAiPreset::Azure.unsupported_features().is_empty());
    for preset in [
        OpenAiPreset::DeepSeek,
        OpenAiPreset::Fireworks,
        OpenAiPreset::Mistral,
        OpenAiPreset::Perplexity,
    ] {
        let mut config = OpenAiPresetConfig::new(preset);
        config.settings.max_completion_tokens = Setting::Value(100);
        assert!(matches!(
            config.request_body(&config.request(user())),
            Err(ServiceError::Unsupported(_))
        ));
    }
    for preset in [
        OpenAiPreset::DeepSeek,
        OpenAiPreset::Fireworks,
        OpenAiPreset::Perplexity,
    ] {
        let mut req = request(preset);
        req.settings.seed = Setting::Value(123);
        assert!(matches!(
            OpenAiPresetConfig::new(preset).request_body(&req),
            Err(ServiceError::Unsupported(_))
        ));
    }
    let mut config = OpenAiPresetConfig::new(OpenAiPreset::SambaNova);
    config.settings.service_tier = Setting::Value("priority".into());
    assert!(matches!(
        config.request_body(&config.request(user())),
        Err(ServiceError::Unsupported(_))
    ));
    let mut req = request(OpenAiPreset::Perplexity);
    req.context.tool_choice = Some(ToolChoice::Auto);
    assert!(matches!(
        OpenAiPresetConfig::new(OpenAiPreset::Perplexity).request_body(&req),
        Err(ServiceError::Unsupported(_))
    ));
}

#[test]
fn typed_provider_settings_are_not_silently_sent_to_other_providers() {
    let mut config = OpenAiPresetConfig::new(OpenAiPreset::OpenAi);
    config.settings.thinking = Setting::Value(DeepSeekThinking::Enabled);
    assert!(matches!(
        config.request_body(&config.request(user())),
        Err(ServiceError::Unsupported(_))
    ));
    let mut config = OpenAiPresetConfig::new(OpenAiPreset::Groq);
    config.settings.reasoning_effort = Setting::Value(ReasoningEffort::Xhigh);
    assert!(matches!(
        config.request_body(&config.request(user())),
        Err(ServiceError::Unsupported(_))
    ));
    config.settings.reasoning_effort = Setting::Value(ReasoningEffort::Default);
    assert_eq!(
        config.request_body(&config.request(user())).unwrap()["reasoning_effort"],
        "default"
    );
}

#[test]
fn extra_body_cannot_bypass_reserved_streaming_contract() {
    for key in [
        "stream",
        "model",
        "messages",
        "tools",
        "tool_choice",
        "stream_options",
        "n",
        "extra_body",
    ] {
        let config = OpenAiPresetConfig::new(OpenAiPreset::OpenRouter);
        let mut request = config.request(user());
        request
            .extra
            .insert("extra_body".into(), json!({key:false}));
        assert!(
            matches!(
                config.request_body(&request),
                Err(ServiceError::InvalidRequest(_))
            ),
            "{key}"
        );
    }
    let config = OpenAiPresetConfig::new(OpenAiPreset::DeepSeek);
    let mut request = config.request(user());
    request
        .settings
        .base
        .extra
        .insert("extra_body".into(), json!({"thinking":{"type":"enabled"}}));
    assert_eq!(
        config.request_body(&request).unwrap()["thinking"],
        json!({"type":"enabled"})
    );
    assert!(request.settings.base.extra.contains_key("extra_body"));
    request.extra.insert("extra_body".into(), json!(42));
    request.settings.base.extra.clear();
    assert!(matches!(
        config.request_body(&request),
        Err(ServiceError::InvalidRequest(_))
    ));
}

#[test]
fn xai_usage_matches_python_without_double_counting_or_overflow() {
    let config = OpenAiPresetConfig::new(OpenAiPreset::Xai);
    let mut usage = Usage {
        prompt_tokens: 10,
        completion_tokens: 5,
        total_tokens: 18,
        reasoning_tokens: Some(3),
        ..Default::default()
    };
    config.normalize_usage(&mut usage);
    assert_eq!(usage.completion_tokens, 8);
    config.normalize_usage(&mut usage);
    assert_eq!(usage.completion_tokens, 8);
    let mut already_included = Usage {
        prompt_tokens: 10,
        completion_tokens: 5,
        total_tokens: 15,
        reasoning_tokens: Some(3),
        ..Default::default()
    };
    config.normalize_usage(&mut already_included);
    assert_eq!(already_included.completion_tokens, 5);
    let mut overflow = Usage {
        prompt_tokens: u64::MAX,
        completion_tokens: 5,
        total_tokens: 1,
        reasoning_tokens: Some(3),
        ..Default::default()
    };
    config.normalize_usage(&mut overflow);
    assert_eq!(overflow.completion_tokens, 5);
    let mut plain = Usage {
        prompt_tokens: 10,
        completion_tokens: 5,
        total_tokens: 18,
        reasoning_tokens: Some(3),
        ..Default::default()
    };
    OpenAiPresetConfig::new(OpenAiPreset::OpenAi).normalize_usage(&mut plain);
    assert_eq!(plain.completion_tokens, 5);
}

#[test]
fn sparse_settings_preserve_null_omission_and_resolved_model() {
    for preset in [
        OpenAiPreset::OpenAi,
        OpenAiPreset::Fireworks,
        OpenAiPreset::DeepSeek,
        OpenAiPreset::Mistral,
    ] {
        let mut req = request(preset);
        req.settings.temperature = Setting::Null;
        req.settings.max_tokens = Setting::Null;
        let out = body(preset, &req);
        assert_eq!(out.get("temperature"), Some(&Value::Null), "{preset:?}");
        assert_eq!(out.get("max_tokens"), Some(&Value::Null), "{preset:?}");
        assert!(out.get("top_p").is_none());
    }
    let mut config = OpenAiPresetConfig::new(OpenAiPreset::OpenAi);
    config.settings.max_completion_tokens = Setting::Null;
    assert_eq!(
        config
            .request_body(&config.request(user()))
            .unwrap()
            .get("max_completion_tokens"),
        Some(&Value::Null)
    );
    let mut req = request(OpenAiPreset::OpenRouter);
    req.context
        .messages
        .insert(0, Message::text(Role::System, "First"));
    req.context
        .messages
        .insert(1, Message::text(Role::System, "Second"));
    req.settings.base.model = Setting::Value("google/gemini-custom".into());
    let out = body(OpenAiPreset::OpenRouter, &req);
    assert_eq!(out["model"], "google/gemini-custom");
    assert_eq!(out["messages"][1]["role"], "user");
    assert_eq!(req.model, "openai/gpt-4.1");
    req.settings.base.model = Setting::Null;
    assert!(matches!(
        OpenAiPresetConfig::new(OpenAiPreset::OpenRouter).request_body(&req),
        Err(ServiceError::InvalidRequest(_))
    ));
}

#[test]
fn empty_context_can_validate_settings_but_perplexity_does_not_erase_a_turn() {
    for preset in OpenAiPreset::ALL
        .into_iter()
        .filter(|p| p.unsupported_reason().is_none())
    {
        let req = OpenAiPresetConfig::new(preset).request(Context::default());
        assert_eq!(body(preset, &req)["messages"], json!([]), "{preset:?}");
    }
    let config = OpenAiPresetConfig::new(OpenAiPreset::Perplexity);
    let req = config.request(Context::new(vec![Message::text(
        Role::Assistant,
        "Partial",
    )]));
    assert!(matches!(
        config.request_body(&req),
        Err(ServiceError::InvalidRequest(_))
    ));
    let req = config.request(Context::new(vec![
        Message::assistant_tool_calls(vec![ToolCall {
            id: "one".into(),
            name: "lookup".into(),
            arguments: json!({}),
        }]),
        Message::tool_result("one", "42"),
    ]));
    assert!(matches!(
        config.request_body(&req),
        Err(ServiceError::Unsupported(_))
    ));
}

#[test]
fn nested_extras_cannot_reenable_explicitly_unsupported_provider_fields() {
    for (preset, key, value) in [
        (OpenAiPreset::DeepSeek, "seed", json!(1)),
        (OpenAiPreset::Fireworks, "max_completion_tokens", json!(8)),
        (OpenAiPreset::Mistral, "service_tier", json!("priority")),
        (OpenAiPreset::SambaNova, "service_tier", json!("priority")),
        (
            OpenAiPreset::DeepSeek,
            "response_format",
            json!({"type":"json_schema"}),
        ),
    ] {
        let config = OpenAiPresetConfig::new(preset);
        let mut req = config.request(user());
        req.extra.insert("extra_body".into(), json!({key:value}));
        assert!(
            matches!(config.request_body(&req), Err(ServiceError::Unsupported(_))),
            "{preset:?} {key}"
        );
    }
}

#[test]
fn ollama_uses_the_source_public_dummy_token() {
    assert_eq!(OpenAiPreset::Ollama.default_api_key(), Some("ollama"));
    for preset in OpenAiPreset::ALL
        .into_iter()
        .filter(|p| *p != OpenAiPreset::Ollama)
    {
        assert_eq!(preset.default_api_key(), None);
    }
}

#[test]
fn ollama_client_uses_source_dummy_key_without_external_credentials() {
    let config = pipecat_services::ClientConfig::ollama().unwrap();
    assert_eq!(config.api_key.as_deref(), Some("ollama"));
    assert_eq!(
        config.endpoint,
        "http://localhost:11434/v1/chat/completions"
    );
    assert_eq!(config.openai_preset.unwrap().preset, OpenAiPreset::Ollama);
}
