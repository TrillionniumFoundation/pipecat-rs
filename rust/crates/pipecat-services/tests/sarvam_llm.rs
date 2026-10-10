//! Static-source Sarvam contracts over real loopback HTTP/SSE, synthetic keys only.
use pipecat_core::{self as core, Direction, Frame, FrameData, Pipeline, PipelineConfig};
use pipecat_services::{presets::*, *};
use serde_json::{json, Value};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("loopback fixture stalled")
}
fn preset() -> OpenAiPresetConfig {
    OpenAiPresetConfig::new(OpenAiPreset::Sarvam)
}
fn request(model: &str) -> CompletionRequest {
    CompletionRequest::new(
        model,
        Context::new(vec![
            Message::text(Role::Developer, "rule"),
            Message::text(Role::User, "hello"),
        ]),
    )
}
fn config(endpoint: String) -> ClientConfig {
    let mut config = ClientConfig::openai_compatible(preset(), "synthetic-sarvam").unwrap();
    config.endpoint = endpoint;
    config.request_timeout = Duration::from_secs(2);
    config.idle_timeout = Duration::from_secs(1);
    config
}
fn tool() -> ToolDefinition {
    ToolDefinition {
        name: "lookup".into(),
        description: "lookup".into(),
        parameters: json!({"type":"object"}),
    }
}
fn core_context() -> core::LlmContextData {
    core::LlmContextData {
        messages: vec![core::ChatMessage::new(core::ChatRole::User, "inference").into()],
        tools: core::ToolSet::NotGiven,
        tool_choice: None,
    }
}
async fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
    let mut chunk = [0; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        assert!(bytes.len() < 1024 * 1024);
        if let Some(i) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).unwrap().to_owned();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|n| n.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    (
        headers,
        serde_json::from_slice(&bytes[end..end + length]).unwrap(),
    )
}
fn wire(records: Vec<Value>, done: bool) -> Vec<u8> {
    let mut wire: String = records
        .into_iter()
        .map(|record| format!("data: {record}\r\n\r\n"))
        .collect();
    if done {
        wire.push_str("data: [DONE]\r\n\r\n");
    }
    wire.into_bytes()
}
fn delta(value: Value) -> Value {
    json!({"choices":[{"index":0,"delta":value}]})
}
async fn peer(body: Vec<u8>, path: &str) -> (ClientConfig, JoinHandle<(String, Value)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(format!("http://{}{}", listener.local_addr().unwrap(), path));
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
        for byte in body {
            if socket.write_all(&[byte]).await.is_err() {
                break;
            }
            tokio::task::yield_now().await;
        }
        request
    });
    (config, peer)
}
async fn collect(mut stream: LlmStream) -> Vec<Result<StreamEvent, ServiceError>> {
    bounded(async move {
        let mut events = vec![];
        while let Some(event) = stream.next().await {
            events.push(event);
        }
        events
    })
    .await
}
fn text(events: &[Result<StreamEvent, ServiceError>]) -> String {
    events
        .iter()
        .filter_map(|event| match event {
            Ok(StreamEvent::TextDelta(text)) => Some(text.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn sarvam_defaults_fields_precedence_and_input_immutability() {
    let mut preset = preset();
    preset.settings.wiki_grounding = Setting::Value(true);
    preset.settings.reasoning_effort = Setting::Value(ReasoningEffort::Low);
    preset.settings.max_completion_tokens = Setting::Value(999);
    preset.settings.service_tier = Setting::Value("priority".into());
    let mut request = request("sarvam-105b");
    request.max_tokens = Some(888);
    request.settings.max_tokens = Setting::Value(123);
    request.settings.seed = Setting::Value(42);
    request
        .extra
        .insert("reasoning_effort".into(), json!("high"));
    request.extra.insert(
        "extra_body".into(),
        json!({"wiki_grounding":false,"custom":17}),
    );
    let original = serde_json::to_value(&request).unwrap();
    assert_eq!(
        preset.request_body(&request).unwrap(),
        json!({"model":"sarvam-105b","stream":true,"messages":[{"role":"user","content":"rule"},{"role":"user","content":"hello"}],"seed":42,"max_tokens":123,"wiki_grounding":false,"reasoning_effort":"low","custom":17})
    );
    assert_eq!(serde_json::to_value(&request).unwrap(), original);
    request
        .settings
        .base
        .extra
        .insert("reasoning_effort".into(), json!("medium"));
    assert_eq!(
        preset.request_body(&request).unwrap()["reasoning_effort"],
        "medium"
    );
    request.extra.insert("extra_body".into(),json!({"reasoning_effort":"high","wiki_grounding":null,"max_completion_tokens":12,"service_tier":"flex"}));
    let body = preset.request_body(&request).unwrap();
    assert_eq!(body["reasoning_effort"], "high");
    for field in [
        "wiki_grounding",
        "stream_options",
        "max_completion_tokens",
        "service_tier",
        "extra_body",
    ] {
        assert!(body.get(field).is_none(), "{field}");
    }
    request.extra.clear();
    request
        .settings
        .base
        .extra
        .insert("reasoning_effort".into(), Value::Null);
    request
        .settings
        .base
        .extra
        .insert("wiki_grounding".into(), Value::Null);
    let body = preset.request_body(&request).unwrap();
    assert!(body.get("reasoning_effort").is_none() && body.get("wiki_grounding").is_none());
}
#[test]
fn sarvam_portable_budget_is_filtered_and_source_budget_is_not_remapped() {
    let mut request = request("sarvam-105b");
    request.max_tokens = Some(91);
    let body = preset().request_body(&request).unwrap();
    assert!(body.get("max_tokens").is_none() && body.get("max_completion_tokens").is_none());
    request
        .extra
        .insert("max_completion_tokens".into(), json!(77));
    request
        .extra
        .insert("service_tier".into(), json!("priority"));
    assert!(preset()
        .request_body(&request)
        .unwrap()
        .get("max_completion_tokens")
        .is_none());
    request.settings.max_tokens = Setting::Value(12);
    assert_eq!(preset().request_body(&request).unwrap()["max_tokens"], 12);
    request.settings.max_tokens = Setting::Null;
    assert!(preset().request_body(&request).unwrap()["max_tokens"].is_null());
}
#[test]
fn sarvam_capability_matrix_checks_all_image_forms_and_resolved_models() {
    for model in sarvam::MODELS {
        for image in [
            ContentPart::ImageUrl {
                url: "https://example.test/a.png".into(),
            },
            ContentPart::ImageUrlDetailed {
                url: "https://example.test/a.png".into(),
                detail: "high".into(),
            },
            ContentPart::InlineData {
                mime_type: "image/png".into(),
                data: "YWJj".into(),
            },
        ] {
            let mut request = request("sarvam-105b");
            request.settings.base.model = Setting::Value(model.into());
            request.context.messages[1].content = Some(Content::Parts(vec![image]));
            assert_eq!(
                preset().request_body(&request).is_ok(),
                model == "gemma4",
                "{model}"
            );
        }
        for effort in [
            "low", "medium", "high", "none", "instant", "default", "xhigh",
        ] {
            let mut request = request(model);
            request
                .settings
                .base
                .extra
                .insert("reasoning_effort".into(), json!(effort));
            assert_eq!(
                preset().request_body(&request).is_ok(),
                model != "sarvam-105b-conversations" && matches!(effort, "low" | "medium" | "high"),
                "{model}/{effort}"
            );
        }
        for wiki in [true, false] {
            let mut request = request(model);
            request
                .extra
                .insert("extra_body".into(), json!({"wiki_grounding":wiki}));
            assert_eq!(
                preset().request_body(&request).is_ok(),
                matches!(model, "gemma4" | "sarvam-105b"),
                "{model}/{wiki}"
            );
        }
        let mut request = request(model);
        request
            .settings
            .base
            .extra
            .insert("reasoning_effort".into(), Value::Null);
        request
            .settings
            .base
            .extra
            .insert("wiki_grounding".into(), Value::Null);
        let body = preset().request_body(&request).unwrap();
        assert!(body.get("wiki_grounding").is_none() && body.get("reasoning_effort").is_none());
    }
}
#[test]
fn sarvam_all_tool_choices_require_nonempty_tools_even_none() {
    for choice in [
        ToolChoice::Auto,
        ToolChoice::None,
        ToolChoice::Required,
        ToolChoice::Function("lookup".into()),
    ] {
        for tools in [None, Some(vec![]), Some(vec![tool()])] {
            let valid = tools.as_ref().is_some_and(|tools| !tools.is_empty());
            let mut request = request("sarvam-105b-conversations");
            request.context.tools = tools;
            request.context.tool_choice = Some(choice.clone());
            assert_eq!(preset().request_body(&request).is_ok(), valid, "{choice:?}");
        }
    }
}
#[tokio::test]
async fn sarvam_model_routes_headers_and_fixed_override_on_actual_http() {
    for model in sarvam::MODELS {
        for path in ["/{version}/chat/completions", "/custom/fixed?opaque=ok"] {
            let (mut config, peer) = peer(wire(vec![], true), path).await;
            for (key, value) in [
                ("uSeR-aGeNt", "wrong"),
                ("API-Subscription-Key", "wrong"),
                ("Authorization", "wrong"),
                ("X-Test", "custom"),
            ] {
                config
                    .sarvam_default_headers
                    .insert(key.into(), value.into());
            }
            let client = LlmClient::new(config).unwrap();
            let events = collect(client.stream(request(model)).unwrap()).await;
            assert!(events.iter().all(Result::is_ok), "{events:?}");
            let (headers, body) = bounded(peer).await.unwrap();
            let expected = if path.contains("{version}") {
                format!("/{}/chat/completions", sarvam::api_version(model).unwrap())
            } else {
                path.into()
            };
            assert!(
                headers.starts_with(&format!("POST {expected} HTTP/1.1")),
                "{headers}"
            );
            let lower = headers.to_ascii_lowercase();
            for (name, value) in [
                ("authorization", "Bearer synthetic-sarvam"),
                ("api-subscription-key", "synthetic-sarvam"),
                ("user-agent", sarvam::USER_AGENT_VALUE),
                ("x-test", "custom"),
            ] {
                let actual: Vec<_> = headers
                    .lines()
                    .filter(|line| line.to_ascii_lowercase().starts_with(&format!("{name}:")))
                    .collect();
                assert_eq!(actual.len(), 1);
                assert_eq!(actual[0].split_once(':').unwrap().1.trim(), value);
            }
            assert!(!lower.contains("python/"));
            assert_eq!(body["model"], model);
            assert!(body.get("stream_options").is_none());
        }
    }
    assert_eq!(
        ClientConfig::openai_compatible(preset(), "synthetic")
            .unwrap()
            .endpoint,
        sarvam::DEFAULT_ENDPOINT
    );
}
#[tokio::test]
async fn sarvam_rejected_inputs_never_connect_and_reserved_contracts_remain_protected() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/{{version}}/chat/completions",
        listener.local_addr().unwrap()
    );
    let client = LlmClient::new(config(endpoint.clone())).unwrap();
    for model in [
        "",
        "unknown",
        "GEMMA4",
        "sarvam-105b-conversations/../gemma4",
    ] {
        assert!(client.stream(request(model)).is_err());
    }
    for (key, value) in [
        ("reasoning_effort", json!(false)),
        ("wiki_grounding", json!("false")),
        ("max_tokens", json!(0)),
        ("max_tokens", json!(-1)),
    ] {
        let mut request = request("sarvam-105b");
        request
            .extra
            .insert("extra_body".into(), json!({key:value}));
        assert!(client.stream(request).is_err());
    }
    for key in [
        "model",
        "stream",
        "messages",
        "tools",
        "tool_choice",
        "stream_options",
        "n",
    ] {
        for nested in [true, false] {
            let mut request = request("sarvam-105b");
            if nested {
                request.extra.insert("extra_body".into(), json!({key:1}));
            } else {
                request.extra.insert(key.into(), json!(1));
            }
            assert!(client.stream(request).is_err(), "{key}/{nested}");
        }
    }
    for value in [json!(1), json!("object"), json!([])] {
        let mut request = request("sarvam-105b");
        request.extra.insert("extra_body".into(), value);
        assert!(client.stream(request).is_err());
    }
    for path in [
        "/{version}/{version}/chat/completions",
        "/{model}/chat/completions",
        "/v{version}/chat/completions",
        "/{version}/other",
        "/chat/completions?version={version}",
        "/{version}/chat/completions?extra=yes",
        "?x=/{version}/chat/completions",
    ] {
        let config = config(format!("http://{}{}", listener.local_addr().unwrap(), path));
        assert!(
            LlmClient::new(config)
                .unwrap()
                .stream(request("gemma4"))
                .is_err(),
            "{path}"
        );
    }
    let mut invalid_authority = config("https://{version}/chat/completions".into());
    assert!(LlmClient::new(invalid_authority.clone())
        .unwrap()
        .stream(request("gemma4"))
        .is_err());
    invalid_authority.endpoint = "https://api.sarvam.ai/{version}/chat/completions#fragment".into();
    assert!(LlmClient::new(invalid_authority)
        .unwrap()
        .stream(request("gemma4"))
        .is_err());
    for model in sarvam::MODELS {
        if model != "gemma4" {
            let mut invalid = request(model);
            invalid.context.messages[1].content =
                Some(Content::Parts(vec![ContentPart::ImageUrl {
                    url: "https://example.test/image.png".into(),
                }]));
            assert!(client.stream(invalid).is_err());
        }
        if !matches!(model, "gemma4" | "sarvam-105b") {
            let mut invalid = request(model);
            invalid
                .settings
                .base
                .extra
                .insert("wiki_grounding".into(), json!(false));
            assert!(client.stream(invalid).is_err());
        }
        let mut invalid = request(model);
        invalid.context.tool_choice = Some(ToolChoice::None);
        assert!(client.stream(invalid).is_err());
    }
    let mut invalid = request("sarvam-105b-conversations");
    invalid
        .extra
        .insert("extra_body".into(), json!({"reasoning_effort":"low"}));
    assert!(client.stream(invalid).is_err());
    for key in [None, Some(""), Some("  "), Some("bad\nvalue")] {
        let mut config = config(endpoint.clone());
        config.api_key = key.map(str::to_owned);
        assert!(LlmClient::new(config).is_err());
    }
    for (key, value) in [
        ("bad\nname", "ok"),
        ("x-bad", "bad\nvalue"),
        ("Content-Type", "application/json"),
        ("Host", "example.test"),
    ] {
        let mut config = config(endpoint.clone());
        config
            .sarvam_default_headers
            .insert(key.into(), value.into());
        assert!(LlmClient::new(config).is_err());
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(60), listener.accept())
            .await
            .is_err(),
        "preflight created a connection"
    );
}
#[tokio::test]
async fn sarvam_fragmented_sse_tools_usage_and_nvidia_filter_isolation() {
    let records = vec![
        delta(json!({"content":"<think>literal</think> नमस्ते🙂"})),
        delta(
            json!({"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"lookup","arguments":"{\"q\":"}}]}),
        ),
        json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5,"completion_tokens_details":{"reasoning_tokens":1}}}),
    ];
    let (config, peer) = peer(wire(records, true), "/stream").await;
    let events = collect(
        LlmClient::new(config)
            .unwrap()
            .stream(request("gemma4"))
            .unwrap(),
    )
    .await;
    bounded(peer).await.unwrap();
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert_eq!(text(&events), "<think>literal</think> नमस्ते🙂");
    assert!(!events.iter().any(|event| matches!(
        event,
        Ok(StreamEvent::ThoughtStart | StreamEvent::ThoughtEnd | StreamEvent::ThoughtDelta(_))
    )));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event, Ok(StreamEvent::Usage(_))))
            .count(),
        1
    );
    assert!(events.iter().any(|event|matches!(event,Ok(StreamEvent::ToolCall(call)) if call.id=="call"&&call.arguments==json!({"q":1}))));
    assert!(events.iter().any(|event|matches!(event,Ok(StreamEvent::Usage(usage)) if usage.prompt_tokens==2&&usage.total_tokens==5&&usage.reasoning_tokens==Some(1))));
    assert!(matches!(
        events.last(),
        Some(Ok(StreamEvent::Finished {
            reason: Some(FinishReason::ToolCalls)
        }))
    ));
}
#[tokio::test]
async fn sarvam_does_not_add_headers_or_filter_body_on_generic_openai() {
    let (mut config, peer) = peer(
        wire(vec![delta(json!({"content":"<think>same</think>"}))], true),
        "/stream",
    )
    .await;
    config.openai_preset = None;
    let events = collect(
        LlmClient::new(config)
            .unwrap()
            .stream(request("arbitrary-openai-model"))
            .unwrap(),
    )
    .await;
    assert_eq!(text(&events), "<think>same</think>");
    let (headers, body) = bounded(peer).await.unwrap();
    let headers = headers.to_ascii_lowercase();
    assert!(!headers.contains("api-subscription-key:"));
    assert!(!headers.contains("rust/native"));
    assert_eq!(body["stream_options"]["include_usage"], true);
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config
        .sarvam_default_headers
        .insert("x-test".into(), "x".into());
    assert!(LlmClient::new(config).is_err());
}
#[tokio::test]
async fn sarvam_atomic_runtime_options_model_switch_and_restore() {
    let (mut config, peer) = peer(wire(vec![], true), "/{version}/chat/completions").await;
    config
        .openai_preset
        .as_mut()
        .unwrap()
        .settings
        .wiki_grounding = Setting::Value(true);
    config
        .openai_preset
        .as_mut()
        .unwrap()
        .settings
        .reasoning_effort = Setting::Value(ReasoningEffort::High);
    let client = LlmClient::new(config).unwrap();
    let processor = LlmProcessor::new(client.clone(), request("sarvam-105b"));
    let before = processor.request_snapshot().unwrap();
    let mut delta = LlmSettings::default();
    delta.base.model = Setting::Value("sarvam-105b-conversations".into());
    assert!(processor.update_settings(&delta).is_err());
    assert_eq!(
        serde_json::to_value(processor.request_snapshot().unwrap()).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    delta
        .base
        .extra
        .insert("reasoning_effort".into(), Value::Null);
    delta
        .base
        .extra
        .insert("wiki_grounding".into(), Value::Null);
    processor.update_settings(&delta).unwrap();
    let updated = processor.request_snapshot().unwrap();
    let body = client.request_body(&updated).unwrap();
    assert_eq!(body["model"], "sarvam-105b-conversations");
    assert!(body.get("wiki_grounding").is_none() && body.get("reasoning_effort").is_none());
    assert!(collect(client.stream(updated).unwrap())
        .await
        .iter()
        .all(Result::is_ok));
    let (headers, body) = bounded(peer).await.unwrap();
    assert!(headers.starts_with("POST /v1/chat/completions "));
    assert_eq!(body["model"], "sarvam-105b-conversations");
    let mut delta = LlmSettings::default();
    delta.base.model = Setting::Value("gemma4".into());
    delta
        .base
        .extra
        .insert("wiki_grounding".into(), json!(false));
    delta
        .base
        .extra
        .insert("reasoning_effort".into(), json!("medium"));
    processor.update_settings(&delta).unwrap();
    assert_eq!(
        client
            .request_body(&processor.request_snapshot().unwrap())
            .unwrap()["wiki_grounding"],
        false
    );
    assert_eq!(
        client.request_body(&before).unwrap()["wiki_grounding"],
        true
    );
    delta
        .base
        .extra
        .insert("extra_body".into(), json!({"reasoning_effort":"invalid"}));
    let previous = serde_json::to_value(processor.request_snapshot().unwrap()).unwrap();
    assert!(processor.update_settings(&delta).is_err());
    assert_eq!(
        serde_json::to_value(processor.request_snapshot().unwrap()).unwrap(),
        previous
    );
}
#[tokio::test]
async fn sarvam_direct_inference_aggregates_sse_with_per_call_settings() {
    let (config, peer) = peer(
        wire(
            vec![
                delta(json!({"content":"one"})),
                delta(json!({"content":"🙂two"})),
            ],
            true,
        ),
        "/{version}/chat/completions",
    )
    .await;
    let processor = LlmProcessor::new(LlmClient::new(config).unwrap(), request("sarvam-105b"));
    let before = serde_json::to_value(processor.request_snapshot().unwrap()).unwrap();
    let mut options = InferenceOptions::default();
    options.settings.base.model = Setting::Value("sarvam-105b-conversations".into());
    options.settings.system_instruction = Setting::Value("per-call rule".into());
    options.settings.max_tokens = Setting::Value(77);
    assert_eq!(
        bounded(processor.run_inference(core_context(), options))
            .await
            .unwrap(),
        Some("one🙂two".into())
    );
    let (headers, body) = bounded(peer).await.unwrap();
    assert!(headers.starts_with("POST /v1/chat/completions "));
    assert_eq!(
        body["messages"][0],
        json!({"role":"system","content":"per-call rule"})
    );
    assert_eq!(body["max_tokens"], 77);
    assert_eq!(body["stream"], true);
    assert_eq!(
        serde_json::to_value(processor.request_snapshot().unwrap()).unwrap(),
        before
    );
}
#[tokio::test]
async fn sarvam_rejected_pipeline_update_is_nonfatal_and_next_request_succeeds() {
    let (config, peer) = peer(
        wire(vec![delta(json!({"content":"recovered"}))], true),
        "/stream",
    )
    .await;
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        request("sarvam-105b"),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut delta = LlmSettings::default();
    delta.base.model = Setting::Value("sarvam-105b-conversations".into());
    delta
        .base
        .extra
        .insert("wiki_grounding".into(), json!(false));
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmUpdateSettings(Box::new(
                core::ServiceUpdateSettingsFrame {
                    delta: Some(delta),
                    ..Default::default()
                },
            ))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    bounded(async {
        loop {
            let event = worker.recv().await.unwrap();
            if let FrameData::Error(error) = event.frame.data {
                assert!(!error.fatal);
                assert_eq!(event.direction, Direction::Upstream);
                break;
            }
        }
    })
    .await;
    assert!(matches!(
        processor.request_snapshot().unwrap().settings.base.model,
        Setting::NotGiven
    ));
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    let mut output = String::new();
    bounded(async {
        loop {
            match worker.recv().await.unwrap().frame.data {
                FrameData::LlmText(text) => output.push_str(&text.text),
                FrameData::LlmResponseEnd => break,
                FrameData::Error(error) => panic!("{}", error.error),
                _ => {}
            }
        }
    })
    .await;
    assert_eq!(output, "recovered");
    assert_eq!(bounded(peer).await.unwrap().1["model"], "sarvam-105b");
    worker.end().await.unwrap();
    bounded(worker.join()).await.unwrap();
}
#[tokio::test]
async fn sarvam_missing_terminal_and_malformed_tool_json_fail_closed() {
    for body in [
        wire(vec![delta(json!({"content":"partial"}))], false),
        wire(
            vec![delta(
                json!({"tool_calls":[{"index":0,"id":"bad","function":{"name":"lookup","arguments":"[1]"}}]}),
            )],
            true,
        ),
    ] {
        let (config, peer) = peer(body, "/stream").await;
        let events = collect(
            LlmClient::new(config)
                .unwrap()
                .stream(request("sarvam-105b"))
                .unwrap(),
        )
        .await;
        bounded(peer).await.unwrap();
        assert!(events.iter().any(Result::is_err));
        assert!(!events
            .iter()
            .any(|event| matches!(event, Ok(StreamEvent::ToolCall(_)))));
    }
}
#[tokio::test]
async fn sarvam_early_text_cancel_drop_timeout_close_socket_and_preserve_usage() {
    for mode in ["cancel", "drop", "timeout"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(format!("http://{}/stream", listener.local_addr().unwrap()));
        if mode == "timeout" {
            config.idle_timeout = Duration::from_millis(70);
        }
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n").await.unwrap();
            socket.write_all(&wire(vec![json!({"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":1,"total_tokens":8}}),delta(json!({"content":"early"}))],false)).await.unwrap();
            let mut byte = [0];
            assert_eq!(bounded(socket.read(&mut byte)).await.unwrap(), 0);
        });
        let mut stream = LlmClient::new(config)
            .unwrap()
            .stream(request("gemma4"))
            .unwrap();
        bounded(async {loop {if matches!(stream.next().await,Some(Ok(StreamEvent::TextDelta(text))) if text=="early") {break;}}}).await;
        if mode == "drop" {
            drop(stream);
        } else {
            if mode == "cancel" {
                stream.cancel();
            }
            let events = collect(stream).await;
            assert!(events.iter().any(
                |event| matches!(event,Ok(StreamEvent::Usage(usage)) if usage.total_tokens==8)
            ));
            assert!(events.iter().any(|event| matches!(
                event,
                Err(ServiceError::Cancelled | ServiceError::Timeout)
            )));
        }
        bounded(peer).await.unwrap();
    }
}

#[tokio::test]
async fn sarvam_http_and_provider_errors_and_non_sse_success_are_not_replayed() {
    for (status, content_type, body) in [
        (
            429,
            "application/json",
            r#"{"error":{"message":"synthetic rate limit"}}"#,
        ),
        (200, "application/json", r#"{"choices":[]}"#),
        (
            200,
            "text/event-stream",
            "data: {\"error\":{\"type\":\"bad_request\",\"message\":\"synthetic SSE error\"}}\n\n",
        ),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = LlmClient::new(config(format!(
            "http://{}/stream",
            listener.local_addr().unwrap()
        )))
        .unwrap();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nRetry-After: 3\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            drop(socket);
            assert!(
                tokio::time::timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
        });
        let events = collect(client.stream(request("sarvam-105b")).unwrap()).await;
        assert_eq!(
            events.iter().filter(|event| event.is_err()).count(),
            1,
            "{events:?}"
        );
        if status == 429 {
            assert!(events.iter().any(|event|matches!(event,Err(ServiceError::Http{status:429,retry_after:Some(delay),..}) if delay=="3")));
        }
        assert!(matches!(
            events.last(),
            Some(Ok(StreamEvent::Finished { .. }))
        ));
        bounded(peer).await.unwrap();
    }
}

#[tokio::test]
async fn sarvam_invalid_inference_is_zero_io_and_valid_tool_only_inference_is_none() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let processor = LlmProcessor::new(
        LlmClient::new(config(format!(
            "http://{}/stream",
            listener.local_addr().unwrap()
        )))
        .unwrap(),
        request("sarvam-105b"),
    );
    let mut options = InferenceOptions::default();
    options.settings.base.model = Setting::Value("sarvam-105b-conversations".into());
    options
        .settings
        .base
        .extra
        .insert("reasoning_effort".into(), json!("medium"));
    assert!(processor
        .run_inference(core_context(), options)
        .await
        .is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
    let(config,peer)=peer(wire(vec![delta(json!({"tool_calls":[{"index":0,"id":"call","function":{"name":"lookup","arguments":"{}"}}]}))],true),"/stream").await;
    let processor = LlmProcessor::new(LlmClient::new(config).unwrap(), request("sarvam-105b"));
    assert_eq!(
        processor
            .run_inference(core_context(), InferenceOptions::default())
            .await
            .unwrap(),
        None
    );
    bounded(peer).await.unwrap();
}
