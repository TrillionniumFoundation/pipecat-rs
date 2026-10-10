//! Independent Sarvam request admission and actual localhost HTTP/SSE contracts.
use pipecat_core::{ChatMessage, ChatRole, LlmContextData, LlmSettings, Setting};
use pipecat_services::{
    presets::{OpenAiPreset, OpenAiPresetConfig, ReasoningEffort},
    ClientConfig, CompletionRequest, Content, ContentPart, Context, InferenceOptions, LlmClient,
    LlmInference, LlmProcessor, Message, Provider, ServiceError, StreamEvent, ToolChoice,
    ToolDefinition,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

const MODELS: [&str; 5] = [
    "deepseekv4-flash",
    "gemma4",
    "glm5.2",
    "sarvam-105b",
    "sarvam-105b-conversations",
];
fn preset() -> OpenAiPresetConfig {
    OpenAiPresetConfig::new(OpenAiPreset::Sarvam)
}
fn request(model: &str) -> CompletionRequest {
    CompletionRequest::new(
        model,
        Context::new(vec![
            Message::text(pipecat_services::Role::Developer, "developer hint"),
            Message::text(pipecat_services::Role::User, "hello"),
        ]),
    )
}
fn config(endpoint: &str) -> ClientConfig {
    let mut c = ClientConfig::openai_compatible(preset(), "synthetic-sarvam-review-key").unwrap();
    c.endpoint = endpoint.into();
    c.request_timeout = Duration::from_secs(4);
    c.idle_timeout = Duration::from_secs(2);
    c
}
fn tool() -> ToolDefinition {
    ToolDefinition {
        name: "lookup".into(),
        description: "Local fixture".into(),
        parameters: json!({"type":"object", "properties":{"city":{"type":"string"}}}),
    }
}
async fn capture(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    let mut buf = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buf).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buf[..n]);
        if let Some(at) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let header = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length: usize = header
        .lines()
        .find_map(|line| {
            let (k, v) = line.split_once(':')?;
            k.eq_ignore_ascii_case("content-length")
                .then(|| v.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut buf).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buf[..n]);
    }
    (
        header,
        serde_json::from_slice(&bytes[end..end + length]).unwrap(),
    )
}
fn wire(records: &[Value], done: bool) -> Vec<u8> {
    let mut out: String = records.iter().map(|v| format!("data: {v}\n\n")).collect();
    if done {
        out.push_str("data: [DONE]\n\n");
    }
    out.into_bytes()
}
async fn response(socket: &mut TcpStream, records: &[Value], done: bool, bytewise: bool) {
    let body = wire(records, done);
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
    for chunk in body.chunks(if bytewise { 1 } else { body.len().max(1) }) {
        if socket
            .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
            .await
            .is_err()
        {
            return;
        }
        if socket.write_all(chunk).await.is_err() {
            return;
        }
        if socket.write_all(b"\r\n").await.is_err() {
            return;
        }
    }
    let _ = socket.write_all(b"0\r\n\r\n").await;
}
fn text_delta(text: &str) -> Value {
    json!({"choices":[{"index":0,"delta":{"content":text}}]})
}
fn stop() -> Value {
    json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})
}
async fn collect(
    mut stream: pipecat_services::LlmStream,
) -> Vec<Result<StreamEvent, ServiceError>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut out = vec![];
        while let Some(v) = stream.next().await {
            out.push(v);
        }
        out
    })
    .await
    .unwrap()
}
async fn exchange(
    req: CompletionRequest,
) -> (String, Value, Vec<Result<StreamEvent, ServiceError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = LlmClient::new(config(&format!(
        "http://{}/explicit/v3/chat/completions",
        listener.local_addr().unwrap()
    )))
    .unwrap();
    let peer = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        let captured = capture(&mut s).await;
        response(&mut s, &[text_delta("हैलो🙂"), stop()], true, true).await;
        captured
    });
    let events = collect(client.stream(req).unwrap()).await;
    let (headers, body) = peer.await.unwrap();
    (headers, body, events)
}

#[tokio::test]
async fn all_five_models_execute_and_explicit_route_is_preserved() {
    for model in MODELS {
        let mut req = request(model);
        req.context.tools = Some(vec![tool()]);
        req.context.tool_choice = Some(ToolChoice::Required);
        if model != "sarvam-105b-conversations" {
            req.settings
                .base
                .extra
                .insert("reasoning_effort".into(), json!("low"));
        }
        if ["gemma4", "sarvam-105b"].contains(&model) {
            req.settings
                .base
                .extra
                .insert("wiki_grounding".into(), json!(false));
        }
        let before = serde_json::to_value(&req).unwrap();
        let (head, body, events) = exchange(req.clone()).await;
        assert!(head.starts_with("POST /explicit/v3/chat/completions HTTP/1.1\r\n"));
        assert_eq!(body["model"], model);
        assert_eq!(body["messages"][0]["role"], "user");
        assert_eq!(body["tools"][0]["function"]["name"], "lookup");
        assert_eq!(serde_json::to_value(req).unwrap(), before);
        assert!(events.iter().all(Result::is_ok), "{model}: {events:?}");
        assert!(events
            .iter()
            .any(|e| matches!(e,Ok(StreamEvent::TextDelta(s)) if s=="हैलो🙂")));
        assert!(matches!(
            events.last(),
            Some(Ok(StreamEvent::Finished { .. }))
        ));
    }
}

#[tokio::test]
async fn dual_auth_truthful_user_agent_case_insensitive_overrides_and_openai_isolation() {
    for sarvam in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/headers", listener.local_addr().unwrap());
        let mut c = if sarvam {
            config(&url)
        } else {
            let mut c = ClientConfig::new(Provider::OpenAi, "synthetic-sarvam-review-key");
            c.endpoint = url;
            c
        };
        if sarvam {
            c.sarvam_default_headers
                .insert("uSeR-aGeNt".into(), "wrong-language-agent".into());
            c.sarvam_default_headers
                .insert("API-SUBSCRIPTION-KEY".into(), "wrong-key".into());
            c.sarvam_default_headers
                .insert("aUtHoRiZaTiOn".into(), "Bearer wrong-key".into());
            c.sarvam_default_headers
                .insert("X-Review".into(), "kept".into());
        }
        let peer = tokio::spawn(async move {
            let (mut s, _) = listener.accept().await.unwrap();
            let head = capture(&mut s).await.0;
            response(&mut s, &[stop()], true, false).await;
            head
        });
        let result = collect(
            LlmClient::new(c)
                .unwrap()
                .stream(request("sarvam-105b"))
                .unwrap(),
        )
        .await;
        assert!(result.iter().all(Result::is_ok));
        let head = peer.await.unwrap().to_ascii_lowercase();
        assert_eq!(
            head.lines()
                .filter(|l| l.starts_with("authorization:"))
                .count(),
            1
        );
        assert!(head.contains("authorization: bearer synthetic-sarvam-review-key\r\n"));
        if sarvam {
            assert_eq!(
                head.lines()
                    .filter(|l| l.starts_with("api-subscription-key:"))
                    .count(),
                1
            );
            assert!(head.contains("api-subscription-key: synthetic-sarvam-review-key\r\n"));
            let ua = head.lines().find(|l| l.starts_with("user-agent:")).unwrap();
            assert!(ua.contains("pipecat/") && ua.contains("rust"));
            assert!(!ua.contains("python") && !ua.contains("wrong"));
            assert!(head.contains("x-review: kept\r\n"));
        } else {
            assert!(!head.contains("api-subscription-key:"));
        }
    }
}

#[tokio::test]
async fn all_capability_and_tool_failures_are_rejected_before_socket_admission() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = LlmClient::new(config(&format!(
        "http://{}/never",
        listener.local_addr().unwrap()
    )))
    .unwrap();
    let mut invalid = vec![request("unknown"), request(""), request("GEMMA4")];
    for model in MODELS {
        if model != "gemma4" {
            for part in [
                ContentPart::ImageUrl {
                    url: "https://image.invalid/a.png".into(),
                },
                ContentPart::ImageUrlDetailed {
                    url: "data:image/png;base64,YQ==".into(),
                    detail: "high".into(),
                },
                ContentPart::InlineData {
                    mime_type: "image/png".into(),
                    data: "YQ==".into(),
                },
            ] {
                let mut r = request(model);
                r.context.messages[1].content = Some(Content::Parts(vec![part]));
                invalid.push(r);
            }
        }
        if !["gemma4", "sarvam-105b"].contains(&model) {
            for value in [json!(true), json!(false)] {
                let mut r = request(model);
                r.settings.base.extra.insert("wiki_grounding".into(), value);
                invalid.push(r);
            }
        }
        for choice in [
            ToolChoice::Auto,
            ToolChoice::None,
            ToolChoice::Required,
            ToolChoice::Function("lookup".into()),
        ] {
            for tools in [None, Some(vec![])] {
                let mut r = request(model);
                r.context.tools = tools;
                r.context.tool_choice = Some(choice.clone());
                invalid.push(r);
            }
        }
    }
    for value in [json!("low"), json!("medium"), json!("high")] {
        let mut r = request("sarvam-105b-conversations");
        r.extra
            .insert("extra_body".into(), json!({"reasoning_effort":value}));
        invalid.push(r);
    }
    for (key, value) in [
        ("reasoning_effort", json!("xhigh")),
        ("reasoning_effort", json!(false)),
        ("wiki_grounding", json!("false")),
    ] {
        let mut r = request("gemma4");
        r.extra.insert(key.into(), value);
        invalid.push(r);
    }
    for key in [
        "model",
        "messages",
        "tools",
        "tool_choice",
        "stream",
        "stream_options",
        "n",
    ] {
        let mut r = request("gemma4");
        r.extra.insert("extra_body".into(), json!({key:null}));
        invalid.push(r);
    }
    for r in invalid {
        assert!(
            client.stream(r.clone()).is_err(),
            "invalid request escaped: {r:?}"
        );
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn gemma_image_variants_null_clears_and_final_extra_precedence_reach_the_wire() {
    for part in [
        ContentPart::ImageUrl {
            url: "https://image.invalid/a.png".into(),
        },
        ContentPart::ImageUrlDetailed {
            url: "data:image/png;base64,YQ==".into(),
            detail: "high".into(),
        },
        ContentPart::InlineData {
            mime_type: "image/png".into(),
            data: "YQ==".into(),
        },
    ] {
        let mut r = request("sarvam-105b");
        r.settings.base.model = Setting::Value("gemma4".into());
        r.context.messages[1].content = Some(Content::Parts(vec![part]));
        r.settings
            .base
            .extra
            .insert("wiki_grounding".into(), json!(true));
        r.settings
            .base
            .extra
            .insert("reasoning_effort".into(), json!("high"));
        r.extra.insert(
            "extra_body".into(),
            json!({"wiki_grounding":false,"reasoning_effort":"low","custom_marker":"retained"}),
        );
        let (_, body, events) = exchange(r).await;
        assert!(events.iter().all(Result::is_ok));
        assert_eq!(body["model"], "gemma4");
        assert_eq!(body["messages"][1]["content"][0]["type"], "image_url");
        assert_eq!(body["wiki_grounding"], false);
        assert_eq!(body["reasoning_effort"], "low");
        assert_eq!(body["custom_marker"], "retained");
        assert!(body.get("extra_body").is_none());
    }
    let mut r = request("sarvam-105b-conversations");
    r.settings
        .base
        .extra
        .insert("wiki_grounding".into(), Value::Null);
    r.settings
        .base
        .extra
        .insert("reasoning_effort".into(), Value::Null);
    let (_, body, events) = exchange(r).await;
    assert!(events.iter().all(Result::is_ok));
    assert!(body.get("wiki_grounding").is_none());
    assert!(body.get("reasoning_effort").is_none());
}

#[test]
fn source_budget_and_portable_budget_are_distinct_and_typed_precedence_is_preserved() {
    let mut p = preset();
    p.settings.reasoning_effort = Setting::Value(ReasoningEffort::High);
    let mut r = request("sarvam-105b");
    r.max_tokens = Some(33);
    r.extra.insert("reasoning_effort".into(), json!("low"));
    let body = p.request_body(&r).unwrap();
    assert!(body.get("max_completion_tokens").is_none());
    assert!(body.get("max_tokens").is_none());
    assert_eq!(body["reasoning_effort"], "high");
    r.settings.max_tokens = Setting::Value(77);
    r.settings.seed = Setting::Value(0);
    r.settings.temperature = Setting::Value(0.5);
    r.extra.insert("service_tier".into(), json!("auto"));
    r.extra.insert(
        "extra_body".into(),
        json!({"max_completion_tokens":99,"service_tier":"priority"}),
    );
    let body = p.request_body(&r).unwrap();
    assert_eq!(body["max_tokens"], 77);
    assert_eq!(body["seed"], 0);
    assert_eq!(body["temperature"], 0.5);
    for name in ["max_completion_tokens", "service_tier", "stream_options"] {
        assert!(body.get(name).is_none());
    }
    r.settings.max_tokens = Setting::Null;
    assert!(p.request_body(&r).unwrap()["max_tokens"].is_null());
}

#[tokio::test]
async fn sarvam_reuses_generic_utf8_tool_usage_and_literal_thought_contract() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = LlmClient::new(config(&format!(
        "http://{}/sse",
        listener.local_addr().unwrap()
    )))
    .unwrap();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        capture(&mut s).await;
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
        let prefix = wire(&[text_delta("<think>literal🙂</think>")], false);
        for b in prefix {
            s.write_all(&[b'1', b'\r', b'\n', b, b'\r', b'\n'])
                .await
                .unwrap();
        }
        release_rx.await.unwrap();
        let records = [
            json!({"choices":[{"delta":{"reasoning_content":"generic structured extension","tool_calls":[{"index":0,"id":"call-a","function":{"name":"lookup","arguments":"{\"city\":"}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"दिल्ली\"}"}}]},"finish_reason":"tool_calls"}]}),
            json!({"choices":[],"usage":{"prompt_tokens":4,"completion_tokens":3,"total_tokens":7}}),
        ];
        let tail = wire(&records, true);
        s.write_all(format!("{:x}\r\n", tail.len()).as_bytes())
            .await
            .unwrap();
        s.write_all(&tail).await.unwrap();
        s.write_all(b"\r\n0\r\n\r\n").await.unwrap();
    });
    let mut stream = client.stream(request("sarvam-105b")).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), StreamEvent::Started);
    let first = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        first,
        StreamEvent::TextDelta("<think>literal🙂</think>".into())
    );
    assert!(!peer.is_finished());
    release_tx.send(()).unwrap();
    let tail = collect(stream).await;
    assert!(tail.iter().all(Result::is_ok), "{tail:?}");
    assert_eq!(
        tail.iter()
            .filter(|e| matches!(e, Ok(StreamEvent::Usage(_))))
            .count(),
        1
    );
    assert!(tail.iter().any(|e|matches!(e,Ok(StreamEvent::Usage(u))if(u.prompt_tokens,u.completion_tokens,u.total_tokens)==(4,3,7))));
    assert!(tail.iter().any(|e|matches!(e,Ok(StreamEvent::ToolCall(c))if c.id=="call-a"&&c.name=="lookup"&&c.arguments==json!({"city":"दिल्ली"}))));
    assert!(!tail
        .iter()
        .any(|e| matches!(e, Ok(StreamEvent::ThoughtStart | StreamEvent::ThoughtEnd))));
    assert!(tail
        .iter()
        .any(|e| matches!(e,Ok(StreamEvent::ThoughtDelta(t))if t=="generic structured extension")));
    peer.await.unwrap();
}

#[tokio::test]
async fn atomic_invalid_updates_and_admitted_inference_snapshots_survive_model_switches() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let c = config(&format!(
        "http://{}/inference",
        listener.local_addr().unwrap()
    ));
    let mut initial = request("gemma4");
    initial
        .settings
        .base
        .extra
        .insert("wiki_grounding".into(), json!(true));
    let processor = Arc::new(LlmProcessor::new(LlmClient::new(c).unwrap(), initial));
    let before = serde_json::to_value(processor.request_snapshot().unwrap()).unwrap();
    let mut bad = LlmSettings::default();
    bad.base.model = Setting::Value("glm5.2".into());
    bad.temperature = Setting::Value(0.9);
    assert!(processor.update_settings(&bad).is_err());
    assert_eq!(
        serde_json::to_value(processor.request_snapshot().unwrap()).unwrap(),
        before
    );
    let (captured_tx, captured_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let first_body = capture(&mut first).await.1;
        captured_tx.send(first_body).unwrap();
        release_rx.await.unwrap();
        response(&mut first, &[text_delta("first"), stop()], true, false).await;
        let (mut second, _) = listener.accept().await.unwrap();
        let second_body = capture(&mut second).await.1;
        response(&mut second, &[text_delta("second"), stop()], true, false).await;
        second_body
    });
    let context = LlmContextData {
        messages: vec![ChatMessage::new(ChatRole::User, "explicit inference context").into()],
        tools: pipecat_core::ToolSet::Functions(vec![]),
        tool_choice: None,
    };
    let mut options = InferenceOptions::default();
    options.settings.max_tokens = Setting::Value(123);
    options.settings.system_instruction = Setting::Value("inference system".into());
    let running = {
        let processor = processor.clone();
        let context = context.clone();
        let options = options.clone();
        tokio::spawn(async move { processor.run_inference(context, options).await })
    };
    let first = tokio::time::timeout(Duration::from_secs(3), captured_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first["model"], "gemma4");
    assert_eq!(first["wiki_grounding"], true);
    assert_eq!(first["max_tokens"], 123);
    assert_eq!(first["messages"][0]["content"], "inference system");
    let mut next = LlmSettings::default();
    next.base.model = Setting::Value("sarvam-105b-conversations".into());
    next.base.extra.insert("wiki_grounding".into(), Value::Null);
    next.base
        .extra
        .insert("reasoning_effort".into(), Value::Null);
    processor.update_settings(&next).unwrap();
    release_tx.send(()).unwrap();
    assert_eq!(running.await.unwrap().unwrap(), Some("first".into()));
    assert_eq!(
        processor.run_inference(context, options).await.unwrap(),
        Some("second".into())
    );
    let second = peer.await.unwrap();
    assert_eq!(second["model"], "sarvam-105b-conversations");
    assert!(second.get("wiki_grounding").is_none());
    assert!(second.get("reasoning_effort").is_none());
    assert_eq!(first["wiki_grounding"], true);
}

#[tokio::test]
async fn truncated_sse_reports_error_before_finish_and_does_not_admit_tools() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = LlmClient::new(config(&format!(
        "http://{}/truncated",
        listener.local_addr().unwrap()
    )))
    .unwrap();
    let peer = tokio::spawn(async move {
        let (mut s, _) = listener.accept().await.unwrap();
        capture(&mut s).await;
        response(&mut s,&[json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"partial","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":"tool_calls"}]})],false,false).await;
    });
    let events = collect(client.stream(request("gemma4")).unwrap()).await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Protocol(_)))));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Ok(StreamEvent::ToolCall(_)))));
    assert_eq!(events.iter().filter(|event| event.is_err()).count(), 1);
    assert!(matches!(
        events.last(),
        Some(Ok(StreamEvent::Finished { .. }))
    ));
    assert!(matches!(
        events[events.len() - 2],
        Err(ServiceError::Protocol(_))
    ));
    peer.await.unwrap();
}

#[tokio::test]
async fn resolved_runtime_model_selects_v1_or_v2_on_actual_http_path() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/{{version}}/chat/completions",
        listener.local_addr().unwrap()
    );
    let client = LlmClient::new(config(&endpoint)).unwrap();
    let peer = tokio::spawn(async move {
        let mut seen = Vec::new();
        for _ in MODELS {
            let (mut s, _) = listener.accept().await.unwrap();
            let captured = capture(&mut s).await;
            seen.push(captured);
            response(&mut s, &[stop()], true, false).await;
        }
        seen
    });
    for model in MODELS {
        let mut r = request(if model == "sarvam-105b-conversations" {
            "gemma4"
        } else {
            "sarvam-105b-conversations"
        });
        r.settings.base.model = Setting::Value(model.into());
        let events = collect(client.stream(r).unwrap()).await;
        assert!(events.iter().all(Result::is_ok));
    }
    for ((head, body), model) in peer.await.unwrap().into_iter().zip(MODELS) {
        let version = if model == "sarvam-105b-conversations" {
            "v1"
        } else {
            "v2"
        };
        assert!(
            head.starts_with(&format!("POST /{version}/chat/completions HTTP/1.1\r\n")),
            "{model}: {head}"
        );
        assert_eq!(body["model"], model);
    }
    assert_eq!(
        ClientConfig::openai_compatible(preset(), "synthetic")
            .unwrap()
            .endpoint,
        "https://api.sarvam.ai/{version}/chat/completions"
    );
}

#[tokio::test]
async fn invalid_keys_and_custom_headers_are_rejected_without_network_or_secret_diagnostics() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/never", listener.local_addr().unwrap());
    for key in [
        None,
        Some(""),
        Some("   "),
        Some("sentinel-secret\r\nbad-header: yes"),
    ] {
        let mut c = config(&endpoint);
        c.api_key = key.map(str::to_owned);
        let error = LlmClient::new(c).unwrap_err().to_string();
        assert!(!error.contains("sentinel-secret"));
    }
    for (name, value) in [
        ("Host", "invalid.example"),
        ("Content-Length", "0"),
        ("bad header", "x"),
        ("X-Review", "sentinel-secret\r\nbad: value"),
    ] {
        let mut c = config(&endpoint);
        c.sarvam_default_headers.insert(name.into(), value.into());
        let error = LlmClient::new(c).unwrap_err().to_string();
        assert!(!error.contains("sentinel-secret"));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
}

#[test]
fn version_placeholder_cannot_move_into_authority_query_fragment_or_repeat() {
    for endpoint in [
        "https://{version}/chat/completions",
        "https://api.sarvam.ai?x=/{version}/chat/completions",
        "http://127.0.0.1:12345?x=/{version}/chat/completions",
        "https://api.sarvam.ai/#/{version}/chat/completions",
        "https://api.sarvam.ai/{version}/{version}/chat/completions",
        "https://api.sarvam.ai/{model}/chat/completions",
        "https://api.sarvam.ai/{version}/chat/completions?x=1",
    ] {
        let client = LlmClient::new(config(endpoint)).unwrap();
        assert!(
            client.request_body(&request("gemma4")).is_err(),
            "invalid endpoint template escaped: {endpoint}"
        );
    }
}
