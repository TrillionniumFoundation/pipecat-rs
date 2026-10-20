//! Valid universal image detail is OpenAI-specific; no live provider calls.
use pipecat_services::{adapters::request_body, *};
use serde_json::{json, Value};

fn request(url: &str, detail: Option<&str>) -> CompletionRequest {
    let mut message = Message::text(Role::User, "");
    message.content = Some(Content::Parts(vec![
        ContentPart::Text {
            text: "Describe".into(),
        },
        match detail {
            Some(detail) => ContentPart::ImageUrlDetailed {
                url: url.into(),
                detail: detail.into(),
            },
            None => ContentPart::ImageUrl { url: url.into() },
        },
    ]));
    CompletionRequest::new("synthetic-model", Context::new(vec![message]))
}
fn body(provider: Provider, request: &CompletionRequest) -> Value {
    let before = serde_json::to_value(request).unwrap();
    let body = request_body(provider, request).unwrap();
    assert_eq!(request_body(provider, request).unwrap(), body);
    assert_eq!(serde_json::to_value(request).unwrap(), before);
    body
}
#[test]
fn valid_details_reuse_native_media_conversion_and_preserve_context_and_order() {
    for (url, gemini, anthropic) in [
        (
            "data:image/png;base64,YQ==",
            json!({"inlineData":{"mimeType":"image/png","data":"YQ=="}}),
            json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"YQ=="}}),
        ),
        (
            "https://example.com/photo.jpg",
            json!({"fileData":{"fileUri":"https://example.com/photo.jpg","mimeType":"image/jpeg"}}),
            json!({"type":"image","source":{"type":"url","url":"https://example.com/photo.jpg"}}),
        ),
    ] {
        for detail in ["auto", "low", "high"] {
            for provider in [Provider::Gemini, Provider::Anthropic] {
                let result = body(provider, &request(url, Some(detail)));
                assert_eq!(result, body(provider, &request(url, None)));
                match provider {
                    Provider::Gemini => assert_eq!(
                        result["contents"][0]["parts"],
                        json!([{"text":"Describe"},gemini])
                    ),
                    Provider::Anthropic => assert_eq!(
                        result["messages"][0]["content"],
                        json!([anthropic,{"type":"text","text":"Describe"}])
                    ),
                    _ => unreachable!(),
                }
            }
            assert_eq!(
                body(Provider::OpenAi, &request(url, Some(detail)))["messages"][0]["content"][1],
                json!({"type":"image_url","image_url":{"url":url,"detail":detail}})
            );
        }
    }
}
#[test]
fn invalid_detail_rejects_for_all_providers_without_context_mutation() {
    for provider in [Provider::Gemini, Provider::Anthropic, Provider::OpenAi] {
        for detail in ["", "HIGH", "unexpected"] {
            for url in [
                "https://example.com/photo.jpg",
                "data:image/png;base64,YQ==",
            ] {
                let request = request(url, Some(detail));
                let before = serde_json::to_value(&request).unwrap();
                let error = request_body(provider, &request).unwrap_err();
                if provider == Provider::OpenAi {
                    assert!(matches!(error, ServiceError::InvalidRequest(_)));
                } else {
                    assert!(matches!(error, ServiceError::Unsupported(_)));
                }
                assert_eq!(serde_json::to_value(&request).unwrap(), before);
            }
        }
    }
}
#[test]
fn details_do_not_broaden_data_or_anthropic_scheme_and_mime_boundaries() {
    for provider in [Provider::Gemini, Provider::Anthropic] {
        for url in [
            "data:image/png;base64",
            "data:image/png;base64,",
            "data:image/png,payload",
            "file:///tmp/photo.jpg",
            "gs://bucket/photo.jpg",
        ] {
            let baseline = request_body(provider, &request(url, None)).unwrap_err();
            for detail in ["auto", "low", "high"] {
                let detailed = request(url, Some(detail));
                let before = serde_json::to_value(&detailed).unwrap();
                assert_eq!(
                    request_body(provider, &detailed).unwrap_err().to_string(),
                    baseline.to_string()
                );
                assert_eq!(serde_json::to_value(&detailed).unwrap(), before);
            }
        }
    }
    for detail in ["auto", "low", "high"] {
        assert!(request_body(
            Provider::Anthropic,
            &request("data:application/octet-stream;base64,YQ==", Some(detail))
        )
        .is_err());
        // Anthropic is intentionally not tightened to Gemini's suffix/query policy.
        let url = "https://example.com/opaque?token=synthetic#fragment";
        assert_eq!(
            body(Provider::Anthropic, &request(url, Some(detail))),
            body(Provider::Anthropic, &request(url, None))
        );
    }
}

#[test]
fn normalization_preserves_adjacent_signed_and_tool_history() {
    for provider in [Provider::Gemini, Provider::Anthropic] {
        let mut history = Message::assistant_tool_calls(vec![ToolCall {
            id: "call-one".into(),
            name: "echo".into(),
            arguments: json!({"value":1}),
        }]);
        let mut signed_text = Message::text(Role::Assistant, "");
        signed_text.content = Some(Content::Parts(vec![match provider {
            Provider::Gemini => ContentPart::GeminiText {
                text: "thought".into(),
                thought: true,
                signature: Some("text-signature".into()),
                incomplete: false,
            },
            Provider::Anthropic => ContentPart::Thinking {
                text: "thought".into(),
                signature: Some("text-signature".into()),
            },
            _ => unreachable!(),
        }]));
        if provider == Provider::Gemini {
            history.content = Some(Content::Parts(vec![ContentPart::GeminiToolMetadata {
                tool_call_id: "call-one".into(),
                signature: Some("tool-signature".into()),
            }]));
        }
        for detail in ["auto", "low", "high"] {
            let mut detailed = request("https://example.com/photo.jpg", Some(detail));
            detailed.context.messages.extend([
                signed_text.clone(),
                history.clone(),
                Message::tool_result("call-one", "done"),
            ]);
            let mut undetailed = request("https://example.com/photo.jpg", None);
            undetailed.context.messages.extend([
                signed_text.clone(),
                history.clone(),
                Message::tool_result("call-one", "done"),
            ]);
            let wire = body(provider, &detailed);
            assert_eq!(wire, body(provider, &undetailed));
            assert!(wire.to_string().contains("text-signature"));
            assert!(wire.to_string().contains("call-one"));
            assert!(wire.to_string().contains("done"));
            if provider == Provider::Gemini {
                assert!(wire.to_string().contains("tool-signature"));
            }
        }
    }
}
