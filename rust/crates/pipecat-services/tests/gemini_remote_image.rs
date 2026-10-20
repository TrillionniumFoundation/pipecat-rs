//! Bounded SDK URI inference contracts; no real media download or model calls.
use pipecat_services::{
    adapters::{request_body, request_body_for_logging},
    *,
};
use serde_json::{json, Value};

fn request(parts: Vec<ContentPart>) -> CompletionRequest {
    let mut message = Message::text(Role::User, "");
    message.content = Some(Content::Parts(parts));
    CompletionRequest::new("gemini-3.8-flash", Context::new(vec![message]))
}
fn image(url: &str, detail: Option<&str>) -> ContentPart {
    if let Some(detail) = detail {
        ContentPart::ImageUrlDetailed {
            url: url.into(),
            detail: detail.into(),
        }
    } else {
        ContentPart::ImageUrl { url: url.into() }
    }
}
fn wire(request: &CompletionRequest) -> Value {
    let before = serde_json::to_value(request).unwrap();
    let body = request_body(Provider::Gemini, request).unwrap();
    assert_eq!(request_body(Provider::Gemini, request).unwrap(), body);
    assert_eq!(serde_json::to_value(request).unwrap(), before);
    body
}
#[test]
fn literal_remote_image_suffixes_preserve_exact_uri_and_order() {
    for (url, mime) in [
        ("https://example.com/photo.jpg", "image/jpeg"),
        ("http://example.com/photo.jpeg", "image/jpeg"),
        ("https://example.com/photo.jpe", "image/jpeg"),
        ("https://example.com/photo.png", "image/png"),
        ("https://example.com/photo.gif", "image/gif"),
        ("https://example.com/photo.JpEg", "image/jpeg"),
        ("HTTPS://EXAMPLE.com:443/photo.PNG", "image/png"),
        ("https://example.com/photo%20one.jpg", "image/jpeg"),
    ] {
        for detailed in [None, Some("auto"), Some("low"), Some("high")] {
            let body = wire(&request(vec![
                ContentPart::Text {
                    text: "Describe".into(),
                },
                image(url, detailed),
            ]));
            assert_eq!(
                body["contents"],
                json!([{"role":"user","parts":[{"text":"Describe"},{"fileData":{"fileUri":url,"mimeType":mime}}]}])
            );
        }
    }
}
#[test]
fn unsupported_remote_boundaries_are_not_normalized_or_leaked() {
    for url in [
        "https://example.com/photo.png;name.jpg",
        "https://example.com/photo;name.jpg",
        "https://example.com/photo.webp",
        "https://example.com/photo.jpg.gz",
        "https://example.com/photo.%6apg",
        "https://example.com/photo",
        "https://example.com/photo.svg",
        "https://example.com/photo.jpg?token=secret",
        "https://example.com/photo.jpg#secret",
        "https://example.com/photo.jpg?",
        "https://example.com/photo.jpg#",
        "https://example.com/no-image?file=photo.jpg",
        "https://example.jpg",
        "https://example.jpg/",
        "https://example.com/.jpg",
        "https://example.com/...jpg",
        "https://example.com/photo.jpg/",
        "files/opaque",
        "https://generativelanguage.googleapis.com/v1beta/files/opaque",
        "gs://bucket/photo.jpg",
        "file:///tmp/photo.jpg",
        "ftp://example.com/photo.jpg",
        "/photo.jpg",
        "//example.com/photo.jpg",
        "https:/example.com/photo.jpg",
        "https:///example.com/photo.jpg",
        "https://user:secret@example.com/photo.jpg",
        "https://@example.com/photo.jpg",
        "https://user@example.com/photo.jpg",
        "https://example.com\\photo.jpg",
        " https://example.com/photo.jpg",
        "https://example.com/photo one.jpg",
        "https://example.com/photo\t.jpg",
        "https://example.com/photo\n.jpg",
        "https://example.com/photo\r.jpg",
        "https://example.com/photo\0.jpg",
        "https://example.com/photo\u{7f}.jpg",
        "https://[broken/photo.jpg",
        "https://example.com:bad/photo.jpg",
    ] {
        for detailed in [None, Some("auto"), Some("low"), Some("high")] {
            let request = request(vec![image(url, detailed)]);
            let before = serde_json::to_value(&request).unwrap();
            let error = request_body(Provider::Gemini, &request).unwrap_err();
            assert!(
                matches!(error, ServiceError::Unsupported(_)),
                "{url:?}: {error}"
            );
            assert!(!error.to_string().contains(url));
            assert_eq!(serde_json::to_value(&request).unwrap(), before);
            assert!(request_body_for_logging(Provider::Gemini, &request).is_err());
        }
    }
}
#[test]
fn invalid_detail_remains_unsupported_and_data_urls_remain_inline() {
    for detail in ["", "HIGH", "unexpected"] {
        assert!(matches!(
            request_body(
                Provider::Gemini,
                &request(vec![ContentPart::ImageUrlDetailed {
                    url: "https://example.com/photo.jpg".into(),
                    detail: detail.into()
                }])
            ),
            Err(ServiceError::Unsupported(_))
        ));
    }
    for detailed in [None, Some("auto"), Some("low"), Some("high")] {
        let body = wire(&request(vec![image(
            "data:image/png;base64,cGF5bG9hZA==",
            detailed,
        )]));
        assert_eq!(
            body["contents"][0]["parts"],
            json!([{"inlineData":{"mimeType":"image/png","data":"cGF5bG9hZA=="}}])
        );
        for malformed in [
            "data:image/png;base64",
            "data:image/png;base64,",
            "data:image/png,payload",
        ] {
            assert!(
                request_body(Provider::Gemini, &request(vec![image(malformed, detailed)])).is_err()
            );
        }
    }
}
#[test]
fn media_with_service_system_seed_keeps_distinct_ordered_content() {
    let mut request = request(vec![
        ContentPart::Text {
            text: "Describe".into(),
        },
        image("https://example.com/photo.jpg", None),
    ]);
    request.system_instruction = Some("Be precise".into());
    let body = wire(&request);
    assert_eq!(
        body["contents"],
        json!([
            {"role":"user","parts":[{"text":"Describe"},{"fileData":{"fileUri":"https://example.com/photo.jpg","mimeType":"image/jpeg"}}]},
            {"role":"user","parts":[{"text":"Be precise"}]}
        ])
    );
    assert_eq!(
        body["systemInstruction"],
        json!({"parts":[{"text":"Be precise"}]})
    );
}
#[test]
fn diagnostics_preserve_public_uri_but_redact_payload_and_signature_without_mutation() {
    let mut request = request(vec![
        image("https://example.com/photo.jpg", None),
        image("data:image/png;base64,cGF5bG9hZA==", None),
        ContentPart::GeminiText {
            text: "visible".into(),
            thought: true,
            signature: Some("private-signature".into()),
            incomplete: false,
        },
    ]);
    request.context.messages[0].role = Role::Assistant;
    let before = serde_json::to_value(&request).unwrap();
    let body = wire(&request);
    let logged = request_body_for_logging(Provider::Gemini, &request).unwrap();
    assert_eq!(
        logged["contents"][0]["parts"][0],
        body["contents"][0]["parts"][0]
    );
    assert!(!logged.to_string().contains("cGF5bG9hZA=="));
    assert!(!logged.to_string().contains("private-signature"));
    assert!(body.to_string().contains("private-signature"));
    assert_eq!(serde_json::to_value(&request).unwrap(), before);
}
#[test]
fn other_providers_keep_their_existing_remote_image_shapes() {
    let request = request(vec![image("https://example.com/photo.jpg", None)]);
    assert_eq!(
        request_body(Provider::OpenAi, &request).unwrap()["messages"][0]["content"],
        json!([{"type":"image_url","image_url":{"url":"https://example.com/photo.jpg"}}])
    );
    assert_eq!(
        request_body(Provider::Anthropic, &request).unwrap()["messages"][0]["content"],
        json!([{"type":"image","source":{"type":"url","url":"https://example.com/photo.jpg"}}])
    );
}
