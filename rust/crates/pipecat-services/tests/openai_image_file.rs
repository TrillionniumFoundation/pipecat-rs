//! Wire-only parity with pinned open_ai_adapter.py::_inline_file_item.
//! Native File identity and non-image acceptance are deliberately unchanged.
use pipecat_services::*;
use serde_json::json;

fn request(parts: Vec<ContentPart>) -> CompletionRequest {
    let mut message = Message::text(Role::User, "");
    message.content = Some(Content::Parts(parts));
    CompletionRequest::new("synthetic-model", Context::new(vec![message]))
}

fn file(mime: &str, data: &str, filename: Option<&str>) -> ContentPart {
    ContentPart::File {
        mime_type: mime.into(),
        data: data.into(),
        filename: filename.map(str::to_owned),
    }
}

#[test]
fn image_files_use_exact_data_urls_without_filename_or_native_mutation() {
    for (mime, data, filename) in [
        ("image/png", "YQ==", Some("original.png")),
        ("image/jpeg", "/9j/+w==", None),
        ("image/PNG", "opaque+/=", Some("misleading.pdf")),
        ("image/", "", Some("")),
        ("image/svg+xml; charset=UTF-8", "PHN2Zz4=", None),
    ] {
        let request = request(vec![
            ContentPart::Text {
                text: "before".into(),
            },
            file(mime, data, filename),
            ContentPart::Text {
                text: "after".into(),
            },
        ]);
        let before = serde_json::to_value(&request).unwrap();
        let first_body = adapters::request_body(Provider::OpenAi, &request).unwrap();
        assert_eq!(
            first_body,
            json!({"model":"synthetic-model","stream":true,"stream_options":{"include_usage":true},
            "messages":[{"role":"user","content":[
                {"type":"text","text":"before"},
                {"type":"image_url","image_url":{"url":format!("data:{mime};base64,{data}")}},
                {"type":"text","text":"after"}
            ]}]}),
            "{mime}"
        );
        assert_eq!(
            adapters::request_body(Provider::OpenAi, &request).unwrap(),
            first_body
        );
        assert_eq!(serde_json::to_value(&request).unwrap(), before);
    }
}

#[test]
fn nonimage_files_keep_existing_acceptance_and_optional_filename() {
    for mime in [
        "application/pdf",
        "text/plain",
        "IMAGE/png",
        " image/png",
        "image",
        "",
    ] {
        for filename in [Some("original.pdf"), None] {
            let request = request(vec![file(mime, "YQ==", filename)]);
            let before = request.context.clone();
            let mut expected = json!({"file_data":format!("data:{mime};base64,YQ==")});
            if let Some(name) = filename {
                expected["filename"] = json!(name);
            }
            assert_eq!(
                adapters::request_body(Provider::OpenAi, &request).unwrap()["messages"][0]
                    ["content"],
                json!([{"type":"file","file":expected}]),
                "{mime}"
            );
            assert_eq!(request.context, before);
        }
    }
}

#[test]
fn existing_image_variants_and_other_provider_file_shapes_are_unchanged() {
    let request = request(vec![
        ContentPart::ImageUrl {
            url: "https://example.invalid/photo.png".into(),
        },
        ContentPart::InlineData {
            mime_type: "image/png".into(),
            data: "YQ==".into(),
        },
        ContentPart::ImageUrlDetailed {
            url: "data:image/jpeg;base64,Yg==".into(),
            detail: "low".into(),
        },
        file("image/jpeg", "Yw==", Some("photo.jpg")),
    ]);
    let before = request.context.clone();
    assert_eq!(
        adapters::request_body(Provider::OpenAi, &request).unwrap()["messages"][0]["content"],
        json!([
            {"type":"image_url","image_url":{"url":"https://example.invalid/photo.png"}},
            {"type":"image_url","image_url":{"url":"data:image/png;base64,YQ=="}},
            {"type":"image_url","image_url":{"url":"data:image/jpeg;base64,Yg==","detail":"low"}},
            {"type":"image_url","image_url":{"url":"data:image/jpeg;base64,Yw=="}}
        ])
    );
    assert_eq!(request.context, before);
    let request = self::request(vec![file("image/png", "YQ==", Some("photo.png"))]);
    let before = request.context.clone();
    assert_eq!(
        adapters::request_body(Provider::Anthropic, &request).unwrap()["messages"][0]["content"],
        json!([{"type":"image","source":{"type":"base64","media_type":"image/png","data":"YQ=="}}])
    );
    assert_eq!(
        adapters::request_body(Provider::Gemini, &request).unwrap()["contents"][0]["parts"],
        json!([{"inlineData":{"mimeType":"image/png","data":"YQ=="}}])
    );
    assert_eq!(request.context, before);
}

#[test]
fn diagnostics_redact_inline_image_urls_without_changing_wire_or_native_context() {
    let request = request(vec![
        file("image/png", "FILE_PAYLOAD", Some("photo.png")),
        ContentPart::InlineData {
            mime_type: "image/jpeg".into(),
            data: "INLINE_PAYLOAD".into(),
        },
        ContentPart::ImageUrl {
            url: "data:image/png;base64,URL_PAYLOAD".into(),
        },
        ContentPart::ImageUrlDetailed {
            url: "data:image/jpeg;base64,DETAIL_PAYLOAD".into(),
            detail: "high".into(),
        },
        ContentPart::ImageUrl {
            url: "https://example.invalid/photo.png".into(),
        },
        file("application/pdf", "PDF_PAYLOAD", Some("note.pdf")),
    ]);
    let native_before = serde_json::to_value(&request).unwrap();
    let wire_before = adapters::request_body(Provider::OpenAi, &request).unwrap();
    let logged = adapters::request_body_for_logging(Provider::OpenAi, &request).unwrap();
    assert_eq!(
        logged["messages"][0]["content"],
        json!([
            {"type":"image_url","image_url":{"url":"[redacted]"}},
            {"type":"image_url","image_url":{"url":"[redacted]"}},
            {"type":"image_url","image_url":{"url":"[redacted]"}},
            {"type":"image_url","image_url":{"url":"[redacted]","detail":"high"}},
            {"type":"image_url","image_url":{"url":"https://example.invalid/photo.png"}},
            {"type":"file","file":{"file_data":"[redacted]","filename":"note.pdf"}}
        ])
    );
    for payload in [
        "FILE_PAYLOAD",
        "INLINE_PAYLOAD",
        "URL_PAYLOAD",
        "DETAIL_PAYLOAD",
        "PDF_PAYLOAD",
    ] {
        assert!(!logged.to_string().contains(payload));
        assert!(wire_before.to_string().contains(payload));
    }
    assert_eq!(
        adapters::request_body(Provider::OpenAi, &request).unwrap(),
        wire_before
    );
    assert_eq!(serde_json::to_value(&request).unwrap(), native_before);
}
