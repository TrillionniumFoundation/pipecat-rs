//! Synthetic protocol goldens and regressions ported from Python test contracts.
//! No provider is contacted by these tests.
use pipecat_services::{
    adapters::request_body,
    protocol::ProtocolDecoder,
    sse::{SseDecoder, SseEvent},
    *,
};
use serde_json::{json, Value};

fn decode(d: &mut ProtocolDecoder, value: Value) -> Result<Vec<StreamEvent>, ServiceError> {
    d.decode(SseEvent {
        event: None,
        data: value.to_string(),
    })
}
fn done(d: &mut ProtocolDecoder) -> Result<Vec<StreamEvent>, ServiceError> {
    d.decode(SseEvent {
        event: None,
        data: "[DONE]".into(),
    })
}

#[test]
fn sse_every_byte_boundary_supports_utf8_bom_crlf_multiline_and_comments() {
    let data = "\u{feff}: keepalive\r\nevent: token\r\ndata: 你好🙂\r\ndata: second\r\n\r\n:comment\n\ndata: last\r\r";
    for split in 0..=data.len() {
        let mut decoder = SseDecoder::new(4096);
        let mut events = decoder.feed(&data.as_bytes()[..split]).unwrap();
        events.extend(decoder.feed(&data.as_bytes()[split..]).unwrap());
        assert_eq!(
            events,
            vec![
                SseEvent {
                    event: Some("token".into()),
                    data: "你好🙂\nsecond".into()
                },
                SseEvent {
                    event: None,
                    data: "last".into()
                }
            ],
            "split {split}"
        );
    }
    let mut decoder = SseDecoder::new(4096);
    let mut events = vec![];
    for byte in data.bytes() {
        events.extend(decoder.feed(&[byte]).unwrap());
    }
    assert_eq!(events.len(), 2);
}
#[test]
fn sse_negative_invalid_utf8_size_and_partial_eof() {
    assert!(SseDecoder::new(40).feed(b"data: \xff\n\n").is_err());
    assert!(SseDecoder::new(4).feed(b"data: 1\n\n").is_err());
    let mut d = SseDecoder::new(30);
    assert!(d.feed(b"data: partial").unwrap().is_empty());
    assert!(d.has_pending_data());
}
#[test]
fn sse_ignores_id_retry_and_dispatches_empty_data() {
    let mut d = SseDecoder::new(1024);
    assert_eq!(
        d.feed(b"retry: 500\nid: 3\ndata:\n\n").unwrap(),
        vec![SseEvent {
            event: None,
            data: String::new()
        }]
    );
}
#[test]
fn openai_interleaved_tools_aggregate_by_index_and_usage_is_latest_snapshot() {
    let mut d = ProtocolDecoder::new(Provider::OpenAi);
    let first=decode(&mut d,json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"b","type":"function","function":{"name":"weather","arguments":"{\"city\":\""}},{"index":0,"id":"a","type":"function","function":{"name":"time","arguments":"{"}}]}}],"usage":{"prompt_tokens":20,"completion_tokens":5,"total_tokens":25}})).unwrap();
    assert_eq!(first.len(), 3);
    assert_eq!(first[0], StreamEvent::ResponseProgress);
    decode(&mut d,json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"}"}},{"index":1,"function":{"arguments":"北京\"}"}}]},"finish_reason":"tool_calls"}]})).unwrap();
    decode(&mut d,json!({"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":30,"total_tokens":50,"prompt_tokens_details":{"cached_tokens":12,"cache_write_tokens":8},"completion_tokens_details":{"reasoning_tokens":7}}})).unwrap();
    let calls = done(&mut d).unwrap();
    assert_eq!(
        calls,
        vec![
            StreamEvent::ToolCall(ToolCall {
                id: "a".into(),
                name: "time".into(),
                arguments: json!({})
            }),
            StreamEvent::ToolCall(ToolCall {
                id: "b".into(),
                name: "weather".into(),
                arguments: json!({"city":"北京"})
            })
        ]
    );
    let usage = d.usage().unwrap();
    assert_eq!(usage.total_tokens, 50);
    assert_eq!(usage.cache_read_input_tokens, Some(12));
    assert_eq!(usage.cache_creation_input_tokens, Some(8));
    assert_eq!(usage.reasoning_tokens, Some(7));
    assert_eq!(d.finish_reason(), Some(&FinishReason::ToolCalls));
    assert!(d.is_done());
}
#[test]
fn openai_negative_incomplete_json_duplicate_id_choices_and_truncation() {
    for arguments in ["{", "[]", "null"] {
        let mut d = ProtocolDecoder::new(Provider::OpenAi);
        decode(&mut d,json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"f","arguments":arguments}}]}}]})).unwrap();
        assert!(done(&mut d).is_err());
    }
    let mut d = ProtocolDecoder::new(Provider::OpenAi);
    decode(&mut d,json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"f","arguments":"{}"}},{"index":1,"id":"a","function":{"name":"g","arguments":"{}"}}]}}]})).unwrap();
    assert!(done(&mut d).is_err());
    let mut d = ProtocolDecoder::new(Provider::OpenAi);
    assert!(decode(
        &mut d,
        json!({"choices":[{"index":1,"delta":{"content":"bad"}}]})
    )
    .is_err());
    assert!(d.end_of_stream().is_err());
}
#[test]
fn openai_content_and_reasoning_are_distinct() {
    let mut d = ProtocolDecoder::new(Provider::OpenAi);
    assert_eq!(
        decode(
            &mut d,
            json!({"choices":[{"delta":{"content":"answer","reasoning_content":"thought"}}]})
        )
        .unwrap(),
        vec![
            StreamEvent::ResponseProgress,
            StreamEvent::TextDelta("answer".into()),
            StreamEvent::ThoughtDelta("thought".into())
        ]
    );
}
#[test]
fn anthropic_text_tools_and_cached_usage() {
    let mut d = ProtocolDecoder::new(Provider::Anthropic);
    decode(&mut d,json!({"type":"message_start","message":{"usage":{"input_tokens":10,"output_tokens":1,"cache_read_input_tokens":8,"cache_creation_input_tokens":2}}})).unwrap();
    decode(
        &mut d,
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    )
    .unwrap();
    assert_eq!(decode(&mut d,json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hi"}})).unwrap(),vec![StreamEvent::TextDelta("Hi".into())]);
    decode(&mut d, json!({"type":"content_block_stop","index":0})).unwrap();
    decode(&mut d,json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"call1","name":"f","input":{}}})).unwrap();
    decode(&mut d,json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"n\":"}})).unwrap();
    decode(&mut d,json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"42}"}})).unwrap();
    assert_eq!(
        decode(&mut d, json!({"type":"content_block_stop","index":1})).unwrap(),
        vec![StreamEvent::ToolCall(ToolCall {
            id: "call1".into(),
            name: "f".into(),
            arguments: json!({"n":42})
        })]
    );
    decode(&mut d,json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":30}})).unwrap();
    assert!(decode(&mut d, json!({"type":"message_stop"}))
        .unwrap()
        .is_empty());
    assert_eq!(d.usage().unwrap().prompt_tokens, 20);
    assert_eq!(d.usage().unwrap().total_tokens, 50);
}
#[test]
fn anthropic_errors_and_unclosed_blocks_are_not_success() {
    let mut d = ProtocolDecoder::new(Provider::Anthropic);
    assert!(
        matches!(decode(&mut d,json!({"type":"error","error":{"type":"overloaded_error","message":"busy"}})),Err(ServiceError::Provider{kind,..}) if kind=="overloaded_error")
    );
    assert!(decode(
        &mut d,
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}})
    )
    .is_err());
    let mut d = ProtocolDecoder::new(Provider::Anthropic);
    decode(
        &mut d,
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    )
    .unwrap();
    assert!(decode(&mut d, json!({"type":"message_stop"})).is_err());
}
#[test]
fn google_usage_matches_python_thoughts_and_tool_prompt_contracts() {
    // tests/test_google_token_usage.py: generated thoughts count as output.
    let mut d = ProtocolDecoder::new(Provider::Gemini);
    decode(&mut d,json!({"candidates":[{"content":{"parts":[{"text":"Hi!"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":40,"candidatesTokenCount":10,"thoughtsTokenCount":120,"totalTokenCount":170}})).unwrap();
    assert_eq!(d.usage().unwrap().completion_tokens, 130);
    assert_eq!(d.usage().unwrap().reasoning_tokens, Some(120));
    d.end_of_stream().unwrap();
    let mut d = ProtocolDecoder::new(Provider::Gemini);
    decode(&mut d,json!({"candidates":[],"usageMetadata":{"promptTokenCount":23,"candidatesTokenCount":57,"toolUsePromptTokenCount":79,"totalTokenCount":159}})).unwrap();
    assert_eq!(d.usage().unwrap().prompt_tokens, 102);
    assert_eq!(d.usage().unwrap().completion_tokens, 57);
    assert_eq!(d.usage().unwrap().total_tokens, 159);
}
#[test]
fn google_preserves_non_normal_finish_reasons_and_empty_turns() {
    for (reason, expected) in [
        ("STOP", FinishReason::Stop),
        ("MAX_TOKENS", FinishReason::Length),
        ("SAFETY", FinishReason::ContentFilter),
        (
            "MALFORMED_FUNCTION_CALL",
            FinishReason::Other("MALFORMED_FUNCTION_CALL".into()),
        ),
    ] {
        let mut d = ProtocolDecoder::new(Provider::Gemini);
        decode(&mut d, json!({"candidates":[{"finishReason":reason}]})).unwrap();
        assert_eq!(d.finish_reason(), Some(&expected));
        d.end_of_stream().unwrap();
    }
}
#[test]
fn google_tools_have_unique_ids_and_preserve_signed_checkpoints() {
    let mut d = ProtocolDecoder::new(Provider::Gemini);
    let events=decode(&mut d,json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"a","args":{"n":1}}},{"functionCall":{"name":"b","args":{}}}]},"finishReason":"STOP"}]})).unwrap();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0], StreamEvent::ResponseProgress);
    let ids: Vec<_> = events[1..]
        .iter()
        .map(|e| {
            if let StreamEvent::ToolCall(c) = e {
                &c.id
            } else {
                panic!()
            }
        })
        .collect();
    assert_ne!(ids[0], ids[1]);
    let mut d = ProtocolDecoder::new(Provider::Gemini);
    assert!(decode(
            &mut d,
            json!({"candidates":[{"content":{"parts":[{"functionCall":{"name":"x","args":{}},"thoughtSignature":"opaque"}]}}]})
        ).unwrap().iter().any(|event| matches!(event, StreamEvent::GeminiCheckpoint(_))));
}
#[test]
fn context_provider_goldens_system_tools_results_and_role_merging() {
    let call = ToolCall {
        id: "c1".into(),
        name: "weather".into(),
        arguments: json!({"city":"Paris"}),
    };
    let mut context = Context::new(vec![
        Message::text(Role::System, "old"),
        Message::text(Role::User, "Hi"),
        Message::text(Role::Developer, "rule"),
        Message::assistant_tool_calls(vec![call]),
        Message::tool_result("c1", "{\"temperature\":20}"),
    ]);
    context.tools = Some(vec![ToolDefinition {
        name: "weather".into(),
        description: "Get weather".into(),
        parameters: json!({"type":"object","properties":{"city":{"type":"string"}},"required":["city"]}),
    }]);
    context.tool_choice = Some(ToolChoice::Function("weather".into()));
    let mut req = CompletionRequest::new("synthetic", context);
    req.system_instruction = Some("override".into());
    let openai = request_body(Provider::OpenAi, &req).unwrap();
    assert_eq!(
        openai["messages"][0],
        json!({"role":"system","content":"override"})
    );
    assert_eq!(openai["messages"][1]["content"], "old");
    assert_eq!(
        openai["messages"][4]["tool_calls"][0]["function"]["arguments"],
        "{\"city\":\"Paris\"}"
    );
    assert_eq!(
        openai["tool_choice"],
        json!({"type":"function","function":{"name":"weather"}})
    );
    let anthropic = request_body(Provider::Anthropic, &req).unwrap();
    assert_eq!(anthropic["system"], "override");
    assert_eq!(anthropic["messages"].as_array().unwrap().len(), 3);
    assert_eq!(
        anthropic["messages"][0],
        json!({"role":"user","content":[{"type":"text","text":"Hi"},{"type":"text","text":"rule"}]})
    );
    assert_eq!(
        anthropic["messages"][1]["content"][0],
        json!({"type":"tool_use","id":"c1","name":"weather","input":{"city":"Paris"}})
    );
    let gemini = request_body(Provider::Gemini, &req).unwrap();
    assert_eq!(
        gemini["systemInstruction"],
        json!({"parts":[{"text":"override"}]})
    );
    assert_eq!(
        gemini["contents"][2]["parts"][0],
        json!({"functionResponse":{"id":"c1","name":"weather","response":{"temperature":20}}})
    );
    assert_eq!(
        gemini["toolConfig"],
        json!({"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["weather"]}})
    );
    assert_eq!(req.context.messages.len(), 5); // Conversion never mutates shared context.
}
#[test]
fn absent_tools_and_explicit_empty_are_distinct() {
    let mut req = CompletionRequest::new("synthetic", Context::default());
    assert!(request_body(Provider::OpenAi, &req)
        .unwrap()
        .get("tools")
        .is_none());
    req.context.tools = Some(vec![]);
    assert_eq!(
        request_body(Provider::OpenAi, &req).unwrap()["tools"],
        json!([])
    );
}
#[test]
fn inline_images_are_converted_but_unsupported_mime_errors_early() {
    let mut message = Message::text(Role::User, "");
    message.content = Some(Content::Parts(vec![ContentPart::ImageUrl {
        url: "data:image/png;base64,cG5n".into(),
    }]));
    let mut req = CompletionRequest::new("synthetic", Context::new(vec![message]));
    assert_eq!(
        request_body(Provider::Anthropic, &req).unwrap()["messages"][0]["content"][0],
        json!({"type":"image","source":{"type":"base64","media_type":"image/png","data":"cG5n"}})
    );
    assert_eq!(
        request_body(Provider::Gemini, &req).unwrap()["contents"][0]["parts"][0],
        json!({"inlineData":{"mimeType":"image/png","data":"cG5n"}})
    );
    req.context.messages[0].content = Some(Content::Parts(vec![ContentPart::InlineData {
        mime_type: "application/zip".into(),
        data: "eg==".into(),
    }]));
    assert!(matches!(
        request_body(Provider::Anthropic, &req),
        Err(ServiceError::Unsupported(_))
    ));
}
#[test]
fn extra_body_cannot_override_stream_or_request_identity() {
    for key in [
        "messages",
        "stream",
        "stream_options",
        "model",
        "n",
        "tools",
    ] {
        let mut req = CompletionRequest::new("synthetic", Context::default());
        req.extra.insert(key.into(), json!(false));
        assert!(request_body(Provider::OpenAi, &req).is_err());
    }
    let mut req = CompletionRequest::new("synthetic", Context::default());
    req.extra.insert("seed".into(), json!(42));
    assert_eq!(request_body(Provider::OpenAi, &req).unwrap()["seed"], 42);
}
#[test]
fn malformed_context_is_rejected_before_request() {
    let mut req = CompletionRequest::new(
        "synthetic",
        Context::new(vec![Message::text(Role::Tool, "orphan")]),
    );
    assert!(request_body(Provider::OpenAi, &req).is_err());
    req.context = Context::default();
    req.temperature = Some(f64::NAN);
    assert!(request_body(Provider::OpenAi, &req).is_err());
}

#[test]
fn anthropic_rejects_delta_for_wrong_block_type() {
    let mut d = ProtocolDecoder::new(Provider::Anthropic);
    decode(&mut d,json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"id","name":"f","input":{}}})).unwrap();
    assert!(matches!(
        decode(
            &mut d,
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"wrong"}})
        ),
        Err(ServiceError::Protocol(_))
    ));
}
#[test]
fn openai_refusal_is_visible_as_a_provider_error() {
    let mut d = ProtocolDecoder::new(Provider::OpenAi);
    assert!(
        matches!(decode(&mut d,json!({"choices":[{"delta":{"refusal":"synthetic refusal"}}]})),Err(ServiceError::Provider{kind,..}) if kind=="refusal")
    );
}
#[test]
fn client_without_tokio_returns_error_instead_of_panicking() {
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = "http://127.0.0.1:1/stream".into();
    let client = LlmClient::new(config).unwrap();
    assert!(matches!(
        client.stream(CompletionRequest::new("synthetic", Context::default())),
        Err(ServiceError::InvalidRequest(_))
    ));
}

#[test]
fn openai_malformed_argument_types_never_become_empty_tool_calls() {
    for arguments in [json!({"dangerous":"ignored"}), json!([1, 2]), json!(42)] {
        let mut d = ProtocolDecoder::new(Provider::OpenAi);
        assert!(matches!(
            decode(
                &mut d,
                json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"f","arguments":arguments}}]}}]})
            ),
            Err(ServiceError::Protocol(_))
        ));
    }
    let mut d = ProtocolDecoder::new(Provider::OpenAi);
    assert!(decode(&mut d, json!({"choices":[{"delta":{"tool_calls":{}}}]})).is_err());
    assert!(decode(
        &mut d,
        json!({"choices":[{"delta":{"content":["non-text"]}}]})
    )
    .is_err());
}
#[test]
fn anthropic_does_not_invent_usage_when_snapshot_is_absent() {
    let mut d = ProtocolDecoder::new(Provider::Anthropic);
    decode(&mut d, json!({"type":"message_start","message":{}})).unwrap();
    assert_eq!(d.usage(), None);
}

#[test]
fn openai_missing_arguments_do_not_invent_an_empty_object() {
    let mut d = ProtocolDecoder::new(Provider::OpenAi);
    decode(&mut d,json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"f"}}]}}]})).unwrap();
    assert!(done(&mut d).is_err());
}
