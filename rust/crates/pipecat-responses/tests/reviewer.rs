//! Independent adversarial protocol and context contracts over loopback transports.
use futures_util::{SinkExt, StreamExt};
use pipecat_responses::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio_tungstenite::{accept_async, tungstenite::Message};
mod common;
use common::*;

async fn response(ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>, id: &str) {
    ws_send(ws, created(id)).await;
    ws_send(ws, delta("fresh")).await;
    ws_send(ws, completed(id, "fresh")).await;
}

#[tokio::test]
async fn nested_error_object_does_not_launder_top_level_retry_code() {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(
            &mut ws,
            json!({"type":"error","error":{},"code":"websocket_connection_limit_reached"}),
        )
        .await;
        let replay = tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;
        if let Ok(Ok((socket, _))) = replay {
            let mut ws = accept_async(socket).await.unwrap();
            ws_read(&mut ws).await;
            response(&mut ws, "replayed").await;
            true
        } else {
            false
        }
    });
    let events = collect(
        ResponsesWebSocketClient::new(cfg)
            .unwrap()
            .stream(request())
            .unwrap(),
    )
    .await;
    assert!(
        !server.await.unwrap(),
        "unstructured top-level code authorized replay"
    );
    assert!(events.iter().any(Result::is_err));
}

#[tokio::test]
async fn response_id_cannot_be_reused_after_intervening_success() {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        for id in ["r1", "r2", "r1"] {
            ws_read(&mut ws).await;
            for event in [created(id), delta("fresh"), completed(id, "fresh")] {
                if ws
                    .send(Message::Text(event.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    for _ in 0..2 {
        assert!(collect(client.stream(request()).unwrap())
            .await
            .iter()
            .all(Result::is_ok));
    }
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events
        .iter()
        .any(|event| matches!(event, Err(ServiceError::Protocol(_)))));
    assert!(!events
        .iter()
        .any(|event| matches!(event, Ok(ResponseEvent::TextDelta(_)))));
    server.await.unwrap();
}

async fn assert_bad_drain_discards_connection(bad_event: Value) {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("cancelled")).await;
        assert_eq!(ws_read(&mut ws).await["type"], "response.cancel");
        ws_send(&mut ws, bad_event).await;
        // A matching terminal after an invalid event must not erase ambiguity.
        let _ = ws
            .send(Message::Text(
                completed("cancelled", "discard").to_string().into(),
            ))
            .await;
        match tokio::time::timeout(Duration::from_millis(500), ws.next()).await {
            Ok(Some(Ok(Message::Text(_)))) => {
                response(&mut ws, "fresh-response").await;
                true
            }
            _ => {
                let (socket, _) = tokio::time::timeout(Duration::from_secs(2), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                let mut fresh = accept_async(socket).await.unwrap();
                assert!(ws_read(&mut fresh)
                    .await
                    .get("previous_response_id")
                    .is_none());
                response(&mut fresh, "fresh-response").await;
                false
            }
        }
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let mut stream = client.stream(request()).unwrap();
    while !matches!(
        stream.recv().await,
        Some(Ok(ResponseEvent::ResponseCreated { .. }))
    ) {}
    stream.close().await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(
        !server.await.unwrap(),
        "ambiguous cancellation boundary reused old socket"
    );
    assert!(events.iter().all(Result::is_ok), "{events:?}");
}

#[tokio::test]
async fn cancellation_drain_rejects_wrong_terminal_id() {
    assert_bad_drain_discards_connection(completed("different-response", "stale")).await;
}
#[tokio::test]
async fn cancellation_drain_rejects_missing_terminal_id() {
    assert_bad_drain_discards_connection(json!({"type":"response.completed","response":{}})).await;
}
#[tokio::test]
async fn cancellation_drain_rejects_unknown_events() {
    assert_bad_drain_discards_connection(json!({"type":"unrecognized.event","data":"private"}))
        .await;
}
#[tokio::test]
async fn cancellation_drain_rejects_a_second_created_response() {
    assert_bad_drain_discards_connection(created("different-response")).await;
}

#[test]
fn unknown_core_extensions_are_rejected_without_echoing_keys() {
    let mut message: pipecat_core::ContextMessage =
        pipecat_core::ChatMessage::new(pipecat_core::ChatRole::User, "hello").into();
    message.extra.insert(
        "https://private.example/?key=private-secret".into(),
        json!(true),
    );
    let context = pipecat_core::LlmContextData {
        messages: vec![message],
        tools: pipecat_core::ToolSet::NotGiven,
        tool_choice: None,
    };
    let error = ResponsesContext::from_core(&context).unwrap_err();
    assert!(!format!("{error:?}").contains("private-secret"));
    assert!(!error.to_string().contains("private.example"));
}

#[test]
fn core_context_never_silently_converts_user_calls_to_assistant_calls() {
    let mut message: pipecat_core::ContextMessage =
        pipecat_core::ChatMessage::new(pipecat_core::ChatRole::User, "").into();
    message.tool_calls.push(pipecat_core::FunctionCall {
        function_name: "lookup".into(),
        tool_call_id: "call-1".into(),
        arguments: json!({}),
    });
    let context = pipecat_core::LlmContextData {
        messages: vec![message],
        tools: pipecat_core::ToolSet::NotGiven,
        tool_choice: None,
    };
    assert!(ResponsesContext::from_core(&context)
        .and_then(|context| ResponsesRequest::new("gpt-4.1", context).body(true))
        .is_err());
}

#[test]
fn core_context_rejects_duplicate_function_identity() {
    let mut message: pipecat_core::ContextMessage =
        pipecat_core::ChatMessage::new(pipecat_core::ChatRole::Assistant, "").into();
    message.tool_calls.push(pipecat_core::FunctionCall {
        function_name: "lookup".into(),
        tool_call_id: "call-1".into(),
        arguments: json!({}),
    });
    let context = pipecat_core::LlmContextData {
        messages: vec![message.clone(), message],
        tools: pipecat_core::ToolSet::NotGiven,
        tool_choice: None,
    };
    assert!(ResponsesContext::from_core(&context)
        .and_then(|context| ResponsesRequest::new("gpt-4.1", context).body(true))
        .is_err());
}

#[tokio::test]
async fn active_websocket_terminal_requires_response_identity() {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("known")).await;
        ws_send(&mut ws, json!({"type":"response.completed","response":{}})).await;
    });
    let events = collect(
        ResponsesWebSocketClient::new(cfg)
            .unwrap()
            .stream(request())
            .unwrap(),
    )
    .await;
    assert!(events
        .iter()
        .any(|event| matches!(event, Err(ServiceError::Protocol(_)))));
    server.await.unwrap();
}

#[tokio::test]
async fn explicit_foreign_response_identity_never_emits_output() {
    for bad_event in [
        json!({"type":"response.output_text.delta","response_id":"foreign","delta":"must-not-emit"}),
        json!({"type":"response.in_progress","response":{"id":"foreign"}}),
    ] {
        let (listener, cfg) = ws_listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws_read(&mut ws).await;
            ws_send(&mut ws, created("known")).await;
            ws_send(&mut ws, bad_event).await;
            let _ = ws
                .send(Message::Text(completed("known", "").to_string().into()))
                .await;
        });
        let events = collect(
            ResponsesWebSocketClient::new(cfg)
                .unwrap()
                .stream(request())
                .unwrap(),
        )
        .await;
        assert!(events
            .iter()
            .any(|event| matches!(event, Err(ServiceError::Protocol(_)))));
        assert!(!events
            .iter()
            .any(|event| matches!(event, Ok(ResponseEvent::TextDelta(_)))));
        server.await.unwrap();
    }
}

#[test]
fn actual_websocket_request_envelope_respects_byte_limit() {
    let req = request();
    let bytes = req.body(true).unwrap().to_string().len();
    let mut cfg = config();
    cfg.max_request_bytes = bytes;
    assert!(ResponsesHttpClient::new(cfg.clone())
        .unwrap()
        .request_body(&req)
        .is_ok());
    assert!(ResponsesWebSocketClient::new(cfg)
        .unwrap()
        .request_body(&req)
        .is_err());
}

#[tokio::test]
async fn cancellation_drain_deadline_is_total_not_per_event() {
    let (listener, mut cfg) = ws_listener().await;
    cfg.drain_timeout = Duration::from_millis(80);
    cfg.idle_timeout = Duration::from_secs(1);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("never-ends")).await;
        assert_eq!(ws_read(&mut ws).await["type"], "response.cancel");
        for _ in 0..100 {
            if ws
                .send(Message::Text(delta("discard").to_string().into()))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let mut stream = client.stream(request()).unwrap();
    while !matches!(
        stream.recv().await,
        Some(Ok(ResponseEvent::ResponseCreated { .. }))
    ) {}
    tokio::time::timeout(Duration::from_millis(500), stream.close())
        .await
        .expect("drain deadline reset on deltas");
    server.await.unwrap();
}

#[tokio::test]
async fn full_consumer_queue_does_not_block_close_or_fresh_work() {
    let (listener, mut cfg) = ws_listener().await;
    cfg.channel_capacity = 1;
    let (ready, wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("old")).await;
        for _ in 0..8 {
            ws_send(&mut ws, delta("discard")).await;
        }
        ready.send(()).unwrap();
        assert_eq!(ws_read(&mut ws).await["type"], "response.cancel");
        ws_send(&mut ws, completed("old", "discard")).await;
        assert!(ws_read(&mut ws).await.get("previous_response_id").is_none());
        response(&mut ws, "fresh").await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let stream = client.stream(request()).unwrap();
    wait.await.unwrap();
    tokio::time::timeout(Duration::from_millis(500), stream.close())
        .await
        .expect("blocked by downstream backpressure");
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(!events
        .iter()
        .any(|event| matches!(event, Ok(ResponseEvent::TextDelta(text)) if text == "discard")));
    server.await.unwrap();
}

#[test]
fn unknown_nested_reasoning_settings_are_rejected() {
    let mut settings = pipecat_core::LlmSettings::default();
    settings
        .base
        .extra
        .insert("reasoning".into(), json!({"effort_typo":"high"}));
    assert!(ResponsesSettings::from_llm(&settings).is_err());
}

#[test]
fn unknown_reasoning_history_fields_are_not_silently_discarded() {
    let mut message: pipecat_core::ContextMessage =
        pipecat_core::ChatMessage::new(pipecat_core::ChatRole::Assistant, "").into();
    message.extra.insert("openai_responses_reasoning".into(), json!({"id":"rs","summary":[],"encrypted_content":"opaque","unknown_payload":"must-not-disappear"}));
    let context = pipecat_core::LlmContextData {
        messages: vec![message],
        tools: pipecat_core::ToolSet::NotGiven,
        tool_choice: None,
    };
    assert!(ResponsesContext::from_core(&context).is_err());
}

#[tokio::test]
async fn reasoning_history_identity_cannot_change_at_completion() {
    for field in ["encrypted_content", "summary"] {
        let item = json!({"type":"reasoning","id":"rs","summary":[{"type":"summary_text","text":"first"}],"encrypted_content":"first-opaque"});
        let mut changed = item.clone();
        changed[field] = if field == "summary" {
            json!([{"type":"summary_text","text":"changed"}])
        } else {
            json!("changed-opaque")
        };
        let (client, _, server) = http_fixture(vec![
            created("r"),
            json!({"type":"response.output_item.done","item":item}),
            json!({"type":"response.completed","response":{"id":"r","output":[changed]}}),
        ])
        .await;
        let events = collect(client.stream(request()).unwrap()).await;
        assert!(
            events
                .iter()
                .any(|event| matches!(event, Err(ServiceError::Protocol(_)))),
            "changed {field}: {events:?}"
        );
        server.await.unwrap();
    }
}

#[test]
fn tool_result_identity_on_non_tool_messages_is_explicitly_rejected() {
    for role in [
        pipecat_services::Role::User,
        pipecat_services::Role::Assistant,
        pipecat_services::Role::System,
    ] {
        let mut message = pipecat_services::Message::text(role, "text");
        message.tool_call_id = Some("orphaned-id".into());
        assert!(
            ResponsesContext::try_from(&pipecat_services::Context::new(vec![message])).is_err()
        );
    }
}

#[tokio::test]
async fn interleaved_function_fragments_keep_identity_and_finality() {
    let (client, _, server) = http_fixture(vec![
        created("r"),
        json!({"type":"response.output_item.added","item":{"type":"function_call","id":"item-a","call_id":"call-a","name":"alpha","arguments":""}}),
        json!({"type":"response.output_item.added","item":{"type":"function_call","id":"item-b","call_id":"call-b","name":"beta","arguments":""}}),
        json!({"type":"response.function_call_arguments.delta","item_id":"item-a","delta":"{\"a\":"}),
        json!({"type":"response.function_call_arguments.delta","item_id":"item-b","delta":"{\"b\":2}"}),
        json!({"type":"response.function_call_arguments.delta","item_id":"item-a","delta":"1}"}),
        json!({"type":"response.function_call_arguments.done","item_id":"item-b","arguments":"{\"b\":2}"}),
        json!({"type":"response.function_call_arguments.done","item_id":"item-a","arguments":"{\"a\":1}"}),
        json!({"type":"response.function_call_arguments.done","item_id":"item-a","arguments":"{\"a\":1}"}),
        json!({"type":"response.completed","response":{"id":"r"}}),
    ]).await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let calls: Vec<_> = events
        .iter()
        .filter_map(|event| match event {
            Ok(ResponseEvent::ToolCall(call)) => Some(call),
            _ => None,
        })
        .collect();
    assert_eq!(calls.len(), 2);
    assert_eq!(
        (&calls[0].id, &calls[0].name, &calls[0].arguments),
        (&"call-b".to_string(), &"beta".to_string(), &json!({"b":2}))
    );
    assert_eq!(
        (&calls[1].id, &calls[1].name, &calls[1].arguments),
        (&"call-a".to_string(), &"alpha".to_string(), &json!({"a":1}))
    );
    server.await.unwrap();
}

#[tokio::test]
async fn function_completion_rejects_identity_or_final_argument_mutation() {
    for changed in [
        json!({"type":"response.output_item.done","item":{"type":"function_call","id":"item","call_id":"foreign","name":"lookup","arguments":"{}"}}),
        json!({"type":"response.function_call_arguments.done","item_id":"item","arguments":"{\"changed\":true}"}),
        json!({"type":"response.function_call_arguments.delta","item_id":"item","delta":"must-not-append"}),
    ] {
        let (client, _, server) = http_fixture(vec![
            created("r"),
            json!({"type":"response.output_item.added","item":{"type":"function_call","id":"item","call_id":"call","name":"lookup","arguments":""}}),
            json!({"type":"response.function_call_arguments.done","item_id":"item","arguments":"{}"}),
            changed,
        ]).await;
        let events = collect(client.stream(request()).unwrap()).await;
        assert!(events
            .iter()
            .any(|event| matches!(event, Err(ServiceError::Protocol(_)))));
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, Ok(ResponseEvent::ToolCall(_))))
                .count(),
            1
        );
        assert!(matches!(
            events.last(),
            Some(Ok(ResponseEvent::Finished {
                status: ResponseStatus::Failed,
                ..
            }))
        ));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn inference_null_max_tokens_clears_only_the_one_shot_cap() {
    use pipecat_services::LlmInference;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let body = read_http(&mut socket).await.1;
        reply(
            &mut socket,
            "200 OK",
            "application/json",
            br#"{"status":"completed","output":[]}"#,
        )
        .await;
        body
    });
    let mut req = request();
    req.settings.max_completion_tokens = pipecat_core::Setting::Value(91);
    let processor = ResponsesProcessor::http(cfg, req).unwrap();
    let context = pipecat_core::LlmContextData {
        messages: vec![
            pipecat_core::ChatMessage::new(pipecat_core::ChatRole::User, "one shot").into(),
        ],
        tools: pipecat_core::ToolSet::NotGiven,
        tool_choice: None,
    };
    let mut options = pipecat_services::InferenceOptions::default();
    options.settings.max_tokens = pipecat_core::Setting::Null;
    assert!(processor
        .run_inference(context, options)
        .await
        .unwrap()
        .is_none());
    let body = server.await.unwrap();
    assert!(body.get("max_output_tokens").is_none(), "{body}");
    assert_eq!(
        processor
            .request_snapshot()
            .unwrap()
            .settings
            .max_completion_tokens,
        pipecat_core::Setting::Value(91)
    );
}

#[tokio::test]
async fn pipeline_cleanup_closes_session_even_when_processor_is_retained() {
    use pipecat_core::{Direction, Frame, FrameData, Pipeline, PipelineConfig};
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        response(&mut ws, "r").await;
        matches!(
            tokio::time::timeout(Duration::from_secs(1), ws.next()).await,
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) | Ok(Some(Err(_)))
        )
    });
    let retained = std::sync::Arc::new(ResponsesProcessor::websocket(cfg, request()).unwrap());
    let mut worker = Pipeline::new(vec![retained.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    while worker.recv().await.is_some() {}
    worker.join().await.unwrap();
    assert!(
        server.await.unwrap(),
        "pipeline cleanup retained a live WebSocket session"
    );
    assert!(retained.request_snapshot().is_ok());
}

#[tokio::test]
async fn cancellation_drain_rejects_foreign_or_malformed_explicit_identity() {
    for event in [
        json!({"type":"response.in_progress","response":{"id":"foreign"}}),
        json!({"type":"response.output_text.delta","response_id":123,"delta":"discard"}),
    ] {
        assert_bad_drain_discards_connection(event).await;
    }
}

#[test]
fn impossible_channel_capacity_is_a_constructor_error_not_a_runtime_panic() {
    let mut cfg = config();
    cfg.channel_capacity = usize::MAX;
    assert!(matches!(
        ResponsesHttpClient::new(cfg.clone()),
        Err(ServiceError::InvalidRequest(_))
    ));
    assert!(matches!(
        ResponsesWebSocketClient::new(cfg),
        Err(ServiceError::InvalidRequest(_))
    ));
}

#[test]
fn overflowing_duration_is_rejected_before_task_spawn() {
    for field in 0..5 {
        let mut cfg = config();
        match field {
            0 => cfg.request_timeout = Duration::MAX,
            1 => cfg.connect_timeout = Duration::MAX,
            2 => cfg.idle_timeout = Duration::MAX,
            3 => cfg.drain_timeout = Duration::MAX,
            _ => cfg.max_connection_age = Duration::MAX,
        }
        assert!(matches!(
            ResponsesHttpClient::new(cfg.clone()),
            Err(ServiceError::InvalidRequest(_))
        ));
        assert!(matches!(
            ResponsesWebSocketClient::new(cfg),
            Err(ServiceError::InvalidRequest(_))
        ));
    }
}

#[tokio::test]
async fn refusal_deltas_done_and_repeated_final_objects_do_not_duplicate_text() {
    let item = json!({"type":"message","id":"message","role":"assistant","content":[{"type":"refusal","refusal":"I cannot help with that"}]});
    let (client, _, server) = http_fixture(vec![
        created("r"),
        json!({"type":"response.output_item.added","item":{"type":"message","id":"message","role":"assistant","content":[]}}),
        json!({"type":"response.content_part.added","item_id":"message","output_index":0,"content_index":0,"part":{"type":"refusal","refusal":""}}),
        json!({"type":"response.refusal.delta","item_id":"message","output_index":0,"content_index":0,"delta":"I cannot "}),
        json!({"type":"response.refusal.delta","item_id":"message","output_index":0,"content_index":0,"delta":"help with that"}),
        json!({"type":"response.refusal.done","item_id":"message","output_index":0,"content_index":0,"refusal":"I cannot help with that"}),
        json!({"type":"response.content_part.done","item_id":"message","output_index":0,"content_index":0,"part":{"type":"refusal","refusal":"I cannot help with that"}}),
        json!({"type":"response.output_item.done","item":item}),
        json!({"type":"response.completed","response":{"id":"r","output":[item]}}),
    ]).await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let text: String = events
        .iter()
        .filter_map(|event| match event {
            Ok(ResponseEvent::TextDelta(text)) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "I cannot help with that");
    assert!(matches!(
        events.last(),
        Some(Ok(ResponseEvent::Finished {
            status: ResponseStatus::Completed,
            ..
        }))
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn final_only_refusal_parts_keep_item_and_content_identity() {
    let first = json!({"type":"message","id":"first","role":"assistant","content":[{"type":"refusal","refusal":"A"},{"type":"refusal","refusal":"B"}]});
    let second = json!({"type":"message","id":"second","role":"assistant","content":[{"type":"refusal","refusal":"C"}]});
    let (client, _, server) = http_fixture(vec![
        created("r"),
        json!({"type":"response.output_item.done","item":first}),
        json!({"type":"response.completed","response":{"id":"r","output":[first,second]}}),
    ])
    .await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    let text: String = events
        .iter()
        .filter_map(|event| match event {
            Ok(ResponseEvent::TextDelta(text)) => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(text, "ABC");
    server.await.unwrap();
}
