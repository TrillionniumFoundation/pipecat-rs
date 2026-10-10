//! Real loopback HTTP/SSE fixtures; no provider credentials or API calls.
use pipecat_responses::*;
use serde_json::json;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
};
mod common;
use common::*;

#[tokio::test]
async fn text_reasoning_tools_and_usage_stream_incrementally() {
    let reasoning = json!({"type":"reasoning","id":"rs_1","summary":[{"type":"summary_text","text":"think"}],"encrypted_content":"opaque"});
    let call = json!({"type":"function_call","id":"fc_1","call_id":"call_1","name":"lookup","arguments":""});
    let mut done = call.clone();
    done["arguments"] = json!("{\"city\":\"Paris\"}");
    let (client,capture,task)=http_fixture(vec![created("r1"),json!({"type":"response.reasoning_summary_text.delta","delta":"think"}),json!({"type":"response.output_item.done","item":reasoning}),delta("hé"),json!({"type":"response.output_item.added","item":call}),json!({"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"{\"city\":"}),json!({"type":"response.function_call_arguments.delta","item_id":"fc_1","delta":"\"Paris\"}"}),json!({"type":"response.function_call_arguments.done","item_id":"fc_1","arguments":"{\"city\":\"Paris\"}"}),json!({"type":"response.output_item.done","item":done}),completed("r1","hé")]).await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert!(events
        .iter()
        .any(|e| matches!(e,Ok(ResponseEvent::TextDelta(text)) if text=="hé")));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(ResponseEvent::ToolCall(_))))
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(ResponseEvent::ThoughtStart)))
            .count(),
        1
    );
    assert!(events.iter().any(|e|matches!(e,Ok(ResponseEvent::ReasoningItem(item)) if item.encrypted_content.as_deref()==Some("opaque"))));
    let usage = events
        .iter()
        .find_map(|e| {
            if let Ok(ResponseEvent::Usage(u)) = e {
                Some(u)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(usage.prompt_tokens, 10);
    assert_eq!(usage.cache_read_input_tokens, Some(3));
    assert_eq!(usage.cache_creation_input_tokens, Some(2));
    assert_eq!(usage.reasoning_tokens, Some(1));
    assert!(matches!(
        events.last(),
        Some(Ok(ResponseEvent::Finished {
            status: ResponseStatus::Completed,
            ..
        }))
    ));
    let (headers, body) = capture.await.unwrap();
    assert!(headers.starts_with("POST /v1/responses"));
    assert_eq!(body["store"], false);
    assert_eq!(body["stream"], true);
    assert!(body.get("messages").is_none());
    task.await.unwrap();
}
#[tokio::test]
async fn partial_usage_is_zero_and_terminal_error_stops_stream() {
    let (client,_,task)=http_fixture(vec![json!({"type":"response.completed","response":{"id":"r","usage":{"input_tokens":null,"input_tokens_details":null,"output_tokens_details":{}}}}),delta("must not be read")]).await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(!events
        .iter()
        .any(|e| matches!(e, Ok(ResponseEvent::TextDelta(_)))));
    assert!(events.iter().any(|e|matches!(e,Ok(ResponseEvent::Usage(u)) if u.prompt_tokens==0 && u.reasoning_tokens==Some(0))));
    task.await.unwrap();
}
#[tokio::test]
async fn terminal_failures_do_not_fabricate_calls_or_leak_provider_body() {
    for terminal in [
        json!({"type":"response.failed","response":{"error":{"message":"synthetic-secret private prompt"}}}),
        json!({"type":"response.incomplete","response":{"incomplete_details":{"reason":"synthetic-secret"}}}),
        json!({"type":"error","code":"synthetic-secret","message":"private prompt"}),
    ] {
        let (client,_,task)=http_fixture(vec![json!({"type":"response.output_item.added","item":{"type":"function_call","id":"fc","call_id":"c","name":"lookup","arguments":""}}),terminal,delta("ignored")]).await;
        let events = collect(client.stream(request()).unwrap()).await;
        assert!(!events
            .iter()
            .any(|e| matches!(e, Ok(ResponseEvent::ToolCall(_)))));
        let errors: Vec<_> = events.iter().filter_map(|e| e.as_ref().err()).collect();
        assert_eq!(errors.len(), 1);
        assert!(!format!("{errors:?}").contains("synthetic-secret"));
        assert!(!format!("{errors:?}").contains("private prompt"));
        task.await.unwrap();
    }
}
#[tokio::test]
async fn completed_parallel_call_survives_later_failed_item() {
    let events = vec![
        json!({"type":"response.output_item.added","item":{"type":"function_call","id":"fc","call_id":"c","name":"lookup","arguments":""}}),
        json!({"type":"response.function_call_arguments.done","item_id":"fc","arguments":"{}"}),
        json!({"type":"response.failed","response":{}}),
    ];
    let (client, _, task) = http_fixture(events).await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(ResponseEvent::ToolCall(_))))
            .count(),
        1
    );
    assert!(events.iter().any(Result::is_err));
    task.await.unwrap();
}
#[tokio::test]
async fn malformed_tool_json_fails_without_inventing_empty_arguments() {
    let(client,_,task)=http_fixture(vec![json!({"type":"response.output_item.added","item":{"type":"function_call","id":"fc","call_id":"c","name":"lookup","arguments":""}}),json!({"type":"response.function_call_arguments.done","item_id":"fc","arguments":"secret invalid"})]).await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Protocol(_)))));
    assert!(!events
        .iter()
        .any(|e| matches!(e, Ok(ResponseEvent::ToolCall(_)))));
    assert!(!format!("{events:?}").contains("secret invalid"));
    task.await.unwrap();
}
#[tokio::test]
async fn truncated_and_done_only_streams_are_errors() {
    for bytes in [
        record(delta("partial")),
        b"data: [DONE]\n\n".to_vec(),
        b"data: secret invalid\n\n".to_vec(),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = config();
        cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http(&mut socket).await;
            reply(&mut socket, "200 OK", "text/event-stream", &bytes).await;
        });
        let events = collect(
            ResponsesHttpClient::new(cfg)
                .unwrap()
                .stream(request())
                .unwrap(),
        )
        .await;
        assert!(events
            .iter()
            .any(|e| matches!(e, Err(ServiceError::Protocol(_)))));
        task.await.unwrap();
    }
}
#[tokio::test]
async fn real_one_byte_chunks_preserve_unicode_and_sse_boundaries() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        for byte in [record(delta("你好🌍")), record(completed("r", "你好🌍"))].concat() {
            socket
                .write_all(&[b'1', b'\r', b'\n', byte, b'\r', b'\n'])
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
    });
    let events = collect(
        ResponsesHttpClient::new(cfg)
            .unwrap()
            .stream(request())
            .unwrap(),
    )
    .await;
    assert!(events.iter().all(Result::is_ok));
    assert!(events
        .iter()
        .any(|e| matches!(e,Ok(ResponseEvent::TextDelta(text)) if text=="你好🌍")));
    task.await.unwrap();
}
#[tokio::test]
async fn status_errors_never_include_response_body_or_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        reply(
            &mut socket,
            "429 Too Many Requests",
            "application/json",
            b"synthetic-secret https://secret.test/?key=secret",
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err()
        );
    });
    let events = collect(
        ResponsesHttpClient::new(cfg)
            .unwrap()
            .stream(request())
            .unwrap(),
    )
    .await;
    assert!(events.iter().any(
        |e| matches!(e,Err(ServiceError::Http{status:429,retry_after:Some(delay),..}) if delay=="3")
    ));
    assert!(!format!("{events:?}").contains("synthetic-secret"));
    assert!(!format!("{events:?}").contains("secret.test"));
    task.await.unwrap();
}
#[tokio::test]
async fn bounded_event_and_total_response_limits() {
    for total in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut cfg = config();
        cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
        if total {
            cfg.max_response_bytes = 100;
        } else {
            cfg.max_event_bytes = 100;
        }
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_http(&mut socket).await;
            reply(
                &mut socket,
                "200 OK",
                "text/event-stream",
                &record(delta(&"x".repeat(300))),
            )
            .await;
        });
        let events = collect(
            ResponsesHttpClient::new(cfg)
                .unwrap()
                .stream(request())
                .unwrap(),
        )
        .await;
        assert!(events
            .iter()
            .any(|e| matches!(e, Err(ServiceError::Protocol(_)))));
        task.await.unwrap();
    }
}
#[tokio::test]
async fn cancel_while_waiting_headers_closes_socket_and_never_replays() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        tx.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err()
        );
    });
    let stream = ResponsesHttpClient::new(cfg)
        .unwrap()
        .stream(request())
        .unwrap();
    rx.await.unwrap();
    stream.close().await;
    task.await.unwrap();
}
#[tokio::test]
async fn timeout_is_bounded_and_does_not_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    cfg.request_timeout = Duration::from_millis(60);
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err()
        );
    });
    let events = collect(
        ResponsesHttpClient::new(cfg)
            .unwrap()
            .stream(request())
            .unwrap(),
    )
    .await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Timeout))));
    task.await.unwrap();
}
#[tokio::test]
async fn one_shot_uses_nonstream_responses_schema_and_text_extraction() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (_, body) = read_http(&mut socket).await;
        assert_eq!(body["stream"], false);
        assert_eq!(body["text"]["format"]["type"], "json_schema");
        let body=json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"{\"ok\":true}"}]}]}).to_string();
        reply(&mut socket, "200 OK", "application/json", body.as_bytes()).await;
    });
    let mut req = request();
    req.response_schema = Some(
        json!({"type":"object","properties":{"ok":{"type":"boolean"}},"required":["ok"],"additionalProperties":false}),
    );
    let text = ResponsesHttpClient::new(cfg)
        .unwrap()
        .run_inference(&req, CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(text.as_deref(), Some("{\"ok\":true}"));
    task.await.unwrap();
}

#[tokio::test]
async fn dropping_backpressured_stream_finishes_owned_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    cfg.channel_capacity = 1;
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let body = record(delta("blocked"));
        socket
            .write_all(format!("{:x}\r\n", body.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(&body).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        tx.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let stream = ResponsesHttpClient::new(cfg)
        .unwrap()
        .stream(request())
        .unwrap();
    rx.await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), stream.close())
        .await
        .unwrap();
    server.await.unwrap();
}
