//! Native Responses WebSocket fixtures, distinct from the Realtime protocol.
use futures_util::StreamExt;
use pipecat_responses::*;
use pipecat_services::Role;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio_tungstenite::accept_async;
mod common;
use common::*;
fn next_request() -> ResponsesRequest {
    let mut req = request();
    req.context.items.extend([
        InputItem::text(Role::Assistant, "first"),
        InputItem::text(Role::User, "next"),
    ]);
    req
}
fn rejection(code: &str) -> Value {
    json!({"type":"error","error":{"code":code,"message":"synthetic-secret private prompt"}})
}
async fn send_response(
    ws: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    id: &str,
    text: &str,
) {
    ws_send(ws, created(id)).await;
    ws_send(ws, delta(text)).await;
    ws_send(ws, completed(id, text)).await;
}

#[tokio::test]
async fn persistent_session_uses_incremental_context_and_no_stream_parameter() {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let first = ws_read(&mut ws).await;
        assert_eq!(first["type"], "response.create");
        assert_eq!(first["store"], false);
        assert!(first.get("stream").is_none());
        assert!(first.get("previous_response_id").is_none());
        send_response(&mut ws, "r1", "first").await;
        let next = ws_read(&mut ws).await;
        assert_eq!(next["previous_response_id"], "r1");
        assert_eq!(next["input"], json!([{"role":"user","content":"next"}]));
        send_response(&mut ws, "r2", "second").await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    assert!(collect(client.stream(request()).unwrap())
        .await
        .iter()
        .all(Result::is_ok));
    assert!(collect(client.stream(next_request()).unwrap())
        .await
        .iter()
        .all(Result::is_ok));
    server.await.unwrap();
}
#[tokio::test]
async fn edited_prefix_output_or_settings_force_full_context() {
    for variant in 0..3 {
        let (listener, cfg) = ws_listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws_read(&mut ws).await;
            send_response(&mut ws, "r1", "first").await;
            let next = ws_read(&mut ws).await;
            assert!(next.get("previous_response_id").is_none());
            assert_eq!(next["input"].as_array().unwrap().len(), 3);
            send_response(&mut ws, "r2", "second").await;
        });
        let client = ResponsesWebSocketClient::new(cfg).unwrap();
        collect(client.stream(request()).unwrap()).await;
        let mut req = next_request();
        match variant {
            0 => req.context.items[0] = InputItem::text(Role::User, "edited"),
            1 => req.context.items[1] = InputItem::text(Role::Assistant, "edited"),
            _ => req.model = "gpt-5.4".into(),
        };
        assert!(collect(client.stream(req).unwrap())
            .await
            .iter()
            .all(Result::is_ok));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn cache_miss_retries_once_with_full_context_on_same_socket() {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        send_response(&mut ws, "r1", "first").await;
        assert_eq!(ws_read(&mut ws).await["previous_response_id"], "r1");
        ws_send(&mut ws, rejection("previous_response_not_found")).await;
        let retry = ws_read(&mut ws).await;
        assert!(retry.get("previous_response_id").is_none());
        assert_eq!(retry["input"].as_array().unwrap().len(), 3);
        send_response(&mut ws, "r2", "second").await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    collect(client.stream(request()).unwrap()).await;
    let events = collect(client.stream(next_request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(ResponseEvent::Started)))
            .count(),
        1
    );
    server.await.unwrap();
}
#[tokio::test]
async fn connection_limit_reconnects_once_and_never_loops() {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            let request = ws_read(&mut ws).await;
            assert!(request.get("previous_response_id").is_none());
            ws_send(&mut ws, rejection("websocket_connection_limit_reached")).await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let events = collect(client.stream(request()).unwrap()).await;
    assert_eq!(events.iter().filter(|e| e.is_err()).count(), 1);
    assert!(!format!("{events:?}").contains("synthetic-secret"));
    server.await.unwrap();
}
#[tokio::test]
async fn rejection_after_created_or_output_never_replays() {
    for output in [false, true] {
        let (listener, cfg) = ws_listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws_read(&mut ws).await;
            ws_send(&mut ws, created("r1")).await;
            if output {
                ws_send(&mut ws, delta("partial")).await;
            }
            ws_send(&mut ws, rejection("websocket_connection_limit_reached")).await;
            let next = tokio::time::timeout(Duration::from_millis(100), ws.next()).await;
            assert!(!matches!(
                next,
                Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Text(_))))
            ));
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        });
        let client = ResponsesWebSocketClient::new(cfg).unwrap();
        let events = collect(client.stream(request()).unwrap()).await;
        assert!(events.iter().any(Result::is_err));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn top_level_code_and_arbitrary_error_text_cannot_authorize_retry() {
    for error in [
        json!({"type":"error","code":"websocket_connection_limit_reached","message":"server failure"}),
        json!({"type":"error","error":{"code":"other","message":"websocket_connection_limit_reached"}}),
        rejection("previous_response_not_found"),
    ] {
        let (listener, cfg) = ws_listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws_read(&mut ws).await;
            ws_send(&mut ws, error).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        });
        let client = ResponsesWebSocketClient::new(cfg).unwrap();
        let events = collect(client.stream(request()).unwrap()).await;
        assert!(events.iter().any(Result::is_err));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn ambiguous_disconnect_never_replays_and_next_call_starts_full() {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("ambiguous")).await;
        ws_send(&mut ws, delta("partial")).await;
        drop(ws);
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        let req = ws_read(&mut ws).await;
        assert_eq!(req["input"][0]["content"], "explicit next");
        assert!(req.get("previous_response_id").is_none());
        send_response(&mut ws, "r2", "good").await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let first = collect(client.stream(request()).unwrap()).await;
    assert!(first
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Transport(_)))));
    let mut req = request();
    req.context.items[0] = InputItem::text(Role::User, "explicit next");
    assert!(collect(client.stream(req).unwrap())
        .await
        .iter()
        .all(Result::is_ok));
    server.await.unwrap();
}
#[tokio::test]
async fn cancel_after_created_sends_cancel_drains_old_events_then_reuses() {
    let (listener, cfg) = ws_listener().await;
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("old")).await;
        tx.send(()).unwrap();
        let cancel = ws_read(&mut ws).await;
        assert_eq!(
            cancel,
            json!({"type":"response.cancel","response_id":"old"})
        );
        ws_send(&mut ws, delta("stale must be discarded")).await;
        ws_send(
            &mut ws,
            json!({"type":"response.incomplete","response":{"id":"old"}}),
        )
        .await;
        let req = ws_read(&mut ws).await;
        assert!(req.get("previous_response_id").is_none());
        send_response(&mut ws, "new", "clean").await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let mut stream = client.stream(request()).unwrap();
    rx.await.unwrap();
    while !matches!(
        stream.recv().await,
        Some(Ok(ResponseEvent::ResponseCreated { .. }))
    ) {}
    stream.close().await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok));
    assert!(!format!("{events:?}").contains("stale must"));
    server.await.unwrap();
}
#[tokio::test]
async fn cancel_before_created_defers_cancel_until_id_then_drains() {
    let (listener, cfg) = ws_listener().await;
    let (tx, rx) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        tx.send(()).unwrap();
        wait.await.unwrap();
        ws_send(&mut ws, created("late")).await;
        assert_eq!(ws_read(&mut ws).await["response_id"], "late");
        ws_send(
            &mut ws,
            json!({"type":"response.failed","response":{"id":"late"}}),
        )
        .await;
        ws_read(&mut ws).await;
        send_response(&mut ws, "fresh", "good").await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let stream = client.stream(request()).unwrap();
    rx.await.unwrap();
    stream.cancel();
    release.send(()).unwrap();
    stream.close().await;
    assert!(collect(client.stream(request()).unwrap())
        .await
        .iter()
        .all(Result::is_ok));
    server.await.unwrap();
}
#[tokio::test]
async fn unsuccessful_cancel_drain_discards_connection_before_next_request() {
    let (listener, mut cfg) = ws_listener().await;
    cfg.drain_timeout = Duration::from_millis(60);
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut old = accept_async(socket).await.unwrap();
        ws_read(&mut old).await;
        ws_send(&mut old, created("old")).await;
        tx.send(()).unwrap();
        let _cancel = ws_read(&mut old).await;
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        assert!(ws_read(&mut ws).await.get("previous_response_id").is_none());
        send_response(&mut ws, "new", "good").await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let mut stream = client.stream(request()).unwrap();
    rx.await.unwrap();
    while !matches!(
        stream.recv().await,
        Some(Ok(ResponseEvent::ResponseCreated { .. }))
    ) {}
    stream.close().await;
    assert!(collect(client.stream(request()).unwrap())
        .await
        .iter()
        .all(Result::is_ok));
    server.await.unwrap();
}
#[tokio::test]
async fn stale_response_ids_and_precreated_deltas_close_connection() {
    for stale in [false, true] {
        let (listener, cfg) = ws_listener().await;
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_async(socket).await.unwrap();
            ws_read(&mut ws).await;
            if stale {
                send_response(&mut ws, "same", "first").await;
                ws_read(&mut ws).await;
                ws_send(&mut ws, created("same")).await;
            } else {
                ws_send(&mut ws, delta("out of order")).await;
            }
        });
        let client = ResponsesWebSocketClient::new(cfg).unwrap();
        if stale {
            assert!(collect(client.stream(request()).unwrap())
                .await
                .iter()
                .all(Result::is_ok));
        }
        let events = collect(client.stream(request()).unwrap()).await;
        assert!(events
            .iter()
            .any(|e| matches!(e, Err(ServiceError::Protocol(_)))));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn timeout_is_ambiguous_and_not_retried() {
    let (listener, mut cfg) = ws_listener().await;
    cfg.idle_timeout = Duration::from_millis(40);
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("silent")).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err()
        );
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Timeout))));
    server.await.unwrap();
}
#[tokio::test]
async fn byte_limit_is_enforced_by_websocket_framing() {
    let (listener, mut cfg) = ws_listener().await;
    cfg.max_event_bytes = 150;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("r")).await;
        ws_send(&mut ws, delta(&"x".repeat(200))).await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    assert!(collect(client.stream(request()).unwrap())
        .await
        .iter()
        .any(Result::is_err));
    server.await.unwrap();
}
