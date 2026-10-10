//! Regression coverage for provider rejections and response-state recovery.
use futures_util::{SinkExt, StreamExt};
use pipecat_grok_realtime::{Action, Event, RealtimeClient, RealtimeConfig, ResponseOptions};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{net::TcpListener, sync::oneshot, time::timeout};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
async fn rejected_response_create_does_not_block_next_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("mock-key");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.response_timeout = Duration::from_secs(3);
    let (seen_tx, seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        ws.send(Message::Text(json!({"event_id":"conversation","type":"conversation.created","conversation":{"id":"c"}}).to_string().into())).await.unwrap();
        let initial = ws.next().await.unwrap().unwrap();
        assert!(initial.to_text().unwrap().contains("session.update"));
        ws.send(Message::Text(json!({"event_id":"ready","type":"session.updated","session":{"id":"session","type":"realtime"}}).to_string().into())).await.unwrap();
        let request: Value =
            serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
        assert_eq!(request["type"], "response.create");
        ws.send(Message::Text(json!({"event_id":"error-1","type":"error","error":{"type":"invalid_request_error","code":"invalid_value","message":"Invalid response configuration","param":"response.instructions","event_id":request["event_id"]}}).to_string().into())).await.unwrap();
        let next = timeout(Duration::from_millis(500), ws.next()).await;
        let sent_next = matches!(next, Ok(Some(Ok(Message::Text(ref text)))) if serde_json::from_str::<Value>(text).unwrap()["type"] == "response.create");
        seen_tx.send(sent_next).unwrap();
        let _ = ws.close(None).await;
    });
    let mut session = RealtimeClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender
        .send(Action::CreateResponse(ResponseOptions::default()))
        .await
        .unwrap();
    loop {
        let event = timeout(Duration::from_secs(2), session.next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if matches!(event.event, Event::Error { .. }) {
            break;
        }
    }
    sender
        .send(Action::CreateResponse(ResponseOptions::default()))
        .await
        .unwrap();
    let next_sent = seen_rx.await.unwrap();
    let _ = session.shutdown().await;
    server.await.unwrap();
    assert!(next_sent, "provider rejected response.create, but the next response remains deferred behind a nonexistent response");
}

#[tokio::test]
async fn interruption_must_not_discard_an_ordered_finish_waiting_for_readiness() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("mock-key");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (ready_tx, ready_rx) = oneshot::channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        ws.send(Message::Text(json!({"event_id":"conversation","type":"conversation.created","conversation":{"id":"c"}}).to_string().into())).await.unwrap();
        loop {
            let value: Value =
                serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap();
            if value["type"] == "session.update" {
                break;
            }
        }
        ready_rx.await.unwrap();
        ws.send(Message::Text(json!({"event_id":"ready","type":"session.updated","session":{"id":"session","type":"realtime"}}).to_string().into())).await.unwrap();
        let close = timeout(Duration::from_millis(500), async {
            loop { let item=ws.next().await; if !matches!(&item, Some(Ok(Message::Text(text))) if serde_json::from_str::<Value>(text).is_ok_and(|v| v["type"]=="response.cancel")) { break item; } }
        }).await;
        closed_tx
            .send(matches!(close, Ok(Some(Ok(Message::Close(_)))) | Ok(None)))
            .unwrap();
        let _ = ws.close(None).await;
    });
    let mut session = RealtimeClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender.try_send(Action::Finish).unwrap();
    sender.interrupt(None);
    ready_tx.send(()).unwrap();
    let closed = closed_rx.await.unwrap();
    let _ = session.shutdown().await;
    server.await.unwrap();
    assert!(
        closed,
        "interruption discarded Finish, leaving the session open without active work"
    );
}
