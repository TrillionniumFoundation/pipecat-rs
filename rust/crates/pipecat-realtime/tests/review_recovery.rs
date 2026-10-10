//! Regression coverage for provider rejections and response-state recovery.
use futures_util::{SinkExt, StreamExt};
use pipecat_realtime::{Action, Event, RealtimeClient, RealtimeConfig, ResponseOptions};
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
        let _ = ws.next().await.unwrap().unwrap();
        ready_rx.await.unwrap();
        ws.send(Message::Text(json!({"event_id":"ready","type":"session.updated","session":{"id":"session","type":"realtime"}}).to_string().into())).await.unwrap();
        let close = timeout(Duration::from_millis(500), ws.next()).await;
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

#[tokio::test]
async fn unrelated_provider_error_must_not_release_a_pending_response() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("mock-key");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let (seen_tx, seen_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let _ = ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(json!({"event_id":"ready","type":"session.updated","session":{"id":"session","type":"realtime"}}).to_string().into())).await.unwrap();
        let _ = ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(json!({"event_id":"unrelated","type":"error","error":{"type":"invalid_request_error","message":"unrelated request failed","event_id":"unknown-request"}}).to_string().into())).await.unwrap();
        let early = timeout(Duration::from_millis(100), ws.next()).await;
        assert!(
            early.is_err(),
            "unrelated error must not permit overlapping response.create"
        );
        for (kind, status) in [
            ("response.created", "in_progress"),
            ("response.done", "completed"),
        ] {
            ws.send(Message::Text(json!({"event_id":kind,"type":kind,"response":{"id":"first","status":status,"output":[]}}).to_string().into())).await.unwrap();
        }
        let next = timeout(Duration::from_secs(2), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let request: Value = serde_json::from_str(next.to_text().unwrap()).unwrap();
        seen_tx.send(request["type"] == "response.create").unwrap();
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
    assert!(seen_rx.await.unwrap());
    let _ = session.shutdown().await;
    server.await.unwrap();
}

#[tokio::test]
async fn interruption_during_deferred_response_write_cannot_relabel_stale_output() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("mock-key");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.max_message_bytes = 16 * 1024 * 1024;
    let (done_tx, done_rx) = oneshot::channel();
    let (writing_tx, writing_rx) = oneshot::channel();
    let (resume_tx, resume_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let _ = ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(json!({"event_id":"ready","type":"session.updated","session":{"id":"session","type":"realtime"}}).to_string().into())).await.unwrap();
        let _ = ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(json!({"event_id":"created-1","type":"response.created","response":{"id":"first","status":"in_progress","output":[]}}).to_string().into())).await.unwrap();
        done_rx.await.unwrap();
        ws.send(Message::Text(json!({"event_id":"done-1","type":"response.done","response":{"id":"first","status":"completed","output":[]}}).to_string().into())).await.unwrap();
        // A partial multi-megabyte request is now in the TCP receive buffer, so
        // the actor has started the deferred write but cannot have completed it.
        let mut byte = [0u8; 1];
        assert_eq!(ws.get_ref().peek(&mut byte).await.unwrap(), 1);
        writing_tx.send(()).unwrap();
        resume_rx.await.unwrap();
        let request = timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(request.to_text().unwrap()).unwrap()["type"],
            "response.create"
        );
        ws.send(Message::Text(json!({"event_id":"created-2","type":"response.created","response":{"id":"stale","status":"in_progress","output":[]}}).to_string().into())).await.unwrap();
        ws.send(Message::Text(json!({"event_id":"stale-audio","type":"response.output_audio.delta","response_id":"stale","item_id":"audio","output_index":0,"content_index":0,"delta":"AAA="}).to_string().into())).await.unwrap();
        ws.send(Message::Text(json!({"event_id":"done-2","type":"response.done","response":{"id":"stale","status":"cancelled","output":[]}}).to_string().into())).await.unwrap();
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
    while !matches!(
        session.next_event().await.unwrap().unwrap().event,
        Event::ResponseCreated { .. }
    ) {}
    sender
        .send(Action::CreateResponse(ResponseOptions {
            instructions: Some("x".repeat(8 * 1024 * 1024)),
            ..Default::default()
        }))
        .await
        .unwrap();
    done_tx.send(()).unwrap();
    writing_rx.await.unwrap();
    sender.interrupt(None);
    resume_tx.send(()).unwrap();
    let mut stale_audio = false;
    while let Some(event) = timeout(Duration::from_secs(5), session.next_event())
        .await
        .unwrap()
    {
        if let Ok(event) = event {
            stale_audio |= matches!(event.event, Event::AudioDelta { .. });
        }
    }
    server.await.unwrap();
    assert!(!stale_audio, "a deferred pre-interruption response was tagged with the new generation after its write completed");
}
