//! Ordinary model refusals are visible successful results, not service failures.
//! Event fields follow the official OpenAI Responses streaming event reference.
use pipecat_core::{Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig};
use pipecat_responses::*;
use pipecat_services::Role;
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;
mod common;
use common::*;
fn refusal_done(index: u64, text: &str) -> Value {
    json!({"type":"response.refusal.done","item_id":"message","content_index":index,"refusal":text})
}
fn refusal_delta(index: u64, text: &str) -> Value {
    json!({"type":"response.refusal.delta","item_id":"message","content_index":index,"delta":text})
}
fn refused(parts: &[&str]) -> Value {
    json!({"type":"message","id":"message","role":"assistant","content":parts.iter().map(|text|json!({"type":"refusal","refusal":text})).collect::<Vec<_>>()})
}
fn finished(id: &str, item: Value) -> Value {
    json!({"type":"response.completed","response":{"id":id,"model":"gpt-4.1","output":[item]}})
}
fn text(events: &[Result<ResponseEvent, ServiceError>]) -> String {
    events
        .iter()
        .filter_map(|e| {
            if let Ok(ResponseEvent::TextDelta(text)) = e {
                Some(text.as_str())
            } else {
                None
            }
        })
        .collect()
}

#[tokio::test]
async fn refusal_parts_deduplicate_all_final_forms() {
    let item = refused(&["I cannot help. ", "Here is a safer option."]);
    let (client,_,server)=http_fixture(vec![created("r"),refusal_delta(0,"I cannot "),refusal_delta(0,"help. "),refusal_done(0,"I cannot help. "),json!({"type":"response.content_part.done","item_id":"message","content_index":0,"part":{"type":"refusal","refusal":"I cannot help. "}}),refusal_delta(1,"Here is a safer option."),refusal_done(1,"Here is a safer option."),json!({"type":"response.output_item.done","item":item}),finished("r",item)]).await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok), "{events:?}");
    assert_eq!(text(&events), "I cannot help. Here is a safer option.");
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
async fn final_only_refusal_is_visible() {
    let (client, _, server) = http_fixture(vec![
        created("r"),
        finished("r", refused(&["I cannot help with that."])),
    ])
    .await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert!(events.iter().all(Result::is_ok));
    assert_eq!(text(&events), "I cannot help with that.");
    server.await.unwrap();
}
#[tokio::test]
async fn refusal_final_mutation_fails_without_leaking_changed_content() {
    let (client, _, server) = http_fixture(vec![
        created("r"),
        refusal_done(0, "Visible refusal"),
        finished("r", refused(&["secret changed refusal"])),
    ])
    .await;
    let events = collect(client.stream(request()).unwrap()).await;
    assert_eq!(text(&events), "Visible refusal");
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Protocol(_)))));
    assert!(!format!("{events:?}").contains("secret changed"));
    server.await.unwrap();
}
#[tokio::test]
async fn one_shot_refusal_is_returned_as_visible_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        reply(
            &mut socket,
            "200 OK",
            "application/json",
            json!({"status":"completed","output":[refused(&["I cannot do that."])]})
                .to_string()
                .as_bytes(),
        )
        .await;
    });
    let result = ResponsesHttpClient::new(cfg)
        .unwrap()
        .run_inference(&request(), CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(result.as_deref(), Some("I cannot do that."));
    server.await.unwrap();
}
#[tokio::test]
async fn processor_refusal_is_a_completed_response_and_preserves_health() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        reply(
            &mut socket,
            "200 OK",
            "text/event-stream",
            &[
                record(created("r")),
                record(finished("r", refused(&["I cannot help."]))),
            ]
            .concat(),
        )
        .await;
    });
    let processor = Arc::new(ResponsesProcessor::http(cfg, request()).unwrap());
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut text = String::new();
    let mut ended = false;
    while let Some(out) = worker.recv().await {
        match out.frame.data {
            FrameData::LlmText(part) => text.push_str(&part.text),
            FrameData::LlmResponseEnd => {
                ended = true;
                assert_eq!(out.frame.metadata.get("failed"), Some(&json!(false)));
            }
            FrameData::Error(error) => panic!("unexpected refusal error: {error:?}"),
            _ => {}
        }
    }
    worker.join().await.unwrap();
    assert!(ended);
    assert_eq!(text, "I cannot help.");
    assert!(processor.health().unwrap().is_usable());
    server.await.unwrap();
}
#[tokio::test]
async fn websocket_refusal_continuation_matches_visible_assistant_history() {
    let (listener, cfg) = ws_listener().await;
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        ws_read(&mut ws).await;
        ws_send(&mut ws, created("r1")).await;
        ws_send(&mut ws, refusal_delta(0, "I cannot help.")).await;
        ws_send(&mut ws, refusal_done(0, "I cannot help.")).await;
        ws_send(&mut ws, finished("r1", refused(&["I cannot help."]))).await;
        let req = ws_read(&mut ws).await;
        assert_eq!(req["previous_response_id"], "r1");
        assert_eq!(req["input"].as_array().unwrap().len(), 1);
        ws_send(&mut ws, created("r2")).await;
        ws_send(&mut ws, delta("okay")).await;
        ws_send(&mut ws, completed("r2", "okay")).await;
    });
    let client = ResponsesWebSocketClient::new(cfg).unwrap();
    let events = collect(client.stream(request()).unwrap()).await;
    assert_eq!(text(&events), "I cannot help.");
    assert!(events.iter().all(Result::is_ok));
    let mut req = request();
    req.context.items.extend([
        InputItem::text(Role::Assistant, "I cannot help."),
        InputItem::text(Role::User, "a different question"),
    ]);
    assert!(collect(client.stream(req).unwrap())
        .await
        .iter()
        .all(Result::is_ok));
    server.await.unwrap();
}
