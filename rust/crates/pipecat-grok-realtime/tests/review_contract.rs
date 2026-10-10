//! Independent provider-contract regressions. All peers are synthetic localhost WebSockets.
use futures_util::{SinkExt, StreamExt};
use pipecat_grok_realtime::*;
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
type Ws = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(3);
async fn send(ws: &mut Ws, mut value: Value) {
    value["event_id"] = json!("review-event");
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn read(ws: &mut Ws) -> Value {
    let message = timeout(WAIT, ws.next())
        .await
        .expect("peer read deadline")
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn quiet(ws: &mut Ws) {
    assert!(
        timeout(Duration::from_millis(80), ws.next()).await.is_err(),
        "unexpected wire action"
    );
}
async fn handshake(ws: &mut Ws) {
    send(
        ws,
        json!({"type":"session.created","session":{"id":"session"}}),
    )
    .await;
    send(
        ws,
        json!({"type":"conversation.created","conversation":{"id":"conversation"}}),
    )
    .await;
    let update = read(ws).await;
    assert_eq!(update["type"], "session.update");
    assert_eq!(update["session"]["voice"], "eve");
    assert_eq!(update["session"]["turn_detection"]["type"], "server_vad");
    assert!(update["session"]["audio"]["input"]
        .get("turn_detection")
        .is_none());
    send(
        ws,
        json!({"type":"session.updated","session":{"id":"session"}}),
    )
    .await;
}
async fn peer<F, Fut>(f: F) -> (RealtimeConfig, JoinHandle<()>)
where
    F: FnOnce(Ws) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("review-only-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    config.session.tools = Some(vec![FunctionTool::new(
        "echo",
        "Echo",
        json!({"type":"object"}),
    )
    .into()]);
    config.limits.connect_timeout = WAIT;
    config.limits.operation_timeout = WAIT;
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        f(ws).await;
    });
    (config, task)
}
async fn next(session: &mut RealtimeSession) -> RealtimeEvent {
    timeout(WAIT, session.next_event())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn ready(config: RealtimeConfig) -> RealtimeSession {
    let mut session = RealtimeClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    for _ in 0..3 {
        next(&mut session).await;
    }
    session
}
async fn response(ws: &mut Ws, id: &str, done: bool) {
    send(ws, json!({"type":if done {"response.done"} else {"response.created"},"response":{"id":id,"status":if done {"completed"} else {"in_progress"},"output":[]}})).await;
}
async fn call(ws: &mut Ws, id: &str, name: &str) {
    send(ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":format!("item-{id}"),"call_id":id,"name":name,"arguments":""}})).await;
    // The pinned provider deliberately omits response_id and item_id here.
    send(ws, json!({"type":"response.function_call_arguments.done","call_id":id,"name":name,"arguments":"{}"})).await;
}
async fn admitted_call(session: &mut RealtimeSession) {
    loop {
        if matches!(
            next(session).await.event,
            Event::FunctionArgumentsDone { .. }
        ) {
            break;
        }
    }
}
#[tokio::test]
async fn review_pre_ready_audio_never_replays_and_initial_update_waits_for_conversation() {
    let (release, wait) = tokio::sync::oneshot::channel();
    let (config, server) = peer(|mut ws| async move {
        quiet(&mut ws).await;
        send(
            &mut ws,
            json!({"type":"session.created","session":{"id":"s"}}),
        )
        .await;
        wait.await.unwrap();
        quiet(&mut ws).await;
        send(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"c"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        send(
            &mut ws,
            json!({"type":"session.updated","session":{"id":"s"}}),
        )
        .await;
        let audio = read(&mut ws).await;
        assert_eq!(audio["type"], "input_audio_buffer.append");
        assert_eq!(audio["audio"], "AwQ=");
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = RealtimeClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    next(&mut session).await;
    assert!(
        session
            .sender()
            .try_send(Action::AppendAudio(vec![1, 2]))
            .is_err(),
        "pre-ready audio was buffered"
    );
    release.send(()).unwrap();
    for _ in 0..2 {
        next(&mut session).await;
    }
    session
        .sender()
        .send(Action::AppendAudio(vec![3, 4]))
        .await
        .unwrap();
    server.await.unwrap();
    session.shutdown().await.unwrap();
}
#[tokio::test]
async fn review_results_deliver_immediately_but_only_explicit_inference_runs() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "r1", false).await;
        call(&mut ws, "call", "echo").await;
        let interim = read(&mut ws).await;
        assert_eq!(interim["item"]["type"], "message");
        let envelope: Value =
            serde_json::from_str(interim["item"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(envelope["tool_call_id"], "call");
        let final_result = read(&mut ws).await;
        assert_eq!(final_result["item"]["type"], "function_call_output");
        assert_eq!(final_result["item"]["output"], "{\"answer\":42}");
        quiet(&mut ws).await;
        response(&mut ws, "r1", true).await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "r2", false).await;
        response(&mut ws, "r2", true).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = ready(config).await;
    let sender = session.sender();
    sender
        .send(Action::CreateResponse(ResponseOptions::default()))
        .await
        .unwrap();
    admitted_call(&mut session).await;
    sender
        .send(Action::ToolResult {
            call_id: "call".into(),
            output: json!({"progress":1}),
            is_final: false,
        })
        .await
        .unwrap();
    sender
        .send(Action::ToolResult {
            call_id: "call".into(),
            output: json!("{\"answer\":42}"),
            is_final: true,
        })
        .await
        .unwrap();
    loop {
        if matches!(next(&mut session).await.event, Event::ResponseDone { .. }) {
            break;
        }
    }
    sender
        .send(Action::RequestInference(ResponseOptions::default()))
        .await
        .unwrap();
    for _ in 0..2 {
        next(&mut session).await;
    }
    server.await.unwrap();
    session.shutdown().await.unwrap();
}
#[tokio::test]
async fn review_announced_async_result_survives_interruption_without_inference() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        read(&mut ws).await;
        response(&mut ws, "r", false).await;
        call(&mut ws, "async", "echo").await;
        assert_eq!(read(&mut ws).await["type"], "response.cancel");
        let result = read(&mut ws).await;
        assert_eq!(result["item"]["call_id"], "async");
        assert_eq!(result["item"]["output"], "7");
        response(&mut ws, "r", true).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = ready(config).await;
    let sender = session.sender();
    sender
        .send(Action::CreateResponse(ResponseOptions::default()))
        .await
        .unwrap();
    admitted_call(&mut session).await;
    sender.interrupt(None);
    sender
        .send(Action::ToolResult {
            call_id: "async".into(),
            output: json!(7),
            is_final: true,
        })
        .await
        .unwrap();
    server.await.unwrap();
    session.shutdown().await.unwrap();
}
#[tokio::test]
async fn review_rejected_create_never_retries_owed_inference() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        let first = read(&mut ws).await;
        assert_eq!(first["type"], "conversation.item.create");
        let create = read(&mut ws).await;
        assert_eq!(create["type"], "response.create");
        // Give the explicit run request time to be queued behind the pending create.
        tokio::time::sleep(Duration::from_millis(50)).await;
        send(&mut ws, json!({"type":"error","error":{"message":"create rejected","code":"bad_request","event_id":create["event_id"]}})).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    }).await;
    let mut session = ready(config).await;
    let sender = session.sender();
    sender
        .send(Action::CreateItem {
            item: ConversationItem::Message {
                id: "u".into(),
                role: Role::User,
                content: vec![ItemContent::InputText { text: "q".into() }],
            },
            previous_item_id: None,
        })
        .await
        .unwrap();
    sender
        .send(Action::CreateResponse(ResponseOptions::default()))
        .await
        .unwrap();
    sender
        .send(Action::RequestInference(ResponseOptions::default()))
        .await
        .unwrap();
    assert!(matches!(
        next(&mut session).await.event,
        Event::Error { .. }
    ));
    server.await.unwrap();
    session.shutdown().await.unwrap();
}
#[tokio::test]
async fn review_undeclared_function_never_admitted() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        read(&mut ws).await;
        response(&mut ws, "r", false).await;
        call(&mut ws, "bad", "hidden_effect").await;
        let _ = ws.next().await;
    })
    .await;
    let mut session = ready(config).await;
    session
        .sender()
        .send(Action::CreateResponse(ResponseOptions::default()))
        .await
        .unwrap();
    let mut rejected = false;
    while let Some(event) = timeout(WAIT, session.next_event()).await.unwrap() {
        match event {
            Err(_) => rejected = true,
            Ok(event) => assert!(
                !matches!(event.event, Event::FunctionArgumentsDone { .. }),
                "undeclared tool admitted"
            ),
        }
    }
    assert!(rejected);
    server.await.unwrap();
}
#[tokio::test]
async fn review_retained_provider_identity_is_bounded() {
    let (config, server) = peer(|mut ws| async move {
        send(
            &mut ws,
            json!({"type":"session.created","session":{"id":"x".repeat(65536)}}),
        )
        .await;
        let _ = ws.next().await;
    })
    .await;
    let mut session = RealtimeClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert!(
        timeout(WAIT, session.next_event())
            .await
            .unwrap()
            .unwrap()
            .is_err(),
        "oversized provider identity retained"
    );
    server.await.unwrap();
}
#[tokio::test]
async fn review_force_message_cancel_without_response_created_still_cancels() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        let force = read(&mut ws).await;
        assert_eq!(force["item"]["type"], "force_message");
        assert_eq!(read(&mut ws).await["type"], "response.cancel");
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = ready(config).await;
    let sender = session.sender();
    sender
        .send(Action::CreateItem {
            item: ConversationItem::ForceMessage {
                id: "forced".into(),
                role: Role::Assistant,
                content: vec![ItemContent::Text {
                    text: "hello".into(),
                }],
            },
            previous_item_id: None,
        })
        .await
        .unwrap();
    sender.interrupt(None);
    server.await.unwrap();
    session.shutdown().await.unwrap();
}
#[tokio::test]
async fn review_response_created_consumes_earlier_material_and_does_not_run_debt_twice() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        assert_eq!(read(&mut ws).await["type"], "conversation.item.create");
        assert_eq!(read(&mut ws).await["type"], "response.create");
        assert_eq!(read(&mut ws).await["type"], "conversation.item.create");
        tokio::time::sleep(Duration::from_millis(40)).await;
        response(&mut ws, "r", false).await;
        response(&mut ws, "r", true).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = ready(config).await;
    let sender = session.sender();
    for (index, text) in ["first", "second"].into_iter().enumerate() {
        sender
            .send(Action::CreateItem {
                item: ConversationItem::Message {
                    id: format!("u{index}"),
                    role: Role::User,
                    content: vec![ItemContent::InputText { text: text.into() }],
                },
                previous_item_id: None,
            })
            .await
            .unwrap();
        if index == 0 {
            sender
                .send(Action::CreateResponse(ResponseOptions::default()))
                .await
                .unwrap();
        }
    }
    sender
        .send(Action::RequestInference(ResponseOptions::default()))
        .await
        .unwrap();
    for _ in 0..2 {
        next(&mut session).await;
    }
    server.await.unwrap();
    session.shutdown().await.unwrap();
}
#[tokio::test]
async fn review_tool_admission_uses_response_snapshot_across_session_update() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "r", false).await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        call(&mut ws, "bad", "newly_advertised").await;
        let _ = ws.next().await;
    })
    .await;
    let mut session = ready(config).await;
    let sender = session.sender();
    sender
        .send(Action::CreateResponse(ResponseOptions::default()))
        .await
        .unwrap();
    next(&mut session).await;
    let mut update = SessionProperties::new();
    update.tools = Some(vec![FunctionTool::new(
        "newly_advertised",
        "New",
        json!({"type":"object"}),
    )
    .into()]);
    sender
        .send(Action::UpdateSession(update.into()))
        .await
        .unwrap();
    let mut rejected = false;
    while let Some(event) = timeout(WAIT, session.next_event()).await.unwrap() {
        match event {
            Err(_) => rejected = true,
            Ok(event) => assert!(
                !matches!(event.event, Event::FunctionArgumentsDone { .. }),
                "later settings admitted a call from earlier response"
            ),
        }
    }
    assert!(rejected);
    server.await.unwrap();
}
#[tokio::test]
async fn review_source_null_fields_cumulative_transcripts_and_text_audio_views() {
    let (config,server)=peer(|mut ws|async move{
        handshake(&mut ws).await;
        response(&mut ws,"r",false).await;
        for event in [
            json!({"type":"ping","timestamp":123}),
            json!({"type":"response.content_part.added","response_id":"r","item_id":"i","output_index":0,"content_index":0,"part":{"type":"text","transcript":null}}),
            json!({"type":"response.text.delta","response_id":null,"item_id":null,"output_index":null,"content_index":null,"delta":"visible text"}),
            json!({"type":"response.output_audio_transcript.delta","response_id":"r","item_id":"i","delta":"spoken text"}),
            json!({"type":"conversation.item.input_audio_transcription.updated","item_id":"u","content_index":null,"transcript":" good "}),
            json!({"type":"conversation.item.input_audio_transcription.updated","item_id":"u","transcript":" good morning "}),
            json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u","transcript":"good morning"}),
            json!({"type":"mcp_list_tools.completed","tools":[{"name":"remote-observation"}]}),
            json!({"type":"input_audio_buffer.timeout_triggered","item_id":"u"}),
        ]{send(&mut ws,event).await;}
        response(&mut ws,"r",true).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    }).await;
    let mut session = ready(config).await;
    let mut mapper = RealtimeFrameMapper::new("user");
    let mut frames = Vec::new();
    let mut observed = Vec::new();
    loop {
        let event = next(&mut session).await;
        observed.push(event.event_type.clone());
        frames.extend(mapper.frames(&event).unwrap());
        if matches!(event.event, Event::ResponseDone { .. }) {
            break;
        }
    }
    assert!(observed.iter().any(|e| e == "ping"));
    assert!(observed.iter().any(|e| e == "response.content_part.added"));
    assert!(observed.iter().any(|e| e == "mcp_list_tools.completed"));
    assert!(observed
        .iter()
        .any(|e| e == "input_audio_buffer.timeout_triggered"));
    let interim: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            pipecat_core::FrameData::InterimTranscription(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(interim, ["good", "good morning"]);
    assert!(frames.iter().any(|f|matches!(&f.data,pipecat_core::FrameData::LlmText(t) if t.text=="visible text"&&t.append_to_context)));
    assert!(frames.iter().any(|f|matches!(&f.data,pipecat_core::FrameData::LlmText(t) if t.text=="spoken text"&&!t.append_to_context)));
    let spoken: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            pipecat_core::FrameData::TtsText(t) => Some(t.text.text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(spoken, ["spoken text"]);
    server.await.unwrap();
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn review_explicit_cancellation_settles_only_matched_call_once_and_preserves_sibling() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "r", false).await;
        call(&mut ws, "cancelled", "echo").await;
        call(&mut ws, "survivor", "echo").await;
        let cancelled = read(&mut ws).await;
        assert_eq!(cancelled["item"]["call_id"], "cancelled");
        assert_eq!(cancelled["item"]["output"], "CANCELLED");
        let survivor = read(&mut ws).await;
        assert_eq!(survivor["item"]["call_id"], "survivor");
        assert_eq!(survivor["item"]["output"], "7");
        quiet(&mut ws).await;
        response(&mut ws, "r", true).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = ready(config).await;
    let sender = session.sender();
    sender
        .send(Action::CreateResponse(ResponseOptions::default()))
        .await
        .unwrap();
    admitted_call(&mut session).await;
    admitted_call(&mut session).await;
    assert!(sender
        .send(Action::CancelTool {
            call_id: "cancelled".into(),
            function_name: "wrong-name".into()
        })
        .await
        .is_err());
    assert!(sender
        .send(Action::CancelTool {
            call_id: "unknown".into(),
            function_name: "echo".into()
        })
        .await
        .is_err());
    for _ in 0..2 {
        sender
            .send(Action::CancelTool {
                call_id: "cancelled".into(),
                function_name: "echo".into(),
            })
            .await
            .unwrap();
    }
    sender
        .send(Action::ToolResult {
            call_id: "cancelled".into(),
            output: json!("late successful result"),
            is_final: true,
        })
        .await
        .unwrap();
    sender
        .send(Action::ToolResult {
            call_id: "survivor".into(),
            output: json!(7),
            is_final: true,
        })
        .await
        .unwrap();
    assert!(matches!(
        next(&mut session).await.event,
        Event::ResponseDone { .. }
    ));
    server.await.unwrap();
    session.shutdown().await.unwrap();
}
