use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::{Frame, FrameData, Pipeline, PipelineConfig};
use pipecat_grok_realtime::*;
use serde_json::{json, Value};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{
    tungstenite::{
        protocol::frame::{
            coding::{Data, OpCode},
            Frame as WsFrame,
        },
        Message,
    },
    WebSocketStream,
};

type Ws = WebSocketStream<TcpStream>;
#[allow(clippy::result_large_err)] // tungstenite fixes the handshake callback error type.
async fn server<F, Fut>(f: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(Ws) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_hdr_async(
            tcp,
            |req: &tungstenite_request::Request, response| {
                assert_eq!(req.headers()["authorization"], "Bearer test-key");
                assert!(!req.headers().contains_key("openai-beta"));
                assert_eq!(req.uri().query(), Some("model=grok-voice-latest"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        assert!(timeout(Duration::from_millis(20), ws.next()).await.is_err());
        send(
            &mut ws,
            json!({"type":"session.created","session":{"id":"sess_1"}}),
        )
        .await;
        assert!(timeout(Duration::from_millis(20), ws.next()).await.is_err());
        send(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"conv_1"}}),
        )
        .await;
        let update = read(&mut ws).await;
        assert_eq!(update["type"], "session.update");
        assert!(update["session"].get("type").is_none());
        assert_eq!(update["session"]["voice"], "eve");
        assert!(update["session"].get("turn_detection").is_some());
        assert!(update["session"]["audio"]["input"]
            .get("turn_detection")
            .is_none());
        send(
            &mut ws,
            json!({"type":"session.updated","session":{"id":"sess_1","type":"realtime"}}),
        )
        .await;
        f(ws).await;
    });
    (url, task)
}
mod tungstenite_request {
    pub type Request = tokio_tungstenite::tungstenite::handshake::server::Request;
}
async fn read(ws: &mut Ws) -> Value {
    let msg = timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(msg.to_text().unwrap()).unwrap()
}
async fn send(ws: &mut Ws, mut v: Value) {
    v["event_id"] = json!("evt_test");
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}
fn response(id: &str, status: &str) -> Value {
    json!({"id":id,"status":status,"conversation_id":"conv_1","output":[]})
}
async fn created(ws: &mut Ws, id: &str) {
    send(
        ws,
        json!({"type":"response.created","response":response(id,"in_progress")}),
    )
    .await;
}
async fn done(ws: &mut Ws, id: &str) {
    send(
        ws,
        json!({"type":"response.done","response":response(id,"completed")}),
    )
    .await;
}
fn delta(id: &str, item: &str, audio: &[u8]) -> Value {
    json!({"type":"response.output_audio.delta","response_id":id,"item_id":item,"output_index":0,"content_index":0,"delta":STANDARD.encode(audio)})
}
fn config(url: String) -> RealtimeConfig {
    let mut c = RealtimeConfig::new("test-key");
    c.endpoint = url;
    c.context_id = Some("ctx_global".into());
    c.session.tools = Some(
        ["first", "second", "lookup"]
            .iter()
            .map(|name| FunctionTool::new(*name, "test", json!({"type":"object"})).into())
            .collect(),
    );
    c
}
async fn connect(url: String) -> RealtimeSession {
    RealtimeClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap()
}
async fn next(s: &mut RealtimeSession) -> RealtimeEvent {
    timeout(Duration::from_secs(5), s.next_event())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn drain_initial(s: &mut RealtimeSession) {
    for _ in 0..3 {
        next(s).await;
    }
}

#[tokio::test]
async fn authenticated_grok_audio_text_transcription_and_fragmented_wire() {
    let (url, task) = server(|mut ws| async move {
        let append = read(&mut ws).await;
        assert_eq!(append["type"], "input_audio_buffer.append");
        assert_eq!(STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(), vec![1,2,3,4]);
        assert_eq!(read(&mut ws).await["type"], "input_audio_buffer.commit");
        assert_eq!(read(&mut ws).await["type"], "response.create");
        created(&mut ws, "r1").await;
        let mut payload = delta("r1", "a", &[1,2,3]); payload["event_id"] = json!("fragmented");
        let text = payload.to_string().into_bytes(); let mid = text.len()/2;
        ws.send(Message::Frame(WsFrame::message(text[..mid].to_vec(), OpCode::Data(Data::Text), false))).await.unwrap();
        ws.send(Message::Frame(WsFrame::message(text[mid..].to_vec(), OpCode::Data(Data::Continue), true))).await.unwrap();
        send(&mut ws, delta("r1", "a", &[4])).await;
        send(&mut ws, json!({"type":"response.output_audio.done","response_id":"r1","item_id":"a","output_index":0,"content_index":0})).await;
        send(&mut ws, delta("r1", "b", &[5,6])).await;
        send(&mut ws, json!({"type":"response.output_text.delta","response_id":"r1","item_id":"b","output_index":1,"content_index":0,"delta":"hello"})).await;
        send(&mut ws, json!({"type":"response.output_audio_transcript.delta","response_id":"r1","item_id":"b","output_index":1,"content_index":0,"delta":"spoken"})).await;
        send(&mut ws, json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"user1","content_index":0,"transcript":"question"})).await;
        let mut r = response("r1", "completed"); r["usage"] = json!({"total_tokens":12,"input_tokens":7,"output_tokens":5});
        send(&mut ws, json!({"type":"response.done","response":r})).await;
        let _ = ws.close(None).await;
    }).await;
    let mut s = connect(url).await;
    let tx = s.sender();
    drain_initial(&mut s).await;
    tx.send(Action::AppendAudio(vec![1, 2, 3, 4]))
        .await
        .unwrap();
    tx.send(Action::CommitAudio).await.unwrap();
    tx.send(Action::CreateResponse(ResponseOptions {
        context_id: Some("turn_42".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    let mut mapper = RealtimeFrameMapper::new("alice");
    let mut frames = vec![];
    while let Some(event) = timeout(Duration::from_secs(5), s.next_event())
        .await
        .unwrap()
    {
        let event = event.unwrap();
        if event.event.response_id().is_some() {
            assert_eq!(event.context_id.as_deref(), Some("turn_42"));
            assert_eq!(event.conversation_id.as_deref(), Some("conv_1"));
            assert_eq!(event.session_id.as_deref(), Some("sess_1"));
        }
        frames.extend(mapper.frames(&event).unwrap());
    }
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::TtsStopped(_)))
            .count(),
        1
    );
    let pcm: Vec<u8> = frames
        .iter()
        .filter_map(|f| {
            if let FrameData::TtsAudio(a) = &f.data {
                Some(a.audio.clone())
            } else {
                None
            }
        })
        .flatten()
        .collect();
    assert_eq!(pcm, vec![1, 2, 3, 4, 5, 6]);
    assert!(frames.iter().any(|f| matches!(&f.data,FrameData::Transcription(t) if t.text=="question" && t.user_id=="alice")));
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(&f.data, FrameData::LlmText(_)))
            .count(),
        2
    );
    assert!(frames
        .iter()
        .any(|f| matches!(&f.data, FrameData::Metrics(_))));
    task.await.unwrap();
}

#[tokio::test]
async fn interruption_suppresses_queued_and_late_output_and_truncates_exact_item() {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (url, task) = server(|mut ws| async move {
        assert_eq!(read(&mut ws).await["type"],"response.create");
        created(&mut ws,"old").await; send(&mut ws,delta("old","old_item",&[1,2])).await;
        ready_tx.send(()).unwrap();
        assert_eq!(read(&mut ws).await["type"],"response.cancel");
        let truncate=read(&mut ws).await; assert_eq!(truncate["type"],"conversation.item.truncate");
        assert_eq!(truncate["item_id"],"old_item"); assert_eq!(truncate["content_index"],2); assert_eq!(truncate["audio_end_ms"],120);
        send(&mut ws,delta("old","old_item",&[3,4])).await; done(&mut ws,"old").await;
        // Let the client know cancellation completed without exposing stale output.
        send(&mut ws,json!({"type":"conversation.item.truncated","item_id":"old_item","content_index":2,"audio_end_ms":120})).await;
        assert_eq!(read(&mut ws).await["type"],"response.create");
        created(&mut ws,"new").await; send(&mut ws,delta("new","new_item",&[5,6])).await; done(&mut ws,"new").await;
        let _=ws.close(None).await;
    }).await;
    let mut s = connect(url).await;
    drain_initial(&mut s).await;
    let tx = s.sender();
    tx.send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    ready_rx.await.unwrap();
    tx.interrupt(Some(PlaybackPosition {
        item_id: "old_item".into(),
        content_index: 2,
        audio_end_ms: 120,
    }));
    loop {
        let e = next(&mut s).await;
        assert_ne!(e.event.response_id(), Some("old"));
        if matches!(e.event, Event::ItemTruncated { .. }) {
            break;
        }
    }
    tx.send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut audio = vec![];
    while let Some(event) = s.next_event().await {
        let event = event.unwrap();
        assert_ne!(event.event.response_id(), Some("old"));
        if let Event::AudioDelta { audio: a, .. } = event.event {
            audio.extend(a);
        }
    }
    assert_eq!(audio, vec![5, 6]);
    task.await.unwrap();
}

async fn tool(ws: &mut Ws, call: &str, name: &str) {
    send(ws,json!({"type":"response.output_item.added","response_id":"tools","output_index":0,"item":{"type":"function_call","id":format!("item_{call}"),"call_id":call,"name":name,"arguments":""}})).await;
    send(ws,json!({"type":"response.function_call_arguments.delta","response_id":"tools","item_id":format!("item_{call}"),"output_index":0,"call_id":call,"delta":"{\"x\":"})).await;
    send(ws,json!({"type":"response.function_call_arguments.done","response_id":"tools","item_id":format!("item_{call}"),"output_index":0,"call_id":call,"name":name,"arguments":"{\"x\":1}"})).await;
}
#[tokio::test]
async fn parallel_tool_outputs_wait_for_explicit_aggregator_inference() {
    let (url, task) = server(|mut ws| async move {
        assert_eq!(read(&mut ws).await["type"], "response.create");
        created(&mut ws, "tools").await;
        tool(&mut ws, "a", "first").await;
        tool(&mut ws, "b", "second").await;
        let partial = read(&mut ws).await;
        assert_eq!(partial["item"]["type"], "message");
        let payload: Value =
            serde_json::from_str(partial["item"]["content"][0]["text"].as_str().unwrap()).unwrap();
        assert_eq!(payload["type"], "async_tool");
        assert_eq!(payload["status"], "running");
        assert_eq!(payload["result"], "partial");
        let a = read(&mut ws).await;
        assert_eq!(a["item"]["call_id"], "a");
        assert_eq!(a["item"]["output"], "plain");
        assert!(timeout(Duration::from_millis(50), ws.next()).await.is_err());
        done(&mut ws, "tools").await;
        let b = read(&mut ws).await;
        assert_eq!(b["item"]["call_id"], "b");
        assert_eq!(b["item"]["output"], "{\"ok\":true}");
        assert!(timeout(Duration::from_millis(40), ws.next()).await.is_err());
        send(&mut ws, json!({"type":"input_audio_buffer.cleared"})).await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        created(&mut ws, "continued").await;
        done(&mut ws, "continued").await;
        let _ = ws.close(None).await;
    })
    .await;
    let mut s = connect(url).await;
    let tx = s.sender();
    tx.send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut calls = vec![];
    while calls.len() < 2 {
        let e = next(&mut s).await;
        if let Event::FunctionArgumentsDone { name, call_id, .. } = e.event {
            calls.push((name.unwrap(), call_id));
        }
    }
    assert_eq!(
        calls,
        vec![("first".into(), "a".into()), ("second".into(), "b".into())]
    );
    tx.send(Action::ToolResult {
        call_id: "a".into(),
        output: json!("partial"),
        is_final: false,
    })
    .await
    .unwrap();
    tx.send(Action::ToolResult {
        call_id: "a".into(),
        output: json!("plain"),
        is_final: true,
    })
    .await
    .unwrap();
    assert_eq!(
        tx.send(Action::ToolResult {
            call_id: "a".into(),
            output: json!("duplicate"),
            is_final: true,
        })
        .await,
        Ok(())
    );
    loop {
        if matches!(next(&mut s).await.event, Event::ResponseDone { .. }) {
            break;
        }
    }
    tx.send(Action::ToolResult {
        call_id: "b".into(),
        output: json!({"ok":true}),
        is_final: true,
    })
    .await
    .unwrap();
    loop {
        if matches!(next(&mut s).await.event, Event::AudioCleared) {
            break;
        }
    }
    tx.send(Action::RequestInference(Default::default()))
        .await
        .unwrap();
    let mut continued = 0;
    while let Some(e) = s.next_event().await {
        if matches!(e.unwrap().event, Event::ResponseCreated { .. }) {
            continued += 1;
        }
    }
    assert_eq!(continued, 1);
    task.await.unwrap();
}

#[tokio::test]
async fn provider_errors_redact_credentials() {
    let (url,task)=server(|mut ws| async move {
        send(&mut ws,json!({"type":"error","error":{"type":"invalid_request_error","code":"bad","message":"echo test-key"}})).await;
        let _=ws.close(None).await;
    }).await;
    let mut s = connect(url).await;
    drain_initial(&mut s).await;
    let e = next(&mut s).await;
    assert!(!format!("{e:?}").contains("test-key"));
    assert!(matches!(e.event,Event::Error{error} if error.message=="echo [REDACTED]"));
    task.await.unwrap();
}

#[tokio::test]
async fn malformed_event_is_terminal_and_remote_eof_during_response_is_error() {
    let (url, task) = server(|mut ws| async move {
        ws.send(Message::Text("{bad".into())).await.unwrap();
    })
    .await;
    let mut s = connect(url).await;
    drain_initial(&mut s).await;
    assert!(matches!(
        s.next_event().await,
        Some(Err(RealtimeError::Protocol(_)))
    ));
    task.await.unwrap();
    let (url, task) = server(|mut ws| async move {
        read(&mut ws).await;
        created(&mut ws, "cut").await;
        let _ = ws.close(None).await;
    })
    .await;
    let mut s = connect(url).await;
    s.sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut error = None;
    while let Some(e) = s.next_event().await {
        if let Err(e) = e {
            error = Some(e);
        }
    }
    assert!(matches!(error, Some(RealtimeError::Protocol(_))));
    task.await.unwrap();
}

#[tokio::test]
async fn response_deadline_and_bounded_output_queue_are_enforced() {
    let (url, task) = server(|mut ws| async move {
        read(&mut ws).await;
        created(&mut ws, "slow").await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    })
    .await;
    let mut c = config(url);
    c.limits.response_timeout = Duration::from_millis(30);
    let mut s = RealtimeClient::new(c).unwrap().connect().await.unwrap();
    s.sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut error = None;
    while let Some(e) = s.next_event().await {
        if let Err(e) = e {
            error = Some(e);
        }
    }
    assert_eq!(error, Some(RealtimeError::Timeout("response")));
    task.await.unwrap();
    let (url, task) = server(|mut ws| async move {
        for _ in 0..10 {
            if ws
                .send(Message::Text(
                    json!({"event_id":"e","type":"input_audio_buffer.cleared"})
                        .to_string()
                        .into(),
                ))
                .await
                .is_err()
            {
                break;
            }
        }
    })
    .await;
    let mut c = config(url);
    c.limits.queue_capacity = 3;
    let mut s = RealtimeClient::new(c).unwrap().connect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    let mut error = None;
    while let Some(e) = s.next_event().await {
        if let Err(e) = e {
            error = Some(e);
        }
    }
    assert_eq!(error, Some(RealtimeError::Backpressure));
    task.await.unwrap();
}

#[test]
fn security_configuration_and_pcm_limits() {
    for endpoint in [
        "ws://example.com/v1/realtime",
        "ws://localhost/v1/realtime",
        "wss://user:password@example.com/v1/realtime",
        "wss://example.com?api_key=secret",
        "https://example.com",
    ] {
        let mut c = RealtimeConfig::new("top-secret");
        c.endpoint = endpoint.into();
        assert!(c.validate().is_err());
        assert!(!format!("{c:?}").contains("top-secret"));
    }
    let mut c = RealtimeConfig::new("key\r\nheader");
    assert!(c.validate().is_err());
    c = RealtimeConfig::new("safe");
    c.session
        .audio
        .as_mut()
        .unwrap()
        .input
        .as_mut()
        .unwrap()
        .format = AudioFormat::Pcm { rate: 12_345 };
    assert!(c.validate().is_err());
    assert_eq!(
        serde_json::to_value(SessionProperties::pcm()).unwrap()["voice"],
        "eve"
    );
}

#[tokio::test]
async fn redirect_is_not_followed_and_response_bodies_are_not_leaked() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let destination = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let target = destination.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 4096];
        let mut used = 0;
        while !buffer[..used].windows(4).any(|w| w == b"\r\n\r\n") {
            let count = tcp.read(&mut buffer[used..]).await.unwrap();
            assert!(count > 0);
            used += count;
            assert!(used < buffer.len());
        }
        let response=format!("HTTP/1.1 302 Found\r\nLocation: ws://{target}/stolen\r\nContent-Length: 8\r\n\r\ntest-key");
        tcp.write_all(response.as_bytes()).await.unwrap();
    });
    let result = RealtimeClient::new(config(url)).unwrap().connect().await;
    let error = match result {
        Err(e) => e,
        Ok(_) => panic!("redirect followed"),
    };
    assert_eq!(error, RealtimeError::Http(302));
    assert!(!error.to_string().contains("test-key"));
    assert!(timeout(Duration::from_millis(50), destination.accept())
        .await
        .is_err());
    task.await.unwrap();
}

#[tokio::test]
async fn frame_pipeline_emits_lifecycle_audio_and_closes_on_end() {
    let (url, task) = server(|mut ws| async move {
        let item = read(&mut ws).await;
        assert_eq!(item["type"], "conversation.item.create");
        assert_eq!(item["item"]["content"][0]["text"], "hello");
        read(&mut ws).await;
        created(&mut ws, "pipeline").await;
        send(&mut ws, delta("pipeline", "audio", &[1, 2])).await;
        done(&mut ws, "pipeline").await;
        let close = timeout(Duration::from_secs(5), ws.next()).await.unwrap();
        assert!(matches!(close, Some(Ok(Message::Close(_))) | None));
    })
    .await;
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config(url)).unwrap()).unwrap());
    let mut cfg = PipelineConfig::default();
    cfg.start.audio_in_sample_rate = 24_000;
    cfg.start.audio_out_sample_rate = 24_000;
    let mut worker = Pipeline::new(vec![processor]).start(cfg).await.unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputText(pipecat_core::TextFrame::new("hello"))),
            pipecat_core::Direction::Downstream,
        )
        .await
        .unwrap();
    let mut kinds = vec![];
    loop {
        let f = timeout(Duration::from_secs(5), worker.recv())
            .await
            .unwrap()
            .unwrap();
        let done = matches!(f.frame.data, FrameData::LlmResponseEnd);
        kinds.push(f.frame.data);
        if done {
            break;
        }
    }
    assert!(kinds.iter().any(|d| matches!(d, FrameData::TtsAudio(_))));
    assert!(kinds.iter().any(|d| matches!(d, FrameData::TtsStopped(_))));
    worker.end().await.unwrap();
    worker.join().await.unwrap();
    task.await.unwrap();
}

#[test]
fn partial_pcm_completion_is_rejected() {
    let mut mapper = RealtimeFrameMapper::default();
    let e = RealtimeEvent {
        event_id: "e".into(),
        event_type: "response.output_audio.delta".into(),
        session_id: None,
        conversation_id: None,
        context_id: None,
        generation: 0,
        event: serde_json::from_value(delta("r", "a", &[1])).unwrap(),
    };
    mapper.frames(&e).unwrap();
    let e = RealtimeEvent {
        event: Event::AudioDone {
            response_id: "r".into(),
            item_id: "a".into(),
            output_index: 0,
            content_index: 0,
        },
        ..e
    };
    assert!(matches!(mapper.frames(&e), Err(RealtimeError::Protocol(_))));
    mapper.reset();
    assert!(mapper.frames(&e).is_ok());
}

#[tokio::test]
async fn session_readiness_has_a_deadline_and_does_not_send_audio_early() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        send(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"c"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        let next = timeout(Duration::from_secs(1), ws.next()).await.unwrap();
        assert!(matches!(next, Some(Ok(Message::Close(_))) | None));
    });
    let mut c = config(url);
    c.limits.connect_timeout = Duration::from_millis(30);
    let mut session = RealtimeClient::new(c).unwrap().connect().await.unwrap();
    assert_eq!(
        session.sender().try_send(Action::AppendAudio(vec![0, 0])),
        Err(RealtimeError::NotReady)
    );
    assert_eq!(
        {
            next(&mut session).await;
            session.next_event().await.unwrap().unwrap_err()
        },
        RealtimeError::Timeout("session readiness")
    );
    task.await.unwrap();
}

#[tokio::test]
async fn oversized_audio_and_context_are_rejected_before_enqueue() {
    let (url, task) = server(|mut ws| async move {
        let close = timeout(Duration::from_secs(5), ws.next()).await.unwrap();
        assert!(matches!(close, Some(Ok(Message::Close(_))) | None));
    })
    .await;
    let mut c = config(url);
    c.limits.max_audio_chunk_bytes = 8;
    let mut session = RealtimeClient::new(c).unwrap().connect().await.unwrap();
    drain_initial(&mut session).await;
    assert_eq!(
        session.sender().try_send(Action::AppendAudio(vec![0; 9])),
        Err(RealtimeError::TooLarge)
    );
    assert_eq!(
        session
            .sender()
            .try_send(Action::CreateResponse(ResponseOptions {
                instructions: Some("x".repeat(2 * 1024 * 1024)),
                ..Default::default()
            })),
        Err(RealtimeError::TooLarge)
    );
    session.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn oversized_and_invalid_base64_provider_messages_are_rejected() {
    let (url, task) = server(|mut ws| async move {
        let _ = ws.send(Message::Text("x".repeat(2048).into())).await;
    })
    .await;
    let mut c = config(url);
    c.limits.max_message_bytes = 1024;
    c.limits.max_audio_chunk_bytes = 256;
    let mut session = RealtimeClient::new(c).unwrap().connect().await.unwrap();
    drain_initial(&mut session).await;
    assert_eq!(
        session.next_event().await.unwrap().unwrap_err(),
        RealtimeError::TooLarge
    );
    task.await.unwrap();
    let (url, task) = server(|mut ws| async move {
        read(&mut ws).await;
        created(&mut ws, "bad_audio").await;
        let mut event = delta("bad_audio", "a", &[1, 2]);
        event["delta"] = json!("not@base64");
        send(&mut ws, event).await;
    })
    .await;
    let mut session = connect(url).await;
    session
        .sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut error = None;
    while let Some(result) = session.next_event().await {
        if let Err(e) = result {
            error = Some(e);
        }
    }
    assert!(matches!(error, Some(RealtimeError::Protocol(_))));
    task.await.unwrap();
}

#[tokio::test]
async fn next_response_is_deferred_until_active_response_done() {
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (url, task) = server(|mut ws| async move {
        read(&mut ws).await;
        created(&mut ws, "first").await;
        assert_eq!(read(&mut ws).await["type"], "conversation.item.create");
        release_rx.await.unwrap();
        assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
        done(&mut ws, "first").await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        created(&mut ws, "second").await;
        done(&mut ws, "second").await;
        let _ = ws.close(None).await;
    })
    .await;
    let mut session = connect(url).await;
    let tx = session.sender();
    tx.send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    loop {
        if matches!(
            next(&mut session).await.event,
            Event::ResponseCreated { .. }
        ) {
            break;
        }
    }
    tx.send(Action::CreateItem {
        item: ConversationItem::Message {
            id: "new-material".into(),
            role: Role::User,
            content: vec![ItemContent::InputText { text: "new".into() }],
        },
        previous_item_id: None,
    })
    .await
    .unwrap();
    tx.send(Action::CreateResponse(ResponseOptions {
        context_id: Some("deferred-context".into()),
        ..Default::default()
    }))
    .await
    .unwrap();
    release_tx.send(()).unwrap();
    let mut second = false;
    while let Some(e) = session.next_event().await {
        let e = e.unwrap();
        if e.event.response_id() == Some("second") {
            assert_eq!(e.context_id.as_deref(), Some("deferred-context"));
            second = true;
        }
    }
    assert!(second);
    task.await.unwrap();
}

#[tokio::test]
async fn failed_response_emits_error_and_response_end_frames() {
    let (url, task) = server(|mut ws| async move {
        read(&mut ws).await;
        created(&mut ws, "failure").await;
        let mut r = response("failure", "failed");
        r["status_details"] = json!({"error":{"code":"server_error","message":"safe failure"}});
        send(&mut ws, json!({"type":"response.done","response":r})).await;
        let _ = ws.close(None).await;
    })
    .await;
    let mut session = connect(url).await;
    session
        .sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut mapper = RealtimeFrameMapper::default();
    let mut frames = vec![];
    while let Some(e) = session.next_event().await {
        frames.extend(mapper.frames(&e.unwrap()).unwrap());
    }
    assert!(frames
        .iter()
        .any(|f| matches!(&f.data,FrameData::Error(e) if e.error=="safe failure")));
    assert!(matches!(
        frames.last().unwrap().data,
        FrameData::LlmResponseEnd
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn dropping_session_closes_socket_even_with_a_retained_sender() {
    let (url, task) = server(|mut ws| async move {
        let close = timeout(Duration::from_secs(5), ws.next()).await.unwrap();
        assert!(matches!(close, Some(Ok(Message::Close(_))) | None));
    })
    .await;
    let mut session = connect(url).await;
    drain_initial(&mut session).await;
    let sender = session.sender();
    drop(session);
    task.await.unwrap();
    assert_eq!(
        sender.send(Action::ClearAudio).await.unwrap_err(),
        RealtimeError::Closed
    );
}

#[tokio::test]
async fn external_turn_policy_interrupts_after_provider_proposes_speech() {
    let (cancelled_tx, cancelled_rx) = tokio::sync::oneshot::channel();
    let (url,task)=server(|mut ws|async move {
        read(&mut ws).await;created(&mut ws,"before_vad").await;
        send(&mut ws,delta("before_vad","old_item",&[1,2])).await;
        send(&mut ws,json!({"type":"input_audio_buffer.speech_started","item_id":"utterance","audio_start_ms":10})).await;
        assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
        send(&mut ws,json!({"type":"input_audio_buffer.cleared"})).await;
        assert_eq!(read(&mut ws).await["type"],"response.cancel");
        cancelled_tx.send(()).unwrap();
        send(&mut ws,delta("before_vad","old_item",&[3,4])).await;
        done(&mut ws,"before_vad").await;
        send(&mut ws,json!({"type":"input_audio_buffer.speech_stopped","item_id":"utterance","audio_end_ms":500})).await;
        created(&mut ws,"after_vad").await;send(&mut ws,delta("after_vad","new_item",&[5,6])).await;done(&mut ws,"after_vad").await;
        let _=ws.close(None).await;
    }).await;
    let mut c = config(url);
    c.session.turn_detection = Some(Some(TurnDetection::server_vad()));
    let mut session = RealtimeClient::new(c).unwrap().connect().await.unwrap();
    session
        .sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    loop {
        if matches!(next(&mut session).await.event, Event::AudioCleared) {
            break;
        }
    }
    session.sender().interrupt(None);
    cancelled_rx.await.unwrap();
    let mut audio = vec![];
    let mut speech = false;
    while let Some(e) = session.next_event().await {
        let e = e.unwrap();
        assert_ne!(e.event.response_id(), Some("before_vad"));
        match e.event {
            Event::AudioDelta { audio: a, .. } => audio.extend(a),
            Event::SpeechStarted { .. } => speech = true,
            _ => {}
        }
    }
    // The earlier SpeechStarted proposal was consumed before explicit interruption.
    let _ = speech;
    assert_eq!(audio, vec![5, 6]);
    task.await.unwrap();
}

#[tokio::test]
async fn announced_async_tool_result_survives_interruption_without_continuation() {
    let (url, task) = server(|mut ws| async move {
        read(&mut ws).await;
        created(&mut ws, "tools").await;
        tool(&mut ws, "stale_call", "lookup").await;
        assert_eq!(read(&mut ws).await["type"], "response.cancel");
        done(&mut ws, "tools").await;
        let final_result = read(&mut ws).await;
        assert_eq!(final_result["item"]["call_id"], "stale_call");
        send(&mut ws, json!({"type":"input_audio_buffer.cleared"})).await;
        assert!(timeout(Duration::from_millis(50), ws.next()).await.is_err());
        let _ = ws.close(None).await;
    })
    .await;
    let mut session = connect(url).await;
    let sender = session.sender();
    sender
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    loop {
        if matches!(
            next(&mut session).await.event,
            Event::FunctionArgumentsDone { .. }
        ) {
            break;
        }
    }
    sender.interrupt(None);
    sender
        .send(Action::ToolResult {
            call_id: "stale_call".into(),
            output: json!({"too":"late"}),
            is_final: true,
        })
        .await
        .unwrap();
    while let Some(e) = session.next_event().await {
        assert!(!matches!(e.unwrap().event, Event::ResponseCreated { .. }));
    }
    task.await.unwrap();
}

#[tokio::test]
async fn duplicate_lifecycle_and_audio_after_response_done_do_not_resurrect_output() {
    let (url, task) = server(|mut ws| async move {
        read(&mut ws).await;
        created(&mut ws, "r").await;
        send(&mut ws, delta("r", "a", &[1, 2])).await;
        done(&mut ws, "r").await;
        created(&mut ws, "r").await;
        send(&mut ws, delta("r", "a", &[3, 4])).await;
        done(&mut ws, "r").await;
        let _ = ws.close(None).await;
    })
    .await;
    let mut session = connect(url).await;
    session
        .sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut audio = vec![];
    let mut starts = 0;
    let mut ends = 0;
    while let Some(e) = session.next_event().await {
        match e.unwrap().event {
            Event::ResponseCreated { .. } => starts += 1,
            Event::ResponseDone { .. } => ends += 1,
            Event::AudioDelta { audio: a, .. } => audio.extend(a),
            _ => {}
        }
    }
    assert_eq!((starts, ends), (1, 1));
    assert_eq!(audio, vec![1, 2]);
    task.await.unwrap();
}

struct DetailedStartup;
#[pipecat_core::async_trait]
impl pipecat_core::FrameProcessor for DetailedStartup {
    fn name(&self) -> &str {
        "DetailedStartup"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: pipecat_core::Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> Result<(), pipecat_core::ProcessorError> {
        if let FrameData::Start(configuration) = frame.data {
            frame.data = FrameData::StartDetailed(pipecat_core::DetailedStartFrame {
                configuration,
                enable_tracing: true,
                tracing_context: None,
            });
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn detailed_start_and_end_drain_preceding_socket_commands_and_response() {
    let (url, task) = server(|mut ws| async move {
        assert_eq!(read(&mut ws).await["type"], "conversation.item.create");
        assert_eq!(read(&mut ws).await["type"], "response.create");
        created(&mut ws, "drain").await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        send(&mut ws, delta("drain", "a", &[7, 8])).await;
        done(&mut ws, "drain").await;
        let close = timeout(Duration::from_secs(5), ws.next()).await.unwrap();
        assert!(matches!(close, Some(Ok(Message::Close(_))) | None));
    })
    .await;
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config(url)).unwrap()).unwrap());
    let mut cfg = PipelineConfig::default();
    cfg.start.audio_in_sample_rate = 24_000;
    cfg.start.audio_out_sample_rate = 24_000;
    let worker = Pipeline::new(vec![Arc::new(DetailedStartup), processor])
        .start(cfg)
        .await
        .unwrap();
    let sender = worker.sender();
    sender
        .send(
            Frame::new(FrameData::InputText(pipecat_core::TextFrame::new(
                "finish this",
            ))),
            pipecat_core::Direction::Downstream,
        )
        .await
        .unwrap();
    sender
        .send(
            Frame::new(FrameData::EndDetailed(pipecat_core::ReasonFrame {
                reason: Some(json!("graceful")),
            })),
            pipecat_core::Direction::Downstream,
        )
        .await
        .unwrap();
    let (frames, report) = timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, pipecat_core::WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::StartDetailed(_))));
    let audio = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::TtsAudio(_)))
        .unwrap();
    let response_end = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
        .unwrap();
    let end = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::EndDetailed(_)))
        .unwrap();
    assert!(audio < response_end && response_end < end);
    task.await.unwrap();
}
#[tokio::test]
async fn detailed_cancel_aborts_live_response_and_closes_socket() {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (url, task) = server(|mut ws| async move {
        read(&mut ws).await;
        read(&mut ws).await;
        created(&mut ws, "cancel_live").await;
        ready_tx.send(()).unwrap();
        let close = timeout(Duration::from_secs(5), ws.next()).await.unwrap();
        assert!(matches!(close, Some(Ok(Message::Close(_))) | None));
    })
    .await;
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config(url)).unwrap()).unwrap());
    let mut cfg = PipelineConfig::default();
    cfg.start.audio_in_sample_rate = 24_000;
    cfg.start.audio_out_sample_rate = 24_000;
    let worker = Pipeline::new(vec![Arc::new(DetailedStartup), processor])
        .start(cfg)
        .await
        .unwrap();
    let sender = worker.sender();
    sender
        .send(
            Frame::new(FrameData::InputText(pipecat_core::TextFrame::new(
                "cancel this",
            ))),
            pipecat_core::Direction::Downstream,
        )
        .await
        .unwrap();
    ready_rx.await.unwrap();
    sender
        .send(
            Frame::new(FrameData::CancelDetailed(pipecat_core::ReasonFrame {
                reason: Some(json!("cancel now")),
            })),
            pipecat_core::Direction::Downstream,
        )
        .await
        .unwrap();
    let (frames, report) = timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, pipecat_core::WorkerState::Cancelled);
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::CancelDetailed(_))));
    task.await.unwrap();
}

#[tokio::test]
async fn explicit_cancel_settles_once_while_async_sibling_survives_without_inference() {
    let (url, task) = server(|mut ws| async move {
        assert_eq!(read(&mut ws).await["type"], "response.create");
        created(&mut ws, "tools").await;
        tool(&mut ws, "cancel", "first").await;
        tool(&mut ws, "async", "second").await;
        assert_eq!(read(&mut ws).await["type"], "response.cancel");
        let cancel = read(&mut ws).await;
        assert_eq!(cancel["item"]["type"], "function_call_output");
        assert_eq!(cancel["item"]["call_id"], "cancel");
        assert_eq!(cancel["item"]["output"], "CANCELLED");
        let result = read(&mut ws).await;
        assert_eq!(result["item"]["call_id"], "async");
        assert_eq!(result["item"]["output"], "{\"done\":true}");
        done(&mut ws, "tools").await;
        assert!(timeout(Duration::from_millis(80), ws.next()).await.is_err());
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = connect(url).await;
    let sender = session.sender();
    sender
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut calls = 0;
    while calls < 2 {
        if matches!(
            next(&mut session).await.event,
            Event::FunctionArgumentsDone { .. }
        ) {
            calls += 1;
        }
    }
    sender.interrupt(None);
    for _ in 0..2 {
        sender
            .send(Action::CancelTool {
                call_id: "cancel".into(),
                function_name: "first".into(),
            })
            .await
            .unwrap();
    }
    // A late successful callback is not replayed over the settled cancellation.
    sender
        .send(Action::ToolResult {
            call_id: "cancel".into(),
            output: json!("too late"),
            is_final: true,
        })
        .await
        .unwrap();
    sender
        .send(Action::ToolResult {
            call_id: "async".into(),
            output: json!({"done":true}),
            is_final: true,
        })
        .await
        .unwrap();
    task.await.unwrap();
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn graceful_finish_after_clean_close_is_idempotent_but_protocol_failure_is_preserved() {
    for malformed in [false, true] {
        let (url, task) = server(move |mut ws| async move {
            if malformed {
                ws.send(Message::Text("not json".into())).await.unwrap();
            } else {
                ws.close(None).await.unwrap();
            }
        })
        .await;
        let mut session = connect(url).await;
        let sender = session.sender();
        drain_initial(&mut session).await;
        let terminal = timeout(Duration::from_secs(2), session.next_event())
            .await
            .unwrap();
        if malformed {
            assert!(matches!(terminal, Some(Err(RealtimeError::Protocol(_)))));
            assert!(matches!(
                sender.finish().await,
                Err(RealtimeError::Protocol(_))
            ));
        } else {
            assert!(terminal.is_none());
            sender.finish().await.unwrap();
            sender.finish().await.unwrap();
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn accepted_settings_survive_interruption_and_projection_changes_only_on_admission() {
    let (url, task) = server(|mut ws| async move {
        // Both commands are queued before this current-thread test yields to the
        // actor. The priority cancellation lane must not discard session state.
        assert_eq!(read(&mut ws).await["type"], "response.cancel");
        let update = read(&mut ws).await;
        assert_eq!(update["type"], "session.update");
        assert_eq!(update["session"]["instructions"], "runtime instruction");
        assert!(timeout(Duration::from_millis(40), ws.next()).await.is_err());
        ws.close(None).await.unwrap();
    })
    .await;
    let mut c = config(url);
    c.limits.queue_capacity = 1;
    let mut session = RealtimeClient::new(c).unwrap().connect().await.unwrap();
    drain_initial(&mut session).await;
    let sender = session.sender();
    let patch = SessionProperties {
        instructions: Some("runtime instruction".into()),
        ..SessionProperties::new()
    };
    sender
        .try_send(Action::UpdateSession(patch.into()))
        .unwrap();
    assert_eq!(
        sender.projected_session().instructions.as_deref(),
        Some("runtime instruction")
    );
    let rejected = SessionProperties {
        instructions: Some("must not project".into()),
        ..SessionProperties::new()
    };
    assert_eq!(
        sender.try_send(Action::UpdateSession(rejected.into())),
        Err(RealtimeError::Backpressure)
    );
    assert_eq!(
        sender.projected_session().instructions.as_deref(),
        Some("runtime instruction")
    );
    sender.interrupt(None);
    while let Some(event) = session.next_event().await {
        event.unwrap();
    }
    task.await.unwrap();
}

#[tokio::test]
async fn speech_proposal_without_interruption_preserves_owed_aggregator_inference() {
    let (release, released) = tokio::sync::oneshot::channel();
    let (url, task) = server(|mut ws| async move {
        assert_eq!(read(&mut ws).await["type"], "response.create");
        created(&mut ws, "tools").await;
        tool(&mut ws, "a", "first").await;
        let result = read(&mut ws).await;
        assert_eq!(result["item"]["type"], "function_call_output");
        released.await.unwrap();
        send(&mut ws, json!({"type":"input_audio_buffer.speech_started","item_id":"utterance","audio_start_ms":1})).await;
        assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
        done(&mut ws, "tools").await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        created(&mut ws, "continued").await;
        done(&mut ws, "continued").await;
        assert!(timeout(Duration::from_millis(30), ws.next()).await.is_err());
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = connect(url).await;
    let sender = session.sender();
    sender
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    loop {
        if matches!(
            next(&mut session).await.event,
            Event::FunctionArgumentsDone { .. }
        ) {
            break;
        }
    }
    sender
        .send(Action::ToolResult {
            call_id: "a".into(),
            output: json!("result"),
            is_final: true,
        })
        .await
        .unwrap();
    sender
        .send(Action::RequestInference(Default::default()))
        .await
        .unwrap();
    release.send(()).unwrap();
    let mut proposed = false;
    let mut continued = false;
    while let Some(event) = session.next_event().await {
        match event.unwrap().event {
            Event::SpeechStarted { .. } => proposed = true,
            Event::ResponseCreated { response } if response.id == "continued" => continued = true,
            _ => {}
        }
    }
    assert!(proposed && continued);
    task.await.unwrap();
}
