use futures_util::{SinkExt, StreamExt};
use pipecat_core::{
    AudioRawFrame, ChatMessage, ChatRole, Direction, Frame, FrameData, LlmContextFrame, Pipeline,
    PipelineConfig,
};
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{
    tungstenite::{handshake::server::Request, Message},
    WebSocketStream,
};

type Ws = WebSocketStream<TcpStream>;
const DEADLINE: Duration = Duration::from_secs(3);
#[allow(clippy::result_large_err)]
async fn server<F, Fut>(f: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(Ws, Value) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_hdr_async(stream, |request: &Request, response| {
            assert_eq!(request.uri().query(), Some("key=test-key"));
            assert_eq!(request.uri().path(), "/live");
            assert!(!request.headers().contains_key("authorization"));
            Ok(response)
        })
        .await
        .unwrap();
        let setup = read(&mut ws).await;
        assert!(setup["setup"].is_object());
        f(ws, setup).await;
    });
    (endpoint, task)
}
fn config(endpoint: String) -> GeminiConfig {
    let mut c = GeminiConfig::new("test-key", "source-model");
    c.endpoint = endpoint;
    c
}
fn declaration() -> FunctionDeclaration {
    FunctionDeclaration {
        name: "lookup".into(),
        description: "Lookup a city".into(),
        parameters: json!({"type":"object","properties":{"city":{"type":"string"}}}),
    }
}
async fn read(ws: &mut Ws) -> Value {
    loop {
        let message = timeout(DEADLINE, ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match message {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Binary(bytes) => return serde_json::from_slice(&bytes).unwrap(),
            Message::Pong(_) => {}
            other => panic!("expected JSON, got {other:?}"),
        }
    }
}
async fn send(ws: &mut Ws, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn ready(ws: &mut Ws) {
    send(ws, json!({"setupComplete":{}})).await;
}
async fn done(ws: &mut Ws) {
    send(ws, json!({"serverContent":{"turnComplete":true}})).await;
}
async fn disconnected(ws: &mut Ws) {
    loop {
        match timeout(DEADLINE, ws.next()).await.unwrap() {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
            Some(Ok(Message::Ping(bytes))) => {
                let _ = ws.send(Message::Pong(bytes)).await;
            }
            _ => {}
        }
    }
}
async fn next(session: &mut GeminiSession) -> GeminiEvent {
    timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn terminal(session: &mut GeminiSession) -> GeminiError {
    loop {
        match timeout(DEADLINE, session.next_event()).await.unwrap() {
            Some(Err(e)) => return e,
            Some(Ok(_)) => {}
            None => panic!("expected error"),
        }
    }
}
fn fixture(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

#[tokio::test]
async fn setup_is_first_authenticated_and_ready_is_a_real_barrier() {
    let (release_tx, release_rx) = oneshot::channel();
    let (seen_tx, seen_rx) = oneshot::channel();
    let (url, task) = server(|mut ws, setup| async move {
        assert_eq!(setup, fixture(include_str!("fixtures/setup_audio.json")));
        ws.send(Message::Ping(vec![1, 2].into())).await.unwrap();
        assert!(matches!(
            ws.next().await.unwrap().unwrap(),
            Message::Pong(_)
        ));
        seen_tx.send(()).unwrap();
        release_rx.await.unwrap();
        ready(&mut ws).await;
        assert_eq!(
            read(&mut ws).await,
            json!({"realtimeInput":{"text":"hello"}})
        );
        done(&mut ws).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.session.voice = Some("Puck".into());
    c.session.language_code = Some("en-US".into());
    c.session.system_instruction = Some("Be brief.".into());
    c.session.temperature = Some(0.7);
    c.session.max_output_tokens = Some(256);
    c.session.tools = vec![declaration()];
    let client = GeminiClient::new(c).unwrap();
    let connect = tokio::spawn(async move { client.connect().await });
    timeout(DEADLINE, seen_rx).await.unwrap().unwrap();
    assert!(!connect.is_finished());
    release_tx.send(()).unwrap();
    let mut session = connect.await.unwrap().unwrap();
    session
        .sender()
        .try_send(Action::RealtimeText("hello".into()), Origin::default())
        .unwrap();
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    session.sender().finish().await.unwrap();
    assert!(timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
        .is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn bundled_content_preserves_all_parts_before_completion_and_maps_identity() {
    use tokio_tungstenite::tungstenite::protocol::frame::{
        coding::{Data, OpCode},
        Frame as RawFrame,
    };
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        read(&mut ws).await;
        let bytes = include_str!("fixtures/bundled_turn.json").as_bytes();
        let mid = bytes.len() / 2;
        ws.send(Message::Frame(RawFrame::message(
            bytes[..mid].to_vec(),
            OpCode::Data(Data::Text),
            false,
        )))
        .await
        .unwrap();
        ws.send(Message::Frame(RawFrame::message(
            bytes[mid..].to_vec(),
            OpCode::Data(Data::Continue),
            true,
        )))
        .await
        .unwrap();
        disconnected(&mut ws).await;
    })
    .await;
    let mut session = GeminiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let origin = Origin {
        frame_id: Some(42),
        context_id: Some("ctx-7".into()),
    };
    session
        .sender()
        .try_send(Action::RealtimeText("question".into()), origin.clone())
        .unwrap();
    let mut mapper = GeminiFrameMapper::new(SessionMode::Conversation, "user");
    let mut events = Vec::new();
    let mut frames = Vec::new();
    for _ in 0..8 {
        let event = next(&mut session).await;
        frames.extend(mapper.frames(&event));
        events.push(event);
    }
    assert!(matches!(events[0].event, Event::Text { thought: true, .. }));
    assert_eq!(
        events[2].event,
        Event::Audio {
            pcm: vec![1, 2, 3, 4],
            sample_rate: 24_000
        }
    );
    assert!(matches!(
        events[3].event,
        Event::InputTranscription {
            is_final: false,
            ..
        }
    ));
    assert_eq!(events[6].event, Event::TurnComplete);
    assert_eq!(events[2].origin, origin);
    assert_eq!(events[3].origin, Origin::default());
    let audio = frames
        .iter()
        .find(|f| matches!(f.data, FrameData::TtsAudio(_)))
        .unwrap();
    assert_eq!(audio.metadata["source_frame_id"], 42);
    assert_eq!(audio.metadata["context_id"], "ctx-7");
    assert!(frames.iter().all(|f| !matches!(
        f.data,
        FrameData::BotStartedSpeaking | FrameData::BotStoppedSpeaking
    )));
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::LlmResponseStart))
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::LlmResponseEnd))
            .count(),
        1
    );
    let tts_end = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsStopped(_)))
        .unwrap();
    let text = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsText(_)))
        .unwrap();
    assert!(text < tts_end);
    session.sender().finish().await.unwrap();
    assert!(session.next_event().await.is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn transcription_mode_emits_interim_and_final_and_flushes_before_finish() {
    let (url, task) = server(|mut ws, setup| async move {
        assert_eq!(
            setup["setup"]["generationConfig"]["responseModalities"],
            json!(["TEXT"])
        );
        assert!(setup["setup"].get("outputAudioTranscription").is_none());
        ready(&mut ws).await;
        assert_eq!(
            read(&mut ws).await["realtimeInput"]["audio"]["mimeType"],
            "audio/pcm;rate=16000"
        );
        send(&mut ws, fixture(include_str!("fixtures/stt_interim.json"))).await;
        assert_eq!(
            read(&mut ws).await,
            json!({"realtimeInput":{"audioStreamEnd":true}})
        );
        send(&mut ws, fixture(include_str!("fixtures/stt_final.json"))).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = GeminiConfig::transcription("test-key", "source-transcription-model");
    c.endpoint = url;
    let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    session
        .sender()
        .try_send(Action::Audio(vec![0, 0, 1, 0]), Origin::default())
        .unwrap();
    let interim = next(&mut session).await;
    session.sender().finish().await.unwrap();
    let final_event = next(&mut session).await;
    let mut mapper = GeminiFrameMapper::new(SessionMode::Transcription, "alice");
    assert!(matches!(
        mapper.frames(&interim)[0].data,
        FrameData::InterimTranscription(_)
    ));
    let frames = mapper.frames(&final_event);
    let FrameData::Transcription(transcript) = &frames[0].data else {
        panic!("not final transcription")
    };
    assert_eq!(transcript.text, "hello there");
    assert_eq!(transcript.user_id, "alice");
    assert!(transcript.finalized);
    assert_eq!(transcript.language.as_deref(), Some("en-US"));
    assert!(transcript.timestamp.contains('T'));
    assert!(session.next_event().await.is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn manual_vad_preroll_is_bounded_and_activity_precedes_audio_in_real_pipeline() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (url, task) = server(|mut ws, setup| async move {
        assert_eq!(
            setup["setup"]["realtimeInputConfig"]["automaticActivityDetection"]["disabled"],
            true
        );
        ready(&mut ws).await;
        let mut messages = Vec::new();
        for _ in 0..4 {
            messages.push(read(&mut ws).await);
        }
        assert_eq!(
            json!(messages),
            fixture(include_str!("fixtures/manual_activity.json"))
        );
        done(&mut ws).await;
        seen_tx.send(()).unwrap();
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.session.activity_detection.disabled = true;
    let processor = Arc::new(
        GeminiLiveProcessor::new(GeminiClient::new(c).unwrap())
            .with_preroll_ms(1)
            .unwrap(),
    );
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut a = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![0xaa, 0xbb, 0xcc, 0xdd], 16000, 1).unwrap(),
    ));
    a.metadata.insert("sentinel".into(), json!("kept"));
    let preserved = a.clone();
    worker
        .sender()
        .send(a, Direction::Downstream)
        .await
        .unwrap();
    loop {
        let f = timeout(DEADLINE, worker.recv()).await.unwrap().unwrap();
        if f.frame == preserved {
            break;
        }
    }
    worker
        .sender()
        .send(
            Frame::new(FrameData::UserStartedSpeaking),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![0x11, 0x22], 16000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    // UserStoppedSpeaking is a system frame; wait for audio to pass before it.
    // Wait until the second input has traversed the processor before stopping.
    loop {
        let f = timeout(DEADLINE, worker.recv()).await.unwrap().unwrap();
        if matches!(&f.frame.data, FrameData::InputAudio(a) if a.audio == [0x11,0x22]) {
            break;
        }
    }
    worker
        .sender()
        .send(
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Downstream,
        )
        .await
        .unwrap();
    seen_rx.await.unwrap();
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    task.await.unwrap();
}

#[tokio::test]
async fn tools_are_declared_matched_and_canceled_without_execution() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        send(&mut ws, json!({"toolCall":{"functionCalls":[{"id":"one","name":"lookup","args":{"city":"Paris"}},{"id":"two","name":"lookup","args":{}}]}})).await;
        let response = read(&mut ws).await;
        assert_eq!(response, json!({"toolResponse":{"functionResponses":[{"id":"one","name":"lookup","response":{"temperature":18}}]}}));
        send(&mut ws, json!({"toolCallCancellation":{"ids":["two"]}})).await;
        done(&mut ws).await; disconnected(&mut ws).await;
    }).await;
    let mut c = config(url);
    c.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    let call = next(&mut session).await;
    let Event::ToolCalls(calls) = &call.event else {
        panic!("tool calls")
    };
    assert_eq!(calls.len(), 2);
    let mut mapper = GeminiFrameMapper::new(SessionMode::Conversation, "");
    let frames = mapper.frames(&call);
    assert!(frames.iter().any(
        |f| matches!(&f.data, FrameData::FunctionCallInProgress(c) if c.tool_call_id == "one")
    ));
    session
        .sender()
        .try_send(
            Action::ToolResponses(vec![FunctionResponse {
                id: "one".into(),
                name: "lookup".into(),
                response: json!({"temperature":18}),
            }]),
            Origin::default(),
        )
        .unwrap();
    let canceled = next(&mut session).await;
    assert!(matches!(canceled.event, Event::ToolCallsCancelled(ref c) if c[0].id == "two"));
    assert!(
        matches!(&mapper.frames(&canceled)[0].data, FrameData::FunctionCallCancel(c) if c.tool_call_id == "two" && !c.run_llm)
    );
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    session.sender().finish().await.unwrap();
    assert!(session.next_event().await.is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn canceled_tool_result_is_rejected_instead_of_sent() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        send(
            &mut ws,
            json!({"toolCall":{"functionCalls":[{"id":"one","name":"lookup","args":{}}]}}),
        )
        .await;
        send(&mut ws, json!({"toolCallCancellation":{"ids":["one"]}})).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    next(&mut session).await;
    next(&mut session).await;
    session
        .sender()
        .try_send(
            Action::ToolResponses(vec![FunctionResponse {
                id: "one".into(),
                name: "lookup".into(),
                response: json!({}),
            }]),
            Origin::default(),
        )
        .unwrap();
    assert!(matches!(
        terminal(&mut session).await,
        GeminiError::Invalid(_)
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn local_interrupt_drops_old_output_but_does_not_fabricate_wire_cancel() {
    let (release_tx, release_rx) = oneshot::channel();
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await; read(&mut ws).await;
        send(&mut ws, json!({"serverContent":{"modelTurn":{"parts":[{"text":"first"}]}}})).await;
        release_rx.await.unwrap();
        send(&mut ws, json!({"serverContent":{"modelTurn":{"parts":[{"text":"stale"}]} ,"turnComplete":true}})).await;
        // Only the caller's new text arrives; no made-up cancellation message.
        assert_eq!(read(&mut ws).await, json!({"realtimeInput":{"text":"second"}}));
        send(&mut ws, json!({"serverContent":{"modelTurn":{"parts":[{"text":"fresh"}]},"turnComplete":true}})).await;
        disconnected(&mut ws).await;
    }).await;
    let mut session = GeminiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender
        .try_send(Action::RealtimeText("first".into()), Origin::default())
        .unwrap();
    next(&mut session).await;
    sender.interrupt().await;
    release_tx.send(()).unwrap();
    sender
        .try_send(Action::RealtimeText("second".into()), Origin::default())
        .unwrap();
    assert_eq!(
        next(&mut session).await.event,
        Event::Text {
            text: "fresh".into(),
            thought: false
        }
    );
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    sender.finish().await.unwrap();
    assert!(session.next_event().await.is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn provider_interruption_is_a_new_epoch_and_never_user_vad_or_playback() {
    let (url,task)=server(|mut ws,_|async move{
        ready(&mut ws).await;
        send(&mut ws,json!({"serverContent":{"interrupted":true,"modelTurn":{"parts":[{"text":"new"}]},"turnComplete":true}})).await;
        disconnected(&mut ws).await;
    }).await;
    let mut session = GeminiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let interrupted = next(&mut session).await;
    assert_eq!(interrupted.event, Event::Interrupted);
    assert_eq!(interrupted.generation, 1);
    let mut mapper = GeminiFrameMapper::new(SessionMode::Conversation, "");
    assert!(matches!(
        mapper.frames(&interrupted)[0].data,
        FrameData::Interruption
    ));
    assert!(matches!(next(&mut session).await.event, Event::Text { .. }));
    next(&mut session).await;
    session.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn bounded_input_queue_and_cancel_bypass_capacity() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.limits.queue_capacity = 1;
    let session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    let sender = session.sender();
    sender
        .try_send(Action::RealtimeText("one".into()), Origin::default())
        .unwrap();
    assert_eq!(
        sender.try_send(Action::RealtimeText("two".into()), Origin::default()),
        Err(GeminiError::Backpressure("input"))
    );
    sender.cancel();
    timeout(DEADLINE, session.shutdown())
        .await
        .unwrap()
        .unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn bounded_output_queue_reports_failure_instead_of_deadlocking() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        send(&mut ws, fixture(include_str!("fixtures/bundled_turn.json"))).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.limits.queue_capacity = 1;
    let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    assert_eq!(
        terminal(&mut session).await,
        GeminiError::Backpressure("output")
    );
    task.await.unwrap();
}

#[tokio::test]
async fn dropping_session_owns_and_aborts_its_socket_task() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        disconnected(&mut ws).await;
    })
    .await;
    let session = GeminiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    drop(session);
    timeout(DEADLINE, task).await.unwrap().unwrap();
    assert_eq!(
        sender.try_send(Action::RealtimeText("late".into()), Origin::default()),
        Err(GeminiError::Closed)
    );
}

#[tokio::test]
async fn finish_timeout_is_reported_and_socket_closed() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        read(&mut ws).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.limits.finish_timeout = Duration::from_millis(30);
    let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    session
        .sender()
        .try_send(Action::RealtimeText("pending".into()), Origin::default())
        .unwrap();
    session.sender().finish().await.unwrap();
    assert_eq!(terminal(&mut session).await, GeminiError::Timeout("finish"));
    task.await.unwrap();
}

#[tokio::test]
async fn setup_timeout_and_early_content_fail_without_background_leaks() {
    for early_content in [false, true] {
        let (url, task) = server(move |mut ws, _| async move {
            if early_content {
                send(&mut ws, json!({"serverContent":{"turnComplete":true}})).await;
            }
            disconnected(&mut ws).await;
        })
        .await;
        let mut c = config(url);
        c.limits.setup_timeout = Duration::from_millis(30);
        let result = GeminiClient::new(c).unwrap().connect().await;
        assert!(matches!(result, Err(GeminiError::Protocol(_))) == early_content);
        if !early_content {
            assert!(matches!(result, Err(GeminiError::Timeout("setup"))));
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn malformed_unsupported_and_provider_errors_are_explicit() {
    let cases = [
        (
            json!({"setupComplete":{}}),
            GeminiError::Protocol("duplicate setupComplete"),
        ),
        (
            json!({"serverContent":{"interactionStatus":"IN_PROGRESS","turnComplete":true}}),
            GeminiError::Unsupported("background reasoning interaction status"),
        ),
        (
            json!({"sessionResumptionUpdate":{"newHandle":"never-use","resumable":true}}),
            GeminiError::Unsupported("session resumption"),
        ),
        (
            json!({"serverContent":{"groundingMetadata":{}}}),
            GeminiError::Unsupported("search/URL grounding"),
        ),
        (
            json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"video/mp4","data":"AA=="}}]}}}),
            GeminiError::Unsupported("output media must be mono PCM16 at 24000 Hz"),
        ),
        (
            json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm","data":"???"}}]}}}),
            GeminiError::Protocol("invalid base64 audio"),
        ),
        (
            json!({"serverContent":{"modelTurn":{"parts":[{"inlineData":{"mimeType":"audio/pcm","data":"AA=="}}]}}}),
            GeminiError::Protocol("partial PCM16 output sample"),
        ),
        (
            json!({"error":{"code":429,"message":"bad test-key"}}),
            GeminiError::Provider {
                code: 429,
                message: "bad [REDACTED]".into(),
            },
        ),
    ];
    for (message, expected) in cases {
        let (url, task) = server(move |mut ws, _| async move {
            ready(&mut ws).await;
            send(&mut ws, message).await;
            disconnected(&mut ws).await;
        })
        .await;
        let mut session = GeminiClient::new(config(url))
            .unwrap()
            .connect()
            .await
            .unwrap();
        assert_eq!(terminal(&mut session).await, expected);
        task.await.unwrap();
    }
}

#[tokio::test]
async fn wire_message_audio_and_pending_tool_limits_are_enforced() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        send(
            &mut ws,
            json!({"serverContent":{"modelTurn":{"parts":[{"text":"x".repeat(2048)}]}}}),
        )
        .await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.limits.max_message_bytes = 1024;
    c.limits.max_audio_bytes = 64;
    let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    assert_eq!(
        session
            .sender()
            .try_send(Action::Audio(vec![0; 66]), Origin::default()),
        Err(GeminiError::TooLarge)
    );
    assert!(matches!(
        session
            .sender()
            .try_send(Action::Audio(vec![0]), Origin::default()),
        Err(GeminiError::Invalid(_))
    ));
    assert_eq!(terminal(&mut session).await, GeminiError::TooLarge);
    task.await.unwrap();
}

#[test]
fn invalid_configs_and_unsupported_fields_never_silently_alias() {
    let mut c = GeminiConfig::new("secret-123", "source-model");
    assert!(!format!("{c:?}").contains("secret-123"));
    for url in [
        "http://example.com/live",
        "ws://example.com/live",
        "wss://user:pass@example.com/live",
        "wss://example.com/live?key=other",
    ] {
        c.endpoint = url.into();
        assert!(matches!(
            GeminiClient::new(c.clone()),
            Err(GeminiError::Invalid(_))
        ));
    }
    c.endpoint = DEFAULT_ENDPOINT.into();
    c.session.temperature = Some(f64::NAN);
    assert!(GeminiClient::new(c).is_err());
    let mut invalid_limits = GeminiConfig::new("test-key", "source-model");
    invalid_limits.limits.finish_timeout = Duration::MAX;
    assert!(matches!(
        GeminiClient::new(invalid_limits),
        Err(GeminiError::Invalid("limits"))
    ));
    let mut value = serde_json::to_value(SessionConfig::new("m")).unwrap();
    value["session_resumption"] = json!({});
    assert!(serde_json::from_value::<SessionConfig>(value).is_err());
}

#[tokio::test]
async fn pipeline_context_seeding_preserves_source_frame_without_replay() {
    let (seen_tx, seen_rx) = oneshot::channel();
    let (url,task)=server(|mut ws,_|async move{
        ready(&mut ws).await;
        assert_eq!(read(&mut ws).await,json!({"clientContent":{"turns":[{"role":"user","parts":[{"text":"hello"}]}],"turnComplete":true}}));
        done(&mut ws).await;seen_tx.send(()).unwrap();
        // Repeated context is not sent again.
        assert!(matches!(timeout(DEADLINE,ws.next()).await.unwrap().unwrap().unwrap(),Message::Close(_)));
    }).await;
    let processor = Arc::new(GeminiLiveProcessor::new(
        GeminiClient::new(config(url)).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let frame = Frame::new(FrameData::LlmContext(LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "hello")],
        ..Default::default()
    }));
    worker
        .sender()
        .send(frame.clone(), Direction::Downstream)
        .await
        .unwrap();
    seen_rx.await.unwrap();
    worker
        .sender()
        .send(frame.clone(), Direction::Downstream)
        .await
        .unwrap();
    let mut copies = 0;
    while copies < 2 {
        let output = timeout(DEADLINE, worker.recv()).await.unwrap().unwrap();
        if output.frame == frame {
            copies += 1;
        }
    }
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    task.await.unwrap();
}

#[tokio::test]
async fn finishing_allows_outstanding_tool_results_and_waits_for_continuation() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        send(&mut ws, json!({"toolCall":{"functionCalls":[{"id":"one","name":"lookup","args":{}}]}})).await;
        assert_eq!(read(&mut ws).await["toolResponse"]["functionResponses"][0]["id"], "one");
        send(&mut ws,json!({"serverContent":{"modelTurn":{"parts":[{"text":"result"}]},"turnComplete":true}})).await;
        disconnected(&mut ws).await;
    }).await;
    let mut c = config(url);
    c.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    next(&mut session).await;
    let sender = session.sender();
    sender.finish().await.unwrap();
    assert_eq!(
        sender.try_send(Action::RealtimeText("late".into()), Origin::default()),
        Err(GeminiError::Closed)
    );
    sender
        .try_send(
            Action::ToolResponses(vec![FunctionResponse {
                id: "one".into(),
                name: "lookup".into(),
                response: json!({"ok":true}),
            }]),
            Origin::default(),
        )
        .unwrap();
    assert!(matches!(next(&mut session).await.event, Event::Text { .. }));
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    assert!(session.next_event().await.is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn manual_audio_outside_activity_is_an_explicit_error() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.session.activity_detection.disabled = true;
    let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    session
        .sender()
        .try_send(Action::Audio(vec![1, 2]), Origin::default())
        .unwrap();
    assert_eq!(
        terminal(&mut session).await,
        GeminiError::Invalid("manual VAD audio outside activity window")
    );
    task.await.unwrap();
}

#[tokio::test]
async fn manual_preroll_evicts_oldest_samples_before_activity_and_end_flushes() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        assert_eq!(
            read(&mut ws).await,
            json!({"realtimeInput":{"activityStart":{}}})
        );
        let audio = read(&mut ws).await;
        use base64::{engine::general_purpose::STANDARD, Engine};
        let actual = STANDARD
            .decode(audio["realtimeInput"]["audio"]["data"].as_str().unwrap())
            .unwrap();
        assert_eq!(actual, (8u8..40).collect::<Vec<_>>());
        assert_eq!(
            read(&mut ws).await,
            json!({"realtimeInput":{"activityEnd":{}}})
        );
        done(&mut ws).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.session.activity_detection.disabled = true;
    let processor = Arc::new(
        GeminiLiveProcessor::new(GeminiClient::new(c).unwrap())
            .with_preroll_ms(1)
            .unwrap(),
    );
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    for bytes in [
        (0u8..20).collect::<Vec<_>>(),
        (20u8..40).collect::<Vec<_>>(),
    ] {
        let audio = Frame::new(FrameData::InputAudio(
            AudioRawFrame::new(bytes, 16000, 1).unwrap(),
        ));
        let id = audio.id;
        worker
            .sender()
            .send(audio, Direction::Downstream)
            .await
            .unwrap();
        loop {
            if timeout(DEADLINE, worker.recv())
                .await
                .unwrap()
                .unwrap()
                .frame
                .id
                == id
            {
                break;
            }
        }
    }
    let start = Frame::new(FrameData::UserStartedSpeaking);
    let id = start.id;
    worker
        .sender()
        .send(start, Direction::Downstream)
        .await
        .unwrap();
    loop {
        if timeout(DEADLINE, worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame
            .id
            == id
        {
            break;
        }
    }
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    task.await.unwrap();
}

#[tokio::test]
async fn pending_tool_limits_and_duplicate_ids_are_enforced_across_packets() {
    for duplicate in [false, true] {
        let (url,task)=server(move|mut ws,_|async move{
            ready(&mut ws).await;
            send(&mut ws,json!({"toolCall":{"functionCalls":[{"id":"one","name":"lookup","args":{}}]}})).await;
            send(&mut ws,json!({"toolCall":{"functionCalls":[{"id":if duplicate{"one"}else{"two"},"name":"lookup","args":{}}]}})).await;
            disconnected(&mut ws).await;
        }).await;
        let mut c = config(url);
        c.session.tools = vec![declaration()];
        c.limits.max_pending_tools = if duplicate { 2 } else { 1 };
        let mut session = GeminiClient::new(c).unwrap().connect().await.unwrap();
        let error = terminal(&mut session).await;
        assert_eq!(
            error,
            if duplicate {
                GeminiError::Protocol("duplicate pending tool ID")
            } else {
                GeminiError::TooLarge
            }
        );
        task.await.unwrap();
    }
}

#[tokio::test]
async fn pipeline_rejects_context_mutation_and_forwards_upstream_identity() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        read(&mut ws).await;
        done(&mut ws).await;
        disconnected(&mut ws).await;
    })
    .await;
    let processor = Arc::new(GeminiLiveProcessor::new(
        GeminiClient::new(config(url)).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let context = LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "hello")],
        ..Default::default()
    };
    let seed = Frame::new(FrameData::LlmContext(context.clone()));
    let id = seed.id;
    worker
        .sender()
        .send(seed, Direction::Downstream)
        .await
        .unwrap();
    loop {
        if timeout(DEADLINE, worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame
            .id
            == id
        {
            break;
        }
    }
    let mut changed = context;
    changed
        .messages
        .push(ChatMessage::new(ChatRole::User, "new"));
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(changed)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let upstream = Frame::text("keep my identity");
    let id = upstream.id;
    worker
        .sender()
        .send(upstream.clone(), Direction::Upstream)
        .await
        .unwrap();
    let mut error_seen = false;
    let mut upstream_seen = false;
    while !error_seen || !upstream_seen {
        let output = timeout(DEADLINE, worker.recv()).await.unwrap().unwrap();
        if let FrameData::Error(error) = &output.frame.data {
            assert!(error
                .error
                .contains("replacing or replaying evolving context"));
            assert_eq!(output.direction, Direction::Upstream);
            error_seen = true;
        }
        if output.frame.id == id {
            assert_eq!(output.frame, upstream);
            assert_eq!(output.direction, Direction::Upstream);
            upstream_seen = true;
        }
    }
    worker.cancel("done").await.unwrap();
    worker.collect().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn cancel_interrupts_pipeline_receive_and_joins_owned_work() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        disconnected(&mut ws).await;
    })
    .await;
    let processor = Arc::new(GeminiLiveProcessor::new(
        GeminiClient::new(config(url)).unwrap(),
    ));
    let worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.cancel("test cancellation").await.unwrap();
    let (_, report) = timeout(DEADLINE, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(report.state, pipecat_core::WorkerState::Cancelled);
    task.await.unwrap();
}

#[tokio::test]
async fn handshake_http_error_preserves_category_without_echoing_secret() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/live", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 4096];
        let count = socket.read(&mut request).await.unwrap();
        assert!(count > 0);
        socket
            .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 8\r\n\r\ntest-key")
            .await
            .unwrap();
    });
    let error = match GeminiClient::new(config(url)).unwrap().connect().await {
        Err(error) => error,
        Ok(_) => panic!("expected handshake rejection"),
    };
    assert_eq!(error, GeminiError::Http(401));
    assert!(!error.to_string().contains("test-key"));
    server.await.unwrap();
}

#[tokio::test]
async fn cancellation_interrupts_a_large_stalled_socket_write() {
    let (release_tx, release_rx) = oneshot::channel();
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        release_rx.await.unwrap();
        disconnected(&mut ws).await;
    })
    .await;
    let mut c = config(url);
    c.limits.max_message_bytes = 8 * 1024 * 1024;
    c.limits.write_timeout = Duration::from_secs(60);
    let session = GeminiClient::new(c).unwrap().connect().await.unwrap();
    let sender = session.sender();
    sender
        .try_send(
            Action::RealtimeText("x".repeat(7 * 1024 * 1024)),
            Origin::default(),
        )
        .unwrap();
    // The peer withholds reads and the payload is larger than socket buffers.
    tokio::time::sleep(Duration::from_millis(30)).await;
    sender.cancel();
    timeout(Duration::from_secs(1), session.shutdown())
        .await
        .unwrap()
        .unwrap();
    release_tx.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn malformed_json_and_malformed_readiness_are_not_accepted() {
    for setup in [false, true] {
        let (url, task) = server(move |mut ws, _| async move {
            if !setup {
                ready(&mut ws).await;
            }
            ws.send(Message::Text(
                if setup {
                    "{\"setupComplete\":[]}"
                } else {
                    "{broken"
                }
                .into(),
            ))
            .await
            .unwrap();
            disconnected(&mut ws).await;
        })
        .await;
        let result = GeminiClient::new(config(url)).unwrap().connect().await;
        if setup {
            assert!(matches!(
                result,
                Err(GeminiError::Protocol("setupComplete must be an object"))
            ));
        } else {
            let mut session = result.unwrap();
            assert_eq!(
                terminal(&mut session).await,
                GeminiError::Protocol("invalid JSON")
            );
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn binary_json_messages_are_decoded_but_unexpected_peer_close_is_reported() {
    let (url, task) = server(|mut ws, _| async move {
        ready(&mut ws).await;
        ws.send(Message::Binary(
            br#"{"serverContent":{"modelTurn":{"parts":[{"text":"binary"}]}}}"#
                .to_vec()
                .into(),
        ))
        .await
        .unwrap();
        ws.close(None).await.unwrap();
    })
    .await;
    let mut session = GeminiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert!(matches!(next(&mut session).await.event,Event::Text{text,..} if text=="binary"));
    assert_eq!(terminal(&mut session).await, GeminiError::Closed);
    task.await.unwrap();
}

#[test]
fn incomplete_usage_never_fabricates_zero_tokens_or_playback() {
    let mut mapper = GeminiFrameMapper::new(SessionMode::Conversation, "");
    let event = GeminiEvent {
        event: Event::Usage(Usage {
            prompt_token_count: Some(3),
            response_token_count: None,
            total_token_count: None,
        }),
        origin: Origin::default(),
        generation: 0,
    };
    assert!(mapper.frames(&event).is_empty());
    let event = GeminiEvent {
        event: Event::GenerationComplete,
        origin: Origin::default(),
        generation: 0,
    };
    assert!(mapper.frames(&event).is_empty());
}
