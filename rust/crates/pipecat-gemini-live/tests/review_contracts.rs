//! Independent synthetic loopback contract regressions. No provider calls.
use futures_util::{SinkExt, StreamExt};
use pipecat_gemini_live::*;
use serde_json::{json, Value};
use std::{future::Future, task::Poll, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

type Ws = WebSocketStream<TcpStream>;
const DEADLINE: Duration = Duration::from_secs(3);
async fn send(ws: &mut Ws, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn read(ws: &mut Ws) -> Value {
    match timeout(DEADLINE, ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        other => panic!("expected JSON: {other:?}"),
    }
}
async fn disconnected(ws: &mut Ws) {
    loop {
        match timeout(DEADLINE, ws.next()).await.unwrap() {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => return,
            _ => (),
        }
    }
}
async fn server<F, Fut>(f: F) -> (GeminiConfig, JoinHandle<()>)
where
    F: FnOnce(Ws) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GeminiConfig::new("synthetic-key", "source-model");
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        assert!(read(&mut ws).await["setup"].is_object());
        send(&mut ws, json!({"setupComplete":{}})).await;
        f(ws).await;
    });
    (config, task)
}
async fn next(session: &mut GeminiSession) -> GeminiEvent {
    timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
fn declaration() -> FunctionDeclaration {
    FunctionDeclaration {
        name: "lookup".into(),
        description: "Lookup".into(),
        parameters: json!({"type":"object"}),
    }
}

#[tokio::test]
async fn canceling_finish_admission_leaves_finish_retryable() {
    let (config, task) = server(|mut ws| async move {
        assert_eq!(read(&mut ws).await["realtimeInput"]["text"], "queued");
        send(&mut ws, json!({"serverContent":{"turnComplete":true}})).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut config = config;
    config.limits.queue_capacity = 1;
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    // No yield: the newly spawned connection worker cannot drain this slot yet.
    sender
        .try_send(Action::RealtimeText("queued".into()), Origin::default())
        .unwrap();
    {
        let finish = sender.finish();
        tokio::pin!(finish);
        assert!(matches!(futures_util::poll!(&mut finish), Poll::Pending));
    }
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    sender.finish().await.unwrap();
    assert!(timeout(Duration::from_millis(200), session.next_event())
        .await
        .expect("canceled finish must remain retryable")
        .is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn untagged_new_input_does_not_inherit_previous_origin() {
    let (config, task) = server(|mut ws| async move {
        for expected in ["tagged", "untagged"] {
            assert_eq!(read(&mut ws).await["realtimeInput"]["text"], expected);
            send(&mut ws, json!({"serverContent":{"modelTurn":{"parts":[{"text":expected}]},"turnComplete":true}})).await;
        }
        disconnected(&mut ws).await;
    }).await;
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    let origin = Origin {
        frame_id: Some(7),
        context_id: Some("first".into()),
    };
    sender
        .try_send(Action::RealtimeText("tagged".into()), origin.clone())
        .unwrap();
    assert_eq!(next(&mut session).await.origin, origin);
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    sender
        .try_send(Action::RealtimeText("untagged".into()), Origin::default())
        .unwrap();
    assert_eq!(next(&mut session).await.origin, Origin::default());
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    sender.finish().await.unwrap();
    assert!(timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
        .is_none());
    task.await.unwrap();
}

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn provider_errors_redact_url_encoded_keys_too() {
    use tokio_tungstenite::tungstenite::handshake::server::Request;
    let encoded = "synthetic%2Fsecret%2Bkey+value";
    let raw = "synthetic/secret+key value";
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GeminiConfig::new(raw, "source-model");
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut request_uri = String::new();
        let mut ws = tokio_tungstenite::accept_hdr_async(stream, |request: &Request, response| {
            request_uri = request.uri().to_string();
            Ok(response)
        })
        .await
        .unwrap();
        assert!(request_uri.contains(encoded));
        read(&mut ws).await;
        send(&mut ws, json!({"setupComplete":{}})).await;
        // Exercise the actual request encoding, an equivalent escaped space,
        // and the literal key after a long prefix that must not be cut first.
        let message = format!(
            "{} failed URL {request_uri}; other {}; raw {raw}; tail",
            "x".repeat(1023),
            encoded.replace('+', "%20")
        );
        send(&mut ws, json!({"error":{"code":400,"message":message}})).await;
        disconnected(&mut ws).await;
    });
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let error = timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(
        !format!("{error:?}").contains(encoded)
            && !error.to_string().contains(&encoded.replace('+', "%20"))
            && !error.to_string().contains(raw),
        "provider error leaked encoded API key: {error:?}"
    );
    task.await.unwrap();
}

#[tokio::test]
async fn suppressed_tool_calls_cannot_leave_hidden_pending_work_after_turn_complete() {
    let (release_tx, release_rx) = oneshot::channel();
    let (config, task) = server(|mut ws| async move {
        read(&mut ws).await;
        send(
            &mut ws,
            json!({"serverContent":{"modelTurn":{"parts":[{"text":"old"}]}}}),
        )
        .await;
        release_rx.await.unwrap();
        send(
            &mut ws,
            json!({"toolCall":{"functionCalls":[{"id":"stale","name":"lookup","args":{}}]}}),
        )
        .await;
        send(
            &mut ws,
            json!({"serverContent":{"turnComplete":true},"usageMetadata":{"totalTokenCount":1}}),
        )
        .await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut config = config;
    config.session.tools = vec![declaration()];
    config.limits.finish_timeout = Duration::from_millis(100);
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    sender
        .try_send(Action::RealtimeText("first".into()), Origin::default())
        .unwrap();
    assert!(matches!(next(&mut session).await.event, Event::Text { .. }));
    sender.interrupt().await;
    release_tx.send(()).unwrap();
    assert!(matches!(next(&mut session).await.event, Event::Usage(_)));
    sender.finish().await.unwrap();
    assert!(
        timeout(DEADLINE, session.next_event())
            .await
            .unwrap()
            .is_none(),
        "suppressed tool call was never exposed but still blocks finish"
    );
    task.await.unwrap();
}

#[tokio::test]
async fn locally_interrupted_tool_results_are_rejected_before_the_wire() {
    let (config, task) = server(|mut ws| async move {
        send(
            &mut ws,
            json!({"toolCall":{"functionCalls":[{"id":"old","name":"lookup","args":{}}]}}),
        )
        .await;
        match timeout(DEADLINE, ws.next()).await.unwrap() {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => (),
            Some(Ok(other)) => panic!("stale tool result reached the wire: {other:?}"),
        }
    })
    .await;
    let mut config = config;
    config.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    assert!(matches!(
        next(&mut session).await.event,
        Event::ToolCalls(_)
    ));
    sender.interrupt().await;
    sender
        .try_send(
            Action::ToolResponses(vec![FunctionResponse {
                id: "old".into(),
                name: "lookup".into(),
                response: json!({"result":"stale"}),
            }]),
            Origin::default(),
        )
        .unwrap();
    assert!(matches!(
        timeout(DEADLINE, session.next_event()).await.unwrap(),
        Some(Err(GeminiError::Invalid(_)))
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn provider_cancellation_after_local_interruption_retains_original_tool_origin() {
    let (release_tx, release_rx) = oneshot::channel();
    let (config, task) = server(|mut ws| async move {
        read(&mut ws).await;
        send(
            &mut ws,
            json!({"toolCall":{"functionCalls":[{"id":"old","name":"lookup","args":{}}]}}),
        )
        .await;
        release_rx.await.unwrap();
        send(
            &mut ws,
            json!({"serverContent":{"interrupted":true,"turnComplete":true}}),
        )
        .await;
        read(&mut ws).await;
        send(
            &mut ws,
            json!({"serverContent":{"modelTurn":{"parts":[{"text":"new"}]}}}),
        )
        .await;
        send(&mut ws, json!({"toolCallCancellation":{"ids":["old"]}})).await;
        send(&mut ws, json!({"serverContent":{"turnComplete":true}})).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut config = config;
    config.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    let original = Origin {
        frame_id: Some(123),
        context_id: Some("old".into()),
    };
    sender
        .try_send(Action::RealtimeText("old".into()), original.clone())
        .unwrap();
    assert!(matches!(
        next(&mut session).await.event,
        Event::ToolCalls(_)
    ));
    sender.interrupt().await;
    release_tx.send(()).unwrap();
    assert_eq!(next(&mut session).await.event, Event::Interrupted);
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    sender
        .try_send(
            Action::RealtimeText("new".into()),
            Origin {
                frame_id: Some(456),
                context_id: Some("new".into()),
            },
        )
        .unwrap();
    assert!(matches!(next(&mut session).await.event, Event::Text { .. }));
    let canceled = next(&mut session).await;
    assert!(matches!(canceled.event, Event::ToolCallsCancelled(ref calls) if calls[0].id == "old"));
    assert_eq!(canceled.origin, original);
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    sender.finish().await.unwrap();
    assert!(timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
        .is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn bundled_stale_tool_call_is_suppressed_before_its_terminal_boundary() {
    let (release_tx, release_rx) = oneshot::channel();
    let (config, task) = server(|mut ws| async move {
        read(&mut ws).await;
        send(
            &mut ws,
            json!({"serverContent":{"modelTurn":{"parts":[{"text":"old"}]}}}),
        )
        .await;
        release_rx.await.unwrap();
        send(
            &mut ws,
            json!({
                "toolCall":{"functionCalls":[{"id":"old","name":"lookup","args":{}}]},
                "serverContent":{
                    "modelTurn":{"parts":[
                        {"text":"stale thought","thought":true},
                        {"text":"stale text"},
                        {"inlineData":{"mimeType":"audio/pcm;rate=24000","data":"AQIDBA=="}}
                    ]},
                    "outputTranscription":{"text":"stale spoken text"},
                    "generationComplete":true,"turnComplete":true
                },
                "usageMetadata":{"totalTokenCount":1}
            }),
        )
        .await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut config = config;
    config.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    sender
        .try_send(Action::RealtimeText("old".into()), Origin::default())
        .unwrap();
    next(&mut session).await;
    sender.interrupt().await;
    release_tx.send(()).unwrap();
    assert!(
        matches!(next(&mut session).await.event, Event::Usage(_)),
        "stale tool call escaped after its own bundled turnComplete"
    );
    sender.finish().await.unwrap();
    assert!(timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
        .is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn argument_free_tool_calls_use_an_empty_object_when_args_are_omitted() {
    let (config, task) = server(|mut ws| async move {
        send(
            &mut ws,
            json!({"toolCall":{"functionCalls":[{"id":"empty","name":"lookup"}]}}),
        )
        .await;
        assert_eq!(
            read(&mut ws).await["toolResponse"]["functionResponses"][0]["id"],
            "empty"
        );
        send(&mut ws, json!({"serverContent":{"turnComplete":true}})).await;
        disconnected(&mut ws).await;
    })
    .await;
    let mut config = config;
    config.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    let event = next(&mut session).await;
    assert!(matches!(event.event, Event::ToolCalls(ref calls) if calls[0].args == json!({})));
    sender
        .try_send(
            Action::ToolResponses(vec![FunctionResponse {
                id: "empty".into(),
                name: "lookup".into(),
                response: json!({}),
            }]),
            Origin::default(),
        )
        .unwrap();
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    sender.finish().await.unwrap();
    assert!(timeout(DEADLINE, session.next_event())
        .await
        .unwrap()
        .is_none());
    task.await.unwrap();
}

#[tokio::test]
async fn missing_tool_ids_get_unique_owned_ids_and_results_echo_them() {
    let (config, task) = server(|mut ws| async move {
        send(
            &mut ws,
            json!({"toolCall":{"functionCalls":[
                {"name":"lookup"}, {"id":null,"name":"lookup"}, {"id":"","name":"lookup"}
            ]}}),
        )
        .await;
        let responses = read(&mut ws).await;
        let responses = responses["toolResponse"]["functionResponses"]
            .as_array()
            .unwrap();
        assert_eq!(responses.len(), 3);
        let ids: std::collections::HashSet<_> = responses
            .iter()
            .map(|response| response["id"].as_str().unwrap())
            .collect();
        assert_eq!(ids.len(), 3);
        assert!(ids.iter().all(|id| !id.is_empty()));
        send(&mut ws, json!({"serverContent":{"turnComplete":true}})).await;
        read(&mut ws).await;
        send(
            &mut ws,
            json!({"toolCall":{"functionCalls":[{"name":"lookup"}]}}),
        )
        .await;
        match timeout(DEADLINE, ws.next()).await.unwrap() {
            None | Some(Err(_)) | Some(Ok(Message::Close(_))) => (),
            Some(Ok(other)) => panic!("replayed completed response reached the wire: {other:?}"),
        }
    })
    .await;
    let mut config = config;
    config.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    let Event::ToolCalls(calls) = next(&mut session).await.event else {
        panic!("missing tool calls");
    };
    let ids: std::collections::HashSet<_> = calls.iter().map(|call| &call.id).collect();
    assert_eq!(ids.len(), 3);
    assert!(ids.iter().all(|id| !id.is_empty()));
    let responses: Vec<_> = calls
        .iter()
        .map(|call| FunctionResponse {
            id: call.id.clone(),
            name: call.name.clone(),
            response: json!({}),
        })
        .collect();
    sender
        .try_send(Action::ToolResponses(responses.clone()), Origin::default())
        .unwrap();
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    sender
        .try_send(Action::RealtimeText("again".into()), Origin::default())
        .unwrap();
    let Event::ToolCalls(new_calls) = next(&mut session).await.event else {
        panic!("missing next call");
    };
    assert!(!ids.contains(&new_calls[0].id));
    sender
        .try_send(
            Action::ToolResponses(vec![responses[0].clone()]),
            Origin::default(),
        )
        .unwrap();
    assert!(matches!(
        timeout(DEADLINE, session.next_event()).await.unwrap(),
        Some(Err(GeminiError::Invalid(_)))
    ));
    task.await.unwrap();
}

#[tokio::test]
async fn supplied_ids_cannot_replay_a_completed_generated_identity() {
    let (config, task) = server(|mut ws| async move {
        send(&mut ws, json!({"toolCall":{"functionCalls":[{"name":"lookup"}]}})).await;
        let result = read(&mut ws).await;
        let id = result["toolResponse"]["functionResponses"][0]["id"].clone();
        send(&mut ws, json!({"serverContent":{"turnComplete":true}})).await;
        // A peer cannot impersonate local identity by supplying its namespace
        // or by injecting the native API's identity-source metadata.
        send(&mut ws, json!({"toolCall":{"functionCalls":[{"id":id,"id_source":"local_generated","name":"lookup","args":{}}]}})).await;
        disconnected(&mut ws).await;
    }).await;
    let mut config = config;
    config.session.tools = vec![declaration()];
    let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    let event = next(&mut session).await;
    let Event::ToolCalls(calls) = &event.event else {
        panic!("missing tool call");
    };
    assert_eq!(calls[0].id_source, ToolCallIdSource::LocalGenerated);
    let mut mapper = GeminiFrameMapper::new(SessionMode::Conversation, "user");
    for frame in mapper.frames(&event).into_iter().filter(|frame| {
        matches!(
            frame.data,
            pipecat_core::FrameData::FunctionCallsStarted(_)
                | pipecat_core::FrameData::FunctionCallInProgress(_)
        )
    }) {
        assert_eq!(
            frame.metadata["gemini_tool_id_sources"][&calls[0].id],
            "local_generated"
        );
    }
    sender
        .try_send(
            Action::ToolResponses(vec![FunctionResponse {
                id: calls[0].id.clone(),
                name: "lookup".into(),
                response: json!({}),
            }]),
            Origin::default(),
        )
        .unwrap();
    assert_eq!(next(&mut session).await.event, Event::TurnComplete);
    assert_eq!(
        timeout(DEADLINE, session.next_event()).await.unwrap(),
        Some(Err(GeminiError::Protocol(
            "provider tool ID uses reserved local namespace"
        )))
    );
    task.await.unwrap();
}

#[tokio::test]
async fn generated_results_cannot_cross_sessions_or_survive_interruption() {
    let mut sessions = Vec::new();
    let mut tasks = Vec::new();
    let mut calls = Vec::new();
    for _ in 0..2 {
        let (mut config, task) = server(|mut ws| async move {
            send(
                &mut ws,
                json!({"toolCall":{"functionCalls":[{"name":"lookup"}]}}),
            )
            .await;
            match timeout(DEADLINE, ws.next()).await.unwrap() {
                None | Some(Err(_)) | Some(Ok(Message::Close(_))) => (),
                Some(Ok(other)) => panic!("unowned tool response reached the wire: {other:?}"),
            }
        })
        .await;
        config.session.tools = vec![declaration()];
        let mut session = GeminiClient::new(config).unwrap().connect().await.unwrap();
        let Event::ToolCalls(mut new_calls) = next(&mut session).await.event else {
            panic!("missing call");
        };
        calls.push(new_calls.remove(0));
        sessions.push(session);
        tasks.push(task);
    }
    assert_ne!(calls[0].id, calls[1].id);
    let response = FunctionResponse {
        id: calls[0].id.clone(),
        name: "lookup".into(),
        response: json!({}),
    };
    sessions[1]
        .sender()
        .try_send(
            Action::ToolResponses(vec![response.clone()]),
            Origin::default(),
        )
        .unwrap();
    assert!(matches!(
        timeout(DEADLINE, sessions[1].next_event()).await.unwrap(),
        Some(Err(GeminiError::Invalid(_)))
    ));
    sessions[0].sender().interrupt().await;
    sessions[0]
        .sender()
        .try_send(Action::ToolResponses(vec![response]), Origin::default())
        .unwrap();
    assert!(matches!(
        timeout(DEADLINE, sessions[0].next_event()).await.unwrap(),
        Some(Err(GeminiError::Invalid(_)))
    ));
    for task in tasks {
        task.await.unwrap();
    }
}

#[test]
fn initial_history_is_additive_and_existing_config_literals_stay_source_compatible() {
    let session = SessionConfig {
        model: "source-model".into(),
        modality: Modality::Audio,
        system_instruction: None,
        voice: None,
        language_code: None,
        temperature: None,
        max_output_tokens: None,
        top_p: None,
        top_k: None,
        input_transcription: true,
        output_transcription: true,
        activity_detection: AutomaticActivityDetection::default(),
        tools: vec![],
    };
    let config = GeminiConfig {
        endpoint: "ws://127.0.0.1:9/live".into(),
        api_key: "synthetic-key".into(),
        session,
        mode: SessionMode::Conversation,
        input_sample_rate: 16_000,
        limits: Limits::default(),
    };
    assert!(GeminiClient::new(config).is_ok());
    #[cfg(feature = "paired-history")]
    {
        let options = GeminiHistoryOptions {
            initial_seed: None,
            ..Default::default()
        };
        assert!(options.initial_seed.is_none());
    }
}

#[tokio::test]
async fn selected_model_grammar_and_public_connect_are_rejected_before_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let selected = InitialHistoryPolicy {
        inference_on_initialization: false,
    };
    for model in [
        "gemini-2.5-flash-native-audio-preview-12-2025",
        "models/gemini-3.8-live",
        "gemini-3-live",
        "gemini-live-preview-3.8-live",
    ] {
        let mut config = GeminiConfig::new("synthetic-key", model);
        config.endpoint = endpoint.clone();
        let client = GeminiClient::new(config)
            .unwrap()
            .with_initial_history(selected)
            .unwrap();
        let outcome = timeout(DEADLINE, client.connect())
            .await
            .expect("selected public connect did not reject locally");
        assert!(
            matches!(outcome, Err(GeminiError::Unsupported(_))),
            "model={model}"
        );
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(listener.poll_accept(cx)))
                .await
                .is_pending()
        );
    }
    for model in [
        "arbitrary-3-live",
        "notgemini-3-live",
        "gemini-2.0-live",
        "gemini-4.0-live",
        "gemini-3.8.1-live",
        "gemini-3.8x-live",
        "gemini-3.-live",
        "gemini-3x-live",
        "gemini-30-live",
        "gemini-2.50-live",
        "gemini-3..8-live",
        "gemini-live3-preview",
    ] {
        let mut config = GeminiConfig::new("synthetic-key", model);
        config.endpoint = endpoint.clone();
        // Generic raw model acceptance remains independent of selected parsing.
        let client = GeminiClient::new(config).unwrap();
        assert!(
            client.with_initial_history(selected).is_err(),
            "accepted invalid selected model {model}"
        );
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(listener.poll_accept(cx)))
                .await
                .is_pending()
        );
    }
    let mut transcription = GeminiConfig::transcription("synthetic-key", "gemini-3-live");
    transcription.endpoint = endpoint;
    assert!(GeminiClient::new(transcription)
        .unwrap()
        .with_initial_history(selected)
        .is_err());
    assert!(
        std::future::poll_fn(|cx| std::task::Poll::Ready(listener.poll_accept(cx)))
            .await
            .is_pending()
    );
}
