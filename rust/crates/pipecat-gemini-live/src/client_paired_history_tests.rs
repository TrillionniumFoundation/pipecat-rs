use super::*;
use serde_json::{json, Value};
use std::{task::Poll, time::Duration};
use tokio::sync::oneshot;

const WAIT: Duration = Duration::from_secs(3);

fn config() -> GeminiConfig {
    let mut config = GeminiConfig::new("synthetic-only", "gemini-3.8-live");
    config.session.input_transcription = true;
    config
}

fn parsed(value: Value) -> protocol::Parsed {
    protocol::parse(&serde_json::to_vec(&value).unwrap(), &config(), None).unwrap()
}

struct Peer {
    packets: mpsc::Sender<Value>,
    worker: JoinHandle<()>,
}

async fn fixture(paired: bool, thinking: bool) -> (GeminiSession, Peer) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config();
    config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
    let (packets, mut incoming) = mpsc::channel::<Value>(8);
    let worker = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
        let setup = timeout(WAIT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(serde_json::from_slice::<Value>(&setup.into_data()).unwrap()["setup"].is_object());
        socket
            .send(Message::Text(
                json!({"setupComplete": {}}).to_string().into(),
            ))
            .await
            .unwrap();
        loop {
            tokio::select! {
                packet = incoming.recv() => match packet {
                    Some(packet) => socket.send(Message::Text(packet.to_string().into())).await.unwrap(),
                    None => break,
                },
                message = socket.next() => match message {
                    None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                    Some(Ok(_)) => {},
                },
            }
        }
    });
    let mut client = GeminiClient::new(config).unwrap();
    if thinking {
        client = client.with_thinking(ThinkingPolicy::default()).unwrap();
    }
    let session = if paired {
        client.connect_paired_history().await.unwrap()
    } else {
        client.connect().await.unwrap()
    };
    (session, Peer { packets, worker })
}

async fn receive(session: &mut GeminiSession) -> ReceivedEvent {
    timeout(WAIT, session.next_event_with_details())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn close(mut session: GeminiSession, peer: Peer) {
    timeout(WAIT, session.cancel_and_join())
        .await
        .unwrap()
        .unwrap();
    assert!(
        session.worker.is_none(),
        "actual inner JoinHandle was awaited"
    );
    timeout(WAIT, peer.worker).await.unwrap().unwrap();
}

#[test]
fn source_watermark_ignores_empty_and_interim_and_is_checked() {
    let mut watermarks = InputWatermarks::default();
    let mut first = parsed(json!({"serverContent": {
        "modelTurn": {"parts": [{"text": "before input"}]},
        "interimInputTranscription": {"text": "not actual"},
        "inputTranscription": {"text": "actual.", "languageCode": "ja"}
    }}));
    let descriptor = watermarks.observe(&mut first).unwrap();
    assert_eq!(descriptor.sequence, 1);
    assert_eq!(descriptor.received_input_watermark, 1);
    assert_eq!(
        descriptor.actual_input,
        Some(Transcript {
            text: "actual.".into(),
            language_code: Some("ja".into()),
        })
    );
    assert!(matches!(first.events.first(), Some(Event::Text { .. })));
    for (sequence, wire) in [
        json!({"serverContent": {"inputTranscription": {"text": ""}}}),
        json!({"serverContent": {"interimInputTranscription": {"text": "hypothesis"}}}),
        json!({"usageMetadata": {"promptTokenCount": 4}}),
    ]
    .into_iter()
    .enumerate()
    {
        let descriptor = watermarks.observe(&mut parsed(wire)).unwrap();
        assert_eq!(descriptor.sequence, sequence as u64 + 2);
        assert_eq!(descriptor.received_input_watermark, 1);
        assert!(descriptor.actual_input.is_none());
    }
    let descriptor = watermarks
        .observe(&mut parsed(json!({"serverContent": {
            "inputTranscription": {"text": " \t"}
        }})))
        .unwrap();
    assert_eq!(
        descriptor.received_input_watermark, 2,
        "whitespace is actual source input"
    );

    for (sequence, received) in [(u64::MAX, 0), (4, u64::MAX)] {
        let mut watermarks = InputWatermarks { sequence, received };
        let mut packet = parsed(json!({"serverContent": {"inputTranscription": {"text": "x"}}}));
        assert!(matches!(
            watermarks.observe(&mut packet),
            Err(GeminiError::TooLarge)
        ));
        assert_eq!(
            (watermarks.sequence, watermarks.received),
            (sequence, received)
        );
        assert!(
            packet.actual_input.is_some(),
            "failed observation did not consume source"
        );
    }
}

#[tokio::test]
async fn real_packet_shares_input_lookahead_and_keeps_public_model_first_order() {
    let wire = json!({
        "serverContent": {
            "modelTurn": {"parts": [
                {"text": "thought", "thought": true},
                {"text": "answer"},
                {"inlineData": {"mimeType": "audio/pcm;rate=24000", "data": "AAA="}}
            ]},
            "interimInputTranscription": {"text": "hypothesis"},
            "inputTranscription": {"text": "actual.", "languageCode": "en"},
            "outputTranscription": {"text": "spoken answer"},
            "generationComplete": true,
            "turnComplete": true
        },
        "usageMetadata": {"promptTokenCount": 1, "responseTokenCount": 2, "totalTokenCount": 3,
            "thoughtsTokenCount": 1}
    });
    let expected = parsed(wire.clone()).events;
    assert!(matches!(expected.first(), Some(Event::Text { .. })));
    for paired in [false, true] {
        let (mut session, peer) = fixture(paired, false).await;
        peer.packets.send(wire.clone()).await.unwrap();
        let mut shared = None;
        for expected in &expected {
            let event = receive(&mut session).await;
            assert_eq!(&event.event.event, expected);
            assert_eq!(
                event.usage_details.is_some(),
                matches!(expected, Event::Usage(_))
            );
            if paired {
                let packet = event.packet.unwrap();
                assert_eq!(packet.sequence, 1);
                assert_eq!(packet.received_input_watermark, 1);
                assert_eq!(packet.actual_input.as_ref().unwrap().text, "actual.");
                if let Some(first) = &shared {
                    assert!(Arc::ptr_eq(first, &packet));
                } else {
                    shared = Some(packet);
                }
            } else {
                assert!(
                    event.packet.is_none(),
                    "raw connect does not select private history"
                );
            }
        }
        // Usage-only envelopes inherit the receive prefix without fabricating
        // a source contribution, even if a production consumer ignores usage.
        peer.packets
            .send(json!({"usageMetadata": {"thoughtsTokenCount": 0}}))
            .await
            .unwrap();
        let usage = receive(&mut session).await;
        assert!(matches!(usage.event.event, Event::Usage(_)));
        if paired {
            let packet = usage.packet.unwrap();
            assert_eq!((packet.sequence, packet.received_input_watermark), (2, 1));
            assert!(packet.actual_input.is_none());
        }
        close(session, peer).await;
    }
}

#[tokio::test]
async fn thinking_completion_keeps_original_packet_for_both_idle_paths() {
    for repeated_turn_complete in [false, true] {
        let (mut session, peer) = fixture(true, true).await;
        peer.packets
            .send(json!({"serverContent": {
                "modelTurn": {"parts": [{"text": "answer"}]},
                "inputTranscription": {"text": "first."},
                "turnComplete": true, "interactionStatus": "IN_PROGRESS"
            }}))
            .await
            .unwrap();
        let text = receive(&mut session).await;
        let original = text.packet.unwrap();
        assert!(matches!(text.event.event, Event::Text { .. }));
        assert!(matches!(
            receive(&mut session).await.event.event,
            Event::InputTranscription { .. }
        ));
        peer.packets
            .send(json!({"serverContent": {"inputTranscription": {"text": "later."}}}))
            .await
            .unwrap();
        let later = receive(&mut session).await;
        assert_eq!(later.packet.unwrap().received_input_watermark, 2);
        peer.packets
            .send(json!({"serverContent": {
                "inputTranscription": {"text": "idle input."},
                "interactionStatus": "IDLE", "turnComplete": repeated_turn_complete
            }}))
            .await
            .unwrap();
        let idle_input = receive(&mut session).await;
        assert!(matches!(
            idle_input.event.event,
            Event::InputTranscription { .. }
        ));
        assert_eq!(idle_input.packet.unwrap().received_input_watermark, 3);
        let completion = receive(&mut session).await;
        assert_eq!(completion.event.event, Event::TurnComplete);
        let source = completion.packet.unwrap();
        assert!(Arc::ptr_eq(&source, &original));
        assert_eq!((source.sequence, source.received_input_watermark), (1, 1));
        close(session, peer).await;
    }
}

#[tokio::test]
async fn suppressed_output_never_discards_actual_input_or_its_next_prefix() {
    let (mut session, peer) = fixture(true, false).await;
    peer.packets
        .send(json!({"serverContent": {"modelTurn": {"parts": [{"text": "old answer"}]}}}))
        .await
        .unwrap();
    let old = receive(&mut session).await;
    assert_eq!(old.packet.unwrap().received_input_watermark, 0);
    session.sender().interrupt().await;
    peer.packets
        .send(json!({
            "serverContent": {
                "modelTurn": {"parts": [{"text": "suppressed output"}]},
                "inputTranscription": {"text": "retained input."},
                "turnComplete": true
            },
            "usageMetadata": {"totalTokenCount": 3}
        }))
        .await
        .unwrap();
    let input = receive(&mut session).await;
    assert!(matches!(
        input.event.event,
        Event::InputTranscription { .. }
    ));
    let source = input.packet.unwrap();
    assert_eq!((source.sequence, source.received_input_watermark), (2, 1));
    assert_eq!(
        source.actual_input.as_ref().unwrap().text,
        "retained input."
    );
    let usage = receive(&mut session).await;
    assert!(matches!(usage.event.event, Event::Usage(_)));
    assert!(Arc::ptr_eq(&source, &usage.packet.unwrap()));
    peer.packets
        .send(json!({"serverContent": {"modelTurn": {"parts": [{"text": "next answer"}]}}}))
        .await
        .unwrap();
    let next = receive(&mut session).await;
    assert_eq!(
        next.event.event,
        Event::Text {
            text: "next answer".into(),
            thought: false
        }
    );
    let dependency = next.packet.unwrap();
    assert_eq!(
        (dependency.sequence, dependency.received_input_watermark),
        (3, 1)
    );
    assert!(dependency.actual_input.is_none());
    close(session, peer).await;
}

#[tokio::test]
async fn selected_tools_fail_before_earlier_model_or_input_dispatch() {
    for tool_field in [
        json!({"toolCall": {"functionCalls": [{"id": "one", "name": "unexpected", "args": {}}]}}),
        json!({"toolCallCancellation": {"ids": ["unknown"]}}),
        json!({"toolCallCancellation": {"ids": []}}),
        json!({"toolCall": null}),
        json!({"toolCallCancellation": null}),
    ] {
        let (mut session, peer) = fixture(true, false).await;
        let mut wire = json!({"serverContent": {
            "modelTurn": {"parts": [{"text": "must not dispatch"}]},
            "inputTranscription": {"text": "must not dispatch either."}
        }});
        wire.as_object_mut()
            .unwrap()
            .extend(tool_field.as_object().unwrap().clone());
        peer.packets.send(wire).await.unwrap();
        assert!(matches!(
            timeout(WAIT, session.next_event_with_details())
                .await
                .unwrap(),
            Some(Err(GeminiError::Unsupported(
                "paired history provider tools"
            )))
        ));
        assert!(
            session.worker.is_none(),
            "error read joined actual provider worker"
        );
        close(session, peer).await;
    }
}

#[test]
fn paired_preflight_rejects_resumption_and_all_tool_configuration() {
    let valid = GeminiClient::new(config()).unwrap();
    valid.validate_paired_history().unwrap();
    assert!(GeminiClient::new(GeminiConfig::transcription(
        "synthetic-only",
        "source-model"
    ))
    .unwrap()
    .validate_paired_history()
    .is_err());
    assert!(valid
        .clone()
        .with_session_resumption(ResumptionPolicy::default())
        .unwrap()
        .validate_paired_history()
        .is_err());
    let mut disabled = config();
    disabled.session.input_transcription = false;
    assert!(GeminiClient::new(disabled)
        .unwrap()
        .validate_paired_history()
        .is_err());
    let mut tools = config();
    tools.session.tools = vec![FunctionDeclaration {
        name: "test".into(),
        description: String::new(),
        parameters: json!({"type": "object"}),
    }];
    let tools = GeminiClient::new(tools).unwrap();
    assert!(tools.validate_paired_history().is_err());
    assert!(tools
        .with_non_blocking_tools(vec!["test".into()])
        .unwrap()
        .validate_paired_history()
        .is_err());
}

#[tokio::test]
async fn selected_setup_rejects_tool_fields_without_changing_raw_setup() {
    for paired in [false, true] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config();
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            assert!(timeout(WAIT, socket.next()).await.unwrap().is_some());
            socket
                .send(Message::Text(
                    json!({
                        "setupComplete": {}, "toolCallCancellation": {"ids": []}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            assert!(matches!(
                timeout(WAIT, socket.next()).await.unwrap(),
                None | Some(Err(_)) | Some(Ok(Message::Close(_)))
            ));
        });
        let client = GeminiClient::new(config).unwrap();
        if paired {
            assert!(matches!(
                client.connect_paired_history().await,
                Err(GeminiError::Unsupported("paired history provider tools"))
            ));
        } else {
            let mut raw = client.connect().await.unwrap();
            timeout(WAIT, raw.cancel_and_join()).await.unwrap().unwrap();
        }
        timeout(WAIT, peer).await.unwrap().unwrap();
    }
}

#[tokio::test]
async fn selected_cancel_join_wait_retains_worker_when_canceled() {
    // Use an actual worker and actual loopback socket, with its final return
    // deliberately held independently of peer closure.
    let (mut session, peer) = fixture(true, false).await;
    let original = session.worker.take().unwrap();
    let (release, released) = oneshot::channel();
    let (joined, inner_joined) = oneshot::channel();
    session.worker = Some(tokio::spawn(async move {
        original.await.map_err(|_| GeminiError::Transport)??;
        joined.send(()).unwrap();
        released.await.unwrap();
        Err(GeminiError::Protocol("held selected terminal failure"))
    }));
    session.sender.cancel();
    timeout(WAIT, inner_joined).await.unwrap().unwrap();
    {
        let pending = session.cancel_and_join();
        tokio::pin!(pending);
        assert!(matches!(futures_util::poll!(&mut pending), Poll::Pending));
    }
    assert!(
        session.worker.is_some(),
        "dropped waiter retained actual handle"
    );
    release.send(()).unwrap();
    assert_eq!(
        timeout(WAIT, session.cancel_and_join()).await.unwrap(),
        Err(GeminiError::Protocol("held selected terminal failure"))
    );
    assert!(session.worker.is_none());
    close(session, peer).await;
}
