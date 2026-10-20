//! Native Inworld protocol and owner lifecycle, exclusively synthetic loopback peers.
mod common;
use base64::{engine::general_purpose::STANDARD, Engine};
use common::*;
use futures_util::SinkExt;
use pipecat_core::{FrameData, FunctionCall};
use pipecat_inworld_realtime::*;
use serde_json::{json, Value};
use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::tungstenite::Message;

#[tokio::test]
#[allow(clippy::result_large_err)]
async fn basic_and_explicit_bearer_use_distinct_keys_and_source_defaults() {
    let keys = Arc::new(Mutex::new(HashSet::new()));
    for auth_type in [AuthType::Basic, AuthType::Bearer] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = InworldConfig::new("already-base64-or-jwt");
        config.endpoint = format!(
            "ws://{}/api/v1/realtime/session",
            listener.local_addr().unwrap()
        );
        config.auth_type = auth_type;
        let expected_auth = match auth_type {
            AuthType::Basic => "Basic already-base64-or-jwt",
            AuthType::Bearer => "Bearer already-base64-or-jwt",
        };
        let keys = keys.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = bounded(listener.accept()).await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                      response| {
                    assert_eq!(request.headers()["authorization"], expected_auth);
                    assert!(!request.headers().contains_key("openai-beta"));
                    let url = url::Url::parse(&format!("ws://localhost{}", request.uri())).unwrap();
                    let query: std::collections::HashMap<_, _> =
                        url.query_pairs().into_owned().collect();
                    assert_eq!(query.len(), 2);
                    assert_eq!(query["protocol"], "realtime");
                    assert!(query["key"].starts_with("pipecat-realtime-"));
                    assert!(query["key"].len() <= 96);
                    assert!(keys.lock().unwrap().insert(query["key"].clone()));
                    Ok(response)
                },
            )
            .await
            .unwrap();
            quiet(&mut ws).await;
            let update = handshake(&mut ws).await;
            let session = &update["session"];
            assert_eq!(session["model"], "openai/gpt-4.1-mini");
            assert_eq!(session["output_modalities"], json!(["audio", "text"]));
            assert_eq!(
                session["audio"]["input"]["format"],
                json!({"type":"audio/pcm","rate":24000})
            );
            assert_eq!(
                session["audio"]["input"]["transcription"]["model"],
                "inworld/inworld-stt-1"
            );
            assert_eq!(
                session["audio"]["input"]["turn_detection"]["type"],
                "semantic_vad"
            );
            assert_eq!(session["audio"]["output"]["model"], "inworld-tts-2");
            assert_eq!(session["audio"]["output"]["voice"], "Clive");
            closed(&mut ws).await;
        });
        let mut session = ready(config).await;
        session.shutdown().await.unwrap();
        bounded(server).await.unwrap();
    }
    assert_eq!(keys.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn exact_sixty_ms_chunks_and_short_tail_precede_manual_commit() {
    for rate in [8000, 16000, 24000, 32000, 44100, 48000] {
        let bytes_per_chunk = (rate as usize * 2 * 60) / 1000;
        let pcm: Vec<u8> = (0..bytes_per_chunk + 6).map(|n| (n % 251) as u8).collect();
        let expected = pcm.clone();
        let (mut config, server) = peer(move |mut ws| async move {
            let update = handshake(&mut ws).await;
            assert!(update["session"]["audio"]["input"]["turn_detection"].is_null());
            let first = read(&mut ws).await;
            assert_eq!(first["type"], "input_audio_buffer.append");
            assert_eq!(
                STANDARD.decode(first["audio"].as_str().unwrap()).unwrap(),
                expected[..bytes_per_chunk]
            );
            let tail = read(&mut ws).await;
            assert_eq!(tail["type"], "input_audio_buffer.append");
            assert_eq!(
                STANDARD.decode(tail["audio"].as_str().unwrap()).unwrap(),
                expected[bytes_per_chunk..]
            );
            assert_eq!(read(&mut ws).await["type"], "input_audio_buffer.commit");
            quiet(&mut ws).await;
            closed(&mut ws).await;
        })
        .await;
        config.session = SessionProperties::manual();
        config
            .session
            .audio
            .as_mut()
            .unwrap()
            .input
            .as_mut()
            .unwrap()
            .format = AudioFormat::Pcm { rate };
        let mut session = ready(config).await;
        let sender = session.sender();
        sender
            .send(Action::AppendAudio(pcm[..100].to_vec()))
            .await
            .unwrap();
        sender
            .send(Action::AppendAudio(pcm[100..].to_vec()))
            .await
            .unwrap();
        sender.send(Action::CommitAudio).await.unwrap();
        tokio::time::sleep(Duration::from_millis(90)).await;
        session.shutdown().await.unwrap();
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn readiness_rejects_audio_and_mismatched_ack_never_unlocks_it() {
    let (release, wait) = tokio::sync::oneshot::channel();
    let (mut config, server) = peer(move |mut ws| async move {
        wire(&mut ws, json!({"type":"session.created"})).await;
        let update = read(&mut ws).await;
        let mut wrong = update["session"].clone();
        wrong["model"] = json!("wrong-model");
        wire(&mut ws, json!({"type":"session.updated","session":wrong})).await;
        wait.await.unwrap();
        closed(&mut ws).await;
    })
    .await;
    config.limits.connect_timeout = Duration::from_millis(300);
    let mut session = InworldClient::new(config).unwrap().connect().await.unwrap();
    assert_eq!(
        session.sender().try_send(Action::AppendAudio(vec![1, 2])),
        Err(RealtimeError::NotReady)
    );
    next(&mut session).await;
    release.send(()).unwrap();
    let mut rejected = false;
    while let Some(event) = bounded(session.next_event()).await {
        if event.is_err() {
            rejected = true;
        }
    }
    assert!(rejected || !session.sender().is_ready());
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn readiness_deadline_terminates_owned_session() {
    let (mut config, server) = peer(|mut ws| async move {
        wire(&mut ws, json!({"type":"session.created"})).await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        closed(&mut ws).await;
    })
    .await;
    config.limits.connect_timeout = Duration::from_millis(60);
    let mut session = InworldClient::new(config).unwrap().connect().await.unwrap();
    let mut error = None;
    while let Some(result) = bounded(session.next_event()).await {
        if let Err(value) = result {
            error = Some(value);
        }
    }
    assert!(matches!(error, Some(RealtimeError::Timeout(_))));
    assert!(!session.sender().is_ready());
    bounded(server).await.unwrap();
}

#[test]
fn unsafe_urls_headers_unsupported_formats_and_modalities_are_rejected() {
    for endpoint in [
        "http://127.0.0.1/session",
        "ws://localhost/session",
        "ws://192.0.2.1/session",
        "wss://user:secret@example.com/session",
        "wss://example.com/session?key=collision",
        "wss://example.com/session?protocol=realtime",
        "wss://example.com/session?arbitrary=1",
        "wss://example.com/session#fragment",
    ] {
        let mut config = InworldConfig::new("synthetic");
        config.endpoint = endpoint.into();
        let error = config.validate().unwrap_err();
        assert!(!format!("{error:?}").contains(endpoint));
    }
    for credential in ["", "synthetic\r\ninjected: true"] {
        assert!(InworldConfig::new(credential).validate().is_err());
    }
    for rate in [0, 12000, 22050, 96000] {
        let mut config = InworldConfig::new("synthetic");
        config
            .session
            .audio
            .as_mut()
            .unwrap()
            .input
            .as_mut()
            .unwrap()
            .format = AudioFormat::Pcm { rate };
        assert!(config.validate().is_err());
    }
    for kind in ["audio/pcmu", "audio/pcma"] {
        assert!(
            serde_json::from_value::<AudioFormat>(json!({"type":kind})).is_err(),
            "G.711 is not the declared native PCM profile"
        );
    }
    let mut config = InworldConfig::new("synthetic-secret-not-for-debug");
    config.session.output_modalities = Some(vec![Modality::Text]);
    assert!(config.validate().is_err());
    assert!(!format!("{config:?}").contains("synthetic-secret-not-for-debug"));
}

#[tokio::test]
async fn provider_data_is_preserved_without_mutation_and_auto_continuation_is_rejected() {
    let (mut config, server) = peer(|mut ws| async move {
        let update = handshake(&mut ws).await;
        assert_eq!(
            update["session"]["providerData"]["tenant_hint"],
            "synthetic-tenant"
        );
        closed(&mut ws).await;
    })
    .await;
    config.session.provider_data =
        Some(serde_json::from_value(json!({"tenant_hint":"synthetic-tenant"})).unwrap());
    let saved = config.session.clone();
    let mut session = ready(config.clone()).await;
    assert_eq!(config.session, saved);
    session.shutdown().await.unwrap();
    bounded(server).await.unwrap();
    config.session.provider_data =
        Some(serde_json::from_value(json!({"auto_tool_response":true})).unwrap());
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn fragmented_audio_transcript_usage_and_split_pcm_are_mapped_once() {
    use tokio_tungstenite::tungstenite::protocol::frame::{
        coding::{Data, OpCode},
        Frame as WsFrame,
    };
    let (config,server)=peer(|mut ws|async move {
        handshake(&mut ws).await;
        assert_eq!(read(&mut ws).await["type"],"response.create");
        response(&mut ws,"r",false).await;
        let mut value=audio_delta("r","a",&[1,2,3]); value["event_id"]=json!("fragment");
        let bytes=value.to_string().into_bytes(); let middle=bytes.len()/2;
        ws.send(Message::Frame(WsFrame::message(bytes[..middle].to_vec(),OpCode::Data(Data::Text),false))).await.unwrap();
        ws.send(Message::Frame(WsFrame::message(bytes[middle..].to_vec(),OpCode::Data(Data::Continue),true))).await.unwrap();
        wire(&mut ws,audio_delta("r","a",&[4])).await;
        wire(&mut ws,json!({"type":"response.output_audio_transcript.delta","response_id":"r","item_id":"a","delta":"spoken"})).await;
        wire(&mut ws,json!({"type":"response.output_audio.done","response_id":"r","item_id":"a"})).await;
        wire(&mut ws,json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"u","delta":"ques"})).await;
        wire(&mut ws,json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"u","delta":"tion"})).await;
        wire(&mut ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u","transcript":"question"})).await;
        wire(&mut ws,json!({"type":"response.done","response":{"id":"r","status":"completed","output":[],"usage":{"total_tokens":12,"input_tokens":7,"output_tokens":5}}})).await;
        peer_close(&mut ws).await;
    }).await;
    let mut session = ready(config).await;
    session
        .sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut mapper = RealtimeFrameMapper::new("synthetic-speaker");
    let mut frames = vec![];
    while let Some(event) = bounded(session.next_event()).await {
        frames.extend(mapper.frames(&event.unwrap()).unwrap());
    }
    let pcm: Vec<u8> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsAudio(a) => Some(a.audio.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(pcm, [1, 2, 3, 4]);
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
    assert_eq!(frames.iter().filter(|f|matches!(&f.data,FrameData::LlmText(t) if t.text=="spoken" && !t.append_to_context)).count(),1);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(&f.data,FrameData::TtsText(t) if t.text.text=="spoken"))
            .count(),
        1
    );
    assert!(frames.iter().any(|f|matches!(&f.data,FrameData::Transcription(t) if t.text=="question" && t.user_id=="synthetic-speaker")));
    assert!(frames
        .iter()
        .any(|f| matches!(&f.data,FrameData::InterimTranscription(t) if t.text=="question")));
    assert!(frames
        .iter()
        .any(|f| matches!(f.data, FrameData::Metrics(_))));
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn malformed_known_events_invalid_base64_and_oversized_messages_fail_sanitized() {
    for payload in ["{not-json".to_string(),json!({"event_id":"malformed","type":"response.created","response":{"id":"r"}}).to_string(),json!({"event_id":"malformed","type":"response.output_audio.delta","response_id":"r","item_id":"a","output_index":0,"content_index":0,"delta":"not@base64"}).to_string(),"x".repeat(4096)] {
        let (mut config,server)=peer(move |mut ws|async move {
            handshake(&mut ws).await;
            assert_eq!(read(&mut ws).await["type"],"response.create");
            response(&mut ws,"r",false).await;
            let _=ws.send(Message::Text(payload.into())).await;
            closed(&mut ws).await;
        }).await;
        config.limits.max_message_bytes=2048; config.limits.max_audio_chunk_bytes=1024;
        let mut session=ready(config).await;
        session.sender().send(Action::CreateResponse(Default::default())).await.unwrap();
        let mut errors=vec![];
        while let Some(event)=bounded(session.next_event()).await { if let Err(e)=event { errors.push(e); } }
        assert_eq!(errors.len(),1);
        let message=format!("{:?}",errors[0]); assert!(!message.contains("not@base64")); assert!(!message.contains("{not-json"));
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn odd_terminal_pcm_is_rejected_without_partial_sample_output() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        read(&mut ws).await;
        response(&mut ws, "r", false).await;
        wire(&mut ws, audio_delta("r", "a", &[1])).await;
        wire(
            &mut ws,
            json!({"type":"response.output_audio.done","response_id":"r","item_id":"a"}),
        )
        .await;
        response(&mut ws, "r", true).await;
        peer_close(&mut ws).await;
    })
    .await;
    let mut session = ready(config).await;
    session
        .sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut mapper = RealtimeFrameMapper::default();
    let mut rejected_audio_done = false;
    while let Some(event) = bounded(session.next_event()).await {
        let event =
            event.expect("malformed PCM mapping must not be confused with transport failure");
        let audio_done = matches!(&event.event, Event::AudioDone { .. });
        let response_done = matches!(&event.event, Event::ResponseDone { .. });
        match mapper.frames(&event) {
            Err(error) if audio_done => {
                assert_eq!(
                    error,
                    RealtimeError::Protocol("audio ends with a partial PCM sample")
                );
                rejected_audio_done = true;
            }
            Err(error) if response_done => assert_eq!(
                error,
                RealtimeError::Protocol("response ends with a partial PCM sample")
            ),
            Err(error) => panic!("unexpected mapper error: {error:?}"),
            Ok(frames) => {
                assert!(
                    !audio_done,
                    "AudioDone must reject the incomplete PCM sample"
                );
                assert!(!frames
                    .iter()
                    .any(|frame| matches!(frame.data, FrameData::TtsAudio(_))));
            }
        }
    }
    assert!(rejected_audio_done);
    assert_eq!(session.cleanup_error(), None);
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn final_result_encoding_preserves_json_strings_and_false_zero_null() {
    for (index, result) in [json!("{\"answer\":7}"), json!(false), json!(0), Value::Null]
        .into_iter()
        .enumerate()
    {
        let expected = match &result {
            Value::String(s) => s.clone(),
            value => value.to_string(),
        };
        let (mut config, server) = peer(move |mut ws| async move {
            handshake(&mut ws).await;
            read(&mut ws).await;
            response(&mut ws, "r", false).await;
            call(&mut ws, "call", "echo", "{}", true).await;
            let output = read(&mut ws).await;
            assert_eq!(output["item"]["type"], "function_call_output");
            assert_eq!(output["item"]["output"], expected);
            assert_eq!(output["item"]["call_id"], "call");
            quiet(&mut ws).await;
            response(&mut ws, "r", true).await;
            peer_close(&mut ws).await;
        })
        .await;
        config.session.tools = Some(vec![FunctionTool::new(
            "echo",
            "synthetic",
            json!({"type":"object"}),
        )
        .into()]);
        let mut session = ready(config).await;
        let tx = session.sender();
        tx.send(Action::CreateResponse(Default::default()))
            .await
            .unwrap();
        admitted_call(&mut session).await;
        tx.send(Action::ToolResult {
            call_id: "call".into(),
            output: result,
            is_final: true,
        })
        .await
        .unwrap();
        assert_eq!(
            tx.send(Action::ToolResult {
                call_id: "call".into(),
                output: json!(index),
                is_final: true
            })
            .await,
            Err(RealtimeError::UnknownToolCall)
        );
        while let Some(event) = bounded(session.next_event()).await {
            event.unwrap();
        }
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn intermediate_and_mismatched_results_never_answer_the_call() {
    let (mut config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        read(&mut ws).await;
        response(&mut ws, "r", false).await;
        call(&mut ws, "c", "echo", "{\"n\":7}", false).await;
        let output = read(&mut ws).await;
        assert_eq!(output["item"]["output"], "final");
        quiet(&mut ws).await;
        response(&mut ws, "r", true).await;
        peer_close(&mut ws).await;
    })
    .await;
    config.session.tools = Some(vec![FunctionTool::new(
        "echo",
        "synthetic",
        json!({"type":"object"}),
    )
    .into()]);
    let mut session = ready(config).await;
    let tx = session.sender();
    tx.send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    admitted_call(&mut session).await;
    assert!(tx
        .send(Action::ToolResult {
            call_id: "c".into(),
            output: json!("progress"),
            is_final: false
        })
        .await
        .is_err());
    for (id, name, arguments) in [
        ("unknown", "echo", json!({"n":7})),
        ("c", "wrong", json!({"n":7})),
        ("c", "echo", json!({"n":8})),
    ] {
        assert!(tx
            .send(Action::CheckedToolResult {
                call: FunctionCall {
                    tool_call_id: id.into(),
                    function_name: name.into(),
                    arguments
                },
                output: json!("forged"),
                is_final: true
            })
            .await
            .is_err());
    }
    tx.send(Action::CheckedToolResult {
        call: FunctionCall {
            tool_call_id: "c".into(),
            function_name: "echo".into(),
            arguments: json!({"n":7}),
        },
        output: json!("final"),
        is_final: true,
    })
    .await
    .unwrap();
    while let Some(event) = bounded(session.next_event()).await {
        event.unwrap();
    }
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn dropped_session_closes_even_when_sender_is_retained() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        closed(&mut ws).await;
    })
    .await;
    let session = ready(config).await;
    let tx = session.sender();
    drop(session);
    bounded(server).await.unwrap();
    assert_eq!(
        tx.send(Action::ClearAudio).await,
        Err(RealtimeError::Closed)
    );
}

#[tokio::test]
async fn response_timeout_and_event_queue_saturation_are_bounded() {
    for saturate in [false, true] {
        let (mut config, server) = peer(move |mut ws| async move {
            handshake(&mut ws).await;
            assert_eq!(read(&mut ws).await["type"], "response.create");
            response(&mut ws, "r", false).await;
            if saturate {
                for index in 0..32 {
                    let mut event = audio_delta("r", "a", &[1, 2]);
                    event["event_id"] = json!(format!("bounded-{index}"));
                    if ws
                        .send(Message::Text(event.to_string().into()))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
            closed(&mut ws).await;
        })
        .await;
        config.limits.queue_capacity = 4;
        config.limits.response_timeout = Duration::from_millis(80);
        let mut session = ready(config).await;
        session
            .sender()
            .send(Action::CreateResponse(Default::default()))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut error = None;
        while let Some(event) = bounded(session.next_event()).await {
            if let Err(e) = event {
                error = Some(e);
            }
        }
        assert!(matches!(
            error,
            Some(RealtimeError::Backpressure | RealtimeError::Timeout(_))
        ));
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn provider_error_and_midresponse_eof_preserve_primary_failure() {
    for provider_error in [false, true] {
        let (config,server)=peer(move |mut ws|async move {
            handshake(&mut ws).await; read(&mut ws).await; response(&mut ws,"r",false).await;
            if provider_error {wire(&mut ws,json!({"type":"error","error":{"type":"provider_error","code":"conversation_already_has_active_response","message":"synthetic-inworld-credential provider-secret"}})).await; closed(&mut ws).await;}
        }).await;
        let mut session = ready(config).await;
        session
            .sender()
            .send(Action::CreateResponse(Default::default()))
            .await
            .unwrap();
        let mut observed = false;
        while let Some(event) = bounded(session.next_event()).await {
            match event {
                Ok(event) => {
                    let debug = format!("{event:?}");
                    assert!(!debug.contains("provider-secret"));
                    if matches!(event.event, Event::Error { .. }) {
                        observed = true;
                    }
                }
                Err(error) => {
                    observed = true;
                    assert!(!format!("{error:?}").contains("provider-secret"));
                }
            }
        }
        assert!(observed);
        assert!(session.shutdown().await.is_err());
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn full_settings_replace_atomically_and_bad_update_never_reaches_wire() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        let update = read(&mut ws).await;
        assert_eq!(update["type"], "session.update");
        assert_eq!(update["session"]["instructions"], "new instructions");
        assert_eq!(update["session"]["model"], "synthetic/replacement-model");
        wire(
            &mut ws,
            json!({"type":"session.updated","session":update["session"]}),
        )
        .await;
        closed(&mut ws).await;
    })
    .await;
    let mut session = ready(config).await;
    let tx = session.sender();
    let original = tx.projected_session();
    let mut bad = original.clone();
    bad.provider_data = Some(serde_json::from_value(json!({"auto_tool_response":true})).unwrap());
    assert!(tx.try_send(Action::UpdateSession(Box::new(bad))).is_err());
    assert_eq!(tx.projected_session(), original);
    let mut replacement = SessionProperties::pcm();
    replacement.instructions = Some("new instructions".into());
    replacement.model = Some("synthetic/replacement-model".into());
    tx.send(Action::UpdateSession(Box::new(replacement.clone())))
        .await
        .unwrap();
    assert_eq!(tx.projected_session(), replacement);
    loop {
        if matches!(next(&mut session).await.event, Event::SessionUpdated { .. }) {
            break;
        }
    }
    session.shutdown().await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn finite_command_admission_and_audio_size_bounds_are_local() {
    let (mut config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        closed(&mut ws).await;
    })
    .await;
    config.limits.queue_capacity = 2;
    let mut session = ready(config).await;
    let tx = session.sender();
    assert_eq!(
        tx.try_send(Action::AppendAudio(vec![0; 192_002])),
        Err(RealtimeError::TooLarge)
    );
    tx.try_send(Action::AppendAudio(vec![1, 2])).unwrap();
    tx.try_send(Action::AppendAudio(vec![3, 4])).unwrap();
    assert_eq!(
        tx.try_send(Action::AppendAudio(vec![5, 6])),
        Err(RealtimeError::Backpressure)
    );
    session.shutdown().await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn graceful_finish_flushes_tail_but_close_discards_it() {
    for finish in [false, true] {
        let (config, server) = peer(move |mut ws| async move {
            handshake(&mut ws).await;
            if finish {
                let append = read(&mut ws).await;
                assert_eq!(append["type"], "input_audio_buffer.append");
                assert_eq!(
                    STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(),
                    [7, 8, 9, 10]
                );
            }
            closed(&mut ws).await;
        })
        .await;
        let mut session = ready(config).await;
        session
            .sender()
            .send(Action::AppendAudio(vec![7, 8, 9, 10]))
            .await
            .unwrap();
        if finish {
            session.sender().finish().await.unwrap();
            while let Some(event) = bounded(session.next_event()).await {
                event.unwrap();
            }
        } else {
            session.shutdown().await.unwrap();
        }
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn repeated_shutdown_joins_once_and_retained_sender_stays_closed() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        closed(&mut ws).await;
    })
    .await;
    let mut session = ready(config).await;
    let retained = session.sender();
    bounded(session.shutdown()).await.unwrap();
    bounded(session.shutdown()).await.unwrap();
    bounded(server).await.unwrap();
    assert_eq!(
        retained.send(Action::ClearAudio).await,
        Err(RealtimeError::Closed)
    );
}

#[tokio::test]
async fn unknown_events_are_ignored_and_completed_response_cannot_resurrect_audio() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        wire(
            &mut ws,
            json!({"type":"future.feature","private_payload":"not-retained"}),
        )
        .await;
        response(&mut ws, "r", false).await;
        wire(&mut ws, audio_delta("r", "a", &[1, 2])).await;
        response(&mut ws, "r", true).await;
        response(&mut ws, "r", false).await;
        wire(&mut ws, audio_delta("r", "a", &[3, 4])).await;
        response(&mut ws, "r", true).await;
        peer_close(&mut ws).await;
    })
    .await;
    let mut session = ready(config).await;
    session
        .sender()
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    let mut mapper = RealtimeFrameMapper::default();
    let mut frames = vec![];
    while let Some(event) = bounded(session.next_event()).await {
        let event = event.unwrap();
        assert!(!format!("{event:?}").contains("not-retained"));
        frames.extend(mapper.frames(&event).unwrap());
    }
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
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsAudio(a) => Some(a.audio.as_slice()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect();
    assert_eq!(audio, [1, 2]);
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn invalid_tool_declarations_and_completion_identity_fail_closed() {
    for attack in 0..5 {
        let (mut config,server)=peer(move |mut ws|async move {
            handshake(&mut ws).await;read(&mut ws).await;response(&mut ws,"r",false).await;
            if attack!=0 {wire(&mut ws,json!({"type":"conversation.item.added","item":{"type":"function_call","id":"i","call_id":"c","name":if attack==1 {"unoffered"}else{"echo"},"arguments":""}})).await;}
            if attack!=1 {
                wire(&mut ws,json!({"type":"response.function_call_arguments.done","call_id":"c","item_id":if attack==2 {"wrong-item"}else{"i"},"name":if attack==3 {"wrong-name"}else{"echo"},"arguments":if attack==4 {"not-json"}else{"{}"}})).await;
            }
            closed(&mut ws).await;
        }).await;
        config.session.tools = Some(vec![FunctionTool::new(
            "echo",
            "synthetic",
            json!({"type":"object"}),
        )
        .into()]);
        let mut session = ready(config).await;
        session
            .sender()
            .send(Action::CreateResponse(Default::default()))
            .await
            .unwrap();
        let mut error = None;
        while let Some(event) = bounded(session.next_event()).await {
            match event {
                Ok(event) => assert!(!matches!(event.event, Event::FunctionArgumentsDone { .. })),
                Err(value) => error = Some(value),
            }
        }
        assert!(matches!(error, Some(RealtimeError::Protocol(_))));
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn redirect_is_not_followed_and_private_body_is_not_disclosed() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = InworldConfig::new("synthetic");
    config.endpoint = format!("ws://{}/session", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut stream, _) = bounded(listener.accept()).await.unwrap();
        let mut request = vec![0; 4096];
        let _ = bounded(stream.read(&mut request)).await.unwrap();
        bounded(stream.write_all(b"HTTP/1.1 302 Found\r\nLocation: wss://example.com/private-redirect\r\nContent-Length: 12\r\n\r\nprivate-body")).await.unwrap();
        assert!(timeout(Duration::from_millis(80), listener.accept())
            .await
            .is_err());
    });
    let error = match InworldClient::new(config).unwrap().connect().await {
        Ok(_) => panic!("redirect unexpectedly connected"),
        Err(error) => error,
    };
    assert_eq!(error, RealtimeError::Http(302));
    assert!(!format!("{error:?}").contains("private"));
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn split_input_samples_are_buffered_but_odd_commit_tail_is_rejected() {
    let (mut config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        let clear = read(&mut ws).await;
        assert_eq!(clear["type"], "input_audio_buffer.clear");
        let append = read(&mut ws).await;
        assert_eq!(
            STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(),
            [7, 8]
        );
        assert_eq!(read(&mut ws).await["type"], "input_audio_buffer.commit");
        closed(&mut ws).await;
    })
    .await;
    config.session = SessionProperties::manual();
    let mut session = ready(config).await;
    let tx = session.sender();
    tx.send(Action::AppendAudio(vec![1, 2, 3])).await.unwrap();
    assert_eq!(
        tx.send(Action::CommitAudio).await,
        Err(RealtimeError::Invalid(
            "incomplete PCM sample at input boundary"
        ))
    );
    tx.send(Action::ClearAudio).await.unwrap();
    tx.send(Action::AppendAudio(vec![7])).await.unwrap();
    tx.send(Action::AppendAudio(vec![8])).await.unwrap();
    tx.send(Action::CommitAudio).await.unwrap();
    session.shutdown().await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn one_owed_inference_survives_active_response_but_interruption_retires_it() {
    for interrupted in [false, true] {
        let (release, wait) = tokio::sync::oneshot::channel();
        let (config, server) = peer(move |mut ws| async move {
            handshake(&mut ws).await;
            read(&mut ws).await;
            response(&mut ws, "first", false).await;
            assert_eq!(read(&mut ws).await["type"], "conversation.item.create");
            wait.await.unwrap();
            quiet(&mut ws).await;
            response(&mut ws, "first", true).await;
            if !interrupted {
                assert_eq!(read(&mut ws).await["type"], "response.create");
                response(&mut ws, "second", false).await;
                response(&mut ws, "second", true).await;
            }
            quiet(&mut ws).await;
            peer_close(&mut ws).await;
        })
        .await;
        let mut session = ready(config).await;
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
                id: "next-material".into(),
                role: pipecat_inworld_realtime::Role::User,
                content: vec![ItemContent::InputText {
                    text: "next question".into(),
                }],
            },
            previous_item_id: None,
        })
        .await
        .unwrap();
        for _ in 0..2 {
            tx.send(Action::RequestInference(Default::default()))
                .await
                .unwrap();
        }
        if interrupted {
            tx.interrupt();
        }
        release.send(()).unwrap();
        while let Some(event) = bounded(session.next_event()).await {
            event.unwrap();
        }
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn failed_and_incomplete_responses_are_terminal_not_successful_completions() {
    for status in ["failed", "incomplete"] {
        let (config,server)=peer(move |mut ws|async move {
            handshake(&mut ws).await;read(&mut ws).await;response(&mut ws,"failed",false).await;
            wire(&mut ws,json!({"type":"response.done","response":{"id":"failed","status":status,"output":[],"status_details":{"error":{"code":"synthetic-failure","message":"must-not-leak-private-failure"}}}})).await;
            closed(&mut ws).await;
        }).await;
        let mut session = ready(config).await;
        session
            .sender()
            .send(Action::CreateResponse(Default::default()))
            .await
            .unwrap();
        let mut error = None;
        while let Some(event) = bounded(session.next_event()).await {
            match event {
                Ok(event) => assert!(!matches!(event.event, Event::ResponseDone { .. })),
                Err(e) => error = Some(e),
            }
        }
        assert!(matches!(error, Some(RealtimeError::Protocol(_))));
        assert!(!format!("{error:?}").contains("must-not-leak"));
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn graceful_finish_waits_for_outstanding_tool_after_response_done() {
    let (waiting, wait) = tokio::sync::oneshot::channel();
    let (mut config, server) = peer(move |mut ws| async move {
        handshake(&mut ws).await;
        read(&mut ws).await;
        response(&mut ws, "r", false).await;
        call(&mut ws, "finishing-call", "echo", "{}", true).await;
        response(&mut ws, "r", true).await;
        quiet(&mut ws).await;
        waiting.send(()).unwrap();
        let output = read(&mut ws).await;
        assert_eq!(output["item"]["call_id"], "finishing-call");
        assert_eq!(output["item"]["output"], "final-before-close");
        closed(&mut ws).await;
    })
    .await;
    config.session.tools = Some(vec![FunctionTool::new(
        "echo",
        "synthetic",
        json!({"type":"object"}),
    )
    .into()]);
    let mut session = ready(config).await;
    let tx = session.sender();
    tx.send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    admitted_call(&mut session).await;
    loop {
        if matches!(next(&mut session).await.event, Event::ResponseDone { .. }) {
            break;
        }
    }
    tx.finish().await.unwrap();
    bounded(wait).await.unwrap();
    tx.send(Action::ToolResult {
        call_id: "finishing-call".into(),
        output: json!("final-before-close"),
        is_final: true,
    })
    .await
    .unwrap();
    while let Some(event) = bounded(session.next_event()).await {
        event.unwrap();
    }
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn finish_waits_for_runtime_settings_ack_and_missing_ack_times_out() {
    for acknowledge in [false, true] {
        let (finishing, wait) = tokio::sync::oneshot::channel();
        let (mut config, server) = peer(move |mut ws| async move {
            handshake(&mut ws).await;
            let update = read(&mut ws).await;
            assert_eq!(update["type"], "session.update");
            wait.await.unwrap();
            quiet(&mut ws).await;
            if acknowledge {
                wire(
                    &mut ws,
                    json!({"type":"session.updated","session":update["session"]}),
                )
                .await;
            }
            closed(&mut ws).await;
        })
        .await;
        config.limits.operation_timeout = Duration::from_millis(200);
        let mut session = ready(config).await;
        let tx = session.sender();
        let mut replacement = tx.projected_session();
        replacement.instructions = Some("runtime replacement".into());
        tx.send(Action::UpdateSession(Box::new(replacement)))
            .await
            .unwrap();
        tx.finish().await.unwrap();
        finishing.send(()).unwrap();
        let mut error = None;
        let mut acknowledged = false;
        while let Some(event) = bounded(session.next_event()).await {
            match event {
                Ok(event) => acknowledged |= matches!(event.event, Event::SessionUpdated { .. }),
                Err(value) => error = Some(value),
            }
        }
        if acknowledge {
            assert!(acknowledged);
            assert!(error.is_none(), "{error:?}");
        } else {
            assert_eq!(error, Some(RealtimeError::Timeout("session update")));
        }
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn finish_rejects_new_public_work_while_existing_response_drains() {
    let (checked, wait) = tokio::sync::oneshot::channel();
    let (config, server) = peer(move |mut ws| async move {
        handshake(&mut ws).await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "draining", false).await;
        wait.await.unwrap();
        quiet(&mut ws).await;
        response(&mut ws, "draining", true).await;
        closed(&mut ws).await;
    })
    .await;
    let mut session = ready(config).await;
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
    tx.finish().await.unwrap();
    for action in [
        Action::CreateResponse(Default::default()),
        Action::RequestInference(Default::default()),
        Action::AppendAudio(vec![1, 2]),
        Action::UpdateSession(Box::new(tx.projected_session())),
        Action::CreateItem {
            item: ConversationItem::Message {
                id: "after-finish".into(),
                role: pipecat_inworld_realtime::Role::User,
                content: vec![ItemContent::InputText {
                    text: "must-not-run".into(),
                }],
            },
            previous_item_id: None,
        },
    ] {
        assert!(
            tx.send(action).await.is_err(),
            "new work must not be admitted after Finish"
        );
    }
    checked.send(()).unwrap();
    while let Some(event) = bounded(session.next_event()).await {
        event.unwrap();
    }
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn settled_tool_argument_echo_is_inert_but_new_call_item_reuse_is_fatal() {
    let (mut config,server)=peer(|mut ws|async move {
        handshake(&mut ws).await;read(&mut ws).await;response(&mut ws,"first",false).await;call(&mut ws,"once","echo","{}",true).await;
        let output=read(&mut ws).await;assert_eq!(output["item"]["call_id"],"once");
        wire(&mut ws,json!({"type":"response.function_call_arguments.done","call_id":"once","arguments":"{}"})).await;
        quiet(&mut ws).await;response(&mut ws,"first",true).await;response(&mut ws,"new",false).await;
        wire(&mut ws,json!({"type":"conversation.item.added","item":{"type":"function_call","id":"new-item-reused-call","call_id":"once","name":"echo","arguments":""}})).await;
        closed(&mut ws).await;
    }).await;
    config.session.tools = Some(vec![FunctionTool::new(
        "echo",
        "synthetic",
        json!({"type":"object"}),
    )
    .into()]);
    let mut session = ready(config).await;
    let tx = session.sender();
    tx.send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    admitted_call(&mut session).await;
    tx.send(Action::ToolResult {
        call_id: "once".into(),
        output: Value::Null,
        is_final: true,
    })
    .await
    .unwrap();
    let mut error = None;
    while let Some(event) = bounded(session.next_event()).await {
        match event {
            Ok(event) => assert!(
                !matches!(event.event, Event::FunctionArgumentsDone { .. }),
                "late completion must not execute again"
            ),
            Err(value) => error = Some(value),
        }
    }
    assert!(matches!(error, Some(RealtimeError::Protocol(_))));
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn clean_peer_close_flushes_automatic_reply_and_preserves_successful_cleanup() {
    let (config, server) = peer(|mut ws| async move {
        handshake(&mut ws).await;
        // Explicit caller synchronization prevents readiness from racing a peer
        // that closes before the application can observe the session.
        assert_eq!(read(&mut ws).await["type"], "input_audio_buffer.clear");
        peer_close(&mut ws).await;
    })
    .await;
    let mut session = ready(config).await;
    let retained = session.sender();
    retained.send(Action::ClearAudio).await.unwrap();
    while let Some(event) = bounded(session.next_event()).await {
        event.expect("clean peer close must not become Transport");
    }
    assert_eq!(session.cleanup_error(), None);
    bounded(session.shutdown()).await.unwrap();
    bounded(session.shutdown()).await.unwrap();
    assert!(!retained.is_ready());
    assert_eq!(
        retained.send(Action::ClearAudio).await,
        Err(RealtimeError::Closed)
    );
    bounded(server).await.unwrap();
}
