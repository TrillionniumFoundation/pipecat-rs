// Tungstenite fixes the handshake callback error to its full HTTP response type.
#![allow(clippy::result_large_err)]
use futures_util::{SinkExt, StreamExt};
use pipecat_assemblyai::*;
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};
type Peer = WebSocketStream<TcpStream>;
async fn listener() -> (TcpListener, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/v3/ws", l.local_addr().unwrap());
    (l, url)
}
fn config(url: String) -> AssemblyAiConfig {
    let mut c = AssemblyAiConfig::new("explicit-test-key");
    c.endpoint = url;
    c.limits.response_timeout = Duration::from_millis(700);
    c.limits.close_timeout = Duration::from_millis(100);
    c.limits.write_timeout = Duration::from_millis(500);
    c
}
async fn accept(listener: &TcpListener) -> Peer {
    tokio_tungstenite::accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap()
}
async fn message(peer: &mut Peer) -> Message {
    tokio::time::timeout(Duration::from_secs(2), peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn send(peer: &mut Peer, value: Value) {
    peer.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
fn turn(order: u64, text: &str, end: bool, formatted: bool) -> Value {
    json!({"type":"Turn", "turn_order":order, "turn_is_formatted":formatted, "end_of_turn":end, "transcript":text, "end_of_turn_confidence":0.9, "words":[{"start":0,"end":40,"text":text,"confidence":0.8,"word_is_final":end}]})
}
fn termination() -> Value {
    json!({"type":"Termination", "audio_duration_seconds":1.0,"session_duration_seconds":2.0})
}
async fn collect(session: &mut AssemblyAiSession) -> Result<Vec<AssemblyAiEvent>, StreamingError> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut out = vec![];
        while let Some(event) = session.next().await {
            out.push(event?);
        }
        Ok(out)
    })
    .await
    .unwrap()
}
fn object(value: Value) -> serde_json::Map<String, Value> {
    value.as_object().unwrap().clone()
}

#[tokio::test]
async fn default_handshake_exact_headers_and_query_audio_chunking_and_force_endpoint() {
    let (l, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut peer = accept_hdr_async(
            l.accept().await.unwrap().0,
            |request: &Request, response: Response| {
                assert_eq!(request.uri().path(), "/v3/ws");
                assert_eq!(request.headers()["authorization"], "explicit-test-key");
                assert_eq!(
                    request.headers()["user-agent"],
                    "AssemblyAI/1.0 (integration=Pipecat/0.1.0)"
                );
                let q: BTreeMap<_, _> =
                    url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
                        .into_owned()
                        .collect();
                assert_eq!(
                    q,
                    BTreeMap::from([
                        ("sample_rate".into(), "16000".into()),
                        ("encoding".into(), "pcm_s16le".into()),
                        ("speech_model".into(), "universal-3-6-pro".into()),
                        ("formatted_finals".into(), "true".into()),
                        ("format_turns".into(), "true".into()),
                        ("continuous_partials".into(), "true".into()),
                        ("min_turn_silence".into(), "100".into()),
                        ("max_turn_silence".into(), "100".into())
                    ])
                );
                assert!(!request.uri().to_string().contains("explicit-test-key"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        assert_eq!(
            message(&mut peer).await.into_data().as_ref(),
            vec![7u8; 1600]
        );
        assert_eq!(
            serde_json::from_str::<Value>(message(&mut peer).await.to_text().unwrap()).unwrap(),
            json!({"type":"ForceEndpoint"})
        );
        send(&mut peer, turn(0, "recognised", true, true)).await;
        assert_eq!(message(&mut peer).await.into_data().as_ref(), vec![7u8; 4]);
        assert_eq!(
            message(&mut peer).await.to_text().unwrap(),
            "{\"type\":\"Terminate\"}"
        );
        send(&mut peer, termination()).await;
    });
    let mut session = AssemblyAiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender.send_audio(vec![7; 1604]).await.unwrap();
    sender.finalize().await.unwrap();
    let event = session.next().await.unwrap().unwrap();
    assert!(matches!(
        event,
        AssemblyAiEvent::Transcript(Transcript {
            finalized: true,
            is_final: true,
            ..
        })
    ));
    sender.finish().await.unwrap();
    let events = collect(&mut session).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, AssemblyAiEvent::Terminated { .. }))
            .count(),
        1
    );
    assert_eq!(
        sender.send_audio(vec![0; 2]).await,
        Err(StreamingError::Closed)
    );
    server.await.unwrap();
}
#[tokio::test]
async fn u3_variants_send_features_and_unicode_context_tail() {
    for model in [
        "u3-rt-pro",
        "u3-rt-pro-beta-1",
        "universal-3-5-pro",
        "universal-3-6-pro-preview",
    ] {
        let (l, url) = listener().await;
        let model = model.to_owned();
        let expected_model = model.clone();
        let server = tokio::spawn(async move {
            let mut p = accept_hdr_async(
                l.accept().await.unwrap().0,
                move |r: &Request, s: Response| {
                    let q: BTreeMap<_, _> =
                        url::form_urlencoded::parse(r.uri().query().unwrap().as_bytes())
                            .into_owned()
                            .collect();
                    assert_eq!(q["speech_model"], expected_model);
                    assert_eq!(q["agent_context"], "界".repeat(1500));
                    assert_eq!(q["interruption_delay"], "1000");
                    assert_eq!(q["previous_context_n_turns"], "100");
                    assert_eq!(q["voice_focus"], "near-field");
                    assert_eq!(q["voice_focus_threshold"], "1.0");
                    assert_eq!(q["mode"], "max_accuracy");
                    assert_eq!(
                        serde_json::from_str::<Value>(&q["language_codes"]).unwrap(),
                        json!(["es", "en"])
                    );
                    assert_eq!(q["prompt"], "meeting");
                    assert_eq!(
                        serde_json::from_str::<Value>(&q["keyterms_prompt"]).unwrap(),
                        json!(["Pipecat"])
                    );
                    assert!(!q.contains_key("min_turn_silence"));
                    Ok(s)
                },
            )
            .await
            .unwrap();
            send(&mut p, termination()).await;
        });
        let mut c = config(url);
        c.vad_force_turn_endpoint = false;
        c.settings.model = model;
        c.settings.agent_context = Some(format!("prefix{}", "界".repeat(1501)));
        c.settings.interruption_delay = Some(1000);
        c.settings.previous_context_n_turns = Some(100);
        c.settings.voice_focus = Some("near-field".into());
        c.settings.voice_focus_threshold = Some(1.0);
        c.settings.mode = Some("max_accuracy".into());
        c.settings.language_codes = Some(vec!["es-MX".into(), "en-US".into(), "es".into()]);
        c.settings.prompt = Some("meeting".into());
        c.settings.keyterms_prompt = Some(vec!["Pipecat".into()]);
        collect(&mut AssemblyAiClient::new(c).unwrap().connect().await.unwrap())
            .await
            .unwrap();
        server.await.unwrap();
    }
}
#[tokio::test]
async fn legacy_model_omits_u3_fields_and_uses_actual_source_threshold() {
    let (l, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut p = accept_hdr_async(l.accept().await.unwrap().0, |r: &Request, s: Response| {
            let q: BTreeMap<_, _> =
                url::form_urlencoded::parse(r.uri().query().unwrap().as_bytes())
                    .into_owned()
                    .collect();
            for key in [
                "agent_context",
                "language_codes",
                "interruption_delay",
                "continuous_partials",
                "voice_focus",
                "voice_focus_threshold",
                "previous_context_n_turns",
                "mode",
            ] {
                assert!(!q.contains_key(key));
            }
            assert_eq!(q["min_turn_silence"], "160");
            assert_eq!(q["end_of_turn_confidence_threshold"], "1.0");
            assert_eq!(q["language_code"], "es");
            Ok(s)
        })
        .await
        .unwrap();
        send(&mut p, termination()).await;
    });
    let mut c = config(url);
    c.settings.model = "universal-streaming-multilingual".into();
    c.settings.agent_context = Some("secret context".into());
    c.settings.language_codes = Some(vec!["es".into()]);
    c.settings.interruption_delay = Some(0);
    c.settings.voice_focus = Some("far-field".into());
    c.settings.voice_focus_threshold = Some(0.2);
    c.settings.previous_context_n_turns = Some(0);
    c.settings.mode = Some("balanced".into());
    c.settings.language_code = Some("es".into());
    collect(&mut AssemblyAiClient::new(c).unwrap().connect().await.unwrap())
        .await
        .unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn hot_updates_clear_languages_drop_over_limit_and_reconnect_only_changes() {
    let (l, url) = listener().await;
    let (ready_tx, ready_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        assert_eq!(
            serde_json::from_str::<Value>(message(&mut p).await.to_text().unwrap()).unwrap(),
            json!({"type":"UpdateConfiguration","agent_context":"界".repeat(1500)})
        );
        assert_eq!(
            serde_json::from_str::<Value>(message(&mut p).await.to_text().unwrap()).unwrap(),
            json!({"type":"UpdateConfiguration","language_codes":["en","es"]})
        );
        assert_eq!(
            serde_json::from_str::<Value>(message(&mut p).await.to_text().unwrap()).unwrap(),
            json!({"type":"UpdateConfiguration","language_codes":[]})
        );
        assert_eq!(
            message(&mut p).await.to_text().unwrap(),
            "{\"type\":\"Terminate\"}"
        );
        send(&mut p, termination()).await;
        let mut p = accept_hdr_async(l.accept().await.unwrap().0, |r: &Request, s: Response| {
            let q: BTreeMap<_, _> =
                url::form_urlencoded::parse(r.uri().query().unwrap().as_bytes())
                    .into_owned()
                    .collect();
            assert_eq!(q["min_turn_silence"], "250");
            assert_eq!(q["max_turn_silence"], "250");
            assert_eq!(q["agent_context"], "new seed");
            assert_eq!(q["language_codes"], "[]");
            Ok(s)
        })
        .await
        .unwrap();
        ready_tx.send(()).unwrap();
        assert_eq!(
            message(&mut p).await.to_text().unwrap(),
            "{\"type\":\"Terminate\"}"
        );
        send(&mut p, termination()).await;
    });
    let mut session = AssemblyAiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    let result = sender
        .update_settings(object(
            json!({"agent_context":"界".repeat(1600),"language_codes":["en-US","es-MX","en"]}),
        ))
        .await
        .unwrap();
    assert!(!result.reconnected);
    assert!(
        !sender
            .update_settings(object(json!({"language_codes":[]})))
            .await
            .unwrap()
            .reconnected
    );
    let codes = json!(["en", "es", "fr", "de", "it", "pt", "no", "fi", "da", "sv", "ja"]);
    let result = sender
        .update_settings(object(json!({"language_codes":codes})))
        .await
        .unwrap();
    assert!(result.dropped_language_codes);
    assert!(!result.reconnected);
    assert_eq!(sender.settings().unwrap().language_codes, Some(vec![]));
    let result = sender
        .update_settings(object(
            json!({"language_codes":codes,"min_turn_silence":250,"agent_context":"new seed"}),
        ))
        .await
        .unwrap();
    assert!(result.reconnected && result.dropped_language_codes);
    ready_rx.await.unwrap();
    sender.finish().await.unwrap();
    let events = collect(&mut session).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, AssemblyAiEvent::Terminated { .. }))
            .count(),
        1
    );
    server.await.unwrap();
}
#[tokio::test]
async fn invalid_update_keeps_settings_and_does_not_disconnect() {
    let (l, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        assert_eq!(
            message(&mut p).await.to_text().unwrap(),
            "{\"type\":\"Terminate\"}"
        );
        send(&mut p, termination()).await;
    });
    let mut s = AssemblyAiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let tx = s.sender();
    let before = tx.settings().unwrap();
    for value in [
        json!({"api_key":"injected"}),
        json!({"interruption_delay":1001}),
        json!({"voice_focus":"other"}),
        json!({"language_codes":[7]}),
        json!({"model":null}),
    ] {
        assert!(tx.update_settings(object(value)).await.is_err());
        assert_eq!(tx.settings().unwrap(), before);
    }
    tx.finish().await.unwrap();
    collect(&mut s).await.unwrap();
    server.await.unwrap();
}
#[tokio::test]
async fn provider_turns_format_gate_diarization_language_duplicate_and_late_contract() {
    let (l, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        send(
            &mut p,
            json!({"type":"Begin","id":"session","expires_at":99}),
        )
        .await;
        send(&mut p, json!({"type":"SpeechStarted","timestamp":1})).await;
        send(&mut p, json!({"type":"SpeechStarted","timestamp":2})).await;
        send(&mut p, turn(2, "raw", true, false)).await;
        let mut final_turn = turn(2, "Hola", true, true);
        final_turn["language_code"] = json!("ES");
        final_turn["language_confidence"] = json!(0.9);
        final_turn["speaker_label"] = json!("A");
        send(&mut p, final_turn.clone()).await;
        send(&mut p, final_turn).await;
        send(&mut p, turn(1, "late old", true, true)).await;
        send(&mut p, json!({"type":"SpeechStarted","timestamp":3})).await;
        send(&mut p, turn(3, "", true, true)).await;
        send(&mut p, termination()).await;
        let _ = p
            .send(Message::Text(
                turn(4, "late terminal", true, true).to_string().into(),
            ))
            .await;
    });
    let mut c = config(url);
    c.vad_force_turn_endpoint = false;
    c.speaker_format = Some("{speaker}: {text}".into());
    let events = collect(&mut AssemblyAiClient::new(c).unwrap().connect().await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, AssemblyAiEvent::ProposedUserStartedSpeaking))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, AssemblyAiEvent::ProposedUserStoppedSpeaking))
            .count(),
        2
    );
    let ts: Vec<_> = events
        .iter()
        .filter_map(|e| {
            if let AssemblyAiEvent::Transcript(t) = e {
                Some(t)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(ts.len(), 2);
    assert!(!ts[0].is_final);
    assert_eq!(ts[1].text, "A: Hola");
    assert!(ts[1].finalized);
    assert_eq!(ts[1].language, "es");
    assert_eq!(ts[1].speaker.as_deref(), Some("A"));
    let final_index = events
        .iter()
        .position(|e| matches!(e,AssemblyAiEvent::Transcript(t) if t.is_final))
        .unwrap();
    assert!(matches!(
        events[final_index + 1],
        AssemblyAiEvent::ProposedUserStoppedSpeaking
    ));
    server.await.unwrap();
}
#[tokio::test]
async fn unformatted_finals_no_force_ack_and_empty_results_never_invent_text() {
    let (l, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        send(&mut p, json!({"type":"SpeechStarted","timestamp":0})).await;
        send(&mut p, turn(0, "", true, false)).await;
        let mut t = turn(1, "real", true, false);
        t["language_code"] = json!("es");
        t["language_confidence"] = json!(0.69);
        send(&mut p, t).await;
        send(&mut p, termination()).await;
    });
    let mut c = config(url);
    c.settings.format_turns = false;
    let events = collect(&mut AssemblyAiClient::new(c).unwrap().connect().await.unwrap())
        .await
        .unwrap();
    assert!(!events.iter().any(|e| matches!(
        e,
        AssemblyAiEvent::ProposedUserStartedSpeaking | AssemblyAiEvent::ProposedUserStoppedSpeaking
    )));
    let ts: Vec<_> = events
        .iter()
        .filter_map(|e| {
            if let AssemblyAiEvent::Transcript(t) = e {
                Some(t)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(ts.len(), 1);
    assert_eq!(ts[0].text, "real");
    assert!(ts[0].is_final);
    assert!(!ts[0].finalized);
    assert_eq!(ts[0].language, "en");
    server.await.unwrap();
}
#[tokio::test]
async fn malformed_and_oversized_responses_fail_without_disclosing_body() {
    for payload in [
        "private body not json".to_owned(),
        json!({"type":"Turn","transcript":"private malformed transcript"}).to_string(),
        json!({"type":"Unknown","api_key":"private"}).to_string(),
        "x".repeat(5000),
    ] {
        let (l, url) = listener().await;
        let server = tokio::spawn(async move {
            let mut p = accept(&l).await;
            let _ = p.send(Message::Text(payload.into())).await;
        });
        let mut c = config(url);
        c.limits.max_message_bytes = 2048;
        let error = collect(&mut AssemblyAiClient::new(c).unwrap().connect().await.unwrap())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            StreamingError::Protocol(_) | StreamingError::TooLarge
        ));
        assert!(!error.to_string().contains("private"));
        server.await.unwrap();
    }
}
#[tokio::test]
async fn cancellation_discards_queued_outputs_and_never_flushes_buffered_audio() {
    let (l, url) = listener().await;
    let (ready_tx, ready_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        for n in 0..8 {
            send(&mut p, turn(n, "queued private", false, false)).await;
        }
        ready_tx.send(()).unwrap();
        while let Some(result) = p.next().await {
            match result {
                Ok(Message::Close(_)) | Err(_) => break,
                Ok(Message::Binary(_) | Message::Text(_)) => {
                    panic!("cancel must not flush audio or send Terminate")
                }
                _ => (),
            }
        }
    });
    let mut c = config(url);
    c.limits.queue_capacity = 1;
    let mut s = AssemblyAiClient::new(c).unwrap().connect().await.unwrap();
    s.sender().send_audio(vec![0; 20]).await.unwrap();
    ready_rx.await.unwrap();
    s.cancel();
    assert_eq!(s.next().await.unwrap(), Err(StreamingError::Cancelled));
    assert!(s.next().await.is_none());
    server.await.unwrap();
}
#[tokio::test]
async fn termination_timeout_is_bounded_and_cancel_during_connect_is_immediate() {
    let (l, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        assert_eq!(
            message(&mut p).await.to_text().unwrap(),
            "{\"type\":\"Terminate\"}"
        );
        while p.next().await.is_some() {}
    });
    let mut c = config(url);
    c.limits.response_timeout = Duration::from_millis(30);
    let mut s = AssemblyAiClient::new(c).unwrap().connect().await.unwrap();
    s.sender().finish().await.unwrap();
    assert_eq!(
        collect(&mut s).await.unwrap_err(),
        StreamingError::Timeout("termination")
    );
    server.await.unwrap();
    let token = CancellationToken::new();
    token.cancel();
    let client = AssemblyAiClient::new(AssemblyAiConfig::new("key")).unwrap();
    assert!(matches!(
        client.connect_with_cancel(token).await,
        Err(StreamingError::Cancelled)
    ));
}
#[tokio::test]
async fn mulaw_uses_fifty_millisecond_byte_count() {
    let (l, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        assert_eq!(message(&mut p).await.into_data().len(), 400);
        assert_eq!(message(&mut p).await.into_data().len(), 1);
        assert_eq!(
            message(&mut p).await.to_text().unwrap(),
            "{\"type\":\"Terminate\"}"
        );
        send(&mut p, termination()).await;
    });
    let mut c = config(url);
    c.encoding = Encoding::PcmMulaw;
    c.sample_rate = Some(8000);
    let mut s = AssemblyAiClient::new(c).unwrap().connect().await.unwrap();
    s.sender().send_audio(vec![0; 401]).await.unwrap();
    s.sender().finish().await.unwrap();
    collect(&mut s).await.unwrap();
    server.await.unwrap();
}
#[test]
fn construction_validation_and_debug_privacy() {
    assert!(is_u3_pro_model("universal-3-6-pro-beta"));
    assert!(!is_u3_pro_model("universal-streaming-english"));
    assert_eq!(
        prepare_language_codes(&["es-MX".into(), "es-ES".into(), "en-US".into()], Some(10))
            .unwrap(),
        vec!["es", "en"]
    );
    for bad in [
        "ws://example.com/v3/ws",
        "wss://user:secret@host/v3/ws",
        "wss://host/v3/ws?token=secret",
        "wss://host/v3/ws#fragment",
    ] {
        let mut c = AssemblyAiConfig::new("secret");
        c.endpoint = bad.into();
        assert!(AssemblyAiClient::new(c).is_err());
    }
    for value in [1001, u32::MAX] {
        let mut c = AssemblyAiConfig::new("key");
        c.settings.interruption_delay = Some(value);
        assert!(AssemblyAiClient::new(c).is_err());
    }
    let mut c = AssemblyAiConfig::new("private-key");
    c.settings.prompt = Some("private-prompt".into());
    c.settings.agent_context = Some("private-context".into());
    assert!(!format!("{:?}", AssemblyAiClient::new(c).unwrap()).contains("private"));
    let mut c = AssemblyAiConfig::new("key");
    c.vad_force_turn_endpoint = false;
    c.settings.model = "universal-streaming-english".into();
    assert!(AssemblyAiClient::new(c).is_err());
    let mut c = AssemblyAiConfig::new("key");
    c.settings.model = "universal-streaming-english".into();
    c.settings.prompt = Some("bad".into());
    assert!(AssemblyAiClient::new(c).is_err());
}

#[tokio::test]
async fn automatic_context_disabled_and_non_u3_updates_are_wire_noops() {
    for legacy in [false, true] {
        let (l, url) = listener().await;
        let server = tokio::spawn(async move {
            let mut p = accept(&l).await;
            if !legacy {
                assert_eq!(
                    serde_json::from_str::<Value>(message(&mut p).await.to_text().unwrap())
                        .unwrap(),
                    json!({"type":"UpdateConfiguration","agent_context":"explicit"})
                );
            }
            assert_eq!(
                message(&mut p).await.to_text().unwrap(),
                "{\"type\":\"Terminate\"}"
            );
            send(&mut p, termination()).await;
        });
        let mut c = config(url);
        c.settings.previous_context_n_turns = Some(0);
        if legacy {
            c.settings.model = "universal-streaming-english".into();
        }
        let mut s = AssemblyAiClient::new(c).unwrap().connect().await.unwrap();
        let tx = s.sender();
        tx.assistant_turn("automatic must not be sent").unwrap();
        tx.update_agent_context("").await.unwrap();
        tx.update_agent_context("explicit").await.unwrap();
        if legacy {
            tx.update_settings(object(
                json!({"language_codes":["es"],"agent_context":"other ignored context"}),
            ))
            .await
            .unwrap();
        }
        tx.finish().await.unwrap();
        collect(&mut s).await.unwrap();
        server.await.unwrap();
    }
}
#[tokio::test]
async fn input_bounds_partial_samples_and_unknown_language_are_explicit() {
    let (l, url) = listener().await;
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        let mut t = turn(0, "real", true, true);
        t["language_code"] = json!("not-a-language");
        t["language_confidence"] = json!(0.99);
        send(&mut p, t).await;
        send(&mut p, termination()).await;
    });
    let c = config(url);
    let cap = c.limits.max_message_bytes;
    let mut s = AssemblyAiClient::new(c).unwrap().connect().await.unwrap();
    assert_eq!(
        s.sender().send_audio(vec![0; 1]).await,
        Err(StreamingError::PartialSample)
    );
    assert_eq!(
        s.sender().send_audio(vec![0; cap + 2]).await,
        Err(StreamingError::TooLarge)
    );
    let events = collect(&mut s).await.unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e,AssemblyAiEvent::Transcript(t) if t.language=="en" && t.text=="real")));
    server.await.unwrap();
}
#[test]
fn settings_boundary_matrix_matches_original_groups() {
    for delay in [0, 1000] {
        let mut c = AssemblyAiConfig::new("key");
        c.settings.interruption_delay = Some(delay);
        assert!(AssemblyAiClient::new(c).is_ok());
    }
    for n in [0, 100] {
        let mut c = AssemblyAiConfig::new("key");
        c.settings.previous_context_n_turns = Some(n);
        assert!(AssemblyAiClient::new(c).is_ok());
    }
    for n in [101, u32::MAX] {
        let mut c = AssemblyAiConfig::new("key");
        c.settings.previous_context_n_turns = Some(n);
        assert!(AssemblyAiClient::new(c).is_err());
    }
    for n in [0.0, 1.0] {
        let mut c = AssemblyAiConfig::new("key");
        c.settings.voice_focus_threshold = Some(n);
        assert!(AssemblyAiClient::new(c).is_ok());
    }
    for n in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
        let mut c = AssemblyAiConfig::new("key");
        c.settings.voice_focus_threshold = Some(n);
        assert!(AssemblyAiClient::new(c).is_err());
    }
    for mode in ["min_latency", "balanced", "max_accuracy"] {
        let mut c = AssemblyAiConfig::new("key");
        c.settings.mode = Some(mode.into());
        assert!(AssemblyAiClient::new(c).is_ok());
    }
    let codes = vec![
        "en", "es", "fr", "de", "it", "pt", "no", "fi", "da", "sv", "ja",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect::<Vec<_>>();
    let mut c = AssemblyAiConfig::new("key");
    c.settings.language_codes = Some(codes.clone());
    assert!(AssemblyAiClient::new(c).is_err());
    assert_eq!(prepare_language_codes(&codes, None).unwrap().len(), 11);
    c = AssemblyAiConfig::new("key");
    c.settings.language_codes = Some(codes[..10].to_vec());
    assert!(AssemblyAiClient::new(c).is_ok());
    assert_eq!(
        prepare_language_codes(&["pl-PL".into()], Some(10)).unwrap(),
        vec!["pl"]
    );
}

#[tokio::test]
async fn speaker_substitutions_do_not_reparse_provider_text_and_expansion_is_bounded() {
    for oversized in [false, true] {
        let (l, url) = listener().await;
        let server = tokio::spawn(async move {
            let mut p = accept(&l).await;
            let mut t = turn(0, "hello", true, true);
            t["speaker_label"] = json!(if oversized {
                "x".repeat(1000)
            } else {
                "{text}".into()
            });
            send(&mut p, t).await;
            let _ = p
                .send(Message::Text(termination().to_string().into()))
                .await;
        });
        let mut c = config(url);
        c.limits.max_message_bytes = 2048;
        c.speaker_format = Some(if oversized {
            "{speaker}".repeat(4)
        } else {
            "{speaker}: {text}".into()
        });
        let result = collect(&mut AssemblyAiClient::new(c).unwrap().connect().await.unwrap()).await;
        if oversized {
            assert_eq!(result.unwrap_err(), StreamingError::TooLarge);
        } else {
            assert!(result
                .unwrap()
                .iter()
                .any(|e| matches!(e,AssemblyAiEvent::Transcript(t) if t.text=="{text}: hello")));
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn next_vad_start_resets_an_old_force_endpoint_ack() {
    let (l, url) = listener().await;
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut p = accept(&l).await;
        assert_eq!(
            message(&mut p).await.to_text().unwrap(),
            "{\"type\":\"ForceEndpoint\"}"
        );
        rx.await.unwrap();
        send(&mut p, turn(1, "next utterance", true, true)).await;
        send(&mut p, termination()).await;
    });
    let mut s = AssemblyAiClient::new(config(url))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = s.sender();
    sender.finalize().await.unwrap();
    sender.start_utterance().unwrap();
    sender.update_settings(object(json!({}))).await.unwrap();
    tx.send(()).unwrap();
    let events = collect(&mut s).await.unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e,AssemblyAiEvent::Transcript(t) if t.is_final&&!t.finalized)));
    server.await.unwrap();
}
