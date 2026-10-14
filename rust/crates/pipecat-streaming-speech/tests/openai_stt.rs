#![cfg(feature = "openai-stt")]
#![allow(clippy::result_large_err)]

use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};
type Ws = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(3);

struct Server(Option<JoinHandle<()>>);
impl Server {
    fn spawn(task: impl Future<Output = ()> + Send + 'static) -> Self {
        Self(Some(tokio::spawn(task)))
    }
    async fn join(mut self) {
        let task = self.0.as_mut().unwrap();
        match tokio::time::timeout(WAIT, &mut *task).await {
            Ok(result) => {
                self.0.take();
                result.unwrap();
            }
            Err(_) => {
                task.abort();
                let _ = task.await;
                self.0.take();
                panic!("synthetic server timed out");
            }
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
}
async fn listen() -> (TcpListener, OpenAiSttConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = OpenAiSttConfig::new("synthetic-test-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    config.ready_timeout = Duration::from_millis(600);
    config.limits.write_timeout = Duration::from_millis(500);
    config.limits.response_timeout = Duration::from_millis(400);
    config.limits.close_timeout = Duration::from_millis(100);
    (listener, config)
}
async fn accept(listener: TcpListener) -> Ws {
    accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap()
}
async fn send(ws: &mut Ws, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn recv(ws: &mut Ws) -> Value {
    match tokio::time::timeout(WAIT, ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        _ => panic!("expected synthetic JSON message"),
    }
}
fn created() -> Value {
    json!({"type":"session.created","session":{"id":"session-test","type":"transcription"}})
}
fn updated(model: &str) -> Value {
    json!({"type":"session.updated","session":{"id":"session-test","type":"transcription","audio":{"input":{"format":{"type":"audio/pcm","rate":24000},"transcription":{"model":model,"language":"en"},"turn_detection":null}}}})
}
async fn ready(ws: &mut Ws) -> Value {
    send(ws, created()).await;
    let update = recv(ws).await;
    let model = update["session"]["audio"]["input"]["transcription"]["model"]
        .as_str()
        .unwrap();
    send(ws, updated(model)).await;
    update
}
async fn close(ws: &mut Ws) {
    assert!(matches!(
        tokio::time::timeout(WAIT, ws.next()).await.unwrap(),
        Some(Ok(Message::Close(_)))
    ));
    // Reading a Close queues tungstenite's reply; flush it rather than detaching.
    let _ = ws.flush().await;
}
fn ack(id: &str) -> Value {
    json!({"type":"input_audio_buffer.committed","item_id":id,"previous_item_id":null})
}
fn event(kind: &str, id: &str, text: &str) -> Value {
    let mut value = json!({"type":format!("conversation.item.input_audio_transcription.{kind}"),"item_id":id,"content_index":0,"event_id":"synthetic-event","extra":{"preserved":true}});
    value[if kind == "delta" {
        "delta"
    } else {
        "transcript"
    }] = text.into();
    value
}
fn failed(id: &str) -> Value {
    json!({"type":"conversation.item.input_audio_transcription.failed","item_id":id,"content_index":0,"error":{"code":"synthetic","message":"private provider text"},"extra":7})
}
fn pcm() -> AudioRawFrame {
    AudioRawFrame::new(vec![1, 2, 3, 4], 24000, 1).unwrap()
}
async fn next(session: &mut OpenAiSttSession) -> Option<Result<OpenAiSttEvent, StreamingError>> {
    tokio::time::timeout(WAIT, session.next()).await.unwrap()
}
async fn finish(session: &mut OpenAiSttSession) {
    session.sender().finish().await.unwrap();
    assert!(next(session).await.is_none());
}

#[test]
fn immutable_profile_validation_and_redacted_debug() {
    let config = OpenAiSttConfig::new("private-key");
    assert_eq!(config.model, "gpt-realtime-whisper");
    assert_eq!(config.language.as_deref(), Some("en"));
    assert!(OpenAiSttClient::new(config.clone()).is_ok());
    assert!(!format!("{config:?}").contains("private-key"));
    for endpoint in [
        "ws://api.openai.com/v1/realtime",
        "wss://example.org/v1/realtime",
        "wss://api.openai.com/v1/realtime?model=x",
        "wss://api.openai.com/v1/realtime#x",
        "wss://user@api.openai.com/v1/realtime",
        "wss://api.openai.com/other",
    ] {
        let mut c = config.clone();
        c.endpoint = endpoint.into();
        assert!(OpenAiSttClient::new(c).is_err());
    }
    for (rate, channels) in [(16000, 1), (24000, 2)] {
        let mut c = config.clone();
        c.sample_rate = rate;
        c.channels = channels;
        assert!(OpenAiSttClient::new(c).is_err());
    }
    let mut c = config.clone();
    c.ready_timeout = Duration::MAX;
    assert!(OpenAiSttClient::new(c).is_err());
    let mut c = config;
    c.max_tracked_items = 0;
    assert!(OpenAiSttClient::new(c).is_err());
}

#[tokio::test]
async fn real_handshake_gate_auth_audio_commit_and_lossless_arrival_events() {
    let (listener, config) = listen().await;
    let (release, held) = oneshot::channel();
    let server = Server::spawn(async move {
        let mut ws = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |request: &Request, response: Response| {
                assert_eq!(
                    request.headers()["authorization"],
                    "Bearer synthetic-test-key"
                );
                assert_eq!(
                    request.uri().to_string(),
                    "/v1/realtime?intent=transcription"
                );
                assert!(!request.headers().contains_key("openai-beta"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(25), ws.next())
            .await
            .is_err());
        send(&mut ws, created()).await;
        let update = recv(&mut ws).await;
        assert_eq!(
            update,
            json!({"type":"session.update","session":{"type":"transcription","audio":{"input":{"format":{"type":"audio/pcm","rate":24000},"transcription":{"model":"gpt-realtime-whisper","language":"en"},"turn_detection":null}}}})
        );
        assert!(tokio::time::timeout(Duration::from_millis(25), ws.next())
            .await
            .is_err());
        held.await.unwrap();
        send(&mut ws, updated("gpt-realtime-whisper")).await;
        assert_eq!(
            recv(&mut ws).await,
            json!({"type":"input_audio_buffer.append","audio":STANDARD.encode([1,2,3,4])})
        );
        assert_eq!(
            recv(&mut ws).await,
            json!({"type":"input_audio_buffer.commit"})
        );
        send(
            &mut ws,
            json!({"type":"response.unrelated","private":"ignored"}),
        )
        .await;
        send(&mut ws, ack("a")).await;
        send(&mut ws, event("delta", "a", "hel")).await;
        send(&mut ws, event("delta", "a", "lo")).await;
        send(&mut ws, event("completed", "a", "hello")).await;
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().send_audio(pcm()).await.unwrap();
    session.sender().commit().await.unwrap();
    release.send(()).unwrap();
    for text in ["hel", "lo"] {
        match next(&mut session).await.unwrap().unwrap() {
            OpenAiSttEvent::Delta {
                delta,
                item_id,
                content_index,
                result,
            } => {
                assert_eq!(delta, text);
                assert_eq!(item_id, "a");
                assert_eq!(content_index, 0);
                assert_eq!(result, event("delta", "a", text));
            }
            _ => panic!("expected independent delta"),
        }
    }
    match next(&mut session).await.unwrap().unwrap() {
        OpenAiSttEvent::Completed {
            transcript, result, ..
        } => {
            assert_eq!(transcript, "hello");
            assert_eq!(result, event("completed", "a", "hello"));
        }
        _ => panic!("expected completed"),
    }
    finish(&mut session).await;
    assert_eq!(session.sender().commit().await, Err(StreamingError::Closed));
    assert!(next(&mut session).await.is_none());
    server.join().await;
}

#[tokio::test]
async fn source_settings_normalization_and_whisper_prompt_omission() {
    for model in ["gpt-realtime-whisper", "gpt-transcribe"] {
        let (listener, mut config) = listen().await;
        config.model = model.into();
        config.language = Some("FR-CA".into());
        config.prompt = Some("synthetic hint".into());
        config.noise_reduction = Some(OpenAiSttNoiseReduction::FarField);
        config.delay = Some(OpenAiSttDelay::Low);
        let expected_model = model.to_owned();
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            let update = ready(&mut ws).await;
            let input = &update["session"]["audio"]["input"];
            assert_eq!(input["transcription"]["model"], expected_model);
            assert_eq!(input["transcription"]["language"], "fr");
            assert_eq!(input["transcription"]["delay"], "low");
            assert_eq!(input["noise_reduction"]["type"], "far_field");
            if expected_model == "gpt-realtime-whisper" {
                assert!(input["transcription"].get("prompt").is_none());
            } else {
                assert_eq!(input["transcription"]["prompt"], "synthetic hint");
            }
            close(&mut ws).await;
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        finish(&mut session).await;
        server.join().await;
    }
}

#[tokio::test]
async fn ready_gate_input_backpressure_and_cloned_sender_cancellation() {
    let (listener, mut config) = listen().await;
    config.limits.queue_capacity = 1;
    let (seen, received) = oneshot::channel();
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        seen.send(()).unwrap();
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    received.await.unwrap();
    let clone = session.sender();
    clone.try_send_audio(pcm()).unwrap();
    assert_eq!(clone.try_commit(), Err(StreamingError::Backpressure));
    session.cancel();
    assert_eq!(session.shutdown().await, Err(StreamingError::Cancelled));
    assert_eq!(clone.try_commit(), Err(StreamingError::Cancelled));
    server.join().await;
}

#[tokio::test]
async fn malformed_known_events_and_illegal_session_acknowledgements_fail_closed() {
    let cases = [
        updated("gpt-realtime-whisper"),
        json!({"type":"session.created","session":{"id":"s","type":"realtime"}}),
        json!({"type":"error","error":"wrong"}),
        json!({"type":42}),
    ];
    for value in cases {
        let (listener, config) = listen().await;
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            send(&mut ws, value).await;
            close(&mut ws).await;
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        assert!(matches!(
            next(&mut session).await,
            Some(Err(StreamingError::Protocol(_)))
        ));
        server.join().await;
    }
    for mutation in [0, 1, 2, 3, 4] {
        let (listener, config) = listen().await;
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            send(&mut ws, created()).await;
            recv(&mut ws).await;
            let mut value = updated("gpt-realtime-whisper");
            match mutation {
                0 => {
                    value["session"]["audio"]["input"]["turn_detection"] =
                        json!({"type":"server_vad"})
                }
                1 => value["session"]["audio"]["input"]["format"]["rate"] = 16000.into(),
                2 => value["session"]["audio"]["output"] = json!({}),
                3 => value["session"]["id"] = "other-session".into(),
                _ => {
                    value["session"]["audio"]["input"]["transcription"]["model"] =
                        "other-model".into()
                }
            }
            send(&mut ws, value).await;
            close(&mut ws).await;
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        assert!(matches!(
            next(&mut session).await,
            Some(Err(StreamingError::Protocol(_)))
        ));
        server.join().await;
    }
}

#[tokio::test]
async fn ready_timeout_and_unknown_events_do_not_extend_deadline() {
    let (listener, mut config) = listen().await;
    config.ready_timeout = Duration::from_millis(80);
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        send(&mut ws, json!({"type":"unknown","large":"not retained"})).await;
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert_eq!(
        next(&mut session).await,
        Some(Err(StreamingError::Timeout("OpenAI STT ready")))
    );
    server.join().await;
}

#[tokio::test]
async fn completed_before_ack_and_duplicate_terminals_cannot_release_another_item() {
    let (listener, config) = listen().await;
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        ready(&mut ws).await;
        recv(&mut ws).await;
        recv(&mut ws).await;
        send(&mut ws, event("completed", "a", "first")).await;
        send(&mut ws, event("completed", "a", "duplicate")).await;
        assert!(tokio::time::timeout(Duration::from_millis(20), ws.next())
            .await
            .is_err());
        send(&mut ws, ack("a")).await;
        send(&mut ws, ack("a")).await;
        send(&mut ws, ack("b")).await;
        send(&mut ws, event("completed", "a", "late duplicate")).await;
        assert!(tokio::time::timeout(Duration::from_millis(20), ws.next())
            .await
            .is_err());
        send(&mut ws, event("completed", "b", "second")).await;
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().commit().await.unwrap();
    session.sender().commit().await.unwrap();
    session.sender().finish().await.unwrap();
    for expected in ["first", "duplicate", "late duplicate", "second"] {
        assert!(
            matches!(next(&mut session).await, Some(Ok(OpenAiSttEvent::Completed { transcript, .. })) if transcript == expected)
        );
    }
    assert!(next(&mut session).await.is_none());
    server.join().await;
}

#[tokio::test]
async fn out_of_order_items_and_failed_event_preserve_arrival_order_and_continue() {
    let (listener, config) = listen().await;
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        ready(&mut ws).await;
        recv(&mut ws).await;
        recv(&mut ws).await;
        send(&mut ws, ack("a")).await;
        send(&mut ws, ack("b")).await;
        send(&mut ws, event("completed", "b", "second first")).await;
        send(&mut ws, failed("a")).await;
        assert_eq!(
            recv(&mut ws).await,
            json!({"type":"input_audio_buffer.commit"})
        );
        send(&mut ws, ack("c")).await;
        send(&mut ws, event("completed", "c", "after failure")).await;
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().commit().await.unwrap();
    session.sender().commit().await.unwrap();
    assert!(
        matches!(next(&mut session).await, Some(Ok(OpenAiSttEvent::Completed { item_id, .. })) if item_id == "b")
    );
    let failure = next(&mut session).await.unwrap().unwrap();
    assert!(!format!("{failure:?}").contains("private provider text"));
    assert!(
        matches!(failure, OpenAiSttEvent::Failed { item_id, result, .. } if item_id == "a" && result == failed("a"))
    );
    session.sender().commit().await.unwrap();
    session.sender().finish().await.unwrap();
    assert!(
        matches!(next(&mut session).await, Some(Ok(OpenAiSttEvent::Completed { item_id, .. })) if item_id == "c")
    );
    assert!(next(&mut session).await.is_none());
    server.join().await;
}

#[tokio::test]
async fn unknown_terminal_never_releases_real_pending_and_deadline_is_absolute() {
    let (listener, mut config) = listen().await;
    config.limits.response_timeout = Duration::from_millis(100);
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        ready(&mut ws).await;
        recv(&mut ws).await;
        send(&mut ws, ack("real")).await;
        send(&mut ws, event("completed", "unknown", "unknown")).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        send(&mut ws, ack("real")).await;
        send(&mut ws, event("completed", "unknown", "duplicate")).await;
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().commit().await.unwrap();
    session.sender().finish().await.unwrap();
    assert!(matches!(
        next(&mut session).await,
        Some(Ok(OpenAiSttEvent::Completed { .. }))
    ));
    assert!(matches!(
        next(&mut session).await,
        Some(Ok(OpenAiSttEvent::Completed { .. }))
    ));
    assert_eq!(
        next(&mut session).await,
        Some(Err(StreamingError::Timeout("OpenAI STT response")))
    );
    server.join().await;
}

#[tokio::test]
async fn unsolicited_ack_and_session_id_budget_are_bounded() {
    for unsolicited in [true, false] {
        let (listener, mut config) = listen().await;
        config.max_tracked_items = 1;
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            ready(&mut ws).await;
            if unsolicited {
                send(&mut ws, ack("unasked")).await;
            } else {
                send(&mut ws, event("completed", "a", "one")).await;
                send(&mut ws, event("completed", "b", "two")).await;
            }
            close(&mut ws).await;
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        if unsolicited {
            assert!(matches!(
                next(&mut session).await,
                Some(Err(StreamingError::Protocol(_)))
            ));
        } else {
            assert!(matches!(next(&mut session).await, Some(Ok(_))));
            assert_eq!(
                next(&mut session).await,
                Some(Err(StreamingError::TooLarge))
            );
        }
        server.join().await;
    }
}

#[tokio::test]
async fn finish_does_not_commit_unsubmitted_audio_and_closes_after_prior_commands() {
    let (listener, config) = listen().await;
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        ready(&mut ws).await;
        assert_eq!(recv(&mut ws).await["type"], "input_audio_buffer.append");
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().send_audio(pcm()).await.unwrap();
    finish(&mut session).await;
    server.join().await;
}

#[tokio::test]
async fn audio_format_raw_and_encoded_limits_reject_before_admission() {
    let (listener, mut config) = listen().await;
    config.max_audio_bytes = 4;
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        ready(&mut ws).await;
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert!(matches!(
        session
            .sender()
            .send_audio(AudioRawFrame::new(vec![0; 4], 16000, 1).unwrap())
            .await,
        Err(StreamingError::Invalid(_))
    ));
    assert_eq!(
        session
            .sender()
            .send_audio(AudioRawFrame::new(vec![0; 6], 24000, 1).unwrap())
            .await,
        Err(StreamingError::TooLarge)
    );
    finish(&mut session).await;
    server.join().await;
    let (listener, mut config) = listen().await;
    config.max_audio_bytes = 512;
    config.limits.max_message_bytes = 512;
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        ready(&mut ws).await;
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert_eq!(
        session
            .sender()
            .send_audio(AudioRawFrame::new(vec![0; 400], 24000, 1).unwrap())
            .await,
        Err(StreamingError::TooLarge)
    );
    finish(&mut session).await;
    server.join().await;
}

#[tokio::test]
async fn full_outbox_has_response_deadline_and_cancel_bypasses_backpressure() {
    for cancel in [false, true] {
        let (listener, mut config) = listen().await;
        config.limits.queue_capacity = 1;
        config.limits.response_timeout = Duration::from_millis(80);
        let (sent, received) = oneshot::channel();
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            ready(&mut ws).await;
            recv(&mut ws).await;
            send(&mut ws, ack("a")).await;
            send(&mut ws, event("delta", "a", "first")).await;
            send(&mut ws, event("delta", "a", "blocked")).await;
            sent.send(()).unwrap();
            close(&mut ws).await;
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        session.sender().commit().await.unwrap();
        received.await.unwrap();
        if cancel {
            assert_eq!(session.shutdown().await, Err(StreamingError::Cancelled));
        } else {
            tokio::time::sleep(Duration::from_millis(150)).await;
            assert!(matches!(next(&mut session).await, Some(Ok(_))));
            assert_eq!(
                next(&mut session).await,
                Some(Err(StreamingError::Timeout("OpenAI STT response")))
            );
        }
        server.join().await;
    }
}

#[tokio::test]
async fn cancelled_borrowed_shutdown_retains_join_owner_and_drop_cancels_clones() {
    let (listener, mut config) = listen().await;
    config.limits.close_timeout = Duration::from_millis(300);
    let (close_seen, received) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
        close_seen.send(()).unwrap();
        released.await.unwrap();
        let _ = ws.flush().await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(25), session.shutdown())
            .await
            .is_err()
    );
    received.await.unwrap();
    release.send(()).unwrap();
    assert_eq!(session.shutdown().await, Err(StreamingError::Cancelled));
    server.join().await;
    let (listener, config) = listen().await;
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        close(&mut ws).await;
    });
    let session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    drop(session);
    assert_eq!(sender.try_commit(), Err(StreamingError::Cancelled));
    server.join().await;
}

#[tokio::test]
async fn provider_fatal_error_is_redacted_and_remote_eof_is_not_success() {
    for fatal in [false, true] {
        let (listener, config) = listen().await;
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            ready(&mut ws).await;
            if fatal {
                send(
                    &mut ws,
                    json!({"type":"error","error":{"message":"secret text","code":"private-code"}}),
                )
                .await;
                close(&mut ws).await;
            } else {
                ws.close(None).await.unwrap();
                assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
            }
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let error = next(&mut session).await.unwrap().unwrap_err();
        assert_eq!(
            error,
            if fatal {
                StreamingError::Provider { status: None }
            } else {
                StreamingError::Closed
            }
        );
        assert!(!format!("{error:?} {error}").contains("secret text"));
        server.join().await;
    }
}

#[tokio::test]
async fn malformed_transcriptions_binary_and_oversized_messages_are_terminal() {
    for mode in [0, 1, 2, 3] {
        let (listener, mut config) = listen().await;
        config.limits.max_message_bytes = 512;
        config.max_audio_bytes = 256;
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            ready(&mut ws).await;
            match mode {
                0 => send(&mut ws, json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"a","content_index":0,"delta":42})).await,
                1 => send(&mut ws, json!({"type":"conversation.item.input_audio_transcription.failed","item_id":"a","content_index":0})).await,
                2 => ws.send(Message::Binary(vec![1,2].into())).await.unwrap(),
                _ => send(&mut ws, json!({"type":"unknown","data":"x".repeat(1024)})).await,
            }
            close(&mut ws).await;
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        let error = next(&mut session).await.unwrap().unwrap_err();
        if mode == 3 {
            assert_eq!(error, StreamingError::TooLarge);
        } else {
            assert!(matches!(error, StreamingError::Protocol(_)));
        }
        server.join().await;
    }
}

#[tokio::test]
async fn held_commit_ack_and_unresponsive_close_have_explicit_timeouts() {
    let (listener, mut config) = listen().await;
    config.limits.response_timeout = Duration::from_millis(70);
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        ready(&mut ws).await;
        recv(&mut ws).await;
        close(&mut ws).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().commit().await.unwrap();
    session.sender().finish().await.unwrap();
    assert_eq!(
        next(&mut session).await,
        Some(Err(StreamingError::Timeout("OpenAI STT response")))
    );
    server.join().await;
    let (listener, config) = listen().await;
    let server = Server::spawn(async move {
        let mut ws = accept(listener).await;
        ready(&mut ws).await;
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
        tokio::time::sleep(Duration::from_millis(200)).await;
    });
    let mut session = OpenAiSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().finish().await.unwrap();
    assert_eq!(
        next(&mut session).await,
        Some(Err(StreamingError::Timeout("OpenAI STT close")))
    );
    server.join().await;
}

#[tokio::test]
async fn independently_executed_python_session_oracle_matches_real_wire() {
    let oracle: Value =
        serde_json::from_str(include_str!("fixtures/openai_stt_source.json")).unwrap();
    assert_eq!(
        oracle["schema"],
        "pipecat-openai-realtime-stt-source-oracle-v1"
    );
    for case in oracle["session_cases"].as_array().unwrap() {
        if case["server_vad_enabled"] == true {
            continue;
        } // Explicitly outside this local-VAD profile.
        let (listener, mut config) = listen().await;
        let settings = &case["settings_after_init"];
        config.model = settings["model"].as_str().unwrap().into();
        config.language = settings["language"].as_str().map(str::to_owned);
        config.prompt = settings["prompt"]
            .as_str()
            .or(case["input"]["prompt"].as_str())
            .map(str::to_owned);
        config.noise_reduction = match settings["noise_reduction"].as_str() {
            Some("near_field") => Some(OpenAiSttNoiseReduction::NearField),
            Some("far_field") => Some(OpenAiSttNoiseReduction::FarField),
            None | Some("") => None,
            _ => panic!("unknown oracle noise enum"),
        };
        config.delay = match settings["delay"].as_str() {
            Some("minimal") => Some(OpenAiSttDelay::Minimal),
            Some("low") => Some(OpenAiSttDelay::Low),
            Some("medium") => Some(OpenAiSttDelay::Medium),
            Some("high") => Some(OpenAiSttDelay::High),
            Some("xhigh") => Some(OpenAiSttDelay::XHigh),
            None | Some("") => None,
            _ => panic!("unknown oracle delay enum"),
        };
        // This exact expected value comes from the pinned original Python AST,
        // never from the Rust serializer or a rewritten protocol fixture.
        let expected = case["trace"][0]["message"].clone();
        let case_id = case["id"].as_str().unwrap().to_owned();
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            assert_eq!(ready(&mut ws).await, expected, "source case {case_id}");
            close(&mut ws).await;
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        finish(&mut session).await;
        server.join().await;
    }
}

#[tokio::test]
async fn terminal_before_ack_has_no_credit_even_for_current_commits() {
    for commit_first in [false, true] {
        let (listener, mut config) = listen().await;
        config.limits.response_timeout = Duration::from_millis(120);
        let server = Server::spawn(async move {
            let mut ws = accept(listener).await;
            ready(&mut ws).await;
            if commit_first {
                recv(&mut ws).await;
            }
            send(&mut ws, event("completed", "future", "early event")).await;
            if !commit_first {
                recv(&mut ws).await;
            }
            send(&mut ws, ack("future")).await;
            // There has been no terminal after this ID's acknowledgement.
            close(&mut ws).await;
        });
        let mut session = OpenAiSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        if commit_first {
            session.sender().commit().await.unwrap();
        }
        assert!(matches!(
            next(&mut session).await,
            Some(Ok(OpenAiSttEvent::Completed { .. }))
        ));
        if !commit_first {
            session.sender().commit().await.unwrap();
        }
        session.sender().finish().await.unwrap();
        assert_eq!(
            next(&mut session).await,
            Some(Err(StreamingError::Timeout("OpenAI STT response")))
        );
        server.join().await;
    }
}
