#![cfg(feature = "soniox")]
//! Real native sockets with synthetic provider payloads, never live credentials.
#[path = "../examples/support/soniox_tools.rs"]
mod arithmetic;
use pipecat_browser_client::{client_ready, protocol};
use pipecat_client_server::{server::BotApi, SessionMethod};
use pipecat_runner::{NativeRunner, Options, RunnerConfig, SonioxVoiceConfig};
use pipecat_services::{Content, Role};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .expect("Soniox cascade stalled")
}
async fn accept(listener: &TcpListener) -> WebSocketSession {
    WebSocketSession::accept(listener.accept().await.unwrap().0, SessionConfig::default())
        .await
        .unwrap()
}
async fn send(session: &WebSocketSession, value: Value) {
    session
        .sender()
        .send(Packet::Text(value.to_string()))
        .await
        .unwrap();
}
async fn message(session: &mut WebSocketSession) -> Value {
    let Packet::Text(text) = session.recv().await.unwrap() else {
        panic!("expected JSON")
    };
    serde_json::from_str(&text).unwrap()
}
async fn request(stream: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut buf = [0; 4096];
    let end = loop {
        let count = stream.read(&mut buf).await.unwrap();
        assert!(count > 0 && bytes.len() + count <= 1024 * 1024);
        bytes.extend_from_slice(&buf[..count]);
        if let Some(index) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end])
        .unwrap()
        .to_ascii_lowercase();
    assert!(headers.contains("authorization: bearer synthetic-openai"));
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.strip_prefix("content-length:")
                .map(|value| value.trim().parse().unwrap())
        })
        .unwrap();
    assert!(length <= 1024 * 1024);
    while bytes.len() < end + length {
        let count = stream.read(&mut buf).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buf[..count]);
    }
    serde_json::from_slice(&bytes[end..end + length]).unwrap()
}
async fn reply(stream: &mut TcpStream, delta: Value, reason: &str) {
    let body = format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"index":0,"delta":delta,"finish_reason":reason}]})
    );
    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
}
fn config(stt: &TcpListener, llm: &TcpListener, tts: &TcpListener) -> SonioxVoiceConfig {
    let mut config = SonioxVoiceConfig::new("synthetic-soniox", "synthetic-openai");
    config.stt.endpoint = format!("ws://{}/stt", stt.local_addr().unwrap());
    config.tts.endpoint = format!("ws://{}/tts", tts.local_addr().unwrap());
    config.llm.endpoint = format!("http://{}/chat", llm.local_addr().unwrap());
    config.stt.limits.close_timeout = Duration::from_millis(100);
    config.tts.limits.close_timeout = Duration::from_millis(100);
    config.tts.sample_rate = 16000;
    config.session_tools = Some(Arc::new(arithmetic::tools));
    config
}
async fn connect(runner: &NativeRunner) -> (Value, WebSocketSession) {
    let started = runner
        .start(&json!({"transport":"websocket"}))
        .await
        .unwrap();
    assert_eq!(started["contextMode"], "generated-text");
    assert_eq!(started["playbackAcknowledgements"], false);
    let mut client =
        WebSocketSession::connect(started["wsUrl"].as_str().unwrap(), SessionConfig::default())
            .await
            .unwrap();
    client
        .sender()
        .send(Packet::Binary(
            protocol::encode_message(&client_ready(57)).into(),
        ))
        .await
        .unwrap();
    loop {
        if let Some(protocol::frame::Body::Message(frame)) = decoded(&mut client).await {
            let frame: Value = serde_json::from_str(&frame.data).unwrap();
            if frame["type"] == "bot-ready" {
                assert_eq!(frame["id"], "57");
                assert_eq!(
                    frame["data"]["capabilities"]["context_mode"],
                    "generated-text"
                );
                break;
            }
        }
    }
    (started, client)
}
async fn decoded(client: &mut WebSocketSession) -> Option<protocol::frame::Body> {
    let Packet::Binary(bytes) = client.recv().await.expect("browser disconnected") else {
        panic!("expected binary framing")
    };
    protocol::decode(&bytes).unwrap().body
}
async fn speak(client: &WebSocketSession) {
    client
        .sender()
        .send(Packet::Binary(
            protocol::encode_audio(&vec![0.; 1536], 48000).into(),
        ))
        .await
        .unwrap();
}
async fn output(client: &mut WebSocketSession, expected: &str) {
    let mut generated = String::new();
    let mut audio = vec![];
    let mut stopped = false;
    while !stopped || audio.len() != 8 || generated != expected {
        match decoded(client).await {
            Some(protocol::frame::Body::Audio(frame)) => {
                assert_eq!(frame.sample_rate, 16000);
                assert_eq!(frame.num_channels, 1);
                audio.extend(frame.audio);
            }
            Some(protocol::frame::Body::Message(frame)) => {
                let frame: Value = serde_json::from_str(&frame.data).unwrap();
                assert_ne!(frame["type"], "bot-started-speaking");
                assert_ne!(frame["type"], "bot-stopped-speaking");
                if frame["type"] == "bot-output" {
                    assert_eq!(frame["data"]["spoken"], false);
                    assert!(frame["data"].get("spoken_status").is_none());
                    generated = frame["data"]["text"].as_str().unwrap().into();
                }
                if frame["type"] == "bot-tts-stopped" {
                    stopped = true;
                }
            }
            _ => {}
        }
    }
    assert_eq!(audio, [0, 0, 1, 0, 255, 255, 2, 0]);
}

#[tokio::test]
async fn complete_cascade_tool_continuation_two_turn_history_and_fresh_sessions() {
    bounded(async {
        let stt = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let llm = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let runner = NativeRunner::new_soniox(RunnerConfig { allow_missing_origin: true, options: Options { introduce: false, ..Default::default() }, ..Default::default() }, config(&stt, &llm, &tts)).unwrap();
        let mut reports = runner.subscribe();
        let finalized = Arc::new(AtomicBool::new(false));
        let final_stt = finalized.clone();
        let stt_server = tokio::spawn(async move {
            for _ in 0..2 {
                final_stt.store(false, Ordering::SeqCst);
                let mut peer = accept(&stt).await;
                let setup = message(&mut peer).await;
                assert_eq!(setup["api_key"], "synthetic-soniox");
                assert_eq!(setup["enable_endpoint_detection"], true);
                assert_eq!(setup["sample_rate"], 16000);
                assert_eq!(setup["audio_format"], "pcm_s16le");
                assert_eq!(setup["num_channels"], 1);
                for (index, user) in ["Add two and three.", "Remember the result."].iter().enumerate() {
                    let Packet::Binary(audio) = peer.recv().await.unwrap() else { panic!("missing PCM") };
                    assert_eq!(audio, vec![0; if index == 0 { 956 } else { 1024 }]);
                    send(&peer, json!({"tokens":[{"text":user,"is_final":true},{"text":"ignored interim","is_final":false}]})).await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    final_stt.store(true, Ordering::SeqCst);
                    send(&peer, json!({"tokens":[{"text":"<end>","is_final":true}]})).await;
                }
                let Packet::Binary(tail) = peer.recv().await.unwrap() else { panic!("End failed to drain resampler") };
                assert_eq!(tail, vec![0; 68]);
                assert_eq!(peer.recv().await, Some(Packet::Text(String::new())));
                while peer.recv().await.is_some() {}
                let _ = peer.finish().await;
            }
        });
        let llm_server = tokio::spawn(async move {
            for _ in 0..2 {
                for round in 0..3 {
                    let (mut stream, _) = llm.accept().await.unwrap();
                    let body = request(&mut stream).await;
                    assert!(finalized.load(Ordering::SeqCst), "interim transcript ran LLM");
                    assert_eq!(body["stream"], true);
                    assert_eq!(body["tools"][0]["function"]["name"], "add");
                    let messages: Vec<_> = body["messages"].as_array().unwrap().iter().filter(|message| message["role"] != "system").collect();
                    assert_eq!(messages[0]["content"], "Add two and three.");
                    match round {
                        0 => {
                            assert_eq!(messages.len(), 1, "session inherited history");
                            reply(&mut stream, json!({"tool_calls":[{"index":0,"id":"same-call-id","type":"function","function":{"name":"add","arguments":"{\"a\":2,\"b\":3}"}}]}), "tool_calls").await;
                        }
                        1 => {
                            assert_eq!(messages.len(), 3);
                            assert_eq!(messages[1]["tool_calls"][0]["id"], "same-call-id");
                            assert_eq!(messages[2]["role"], "tool");
                            assert_eq!(messages[2]["tool_call_id"], "same-call-id");
                            let result: Value = serde_json::from_str(messages[2]["content"].as_str().unwrap()).unwrap();
                            assert_eq!(result, json!({"sum":5,"session_invocation":1}));
                            reply(&mut stream, json!({"content":"Five."}), "stop").await;
                        }
                        _ => {
                            assert_eq!(messages.len(), 5);
                            assert_eq!(messages[3]["role"], "assistant");
                            assert_eq!(messages[3]["content"], "Five.");
                            assert_eq!(messages[4]["content"], "Remember the result.");
                            reply(&mut stream, json!({"content":"Still five."}), "stop").await;
                        }
                    }
                }
            }
        });
        let tts_server = tokio::spawn(async move {
            for _ in 0..2 {
                let mut peer = accept(&tts).await;
                let mut streams: BTreeMap<String, String> = BTreeMap::new();
                let mut completed = vec![];
                while let Some(Packet::Text(text)) = peer.recv().await {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    let id = value["stream_id"].as_str().unwrap().to_owned();
                    if value.get("api_key").is_some() {
                        assert_eq!(value["api_key"], "synthetic-soniox");
                        assert_eq!(value["sample_rate"], 16000);
                        assert_eq!(value["audio_format"], "pcm_s16le");
                        assert!(streams.insert(id, String::new()).is_none());
                    } else if value["cancel"] == true {
                        assert_eq!(streams.remove(&id).unwrap_or_default(), "");
                    } else {
                        streams.get_mut(&id).unwrap().push_str(value["text"].as_str().unwrap());
                        if value["text_end"] == true {
                            let text = streams.remove(&id).unwrap();
                            assert_eq!(text, if completed.is_empty() { "Five." } else { "Still five." });
                            completed.push(text);
                            send(&peer, json!({"stream_id":id,"audio":"AAABAP//AgA="})).await;
                            send(&peer, json!({"stream_id":id,"terminated":true})).await;
                        }
                    }
                }
                assert_eq!(completed, ["Five.", "Still five."]);
                let _ = peer.finish().await;
            }
        });
        for _ in 0..2 {
            let (_, mut client) = connect(&runner).await;
            speak(&client).await; output(&mut client, "Five.").await;
            speak(&client).await; output(&mut client, "Still five.").await;
            client.sender().send(Packet::Binary(protocol::encode_message(&json!({"id":"end","label":"rtvi-ai","type":"disconnect-bot"})).into())).await.unwrap();
            while client.recv().await.is_some() {}
            let _ = client.finish().await;
            let summary = reports.recv().await.unwrap();
            assert!(summary.successful);
            assert_eq!(summary.pipeline_errors, 0);
            let context = summary.generated_context;
            assert_eq!(context.messages.len(), 6);
            assert_eq!(context.messages[5].role, Role::Assistant);
            assert_eq!(context.messages[5].content, Some(Content::Text("Still five.".into())));
            assert_eq!(runner.active_sessions(), 0);
        }
        stt_server.await.unwrap(); llm_server.await.unwrap(); tts_server.await.unwrap();
        runner.shutdown().await;
    }).await;
}

#[tokio::test]
async fn canceled_and_dropped_runner_close_blocked_stt_setup_and_session_listener() {
    bounded(async {
        for dropped in [false, true] {
            let stt = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let llm = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let runner = NativeRunner::new_soniox(
                RunnerConfig {
                    allow_missing_origin: true,
                    ..Default::default()
                },
                config(&stt, &llm, &tts),
            )
            .unwrap();
            let started = runner
                .start(&json!({"transport":"websocket"}))
                .await
                .unwrap();
            let mut client = WebSocketSession::connect(
                started["wsUrl"].as_str().unwrap(),
                SessionConfig::default(),
            )
            .await
            .unwrap();
            let (mut provider, _) = stt.accept().await.unwrap();
            if dropped {
                drop(runner);
            } else {
                runner
                    .session(
                        SessionMethod::Post,
                        started["sessionId"].as_str().unwrap(),
                        &["stop"],
                        &json!({}),
                    )
                    .await
                    .unwrap();
                runner.shutdown().await;
                assert_eq!(runner.active_sessions(), 0);
            }
            let mut buffer = [0; 1024];
            loop {
                if provider.read(&mut buffer).await.unwrap() == 0 {
                    break;
                }
            }
            assert!(client.recv().await.is_none());
            let _ = client.finish().await;
            assert!(
                tokio::time::timeout(Duration::from_millis(50), llm.accept())
                    .await
                    .is_err()
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(50), tts.accept())
                    .await
                    .is_err()
            );
        }
    })
    .await;
}

#[tokio::test]
async fn factory_panic_and_error_release_slot_before_a_replacement_session() {
    bounded(async {
        for panic in [false, true] {
            let stt = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let llm = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let mut providers = config(&stt, &llm, &tts);
            let first = Arc::new(AtomicBool::new(true));
            providers.session_tools = Some(Arc::new(move || {
                if first.swap(false, Ordering::SeqCst) {
                    if panic {
                        panic!("synthetic factory panic");
                    }
                    return Err(pipecat_core::ProcessorError::new("synthetic factory error"));
                }
                arithmetic::tools()
            }));
            let runner = NativeRunner::new_soniox(
                RunnerConfig {
                    allow_missing_origin: true,
                    max_sessions: 1,
                    options: Options {
                        introduce: false,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                providers,
            )
            .unwrap();
            let mut reports = runner.subscribe();
            let start = runner
                .start(&json!({"transport":"websocket"}))
                .await
                .unwrap();
            let mut failed = WebSocketSession::connect(
                start["wsUrl"].as_str().unwrap(),
                SessionConfig::default(),
            )
            .await
            .unwrap();
            assert!(failed.recv().await.is_none());
            let _ = failed.finish().await;
            let report = reports.recv().await.unwrap();
            assert!(!report.successful);
            assert_eq!(runner.active_sessions(), 0);
            assert!(
                tokio::time::timeout(Duration::from_millis(30), stt.accept())
                    .await
                    .is_err()
            );
            let provider = tokio::spawn(async move {
                let mut peer = accept(&stt).await;
                assert_eq!(message(&mut peer).await["api_key"], "synthetic-soniox");
                while peer.recv().await.is_some() {}
                let _ = peer.finish().await;
            });
            let (start, mut replacement) = connect(&runner).await;
            runner
                .session(
                    SessionMethod::Post,
                    start["sessionId"].as_str().unwrap(),
                    &["stop"],
                    &json!({}),
                )
                .await
                .unwrap();
            while replacement.recv().await.is_some() {}
            let _ = replacement.finish().await;
            let report = reports.recv().await.unwrap();
            assert!(report.successful);
            assert_eq!(runner.active_sessions(), 0);
            runner.shutdown().await;
            provider.await.unwrap();
        }
    })
    .await;
}
