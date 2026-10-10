use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use pipecat_soniox::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
type Ws = WebSocketStream<TcpStream>;
async fn listen() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/native", listener.local_addr().unwrap());
    (listener, endpoint)
}
async fn msg(ws: &mut Ws) -> Message {
    tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn read(ws: &mut Ws) -> Value {
    match msg(ws).await {
        Message::Text(t) => serde_json::from_str(&t).unwrap(),
        m => panic!("expected JSON got {m:?}"),
    }
}
async fn send(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}
async fn stt_next(s: &mut SonioxSttSession) -> Result<SonioxSttEvent, StreamingError> {
    tokio::time::timeout(Duration::from_secs(3), s.next())
        .await
        .unwrap()
        .unwrap()
}
async fn tts_next(s: &mut SonioxTtsStream) -> Result<SonioxTtsEvent, StreamingError> {
    tokio::time::timeout(Duration::from_secs(3), s.next())
        .await
        .unwrap()
        .unwrap()
}
fn token(text: &str, finalized: bool, language: &str) -> Value {
    json!({"text":text,"is_final":finalized,"language":language,"speaker":"speaker-1"})
}
fn times(text: &str, offset: f64) -> Value {
    json!({"characters":text.chars().map(|c|c.to_string()).collect::<Vec<_>>(),"character_start_times_seconds":text.chars().enumerate().map(|(i,_)|offset+i as f64/10.0).collect::<Vec<_>>()})
}
#[tokio::test]
async fn stt_wire_pcm_finalization_language_and_end_text() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let config = read(&mut ws).await;
        assert_eq!(config["api_key"], "secret");
        assert_eq!(config["model"], "stt-rt-v5");
        assert_eq!(config["language_hints"], json!(["en", "fr"]));
        assert_eq!(config["audio_format"], "pcm_s16le");
        assert_eq!(config["sample_rate"], 16000);
        assert_eq!(config["context"], Value::Null);
        assert_eq!(config["enable_endpoint_detection"], false);
        assert_eq!(msg(&mut ws).await, Message::Binary(vec![0, 0, 1, 0].into()));
        assert_eq!(read(&mut ws).await, json!({"type":"finalize"}));
        send(
            &mut ws,
            json!({"tokens":[token("Bonjour",true,"fr"),token(" hello",false,"en")]}),
        )
        .await;
        send(
            &mut ws,
            json!({"tokens":[token(" friend",true,"en"),token("<fin>",true,"en")]}),
        )
        .await;
        assert_eq!(msg(&mut ws).await, Message::Text("".into()));
        assert!(matches!(msg(&mut ws).await, Message::Close(_)));
    });
    let mut config = SonioxSttConfig::new("secret");
    config.endpoint = endpoint;
    config.settings.language_hints = Some(vec!["en-US".into(), "fr".into(), "en-GB".into()]);
    let mut session = SonioxSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender
        .send_audio(AudioRawFrame::new(vec![0, 0, 1, 0], 16000, 1).unwrap())
        .await
        .unwrap();
    sender.finalize().await.unwrap();
    assert!(
        matches!(stt_next(&mut session).await.unwrap(),SonioxSttEvent::AudioSent{seconds} if seconds>0.0)
    );
    let SonioxSttEvent::Transcript(interim) = stt_next(&mut session).await.unwrap() else {
        panic!()
    };
    assert_eq!(interim.text, "Bonjour hello");
    assert!(!interim.finalized);
    let SonioxSttEvent::Transcript(finalized) = stt_next(&mut session).await.unwrap() else {
        panic!()
    };
    assert_eq!(finalized.text, "Bonjour friend");
    assert!(finalized.finalized);
    assert_eq!(finalized.language.as_deref(), Some("fr"));
    assert_eq!(finalized.tokens[0]["speaker"], "speaker-1");
    sender.finish().await.unwrap();
    assert_eq!(session.next().await, None);
    assert_eq!(sender.finalize().await, Err(StreamingError::Closed));
    peer.await.unwrap();
}
#[tokio::test]
async fn stt_provider_turn_vad_dedup_close_reopen_error_flush() {
    let (listener, endpoint) = listen().await;
    let (go, wait) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut ws).await["enable_endpoint_detection"], true);
        wait.await.unwrap();
        send(&mut ws,json!({"tokens":[token("one",true,"xx"),token("<end>",true,"en"),token("two",true,"en")]})).await;
        send(
            &mut ws,
            json!({"error_code":429,"error_message":"private provider content"}),
        )
        .await;
        let _ = ws.next().await;
    });
    let mut config = SonioxSttConfig::new("secret");
    config.endpoint = endpoint;
    config.vad_force_turn_endpoint = false;
    let mut s = SonioxSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    s.sender().try_start_turn().unwrap();
    assert_eq!(stt_next(&mut s).await.unwrap(), SonioxSttEvent::TurnStarted);
    s.sender().try_start_turn().unwrap();
    s.sender().try_finalize().unwrap();
    go.send(()).unwrap();
    let expected = [
        "final-one",
        "stop",
        "start",
        "interim-two",
        "interim-two",
        "final-two",
        "stop",
        "error",
    ];
    for label in expected {
        let event = stt_next(&mut s).await;
        match label {
            "final-one" => assert!(
                matches!(event,Ok(SonioxSttEvent::Transcript(t)) if t.text=="one"&&t.finalized&&t.language.is_none())
            ),
            "final-two" => assert!(
                matches!(event,Ok(SonioxSttEvent::Transcript(t)) if t.text=="two"&&t.finalized)
            ),
            "interim-two" => assert!(
                matches!(event,Ok(SonioxSttEvent::Transcript(t)) if t.text=="two"&&!t.finalized)
            ),
            "stop" => assert_eq!(event, Ok(SonioxSttEvent::TurnStopped)),
            "start" => assert_eq!(event, Ok(SonioxSttEvent::TurnStarted)),
            _ => assert_eq!(event, Err(StreamingError::Provider { status: Some(429) })),
        }
    }
    peer.await.unwrap();
}
#[tokio::test]
async fn stt_settings_reconnect_once_without_replaying_audio() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut first).await["model"], "stt-rt-v5");
        assert!(matches!(msg(&mut first).await, Message::Binary(_)));
        assert!(matches!(msg(&mut first).await, Message::Close(_)));
        drop(first);
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut second).await["model"], "stt-rt-custom");
        assert_eq!(msg(&mut second).await, Message::Binary(vec![2, 0].into()));
        assert_eq!(msg(&mut second).await, Message::Text("".into()));
        let _ = second.next().await;
    });
    let mut c = SonioxSttConfig::new("key");
    c.endpoint = endpoint;
    let mut s = SonioxSttClient::new(c).unwrap().connect().await.unwrap();
    let tx = s.sender();
    tx.send_audio(AudioRawFrame::new(vec![1, 0], 16000, 1).unwrap())
        .await
        .unwrap();
    tx.merge_settings(json!({"model":"stt-rt-custom"}).as_object().unwrap())
        .unwrap();
    tx.send_audio(AudioRawFrame::new(vec![2, 0], 16000, 1).unwrap())
        .await
        .unwrap();
    assert!(matches!(
        stt_next(&mut s).await.unwrap(),
        SonioxSttEvent::AudioSent { .. }
    ));
    assert_eq!(stt_next(&mut s).await.unwrap(), SonioxSttEvent::Reset);
    assert!(matches!(
        stt_next(&mut s).await.unwrap(),
        SonioxSttEvent::AudioSent { .. }
    ));
    tx.finish().await.unwrap();
    assert_eq!(s.next().await, None);
    peer.await.unwrap();
}
#[tokio::test]
async fn stt_cancel_discards_saturated_output_and_never_sends_end_audio() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        for _ in 0..5 {
            send(&mut ws, json!({"tokens":[token("hello",false,"en")]})).await;
        }
        assert!(matches!(msg(&mut ws).await, Message::Close(_)));
    });
    let mut c = SonioxSttConfig::new("key");
    c.endpoint = endpoint;
    c.limits.queue_capacity = 1;
    let mut s = SonioxSttClient::new(c).unwrap().connect().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    s.cancel();
    assert_eq!(stt_next(&mut s).await, Err(StreamingError::Cancelled));
    assert_eq!(s.next().await, None);
    peer.await.unwrap();
}
#[tokio::test]
async fn stt_malformed_and_metadata_accumulation_are_bounded() {
    for bad in [
        json!({"tokens":[{"text":"missing"}]}),
        json!({"tokens":[{"text":"a","is_final":true,"metadata":"x".repeat(500)}]}),
    ] {
        let (listener, endpoint) = listen().await;
        let peer = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            read(&mut ws).await;
            send(&mut ws, bad).await;
            let _ = ws.next().await;
        });
        let mut c = SonioxSttConfig::new("key");
        c.endpoint = endpoint;
        c.max_transcript_bytes = 256;
        let mut s = SonioxSttClient::new(c).unwrap().connect().await.unwrap();
        assert!(matches!(
            stt_next(&mut s).await,
            Err(StreamingError::Protocol(_) | StreamingError::TooLarge)
        ));
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn tts_interleaves_two_streams_on_one_socket_with_language_snapshots() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let a = read(&mut ws).await;
        let b = read(&mut ws).await;
        assert_eq!(a["stream_id"], "a");
        assert_eq!(a["language"], "en");
        assert_eq!(b["stream_id"], "b");
        assert_eq!(b["language"], "ja");
        assert_eq!(a["return_timestamps"], true);
        assert_eq!(a["api_key"], "key");
        let mut chunks = vec![];
        for _ in 0..4 {
            chunks.push(read(&mut ws).await);
        }
        assert_eq!(chunks.iter().filter(|v| v["text_end"] == true).count(), 2);
        send(
            &mut ws,
            json!({"stream_id":"a","audio":STANDARD.encode([1]),"timestamps":times("Hel",0.0)}),
        )
        .await;
        send(&mut ws,json!({"stream_id":"b","audio":STANDARD.encode([4,0]),"timestamps":times("你，好",0.0)})).await;
        send(&mut ws,json!({"stream_id":"a","audio":STANDARD.encode([0,2,0]),"timestamps":times("lo world",0.3),"audio_end":true})).await;
        send(&mut ws, json!({"stream_id":"b","terminated":true})).await;
        send(
            &mut ws,
            json!({"stream_id":"a","terminated":true,"audio":"ignored terminal payload"}),
        )
        .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
        let _ = ws.next().await;
    });
    let mut c = SonioxTtsConfig::new("key");
    c.endpoint = endpoint;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut a = client.stream("a").unwrap();
    let mut settings = client.config().settings.clone();
    settings.language = Some("ja".into());
    let next = client.with_settings(settings).unwrap();
    let mut b = next.stream("b").unwrap();
    a.sender().send_text("Hello world").await.unwrap();
    b.sender().send_text("你，好").await.unwrap();
    a.sender().finish().await.unwrap();
    b.sender().finish().await.unwrap();
    let (mut audio_a, mut words_a, mut audio_b, mut words_b) = (vec![], vec![], vec![], vec![]);
    for stream in [&mut a, &mut b] {
        loop {
            match tts_next(stream).await.unwrap() {
                SonioxTtsEvent::Audio(frame) => {
                    if frame.context_id.as_deref() == Some("a") {
                        audio_a.extend(frame.audio);
                    } else {
                        audio_b.extend(frame.audio);
                    }
                }
                SonioxTtsEvent::WordTimestamps { context_id, words } => {
                    if context_id == "a" {
                        words_a.extend(words.into_iter().map(|w| w.word));
                    } else {
                        words_b.extend(words.into_iter().map(|w| w.word));
                    }
                }
                SonioxTtsEvent::Done { .. } => break,
            }
        }
    }
    assert_eq!(audio_a, [1, 0, 2, 0]);
    assert_eq!(audio_b, [4, 0]);
    assert_eq!(words_a, ["Hello", "world"]);
    assert_eq!(words_b, ["你", "好"]);
    assert!(a.source_audio_observed());
    assert!(matches!(
        client.stream("a"),
        Err(StreamingError::Invalid(_))
    ));
    client.cancel();
    peer.await.unwrap();
}
#[tokio::test]
async fn tts_one_provider_error_does_not_fail_a_sibling() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        read(&mut ws).await;
        let mut ended = 0;
        while ended < 2 {
            if read(&mut ws).await["text_end"] == true {
                ended += 1;
            }
        }
        send(&mut ws,json!({"stream_id":"a","error_code":429,"error_message":"secret echo","audio":STANDARD.encode([9,0])})).await;
        send(
            &mut ws,
            json!({"stream_id":"b","audio":STANDARD.encode([2,0])}),
        )
        .await;
        send(&mut ws, json!({"stream_id":"b","terminated":true})).await;
        let _ = ws.next().await;
    });
    let mut c = SonioxTtsConfig::new("key");
    c.endpoint = endpoint;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut a = client.stream("a").unwrap();
    let mut b = client.stream("b").unwrap();
    for tx in [a.sender(), b.sender()] {
        tx.send_text("hello").await.unwrap();
        tx.finish().await.unwrap();
    }
    assert_eq!(
        tts_next(&mut a).await,
        Err(StreamingError::Provider { status: Some(429) })
    );
    assert!(!a.source_audio_observed());
    assert!(matches!(
        tts_next(&mut b).await,
        Ok(SonioxTtsEvent::Audio(_))
    ));
    assert!(matches!(
        tts_next(&mut b).await,
        Ok(SonioxTtsEvent::Done { .. })
    ));
    client.cancel();
    peer.await.unwrap();
}
#[tokio::test]
async fn tts_cancel_is_per_stream_and_ignores_late_packets() {
    let (listener, endpoint) = listen().await;
    let (ready, wait) = oneshot::channel();
    let (go, release) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        read(&mut ws).await;
        ready.send(()).unwrap();
        release.await.unwrap();
        assert_eq!(read(&mut ws).await, json!({"stream_id":"a","cancel":true}));
        send(
            &mut ws,
            json!({"stream_id":"a","audio":STANDARD.encode([9,0])}),
        )
        .await;
        let text = read(&mut ws).await;
        assert_eq!(text["stream_id"], "b");
        assert_eq!(read(&mut ws).await["text_end"], true);
        send(
            &mut ws,
            json!({"stream_id":"b","audio":STANDARD.encode([2,0])}),
        )
        .await;
        send(&mut ws, json!({"stream_id":"b","terminated":true})).await;
        let _ = ws.next().await;
    });
    let mut c = SonioxTtsConfig::new("key");
    c.endpoint = endpoint;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut a = client.stream("a").unwrap();
    let mut b = client.stream("b").unwrap();
    wait.await.unwrap();
    a.cancel();
    go.send(()).unwrap();
    assert_eq!(tts_next(&mut a).await, Err(StreamingError::Cancelled));
    tokio::time::sleep(Duration::from_millis(20)).await;
    b.sender().send_text("b").await.unwrap();
    b.sender().finish().await.unwrap();
    assert!(matches!(
        tts_next(&mut b).await,
        Ok(SonioxTtsEvent::Audio(_))
    ));
    assert!(matches!(
        tts_next(&mut b).await,
        Ok(SonioxTtsEvent::Done { .. })
    ));
    client.cancel();
    peer.await.unwrap();
}
#[tokio::test]
async fn tts_backpressure_retires_only_stalled_stream() {
    let (listener, endpoint) = listen().await;
    let (peer_done, done) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        read(&mut ws).await;
        for _ in 0..3 {
            send(
                &mut ws,
                json!({"stream_id":"a","audio":STANDARD.encode([1,0])}),
            )
            .await;
        }
        assert_eq!(read(&mut ws).await, json!({"stream_id":"a","cancel":true}));
        peer_done.send(()).unwrap();
        let _ = ws.next().await;
    });
    let mut c = SonioxTtsConfig::new("key");
    c.endpoint = endpoint;
    c.limits.queue_capacity = 2;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut a = client.stream("a").unwrap();
    let b = client.stream("b").unwrap();
    done.await.unwrap();
    for _ in 0..2 {
        assert!(matches!(
            tts_next(&mut a).await,
            Ok(SonioxTtsEvent::Audio(_))
        ));
    }
    assert_eq!(tts_next(&mut a).await, Err(StreamingError::Backpressure));
    b.sender().try_send_text("still alive").unwrap();
    client.cancel();
    peer.await.unwrap();
}
#[tokio::test]
async fn tts_malformed_timestamp_or_pcm_fails_stream_without_done() {
    for value in [
        json!({"stream_id":"x","timestamps":{"characters":["h"],"character_start_times_seconds":[]}}),
        json!({"stream_id":"x","audio":"!invalid"}),
        json!({"stream_id":"x","audio":STANDARD.encode([1])}),
    ] {
        let (listener, endpoint) = listen().await;
        let peer = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            read(&mut ws).await;
            read(&mut ws).await;
            read(&mut ws).await;
            send(&mut ws, value).await;
            send(&mut ws, json!({"stream_id":"x","terminated":true})).await;
            let _ = ws.next().await;
        });
        let mut c = SonioxTtsConfig::new("key");
        c.endpoint = endpoint;
        let client = SonioxTtsClient::new(c).unwrap();
        let mut x = client.stream("x").unwrap();
        x.sender().send_text("h").await.unwrap();
        x.sender().finish().await.unwrap();
        assert!(matches!(
            tts_next(&mut x).await,
            Err(StreamingError::Protocol(_) | StreamingError::PartialSample)
        ));
        client.cancel();
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn tts_timeout_and_socket_failure_do_not_retry_paid_work() {
    for timeout in [true, false] {
        let (listener, endpoint) = listen().await;
        let peer = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            read(&mut ws).await;
            read(&mut ws).await;
            read(&mut ws).await;
            if timeout {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            drop(ws);
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        });
        let mut c = SonioxTtsConfig::new("key");
        c.endpoint = endpoint;
        c.limits.response_timeout = Duration::from_millis(100);
        let client = SonioxTtsClient::new(c).unwrap();
        let mut x = client.stream("x").unwrap();
        x.sender().send_text("paid").await.unwrap();
        x.sender().finish().await.unwrap();
        assert!(matches!(
            tts_next(&mut x).await,
            Err(StreamingError::Timeout(_) | StreamingError::Closed | StreamingError::Transport)
        ));
        client.cancel();
        peer.await.unwrap();
    }
}
#[test]
fn typed_settings_validate_atomically_and_redact_secrets() {
    let settings = SonioxTtsSettings::default();
    assert!(settings
        .merge(json!({"speed":1.4}).as_object().unwrap())
        .is_err());
    assert!(settings
        .merge(json!({"unknown":1}).as_object().unwrap())
        .is_err());
    assert_eq!(settings.speed, None);
    assert_eq!(
        settings
            .merge(json!({"speed":1.2,"language":null}).as_object().unwrap())
            .unwrap()
            .language,
        None
    );
    let c = SonioxTtsConfig::new("DO_NOT_LOG_ME");
    assert!(!format!("{c:?}").contains("DO_NOT_LOG_ME"));
    let mut c = SonioxSttConfig::new("DO_NOT_LOG_ME");
    assert!(!format!("{c:?}").contains("DO_NOT_LOG_ME"));
    c.endpoint = "ws://example.com/stt".into();
    assert!(SonioxSttClient::new(c).is_err());
}
#[tokio::test]
async fn stt_fragmented_websocket_preserves_region_language_and_finished_only_finals() {
    use tokio_tungstenite::tungstenite::protocol::frame::{
        coding::{Data, OpCode},
        Frame as WireFrame,
    };
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        let payload=json!({"tokens":[token("region",true,"en-US"),token(" uncertain",false,"en")],"finished":true}).to_string();
        let split = payload.len() / 2;
        ws.send(Message::Frame(WireFrame::message(
            payload.as_bytes()[..split].to_vec(),
            OpCode::Data(Data::Text),
            false,
        )))
        .await
        .unwrap();
        ws.send(Message::Frame(WireFrame::message(
            payload.as_bytes()[split..].to_vec(),
            OpCode::Data(Data::Continue),
            true,
        )))
        .await
        .unwrap();
        let _ = ws.next().await;
    });
    let mut c = SonioxSttConfig::new("key");
    c.endpoint = endpoint;
    let mut s = SonioxSttClient::new(c).unwrap().connect().await.unwrap();
    assert!(
        matches!(stt_next(&mut s).await,Ok(SonioxSttEvent::Transcript(t)) if t.text=="region uncertain"&&!t.finalized)
    );
    assert!(
        matches!(stt_next(&mut s).await,Ok(SonioxSttEvent::Transcript(t)) if t.text=="region"&&t.finalized&&t.language.as_deref()==Some("en-US"))
    );
    assert_eq!(s.next().await, None);
    peer.await.unwrap();
}
#[tokio::test]
async fn handshake_failure_is_typed_redacted_and_never_retried() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut bytes = vec![0; 4096];
        let n = socket.read(&mut bytes).await.unwrap();
        let request = String::from_utf8_lossy(&bytes[..n]);
        assert!(!request.contains("api_key"));
        assert!(!request.to_lowercase().contains("authorization"));
        socket.write_all(b"HTTP/1.1 429 Too Many Requests\r\nContent-Length: 11\r\nConnection: close\r\n\r\nsecret echo").await.unwrap();
        drop(socket);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let mut c = SonioxTtsConfig::new("DO_NOT_LOG_ME");
    c.endpoint = endpoint;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut s = client.stream("x").unwrap();
    s.sender().send_text("paid").await.unwrap();
    assert_eq!(tts_next(&mut s).await, Err(StreamingError::Http(429)));
    assert!(matches!(client.stream("y"), Err(StreamingError::Closed)));
    peer.await.unwrap();
}
#[tokio::test]
async fn stt_multichannel_bytes_are_not_relabelled_or_resampled() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let c = read(&mut ws).await;
        assert_eq!(c["num_channels"], 2);
        assert_eq!(c["sample_rate"], 44100);
        assert_eq!(msg(&mut ws).await, Message::Binary(vec![1, 0, 2, 0].into()));
        assert_eq!(msg(&mut ws).await, Message::Text("".into()));
        let _ = ws.next().await;
    });
    let mut c = SonioxSttConfig::new("key");
    c.endpoint = endpoint;
    c.num_channels = 2;
    c.sample_rate = 44100;
    let mut s = SonioxSttClient::new(c).unwrap().connect().await.unwrap();
    let tx = s.sender();
    assert!(tx
        .try_send_audio(AudioRawFrame::new(vec![1, 0], 44100, 1).unwrap())
        .is_err());
    tx.send_audio(AudioRawFrame::new(vec![1, 0, 2, 0], 44100, 2).unwrap())
        .await
        .unwrap();
    tx.finish().await.unwrap();
    assert!(
        matches!(stt_next(&mut s).await,Ok(SonioxSttEvent::AudioSent{seconds}) if (seconds-1.0/44100.0).abs()<1e-9)
    );
    assert_eq!(s.next().await, None);
    peer.await.unwrap();
}
#[tokio::test]
async fn tts_finish_and_text_race_has_one_terminal_and_no_post_terminal_text() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        loop {
            let event = read(&mut ws).await;
            if event["text_end"] == true {
                send(&mut ws, json!({"stream_id":"x","terminated":true})).await;
                break;
            }
        }
        assert!(matches!(msg(&mut ws).await, Message::Close(_)));
    });
    let mut c = SonioxTtsConfig::new("key");
    c.endpoint = endpoint;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut s = client.stream("x").unwrap();
    let tx = s.sender();
    let (other, finish) = tokio::join!(tx.send_text("hello"), tx.finish());
    finish.unwrap();
    assert!(other.is_ok() || other == Err(StreamingError::Closed));
    assert_eq!(tx.try_send_text("late"), Err(StreamingError::Closed));
    assert!(matches!(
        tts_next(&mut s).await,
        Ok(SonioxTtsEvent::Done { .. })
    ));
    client.cancel();
    peer.await.unwrap();
}
#[tokio::test]
async fn both_protocol_keepalives_are_real_text_messages() {
    let (stt_listener, stt_endpoint) = listen().await;
    let (tts_listener, tts_endpoint) = listen().await;
    let stt_peer = tokio::spawn(async move {
        let mut ws = accept_async(stt_listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        let event = tokio::time::timeout(Duration::from_secs(8), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event, Message::Text("{\"type\":\"keepalive\"}".into()));
        assert_eq!(msg(&mut ws).await, Message::Text("".into()));
        let _ = ws.next().await;
    });
    let tts_peer = tokio::spawn(async move {
        let mut ws = accept_async(tts_listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        let event = tokio::time::timeout(Duration::from_secs(23), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(event, Message::Text("{\"keep_alive\":true}".into()));
        let _ = ws.next().await;
    });
    let mut stt = SonioxSttConfig::new("key");
    stt.endpoint = stt_endpoint;
    let mut stt = SonioxSttClient::new(stt).unwrap().connect().await.unwrap();
    let mut tts = SonioxTtsConfig::new("key");
    tts.endpoint = tts_endpoint;
    let tts = SonioxTtsClient::new(tts).unwrap();
    let _stream = tts.stream("keep").unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    stt.sender().finish().await.unwrap();
    assert_eq!(stt.next().await, None);
    stt_peer.await.unwrap();
    tokio::time::sleep(Duration::from_secs(15)).await;
    tts.cancel();
    tts_peer.await.unwrap();
}
#[tokio::test]
async fn dropped_finish_and_read_futures_leave_admission_and_delivery_retryable() {
    let (listener, endpoint) = listen().await;
    let (accepted, ready) = oneshot::channel();
    let (release, gate) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        accepted.send(()).unwrap();
        gate.await.unwrap();
        let mut ws = accept_async(socket).await.unwrap();
        read(&mut ws).await;
        let text = read(&mut ws).await;
        assert_eq!(text["text"], "hello");
        assert_eq!(read(&mut ws).await["text_end"], true);
        tokio::time::sleep(Duration::from_millis(40)).await;
        send(&mut ws, json!({"stream_id":"x","terminated":true})).await;
        let _ = ws.next().await;
    });
    let mut c = SonioxTtsConfig::new("key");
    c.endpoint = endpoint;
    c.limits.queue_capacity = 1;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut stream = client.stream("x").unwrap();
    ready.await.unwrap();
    let tx = stream.sender();
    assert!(tokio::time::timeout(Duration::from_millis(20), tx.finish())
        .await
        .is_err());
    release.send(()).unwrap();
    tx.send_text("hello").await.unwrap();
    tx.finish().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), stream.next())
            .await
            .is_err()
    );
    assert!(matches!(
        tts_next(&mut stream).await,
        Ok(SonioxTtsEvent::Done { .. })
    ));
    client.cancel();
    peer.await.unwrap();
}
#[tokio::test]
async fn tts_stream_count_and_id_budget_are_bounded_before_network_admission() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let open = read(&mut ws).await;
        assert_eq!(open["stream_id"], "x");
        assert_eq!(read(&mut ws).await["text_end"], true);
        send(&mut ws, json!({"stream_id":"x","terminated":true})).await;
        let _ = ws.next().await;
    });
    let mut c = SonioxTtsConfig::new("key");
    c.endpoint = endpoint;
    c.max_streams = 1;
    c.max_streams_per_connection = 1;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut stream = client.stream("x").unwrap();
    assert!(matches!(
        client.stream("y"),
        Err(StreamingError::Backpressure)
    ));
    stream.sender().finish().await.unwrap();
    assert!(matches!(
        tts_next(&mut stream).await,
        Ok(SonioxTtsEvent::Done { .. })
    ));
    assert!(matches!(client.stream("y"), Err(StreamingError::TooLarge)));
    client.cancel();
    peer.await.unwrap();
}
#[tokio::test]
async fn malformed_tts_packet_cannot_admit_its_audio_or_words_but_preserves_prior_packet() {
    let (listener, endpoint) = listen().await;
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        read(&mut ws).await;
        read(&mut ws).await;
        send(
            &mut ws,
            json!({"stream_id":"x","audio":STANDARD.encode([1,0]),"timestamps":times("Hi ",0.0)}),
        )
        .await;
        send(&mut ws,json!({"stream_id":"x","audio":STANDARD.encode([9,0]),"timestamps":{"characters":["B","a","d"," "],"character_start_times_seconds":[1.0,1.1,0.2,1.3]}})).await;
        let _ = ws.next().await;
    });
    let mut c = SonioxTtsConfig::new("key");
    c.endpoint = endpoint;
    let client = SonioxTtsClient::new(c).unwrap();
    let mut stream = client.stream("x").unwrap();
    stream.sender().send_text("Hi Bad").await.unwrap();
    stream.sender().finish().await.unwrap();
    // Let both packets arrive before draining; terminal error cannot retract
    // the fully admitted prefix, or admit any part of the malformed successor.
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(
        matches!(tts_next(&mut stream).await,Ok(SonioxTtsEvent::Audio(frame)) if frame.audio==[1,0])
    );
    assert!(
        matches!(tts_next(&mut stream).await,Ok(SonioxTtsEvent::WordTimestamps{words,..}) if words.len()==1&&words[0].word=="Hi")
    );
    assert!(matches!(
        tts_next(&mut stream).await,
        Err(StreamingError::Protocol(_))
    ));
    assert_eq!(stream.next().await, None);
    client.cancel();
    peer.await.unwrap();
}
