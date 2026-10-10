//! Independent Gradium TTS wire and lifecycle regressions using literal-loopback
//! WebSockets only. These fixtures do not establish hosted synthesis quality.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gradium::*;
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    sync::{mpsc, oneshot, Notify, Semaphore},
    task::JoinHandle,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("independent Gradium TTS fixture deadline")
}

struct Peer {
    config: GradiumTtsConfig,
    outgoing: mpsc::Sender<Message>,
    incoming: mpsc::Receiver<Value>,
    task: JoinHandle<()>,
    connected: Arc<AtomicBool>,
}

// Tungstenite requires this fixed, unboxed HTTP callback error type.
#[allow(clippy::result_large_err)]
async fn peer(auto_ready: bool) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GradiumTtsConfig::new("review-key-never-live");
    config.endpoint = format!("ws://{}/api/speech/tts", listener.local_addr().unwrap());
    config.allow_loopback = true;
    config.limits.queue_capacity = 16;
    config.limits.connect_timeout = Duration::from_secs(1);
    config.limits.read_timeout = Duration::from_secs(2);
    config.limits.write_timeout = Duration::from_secs(1);
    config.limits.finish_timeout = Duration::from_secs(1);
    config.limits.close_timeout = Duration::from_millis(80);
    let (outgoing, mut outputs) = mpsc::channel::<Message>(64);
    let (inputs, incoming) = mpsc::channel(64);
    let connected = Arc::new(AtomicBool::new(false));
    let connection_observed = connected.clone();
    let task = tokio::spawn(async move {
        let (tcp, _) = bounded(listener.accept()).await.unwrap();
        connection_observed.store(true, Ordering::Release);
        let mut socket = bounded(accept_hdr_async(
            tcp,
            |request: &Request, response: Response| {
                assert_eq!(request.uri().path(), "/api/speech/tts");
                assert!(request.uri().query().is_none());
                assert_eq!(request.headers()["x-api-key"], "review-key-never-live");
                assert_eq!(request.headers()["x-api-source"], "pipecat");
                Ok(response)
            },
        ))
        .await
        .unwrap();
        loop {
            tokio::select! {
                output = outputs.recv() => match output {
                    Some(output) => if socket.send(output).await.is_err() { break; },
                    None => break,
                },
                input = socket.next() => match input {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        if auto_ready && value["type"] == "setup" {
                            let ready = json!({"type":"ready", "client_req_id":value["client_req_id"],
                                "request_id":"provider-identity-is-not-routing-identity",
                                "sample_rate":48000, "frame_size":3840});
                            if socket.send(Message::Text(ready.to_string().into())).await.is_err() { break; }
                        }
                        if inputs.send(value).await.is_err() { break; }
                    },
                    Some(Ok(Message::Ping(data))) => if socket.send(Message::Pong(data)).await.is_err() { break; },
                    Some(Ok(Message::Pong(_))) => (),
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => panic!("unexpected non-JSON client message"),
                }
            }
        }
    });
    Peer {
        config,
        outgoing,
        incoming,
        task,
        connected,
    }
}

async fn emit(peer: &Peer, value: Value) {
    bounded(peer.outgoing.send(Message::Text(value.to_string().into())))
        .await
        .unwrap();
}
async fn wire(peer: &mut Peer) -> Value {
    bounded(peer.incoming.recv())
        .await
        .expect("client wire message")
}
async fn event(stream: &mut GradiumTtsStream) -> Result<GradiumTtsEvent, GradiumError> {
    bounded(stream.next()).await.expect("stream event")
}
async fn close(peer: Peer, client: GradiumTtsClient) {
    client.cancel();
    bounded(peer.task).await.unwrap();
}
async fn request(peer: &mut Peer, client: &GradiumTtsClient, id: &str) -> GradiumTtsStream {
    let stream = client.stream(id).unwrap();
    let setup = wire(peer).await;
    assert_eq!(setup["type"], "setup");
    assert_eq!(setup["client_req_id"], id);
    stream.sender().send_text("Hello.").await.unwrap();
    stream.sender().finish().await.unwrap();
    assert_eq!(
        wire(peer).await,
        json!({"type":"text", "client_req_id":id, "text":"Hello."})
    );
    assert_eq!(
        wire(peer).await,
        json!({"type":"end_of_stream", "client_req_id":id})
    );
    stream
}
async fn audio(peer: &Peer, id: &str, bytes: &[u8]) {
    emit(
        peer,
        json!({"type":"audio", "client_req_id":id, "audio":STANDARD.encode(bytes)}),
    )
    .await;
}
async fn eos(peer: &Peer, id: &str) {
    emit(peer, json!({"type":"end_of_stream", "client_req_id":id})).await;
}

#[tokio::test]
async fn review_exact_wire_string_config_and_interleaved_request_identity() {
    let mut peer = peer(true).await;
    peer.config.settings.json_config = Some("{ \"temperature\": 0.4 }".into());
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut first = client.stream("01").unwrap();
    let mut changed = peer.config.settings.clone();
    changed.voice = "second-voice".into();
    changed.json_config = Some("{\"temperature\":0.8}".into());
    let next = client.with_settings(changed).unwrap();
    let mut second = next.stream("1").unwrap();
    first.sender().send_text("First.").await.unwrap();
    second.sender().send_text("Second.").await.unwrap();
    first.sender().finish().await.unwrap();
    second.sender().finish().await.unwrap();
    let mut messages = vec![];
    for _ in 0..6 {
        messages.push(wire(&mut peer).await);
    }
    for (id, voice, config, text) in [
        (
            "01",
            "_6Aslh2DxfmnRLmP",
            "{ \"temperature\": 0.4 }",
            "First.",
        ),
        ("1", "second-voice", "{\"temperature\":0.8}", "Second."),
    ] {
        let routed: Vec<_> = messages
            .iter()
            .filter(|value| value["client_req_id"] == id)
            .cloned()
            .collect();
        assert_eq!(
            routed,
            vec![
                json!({"type":"setup", "voice_id":voice, "output_format":"pcm", "close_ws_on_eos":false,
                "client_req_id":id, "json_config":config}),
                json!({"type":"text", "client_req_id":id, "text":text}),
                json!({"type":"end_of_stream", "client_req_id":id}),
            ]
        );
    }
    // Replies are deliberately reversed and split within a PCM16 sample.
    audio(&peer, "1", &[10, 11, 12]).await;
    audio(&peer, "01", &[20]).await;
    audio(&peer, "1", &[13]).await;
    emit(
        &peer,
        json!({"type":"text", "client_req_id":"1", "text":"Second.", "start_s":0.0}),
    )
    .await;
    eos(&peer, "1").await;
    audio(&peer, "01", &[21, 22, 23]).await;
    emit(
        &peer,
        json!({"type":"text", "client_req_id":"01", "text":"First.", "start_s":0.0}),
    )
    .await;
    // Duplicate and late retired events cannot target the still-active request.
    eos(&peer, "1").await;
    audio(&peer, "1", &[99, 99]).await;
    eos(&peer, "01").await;
    for (id, stream, expected, word) in [
        ("1", &mut second, vec![10, 11, 12, 13], "Second."),
        ("01", &mut first, vec![20, 21, 22, 23], "First."),
    ] {
        let mut pcm = vec![];
        let mut done = 0;
        let mut words = vec![];
        while let Some(result) = bounded(stream.next()).await {
            match result.unwrap() {
                GradiumTtsEvent::Audio(frame) => {
                    assert_eq!(frame.context_id.as_deref(), Some(id));
                    assert_eq!(frame.sample_rate, 48000);
                    assert_eq!(frame.num_channels, 1);
                    assert_eq!(frame.audio.len() % 2, 0);
                    pcm.extend(frame.audio);
                }
                GradiumTtsEvent::WordTimestamps {
                    context_id,
                    words: incoming,
                } => {
                    assert_eq!(context_id, id);
                    words.extend(incoming.into_iter().map(|value| value.word));
                }
                GradiumTtsEvent::Done { context_id } => {
                    assert_eq!(context_id, id);
                    done += 1;
                }
            }
        }
        assert_eq!(pcm, expected);
        assert_eq!(words, vec![word]);
        assert_eq!(done, 1);
        assert!(bounded(stream.next()).await.is_none());
    }
    assert!(
        client.stream("1").is_err(),
        "retired request identity reused"
    );
    assert!(first.sender().send_text("late").await.is_err());
    close(peer, client).await;
}

#[tokio::test]
async fn review_cancel_discards_queued_audio_and_late_events_without_harming_fresh_request() {
    let mut peer = peer(true).await;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut old = request(&mut peer, &client, "old").await;
    audio(&peer, "old", &[1, 2]).await;
    bounded(async {
        while !old.source_audio_observed() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    old.cancel();
    assert!(matches!(
        event(&mut old).await,
        Err(GradiumError::Cancelled)
    ));
    assert!(bounded(old.next()).await.is_none());
    let mut fresh = request(&mut peer, &client, "fresh").await;
    for value in [
        json!({"type":"audio", "client_req_id":"old", "audio":"malformed-old-payload"}),
        json!({"type":"text", "client_req_id":"old", "text":"stale", "start_s":-5}),
        json!({"type":"end_of_stream", "client_req_id":"old"}),
        json!({"type":"error", "client_req_id":"old", "message":"late old error"}),
    ] {
        emit(&peer, value).await;
    }
    audio(&peer, "fresh", &[3, 4]).await;
    eos(&peer, "fresh").await;
    assert!(
        matches!(event(&mut fresh).await, Ok(GradiumTtsEvent::Audio(frame)) if frame.audio == [3,4] && frame.context_id.as_deref() == Some("fresh"))
    );
    assert!(
        matches!(event(&mut fresh).await, Ok(GradiumTtsEvent::Done { context_id }) if context_id == "fresh")
    );
    assert!(bounded(fresh.next()).await.is_none());
    assert!(client.stream("old").is_err());
    close(peer, client).await;
}

#[tokio::test]
async fn review_scoped_failure_is_redacted_and_has_one_terminal_owner() {
    let mut peer = peer(true).await;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut first = request(&mut peer, &client, "failed").await;
    let mut second = request(&mut peer, &client, "healthy").await;
    emit(&peer, json!({"type":"error", "client_req_id":"failed", "message":"secret-provider-body review-key-never-live"})).await;
    let error = event(&mut first).await.unwrap_err();
    assert!(matches!(error, GradiumError::Provider));
    assert!(!format!("{error:?} {error}").contains("secret-provider-body"));
    assert!(!format!("{error:?} {error}").contains("review-key-never-live"));
    eos(&peer, "failed").await;
    audio(&peer, "failed", &[1, 2]).await;
    audio(&peer, "healthy", &[3, 4]).await;
    eos(&peer, "healthy").await;
    assert!(bounded(first.next()).await.is_none());
    assert!(
        matches!(event(&mut second).await, Ok(GradiumTtsEvent::Audio(frame)) if frame.audio == [3,4])
    );
    assert!(matches!(
        event(&mut second).await,
        Ok(GradiumTtsEvent::Done { .. })
    ));
    assert!(bounded(second.next()).await.is_none());
    close(peer, client).await;
}

#[tokio::test]
async fn review_odd_terminal_pcm_never_reports_success_or_fabricates_sample() {
    let mut peer = peer(true).await;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut stream = request(&mut peer, &client, "odd").await;
    audio(&peer, "odd", &[1]).await;
    eos(&peer, "odd").await;
    assert!(matches!(
        event(&mut stream).await,
        Err(GradiumError::PartialSample)
    ));
    assert!(bounded(stream.next()).await.is_none());
    close(peer, client).await;
}

#[tokio::test]
async fn review_timestamps_are_monotonic_per_request() {
    let mut peer = peer(true).await;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut stream = request(&mut peer, &client, "times").await;
    emit(
        &peer,
        json!({"type":"text", "client_req_id":"times", "text":"later", "start_s":0.5}),
    )
    .await;
    assert!(matches!(
        event(&mut stream).await,
        Ok(GradiumTtsEvent::WordTimestamps { .. })
    ));
    emit(
        &peer,
        json!({"type":"text", "client_req_id":"times", "text":"earlier", "start_s":0.4}),
    )
    .await;
    assert!(matches!(
        event(&mut stream).await,
        Err(GradiumError::Protocol(_))
    ));
    assert!(bounded(stream.next()).await.is_none());
    close(peer, client).await;
}

#[tokio::test]
async fn review_settings_snapshot_validation_is_atomic() {
    let mut peer = peer(true).await;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let original = client.config().settings.clone();
    let mut invalid = original.clone();
    invalid.voice.clear();
    assert!(client.with_settings(invalid).is_err());
    assert_eq!(client.config().settings, original);
    let mut stream = request(&mut peer, &client, "unchanged").await;
    audio(&peer, "unchanged", &[1, 2]).await;
    eos(&peer, "unchanged").await;
    assert!(matches!(
        event(&mut stream).await,
        Ok(GradiumTtsEvent::Audio(_))
    ));
    assert!(matches!(
        event(&mut stream).await,
        Ok(GradiumTtsEvent::Done { .. })
    ));
    assert!(bounded(stream.next()).await.is_none());
    close(peer, client).await;
}

#[tokio::test]
async fn review_malformed_routing_and_unscoped_errors_fail_every_live_request() {
    for routing in [Value::Null, json!(1), json!(true), json!("")] {
        let mut peer = peer(true).await;
        let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
        let mut first = request(&mut peer, &client, "first").await;
        let mut second = request(&mut peer, &client, "second").await;
        emit(
            &peer,
            json!({"type":"audio", "client_req_id":routing, "audio":"AQI="}),
        )
        .await;
        assert!(matches!(
            event(&mut first).await,
            Err(GradiumError::Protocol(_))
        ));
        assert!(matches!(
            event(&mut second).await,
            Err(GradiumError::Protocol(_))
        ));
        assert!(bounded(first.next()).await.is_none());
        assert!(bounded(second.next()).await.is_none());
        close(peer, client).await;
    }
    let mut peer = peer(true).await;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut first = request(&mut peer, &client, "first").await;
    let mut second = request(&mut peer, &client, "second").await;
    emit(
        &peer,
        json!({"type":"error", "message":"do-not-print-provider-body"}),
    )
    .await;
    assert!(matches!(
        event(&mut first).await,
        Err(GradiumError::Provider)
    ));
    assert!(matches!(
        event(&mut second).await,
        Err(GradiumError::Provider)
    ));
    close(peer, client).await;
}

#[tokio::test]
async fn review_ready_geometry_duplicates_and_unsolicited_eos_are_rejected() {
    for scenario in 0..4 {
        let mut peer = peer(false).await;
        let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
        let mut stream = client.stream("shape").unwrap();
        assert_eq!(wire(&mut peer).await["type"], "setup");
        match scenario {
            0 => emit(&peer, json!({"type":"ready", "client_req_id":"shape", "sample_rate":24000})).await,
            1 => emit(&peer, json!({"type":"ready", "client_req_id":"shape", "sample_rate":48000, "frame_size":0})).await,
            2 => {
                emit(&peer, json!({"type":"ready", "client_req_id":"shape", "sample_rate":48000})).await;
                emit(&peer, json!({"type":"ready", "client_req_id":"shape", "sample_rate":48000})).await;
            }
            3 => eos(&peer, "shape").await,
            _ => unreachable!(),
        }
        assert!(
            matches!(event(&mut stream).await, Err(GradiumError::Protocol(_))),
            "accepted invalid lifecycle scenario {scenario}"
        );
        assert!(bounded(stream.next()).await.is_none());
        close(peer, client).await;
    }
}

#[tokio::test]
async fn review_output_backpressure_is_local_bounded_and_retains_only_admitted_prefix() {
    let mut peer = peer(true).await;
    peer.config.limits.queue_capacity = 4;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut blocked = request(&mut peer, &client, "blocked").await;
    let mut probe = request(&mut peer, &client, "probe").await;
    for _ in 0..5 {
        audio(&peer, "blocked", &[1, 2]).await;
    }
    audio(&peer, "probe", &[3, 4]).await;
    // Receiving this frame proves the preceding saturated-request messages were
    // processed on the same socket before the blocked request is drained.
    assert!(matches!(
        event(&mut probe).await,
        Ok(GradiumTtsEvent::Audio(_))
    ));
    let mut prefix = 0;
    loop {
        match event(&mut blocked).await {
            Ok(GradiumTtsEvent::Audio(_)) => prefix += 1,
            Err(GradiumError::Backpressure) => break,
            other => panic!("unexpected saturation result {other:?}"),
        }
    }
    assert_eq!(prefix, 4);
    assert!(bounded(blocked.next()).await.is_none());
    eos(&peer, "probe").await;
    assert!(matches!(
        event(&mut probe).await,
        Ok(GradiumTtsEvent::Done { .. })
    ));
    assert!(bounded(probe.next()).await.is_none());
    close(peer, client).await;
}

#[tokio::test]
async fn review_ignored_unknown_requests_still_consume_connection_budget() {
    let mut peer = peer(true).await;
    peer.config.limits.max_session_messages = 3;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut stream = request(&mut peer, &client, "live").await;
    for _ in 0..4 {
        emit(
            &peer,
            json!({"type":"audio", "client_req_id":"unknown", "audio":"ignored-malformed-base64"}),
        )
        .await;
    }
    assert!(matches!(
        event(&mut stream).await,
        Err(GradiumError::TooLarge)
    ));
    assert!(bounded(stream.next()).await.is_none());
    close(peer, client).await;
}

#[tokio::test]
async fn review_finish_deadline_is_absolute_despite_continuing_valid_output() {
    let mut peer = peer(true).await;
    peer.config.limits.finish_timeout = Duration::from_millis(120);
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let started = tokio::time::Instant::now();
    let mut stream = request(&mut peer, &client, "deadline").await;
    let outgoing = peer.outgoing.clone();
    let producer = tokio::spawn(async move {
        for index in 0..100 {
            let message = json!({"type":"text", "client_req_id":"deadline", "text":"part", "start_s":f64::from(index) / 100.0});
            if outgoing
                .send(Message::Text(message.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    });
    loop {
        match event(&mut stream).await {
            Ok(GradiumTtsEvent::WordTimestamps { .. }) => (),
            Err(GradiumError::Timeout(_)) => break,
            other => panic!("unexpected finishing event {other:?}"),
        }
    }
    assert!(started.elapsed() < Duration::from_millis(500));
    assert!(bounded(stream.next()).await.is_none());
    producer.abort();
    let _ = producer.await;
    close(peer, client).await;
}

#[tokio::test]
async fn review_empty_ready_stream_arrays_and_consumed_done_are_terminal() {
    let mut peer = peer(false).await;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut stream = request(&mut peer, &client, "done").await;
    emit(
        &peer,
        json!({"type":"ready", "client_req_id":"done", "sample_rate":48000,
        "frame_size":3840, "audio_stream_names":[], "text_stream_names":[]}),
    )
    .await;
    audio(&peer, "done", &[1, 2]).await;
    eos(&peer, "done").await;
    assert!(matches!(
        event(&mut stream).await,
        Ok(GradiumTtsEvent::Audio(_))
    ));
    assert!(matches!(
        event(&mut stream).await,
        Ok(GradiumTtsEvent::Done { .. })
    ));
    stream.cancel();
    assert!(
        bounded(stream.next()).await.is_none(),
        "cancel after delivered Done produced a second terminal"
    );
    close(peer, client).await;
}

async fn pipeline(config: GradiumTtsConfig) -> (Arc<GradiumTtsProcessor>, PipelineWorker) {
    let processor = Arc::new(GradiumTtsProcessor::new(
        GradiumTtsClient::new(config).unwrap(),
    ));
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig {
            start: StartFrame {
                audio_out_sample_rate: 48000,
                ..Default::default()
            },
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .unwrap();
    (processor, worker)
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    bounded(
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream),
    )
    .await
    .unwrap();
}
async fn through(worker: &mut PipelineWorker, id: u64) -> Vec<DirectedFrame> {
    let mut frames = vec![];
    loop {
        let frame = bounded(worker.recv()).await.expect("pipeline output");
        let complete = frame.frame.id == id;
        frames.push(frame);
        if complete {
            return frames;
        }
    }
}
async fn response(worker: &PipelineWorker, text: &str) -> u64 {
    send(worker, FrameData::LlmResponseStart).await;
    send(worker, FrameData::LlmText(TextFrame::llm(text))).await;
    let end = Frame::new(FrameData::LlmResponseEnd);
    let id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    id
}
async fn provider_request(peer: &mut Peer, expected: &str) -> String {
    let setup = wire(peer).await;
    assert_eq!(setup["type"], "setup");
    let id = setup["client_req_id"].as_str().unwrap().to_owned();
    assert_eq!(
        wire(peer).await,
        json!({"type":"text", "client_req_id":id, "text":expected})
    );
    assert_eq!(
        wire(peer).await,
        json!({"type":"end_of_stream", "client_req_id":id})
    );
    id
}
async fn end_pipeline(peer: Peer, worker: PipelineWorker) -> Vec<DirectedFrame> {
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(
        report.errors.is_empty(),
        "pipeline errors: {:?}",
        report.errors
    );
    bounded(peer.task).await.unwrap();
    frames
}

#[tokio::test]
async fn review_processor_preserves_response_order_and_generation_is_not_playback() {
    let mut peer = peer(true).await;
    let (_, mut worker) = pipeline(peer.config.clone()).await;
    let end = response(&worker, "Hello.").await;
    let id = provider_request(&mut peer, "Hello.").await;
    emit(
        &peer,
        json!({"type":"text", "client_req_id":id, "text":"Hello.", "start_s":0.0}),
    )
    .await;
    audio(&peer, &id, &[1, 2, 3, 4]).await;
    eos(&peer, &id).await;
    let mut frames = through(&mut worker, end).await;
    frames.extend(end_pipeline(peer, worker).await);
    let order: Vec<_> = frames
        .iter()
        .filter_map(|frame| match frame.frame.data {
            FrameData::LlmResponseStart => Some("response-start"),
            FrameData::TtsStarted(_) => Some("tts-start"),
            FrameData::TtsAudio(_) => Some("audio"),
            FrameData::TtsStopped(_) => Some("tts-stop"),
            FrameData::LlmResponseEnd => Some("response-end"),
            FrameData::End(_) => Some("end"),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        [
            "response-start",
            "tts-start",
            "audio",
            "tts-stop",
            "response-end",
            "end"
        ]
    );
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::BotStartedSpeaking | FrameData::BotStoppedSpeaking
    )));
    assert_eq!(
        frames.iter().filter(|frame| frame.frame.id == end).count(),
        1
    );
    assert!(!frames
        .iter()
        .find(|frame| frame.frame.id == end)
        .unwrap()
        .frame
        .metadata
        .contains_key("failed"));
}

#[tokio::test]
async fn review_processor_empty_pcm_cannot_emit_successful_synthesis_terminal() {
    let mut peer = peer(true).await;
    let (_, mut worker) = pipeline(peer.config.clone()).await;
    let end = response(&worker, "Empty.").await;
    let id = provider_request(&mut peer, "Empty.").await;
    eos(&peer, &id).await;
    let frames = through(&mut worker, end).await;
    assert_eq!(
        frames.last().unwrap().frame.metadata.get("failed"),
        Some(&json!(true))
    );
    assert!(!frames.iter().any(|frame| matches!(&frame.frame.data,
        FrameData::TtsAlignmentDiagnostic(diagnostic) if diagnostic.synthesis_outcome == TtsSynthesisOutcome::Succeeded)),
        "zero-audio EOS produced successful synthesis diagnostic");
    for frame in &frames {
        if matches!(frame.frame.data, FrameData::TtsStopped(_)) {
            assert_eq!(frame.frame.metadata.get("failed"), Some(&json!(true)));
        }
    }
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::TtsAudio(_) | FrameData::BotStartedSpeaking
    )));
    worker.end().await.unwrap();
    let _ = bounded(worker.collect()).await.unwrap();
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_processor_rejected_and_noop_settings_do_not_rotate_pending_sentence() {
    let mut peer = peer(true).await;
    let (processor, mut worker) = pipeline(peer.config.clone()).await;
    let before = processor.config_snapshot().unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello"))).await;
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [("voice".into(), json!(""))].into(),
            ..Default::default()
        })),
    )
    .await;
    loop {
        let frame = bounded(worker.recv()).await.unwrap();
        if let FrameData::Error(error) = frame.frame.data {
            assert_eq!(error.category, ErrorCategory::InvalidRequest);
            break;
        }
    }
    assert_eq!(processor.config_snapshot().unwrap(), before);
    assert!(worker.processor_health(0).unwrap().is_usable());
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [("voice".into(), json!(before.settings.voice))].into(),
            ..Default::default()
        })),
    )
    .await;
    send(&worker, FrameData::LlmText(TextFrame::llm("."))).await;
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    let id = provider_request(&mut peer, "Hello.").await;
    audio(&peer, &id, &[1, 2]).await;
    eos(&peer, &id).await;
    let frames = through(&mut worker, end_id).await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    assert_eq!(processor.config_snapshot().unwrap(), before);
    assert!(
        tokio::time::timeout(Duration::from_millis(40), peer.incoming.recv())
            .await
            .is_err()
    );
    worker.end().await.unwrap();
    let _ = bounded(worker.collect()).await.unwrap();
    bounded(peer.task).await.unwrap();
}

struct HeldOldAudio {
    entered: Notify,
    release: Semaphore,
}
#[async_trait]
impl FrameProcessor for HeldOldAudio {
    fn name(&self) -> &str {
        "ReviewHeldOldGradiumAudio"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::TtsAudio(audio) if audio.audio == [1, 2]) {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_interruption_invalidates_blocked_audio_and_late_old_request_callbacks() {
    let mut peer = peer(true).await;
    let processor = Arc::new(GradiumTtsProcessor::new(
        GradiumTtsClient::new(peer.config.clone()).unwrap(),
    ));
    let held = Arc::new(HeldOldAudio {
        entered: Notify::new(),
        release: Semaphore::new(0),
    });
    let mut worker = Pipeline::new(vec![processor, held.clone()])
        .start(PipelineConfig {
            start: StartFrame {
                audio_out_sample_rate: 48000,
                ..Default::default()
            },
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await
        .unwrap();
    let old_end = response(&worker, "Old.").await;
    let old = provider_request(&mut peer, "Old.").await;
    audio(&peer, &old, &[1, 2]).await;
    bounded(held.entered.notified()).await;
    let interruption = Frame::new(FrameData::Interruption);
    let interruption_id = interruption.id;
    worker
        .sender()
        .send(interruption, Direction::Downstream)
        .await
        .unwrap();
    let before = through(&mut worker, interruption_id).await;
    assert!(!before
        .iter()
        .any(|frame| matches!(frame.frame.data, FrameData::TtsAudio(_))));
    held.release.add_permits(1);
    emit(
        &peer,
        json!({"type":"text", "client_req_id":old, "text":"stale", "start_s":0.0}),
    )
    .await;
    audio(&peer, &old, &[5, 6]).await;
    eos(&peer, &old).await;
    let fresh_end = response(&worker, "Fresh.").await;
    let fresh = provider_request(&mut peer, "Fresh.").await;
    assert_ne!(fresh, old);
    audio(&peer, &fresh, &[3, 4]).await;
    eos(&peer, &fresh).await;
    let mut after = through(&mut worker, fresh_end).await;
    after.extend(end_pipeline(peer, worker).await);
    let audio: Vec<_> = after
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::TtsAudio(audio) => Some((audio.context_id.clone(), audio.audio.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(audio, [(Some(fresh.clone()), vec![3, 4])]);
    assert!(!after.iter().any(|frame| frame.frame.id == old_end));
    assert!(!after.iter().any(|frame| matches!(&frame.frame.data,
        FrameData::TtsStopped(stop) if stop.context_id.as_deref() == Some(old.as_str()))));
}

#[tokio::test]
async fn review_health_disable_restore_cannot_revive_old_generation() {
    let mut peer = peer(true).await;
    let (_, mut worker) = pipeline(peer.config.clone()).await;
    let old_end = response(&worker, "Old health.").await;
    let old = provider_request(&mut peer, "Old health.").await;
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    audio(&peer, &old, &[1, 2]).await;
    eos(&peer, &old).await;
    let old_frames = through(&mut worker, old_end).await;
    assert_eq!(
        old_frames.last().unwrap().frame.metadata.get("failed"),
        Some(&json!(true))
    );
    assert!(!old_frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::TtsAudio(_) | FrameData::TtsText(_)
    )));
    let fresh_end = response(&worker, "Fresh health.").await;
    let fresh = provider_request(&mut peer, "Fresh health.").await;
    assert_ne!(fresh, old);
    audio(&peer, &fresh, &[3, 4]).await;
    eos(&peer, &fresh).await;
    let frames = through(&mut worker, fresh_end).await;
    assert!(!frames.last().unwrap().frame.metadata.contains_key("failed"));
    assert!(frames.iter().any(
        |frame| matches!(&frame.frame.data, FrameData::TtsAudio(audio) if audio.audio == [3,4])
    ));
    assert!(health.is_usable());
    end_pipeline(peer, worker).await;
}

#[tokio::test]
async fn review_processor_cancel_and_drop_close_owned_socket_without_eos_success() {
    for drop_worker in [false, true] {
        let mut peer = peer(true).await;
        let (_, worker) = pipeline(peer.config.clone()).await;
        response(&worker, "In flight.").await;
        provider_request(&mut peer, "In flight.").await;
        if drop_worker {
            drop(worker);
        } else {
            worker.cancel("independent-review-cancel").await.unwrap();
            let (frames, _) = bounded(worker.collect()).await.unwrap();
            assert!(!frames
                .iter()
                .any(|frame| matches!(frame.frame.data, FrameData::TtsAudio(_))));
        }
        bounded(peer.task).await.unwrap();
    }
}

#[tokio::test]
async fn review_finish_bypasses_full_input_queue_and_preserves_admitted_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GradiumTtsConfig::new("synthetic");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    config.allow_loopback = true;
    config.limits.queue_capacity = 2;
    config.limits.close_timeout = Duration::from_millis(80);
    let (connected, accepted) = oneshot::channel();
    let (release, upgrade) = oneshot::channel();
    let (transcript, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        connected.send(()).unwrap();
        upgrade.await.unwrap();
        let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
        let mut wire = vec![];
        for _ in 0..3 {
            let message = bounded(socket.next()).await.unwrap().unwrap();
            wire.push(serde_json::from_str::<Value>(message.to_text().unwrap()).unwrap());
        }
        transcript.send(wire).unwrap();
        socket
            .send(Message::Text(
                json!({"type":"end_of_stream", "client_req_id":"full-input"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        while let Some(Ok(message)) = socket.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });
    let client = GradiumTtsClient::new(config).unwrap();
    let mut stream = client.stream("full-input").unwrap();
    bounded(accepted).await.unwrap();
    stream.sender().try_send_text("admitted").unwrap();
    assert_eq!(
        stream.sender().try_send_text("rejected"),
        Err(GradiumError::Backpressure)
    );
    stream.sender().try_finish().unwrap();
    assert!(stream.sender().try_send_text("after finish").is_err());
    release.send(()).unwrap();
    let wire = bounded(received).await.unwrap();
    assert_eq!(wire[0]["type"], "setup");
    assert_eq!(
        wire[1],
        json!({"type":"text", "client_req_id":"full-input", "text":"admitted"})
    );
    assert_eq!(
        wire[2],
        json!({"type":"end_of_stream", "client_req_id":"full-input"})
    );
    assert!(matches!(
        event(&mut stream).await,
        Ok(GradiumTtsEvent::Done { .. })
    ));
    assert!(bounded(stream.next()).await.is_none());
    bounded(client.shutdown()).await;
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn review_finish_deadline_interrupts_pending_http_upgrade_and_closes_tcp_owner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = GradiumTtsConfig::new("synthetic");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    config.allow_loopback = true;
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.finish_timeout = Duration::from_millis(100);
    config.limits.close_timeout = Duration::from_secs(1);
    let (connected, accepted) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        connected.send(()).unwrap();
        let mut bytes = [0; 2048];
        loop {
            match bounded(tcp.read(&mut bytes)).await {
                Ok(0) | Err(_) => break,
                Ok(_) => (),
            }
        }
    });
    let client = GradiumTtsClient::new(config).unwrap();
    let mut stream = client.stream("pending-upgrade").unwrap();
    bounded(accepted).await.unwrap();
    let started = tokio::time::Instant::now();
    stream.sender().finish().await.unwrap();
    assert!(matches!(
        event(&mut stream).await,
        Err(GradiumError::Timeout(_))
    ));
    assert!(
        started.elapsed() < Duration::from_millis(500),
        "finish waited for connect/close deadline"
    );
    assert!(bounded(stream.next()).await.is_none());
    bounded(server).await.unwrap();
    bounded(client.shutdown()).await;
}

#[tokio::test]
async fn review_active_and_lifetime_context_bounds_include_retired_identities() {
    let mut peer = peer(true).await;
    peer.config.max_streams = 1;
    peer.config.max_streams_per_connection = 2;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    for id in ["first", "second"] {
        let mut stream = request(&mut peer, &client, id).await;
        assert!(matches!(
            client.stream("extra"),
            Err(GradiumError::Backpressure)
        ));
        eos(&peer, id).await;
        assert!(matches!(
            event(&mut stream).await,
            Ok(GradiumTtsEvent::Done { .. })
        ));
        assert!(bounded(stream.next()).await.is_none());
        assert!(client.stream(id).is_err(), "retired identity was reused");
    }
    assert!(matches!(
        client.stream("fresh-third"),
        Err(GradiumError::TooLarge)
    ));
    close(peer, client).await;
}

#[tokio::test]
async fn review_retained_pcm_byte_budget_applies_before_queue_count_limit() {
    let mut peer = peer(true).await;
    peer.config.limits.queue_capacity = 4;
    peer.config.limits.max_audio_chunk_bytes = 2;
    peer.config.limits.max_buffered_audio_bytes = 2;
    let client = GradiumTtsClient::new(peer.config.clone()).unwrap();
    let mut blocked = request(&mut peer, &client, "byte-bound").await;
    let mut probe = request(&mut peer, &client, "probe").await;
    audio(&peer, "byte-bound", &[1, 2]).await;
    audio(&peer, "byte-bound", &[3, 4]).await;
    audio(&peer, "probe", &[5, 6]).await;
    assert!(matches!(
        event(&mut probe).await,
        Ok(GradiumTtsEvent::Audio(_))
    ));
    assert!(
        matches!(event(&mut blocked).await, Ok(GradiumTtsEvent::Audio(audio)) if audio.audio == [1,2])
    );
    assert!(matches!(
        event(&mut blocked).await,
        Err(GradiumError::Backpressure)
    ));
    assert!(bounded(blocked.next()).await.is_none());
    eos(&peer, "probe").await;
    assert!(matches!(
        event(&mut probe).await,
        Ok(GradiumTtsEvent::Done { .. })
    ));
    close(peer, client).await;
}

#[tokio::test]
async fn review_explicit_stop_drains_provider_audio_before_pipeline_terminal() {
    let mut peer = peer(true).await;
    let (_, worker) = pipeline(peer.config.clone()).await;
    let end = response(&worker, "Stop drain.").await;
    let id = provider_request(&mut peer, "Stop drain.").await;
    send(&worker, FrameData::Stop).await;
    audio(&peer, &id, &[1, 2]).await;
    eos(&peer, &id).await;
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty());
    let order: Vec<_> = frames
        .iter()
        .filter_map(|frame| match frame.frame.data {
            FrameData::TtsStarted(_) => Some("tts-start"),
            FrameData::TtsAudio(_) => Some("audio"),
            FrameData::TtsStopped(_) => Some("tts-stop"),
            FrameData::LlmResponseEnd => Some("response-end"),
            FrameData::Stop => Some("stop"),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        ["tts-start", "audio", "tts-stop", "response-end", "stop"]
    );
    assert_eq!(
        frames.iter().filter(|frame| frame.frame.id == end).count(),
        1
    );
    bounded(peer.task).await.unwrap();
}

async fn no_provider(peer: &Peer) {
    tokio::time::sleep(Duration::from_millis(40)).await;
    assert!(
        !peer.connected.load(Ordering::Acquire),
        "text-free response opened provider TCP connection"
    );
    assert!(
        !peer.task.is_finished(),
        "no-connection fixture listener ended unexpectedly"
    );
}

#[tokio::test]
async fn review_empty_and_tool_only_turns_open_no_provider_then_real_text_opens_one_context() {
    let mut peer = peer(true).await;
    let (_, mut worker) = pipeline(peer.config.clone()).await;
    for tool_only in [false, true] {
        send(&worker, FrameData::LlmResponseStart).await;
        if tool_only {
            send(&worker, FrameData::LlmThoughtStart).await;
            send(
                &worker,
                FrameData::LlmThoughtText(TextFrame::llm("private tool planning")),
            )
            .await;
            send(&worker, FrameData::LlmThoughtEnd).await;
            send(
                &worker,
                FrameData::FunctionCallsStarted(vec![FunctionCall {
                    function_name: "synthetic_lookup".into(),
                    tool_call_id: "synthetic-only".into(),
                    arguments: json!({}),
                }]),
            )
            .await;
        }
        let end = Frame::new(FrameData::LlmResponseEnd);
        let end_id = end.id;
        worker
            .sender()
            .send(end, Direction::Downstream)
            .await
            .unwrap();
        let frames = through(&mut worker, end_id).await;
        assert!(!frames.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::TtsStarted(_)
                | FrameData::TtsAudio(_)
                | FrameData::TtsText(_)
                | FrameData::TtsStopped(_)
                | FrameData::Error(_)
        )));
        assert!(!frames.last().unwrap().frame.metadata.contains_key("failed"));
        no_provider(&peer).await;
    }
    let end = response(&worker, "Real speech.").await;
    let id = provider_request(&mut peer, "Real speech.").await;
    audio(&peer, &id, &[1, 2]).await;
    eos(&peer, &id).await;
    let frames = through(&mut worker, end).await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.incoming.recv())
            .await
            .is_err()
    );
    end_pipeline(peer, worker).await;
}

#[tokio::test]
async fn review_early_end_stop_cancel_and_drop_before_text_open_no_provider() {
    for mode in 0..4 {
        let peer = peer(true).await;
        let (_, mut worker) = pipeline(peer.config.clone()).await;
        send(&worker, FrameData::LlmResponseStart).await;
        let marker = Frame::new(FrameData::Text(TextFrame {
            text: "not for synthesis".into(),
            skip_tts: Some(true),
            ..Default::default()
        }));
        let marker_id = marker.id;
        worker
            .sender()
            .send(marker, Direction::Downstream)
            .await
            .unwrap();
        through(&mut worker, marker_id).await;
        let frames = match mode {
            0 => {
                worker.end().await.unwrap();
                bounded(worker.collect()).await.unwrap().0
            }
            1 => {
                send(&worker, FrameData::Stop).await;
                bounded(worker.collect()).await.unwrap().0
            }
            2 => {
                worker.cancel("before-text").await.unwrap();
                bounded(worker.collect()).await.unwrap().0
            }
            3 => {
                drop(worker);
                vec![]
            }
            _ => unreachable!(),
        };
        assert!(!frames.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::TtsStarted(_) | FrameData::TtsAudio(_) | FrameData::TtsStopped(_)
        )));
        no_provider(&peer).await;
        peer.task.abort();
        assert!(bounded(peer.task).await.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn review_interrupted_pending_sentence_opens_no_provider_and_is_not_replayed() {
    let mut peer = peer(true).await;
    let (_, mut worker) = pipeline(peer.config.clone()).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("unfinished"))).await;
    let marker = Frame::new(FrameData::Text(TextFrame {
        text: "aggregation barrier".into(),
        skip_tts: Some(true),
        ..Default::default()
    }));
    let marker_id = marker.id;
    worker
        .sender()
        .send(marker, Direction::Downstream)
        .await
        .unwrap();
    through(&mut worker, marker_id).await;
    no_provider(&peer).await;
    let interruption = Frame::new(FrameData::Interruption);
    let interrupted = interruption.id;
    worker
        .sender()
        .send(interruption, Direction::Downstream)
        .await
        .unwrap();
    through(&mut worker, interrupted).await;
    no_provider(&peer).await;
    let end = response(&worker, "Fresh.").await;
    let id = provider_request(&mut peer, "Fresh.").await;
    audio(&peer, &id, &[1, 2]).await;
    eos(&peer, &id).await;
    let frames = through(&mut worker, end).await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    end_pipeline(peer, worker).await;
}

#[tokio::test]
async fn review_valid_settings_before_first_text_and_during_active_context_have_distinct_snapshots()
{
    let mut peer = peer(true).await;
    let (_, mut worker) = pipeline(peer.config.clone()).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [("voice".into(), json!("before-first-text"))].into(),
            ..Default::default()
        })),
    )
    .await;
    send(
        &worker,
        FrameData::TtsTextInput(TtsTextInputFrame::new("First.")),
    )
    .await;
    let first = wire(&mut peer).await;
    assert_eq!(first["voice_id"], "before-first-text");
    let first_id = first["client_req_id"].as_str().unwrap().to_owned();
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"text", "client_req_id":first_id, "text":"First."})
    );
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [("voice".into(), json!("after-active-text"))].into(),
            ..Default::default()
        })),
    )
    .await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Second."))).await;
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"end_of_stream", "client_req_id":first_id})
    );
    audio(&peer, &first_id, &[1, 2]).await;
    eos(&peer, &first_id).await;
    let second = wire(&mut peer).await;
    assert_eq!(second["voice_id"], "after-active-text");
    let second_id = second["client_req_id"].as_str().unwrap().to_owned();
    assert_ne!(second_id, first_id);
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"text", "client_req_id":second_id, "text":"Second."})
    );
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"end_of_stream", "client_req_id":second_id})
    );
    audio(&peer, &second_id, &[3, 4]).await;
    eos(&peer, &second_id).await;
    let frames = through(&mut worker, end_id).await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.frame.data, FrameData::TtsStarted(_)))
            .count(),
        2
    );
    assert!(!frames.last().unwrap().frame.metadata.contains_key("failed"));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), peer.incoming.recv())
            .await
            .is_err()
    );
    end_pipeline(peer, worker).await;
}

#[tokio::test]
async fn review_valid_update_flushes_pending_text_with_its_original_admitted_snapshot() {
    let mut peer = peer(true).await;
    let (_, mut worker) = pipeline(peer.config.clone()).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("pending"))).await;
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [("voice".into(), json!("new-snapshot"))].into(),
            ..Default::default()
        })),
    )
    .await;
    let first = wire(&mut peer).await;
    assert_eq!(first["voice_id"], "_6Aslh2DxfmnRLmP");
    let first_id = first["client_req_id"].as_str().unwrap().to_owned();
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"text", "client_req_id":first_id, "text":"pending"})
    );
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"end_of_stream", "client_req_id":first_id})
    );
    audio(&peer, &first_id, &[1, 2]).await;
    eos(&peer, &first_id).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("New."))).await;
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    let second = wire(&mut peer).await;
    assert_eq!(second["voice_id"], "new-snapshot");
    let second_id = second["client_req_id"].as_str().unwrap().to_owned();
    assert_ne!(first_id, second_id);
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"text", "client_req_id":second_id, "text":"New."})
    );
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"end_of_stream", "client_req_id":second_id})
    );
    audio(&peer, &second_id, &[3, 4]).await;
    eos(&peer, &second_id).await;
    let frames = through(&mut worker, end_id).await;
    assert!(!frames.last().unwrap().frame.metadata.contains_key("failed"));
    end_pipeline(peer, worker).await;
}
