//! Public frame adapter contract over a real localhost Realtime WebSocket.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_realtime::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

struct Peer {
    events: mpsc::UnboundedSender<Value>,
    commands: mpsc::UnboundedReceiver<Value>,
    task: JoinHandle<()>,
}
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite fixes the handshake callback error type.
    async fn new(ready: bool, user_id: &str) -> (Self, Arc<OpenAiRealtimeProcessor>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("audio-family-loopback-key");
        config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        // Keep all production queue, byte and deadline capacities unchanged.
        let (events, mut event_rx) = mpsc::unbounded_channel::<Value>();
        let (command_tx, commands) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_hdr_async(
                tcp,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(
                        request.headers()["authorization"],
                        "Bearer audio-family-loopback-key"
                    );
                    assert_eq!(request.uri().query(), Some("model=gpt-realtime-2.1"));
                    assert!(!request.headers().contains_key("openai-beta"));
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let initial = ws.next().await.unwrap().unwrap();
            let initial: Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
            assert_eq!(initial["type"], "session.update");
            assert_eq!(
                initial["session"]["audio"]["input"]["format"]["rate"],
                24_000
            );
            if ready {
                ws.send(Message::Text(Self::ready_event().to_string().into()))
                    .await
                    .unwrap();
            }
            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        let Some(mut event) = event else { break };
                        event["event_id"] = json!("audio-family-event");
                        if ws.send(Message::Text(event.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    message = ws.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            command_tx.send(serde_json::from_str(&text).unwrap()).unwrap();
                        }
                        Some(Ok(Message::Close(_))) | None => {
                            let _ = ws.close(None).await;
                            break;
                        }
                        Some(Err(error)) => panic!("unexpected localhost socket error: {error}"),
                        _ => {}
                    }
                }
            }
        });
        let processor = Arc::new(
            OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap())
                .unwrap()
                .with_user_id(user_id),
        );
        (
            Self {
                events,
                commands,
                task,
            },
            processor,
        )
    }
    fn ready_event() -> Value {
        json!({"event_id":"audio-family-ready","type":"session.updated","session":{"id":"audio-family-session","type":"realtime"}})
    }
    fn event(&self, event: Value) {
        self.events.send(event).unwrap();
    }
    async fn command(&mut self) -> Value {
        timeout(Duration::from_secs(2), self.commands.recv())
            .await
            .expect("expected Realtime command before deadline")
            .expect("Realtime peer closed before command")
    }
    async fn quiet(&mut self) {
        match timeout(Duration::from_millis(80), self.commands.recv()).await {
            Err(_) => {}
            Ok(value) => panic!("unexpected Realtime command or closure: {value:?}"),
        }
    }
    async fn closed(self) -> Vec<Value> {
        timeout(Duration::from_secs(3), self.task)
            .await
            .expect("Realtime socket must close")
            .unwrap();
        let mut commands = self.commands;
        let mut remaining = Vec::new();
        while let Ok(command) = commands.try_recv() {
            remaining.push(command);
        }
        remaining
    }
}
fn audio(user: Option<&str>, bytes: Vec<u8>) -> Frame {
    frame_audio(user, AudioRawFrame::new(bytes, 24_000, 1).unwrap())
}
fn frame_audio(user: Option<&str>, audio: AudioRawFrame) -> Frame {
    Frame::new(match user {
        None => FrameData::InputAudio(audio),
        Some(user) => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: user.into(),
        }),
    })
}
fn is_audio(data: &FrameData) -> bool {
    matches!(data, FrameData::InputAudio(_) | FrameData::UserAudio(_))
}
async fn start(processor: Arc<OpenAiRealtimeProcessor>) -> PipelineWorker {
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24_000;
    config.start.audio_out_sample_rate = 24_000;
    Pipeline::new(vec![processor]).start(config).await.unwrap()
}
async fn send(worker: &PipelineWorker, frame: Frame, direction: Direction) {
    worker.sender().send(frame, direction).await.unwrap();
}
async fn receive(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&DirectedFrame) -> bool,
) -> DirectedFrame {
    timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker
                .recv()
                .await
                .expect("pipeline closed before expected frame");
            if wanted(&frame) {
                return frame;
            }
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("unexpected pipeline error: {error:?}");
            }
        }
    })
    .await
    .expect("expected pipeline frame before deadline")
}
async fn error(worker: &mut PipelineWorker, expected: &str) -> ErrorFrame {
    let frame = receive(worker, |frame| {
        matches!(frame.frame.data, FrameData::Error(_))
    })
    .await;
    assert_eq!(frame.direction, Direction::Upstream);
    let FrameData::Error(error) = frame.frame.data else {
        unreachable!()
    };
    assert!(error.error.contains(expected), "{error:?}");
    assert!(
        !error.fatal,
        "existing processor error must remain nonfatal"
    );
    error
}
async fn end(worker: PipelineWorker) -> WorkerReport {
    worker.end().await.unwrap();
    timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap()
        .1
}
fn append_pcm(command: &Value) -> Vec<u8> {
    assert_eq!(command["type"], "input_audio_buffer.append");
    assert!(command.get("user_id").is_none());
    STANDARD.decode(command["audio"].as_str().unwrap()).unwrap()
}

#[tokio::test]
async fn downstream_input_and_user_audio_share_pcm_wire_and_preserve_frame_identity() {
    let (peer, processor) = Peer::new(true, "").await;
    let mut worker = start(processor.clone()).await;
    for user in [None, Some("captured-speaker")] {
        let mut frame = audio(user, vec![1, 2, 3, 4]);
        frame.name = "application-pcm".into();
        frame.set_pipeline_pts(12345);
        frame.transport_source = Some("microphone".into());
        frame.transport_destination = Some("route".into());
        frame
            .metadata
            .insert("application".into(), json!({"sequence":7}));
        let tag = LocalFrameTag::new();
        tag.mark(&mut frame).unwrap();
        let expected = frame.clone();
        send(&worker, frame, Direction::Downstream).await;
        let output = receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
        assert_eq!(output.direction, Direction::Downstream);
        assert_eq!(output.frame.id, expected.id);
        assert_eq!(output.frame.name, expected.name);
        assert_eq!(output.frame.pts, expected.pts);
        assert_eq!(output.frame.data, expected.data);
        assert_eq!(output.frame.metadata, expected.metadata);
        assert_eq!(output.frame.transport_source, expected.transport_source);
        assert_eq!(
            output.frame.transport_destination,
            expected.transport_destination
        );
        assert!(tag.contains(&output.frame));
    }
    // This independent public command is an ordered socket barrier even when
    // an input class was accidentally forwarded without appending its PCM.
    processor
        .sender()
        .unwrap()
        .send(Action::CommitAudio)
        .await
        .unwrap();
    assert!(end(worker).await.errors.is_empty());
    let commands = peer.closed().await;
    assert_eq!(
        commands.last().unwrap()["type"],
        "input_audio_buffer.commit"
    );
    let appends: Vec<_> = commands
        .iter()
        .filter(|command| command["type"] == "input_audio_buffer.append")
        .map(append_pcm)
        .collect();
    assert_eq!(
        appends,
        vec![vec![1, 2, 3, 4], vec![1, 2, 3, 4]],
        "both downstream audio classes must reach the Realtime wire"
    );
    assert_eq!(commands.len(), 3);
}

#[tokio::test]
async fn upstream_audio_family_is_forwarded_without_realtime_append() {
    let (mut peer, processor) = Peer::new(true, "").await;
    let mut worker = start(processor).await;
    for user in [None, Some("captured-speaker")] {
        let frame = audio(user, vec![1, 2]);
        let expected = frame.clone();
        send(&worker, frame, Direction::Upstream).await;
        let output = receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
        assert_eq!(output.direction, Direction::Upstream);
        assert_eq!(output.frame.id, expected.id);
        assert_eq!(output.frame.data, expected.data);
    }
    peer.quiet().await;
    assert!(end(worker).await.errors.is_empty());
    assert!(peer.closed().await.is_empty());
}

#[tokio::test]
async fn output_and_tts_audio_are_forwarded_without_input_append() {
    let (mut peer, processor) = Peer::new(true, "").await;
    let mut worker = start(processor).await;
    let pcm = AudioRawFrame::new(vec![1, 2], 24_000, 1).unwrap();
    for data in [
        FrameData::OutputAudio(pcm.clone()),
        FrameData::TtsAudio(pcm),
    ] {
        let frame = Frame::new(data);
        let expected = frame.clone();
        send(&worker, frame, Direction::Downstream).await;
        let output = receive(&mut worker, |frame| {
            matches!(
                frame.frame.data,
                FrameData::OutputAudio(_) | FrameData::TtsAudio(_)
            )
        })
        .await;
        assert_eq!(output.direction, Direction::Downstream);
        assert_eq!(output.frame.id, expected.id);
        assert_eq!(output.frame.data, expected.data);
    }
    peer.quiet().await;
    assert!(end(worker).await.errors.is_empty());
    assert!(peer.closed().await.is_empty());
}

#[tokio::test]
async fn both_audio_classes_reject_invalid_pcm_without_wire_and_recover() {
    let (mut peer, processor) = Peer::new(true, "").await;
    let mut worker = start(processor).await;
    for user in [None, Some("captured-speaker")] {
        for (bytes, rate, channels, message) in [
            (vec![1], 24_000, 1, "invalid realtime PCM"),
            (vec![1, 2], 0, 1, "invalid realtime PCM"),
            (vec![1, 2], 24_000, 0, "invalid realtime PCM"),
            (
                vec![1, 2],
                16_000,
                1,
                "realtime requires mono PCM16 at 24000 Hz",
            ),
            (
                vec![1, 2, 3, 4],
                24_000,
                2,
                "realtime requires mono PCM16 at 24000 Hz",
            ),
        ] {
            let invalid = frame_audio(
                user,
                AudioRawFrame {
                    audio: bytes,
                    sample_rate: rate,
                    num_channels: channels,
                    context_id: None,
                },
            );
            send(&worker, invalid, Direction::Downstream).await;
            error(&mut worker, message).await;
            peer.quiet().await;
        }
        send(&worker, audio(user, vec![5, 6]), Direction::Downstream).await;
        assert_eq!(append_pcm(&peer.command().await), vec![5, 6]);
        receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
    }
    let report = end(worker).await;
    assert_eq!(report.errors.len(), 10);
    assert_eq!(report.state, WorkerState::Ended);
    assert!(peer.closed().await.is_empty());
}

#[tokio::test]
async fn both_audio_classes_keep_default_audio_chunk_budget() {
    let (mut peer, processor) = Peer::new(true, "").await;
    let mut worker = start(processor).await;
    let limit = RealtimeLimits::default().max_audio_chunk_bytes;
    for user in [None, Some("captured-speaker")] {
        send(
            &worker,
            audio(user, vec![0; limit + 2]),
            Direction::Downstream,
        )
        .await;
        let failure = error(&mut worker, "configured limit").await;
        assert_eq!(failure.category, ErrorCategory::InvalidRequest);
        peer.quiet().await;
        send(&worker, audio(user, vec![0; limit]), Direction::Downstream).await;
        assert_eq!(append_pcm(&peer.command().await), vec![0; limit]);
        receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
    }
    assert_eq!(end(worker).await.errors.len(), 2);
    assert!(peer.closed().await.is_empty());
}

#[tokio::test]
async fn empty_audio_keeps_the_existing_async_actor_error() {
    for user in [None, Some("captured-speaker")] {
        let (peer, processor) = Peer::new(true, "").await;
        let mut worker = start(processor).await;
        send(&worker, audio(user, vec![]), Direction::Downstream).await;
        // The public frame is valid; the queued actor rejects an empty append.
        error(&mut worker, "empty audio chunk").await;
        assert!(peer.closed().await.is_empty());
        worker.cancel("empty-input cleanup").await.unwrap();
        let (_, report) = timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report
            .errors
            .iter()
            .any(|error| error.error.contains("empty audio chunk")));
    }
}

#[tokio::test]
async fn both_audio_classes_obey_readiness_then_append_in_order() {
    let (mut peer, processor) = Peer::new(false, "").await;
    let mut worker = start(processor).await;
    for user in [None, Some("captured-speaker")] {
        send(&worker, audio(user, vec![1, 2]), Direction::Downstream).await;
        receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
    }
    peer.quiet().await;
    peer.event(Peer::ready_event());
    for _ in 0..2 {
        assert_eq!(append_pcm(&peer.command().await), vec![1, 2]);
    }
    assert!(end(worker).await.errors.is_empty());
    assert!(peer.closed().await.is_empty());
}

#[tokio::test]
async fn full_default_audio_queue_still_allows_cancel_before_readiness() {
    for user in [None, Some("captured-speaker")] {
        let (mut peer, processor) = Peer::new(false, "").await;
        let mut worker = start(processor).await;
        for _ in 0..RealtimeLimits::default().queue_capacity {
            send(&worker, audio(user, vec![1, 2]), Direction::Downstream).await;
            receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
        }
        send(&worker, audio(user, vec![3, 4]), Direction::Downstream).await;
        error(&mut worker, "queue is full").await;
        peer.quiet().await;
        worker.cancel("cancel saturated audio queue").await.unwrap();
        let (_, report) = timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, WorkerState::Cancelled);
        assert_eq!(report.errors.len(), 1);
        assert!(peer.closed().await.is_empty());
    }
}

#[tokio::test]
async fn graceful_end_drains_both_audio_classes_without_extra_commands() {
    let (peer, processor) = Peer::new(true, "").await;
    let mut worker = start(processor).await;
    for user in [None, Some("captured-speaker")] {
        send(&worker, audio(user, vec![9, 10]), Direction::Downstream).await;
        receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
    }
    let report = end(worker).await;
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let commands = peer.closed().await;
    assert_eq!(commands.len(), 2);
    for command in commands {
        assert_eq!(append_pcm(&command), vec![9, 10]);
    }
}

#[tokio::test]
async fn user_audio_keeps_manual_turn_output_and_session_transcription_identity() {
    for configured_user in ["", "configured-session-user"] {
        let (mut peer, processor) = Peer::new(true, configured_user).await;
        let mut worker = start(processor).await;
        send(
            &worker,
            audio(Some("captured-speaker"), vec![1, 2]),
            Direction::Downstream,
        )
        .await;
        assert_eq!(append_pcm(&peer.command().await), vec![1, 2]);
        send(
            &worker,
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Downstream,
        )
        .await;
        assert_eq!(peer.command().await["type"], "input_audio_buffer.commit");
        assert_eq!(peer.command().await["type"], "response.create");
        peer.event(json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"user-item","content_index":0,"delta":"hel"}));
        peer.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"user-item","content_index":0,"transcript":"hello"}));
        peer.event(json!({"type":"response.created","response":{"id":"audio-response","status":"in_progress","output":[]}}));
        peer.event(json!({"type":"response.output_audio.delta","response_id":"audio-response","item_id":"assistant-item","output_index":0,"content_index":0,"delta":"AQID"}));
        peer.event(json!({"type":"response.output_audio.delta","response_id":"audio-response","item_id":"assistant-item","output_index":0,"content_index":0,"delta":"BA=="}));
        peer.event(json!({"type":"response.output_audio.done","response_id":"audio-response","item_id":"assistant-item","output_index":0,"content_index":0}));
        peer.event(json!({"type":"response.done","response":{"id":"audio-response","status":"completed","output":[]}}));
        let mut frames = Vec::new();
        timeout(Duration::from_secs(2), async {
            loop {
                let frame = worker.recv().await.unwrap();
                assert!(
                    !matches!(frame.frame.data, FrameData::Error(_)),
                    "{:?}",
                    frame.frame.data
                );
                let done = matches!(frame.frame.data, FrameData::LlmResponseEnd);
                frames.push(frame);
                if done {
                    break;
                }
            }
        })
        .await
        .unwrap();
        for (is_final, text) in [(false, "hel"), (true, "hello")] {
            assert!(frames.iter().any(|frame| {
                let transcription = match &frame.frame.data {
                    FrameData::Transcription(value) if is_final => value,
                    FrameData::InterimTranscription(value) if !is_final => value,
                    _ => return false,
                };
                frame.direction == Direction::Upstream
                    && transcription.text == text
                    && transcription.user_id == configured_user
                    && transcription.finalized == is_final
            }));
        }
        let pcm: Vec<u8> = frames
            .iter()
            .filter_map(|frame| match &frame.frame.data {
                FrameData::TtsAudio(audio) => {
                    assert_eq!(frame.direction, Direction::Downstream);
                    assert_eq!(audio.sample_rate, 24_000);
                    assert_eq!(audio.num_channels, 1);
                    Some(audio.audio.clone())
                }
                _ => None,
            })
            .flatten()
            .collect();
        assert_eq!(pcm, vec![1, 2, 3, 4]);
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.frame.data, FrameData::TtsStarted(_)))
                .count(),
            1
        );
        assert_eq!(
            frames
                .iter()
                .filter(|frame| matches!(frame.frame.data, FrameData::TtsStopped(_)))
                .count(),
            1
        );
        assert!(end(worker).await.errors.is_empty());
        assert!(peer.closed().await.is_empty());
    }
}
