//! Public frame adapter contract over a real localhost Grok WebSocket.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_grok_realtime::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::tungstenite::Message;

struct Peer {
    configured: oneshot::Receiver<()>,
    events: mpsc::UnboundedSender<Value>,
    commands: mpsc::UnboundedReceiver<Value>,
    task: JoinHandle<()>,
}
impl Peer {
    async fn new(ready: bool, user_id: &str) -> (Self, Arc<GrokRealtimeProcessor>) {
        Self::with_rates(ready, user_id, 24_000, 24_000).await
    }
    #[allow(clippy::result_large_err)] // tungstenite fixes the handshake callback error type.
    async fn with_rates(
        ready: bool,
        user_id: &str,
        input_rate: u32,
        output_rate: u32,
    ) -> (Self, Arc<GrokRealtimeProcessor>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("audio-family-loopback-key");
        config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        config.session.turn_detection = Some(None);
        let formats = config.session.audio.as_mut().unwrap();
        formats.input.as_mut().unwrap().format = AudioFormat::Pcm { rate: input_rate };
        formats.output.as_mut().unwrap().format = AudioFormat::Pcm { rate: output_rate };
        let (configured_tx, configured) = oneshot::channel();
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
                    assert_eq!(request.uri().query(), Some("model=grok-voice-latest"));
                    assert!(!request.headers().contains_key("openai-beta"));
                    Ok(response)
                },
            )
            .await
            .unwrap();
            for event in [
                json!({"event_id":"session-created","type":"session.created","session":{"id":"audio-family-session"}}),
                json!({"event_id":"conversation-created","type":"conversation.created","conversation":{"id":"audio-family-conversation"}}),
            ] {
                ws.send(Message::Text(event.to_string().into()))
                    .await
                    .unwrap();
            }
            let initial = ws.next().await.unwrap().unwrap();
            let initial: Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
            assert_eq!(initial["type"], "session.update");
            assert_eq!(
                initial["session"]["audio"]["input"]["format"]["rate"],
                input_rate
            );
            assert_eq!(
                initial["session"]["audio"]["output"]["format"]["rate"],
                output_rate
            );
            assert_eq!(initial["session"]["turn_detection"], Value::Null);
            let _ = configured_tx.send(());
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
            GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap())
                .unwrap()
                .with_user_id(user_id),
        );
        (
            Self {
                configured,
                events,
                commands,
                task,
            },
            processor,
        )
    }
    fn ready_event() -> Value {
        json!({"event_id":"audio-family-ready","type":"session.updated","session":{"id":"audio-family-session"}})
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
async fn start(processor: Arc<GrokRealtimeProcessor>) -> PipelineWorker {
    start_rates(processor, 24_000, 24_000).await
}
async fn start_rates(
    processor: Arc<GrokRealtimeProcessor>,
    input_rate: u32,
    output_rate: u32,
) -> PipelineWorker {
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = input_rate;
    config.start.audio_out_sample_rate = output_rate;
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
        "both downstream audio classes must reach the Grok wire"
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
                "Grok input requires mono PCM16 at the configured input sample rate",
            ),
            (
                vec![1, 2, 3, 4],
                24_000,
                2,
                "Grok input requires mono PCM16 at the configured input sample rate",
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
    let limit = RealtimeLimits::default().max_audio_chunk_bytes;
    for user in [None, Some("captured-speaker")] {
        let (mut peer, processor) = Peer::new(true, "").await;
        let mut worker = start(processor.clone()).await;
        send(&worker, audio(user, vec![0; limit]), Direction::Downstream).await;
        assert_eq!(append_pcm(&peer.command().await), vec![0; limit]);
        receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
        send(
            &worker,
            audio(user, vec![0; limit + 2]),
            Direction::Downstream,
        )
        .await;
        let failure = error(&mut worker, "configured limit").await;
        assert_eq!(failure.category, ErrorCategory::InvalidRequest);
        // InvalidRequest retires this session, independently of error fatality.
        assert!(!processor.health_handle().is_usable());
        assert!(peer.closed().await.is_empty());
        let report = end(worker).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.errors.len(), 1);
        assert_eq!(report.errors[0].category, ErrorCategory::InvalidRequest);
    }
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
        peer.event(json!({"type":"conversation.item.input_audio_transcription.updated","item_id":"user-item","content_index":0,"transcript":" hel "}));
        peer.event(json!({"type":"conversation.item.input_audio_transcription.updated","item_id":"user-item","content_index":null,"transcript":" hello "}));
        peer.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"user-item","content_index":0,"transcript":" hello "}));
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
        for (is_final, text) in [(false, "hel"), (false, "hello"), (true, "hello")] {
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

#[tokio::test]
async fn configured_input_rate_is_shared_by_both_audio_classes() {
    let (mut peer, processor) = Peer::with_rates(true, "", 16_000, 48_000).await;
    let mut worker = start_rates(processor, 16_000, 48_000).await;
    for user in [None, Some("captured-speaker")] {
        send(&worker, audio(user, vec![1, 2]), Direction::Downstream).await;
        error(&mut worker, "configured input sample rate").await;
        peer.quiet().await;
        let frame = frame_audio(user, AudioRawFrame::new(vec![3, 4], 16_000, 1).unwrap());
        let expected = frame.clone();
        send(&worker, frame, Direction::Downstream).await;
        let output = receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
        assert_eq!(output.frame.id, expected.id);
        assert_eq!(output.frame.data, expected.data);
        assert_eq!(append_pcm(&peer.command().await), vec![3, 4]);
    }
    assert_eq!(end(worker).await.errors.len(), 2);
    assert!(peer.closed().await.is_empty());
}

#[tokio::test]
async fn paused_audio_is_forwarded_before_validation_and_resume_accepts_both_classes() {
    let (mut peer, processor) = Peer::new(true, "").await;
    processor.set_audio_input_paused(true);
    let mut worker = start(processor.clone()).await;
    for user in [None, Some("captured-speaker")] {
        for bytes in [
            vec![],
            vec![1],
            vec![0; RealtimeLimits::default().max_audio_chunk_bytes + 2],
        ] {
            let mut frame = frame_audio(
                user,
                AudioRawFrame {
                    audio: bytes,
                    sample_rate: 0,
                    num_channels: 0,
                    context_id: Some("application-context".into()),
                },
            );
            frame.metadata.insert("paused".into(), json!(true));
            let expected = frame.clone();
            send(&worker, frame, Direction::Downstream).await;
            let output = receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
            assert_eq!(output.frame.id, expected.id);
            assert_eq!(output.frame.data, expected.data);
            assert_eq!(output.frame.metadata, expected.metadata);
        }
    }
    peer.quiet().await;
    processor.set_audio_input_paused(false);
    for user in [None, Some("resumed-speaker")] {
        send(&worker, audio(user, vec![5, 6]), Direction::Downstream).await;
        receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
    }
    processor
        .sender()
        .unwrap()
        .send(Action::CommitAudio)
        .await
        .unwrap();
    let report = end(worker).await;
    let commands = peer.closed().await;
    assert!(report.errors.is_empty());
    assert_eq!(commands.len(), 3);
    assert_eq!(append_pcm(&commands[0]), vec![5, 6]);
    assert_eq!(append_pcm(&commands[1]), vec![5, 6]);
    assert_eq!(commands[2]["type"], "input_audio_buffer.commit");
}

#[tokio::test]
async fn setup_waits_for_session_updated_before_audio_frames_can_enter() {
    let (mut peer, processor) = Peer::new(false, "").await;
    let mut startup = tokio::spawn(start(processor.clone()));
    timeout(Duration::from_secs(2), &mut peer.configured)
        .await
        .unwrap()
        .unwrap();
    assert!(processor.sender().is_err());
    assert!(timeout(Duration::from_millis(80), &mut startup)
        .await
        .is_err());
    peer.quiet().await;
    peer.event(Peer::ready_event());
    let mut worker = timeout(Duration::from_secs(2), startup)
        .await
        .unwrap()
        .unwrap();
    assert!(processor.sender().unwrap().is_ready());
    for user in [None, Some("captured-speaker")] {
        send(&worker, audio(user, vec![7, 8]), Direction::Downstream).await;
        receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
    }
    processor
        .sender()
        .unwrap()
        .send(Action::CommitAudio)
        .await
        .unwrap();
    assert!(end(worker).await.errors.is_empty());
    let commands = peer.closed().await;
    assert_eq!(commands.len(), 3);
    assert_eq!(append_pcm(&commands[0]), vec![7, 8]);
    assert_eq!(append_pcm(&commands[1]), vec![7, 8]);
    assert_eq!(commands[2]["type"], "input_audio_buffer.commit");
}

#[tokio::test]
async fn unusable_processor_rejects_both_input_audio_classes_but_keeps_controls() {
    let (peer, processor) = Peer::new(true, "").await;
    for direction in [Direction::Downstream, Direction::Upstream] {
        for user in [None, Some("captured-speaker")] {
            assert!(!processor.process_when_unusable(&audio(user, vec![1, 2]), direction));
        }
        assert!(processor.process_when_unusable(&Frame::new(FrameData::Stop), direction));
        assert!(processor.process_when_unusable(
            &Frame::new(FrameData::OutputAudio(
                AudioRawFrame::new(vec![1, 2], 24_000, 1).unwrap()
            )),
            direction,
        ));
    }
    let mut worker = start(processor.clone()).await;
    let health = processor.health_handle();
    assert!(health.set_usable(false));
    assert!(peer.closed().await.is_empty());
    // System audio retains its public scheduling priority; the sender checks
    // the retired health generation even after a local restoration.
    for restored in [false, true] {
        if restored {
            assert!(health.set_usable(true));
        }
        for user in [None, Some("captured-speaker")] {
            send(&worker, audio(user, vec![1, 2]), Direction::Downstream).await;
            error(&mut worker, "Grok health changed").await;
        }
        let control = Frame::new(FrameData::OutputAudio(
            AudioRawFrame::new(vec![3, 4], 24_000, 1).unwrap(),
        ));
        let id = control.id;
        send(&worker, control, Direction::Downstream).await;
        receive(&mut worker, |frame| frame.frame.id == id).await;
    }
    worker.cancel("retired health cleanup").await.unwrap();
    let (_, report) = timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(report.errors.len(), 4);
}

#[tokio::test]
async fn resumed_user_audio_binds_live_conversation_but_paused_audio_does_not() {
    let (peer, processor) = Peer::new(true, "").await;
    processor.set_audio_input_paused(true);
    let mut worker = start(processor.clone()).await;
    let messages = |text: &str| {
        Frame::new(FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, text)],
            run_llm: false,
        }))
    };
    send(
        &worker,
        audio(Some("paused-speaker"), vec![1, 2]),
        Direction::Downstream,
    )
    .await;
    receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
    send(&worker, messages("not-bound"), Direction::Downstream).await;
    receive(&mut worker, |frame| {
        matches!(frame.frame.data, FrameData::LlmMessagesAppend(_))
    })
    .await;
    processor.set_audio_input_paused(false);
    send(
        &worker,
        audio(Some("resumed-speaker"), vec![3, 4]),
        Direction::Downstream,
    )
    .await;
    receive(&mut worker, |frame| is_audio(&frame.frame.data)).await;
    send(&worker, messages("live-history"), Direction::Downstream).await;
    receive(&mut worker, |frame| {
        matches!(frame.frame.data, FrameData::LlmMessagesAppend(_))
    })
    .await;
    processor
        .sender()
        .unwrap()
        .send(Action::CommitAudio)
        .await
        .unwrap();
    assert!(end(worker).await.errors.is_empty());
    let commands = peer.closed().await;
    assert_eq!(commands.len(), 3);
    assert_eq!(append_pcm(&commands[0]), vec![3, 4]);
    assert_eq!(commands[1]["type"], "conversation.item.create");
    assert_eq!(commands[1]["item"]["content"][0]["text"], "live-history");
    assert_eq!(commands[2]["type"], "input_audio_buffer.commit");
}
