use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_speechmatics::*;
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("fixture timeout")
}
fn audio() -> AudioRawFrame {
    AudioRawFrame::new(vec![1; 640], 16000, 1).unwrap()
}
struct Fixture {
    config: SpeechmaticsSttConfig,
    input: mpsc::Sender<Value>,
    wire: mpsc::Receiver<Message>,
    task: tokio::task::JoinHandle<()>,
}
async fn fixture(eos_reply: bool, capacity: usize) -> Fixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechmaticsSttConfig::new("fixture-secret");
    config.endpoint = format!("ws://{}/v2", listener.local_addr().unwrap());
    config.limits.queue_capacity = capacity;
    config.limits.output_timeout = Duration::from_millis(200);
    config.limits.response_timeout = Duration::from_millis(150);
    config.limits.close_timeout = Duration::from_millis(20);
    let (input, mut rx) = mpsc::channel::<Value>(64);
    let (tx, wire) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(
                json!({"message":"RecognitionStarted"}).to_string().into(),
            ))
            .await
            .unwrap();
        loop {
            tokio::select! {value=rx.recv()=>{let Some(value)=value else{break};if socket.send(Message::Text(value.to_string().into())).await.is_err(){break}},message=socket.next()=>match message{None|Some(Err(_))|Some(Ok(Message::Close(_)))=>break,Some(Ok(message))=>{let eos=message.to_text().is_ok_and(|s|s.contains("EndOfStream"));if tx.send(message).await.is_err(){break}
            if eos&&eos_reply{let _=socket.send(Message::Text(json!({"message":"EndOfTranscript"}).to_string().into())).await;}}}}
        }
    });
    Fixture {
        config,
        input,
        wire,
        task,
    }
}
async fn next(
    session: &mut SpeechmaticsSttSession,
) -> Result<SpeechmaticsSttEvent, StreamingError> {
    bounded(session.next()).await.unwrap()
}
async fn terminal(session: &mut SpeechmaticsSttSession) -> StreamingError {
    loop {
        if let Err(e) = next(session).await {
            return e;
        }
    }
}
#[test]
fn invalid_endpoint_secrets_and_format_fail_without_leaking() {
    for endpoint in [
        "ws://example.com/agent",
        "wss://user:super-secret@example.com/agent",
        "wss://example.com/agent?token=super-secret",
        "wss://example.com/agent#super-secret",
    ] {
        let mut c = SpeechmaticsSttConfig::new("super-secret");
        c.endpoint = endpoint.into();
        let e = SpeechmaticsSttClient::new(c).unwrap_err();
        assert!(!format!("{e:?} {e}").contains("super-secret"));
    }
    let mut c = SpeechmaticsSttConfig::new("header\r\ninjection");
    assert!(SpeechmaticsSttClient::new(c.clone()).is_err());
    c.api_key = "okay".into();
    c.ttfs_p99_latency = f64::NAN;
    assert!(SpeechmaticsSttClient::new(c).is_err());
}
#[tokio::test]
async fn active_settings_are_atomic_and_finish_is_a_shared_one_way_gate() {
    let mut f = fixture(true, 16).await;
    f.config.settings.enable_diarization = true;
    let mut session = SpeechmaticsSttClient::new(f.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    let clone = sender.clone();
    next(&mut session).await.unwrap();
    assert_eq!(
        sender.merge_settings(
            json!({"language":"es","speaker_active_format":"changed {text}"})
                .as_object()
                .unwrap()
        ),
        Err(StreamingError::RequiresRestart)
    );
    assert_eq!(sender.settings_snapshot().unwrap().language, "en");
    assert_eq!(
        sender.settings_snapshot().unwrap().speaker_active_format,
        "{text}"
    );
    sender
        .merge_settings(
            json!({"speaker_active_format":"{ts}/{speaker_id}: {text}"})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    // AudioSent proves the preceding local-format command was processed.
    sender.try_send_audio(audio()).unwrap();
    assert!(matches!(
        next(&mut session).await.unwrap(),
        SpeechmaticsSttEvent::AudioSent { .. }
    ));
    f.input.send(json!({"message":"AddSegment","segment":{"transcript":"text"},"metadata":{"start_time":0.5,"end_time":1.0}})).await.unwrap();
    let t = next(&mut session).await.unwrap();
    assert!(matches!(t,SpeechmaticsSttEvent::Transcript(t) if t.text=="0.5/UU: text"));
    f.input.send(json!({"message":"AddSegment","segment":{"transcript":"zero"},"metadata":{"start_time":0.0}})).await.unwrap();
    assert!(
        matches!(next(&mut session).await.unwrap(),SpeechmaticsSttEvent::Transcript(t) if t.text=="0.0/UU: zero")
    );
    sender.try_get_speakers(true).unwrap();
    sender.finish().await.unwrap();
    assert_eq!(
        clone.merge_settings(json!({}).as_object().unwrap()),
        Err(StreamingError::Closed)
    );
    assert!(clone.try_get_speakers(false).is_err());
    assert!(clone.finish().await.is_err());
    let mut got = vec![];
    while !got.iter().any(|v: &Value| v["message"] == "EndOfStream") {
        let message = bounded(f.wire.recv()).await.unwrap();
        if let Message::Text(text) = message {
            got.push(serde_json::from_str::<Value>(&text).unwrap());
        }
    }
    assert!(got.contains(&json!({"message":"GetSpeakers","final":true})));
    assert_eq!(
        got.iter().filter(|v| v["message"] == "EndOfStream").count(),
        1
    );
    while bounded(session.next()).await.is_some() {}
    assert_eq!(sender.snapshot().lifecycle, SessionLifecycle::Closed);
    bounded(f.task).await.unwrap();
}
#[tokio::test]
async fn bounded_audio_budget_queue_rollback_and_cancel_under_output_pressure() {
    let mut f = fixture(false, 1).await;
    f.config.limits.max_buffered_audio_bytes = 640;
    let mut session = SpeechmaticsSttClient::new(f.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    // Ready occupies the sole output slot; a sent counter is an observed
    // barrier that the actor is blocked publishing the first audio receipt.
    sender.try_send_audio(audio()).unwrap();
    wait(|| sender.snapshot().sent_seq == 1).await;
    assert_eq!(
        sender.try_send_audio(audio()),
        Err(StreamingError::Backpressure)
    );
    sender.try_start_turn().unwrap(); // Occupy the sole command slot.
    assert_eq!(
        sender.merge_settings(
            json!({"speaker_active_format":"new {text}"})
                .as_object()
                .unwrap()
        ),
        Err(StreamingError::Backpressure)
    );
    assert_eq!(
        sender.settings_snapshot().unwrap().speaker_active_format,
        "{text}"
    );
    sender.cancel();
    assert_eq!(terminal(&mut session).await, StreamingError::Cancelled);
    bounded(f.task).await.unwrap();
}
#[tokio::test]
async fn output_pressure_and_missing_end_are_failures_not_graceful_completion() {
    let f = fixture(false, 1).await;
    let mut session = SpeechmaticsSttClient::new(f.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    f.input
        .send(json!({"message":"AddSegment","segment":{"transcript":"not consumed"}}))
        .await
        .unwrap();
    wait(|| session.sender().snapshot().lifecycle == SessionLifecycle::Failed).await;
    assert!(matches!(
        next(&mut session).await.unwrap(),
        SpeechmaticsSttEvent::Ready { .. }
    ));
    assert_eq!(
        terminal(&mut session).await,
        StreamingError::Timeout("event output")
    );
    bounded(f.task).await.unwrap();
    let f = fixture(false, 4).await;
    let mut session = SpeechmaticsSttClient::new(f.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    next(&mut session).await.unwrap();
    session.sender().finish().await.unwrap();
    assert_eq!(
        terminal(&mut session).await,
        StreamingError::Timeout("response drain")
    );
    bounded(f.task).await.unwrap();
}
#[tokio::test]
async fn impossible_ack_duplicate_ready_and_raw_error_are_bounded_failures() {
    for value in [
        json!({"message":"AudioAdded","seq_no":1}),
        json!({"message":"RecognitionStarted"}),
        json!({"message":"AddSegment","segment":false}),
        json!({"message":"Error","type":"not_authorised","reason":"super-secret transcript"}),
    ] {
        let f = fixture(false, 4).await;
        let mut session = SpeechmaticsSttClient::new(f.config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        next(&mut session).await.unwrap();
        f.input.send(value).await.unwrap();
        let e = terminal(&mut session).await;
        assert!(!format!("{e:?} {e}").contains("super-secret"));
        bounded(f.task).await.unwrap();
    }
}
#[derive(Default)]
struct Recorder(Mutex<Vec<(Frame, Direction)>>);
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "LifecycleRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0.lock().unwrap().push((frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}
async fn wait(mut test: impl FnMut() -> bool) {
    bounded(async {
        while !test() {
            tokio::task::yield_now().await;
        }
    })
    .await
}
#[tokio::test]
async fn external_pipeline_interruption_keeps_input_alive_and_usage_precedes_final() {
    let mut f = fixture(true, 16).await;
    let recorder = Arc::new(Recorder::default());
    let stt = Arc::new(SpeechmaticsSttProcessor::new(
        SpeechmaticsSttClient::new(f.config).unwrap(),
    ));
    let worker = Pipeline::new(vec![stt, recorder.clone()])
        .start(PipelineConfig {
            start: StartFrame {
                enable_usage_metrics: true,
                enable_metrics: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(audio())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    bounded(f.wire.recv()).await.unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::Interruption), Direction::Downstream)
        .await
        .unwrap();
    f.input
        .send(json!({"message":"StartOfTurn"}))
        .await
        .unwrap();
    f.input
        .send(json!({"message":"AddSegment","segment":{"transcript":"persistent","speaker":"S9"}}))
        .await
        .unwrap();
    f.input.send(json!({"message":"EndOfTurn"})).await.unwrap();
    wait(|| {
        recorder
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|(f, _)| matches!(f.data, FrameData::TranscriptionDetailed(_)))
    })
    .await;
    {
        let frames = recorder.0.lock().unwrap();
        let usage=frames.iter().position(|(f,_)|matches!(&f.data,FrameData::Metrics(m) if m.iter().any(|m|matches!(m,Metric::SttUsage{audio_seconds,..} if (*audio_seconds-0.02).abs()<1e-9)))).unwrap();
        let transcript = frames
            .iter()
            .position(|(f, _)| matches!(f.data, FrameData::TranscriptionDetailed(_)))
            .unwrap();
        assert!(usage < transcript);
        assert!(!frames.iter().any(|(f, _)| matches!(
            f.data,
            FrameData::ProposedUserStartedSpeaking | FrameData::ProposedUserStoppedSpeaking
        )));
        assert!(!frames.iter().any(|(f,_)|matches!(&f.data,FrameData::Metrics(m) if m.iter().any(|m|matches!(m,Metric::Processing{..})))));
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(f.task).await.unwrap();
}

#[tokio::test]
async fn mute_cancels_epoch_and_unmute_never_replays_old_audio_or_callbacks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechmaticsSttConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/agent", listener.local_addr().unwrap());
    config.settings.turn_detection_mode = TurnDetectionMode::Vad;
    config.limits.close_timeout = Duration::from_millis(50);
    let (sessions, mut accepted) = mpsc::channel(2);
    let server = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        for epoch in 0..2 {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(
                    json!({"message":"RecognitionStarted","id":format!("epoch-{epoch}")})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            let (tx, mut rx) = mpsc::channel::<Value>(16);
            let (wire, wire_rx) = mpsc::channel(16);
            sessions.send((tx, wire_rx)).await.unwrap();
            tasks.spawn(async move{loop{tokio::select!{
                value=rx.recv()=>{let Some(value)=value else{break};if socket.send(Message::Text(value.to_string().into())).await.is_err(){break}},
                value=socket.next()=>match value {None|Some(Err(_))|Some(Ok(Message::Close(_)))=>{let _=socket.send(Message::Text(json!({"message":"AddSegment","segment":{"transcript":"late pre-mute"}}).to_string().into())).await;break},Some(Ok(value))=>{let eos=value.to_text().is_ok_and(|s|s.contains("EndOfStream"));if wire.send(value).await.is_err(){break}
if eos{let _=socket.send(Message::Text(json!({"message":"EndOfTranscript"}).to_string().into())).await;}}}
            }}});
        }
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    });
    let stt = Arc::new(SpeechmaticsSttProcessor::new(
        SpeechmaticsSttClient::new(config).unwrap(),
    ));
    let record = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt.clone(), record.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let (old, mut first) = bounded(accepted.recv()).await.unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(audio())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(matches!(
        bounded(first.recv()).await,
        Some(Message::Binary(_))
    ));
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
        .await
        .unwrap();
    wait(|| {
        record
            .0
            .lock()
            .unwrap()
            .iter()
            .any(|(f, _)| matches!(f.data, FrameData::SttMute(true)))
    })
    .await;
    assert!(stt.sender().unwrap().is_closed());
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(audio())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    wait(|| {
        record
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|(f, d)| {
                *d == Direction::Downstream && matches!(f.data, FrameData::InputAudio(_))
            })
            .count()
            == 2
    })
    .await;
    let _ = old
        .send(json!({"message":"AddSegment","segment":{"transcript":"muted old"}}))
        .await;
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(false)), Direction::Downstream)
        .await
        .unwrap();
    let (new, mut second) = bounded(accepted.recv()).await.unwrap();
    wait(|| stt.sender().is_ok_and(|s| !s.is_closed())).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(80), second.recv())
            .await
            .is_err(),
        "pre-mute audio replayed into fresh session"
    );
    new.send(json!({"message":"AddSegment","segment":{"transcript":"fresh session"}}))
        .await
        .unwrap();
    wait(||record.0.lock().unwrap().iter().any(|(f,_)|matches!(&f.data,FrameData::TranscriptionDetailed(t) if t.text.text=="fresh session"))).await;
    {
        let frames = record.0.lock().unwrap();
        assert_eq!(
            frames
                .iter()
                .filter(|(f, _)| matches!(f.data, FrameData::TranscriptionDetailed(_)))
                .count(),
            1
        );
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn provider_metadata_does_not_replace_caller_turn_strategies() {
    use pipecat_turns::{
        LLMContextAggregatorPair, UserAggregatorConfig, UserTurnStartStrategies,
        UserTurnStartStrategy, UserTurnStopStrategies, UserTurnStopStrategy,
    };
    let mut f = fixture(true, 16).await;
    f.config.settings.turn_detection_mode = TurnDetectionMode::Vad;
    let stt = Arc::new(SpeechmaticsSttProcessor::new(
        SpeechmaticsSttClient::new(f.config).unwrap(),
    ));
    let mut config = UserAggregatorConfig {
        empty_user_turn: None,
        ..Default::default()
    };
    config.turn.start_on_transcription = false;
    config.turn.start_strategies =
        Some(UserTurnStartStrategies::new(vec![UserTurnStartStrategy::vad()]).unwrap());
    config.turn.stop_strategies = Some(
        UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(
            Duration::from_secs(30),
            true,
        )])
        .unwrap(),
    );
    let pair = LLMContextAggregatorPair::new(
        pipecat_services::Context::default(),
        config,
        Default::default(),
    );
    let record = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt, pair.user(), record.clone()])
        .start(Default::default())
        .await
        .unwrap();
    f.input
        .send(json!({"message":"StartOfTurn"}))
        .await
        .unwrap();
    f.input.send(json!({"message":"EndOfTurn"})).await.unwrap();
    wait(|| {
        record.0.lock().unwrap().iter().any(|(f, d)| {
            *d == Direction::Downstream && matches!(f.data, FrameData::ProposedUserStoppedSpeaking)
        })
    })
    .await;
    assert!(!record.0.lock().unwrap().iter().any(|(f, _)| matches!(
        f.data,
        FrameData::UserStartedSpeaking | FrameData::Interruption
    )));
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(f.task).await.unwrap();
}

#[tokio::test]
async fn malformed_audio_and_zero_progress_finalize_do_not_mutate_counters() {
    let mut f = fixture(true, 8).await;
    let limit = f.config.limits.max_message_bytes;
    let mut session = SpeechmaticsSttClient::new(f.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    next(&mut session).await.unwrap();
    let sender = session.sender();
    for (bytes, rate, channels) in [
        (vec![], 16000, 1),
        (vec![0; 3], 16000, 1),
        (vec![0; 640], 8000, 1),
        (vec![0; 640], 16000, 2),
        (vec![0; limit + 2], 16000, 1),
    ] {
        assert!(sender
            .try_send_audio(AudioRawFrame {
                audio: bytes,
                sample_rate: rate,
                num_channels: channels,
                context_id: None
            })
            .is_err());
    }
    assert!(sender.try_finalize().is_err());
    assert_eq!(sender.snapshot().sent_seq, 0);
    assert_eq!(sender.snapshot().audio_bytes, 0);
    sender
        .try_send_audio(AudioRawFrame::new(vec![0; 2], 16000, 1).unwrap())
        .unwrap();
    sender.try_finalize().unwrap();
    assert!(sender.try_finalize().is_err());
    assert_eq!(
        bounded(f.wire.recv()).await.unwrap(),
        Message::Binary(vec![0; 2].into())
    );
    let wire = bounded(f.wire.recv()).await.unwrap();
    let value: Value = serde_json::from_str(wire.to_text().unwrap()).unwrap();
    assert_eq!(
        value,
        json!({"message":"ForceEndOfUtterance","timestamp":0.0000625})
    );
    sender.finish().await.unwrap();
    while bounded(session.next()).await.is_some() {}
    assert_eq!(sender.snapshot().sent_seq, 1);
    assert_eq!(sender.snapshot().audio_bytes, 2);
    bounded(f.task).await.unwrap();
}

#[tokio::test]
async fn startup_rejection_and_oversized_provider_message_have_bounded_safe_errors() {
    for rejection in [
        json!({"message":"Error","type":"not_authorised","reason":"do not echo this secret"}),
        json!({"message":"EndOfTranscript"}),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = SpeechmaticsSttConfig::new("fixture");
        config.endpoint = format!("ws://{}/v2/agent", listener.local_addr().unwrap());
        config.limits.ready_timeout = Duration::from_secs(30);
        config.limits.close_timeout = Duration::from_millis(20);
        let peer = tokio::spawn(async move {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text(rejection.to_string().into()))
                .await
                .unwrap();
            while let Some(Ok(value)) = socket.next().await {
                if value.is_close() {
                    break;
                }
            }
        });
        let error = match tokio::time::timeout(
            Duration::from_millis(300),
            SpeechmaticsSttClient::new(config).unwrap().connect(),
        )
        .await
        .unwrap()
        {
            Ok(_) => panic!("startup rejection was accepted"),
            Err(e) => e,
        };
        assert!(!format!("{error:?} {error}").contains("secret"));
        assert!(!matches!(error, StreamingError::Timeout(_)));
        bounded(peer).await.unwrap();
    }
    let mut f = fixture(false, 4).await;
    f.config.limits.max_message_bytes = 512;
    let mut session = SpeechmaticsSttClient::new(f.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    next(&mut session).await.unwrap();
    f.input
        .send(json!({"message":"AddSegment","segment":{"transcript":"x".repeat(1024)}}))
        .await
        .unwrap();
    assert_eq!(terminal(&mut session).await, StreamingError::TooLarge);
    bounded(f.task).await.unwrap();
}

#[tokio::test]
async fn targeted_updates_passthrough_directions_and_bounded_speaker_subscription() {
    for enabled in [false, true] {
        let mut f = fixture(true, 16).await;
        f.config.settings.enable_diarization = enabled;
        let stt = Arc::new(SpeechmaticsSttProcessor::new(
            SpeechmaticsSttClient::new(f.config).unwrap(),
        ));
        let mut speakers = stt.subscribe_speakers();
        let before = Arc::new(Recorder::default());
        let after = Arc::new(Recorder::default());
        let worker = Pipeline::new(vec![before.clone(), stt.clone(), after.clone()])
            .start(Default::default())
            .await
            .unwrap();
        let other = Frame::new(FrameData::SttUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame::<SttSettings> {
                service: Some(ProcessorReference::Name("AnotherService".into())),
                settings: [("language".into(), json!("es"))].into(),
                ..Default::default()
            },
        )));
        let other_id = other.id;
        worker
            .sender()
            .send(other, Direction::Downstream)
            .await
            .unwrap();
        wait(|| {
            after
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|(f, d)| f.id == other_id && *d == Direction::Downstream)
        })
        .await;
        assert_eq!(stt.config_snapshot().unwrap().settings.language, "en");
        let passthrough = Frame::new(FrameData::Text(TextFrame::new("upstream control")));
        let id = passthrough.id;
        worker
            .sender()
            .send(passthrough, Direction::Upstream)
            .await
            .unwrap();
        wait(|| {
            before
                .0
                .lock()
                .unwrap()
                .iter()
                .any(|(f, d)| f.id == id && *d == Direction::Upstream)
        })
        .await;
        let update = Frame::new(FrameData::SttUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame::<SttSettings> {
                delta: Some(SttSettings {
                    base: ServiceSettings {
                        extra: [("speaker_active_format".into(), json!("tag {text}"))].into(),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
                ..Default::default()
            },
        )));
        worker
            .sender()
            .send(update, Direction::Downstream)
            .await
            .unwrap();
        wait(|| {
            stt.config_snapshot()
                .unwrap()
                .settings
                .speaker_active_format
                == "tag {text}"
        })
        .await;
        f.input.send(json!({"message":"SpeakersResult","speakers":[{"label":"S1","speaker_identifiers":["opaque-enrollment"]}]})).await.unwrap();
        if enabled {
            let result = bounded(speakers.recv()).await.unwrap();
            assert_eq!(result.message["speakers"][0]["label"], "S1");
            assert!(!format!("{result:?}").contains("opaque-enrollment"));
        } else {
            f.input.send(json!({"message":"AddSegment","segment":{"transcript":"speaker delivery barrier"}})).await.unwrap();
            wait(|| after.0.lock().unwrap().iter().any(|(f, _)| matches!(&f.data, FrameData::TranscriptionDetailed(t) if t.text.text == "tag speaker delivery barrier"))).await;
            assert!(matches!(
                speakers.try_recv(),
                Err(tokio::sync::broadcast::error::TryRecvError::Empty)
            ));
        }
        worker.end().await.unwrap();
        let (_, report) = bounded(worker.collect()).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        bounded(f.task).await.unwrap();
    }
}

#[tokio::test]
async fn dropped_finish_admission_does_not_close_shared_sender_gate() {
    let f = fixture(true, 1).await;
    let mut session = SpeechmaticsSttClient::new(f.config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    // Ready occupies the sole output slot. The sent counter proves the actor
    // has consumed audio one and is blocked publishing AudioSent behind Ready.
    sender.try_send_audio(audio()).unwrap();
    wait(|| sender.snapshot().sent_seq == 1).await;
    // This second packet now occupies the sole command slot. Neither its
    // dequeue nor a finish reservation can advance until output is consumed.
    sender.try_send_audio(audio()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), sender.finish())
            .await
            .is_err()
    );
    assert!(!sender.is_closed());
    assert!(matches!(
        next(&mut session).await.unwrap(),
        SpeechmaticsSttEvent::Ready { .. }
    ));
    assert!(matches!(
        next(&mut session).await.unwrap(),
        SpeechmaticsSttEvent::AudioSent { sent_seq: 1, .. }
    ));
    assert!(matches!(
        next(&mut session).await.unwrap(),
        SpeechmaticsSttEvent::AudioSent { sent_seq: 2, .. }
    ));
    sender.try_send_audio(audio()).unwrap();
    assert!(matches!(
        next(&mut session).await.unwrap(),
        SpeechmaticsSttEvent::AudioSent { sent_seq: 3, .. }
    ));
    sender.finish().await.unwrap();
    while bounded(session.next()).await.is_some() {}
    bounded(f.task).await.unwrap();
}
