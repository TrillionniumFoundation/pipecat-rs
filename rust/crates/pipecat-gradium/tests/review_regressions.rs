//! Independently authored loopback and pipeline regressions. No hosted service.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gradium::*;
use pipecat_services::Context;
use pipecat_turns::{
    LLMContextAggregatorPair, UserAggregatorConfig, UserTurnStartStrategies, UserTurnStartStrategy,
    UserTurnStopStrategies, UserTurnStopStrategy,
};
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{accept_hdr_async, tungstenite::Message};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("independent test deadline")
}
async fn until(mut condition: impl FnMut() -> bool) {
    bounded(async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
}
struct Peer {
    config: GradiumSttConfig,
    outgoing: mpsc::Sender<Value>,
    incoming: mpsc::Receiver<Value>,
    setup: mpsc::Receiver<Value>,
    task: JoinHandle<()>,
}
async fn peer() -> Peer {
    peer_sessions(1).await
}
// Tungstenite requires the fixed, unboxed HTTP ErrorResponse callback type.
#[allow(clippy::result_large_err)]
async fn peer_sessions(sessions: usize) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mut config = GradiumSttConfig::new("fixture-key-never-live");
    config.endpoint = format!("ws://{address}/api/speech/asr");
    config.allow_loopback = true;
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.read_timeout = Duration::from_secs(3);
    config.limits.flush_timeout = Duration::from_millis(700);
    config.limits.finish_timeout = Duration::from_secs(2);
    config.limits.close_timeout = Duration::from_millis(100);
    let (outgoing, mut outgoing_rx) = mpsc::channel::<Value>(64);
    let (incoming_tx, incoming) = mpsc::channel(256);
    let (setup_tx, setup) = mpsc::channel(8);
    let task = tokio::spawn(async move {
        for _ in 0..sessions {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(
                socket,
                |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                    assert_eq!(request.uri().path(), "/api/speech/asr");
                    assert_eq!(request.headers()["x-api-key"], "fixture-key-never-live");
                    assert_eq!(request.headers()["x-api-source"], "pipecat");
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let Message::Text(setup) = socket.next().await.unwrap().unwrap() else {
                panic!("JSON setup required")
            };
            let setup: Value = serde_json::from_str(&setup).unwrap();
            assert_eq!(setup["type"], "setup");
            let rate: u64 = setup["input_format"]
                .as_str()
                .unwrap()
                .strip_prefix("pcm_")
                .unwrap()
                .parse()
                .unwrap();
            setup_tx.send(setup).await.unwrap();
            socket
                .send(Message::Text(
                    json!({"type":"ready","sample_rate":rate,"frame_size":rate*80/1000})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
            loop {
                tokio::select! {
                    output = outgoing_rx.recv() => match output {
                        Some(output) => if socket.send(Message::Text(output.to_string().into())).await.is_err() { break; },
                        None => break,
                    },
                    input = socket.next() => match input {
                        Some(Ok(Message::Text(text))) => {
                            let value: Value = serde_json::from_str(&text).unwrap();
                            let eos = value["type"] == "end_of_stream";
                            if incoming_tx.send(value).await.is_err() { break; }
                            if eos && socket.send(Message::Text(json!({"type":"end_of_stream"}).to_string().into())).await.is_err() { break; }
                        },
                        Some(Ok(Message::Ping(bytes))) => if socket.send(Message::Pong(bytes)).await.is_err() { break; },
                        Some(Ok(Message::Pong(_))) => (),
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                        _ => panic!("unexpected client frame"),
                    }
                }
            }
        }
    });
    Peer {
        config,
        outgoing,
        incoming,
        setup,
        task,
    }
}
async fn emit(peer: &Peer, value: Value) {
    bounded(peer.outgoing.send(value)).await.unwrap();
}
async fn wire(peer: &mut Peer) -> Value {
    bounded(peer.incoming.recv()).await.expect("wire message")
}
async fn event(session: &mut GradiumSttSession) -> Result<GradiumSttEvent, GradiumError> {
    bounded(session.next()).await.expect("session event")
}
fn pcm(rate: u32, bytes: Vec<u8>) -> AudioRawFrame {
    AudioRawFrame {
        audio: bytes,
        sample_rate: rate,
        num_channels: 1,
        context_id: None,
    }
}
async fn closed(mut peer: Peer, mut session: GradiumSttSession) {
    session.cancel();
    while bounded(session.next()).await.is_some() {}
    bounded(&mut peer.task).await.unwrap();
}
fn step(index: u64, probability: f64) -> Value {
    json!({"type":"step","step_idx":index,"vad":[{"horizon_s":3.0,"inactivity_prob":probability}]})
}

#[tokio::test]
async fn review_flush_ids_are_exact_string_identity_not_numeric_coercion() {
    for bad in [
        json!(1),
        json!("01"),
        json!("+1"),
        json!(" 1"),
        json!("2"),
        Value::Null,
        json!(true),
    ] {
        let mut peer = peer().await;
        let mut session = GradiumSttClient::new(peer.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        session.sender().try_flush().unwrap();
        assert_eq!(
            wire(&mut peer).await,
            json!({"type":"flush","flush_id":"1"})
        );
        emit(&peer, json!({"type":"flushed","flush_id":bad})).await;
        assert!(
            matches!(event(&mut session).await, Err(GradiumError::Protocol(_))),
            "invalid flush identity accepted"
        );
        closed(peer, session).await;
    }
}

#[tokio::test]
async fn review_unsolicited_duplicate_and_early_terminal_cannot_commit_text() {
    for scenario in 0..3 {
        let mut peer = peer().await;
        let mut session = GradiumSttClient::new(peer.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        emit(&peer, json!({"type":"text","text":"pending"})).await;
        assert!(
            matches!(event(&mut session).await, Ok(GradiumSttEvent::Transcript(t)) if !t.is_final)
        );
        if scenario == 1 {
            session.sender().try_flush().unwrap();
            wire(&mut peer).await;
            emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
            assert!(matches!(
                event(&mut session).await,
                Ok(GradiumSttEvent::Flushed { .. })
            ));
        }
        emit(
            &peer,
            if scenario == 2 {
                json!({"type":"end_of_stream"})
            } else {
                json!({"type":"flushed","flush_id":"1"})
            },
        )
        .await;
        assert!(
            matches!(event(&mut session).await, Err(GradiumError::Protocol(_))),
            "unsolicited/duplicate/EOS finalized pending text"
        );
        closed(peer, session).await;
    }
}

#[tokio::test]
async fn review_split_pcm_and_resample_geometry_match_independent_sample_counts() {
    for input_rate in [1000u32, 8000, 11025, 16000, 22050, 24000, 48000, 192000] {
        let mut peer = peer().await;
        peer.config.sample_rate = input_rate;
        let output_rate = peer.config.wire_sample_rate();
        let mut session = GradiumSttClient::new(peer.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        let setup = bounded(peer.setup.recv()).await.unwrap();
        assert_eq!(
            setup,
            json!({"type":"setup","model_name":"default","input_format":format!("pcm_{output_rate}"),"json_config":{"language":"en","delay_in_frames":12}})
        );
        // 100 ms constant signal, split within PCM samples. Resampling preserves DC.
        let samples = input_rate as usize / 10;
        let bytes: Vec<_> = (0..samples).flat_map(|_| 1234i16.to_le_bytes()).collect();
        let sender = session.sender();
        for range in [0..1, 1..bytes.len() - 1, bytes.len() - 1..bytes.len()] {
            sender
                .try_send_audio(pcm(input_rate, bytes[range].to_vec()))
                .unwrap();
        }
        sender.try_flush().unwrap();
        let mut received = Vec::new();
        loop {
            let value = wire(&mut peer).await;
            if value["type"] == "flush" {
                assert_eq!(value["flush_id"], "1");
                break;
            }
            assert_eq!(value["type"], "audio");
            let data = STANDARD.decode(value["audio"].as_str().unwrap()).unwrap();
            assert_eq!(data.len() % 2, 0);
            assert!(data.len() <= output_rate as usize * 80 / 1000 * 2);
            received.extend(data);
        }
        assert_eq!(sender.statistics().accepted_input_bytes, bytes.len() as u64);
        assert_eq!(sender.statistics().sent_wire_bytes, received.len() as u64);
        assert_eq!(sender.statistics().confirmed_wire_bytes, 0);
        emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
        assert!(
            matches!(event(&mut session).await, Ok(GradiumSttEvent::Flushed{confirmed_wire_bytes, ..}) if confirmed_wire_bytes == received.len() as u64)
        );
        let finished = tokio::spawn(async move { sender.finish().await });
        loop {
            let value = wire(&mut peer).await;
            if value["type"] == "end_of_stream" {
                break;
            }
            assert_eq!(value["type"], "audio");
            let data = STANDARD.decode(value["audio"].as_str().unwrap()).unwrap();
            assert_eq!(data.len() % 2, 0);
            assert!(data.len() <= output_rate as usize * 80 / 1000 * 2);
            received.extend(data);
        }
        while let Some(event) = bounded(session.next()).await {
            event.unwrap();
        }
        bounded(finished).await.unwrap().unwrap();
        let expected_frames = (samples as u64 * u64::from(output_rate) + u64::from(input_rate) / 2)
            / u64::from(input_rate);
        assert_eq!(
            received.len() as u64,
            expected_frames * 2,
            "rate {input_rate}"
        );
        assert!(received
            .as_chunks::<2>()
            .0
            .iter()
            .all(|v| i16::from_le_bytes([v[0], v[1]]) == 1234));
        bounded(peer.task).await.unwrap();
    }
}

#[tokio::test]
async fn review_incomplete_pcm_tail_is_failure_not_zero_padding() {
    let mut peer = peer().await;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    session
        .sender()
        .try_send_audio(pcm(16000, vec![7]))
        .unwrap();
    let sender = session.sender();
    let finished = tokio::spawn(async move { sender.finish().await });
    assert_eq!(event(&mut session).await, Err(GradiumError::PartialSample));
    assert_eq!(
        bounded(finished).await.unwrap(),
        Err(GradiumError::PartialSample)
    );
    assert!(
        peer.incoming.try_recv().is_err(),
        "incomplete sample was sent/padded"
    );
    closed(peer, session).await;
}

#[tokio::test]
async fn review_output_backpressure_and_cumulative_input_have_finite_failure() {
    let mut peer = peer().await;
    peer.config.limits.queue_capacity = 2;
    peer.config.limits.max_session_input_bytes = 3;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender.try_send_audio(pcm(16000, vec![1, 2])).unwrap();
    assert_eq!(
        sender.try_send_audio(pcm(16000, vec![3, 4])),
        Err(GradiumError::TooLarge)
    );
    assert_eq!(sender.statistics().accepted_input_bytes, 2);
    for text in ["one", "two", "three"] {
        emit(&peer, json!({"type":"text","text":text})).await;
    }
    until(|| sender.is_closed()).await;
    assert_eq!(event(&mut session).await, Err(GradiumError::Backpressure));
    closed(peer, session).await;
}

#[derive(Default)]
struct Recorder(Mutex<Vec<Frame>>);
impl Recorder {
    fn count(&self, predicate: impl Fn(&FrameData) -> bool) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|f| predicate(&f.data))
            .count()
    }
}
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "GradiumIndependentRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_real_aggregator_waits_for_trailing_final_and_respects_interruption_policy() {
    let mut peer = peer().await;
    peer.config.enable_turn_detection = true;
    peer.config.settings.turn.post_flush_cooldown_frames = 0;
    let stt = Arc::new(
        GradiumSttProcessor::new(GradiumSttClient::new(peer.config.clone()).unwrap())
            .with_user_id("caller-review")
            .unwrap(),
    );
    let mut user = UserAggregatorConfig {
        empty_user_turn: None,
        ..Default::default()
    };
    user.turn.enable_interruptions = false;
    let pair = LLMContextAggregatorPair::new(Context::default(), user, Default::default());
    let raw = Arc::new(Recorder::default());
    let downstream = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt, raw.clone(), pair.user(), downstream.clone()])
        .start(PipelineConfig {
            start: StartFrame {
                enable_usage_metrics: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    emit(&peer, step(1, 0.1)).await;
    emit(&peer, step(2, 0.5)).await;
    emit(&peer, step(3, 0.1)).await;
    emit(&peer, json!({"type":"text","text":"first"})).await;
    until(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    emit(&peer, step(4, 0.5)).await;
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"flush","flush_id":"1"})
    );
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    emit(&peer, json!({"type":"text","text":"tail"})).await;
    emit(&peer, step(5, 0.1)).await;
    until(|| downstream.count(|f| matches!(f, FrameData::UserStoppedSpeaking)) == 1).await;
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::ProposedUserStartedSpeaking)),
        1
    );
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::Interruption)),
        0
    );
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    {
        let frames = raw.0.lock().unwrap();
        let final_index = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::TranscriptionDetailed(_)))
            .unwrap();
        let usage_index = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::Metrics(_)))
            .unwrap();
        assert!(usage_index < final_index);
        let FrameData::TranscriptionDetailed(final_text) = &frames[final_index].data else {
            unreachable!()
        };
        assert_eq!(final_text.text.text, "first tail");
        assert_eq!(final_text.user_id, "caller-review");
        assert_eq!(final_text.language.as_deref(), Some("en"));
        assert!(final_text.finalized);
        assert!(!final_text.timestamp.is_empty());
        assert!(frames.iter().any(
            |f| matches!(&f.data,FrameData::SttMetadata(m) if m.base.user_turn_strategies.is_some())
        ));
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_explicit_application_strategies_override_provider_metadata() {
    let peer = peer().await;
    let mut config = peer.config.clone();
    config.enable_turn_detection = true;
    let mut user = UserAggregatorConfig {
        empty_user_turn: None,
        ..Default::default()
    };
    user.turn.start_strategies = Some(
        UserTurnStartStrategies::new(vec![UserTurnStartStrategy::vad().with_interruptions(false)])
            .unwrap(),
    );
    user.turn.stop_strategies = Some(
        UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(
            Duration::from_millis(250),
            true,
        )])
        .unwrap(),
    );
    let pair = LLMContextAggregatorPair::new(Context::default(), user, Default::default());
    let raw = Arc::new(Recorder::default());
    let output = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        Arc::new(GradiumSttProcessor::new(
            GradiumSttClient::new(config).unwrap(),
        )),
        raw.clone(),
        pair.user(),
        output.clone(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    emit(&peer, step(1, 0.9)).await;
    emit(&peer, step(2, 0.1)).await;
    emit(&peer, json!({"type":"text","text":"interim sentinel"})).await;
    until(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::ProposedUserStartedSpeaking)),
        1
    );
    assert_eq!(
        output.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
        0
    );
    worker.cancel("independent test").await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_conversation_interruption_preserves_current_input_recognition() {
    let mut peer = peer().await;
    let stt = Arc::new(GradiumSttProcessor::new(
        GradiumSttClient::new(peer.config.clone()).unwrap(),
    ));
    let raw = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt.clone(), raw.clone()])
        .start(Default::default())
        .await
        .unwrap();
    emit(&peer, json!({"type":"text","text":"old"})).await;
    until(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    stt.sender().unwrap().try_flush().unwrap();
    wire(&mut peer).await;
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    until(|| raw.count(|f| matches!(f, FrameData::Interruption)) == 1).await;
    // Conversation interruption cancels assistant output, not current input speech.
    let _ = peer
        .outgoing
        .send(json!({"type":"text","text":"stale tail"}))
        .await;
    tokio::time::sleep(Duration::from_millis(160)).await;
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
        1,
        "conversation interruption dropped current input final"
    );
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))),
        2,
        "conversation interruption dropped current input tail"
    );
    worker.cancel("independent test").await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_end_and_stop_drain_transcript_before_pipeline_terminal() {
    for stop in [false, true] {
        let peer = peer().await;
        let raw = Arc::new(Recorder::default());
        let stt = Arc::new(GradiumSttProcessor::new(
            GradiumSttClient::new(peer.config.clone()).unwrap(),
        ));
        let worker = Pipeline::new(vec![stt, raw.clone()])
            .start(Default::default())
            .await
            .unwrap();
        emit(&peer, json!({"type":"text","text":"last pending text"})).await;
        until(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
        if stop {
            worker
                .sender()
                .send(Frame::new(FrameData::Stop), Direction::Downstream)
                .await
                .unwrap();
        } else {
            worker.end().await.unwrap();
        }
        let (_, report) = bounded(worker.collect()).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        {
            let frames = raw.0.lock().unwrap();
            let final_index = frames
                .iter()
                .position(|f| matches!(f.data, FrameData::TranscriptionDetailed(_)))
                .unwrap();
            let terminal_index = frames
                .iter()
                .position(|f| matches!(f.data, FrameData::End(_) | FrameData::Stop))
                .unwrap();
            assert!(
                final_index < terminal_index,
                "final transcript must precede lifecycle terminal"
            );
        }
        bounded(peer.task).await.unwrap();
    }
}

#[tokio::test]
async fn review_cancel_and_drop_close_actual_socket_without_delayed_final() {
    for drop_session in [false, true] {
        let mut peer = peer().await;
        let mut session = GradiumSttClient::new(peer.config.clone())
            .unwrap()
            .connect()
            .await
            .unwrap();
        emit(&peer, json!({"type":"text","text":"must not finalize"})).await;
        event(&mut session).await.unwrap();
        session.sender().try_flush().unwrap();
        wire(&mut peer).await;
        emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
        assert!(matches!(
            event(&mut session).await,
            Ok(GradiumSttEvent::Flushed { .. })
        ));
        if drop_session {
            drop(session);
        } else {
            session.cancel();
            assert_eq!(event(&mut session).await, Err(GradiumError::Cancelled));
            assert!(session.next().await.is_none());
        }
        bounded(peer.task).await.unwrap();
    }
}

#[tokio::test]
async fn review_missing_flush_ack_has_absolute_deadline_under_busy_provider_input() {
    let mut peer = peer().await;
    peer.config.limits.flush_timeout = Duration::from_millis(80);
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().try_flush().unwrap();
    wire(&mut peer).await;
    let outgoing = peer.outgoing.clone();
    let busy = tokio::spawn(async move {
        for index in 0..200u64 {
            if outgoing.send(step(index, 0.5)).await.is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    });
    assert_eq!(
        event(&mut session).await,
        Err(GradiumError::Timeout("flush acknowledgment"))
    );
    bounded(busy).await.unwrap();
    closed(peer, session).await;
}

#[tokio::test]
async fn review_mute_health_and_cancel_fence_pending_generation() {
    for boundary in ["mute", "health", "cancel"] {
        let mut peer = peer().await;
        let stt = Arc::new(GradiumSttProcessor::new(
            GradiumSttClient::new(peer.config.clone()).unwrap(),
        ));
        let raw = Arc::new(Recorder::default());
        let worker = Pipeline::new(vec![stt.clone(), raw.clone()])
            .start(Default::default())
            .await
            .unwrap();
        emit(
            &peer,
            json!({"type":"text","text":"discard old generation"}),
        )
        .await;
        until(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
        stt.sender().unwrap().try_flush().unwrap();
        wire(&mut peer).await;
        emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
        match boundary {
            "mute" => {
                worker
                    .sender()
                    .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
                    .await
                    .unwrap();
                until(|| raw.count(|f| matches!(f, FrameData::SttMute(true))) == 1).await;
            }
            "health" => {
                let health = worker.processor_health(0).unwrap();
                health.set_usable(false);
                health.set_usable(true);
            }
            "cancel" => worker.cancel("independent generation fence").await.unwrap(),
            _ => unreachable!(),
        }
        let _ = peer
            .outgoing
            .send(json!({"type":"text","text":"late old tail"}))
            .await;
        bounded(peer.task).await.unwrap();
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(
            raw.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
            0,
            "{boundary} allowed delayed old final"
        );
        assert_eq!(
            raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))),
            1,
            "{boundary} allowed late old interim"
        );
        if boundary != "cancel" {
            worker.cancel("cleanup").await.unwrap();
        }
        bounded(worker.collect()).await.unwrap();
    }
}

#[tokio::test]
async fn review_provider_traffic_budget_counts_valid_no_output_messages() {
    let mut peer = peer().await;
    peer.config.limits.max_session_messages = 2;
    let mut session = GradiumSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    for index in 0..3 {
        emit(&peer, step(index, 0.5)).await;
    }
    assert_eq!(event(&mut session).await, Err(GradiumError::TooLarge));
    closed(peer, session).await;
}

#[tokio::test]
async fn review_unmute_replaces_provider_generation_without_replaying_old_audio_or_text() {
    let mut peer = peer_sessions(2).await;
    let stt = Arc::new(GradiumSttProcessor::new(
        GradiumSttClient::new(peer.config.clone()).unwrap(),
    ));
    let raw = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt.clone(), raw.clone()])
        .start(Default::default())
        .await
        .unwrap();
    bounded(peer.setup.recv()).await.unwrap();
    let previous_sender = stt.sender().unwrap();
    previous_sender
        .try_send_audio(pcm(16000, vec![17; 32]))
        .unwrap();
    emit(&peer, json!({"type":"text","text":"old partial"})).await;
    until(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    previous_sender.try_flush().unwrap();
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"flush","flush_id":"1"})
    );
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
        .await
        .unwrap();
    until(|| raw.count(|f| matches!(f, FrameData::SttMute(true))) == 1).await;
    assert!(previous_sender.is_closed());
    worker
        .sender()
        .send(Frame::new(FrameData::SttMute(false)), Direction::Downstream)
        .await
        .unwrap();
    bounded(peer.setup.recv()).await.unwrap();
    until(|| raw.count(|f| matches!(f, FrameData::SttMute(false))) == 1).await;
    let current_sender = stt.sender().unwrap();
    assert_eq!(current_sender.statistics().accepted_input_bytes, 0);
    emit(&peer, json!({"type":"text","text":"new generation"})).await;
    until(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 2).await;
    current_sender.try_flush().unwrap();
    assert_eq!(
        wire(&mut peer).await,
        json!({"type":"flush","flush_id":"1"}),
        "old unsent audio was replayed"
    );
    emit(&peer, json!({"type":"flushed","flush_id":"1"})).await;
    until(|| raw.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))) == 1).await;
    assert!(raw.0.lock().unwrap().iter().any(
        |f| matches!(&f.data,FrameData::TranscriptionDetailed(t) if t.text.text=="new generation")
    ));
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn review_full_pipeline_output_fails_boundedly_and_closes_owned_socket() {
    let peer = peer().await;
    let stt = Arc::new(GradiumSttProcessor::new(
        GradiumSttClient::new(peer.config.clone()).unwrap(),
    ));
    // Metadata and Start fill this boundary. Deliberately do not consume it.
    let mut worker = Pipeline::new(vec![stt.clone()])
        .start(PipelineConfig {
            output_capacity: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    let sender = stt.sender().unwrap();
    emit(&peer, json!({"type":"text","text":"cannot enqueue"})).await;
    assert!(matches!(
        bounded(worker.join()).await,
        Err(PipelineError::Backpressure(_))
    ));
    bounded(peer.task).await.unwrap();
    assert!(
        sender.is_closed(),
        "output failure leaked provider ownership"
    );
}
