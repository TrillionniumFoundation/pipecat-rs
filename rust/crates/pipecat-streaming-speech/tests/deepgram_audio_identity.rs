//! Real loopback protocol checks for the input-audio family and the supervisor's
//! latest consumed speaker. Provider results do not identify individual PCM frames.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

const DEADLINE: Duration = Duration::from_secs(4);
type Socket = WebSocketStream<TcpStream>;
type Receipt = (u64, Result<(), ProcessorError>);

// A process receipt is stronger than pipeline input admission. Finalize on the
// same supervisor FIFO additionally proves that earlier empty audio was consumed.
struct ProcessedStt {
    stt: Arc<DeepgramSttProcessor>,
    receipts: mpsc::UnboundedSender<Receipt>,
}
#[async_trait]
impl FrameProcessor for ProcessedStt {
    fn name(&self) -> &str {
        self.stt.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.stt.health()
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.stt.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let id = frame.id;
        let result = self.stt.process(frame, direction, ctx).await;
        let _ = self.receipts.send((id, result.clone()));
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.stt.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.stt.cleanup().await
    }
}
struct Harness {
    worker: PipelineWorker,
    stt: Arc<DeepgramSttProcessor>,
    receipts: mpsc::UnboundedReceiver<Receipt>,
    frames: Vec<DirectedFrame>,
}
impl Harness {
    async fn start(config: DeepgramConfig, fixed: Option<&str>, passthrough: bool) -> Self {
        let mut stt = DeepgramSttProcessor::new(DeepgramClient::new(config).unwrap())
            .with_audio_passthrough(passthrough)
            .with_reconnect_policy(ReconnectPolicy {
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            });
        if let Some(id) = fixed {
            stt = stt.with_user_id(id);
        }
        let stt = Arc::new(stt);
        let (tx, receipts) = mpsc::unbounded_channel();
        let worker = Pipeline::new(vec![Arc::new(ProcessedStt {
            stt: stt.clone(),
            receipts: tx,
        })])
        .start(PipelineConfig {
            start: StartFrame {
                enable_usage_metrics: true,
                ..Default::default()
            },
            output_capacity: 512,
            ..Default::default()
        })
        .await
        .unwrap();
        Self {
            worker,
            stt,
            receipts,
            frames: vec![],
        }
    }
    async fn send(&mut self, frame: Frame, direction: Direction) -> Result<(), ProcessorError> {
        let id = frame.id;
        self.worker.sender().send(frame, direction).await.unwrap();
        bounded(async {
            loop {
                let (processed, result) = self.receipts.recv().await.expect("processor stopped");
                if processed == id {
                    return result;
                }
            }
        })
        .await
    }
    async fn data(&mut self, data: FrameData) -> Result<(), ProcessorError> {
        self.send(Frame::new(data), Direction::Downstream).await
    }
    async fn finalize(&mut self) {
        self.data(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.02,
            timestamp: 1.0,
        }))
        .await
        .unwrap();
    }
    async fn pair(&mut self) {
        for _ in 0..2 {
            loop {
                let frame = bounded(self.worker.recv()).await.expect("pipeline stopped");
                let is_transcript = matches!(
                    frame.frame.data,
                    FrameData::InterimTranscription(_) | FrameData::Transcription(_)
                );
                self.frames.push(frame);
                if is_transcript {
                    break;
                }
            }
        }
    }
    async fn finish(mut self, cancel: bool) -> (Vec<DirectedFrame>, WorkerReport) {
        if cancel {
            self.worker.cancel("identity fixture").await.unwrap();
        } else {
            self.worker.end().await.unwrap();
        }
        let (tail, report) = bounded(self.worker.collect()).await.unwrap();
        self.frames.extend(tail);
        assert_eq!(
            report.state,
            if cancel {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        (self.frames, report)
    }
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(DEADLINE, future)
        .await
        .expect("loopback fixture timed out")
}
async fn fixture() -> (TcpListener, DeepgramConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramConfig::new("local-fixture-key");
    config.endpoint = format!("ws://{}/listen", listener.local_addr().unwrap());
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_secs(2);
    (listener, config)
}
async fn accept(listener: &TcpListener) -> Socket {
    bounded(accept_async(bounded(listener.accept()).await.unwrap().0))
        .await
        .unwrap()
}
async fn wire(socket: &mut Socket) -> Message {
    bounded(socket.next())
        .await
        .expect("socket closed")
        .unwrap()
}
async fn control(socket: &mut Socket, kind: &str) {
    let value: Value = serde_json::from_str(wire(socket).await.to_text().unwrap()).unwrap();
    assert_eq!(value, json!({"type": kind}));
}
// Read until an actual FIFO wire barrier, returning bytes for assertions only
// after both native worker and peer have drained. The old implementation still
// completes the fixture, so a parity red identifies missing PCM/identity directly.
async fn through_finalize(socket: &mut Socket) -> Vec<Vec<u8>> {
    let mut packets = vec![];
    loop {
        match wire(socket).await {
            Message::Binary(bytes) => packets.push(bytes.to_vec()),
            Message::Text(value) => {
                assert_eq!(
                    serde_json::from_str::<Value>(&value).unwrap(),
                    json!({"type":"Finalize"})
                );
                return packets;
            }
            other => panic!("unexpected wire frame: {other:?}"),
        }
    }
}
async fn results(socket: &mut Socket, stage: &str) {
    for final_result in [false, true] {
        socket.send(Message::Text(json!({
            "type": "Results", "is_final": final_result, "from_finalize": final_result,
            "speech_final": final_result, "fixture": stage,
            "channel": {"alternatives": [{"transcript": format!("{stage}-{final_result}"), "languages": ["en"]}]}
        }).to_string().into())).await.unwrap();
    }
}
fn audio(bytes: Vec<u8>, user: Option<&str>) -> FrameData {
    let mut audio = AudioRawFrame::new(bytes, 16_000, 1).unwrap();
    audio.context_id = Some("opaque-pcm-context".into());
    match user {
        Some(user_id) => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: user_id.into(),
        }),
        None => FrameData::InputAudio(audio),
    }
}
fn envelope(data: FrameData, tag: &LocalFrameTag) -> Frame {
    let mut frame = Frame::new(data);
    frame.name = "unchanged-original-audio".into();
    frame.pts = Some(17_000);
    frame
        .metadata
        .insert("nested".into(), json!({"preserve": [true, 2, "three"]}));
    frame.transport_source = Some("microphone-track".into());
    frame.transport_destination = Some("monitor-track".into());
    frame.broadcast_sibling_id = Some(90_000);
    frame.interruptible = false;
    tag.mark(&mut frame).unwrap();
    frame
}
fn passthrough(frames: &[DirectedFrame], originals: &[Frame], tag: &LocalFrameTag, enabled: bool) {
    for original in originals {
        let found: Vec<_> = frames
            .iter()
            .filter(|f| f.frame.id == original.id)
            .collect();
        assert_eq!(found.len(), usize::from(enabled));
        if enabled {
            assert_eq!(found[0].direction, Direction::Downstream);
            assert_eq!(found[0].frame, *original);
            assert_eq!(
                found[0].frame.observation_token(),
                original.observation_token()
            );
            assert!(tag.contains(&found[0].frame));
        }
    }
}
fn usage(frames: &[DirectedFrame]) -> Vec<f64> {
    frames
        .iter()
        .flat_map(|f| match &f.frame.data {
            FrameData::Metrics(metrics) => metrics.as_slice(),
            _ => &[],
        })
        .filter_map(|m| match m {
            Metric::SttUsage { audio_seconds, .. } => Some(*audio_seconds),
            _ => None,
        })
        .collect()
}

fn assert_pairs(frames: &[DirectedFrame], stages: &[(&str, &str)]) {
    let transcripts: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::InterimTranscription(value) => Some((frame, value, false)),
            FrameData::Transcription(value) => Some((frame, value, true)),
            _ => None,
        })
        .collect();
    assert_eq!(transcripts.len(), stages.len() * 2);
    for ((frame, value, actual_final), (stage, speaker, final_result)) in transcripts.iter().zip(
        stages
            .iter()
            .flat_map(|(stage, speaker)| [(*stage, *speaker, false), (*stage, *speaker, true)]),
    ) {
        assert_eq!(*actual_final, final_result);
        assert_eq!(value.text, format!("{stage}-{final_result}"));
        assert_eq!(value.user_id, speaker);
        assert_eq!(value.language.as_deref(), Some("en"));
        assert_eq!(value.finalized, final_result);
        assert_eq!(value.result.as_ref().unwrap()["fixture"], stage);
        assert_eq!(frame.frame.metadata["speech_final"], json!(final_result));
        assert!(value.timestamp.contains('T'));
        assert_eq!(frame.direction, Direction::Downstream);
    }
}

#[tokio::test]
async fn default_current_speaker_tracks_user_plain_and_empty_audio() {
    let (listener, config) = fixture().await;
    let server = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let mut packets = vec![];
        for index in 0..4 {
            packets.push(through_finalize(&mut socket).await);
            results(&mut socket, &format!("stage-{index}")).await;
        }
        control(&mut socket, "CloseStream").await;
        socket.close(None).await.unwrap();
        packets
    });
    let mut harness = Harness::start(config, None, true).await;
    let tag = LocalFrameTag::new();
    let originals = vec![
        envelope(audio(vec![1, 0, 2, 0], Some("alice")), &tag),
        envelope(audio(vec![3, 0, 4, 0], None), &tag),
        envelope(audio(vec![], Some("bob")), &tag),
        envelope(audio(vec![], None), &tag),
    ];
    for frame in &originals {
        harness
            .send(frame.clone(), Direction::Downstream)
            .await
            .unwrap();
        harness.finalize().await;
        harness.pair().await;
    }
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        packets,
        vec![
            vec![vec![1, 0, 2, 0]],
            vec![vec![3, 0, 4, 0]],
            vec![],
            vec![]
        ]
    );
    assert_pairs(
        &frames,
        &[
            ("stage-0", "alice"),
            ("stage-1", ""),
            ("stage-2", "bob"),
            ("stage-3", ""),
        ],
    );
    passthrough(&frames, &originals, &tag, true);
    assert_eq!(usage(&frames), vec![2.0 / 16_000.0; 2]);
}

#[tokio::test]
async fn explicit_fixed_speaker_including_empty_survives_audio_family_with_passthrough_disabled() {
    for fixed in ["fixed-speaker", ""] {
        let (listener, config) = fixture().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            let mut packets = vec![];
            for index in 0..3 {
                packets.push(through_finalize(&mut socket).await);
                results(&mut socket, &format!("fixed-{index}")).await;
            }
            control(&mut socket, "CloseStream").await;
            socket.close(None).await.unwrap();
            packets
        });
        let mut harness = Harness::start(config, Some(fixed), false).await;
        let tag = LocalFrameTag::new();
        let originals = vec![
            envelope(audio(vec![5, 0], Some("alice")), &tag),
            envelope(audio(vec![5, 0], None), &tag),
            envelope(audio(vec![], Some("bob")), &tag),
        ];
        for frame in &originals {
            harness
                .send(frame.clone(), Direction::Downstream)
                .await
                .unwrap();
            harness.finalize().await;
            harness.pair().await;
        }
        let (frames, report) = harness.finish(false).await;
        let packets = bounded(server).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(packets, vec![vec![vec![5, 0]], vec![vec![5, 0]], vec![]]);
        assert_pairs(
            &frames,
            &[("fixed-0", fixed), ("fixed-1", fixed), ("fixed-2", fixed)],
        );
        passthrough(&frames, &originals, &tag, false);
        assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
    }
}
#[tokio::test]
async fn muted_upstream_and_output_audio_preserve_current_speaker_and_original_envelopes() {
    let (listener, config) = fixture().await;
    let server = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let first = through_finalize(&mut socket).await;
        results(&mut socket, "seed").await;
        let second = through_finalize(&mut socket).await;
        results(&mut socket, "unchanged").await;
        control(&mut socket, "CloseStream").await;
        socket.close(None).await.unwrap();
        [first, second]
    });
    let mut harness = Harness::start(config, None, true).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    harness.data(FrameData::SttMute(true)).await.unwrap();
    let tag = LocalFrameTag::new();
    let mut originals = vec![];
    for (data, direction) in [
        (audio(vec![2, 0], Some("muted-bob")), Direction::Downstream),
        (audio(vec![], Some("muted-empty")), Direction::Downstream),
        (audio(vec![], None), Direction::Downstream),
    ] {
        let frame = envelope(data, &tag);
        originals.push((frame.clone(), direction));
        harness.send(frame, direction).await.unwrap();
    }
    harness.data(FrameData::SttMute(false)).await.unwrap();
    for (data, direction) in [
        (audio(vec![3, 0], Some("upstream-bob")), Direction::Upstream),
        (audio(vec![], None), Direction::Upstream),
        (
            FrameData::OutputAudio(AudioRawFrame::new(vec![4, 0], 16_000, 1).unwrap()),
            Direction::Downstream,
        ),
        (
            FrameData::TtsAudio(AudioRawFrame::new(vec![5, 0], 16_000, 1).unwrap()),
            Direction::Downstream,
        ),
    ] {
        let frame = envelope(data, &tag);
        originals.push((frame.clone(), direction));
        harness.send(frame, direction).await.unwrap();
    }
    harness.data(FrameData::Interruption).await.unwrap();
    harness.finalize().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(packets, [vec![vec![1, 0]], vec![]]);
    assert_pairs(&frames, &[("seed", "alice"), ("unchanged", "alice")]);
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);
    for (original, direction) in originals {
        let found: Vec<_> = frames
            .iter()
            .filter(|f| f.frame.id == original.id)
            .collect();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].direction, direction);
        assert_eq!(found[0].frame, original);
        assert_eq!(
            found[0].frame.observation_token(),
            original.observation_token()
        );
        assert!(tag.contains(&found[0].frame));
    }
}

#[tokio::test]
async fn dynamic_pcm_and_utf8_speaker_share_byte_budget_without_relabeling_on_rejection() {
    // Large enough for real response JSON; tiny budgets would reject the
    // provider's own response before exercising processor speaker admission.
    const BUDGET: usize = 1024;
    let (listener, mut config) = fixture().await;
    config.limits.max_message_bytes = BUDGET;
    let server = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let mut packets = vec![];
        for stage in ["exact", "rejected", "empty-exact", "empty-rejected"] {
            packets.push(through_finalize(&mut socket).await);
            results(&mut socket, stage).await;
        }
        control(&mut socket, "CloseStream").await;
        socket.close(None).await.unwrap();
        packets
    });
    let mut harness = Harness::start(config, None, false).await;
    let exact_id = "é".repeat((BUDGET - 2) / 2);
    harness
        .data(audio(vec![6, 0], Some(&exact_id)))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    for data in [
        audio(vec![7, 0], Some(&"a".repeat(BUDGET - 1))),
        audio(vec![7, 0], Some(&"é".repeat(BUDGET / 2))),
        audio(vec![0; BUDGET + 2], None),
    ] {
        let error = harness.data(data).await.unwrap_err();
        assert_eq!(error.category, ErrorCategory::Application);
        assert!(error.message.contains("configured limit"));
    }
    harness.finalize().await;
    harness.pair().await;
    let exact_empty_id = "e".repeat(BUDGET);
    harness
        .data(audio(vec![], Some(&exact_empty_id)))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    assert!(harness
        .data(audio(vec![], Some(&"x".repeat(BUDGET + 1))))
        .await
        .is_err());
    harness.finalize().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert_eq!(report.errors.len(), 4);
    assert_eq!(packets, vec![vec![vec![6, 0]], vec![], vec![], vec![]]);
    assert_pairs(
        &frames,
        &[
            ("exact", &exact_id),
            ("rejected", &exact_id),
            ("empty-exact", &exact_empty_id),
            ("empty-rejected", &exact_empty_id),
        ],
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);
}

#[tokio::test]
async fn fixed_override_does_not_charge_or_retain_ignored_frame_speaker_bytes() {
    let (listener, mut config) = fixture().await;
    config.limits.max_message_bytes = 1024;
    let server = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let packets = through_finalize(&mut socket).await;
        results(&mut socket, "fixed-budget").await;
        control(&mut socket, "CloseStream").await;
        socket.close(None).await.unwrap();
        packets
    });
    let fixed = "legacy-fixed".repeat(100);
    let mut harness = Harness::start(config, Some(&fixed), false).await;
    harness
        .data(audio(vec![0; 1024], Some(&"ignored".repeat(1000))))
        .await
        .unwrap();
    harness
        .data(audio(vec![], Some(&"ignored-empty".repeat(1000))))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(packets, vec![vec![0; 1024]]);
    assert_pairs(&frames, &[("fixed-budget", &fixed)]);
    assert_eq!(usage(&frames), vec![512.0 / 16_000.0]);
}

#[tokio::test]
async fn malformed_user_pcm_preserves_invalid_request_and_retires_socket_without_wire_audio() {
    for (bytes, rate, channels) in [
        (vec![1], 16_000, 1),
        (vec![1, 0], 8_000, 1),
        (vec![1, 0, 2, 0], 16_000, 2),
    ] {
        let (listener, config) = fixture().await;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            let mut packets = vec![];
            while let Some(Ok(message)) = bounded(socket.next()).await {
                if let Message::Binary(bytes) = message {
                    packets.push(bytes.to_vec());
                } else if matches!(message, Message::Close(_)) {
                    break;
                }
            }
            packets
        });
        let mut harness = Harness::start(config, None, false).await;
        let error = harness
            .data(FrameData::UserAudio(UserAudioRawFrame {
                audio: AudioRawFrame {
                    audio: bytes,
                    sample_rate: rate,
                    num_channels: channels,
                    context_id: None,
                },
                user_id: "invalid-speaker".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(error.category, ErrorCategory::InvalidRequest);
        // Consume the runtime error before testing its health effect.
        loop {
            let frame = bounded(harness.worker.recv()).await.unwrap();
            let is_error = matches!(frame.frame.data, FrameData::Error(_));
            harness.frames.push(frame);
            if is_error {
                break;
            }
        }
        assert!(!harness.stt.health().unwrap().is_usable());
        harness
            .data(audio(vec![2, 0], Some("unusable-speaker")))
            .await
            .unwrap();
        let (frames, report) = harness.finish(false).await;
        assert_eq!(report.errors.len(), 1);
        assert!(bounded(server).await.unwrap().is_empty());
        assert!(usage(&frames).is_empty());
        assert_pairs(&frames, &[]);
    }
}

#[tokio::test]
async fn empty_identity_seeds_idle_session_without_connection_and_health_generation_resets_it() {
    let (listener, config) = fixture().await;
    let (idle_tx, idle_rx) = oneshot::channel();
    let (checked_tx, checked_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_packets = through_finalize(&mut first).await;
        results(&mut first, "old-generation").await;
        while let Some(Ok(message)) = bounded(first.next()).await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
        idle_rx.await.unwrap();
        // Empty usable input must not even initiate a TCP handshake.
        assert!(
            tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err()
        );
        checked_tx.send(()).unwrap();
        let mut second = accept(&listener).await;
        let second_packets = through_finalize(&mut second).await;
        results(&mut second, "idle-seed").await;
        while let Some(Ok(message)) = bounded(second.next()).await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
        let mut third = accept(&listener).await;
        let third_packets = through_finalize(&mut third).await;
        results(&mut third, "fresh-default").await;
        control(&mut third, "CloseStream").await;
        third.close(None).await.unwrap();
        [first_packets, second_packets, third_packets]
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    let health = harness.stt.health().unwrap();
    health.set_usable(false);
    harness
        .data(audio(vec![], Some("discarded-unusable")))
        .await
        .unwrap();
    health.set_usable(true);
    // Preserve the old empty geometry bypass, including zero channels/rate.
    harness
        .data(FrameData::UserAudio(UserAudioRawFrame {
            audio: AudioRawFrame {
                audio: vec![],
                sample_rate: 0,
                num_channels: 0,
                context_id: None,
            },
            user_id: "idle-bob".into(),
        }))
        .await
        .unwrap();
    idle_tx.send(()).unwrap();
    bounded(checked_rx).await.unwrap();
    harness.finalize().await;
    harness.pair().await;
    health.set_usable(false);
    harness
        .data(audio(vec![], Some("discarded-again")))
        .await
        .unwrap();
    health.set_usable(true);
    harness.finalize().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(packets, [vec![vec![1, 0]], vec![], vec![]]);
    assert_pairs(
        &frames,
        &[
            ("old-generation", "alice"),
            ("idle-seed", "idle-bob"),
            ("fresh-default", ""),
        ],
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);
}

#[tokio::test]
async fn settings_drain_keeps_previous_current_speaker_until_queued_audio_is_consumed() {
    let (listener, mut config) = fixture().await;
    config.limits.queue_capacity = 2;
    let (draining_tx, draining_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_packets = through_finalize(&mut first).await;
        results(&mut first, "before-settings").await;
        control(&mut first, "CloseStream").await;
        draining_tx.send(()).unwrap();
        release_rx.await.unwrap();
        // Old socket has no pending Finalize; inspect this drain pair for
        // current speaker without changing existing finalized semantics.
        results(&mut first, "old-drain").await;
        first.close(None).await.unwrap();
        let mut second = accept(&listener).await;
        let next_packets = through_finalize(&mut second).await;
        results(&mut second, "next-socket").await;
        control(&mut second, "CloseStream").await;
        second.close(None).await.unwrap();
        [first_packets, next_packets]
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    // The public synchronous method admits Update before any following audio;
    // normal settings frames otherwise can be overtaken by system UserAudio.
    harness
        .stt
        .update_settings(&SttSettings {
            language: Setting::Value("es".into()),
            ..Default::default()
        })
        .unwrap();
    bounded(draining_rx).await.unwrap();
    harness.data(audio(vec![2, 0], Some("bob"))).await.unwrap();
    harness.finalize().await;
    let rejected = harness
        .data(audio(vec![], Some("rejected-mallory")))
        .await
        .unwrap_err();
    assert_eq!(rejected.category, ErrorCategory::Application);
    assert!(rejected.message.contains("queue is full"));
    release_tx.send(()).unwrap();
    harness.pair().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(packets, [vec![vec![1, 0]], vec![vec![2, 0]]]);
    let values: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Transcription(value) | FrameData::InterimTranscription(value) => {
                Some((value.text.as_str(), value.user_id.as_str()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        values,
        [
            ("before-settings-false", "alice"),
            ("before-settings-true", "alice"),
            ("old-drain-false", "alice"),
            ("old-drain-true", "alice"),
            ("next-socket-false", "bob"),
            ("next-socket-true", "bob")
        ]
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
}

#[tokio::test]
async fn reconnect_preserves_current_speaker_without_replaying_pcm() {
    let (listener, config) = fixture().await;
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_packets = through_finalize(&mut first).await;
        results(&mut first, "before-reconnect").await;
        first.close(None).await.unwrap();
        let mut second = accept(&listener).await;
        closed_tx.send(()).unwrap();
        let second_packets = through_finalize(&mut second).await;
        results(&mut second, "after-reconnect").await;
        control(&mut second, "CloseStream").await;
        second.close(None).await.unwrap();
        [first_packets, second_packets]
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    bounded(closed_rx).await.unwrap();
    harness.finalize().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].category, ErrorCategory::Connectivity);
    assert_eq!(packets, [vec![vec![1, 0]], vec![]]);
    assert_pairs(
        &frames,
        &[("before-reconnect", "alice"), ("after-reconnect", "alice")],
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);
}

#[tokio::test]
async fn cancelling_reconnect_discards_bounded_queued_speakers_and_pcm() {
    let (listener, mut config) = fixture().await;
    config.limits.queue_capacity = 2;
    let (pending_tx, pending_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let packets = through_finalize(&mut first).await;
        results(&mut first, "before-cancel").await;
        first.close(None).await.unwrap();
        let (mut pending, _) = bounded(listener.accept()).await.unwrap();
        pending_tx.send(()).unwrap();
        // A held real TCP handshake makes supervisor queue occupancy exact.
        use tokio::io::AsyncReadExt;
        let mut request = vec![];
        bounded(pending.read_to_end(&mut request)).await.unwrap();
        assert!(request.starts_with(b"GET /listen?"));
        packets
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    bounded(pending_rx).await.unwrap();
    harness
        .data(audio(vec![2, 0], Some("queued-bob")))
        .await
        .unwrap();
    harness
        .data(audio(vec![], Some("queued-carol")))
        .await
        .unwrap();
    let rejected = harness
        .data(audio(vec![], Some("rejected-mallory")))
        .await
        .unwrap_err();
    assert_eq!(rejected.category, ErrorCategory::Application);
    assert!(rejected.message.contains("queue is full"));
    let (frames, report) = harness.finish(true).await;
    assert_eq!(report.errors.len(), 2);
    assert_eq!(bounded(server).await.unwrap(), vec![vec![1, 0]]);
    assert_pairs(&frames, &[("before-cancel", "alice")]);
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
}

// The peer has received Finish from an effective settings rotation before it
// holds that old socket open. This pins the supervisor inside its drain while
// exactly Q ordinary audio commands occupy its input queue. Empty fixed input
// must remain a no-op; dynamic identity-only input still requires FIFO space.
async fn assert_saturated_empty_contract(fixed: Option<&str>, passthrough_enabled: bool) {
    const Q: usize = 2;
    const BUDGET: usize = 1024;
    let (listener, mut config) = fixture().await;
    config.limits.queue_capacity = Q;
    config.limits.max_message_bytes = BUDGET;
    let (draining_tx, draining_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_packets = through_finalize(&mut first).await;
        results(&mut first, "saturated-seed").await;
        control(&mut first, "CloseStream").await;
        draining_tx.send(()).unwrap();
        release_rx.await.unwrap();
        results(&mut first, "saturated-old-drain").await;
        first.close(None).await.unwrap();
        let mut second = accept(&listener).await;
        let mut second_packets = vec![];
        loop {
            match wire(&mut second).await {
                Message::Binary(bytes) => second_packets.push(bytes.to_vec()),
                Message::Text(value) => {
                    assert_eq!(
                        serde_json::from_str::<Value>(&value).unwrap(),
                        json!({"type":"CloseStream"})
                    );
                    break;
                }
                other => panic!("unexpected wire frame: {other:?}"),
            }
        }
        results(&mut second, "saturated-next-socket").await;
        second.close(None).await.unwrap();
        [first_packets, second_packets]
    });
    let mut harness = Harness::start(config, fixed, passthrough_enabled).await;
    harness
        .data(audio(vec![1, 0], Some("seed-alice")))
        .await
        .unwrap();
    harness.finalize().await;
    harness.pair().await;
    harness
        .stt
        .update_settings(&SttSettings {
            language: Setting::Value("es".into()),
            ..Default::default()
        })
        .unwrap();
    bounded(draining_rx).await.unwrap();

    let tag = LocalFrameTag::new();
    let queued = [
        envelope(audio(vec![2, 0], Some("queued-bob")), &tag),
        envelope(audio(vec![3, 0], Some("queued-bob")), &tag),
    ];
    let mut admitted = vec![];
    for frame in &queued {
        admitted.push(harness.send(frame.clone(), Direction::Downstream).await);
    }
    // Preserve the original empty geometry bypass. The fixed override ignores
    // even an over-budget input ID; the dynamic probe fits the identity budget
    // exactly so that its rejection can only come from the saturated queue.
    let empty_audio = AudioRawFrame {
        audio: vec![],
        sample_rate: 0,
        num_channels: 0,
        context_id: Some("empty-context".into()),
    };
    let empty = [
        envelope(FrameData::InputAudio(empty_audio.clone()), &tag),
        envelope(
            FrameData::UserAudio(UserAudioRawFrame {
                audio: empty_audio,
                user_id: "m".repeat(if fixed.is_some() { BUDGET + 1 } else { BUDGET }),
            }),
            &tag,
        ),
    ];
    let mut empty_receipts = vec![];
    for frame in &empty {
        empty_receipts.push(harness.send(frame.clone(), Direction::Downstream).await);
    }
    let overflow_frame = envelope(audio(vec![9, 0], Some("overflow-mallory")), &tag);
    let overflow = harness
        .send(overflow_frame.clone(), Direction::Downstream)
        .await;

    // Save outcomes without asserting: even the old implementation must release
    // the held peer and fully End/collect/join before the compatibility red.
    release_tx.send(()).unwrap();
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    eprintln!("saturated fixed={fixed:?} passthrough={passthrough_enabled}: admitted={admitted:?}; empty={empty_receipts:?}; overflow={overflow:?}; packets={packets:?}; errors={:?}", report.errors);

    // Plain InputAudio comes first: this is the direct b7 empty no-op contract.
    // UserAudio and disabled passthrough exercise the qualified audio-family
    // extension rather than claiming that b7 already recognized UserAudio.
    for (frame, result) in empty.iter().zip(&empty_receipts) {
        if fixed.is_some() {
            assert!(
                result.is_ok(),
                "fixed empty frame {} must bypass full queue: {result:?}",
                frame.id
            );
        } else {
            assert_saturated_backpressure(result);
        }
    }
    assert!(admitted.iter().all(Result::is_ok), "{admitted:?}");
    assert_saturated_backpressure(&overflow);
    assert_eq!(report.errors.len(), if fixed.is_some() { 1 } else { 3 });
    assert_eq!(packets, [vec![vec![1, 0]], vec![vec![2, 0], vec![3, 0]]]);
    let transcripts: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::InterimTranscription(value) | FrameData::Transcription(value) => {
                Some((value.text.as_str(), value.user_id.as_str()))
            }
            _ => None,
        })
        .collect();
    let old = fixed.unwrap_or("seed-alice");
    let next = fixed.unwrap_or("queued-bob");
    assert_eq!(
        transcripts,
        [
            ("saturated-seed-false", old),
            ("saturated-seed-true", old),
            ("saturated-old-drain-false", old),
            ("saturated-old-drain-true", old),
            ("saturated-next-socket-false", next),
            ("saturated-next-socket-true", next),
        ]
    );
    passthrough(&frames, &queued, &tag, passthrough_enabled);
    passthrough(
        &frames,
        &empty,
        &tag,
        fixed.is_some() && passthrough_enabled,
    );
    passthrough(&frames, &[overflow_frame], &tag, false);
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0; Q + 1]);
}
fn assert_saturated_backpressure(result: &Result<(), ProcessorError>) {
    let error = result
        .as_ref()
        .expect_err("saturated command must be rejected");
    assert_eq!(error.category, ErrorCategory::Application);
    assert!(error.message.contains("queue is full"), "{error:?}");
}

#[tokio::test]
async fn saturated_fixed_nonempty_id_empty_audio_preserves_passthrough() {
    assert_saturated_empty_contract(Some("fixed-speaker"), true).await;
}
#[tokio::test]
async fn saturated_fixed_empty_id_empty_audio_preserves_passthrough() {
    assert_saturated_empty_contract(Some(""), true).await;
}
#[tokio::test]
async fn saturated_fixed_nonempty_id_empty_audio_keeps_passthrough_disabled() {
    assert_saturated_empty_contract(Some("fixed-speaker"), false).await;
}
#[tokio::test]
async fn saturated_fixed_empty_id_empty_audio_keeps_passthrough_disabled() {
    assert_saturated_empty_contract(Some(""), false).await;
}
#[tokio::test]
async fn saturated_dynamic_empty_audio_rejects_without_relabeling() {
    assert_saturated_empty_contract(None, true).await;
}
