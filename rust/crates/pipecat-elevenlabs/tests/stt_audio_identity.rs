//! Real loopback protocol checks for the input-audio family and the supervisor's
//! latest consumed speaker. Provider results do not identify individual PCM frames.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_elevenlabs::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};

const DEADLINE: Duration = Duration::from_secs(4);
type Socket = WebSocketStream<TcpStream>;
type Receipt = (u64, Result<(), ProcessorError>);

// A process receipt proves command admission. Manual Commit on the same
// supervisor FIFO additionally proves that earlier empty audio was consumed.
struct ProcessedStt {
    stt: Arc<ElevenLabsSttProcessor>,
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
    stt: Arc<ElevenLabsSttProcessor>,
    receipts: mpsc::UnboundedReceiver<Receipt>,
    frames: Vec<DirectedFrame>,
}
impl Harness {
    async fn start(config: ElevenLabsSttConfig, fixed: Option<&str>, passthrough: bool) -> Self {
        let mut stt = ElevenLabsSttProcessor::new(ElevenLabsSttClient::new(config).unwrap())
            .with_audio_passthrough(passthrough);
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
    async fn commit(&mut self) {
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
async fn fixture() -> (TcpListener, ElevenLabsSttConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("local-fixture-key");
    config.endpoint = format!(
        "ws://{}/v1/speech-to-text/realtime",
        listener.local_addr().unwrap()
    );
    config.keepalive_interval = Duration::from_secs(3600);
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_secs(2);
    (listener, config)
}
async fn accept(listener: &TcpListener) -> Socket {
    accept_strategy(listener, "manual").await
}
#[allow(clippy::result_large_err)]
async fn accept_strategy(listener: &TcpListener, strategy: &'static str) -> Socket {
    bounded(accept_hdr_async(
        bounded(listener.accept()).await.unwrap().0,
        move |request: &Request, response: Response| {
            assert_eq!(request.uri().path(), "/v1/speech-to-text/realtime");
            assert_eq!(request.headers()["xi-api-key"], "local-fixture-key");
            let query: std::collections::BTreeMap<_, _> =
                url::form_urlencoded::parse(request.uri().query().unwrap().as_bytes())
                    .into_owned()
                    .collect();
            assert_eq!(query["audio_format"], "pcm_16000");
            assert_eq!(query["commit_strategy"], strategy);
            Ok(response)
        },
    ))
    .await
    .unwrap()
}
async fn wire(socket: &mut Socket) -> Message {
    bounded(socket.next())
        .await
        .expect("socket closed")
        .unwrap()
}
// Read through the actual manual-commit wire barrier. Empty identity updates
// must not become either audio chunks or extra provider commits. Keepalive is
// disabled for this bounded fixture by its long configured interval.
async fn through_commit(socket: &mut Socket) -> Vec<Vec<u8>> {
    let mut packets = vec![];
    loop {
        let message = wire(socket).await;
        let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
        assert_eq!(value["message_type"], "input_audio_chunk");
        assert_eq!(value["sample_rate"], 16_000);
        let bytes = STANDARD
            .decode(value["audio_base_64"].as_str().unwrap())
            .unwrap();
        if value["commit"] == true {
            assert!(bytes.is_empty());
            return packets;
        }
        assert_eq!(value["commit"], false);
        assert!(
            !bytes.is_empty(),
            "identity-only input created a provider audio message"
        );
        packets.push(bytes);
    }
}
async fn close(socket: &mut Socket) {
    assert!(matches!(wire(socket).await, Message::Close(_)));
}
async fn results(socket: &mut Socket, stage: &str) {
    for final_result in [false, true] {
        socket.send(Message::Text(json!({
            "message_type": if final_result { "committed_transcript" } else { "partial_transcript" },
            "text": format!("{stage}-{final_result}"), "language_code": "en", "fixture": stage,
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
            packets.push(through_commit(&mut socket).await);
            results(&mut socket, &format!("stage-{index}")).await;
        }
        close(&mut socket).await;
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
        harness.commit().await;
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
                packets.push(through_commit(&mut socket).await);
                results(&mut socket, &format!("fixed-{index}")).await;
            }
            close(&mut socket).await;
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
            harness.commit().await;
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
        let first = through_commit(&mut socket).await;
        results(&mut socket, "seed").await;
        let second = through_commit(&mut socket).await;
        results(&mut socket, "unchanged").await;
        close(&mut socket).await;
        [first, second]
    });
    let mut harness = Harness::start(config, None, true).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.commit().await;
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
    harness.commit().await;
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
            packets.push(through_commit(&mut socket).await);
            results(&mut socket, stage).await;
        }
        close(&mut socket).await;
        packets
    });
    let mut harness = Harness::start(config, None, false).await;
    let exact_id = "é".repeat((BUDGET - 2) / 2);
    harness
        .data(audio(vec![6, 0], Some(&exact_id)))
        .await
        .unwrap();
    harness.commit().await;
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
    harness.commit().await;
    harness.pair().await;
    let exact_empty_id = "e".repeat(BUDGET);
    harness
        .data(audio(vec![], Some(&exact_empty_id)))
        .await
        .unwrap();
    harness.commit().await;
    harness.pair().await;
    assert!(harness
        .data(audio(vec![], Some(&"x".repeat(BUDGET + 1))))
        .await
        .is_err());
    harness.commit().await;
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
        let packets = through_commit(&mut socket).await;
        results(&mut socket, "fixed-budget").await;
        close(&mut socket).await;
        packets
    });
    let fixed = "legacy-fixed".repeat(100);
    let mut harness = Harness::start(config, Some(&fixed), false).await;
    harness
        .data(audio(vec![0; 512], Some(&"ignored".repeat(1000))))
        .await
        .unwrap();
    harness
        .data(audio(vec![], Some(&"ignored-empty".repeat(1000))))
        .await
        .unwrap();
    harness.commit().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(packets, vec![vec![0; 512]]);
    assert_pairs(&frames, &[("fixed-budget", &fixed)]);
    assert_eq!(usage(&frames), vec![256.0 / 16_000.0]);
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
                if let Message::Text(text) = message {
                    packets.push(text.to_string());
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
        let first_packets = through_commit(&mut first).await;
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
        let second_packets = through_commit(&mut second).await;
        results(&mut second, "idle-seed").await;
        while let Some(Ok(message)) = bounded(second.next()).await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
        let mut third = accept(&listener).await;
        let third_packets = through_commit(&mut third).await;
        results(&mut third, "fresh-default").await;
        close(&mut third).await;
        [first_packets, second_packets, third_packets]
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.commit().await;
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
    harness.commit().await;
    harness.pair().await;
    health.set_usable(false);
    harness
        .data(audio(vec![], Some("discarded-again")))
        .await
        .unwrap();
    health.set_usable(true);
    harness.commit().await;
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
async fn settings_drain_keeps_consumed_speaker_and_full_queue_rejection_does_not_relabel() {
    let (listener, mut config) = fixture().await;
    config.limits.queue_capacity = 2;
    let (draining_tx, draining_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (audio_tx, audio_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_message: Value =
            serde_json::from_str(wire(&mut first).await.to_text().unwrap()).unwrap();
        let first_pcm = STANDARD
            .decode(first_message["audio_base_64"].as_str().unwrap())
            .unwrap();
        assert_eq!(first_message["commit"], false);
        results(&mut first, "before-settings").await;
        let first_tail = through_commit(&mut first).await;
        draining_tx.send(()).unwrap();
        release_rx.await.unwrap();
        results(&mut first, "old-drain").await;
        close(&mut first).await;
        let mut second = accept(&listener).await;
        let second_message: Value =
            serde_json::from_str(wire(&mut second).await.to_text().unwrap()).unwrap();
        let second_pcm = STANDARD
            .decode(second_message["audio_base_64"].as_str().unwrap())
            .unwrap();
        assert_eq!(second_message["commit"], false);
        audio_tx.send(()).unwrap();
        let second_tail = through_commit(&mut second).await;
        results(&mut second, "next-socket").await;
        close(&mut second).await;
        (first_pcm, first_tail, second_pcm, second_tail)
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.pair().await;
    harness
        .stt
        .update_settings(&SttSettings {
            language: Setting::Value("es".into()),
            ..Default::default()
        })
        .unwrap();
    bounded(draining_rx).await.unwrap();
    harness.data(audio(vec![2, 0], Some("bob"))).await.unwrap();
    harness.data(audio(vec![], Some("carol"))).await.unwrap();
    let error = harness
        .data(audio(vec![], Some("rejected-mallory")))
        .await
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::Application);
    assert!(error.message.contains("queue is full"));
    release_tx.send(()).unwrap();
    harness.pair().await;
    bounded(audio_rx).await.unwrap();
    harness.commit().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(packets, (vec![1, 0], vec![], vec![2, 0], vec![]));
    assert_pairs(
        &frames,
        &[
            ("before-settings", "alice"),
            ("old-drain", "alice"),
            ("next-socket", "carol"),
        ],
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
}

#[tokio::test]
async fn fixed_empty_input_remains_noop_even_while_the_real_command_queue_is_full() {
    for fixed in ["fixed", ""] {
        let (listener, mut config) = fixture().await;
        config.limits.queue_capacity = 1;
        let (draining_tx, draining_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let (audio_tx, audio_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first = accept(&listener).await;
            let first_message: Value =
                serde_json::from_str(wire(&mut first).await.to_text().unwrap()).unwrap();
            assert_eq!(first_message["audio_base_64"], "AQA=");
            assert_eq!(first_message["commit"], false);
            results(&mut first, "fixed-before").await;
            assert!(through_commit(&mut first).await.is_empty());
            draining_tx.send(()).unwrap();
            release_rx.await.unwrap();
            results(&mut first, "fixed-drain").await;
            close(&mut first).await;
            let mut second = accept(&listener).await;
            let second_message: Value =
                serde_json::from_str(wire(&mut second).await.to_text().unwrap()).unwrap();
            assert_eq!(second_message["audio_base_64"], "AgA=");
            assert_eq!(second_message["commit"], false);
            audio_tx.send(()).unwrap();
            assert!(through_commit(&mut second).await.is_empty());
            results(&mut second, "fixed-next").await;
            close(&mut second).await;
        });
        let mut harness = Harness::start(config, Some(fixed), false).await;
        harness
            .data(audio(vec![1, 0], Some("ignored")))
            .await
            .unwrap();
        harness.pair().await;
        harness
            .stt
            .update_settings(&SttSettings {
                language: Setting::Value("es".into()),
                ..Default::default()
            })
            .unwrap();
        bounded(draining_rx).await.unwrap();
        harness
            .data(audio(vec![2, 0], Some("also-ignored")))
            .await
            .unwrap();
        let empty_result = harness
            .data(FrameData::UserAudio(UserAudioRawFrame {
                audio: AudioRawFrame {
                    audio: vec![],
                    sample_rate: 0,
                    num_channels: 0,
                    context_id: None,
                },
                user_id: "ignored-empty".repeat(100_000),
            }))
            .await;
        release_tx.send(()).unwrap();
        harness.pair().await;
        bounded(audio_rx).await.unwrap();
        harness.commit().await;
        harness.pair().await;
        let (frames, report) = harness.finish(false).await;
        bounded(server).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(
            empty_result.is_ok(),
            "fixed empty input rejected: {empty_result:?}"
        );
        assert_pairs(
            &frames,
            &[
                ("fixed-before", fixed),
                ("fixed-drain", fixed),
                ("fixed-next", fixed),
            ],
        );
        assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
    }
}

#[tokio::test]
async fn raw_chunk_limit_and_encoded_message_limit_remain_separate() {
    // The raw PCM chunk cap is independent of the dynamic retention budget.
    let (listener, mut config) = fixture().await;
    config.max_audio_chunk_bytes = 2;
    let server = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let first = through_commit(&mut socket).await;
        results(&mut socket, "raw-limit").await;
        close(&mut socket).await;
        first
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    let error = harness
        .data(audio(vec![2, 0, 3, 0], Some("rejected")))
        .await
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::Application);
    harness.commit().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    assert_eq!(bounded(server).await.unwrap(), vec![vec![1, 0]]);
    assert_eq!(report.errors.len(), 1);
    assert_pairs(&frames, &[("raw-limit", "alice")]);
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);

    // Logical PCM+speaker fits, but its actual base64+JSON exceeds the wire cap.
    // This is an asynchronous low-level session error, not rejected admission.
    let (listener, mut config) = fixture().await;
    config.limits.max_message_bytes = 1024;
    let (quiet_tx, quiet_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        close(&mut first).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err()
        );
        quiet_tx.send(()).unwrap();
        let mut second = accept(&listener).await;
        let packets = through_commit(&mut second).await;
        results(&mut second, "fresh-after-wire-limit").await;
        close(&mut second).await;
        packets
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![0; 768], Some("wire-rejected")))
        .await
        .unwrap();
    loop {
        let frame = bounded(harness.worker.recv()).await.unwrap();
        let is_error = matches!(frame.frame.data, FrameData::Error(_));
        harness.frames.push(frame);
        if is_error {
            break;
        }
    }
    assert!(harness.stt.health().unwrap().is_usable());
    bounded(quiet_rx).await.unwrap();
    harness.commit().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    assert!(bounded(server).await.unwrap().is_empty());
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].category, ErrorCategory::Application);
    assert_pairs(&frames, &[("fresh-after-wire-limit", "")]);
    assert_eq!(usage(&frames), vec![384.0 / 16_000.0]);
}

#[tokio::test]
async fn timestamped_final_pairs_use_current_speaker_and_preserve_provider_word_speaker() {
    for (timestamps, detection) in [(true, false), (false, true), (true, true)] {
        let (listener, mut config) = fixture().await;
        config.include_timestamps = timestamps;
        config.include_language_detection = detection;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            let mut packets = vec![];
            for index in 0..2 {
                packets.push(through_commit(&mut socket).await);
                let partial = json!({"message_type":"partial_transcript", "text":format!("timestamp-{index}-false"), "language_code":"en", "fixture":format!("timestamp-{index}")});
                let plain =
                    json!({"message_type":"committed_transcript", "text":"must be suppressed"});
                let final_result = json!({"message_type":"committed_transcript_with_timestamps", "text":format!("timestamp-{index}-true"), "language_code":"en", "fixture":format!("timestamp-{index}"), "words":[{"text":"provider word", "speaker_id":"provider-diarization-only", "start":0.0, "end":0.1}]});
                socket
                    .send(Message::Text(partial.to_string().into()))
                    .await
                    .unwrap();
                for value in if index == 0 {
                    [plain, final_result]
                } else {
                    [final_result, plain]
                } {
                    socket
                        .send(Message::Text(value.to_string().into()))
                        .await
                        .unwrap();
                }
                if index == 1 {
                    socket
                        .send(Message::Text(
                            json!({
                                "message_type": "partial_transcript",
                                "text": "after-reversed-plain",
                                "language_code": "en",
                                "fixture": "reverse-consumption-sentinel",
                            })
                            .to_string()
                            .into(),
                        ))
                        .await
                        .unwrap();
                }
            }
            close(&mut socket).await;
            packets
        });
        let mut harness = Harness::start(config, None, false).await;
        harness
            .data(audio(vec![1, 0], Some("alice")))
            .await
            .unwrap();
        harness.commit().await;
        harness.pair().await;
        harness.data(audio(vec![], Some("bob"))).await.unwrap();
        harness.commit().await;
        harness.pair().await;
        // This visible event is ordered after the reversed plain final on the
        // socket, so End cannot retire the session before that final is decoded.
        loop {
            let frame = bounded(harness.worker.recv()).await.unwrap();
            let sentinel = matches!(&frame.frame.data,
                FrameData::InterimTranscription(value) if value.text == "after-reversed-plain");
            harness.frames.push(frame);
            if sentinel {
                break;
            }
        }
        let (mut frames, report) = harness.finish(false).await;
        let packets = bounded(server).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(packets, vec![vec![vec![1, 0]], vec![]]);
        let sentinel_positions: Vec<_> = frames
            .iter()
            .enumerate()
            .filter_map(|(index, frame)| {
                matches!(&frame.frame.data,
                FrameData::InterimTranscription(value) if value.text == "after-reversed-plain")
                .then_some(index)
            })
            .collect();
        assert_eq!(sentinel_positions.len(), 1);
        let sentinel = frames.remove(sentinel_positions[0]);
        assert_eq!(sentinel.direction, Direction::Downstream);
        let FrameData::InterimTranscription(value) = sentinel.frame.data else {
            panic!()
        };
        assert_eq!(value.user_id, "bob");
        assert_eq!(value.language.as_deref(), Some("en"));
        assert!(!value.finalized);
        assert!(value.timestamp.contains('T'));
        assert_eq!(
            value.result.as_ref().unwrap()["fixture"],
            "reverse-consumption-sentinel"
        );
        assert_pairs(&frames, &[("timestamp-0", "alice"), ("timestamp-1", "bob")]);
        for frame in &frames {
            if let FrameData::Transcription(value) = &frame.frame.data {
                assert_eq!(
                    value.result.as_ref().unwrap()["words"][0]["speaker_id"],
                    "provider-diarization-only"
                );
            }
        }
        assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);
    }
}

#[tokio::test]
async fn vad_mode_uses_audio_wire_barriers_and_keeps_segment_final_not_turn_finalized() {
    let (listener, mut config) = fixture().await;
    config.commit_strategy = CommitStrategy::Vad;
    let server = tokio::spawn(async move {
        let mut socket = accept_strategy(&listener, "vad").await;
        let mut packets = vec![];
        for index in 0..3 {
            let message: Value =
                serde_json::from_str(wire(&mut socket).await.to_text().unwrap()).unwrap();
            assert_eq!(message["message_type"], "input_audio_chunk");
            assert_eq!(message["commit"], false);
            assert_eq!(message["sample_rate"], 16_000);
            packets.push(
                STANDARD
                    .decode(message["audio_base_64"].as_str().unwrap())
                    .unwrap(),
            );
            results(&mut socket, &format!("vad-{index}")).await;
        }
        close(&mut socket).await;
        packets
    });
    let mut harness = Harness::start(config, None, false).await;
    for (bytes, speaker) in [
        (vec![1, 0], Some("alice")),
        (vec![2, 0], None),
        (vec![3, 0], Some("bob")),
    ] {
        harness.data(audio(bytes, speaker)).await.unwrap();
        harness.pair().await;
    }
    // VAD stop in provider VAD mode must not add a manual commit message.
    harness.commit().await;
    let (frames, report) = harness.finish(false).await;
    assert_eq!(
        bounded(server).await.unwrap(),
        [vec![1, 0], vec![2, 0], vec![3, 0]]
    );
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let transcripts: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::InterimTranscription(value) | FrameData::Transcription(value) => Some(value),
            _ => None,
        })
        .collect();
    assert_eq!(transcripts.len(), 6);
    for (value, speaker) in transcripts
        .iter()
        .zip(["alice", "alice", "", "", "bob", "bob"])
    {
        assert_eq!(value.user_id, speaker);
        assert!(!value.finalized);
        assert_eq!(value.language.as_deref(), Some("en"));
    }
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 3]);
}

#[tokio::test]
async fn cancel_during_settings_drain_discards_queued_speakers_without_wire_replay() {
    let (listener, mut config) = fixture().await;
    config.limits.queue_capacity = 2;
    let (draining_tx, draining_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let message: Value =
            serde_json::from_str(wire(&mut socket).await.to_text().unwrap()).unwrap();
        assert_eq!(message["audio_base_64"], "AQA=");
        assert_eq!(message["commit"], false);
        results(&mut socket, "before-cancel").await;
        assert!(through_commit(&mut socket).await.is_empty());
        draining_tx.send(()).unwrap();
        close(&mut socket).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err()
        );
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.pair().await;
    harness
        .stt
        .update_settings(&SttSettings {
            language: Setting::Value("es".into()),
            ..Default::default()
        })
        .unwrap();
    bounded(draining_rx).await.unwrap();
    harness
        .data(audio(vec![2, 0], Some("queued-bob")))
        .await
        .unwrap();
    harness
        .data(audio(vec![], Some("queued-carol")))
        .await
        .unwrap();
    let (frames, report) = harness.finish(true).await;
    bounded(server).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_pairs(&frames, &[("before-cancel", "alice")]);
    // Usage records accepted PCM, including PCM retired before provider send.
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
}

#[tokio::test]
async fn fixed_empty_input_after_retirement_neither_connects_nor_seeds_dynamic_identity() {
    for fixed in ["fixed", ""] {
        let (listener, config) = fixture().await;
        let (idle_tx, idle_rx) = oneshot::channel();
        let (quiet_tx, quiet_rx) = oneshot::channel();
        let server = tokio::spawn(async move {
            let mut first = accept(&listener).await;
            close(&mut first).await;
            idle_rx.await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(80), listener.accept())
                    .await
                    .is_err()
            );
            quiet_tx.send(()).unwrap();
            let mut second = accept(&listener).await;
            assert!(through_commit(&mut second).await.is_empty());
            results(&mut second, "fixed-idle").await;
            close(&mut second).await;
        });
        let mut harness = Harness::start(config, Some(fixed), false).await;
        let health = harness.stt.health().unwrap();
        health.set_usable(false);
        harness.data(FrameData::Interruption).await.unwrap();
        health.set_usable(true);
        let empty_result = harness
            .data(FrameData::UserAudio(UserAudioRawFrame {
                audio: AudioRawFrame {
                    audio: vec![],
                    sample_rate: 0,
                    num_channels: 0,
                    context_id: None,
                },
                user_id: "ignored-idle".repeat(100_000),
            }))
            .await;
        idle_tx.send(()).unwrap();
        bounded(quiet_rx).await.unwrap();
        harness.commit().await;
        harness.pair().await;
        let (frames, report) = harness.finish(false).await;
        bounded(server).await.unwrap();
        assert!(
            empty_result.is_ok(),
            "fixed idle empty input rejected: {empty_result:?}"
        );
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_pairs(&frames, &[("fixed-idle", fixed)]);
        assert!(usage(&frames).is_empty());
    }
}

#[tokio::test]
async fn upstream_mute_and_resume_control_new_pcm_without_relabeling_late_results() {
    for passthrough in [true, false] {
        check_upstream_mute_and_resume(passthrough).await;
    }
}
async fn check_upstream_mute_and_resume(passthrough: bool) {
    use futures_util::FutureExt;
    use std::panic::AssertUnwindSafe;
    let (listener, config) = fixture().await;
    let mut peer = tokio::spawn(async move {
        let mut socket = accept(&listener).await;
        let mut packets = vec![];
        for stage in ["seed", "muted", "resumed", "muted-again", "restored"] {
            packets.push(through_commit(&mut socket).await);
            results(&mut socket, stage).await;
        }
        close(&mut socket).await;
        packets
    });
    let created = AssertUnwindSafe(Harness::start(config, None, passthrough))
        .catch_unwind()
        .await;
    let mut harness = match created {
        Ok(harness) => harness,
        Err(panic) => {
            peer.abort();
            let _ = peer.await;
            std::panic::resume_unwind(panic);
        }
    };
    let tag = LocalFrameTag::new();
    let mut controls = vec![];
    let observed = tokio::time::timeout(
        DEADLINE,
        AssertUnwindSafe(async {
            harness
                .data(audio(vec![1, 0], Some("alice")))
                .await
                .unwrap();
            harness.commit().await;
            harness.pair().await;
            for value in [true, true] {
                let frame = envelope(FrameData::SttMute(value), &tag);
                controls.push((frame.clone(), Direction::Upstream));
                harness.send(frame, Direction::Upstream).await.unwrap();
            }
            for data in [
                audio(vec![2, 0], Some("muted-bob")),
                audio(vec![3, 0], None),
                audio(vec![], Some("muted-empty")),
            ] {
                harness.data(data).await.unwrap();
            }
            harness.commit().await;
            harness.pair().await;
            for value in [false, false] {
                let frame = envelope(FrameData::SttMute(value), &tag);
                controls.push((frame.clone(), Direction::Upstream));
                harness.send(frame, Direction::Upstream).await.unwrap();
            }
            harness
                .data(audio(vec![4, 0], Some("carol")))
                .await
                .unwrap();
            harness.commit().await;
            harness.pair().await;
            let frame = envelope(FrameData::SttMute(true), &tag);
            controls.push((frame.clone(), Direction::Downstream));
            harness.send(frame, Direction::Downstream).await.unwrap();
            harness
                .data(audio(vec![5, 0], Some("muted-dave")))
                .await
                .unwrap();
            harness.commit().await;
            harness.pair().await;
            let frame = envelope(FrameData::SttMute(false), &tag);
            controls.push((frame.clone(), Direction::Upstream));
            harness.send(frame, Direction::Upstream).await.unwrap();
            harness.data(audio(vec![6, 0], None)).await.unwrap();
            harness.commit().await;
            harness.pair().await;
        })
        .catch_unwind(),
    )
    .await;
    // Always settle both owners before reporting assertions, including the
    // deliberately failing pre-fix replay. A deadline never detaches a handle.
    let normal = matches!(observed, Ok(Ok(())));
    let mut cleanup_errors = vec![];
    let admission = tokio::time::timeout(DEADLINE, async {
        if normal {
            harness.worker.end().await
        } else {
            harness
                .worker
                .cancel("mute fixture observation failed")
                .await
        }
    })
    .await;
    if !matches!(admission, Ok(Ok(()))) {
        cleanup_errors.push(format!("terminal admission: {admission:?}"));
        let _ = harness
            .worker
            .cancel("mute fixture admission deadline")
            .await;
    }
    let joined = match tokio::time::timeout(DEADLINE, async {
        while let Some(frame) = harness.worker.recv().await {
            harness.frames.push(frame);
        }
        harness.worker.join().await
    })
    .await
    {
        Ok(result) => result,
        Err(error) => {
            cleanup_errors.push(format!("join deadline: {error}"));
            let _ = harness.worker.cancel("mute fixture join deadline").await;
            harness.worker.join().await
        }
    };
    let packets = match tokio::time::timeout(DEADLINE, &mut peer).await {
        Ok(result) => result.map_err(|e| e.to_string()),
        Err(error) => {
            peer.abort();
            let _ = peer.await;
            Err(format!("peer deadline, aborted and joined: {error}"))
        }
    };
    assert!(normal, "observation failed after owners settled");
    assert!(cleanup_errors.is_empty(), "{cleanup_errors:?}");
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0, "{:?}", report.errors);
    assert_eq!(
        packets.unwrap(),
        vec![
            vec![vec![1, 0]],
            vec![],
            vec![vec![4, 0]],
            vec![],
            vec![vec![6, 0]]
        ]
    );
    assert_pairs(
        &harness.frames,
        &[
            ("seed", "alice"),
            ("muted", "alice"),
            ("resumed", "carol"),
            ("muted-again", "carol"),
            ("restored", ""),
        ],
    );
    assert_eq!(usage(&harness.frames), vec![1.0 / 16_000.0; 3]);
    for (original, direction) in controls {
        let found: Vec<_> = harness
            .frames
            .iter()
            .filter(|f| f.frame.id == original.id)
            .collect();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].direction, direction);
        assert_eq!(found[0].frame, original);
        assert!(tag.contains(&found[0].frame));
    }
}

#[test]
fn pinned_realtime_source_mute_preserves_manual_commit_without_new_pcm() {
    let source: Value = serde_json::from_str(include_str!(
        "fixtures/python_realtime_stt_mute_source.json"
    ))
    .unwrap();
    for name in ["mute_resume_downstream", "mute_resume_upstream"] {
        let muted = &source[name]["after_mute"];
        assert_eq!(muted["muted"], true);
        assert_eq!(muted["provider_json_strings"], json!([]));
        assert_eq!(muted["speaker"], "");
        assert_eq!(muted["pending_usage_seconds"], 0.0);
        let resumed: Value = serde_json::from_str(
            source[name]["after_resume"]["provider_json_strings"][0]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            resumed,
            json!({"message_type":"input_audio_chunk","audio_base_64":"AgA=","commit":false,"sample_rate":16000})
        );
    }
    let manual = &source["muted_commit_and_late_results_manual_downstream"]["after_commit"];
    assert_eq!(manual["muted"], true);
    let commit: Value =
        serde_json::from_str(manual["provider_json_strings"][1].as_str().unwrap()).unwrap();
    assert_eq!(
        commit,
        json!({"message_type":"input_audio_chunk","audio_base_64":"","commit":true,"sample_rate":16000})
    );
    assert_eq!(manual["speaker"], "original-speaker");
}
