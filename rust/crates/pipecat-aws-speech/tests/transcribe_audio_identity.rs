//! Real loopback protocol checks for the input-audio family and the supervisor's
//! latest consumed speaker. Provider results do not identify individual PCM frames.
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use pipecat_aws_speech::*;
use pipecat_core::*;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
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

// A process receipt proves admission. A subsequent effective settings update
// on the same supervisor FIFO produces Finish on the old socket, proving that
// preceding empty input was consumed without confusing it with provider Finish.
struct ProcessedStt {
    stt: Arc<TranscribeSttProcessor>,
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
    stt: Arc<TranscribeSttProcessor>,
    receipts: mpsc::UnboundedReceiver<Receipt>,
    frames: Vec<DirectedFrame>,
}
impl Harness {
    async fn start(config: TranscribeConfig, fixed: Option<&str>, passthrough: bool) -> Self {
        let mut stt = TranscribeSttProcessor::new(TranscribeClient::new(config).unwrap())
            .with_audio_passthrough(passthrough)
            .with_reconnect_policy(ReconnectPolicy {
                initial_delay: Duration::from_millis(1),
                ..Default::default()
            });
        if let Some(id) = fixed {
            stt = stt.with_user_id(id);
        }
        let sample_rate = stt.config_snapshot().unwrap().sample_rate;
        let stt = Arc::new(stt);
        let (tx, receipts) = mpsc::unbounded_channel();
        let worker = Pipeline::new(vec![Arc::new(ProcessedStt {
            stt: stt.clone(),
            receipts: tx,
        })])
        .start(PipelineConfig {
            start: StartFrame {
                enable_usage_metrics: true,
                audio_in_sample_rate: sample_rate,
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
    fn rotate(&self) {
        let current = self.stt.config_snapshot().unwrap();
        let language = if current.language == "en-US" {
            "en-GB"
        } else {
            "en-US"
        };
        self.stt
            .update_settings(&SttSettings {
                language: Setting::Value(language.into()),
                ..Default::default()
            })
            .unwrap();
    }
    async fn start_idle(&mut self) {
        self.data(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 1.0,
        }))
        .await
        .unwrap();
        self.data(FrameData::UserStoppedSpeaking).await.unwrap();
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
async fn fixture() -> (TcpListener, TranscribeConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = TranscribeConfig::new(
        AwsCredentials::new("IDENTITYACCESS", "IDENTITYSECRET", None).unwrap(),
        "us-east-1",
    );
    config.endpoint = format!(
        "ws://{}/stream-transcription-websocket",
        listener.local_addr().unwrap()
    );
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_secs(2);
    (listener, config)
}
fn mac(key: &[u8], bytes: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(bytes);
    mac.finalize().into_bytes().to_vec()
}
fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
// Verify the real upgrade request independently, including the exact host and
// canonical query. These EventStream AudioEvents are CRC-framed, not chunk-MACed.
fn handshake(request: &Request, rate: u32) {
    assert_eq!(request.uri().path(), "/stream-transcription-websocket");
    assert_eq!(request.headers()["origin"], "https://localhost");
    let (query, signature) = request
        .uri()
        .query()
        .unwrap()
        .rsplit_once("&X-Amz-Signature=")
        .unwrap();
    let values: std::collections::BTreeMap<_, _> = url::form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    assert_eq!(values["sample-rate"], rate.to_string());
    assert_eq!(values["media-encoding"], "pcm");
    assert_eq!(values["X-Amz-Algorithm"], "AWS4-HMAC-SHA256");
    assert_eq!(values["X-Amz-SignedHeaders"], "host");
    let date = &values["X-Amz-Date"];
    let scope = format!("{}/us-east-1/transcribe/aws4_request", &date[..8]);
    assert_eq!(
        values["X-Amz-Credential"],
        format!("IDENTITYACCESS/{scope}")
    );
    let canonical = format!(
        "GET\n/stream-transcription-websocket\n{query}\nhost:{}\n\nhost\n{}",
        request.headers()["host"].to_str().unwrap(),
        digest(b"")
    );
    let key = mac(
        &mac(
            &mac(
                &mac(b"AWS4IDENTITYSECRET", &date.as_bytes()[..8]),
                b"us-east-1",
            ),
            b"transcribe",
        ),
        b"aws4_request",
    );
    let signed = mac(
        &key,
        format!(
            "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
            digest(canonical.as_bytes())
        )
        .as_bytes(),
    );
    assert_eq!(
        signature,
        signed
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    );
}
#[allow(clippy::result_large_err)]
async fn accept_rate(listener: &TcpListener, rate: u32) -> Socket {
    bounded(accept_hdr_async(
        bounded(listener.accept()).await.unwrap().0,
        move |request: &Request, response: Response| {
            handshake(request, rate);
            Ok(response)
        },
    ))
    .await
    .unwrap()
}
async fn accept(listener: &TcpListener) -> Socket {
    accept_rate(listener, 16_000).await
}
fn crc(bytes: &[u8]) -> u32 {
    let mut value = !0u32;
    for byte in bytes {
        value ^= u32::from(*byte);
        for _ in 0..8 {
            value = (value >> 1) ^ if value & 1 == 1 { 0xedb88320 } else { 0 };
        }
    }
    !value
}
fn header(name: &str, value: &str) -> Vec<u8> {
    let mut bytes = vec![name.len() as u8];
    bytes.extend_from_slice(name.as_bytes());
    bytes.push(7);
    bytes.extend_from_slice(&(value.len() as u16).to_be_bytes());
    bytes.extend_from_slice(value.as_bytes());
    bytes
}
fn event(payload: &Value) -> Message {
    let headers = [
        header(":message-type", "event"),
        header(":event-type", "TranscriptEvent"),
        header(":content-type", "application/json"),
    ]
    .concat();
    let payload = serde_json::to_vec(payload).unwrap();
    let mut bytes = vec![];
    bytes.extend_from_slice(&((16 + headers.len() + payload.len()) as u32).to_be_bytes());
    bytes.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&crc(&bytes).to_be_bytes());
    bytes.extend_from_slice(&headers);
    bytes.extend_from_slice(&payload);
    bytes.extend_from_slice(&crc(&bytes).to_be_bytes());
    Message::Binary(bytes.into())
}
async fn wire(socket: &mut Socket) -> Message {
    bounded(socket.next())
        .await
        .expect("socket closed")
        .unwrap()
}
fn decode_audio(message: Message) -> Vec<u8> {
    let Message::Binary(bytes) = message else {
        panic!("expected binary AudioEvent");
    };
    assert!(bytes.len() >= 16);
    assert_eq!(
        u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize,
        bytes.len()
    );
    assert_eq!(
        u32::from_be_bytes(bytes[8..12].try_into().unwrap()),
        crc(&bytes[..8])
    );
    assert_eq!(
        u32::from_be_bytes(bytes[bytes.len() - 4..].try_into().unwrap()),
        crc(&bytes[..bytes.len() - 4])
    );
    let count = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
    assert!(count <= bytes.len() - 16);
    let expected = [
        header(":content-type", "application/octet-stream"),
        header(":event-type", "AudioEvent"),
        header(":message-type", "event"),
    ];
    let mut raw = &bytes[12..12 + count];
    let mut headers = vec![];
    while !raw.is_empty() {
        let length = raw[0] as usize;
        assert!(raw.len() >= length + 4);
        assert_eq!(raw[length + 1], 7);
        let value = u16::from_be_bytes(raw[length + 2..length + 4].try_into().unwrap()) as usize;
        let total = length + 4 + value;
        assert!(raw.len() >= total);
        headers.push(raw[..total].to_vec());
        raw = &raw[total..];
    }
    headers.sort();
    let mut expected = expected.to_vec();
    expected.sort();
    assert_eq!(headers, expected);
    bytes[12 + count..bytes.len() - 4].to_vec()
}
// The zero-payload event belongs only to Finish. Assert PCM after runtime/peer
// cleanup, so the old implementation fails directly on missing UserAudio bytes.
async fn through_finish(socket: &mut Socket) -> Vec<Vec<u8>> {
    let mut packets = vec![];
    loop {
        let pcm = decode_audio(wire(socket).await);
        if pcm.is_empty() {
            return packets;
        }
        packets.push(pcm);
    }
}
async fn results(socket: &mut Socket, stage: &str) {
    for final_result in [false, true] {
        socket
            .send(event(&json!({"Transcript":{"Results":[{
                "ResultId": format!("{stage}-{final_result}"),
                "IsPartial": !final_result,
                "fixture": stage,
                "Alternatives": [{"Transcript": format!("{stage}-{final_result}")}]
            }]}})))
            .await
            .unwrap();
    }
}
async fn rotating_peer(listener: TcpListener, stages: Vec<String>) -> Vec<Vec<Vec<u8>>> {
    let mut packets = vec![];
    for stage in stages {
        let mut socket = accept(&listener).await;
        packets.push(through_finish(&mut socket).await);
        results(&mut socket, &stage).await;
        socket.close(None).await.unwrap();
    }
    let mut final_socket = accept(&listener).await;
    assert!(through_finish(&mut final_socket).await.is_empty());
    final_socket.close(None).await.unwrap();
    packets
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

fn assert_pairs(frames: &[DirectedFrame], stages: &[(&str, &str, &str)]) {
    let transcripts: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::InterimTranscription(value) => Some((frame, value, false)),
            FrameData::Transcription(value) => Some((frame, value, true)),
            _ => None,
        })
        .collect();
    assert_eq!(transcripts.len(), stages.len() * 2);
    for ((frame, value, actual_final), (stage, speaker, language, final_result)) in transcripts
        .iter()
        .zip(stages.iter().flat_map(|(stage, speaker, language)| {
            [
                (*stage, *speaker, *language, false),
                (*stage, *speaker, *language, true),
            ]
        }))
    {
        assert_eq!(*actual_final, final_result);
        assert_eq!(value.text, format!("{stage}-{final_result}"));
        assert_eq!(value.user_id, speaker);
        assert_eq!(value.language.as_deref(), Some(language));
        assert_eq!(value.finalized, final_result);
        assert_eq!(value.result.as_ref().unwrap()["fixture"], stage);
        assert!(value.timestamp.contains('T'));
        assert_eq!(frame.direction, Direction::Downstream);
    }
}

#[tokio::test]
async fn default_current_speaker_tracks_user_plain_and_empty_audio() {
    let (listener, config) = fixture().await;
    let server = tokio::spawn(rotating_peer(
        listener,
        (0..4).map(|n| format!("stage-{n}")).collect(),
    ));
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
        harness.rotate();
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
            ("stage-0", "alice", "en-US"),
            ("stage-1", "", "en-GB"),
            ("stage-2", "bob", "en-US"),
            ("stage-3", "", "en-GB"),
        ],
    );
    passthrough(&frames, &originals, &tag, true);
    assert_eq!(usage(&frames), vec![2.0 / 16_000.0; 2]);
}
#[tokio::test]
async fn explicit_fixed_speaker_including_empty_survives_audio_family_with_passthrough_disabled() {
    for fixed in ["fixed-speaker", ""] {
        let (listener, config) = fixture().await;
        let server = tokio::spawn(rotating_peer(
            listener,
            (0..3).map(|n| format!("fixed-{n}")).collect(),
        ));
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
            harness.rotate();
            harness.pair().await;
        }
        let (frames, report) = harness.finish(false).await;
        let packets = bounded(server).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(packets, vec![vec![vec![5, 0]], vec![vec![5, 0]], vec![]]);
        assert_pairs(
            &frames,
            &[
                ("fixed-0", fixed, "en-US"),
                ("fixed-1", fixed, "en-GB"),
                ("fixed-2", fixed, "en-US"),
            ],
        );
        passthrough(&frames, &originals, &tag, false);
        assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
    }
}
#[tokio::test]
async fn muted_upstream_and_output_audio_preserve_current_speaker_and_original_envelopes() {
    let (listener, config) = fixture().await;
    let server = tokio::spawn(rotating_peer(
        listener,
        vec!["seed".into(), "unchanged".into()],
    ));
    let mut harness = Harness::start(config, None, true).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.rotate();
    harness.pair().await;
    harness.data(FrameData::SttMute(true)).await.unwrap();
    let tag = LocalFrameTag::new();
    let mut originals = vec![];
    for data in [
        audio(vec![2, 0], Some("muted-bob")),
        audio(vec![], Some("muted-empty")),
        audio(vec![], None),
    ] {
        let frame = envelope(data, &tag);
        originals.push((frame.clone(), Direction::Downstream));
        harness.send(frame, Direction::Downstream).await.unwrap();
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
    harness.rotate();
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(packets, vec![vec![vec![1, 0]], vec![]]);
    assert_pairs(
        &frames,
        &[("seed", "alice", "en-US"), ("unchanged", "alice", "en-GB")],
    );
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
async fn speaker_and_pcm_share_chunk_budget_while_fixed_override_retains_wire_chunking() {
    const BUDGET: usize = 1024;
    let (listener, mut config) = fixture().await;
    config.max_audio_chunk_bytes = BUDGET;
    config.limits.max_message_bytes = 1024;
    let stages = vec![
        "exact".into(),
        "rejected".into(),
        "empty-exact".into(),
        "empty-rejected".into(),
    ];
    let server = tokio::spawn(rotating_peer(listener, stages));
    let mut harness = Harness::start(config, None, false).await;
    let exact_id = "é".repeat((BUDGET - 2) / 2);
    harness
        .data(audio(vec![6, 0], Some(&exact_id)))
        .await
        .unwrap();
    harness.rotate();
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
    harness.rotate();
    harness.pair().await;
    let empty_id = "e".repeat(BUDGET);
    harness.data(audio(vec![], Some(&empty_id))).await.unwrap();
    harness.rotate();
    harness.pair().await;
    assert!(harness
        .data(audio(vec![], Some(&"x".repeat(BUDGET + 1))))
        .await
        .is_err());
    harness.rotate();
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    let packets = bounded(server).await.unwrap();
    assert_eq!(report.errors.len(), 4);
    assert_eq!(packets, vec![vec![vec![6, 0]], vec![], vec![], vec![]]);
    assert_pairs(
        &frames,
        &[
            ("exact", &exact_id, "en-US"),
            ("rejected", &exact_id, "en-GB"),
            ("empty-exact", &empty_id, "en-US"),
            ("empty-rejected", &empty_id, "en-GB"),
        ],
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);

    // Dynamic plain input and explicit fixed IDs retain the old PCM budget.
    // PCM larger than a wire message is valid and split by the unchanged sender.
    let fixed = "legacy-fixed".repeat(100);
    for override_id in [Some(fixed.as_str()), None] {
        let (listener, mut config) = fixture().await;
        config.max_audio_chunk_bytes = 2048;
        config.limits.max_message_bytes = 1024;
        let server = tokio::spawn(async move {
            let mut socket = accept(&listener).await;
            let packets = through_finish(&mut socket).await;
            results(&mut socket, "chunk-budget").await;
            socket.close(None).await.unwrap();
            packets
        });
        let mut harness = Harness::start(config, override_id, false).await;
        harness.data(audio(vec![0; 2048], None)).await.unwrap();
        let (next_len, speaker) = if let Some(id) = override_id {
            harness
                .data(audio(vec![0; 2048], Some(&"ignored".repeat(1000))))
                .await
                .unwrap();
            harness
                .data(audio(vec![], Some(&"ignored-empty".repeat(1000))))
                .await
                .unwrap();
            (2048, id)
        } else {
            harness.data(audio(vec![0; 2046], Some("é"))).await.unwrap();
            (2046, "é")
        };
        let (frames, report) = harness.finish(false).await;
        let packets = bounded(server).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            packets.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![920, 920, 208, 920, 920, next_len - 1840]
        );
        assert_eq!(packets.concat(), vec![0; 2048 + next_len]);
        assert_pairs(&frames, &[("chunk-budget", speaker, "en-US")]);
        assert_eq!(
            usage(&frames),
            vec![1024.0 / 16_000.0, next_len as f64 / 2.0 / 16_000.0]
        );
    }
}

#[tokio::test]
async fn malformed_user_pcm_preserves_invalid_request_and_unusable_gate_without_wire_audio() {
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
                if matches!(message, Message::Binary(_)) {
                    packets.push(decode_audio(message));
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
        harness.data(audio(vec![], None)).await.unwrap();
        let (frames, report) = harness.finish(false).await;
        assert_eq!(report.errors.len(), 1);
        assert!(bounded(server).await.unwrap().is_empty());
        assert!(usage(&frames).is_empty());
        assert_pairs(&frames, &[]);
    }
}

#[tokio::test]
async fn empty_identity_seeds_idle_without_connecting_and_health_generation_resets_it() {
    let (listener, config) = fixture().await;
    let (idle_tx, idle_rx) = oneshot::channel();
    let (checked_tx, checked_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_pcm = decode_audio(wire(&mut first).await);
        results(&mut first, "old-generation").await;
        while let Some(Ok(message)) = bounded(first.next()).await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
        idle_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err()
        );
        checked_tx.send(()).unwrap();
        let mut second = accept(&listener).await;
        results(&mut second, "idle-seed").await;
        while let Some(Ok(message)) = bounded(second.next()).await {
            assert!(!matches!(message, Message::Binary(_)));
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
        let mut third = accept(&listener).await;
        results(&mut third, "fresh-default").await;
        let third_pcm = through_finish(&mut third).await;
        third.close(None).await.unwrap();
        (first_pcm, third_pcm)
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.pair().await;
    let health = harness.stt.health().unwrap();
    health.set_usable(false);
    harness
        .data(audio(vec![], Some("discarded-unusable")))
        .await
        .unwrap();
    health.set_usable(true);
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
    harness.start_idle().await;
    harness.pair().await;
    health.set_usable(false);
    harness
        .data(audio(vec![], Some("discarded-again")))
        .await
        .unwrap();
    health.set_usable(true);
    harness.start_idle().await;
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(bounded(server).await.unwrap(), (vec![1, 0], vec![]));
    assert_pairs(
        &frames,
        &[
            ("old-generation", "alice", "en-US"),
            ("idle-seed", "idle-bob", "en-US"),
            ("fresh-default", "", "en-US"),
        ],
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);
}

#[tokio::test]
async fn settings_drain_keeps_consumed_speaker_and_queue_rejection_does_not_relabel() {
    let (listener, mut config) = fixture().await;
    config.limits.queue_capacity = 2;
    let (draining_tx, draining_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (second_tx, second_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_pcm = decode_audio(wire(&mut first).await);
        results(&mut first, "before-settings").await;
        let first_tail = through_finish(&mut first).await;
        draining_tx.send(()).unwrap();
        release_rx.await.unwrap();
        results(&mut first, "old-drain").await;
        first.close(None).await.unwrap();
        let mut second = accept(&listener).await;
        let second_pcm = decode_audio(wire(&mut second).await);
        second_tx.send(()).unwrap();
        let second_tail = through_finish(&mut second).await;
        results(&mut second, "next-socket").await;
        second.close(None).await.unwrap();
        let mut third = accept(&listener).await;
        assert!(through_finish(&mut third).await.is_empty());
        third.close(None).await.unwrap();
        (first_pcm, first_tail, second_pcm, second_tail)
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.pair().await;
    harness.rotate();
    bounded(draining_rx).await.unwrap();
    harness.data(audio(vec![2, 0], Some("bob"))).await.unwrap();
    harness.data(audio(vec![], Some("bob"))).await.unwrap();
    let error = harness
        .data(audio(vec![], Some("rejected-mallory")))
        .await
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::Application);
    assert!(error.message.contains("queue is full"));
    release_tx.send(()).unwrap();
    harness.pair().await;
    bounded(second_rx).await.unwrap();
    harness.rotate();
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    assert_eq!(report.errors.len(), 1);
    assert_eq!(
        bounded(server).await.unwrap(),
        (vec![1, 0], vec![], vec![2, 0], vec![])
    );
    assert_pairs(
        &frames,
        &[
            ("before-settings", "alice", "en-US"),
            ("old-drain", "alice", "en-US"),
            ("next-socket", "bob", "en-GB"),
        ],
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
}

#[tokio::test]
async fn reconnect_preserves_consumed_speaker_without_replaying_pcm() {
    let (listener, config) = fixture().await;
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_pcm = decode_audio(wire(&mut first).await);
        results(&mut first, "before-reconnect").await;
        first.close(None).await.unwrap();
        let mut second = accept(&listener).await;
        closed_tx.send(()).unwrap();
        let second_pcm = through_finish(&mut second).await;
        results(&mut second, "after-reconnect").await;
        second.close(None).await.unwrap();
        let mut third = accept(&listener).await;
        assert!(through_finish(&mut third).await.is_empty());
        third.close(None).await.unwrap();
        (first_pcm, second_pcm)
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
    harness.pair().await;
    bounded(closed_rx).await.unwrap();
    harness.rotate();
    harness.pair().await;
    let (frames, report) = harness.finish(false).await;
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].category, ErrorCategory::Connectivity);
    assert_eq!(bounded(server).await.unwrap(), (vec![1, 0], vec![]));
    assert_pairs(
        &frames,
        &[
            ("before-reconnect", "alice", "en-US"),
            ("after-reconnect", "alice", "en-US"),
        ],
    );
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0]);
}

#[tokio::test]
async fn cancelling_reconnect_discards_queued_speakers_and_pcm() {
    let (listener, mut config) = fixture().await;
    config.limits.queue_capacity = 2;
    let (pending_tx, pending_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_pcm = decode_audio(wire(&mut first).await);
        results(&mut first, "before-cancel").await;
        first.close(None).await.unwrap();
        let (mut pending, _) = bounded(listener.accept()).await.unwrap();
        use tokio::io::AsyncReadExt;
        let mut request = vec![];
        bounded(async {
            while !request.ends_with(b"\r\n\r\n") {
                request.push(pending.read_u8().await.unwrap());
            }
        })
        .await;
        // Hold a real, fully received upgrade request before filling the
        // supervisor queue; cancellation then cannot race request submission.
        pending_tx.send(()).unwrap();
        let mut tail = vec![];
        bounded(pending.read_to_end(&mut tail)).await.unwrap();
        assert!(request.starts_with(b"GET /stream-transcription-websocket?"));
        assert!(tail.is_empty());
        first_pcm
    });
    let mut harness = Harness::start(config, None, false).await;
    harness
        .data(audio(vec![1, 0], Some("alice")))
        .await
        .unwrap();
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
    let error = harness
        .data(audio(vec![], Some("rejected-mallory")))
        .await
        .unwrap_err();
    assert_eq!(error.category, ErrorCategory::Application);
    assert!(error.message.contains("queue is full"));
    let (frames, report) = harness.finish(true).await;
    assert_eq!(report.errors.len(), 2);
    assert_eq!(bounded(server).await.unwrap(), vec![1, 0]);
    assert_pairs(&frames, &[("before-cancel", "alice", "en-US")]);
    assert_eq!(usage(&frames), vec![1.0 / 16_000.0; 2]);
}

#[tokio::test]
async fn user_audio_keeps_plain_stream_resampler_state_across_speaker_changes() {
    let input: Vec<u8> = (0..2400)
        .flat_map(|n| (((n % 127) as i16 - 63) * 100).to_le_bytes())
        .collect();
    let mut captures = vec![];
    for users in [false, true] {
        let (listener, mut config) = fixture().await;
        config.sample_rate = 24_000;
        let server = tokio::spawn(async move {
            let mut socket = accept_rate(&listener, 16_000).await;
            let packets = through_finish(&mut socket).await;
            results(&mut socket, "resampled").await;
            socket.close(None).await.unwrap();
            packets
        });
        let mut harness = Harness::start(config, None, false).await;
        for (part, user_id) in input.chunks(1200).zip(["alice", "bob", "carol", "dan"]) {
            let audio = AudioRawFrame::new(part.to_vec(), 24_000, 1).unwrap();
            harness
                .data(if users {
                    FrameData::UserAudio(UserAudioRawFrame {
                        audio,
                        user_id: user_id.into(),
                    })
                } else {
                    FrameData::InputAudio(audio)
                })
                .await
                .unwrap();
        }
        let (frames, report) = harness.finish(false).await;
        let packets = bounded(server).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(packets.concat().len(), 3200);
        assert!(packets.iter().all(|p| p.len() % 2 == 0));
        assert_pairs(
            &frames,
            &[("resampled", if users { "dan" } else { "" }, "en-US")],
        );
        assert_eq!(usage(&frames), vec![600.0 / 24_000.0; 4]);
        captures.push(packets.concat());
    }
    assert_eq!(captures[0], captures[1]);
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
    config.max_audio_chunk_bytes = BUDGET;
    let (draining_tx, draining_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept(&listener).await;
        let first_pcm = decode_audio(wire(&mut first).await);
        results(&mut first, "saturated-seed").await;
        let first_tail = through_finish(&mut first).await;
        draining_tx.send(()).unwrap();
        release_rx.await.unwrap();
        results(&mut first, "saturated-old-drain").await;
        first.close(None).await.unwrap();
        let mut second = accept(&listener).await;
        let second_packets = through_finish(&mut second).await;
        results(&mut second, "saturated-next-socket").await;
        second.close(None).await.unwrap();
        (first_pcm, first_tail, second_packets)
    });
    let mut harness = Harness::start(config, fixed, passthrough_enabled).await;
    harness
        .data(audio(vec![1, 0], Some("seed-alice")))
        .await
        .unwrap();

    harness.pair().await;
    harness.rotate();
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
    assert_eq!(packets, (vec![1, 0], vec![], vec![vec![2, 0], vec![3, 0]]));
    assert_pairs(
        &frames,
        &[
            ("saturated-seed", fixed.unwrap_or("seed-alice"), "en-US"),
            (
                "saturated-old-drain",
                fixed.unwrap_or("seed-alice"),
                "en-US",
            ),
            (
                "saturated-next-socket",
                fixed.unwrap_or("queued-bob"),
                "en-GB",
            ),
        ],
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
