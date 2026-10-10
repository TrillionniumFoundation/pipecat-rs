//! Independent protocol and lifecycle regressions using native pipelines and real TCP peers.
use pipecat_audio::read_pcm16_wav;
use pipecat_core::*;
use pipecat_speech::{elevenlabs::*, *};
use pipecat_turns::{LLMUserAggregator, SharedContext, UserAggregatorConfig};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, Notify},
    task::{JoinHandle, JoinSet},
};

const DEADLINE: Duration = Duration::from_secs(5);

#[derive(Default)]
struct TranscriptProbe {
    frames: Mutex<Vec<DirectedFrame>>,
    arrived: Notify,
}
#[async_trait]
impl FrameProcessor for TranscriptProbe {
    fn name(&self) -> &str {
        "review-transcript-before-aggregator"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::Transcription(_) | FrameData::TranscriptionDetailed(_)
        ) {
            self.frames
                .lock()
                .unwrap()
                .push(DirectedFrame::new(frame.clone(), direction));
            self.arrived.notify_one();
        }
        ctx.push(frame, direction).await
    }
}
impl TranscriptProbe {
    async fn received(&self) {
        tokio::time::timeout(DEADLINE, async {
            loop {
                if !self.frames.lock().unwrap().is_empty() {
                    break;
                }
                self.arrived.notified().await;
            }
        })
        .await
        .expect("transcript must reach native aggregator");
    }
}

#[derive(Clone, Copy, Debug)]
enum Provider {
    OpenAi,
    ElevenLabs,
}
const PROVIDERS: [Provider; 2] = [Provider::OpenAi, Provider::ElevenLabs];

struct Part {
    name: String,
    headers: String,
    bytes: Vec<u8>,
}
struct Request {
    path: String,
    headers: BTreeMap<String, String>,
    parts: Vec<Part>,
    reply: oneshot::Sender<(u16, Vec<u8>)>,
    done: oneshot::Receiver<bool>,
}
impl Request {
    fn field(&self, name: &str) -> Option<&[u8]> {
        self.parts
            .iter()
            .find(|part| part.name == name)
            .map(|part| part.bytes.as_slice())
    }
    fn pcm(&self) -> Vec<u8> {
        let wav = read_pcm16_wav(self.field("file").expect("WAV file part")).unwrap();
        assert_eq!(wav.format.channels(), 1);
        wav.pcm.to_vec()
    }
    async fn respond(self, status: u16, body: Value) {
        self.reply
            .send((status, serde_json::to_vec(&body).unwrap()))
            .unwrap();
        assert!(tokio::time::timeout(DEADLINE, self.done)
            .await
            .unwrap()
            .unwrap());
    }
    async fn respond_or_disconnected(self, status: u16, body: Value) {
        let _ = self
            .reply
            .send((status, serde_json::to_vec(&body).unwrap()));
        let _ = tokio::time::timeout(DEADLINE, self.done)
            .await
            .expect("old request must complete or be cancelled")
            .unwrap();
    }
    async fn disconnected(self) {
        assert!(!tokio::time::timeout(DEADLINE, self.done)
            .await
            .expect("local HTTP future must drop")
            .unwrap());
    }
}
struct Peer {
    base: String,
    requests: mpsc::Receiver<Request>,
    task: JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Peer {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let mut children = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (socket, _) = result.unwrap();
                        children.spawn(serve(socket, tx.clone()));
                    },
                    Some(result) = children.join_next(), if !children.is_empty() => { result.unwrap(); }
                }
            }
        });
        Self {
            base,
            requests,
            task,
        }
    }
    async fn next(&mut self) -> Request {
        tokio::time::timeout(DEADLINE, self.requests.recv())
            .await
            .expect("HTTP request deadline")
            .unwrap()
    }
    fn assert_none(&mut self) {
        assert!(
            self.requests.try_recv().is_err(),
            "unexpected extra HTTP request"
        );
    }
}
async fn serve(mut socket: TcpStream, tx: mpsc::Sender<Request>) {
    let mut bytes = vec![];
    let split = loop {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..n]);
        assert!(bytes.len() <= 2 * 1024 * 1024);
        if let Some(end) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
            break end;
        }
    };
    let head = std::str::from_utf8(&bytes[..split]).unwrap();
    let mut lines = head.split("\r\n");
    let first: Vec<_> = lines.next().unwrap().split_whitespace().collect();
    assert_eq!(first[0], "POST");
    let path = first[1].to_string();
    let headers: BTreeMap<String, String> = lines
        .map(|line| {
            let (k, v) = line.split_once(':').unwrap();
            (k.to_ascii_lowercase(), v.trim().into())
        })
        .collect();
    let length: usize = headers["content-length"].parse().unwrap();
    assert!(length <= 2 * 1024 * 1024);
    let body_start = split + 4;
    while bytes.len() < body_start + length {
        let mut chunk = [0; 4096];
        let n = socket.read(&mut chunk).await.unwrap();
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    assert_eq!(bytes.len(), body_start + length);
    let boundary = headers["content-type"].split("boundary=").nth(1).unwrap();
    let delimiter = format!("--{boundary}").into_bytes();
    let body = &bytes[body_start..];
    let mut parts = vec![];
    let mut cursor = 0;
    loop {
        assert!(body[cursor..].starts_with(&delimiter));
        cursor += delimiter.len();
        if body[cursor..].starts_with(b"--") {
            break;
        }
        assert!(body[cursor..].starts_with(b"\r\n"));
        cursor += 2;
        let h_end = cursor
            + body[cursor..]
                .windows(4)
                .position(|p| p == b"\r\n\r\n")
                .unwrap();
        let headers = std::str::from_utf8(&body[cursor..h_end])
            .unwrap()
            .to_string();
        let name = headers
            .split("name=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap()
            .to_string();
        cursor = h_end + 4;
        let marker = [b"\r\n".as_slice(), &delimiter].concat();
        let end = cursor
            + body[cursor..]
                .windows(marker.len())
                .position(|p| p == marker)
                .unwrap();
        parts.push(Part {
            name,
            headers,
            bytes: body[cursor..end].to_vec(),
        });
        cursor = end + 2;
    }
    let (reply, mut response) = oneshot::channel();
    let (done_tx, done) = oneshot::channel();
    tx.send(Request {
        path,
        headers,
        parts,
        reply,
        done,
    })
    .await
    .unwrap();
    let mut probe = [0; 1];
    let response = tokio::select! {
        biased;
        _ = socket.read(&mut probe) => { let _ = done_tx.send(false); return; },
        result = &mut response => result,
    };
    let Ok((status, body)) = response else {
        let _ = done_tx.send(false);
        return;
    };
    let head = format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
    let success =
        socket.write_all(head.as_bytes()).await.is_ok() && socket.write_all(&body).await.is_ok();
    let _ = done_tx.send(success);
}
fn processor(provider: Provider, peer: &Peer, options: HttpSttOptions) -> Arc<HttpSttProcessor> {
    let mut config = match provider {
        Provider::OpenAi => SpeechConfig::openai("review-secret"),
        Provider::ElevenLabs => SpeechConfig::elevenlabs("review-secret"),
    };
    config.base_url = peer.base.clone();
    config.timeout = Duration::from_secs(30);
    let backend = match provider {
        Provider::OpenAi => HttpSttBackend::OpenAi {
            client: SpeechClient::new(config).unwrap(),
            request: TranscriptionRequest::default(),
        },
        Provider::ElevenLabs => HttpSttBackend::ElevenLabs {
            client: ElevenLabsHttpClient::new(config).unwrap(),
            request: ElevenLabsTranscriptionRequest::default(),
        },
    };
    Arc::new(HttpSttProcessor::new(backend, options).unwrap())
}
fn options() -> HttpSttOptions {
    HttpSttOptions {
        trailing_silence: Duration::ZERO,
        ..Default::default()
    }
}
fn audio(sample: u8, count: usize, user: &str, rate: u32) -> Frame {
    Frame::new(FrameData::UserAudio(UserAudioRawFrame {
        audio: AudioRawFrame {
            audio: vec![sample; count],
            sample_rate: rate,
            num_channels: 1,
            context_id: None,
        },
        user_id: user.into(),
    }))
}
fn vad(start: bool, detailed: bool) -> Frame {
    Frame::new(match (start, detailed) {
        (true, false) => FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        }),
        (false, false) => FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        }),
        (true, true) => FrameData::VadUserStartedSpeakingDetailed(VadUserStartedSpeakingFrame {
            start_secs: 0.0,
            timestamp: 0.0,
        }),
        (false, true) => FrameData::VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame {
            stop_secs: 0.0,
            timestamp: 0.0,
        }),
    })
}
struct Driver {
    worker: PipelineWorker,
    frames: Vec<DirectedFrame>,
}
impl Driver {
    async fn new(processors: Vec<Arc<dyn FrameProcessor>>, mut config: PipelineConfig) -> Self {
        config.shutdown_timeout = Duration::from_secs(3);
        let worker = Pipeline::new(processors).start(config).await.unwrap();
        let mut driver = Self {
            worker,
            frames: vec![],
        };
        driver
            .until(|f| {
                matches!(f.frame.data, FrameData::SttMetadata(_))
                    && f.direction == Direction::Downstream
            })
            .await;
        driver
    }
    async fn until(&mut self, predicate: impl Fn(&DirectedFrame) -> bool) {
        loop {
            let frame = tokio::time::timeout(DEADLINE, self.worker.recv())
                .await
                .expect("frame deadline")
                .expect("pipeline prematurely closed");
            let found = predicate(&frame);
            self.frames.push(frame);
            if found {
                return;
            }
        }
    }
    async fn seen(&mut self, frame: Frame, direction: Direction) {
        let id = frame.id;
        self.worker.sender().send(frame, direction).await.unwrap();
        self.until(|f| f.frame.id == id && f.direction == direction)
            .await;
    }
    async fn segment(&mut self, sample: u8, user: &str) {
        self.seen(vad(true, false), Direction::Downstream).await;
        self.seen(audio(sample, 32, user, 16000), Direction::Downstream)
            .await;
        self.seen(vad(false, false), Direction::Downstream).await;
    }
    async fn mute(&mut self, muted: bool) {
        self.worker
            .sender()
            .send(Frame::new(FrameData::SttMute(muted)), Direction::Downstream)
            .await
            .unwrap();
        // Both frames use the single system lane; the observed no-op audio proves mute ran.
        self.seen(audio(0, 0, "barrier", 16000), Direction::Downstream)
            .await;
    }
    async fn finish(mut self, end: bool) -> (Vec<DirectedFrame>, WorkerReport) {
        if end {
            self.worker.end().await.unwrap();
        }
        tokio::time::timeout(DEADLINE, async {
            while let Some(frame) = self.worker.recv().await {
                self.frames.push(frame);
            }
            let report = self.worker.join().await.unwrap();
            (self.frames, report)
        })
        .await
        .expect("owned tasks must terminate")
    }
}
fn transcript(frame: &DirectedFrame) -> Option<(&str, &str, bool)> {
    match &frame.frame.data {
        FrameData::Transcription(t) => Some((&t.text, &t.user_id, t.finalized)),
        FrameData::TranscriptionDetailed(t) => Some((&t.text.text, &t.user_id, t.finalized)),
        _ => None,
    }
}
fn transcripts(frames: &[DirectedFrame]) -> Vec<(&str, &str, bool)> {
    frames.iter().filter_map(transcript).collect()
}
fn usage(frames: &[DirectedFrame]) -> Vec<f64> {
    frames
        .iter()
        .flat_map(|f| match &f.frame.data {
            FrameData::Metrics(m) => m.iter(),
            _ => [].iter(),
        })
        .filter_map(|m| match m {
            Metric::SttUsage { audio_seconds, .. } => Some(*audio_seconds),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn review_real_aggregator_receives_one_final_with_exact_preroll_and_padding() {
    for provider in PROVIDERS {
        for detailed in [false, true] {
            for rate in [8000, 16000] {
                let mut peer = Peer::new().await;
                let stt = processor(
                    provider,
                    &peer,
                    HttpSttOptions {
                        detailed_transcripts: detailed,
                        ..Default::default()
                    },
                );
                let context = SharedContext::default();
                let aggregator = Arc::new(LLMUserAggregator::new(
                    context.clone(),
                    UserAggregatorConfig {
                        empty_user_turn: None,
                        ..Default::default()
                    },
                ));
                let mut config = PipelineConfig::default();
                config.start.audio_in_sample_rate = rate;
                config.start.enable_usage_metrics = true;
                let probe = Arc::new(TranscriptProbe::default());
                let mut d = Driver::new(vec![stt, probe.clone(), aggregator], config).await;
                d.seen(
                    audio(11, rate as usize * 2, "speaker-a", rate),
                    Direction::Downstream,
                )
                .await;
                d.seen(
                    audio(22, rate as usize, "speaker-a", rate),
                    Direction::Downstream,
                )
                .await;
                d.seen(vad(true, detailed), Direction::Downstream).await;
                d.seen(audio(33, 128, "speaker-a", rate), Direction::Downstream)
                    .await;
                d.seen(vad(false, detailed), Direction::Downstream).await;
                let request = peer.next().await;
                let wav = read_pcm16_wav(request.field("file").unwrap()).unwrap();
                assert_eq!(wav.format.sample_rate(), rate);
                assert_eq!(wav.format.channels(), 1);
                let expected = [
                    vec![11; rate as usize],
                    vec![22; rate as usize],
                    vec![33; 128],
                    vec![0; rate as usize],
                ]
                .concat();
                assert_eq!(wav.pcm, expected, "{provider:?}, rate={rate}");
                assert_eq!(request.field("file").unwrap().len(), expected.len() + 44);
                match provider {
                    Provider::OpenAi => {
                        assert_eq!(request.path, "/audio/transcriptions");
                        assert_eq!(request.headers["authorization"], "Bearer review-secret");
                        assert_eq!(request.field("model").unwrap(), b"gpt-transcribe");
                    }
                    Provider::ElevenLabs => {
                        assert_eq!(request.path, "/v1/speech-to-text");
                        assert_eq!(request.headers["xi-api-key"], "review-secret");
                        assert!(request
                            .parts
                            .iter()
                            .find(|p| p.name == "file")
                            .unwrap()
                            .headers
                            .contains("audio/x-wav"));
                    }
                }
                request.respond(200, json!({"text":"  reviewed words  ", "language_code":"eng", "words":[], "review_unknown":{"kept":true}})).await;
                probe.received().await;
                let (frames, report) = d.finish(true).await;
                assert_eq!(report.state, WorkerState::Ended);
                let captured = probe.frames.lock().unwrap().clone();
                assert_eq!(
                    transcripts(&captured),
                    [("reviewed words", "speaker-a", true)]
                );
                assert_eq!(context.snapshot_rich().unwrap().messages.len(), 1);
                assert_eq!(
                    context.snapshot_rich().unwrap().messages[0].content,
                    MessageContent::Text("reviewed words".into())
                );
                assert_eq!(
                    usage(&frames),
                    [expected.len() as f64 / (rate as f64 * 2.0)]
                );
                assert!(!frames.iter().any(|f| matches!(
                    f.frame.data,
                    FrameData::ProposedUserStoppedSpeaking
                        | FrameData::EagerTranscription(_)
                        | FrameData::InterimTranscription(_)
                )));
                peer.assert_none();
            }
        }
    }
}

#[tokio::test]
async fn review_end_drains_only_closed_fifo_with_immutable_speakers() {
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(provider, &peer, options());
        let mut config = PipelineConfig::default();
        config.start.enable_usage_metrics = true;
        let mut d = Driver::new(vec![stt], config).await;
        d.segment(1, "speaker-a").await;
        let a = peer.next().await;
        d.segment(2, "speaker-b").await;
        d.seen(vad(true, false), Direction::Downstream).await;
        d.seen(audio(3, 32, "unfinished-c", 16000), Direction::Downstream)
            .await;
        d.seen(
            Frame::text("audio/control path remains live"),
            Direction::Downstream,
        )
        .await;
        peer.assert_none();
        d.worker.end().await.unwrap();
        assert_eq!(a.pcm(), vec![1; 32]);
        a.respond(200, json!({"text":"first", "words":[]})).await;
        let b = peer.next().await;
        assert_eq!(b.pcm(), vec![2; 32]);
        b.respond(200, json!({"text":"second", "words":[]})).await;
        let (frames, report) = tokio::select! {
            result = d.finish(false) => result,
            Some(unexpected) = peer.requests.recv() => panic!("End uploaded an unfinished third utterance: {} PCM bytes", unexpected.pcm().len()),
        };
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(
            transcripts(&frames),
            [("first", "speaker-a", true), ("second", "speaker-b", true)]
        );
        let terminal = frames
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::End(_)))
            .unwrap();
        assert!(frames
            .iter()
            .enumerate()
            .filter(|(_, f)| transcript(f).is_some())
            .all(|(i, _)| i < terminal));
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::End(_)))
                .count(),
            1
        );
        assert_eq!(usage(&frames), [0.001, 0.001]);
        peer.assert_none();
    }
}

#[tokio::test]
async fn review_mute_revokes_inflight_queue_and_open_audio_then_fresh_unmute_works() {
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(
            provider,
            &peer,
            HttpSttOptions {
                queue_capacity: 2,
                max_pending_audio_bytes: 96,
                ..options()
            },
        );
        let mut config = PipelineConfig::default();
        config.start.enable_usage_metrics = true;
        config.start.enable_metrics = true;
        let mut d = Driver::new(vec![stt], config).await;
        d.segment(1, "old-a").await;
        let a = peer.next().await;
        d.segment(2, "old-b").await;
        d.segment(3, "old-c").await;
        d.seen(vad(true, false), Direction::Downstream).await;
        d.seen(audio(4, 32, "old-d", 16000), Direction::Downstream)
            .await;
        d.mute(true).await;
        a.disconnected().await;
        d.seen(audio(5, 32, "muted", 16000), Direction::Downstream)
            .await;
        d.seen(vad(false, false), Direction::Downstream).await;
        d.mute(false).await;
        d.seen(vad(false, false), Direction::Downstream).await;
        d.segment(6, "fresh-e").await;
        let e = peer.next().await;
        assert_eq!(e.pcm(), vec![6; 32]);
        e.respond(200, json!({"text":"fresh", "words":[]})).await;
        d.until(|f| transcript(f).is_some()).await;
        let (frames, report) = d.finish(true).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(transcripts(&frames), [("fresh", "fresh-e", true)]);
        assert_eq!(usage(&frames), [0.001, 0.001]);
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::Error(_))));
        peer.assert_none();
    }
}

#[tokio::test]
async fn review_interruption_preserves_inflight_and_queued_input_stt() {
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(provider, &peer, options());
        let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
        d.segment(7, "first").await;
        let a = peer.next().await;
        d.segment(8, "second").await;
        d.seen(Frame::interruption(), Direction::Downstream).await;
        a.respond(200, json!({"text":"survives-a","words":[]}))
            .await;
        let b = peer.next().await;
        b.respond(200, json!({"text":"survives-b","words":[]}))
            .await;
        d.until(|f| transcript(f).is_some_and(|t| t.0 == "survives-b"))
            .await;
        let (frames, report) = d.finish(true).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(
            transcripts(&frames),
            [
                ("survives-a", "first", true),
                ("survives-b", "second", true)
            ]
        );
        peer.assert_none();
    }
}

#[tokio::test]
async fn review_cancel_preempts_prepare_end_and_releases_active_http() {
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(provider, &peer, options());
        let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
        d.segment(7, "first").await;
        let a = peer.next().await;
        d.segment(8, "second").await;
        d.worker.end().await.unwrap();
        d.worker.cancel("review cancellation").await.unwrap();
        let (frames, report) = d.finish(false).await;
        assert_eq!(report.state, WorkerState::Cancelled);
        assert!(transcripts(&frames).is_empty());
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::End(_))));
        a.disconnected().await;
        peer.assert_none();
    }
}

#[tokio::test]
async fn review_same_true_health_restore_rejects_old_success_and_permanent_error() {
    for provider in PROVIDERS {
        for status in [200, 401] {
            let mut peer = Peer::new().await;
            let stt = processor(provider, &peer, options());
            let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
            d.segment(7, "old").await;
            let a = peer.next().await;
            let health = d.worker.processor_health(0).unwrap();
            let generation = health.generation();
            assert!(!health.set_usable(true));
            assert_ne!(generation, health.generation());
            a.respond_or_disconnected(
                status,
                json!({"text":"stale secret", "message":"private provider detail","words":[]}),
            )
            .await;
            d.segment(8, "fresh").await;
            let b = peer.next().await;
            b.respond(200, json!({"text":"fresh","words":[]})).await;
            d.until(|f| transcript(f).is_some_and(|t| t.0 == "fresh"))
                .await;
            let (frames, report) = d.finish(true).await;
            assert_eq!(report.state, WorkerState::Ended);
            assert!(health.is_usable());
            assert_eq!(transcripts(&frames), [("fresh", "fresh", true)]);
            assert!(!frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::Error(_))));
            peer.assert_none();
        }
    }
}

#[tokio::test]
async fn review_upstream_detailed_vad_geometry_failure_discards_whole_utterance() {
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(provider, &peer, options());
        let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
        d.seen(vad(true, true), Direction::Upstream).await;
        d.seen(audio(1, 32, "u", 16000), Direction::Downstream)
            .await;
        d.seen(audio(2, 3, "u", 16000), Direction::Downstream).await;
        d.seen(audio(3, 32, "u", 16000), Direction::Downstream)
            .await;
        d.seen(vad(false, true), Direction::Upstream).await;
        d.seen(
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Downstream,
        )
        .await;
        d.seen(vad(false, true), Direction::Upstream).await;
        d.seen(vad(true, true), Direction::Upstream).await;
        d.seen(audio(4, 32, "fresh", 16000), Direction::Downstream)
            .await;
        d.seen(vad(false, true), Direction::Upstream).await;
        let request = peer.next().await;
        assert_eq!(request.pcm(), vec![4; 32]);
        request
            .respond(200, json!({"text":"valid","words":[]}))
            .await;
        d.until(|f| transcript(f).is_some()).await;
        let (frames, report) = d.finish(true).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(transcripts(&frames), [("valid", "fresh", true)]);
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                .count(),
            1
        );
        peer.assert_none();
    }
}

#[tokio::test]
async fn review_settings_snapshot_and_typed_delta_precedence_are_atomic() {
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(provider, &peer, options());
        let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
        d.segment(1, "a").await;
        let a = peer.next().await;
        d.segment(2, "b").await;
        let update = Frame::new(FrameData::SttUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                settings: [("language".into(), json!("de"))].into_iter().collect(),
                delta: Some(SttSettings {
                    language: Setting::Value("fr-FR".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )));
        d.worker
            .sender()
            .send(update, Direction::Downstream)
            .await
            .unwrap();
        d.seen(Frame::text("settings committed"), Direction::Downstream)
            .await;
        d.segment(3, "c").await;
        a.respond(200, json!({"text":"a","words":[]})).await;
        let b = peer.next().await;
        assert_eq!(
            b.field(match provider {
                Provider::OpenAi => "language",
                Provider::ElevenLabs => "language_code",
            })
            .unwrap(),
            match provider {
                Provider::OpenAi => b"en".as_slice(),
                Provider::ElevenLabs => b"eng".as_slice(),
            }
        );
        b.respond(200, json!({"text":"b","words":[]})).await;
        let c = peer.next().await;
        assert_eq!(
            c.field(match provider {
                Provider::OpenAi => "language",
                Provider::ElevenLabs => "language_code",
            })
            .unwrap(),
            match provider {
                Provider::OpenAi => b"fr".as_slice(),
                Provider::ElevenLabs => b"fra".as_slice(),
            }
        );
        c.respond(200, json!({"text":"c","words":[]})).await;
        d.until(|f| transcript(f).is_some_and(|t| t.0 == "c")).await;
        let (frames, report) = d.finish(true).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(
            transcripts(&frames),
            [("a", "a", true), ("b", "b", true), ("c", "c", true)]
        );
        peer.assert_none();
    }
}

#[tokio::test]
async fn review_count_and_byte_limits_reject_without_retry_and_release_on_completion() {
    for provider in PROVIDERS {
        for count_limited in [false, true] {
            let mut peer = Peer::new().await;
            let limits = HttpSttOptions {
                queue_capacity: if count_limited { 1 } else { 4 },
                max_pending_audio_bytes: if count_limited { 128 } else { 64 },
                ..options()
            };
            let stt = processor(provider, &peer, limits);
            let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
            d.segment(1, "a").await;
            let a = peer.next().await;
            d.segment(2, "b").await;
            d.segment(3, "refused").await;
            d.seen(audio(0, 0, "barrier", 16000), Direction::Downstream)
                .await;
            assert_eq!(
                d.frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                    .count(),
                1
            );
            assert!(d.worker.processor_health(0).unwrap().is_usable());
            peer.assert_none();
            a.respond(200, json!({"text":"a","words":[]})).await;
            let b = peer.next().await;
            assert_eq!(b.pcm(), vec![2; 32]);
            b.respond(200, json!({"text":"b","words":[]})).await;
            d.until(|f| transcript(f).is_some_and(|t| t.0 == "b")).await;
            d.segment(4, "after-release").await;
            let c = peer.next().await;
            assert_eq!(c.pcm(), vec![4; 32]);
            c.respond(200, json!({"text":"c","words":[]})).await;
            d.until(|f| transcript(f).is_some_and(|t| t.0 == "c")).await;
            let (frames, report) = d.finish(true).await;
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(
                transcripts(&frames),
                [
                    ("a", "a", true),
                    ("b", "b", true),
                    ("c", "after-release", true)
                ]
            );
            peer.assert_none();
        }
    }
}

#[tokio::test]
async fn review_local_settings_target_skips_same_named_instance_and_rejects_atomic_bad_update() {
    for provider in PROVIDERS {
        let mut first_peer = Peer::new().await;
        let mut second_peer = Peer::new().await;
        let first = processor(provider, &first_peer, options());
        let second = processor(provider, &second_peer, options());
        let mut a = Driver::new(vec![first], PipelineConfig::default()).await;
        let mut b = Driver::new(vec![second.clone()], PipelineConfig::default()).await;
        let update = Frame::new(FrameData::SttUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                settings: [("language".into(), json!("fr"))].into_iter().collect(),
                service: Some(ProcessorReference::Local(LocalProcessorReference(second))),
                ..Default::default()
            },
        )));
        a.seen(update.clone(), Direction::Downstream).await;
        b.worker
            .sender()
            .send(update, Direction::Downstream)
            .await
            .unwrap();
        b.seen(Frame::text("local update barrier"), Direction::Downstream)
            .await;
        let bad = Frame::new(FrameData::SttUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                settings: [
                    ("language".into(), json!("de")),
                    (
                        "unsupported_private_setting".into(),
                        json!("review-sensitive-prompt"),
                    ),
                ]
                .into_iter()
                .collect(),
                ..Default::default()
            },
        )));
        b.worker
            .sender()
            .send(bad, Direction::Downstream)
            .await
            .unwrap();
        b.seen(
            Frame::text("rejected update barrier"),
            Direction::Downstream,
        )
        .await;
        a.segment(1, "a").await;
        b.segment(2, "b").await;
        let ar = first_peer.next().await;
        let br = second_peer.next().await;
        let key = match provider {
            Provider::OpenAi => "language",
            Provider::ElevenLabs => "language_code",
        };
        assert_eq!(
            ar.field(key).unwrap(),
            match provider {
                Provider::OpenAi => b"en".as_slice(),
                Provider::ElevenLabs => b"eng".as_slice(),
            }
        );
        assert_eq!(
            br.field(key).unwrap(),
            match provider {
                Provider::OpenAi => b"fr".as_slice(),
                Provider::ElevenLabs => b"fra".as_slice(),
            }
        );
        ar.respond(200, json!({"text":"a","words":[]})).await;
        br.respond(200, json!({"text":"b","words":[]})).await;
        a.until(|f| transcript(f).is_some()).await;
        b.until(|f| transcript(f).is_some()).await;
        let (_, ar) = a.finish(true).await;
        let (bf, br) = b.finish(true).await;
        assert_eq!(ar.state, WorkerState::Ended);
        assert_eq!(br.state, WorkerState::Ended);
        assert!(bf
            .iter()
            .filter_map(|f| match &f.frame.data {
                FrameData::Error(e) => Some(e),
                _ => None,
            })
            .all(|e| !e.error.contains("review-sensitive-prompt")));
        assert_eq!(
            bf.iter()
                .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                .count(),
            1
        );
        first_peer.assert_none();
        second_peer.assert_none();
    }
}

#[tokio::test]
async fn review_speaker_switch_inside_utterance_never_mixes_or_relabels_audio() {
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(provider, &peer, options());
        let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
        d.seen(audio(1, 32, "old-idle", 16000), Direction::Downstream)
            .await;
        d.seen(audio(2, 32, "new-idle", 16000), Direction::Downstream)
            .await;
        d.seen(vad(true, false), Direction::Downstream).await;
        d.seen(audio(3, 32, "new-idle", 16000), Direction::Downstream)
            .await;
        d.seen(vad(false, false), Direction::Downstream).await;
        let idle = peer.next().await;
        assert_eq!(idle.pcm(), [vec![2; 32], vec![3; 32]].concat());
        idle.respond(200, json!({"text":"idle-owner","words":[]}))
            .await;
        d.until(|f| transcript(f).is_some()).await;
        d.seen(vad(true, false), Direction::Downstream).await;
        d.seen(audio(4, 32, "first-active", 16000), Direction::Downstream)
            .await;
        d.seen(audio(5, 32, "changed-active", 16000), Direction::Downstream)
            .await;
        d.seen(vad(false, false), Direction::Downstream).await;
        d.segment(6, "fresh").await;
        let fresh = peer.next().await;
        assert_eq!(fresh.pcm(), vec![6; 32]);
        fresh.respond(200, json!({"text":"fresh","words":[]})).await;
        d.until(|f| transcript(f).is_some_and(|t| t.0 == "fresh"))
            .await;
        let (frames, report) = d.finish(true).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(
            transcripts(&frames),
            [("idle-owner", "new-idle", true), ("fresh", "fresh", true)]
        );
        peer.assert_none();
    }
}

#[tokio::test]
async fn review_muted_passthrough_is_separate_from_stt_authority() {
    for provider in PROVIDERS {
        for passthrough in [false, true] {
            let mut peer = Peer::new().await;
            let stt = processor(
                provider,
                &peer,
                HttpSttOptions {
                    audio_passthrough: passthrough,
                    ..options()
                },
            );
            let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
            d.worker
                .sender()
                .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
                .await
                .unwrap();
            d.seen(vad(true, false), Direction::Downstream).await;
            let muted_audio = audio(9, 32, "muted", 16000);
            let id = muted_audio.id;
            d.worker
                .sender()
                .send(muted_audio, Direction::Downstream)
                .await
                .unwrap();
            d.seen(vad(false, false), Direction::Downstream).await;
            let (frames, report) = d.finish(true).await;
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(frames.iter().any(|f| f.frame.id == id), passthrough);
            assert!(transcripts(&frames).is_empty());
            assert!(!frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::SttMute(_))));
            peer.assert_none();
        }
    }
}

#[tokio::test]
async fn review_disconnected_and_malformed_peers_yield_one_sanitized_recoverable_error() {
    for provider in PROVIDERS {
        for malformed in [false, true] {
            let mut peer = Peer::new().await;
            let stt = processor(provider, &peer, options());
            let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
            d.segment(1, "u").await;
            let request = peer.next().await;
            if malformed {
                request
                    .reply
                    .send((
                        200,
                        br#"{"text":{"private":"sentinel-json-body"}}"#.to_vec(),
                    ))
                    .unwrap();
                assert!(request.done.await.unwrap());
            } else {
                drop(request.reply);
                assert!(!request.done.await.unwrap());
            }
            d.until(|f| matches!(f.frame.data, FrameData::Error(_)))
                .await;
            assert!(d.worker.processor_health(0).unwrap().is_usable());
            d.segment(2, "recovered").await;
            let good = peer.next().await;
            good.respond(200, json!({"text":"good","words":[]})).await;
            d.until(|f| transcript(f).is_some()).await;
            let (frames, report) = d.finish(true).await;
            assert_eq!(report.state, WorkerState::Ended);
            assert_eq!(transcripts(&frames), [("good", "recovered", true)]);
            let errors: Vec<_> = frames
                .iter()
                .filter_map(|f| match &f.frame.data {
                    FrameData::Error(e) => Some(e),
                    _ => None,
                })
                .collect();
            assert_eq!(errors.len(), 1);
            assert!(!format!("{:?}", errors[0]).contains("sentinel-json-body"));
            assert!(!format!("{:?}", errors[0]).contains("review-secret"));
            peer.assert_none();
        }
    }
}

#[tokio::test]
async fn review_normalized_noop_setting_cannot_restore_health_or_unmute() {
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(provider, &peer, options());
        let mut d = Driver::new(vec![stt], PipelineConfig::default()).await;
        let health = d.worker.processor_health(0).unwrap();
        health.set_usable(false);
        let before = health.generation();
        d.worker
            .sender()
            .send(
                Frame::new(FrameData::SttUpdateSettings(Box::new(
                    ServiceUpdateSettingsFrame {
                        settings: [("language".into(), json!("en-US"))].into_iter().collect(),
                        ..Default::default()
                    },
                ))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        d.seen(
            Frame::text("normalized no-op barrier"),
            Direction::Downstream,
        )
        .await;
        assert_eq!(
            health.generation(),
            before,
            "normalization-equivalent setting must not grant recovery"
        );
        d.mute(true).await;
        d.worker
            .sender()
            .send(
                Frame::new(FrameData::SttUpdateSettings(Box::new(
                    ServiceUpdateSettingsFrame {
                        settings: [("language".into(), json!("fr"))].into_iter().collect(),
                        ..Default::default()
                    },
                ))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        d.seen(
            Frame::text("effective recovery barrier"),
            Direction::Downstream,
        )
        .await;
        assert!(health.is_usable());
        d.segment(1, "still-muted").await;
        d.mute(false).await;
        d.segment(2, "fresh").await;
        let r = peer.next().await;
        assert_eq!(r.pcm(), vec![2; 32]);
        r.respond(200, json!({"text":"fresh","words":[]})).await;
        d.until(|f| transcript(f).is_some()).await;
        let (frames, report) = d.finish(true).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(transcripts(&frames), [("fresh", "fresh", true)]);
        peer.assert_none();
    }
}

#[tokio::test]
#[ignore = "requires qualified native Silero model/runtime"]
async fn review_real_silero_upstream_vad_closes_http_stt_before_native_aggregation() {
    use pipecat_vad::{
        ProcessorOptions, RuntimeOptions, SileroModel, SileroVadAnalyzer, SileroVadProcessor,
    };
    use std::path::PathBuf;
    let runtime = RuntimeOptions {
        library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .expect("qualified native ONNX runtime path required"),
    };
    let model_path = std::env::var_os("PIPECAT_SILERO_MODEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
        });
    for provider in PROVIDERS {
        let mut peer = Peer::new().await;
        let stt = processor(provider, &peer, options());
        let analyzer = SileroVadAnalyzer::new(
            SileroModel::load(&runtime, &model_path).unwrap(),
            16000,
            VadParams::default(),
        )
        .unwrap();
        let vad = Arc::new(
            SileroVadProcessor::new(
                analyzer,
                ProcessorOptions {
                    queue_capacity: 256,
                    audio_idle_timeout: Duration::ZERO,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let context = SharedContext::default();
        let user = Arc::new(LLMUserAggregator::new(
            context.clone(),
            UserAggregatorConfig {
                empty_user_turn: None,
                ..Default::default()
            },
        ));
        let probe = Arc::new(TranscriptProbe::default());
        let mut d = Driver::new(
            vec![stt, vad.clone(), probe.clone(), user],
            PipelineConfig::default(),
        )
        .await;
        let speech = include_bytes!("../../pipecat-vad/tests/fixtures/smart_turn_speech.pcm16");
        for chunk in speech.as_chunks::<1024>().0.iter().take(40) {
            d.seen(
                Frame::new(FrameData::InputAudio(AudioRawFrame {
                    audio: chunk.to_vec(),
                    sample_rate: 16000,
                    num_channels: 1,
                    context_id: None,
                })),
                Direction::Downstream,
            )
            .await;
        }
        if !d
            .frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::VadUserStartedSpeaking(_)))
        {
            d.until(|f| matches!(f.frame.data, FrameData::VadUserStartedSpeaking(_)))
                .await;
        }
        for _ in 0..60 {
            d.seen(
                Frame::new(FrameData::InputAudio(AudioRawFrame {
                    audio: vec![0; 1024],
                    sample_rate: 16000,
                    num_channels: 1,
                    context_id: None,
                })),
                Direction::Downstream,
            )
            .await;
        }
        if !d
            .frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::VadUserStoppedSpeaking(_)))
        {
            d.until(|f| matches!(f.frame.data, FrameData::VadUserStoppedSpeaking(_)))
                .await;
        }
        let request = peer.next().await;
        let pcm = request.pcm();
        assert!(
            pcm.iter().any(|b| *b != 0),
            "real speech must reach upload, not only tail silence"
        );
        request
            .respond(200, json!({"text":"native VAD composition","words":[]}))
            .await;
        probe.received().await;
        let (_, report) = d.finish(true).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert!(!vad.is_running(), "native inference owner must join on End");
        let captured = probe.frames.lock().unwrap().clone();
        assert_eq!(
            transcripts(&captured),
            [("native VAD composition", "", true)]
        );
        assert_eq!(context.snapshot_rich().unwrap().messages.len(), 1);
        assert_eq!(
            context.snapshot_rich().unwrap().messages[0].content,
            MessageContent::Text("native VAD composition".into())
        );
        peer.assert_none();
    }
}
