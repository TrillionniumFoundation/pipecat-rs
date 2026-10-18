//! Independent loopback fixtures. Every returned result follows actual owner joins.
//! The host recognizes exact synthetic bytes, not human speech or audible output.

use super::common::*;
use pipecat_core::*;
use pipecat_evals::{bot_host::*, matcher::ScriptResult, Cancellation};
use pipecat_transports::SessionConfig;
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
    task::{JoinHandle, JoinSet},
};

pub fn pcm(seed: u8) -> Vec<u8> {
    // Three distinct complete packets and a 74-byte tail. Every sample is nonzero.
    [
        [seed, 0x31].repeat(640),
        [seed.wrapping_add(1), 0x52].repeat(640),
        [seed.wrapping_add(2), 0x73].repeat(640),
        [seed.wrapping_add(3), 0x24].repeat(37),
    ]
    .concat()
}
pub fn packets(bytes: &[u8]) -> Vec<Vec<u8>> {
    bytes
        .chunks(1280)
        .map(|chunk| {
            let mut padded = vec![0; 1280];
            padded[..chunk.len()].copy_from_slice(chunk);
            padded
        })
        .collect()
}
pub fn wav(pcm: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend(b"RIFF");
    b.extend((36 + pcm.len() as u32).to_le_bytes());
    b.extend(b"WAVEfmt ");
    b.extend(16u32.to_le_bytes());
    b.extend(1u16.to_le_bytes());
    b.extend(1u16.to_le_bytes());
    b.extend(16_000u32.to_le_bytes());
    b.extend(32_000u32.to_le_bytes());
    b.extend(2u16.to_le_bytes());
    b.extend(16u16.to_le_bytes());
    b.extend(b"data");
    b.extend((pcm.len() as u32).to_le_bytes());
    b.extend(pcm);
    b
}
pub fn transport() -> SessionConfig {
    SessionConfig {
        queue_capacity: 8,
        connect_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_millis(500),
        close_timeout: Duration::from_millis(100),
        ..Default::default()
    }
}

pub struct Trace {
    pub audio: Mutex<Vec<Vec<u8>>>,
    pub errors: Mutex<Vec<String>>,
    pub text: AtomicUsize,
    pub completed: AtomicUsize,
    pub cleaned: AtomicUsize,
    pub dropped: AtomicUsize,
    pub final_arrived: Notify,
    pub first_arrived: Notify,
    pub final_gate: Semaphore,
    pub gated: AtomicBool,
    pub chatty: AtomicBool,
    pub early: AtomicBool,
    pub interrupt: AtomicBool,
}
impl Default for Trace {
    fn default() -> Self {
        Self {
            audio: Mutex::new(Vec::new()),
            errors: Mutex::new(Vec::new()),
            text: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            cleaned: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            final_arrived: Notify::new(),
            first_arrived: Notify::new(),
            final_gate: Semaphore::new(0),
            gated: AtomicBool::new(false),
            chatty: AtomicBool::new(false),
            early: AtomicBool::new(false),
            interrupt: AtomicBool::new(false),
        }
    }
}

struct Consumer {
    trace: Arc<Trace>,
    clips: Mutex<VecDeque<VecDeque<Vec<u8>>>>,
    number: AtomicUsize,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        self.trace.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
async fn response(ctx: &ProcessorContext, text: String) -> Result<(), ProcessorError> {
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::new(text)),
        FrameData::LlmResponseEnd,
    ] {
        ctx.push(Frame::new(data), Direction::Downstream).await?;
    }
    Ok(())
}
#[async_trait]
impl FrameProcessor for Consumer {
    fn name(&self) -> &str {
        "complete-user-speech-pcm-consumer"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::InputAudio(audio) => {
                self.trace.audio.lock().unwrap().push(audio.audio.clone());
                if audio.audio.iter().all(|&b| b == 0) {
                    if self.trace.interrupt.swap(false, Ordering::SeqCst) {
                        ctx.push(
                            Frame::new(FrameData::OutputTransportMessageUrgent(
                                json!({"label":"rtvi-ai","type":"bot-interrupted","data":{}}),
                            )),
                            direction,
                        )
                        .await?;
                    }
                    if self.trace.chatty.load(Ordering::SeqCst) {
                        ctx.push(
                            Frame::new(FrameData::Transcription(TranscriptionFrame {
                                text: "unrelated continuing socket event".into(),
                                finalized: true,
                                ..Default::default()
                            })),
                            direction,
                        )
                        .await?;
                    }
                } else {
                    let (first, last, valid) = {
                        let mut clips = self.clips.lock().unwrap();
                        match clips.front_mut() {
                            Some(clip) => {
                                let first = clip.len() == 4;
                                let want = clip.pop_front().unwrap();
                                let last = clip.is_empty();
                                if last {
                                    clips.pop_front();
                                }
                                (
                                    first,
                                    last,
                                    audio.audio == want
                                        && audio.sample_rate == 16000
                                        && audio.num_channels == 1,
                                )
                            }
                            None => (false, false, false),
                        }
                    };
                    if !valid {
                        self.trace
                            .errors
                            .lock()
                            .unwrap()
                            .push("unexpected PCM, ordering, format, or tail padding".into());
                    }
                    if first {
                        self.trace.first_arrived.notify_one();
                        if self.trace.early.load(Ordering::SeqCst) {
                            response(ctx, "early assertion".into()).await?;
                        }
                    }
                    if last && valid {
                        self.trace.final_arrived.notify_one();
                        if self.trace.gated.load(Ordering::SeqCst) {
                            let permit = self
                                .trace
                                .final_gate
                                .acquire()
                                .await
                                .map_err(|_| ProcessorError::fatal("fixture gate closed"))?;
                            permit.forget();
                        }
                        if self.trace.errors.lock().unwrap().is_empty() {
                            let n = self.number.fetch_add(1, Ordering::SeqCst) + 1;
                            self.trace.completed.fetch_add(1, Ordering::SeqCst);
                            response(ctx, format!("complete PCM {n}")).await?;
                        }
                    }
                }
            }
            FrameData::Text(_) | FrameData::LlmMessagesAppend(_) => {
                self.trace.text.fetch_add(1, Ordering::SeqCst);
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.trace.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Factory {
    trace: Arc<Trace>,
    clips: Vec<Vec<u8>>,
}
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if !settings.skip_tts {
            return Err(BotHostError::Invalid("fixture requires skip_tts".into()));
        }
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(Consumer {
                trace: self.trace.clone(),
                clips: Mutex::new(self.clips.iter().map(|c| packets(c).into()).collect()),
                number: AtomicUsize::new(0),
            })],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
pub struct Host {
    pub url: String,
    pub trace: Arc<Trace>,
    cancel: Cancellation,
    task: JoinHandle<Result<EvalBotHostReport, BotHostError>>,
}
impl Host {
    pub async fn start(clips: Vec<Vec<u8>>) -> Self {
        let host = EvalBotHost::bind(
            "127.0.0.1:0".parse().unwrap(),
            EvalBotHostConfig {
                startup_timeout: Duration::from_secs(2),
                session_timeout: WAIT,
                host_timeout: Duration::from_secs(20),
                shutdown_timeout: Duration::from_secs(2),
                transport: transport(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .with_raw_audio_input(true);
        let url = format!("ws://{}/", host.local_addr().unwrap());
        let cancel = Cancellation::new();
        let c = cancel.clone();
        let trace = Arc::new(Trace::default());
        let factory = Factory {
            trace: trace.clone(),
            clips,
        };
        let task = tokio::spawn(async move { host.serve(&factory, c).await });
        Self {
            url,
            trace,
            cancel,
            task,
        }
    }
    pub async fn finish(mut self) -> (Result<EvalBotHostReport, BotHostError>, Arc<Trace>) {
        self.trace.final_gate.add_permits(128);
        self.cancel.cancel();
        let result = match tokio::time::timeout(WAIT, &mut self.task).await {
            Ok(r) => r,
            Err(_) => {
                let actual = self.task.await;
                panic!("host cleanup deadline after actual join: {actual:?}");
            }
        };
        (result.expect("host fixture task panicked"), self.trace)
    }
}
pub fn assert_consumed(trace: &Trace, clips: &[Vec<u8>]) {
    let errors = trace.errors.lock().unwrap().clone();
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(
        trace.text.load(Ordering::SeqCst),
        0,
        "reference text was injected"
    );
    assert_eq!(trace.completed.load(Ordering::SeqCst), clips.len());
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    let actual: Vec<_> = trace
        .audio
        .lock()
        .unwrap()
        .iter()
        .filter(|b| b.iter().any(|&v| v != 0))
        .cloned()
        .collect();
    let expected: Vec<_> = clips.iter().flat_map(|c| packets(c)).collect();
    assert_eq!(actual, expected, "whole PCM sequence and final padding");
}
pub fn assert_no_speech(trace: &Trace) {
    assert_eq!(trace.text.load(Ordering::SeqCst), 0);
    assert_eq!(trace.completed.load(Ordering::SeqCst), 0);
    assert!(trace
        .audio
        .lock()
        .unwrap()
        .iter()
        .all(|p| p.iter().all(|&b| b == 0)));
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
pub struct Reply {
    pub status: u16,
    pub body: Vec<u8>,
    pub mime: &'static str,
    pub delay: Duration,
    pub truncated: bool,
    pub stalled: bool,
    pub redirect: Option<String>,
}
impl Reply {
    pub fn pcm(body: Vec<u8>) -> Self {
        Self {
            status: 200,
            body,
            mime: "audio/pcm",
            delay: Duration::ZERO,
            truncated: false,
            stalled: false,
            redirect: None,
        }
    }
}
#[derive(Debug, Clone)]
pub struct Request {
    pub header: String,
    pub body: Value,
}
#[derive(Default)]
pub struct HttpTrace {
    pub requests: Mutex<Vec<Request>>,
    pub admitted: Notify,
    pub active: AtomicUsize,
    pub closed: AtomicUsize,
    pub joined: AtomicUsize,
}
struct HandlerGuard(Arc<HttpTrace>);
impl Drop for HandlerGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.joined.fetch_add(1, Ordering::SeqCst);
    }
}
pub struct Http {
    pub url: String,
    pub trace: Arc<HttpTrace>,
    cancel: Cancellation,
    task: JoinHandle<Result<(), Error>>,
}
impl Http {
    pub async fn start(replies: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let cancel = Cancellation::new();
        let c = cancel.clone();
        let trace = Arc::new(HttpTrace::default());
        let t = trace.clone();
        let task = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            let mut next = 0;
            loop {
                tokio::select! {
                    biased;
                    _ = c.cancelled() => break,
                    accepted = listener.accept() => {
                        let (stream, _) = accepted?;
                        let reply = replies.get(next).cloned().unwrap_or_else(|| { let mut r = Reply::pcm(vec![]); r.status = 503; r });
                        next += 1;
                        let t = t.clone();
                        let c = c.clone();
                        t.active.fetch_add(1, Ordering::SeqCst);
                        handlers.spawn(async move {
                            let _guard = HandlerGuard(t.clone());
                            handle_http(stream, reply, t, c).await
                        });
                    }
                }
            }
            while let Some(joined) = handlers.join_next().await {
                joined??;
            }
            Ok(())
        });
        Self {
            url,
            trace,
            cancel,
            task,
        }
    }
    pub async fn finish(mut self) -> Arc<HttpTrace> {
        self.cancel.cancel();
        let result = match tokio::time::timeout(WAIT, &mut self.task).await {
            Ok(r) => r,
            Err(_) => {
                let actual = self.task.await;
                panic!("HTTP fixture deadline after actual join: {actual:?}");
            }
        };
        result
            .expect("HTTP owner panicked")
            .expect("HTTP fixture failed");
        self.trace
    }
}
async fn handle_http(
    mut stream: TcpStream,
    reply: Reply,
    trace: Arc<HttpTrace>,
    cancel: Cancellation,
) -> Result<(), Error> {
    let mut bytes = Vec::new();
    let end;
    loop {
        let mut chunk = [0; 2048];
        let n = tokio::select! { biased; _ = cancel.cancelled() => return Ok(()), r = stream.read(&mut chunk) => r? };
        if n == 0 {
            return Ok(());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > 64 * 1024 {
            return Err("fixture HTTP header limit".into());
        }
        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            end = i + 4;
            break;
        }
    }
    let header = String::from_utf8(bytes[..end].to_vec())?;
    let len: usize = header
        .lines()
        .find_map(|line| {
            let (key, value) = line.split_once(':')?;
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().ok())
                .flatten()
        })
        .ok_or("missing fixture request length")?;
    if len > 64 * 1024 {
        return Err("fixture request limit".into());
    }
    while bytes.len() - end < len {
        let mut chunk = [0; 2048];
        let n = tokio::select! { biased; _ = cancel.cancelled() => return Ok(()), r = stream.read(&mut chunk) => r? };
        if n == 0 {
            return Err("truncated fixture request".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    trace.requests.lock().unwrap().push(Request {
        header,
        body: serde_json::from_slice(&bytes[end..end + len])?,
    });
    trace.admitted.notify_one();
    if reply.delay != Duration::ZERO {
        tokio::select! { biased; _ = cancel.cancelled() => return Ok(()), _ = tokio::time::sleep(reply.delay) => {} }
    }
    let extra = reply
        .redirect
        .as_ref()
        .map(|url| format!("Location: {url}\r\n"))
        .unwrap_or_default();
    let head = if reply.truncated || reply.stalled {
        format!("HTTP/1.1 {} Fixture\r\nContent-Type: {}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n", reply.status, reply.mime, reply.body.len() + 1280, extra)
    } else {
        format!("HTTP/1.1 {} Fixture\r\nContent-Type: {}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n{}\r\n", reply.status, reply.mime, extra)
    };
    let write = async {
        stream.write_all(head.as_bytes()).await?;
        if reply.truncated || reply.stalled {
            stream.write_all(&reply.body).await?;
        } else {
            // Deliberately split PCM samples and transport-frame boundaries.
            let mut offset = 0;
            for size in [1, 1278, 5, 511, 7].into_iter().cycle() {
                if offset == reply.body.len() {
                    break;
                }
                let n = size.min(reply.body.len() - offset);
                stream.write_all(format!("{n:x}\r\n").as_bytes()).await?;
                stream.write_all(&reply.body[offset..offset + n]).await?;
                stream.write_all(b"\r\n").await?;
                offset += n;
            }
            stream.write_all(b"0\r\n\r\n").await?;
        }
        stream.flush().await
    };
    let written =
        tokio::select! { biased; _ = cancel.cancelled() => return Ok(()), r = write => r };
    if written.is_err() {
        trace.closed.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    }
    if reply.stalled {
        let mut b = [0; 1];
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {},
            read = stream.read(&mut b) => {
                if !matches!(read, Ok(n) if n != 0) { trace.closed.fetch_add(1, Ordering::SeqCst); }
            }
        }
    }
    Ok(())
}
pub fn assert_request(request: &Request, text: &str) {
    assert!(request.header.starts_with("POST /tts/bytes HTTP/1.1\r\n"));
    let header = request.header.to_ascii_lowercase();
    assert!(header.contains(&format!("x-api-key: {}\r\n", SECRET.to_ascii_lowercase())));
    assert!(header.contains("cartesia-version: 2026-03-01\r\n"));
    assert_eq!(request.body["transcript"], text);
    assert_eq!(request.body["model_id"], "sonic-2");
    assert_eq!(
        request.body["voice"],
        json!({"mode":"id","id":"Fixture-VOICE"})
    );
    assert_eq!(request.body["language"], "en");
    assert_eq!(
        request.body["output_format"],
        json!({"container":"raw","encoding":"pcm_s16le","sample_rate":16000})
    );
}
pub fn cache_wavs(root: &Path) -> Vec<PathBuf> {
    fn visit(path: &Path, found: &mut Vec<PathBuf>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let ty = entry.file_type().unwrap();
            if ty.is_dir() {
                visit(&entry.path(), found);
            } else if entry.path().extension().is_some_and(|e| e == "wav") {
                found.push(entry.path());
            }
        }
    }
    let mut found = Vec::new();
    visit(root, &mut found);
    found.sort();
    found
}
pub fn assert_private_cache(root: &Path, bytes: &[u8]) {
    use std::os::unix::fs::PermissionsExt;
    let files = cache_wavs(root);
    assert_eq!(files.len(), 1, "one complete successful cache WAV");
    assert_eq!(
        std::fs::metadata(root).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&files[0]).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let wav = std::fs::read(&files[0]).unwrap();
    let audio = pipecat_evals::RecordedAudio::from_wav(&wav).unwrap();
    assert_eq!(audio.pcm_bytes(), bytes);
    assert!(!String::from_utf8_lossy(&wav).contains(SECRET));
    assert!(!files[0].to_string_lossy().contains(REFERENCE.trim()));
}
pub fn assert_redacted(result: &ScriptResult) {
    let errors = format!("{:?} {:?}", result.failures, result.debug_log);
    assert!(!errors.contains(SECRET));
    assert!(!errors.contains(REFERENCE));
    assert!(!errors.contains("provider body sentinel"));
}

pub fn assert_no_cached_audio(root: &Path) {
    fn visit(path: &Path) {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                visit(&entry.path());
            } else {
                assert_eq!(
                    entry.file_name(),
                    ".lock",
                    "failed synthesis left cache audio or a temporary file"
                );
                assert_eq!(entry.metadata().unwrap().len(), 0);
            }
        }
    }
    visit(root);
}
