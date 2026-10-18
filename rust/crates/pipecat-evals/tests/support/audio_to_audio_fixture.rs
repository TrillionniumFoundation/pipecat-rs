//! Bounded loopback fixtures for the explicit joint route. The host checks bytes,
//! not speech meaning; synthetic HTTP transcripts do not establish STT accuracy.
use super::common::{Bundle, Error, REFERENCE, SECRET};
use super::input_fixture::{packets, transport};
use pipecat_core::*;
use pipecat_evals::{
    bot_host::*, matcher::ScriptResult, AudioToAudioScenarioFile, BotTranscriptionSettings,
    Cancellation, CartesiaUserSpeechSettings, EvalSession, SessionOptions, UserSpeechCachePolicy,
};
use pipecat_speech::SpeechConfig;
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
    task::JoinHandle,
};

pub const WAIT: Duration = Duration::from_secs(15);
pub const LLM: &str = "reported LLM decoy, never the spoken answer";
pub const TTS: &str = "reported TTS observer decoy, never acoustic truth";
pub const SPOKEN: &str = "actual STT fixture spoken answer";
pub const STT_KEY: &str = "synthetic-duplex-stt-key-125";
pub const STT_ENV: &str = "PIPECAT_DUPLEX_FIXTURE_STT_KEY_125";
pub const JUDGE_KEY: &str = "synthetic-duplex-judge-key-125";
pub const JUDGE_ENV: &str = "PIPECAT_DUPLEX_FIXTURE_JUDGE_KEY_125";
pub const TTS_ENV: &str = "PIPECAT_DUPLEX_FIXTURE_TTS_KEY_125";
pub const PCM: &[u8] =
    include_bytes!("../../../pipecat-vad/tests/fixtures/smart_turn_speech.pcm16");
pub const CLIP_BYTES: usize = 30_720;

pub fn input_pcm() -> Vec<u8> {
    // A non-silent native fixture excerpt, three packets plus an uneven tail.
    PCM[25_600..29_514].to_vec()
}
pub fn options() -> SessionOptions {
    SessionOptions {
        stop_bot: true,
        transport: transport(),
        handshake_timeout: Duration::from_secs(2),
        default_timeout: Duration::from_secs(7),
        bot_quiet_timeout: Duration::from_secs(7),
        max_duration: WAIT,
        ..Default::default()
    }
}
pub fn expectation(event: &str, text: &str) -> Value {
    json!({"event":event,"text_contains":text,"within_ms":7000})
}
pub fn source() -> Value {
    json!({"name":"complete native audio to audio", "user":{"modality":"audio", "speech":{"service":"cartesia","voice":"Fixture-VOICE"}}, "turns":[{"user":REFERENCE,"expect":[expectation("llm_response", LLM), expectation("response", SPOKEN)]}]})
}
pub fn tts_settings(
    base: &str,
    cache: Option<&Path>,
    timeout: Duration,
) -> CartesiaUserSpeechSettings {
    assert!(base.starts_with("http://127.0.0.1:"));
    let mut speech = SpeechConfig::cartesia(SECRET);
    speech.base_url = base.into();
    speech.timeout = timeout;
    CartesiaUserSpeechSettings::new(
        speech,
        cache.map_or(UserSpeechCachePolicy::Disabled, |directory| {
            UserSpeechCachePolicy::Private {
                directory: directory.into(),
                max_entries: 8,
                max_bytes: 16 * 1024 * 1024,
            }
        }),
    )
    .unwrap()
}
pub fn tts_runtime(base: &str, cache: Option<&Path>) -> Value {
    json!({"provider":"cartesia","base_url":base,"api_key_env":TTS_ENV,"request_timeout_ms":2000,"cache_dir":cache})
}
pub fn stt_runtime(base: &str) -> Value {
    let absolute = |path: PathBuf| {
        if path.is_absolute() {
            path
        } else {
            std::env::current_dir().unwrap().join(path)
        }
    };
    let runtime = absolute(
        std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0".into()),
    );
    let model = absolute(
        std::env::var_os("PIPECAT_SILERO_MODEL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
            }),
    );
    json!({"base_url":base,"model":"fixture-stt","api_key_env":STT_ENV,"onnx_runtime":runtime,"silero_model":model,"timeout_ms":4000})
}
pub fn stt_settings(b: &Bundle, base: &str) -> BotTranscriptionSettings {
    static KEY: std::sync::Once = std::sync::Once::new();
    KEY.call_once(|| std::env::set_var(STT_ENV, STT_KEY));
    BotTranscriptionSettings::load(b.json("stt-runtime.json", &stt_runtime(base))).unwrap()
}
pub fn judge_settings(base: &str) -> Value {
    static KEY: std::sync::Once = std::sync::Once::new();
    KEY.call_once(|| std::env::set_var(JUDGE_ENV, JUDGE_KEY));
    json!({"modality":"text","eval":{"service":"jev","base_url":base,"api_key_env":JUDGE_ENV,"timeout_ms":4000}})
}
pub async fn prepare(
    b: &Bundle,
    value: &Value,
    url: &str,
    tts: CartesiaUserSpeechSettings,
    stt: BotTranscriptionSettings,
    options: SessionOptions,
) -> Result<EvalSession, pipecat_evals::EvalError> {
    let mut sessions = AudioToAudioScenarioFile::load(b.json("scenario.json", value))?
        .into_sessions_with_cancellation(url, options, tts, stt, Cancellation::new())
        .await?;
    assert_eq!(sessions.len(), 1);
    Ok(sessions.remove(0))
}
pub async fn joined(mut task: JoinHandle<ScriptResult>, cancel: &Cancellation) -> ScriptResult {
    match tokio::time::timeout(WAIT, &mut task).await {
        Ok(result) => result.expect("session owner panicked"),
        Err(_) => {
            cancel.cancel();
            let actual = task.await;
            panic!("session deadline; actual cooperative join: {actual:?}");
        }
    }
}
pub async fn run(session: EvalSession) -> ScriptResult {
    let cancel = Cancellation::new();
    joined(tokio::spawn(session.run(cancel.clone())), &cancel).await
}
pub fn responses(result: &ScriptResult) -> Vec<&str> {
    result
        .events_seen
        .iter()
        .filter(|e| e["type"] == "response")
        .filter_map(|e| e["text"].as_str())
        .collect()
}
pub fn assert_private(result: &ScriptResult) {
    let rendered = serde_json::to_string(result).unwrap();
    for secret in [SECRET, STT_KEY, JUDGE_KEY] {
        assert!(!rendered.contains(secret));
    }
    assert!(result
        .events_seen
        .iter()
        .all(|e| e["type"] != "eval-bot-audio"));
}

#[derive(Clone, Copy, Default)]
pub enum Mode {
    #[default]
    Normal,
    Silent,
    Early,
    InterruptInput,
}
pub struct Trace {
    pub audio: Mutex<Vec<Vec<u8>>>,
    pub errors: Mutex<Vec<String>>,
    pub text: AtomicUsize,
    pub completed: AtomicUsize,
    pub emitted: AtomicUsize,
    pub cleaned: AtomicUsize,
    pub dropped: AtomicUsize,
    pub first: Notify,
    pub tail: Notify,
    pub output: Notify,
    pub gated: AtomicBool,
    pub release: Semaphore,
}
impl Default for Trace {
    fn default() -> Self {
        Self {
            audio: Mutex::new(vec![]),
            errors: Mutex::new(vec![]),
            text: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            emitted: AtomicUsize::new(0),
            cleaned: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            first: Notify::new(),
            tail: Notify::new(),
            output: Notify::new(),
            gated: AtomicBool::new(false),
            release: Semaphore::new(0),
        }
    }
}
struct Consumer {
    trace: Arc<Trace>,
    clips: Mutex<VecDeque<VecDeque<Vec<u8>>>>,
    in_clip: AtomicBool,
    mode: Mode,
}
impl Drop for Consumer {
    fn drop(&mut self) {
        self.trace.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
async fn push(ctx: &ProcessorContext, data: FrameData) -> Result<(), ProcessorError> {
    ctx.push(Frame::new(data), Direction::Downstream).await
}
async fn response(
    ctx: &ProcessorContext,
    trace: &Trace,
    silent: bool,
) -> Result<(), ProcessorError> {
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::new(LLM)),
        FrameData::OutputTransportMessageUrgent(
            json!({"label":"rtvi-ai","type":"bot-tts-text","data":{"text":TTS}}),
        ),
        FrameData::BotStoppedSpeaking,
    ] {
        push(ctx, data).await?;
    }
    if !silent {
        push(
            ctx,
            FrameData::OutputAudio(
                AudioRawFrame::new(PCM[..CLIP_BYTES].to_vec(), 16_000, 1).unwrap(),
            ),
        )
        .await?;
    }
    // A real root flush follows the same normal frame queue as bot PCM. A
    // remote stopped-speaking event is deliberately sent before that PCM.
    let delivered = LocalFlushEvent::default();
    push(
        ctx,
        FrameData::FlushDetailed(PipelineFlushFrame {
            event: Some(delivered.clone()),
            returning: true,
            origin: Some("duplex-fixture".into()),
        }),
    )
    .await?;
    tokio::time::timeout(WAIT, delivered.wait())
        .await
        .map_err(|_| ProcessorError::fatal("duplex output flush deadline"))?;
    // Complete the reported LLM response only after the PCM capture queue was flushed.
    push(ctx, FrameData::LlmResponseEnd).await?;
    trace.emitted.fetch_add(1, Ordering::SeqCst);
    trace.output.notify_one();
    Ok(())
}
#[async_trait]
impl FrameProcessor for Consumer {
    fn name(&self) -> &str {
        "duplex-complete-input-validator"
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
                let idle =
                    audio.audio.iter().all(|b| *b == 0) && !self.in_clip.load(Ordering::SeqCst);
                if !idle {
                    let first = !self.in_clip.swap(true, Ordering::SeqCst);
                    let (last, valid) = {
                        let mut clips = self.clips.lock().unwrap();
                        if let Some(clip) = clips.front_mut() {
                            let expected = clip.pop_front().unwrap();
                            let last = clip.is_empty();
                            if last {
                                clips.pop_front();
                            }
                            (
                                last,
                                audio.audio == expected
                                    && audio.sample_rate == 16_000
                                    && audio.num_channels == 1,
                            )
                        } else {
                            (false, false)
                        }
                    };
                    if !valid {
                        self.trace.errors.lock().unwrap().push(
                            "truncated, reordered, repeated or incorrectly padded user PCM".into(),
                        );
                    }
                    if first {
                        self.trace.first.notify_one();
                        if matches!(self.mode, Mode::Early) {
                            response(ctx, &self.trace, false).await?;
                        }
                        if matches!(self.mode, Mode::InterruptInput) {
                            push(
                                ctx,
                                FrameData::OutputTransportMessageUrgent(
                                    json!({"label":"rtvi-ai","type":"bot-interrupted","data":{}}),
                                ),
                            )
                            .await?;
                        }
                    }
                    if last {
                        self.trace.tail.notify_one();
                        if self.trace.gated.load(Ordering::SeqCst) {
                            self.trace
                                .release
                                .acquire()
                                .await
                                .map_err(|_| ProcessorError::fatal("fixture tail gate closed"))?
                                .forget();
                        }
                        self.in_clip.store(false, Ordering::SeqCst);
                        if self.trace.errors.lock().unwrap().is_empty() {
                            self.trace.completed.fetch_add(1, Ordering::SeqCst);
                            if !matches!(self.mode, Mode::Early) {
                                response(ctx, &self.trace, matches!(self.mode, Mode::Silent))
                                    .await?;
                            }
                        }
                    }
                }
            }
            FrameData::Text(_) | FrameData::LlmMessagesAppend(_) | FrameData::Transcription(_) => {
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
    mode: Mode,
}
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if settings.skip_tts {
            return Err(BotHostError::Invalid(
                "joint route lost capture_bot_audio selection".into(),
            ));
        }
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(Consumer {
                trace: self.trace.clone(),
                clips: Mutex::new(self.clips.iter().map(|c| packets(c).into()).collect()),
                in_clip: AtomicBool::new(false),
                mode: self.mode,
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
    pub async fn start(clips: Vec<Vec<u8>>, mode: Mode) -> Self {
        let host = EvalBotHost::bind(
            "127.0.0.1:0".parse().unwrap(),
            EvalBotHostConfig {
                startup_timeout: Duration::from_secs(2),
                session_timeout: WAIT,
                host_timeout: WAIT * 2,
                shutdown_timeout: Duration::from_secs(2),
                transport: transport(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .with_raw_audio_input(true)
        .with_audio_output(true);
        let url = format!("ws://{}/", host.local_addr().unwrap());
        let cancel = Cancellation::new();
        let c = cancel.clone();
        let trace = Arc::new(Trace::default());
        let factory = Factory {
            trace: trace.clone(),
            clips,
            mode,
        };
        let task = tokio::spawn(async move { host.serve(&factory, c).await });
        Self {
            url,
            trace,
            cancel,
            task,
        }
    }
    /// Observe the client's terminal control/close without fixture cancellation.
    /// This joins the endpoint; it does not promise complete input consumption.
    /// A timeout fails after fallback cancellation and an actual join.
    pub async fn finish_without_cancel(
        mut self,
    ) -> (Result<EvalBotHostReport, BotHostError>, Arc<Trace>) {
        let result = match tokio::time::timeout(WAIT, &mut self.task).await {
            Ok(result) => result,
            Err(_) => {
                self.trace.release.add_permits(128);
                self.cancel.cancel();
                let actual = self.task.await;
                panic!("natural host exit deadline; fallback cancelled and actually joined: {actual:?}");
            }
        };
        (result.expect("host owner panicked"), self.trace)
    }
    pub async fn finish(mut self) -> (Result<EvalBotHostReport, BotHostError>, Arc<Trace>) {
        self.trace.release.add_permits(128);
        self.cancel.cancel();
        let result = match tokio::time::timeout(WAIT, &mut self.task).await {
            Ok(r) => r,
            Err(_) => {
                let actual = self.task.await;
                panic!("host deadline; actual join: {actual:?}");
            }
        };
        (result.expect("host owner panicked"), self.trace)
    }
}
pub fn assert_consumed(trace: &Trace, clips: &[Vec<u8>]) {
    let errors = trace.errors.lock().unwrap().clone();
    assert!(errors.is_empty(), "{errors:?}");
    assert_eq!(
        trace.text.load(Ordering::SeqCst),
        0,
        "text or fabricated transcript entered host"
    );
    assert_eq!(trace.completed.load(Ordering::SeqCst), clips.len());
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    let actual: Vec<_> = trace
        .audio
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.iter().any(|b| *b != 0))
        .cloned()
        .collect();
    let expected: Vec<_> = clips
        .iter()
        .flat_map(|c| packets(c))
        .filter(|p| p.iter().any(|b| *b != 0))
        .collect();
    assert_eq!(
        actual, expected,
        "complete consumed input and legal final padding"
    );
}

#[derive(Clone)]
pub enum Reply {
    Stt(&'static str),
    Judge(&'static str),
    Hold,
    Status(u16),
    Invalid,
}
pub struct HttpTrace {
    pub requests: Mutex<Vec<Vec<u8>>>,
    pub received: Notify,
    pub released: Semaphore,
    pub closed: AtomicUsize,
    pub joined: AtomicUsize,
}
pub struct Http {
    pub url: String,
    pub trace: Arc<HttpTrace>,
    cancel: Cancellation,
    task: JoinHandle<Result<(), Error>>,
}
impl Http {
    pub async fn start(replies: Vec<Reply>, judge: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let trace = Arc::new(HttpTrace {
            requests: Mutex::new(vec![]),
            received: Notify::new(),
            released: Semaphore::new(0),
            closed: AtomicUsize::new(0),
            joined: AtomicUsize::new(0),
        });
        let t = trace.clone();
        let cancel = Cancellation::new();
        let c = cancel.clone();
        let task = tokio::spawn(async move {
            let work = async {
                let mut replies = replies.into_iter();
                loop {
                    let (mut stream, _) = listener.accept().await?;
                    let reply = replies.next().ok_or("unexpected extra HTTP request")?;
                    let bytes = read_request(&mut stream).await?;
                    if judge {
                        validate_judge(&bytes)?;
                    } else {
                        validate_stt(&bytes)?;
                    }
                    // Validate before the synthetic transcript is available.
                    t.requests.lock().unwrap().push(bytes);
                    t.received.notify_one();
                    let (status, body) = match reply {
                        Reply::Stt(text) => (200, json!({"text":text}).to_string()),
                        Reply::Judge(decision) => (200, json!({"answers":{"verdict":{"type":"choice","choice":decision,"confidence":0.9,"probabilities":{"yes":0.4,"no":0.3,"continue":0.3}}}}).to_string()),
                        Reply::Status(code) => (code, json!({"error":STT_KEY}).to_string()),
                        Reply::Invalid => (200, format!("invalid-{STT_KEY}")),
                        Reply::Hold => {
                            let mut b = [0; 1];
                            tokio::select! {
                                result = stream.read(&mut b) => {
                                    match result { Ok(0) => {}, Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {}, other => return Err(format!("unexpected held request read: {other:?}").into()) }
                                    t.closed.fetch_add(1, Ordering::SeqCst);
                                    continue;
                                }
                                permit = t.released.acquire() => { permit?.forget(); }
                            }
                            (200, json!({"text":"stale forbidden transcript"}).to_string())
                        }
                    };
                    let header = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                    stream.write_all(header.as_bytes()).await?;
                    stream.write_all(body.as_bytes()).await?;
                    stream.shutdown().await?;
                }
            };
            let result =
                tokio::select! { biased; _ = c.cancelled() => Ok(()), result = work => result };
            t.joined.fetch_add(1, Ordering::SeqCst);
            result
        });
        Self {
            url,
            trace,
            cancel,
            task,
        }
    }
    pub async fn finish(mut self) -> (Result<(), Error>, Arc<HttpTrace>) {
        self.cancel.cancel();
        let result = match tokio::time::timeout(WAIT, &mut self.task).await {
            Ok(r) => r,
            Err(_) => {
                let actual = self.task.await;
                panic!("HTTP deadline; actual join: {actual:?}");
            }
        };
        (result.expect("HTTP owner panicked"), self.trace)
    }
}
pub async fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    let mut block = [0; 4096];
    let end = loop {
        let n = stream.read(&mut block).await?;
        if n == 0 {
            return Err("HTTP EOF before headers".into());
        }
        bytes.extend_from_slice(&block[..n]);
        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            if i + 4 > 16 * 1024 {
                return Err("HTTP header budget".into());
            }
            break i + 4;
        }
        if bytes.len() > 16 * 1024 {
            return Err("HTTP header budget".into());
        }
    };
    let header = std::str::from_utf8(&bytes[..end])?;
    let length = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .ok_or("missing content length")?
        .parse::<usize>()?;
    if length > 512 * 1024 {
        return Err("HTTP body budget".into());
    }
    let total = end.checked_add(length).ok_or("HTTP length overflow")?;
    while bytes.len() < total {
        let n = stream.read(&mut block).await?;
        if n == 0 {
            return Err("HTTP truncated body".into());
        }
        bytes.extend_from_slice(&block[..n]);
    }
    if bytes.len() != total {
        return Err("HTTP extra body bytes".into());
    }
    Ok(bytes)
}
pub fn request_json(bytes: &[u8]) -> Value {
    let end = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    serde_json::from_slice(&bytes[end..]).unwrap()
}
pub fn validate_stt(request: &[u8]) -> Result<(), Error> {
    let text = String::from_utf8_lossy(request);
    if !request.starts_with(b"POST /audio/transcriptions ")
        || !text
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {STT_KEY}\r\n"))
        || !text.contains("multipart/form-data; boundary=")
        || !text.contains("name=\"model\"\r\n\r\nfixture-stt")
    {
        return Err("wrong STT request endpoint, key or multipart model".into());
    }
    let offset = request
        .windows(4)
        .position(|w| w == b"RIFF")
        .ok_or("missing WAV")?;
    let header = request.get(offset..offset + 8).ok_or("short WAV")?;
    let length = u32::from_le_bytes(header[4..8].try_into()?) as usize + 8;
    let wav = pipecat_audio::read_pcm16_wav(
        request
            .get(offset..offset + length)
            .ok_or("short WAV payload")?,
    )?;
    let voiced = &PCM[25_600..27_648];
    if wav.format.sample_rate() != 16_000
        || wav.format.channels() != 1
        || !wav.pcm.windows(voiced.len()).any(|w| w == voiced)
    {
        return Err("multipart WAV lacks actual captured speech at mono16k".into());
    }
    Ok(())
}
fn validate_judge(request: &[u8]) -> Result<(), Error> {
    if !request.starts_with(b"POST /v1/systemone ")
        || !String::from_utf8_lossy(request)
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {JUDGE_KEY}\r\n"))
    {
        return Err("wrong judge endpoint or key".into());
    }
    let body = request_json(request);
    if !body["state"].is_object() || body["questions"]["verdict"]["type"] != "choice" {
        return Err("wrong judge schema".into());
    }
    Ok(())
}
