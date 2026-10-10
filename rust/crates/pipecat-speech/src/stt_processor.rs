//! Bounded mono PCM segmentation for the two whole-upload HTTP STT clients.
//!
//! VAD closes requests; End drains only already closed requests. Mute revokes
//! local request/result authority, not bytes already uploaded or frames already
//! admitted downstream. External health restoration is rechecked, but core does
//! not offer an atomic health-generation-and-transcript publication operation.
use crate::{
    elevenlabs::{ElevenLabsHttpClient, ElevenLabsTranscriptionRequest},
    OpenAiTranscriptionOptions, SpeechClient, SpeechConfig, SpeechError, TranscriptionRequest,
};
use pipecat_core::{
    async_trait, AudioRawFrame, DetailedTranscriptionFrame, Direction, ErrorCategory, ErrorFrame,
    Frame, FrameData, FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorHealthGeneration, ProcessorReference, ServiceMetadataFrame,
    ServiceUpdateSettingsFrame, Setting, StartFrame, SttMetadataFrame, SttSettings, TextFrame,
    TranscriptionFrame,
};
use pipecat_services::CancellationToken;
use serde_json::Value;
use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};
use tokio::sync::Notify;

const MAX_SETTINGS_BYTES: usize = 16 * 1024;
const MAX_KEYTERMS: usize = 100;
const MAX_KEYTERM_BYTES: usize = 256;
const HEALTH_POLL: Duration = Duration::from_millis(25);

/// A typed backend. The client and accepted request template are never mutated
/// by an in-flight segment; runtime updates apply at the next VAD close.
#[derive(Clone)]
pub enum HttpSttBackend {
    OpenAi {
        client: SpeechClient,
        request: TranscriptionRequest,
    },
    ElevenLabs {
        client: ElevenLabsHttpClient,
        request: ElevenLabsTranscriptionRequest,
    },
}
impl fmt::Debug for HttpSttBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}
impl HttpSttBackend {
    fn name(&self) -> &'static str {
        match self {
            Self::OpenAi { .. } => "OpenAIHttpSTTService",
            Self::ElevenLabs { .. } => "ElevenLabsHttpSTTService",
        }
    }
    fn config(&self) -> &SpeechConfig {
        match self {
            Self::OpenAi { client, .. } => client.config(),
            Self::ElevenLabs { client, .. } => client.config(),
        }
    }
    fn validate(&self, options: &OpenAiTranscriptionOptions) -> Result<(), SpeechError> {
        let config = self.config();
        if config.timeout.is_zero()
            || config.timeout > Duration::from_secs(300)
            || config.max_audio_bytes <= 44
            || config.max_audio_bytes > 64 * 1024 * 1024
            || config.max_json_bytes == 0
            || config.max_json_bytes > 8 * 1024 * 1024
            || config.base_url.len() > 8192
            || config.api_key.len() > 8192
        {
            return Err(invalid("bounded HTTP policy"));
        }
        let size = match self {
            Self::OpenAi { request, .. } => {
                validate_model(&request.model)?;
                validate_language(&request.language)?;
                if request.prompt.as_ref().is_some_and(|p| p.len() > 8192) {
                    return Err(invalid("prompt limit"));
                }
                options.validate(request)?;
                validate_terms(&options.keywords)?;
                request.model.len()
                    + request.language.as_ref().map_or(0, String::len)
                    + request.prompt.as_ref().map_or(0, String::len)
                    + options.keywords.iter().map(String::len).sum::<usize>()
            }
            Self::ElevenLabs { request, .. } => {
                if options != &OpenAiTranscriptionOptions::default() {
                    return Err(invalid("OpenAI options on ElevenLabs backend"));
                }
                validate_model(&request.model)?;
                validate_language(&request.language_code)?;
                validate_terms(&request.keyterms)?;
                if request.no_verbatim.is_some() && request.model != "scribe_v2" {
                    return Err(invalid("no_verbatim requires scribe_v2"));
                }
                request.model.len()
                    + request.language_code.as_ref().map_or(0, String::len)
                    + request.keyterms.iter().map(String::len).sum::<usize>()
            }
        };
        if size > MAX_SETTINGS_BYTES {
            return Err(invalid("settings limit"));
        }
        Ok(())
    }
    fn same_settings(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::OpenAi { request: a, .. }, Self::OpenAi { request: b, .. }) => {
                a.model == b.model && a.language == b.language && a.prompt == b.prompt
            }
            (Self::ElevenLabs { request: a, .. }, Self::ElevenLabs { request: b, .. }) => {
                a.model == b.model
                    && a.language_code == b.language_code
                    && a.tag_audio_events == b.tag_audio_events
                    && a.keyterms == b.keyterms
                    && a.no_verbatim == b.no_verbatim
            }
            _ => false,
        }
    }
    async fn transcribe(&self, segment: &Segment, rate: u32) -> Result<Projection, SpeechError> {
        match self {
            Self::OpenAi { client, request } => {
                let output = client
                    .transcribe_pcm_with_options(
                        &segment.audio,
                        rate,
                        1,
                        request.clone(),
                        segment.options.clone(),
                        segment.token.clone(),
                    )
                    .await?;
                Ok(Projection {
                    text: output.transcript.text,
                    language: None,
                    result: output.result,
                })
            }
            Self::ElevenLabs { client, request } => {
                let output = client
                    .transcribe_pcm_with_result(
                        &segment.audio,
                        rate,
                        1,
                        request.clone(),
                        segment.token.clone(),
                    )
                    .await?;
                // Preserve the source distinction: missing defaults to English,
                // explicitly null remains absent. Raw JSON retains both shapes.
                let language = if output.result.get("language_code").is_none() {
                    Some("eng".into())
                } else {
                    output.transcript.language_code
                };
                Ok(Projection {
                    text: output.transcript.text,
                    language,
                    result: output.result,
                })
            }
        }
    }
}
fn invalid(reason: &'static str) -> SpeechError {
    SpeechError::Invalid(reason)
}
fn validate_model(model: &str) -> Result<(), SpeechError> {
    if model.is_empty()
        || model.len() > 256
        || !model
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/'))
    {
        Err(invalid("model"))
    } else {
        Ok(())
    }
}
fn validate_language(language: &Option<String>) -> Result<(), SpeechError> {
    if language.as_ref().is_some_and(|v| {
        v.is_empty()
            || v.len() > 64
            || !v
                .bytes()
                .all(|b| b.is_ascii_alphabetic() || matches!(b, b'-' | b'_'))
    }) {
        Err(invalid("language"))
    } else {
        Ok(())
    }
}
fn validate_terms(terms: &[String]) -> Result<(), SpeechError> {
    if terms.len() > MAX_KEYTERMS
        || terms
            .iter()
            .any(|s| s.is_empty() || s.len() > MAX_KEYTERM_BYTES)
        || terms.iter().map(String::len).sum::<usize>() > 8192
    {
        Err(invalid("keyterms limit"))
    } else {
        Ok(())
    }
}

/// Finite local limits, independent of provider-supported upload duration.
/// Pending bytes include the active request; the transient WAV/body allocation
/// adds at most one upload-sized allocation. Raw audio passthrough is separate
/// from STT mute. TTFB is not emitted; P99 metadata is a configured benchmark.
#[derive(Clone, Debug)]
pub struct HttpSttOptions {
    pub sample_rate: Option<u32>,
    pub trailing_silence: Duration,
    /// Maximum waiting segments, in addition to one active HTTP request.
    pub queue_capacity: usize,
    pub max_pending_audio_bytes: usize,
    /// Includes preroll and trailing padding.
    pub max_segment_duration: Duration,
    pub max_frame_bytes: usize,
    /// Absolute queue-plus-request deadline, measured from VAD close.
    pub max_segment_age: Duration,
    pub max_transcript_bytes: usize,
    pub max_user_id_bytes: usize,
    pub audio_passthrough: bool,
    pub push_empty_transcripts: bool,
    pub detailed_transcripts: bool,
    pub ttfs_p99_latency: f64,
    pub openai_options: OpenAiTranscriptionOptions,
}
impl Default for HttpSttOptions {
    fn default() -> Self {
        Self {
            sample_rate: None,
            trailing_silence: Duration::from_millis(500),
            queue_capacity: 4,
            max_pending_audio_bytes: 16 * 1024 * 1024,
            max_segment_duration: Duration::from_secs(120),
            max_frame_bytes: 1024 * 1024,
            max_segment_age: Duration::from_secs(300),
            max_transcript_bytes: 64 * 1024,
            max_user_id_bytes: 256,
            audio_passthrough: true,
            push_empty_transcripts: false,
            detailed_transcripts: true,
            ttfs_p99_latency: 2.01,
            openai_options: OpenAiTranscriptionOptions::default(),
        }
    }
}
impl HttpSttOptions {
    fn validate(&self) -> Result<(), SpeechError> {
        if self
            .sample_rate
            .is_some_and(|r| !(8000..=192000).contains(&r))
            || self.trailing_silence > Duration::from_secs(5)
            || self.queue_capacity == 0
            || self.queue_capacity > 16
            || self.max_pending_audio_bytes == 0
            || self.max_pending_audio_bytes > 64 * 1024 * 1024
            || self.max_segment_duration
                < Duration::from_secs(1).saturating_add(self.trailing_silence)
            || self.max_segment_duration > Duration::from_secs(120)
            || self.max_frame_bytes < 2
            || self.max_frame_bytes > 4 * 1024 * 1024
            || self.max_segment_age.is_zero()
            || self.max_segment_age > Duration::from_secs(900)
            || self.max_transcript_bytes == 0
            || self.max_transcript_bytes > 1024 * 1024
            || self.max_user_id_bytes == 0
            || self.max_user_id_bytes > 4096
            || !self.ttfs_p99_latency.is_finite()
            || !(0.0..=60.0).contains(&self.ttfs_p99_latency)
        {
            return Err(invalid("segmentation limits"));
        }
        Ok(())
    }
}
#[derive(Clone, Copy)]
struct Geometry {
    rate: u32,
    preroll: usize,
    padding: usize,
    maximum: usize,
}
fn pcm_bytes(rate: u32, duration: Duration) -> Result<usize, SpeechError> {
    let samples = u128::from(rate)
        .checked_mul(duration.as_nanos())
        .ok_or(invalid("sample geometry"))?
        / 1_000_000_000;
    usize::try_from(samples.checked_mul(2).ok_or(invalid("sample geometry"))?)
        .map_err(|_| invalid("sample geometry"))
}
impl Geometry {
    fn new(
        rate: u32,
        options: &HttpSttOptions,
        config: &SpeechConfig,
    ) -> Result<Self, SpeechError> {
        if !(8000..=192000).contains(&rate) || options.sample_rate.is_some_and(|r| r != rate) {
            return Err(invalid("pipeline sample rate"));
        }
        let maximum =
            pcm_bytes(rate, options.max_segment_duration)?.min(config.max_audio_bytes - 44) & !1;
        let preroll = pcm_bytes(rate, Duration::from_secs(1))?;
        let padding = pcm_bytes(rate, options.trailing_silence)?;
        if padding >= maximum {
            return Err(invalid("upload budget below padding"));
        }
        Ok(Self {
            rate,
            preroll,
            padding,
            maximum,
        })
    }
}
struct Projection {
    text: String,
    language: Option<String>,
    result: Value,
}
struct Segment {
    audio: Vec<u8>,
    user_id: String,
    backend: HttpSttBackend,
    options: OpenAiTranscriptionOptions,
    privacy: u64,
    health: ProcessorHealthGeneration,
    token: CancellationToken,
    accepted: Instant,
}
struct State {
    backend: HttpSttBackend,
    options: OpenAiTranscriptionOptions,
    geometry: Option<Geometry>,
    audio: Vec<u8>,
    user_id: String,
    speaking: bool,
    discarded: bool,
    muted: bool,
    started: bool,
    closing: bool,
    cancelled: bool,
    privacy: u64,
    token: CancellationToken,
    health: ProcessorHealthGeneration,
    queue: VecDeque<Segment>,
    pending_bytes: usize,
    active_bytes: usize,
    #[cfg(test)]
    publication_gate: Option<Arc<TestPublicationGate>>,
}
impl State {
    fn clear_audio(&mut self) {
        self.audio.clear();
        self.user_id.clear();
        self.speaking = false;
        self.discarded = false;
    }
    fn revoke(&mut self) {
        self.token.cancel();
        self.token = CancellationToken::new();
        self.clear_audio();
        self.queue.clear();
        self.pending_bytes = self.active_bytes;
    }
    fn sync_health(&mut self, health: &ProcessorHealth) -> bool {
        let current = health.generation();
        if self.health != current {
            self.revoke();
            self.health = current;
        }
        current.snapshot.is_usable
    }
    fn eligible(&self, segment: &Segment, health: &ProcessorHealth) -> bool {
        !self.cancelled
            && !self.muted
            && self.privacy == segment.privacy
            && health.generation() == segment.health
            && segment.health.snapshot.is_usable
    }
}
/// A single-use, owned FIFO HTTP transcription processor. Audio remains on the
/// fast path; bot interruption does not revoke valid user speech.
/// Setup rate mismatch and reuse are fatal: this instance has no usable worker
/// after either failure. Later segment/settings validation errors are nonfatal.
///
/// Observer clocks and health-watch wakers must be nonblocking and must not
/// synchronously reenter this processor or runtime admission. Publication and
/// recovery hold a short private state lock; core can invoke those hooks there.
/// Normal Tokio wakers schedule subsequent work and satisfy this requirement.
pub struct HttpSttProcessor {
    state: Arc<Mutex<State>>,
    options: HttpSttOptions,
    health: ProcessorHealth,
    changed: Arc<Notify>,
    cancellation: CancellationToken,
    name: &'static str,
}
impl fmt::Debug for HttpSttProcessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpSttProcessor")
            .field("backend", &self.name)
            .finish_non_exhaustive()
    }
}
impl HttpSttProcessor {
    pub fn new(mut backend: HttpSttBackend, options: HttpSttOptions) -> Result<Self, SpeechError> {
        options.validate()?;
        backend.validate(&options.openai_options)?;
        normalize_language(&mut backend);
        backend.validate(&options.openai_options)?;
        if options.push_empty_transcripts && !matches!(backend, HttpSttBackend::OpenAi { .. }) {
            return Err(invalid("empty transcripts only supported by OpenAI"));
        }
        if let Some(rate) = options.sample_rate {
            Geometry::new(rate, &options, backend.config())?;
        }
        let name = backend.name();
        let health = ProcessorHealth::default();
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                backend,
                options: options.openai_options.clone(),
                geometry: None,
                audio: vec![],
                user_id: String::new(),
                speaking: false,
                discarded: false,
                muted: false,
                started: false,
                closing: false,
                cancelled: false,
                privacy: 0,
                token: CancellationToken::new(),
                health: health.generation(),
                queue: VecDeque::new(),
                pending_bytes: 0,
                active_bytes: 0,
                #[cfg(test)]
                publication_gate: None,
            })),
            options,
            health,
            changed: Arc::new(Notify::new()),
            cancellation: CancellationToken::new(),
            name,
        })
    }
    fn lock(&self) -> Result<MutexGuard<'_, State>, ProcessorError> {
        lock(&self.state)
    }
    fn targets(&self, target: &Option<ProcessorReference>) -> bool {
        match target {
            None => true,
            Some(ProcessorReference::Name(name)) => name == self.name,
            Some(ProcessorReference::Local(target)) => {
                std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
            }
        }
    }
    fn buffer(&self, audio: &AudioRawFrame, user_id: &str) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if !state.sync_health(&self.health) || state.closing || state.muted || state.discarded {
            return Ok(());
        }
        let g = state
            .geometry
            .ok_or_else(|| processor_invalid("STT has not started"))?;
        if audio.sample_rate != g.rate
            || audio.num_channels != 1
            || !audio.audio.len().is_multiple_of(2)
            || audio.audio.len() > self.options.max_frame_bytes
            || user_id.len() > self.options.max_user_id_bytes
        {
            state.clear_audio();
            state.discarded = true;
            return Err(processor_invalid(
                "invalid mono PCM geometry or frame/user limit",
            ));
        }
        if audio.audio.is_empty() {
            return Ok(());
        }
        if state.user_id != user_id {
            if state.speaking && !state.audio.is_empty() {
                state.clear_audio();
                state.discarded = true;
                return Err(processor_invalid("speaker changed within a segment"));
            }
            state.audio.clear();
            state.user_id = user_id.into();
        }
        if state.speaking {
            if state
                .audio
                .len()
                .checked_add(audio.audio.len())
                .and_then(|v| v.checked_add(g.padding))
                .is_none_or(|v| v > g.maximum)
            {
                state.clear_audio();
                state.discarded = true;
                return Err(processor_invalid("speech segment limit"));
            }
            state.audio.extend_from_slice(&audio.audio);
        } else if audio.audio.len() >= g.preroll {
            state.audio.clear();
            state
                .audio
                .extend_from_slice(&audio.audio[audio.audio.len() - g.preroll..]);
        } else {
            let discard = state
                .audio
                .len()
                .saturating_add(audio.audio.len())
                .saturating_sub(g.preroll);
            state.audio.drain(..discard);
            state.audio.extend_from_slice(&audio.audio);
        }
        Ok(())
    }
    fn start_speech(&self) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if state.sync_health(&self.health) && !state.closing && !state.muted {
            // Duplicate starts preserve the active utterance. A fresh start is
            // the only way to recover an invalid or privacy-revoked segment.
            state.speaking = true;
            state.discarded = false;
        }
        Ok(())
    }
    fn close_speech(&self) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if !state.sync_health(&self.health)
            || state.closing
            || state.muted
            || state.discarded
            || !state.speaking
        {
            return Ok(());
        }
        state.speaking = false;
        let mut audio = std::mem::take(&mut state.audio);
        if audio.is_empty() {
            state.user_id.clear();
            return Ok(());
        }
        let g = state
            .geometry
            .ok_or_else(|| processor_invalid("STT has not started"))?;
        let size = audio
            .len()
            .checked_add(g.padding)
            .ok_or_else(|| processor_invalid("segment limit"))?;
        if matches!(&state.backend, HttpSttBackend::OpenAi { request, .. } if request.model == "gpt-4o-transcribe-diarize")
            && size > g.rate as usize * 60
        {
            state.user_id.clear();
            return Err(processor_invalid(
                "diarize audio above 30 seconds requires unsupported chunking settings",
            ));
        }
        if size > g.maximum
            || state.queue.len() >= self.options.queue_capacity
            || state
                .pending_bytes
                .checked_add(size)
                .is_none_or(|v| v > self.options.max_pending_audio_bytes)
        {
            state.user_id.clear();
            return Err(processor_invalid("segment queue capacity"));
        }
        audio.resize(size, 0);
        let segment = Segment {
            audio,
            user_id: std::mem::take(&mut state.user_id),
            backend: state.backend.clone(),
            options: state.options.clone(),
            privacy: state.privacy,
            health: state.health,
            token: state.token.clone(),
            accepted: Instant::now(),
        };
        state.pending_bytes += size;
        state.queue.push_back(segment);
        self.changed.notify_one();
        Ok(())
    }
    fn mute(&self, muted: bool) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if state.closing && !muted {
            return Ok(());
        }
        if muted || state.muted != muted {
            state.privacy = state
                .privacy
                .checked_add(1)
                .ok_or_else(|| ProcessorError::fatal("STT privacy generation exhausted"))?;
            state.revoke();
            state.muted = muted;
        }
        self.changed.notify_one();
        Ok(())
    }
    fn finish(&self, cancel: bool) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        state.closing = true;
        state.clear_audio();
        if cancel {
            state.cancelled = true;
            state.revoke();
            self.cancellation.cancel();
        }
        self.changed.notify_one();
        Ok(())
    }
    fn update(
        &self,
        update: &ServiceUpdateSettingsFrame<SttSettings>,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if state.closing {
            return Ok(());
        }
        let source = update
            .delta
            .as_ref()
            .map_or(&update.settings, |delta| &delta.base.extra);
        let mut changes = std::collections::BTreeMap::new();
        let mut total = 0usize;
        for (key, value) in source {
            let shadowed = update.delta.as_ref().is_some_and(|delta| {
                (matches!(key.as_str(), "model" | "model_id")
                    && !matches!(delta.base.model, Setting::NotGiven))
                    || (matches!(key.as_str(), "language" | "language_code")
                        && !matches!(delta.language, Setting::NotGiven))
            });
            if shadowed {
                continue;
            }
            let size = value_size(value, MAX_SETTINGS_BYTES)
                .ok_or_else(|| processor_invalid("settings update limit"))?;
            total = total
                .checked_add(size)
                .ok_or_else(|| processor_invalid("settings update limit"))?;
            if changes.len() >= 16 || key.len() > 64 || total > MAX_SETTINGS_BYTES {
                return Err(processor_invalid("settings update limit"));
            }
            changes.insert(key.clone(), value.clone());
        }
        if update.delta.as_ref().is_some_and(|delta| {
            [&delta.base.model, &delta.language]
                .iter()
                .any(|setting| matches!(setting, Setting::Value(v) if v.len() > MAX_SETTINGS_BYTES))
        }) {
            return Err(processor_invalid("settings update limit"));
        }
        if let Some(delta) = &update.delta {
            for (key, setting) in [("model", &delta.base.model), ("language", &delta.language)] {
                match setting {
                    Setting::NotGiven => {}
                    Setting::Null => {
                        changes.insert(key.into(), Value::Null);
                    }
                    Setting::Value(v) => {
                        changes.insert(key.into(), v.clone().into());
                    }
                }
            }
            if !matches!(delta.base.model, Setting::NotGiven) {
                changes.remove("model_id");
            }
            if !matches!(delta.language, Setting::NotGiven) {
                changes.remove("language_code");
            }
        }
        // Bound the incoming delta before cloning provider strings or lists.
        if changes.len() > 16
            || changes.iter().any(|(key, value)| {
                key.len() > 64 || value_size(value, MAX_SETTINGS_BYTES).is_none()
            })
            || changes
                .values()
                .try_fold(0usize, |n, v| {
                    n.checked_add(value_size(v, MAX_SETTINGS_BYTES)?)
                })
                .is_none_or(|n| n > MAX_SETTINGS_BYTES)
        {
            return Err(processor_invalid("settings update limit"));
        }
        let mut backend = state.backend.clone();
        let mut options = state.options.clone();
        let mut changed = false;
        for (key, value) in changes {
            match &mut backend {
                HttpSttBackend::OpenAi { request, .. } => match key.as_str() {
                    "model" => changed |= required(&mut request.model, value)?,
                    "language" => changed |= optional_string(&mut request.language, value)?,
                    "prompt" => changed |= optional_string(&mut request.prompt, value)?,
                    "temperature" => {
                        let v = if value.is_null() {
                            None
                        } else {
                            Some(
                                value
                                    .as_f64()
                                    .ok_or_else(|| processor_invalid("temperature type"))?,
                            )
                        };
                        changed |= options.temperature != v;
                        options.temperature = v;
                    }
                    "keywords" => changed |= terms(&mut options.keywords, value)?,
                    "include_prob_metrics" => {
                        let v = value
                            .as_bool()
                            .ok_or_else(|| processor_invalid("probability option type"))?;
                        changed |= options.include_prob_metrics != v;
                        options.include_prob_metrics = v;
                    }
                    _ => return Err(processor_invalid("unsupported OpenAI HTTP STT setting")),
                },
                HttpSttBackend::ElevenLabs { request, .. } => match key.as_str() {
                    "model" | "model_id" => changed |= required(&mut request.model, value)?,
                    "language" | "language_code" => {
                        changed |= optional_string(&mut request.language_code, value)?
                    }
                    "keyterms" => changed |= terms(&mut request.keyterms, value)?,
                    "tag_audio_events" => {
                        changed |= optional_bool(&mut request.tag_audio_events, value)?
                    }
                    "no_verbatim" => changed |= optional_bool(&mut request.no_verbatim, value)?,
                    _ => return Err(processor_invalid("unsupported ElevenLabs HTTP STT setting")),
                },
            }
        }
        normalize_language(&mut backend);
        backend
            .validate(&options)
            .map_err(|_| processor_invalid("invalid HTTP STT settings"))?;
        let changed =
            changed && (!backend.same_settings(&state.backend) || options != state.options);
        if changed {
            state.backend = backend;
            state.options = options;
            // Core notification hooks must not synchronously reenter this state.
            // Ordinary changes retain accepted immutable snapshots. Only an
            // effective recovery change grants a fresh health generation.
            if !self.health.is_usable() {
                self.health.set_usable(true);
                state.sync_health(&self.health);
            }
        }
        Ok(())
    }
}
impl Drop for HttpSttProcessor {
    fn drop(&mut self) {
        self.cancellation.cancel();
        if let Ok(mut state) = self.state.lock() {
            state.cancelled = true;
            state.revoke();
        }
        self.changed.notify_one();
    }
}
fn lock(state: &Mutex<State>) -> Result<MutexGuard<'_, State>, ProcessorError> {
    state
        .lock()
        .map_err(|_| ProcessorError::fatal("HTTP STT state unavailable"))
}
fn processor_invalid(reason: &'static str) -> ProcessorError {
    ProcessorError::new(reason)
        .with_category(ErrorCategory::InvalidRequest)
        .with_permanence_override(false)
}
fn required(target: &mut String, value: Value) -> Result<bool, ProcessorError> {
    let v = value
        .as_str()
        .ok_or_else(|| processor_invalid("required text setting type"))?;
    let changed = target != v;
    *target = v.into();
    Ok(changed)
}
fn optional_string(target: &mut Option<String>, value: Value) -> Result<bool, ProcessorError> {
    let v = if value.is_null() {
        None
    } else {
        Some(
            value
                .as_str()
                .ok_or_else(|| processor_invalid("optional text setting type"))?
                .into(),
        )
    };
    let changed = *target != v;
    *target = v;
    Ok(changed)
}
fn optional_bool(target: &mut Option<bool>, value: Value) -> Result<bool, ProcessorError> {
    let v = if value.is_null() {
        None
    } else {
        Some(
            value
                .as_bool()
                .ok_or_else(|| processor_invalid("optional boolean setting type"))?,
        )
    };
    let changed = *target != v;
    *target = v;
    Ok(changed)
}
fn terms(target: &mut Vec<String>, value: Value) -> Result<bool, ProcessorError> {
    let v = if value.is_null() {
        vec![]
    } else {
        value
            .as_array()
            .ok_or_else(|| processor_invalid("keyterms type"))?
            .iter()
            .map(|v| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| processor_invalid("keyterm type"))
            })
            .collect::<Result<Vec<_>, _>>()?
    };
    let changed = *target != v;
    *target = v;
    Ok(changed)
}
fn value_size(value: &Value, limit: usize) -> Option<usize> {
    // Settings have only scalar/string-list forms; nested objects are rejected
    // without recursion, allocation or echoing their contents.
    let n = match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 8,
        Value::String(s) => s.len(),
        Value::Array(values) if values.len() <= MAX_KEYTERMS => {
            values.iter().try_fold(0usize, |n, v| match v {
                Value::String(s) => n.checked_add(s.len()),
                _ => None,
            })?
        }
        _ => return None,
    };
    (n <= limit).then_some(n)
}
fn normalize_language(backend: &mut HttpSttBackend) {
    match backend {
        HttpSttBackend::OpenAi { request, .. } => {
            if let Some(language) = &mut request.language {
                *language = language
                    .split(['-', '_'])
                    .next()
                    .unwrap_or("")
                    .to_ascii_lowercase();
            }
        }
        HttpSttBackend::ElevenLabs { request, .. } => {
            if let Some(language) = &mut request.language_code {
                *language = eleven_language(language);
            }
        }
    }
}
fn eleven_language(language: &str) -> String {
    let base = language
        .split(['-', '_'])
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    let code = match base.as_str() {
        "af" => "afr",
        "am" => "amh",
        "ar" => "ara",
        "hy" => "hye",
        "as" => "asm",
        "ast" => "ast",
        "az" => "aze",
        "be" => "bel",
        "bn" => "ben",
        "bs" => "bos",
        "bg" => "bul",
        "my" => "mya",
        "yue" => "yue",
        "ca" => "cat",
        "ceb" => "ceb",
        "ny" => "nya",
        "hr" => "hrv",
        "cs" => "ces",
        "da" => "dan",
        "nl" => "nld",
        "en" => "eng",
        "et" => "est",
        "fil" => "fil",
        "fi" => "fin",
        "fr" => "fra",
        "ff" => "ful",
        "gl" => "glg",
        "lg" => "lug",
        "ka" => "kat",
        "de" => "deu",
        "el" => "ell",
        "gu" => "guj",
        "ha" => "hau",
        "he" => "heb",
        "hi" => "hin",
        "hu" => "hun",
        "is" => "isl",
        "ig" => "ibo",
        "id" => "ind",
        "ga" => "gle",
        "it" => "ita",
        "ja" => "jpn",
        "jv" => "jav",
        "kea" => "kea",
        "kn" => "kan",
        "kk" => "kaz",
        "km" => "khm",
        "ko" => "kor",
        "ku" => "kur",
        "ky" => "kir",
        "lo" => "lao",
        "lv" => "lav",
        "ln" => "lin",
        "lt" => "lit",
        "luo" => "luo",
        "lb" => "ltz",
        "mk" => "mkd",
        "ms" => "msa",
        "ml" => "mal",
        "mt" => "mlt",
        "zh" => "zho",
        "mi" => "mri",
        "mr" => "mar",
        "mn" => "mon",
        "ne" => "nep",
        "nso" => "nso",
        "no" => "nor",
        "oc" => "oci",
        "or" => "ori",
        "ps" => "pus",
        "fa" => "fas",
        "pl" => "pol",
        "pt" => "por",
        "pa" => "pan",
        "ro" => "ron",
        "ru" => "rus",
        "sr" => "srp",
        "sn" => "sna",
        "sd" => "snd",
        "sk" => "slk",
        "sl" => "slv",
        "so" => "som",
        "es" => "spa",
        "sw" => "swa",
        "sv" => "swe",
        "ta" => "tam",
        "tg" => "tgk",
        "te" => "tel",
        "th" => "tha",
        "tr" => "tur",
        "uk" => "ukr",
        "umb" => "umb",
        "ur" => "urd",
        "uz" => "uzb",
        "vi" => "vie",
        "cy" => "cym",
        "wo" => "wol",
        "xh" => "xho",
        "zu" => "zul",
        _ if base.len() == 3 && base.bytes().all(|c| c.is_ascii_lowercase()) => return base,
        _ => return language.to_owned(),
    };
    code.into()
}

struct Pending {
    state: Arc<Mutex<State>>,
    segment: Option<Segment>,
}
impl Drop for Pending {
    fn drop(&mut self) {
        let bytes = self
            .segment
            .as_ref()
            .map_or(0, |segment| segment.audio.len());
        // Release actual PCM/settings ownership before admitting its replacement.
        drop(self.segment.take());
        if let Ok(mut state) = self.state.lock() {
            state.pending_bytes = state.pending_bytes.saturating_sub(bytes);
            state.active_bytes = 0;
        }
    }
}
#[cfg(test)]
struct TestPublicationGate {
    reached: Notify,
    release: Notify,
}

#[async_trait]
impl FrameProcessor for HttpSttProcessor {
    fn name(&self) -> &str {
        self.name
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let g = {
            let mut state = self.lock()?;
            if state.started || state.closing {
                return Err(ProcessorError::fatal("HTTP STT processor is single-use"));
            }
            let g = Geometry::new(
                start.audio_in_sample_rate,
                &self.options,
                state.backend.config(),
            )
            .map_err(|_| ProcessorError::fatal("invalid pipeline PCM configuration"))?;
            state.started = true;
            state.geometry = Some(g);
            g
        };
        let state = self.state.clone();
        let health = self.health.clone();
        let changed = self.changed.clone();
        let cancel = self.cancellation.clone();
        let output = ctx.clone();
        let options = self.options.clone();
        let name = self.name;
        let usage = start.enable_usage_metrics;
        let metrics = start.enable_metrics;
        ctx.spawn("http-stt-segments", false, async move {
            let mut health_updates = health.watch();
            loop {
                let notified = changed.notified();
                let segment = {
                    let mut s = lock(&state)?; s.sync_health(&health);
                    if s.cancelled { break; }
                    let next = s.queue.pop_front();
                    if let Some(segment) = &next { s.active_bytes = segment.audio.len(); }
                    if next.is_none() && s.closing { break; }
                    next
                };
                let Some(segment) = segment else {
                    tokio::select! { biased; _ = cancel.cancelled() => break, _ = notified => {}, _ = health_updates.changed() => {}, _ = tokio::time::sleep(HEALTH_POLL) => {} }
                    continue;
                };
                let pending = Pending { state: state.clone(), segment: Some(segment) };
                let segment = pending.segment.as_ref().expect("owned segment");
                let started = Instant::now();
                {
                    let s = lock(&state)?;
                    if !s.eligible(segment, &health) { continue; }
                    if segment.accepted.elapsed() >= options.max_segment_age {
                        output.try_push_error_if_health_generation(ErrorFrame { error: "HTTP STT segment expired before dispatch".into(), fatal: false, processor: None, category: ErrorCategory::Connectivity }, false, segment.health)?;
                        continue;
                    }
                    if usage { output.try_push(Frame::new(FrameData::Metrics(vec![Metric::SttUsage { processor: name.into(), audio_seconds: segment.audio.len() as f64 / (g.rate as f64 * 2.0) }])), Direction::Downstream)?; }
                }
                let request = segment.backend.transcribe(segment, g.rate);
                tokio::pin!(request);
                let remaining = options.max_segment_age.saturating_sub(segment.accepted.elapsed());
                let deadline = tokio::time::sleep(remaining); tokio::pin!(deadline);
                let result = loop {
                    tokio::select! { biased;
                        _ = cancel.cancelled() => break Err(SpeechError::Cancelled),
                        _ = segment.token.cancelled() => break Err(SpeechError::Cancelled),
                        _ = &mut deadline => break Err(SpeechError::Timeout),
                        _ = health_updates.changed() => { let mut s = lock(&state)?; s.sync_health(&health); if !s.eligible(segment,&health) { break Err(SpeechError::Cancelled); } },
                        _ = tokio::time::sleep(HEALTH_POLL) => { let mut s = lock(&state)?; s.sync_health(&health); if !s.eligible(segment,&health) { break Err(SpeechError::Cancelled); } },
                        result = &mut request => break result,
                    }
                };
                #[cfg(test)]
                { let gate = { lock(&state)?.publication_gate.clone() }; if let Some(gate) = gate { gate.reached.notify_one(); gate.release.notified().await; } }
                let mut s = lock(&state)?; s.sync_health(&health);
                if !s.eligible(segment, &health) { continue; }
                let result = result.and_then(|mut result| {
                    if result.text.len() > options.max_transcript_bytes { return Err(SpeechError::TooLarge); }
                    result.text = result.text.trim().into(); Ok(result)
                });
                if metrics && !matches!(result, Err(SpeechError::Cancelled)) {
                    output.try_push(Frame::new(FrameData::Metrics(vec![Metric::Processing { processor: name.into(), seconds: started.elapsed().as_secs_f64() }])), Direction::Downstream)?;
                }
                match result {
                    Ok(result) if !result.text.is_empty() || options.push_empty_transcripts => {
                        // The mutex is the local mute/result admission fence.
                        // Health is rechecked but has an independent external setter.
                        if !s.eligible(segment, &health) { continue; }
                        let timestamp = chrono::Utc::now().to_rfc3339();
                        let data = if options.detailed_transcripts { FrameData::TranscriptionDetailed(DetailedTranscriptionFrame { text: TextFrame::new(result.text), user_id: segment.user_id.clone(), timestamp, language: result.language, result: Some(result.result), finalized: true }) }
                        else { FrameData::Transcription(TranscriptionFrame { text: result.text, user_id: segment.user_id.clone(), timestamp, language: result.language, result: Some(result.result), finalized: true }) };
                        output.try_push(Frame::new(data), Direction::Downstream)?;
                    }
                    Ok(_) | Err(SpeechError::Cancelled) => {},
                    Err(error) => {
                        let category = match error { SpeechError::Http(status) => ErrorCategory::from_http_status(status), SpeechError::Transport | SpeechError::Timeout => ErrorCategory::Connectivity, _ => ErrorCategory::Application };
                        output.try_push_error_if_health_generation(ErrorFrame { error: error.to_string(), fatal: false, processor: None, category }, false, segment.health)?;
                    }
                }
            }
            Ok(())
        })?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::Start(_) | FrameData::StartDetailed(_)
                if direction == Direction::Downstream =>
            {
                ctx.push(frame, direction).await?;
                for direction in [Direction::Downstream, Direction::Upstream] {
                    ctx.push(
                        Frame::new(FrameData::SttMetadata(SttMetadataFrame {
                            base: ServiceMetadataFrame {
                                service_name: self.name.into(),
                                user_turn_strategies: None,
                            },
                            ttfs_p99_latency: self.options.ttfs_p99_latency,
                        })),
                        direction,
                    )
                    .await?;
                }
                return Ok(());
            }
            FrameData::InputAudio(audio) if direction == Direction::Downstream => {
                let result = self.buffer(audio, "");
                if self.options.audio_passthrough {
                    ctx.push(frame, direction).await?;
                }
                return result;
            }
            FrameData::UserAudio(audio) if direction == Direction::Downstream => {
                let result = self.buffer(&audio.audio, &audio.user_id);
                if self.options.audio_passthrough {
                    ctx.push(frame, direction).await?;
                }
                return result;
            }
            FrameData::VadUserStartedSpeaking(_) | FrameData::VadUserStartedSpeakingDetailed(_) => {
                self.start_speech()?
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                // VAD forwarding precedes derived request usage/results.
                ctx.push(frame, direction).await?;
                return self.close_speech();
            }
            FrameData::SttMute(muted) => {
                self.mute(*muted)?;
                return Ok(());
            }
            FrameData::SttUpdateSettings(update) if self.targets(&update.service) => {
                self.update(update)?;
                return Ok(());
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self.finish(true)?,
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop => {
                self.finish(false)?
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.finish(false)
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.finish(true)
    }
}

#[cfg(test)]
#[path = "stt_review_unit.rs"]
mod review;
