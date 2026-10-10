// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Native AssemblyAI Sync HTTP STT and bounded PCM16 segmentation.
//!
//! Credentials and endpoints are supplied explicitly. No environment lookup,
//! upload/polling API, provider SDK, audio logging, or automatic retries are used.
use futures_util::StreamExt;
use pipecat_audio::{pcm_to_wav, read_pcm16_wav, PcmFormat};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, ErrorCategory, ErrorFrame, Frame, FrameData,
    FrameProcessor, Metric, ProcessorContext, ProcessorError, ProcessorHealth,
    ProcessorHealthGeneration, ServiceMetadataFrame, StartFrame, SttMetadataFrame,
    TranscriptionFrame,
};
use pipecat_services::CancellationToken;
use reqwest::{header::HeaderValue, multipart, Client};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{
    collections::VecDeque,
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Notify};
use url::Url;

pub const SYNC_BASE_URL: &str = "https://sync.assemblyai.com";
pub const SYNC_MODEL: &str = "universal-3-5-pro";
pub const SYNC_TTFS_P99: f64 = 0.65;
pub const MAX_SYNC_AUDIO_SECONDS: u32 = 120;
const MAX_CONFIG_BYTES: usize = 65_536;
const MAX_RESPONSE_BYTES: usize = 1_048_576;
const MAX_WAV_BYTES: usize = 120 * 48_000 * 4 + 65_536;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AssemblyAiSyncError {
    #[error("invalid AssemblyAI Sync configuration or audio: {0}")]
    Invalid(&'static str),
    #[error("AssemblyAI Sync HTTP status {status}")]
    Http { status: u16 },
    #[error("AssemblyAI Sync transport failed")]
    Transport,
    #[error("AssemblyAI Sync request timed out")]
    Timeout,
    #[error("AssemblyAI Sync request cancelled")]
    Cancelled,
    #[error("AssemblyAI Sync response exceeds the configured limit")]
    ResponseTooLarge,
    #[error("invalid AssemblyAI Sync JSON response")]
    Protocol,
    #[error("AssemblyAI Sync segment queue is full")]
    Backpressure,
    #[error("AssemblyAI Sync service is closed or unusable")]
    Closed,
}
impl AssemblyAiSyncError {
    pub fn category(&self) -> ErrorCategory {
        match self {
            Self::Http { status } => ErrorCategory::from_http_status(*status),
            Self::Transport | Self::Timeout => ErrorCategory::Connectivity,
            Self::Invalid(_) => ErrorCategory::InvalidRequest,
            _ => ErrorCategory::Application,
        }
    }
    /// Bad individual segments are recoverable. Only a rejected request/key
    /// poisons service health; transient failures never initiate a retry.
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            Self::Http {
                status: 400 | 401 | 403 | 404 | 422
            }
        )
    }
    fn processor_error(self) -> ProcessorError {
        ProcessorError::new(self.to_string())
            .with_category(self.category())
            .with_permanence_override(self.is_permanent())
    }
}
pub type SyncResult<T> = Result<T, AssemblyAiSyncError>;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ConversationContext {
    Text(String),
    Turns(Vec<String>),
}
impl ConversationContext {
    fn is_empty(&self) -> bool {
        match self {
            Self::Text(s) => s.is_empty(),
            Self::Turns(v) => v.is_empty(),
        }
    }
    fn chars(&self) -> usize {
        match self {
            Self::Text(s) => s.chars().count(),
            Self::Turns(v) => v.iter().map(|s| s.chars().count()).sum(),
        }
    }
}
impl fmt::Debug for ConversationContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ConversationContext([redacted])")
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AssemblyAiSyncSettings {
    pub model: String,
    pub language: Option<String>,
    pub language_codes: Option<Vec<String>>,
    pub prompt: Option<String>,
    pub keyterms_prompt: Option<Vec<String>>,
    pub conversation_context: Option<ConversationContext>,
    pub timestamps: Option<bool>,
}
impl Default for AssemblyAiSyncSettings {
    fn default() -> Self {
        Self {
            model: SYNC_MODEL.into(),
            language: Some("en".into()),
            language_codes: None,
            prompt: None,
            keyterms_prompt: None,
            conversation_context: None,
            timestamps: None,
        }
    }
}
impl fmt::Debug for AssemblyAiSyncSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssemblyAiSyncSettings")
            .field("content", &"[redacted]")
            .finish()
    }
}

#[derive(Clone)]
pub struct AssemblyAiSyncConfig {
    api_key: String,
    pub base_url: String,
    pub settings: AssemblyAiSyncSettings,
    pub request_timeout: Duration,
    pub warm_timeout: Duration,
    pub max_response_bytes: usize,
    pub max_context_turns: usize,
    pub max_context_chars: usize,
}
impl AssemblyAiSyncConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: SYNC_BASE_URL.into(),
            settings: Default::default(),
            request_timeout: Duration::from_secs(180),
            warm_timeout: Duration::from_secs(5),
            max_response_bytes: MAX_RESPONSE_BYTES,
            max_context_turns: 5,
            max_context_chars: 1500,
        }
    }
    fn validate(&self) -> SyncResult<Url> {
        if self.api_key.is_empty()
            || self.api_key.len() > 4096
            || HeaderValue::from_str(&self.api_key).is_err()
        {
            return Err(AssemblyAiSyncError::Invalid(
                "API key must be a nonempty valid header",
            ));
        }
        if self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(600)
            || self.warm_timeout.is_zero()
            || self.warm_timeout > Duration::from_secs(30)
            || self.max_response_bytes == 0
            || self.max_response_bytes > MAX_RESPONSE_BYTES
            || self.max_context_turns > 128
            || self.max_context_chars > 65_536
        {
            return Err(AssemblyAiSyncError::Invalid(
                "invalid timeout or buffer limit",
            ));
        }
        self.validate_settings(&self.settings)?;
        let mut url = Url::parse(&self.base_url)
            .map_err(|_| AssemblyAiSyncError::Invalid("invalid endpoint URL"))?;
        let loopback = match url.host() {
            Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
            // HTTP hostnames are intentionally not allowed, including localhost:
            // only a literal loopback address avoids DNS ambiguity.
            _ => false,
        };
        if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
            || url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || (url.path() != "/" && !url.path().is_empty())
        {
            return Err(AssemblyAiSyncError::Invalid(
                "endpoint must be HTTPS origin or literal HTTP loopback",
            ));
        }
        url.set_path("/");
        Ok(url)
    }
    fn validate_settings(&self, settings: &AssemblyAiSyncSettings) -> SyncResult<()> {
        if settings.model.is_empty()
            || settings.model.len() > 256
            || HeaderValue::from_str(&settings.model).is_err()
        {
            return Err(AssemblyAiSyncError::Invalid(
                "model must be a nonempty valid header",
            ));
        }
        if let Some(language) = &settings.language {
            normalize_language(language)?;
        }
        if let Some(languages) = &settings.language_codes {
            // There is no streaming-style ten-language limit on Sync.
            if languages.len() > 1024 {
                return Err(AssemblyAiSyncError::Invalid(
                    "language list exceeds local input bound",
                ));
            }
            for language in languages {
                normalize_language(language)?;
            }
        }
        if settings
            .prompt
            .as_ref()
            .is_some_and(|s| s.len() > MAX_CONFIG_BYTES)
            || settings
                .keyterms_prompt
                .as_ref()
                .is_some_and(|v| v.len() > 1024 || v.iter().any(|s| s.len() > MAX_CONFIG_BYTES))
        {
            return Err(AssemblyAiSyncError::Invalid(
                "prompt or keyterms exceed local input bound",
            ));
        }
        if let Some(context) = &settings.conversation_context {
            if context.chars() > self.max_context_chars
                || matches!(context, ConversationContext::Turns(v) if v.len() > 128)
            {
                return Err(AssemblyAiSyncError::Invalid(
                    "manual context exceeds context budget",
                ));
            }
        }
        let encoded = serde_json::to_vec(settings)
            .map_err(|_| AssemblyAiSyncError::Invalid("invalid settings"))?;
        if encoded.len() > MAX_CONFIG_BYTES {
            return Err(AssemblyAiSyncError::Invalid("settings exceed byte budget"));
        }
        Ok(())
    }
}
impl fmt::Debug for AssemblyAiSyncConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssemblyAiSyncConfig")
            .field("api_key", &"[redacted]")
            .field("base_url", &"[redacted]")
            .field("settings", &self.settings)
            .finish()
    }
}

fn normalize_language(language: &str) -> SyncResult<String> {
    crate::prepare_language_codes(&[language.to_owned()], None)
        .map_err(|_| AssemblyAiSyncError::Invalid("invalid language code"))?
        .into_iter()
        .next()
        .ok_or(AssemblyAiSyncError::Invalid("invalid language code"))
}

/// A serial, stateful conversation. `&mut self` prevents concurrent requests
/// from racing automatic context. Dropping a request future aborts its I/O.
pub struct AssemblyAiSyncStt {
    config: AssemblyAiSyncConfig,
    base_url: Url,
    client: Client,
    context: VecDeque<String>,
    usable: bool,
}
impl fmt::Debug for AssemblyAiSyncStt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssemblyAiSyncStt")
            .field("usable", &self.usable)
            .field("context_turns", &self.context.len())
            .finish()
    }
}
impl AssemblyAiSyncStt {
    pub fn new(config: AssemblyAiSyncConfig) -> SyncResult<Self> {
        let base_url = config.validate()?;
        // A private client has no inherited default auth/cookies. Redirects and
        // environment proxies must not move credentials to another destination.
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| AssemblyAiSyncError::Transport)?;
        Ok(Self {
            config,
            base_url,
            client,
            context: VecDeque::new(),
            usable: true,
        })
    }
    pub fn settings(&self) -> &AssemblyAiSyncSettings {
        &self.config.settings
    }
    pub fn is_usable(&self) -> bool {
        self.usable
    }
    pub fn context_turns(&self) -> &VecDeque<String> {
        &self.context
    }
    /// Explicitly begin another conversation. Does not restore rejected credentials.
    pub fn reset_context(&mut self) {
        self.context.clear();
    }
    pub fn update_settings(&mut self, settings: AssemblyAiSyncSettings) -> SyncResult<()> {
        self.config.validate_settings(&settings)?;
        if self.config.settings != settings {
            self.config.settings = settings;
        }
        Ok(())
    }
    pub fn append_assistant_turn(&mut self, text: &str) {
        self.append_context(text);
    }
    fn manual_context(&self) -> bool {
        self.config
            .settings
            .conversation_context
            .as_ref()
            .is_some_and(|c| !c.is_empty())
    }
    fn append_context(&mut self, text: &str) {
        if self.manual_context()
            || self.config.max_context_turns == 0
            || self.config.max_context_chars == 0
        {
            return;
        }
        let text = text.trim();
        if text.is_empty() {
            return;
        }
        let text: String = text.chars().take(self.config.max_context_chars).collect();
        self.context.push_back(text);
        while self.context.len() > self.config.max_context_turns
            || self
                .context
                .iter()
                .map(|s| s.chars().count())
                .sum::<usize>()
                > self.config.max_context_chars
        {
            self.context.pop_front();
        }
    }
    pub fn build_config(&self) -> SyncResult<Value> {
        let mut config = Map::new();
        let settings = &self.config.settings;
        let mut languages = Vec::new();
        if let Some(codes) = settings.language_codes.as_ref().filter(|v| !v.is_empty()) {
            for code in codes {
                let code = normalize_language(code)?;
                if !languages.contains(&code) {
                    languages.push(code);
                }
            }
        } else if let Some(language) = &settings.language {
            languages.push(normalize_language(language)?);
        }
        if !languages.is_empty() {
            config.insert("language_codes".into(), serde_json::json!(languages));
        }
        if let Some(prompt) = &settings.prompt {
            config.insert("prompt".into(), prompt.clone().into());
        }
        if let Some(keyterms) = settings.keyterms_prompt.as_ref().filter(|v| !v.is_empty()) {
            config.insert("keyterms_prompt".into(), serde_json::json!(keyterms));
        }
        if let Some(timestamps) = settings.timestamps {
            config.insert("timestamps".into(), timestamps.into());
        }
        if self.manual_context() {
            config.insert(
                "conversation_context".into(),
                serde_json::to_value(&settings.conversation_context)
                    .map_err(|_| AssemblyAiSyncError::Protocol)?,
            );
        } else if !self.context.is_empty() {
            config.insert(
                "conversation_context".into(),
                serde_json::json!(self.context),
            );
        }
        let value = Value::Object(config);
        if serde_json::to_vec(&value)
            .map_err(|_| AssemblyAiSyncError::Protocol)?
            .len()
            > MAX_CONFIG_BYTES
        {
            return Err(AssemblyAiSyncError::Invalid("config exceeds byte budget"));
        }
        Ok(value)
    }
    /// Best-effort prewarming shares the exact transcription connection pool.
    /// Neither authentication nor conversation settings are sent to `/v1/warm`.
    pub async fn warm(&self, cancellation: &CancellationToken) {
        warm(
            &self.client,
            &self.base_url,
            &self.config.settings.model,
            self.config.warm_timeout,
            self.config.max_response_bytes,
            cancellation,
        )
        .await;
    }
    pub async fn transcribe_pcm(
        &mut self,
        audio: &AudioRawFrame,
        cancellation: &CancellationToken,
    ) -> SyncResult<Option<TranscriptionFrame>> {
        validate_pcm(audio)?;
        let wav = pcm_to_wav(&audio.audio, audio.sample_rate, audio.num_channels)
            .map_err(|_| AssemblyAiSyncError::Invalid("invalid PCM16 audio"))?;
        self.transcribe_wav(&wav, cancellation).await
    }
    pub async fn transcribe_wav(
        &mut self,
        audio: &[u8],
        cancellation: &CancellationToken,
    ) -> SyncResult<Option<TranscriptionFrame>> {
        if cancellation.is_cancelled() {
            return Err(AssemblyAiSyncError::Cancelled);
        }
        if !self.usable {
            return Err(AssemblyAiSyncError::Closed);
        }
        validate_wav(audio)?;
        let config = self.build_config()?;
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Err(AssemblyAiSyncError::Cancelled),
            result = tokio::time::timeout(self.config.request_timeout, self.request(audio, config)) => {
                result.map_err(|_| AssemblyAiSyncError::Timeout)?
            }
        };
        if cancellation.is_cancelled() {
            return Err(AssemblyAiSyncError::Cancelled);
        }
        if let Err(error) = &result {
            if error.is_permanent() {
                self.usable = false;
            }
        }
        let result = result?;
        let text = match result.get("text") {
            None | Some(Value::Null) => "",
            Some(Value::String(text)) => text.trim(),
            _ => return Err(AssemblyAiSyncError::Protocol),
        };
        if text.is_empty() {
            return Ok(None);
        }
        let language = self
            .config
            .settings
            .language
            .as_ref()
            .map(|s| normalize_language(s))
            .transpose()?;
        let frame = TranscriptionFrame {
            text: text.into(),
            user_id: String::new(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            language,
            result: Some(result.clone()),
            finalized: true,
        };
        self.append_context(text);
        Ok(Some(frame))
    }
    async fn request(&self, audio: &[u8], config: Value) -> SyncResult<Value> {
        let mut form = multipart::Form::new().part(
            "audio",
            multipart::Part::bytes(audio.to_vec())
                .file_name("audio.wav")
                .mime_str("audio/wav")
                .map_err(|_| AssemblyAiSyncError::Protocol)?,
        );
        if config.as_object().is_some_and(|c| !c.is_empty()) {
            form = form.part(
                "config",
                multipart::Part::text(config.to_string())
                    .mime_str("application/json")
                    .map_err(|_| AssemblyAiSyncError::Protocol)?,
            );
        }
        let mut authorization = HeaderValue::from_str(&self.config.api_key)
            .map_err(|_| AssemblyAiSyncError::Invalid("invalid API key"))?;
        authorization.set_sensitive(true);
        let response = self
            .client
            .post(
                self.base_url
                    .join("v1/transcribe")
                    .map_err(|_| AssemblyAiSyncError::Invalid("invalid endpoint"))?,
            )
            .header(reqwest::header::AUTHORIZATION, authorization)
            .header("X-AAI-Model", &self.config.settings.model)
            .multipart(form)
            .send()
            .await
            .map_err(map_transport)?;
        if response.status().as_u16() != 200 {
            // Provider error bodies can echo credentials, prompts, or audio text.
            // Preserve only typed HTTP status, never untrusted response contents.
            return Err(AssemblyAiSyncError::Http {
                status: response.status().as_u16(),
            });
        }
        let bytes = bounded_body(response, self.config.max_response_bytes).await?;
        let result: Value =
            serde_json::from_slice(&bytes).map_err(|_| AssemblyAiSyncError::Protocol)?;
        if !result.is_object() {
            return Err(AssemblyAiSyncError::Protocol);
        }
        Ok(result)
    }
}
fn map_transport(error: reqwest::Error) -> AssemblyAiSyncError {
    if error.is_timeout() {
        AssemblyAiSyncError::Timeout
    } else {
        AssemblyAiSyncError::Transport
    }
}
async fn bounded_body(response: reqwest::Response, limit: usize) -> SyncResult<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        return Err(AssemblyAiSyncError::ResponseTooLarge);
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(map_transport)?;
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(AssemblyAiSyncError::ResponseTooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
async fn warm(
    client: &Client,
    base_url: &Url,
    model: &str,
    timeout: Duration,
    limit: usize,
    cancellation: &CancellationToken,
) {
    let request = async {
        let url = base_url
            .join("v1/warm")
            .map_err(|_| AssemblyAiSyncError::Invalid("invalid endpoint"))?;
        let response = client
            .get(url)
            .header("X-AAI-Model", model)
            .send()
            .await
            .map_err(map_transport)?;
        bounded_body(response, limit).await.map(|_| ())
    };
    tokio::select! { biased; _ = cancellation.cancelled() => {}, _ = tokio::time::timeout(timeout, request) => {} }
}
fn validate_format(sample_rate: u32, channels: u16) -> SyncResult<PcmFormat> {
    if !(8_000..=48_000).contains(&sample_rate) || !(1..=2).contains(&channels) {
        return Err(AssemblyAiSyncError::Invalid(
            "expected 8–48 kHz mono/stereo PCM16",
        ));
    }
    PcmFormat::new(sample_rate, channels)
        .map_err(|_| AssemblyAiSyncError::Invalid("invalid PCM format"))
}
fn validate_pcm(audio: &AudioRawFrame) -> SyncResult<()> {
    let format = validate_format(audio.sample_rate, audio.num_channels)?;
    format
        .validate(&audio.audio)
        .map_err(|_| AssemblyAiSyncError::Invalid("incomplete PCM16 frame"))?;
    if audio.audio.is_empty()
        || audio.audio.len() / format.frame_bytes()
            > audio.sample_rate as usize * MAX_SYNC_AUDIO_SECONDS as usize
    {
        return Err(AssemblyAiSyncError::Invalid(
            "audio must be nonempty and no longer than 120 seconds",
        ));
    }
    Ok(())
}
fn validate_wav(audio: &[u8]) -> SyncResult<()> {
    if audio.len() > MAX_WAV_BYTES {
        return Err(AssemblyAiSyncError::Invalid("WAV exceeds byte budget"));
    }
    let wav = read_pcm16_wav(audio)
        .map_err(|_| AssemblyAiSyncError::Invalid("expected complete PCM16 WAV"))?;
    if u32::from_le_bytes(audio[4..8].try_into().unwrap()) as usize + 8 != audio.len() {
        return Err(AssemblyAiSyncError::Invalid(
            "trailing WAV data is not accepted",
        ));
    }
    validate_format(wav.format.sample_rate(), wav.format.channels())?;
    if wav.pcm.is_empty()
        || wav.pcm.len() / wav.format.frame_bytes()
            > wav.format.sample_rate() as usize * MAX_SYNC_AUDIO_SECONDS as usize
    {
        return Err(AssemblyAiSyncError::Invalid(
            "audio must be nonempty and no longer than 120 seconds",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncSegmentation {
    /// Buffer a complete utterance between VAD start and stop frames.
    Vad,
    /// Treat each input audio frame as one complete utterance.
    AudioFrames,
}
#[derive(Debug, Clone)]
pub struct SyncProcessorOptions {
    pub sample_rate: u32,
    pub channels: u16,
    pub segmentation: SyncSegmentation,
    pub pre_roll: Duration,
    /// PCM silence appended to VAD utterances; included in the 120-second cap.
    pub trailing_silence: Duration,
    pub enable_prewarming: bool,
    pub audio_passthrough: bool,
    pub queue_capacity: usize,
    /// Includes queued and in-flight PCM, excluding bounded VAD pre-roll.
    pub max_pending_audio_bytes: usize,
    pub ttfs_p99_latency: f64,
}
impl Default for SyncProcessorOptions {
    fn default() -> Self {
        Self {
            sample_rate: 16_000,
            channels: 1,
            segmentation: SyncSegmentation::Vad,
            pre_roll: Duration::from_secs(1),
            trailing_silence: Duration::from_millis(500),
            enable_prewarming: true,
            audio_passthrough: true,
            queue_capacity: 4,
            max_pending_audio_bytes: 16 * 1024 * 1024,
            ttfs_p99_latency: SYNC_TTFS_P99,
        }
    }
}
struct Segment {
    audio: Vec<u8>,
    user_id: String,
    generation: ProcessorHealthGeneration,
}
enum SyncWork {
    Segment(Segment),
    Assistant(String, ProcessorHealthGeneration),
    Settings(AssemblyAiSyncSettings),
}
struct SyncState {
    sender: Option<mpsc::Sender<SyncWork>>,
    settings: AssemblyAiSyncSettings,
    audio: Vec<u8>,
    user_id: String,
    speaking: bool,
    discarded: bool,
    muted: bool,
    started: bool,
    ending: bool,
    pending_bytes: usize,
    pending_changed: Arc<Notify>,
    generation: ProcessorHealthGeneration,
}
impl SyncState {
    fn sync_health(&mut self, health: &ProcessorHealth) -> bool {
        let current = health.generation();
        if self.generation != current || !current.snapshot.is_usable {
            self.generation = current;
            self.audio.clear();
            self.user_id.clear();
            self.speaking = false;
            self.discarded = true;
        }
        current.snapshot.is_usable
    }
}
struct PendingBytes {
    state: Arc<Mutex<SyncState>>,
    bytes: usize,
}
impl Drop for PendingBytes {
    fn drop(&mut self) {
        if let Ok(mut state) = self.state.lock() {
            state.pending_bytes = state.pending_bytes.saturating_sub(self.bytes);
            state.pending_changed.notify_one();
        }
    }
}
struct WarmGuard(Arc<AtomicBool>);
impl Drop for WarmGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Single-run native segmented processor. An ordered bounded worker carries
/// user/assistant context between HTTP requests. User STT survives bot
/// interruptions; Cancel aborts it, while End/Stop flush and drain accepted work.
pub struct AssemblyAiSyncProcessor {
    service: Mutex<Option<AssemblyAiSyncStt>>,
    config: AssemblyAiSyncConfig,
    options: SyncProcessorOptions,
    state: Arc<Mutex<SyncState>>,
    health: ProcessorHealth,
    cancellation: CancellationToken,
    warm_cancellation: CancellationToken,
    warm_active: Arc<AtomicBool>,
    warm_client: Client,
    warm_url: Url,
}
impl AssemblyAiSyncProcessor {
    pub fn new(config: AssemblyAiSyncConfig, options: SyncProcessorOptions) -> SyncResult<Self> {
        validate_format(options.sample_rate, options.channels)?;
        if options.pre_roll > Duration::from_secs(5)
            || options.trailing_silence > Duration::from_secs(5)
            || options.queue_capacity == 0
            || options.queue_capacity > 16
            || options.max_pending_audio_bytes == 0
            || options.max_pending_audio_bytes > 64 * 1024 * 1024
            || !options.ttfs_p99_latency.is_finite()
            || options.ttfs_p99_latency < 0.0
        {
            return Err(AssemblyAiSyncError::Invalid(
                "invalid segmentation or queue limits",
            ));
        }
        let service = AssemblyAiSyncStt::new(config.clone())?;
        let health = ProcessorHealth::default();
        Ok(Self {
            warm_client: service.client.clone(),
            warm_url: service.base_url.clone(),
            service: Mutex::new(Some(service)),
            state: Arc::new(Mutex::new(SyncState {
                sender: None,
                settings: config.settings.clone(),
                audio: vec![],
                user_id: String::new(),
                speaking: false,
                discarded: false,
                muted: false,
                started: false,
                ending: false,
                pending_bytes: 0,
                pending_changed: Arc::new(Notify::new()),
                generation: health.generation(),
            })),
            config,
            options,
            health,
            cancellation: CancellationToken::new(),
            warm_cancellation: CancellationToken::new(),
            warm_active: Arc::new(AtomicBool::new(false)),
        })
    }
    fn targets(&self, target: &Option<pipecat_core::ProcessorReference>) -> bool {
        match target {
            None => true,
            Some(pipecat_core::ProcessorReference::Name(name)) => name == self.name(),
            Some(pipecat_core::ProcessorReference::Local(target)) => {
                std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
            }
        }
    }
    fn lock(&self) -> SyncResult<std::sync::MutexGuard<'_, SyncState>> {
        self.state.lock().map_err(|_| AssemblyAiSyncError::Closed)
    }
    fn trailing_silence_bytes(&self) -> usize {
        if self.options.segmentation == SyncSegmentation::AudioFrames {
            return 0;
        }
        (self.options.trailing_silence.as_secs_f64() * self.options.sample_rate as f64).floor()
            as usize
            * self.options.channels as usize
            * 2
    }
    fn submit(&self, state: &mut SyncState, mut audio: Vec<u8>, user_id: String) -> SyncResult<()> {
        if audio.is_empty() || !state.sync_health(&self.health) || state.muted {
            return Ok(());
        }
        let padded_length = audio.len().saturating_add(self.trailing_silence_bytes());
        let max = self.options.sample_rate as usize
            * self.options.channels as usize
            * 2
            * MAX_SYNC_AUDIO_SECONDS as usize;
        if padded_length > max {
            return Err(AssemblyAiSyncError::Invalid(
                "speech plus trailing silence exceeds 120 seconds",
            ));
        }
        if state.pending_bytes.saturating_add(padded_length) > self.options.max_pending_audio_bytes
        {
            return Err(AssemblyAiSyncError::Backpressure);
        }
        audio.resize(padded_length, 0);
        let size = audio.len();
        let sender = state.sender.as_ref().ok_or(AssemblyAiSyncError::Closed)?;
        sender
            .try_send(SyncWork::Segment(Segment {
                audio,
                user_id,
                generation: state.generation,
            }))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => AssemblyAiSyncError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => AssemblyAiSyncError::Closed,
            })?;
        state.pending_bytes += size;
        Ok(())
    }
    fn buffer_audio(&self, audio: &AudioRawFrame, user_id: &str) -> SyncResult<()> {
        let mut state = self.lock()?;
        if state.ending {
            return Err(AssemblyAiSyncError::Closed);
        }
        if !state.sync_health(&self.health) || state.muted {
            return Ok(());
        }
        if audio.sample_rate != self.options.sample_rate
            || audio.num_channels != self.options.channels
            || validate_pcm(audio).is_err()
            || user_id.len() > 1024
        {
            state.audio.clear();
            state.speaking = false;
            state.discarded = true;
            return Err(AssemblyAiSyncError::Invalid(
                "invalid segment format, length, or user ID",
            ));
        }
        if self.options.segmentation == SyncSegmentation::AudioFrames {
            return self.submit(&mut state, audio.audio.clone(), user_id.into());
        }
        if state.discarded {
            return Ok(());
        }
        if !state.audio.is_empty() && state.user_id != user_id {
            state.audio.clear();
            if state.speaking {
                state.speaking = false;
                state.discarded = true;
                return Err(AssemblyAiSyncError::Invalid(
                    "speaker changed within one segment",
                ));
            }
        }
        state.user_id = user_id.into();
        if state.speaking {
            let max = self.options.sample_rate as usize
                * self.options.channels as usize
                * 2
                * MAX_SYNC_AUDIO_SECONDS as usize
                - self.trailing_silence_bytes();
            if state.audio.len().saturating_add(audio.audio.len()) > max {
                state.audio.clear();
                state.speaking = false;
                state.discarded = true;
                return Err(AssemblyAiSyncError::Invalid(
                    "speech exceeds 120 seconds; segment discarded",
                ));
            }
            state.audio.extend_from_slice(&audio.audio);
        } else {
            let frame_bytes = self.options.channels as usize * 2;
            let max = (self.options.pre_roll.as_secs_f64() * self.options.sample_rate as f64)
                .floor() as usize
                * frame_bytes;
            if audio.audio.len() >= max {
                state.audio.clear();
                state
                    .audio
                    .extend_from_slice(&audio.audio[audio.audio.len() - max..]);
            } else {
                let discard = state
                    .audio
                    .len()
                    .saturating_add(audio.audio.len())
                    .saturating_sub(max);
                state.audio.drain(..discard);
                state.audio.extend_from_slice(&audio.audio);
            }
        }
        Ok(())
    }
    fn stop_speech(&self) -> SyncResult<()> {
        let mut state = self.lock()?;
        if !state.sync_health(&self.health)
            || state.ending
            || state.muted
            || state.discarded
            || !state.speaking
        {
            return Ok(());
        }
        state.speaking = false;
        let audio = std::mem::take(&mut state.audio);
        let user_id = state.user_id.clone();
        self.submit(&mut state, audio, user_id)
    }
    fn cancel(&self) -> SyncResult<()> {
        self.warm_cancellation.cancel();
        self.cancellation.cancel();
        let mut state = self.lock()?;
        state.ending = true;
        state.audio.clear();
        state.speaking = false;
        state.sender.take();
        state.pending_changed.notify_one();
        Ok(())
    }
    async fn finish_graceful(&self) -> SyncResult<()> {
        self.warm_cancellation.cancel();
        let (mut audio, user_id, generation, sender, pending_changed) = {
            let mut state = self.lock()?;
            if state.ending {
                return Ok(());
            }
            let usable = state.sync_health(&self.health);
            let audio = if usable && state.speaking && !state.discarded && !state.muted {
                std::mem::take(&mut state.audio)
            } else {
                Vec::new()
            };
            state.ending = true;
            state.audio.clear();
            state.speaking = false;
            (
                audio,
                state.user_id.clone(),
                state.generation,
                state.sender.take(),
                state.pending_changed.clone(),
            )
        };
        let Some(sender) = sender else {
            return Ok(());
        };
        if audio.is_empty() {
            return Ok(());
        }
        let size = audio.len().saturating_add(self.trailing_silence_bytes());
        let maximum = self.options.sample_rate as usize
            * self.options.channels as usize
            * 2
            * MAX_SYNC_AUDIO_SECONDS as usize;
        if size > maximum {
            return Err(AssemblyAiSyncError::Invalid(
                "speech plus trailing silence exceeds 120 seconds",
            ));
        }
        if size > self.options.max_pending_audio_bytes {
            return Err(AssemblyAiSyncError::Backpressure);
        }
        audio.resize(size, 0);
        let admission = async {
            loop {
                if self.health.generation() != generation || !self.health.is_usable() {
                    return Ok(());
                }
                let permit = sender
                    .reserve()
                    .await
                    .map_err(|_| AssemblyAiSyncError::Closed)?;
                {
                    let mut state = self.lock()?;
                    if state.pending_bytes.saturating_add(size)
                        <= self.options.max_pending_audio_bytes
                    {
                        state.pending_bytes += size;
                        permit.send(SyncWork::Segment(Segment {
                            audio,
                            user_id,
                            generation,
                        }));
                        return Ok(());
                    }
                }
                drop(permit);
                pending_changed.notified().await;
            }
        };
        // Enough for every already admitted request to finish once. A stalled
        // output consumer cannot hold graceful admission without a deadline.
        let deadline = self
            .config
            .request_timeout
            .saturating_mul((self.options.queue_capacity + 1) as u32);
        tokio::select! { biased;
            _ = self.cancellation.cancelled() => Err(AssemblyAiSyncError::Cancelled),
            result = tokio::time::timeout(deadline, admission) => result.map_err(|_| AssemblyAiSyncError::Timeout)?,
        }
    }
    pub fn update_settings(&self, settings: AssemblyAiSyncSettings) -> SyncResult<()> {
        self.config.validate_settings(&settings)?;
        let mut state = self.lock()?;
        if state.ending {
            return Err(AssemblyAiSyncError::Closed);
        }
        if state.settings == settings {
            return Ok(());
        }
        if let Some(sender) = &state.sender {
            sender
                .try_send(SyncWork::Settings(settings.clone()))
                .map_err(|e| match e {
                    mpsc::error::TrySendError::Full(_) => AssemblyAiSyncError::Backpressure,
                    mpsc::error::TrySendError::Closed(_) => AssemblyAiSyncError::Closed,
                })?;
        }
        state.settings = settings;
        Ok(())
    }
    fn assistant_turn(&self, text: &str) -> SyncResult<()> {
        let mut state = self.lock()?;
        if !state.sync_health(&self.health) || state.ending {
            return Ok(());
        }
        let text: String = text
            .trim()
            .chars()
            .take(self.config.max_context_chars)
            .collect();
        if text.is_empty() {
            return Ok(());
        }
        state
            .sender
            .as_ref()
            .ok_or(AssemblyAiSyncError::Closed)?
            .try_send(SyncWork::Assistant(text, state.generation))
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => AssemblyAiSyncError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => AssemblyAiSyncError::Closed,
            })
    }
    fn start_warm(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if !self.options.enable_prewarming
            || self.warm_cancellation.is_cancelled()
            || self.warm_active.swap(true, Ordering::AcqRel)
        {
            return Ok(());
        }
        let guard = WarmGuard(self.warm_active.clone());
        let model = self
            .lock()
            .map_err(AssemblyAiSyncError::processor_error)?
            .settings
            .model
            .clone();
        let client = self.warm_client.clone();
        let url = self.warm_url.clone();
        let cancellation = self.warm_cancellation.clone();
        let timeout = self.config.warm_timeout;
        let limit = self.config.max_response_bytes;
        ctx.spawn("assemblyai-sync-warm", false, async move {
            let _guard = guard;
            warm(&client, &url, &model, timeout, limit, &cancellation).await;
            Ok(())
        })
        .map(|_| ())
    }
}
impl Drop for AssemblyAiSyncProcessor {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.warm_cancellation.cancel();
    }
}
#[async_trait]
impl FrameProcessor for AssemblyAiSyncProcessor {
    fn name(&self) -> &str {
        "AssemblyAISyncSTTService"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if start.audio_in_sample_rate != self.options.sample_rate {
            return Err(AssemblyAiSyncError::Invalid(
                "pipeline sample rate differs from configured PCM format",
            )
            .processor_error());
        }
        let (sender, mut receiver) = mpsc::channel(self.options.queue_capacity);
        let settings = {
            let mut state = self.lock().map_err(AssemblyAiSyncError::processor_error)?;
            if state.started || state.ending {
                return Err(AssemblyAiSyncError::Closed.processor_error());
            }
            state.started = true;
            state.sender = Some(sender);
            state.settings.clone()
        };
        let mut service = self
            .service
            .lock()
            .map_err(|_| AssemblyAiSyncError::Closed.processor_error())?
            .take()
            .ok_or_else(|| AssemblyAiSyncError::Closed.processor_error())?;
        service
            .update_settings(settings)
            .map_err(AssemblyAiSyncError::processor_error)?;
        service.reset_context();
        let state = self.state.clone();
        let health = self.health.clone();
        let cancellation = self.cancellation.clone();
        let output = ctx.clone();
        let options = self.options.clone();
        let emit_metrics = start.enable_metrics;
        let emit_usage = start.enable_usage_metrics;
        ctx.spawn("assemblyai-sync-segments", false, async move {
            let mut health_updates = health.watch();
            let mut context_generation = health.generation();
            loop {
                let work = tokio::select! { biased;
                    _ = cancellation.cancelled() => break,
                    _ = health_updates.changed() => { service.reset_context(); context_generation = health.generation(); continue; },
                    work = receiver.recv() => work,
                };
                let Some(work) = work else { break; };
                let current_generation = health.generation();
                if context_generation != current_generation {
                    service.reset_context(); context_generation = current_generation;
                }
                match work {
                    SyncWork::Settings(settings) => { service.update_settings(settings).map_err(AssemblyAiSyncError::processor_error)?; }
                    SyncWork::Assistant(text, generation) => {
                        if health.generation() == generation && health.is_usable() { service.append_assistant_turn(&text); }
                    }
                    SyncWork::Segment(segment) => {
                        let _pending = PendingBytes { state: state.clone(), bytes: segment.audio.len() };
                        if health.generation() != segment.generation || !health.is_usable() { service.reset_context(); continue; }
                        // The pipeline health handle owns explicit recovery for this adapter.
                        service.usable = true;
                        let started = Instant::now();
                        let audio_seconds = segment.audio.len() as f64 / (options.sample_rate as f64 * options.channels as f64 * 2.0);
                        let audio = AudioRawFrame { audio: segment.audio, sample_rate: options.sample_rate, num_channels: options.channels, context_id: None };
                        if emit_usage { output.push(Frame::new(FrameData::Metrics(vec![Metric::SttUsage { processor: "AssemblyAISyncSTTService".into(), audio_seconds }])), Direction::Downstream).await?; }
                        health_updates.borrow_and_update();
                        if health.generation() != segment.generation || !health.is_usable() { service.reset_context(); continue; }
                        let result = tokio::select! { biased;
                            _ = cancellation.cancelled() => break,
                            _ = health_updates.changed() => { service.reset_context(); continue; },
                            result = service.transcribe_pcm(&audio, &cancellation) => result,
                        };
                        if cancellation.is_cancelled() { break; }
                        if health.generation() != segment.generation || !health.is_usable() { service.reset_context(); continue; }
                        match result {
                            Ok(transcript) => {
                                let mut metrics = Vec::new();
                                if emit_metrics { metrics.push(Metric::Processing { processor: "AssemblyAISyncSTTService".into(), seconds: started.elapsed().as_secs_f64() }); }
                                if !metrics.is_empty() { output.push(Frame::new(FrameData::Metrics(metrics)), Direction::Downstream).await?; }
                                if let Some(mut transcript) = transcript {
                                    transcript.user_id = segment.user_id;
                                    output.push(Frame::new(FrameData::Transcription(transcript)), Direction::Downstream).await?;
                                }
                            }
                            Err(AssemblyAiSyncError::Cancelled) => {},
                            Err(error) => {
                                output.push_error_if_health_generation(ErrorFrame { error: error.to_string(), fatal: false,
                                    processor: None, category: error.category() }, error.is_permanent(), segment.generation).await?;
                            }
                        }
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
            FrameData::Start(_) if direction == Direction::Downstream => {
                ctx.push(frame, direction).await?;
                return ctx
                    .push(
                        Frame::new(FrameData::SttMetadata(SttMetadataFrame {
                            base: ServiceMetadataFrame {
                                service_name: self.name().into(),
                                user_turn_strategies: None,
                            },
                            ttfs_p99_latency: self.options.ttfs_p99_latency,
                        })),
                        direction,
                    )
                    .await;
            }
            FrameData::InputAudio(audio) if direction == Direction::Downstream => {
                self.buffer_audio(audio, "")
                    .map_err(AssemblyAiSyncError::processor_error)?;
                if !self.options.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::UserAudio(audio) if direction == Direction::Downstream => {
                self.buffer_audio(&audio.audio, &audio.user_id)
                    .map_err(AssemblyAiSyncError::processor_error)?;
                if !self.options.audio_passthrough {
                    return Ok(());
                }
            }
            FrameData::VadUserStartedSpeaking(_) | FrameData::VadUserStartedSpeakingDetailed(_) => {
                let should_warm = {
                    let mut state = self.lock().map_err(AssemblyAiSyncError::processor_error)?;
                    if state.sync_health(&self.health) && !state.ending && !state.muted {
                        state.speaking = true;
                        state.discarded = false;
                        true
                    } else {
                        false
                    }
                };
                if should_warm {
                    self.start_warm(ctx)?;
                }
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                self.stop_speech()
                    .map_err(AssemblyAiSyncError::processor_error)?
            }
            FrameData::SttMute(muted) => {
                let mut state = self.lock().map_err(AssemblyAiSyncError::processor_error)?;
                state.muted = *muted;
                if *muted {
                    state.audio.clear();
                    state.speaking = false;
                    state.discarded = true;
                }
            }
            FrameData::SttUpdateSettings(update) if self.targets(&update.service) => {
                let current = self
                    .lock()
                    .map_err(AssemblyAiSyncError::processor_error)?
                    .settings
                    .clone();
                let mut value = serde_json::to_value(current)
                    .map_err(|_| AssemblyAiSyncError::Protocol.processor_error())?;
                let object = value.as_object_mut().unwrap();
                let mut changes = if update.delta.is_some() {
                    std::collections::BTreeMap::new()
                } else {
                    update.settings.clone()
                };
                if let Some(delta) = &update.delta {
                    use pipecat_core::Setting;
                    changes.extend(delta.base.extra.clone());
                    match &delta.base.model {
                        Setting::NotGiven => {}
                        Setting::Null => {
                            changes.insert("model".into(), Value::Null);
                        }
                        Setting::Value(v) => {
                            changes.insert("model".into(), v.clone().into());
                        }
                    }
                    match &delta.language {
                        Setting::NotGiven => {}
                        Setting::Null => {
                            changes.insert("language".into(), Value::Null);
                        }
                        Setting::Value(v) => {
                            changes.insert("language".into(), v.clone().into());
                        }
                    }
                }
                for (key, value) in changes {
                    object.insert(key, value);
                }
                let settings = serde_json::from_value(value).map_err(|_| {
                    AssemblyAiSyncError::Invalid("invalid settings update").processor_error()
                })?;
                self.update_settings(settings)
                    .map_err(AssemblyAiSyncError::processor_error)?;
                return Ok(());
            }
            _ => {}
        }
        if let FrameData::LlmContextAssistantTurn(turn) = &frame.data {
            self.assistant_turn(&turn.text)
                .map_err(AssemblyAiSyncError::processor_error)?;
        }
        match frame.data {
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self
                .cancel()
                .map_err(AssemblyAiSyncError::processor_error)?,
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop => self
                .finish_graceful()
                .await
                .map_err(AssemblyAiSyncError::processor_error)?,
            _ => {}
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.finish_graceful()
            .await
            .map_err(AssemblyAiSyncError::processor_error)
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cancel().map_err(AssemblyAiSyncError::processor_error)
    }
}
