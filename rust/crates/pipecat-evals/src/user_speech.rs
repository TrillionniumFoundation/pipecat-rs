//! Explicit Cartesia user speech and its cancellation-safe per-session owner.
use crate::{
    recorded_audio::{Playback, MAX_TOTAL_BYTES},
    recorded_scenario::read_regular,
    user_speech_cache::{CacheError, CacheKey, CacheLookup, CachePersistence, CacheTransaction},
    Cancellation, EvalError, RecordedAudio, UserSpeechCachePolicy,
};
use pipecat_speech::{
    cartesia::{CartesiaHttpClient, CartesiaSynthesisRequest, CARTESIA_API_VERSION},
    SpeechConfig, SpeechError, SpeechStream,
};
use serde::Deserialize;
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::oneshot, time::Instant};

pub(crate) const MAX_UTTERANCE_BYTES: usize = 1_920_000;
pub(crate) const MAX_TRANSCRIPT_BYTES: usize = 16 * 1024;
const PUBLICATION_BUDGET: Duration = Duration::from_secs(5);
const PLAYBACK_BUDGET: Duration = Duration::from_secs(61);

/// Explicit existing-provider configuration. Construction performs no request,
/// cache probe or credential discovery. Debug never exposes the destination,
/// credential or cache path. The selected cache root is a private trust boundary.
#[derive(Clone)]
pub struct CartesiaUserSpeechSettings {
    config: SpeechConfig,
    client: CartesiaHttpClient,
    cache: UserSpeechCachePolicy,
}
impl fmt::Debug for CartesiaUserSpeechSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CartesiaUserSpeechSettings")
            .field("request_timeout", &self.config.timeout)
            .field("max_audio_bytes", &self.config.max_audio_bytes)
            .finish_non_exhaustive()
    }
}
impl CartesiaUserSpeechSettings {
    /// Restrict output before constructing the HTTP client. Stricter caller
    /// limits are preserved; this entry always uses direct, no-retry transport.
    pub fn new(mut config: SpeechConfig, cache: UserSpeechCachePolicy) -> Result<Self, EvalError> {
        validate_endpoint(&config.base_url)?;
        // Equivalent supported root spellings share the effective cache identity.
        config.base_url = config.base_url.trim_end_matches('/').to_owned();
        if config.timeout.is_zero()
            || config.timeout > Duration::from_secs(60)
            || config.max_audio_bytes == 0
            || config.queue_capacity == 0
            || config.max_json_bytes == 0
            || config.api_key.is_empty()
            || config.api_key.len() > 4096
            || config.api_key.chars().any(char::is_control)
        {
            return Err(invalid(
                "user speech runtime limits or credential are invalid",
            ));
        }
        cache
            .validate()
            .map_err(|_| invalid("user speech cache policy is invalid"))?;
        config.max_audio_bytes = config.max_audio_bytes.min(MAX_UTTERANCE_BYTES);
        config.queue_capacity = config.queue_capacity.min(2);
        config.max_json_bytes = config.max_json_bytes.min(64 * 1024);
        let client = CartesiaHttpClient::new_direct(config.clone())
            .map_err(|_| invalid("user speech endpoint or credential is invalid"))?;
        Ok(Self {
            config,
            client,
            cache,
        })
    }

    /// Load at most 32 KiB of strict JSON. Only the explicitly named existing
    /// environment credential is read; it is never persisted or logged.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        let path = path
            .as_ref()
            .canonicalize()
            .map_err(|_| invalid("user speech runtime file cannot be opened"))?;
        let root = path
            .parent()
            .ok_or_else(|| invalid("user speech runtime directory"))?;
        let bytes = read_regular(&path, root, 32 * 1024)
            .map_err(|_| invalid("user speech runtime file must be a bounded regular file"))?;
        let raw: RuntimeFile = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("user speech runtime JSON fields or types"))?;
        if raw.provider != "cartesia"
            || raw.request_timeout_ms == 0
            || raw.request_timeout_ms > 60_000
            || raw.api_key_env.is_empty()
            || raw.api_key_env.len() > 128
            || !raw
                .api_key_env
                .bytes()
                .enumerate()
                .all(|(i, c)| c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()))
        {
            return Err(invalid(
                "user speech runtime provider, timeout or credential variable",
            ));
        }
        validate_endpoint(&raw.base_url)?;
        let cache = match raw.cache_dir {
            Some(directory) => UserSpeechCachePolicy::Private {
                directory,
                max_entries: raw.cache_max_entries.unwrap_or(256),
                max_bytes: raw.cache_max_bytes.unwrap_or(64 * 1024 * 1024),
            },
            None if raw.cache_max_entries.is_some() || raw.cache_max_bytes.is_some() => {
                return Err(invalid(
                    "user speech cache quotas require an explicit cache directory",
                ));
            }
            None => UserSpeechCachePolicy::Disabled,
        };
        let api_key = std::env::var(&raw.api_key_env).map_err(|_| {
            invalid("user speech explicit credential variable is missing or not Unicode")
        })?;
        Self::new(
            SpeechConfig {
                base_url: raw.base_url,
                api_key,
                timeout: Duration::from_millis(raw.request_timeout_ms),
                max_audio_bytes: MAX_UTTERANCE_BYTES,
                max_json_bytes: 64 * 1024,
                queue_capacity: 1,
            },
            cache,
        )
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeFile {
    provider: String,
    base_url: String,
    api_key_env: String,
    #[serde(default = "default_timeout_ms")]
    request_timeout_ms: u64,
    cache_dir: Option<PathBuf>,
    cache_max_entries: Option<usize>,
    cache_max_bytes: Option<u64>,
}
fn default_timeout_ms() -> u64 {
    30_000
}

fn validate_endpoint(value: &str) -> Result<(), EvalError> {
    if value == "https://api.cartesia.ai" || value == "https://api.cartesia.ai/" {
        return Ok(());
    }
    if value.len() > 4096 || value.chars().any(char::is_control) {
        return Err(invalid(
            "user speech requires the canonical Cartesia origin or literal-loopback fixture",
        ));
    }
    let url = url::Url::parse(value).map_err(|_| invalid("user speech endpoint is invalid"))?;
    let is_literal_loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    if url.scheme() != "http"
        || !is_literal_loopback
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || !(value == url.as_str() || format!("{value}/") == url.as_str())
    {
        return Err(invalid(
            "user speech requires the canonical Cartesia origin or literal-loopback fixture",
        ));
    }
    Ok(())
}

/// Validated immutable source-effective voice settings. Regional language codes
/// use the exact pinned Language enum and resolve to Cartesia's base code.
#[derive(Clone)]
pub struct CartesiaUserSpeechVoice {
    voice: String,
    model: String,
    language: String,
}
impl fmt::Debug for CartesiaUserSpeechVoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CartesiaUserSpeechVoice")
            .finish_non_exhaustive()
    }
}
impl CartesiaUserSpeechVoice {
    pub fn new(
        voice: impl Into<String>,
        model: Option<String>,
        language: Option<String>,
    ) -> Result<Self, EvalError> {
        let voice = voice.into();
        validate_voice_field(&voice, 256, "user speech voice is invalid")?;
        let model = model
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "sonic-2".into());
        validate_voice_field(&model, 256, "user speech model is invalid")?;
        let language = language.as_deref().unwrap_or("en");
        if language.len() > 64 || language.chars().any(char::is_control) {
            return Err(invalid("user speech language is invalid"));
        }
        let language = language.trim();
        let language = if language.is_empty() { "en" } else { language };
        if !SOURCE_LANGUAGE_CODES.contains(&language) {
            return Err(invalid(
                "user speech language is not a pinned Language value",
            ));
        }
        Ok(Self {
            voice,
            model,
            language: language.split('-').next().unwrap_or("en").to_lowercase(),
        })
    }
}
fn validate_voice_field(value: &str, max: usize, message: &'static str) -> Result<(), EvalError> {
    if value.trim().is_empty()
        || value.len() > max
        || value.chars().count() > max
        || value.chars().any(char::is_control)
    {
        return Err(invalid(message));
    }
    Ok(())
}

/// Logical played bytes, charged once for every use, including repeated hits.
#[derive(Clone)]
pub(crate) struct UserSpeechBudget(Arc<AtomicUsize>);
impl UserSpeechBudget {
    pub(crate) fn new(recorded_bytes: usize) -> Result<Self, EvalError> {
        if recorded_bytes > MAX_TOTAL_BYTES {
            return Err(EvalError::Limit("user speech invocation PCM bytes"));
        }
        Ok(Self(Arc::new(AtomicUsize::new(recorded_bytes))))
    }
    fn charge(&self, bytes: usize) -> Result<(), Failure> {
        let mut current = self.0.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(bytes)
                .filter(|next| *next <= MAX_TOTAL_BYTES)
                .ok_or_else(|| {
                    Failure::new(
                        "limit_exceeded",
                        "user speech aggregate PCM budget exceeded",
                    )
                })?;
            match self
                .0
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Clone, Copy)]
struct Failure {
    kind: &'static str,
    reason: &'static str,
}
impl Failure {
    fn new(kind: &'static str, reason: &'static str) -> Self {
        Self { kind, reason }
    }
    fn provider(error: SpeechError) -> Self {
        if matches!(error, SpeechError::Timeout) {
            Self::new("user_speech_timeout", "user speech production timed out")
        } else {
            Self::new(
                "user_speech_error",
                "user speech provider or PCM production failed",
            )
        }
    }
    fn cache(error: CacheError) -> Self {
        if error.is_timeout() {
            Self::new(
                "user_speech_cache_timeout",
                "user speech cache transaction timed out",
            )
        } else {
            Self::new(
                "user_speech_cache_error",
                "user speech cache transaction failed",
            )
        }
    }
    fn error(self) -> EvalError {
        invalid(self.reason)
    }
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Lookup,
    Synthesizing,
    Ready,
    Delivering,
    Settled,
}
struct Production {
    token: u64,
    stage: Stage,
    text: String,
    production_deadline: Instant,
    stream: Option<SpeechStream>,
    cache: Option<CacheTransaction>,
    pcm: Vec<u8>,
    ready: Option<RecordedAudio>,
    playback: Option<oneshot::Receiver<()>>,
    playback_deadline: Option<Instant>,
    cache_deadline: Option<Instant>,
    cache_miss: bool,
}

/// No coordinator task: HTTP belongs to SpeechStream, cache owns one native
/// thread only when selected, and all progress/results live in this owner.
pub(crate) struct UserSpeechOwner {
    settings: CartesiaUserSpeechSettings,
    voice: CartesiaUserSpeechVoice,
    budget: UserSpeechBudget,
    recorded_precharged: bool,
    cancel: Cancellation,
    run_deadline: Option<Instant>,
    token: u64,
    current: Option<Production>,
    failure: Option<Failure>,
    cache_diagnostics: Vec<String>,
    #[cfg(test)]
    progress_gate: Option<ProgressGate>,
}
#[cfg(test)]
struct ProgressGate {
    entered: Option<oneshot::Sender<()>>,
    release: Arc<tokio::sync::Notify>,
}
impl UserSpeechOwner {
    pub(crate) fn new(
        settings: CartesiaUserSpeechSettings,
        voice: CartesiaUserSpeechVoice,
    ) -> Self {
        Self {
            settings,
            voice,
            budget: UserSpeechBudget(Arc::new(AtomicUsize::new(0))),
            recorded_precharged: false,
            cancel: Cancellation::new(),
            run_deadline: None,
            token: 0,
            current: None,
            failure: None,
            cache_diagnostics: Vec::new(),
            #[cfg(test)]
            progress_gate: None,
        }
    }
    pub(crate) fn with_budget(&mut self, budget: UserSpeechBudget) {
        self.budget = budget;
        self.recorded_precharged = true;
    }
    pub(crate) fn configure_run(
        &mut self,
        cancel: Cancellation,
        deadline: Instant,
        recorded_bytes: usize,
    ) -> Result<(), EvalError> {
        self.cancel = cancel;
        self.run_deadline = Some(deadline);
        if !self.recorded_precharged {
            self.budget = UserSpeechBudget::new(recorded_bytes)?;
            self.recorded_precharged = true;
        }
        Ok(())
    }
    pub(crate) fn validate_text(text: &str) -> Result<(), EvalError> {
        if text.trim().is_empty() || text.len() > MAX_TRANSCRIPT_BYTES {
            return Err(invalid(
                "user speech transcript must be nonblank and at most 16384 UTF-8 bytes",
            ));
        }
        Ok(())
    }
    pub(crate) fn begin(&mut self, text: String, admitted: Instant) -> Result<(), EvalError> {
        Self::validate_text(&text)?;
        self.check_deadline()?;
        if !self.settled() {
            return Err(self.fail(Failure::new(
                "user_speech_error",
                "prior user speech transaction is not settled",
            )));
        }
        if self.cancel.is_cancelled() {
            return Err(EvalError::Cancelled);
        }
        self.token = self
            .token
            .checked_add(1)
            .ok_or(EvalError::Limit("user speech turn token"))?;
        let deadline = (admitted + self.settings.config.timeout).min(
            self.run_deadline
                .ok_or_else(|| invalid("user speech run is not initialized"))?,
        );
        let key = CacheKey::new(
            &self.settings.config.base_url,
            CARTESIA_API_VERSION,
            &self.voice.model,
            &self.voice.voice,
            &self.voice.language,
            &text,
        );
        // Disabled is handled here, so it cannot create a worker or probe a path.
        let cache = if matches!(self.settings.cache, UserSpeechCachePolicy::Disabled) {
            None
        } else {
            match CacheTransaction::start(&self.settings.cache, key, deadline) {
                Ok(cache) => cache,
                Err(error) => return Err(self.fail(Failure::cache(error))),
            }
        };
        let stage = if cache.is_some() {
            Stage::Lookup
        } else {
            Stage::Synthesizing
        };
        self.current = Some(Production {
            token: self.token,
            stage,
            text,
            production_deadline: deadline,
            stream: None,
            cache,
            pcm: Vec::new(),
            ready: None,
            playback: None,
            playback_deadline: None,
            cache_deadline: None,
            cache_miss: false,
        });
        // This is the only HTTP admission point on the no-cache path.
        if stage == Stage::Synthesizing {
            self.start_http()?;
        }
        Ok(())
    }
    fn start_http(&mut self) -> Result<(), EvalError> {
        self.check_deadline()?;
        let current = self.current.as_mut().expect("active production");
        let mut request = CartesiaSynthesisRequest::new(
            std::mem::take(&mut current.text),
            self.voice.voice.clone(),
        );
        request.model = self.voice.model.clone();
        request.language = Some(self.voice.language.clone());
        request.sample_rate = 16_000;
        match self.settings.client.synthesize(request) {
            Ok(stream) => {
                current.stream = Some(stream);
                current.stage = Stage::Synthesizing;
                Ok(())
            }
            Err(error) => Err(self.fail(Failure::provider(error))),
        }
    }
    pub(crate) fn settled(&self) -> bool {
        self.current
            .as_ref()
            .is_none_or(|current| current.stage == Stage::Settled)
    }
    pub(crate) fn playback_failed(&mut self) -> EvalError {
        self.fail(Failure::new(
            "user_speech_error",
            "user speech playback transport failed",
        ))
    }
    pub(crate) fn failure_kind(&self) -> Option<&'static str> {
        self.failure.map(|f| f.kind)
    }
    fn fail(&mut self, failure: Failure) -> EvalError {
        self.failure.get_or_insert(failure);
        self.cancel_work();
        failure.error()
    }
    pub(crate) fn deadline(&self) -> Option<Instant> {
        let current = self.current.as_ref()?;
        match current.stage {
            Stage::Lookup | Stage::Synthesizing | Stage::Ready => Some(current.production_deadline),
            Stage::Delivering => current
                .playback_deadline
                .into_iter()
                .chain(current.cache_deadline)
                .min(),
            Stage::Settled => None,
        }
    }
    pub(crate) fn check_deadline(&mut self) -> Result<(), EvalError> {
        if self.cancel.is_cancelled() {
            return Err(EvalError::Cancelled);
        }
        if let Some(failure) = self.failure {
            return Err(failure.error());
        }
        let Some(current) = &self.current else {
            return Ok(());
        };
        if current.token != self.token {
            return Err(self.fail(Failure::new(
                "user_speech_error",
                "user speech admission was revoked",
            )));
        }
        let now = Instant::now();
        let failure = match current.stage {
            Stage::Lookup if now >= current.production_deadline => Some(Failure::new(
                "user_speech_cache_timeout",
                "user speech cache lookup timed out",
            )),
            Stage::Synthesizing | Stage::Ready if now >= current.production_deadline => Some(
                Failure::new("user_speech_timeout", "user speech production timed out"),
            ),
            Stage::Delivering
                if current
                    .cache_deadline
                    .is_some_and(|deadline| now >= deadline) =>
            {
                Some(Failure::new(
                    "user_speech_cache_timeout",
                    "user speech cache settlement timed out",
                ))
            }
            Stage::Delivering
                if current
                    .playback_deadline
                    .is_some_and(|deadline| now >= deadline) =>
            {
                Some(Failure::new(
                    "user_speech_timeout",
                    "user speech local playback timed out",
                ))
            }
            _ => None,
        };
        if let Some(failure) = failure {
            return Err(self.fail(failure));
        }
        Ok(())
    }
    /// Every popped chunk, cache result and completion is put in owner state in
    /// the same poll, before any subsequent await. Losing a competing pump poll
    /// cannot detach a worker, lose PCM, or queue an utterance twice.
    pub(crate) async fn progress(&mut self, playback: Option<&Playback>) -> Result<(), EvalError> {
        self.check_deadline()?;
        let Some(current) = self.current.as_ref() else {
            return std::future::pending().await;
        };
        match current.stage {
            Stage::Lookup => {
                let outcome = self
                    .current
                    .as_mut()
                    .unwrap()
                    .cache
                    .as_mut()
                    .unwrap()
                    .lookup()
                    .await;
                match outcome {
                    Ok(CacheLookup::Hit(clip)) => {
                        let current = self.current.as_mut().unwrap();
                        current.ready = Some(clip);
                        current.text.clear();
                        current.stage = Stage::Ready;
                    }
                    Ok(CacheLookup::Miss) => {
                        self.current.as_mut().unwrap().cache_miss = true;
                        self.start_http()?;
                    }
                    Err(error) => return Err(self.fail(Failure::cache(error))),
                }
            }
            Stage::Synthesizing => {
                let chunk = self
                    .current
                    .as_mut()
                    .unwrap()
                    .stream
                    .as_mut()
                    .unwrap()
                    .next()
                    .await;
                match chunk {
                    Some(Ok(chunk)) => {
                        let current = self.current.as_mut().unwrap();
                        if chunk.sample_rate != 16_000
                            || chunk.num_channels != 1
                            || chunk.pcm.len()
                                > self
                                    .settings
                                    .config
                                    .max_audio_bytes
                                    .saturating_sub(current.pcm.len())
                        {
                            return Err(self.fail(Failure::new(
                                "user_speech_error",
                                "user speech PCM format or size is invalid",
                            )));
                        }
                        current.pcm.extend_from_slice(&chunk.pcm);
                    }
                    Some(Err(error)) => return Err(self.fail(Failure::provider(error))),
                    None => {
                        let current = self.current.as_mut().unwrap();
                        let pcm = std::mem::take(&mut current.pcm);
                        let clip = RecordedAudio::from_pcm16(&pcm);
                        current.stream.take();
                        match clip {
                            Ok(clip) => {
                                current.ready = Some(clip);
                                current.stage = Stage::Ready;
                            }
                            Err(_) => {
                                return Err(self.fail(Failure::new(
                                    "user_speech_error",
                                    "user speech completed with invalid or empty PCM",
                                )))
                            }
                        }
                    }
                }
            }
            Stage::Ready => {
                // No await separates cancellation/deadline checks, charging,
                // queue admission, saving the receiver, or cache publication.
                self.check_deadline()?;
                let current = self.current.as_mut().unwrap();
                let clip = current.ready.as_ref().unwrap();
                if clip.pcm_bytes().len() > self.settings.config.max_audio_bytes {
                    return Err(self.fail(Failure::new(
                        "user_speech_error",
                        "user speech PCM exceeds configured limit",
                    )));
                }
                if let Err(failure) = self.budget.charge(clip.pcm_bytes().len()) {
                    return Err(self.fail(failure));
                }
                let Some(playback) = playback else {
                    return Err(self.fail(Failure::new(
                        "user_speech_error",
                        "user speech playback owner is unavailable",
                    )));
                };
                let completed = match playback.queue(clip.clone()) {
                    Ok(completed) => completed,
                    Err(_) => {
                        return Err(self.fail(Failure::new(
                            "user_speech_error",
                            "user speech playback admission failed",
                        )))
                    }
                };
                current.playback = Some(completed);
                let now = Instant::now();
                let run_deadline = self.run_deadline.unwrap();
                current.playback_deadline = Some((now + PLAYBACK_BUDGET).min(run_deadline));
                current.stage = Stage::Delivering;
                if let Some(cache) = &mut current.cache {
                    let deadline = (now + PUBLICATION_BUDGET).min(run_deadline);
                    current.cache_deadline = Some(deadline);
                    if current.cache_miss {
                        if let Err(error) = cache.commit(clip.clone(), deadline) {
                            return Err(self.fail(Failure::cache(error)));
                        }
                    }
                }
                current.ready.take();
            }
            Stage::Delivering => {
                let current = self.current.as_mut().unwrap();
                let completed = async {
                    match current.playback.as_mut() {
                        Some(rx) => rx.await,
                        None => std::future::pending().await,
                    }
                };
                let cache = async {
                    match current.cache.as_mut() {
                        Some(cache) => cache.join().await,
                        None => std::future::pending().await,
                    }
                };
                tokio::select! {
                    result = completed => {
                        let late = current.playback_deadline.is_some_and(|deadline| Instant::now() >= deadline);
                        current.playback.take(); current.playback_deadline = None;
                        if late { return Err(self.fail(Failure::new("user_speech_timeout", "user speech local playback timed out"))); }
                        if result.is_err() { return Err(self.fail(Failure::new("user_speech_error", "user speech ended before local playback completion"))); }
                    }
                    result = cache => {
                        let late = current.cache_deadline.is_some_and(|deadline| Instant::now() >= deadline);
                        current.cache_deadline = None;
                        // join has actually completed; only now release ownership.
                        if let Err(error) = result { return Err(self.fail(Failure::cache(error))); }
                        if let Some(cache) = &current.cache {
                            self.cache_diagnostics.push(cache_diagnostic(current.token, cache.persistence()));
                        }
                        current.cache.take();
                        if late { return Err(self.fail(Failure::new("user_speech_cache_timeout", "user speech cache settlement timed out"))); }
                    }
                }
                let current = self.current.as_mut().unwrap();
                if current.playback.is_none() && current.cache.is_none() {
                    current.stage = Stage::Settled;
                }
            }
            Stage::Settled => return std::future::pending().await,
        }
        #[cfg(test)]
        if let Some(gate) = &mut self.progress_gate {
            if let Some(entered) = gate.entered.take() {
                let _ = entered.send(());
            }
            gate.release.notified().await;
            self.progress_gate.take();
        }
        self.check_deadline()
    }
    /// Signal all work before awaiting any cleanup. Playback is stopped by its
    /// session owner immediately after this call, before joining native I/O.
    pub(crate) fn cancel_work(&mut self) {
        if let Some(current) = &mut self.current {
            if let Some(stream) = &current.stream {
                stream.cancel();
            }
            if let Some(cache) = &mut current.cache {
                cache.cancel();
            }
            current.ready.take();
            current.pcm.clear();
            current.text.clear();
        }
    }
    pub(crate) fn take_cache_diagnostics(&mut self) -> Vec<String> {
        std::mem::take(&mut self.cache_diagnostics)
    }
    pub(crate) async fn finish(&mut self) -> Result<(), EvalError> {
        self.cancel_work();
        let mut failed = false;
        if let Some(current) = &mut self.current {
            if let Some(stream) = &mut current.stream {
                if stream.cancel_and_join().await.is_err() {
                    failed = true;
                }
            }
            current.stream.take();
            if let Some(cache) = &mut current.cache {
                if let Err(error) = cache.join().await {
                    // Cancellation is an expected terminal transaction, and a
                    // previously reported operational failure is not a join failure.
                    if !matches!(error, CacheError::Cancelled)
                        && (matches!(error, CacheError::Worker) || self.failure.is_none())
                    {
                        failed = true;
                    }
                }
                self.cache_diagnostics
                    .push(cache_diagnostic(current.token, cache.persistence()));
            }
            current.cache.take();
        }
        self.current.take();
        if failed {
            Err(invalid("user speech owned cleanup failed"))
        } else {
            Ok(())
        }
    }
}
impl Drop for UserSpeechOwner {
    fn drop(&mut self) {
        self.cancel_work();
    }
}
fn cache_diagnostic(token: u64, state: CachePersistence) -> String {
    let state = match state {
        CachePersistence::NotWritten => "no new publication completed",
        CachePersistence::Durable => "durable publication confirmed",
        CachePersistence::Uncertain => "publication occurred; persistence is uncertain",
    };
    format!("user speech transaction {token} cache: {state}")
}
fn invalid(message: &str) -> EvalError {
    EvalError::Scenario(message.into())
}

// Fixed Python af9f9da9d7afddaa8811c0e0251b32566c980bdf,
// transcriptions/language.py blob 47a57eccc8d6fb6ce3c02bf6b061d74f9e909314.
// Acceptance is exact after whitespace trimming, not guessed arbitrary BCP-47.
const SOURCE_LANGUAGE_CODES: &[&str] = &[
    "af",
    "af-ZA",
    "am",
    "am-ET",
    "ar",
    "ar-AE",
    "ar-BH",
    "ar-DZ",
    "ar-EG",
    "ar-IQ",
    "ar-JO",
    "ar-KW",
    "ar-LB",
    "ar-LY",
    "ar-MA",
    "ar-OM",
    "ar-QA",
    "ar-SA",
    "ar-SY",
    "ar-TN",
    "ar-XA",
    "ar-YE",
    "ar-001",
    "as",
    "as-IN",
    "ast",
    "az",
    "az-AZ",
    "ba",
    "be",
    "be-BY",
    "bg",
    "bg-BG",
    "bn",
    "bn-BD",
    "bn-IN",
    "bo",
    "br",
    "brx",
    "brx-IN",
    "bs",
    "bs-BA",
    "ca",
    "ca-ES",
    "ceb",
    "ceb-PH",
    "ckb",
    "ckb-IQ",
    "ckb-IR",
    "cmn",
    "cmn-CN",
    "cs",
    "cs-CZ",
    "cy",
    "cy-GB",
    "da",
    "da-DK",
    "de",
    "de-AT",
    "de-BE",
    "de-CH",
    "de-DE",
    "doi",
    "doi-IN",
    "el",
    "el-GR",
    "en",
    "en-AB",
    "en-AU",
    "en-CA",
    "en-GB",
    "en-GH",
    "en-HK",
    "en-IE",
    "en-IN",
    "en-KE",
    "en-NG",
    "en-NZ",
    "en-PH",
    "en-SG",
    "en-TZ",
    "en-US",
    "en-WL",
    "en-ZA",
    "eo",
    "es",
    "es-AR",
    "es-BO",
    "es-CL",
    "es-CO",
    "es-CR",
    "es-CU",
    "es-DO",
    "es-EC",
    "es-ES",
    "es-GQ",
    "es-GT",
    "es-HN",
    "es-MX",
    "es-NI",
    "es-PA",
    "es-PE",
    "es-PR",
    "es-PY",
    "es-SV",
    "es-US",
    "es-UY",
    "es-VE",
    "es-419",
    "et",
    "et-EE",
    "eu",
    "eu-ES",
    "fa",
    "fa-AF",
    "fa-IR",
    "ff",
    "fi",
    "fi-FI",
    "fil",
    "fil-PH",
    "fo",
    "fr",
    "fr-BE",
    "fr-CA",
    "fr-CH",
    "fr-FR",
    "ga",
    "ga-IE",
    "gd",
    "gl",
    "gl-ES",
    "gu",
    "gu-IN",
    "ha",
    "haw",
    "he",
    "he-IL",
    "hi",
    "hi-IN",
    "hr",
    "hr-HR",
    "ht",
    "ht-HT",
    "hu",
    "hu-HU",
    "hy",
    "hy-AM",
    "id",
    "id-ID",
    "ig",
    "is",
    "is-IS",
    "it",
    "it-IT",
    "it-CH",
    "iu-Cans",
    "iu-Cans-CA",
    "iu-Latn",
    "iu-Latn-CA",
    "ja",
    "ja-JP",
    "jv",
    "jv-ID",
    "jv-JV",
    "jw",
    "ka",
    "ka-GE",
    "kea",
    "kab",
    "kab-DZ",
    "kk",
    "kk-KZ",
    "km",
    "km-KH",
    "kn",
    "kn-IN",
    "kok",
    "kok-IN",
    "ko",
    "ko-KR",
    "ks",
    "ks-IN",
    "ku",
    "ky",
    "ky-KG",
    "la",
    "la-VA",
    "lb",
    "lb-LU",
    "ln",
    "lo",
    "lo-LA",
    "lt",
    "lt-LT",
    "lg",
    "luo",
    "lv",
    "lv-LV",
    "mg",
    "mg-MG",
    "mi",
    "mk",
    "mk-MK",
    "mai",
    "mai-IN",
    "ml",
    "ml-IN",
    "mn",
    "mn-MN",
    "mni",
    "mni-IN",
    "mr",
    "mr-IN",
    "ms",
    "ms-MY",
    "mt",
    "mt-MT",
    "my",
    "my-MM",
    "mymr",
    "nb",
    "nb-NO",
    "no",
    "nn",
    "nn-NO",
    "ne",
    "ne-IN",
    "ne-NP",
    "nl",
    "nl-BE",
    "nl-NL",
    "nso",
    "ny",
    "oc",
    "or",
    "or-IN",
    "pa",
    "pa-IN",
    "pl",
    "pl-PL",
    "ps",
    "ps-AF",
    "pt",
    "pt-BR",
    "pt-PT",
    "ro",
    "ro-RO",
    "ru",
    "ru-RU",
    "sa",
    "sa-IN",
    "sat",
    "sat-IN",
    "sd",
    "sd-IN",
    "si",
    "si-LK",
    "sk",
    "sk-SK",
    "sl",
    "sl-SI",
    "sn",
    "so",
    "so-SO",
    "sq",
    "sq-AL",
    "sr",
    "sr-RS",
    "sr-Latn",
    "sr-Latn-RS",
    "su",
    "su-ID",
    "sv",
    "sv-SE",
    "sw",
    "sw-BI",
    "sw-KE",
    "sw-RW",
    "sw-TZ",
    "sw-UG",
    "ta",
    "ta-IN",
    "ta-LK",
    "ta-MY",
    "ta-SG",
    "te",
    "te-IN",
    "tg",
    "th",
    "th-TH",
    "tk",
    "tl",
    "tr",
    "tr-TR",
    "tt",
    "ug",
    "uk",
    "uk-UA",
    "umb",
    "ur",
    "ur-IN",
    "ur-PK",
    "uz",
    "uz-UZ",
    "vi",
    "vi-VN",
    "wo",
    "wuu",
    "wuu-CN",
    "yi",
    "yo",
    "yue",
    "yue-CN",
    "zh",
    "zh-CN",
    "zh-CN-guangxi",
    "zh-CN-henan",
    "zh-CN-liaoning",
    "zh-CN-shaanxi",
    "zh-CN-shandong",
    "zh-CN-sichuan",
    "zh-HK",
    "zh-TW",
    "xh-ZA",
    "zu",
    "zu-ZA",
];

#[cfg(test)]
mod tests {
    use super::*;

    fn settings() -> CartesiaUserSpeechSettings {
        let mut config = SpeechConfig::cartesia("synthetic-user-speech-unit-key");
        config.base_url = "http://127.0.0.1:1".into();
        CartesiaUserSpeechSettings::new(config, UserSpeechCachePolicy::Disabled).unwrap()
    }
    fn voice() -> CartesiaUserSpeechVoice {
        CartesiaUserSpeechVoice::new("test-voice", None, None).unwrap()
    }
    #[test]
    fn source_language_oracle_and_defaults() {
        let vectors: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/cartesia_user_speech_languages.json"
        ))
        .unwrap();
        let mut checked = 0;
        for case in vectors["cases"].as_array().unwrap() {
            let input = &case["input"];
            if !input.is_null() && !input.is_string() {
                continue;
            }
            let result =
                CartesiaUserSpeechVoice::new("voice", None, input.as_str().map(str::to_owned));
            if case["error"] == true {
                assert!(result.is_err(), "invalid pinned language was accepted");
            } else {
                assert_eq!(result.unwrap().language, case["output"].as_str().unwrap());
            }
            checked += 1;
        }
        assert_eq!(checked, 661);
        assert_eq!(voice().model, "sonic-2");
        assert_eq!(voice().language, "en");
        assert_eq!(
            CartesiaUserSpeechVoice::new("voice", Some(String::new()), None)
                .unwrap()
                .model,
            "sonic-2"
        );
    }
    #[test]
    fn settings_are_bounded_before_client_creation_and_redacted() {
        let mut config = SpeechConfig::cartesia("synthetic-secret-never-print");
        config.base_url = "http://127.0.0.1:1/".into();
        let effective =
            CartesiaUserSpeechSettings::new(config.clone(), UserSpeechCachePolicy::Disabled)
                .unwrap();
        assert_eq!(effective.config.max_audio_bytes, MAX_UTTERANCE_BYTES);
        assert_eq!(effective.config.queue_capacity, 2);
        assert_eq!(effective.config.base_url, "http://127.0.0.1:1");
        config.max_audio_bytes = 400;
        config.max_json_bytes = 128;
        config.queue_capacity = 1;
        let strict =
            CartesiaUserSpeechSettings::new(config, UserSpeechCachePolicy::Disabled).unwrap();
        assert_eq!(strict.config.max_audio_bytes, 400);
        assert_eq!(strict.config.max_json_bytes, 128);
        assert_eq!(strict.config.queue_capacity, 1);
        let debug = format!("{strict:?} {:?}", voice());
        for private in ["synthetic-secret", "127.0.0.1", "test-voice"] {
            assert!(!debug.contains(private));
        }
    }
    #[test]
    fn runtime_destination_is_exact_and_never_discovers_endpoints() {
        for endpoint in [
            "https://api.cartesia.ai",
            "https://api.cartesia.ai/",
            "http://127.0.0.1:9911",
            "http://[::1]:9911/",
        ] {
            assert!(validate_endpoint(endpoint).is_ok());
        }
        for endpoint in [
            "https://api.cartesia.ai.evil.test",
            "https://API.CARTESIA.AI",
            "https://api.cartesia.ai:443",
            "https://api.cartesia.ai/tts",
            "https://api.cartesia.ai?x=1",
            "https://key@api.cartesia.ai",
            "http://localhost:9911",
            "http://127.1:9911",
            "http://127.0.0.1:9911/a/..",
            "https://127.0.0.1:9911",
        ] {
            assert!(
                validate_endpoint(endpoint).is_err(),
                "unsupported destination was accepted"
            );
        }
    }
    #[test]
    fn bounded_fields_preserve_literal_text_and_reject_control_settings() {
        assert!(UserSpeechOwner::validate_text(&"x".repeat(MAX_TRANSCRIPT_BYTES)).is_ok());
        assert!(UserSpeechOwner::validate_text(&"x".repeat(MAX_TRANSCRIPT_BYTES + 1)).is_err());
        assert!(UserSpeechOwner::validate_text(" \n ").is_err());
        assert!(CartesiaUserSpeechVoice::new("voice\0", None, None).is_err());
        assert!(CartesiaUserSpeechVoice::new("voice", Some("model\n".into()), None).is_err());
        assert!(CartesiaUserSpeechVoice::new("voice", None, Some("en\0".into())).is_err());
        let configured = CartesiaUserSpeechVoice::new(
            " Voice Case ",
            Some("ModelCase".into()),
            Some(" zh-TW ".into()),
        )
        .unwrap();
        assert_eq!(configured.voice, " Voice Case ");
        assert_eq!(configured.model, "ModelCase");
        assert_eq!(configured.language, "zh");
    }
    #[test]
    fn runtime_json_rejects_duplicate_unknown_and_wrong_types() {
        for source in [
            r#"{"provider":"cartesia","base_url":"http://127.0.0.1:1","api_key_env":"TEST","provider":"cartesia"}"#,
            r#"{"provider":"cartesia","base_url":"http://127.0.0.1:1","api_key_env":"TEST","api_key":"forbidden"}"#,
            r#"{"provider":"cartesia","base_url":"http://127.0.0.1:1","api_key_env":"TEST","request_timeout_ms":"30000"}"#,
        ] {
            assert!(serde_json::from_str::<RuntimeFile>(source).is_err());
        }
    }
    #[test]
    fn shared_budget_charges_logical_uses_without_speculative_maxima() {
        let budget = UserSpeechBudget::new(MAX_TOTAL_BYTES - 4).unwrap();
        let second = budget.clone();
        assert!(budget.charge(2).is_ok());
        assert!(second.charge(2).is_ok());
        assert!(budget.charge(2).is_err());
        assert_eq!(second.0.load(Ordering::Acquire), MAX_TOTAL_BYTES);
        assert!(UserSpeechBudget::new(MAX_TOTAL_BYTES + 1).is_err());
    }
    #[tokio::test]
    async fn expired_producer_stage_is_not_an_event_timeout_and_cannot_admit_pcm() {
        let mut owner = UserSpeechOwner::new(settings(), voice());
        let now = Instant::now();
        owner
            .configure_run(Cancellation::new(), now + Duration::from_secs(10), 0)
            .unwrap();
        owner.current = Some(Production {
            token: 0,
            stage: Stage::Ready,
            text: String::new(),
            production_deadline: now - Duration::from_millis(1),
            stream: None,
            cache: None,
            pcm: Vec::new(),
            ready: Some(RecordedAudio::from_pcm16(&[1, 0]).unwrap()),
            playback: None,
            playback_deadline: None,
            cache_deadline: None,
            cache_miss: false,
        });
        let error = owner.progress(None).await.unwrap_err();
        assert!(!matches!(error, EvalError::Timeout(_)));
        assert_eq!(owner.failure_kind(), Some("user_speech_timeout"));
        assert!(owner.current.as_ref().unwrap().ready.is_none());
        assert_eq!(owner.budget.0.load(Ordering::Acquire), 0);
        owner.finish().await.unwrap();
    }
    #[tokio::test]
    async fn cancelled_before_begin_has_no_stream_or_cache_worker() {
        let mut owner = UserSpeechOwner::new(settings(), voice());
        let cancel = Cancellation::new();
        cancel.cancel();
        owner
            .configure_run(cancel, Instant::now() + Duration::from_secs(10), 0)
            .unwrap();
        assert!(matches!(
            owner.begin("unchanged transcript".into(), Instant::now()),
            Err(EvalError::Cancelled)
        ));
        assert!(owner.current.is_none());
        owner.finish().await.unwrap();
    }

    #[tokio::test]
    async fn discarded_progress_poll_keeps_popped_pcm_and_http_join_owner() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let fixture = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut chunk = [0u8; 1024];
                let count = socket.read(&mut chunk).await.unwrap();
                assert!(count > 0);
                request.extend_from_slice(&chunk[..count]);
                assert!(request.len() <= 32 * 1024);
                if let Some(end) = request.windows(4).position(|part| part == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            let (key, value) = line.split_once(':')?;
                            key.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse().unwrap())
                        })
                        .unwrap();
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: audio/pcm\r\nContent-Length: 4\r\nConnection: close\r\n\r\n\x01\x00\x02\x00").await.unwrap();
            socket.shutdown().await.unwrap();
        });
        let mut config = SpeechConfig::cartesia("synthetic-progress-key");
        config.base_url = endpoint;
        config.timeout = Duration::from_secs(3);
        let settings =
            CartesiaUserSpeechSettings::new(config, UserSpeechCachePolicy::Disabled).unwrap();
        let mut owner = UserSpeechOwner::new(settings, voice());
        owner
            .configure_run(
                Cancellation::new(),
                Instant::now() + Duration::from_secs(5),
                0,
            )
            .unwrap();
        owner
            .begin("Private literal text".into(), Instant::now())
            .unwrap();
        let (entered, mut popped) = oneshot::channel();
        owner.progress_gate = Some(ProgressGate {
            entered: Some(entered),
            release: Arc::new(tokio::sync::Notify::new()),
        });
        let mut first_poll = Box::pin(owner.progress(None));
        tokio::select! {
            result = &mut popped => result.unwrap(),
            result = &mut first_poll => panic!("progress gate returned unexpectedly: {result:?}"),
            _ = tokio::time::sleep(Duration::from_secs(3)) => panic!("HTTP fixture did not produce PCM"),
        }
        // Simulate the competing judge branch discarding this pending pump.
        drop(first_poll);
        assert_eq!(owner.current.as_ref().unwrap().pcm, [1, 0, 2, 0]);
        assert!(owner.current.as_ref().unwrap().stream.is_some());
        owner.progress_gate.take();
        owner.progress(None).await.unwrap();
        let current = owner.current.as_ref().unwrap();
        assert!(
            current.stream.is_none(),
            "successful EOF actually joined HTTP"
        );
        assert_eq!(current.ready.as_ref().unwrap().pcm_bytes(), [1, 0, 2, 0]);
        assert!(
            current.playback.is_none(),
            "unadmitted PCM must remain owner-held"
        );
        owner.finish().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), fixture)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn joined_cache_status_diagnostics_bridge_is_fixed_and_drained_once() {
        // Exercise only the result bridge: actual durable/uncertain filesystem
        // publication and cancellation are covered by cache transaction tests.
        let mut owner = UserSpeechOwner::new(settings(), voice());
        for (token, state) in [
            (1, CachePersistence::Durable),
            (2, CachePersistence::Uncertain),
            (3, CachePersistence::NotWritten),
        ] {
            owner.cache_diagnostics.push(cache_diagnostic(token, state));
        }
        owner.cancel_work();
        owner.finish().await.unwrap();
        owner.finish().await.unwrap();
        let messages = owner.take_cache_diagnostics();
        assert_eq!(
            messages,
            [
                "user speech transaction 1 cache: durable publication confirmed",
                "user speech transaction 2 cache: publication occurred; persistence is uncertain",
                "user speech transaction 3 cache: no new publication completed",
            ]
        );
        assert!(owner.take_cache_diagnostics().is_empty());
    }
}
