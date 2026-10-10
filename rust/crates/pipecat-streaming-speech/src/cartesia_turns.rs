//! Bounded Cartesia v2 committed-turn protocol. No reconnect, replay or eager inference.
use crate::cartesia_stt::{normalize_keyterms, quote, CartesiaKeyterms};
use crate::common::{self, EventStream, Input, Socket};
use crate::{CancellationToken, StreamingError, StreamingLimits};
use futures_util::StreamExt;
use pipecat_core::{AudioRawFrame, Setting, SttSettings};
use serde_json::Value;
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::tungstenite::Message;

pub const CARTESIA_TURNS_VERSION: &str = "2026-03-01";
type Admission = Arc<dyn Fn() -> bool + Send + Sync>;

#[derive(Clone, Debug, PartialEq)]
pub struct CartesiaTurnsSettings {
    /// Constructor-only model. Runtime changes are rejected, even with restart.
    pub model: String,
    pub keyterm: Vec<String>,
    pub turn_start_threshold: Option<f64>,
    pub turn_eager_end_threshold: Option<f64>,
    pub turn_end_threshold: Option<f64>,
    pub turn_end_timeout_ms: Option<u32>,
}
impl Default for CartesiaTurnsSettings {
    fn default() -> Self {
        Self {
            model: "ink-2".into(),
            keyterm: vec![],
            turn_start_threshold: None,
            turn_eager_end_threshold: None,
            turn_end_threshold: None,
            turn_end_timeout_ms: None,
        }
    }
}
impl CartesiaTurnsSettings {
    /// Unlike Live STT, Turns normalizes keyterms for every constructor model.
    pub fn prepared_keyterms(&self) -> CartesiaKeyterms {
        normalize_keyterms(&self.keyterm)
    }
    pub fn merge(&self, delta: &SttSettings) -> Result<Self, StreamingError> {
        if !matches!(delta.base.model, Setting::NotGiven)
            || !matches!(delta.language, Setting::NotGiven)
        {
            return Err(StreamingError::Invalid(
                "Turns runtime model/language is unsupported",
            ));
        }
        let mut next = self.clone();
        for (key, value) in &delta.base.extra {
            match key.as_str() {
                "keyterm" => {
                    next.keyterm = match value {
                        Value::Null => vec![],
                        Value::Array(values) => {
                            if values.len() > 4096
                                || values
                                    .iter()
                                    .filter_map(Value::as_str)
                                    .map(str::len)
                                    .try_fold(0usize, |a, b| a.checked_add(b))
                                    .is_none_or(|n| n > 65536)
                            {
                                return Err(StreamingError::TooLarge);
                            }
                            values
                                .iter()
                                .map(|v| {
                                    v.as_str()
                                        .map(str::to_owned)
                                        .ok_or(StreamingError::Invalid("Turns keyterm"))
                                })
                                .collect::<Result<_, _>>()?
                        }
                        _ => {
                            return Err(StreamingError::Invalid(
                                "Turns keyterm must be an array or null",
                            ))
                        }
                    }
                }
                "turn_start_threshold" => next.turn_start_threshold = optional_number(value)?,
                "turn_eager_end_threshold" => {
                    next.turn_eager_end_threshold = optional_number(value)?
                }
                "turn_end_threshold" => next.turn_end_threshold = optional_number(value)?,
                "turn_end_timeout_ms" => {
                    next.turn_end_timeout_ms = match value {
                        Value::Null => None,
                        _ => Some(
                            value
                                .as_u64()
                                .and_then(|n| u32::try_from(n).ok())
                                .ok_or(StreamingError::Invalid("Turns end timeout"))?,
                        ),
                    }
                }
                "model" | "language" => {
                    return Err(StreamingError::Invalid(
                        "Turns runtime model/language is unsupported",
                    ))
                }
                "enable_eager_end_of_turn" => {
                    return Err(StreamingError::Invalid(
                        "Turns eager integration is unsupported",
                    ))
                }
                _ => return Err(StreamingError::Invalid("unsupported Turns setting")),
            }
        }
        next.validate()?;
        Ok(next)
    }
    fn validate(&self) -> Result<(), StreamingError> {
        if self.model.is_empty() {
            return Err(StreamingError::Invalid("Turns model"));
        }
        if self.model.len() > 4096
            || self.keyterm.len() > 4096
            || self
                .keyterm
                .iter()
                .map(String::len)
                .try_fold(0usize, |a, b| a.checked_add(b))
                .is_none_or(|n| n > 65536)
        {
            return Err(StreamingError::TooLarge);
        }
        for (value, min, max) in [
            (self.turn_start_threshold, 0.5, 0.9),
            (self.turn_eager_end_threshold, 0.3, 0.6),
            (self.turn_end_threshold, 0.05, 0.5),
        ] {
            if value.is_some_and(|v| !v.is_finite() || !(min..=max).contains(&v)) {
                return Err(StreamingError::Invalid("Turns threshold range"));
            }
        }
        if self
            .turn_end_timeout_ms
            .is_some_and(|v| !(640..=11200).contains(&v))
        {
            return Err(StreamingError::Invalid("Turns end timeout range"));
        }
        Ok(())
    }
}
fn optional_number(value: &Value) -> Result<Option<f64>, StreamingError> {
    if value.is_null() {
        Ok(None)
    } else {
        value
            .as_f64()
            .map(Some)
            .ok_or(StreamingError::Invalid("Turns threshold number"))
    }
}

/// Finite watchdog work per socket, including cumulative successful silence.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CartesiaTurnsWatchdog {
    pub minimum_timeout: Duration,
    pub max_catch_up: Duration,
    pub max_silence: Duration,
}
impl Default for CartesiaTurnsWatchdog {
    fn default() -> Self {
        Self {
            minimum_timeout: Duration::from_millis(500),
            max_catch_up: Duration::from_secs(10),
            max_silence: Duration::from_secs(30),
        }
    }
}
#[derive(Clone, PartialEq)]
pub struct CartesiaTurnsConfig {
    pub endpoint: String,
    pub api_key: String,
    pub sample_rate: u32,
    pub settings: CartesiaTurnsSettings,
    pub watchdog: CartesiaTurnsWatchdog,
    /// connect_timeout covers upgrade AND connected acknowledgement. response_timeout
    /// is the complete close/drain deadline, including publication (default 2s).
    pub limits: StreamingLimits,
}
impl fmt::Debug for CartesiaTurnsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CartesiaTurnsConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .field("watchdog", &self.watchdog)
            .finish_non_exhaustive()
    }
}
impl CartesiaTurnsConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.cartesia.ai/stt/turns/websocket".into(),
            api_key: api_key.into(),
            sample_rate: 16000,
            settings: Default::default(),
            watchdog: Default::default(),
            limits: StreamingLimits {
                queue_capacity: 8,
                max_message_bytes: 32768,
                response_timeout: Duration::from_secs(2),
                ..Default::default()
            },
        }
    }
    fn url(&self) -> Result<String, StreamingError> {
        self.settings.validate()?;
        self.limits.validate()?;
        if self.api_key.is_empty() || !(8000..=192000).contains(&self.sample_rate) {
            return Err(StreamingError::Invalid("Turns key or PCM rate"));
        }
        if self.endpoint.len() > 8192 || self.api_key.len() > 8192 {
            return Err(StreamingError::TooLarge);
        }
        // Conservative JSON-tree/transcript expansion budget, both queues and
        // four transient payloads. This is admission accounting, not an allocator metric.
        let footprint = self
            .limits
            .queue_capacity
            .checked_mul(2)
            .and_then(|n| n.checked_add(4))
            .and_then(|n| n.checked_mul(self.limits.max_message_bytes))
            .and_then(|n| n.checked_mul(128));
        if self.limits.queue_capacity > 64
            || self.limits.max_message_bytes < 64
            || self.limits.max_message_bytes > 1024 * 1024
            || footprint.is_none_or(|n| n > 128 * 1024 * 1024)
            || [
                self.limits.connect_timeout,
                self.limits.write_timeout,
                self.limits.response_timeout,
                self.limits.close_timeout,
            ]
            .into_iter()
            .any(|v| v > Duration::from_secs(60))
        {
            return Err(StreamingError::Invalid(
                "Turns aggregate queue or time limits",
            ));
        }
        if self.watchdog.minimum_timeout.is_zero()
            || self.watchdog.minimum_timeout > Duration::from_secs(10)
            || self.watchdog.max_catch_up < self.watchdog.minimum_timeout
            || self.watchdog.max_catch_up > Duration::from_secs(30)
            || self.watchdog.max_silence < self.watchdog.max_catch_up
            || self.watchdog.max_silence > Duration::from_secs(300)
        {
            return Err(StreamingError::Invalid("Turns watchdog bounds"));
        }
        let mut pairs = vec![
            ("model", self.settings.model.clone()),
            ("encoding", "pcm_s16le".into()),
            ("sample_rate", self.sample_rate.to_string()),
        ];
        for (key, value) in [
            ("turn_start_threshold", self.settings.turn_start_threshold),
            (
                "turn_eager_end_threshold",
                self.settings.turn_eager_end_threshold,
            ),
            ("turn_end_threshold", self.settings.turn_end_threshold),
        ] {
            if let Some(value) = value {
                pairs.push((key, value.to_string()));
            }
        }
        if let Some(value) = self.settings.turn_end_timeout_ms {
            pairs.push(("turn_end_timeout_ms", value.to_string()));
        }
        pairs.extend(
            self.settings
                .prepared_keyterms()
                .terms
                .into_iter()
                .map(|s| ("keyterm", s)),
        );
        let query = pairs
            .into_iter()
            .map(|(k, v)| format!("{}={}", quote(k), quote(&v)))
            .collect::<Vec<_>>()
            .join("&");
        let mut url = common::endpoint(&self.endpoint)?;
        url.set_query(Some(&query));
        Ok(url.into())
    }
}

/// Raw provider events retain cumulative text. The processor only commits End.
/// EagerEnd and Resume are observable here but have no speculative pipeline effect.
#[derive(Clone, PartialEq)]
pub enum CartesiaTurnsEvent {
    Connected {
        request_id: Option<String>,
        result: Value,
    },
    Start {
        transcript: String,
        result: Value,
    },
    Update {
        transcript: String,
        result: Value,
    },
    EagerEnd {
        transcript: String,
        result: Value,
    },
    Resume {
        result: Value,
    },
    End {
        transcript: String,
        result: Value,
        audio_seconds: f64,
    },
    Usage {
        audio_seconds: f64,
    },
}
impl fmt::Debug for CartesiaTurnsEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Connected { .. } => "Connected { [redacted] }",
            Self::Start { .. } => "Start { [redacted] }",
            Self::Update { .. } => "Update { [redacted] }",
            Self::EagerEnd { .. } => "EagerEnd { [redacted] }",
            Self::Resume { .. } => "Resume { [redacted] }",
            Self::End { .. } => "End { [redacted] }",
            Self::Usage { .. } => "Usage { [redacted] }",
        })
    }
}
#[derive(Clone, Debug)]
pub struct CartesiaTurnsClient {
    config: CartesiaTurnsConfig,
}
impl CartesiaTurnsClient {
    pub fn new(config: CartesiaTurnsConfig) -> Result<Self, StreamingError> {
        common::request(
            &config.url()?,
            &[
                ("x-api-key", &config.api_key),
                ("cartesia-version", CARTESIA_TURNS_VERSION),
            ],
        )?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &CartesiaTurnsConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<CartesiaTurnsSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<CartesiaTurnsSession, StreamingError> {
        self.connect_guarded(cancel, Arc::new(|| true)).await
    }
    pub(crate) async fn connect_guarded(
        &self,
        cancel: CancellationToken,
        admission: Admission,
    ) -> Result<CartesiaTurnsSession, StreamingError> {
        admitted(&admission, &cancel)?;
        let request = common::request(
            &self.config.url()?,
            &[
                ("x-api-key", &self.config.api_key),
                ("cartesia-version", CARTESIA_TURNS_VERSION),
            ],
        )?;
        let deadline = Instant::now() + self.config.limits.connect_timeout;
        let mut socket =
            common::cancelled(&cancel, common::connect(request, &self.config.limits)).await?;
        let ready = common::cancelled(&cancel, async {
            tokio::time::timeout_at(
                deadline,
                ready(&mut socket, &self.config, &admission, &cancel),
            )
            .await
            .map_err(|_| StreamingError::Timeout("Turns connected acknowledgement"))?
        })
        .await;
        let (request_id, connected) = match ready {
            Ok(ready) => ready,
            Err(error) => {
                common::close(&mut socket, &self.config.limits).await;
                return Err(error);
            }
        };
        admitted(&admission, &cancel)?;
        let config = self.config.clone();
        let (tx, commands) = mpsc::channel(config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(config.limits.queue_capacity);
        let input = Input {
            tx,
            cancel: cancel.clone(),
            limits: config.limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        };
        let finished = input.finished.clone();
        let worker_cancel = cancel.clone();
        let worker_admission = admission.clone();
        let task = tokio::spawn(async move {
            let result = common::cancelled(
                &worker_cancel,
                run(
                    &mut socket,
                    commands,
                    &events,
                    &config,
                    &worker_admission,
                    &worker_cancel,
                    &finished,
                ),
            )
            .await;
            common::close(&mut socket, &config.limits).await;
            result
        });
        Ok(CartesiaTurnsSession {
            sender: CartesiaTurnsSender {
                input,
                sample_rate: self.config.sample_rate,
                admission,
            },
            request_id,
            connected,
            events: EventStream {
                rx,
                cancel,
                task,
                ended: false,
            },
        })
    }
}
fn admitted(admission: &Admission, cancel: &CancellationToken) -> Result<(), StreamingError> {
    if cancel.is_cancelled() || !admission() {
        Err(StreamingError::Cancelled)
    } else {
        Ok(())
    }
}
async fn ready(
    socket: &mut Socket,
    config: &CartesiaTurnsConfig,
    admission: &Admission,
    cancel: &CancellationToken,
) -> Result<(Option<String>, Value), StreamingError> {
    loop {
        admitted(admission, cancel)?;
        match socket.next().await {
            Some(Ok(Message::Text(text))) => match decode_text(&text)? {
                Some(CartesiaTurnsEvent::Connected { request_id, result }) => {
                    admitted(admission, cancel)?;
                    return Ok((request_id, result));
                }
                None => (),
                Some(_) => return Err(StreamingError::Protocol("Turns event before connected")),
            },
            Some(Ok(Message::Ping(bytes))) => {
                admitted(admission, cancel)?;
                common::write(socket, Message::Pong(bytes), &config.limits).await?;
            }
            Some(Ok(Message::Pong(_))) => (),
            Some(Ok(Message::Close(_))) | None => return Err(StreamingError::Closed),
            Some(Ok(_)) => return Err(StreamingError::Protocol("Turns sent non-text data")),
            Some(Err(error)) => return Err(common::transport(error)),
        }
    }
}
enum Command {
    Audio(Vec<u8>),
    Finish,
}
#[derive(Clone)]
pub struct CartesiaTurnsSender {
    input: Input<Command>,
    sample_rate: u32,
    admission: Admission,
}
impl CartesiaTurnsSender {
    fn validate(&self, audio: &AudioRawFrame) -> Result<(), StreamingError> {
        admitted(&self.admission, &self.input.cancel)?;
        audio
            .validate()
            .map_err(|_| StreamingError::Invalid("complete PCM16 samples required"))?;
        if audio.sample_rate != self.sample_rate || audio.num_channels != 1 {
            return Err(StreamingError::Invalid("Turns PCM format"));
        }
        if audio.audio.len() > self.input.limits.max_message_bytes
            || audio.audio.capacity() > self.input.limits.max_message_bytes
        {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    pub fn is_closed(&self) -> bool {
        !self.admission.as_ref()()
            || self.input.cancel.is_cancelled()
            || self.input.tx.is_closed()
            || self.input.finished.lock().map_or(true, |v| *v)
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate(&audio)?;
        self.input.send(Command::Audio(audio.audio), false).await
    }
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate(&audio)?;
        self.input.try_send(Command::Audio(audio.audio), false)
    }
    /// Queue exactly one JSON close behind admitted PCM. Drain with next() until None.
    pub async fn finish(&self) -> Result<(), StreamingError> {
        admitted(&self.admission, &self.input.cancel)?;
        self.input.send(Command::Finish, true).await
    }
    pub fn try_finish(&self) -> Result<(), StreamingError> {
        admitted(&self.admission, &self.input.cancel)?;
        self.input.try_send(Command::Finish, true)
    }
    pub fn cancel(&self) {
        self.input.cancel.cancel();
    }
}
pub struct CartesiaTurnsSession {
    sender: CartesiaTurnsSender,
    request_id: Option<String>,
    connected: Value,
    events: EventStream<CartesiaTurnsEvent>,
}
impl CartesiaTurnsSession {
    pub fn sender(&self) -> CartesiaTurnsSender {
        self.sender.clone()
    }
    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_deref()
    }
    pub fn connected_result(&self) -> &Value {
        &self.connected
    }
    pub async fn next(&mut self) -> Option<Result<CartesiaTurnsEvent, StreamingError>> {
        if !self.sender.admission.as_ref()() {
            self.cancel();
        }
        let event = self.events.next().await;
        if !self.sender.admission.as_ref()() {
            self.cancel();
            return event.map(|_| Err(StreamingError::Cancelled));
        }
        event
    }
    pub fn cancel(&self) {
        self.events.cancel.cancel();
    }
}
async fn emit(
    events: &mpsc::Sender<CartesiaTurnsEvent>,
    event: CartesiaTurnsEvent,
    closing: Option<Instant>,
    config: &CartesiaTurnsConfig,
    admission: &Admission,
    cancel: &CancellationToken,
) -> Result<(), StreamingError> {
    admitted(admission, cancel)?;
    let deadline = closing.unwrap_or_else(|| Instant::now() + config.limits.write_timeout);
    if Instant::now() >= deadline {
        return Err(StreamingError::Timeout("Turns output queue"));
    }
    let permit = tokio::time::timeout_at(deadline, events.reserve())
        .await
        .map_err(|_| StreamingError::Timeout("Turns output queue"))?
        .map_err(|_| StreamingError::Closed)?;
    admitted(admission, cancel)?;
    if Instant::now() >= deadline {
        return Err(StreamingError::Timeout("Turns output queue"));
    }
    permit.send(event);
    Ok(())
}
#[derive(Default)]
struct WatchdogState {
    last_send: Option<Instant>,
    real_duration: Duration,
    silence_samples: u64,
}
impl WatchdogState {
    fn next_check(&self, speaking: bool, config: &CartesiaTurnsConfig) -> Option<Instant> {
        if !speaking {
            return None;
        }
        self.last_send.map(|last| {
            last + self
                .real_duration
                .saturating_mul(2)
                .max(config.watchdog.minimum_timeout)
                + Duration::from_millis(100)
        })
    }
    fn catch_up_samples(
        &self,
        elapsed: Duration,
        config: &CartesiaTurnsConfig,
    ) -> Result<u64, StreamingError> {
        if elapsed > config.watchdog.max_catch_up {
            return Err(StreamingError::Timeout("Turns watchdog catch-up budget"));
        }
        let samples = elapsed
            .as_nanos()
            .checked_mul(u128::from(config.sample_rate))
            .and_then(|n| u64::try_from(n / 1_000_000_000).ok())
            .ok_or(StreamingError::TooLarge)?;
        let budget =
            config.watchdog.max_silence.as_nanos() * u128::from(config.sample_rate) / 1_000_000_000;
        if u128::from(self.silence_samples) + u128::from(samples) > budget {
            return Err(StreamingError::Timeout("Turns watchdog silence budget"));
        }
        Ok(samples)
    }
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<CartesiaTurnsEvent>,
    config: &CartesiaTurnsConfig,
    admission: &Admission,
    cancel: &CancellationToken,
    finished: &Mutex<bool>,
) -> Result<(), StreamingError> {
    let mut closing: Option<Instant> = None;
    let mut speaking = false;
    let mut pending_samples = 0u64;
    let mut watchdog = WatchdogState::default();
    loop {
        admitted(admission, cancel)?;
        if closing.is_some_and(|deadline| Instant::now() >= deadline) {
            return Err(StreamingError::Timeout("Turns close drain"));
        }
        let terminal_admitted = *finished.lock().map_err(|_| StreamingError::Closed)?;
        let watchdog_deadline =
            watchdog.next_check(speaking && closing.is_none() && !terminal_admitted, config);
        let drain_deadline = async {
            match closing {
                Some(t) => tokio::time::sleep_until(t).await,
                None => std::future::pending().await,
            }
        };
        let watchdog_wait = async {
            match watchdog_deadline {
                Some(t) => tokio::time::sleep_until(t).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = events.closed() => return Err(StreamingError::Closed),
            _ = drain_deadline => return Err(StreamingError::Timeout("Turns close drain")),
            command = commands.recv(), if closing.is_none() => match command {
                Some(Command::Audio(audio)) => {
                    admitted(admission, cancel)?;
                    if audio.is_empty() { continue; }
                    let samples = u64::try_from(audio.len() / 2).map_err(|_| StreamingError::TooLarge)?;
                    common::write(socket, Message::Binary(audio.into()), &config.limits).await?;
                    admitted(admission, cancel)?;
                    pending_samples = pending_samples.checked_add(samples).ok_or(StreamingError::TooLarge)?;
                    watchdog.last_send = Some(Instant::now());
                    watchdog.real_duration = Duration::from_secs_f64(samples as f64 / f64::from(config.sample_rate));
                },
                Some(Command::Finish) | None => {
                    speaking = false;
                    let deadline = Instant::now() + config.limits.response_timeout;
                    closing = Some(deadline);
                    admitted(admission, cancel)?;
                    tokio::time::timeout_at(deadline, common::write(socket, Message::Text("{\"type\":\"close\"}".into()), &config.limits)).await
                        .map_err(|_| StreamingError::Timeout("Turns close drain"))??;
                    commands.close();
                },
            },
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    admitted(admission, cancel)?;
                    if let Some(mut event) = decode_text(&text)? {
                        match &mut event {
                            CartesiaTurnsEvent::Connected { .. } => return Err(StreamingError::Protocol("duplicate Turns connected")),
                            CartesiaTurnsEvent::Start { .. } => { if closing.is_none() && !terminal_admitted { speaking = true; } else { continue; } },
                            CartesiaTurnsEvent::End { transcript, audio_seconds, .. } => {
                                speaking = false;
                                if !transcript.is_empty() { *audio_seconds = pending_samples as f64 / f64::from(config.sample_rate); }
                            },
                            _ if closing.is_some() || terminal_admitted => continue,
                            _ => (),
                        }
                        let accounts_usage = matches!(&event, CartesiaTurnsEvent::End { transcript, .. } if !transcript.is_empty());
                        emit(events, event, closing, config, admission, cancel).await?;
                        if accounts_usage { pending_samples = 0; }
                    }
                },
                Some(Ok(Message::Close(_))) | None => {
                    if closing.is_none() { return Err(StreamingError::Closed); }
                    if pending_samples > 0 { emit(events, CartesiaTurnsEvent::Usage { audio_seconds: pending_samples as f64 / f64::from(config.sample_rate) }, closing, config, admission, cancel).await?; }
                    return Ok(());
                },
                Some(Ok(Message::Ping(bytes))) => {
                    admitted(admission, cancel)?;
                    let deadline = closing.unwrap_or_else(|| Instant::now() + config.limits.write_timeout);
                    tokio::time::timeout_at(deadline, common::write(socket, Message::Pong(bytes), &config.limits)).await
                        .map_err(|_| StreamingError::Timeout("Turns pong write"))??;
                },
                Some(Ok(Message::Pong(_))) => (),
                Some(Ok(_)) => return Err(StreamingError::Protocol("Turns sent non-text data")),
                Some(Err(error)) => return Err(common::transport(error)),
            },
            _ = watchdog_wait => {
                let elapsed = watchdog.last_send.expect("armed watchdog").elapsed();
                let samples = watchdog.catch_up_samples(elapsed, config)?;
                let mut remaining = samples;
                let catch_up_deadline = Instant::now() + config.limits.write_timeout;
                let frame_samples = u64::try_from(config.limits.max_message_bytes / 2).map_err(|_| StreamingError::TooLarge)?;
                if samples.div_ceil(frame_samples) > 64 { return Err(StreamingError::TooLarge); }
                while remaining > 0 {
                    admitted(admission, cancel)?;
                    if *finished.lock().map_err(|_| StreamingError::Closed)? { break; }
                    let count = remaining.min(frame_samples);
                    let bytes = usize::try_from(count).ok().and_then(|n| n.checked_mul(2)).ok_or(StreamingError::TooLarge)?;
                    tokio::time::timeout_at(catch_up_deadline, common::write(socket, Message::Binary(vec![0; bytes].into()), &config.limits)).await
                        .map_err(|_| StreamingError::Timeout("Turns watchdog write budget"))??;
                    admitted(admission, cancel)?;
                    watchdog.silence_samples = watchdog.silence_samples.checked_add(count).ok_or(StreamingError::TooLarge)?;
                    pending_samples = pending_samples.checked_add(count).ok_or(StreamingError::TooLarge)?;
                    remaining -= count;
                }
                watchdog.last_send = Some(Instant::now());
            },
        }
    }
}
fn decode_text(text: &str) -> Result<Option<CartesiaTurnsEvent>, StreamingError> {
    let value: Value =
        serde_json::from_str(text).map_err(|_| StreamingError::Protocol("Turns JSON"))?;
    decode(value)
}
fn decode(value: Value) -> Result<Option<CartesiaTurnsEvent>, StreamingError> {
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or(StreamingError::Protocol("Turns message type"))?;
    if kind == "error" {
        return Err(StreamingError::Provider { status: None });
    }
    if kind == "connected" {
        let request_id = match value.get("request_id") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) if s.len() <= 1024 => Some(s.clone()),
            _ => return Err(StreamingError::Protocol("Turns request identifier")),
        };
        return Ok(Some(CartesiaTurnsEvent::Connected {
            request_id,
            result: value,
        }));
    }
    if kind == "turn.resume" {
        return Ok(Some(CartesiaTurnsEvent::Resume { result: value }));
    }
    if !matches!(
        kind,
        "turn.start" | "turn.update" | "turn.eager_end" | "turn.end"
    ) {
        return Ok(None);
    }
    let transcript = match value.get("transcript") {
        None => String::new(),
        Some(Value::String(s)) => s.clone(),
        _ => return Err(StreamingError::Protocol("Turns transcript")),
    };
    Ok(Some(match kind {
        "turn.start" => CartesiaTurnsEvent::Start {
            transcript,
            result: value,
        },
        "turn.update" => CartesiaTurnsEvent::Update {
            transcript,
            result: value,
        },
        "turn.eager_end" => CartesiaTurnsEvent::EagerEnd {
            transcript,
            result: value,
        },
        "turn.end" => CartesiaTurnsEvent::End {
            transcript,
            result: value,
            audio_seconds: 0.0,
        },
        _ => unreachable!(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn delta(value: Value) -> SttSettings {
        SttSettings {
            base: pipecat_core::ServiceSettings {
                extra: serde_json::from_value(value).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        }
    }
    #[test]
    fn exact_defaults_and_query() {
        let mut c = CartesiaTurnsConfig::new("secret");
        assert_eq!(c.url().unwrap(), "wss://api.cartesia.ai/stt/turns/websocket?model=ink-2&encoding=pcm_s16le&sample_rate=16000");
        c.settings = c.settings.merge(&delta(json!({"turn_start_threshold":0.9,"turn_eager_end_threshold":0.3,"turn_end_threshold":0.5,"turn_end_timeout_ms":640,"keyterm":["Ink 2","a+b/~*?&中"]}))).unwrap();
        assert_eq!(c.url().unwrap(), "wss://api.cartesia.ai/stt/turns/websocket?model=ink-2&encoding=pcm_s16le&sample_rate=16000&turn_start_threshold=0.9&turn_eager_end_threshold=0.3&turn_end_threshold=0.5&turn_end_timeout_ms=640&keyterm=Ink%202&keyterm=a%2Bb%2F~%2A%3F%26%E4%B8%AD");
        let clear = c.settings.merge(&delta(json!({"keyterm":null,"turn_start_threshold":null,"turn_eager_end_threshold":null,"turn_end_threshold":null,"turn_end_timeout_ms":null}))).unwrap();
        assert_eq!(clear, CartesiaTurnsSettings::default());
        assert_eq!(c.limits.response_timeout, Duration::from_secs(2));
        assert!(!format!("{c:?}").contains("secret"));
    }
    #[test]
    fn keyterms_use_python_trim_first_overflow_and_no_model_filter() {
        let mut s = CartesiaTurnsSettings {
            model: "future custom".into(),
            keyterm: vec![
                "\u{1c} A \u{1f}".into(),
                "\u{85}".into(),
                "中".repeat(1199),
                "overflow".into(),
                "tail".into(),
            ],
            ..Default::default()
        };
        assert_eq!(
            s.prepared_keyterms().terms,
            vec!["A".to_string(), "中".repeat(1199)]
        );
        assert_eq!(s.prepared_keyterms().dropped, 2);
        assert!(!s.prepared_keyterms().ignored_model);
        s.keyterm = vec!["dup".into(); 101];
        assert_eq!(s.prepared_keyterms().terms, vec!["dup".to_string(); 100]);
    }
    #[test]
    fn updates_validate_transactionally_without_runtime_model_language_or_eager() {
        let s = CartesiaTurnsSettings::default();
        for value in [
            json!({"turn_start_threshold":0}),
            json!({"turn_start_threshold":0.91}),
            json!({"turn_eager_end_threshold":0.29}),
            json!({"turn_end_threshold":0.01}),
            json!({"turn_end_timeout_ms":639}),
            json!({"turn_end_timeout_ms":11201}),
            json!({"turn_end_timeout_ms":4294967296u64}),
            json!({"turn_end_timeout_ms":640.0}),
            json!({"turn_end_threshold":"0.2"}),
            json!({"keyterm":[3]}),
            json!({"model":"ink-2","keyterm":["new"]}),
            json!({"language":null}),
            json!({"enable_eager_end_of_turn":true}),
            json!({"unknown":null}),
        ] {
            assert!(s.merge(&delta(value)).is_err());
            assert_eq!(s, CartesiaTurnsSettings::default());
        }
        for (start, eager, end, timeout) in [(0.5, 0.3, 0.05, 640), (0.9, 0.6, 0.5, 11200)] {
            assert!(s.merge(&delta(json!({"turn_start_threshold":start,"turn_eager_end_threshold":eager,"turn_end_threshold":end,"turn_end_timeout_ms":timeout}))).is_ok());
        }
        let mut c = CartesiaTurnsConfig::new("key");
        c.settings.turn_end_threshold = Some(f64::NAN);
        assert!(CartesiaTurnsClient::new(c).is_err());
    }
    #[test]
    fn endpoint_auth_and_aggregate_limits_are_bounded() {
        for endpoint in [
            "https://api.cartesia.ai",
            "ws://remote.invalid",
            "wss://user:pass@api.cartesia.ai",
            "wss://api.cartesia.ai/?secret=1",
            "wss://api.cartesia.ai/#fragment",
        ] {
            let mut c = CartesiaTurnsConfig::new("key");
            c.endpoint = endpoint.into();
            assert!(CartesiaTurnsClient::new(c).is_err());
        }
        let mut c = CartesiaTurnsConfig::new("key\r\nheader: bad");
        assert!(CartesiaTurnsClient::new(c.clone()).is_err());
        c.api_key = "key".into();
        c.limits.queue_capacity = 64;
        c.limits.max_message_bytes = 1024 * 1024;
        assert!(CartesiaTurnsClient::new(c.clone()).is_err());
        c.limits.queue_capacity = 0;
        assert!(CartesiaTurnsClient::new(c).is_err());
    }
    #[test]
    fn parser_exact_types_fixed_finality_and_sanitization() {
        assert!(matches!(
            decode(json!({"type":"connected","request_id":"request"})).unwrap(),
            Some(CartesiaTurnsEvent::Connected {
                request_id: Some(_),
                ..
            })
        ));
        for (kind, expected) in [
            ("turn.start", 0),
            ("turn.update", 1),
            ("turn.eager_end", 2),
            ("turn.end", 3),
        ] {
            let event = decode(json!({"type":kind,"transcript":" ","is_final":true,"extra":7}))
                .unwrap()
                .unwrap();
            let (actual, text, result) = match event {
                CartesiaTurnsEvent::Start { transcript, result } => (0, transcript, result),
                CartesiaTurnsEvent::Update { transcript, result } => (1, transcript, result),
                CartesiaTurnsEvent::EagerEnd { transcript, result } => (2, transcript, result),
                CartesiaTurnsEvent::End {
                    transcript, result, ..
                } => (3, transcript, result),
                _ => panic!(),
            };
            assert_eq!(actual, expected);
            assert_eq!(text, " ");
            assert_eq!(result["extra"], 7);
        }
        assert!(
            matches!(decode(json!({"type":"turn.end"})).unwrap(), Some(CartesiaTurnsEvent::End { transcript, audio_seconds:0.0,.. }) if transcript.is_empty())
        );
        assert!(matches!(
            decode(json!({"type":"turn.resume"})).unwrap(),
            Some(CartesiaTurnsEvent::Resume { .. })
        ));
        assert!(decode(json!({"type":"future"})).unwrap().is_none());
        for v in [
            Value::Null,
            json!([]),
            json!({}),
            json!({"type":3}),
            json!({"type":"turn.update","transcript":3}),
            json!({"type":"connected","request_id":"x".repeat(1025)}),
        ] {
            assert!(decode(v).is_err());
        }
        assert!(decode_text("{broken").is_err());
        let error = decode(
            json!({"type":"error","message":"secret transcript","error_code":"private key"}),
        )
        .unwrap_err();
        assert!(!format!("{error:?} {error}").contains("secret"));
        assert!(!format!(
            "{:?}",
            decode(json!({"type":"turn.update","transcript":"secret"})).unwrap()
        )
        .contains("secret"));
    }
    #[test]
    fn watchdog_exact_budget_long_chunk_and_integer_samples() {
        let c = CartesiaTurnsConfig::new("key");
        let mut state = WatchdogState::default();
        assert!(state.next_check(true, &c).is_none());
        let now = Instant::now();
        state.last_send = Some(now);
        state.real_duration = Duration::from_millis(400);
        assert_eq!(
            state.next_check(true, &c),
            Some(now + Duration::from_millis(900))
        );
        assert!(state.next_check(false, &c).is_none());
        assert_eq!(
            state.catch_up_samples(Duration::from_secs(10), &c).unwrap(),
            160000
        );
        assert!(state
            .catch_up_samples(Duration::from_secs(10) + Duration::from_nanos(1), &c)
            .is_err());
        state.silence_samples = 479999;
        assert_eq!(
            state
                .catch_up_samples(Duration::from_nanos(62500), &c)
                .unwrap(),
            1
        );
        assert!(state
            .catch_up_samples(Duration::from_nanos(125000), &c)
            .is_err());
    }
}
