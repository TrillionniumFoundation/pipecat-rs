// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use serde_json::{json, Map, Value};
use std::{fmt, time::Duration};
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GradiumError {
    #[error("invalid Gradium configuration: {0}")]
    Invalid(&'static str),
    #[error("Gradium endpoint rejected")]
    Endpoint,
    #[error("Gradium transport failed")]
    Transport,
    #[error("Gradium HTTP status {0}")]
    Http(u16),
    #[error("Gradium protocol error: {0}")]
    Protocol(&'static str),
    #[error("Gradium provider rejected the stream")]
    Provider,
    #[error("Gradium limit exceeded")]
    TooLarge,
    #[error("Gradium input or output capacity exhausted")]
    Backpressure,
    #[error("Gradium flush already pending")]
    Busy,
    #[error("Gradium stream ended with an incomplete PCM sample")]
    PartialSample,
    #[error("Gradium deadline exceeded: {0}")]
    Timeout(&'static str),
    #[error("Gradium session closed")]
    Closed,
    #[error("Gradium session cancelled")]
    Cancelled,
    #[error("Gradium connection settings require a new session")]
    RequiresRestart,
}
#[derive(Debug, Clone)]
pub struct GradiumLimits {
    pub queue_capacity: usize,
    pub max_buffered_audio_bytes: usize,
    pub max_audio_chunk_bytes: usize,
    pub max_message_bytes: usize,
    pub max_text_bytes: usize,
    pub max_session_input_bytes: u64,
    pub max_session_output_bytes: u64,
    pub max_session_messages: u64,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub read_timeout: Duration,
    pub flush_timeout: Duration,
    pub finish_timeout: Duration,
    pub close_timeout: Duration,
    pub session_timeout: Duration,
}
impl Default for GradiumLimits {
    fn default() -> Self {
        Self {
            queue_capacity: 32,
            max_buffered_audio_bytes: 256 * 1024,
            max_audio_chunk_bytes: 64 * 1024,
            max_message_bytes: 256 * 1024,
            max_text_bytes: 64 * 1024,
            max_session_input_bytes: 64 * 1024 * 1024,
            max_session_output_bytes: 64 * 1024 * 1024,
            max_session_messages: 100000,
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(30),
            flush_timeout: Duration::from_secs(5),
            finish_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_millis(500),
            session_timeout: Duration::from_secs(1800),
        }
    }
}
impl GradiumLimits {
    fn validate(&self) -> Result<(), GradiumError> {
        if !(1..=1024).contains(&self.queue_capacity)
            || !(1..=64 * 1024).contains(&self.max_audio_chunk_bytes)
            || self.max_buffered_audio_bytes < self.max_audio_chunk_bytes
            || self.max_buffered_audio_bytes > 16 * 1024 * 1024
            || !(8192..=1024 * 1024).contains(&self.max_message_bytes)
            || !(1..=1024 * 1024).contains(&self.max_text_bytes)
            || self.queue_capacity.saturating_mul(self.max_text_bytes) > 16 * 1024 * 1024
            || !(1..=1024 * 1024 * 1024).contains(&self.max_session_input_bytes)
            || !(1..=1024 * 1024 * 1024).contains(&self.max_session_output_bytes)
            || !(1..=1000000).contains(&self.max_session_messages)
        {
            return Err(GradiumError::Invalid("limits"));
        }
        for d in [
            self.connect_timeout,
            self.write_timeout,
            self.read_timeout,
            self.flush_timeout,
            self.finish_timeout,
            self.close_timeout,
            self.session_timeout,
        ] {
            if d.is_zero() || d > Duration::from_secs(3000) {
                return Err(GradiumError::Invalid("deadline"));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct GradiumTurnSettings {
    pub eot_horizon_s: f64,
    pub eot_threshold: f64,
    pub post_flush_cooldown_frames: u32,
}
impl Default for GradiumTurnSettings {
    fn default() -> Self {
        Self {
            eot_horizon_s: 3.0,
            eot_threshold: 0.5,
            post_flush_cooldown_frames: 8,
        }
    }
}
impl GradiumTurnSettings {
    pub fn validate(&self) -> Result<(), GradiumError> {
        if !self.eot_horizon_s.is_finite()
            || !(0.0..=60.0).contains(&self.eot_horizon_s)
            || !self.eot_threshold.is_finite()
            || !(0.0..=1.0).contains(&self.eot_threshold)
            || self.post_flush_cooldown_frames > 1000
        {
            return Err(GradiumError::Invalid("turn settings"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct GradiumSttSettings {
    pub model: String,
    pub language: Option<String>,
    pub delay_in_frames: Option<u32>,
    pub turn: GradiumTurnSettings,
}
impl Default for GradiumSttSettings {
    fn default() -> Self {
        Self {
            model: "default".into(),
            language: Some("en".into()),
            delay_in_frames: Some(12),
            turn: Default::default(),
        }
    }
}
impl GradiumSttSettings {
    pub fn validate(&self) -> Result<(), GradiumError> {
        if self.model.is_empty()
            || self.model.len() > 128
            || self.model.chars().any(char::is_control)
        {
            return Err(GradiumError::Invalid("model"));
        }
        if let Some(v) = &self.language {
            if v.is_empty()
                || v.len() > 32
                || !v.bytes().all(|b| b.is_ascii_alphabetic() || b == b'-')
            {
                return Err(GradiumError::Invalid("language"));
            }
        }
        if self
            .delay_in_frames
            .is_some_and(|v| ![7, 8, 10, 12, 14, 16, 20, 24, 36, 48].contains(&v))
        {
            return Err(GradiumError::Invalid("delay_in_frames"));
        }
        self.turn.validate()
    }
    pub fn merge(&self, delta: &Map<String, Value>) -> Result<Self, GradiumError> {
        let mut next = self.clone();
        for (k, v) in delta {
            match k.as_str() {
                "model" => next.model = v.as_str().ok_or(GradiumError::Invalid("model"))?.into(),
                "language" => {
                    next.language = if v.is_null() {
                        None
                    } else {
                        Some(v.as_str().ok_or(GradiumError::Invalid("language"))?.into())
                    }
                }
                "delay_in_frames" => {
                    next.delay_in_frames = if v.is_null() {
                        None
                    } else {
                        Some(
                            v.as_u64()
                                .and_then(|n| u32::try_from(n).ok())
                                .ok_or(GradiumError::Invalid("delay_in_frames"))?,
                        )
                    }
                }
                "eot_horizon_s" => {
                    next.turn.eot_horizon_s =
                        v.as_f64().ok_or(GradiumError::Invalid("eot_horizon_s"))?
                }
                "eot_threshold" => {
                    next.turn.eot_threshold =
                        v.as_f64().ok_or(GradiumError::Invalid("eot_threshold"))?
                }
                "post_flush_cooldown_frames" => {
                    next.turn.post_flush_cooldown_frames = v
                        .as_u64()
                        .and_then(|n| u32::try_from(n).ok())
                        .ok_or(GradiumError::Invalid("post_flush_cooldown_frames"))?
                }
                _ => return Err(GradiumError::Invalid("unsupported setting")),
            }
        }
        next.validate()?;
        Ok(next)
    }
    pub(crate) fn same_connection(&self, other: &Self) -> bool {
        self.model == other.model
            && self.language == other.language
            && self.delay_in_frames == other.delay_in_frames
    }
}
#[derive(Clone)]
pub struct GradiumSttConfig {
    pub api_key: String,
    pub endpoint: String,
    /// Only literal loopback endpoints are admitted by this fixture escape hatch.
    pub allow_loopback: bool,
    pub sample_rate: u32,
    pub channels: u16,
    /// Only "pcm" is supported. WAV and Ogg/Opus must not be labelled PCM.
    pub encoding: String,
    pub enable_turn_detection: bool,
    pub enable_interruptions: bool,
    pub settings: GradiumSttSettings,
    pub limits: GradiumLimits,
}
impl fmt::Debug for GradiumSttConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GradiumSttConfig")
            .field("sample_rate", &self.sample_rate)
            .field("channels", &self.channels)
            .field("enable_turn_detection", &self.enable_turn_detection)
            .finish_non_exhaustive()
    }
}
impl GradiumSttConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            endpoint: "wss://api.gradium.ai/api/speech/asr".into(),
            allow_loopback: false,
            sample_rate: 16000,
            channels: 1,
            encoding: "pcm".into(),
            enable_turn_detection: false,
            enable_interruptions: true,
            settings: Default::default(),
            limits: Default::default(),
        }
    }
    pub fn wire_sample_rate(&self) -> u32 {
        [8000, 16000, 24000]
            .into_iter()
            .find(|r| *r >= self.sample_rate)
            .unwrap_or(24000)
    }
    pub fn validate(&self) -> Result<(), GradiumError> {
        if self.api_key.is_empty()
            || self.api_key.len() > 4096
            || !self.api_key.bytes().all(|b| (0x21..=0x7e).contains(&b))
        {
            return Err(GradiumError::Invalid("API key"));
        }
        if !(1000..=192000).contains(&self.sample_rate)
            || self.channels != 1
            || self.encoding != "pcm"
        {
            return Err(GradiumError::Invalid("mono PCM16 format"));
        }
        self.settings.validate()?;
        self.limits.validate()?;
        crate::security::endpoint(self)?;
        Ok(())
    }
    pub fn setup_message(&self) -> Value {
        let mut config = Map::new();
        if let Some(language) = &self.settings.language {
            config.insert(
                "language".into(),
                Value::String(
                    language
                        .split('-')
                        .next()
                        .unwrap_or(language)
                        .to_lowercase(),
                ),
            );
        }
        if let Some(delay) = self.settings.delay_in_frames {
            config.insert("delay_in_frames".into(), json!(delay));
        }
        let mut value = json!({"type":"setup","model_name":self.settings.model,"input_format":format!("pcm_{}",self.wire_sample_rate())});
        if !config.is_empty() {
            value["json_config"] = Value::Object(config)
        }
        value
    }
}
