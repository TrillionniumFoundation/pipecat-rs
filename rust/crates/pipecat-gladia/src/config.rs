// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{security, GladiaSttSettings};
use std::{fmt, time::Duration};
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GladiaError {
    #[error("invalid Gladia configuration: {0}")]
    Invalid(&'static str),
    #[error("Gladia endpoint policy rejected the destination")]
    Endpoint,
    #[error("Gladia HTTP status {0}")]
    Http(u16),
    #[error("Gladia transport failed; submitted audio may have been received")]
    Transport,
    #[error("invalid Gladia message: {0}")]
    Protocol(&'static str),
    #[error("Gladia {0} rejected; provider details redacted")]
    Provider(&'static str),
    #[error("Gladia {0} timed out")]
    Timeout(&'static str),
    #[error("Gladia session cancelled")]
    Cancelled,
    #[error("Gladia session closed")]
    Closed,
    #[error("Gladia bounded queue or unacknowledged audio budget is full")]
    Backpressure,
    #[error("Gladia data exceeds configured bounds")]
    TooLarge,
    #[error("Gladia PCM ends in a partial frame")]
    PartialSample,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GladiaLimits {
    pub queue_capacity: usize,
    pub max_message_bytes: usize,
    pub max_http_body_bytes: usize,
    pub max_audio_chunk_bytes: usize,
    /// Includes queued and submitted-but-unacknowledged PCM. Never trims samples.
    pub max_buffered_audio_bytes: usize,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub read_timeout: Duration,
    pub finish_timeout: Duration,
    pub close_timeout: Duration,
    /// Empty JSON audio chunks are sent only after this much write inactivity.
    pub keepalive_interval: Duration,
}
impl Default for GladiaLimits {
    fn default() -> Self {
        Self {
            queue_capacity: 32,
            max_message_bytes: 1024 * 1024,
            max_http_body_bytes: 65536,
            max_audio_chunk_bytes: 65536,
            max_buffered_audio_bytes: 20 * 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            read_timeout: Duration::from_secs(60),
            finish_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(2),
            keepalive_interval: Duration::from_secs(20),
        }
    }
}
impl GladiaLimits {
    pub fn validate(&self) -> Result<(), GladiaError> {
        if !(1..=128).contains(&self.queue_capacity)
            || !(256..=4 * 1024 * 1024).contains(&self.max_message_bytes)
            || !(128..=1024 * 1024).contains(&self.max_http_body_bytes)
            || self.max_audio_chunk_bytes == 0
            || self.max_audio_chunk_bytes > 1024 * 1024
            || self.max_audio_chunk_bytes > self.max_buffered_audio_bytes
            || self.max_buffered_audio_bytes > 64 * 1024 * 1024
            || self.max_audio_chunk_bytes.saturating_add(2) / 3 * 4 + 64 > self.max_message_bytes
        {
            return Err(GladiaError::Invalid("limits"));
        }
        for t in [
            self.connect_timeout,
            self.write_timeout,
            self.read_timeout,
            self.finish_timeout,
            self.close_timeout,
            self.keepalive_interval,
        ] {
            if t.is_zero() || t > Duration::from_secs(300) {
                return Err(GladiaError::Invalid("timeout bounds"));
            }
        }
        Ok(())
    }
}
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum GladiaRegion {
    UsWest,
    EuWest,
}
impl GladiaRegion {
    pub(crate) fn wire(self) -> &'static str {
        match self {
            Self::UsWest => "us-west",
            Self::EuWest => "eu-west",
        }
    }
}
#[derive(Clone, PartialEq)]
pub struct GladiaSttConfig {
    pub endpoint: String,
    pub api_key: String,
    pub region: Option<GladiaRegion>,
    /// Exact origins, including scheme and effective port. No wildcards.
    pub allowed_session_origins: Vec<String>,
    /// Enables HTTP/WS only for literal loopback addresses, never localhost DNS.
    pub allow_loopback: bool,
    pub sample_rate: u32,
    pub channels: u16,
    pub settings: GladiaSttSettings,
    pub should_interrupt: bool,
    pub limits: GladiaLimits,
}
impl fmt::Debug for GladiaSttConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GladiaSttConfig")
            .field("credentials_and_endpoints", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("channels", &self.channels)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl GladiaSttConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "https://api.gladia.io/v2/live".into(),
            api_key: api_key.into(),
            region: None,
            allowed_session_origins: vec!["wss://api.gladia.io".into()],
            allow_loopback: false,
            sample_rate: 16000,
            channels: 1,
            settings: GladiaSttSettings::default(),
            should_interrupt: true,
            limits: GladiaLimits::default(),
        }
    }
    pub fn validate(&self) -> Result<(), GladiaError> {
        self.limits.validate()?;
        self.settings.validate()?;
        if self.api_key.is_empty()
            || self.api_key.len() > 8192
            || self.api_key.bytes().any(|b| b < 32 || b == 127)
            || ![8000, 16000, 32000, 44100, 48000].contains(&self.sample_rate)
            || !(1..=8).contains(&self.channels)
        {
            return Err(GladiaError::Invalid("credentials or PCM format"));
        }
        security::init_url(self)?;
        security::validate_origins(self)?;
        if self.session_request()?.to_string().len() > self.limits.max_http_body_bytes {
            return Err(GladiaError::TooLarge);
        }
        Ok(())
    }
    pub fn session_request(&self) -> Result<serde_json::Value, GladiaError> {
        self.settings.validate()?;
        let mut value =
            serde_json::to_value(&self.settings).map_err(|_| GladiaError::Invalid("settings"))?;
        let object = value.as_object_mut().unwrap();
        object.remove("enable_vad");
        object.remove("language");
        object.retain(|_, v| !v.is_null());
        object.extend(serde_json::json!({"encoding":"wav/pcm","bit_depth":16,"sample_rate":self.sample_rate,"channels":self.channels}).as_object().unwrap().clone());
        let mut metadata = self.settings.custom_metadata.clone().unwrap_or_default();
        metadata.insert(
            "pipecat".into(),
            serde_json::Value::String(env!("CARGO_PKG_VERSION").into()),
        );
        object.insert(
            "custom_metadata".into(),
            serde_json::Value::Object(metadata),
        );
        Ok(value)
    }
}
