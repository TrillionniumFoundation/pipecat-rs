//! Bounded, original-rate bot PCM captured during an evaluation session.
use crate::{matcher::ScriptResult, EvalError};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::Value;

#[derive(Debug, Clone, Copy)]
pub struct BotAudioLimits {
    pub max_bytes: usize,
    pub max_chunks: usize,
}
impl Default for BotAudioLimits {
    fn default() -> Self {
        Self {
            max_bytes: 8 * 1024 * 1024,
            max_chunks: 4096,
        }
    }
}
impl BotAudioLimits {
    pub(crate) fn validate(self) -> Result<(), EvalError> {
        if self.max_bytes == 0
            || self.max_bytes > 16 * 1024 * 1024
            || self.max_chunks == 0
            || self.max_chunks > 16_384
        {
            return Err(EvalError::Scenario(
                "invalid bot audio capture limits".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotAudioChunk {
    /// Nonempty mono signed little-endian PCM16, at the original sample rate.
    pub audio: Vec<u8>,
    pub sample_rate: u32,
}
/// Audio admitted before the scenario ended, including partial audio on failure.
/// This is neither a playback acknowledgement nor a complete TTS recording.
#[derive(Debug)]
pub struct BotAudioRun {
    pub result: ScriptResult,
    pub chunks: Vec<BotAudioChunk>,
}
pub(crate) struct Capture {
    limits: BotAudioLimits,
    bytes: usize,
    pub chunks: Vec<BotAudioChunk>,
}
impl Capture {
    pub fn new(limits: BotAudioLimits) -> Self {
        Self {
            limits,
            bytes: 0,
            chunks: Vec::new(),
        }
    }
    pub fn ingest(&mut self, data: &Value) -> Result<(), EvalError> {
        let encoded = data
            .get("audio")
            .and_then(Value::as_str)
            .ok_or_else(|| EvalError::Scenario("bot audio requires base64 audio".into()))?;
        let rate = data
            .get("sampleRate")
            .and_then(Value::as_u64)
            .filter(|rate| matches!(*rate, 8_000 | 16_000 | 24_000 | 48_000))
            .ok_or_else(|| {
                EvalError::Scenario(
                    "bot audio sampleRate must be 8000, 16000, 24000 or 48000".into(),
                )
            })?;
        let remaining = self.limits.max_bytes.saturating_sub(self.bytes);
        if self.chunks.len() >= self.limits.max_chunks
            || encoded.len() > remaining.div_ceil(3).saturating_mul(4)
        {
            return Err(EvalError::Limit("bot audio capture"));
        }
        let audio = STANDARD
            .decode(encoded)
            .map_err(|_| EvalError::Scenario("invalid bot audio base64".into()))?;
        validate_pcm(&audio, rate as u32)?;
        if audio.len() > remaining {
            return Err(EvalError::Limit("bot audio capture"));
        }
        self.bytes += audio.len();
        self.chunks.push(BotAudioChunk {
            audio,
            sample_rate: rate as u32,
        });
        Ok(())
    }
}
pub(crate) fn validate_pcm(audio: &[u8], rate: u32) -> Result<(), EvalError> {
    if !matches!(rate, 8_000 | 16_000 | 24_000 | 48_000)
        || audio.is_empty()
        || !audio.len().is_multiple_of(2)
    {
        return Err(EvalError::Scenario(
            "bot audio requires nonempty whole PCM16 samples at 8/16/24/48 kHz".into(),
        ));
    }
    Ok(())
}
