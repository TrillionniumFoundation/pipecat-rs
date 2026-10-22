use std::time::Duration;

use pipecat_audio::PcmFormat;

use crate::{Error, Result};

/// Limits apply to this adapter, not the SDK's internal queues.
#[derive(Debug, Clone)]
pub struct LiveKitConfig {
    pub input_format: PcmFormat,
    pub output_format: Option<PcmFormat>,
    /// Disabled by default; bounded I420 receive, per-source sampling and optional plain pipeline requests.
    pub video_input: Option<crate::VideoInputConfig>,
    /// Disabled by default; one fixed-size room camera track when configured.
    pub video_output: Option<crate::VideoOutputConfig>,
    pub queue_capacity: usize,
    pub max_audio_tracks: usize,
    pub max_data_bytes: usize,
    pub max_audio_frame_bytes: usize,
    pub source_queue_ms: u32,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub close_timeout: Duration,
    /// Receive audio and video only from these participant identities; empty means all.
    pub participant_filter: Vec<String>,
}
impl Default for LiveKitConfig {
    fn default() -> Self {
        Self {
            input_format: PcmFormat::new(16000, 1).expect("valid default format"),
            output_format: Some(PcmFormat::new(24000, 1).expect("valid default format")),
            video_input: None,
            video_output: None,
            queue_capacity: 128,
            max_audio_tracks: 16,
            max_data_bytes: 15_000,
            max_audio_frame_bytes: 192_000,
            source_queue_ms: 100,
            connect_timeout: Duration::from_secs(20),
            operation_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(5),
            participant_filter: Vec::new(),
        }
    }
}
impl LiveKitConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_format(self.input_format)?;
        if let Some(video) = &self.video_input {
            video.validate()?;
        }
        if let Some(video) = &self.video_output {
            video.validate()?;
        }
        if let Some(format) = self.output_format {
            validate_format(format)?;
        }
        if self.queue_capacity == 0
            || self.queue_capacity > 65536
            || self.max_audio_tracks == 0
            || self.max_audio_tracks > 1024
            || self.max_data_bytes == 0
            || self.max_data_bytes > 15000
            || self.max_audio_frame_bytes == 0
            || self.max_audio_frame_bytes > 1_048_576
            || self.source_queue_ms > 2000
            || !self.source_queue_ms.is_multiple_of(10)
            || self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.close_timeout.is_zero()
        {
            return Err(Error::InvalidConfig(
                "invalid queue, track, frame, source-buffer or timeout limit",
            ));
        }
        Ok(())
    }
    pub(crate) fn accepts(&self, identity: &str) -> bool {
        self.participant_filter.is_empty() || self.participant_filter.iter().any(|s| s == identity)
    }
}
fn validate_format(format: PcmFormat) -> Result<()> {
    if !matches!(
        format.sample_rate(),
        8000 | 16000 | 24000 | 32000 | 44100 | 48000
    ) || !(1..=2).contains(&format.channels())
    {
        return Err(Error::InvalidConfig(
            "audio requires a supported 8–48 kHz rate and one or two channels",
        ));
    }
    Ok(())
}
