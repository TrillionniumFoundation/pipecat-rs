use crate::{error, wire::AudioLevelData, wire::ServerEvent};
use pipecat_audio::AudioVolumeTracker;
use pipecat_core::{AudioRawFrame, FrameData, ProcessorError};
use std::time::{Duration, Instant};

/// Explicit, bounded mono PCM16 audio-level reporting. Both streams default off.
#[derive(Debug, Clone)]
pub struct AudioLevelConfig {
    pub user_enabled: bool,
    pub bot_enabled: bool,
    pub period: Duration,
    pub max_frame_bytes: usize,
}
impl Default for AudioLevelConfig {
    fn default() -> Self {
        Self {
            user_enabled: false,
            bot_enabled: false,
            period: Duration::from_millis(150),
            max_frame_bytes: 192_000,
        }
    }
}
impl AudioLevelConfig {
    pub(crate) fn validate(&self) -> Result<(), ProcessorError> {
        if self.period < Duration::from_millis(1)
            || self.period > Duration::from_secs(60)
            || self.max_frame_bytes < 2
            || self.max_frame_bytes > 192_000
        {
            return Err(proposal_error(
                "audio-level period must be 1ms..60s and frame budget 2..192000 bytes",
            ));
        }
        Ok(())
    }
}
fn proposal_error(message: impl std::fmt::Display) -> ProcessorError {
    error(message).with_permanence_override(false)
}
#[derive(Default)]
struct Stream {
    meter: Option<AudioVolumeTracker>,
    rate: u32,
    last_report: Option<Instant>,
}
pub(crate) struct AudioLevels {
    config: AudioLevelConfig,
    user: Stream,
    bot: Stream,
}
impl AudioLevels {
    pub(crate) fn new(config: AudioLevelConfig) -> Self {
        Self {
            config,
            user: Stream::default(),
            bot: Stream::default(),
        }
    }
    pub(crate) fn reset(&mut self) {
        self.user = Stream::default();
        self.bot = Stream::default();
    }
    fn selected<'a>(&self, data: &'a FrameData) -> Option<(&'a AudioRawFrame, bool)> {
        match data {
            FrameData::InputAudio(a) if self.config.user_enabled => Some((a, true)),
            FrameData::UserAudio(a) if self.config.user_enabled => Some((&a.audio, true)),
            FrameData::TtsAudio(a) if self.config.bot_enabled => Some((a, false)),
            _ => None,
        }
    }
    pub(crate) fn enabled_for(&self, data: &FrameData) -> bool {
        self.selected(data).is_some()
    }
    /// Validate before any window, timer or observer dedup state changes.
    pub(crate) fn validate_frame(&self, data: &FrameData) -> Result<(), ProcessorError> {
        let Some((a, _)) = self.selected(data) else {
            return Ok(());
        };
        if a.num_channels != 1
            || !matches!(a.sample_rate, 8_000 | 16_000 | 24_000 | 48_000)
            || !a.audio.len().is_multiple_of(2)
            || a.audio.len() > self.config.max_frame_bytes
        {
            return Err(proposal_error(
                "audio levels require bounded whole mono PCM16 at 8/16/24/48 kHz",
            ));
        }
        Ok(())
    }
    pub(crate) fn observe(
        &mut self,
        data: &FrameData,
        now: Instant,
    ) -> Result<Option<ServerEvent>, ProcessorError> {
        let Some((a, user)) = self.selected(data) else {
            return Ok(None);
        };
        self.validate_frame(data)?;
        let stream = if user { &mut self.user } else { &mut self.bot };
        if stream.rate != a.sample_rate || stream.meter.is_none() {
            stream.meter = Some(AudioVolumeTracker::new(a.sample_rate).map_err(proposal_error)?);
            stream.rate = a.sample_rate;
        }
        let meter = stream.meter.as_mut().expect("validated meter");
        meter.update_pcm16_le(&a.audio).map_err(proposal_error)?;
        if stream
            .last_report
            .is_some_and(|last| now.saturating_duration_since(last) <= self.config.period)
        {
            return Ok(None);
        }
        let value = meter.volume();
        stream.last_report = Some(now);
        let level = AudioLevelData { value };
        Ok(Some(if user {
            ServerEvent::UserAudioLevel(level)
        } else {
            ServerEvent::BotAudioLevel(level)
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn frame(rate: u32, samples: usize) -> FrameData {
        FrameData::InputAudio(AudioRawFrame::new(vec![0; samples * 2], rate, 1).unwrap())
    }
    #[test]
    fn exact_period_and_rate_changes_keep_clock_separate_from_window() {
        let mut levels = AudioLevels::new(AudioLevelConfig {
            user_enabled: true,
            ..Default::default()
        });
        let t = Instant::now();
        assert!(levels.observe(&frame(8_000, 1600), t).unwrap().is_some());
        assert!(levels
            .observe(&frame(8_000, 1600), t + Duration::from_millis(150))
            .unwrap()
            .is_none());
        assert_eq!(levels.user.meter.as_ref().unwrap().buffered_samples(), 3200);
        assert!(levels
            .observe(&frame(48_000, 100), t + Duration::from_millis(150))
            .unwrap()
            .is_none());
        assert_eq!(levels.user.meter.as_ref().unwrap().buffered_samples(), 100);
        assert!(levels
            .observe(&frame(48_000, 0), t + Duration::from_millis(151))
            .unwrap()
            .is_some());
        levels.reset();
        assert!(levels.observe(&frame(8_000, 0), t).unwrap().is_some());
    }
    #[test]
    fn rejected_frame_preserves_window_and_report_clock() {
        let mut levels = AudioLevels::new(AudioLevelConfig {
            user_enabled: true,
            ..Default::default()
        });
        let t = Instant::now();
        levels.observe(&frame(8_000, 1600), t).unwrap();
        let bad = FrameData::InputAudio(AudioRawFrame {
            audio: vec![1],
            sample_rate: 24_000,
            num_channels: 1,
            context_id: None,
        });
        assert!(levels.observe(&bad, t + Duration::from_secs(1)).is_err());
        assert_eq!(levels.user.rate, 8_000);
        assert_eq!(levels.user.last_report, Some(t));
        assert_eq!(levels.user.meter.as_ref().unwrap().buffered_samples(), 1600);
    }
}
