//! Mean-removed RMS onset detection, following Pipecat's TTFA audio utility.
//!
//! This energy detector is for sustained audible onset in a growing buffer.
//! It is not a speech classifier, VAD neural network, or noise suppressor.

use crate::{AudioError, PcmFormat, Result};

#[derive(Debug, Clone, Copy)]
pub struct SpeechOnsetConfig {
    pub frame_ms: f64,
    pub hop_ms: f64,
    pub threshold_db: f64,
    pub min_voiced_ms: f64,
}

impl Default for SpeechOnsetConfig {
    fn default() -> Self {
        Self {
            frame_ms: 10.0,
            hop_ms: 1.0,
            threshold_db: -40.0,
            min_voiced_ms: 50.0,
        }
    }
}

/// Return the per-channel sample index of the first sustained energy onset.
/// Trailing partial channel frames are ignored, as in the Python utility.
/// A zero sample rate, empty buffer, or insufficient audio yields `None`.
pub fn detect_speech_onset(pcm: &[u8], sample_rate: u32, channels: u16) -> Result<Option<usize>> {
    detect_speech_onset_with_config(pcm, sample_rate, channels, SpeechOnsetConfig::default())
}

pub fn detect_speech_onset_with_config(
    pcm: &[u8],
    sample_rate: u32,
    channels: u16,
    config: SpeechOnsetConfig,
) -> Result<Option<usize>> {
    if sample_rate == 0 {
        return Ok(None);
    }
    let format = PcmFormat::new(sample_rate, channels)?;
    for time in [config.frame_ms, config.hop_ms, config.min_voiced_ms] {
        if !time.is_finite() || time <= 0.0 {
            return Err(AudioError::InvalidParameter(
                "onset window, hop, and minimum duration must be positive and finite",
            ));
        }
    }
    if !config.threshold_db.is_finite() {
        return Err(AudioError::InvalidParameter(
            "onset threshold must be finite",
        ));
    }
    let rounded = |n: f64| -> Result<usize> {
        if !n.is_finite() || n >= usize::MAX as f64 {
            return Err(AudioError::SizeOverflow);
        }
        Ok((n.round_ties_even() as usize).max(1))
    };
    let frame = rounded(f64::from(sample_rate) * config.frame_ms / 1000.0)?;
    let hop = rounded(f64::from(sample_rate) * config.hop_ms / 1000.0)?;
    let minimum_windows = rounded(config.min_voiced_ms / config.hop_ms)?;
    let n = pcm.len() / format.frame_bytes();
    if n < frame {
        return Ok(None);
    }
    let mono: Vec<f64> = pcm
        .chunks_exact(format.frame_bytes())
        .map(|samples| {
            let sum: i64 = samples
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| i64::from(i16::from_le_bytes([b[0], b[1]])))
                .sum();
            // Python downmixes to float32 before normalizing to full scale.
            f64::from((sum as f64 / f64::from(channels)) as f32 / 32768.0)
        })
        .collect();
    let gate = 10.0_f64.powf(config.threshold_db / 20.0);
    let padding = frame / 2;
    let last_start = n.checked_add(padding * 2).ok_or(AudioError::SizeOverflow)? - frame;
    let mut run = 0usize;
    for (index, start) in (0..=last_start).step_by(hop).enumerate() {
        let sample = |j: usize| mono[start.saturating_add(j).saturating_sub(padding).min(n - 1)];
        let mean = (0..frame).map(sample).sum::<f64>() / frame as f64;
        let variance = (0..frame).map(|j| (sample(j) - mean).powi(2)).sum::<f64>() / frame as f64;
        if variance.sqrt() > gate {
            run += 1;
            if run >= minimum_windows {
                return Ok(Some((index + 1 - minimum_windows) * hop));
            }
        } else {
            run = 0;
        }
    }
    Ok(None)
}
