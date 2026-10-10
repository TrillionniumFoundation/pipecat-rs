//! PCM16LE conversion, mixing, and WAV encapsulation.

use crate::{AudioError, Result};

/// Inclusive peak threshold used by Pipecat's clean-signal silence helper.
pub const SPEAKING_THRESHOLD: i32 = 20;

/// Validated interleaved signed PCM16LE format.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PcmFormat {
    sample_rate: u32,
    channels: u16,
}

impl PcmFormat {
    pub fn new(sample_rate: u32, channels: u16) -> Result<Self> {
        if sample_rate == 0 {
            return Err(AudioError::InvalidSampleRate);
        }
        if channels == 0 || channels > u16::MAX / 2 {
            return Err(AudioError::InvalidChannels);
        }
        Ok(Self {
            sample_rate,
            channels,
        })
    }

    pub fn sample_rate(self) -> u32 {
        self.sample_rate
    }
    pub fn channels(self) -> u16 {
        self.channels
    }
    pub fn frame_bytes(self) -> usize {
        usize::from(self.channels) * 2
    }

    /// Reject incomplete samples or interleaved channel frames.
    pub fn validate(self, pcm: &[u8]) -> Result<()> {
        if !pcm.len().is_multiple_of(self.frame_bytes()) {
            return Err(AudioError::IncompleteFrame {
                length: pcm.len(),
                frame_bytes: self.frame_bytes(),
            });
        }
        Ok(())
    }

    pub fn frame_count(self, pcm: &[u8]) -> Result<usize> {
        self.validate(pcm)?;
        Ok(pcm.len() / self.frame_bytes())
    }
}

/// Read signed little-endian samples without alignment or host-endian assumptions.
pub fn decode_pcm16_le(pcm: &[u8]) -> Result<Vec<i16>> {
    if !pcm.len().is_multiple_of(2) {
        return Err(AudioError::IncompleteFrame {
            length: pcm.len(),
            frame_bytes: 2,
        });
    }
    Ok(pcm
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect())
}

pub fn encode_pcm16_le(samples: &[i16]) -> Vec<u8> {
    samples.iter().flat_map(|s| s.to_le_bytes()).collect()
}

/// Saturating mix, zero-padding the shorter stream as in `audio/utils.py`.
pub fn mix_audio(first: &[u8], second: &[u8]) -> Result<Vec<u8>> {
    let a = decode_pcm16_le(first)?;
    let b = decode_pcm16_le(second)?;
    Ok((0..a.len().max(b.len()))
        .flat_map(|i| {
            a.get(i)
                .copied()
                .unwrap_or(0)
                .saturating_add(b.get(i).copied().unwrap_or(0))
                .to_le_bytes()
        })
        .collect())
}

/// Scale in float32, clip, and truncate toward zero, matching Pipecat's NumPy path.
/// Non-finite gains are rejected instead of relying on undefined float casts.
pub fn scale_audio(audio: &[u8], factor: f32) -> Result<Vec<u8>> {
    if !factor.is_finite() {
        return Err(AudioError::InvalidParameter("gain must be finite"));
    }
    Ok(decode_pcm16_le(audio)?
        .iter()
        .flat_map(|s| ((*s as f32 * factor).clamp(-32768.0, 32767.0) as i16).to_le_bytes())
        .collect())
}

/// Interleave two mono channels, truncating both to the shorter channel.
pub fn interleave_stereo_audio(left: &[u8], right: &[u8]) -> Result<Vec<u8>> {
    let left = decode_pcm16_le(left)?;
    let right = decode_pcm16_le(right)?;
    Ok(left
        .iter()
        .zip(right.iter())
        .flat_map(|(l, r)| [*l, *r])
        .flat_map(i16::to_le_bytes)
        .collect())
}

/// Encapsulate PCM16LE in RIFF/WAVE, dropping only a trailing incomplete frame.
pub fn pcm_to_wav(pcm: &[u8], sample_rate: u32, num_channels: u16) -> Result<Vec<u8>> {
    let format = PcmFormat::new(sample_rate, num_channels)?;
    let length = pcm.len() - pcm.len() % format.frame_bytes();
    let data_size = u32::try_from(length).map_err(|_| AudioError::SizeOverflow)?;
    let riff_size = data_size.checked_add(36).ok_or(AudioError::SizeOverflow)?;
    let byte_rate = sample_rate
        .checked_mul(format.frame_bytes() as u32)
        .ok_or(AudioError::SizeOverflow)?;
    let mut wav = Vec::with_capacity(length.checked_add(44).ok_or(AudioError::SizeOverflow)?);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&riff_size.to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes());
    wav.extend_from_slice(&1_u16.to_le_bytes());
    wav.extend_from_slice(&num_channels.to_le_bytes());
    wav.extend_from_slice(&sample_rate.to_le_bytes());
    wav.extend_from_slice(&byte_rate.to_le_bytes());
    wav.extend_from_slice(&(format.frame_bytes() as u16).to_le_bytes());
    wav.extend_from_slice(&16_u16.to_le_bytes());
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_size.to_le_bytes());
    wav.extend_from_slice(&pcm[..length]);
    Ok(wav)
}

/// Peak-amplitude check for clean speech or silence, not a voice activity model.
/// Empty input returns an error, matching the Python reduction's invalid input.
pub fn is_silence(pcm: &[u8]) -> Result<bool> {
    let samples = decode_pcm16_le(pcm)?;
    if samples.is_empty() {
        return Err(AudioError::EmptyAudio);
    }
    Ok(samples
        .iter()
        .all(|s| i32::from(*s).abs() <= SPEAKING_THRESHOLD))
}

pub fn exp_smoothing(value: f64, previous: f64, factor: f64) -> f64 {
    previous + factor * (value - previous)
}
