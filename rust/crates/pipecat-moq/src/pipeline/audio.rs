use super::*;
use pipecat_audio::SincResampler;
use std::collections::VecDeque;

pub(super) fn validate_rate(rate: u32) -> Result<()> {
    if !(8_000..=192_000).contains(&rate) {
        return Err(Error::InvalidAudio(
            "pipeline rates must be within 8000..=192000 Hz",
        ));
    }
    Ok(())
}
pub(super) fn validate_audio(audio: &AudioRawFrame) -> Result<()> {
    validate_rate(audio.sample_rate)?;
    audio.validate().map_err(|_| {
        Error::InvalidAudio("PCM format must be nonzero with aligned 16-bit channel samples")
    })?;
    if !(1..=8).contains(&audio.num_channels) {
        return Err(Error::InvalidAudio(
            "pipeline PCM must have one to eight channels",
        ));
    }
    if audio.num_frames() > audio.sample_rate as usize {
        return Err(Error::Limit("pipeline audio frame exceeds one second"));
    }
    Ok(())
}
/// Downmix before rate conversion, using a wider accumulator with no saturation
/// during summation. Empty aligned buffers are valid and retain no samples.
fn mono(audio: &AudioRawFrame) -> Vec<u8> {
    let mut out = Vec::with_capacity(audio.num_frames() * 2);
    for frame in audio
        .audio
        .chunks_exact(usize::from(audio.num_channels) * 2)
    {
        let sum: i32 = frame
            .as_chunks::<2>()
            .0
            .iter()
            .map(|s| i32::from(i16::from_le_bytes(*s)))
            .sum();
        out.extend_from_slice(&((sum / i32::from(audio.num_channels)) as i16).to_le_bytes());
    }
    out
}
pub(super) struct Conversion {
    pub rate: u32,
    pub channels: u16,
    pub context: Option<String>,
    pub generation: u64,
    pub interruptible: bool,
    resampler: SincResampler,
    pending: VecDeque<u8>,
}
impl Conversion {
    pub fn new(audio: &AudioRawFrame, generation: u64, interruptible: bool) -> Result<Self> {
        validate_audio(audio)?;
        Ok(Self {
            rate: audio.sample_rate,
            channels: audio.num_channels,
            context: audio.context_id.clone(),
            generation,
            interruptible,
            resampler: SincResampler::new(audio.sample_rate, 48_000, 1).map_err(Error::media)?,
            pending: VecDeque::new(),
        })
    }
    pub fn matches(&self, audio: &AudioRawFrame, generation: u64, interruptible: bool) -> bool {
        self.rate == audio.sample_rate
            && self.channels == audio.num_channels
            && self.context == audio.context_id
            && self.generation == generation
            && self.interruptible == interruptible
    }
    pub fn push(&mut self, audio: &AudioRawFrame) -> Result<()> {
        validate_audio(audio)?;
        self.pending
            .extend(self.resampler.process(&mono(audio)).map_err(Error::media)?);
        Ok(())
    }
    pub fn finish(&mut self) -> Result<()> {
        self.pending
            .extend(self.resampler.flush().map_err(Error::media)?);
        if !self.pending.is_empty() {
            self.pending
                .resize(self.pending.len().next_multiple_of(1920), 0);
        }
        Ok(())
    }
    pub fn packet(&mut self) -> Option<AudioRawFrame> {
        if self.pending.len() < 1920 {
            return None;
        }
        Some(AudioRawFrame {
            audio: self.pending.drain(..1920).collect(),
            sample_rate: 48_000,
            num_channels: 1,
            context_id: self.context.clone(),
        })
    }
    pub fn has_packet(&self) -> bool {
        self.pending.len() >= 1920
    }
}
