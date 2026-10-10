//! Deterministic, multichannel, streaming windowed-sinc resampling.
//!
//! The filter has a Blackman window and 32 zero crossings at 94% of the lower
//! Nyquist frequency. Downsampling widens the kernel to suppress aliasing.
//! It preserves DC by normalizing each fractional-phase kernel. Start/end edges
//! repeat the nearest input sample; output samples are rounded and saturated.
//! This is an independent algorithm, not SOXR or resampy compatibility.

use std::collections::VecDeque;
use std::f64::consts::PI;

use crate::{AudioError, PcmFormat, PcmStreamDecoder, Result};

/// Stateful PCM16LE resampler. Input can be split at arbitrary byte boundaries.
///
/// Rates and channels are fixed until a new instance is created. `process`
/// delays output by the filter's lookahead. `flush` drains and resets the stream;
/// `reset` discards it. No wall-clock inactivity reset is performed.
/// Ratios outside 1/64..=64 are rejected to bound filter work and buffering.
#[derive(Debug, Clone)]
pub struct SincResampler {
    input: PcmFormat,
    output_rate: u32,
    cutoff: f64,
    radius: u64,
    decoder: PcmStreamDecoder,
    samples: VecDeque<i16>,
    first: Vec<i16>,
    base_frame: u64,
    total_frames: u64,
    next_output: u64,
}

impl SincResampler {
    pub fn new(in_rate: u32, out_rate: u32, channels: u16) -> Result<Self> {
        let input = PcmFormat::new(in_rate, channels)?;
        PcmFormat::new(out_rate, channels)?;
        let ratio = f64::from(out_rate) / f64::from(in_rate);
        if !(1.0 / 64.0..=64.0).contains(&ratio) {
            return Err(AudioError::InvalidParameter(
                "resampling ratio must be within 1/64..=64",
            ));
        }
        let cutoff = ratio.min(1.0) * 0.94;
        let radius = (32.0 / cutoff).ceil() as u64;
        Ok(Self {
            input,
            output_rate: out_rate,
            cutoff,
            radius,
            decoder: PcmStreamDecoder::new(input),
            samples: VecDeque::new(),
            first: Vec::new(),
            base_frame: 0,
            total_frames: 0,
            next_output: 0,
        })
    }

    pub fn input_format(&self) -> PcmFormat {
        self.input
    }
    pub fn output_rate(&self) -> u32 {
        self.output_rate
    }
    /// Filter lookahead in input frames, excluding an incomplete byte frame.
    pub fn latency_frames(&self) -> usize {
        if self.input.sample_rate() == self.output_rate {
            0
        } else {
            self.radius as usize
        }
    }
    pub fn buffered_frames(&self) -> usize {
        self.samples.len() / usize::from(self.input.channels())
    }
    pub fn pending_bytes(&self) -> usize {
        self.decoder.pending_bytes()
    }

    pub fn process(&mut self, bytes: &[u8]) -> Result<Vec<u8>> {
        let samples = self.decoder.push(bytes);
        if self.input.sample_rate() == self.output_rate {
            return Ok(crate::encode_pcm16_le(&samples));
        }
        let frames = samples.len() / usize::from(self.input.channels());
        self.total_frames = self
            .total_frames
            .checked_add(frames as u64)
            .ok_or(AudioError::SizeOverflow)?;
        if self.first.is_empty() && !samples.is_empty() {
            self.first
                .extend_from_slice(&samples[..usize::from(self.input.channels())]);
        }
        self.samples.extend(samples);
        let output = self.emit(false)?;
        self.discard_history();
        Ok(output)
    }

    /// Emit the tail so total output length is nearest(input_frames * out / in),
    /// with exact halves rounded up. Incomplete input frames return an error.
    /// A successful flush resets the instance for another independent stream.
    pub fn flush(&mut self) -> Result<Vec<u8>> {
        self.decoder.finish()?;
        let output = self.emit(true)?;
        self.reset();
        Ok(output)
    }

    pub fn reset(&mut self) {
        self.decoder.reset();
        self.samples.clear();
        self.first.clear();
        self.base_frame = 0;
        self.total_frames = 0;
        self.next_output = 0;
    }

    fn position(&self) -> (u64, f64) {
        let numerator = u128::from(self.next_output) * u128::from(self.input.sample_rate());
        let denominator = u128::from(self.output_rate);
        (
            (numerator / denominator) as u64,
            (numerator % denominator) as f64 / f64::from(self.output_rate),
        )
    }

    fn emit(&mut self, final_chunk: bool) -> Result<Vec<u8>> {
        if self.total_frames == 0 {
            return Ok(Vec::new());
        }
        let target = (u128::from(self.total_frames) * u128::from(self.output_rate)
            + u128::from(self.input.sample_rate()) / 2)
            / u128::from(self.input.sample_rate());
        let target = u64::try_from(target).map_err(|_| AudioError::SizeOverflow)?;
        let channels = usize::from(self.input.channels());
        let mut output = Vec::new();
        loop {
            if final_chunk && self.next_output >= target {
                break;
            }
            let (center, fraction) = self.position();
            if !final_chunk && center.saturating_add(self.radius) >= self.total_frames {
                break;
            }
            let mut accum = vec![0.0; channels];
            let mut weight_sum = 0.0;
            for offset in -(self.radius as i64)..=(self.radius as i64) {
                let distance = offset as f64 - fraction;
                if distance.abs() >= self.radius as f64 {
                    continue;
                }
                let scaled = self.cutoff * distance;
                let sinc = if scaled.abs() < 1e-14 {
                    1.0
                } else {
                    (PI * scaled).sin() / (PI * scaled)
                };
                let phase = PI * distance / self.radius as f64;
                let window = 0.42 + 0.5 * phase.cos() + 0.08 * (2.0 * phase).cos();
                let weight = self.cutoff * sinc * window;
                weight_sum += weight;
                let index = i128::from(center) + i128::from(offset);
                for (channel, value) in accum.iter_mut().enumerate() {
                    *value += f64::from(self.sample(index, channel)) * weight;
                }
            }
            for value in accum {
                output.extend_from_slice(
                    &((value / weight_sum).round().clamp(-32768.0, 32767.0) as i16).to_le_bytes(),
                );
            }
            self.next_output = self
                .next_output
                .checked_add(1)
                .ok_or(AudioError::SizeOverflow)?;
        }
        Ok(output)
    }

    fn sample(&self, index: i128, channel: usize) -> i16 {
        if index < 0 {
            return self.first[channel];
        }
        let index = (index as u64).min(self.total_frames - 1);
        let offset =
            (index - self.base_frame) as usize * usize::from(self.input.channels()) + channel;
        self.samples[offset]
    }

    fn discard_history(&mut self) {
        if self.total_frames == 0 {
            return;
        }
        let (center, _) = self.position();
        let keep_from = center
            .saturating_sub(self.radius)
            .min(self.total_frames - 1);
        let count = (keep_from - self.base_frame) as usize * usize::from(self.input.channels());
        self.samples.drain(..count);
        self.base_frame = keep_from;
    }
}

/// Resample a complete, channel-aligned PCM16LE buffer and drain the filter tail.
pub fn resample_pcm16(pcm: &[u8], in_rate: u32, out_rate: u32, channels: u16) -> Result<Vec<u8>> {
    let mut resampler = SincResampler::new(in_rate, out_rate, channels)?;
    resampler.input.validate(pcm)?;
    let mut result = resampler.process(pcm)?;
    result.extend(resampler.flush()?);
    Ok(result)
}
