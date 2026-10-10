use crate::{AudioVolumeTracker, Result, SileroModel, VadError, VadParams};
use std::{collections::VecDeque, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VadState {
    Quiet,
    Starting,
    Speaking,
    Stopping,
}

/// Pipecat's four-state hysteresis. Confirmation is performed after the input
/// batch, matching the source analyzer's behavior for multi-window buffers.
#[derive(Debug)]
pub struct VadStateMachine {
    params: VadParams,
    state: VadState,
    starting_count: u64,
    stopping_count: u64,
    start_frames: u64,
    stop_frames: u64,
}
impl VadStateMachine {
    pub fn new(params: VadParams) -> Result<Self> {
        validate_params(&params)?;
        Ok(Self {
            start_frames: (params.start_secs / 0.032).round_ties_even() as u64,
            stop_frames: (params.stop_secs / 0.032).round_ties_even() as u64,
            params,
            state: VadState::Quiet,
            starting_count: 0,
            stopping_count: 0,
        })
    }
    pub fn params(&self) -> &VadParams {
        &self.params
    }
    pub fn state(&self) -> VadState {
        self.state
    }
    pub fn reset(&mut self) {
        self.state = VadState::Quiet;
        self.starting_count = 0;
        self.stopping_count = 0;
    }
    pub fn update(&mut self, confidence: f64, volume: f64) {
        let speaking = confidence >= self.params.confidence && volume >= self.params.min_volume;
        match (speaking, self.state) {
            (true, VadState::Quiet) => {
                self.state = VadState::Starting;
                self.starting_count = 1;
            }
            (true, VadState::Starting) => self.starting_count += 1,
            (true, VadState::Stopping) => {
                self.state = VadState::Speaking;
                self.stopping_count = 0;
            }
            (false, VadState::Starting) => {
                self.state = VadState::Quiet;
                self.starting_count = 0;
            }
            (false, VadState::Speaking) => {
                self.state = VadState::Stopping;
                self.stopping_count = 1;
            }
            (false, VadState::Stopping) => self.stopping_count += 1,
            _ => {}
        }
    }
    pub fn finish_batch(&mut self) -> VadState {
        if self.state == VadState::Starting && self.starting_count >= self.start_frames {
            self.state = VadState::Speaking;
            self.starting_count = 0;
        }
        if self.state == VadState::Stopping && self.stopping_count >= self.stop_frames {
            self.state = VadState::Quiet;
            self.stopping_count = 0;
        }
        self.state
    }
}
pub fn validate_params(params: &VadParams) -> Result<()> {
    if !params.confidence.is_finite()
        || !(0.0..=1.0).contains(&params.confidence)
        || !params.min_volume.is_finite()
        || !(0.0..=1.0).contains(&params.min_volume)
    {
        return Err(VadError::InvalidParameter(
            "VAD confidence and min_volume must be finite values in [0, 1]",
        ));
    }
    if !params.start_secs.is_finite()
        || !(0.0..=3600.0).contains(&params.start_secs)
        || !params.stop_secs.is_finite()
        || !(0.0..=3600.0).contains(&params.stop_secs)
    {
        return Err(VadError::InvalidParameter(
            "VAD start_secs and stop_secs must be finite values in [0, 3600]",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy)]
pub struct VadAnalysis {
    pub state: VadState,
    pub confidence: f32,
    pub volume: f64,
    pub windows: usize,
}

pub struct SileroVadAnalyzer {
    model: SileroModel,
    machine: VadStateMachine,
    volume: AudioVolumeTracker,
    pending: VecDeque<u8>,
    sample_rate: u32,
    previous_volume: f64,
    last_confidence: f32,
    last_reset: Option<Duration>,
    model_resets: u64,
}
impl SileroVadAnalyzer {
    pub fn new(model: SileroModel, sample_rate: u32, params: VadParams) -> Result<Self> {
        SileroModel::required_samples(sample_rate)?;
        Ok(Self {
            model,
            machine: VadStateMachine::new(params)?,
            volume: AudioVolumeTracker::new(sample_rate)?,
            pending: VecDeque::new(),
            sample_rate,
            previous_volume: 0.0,
            last_confidence: 0.0,
            last_reset: None,
            model_resets: 0,
        })
    }
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }
    pub fn params(&self) -> &VadParams {
        self.machine.params()
    }
    pub fn state(&self) -> VadState {
        self.machine.state()
    }
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }
    pub fn model_reset_count(&self) -> u64 {
        self.model_resets
    }
    pub fn set_params(&mut self, params: VadParams) -> Result<()> {
        self.machine = VadStateMachine::new(params)?;
        Ok(())
    }
    pub fn reset(&mut self) {
        self.model.reset();
        self.machine.reset();
        self.volume.reset();
        self.pending.clear();
        self.previous_volume = 0.0;
        self.last_confidence = 0.0;
        self.last_reset = None;
    }
    /// Buffer any byte boundaries; never pad the last incomplete model window.
    /// `now` is a monotonic clock, used only for the source's five-second reset.
    pub fn analyze_at(&mut self, audio: &[u8], now: Duration) -> Result<VadAnalysis> {
        self.analyze_interruptible_at(audio, now, || false)
    }
    pub(crate) fn analyze_interruptible_at(
        &mut self,
        audio: &[u8],
        now: Duration,
        cancelled: impl Fn() -> bool,
    ) -> Result<VadAnalysis> {
        let samples_per_window = SileroModel::required_samples(self.sample_rate)?;
        let bytes_per_window = samples_per_window * 2;
        let mut windows = 0;
        // Incremental ingestion bounds retained memory even for a large batch.
        for &byte in audio {
            self.pending.push_back(byte);
            if self.pending.len() != bytes_per_window {
                continue;
            }
            if cancelled() {
                self.reset();
                return Err(VadError::Cancelled);
            }
            let samples: Vec<_> = (0..samples_per_window)
                .map(|_| {
                    let low = self.pending.pop_front().expect("full window");
                    let high = self.pending.pop_front().expect("full window");
                    i16::from_le_bytes([low, high])
                })
                .collect();
            self.last_confidence = self.model.predict_pcm16(&samples, self.sample_rate)?;
            if self
                .last_reset
                .is_none_or(|last| now.saturating_sub(last) >= Duration::from_secs(5))
            {
                self.model.reset();
                self.last_reset = Some(now);
                self.model_resets += 1;
            }
            self.volume.update(&samples);
            self.previous_volume = 0.2 * self.volume.volume() + 0.8 * self.previous_volume;
            self.machine
                .update(f64::from(self.last_confidence), self.previous_volume);
            windows += 1;
        }
        let state = if windows > 0 {
            self.machine.finish_batch()
        } else {
            self.machine.state()
        };
        Ok(VadAnalysis {
            state,
            confidence: self.last_confidence,
            volume: self.previous_volume,
            windows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{cell::Cell, path::PathBuf};

    #[test]
    fn cancellation_stops_between_fixed_size_inference_windows() {
        let runtime = crate::RuntimeOptions {
            library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from("/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0")
                }),
        };
        let path = std::env::var_os("PIPECAT_SILERO_MODEL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
            });
        let model = SileroModel::load(&runtime, path).unwrap();
        let mut analyzer = SileroVadAnalyzer::new(model, 16000, VadParams::default()).unwrap();
        let calls = Cell::new(0);
        let result =
            analyzer.analyze_interruptible_at(&vec![0; 32000 * 60], Duration::ZERO, || {
                calls.set(calls.get() + 1);
                calls.get() > 1
            });
        assert!(matches!(result, Err(VadError::Cancelled)));
        assert_eq!(calls.get(), 2);
        assert_eq!(analyzer.model_reset_count(), 1);
        assert_eq!(analyzer.pending_bytes(), 0);
    }
}
