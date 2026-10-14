// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause AND Apache-2.0
//
// Portions derived from Hugging Face Transformers' Whisper feature extraction:
// Copyright 2022 The HuggingFace Inc. team.
// Copyright 2023 The HuggingFace Inc. team and the librosa & torchaudio authors.
// Licensed under Apache-2.0; see LICENSE-APACHE. Adapted to native Rust.

//! Native Smart Turn v3 inference and Whisper-compatible log-mel preprocessing.
//!
//! The feature equations follow `audio/turn/smart_turn/_whisper_features.py`.
//! Its Whisper feature extraction is derived from Hugging Face Transformers
//! (Copyright 2022-2023 Hugging Face, librosa and torchaudio authors), Apache-2.0.

use std::{f64::consts::PI, path::Path, sync::Arc};

use ndarray::Array3;
use ort::session::{builder::GraphOptimizationLevel, RunOptions, Session};
use rustfft::{num_complex::Complex, Fft, FftPlanner};

use crate::{Result, RuntimeOptions, VadError};

/// The sample rate accepted by the Smart Turn v3 model.
pub const SMART_TURN_SAMPLE_RATE: u32 = 16_000;
/// Eight seconds of model input.
pub const SMART_TURN_SAMPLES: usize = 128_000;
/// Number of Slaney mel bands.
pub const SMART_TURN_MELS: usize = 80;
/// Number of output spectrogram frames.
pub const SMART_TURN_FRAMES: usize = 800;
const FFT_LENGTH: usize = 400;
const HOP_LENGTH: usize = 160;
const FREQUENCY_BINS: usize = FFT_LENGTH / 2 + 1;

/// Reusable constants and FFT plan for Smart Turn's Whisper features.
///
/// Waveform normalization is float32. Windowing, FFT, power, mel projection,
/// and logarithms use float64 before the final float32 conversion.
pub struct WhisperFeatureExtractor {
    fft: Arc<dyn Fft<f64>>,
    window: [f64; FFT_LENGTH],
    mel_filters: Vec<Vec<(usize, f64)>>,
}

impl Default for WhisperFeatureExtractor {
    fn default() -> Self {
        Self::new()
    }
}

impl WhisperFeatureExtractor {
    /// Precompute the periodic Hann window, Slaney filters and FFT plan.
    pub fn new() -> Self {
        let fft = FftPlanner::<f64>::new().plan_fft_forward(FFT_LENGTH);
        let window = std::array::from_fn(|i| {
            // np.hanning(401)[:-1]: retain NumPy's centered cosine expression.
            0.5 + 0.5 * (PI * (2.0 * i as f64 - FFT_LENGTH as f64) / FFT_LENGTH as f64).cos()
        });
        let mel_max = hertz_to_mel(8_000.0);
        let frequencies: Vec<f64> = (0..SMART_TURN_MELS + 2)
            .map(|i| mel_to_hertz(i as f64 * (mel_max / (SMART_TURN_MELS + 1) as f64)))
            .collect();
        let mel_filters = (0..SMART_TURN_MELS)
            .map(|band| {
                let left = frequencies[band];
                let center = frequencies[band + 1];
                let right = frequencies[band + 2];
                let area = 2.0 / (right - left);
                (0..FREQUENCY_BINS)
                    .filter_map(|bin| {
                        let frequency = bin as f64 * 40.0;
                        let weight = ((frequency - left) / (center - left))
                            .min((right - frequency) / (right - center))
                            .max(0.0)
                            * area;
                        (weight > 0.0).then_some((bin, weight))
                    })
                    .collect()
            })
            .collect();
        Self {
            fft,
            window,
            mel_filters,
        }
    }

    /// Compute row-major `(80, 800)` log-mel features.
    ///
    /// Like the Python feature function, this pads short audio at the **end**
    /// and retains the **first** eight seconds of long audio. Endpoint inference
    /// instead prepares its last-eight-second, front-padded window before calling
    /// this method. Non-finite input is rejected.
    pub fn extract(&self, audio: &[f32], do_normalize: bool) -> Result<Vec<f32>> {
        let length = audio.len().min(SMART_TURN_SAMPLES);
        if audio[..length].iter().any(|sample| !sample.is_finite()) {
            return Err(VadError::Runtime(
                "Smart Turn audio contains a non-finite sample".into(),
            ));
        }
        let mut waveform = vec![0.0_f32; SMART_TURN_SAMPLES];
        waveform[..length].copy_from_slice(&audio[..length]);
        if do_normalize {
            normalize(&mut waveform)?;
        }

        let mut spectrum = vec![Complex::new(0.0, 0.0); FFT_LENGTH];
        let mut scratch = vec![Complex::new(0.0, 0.0); self.fft.get_inplace_scratch_len()];
        let mut power = [0.0_f64; FREQUENCY_BINS];
        let mut log_mel = vec![0.0_f64; SMART_TURN_MELS * SMART_TURN_FRAMES];
        let mut maximum = f64::NEG_INFINITY;
        // Centered padding produces 801 frames. The feature function discards
        // the last frame before finding the log-mel maximum.
        for frame in 0..SMART_TURN_FRAMES {
            for (i, value) in spectrum.iter_mut().enumerate() {
                let centered_index = (frame * HOP_LENGTH + i) as isize - (FFT_LENGTH / 2) as isize;
                let index = if centered_index < 0 {
                    -centered_index as usize
                } else if centered_index as usize >= SMART_TURN_SAMPLES {
                    2 * SMART_TURN_SAMPLES - 2 - centered_index as usize
                } else {
                    centered_index as usize
                };
                *value = Complex::new(waveform[index] as f64 * self.window[i], 0.0);
            }
            self.fft.process_with_scratch(&mut spectrum, &mut scratch);
            for (power, value) in power.iter_mut().zip(&spectrum) {
                // NumPy computes abs(complex) ** 2, rather than norm_sqr().
                *power = value.re.hypot(value.im).powi(2);
            }
            for (band, filter) in self.mel_filters.iter().enumerate() {
                let energy: f64 = filter
                    .iter()
                    .map(|&(bin, weight)| power[bin] * weight)
                    .sum();
                let value = energy.max(1e-10).log10();
                log_mel[band * SMART_TURN_FRAMES + frame] = value;
                maximum = maximum.max(value);
            }
        }
        let floor = maximum - 8.0;
        Ok(log_mel
            .into_iter()
            .map(|value| ((value.max(floor) + 4.0) / 4.0) as f32)
            .collect())
    }
}

fn hertz_to_mel(frequency: f64) -> f64 {
    if frequency >= 1_000.0 {
        15.0 + (frequency / 1_000.0).ln() * (27.0 / 6.4_f64.ln())
    } else {
        3.0 * frequency / 200.0
    }
}

fn mel_to_hertz(mel: f64) -> f64 {
    if mel >= 15.0 {
        1_000.0 * ((6.4_f64.ln() / 27.0) * (mel - 15.0)).exp()
    } else {
        200.0 * mel / 3.0
    }
}

fn normalize(waveform: &mut [f32]) -> Result<()> {
    let count = waveform.len() as f32;
    let mean = pairwise_sum(waveform) / count;
    let squared: Vec<f32> = waveform
        .iter()
        .map(|&sample| {
            let delta = sample - mean;
            delta * delta
        })
        .collect();
    let variance = pairwise_sum(&squared) / count;
    if !mean.is_finite() || !variance.is_finite() {
        return Err(VadError::InvalidParameter(
            "Smart Turn float32 audio normalization overflow",
        ));
    }
    let denominator = (variance + 1e-7_f32).sqrt();
    for sample in waveform {
        *sample = (*sample - mean) / denominator;
    }
    Ok(())
}

// Float32 reductions preserve NumPy's eight-lane, 128-element pairwise order.
fn pairwise_sum(values: &[f32]) -> f32 {
    if values.len() < 8 {
        values.iter().fold(-0.0_f32, |total, value| total + value)
    } else if values.len() <= 128 {
        let mut lanes = [0.0_f32; 8];
        lanes.copy_from_slice(&values[..8]);
        let end = values.len() - values.len() % 8;
        for chunk in values[8..end].as_chunks::<8>().0 {
            for (lane, value) in lanes.iter_mut().zip(chunk) {
                *lane += value;
            }
        }
        let mut result = ((lanes[0] + lanes[1]) + (lanes[2] + lanes[3]))
            + ((lanes[4] + lanes[5]) + (lanes[6] + lanes[7]));
        for value in &values[end..] {
            result += value;
        }
        result
    } else {
        let half = (values.len() / 2) & !7;
        pairwise_sum(&values[..half]) + pairwise_sum(&values[half..])
    }
}

fn endpoint_window(samples: &[i16]) -> Vec<f32> {
    let tail = &samples[samples.len().saturating_sub(SMART_TURN_SAMPLES)..];
    let mut waveform = vec![0.0_f32; SMART_TURN_SAMPLES];
    let start = SMART_TURN_SAMPLES - tail.len();
    for (destination, &sample) in waveform[start..].iter_mut().zip(tail) {
        *destination = sample as f32 / 32768.0;
    }
    waveform
}

/// Smart Turn prediction shared by local and explicitly configured predictors.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SmartTurnPrediction {
    /// Model probability that the speaker's turn is complete.
    pub probability: f32,
    /// Completion decision. The local ONNX model uses `probability > 0.5`;
    /// remote predictors may instead use an authoritative server decision.
    pub complete: bool,
}

/// CPU ONNX Smart Turn v3 endpoint inference for mono 16 kHz PCM16.
///
/// Both runtime and model locations must be supplied explicitly. Loading never
/// downloads assets. This model wrapper does not implement turn-controller
/// buffering, silence policies, or non-16-kHz resampling.
pub struct SmartTurnModel {
    session: Session,
    features: WhisperFeatureExtractor,
}

impl SmartTurnModel {
    /// Load an explicit Smart Turn v3 ONNX graph using one CPU inference thread.
    pub fn load(options: &RuntimeOptions, model_path: impl AsRef<Path>) -> Result<Self> {
        let model_path = model_path.as_ref();
        if !model_path.is_file() {
            return Err(VadError::Runtime(format!(
                "Smart Turn model does not exist: {}",
                model_path.display()
            )));
        }
        crate::runtime::initialize(options)?;
        let session = Session::builder()
            .and_then(|builder| builder.with_parallel_execution(false))
            .and_then(|builder| builder.with_inter_threads(1))
            .and_then(|builder| builder.with_intra_threads(1))
            .and_then(|builder| builder.with_optimization_level(GraphOptimizationLevel::Level3))
            .and_then(|builder| builder.commit_from_file(model_path))
            .map_err(|error| VadError::Runtime(format!("loading Smart Turn model: {error}")))?;
        if session.inputs.len() != 1 || session.inputs[0].name != "input_features" {
            return Err(VadError::Runtime(
                "Smart Turn model must have a single input named input_features".into(),
            ));
        }
        if session.outputs.is_empty() {
            return Err(VadError::Runtime("Smart Turn model has no output".into()));
        }
        Ok(Self {
            session,
            features: WhisperFeatureExtractor::new(),
        })
    }

    /// Analyze the final eight seconds, padding short audio at the beginning.
    ///
    /// Rates other than 16 kHz fail explicitly rather than silently changing
    /// inference behavior through an unverified resampler.
    pub fn predict_pcm16(&self, samples: &[i16], sample_rate: u32) -> Result<SmartTurnPrediction> {
        self.predict_pcm16_inner(samples, sample_rate, None)
    }

    /// The cancellable inference entry point used by the serial analyzer owner.
    pub fn predict_pcm16_cancellable(
        &self,
        samples: &[i16],
        sample_rate: u32,
        cancellation: &crate::SmartTurnCancellation,
    ) -> Result<SmartTurnPrediction> {
        self.predict_pcm16_inner(samples, sample_rate, Some(cancellation))
    }

    fn predict_pcm16_inner(
        &self,
        samples: &[i16],
        sample_rate: u32,
        cancellation: Option<&crate::SmartTurnCancellation>,
    ) -> Result<SmartTurnPrediction> {
        if cancellation.is_some_and(|token| token.is_cancelled()) {
            return Err(VadError::Cancelled);
        }
        if sample_rate != SMART_TURN_SAMPLE_RATE {
            return Err(VadError::Runtime(format!(
                "Smart Turn requires mono 16000 Hz PCM16, got {sample_rate} Hz"
            )));
        }
        let waveform = endpoint_window(samples);
        let log_mel = self.features.extract(&waveform, true)?;
        let input = Array3::from_shape_vec((1, SMART_TURN_MELS, SMART_TURN_FRAMES), log_mel)
            .map_err(|error| VadError::Runtime(format!("Smart Turn feature shape: {error}")))?;
        let inputs = ort::inputs!["input_features" => input]
            .map_err(|error| VadError::Runtime(format!("Smart Turn input tensor: {error}")))?;
        let run_options = Arc::new(RunOptions::new()?);
        if let Some(cancellation) = cancellation {
            cancellation.install(run_options.clone())?;
        }
        let output_result = self.session.run_with_options(inputs, &run_options);
        if let Some(cancellation) = cancellation {
            cancellation.uninstall();
            if cancellation.is_cancelled() {
                return Err(VadError::Cancelled);
            }
        }
        let outputs = output_result
            .map_err(|error| VadError::Runtime(format!("Smart Turn inference: {error}")))?;
        let output = outputs[0].try_extract_tensor::<f32>().map_err(|error| {
            VadError::Runtime(format!("Smart Turn probability tensor: {error}"))
        })?;
        if output.len() != 1 {
            return Err(VadError::Runtime(format!(
                "Smart Turn model must return one probability, got {} values",
                output.len()
            )));
        }
        let probability = *output.iter().next().expect("length was checked");
        if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
            return Err(VadError::Runtime(format!(
                "Smart Turn model returned an invalid probability: {probability}"
            )));
        }
        Ok(SmartTurnPrediction {
            probability,
            complete: probability > 0.5,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_f32(bytes: &[u8]) -> Vec<f32> {
        assert_eq!(bytes.len() % 4, 0);
        bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|&chunk| f32::from_le_bytes(chunk))
            .collect()
    }

    fn decode_pcm16(bytes: &[u8]) -> Vec<i16> {
        assert_eq!(bytes.len() % 2, 0);
        bytes
            .as_chunks::<2>()
            .0
            .iter()
            .map(|&chunk| i16::from_le_bytes(chunk))
            .collect()
    }

    fn assert_features_match(samples: &[i16], reference_bytes: &[u8]) {
        let extractor = WhisperFeatureExtractor::new();
        let actual = extractor.extract(&endpoint_window(samples), true).unwrap();
        let expected = decode_f32(reference_bytes);
        assert_eq!(actual.len(), SMART_TURN_MELS * SMART_TURN_FRAMES);
        assert_eq!(actual.len(), expected.len());
        let (index, max_error) = actual
            .iter()
            .zip(&expected)
            .enumerate()
            .map(|(index, (actual, expected))| (index, (actual - expected).abs()))
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .unwrap();
        eprintln!("Whisper feature maximum absolute error: {max_error}");
        assert!(
            max_error <= 2e-6,
            "feature {index}: actual={}, expected={}, maximum error={max_error}",
            actual[index],
            expected[index]
        );
    }

    #[test]
    fn whisper_matches_python_speech_features() {
        assert_features_match(
            &decode_pcm16(include_bytes!("../tests/fixtures/smart_turn_speech.pcm16")),
            include_bytes!("../tests/fixtures/smart_turn_speech.f32"),
        );
    }

    #[test]
    fn whisper_matches_python_tone_features() {
        assert_features_match(
            &decode_pcm16(include_bytes!("../tests/fixtures/smart_turn_tone.pcm16")),
            include_bytes!("../tests/fixtures/smart_turn_tone.f32"),
        );
    }

    #[test]
    fn whisper_matches_python_silence_features() {
        assert_features_match(
            &vec![0; 16_000],
            include_bytes!("../tests/fixtures/smart_turn_silence.f32"),
        );
    }

    #[test]
    fn endpoint_window_pads_front_and_keeps_last_eight_seconds() {
        let short = endpoint_window(&[i16::MIN, i16::MAX]);
        assert!(short[..SMART_TURN_SAMPLES - 2]
            .iter()
            .all(|&sample| sample == 0.0));
        assert_eq!(&short[SMART_TURN_SAMPLES - 2..], &[-1.0, 32767.0 / 32768.0]);
        let mut long = vec![100; SMART_TURN_SAMPLES + 2];
        long[..2].fill(200);
        assert!(endpoint_window(&long)
            .iter()
            .all(|&sample| sample == 100.0 / 32768.0));
    }

    #[test]
    fn feature_function_pads_end_and_keeps_first_eight_seconds() {
        let extractor = WhisperFeatureExtractor::new();
        let mut padded = vec![0.0; SMART_TURN_SAMPLES];
        padded[0] = 0.5;
        assert_eq!(
            extractor.extract(&[0.5], false).unwrap(),
            extractor.extract(&padded, false).unwrap()
        );
        let expected = extractor.extract(&padded, true).unwrap();
        padded.push(1.0);
        assert_eq!(extractor.extract(&padded, true).unwrap(), expected);
        assert!(extractor.extract(&[f32::NAN], true).is_err());
        assert!(extractor.extract(&[f32::INFINITY], false).is_err());
        assert!(extractor
            .extract(&vec![f32::MAX; SMART_TURN_SAMPLES], true)
            .is_err());
        assert!(extractor.extract(&[f32::MAX], true).is_err());
    }

    #[test]
    fn empty_audio_has_silence_features() {
        let features = WhisperFeatureExtractor::new().extract(&[], true).unwrap();
        assert!(features.iter().all(|&sample| sample == -1.5));
    }

    /// Uses configured assets or the supported Linux checkout paths; never downloads.
    #[test]
    fn smart_turn_real_model_matches_python_probabilities() {
        let options = RuntimeOptions {
            library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
                .map(Into::into)
                .unwrap_or_else(|| "/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0".into()),
        };
        let model_path = std::env::var_os("PIPECAT_SMART_TURN_MODEL")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                Path::new(env!("CARGO_MANIFEST_DIR")).join(
                    "../../../src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx",
                )
            });
        let model = SmartTurnModel::load(
            &options,
            model_path,
        )
        .expect("real Smart Turn assets are required; set PIPECAT_ONNX_RUNTIME and PIPECAT_SMART_TURN_MODEL to valid local files");
        let cases = [
            (
                "speech",
                decode_pcm16(include_bytes!("../tests/fixtures/smart_turn_speech.pcm16")),
                include_str!("../tests/fixtures/smart_turn_speech.probability.txt"),
            ),
            (
                "tone",
                decode_pcm16(include_bytes!("../tests/fixtures/smart_turn_tone.pcm16")),
                include_str!("../tests/fixtures/smart_turn_tone.probability.txt"),
            ),
            (
                "silence",
                vec![0; 16_000],
                include_str!("../tests/fixtures/smart_turn_silence.probability.txt"),
            ),
        ];
        for (name, samples, reference) in cases {
            let expected: f32 = reference.trim().parse().unwrap();
            let actual = model
                .predict_pcm16(&samples, SMART_TURN_SAMPLE_RATE)
                .unwrap();
            eprintln!("{name}: native={}, Python={expected}", actual.probability);
            assert!(
                (actual.probability - expected).abs() <= 1e-5,
                "{name}: actual={}, expected={expected}",
                actual.probability
            );
            assert_eq!(actual.complete, expected > 0.5, "{name}");
        }
        assert!(model.predict_pcm16(&[], 8_000).is_err());
        assert!(model.predict_pcm16(&[], 24_000).is_err());
    }
}
