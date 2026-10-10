use crate::{
    DecodingStrategy, Language, Result, WhisperConfig, WhisperError, WhisperSettings, SAMPLE_RATE,
};
use pipecat_core::AudioRawFrame;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use whisper_rs::{
    FullParams, SamplingStrategy, WhisperContext, WhisperContextParameters, WhisperState,
};

/// Cooperative native cancellation. Clones cancel the same operation.
#[derive(Debug, Clone, Default)]
pub struct Cancellation(Arc<AtomicBool>);
impl Cancellation {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptSegment {
    pub text: String,
    pub start_ms: u64,
    pub end_ms: u64,
    pub no_speech_probability: f32,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transcript {
    pub text: String,
    /// Detected input language (also when translating to English).
    pub language: Option<Language>,
    pub segments: Vec<TranscriptSegment>,
    pub audio_duration_seconds: f64,
}

/// Synchronous model API. Prefer WhisperService in async code: loading and
/// inference are CPU-bound and must not run on the async executor.
pub struct WhisperModel {
    state: WhisperState,
    multilingual: bool,
    config: WhisperConfig,
}
impl WhisperModel {
    pub fn load(config: WhisperConfig) -> Result<Self> {
        config.validate()?;
        let mut parameters = WhisperContextParameters::default();
        parameters.use_gpu(false);
        let context = WhisperContext::new_with_params(&config.model_path, parameters)?;
        let multilingual = context.is_multilingual();
        let state = context.create_state()?;
        Ok(Self {
            state,
            multilingual,
            config,
        })
    }
    pub fn is_multilingual(&self) -> bool {
        self.multilingual
    }
    pub fn transcribe(
        &mut self,
        audio: &AudioRawFrame,
        settings: &WhisperSettings,
        cancel: &Cancellation,
    ) -> Result<Transcript> {
        let samples = decode_pcm(audio, self.config.max_samples())?;
        self.transcribe_samples(&samples, settings, cancel, &Cancellation::new())
    }
    pub(crate) fn transcribe_samples(
        &mut self,
        samples: &[f32],
        settings: &WhisperSettings,
        cancel: &Cancellation,
        shutdown: &Cancellation,
    ) -> Result<Transcript> {
        settings.validate_model(self.multilingual)?;
        if cancel.is_cancelled() || shutdown.is_cancelled() {
            return Err(WhisperError::Cancelled);
        }
        let sampling = match settings.strategy {
            DecodingStrategy::Greedy { best_of } => SamplingStrategy::Greedy {
                best_of: best_of.into(),
            },
            DecodingStrategy::BeamSearch { beam_size } => SamplingStrategy::BeamSearch {
                beam_size: beam_size.into(),
                patience: -1.0,
            },
        };
        let mut params = FullParams::new(sampling);
        params.set_n_threads(self.config.inference_threads as i32);
        // English-only models lack language tokens. Asking native auto-detection
        // on such a model can return an unrelated language ID despite English text.
        params.set_language(if self.multilingual {
            settings.language.as_ref().map(Language::as_str)
        } else {
            Some("en")
        });
        params.set_translate(settings.translate_to_english);
        params.set_temperature(settings.temperature);
        params.set_no_context(true);
        params.set_print_special(false);
        params.set_print_progress(false);
        params.set_print_realtime(false);
        params.set_print_timestamps(false);
        if let Some(prompt) = &settings.initial_prompt {
            params.set_initial_prompt(prompt);
        }
        let (local, global) = (cancel.clone(), shutdown.clone());
        params.set_abort_callback_safe(move || local.is_cancelled() || global.is_cancelled());
        // Native Whisper rejects very short input. Zero-padding preserves the
        // requested samples and permits bounded sub-second speech segments.
        let padded;
        let input = if samples.len() < SAMPLE_RATE as usize {
            padded = {
                let mut data = samples.to_vec();
                data.resize(SAMPLE_RATE as usize, 0.0);
                data
            };
            &padded[..]
        } else {
            samples
        };
        let result = self.state.full(params, input);
        if cancel.is_cancelled() || shutdown.is_cancelled() {
            return Err(WhisperError::Cancelled);
        }
        result?;
        let mut segments = Vec::new();
        for segment in self.state.as_iter() {
            let probability = segment.no_speech_probability();
            if !probability.is_finite() {
                return Err(WhisperError::Worker(
                    "nonfinite no-speech probability".into(),
                ));
            }
            if settings
                .no_speech_probability
                .is_some_and(|threshold| probability >= threshold)
            {
                continue;
            }
            let text = segment.to_str()?.trim().to_owned();
            if text.is_empty() {
                continue;
            }
            segments.push(TranscriptSegment {
                text,
                start_ms: segment.start_timestamp().max(0) as u64 * 10,
                end_ms: segment.end_timestamp().max(0) as u64 * 10,
                no_speech_probability: probability,
            });
        }
        let language = whisper_rs::get_lang_str(self.state.full_lang_id_from_state())
            .and_then(|code| code.parse().ok());
        Ok(Transcript {
            text: segments
                .iter()
                .map(|s| s.text.as_str())
                .collect::<Vec<_>>()
                .join(" "),
            language,
            segments,
            audio_duration_seconds: samples.len() as f64 / f64::from(SAMPLE_RATE),
        })
    }
}
pub(crate) fn validate_audio(audio: &AudioRawFrame, max_samples: usize) -> Result<()> {
    if audio.sample_rate != SAMPLE_RATE || audio.num_channels != 1 {
        return Err(WhisperError::Invalid(
            "Whisper requires 16000 Hz mono signed 16-bit little-endian PCM",
        ));
    }
    if audio.audio.is_empty() || !audio.audio.len().is_multiple_of(2) {
        return Err(WhisperError::Invalid(
            "PCM must contain nonempty whole i16 samples",
        ));
    }
    if audio.audio.len() / 2 > max_samples {
        return Err(WhisperError::Invalid(
            "audio exceeds the configured segment duration",
        ));
    }
    Ok(())
}
pub(crate) fn decode_pcm(audio: &AudioRawFrame, max_samples: usize) -> Result<Vec<f32>> {
    validate_audio(audio, max_samples)?;
    Ok(audio
        .audio
        .as_chunks::<2>()
        .0
        .iter()
        .map(|bytes| f32::from(i16::from_le_bytes([bytes[0], bytes[1]])) / 32768.0)
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pcm_normalization_is_little_endian_signed_and_exact() {
        let samples = [i16::MIN, -1, 0, 1, i16::MAX];
        let audio = AudioRawFrame::new(
            samples.into_iter().flat_map(i16::to_le_bytes).collect(),
            SAMPLE_RATE,
            1,
        )
        .unwrap();
        assert_eq!(
            decode_pcm(&audio, 5).unwrap(),
            [-1.0, -1.0 / 32768.0, 0.0, 1.0 / 32768.0, 32767.0 / 32768.0]
        );
    }
    #[test]
    fn pcm_requires_mono_16k_nonempty_aligned_and_bounded() {
        for (bytes, rate, channels, max) in [
            (vec![], 16000, 1, 1),
            (vec![0], 16000, 1, 1),
            (vec![0; 4], 16000, 2, 2),
            (vec![0; 2], 48000, 1, 1),
            (vec![0; 4], 16000, 1, 1),
        ] {
            assert!(decode_pcm(
                &AudioRawFrame {
                    audio: bytes,
                    sample_rate: rate,
                    num_channels: channels,
                    context_id: None
                },
                max
            )
            .is_err());
        }
    }
    #[test]
    fn cancellation_clones_share_state() {
        let first = Cancellation::new();
        let second = first.clone();
        assert!(!first.is_cancelled());
        second.cancel();
        assert!(first.is_cancelled());
    }
}
