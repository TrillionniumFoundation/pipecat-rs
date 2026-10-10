use crate::{
    native::Engine, Language, Model, MoonshineConfig, MoonshineError, MoonshineSettings, Result,
    SAMPLE_RATE,
};
use pipecat_core::AudioRawFrame;
use serde::{Deserialize, Serialize};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Cancellation suppresses results and prevents queued native work. The vendor
/// STT API has no abort hook: an in-flight call must return before native memory
/// can be released. No thread is detached and no hard latency guarantee is made.
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
    pub(crate) fn check(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(MoonshineError::Cancelled)
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptLine {
    pub text: String,
    pub start_seconds: f32,
    pub duration_seconds: f32,
    pub id: u64,
    pub is_complete: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transcript {
    pub text: String,
    /// Configured model language, not language detection.
    pub language: Language,
    pub model: Model,
    pub lines: Vec<TranscriptLine>,
    pub audio_duration_seconds: f64,
}
struct Loaded {
    handle: i32,
    settings: MoonshineSettings,
    // Native ONNX sessions retain pointers into these exact verified buffers.
    _files: Vec<(String, Vec<u8>)>,
}
/// Synchronous native model. Use MoonshineService on async execution paths.
/// Runtime changes load a candidate before freeing the previous model. A failed
/// load leaves it intact but returns an error, never a mislabelled transcript.
pub struct MoonshineModel {
    loaded: Loaded,
    engine: Engine,
    config: MoonshineConfig,
}
impl MoonshineModel {
    pub fn load(config: MoonshineConfig) -> Result<Self> {
        config.validate()?;
        let engine = Engine::load(&config.libraries)?;
        let loaded = load_candidate(&engine, &config, &config.initial_settings)?;
        Ok(Self {
            loaded,
            engine,
            config,
        })
    }
    pub fn settings(&self) -> &MoonshineSettings {
        &self.loaded.settings
    }
    pub fn transcribe(
        &mut self,
        audio: &AudioRawFrame,
        settings: &MoonshineSettings,
        cancel: &Cancellation,
    ) -> Result<Transcript> {
        let samples = decode_pcm(audio, self.config.max_samples())?;
        self.transcribe_samples(&samples, settings, cancel, &Cancellation::new())
    }
    pub(crate) fn transcribe_samples(
        &mut self,
        samples: &[f32],
        settings: &MoonshineSettings,
        cancel: &Cancellation,
        stop: &Cancellation,
    ) -> Result<Transcript> {
        cancel.check()?;
        stop.check()?;
        let settings = settings.resolved();
        if self.loaded.settings != settings {
            let candidate = load_candidate(&self.engine, &self.config, &settings)?;
            let previous = std::mem::replace(&mut self.loaded, candidate);
            self.engine.free_model(previous.handle);
        }
        cancel.check()?;
        stop.check()?;
        let lines = self.engine.transcribe(
            self.loaded.handle,
            samples,
            self.config.max_transcript_bytes,
        )?;
        cancel.check()?;
        stop.check()?;
        let text = lines
            .iter()
            .map(|line| line.text.as_str())
            .collect::<Vec<_>>()
            .join(" ")
            .trim()
            .to_owned();
        Ok(Transcript {
            text,
            language: settings.language,
            model: settings.model,
            lines,
            audio_duration_seconds: samples.len() as f64 / f64::from(SAMPLE_RATE),
        })
    }
}
impl Drop for MoonshineModel {
    fn drop(&mut self) {
        self.engine.free_model(self.loaded.handle);
    }
}
fn load_candidate(
    engine: &Engine,
    config: &MoonshineConfig,
    settings: &MoonshineSettings,
) -> Result<Loaded> {
    let settings = settings.resolved();
    let bundle = config.bundle(&settings)?;
    let mut files = Vec::with_capacity(bundle.files.len());
    let mut remaining = config.max_model_bytes;
    for file in &bundle.files {
        let bytes = file.artifact.read(remaining)?;
        remaining -= bytes.len();
        files.push((file.name.clone(), bytes));
    }
    let handle = engine.load_model(&files, bundle.model as u32)?;
    Ok(Loaded {
        handle,
        settings,
        _files: files,
    })
}
pub(crate) fn validate_audio(audio: &AudioRawFrame, max_samples: usize) -> Result<()> {
    if audio.sample_rate != SAMPLE_RATE || audio.num_channels != 1 {
        return Err(MoonshineError::Invalid(
            "Moonshine requires 16000 Hz mono signed 16-bit little-endian PCM",
        ));
    }
    if audio.audio.is_empty() || !audio.audio.len().is_multiple_of(2) {
        return Err(MoonshineError::Invalid(
            "PCM must contain nonempty whole i16 samples",
        ));
    }
    if audio.audio.len() / 2 > max_samples {
        return Err(MoonshineError::Invalid(
            "audio exceeds configured segment duration",
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
        .map(|b| f32::from(i16::from_le_bytes(*b)) / 32768.0)
        .collect())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pcm_normalization_and_validation() {
        let audio = AudioRawFrame::new(
            [i16::MIN, -1, 0, 1, i16::MAX]
                .into_iter()
                .flat_map(i16::to_le_bytes)
                .collect(),
            SAMPLE_RATE,
            1,
        )
        .unwrap();
        assert_eq!(
            decode_pcm(&audio, 5).unwrap(),
            [-1.0, -1.0 / 32768.0, 0.0, 1.0 / 32768.0, 32767.0 / 32768.0]
        );
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
}
