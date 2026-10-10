use crate::{
    config::normalized, native, Cancellation, PiperConfig, PiperError, PiperSettings, Result,
    VoiceConfig,
};
use ndarray::{arr1, Array2};
use ort::{
    execution_providers::CPUExecutionProvider,
    session::{RunOptions, Session},
    tensor::TensorElementType,
    value::ValueType,
};
use std::sync::Arc;

pub(crate) struct Model {
    config: PiperConfig,
    voice: VoiceConfig,
    session: Session,
}
impl Model {
    pub(crate) fn load(config: PiperConfig, voice: VoiceConfig) -> Result<Self> {
        if voice.phoneme_type == "espeak" {
            #[cfg(not(feature = "espeak-gpl"))]
            return Err(PiperError::Unsupported(
                "espeak-gpl feature is required for this model",
            ));
            #[cfg(feature = "espeak-gpl")]
            native::initialize_espeak(config.espeak.as_ref().ok_or(PiperError::Invalid(
                "explicit eSpeak library/data are required",
            ))?)?;
        }
        native::runtime(&config.runtime)?;
        // Load the exact verified bytes to avoid a model-path hash/load race.
        let bytes = config.model.read(config.max_model_bytes)?;
        let session = Session::builder()
            .and_then(|b| b.with_execution_providers([CPUExecutionProvider::default().build()]))
            .and_then(|b| b.with_intra_threads(config.inference_threads))
            .and_then(|b| b.with_inter_threads(1))
            .and_then(|b| b.with_parallel_execution(false))
            .and_then(|b| b.with_memory_pattern(false))
            .and_then(|b| b.commit_from_memory(&bytes))
            .map_err(|e| PiperError::Model(e.to_string()))?;
        let mut expected = vec![
            ("input", TensorElementType::Int64, vec![1, -1]),
            ("input_lengths", TensorElementType::Int64, vec![1]),
            ("scales", TensorElementType::Float32, vec![3]),
        ];
        if voice.num_speakers > 1 {
            expected.push(("sid", TensorElementType::Int64, vec![1]));
        }
        if session.inputs.len() != expected.len() || session.outputs.len() != 1 {
            return Err(PiperError::Model("unexpected Piper tensor count".into()));
        }
        for (name, ty, shape) in expected {
            let input = session
                .inputs
                .iter()
                .find(|i| i.name == name)
                .ok_or_else(|| PiperError::Model(format!("missing {name} input")))?;
            check_type(&input.input_type, ty, &shape)?;
            if name == "input"
                && matches!(&input.input_type, ValueType::Tensor { dimensions, .. } if dimensions[1] >= 0)
            {
                return Err(PiperError::Unsupported("fixed-width Piper phoneme input"));
            }
        }
        if session.outputs[0].name != "output" {
            return Err(PiperError::Model("missing output tensor".into()));
        }
        if check_type(
            &session.outputs[0].output_type,
            TensorElementType::Float32,
            &[1, 1, -1],
        )
        .is_err()
        {
            check_type(
                &session.outputs[0].output_type,
                TensorElementType::Float32,
                &[1, 1, 1, -1],
            )?;
        }
        Ok(Self {
            config,
            voice,
            session,
        })
    }
    pub(crate) fn synthesize(
        &mut self,
        text: &str,
        settings: &PiperSettings,
        cancel: &Cancellation,
        stop: &Cancellation,
    ) -> Result<Vec<u8>> {
        cancel.check()?;
        stop.check()?;
        let sentences = if self.voice.phoneme_type == "text" {
            vec![normalized(text, self.config.max_phonemes)?]
        } else {
            #[cfg(feature = "espeak-gpl")]
            {
                native::phonemize(
                    text,
                    &self.voice.espeak.as_ref().expect("validated").voice,
                    self.config.max_phonemes,
                    cancel,
                    stop,
                )?
            }
            #[cfg(not(feature = "espeak-gpl"))]
            {
                return Err(PiperError::Unsupported("espeak-gpl feature"));
            }
        };
        let out_rate = self
            .config
            .output_sample_rate
            .unwrap_or(self.voice.audio.sample_rate);
        let mut resampler =
            pipecat_audio::SincResampler::new(self.voice.audio.sample_rate, out_rate, 1)?;
        let mut pcm = Vec::new();
        let mut input_samples = 0usize;
        for phonemes in sentences {
            cancel.check()?;
            stop.check()?;
            if phonemes.is_empty() {
                continue;
            }
            let ids = self
                .voice
                .phoneme_ids(&phonemes, self.config.max_phonemes)?;
            let len = ids.len();
            let input = Array2::from_shape_vec((1, len), ids)
                .map_err(|e| PiperError::Model(e.to_string()))?;
            let scales = arr1(&[
                settings
                    .noise_scale
                    .unwrap_or(self.voice.inference.noise_scale),
                settings
                    .length_scale
                    .unwrap_or(self.voice.inference.length_scale),
                settings.noise_w.unwrap_or(self.voice.inference.noise_w),
            ]);
            let mut inputs = ort::inputs!["input" => input, "input_lengths" => arr1(&[len as i64]), "scales" => scales].map_err(|e| PiperError::Model(e.to_string()))?;
            if self.voice.num_speakers > 1 {
                inputs.push((
                    "sid".into(),
                    ort::value::Tensor::from_array(arr1(&[settings.speaker_id.unwrap_or(0)]))
                        .map_err(|e| PiperError::Model(e.to_string()))?
                        .into(),
                ));
            }
            let options =
                Arc::new(RunOptions::new().map_err(|e| PiperError::Native(e.to_string()))?);
            cancel.install(options.clone())?;
            if let Err(error) = stop.install(options.clone()) {
                cancel.uninstall();
                return Err(error);
            }
            let output = self.session.run_with_options(inputs, &options);
            cancel.uninstall();
            stop.uninstall();
            cancel.check()?;
            stop.check()?;
            let output = output.map_err(|e| PiperError::Model(e.to_string()))?;
            let samples = output[0]
                .try_extract_tensor::<f32>()
                .map_err(|e| PiperError::Model(e.to_string()))?;
            if !matches!(samples.ndim(), 3 | 4)
                || samples.shape()[..samples.ndim() - 1]
                    .iter()
                    .any(|dim| *dim != 1)
                || samples.is_empty()
            {
                return Err(PiperError::Model(
                    "invalid output shape or empty audio".into(),
                ));
            }
            input_samples = input_samples
                .checked_add(samples.len())
                .ok_or(PiperError::TooLarge)?;
            if samples.iter().any(|v| !v.is_finite()) {
                return Err(PiperError::Model("non-finite audio samples".into()));
            }
            if input_samples > self.config.max_audio_samples {
                return Err(PiperError::TooLarge);
            }
            // Piper's default normalize_audio=True: scale each synthesized sentence
            // to signed16 full scale, or zeros below the upstream 1e-8 threshold.
            let peak = samples.iter().fold(0.0f32, |max, v| max.max(v.abs()));
            for chunk in samples
                .as_slice()
                .ok_or_else(|| PiperError::Model("noncontiguous output".into()))?
                .chunks(2048)
            {
                cancel.check()?;
                stop.check()?;
                let bytes: Vec<u8> = chunk
                    .iter()
                    .flat_map(|x| normalized_sample(*x, peak).to_le_bytes())
                    .collect();
                append(
                    &mut pcm,
                    resampler.process(&bytes)?,
                    self.config.max_audio_samples,
                )?;
            }
        }
        append(&mut pcm, resampler.flush()?, self.config.max_audio_samples)?;
        if pcm.is_empty() {
            return Err(PiperError::Model("no speech audio generated".into()));
        }
        Ok(pcm)
    }
}
fn append(pcm: &mut Vec<u8>, chunk: Vec<u8>, max: usize) -> Result<()> {
    if pcm.len().saturating_add(chunk.len()) / 2 > max {
        return Err(PiperError::TooLarge);
    }
    pcm.extend(chunk);
    Ok(())
}
fn check_type(actual: &ValueType, ty: TensorElementType, shape: &[i64]) -> Result<()> {
    if let ValueType::Tensor {
        ty: actual_ty,
        dimensions,
        ..
    } = actual
    {
        if *actual_ty == ty
            && dimensions.len() == shape.len()
            && dimensions
                .iter()
                .zip(shape)
                .all(|(a, b)| *a < 0 || *b < 0 || *a == *b)
        {
            return Ok(());
        }
    }
    Err(PiperError::Model(format!(
        "unexpected Piper tensor type or rank: expected {ty:?} {shape:?}, got {actual:?}"
    )))
}

fn normalized_sample(value: f32, peak: f32) -> i16 {
    if peak < 1e-8 {
        0
    } else {
        ((value / peak).clamp(-1.0, 1.0) * 32767.0) as i16
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pcm_normalization_matches_current_piper_silence_and_quiet_audio() {
        assert_eq!(normalized_sample(1e-9, 1e-9), 0);
        assert_eq!(normalized_sample(0.001, 0.001), 32767);
        assert_eq!(normalized_sample(-0.001, 0.001), -32767);
        assert_eq!(normalized_sample(0., 0.), 0);
    }
}
