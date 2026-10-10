use crate::{runtime, Result, RuntimeOptions, VadError};
use ndarray::{arr0, Array2, Array3};
use ort::{execution_providers::CPUExecutionProvider, session::Session};
use std::path::Path;

/// Stateful batch-one Silero inference. Each call consumes exactly 32 ms of
/// mono samples; sample-rate changes reset both recurrent state and context.
pub struct SileroModel {
    session: Session,
    state: Array3<f32>,
    context: Vec<f32>,
    last_sample_rate: Option<u32>,
}
impl SileroModel {
    pub fn load(runtime: &RuntimeOptions, model_path: impl AsRef<Path>) -> Result<Self> {
        runtime::initialize(runtime)?;
        let session = Session::builder()?
            .with_execution_providers([CPUExecutionProvider::default().build()])?
            .with_inter_threads(1)?
            .with_intra_threads(1)?
            .commit_from_file(model_path)?;
        let names: Vec<_> = session
            .inputs
            .iter()
            .map(|input| input.name.as_str())
            .collect();
        if names != ["input", "state", "sr"] {
            return Err(VadError::Model(
                "Silero model inputs must be input, state, sr".into(),
            ));
        }
        if session.outputs.len() != 2
            || session.outputs[0].name != "output"
            || session.outputs[1].name != "stateN"
            || session.outputs.iter().any(|output| {
                !matches!(
                    output.output_type,
                    ort::value::ValueType::Tensor {
                        ty: ort::tensor::TensorElementType::Float32,
                        ..
                    }
                )
            })
        {
            return Err(VadError::Model(
                "Silero model requires float32 output and stateN tensors".into(),
            ));
        }
        Ok(Self {
            session,
            state: Array3::zeros((2, 1, 128)),
            context: Vec::new(),
            last_sample_rate: None,
        })
    }
    pub fn reset(&mut self) {
        self.state.fill(0.0);
        self.context.clear();
        self.last_sample_rate = None;
    }
    pub fn required_samples(sample_rate: u32) -> Result<usize> {
        match sample_rate {
            8000 => Ok(256),
            16000 => Ok(512),
            _ => Err(VadError::SampleRate(sample_rate)),
        }
    }
    pub fn predict_pcm16(&mut self, samples: &[i16], sample_rate: u32) -> Result<f32> {
        let normalized: Vec<_> = samples.iter().map(|&s| f32::from(s) / 32768.0).collect();
        self.predict(&normalized, sample_rate)
    }
    pub fn predict(&mut self, samples: &[f32], sample_rate: u32) -> Result<f32> {
        let count = Self::required_samples(sample_rate)?;
        if samples.len() != count {
            return Err(VadError::ChunkSize {
                expected: count,
                actual: samples.len(),
            });
        }
        if samples.iter().any(|s| !s.is_finite()) {
            return Err(VadError::InvalidParameter("audio samples must be finite"));
        }
        if self
            .last_sample_rate
            .is_some_and(|rate| rate != sample_rate)
        {
            self.reset();
        }
        let context_len = count / 8;
        if self.context.is_empty() {
            self.context.resize(context_len, 0.0);
        }
        let mut audio = Vec::with_capacity(count + context_len);
        audio.extend_from_slice(&self.context);
        audio.extend_from_slice(samples);
        let input = Array2::from_shape_vec((1, audio.len()), audio)
            .map_err(|e| VadError::Model(e.to_string()))?;
        let output = self.session.run(ort::inputs![
            "input" => input,
            "state" => self.state.view(),
            "sr" => arr0(i64::from(sample_rate))
        ]?)?;
        let probability = output
            .get("output")
            .ok_or_else(|| VadError::Model("missing Silero probability output".into()))?
            .try_extract_tensor::<f32>()?;
        if probability.len() != 1 {
            return Err(VadError::Model(
                "Silero probability output must contain one value".into(),
            ));
        }
        let probability = *probability.iter().next().expect("validated scalar");
        let state = output
            .get("stateN")
            .ok_or_else(|| VadError::Model("missing Silero recurrent state output".into()))?
            .try_extract_tensor::<f32>()?;
        if state.shape() != [2, 1, 128]
            || state.iter().any(|value| !value.is_finite())
            || !probability.is_finite()
            || !(0.0..=1.0).contains(&probability)
        {
            return Err(VadError::Model("invalid Silero model output".into()));
        }
        for (destination, source) in self.state.iter_mut().zip(state.iter()) {
            *destination = *source;
        }
        self.context
            .copy_from_slice(&samples[count - context_len..]);
        self.last_sample_rate = Some(sample_rate);
        Ok(probability)
    }
}
