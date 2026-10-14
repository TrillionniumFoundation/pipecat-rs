/// Compatibility wrapper retaining Silero's original 8/16 kHz validation.
/// The shared BS.1770 coefficients and meter live in pipecat-audio.
pub struct AudioVolumeTracker(pipecat_audio::AudioVolumeTracker);
impl AudioVolumeTracker {
    pub fn new(sample_rate: u32) -> crate::Result<Self> {
        crate::SileroModel::required_samples(sample_rate)?;
        Ok(Self(
            pipecat_audio::AudioVolumeTracker::new(sample_rate)
                .map_err(|_| crate::VadError::SampleRate(sample_rate))?,
        ))
    }
    pub fn reset(&mut self) {
        self.0.reset();
    }
    pub fn buffered_samples(&self) -> usize {
        self.0.buffered_samples()
    }
    pub fn update(&mut self, samples: &[i16]) {
        self.0.update(samples);
    }
    pub fn volume(&self) -> f64 {
        self.0.volume()
    }
}
