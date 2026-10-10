use crate::{Engine, RateConversion, Result, RnNoiseError, FRAME_SAMPLES, SAMPLE_RATE};
use pipecat_audio::{decode_pcm16_le, encode_pcm16_le, PcmChunker, PcmFormat, SincResampler};
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) fn validate_format(format: PcmFormat, conversion: RateConversion) -> Result<()> {
    if format.channels() != 1 {
        return Err(RnNoiseError::Channels);
    }
    if format.sample_rate() != SAMPLE_RATE
        && (conversion != RateConversion::NativeSinc
            || !(8_000..=96_000).contains(&format.sample_rate()))
    {
        return Err(RnNoiseError::SampleRate);
    }
    Ok(())
}

pub(crate) struct Stream {
    engine: Box<dyn Engine>,
    chunker: PcmChunker,
    resample_in: Option<SincResampler>,
    resample_out: Option<SincResampler>,
}
impl Stream {
    pub(crate) fn new(engine: Box<dyn Engine>, format: PcmFormat) -> Result<Self> {
        let native_format = PcmFormat::new(SAMPLE_RATE, 1)?;
        let (resample_in, resample_out) = if format.sample_rate() == SAMPLE_RATE {
            (None, None)
        } else {
            (
                Some(SincResampler::new(format.sample_rate(), SAMPLE_RATE, 1)?),
                Some(SincResampler::new(SAMPLE_RATE, format.sample_rate(), 1)?),
            )
        };
        Ok(Self {
            engine,
            chunker: PcmChunker::new(native_format, FRAME_SAMPLES)?,
            resample_in,
            resample_out,
        })
    }
    pub(crate) fn process(&mut self, bytes: Vec<u8>, cancel: &AtomicBool) -> Result<Vec<u8>> {
        check_cancel(cancel)?;
        let input = match &mut self.resample_in {
            Some(resampler) => resampler.process(&bytes)?,
            None => bytes,
        };
        let mut output = Vec::new();
        for chunk in self.chunker.push(&input) {
            check_cancel(cancel)?;
            let samples: [i16; FRAME_SAMPLES] = decode_pcm16_le(&chunk)?
                .try_into()
                .map_err(|_| RnNoiseError::Invalid("invalid internal frame length"))?;
            output.extend(encode_pcm16_le(&self.engine.process(&samples)?));
        }
        check_cancel(cancel)?;
        match &mut self.resample_out {
            Some(resampler) if !output.is_empty() => Ok(resampler.process(&output)?),
            _ => Ok(output),
        }
    }
}
pub(crate) fn check_cancel(cancel: &AtomicBool) -> Result<()> {
    if cancel.load(Ordering::Acquire) {
        Err(RnNoiseError::Cancelled)
    } else {
        Ok(())
    }
}
