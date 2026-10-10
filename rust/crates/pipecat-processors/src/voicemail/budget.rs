use super::*;
use std::mem::size_of;

#[derive(Clone, Copy)]
pub(super) struct Charge {
    pub bytes: usize,
    pub pcm: usize,
    pub duration: Duration,
}
struct Budget {
    used: usize,
    limit: usize,
}
impl Budget {
    fn add(&mut self, n: usize) -> Result<(), Failure> {
        self.used = self
            .used
            .checked_add(n)
            .filter(|n| *n <= self.limit)
            .ok_or(Failure::FrameLimit)?;
        Ok(())
    }
    fn string(&mut self, s: &String) -> Result<(), Failure> {
        self.add(s.capacity())
    }
    fn optional(&mut self, s: &Option<String>) -> Result<(), Failure> {
        if let Some(s) = s {
            self.string(s)?;
        }
        Ok(())
    }
    fn json(&mut self, value: &Value, depth: usize) -> Result<(), Failure> {
        if depth > 64 {
            return Err(Failure::FrameLimit);
        }
        self.add(size_of::<Value>())?;
        match value {
            Value::String(s) => self.string(s)?,
            Value::Array(a) => {
                self.add(a.capacity().saturating_mul(size_of::<Value>()))?;
                for v in a {
                    self.json(v, depth + 1)?;
                }
            }
            Value::Object(o) => {
                for (k, v) in o {
                    // Conservative B-tree node overhead, charged per entry.
                    self.add(256)?;
                    self.string(k)?;
                    self.json(v, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}
pub(super) fn charge(frame: &Frame, limit: usize) -> Result<Charge, Failure> {
    // Native tool/eager/provider continuation carriers can retain arbitrary
    // external state. This compact speech-only slice rejects them explicitly.
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| Failure::UnsupportedTopology)?;
    if frame.error_origin().is_some() {
        return Err(Failure::UnsupportedTopology);
    }
    if frame.metadata.contains_key("pipecat.media.spoken.source")
        || frame.metadata.contains_key("pipecat.media.spoken.response")
    {
        return Err(Failure::UnsupportedTopology);
    }
    let mut b = Budget { used: 0, limit };
    b.add(size_of::<Held>() + 4096)?; // bounded local tags, context origins, queue slot
    b.string(&frame.name)?;
    b.optional(&frame.transport_source)?;
    b.optional(&frame.transport_destination)?;
    for (key, value) in &frame.metadata {
        b.add(256)?;
        b.string(key)?;
        b.json(value, 0)?;
    }
    let mut pcm = 0;
    let mut duration = Duration::ZERO;
    match &frame.data {
        FrameData::TtsAudio(a) | FrameData::SpeechOutputAudio(a) => {
            a.validate().map_err(|_| Failure::PcmLimit)?;
            pcm = a.audio.capacity();
            b.add(pcm)?;
            b.optional(&a.context_id)?;
            let nanos =
                (a.num_frames() as u128 * 1_000_000_000).div_ceil(u128::from(a.sample_rate));
            duration = Duration::from_nanos(u64::try_from(nanos).map_err(|_| Failure::PcmLimit)?);
        }
        FrameData::TtsStarted(s) => b.optional(&s.context_id)?,
        FrameData::TtsStopped(s) => b.optional(&s.context_id)?,
        FrameData::TtsText(t) => {
            b.string(&t.text.text)?;
            b.string(&t.aggregated_by)?;
            b.optional(&t.context_id)?;
            b.optional(&t.raw_text)?;
        }
        FrameData::TtsTextInput(t) => {
            b.string(&t.synthesis_text)?;
            b.string(&t.display_text)?;
            b.string(&t.original_text)?;
            b.string(&t.aggregated_by)?;
        }
        FrameData::TtsAlignmentDiagnostic(t) => {
            b.string(&t.context_id)?;
            b.string(&t.synthesis_text)?;
            b.optional(&t.original_text)?;
        }
        FrameData::AggregatedTextProgress(t) => {
            b.optional(&t.context_id)?;
            b.string(&t.text)?;
            b.string(&t.aggregated_by)?;
            b.string(&t.accumulated_text)?;
            b.string(&t.remaining_text)?;
        }
        FrameData::LlmResponseStart | FrameData::LlmResponseEnd => {}
        _ => return Err(Failure::UnsupportedTopology),
    }
    // Reserve the original, one drain clone, and compact topology bookkeeping
    // before admitting anything. Clone allocations are at most original capacity.
    let bytes = b
        .used
        .checked_mul(3)
        .filter(|n| *n <= limit)
        .ok_or(Failure::FrameLimit)?;
    let pcm = pcm.checked_mul(2).ok_or(Failure::PcmLimit)?;
    Ok(Charge {
        bytes,
        pcm,
        duration,
    })
}
