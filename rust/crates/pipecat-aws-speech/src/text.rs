//! Bounded sentence aggregation retaining per-utterance provenance and routing.
use pipecat_core::{AggregatedTextFrame, Direction, Frame, FrameData, ProcessorError, TextFrame};
use pipecat_text::{Aggregation, AggregationType, SkipTagsAggregator, TextAggregator};
#[derive(Clone, PartialEq)]
struct Meta {
    append: bool,
    skip: Option<bool>,
    spaces: bool,
    destination: Option<String>,
}
pub(crate) struct TextState {
    aggregator: SkipTagsAggregator,
    meta: Option<Meta>,
    bytes: usize,
    source: u64,
}
impl TextState {
    pub(crate) fn new(language: &str) -> Self {
        Self {
            aggregator: SkipTagsAggregator::new(
                vec![("<spell>".into(), "</spell>".into())],
                AggregationType::Sentence,
                Some(language),
            ),
            meta: None,
            bytes: 0,
            source: 0,
        }
    }
    pub(crate) fn set_language(&mut self, language: &str) {
        self.aggregator.set_language(Some(language));
    }
    pub(crate) fn reset(&mut self) {
        self.aggregator.reset();
        self.meta = None;
        self.bytes = 0;
    }
    fn frames(&self, aggregations: Vec<Aggregation>) -> Vec<Frame> {
        let Some(meta) = &self.meta else {
            return vec![];
        };
        aggregations
            .into_iter()
            .map(|a| {
                let mut text = TextFrame::new(a.text.clone());
                text.append_to_context = meta.append;
                text.skip_tts = meta.skip;
                text.includes_inter_frame_spaces = meta.spaces;
                let mut frame = Frame::new(FrameData::AggregatedText(AggregatedTextFrame {
                    text,
                    aggregated_by: a.kind,
                    raw_text: Some(a.full_match.unwrap_or(a.text)),
                    context_id: None,
                    will_be_spoken: false,
                }));
                frame.transport_destination = meta.destination.clone();
                frame
                    .metadata
                    .insert("source_frame_id".into(), self.source.into());
                frame
            })
            .collect()
    }
    fn flush(&mut self) -> Vec<Frame> {
        let values = self.aggregator.flush();
        let frames = self.frames(values.into_iter().collect());
        self.meta = None;
        self.bytes = 0;
        frames
    }
    pub(crate) fn prepare(
        &mut self,
        frame: Frame,
        direction: Direction,
    ) -> Result<Vec<Frame>, ProcessorError> {
        if direction == Direction::Upstream {
            return Ok(vec![frame]);
        }
        if matches!(
            frame.data,
            FrameData::Interruption | FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            self.reset();
            return Ok(vec![frame]);
        }
        let text = match &frame.data {
            FrameData::Text(t) | FrameData::LlmText(t) | FrameData::VisionText(t) => Some(t),
            _ => None,
        };
        if let Some(text) = text {
            let meta = Meta {
                append: text.append_to_context,
                skip: text.skip_tts,
                spaces: text.includes_inter_frame_spaces,
                destination: frame.transport_destination.clone(),
            };
            let mut output = if self.meta.as_ref().is_some_and(|m| m != &meta) {
                self.flush()
            } else {
                vec![]
            };
            if self.meta.is_none() {
                self.meta = Some(meta);
                self.source = frame.id;
            }
            // Bounds include a whole incoming token, so a huge token cannot cause
            // unbounded aggregation even when it contains many sentence boundaries.
            if text.text.len() > 12000 || self.bytes.saturating_add(text.text.len()) > 12000 {
                self.reset();
                return Err(ProcessorError::new("Polly sentence buffer limit exceeded"));
            }
            self.bytes += text.text.len();
            let values = self.aggregator.aggregate(&text.text);
            for value in &values {
                self.bytes = self
                    .bytes
                    .saturating_sub(value.full_match.as_ref().unwrap_or(&value.text).len());
            }
            output.extend(self.frames(values));
            return Ok(output);
        }
        let flush = matches!(
            frame.data,
            FrameData::LlmResponseEnd
                | FrameData::LlmFullResponseEnd(_)
                | FrameData::VisionFullResponseEnd(_)
                | FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::TtsSpeak(_)
                | FrameData::AggregatedText(_)
                | FrameData::TtsTextInput(_)
                | FrameData::TtsUpdateSettings(_)
                | FrameData::ServiceUpdateSettings(_)
                | FrameData::FlushDetailed(_)
        );
        let mut output = if flush { self.flush() } else { vec![] };
        output.push(frame);
        Ok(output)
    }
}
