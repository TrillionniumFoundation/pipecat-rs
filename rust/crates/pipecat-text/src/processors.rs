//! Typed native counterparts of the source sentence and LLM text processors.
use crate::{string::match_endofsentence, Aggregation, SimpleTextAggregator, TextAggregator};
use pipecat_core::{
    async_trait, AggregatedTextFrame, DirectedFrame, Direction, Frame, FrameData, FrameProcessor,
    ProcessorContext, ProcessorError, TextFrame,
};
use std::sync::Mutex;

/// Chunk-level sentence aggregation. Unlike SimpleTextAggregator, this emits
/// the entire buffered chunk as soon as any sentence ending is found.
/// Provisional transcription is ignored. Interruption passes through without
/// clearing this buffer, matching the original SentenceAggregator.
#[derive(Default)]
pub struct SentenceAggregator {
    buffer: Mutex<String>,
}
impl SentenceAggregator {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn process_frame(
        &self,
        frame: Frame,
        direction: Direction,
    ) -> Result<Vec<DirectedFrame>, ProcessorError> {
        let mut buffer = self
            .buffer
            .lock()
            .map_err(|_| ProcessorError::fatal("sentence buffer poisoned"))?;
        if matches!(
            &frame.data,
            FrameData::InterimTranscription(_)
                | FrameData::EagerTranscription(_)
                | FrameData::InterimTranscriptionDetailed(_)
                | FrameData::EagerTranscriptionDetailed(_)
        ) {
            return Ok(vec![]);
        }
        let text = match &frame.data {
            FrameData::Text(t)
            | FrameData::LlmText(t)
            | FrameData::InputText(t)
            | FrameData::VisionText(t) => Some(t.text.as_str()),
            FrameData::AggregatedText(t) | FrameData::TtsText(t) => Some(t.text.text.as_str()),
            FrameData::TranscriptionDetailed(t) | FrameData::TranslationDetailed(t) => {
                Some(t.text.text.as_str())
            }
            FrameData::Transcription(t) | FrameData::Translation(t) => Some(t.text.as_str()),
            _ => None,
        };
        if let Some(text) = text {
            buffer.push_str(text);
            return if match_endofsentence(&buffer, None) > 0 {
                Ok(vec![DirectedFrame::new(
                    Frame::text(std::mem::take(&mut *buffer)),
                    Direction::Downstream,
                )])
            } else {
                Ok(vec![])
            };
        }
        if matches!(&frame.data, FrameData::End(_) | FrameData::EndDetailed(_)) {
            let mut output = vec![];
            if !buffer.is_empty() {
                output.push(DirectedFrame::new(
                    Frame::text(buffer.clone()),
                    Direction::Downstream,
                ));
            }
            output.push(DirectedFrame::new(frame, Direction::Downstream));
            return Ok(output);
        }
        Ok(vec![DirectedFrame::new(frame, direction)])
    }
}
#[async_trait]
impl FrameProcessor for SentenceAggregator {
    fn name(&self) -> &str {
        "SentenceAggregator"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        for output in self.process_frame(frame, direction)? {
            ctx.push(output.frame, output.direction).await?;
        }
        Ok(())
    }
}
/// Convert LLM/vision tokens into typed aggregated text. Flushes before LLM
/// response-end and pipeline-end frames; interruption clears pending text.
pub struct LlmTextProcessor {
    aggregator: Mutex<Box<dyn TextAggregator>>,
}
impl Default for LlmTextProcessor {
    fn default() -> Self {
        Self::new(SimpleTextAggregator::default())
    }
}
impl LlmTextProcessor {
    pub fn new(aggregator: impl TextAggregator + 'static) -> Self {
        Self {
            aggregator: Mutex::new(Box::new(aggregator)),
        }
    }
    pub fn reset(&self) -> Result<(), ProcessorError> {
        self.aggregator
            .lock()
            .map_err(|_| ProcessorError::fatal("text aggregator poisoned"))?
            .reset();
        Ok(())
    }
    pub fn set_language(&self, language: Option<&str>) -> Result<(), ProcessorError> {
        self.aggregator
            .lock()
            .map_err(|_| ProcessorError::fatal("text aggregator poisoned"))?
            .set_language(language);
        Ok(())
    }
    pub fn process_frame(
        &self,
        frame: Frame,
        direction: Direction,
    ) -> Result<Vec<DirectedFrame>, ProcessorError> {
        let mut aggregator = self
            .aggregator
            .lock()
            .map_err(|_| ProcessorError::fatal("text aggregator poisoned"))?;
        match &frame.data {
            FrameData::Interruption => {
                aggregator.handle_interruption();
            }
            FrameData::LlmText(text) | FrameData::VisionText(text) => {
                return Ok(aggregator
                    .aggregate(&text.text)
                    .into_iter()
                    .map(|a| aggregated_frame(a, text.skip_tts))
                    .collect());
            }
            FrameData::LlmResponseEnd | FrameData::End(_) | FrameData::EndDetailed(_) => {
                let mut output: Vec<_> = aggregator
                    .flush()
                    .into_iter()
                    .map(|a| aggregated_frame(a, None))
                    .collect();
                output.push(DirectedFrame::new(frame, direction));
                return Ok(output);
            }
            FrameData::LlmFullResponseEnd(end) | FrameData::VisionFullResponseEnd(end) => {
                let mut output: Vec<_> = aggregator
                    .flush()
                    .into_iter()
                    .map(|a| aggregated_frame(a, end.skip_tts))
                    .collect();
                output.push(DirectedFrame::new(frame, direction));
                return Ok(output);
            }
            _ => {}
        }
        Ok(vec![DirectedFrame::new(frame, direction)])
    }
}
fn aggregated_frame(aggregation: Aggregation, skip_tts: Option<bool>) -> DirectedFrame {
    let raw_text = aggregation
        .full_match
        .unwrap_or_else(|| aggregation.text.clone());
    let mut text = TextFrame::new(aggregation.text);
    text.skip_tts = skip_tts;
    DirectedFrame::new(
        Frame::new(FrameData::AggregatedText(AggregatedTextFrame {
            text,
            aggregated_by: aggregation.kind,
            context_id: None,
            raw_text: Some(raw_text),
            will_be_spoken: false,
        })),
        Direction::Downstream,
    )
}
#[async_trait]
impl FrameProcessor for LlmTextProcessor {
    fn name(&self) -> &str {
        "LLMTextProcessor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        for output in self.process_frame(frame, direction)? {
            ctx.push(output.frame, output.direction).await?;
        }
        Ok(())
    }
}
