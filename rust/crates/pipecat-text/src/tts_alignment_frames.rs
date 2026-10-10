//! Frame conversion for the timing bridge. Exact emitted anchor IDs connect
//! provider reports to progress; diagnostics never enter the TtsText path.
use crate::{
    AlignmentEvidence, SegmentId, SequencerEvent, SynthesisOutcome, TokenSequencerEvent,
    TtsAlignmentError, TtsAlignmentEvent,
};
use pipecat_core::{
    AggregatedTextFrame, AggregatedTextProgressFrame, Frame, FrameData, TextFrame,
    TtsAlignmentDiagnosticFrame, TtsAlignmentEvidence, TtsSourceSpan, TtsSynthesisOutcome,
};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub struct TtsAlignmentFrames {
    context_id: String,
    context: crate::ContextId,
    append_to_context: bool,
    max_anchors: usize,
    anchors: HashMap<SegmentId, u64>,
    source_parts: HashSet<SegmentId>,
}
impl TtsAlignmentFrames {
    pub fn new(
        context_id: String,
        context: crate::ContextId,
        append_to_context: bool,
        max_anchors: usize,
    ) -> Result<Self, TtsAlignmentError> {
        if context_id.is_empty() || context_id.len() > 1024 || max_anchors == 0 {
            return Err(crate::AlignmentError::InvalidLimits.into());
        }
        Ok(Self {
            context_id,
            context,
            append_to_context,
            max_anchors,
            anchors: HashMap::new(),
            source_parts: HashSet::new(),
        })
    }
    fn anchor(&self, segment: SegmentId) -> Result<u64, TtsAlignmentError> {
        self.anchors
            .get(&segment)
            .copied()
            .ok_or(crate::AlignmentError::UnknownSegment.into())
    }
    /// Conversion errors are terminal for the adapter context. Never dispatch a
    /// partial batch after an error or reconstruct missing anchor identities.
    pub fn convert(
        &mut self,
        events: Vec<TtsAlignmentEvent>,
    ) -> Result<Vec<Frame>, TtsAlignmentError> {
        let mut out = vec![];
        let mut retired = vec![];
        for timed in events {
            let context = match &timed.event {
                TokenSequencerEvent::Promoted { context, .. }
                | TokenSequencerEvent::Progress { context, .. } => *context,
                TokenSequencerEvent::Alignment(event) => match event {
                    SequencerEvent::Word { context, .. }
                    | SequencerEvent::Unreported { context, .. }
                    | SequencerEvent::Skipped { context, .. }
                    | SequencerEvent::UnmatchedWord { context, .. } => *context,
                },
            };
            if context != self.context {
                return Err(crate::AlignmentError::UnknownContext.into());
            }

            // Progress immediately follows its completion. Retire before the
            // next unrelated event so long batches use the live-anchor budget.
            retired.retain(|id| {
                if matches!(&timed.event, TokenSequencerEvent::Progress { segment, .. } if segment == id) {
                    true
                } else { self.anchors.remove(id); self.source_parts.remove(id); false }
            });
            let pts = timed.pts_ns;
            let outcome = match timed.synthesis_outcome {
                SynthesisOutcome::Pending => TtsSynthesisOutcome::Pending,
                SynthesisOutcome::Succeeded => TtsSynthesisOutcome::Succeeded,
            };
            let mut frame = match timed.event {
                TokenSequencerEvent::Promoted {
                    segment, sentence, ..
                } => {
                    if self.anchors.len() >= self.max_anchors {
                        return Err(crate::AlignmentError::LimitExceeded("frame anchors").into());
                    }
                    let mut text = TextFrame::llm(sentence.user_facing_text);
                    text.append_to_context = false;
                    text.includes_inter_frame_spaces = timed.includes_inter_frame_spaces;
                    let frame = Frame::new(FrameData::AggregatedText(AggregatedTextFrame {
                        text,
                        aggregated_by: timed.aggregated_by.clone(),
                        context_id: Some(self.context_id.clone()),
                        raw_text: Some(sentence.llm_text),
                        will_be_spoken: true,
                    }));
                    if self.anchors.insert(segment, frame.id).is_some() {
                        return Err(crate::AlignmentError::UnknownSegment.into());
                    }
                    frame
                }
                TokenSequencerEvent::Alignment(SequencerEvent::Word {
                    segment,
                    completion,
                    append_to_context,
                    includes_inter_frame_spaces,
                    ..
                })
                | TokenSequencerEvent::Alignment(SequencerEvent::Unreported {
                    segment,
                    completion,
                    append_to_context,
                    includes_inter_frame_spaces,
                    ..
                }) => {
                    let id = self.anchor(segment)?;
                    if completion.complete {
                        retired.push(segment);
                    }
                    if completion.evidence != AlignmentEvidence::ProviderReport {
                        let evidence = match completion.evidence {
                            AlignmentEvidence::Resynchronized => {
                                TtsAlignmentEvidence::Resynchronized
                            }
                            _ => TtsAlignmentEvidence::Unreported,
                        };
                        Frame::new(FrameData::TtsAlignmentDiagnostic(
                            TtsAlignmentDiagnosticFrame {
                                context_id: self.context_id.clone(),
                                segment_id: Some(id),
                                evidence,
                                synthesis_text: completion.word.unwrap_or_default(),
                                original_text: completion.source_text,
                                source_span: completion.source_span.map(|s| TtsSourceSpan {
                                    start: s.start,
                                    end: s.end,
                                }),
                                display_position: Some(completion.display_pos),
                                synthesis_outcome: outcome,
                            },
                        ))
                    } else {
                        let mut text = TextFrame::new(completion.word.unwrap_or_default());
                        text.append_to_context = self.append_to_context
                            && append_to_context
                            && !completion.suppress_in_context;
                        // Exact original-text partitions already carry their internal
                        // whitespace. The first appendable part retains aggregate
                        // boundary spacing; held/suppressed transform words do not
                        // consume that boundary.
                        let continuation = text.append_to_context
                            && completion
                                .source_text
                                .as_ref()
                                .is_some_and(|text| !text.is_empty())
                            && !self.source_parts.insert(segment);
                        text.includes_inter_frame_spaces = includes_inter_frame_spaces
                            || timed.includes_inter_frame_spaces
                            || continuation;
                        let mut frame = Frame::new(FrameData::TtsText(AggregatedTextFrame {
                            text,
                            aggregated_by: "word".into(),
                            context_id: Some(self.context_id.clone()),
                            raw_text: completion.source_text,
                            will_be_spoken: true,
                        }));
                        frame.metadata.insert("segment_id".into(), id.into());
                        frame
                            .metadata
                            .insert("alignment_evidence".into(), "provider_report".into());
                        frame
                    }
                }
                TokenSequencerEvent::Progress {
                    segment,
                    text,
                    accumulated_text,
                    remaining_text,
                    evidence,
                    ..
                } => {
                    if evidence != AlignmentEvidence::ProviderReport {
                        continue;
                    }
                    Frame::new(FrameData::AggregatedTextProgress(
                        AggregatedTextProgressFrame {
                            segment_id: self.anchor(segment)?,
                            context_id: Some(self.context_id.clone()),
                            text,
                            aggregated_by: timed.aggregated_by.clone(),
                            accumulated_text,
                            remaining_text,
                        },
                    ))
                }
                TokenSequencerEvent::Alignment(SequencerEvent::Skipped {
                    text,
                    raw_text,
                    transport_destination,
                    ..
                }) => {
                    let mut text = TextFrame::llm(text);
                    text.skip_tts = Some(true);
                    text.append_to_context = self.append_to_context;
                    text.includes_inter_frame_spaces = timed.includes_inter_frame_spaces;
                    let mut frame = Frame::new(FrameData::AggregatedText(AggregatedTextFrame {
                        text,
                        aggregated_by: timed.aggregated_by.clone(),
                        context_id: Some(self.context_id.clone()),
                        raw_text: Some(raw_text),
                        will_be_spoken: false,
                    }));
                    frame.transport_destination = transport_destination;
                    frame
                }
                TokenSequencerEvent::Alignment(SequencerEvent::UnmatchedWord { word, .. }) => {
                    Frame::new(FrameData::TtsAlignmentDiagnostic(
                        TtsAlignmentDiagnosticFrame {
                            context_id: self.context_id.clone(),
                            segment_id: None,
                            evidence: TtsAlignmentEvidence::Unmatched,
                            synthesis_text: word,
                            original_text: None,
                            source_span: None,
                            display_position: None,
                            synthesis_outcome: outcome,
                        },
                    ))
                }
            };
            frame.set_pipeline_pts(pts);
            if matches!(frame.data, FrameData::TtsText(_)) {
                if let Some(offset) = timed.audio_offset_ns {
                    frame
                        .metadata
                        .insert("audio_offset_seconds".into(), (offset as f64 / 1e9).into());
                }
            }
            out.push(frame);
        }
        for id in retired {
            self.anchors.remove(&id);
            self.source_parts.remove(&id);
        }
        Ok(out)
    }
}
