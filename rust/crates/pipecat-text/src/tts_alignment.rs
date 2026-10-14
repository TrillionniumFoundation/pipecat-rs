//! Provider timing and provenance for native TTS adapters. Reconstruction events
//! are not playback receipts. Any error poisons the context; adapters must retire
//! synthesis instead of retrying partially dispatched text.
use crate::{
    AlignmentError, ContextId, TokenFrameSequencer, TokenReporting, TokenSequencerEvent,
    TokenSequencerLimits,
};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
};

/// Synthesis transformations never overwrite display text or original source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtsTextChannels {
    pub synthesis: String,
    pub display: String,
    pub original: String,
}
impl TtsTextChannels {
    pub fn identical(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            synthesis: text.clone(),
            display: text.clone(),
            original: text,
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TtsAlignmentLimits {
    pub sequencer: TokenSequencerLimits,
    pub max_pending_events: usize,
    pub max_pending_event_bytes: usize,
}
impl Default for TtsAlignmentLimits {
    fn default() -> Self {
        Self {
            sequencer: TokenSequencerLimits::default(),
            max_pending_events: 2048,
            max_pending_event_bytes: 1_048_576,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TtsAlignmentError {
    Alignment(AlignmentError),
    InvalidTimestamp,
    TimestampOverflow,
    Closed,
}
impl fmt::Display for TtsAlignmentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for TtsAlignmentError {}
impl From<AlignmentError> for TtsAlignmentError {
    fn from(value: AlignmentError) -> Self {
        Self::Alignment(value)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SynthesisOutcome {
    Pending,
    Succeeded,
}
/// A timing envelope retaining exact context/segment identities. Unreported
/// evidence stays diagnostic even after explicitly successful synthesis.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtsAlignmentEvent {
    pub event: TokenSequencerEvent,
    pub pts_ns: u64,
    pub synthesis_outcome: SynthesisOutcome,
    pub aggregated_by: String,
    pub audio_offset_ns: Option<u64>,
    pub includes_inter_frame_spaces: bool,
}
#[derive(Debug)]
struct EarlyWord {
    text: String,
    offset_ns: u64,
    spaces: bool,
}
/// One provider context. Adapters serialize contexts and pass their previous
/// terminal PTS when establishing this context's first-audio baseline.
#[derive(Debug)]
pub struct TtsAlignment {
    sequencer: TokenFrameSequencer,
    context: ContextId,
    reporting: TokenReporting,
    limits: TtsAlignmentLimits,
    baseline: Option<u64>,
    last_pts: u64,
    early_words: VecDeque<EarlyWord>,
    early_bytes: usize,
    pending_events: Vec<TokenSequencerEvent>,
    pending_bytes: usize,
    closed: bool,
    kinds: HashMap<crate::SegmentId, (String, bool)>,
    pending_kind: Option<(String, bool)>,
}
impl TtsAlignment {
    pub fn new(
        generation: u64,
        append_to_context: bool,
        reporting: TokenReporting,
        language: Option<&str>,
        limits: TtsAlignmentLimits,
    ) -> Result<Self, TtsAlignmentError> {
        if limits.max_pending_events == 0 || limits.max_pending_event_bytes == 0 {
            return Err(AlignmentError::InvalidLimits.into());
        }
        let mut sequencer = TokenFrameSequencer::new(generation, limits.sequencer)?;
        let context = sequencer.open_context(append_to_context, reporting, language)?;
        Ok(Self {
            sequencer,
            context,
            reporting,
            limits,
            baseline: None,
            last_pts: 0,
            early_words: VecDeque::new(),
            early_bytes: 0,
            pending_events: vec![],
            pending_bytes: 0,
            closed: false,
            kinds: HashMap::new(),
            pending_kind: None,
        })
    }
    pub fn context(&self) -> ContextId {
        self.context
    }
    pub fn baseline(&self) -> Option<u64> {
        self.baseline
    }
    pub fn last_pts(&self) -> u64 {
        self.last_pts
    }
    pub fn is_closed(&self) -> bool {
        self.closed
    }
    fn apply<T>(
        &mut self,
        run: impl FnOnce(&mut Self) -> Result<T, TtsAlignmentError>,
    ) -> Result<T, TtsAlignmentError> {
        if self.closed {
            return Err(TtsAlignmentError::Closed);
        }
        let result = run(self);
        if result.is_err() {
            self.cancel();
        }
        result
    }
    /// Admit before provider submission. A complete aggregate flushes sentence
    /// lookahead; streaming tokens accumulate into an exact three-channel unit.
    pub fn register(
        &mut self,
        text: &TtsTextChannels,
        complete_aggregate: bool,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.register_named(text, complete_aggregate, "sentence")
    }
    pub fn register_named(
        &mut self,
        text: &TtsTextChannels,
        complete_aggregate: bool,
        aggregated_by: &str,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.register_prepared(text, complete_aggregate, aggregated_by, false)
    }
    /// Preserve explicit aggregate spacing separately from provider token spacing.
    pub fn register_prepared(
        &mut self,
        text: &TtsTextChannels,
        complete_aggregate: bool,
        aggregated_by: &str,
        includes_inter_frame_spaces: bool,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.apply(|s| {
            if [&text.synthesis, &text.display, &text.original]
                .iter()
                .any(|text| text.len() > s.limits.sequencer.sequencer.alignment.max_text_bytes)
            {
                return Err(AlignmentError::LimitExceeded("text bytes").into());
            }
            if aggregated_by.len() > 128 {
                return Err(AlignmentError::LimitExceeded("aggregation name bytes").into());
            }
            let mut out = Vec::new();
            if complete_aggregate
                || s.pending_kind.as_ref().is_some_and(|(kind, spaces)| {
                    kind != aggregated_by || *spaces != includes_inter_frame_spaces
                })
            {
                let events = s.finalize_pending()?;
                out.extend(s.deliver(events, SynthesisOutcome::Pending)?);
            }
            let events = if complete_aggregate {
                s.sequencer.register_aggregate(
                    s.context,
                    crate::ParallelSentence {
                        tts_text: text.synthesis.clone(),
                        llm_text: text.original.clone(),
                        user_facing_text: text.display.clone(),
                    },
                )?
            } else {
                s.sequencer.register_token(
                    s.context,
                    &text.synthesis,
                    &text.display,
                    &text.original,
                )?
            };
            for event in &events {
                if let TokenSequencerEvent::Promoted { segment, .. } = event {
                    s.kinds.insert(
                        *segment,
                        (aggregated_by.into(), includes_inter_frame_spaces),
                    );
                }
            }
            if !complete_aggregate {
                s.pending_kind = Some((aggregated_by.into(), includes_inter_frame_spaces));
            }
            out.extend(s.deliver(events, SynthesisOutcome::Pending)?);
            Ok(out)
        })
    }
    fn finalize_pending(&mut self) -> Result<Vec<TokenSequencerEvent>, TtsAlignmentError> {
        let events = self.sequencer.finalize(self.context)?;
        if let Some(kind) = self.pending_kind.take() {
            for event in &events {
                if let TokenSequencerEvent::Promoted { segment, .. } = event {
                    self.kinds.insert(*segment, kind.clone());
                }
            }
        }
        Ok(events)
    }
    pub fn finalize(&mut self) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.apply(|s| {
            let events = s.finalize_pending()?;
            s.deliver(events, SynthesisOutcome::Pending)
        })
    }
    pub fn skipped(
        &mut self,
        display: &str,
        original: &str,
        destination: Option<&str>,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.skipped_named(display, original, destination, "skipped")
    }
    pub fn skipped_named(
        &mut self,
        display: &str,
        original: &str,
        destination: Option<&str>,
        aggregated_by: &str,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.skipped_prepared(display, original, destination, aggregated_by, true)
    }
    /// Register skipped text with its explicit aggregate spacing policy.
    pub fn skipped_prepared(
        &mut self,
        display: &str,
        original: &str,
        destination: Option<&str>,
        aggregated_by: &str,
        includes_inter_frame_spaces: bool,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.apply(|s| {
            if aggregated_by.len() > 128 {
                return Err(AlignmentError::LimitExceeded("aggregation name bytes").into());
            }
            let events = s.finalize_pending()?;
            let mut out = s.deliver(events, SynthesisOutcome::Pending)?;
            let (id, events) =
                s.sequencer
                    .register_skipped(s.context, display, original, destination)?;
            s.kinds
                .insert(id, (aggregated_by.into(), includes_inter_frame_spaces));
            out.extend(s.deliver(events, SynthesisOutcome::Pending)?);
            Ok(out)
        })
    }
    /// Finite, nonnegative seconds must fit strictly below 2^64 nanoseconds.
    /// Sub-nanosecond precision is truncated, never rounded into overflow.
    pub fn seconds_to_ns(seconds: f64) -> Result<u64, TtsAlignmentError> {
        let nanos = seconds * 1_000_000_000.0;
        if !seconds.is_finite() || seconds < 0.0 || !nanos.is_finite() || nanos >= u64::MAX as f64 {
            return Err(TtsAlignmentError::InvalidTimestamp);
        }
        Ok(nanos as u64)
    }
    pub fn word(
        &mut self,
        word: &str,
        offset_seconds: f64,
        includes_inter_frame_spaces: bool,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.apply(|s| {
            if s.reporting != TokenReporting::WordTimestamps {
                return Err(
                    AlignmentError::UnsupportedOperation("word timestamps disabled").into(),
                );
            }
            let offset_ns = Self::seconds_to_ns(offset_seconds)?;
            if word.len() > s.limits.sequencer.sequencer.alignment.max_word_bytes {
                return Err(AlignmentError::LimitExceeded("word bytes").into());
            }
            let word = EarlyWord {
                text: word.into(),
                offset_ns,
                spaces: includes_inter_frame_spaces,
            };
            if s.baseline.is_none() {
                if s.early_words.len() >= s.limits.sequencer.max_buffered_words {
                    return Err(AlignmentError::LimitExceeded("pre-audio words").into());
                }
                let bytes = s
                    .early_bytes
                    .checked_add(word.text.len())
                    .filter(|n| *n <= s.limits.sequencer.max_buffered_word_bytes)
                    .ok_or(AlignmentError::LimitExceeded("pre-audio word bytes"))?;
                s.early_words.push_back(word);
                s.early_bytes = bytes;
                Ok(vec![])
            } else {
                s.route_word(word)
            }
        })
    }
    fn route_word(&mut self, word: EarlyWord) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        let pts = self
            .baseline
            .unwrap()
            .checked_add(word.offset_ns)
            .ok_or(TtsAlignmentError::TimestampOverflow)?;
        pts.checked_add(1)
            .ok_or(TtsAlignmentError::TimestampOverflow)?;
        let events = self
            .sequencer
            .process_word(self.context, &word.text, pts, word.spaces)?;
        self.last_pts = self.last_pts.max(pts);
        self.deliver(events, SynthesisOutcome::Pending)
    }
    /// Call before emitting the first nonempty audio. Cached words follow their
    /// anchors and precede that audio, preserving provider order and zero PTS.
    pub fn start_audio(
        &mut self,
        now_ns: u64,
        previous_pts: u64,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.apply(|s| {
            if s.baseline.is_some() {
                return Ok(vec![]);
            }
            s.baseline = Some(now_ns.max(previous_pts));
            s.last_pts = s.baseline.unwrap();
            s.last_pts
                .checked_add(1)
                .ok_or(TtsAlignmentError::TimestampOverflow)?;
            for word in &s.early_words {
                s.baseline
                    .unwrap()
                    .checked_add(word.offset_ns)
                    .ok_or(TtsAlignmentError::TimestampOverflow)?;
            }
            let pending = std::mem::take(&mut s.pending_events);
            s.pending_bytes = 0;
            let mut out = s.deliver(pending, SynthesisOutcome::Pending)?;
            while let Some(word) = s.early_words.pop_front() {
                out.extend(s.route_word(word)?);
            }
            s.early_bytes = 0;
            Ok(out)
        })
    }
    /// Provider success is explicit. Missing reports remain Unreported. If no
    /// audio arrived, early words are discarded rather than emitted as words.
    pub fn finish_success(
        &mut self,
        now_ns: u64,
        previous_pts: u64,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.finish_success_with_audio_offset(now_ns, previous_pts, None)
    }
    /// Opt in to a provider-reported terminal audio offset relative to this
    /// context's first audio. Include trailing silence and mark end times in
    /// this offset. It advances the boundary, never creates a word or playback
    /// receipt. Without observed audio the offset is validated but not applied.
    /// `None` preserves the legacy last-word/synthesis-completion boundary.
    pub fn finish_success_with_audio_offset(
        &mut self,
        now_ns: u64,
        previous_pts: u64,
        audio_offset_seconds: Option<f64>,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        self.apply(|s| {
            let audio_offset_ns = audio_offset_seconds.map(Self::seconds_to_ns).transpose()?;
            if let (Some(baseline), Some(offset)) = (s.baseline, audio_offset_ns) {
                let terminal = baseline
                    .checked_add(offset)
                    .ok_or(TtsAlignmentError::TimestampOverflow)?;
                terminal
                    .checked_add(1)
                    .ok_or(TtsAlignmentError::TimestampOverflow)?;
                s.last_pts = s.last_pts.max(terminal);
            }
            if s.baseline.is_none() {
                s.baseline = Some(now_ns.max(previous_pts));
                s.last_pts = s.baseline.unwrap();
                s.last_pts
                    .checked_add(1)
                    .ok_or(TtsAlignmentError::TimestampOverflow)?;
                s.early_words.clear();
                s.early_bytes = 0;
            }
            let pending = std::mem::take(&mut s.pending_events);
            s.pending_bytes = 0;
            let mut out = s.deliver(pending, SynthesisOutcome::Pending)?;
            let events = s.finalize_pending()?;
            out.extend(s.deliver(events, SynthesisOutcome::Pending)?);
            let events = match s.reporting {
                TokenReporting::WordTimestamps => {
                    s.sequencer.close_context(s.context, s.last_pts)?
                }
                TokenReporting::SynthesisCompletion => {
                    s.sequencer.complete_synthesis(s.context, s.last_pts)?
                }
            };
            out.extend(s.deliver(events, SynthesisOutcome::Succeeded)?);
            s.closed = true;
            Ok(out)
        })
    }
    pub fn boundary_pts(&self) -> Result<u64, TtsAlignmentError> {
        self.last_pts
            .checked_add(1)
            .ok_or(TtsAlignmentError::TimestampOverflow)
    }
    /// Failures and interruptions never force textual completion.
    pub fn cancel(&mut self) {
        self.closed = true;
        self.early_words.clear();
        self.early_bytes = 0;
        self.pending_events.clear();
        self.pending_bytes = 0;
        self.kinds.clear();
        self.pending_kind = None;
        let _ = self.sequencer.interrupt();
    }
    fn deliver(
        &mut self,
        events: Vec<TokenSequencerEvent>,
        outcome: SynthesisOutcome,
    ) -> Result<Vec<TtsAlignmentEvent>, TtsAlignmentError> {
        let Some(baseline) = self.baseline else {
            let mut immediate = Vec::new();
            let mut deferred = Vec::new();
            for event in events {
                if self.pending_events.is_empty() && deferred.is_empty() {
                    if let TokenSequencerEvent::Alignment(crate::SequencerEvent::Skipped {
                        segment,
                        pts_ns: None,
                        ..
                    }) = &event
                    {
                        let (aggregated_by, includes_inter_frame_spaces) = self
                            .kinds
                            .remove(segment)
                            .unwrap_or_else(|| ("skipped".into(), true));
                        immediate.push(TtsAlignmentEvent {
                            event,
                            pts_ns: 0,
                            synthesis_outcome: outcome,
                            aggregated_by,
                            includes_inter_frame_spaces,
                            audio_offset_ns: None,
                        });
                        continue;
                    }
                }
                deferred.push(event);
            }
            let events = deferred;
            self.pending_events
                .len()
                .checked_add(events.len())
                .filter(|n| *n <= self.limits.max_pending_events)
                .ok_or(AlignmentError::LimitExceeded("pre-audio events"))?;
            let mut bytes = self.pending_bytes;
            for event in &events {
                bytes = bytes
                    .checked_add(event_bytes(event))
                    .filter(|n| *n <= self.limits.max_pending_event_bytes)
                    .ok_or(AlignmentError::LimitExceeded("pre-audio event bytes"))?;
            }
            self.pending_events.extend(events);
            self.pending_bytes = bytes;
            return Ok(immediate);
        };
        let retired: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                TokenSequencerEvent::Alignment(
                    crate::SequencerEvent::Word {
                        segment,
                        completion,
                        ..
                    }
                    | crate::SequencerEvent::Unreported {
                        segment,
                        completion,
                        ..
                    },
                ) if completion.complete => Some(*segment),
                TokenSequencerEvent::Alignment(crate::SequencerEvent::Skipped {
                    segment, ..
                }) => Some(*segment),
                _ => None,
            })
            .collect();
        let out = events
            .into_iter()
            .map(|event| {
                let pts_ns = match &event {
                    TokenSequencerEvent::Promoted { .. } => baseline,
                    TokenSequencerEvent::Progress { pts_ns, .. } => *pts_ns,
                    TokenSequencerEvent::Alignment(event) => match event {
                        crate::SequencerEvent::Word { pts_ns, .. }
                        | crate::SequencerEvent::Unreported { pts_ns, .. }
                        | crate::SequencerEvent::UnmatchedWord { pts_ns, .. } => *pts_ns,
                        crate::SequencerEvent::Skipped { pts_ns, .. } => pts_ns.unwrap_or(baseline),
                    },
                };
                self.last_pts = self.last_pts.max(pts_ns);
                let (segment, word) = match &event {
                    TokenSequencerEvent::Promoted { segment, .. }
                    | TokenSequencerEvent::Progress { segment, .. } => (Some(*segment), false),
                    TokenSequencerEvent::Alignment(
                        crate::SequencerEvent::Word { segment, .. }
                        | crate::SequencerEvent::Unreported { segment, .. },
                    ) => (Some(*segment), true),
                    TokenSequencerEvent::Alignment(crate::SequencerEvent::Skipped {
                        segment,
                        ..
                    }) => (Some(*segment), false),
                    _ => (None, false),
                };
                let (aggregated_by, includes_inter_frame_spaces) = segment
                    .and_then(|id| self.kinds.get(&id))
                    .cloned()
                    .unwrap_or_else(|| ("sentence".into(), false));
                // Progress carries the same aggregate name as its preceding word.
                // Finished entries are removed after the whole output batch.
                TtsAlignmentEvent {
                    aggregated_by,
                    includes_inter_frame_spaces,
                    audio_offset_ns: word.then(|| pts_ns.saturating_sub(baseline)),
                    event,
                    pts_ns,
                    synthesis_outcome: outcome,
                }
            })
            .collect();
        for id in retired {
            self.kinds.remove(&id);
        }
        Ok(out)
    }
}
fn event_bytes(event: &TokenSequencerEvent) -> usize {
    match event {
        TokenSequencerEvent::Promoted { sentence, .. } => sentence
            .tts_text
            .len()
            .saturating_add(sentence.llm_text.len())
            .saturating_add(sentence.user_facing_text.len()),
        TokenSequencerEvent::Progress {
            text,
            accumulated_text,
            remaining_text,
            ..
        } => text
            .len()
            .saturating_add(accumulated_text.len())
            .saturating_add(remaining_text.len()),
        TokenSequencerEvent::Alignment(event) => match event {
            crate::SequencerEvent::Word { completion, .. }
            | crate::SequencerEvent::Unreported { completion, .. } => completion
                .word
                .as_ref()
                .map_or(0, String::len)
                .saturating_add(completion.source_text.as_ref().map_or(0, String::len))
                .saturating_add(completion.overflow.as_ref().map_or(0, String::len)),
            crate::SequencerEvent::Skipped {
                text,
                raw_text,
                transport_destination,
                ..
            } => text
                .len()
                .saturating_add(raw_text.len())
                .saturating_add(transport_destination.as_ref().map_or(0, String::len)),
            crate::SequencerEvent::UnmatchedWord { word, .. } => word.len(),
        },
    }
}
