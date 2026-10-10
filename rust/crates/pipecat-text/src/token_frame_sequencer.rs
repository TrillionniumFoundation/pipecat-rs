//! Bounded TOKEN-mode sentence promotion and provider-report reconstruction.
//!
//! Callers register tokens before dispatching them to synthesis, and route every
//! response with the same issued context identity. Neither promotion nor progress
//! is an audio receipt. Returned events must be queued in the owning audio
//! context's bounded, single-consumer event stream by the service adapter.
use crate::string::{char_to_byte, match_endofsentence};
use crate::text_segment_map::is_space;
use crate::{
    AggregatedFrameSequencer, AggregationType, AlignmentError, AlignmentEvidence, AlignmentLimits,
    ContextId, SegmentId, SequencerEvent, SequencerLimits, SimpleTextAggregator, TextAggregator,
};
use std::collections::{HashMap, VecDeque};

/// Locale identifiers are metadata, with a separate bound before normalization.
pub const MAX_SENTENCE_LANGUAGE_BYTES: usize = 64;
fn check_language(language: Option<&str>) -> Result<(), AlignmentError> {
    if language.is_some_and(|s| s.len() > MAX_SENTENCE_LANGUAGE_BYTES) {
        Err(AlignmentError::LimitExceeded("language bytes"))
    } else {
        Ok(())
    }
}

/// One sentence's parallel synthesis, original-source, and display channels.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ParallelSentence {
    pub tts_text: String,
    pub llm_text: String,
    pub user_facing_text: String,
}
impl ParallelSentence {
    fn bytes(&self) -> usize {
        self.tts_text.len() + self.llm_text.len() + self.user_facing_text.len()
    }
    fn is_empty(&self) -> bool {
        self.tts_text.is_empty() && self.llm_text.is_empty() && self.user_facing_text.is_empty()
    }
    fn identical(&self) -> bool {
        self.tts_text == self.llm_text && self.tts_text == self.user_facing_text
    }
}

/// Uses the existing sentence aggregator's boundary timing. Identical channels
/// can split inside coarse input chunks; divergent channels cut at token edges.
#[derive(Clone, Debug)]
pub struct ParallelSentenceAggregator {
    driver: SimpleTextAggregator,
    pending: ParallelSentence,
    aligned: bool,
    limits: AlignmentLimits,
}
impl ParallelSentenceAggregator {
    pub fn new(language: Option<&str>, limits: AlignmentLimits) -> Result<Self, AlignmentError> {
        check_language(language)?;
        // Share the alignment layer's limit validation.
        crate::TextSegmentMap::new("", "", None, limits)?;
        Ok(Self {
            driver: SimpleTextAggregator::new(AggregationType::Sentence, language),
            pending: ParallelSentence::default(),
            aligned: true,
            limits,
        })
    }
    pub fn set_language(&mut self, language: Option<&str>) -> Result<(), AlignmentError> {
        check_language(language)?;
        self.driver.set_language(language);
        Ok(())
    }
    pub fn language(&self) -> &str {
        self.driver.language()
    }
    pub fn pending(&self) -> &ParallelSentence {
        &self.pending
    }
    /// Includes the sentence driver's separate synthesis buffer.
    pub fn pending_text_bytes(&self) -> usize {
        self.pending.bytes() + self.driver.buffer.len()
    }
    fn check_append(&self, tts: &str, llm: &str, user: &str) -> Result<(), AlignmentError> {
        for (buffer, incoming) in [
            (self.pending.tts_text.as_str(), tts),
            (self.pending.llm_text.as_str(), llm),
            (self.pending.user_facing_text.as_str(), user),
            (self.driver.buffer.as_str(), tts),
        ] {
            if buffer
                .len()
                .checked_add(incoming.len())
                .is_none_or(|n| n > self.limits.max_text_bytes)
            {
                return Err(AlignmentError::LimitExceeded("text bytes"));
            }
            // Count the joined stream without allocating it or double-counting
            // a whitespace token split across incoming chunks.
            let mut inside = false;
            let mut tokens = 0usize;
            for c in buffer.chars().chain(incoming.chars()) {
                if is_space(c) {
                    inside = false;
                } else if !inside {
                    tokens += 1;
                    if tokens > self.limits.max_tokens {
                        return Err(AlignmentError::LimitExceeded("tokens"));
                    }
                    inside = true;
                }
            }
        }
        Ok(())
    }
    /// Returns only confirmed sentences; a terminal mark alone needs lookahead.
    /// Admission checks are conservative over pending text plus this whole token.
    pub fn aggregate(
        &mut self,
        tts: &str,
        llm: &str,
        user: &str,
    ) -> Result<Vec<ParallelSentence>, AlignmentError> {
        self.check_append(tts, llm, user)?;
        let boundary_count = self.driver.aggregate(tts).len();
        let identical = tts == llm && tts == user;
        let mut output = Vec::new();
        if boundary_count > 0 && self.aligned && identical {
            let combined = format!("{}{tts}", self.pending.tts_text);
            let mut offset = 0;
            for _ in 0..boundary_count {
                let boundary =
                    match_endofsentence(&combined[offset..], Some(self.driver.language()));
                if boundary == 0 {
                    break;
                }
                let end = offset + char_to_byte(&combined[offset..], boundary);
                let sentence = &combined[offset..end];
                output.push(ParallelSentence {
                    tts_text: sentence.into(),
                    llm_text: sentence.into(),
                    user_facing_text: sentence.into(),
                });
                offset = end;
            }
            let rest = &combined[offset..];
            self.pending = ParallelSentence {
                tts_text: rest.into(),
                llm_text: rest.into(),
                user_facing_text: rest.into(),
            };
            self.aligned = true;
            return Ok(output);
        }
        if boundary_count > 0 && !self.pending.is_empty() {
            output.push(std::mem::take(&mut self.pending));
        }
        self.pending.tts_text.push_str(tts);
        self.pending.llm_text.push_str(llm);
        self.pending.user_facing_text.push_str(user);
        // A transformed coarse chunk may leave our buffer behind the driver.
        // Equal channels alone do not establish a shared slicing offset.
        self.aligned = self.pending.identical() && self.pending.tts_text == self.driver.buffer;
        Ok(output)
    }
    pub fn flush(&mut self) -> Option<ParallelSentence> {
        self.driver.flush();
        if self
            .pending
            .user_facing_text
            .trim_matches(is_space)
            .is_empty()
        {
            return None;
        }
        let pending = std::mem::take(&mut self.pending);
        self.aligned = true;
        Some(pending)
    }
    pub fn interrupt(&mut self) {
        self.driver.reset();
        self.pending = ParallelSentence::default();
        self.aligned = true;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenReporting {
    WordTimestamps,
    /// Only an explicit synthesis-completion call reconstructs the text. This
    /// mode cannot infer reports from dispatch, sentence promotion, or audio.
    SynthesisCompletion,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenSequencerLimits {
    pub sequencer: SequencerLimits,
    pub max_buffered_words: usize,
    pub max_buffered_word_bytes: usize,
}
impl Default for TokenSequencerLimits {
    fn default() -> Self {
        Self {
            sequencer: SequencerLimits::default(),
            max_buffered_words: 1024,
            max_buffered_word_bytes: 65_536,
        }
    }
}
/// All events are reconstruction metadata. They must not be turned into Played
/// receipts or written directly to audible spoken history.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TokenSequencerEvent {
    /// Stable anchor for later progress. Announcements never append to context.
    Promoted {
        context: ContextId,
        segment: SegmentId,
        sentence: ParallelSentence,
    },
    Alignment(SequencerEvent),
    /// Source-style display reconstruction, suppressed inside atomic transforms.
    Progress {
        context: ContextId,
        segment: SegmentId,
        pts_ns: u64,
        text: String,
        accumulated_text: String,
        remaining_text: String,
        evidence: AlignmentEvidence,
    },
}
#[derive(Clone, Debug)]
struct TokenContext {
    aggregator: ParallelSentenceAggregator,
    reporting: TokenReporting,
}
#[derive(Clone, Debug)]
struct BufferedWord {
    context: ContextId,
    word: String,
    pts_ns: u64,
    spaces: bool,
}
/// Streaming facade over the completed-aggregate sequencer. Mutating operations
/// use bounded private checkpoints, so backpressure or validation errors leave
/// input text, words, identities, and output ordering unchanged for a retry.
#[derive(Debug)]
pub struct TokenFrameSequencer {
    core: AggregatedFrameSequencer,
    limits: TokenSequencerLimits,
    contexts: HashMap<ContextId, TokenContext>,
    anchors: HashMap<SegmentId, String>,
    buffered: VecDeque<BufferedWord>,
    buffered_bytes: usize,
}
impl TokenFrameSequencer {
    pub fn new(generation: u64, limits: TokenSequencerLimits) -> Result<Self, AlignmentError> {
        if limits.max_buffered_words == 0
            || limits.max_buffered_word_bytes == 0
            || limits.sequencer.max_slots == 0
        {
            return Err(AlignmentError::InvalidLimits);
        }
        let mut working_limits = limits.sequencer;
        // One bounded working slot lets a closing context promote its tail even
        // while another context fills the public queue. Commit checks retain the
        // configured public bound; closed completed slots do not block ordering.
        working_limits.max_slots = working_limits
            .max_slots
            .checked_add(1)
            .ok_or(AlignmentError::InvalidLimits)?;
        Ok(Self {
            core: AggregatedFrameSequencer::new(generation, working_limits)?,
            limits,
            contexts: HashMap::new(),
            anchors: HashMap::new(),
            buffered: VecDeque::new(),
            buffered_bytes: 0,
        })
    }
    pub fn generation(&self) -> u64 {
        self.core.generation()
    }
    pub fn pending_slots(&self) -> usize {
        self.core.pending_slots()
    }
    pub fn live_contexts(&self) -> usize {
        self.contexts.len()
    }
    pub fn buffered_words(&self) -> usize {
        self.buffered.len()
    }
    pub fn buffered_word_bytes(&self) -> usize {
        self.buffered_bytes
    }
    /// Counts pending slot strings, parallel/driver buffers, display anchor
    /// copies and early words. Pending channels reserve any additional display
    /// copy needed for promotion. Map internals and allocator overhead are excluded.
    pub fn pending_text_bytes(&self) -> usize {
        self.core.pending_text_bytes()
            + self.buffered_bytes
            + self
                .contexts
                .values()
                .map(|c| {
                    let a = &c.aggregator;
                    a.pending_text_bytes()
                        + a.pending
                            .user_facing_text
                            .len()
                            .saturating_sub(a.driver.buffer.len())
                })
                .sum::<usize>()
            + self.anchors.values().map(String::len).sum::<usize>()
    }
    fn check_bytes(&self, added: usize) -> Result<(), AlignmentError> {
        if self
            .pending_text_bytes()
            .checked_add(added)
            .is_none_or(|n| n > self.limits.sequencer.max_pending_text_bytes)
        {
            Err(AlignmentError::LimitExceeded("pending text bytes"))
        } else {
            Ok(())
        }
    }
    fn checkpoint(&self) -> Self {
        Self {
            core: self.core.checkpoint(),
            limits: self.limits,
            contexts: self.contexts.clone(),
            anchors: self.anchors.clone(),
            buffered: self.buffered.clone(),
            buffered_bytes: self.buffered_bytes,
        }
    }
    fn transaction<T>(
        &mut self,
        apply: impl FnOnce(&mut Self) -> Result<T, AlignmentError>,
    ) -> Result<T, AlignmentError> {
        let mut candidate = self.checkpoint();
        let result = apply(&mut candidate)?;
        if candidate.core.pending_slots() > self.limits.sequencer.max_slots {
            return Err(AlignmentError::LimitExceeded("slots"));
        }
        candidate.check_bytes(0)?;
        *self = candidate;
        Ok(result)
    }
    pub fn open_context(
        &mut self,
        append_to_context: bool,
        reporting: TokenReporting,
        language: Option<&str>,
    ) -> Result<ContextId, AlignmentError> {
        let aggregator =
            ParallelSentenceAggregator::new(language, self.limits.sequencer.alignment)?;
        let id = self.core.open_context(append_to_context)?;
        self.contexts.insert(
            id,
            TokenContext {
                aggregator,
                reporting,
            },
        );
        Ok(id)
    }
    /// Changes boundary rules without clearing text or altering the reporting mode.
    pub fn set_language(
        &mut self,
        context: ContextId,
        language: Option<&str>,
    ) -> Result<(), AlignmentError> {
        self.core.check_context(context)?;
        self.contexts
            .get_mut(&context)
            .unwrap()
            .aggregator
            .set_language(language)
    }
    pub fn register_token(
        &mut self,
        context: ContextId,
        tts: &str,
        user: &str,
        llm: &str,
    ) -> Result<Vec<TokenSequencerEvent>, AlignmentError> {
        self.core.check_context(context)?;
        self.contexts[&context]
            .aggregator
            .check_append(tts, llm, user)?;
        let added = tts
            .len()
            .checked_add(tts.len().max(user.len()))
            .and_then(|n| n.checked_add(user.len()))
            .and_then(|n| n.checked_add(llm.len()))
            .ok_or(AlignmentError::LimitExceeded("pending text bytes"))?;
        self.check_bytes(added)?;
        self.transaction(|s| {
            let sentences = s
                .contexts
                .get_mut(&context)
                .unwrap()
                .aggregator
                .aggregate(tts, llm, user)?;
            let mut out = Vec::new();
            for sentence in sentences {
                s.promote(context, sentence, &mut out)?;
            }
            Ok(out)
        })
    }
    /// Admit one already-complete aggregate without splitting its explicit
    /// provenance into sentence guesses. Any preceding token suffix is promoted
    /// first. Validation/backpressure preserve the existing transactional API.
    pub fn register_aggregate(
        &mut self,
        context: ContextId,
        sentence: ParallelSentence,
    ) -> Result<Vec<TokenSequencerEvent>, AlignmentError> {
        self.core.check_context(context)?;
        self.check_bytes(
            sentence
                .bytes()
                .checked_add(sentence.user_facing_text.len())
                .ok_or(AlignmentError::LimitExceeded("pending text bytes"))?,
        )?;
        self.transaction(|s| {
            let mut out = Vec::new();
            s.finalize_inner(context, &mut out)?;
            s.promote(context, sentence, &mut out)?;
            Ok(out)
        })
    }
    fn promote(
        &mut self,
        context: ContextId,
        sentence: ParallelSentence,
        out: &mut Vec<TokenSequencerEvent>,
    ) -> Result<(), AlignmentError> {
        if sentence.user_facing_text.trim_matches(is_space).is_empty() {
            return Ok(());
        }
        let segment = self.core.register_spoken(
            context,
            &sentence.tts_text,
            &sentence.user_facing_text,
            &sentence.llm_text,
            false,
        )?;
        self.anchors
            .insert(segment, sentence.user_facing_text.clone());
        out.push(TokenSequencerEvent::Promoted {
            context,
            segment,
            sentence,
        });
        self.replay(context, out)
    }
    fn wrap(&mut self, events: Vec<SequencerEvent>, out: &mut Vec<TokenSequencerEvent>) {
        for event in events {
            let progress = match &event {
                SequencerEvent::Word {
                    context,
                    segment,
                    pts_ns,
                    completion,
                    ..
                }
                | SequencerEvent::Unreported {
                    context,
                    segment,
                    pts_ns,
                    completion,
                    ..
                } if !completion.suppress_in_context
                    || completion.evidence == AlignmentEvidence::UnreportedRemainder =>
                {
                    self.anchors.get(segment).map(|text| {
                        let cut = char_to_byte(text, completion.display_pos);
                        TokenSequencerEvent::Progress {
                            context: *context,
                            segment: *segment,
                            pts_ns: *pts_ns,
                            text: text.clone(),
                            accumulated_text: text[..cut].into(),
                            remaining_text: text[cut..].into(),
                            evidence: completion.evidence,
                        }
                    })
                }
                _ => None,
            };
            out.push(TokenSequencerEvent::Alignment(event));
            if let Some(progress) = progress {
                out.push(progress);
            }
        }
        self.anchors
            .retain(|id, _| self.core.segment_is_pending(*id));
    }
    fn buffer(&mut self, word: BufferedWord) -> Result<(), AlignmentError> {
        if self.buffered.len() >= self.limits.max_buffered_words {
            return Err(AlignmentError::LimitExceeded("buffered words"));
        }
        let bytes = self
            .buffered_bytes
            .checked_add(word.word.len())
            .ok_or(AlignmentError::LimitExceeded("buffered word bytes"))?;
        if bytes > self.limits.max_buffered_word_bytes {
            return Err(AlignmentError::LimitExceeded("buffered word bytes"));
        }
        self.buffered_bytes = bytes;
        self.buffered.push_back(word);
        Ok(())
    }
    fn route_word(
        &mut self,
        word: BufferedWord,
        out: &mut Vec<TokenSequencerEvent>,
    ) -> Result<(), AlignmentError> {
        let events = self
            .core
            .process_word(word.context, &word.word, word.pts_ns, word.spaces)?;
        // Overflow can produce matched events and a still-early suffix in one call.
        let mut matched = Vec::new();
        for event in events {
            if let SequencerEvent::UnmatchedWord {
                context,
                word: token,
                pts_ns,
            } = event
            {
                self.buffer(BufferedWord {
                    context,
                    word: token,
                    pts_ns,
                    spaces: word.spaces,
                })?;
            } else {
                matched.push(event);
            }
        }
        self.wrap(matched, out);
        Ok(())
    }
    /// Early/unmatched words are bounded and replayed only in this exact context.
    /// Provider duplicate rejection still requires a genuine provider event ID.
    pub fn process_word(
        &mut self,
        context: ContextId,
        word: &str,
        pts_ns: u64,
        includes_inter_frame_spaces: bool,
    ) -> Result<Vec<TokenSequencerEvent>, AlignmentError> {
        self.core.check_context(context)?;
        if word.len() > self.limits.sequencer.alignment.max_word_bytes {
            return Err(AlignmentError::LimitExceeded("word bytes"));
        }
        if self.contexts[&context].reporting != TokenReporting::WordTimestamps {
            return Err(AlignmentError::UnsupportedOperation(
                "word timestamps disabled",
            ));
        }
        self.transaction(|s| {
            let mut out = Vec::new();
            s.route_word(
                BufferedWord {
                    context,
                    word: word.into(),
                    pts_ns,
                    spaces: includes_inter_frame_spaces,
                },
                &mut out,
            )?;
            Ok(out)
        })
    }
    fn replay(
        &mut self,
        context: ContextId,
        out: &mut Vec<TokenSequencerEvent>,
    ) -> Result<(), AlignmentError> {
        // Snapshot once: still-unmatched words wait for the next promotion rather
        // than spinning, and another context's pending words cannot be consumed.
        let buffered = std::mem::take(&mut self.buffered);
        self.buffered_bytes = 0;
        for word in buffered {
            if word.context == context {
                self.route_word(word, out)?;
            } else {
                self.buffer(word)?;
            }
        }
        Ok(())
    }
    fn finalize_inner(
        &mut self,
        context: ContextId,
        out: &mut Vec<TokenSequencerEvent>,
    ) -> Result<(), AlignmentError> {
        if let Some(sentence) = self.contexts.get_mut(&context).unwrap().aggregator.flush() {
            self.promote(context, sentence, out)?;
        } else {
            // Finalize releases the source's per-context pending aggregator,
            // including whitespace-only state, but leaves word routing live.
            self.contexts
                .get_mut(&context)
                .unwrap()
                .aggregator
                .interrupt();
        }
        Ok(())
    }
    /// Promote a partial sentence. The context stays live for later reports and
    /// may accept more tokens, for example after an intervening skipped aggregate.
    pub fn finalize(
        &mut self,
        context: ContextId,
    ) -> Result<Vec<TokenSequencerEvent>, AlignmentError> {
        self.core.check_context(context)?;
        self.transaction(|s| {
            let mut out = Vec::new();
            s.finalize_inner(context, &mut out)?;
            Ok(out)
        })
    }
    /// Finalizes this context's pending sentence before inserting skipped text.
    pub fn register_skipped(
        &mut self,
        context: ContextId,
        text: &str,
        raw: &str,
        destination: Option<&str>,
    ) -> Result<(SegmentId, Vec<TokenSequencerEvent>), AlignmentError> {
        self.core.check_context(context)?;
        self.transaction(|s| {
            let mut out = Vec::new();
            s.finalize_inner(context, &mut out)?;
            let (id, events) = s.core.register_skipped(context, text, raw, destination)?;
            s.wrap(events, &mut out);
            Ok((id, out))
        })
    }
    /// End provider reporting: promote/replay a final partial, expose unmatched
    /// diagnostics, then explicitly reconstruct any unreported suffixes.
    pub fn close_context(
        &mut self,
        context: ContextId,
        pts_ns: u64,
    ) -> Result<Vec<TokenSequencerEvent>, AlignmentError> {
        self.core.check_context(context)?;
        self.transaction(|s| {
            let mut out = Vec::new();
            s.finalize_inner(context, &mut out)?;
            let buffered = std::mem::take(&mut s.buffered);
            s.buffered_bytes = 0;
            for word in buffered {
                if word.context == context {
                    out.push(TokenSequencerEvent::Alignment(
                        SequencerEvent::UnmatchedWord {
                            context,
                            word: word.word,
                            pts_ns: word.pts_ns,
                        },
                    ));
                } else {
                    s.buffer(word)?;
                }
            }
            let events = s.core.close_context(context, pts_ns)?;
            s.wrap(events, &mut out);
            s.core.retire_closed_context_slots(context);
            s.contexts.remove(&context);
            Ok(out)
        })
    }
    /// Explicit completion signal for a provider without timestamps. The result
    /// remains Unreported, even when synthesis was successful. No playout claim
    /// follows from this signal, and no failed/cancelled request should call it.
    pub fn complete_synthesis(
        &mut self,
        context: ContextId,
        pts_ns: u64,
    ) -> Result<Vec<TokenSequencerEvent>, AlignmentError> {
        self.core.check_context(context)?;
        if self.contexts[&context].reporting != TokenReporting::SynthesisCompletion {
            return Err(AlignmentError::UnsupportedOperation(
                "context uses word timestamps",
            ));
        }
        self.close_context(context, pts_ns)
    }
    /// Complete only the earliest incomplete sentence in this context, after the service establishes that its
    /// synthesis finished. Other sentences and pending tokens remain live. This
    /// releases backpressure without treating dispatch/promotion as completion.
    /// Out-of-order completion is rejected without mutation and can be retried
    /// after its predecessors. Other contexts remain independent.
    pub fn complete_synthesis_segment(
        &mut self,
        context: ContextId,
        segment: SegmentId,
        pts_ns: u64,
    ) -> Result<Vec<TokenSequencerEvent>, AlignmentError> {
        self.core.check_context(context)?;
        if self.contexts[&context].reporting != TokenReporting::SynthesisCompletion {
            return Err(AlignmentError::UnsupportedOperation(
                "context uses word timestamps",
            ));
        }
        self.transaction(|s| {
            let events = s
                .core
                .complete_segment_unreported(context, segment, pts_ns)?;
            let mut out = Vec::new();
            s.wrap(events, &mut out);
            Ok(out)
        })
    }
    pub fn interrupt(&mut self) -> Result<u64, AlignmentError> {
        let generation = self.core.interrupt()?;
        self.contexts.clear();
        self.anchors.clear();
        self.buffered.clear();
        self.buffered_bytes = 0;
        Ok(generation)
    }
}
