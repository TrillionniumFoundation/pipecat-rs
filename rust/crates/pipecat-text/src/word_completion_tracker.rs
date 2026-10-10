//! Bounded, generation-scoped reconstruction from provider word reports.
use crate::alnum_utils::{alnum_only, is_alnum};
use crate::markup_utils::{strip_complete_markup, strip_markup};
use crate::text_segment_map::{count, is_space, slice, tail, trim};
use crate::{AlignmentError, AlignmentLimits, CharacterSpan, TextSegmentMap};

/// How the source-reconstruction span was obtained. None proves audio playout.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AlignmentEvidence {
    /// A provider token matched this text (possibly case/accent/markup folded).
    ProviderReport,
    /// The token matched ahead of the cursor; intervening events were missing.
    Resynchronized,
    /// Source-compatible completion without a matching provider word report.
    UnreportedRemainder,
    /// The tracker had already consumed its raw input; the late word was ignored.
    AlreadyComplete,
}

/// One provider event's share of this aggregate, plus reconstruction metadata.
/// `source_text` preserves original whitespace; `word` and `overflow` follow the
/// source's trimmed frame API. `complete` means textual completion, not playback.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WordCompletion {
    pub generation: u64,
    pub word: Option<String>,
    pub overflow: Option<String>,
    pub source_text: Option<String>,
    pub source_span: Option<CharacterSpan>,
    pub display_pos: usize,
    pub raw_pos: usize,
    pub complete: bool,
    pub suppress_in_context: bool,
    pub evidence: AlignmentEvidence,
}

#[derive(Clone, Debug)]
pub struct WordCompletionTracker {
    generation: u64,
    map: TextSegmentMap,
    has_llm: bool,
    display_pos: usize,
    llm_pos: usize,
    llm_reconstructed_pos: usize,
    emitted_llm_pos: usize,
    forced: bool,
    has_unreported_text: bool,
    last: Option<WordCompletion>,
}
impl WordCompletionTracker {
    pub fn new(
        generation: u64,
        tts_text: &str,
        llm_text: Option<&str>,
        user_facing_text: Option<&str>,
        limits: AlignmentLimits,
    ) -> Result<Self, AlignmentError> {
        // Check the supplied strings before allocating the markup-stripped
        // fallback. The map validates the full alignment contract afterwards.
        if [
            tts_text,
            llm_text.unwrap_or(""),
            user_facing_text.unwrap_or(""),
        ]
        .iter()
        .any(|text| text.len() > limits.max_text_bytes)
        {
            return Err(AlignmentError::LimitExceeded("text bytes"));
        }
        let fallback;
        let user = match user_facing_text {
            Some(s) => s,
            None => {
                fallback = strip_complete_markup(tts_text);
                &fallback
            }
        };
        let map = TextSegmentMap::new(tts_text, user, llm_text, limits)?;
        Ok(Self::from_map(generation, map, llm_text.is_some()))
    }
    /// Use a validated explicit alignment. `has_llm` selects source attribution;
    /// false retains the source tracker's no-original-text behavior.
    pub fn from_map(generation: u64, map: TextSegmentMap, has_llm: bool) -> Self {
        Self {
            generation,
            map,
            has_llm,
            display_pos: 0,
            llm_pos: 0,
            llm_reconstructed_pos: 0,
            emitted_llm_pos: 0,
            forced: false,
            has_unreported_text: false,
            last: None,
        }
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn segment_map(&self) -> &TextSegmentMap {
        &self.map
    }
    pub fn is_complete(&self) -> bool {
        self.forced || self.map.is_complete()
    }
    pub fn has_unreported_text(&self) -> bool {
        self.has_unreported_text
    }
    pub fn last_completion(&self) -> Option<&WordCompletion> {
        self.last.as_ref()
    }
    pub fn word_belongs_here(&self, word: &str) -> Result<bool, AlignmentError> {
        self.map.word_belongs_current_segment(word)
    }

    pub fn add_word(
        &mut self,
        generation: u64,
        word: &str,
    ) -> Result<WordCompletion, AlignmentError> {
        if generation != self.generation {
            return Err(AlignmentError::StaleGeneration);
        }
        // Validate before any mutation, including already-completed trackers.
        let belongs = self.word_belongs_here(word)?;
        if self.forced || self.map.raw_pos() >= count(self.map.tts_text()) {
            return Ok(self.finish_result(None, None, None, AlignmentEvidence::AlreadyComplete));
        }
        if !belongs {
            let result = self.complete_unreported(generation, Some(word))?;
            return Ok(result);
        }
        let llm_before = self.llm_pos;
        let reconstructed_before = self.llm_reconstructed_pos;
        let raw_before = self.map.raw_pos();
        self.map.advance_word(word)?;
        let overflow = self.map.last_overflow().map(str::to_owned);
        let crossed = slice(self.map.tts_text(), raw_before, self.map.raw_pos());
        let skipped = count(&alnum_only(&strip_markup(crossed))) > count(&alnum_only(word));
        let frame_word = if skipped {
            crossed.to_owned()
        } else {
            let head = if self.has_llm {
                self.map.last_leading_duplicate()
            } else {
                0
            };
            let end = count(word) - overflow.as_deref().map_or(0, count);
            slice(word, head.min(end), end).to_owned()
        };
        self.display_pos = self.map.user_facing_pos();
        self.llm_pos = self.map.llm_pos();
        self.llm_reconstructed_pos = self.map.llm_reported_pos();
        let mut source_span = None;
        if self.has_llm {
            if self.is_complete() {
                self.llm_pos = count(self.map.llm_text());
                self.llm_reconstructed_pos = self.llm_pos;
                source_span = Some(CharacterSpan {
                    start: llm_before,
                    end: self.llm_pos,
                });
            } else if !self.map.in_transformed_segment() {
                if self.llm_reconstructed_pos == reconstructed_before
                    && self.llm_pos == llm_before
                    && self.map.last_completed_segment().is_none()
                {
                    let start = self.llm_pos
                        + tail(self.map.llm_text(), self.llm_pos)
                            .chars()
                            .take_while(|c| is_space(*c))
                            .count();
                    // A substituted or synthesis-only symbol cannot stand for
                    // the next source letter merely because no cursor moved.
                    let symbols = tail(self.map.llm_text(), start)
                        .chars()
                        .take_while(|c| !is_alnum(*c) && !is_space(*c) && *c != '<')
                        .count();
                    self.llm_pos = if symbols == 0 {
                        self.llm_pos
                    } else {
                        start + count(word).min(symbols)
                    };
                    source_span = Some(CharacterSpan {
                        start: start.min(self.llm_pos),
                        end: self.llm_pos,
                    });
                } else {
                    source_span = Some(CharacterSpan {
                        start: llm_before,
                        end: self.llm_pos.max(llm_before),
                    });
                }
            }
        }
        if self.is_complete() {
            self.display_pos = count(self.map.original_text());
        }
        let evidence = if skipped || self.map.last_resynchronized() {
            self.has_unreported_text = true;
            AlignmentEvidence::Resynchronized
        } else {
            AlignmentEvidence::ProviderReport
        };
        Ok(self.finish_result(Some(frame_word), overflow, source_span, evidence))
    }
    /// Release an unreported suffix as a distinctly typed reconstruction event.
    /// The provider cursor stays put. Applications may choose to retain this text
    /// in context, but must not treat it as a word or playout acknowledgment.
    pub fn complete_unreported(
        &mut self,
        generation: u64,
        overflow: Option<&str>,
    ) -> Result<WordCompletion, AlignmentError> {
        if generation != self.generation {
            return Err(AlignmentError::StaleGeneration);
        }
        if let Some(word) = overflow {
            self.word_belongs_here(word)?;
        }
        if self.forced {
            return Ok(self.finish_result(None, None, None, AlignmentEvidence::AlreadyComplete));
        }
        let remaining = tail(self.map.tts_text(), self.map.raw_pos()).to_owned();
        let source = self.has_llm.then_some(CharacterSpan {
            start: self.llm_reconstructed_pos,
            end: count(self.map.llm_text()),
        });
        self.has_unreported_text |= !trim(&remaining).is_empty();
        self.display_pos = count(self.map.original_text());
        self.llm_pos = count(self.map.llm_text());
        self.llm_reconstructed_pos = self.llm_pos;
        self.forced = true;
        Ok(self.finish_result(
            Some(remaining),
            overflow.map(str::to_owned),
            source,
            AlignmentEvidence::UnreportedRemainder,
        ))
    }
    fn finish_result(
        &mut self,
        word: Option<String>,
        overflow: Option<String>,
        source_span: Option<CharacterSpan>,
        evidence: AlignmentEvidence,
    ) -> WordCompletion {
        // Keep whitespace crossed while a transformed span was held, and do
        // not reattribute punctuation already carried by a previous word.
        // Source ranges form a monotonic partition even on forced completion.
        let source_span = source_span.map(|mut span| {
            span.start = self.emitted_llm_pos;
            span.end = span.end.max(span.start);
            self.emitted_llm_pos = span.end;
            span
        });
        let source_text =
            source_span.map(|s| slice(self.map.llm_text(), s.start, s.end).to_owned());
        let suppress = self.map.in_transformed_segment()
            || (self.has_llm && source_text.as_deref().is_none_or(|s| trim(s).is_empty()));
        let result = WordCompletion {
            generation: self.generation,
            word: word.map(|s| trim(&s).to_owned()),
            overflow: overflow.map(|s| trim(&s).to_owned()),
            source_text,
            source_span,
            display_pos: self.display_pos,
            raw_pos: self.map.raw_pos(),
            complete: self.is_complete(),
            suppress_in_context: suppress,
            evidence,
        };
        self.last = Some(result.clone());
        result
    }
    /// These views partition reconstruction text, not an audible transcript.
    pub fn accumulated_user_facing_text(&self) -> &str {
        slice(self.map.original_text(), 0, self.display_pos)
    }
    pub fn remaining_user_facing_text(&self) -> &str {
        tail(self.map.original_text(), self.display_pos)
    }
    pub fn accumulated_tts_text(&self) -> &str {
        slice(self.map.tts_text(), 0, self.map.raw_pos())
    }
    pub fn remaining_tts_text(&self) -> &str {
        tail(self.map.tts_text(), self.map.raw_pos())
    }
    pub fn accumulated_llm_text(&self) -> Option<&str> {
        self.has_llm
            .then(|| slice(self.map.llm_text(), 0, self.llm_reconstructed_pos))
    }
    pub fn remaining_llm_text(&self) -> Option<&str> {
        self.has_llm
            .then(|| tail(self.map.llm_text(), self.llm_reconstructed_pos))
    }
    /// Rewind immutable texts under a strictly newer generation. Late reports
    /// carrying the previous generation are rejected without changing state.
    pub fn reset(&mut self, generation: u64) -> Result<(), AlignmentError> {
        if generation <= self.generation {
            return Err(AlignmentError::StaleGeneration);
        }
        self.generation = generation;
        self.map.reset();
        self.display_pos = 0;
        self.llm_pos = 0;
        self.llm_reconstructed_pos = 0;
        self.emitted_llm_pos = 0;
        self.forced = false;
        self.has_unreported_text = false;
        self.last = None;
        Ok(())
    }
}
