//! Bounded source-compatible input sentence finalization, with no task or timer owner.
//!
//! The receive owner supplies only actual input transcription contributions. Explicit
//! interim hypotheses and production/completion events never enter this state. The
//! pinned Python handler uses the default sentence language, even when a transcript
//! carries a language code; that code is retained only as normalized provenance.

use crate::{GeminiError, Transcript};
use pipecat_text::string::match_endofsentence;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::time::Instant;

const INACTIVITY: Duration = Duration::from_millis(500);

#[derive(Debug)]
pub(super) struct FinalizedSentence {
    pub text: String,
    /// The triggering normalized contribution, shared by its punctuation finals.
    /// Timeout finals have no source result, matching the pinned Python handler.
    pub result: Option<Arc<Transcript>>,
    /// Last contribution entirely consumed at this final's boundary. This is
    /// source coverage, never proof that the standard user accepted or committed it.
    pub source_through: u64,
}

#[derive(Debug)]
struct FragmentEnd {
    watermark: u64,
    /// UTF-8 byte offset in the currently retained suffix.
    end: usize,
}

#[derive(Debug)]
pub(super) struct SourceFinalizer {
    buffer: String,
    fragments: VecDeque<FragmentEnd>,
    received_watermark: u64,
    covered_watermark: u64,
    deadline: Option<Instant>,
    max_fragment_bytes: usize,
    max_suffix_bytes: usize,
    failed: bool,
}

impl SourceFinalizer {
    /// Bounds apply to each incoming text/language component and the retained
    /// unfinalized suffix. A working buffer can temporarily contain one bounded
    /// suffix plus one bounded fragment while complete sentences are extracted.
    pub(super) fn new(
        max_fragment_bytes: usize,
        max_suffix_bytes: usize,
    ) -> Result<Self, GeminiError> {
        if max_fragment_bytes == 0
            || max_suffix_bytes == 0
            || max_fragment_bytes
                .checked_add(max_suffix_bytes)
                .is_none_or(|n| n > isize::MAX as usize)
        {
            return Err(GeminiError::Invalid("history source finalization bounds"));
        }
        Ok(Self {
            buffer: String::new(),
            fragments: VecDeque::new(),
            received_watermark: 0,
            covered_watermark: 0,
            deadline: None,
            max_fragment_bytes,
            max_suffix_bytes,
            failed: false,
        })
    }

    /// Empty actual text is ignored, including its timer and watermark. Every
    /// nonempty contribution must have the next checked receive watermark.
    pub(super) fn ingest(
        &mut self,
        watermark: u64,
        transcript: &Transcript,
        now: Instant,
    ) -> Result<Vec<FinalizedSentence>, GeminiError> {
        if self.failed {
            return Err(GeminiError::Closed);
        }
        let result = self.ingest_inner(watermark, transcript, now);
        if result.is_err() {
            // An invalid or over-limit contribution cannot be retried after
            // selectively keeping finals or dropping the offending suffix.
            self.failed = true;
            self.buffer.clear();
            self.fragments.clear();
            self.deadline = None;
        }
        result
    }

    fn ingest_inner(
        &mut self,
        watermark: u64,
        transcript: &Transcript,
        now: Instant,
    ) -> Result<Vec<FinalizedSentence>, GeminiError> {
        if transcript.text.is_empty() {
            return Ok(Vec::new());
        }
        let expected = self
            .received_watermark
            .checked_add(1)
            .ok_or(GeminiError::Protocol("history source watermark exhausted"))?;
        if watermark != expected {
            return Err(GeminiError::Protocol(
                "history source watermark gap or replay",
            ));
        }
        if transcript.text.len() > self.max_fragment_bytes
            || transcript
                .language_code
                .as_ref()
                .is_some_and(|code| code.len() > self.max_fragment_bytes)
        {
            return Err(GeminiError::TooLarge);
        }
        let deadline = now
            .checked_add(INACTIVITY)
            .ok_or(GeminiError::Invalid("history source timeout overflow"))?;
        let text = if self.buffer.is_empty() && transcript.text.starts_with(' ') {
            transcript.text.trim_start_matches(python_whitespace)
        } else {
            &transcript.text
        };
        self.received_watermark = watermark;
        if text.is_empty() {
            // Python's conditional lstrip consumed this real contribution.
            // There is no content frame or successful user commit to fabricate.
            self.covered_watermark = watermark;
            self.deadline = None;
            return Ok(Vec::new());
        }
        let end = self
            .buffer
            .len()
            .checked_add(text.len())
            .ok_or(GeminiError::TooLarge)?;
        self.buffer.push_str(text);
        self.fragments.push_back(FragmentEnd { watermark, end });

        let mut finalized = Vec::new();
        let mut consumed = 0usize;
        let mut provenance = None;
        loop {
            let remaining = &self.buffer[consumed..];
            let characters = match_endofsentence(remaining, None);
            if characters == 0 {
                break;
            }
            let bytes = character_boundary(remaining, characters)?;
            let next = consumed.checked_add(bytes).ok_or(GeminiError::TooLarge)?;
            if next <= consumed || next > self.buffer.len() {
                return Err(GeminiError::Protocol("invalid history sentence boundary"));
            }
            while self.fragments.front().is_some_and(|part| part.end <= next) {
                let part = self
                    .fragments
                    .pop_front()
                    .ok_or(GeminiError::Protocol("missing history source fragment"))?;
                self.covered_watermark = part.watermark;
            }
            let text = &self.buffer[consumed..next];
            if !is_no_content(text) {
                let result = provenance.get_or_insert_with(|| Arc::new(transcript.clone()));
                finalized.push(FinalizedSentence {
                    text: text.to_owned(),
                    result: Some(Arc::clone(result)),
                    source_through: self.covered_watermark,
                });
            }
            consumed = next;
        }
        if consumed != 0 {
            drop(self.buffer.drain(..consumed));
            for part in &mut self.fragments {
                part.end = part.end.checked_sub(consumed).ok_or(GeminiError::Protocol(
                    "invalid history source fragment boundary",
                ))?;
            }
        }
        if self.buffer.len() > self.max_suffix_bytes {
            return Err(GeminiError::TooLarge);
        }
        self.deadline = (!self.buffer.is_empty()).then_some(deadline);
        Ok(finalized)
    }

    /// Observe the actor's clock without sleeping. Early or repeated expiration
    /// is a no-op; EOF and turn-complete do not grant permission to flush early.
    pub(super) fn expire(&mut self, now: Instant) -> Result<Vec<FinalizedSentence>, GeminiError> {
        if self.failed {
            return Err(GeminiError::Closed);
        }
        if self.deadline.is_none_or(|deadline| now < deadline) {
            return Ok(Vec::new());
        }
        let text = std::mem::take(&mut self.buffer);
        self.fragments.clear();
        self.covered_watermark = self.received_watermark;
        self.deadline = None;
        if is_no_content(&text) {
            Ok(Vec::new())
        } else {
            Ok(vec![FinalizedSentence {
                text,
                result: None,
                source_through: self.covered_watermark,
            }])
        }
    }

    pub(super) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Includes consumed whitespace-only source results. The actor must still
    /// await real receipts for any earlier nonempty finals covering this prefix.
    pub(super) fn covered_watermark(&self) -> u64 {
        self.covered_watermark
    }
}

fn character_boundary(text: &str, characters: usize) -> Result<usize, GeminiError> {
    text.char_indices()
        .map(|(byte, _)| byte)
        .chain(std::iter::once(text.len()))
        .nth(characters)
        .ok_or(GeminiError::Protocol(
            "invalid history sentence character offset",
        ))
}

// Python str.isspace/lstrip additionally recognizes these four ASCII separators.
fn python_whitespace(character: char) -> bool {
    character.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&character)
}

fn is_no_content(text: &str) -> bool {
    text.chars().all(python_whitespace)
}

#[cfg(test)]
#[path = "finalizer_tests.rs"]
mod tests;
