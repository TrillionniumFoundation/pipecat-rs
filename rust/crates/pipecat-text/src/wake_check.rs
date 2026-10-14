//! Bounded literal matcher for the deprecated Python `WakeCheckFilter`.
//!
//! This migration profile accepts nonempty ASCII phrase tokens separated by
//! Python whitespace. It matches the source's `\b` + `\s*`-joined escaped
//! tokens + `\b`, with Python's Unicode `IGNORECASE` behavior for ASCII
//! literals. The boundary alphabet is Unicode 15 letters/numbers and `_`;
//! neither combining marks nor hyphens are word characters. There is no Unicode
//! normalization, and `ß` does not equal `ss`. Empty configurations and empty
//! phrases are deliberately rejected rather than adopting the source's empty
//! regular expression behavior.
//!
//! Phrase order wins over earliest position across phrases. Within the first
//! matching phrase, the earliest start wins. Returned ranges are **UTF-8 byte
//! offsets** into the unchanged input, unlike the crate's character-indexed
//! alignment utilities. This module does not implement activation, participant
//! state, frame routing, keepalive, or the recommended wake-phrase turn strategy.

use crate::alnum_utils::is_whitespace;
use crate::unicode_categories::{is_letter, is_number};
use std::fmt;
use std::ops::Range;

/// Positive per-construction and per-search limits, with fixed hard ceilings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WakeCheckLimits {
    /// Visited phrases, including duplicates. Ceiling: 128; default: 32.
    pub max_phrases: usize,
    /// Raw UTF-8 bytes per phrase, before splitting. Ceiling: 1,024; default: 256.
    pub max_phrase_bytes: usize,
    /// Cumulative raw phrase bytes, including whitespace and duplicates.
    /// Ceiling: 65,536; default: 8,192. This also bounds constructor work.
    pub max_total_phrase_bytes: usize,
    /// UTF-8 bytes in one search input. Ceiling: 65,536; default: 16,384.
    pub max_input_bytes: usize,
    /// Search operations per `find` call. Ceiling: 4,194,304; default: 1,048,576.
    /// One operation is charged for each phrase visited, scalar scanned for a
    /// possible start, attempted literal-scalar comparison, whitespace lookahead,
    /// or trailing-boundary lookahead. Revisited scalars are charged again. The
    /// budget is shared across phrases and resets on every call. This bounds
    /// synchronous search work; it is not a wall-clock or preemption guarantee.
    pub max_match_work: usize,
}

impl Default for WakeCheckLimits {
    fn default() -> Self {
        Self {
            max_phrases: 32,
            max_phrase_bytes: 256,
            max_total_phrase_bytes: 8 * 1024,
            max_input_bytes: 16 * 1024,
            max_match_work: 1024 * 1024,
        }
    }
}

impl WakeCheckLimits {
    fn validate(self) -> Result<(), WakeCheckError> {
        for (value, ceiling) in [
            (self.max_phrases, 128),
            (self.max_phrase_bytes, 1024),
            (self.max_total_phrase_bytes, 64 * 1024),
            (self.max_input_bytes, 64 * 1024),
            (self.max_match_work, 4 * 1024 * 1024),
        ] {
            if value == 0 || value > ceiling {
                return Err(WakeCheckError::InvalidLimits);
            }
        }
        Ok(())
    }
}

/// Payload-free errors never expose configured phrases or input text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakeCheckError {
    InvalidLimits,
    EmptyPhrases,
    TooManyPhrases,
    PhraseTooLong,
    TotalPhraseBytesExceeded,
    EmptyPhrase,
    NonAsciiToken,
    InputTooLong,
    WorkLimitExceeded,
}

impl fmt::Display for WakeCheckError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "wake-check limits must be positive and within hard ceilings",
            Self::EmptyPhrases => "wake-check requires at least one phrase",
            Self::TooManyPhrases => "wake-check phrase count limit exceeded",
            Self::PhraseTooLong => "wake-check raw phrase byte limit exceeded",
            Self::TotalPhraseBytesExceeded => {
                "wake-check cumulative raw phrase byte limit exceeded"
            }
            Self::EmptyPhrase => "wake-check phrases must contain a non-whitespace token",
            Self::NonAsciiToken => "wake-check phrase tokens must be ASCII",
            Self::InputTooLong => "wake-check input byte limit exceeded",
            Self::WorkLimitExceeded => "wake-check search work limit exceeded",
        })
    }
}

impl std::error::Error for WakeCheckError {}

/// Ranges refer to the exact input passed to `find`; all offsets count UTF-8 bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WakeCheckMatch {
    /// Index in the original configured phrase sequence, including duplicates.
    pub phrase_index: usize,
    /// The literal phrase and any intervening matched whitespace.
    pub match_range: Range<usize>,
    /// The source filter's retained suffix, beginning at the wake phrase.
    pub suffix_range: Range<usize>,
}

#[derive(Clone)]
struct Phrase {
    tokens: Vec<Box<[u8]>>,
}

/// Immutable, pure matcher for the explicitly bounded legacy migration profile.
#[derive(Clone)]
pub struct WakeCheckMatcher {
    phrases: Vec<Phrase>,
    limits: WakeCheckLimits,
}

impl fmt::Debug for WakeCheckMatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WakeCheckMatcher")
            .field("phrase_count", &self.phrases.len())
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl WakeCheckMatcher {
    /// Compile ASCII literals after Python-whitespace splitting. Construction
    /// scans only bounded raw phrase bytes and never first collects the iterator.
    /// Every visited entry counts, including duplicate phrases. Literal regex
    /// punctuation is accepted without acquiring any regex operator meaning.
    pub fn new<I, S>(phrases: I, limits: WakeCheckLimits) -> Result<Self, WakeCheckError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        limits.validate()?;
        let mut compiled = Vec::new();
        let mut total_bytes = 0usize;
        for phrase in phrases {
            if compiled.len() >= limits.max_phrases {
                return Err(WakeCheckError::TooManyPhrases);
            }
            let phrase = phrase.as_ref();
            if phrase.len() > limits.max_phrase_bytes {
                return Err(WakeCheckError::PhraseTooLong);
            }
            total_bytes = total_bytes
                .checked_add(phrase.len())
                .filter(|&total| total <= limits.max_total_phrase_bytes)
                .ok_or(WakeCheckError::TotalPhraseBytesExceeded)?;
            let mut tokens = Vec::new();
            for token in phrase
                .split(is_whitespace)
                .filter(|token| !token.is_empty())
            {
                if !token.is_ascii() {
                    return Err(WakeCheckError::NonAsciiToken);
                }
                tokens.push(token.to_ascii_lowercase().into_bytes().into_boxed_slice());
            }
            if tokens.is_empty() {
                return Err(WakeCheckError::EmptyPhrase);
            }
            compiled.push(Phrase { tokens });
        }
        if compiled.is_empty() {
            return Err(WakeCheckError::EmptyPhrases);
        }
        Ok(Self {
            phrases: compiled,
            limits,
        })
    }

    /// Find the first configured phrase that occurs anywhere in the input, then
    /// its earliest start. No text is copied or modified. An exhausted budget is
    /// an error, never a partial match or a false no-match. The budget resets for
    /// every invocation, including after errors; no mutable search state is kept.
    pub fn find(&self, text: &str) -> Result<Option<WakeCheckMatch>, WakeCheckError> {
        if text.len() > self.limits.max_input_bytes {
            return Err(WakeCheckError::InputTooLong);
        }
        let mut work = WorkBudget(self.limits.max_match_work);
        for (phrase_index, phrase) in self.phrases.iter().enumerate() {
            work.charge()?;
            let mut previous_word = false;
            for (start, scalar) in text.char_indices() {
                work.charge()?;
                let current_word = is_word(scalar);
                // Python \b is XOR, including when the literal starts with
                // punctuation. It is not merely a ban on adjacent word chars.
                if previous_word != current_word {
                    if let Some(end) = match_at(text, start, phrase, &mut work)? {
                        return Ok(Some(WakeCheckMatch {
                            phrase_index,
                            match_range: start..end,
                            suffix_range: start..text.len(),
                        }));
                    }
                }
                previous_word = current_word;
            }
        }
        Ok(None)
    }
}

struct WorkBudget(usize);

impl WorkBudget {
    fn charge(&mut self) -> Result<(), WakeCheckError> {
        self.0 = self
            .0
            .checked_sub(1)
            .ok_or(WakeCheckError::WorkLimitExceeded)?;
        Ok(())
    }
}

fn is_word(scalar: char) -> bool {
    scalar == '_' || is_letter(scalar) || is_number(scalar)
}

fn matches_ascii_literal(scalar: char, literal: u8) -> bool {
    match scalar {
        'İ' | 'ı' => literal == b'i',
        'ſ' => literal == b's',
        'K' => literal == b'k',
        c if c.is_ascii() => c.to_ascii_lowercase() as u8 == literal,
        _ => false,
    }
}

fn match_at(
    text: &str,
    start: usize,
    phrase: &Phrase,
    work: &mut WorkBudget,
) -> Result<Option<usize>, WakeCheckError> {
    let mut input = text[start..].char_indices().peekable();
    let mut end = start;
    let mut last_word = false;
    for (index, token) in phrase.tokens.iter().enumerate() {
        if index != 0 {
            // Tokens contain no Python whitespace. Thus greedily consuming
            // \s* needs no backtracking: the next literal cannot consume any
            // skipped scalar, including Python's U+001C..U+001F separators.
            loop {
                work.charge()?;
                if input.peek().is_some_and(|&(_, c)| is_whitespace(c)) {
                    input.next();
                } else {
                    break;
                }
            }
        }
        for &literal in token.iter() {
            work.charge()?;
            let Some((offset, scalar)) = input.next() else {
                return Ok(None);
            };
            if !matches_ascii_literal(scalar, literal) {
                return Ok(None);
            }
            end = start + offset + scalar.len_utf8();
            last_word = is_word(scalar);
        }
    }
    work.charge()?;
    let next_word = input.peek().is_some_and(|&(_, scalar)| is_word(scalar));
    Ok((last_word != next_word).then_some(end))
}
