//! Bounded, opt-in Cartesia inline-IPA transformation for prepared TTS input.
//!
//! This is a notation formatter, not an IPA validator or a markup sanitizer.
//! Unknown symbols are preserved. Keys are trimmed then restricted to ASCII;
//! matching folds ASCII case only, with Unicode 15 letter/number, underscore,
//! and ASCII-hyphen boundaries. Non-ASCII case-fold equivalences are excluded.
//! Original markup is ordinary input and can contain matches. Only replacement
//! strings generated during this invocation are never rescanned. Apply exactly
//! once, after preparing synthesis text and only for a compatible provider.

use crate::alnum_utils::is_whitespace;
use crate::unicode_categories::{is_letter, is_nonspacing_mark, is_number};
use pipecat_core::TtsTextInputFrame;
use std::fmt;
use unicode_normalization::UnicodeNormalization;

const MAX_WORK_BYTES: usize = 128 * 1024;
const PRIMARY_STRESS: &str = "ˈ";
const SECONDARY_STRESS: &str = "ˌ";
const MULTI: [&str; 7] = ["tʃ", "dʒ", "aɪ", "aʊ", "ɔɪ", "oʊ", "eɪ"];

/// Positive byte/count budgets. Configuration cannot exceed the documented
/// hard ceilings, even when supplied by an untrusted caller.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PronunciationLimits {
    /// All visited entries, including empty keys and duplicates. Ceiling: 1,024.
    pub max_entries: usize,
    /// Raw key bytes, before trimming. Ceiling: 1,024.
    pub max_word_bytes: usize,
    /// Raw IPA bytes per entry or standalone formatter call. Ceiling: 16,384.
    pub max_ipa_bytes: usize,
    /// Cumulative raw key + raw IPA + formatted bytes, including overwritten
    /// and ignored entries. Ceiling: 4 MiB.
    pub max_total_bytes: usize,
    /// Bytes in the original synthesis input. Ceiling: 1 MiB.
    pub max_input_bytes: usize,
    /// Bytes in one formatted pronunciation or complete transformed output.
    /// Ceiling: 4 MiB.
    pub max_output_bytes: usize,
}

impl Default for PronunciationLimits {
    fn default() -> Self {
        Self {
            max_entries: 256,
            max_word_bytes: 256,
            max_ipa_bytes: 2048,
            max_total_bytes: 256 * 1024,
            max_input_bytes: 64 * 1024,
            max_output_bytes: 256 * 1024,
        }
    }
}

impl PronunciationLimits {
    fn validate(self) -> Result<()> {
        for (value, ceiling) in [
            (self.max_entries, 1024),
            (self.max_word_bytes, 1024),
            (self.max_ipa_bytes, 16 * 1024),
            (self.max_total_bytes, 4 * 1024 * 1024),
            (self.max_input_bytes, 1024 * 1024),
            (self.max_output_bytes, 4 * 1024 * 1024),
        ] {
            if value == 0 || value > ceiling {
                return Err(PronunciationError::InvalidLimits);
            }
        }
        Ok(())
    }
}

/// Payload-free failures: neither display nor debug contains user text or IPA.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PronunciationError {
    InvalidLimits,
    TooManyEntries,
    WordTooLong,
    NonAsciiWord,
    IpaTooLong,
    TotalBytesExceeded,
    InputTooLong,
    OutputTooLong,
    WorkLimitExceeded,
}

impl fmt::Display for PronunciationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidLimits => "pronunciation limits must be positive and within hard ceilings",
            Self::TooManyEntries => "pronunciation entry limit exceeded",
            Self::WordTooLong => "pronunciation key byte limit exceeded",
            Self::NonAsciiWord => "pronunciation keys must be ASCII after trimming",
            Self::IpaTooLong => "pronunciation IPA byte limit exceeded",
            Self::TotalBytesExceeded => "pronunciation cumulative byte limit exceeded",
            Self::InputTooLong => "pronunciation input byte limit exceeded",
            Self::OutputTooLong => "pronunciation output byte limit exceeded",
            Self::WorkLimitExceeded => "pronunciation key comparison work limit exceeded",
        })
    }
}
impl std::error::Error for PronunciationError {}

type Result<T> = std::result::Result<T, PronunciationError>;

fn add_budget(total: &mut usize, amount: usize, maximum: usize) -> Result<()> {
    *total = total
        .checked_add(amount)
        .filter(|&value| value <= maximum)
        .ok_or(PronunciationError::TotalBytesExceeded)?;
    Ok(())
}

fn charge_comparison(work: &mut usize, key_bytes: usize) -> Result<()> {
    *work = key_bytes
        .checked_add(1)
        .and_then(|cost| work.checked_add(cost))
        .filter(|&value| value <= CartesiaPronunciationTransform::MAX_COMPARISON_WORK)
        .ok_or(PronunciationError::WorkLimitExceeded)?;
    Ok(())
}

fn append(output: &mut String, text: &str, maximum: usize) -> Result<()> {
    output
        .len()
        .checked_add(text.len())
        .filter(|&value| value <= maximum)
        .ok_or(PronunciationError::OutputTooLong)?;
    output.push_str(text);
    Ok(())
}

fn append_char(output: &mut String, c: char) -> Result<()> {
    append(output, c.encode_utf8(&mut [0; 4]), MAX_WORK_BYTES)
}

fn replace_notation(input: &str, old: &str, new: &str) -> Result<String> {
    let mut output = String::new();
    let mut cursor = 0;
    for (start, matched) in input.match_indices(old) {
        append(&mut output, &input[cursor..start], MAX_WORK_BYTES)?;
        append(&mut output, new, MAX_WORK_BYTES)?;
        cursor = start + matched.len(); // Both offsets belong to bounded input.
    }
    append(&mut output, &input[cursor..], MAX_WORK_BYTES)?;
    Ok(output)
}

fn normalize_ipa(ipa: &str) -> Result<String> {
    let mut normalized = String::new();
    for c in ipa.trim_matches(is_whitespace).nfc() {
        append_char(&mut normalized, c)?;
    }
    let unwrapped = if (normalized.starts_with('/') && normalized.ends_with('/'))
        || (normalized.starts_with('[') && normalized.ends_with(']'))
    {
        if normalized.len() >= 2 {
            &normalized[1..normalized.len() - 1]
        } else {
            normalized.as_str()
        }
    } else {
        normalized.as_str()
    };
    let mut value = unwrapped.to_owned();
    for (old, new) in [
        ("\u{035c}", "\u{0361}"),
        ("ʧ", "tʃ"),
        ("ʤ", "dʒ"),
        ("t\u{0361}ʃ", "tʃ"),
        ("d\u{0361}ʒ", "dʒ"),
        ("ʦ", "t\u{0361}s"),
        ("ʣ", "d\u{0361}z"),
        ("g", "ɡ"),
        ("'", PRIMARY_STRESS),
        (":", "ː"),
        (".", ""),
    ] {
        value = replace_notation(&value, old, new)?;
    }
    let mut output = String::new();
    for word in value.split(is_whitespace).filter(|word| !word.is_empty()) {
        if !output.is_empty() {
            append(&mut output, " ", MAX_WORK_BYTES)?;
        }
        append(&mut output, word, MAX_WORK_BYTES)?;
    }
    Ok(output)
}

fn is_stress(phone: &str) -> bool {
    phone == PRIMARY_STRESS || phone == SECONDARY_STRESS
}
fn is_modifier(c: char) -> bool {
    matches!(c, 'ː' | 'ˑ') || is_nonspacing_mark(c)
}
fn is_ipa_vowel(phone: &str) -> bool {
    phone
        .chars()
        .next()
        .is_some_and(|c| "aeiouyæɑɒɔəɚɛɜɝɪʊʌɐᵻɨʉɯɤøœɶɵɘɞ".contains(c))
}

fn ipa_phones(ipa: &str) -> Result<Vec<String>> {
    // The source normalizes each word again, including wrapper removal.
    let normalized = normalize_ipa(ipa)?;
    let chars: Vec<char> = normalized.chars().filter(|&c| c != ' ').collect();
    let mut phones: Vec<String> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if matches!(c, 'ˈ' | 'ˌ') {
            phones.push(c.to_string());
            i += 1;
            continue;
        }
        let mut phone = String::new();
        if chars.get(i + 1) == Some(&'\u{0361}') && i + 2 < chars.len() {
            append_char(&mut phone, c)?;
            append_char(&mut phone, chars[i + 2])?;
            i += 3;
        } else if let Some(multi) = MULTI.iter().find(|multi| {
            let mut expected = multi.chars();
            expected.next() == Some(c) && expected.next() == chars.get(i + 1).copied()
        }) {
            append(&mut phone, multi, MAX_WORK_BYTES)?;
            i += 2;
        } else {
            append_char(&mut phone, c)?;
            i += 1;
        }
        while i < chars.len() && is_modifier(chars[i]) {
            append_char(&mut phone, chars[i])?;
            i += 1;
        }
        if phone.chars().all(is_modifier) {
            if let Some(previous) = phones.last_mut().filter(|previous| !is_stress(previous)) {
                append(previous, &phone, MAX_WORK_BYTES)?;
                continue;
            }
        }
        phones.push(phone);
    }
    Ok(phones)
}

fn stress_before_vowels(phones: Vec<String>) -> Vec<String> {
    let mut output = Vec::new();
    let mut pending = None;
    for phone in phones {
        if is_stress(&phone) {
            pending = Some(phone);
            continue;
        }
        if is_ipa_vowel(&phone) {
            if let Some(stress) = pending.take() {
                output.push(stress);
            }
        }
        output.push(phone);
    }
    output
}

/// Render source-compatible Cartesia inline phones. Empty normalized IPA gives
/// `None`; a stress-only word gives `Some("<<>>")`. Unknown symbols, including
/// markup metacharacters, are preserved, not validated or escaped.
///
/// All limits must be valid. Raw IPA and final output have configurable byte
/// limits; normalization/phone intermediates have an independent 128 KiB hard
/// bound, and their counts cannot exceed that bounded number of input scalars.
pub fn format_cartesia_pronunciation(
    ipa: &str,
    limits: PronunciationLimits,
) -> Result<Option<String>> {
    limits.validate()?;
    if ipa.len() > limits.max_ipa_bytes {
        return Err(PronunciationError::IpaTooLong);
    }
    let normalized = normalize_ipa(ipa)?;
    let mut output = String::new();
    for word in normalized.split(' ').filter(|word| !word.is_empty()) {
        if !output.is_empty() {
            append(&mut output, " ", limits.max_output_bytes)?;
        }
        append(&mut output, "<<", limits.max_output_bytes)?;
        for (index, phone) in stress_before_vowels(ipa_phones(word)?).iter().enumerate() {
            if index != 0 {
                append(&mut output, "|", limits.max_output_bytes)?;
            }
            append(&mut output, phone, limits.max_output_bytes)?;
        }
        append(&mut output, ">>", limits.max_output_bytes)?;
    }
    Ok((!output.is_empty()).then_some(output))
}

#[derive(Clone)]
struct Entry {
    key: String,
    formatted: String,
}

/// Preformatted, immutable replacement map. Construction and application fail
/// atomically on budget errors. Empty/unusable entries are ignored; a later
/// unusable duplicate does not remove an earlier usable pronunciation.
#[derive(Clone)]
pub struct CartesiaPronunciationTransform {
    entries: Vec<Entry>,
    limits: PronunciationLimits,
}
impl fmt::Debug for CartesiaPronunciationTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CartesiaPronunciationTransform")
            .field("entry_count", &self.entries.len())
            .field("limits", &self.limits)
            .finish()
    }
}

impl CartesiaPronunciationTransform {
    /// Fixed cumulative key-comparison work ceiling for each construction or
    /// application: 8 MiB of charged work. Every candidate costs `1 + key.len()`
    /// before comparison, even if lengths differ or an input slice is absent.
    /// This is a conservative comparison budget, not elapsed time or memory.
    pub const MAX_COMPARISON_WORK: usize = 8 * 1024 * 1024;

    pub fn new(
        entries: impl IntoIterator<Item = (String, String)>,
        limits: PronunciationLimits,
    ) -> Result<Self> {
        limits.validate()?;
        let mut prepared: Vec<Entry> = Vec::new();
        let mut total = 0;
        let mut comparison_work = 0;
        for (index, (raw_word, ipa)) in entries.into_iter().enumerate() {
            if index >= limits.max_entries {
                return Err(PronunciationError::TooManyEntries);
            }
            if raw_word.len() > limits.max_word_bytes {
                return Err(PronunciationError::WordTooLong);
            }
            if ipa.len() > limits.max_ipa_bytes {
                return Err(PronunciationError::IpaTooLong);
            }
            add_budget(&mut total, raw_word.len(), limits.max_total_bytes)?;
            add_budget(&mut total, ipa.len(), limits.max_total_bytes)?;
            let word = raw_word.trim_matches(is_whitespace);
            if word.is_empty() {
                continue;
            }
            if !word.is_ascii() {
                return Err(PronunciationError::NonAsciiWord);
            }
            let Some(formatted) = format_cartesia_pronunciation(&ipa, limits)? else {
                continue;
            };
            add_budget(&mut total, formatted.len(), limits.max_total_bytes)?;
            let key = word.to_ascii_lowercase();
            let mut existing_index = None;
            for (index, entry) in prepared.iter().enumerate() {
                charge_comparison(&mut comparison_work, entry.key.len())?;
                if entry.key == key {
                    existing_index = Some(index);
                    break;
                }
            }
            if let Some(index) = existing_index {
                prepared[index].formatted = formatted;
            } else {
                prepared.push(Entry { key, formatted });
            }
        }
        // Stable sort retains the first insertion order for equal-length keys.
        prepared.sort_by_key(|entry| std::cmp::Reverse(entry.key.len()));
        Ok(Self {
            entries: prepared,
            limits,
        })
    }

    /// Match only the original input, left to right, longest first. Existing
    /// markup receives no special treatment and may itself contain matches.
    /// Exceeding [`Self::MAX_COMPARISON_WORK`] returns an atomic error.
    pub fn apply(&self, text: &str) -> Result<String> {
        if text.len() > self.limits.max_input_bytes {
            return Err(PronunciationError::InputTooLong);
        }
        let mut output = String::new();
        let mut cursor = 0;
        let mut previous = None;
        let mut comparison_work = 0;
        while cursor < text.len() {
            let remaining = &text[cursor..];
            let mut matched = None;
            if !previous.is_some_and(is_word_boundary_char) {
                for entry in &self.entries {
                    charge_comparison(&mut comparison_work, entry.key.len())?;
                    let Some(prefix) = remaining.get(..entry.key.len()) else {
                        continue;
                    };
                    if prefix.eq_ignore_ascii_case(&entry.key)
                        && !remaining[entry.key.len()..]
                            .chars()
                            .next()
                            .is_some_and(is_word_boundary_char)
                    {
                        matched = Some(entry);
                        break;
                    }
                }
            }
            if let Some(entry) = matched {
                append(&mut output, &entry.formatted, self.limits.max_output_bytes)?;
                previous = entry.key.chars().next_back();
                cursor += entry.key.len();
            } else if let Some(c) = remaining.chars().next() {
                append(
                    &mut output,
                    c.encode_utf8(&mut [0; 4]),
                    self.limits.max_output_bytes,
                )?;
                previous = Some(c);
                cursor += c.len_utf8();
            }
        }
        Ok(output)
    }

    /// Change only synthesis text, after the entire transform succeeds. A
    /// skipped frame is a strict no-op, even if its text exceeds the budgets.
    /// No request, synthesis, playback, or history event is constructed here.
    pub fn apply_to_input(&self, input: &mut TtsTextInputFrame) -> Result<()> {
        if input.skip_tts {
            return Ok(());
        }
        let transformed = self.apply(&input.synthesis_text)?;
        input.synthesis_text = transformed;
        Ok(())
    }
}

fn is_word_boundary_char(c: char) -> bool {
    c == '_' || c == '-' || is_letter(c) || is_number(c)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn comparison_budget_rejects_expensive_construction() {
        let entries = (0..130).map(|index| (format!("{index:01024}"), "a".to_owned()));
        let result = CartesiaPronunciationTransform::new(
            entries,
            PronunciationLimits {
                max_word_bytes: 1024,
                ..PronunciationLimits::default()
            },
        );
        assert!(matches!(result, Err(PronunciationError::WorkLimitExceeded)));
    }

    #[test]
    fn comparison_budget_rejects_expensive_application_atomically() {
        let key = format!("{}!", "~".repeat(1023));
        let transform = CartesiaPronunciationTransform::new(
            [(key, "a".to_owned())],
            PronunciationLimits {
                max_word_bytes: 1024,
                ..PronunciationLimits::default()
            },
        )
        .unwrap();
        let mut frame = TtsTextInputFrame::new("~".repeat(9000));
        let original = frame.clone();
        assert_eq!(
            transform.apply_to_input(&mut frame),
            Err(PronunciationError::WorkLimitExceeded)
        );
        assert_eq!(frame, original);
        assert_eq!(transform.apply("~").unwrap(), "~");
    }

    #[test]
    fn comparison_budget_is_checked_before_work_and_cannot_overflow() {
        let maximum = CartesiaPronunciationTransform::MAX_COMPARISON_WORK;
        let mut work = maximum - 2;
        charge_comparison(&mut work, 1).unwrap();
        assert_eq!(work, maximum);
        assert_eq!(
            charge_comparison(&mut work, 0),
            Err(PronunciationError::WorkLimitExceeded)
        );
        assert_eq!(work, maximum);
        let mut work = 0;
        assert_eq!(
            charge_comparison(&mut work, usize::MAX),
            Err(PronunciationError::WorkLimitExceeded)
        );
        assert_eq!(work, 0);
    }

    #[test]
    fn private_helpers_match_fixed_original_source_oracle() {
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/pronunciations_source.json"))
                .unwrap();
        for case in fixture["format_cases"].as_array().unwrap() {
            let ipa = case["ipa"].as_str().unwrap();
            assert_eq!(
                normalize_ipa(ipa).unwrap(),
                case["normalized"].as_str().unwrap()
            );
            let phones = ipa_phones(ipa).unwrap();
            let expected: Vec<&str> = case["phones"]
                .as_array()
                .unwrap()
                .iter()
                .map(|phone| phone.as_str().unwrap())
                .collect();
            assert_eq!(phones, expected, "phones: {ipa:?}");
            let expected: Vec<&str> = case["stressed"]
                .as_array()
                .unwrap()
                .iter()
                .map(|phone| phone.as_str().unwrap())
                .collect();
            assert_eq!(stress_before_vowels(phones), expected, "stress: {ipa:?}");
        }
    }

    #[test]
    fn helper_source_semantics() {
        assert_eq!(normalize_ipa(" /ˈʧɪ.kən/ ").unwrap(), "ˈtʃɪkən");
        assert_eq!(normalize_ipa("[g:'ʦʣt\u{035c}ʃ]").unwrap(), "ɡːˈt͡sd͡ztʃ");
        assert_eq!(ipa_phones("t͡s aɪ n̩ː").unwrap(), ["ts", "aɪ", "n̩ː"]);
        assert_eq!(ipa_phones("ˈːa").unwrap(), ["ˈ", "ː", "a"]);
        assert_eq!(
            stress_before_vowels(ipa_phones("mɛtˈfɔɹmɪnˌ").unwrap()),
            ["m", "ɛ", "t", "f", "ˈ", "ɔ", "ɹ", "m", "ɪ", "n"]
        );
    }
}
