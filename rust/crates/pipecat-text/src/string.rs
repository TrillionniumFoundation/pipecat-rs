//! SentenceX adapter and literal-delimiter utilities.
use regex::Regex;
use std::sync::LazyLock;

pub const SENTENCE_ENDING_PUNCTUATION: &str = ".!?;…。？！；．｡।॥؟؛۔؏၊။។៕໌།༎։՜՞።፧፨";
pub fn is_sentence_ending(c: char) -> bool {
    SENTENCE_ENDING_PUNCTUATION.contains(c)
}
pub fn resolve_sentence_tokenizer_language(language: Option<&str>) -> String {
    let normalized = language
        .unwrap_or("en")
        .trim()
        .to_lowercase()
        .replace('_', "-");
    match normalized.split('-').next().unwrap_or("") {
        "" | "auto" => "en".into(),
        s => s.into(),
    }
}
fn tokenize(language: &str, text: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut pending = String::new();
    for span in sentencex::segment(language, text) {
        pending.push_str(span);
        if !span.trim().is_empty() {
            let end = pending.trim_end().len();
            sentences.push(pending[..end].to_owned());
            pending = pending[end..].to_owned();
        }
    }
    sentences
}
static QUOTED_WORD_END: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r#"(["'“‘«‹„‟「『]+)[\w.]+\.$"#).unwrap());
/// Return the character offset after the first complete sentence, or zero.
pub fn match_endofsentence(text: &str, language: Option<&str>) -> usize {
    let text = text.trim_end();
    if text.is_empty() {
        return 0;
    }
    let language = resolve_sentence_tokenizer_language(language);
    let mut sentences = tokenize(&language, text);
    if sentences.is_empty() {
        return 0;
    }
    let mut tokenizer_text = text.to_owned();
    if sentences.len() > 1 {
        if let Some(captures) = QUOTED_WORD_END.captures(&sentences[0]) {
            let quote = captures.get(1).unwrap();
            let prev = sentences[0][..quote.start()].chars().next_back();
            if prev.is_none_or(|c| !(crate::alnum_utils::is_alnum(c) || c == '_')) {
                tokenizer_text = format!(
                    "{}{}{}",
                    &text[..quote.start()],
                    " ".repeat(quote.as_str().chars().count()),
                    &text[quote.end()..]
                );
                sentences = tokenize(&language, &tokenizer_text);
            }
        }
    }
    if sentences.len() > 1 {
        return sentences[0].chars().count();
    }
    if sentences.first().is_some_and(|s| s == &tokenizer_text) {
        if text.chars().next_back().is_some_and(is_sentence_ending) {
            return text.chars().count();
        }
        if let Some((i, _)) = text
            .chars()
            .enumerate()
            .find(|(_, c)| is_sentence_ending(*c) && !".!?;…".contains(*c))
        {
            return i + 1;
        }
    }
    0
}
pub(crate) fn char_to_byte(text: &str, offset: usize) -> usize {
    text.char_indices()
        .nth(offset)
        .map_or(text.len(), |(i, _)| i)
}
/// Return a character length, never a byte count.
pub fn longest_trailing_partial_match(text: &str, candidates: &[impl AsRef<str>]) -> usize {
    let mut longest = 0;
    for candidate in candidates {
        let candidate = candidate.as_ref();
        for (length, (end, _)) in candidate.char_indices().enumerate().skip(1) {
            if length > longest && text.ends_with(&candidate[..end]) {
                longest = length;
            }
        }
    }
    longest
}
pub type StartEndTags = (String, String);
/// Parse one increment of source-compatible tag state. Indices are characters.
pub fn parse_start_end_tags(
    text: &str,
    tags: &[StartEndTags],
    current_tag: Option<usize>,
    current_tag_index: usize,
) -> (Option<usize>, usize) {
    let scan = &text[char_to_byte(text, current_tag_index)..];
    if let Some(tag) = current_tag {
        return if scan.contains(&tags[tag].1) {
            (None, text.chars().count())
        } else {
            (current_tag, current_tag_index)
        };
    }
    for (i, (start, end)) in tags.iter().enumerate() {
        let starts = scan.matches(start.as_str()).count();
        let ends = scan.matches(end.as_str()).count();
        if starts == 0 && ends == 0 {
            return (None, current_tag_index);
        }
        if starts > ends {
            return (Some(i), text.chars().count());
        }
        if starts == ends {
            return (None, text.chars().count());
        }
    }
    (None, current_tag_index)
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextPartForConcatenation {
    pub text: String,
    pub includes_inter_part_spaces: bool,
}
pub fn concatenate_aggregated_text(parts: &[TextPartForConcatenation]) -> String {
    let mut result = String::new();
    let mut last_spaces = false;
    for part in parts.iter().filter(|p| !p.text.is_empty()) {
        if !result.is_empty() {
            let add_space = if last_spaces == part.includes_inter_part_spaces {
                !last_spaces
            } else {
                !result.chars().next_back().unwrap().is_whitespace()
                    && !part.text.chars().next().unwrap().is_whitespace()
            };
            if add_space {
                result.push(' ');
            }
        }
        result.push_str(&part.text);
        last_spaces = part.includes_inter_part_spaces;
    }
    result.trim().to_owned()
}
