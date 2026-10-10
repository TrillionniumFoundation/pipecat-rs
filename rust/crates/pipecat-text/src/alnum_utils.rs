//! Unicode folding and character-indexed cursors shared by timestamp matching.
use crate::markup_utils::strip_complete_markup;
use crate::unicode_categories::{is_letter, is_nonspacing_mark, is_number, is_punctuation};
/// Unicode category database version used by the native matching helpers.
pub const UNICODE_VERSION: &str = crate::unicode_categories::UNICODE_VERSION;
use unicode_normalization::UnicodeNormalization;
pub(crate) fn is_alnum(c: char) -> bool {
    is_letter(c) || is_number(c)
}
/// Python's Unicode whitespace predicate includes the information separators.
pub(crate) fn is_whitespace(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}
pub fn strip_trailing_punctuation(text: &str) -> String {
    text.trim_end_matches(|c: char| is_punctuation(c))
        .to_owned()
}
pub fn fold_typography(text: &str) -> String {
    text.chars()
        .map(|c| match c {
            '‘' | '’' | 'ʼ' => '\'',
            '“' | '”' => '"',
            '–' | '—' => '-',
            c => c,
        })
        .collect()
}
fn fold_accented_char(c: char) -> String {
    let decomposed: Vec<char> = c.to_string().nfd().collect();
    if decomposed.len() >= 2 && is_nonspacing_mark(decomposed[1]) {
        decomposed[0].to_lowercase().collect()
    } else {
        c.to_lowercase().collect()
    }
}
pub fn fold_for_matching(text: &str) -> String {
    let folded: String = text
        .chars()
        .map(|c| {
            if is_letter(c) {
                fold_accented_char(c)
            } else {
                c.to_string()
            }
        })
        .collect();
    fold_typography(&folded)
}
pub fn alnum_only(text: &str) -> String {
    strip_complete_markup(text)
        .chars()
        .filter(|&c| is_alnum(c))
        .map(|c| {
            if is_letter(c) {
                fold_accented_char(c)
            } else {
                c.to_lowercase().collect()
            }
        })
        .collect()
}
pub fn has_alnum(text: &str) -> bool {
    strip_complete_markup(text).chars().any(is_alnum)
}
/// Cursor inputs and output count Unicode scalar values, not UTF-8 bytes.
pub fn advance_by_chars(text: &str, start_pos: usize, n: usize) -> usize {
    advance(text, start_pos, n, false)
}
pub fn advance_by_alnums(text: &str, start_pos: usize, n: usize) -> usize {
    advance(text, start_pos, n, true)
}
fn advance(text: &str, start_pos: usize, n: usize, alnum: bool) -> usize {
    let chars: Vec<char> = text.chars().collect();
    let mut pos = start_pos;
    let mut count = 0;
    while pos < chars.len() && count < n {
        if chars[pos] == '<' {
            pos = chars[pos..]
                .iter()
                .position(|&c| c == '>')
                .map_or(pos + 1, |end| pos + end + 1);
        } else {
            if !alnum || is_alnum(chars[pos]) {
                count += 1;
            }
            pos += 1;
        }
    }
    if alnum {
        while pos < chars.len()
            && chars[pos] != '<'
            && !is_alnum(chars[pos])
            && !is_whitespace(chars[pos])
        {
            pos += 1;
        }
    }
    pos
}
