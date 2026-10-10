//! Fragment-tolerant and complete-text XML/SSML markup operations.
use regex::Regex;
use std::sync::LazyLock;
static COMPLETE_MARKUP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"<[^>]+>").unwrap());

pub fn strip_markup(text: &str) -> String {
    let mut in_tag = false;
    text.chars()
        .filter(|&c| {
            if in_tag {
                if c == '>' {
                    in_tag = false;
                }
                false
            } else if c == '<' {
                in_tag = true;
                false
            } else {
                true
            }
        })
        .collect()
}
pub fn strip_complete_markup(text: &str) -> String {
    COMPLETE_MARKUP.replace_all(text, "").into_owned()
}
/// Return a Unicode character offset, including any skipped markup.
pub fn raw_offset_after_clean_chars(text: &str, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    let mut seen = 0;
    let mut in_tag = false;
    for (i, c) in text.chars().enumerate() {
        if in_tag {
            if c == '>' {
                in_tag = false;
            }
        } else if c == '<' {
            in_tag = true;
        } else {
            seen += 1;
            if seen == n {
                return i + 1;
            }
        }
    }
    text.chars().count()
}
pub fn split_markup_runs(text: &str) -> Vec<String> {
    let spans: Vec<_> = COMPLETE_MARKUP.find_iter(text).map(|m| m.range()).collect();
    if spans.is_empty() {
        return if text.is_empty() {
            vec![]
        } else {
            vec![text.to_owned()]
        };
    }
    let mut tokens = vec![];
    let mut begin = 0;
    let mut whitespace = None;
    for (i, c) in text.char_indices() {
        let next = crate::alnum_utils::is_whitespace(c);
        if whitespace.is_some_and(|previous| previous != next) {
            tokens.push(begin..i);
            begin = i;
        }
        whitespace = Some(next);
    }
    tokens.push(begin..text.len());
    let mut result: Vec<String> = vec![];
    let mut last_tagged = None;
    for range in tokens {
        let tagged = spans
            .iter()
            .any(|s| s.start < range.end && range.start < s.end);
        if last_tagged == Some(tagged) {
            result.last_mut().unwrap().push_str(&text[range]);
        } else {
            result.push(text[range].to_owned());
            last_tagged = Some(tagged);
        }
    }
    result
}
