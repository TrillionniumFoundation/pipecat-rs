use crate::string::{
    char_to_byte, is_sentence_ending, match_endofsentence, resolve_sentence_tokenizer_language,
};
use crate::{Aggregation, AggregationType, TextAggregator};

#[derive(Clone, Copy, Debug, Default)]
enum LookaheadState {
    #[default]
    Idle,
    AwaitingCharacter,
    AwaitingWord,
    InWord,
}
#[derive(Clone, Debug)]
pub struct SimpleTextAggregator {
    pub(crate) buffer: String,
    pub(crate) mode: AggregationType,
    language: String,
    lookahead: LookaheadState,
}
impl Default for SimpleTextAggregator {
    fn default() -> Self {
        Self::new(AggregationType::Sentence, None)
    }
}
impl SimpleTextAggregator {
    pub fn new(mode: AggregationType, language: Option<&str>) -> Self {
        Self {
            buffer: String::new(),
            mode,
            language: resolve_sentence_tokenizer_language(language),
            lookahead: LookaheadState::Idle,
        }
    }
    fn advance_lookahead(&mut self, c: char, punctuation: bool) -> bool {
        match self.lookahead {
            LookaheadState::Idle => false,
            LookaheadState::AwaitingCharacter => {
                if c.is_whitespace() || punctuation {
                    return false;
                }
                self.lookahead = if crate::alnum_utils::is_alnum(c) {
                    LookaheadState::InWord
                } else {
                    LookaheadState::AwaitingWord
                };
                true
            }
            LookaheadState::AwaitingWord => {
                if crate::alnum_utils::is_alnum(c) {
                    self.lookahead = LookaheadState::InWord;
                }
                false
            }
            LookaheadState::InWord => {
                if c.is_whitespace() || punctuation {
                    self.lookahead = LookaheadState::Idle;
                    true
                } else {
                    false
                }
            }
        }
    }
    pub(crate) fn check_sentence(&mut self, c: char) -> Option<Aggregation> {
        self.check_sentence_using(c, |text, language| {
            match_endofsentence(text, Some(language))
        })
    }
    pub(crate) fn check_sentence_using(
        &mut self,
        c: char,
        mut matcher: impl FnMut(&str, &str) -> usize,
    ) -> Option<Aggregation> {
        let punctuation = is_sentence_ending(c);
        let mut result = None;
        if self.advance_lookahead(c, punctuation) {
            let end = if punctuation {
                self.buffer.len().saturating_sub(c.len_utf8())
            } else {
                self.buffer.len()
            };
            let offset = matcher(&self.buffer[..end], &self.language);
            if offset > 0 {
                let boundary = char_to_byte(&self.buffer, offset);
                result = Some(Aggregation::new(
                    self.buffer[..boundary].trim_matches(' '),
                    AggregationType::Sentence,
                ));
                self.buffer = self.buffer[boundary..].to_owned();
                self.lookahead = LookaheadState::Idle;
            }
        }
        if punctuation {
            self.lookahead = LookaheadState::AwaitingCharacter;
        }
        result
    }
}
impl TextAggregator for SimpleTextAggregator {
    fn aggregate(&mut self, text: &str) -> Vec<Aggregation> {
        if self.mode == AggregationType::Token {
            return if text.is_empty() {
                vec![]
            } else {
                vec![Aggregation::new(text, AggregationType::Token)]
            };
        }
        let mut output = vec![];
        for c in text.chars() {
            self.buffer.push(c);
            if let Some(a) = self.check_sentence(c) {
                output.push(a);
            }
        }
        output
    }
    fn text(&self) -> Aggregation {
        Aggregation::new(self.buffer.trim_matches(' '), AggregationType::Sentence)
    }
    fn flush(&mut self) -> Option<Aggregation> {
        if self.mode == AggregationType::Token || self.buffer.is_empty() {
            return None;
        }
        let text = self.buffer.trim_matches(' ').to_owned();
        self.reset();
        Some(Aggregation::new(text, AggregationType::Sentence))
    }
    fn reset(&mut self) {
        self.buffer.clear();
        self.lookahead = LookaheadState::Idle;
    }
    fn set_language(&mut self, language: Option<&str>) {
        self.language = resolve_sentence_tokenizer_language(language);
    }
    fn language(&self) -> &str {
        &self.language
    }
    fn aggregation_type(&self) -> AggregationType {
        self.mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unbroken_lookahead_requests_only_two_tokenizer_checks() {
        let mut aggregator = SimpleTextAggregator::default();
        let text = format!("I. {} more words without punctuation", "x".repeat(10_000));
        let mut calls = vec![];
        for c in text.chars() {
            aggregator.buffer.push(c);
            assert!(aggregator
                .check_sentence_using(c, |candidate, _| {
                    calls.push(candidate.to_owned());
                    0
                })
                .is_none());
        }
        assert_eq!(
            calls,
            ["I. x".to_owned(), format!("I. {} ", "x".repeat(10_000))]
        );
        assert_eq!(aggregator.flush().unwrap().text, text);
    }
    #[test]
    fn long_lookahead_words_retry_only_when_complete() {
        for length in [64, 65, 10_000] {
            let mut aggregator = SimpleTextAggregator::default();
            let text = format!("I. {} ", "x".repeat(length));
            let mut calls = vec![];
            let mut output = vec![];
            for c in text.chars() {
                aggregator.buffer.push(c);
                if let Some(a) = aggregator.check_sentence_using(c, |candidate, _| {
                    calls.push(candidate.to_owned());
                    if candidate.ends_with(' ') {
                        2
                    } else {
                        0
                    }
                }) {
                    output.push(a.text);
                }
            }
            assert_eq!(calls.len(), 2);
            assert_eq!(output, ["I."]);
            assert_eq!(aggregator.flush().unwrap().text, "x".repeat(length));
        }
    }
    #[test]
    fn non_word_prefix_retries_only_after_following_word() {
        for prefix in ["\"", "“", "—", "👋 "] {
            let mut aggregator = SimpleTextAggregator::default();
            let text = format!("I. {prefix}Douglas ");
            let mut calls = vec![];
            for c in text.chars() {
                aggregator.buffer.push(c);
                aggregator.check_sentence_using(c, |candidate, _| {
                    calls.push(candidate.to_owned());
                    0
                });
            }
            assert_eq!(
                calls,
                [format!("I. {}", prefix.chars().next().unwrap()), text]
            );
        }
    }
}
