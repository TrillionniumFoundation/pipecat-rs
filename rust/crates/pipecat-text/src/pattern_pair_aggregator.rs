use crate::string::{char_to_byte, longest_trailing_partial_match};
use crate::{Aggregation, AggregationType, SimpleTextAggregator, TextAggregator};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MatchAction {
    #[default]
    Remove,
    Keep,
    Aggregate,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PatternMatch {
    pub text: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub full_match: String,
}
impl PatternMatch {
    fn new(content: impl Into<String>, kind: impl ToString, full_match: impl Into<String>) -> Self {
        Self {
            text: content.into(),
            kind: kind.to_string(),
            full_match: full_match.into(),
        }
    }
    pub fn aggregation(&self) -> Aggregation {
        let mut aggregation = Aggregation::new(&self.text, &self.kind);
        aggregation.full_match = Some(self.full_match.clone());
        aggregation
    }
}
impl fmt::Display for PatternMatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PatternMatch(type={}, text={}, full_match={})",
            self.kind, self.text, self.full_match
        )
    }
}
#[derive(Clone, Debug)]
struct Pattern {
    kind: String,
    start: String,
    end: String,
    action: MatchAction,
}
type Handler = Box<dyn FnMut(&PatternMatch) -> Result<(), String> + Send>;
/// Literal pattern-pair recognition. KEEP preserves delimiters as in the source.
/// Registered handlers are synchronous and ordered; errors are retained in
/// `take_handler_errors` and do not interrupt processing.
pub struct PatternPairAggregator {
    inner: SimpleTextAggregator,
    patterns: Vec<Pattern>,
    handlers: HashMap<String, Handler>,
    last_processed_position: usize,
    matches: Vec<PatternMatch>,
    handler_errors: Vec<String>,
}
impl Default for PatternPairAggregator {
    fn default() -> Self {
        Self::new(AggregationType::Sentence, None)
    }
}
impl PatternPairAggregator {
    pub fn new(mode: AggregationType, language: Option<&str>) -> Self {
        Self {
            inner: SimpleTextAggregator::new(mode, language),
            patterns: vec![],
            handlers: HashMap::new(),
            last_processed_position: 0,
            matches: vec![],
            handler_errors: vec![],
        }
    }
    pub fn add_pattern(
        &mut self,
        kind: &str,
        start: &str,
        end: &str,
        action: MatchAction,
    ) -> Result<&mut Self, String> {
        if ["sentence", "word", "token"].contains(&kind) {
            return Err(format!(
                "The aggregation type '{kind}' is reserved for default behavior"
            ));
        }
        if start.is_empty() || end.is_empty() {
            return Err("Pattern delimiters must be nonempty".into());
        }
        let pattern = Pattern {
            kind: kind.into(),
            start: start.into(),
            end: end.into(),
            action,
        };
        if let Some(existing) = self.patterns.iter_mut().find(|p| p.kind == kind) {
            *existing = pattern;
        } else {
            self.patterns.push(pattern);
        }
        Ok(self)
    }
    pub fn on_pattern_match(
        &mut self,
        kind: impl Into<String>,
        handler: impl FnMut(&PatternMatch) -> Result<(), String> + Send + 'static,
    ) -> &mut Self {
        self.handlers.insert(kind.into(), Box::new(handler));
        self
    }
    /// Drain each newly completed pattern, including REMOVE and KEEP matches.
    pub fn take_matches(&mut self) -> Vec<PatternMatch> {
        std::mem::take(&mut self.matches)
    }
    pub fn take_handler_errors(&mut self) -> Vec<String> {
        std::mem::take(&mut self.handler_errors)
    }
    fn incomplete(&self) -> Option<(usize, usize)> {
        self.patterns.iter().enumerate().find_map(|(i, p)| {
            (self.inner.buffer.matches(&p.start).count()
                > self.inner.buffer.matches(&p.end).count())
            .then(|| (self.inner.buffer.find(&p.start).unwrap(), i))
        })
    }
    fn process_complete(&mut self) -> Vec<PatternMatch> {
        let mut result = vec![];
        let original_len = self.inner.buffer.chars().count();
        for p in &self.patterns {
            let mut scan = 0;
            let mut found = vec![];
            while let Some(start) = self.inner.buffer[scan..].find(&p.start).map(|n| scan + n) {
                let content_start = start + p.start.len();
                let Some(end_start) = self.inner.buffer[content_start..]
                    .find(&p.end)
                    .map(|n| content_start + n)
                else {
                    break;
                };
                let end = end_start + p.end.len();
                let pattern_match = PatternMatch::new(
                    self.inner.buffer[content_start..end_start].trim(),
                    &p.kind,
                    &self.inner.buffer[start..end],
                );
                let already =
                    self.inner.buffer[..end].chars().count() <= self.last_processed_position;
                found.push((pattern_match, already));
                scan = end;
            }
            for (matched, already) in found {
                if !already {
                    if let Some(handler) = self.handlers.get_mut(&p.kind) {
                        if let Err(error) = handler(&matched) {
                            self.handler_errors.push(format!("{}: {error}", p.kind));
                        }
                    }
                    self.matches.push(matched.clone());
                }
                if p.action == MatchAction::Remove {
                    if !already {
                        self.inner.buffer = self.inner.buffer.replacen(&matched.full_match, "", 1);
                    }
                } else {
                    result.push(matched);
                }
            }
        }
        self.last_processed_position = original_len;
        result
    }
    pub fn aggregate_matches(&mut self, text: &str) -> Vec<PatternMatch> {
        let mut output = vec![];
        for c in text.chars() {
            self.inner.buffer.push(c);
            let patterns = self.process_complete();
            if let Some(first) = patterns.first() {
                if self
                    .patterns
                    .iter()
                    .any(|p| p.kind == first.kind && p.action == MatchAction::Aggregate)
                {
                    self.inner.buffer.clear();
                    output.push(first.clone());
                    continue;
                }
            }
            if let Some((start, i)) = self.incomplete() {
                if start == 0 || self.patterns[i].action != MatchAction::Aggregate {
                    continue;
                }
                let prefix = self.inner.buffer[..start].to_owned();
                self.inner.buffer = self.inner.buffer[start..].to_owned();
                output.push(PatternMatch::new(
                    prefix.trim(),
                    self.output_mode(),
                    &prefix,
                ));
                continue;
            }
            if self.inner.mode != AggregationType::Token {
                if let Some(a) = self.inner.check_sentence(c) {
                    output.push(PatternMatch::new(&a.text, &a.kind, &a.text));
                }
            }
        }
        if self.inner.mode == AggregationType::Token
            && !self.inner.buffer.is_empty()
            && self.incomplete().is_none()
        {
            let starts: Vec<&str> = self.patterns.iter().map(|p| p.start.as_str()).collect();
            let held = longest_trailing_partial_match(&self.inner.buffer, &starts);
            let boundary =
                char_to_byte(&self.inner.buffer, self.inner.buffer.chars().count() - held);
            if boundary > 0 {
                let content = self.inner.buffer[..boundary].to_owned();
                self.inner.buffer = self.inner.buffer[boundary..].to_owned();
                self.last_processed_position = self.inner.buffer.chars().count();
                output.push(PatternMatch::new(
                    &content,
                    AggregationType::Token,
                    &content,
                ));
            }
        }
        output
    }
    fn output_mode(&self) -> AggregationType {
        if self.inner.mode == AggregationType::Token {
            AggregationType::Token
        } else {
            AggregationType::Sentence
        }
    }
    pub fn flush_match(&mut self) -> Option<PatternMatch> {
        let mut earliest = self.inner.buffer.len();
        for p in self
            .patterns
            .iter()
            .filter(|p| p.action != MatchAction::Keep)
        {
            let mut scan = 0;
            while let Some(start) = self.inner.buffer[scan..].find(&p.start).map(|n| scan + n) {
                let content_start = start + p.start.len();
                if let Some(end) = self.inner.buffer[content_start..].find(&p.end) {
                    scan = content_start + end + p.end.len();
                } else {
                    earliest = earliest.min(start);
                    break;
                }
            }
        }
        let prefix = &self.inner.buffer[..earliest];
        let starts: Vec<&str> = self
            .patterns
            .iter()
            .filter(|p| p.action != MatchAction::Keep)
            .map(|p| p.start.as_str())
            .collect();
        let held = longest_trailing_partial_match(prefix, &starts);
        let boundary = char_to_byte(prefix, prefix.chars().count() - held);
        let result = &prefix[..boundary];
        let result = (!result.trim().is_empty())
            .then(|| PatternMatch::new(result.trim(), self.output_mode(), result));
        self.reset();
        result
    }
}
impl TextAggregator for PatternPairAggregator {
    fn aggregate(&mut self, text: &str) -> Vec<Aggregation> {
        self.aggregate_matches(text)
            .iter()
            .map(PatternMatch::aggregation)
            .collect()
    }
    fn text(&self) -> Aggregation {
        let kind = self
            .incomplete()
            .map_or("sentence", |(_, i)| self.patterns[i].kind.as_str());
        Aggregation::new(self.inner.buffer.trim(), kind)
    }
    fn flush(&mut self) -> Option<Aggregation> {
        self.flush_match().map(|a| a.aggregation())
    }
    fn reset(&mut self) {
        self.inner.reset();
        self.last_processed_position = 0;
    }
    fn set_language(&mut self, language: Option<&str>) {
        self.inner.set_language(language);
    }
    fn language(&self) -> &str {
        self.inner.language()
    }
    fn aggregation_type(&self) -> AggregationType {
        self.inner.mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn flush_skips_completed_pair_still_in_buffer() {
        let mut a = PatternPairAggregator::default();
        a.add_pattern("test", "<test>", "</test>", MatchAction::Remove)
            .unwrap();
        a.inner.buffer = "Start <test>closed</test> middle <test>unclosed".into();
        assert_eq!(a.flush().unwrap().text, "Start <test>closed</test> middle");
    }
}
