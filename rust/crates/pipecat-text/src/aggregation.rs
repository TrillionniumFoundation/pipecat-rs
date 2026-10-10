use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregationType {
    #[default]
    Sentence,
    Token,
    /// The source SimpleTextAggregator treats WORD like SENTENCE.
    Word,
}
impl fmt::Display for AggregationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Sentence => "sentence",
            Self::Token => "token",
            Self::Word => "word",
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Aggregation {
    pub text: String,
    #[serde(rename = "type")]
    pub kind: String,
    /// Complete source pattern, including delimiters, when produced by a pattern aggregator.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_match: Option<String>,
}
impl Aggregation {
    pub fn new(text: impl Into<String>, kind: impl ToString) -> Self {
        Self {
            text: text.into(),
            kind: kind.to_string(),
            full_match: None,
        }
    }
}
impl fmt::Display for Aggregation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Aggregation by {}: {}", self.kind, self.text)
    }
}
/// Synchronous, allocation-owned equivalent of the source's async iterator.
/// The source aggregators do no asynchronous I/O. Returned vectors preserve
/// the precise emission order within a single input chunk.
pub trait TextAggregator: Send {
    fn aggregate(&mut self, text: &str) -> Vec<Aggregation>;
    fn text(&self) -> Aggregation;
    fn flush(&mut self) -> Option<Aggregation>;
    fn reset(&mut self);
    fn handle_interruption(&mut self) {
        self.reset();
    }
    fn set_language(&mut self, language: Option<&str>);
    fn language(&self) -> &str;
    fn aggregation_type(&self) -> AggregationType;
}
