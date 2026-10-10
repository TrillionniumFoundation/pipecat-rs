use crate::string::{
    char_to_byte, longest_trailing_partial_match, match_endofsentence, parse_start_end_tags,
    StartEndTags,
};
use crate::{Aggregation, AggregationType, SimpleTextAggregator, TextAggregator};

#[derive(Clone, Debug)]
pub struct SkipTagsAggregator {
    inner: SimpleTextAggregator,
    tags: Vec<StartEndTags>,
    current_tag: Option<usize>,
    current_tag_index: usize,
}
impl SkipTagsAggregator {
    pub fn new(tags: Vec<StartEndTags>, mode: AggregationType, language: Option<&str>) -> Self {
        Self {
            inner: SimpleTextAggregator::new(mode, language),
            tags,
            current_tag: None,
            current_tag_index: 0,
        }
    }
}
impl TextAggregator for SkipTagsAggregator {
    fn aggregate(&mut self, text: &str) -> Vec<Aggregation> {
        let mut output = vec![];
        for c in text.chars() {
            self.inner.buffer.push(c);
            (self.current_tag, self.current_tag_index) = parse_start_end_tags(
                &self.inner.buffer,
                &self.tags,
                self.current_tag,
                self.current_tag_index,
            );
            if self.inner.mode != AggregationType::Token && self.current_tag.is_none() {
                let tags = &self.tags;
                if let Some(a) = self.inner.check_sentence_using(c, |text, language| {
                    match_endofsentence(&mask_protected_spans(text, tags), Some(language))
                }) {
                    output.push(a);
                    // Sentence emission removes a prefix; tag scan offsets
                    // must refer to the retained buffer, not the old sentence.
                    (self.current_tag, self.current_tag_index) =
                        parse_start_end_tags(&self.inner.buffer, &self.tags, None, 0);
                }
            }
        }
        if self.inner.mode == AggregationType::Token
            && self.current_tag.is_none()
            && !self.inner.buffer.is_empty()
        {
            let starts: Vec<&str> = self.tags.iter().map(|(s, _)| s.as_str()).collect();
            let held = longest_trailing_partial_match(&self.inner.buffer, &starts);
            let boundary =
                char_to_byte(&self.inner.buffer, self.inner.buffer.chars().count() - held);
            if boundary > 0 {
                output.push(Aggregation::new(
                    &self.inner.buffer[..boundary],
                    AggregationType::Token,
                ));
                self.inner.buffer = self.inner.buffer[boundary..].to_owned();
                self.current_tag_index = 0;
            }
        }
        output
    }
    fn text(&self) -> Aggregation {
        self.inner.text()
    }
    fn flush(&mut self) -> Option<Aggregation> {
        let result = if self.inner.mode == AggregationType::Token {
            (!self.inner.buffer.is_empty())
                .then(|| Aggregation::new(&self.inner.buffer, AggregationType::Token))
        } else {
            self.inner.flush()
        };
        self.reset();
        result
    }
    fn reset(&mut self) {
        self.inner.reset();
        self.current_tag = None;
        self.current_tag_index = 0;
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

/// Mask whole literal spans for the tokenizer, preserving Unicode character
/// offsets. Skipping checks while a tag is open alone is insufficient: a later
/// check would still tokenize punctuation inside a previously closed span.
fn mask_protected_spans(text: &str, tags: &[StartEndTags]) -> String {
    let mut protected = vec![false; text.len()];
    for (start_tag, end_tag) in tags {
        if start_tag.is_empty() || end_tag.is_empty() {
            continue;
        }
        let mut cursor = 0;
        while let Some(offset) = text[cursor..].find(start_tag) {
            let start = cursor + offset;
            let content = start + start_tag.len();
            let end = text[content..]
                .find(end_tag)
                .map_or(text.len(), |offset| content + offset + end_tag.len());
            protected[start..end].fill(true);
            cursor = end;
            if cursor == text.len() {
                break;
            }
        }
    }
    text.char_indices()
        .map(|(index, c)| if protected[index] { 'x' } else { c })
        .collect()
}
