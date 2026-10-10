//! NVIDIA's leading inline thought and structured reasoning stream contract.
use crate::{ServiceError, StreamEvent};
use serde_json::Value;

const OPEN: &str = "<think>";
const CLOSE: &str = "</think>";

#[derive(Clone, Default)]
enum TagState {
    #[default]
    Detecting,
    Thought,
    Content,
}

#[derive(Clone, Default)]
pub(crate) struct NvidiaReasoning {
    state: TagState,
    buffer: String,
    structured: bool,
    progressed: bool,
}

impl NvidiaReasoning {
    pub(crate) fn progress(&mut self, events: &mut Vec<StreamEvent>) {
        if !self.progressed {
            self.progressed = true;
            events.push(StreamEvent::ResponseProgress);
        }
    }

    pub(crate) fn delta(
        &mut self,
        delta: &Value,
        content: Option<&str>,
        events: &mut Vec<StreamEvent>,
    ) -> Result<(), ServiceError> {
        let selected = delta
            .get("reasoning_content")
            .filter(|value| truthy(value))
            .or_else(|| delta.get("reasoning").filter(|value| truthy(value)));
        let reasoning = selected
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| ServiceError::Protocol("NVIDIA reasoning must be text".into()))
            })
            .transpose()?;
        if let Some(reasoning) = reasoning {
            if !self.structured {
                self.structured = true;
                events.push(StreamEvent::ThoughtStart);
            }
            events.push(StreamEvent::ThoughtDelta(reasoning.into()));
        } else if self.structured && content.is_some() {
            self.structured = false;
            events.push(StreamEvent::ThoughtEnd);
        }
        if let Some(content) = content {
            self.content(content, events);
        }
        Ok(())
    }

    fn content(&mut self, text: &str, events: &mut Vec<StreamEvent>) {
        if matches!(self.state, TagState::Content) {
            events.push(StreamEvent::TextDelta(text.into()));
            return;
        }
        self.buffer.push_str(text);
        if matches!(self.state, TagState::Detecting) {
            if self.buffer.len() < OPEN.len() && OPEN.starts_with(&self.buffer) {
                return;
            }
            if !self.buffer.starts_with(OPEN) {
                self.state = TagState::Content;
                events.push(StreamEvent::TextDelta(std::mem::take(&mut self.buffer)));
                return;
            }
            self.state = TagState::Thought;
            events.push(StreamEvent::ThoughtStart);
            self.buffer = self.buffer[OPEN.len()..].to_owned();
        }
        if let Some(end) = self.buffer.find(CLOSE) {
            if end > 0 {
                events.push(StreamEvent::ThoughtDelta(self.buffer[..end].into()));
            }
            events.push(StreamEvent::ThoughtEnd);
            let rest = self.buffer[end + CLOSE.len()..].to_owned();
            self.buffer = String::new();
            self.state = TagState::Content;
            if !rest.is_empty() {
                events.push(StreamEvent::TextDelta(rest));
            }
        } else {
            // Python retains seven Unicode scalars, not seven UTF-8 bytes.
            // Copy only the small suffix so retained capacity stays bounded.
            if let Some((split, _)) = self.buffer.char_indices().rev().nth(CLOSE.len() - 2) {
                if split > 0 {
                    events.push(StreamEvent::ThoughtDelta(self.buffer[..split].into()));
                    self.buffer = self.buffer[split..].to_owned();
                }
            }
            if self.buffer.capacity() > 28 {
                self.buffer = self.buffer.to_owned();
            }
        }
    }

    pub(crate) fn finish(&mut self, completed: bool) -> Vec<StreamEvent> {
        let mut events = Vec::new();
        match self.state {
            TagState::Thought => {
                if completed && !self.buffer.is_empty() {
                    events.push(StreamEvent::ThoughtDelta(std::mem::take(&mut self.buffer)));
                }
                events.push(StreamEvent::ThoughtEnd);
            }
            TagState::Detecting if completed && !self.buffer.is_empty() => {
                events.push(StreamEvent::TextDelta(std::mem::take(&mut self.buffer)));
            }
            _ => {}
        }
        self.buffer = String::new();
        self.state = TagState::Content;
        if self.structured {
            events.push(StreamEvent::ThoughtEnd);
            self.structured = false;
        }
        events
    }
}

// JSON equivalents of the source wrapper's Python truthy field selection.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(value) => *value,
        Value::Number(value) => value.as_f64().is_some_and(|number| number != 0.0),
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn decode(parts: &[&str], completed: bool) -> Vec<StreamEvent> {
        let mut state = NvidiaReasoning::default();
        let mut events = Vec::new();
        for part in parts {
            state
                .delta(
                    &json!({}),
                    Some(*part).filter(|s| !s.is_empty()),
                    &mut events,
                )
                .unwrap();
            assert!(state.buffer.chars().count() <= 7);
            assert!(state.buffer.capacity() <= 28);
        }
        events.extend(state.finish(completed));
        assert!(state.finish(completed).is_empty());
        events
    }
    fn projection(events: &[StreamEvent]) -> (String, String, Vec<&'static str>) {
        let (mut visible, mut thought, mut boundaries) = (String::new(), String::new(), Vec::new());
        for event in events {
            match event {
                StreamEvent::TextDelta(text) => visible.push_str(text),
                StreamEvent::ThoughtDelta(text) => thought.push_str(text),
                StreamEvent::ThoughtStart => boundaries.push("start"),
                StreamEvent::ThoughtEnd => boundaries.push("end"),
                _ => panic!("unexpected event"),
            }
        }
        (visible, thought, boundaries)
    }
    #[test]
    fn all_two_and_three_way_unicode_and_delimiter_splits() {
        let text = "<think>甲乙🙂abcdefgh</think>答🙂<think>literal</think>";
        let splits: Vec<_> = text
            .char_indices()
            .map(|(i, _)| i)
            .chain([text.len()])
            .collect();
        for &i in &splits {
            for &j in splits.iter().filter(|&&j| j >= i) {
                let events = decode(&[&text[..i], &text[i..j], &text[j..]], true);
                assert_eq!(
                    projection(&events),
                    (
                        "答🙂<think>literal</think>".into(),
                        "甲乙🙂abcdefgh".into(),
                        vec!["start", "end"]
                    ),
                    "splits {i},{j}"
                );
            }
        }
    }
    #[test]
    fn literal_nonleading_case_and_malformed_prefixes() {
        for text in [
            "answer",
            " <think>secret</think>",
            "\n<think>x",
            "<Think>x</Think>",
            "a<think>x",
            "<thing>x",
            "<thi",
            "<",
            "<think",
        ] {
            let parts: Vec<_> = text
                .char_indices()
                .map(|(i, c)| &text[i..i + c.len_utf8()])
                .collect();
            assert_eq!(
                projection(&decode(&parts, true)),
                (text.into(), String::new(), vec![])
            );
        }
    }
    #[test]
    fn empty_and_unterminated_thoughts_and_cancel_discard() {
        assert_eq!(
            projection(&decode(&["<think></think>"], true)),
            ("".into(), "".into(), vec!["start", "end"])
        );
        assert_eq!(
            projection(&decode(&["<think>甲乙🙂ab"], true)),
            ("".into(), "甲乙🙂ab".into(), vec!["start", "end"])
        );
        assert_eq!(
            projection(&decode(&["<think>甲乙🙂ab"], false)),
            ("".into(), "".into(), vec!["start", "end"])
        );
        assert_eq!(
            projection(&decode(&["<thi"], false)),
            ("".into(), "".into(), vec![])
        );
        assert_eq!(
            projection(&decode(&["<think>12345678</THINK>"], true)),
            ("".into(), "12345678</THINK>".into(), vec!["start", "end"])
        );
    }
    #[test]
    fn large_thought_retains_only_seven_scalars() {
        let short = decode(&["<think>🙂🙂🙂🙂🙂", "🙂🙂"], false);
        assert_eq!(
            projection(&short),
            ("".into(), "".into(), vec!["start", "end"])
        );
        let text = format!("<think>{}", "🙂".repeat(100_000));
        let events = decode(&[&text], false);
        let (_, thought, bounds) = projection(&events);
        assert_eq!(thought.chars().count(), 99_993);
        assert_eq!(bounds, ["start", "end"]);
    }
}
