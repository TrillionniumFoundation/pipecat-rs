use crate::{ClassifierError as Error, ClassifierQuestion, Questions, Result};
use serde::{
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    Serialize,
};
use serde_json::Value;
use std::fmt;
use std::io::{self, Write};

#[derive(Debug, Clone)]
pub struct ClassifierLimits {
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    pub max_json_depth: usize,
    pub max_json_nodes: usize,
    pub max_questions: usize,
    pub max_options: usize,
    pub max_name_bytes: usize,
    pub max_in_flight: usize,
}
impl Default for ClassifierLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: 1024 * 1024,
            max_response_bytes: 1024 * 1024,
            max_json_depth: 32,
            max_json_nodes: 32768,
            max_questions: 64,
            max_options: 255,
            max_name_bytes: 256,
            max_in_flight: 16,
        }
    }
}
impl ClassifierLimits {
    pub(crate) fn validate(&self) -> Result<()> {
        for (value, max) in [
            (self.max_request_bytes, 16 * 1024 * 1024),
            (self.max_response_bytes, 16 * 1024 * 1024),
            (self.max_json_depth, 64),
            (self.max_json_nodes, 1_000_000),
            (self.max_questions, 1024),
            (self.max_options, 255),
            (self.max_name_bytes, 4096),
            (self.max_in_flight, 1024),
        ] {
            if value == 0 || value > max {
                return Err(Error::Configuration(
                    "limits must be positive and within hard ceilings",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn request(&self, state: &Value, questions: &Questions) -> Result<()> {
        if questions.is_empty() || questions.len() > self.max_questions {
            return Err(Error::Limit("question count"));
        }
        let mut budget = Budget {
            limits: self,
            nodes: 0,
            bytes: 0,
        };
        structured(state)?;
        budget.value(state)?;
        for (name, question) in questions {
            self.name(name)?;
            budget.text(name)?;
            match question {
                ClassifierQuestion::YesNo(q) => {
                    structured(&q.instructions)?;
                    budget.value(&q.instructions)?;
                    for criteria in [&q.yes, &q.no].into_iter().flatten() {
                        structured(criteria)?;
                        budget.value(criteria)?;
                    }
                }
                ClassifierQuestion::Choice(q) => {
                    structured(&q.instructions)?;
                    budget.value(&q.instructions)?;
                    if q.options.is_empty() || q.options.len() > self.max_options {
                        return Err(Error::Limit(
                            "choice requires 1..=255 options within configured limit",
                        ));
                    }
                    for (key, value) in &q.options {
                        self.name(key)?;
                        budget.text(key)?;
                        if let Some(value) = value {
                            structured(value)?;
                            budget.value(value)?;
                        }
                    }
                }
                ClassifierQuestion::Score(q) => {
                    structured(&q.instructions)?;
                    budget.value(&q.instructions)?;
                    if q.levels.len() < 2 || q.levels.len() > self.max_options {
                        return Err(Error::Limit(
                            "score requires 2..=255 levels within configured limit",
                        ));
                    }
                    for value in &q.levels {
                        structured(value)?;
                        budget.value(value)?;
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn name(&self, name: &str) -> Result<()> {
        if name.is_empty() || name.len() > self.max_name_bytes {
            return Err(Error::Limit("name bytes"));
        }
        Ok(())
    }
    pub(crate) fn request_json(&self, bytes: &[u8]) -> Result<()> {
        let limits = Self {
            max_response_bytes: self.max_request_bytes,
            ..self.clone()
        };
        limits.parse(bytes)?;
        Ok(())
    }
    pub(crate) fn parse(&self, bytes: &[u8]) -> Result<Value> {
        if bytes.len() > self.max_response_bytes {
            return Err(Error::Limit("response bytes"));
        }
        let mut budget = ParseBudget {
            limits: self,
            nodes: 0,
            exceeded: None,
        };
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        let value = NodeSeed {
            budget: &mut budget,
            depth: 1,
        }
        .deserialize(&mut deserializer);
        let value = value.map_err(|_| match budget.exceeded {
            Some(reason) => Error::Limit(reason),
            None => Error::Response("not valid JSON"),
        })?;
        deserializer
            .end()
            .map_err(|_| Error::Response("not valid JSON"))?;
        Ok(value)
    }
}
// Values are charged before serde is allowed to construct them. This retains
// serde_json's full grammar and last-key-wins behavior while bounding temporary
// allocation even for duplicate-key or huge flat input. Map keys are bounded by
// response bytes; every key's value is separately charged before construction.
struct ParseBudget<'a> {
    limits: &'a ClassifierLimits,
    nodes: usize,
    exceeded: Option<&'static str>,
}
struct NodeSeed<'a, 'b> {
    budget: &'a mut ParseBudget<'b>,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for NodeSeed<'_, '_> {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Value, D::Error> {
        let reason = if self.depth > self.budget.limits.max_json_depth {
            Some("JSON depth")
        } else if self.budget.nodes >= self.budget.limits.max_json_nodes {
            Some("JSON nodes")
        } else {
            None
        };
        if let Some(reason) = reason {
            self.budget.exceeded = Some(reason);
            return Err(de::Error::custom(reason));
        }
        self.budget.nodes += 1;
        deserializer.deserialize_any(NodeVisitor {
            budget: self.budget,
            depth: self.depth,
        })
    }
}
struct NodeVisitor<'a, 'b> {
    budget: &'a mut ParseBudget<'b>,
    depth: usize,
}
impl<'de> Visitor<'de> for NodeVisitor<'_, '_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a bounded JSON value")
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Value, E> {
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Value, E> {
        Ok(Value::Number(value.into()))
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Value, E> {
        serde_json::Number::from_f64(value)
            .map(Value::Number)
            .ok_or_else(|| de::Error::custom("nonfinite number"))
    }
    fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Value, E> {
        Ok(Value::String(value.to_owned()))
    }
    fn visit_string<E: de::Error>(self, value: String) -> std::result::Result<Value, E> {
        Ok(Value::String(value))
    }
    fn visit_unit<E: de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_none<E: de::Error>(self) -> std::result::Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> std::result::Result<Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = seq.next_element_seed(NodeSeed {
            budget: self.budget,
            depth: self.depth + 1,
        })? {
            values.push(value);
        }
        Ok(Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> std::result::Result<Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            let value = map.next_value_seed(NodeSeed {
                budget: self.budget,
                depth: self.depth + 1,
            })?;
            values.insert(key, value);
        }
        Ok(Value::Object(values))
    }
}

fn structured(value: &Value) -> Result<()> {
    if matches!(value, Value::String(_) | Value::Array(_) | Value::Object(_)) {
        Ok(())
    } else {
        Err(Error::Question(
            "state, instructions and criteria must be text, object or array",
        ))
    }
}
struct Budget<'a> {
    limits: &'a ClassifierLimits,
    nodes: usize,
    bytes: usize,
}
impl Budget<'_> {
    fn text(&mut self, text: &str) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(text.len())
            .ok_or(Error::Limit("request bytes"))?;
        if self.bytes > self.limits.max_request_bytes {
            return Err(Error::Limit("request bytes"));
        }
        Ok(())
    }
    fn value(&mut self, value: &Value) -> Result<()> {
        self.visit(value, true)
    }
    fn visit(&mut self, value: &Value, input: bool) -> Result<()> {
        let mut stack = vec![(value, 1usize)];
        while let Some((value, depth)) = stack.pop() {
            if depth > self.limits.max_json_depth {
                return Err(Error::Limit("JSON depth"));
            }
            self.nodes += 1;
            if self.nodes > self.limits.max_json_nodes {
                return Err(Error::Limit("JSON nodes"));
            }
            let children = match value {
                Value::Array(v) => v.len(),
                Value::Object(v) => v.len(),
                _ => 0,
            };
            if children
                > self
                    .limits
                    .max_json_nodes
                    .saturating_sub(self.nodes + stack.len())
            {
                return Err(Error::Limit("JSON nodes"));
            }
            match value {
                Value::Array(v) => stack.extend(v.iter().map(|v| (v, depth + 1))),
                Value::Object(v) => {
                    for (key, v) in v {
                        if input {
                            self.text(key)?;
                        }
                        stack.push((v, depth + 1));
                    }
                }
                Value::String(v) if input => self.text(v)?,
                _ => {}
            }
        }
        Ok(())
    }
}
struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(crate) fn encode(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| Error::Limit("request bytes"))?;
    Ok(writer.bytes)
}
pub(crate) fn duration_valid(duration: std::time::Duration) -> bool {
    !duration.is_zero() && duration <= std::time::Duration::from_secs(300)
}

pub(crate) fn encode_pretty(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut writer = LimitedWriter {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer_pretty(&mut writer, value).map_err(|_| Error::Limit("request bytes"))?;
    Ok(writer.bytes)
}
