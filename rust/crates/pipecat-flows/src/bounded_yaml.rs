//! Budgeted Value construction over Serde's YAML/JSON parsers.
//!
//! Serde YAML buffers source events before this visitor runs. The visitor bounds
//! the expanded tree, including repeated aliases, before allocating its children.
use crate::{FlowError, FlowLoadLimits};
use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde_yaml::{
    value::{Tag, TaggedValue},
    Mapping, Value,
};
use std::fmt;

enum ConstructionFailure {
    Limit(&'static str),
    Unsupported(String),
}
/// Shared across every include in one load. Counts include mapping keys, tags,
/// alias expansions, and values later replaced by an include's contents.
pub(crate) struct ConstructionBudget {
    nodes: usize,
    scalar_bytes: usize,
    max_nodes: usize,
    max_depth: usize,
    max_scalar_bytes: usize,
    failure: Option<ConstructionFailure>,
}
impl ConstructionBudget {
    pub(crate) fn new(limits: FlowLoadLimits) -> Self {
        Self {
            nodes: 0,
            scalar_bytes: 0,
            max_nodes: limits.max_expanded_nodes,
            max_depth: limits.max_depth,
            max_scalar_bytes: limits.max_expanded_scalar_bytes,
            failure: None,
        }
    }
}
impl ConstructionBudget {
    fn limit<E: de::Error>(&mut self, kind: &'static str) -> E {
        self.failure = Some(ConstructionFailure::Limit(kind));
        E::custom(kind)
    }
    fn enter<E: de::Error>(&mut self, depth: usize) -> Result<(), E> {
        if depth > self.max_depth {
            return Err(self.limit("flow expanded depth"));
        }
        if self.nodes >= self.max_nodes {
            return Err(self.limit("flow expanded nodes"));
        }
        self.nodes += 1;
        Ok(())
    }
    fn scalar<E: de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        if bytes > self.max_scalar_bytes.saturating_sub(self.scalar_bytes) {
            return Err(self.limit("flow expanded scalar bytes"));
        }
        self.scalar_bytes += bytes;
        Ok(())
    }
    fn error(&mut self, error: impl fmt::Display) -> FlowError {
        match self.failure.take() {
            Some(ConstructionFailure::Limit(kind)) => FlowError::Limit(kind),
            Some(ConstructionFailure::Unsupported(detail)) => FlowError::Unsupported(detail),
            None => FlowError::Config(error.to_string()),
        }
    }
}
pub(crate) fn parse_yaml(
    bytes: &[u8],
    budget: &mut ConstructionBudget,
    depth: usize,
) -> Result<Value, FlowError> {
    check_global_tags(bytes, budget)?;
    ValueSeed { budget, depth }
        .deserialize(serde_yaml::Deserializer::from_slice(bytes))
        .map_err(|error| budget.error(error))
}
pub(crate) fn parse_json(
    bytes: &[u8],
    budget: &mut ConstructionBudget,
    depth: usize,
) -> Result<Value, FlowError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let value = ValueSeed { budget, depth }
        .deserialize(&mut deserializer)
        .map_err(|error| budget.error(error))?;
    deserializer
        .end()
        .map_err(|e| FlowError::Config(e.to_string()))?;
    Ok(value)
}
struct ValueSeed<'a> {
    budget: &'a mut ConstructionBudget,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for ValueSeed<'_> {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Value, D::Error> {
        self.budget.enter(self.depth)?;
        deserializer.deserialize_any(ValueVisitor {
            budget: self.budget,
            depth: self.depth,
        })
    }
}
struct ValueVisitor<'a> {
    budget: &'a mut ConstructionBudget,
    depth: usize,
}
impl<'de> Visitor<'de> for ValueVisitor<'_> {
    type Value = Value;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a bounded YAML/JSON value")
    }
    fn visit_bool<E: de::Error>(self, value: bool) -> Result<Value, E> {
        self.budget.scalar(1)?;
        Ok(Value::Bool(value))
    }
    fn visit_i64<E: de::Error>(self, value: i64) -> Result<Value, E> {
        self.budget.scalar(8)?;
        Ok(Value::Number(value.into()))
    }
    fn visit_u64<E: de::Error>(self, value: u64) -> Result<Value, E> {
        self.budget.scalar(8)?;
        Ok(Value::Number(value.into()))
    }
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<Value, E> {
        self.budget.scalar(8)?;
        if !value.is_finite() {
            return Err(E::custom("non-finite YAML numbers are unsupported"));
        }
        Ok(Value::Number(value.into()))
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<Value, E> {
        self.budget.scalar(value.len())?;
        Ok(Value::String(value.to_owned()))
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<Value, E> {
        self.budget.scalar(value.len())?;
        Ok(Value::String(value))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_none<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut data: A) -> Result<Value, A::Error> {
        // Do not reserve using an untrusted size_hint. Each child's seed charges
        // its node/depth/scalars before construction and before the vector grows.
        let mut values = Vec::new();
        while let Some(value) = data.next_element_seed(ValueSeed {
            budget: &mut *self.budget,
            depth: self.depth + 1,
        })? {
            values.push(value);
        }
        Ok(Value::Sequence(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut data: A) -> Result<Value, A::Error> {
        let mut mapping = Mapping::new();
        while let Some(key) = data.next_key_seed(KeySeed {
            budget: &mut *self.budget,
            depth: self.depth + 1,
        })? {
            // Reject duplicates before decoding (or allocating) their values.
            if mapping.contains_key(&key) {
                return Err(de::Error::custom("duplicate mapping key"));
            }
            let value = data.next_value_seed(ValueSeed {
                budget: &mut *self.budget,
                depth: self.depth + 1,
            })?;
            mapping.insert(key, value);
        }
        Ok(Value::Mapping(mapping))
    }
    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Value, A::Error> {
        let (tag, contents) = data.variant_seed(StringSeed {
            budget: &mut *self.budget,
            depth: self.depth + 1,
        })?;
        if !matches!(tag.as_str(), "include" | "!include") {
            let detail = format!("YAML tag {:?}", tag.chars().take(128).collect::<String>());
            self.budget.failure = Some(ConstructionFailure::Unsupported(detail));
            return Err(de::Error::custom("unsupported YAML tag"));
        }
        let value = contents.newtype_variant_seed(ValueSeed {
            budget: self.budget,
            depth: self.depth + 1,
        })?;
        Ok(Value::Tagged(Box::new(TaggedValue {
            tag: Tag::new(tag),
            value,
        })))
    }
}
/// Tags cannot allocate compound subtrees.
struct StringSeed<'a> {
    budget: &'a mut ConstructionBudget,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for StringSeed<'_> {
    type Value = String;
    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<String, D::Error> {
        self.budget.enter(self.depth)?;
        deserializer.deserialize_any(StringVisitor {
            budget: self.budget,
        })
    }
}
struct StringVisitor<'a> {
    budget: &'a mut ConstructionBudget,
}
impl<'de> Visitor<'de> for StringVisitor<'_> {
    type Value = String;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a string mapping key or tag")
    }
    fn visit_str<E: de::Error>(self, value: &str) -> Result<String, E> {
        self.budget.scalar(value.len())?;
        Ok(value.to_owned())
    }
    fn visit_string<E: de::Error>(self, value: String) -> Result<String, E> {
        self.budget.scalar(value.len())?;
        Ok(value)
    }
}

/// Scalar keys are charged before allocation. Compound/tagged keys are rejected
/// without constructing their contents; only branch cases may later use typed keys.
struct KeySeed<'a> {
    budget: &'a mut ConstructionBudget,
    depth: usize,
}
impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = Value;
    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        self.budget.enter(self.depth)?;
        d.deserialize_any(KeyVisitor {
            budget: self.budget,
        })
    }
}
struct KeyVisitor<'a> {
    budget: &'a mut ConstructionBudget,
}
impl<'de> Visitor<'de> for KeyVisitor<'_> {
    type Value = Value;
    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a scalar mapping key")
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        self.budget.scalar(v.len())?;
        Ok(Value::String(v.into()))
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<Value, E> {
        self.budget.scalar(v.len())?;
        Ok(Value::String(v))
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
        self.budget.scalar(1)?;
        Ok(Value::Bool(v))
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        self.budget.scalar(8)?;
        Ok(Value::Number(v.into()))
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        self.budget.scalar(8)?;
        Ok(Value::Number(v.into()))
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
        self.budget.scalar(8)?;
        if !v.is_finite() {
            return Err(E::custom("non-finite branch keys are unsupported"));
        }
        Ok(Value::Number(v.into()))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Null)
    }
}

/// Serde YAML does not expose global tags to `Visitor::visit_enum`. A
/// maintained YAML token scanner checks original tags without constructing a
/// document or expanding aliases. Source bytes bound scanner lookahead/scalar
/// storage; streamed tokens and nesting have independent hard ceilings.
fn check_global_tags(bytes: &[u8], budget: &ConstructionBudget) -> Result<(), FlowError> {
    use yaml_rust2::scanner::{Scanner, Token, TokenType};
    let text = std::str::from_utf8(bytes).map_err(|e| FlowError::Config(e.to_string()))?;
    // Libyaml also recognizes YAML 1.1 NEL/LS/PS line boundaries. Normalize
    // those only for tokenization so both parsers see the same tag boundaries;
    // the construction parser retains the original scalar text.
    let mut scanner = Scanner::new(text.chars().map(|c| {
        if matches!(c, '\u{85}' | '\u{2028}' | '\u{2029}') {
            '\n'
        } else {
            c
        }
    }));
    let mut tokens = 0usize;
    let mut depth = 0usize;
    while let Some(Token(_, token)) = scanner
        .next_token()
        .map_err(|e| FlowError::Config(e.to_string()))?
    {
        tokens += 1;
        if tokens > 4 * budget.max_nodes + 16 {
            return Err(FlowError::Limit("flow YAML tokens"));
        }
        match token {
            TokenType::TagDirective(..) => {
                return Err(FlowError::Unsupported("YAML tag directives".into()))
            }
            TokenType::Tag(handle, suffix) if handle != "!" || suffix != "include" => {
                return Err(FlowError::Unsupported(
                    "YAML tags other than local !include".into(),
                ));
            }
            TokenType::BlockSequenceStart
            | TokenType::BlockMappingStart
            | TokenType::FlowSequenceStart
            | TokenType::FlowMappingStart => {
                depth += 1;
                if depth > budget.max_depth + 1 {
                    return Err(FlowError::Limit("flow expanded depth"));
                }
            }
            TokenType::BlockEnd | TokenType::FlowSequenceEnd | TokenType::FlowMappingEnd => {
                depth = depth.saturating_sub(1);
            }
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn budget() -> ConstructionBudget {
        ConstructionBudget::new(FlowLoadLimits::default())
    }
    #[test]
    fn construction_limits_precede_values_and_alias_expansion() {
        let source = format!(
            "anchor: &row [{}]\ncopies: [{}]",
            vec!["0"; 256].join(","),
            vec!["*row"; 256].join(",")
        );
        let mut b = budget();
        assert!(matches!(
            parse_yaml(source.as_bytes(), &mut b, 0),
            Err(FlowError::Limit("flow expanded nodes"))
        ));
        assert_eq!(b.nodes, 65_536);
        let source = format!("{}0{}", "[".repeat(65), "]".repeat(65));
        let mut b = budget();
        assert!(matches!(
            parse_yaml(source.as_bytes(), &mut b, 0),
            Err(FlowError::Limit("flow expanded depth"))
        ));
        assert_eq!(b.nodes, 65);
        let source = format!(
            "anchor: &text {:?}\ncopies: [{}]",
            "x".repeat(128),
            vec!["*text"; 20].join(",")
        );
        let mut b = ConstructionBudget::new(FlowLoadLimits {
            max_expanded_scalar_bytes: 1024,
            ..Default::default()
        });
        assert!(matches!(
            parse_yaml(source.as_bytes(), &mut b, 0),
            Err(FlowError::Limit("flow expanded scalar bytes"))
        ));
        assert!(b.scalar_bytes <= 1024);
    }
    #[test]
    fn duplicate_and_compound_keys_stop_before_value_construction() {
        let mut b = budget();
        assert!(parse_yaml(b"a: 1\na: [1,2,3]", &mut b, 0)
            .unwrap_err()
            .to_string()
            .contains("duplicate"));
        assert_eq!(b.nodes, 4);
        let mut b = budget();
        assert!(parse_yaml(b"? [one,two]\n: three", &mut b, 0).is_err());
        assert_eq!(b.nodes, 2);
        assert!(parse_json(b"{\"a\":1,\"a\":2}", &mut budget(), 0).is_err());
    }
    #[test]
    fn all_global_tag_forms_are_rejected_without_interpreting_them() {
        for source in [
            "a: !!python/object:evil {x: 1}",
            "a: !<tag:example.com,2026:thing> {}",
            "%TAG !p! tag:yaml.org,2002:python/\n---\na: !p!object:evil {}",
            "a: &foo !!str text",
            "a: [!!str text]",
            "{a: !!str text}",
            "'a': !!str text",
            "--- !!python/object:evil {}",
            "a:\n  - !!str text",
            "a: |\n  literal\nb: !!str rejected",
            "a:\n  - x: |\n      literal\n    b: !!str rejected",
        ] {
            assert!(
                matches!(
                    parse_yaml(source.as_bytes(), &mut budget(), 0),
                    Err(FlowError::Unsupported(_))
                ),
                "{source}"
            );
        }
    }
    #[test]
    fn global_tag_lookalikes_are_literal_in_prompts_and_comments() {
        for source in [
            "a: '!!python/object:evil %TAG !<tag>'",
            "a: \"!!python/object:evil %TAG !<tag>\"",
            "# %TAG !!python\na: hello # !!python/object:evil",
            "a: |\n  !!python/object:evil\n  %TAG !p! tag:evil\n  !<tag>",
            "a: >-\n  !!python/object:evil\n  %TAG !p! tag:evil\nb: fine",
            "a: don't interpret !!python as a tag",
            "a: 'escaped '' !!python'",
            "a: \"escaped \\\" !!python\"",
            "a: 'multiline\n  !!python literal\n  %TAG literal'",
            "a: some text\n  'plain continuation !!python still text",
            "a: [\"!!python\", '!!python']",
        ] {
            assert!(
                parse_yaml(source.as_bytes(), &mut budget(), 0).is_ok(),
                "{source}"
            );
        }
    }
}
