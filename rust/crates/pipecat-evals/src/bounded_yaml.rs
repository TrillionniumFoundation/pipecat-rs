//! Budgeted Value construction over Serde's YAML/JSON parsers.
//!
//! Serde YAML buffers source events before this visitor runs. The visitor bounds
//! the expanded tree, including repeated aliases, before allocating its children.
use crate::EvalError;
use serde::de::{self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor};
use serde_yaml::{
    value::{Tag, TaggedValue},
    Mapping, Value,
};
use std::fmt;

const MAX_NODES: usize = 65_536;
const MAX_DEPTH: usize = 64;
const MAX_SCALAR_BYTES: usize = 4 * 1024 * 1024;

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
impl Default for ConstructionBudget {
    fn default() -> Self {
        Self {
            nodes: 0,
            scalar_bytes: 0,
            max_nodes: MAX_NODES,
            max_depth: MAX_DEPTH,
            max_scalar_bytes: MAX_SCALAR_BYTES,
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
            return Err(self.limit("scenario expanded depth"));
        }
        if self.nodes >= self.max_nodes {
            return Err(self.limit("scenario expanded nodes"));
        }
        self.nodes += 1;
        Ok(())
    }
    fn scalar<E: de::Error>(&mut self, bytes: usize) -> Result<(), E> {
        if bytes > self.max_scalar_bytes.saturating_sub(self.scalar_bytes) {
            return Err(self.limit("scenario expanded scalar bytes"));
        }
        self.scalar_bytes += bytes;
        Ok(())
    }
    fn error(&mut self, error: impl fmt::Display) -> EvalError {
        match self.failure.take() {
            Some(ConstructionFailure::Limit(kind)) => EvalError::Limit(kind),
            Some(ConstructionFailure::Unsupported(detail)) => EvalError::Unsupported(detail),
            None => EvalError::Scenario(error.to_string()),
        }
    }
}
pub(crate) fn parse_yaml(
    bytes: &[u8],
    budget: &mut ConstructionBudget,
    depth: usize,
) -> Result<Value, EvalError> {
    ValueSeed { budget, depth }
        .deserialize(serde_yaml::Deserializer::from_slice(bytes))
        .map_err(|error| budget.error(error))
}
pub(crate) fn parse_json(text: &str, budget: &mut ConstructionBudget) -> Result<Value, EvalError> {
    let mut deserializer = serde_json::Deserializer::from_str(text);
    let value = ValueSeed { budget, depth: 0 }
        .deserialize(&mut deserializer)
        .map_err(|error| budget.error(error))?;
    deserializer.end()?;
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
        while let Some(key) = data.next_key_seed(StringSeed {
            budget: &mut *self.budget,
            depth: self.depth + 1,
        })? {
            let key = Value::String(key);
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
/// Keys and tags cannot allocate a compound subtree; only strings are accepted.
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

#[cfg(test)]
mod tests {
    use super::*;
    fn nodes(value: &Value) -> usize {
        1 + match value {
            Value::Sequence(values) => values.iter().map(nodes).sum(),
            Value::Mapping(values) => values
                .iter()
                .map(|(key, value)| nodes(key) + nodes(value))
                .sum(),
            Value::Tagged(value) => nodes(&value.value),
            _ => 0,
        }
    }
    #[test]
    fn modest_alias_fixture_exceeds_native_node_budget_before_upstream_limit() {
        let source = include_bytes!("../tests/fixtures/alias_nodes.yaml");
        assert!(source.len() < 4096);
        // Safely demonstrate upstream acceptance with only ~66k tiny Values.
        let expanded: Value = serde_yaml::from_slice(source).unwrap();
        assert_eq!(nodes(&expanded), 66_053);
        drop(expanded);
        let mut budget = ConstructionBudget::default();
        assert!(matches!(
            parse_yaml(source, &mut budget, 0),
            Err(EvalError::Limit("scenario expanded nodes"))
        ));
        assert_eq!(budget.nodes, MAX_NODES);
    }
    #[test]
    fn repeated_alias_strings_are_charged_before_tree_string_allocation() {
        let source = format!(
            "anchor: &text {:?}\ncopies: [{}]\n",
            "x".repeat(128),
            vec!["*text"; 20].join(", ")
        );
        let mut budget = ConstructionBudget {
            max_scalar_bytes: 1024,
            ..Default::default()
        };
        assert!(matches!(
            parse_yaml(source.as_bytes(), &mut budget, 0),
            Err(EvalError::Limit("scenario expanded scalar bytes"))
        ));
        assert!(budget.scalar_bytes <= 1024);
        assert!(budget.nodes < 32);
    }
    #[test]
    fn depth_is_rejected_during_construction_and_normal_aliases_remain_supported() {
        let source = format!("{}0{}", "[".repeat(65), "]".repeat(65));
        let mut budget = ConstructionBudget::default();
        assert!(matches!(
            parse_yaml(source.as_bytes(), &mut budget, 0),
            Err(EvalError::Limit("scenario expanded depth"))
        ));
        assert_eq!(budget.nodes, 65);
        let source = b"name: 'quoted *not-an-alias &not-an-anchor !include'\nturns: &turns []\ncopy: *turns\n";
        let value = parse_yaml(source, &mut ConstructionBudget::default(), 0).unwrap();
        assert_eq!(value["turns"], value["copy"]);
        assert!(value["name"].as_str().unwrap().contains("*not-an-alias"));
    }
    #[test]
    fn duplicate_keys_and_compound_keys_rejected_before_their_values() {
        for source in [
            "a: 1\na: [1,2,3]",
            "? [compound, key]\n: value",
            "true: value",
        ] {
            assert!(parse_yaml(source.as_bytes(), &mut ConstructionBudget::default(), 0).is_err());
        }
        let mut budget = ConstructionBudget::default();
        let error = parse_yaml(b"a: 1\na: [1,2,3]", &mut budget, 0).unwrap_err();
        assert!(error.to_string().contains("duplicate"));
        assert_eq!(budget.nodes, 4);
        assert!(parse_json("{\"a\":1,\"a\":2}", &mut ConstructionBudget::default()).is_err());
    }
    #[test]
    fn json_entrypoint_uses_same_construction_limits_and_requires_json() {
        let mut budget = ConstructionBudget {
            max_nodes: 3,
            ..Default::default()
        };
        assert!(matches!(
            parse_json("[1,2,3]", &mut budget),
            Err(EvalError::Limit("scenario expanded nodes"))
        ));
        assert!(parse_json("name: yaml", &mut ConstructionBudget::default()).is_err());
        assert!(parse_json("{} {}", &mut ConstructionBudget::default()).is_err());
    }
}
