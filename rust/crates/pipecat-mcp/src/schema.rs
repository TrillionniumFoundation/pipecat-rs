use crate::{McpConfig, McpError};
use pipecat_core::ToolDefinition;
use rmcp::model::Tool;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

pub(crate) fn convert(tool: Tool, config: &McpConfig) -> Result<ToolDefinition, McpError> {
    let name = tool.name.to_string();
    if name.is_empty() || name.len() > 256 || name.chars().any(char::is_control) {
        return Err(McpError::Protocol);
    }
    let mut schema = (*tool.input_schema).clone();
    if schema.get("type").is_some_and(|value| value != "object") {
        return Err(McpError::Protocol);
    }
    schema
        .entry("type")
        .or_insert(Value::String("object".into()));
    if schema
        .get("properties")
        .is_some_and(|value| !value.is_object())
        || schema.get("required").is_some_and(|value| {
            !value
                .as_array()
                .is_some_and(|a| a.iter().all(Value::is_string))
        })
    {
        return Err(McpError::Protocol);
    }
    if let Some(fixed) = config.tools_arguments.get(&name) {
        hide_fixed(&mut schema, fixed);
    }
    Ok(ToolDefinition {
        name,
        description: tool.description.map(|s| s.into_owned()).unwrap_or_default(),
        parameters: Value::Object(schema),
    })
}

fn hide_fixed(schema: &mut Map<String, Value>, fixed: &Map<String, Value>) {
    if let Some(properties) = schema.get_mut("properties").and_then(Value::as_object_mut) {
        properties.retain(|key, _| !fixed.contains_key(key));
    }
    if let Some(required) = schema.get_mut("required").and_then(Value::as_array_mut) {
        required.retain(|value| !value.as_str().is_some_and(|key| fixed.contains_key(key)));
    }
    // These subschemas describe the same object, unlike properties/items/$defs.
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(branches) = schema.get_mut(key).and_then(Value::as_array_mut) {
            for branch in branches.iter_mut().filter_map(Value::as_object_mut) {
                hide_fixed(branch, fixed);
            }
        }
    }
    for key in ["if", "then", "else", "not"] {
        if let Some(branch) = schema.get_mut(key).and_then(Value::as_object_mut) {
            hide_fixed(branch, fixed);
        }
    }
}

pub(crate) fn index(
    schemas: &[ToolDefinition],
) -> Result<BTreeMap<String, ToolDefinition>, McpError> {
    let mut out = BTreeMap::new();
    for schema in schemas {
        if out.insert(schema.name.clone(), schema.clone()).is_some() {
            return Err(McpError::Protocol);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{HttpConfig, ServerConfig};
    use serde_json::json;
    #[test]
    fn preserves_nested_refs_definitions_constraints_and_hides_fixed_root_arguments() {
        let tool: Tool = serde_json::from_value(json!({"name":"search","inputSchema":{"type":"object","properties":{"query":{"$ref":"#/$defs/Query"},"hidden":{"type":"string"}},"required":["query","hidden"],"$defs":{"Query":{"type":"object","properties":{"hidden":{"type":"number"}}}},"allOf":[{"required":["hidden"],"properties":{"hidden":{"type":"string"}}}],"additionalProperties":false}})).unwrap();
        let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(
            "http://localhost/mcp",
        )));
        config.tools_arguments.insert(
            "search".into(),
            json!({"hidden":"not-published"})
                .as_object()
                .unwrap()
                .clone(),
        );
        let result = convert(tool, &config).unwrap();
        assert_eq!(result.parameters["required"], json!(["query"]));
        assert_eq!(result.parameters["allOf"][0]["required"], json!([]));
        assert_eq!(result.parameters["allOf"][0]["properties"], json!({}));
        assert_eq!(
            result.parameters["$defs"]["Query"]["properties"]["hidden"]["type"],
            "number"
        );
        assert_eq!(
            result.parameters["properties"]["query"]["$ref"],
            "#/$defs/Query"
        );
        assert_eq!(result.parameters["additionalProperties"], false);
        assert!(!result.parameters.to_string().contains("not-published"));
    }
    #[test]
    fn rejects_duplicate_invalid_names_nonobject_schema_and_invalid_required() {
        let config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(
            "http://localhost/mcp",
        )));
        for input in [
            json!({"name":"","inputSchema":{}}),
            json!({"name":"bad\nname","inputSchema":{}}),
            json!({"name":"x","inputSchema":{"type":"array"}}),
            json!({"name":"x","inputSchema":{"required":[7]}}),
        ] {
            let tool: Tool = serde_json::from_value(input).unwrap();
            assert_eq!(convert(tool, &config), Err(McpError::Protocol));
        }
        let tool: Tool = serde_json::from_value(json!({"name":"x","inputSchema":{}})).unwrap();
        let converted = convert(tool, &config).unwrap();
        assert_eq!(
            index(&[converted.clone(), converted]),
            Err(McpError::Protocol)
        );
    }
}
