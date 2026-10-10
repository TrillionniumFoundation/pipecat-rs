use serde_json::{json, Value};
pub fn tools() -> Value {
    json!({"tools":[
        {"name":"echo","description":"Echo explicit arguments","inputSchema":{"type":"object","properties":{"value":{"type":"string"},"hidden":{"type":"string"}},"required":["value","hidden"],"additionalProperties":false,"$defs":{"Item":{"type":"string"}},"title":"Echo input"}},
        {"name":"other","inputSchema":{"type":"object","properties":{}}},
        {"name":"failure","inputSchema":{"type":"object"}},
        {"name":"empty","inputSchema":{"type":"object"}},
        {"name":"blocks","inputSchema":{"type":"object"}},
        {"name":"slow","inputSchema":{"type":"object"}},
        {"name":"side_effect","inputSchema":{"type":"object"}},
        {"name":"environment","inputSchema":{"type":"object"}},
        {"name":"oversized","inputSchema":{"type":"object"}}
    ]})
}
pub fn initialize() -> Value {
    json!({"protocolVersion":"2025-11-25","capabilities":{"tools":{"listChanged":true}},"serverInfo":{"name":"owned-native-fixture","version":"1"}})
}
pub fn call(name: &str, args: &Value) -> Value {
    match name {
        "echo" => {
            json!({"content":[{"type":"text","text":args.to_string()}],"structuredContent":{"echo":args,"pid":std::process::id()}})
        }
        "empty" => json!({"content":[]}),
        "blocks" => {
            json!({"content":[{"type":"text","text":"one"},{"type":"image","data":"YQ==","mimeType":"image/png"},{"type":"text","text":"two"}],"structuredContent":{"answer":42},"isError":true})
        }
        "oversized" => json!({"content":[{"type":"text","text":"x".repeat(4096)}]}),
        "environment" => {
            json!({"content":[{"type":"text","text":std::env::var("MCP_FIXTURE_EXPLICIT").unwrap_or_default()}],"structuredContent":{"path_present":std::env::var_os("PATH").is_some()}})
        }
        _ => json!({"content":[{"type":"text","text":"done"}]}),
    }
}
