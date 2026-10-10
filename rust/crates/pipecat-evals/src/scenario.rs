//! Source-compatible scripted scenario schema, with explicit capability validation.
use crate::{
    bounded_yaml::{self, ConstructionBudget},
    EvalError,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{collections::HashSet, io::Read, path::Path};
const MAX_FILE_BYTES: usize = 1024 * 1024;
const MAX_DOCUMENT_BYTES: usize = 4 * MAX_FILE_BYTES;
const MAX_BUDGET_MS: u64 = 3_600_000;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct FunctionCall {
    pub name: Option<String>,
    pub args: Option<Map<String, Value>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expectation {
    pub event: String,
    pub within_ms: Option<u64>,
    pub text_contains: Option<String>,
    pub text_excludes: Option<String>,
    pub calls: Option<Vec<FunctionCall>>,
    pub marker: Option<String>,
    pub marker_first: Option<bool>,
    pub markers: Option<usize>,
    pub text_after: Option<bool>,
    #[serde(default)]
    pub absent: bool,
}
impl Expectation {
    pub fn is_call(&self) -> bool {
        matches!(
            self.event.as_str(),
            "function_call" | "function_call_stopped"
        )
    }
    pub fn aggregates(&self) -> bool {
        self.text_contains.is_some()
            && matches!(self.event.as_str(), "llm_response" | "user_transcription")
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SendAfter {
    pub event: Option<String>,
    #[serde(default)]
    pub delay_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptTurn {
    pub user: Option<String>,
    pub dtmf: Option<String>,
    #[serde(default)]
    pub expect: Vec<Expectation>,
    pub send_after: Option<SendAfter>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptScenario {
    pub name: String,
    pub turns: Vec<ScriptTurn>,
    #[serde(default)]
    pub context: Vec<Map<String, Value>>,
    #[serde(default = "yes")]
    pub stop_on_failure: bool,
    #[serde(default)]
    pub trigger_disconnect: bool,
}
fn yes() -> bool {
    true
}
#[derive(Debug, Clone)]
pub struct ScenarioFile {
    pub name: String,
    pub scenarios: Vec<ScriptScenario>,
}
impl ScenarioFile {
    /// Load JSON or YAML, resolving bounded !include tags relative to each file.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        let mut budget = MAX_DOCUMENT_BYTES;
        let mut construction = ConstructionBudget::default();
        let yaml = load_yaml(
            path.as_ref(),
            &mut Vec::new(),
            &mut budget,
            &mut construction,
            0,
        )?;
        Self::from_value(serde_json::to_value(yaml).map_err(|e| invalid(&e.to_string()))?)
    }
    pub fn from_json(text: &str) -> Result<Self, EvalError> {
        if text.len() > MAX_FILE_BYTES {
            return Err(EvalError::Limit("scenario file bytes"));
        }
        let yaml = bounded_yaml::parse_json(text, &mut ConstructionBudget::default())?;
        Self::from_value(serde_json::to_value(yaml).map_err(|e| invalid(&e.to_string()))?)
    }
    pub fn from_value(value: Value) -> Result<Self, EvalError> {
        let mut root = object(value, "scenario file")?;
        let name = nonempty(root.get("name"), "file name")?.to_owned();
        let entries = root.remove("scenarios");
        let mut scenarios = Vec::new();
        if let Some(entries) = entries {
            let entries = entries
                .as_array()
                .ok_or_else(|| invalid("scenarios must be a list"))?;
            if entries.is_empty() || entries.len() > 1024 {
                return Err(invalid("scenarios needs 1..1024 entries"));
            }
            let mut names = HashSet::new();
            for entry in entries {
                let entry = entry
                    .as_object()
                    .ok_or_else(|| invalid("scenario must be a mapping"))?;
                let entry_name = nonempty(entry.get("name"), "scenario name")?;
                if !names.insert(entry_name) {
                    return Err(invalid("duplicate scenario name"));
                }
                if entry.contains_key("scenarios") {
                    return Err(invalid("nested scenarios are not supported"));
                }
                let mut merged = root.clone();
                merged.extend(entry.clone());
                merged.insert("name".into(), format!("{name}/{entry_name}").into());
                scenarios.push(parse_script(merged)?);
            }
        } else {
            scenarios.push(parse_script(root)?);
        }
        Ok(Self { name, scenarios })
    }
}
impl ScriptScenario {
    /// Validate programmatically constructed scenarios using the same limits as files.
    pub fn validate(&self) -> Result<(), EvalError> {
        if self.name.trim().is_empty() {
            return Err(invalid("scenario name must not be empty"));
        }
        if self.turns.len() > 1024 {
            return Err(EvalError::Limit("turn count"));
        }
        let mut count = 0;
        for turn in &self.turns {
            if turn.user.is_some() && turn.dtmf.is_some() {
                return Err(invalid("user and dtmf are mutually exclusive"));
            }
            if let Some(dtmf) = &turn.dtmf {
                if dtmf.is_empty() || !dtmf.chars().all(|c| "0123456789*#".contains(c)) {
                    return Err(invalid("dtmf contains invalid keypad entries"));
                }
            }
            if let Some(after) = &turn.send_after {
                if turn.user.is_none() && turn.dtmf.is_none() {
                    return Err(invalid("send_after requires user or dtmf"));
                }
                if after.event.is_none() && after.delay_ms == 0 {
                    return Err(invalid("send_after needs an event or positive delay"));
                }
                if let Some(event) = &after.event {
                    validate_event(event)?;
                }
                if after.delay_ms > MAX_BUDGET_MS {
                    return Err(EvalError::Limit("send_after delay"));
                }
            }
            count += turn.expect.len();
            for exp in &turn.expect {
                validate_expectation(exp)?;
            }
        }
        if count > 4096 {
            return Err(EvalError::Limit("expectation count"));
        }
        if serde_json::to_vec(self)?.len() > MAX_DOCUMENT_BYTES {
            return Err(EvalError::Limit("scenario bytes"));
        }
        Ok(())
    }
    pub fn observer_config(&self) -> Value {
        let mut config = Map::new();
        let mut full = false;
        let mut named = false;
        let mut vad = false;
        for turn in &self.turns {
            vad |= turn
                .send_after
                .as_ref()
                .and_then(|s| s.event.as_deref())
                .is_some_and(|s| s.starts_with("vad_"));
            for exp in &turn.expect {
                vad |= exp.event.starts_with("vad_");
                if exp.event == "llm_marker" {
                    config.insert("llm_markers".into(), true.into());
                }
                for call in exp.calls.as_deref().unwrap_or_default() {
                    full |= call.args.is_some();
                    named |= call.name.is_some();
                }
            }
        }
        if full || named {
            config.insert(
                "function_call_report_level".into(),
                serde_json::json!({"*":if full {"full"} else {"name"}}),
            );
        }
        if vad {
            config.insert("vad_user_speaking".into(), true.into());
        }
        Value::Object(config)
    }
    pub fn needs_images(&self) -> bool {
        self.turns
            .iter()
            .flat_map(|t| &t.expect)
            .any(|e| e.event == "image")
    }
}
fn parse_script(mut raw: Map<String, Value>) -> Result<ScriptScenario, EvalError> {
    for key in ["persona", "goal", "success", "metrics", "simulation"] {
        if raw.contains_key(key) {
            return Err(EvalError::Unsupported(format!(
                "simulated scenario field {key}"
            )));
        }
    }
    for key in ["user", "judge"] {
        if let Some(block) = raw.remove(key) {
            let mut block = object(block, key)?;
            if let Some(mode) = block.remove("modality") {
                if mode != "text" {
                    return Err(EvalError::Unsupported(format!(
                        "{key}.modality other than text"
                    )));
                }
            }
            if !block.is_empty() {
                return Err(EvalError::Unsupported(format!(
                    "{key} provider configuration: {}",
                    block.keys().cloned().collect::<Vec<_>>().join(", ")
                )));
            }
        }
    }
    let turns = raw
        .get_mut("turns")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| invalid("turns must be a list"))?;
    for turn in turns {
        let turn = turn
            .as_object_mut()
            .ok_or_else(|| invalid("turn must be a mapping"))?;
        for key in ["audio", "image"] {
            if turn.contains_key(key) {
                return Err(EvalError::Unsupported(format!("turn {key} input")));
            }
        }
        if let Some(Value::Number(value)) = turn.get("dtmf") {
            if !value.is_u64() {
                return Err(invalid("numeric dtmf must be a nonnegative integer"));
            }
            turn.insert("dtmf".into(), value.to_string().into());
        }
        if let Some(expect) = turn.get_mut("expect") {
            for exp in expect
                .as_array_mut()
                .ok_or_else(|| invalid("expect must be a list"))?
            {
                let exp = exp
                    .as_object_mut()
                    .ok_or_else(|| invalid("expectation must be a mapping"))?;
                if exp.contains_key("eval") {
                    return Err(EvalError::Unsupported("LLM eval judge assertions".into()));
                }
                if exp.get("event") == Some(&Value::String("response".into())) {
                    exp.insert("event".into(), "llm_response".into());
                }
                let is_call = exp
                    .get("event")
                    .and_then(Value::as_str)
                    .is_some_and(|e| matches!(e, "function_call" | "function_call_stopped"));
                if is_call && exp.get("absent") != Some(&Value::Bool(true)) {
                    if exp.contains_key("calls")
                        && (exp.contains_key("name") || exp.contains_key("args"))
                    {
                        return Err(invalid(
                            "calls and name/args shorthand are mutually exclusive",
                        ));
                    }
                    let mut calls = if let Some(value) = exp.remove("calls") {
                        value
                    } else {
                        let mut call = Map::new();
                        if let Some(v) = exp.remove("name") {
                            call.insert("name".into(), v);
                        }
                        if let Some(v) = exp.remove("args") {
                            call.insert("args".into(), v);
                        }
                        Value::Array(vec![Value::Object(call)])
                    };
                    let list = calls
                        .as_array_mut()
                        .ok_or_else(|| invalid("calls must be a non-empty list"))?;
                    if list.is_empty() {
                        return Err(invalid("calls must be a non-empty list"));
                    }
                    for call in list {
                        if let Value::String(name) = call {
                            *call = serde_json::json!({"name":name});
                        }
                    }
                    exp.insert("calls".into(), calls);
                }
            }
        }
    }
    let scenario: ScriptScenario =
        serde_json::from_value(Value::Object(raw)).map_err(|e| invalid(&e.to_string()))?;
    scenario.validate()?;
    Ok(scenario)
}
fn validate_expectation(exp: &Expectation) -> Result<(), EvalError> {
    validate_event(&exp.event)?;
    if exp.calls.as_ref().is_some_and(|calls| calls.len() > 256) {
        return Err(EvalError::Limit("function calls per expectation"));
    }
    if exp.within_ms.is_some_and(|v| v == 0 || v > MAX_BUDGET_MS) {
        return Err(invalid("within_ms must be 1..3600000"));
    }
    let marker = exp.marker.is_some()
        || exp.marker_first.is_some()
        || exp.markers.is_some()
        || exp.text_after.is_some();
    if marker && exp.event != "llm_marker" {
        return Err(invalid("marker checks require llm_marker"));
    }
    if exp
        .marker
        .as_deref()
        .is_some_and(|m| !matches!(m, "complete" | "short" | "long" | "incomplete"))
    {
        return Err(invalid("invalid marker kind"));
    }
    if exp.absent
        && (marker
            || exp.text_contains.is_some()
            || exp.text_excludes.is_some()
            || exp.calls.is_some())
    {
        return Err(invalid("absent cannot be combined with payload checks"));
    }
    if exp.calls.is_some() && !exp.is_call() {
        return Err(invalid("calls requires a function call event"));
    }
    if exp.is_call() && !exp.absent && exp.calls.as_ref().is_none_or(Vec::is_empty) {
        return Err(invalid(
            "function expectation requires calls (use an empty call object for any call)",
        ));
    }
    if exp.is_call() && (exp.text_contains.is_some() || exp.text_excludes.is_some()) {
        return Err(invalid("function calls use name/args, not text checks"));
    }
    Ok(())
}
fn validate_event(event: &str) -> Result<(), EvalError> {
    if matches!(
        event,
        "user_started_speaking"
            | "user_stopped_speaking"
            | "vad_user_started_speaking"
            | "vad_user_stopped_speaking"
            | "user_transcription"
            | "bot_started_speaking"
            | "bot_stopped_speaking"
            | "bot_interrupted"
            | "llm_started"
            | "llm_response"
            | "llm_marker"
            | "function_call"
            | "function_call_stopped"
            | "image"
    ) {
        Ok(())
    } else {
        Err(EvalError::Unsupported(format!(
            "event {event:?} (native session supports text mode only)"
        )))
    }
}
fn object(value: Value, at: &str) -> Result<Map<String, Value>, EvalError> {
    value
        .as_object()
        .cloned()
        .ok_or_else(|| invalid(&format!("{at} must be a mapping")))
}
fn nonempty<'a>(value: Option<&'a Value>, at: &str) -> Result<&'a str, EvalError> {
    value
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| invalid(&format!("{at} must be a non-empty string")))
}
fn invalid(message: &str) -> EvalError {
    EvalError::Scenario(message.into())
}
fn load_yaml(
    path: &Path,
    stack: &mut Vec<std::path::PathBuf>,
    budget: &mut usize,
    construction: &mut ConstructionBudget,
    depth: usize,
) -> Result<serde_yaml::Value, EvalError> {
    let path = path.canonicalize()?;
    if stack.len() >= 8 || stack.contains(&path) {
        return Err(invalid("include cycle or depth exceeds 8"));
    }
    let mut bytes = Vec::new();
    std::fs::File::open(&path)?
        .take((MAX_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)?;
    if bytes.len() > MAX_FILE_BYTES || bytes.len() > *budget {
        return Err(EvalError::Limit("scenario/include bytes"));
    }
    *budget -= bytes.len();
    let mut value =
        bounded_yaml::parse_yaml(&bytes, construction, depth).map_err(|error| match error {
            EvalError::Scenario(detail) => invalid(&format!("{}: {detail}", path.display())),
            other => other,
        })?;
    stack.push(path.clone());
    resolve_yaml(
        &mut value,
        path.parent().unwrap_or(Path::new(".")),
        stack,
        budget,
        construction,
        depth,
    )?;
    stack.pop();
    Ok(value)
}
fn resolve_yaml(
    value: &mut serde_yaml::Value,
    base: &Path,
    stack: &mut Vec<std::path::PathBuf>,
    budget: &mut usize,
    construction: &mut ConstructionBudget,
    depth: usize,
) -> Result<(), EvalError> {
    match value {
        serde_yaml::Value::Tagged(tag) => {
            if tag.tag != "!include" {
                return Err(EvalError::Unsupported(format!("YAML tag {}", tag.tag)));
            }
            let path = tag
                .value
                .as_str()
                .ok_or_else(|| invalid("!include must name a path"))?;
            if path.len() > 4096 {
                return Err(EvalError::Limit("include path bytes"));
            }
            *value = load_yaml(&base.join(path), stack, budget, construction, depth)?;
        }
        serde_yaml::Value::Sequence(seq) => {
            for value in seq {
                resolve_yaml(value, base, stack, budget, construction, depth + 1)?;
            }
        }
        serde_yaml::Value::Mapping(map) => {
            for (key, value) in map {
                if !key.is_string() {
                    return Err(invalid("mapping keys must be strings"));
                }
                resolve_yaml(value, base, stack, budget, construction, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
