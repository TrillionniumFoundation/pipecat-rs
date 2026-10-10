use crate::{FlowError, FlowLoadOptions};
use pipecat_core::{ChatMessage, ChatRole, ContextMessage};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContextStrategy {
    #[default]
    Append,
    Reset,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowMessage {
    pub role: ChatRole,
    pub content: String,
}
impl FlowMessage {
    pub fn new(role: ChatRole, content: impl Into<String>) -> Self {
        Self {
            role,
            content: content.into(),
        }
    }
    pub fn to_context(&self) -> ContextMessage {
        ChatMessage {
            role: self.role,
            content: self.content.clone(),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
        }
        .into()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Branch {
    pub field: String,
    pub cases: BTreeMap<String, String>,
    #[serde(default)]
    pub default: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Transition {
    Node(String),
    Branch(Branch),
}
impl Transition {
    pub fn targets(&self) -> Vec<&str> {
        match self {
            Self::Node(name) => vec![name],
            Self::Branch(branch) => branch
                .cases
                .values()
                .map(String::as_str)
                .chain(branch.default.as_deref())
                .collect(),
        }
    }
    pub fn destination(&self, result: &Value) -> Result<Option<String>, FlowError> {
        match self {
            Self::Node(node) => Ok(Some(node.clone())),
            Self::Branch(branch) => {
                let value = result
                    .as_object()
                    .and_then(|r| r.get(&branch.field))
                    .ok_or_else(|| {
                        FlowError::Tool(format!("branch result has no field '{}'", branch.field))
                    })?;
                let key = case_key(value);
                Ok(branch
                    .cases
                    .iter()
                    .find(|(candidate, _)| canonical_case(candidate) == key)
                    .map(|(_, node)| node.clone())
                    .or_else(|| branch.default.clone()))
            }
        }
    }
}
pub fn case_key(value: &Value) -> String {
    match value {
        Value::String(s) => canonical_case(s),
        Value::Bool(value) => value.to_string(),
        Value::Null => "None".into(),
        _ => python_string(value),
    }
}
fn canonical_case(text: &str) -> String {
    if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("false") {
        text.to_ascii_lowercase()
    } else {
        text.into()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionConfig {
    pub name: String,
    #[serde(default)]
    pub transition_only: bool,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub transition_to: Option<Transition>,
}
impl FunctionConfig {
    pub fn tool(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            transition_only: false,
            description: None,
            transition_to: None,
        }
    }
    pub fn edge(
        name: impl Into<String>,
        description: impl Into<String>,
        node: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            transition_only: true,
            description: Some(description.into()),
            transition_to: Some(Transition::Node(node.into())),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ActionConfig {
    #[serde(rename = "type")]
    pub action_type: String,
    #[serde(default)]
    pub handler: Option<String>,
    #[serde(flatten)]
    pub parameters: Map<String, Value>,
}
impl ActionConfig {
    pub fn new(action_type: impl Into<String>) -> Self {
        Self {
            action_type: action_type.into(),
            handler: None,
            parameters: Map::new(),
        }
    }
    pub fn say(text: impl Into<String>) -> Self {
        let mut action = Self::new("tts_say");
        action.parameters.insert("text".into(), text.into().into());
        action
    }
    pub fn text(&self) -> Option<&str> {
        self.parameters.get("text").and_then(Value::as_str)
    }
    pub fn append_text_to_context(&self) -> Result<bool, FlowError> {
        match self.parameters.get("append_text_to_context") {
            None => Ok(true),
            Some(Value::Bool(value)) => Ok(*value),
            _ => Err(FlowError::Config(
                "append_text_to_context must be a boolean".into(),
            )),
        }
    }
    fn validate(&self) -> Result<(), FlowError> {
        if self.action_type.is_empty() {
            return Err(FlowError::Config("action requires a type".into()));
        }
        if self.action_type == "function" && self.handler.as_deref().is_none_or(str::is_empty) {
            return Err(FlowError::Config(
                "function action requires a handler".into(),
            ));
        }
        if matches!(self.action_type.as_str(), "tts_say" | "end_conversation") {
            if self.handler.is_some() {
                return Err(FlowError::Config(format!(
                    "{} does not take a handler",
                    self.action_type
                )));
            }
            if self.parameters.get("text").is_some_and(|v| !v.is_string()) {
                return Err(FlowError::Config("action text must be a string".into()));
            }
            self.append_text_to_context()?;
        }
        Ok(())
    }
}
fn yes() -> bool {
    true
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    pub task_messages: Vec<FlowMessage>,
    #[serde(default)]
    pub role_message: Option<String>,
    #[serde(default)]
    pub functions: Vec<FunctionConfig>,
    #[serde(default)]
    pub pre_actions: Vec<ActionConfig>,
    #[serde(default)]
    pub post_actions: Vec<ActionConfig>,
    #[serde(default)]
    pub context_strategy: Option<ContextStrategy>,
    #[serde(default = "yes")]
    pub respond_immediately: bool,
}
impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            task_messages: vec![],
            role_message: None,
            functions: vec![],
            pre_actions: vec![],
            post_actions: vec![],
            context_strategy: None,
            respond_immediately: true,
        }
    }
}
impl NodeConfig {
    pub fn new(messages: Vec<FlowMessage>) -> Self {
        Self {
            task_messages: messages,
            ..Self::default()
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowConfig {
    pub initial_node: String,
    pub nodes: BTreeMap<String, NodeConfig>,
    #[serde(default)]
    pub global_functions: Vec<FunctionConfig>,
}
impl FlowConfig {
    pub fn from_json(text: &str) -> Result<Self, FlowError> {
        Self::from_json_with_options(text, &FlowLoadOptions::default())
    }
    pub fn from_json_with_options(
        text: &str,
        options: &FlowLoadOptions,
    ) -> Result<Self, FlowError> {
        crate::loading::from_text(text, true, options)
    }
    /// Load bounded data-only YAML, with filesystem includes disabled.
    pub fn from_yaml(text: &str) -> Result<Self, FlowError> {
        Self::from_yaml_with_options(text, &FlowLoadOptions::default())
    }
    pub fn from_yaml_with_options(
        text: &str,
        options: &FlowLoadOptions,
    ) -> Result<Self, FlowError> {
        crate::loading::from_text(text, false, options)
    }
    /// Read a bounded JSON/YAML file. Includes require explicit options.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, FlowError> {
        Self::from_file_with_options(path, &FlowLoadOptions::default())
    }
    pub fn from_file_with_options(
        path: impl AsRef<Path>,
        options: &FlowLoadOptions,
    ) -> Result<Self, FlowError> {
        crate::loading::from_file(path.as_ref(), options)
    }
    pub fn validate(&self) -> Result<(), FlowError> {
        if self.nodes.is_empty() {
            return Err(FlowError::Config("nodes must not be empty".into()));
        }
        if !self.nodes.contains_key(&self.initial_node) {
            return Err(FlowError::UnknownNode(self.initial_node.clone()));
        }
        let global = self.validate_functions(&self.global_functions)?;
        for (name, node) in &self.nodes {
            if name.is_empty() {
                return Err(FlowError::Config("node name must not be empty".into()));
            }
            let names = self.validate_functions(&node.functions)?;
            if let Some(duplicate) = names.intersection(&global).next() {
                return Err(FlowError::Config(format!(
                    "node '{name}' duplicates global tool '{duplicate}'"
                )));
            }
            for action in node.pre_actions.iter().chain(&node.post_actions) {
                action.validate()?;
            }
        }
        Ok(())
    }
    fn validate_functions<'a>(
        &self,
        functions: &'a [FunctionConfig],
    ) -> Result<HashSet<&'a str>, FlowError> {
        let mut names = HashSet::new();
        for function in functions {
            if function.name.is_empty() || !names.insert(function.name.as_str()) {
                return Err(FlowError::Config(format!(
                    "empty or duplicate function '{}'",
                    function.name
                )));
            }
            if function.transition_only {
                if function.description.as_deref().is_none_or(str::is_empty)
                    || !matches!(function.transition_to, Some(Transition::Node(_)))
                {
                    return Err(FlowError::Config(format!(
                        "transition-only '{}' needs description and a node target",
                        function.name
                    )));
                }
            } else if function.description.is_some() {
                return Err(FlowError::Config(format!(
                    "only transition-only functions take descriptions: '{}'",
                    function.name
                )));
            }
            if let Some(transition) = &function.transition_to {
                if let Transition::Branch(branch) = transition {
                    if branch.field.is_empty() || branch.cases.is_empty() {
                        return Err(FlowError::Config("branch needs field and cases".into()));
                    }
                    let mut cases = HashSet::new();
                    for key in branch.cases.keys() {
                        if !cases.insert(canonical_case(key)) {
                            return Err(FlowError::Config(format!(
                                "ambiguous canonical branch case '{key}'"
                            )));
                        }
                    }
                }
                for target in transition.targets() {
                    if !self.nodes.contains_key(target) {
                        return Err(FlowError::UnknownNode(target.into()));
                    }
                }
            }
        }
        Ok(names)
    }
}

pub fn render_template(text: &str, state: &Map<String, Value>) -> Result<String, FlowError> {
    static PATTERN: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let regex = PATTERN.get_or_init(|| {
        regex::Regex::new(
            r"(\\?)\{\{\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s*\}\}",
        )
        .expect("constant pattern")
    });
    let mut rendered = String::new();
    let mut end = 0;
    for captures in regex.captures_iter(text) {
        let matched = captures.get(0).expect("whole match");
        rendered.push_str(&text[end..matched.start()]);
        if !captures[1].is_empty() {
            rendered.push_str(&matched.as_str()[1..]);
        } else {
            let path = &captures[2];
            let mut parts = path.split('.');
            let mut value = state.get(parts.next().expect("nonempty identifier"));
            for part in parts {
                value = value
                    .and_then(Value::as_object)
                    .and_then(|object| object.get(part));
            }
            let value = value.ok_or_else(|| {
                FlowError::Config(format!("template '{{{{ {path} }}}}' is not in state"))
            })?;
            rendered.push_str(&python_string(value));
        }
        end = matched.end();
    }
    rendered.push_str(&text[end..]);
    Ok(rendered)
}
fn python_string(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        Value::Null => "None".into(),
        Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(python_repr)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(values) => format!(
            "{{{}}}",
            values
                .iter()
                .map(|(k, v)| format!(
                    "{}: {}",
                    python_repr(&Value::String(k.clone())),
                    python_repr(v)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        _ => value.to_string(),
    }
}
fn python_repr(value: &Value) -> String {
    match value {
        Value::String(s) => format!(
            "'{}'",
            s.replace('\\', "\\\\")
                .replace('\'', "\\'")
                .replace('\n', "\\n")
                .replace('\r', "\\r")
                .replace('\t', "\\t")
        ),
        _ => python_string(value),
    }
}
pub(crate) fn render_node(
    mut node: NodeConfig,
    state: &Map<String, Value>,
) -> Result<NodeConfig, FlowError> {
    if let Some(role) = &mut node.role_message {
        *role = render_template(role, state)?;
    }
    for message in &mut node.task_messages {
        message.content = render_template(&message.content, state)?;
    }
    for action in node.pre_actions.iter_mut().chain(&mut node.post_actions) {
        if action.action_type == "tts_say" {
            if let Some(text) = action.text() {
                action
                    .parameters
                    .insert("text".into(), render_template(text, state)?.into());
            }
        }
    }
    Ok(node)
}
