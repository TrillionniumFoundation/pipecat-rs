use crate::{ActionConfig, FlowConfig, FlowError, FlowHandle, FunctionConfig, NodeConfig};
use pipecat_core::ToolDefinition;
use serde_json::Value;
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc, time::Duration};

/// Configured handlers return Configured or NoResponse. Native handlers can
/// return Stay or Node and choose the transition in Rust instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NextStep {
    Configured,
    Stay,
    NoResponse,
    Node(String),
}
#[derive(Debug, Clone, PartialEq)]
pub struct ToolResult {
    pub result: Value,
    pub next: NextStep,
}
impl ToolResult {
    pub fn configured(result: Value) -> Self {
        Self {
            result,
            next: NextStep::Configured,
        }
    }
    pub fn stay(result: Value) -> Self {
        Self {
            result,
            next: NextStep::Stay,
        }
    }
    pub fn no_response(result: Value) -> Self {
        Self {
            result,
            next: NextStep::NoResponse,
        }
    }
    pub fn transition(result: Value, node: impl Into<String>) -> Self {
        Self {
            result,
            next: NextStep::Node(node.into()),
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct ToolOptions {
    pub cancel_on_interruption: bool,
    /// None disables the timeout; zero is invalid.
    pub timeout: Option<Duration>,
}
type ToolFuture = Pin<Box<dyn Future<Output = Result<ToolResult, FlowError>> + Send>>;
type ActionFuture = Pin<Box<dyn Future<Output = Result<(), FlowError>> + Send>>;
#[derive(Clone)]
pub struct ToolHandler {
    pub schema: ToolDefinition,
    pub options: ToolOptions,
    function: Arc<dyn Fn(Value, FlowHandle) -> ToolFuture + Send + Sync>,
}
impl ToolHandler {
    pub fn new<F, Fut>(schema: ToolDefinition, function: F) -> Self
    where
        F: Fn(Value, FlowHandle) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, FlowError>> + Send + 'static,
    {
        Self {
            schema,
            options: ToolOptions::default(),
            function: Arc::new(move |args, handle| Box::pin(function(args, handle))),
        }
    }
    /// Decode arguments into an application type before invoking its handler.
    pub fn typed<T, F, Fut>(schema: ToolDefinition, function: F) -> Self
    where
        T: serde::de::DeserializeOwned + Send + 'static,
        F: Fn(T, FlowHandle) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ToolResult, FlowError>> + Send + 'static,
    {
        let function = Arc::new(function);
        Self::new(schema, move |args, handle| {
            let function = function.clone();
            async move {
                let typed = serde_json::from_value(args)
                    .map_err(|e| FlowError::Tool(format!("invalid arguments: {e}")))?;
                function(typed, handle).await
            }
        })
    }
    pub fn with_options(mut self, options: ToolOptions) -> Self {
        self.options = options;
        self
    }
    pub(crate) async fn call(
        &self,
        args: Value,
        handle: FlowHandle,
    ) -> Result<ToolResult, FlowError> {
        (self.function)(args, handle).await
    }
}
#[derive(Clone)]
pub struct ActionHandler(Arc<dyn Fn(ActionConfig, FlowHandle) -> ActionFuture + Send + Sync>);
impl ActionHandler {
    pub fn new<F, Fut>(function: F) -> Self
    where
        F: Fn(ActionConfig, FlowHandle) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), FlowError>> + Send + 'static,
    {
        Self(Arc::new(move |action, handle| {
            Box::pin(function(action, handle))
        }))
    }
    pub(crate) async fn call(
        &self,
        action: ActionConfig,
        handle: FlowHandle,
    ) -> Result<(), FlowError> {
        (self.0)(action, handle).await
    }
}
#[derive(Clone, Default)]
pub struct HandlerRegistry {
    pub(crate) tools: BTreeMap<String, ToolHandler>,
    pub(crate) actions: BTreeMap<String, ActionHandler>,
}
impl HandlerRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register_tool(
        &mut self,
        name: impl Into<String>,
        handler: ToolHandler,
    ) -> Result<(), FlowError> {
        let name = name.into();
        if self.tools.contains_key(&name) {
            return Err(FlowError::Config(format!(
                "duplicate tool handler '{name}'"
            )));
        }
        if !handler.schema.parameters.is_object()
            || handler.options.timeout.is_some_and(|d| d.is_zero())
        {
            return Err(FlowError::Config(format!(
                "invalid tool schema or timeout for '{name}'"
            )));
        }
        self.tools.insert(name, handler);
        Ok(())
    }
    pub fn register_action(
        &mut self,
        name: impl Into<String>,
        handler: ActionHandler,
    ) -> Result<(), FlowError> {
        let name = name.into();
        if self.actions.contains_key(&name) {
            return Err(FlowError::Config(format!(
                "duplicate action handler '{name}'"
            )));
        }
        self.actions.insert(name, handler);
        Ok(())
    }
}
#[derive(Clone)]
pub struct Flow {
    pub(crate) config: FlowConfig,
    pub(crate) handlers: HandlerRegistry,
    pub(crate) configured: bool,
}
impl Flow {
    /// Joins a declarative graph to Rust handlers and reports all missing names.
    pub fn new(config: FlowConfig, handlers: HandlerRegistry) -> Result<Self, FlowError> {
        Self::build(config, handlers, true)
    }
    /// Uses Rust ToolResult destinations instead of declarative tool edges.
    pub fn native(config: FlowConfig, handlers: HandlerRegistry) -> Result<Self, FlowError> {
        Self::build(config, handlers, false)
    }
    fn build(
        config: FlowConfig,
        handlers: HandlerRegistry,
        configured: bool,
    ) -> Result<Self, FlowError> {
        config.validate()?;
        let mut problems = vec![];
        for (node_name, node) in &config.nodes {
            for action in node.pre_actions.iter().chain(&node.post_actions) {
                if let Some(name) = &action.handler {
                    if !handlers.actions.contains_key(name) {
                        problems.push(format!(
                            "node '{node_name}' missing action handler '{name}'"
                        ));
                    }
                }
            }
        }
        for function in config
            .global_functions
            .iter()
            .chain(config.nodes.values().flat_map(|n| &n.functions))
        {
            if !function.transition_only && !handlers.tools.contains_key(&function.name) {
                problems.push(format!("missing tool '{}'", function.name));
            }
        }
        if !problems.is_empty() {
            return Err(FlowError::References(problems));
        }
        Ok(Self {
            config,
            handlers,
            configured,
        })
    }
    pub fn config(&self) -> &FlowConfig {
        &self.config
    }
    pub fn initial_node(&self) -> &NodeConfig {
        &self.config.nodes[&self.config.initial_node]
    }
    pub fn node(&self, name: &str) -> Result<&NodeConfig, FlowError> {
        self.config
            .nodes
            .get(name)
            .ok_or_else(|| FlowError::UnknownNode(name.into()))
    }
    pub(crate) fn schema(&self, function: &FunctionConfig) -> ToolDefinition {
        if function.transition_only {
            ToolDefinition {
                name: function.name.clone(),
                description: function.description.clone().unwrap_or_default(),
                parameters: serde_json::json!({"type":"object","properties":{},"required":[]}),
            }
        } else {
            let mut schema = self.handlers.tools[&function.name].schema.clone();
            schema.name = function.name.clone();
            schema
        }
    }
}
