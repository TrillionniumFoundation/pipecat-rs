//! Admission and owned-memory bounds for the opt-in, final-tool PCM application.
//!
//! Configuration is editable only before it is consumed by the application. The
//! running owner retains validated copies, never a caller-mutable shared context.
//! Callback captures and the caller's trusted output implementation are integration
//! responsibilities; these limits bound the application's data, not arbitrary code.
use crate::{AudioFormat, AudioTransport, FunctionTool, GrokTool, RealtimeConfig, TurnDetection};
use pipecat_core::{ChatMessage, ChatRole};
use pipecat_processors::tools::RegisteredTool;
use serde::{ser::SerializeSeq, Serialize};
use serde_json::Value;
use std::{fmt, io::Write, mem, net::SocketAddr, time::Duration};

const HOSTED_ENDPOINT: &str = "wss://api.x.ai/v1/realtime";
const MAX_JSON_DEPTH: usize = 32;
const MAX_JSON_VALUES: usize = 65_536;
const MAX_JSON_BYTES: usize = 1024 * 1024;
const WIRE_RESERVE: usize = 512;
const HISTORY_PREFIX: &str = "This is a previously saved conversation. Please treat this conversation history as a starting point for the current conversation.\n\n";
const HISTORY_SUFFIX: &str = "\n\nThis is the end of the previously saved conversation. Please continue the conversation from here. If the last message is a user instruction or question, act on that instruction or answer the question. If the last message is an assistant response, simply say that you are ready to continue the conversation.";

/// Bounded, non-payload diagnostics. No provider error, credential, context, or
/// callback error string is copied into an application error.
#[derive(Clone, PartialEq, Eq)]
pub enum GrokAppError {
    Invalid(&'static str),
    Failed(&'static str),
    Closed,
    Timeout(&'static str),
    Cancelled,
}
impl fmt::Display for GrokAppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (prefix, stage) = match self {
            Self::Invalid(stage) => ("invalid Grok application: ", Some(*stage)),
            Self::Failed(stage) => ("Grok application failed: ", Some(*stage)),
            Self::Closed => ("Grok application is closed", None),
            Self::Timeout(stage) => ("Grok application timed out: ", Some(*stage)),
            Self::Cancelled => ("Grok application was cancelled", None),
        };
        f.write_str(prefix)?;
        if let Some(stage) = stage {
            for byte in stage.bytes().take(96) {
                let character = if byte.is_ascii_alphanumeric() || b" _-/".contains(&byte) {
                    char::from(byte)
                } else {
                    '_'
                };
                write!(f, "{character}")?;
            }
        }
        Ok(())
    }
}
impl fmt::Debug for GrokAppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl std::error::Error for GrokAppError {}

/// Finite limits for one immutable application session. Encoded byte bounds and
/// retained-capacity bounds are separate. Runtime input/output/history counters
/// must enforce these same limits, rather than resetting them on a new turn.
#[derive(Clone, Debug)]
pub struct GrokAppLimits {
    pub max_input_bytes: usize,
    pub max_audio_chunk_bytes: usize,
    pub max_input_messages: usize,
    pub max_text_bytes: usize,
    pub max_history_messages: usize,
    pub max_history_bytes: usize,
    /// Aggregate pre-clone backing storage of configuration, seed and schemas.
    pub max_retained_bytes: usize,
    pub max_tools: usize,
    /// Aggregate encoded local schemas, not an independent budget per tool.
    pub max_schema_bytes: usize,
    pub max_schema_depth: usize,
    /// Aggregate schema nodes, including duplicated explicit session schemas.
    pub max_json_values: usize,
    pub max_tool_result_bytes: usize,
    pub max_tool_argument_bytes: usize,
    pub max_output_bytes: usize,
    pub queue_capacity: usize,
    pub output_event_capacity: usize,
    pub max_tool_duration: Duration,
    pub session_timeout: Duration,
    pub drain_timeout: Duration,
    pub shutdown_timeout: Duration,
}
impl Default for GrokAppLimits {
    fn default() -> Self {
        Self {
            max_input_bytes: 8 * 1024 * 1024,
            max_audio_chunk_bytes: 48_000,
            max_input_messages: 256,
            max_text_bytes: 16 * 1024,
            max_history_messages: 256,
            max_history_bytes: 256 * 1024,
            max_retained_bytes: 8 * 1024 * 1024,
            max_tools: 32,
            max_schema_bytes: 64 * 1024,
            max_schema_depth: MAX_JSON_DEPTH,
            max_json_values: 4096,
            max_tool_result_bytes: 64 * 1024,
            max_tool_argument_bytes: 64 * 1024,
            max_output_bytes: 8 * 1024 * 1024,
            queue_capacity: 64,
            output_event_capacity: 64,
            max_tool_duration: Duration::from_secs(30),
            session_timeout: Duration::from_secs(300),
            drain_timeout: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}
impl GrokAppLimits {
    pub fn validate(&self) -> Result<(), GrokAppError> {
        let bounds = [
            (self.max_input_bytes, 256 * 1024 * 1024),
            (self.max_audio_chunk_bytes, 192_000),
            (self.max_input_messages, 4096),
            (self.max_text_bytes, MAX_JSON_BYTES),
            (self.max_history_messages, 1024),
            (self.max_history_bytes, 4 * 1024 * 1024),
            (self.max_retained_bytes, 64 * 1024 * 1024),
            (self.max_tools, 128),
            (self.max_schema_bytes, MAX_JSON_BYTES),
            (self.max_schema_depth, MAX_JSON_DEPTH),
            (self.max_json_values, MAX_JSON_VALUES),
            (self.max_tool_result_bytes, MAX_JSON_BYTES),
            (self.max_tool_argument_bytes, MAX_JSON_BYTES),
            (self.max_output_bytes, 256 * 1024 * 1024),
            (self.queue_capacity, 1024),
            (self.output_event_capacity, 1024),
        ];
        if bounds
            .iter()
            .any(|&(value, maximum)| value == 0 || value > maximum)
            || !self.max_audio_chunk_bytes.is_multiple_of(2)
            || self.max_audio_chunk_bytes > self.max_input_bytes
            || self.max_text_bytes > self.max_input_bytes
            || self.max_text_bytes > self.max_history_bytes
            || self.max_history_bytes > self.max_retained_bytes
            || self.max_schema_bytes > self.max_retained_bytes
        {
            return Err(GrokAppError::Invalid("finite application limits"));
        }
        for (duration, ceiling) in [
            (self.max_tool_duration, 300),
            (self.session_timeout, 3600),
            (self.drain_timeout, 300),
            (self.shutdown_timeout, 60),
        ] {
            if duration.is_zero() || duration > Duration::from_secs(ceiling) {
                return Err(GrokAppError::Invalid("finite application deadlines"));
            }
        }
        if self.max_tool_duration > self.session_timeout
            || self.drain_timeout > self.session_timeout
        {
            return Err(GrokAppError::Invalid("application deadline ordering"));
        }
        Ok(())
    }
}

/// One final-returning native callback, bound to exactly one immutable schema.
/// Progressive reporters and noninterruptible callbacks are not this app profile.
pub struct GrokLocalTool {
    pub schema: FunctionTool,
    pub handler: RegisteredTool,
}

/// Caller-editable preparation data, consumed and sealed by the app constructor.
/// The hosted route always retains the fixed xAI Host/SNI identity while connecting
/// to the single supplied numeric address. No resolver, redirect, or retry is used.
#[derive(Clone, Debug)]
pub struct GrokAppConfig {
    pub connection: RealtimeConfig,
    pub resolved_address: SocketAddr,
    pub limits: GrokAppLimits,
}
impl GrokAppConfig {
    pub fn new(api_key: impl Into<String>, resolved_address: SocketAddr) -> Self {
        let mut connection = RealtimeConfig::new(api_key);
        connection.limits.max_session_responses = 64;
        connection.limits.max_session_tool_calls = 64;
        connection.limits.max_outstanding_tools = 8;
        if let Some(audio) = connection.session.audio.as_mut() {
            if let Some(input) = audio.input.as_mut() {
                input.transport = Some(AudioTransport::Json);
            }
            if let Some(output) = audio.output.as_mut() {
                output.transport = Some(AudioTransport::Json);
            }
        }
        Self {
            connection,
            resolved_address,
            limits: GrokAppLimits::default(),
        }
    }

    /// Explicit offline fixture route. Validation rejects nonloopback addresses,
    /// port zero, scoped IPv6 addresses, and any later endpoint/address mismatch.
    pub fn loopback(api_key: impl Into<String>, address: SocketAddr) -> Self {
        let mut config = Self::new(api_key, address);
        config.connection.endpoint = format!("ws://{address}/v1/realtime");
        config
    }

    /// Validate without networking, history mutation, callback execution, or output
    /// creation. All recursive/encoded/capacity checks precede schema cloning.
    /// Successful validation compacts owned allocations and installs one exact
    /// schema source in the connection. The owner of `tools` must also compact its
    /// outer Vec, whose spare capacity cannot be inspected through a slice.
    pub fn validate_owned(
        &mut self,
        seed: &mut Vec<ChatMessage>,
        tools: &mut [GrokLocalTool],
    ) -> Result<(), GrokAppError> {
        let result = self.validate_owned_inner(seed, tools);
        if result.is_err() {
            // Constructors own these buffers. Even a failure before graph inspection
            // must not leave an arbitrarily deep Value to Rust's recursive Drop.
            discard_invalid_inputs(&mut self.connection.session, seed, tools);
        }
        result
    }

    fn validate_owned_inner(
        &mut self,
        seed: &mut Vec<ChatMessage>,
        tools: &mut [GrokLocalTool],
    ) -> Result<(), GrokAppError> {
        self.limits.validate()?;
        self.validate_connection_profile()?;
        let limits = &self.limits;
        let mut retained = Budget::new(limits.max_retained_bytes);
        retained.add(self.connection.app_credential_capacity())?;
        let mut json = JsonStats::new(limits.max_json_values, limits.max_schema_bytes);
        self.inspect_configuration(&mut retained)?;
        retained.add_slots::<ChatMessage>(seed.capacity())?;
        if seed.len() > limits.max_history_messages || seed.len() > limits.max_input_messages {
            return Err(GrokAppError::Invalid("seed message count"));
        }
        for message in seed.iter() {
            if message.role == ChatRole::Tool
                || message.name.is_some()
                || message.tool_call_id.is_some()
                || !message.tool_calls.is_empty()
            {
                return Err(GrokAppError::Invalid("ordinary plain text seed required"));
            }
            inspect_string(&message.content, limits.max_text_bytes, &mut retained)?;
            retained.add_slots::<pipecat_core::FunctionCall>(message.tool_calls.capacity())?;
        }
        let seed_bytes = encoded_len(seed, limits.max_history_bytes)?;
        validate_seed_wire(seed, self.connection.limits.max_message_bytes)?;
        if tools.len() > limits.max_tools {
            return Err(GrokAppError::Invalid("local tool count"));
        }
        retained.add_slots::<GrokLocalTool>(tools.len())?;
        let mut schema_bytes = 0usize;
        let before_local_schemas = retained.used;
        for (index, tool) in tools.iter().enumerate() {
            retained.add(tool.schema.app_kind_capacity())?;
            inspect_schema(
                &tool.schema.name,
                &tool.schema.description,
                &tool.schema.parameters,
                limits,
                &mut retained,
                &mut json,
            )?;
            if tool.handler.timeout.is_zero()
                || tool.handler.timeout > limits.max_tool_duration
                || !tool.handler.cancel_on_interruption
            {
                return Err(GrokAppError::Invalid(
                    "interruptible bounded final tool required",
                ));
            }
            if tools[..index]
                .iter()
                .any(|other| other.schema.name == tool.schema.name)
            {
                return Err(GrokAppError::Invalid("duplicate local tool name"));
            }
            checked_accumulate(
                &mut schema_bytes,
                encoded_len(&tool.schema, limits.max_schema_bytes)?,
                limits.max_schema_bytes,
                "aggregate tool schema bytes",
            )?;
        }
        let local_schema_storage = retained.used - before_local_schemas;
        if let Some(session_tools) = &self.connection.session.tools {
            retained.add_slots::<GrokTool>(session_tools.capacity())?;
            if session_tools.len() != tools.len() {
                return Err(GrokAppError::Invalid("session and local tool set differ"));
            }
            for (index, session_tool) in session_tools.iter().enumerate() {
                let GrokTool::Function {
                    name,
                    description,
                    parameters,
                } = session_tool
                else {
                    return Err(GrokAppError::Invalid("hosted tools are not supported"));
                };
                inspect_schema(
                    name,
                    description,
                    parameters,
                    limits,
                    &mut retained,
                    &mut json,
                )?;
                if session_tools[..index].iter().any(|other| {
                    matches!(other, GrokTool::Function { name: other_name, .. } if other_name == name)
                }) {
                    return Err(GrokAppError::Invalid("duplicate session tool name"));
                }
                let local = tools
                    .iter()
                    .find(|tool| &tool.schema.name == name)
                    .ok_or(GrokAppError::Invalid("session tool has no local binding"))?;
                if &local.schema.description != description
                    || &local.schema.parameters != parameters
                {
                    return Err(GrokAppError::Invalid("conflicting session tool schema"));
                }
            }
        }
        // Bound both context conversion and session installation, including wire
        // framing, before constructing either context tools or a session-tool copy.
        let mut context_bytes = seed_bytes;
        checked_accumulate(
            &mut context_bytes,
            schema_bytes,
            self.connection.limits.max_message_bytes,
            "seed context and schemas",
        )?;
        checked_accumulate(
            &mut context_bytes,
            WIRE_RESERVE,
            self.connection.limits.max_message_bytes,
            "seed context framing",
        )?;
        let mut session_bytes = encoded_len(
            &self.connection.session,
            self.connection.limits.max_message_bytes,
        )?;
        if self.connection.session.tools.is_none() {
            checked_accumulate(
                &mut session_bytes,
                schema_bytes,
                self.connection.limits.max_message_bytes,
                "initial session schemas",
            )?;
        }
        checked_accumulate(
            &mut session_bytes,
            WIRE_RESERVE,
            self.connection.limits.max_message_bytes,
            "initial session framing",
        )?;
        // Reserve an additional compact schema representation before cloning it.
        // The compact live structure cannot exceed the inspected source storage.
        retained.add(local_schema_storage)?;
        retained.add_slots::<GrokTool>(tools.len())?;
        if !tools.is_empty() {
            // One temporary serialized FunctionTool adds a small outer object
            // beyond its already-budgeted parameter graph.
            retained.add(4096)?;
        }

        self.connection
            .compact_app_credential(limits.max_retained_bytes)
            .map_err(|_| GrokAppError::Invalid("application credential"))?;
        self.connection
            .validate()
            .map_err(|_| GrokAppError::Invalid("realtime application configuration"))?;
        compact_string(&mut self.connection.endpoint);
        compact_string(&mut self.connection.model);
        compact_optional_string(&mut self.connection.context_id);
        compact_session(&mut self.connection.session);
        for message in seed.iter_mut() {
            compact_string(&mut message.content);
            message.tool_calls = Vec::new();
        }
        *seed = mem::take(seed).into_boxed_slice().into_vec();
        for tool in tools.iter_mut() {
            compact_string(&mut tool.schema.name);
            compact_string(&mut tool.schema.description);
            compact_json_unchecked(&mut tool.schema.parameters);
            // FunctionTool's serialized discriminator is private and deserializable.
            // Inspect only after the entire graph was bounded and compacted. This
            // transient representation has an already-proven encoded/recursive bound.
            let encoded = serde_json::to_value(&tool.schema)
                .map_err(|_| GrokAppError::Invalid("function tool discriminator"))?;
            if encoded.get("type").and_then(Value::as_str) != Some("function") {
                return Err(GrokAppError::Invalid("function tool discriminator"));
            }
            tool.schema = FunctionTool::new(
                mem::take(&mut tool.schema.name),
                mem::take(&mut tool.schema.description),
                mem::take(&mut tool.schema.parameters),
            );
        }
        self.connection.session.tools = Some(
            tools
                .iter()
                .map(|tool| GrokTool::Function {
                    name: tool.schema.name.clone(),
                    description: tool.schema.description.clone(),
                    parameters: tool.schema.parameters.clone(),
                })
                .collect(),
        );
        self.connection
            .validate()
            .map_err(|_| GrokAppError::Invalid("realtime application configuration"))?;
        Ok(())
    }

    fn validate_connection_profile(&self) -> Result<(), GrokAppError> {
        let connection = &self.connection;
        if connection.endpoint.len() > 256 || connection.model.len() > 256 {
            return Err(GrokAppError::Invalid("connection identity size"));
        }
        let address = self.resolved_address;
        if address.port() == 0
            || address.ip().is_unspecified()
            || address.ip().is_multicast()
            || matches!(address, SocketAddr::V6(v6) if v6.scope_id() != 0 || v6.flowinfo() != 0)
        {
            return Err(GrokAppError::Invalid("numeric connection address"));
        }
        if connection.endpoint == HOSTED_ENDPOINT {
            if address.port() != 443
                || address.ip().is_loopback()
                || matches!(address.ip(), std::net::IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() || ip.is_broadcast()))
                || matches!(address.ip(), std::net::IpAddr::V4(ip) if ip.is_broadcast())
            {
                return Err(GrokAppError::Invalid(
                    "hosted numeric address on port 443 required",
                ));
            }
        } else if !address.ip().is_loopback()
            || connection.endpoint != format!("ws://{address}/v1/realtime")
        {
            return Err(GrokAppError::Invalid(
                "fixed hosted or matching literal loopback route required",
            ));
        }
        let wire = &connection.limits;
        if wire.queue_capacity == 0
            || wire.queue_capacity > 1024
            || !(1024..=4 * 1024 * 1024).contains(&wire.max_message_bytes)
            || wire.max_audio_chunk_bytes == 0
            || wire.max_audio_chunk_bytes > 192_000
            || wire.max_audio_chunk_bytes > wire.max_message_bytes / 2
            || wire.max_outstanding_tools == 0
            || wire.max_outstanding_tools > 128
            || wire.max_session_responses == 0
            || wire.max_session_responses > 4096
            || wire.max_session_tool_calls < wire.max_outstanding_tools
            || wire.max_session_tool_calls > 4096
            || self.limits.max_audio_chunk_bytes > wire.max_audio_chunk_bytes
            || self
                .limits
                .max_text_bytes
                .saturating_mul(6)
                .saturating_add(WIRE_RESERVE)
                > wire.max_message_bytes
            || self
                .limits
                .max_tool_result_bytes
                .saturating_mul(2)
                .saturating_add(WIRE_RESERVE)
                > wire.max_message_bytes
            || self
                .limits
                .max_tool_argument_bytes
                .saturating_add(WIRE_RESERVE)
                > wire.max_message_bytes
        {
            return Err(GrokAppError::Invalid("finite realtime application limits"));
        }
        for duration in [
            wire.connect_timeout,
            wire.operation_timeout,
            wire.response_timeout,
            wire.close_timeout,
        ] {
            if duration.is_zero() || duration > self.limits.session_timeout {
                return Err(GrokAppError::Invalid("realtime application deadlines"));
            }
        }
        if wire.close_timeout > self.limits.shutdown_timeout {
            return Err(GrokAppError::Invalid("close exceeds shutdown deadline"));
        }
        let session = &connection.session;
        if session.id.is_some()
            || session.resumption.is_some()
            || session.replace.is_some()
            || session.reasoning.is_some()
        {
            return Err(GrokAppError::Invalid(
                "unsupported application session mode",
            ));
        }
        let Some(Some(vad)) = &session.turn_detection else {
            return Err(GrokAppError::Invalid("server VAD required"));
        };
        if !vad.valid_kind()
            || vad
                .threshold
                .is_some_and(|value| !value.is_finite() || !(0.1..=0.9).contains(&value))
            || vad.silence_duration_ms.is_some_and(|value| value > 60_000)
            || vad.prefix_padding_ms.is_some_and(|value| value > 10_000)
            || vad.idle_timeout_ms.is_some_and(|value| value > 300_000)
        {
            return Err(GrokAppError::Invalid("bounded server VAD required"));
        }
        let Some(audio) = &session.audio else {
            return Err(GrokAppError::Invalid("explicit mono PCM24 audio required"));
        };
        let (Some(input), Some(output)) = (&audio.input, &audio.output) else {
            return Err(GrokAppError::Invalid(
                "explicit input and output audio required",
            ));
        };
        if input.format != (AudioFormat::Pcm { rate: 24_000 })
            || output.format != (AudioFormat::Pcm { rate: 24_000 })
            || input.transport == Some(AudioTransport::Binary)
            || output.transport == Some(AudioTransport::Binary)
            || input.transcription.is_some()
            || output
                .speed
                .is_some_and(|speed| !speed.is_finite() || !(0.7..=1.5).contains(&speed))
        {
            return Err(GrokAppError::Invalid(
                "mono PCM24 JSON audio profile required",
            ));
        }
        Ok(())
    }

    fn inspect_configuration(&self, retained: &mut Budget) -> Result<(), GrokAppError> {
        if let Some(Some(vad)) = &self.connection.session.turn_detection {
            retained.add(vad.app_kind_capacity())?;
        }
        inspect_string(&self.connection.endpoint, 256, retained)?;
        inspect_string(&self.connection.model, 256, retained)?;
        for value in [
            &self.connection.context_id,
            &self.connection.session.model,
            &self.connection.session.voice,
        ]
        .into_iter()
        .flatten()
        {
            inspect_string(value, 256, retained)?;
        }
        if let Some(instructions) = &self.connection.session.instructions {
            inspect_string(instructions, self.limits.max_history_bytes, retained)?;
        }
        Ok(())
    }
}

fn checked_accumulate(
    total: &mut usize,
    amount: usize,
    maximum: usize,
    stage: &'static str,
) -> Result<(), GrokAppError> {
    *total = total
        .checked_add(amount)
        .filter(|total| *total <= maximum)
        .ok_or(GrokAppError::Invalid(stage))?;
    Ok(())
}
struct Budget {
    used: usize,
    maximum: usize,
}
impl Budget {
    fn new(maximum: usize) -> Self {
        Self { used: 0, maximum }
    }
    fn add(&mut self, amount: usize) -> Result<(), GrokAppError> {
        checked_accumulate(
            &mut self.used,
            amount,
            self.maximum,
            "retained allocation bound",
        )
    }
    fn add_slots<T>(&mut self, count: usize) -> Result<(), GrokAppError> {
        self.add(
            count
                .checked_mul(mem::size_of::<T>())
                .ok_or(GrokAppError::Invalid("retained allocation overflow"))?,
        )
    }
}
impl Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.add(bytes.len())
            .map_err(|_| std::io::Error::other("bounded Grok application data"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encoded_len(value: &impl Serialize, maximum: usize) -> Result<usize, GrokAppError> {
    let mut budget = Budget::new(maximum);
    serde_json::to_writer(&mut budget, value)
        .map_err(|_| GrokAppError::Invalid("encoded application data bound"))?;
    Ok(budget.used)
}
fn inspect_string(
    value: &String,
    maximum: usize,
    retained: &mut Budget,
) -> Result<(), GrokAppError> {
    if value.len() > maximum {
        return Err(GrokAppError::Invalid("application string byte bound"));
    }
    retained.add(value.capacity())
}
struct JsonStats {
    values: usize,
    maximum_values: usize,
    string_bytes: usize,
    maximum_bytes: usize,
}
impl JsonStats {
    fn new(maximum_values: usize, maximum_bytes: usize) -> Self {
        Self {
            values: 0,
            maximum_values,
            string_bytes: 0,
            maximum_bytes,
        }
    }
    fn string(&mut self, text: &String, retained: &mut Budget) -> Result<(), GrokAppError> {
        checked_accumulate(
            &mut self.string_bytes,
            text.len(),
            self.maximum_bytes,
            "aggregate JSON string bytes",
        )?;
        retained.add(text.capacity())
    }
    fn inspect(
        &mut self,
        value: &Value,
        depth: usize,
        maximum_depth: usize,
        retained: &mut Budget,
    ) -> Result<(), GrokAppError> {
        if depth > maximum_depth {
            return Err(GrokAppError::Invalid("JSON nesting depth"));
        }
        checked_accumulate(&mut self.values, 1, self.maximum_values, "JSON value count")?;
        match value {
            Value::String(text) => self.string(text, retained)?,
            Value::Array(values) => {
                if values.len() > self.maximum_values.saturating_sub(self.values) {
                    return Err(GrokAppError::Invalid("JSON value count"));
                }
                retained.add_slots::<Value>(values.capacity())?;
                for child in values {
                    self.inspect(child, depth + 1, maximum_depth, retained)?;
                }
            }
            Value::Object(values) => {
                if values.len() > self.maximum_values.saturating_sub(self.values) {
                    return Err(GrokAppError::Invalid("JSON value count"));
                }
                // serde_json does not expose object backing capacity. Conservatively
                // account node/key/value storage, then rebuild the map before cloning.
                retained.add(1024)?;
                retained.add_slots::<[u8; 256]>(values.len())?;
                for (key, child) in values {
                    self.string(key, retained)?;
                    self.inspect(child, depth + 1, maximum_depth, retained)?;
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) => {}
        }
        Ok(())
    }
}
fn inspect_schema(
    name: &String,
    description: &String,
    parameters: &Value,
    limits: &GrokAppLimits,
    retained: &mut Budget,
    json: &mut JsonStats,
) -> Result<(), GrokAppError> {
    if name.is_empty() || name.chars().any(char::is_control) || !parameters.is_object() {
        return Err(GrokAppError::Invalid("exact function schema required"));
    }
    inspect_string(name, 256, retained)?;
    inspect_string(description, limits.max_schema_bytes, retained)?;
    json.inspect(parameters, 0, limits.max_schema_depth, retained)
}
fn compact_string(value: &mut String) {
    *value = mem::take(value).into_boxed_str().into_string();
}
fn compact_optional_string(value: &mut Option<String>) {
    if let Some(value) = value {
        compact_string(value);
    }
}
fn compact_session(session: &mut crate::SessionProperties) {
    compact_optional_string(&mut session.model);
    compact_optional_string(&mut session.instructions);
    compact_optional_string(&mut session.voice);
    if let Some(Some(vad)) = &mut session.turn_detection {
        let mut compact = TurnDetection::server_vad();
        compact.threshold = vad.threshold;
        compact.silence_duration_ms = vad.silence_duration_ms;
        compact.prefix_padding_ms = vad.prefix_padding_ms;
        compact.idle_timeout_ms = vad.idle_timeout_ms;
        *vad = compact;
    }
    if let Some(audio) = &mut session.audio {
        if let Some(input) = &mut audio.input {
            input.transport = Some(AudioTransport::Json);
        }
        if let Some(output) = &mut audio.output {
            output.transport = Some(AudioTransport::Json);
        }
    }
    // Explicit supplied tools have already been compared; the canonical local
    // source replaces them without cloning their spare storage.
    session.tools = None;
}

/// Validate and compact a callback's owned result BEFORE any native publisher
/// clones it. Recursive inspection is capped at 32 and serialization cannot grow
/// an allocation. Large spare capacities fail closed, modest spare storage shrinks.
pub(crate) fn compact_json(value: &mut Value, max_bytes: usize) -> Result<(), GrokAppError> {
    let result = (|| {
        if max_bytes == 0 || max_bytes > MAX_JSON_BYTES {
            return Err(GrokAppError::Invalid("JSON byte limit"));
        }
        let retained_maximum = max_bytes
            .checked_mul(16)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or(GrokAppError::Invalid("JSON retained limit"))?;
        let mut retained = Budget::new(retained_maximum);
        let mut json = JsonStats::new(max_bytes.min(MAX_JSON_VALUES), max_bytes);
        json.inspect(value, 0, MAX_JSON_DEPTH, &mut retained)?;
        encoded_len(value, max_bytes)?;
        compact_json_unchecked(value);
        Ok(())
    })();
    if result.is_err() {
        discard_json(value);
    }
    result
}

/// Consume rejected JSON without recursive destruction. The failure-only worklist
/// tracks containers from the already-owned rejected graph, not copies of payloads.
/// Its storage is proportional to that rejected graph, not a successful admission
/// budget; arbitrary preconstructed caller allocation is not made retroactively bounded.
pub(crate) fn discard_json(value: &mut Value) {
    let mut pending = vec![mem::take(value)];
    while let Some(value) = pending.pop() {
        match value {
            Value::Array(values) => {
                for child in values {
                    if matches!(child, Value::Array(_) | Value::Object(_)) {
                        pending.push(child);
                    }
                }
            }
            Value::Object(values) => {
                for (_, child) in values {
                    if matches!(child, Value::Array(_) | Value::Object(_)) {
                        pending.push(child);
                    }
                }
            }
            Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
        }
    }
}

/// Also usable by the owning constructor if it rejects an outer Vec capacity
/// before calling validate_owned. Rejected parameter/call graphs become Null.
pub(crate) fn discard_invalid_inputs(
    session: &mut crate::SessionProperties,
    seed: &mut [ChatMessage],
    tools: &mut [GrokLocalTool],
) {
    for tool in tools {
        discard_json(&mut tool.schema.parameters);
    }
    for message in seed {
        for call in &mut message.tool_calls {
            discard_json(&mut call.arguments);
        }
    }
    if let Some(tools) = &mut session.tools {
        for tool in tools {
            match tool {
                GrokTool::Function { parameters, .. } => discard_json(parameters),
                GrokTool::Mcp { properties } => {
                    for (_, mut value) in mem::take(properties) {
                        discard_json(&mut value);
                    }
                }
                GrokTool::WebSearch | GrokTool::XSearch { .. } | GrokTool::FileSearch { .. } => {}
            }
        }
    }
}
fn compact_json_unchecked(value: &mut Value) {
    match value {
        Value::String(text) => compact_string(text),
        Value::Array(values) => {
            for child in values.iter_mut() {
                compact_json_unchecked(child);
            }
            *values = mem::take(values).into_boxed_slice().into_vec();
        }
        Value::Object(values) => {
            let old = mem::take(values);
            let mut compact = serde_json::Map::with_capacity(old.len());
            for (mut key, mut child) in old {
                compact_string(&mut key);
                compact_json_unchecked(&mut child);
                compact.insert(key, child);
            }
            *values = compact;
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

// Match the real provider's leading-system extraction and subsequent role
// normalization while measuring the nested JSON string without constructing it.
struct SeedMessages<'a>(&'a [ChatMessage]);
impl Serialize for SeedMessages<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Message<'a> {
            role: ChatRole,
            content: &'a str,
            name: Option<&'a str>,
            tool_call_id: Option<&'a str>,
            tool_calls: &'a [pipecat_core::FunctionCall],
        }
        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for message in self.0 {
            sequence.serialize_element(&Message {
                role: match message.role {
                    ChatRole::System | ChatRole::Developer => ChatRole::User,
                    role => role,
                },
                content: &message.content,
                name: None,
                tool_call_id: None,
                tool_calls: &[],
            })?;
        }
        sequence.end()
    }
}
struct EscapedBudget(Budget);
impl Write for EscapedBudget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        for &byte in bytes {
            let width = match byte {
                b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 8 | 12 => 2,
                0..=31 => 6,
                _ => 1,
            };
            self.0
                .add(width)
                .map_err(|_| std::io::Error::other("bounded packed seed"))?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn validate_seed_wire(seed: &[ChatMessage], maximum: usize) -> Result<(), GrokAppError> {
    let messages = if seed
        .first()
        .is_some_and(|message| message.role == ChatRole::System)
    {
        &seed[1..]
    } else {
        seed
    };
    let mut budget = EscapedBudget(Budget::new(maximum));
    budget.0.add(WIRE_RESERVE)?;
    let packed = if messages.len() == 1
        && matches!(
            messages[0].role,
            ChatRole::User | ChatRole::System | ChatRole::Developer
        ) {
        budget.write_all(messages[0].content.as_bytes())
    } else if !messages.is_empty() {
        budget
            .write_all(HISTORY_PREFIX.as_bytes())
            .and_then(|()| {
                serde_json::to_writer(&mut budget, &SeedMessages(messages))
                    .map_err(std::io::Error::other)
            })
            .and_then(|()| budget.write_all(HISTORY_SUFFIX.as_bytes()))
    } else {
        Ok(())
    };
    packed.map_err(|_| GrokAppError::Invalid("source packed seed wire bound"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        net::{IpAddr, Ipv4Addr},
        sync::Arc,
    };

    fn config() -> GrokAppConfig {
        GrokAppConfig::loopback(
            "synthetic-app-key",
            SocketAddr::from(([127, 0, 0, 1], 19001)),
        )
    }
    fn tool(name: &str) -> GrokLocalTool {
        GrokLocalTool {
            schema: FunctionTool::new(
                name,
                "Deterministic echo",
                json!({"type":"object", "properties":{"text":{"type":"string"}}, "required":["text"], "additionalProperties":false}),
            ),
            handler: RegisteredTool::new(Arc::new(|call| {
                Box::pin(async move { Ok(call.arguments) })
            })),
        }
    }
    fn validate(
        config: &mut GrokAppConfig,
        tools: &mut [GrokLocalTool],
    ) -> Result<(), GrokAppError> {
        config.validate_owned(&mut Vec::new(), tools)
    }

    #[test]
    fn exact_bindings_and_seed_are_compacted_before_retention() {
        let mut config = config();
        let mut text = String::with_capacity(4096);
        text.push_str("hello");
        let mut seed = Vec::with_capacity(100);
        seed.push(ChatMessage::new(ChatRole::System, "Be concise."));
        seed.push(ChatMessage::new(ChatRole::User, text));
        let mut tools = vec![tool("echo")];
        config.validate_owned(&mut seed, &mut tools).unwrap();
        assert_eq!(seed.capacity(), seed.len());
        assert_eq!(seed[1].content.capacity(), seed[1].content.len());
        let offered = config.connection.session.tools.as_ref().unwrap();
        assert_eq!(offered, &vec![tools[0].schema.clone().into()]);
        assert_eq!(config.connection.limits.max_session_responses, 64);
        assert_eq!(config.connection.limits.max_outstanding_tools, 8);
        assert_eq!(seed[0].role, ChatRole::System);
    }

    #[test]
    fn matching_explicit_schemas_may_differ_only_in_order() {
        let mut config = config();
        let mut tools = vec![tool("first"), tool("second")];
        config.connection.session.tools = Some(vec![
            tools[1].schema.clone().into(),
            tools[0].schema.clone().into(),
        ]);
        validate(&mut config, &mut tools).unwrap();
        assert_eq!(
            config.connection.session.tools.as_ref().unwrap()[0],
            tools[0].schema.clone().into()
        );
    }

    #[test]
    fn schema_conflicts_missing_extra_and_duplicate_names_fail() {
        let mut conflict = config();
        let mut other = tool("echo");
        other.schema.description.push('!');
        conflict.connection.session.tools = Some(vec![other.schema.into()]);
        assert!(validate(&mut conflict, &mut [tool("echo")]).is_err());
        let mut missing = config();
        missing.connection.session.tools = Some(Vec::new());
        assert!(validate(&mut missing, &mut [tool("echo")]).is_err());
        let mut extra = config();
        extra.connection.session.tools = Some(vec![tool("echo").schema.into()]);
        assert!(validate(&mut extra, &mut []).is_err());
        assert!(validate(&mut config(), &mut [tool("echo"), tool("echo")]).is_err());
    }

    #[test]
    fn session_hosted_tools_and_noninterruptible_handlers_fail() {
        let mut hosted = config();
        hosted.connection.session.tools = Some(vec![GrokTool::WebSearch]);
        assert!(validate(&mut hosted, &mut [tool("echo")]).is_err());
        let mut local = tool("echo");
        local.handler.cancel_on_interruption = false;
        assert!(validate(&mut config(), &mut [local]).is_err());
        let mut local = tool("echo");
        local.handler.timeout = Duration::ZERO;
        assert!(validate(&mut config(), &mut [local]).is_err());
        let mut local = tool("echo");
        local.handler.timeout = Duration::from_secs(31);
        assert!(validate(&mut config(), &mut [local]).is_err());
    }

    #[test]
    fn deserialized_nonfunction_discriminator_fails() {
        let mut local = tool("echo");
        local.schema = serde_json::from_value(
            json!({"type":"web_search", "name":"echo", "description":"", "parameters":{}}),
        )
        .unwrap();
        assert!(validate(&mut config(), &mut [local]).is_err());
    }

    #[test]
    fn private_discriminator_and_credential_capacities_are_charged() {
        fn small_config() -> GrokAppConfig {
            let mut config = config();
            config.limits.max_retained_bytes = 64 * 1024;
            config.limits.max_history_bytes = 32 * 1024;
            config.limits.max_schema_bytes = 16 * 1024;
            config
        }
        fn oversized(text: &str) -> String {
            let mut value = String::with_capacity(65_537);
            value.push_str(text);
            value
        }
        let mut config = small_config();
        let mut local = tool("echo");
        let mut raw = json!({ "name": "echo", "description": "", "parameters": {} });
        raw["type"] = Value::String(oversized("function"));
        local.schema = serde_json::from_value(raw).unwrap();
        assert!(validate(&mut config, &mut [local]).is_err());
        let mut config = small_config();
        let mut raw = json!({});
        raw["type"] = Value::String(oversized("server_vad"));
        config.connection.session.turn_detection = Some(Some(serde_json::from_value(raw).unwrap()));
        assert!(validate(&mut config, &mut []).is_err());
        let mut config = small_config();
        config.connection = RealtimeConfig::new(oversized("synthetic"));
        config.connection.endpoint = "ws://127.0.0.1:19001/v1/realtime".into();
        assert!(validate(&mut config, &mut []).is_err());
    }

    #[test]
    fn ordinary_seed_rejects_tool_roles_names_ids_and_calls() {
        for message in [
            ChatMessage::new(ChatRole::Tool, "forged"),
            ChatMessage {
                name: Some("named".into()),
                ..ChatMessage::new(ChatRole::User, "text")
            },
            ChatMessage {
                tool_call_id: Some("call".into()),
                ..ChatMessage::new(ChatRole::Assistant, "text")
            },
            ChatMessage {
                tool_calls: vec![pipecat_core::FunctionCall {
                    function_name: "echo".into(),
                    tool_call_id: "call".into(),
                    arguments: json!({}),
                }],
                ..ChatMessage::new(ChatRole::Assistant, "text")
            },
        ] {
            assert!(config()
                .validate_owned(&mut vec![message], &mut [])
                .is_err());
        }
    }

    #[test]
    fn oversized_seed_and_hidden_empty_call_capacity_fail() {
        let mut config = config();
        let oversized = "x".repeat(config.limits.max_text_bytes + 1);
        assert!(config
            .validate_owned(
                &mut vec![ChatMessage::new(ChatRole::User, oversized)],
                &mut []
            )
            .is_err());
        let mut config = super::tests::config();
        config.limits.max_retained_bytes = 512 * 1024;
        let mut message = ChatMessage::new(ChatRole::User, "small");
        message.tool_calls = Vec::with_capacity(8192);
        assert!(config.validate_owned(&mut vec![message], &mut []).is_err());
    }

    #[test]
    fn fixed_hosted_and_exact_loopback_routes_only() {
        let mut hosted = GrokAppConfig::new(
            "synthetic",
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)), 443),
        );
        validate(&mut hosted, &mut []).unwrap();
        hosted.resolved_address.set_port(8443);
        assert!(validate(&mut hosted, &mut []).is_err());
        for endpoint in [
            "ws://localhost:19001/v1/realtime",
            "ws://127.0.0.1:19002/v1/realtime",
            "ws://127.0.0.1:19001/v1/realtime?model=x",
            "wss://example.com/v1/realtime",
        ] {
            let mut local = config();
            local.connection.endpoint = endpoint.into();
            assert!(validate(&mut local, &mut []).is_err());
        }
        let mut local = GrokAppConfig::loopback("synthetic", "[::1]:19001".parse().unwrap());
        validate(&mut local, &mut []).unwrap();
    }

    #[test]
    fn manual_vad_binary_other_rates_and_resumption_fail() {
        let mut manual = config();
        manual.connection.session.turn_detection = Some(None);
        assert!(validate(&mut manual, &mut []).is_err());
        let mut binary = config();
        binary
            .connection
            .session
            .audio
            .as_mut()
            .unwrap()
            .input
            .as_mut()
            .unwrap()
            .transport = Some(AudioTransport::Binary);
        assert!(validate(&mut binary, &mut []).is_err());
        let mut rate = config();
        rate.connection
            .session
            .audio
            .as_mut()
            .unwrap()
            .output
            .as_mut()
            .unwrap()
            .format = AudioFormat::Pcm { rate: 48_000 };
        assert!(validate(&mut rate, &mut []).is_err());
        let mut resume = config();
        resume.connection.session.resumption = Some(crate::SessionResumption { enabled: false });
        assert!(validate(&mut resume, &mut []).is_err());
    }

    #[test]
    fn json_encoded_size_depth_and_value_count_are_independently_bounded() {
        let mut large = Value::String("\0".repeat(100));
        assert!(compact_json(&mut large, 100).is_err());
        let mut deep = Value::Null;
        for _ in 0..34 {
            deep = Value::Array(vec![deep]);
        }
        assert!(compact_json(&mut deep, 4096).is_err());
        let mut config = config();
        config.limits.max_json_values = 2;
        assert!(validate(&mut config, &mut [tool("echo")]).is_err());
    }

    #[test]
    fn json_capacity_is_checked_and_safe_spare_storage_is_removed() {
        let mut huge = String::with_capacity(8192);
        huge.push('x');
        assert!(compact_json(&mut Value::String(huge), 64).is_err());
        let mut text = String::with_capacity(512);
        text.push_str("ok");
        let mut array = Vec::with_capacity(32);
        array.push(Value::String(text));
        let mut value = Value::Array(array);
        compact_json(&mut value, 128).unwrap();
        let array = value.as_array().unwrap();
        assert_eq!(array.capacity(), array.len());
        let Value::String(text) = &array[0] else {
            panic!("string retained")
        };
        assert_eq!(text.capacity(), text.len());
        let mut object = json!({"ok":true});
        compact_json(&mut object, 128).unwrap();
    }

    #[test]
    fn deeply_rejected_values_are_disposed_without_recursive_drop() {
        fn deep() -> Value {
            let mut value = Value::Null;
            for _ in 0..20_000 {
                value = Value::Array(vec![value]);
            }
            value
        }
        let mut value = deep();
        assert!(compact_json(&mut value, 4096).is_err());
        assert!(value.is_null());
        let mut config = config();
        config.connection.endpoint = "invalid".into();
        let mut local = tool("echo");
        local.schema.parameters = deep();
        let mut tools = [local];
        assert!(validate(&mut config, &mut tools).is_err());
        assert!(tools[0].schema.parameters.is_null());
        let mut seed = vec![ChatMessage {
            tool_calls: vec![pipecat_core::FunctionCall {
                function_name: "bad".into(),
                tool_call_id: "bad".into(),
                arguments: deep(),
            }],
            ..ChatMessage::new(ChatRole::User, "bad")
        }];
        assert!(config.validate_owned(&mut seed, &mut []).is_err());
        assert!(seed[0].tool_calls[0].arguments.is_null());
    }

    #[test]
    fn source_packed_seed_accounts_for_double_escaping_and_source_framing() {
        let seed = vec![ChatMessage::new(ChatRole::Assistant, "\"\\\n".repeat(100))];
        let raw = encoded_len(&seed, 4096).unwrap();
        assert!(validate_seed_wire(&seed, raw + WIRE_RESERVE).is_err());
        validate_seed_wire(&seed, 4096).unwrap();
        let single = vec![ChatMessage::new(ChatRole::User, "hello")];
        validate_seed_wire(&single, WIRE_RESERVE + 5).unwrap();
    }

    #[test]
    fn zero_and_excessive_limits_fail_without_allocating() {
        let mut config = config();
        config.limits.max_json_values = usize::MAX;
        assert!(validate(&mut config, &mut []).is_err());
        let mut config = super::tests::config();
        config.limits.session_timeout = Duration::ZERO;
        assert!(validate(&mut config, &mut []).is_err());
    }

    #[test]
    fn error_format_is_static_bounded_and_single_line() {
        let error = GrokAppError::Failed("a\n\r\tstage");
        assert!(!format!("{error:?}").contains(['\n', '\r', '\t']));
        assert_eq!(format!("{error:?}"), error.to_string());
        assert!(GrokAppError::Invalid("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx").to_string().len() < 128);
    }
}
