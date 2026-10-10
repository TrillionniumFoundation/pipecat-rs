//! Per-registration, bounded projections. Never attach these to shared observations.
use crate::{ErrorCategory, Frame, FrameData, ObservedProcessor, ProcessorHealthSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Limits are checked before cloning any application content. Node limits also
/// bound container overhead; bytes count UTF-8 content and fixed scalar costs,
/// not wire-encoded JSON. No serialization or arbitrary-object Debug is invoked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CaptureLimits {
    field_bytes: usize,
    total_bytes: usize,
    calls: usize,
    json_nodes: usize,
    json_depth: usize,
}
impl CaptureLimits {
    pub fn new(
        field_bytes: usize,
        total_bytes: usize,
        calls: usize,
        json_nodes: usize,
        json_depth: usize,
    ) -> Result<Self, &'static str> {
        if !(1..=65_536).contains(&field_bytes)
            || !(1..=262_144).contains(&total_bytes)
            || !(1..=256).contains(&calls)
            || !(1..=8192).contains(&json_nodes)
            || !(1..=32).contains(&json_depth)
        {
            return Err("capture limits exceed their supported positive bounds");
        }
        Ok(Self {
            field_bytes,
            total_bytes,
            calls,
            json_nodes,
            json_depth,
        })
    }
}
impl Default for CaptureLimits {
    fn default() -> Self {
        Self {
            field_bytes: 4096,
            total_bytes: 16_384,
            calls: 64,
            json_nodes: 1024,
            json_depth: 16,
        }
    }
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ErrorCaptureOptions {
    pub message: bool,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FunctionCaptureOptions {
    /// Function names, exact tool-call IDs, and group IDs. Off by default.
    pub identity: bool,
    pub arguments: bool,
    pub results: bool,
    pub errors: bool,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ObserverCapturePolicy {
    pub errors: Option<ErrorCaptureOptions>,
    pub functions: Option<FunctionCaptureOptions>,
    pub speech: bool,
    pub limits: CaptureLimits,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CaptureOmission {
    FieldBytes,
    TotalBytes,
    JsonNodes,
    JsonDepth,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Captured<T> {
    NotRequested,
    Present(T),
    Omitted(CaptureOmission),
}
impl<T> Captured<T> {
    pub fn value(&self) -> Option<&T> {
        if let Self::Present(value) = self {
            Some(value)
        } else {
            None
        }
    }
    pub fn is_omitted(&self) -> bool {
        matches!(self, Self::Omitted(_))
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedError {
    pub processor: ObservedProcessor,
    pub processor_usable: bool,
    pub category: ErrorCategory,
    pub fatal: bool,
    pub message: Captured<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FunctionCallPhase {
    Started,
    InProgress,
    Completed,
    Failed,
    TimedOut,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedFunctionCall {
    pub phase: FunctionCallPhase,
    pub function_name: Captured<String>,
    pub tool_call_id: Captured<String>,
    pub group_id: Captured<Option<String>>,
    pub blocking: Option<bool>,
    pub arguments: Captured<Value>,
    pub result: Captured<Value>,
    pub error: Captured<Option<String>>,
}
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedFunctionCalls {
    pub calls: Vec<CapturedFunctionCall>,
    pub omitted_calls: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpeechEventKind {
    UserSpeechStarted,
    UserSpeechStopped,
    UserTurnStarted,
    UserTurnStopped,
    BotSpeechStarted,
    BotSpeechStopped,
    Interruption,
}
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CapturedSpeech {
    pub kind: SpeechEventKind,
    /// VAD confirmation delay only. No payload wall-clock value is retained.
    pub confirmation_secs: f64,
}
#[derive(Debug, Clone, PartialEq)]
pub enum CapturedPayload {
    Error(CapturedError),
    Functions(CapturedFunctionCalls),
    Speech(CapturedSpeech),
}
#[derive(Debug, Clone, PartialEq)]
pub struct CapturedPush {
    /// Presence only, not a replayable sibling ID.
    pub broadcast: bool,
    pub payload: CapturedPayload,
}

struct Budget {
    limits: CaptureLimits,
    bytes: usize,
    nodes: usize,
}
impl Budget {
    fn string(&mut self, input: &str, enabled: bool) -> Captured<String> {
        if !enabled {
            return Captured::NotRequested;
        }
        if input.len() > self.limits.field_bytes {
            return Captured::Omitted(CaptureOmission::FieldBytes);
        }
        if input.len() > self.limits.total_bytes - self.bytes {
            return Captured::Omitted(CaptureOmission::TotalBytes);
        }
        self.bytes += input.len();
        Captured::Present(input.to_owned())
    }
    fn optional_string(&mut self, input: Option<&str>, enabled: bool) -> Captured<Option<String>> {
        if !enabled {
            return Captured::NotRequested;
        }
        match input {
            None => Captured::Present(None),
            Some(input) => match self.string(input, true) {
                Captured::Present(value) => Captured::Present(Some(value)),
                Captured::Omitted(reason) => Captured::Omitted(reason),
                Captured::NotRequested => unreachable!(),
            },
        }
    }
    fn json(&mut self, value: &Value, enabled: bool) -> Captured<Value> {
        if !enabled {
            return Captured::NotRequested;
        }
        let mut cost = Cost { bytes: 0, nodes: 0 };
        match self.scan(value, 0, &mut cost) {
            Ok(()) => {
                self.bytes += cost.bytes;
                self.nodes += cost.nodes;
                Captured::Present(value.clone())
            }
            Err(reason) => Captured::Omitted(reason),
        }
    }
    fn map(
        &mut self,
        value: &std::collections::BTreeMap<String, Value>,
        enabled: bool,
    ) -> Captured<Value> {
        if !enabled {
            return Captured::NotRequested;
        }
        let mut cost = Cost { bytes: 0, nodes: 0 };
        let checked = (|| {
            self.node(0, &mut cost)?;
            for (key, value) in value {
                self.bytes(key.len(), &mut cost)?;
                self.scan(value, 1, &mut cost)?;
            }
            Ok::<(), CaptureOmission>(())
        })();
        match checked {
            Ok(()) => {
                self.bytes += cost.bytes;
                self.nodes += cost.nodes;
                Captured::Present(Value::Object(
                    value
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect(),
                ))
            }
            Err(reason) => Captured::Omitted(reason),
        }
    }
    fn node(&self, depth: usize, cost: &mut Cost) -> Result<(), CaptureOmission> {
        if depth > self.limits.json_depth {
            return Err(CaptureOmission::JsonDepth);
        }
        if cost.nodes >= self.limits.json_nodes - self.nodes {
            return Err(CaptureOmission::JsonNodes);
        }
        cost.nodes += 1;
        self.bytes(1, cost)
    }
    fn bytes(&self, count: usize, cost: &mut Cost) -> Result<(), CaptureOmission> {
        cost.bytes = cost
            .bytes
            .checked_add(count)
            .ok_or(CaptureOmission::FieldBytes)?;
        if cost.bytes > self.limits.field_bytes {
            return Err(CaptureOmission::FieldBytes);
        }
        if cost.bytes > self.limits.total_bytes - self.bytes {
            return Err(CaptureOmission::TotalBytes);
        }
        Ok(())
    }
    fn scan(&self, value: &Value, depth: usize, cost: &mut Cost) -> Result<(), CaptureOmission> {
        self.node(depth, cost)?;
        match value {
            Value::Null | Value::Bool(_) => Ok(()),
            Value::Number(_) => self.bytes(32, cost),
            Value::String(value) => self.bytes(value.len(), cost),
            Value::Array(values) => {
                for value in values {
                    self.scan(value, depth + 1, cost)?;
                }
                Ok(())
            }
            Value::Object(values) => {
                for (key, value) in values {
                    self.bytes(key.len(), cost)?;
                    self.scan(value, depth + 1, cost)?;
                }
                Ok(())
            }
        }
    }
}
struct Cost {
    bytes: usize,
    nodes: usize,
}
impl ObserverCapturePolicy {
    pub(crate) fn capture(
        &self,
        frame: &Frame,
        source: &ObservedProcessor,
        source_health: ProcessorHealthSnapshot,
    ) -> Option<CapturedPush> {
        let mut budget = Budget {
            limits: self.limits,
            bytes: 0,
            nodes: 0,
        };
        let payload = if let Some(options) = self.errors {
            let error = match &frame.data {
                FrameData::Error(error) | FrameData::FatalError(error) => {
                    Some((error.category, error.fatal, error.error.as_str()))
                }
                FrameData::ErrorDetailed(error) => Some((
                    error.category.unwrap_or_default(),
                    error.fatal,
                    error.error.as_str(),
                )),
                _ => None,
            };
            error.map(|(category, fatal, message)| {
                let (processor, health) = frame
                    .error_origin()
                    .map_or((source, source_health), |origin| {
                        (&origin.processor, origin.reported_health)
                    });
                CapturedPayload::Error(CapturedError {
                    processor: processor.clone(),
                    processor_usable: health.is_usable,
                    category,
                    fatal,
                    message: budget.string(message, options.message),
                })
            })
        } else {
            None
        };
        let payload = payload
            .or_else(|| {
                self.functions.and_then(|options| {
                    self.capture_functions(&frame.data, options, &mut budget)
                        .map(CapturedPayload::Functions)
                })
            })
            .or_else(|| {
                self.speech
                    .then(|| capture_speech(&frame.data))
                    .flatten()
                    .map(CapturedPayload::Speech)
            })?;
        Some(CapturedPush {
            broadcast: frame.broadcast_sibling_id.is_some(),
            payload,
        })
    }
    fn capture_functions(
        &self,
        frame: &FrameData,
        options: FunctionCaptureOptions,
        budget: &mut Budget,
    ) -> Option<CapturedFunctionCalls> {
        use FunctionCallPhase::*;
        let mut calls = Vec::new();
        let mut omitted_calls = 0;
        match frame {
            FrameData::FunctionCallsStarted(values) => {
                omitted_calls = values.len().saturating_sub(self.limits.calls);
                for value in values.iter().take(self.limits.calls) {
                    let mut call = call_identity(
                        budget,
                        options,
                        Started,
                        &value.function_name,
                        &value.tool_call_id,
                    );
                    call.arguments = budget.json(&value.arguments, options.arguments);
                    calls.push(call);
                }
            }
            FrameData::FunctionCallsStartedDetailed(values) => {
                omitted_calls = values.len().saturating_sub(self.limits.calls);
                for value in values.iter().take(self.limits.calls) {
                    let mut call = call_identity(
                        budget,
                        options,
                        Started,
                        &value.function_name,
                        &value.tool_call_id,
                    );
                    call.arguments = budget.map(&value.arguments, options.arguments);
                    calls.push(call);
                }
            }
            FrameData::FunctionCallInProgress(value) => {
                let mut call = call_identity(
                    budget,
                    options,
                    InProgress,
                    &value.function_name,
                    &value.tool_call_id,
                );
                call.group_id = budget.optional_string(value.group_id.as_deref(), options.identity);
                call.blocking = Some(value.cancel_on_interruption);
                call.arguments = budget.json(&value.arguments, options.arguments);
                calls.push(call);
            }
            FrameData::FunctionCallResult(value) => {
                if value.is_final {
                    calls.push(result_call(
                        budget,
                        options,
                        &value.function_name,
                        &value.tool_call_id,
                        &value.result,
                        value.error.as_deref(),
                    ));
                }
            }
            FrameData::FunctionCallResultDetailed(value) => {
                if value
                    .properties
                    .as_ref()
                    .is_none_or(|properties| properties.is_final)
                {
                    calls.push(result_call(
                        budget,
                        options,
                        &value.function_name,
                        &value.tool_call_id,
                        &value.result,
                        value.error.as_deref(),
                    ));
                }
            }
            FrameData::FunctionCallCancel(value) => calls.push(call_identity(
                budget,
                options,
                if value.run_llm { TimedOut } else { Cancelled },
                &value.function_name,
                &value.tool_call_id,
            )),
            _ => return None,
        }
        Some(CapturedFunctionCalls {
            calls,
            omitted_calls,
        })
    }
}
fn call_identity(
    budget: &mut Budget,
    options: FunctionCaptureOptions,
    phase: FunctionCallPhase,
    name: &str,
    id: &str,
) -> CapturedFunctionCall {
    CapturedFunctionCall {
        phase,
        function_name: budget.string(name, options.identity),
        tool_call_id: budget.string(id, options.identity),
        group_id: Captured::NotRequested,
        blocking: None,
        arguments: Captured::NotRequested,
        result: Captured::NotRequested,
        error: Captured::NotRequested,
    }
}
fn result_call(
    budget: &mut Budget,
    options: FunctionCaptureOptions,
    name: &str,
    id: &str,
    result: &Value,
    error: Option<&str>,
) -> CapturedFunctionCall {
    // Match source truthiness: an empty error string is not a failed result.
    let failed = error.is_some_and(|error| !error.is_empty());
    let mut call = call_identity(
        budget,
        options,
        if failed {
            FunctionCallPhase::Failed
        } else {
            FunctionCallPhase::Completed
        },
        name,
        id,
    );
    call.result = budget.json(result, options.results && !failed);
    call.error = budget.optional_string(error, options.errors);
    call
}
fn capture_speech(frame: &FrameData) -> Option<CapturedSpeech> {
    use SpeechEventKind::*;
    let (kind, confirmation_secs) = match frame {
        FrameData::VadUserStartedSpeaking(value) => (UserSpeechStarted, value.duration_secs),
        FrameData::VadUserStoppedSpeaking(value) => (UserSpeechStopped, value.duration_secs),
        FrameData::VadUserStartedSpeakingDetailed(value) => (UserSpeechStarted, value.start_secs),
        FrameData::VadUserStoppedSpeakingDetailed(value) => (UserSpeechStopped, value.stop_secs),
        FrameData::UserStartedSpeaking => (UserTurnStarted, 0.0),
        FrameData::UserStoppedSpeaking => (UserTurnStopped, 0.0),
        FrameData::BotStartedSpeaking => (BotSpeechStarted, 0.0),
        FrameData::BotStoppedSpeaking => (BotSpeechStopped, 0.0),
        FrameData::Interruption => (Interruption, 0.0),
        _ => return None,
    };
    Some(CapturedSpeech {
        kind,
        confirmation_secs,
    })
}
