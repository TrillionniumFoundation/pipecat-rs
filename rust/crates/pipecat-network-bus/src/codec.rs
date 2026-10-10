//! Python's qualified-name JSON format, without runtime type loading.
use std::sync::atomic::{AtomicU64, Ordering};

use pipecat_workers::{BusMessage, JobStatus, MessageData};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::frames::{decode_frame, encode_frame, supported_frame};

pub const JOB_STATUS_TYPE: &str = "pipecat.pipeline.job_context.JobStatus";
pub const DIRECTION_TYPE: &str = "pipecat.processors.frame_processor.FrameDirection";
pub const REGISTRY_ENTRY_TYPE: &str = "pipecat.registry.types.WorkerRegistryEntry";
pub(crate) const BUS_PREFIX: &str = "pipecat.bus.messages.";
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("unsupported Python type: {0}")]
    UnsupportedType(String),
    #[error("unsupported native frame: {0}")]
    UnsupportedFrame(String),
    #[error("invalid field: {0}")]
    InvalidField(String),
    #[error("local-only worker references cannot cross the network")]
    LocalWorkerReference,
    #[error("message exceeds {0} bytes")]
    TooLarge(usize),
    #[error("Python integer outside the supported signed/unsigned 64-bit range")]
    UnsupportedInteger,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerRegistryEntry {
    pub name: String,
    pub parent: Option<String>,
    #[serde(default)]
    pub active: bool,
    #[serde(default)]
    pub bridged: bool,
    pub started_at: Option<f64>,
}

/// Typed projections of every network-safe bus message in the Python source.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedMessage {
    Bus(BusMessage),
    Registry {
        runner: String,
        workers: Vec<WorkerRegistryEntry>,
    },
    FlushProgress {
        flush_id: u64,
    },
    Base,
    LocalError {
        error: String,
    },
}

/// Immutable, validated envelope. Forward this to preserve Python message/frame
/// identity, optional fields, adapter data, and fields absent from native workers.
/// `as_bus_message` is a projection; re-encoding it as a fresh message can lose
/// source-only fields (ready timestamps, stream metadata, unknown extensions).
#[derive(Debug, Clone, PartialEq)]
pub struct MessageEnvelope {
    wire: Value,
    decoded: DecodedMessage,
}
impl MessageEnvelope {
    pub fn decoded(&self) -> &DecodedMessage {
        &self.decoded
    }
    pub fn as_bus_message(&self) -> Option<&BusMessage> {
        match &self.decoded {
            DecodedMessage::Bus(message) => Some(message),
            _ => None,
        }
    }
    /// Fields retained by the envelope but absent from the native worker model.
    /// Keep the envelope intact when forwarding a message with these fields.
    pub fn projection_warnings(&self) -> Vec<&'static str> {
        let mut warnings = vec![];
        let fields = self.fields();
        if self.type_name().ends_with("BusWorkerReadyMessage") && fields.contains_key("started_at")
        {
            warnings.push("ready.started_at");
        }
        if (self.type_name().ends_with("BusJobStreamStartMessage")
            || self.type_name().ends_with("BusJobStreamEndMessage"))
            && fields.contains_key("data")
        {
            warnings.push("stream.data");
        }

        warnings
    }
    pub fn wire_value(&self) -> &Value {
        &self.wire
    }
    pub fn fields(&self) -> &Map<String, Value> {
        self.wire["__data__"]
            .as_object()
            .expect("validated envelope")
    }
    pub fn source(&self) -> &str {
        self.fields()["source"].as_str().expect("validated source")
    }
    pub fn target(&self) -> Option<&str> {
        self.fields().get("target").and_then(Value::as_str)
    }
    pub fn id(&self) -> Option<u64> {
        self.fields().get("id").and_then(Value::as_u64)
    }
    pub fn name(&self) -> Option<&str> {
        self.fields().get("name").and_then(Value::as_str)
    }
    pub fn type_name(&self) -> &str {
        self.wire["__type__"].as_str().expect("validated type")
    }
    pub fn is_local_only(&self) -> bool {
        matches!(self.decoded, DecodedMessage::LocalError { .. })
    }
    pub fn is_system(&self) -> bool {
        matches!(
            self.type_name().strip_prefix(BUS_PREFIX).unwrap_or(""),
            "BusSystemMessage"
                | "BusCancelMessage"
                | "BusCancelWorkerMessage"
                | "BusWorkerRegistryMessage"
                | "BusWorkerErrorMessage"
                | "BusWorkerLocalErrorMessage"
                | "BusJobResponseUrgentMessage"
                | "BusJobUpdateUrgentMessage"
                | "BusJobCancelMessage"
        )
    }
}

#[derive(Debug, Clone)]
pub struct PythonJsonCodec {
    pub max_message_bytes: usize,
}
impl Default for PythonJsonCodec {
    fn default() -> Self {
        Self {
            max_message_bytes: 8 * 1024 * 1024,
        }
    }
}
impl PythonJsonCodec {
    pub fn decode(&self, bytes: &[u8]) -> Result<MessageEnvelope, CodecError> {
        if bytes.len() > self.max_message_bytes {
            return Err(CodecError::TooLarge(self.max_message_bytes));
        }
        validate_integer_range(bytes)?;
        self.decode_value(serde_json::from_slice(bytes)?)
    }
    pub fn encode(&self, envelope: &MessageEnvelope) -> Result<Vec<u8>, CodecError> {
        let bytes = serde_json::to_vec(envelope.wire_value())?;
        if bytes.len() > self.max_message_bytes {
            return Err(CodecError::TooLarge(self.max_message_bytes));
        }
        Ok(bytes)
    }
    pub fn decode_value(&self, wire: Value) -> Result<MessageEnvelope, CodecError> {
        validate_value(&wire, 0)?;
        let bytes = serde_json::to_vec(&wire)?;
        if bytes.len() > self.max_message_bytes {
            return Err(CodecError::TooLarge(self.max_message_bytes));
        }
        validate_integer_range(&bytes)?;
        let (qualified, fields) = object_tag(&wire)?;
        let name = qualified
            .strip_prefix(BUS_PREFIX)
            .ok_or_else(|| CodecError::UnsupportedType(qualified.into()))?;
        let source = required(fields, "source")?;
        let target = optional(fields, "target")?;
        let _: Option<u64> = optional(fields, "id")?;
        let _: Option<String> = optional(fields, "name")?;
        let data = match name {
            "BusMessage" | "BusDataMessage" | "BusSystemMessage" => {
                return Ok(MessageEnvelope {
                    wire,
                    decoded: DecodedMessage::Base,
                })
            }
            "BusWorkerLocalErrorMessage" => {
                return Ok(MessageEnvelope {
                    decoded: DecodedMessage::LocalError {
                        error: required(fields, "error")?,
                    },
                    wire,
                })
            }
            "BusFlushProgressMessage" => {
                return Ok(MessageEnvelope {
                    decoded: DecodedMessage::FlushProgress {
                        flush_id: required(fields, "flush_id")?,
                    },
                    wire,
                })
            }
            "BusWorkerRegistryMessage" => {
                let entries = fields
                    .get("workers")
                    .and_then(Value::as_array)
                    .ok_or_else(|| invalid("workers"))?;
                let workers = entries
                    .iter()
                    .map(|entry| {
                        let (name, data) = object_tag(entry)?;
                        if name != REGISTRY_ENTRY_TYPE {
                            return Err(CodecError::UnsupportedType(name.into()));
                        }
                        serde_json::from_value(Value::Object(data.clone()))
                            .map_err(CodecError::from)
                    })
                    .collect::<Result<_, _>>()?;
                return Ok(MessageEnvelope {
                    decoded: DecodedMessage::Registry {
                        runner: required(fields, "runner")?,
                        workers,
                    },
                    wire,
                });
            }
            "BusActivateWorkerMessage" => MessageData::Activate {
                args: dictionary(fields, "args")?,
            },
            "BusDeactivateWorkerMessage" => MessageData::Deactivate,
            "BusEndMessage" => MessageData::End {
                reason: optional(fields, "reason")?,
            },
            "BusEndWorkerMessage" => MessageData::EndWorker {
                reason: optional(fields, "reason")?,
            },
            "BusCancelMessage" => MessageData::Cancel {
                reason: optional(fields, "reason")?,
            },
            "BusCancelWorkerMessage" => MessageData::CancelWorker {
                reason: optional(fields, "reason")?,
            },
            "BusWorkerReadyMessage" => {
                let _: Option<f64> = optional(fields, "started_at")?;
                MessageData::Ready {
                    runner: required(fields, "runner")?,
                    parent: optional(fields, "parent")?,
                    active: defaulted(fields, "active", false)?,
                    bridged: defaulted(fields, "bridged", false)?,
                }
            }
            "BusWorkerErrorMessage" => MessageData::WorkerError {
                error: required(fields, "error")?,
            },
            "BusTTSSpeakMessage" => MessageData::Speak {
                text: required(fields, "text")?,
                append_to_context: defaulted(fields, "append_to_context", true)?,
            },
            "BusFrameMessage" => MessageData::Frame {
                frame: Box::new(decode_frame(
                    fields.get("frame").ok_or_else(|| invalid("frame"))?,
                )?),
                direction: decode_direction(
                    fields
                        .get("direction")
                        .ok_or_else(|| invalid("direction"))?,
                )?,
                bridge: optional(fields, "bridge")?,
            },
            "BusJobRequestMessage" => MessageData::JobRequest {
                job_id: required(fields, "job_id")?,
                name: optional(fields, "job_name")?,
                payload: dictionary(fields, "payload")?,
            },
            "BusJobResponseMessage" | "BusJobResponseUrgentMessage" => MessageData::JobResponse {
                job_id: required(fields, "job_id")?,
                status: decode_status(fields.get("status").ok_or_else(|| invalid("status"))?)?,
                response: dictionary(fields, "response")?,
                urgent: name == "BusJobResponseUrgentMessage",
            },
            "BusJobUpdateMessage" | "BusJobUpdateUrgentMessage" => MessageData::JobUpdate {
                job_id: required(fields, "job_id")?,
                update: dictionary(fields, "update")?,
                urgent: name == "BusJobUpdateUrgentMessage",
            },
            "BusJobCancelMessage" => MessageData::JobCancel {
                job_id: required(fields, "job_id")?,
                reason: optional(fields, "reason")?,
            },
            "BusJobUpdateRequestMessage" => MessageData::JobUpdateRequest {
                job_id: required(fields, "job_id")?,
            },
            "BusJobStreamStartMessage" => {
                dictionary(fields, "data")?;
                MessageData::StreamStart {
                    job_id: required(fields, "job_id")?,
                }
            }
            "BusJobStreamDataMessage" => MessageData::StreamData {
                job_id: required(fields, "job_id")?,
                data: dictionary(fields, "data")?.unwrap_or(Value::Null),
            },
            "BusJobStreamEndMessage" => {
                dictionary(fields, "data")?;
                MessageData::StreamEnd {
                    job_id: required(fields, "job_id")?,
                }
            }
            _ => return Err(CodecError::UnsupportedType(qualified.into())),
        };
        Ok(MessageEnvelope {
            wire,
            decoded: DecodedMessage::Bus(BusMessage {
                source,
                target,
                data,
            }),
        })
    }
    /// Make a new Python message identity. Keep an existing envelope when forwarding.
    pub fn from_bus_message(&self, message: &BusMessage) -> Result<MessageEnvelope, CodecError> {
        let (name, mut fields) = encode_message(&message.data)?;
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        fields.insert("id".into(), id.into());
        fields.insert("name".into(), format!("{name}#{id}").into());
        fields.insert("source".into(), message.source.clone().into());
        put_optional(&mut fields, "target", &message.target)?;
        self.decode_value(tag(&format!("{BUS_PREFIX}{name}"), Value::Object(fields)))
    }
}

// serde_json's default Value parser can silently round an integer outside
// i64/u64 through f64. Reject those tokens before parsing while leaving quoted
// numbers untouched. JSON syntax and finite floating-point validation remain
// the JSON parser's responsibility.
pub(crate) fn validate_integer_range(bytes: &[u8]) -> Result<(), CodecError> {
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                index += 1;
                while index < bytes.len() {
                    match bytes[index] {
                        b'\\' => index += 2,
                        b'"' => {
                            index += 1;
                            break;
                        }
                        _ => index += 1,
                    }
                }
            }
            b'-' | b'0'..=b'9' => {
                if bytes[index] == b'-' && !bytes.get(index + 1).is_some_and(u8::is_ascii_digit) {
                    index += 1;
                    continue;
                }
                let start = index;
                while index < bytes.len()
                    && matches!(bytes[index], b'0'..=b'9' | b'-' | b'+' | b'.' | b'e' | b'E')
                {
                    index += 1;
                }
                let token = &bytes[start..index];
                if !token.iter().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
                    let token =
                        std::str::from_utf8(token).map_err(|_| CodecError::UnsupportedInteger)?;
                    let supported = if token.starts_with('-') {
                        token.parse::<i64>().is_ok()
                    } else {
                        token.parse::<u64>().is_ok()
                    };
                    if !supported {
                        return Err(CodecError::UnsupportedInteger);
                    }
                }
            }
            _ => index += 1,
        }
    }
    Ok(())
}

fn encode_message(data: &MessageData) -> Result<(&'static str, Map<String, Value>), CodecError> {
    let mut f = Map::new();
    let name = match data {
        MessageData::Activate { args } => {
            put_optional(&mut f, "args", args)?;
            "BusActivateWorkerMessage"
        }
        MessageData::Deactivate => "BusDeactivateWorkerMessage",
        MessageData::End { reason } => {
            put_optional(&mut f, "reason", reason)?;
            "BusEndMessage"
        }
        MessageData::EndWorker { reason } => {
            put_optional(&mut f, "reason", reason)?;
            "BusEndWorkerMessage"
        }
        MessageData::Cancel { reason } => {
            put_optional(&mut f, "reason", reason)?;
            "BusCancelMessage"
        }
        MessageData::CancelWorker { reason } => {
            put_optional(&mut f, "reason", reason)?;
            "BusCancelWorkerMessage"
        }
        MessageData::Ready {
            runner,
            parent,
            active,
            bridged,
        } => {
            f.extend(
                json!({"runner":runner,"active":active,"bridged":bridged})
                    .as_object()
                    .unwrap()
                    .clone(),
            );
            put_optional(&mut f, "parent", parent)?;
            "BusWorkerReadyMessage"
        }
        MessageData::WorkerError { error } => {
            f.insert("error".into(), error.clone().into());
            "BusWorkerErrorMessage"
        }
        MessageData::Speak {
            text,
            append_to_context,
        } => {
            f.insert("text".into(), text.clone().into());
            f.insert("append_to_context".into(), (*append_to_context).into());
            "BusTTSSpeakMessage"
        }
        MessageData::Frame {
            frame,
            direction,
            bridge,
        } => {
            f.insert("frame".into(), encode_frame(frame)?);
            f.insert(
                "direction".into(),
                tag(
                    DIRECTION_TYPE,
                    match direction {
                        pipecat_core::Direction::Downstream => "DOWNSTREAM",
                        pipecat_core::Direction::Upstream => "UPSTREAM",
                    }
                    .into(),
                ),
            );
            put_optional(&mut f, "bridge", bridge)?;
            "BusFrameMessage"
        }
        MessageData::JobRequest {
            job_id,
            name,
            payload,
        } => {
            f.insert("job_id".into(), job_id.clone().into());
            put_optional(&mut f, "job_name", name)?;
            put_optional(&mut f, "payload", payload)?;
            "BusJobRequestMessage"
        }
        MessageData::JobResponse {
            job_id,
            status,
            response,
            urgent,
        } => {
            f.insert("job_id".into(), job_id.clone().into());
            f.insert("status".into(), serde_json::to_value(status)?);
            put_optional(&mut f, "response", response)?;
            if *urgent {
                "BusJobResponseUrgentMessage"
            } else {
                "BusJobResponseMessage"
            }
        }
        MessageData::JobUpdate {
            job_id,
            update,
            urgent,
        } => {
            f.insert("job_id".into(), job_id.clone().into());
            put_optional(&mut f, "update", update)?;
            if *urgent {
                "BusJobUpdateUrgentMessage"
            } else {
                "BusJobUpdateMessage"
            }
        }
        MessageData::JobCancel { job_id, reason } => {
            f.insert("job_id".into(), job_id.clone().into());
            put_optional(&mut f, "reason", reason)?;
            "BusJobCancelMessage"
        }
        MessageData::JobUpdateRequest { job_id } => {
            f.insert("job_id".into(), job_id.clone().into());
            "BusJobUpdateRequestMessage"
        }
        MessageData::StreamStart { job_id } => {
            f.insert("job_id".into(), job_id.clone().into());
            "BusJobStreamStartMessage"
        }
        MessageData::StreamData { job_id, data } => {
            f.insert("job_id".into(), job_id.clone().into());
            if !data.is_null() {
                f.insert("data".into(), data.clone());
            }
            "BusJobStreamDataMessage"
        }
        MessageData::StreamEnd { job_id } => {
            f.insert("job_id".into(), job_id.clone().into());
            "BusJobStreamEndMessage"
        }
    };
    Ok((name, f))
}

pub(crate) fn tag(name: &str, data: Value) -> Value {
    json!({"__type__": name, "__data__": data})
}
pub(crate) fn invalid(field: &str) -> CodecError {
    CodecError::InvalidField(field.into())
}
pub(crate) fn object_tag(value: &Value) -> Result<(&str, &Map<String, Value>), CodecError> {
    Ok((
        value
            .get("__type__")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("__type__"))?,
        value
            .get("__data__")
            .and_then(Value::as_object)
            .ok_or_else(|| invalid("__data__"))?,
    ))
}
pub(crate) fn required<T: DeserializeOwned>(
    fields: &Map<String, Value>,
    key: &str,
) -> Result<T, CodecError> {
    serde_json::from_value(fields.get(key).cloned().ok_or_else(|| invalid(key))?)
        .map_err(|_| invalid(key))
}
pub(crate) fn optional<T: DeserializeOwned>(
    fields: &Map<String, Value>,
    key: &str,
) -> Result<Option<T>, CodecError> {
    match fields.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => serde_json::from_value(value.clone())
            .map(Some)
            .map_err(|_| invalid(key)),
    }
}
pub(crate) fn defaulted<T: DeserializeOwned>(
    fields: &Map<String, Value>,
    key: &str,
    default: T,
) -> Result<T, CodecError> {
    match fields.get(key) {
        None => Ok(default),
        Some(_) => required(fields, key),
    }
}
pub(crate) fn put_optional<T: Serialize>(
    fields: &mut Map<String, Value>,
    key: &str,
    value: &Option<T>,
) -> Result<(), CodecError> {
    if let Some(value) = value {
        let value = serde_json::to_value(value)?;
        if !value.is_null() {
            fields.insert(key.into(), value);
        }
    }
    Ok(())
}
fn dictionary(fields: &Map<String, Value>, key: &str) -> Result<Option<Value>, CodecError> {
    match fields.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value @ Value::Object(_)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid(key)),
    }
}
fn decode_status(value: &Value) -> Result<JobStatus, CodecError> {
    if let Some(value) = value.as_str() {
        return serde_json::from_value(value.into()).map_err(|_| invalid("status"));
    }
    if value.get("__type__").and_then(Value::as_str) != Some(JOB_STATUS_TYPE) {
        return Err(invalid("status"));
    }
    match value["__data__"].as_str() {
        Some("COMPLETED") => Ok(JobStatus::Completed),
        Some("CANCELLED") => Ok(JobStatus::Cancelled),
        Some("FAILED") => Ok(JobStatus::Failed),
        Some("ERROR") => Ok(JobStatus::Error),
        _ => Err(invalid("status")),
    }
}
fn decode_direction(value: &Value) -> Result<pipecat_core::Direction, CodecError> {
    if value.get("__type__").and_then(Value::as_str) != Some(DIRECTION_TYPE) {
        return Err(invalid("direction"));
    }
    match value["__data__"].as_str() {
        Some("DOWNSTREAM") => Ok(pipecat_core::Direction::Downstream),
        Some("UPSTREAM") => Ok(pipecat_core::Direction::Upstream),
        _ => Err(invalid("direction")),
    }
}
fn validate_value(value: &Value, depth: usize) -> Result<(), CodecError> {
    if depth > 64 {
        return Err(invalid("maximum nesting depth"));
    }
    match value {
        Value::Array(values) => {
            for value in values {
                validate_value(value, depth + 1)?;
            }
        }
        Value::Object(fields) => {
            if fields.contains_key("__type__") && fields.contains_key("__data__") {
                let name = fields
                    .get("__type__")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("__type__"))?;
                let data = fields.get("__data__").ok_or_else(|| invalid("__data__"))?;
                if name == "bytes" {
                    crate::frames::decode_bytes(value)?;
                } else if name == DIRECTION_TYPE {
                    decode_direction(value)?;
                } else if name == JOB_STATUS_TYPE {
                    decode_status(value)?;
                } else if name == "pipecat.bus.messages.BusAddWorkerMessage" {
                    return Err(CodecError::LocalWorkerReference);
                } else if crate::frames::supported_aux_type(name, data)? {
                    if name != "pipecat.utils.errors.ErrorCategory" && !data.is_object() {
                        return Err(invalid("__data__"));
                    }
                } else if is_bus_type(name)
                    || name == REGISTRY_ENTRY_TYPE
                    || supported_frame(name)
                    || matches!(
                        name,
                        "pipecat.processors.aggregators.llm_context.LLMContext"
                            | "pipecat.adapters.schemas.tools_schema.ToolsSchema"
                    )
                {
                    if !data.is_object() {
                        return Err(invalid("__data__"));
                    }
                } else {
                    return Err(CodecError::UnsupportedType(name.into()));
                }
            }
            for value in fields.values() {
                validate_value(value, depth + 1)?;
            }
        }
        _ => {}
    }
    Ok(())
}
fn is_bus_type(name: &str) -> bool {
    matches!(
        name.strip_prefix(BUS_PREFIX),
        Some(
            "BusMessage"
                | "BusDataMessage"
                | "BusSystemMessage"
                | "BusFrameMessage"
                | "BusTTSSpeakMessage"
                | "BusFlushProgressMessage"
                | "BusActivateWorkerMessage"
                | "BusDeactivateWorkerMessage"
                | "BusEndMessage"
                | "BusEndWorkerMessage"
                | "BusCancelMessage"
                | "BusCancelWorkerMessage"
                | "BusWorkerRegistryMessage"
                | "BusWorkerReadyMessage"
                | "BusWorkerErrorMessage"
                | "BusWorkerLocalErrorMessage"
                | "BusJobRequestMessage"
                | "BusJobResponseMessage"
                | "BusJobResponseUrgentMessage"
                | "BusJobUpdateMessage"
                | "BusJobUpdateUrgentMessage"
                | "BusJobUpdateRequestMessage"
                | "BusJobCancelMessage"
                | "BusJobStreamStartMessage"
                | "BusJobStreamDataMessage"
                | "BusJobStreamEndMessage"
        )
    )
}
