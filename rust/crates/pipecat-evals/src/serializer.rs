//! Eval-only control messages. Production RTVI endpoints must not accept these controls.
use crate::EvalError;
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_protocol::rtvi::{Envelope, MESSAGE_LABEL};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub const EVAL_CONTEXT: &str = "eval-context";
pub const EVAL_CONFIGURE: &str = "eval-configure";
pub const EVAL_CANCEL: &str = "eval-cancel";
pub const EVAL_IMAGE: &str = "eval-image";
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ReportLevel {
    None,
    Name,
    Full,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ObserverConfig {
    pub function_call_report_level: Option<BTreeMap<String, ReportLevel>>,
    pub vad_user_speaking: Option<bool>,
    pub llm_markers: Option<bool>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum EvalControl {
    /// Replace LLM context without running the LLM, matching run_llm=False.
    Context(Vec<Map<String, Value>>),
    Configure(ObserverConfig),
    Cancel,
    Image {
        bytes: Vec<u8>,
        format: String,
    },
    Rtvi(Envelope),
}
pub fn decode_control(bytes: &[u8], max_bytes: usize) -> Result<EvalControl, EvalError> {
    let message = Envelope::parse_with_limit(bytes, max_bytes)?;
    if message.message_type != "client-message" {
        return Ok(EvalControl::Rtvi(message));
    }
    let data = message.data.as_ref().unwrap_or(&Value::Null);
    let payload = data.get("d").cloned().unwrap_or_else(|| json!({}));
    match data.get("t").and_then(Value::as_str) {
        Some(EVAL_CONTEXT) => {
            let messages = payload
                .get("messages")
                .cloned()
                .unwrap_or_else(|| json!([]));
            Ok(EvalControl::Context(serde_json::from_value(messages)?))
        }
        Some(EVAL_CONFIGURE) => Ok(EvalControl::Configure(serde_json::from_value(payload)?)),
        Some(EVAL_CANCEL) => Ok(EvalControl::Cancel),
        Some(EVAL_IMAGE) => {
            let encoded = payload
                .get("image")
                .and_then(Value::as_str)
                .ok_or_else(|| EvalError::Scenario("eval-image requires image".into()))?;
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|e| EvalError::Scenario(format!("eval-image base64: {e}")))?;
            let format = payload
                .get("format")
                .and_then(Value::as_str)
                .unwrap_or("image/jpeg")
                .to_owned();
            Ok(EvalControl::Image { bytes, format })
        }
        _ => Ok(EvalControl::Rtvi(message)),
    }
}
pub fn message(id: u64, kind: &str, data: Value) -> Envelope {
    Envelope {
        label: MESSAGE_LABEL.into(),
        message_type: kind.into(),
        id: Some(id.to_string()),
        data: Some(data),
    }
}
pub fn control(id: u64, kind: &str, data: Value) -> Envelope {
    message(id, "client-message", json!({"t":kind,"d":data}))
}
