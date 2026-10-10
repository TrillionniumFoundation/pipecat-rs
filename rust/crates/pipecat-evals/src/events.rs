//! RTVI-to-friendly-event mapping and stale interrupted-response suppression.
use crate::EvalError;
use pipecat_protocol::rtvi::Envelope;
use serde_json::{json, Value};
use std::collections::{HashMap, VecDeque};
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct EventLimits {
    pub max_events: usize,
    pub max_event_bytes: usize,
    pub max_text_bytes: usize,
}
impl Default for EventLimits {
    fn default() -> Self {
        Self {
            max_events: 16_384,
            max_event_bytes: 16 * 1024 * 1024,
            max_text_bytes: 1024 * 1024,
        }
    }
}
pub struct EventStream {
    pub events_seen: Vec<Value>,
    pub latest_event_times: HashMap<String, Instant>,
    pub bot_speaking: bool,
    queue: VecDeque<Value>,
    text: String,
    started_at: Option<f64>,
    awaiting_reply: bool,
    started: Instant,
    bytes_seen: usize,
    limits: EventLimits,
    epoch: u64,
}
impl EventStream {
    pub fn new(limits: EventLimits) -> Self {
        Self {
            events_seen: vec![],
            latest_event_times: HashMap::new(),
            bot_speaking: false,
            queue: VecDeque::new(),
            text: String::new(),
            started_at: None,
            awaiting_reply: false,
            started: Instant::now(),
            bytes_seen: 0,
            limits,
            epoch: 0,
        }
    }
    pub fn pop(&mut self) -> Option<Value> {
        self.queue.pop_front()
    }
    pub fn drop_pending_bot_output(&mut self) {
        self.epoch += 1;
        self.text.clear();
        self.started_at = None;
        self.queue
            .retain(|event| event["type"] == "user_transcription");
    }
    pub fn turn_boundary(&mut self) {
        self.drop_pending_bot_output();
        self.awaiting_reply = true;
    }
    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }
    pub(crate) fn pending_responses(&self) -> Vec<Value> {
        self.queue
            .iter()
            .filter(|event| event["type"] == "llm_response")
            .cloned()
            .collect()
    }
    pub fn ingest(&mut self, envelope: &Envelope) -> Result<(), EvalError> {
        let payload = envelope.data.as_ref().unwrap_or(&Value::Null);
        if !payload.is_null() && !payload.is_object() {
            return Err(EvalError::Scenario(
                "RTVI data must be a mapping or null".into(),
            ));
        }
        let event = match envelope.message_type.as_str() {
            "bot-llm-started" => {
                self.awaiting_reply = false;
                self.text.clear();
                self.started_at = None;
                Some(json!({"type":"llm_started"}))
            }
            "bot-llm-text" => {
                if !self.awaiting_reply {
                    let text = string(payload, "text")?;
                    if self.text.len().saturating_add(text.len()) > self.limits.max_text_bytes {
                        return Err(EvalError::Limit("LLM response text"));
                    }
                    if self.started_at.is_none() {
                        self.started_at = Some(self.started.elapsed().as_secs_f64());
                    }
                    self.text.push_str(text);
                }
                None
            }
            "bot-llm-stopped" => {
                if self.awaiting_reply {
                    self.text.clear();
                    None
                } else {
                    let mut event =
                        json!({"type":"llm_response","text":std::mem::take(&mut self.text)});
                    if let Some(at) = self.started_at.take() {
                        event["started_at"] = at.into();
                    }
                    Some(event)
                }
            }
            "bot-llm-marker" if !self.awaiting_reply => {
                let markers = payload.get("markers").cloned().unwrap_or_else(|| json!([]));
                let values = markers
                    .as_array()
                    .ok_or_else(|| EvalError::Scenario("markers must be a list".into()))?;
                if values.len() > 64
                    || values
                        .iter()
                        .any(|v| v.as_str().is_some_and(|s| s.len() > 256))
                {
                    return Err(EvalError::Limit("marker alphabet"));
                }
                if values.iter().any(|v| v.as_str().is_none_or(str::is_empty)) {
                    return Err(EvalError::Scenario(
                        "markers must be nonempty strings".into(),
                    ));
                }
                Some(
                    json!({"type":"llm_marker","text":string(payload,"text")?,"kind":payload.get("kind"),"raw":string(payload,"raw")?,"markers":markers}),
                )
            }
            "bot-started-speaking" => {
                self.bot_speaking = true;
                Some(json!({"type":"bot_started_speaking"}))
            }
            "bot-stopped-speaking" => {
                self.bot_speaking = false;
                Some(json!({"type":"bot_stopped_speaking"}))
            }
            "user-started-speaking" | "bot-interrupted" => {
                self.drop_pending_bot_output();
                self.awaiting_reply = true;
                self.bot_speaking = false;
                Some(json!({"type":envelope.message_type.replace('-',"_")}))
            }
            "user-stopped-speaking" | "vad-user-started-speaking" | "vad-user-stopped-speaking" => {
                Some(json!({"type":envelope.message_type.replace('-',"_")}))
            }
            "user-transcription" => {
                if payload.get("final").is_some_and(|v| v != true) {
                    None
                } else {
                    Some(json!({"type":"user_transcription","transcript":string(payload,"text")?}))
                }
            }
            "llm-function-call-in-progress" => {
                let args = payload
                    .get("arguments")
                    .filter(|v| !v.is_null())
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                if !args.is_object() {
                    return Err(EvalError::Scenario(
                        "function arguments must be a mapping".into(),
                    ));
                }
                Some(
                    json!({"type":"function_call","name":payload.get("function_name"),"args":args}),
                )
            }
            "llm-function-call-stopped" => Some(
                json!({"type":"function_call_stopped","name":payload.get("function_name"),"args":{"tool_call_id":string(payload,"tool_call_id")?,"cancelled":payload.get("cancelled").and_then(Value::as_bool).unwrap_or(false)}}),
            ),
            "eval-bot-image" => Some(
                json!({"type":"image","width":payload.get("width"),"height":payload.get("height"),"format":payload.get("format")}),
            ),
            "eval-bot-audio" => {
                return Err(EvalError::Unsupported(
                    "bot audio received in text-only evaluation".into(),
                ))
            }
            "error" | "error-response" => {
                return Err(EvalError::Scenario(format!(
                    "bot reported error: {payload}"
                )))
            }
            _ => None,
        };
        if let Some(event) = event {
            self.append(event)?;
        }
        Ok(())
    }
    fn append(&mut self, mut event: Value) -> Result<(), EvalError> {
        event["at"] = self.started.elapsed().as_secs_f64().into();
        let bytes = serde_json::to_vec(&event)?.len();
        if self.events_seen.len() >= self.limits.max_events
            || bytes > self.limits.max_event_bytes.saturating_sub(self.bytes_seen)
        {
            return Err(EvalError::Limit("event history"));
        }
        self.bytes_seen += bytes;
        self.latest_event_times
            .insert(event["type"].as_str().unwrap().to_owned(), Instant::now());
        self.events_seen.push(event.clone());
        self.queue.push_back(event);
        Ok(())
    }
}
fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, EvalError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(""),
        Some(Value::String(value)) => Ok(value),
        _ => Err(EvalError::Scenario(format!("RTVI {key} must be a string"))),
    }
}
