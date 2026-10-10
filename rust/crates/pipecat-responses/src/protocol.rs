// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{ReasoningItem, ServiceError, ToolCall, Usage};
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseStatus {
    Completed,
    Failed,
    Incomplete,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ResponseEvent {
    Started,
    ResponseCreated {
        id: String,
    },
    TextDelta(String),
    ThoughtStart,
    ThoughtDelta(String),
    ThoughtEnd,
    ReasoningItem(ReasoningItem),
    ToolCallDelta {
        item_id: String,
        call_id: String,
        name: String,
        arguments: String,
    },
    /// Emitted only after arguments.done or output_item.done supplies valid JSON.
    /// Returned calls are never executed by this crate.
    ToolCall(ToolCall),
    Usage(Usage),
    Finished {
        status: ResponseStatus,
        response_id: Option<String>,
        model: Option<String>,
    },
}
#[derive(Default)]
struct Call {
    id: String,
    name: String,
    arguments: String,
    done: bool,
}
#[derive(Default)]
struct Refusal {
    text: String,
    done: bool,
}

pub(crate) struct Decoder {
    pub id: Option<String>,
    pub model: Option<String>,
    pub status: Option<ResponseStatus>,
    pub output: Vec<Value>,
    pub output_complete: bool,
    pub output_seen: bool,
    pub retry_code: Option<&'static str>,
    pub error: Option<ServiceError>,
    calls: BTreeMap<String, Call>,
    call_ids: HashSet<String>,
    reasoning_items: BTreeMap<String, ReasoningItem>,
    refusals: BTreeMap<(String, u64), Refusal>,
    thought_open: bool,
    bytes: usize,
    max_bytes: usize,
}
impl Decoder {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            id: None,
            model: None,
            status: None,
            output: vec![],
            output_complete: false,
            output_seen: false,
            retry_code: None,
            error: None,
            calls: BTreeMap::new(),
            call_ids: HashSet::new(),
            reasoning_items: BTreeMap::new(),
            refusals: BTreeMap::new(),
            thought_open: false,
            bytes: 0,
            max_bytes,
        }
    }
    pub fn terminal(&self) -> bool {
        self.status.is_some() || self.error.is_some()
    }
    pub fn feed(&mut self, value: Value) -> Result<Vec<ResponseEvent>, ServiceError> {
        if self.terminal() {
            return Err(protocol("event after terminal response"));
        }
        self.bytes = self
            .bytes
            .checked_add(value.to_string().len())
            .ok_or_else(|| protocol("response size limit"))?;
        if self.bytes > self.max_bytes {
            return Err(protocol("response size limit"));
        }
        let kind = required_str(&value, "type")?;
        let mut out = vec![];
        match kind {
            "response.created" => {
                let id = required_str(&value["response"], "id")?.to_owned();
                if self.id.as_ref().is_some_and(|old| old != &id) {
                    return Err(protocol("response id changed"));
                }
                self.id = Some(id.clone());
                out.push(ResponseEvent::ResponseCreated { id });
            }
            "response.output_text.delta" => {
                self.output_seen = true;
                out.push(ResponseEvent::TextDelta(
                    required_str_allow_empty(&value, "delta")?.into(),
                ));
            }
            "response.refusal.delta" => {
                self.output_seen = true;
                let key = refusal_key(&value)?;
                let delta = required_str_allow_empty(&value, "delta")?;
                let refusal = self.refusals.entry(key).or_default();
                if refusal.done {
                    return Err(protocol("refusal delta after completion"));
                }
                refusal.text.push_str(delta);
                out.push(ResponseEvent::TextDelta(delta.into()));
            }
            "response.refusal.done" => {
                self.output_seen = true;
                self.complete_refusal(
                    refusal_key(&value)?,
                    required_str_allow_empty(&value, "refusal")?,
                    &mut out,
                )?;
            }
            "response.content_part.done" if value["part"]["type"] == "refusal" => {
                self.output_seen = true;
                self.complete_refusal(
                    refusal_key(&value)?,
                    required_str_allow_empty(&value["part"], "refusal")?,
                    &mut out,
                )?;
            }
            "response.reasoning_summary_text.delta" => {
                self.output_seen = true;
                if !self.thought_open {
                    self.thought_open = true;
                    out.push(ResponseEvent::ThoughtStart);
                }
                out.push(ResponseEvent::ThoughtDelta(
                    required_str_allow_empty(&value, "delta")?.into(),
                ));
            }
            "response.output_item.added" => {
                self.output_seen = true;
                let item = &value["item"];
                match required_str(item, "type")? {
                    "function_call" => {
                        let item_id = required_str(item, "id")?.to_owned();
                        let call_id = required_str(item, "call_id")?.to_owned();
                        let name = required_str(item, "name")?.to_owned();
                        if self.calls.contains_key(&item_id)
                            || !self.call_ids.insert(call_id.clone())
                        {
                            return Err(protocol("duplicate function call"));
                        }
                        let arguments = item["arguments"].as_str().unwrap_or("").to_owned();
                        self.calls.insert(
                            item_id.clone(),
                            Call {
                                id: call_id.clone(),
                                name: name.clone(),
                                arguments: arguments.clone(),
                                done: false,
                            },
                        );
                        out.push(ResponseEvent::ToolCallDelta {
                            item_id,
                            call_id,
                            name,
                            arguments,
                        });
                    }
                    "message" | "reasoning" => {}
                    _ => {
                        return Err(ServiceError::Unsupported(
                            "Responses hosted output item".into(),
                        ))
                    }
                }
            }
            "response.function_call_arguments.delta" => {
                self.output_seen = true;
                let item_id = required_str(&value, "item_id")?.to_owned();
                let delta = required_str_allow_empty(&value, "delta")?;
                let call = self
                    .calls
                    .get_mut(&item_id)
                    .ok_or_else(|| protocol("arguments for unknown function call"))?;
                if call.done {
                    return Err(protocol("arguments after completed function call"));
                }
                call.arguments.push_str(delta);
                out.push(ResponseEvent::ToolCallDelta {
                    item_id,
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: delta.into(),
                });
            }
            "response.function_call_arguments.done" => {
                let item_id = required_str(&value, "item_id")?;
                self.complete_call(
                    item_id,
                    required_str_allow_empty(&value, "arguments")?,
                    &mut out,
                )?;
            }
            "response.output_item.done" => {
                self.output_seen = true;
                self.output_item(&value["item"], &mut out)?;
            }
            "response.completed" | "response.failed" | "response.incomplete" => {
                let response = value
                    .get("response")
                    .filter(|v| v.is_object())
                    .ok_or_else(|| protocol("terminal response object missing"))?;
                if let Some(id) = response["id"].as_str() {
                    if self.id.as_deref().is_some_and(|old| old != id) {
                        return Err(protocol("terminal response id mismatch"));
                    }
                    self.id = Some(id.into());
                }
                self.model = response["model"].as_str().map(str::to_owned);
                let status = match kind {
                    "response.completed" => ResponseStatus::Completed,
                    "response.failed" => ResponseStatus::Failed,
                    _ => ResponseStatus::Incomplete,
                };
                if let Some(output) = response["output"].as_array() {
                    self.output = output.clone();
                    self.output_complete = true;
                    if status == ResponseStatus::Completed {
                        for item in output {
                            self.output_item(item, &mut out)?;
                        }
                    }
                }
                if self.thought_open {
                    self.thought_open = false;
                    out.push(ResponseEvent::ThoughtEnd);
                }
                if let Some(usage) = response.get("usage").filter(|v| v.is_object()) {
                    out.push(ResponseEvent::Usage(usage_from(usage)));
                }
                if status != ResponseStatus::Completed {
                    self.error = Some(provider_error(if status == ResponseStatus::Failed {
                        "response_failed"
                    } else {
                        "response_incomplete"
                    }));
                } else if self.calls.values().any(|call| !call.done) {
                    return Err(protocol(
                        "completed response contains unfinished function call",
                    ));
                }
                self.status = Some(status);
            }
            "error" => {
                let code = value["error"]["code"]
                    .as_str()
                    .or_else(|| value["code"].as_str())
                    .unwrap_or("");
                if !self.output_seen && self.id.is_none() && value["error"].is_object() {
                    self.retry_code = match value["error"]["code"].as_str().unwrap_or("") {
                        "previous_response_not_found" => Some("previous_response_not_found"),
                        "websocket_connection_limit_reached" => {
                            Some("websocket_connection_limit_reached")
                        }
                        _ => None,
                    };
                }
                self.error = Some(provider_error(code));
                self.status = Some(ResponseStatus::Failed);
            }
            "response.output_text.done"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.in_progress"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.done" => {}
            _ => {
                return Err(ServiceError::Unsupported(
                    "unrecognized Responses event".into(),
                ))
            }
        }
        Ok(out)
    }
    fn complete_refusal(
        &mut self,
        key: (String, u64),
        text: &str,
        out: &mut Vec<ResponseEvent>,
    ) -> Result<(), ServiceError> {
        let refusal = self.refusals.entry(key).or_default();
        if refusal.done {
            if refusal.text != text {
                return Err(protocol("completed refusal changed"));
            }
            return Ok(());
        }
        let suffix = text
            .strip_prefix(&refusal.text)
            .ok_or_else(|| protocol("refusal final text conflicts with deltas"))?;
        if !suffix.is_empty() {
            out.push(ResponseEvent::TextDelta(suffix.into()));
        }
        refusal.text = text.into();
        refusal.done = true;
        Ok(())
    }
    fn complete_call(
        &mut self,
        item_id: &str,
        arguments: &str,
        out: &mut Vec<ResponseEvent>,
    ) -> Result<(), ServiceError> {
        let call = self
            .calls
            .get_mut(item_id)
            .ok_or_else(|| protocol("completion for unknown function call"))?;
        if call.done {
            if call.arguments != arguments {
                return Err(protocol("completed function arguments changed"));
            }
            return Ok(());
        }
        let arguments_value: Value = serde_json::from_str(arguments)
            .map_err(|_| protocol("invalid function arguments JSON"))?;
        if !arguments_value.is_object() {
            return Err(protocol("function arguments must be an object"));
        }
        call.arguments = arguments.into();
        call.done = true;
        out.push(ResponseEvent::ToolCall(ToolCall {
            id: call.id.clone(),
            name: call.name.clone(),
            arguments: arguments_value,
        }));
        Ok(())
    }
    fn output_item(
        &mut self,
        item: &Value,
        out: &mut Vec<ResponseEvent>,
    ) -> Result<(), ServiceError> {
        match required_str(item, "type")? {
            "function_call" => {
                let item_id = required_str(item, "id")?;
                let call = self
                    .calls
                    .get(item_id)
                    .ok_or_else(|| protocol("completed unannounced function call"))?;
                if item["call_id"].as_str() != Some(call.id.as_str())
                    || item["name"].as_str() != Some(call.name.as_str())
                {
                    return Err(protocol("function call identity changed"));
                }
                self.complete_call(item_id, required_str_allow_empty(item, "arguments")?, out)?;
            }
            "reasoning" => {
                if self.thought_open {
                    self.thought_open = false;
                    out.push(ResponseEvent::ThoughtEnd);
                }
                let id = required_str(item, "id")?.to_owned();
                let summary = match &item["summary"] {
                    Value::Null => vec![],
                    Value::Array(summary) => summary
                        .iter()
                        .map(|part| {
                            if part["type"] != "summary_text" {
                                return Err(protocol("invalid reasoning summary part"));
                            }
                            required_str_allow_empty(part, "text").map(str::to_owned)
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => return Err(protocol("invalid reasoning summary")),
                };
                let encrypted_content = match &item["encrypted_content"] {
                    Value::Null => None,
                    Value::String(value) => (!value.is_empty()).then(|| value.clone()),
                    _ => return Err(protocol("invalid encrypted reasoning payload")),
                };
                let reasoning = ReasoningItem {
                    id: id.clone(),
                    summary,
                    encrypted_content,
                };
                if let Some(previous) = self.reasoning_items.get(&id) {
                    if previous != &reasoning {
                        return Err(protocol("completed reasoning item changed"));
                    }
                } else {
                    self.reasoning_items.insert(id, reasoning.clone());
                    if reasoning.encrypted_content.is_some() {
                        out.push(ResponseEvent::ReasoningItem(reasoning));
                    }
                }
            }
            "message" => {
                let parts = item["content"]
                    .as_array()
                    .ok_or_else(|| protocol("invalid completed message content"))?;
                for (index, part) in parts.iter().enumerate() {
                    match part["type"].as_str() {
                        Some("output_text") => {
                            required_str_allow_empty(part, "text")?;
                        }
                        Some("refusal") => {
                            let key = (required_str(item, "id")?.to_owned(), index as u64);
                            self.complete_refusal(
                                key,
                                required_str_allow_empty(part, "refusal")?,
                                out,
                            )?;
                        }
                        _ => {
                            return Err(ServiceError::Unsupported(
                                "Responses non-text message output".into(),
                            ))
                        }
                    }
                }
            }
            _ => {
                return Err(ServiceError::Unsupported(
                    "Responses hosted output item".into(),
                ))
            }
        }
        Ok(())
    }
    pub fn finish_event(&self) -> ResponseEvent {
        ResponseEvent::Finished {
            status: self.status.unwrap_or(ResponseStatus::Failed),
            response_id: self.id.clone(),
            model: self.model.clone(),
        }
    }
}
fn required_str<'a>(value: &'a Value, key: &str) -> Result<&'a str, ServiceError> {
    let text = required_str_allow_empty(value, key)?;
    if text.is_empty() {
        Err(protocol("required event string is empty"))
    } else {
        Ok(text)
    }
}
fn required_str_allow_empty<'a>(value: &'a Value, key: &str) -> Result<&'a str, ServiceError> {
    value[key]
        .as_str()
        .ok_or_else(|| protocol("required event string missing"))
}
fn protocol(message: &str) -> ServiceError {
    ServiceError::Protocol(message.into())
}
pub(crate) fn provider_error(code: &str) -> ServiceError {
    let code = match code {
        "previous_response_not_found"
        | "websocket_connection_limit_reached"
        | "rate_limit_exceeded"
        | "invalid_api_key"
        | "insufficient_quota"
        | "server_error"
        | "response_failed"
        | "response_incomplete" => code,
        _ => "provider_error",
    };
    // Provider messages can echo credentials, prompt text, URLs and tool data.
    ServiceError::Provider {
        kind: code.into(),
        message: "Responses request failed; provider detail redacted".into(),
    }
}
fn usage_from(value: &Value) -> Usage {
    Usage {
        prompt_tokens: value["input_tokens"].as_u64().unwrap_or(0),
        completion_tokens: value["output_tokens"].as_u64().unwrap_or(0),
        total_tokens: value["total_tokens"].as_u64().unwrap_or(0),
        cache_read_input_tokens: Some(
            value["input_tokens_details"]["cached_tokens"]
                .as_u64()
                .unwrap_or(0),
        ),
        cache_creation_input_tokens: Some(
            value["input_tokens_details"]["cache_write_tokens"]
                .as_u64()
                .unwrap_or(0),
        ),
        reasoning_tokens: Some(
            value["output_tokens_details"]["reasoning_tokens"]
                .as_u64()
                .unwrap_or(0),
        ),
    }
}

fn refusal_key(value: &Value) -> Result<(String, u64), ServiceError> {
    Ok((
        required_str(value, "item_id")?.to_owned(),
        value["content_index"]
            .as_u64()
            .ok_or_else(|| protocol("refusal content index missing"))?,
    ))
}
