// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::StreamingError;
use serde_json::{Map, Value};

/// Provider-controlled descriptions are deliberately not retained in errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderErrorKind {
    InvalidMessage,
    InvalidModel,
    InvalidConfig,
    InvalidAudioType,
    InvalidOutputFormat,
    NotAuthorised,
    InsufficientFunds,
    NotAllowed,
    ProtocolError,
    QuotaExceeded,
    Other,
}
impl ProviderErrorKind {
    pub fn is_permanent(self) -> bool {
        self != Self::Other
    }
    fn parse(value: Option<&str>) -> Self {
        match value {
            Some("invalid_message") => Self::InvalidMessage,
            Some("invalid_model") => Self::InvalidModel,
            Some("invalid_config") => Self::InvalidConfig,
            Some("invalid_audio_type") => Self::InvalidAudioType,
            Some("invalid_output_format") => Self::InvalidOutputFormat,
            Some("not_authorised") => Self::NotAuthorised,
            Some("insufficient_funds") => Self::InsufficientFunds,
            Some("not_allowed") => Self::NotAllowed,
            Some("protocol_error") => Self::ProtocolError,
            Some("quota_exceeded") => Self::QuotaExceeded,
            _ => Self::Other,
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SpeechmaticsTranscript {
    pub text: String,
    pub speaker: Option<String>,
    pub language: String,
    pub start_time: f64,
    pub end_time: f64,
    /// AddSegment versus AddPartialSegment. Independent of `finalized`.
    pub is_final: bool,
    /// First nonempty AddSegment after a sent finalize; a heuristic, not a causal acknowledgment.
    pub finalized: bool,
    pub turn_generation: u64,
}
#[derive(Clone, PartialEq)]
pub struct SpeakersResult {
    pub message: Map<String, Value>,
}
impl std::fmt::Debug for SpeakersResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpeakersResult([redacted])")
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum SpeechmaticsSttEvent {
    Ready {
        session_id: Option<String>,
        request_id: String,
    },
    Transcript(SpeechmaticsTranscript),
    TurnStarted {
        time: f64,
    },
    TurnStopped {
        time: f64,
    },
    SpeechStarted {
        time: f64,
    },
    SpeechEnded {
        time: f64,
    },
    AudioSent {
        seconds: f64,
        sent_seq: u64,
        audio_bytes: u64,
    },
    AudioAdded {
        acked_seq: u64,
    },
    Speakers(SpeakersResult),
    /// Raw server messages are not copied into diagnostics.
    Warning,
    Info,
    LocaleFallback,
    EndOfTranscript,
}
pub(crate) enum WireEvent {
    Ready(Option<String>),
    AudioAdded(u64),
    Transcript {
        text: String,
        speaker: Option<String>,
        start: f64,
        end: f64,
        is_final: bool,
    },
    TurnStarted(f64),
    TurnStopped(f64),
    SpeechStarted(f64),
    SpeechEnded(f64),
    Speakers(SpeakersResult),
    Warning,
    Info,
    Error(ProviderErrorKind),
    End,
    Unknown,
}
pub(crate) fn parse(text: &str, max_transcript: usize) -> Result<WireEvent, StreamingError> {
    let value: Value = serde_json::from_str(text).map_err(|_| StreamingError::Protocol("JSON"))?;
    let obj = value
        .as_object()
        .ok_or(StreamingError::Protocol("message object"))?;
    let kind = obj
        .get("message")
        .and_then(Value::as_str)
        .ok_or(StreamingError::Protocol("message type"))?;
    Ok(match kind {
        "RecognitionStarted" => WireEvent::Ready(optional_string(obj, "id", 1024)?),
        "AudioAdded" => WireEvent::AudioAdded(
            obj.get("seq_no")
                .and_then(Value::as_u64)
                .ok_or(StreamingError::Protocol("audio sequence"))?,
        ),
        "AddSegment" | "AddPartialSegment" => {
            let segment = optional_object(obj, "segment")?;
            let metadata = optional_object(obj, "metadata")?;
            let text = match segment.and_then(|s| s.get("transcript")) {
                None => String::new(),
                Some(Value::String(s)) if s.len() <= max_transcript => s.clone(),
                Some(Value::String(_)) => return Err(StreamingError::TooLarge),
                _ => return Err(StreamingError::Protocol("transcript text")),
            };
            let speaker = match segment {
                Some(s) => optional_string(s, "speaker", 1024)?,
                None => None,
            };
            let start = time(metadata, "start_time")?;
            let end = time(metadata, "end_time")?;
            WireEvent::Transcript {
                text,
                speaker,
                start,
                end,
                is_final: kind == "AddSegment",
            }
        }
        "StartOfTurn" => WireEvent::TurnStarted(event_time(obj)?),
        "EndOfTurn" => WireEvent::TurnStopped(event_time(obj)?),
        "SpeechStarted" => WireEvent::SpeechStarted(event_time(obj)?),
        "SpeechEnded" => WireEvent::SpeechEnded(event_time(obj)?),
        "SpeakersResult" => {
            if let Some(v) = obj.get("speakers") {
                if !v.is_array() && !v.is_null() {
                    return Err(StreamingError::Protocol("speakers result"));
                }
            }
            WireEvent::Speakers(SpeakersResult {
                message: obj.clone(),
            })
        }
        "Warning" => WireEvent::Warning,
        "Info" => WireEvent::Info,
        "Error" => WireEvent::Error(ProviderErrorKind::parse(
            obj.get("type").and_then(Value::as_str),
        )),
        "EndOfTranscript" => WireEvent::End,
        _ => WireEvent::Unknown,
    })
}
fn optional_object<'a>(
    obj: &'a Map<String, Value>,
    key: &str,
) -> Result<Option<&'a Map<String, Value>>, StreamingError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(v)) => Ok(Some(v)),
        _ => Err(StreamingError::Protocol("optional object")),
    }
}
fn optional_string(
    obj: &Map<String, Value>,
    key: &str,
    max: usize,
) -> Result<Option<String>, StreamingError> {
    match obj.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(v)) if v.len() <= max => Ok(Some(v.clone())),
        Some(Value::String(_)) => Err(StreamingError::TooLarge),
        _ => Err(StreamingError::Protocol("optional text")),
    }
}
fn time(obj: Option<&Map<String, Value>>, key: &str) -> Result<f64, StreamingError> {
    match obj.and_then(|s| s.get(key)) {
        None => Ok(0.0),
        Some(v) => v
            .as_f64()
            .filter(|n| n.is_finite() && *n >= 0.0)
            .ok_or(StreamingError::Protocol("event time")),
    }
}
fn event_time(obj: &Map<String, Value>) -> Result<f64, StreamingError> {
    let metadata = optional_object(obj, "metadata")?;
    time(
        metadata,
        if metadata.is_some_and(|m| m.contains_key("start_time")) {
            "start_time"
        } else {
            "end_time"
        },
    )
}
