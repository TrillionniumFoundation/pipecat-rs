// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{FluxModel, StreamingError};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TurnEvent {
    StartOfTurn,
    TurnResumed,
    EndOfTurn,
    EagerEndOfTurn,
    Update,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Word {
    #[serde(default)]
    pub word: String,
    #[serde(default)]
    pub confidence: Option<f64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FluxTurn {
    pub event: TurnEvent,
    pub turn_index: u64,
    #[serde(default)]
    pub transcript: String,
    #[serde(default, deserialize_with = "null_default")]
    pub words: Vec<Word>,
    #[serde(default, deserialize_with = "null_default")]
    pub languages: Vec<String>,
    /// Bounded original response, retaining provider timings and word metadata.
    #[serde(skip)]
    pub result: Value,
}
impl FluxTurn {
    pub fn average_confidence(&self) -> Option<f64> {
        let mut sum = 0.0;
        let mut count = 0;
        for value in self.words.iter().filter_map(|w| w.confidence) {
            sum += value;
            count += 1;
        }
        (count > 0).then(|| sum / f64::from(count))
    }
    pub fn accepts(&self, minimum: Option<f64>) -> bool {
        minimum.is_none_or(|v| {
            v == 0.0
                || self
                    .average_confidence()
                    .is_some_and(|confidence| confidence > v)
        })
    }
    pub fn language(&self, model: FluxModel) -> Option<String> {
        match self.languages.first() {
            Some(code) => matches!(
                code.as_str(),
                "de" | "en" | "es" | "fr" | "hi" | "it" | "ja" | "nl" | "pt" | "ru"
            )
            .then(|| code.clone()),
            None => (model == FluxModel::English).then(|| "en".into()),
        }
    }
}
pub(crate) enum Incoming {
    Connected,
    Turn(FluxTurn),
    ConfigureSuccess,
    ConfigureFailure,
    Unknown,
}
pub(crate) fn parse(text: &str, max_bytes: usize) -> Result<Incoming, StreamingError> {
    if text.len() > max_bytes {
        return Err(StreamingError::TooLarge);
    }
    let value: Value = serde_json::from_str(text).map_err(|_| StreamingError::Protocol("JSON"))?;
    let kind = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or(StreamingError::Protocol("message type"))?;
    match kind {
        "Connected" => Ok(Incoming::Connected),
        "Error" => Err(
            if value.get("code").and_then(Value::as_str) == Some("UNPARSABLE_CLIENT_MESSAGE") {
                StreamingError::Invalid("provider rejected client message")
            } else {
                StreamingError::Provider { status: None }
            },
        ),
        "ConfigureSuccess" => Ok(Incoming::ConfigureSuccess),
        "ConfigureFailure" => Ok(Incoming::ConfigureFailure),
        "TurnInfo" => {
            let event = value
                .get("event")
                .and_then(Value::as_str)
                .ok_or(StreamingError::Protocol("TurnInfo event"))?;
            if !matches!(
                event,
                "StartOfTurn" | "TurnResumed" | "EndOfTurn" | "EagerEndOfTurn" | "Update"
            ) {
                return Ok(Incoming::Unknown);
            }
            let mut turn: FluxTurn = serde_json::from_value(value.clone())
                .map_err(|_| StreamingError::Protocol("TurnInfo fields"))?;
            if turn.transcript.len() > 64 * 1024
                || turn.words.len() > 4096
                || turn.languages.len() > 16
                || turn.words.iter().any(|w| w.word.len() > 4096)
                || turn.languages.iter().any(|v| v.len() > 32)
            {
                return Err(StreamingError::TooLarge);
            }
            if turn
                .words
                .iter()
                .filter_map(|w| w.confidence)
                .any(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
            {
                return Err(StreamingError::Protocol("word confidence"));
            }
            turn.result = value;
            Ok(Incoming::Turn(turn))
        }
        _ => Ok(Incoming::Unknown),
    }
}

fn null_default<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}
