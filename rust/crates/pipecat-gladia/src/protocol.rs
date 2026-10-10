// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::GladiaError;
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use tokio::sync::OwnedSemaphorePermit;
#[derive(Debug, Clone, PartialEq)]
pub struct GladiaTranscript {
    pub text: String,
    pub language: Option<String>,
    pub is_final: bool,
    pub result: Value,
    /// Successfully written PCM since the preceding final; no keepalive PCM.
    pub audio_seconds: f64,
}
#[derive(Debug, Clone, PartialEq)]
pub struct GladiaTranslation {
    pub text: String,
    pub language: String,
    pub original_language: String,
    pub result: Value,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GladiaAcknowledgment {
    pub generation: u64,
    pub submitted_bytes: u64,
    /// Contiguous acknowledged prefix; exclusive offset, aligned to PCM frames.
    pub acknowledged_bytes: u64,
    pub buffered_bytes: usize,
}
#[derive(Debug, Clone, PartialEq)]
pub enum GladiaSttEvent {
    Transcript(GladiaTranscript),
    Translation(GladiaTranslation),
    TurnStarted,
    TurnStopped,
    Acknowledgment(GladiaAcknowledgment),
    /// Malformed/addon/audio rejection. No untrusted provider text in diagnostics.
    Error(GladiaError),
    /// Bounded observation of unimplemented addon/lifecycle message types; payload discarded.
    Observed {
        kind: String,
    },
}
pub(crate) struct Accounting {
    pub generation: u64,
    pub submitted: u64,
    pub acknowledged: u64,
    pub buffered: usize,
    ranges: BTreeMap<u64, u64>,
    pending: VecDeque<(u64, OwnedSemaphorePermit)>,
    alignment: u64,
}
impl Accounting {
    pub fn new(generation: u64, channels: u16) -> Self {
        Self {
            generation,
            submitted: 0,
            acknowledged: 0,
            buffered: 0,
            ranges: BTreeMap::new(),
            pending: VecDeque::new(),
            alignment: u64::from(channels) * 2,
        }
    }
    pub fn submit(
        &mut self,
        bytes: usize,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), GladiaError> {
        if self.pending.len() >= 4096 {
            return Err(GladiaError::Backpressure);
        }
        self.submitted = self
            .submitted
            .checked_add(bytes as u64)
            .ok_or(GladiaError::TooLarge)?;
        self.buffered += bytes;
        self.pending.push_back((self.submitted, permit));
        Ok(())
    }
    /// Wire ranges have inclusive ends (official example: 0..4095 is 4096 bytes).
    /// Overlap, duplicate and delayed ranges merge; gaps never retire the prefix.
    pub fn acknowledge(
        &mut self,
        start: u64,
        end_inclusive: u64,
    ) -> Result<GladiaAcknowledgment, GladiaError> {
        let end = end_inclusive
            .checked_add(1)
            .ok_or(GladiaError::Protocol("ack range overflow"))?;
        if start >= end
            || end > self.submitted
            || !start.is_multiple_of(self.alignment)
            || !end.is_multiple_of(self.alignment)
        {
            return Err(GladiaError::Protocol(
                "ack range outside aligned submitted audio",
            ));
        }
        if end > self.acknowledged {
            let mut ranges = self.ranges.clone();
            let mut start = start.max(self.acknowledged);
            let mut end = end;
            let overlaps: Vec<_> = ranges
                .range(..=end)
                .filter_map(|(&a, &b)| if b >= start { Some((a, b)) } else { None })
                .collect();
            for (a, b) in overlaps {
                start = start.min(a);
                end = end.max(b);
                ranges.remove(&a);
            }
            ranges.insert(start, end);
            if ranges.len() > 128 {
                return Err(GladiaError::TooLarge);
            }
            self.ranges = ranges;
            while let Some((&a, &b)) = self.ranges.first_key_value() {
                if a > self.acknowledged {
                    break;
                }
                self.acknowledged = self.acknowledged.max(b);
                self.ranges.pop_first();
            }
            while self
                .pending
                .front()
                .is_some_and(|(end, _)| *end <= self.acknowledged)
            {
                let (_, permit) = self.pending.pop_front().unwrap();
                self.buffered -= permit.num_permits();
                drop(permit);
            }
            // A partially acknowledged chunk retains its whole admission budget.
        }
        Ok(self.snapshot())
    }
    pub fn snapshot(&self) -> GladiaAcknowledgment {
        GladiaAcknowledgment {
            generation: self.generation,
            submitted_bytes: self.submitted,
            acknowledged_bytes: self.acknowledged,
            buffered_bytes: self.buffered,
        }
    }
}
pub(crate) struct Protocol {
    pub accounting: Accounting,
    pub speaking: bool,
    pub usage_bytes: u64,
    pub session_id: String,
    pub vad: bool,
    pub bytes_per_second: f64,
}
impl Protocol {
    pub fn message(&mut self, value: Value) -> Result<Option<GladiaSttEvent>, GladiaError> {
        if value
            .get("session_id")
            .is_some_and(|v| v.as_str() != Some(self.session_id.as_str()))
        {
            return Err(GladiaError::Protocol("stale or invalid session identity"));
        }
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or(GladiaError::Protocol("message type"))?;
        if kind.len() > 128 {
            return Err(GladiaError::TooLarge);
        }
        match kind {
            "audio_chunk" => {
                if value.get("acknowledged").and_then(Value::as_bool) != Some(true) {
                    return Err(GladiaError::Provider("audio chunk"));
                }
                let range = value
                    .pointer("/data/byte_range")
                    .and_then(Value::as_array)
                    .filter(|v| v.len() == 2)
                    .ok_or(GladiaError::Protocol("ack byte range"))?;
                let start = range[0]
                    .as_u64()
                    .ok_or(GladiaError::Protocol("ack start"))?;
                let end = range[1].as_u64().ok_or(GladiaError::Protocol("ack end"))?;
                Ok(Some(GladiaSttEvent::Acknowledgment(
                    self.accounting.acknowledge(start, end)?,
                )))
            }
            "transcript" => {
                let data = value
                    .get("data")
                    .ok_or(GladiaError::Protocol("transcript data"))?;
                let text = data
                    .pointer("/utterance/text")
                    .and_then(Value::as_str)
                    .ok_or(GladiaError::Protocol("transcript text"))?
                    .to_owned();
                let language = language(data.pointer("/utterance/language"))?;
                let is_final = data
                    .get("is_final")
                    .and_then(Value::as_bool)
                    .ok_or(GladiaError::Protocol("transcript finality"))?;
                let audio_seconds = if is_final {
                    std::mem::take(&mut self.usage_bytes) as f64 / self.bytes_per_second
                } else {
                    0.0
                };
                Ok(Some(GladiaSttEvent::Transcript(GladiaTranscript {
                    text,
                    language,
                    is_final,
                    result: value,
                    audio_seconds,
                })))
            }
            "translation" => {
                if value.get("error").is_some_and(|v| !v.is_null())
                    || value.get("data").is_none_or(Value::is_null)
                {
                    return Err(GladiaError::Provider("translation addon"));
                }
                let original = language(value.pointer("/data/original_language"))?
                    .ok_or(GladiaError::Protocol("original language"))?;
                let language = language(value.pointer("/data/translated_utterance/language"))?
                    .ok_or(GladiaError::Protocol("translation language"))?;
                let text = value
                    .pointer("/data/translated_utterance/text")
                    .and_then(Value::as_str)
                    .ok_or(GladiaError::Protocol("translation text"))?
                    .to_owned();
                if original == language {
                    return Ok(None);
                }
                Ok(Some(GladiaSttEvent::Translation(GladiaTranslation {
                    text,
                    language,
                    original_language: original,
                    result: value,
                })))
            }
            "speech_start" if self.vad && !self.speaking => {
                self.speaking = true;
                Ok(Some(GladiaSttEvent::TurnStarted))
            }
            "speech_end" if self.vad && self.speaking => {
                self.speaking = false;
                Ok(Some(GladiaSttEvent::TurnStopped))
            }
            "speech_start" | "speech_end" => Ok(None),
            "error" => Err(GladiaError::Provider("session")),
            _ => Ok(Some(GladiaSttEvent::Observed {
                kind: kind.to_owned(),
            })),
        }
    }
}
fn language(value: Option<&Value>) -> Result<Option<String>, GladiaError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(v)) if v.len() <= 64 => Ok(Some(v.clone())),
        _ => Err(GladiaError::Protocol("language")),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::sync::Semaphore;
    fn submitted() -> Accounting {
        let mut a = Accounting::new(7, 1);
        let semaphore = Arc::new(Semaphore::new(8192));
        for _ in 0..2 {
            a.submit(
                4096,
                semaphore.clone().try_acquire_many_owned(4096).unwrap(),
            )
            .unwrap()
        }
        a
    }
    #[test]
    fn inclusive_partial_duplicate_overlap_gap_and_delayed_ack() {
        let mut a = submitted();
        assert_eq!(a.acknowledge(4096, 8191).unwrap().acknowledged_bytes, 0);
        assert_eq!(a.acknowledge(0, 2047).unwrap().acknowledged_bytes, 2048);
        assert_eq!(a.snapshot().buffered_bytes, 8192);
        assert_eq!(a.acknowledge(1024, 4095).unwrap().acknowledged_bytes, 8192);
        assert_eq!(a.snapshot().buffered_bytes, 0);
        assert_eq!(a.acknowledge(0, 4095).unwrap().acknowledged_bytes, 8192);
        assert_eq!(a.acknowledge(0, 2047).unwrap().acknowledged_bytes, 8192);
    }
    #[test]
    fn invalid_ack_cannot_mutate_or_split_frames() {
        let mut a = submitted();
        let before = a.snapshot();
        for (start, end) in [(0, 8192), (5, 7), (0, 6), (10, 9), (0, u64::MAX)] {
            assert!(a.acknowledge(start, end).is_err());
            assert_eq!(a.snapshot(), before)
        }
        let mut fresh = Accounting::new(8, 1);
        assert!(fresh.acknowledge(0, 4095).is_err());
        assert_eq!(fresh.snapshot().generation, 8);
    }
}
