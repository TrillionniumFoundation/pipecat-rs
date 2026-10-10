// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{GradiumError, GradiumSttConfig, GradiumTurnSettings};
use serde_json::Value;
use std::time::Duration;
use tokio::time::Instant;
pub const TRANSCRIPT_AGGREGATION_DELAY: Duration = Duration::from_millis(100);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GradiumTurnPhase {
    Idle,
    Armed,
    Open,
    Ending,
}
#[derive(Clone, PartialEq)]
pub struct GradiumTranscript {
    pub text: String,
    pub language: Option<String>,
    pub is_final: bool,
    pub audio_seconds: f64,
}
impl std::fmt::Debug for GradiumTranscript {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GradiumTranscript")
            .field("is_final", &self.is_final)
            .field("bytes", &self.text.len())
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum GradiumSttEvent {
    Transcript(GradiumTranscript),
    TurnStarted,
    TurnStopped,
    /// A matching server acknowledgment covers only bytes sent before this flush.
    Flushed {
        flush_id: u64,
        confirmed_wire_bytes: u64,
    },
    EndOfStream {
        confirmed_wire_bytes: u64,
    },
}
pub(crate) struct Flush {
    pub id: u64,
    pub bytes: u64,
    pub deadline: Instant,
    pub final_at: Option<Instant>,
}
pub(crate) struct Protocol {
    pub phase: GradiumTurnPhase,
    pub pending: Option<Flush>,
    pub sent_bytes: u64,
    pub confirmed_bytes: u64,
    pub eos_sent: bool,
    counter: u64,
    text: String,
    cooldown: u32,
    last_step: Option<u64>,
    finalized_bytes: u64,
}
pub(crate) struct Outcome {
    pub events: Vec<GradiumSttEvent>,
    pub flush: bool,
    pub ended: bool,
}
impl Protocol {
    pub fn new() -> Self {
        Self {
            phase: GradiumTurnPhase::Idle,
            pending: None,
            sent_bytes: 0,
            confirmed_bytes: 0,
            eos_sent: false,
            counter: 0,
            text: String::new(),
            cooldown: 0,
            last_step: None,
            finalized_bytes: 0,
        }
    }
    pub fn start_flush(&mut self, c: &GradiumSttConfig) -> Result<u64, GradiumError> {
        if self.pending.is_some() {
            return Err(GradiumError::Busy);
        }
        self.counter = self.counter.checked_add(1).ok_or(GradiumError::TooLarge)?;
        self.pending = Some(Flush {
            id: self.counter,
            bytes: self.sent_bytes,
            deadline: Instant::now() + c.limits.flush_timeout,
            final_at: None,
        });
        Ok(self.counter)
    }
    pub fn message(
        &mut self,
        v: Value,
        c: &GradiumSttConfig,
        turn: &GradiumTurnSettings,
    ) -> Result<Outcome, GradiumError> {
        validate_json(&v, 0, &mut 0)?;
        let mut out = Outcome {
            events: Vec::new(),
            flush: false,
            ended: false,
        };
        if v.get("client_req_id").is_some() {
            return Err(GradiumError::Protocol("multiplexing unsupported"));
        }
        match v
            .get("type")
            .and_then(Value::as_str)
            .ok_or(GradiumError::Protocol("message type"))?
        {
            "text" => {
                single_stream(&v)?;
                let text = v
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(GradiumError::Protocol("text"))?;
                if text.is_empty() {
                    return Ok(out);
                }
                let length = self
                    .text
                    .len()
                    .checked_add(text.len())
                    .and_then(|n| n.checked_add(usize::from(!self.text.is_empty())))
                    .ok_or(GradiumError::TooLarge)?;
                if length > c.limits.max_text_bytes {
                    return Err(GradiumError::TooLarge);
                }
                if !self.text.is_empty() {
                    self.text.push(' ')
                }
                self.text.push_str(text);
                out.events
                    .push(GradiumSttEvent::Transcript(GradiumTranscript {
                        text: self.text.clone(),
                        language: c.settings.language.clone(),
                        is_final: false,
                        audio_seconds: 0.0,
                    }));
            }
            "flushed" => {
                let id = v
                    .get("flush_id")
                    .and_then(Value::as_str)
                    .ok_or(GradiumError::Protocol("flush identity"))?;
                let pending = self
                    .pending
                    .as_mut()
                    .ok_or(GradiumError::Protocol("unsolicited flush acknowledgment"))?;
                if pending.id.to_string() != id || pending.final_at.is_some() {
                    return Err(GradiumError::Protocol(
                        "mismatched or duplicate flush acknowledgment",
                    ));
                }
                pending.final_at = Some(Instant::now() + TRANSCRIPT_AGGREGATION_DELAY);
                self.confirmed_bytes = pending.bytes;
                self.cooldown = turn.post_flush_cooldown_frames;
                out.events.push(GradiumSttEvent::Flushed {
                    flush_id: pending.id,
                    confirmed_wire_bytes: pending.bytes,
                });
            }
            "step" => {
                let entries = v
                    .get("vad")
                    .and_then(Value::as_array)
                    .ok_or(GradiumError::Protocol("VAD entries"))?;
                if entries.len() > 64 {
                    return Err(GradiumError::TooLarge);
                }
                let mut closest = None;
                for entry in entries {
                    let horizon = entry
                        .get("horizon_s")
                        .and_then(Value::as_f64)
                        .filter(|n| n.is_finite() && (0.0..=60.0).contains(n))
                        .ok_or(GradiumError::Protocol("VAD horizon"))?;
                    let p = entry
                        .get("inactivity_prob")
                        .and_then(Value::as_f64)
                        .filter(|n| n.is_finite() && (0.0..=1.0).contains(n))
                        .ok_or(GradiumError::Protocol("VAD probability"))?;
                    let distance = (horizon - turn.eot_horizon_s).abs();
                    if closest.is_none_or(|(d, _)| distance < d) {
                        closest = Some((distance, p))
                    }
                }
                if let Some(index) = v.get("step_idx") {
                    let index = index.as_u64().ok_or(GradiumError::Protocol("step index"))?;
                    if self.last_step.is_some_and(|old| index <= old) {
                        return Err(GradiumError::Protocol("nonmonotonic step"));
                    }
                    self.last_step = Some(index);
                }
                if !c.enable_turn_detection || entries.is_empty() {
                    return Ok(out);
                }
                if self.cooldown > 0 {
                    self.cooldown -= 1;
                    return Ok(out);
                }
                let inactive = closest.expect("nonempty entries").1 >= turn.eot_threshold;
                match self.phase {
                    GradiumTurnPhase::Idle if inactive => self.phase = GradiumTurnPhase::Armed,
                    GradiumTurnPhase::Armed if !inactive => {
                        self.phase = GradiumTurnPhase::Open;
                        out.events.push(GradiumSttEvent::TurnStarted)
                    }
                    GradiumTurnPhase::Open if inactive => {
                        self.phase = GradiumTurnPhase::Ending;
                        out.events.push(GradiumSttEvent::TurnStopped);
                        out.flush = true
                    }
                    _ => (),
                }
            }
            "end_text" => {
                single_stream(&v)?;
                if let Some(n) = v.get("stop_s") {
                    if !n.as_f64().is_some_and(|n| n.is_finite() && n >= 0.0) {
                        return Err(GradiumError::Protocol("timestamp"));
                    }
                }
            }
            "end_of_stream" => {
                if !self.eos_sent {
                    return Err(GradiumError::Protocol("early end of stream"));
                }
                self.confirmed_bytes = self.sent_bytes;
                // EOS is terminal and cannot have trailing tokens. An outstanding
                // flush acknowledgment remains required before claiming success.
                if self.pending.as_ref().is_some_and(|p| p.final_at.is_none()) {
                    return Err(GradiumError::Protocol("EOS before flush acknowledgment"));
                }
                if self.phase == GradiumTurnPhase::Open {
                    out.events.push(GradiumSttEvent::TurnStopped)
                }
                if let Some(event) = self.finalize(c, self.sent_bytes) {
                    out.events.push(event)
                }
                self.pending = None;
                self.phase = GradiumTurnPhase::Idle;
                out.events.push(GradiumSttEvent::EndOfStream {
                    confirmed_wire_bytes: self.sent_bytes,
                });
                out.ended = true;
            }
            "error" => return Err(GradiumError::Provider),
            _ => return Err(GradiumError::Protocol("unexpected message type")),
        }
        Ok(out)
    }
    pub fn finalize_flush(&mut self, c: &GradiumSttConfig) -> Option<GradiumSttEvent> {
        let pending = self
            .pending
            .take()
            .expect("finalization requires a pending flush");
        let event = self.finalize(c, pending.bytes);
        if self.phase == GradiumTurnPhase::Ending {
            self.phase = GradiumTurnPhase::Armed
        }
        event
    }
    fn finalize(&mut self, c: &GradiumSttConfig, bytes: u64) -> Option<GradiumSttEvent> {
        let elapsed = bytes.saturating_sub(self.finalized_bytes) as f64
            / (f64::from(c.wire_sample_rate()) * 2.0);
        self.finalized_bytes = bytes;
        if self.text.is_empty() {
            return None;
        }
        Some(GradiumSttEvent::Transcript(GradiumTranscript {
            text: std::mem::take(&mut self.text),
            language: c.settings.language.clone(),
            is_final: true,
            audio_seconds: elapsed,
        }))
    }
}
fn single_stream(v: &Value) -> Result<(), GradiumError> {
    if v.get("stream_id").is_some_and(|n| n.as_u64() != Some(0)) {
        Err(GradiumError::Protocol("multiple text streams unsupported"))
    } else {
        Ok(())
    }
}
pub(crate) fn validate_json(
    v: &Value,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), GradiumError> {
    *nodes += 1;
    if depth > 16 || *nodes > 4096 {
        return Err(GradiumError::TooLarge);
    }
    match v {
        Value::Array(a) => {
            for child in a {
                validate_json(child, depth + 1, nodes)?
            }
        }
        Value::Object(o) => {
            if o.len() > 128 {
                return Err(GradiumError::TooLarge);
            }
            for child in o.values() {
                validate_json(child, depth + 1, nodes)?
            }
        }
        _ => (),
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn c() -> GradiumSttConfig {
        let mut c = GradiumSttConfig::new("fixture");
        c.enable_turn_detection = true;
        c
    }
    fn step(p: f64) -> Value {
        json!({"type":"step","vad":[{"horizon_s":3.0,"inactivity_prob":p}]})
    }
    #[tokio::test(start_paused = true)]
    async fn exact_delayed_finalization_empty_turn_and_no_reopen_while_ending() {
        let c = c();
        let mut p = Protocol::new();
        let turn = &c.settings.turn;
        p.message(step(0.0), &c, turn).unwrap();
        assert_eq!(p.phase, GradiumTurnPhase::Idle);
        p.message(step(0.5), &c, turn).unwrap();
        assert_eq!(p.phase, GradiumTurnPhase::Armed);
        assert_eq!(
            p.message(step(0.4), &c, turn).unwrap().events,
            vec![GradiumSttEvent::TurnStarted]
        );
        assert!(p.message(step(0.5), &c, turn).unwrap().flush);
        assert_eq!(p.start_flush(&c).unwrap(), 1);
        assert!(p.message(step(0.0), &c, turn).unwrap().events.is_empty());
        assert_eq!(p.phase, GradiumTurnPhase::Ending);
        p.message(json!({"type":"flushed","flush_id":"1"}), &c, turn)
            .unwrap();
        let at = p.pending.as_ref().unwrap().final_at.unwrap();
        tokio::time::advance(Duration::from_millis(99)).await;
        assert!(Instant::now() < at);
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(Instant::now(), at);
        assert!(p.finalize_flush(&c).is_none());
        assert_eq!(p.phase, GradiumTurnPhase::Armed);
        for _ in 0..8 {
            assert!(p.message(step(0.0), &c, turn).unwrap().events.is_empty())
        }
        assert_eq!(
            p.message(step(0.0), &c, turn).unwrap().events,
            vec![GradiumSttEvent::TurnStarted]
        );
    }
    #[tokio::test]
    async fn accumulated_text_has_hard_limit_and_duplicate_steps_are_not_turns() {
        let mut c = c();
        c.limits.max_text_bytes = 5;
        let mut p = Protocol::new();
        let turn = &c.settings.turn;
        p.message(json!({"type":"text","text":"abc"}), &c, turn)
            .unwrap();
        assert_eq!(
            p.message(json!({"type":"text","text":"de"}), &c, turn)
                .err(),
            Some(GradiumError::TooLarge)
        );
        p.message(json!({"type":"step","step_idx":1,"vad":[]}), &c, turn)
            .unwrap();
        assert!(p
            .message(json!({"type":"step","step_idx":1,"vad":[]}), &c, turn)
            .is_err());
    }
    #[test]
    fn nested_json_and_nonfinite_configuration_are_rejected() {
        let mut value = Value::Null;
        for _ in 0..20 {
            value = json!([value])
        }
        assert_eq!(
            validate_json(&value, 0, &mut 0),
            Err(GradiumError::TooLarge)
        );
        let mut settings = GradiumTurnSettings {
            eot_threshold: f64::NAN,
            ..Default::default()
        };
        assert!(settings.validate().is_err());
        settings.eot_threshold = 0.5;
        settings.eot_horizon_s = f64::INFINITY;
        assert!(settings.validate().is_err());
    }
}

#[cfg(test)]
#[path = "review_protocol.rs"]
mod independent_review;
