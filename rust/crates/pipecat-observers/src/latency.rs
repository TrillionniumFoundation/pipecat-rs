//! User-silence/greeting latency from observed pipeline milestones.
use pipecat_core::{Direction, PipelineObservation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyAnchor {
    UserSilence,
    ClientConnected,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyContribution {
    pub kind: String,
    pub start_ns: u64,
    pub seconds: f64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserBotLatencyReport {
    pub measured_from: LatencyAnchor,
    pub started_ns: u64,
    pub finished_ns: u64,
    pub seconds: f64,
    pub user_turn_seconds: Option<f64>,
    /// Observed adjacent milestones tile the measured interval exactly.
    pub contributions: Vec<LatencyContribution>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Moment {
    Silence,
    Ready,
    VadStop,
    Transcript,
    Request,
    Text,
    Audio,
    Speaking,
}
struct Cycle {
    start: u64,
    anchor: LatencyAnchor,
    moments: Vec<(u64, Moment)>,
    turn: Option<u64>,
}
/// Monotonic event timestamps are used, never callback dispatch time or a
/// payload wall-clock epoch. VAD silence is derived from its stop duration at
/// first push; a delayed producer push therefore includes that producer delay.
#[derive(Default)]
pub struct UserBotLatency {
    client: Option<u64>,
    started: Option<u64>,
    root_runtime: Option<u64>,
    greeting_done: bool,
    cycle: Option<Cycle>,
}
impl UserBotLatency {
    pub fn reset(&mut self) {
        *self = Self::default();
    }
    pub fn observe(&mut self, event: &PipelineObservation) -> Option<UserBotLatencyReport> {
        match event {
            PipelineObservation::RuntimeStarted { runtime_id, .. } => {
                self.root_runtime.get_or_insert(*runtime_id);
                return None;
            }
            PipelineObservation::PipelineStarted {
                runtime_id,
                timestamp_ns,
            } if Some(*runtime_id) == self.root_runtime => {
                self.started.get_or_insert(*timestamp_ns);
                return None;
            }
            _ => {}
        }
        let PipelineObservation::Push {
            frame,
            direction: Direction::Downstream,
            first_push: true,
            timestamp_ns: at,
            ..
        } = event
        else {
            return None;
        };
        match frame.kind {
            "ClientConnectedFrame" => {
                self.client.get_or_insert(*at);
            }
            "VADUserStartedSpeakingFrame" | "UserStartedSpeakingFrame" => {
                self.cycle = None;
                self.greeting_done = true;
            }
            "InterruptionFrame" | "CancelFrame" | "EndFrame" | "StopFrame" => {
                self.cycle = None;
                self.greeting_done = true;
            }
            "VADUserStoppedSpeakingFrame" => {
                self.cycle = None;
                self.greeting_done = true;
                let duration = frame.vad_stop_secs?;
                if !duration.is_finite() || duration < 0.0 || duration * 1e9 > u64::MAX as f64 {
                    self.cycle = None;
                    return None;
                }
                let start = at.checked_sub((duration * 1e9).round() as u64)?;
                self.cycle = Some(Cycle {
                    start,
                    anchor: LatencyAnchor::UserSilence,
                    moments: vec![(start, Moment::Silence), (*at, Moment::VadStop)],
                    turn: None,
                });
            }
            _ => {}
        }
        if self.cycle.is_none() && !self.greeting_done {
            if let Some(client) = self.client {
                let ready = client.max(self.started.unwrap_or(client));
                self.cycle = Some(Cycle {
                    start: ready,
                    anchor: LatencyAnchor::ClientConnected,
                    moments: vec![(ready, Moment::Ready)],
                    turn: None,
                });
            }
        }
        let cycle = self.cycle.as_mut()?;
        if *at < cycle.start {
            return None;
        }
        if cycle.anchor == LatencyAnchor::ClientConnected {
            // Setup may finish after ClientConnected, but before the greeting.
            let ready = self.client?.max(self.started.unwrap_or(self.client?));
            cycle.start = ready;
            cycle
                .moments
                .retain(|(t, moment)| *moment != Moment::Ready && *t >= ready);
            cycle.moments.insert(0, (ready, Moment::Ready));
        }
        let moment = match frame.kind {
            "UserStoppedSpeakingFrame" => {
                cycle.turn = at.checked_sub(cycle.start);
                None
            }
            "TranscriptionFrame" if !cycle.moments.iter().any(|(_, m)| *m == Moment::Request) => {
                cycle.moments.retain(|(_, m)| *m != Moment::Transcript);
                Some(Moment::Transcript)
            }
            "LLMFullResponseStartFrame" => Some(Moment::Request),
            "LLMTextFrame" => Some(Moment::Text),
            "TTSAudioRawFrame" => Some(Moment::Audio),
            "BotStartedSpeakingFrame" => Some(Moment::Speaking),
            _ => None,
        };
        if let Some(moment) = moment {
            if !cycle.moments.iter().any(|(_, m)| *m == moment) {
                cycle.moments.push((*at, moment));
            }
        }
        if moment != Some(Moment::Speaking) {
            return None;
        }
        let cycle = self.cycle.take()?;
        self.greeting_done = true;
        let total = at.checked_sub(cycle.start)?;
        let mut moments = cycle.moments;
        moments.sort_by_key(|(at, _)| *at);
        let contributions = moments
            .windows(2)
            .filter_map(|pair| {
                let duration = pair[1].0.checked_sub(pair[0].0)?;
                if duration == 0 {
                    return None;
                }
                let kind = match (pair[0].1, pair[1].1) {
                    (Moment::Silence, Moment::VadStop) => "endpointing_wait",
                    (Moment::Ready, Moment::Request) => "first_request",
                    (Moment::VadStop, Moment::Transcript) => "transcription",
                    (Moment::Transcript, Moment::Request) => "turn_detection",
                    (Moment::Request, Moment::Text) => "llm_until_text",
                    (Moment::Text, Moment::Audio) => "text_to_audio",
                    (Moment::Audio, Moment::Speaking) => "output_transport",
                    _ => "pipeline",
                };
                Some(LatencyContribution {
                    kind: kind.into(),
                    start_ns: pair[0].0,
                    seconds: duration as f64 / 1e9,
                })
            })
            .collect();
        Some(UserBotLatencyReport {
            measured_from: cycle.anchor,
            started_ns: cycle.start,
            finished_ns: *at,
            seconds: total as f64 / 1e9,
            user_turn_seconds: cycle.turn.map(|d| d as f64 / 1e9),
            contributions,
        })
    }
}
