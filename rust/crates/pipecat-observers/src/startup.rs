//! Setup and Start-frame timing from runtime-captured boundaries.
use pipecat_core::{Direction, PipelineObservation, SetupOutcome};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcessorStartupTiming {
    pub index: usize,
    pub processor: String,
    pub setup_seconds: Option<f64>,
    pub setup_completed: bool,
    pub start_offset_seconds: Option<f64>,
    pub start_seconds: Option<f64>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StartupTimingReport {
    pub runtime_id: u64,
    pub started_ns: u64,
    pub finished_ns: u64,
    pub total_seconds: f64,
    /// Actual setup span, never the sum of overlapping processor times.
    pub setup_phase_seconds: f64,
    pub start_phase_seconds: f64,
    pub processors: Vec<ProcessorStartupTiming>,
    /// False for missing, backwards or aborted setup/Start intervals.
    pub complete: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TransportTimingReport {
    pub runtime_id: u64,
    pub started_ns: u64,
    pub bot_connected_seconds: Option<f64>,
    pub client_connected_seconds: f64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum StartupMeasurement {
    Startup(StartupTimingReport),
    Transport(TransportTimingReport),
}
#[derive(Default)]
struct ProcessorState {
    name: String,
    setup: Option<(u64, u64, SetupOutcome)>,
    arrival: Option<u64>,
    departure: Option<u64>,
}
struct RuntimeState {
    origin: u64,
    start: Option<(u64, u64)>,
    processors: BTreeMap<usize, ProcessorState>,
    reported: bool,
    transport_reported: bool,
    bot_connected: Option<u64>,
    incomplete: bool,
}
/// A synchronous reducer. Feed events in dispatch order; no wall-clock reads.
/// Pending runtimes/processors are bounded, with evictions exposed explicitly.
pub struct StartupTiming {
    runtimes: BTreeMap<u64, RuntimeState>,
    max_runtimes: usize,
    max_processors: usize,
    pub evicted_runtimes: u64,
}
impl Default for StartupTiming {
    fn default() -> Self {
        Self::new(64, 4096)
    }
}
impl StartupTiming {
    pub fn new(max_runtimes: usize, max_processors: usize) -> Self {
        Self {
            runtimes: BTreeMap::new(),
            max_runtimes: max_runtimes.max(1),
            max_processors: max_processors.max(1),
            evicted_runtimes: 0,
        }
    }
    pub fn reset(&mut self) {
        self.runtimes.clear();
    }
    pub fn observe(&mut self, event: &PipelineObservation) -> Option<StartupMeasurement> {
        match event {
            PipelineObservation::RuntimeStarted {
                runtime_id,
                timestamp_ns,
            } => {
                if !self.runtimes.contains_key(runtime_id)
                    && self.runtimes.len() == self.max_runtimes
                {
                    self.runtimes.pop_first();
                    self.evicted_runtimes += 1;
                }
                self.runtimes.insert(
                    *runtime_id,
                    RuntimeState {
                        origin: *timestamp_ns,
                        start: None,
                        processors: BTreeMap::new(),
                        reported: false,
                        transport_reported: false,
                        bot_connected: None,
                        incomplete: false,
                    },
                );
            }
            PipelineObservation::ProcessorSetup {
                processor,
                started_ns,
                finished_ns,
                outcome,
            } => {
                let state = self.runtimes.get_mut(&processor.runtime_id)?;
                if state.reported {
                    return None;
                }
                if state.processors.len() >= self.max_processors
                    && !state.processors.contains_key(&processor.index)
                {
                    state.incomplete = true;
                    return None;
                }
                let entry = state.processors.entry(processor.index).or_default();
                entry.name = processor.name.to_string();
                entry.setup = Some((*started_ns, *finished_ns, *outcome));
            }
            PipelineObservation::Process {
                processor,
                frame,
                direction: Direction::Downstream,
                timestamp_ns,
            } if frame.kind == "StartFrame" => {
                let state = self.runtimes.get_mut(&processor.runtime_id)?;
                if state.reported {
                    return None;
                }
                let start = state.start.get_or_insert((frame.id, *timestamp_ns));
                if start.0 != frame.id {
                    return None;
                }
                if state.processors.len() >= self.max_processors
                    && !state.processors.contains_key(&processor.index)
                {
                    state.incomplete = true;
                    return None;
                }
                let entry = state.processors.entry(processor.index).or_default();
                entry.name = processor.name.to_string();
                entry.arrival.get_or_insert(*timestamp_ns);
            }
            PipelineObservation::Push {
                source,
                frame,
                direction: Direction::Downstream,
                timestamp_ns,
                ..
            } if frame.kind == "StartFrame" => {
                let state = self.runtimes.get_mut(&source.runtime_id)?;
                if state.reported || state.start.map(|s| s.0) != Some(frame.id) {
                    return None;
                }
                if let Some(processor) = state.processors.get_mut(&source.index) {
                    processor.departure.get_or_insert(*timestamp_ns);
                }
            }
            PipelineObservation::Push {
                source,
                frame,
                timestamp_ns,
                ..
            } if frame.kind == "BotConnectedFrame" || frame.kind == "ClientConnectedFrame" => {
                let state = self.runtimes.get_mut(&source.runtime_id)?;
                if state.transport_reported || *timestamp_ns < state.origin {
                    return None;
                }
                if frame.kind == "BotConnectedFrame" {
                    state.bot_connected.get_or_insert(*timestamp_ns);
                } else {
                    state.transport_reported = true;
                    return Some(StartupMeasurement::Transport(TransportTimingReport {
                        runtime_id: source.runtime_id,
                        started_ns: state.origin,
                        bot_connected_seconds: state
                            .bot_connected
                            .and_then(|t| seconds(state.origin, t)),
                        client_connected_seconds: seconds(state.origin, *timestamp_ns)?,
                    }));
                }
            }
            PipelineObservation::PipelineStarted {
                runtime_id,
                timestamp_ns,
            } => {
                let state = self.runtimes.get_mut(runtime_id)?;
                if state.reported {
                    return None;
                }
                state.reported = true;
                let total_seconds = seconds(state.origin, *timestamp_ns)?;
                let setup_end = state
                    .processors
                    .values()
                    .filter_map(|p| p.setup.map(|(_, end, _)| end))
                    .max()
                    .unwrap_or(state.origin);
                let setup_phase_seconds = seconds(state.origin, setup_end.min(*timestamp_ns))?;
                let mut complete =
                    !state.incomplete && state.start.is_some() && !state.processors.is_empty();
                let processors = state
                    .processors
                    .iter()
                    .map(|(&index, p)| {
                        let setup_seconds = p.setup.and_then(|(start, end, _)| seconds(start, end));
                        let setup_completed = p
                            .setup
                            .is_some_and(|(_, _, outcome)| outcome == SetupOutcome::Completed);
                        let start_seconds =
                            p.arrival.zip(p.departure).and_then(|(a, b)| seconds(a, b));
                        let start_offset_seconds = state
                            .start
                            .and_then(|(_, start)| p.arrival.and_then(|a| seconds(start, a)));
                        complete &= setup_seconds.is_some()
                            && setup_completed
                            && start_seconds.is_some()
                            && start_offset_seconds.is_some();
                        ProcessorStartupTiming {
                            index,
                            processor: p.name.clone(),
                            setup_seconds,
                            setup_completed,
                            start_offset_seconds,
                            start_seconds,
                        }
                    })
                    .collect();
                return Some(StartupMeasurement::Startup(StartupTimingReport {
                    runtime_id: *runtime_id,
                    started_ns: state.origin,
                    finished_ns: *timestamp_ns,
                    total_seconds,
                    setup_phase_seconds,
                    start_phase_seconds: total_seconds - setup_phase_seconds,
                    processors,
                    complete,
                }));
            }
            _ => {}
        }
        None
    }
}
fn seconds(start: u64, end: u64) -> Option<f64> {
    end.checked_sub(start).map(|d| d as f64 / 1e9)
}
