//! Opt-in autonomous text evaluation, independent of scripted run/suite semantics.
mod config;
mod session;
pub use config::{Metric, ModelConfig, SimulationRuntime, SimulationScenario};
use serde::{Deserialize, Serialize};
use serde_json::Value;
pub use session::SimulationSession;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EndCall {
    pub success: bool,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TranscriptLine {
    pub role: String,
    pub text: String,
    pub at_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BotTool {
    pub id: String,
    pub name: String,
    pub arguments: Value,
    pub at_ms: u64,
    pub stopped: bool,
    pub cancelled: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoalVerdict {
    pub verdict: Verdict,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Yes,
    No,
    None,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricResult {
    pub name: String,
    pub value: Option<Value>,
    pub passed: Option<bool>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationFailure {
    pub kind: String,
    pub reason: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    pub name: String,
    /// passed, failed, error, or unjudged. Persona claims never set this field.
    pub status: String,
    pub ended_by: String,
    pub end_call: Option<EndCall>,
    pub goal: Option<GoalVerdict>,
    pub transcript: Vec<TranscriptLine>,
    pub tools: Vec<BotTool>,
    pub turns: usize,
    pub duration_ms: u64,
    pub total_elapsed_ms: u64,
    pub longest_bot_reply_words: Option<u64>,
    pub slowest_text_response_ms: Option<u64>,
    pub metrics: Vec<MetricResult>,
    pub failures: Vec<SimulationFailure>,
}
impl SimulationResult {
    pub fn passed(&self) -> bool {
        self.status == "passed"
    }
    pub(super) fn fail(&mut self, kind: &str, reason: impl Into<String>) {
        self.failures.push(SimulationFailure {
            kind: kind.into(),
            reason: reason.into(),
        });
    }
}
