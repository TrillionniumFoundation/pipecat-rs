//! Deterministic source assertion semantics, independent of network I/O.
use crate::{EvalError, Expectation, FunctionCall};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum MatchOutcome {
    Pending,
    Passed(String),
    Failed { kind: String, reason: String },
}
impl MatchOutcome {
    fn failed(kind: &str, reason: impl Into<String>) -> Self {
        Self::Failed {
            kind: kind.into(),
            reason: reason.into(),
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssertionFailure {
    pub turn_index: i64,
    pub expectation_index: i64,
    pub event_name: String,
    pub reason: String,
    pub kind: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExpectationResult {
    pub expectation_index: usize,
    pub event_name: String,
    pub passed: bool,
    pub matched: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub judgments: Vec<crate::JudgeVerdict>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnResult {
    pub turn_index: usize,
    pub status: String,
    pub failures: Vec<AssertionFailure>,
    pub expectations: Vec<ExpectationResult>,
    pub duration_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptResult {
    pub scenario_name: String,
    pub passed: bool,
    pub failures: Vec<AssertionFailure>,
    pub turns: Vec<TurnResult>,
    pub duration_ms: u64,
    pub events_seen: Vec<Value>,
    pub debug_log: Vec<String>,
    pub skipped: Option<String>,
}
impl ScriptResult {
    pub fn new(name: &str, turns: usize) -> Self {
        Self {
            scenario_name: name.into(),
            passed: false,
            failures: vec![],
            turns: (0..turns)
                .map(|i| TurnResult {
                    turn_index: i,
                    status: "not_run".into(),
                    failures: vec![],
                    expectations: vec![],
                    duration_ms: 0,
                })
                .collect(),
            duration_ms: 0,
            events_seen: vec![],
            debug_log: vec![],
            skipped: None,
        }
    }
    pub(crate) fn fail(&mut self, mut failure: AssertionFailure) {
        // Full payloads remain in events_seen; repeated failures must not multiply
        // their diagnostic storage by the number of later expectations.
        if failure.reason.len() > 4096 {
            let mut end = 4096;
            while !failure.reason.is_char_boundary(end) {
                end -= 1;
            }
            failure.reason.truncate(end);
            failure.reason.push_str("… (see events_seen)");
        }
        if let Ok(index) = usize::try_from(failure.turn_index) {
            if let Some(turn) = self.turns.get_mut(index) {
                turn.status = "failed".into();
                turn.failures.push(failure.clone());
            }
        }
        self.failures.push(failure);
        self.passed = false;
    }
}
pub struct PendingCheck {
    expectation: Expectation,
    aggregate: String,
    seen: bool,
    remaining: Vec<FunctionCall>,
    matched: Vec<String>,
    judged_calls: Vec<Value>,
    judge_enabled: bool,
}
impl PendingCheck {
    pub fn new(expectation: Expectation) -> Self {
        let remaining = expectation.calls.clone().unwrap_or_default();
        Self {
            expectation,
            aggregate: String::new(),
            seen: false,
            remaining,
            matched: vec![],
            judged_calls: vec![],
            judge_enabled: false,
        }
    }
    pub(crate) fn for_session(expectation: Expectation) -> Self {
        Self {
            judge_enabled: true,
            ..Self::new(expectation)
        }
    }
    pub(crate) fn take_judged_calls(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.judged_calls)
    }
}
pub struct ExpectationMatcher {
    pending_calls: Vec<Value>,
    max_text_bytes: usize,
    max_calls: usize,
}
impl ExpectationMatcher {
    pub fn new(max_text_bytes: usize, max_calls: usize) -> Self {
        Self {
            pending_calls: vec![],
            max_text_bytes,
            max_calls,
        }
    }
    pub fn reset_turn(&mut self) {
        self.pending_calls.clear();
    }
    pub fn buffered(&mut self, check: &mut PendingCheck) -> MatchOutcome {
        if check.expectation.eval.is_some() && !check.judge_enabled {
            return MatchOutcome::failed(
                "no_judge",
                "eval requires an EvalSession with a native judge",
            );
        }
        if !check.expectation.is_call() || check.expectation.absent {
            return MatchOutcome::Pending;
        }
        while let Some(spec) = check.remaining.first() {
            let Some(index) = self
                .pending_calls
                .iter()
                .position(|event| call_matches(event, &check.expectation.event, spec))
            else {
                return MatchOutcome::Pending;
            };
            let event = self.pending_calls.remove(index);
            check
                .matched
                .push(event["name"].as_str().unwrap_or("?").into());
            if check.expectation.eval.is_some() {
                check.judged_calls.push(event);
            }
            check.remaining.remove(0);
        }
        MatchOutcome::Passed(check.matched.join(", "))
    }
    pub fn feed(
        &mut self,
        check: &mut PendingCheck,
        event: Value,
    ) -> Result<MatchOutcome, EvalError> {
        if check.expectation.eval.is_some() && !check.judge_enabled {
            return Err(EvalError::Unsupported(
                "eval requires an EvalSession with a native judge".into(),
            ));
        }
        let exp = &check.expectation;
        if exp.absent {
            return Ok(if event["type"] == exp.event {
                MatchOutcome::failed(
                    "unexpected_event",
                    format!("expected no {:?}, received {}", exp.event, event),
                )
            } else {
                MatchOutcome::Pending
            });
        }
        if exp.is_call() {
            if matches!(
                event["type"].as_str(),
                Some("function_call" | "function_call_stopped")
            ) {
                if self.pending_calls.len() >= self.max_calls {
                    return Err(EvalError::Limit("unmatched function calls"));
                }
                self.pending_calls.push(event);
            }
            return Ok(self.buffered(check));
        }
        if event["type"] != exp.event {
            return Ok(MatchOutcome::Pending);
        }
        check.seen = true;
        let text = event_text(&event);
        if exp.aggregates() {
            let separator = usize::from(!check.aggregate.is_empty());
            if check
                .aggregate
                .len()
                .saturating_add(text.len())
                .saturating_add(separator)
                > self.max_text_bytes
            {
                return Err(EvalError::Limit("assertion aggregate text"));
            }
            if separator > 0 {
                check.aggregate.push(' ');
            }
            check.aggregate.push_str(text);
            if let Some(needle) = &exp.text_excludes {
                if text_contains(&check.aggregate, needle) {
                    return Ok(MatchOutcome::failed(
                        "text_present",
                        format!("text contains {needle:?}"),
                    ));
                }
            }
            if exp
                .text_contains
                .as_ref()
                .is_some_and(|needle| !text_contains(&check.aggregate, needle))
            {
                return Ok(MatchOutcome::Pending);
            }
            return Ok(MatchOutcome::Passed(check.aggregate.clone()));
        }
        if exp
            .text_contains
            .as_ref()
            .is_some_and(|needle| !text_contains(text, needle))
        {
            return Ok(MatchOutcome::failed(
                "text_mismatch",
                format!("text {text:?} does not contain {:?}", exp.text_contains),
            ));
        }
        if exp
            .text_excludes
            .as_ref()
            .is_some_and(|needle| text_contains(text, needle))
        {
            return Ok(MatchOutcome::failed(
                "text_present",
                format!("text {text:?} contains {:?}", exp.text_excludes),
            ));
        }
        if let Some(want) = &exp.marker {
            let kind = event["kind"].as_str().unwrap_or("");
            if (want == "incomplete" && !matches!(kind, "short" | "long"))
                || (want != "incomplete" && kind != want)
            {
                return Ok(MatchOutcome::failed(
                    "marker_mismatch",
                    format!("marker kind {kind:?}, expected {want:?}"),
                ));
            }
        }
        if let Some(reason) = marker_problem(&event, exp) {
            return Ok(MatchOutcome::failed("marker_format", reason));
        }
        Ok(MatchOutcome::Passed(text.into()))
    }
    pub fn timeout(&self, check: &PendingCheck) -> MatchOutcome {
        if check.expectation.eval.is_some() && !check.judge_enabled {
            return MatchOutcome::failed(
                "no_judge",
                "eval requires an EvalSession with a native judge",
            );
        }
        let exp = &check.expectation;
        if exp.absent {
            return MatchOutcome::Passed(format!("no {:?} during quiet window", exp.event));
        }
        if exp.is_call() {
            if let Some(spec) = check.remaining.first() {
                let near = self
                    .pending_calls
                    .iter()
                    .filter(|event| {
                        event["type"] == exp.event
                            && spec.name.as_ref().is_none_or(|n| event["name"] == *n)
                    })
                    .count();
                if spec.args.is_some() && near > 0 {
                    return MatchOutcome::failed(
                        "function_args_mismatch",
                        format!(
                            "{near} call(s) named {:?} had different arguments; see events_seen",
                            spec.name.as_deref().map(preview)
                        ),
                    );
                }
                return MatchOutcome::failed(
                    "missing_function_call",
                    format!("call {:?} not seen; matched {:?}", spec.name, check.matched),
                );
            }
        }
        if check.seen && exp.aggregates() {
            MatchOutcome::failed(
                "text_mismatch",
                format!(
                    "text {:?} never contained {:?}",
                    preview(&check.aggregate),
                    exp.text_contains.as_deref().map(preview)
                ),
            )
        } else {
            MatchOutcome::failed(
                "timeout",
                format!("no matching {:?} event before deadline", exp.event),
            )
        }
    }
}
fn preview(text: &str) -> String {
    text.chars().take(512).collect()
}
fn call_matches(event: &Value, kind: &str, spec: &FunctionCall) -> bool {
    event["type"] == kind
        && spec.name.as_ref().is_none_or(|name| event["name"] == *name)
        && spec.args.as_ref().is_none_or(|args| {
            args.iter()
                .all(|(key, value)| event["args"].get(key) == Some(value))
        })
}
pub fn text_contains(text: &str, needle: &str) -> bool {
    text.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .contains(&needle.split_whitespace().collect::<Vec<_>>().join(" "))
}
fn event_text(event: &Value) -> &str {
    event["text"]
        .as_str()
        .filter(|s| !s.is_empty())
        .or_else(|| event["transcript"].as_str())
        .unwrap_or("")
}
fn marker_problem(event: &Value, exp: &Expectation) -> Option<String> {
    if exp.marker_first.is_none() && exp.markers.is_none() && exp.text_after.is_none() {
        return None;
    }
    let raw = event["raw"].as_str().unwrap_or("");
    let known: Vec<_> = event["markers"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|s| !s.is_empty())
        .collect();
    let count: usize = known.iter().map(|m| raw.matches(m).count()).sum();
    let first = known
        .iter()
        .filter_map(|m| raw.find(m).map(|at| (at, *m)))
        .min();
    if exp.markers.is_some_and(|n| n != count) {
        return Some(format!(
            "raw text holds {count} marker(s), expected {:?}",
            exp.markers
        ));
    }
    if exp
        .marker_first
        .is_some_and(|want| want != first.is_some_and(|(at, _)| raw[..at].trim().is_empty()))
    {
        return Some("marker_first does not match raw response".into());
    }
    if let Some(want) = exp.text_after {
        let Some((at, marker)) = first else {
            return Some("raw text holds no marker to check text after".into());
        };
        if want == raw[at + marker.len()..].trim().is_empty() {
            return Some("text_after does not match raw response".into());
        }
    }
    None
}
