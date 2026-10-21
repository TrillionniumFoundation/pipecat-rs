//! Session-local, arrival-ordered evidence. No timestamp sorting or mutable tool lookup.
use super::{
    config::{invalid, MAX_HISTORY, MAX_TEXT},
    *,
};
use crate::EvalError;
use serde_json::json;

#[derive(Default)]
pub(super) struct QualityLedger {
    pub entries: Vec<Value>,
    pub targets: Vec<(usize, String)>,
    pending: Vec<usize>,
    text: String,
    bytes: usize,
    awaiting_reply: bool,
}
impl QualityLedger {
    fn append(&mut self, value: Value) -> Result<(), EvalError> {
        self.bytes = self.bytes.saturating_add(serde_json::to_vec(&value)?.len());
        if self.bytes > MAX_HISTORY || self.entries.len() >= 2048 {
            return Err(EvalError::Limit("quality evidence ledger"));
        }
        self.entries.push(value);
        Ok(())
    }
    fn flush(&mut self, max: usize) -> Result<(), EvalError> {
        if self.text.is_empty() {
            return Ok(());
        }
        let text = std::mem::take(&mut self.text);
        let index = if self.entries.last().is_some_and(|e| e["role"] == "bot") {
            let index = self.entries.len() - 1;
            let prior = self.entries[index]["content"].as_str().expect("bot text");
            if prior.len().saturating_add(text.len() + 1) > MAX_TEXT {
                return Err(EvalError::Limit("quality bot turn text"));
            }
            self.bytes = self
                .bytes
                .saturating_add(serde_json::to_vec(&text)?.len() + 1);
            if self.bytes > MAX_HISTORY {
                return Err(EvalError::Limit("quality evidence ledger"));
            }
            self.entries[index]["content"] = json!(format!("{prior} {text}"));
            index
        } else {
            // Provisional segments count too: no unbounded incomplete response storage.
            if self.entries.iter().filter(|e| e["role"] == "bot").count() >= max {
                return Err(EvalError::Limit("quality bot turns"));
            }
            let index = self.entries.len();
            self.append(json!({"role":"bot", "content":text, "evidence_kind":"observed_text_not_completion"}))?;
            index
        };
        if !self.pending.contains(&index) {
            self.pending.push(index);
        }
        Ok(())
    }
    pub fn started(&mut self) {
        // Match EventStream: a new started discards an unfinished text buffer.
        self.text.clear();
        self.pending.clear();
        self.awaiting_reply = false;
    }
    pub fn text(&mut self, text: &str) -> Result<(), EvalError> {
        if self.awaiting_reply {
            return Ok(());
        }
        if self.text.len().saturating_add(text.len()) > MAX_TEXT {
            return Err(EvalError::Limit("quality response text"));
        }
        self.text.push_str(text);
        Ok(())
    }
    pub fn stopped(&mut self, max: usize) -> Result<(), EvalError> {
        if self.awaiting_reply {
            return Ok(());
        }
        self.flush(max)?;
        for index in self.pending.drain(..) {
            let text = self.entries[index]["content"]
                .as_str()
                .expect("bot text")
                .to_owned();
            if let Some(target) = self.targets.iter_mut().find(|t| t.0 == index) {
                target.1 = text;
            } else {
                self.targets.push((index, text));
            }
        }
        Ok(())
    }
    pub fn tool(&mut self, tool: &BotTool, max: usize) -> Result<(), EvalError> {
        self.flush(max)?;
        self.append(json!({"role":"tool", "content":tool}))
    }
    pub fn user(&mut self, text: &str, max: usize) -> Result<(), EvalError> {
        self.flush(max)?;
        self.append(json!({"role":"user", "content":text}))?;
        self.awaiting_reply = true;
        self.pending.clear();
        Ok(())
    }
    pub fn evidence(&self, target: &(usize, String), turn: usize, metric: &JudgedMetric) -> Value {
        json!({"metric":metric.name, "criterion":metric.criterion, "turn":turn,
            "conversation":&self.entries[..target.0], "latest_bot_reply":target.1})
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QualityVerdict {
    pub metric: String,
    pub turn: usize,
    pub outcome: TurnOutcome,
    pub reason: String,
}
pub(super) fn parse_verdict(
    text: &str,
    metric: &str,
    turn: usize,
) -> Result<QualityVerdict, EvalError> {
    let verdict: QualityVerdict = serde_json::from_str(text)
        .map_err(|_| invalid("judge must return a strict turn verdict"))?;
    if verdict.metric != metric
        || verdict.turn != turn
        || verdict.reason.trim().is_empty()
        || verdict.reason.len() > 4096
    {
        return Err(invalid("judge turn identity or bounded reason is invalid"));
    }
    Ok(verdict)
}
pub(super) fn initialize(metrics: &[JudgedMetric], turns: usize) -> Vec<JudgedMetricResult> {
    metrics
        .iter()
        .map(|m| JudgedMetricResult {
            name: m.name.clone(),
            score: None,
            min_score: m.min_score,
            passed: None,
            failure_kind: None,
            turns: (1..=turns)
                .map(|turn| TurnJudgment {
                    turn,
                    outcome: TurnOutcome::None,
                    reason: "not judged".into(),
                    diagnostic: Some("not_judged".into()),
                })
                .collect(),
        })
        .collect()
}
pub(super) fn score(metric: &mut JudgedMetricResult, judge_configured: bool) {
    if metric.turns.is_empty() || !judge_configured {
        return;
    }
    let yes = metric
        .turns
        .iter()
        .filter(|v| matches!(v.outcome, TurnOutcome::Meets | TurnOutcome::NotApplicable))
        .count();
    let score = yes as f64 / metric.turns.len() as f64;
    metric.score = Some(score);
    metric.passed = metric.min_score.map(|min| score >= min);
    if metric.passed == Some(false) {
        metric.failure_kind = Some(
            if metric.turns.iter().any(|v| v.outcome == TurnOutcome::Fails) {
                "criterion_failure"
            } else {
                "criterion_no_verdict"
            }
            .into(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn criterion() -> JudgedMetric {
        JudgedMetric {
            name: "q".into(),
            criterion: "polite".into(),
            min_score: None,
        }
    }
    #[test]
    fn ledger_concatenates_chunks_coalesces_complete_segments_and_bounds_storage() {
        let mut ledger = QualityLedger::default();
        ledger.started();
        ledger.text("Hel").unwrap();
        ledger.text("lo").unwrap();
        ledger.stopped(2).unwrap();
        ledger.started();
        ledger.text("again").unwrap();
        ledger.stopped(2).unwrap();
        assert_eq!(ledger.targets.len(), 1);
        assert_eq!(ledger.targets[0].1, "Hello again");
        ledger.user("next", 2).unwrap();
        ledger.text("STALE").unwrap();
        ledger.stopped(2).unwrap();
        assert_eq!(ledger.targets.len(), 1);
        ledger.started();
        ledger.text("second").unwrap();
        ledger.stopped(2).unwrap();
        assert_eq!(ledger.targets.len(), 2);
        ledger.user("more", 2).unwrap();
        ledger.started();
        ledger.text("over budget").unwrap();
        assert!(ledger.stopped(2).is_err());
        let mut ledger = QualityLedger::default();
        assert!(ledger.text(&"x".repeat(MAX_TEXT + 1)).is_err());
        ledger.bytes = MAX_HISTORY;
        assert!(ledger.user("x", 2).is_err());
        let mut ledger = QualityLedger::default();
        ledger.entries = vec![json!({}); 2048];
        assert!(ledger.user("x", 2).is_err());
    }
    #[test]
    fn strict_turn_parser_rejects_duplicate_missing_unknown_identity_and_oversized_reasons() {
        for bad in [
            "{}".to_owned(),
            "{\"metric\":\"q\",\"metric\":\"q\",\"turn\":1,\"outcome\":\"meets\",\"reason\":\"r\"}"
                .into(),
            json!({"metric":"q","turn":1,"outcome":"yes","reason":"r"}).to_string(),
            json!({"metric":"q","turn":2,"outcome":"meets","reason":"r"}).to_string(),
            json!({"metric":"wrong","turn":1,"outcome":"meets","reason":"r"}).to_string(),
            json!({"metric":"q","turn":1,"outcome":"meets","reason":""}).to_string(),
            json!({"metric":"q","turn":1,"outcome":"meets","reason":"x".repeat(4097)}).to_string(),
            json!({"metric":"q","turn":1,"outcome":"meets","reason":"r","confidence":1})
                .to_string(),
        ] {
            assert!(parse_verdict(&bad, "q", 1).is_err(), "{bad}");
        }
        for outcome in ["meets", "fails", "not_applicable", "none"] {
            assert!(parse_verdict(
                &json!({"metric":"q","turn":1,"outcome":outcome,"reason":"r"}).to_string(),
                "q",
                1
            )
            .is_ok());
        }
    }
    #[test]
    fn expected_none_denominator_report_only_and_zero_turn_semantics() {
        let mut metric = criterion();
        metric.min_score = Some(0.75);
        let mut results = initialize(&[metric.clone()], 4);
        let r = &mut results[0];
        r.turns[0].outcome = TurnOutcome::Meets;
        r.turns[1].outcome = TurnOutcome::NotApplicable;
        r.turns[2].outcome = TurnOutcome::Meets;
        score(r, true);
        assert_eq!(r.score, Some(0.75));
        assert_eq!(r.passed, Some(true));
        r.min_score = Some(0.8);
        score(r, true);
        assert_eq!(r.failure_kind.as_deref(), Some("criterion_no_verdict"));
        let mut r = initialize(&[criterion()], 1).remove(0);
        score(&mut r, true);
        assert_eq!(r.score, Some(0.0));
        assert!(r.passed.is_none());
        assert!(r.failure_kind.is_none());
        let mut r = initialize(&[metric], 0).remove(0);
        score(&mut r, true);
        assert!(r.score.is_none());
        assert!(r.passed.is_none());
        let mut r = initialize(&[criterion()], 1).remove(0);
        score(&mut r, false);
        assert!(r.score.is_none());
        assert!(r.passed.is_none());
    }
}
