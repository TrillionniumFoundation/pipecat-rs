//! Bounded suites against explicitly supplied, independently hosted bot endpoints.
use crate::{
    matcher::{AssertionFailure, ScriptResult},
    Cancellation, EvalError, EvalSession, ScriptScenario, SessionOptions,
};
use serde::Serialize;
use std::{collections::VecDeque, io::Write};
use tokio::task::JoinSet;

const MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct SuiteJob {
    pub scenario: ScriptScenario,
    pub bot_url: String,
}
#[derive(Debug, Clone, Serialize)]
pub struct SuiteResult {
    pub passed: bool,
    pub runs: Vec<ScriptResult>,
}
/// Validate all jobs before networking, run at most `concurrency` sessions, and
/// join every owned task before returning. Dropping the suite aborts its JoinSet.
/// Endpoints must be isolated if the bot has per-process conversation state.
///
/// The returned result's JSON representation is bounded to 64 MiB. Space for a
/// small failure summary is reserved for every pending job; this is a serialized
/// output limit, not a bound on allocator overhead or active-session memory.
pub async fn run_suite(
    jobs: Vec<SuiteJob>,
    concurrency: usize,
    options: SessionOptions,
    cancel: Cancellation,
) -> Result<SuiteResult, EvalError> {
    run_suite_with_budget(jobs, concurrency, options, cancel, MAX_RESULT_BYTES).await
}

async fn run_suite_with_budget(
    jobs: Vec<SuiteJob>,
    concurrency: usize,
    options: SessionOptions,
    cancel: Cancellation,
    max_result_bytes: usize,
) -> Result<SuiteResult, EvalError> {
    if !(1..=32).contains(&concurrency) || jobs.len() > 128 {
        return Err(EvalError::Scenario(
            "suite supports 1..32 concurrent sessions and at most 128 jobs".into(),
        ));
    }
    // Refuse an impossible summary budget before admitting any network work.
    let mut retained = RetainedResults::new(
        jobs.iter().map(|job| job.scenario.name.as_str()),
        max_result_bytes,
    )?;
    let mut pending = VecDeque::new();
    for (index, job) in jobs.into_iter().enumerate() {
        pending.push_back((
            index,
            EvalSession::new(job.scenario, job.bot_url, options.clone())?,
        ));
    }
    let mut tasks = JoinSet::new();
    loop {
        while tasks.len() < concurrency {
            let Some((index, session)) = pending.pop_front() else {
                break;
            };
            if cancel.is_cancelled() {
                // A sticky pre-cancelled session resolves its biased cancellation
                // branch before drive() is polled: no socket is opened or spawned.
                let result = session.run(cancel.clone()).await;
                retain_result(&mut retained, index, result, &cancel, &mut tasks).await?;
            } else {
                let cancel = cancel.clone();
                tasks.spawn(async move { (index, session.run(cancel).await) });
            }
        }
        let Some(joined) = tasks.join_next().await else {
            break;
        };
        let (index, result) = match joined {
            Ok(result) => result,
            Err(error) => {
                tasks.shutdown().await;
                return Err(EvalError::Scenario(format!(
                    "suite session task failed: {error}"
                )));
            }
        };
        retain_result(&mut retained, index, result, &cancel, &mut tasks).await?;
    }
    retained.finish()
}

async fn retain_result(
    retained: &mut RetainedResults,
    index: usize,
    result: ScriptResult,
    cancel: &Cancellation,
    tasks: &mut JoinSet<(usize, ScriptResult)>,
) -> Result<(), EvalError> {
    match retained.insert(index, result) {
        Ok(overflowed) => {
            if overflowed {
                cancel.cancel();
            }
            Ok(())
        }
        Err(error) => {
            cancel.cancel();
            tasks.shutdown().await;
            Err(error)
        }
    }
}

/// Charge every stored record and reserve exactly sized summaries for the rest.
/// The false-result envelope (one byte longer than true) and array commas are
/// included so the final SuiteResult, rather than just its event arrays, fits.
struct RetainedResults {
    results: Vec<Option<ScriptResult>>,
    summaries: Vec<Option<(ScriptResult, usize)>>,
    stored_bytes: usize,
    reserved_bytes: usize,
    max_bytes: usize,
}
impl RetainedResults {
    fn new<'a>(
        names: impl IntoIterator<Item = &'a str>,
        max_bytes: usize,
    ) -> Result<Self, EvalError> {
        let mut summaries = Vec::new();
        let mut reserved_bytes = 0usize;
        for (index, name) in names.into_iter().enumerate() {
            let summary = limit_summary(index, name);
            let size = serialized_size(&summary)?;
            reserved_bytes = reserved_bytes
                .checked_add(size)
                .ok_or(EvalError::Limit("suite result size"))?;
            summaries.push(Some((summary, size)));
        }
        let stored_bytes = serialized_size(&SuiteResult {
            passed: false,
            runs: Vec::new(),
        })?
        .checked_add(summaries.len().saturating_sub(1))
        .ok_or(EvalError::Limit("suite result size"))?;
        if stored_bytes
            .checked_add(reserved_bytes)
            .is_none_or(|required| required > max_bytes)
        {
            return Err(EvalError::Limit(
                "suite result budget cannot hold failure summaries",
            ));
        }
        Ok(Self {
            results: vec![None; summaries.len()],
            summaries,
            stored_bytes,
            reserved_bytes,
            max_bytes,
        })
    }
    fn insert(&mut self, index: usize, result: ScriptResult) -> Result<bool, EvalError> {
        let (summary, summary_bytes) = self
            .summaries
            .get_mut(index)
            .and_then(Option::take)
            .ok_or_else(|| EvalError::Scenario("duplicate or invalid suite result index".into()))?;
        self.reserved_bytes -= summary_bytes;
        let size = serialized_size(&result)?;
        let available = self.max_bytes - self.stored_bytes - self.reserved_bytes;
        let overflowed = size > available;
        let (result, size) = if overflowed {
            (summary, summary_bytes)
        } else {
            (result, size)
        };
        // The summary was reserved before networking; its own size is charged,
        // including names, failures, diagnostics, and all JSON syntax overhead.
        self.stored_bytes += size;
        self.results[index] = Some(result);
        debug_assert!(self.stored_bytes + self.reserved_bytes <= self.max_bytes);
        Ok(overflowed)
    }
    fn finish(self) -> Result<SuiteResult, EvalError> {
        let runs = self
            .results
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| EvalError::Scenario("suite ended without every job result".into()))?;
        let result = SuiteResult {
            passed: runs.iter().all(|run| run.passed),
            runs,
        };
        if serialized_size(&result)? > self.max_bytes {
            return Err(EvalError::Limit("suite result accounting"));
        }
        Ok(result)
    }
}
fn limit_summary(index: usize, name: &str) -> ScriptResult {
    let mut end = name.len().min(64);
    while !name.is_char_boundary(end) {
        end -= 1;
    }
    let abbreviated = if end == name.len() {
        name.to_owned()
    } else {
        format!("{}…", &name[..end])
    };
    let mut result = ScriptResult::new(&format!("job {index}: {abbreviated}"), 0);
    result.fail(AssertionFailure {
        turn_index: -1,
        expectation_index: -1,
        event_name: "suite".into(),
        reason: "suite result budget exceeded; full turns, matches, events and diagnostics omitted; remaining work cancelled".into(),
        kind: "limit_exceeded".into(),
    });
    result
}
/// Measure JSON without allocating another copy of a potentially large result.
pub(crate) fn serialized_size(value: &impl Serialize) -> Result<usize, EvalError> {
    #[derive(Default)]
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("serialized size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut count = Counter::default();
    serde_json::to_writer(&mut count, value)?;
    Ok(count.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{matcher::ExpectationResult, ScenarioFile};
    use pipecat_transports::{Packet, WebSocketSession};
    use serde_json::{json, Value};
    use std::time::Duration;

    fn scenario(name: &str) -> ScriptScenario {
        ScenarioFile::from_value(
            json!({"name":name,"turns":[{"user":"hi","expect":[{"event":"response"}]}]}),
        )
        .unwrap()
        .scenarios
        .remove(0)
    }
    fn minimal_budget(names: &[&str]) -> usize {
        let store = RetainedResults::new(names.iter().copied(), usize::MAX).unwrap();
        store.stored_bytes + store.reserved_bytes
    }
    fn bulky_result(name: &str) -> ScriptResult {
        let mut result = ScriptResult::new(name, 1);
        result.passed = true;
        result.turns[0].status = "passed".into();
        result.turns[0].expectations.push(ExpectationResult {
            expectation_index: 0,
            event_name: "llm_response".into(),
            passed: true,
            matched: "large matched reply".repeat(4096),
        });
        // No events at all: clearing events_seen cannot fix this result.
        result.debug_log.push("large diagnostic".repeat(4096));
        result.fail(AssertionFailure {
            turn_index: 0,
            expectation_index: 0,
            event_name: "llm_response".into(),
            reason: "failure detail".repeat(4096),
            kind: "text_mismatch".into(),
        });
        result
    }
    #[test]
    fn tiny_budget_replaces_bulky_non_event_fields_and_charges_all_summaries() {
        let long_name = "☃\nquoted\"name".repeat(2048);
        let names = [long_name.as_str(), "second"];
        let budget = minimal_budget(&names);
        assert!(budget < 1024);
        let mut retained = RetainedResults::new(names, budget).unwrap();
        let first = bulky_result(&long_name);
        assert!(first.events_seen.is_empty());
        assert!(serialized_size(&first).unwrap() > budget);
        assert!(retained.insert(0, first).unwrap());
        assert!(retained.stored_bytes > 26);
        assert!(retained.reserved_bytes > 0);
        assert!(retained.insert(1, bulky_result("second")).unwrap());
        assert_eq!(retained.reserved_bytes, 0);
        let accounted = retained.stored_bytes;
        let result = retained.finish().unwrap();
        assert!(!result.passed);
        assert!(result.runs.iter().all(|run| run.turns.is_empty()
            && run.events_seen.is_empty()
            && run.debug_log.is_empty()));
        assert!(result
            .runs
            .iter()
            .all(|run| run.failures.len() == 1 && run.failures[0].kind == "limit_exceeded"));
        assert!(result.runs[0].scenario_name.len() < 100);
        assert_eq!(serde_json::to_vec(&result).unwrap().len(), accounted);
        assert_eq!(accounted, budget);
    }
    #[test]
    fn exact_size_accounting_charges_success_and_overflow_records() {
        let names = ["first", "second", "third"];
        let mut small = ScriptResult::new("first", 0);
        small.passed = true;
        let budget = minimal_budget(&names) + serialized_size(&small).unwrap();
        let mut retained = RetainedResults::new(names, budget).unwrap();
        assert!(!retained.insert(0, small).unwrap());
        assert!(retained.insert(1, bulky_result("second")).unwrap());
        assert!(retained.insert(2, bulky_result("third")).unwrap());
        let accounted = retained.stored_bytes;
        let result = retained.finish().unwrap();
        assert_eq!(serde_json::to_vec(&result).unwrap().len(), accounted);
        assert!(accounted <= budget);
    }
    #[tokio::test]
    async fn impossible_summary_budget_fails_before_any_socket_is_opened() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let budget = minimal_budget(&["a"]) - 1;
        let result = run_suite_with_budget(
            vec![SuiteJob {
                scenario: scenario("a"),
                bot_url: url,
            }],
            1,
            SessionOptions::default(),
            Cancellation::new(),
            budget,
        )
        .await;
        assert!(matches!(
            result,
            Err(EvalError::Limit(
                "suite result budget cannot hold failure summaries"
            ))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn overflowing_real_reply_cancels_pending_job_without_connecting_it() {
        let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let pending = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let first_url = format!("ws://{}", first.local_addr().unwrap());
        let pending_url = format!("ws://{}", pending.local_addr().unwrap());
        let mut options = SessionOptions::default();
        options.transport.close_timeout = Duration::from_millis(50);
        options.transport.write_timeout = Duration::from_millis(200);
        options.max_duration = Duration::from_secs(2);
        let config = options.transport.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = first.accept().await.unwrap();
            let mut socket = WebSocketSession::accept(stream, config).await.unwrap();
            let ready = socket.recv().await.unwrap();
            assert!(
                matches!(ready, Packet::Text(ref text) if serde_json::from_str::<Value>(text).unwrap()["type"] == "client-ready")
            );
            socket
                .sender()
                .send(Packet::Text(
                    json!({"label":"rtvi-ai","type":"bot-ready","data":{}}).to_string(),
                ))
                .await
                .unwrap();
            let turn = socket.recv().await.unwrap();
            assert!(
                matches!(turn, Packet::Text(ref text) if serde_json::from_str::<Value>(text).unwrap()["type"] == "send-text")
            );
            for (kind, data) in [
                ("bot-llm-started", json!({})),
                (
                    "bot-llm-text",
                    json!({"text":"large native reply ".repeat(512)}),
                ),
                ("bot-llm-stopped", json!({})),
            ] {
                socket
                    .sender()
                    .send(Packet::Text(
                        json!({"label":"rtvi-ai","type":kind,"data":data}).to_string(),
                    ))
                    .await
                    .unwrap();
            }
            while socket.recv().await.is_some() {}
            let _ = socket.finish().await;
        });
        let budget = minimal_budget(&["first", "pending"]) + 100;
        let cancel = Cancellation::new();
        let result = run_suite_with_budget(
            vec![
                SuiteJob {
                    scenario: scenario("first"),
                    bot_url: first_url,
                },
                SuiteJob {
                    scenario: scenario("pending"),
                    bot_url: pending_url,
                },
            ],
            1,
            options,
            cancel.clone(),
            budget,
        )
        .await
        .unwrap();
        assert!(cancel.is_cancelled());
        assert_eq!(result.runs[0].failures[0].kind, "limit_exceeded");
        assert!(!result.passed);
        assert!(serialized_size(&result).unwrap() <= budget);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), pending.accept())
                .await
                .is_err()
        );
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}
