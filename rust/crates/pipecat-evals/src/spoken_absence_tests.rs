//! Pinned matcher.py continuation semantics at the native session boundary.
//! Comparison tests inject instants; judge ordering uses an explicit gate, not sleeps.
use super::*;
use pipecat_classifiers::{
    ChoiceResult, ClassificationBatch, ClassificationMetrics, Classifier, ClassifierQuestion,
    ClassifierResult, ProbabilityProvenance, Questions, YesNoResult,
};
use std::{future::Future, pin::Pin, task::Poll};
use tokio::sync::Notify;

const WAIT: Duration = Duration::from_secs(2);

fn session(limits: EventLimits) -> EvalSession {
    let scenario = crate::ScenarioFile::from_value(json!({
        "name": "spoken continuation boundary", "turns": []
    }))
    .unwrap()
    .scenarios
    .remove(0);
    EvalSession::new(
        scenario,
        "ws://127.0.0.1:1",
        SessionOptions {
            event_limits: limits,
            ..Default::default()
        },
    )
    .unwrap()
}

fn expectation(value: Value) -> Expectation {
    serde_json::from_value(value).unwrap()
}

fn ingest(session: &mut EvalSession, kind: &str, data: Value) {
    session
        .ingest(Packet::Text(
            json!({"label":"rtvi-ai", "type":kind, "data":data}).to_string(),
        ))
        .unwrap();
}

fn spoken(session: &mut EvalSession, text: &str) {
    session
        .stream
        .transcribed_response(session.stream.epoch(), text.into())
        .unwrap();
}

fn generated(session: &mut EvalSession, text: &str) {
    ingest(session, "bot-llm-text", json!({"text":text}));
    ingest(session, "bot-llm-stopped", json!({}));
}

fn function(session: &mut EvalSession, name: &str) {
    ingest(
        session,
        "llm-function-call-in-progress",
        json!({"function_name":name, "arguments":{"city":"Paris"}}),
    );
}

fn buffer_call(session: &mut EvalSession, name: &str) {
    let mut unrelated = PendingCheck::for_session(expectation(json!({
        "event":"function_call", "calls":[{"name":"different_function"}]
    })));
    assert_eq!(
        session
            .matcher
            .feed(
                &mut unrelated,
                json!({"type":"function_call", "name":name, "args":{"city":"Paris"}}),
            )
            .unwrap(),
        MatchOutcome::Pending
    );
}

async fn close(session: &mut EvalSession, peer: WebSocketSession) {
    let client = session.socket.take().unwrap();
    let (client, peer) = tokio::join!(client.cancel(), peer.cancel());
    assert!(client.is_ok(), "client close: {client:?}");
    assert!(peer.is_ok(), "peer close: {peer:?}");
}

fn failure(outcome: MatchOutcome, expected_kind: &str, expected_text: &str) {
    let MatchOutcome::Failed { kind, reason } = outcome else {
        panic!("expected {expected_kind}, got {outcome:?}");
    };
    assert_eq!(kind, expected_kind);
    assert!(reason.contains(expected_text), "{reason:?}");
}

#[test]
fn missing_remote_starts_are_not_a_new_reply_even_before_first_match() {
    let mut session = session(EventLimits::default());
    assert_eq!(session.last_positive_match_at, None);
    assert!(!session.reply_began_after_last_match());
    // Local VAD, response arrival, and other events are deliberately not starts.
    for kind in ["vad_user_started_speaking", "response", "llm_response"] {
        session
            .stream
            .latest_event_times
            .insert(kind.into(), Instant::now());
    }
    assert!(!session.reply_began_after_last_match());
    for kind in ["llm_started", "bot_started_speaking"] {
        session.stream.latest_event_times.clear();
        session
            .stream
            .latest_event_times
            .insert(kind.into(), Instant::now());
        assert!(session.reply_began_after_last_match(), "{kind}");
    }
}

#[test]
fn remote_start_comparison_is_strict_and_uses_either_signal() {
    let mut session = session(EventLimits::default());
    let boundary = Instant::now();
    session.last_positive_match_at = Some(boundary);
    assert!(!session.reply_began_after_last_match());
    for kind in ["llm_started", "bot_started_speaking"] {
        for (start, expected) in [
            (boundary - Duration::from_secs(1), false),
            (boundary, false),
            (boundary + Duration::from_nanos(1), true),
        ] {
            session.stream.latest_event_times.clear();
            session.stream.latest_event_times.insert(kind.into(), start);
            assert_eq!(session.reply_began_after_last_match(), expected, "{kind}");
        }
    }
    session
        .stream
        .latest_event_times
        .insert("llm_started".into(), boundary);
    session.stream.latest_event_times.insert(
        "bot_started_speaking".into(),
        boundary + Duration::from_nanos(1),
    );
    assert!(session.reply_began_after_last_match());
}

#[tokio::test]
async fn ignored_continuations_wait_for_original_deadline_and_remain_charged() {
    let limits = EventLimits {
        max_events: 3,
        max_text_bytes: 32,
        ..Default::default()
    };
    let mut session = session(limits);
    let (client, peer) = super::recognition_ownership_tests::socket_pair().await;
    session.socket = Some(client);
    let boundary = Instant::now();
    session.last_positive_match_at = Some(boundary);
    session
        .stream
        .latest_event_times
        .insert("llm_started".into(), boundary);
    spoken(&mut session, "continued second sentence");
    spoken(&mut session, "continued third sentence");
    let retained = session.stream.events_seen.clone();
    let deadline = Instant::now() + Duration::from_millis(30);
    // An intentionally larger nominal window must never replace this caller's
    // already-clipped turn/session deadline after an ignored segment.
    let mut matching = Box::pin(session.match_expectation(
        expectation(json!({"event":"response", "absent":true, "within_ms":60_000})),
        deadline,
    ));
    let first_poll = std::future::poll_fn(|cx| Poll::Ready(matching.as_mut().poll(cx))).await;
    let completion = if first_poll.is_pending() {
        Some(tokio::time::timeout_at(deadline + WAIT, &mut matching).await)
    } else {
        None
    };
    drop(matching);
    let finished = Instant::now();
    close(&mut session, peer).await;

    assert!(
        first_poll.is_pending(),
        "continuation is not immediate success/failure"
    );
    let (outcome, judgments) = completion.unwrap().unwrap().unwrap();
    assert!(matches!(outcome, MatchOutcome::Passed(_)), "{outcome:?}");
    assert!(judgments.is_empty());
    assert!(finished >= deadline);
    assert_eq!(session.last_positive_match_at, Some(boundary));
    assert_eq!(session.stream.events_seen, retained);
    assert!(session.stream.pop().is_none());
    spoken(&mut session, "third retained event");
    assert!(matches!(
        session
            .stream
            .transcribed_response(session.stream.epoch(), "over count".into()),
        Err(EvalError::Limit("event history"))
    ));
    assert!(matches!(
        session
            .stream
            .transcribed_response(session.stream.epoch(), "x".repeat(33)),
        Err(EvalError::Limit("audio response text"))
    ));
}

#[tokio::test]
async fn consumed_continuation_does_not_refund_event_history_bytes() {
    let mut session = session(EventLimits {
        max_event_bytes: 256,
        max_text_bytes: 256,
        ..Default::default()
    });
    let (client, peer) = super::recognition_ownership_tests::socket_pair().await;
    session.socket = Some(client);
    spoken(&mut session, &"x".repeat(160));
    let before = session.stream.events_seen.clone();
    let matched = session
        .match_expectation(
            expectation(json!({"event":"response", "absent":true})),
            Instant::now() + Duration::from_millis(20),
        )
        .await;
    close(&mut session, peer).await;
    assert!(matches!(matched.unwrap().0, MatchOutcome::Passed(_)));
    assert_eq!(session.stream.events_seen, before);
    assert!(session.stream.pop().is_none());
    assert!(matches!(
        session
            .stream
            .transcribed_response(session.stream.epoch(), "y".repeat(160)),
        Err(EvalError::Limit("event history"))
    ));
}

#[tokio::test]
async fn no_observed_start_response_is_consumed_until_deadline_not_literal_silence() {
    let mut session = session(EventLimits::default());
    let (client, peer) = super::recognition_ownership_tests::socket_pair().await;
    session.socket = Some(client);
    spoken(&mut session, "unattributed first response");
    let deadline = Instant::now() + Duration::from_millis(20);
    let matched = session
        .match_expectation(
            expectation(json!({"event":"response", "absent":true})),
            deadline,
        )
        .await;
    let completed = Instant::now();
    close(&mut session, peer).await;
    assert!(matches!(matched.unwrap().0, MatchOutcome::Passed(_)));
    assert!(completed >= deadline);
    assert_eq!(session.last_positive_match_at, None);
    assert!(session.stream.pop().is_none());
    assert_eq!(
        session.stream.events_seen[0]["text"],
        "unattributed first response"
    );
}

#[tokio::test]
async fn either_fresh_remote_start_rejects_spoken_absence_with_content() {
    for kind in ["llm_started", "bot_started_speaking"] {
        for prior_match in [false, true] {
            let mut session = session(EventLimits::default());
            let boundary = Instant::now();
            session.last_positive_match_at = prior_match.then_some(boundary);
            session
                .stream
                .latest_event_times
                .insert(kind.into(), boundary + Duration::from_nanos(1));
            spoken(&mut session, "unexpected new reply");
            let (outcome, _) = session
                .match_expectation(
                    expectation(json!({"event":"response", "absent":true})),
                    Instant::now() + WAIT,
                )
                .await
                .unwrap();
            failure(outcome, "unexpected_event", "unexpected new reply");
            assert_eq!(
                session.last_positive_match_at,
                prior_match.then_some(boundary)
            );
        }
    }
}

#[tokio::test]
async fn already_ingested_later_start_affects_an_older_queued_response() {
    let mut session = session(EventLimits::default());
    let boundary = Instant::now();
    session.last_positive_match_at = Some(boundary);
    spoken(&mut session, "queued before the new start");
    ingest(&mut session, "bot-started-speaking", json!({}));
    session.stream.latest_event_times.insert(
        "bot_started_speaking".into(),
        boundary + Duration::from_nanos(1),
    );
    let (outcome, _) = session
        .match_expectation(
            expectation(json!({"event":"response", "absent":true})),
            Instant::now() + WAIT,
        )
        .await
        .unwrap();
    failure(outcome, "unexpected_event", "queued before the new start");
    // Failure happened at the older candidate, before consuming the start.
    assert_eq!(
        session.stream.pop().unwrap()["type"],
        "bot_started_speaking"
    );
    assert_eq!(session.last_positive_match_at, Some(boundary));
}

#[tokio::test]
async fn ordinary_and_aggregate_positive_matches_advance_the_same_boundary() {
    let mut session = session(EventLimits::default());
    let old = Instant::now() - Duration::from_secs(1);
    session.last_positive_match_at = Some(old);
    for (wire, friendly, data) in [
        (
            "user-transcription",
            "user_transcription",
            json!({"text":"heard user", "final":true}),
        ),
        ("bot-started-speaking", "bot_started_speaking", json!({})),
        ("bot-llm-started", "llm_started", json!({})),
    ] {
        ingest(&mut session, wire, data);
        let before = Instant::now();
        let (outcome, _) = session
            .match_expectation(expectation(json!({"event":friendly})), before + WAIT)
            .await
            .unwrap();
        assert!(matches!(outcome, MatchOutcome::Passed(_)));
        assert!(session.last_positive_match_at.unwrap() >= before);
    }
    spoken(&mut session, "first");
    spoken(&mut session, "second");
    let before = Instant::now();
    let (outcome, _) = session
        .match_expectation(
            expectation(json!({"event":"response", "text_contains":"first second"})),
            before + WAIT,
        )
        .await
        .unwrap();
    assert_eq!(outcome, MatchOutcome::Passed("first second".into()));
    assert!(session.last_positive_match_at.unwrap() >= before);
    assert!(session.last_positive_match_at.unwrap() > old);
}

#[tokio::test]
async fn complete_function_sets_and_buffered_calls_advance_the_boundary() {
    for buffered in [false, true] {
        let mut session = session(EventLimits::default());
        let old = Instant::now() - Duration::from_secs(1);
        session.last_positive_match_at = Some(old);
        for name in ["weather", "time"] {
            if buffered {
                buffer_call(&mut session, name);
            } else {
                function(&mut session, name);
            }
        }
        let before = Instant::now();
        let (outcome, _) = session
            .match_expectation(
                expectation(
                    json!({"event":"function_call", "calls":[{"name":"weather"}, {"name":"time"}]}),
                ),
                before + WAIT,
            )
            .await
            .unwrap();
        assert_eq!(outcome, MatchOutcome::Passed("weather, time".into()));
        assert!(session.last_positive_match_at.unwrap() >= before);
        assert!(session.last_positive_match_at.unwrap() > old);
    }
}

#[tokio::test]
async fn failure_timeout_and_successful_absence_never_advance_the_boundary() {
    let mut session = session(EventLimits::default());
    let old = Instant::now() - Duration::from_secs(1);
    session.last_positive_match_at = Some(old);
    spoken(&mut session, "forbidden text");
    let failed = session
        .match_expectation(
            expectation(json!({"event":"response", "text_excludes":"forbidden"})),
            Instant::now() + WAIT,
        )
        .await
        .unwrap();
    failure(failed.0, "text_present", "forbidden");
    assert_eq!(session.last_positive_match_at, Some(old));
    let timeout = session
        .match_expectation(expectation(json!({"event":"response"})), Instant::now())
        .await
        .unwrap();
    assert!(matches!(timeout.0, MatchOutcome::Failed { .. }));
    assert_eq!(session.last_positive_match_at, Some(old));
    let absent = session
        .match_expectation(
            expectation(json!({"event":"response", "absent":true})),
            Instant::now(),
        )
        .await
        .unwrap();
    assert!(matches!(absent.0, MatchOutcome::Passed(_)));
    assert_eq!(session.last_positive_match_at, Some(old));
}

#[test]
fn turn_reset_and_interruption_preserve_boundary_and_stale_epoch_rejection() {
    let mut session = session(EventLimits::default());
    let boundary = Instant::now();
    session.last_positive_match_at = Some(boundary);
    buffer_call(&mut session, "weather");
    session.matcher.reset_turn();
    session.turn_boundary().unwrap();
    assert_eq!(session.last_positive_match_at, Some(boundary));
    let old_epoch = session.stream.epoch();
    ingest(&mut session, "bot-interrupted", json!({}));
    assert_eq!(session.last_positive_match_at, Some(boundary));
    let retained = session.stream.events_seen.clone();
    session
        .stream
        .transcribed_response(old_epoch, "stale segment".into())
        .unwrap();
    assert_eq!(session.stream.events_seen, retained);
    let mut cleared = PendingCheck::for_session(expectation(json!({
        "event":"function_call", "calls":[{"name":"weather"}]
    })));
    assert_eq!(
        session.matcher.buffered(&mut cleared),
        MatchOutcome::Pending
    );
}

#[tokio::test]
async fn resolved_text_and_nonresponse_absence_remain_unconditional() {
    let parsed = crate::ScenarioFile::from_value(json!({
        "name":"text normalization", "turns":[{"expect":[{"event":"response", "absent":true}]}]
    }))
    .unwrap();
    let text_absence = parsed.scenarios[0].turns[0].expect[0].clone();
    assert_eq!(text_absence.event, "llm_response");
    let mut session = session(EventLimits::default());
    session.last_positive_match_at = Some(Instant::now());
    generated(&mut session, "text is always unexpected");
    let (outcome, _) = session
        .match_expectation(text_absence, Instant::now() + WAIT)
        .await
        .unwrap();
    failure(outcome, "unexpected_event", "text is always unexpected");
    ingest(
        &mut session,
        "user-transcription",
        json!({"text":"user spoke", "final":true}),
    );
    let (outcome, _) = session
        .match_expectation(
            expectation(json!({"event":"user_transcription", "absent":true})),
            Instant::now() + WAIT,
        )
        .await
        .unwrap();
    failure(outcome, "unexpected_event", "user spoke");
}

#[test]
fn context_free_matcher_does_not_invent_a_spoken_start_boundary() {
    let mut matcher = ExpectationMatcher::new(1024, 4);
    let mut check = PendingCheck::new(expectation(json!({"event":"response", "absent":true})));
    failure(
        matcher
            .feed(
                &mut check,
                json!({"type":"response", "text":"context free"}),
            )
            .unwrap(),
        "unexpected_event",
        "context free",
    );
}

#[derive(Default)]
struct JudgeGate {
    release: Notify,
    entered: Mutex<usize>,
    accepted_at: Mutex<Option<Instant>>,
}

struct GatedClassifier {
    gate: Arc<JudgeGate>,
    decision: JudgeDecision,
}

// Match async-trait's object-safe expansion without adding a dev dependency.
impl Classifier for GatedClassifier {
    fn ask<'a, 'b, 'c, 'f>(
        &'a self,
        _state: &'b Value,
        questions: &'c Questions,
        _options: CallOptions,
    ) -> Pin<Box<dyn Future<Output = pipecat_classifiers::Result<ClassificationBatch>> + Send + 'f>>
    where
        'a: 'f,
        'b: 'f,
        'c: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            *self.gate.entered.lock().unwrap() += 1;
            self.gate.release.notified().await;
            *self.gate.accepted_at.lock().unwrap() = Some(Instant::now());
            let choice = match self.decision {
                JudgeDecision::Yes => "yes",
                JudgeDecision::No => "no",
                JudgeDecision::Continue => "continue",
            };
            let answer = match &questions["verdict"] {
                ClassifierQuestion::Choice(question) => ClassifierResult::Choice(ChoiceResult {
                    choice: choice.into(),
                    probabilities: question
                        .options
                        .keys()
                        .map(|key| (key.clone(), if key == choice { 1.0 } else { 0.0 }))
                        .collect(),
                    confidence: 1.0,
                }),
                ClassifierQuestion::YesNo(_) => ClassifierResult::YesNo(YesNoResult {
                    probability: if self.decision == JudgeDecision::Yes {
                        1.0
                    } else {
                        0.0
                    },
                }),
                ClassifierQuestion::Score(_) => panic!("unexpected score question"),
            };
            Ok(ClassificationBatch {
                answers: BTreeMap::from([("verdict".into(), answer)]),
                metrics: ClassificationMetrics {
                    classifier: "gated test classifier".into(),
                    model: None,
                    elapsed: Duration::ZERO,
                    usage: None,
                    provenance: ProbabilityProvenance::LlmEstimate,
                },
            })
        })
    }
}

async fn judged_boundary(decision: JudgeDecision, buffered_call: bool) {
    let mut session = session(EventLimits::default());
    let (client, peer) = super::recognition_ownership_tests::socket_pair().await;
    session.socket = Some(client);
    let gate = Arc::new(JudgeGate::default());
    session.judge = Some(
        EvalJudge::new(
            Arc::new(GatedClassifier {
                gate: gate.clone(),
                decision,
            }),
            crate::JudgeOptions {
                no_grace: Duration::ZERO,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let old = Instant::now() - Duration::from_secs(1);
    session.last_positive_match_at = Some(old);
    let expected = if buffered_call {
        buffer_call(&mut session, "weather");
        expectation(
            json!({"event":"function_call", "calls":[{"name":"weather"}], "eval":"valid lookup"}),
        )
    } else {
        generated(&mut session, "candidate answer");
        expectation(
            json!({"event":"llm_response", "text_contains":"candidate", "eval":"complete answer"}),
        )
    };
    let deadline = Instant::now()
        + if decision == JudgeDecision::Continue {
            Duration::from_millis(100)
        } else {
            WAIT
        };
    let mut matching = Box::pin(session.match_expectation(expected, deadline));
    let first_poll = std::future::poll_fn(|cx| Poll::Ready(matching.as_mut().poll(cx))).await;
    let entered_before_release = *gate.entered.lock().unwrap();
    let released_at = Instant::now();
    gate.release.notify_one();
    let completion = if first_poll.is_pending() {
        Some(tokio::time::timeout_at(deadline + WAIT, &mut matching).await)
    } else {
        None
    };
    drop(matching);
    close(&mut session, peer).await;

    assert!(
        first_poll.is_pending(),
        "judge must not complete before the gate opens"
    );
    assert_eq!(entered_before_release, 1);
    let accepted_at = gate.accepted_at.lock().unwrap().unwrap();
    assert!(accepted_at >= released_at);
    let (outcome, verdicts) = completion.unwrap().unwrap().unwrap();
    assert_eq!(verdicts.len(), 1);
    assert_eq!(verdicts[0].decision, decision);
    if decision == JudgeDecision::Yes {
        assert!(matches!(outcome, MatchOutcome::Passed(_)), "{outcome:?}");
        assert!(session.last_positive_match_at.unwrap() >= accepted_at,
            "positive boundary must be stamped after accepted Yes, never preliminary text/call match");
        assert!(session.last_positive_match_at.unwrap() > old);
    } else {
        let kind = if decision == JudgeDecision::No {
            "judge_no"
        } else {
            "judge_continue"
        };
        failure(outcome, kind, "judge");
        assert_eq!(session.last_positive_match_at, Some(old));
    }
}

#[tokio::test]
async fn judge_yes_stamps_after_acceptance_not_preliminary_reply_match() {
    judged_boundary(JudgeDecision::Yes, false).await;
}

#[tokio::test]
async fn judge_no_and_continue_do_not_advance_positive_boundary() {
    judged_boundary(JudgeDecision::No, false).await;
    judged_boundary(JudgeDecision::Continue, false).await;
}

#[tokio::test]
async fn buffered_call_judge_yes_stamps_after_acceptance_and_no_does_not() {
    judged_boundary(JudgeDecision::Yes, true).await;
    judged_boundary(JudgeDecision::No, true).await;
}
