//! Native loopback integration counterparts to tests/test_evals_session.py.
use pipecat_evals::{
    matcher::ScriptResult,
    suite::{run_suite, SuiteJob},
    Cancellation, EvalSession, ScenarioFile, ScriptScenario, SessionOptions,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::Notify, task::JoinHandle};
type Step = (u64, Value);
struct Bot {
    url: String,
    task: JoinHandle<Vec<Value>>,
    input: Arc<Notify>,
    request: Arc<Mutex<String>>,
}
fn wire(kind: &str, data: Value) -> Value {
    json!({"label":"rtvi-ai","type":kind,"data":data})
}
fn reply(text: &str) -> Vec<Step> {
    vec![
        (0, wire("bot-llm-started", json!({}))),
        (0, wire("bot-llm-text", json!({"text":text}))),
        (0, wire("bot-llm-stopped", json!({}))),
    ]
}
fn scenario(turns: Value) -> ScriptScenario {
    ScenarioFile::from_value(json!({"name":"integration","turns":turns}))
        .unwrap()
        .scenarios
        .remove(0)
}
fn options() -> SessionOptions {
    SessionOptions {
        transport: SessionConfig {
            write_timeout: Duration::from_millis(200),
            close_timeout: Duration::from_millis(50),
            connect_timeout: Duration::from_millis(100),
            ..Default::default()
        },
        default_timeout: Duration::from_millis(80),
        handshake_timeout: Duration::from_millis(80),
        send_after_timeout: Duration::from_millis(80),
        bot_quiet_timeout: Duration::from_millis(80),
        max_duration: Duration::from_secs(2),
        ..Default::default()
    }
}
async fn bot(ready: bool, greeting: Vec<Step>, scripts: Vec<Vec<Step>>) -> Bot {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/eval?custom=kept", listener.local_addr().unwrap());
    let input = Arc::new(Notify::new());
    let signal = input.clone();
    let request = Arc::new(Mutex::new(String::new()));
    let request_out = request.clone();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = WebSocketSession::accept_with_request_policy(
            stream,
            options().transport,
            move |request| {
                *request_out.lock().unwrap() = request.uri().to_string();
                true
            },
        )
        .await
        .unwrap();
        let mut seen = Vec::new();
        let mut turns = 0;
        while let Some(packet) = socket.recv().await {
            let text = match packet {
                Packet::Text(s) => s,
                Packet::Binary(b) => String::from_utf8(b.to_vec()).unwrap(),
            };
            let message: Value = serde_json::from_str(&text).unwrap();
            seen.push(message.clone());
            let steps = match message["type"].as_str().unwrap() {
                "client-ready" => {
                    if ready {
                        let mut steps = vec![(0, wire("bot-ready", json!({"version":"2.2.0"})))];
                        steps.extend(greeting.clone());
                        steps
                    } else {
                        vec![]
                    }
                }
                "send-text" | "dtmf" => {
                    signal.notify_one();
                    let steps = scripts.get(turns).cloned().unwrap_or_default();
                    turns += 1;
                    steps
                }
                "client-message" if message["data"]["t"] == "eval-cancel" => break,
                _ => vec![],
            };
            for (delay, event) in steps {
                if delay > 0 {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                }
                if socket
                    .sender()
                    .send(Packet::Text(event.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
        let _ = socket.finish().await;
        seen
    });
    Bot {
        url,
        task,
        input,
        request,
    }
}
async fn finish(bot: Bot) -> Vec<Value> {
    tokio::time::timeout(Duration::from_secs(3), bot.task)
        .await
        .unwrap()
        .unwrap()
}
async fn run(s: ScriptScenario, bot: &Bot) -> ScriptResult {
    EvalSession::new(s, &bot.url, options())
        .unwrap()
        .run(Cancellation::new())
        .await
}
#[tokio::test]
async fn real_rtv_i_handshake_context_tools_marker_text_dtmf_and_stop_bot() {
    let mut scripts = vec![
        (
            0,
            wire(
                "llm-function-call-in-progress",
                json!({"function_name":"time","arguments":{}}),
            ),
        ),
        (
            0,
            wire(
                "llm-function-call-in-progress",
                json!({"function_name":"weather","arguments":{"city":"Paris","units":"C"}}),
            ),
        ),
        (0, wire("bot-llm-started", json!({}))),
        (0, wire("bot-llm-text", json!({"text":"Hello "}))),
        (0, wire("bot-llm-text", json!({"text":"world"}))),
        (
            0,
            wire(
                "bot-llm-marker",
                json!({"text":"●","kind":"complete","raw":"● Hello world","markers":["●","◐","○"]}),
            ),
        ),
        (0, wire("bot-llm-stopped", json!({}))),
    ];
    let b = bot(
        true,
        vec![],
        vec![
            std::mem::take(&mut scripts),
            vec![(
                0,
                wire(
                    "user-transcription",
                    json!({"text":"DTMF: 012#","final":true}),
                ),
            )],
        ],
    )
    .await;
    let s = ScenarioFile::load(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/script.yaml"
    ))
    .unwrap()
    .scenarios
    .remove(0);
    let mut opt = options();
    opt.stop_bot = true;
    let result = EvalSession::new(s, &b.url, opt)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert!(result.passed, "{result:?}");
    assert_eq!(result.turns.len(), 2);
    assert_eq!(result.turns[0].expectations[1].matched, "●");
    assert!(b.request.lock().unwrap().contains("skip_tts=true"));
    assert!(b.request.lock().unwrap().contains("custom=kept"));
    let seen = finish(b).await;
    assert_eq!(seen[0]["type"], "client-ready");
    assert_eq!(seen[1]["data"]["t"], "eval-configure");
    assert_eq!(seen[2]["data"]["t"], "eval-context");
    assert_eq!(
        seen[3]["data"]["options"],
        json!({"run_immediately":true,"audio_response":false})
    );
    assert_eq!(seen[4]["data"]["buttons"], json!(["0", "1", "2", "#"]));
    assert_eq!(seen.last().unwrap()["data"]["t"], "eval-cancel");
}
#[tokio::test]
async fn text_mismatch_stops_and_remaining_turns_are_not_run() {
    let b = bot(true, vec![], vec![reply("wrong")]).await;
    let s = scenario(
        json!([{"user":"hi","expect":[{"event":"response","text_contains":"right"}]},{"user":"never"}]),
    );
    let result = run(s, &b).await;
    assert!(!result.passed);
    assert_eq!(result.failures[0].kind, "text_mismatch");
    assert_eq!(result.turns[1].status, "not_run");
    assert_eq!(
        finish(b)
            .await
            .iter()
            .filter(|m| m["type"] == "send-text")
            .count(),
        1
    );
}
#[tokio::test]
async fn stop_on_failure_false_drives_and_scores_later_turns() {
    let b = bot(true, vec![], vec![reply("wrong"), reply("right")]).await;
    let mut s = scenario(
        json!([{"user":"one","expect":[{"event":"response","text_excludes":"wrong"}]},{"user":"two","expect":[{"event":"response","text_contains":"right"}]}]),
    );
    s.stop_on_failure = false;
    let result = run(s, &b).await;
    assert_eq!(result.turns[0].status, "failed");
    assert_eq!(result.turns[1].status, "passed");
    finish(b).await;
}
#[tokio::test]
async fn no_reply_times_out_and_does_not_turn_silence_into_a_pass() {
    let b = bot(true, vec![], vec![vec![]]).await;
    let result = run(
        scenario(json!([{"user":"hi","expect":[{"event":"response"}]}])),
        &b,
    )
    .await;
    assert_eq!(result.failures[0].kind, "timeout");
    assert_eq!(result.turns[0].expectations.len(), 1);
    finish(b).await;
}
#[tokio::test]
async fn expectations_share_one_turn_deadline() {
    let b = bot(
        true,
        vec![],
        vec![vec![(80, wire("bot-llm-started", json!({})))]],
    )
    .await;
    let s = scenario(
        json!([{"user":"hi","expect":[{"event":"llm_started","within_ms":120},{"event":"response","within_ms":120}]}]),
    );
    let result = run(s, &b).await;
    assert_eq!(result.failures[0].expectation_index, 1);
    assert!(
        result.turns[0].duration_ms < 180,
        "{}",
        result.turns[0].duration_ms
    );
    finish(b).await;
}
#[tokio::test]
async fn absence_has_a_real_quiet_window_and_rejects_unexpected_output() {
    let b = bot(true, vec![], vec![vec![]]).await;
    let result = run(
        scenario(
            json!([{"user":"hi","expect":[{"event":"response","absent":true,"within_ms":30}]}]),
        ),
        &b,
    )
    .await;
    assert!(result.passed);
    assert!(result.turns[0].duration_ms >= 25);
    finish(b).await;
    let b = bot(true, vec![], vec![reply("unexpected")]).await;
    let result = run(
        scenario(
            json!([{"user":"hi","expect":[{"event":"response","absent":true,"within_ms":100}]}]),
        ),
        &b,
    )
    .await;
    assert_eq!(result.failures[0].kind, "unexpected_event");
    finish(b).await;
}
#[tokio::test]
async fn cancellation_closes_live_socket_and_transmits_requested_eval_cancel() {
    let b = bot(true, vec![], vec![]).await;
    let cancel = Cancellation::new();
    let mut opt = options();
    opt.stop_bot = true;
    let s = scenario(
        json!([{"user":"hi","expect":[{"event":"response","within_ms":5000}]},{"user":"never"}]),
    );
    let session = EvalSession::new(s, &b.url, opt).unwrap();
    let c = cancel.clone();
    let running = tokio::spawn(session.run(c));
    b.input.notified().await;
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_secs(1), running)
        .await
        .unwrap()
        .unwrap();
    assert!(!result.passed);
    assert_eq!(result.failures[0].kind, "cancelled");
    assert_eq!(result.turns[0].status, "failed");
    assert_eq!(result.turns[1].status, "not_run");
    let seen = finish(b).await;
    assert!(seen.iter().any(|m| m["data"]["t"] == "eval-cancel"));
}
#[tokio::test]
async fn dropping_session_future_closes_transport_without_detached_reader() {
    let b = bot(true, vec![], vec![]).await;
    let s = scenario(json!([{"user":"hi","expect":[{"event":"response","within_ms":5000}]}]));
    let running = tokio::spawn(
        EvalSession::new(s, &b.url, options())
            .unwrap()
            .run(Cancellation::new()),
    );
    b.input.notified().await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    finish(b).await;
}
#[tokio::test]
async fn handshake_timeout_is_distinct_and_preserves_not_run_turns() {
    let b = bot(false, vec![], vec![]).await;
    let result = run(scenario(json!([{"user":"never"}])), &b).await;
    assert_eq!(result.failures[0].kind, "handshake_timeout");
    assert_eq!(result.failures[0].turn_index, -1);
    assert_eq!(result.turns[0].status, "not_run");
    assert_eq!(finish(b).await.len(), 1);
}
#[tokio::test]
async fn refused_connect_and_pre_cancel_are_diagnostic() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    drop(listener);
    let s = scenario(json!([{"user":"hi"}]));
    let result = EvalSession::new(s.clone(), &url, options())
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.failures[0].kind, "connect_failed");
    let cancel = Cancellation::new();
    cancel.cancel();
    let result = EvalSession::new(s, &url, options())
        .unwrap()
        .run(cancel)
        .await;
    assert_eq!(result.failures[0].kind, "cancelled");
}
#[tokio::test]
async fn send_after_event_and_delay_work_and_missing_anchor_fails() {
    let b = bot(
        true,
        vec![(0, wire("vad-user-stopped-speaking", json!({})))],
        vec![reply("ok")],
    )
    .await;
    let s = scenario(
        json!([{"user":"hi","send_after":{"event":"vad_user_stopped_speaking","delay_ms":20},"expect":[{"event":"response","text_contains":"ok"}]}]),
    );
    let result = run(s, &b).await;
    assert!(result.passed, "{result:?}");
    assert!(result.duration_ms >= 15);
    let seen = finish(b).await;
    assert_eq!(seen[1]["data"]["d"]["vad_user_speaking"], true);
    let b = bot(true, vec![], vec![]).await;
    let s = scenario(json!([{"user":"hi","send_after":{"event":"llm_started"}}]));
    let result = run(s, &b).await;
    assert_eq!(result.failures[0].kind, "send_after_timeout");
    finish(b).await;
}
#[tokio::test]
async fn prior_queued_reply_and_interrupted_stragglers_cannot_satisfy_new_turn() {
    let b = bot(true, vec![], vec![reply("stale"), vec![]]).await;
    let s = scenario(
        json!([{"user":"one","expect":[{"event":"llm_started"}]},{"user":"two","expect":[{"event":"response","text_contains":"stale"}]}]),
    );
    let result = run(s, &b).await;
    assert_eq!(result.turns[1].status, "failed");
    finish(b).await;
    let b = bot(
        true,
        vec![],
        vec![vec![
            (0, wire("bot-llm-started", json!({}))),
            (0, wire("bot-llm-text", json!({"text":"stale"}))),
            (0, wire("bot-interrupted", json!({}))),
            (0, wire("bot-llm-text", json!({"text":"tail"}))),
            (0, wire("bot-llm-stopped", json!({}))),
        ]],
    )
    .await;
    let result = run(
        scenario(json!([{"user":"one","expect":[{"event":"response"}]}])),
        &b,
    )
    .await;
    assert_eq!(result.failures[0].kind, "timeout");
    assert!(!result
        .events_seen
        .iter()
        .any(|v| v["type"] == "llm_response"));
    finish(b).await;
}
#[tokio::test]
async fn received_image_metadata_and_capture_flag_are_source_compatible() {
    let b = bot(
        true,
        vec![],
        vec![vec![(
            0,
            wire(
                "eval-bot-image",
                json!({"width":12,"height":34,"format":"RGB"}),
            ),
        )]],
    )
    .await;
    let result = run(
        scenario(json!([{"user":"image","expect":[{"event":"image"}]}])),
        &b,
    )
    .await;
    assert!(result.passed);
    assert!(b
        .request
        .lock()
        .unwrap()
        .contains("capture_bot_images=true"));
    assert_eq!(result.events_seen[0]["width"], 12);
    finish(b).await;
}
#[tokio::test]
async fn traffic_and_history_limits_fail_instead_of_truncating_evidence() {
    let events = (0..10)
        .map(|_| (0, wire("bot-started-speaking", json!({}))))
        .collect();
    let b = bot(true, vec![], vec![events]).await;
    let mut opt = options();
    opt.event_limits.max_events = 2;
    let result = EvalSession::new(
        scenario(json!([{"user":"hi","expect":[{"event":"response"}]}])),
        &b.url,
        opt,
    )
    .unwrap()
    .run(Cancellation::new())
    .await;
    assert_eq!(result.failures[0].kind, "limit_exceeded");
    assert!(result.events_seen.len() <= 2);
    finish(b).await;
}
#[tokio::test]
async fn concurrent_suite_reports_ordered_results_and_cancels_queued_jobs() {
    let b1 = bot(true, vec![], vec![]).await;
    let b2 = bot(true, vec![], vec![]).await;
    let pending_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let s = scenario(json!([{"user":"hi","expect":[{"event":"response","within_ms":5000}]}]));
    let jobs = vec![
        SuiteJob {
            scenario: s.clone(),
            bot_url: b1.url.clone(),
        },
        SuiteJob {
            scenario: s.clone(),
            bot_url: b2.url.clone(),
        },
        SuiteJob {
            scenario: s,
            bot_url: format!("ws://{}", pending_listener.local_addr().unwrap()),
        },
    ];
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let running = tokio::spawn(run_suite(jobs, 2, options(), c));
    b1.input.notified().await;
    b2.input.notified().await;
    cancel.cancel();
    let result = running.await.unwrap().unwrap();
    assert!(!result.passed);
    assert_eq!(result.runs.len(), 3);
    assert!(result
        .runs
        .iter()
        .all(|r| r.failures[0].kind == "cancelled"));
    assert_eq!(result.runs[2].turns[0].status, "not_run");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), pending_listener.accept())
            .await
            .is_err()
    );
    finish(b1).await;
    finish(b2).await;
}
#[tokio::test]
async fn suite_collects_real_pass_and_fail_without_cancelling_other_sessions() {
    let b1 = bot(true, vec![], vec![reply("ok")]).await;
    let b2 = bot(true, vec![], vec![reply("bad")]).await;
    let s = scenario(json!([{"user":"hi","expect":[{"event":"response","text_contains":"ok"}]}]));
    let result = run_suite(
        vec![
            SuiteJob {
                scenario: s.clone(),
                bot_url: b1.url.clone(),
            },
            SuiteJob {
                scenario: s,
                bot_url: b2.url.clone(),
            },
        ],
        2,
        options(),
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert!(!result.passed);
    assert!(result.runs[0].passed);
    assert!(!result.runs[1].passed);
    finish(b1).await;
    finish(b2).await;
}

#[tokio::test]
async fn absolute_session_deadline_bounds_long_expectations() {
    let b = bot(true, vec![], vec![]).await;
    let mut opt = options();
    opt.max_duration = Duration::from_millis(70);
    let result = EvalSession::new(
        scenario(json!([{"user":"hi","expect":[{"event":"response","within_ms":5000}]}])),
        &b.url,
        opt,
    )
    .unwrap()
    .run(Cancellation::new())
    .await;
    assert_eq!(result.failures[0].kind, "timeout");
    assert!(result.failures[0].reason.contains("session"));
    assert!(result.duration_ms < 500);
    assert!(result.turns[0].duration_ms > 0);
    finish(b).await;
}
#[tokio::test]
async fn unknown_protocol_messages_still_count_toward_traffic_limits() {
    // This checks traffic accounting, not an 80-ms scheduling deadline. Keep
    // the product limits unchanged and make actual peer progress observable.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/eval", listener.local_addr().unwrap());
    let mut peer = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = WebSocketSession::accept(stream, options().transport)
            .await
            .unwrap();
        let Packet::Text(ready) = socket.recv().await.expect("peer: client-ready") else {
            panic!("peer: client-ready must be text");
        };
        let ready: Value = serde_json::from_str(&ready).unwrap();
        assert_eq!(ready["type"], "client-ready", "peer: handshake {ready}");
        socket
            .sender()
            .send(Packet::Text(
                wire("bot-ready", json!({"version":"2.2.0"})).to_string(),
            ))
            .await
            .unwrap();
        socket.sender().flush().await.unwrap();
        let Packet::Text(user) = socket.recv().await.expect("peer: send-text") else {
            panic!("peer: send-text must be text");
        };
        let user: Value = serde_json::from_str(&user).unwrap();
        assert_eq!(user["type"], "send-text", "peer: user input {user}");
        assert_eq!(user["data"]["content"], "hi");
        // bot-ready plus this flushed prefix equals the five-message budget.
        // The next unknown frame must cross it, even without a friendly event.
        for _ in 0..4 {
            socket
                .sender()
                .send(Packet::Text(wire("irrelevant", json!({})).to_string()))
                .await
                .unwrap();
        }
        let prefix_flush = socket.sender().flush().await;
        let mut admitted = 4;
        for _ in 4..20 {
            if socket
                .sender()
                .send(Packet::Text(wire("irrelevant", json!({})).to_string()))
                .await
                .is_err()
            {
                break;
            }
            admitted += 1;
        }
        // The client deliberately closes at overflow; do not require the
        // remaining burst to flush after that valid termination.
        let tail_flush = socket.sender().flush().await;
        let cleanup = socket.finish_and_join().await;
        (user, admitted, prefix_flush, tail_flush, cleanup)
    });
    let mut opt = options();
    opt.max_wire_messages = 5;
    let result = EvalSession::new(
        scenario(json!([{"user":"hi","expect":[{"event":"response","within_ms":1000}]}])),
        &url,
        opt,
    )
    .unwrap()
    .run(Cancellation::new())
    .await;
    // Join the actual peer before any assertion can unwind the test runtime.
    let evidence = match tokio::time::timeout(Duration::from_secs(3), &mut peer).await {
        Ok(joined) => joined.expect("peer: handshake/send-text/burst task"),
        Err(_) => {
            peer.abort();
            let _ = peer.await;
            panic!("peer: bounded join expired; result: {result:?}");
        }
    };
    assert!(
        evidence.2.is_ok(),
        "peer prefix evidence: {evidence:?}; result: {result:?}"
    );
    assert!(
        evidence.1 >= 5,
        "peer crossing admission: {evidence:?}; result: {result:?}"
    );
    assert!(
        !result.failures.is_empty(),
        "result: {result:?}; peer: {evidence:?}"
    );
    assert_eq!(
        result.failures[0].kind, "limit_exceeded",
        "result: {result:?}; peer: {evidence:?}"
    );
    assert_eq!(
        result.failures[0].reason, "evaluation limit exceeded: wire message traffic",
        "result: {result:?}; peer: {evidence:?}"
    );
    assert!(
        result.events_seen.is_empty(),
        "result: {result:?}; peer: {evidence:?}"
    );
}
#[tokio::test]
async fn disconnect_is_failure_even_for_an_absence_assertion() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = WebSocketSession::accept(stream, options().transport)
            .await
            .unwrap();
        socket.recv().await.unwrap();
        socket
            .sender()
            .send(Packet::Text(wire("bot-ready", json!({})).to_string()))
            .await
            .unwrap();
        socket.recv().await.unwrap();
        let _ = socket.finish().await;
    });
    let s = scenario(
        json!([{"user":"hi","expect":[{"event":"response","absent":true,"within_ms":500}]}]),
    );
    let result = EvalSession::new(s, &url, options())
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.failures[0].kind, "disconnected");
    server.await.unwrap();
}
#[tokio::test]
async fn dropping_suite_aborts_all_owned_sessions() {
    let b1 = bot(true, vec![], vec![]).await;
    let b2 = bot(true, vec![], vec![]).await;
    let s = scenario(json!([{"user":"hi","expect":[{"event":"response","within_ms":5000}]}]));
    let jobs = vec![
        SuiteJob {
            scenario: s.clone(),
            bot_url: b1.url.clone(),
        },
        SuiteJob {
            scenario: s,
            bot_url: b2.url.clone(),
        },
    ];
    let running = tokio::spawn(run_suite(jobs, 2, options(), Cancellation::new()));
    b1.input.notified().await;
    b2.input.notified().await;
    running.abort();
    assert!(running.await.unwrap_err().is_cancelled());
    finish(b1).await;
    finish(b2).await;
}

#[tokio::test]
async fn first_observing_turn_sees_greeting_without_waiting_for_bot_quiet() {
    let b = bot(
        true,
        vec![(0, wire("bot-started-speaking", json!({})))],
        vec![],
    )
    .await;
    let result = run(
        scenario(json!([{"expect":[{"event":"bot_started_speaking"}]}])),
        &b,
    )
    .await;
    assert!(result.passed);
    assert!(result.turns[0].duration_ms < 60);
    finish(b).await;
}
#[tokio::test]
async fn failed_send_after_can_continue_when_stop_on_failure_is_false() {
    let b = bot(true, vec![], vec![reply("later")]).await;
    let mut s = scenario(
        json!([{"user":"never","send_after":{"event":"llm_started"}},{"user":"later","expect":[{"event":"response","text_contains":"later"}]}]),
    );
    s.stop_on_failure = false;
    let result = run(s, &b).await;
    assert_eq!(result.turns[0].failures[0].kind, "send_after_timeout");
    assert_eq!(result.turns[0].failures[0].event_name, "llm_started");
    assert_eq!(result.turns[1].status, "passed");
    finish(b).await;
}
