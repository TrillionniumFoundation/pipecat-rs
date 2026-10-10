//! Localhost-only integration of source scripted judging with native Jev and RTVI.
use pipecat_classifiers::{CallOptions, CancellationToken, JevClassifier, JevConfig};
use pipecat_evals::{
    matcher::ScriptResult, Cancellation, EvalError, EvalJudge, EvalSession, JudgeDecision,
    JudgeOptions, ScenarioFile, ScriptScenario, SessionOptions,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::Notify,
    task::{JoinHandle, JoinSet},
};

#[derive(Clone)]
struct Reply {
    status: u16,
    body: String,
    delay: Duration,
}
impl Reply {
    fn body(value: Value) -> Self {
        Self {
            status: 200,
            body: value.to_string(),
            delay: Duration::ZERO,
        }
    }
    fn choice(choice: &str) -> Self {
        Self::body(
            json!({"answers":{"verdict":{"type":"choice","choice":choice,"confidence":0.9,
            "probabilities":{"yes":if choice == "yes" {0.9} else {0.05}, "no":if choice == "no" {0.9} else {0.05},
            "continue":if choice == "continue" {0.9} else {0.05}}}}}),
        )
    }
    fn call(probability: f64) -> Self {
        Self::body(json!({"answers":{"verdict":{"type":"noul","noul":probability}}}))
    }
    fn delayed(mut self, ms: u64) -> Self {
        self.delay = Duration::from_millis(ms);
        self
    }
}
struct Http {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    count: Arc<AtomicUsize>,
    closed: Arc<AtomicUsize>,
    received: Arc<Notify>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}
impl Http {
    async fn new(replies: Vec<Reply>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let count = Arc::new(AtomicUsize::new(0));
        let closed = Arc::new(AtomicUsize::new(0));
        let received = Arc::new(Notify::new());
        let stop = CancellationToken::new();
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        let (rq, n, cl, notify, cancellation) = (
            requests.clone(),
            count.clone(),
            closed.clone(),
            received.clone(),
            stop.clone(),
        );
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => break,
                    accepted = listener.accept() => {
                        let (mut socket, _) = accepted.unwrap();
                        let (rq,n,cl,notify,replies) = (rq.clone(),n.clone(),cl.clone(),notify.clone(),replies.clone());
                        tasks.spawn(async move {
                            let mut bytes = vec![];
                            let mut block = [0; 2048];
                            let (header_end, length) = loop {
                                let read = socket.read(&mut block).await.unwrap();
                                if read == 0 { return; }
                                bytes.extend_from_slice(&block[..read]);
                                assert!(bytes.len() < 2*1024*1024);
                                if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                                    let headers = String::from_utf8_lossy(&bytes[..end]);
                                    assert!(headers.starts_with("POST /v1/systemone HTTP/1.1"));
                                    let length = headers.lines().find_map(|line| {
                                        let (key,value) = line.split_once(':')?;
                                        key.eq_ignore_ascii_case("content-length").then(|| value.trim().parse::<usize>().unwrap())
                                    }).unwrap();
                                    break (end+4, length);
                                }
                            };
                            while bytes.len() < header_end + length {
                                let read = socket.read(&mut block).await.unwrap();
                                if read == 0 { return; }
                                bytes.extend_from_slice(&block[..read]);
                            }
                            rq.lock().unwrap().push(serde_json::from_slice(&bytes[header_end..header_end+length]).unwrap());
                            n.fetch_add(1, Ordering::SeqCst);
                            notify.notify_one();
                            let reply = replies.lock().unwrap().pop_front().unwrap_or_else(|| Reply { status: 500, body: "unexpected duplicate request".into(), delay: Duration::ZERO });
                            if !reply.delay.is_zero() {
                                tokio::select! {
                                    _ = tokio::time::sleep(reply.delay) => {},
                                    read = socket.read(&mut block) => { if read.unwrap_or(0) == 0 { cl.fetch_add(1, Ordering::SeqCst); } return; }
                                }
                            }
                            let head = format!("HTTP/1.1 {} Fixture\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n", reply.status, reply.body.len());
                            let _ = socket.write_all(head.as_bytes()).await;
                            let _ = socket.write_all(reply.body.as_bytes()).await;
                        });
                    },
                    Some(result) = tasks.join_next(), if !tasks.is_empty() => { result.unwrap(); }
                }
            }
            tasks.abort_all();
            while let Some(result) = tasks.join_next().await {
                if let Err(e) = result {
                    assert!(e.is_cancelled());
                }
            }
        });
        Self {
            url,
            requests,
            count,
            closed,
            received,
            stop,
            task,
        }
    }
    fn judge(&self, options: JudgeOptions) -> EvalJudge {
        let mut config = JevConfig::new("synthetic-secret-for-localhost");
        config.base_url = self.url.clone();
        config.max_retries = 0;
        config.timeout = options.timeout;
        config.limits.max_response_bytes = options.max_response_bytes;
        config.limits.max_request_bytes = options.max_request_bytes;
        config.limits.max_in_flight = 1;
        EvalJudge::new(Arc::new(JevClassifier::owned(config).unwrap()), options).unwrap()
    }
    async fn finish(self) -> Vec<Value> {
        self.stop.cancel();
        self.task.await.unwrap();
        Arc::try_unwrap(self.requests)
            .unwrap()
            .into_inner()
            .unwrap()
    }
}
fn options() -> SessionOptions {
    SessionOptions {
        transport: SessionConfig {
            close_timeout: Duration::from_millis(30),
            write_timeout: Duration::from_millis(300),
            ..Default::default()
        },
        default_timeout: Duration::from_millis(700),
        handshake_timeout: Duration::from_secs(1),
        max_duration: Duration::from_secs(3),
        ..Default::default()
    }
}
fn judge_options() -> JudgeOptions {
    JudgeOptions {
        timeout: Duration::from_secs(1),
        no_grace: Duration::from_millis(80),
        ..Default::default()
    }
}
fn scenario(turns: Value) -> ScriptScenario {
    ScenarioFile::from_value(json!({"name":"judged","turns":turns}))
        .unwrap()
        .scenarios
        .remove(0)
}
type Step = (u64, &'static str, Value);
fn reply(text: &str) -> Vec<Step> {
    vec![
        (0, "bot-llm-started", json!({})),
        (0, "bot-llm-text", json!({"text":text})),
        (0, "bot-llm-stopped", json!({})),
    ]
}
struct Bot {
    url: String,
    task: JoinHandle<Vec<Value>>,
}
impl Bot {
    async fn new(scripts: Vec<Vec<Step>>) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = WebSocketSession::accept(stream, options().transport)
                .await
                .unwrap();
            let mut turn = 0;
            let mut seen = vec![];
            while let Some(packet) = ws.recv().await {
                let Packet::Text(text) = packet else {
                    panic!("text protocol expected");
                };
                let value: Value = serde_json::from_str(&text).unwrap();
                seen.push(value.clone());
                let steps = if value["type"] == "client-ready" {
                    vec![(0, "bot-ready", json!({"version":"2.2.0"}))]
                } else if value["type"] == "send-text" || value["type"] == "dtmf" {
                    let script = scripts.get(turn).cloned().unwrap_or_default();
                    turn += 1;
                    script
                } else {
                    vec![]
                };
                for (delay, kind, data) in steps {
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    if ws
                        .sender()
                        .send(Packet::Text(
                            json!({"label":"rtvi-ai","type":kind,"data":data}).to_string(),
                        ))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            }
            let _ = ws.finish().await;
            seen
        });
        Self { url, task }
    }
    async fn finish(self) -> Vec<Value> {
        tokio::time::timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap()
    }
}
async fn run(scenario: ScriptScenario, bot: &Bot, http: &Http, opts: JudgeOptions) -> ScriptResult {
    EvalSession::with_judge(scenario, &bot.url, options(), http.judge(opts))
        .unwrap()
        .run(Cancellation::new())
        .await
}
fn one() -> ScriptScenario {
    scenario(
        json!([{"user":"Where is Paris?","expect":[{"event":"response","eval":"Names France"}]}]),
    )
}

#[tokio::test]
async fn scripted_yaml_and_cli_reach_real_native_classifier_and_keep_results_redacted() {
    let http = Http::new(vec![Reply::choice("yes")]).await;
    let bot = Bot::new(vec![reply("Paris is in France")]).await;
    let path = std::env::temp_dir().join(format!(
        "pipecat-native-judge-cli-{}.yaml",
        std::process::id()
    ));
    std::fs::write(&path,format!("name: yaml-cli\njudge:\n  modality: text\n  eval:\n    service: jev\n    api_key_env: PIPECAT_LOCAL_JUDGE_TEST_KEY\n    base_url: {}\n    explainer: false\nturns:\n  - user: Where is Paris?\n    expect:\n      - event: response\n        text_contains: France\n        eval: Names France\n        within_ms: 2000\n",http.url)).unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_pipecat-eval"))
        .args(["run", path.to_str().unwrap(), &bot.url])
        .env(
            "PIPECAT_LOCAL_JUDGE_TEST_KEY",
            "synthetic-secret-for-localhost",
        )
        .output()
        .await
        .unwrap();
    std::fs::remove_file(path).unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: ScriptResult = serde_json::from_slice(&output.stdout).unwrap();
    assert!(result.passed);
    assert_eq!(
        result.turns[0].expectations[0].judgments[0].decision,
        JudgeDecision::Yes
    );
    let rendered = String::from_utf8(output.stdout).unwrap();
    assert!(!rendered.contains("synthetic-secret"));
    assert!(!rendered.contains("Criterion:"));
    bot.finish().await;
    let requests = http.finish().await;
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]["state"],
        json!({"conversation":[{"speaker":"user","text":"Where is Paris?"}],"latest_bot_reply":"Paris is in France"})
    );
    assert_eq!(requests[0]["questions"]["verdict"]["type"], "choice");
}
#[tokio::test]
async fn continue_then_yes_pumps_socket_and_judges_only_new_segments() {
    let http = Http::new(vec![
        Reply::choice("continue").delayed(90),
        Reply::choice("yes"),
    ])
    .await;
    let mut script = reply("I am checking.");
    let mut rest = reply("Paris is in France.");
    rest[0].0 = 20;
    script.extend(rest);
    let bot = Bot::new(vec![script]).await;
    let result = run(one(), &bot, &http, judge_options()).await;
    assert!(result.passed, "{:?}", result.failures);
    assert_eq!(result.turns[0].expectations[0].judgments.len(), 2);
    bot.finish().await;
    let requests = http.finish().await;
    assert_eq!(requests[0]["state"]["latest_bot_reply"], "I am checking.");
    assert_eq!(
        requests[1]["state"]["latest_bot_reply"],
        "I am checking. Paris is in France."
    );
}
#[tokio::test]
async fn no_can_be_corrected_within_grace_but_final_no_is_an_assertion_failure() {
    for corrected in [false, true] {
        let http = Http::new(vec![Reply::choice("no"), Reply::choice("yes")]).await;
        let mut script = reply("Berlin.");
        if corrected {
            let mut rest = reply("Sorry, France.");
            rest[0].0 = 20;
            script.extend(rest);
        }
        let bot = Bot::new(vec![script]).await;
        let result = run(one(), &bot, &http, judge_options()).await;
        assert_eq!(result.passed, corrected, "{:?}", result.failures);
        if !corrected {
            assert_eq!(result.failures[0].kind, "judge_no");
        }
        assert_eq!(
            http.count.load(Ordering::SeqCst),
            if corrected { 2 } else { 1 }
        );
        bot.finish().await;
        http.finish().await;
    }
}
#[tokio::test]
async fn matched_calls_use_actual_arguments_and_full_reporting_before_reply() {
    let http = Http::new(vec![
        Reply::call(0.95),
        Reply::call(0.9),
        Reply::choice("yes"),
    ])
    .await;
    let mut script = vec![
        (
            0,
            "llm-function-call-in-progress",
            json!({"function_name":"lookup","arguments":{"city":"wrong"}}),
        ),
        (
            0,
            "llm-function-call-in-progress",
            json!({"function_name":"book","arguments":{"at":"18:00","people":2}}),
        ),
        (
            0,
            "llm-function-call-in-progress",
            json!({"function_name":"lookup","arguments":{"city":"Paris","units":"C"}}),
        ),
    ];
    script.extend(reply("Booked."));
    let bot = Bot::new(vec![script]).await;
    let s = scenario(json!([{"user":"Book after looking up Paris","expect":[
        {"event":"function_call","calls":[{"name":"lookup","args":{"city":"Paris"}},"book"],"eval":"Uses requested parameters"},
        {"event":"response","eval":"Confirms the booking"}
    ]}]));
    let result = run(s, &bot, &http, judge_options()).await;
    assert!(result.passed, "{:?}", result.failures);
    let controls = bot.finish().await;
    assert!(controls
        .iter()
        .any(|v| v["data"]["d"]["function_call_report_level"]["*"] == "full"));
    let requests = http.finish().await;
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[0]["state"]["call"],
        json!({"name":"lookup","arguments":{"city":"Paris","units":"C"}})
    );
    assert_eq!(
        requests[1]["state"]["call"],
        json!({"name":"book","arguments":{"at":"18:00","people":2}})
    );
    assert_eq!(requests[2]["state"]["latest_bot_reply"], "Booked.");
    assert!(requests
        .iter()
        .all(|r| !r["state"].to_string().contains("wrong")));
}
#[tokio::test]
async fn malformed_provider_failures_are_not_assertions_and_never_retry() {
    let replies = vec![
        Reply {status:401,body:"synthetic-secret PROVIDER_PRIVATE".into(),delay:Duration::ZERO},
        Reply {status:429,body:"PROVIDER_PRIVATE".into(),delay:Duration::ZERO},
        Reply {status:200,body:"not json PROVIDER_PRIVATE".into(),delay:Duration::ZERO},
        Reply::body(json!({"answers":{"verdict":{"type":"choice","choice":"yes","confidence":"invalid"}}})),
        Reply::choice("unknown"),
        Reply::call(0.9),
        Reply {status:200,body:r#"{"answers":{"verdict":{"type":"choice","choice":"no","choice":"yes","confidence":0.9}}}"#.into(),delay:Duration::ZERO},
        Reply {status:200,body:r#"{"answers":{"verdict":{"choice":"no"},"verdict":{"type":"choice","choice":"yes","confidence":0.9}}}"#.into(),delay:Duration::ZERO},
    ];
    for reply in replies {
        let http = Http::new(vec![reply, Reply::choice("yes")]).await;
        let bot = Bot::new(vec![self::reply("France")]).await;
        let result = run(one(), &bot, &http, judge_options()).await;
        assert!(!result.passed);
        assert_eq!(
            result.failures[0].kind, "judge_error",
            "{:?}",
            result.failures
        );
        assert_eq!(result.failures[0].expectation_index, 0);
        assert_eq!(result.failures[0].event_name, "llm_response");
        let rendered = serde_json::to_string(&result).unwrap();
        assert!(!rendered.contains("PROVIDER_PRIVATE"));
        assert!(!rendered.contains("synthetic-secret"));
        assert_eq!(http.count.load(Ordering::SeqCst), 1);
        bot.finish().await;
        http.finish().await;
    }
}
#[tokio::test]
async fn shared_deadlines_timeout_and_continue_never_become_passes() {
    let http = Http::new(vec![Reply::choice("yes").delayed(300)]).await;
    let bot = Bot::new(vec![reply("France")]).await;
    let mut opts = options();
    opts.default_timeout = Duration::from_millis(70);
    let result = EvalSession::with_judge(one(), &bot.url, opts, http.judge(judge_options()))
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.failures[0].kind, "judge_timeout");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(http.closed.load(Ordering::SeqCst), 1);
    bot.finish().await;
    http.finish().await;

    let http = Http::new(vec![Reply::choice("continue")]).await;
    let bot = Bot::new(vec![reply("Checking")]).await;
    let mut opts = options();
    opts.default_timeout = Duration::from_millis(70);
    let result = EvalSession::with_judge(one(), &bot.url, opts, http.judge(judge_options()))
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.failures[0].kind, "judge_continue");
    bot.finish().await;
    http.finish().await;
}
#[tokio::test]
async fn cancellation_and_drop_close_inflight_http_and_rtv_i() {
    for drop_owner in [false, true] {
        let http = Http::new(vec![Reply::choice("yes").delayed(2000)]).await;
        let bot = Bot::new(vec![reply("France")]).await;
        let session =
            EvalSession::with_judge(one(), &bot.url, options(), http.judge(judge_options()))
                .unwrap();
        let cancel = Cancellation::new();
        let task = tokio::spawn(session.run(cancel.clone()));
        tokio::time::timeout(Duration::from_secs(1), http.received.notified())
            .await
            .unwrap();
        if drop_owner {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            cancel.cancel();
            let result = task.await.unwrap();
            assert_eq!(result.failures[0].kind, "cancelled");
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(http.closed.load(Ordering::SeqCst), 1);
        assert_eq!(http.count.load(Ordering::SeqCst), 1);
        bot.finish().await;
        http.finish().await;
    }
}
#[tokio::test]
async fn interrupt_during_judging_rejects_late_yes_and_duplicate_stop_has_no_verdict() {
    let http = Http::new(vec![Reply::choice("yes").delayed(180)]).await;
    let mut script = reply("France");
    script.push((20, "bot-interrupted", json!({})));
    let bot = Bot::new(vec![script]).await;
    let result = run(one(), &bot, &http, judge_options()).await;
    assert!(!result.passed);
    assert_eq!(result.failures[0].kind, "judge_interrupted");
    assert!(result.turns[0].expectations[0].judgments.is_empty());
    bot.finish().await;
    http.finish().await;

    let http = Http::new(vec![Reply::choice("continue"), Reply::choice("yes")]).await;
    let mut script = reply("Checking");
    script.push((10, "bot-llm-stopped", json!({})));
    let bot = Bot::new(vec![script]).await;
    let mut opts = options();
    opts.default_timeout = Duration::from_millis(80);
    let result = EvalSession::with_judge(one(), &bot.url, opts, http.judge(judge_options()))
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.failures[0].kind, "judge_continue");
    assert_eq!(http.count.load(Ordering::SeqCst), 1);
    bot.finish().await;
    http.finish().await;
}
#[tokio::test]
async fn runtime_cache_keys_criterion_state_and_kind_and_obeys_limits_and_cancel() {
    let http = Http::new(vec![
        Reply::choice("yes"),
        Reply::choice("yes"),
        Reply::choice("yes"),
        Reply::call(0.9),
    ])
    .await;
    let mut judge = http.judge(judge_options());
    judge.add_user_message("Where?").unwrap();
    judge.add_assistant_message("France").unwrap();
    let first = judge
        .evaluate_reply("right country", CallOptions::default())
        .await
        .unwrap();
    let cached = judge
        .evaluate_reply(" right country ", CallOptions::default())
        .await
        .unwrap();
    assert!(!first.cached);
    assert!(cached.cached);
    assert_eq!(judge.request_count(), 1);
    judge
        .evaluate_reply("right continent", CallOptions::default())
        .await
        .unwrap();
    judge.add_assistant_message("in Europe").unwrap();
    judge
        .evaluate_reply("right country", CallOptions::default())
        .await
        .unwrap();
    judge
        .evaluate_call(
            "lookup",
            &json!({"city":"Paris"}),
            "right country",
            CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(judge.request_count(), 4);
    let token = CancellationToken::new();
    token.cancel();
    assert!(judge
        .evaluate_reply(
            "right country",
            CallOptions {
                cancellation: token,
                deadline: None
            }
        )
        .await
        .is_err());
    drop(judge);
    assert_eq!(http.finish().await.len(), 4);
}
#[tokio::test]
async fn byte_request_and_wire_limits_stop_evaluation_instead_of_discarding_evidence() {
    let http = Http::new(vec![Reply {
        status: 200,
        body: "x".repeat(4097),
        delay: Duration::ZERO,
    }])
    .await;
    let bot = Bot::new(vec![reply("France")]).await;
    let result = run(
        one(),
        &bot,
        &http,
        JudgeOptions {
            max_response_bytes: 4096,
            ..judge_options()
        },
    )
    .await;
    assert_eq!(result.failures[0].kind, "limit_exceeded");
    bot.finish().await;
    http.finish().await;

    let http = Http::new(vec![Reply::choice("continue"), Reply::choice("yes")]).await;
    let mut script = reply("Checking");
    let mut rest = reply("France");
    rest[0].0 = 15;
    script.extend(rest);
    let bot = Bot::new(vec![script]).await;
    let result = run(
        one(),
        &bot,
        &http,
        JudgeOptions {
            max_requests: 1,
            ..judge_options()
        },
    )
    .await;
    assert_eq!(result.failures[0].kind, "limit_exceeded");
    assert_eq!(http.count.load(Ordering::SeqCst), 1);
    bot.finish().await;
    http.finish().await;

    let http = Http::new(vec![Reply::choice("yes").delayed(1000)]).await;
    let mut script = reply("France");
    script.extend((0..50).map(|_| (1, "vad-user-started-speaking", json!({}))));
    let bot = Bot::new(vec![script]).await;
    let mut opts = options();
    opts.max_wire_messages = 10;
    let result = EvalSession::with_judge(one(), &bot.url, opts, http.judge(judge_options()))
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.failures[0].kind, "limit_exceeded");
    bot.finish().await;
    http.finish().await;
}
#[test]
fn unsupported_judgments_and_missing_judge_fail_before_connecting() {
    for expectation in [
        json!({"event":"function_call_stopped","eval":"stopped"}),
        json!({"event":"image","eval":"visible"}),
        json!({"event":"response","eval":" "}),
        json!({"event":"response","eval":"ok","absent":true}),
    ] {
        assert!(
            ScenarioFile::from_value(json!({"name":"bad","turns":[{"expect":[expectation]}]}))
                .is_err()
        );
    }
    assert!(matches!(
        EvalSession::new(one(), "ws://127.0.0.1:1", options()),
        Err(EvalError::Scenario(_))
    ));
}

#[test]
fn standalone_deterministic_matcher_cannot_pass_an_unjudged_eval() {
    use pipecat_evals::matcher::{ExpectationMatcher, MatchOutcome, PendingCheck};
    let s = one();
    let mut check = PendingCheck::new(s.turns[0].expect[0].clone());
    let mut matcher = ExpectationMatcher::new(1024, 16);
    assert!(matches!(
        matcher.buffered(&mut check),
        MatchOutcome::Failed { .. }
    ));
    assert!(matcher
        .feed(&mut check, json!({"type":"llm_response","text":"France"}))
        .is_err());
    assert!(matches!(
        matcher.timeout(&check),
        MatchOutcome::Failed { .. }
    ));
}
#[tokio::test]
async fn transcript_cache_entry_and_evaluation_limits_fail_before_another_http_request() {
    let http = Http::new(vec![Reply::choice("yes"), Reply::choice("yes")]).await;
    let mut judge = http.judge(JudgeOptions {
        max_transcript_bytes: 5,
        ..judge_options()
    });
    assert!(matches!(
        judge.add_user_message("secret-too-long"),
        Err(EvalError::Limit(_))
    ));
    assert_eq!(http.count.load(Ordering::SeqCst), 0);
    let mut judge = http.judge(JudgeOptions {
        max_cache_entries: 1,
        ..judge_options()
    });
    judge.add_user_message("u").unwrap();
    judge.add_assistant_message("a").unwrap();
    judge
        .evaluate_reply("criterion", CallOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        judge
            .evaluate_reply("different", CallOptions::default())
            .await,
        Err(EvalError::Limit(_))
    ));
    let mut judge = http.judge(JudgeOptions {
        max_evaluations: 1,
        ..judge_options()
    });
    judge.add_user_message("u").unwrap();
    judge.add_assistant_message("a").unwrap();
    judge
        .evaluate_reply("criterion", CallOptions::default())
        .await
        .unwrap();
    assert!(matches!(
        judge
            .evaluate_reply("criterion", CallOptions::default())
            .await,
        Err(EvalError::Limit(_))
    ));
    let mut judge = http.judge(JudgeOptions {
        max_cache_bytes: 20,
        ..judge_options()
    });
    assert!(matches!(
        judge
            .evaluate_reply("criterion", CallOptions::default())
            .await,
        Err(EvalError::Limit(_))
    ));
    assert_eq!(http.count.load(Ordering::SeqCst), 2);
    drop(judge);
    http.finish().await;
}

struct CancelOnCompletion {
    classifier: JevClassifier,
    cancellation: Cancellation,
}
impl pipecat_classifiers::Classifier for CancelOnCompletion {
    fn ask<'s, 'v, 'q, 'f>(
        &'s self,
        state: &'v Value,
        questions: &'q pipecat_classifiers::Questions,
        options: CallOptions,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = pipecat_classifiers::Result<pipecat_classifiers::ClassificationBatch>,
                > + Send
                + 'f,
        >,
    >
    where
        's: 'f,
        'v: 'f,
        'q: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            let answer = self.classifier.ask(state, questions, options).await;
            self.cancellation.cancel();
            answer
        })
    }
}
#[tokio::test]
async fn session_cancellation_during_ready_real_http_verdict_prevents_final_pass() {
    let http = Http::new(vec![Reply::choice("yes")]).await;
    let bot = Bot::new(vec![reply("France")]).await;
    let cancel = Cancellation::new();
    let mut config = JevConfig::new("synthetic-secret-for-localhost");
    config.base_url = http.url.clone();
    config.max_retries = 0;
    let classifier = CancelOnCompletion {
        classifier: JevClassifier::owned(config).unwrap(),
        cancellation: cancel.clone(),
    };
    let judge = EvalJudge::new(Arc::new(classifier), judge_options()).unwrap();
    let result = EvalSession::with_judge(one(), &bot.url, options(), judge)
        .unwrap()
        .run(cancel)
        .await;
    assert!(!result.passed);
    assert!(result
        .failures
        .iter()
        .any(|failure| failure.kind == "cancelled"));
    assert_eq!(http.count.load(Ordering::SeqCst), 1);
    bot.finish().await;
    http.finish().await;
}
