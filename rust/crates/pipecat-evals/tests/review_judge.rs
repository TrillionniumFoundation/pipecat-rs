//! Independent raw-HTTP + RTVI regressions. All data and credentials are synthetic.
use pipecat_classifiers::{CallOptions, JevClassifier, JevConfig};
use pipecat_evals::{
    matcher::ScriptResult, Cancellation, EvalJudge, EvalSession, JudgeDecision, JudgeOptions,
    ScenarioFile, ScriptScenario, SessionOptions,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Notify},
    task::JoinHandle,
    time::Instant,
};

struct Reply {
    status: u16,
    body: String,
    delay: Duration,
}
impl Reply {
    fn body(body: impl Into<String>) -> Self {
        Self {
            status: 200,
            body: body.into(),
            delay: Duration::ZERO,
        }
    }
    fn choice(choice: &str) -> Self {
        Self::body(
            json!({"answers":{"verdict":{"type":"choice","choice":choice,"confidence":0.9,
            "probabilities":{"yes":0.4,"no":0.3,"continue":0.3}}}})
            .to_string(),
        )
    }
    fn call_yes() -> Self {
        Self::body(json!({"answers":{"verdict":{"type":"noul","noul":0.9}}}).to_string())
    }
    fn delayed(mut self, ms: u64) -> Self {
        self.delay = Duration::from_millis(ms);
        self
    }
}
struct HttpPeer {
    url: String,
    requests: mpsc::Receiver<Value>,
    count: Arc<AtomicUsize>,
    closed: Arc<Notify>,
    received: Arc<Notify>,
    task: JoinHandle<()>,
}
impl Drop for HttpPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn request(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    let mut chunk = [0; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        assert!(bytes.len() < 1024 * 1024);
        if let Some(p) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
    assert!(headers.starts_with("POST /v1/systemone "));
    assert!(headers
        .to_ascii_lowercase()
        .contains("authorization: bearer review-synthetic-key\r\n"));
    let len = headers
        .lines()
        .find_map(|v| {
            v.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while bytes.len() < end + len {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    serde_json::from_slice(&bytes[end..end + len]).unwrap()
}
async fn http(replies: Vec<Reply>) -> HttpPeer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, requests) = mpsc::channel(32);
    let count = Arc::new(AtomicUsize::new(0));
    let total = count.clone();
    let closed = Arc::new(Notify::new());
    let dropped = closed.clone();
    let received = Arc::new(Notify::new());
    let admitted = received.clone();
    let task = tokio::spawn(async move {
        let mut replies: VecDeque<_> = replies.into();
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted=listener.accept()=>{
                    let (mut socket,_)=accepted.unwrap();
                    let reply=replies.pop_front().unwrap_or(Reply{status:500,body:"unexpected retry".into(),delay:Duration::ZERO});
                    let tx=tx.clone();let total=total.clone();let dropped=dropped.clone();let admitted=admitted.clone();
                    tasks.spawn(async move {
                        let body=request(&mut socket).await;total.fetch_add(1,Ordering::SeqCst);
                        tx.send(body).await.unwrap();
                        admitted.notify_one();
                        let mut byte=[0];
                        tokio::select! {
                            biased;
                            read=socket.read(&mut byte)=>{assert_eq!(read.unwrap(),0);dropped.notify_one();return;}
                            _=tokio::time::sleep(reply.delay)=>{}
                        }
                        let headers=format!("HTTP/1.1 {} Synthetic\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",reply.status,reply.body.len());
                        if socket.write_all(headers.as_bytes()).await.is_ok() {let _=socket.write_all(reply.body.as_bytes()).await;}
                    });
                }
                Some(result)=tasks.join_next(),if !tasks.is_empty()=>{result.unwrap();}
            }
        }
    });
    HttpPeer {
        url,
        requests,
        count,
        closed,
        received,
        task,
    }
}
fn judge(peer: &HttpPeer) -> EvalJudge {
    let mut config = JevConfig::new("review-synthetic-key");
    config.base_url = peer.url.clone();
    config.max_retries = 0;
    config.timeout = Duration::from_secs(2);
    EvalJudge::new(
        Arc::new(JevClassifier::owned(config).unwrap()),
        JudgeOptions {
            timeout: Duration::from_secs(2),
            no_grace: Duration::from_millis(30),
            ..Default::default()
        },
    )
    .unwrap()
}
fn wire(kind: &str, data: Value) -> Value {
    json!({"label":"rtvi-ai","type":kind,"data":data})
}
type Step = (u64, Value);
fn reply(text: &str) -> Vec<Step> {
    vec![
        (0, wire("bot-llm-started", json!({}))),
        (0, wire("bot-llm-text", json!({"text":text}))),
        (0, wire("bot-llm-stopped", json!({}))),
    ]
}
struct Bot {
    url: String,
    task: JoinHandle<()>,
}
impl Drop for Bot {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn bot(scripts: Vec<Vec<Step>>) -> Bot {
    bot_with_admission(scripts, None).await
}
async fn bot_with_admission(scripts: Vec<Vec<Step>>, admission: Option<Arc<Notify>>) -> Bot {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/eval", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut socket = WebSocketSession::accept(socket, options().transport)
            .await
            .unwrap();
        let mut scripts: VecDeque<_> = scripts.into();
        while let Some(packet) = socket.recv().await {
            let Packet::Text(text) = packet else { continue };
            let msg: Value = serde_json::from_str(&text).unwrap();
            let steps = match msg["type"].as_str().unwrap() {
                "client-ready" => vec![(0, wire("bot-ready", json!({"version":"2.2.0"})))],
                "send-text" | "dtmf" => scripts.pop_front().unwrap_or_default(),
                _ => Vec::new(),
            };
            for (ms, event) in steps {
                if event["type"] == "review-wait-for-http-admission" {
                    tokio::time::timeout(
                        Duration::from_secs(1),
                        admission.as_ref().unwrap().notified(),
                    )
                    .await
                    .unwrap();
                    continue;
                }
                if ms > 0 {
                    tokio::time::sleep(Duration::from_millis(ms)).await;
                }
                if socket
                    .sender()
                    .send(Packet::Text(event.to_string()))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
        let _ = socket.finish().await;
    });
    Bot { url, task }
}
fn options() -> SessionOptions {
    SessionOptions {
        transport: SessionConfig {
            connect_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_millis(100),
            close_timeout: Duration::from_millis(20),
            ..Default::default()
        },
        handshake_timeout: Duration::from_millis(500),
        default_timeout: Duration::from_millis(500),
        max_duration: Duration::from_secs(3),
        ..Default::default()
    }
}
fn scenario(turns: Value) -> ScriptScenario {
    ScenarioFile::from_value(json!({"name":"independent-judge","turns":turns}))
        .unwrap()
        .scenarios
        .remove(0)
}
async fn run(s: ScriptScenario, b: &Bot, p: &HttpPeer) -> ScriptResult {
    EvalSession::with_judge(s, &b.url, options(), judge(p))
        .unwrap()
        .run(Cancellation::new())
        .await
}
async fn next_request(peer: &mut HttpPeer) -> Value {
    tokio::time::timeout(Duration::from_secs(1), peer.requests.recv())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn duplicate_conflicting_classifier_verdict_is_rejected() {
    let p=http(vec![Reply::body(r#"{"answers":{"verdict":{"type":"choice","choice":"no","confidence":0.9},"verdict":{"type":"choice","choice":"yes","confidence":0.9}}}"#)]).await;
    let b = bot(vec![reply("synthetic answer")]).await;
    let result=run(scenario(json!([{"user":"synthetic question","expect":[{"event":"response","eval":"satisfies criterion"}]}])),&b,&p).await;
    assert!(
        !result.passed,
        "conflicting duplicate verdict must not be accepted as yes"
    );
    assert_eq!(result.failures[0].kind, "judge_error");
    assert_eq!(p.count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn changed_state_rejudges_and_current_turn_segments_appear_once() {
    let mut p = http(vec![
        Reply::choice("continue"),
        Reply::choice("yes"),
        Reply::call_yes(),
        Reply::choice("yes"),
    ])
    .await;
    let mut first = reply("checking now");
    let mut tail = reply("answer is four");
    tail[0].0 = 20;
    first.extend(tail);
    let mut second = vec![
        (
            0,
            wire(
                "llm-function-call-in-progress",
                json!({"function_name":"lookup","arguments":{"city":"wrong"}}),
            ),
        ),
        (
            0,
            wire(
                "llm-function-call-in-progress",
                json!({"function_name":"lookup","arguments":{"city":"Paris","extra":true}}),
            ),
        ),
    ];
    second.extend(reply("second answer"));
    let b = bot(vec![first, second]).await;
    let s = scenario(json!([
        {"user":"first question","expect":[{"event":"response","eval":"satisfies criterion"}]},
        {"user":"second question","expect":[{"event":"function_call","name":"lookup","args":{"city":"Paris"},"eval":"right city"},{"event":"response","eval":"satisfies criterion"}]}
    ]));
    let result = run(s, &b, &p).await;
    assert!(result.passed, "{:?}", result.failures);
    let a = next_request(&mut p).await;
    let b = next_request(&mut p).await;
    let c = next_request(&mut p).await;
    let d = next_request(&mut p).await;
    assert_eq!(
        a["state"],
        json!({"conversation":[{"speaker":"user","text":"first question"}],"latest_bot_reply":"checking now"})
    );
    assert_eq!(
        b["state"]["latest_bot_reply"],
        "checking now answer is four"
    );
    assert_eq!(
        c["state"]["call"],
        json!({"name":"lookup","arguments":{"city":"Paris","extra":true}})
    );
    assert_eq!(c["questions"]["verdict"]["type"], "noul");
    assert_eq!(
        d["state"],
        json!({"conversation":[{"speaker":"user","text":"first question"},{"speaker":"bot","turn":1,"text":"checking now answer is four"},{"speaker":"user","text":"second question"}],"latest_bot_reply":"second answer"})
    );
    assert_eq!(p.count.load(Ordering::SeqCst), 4);
    assert_eq!(
        result.turns[0].expectations[0]
            .judgments
            .iter()
            .map(|v| v.decision)
            .collect::<Vec<_>>(),
        vec![JudgeDecision::Continue, JudgeDecision::Yes]
    );
}
#[tokio::test]
async fn same_state_cache_is_session_local_and_criterion_specific() {
    let mut p = http(vec![
        Reply::choice("yes"),
        Reply::choice("yes"),
        Reply::choice("yes"),
        Reply::choice("yes"),
    ])
    .await;
    let mut j = judge(&p);
    j.add_user_message("u").unwrap();
    j.add_assistant_message("a").unwrap();
    assert!(
        !j.evaluate_reply("criterion", CallOptions::default())
            .await
            .unwrap()
            .cached
    );
    assert!(
        j.evaluate_reply("criterion", CallOptions::default())
            .await
            .unwrap()
            .cached
    );
    assert!(
        !j.evaluate_reply("different criterion", CallOptions::default())
            .await
            .unwrap()
            .cached
    );
    j.add_assistant_message("b").unwrap();
    assert!(
        !j.evaluate_reply("criterion", CallOptions::default())
            .await
            .unwrap()
            .cached
    );
    let mut next = judge(&p);
    next.add_user_message("u").unwrap();
    next.add_assistant_message("a").unwrap();
    assert!(
        !next
            .evaluate_reply("criterion", CallOptions::default())
            .await
            .unwrap()
            .cached
    );
    assert_eq!(p.count.load(Ordering::SeqCst), 4);
    for _ in 0..4 {
        let _ = next_request(&mut p).await;
    }
}
#[tokio::test]
async fn slow_classifier_does_not_stop_bounded_socket_ingestion() {
    let p = http(vec![Reply::choice("yes").delayed(1000)]).await;
    let mut steps = reply("answer");
    steps.push((0, wire("review-wait-for-http-admission", json!({}))));
    steps.push((0, wire("bot-started-speaking", json!({}))));
    for _ in 0..20 {
        steps.push((0, wire("bot-stopped-speaking", json!({}))));
    }
    let b = bot_with_admission(vec![steps], Some(p.received.clone())).await;
    let mut opts = options();
    opts.event_limits.max_events = 8;
    let started = Instant::now();
    let result = EvalSession::with_judge(
        scenario(json!([{"user":"question","expect":[{"event":"response","eval":"criterion"}]}])),
        &b.url,
        opts,
        judge(&p),
    )
    .unwrap()
    .run(Cancellation::new())
    .await;
    assert!(!result.passed);
    assert_eq!(result.failures[0].kind, "limit_exceeded");
    assert!(started.elapsed() < Duration::from_millis(500));
    tokio::time::timeout(Duration::from_millis(500), p.closed.notified())
        .await
        .unwrap();
}
#[tokio::test]
async fn cancellation_closes_actual_stalled_classifier_socket() {
    let mut p = http(vec![Reply::choice("yes").delayed(1000)]).await;
    let b = bot(vec![reply("answer")]).await;
    let cancel = Cancellation::new();
    let token = cancel.clone();
    let session = EvalSession::with_judge(
        scenario(json!([{"user":"question","expect":[{"event":"response","eval":"criterion"}]}])),
        &b.url,
        options(),
        judge(&p),
    )
    .unwrap();
    let task = tokio::spawn(session.run(token));
    let _ = next_request(&mut p).await;
    cancel.cancel();
    let result = tokio::time::timeout(Duration::from_millis(500), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.failures[0].kind, "cancelled");
    assert!(!result.passed);
    tokio::time::timeout(Duration::from_millis(500), p.closed.notified())
        .await
        .unwrap();
    assert_eq!(p.count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn late_yes_after_interruption_is_not_committed() {
    let p = http(vec![Reply::choice("yes").delayed(150)]).await;
    let mut steps = reply("answer");
    steps.push((0, wire("review-wait-for-http-admission", json!({}))));
    steps.push((0, wire("bot-interrupted", json!({}))));
    let b = bot_with_admission(vec![steps], Some(p.received.clone())).await;
    let result = run(
        scenario(json!([{"user":"question","expect":[{"event":"response","eval":"criterion"}]}])),
        &b,
        &p,
    )
    .await;
    assert!(!result.passed);
    assert_eq!(result.failures[0].kind, "judge_interrupted");
    assert!(result.turns[0].expectations[0].judgments.is_empty());
    tokio::time::timeout(Duration::from_millis(500), p.closed.notified())
        .await
        .unwrap();
}
#[tokio::test]
async fn provider_failure_and_continue_are_honestly_distinct_and_not_retried() {
    for status in [429, 529, 500] {
        let p = http(vec![Reply {
            status,
            body: "review-provider-secret".into(),
            delay: Duration::ZERO,
        }])
        .await;
        let b = bot(vec![reply("answer")]).await;
        let result=run(scenario(json!([{"user":"question","expect":[{"event":"response","eval":"review-private-criterion"}]}])),&b,&p).await;
        assert!(!result.passed);
        assert_eq!(result.failures[0].kind, "judge_error");
        assert_eq!(p.count.load(Ordering::SeqCst), 1);
        let report = serde_json::to_string(&result).unwrap();
        for secret in [
            "review-provider-secret",
            "review-synthetic-key",
            "review-private-criterion",
        ] {
            assert!(!report.contains(secret));
        }
    }
    let p = http(vec![Reply::choice("continue")]).await;
    let b = bot(vec![reply("checking")]).await;
    let result=run(scenario(json!([{"user":"question","expect":[{"event":"response","eval":"criterion","within_ms":60}]}])),&b,&p).await;
    assert!(!result.passed);
    assert_eq!(result.failures[0].kind, "judge_continue");
    assert_eq!(p.count.load(Ordering::SeqCst), 1);
}
type ObservedJudgeDeadlines = Arc<std::sync::Mutex<Vec<Option<Instant>>>>;

struct RecordRealClassifierDeadlines {
    inner: JevClassifier,
    deadlines: ObservedJudgeDeadlines,
}
impl pipecat_classifiers::Classifier for RecordRealClassifierDeadlines {
    fn ask<'a, 'b, 'c, 'f>(
        &'a self,
        state: &'b Value,
        questions: &'c pipecat_classifiers::Questions,
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
        'a: 'f,
        'b: 'f,
        'c: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            // Record only an actually polled invocation. Never hold this guard
            // across the real classifier's await or alter its CallOptions.
            {
                let mut deadlines = self.deadlines.lock().unwrap();
                assert!(
                    deadlines.len() < 2,
                    "unexpected third classifier invocation"
                );
                deadlines.push(options.deadline);
            }
            pipecat_classifiers::Classifier::ask(&self.inner, state, questions, options).await
        })
    }
}
fn judge_recording_deadlines(peer: &HttpPeer, deadlines: ObservedJudgeDeadlines) -> EvalJudge {
    let mut config = JevConfig::new("review-synthetic-key");
    config.base_url = peer.url.clone();
    config.max_retries = 0;
    config.timeout = Duration::from_secs(2);
    EvalJudge::new(
        Arc::new(RecordRealClassifierDeadlines {
            inner: JevClassifier::owned(config).unwrap(),
            deadlines,
        }),
        JudgeOptions {
            timeout: Duration::from_secs(2),
            no_grace: Duration::from_millis(30),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn absolute_turn_deadline_includes_all_judgments() {
    let p = http(vec![
        Reply::call_yes().delayed(65),
        Reply::choice("yes").delayed(200),
    ])
    .await;
    let mut steps = vec![(
        0,
        wire(
            "llm-function-call-in-progress",
            json!({"function_name":"lookup","arguments":{}}),
        ),
    )];
    steps.push((0, wire("review-wait-for-http-admission", json!({}))));
    steps.extend(reply("answer"));
    let b = bot_with_admission(vec![steps], Some(p.received.clone())).await;
    let deadlines = Arc::new(std::sync::Mutex::new(Vec::with_capacity(2)));
    let started = Instant::now();
    let result = EvalSession::with_judge(
        scenario(json!([{"user":"question","expect":[
            {"event":"function_call","name":"lookup","eval":"criterion","within_ms":140},
            {"event":"response","eval":"criterion","within_ms":140}
        ]}])),
        &b.url,
        options(),
        judge_recording_deadlines(&p, deadlines.clone()),
    )
    .unwrap()
    .run(Cancellation::new())
    .await;
    // Format completed-run evidence only when an existing assertion fails.
    let failure_details = || {
        format!(
            "result={result:#?}\nhttp_requests={}\nelapsed={:?}\nclassifier_deadlines={:?}",
            p.count.load(Ordering::SeqCst),
            started.elapsed(),
            deadlines.lock().unwrap(),
        )
    };
    assert!(!result.passed, "{}", failure_details());
    assert_eq!(
        result.failures[0].kind,
        "judge_timeout",
        "{}",
        failure_details()
    );
    assert_eq!(
        result.failures[0].expectation_index,
        1,
        "{}",
        failure_details()
    );
    assert_eq!(p.count.load(Ordering::SeqCst), 2, "{}", failure_details());
    assert!(
        started.elapsed() < Duration::from_millis(240),
        "{}",
        failure_details()
    );
    tokio::time::timeout(Duration::from_millis(500), p.closed.notified())
        .await
        .unwrap_or_else(|error| {
            panic!(
                "classifier socket did not close: {error}; {}",
                failure_details()
            )
        });
    // Admission above orders first-call matching/HTTP before the reply. It does
    // not exclude initial event admission from the original 140 ms turn budget.
    // Check actual HTTP admissions after the original socket cleanup assertion.
    assert_eq!(p.count.load(Ordering::SeqCst), 2, "{}", failure_details());
    let observed = deadlines.lock().unwrap().clone();
    assert_eq!(observed.len(), 2, "{}", failure_details());
    assert!(observed[0].is_some(), "{}", failure_details());
    assert_eq!(observed[0], observed[1], "{}", failure_details());
}

#[tokio::test]
async fn duplicate_conflicting_choice_field_is_rejected() {
    let p = http(vec![Reply::body(r#"{"answers":{"verdict":{"type":"choice","choice":"no","choice":"yes","confidence":0.9}}}"#)]).await;
    let mut j = judge(&p);
    j.add_assistant_message("answer").unwrap();
    assert!(j
        .evaluate_reply("criterion", CallOptions::default())
        .await
        .is_err());
    assert_eq!(p.count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn cache_and_transcript_limits_fail_without_silent_evidence_truncation() {
    let p = http(vec![Reply::choice("yes")]).await;
    let mut config = JevConfig::new("review-synthetic-key");
    config.base_url = p.url.clone();
    config.max_retries = 0;
    let mut j = EvalJudge::new(
        Arc::new(JevClassifier::owned(config).unwrap()),
        JudgeOptions {
            max_transcript_bytes: 10,
            max_cache_entries: 1,
            ..Default::default()
        },
    )
    .unwrap();
    j.add_user_message("abc").unwrap();
    j.add_assistant_message("def").unwrap();
    j.add_assistant_message("ghi").unwrap();
    assert!(matches!(
        j.add_assistant_message("j"),
        Err(pipecat_evals::EvalError::Limit(_))
    ));
    j.evaluate_reply("criterion", CallOptions::default())
        .await
        .unwrap();
    assert!(
        j.evaluate_reply("criterion", CallOptions::default())
            .await
            .unwrap()
            .cached
    );
    assert!(matches!(
        j.evaluate_reply("other criterion", CallOptions::default())
            .await,
        Err(pipecat_evals::EvalError::Limit(_))
    ));
    assert_eq!(p.count.load(Ordering::SeqCst), 1);
    let mut p = p;
    let body = next_request(&mut p).await;
    assert_eq!(body["state"]["latest_bot_reply"], "def ghi");
}
#[tokio::test]
async fn allow_continue_false_rejects_continue_and_sends_only_yes_no_options() {
    let mut p = http(vec![Reply::choice("continue")]).await;
    let mut config = JevConfig::new("review-synthetic-key");
    config.base_url = p.url.clone();
    config.max_retries = 0;
    let mut j = EvalJudge::new(
        Arc::new(JevClassifier::owned(config).unwrap()),
        JudgeOptions {
            allow_continue: false,
            ..Default::default()
        },
    )
    .unwrap();
    j.add_assistant_message("answer").unwrap();
    assert!(j
        .evaluate_reply("criterion", CallOptions::default())
        .await
        .is_err());
    let body = next_request(&mut p).await;
    let criteria = body["questions"]["verdict"]["criteria"]
        .as_object()
        .unwrap();
    assert_eq!(criteria.len(), 2);
    assert!(criteria.contains_key("yes"));
    assert!(criteria.contains_key("no"));
}
#[tokio::test]
async fn function_judging_uses_the_source_half_probability_threshold() {
    let p = http(vec![
        Reply::body(r#"{"answers":{"verdict":{"type":"noul","noul":0.49}}}"#),
        Reply::body(r#"{"answers":{"verdict":{"type":"noul","noul":0.5}}}"#),
    ])
    .await;
    let mut j = judge(&p);
    assert_eq!(
        j.evaluate_call(
            "lookup",
            &json!({}),
            "first criterion",
            CallOptions::default()
        )
        .await
        .unwrap()
        .decision,
        JudgeDecision::No
    );
    assert_eq!(
        j.evaluate_call(
            "lookup",
            &json!({}),
            "second criterion",
            CallOptions::default()
        )
        .await
        .unwrap()
        .decision,
        JudgeDecision::Yes
    );
}

struct CancelAfterRealClassifier {
    inner: JevClassifier,
    cancellation: Cancellation,
}
impl pipecat_classifiers::Classifier for CancelAfterRealClassifier {
    fn ask<'a, 'b, 'c, 'f>(
        &'a self,
        state: &'b Value,
        questions: &'c pipecat_classifiers::Questions,
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
        'a: 'f,
        'b: 'f,
        'c: 'f,
        Self: 'f,
    {
        Box::pin(async move {
            let result =
                pipecat_classifiers::Classifier::ask(&self.inner, state, questions, options).await;
            self.cancellation.cancel();
            result
        })
    }
}
#[tokio::test]
async fn cancellation_on_real_classifier_completion_never_commits_session_success() {
    let p = http(vec![Reply::choice("yes")]).await;
    let b = bot(vec![reply("answer")]).await;
    let cancellation = Cancellation::new();
    let mut config = JevConfig::new("review-synthetic-key");
    config.base_url = p.url.clone();
    config.max_retries = 0;
    let classifier = CancelAfterRealClassifier {
        inner: JevClassifier::owned(config).unwrap(),
        cancellation: cancellation.clone(),
    };
    let judge = EvalJudge::new(Arc::new(classifier), JudgeOptions::default()).unwrap();
    let result = EvalSession::with_judge(
        scenario(json!([{"user":"question","expect":[{"event":"response","eval":"criterion"}]}])),
        &b.url,
        options(),
        judge,
    )
    .unwrap()
    .run(cancellation.clone())
    .await;
    assert!(cancellation.is_cancelled());
    assert_eq!(p.count.load(Ordering::SeqCst), 1);
    assert!(
        !result.passed,
        "cancellation set by the completing real classifier must fence session success"
    );
    assert_eq!(result.failures[0].kind, "cancelled");
}
