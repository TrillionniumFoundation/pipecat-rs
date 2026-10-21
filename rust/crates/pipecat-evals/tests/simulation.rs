#![cfg(feature = "simulation")]
//! Production typed/compiled-CLI loops over real local HTTP/SSE and RTVI.
//! Synthetic peers prove control flow, not model quality or live-provider parity.
use pipecat_evals::{simulation::*, Cancellation};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::{json, Value};
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

fn scenario() -> SimulationScenario {
    SimulationScenario::from_yaml(b"name: shopping\npersona: impatient shopper\ngoal: buy blue mug\nsuccess: blue mug order confirmed\nmax_duration_ms: 3000\nmax_silence_ms: 1000\nclosing_timeout_ms: 100\nhandshake_timeout_ms: 300\n").unwrap()
}
fn model(endpoint: String) -> ModelConfig {
    ModelConfig {
        endpoint,
        model: "synthetic-only".into(),
        api_key_env: None,
        timeout_ms: 1000,
    }
}
fn runtime(endpoint: String) -> SimulationRuntime {
    SimulationRuntime {
        caller: model(endpoint),
        judge: None,
    }
}
fn wire(kind: &str, data: Value) -> Value {
    json!({"label":"rtvi-ai","type":kind,"data":data})
}
fn reply(text: &str) -> Vec<Value> {
    vec![
        wire("bot-llm-started", json!({})),
        wire("bot-llm-text", json!({"text":text})),
        wire("bot-llm-stopped", json!({})),
    ]
}
async fn read_request(socket: &mut TcpStream) -> Value {
    let mut data = vec![];
    let mut chunk = [0; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&chunk[..n]);
        assert!(data.len() < 1024 * 1024);
        if let Some(i) = data.windows(4).position(|s| s == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = String::from_utf8_lossy(&data[..end]);
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while data.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        data.extend_from_slice(&chunk[..n]);
    }
    serde_json::from_slice(&data[end..end + length]).unwrap()
}
fn completion(text: &str, claim: Option<Value>) -> String {
    let mut delta = json!({"content":text});
    let finish = if let Some(arguments) = claim {
        delta["tool_calls"] = json!([{"index":0,"id":"hangup","type":"function","function":{"name":"end_call","arguments":arguments.to_string()}}]);
        "tool_calls"
    } else {
        "stop"
    };
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"index":0,"delta":delta,"finish_reason":null}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":finish}]})
    )
}
async fn provider<F>(count: usize, respond: F) -> (String, JoinHandle<Vec<Value>>)
where
    F: Fn(usize, &Value) -> String + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let task = tokio::spawn(async move {
        let mut requests = vec![];
        for index in 0..count {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(4), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let request = read_request(&mut socket).await;
            let response = respond(index, &request);
            requests.push(request);
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        }
        requests
    });
    (endpoint, task)
}
async fn bot(greeting: Vec<Value>, scripts: Vec<Vec<Value>>) -> (String, JoinHandle<Vec<Value>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/eval", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = WebSocketSession::accept(
            stream,
            SessionConfig {
                close_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let mut sent = vec![];
        let mut index = 0;
        while let Some(packet) = socket.recv().await {
            let Packet::Text(text) = packet else {
                panic!("expected text")
            };
            let message: Value = serde_json::from_str(&text).unwrap();
            let output = if message["type"] == "client-ready" {
                let mut ready = vec![wire("bot-ready", json!({"version":"2.2.0"}))];
                ready.extend(greeting.clone());
                ready
            } else if message["type"] == "send-text" {
                let result = scripts.get(index).cloned().unwrap_or_default();
                index += 1;
                sent.push(message);
                result
            } else {
                vec![]
            };
            for envelope in output {
                if socket
                    .sender()
                    .send(Packet::Text(envelope.to_string()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
        let _ = socket.finish_and_join().await;
        sent
    });
    (url, task)
}
async fn finish<T>(task: JoinHandle<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn responsive_two_turns_reverse_context_and_unjudged_closing() {
    let (endpoint, http) = provider(2, |index, request| {
        assert!(request["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("impatient shopper"));
        assert!(request["messages"][0]["content"]
            .as_str()
            .unwrap()
            .contains("buy blue mug"));
        assert_eq!(request["tools"][0]["function"]["name"], "end_call");
        let messages = request["messages"].as_array().unwrap();
        assert_eq!(messages.last().unwrap()["role"], "user");
        if index == 0 {
            assert_eq!(messages.last().unwrap()["content"], "Welcome");
            completion("A blue mug please", None)
        } else {
            assert_eq!(messages[2]["role"], "assistant");
            assert_eq!(messages[2]["content"], "A blue mug please");
            assert_eq!(messages.last().unwrap()["content"], "Blue mug ordered");
            completion(
                "Thanks goodbye",
                Some(json!({"success":true,"reason":"ordered"})),
            )
        }
    })
    .await;
    let (url, bot) = bot(
        reply("Welcome"),
        vec![reply("Blue mug ordered"), reply("Goodbye")],
    )
    .await;
    let result = SimulationSession::new(scenario(), runtime(endpoint), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.status, "unjudged", "{result:?}");
    assert_eq!(result.ended_by, "end_call");
    assert_eq!(result.turns, 2);
    assert!(result.goal.is_none());
    assert!(!result.passed());
    assert_eq!(result.transcript.len(), 5);
    assert_eq!(result.transcript[4].text, "Goodbye");
    assert!(result.slowest_text_response_ms.is_some());
    assert_eq!(finish(http).await.len(), 2);
    assert_eq!(
        finish(bot)
            .await
            .iter()
            .map(|m| m["data"]["content"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec!["A blue mug please", "Thanks goodbye"]
    );
}

#[tokio::test]
async fn overlapping_same_name_tools_hold_preamble_and_exclude_cancelled_by_id() {
    let mut greeting = vec![
        wire(
            "llm-function-call-in-progress",
            json!({"tool_call_id":"a","function_name":"order","arguments":{"color":"red"}}),
        ),
        wire(
            "llm-function-call-in-progress",
            json!({"tool_call_id":"b","function_name":"order","arguments":{"color":"blue","extra":1}}),
        ),
    ];
    greeting.extend(reply("Checking"));
    greeting.push(wire(
        "llm-function-call-stopped",
        json!({"tool_call_id":"b","function_name":"order","cancelled":false}),
    ));
    greeting.push(wire(
        "llm-function-call-stopped",
        json!({"tool_call_id":"a","function_name":"order","cancelled":true}),
    ));
    greeting.extend(reply("Blue ordered"));
    let (endpoint, http) = provider(1, |_, request| {
        assert_eq!(request["messages"][1]["content"], "Checking Blue ordered");
        completion("", Some(json!({"success":true,"reason":"done"})))
    })
    .await;
    let (url, bot) = bot(greeting, vec![]).await;
    let mut spec = scenario();
    spec.metrics = vec![
        Metric::FunctionCall {
            name: "blue".into(),
            function: "order".into(),
            arguments: json!({"color":"blue"}),
        },
        Metric::FunctionCall {
            name: "red".into(),
            function: "order".into(),
            arguments: json!({"color":"red"}),
        },
    ];
    let result = SimulationSession::new(spec, runtime(endpoint), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.ended_by, "end_call", "{result:?}");
    assert_eq!(result.status, "failed");
    assert_eq!(result.tools[0].id, "a");
    assert!(result.tools[0].cancelled);
    assert_eq!(result.metrics[0].passed, Some(true));
    assert_eq!(result.metrics[1].passed, Some(false));
    assert_eq!(result.transcript.len(), 1);
    assert!(finish(bot).await.is_empty());
    finish(http).await;
}

#[tokio::test]
async fn independent_judge_overrides_both_persona_claim_directions() {
    for (claim, verdict, status) in [(true, "no", "failed"), (false, "yes", "passed")] {
        let (endpoint, http) = provider(1, move |_, _| {
            completion("", Some(json!({"success":claim,"reason":"my opinion"})))
        })
        .await;
        let (judge, judged) = provider(1, move |_, request| {
            assert!(request.get("tools").is_none());
            let evidence: Value =
                serde_json::from_str(request["messages"][1]["content"].as_str().unwrap()).unwrap();
            assert_eq!(evidence["conversation"][0]["text"], "Blue mug ordered");
            assert!(evidence.get("ordered_bot_tools").is_some());
            assert!(evidence.get("end_call").is_none());
            completion(
                &json!({"verdict":verdict,"reason":"independent evidence"}).to_string(),
                None,
            )
        })
        .await;
        let (url, bot) = bot(reply("Blue mug ordered"), vec![]).await;
        let mut config = runtime(endpoint);
        config.judge = Some(model(judge));
        let result = SimulationSession::new(scenario(), config, url)
            .unwrap()
            .run(Cancellation::new())
            .await;
        assert_eq!(result.status, status, "{result:?}");
        assert_eq!(result.end_call.unwrap().success, claim);
        finish(http).await;
        finish(judged).await;
        finish(bot).await;
    }
}

#[tokio::test]
async fn missing_malformed_and_negative_judge_verdicts_are_distinct() {
    for (answer, kind) in [
        (
            "{\"verdict\":\"none\",\"reason\":\"insufficient\"}",
            "judge_no_verdict",
        ),
        ("not JSON", "judge_error"),
        (
            "{\"verdict\":\"yes\",\"reason\":\"fine\",\"extra\":1}",
            "judge_error",
        ),
    ] {
        let (endpoint, http) = provider(1, |_, _| {
            completion("", Some(json!({"success":true,"reason":"done"})))
        })
        .await;
        let (judge, judged) = provider(1, move |_, _| completion(answer, None)).await;
        let (url, bot) = bot(reply("Hello"), vec![]).await;
        let mut config = runtime(endpoint);
        config.judge = Some(model(judge));
        let result = SimulationSession::new(scenario(), config, url)
            .unwrap()
            .run(Cancellation::new())
            .await;
        assert_eq!(result.status, "error");
        assert!(
            result.failures.iter().any(|failure| failure.kind == kind),
            "{result:?}"
        );
        finish(http).await;
        finish(judged).await;
        finish(bot).await;
    }
}

#[tokio::test]
async fn goodbye_closing_timeout_is_bounded_without_new_inference() {
    let (endpoint, http) = provider(1, |_, _| {
        completion("Bye", Some(json!({"success":false,"reason":"done"})))
    })
    .await;
    let (url, bot) = bot(reply("Hello"), vec![]).await;
    let result = SimulationSession::new(scenario(), runtime(endpoint), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.status, "unjudged", "{result:?}");
    assert_eq!(result.ended_by, "end_call");
    assert_eq!(result.turns, 1);
    assert_eq!(result.transcript.len(), 2);
    assert_eq!(finish(bot).await.len(), 1);
    finish(http).await;
}

#[tokio::test]
async fn cap_keeps_judge_evidence_but_cannot_pass() {
    let (endpoint, http) = provider(1, |_, _| completion("Please order it", None)).await;
    let (judge, judged) = provider(1, |_, _| {
        completion("{\"verdict\":\"yes\",\"reason\":\"achieved\"}", None)
    })
    .await;
    let (url, bot) = bot(reply("Hello"), vec![]).await;
    let mut config = runtime(endpoint);
    config.judge = Some(model(judge));
    let mut spec = scenario();
    spec.max_turns = 1;
    let result = SimulationSession::new(spec, config, url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.ended_by, "max_turns");
    assert_eq!(result.status, "error");
    assert_eq!(result.goal.unwrap().verdict, Verdict::Yes);
    finish(http).await;
    finish(judged).await;
    finish(bot).await;
}

#[tokio::test]
async fn greeting_silence_has_unscored_reply_metrics_and_no_provider_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    let (url, bot) = bot(vec![], vec![]).await;
    let mut spec = scenario();
    spec.max_silence_ms = 30;
    spec.metrics = vec![
        Metric::LongestBotReplyWords {
            name: "words".into(),
            max: 10,
        },
        Metric::SlowestTextResponseMs {
            name: "latency".into(),
            max: 100,
        },
    ];
    let result = SimulationSession::new(spec, runtime(endpoint), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.ended_by, "silence", "{result:?}");
    assert!(result
        .metrics
        .iter()
        .all(|metric| metric.value.is_none() && metric.passed.is_none()));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
    finish(bot).await;
}

#[tokio::test]
async fn already_cancelled_run_reads_no_credentials_or_sockets() {
    let mut config = runtime("http://127.0.0.1:9/completions".into());
    config.caller.api_key_env = Some("PIPECAT_SIMULATION_MISSING_CREDENTIAL_156".into());
    let cancel = Cancellation::new();
    cancel.cancel();
    let result = SimulationSession::new(scenario(), config, "ws://127.0.0.1:9")
        .unwrap()
        .run(cancel)
        .await;
    assert_eq!(result.ended_by, "cancelled");
    assert!(result.transcript.is_empty());
}

#[test]
fn strict_preflight_rejects_unsupported_and_unbounded_forms() {
    let base = json!({"name":"one","persona":"p","goal":"g","success":"s"});
    for (key, value) in [
        ("audio", json!(true)),
        ("repeats", json!(2)),
        ("scenarios", json!([])),
        ("max_turns", json!(0)),
        ("max_duration_ms", json!(0)),
        (
            "metrics",
            json!([{"name":"quality","criteria":"nice","min_score":0.5}]),
        ),
    ] {
        let mut input = base.clone();
        input[key] = value;
        assert!(
            SimulationScenario::from_yaml(input.to_string().as_bytes()).is_err(),
            "{key}"
        );
    }
    assert!(
        SimulationScenario::from_yaml(b"name: a\nname: b\npersona: p\ngoal: g\nsuccess: s")
            .is_err()
    );
    let mut spec = scenario();
    spec.metrics = vec![
        Metric::Turns {
            name: "same".into(),
            max: 2,
        },
        Metric::DurationMs {
            name: "same".into(),
            max: 2,
        },
    ];
    assert!(spec.validate().is_err());
    assert!(SimulationSession::new(
        scenario(),
        runtime("http://example.com/api".into()),
        "ws://127.0.0.1:9"
    )
    .is_err());
}

#[tokio::test]
async fn invalid_caller_tools_and_incomplete_finishes_fail_without_transmission() {
    for response in [completion("",Some(json!({"success":"true","reason":"bad"}))),completion("",Some(json!({"success":true,"reason":"bad","extra":1}))),"data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"partial\"},\"finish_reason\":\"length\"}]}\n\ndata: [DONE]\n\n".into()]{
        let(endpoint,http)=provider(1,move|_,_|response.clone()).await;let(url,bot)=bot(reply("Hello"),vec![]).await;
        let result=SimulationSession::new(scenario(),runtime(endpoint),url).unwrap().run(Cancellation::new()).await;assert_eq!(result.status,"error","{result:?}");assert!(finish(bot).await.is_empty());finish(http).await;
    }
}

#[tokio::test]
async fn cancellation_joins_actual_http_producer_and_disconnects_peer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api", listener.local_addr().unwrap());
    let cancel = Cancellation::new();
    let signal = cancel.clone();
    let http = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        signal.cancel();
        let mut byte = [0; 1];
        let read = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .unwrap();
        assert!(matches!(read, Ok(0) | Err(_)));
    });
    let (url, bot) = bot(reply("Hello"), vec![]).await;
    let result = SimulationSession::new(scenario(), runtime(endpoint), url)
        .unwrap()
        .run(cancel)
        .await;
    assert_eq!(result.ended_by, "cancelled", "{result:?}");
    assert_eq!(result.status, "error");
    assert!(!result.failures.iter().any(|f| f.kind == "cleanup_error"));
    finish(http).await;
    assert!(finish(bot).await.is_empty());
}

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Files(std::path::PathBuf);
impl Files {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pipecat-simulation-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn write(&self, name: &str, value: &impl serde::Serialize) -> String {
        let path = self.0.join(name);
        std::fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        path.to_str().unwrap().into()
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[tokio::test]
async fn compiled_cli_runs_real_loop_and_reports_unjudged_exit_one() {
    let (endpoint, http) = provider(1, |_, _| {
        completion("", Some(json!({"success":true,"reason":"done"})))
    })
    .await;
    let (url, bot) = bot(reply("Hello"), vec![]).await;
    let files = Files::new();
    let spec = files.write("scenario.json", &scenario());
    let config = files.write("runtime.json", &runtime(endpoint));
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_pipecat-eval"))
        .args(["simulate", &spec, &url, &config])
        .output()
        .await
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["status"], "unjudged");
    assert_eq!(report["turns"], 0);
    finish(http).await;
    finish(bot).await;
}
#[tokio::test]
async fn compiled_cli_help_and_invalid_audio_do_zero_network() {
    let help = tokio::process::Command::new(env!("CARGO_BIN_EXE_pipecat-eval"))
        .args(["simulate", "--help"])
        .output()
        .await
        .unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("unjudged"));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let files = Files::new();
    let spec = files.write(
        "bad.json",
        &json!({"name":"a","persona":"b","goal":"c","success":"d","audio":true}),
    );
    let config = files.write(
        "runtime.json",
        &runtime(format!("http://{}/api", listener.local_addr().unwrap())),
    );
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_pipecat-eval"))
        .args([
            "simulate",
            &spec,
            &format!("ws://{}", listener.local_addr().unwrap()),
            &config,
        ])
        .output()
        .await
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn empty_final_response_releases_held_tool_preamble() {
    let mut greeting = vec![wire(
        "llm-function-call-in-progress",
        json!({"tool_call_id":"a","function_name":"lookup","arguments":{}}),
    )];
    greeting.extend(reply("Found it"));
    greeting.push(wire(
        "llm-function-call-stopped",
        json!({"tool_call_id":"a","cancelled":false}),
    ));
    greeting.extend(reply(""));
    let (endpoint, http) = provider(1, |_, request| {
        assert_eq!(request["messages"][1]["content"], "Found it");
        completion("", Some(json!({"success":true,"reason":"done"})))
    })
    .await;
    let (url, bot) = bot(greeting, vec![]).await;
    let result = SimulationSession::new(scenario(), runtime(endpoint), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.ended_by, "end_call", "{result:?}");
    assert_eq!(result.turns, 0);
    finish(http).await;
    finish(bot).await;
}

async fn stalled_provider(
    cancel: Option<Cancellation>,
) -> (String, JoinHandle<()>, tokio::sync::oneshot::Receiver<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api", listener.local_addr().unwrap());
    let (tx, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let _ = tx.send(());
        if let Some(cancel) = cancel {
            cancel.cancel();
        }
        let mut byte = [0; 1];
        let result = tokio::time::timeout(Duration::from_secs(3), socket.read(&mut byte))
            .await
            .unwrap();
        assert!(matches!(result, Ok(0) | Err(_)));
    });
    (endpoint, task, rx)
}

#[tokio::test]
async fn duration_and_silence_deadlines_remain_live_while_provider_stalls() {
    for duration in [true, false] {
        let (endpoint, http, _) = stalled_provider(None).await;
        let (url, bot) = bot(reply("Hello"), vec![]).await;
        let mut spec = scenario();
        if duration {
            spec.max_duration_ms = 600;
        } else {
            spec.max_silence_ms = 400;
        }
        let result = SimulationSession::new(spec, runtime(endpoint), url)
            .unwrap()
            .run(Cancellation::new())
            .await;
        assert_eq!(
            result.ended_by,
            if duration { "max_duration" } else { "silence" },
            "{result:?}"
        );
        assert_eq!(result.status, "error");
        finish(http).await;
        assert!(finish(bot).await.is_empty());
    }
}

#[tokio::test]
async fn cancellation_during_judging_joins_separate_model_owner() {
    let (endpoint, http) = provider(1, |_, _| {
        completion("", Some(json!({"success":true,"reason":"done"})))
    })
    .await;
    let cancel = Cancellation::new();
    let (judge, judged, _) = stalled_provider(Some(cancel.clone())).await;
    let (url, bot) = bot(reply("Hello"), vec![]).await;
    let mut config = runtime(endpoint);
    config.judge = Some(model(judge));
    let result = SimulationSession::new(scenario(), config, url)
        .unwrap()
        .run(cancel)
        .await;
    assert_eq!(result.status, "error", "{result:?}");
    assert!(result.failures.iter().any(|f| f.kind == "judge_error"));
    assert!(result.goal.is_none());
    finish(http).await;
    finish(judged).await;
    finish(bot).await;
}

#[tokio::test]
async fn provider_token_activity_refreshes_silence_not_duration() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/api", listener.local_addr().unwrap());
    let http = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        for _ in 0..32 {
            let data = format!(
                "data: {}\n\n",
                json!({"choices":[{"index":0,"delta":{"content":"a"},"finish_reason":null}]})
            );
            if socket
                .write_all(format!("{:x}\r\n{}\r\n", data.len(), data).as_bytes())
                .await
                .is_err()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        let mut byte = [0; 1];
        let _ = tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
            .await
            .unwrap();
    });
    let (url, bot) = bot(reply("Hello"), vec![]).await;
    let mut spec = scenario();
    spec.max_duration_ms = 600;
    spec.max_silence_ms = 300;
    let result = SimulationSession::new(spec, runtime(endpoint), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.ended_by, "max_duration", "{result:?}");
    assert!(result.transcript.iter().all(|line| line.role != "user"));
    finish(http).await;
    finish(bot).await;
}

#[tokio::test]
async fn readiness_and_closing_cancellation_do_not_start_more_models() {
    for ready in [false, true] {
        let cancel = Cancellation::new();
        let signal = cancel.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap();
            while let Some(Packet::Text(text)) = socket.recv().await {
                let value: Value = serde_json::from_str(&text).unwrap();
                if value["type"] == "client-ready" {
                    if !ready {
                        signal.cancel();
                    } else {
                        for event in std::iter::once(wire("bot-ready", json!({"version":"2.2.0"})))
                            .chain(reply("Hello"))
                        {
                            socket
                                .sender()
                                .send(Packet::Text(event.to_string()))
                                .await
                                .unwrap();
                        }
                    }
                } else if value["type"] == "send-text" {
                    signal.cancel();
                }
            }
            let _ = socket.finish_and_join().await;
        });
        let http = if ready {
            Some(
                provider(1, |_, _| {
                    completion("Bye", Some(json!({"success":true,"reason":"done"})))
                })
                .await,
            )
        } else {
            None
        };
        let endpoint = http
            .as_ref()
            .map(|p| p.0.clone())
            .unwrap_or_else(|| "http://127.0.0.1:9/api".into());
        let result = SimulationSession::new(scenario(), runtime(endpoint), url)
            .unwrap()
            .run(cancel)
            .await;
        assert_eq!(result.ended_by, "cancelled", "{result:?}");
        assert!(!result.passed());
        finish(server).await;
        if let Some((_, task)) = http {
            finish(task).await;
        }
    }
}

#[tokio::test]
async fn unknown_tool_stop_and_abrupt_disconnect_are_execution_errors() {
    let (url, bot) = bot(
        vec![wire(
            "llm-function-call-stopped",
            json!({"tool_call_id":"missing"}),
        )],
        vec![],
    )
    .await;
    let result = SimulationSession::new(scenario(), runtime("http://127.0.0.1:9/api".into()), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.status, "error");
    assert!(result.failures.iter().any(|f| f.reason.contains("unknown")));
    finish(bot).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = WebSocketSession::accept(stream, SessionConfig::default())
            .await
            .unwrap();
        socket.recv().await;
        socket.finish_and_join().await.unwrap();
    });
    let result = SimulationSession::new(scenario(), runtime("http://127.0.0.1:9/api".into()), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.status, "error");
    assert_eq!(result.ended_by, "error");
    finish(task).await;
}

#[tokio::test]
async fn judge_timeout_has_separate_budget_and_no_false_goal_verdict() {
    let (endpoint, http) = provider(1, |_, _| {
        completion("", Some(json!({"success":true,"reason":"done"})))
    })
    .await;
    let (judge, judged, _) = stalled_provider(None).await;
    let (url, bot) = bot(reply("Hello"), vec![]).await;
    let mut config = runtime(endpoint);
    let mut judge = model(judge);
    judge.timeout_ms = 400;
    config.judge = Some(judge);
    let result = SimulationSession::new(scenario(), config, url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.ended_by, "end_call", "{result:?}");
    assert_eq!(result.status, "error");
    assert!(result.goal.is_none());
    assert!(result.failures.iter().any(|f| f.kind == "judge_error"));
    assert!(result.total_elapsed_ms >= result.duration_ms + 300);
    finish(http).await;
    finish(judged).await;
    finish(bot).await;
}
#[tokio::test]
async fn sequential_sessions_never_reuse_caller_history_or_claims() {
    let (endpoint, http) = provider(2, |index, request| {
        assert_eq!(request["messages"].as_array().unwrap().len(), 2);
        assert_eq!(
            request["messages"][1]["content"],
            format!("greeting {index}")
        );
        completion(
            "",
            Some(json!({"success":index==0,"reason":format!("run {index}")})),
        )
    })
    .await;
    let config = runtime(endpoint);
    for index in 0..2 {
        let (url, bot) = bot(reply(&format!("greeting {index}")), vec![]).await;
        let result = SimulationSession::new(scenario(), config.clone(), url)
            .unwrap()
            .run(Cancellation::new())
            .await;
        assert_eq!(result.status, "unjudged");
        assert_eq!(result.transcript.len(), 1);
        assert_eq!(result.end_call.unwrap().reason, format!("run {index}"));
        assert_eq!(result.turns, 0);
        finish(bot).await;
    }
    finish(http).await;
}

fn quality_scenario(min_score: Option<f64>) -> SimulationScenario {
    let mut spec = scenario();
    spec.judged_metrics = vec![JudgedMetric {
        name: "courtesy".into(),
        criterion: "Every reply is polite".into(),
        min_score,
    }];
    spec
}
fn quality_answer(request: &Value, outcome: &str) -> String {
    let evidence: Value =
        serde_json::from_str(request["messages"][1]["content"].as_str().unwrap()).unwrap();
    completion(&json!({"metric":evidence["metric"],"turn":evidence["turn"],"outcome":outcome,"reason":"synthetic evidence"}).to_string(), None)
}

#[tokio::test]
async fn quality_four_turn_prefixes_inclusive_score_and_independent_goal() {
    for (goal, threshold, status) in [
        ("yes", Some(0.75), "passed"),
        ("yes", Some(0.76), "failed"),
        ("no", None, "failed"),
    ] {
        let (endpoint, caller) = provider(4, |i, _| {
            if i == 3 {
                completion(
                    "",
                    Some(json!({"success":true,"reason":"SECRET CALLER CLAIM"})),
                )
            } else {
                completion(&format!("user{i}"), None)
            }
        })
        .await;
        let (judge, judged) = provider(5, move |i, request| {
            if i == 0 {
                return completion(
                    &json!({"verdict":goal,"reason":"independent"}).to_string(),
                    None,
                );
            }
            assert!(request.get("tools").is_none());
            let text = request["messages"][1]["content"].as_str().unwrap();
            assert!(!text.contains("SECRET CALLER CLAIM"));
            let evidence: Value = serde_json::from_str(text).unwrap();
            assert_eq!(evidence["turn"], i);
            assert_eq!(evidence["latest_bot_reply"], format!("bot{}", i - 1));
            let prefix = evidence["conversation"].as_array().unwrap();
            assert_eq!(prefix.len(), (i - 1) * 2);
            for future in i..4 {
                assert!(!text.contains(&format!("bot{future}")));
            }
            quality_answer(
                request,
                if goal == "no" {
                    "meets"
                } else {
                    ["meets", "not_applicable", "fails", "meets"][i - 1]
                },
            )
        })
        .await;
        let (url, bot) = bot(
            reply("bot0"),
            vec![reply("bot1"), reply("bot2"), reply("bot3")],
        )
        .await;
        let mut config = runtime(endpoint);
        config.judge = Some(model(judge));
        let result = SimulationSession::new(quality_scenario(threshold), config, url)
            .unwrap()
            .run(Cancellation::new())
            .await;
        assert_eq!(result.status, status, "{result:?}");
        assert_eq!(
            result.judged_metrics[0].score,
            Some(if goal == "no" { 1.0 } else { 0.75 })
        );
        assert_eq!(result.judged_metrics[0].turns.len(), 4);
        assert_eq!(
            result.judged_metrics[0].passed,
            threshold.map(|t| 0.75 >= t)
        );
        assert_eq!(
            result.failures.iter().any(|f| f.kind == "goal_failure"),
            goal == "no"
        );
        assert_eq!(
            result
                .failures
                .iter()
                .any(|f| f.kind == "criterion_failure"),
            threshold == Some(0.76)
        );
        finish(caller).await;
        finish(judged).await;
        finish(bot).await;
    }
}

#[tokio::test]
async fn quality_tool_preamble_preserves_arrival_order_and_immutable_stops() {
    let greeting = vec![
        wire("bot-llm-started", json!({})),
        wire("bot-llm-text", json!({"text":"Check"})),
        wire("bot-llm-text", json!({"text":"ing"})),
        wire(
            "llm-function-call-in-progress",
            json!({"tool_call_id":"a","function_name":"order","arguments":{"color":"red"}}),
        ),
        wire(
            "llm-function-call-in-progress",
            json!({"tool_call_id":"b","function_name":"order","arguments":{"color":"blue"}}),
        ),
        wire("bot-llm-stopped", json!({})),
        wire(
            "llm-function-call-stopped",
            json!({"tool_call_id":"a","cancelled":true}),
        ),
        wire(
            "llm-function-call-stopped",
            json!({"tool_call_id":"b","cancelled":false}),
        ),
        wire("bot-llm-started", json!({})),
        wire("bot-llm-text", json!({"text":"Blue ordered"})),
        wire("bot-llm-stopped", json!({})),
    ];
    let (endpoint, caller) = provider(1, |_, request| {
        assert_eq!(request["messages"][1]["content"], "Checking Blue ordered");
        completion("", Some(json!({"success":true,"reason":"done"})))
    })
    .await;
    let (judge, judged) = provider(3, |i, request| {
        if i == 0 {
            return completion("{\"verdict\":\"yes\",\"reason\":\"done\"}", None);
        }
        let e: Value =
            serde_json::from_str(request["messages"][1]["content"].as_str().unwrap()).unwrap();
        if i == 1 {
            assert_eq!(e["latest_bot_reply"], "Checking");
            assert_eq!(e["conversation"], json!([]));
        } else {
            let p = e["conversation"].as_array().unwrap();
            assert_eq!(p.len(), 5);
            assert_eq!(p[1]["content"]["id"], "a");
            assert_eq!(p[1]["content"]["stopped"], false);
            assert_eq!(p[2]["content"]["id"], "b");
            assert_eq!(p[2]["content"]["stopped"], false);
            assert_eq!(p[3]["content"]["cancelled"], true);
            assert_eq!(p[4]["content"]["stopped"], true);
        }
        quality_answer(request, "meets")
    })
    .await;
    let (url, bot) = bot(greeting, vec![]).await;
    let mut config = runtime(endpoint);
    config.judge = Some(model(judge));
    let result = SimulationSession::new(quality_scenario(Some(1.0)), config, url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(result.status, "passed", "{result:?}");
    assert_eq!(result.judged_metrics[0].turns.len(), 2);
    finish(caller).await;
    finish(judged).await;
    finish(bot).await;
}

#[tokio::test]
async fn quality_missing_malformed_and_wrong_identity_keep_expected_denominator() {
    for threshold in [None, Some(0.9)] {
        let (endpoint, caller) = provider(4, |i, _| {
            if i == 3 {
                completion("", Some(json!({"success":true,"reason":"done"})))
            } else {
                completion("next", None)
            }
        })
        .await;
        let (judge, judged) = provider(5, |i, r| match i {
            0 => completion("{\"verdict\":\"yes\",\"reason\":\"done\"}", None),
            1 => quality_answer(r, "meets"),
            2 => completion("{}", None),
            3 => completion(
                "{\"metric\":\"wrong\",\"turn\":3,\"outcome\":\"meets\",\"reason\":\"x\"}",
                None,
            ),
            _ => quality_answer(r, "none"),
        })
        .await;
        let (url, bot) = bot(reply("a"), vec![reply("b"), reply("c"), reply("d")]).await;
        let mut config = runtime(endpoint);
        config.judge = Some(model(judge));
        let result = SimulationSession::new(quality_scenario(threshold), config, url)
            .unwrap()
            .run(Cancellation::new())
            .await;
        assert_eq!(
            result.status,
            if threshold.is_none() {
                "passed"
            } else {
                "error"
            },
            "{result:?}"
        );
        let m = &result.judged_metrics[0];
        assert_eq!(m.score, Some(0.25));
        assert_eq!(m.turns.len(), 4);
        assert!(m.turns[1..].iter().all(|v| v.outcome == TurnOutcome::None));
        assert_eq!(
            m.failure_kind.as_deref(),
            threshold.map(|_| "criterion_no_verdict")
        );
        finish(caller).await;
        finish(judged).await;
        finish(bot).await;
    }
}

#[tokio::test]
async fn quality_no_judge_is_unjudged_and_zero_turns_have_no_score() {
    let (endpoint, caller) = provider(1, |_, _| {
        completion("", Some(json!({"success":true,"reason":"done"})))
    })
    .await;
    let (url, bot_task) = bot(reply("hello"), vec![]).await;
    let r = SimulationSession::new(quality_scenario(Some(1.0)), runtime(endpoint), url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(r.status, "unjudged");
    assert_eq!(r.judged_metrics[0].turns.len(), 1);
    assert!(r.judged_metrics[0].score.is_none());
    assert!(r.judged_metrics[0].passed.is_none());
    finish(caller).await;
    finish(bot_task).await;
    let (judge, judged) = provider(1, |_, _| {
        completion("{\"verdict\":\"yes\",\"reason\":\"no turns\"}", None)
    })
    .await;
    let (url, bot) = bot(vec![], vec![]).await;
    let mut spec = quality_scenario(Some(1.0));
    spec.max_silence_ms = 30;
    let mut config = runtime("http://127.0.0.1:9/api".into());
    config.judge = Some(model(judge));
    let r = SimulationSession::new(spec, config, url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(r.ended_by, "silence");
    assert!(r.judged_metrics[0].score.is_none());
    assert!(r.judged_metrics[0].turns.is_empty());
    finish(judged).await;
    finish(bot).await;
}

#[test]
fn quality_preflight_rejects_bounds_duplicates_and_mixed_forms_before_io() {
    for value in [
        json!({"name":"x","criterion":"c","measure":"turns"}),
        json!({"name":" ","criterion":"c"}),
        json!({"name":"x","criterion":" "}),
        json!({"name":"x","criterion":"c","min_score":1.1}),
        json!({"name":"x","criterion":"c","min_score":true}),
    ] {
        let input =
            json!({"name":"n","persona":"p","goal":"g","success":"s","judged_metrics":[value]});
        assert!(SimulationScenario::from_yaml(&serde_json::to_vec(&input).unwrap()).is_err());
    }
    let mut spec = quality_scenario(Some(f64::NAN));
    assert!(spec.validate().is_err());
    spec.judged_metrics[0].min_score = Some(f64::INFINITY);
    assert!(spec.validate().is_err());
    spec.judged_metrics[0].min_score = None;
    spec.metrics.push(Metric::Turns {
        name: "courtesy".into(),
        max: 4,
    });
    assert!(spec.validate().is_err());
    spec.metrics.clear();
    spec.judged_metrics.push(spec.judged_metrics[0].clone());
    assert!(spec.validate().is_err());
    spec.judged_metrics = (0..3)
        .map(|i| JudgedMetric {
            name: format!("q{i}"),
            criterion: "c".into(),
            min_score: None,
        })
        .collect();
    spec.max_judged_turns = 512;
    let mut config = runtime("http://127.0.0.1:9/api".into());
    config.caller.api_key_env = Some("MISSING_BATCH157_PREFLIGHT".into());
    assert!(SimulationSession::new(spec, config, "ws://127.0.0.1:9").is_err());
}

#[tokio::test]
async fn quality_cancel_and_shared_deadline_join_first_middle_last_and_admit_no_later_calls() {
    for cancelled in [true, false] {
        for stalled_turn in 1..=3 {
            let (endpoint, caller) = provider(3, |i, _| {
                if i == 2 {
                    completion("", Some(json!({"success":true,"reason":"done"})))
                } else {
                    completion("next", None)
                }
            })
            .await;
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let judge = format!("http://{}/api", listener.local_addr().unwrap());
            let cancel = Cancellation::new();
            let signal = cancel.clone();
            let judged = tokio::spawn(async move {
                let mut first_admission = None;
                for i in 0..=stalled_turn {
                    let (mut socket, _) =
                        tokio::time::timeout(Duration::from_secs(3), listener.accept())
                            .await
                            .unwrap()
                            .unwrap();
                    let r = read_request(&mut socket).await;
                    let first = *first_admission.get_or_insert_with(std::time::Instant::now);
                    if i == stalled_turn {
                        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
                        if cancelled {
                            signal.cancel();
                        }
                        let mut byte = [0; 1];
                        let closed =
                            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                                .await
                                .unwrap();
                        assert!(matches!(closed, Ok(0) | Err(_)));
                        if !cancelled && stalled_turn == 3 {
                            // Three earlier answers consume at least 450ms. A fresh
                            // 800ms budget here would close no earlier than 1250ms.
                            assert!(first.elapsed() < Duration::from_millis(1100));
                        }
                    } else {
                        // Earlier work consumes the SAME aggregate budget, not a fresh
                        // timeout per completed question. Production guards every admission.
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        let body = if i == 0 {
                            completion("{\"verdict\":\"yes\",\"reason\":\"done\"}", None)
                        } else {
                            quality_answer(&r, "meets")
                        };
                        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                    }
                }
                assert!(
                    tokio::time::timeout(Duration::from_millis(80), listener.accept())
                        .await
                        .is_err()
                );
            });
            let (url, bot) = bot(reply("a"), vec![reply("b"), reply("c")]).await;
            let mut config = runtime(endpoint);
            let mut j = model(judge);
            j.timeout_ms = 800;
            config.judge = Some(j);
            let r = SimulationSession::new(quality_scenario(None), config, url)
                .unwrap()
                .run(cancel)
                .await;
            assert_eq!(r.status, "error", "{r:?}");
            assert_eq!(r.goal.as_ref().unwrap().verdict, Verdict::Yes);
            assert!(r.failures.iter().any(|f| f.kind == "judge_error"));
            assert_eq!(r.judged_metrics[0].turns.len(), 3);
            for (i, v) in r.judged_metrics[0].turns.iter().enumerate() {
                assert_eq!(
                    v.outcome,
                    if i + 1 < stalled_turn {
                        TurnOutcome::Meets
                    } else {
                        TurnOutcome::None
                    }
                );
            }
            if cancelled {
                assert!(r.failures.iter().any(|f| f.kind == "cancelled"));
            }
            assert!(!r.failures.iter().any(|f| f.kind == "cleanup_error"));
            finish(caller).await;
            finish(judged).await;
            finish(bot).await;
        }
    }
}

#[tokio::test]
async fn quality_compiled_cli_serializes_real_verdicts_and_exit_status() {
    for (outcome, exit, status) in [
        ("meets", 0, "passed"),
        ("fails", 1, "failed"),
        ("none", 1, "error"),
    ] {
        let (endpoint, caller) = provider(1, |_, _| {
            completion("", Some(json!({"success":true,"reason":"done"})))
        })
        .await;
        let (judge, judged) = provider(2, move |i, r| {
            if i == 0 {
                completion("{\"verdict\":\"yes\",\"reason\":\"done\"}", None)
            } else {
                quality_answer(r, outcome)
            }
        })
        .await;
        let (url, bot) = bot(reply("hello"), vec![]).await;
        let mut config = runtime(endpoint);
        config.judge = Some(model(judge));
        let files = Files::new();
        let spec = files.write("quality.json", &quality_scenario(Some(1.0)));
        let config = files.write("runtime.json", &config);
        let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_pipecat-eval"))
            .args(["simulate", &spec, &url, &config])
            .output()
            .await
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(exit),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let r: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(r["status"], status);
        assert_eq!(r["goal"]["verdict"], "yes");
        assert_eq!(r["judged_metrics"][0]["turns"][0]["outcome"], outcome);
        assert_eq!(r["judged_metrics"][0]["turns"][0]["turn"], 1);
        finish(caller).await;
        finish(judged).await;
        finish(bot).await;
    }
    let files = Files::new();
    let mut spec = quality_scenario(None);
    spec.max_judged_turns = 0;
    let spec = files.write("invalid.json", &spec);
    let config = files.write("runtime.json", &runtime("http://127.0.0.1:9/api".into()));
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_pipecat-eval"))
        .args(["simulate", &spec, "ws://127.0.0.1:9", &config])
        .output()
        .await
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
}

#[tokio::test]
async fn quality_sequential_sessions_never_share_prefix_history() {
    for name in ["first unique session", "second unique session"] {
        let (endpoint, caller) = provider(1, |_, _| {
            completion("", Some(json!({"success":true,"reason":"done"})))
        })
        .await;
        let (judge, judged) = provider(2, move |i, r| {
            if i == 0 {
                return completion("{\"verdict\":\"yes\",\"reason\":\"done\"}", None);
            }
            let evidence: Value =
                serde_json::from_str(r["messages"][1]["content"].as_str().unwrap()).unwrap();
            assert_eq!(evidence["turn"], 1);
            assert_eq!(evidence["conversation"], json!([]));
            assert_eq!(evidence["latest_bot_reply"], name);
            quality_answer(r, "meets")
        })
        .await;
        let (url, bot) = bot(reply(name), vec![]).await;
        let mut config = runtime(endpoint);
        config.judge = Some(model(judge));
        let r = SimulationSession::new(quality_scenario(Some(1.0)), config, url)
            .unwrap()
            .run(Cancellation::new())
            .await;
        assert_eq!(r.status, "passed", "{r:?}");
        assert_eq!(r.judged_metrics[0].turns.len(), 1);
        finish(caller).await;
        finish(judged).await;
        finish(bot).await;
    }
}

#[tokio::test]
async fn quality_multiple_criteria_keep_identity_order_and_scores_independent() {
    let (endpoint, caller) = provider(2, |i, _| {
        if i == 0 {
            completion("next", None)
        } else {
            completion("", Some(json!({"success":true,"reason":"done"})))
        }
    })
    .await;
    let (judge, judged) = provider(5, |i, r| {
        if i == 0 {
            return completion("{\"verdict\":\"yes\",\"reason\":\"done\"}", None);
        }
        let e: Value = serde_json::from_str(r["messages"][1]["content"].as_str().unwrap()).unwrap();
        assert_eq!(e["metric"], if i <= 2 { "courtesy" } else { "accuracy" });
        assert_eq!(e["turn"], if i % 2 == 1 { 1 } else { 2 });
        assert_eq!(
            e["latest_bot_reply"],
            if i % 2 == 1 { "first" } else { "second" }
        );
        quality_answer(r, ["meets", "not_applicable", "fails", "meets"][i - 1])
    })
    .await;
    let (url, bot) = bot(reply("first"), vec![reply("second")]).await;
    let mut config = runtime(endpoint);
    config.judge = Some(model(judge));
    let mut spec = quality_scenario(Some(1.0));
    spec.judged_metrics.push(JudgedMetric {
        name: "accuracy".into(),
        criterion: "Every reply is accurate".into(),
        min_score: Some(0.75),
    });
    let r = SimulationSession::new(spec, config, url)
        .unwrap()
        .run(Cancellation::new())
        .await;
    assert_eq!(r.status, "failed", "{r:?}");
    assert_eq!(r.judged_metrics.len(), 2);
    assert_eq!(r.judged_metrics[0].name, "courtesy");
    assert_eq!(r.judged_metrics[0].score, Some(1.0));
    assert_eq!(r.judged_metrics[0].passed, Some(true));
    assert_eq!(r.judged_metrics[1].name, "accuracy");
    assert_eq!(r.judged_metrics[1].score, Some(0.5));
    assert_eq!(r.judged_metrics[1].passed, Some(false));
    assert_eq!(
        r.judged_metrics[1].failure_kind.as_deref(),
        Some("criterion_failure")
    );
    finish(caller).await;
    finish(judged).await;
    finish(bot).await;
}
