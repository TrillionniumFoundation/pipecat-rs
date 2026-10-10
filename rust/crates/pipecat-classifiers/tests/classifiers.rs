//! Source-derived synthetic peers. No live provider or calibration claims.
use async_trait::async_trait;
use pipecat_classifiers::*;
use pipecat_core::{
    Direction, Frame, FrameProcessor, LlmContextData, ProcessorContext, ProcessorError, Setting,
    ToolChoice, ToolSet,
};
use pipecat_services::{InferenceOptions, LlmInference, ServiceError};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    time::Instant,
};

#[derive(Clone)]
struct Reply {
    status: u16,
    body: String,
    delay: Duration,
    headers: String,
    chunked: bool,
    disconnect: bool,
}
impl Reply {
    fn json(value: Value) -> Self {
        Self {
            status: 200,
            body: value.to_string(),
            delay: Duration::ZERO,
            headers: String::new(),
            chunked: false,
            disconnect: false,
        }
    }
    fn status(status: u16) -> Self {
        Self {
            status,
            ..Self::json(json!({}))
        }
    }
}
struct Peer {
    url: String,
    requests: mpsc::Receiver<(String, Value)>,
    count: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
    let mut chunk = [0u8; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(p) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break p + 4;
        }
        assert!(bytes.len() < 65536);
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    let body = if length == 0 {
        Value::Null
    } else {
        serde_json::from_slice(&bytes[end..end + length]).unwrap()
    };
    (headers, body)
}
async fn peer(replies: Vec<Reply>) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, requests) = mpsc::channel(128);
    let count = Arc::new(AtomicUsize::new(0));
    let total = count.clone();
    let task = tokio::spawn(async move {
        let mut replies: VecDeque<_> = replies.into();
        let mut children = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut socket, _) = accepted.unwrap();
                    let reply = replies.pop_front().unwrap_or_else(|| Reply::status(500));
                    let tx = tx.clone(); let total = total.clone();
                    children.spawn(async move {
                        let request = read_request(&mut socket).await;
                        total.fetch_add(1, Ordering::SeqCst);
                        tx.send(request).await.unwrap();
                        if reply.disconnect { return; }
                        tokio::time::sleep(reply.delay).await;
                        let content_type = if reply.body.starts_with("data:") { "text/event-stream" } else { "application/json" };
                        let size = if reply.chunked { "Transfer-Encoding: chunked\r\n".into() } else { format!("Content-Length: {}\r\n", reply.body.len()) };
                        if socket.write_all(format!("HTTP/1.1 {} Synthetic\r\nContent-Type: {content_type}\r\n{size}{}Connection: close\r\n\r\n", reply.status, reply.headers).as_bytes()).await.is_err() { return; }
                        if reply.chunked {
                            for bytes in reply.body.as_bytes().chunks(32) {
                                if socket.write_all(format!("{:x}\r\n", bytes.len()).as_bytes()).await.is_err() { return; }
                                if socket.write_all(bytes).await.is_err() { return; }
                                if socket.write_all(b"\r\n").await.is_err() { return; }
                            }
                            let _ = socket.write_all(b"0\r\n\r\n").await;
                        } else { let _ = socket.write_all(reply.body.as_bytes()).await; }
                    });
                }
                Some(result) = children.join_next(), if !children.is_empty() => { result.unwrap(); }
            }
        }
    });
    Peer {
        url,
        requests,
        count,
        task,
    }
}
fn q() -> Questions {
    BTreeMap::from([("q".into(), YesNoQuestion::new("a greeting?").into())])
}
fn mixed() -> Questions {
    BTreeMap::from([
        (
            "greeting".into(),
            YesNoQuestion {
                instructions: json!({"question":"greeting?"}),
                yes: Some(json!({"examples":["hello"]})),
                no: None,
            }
            .into(),
        ),
        (
            "choice".into(),
            ChoiceQuestion {
                instructions: json!(["which?"]),
                options: BTreeMap::from([
                    ("a".into(), Some(json!({"examples":["a"]}))),
                    ("b".into(), None),
                ]),
            }
            .into(),
        ),
        (
            "mood".into(),
            ScoreQuestion {
                instructions: json!("how upset?"),
                levels: vec![json!({"level":"calm"}), json!({"level":"angry"})],
            }
            .into(),
        ),
    ])
}
fn response(answer: Value) -> Value {
    json!({"answers":{"q":answer}, "usage":{"input_tokens":12,"output_tokens":3}})
}
fn yes_response() -> Reply {
    Reply::json(response(json!({"type":"noul","noul":0.9})))
}
fn client(peer: &Peer) -> JevClient {
    let mut c = JevConfig::new("synthetic-secret");
    c.base_url = peer.url.clone();
    JevClient::new(c).unwrap()
}

#[tokio::test]
async fn jev_exact_mixed_protocol_and_provider_metrics() {
    let mut server = peer(vec![Reply::json(json!({"answers":{
        "greeting":{"type":"noul","noul":0.95},
        "choice":{"type":"choice","choice":"b","probabilities":{"b":0.8},"confidence":0.7},
        "mood":{"type":"score","score":0.9,"probabilities":{"0":0.1,"1":0.9},"confidence":0.85}
    },"usage":{"input_tokens":12,"output_tokens":3}}))])
    .await;
    let client = client(&server);
    let classifier = JevClassifier::shared(client.clone())
        .with_name("mixed")
        .unwrap();
    let batch = classifier
        .ask(&json!({"user":"hello"}), &mixed(), CallOptions::default())
        .await
        .unwrap();
    let (headers, body) = server.requests.recv().await.unwrap();
    assert!(headers.starts_with("POST /v1/systemone HTTP/1.1"));
    assert!(headers
        .to_ascii_lowercase()
        .contains("authorization: bearer synthetic-secret"));
    assert_eq!(
        body,
        json!({"model":"jev-1.13.0","state":{"user":"hello"},"questions":{
            "greeting":{"type":"noul","instructions":{"question":"greeting?"},"criteria":{"true":{"examples":["hello"]},"false":""}},
            "choice":{"type":"choice","instructions":["which?"],"criteria":{"a":{"examples":["a"]},"b":null}},
            "mood":{"type":"score","instructions":"how upset?","criteria":[{"level":"calm"},{"level":"angry"}]}
        }})
    );
    let ClassifierResult::Choice(choice) = &batch.answers["choice"] else {
        panic!()
    };
    assert_eq!(choice.probabilities["a"], 0.0);
    assert_eq!(choice.confidence, 0.7);
    let ClassifierResult::Score(score) = &batch.answers["mood"] else {
        panic!()
    };
    assert_eq!(score.score, 0.9);
    assert_eq!(score.probability(&json!({"level":"angry"})), Some(0.9));
    assert_eq!(score.probability(&json!("missing")), None);
    assert_eq!(batch.metrics.provenance, ProbabilityProvenance::JevProvider);
    assert_eq!(batch.metrics.usage.unwrap().total_tokens().unwrap(), 15);
    assert_eq!(batch.metrics.classifier, "mixed");
    assert!(batch.metrics.elapsed > Duration::ZERO);
    assert_eq!(client.usage().unwrap().input_tokens, 12);
}
#[tokio::test]
async fn omitted_and_empty_criteria_preserve_source_mapping() {
    let mut server = peer(vec![yes_response(), yes_response()]).await;
    let client = client(&server);
    client
        .ask(&json!("hello"), &q(), CallOptions::default())
        .await
        .unwrap();
    assert_eq!(
        server.requests.recv().await.unwrap().1["questions"]["q"],
        json!({"type":"noul","instructions":"a greeting?"})
    );
    let q = BTreeMap::from([(
        "q".into(),
        YesNoQuestion {
            instructions: json!("?"),
            yes: Some(json!({})),
            no: Some(json!([])),
        }
        .into(),
    )]);
    client
        .ask(&json!("hello"), &q, CallOptions::default())
        .await
        .unwrap();
    assert_eq!(
        server.requests.recv().await.unwrap().1["questions"]["q"]["criteria"],
        json!({"true":"","false":""})
    );
}
#[tokio::test]
async fn retries_only_429_and_529_with_finite_attempts() {
    let server = peer(vec![Reply::status(429), Reply::status(529), yes_response()]).await;
    let client = client(&server);
    let start = Instant::now();
    client
        .ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap();
    assert!(start.elapsed() >= Duration::from_millis(750));
    assert_eq!(server.count.load(Ordering::SeqCst), 3);
    let server = peer(vec![Reply::status(429), Reply::status(429)]).await;
    let mut config = JevConfig::new("key");
    config.base_url = server.url.clone();
    config.max_retries = 1;
    let error = JevClient::new(config)
        .unwrap()
        .ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap_err();
    assert_eq!(
        error,
        ClassifierError::Busy {
            status: 429,
            attempts: 2
        }
    );
    for status in [400, 401, 408, 500, 502, 503] {
        let server = peer(vec![Reply::status(status), yes_response()]).await;
        assert_eq!(
            super_client(&server)
                .ask(&json!("x"), &q(), CallOptions::default())
                .await
                .unwrap_err(),
            ClassifierError::Http(status)
        );
        assert_eq!(server.count.load(Ordering::SeqCst), 1);
    }
}
fn super_client(peer: &Peer) -> JevClient {
    client(peer)
}
#[tokio::test]
async fn transport_disconnect_and_timeout_are_never_replayed() {
    for disconnect in [true, false] {
        let server = peer(vec![
            Reply {
                disconnect,
                delay: Duration::from_millis(150),
                ..yes_response()
            },
            yes_response(),
        ])
        .await;
        let mut config = JevConfig::new("key");
        config.base_url = server.url.clone();
        config.timeout = Duration::from_millis(40);
        let error = JevClient::new(config)
            .unwrap()
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ClassifierError::Transport | ClassifierError::Deadline
        ));
        assert_eq!(server.count.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn cancellation_and_total_deadline_cover_retry_sleep_and_queue() {
    let mut server = peer(vec![Reply::status(429), yes_response()]).await;
    let client = client(&server);
    let cancel = CancellationToken::new();
    let c = cancel.clone();
    let clone = client.clone();
    let call = tokio::spawn(async move {
        clone
            .ask(
                &json!("x"),
                &q(),
                CallOptions {
                    cancellation: c,
                    deadline: None,
                },
            )
            .await
    });
    server.requests.recv().await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    cancel.cancel();
    assert_eq!(call.await.unwrap().unwrap_err(), ClassifierError::Cancelled);
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    let server = peer(vec![Reply::status(429), yes_response()]).await;
    let start = Instant::now();
    let result = super_client(&server)
        .ask(
            &json!("x"),
            &q(),
            CallOptions {
                deadline: Some(start + Duration::from_millis(40)),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(result.unwrap_err(), ClassifierError::Deadline);
    assert!(start.elapsed() < Duration::from_millis(200));
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    let mut server = peer(vec![Reply {
        delay: Duration::from_millis(150),
        ..yes_response()
    }])
    .await;
    let mut config = JevConfig::new("key");
    config.base_url = server.url.clone();
    config.limits.max_in_flight = 1;
    let client = JevClient::new(config).unwrap();
    let clone = client.clone();
    let first =
        tokio::spawn(async move { clone.ask(&json!("x"), &q(), CallOptions::default()).await });
    server.requests.recv().await.unwrap();
    assert_eq!(
        client
            .ask(
                &json!("x"),
                &q(),
                CallOptions {
                    deadline: Some(Instant::now() + Duration::from_millis(20)),
                    ..Default::default()
                }
            )
            .await
            .unwrap_err(),
        ClassifierError::Deadline
    );
    first.await.unwrap().unwrap();
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn connect_once_concurrent_usage_and_owned_shared_cleanup() {
    let mut replies = vec![Reply::json(json!({"models":[]}))];
    replies.extend((0..8).map(|_| yes_response()));
    let server = peer(replies).await;
    let client = client(&server);
    let (a, b, c) = tokio::join!(
        client.connect(CallOptions::default()),
        client.connect(CallOptions::default()),
        client.connect(CallOptions::default())
    );
    a.unwrap();
    b.unwrap();
    c.unwrap();
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let client = client.clone();
        tasks.spawn(async move { client.ask(&json!("x"), &q(), CallOptions::default()).await });
    }
    while let Some(answer) = tasks.join_next().await {
        answer.unwrap().unwrap();
    }
    assert_eq!(
        client.usage().unwrap(),
        JevUsage {
            input_tokens: 96,
            output_tokens: 24
        }
    );
    let shared = JevClassifier::shared(client.clone());
    shared.cleanup();
    drop(shared);
    assert!(!client.is_closed());
    client.close();
    assert!(client.is_closed());
    assert_eq!(
        client
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Closed
    );
    let owned = JevClassifier::owned(JevConfig::new("key")).unwrap();
    let handle = owned.client().clone();
    drop(owned);
    assert!(handle.is_closed());
}
#[tokio::test]
async fn close_during_request_cancels_and_failed_warmup_is_retryable() {
    let mut server = peer(vec![Reply {
        delay: Duration::from_secs(1),
        ..yes_response()
    }])
    .await;
    let client = client(&server);
    let clone = client.clone();
    let task =
        tokio::spawn(async move { clone.ask(&json!("x"), &q(), CallOptions::default()).await });
    server.requests.recv().await.unwrap();
    client.close();
    assert_eq!(task.await.unwrap().unwrap_err(), ClassifierError::Closed);
    let server = peer(vec![
        Reply::status(500),
        Reply::json(json!({"models":[]})),
        yes_response(),
    ])
    .await;
    let client = super_client(&server);
    assert_eq!(
        client.connect(CallOptions::default()).await.unwrap_err(),
        ClassifierError::Http(500)
    );
    client.connect(CallOptions::default()).await.unwrap();
    client
        .ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap();
}
#[tokio::test]
async fn no_redirects_auth_leaks_or_reflected_secrets() {
    let target = peer(vec![yes_response()]).await;
    let server = peer(vec![Reply {
        status: 307,
        headers: format!("Location: {}/v1/systemone\r\n", target.url),
        body: "synthetic-secret".into(),
        ..yes_response()
    }])
    .await;
    let client = client(&server);
    let error = client
        .ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error, ClassifierError::Http(307));
    assert!(!format!("{error:?} {client:?}").contains("synthetic-secret"));
    assert_eq!(target.count.load(Ordering::SeqCst), 0);
    let config = JevConfig::new("synthetic-secret");
    assert!(!format!("{config:?}").contains("synthetic-secret"));
    for url in [
        "http://example.com",
        "https://secret@example.com",
        "https://example.com/?key=secret",
        "https://example.com/#secret",
        "https://example.com/v1",
    ] {
        let mut config = JevConfig::new("secret");
        config.base_url = url.into();
        let error = JevClient::new(config).unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
}
#[tokio::test]
async fn invalid_answer_usage_type_range_and_nonfinite_are_rejected() {
    for answer in [
        json!({}),
        json!({"type":"choice","noul":0.5}),
        json!({"noul":null}),
        json!({"noul":true}),
        json!({"noul":"NaN"}),
        json!({"noul":"inf"}),
        json!({"noul":1.1}),
        json!({"noul":-0.1}),
    ] {
        let server = peer(vec![Reply::json(response(answer))]).await;
        assert!(JevClassifier::shared(client(&server))
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .is_err());
    }
    for data in [
        json!([]),
        json!({}),
        json!({"answers":{"q":[]}}),
        json!({"answers":{"other":{}}}),
        json!({"answers":{"q":{}},"usage":{"input_tokens":-1}}),
        json!({"answers":{"q":{}},"usage":{"input_tokens":1.5}}),
        json!({"answers":{"q":{}},"usage":{"input_tokens":"12"}}),
        json!({"answers":{"q":{}},"usage":{"input_tokens":u64::MAX,"output_tokens":1}}),
    ] {
        let server = peer(vec![Reply::json(data)]).await;
        assert!(client(&server)
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .is_err());
    }
    for usage in [Value::Null, json!("none"), json!({})] {
        let server = peer(vec![Reply::json(
            json!({"answers":{"q":{"noul":0.5}},"usage":usage}),
        )])
        .await;
        assert_eq!(
            client(&server)
                .ask(&json!("x"), &q(), CallOptions::default())
                .await
                .unwrap()
                .usage,
            JevUsage::default()
        );
    }
    let server = peer(vec![
        Reply::json(json!({"answers":{"q":{}},"usage":{"input_tokens":u64::MAX}})),
        Reply::json(json!({"answers":{"q":{}},"usage":{"input_tokens":1}})),
    ])
    .await;
    let client = client(&server);
    client
        .ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap();
    assert_eq!(
        client
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::UsageOverflow
    );
    assert_eq!(client.usage().unwrap().input_tokens, u64::MAX);
}
#[tokio::test]
async fn request_response_depth_nodes_bytes_and_option_budgets() {
    let server = peer(vec![yes_response()]).await;
    let client = client(&server);
    let too_many = Questions::from([(
        "q".into(),
        ChoiceQuestion {
            instructions: json!("?"),
            options: (0..256).map(|i| (i.to_string(), None)).collect(),
        }
        .into(),
    )]);
    assert!(matches!(
        client
            .ask(&json!("x"), &too_many, CallOptions::default())
            .await,
        Err(ClassifierError::Limit(_))
    ));
    assert_eq!(server.count.load(Ordering::SeqCst), 0);
    let mut config = JevConfig::new("key");
    config.base_url = server.url.clone();
    config.limits.max_request_bytes = 100;
    config.limits.max_json_depth = 4;
    config.limits.max_json_nodes = 8;
    let bounded = JevClient::new(config).unwrap();
    for state in [
        json!("x".repeat(101)),
        json!([[[[["x"]]]]]),
        json!([0, 1, 2, 3, 4, 5, 6, 7, 8]),
    ] {
        assert!(matches!(
            bounded.ask(&state, &q(), CallOptions::default()).await,
            Err(ClassifierError::Limit(_))
        ));
    }
    assert_eq!(server.count.load(Ordering::SeqCst), 0);
    for chunked in [false, true] {
        let server = peer(vec![Reply {
            body: "x".repeat(500),
            chunked,
            ..yes_response()
        }])
        .await;
        let mut config = JevConfig::new("key");
        config.base_url = server.url.clone();
        config.limits.max_response_bytes = 100;
        assert_eq!(
            JevClient::new(config)
                .unwrap()
                .ask(&json!("x"), &q(), CallOptions::default())
                .await
                .unwrap_err(),
            ClassifierError::Limit("response bytes")
        );
    }
    for (data, max_depth, max_nodes) in [
        (json!({"answers":{"q":{"x":[[[[]]]]}}}), 4, 100),
        (json!({"answers":{"q":{"x":[0,1,2,3,4,5,6,7,8,9]}}}), 32, 8),
    ] {
        let server = peer(vec![Reply::json(data)]).await;
        let mut config = JevConfig::new("key");
        config.base_url = server.url.clone();
        config.limits.max_json_depth = max_depth;
        config.limits.max_json_nodes = max_nodes;
        assert!(matches!(
            JevClient::new(config)
                .unwrap()
                .ask(&json!("x"), &q(), CallOptions::default())
                .await,
            Err(ClassifierError::Limit(_))
        ));
    }
}

#[derive(Default)]
struct Scripted {
    replies: Mutex<VecDeque<Option<String>>>,
    requests: Mutex<Vec<(LlmContextData, InferenceOptions)>>,
    delay: Duration,
    fail: bool,
    dropped: Arc<AtomicUsize>,
}
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for Scripted {
    fn name(&self) -> &str {
        "ScriptedClassifier"
    }
    async fn process(
        &self,
        _frame: Frame,
        _direction: Direction,
        _ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        panic!("classification must not use the pipeline");
    }
}
#[async_trait]
impl LlmInference for Scripted {
    async fn run_inference(
        &self,
        context: LlmContextData,
        options: InferenceOptions,
    ) -> std::result::Result<Option<String>, ServiceError> {
        let _drop = DropCount(self.dropped.clone());
        self.requests.lock().unwrap().push((context, options));
        tokio::time::sleep(self.delay).await;
        if self.fail {
            return Err(ServiceError::InvalidRequest("synthetic-secret".into()));
        }
        Ok(self.replies.lock().unwrap().pop_front().flatten())
    }
}
fn llm(reply: &str) -> (LlmClassifier, Arc<Scripted>) {
    let scripted = Arc::new(Scripted {
        replies: Mutex::new(VecDeque::from([Some(reply.into())])),
        ..Default::default()
    });
    (
        LlmClassifier::new(scripted.clone(), LlmClassifierConfig::default()).unwrap(),
        scripted,
    )
}
#[tokio::test]
async fn llm_mixed_named_schema_isolated_context_and_estimated_metrics() {
    let (classifier, scripted) = llm(
        r#"{"greeting":{"probability":0.95},"choice":{"choice":"b","probabilities":{"a":0.2,"b":0.8}},"mood":{"probabilities":{"0":0.5,"1":1}}}"#,
    );
    let batch = classifier
        .ask(&json!({"user":"hello"}), &mixed(), CallOptions::default())
        .await
        .unwrap();
    let requests = scripted.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let (context, options) = &requests[0];
    assert_eq!(context.messages.len(), 1);
    assert!(matches!(&context.tools,ToolSet::Functions(v) if v.is_empty()));
    assert_eq!(context.tool_choice, Some(ToolChoice::None));
    assert_eq!(
        options.settings.system_instruction,
        Setting::Value(DEFAULT_INSTRUCTIONS.into())
    );
    let pipecat_core::MessageContent::Text(prompt) = &context.messages[0].content else {
        panic!()
    };
    assert!(prompt.contains("Input:\n{\n"));
    assert!(prompt.contains("Yes means:"));
    assert!(prompt.contains("JSON response schema"));
    let schema = classifier.response_schema(&mixed()).unwrap();
    assert_eq!(schema["required"], json!(["choice", "greeting", "mood"]));
    assert_eq!(
        schema["properties"]["choice"]["properties"]["choice"]["enum"],
        json!(["a", "b"])
    );
    assert_eq!(schema["additionalProperties"], false);
    let ClassifierResult::Score(score) = &batch.answers["mood"] else {
        panic!()
    };
    assert!((score.score - 2.0 / 3.0).abs() < 1e-12);
    assert!((score.levels.iter().map(|l| l.probability).sum::<f64>() - 1.0).abs() < 1e-12);
    assert_eq!(batch.metrics.provenance, ProbabilityProvenance::LlmEstimate);
    assert_eq!(batch.metrics.usage, None);
}
#[tokio::test]
async fn llm_tolerates_fences_prose_and_single_unwrapped_answers() {
    for reply in [
        r#"{"q":{"probability":0.4}}"#,
        "Sure:\n```json\n{\"q\":{\"probability\":0.4}}\n```\n",
        r#"Here: {"q":{"probability":0.4}} Thanks"#,
        r#"{"probability":0.4}"#,
    ] {
        let (classifier, _) = llm(reply);
        let batch = classifier
            .yes_no(
                &json!("hi"),
                BTreeMap::from([("q".into(), YesNoQuestion::new("?"))]),
                CallOptions::default(),
            )
            .await
            .unwrap();
        assert_eq!(batch.answers["q"].probability, 0.4);
        assert!(!batch.answers["q"].is_yes());
    }
    for (number, expected) in [("2", 1.0), ("-2", 0.0), ("\"0.8\"", 0.8), ("1e308", 1.0)] {
        let (classifier, _) = llm(&format!("{{\"q\":{{\"probability\":{number}}}}}"));
        let batch = classifier
            .ask(&json!("hi"), &q(), CallOptions::default())
            .await
            .unwrap();
        assert_eq!(
            batch.answers["q"],
            ClassifierResult::YesNo(YesNoResult {
                probability: expected
            })
        );
    }
}
#[tokio::test]
async fn llm_missing_wrong_unknown_nonfinite_and_zero_distribution_fail() {
    for reply in [
        "I think yes",
        "not {json}",
        r#"{"q":[]}"#,
        r#"{"probability":true}"#,
        r#"{"probability":"NaN"}"#,
        r#"{"probability":"inf"}"#,
        r#"{"probability":1e999}"#,
        r#"{"q":{"choice":"a"}}"#,
    ] {
        let (classifier, _) = llm(reply);
        assert!(classifier
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .is_err());
    }
    let (classifier, _) = llm(r#"{"other":{"probability":0.5}}"#);
    let mut questions = q();
    questions.insert("second".into(), YesNoQuestion::new("?").into());
    assert!(classifier
        .ask(&json!("x"), &questions, CallOptions::default())
        .await
        .is_err());
    for reply in [
        r#"{"choice":"c"}"#,
        r#"{"choice":"b","probabilities":{"b":"NaN"}}"#,
    ] {
        let (classifier, _) = llm(reply);
        let question = match mixed().remove("choice").unwrap() {
            ClassifierQuestion::Choice(q) => q,
            _ => panic!(),
        };
        assert!(classifier
            .choice(
                &json!("x"),
                BTreeMap::from([("q".into(), question)]),
                CallOptions::default()
            )
            .await
            .is_err());
    }
    let (classifier, _) = llm(r#"{"choice":"b"}"#);
    let question = match mixed().remove("choice").unwrap() {
        ClassifierQuestion::Choice(q) => q,
        _ => panic!(),
    };
    let batch = classifier
        .choice(
            &json!("x"),
            BTreeMap::from([("q".into(), question)]),
            CallOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(batch.answers["q"].confidence, 0.0);
    for reply in [
        r#"{"probabilities":{}}"#,
        r#"{"probabilities":{"0":-1,"1":0}}"#,
    ] {
        let (classifier, _) = llm(reply);
        assert!(classifier
            .score(
                &json!("x"),
                BTreeMap::from([(
                    "q".into(),
                    ScoreQuestion {
                        instructions: json!("?"),
                        levels: vec![json!("a"), json!("b")]
                    }
                )]),
                CallOptions::default()
            )
            .await
            .is_err());
    }
}
#[tokio::test]
async fn llm_timeout_cancel_failure_and_future_drop_release_inference() {
    let scripted = Arc::new(Scripted {
        delay: Duration::from_secs(60),
        ..Default::default()
    });
    let config = LlmClassifierConfig {
        timeout: Duration::from_millis(30),
        ..Default::default()
    };
    let classifier = Arc::new(LlmClassifier::new(scripted.clone(), config).unwrap());
    assert_eq!(
        classifier
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Deadline
    );
    assert_eq!(scripted.dropped.load(Ordering::SeqCst), 1);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        classifier
            .ask(
                &json!("x"),
                &q(),
                CallOptions {
                    cancellation,
                    ..Default::default()
                }
            )
            .await
            .unwrap_err(),
        ClassifierError::Cancelled
    );
    assert_eq!(scripted.requests.lock().unwrap().len(), 1);
    let cloned = classifier.clone();
    let task =
        tokio::spawn(async move { cloned.ask(&json!("x"), &q(), CallOptions::default()).await });
    while scripted.requests.lock().unwrap().len() < 2 {
        tokio::task::yield_now().await;
    }
    task.abort();
    let _ = task.await;
    assert_eq!(scripted.dropped.load(Ordering::SeqCst), 2);
    let scripted = Arc::new(Scripted {
        fail: true,
        ..Default::default()
    });
    let classifier = LlmClassifier::new(scripted, LlmClassifierConfig::default()).unwrap();
    let error = classifier
        .ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error, ClassifierError::Inference);
    assert!(!format!("{error:?}").contains("synthetic-secret"));
}
#[tokio::test]
async fn real_native_llm_inference_uses_fresh_request_without_mutating_service() {
    use pipecat_services::{
        ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
    };
    let stream = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":"{\"q\":{\"probability\":0.9}}"}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"stop"}]})
    );
    let mut server = peer(vec![Reply {
        body: stream,
        ..Reply::status(200)
    }])
    .await;
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic-llm-key");
    config.endpoint = format!("{}/v1/chat/completions", server.url);
    let mut request = CompletionRequest::new(
        "synthetic-model",
        Context::new(vec![Message::text(
            Role::User,
            "LIVE-CONVERSATION-MUST-STAY-PRIVATE",
        )]),
    );
    request.context.tools = Some(vec![pipecat_services::ToolDefinition {
        name: "LIVE_TOOL_MUST_NOT_LEAK".into(),
        description: "private tool".into(),
        parameters: json!({"type":"object"}),
    }]);
    request.context.tool_choice = Some(pipecat_services::ToolChoice::Required);
    let processor = Arc::new(LlmProcessor::new(LlmClient::new(config).unwrap(), request));
    let before = serde_json::to_value(processor.request_snapshot().unwrap().context).unwrap();
    let classifier = LlmClassifier::new(
        processor.clone(),
        LlmClassifierConfig {
            instructions: "Classify only.".into(),
            max_tokens: Some(42),
            ..Default::default()
        },
    )
    .unwrap();
    let batch = classifier
        .ask(&json!("hello"), &q(), CallOptions::default())
        .await
        .unwrap();
    assert_eq!(
        batch.answers["q"],
        ClassifierResult::YesNo(YesNoResult { probability: 0.9 })
    );
    let (_, body) = server.requests.recv().await.unwrap();
    assert!(!body.to_string().contains("LIVE-CONVERSATION"));
    assert!(!body.to_string().contains("LIVE_TOOL"));
    assert_eq!(body["messages"][0]["content"], "Classify only.");
    assert_eq!(body["max_tokens"], 42);
    assert!(body.get("max_completion_tokens").is_none());
    assert!(body.get("tools").is_none() || body["tools"] == json!([]));
    assert_eq!(body["tool_choice"], "none");
    assert_eq!(
        serde_json::to_value(processor.request_snapshot().unwrap().context).unwrap(),
        before
    );
}

#[tokio::test]
async fn jev_choice_and_score_reject_invalid_fields_but_preserve_provider_values() {
    let choice = Questions::from([("q".into(), mixed().remove("choice").unwrap())]);
    for answer in [
        json!({"choice":"unknown","confidence":0.8}),
        json!({"choice":"a"}),
        json!({"choice":"a","confidence":1.1}),
        json!({"choice":"a","confidence":0.8,"probabilities":[]}),
        json!({"choice":"a","confidence":0.8,"probabilities":{"a":"high"}}),
        json!({"choice":"a","confidence":0.8,"probabilities":{"a":-0.1}}),
    ] {
        let server = peer(vec![Reply::json(response(answer))]).await;
        assert!(JevClassifier::shared(client(&server))
            .ask(&json!("x"), &choice, CallOptions::default())
            .await
            .is_err());
    }
    let score = Questions::from([("q".into(), mixed().remove("mood").unwrap())]);
    for answer in [
        json!({"score":2,"confidence":0.8}),
        json!({"score":"NaN","confidence":0.8}),
        json!({"score":0.4,"confidence":true}),
        json!({"score":0.4,"confidence":0.8,"probabilities":{"0":"inf"}}),
    ] {
        let server = peer(vec![Reply::json(response(answer))]).await;
        assert!(JevClassifier::shared(client(&server))
            .ask(&json!("x"), &score, CallOptions::default())
            .await
            .is_err());
    }
    // The score and confidence are provider facts, not recomputed from a partial map.
    let server = peer(vec![Reply::json(response(
        json!({"score":"0.9","confidence":"0.7","probabilities":{"0":"0.2"}}),
    ))])
    .await;
    let batch = JevClassifier::shared(client(&server))
        .ask(&json!("x"), &score, CallOptions::default())
        .await
        .unwrap();
    let ClassifierResult::Score(result) = &batch.answers["q"] else {
        panic!()
    };
    assert_eq!(result.score, 0.9);
    assert_eq!(result.confidence, 0.7);
    assert_eq!(result.levels[1].probability, 0.0);
}

#[tokio::test]
async fn dropping_jev_call_closes_pending_socket_without_a_detached_task() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = JevConfig::new("key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    let client = JevClient::new(config).unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        accepted_tx.send(()).unwrap();
        let mut byte = [0];
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap()
    });
    let caller =
        tokio::spawn(async move { client.ask(&json!("x"), &q(), CallOptions::default()).await });
    accepted_rx.await.unwrap();
    caller.abort();
    let _ = caller.await;
    assert_eq!(server.await.unwrap(), 0);
}

#[tokio::test]
async fn llm_config_request_and_reply_limits_and_escaped_json_depth() {
    let scripted = Arc::new(Scripted::default());
    for config in [
        LlmClassifierConfig {
            timeout: Duration::ZERO,
            ..Default::default()
        },
        LlmClassifierConfig {
            max_tokens: Some(0),
            ..Default::default()
        },
        LlmClassifierConfig {
            limits: ClassifierLimits {
                max_json_depth: 65,
                ..Default::default()
            },
            ..Default::default()
        },
    ] {
        assert!(LlmClassifier::new(scripted.clone(), config).is_err());
    }
    let classifier = LlmClassifier::new(
        scripted.clone(),
        LlmClassifierConfig {
            limits: ClassifierLimits {
                max_request_bytes: 32,
                ..Default::default()
            },
            ..Default::default()
        },
    );
    assert!(classifier.is_err()); // default instructions alone exceed this cap
    let classifier = LlmClassifier::new(
        scripted.clone(),
        LlmClassifierConfig {
            instructions: "C".into(),
            limits: ClassifierLimits {
                max_request_bytes: 128,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        classifier
            .ask(&json!("x"), &q(), CallOptions::default())
            .await,
        Err(ClassifierError::Limit(_))
    ));
    assert!(scripted.requests.lock().unwrap().is_empty());
    let scripted = Arc::new(Scripted {
        replies: Mutex::new(VecDeque::from([Some("x".repeat(65))])),
        ..Default::default()
    });
    let classifier = LlmClassifier::new(
        scripted,
        LlmClassifierConfig {
            limits: ClassifierLimits {
                max_response_bytes: 64,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        classifier
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Limit("response bytes")
    );
    let reply = json!({"q":{"probability":0.5,"ignored":"{{{{\\\"[[[["}}).to_string();
    let (classifier, _) = llm(&reply);
    classifier
        .ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap();
}

struct WrongClassifier;
#[async_trait]
impl Classifier for WrongClassifier {
    async fn ask(
        &self,
        _state: &Value,
        _questions: &Questions,
        _options: CallOptions,
    ) -> Result<ClassificationBatch> {
        Ok(ClassificationBatch {
            answers: BTreeMap::from([(
                "q".into(),
                ClassifierResult::Choice(ChoiceResult {
                    choice: "x".into(),
                    probabilities: BTreeMap::new(),
                    confidence: 0.0,
                }),
            )]),
            metrics: ClassificationMetrics {
                classifier: "wrong".into(),
                model: None,
                elapsed: Duration::ZERO,
                usage: None,
                provenance: ProbabilityProvenance::LlmEstimate,
            },
        })
    }
}
#[tokio::test]
async fn typed_helpers_reject_wrong_result_variants() {
    assert_eq!(
        WrongClassifier
            .yes_no(
                &json!("x"),
                BTreeMap::from([("q".into(), YesNoQuestion::new("?"))]),
                CallOptions::default()
            )
            .await
            .unwrap_err(),
        ClassifierError::Response("wrong result type")
    );
    assert_eq!(
        WrongClassifier
            .score(
                &json!("x"),
                BTreeMap::from([(
                    "q".into(),
                    ScoreQuestion {
                        instructions: json!("?"),
                        levels: vec![json!("a"), json!("b")]
                    }
                )]),
                CallOptions::default()
            )
            .await
            .unwrap_err(),
        ClassifierError::Response("wrong result type")
    );
}

#[tokio::test]
async fn jev_accepts_exactly_255_options_and_finite_numeric_strings() {
    let mut server = peer(vec![Reply::json(response(
        json!({"choice":"254","confidence":"0.8","probabilities":{"254":"0.9"}}),
    ))])
    .await;
    let questions = Questions::from([(
        "q".into(),
        ChoiceQuestion {
            instructions: json!("pick"),
            options: (0..255).map(|i| (i.to_string(), None)).collect(),
        }
        .into(),
    )]);
    let batch = JevClassifier::shared(client(&server))
        .ask(&json!("x"), &questions, CallOptions::default())
        .await
        .unwrap();
    let ClassifierResult::Choice(answer) = &batch.answers["q"] else {
        panic!()
    };
    assert_eq!(answer.probabilities.len(), 255);
    assert_eq!(answer.confidence, 0.8);
    assert_eq!(answer.probabilities["254"], 0.9);
    assert_eq!(
        server.requests.recv().await.unwrap().1["questions"]["q"]["criteria"]
            .as_object()
            .unwrap()
            .len(),
        255
    );
}

#[tokio::test]
async fn jev_final_envelope_counts_null_options_in_node_budget() {
    let server = peer(vec![yes_response()]).await;
    let mut config = JevConfig::new("key");
    config.base_url = server.url.clone();
    config.limits.max_json_nodes = 12;
    let client = JevClient::new(config).unwrap();
    let questions = Questions::from([(
        "q".into(),
        ChoiceQuestion {
            instructions: json!("pick"),
            options: (0..20).map(|i| (i.to_string(), None)).collect(),
        }
        .into(),
    )]);
    assert_eq!(
        client
            .ask(&json!("x"), &questions, CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Limit("JSON nodes")
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn node_budget_admission_precedes_huge_flat_parse_and_duplicate_keys_are_rejected() {
    // Malformed tail distinguishes admission during parsing from allocating the
    // entire Value first: node exhaustion must win long before the invalid tail.
    for body in [
        format!("[{}BROKEN]", "0,".repeat(100_000)),
        format!(
            "{{{}BROKEN}}",
            (0..40_000)
                .map(|i| format!("\"key{i}\":0,"))
                .collect::<String>()
        ),
    ] {
        let server = peer(vec![Reply {
            body,
            ..yes_response()
        }])
        .await;
        let mut config = JevConfig::new("key");
        config.base_url = server.url.clone();
        config.limits.max_json_nodes = 8;
        assert_eq!(
            JevClient::new(config)
                .unwrap()
                .ask(&json!("x"), &q(), CallOptions::default())
                .await
                .unwrap_err(),
            ClassifierError::Limit("JSON nodes")
        );
    }
    let server = peer(vec![Reply {
        body: r#"{"answers":{"q":{"noul":0.2,"noul":0.8}}}"#.into(),
        ..yes_response()
    }])
    .await;
    let error = JevClassifier::shared(client(&server))
        .ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap_err();
    assert_eq!(error, ClassifierError::Response("not valid JSON"));
    for body in [
        r#"{"answers":{"q":{"noul":0.8,}}}"#,
        r#"{"answers":{"q":{"noul":0.8}}} trailing"#,
        r#"{"answers":{"q":{"noul":NaN}}}"#,
        r#"{"answers":{"q":{"noul":0.1},"q":{"noul":0.9}}}"#,
        r#"{"answers":{"q":{"noul":0.1}},"answers":{"q":{"noul":0.9}}}"#,
    ] {
        let server = peer(vec![Reply {
            body: body.into(),
            ..yes_response()
        }])
        .await;
        assert!(matches!(
            client(&server)
                .ask(&json!("x"), &q(), CallOptions::default())
                .await,
            Err(ClassifierError::Response("not valid JSON"))
        ));
    }
}

#[tokio::test]
async fn derived_schema_nodes_depth_and_aggregate_bytes_are_admitted_before_inference() {
    let scripted = Arc::new(Scripted::default());
    let classifier = LlmClassifier::new(
        scripted.clone(),
        LlmClassifierConfig {
            limits: ClassifierLimits {
                max_json_nodes: 13,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        classifier.response_schema(&q()).unwrap_err(),
        ClassifierError::Limit("JSON nodes")
    );
    assert_eq!(
        classifier
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Limit("JSON nodes")
    );
    let classifier = LlmClassifier::new(
        scripted.clone(),
        LlmClassifierConfig {
            limits: ClassifierLimits {
                max_json_nodes: 14,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    classifier.response_schema(&q()).unwrap(); // Exactly fourteen schema nodes.
    let classifier = LlmClassifier::new(
        scripted.clone(),
        LlmClassifierConfig {
            limits: ClassifierLimits {
                max_json_depth: 5,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        classifier.response_schema(&q()).unwrap_err(),
        ClassifierError::Limit("JSON depth")
    );
    let many = Questions::from_iter((0..64).map(|i| {
        (
            i.to_string(),
            ChoiceQuestion {
                instructions: json!("?"),
                options: (0..255).map(|i| (i.to_string(), None)).collect(),
            }
            .into(),
        )
    }));
    let classifier = LlmClassifier::new(scripted.clone(), LlmClassifierConfig::default()).unwrap();
    assert_eq!(
        classifier.response_schema(&many).unwrap_err(),
        ClassifierError::Limit("JSON nodes")
    );
    let classifier = LlmClassifier::new(
        scripted.clone(),
        LlmClassifierConfig {
            instructions: "C".into(),
            limits: ClassifierLimits {
                max_request_bytes: 300,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(
        classifier
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Limit("request bytes")
    );
    assert!(scripted.requests.lock().unwrap().is_empty());
}
