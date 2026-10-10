//! Source-derived synthetic peers. No live provider or calibration claims.
use async_trait::async_trait;
use pipecat_classifiers::*;
use pipecat_core::{
    Direction, Frame, FrameProcessor, LlmContextData, ProcessorContext, ProcessorError,
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
// Independent reviewer adversarial cases, using the author's socket scaffolding.
#[tokio::test]
async fn reviewer_llm_parser_preserves_unicode_and_rejects_duplicate_values() {
    for reply in [
        r#"{"q":{"probability":0.6},"ignored":"\uD83D\uDE00 { [ \\\""}"#,
        "prefix ```json\n{\"q\":{\"probability\":0.6}}\n``` suffix",
    ] {
        let (classifier, _) = llm(reply);
        assert_eq!(
            classifier
                .ask(&json!("x"), &q(), CallOptions::default())
                .await
                .unwrap()
                .answers["q"],
            ClassifierResult::YesNo(YesNoResult { probability: 0.6 })
        );
    }
    let (classifier, _) = llm(r#"{"q":{"probability":0.1,"probability":0.6}}"#);
    assert_eq!(
        classifier
            .ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Response("not valid JSON")
    );
    for tail in ["", ",BROKEN"] {
        let reply = format!(
            "{{\"q\":{{\"probability\":0.6}},{}\"last\":null{tail}}}",
            (0..10000)
                .map(|i| format!("\"key{i}\":null,"))
                .collect::<String>()
        );
        let scripted = Arc::new(Scripted {
            replies: Mutex::new(VecDeque::from([Some(reply)])),
            ..Default::default()
        });
        let classifier = LlmClassifier::new(
            scripted,
            LlmClassifierConfig {
                limits: ClassifierLimits {
                    max_json_nodes: 16,
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
            ClassifierError::Limit("JSON nodes")
        );
    }
}

#[tokio::test]
async fn reviewer_parser_rejects_invalid_json_grammar_on_real_http() {
    for body in [
        r#"{"answers":{"q":{"noul":01}}}"#,
        r#"{"answers":{"q":{"noul":.5}}}"#,
        r#"{"answers":{"q":{"noul":0.5}},"ignored":"\uD800"}"#,
        r#"{"answers":{"q":{"noul":0.5}},"ignored":[0,]}"#,
        r#"{"answers":{"q":{"noul":0.5}},"ignored":1e9999}"#,
        r#"{"answers":{"q":{"noul":0.5}}} {}"#,
    ] {
        let server = peer(vec![Reply {
            body: body.into(),
            ..yes_response()
        }])
        .await;
        let c = client(&server);
        assert_eq!(
            c.ask(&json!("x"), &q(), CallOptions::default())
                .await
                .unwrap_err(),
            ClassifierError::Response("not valid JSON")
        );
        assert_eq!(c.usage().unwrap(), JevUsage::default());
        assert_eq!(server.count.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn reviewer_response_depth_boundary_and_quoted_delimiters() {
    for (ignored, passes) in [
        (json!("[[[[{{{{\\\""), true),
        (json!({"a":{"b":{"c":null}}}), false),
    ] {
        let server = peer(vec![Reply::json(
            json!({"answers":{"q":{"noul":0.5}}, "ignored":ignored}),
        )])
        .await;
        let mut cfg = JevConfig::new("key");
        cfg.base_url = server.url.clone();
        cfg.limits.max_json_depth = 4;
        let result = JevClient::new(cfg)
            .unwrap()
            .ask(&json!("x"), &q(), CallOptions::default())
            .await;
        if passes {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err(), ClassifierError::Limit("JSON depth"));
        }
    }
}

#[tokio::test]
async fn reviewer_usage_overflow_is_transactional_and_invalid_answers_still_bill() {
    let server = peer(vec![
        Reply::json(json!({"answers":{},"usage":{"input_tokens":u64::MAX-1,"output_tokens":0}})),
        Reply::json(
            json!({"answers":{"q":{"noul":0.5}},"usage":{"input_tokens":0,"output_tokens":1}}),
        ),
        Reply::json(
            json!({"answers":{"q":{"noul":0.5}},"usage":{"input_tokens":1,"output_tokens":0}}),
        ),
        Reply::json(
            json!({"answers":{"q":{"noul":0.5}},"usage":{"input_tokens":-1,"output_tokens":0}}),
        ),
    ])
    .await;
    let c = client(&server);
    assert_eq!(
        c.ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Response("missing or non-object answer")
    );
    assert_eq!(c.usage().unwrap().input_tokens, u64::MAX - 1);
    c.ask(&json!("x"), &q(), CallOptions::default())
        .await
        .unwrap();
    let before = c.usage().unwrap();
    assert_eq!(before.total_tokens().unwrap(), u64::MAX);
    assert_eq!(
        c.ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::UsageOverflow
    );
    assert_eq!(c.usage().unwrap(), before);
    assert_eq!(
        c.ask(&json!("x"), &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Response("invalid usage counter")
    );
    assert_eq!(c.usage().unwrap(), before);
}

#[tokio::test]
async fn reviewer_score_subnormal_normalization_and_order_are_finite() {
    let questions = Questions::from([(
        "q".into(),
        ScoreQuestion {
            instructions: json!("score"),
            levels: vec![json!({"z":"first"}), json!(["middle"]), json!("last")],
        }
        .into(),
    )]);
    for (raw, score, confidence) in [
        (
            r#"{"0":"1e-310","1":"2e-310","2":"3e-310"}"#,
            4.0 / 3.0,
            0.5,
        ),
        (r#"{"0":-1e100,"1":1e100,"2":1e100}"#, 1.5, 0.5),
    ] {
        let (classifier, _) = llm(&format!("{{\"q\":{{\"probabilities\":{raw}}}}}"));
        let answer = classifier
            .ask(&json!("x"), &questions, CallOptions::default())
            .await
            .unwrap();
        let ClassifierResult::Score(actual) = &answer.answers["q"] else {
            panic!()
        };
        assert!(actual.score.is_finite());
        assert!((actual.score - score).abs() < 1e-12);
        assert!((actual.confidence - confidence).abs() < 1e-12);
        assert!((actual.levels.iter().map(|x| x.probability).sum::<f64>() - 1.0).abs() < 1e-12);
        assert_eq!(actual.levels[0].level, json!({"z":"first"}));
    }
}

#[tokio::test]
async fn reviewer_huge_flat_input_is_rejected_without_inference_or_network() {
    let flat = Value::Array(vec![Value::Null; 500_000]);
    let (classifier, scripted) = llm(r#"{"q":{"probability":0.5}}"#);
    assert_eq!(
        classifier
            .ask(&flat, &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Limit("JSON nodes")
    );
    assert!(scripted.requests.lock().unwrap().is_empty());
    let server = peer(vec![yes_response()]).await;
    assert_eq!(
        client(&server)
            .ask(&flat, &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Limit("JSON nodes")
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reviewer_pre_cancelled_and_past_deadline_never_touch_transport() {
    let server = peer(vec![yes_response()]).await;
    let c = client(&server);
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    assert_eq!(
        c.ask(
            &json!("x"),
            &q(),
            CallOptions {
                cancellation: cancellation.clone(),
                ..Default::default()
            }
        )
        .await
        .unwrap_err(),
        ClassifierError::Cancelled
    );
    assert_eq!(
        c.connect(CallOptions {
            cancellation,
            ..Default::default()
        })
        .await
        .unwrap_err(),
        ClassifierError::Cancelled
    );
    assert_eq!(
        c.ask(
            &json!("x"),
            &q(),
            CallOptions {
                deadline: Some(Instant::now() - Duration::from_secs(1)),
                ..Default::default()
            }
        )
        .await
        .unwrap_err(),
        ClassifierError::Deadline
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn reviewer_llm_whole_response_and_pretty_render_are_bounded() {
    let reply = format!("{}{{\"q\":{{\"probability\":0.5}}}}", "p".repeat(1024));
    let scripted = Arc::new(Scripted {
        replies: Mutex::new(VecDeque::from([Some(reply)])),
        ..Default::default()
    });
    let classifier = LlmClassifier::new(
        scripted,
        LlmClassifierConfig {
            limits: ClassifierLimits {
                max_response_bytes: 1024,
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
    let scripted = Arc::new(Scripted::default());
    let classifier = LlmClassifier::new(
        scripted.clone(),
        LlmClassifierConfig {
            instructions: "x".into(),
            limits: ClassifierLimits {
                max_request_bytes: 2048,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let state = Value::Array(vec![json!("x"); 1000]);
    assert_eq!(
        classifier
            .ask(&state, &q(), CallOptions::default())
            .await
            .unwrap_err(),
        ClassifierError::Limit("request bytes")
    );
    assert!(scripted.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reviewer_native_inference_ignores_unsolicited_tool_calls_and_preserves_state() {
    use pipecat_services::{
        ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
    };
    let stream = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"bad-call","type":"function","function":{"name":"launch","arguments":"{}"}}],"content":"{\"q\":{\"probability\":0.5}}"}}]}),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]})
    );
    let mut server = peer(vec![Reply {
        body: stream,
        ..Reply::status(200)
    }])
    .await;
    let mut cfg = ClientConfig::new(Provider::OpenAi, "fixture-secret");
    cfg.endpoint = format!("{}/v1/chat/completions", server.url);
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(cfg).unwrap(),
        CompletionRequest::new(
            "fixture-model",
            Context::new(vec![Message::text(Role::User, "PRIVATE-STATE")]),
        ),
    ));
    let before = serde_json::to_value(processor.request_snapshot().unwrap().context).unwrap();
    let classifier = LlmClassifier::new(processor.clone(), LlmClassifierConfig::default()).unwrap();
    let batch = classifier
        .ask(&json!("public input"), &q(), CallOptions::default())
        .await
        .unwrap();
    assert_eq!(
        batch.answers["q"],
        ClassifierResult::YesNo(YesNoResult { probability: 0.5 })
    );
    let (_, body) = server.requests.recv().await.unwrap();
    assert_eq!(body["tool_choice"], "none");
    assert!(!body.to_string().contains("PRIVATE-STATE"));
    assert_eq!(
        serde_json::to_value(processor.request_snapshot().unwrap().context).unwrap(),
        before
    );
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn reviewer_abort_queued_call_does_not_consume_admission() {
    let mut server = peer(vec![
        Reply {
            delay: Duration::from_secs(60),
            ..yes_response()
        },
        yes_response(),
    ])
    .await;
    let mut cfg = JevConfig::new("key");
    cfg.base_url = server.url.clone();
    cfg.limits.max_in_flight = 1;
    let c = JevClient::new(cfg).unwrap();
    let c1 = c.clone();
    let first =
        tokio::spawn(async move { c1.ask(&json!("first"), &q(), CallOptions::default()).await });
    server.requests.recv().await.unwrap();
    let c2 = c.clone();
    let queued =
        tokio::spawn(async move { c2.ask(&json!("queued"), &q(), CallOptions::default()).await });
    tokio::task::yield_now().await;
    queued.abort();
    queued.await.unwrap_err();
    assert_eq!(server.count.load(Ordering::SeqCst), 1);
    first.abort();
    first.await.unwrap_err();
    tokio::time::timeout(
        Duration::from_secs(1),
        c.ask(&json!("third"), &q(), CallOptions::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(server.count.load(Ordering::SeqCst), 2);
}

#[test]
fn reviewer_derived_schema_limits_match_actual_tree_for_all_question_kinds() {
    fn shape(root: &Value) -> (usize, usize) {
        let mut pending = vec![(root, 1)];
        let mut count = 0;
        let mut maximum = 0;
        while let Some((v, depth)) = pending.pop() {
            count += 1;
            maximum = maximum.max(depth);
            match v {
                Value::Object(v) => pending.extend(v.values().map(|v| (v, depth + 1))),
                Value::Array(v) => pending.extend(v.iter().map(|v| (v, depth + 1))),
                _ => {}
            }
        }
        (count, maximum)
    }
    let scripted = Arc::new(Scripted::default());
    let cases = [
        q(),
        Questions::from([(
            "q".into(),
            ChoiceQuestion {
                instructions: json!("x"),
                options: BTreeMap::from([("a".into(), None)]),
            }
            .into(),
        )]),
        Questions::from([(
            "q".into(),
            ChoiceQuestion {
                instructions: json!("x"),
                options: (0..255).map(|i| (i.to_string(), None)).collect(),
            }
            .into(),
        )]),
        Questions::from([(
            "q".into(),
            ScoreQuestion {
                instructions: json!("x"),
                levels: vec![json!("a"), json!("b")],
            }
            .into(),
        )]),
        Questions::from([(
            "q".into(),
            ScoreQuestion {
                instructions: json!("x"),
                levels: (0..255).map(|i| json!(i.to_string())).collect(),
            }
            .into(),
        )]),
    ];
    for questions in cases {
        let schema = LlmClassifier::new(scripted.clone(), LlmClassifierConfig::default())
            .unwrap()
            .response_schema(&questions)
            .unwrap();
        let (nodes, depth) = shape(&schema);
        for (node_limit, depth_limit, expected) in [
            (nodes - 1, 64, Some(ClassifierError::Limit("JSON nodes"))),
            (nodes, depth - 1, Some(ClassifierError::Limit("JSON depth"))),
            (nodes, depth, None),
        ] {
            let classifier = LlmClassifier::new(
                scripted.clone(),
                LlmClassifierConfig {
                    limits: ClassifierLimits {
                        max_json_nodes: node_limit,
                        max_json_depth: depth_limit,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            )
            .unwrap();
            let answer = classifier.response_schema(&questions);
            match expected {
                Some(e) => assert_eq!(answer.unwrap_err(), e),
                None => assert_eq!(answer.unwrap(), schema),
            }
        }
    }
}

#[tokio::test]
async fn reviewer_score_roundoff_does_not_exceed_last_level() {
    let (classifier, _) = llm(r#"{"q":{"probabilities":{"253":1e-16,"254":1.0}}}"#);
    let batch = classifier
        .score(
            &json!("state"),
            BTreeMap::from([(
                "q".into(),
                ScoreQuestion {
                    instructions: json!("score"),
                    levels: (0..255).map(|index| json!(index.to_string())).collect(),
                },
            )]),
            CallOptions::default(),
        )
        .await
        .unwrap();
    let score = &batch.answers["q"];
    assert!(
        score.score <= 254.0,
        "score must remain within the scale: {}",
        score.score
    );
    assert_eq!(score.score, 254.0);
    assert!(score.levels[253].probability > 0.0);
    assert_eq!(score.levels[254].probability, 1.0);
}
