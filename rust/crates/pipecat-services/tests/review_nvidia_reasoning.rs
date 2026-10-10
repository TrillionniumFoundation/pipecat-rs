//! Independent NVIDIA source contracts over actual localhost HTTP/SSE and pipelines.
use pipecat_core::{
    Direction, Frame, FrameData, Metric, Pipeline, PipelineConfig, PipelineWorker, WorkerState,
};
use pipecat_services::{
    presets::{OpenAiPreset, OpenAiPresetConfig},
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut buffer = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(at) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[end..end + length]).unwrap()
}
fn record(value: Value) -> String {
    format!("data: {value}\n\n")
}
fn delta(value: Value) -> Value {
    json!({"choices":[{"index":0,"delta":value}]})
}
fn text(value: &str) -> Value {
    delta(json!({"content":value}))
}
fn wire(events: Vec<Value>, done: bool) -> Vec<u8> {
    let mut body: String = events.into_iter().map(record).collect();
    if done {
        body.push_str("data: [DONE]\n\n");
    }
    body.into_bytes()
}
async fn try_write_chunk(socket: &mut TcpStream, chunk: &[u8]) -> std::io::Result<()> {
    socket
        .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
        .await?;
    socket.write_all(chunk).await?;
    socket.write_all(b"\r\n").await
}
async fn write_chunk(socket: &mut TcpStream, chunk: &[u8]) {
    try_write_chunk(socket, chunk).await.unwrap();
}
async fn header(socket: &mut TcpStream) {
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
}
fn config(listener: &TcpListener, nvidia: bool) -> ClientConfig {
    let mut config = if nvidia {
        ClientConfig::openai_compatible(
            OpenAiPresetConfig::new(OpenAiPreset::Nvidia),
            "synthetic-local-review",
        )
        .unwrap()
    } else {
        ClientConfig::new(Provider::OpenAi, "synthetic-local-review")
    };
    config.endpoint = format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    );
    config.request_timeout = Duration::from_secs(5);
    config.idle_timeout = Duration::from_secs(3);
    config
}
async fn pipeline(config: ClientConfig) -> PipelineWorker {
    let processor = LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new(
            "review-model",
            Context::new(vec![Message::text(Role::User, "question")]),
        ),
    );
    let mut params = PipelineConfig::default();
    params.start.enable_metrics = true;
    params.start.enable_usage_metrics = true;
    let worker = Pipeline::new(vec![Arc::new(processor)])
        .start(params)
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    worker
}
#[derive(Default, Debug)]
struct Trace {
    events: Vec<String>,
    errors: Vec<String>,
    usages: Vec<(u64, u64, u64)>,
    ttfb: usize,
    failed: bool,
}
impl Trace {
    fn push(&mut self, frame: Frame) -> bool {
        match frame.data {
            FrameData::LlmThoughtStart | FrameData::LlmThoughtStartDetailed(_) => {
                self.events.push("start".into())
            }
            FrameData::LlmThoughtEnd | FrameData::LlmThoughtEndDetailed(_) => {
                self.events.push("end".into())
            }
            FrameData::LlmThoughtText(text) => {
                assert_eq!(text.skip_tts, Some(true));
                assert!(!text.append_to_context);
                self.events.push(format!("thought:{}", text.text));
            }
            FrameData::LlmText(text) => self.events.push(format!("visible:{}", text.text)),
            FrameData::FunctionCallsStarted(calls) => {
                for call in calls {
                    self.events.push(format!(
                        "tool:{}:{}:{}",
                        call.tool_call_id, call.function_name, call.arguments
                    ));
                }
            }
            FrameData::Metrics(metrics) => {
                for metric in metrics {
                    match metric {
                        Metric::Ttfb { .. } => self.ttfb += 1,
                        Metric::LlmUsage {
                            prompt_tokens,
                            completion_tokens,
                            total_tokens,
                            ..
                        } => self
                            .usages
                            .push((prompt_tokens, completion_tokens, total_tokens)),
                        _ => {}
                    }
                }
            }
            FrameData::Error(error) => self.errors.push(error.error),
            FrameData::LlmResponseEnd => {
                self.failed = frame.metadata.get("failed") == Some(&json!(true));
                return true;
            }
            _ => {}
        }
        false
    }
    fn compact(&self) -> Vec<String> {
        let mut result: Vec<String> = vec![];
        for value in &self.events {
            if let Some((kind, content)) = value.split_once(':') {
                if ["thought", "visible"].contains(&kind) {
                    if let Some(last) = result.last_mut() {
                        if last.starts_with(&format!("{kind}:")) {
                            last.push_str(content);
                            continue;
                        }
                    }
                }
            }
            result.push(value.clone());
        }
        result
    }
}
async fn finish(mut worker: PipelineWorker, peer: JoinHandle<()>) -> Trace {
    tokio::time::timeout(Duration::from_secs(7), async {
        let mut trace = Trace::default();
        loop {
            if trace.push(worker.recv().await.unwrap().frame) {
                break;
            }
        }
        worker.end().await.unwrap();
        assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
        peer.await.unwrap();
        trace
    })
    .await
    .unwrap()
}
async fn trace(events: Vec<Value>, done: bool, byte_fragments: bool, nvidia: bool) -> Trace {
    trace_with_policy(events, done, byte_fragments, nvidia, false).await
}
async fn rejected_trace(
    events: Vec<Value>,
    done: bool,
    byte_fragments: bool,
    nvidia: bool,
) -> Trace {
    trace_with_policy(events, done, byte_fragments, nvidia, true).await
}
async fn trace_with_policy(
    events: Vec<Value>,
    done: bool,
    byte_fragments: bool,
    nvidia: bool,
    allow_early_rejection: bool,
) -> Trace {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener, nvidia);
    let body = wire(events, done);
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let body_request = read_request(&mut socket).await;
        assert_eq!(body_request["stream"], true);
        header(&mut socket).await;
        let chunk_size = if byte_fragments { 1 } else { body.len() };
        for part in body.chunks(chunk_size) {
            if let Err(error) = try_write_chunk(&mut socket, part).await {
                assert!(
                    allow_early_rejection,
                    "positive SSE fixture closed before all records: {error}"
                );
                assert!(matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                ));
                return;
            }
        }
        if let Err(error) = socket.write_all(b"0\r\n\r\n").await {
            assert!(matches!(
                error.kind(),
                std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
            ));
        }
    });
    finish(pipeline(config).await, peer).await
}
fn clean(actual: &Trace) {
    assert!(actual.errors.is_empty(), "{actual:?}");
    assert!(!actual.failed, "{actual:?}");
}

#[tokio::test]
async fn review_every_unicode_content_split_and_network_byte_split_preserves_one_leading_thought() {
    let source = "<think>甲🙂é</think>answer<think>literal</think>";
    for split in source
        .char_indices()
        .map(|(at, _)| at)
        .chain([source.len()])
    {
        let actual = trace(
            vec![text(&source[..split]), text(&source[split..])],
            true,
            true,
            true,
        )
        .await;
        clean(&actual);
        assert_eq!(
            actual.compact(),
            [
                "start",
                "thought:甲🙂é",
                "end",
                "visible:answer<think>literal</think>"
            ],
            "split {split}"
        );
        assert_eq!(actual.ttfb, 1);
    }
}

#[tokio::test]
async fn review_exact_start_grammar_does_not_strip_near_misses_or_later_tags() {
    for source in [
        " <think>visible</think>",
        "\n<think>visible</think>",
        "\u{feff}<think>visible</think>",
        "<Think>visible</Think>",
        "<think >visible</think>",
        "x<think>visible</think>",
        "<thinking>visible",
        "</think>visible",
    ] {
        let actual = trace(
            source.chars().map(|c| text(&c.to_string())).collect(),
            true,
            false,
            true,
        )
        .await;
        clean(&actual);
        assert_eq!(actual.compact(), [format!("visible:{source}")]);
    }
}

#[tokio::test]
async fn review_empty_and_unclosed_thoughts_have_explicit_source_boundaries() {
    for (source, expected) in [
        ("<think></think>", vec!["start", "end"]),
        ("<think>", vec!["start", "end"]),
        ("<think>甲🙂ab", vec!["start", "thought:甲🙂ab", "end"]),
        ("<thin", vec!["visible:<thin"]),
        (
            "<think>secret</thi",
            vec!["start", "thought:secret</thi", "end"],
        ),
    ] {
        let actual = trace(vec![text(source)], true, true, true).await;
        clean(&actual);
        assert_eq!(actual.compact(), expected, "{source}");
    }
}

#[tokio::test]
async fn review_abnormal_eof_discards_unicode_suffix_and_partial_open_without_content_leaks() {
    for (source, expected) in [
        ("<thin", vec![]),
        ("<think>甲乙🙂ab", vec!["start", "end"]),
        (
            "<think>甲乙🙂12345678",
            vec!["start", "thought:甲乙🙂1", "end"],
        ),
    ] {
        let actual = trace(vec![text(source)], false, true, true).await;
        assert!(!actual.errors.is_empty(), "{actual:?}");
        assert!(actual.failed);
        assert_eq!(actual.compact(), expected, "{source}");
    }
}

#[tokio::test]
async fn review_truthy_fallback_and_structured_inline_boundaries_are_independent() {
    let actual = trace(vec![
        delta(json!({"reasoning_content":"", "reasoning":"fallback", "content":"<th"})),
        delta(json!({"reasoning_content":"primary", "reasoning":"ignored", "content":"ink></think>A"})),
        delta(json!({"content":""})),
        json!({"choices":[],"usage":{"prompt_tokens":7,"completion_tokens":4,"total_tokens":11}}),
        delta(json!({"content":"B"})),
        delta(json!({"reasoning_content":null,"reasoning":"later"})),
    ], true, true, true).await;
    clean(&actual);
    assert_eq!(
        actual.compact(),
        [
            "start",
            "thought:fallbackprimary",
            "start",
            "end",
            "visible:A",
            "end",
            "visible:B",
            "start",
            "thought:later",
            "end"
        ]
    );
    assert_eq!(actual.usages, [(7, 4, 11)]);
}

#[tokio::test]
async fn review_false_reasoning_closes_structured_before_opening_inline_thought() {
    let actual = trace(vec![
        delta(json!({"reasoning":"structured"})),
        delta(json!({"reasoning_content":"", "reasoning":"", "content":"<think>inline</think>answer"})),
    ], true, false, true).await;
    clean(&actual);
    assert_eq!(
        actual.compact(),
        [
            "start",
            "thought:structured",
            "end",
            "start",
            "thought:inline",
            "end",
            "visible:answer"
        ]
    );
}

#[tokio::test]
async fn review_finalization_closes_inline_then_structured_and_keeps_empty_lifecycle() {
    let actual = trace(
        vec![delta(
            json!({"reasoning_content":"structured", "content":"<think>"}),
        )],
        true,
        false,
        true,
    )
    .await;
    clean(&actual);
    assert_eq!(
        actual.events,
        ["start", "thought:structured", "start", "end", "end"]
    );
}

#[tokio::test]
async fn review_tool_and_usage_only_records_preserve_reasoning_and_metadata() {
    let actual = trace(vec![
        delta(json!({"reasoning":"private"})),
        delta(json!({"tool_calls":[{"index":0,"id":"call","function":{"name":"lookup","arguments":"{\"n\":"}}]})),
        json!({"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}),
        delta(json!({"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]})),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":2,"completion_tokens":5,"total_tokens":7}}),
    ], true, true, true).await;
    clean(&actual);
    assert_eq!(
        actual.compact(),
        [
            "start",
            "thought:private",
            "end",
            "tool:call:lookup:{\"n\":1}"
        ]
    );
    assert_eq!(actual.usages, [(2, 5, 7)]);
}

#[tokio::test]
async fn review_generic_openai_keeps_literal_tags_and_its_existing_field_presence_precedence() {
    let actual = trace(vec![delta(json!({"reasoning_content":"", "reasoning":"must-not-fallback", "content":"<think>literal</think>answer"}))], true, true, false).await;
    clean(&actual);
    assert_eq!(actual.compact(), ["visible:<think>literal</think>answer"]);
}

#[tokio::test]
async fn review_choice_record_ttfb_arrives_before_buffered_prefix_is_resolved() {
    for first in [
        delta(json!({"role":"assistant"})),
        text("<th"),
        json!({"choices":[{"index":0,"delta":null}]}),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = config(&listener, true);
        let (release, wait) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            header(&mut socket).await;
            write_chunk(&mut socket, record(first).as_bytes()).await;
            wait.await.unwrap();
            write_chunk(&mut socket, b"data: [DONE]\n\n").await;
            socket.write_all(b"0\r\n\r\n").await.unwrap();
        });
        let mut worker = pipeline(config).await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match worker.recv().await.unwrap().frame.data {
                    FrameData::Metrics(metrics)
                        if metrics.iter().any(|m| matches!(m, Metric::Ttfb { .. })) =>
                    {
                        break
                    }
                    FrameData::LlmResponseEnd | FrameData::LlmText(_) => {
                        panic!("completed before source choice TTFB")
                    }
                    FrameData::Error(error) => panic!("{}", error.error),
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        release.send(()).unwrap();
        let remaining = finish(worker, peer).await;
        clean(&remaining);
        assert_eq!(remaining.ttfb, 0, "one TTFB per response");
    }
}

#[tokio::test]
async fn review_usage_only_record_does_not_fabricate_ttfb() {
    let actual = trace(vec![json!({"choices":[], "usage":{"prompt_tokens":1,"completion_tokens":0,"total_tokens":1}})], true, false, true).await;
    clean(&actual);
    assert_eq!(actual.ttfb, 0);
    assert_eq!(actual.usages, [(1, 0, 1)]);
    assert!(actual.events.is_empty());
}

#[tokio::test]
async fn review_rejected_record_does_not_commit_unobserved_thought_lifecycle() {
    let actual = rejected_trace(
        vec![
            text("<think>"),
            delta(json!({"content":"</think>visible", "tool_calls":"invalid"})),
        ],
        true,
        false,
        true,
    )
    .await;
    assert!(actual.failed);
    assert!(!actual.errors.is_empty());
    assert_eq!(actual.events, ["start", "end"]);
}

#[tokio::test]
async fn review_response_state_is_fresh_for_each_request_on_one_processor() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener, true);
    let peer = tokio::spawn(async move {
        for content in ["ordinary", "<think></think>second", "<thi"] {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            let body = wire(vec![text(content)], true);
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    let mut worker = pipeline(config).await;
    let mut results = vec![];
    for response in 0..3 {
        if response > 0 {
            worker
                .sender()
                .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
                .await
                .unwrap();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            let mut result = Trace::default();
            loop {
                if result.push(worker.recv().await.unwrap().frame) {
                    break result;
                }
            }
        })
        .await
        .unwrap();
        clean(&result);
        assert_eq!(result.ttfb, 1);
        results.push(result.compact());
    }
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    peer.await.unwrap();
    assert_eq!(
        results,
        vec![
            vec!["visible:ordinary"],
            vec!["start", "end", "visible:second"],
            vec!["visible:<thi"]
        ]
    );
}

#[tokio::test]
async fn review_cancelled_direct_stream_closes_both_channels_without_flushing_secret_suffix() {
    use pipecat_services::{ServiceError, StreamEvent};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = config(&listener, true);
    let (closed_tx, closed_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        header(&mut socket).await;
        write_chunk(
            &mut socket,
            record(delta(
                json!({"reasoning":"structured","content":"<think>甲🙂ab"}),
            ))
            .as_bytes(),
        )
        .await;
        let mut byte = [0; 1];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        closed_tx.send(()).unwrap();
    });
    let mut stream = LlmClient::new(config)
        .unwrap()
        .stream(CompletionRequest::new("review", Context::default()))
        .unwrap();
    let mut starts = 0;
    while starts < 2 {
        let event = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if matches!(event, StreamEvent::ThoughtStart) {
            starts += 1;
        }
    }
    stream.cancel();
    let mut ends = 0;
    let mut cancelled = 0;
    let mut finished = 0;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(event) = stream.next().await {
            match event {
                Ok(StreamEvent::ThoughtEnd) => ends += 1,
                Err(ServiceError::Cancelled) => cancelled += 1,
                Ok(StreamEvent::Finished { .. }) => finished += 1,
                Ok(StreamEvent::ThoughtDelta(_) | StreamEvent::TextDelta(_)) => {
                    panic!("buffer leaked on cancellation")
                }
                other => panic!("unexpected cancellation event: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!((ends, cancelled, finished), (2, 1, 1));
    closed_rx.await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn review_capacity_one_cancellation_balances_only_delivered_boundaries() {
    use pipecat_services::{ServiceError, StreamEvent};
    for cancel_after_start in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config(&listener, true);
        config.channel_capacity = 1;
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            header(&mut socket).await;
            write_chunk(
                &mut socket,
                record(text("<think></think>NEVER-VISIBLE-AFTER-CANCEL")).as_bytes(),
            )
            .await;
            let mut byte = [0; 1];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        let mut stream = LlmClient::new(config)
            .unwrap()
            .stream(CompletionRequest::new("review", Context::default()))
            .unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap(), StreamEvent::Started);
        assert_eq!(
            stream.next().await.unwrap().unwrap(),
            StreamEvent::ResponseProgress
        );
        if cancel_after_start {
            assert_eq!(
                stream.next().await.unwrap().unwrap(),
                StreamEvent::ThoughtStart
            );
        }
        stream.cancel();
        let mut ends = 0;
        let mut cancelled = 0;
        let mut finished = 0;
        tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(event) = stream.next().await {
                match event {
                    Ok(StreamEvent::ThoughtEnd) => ends += 1,
                    Err(ServiceError::Cancelled) => cancelled += 1,
                    Ok(StreamEvent::Finished { .. }) => finished += 1,
                    other => panic!("undelivered boundary/text escaped cancellation: {other:?}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(
            (ends, cancelled, finished),
            (usize::from(cancel_after_start), 1, 1)
        );
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn review_json_falsey_fallback_and_truthy_nontext_rejection_are_explicit() {
    for primary in [
        Value::Null,
        json!(""),
        json!(false),
        json!(0),
        json!(0.0),
        json!([]),
        json!({}),
    ] {
        let actual = trace(
            vec![delta(
                json!({"reasoning_content":primary,"reasoning":"fallback"}),
            )],
            true,
            false,
            true,
        )
        .await;
        clean(&actual);
        assert_eq!(actual.compact(), ["start", "thought:fallback", "end"]);
    }
    for primary in [json!(true), json!(1), json!([1]), json!({"x":1})] {
        let actual = rejected_trace(vec![delta(json!({"reasoning_content":primary,"reasoning":"must-not-fallback","content":"must-not-leak"}))],true,false,true).await;
        assert!(actual.failed);
        assert!(!actual.errors.is_empty());
        assert!(actual.events.is_empty(), "{actual:?}");
    }
}
