//! Opt-in numeric Usage metadata projection; exact wire shape and real HTTP/SSE.
use pipecat_core::*;
use pipecat_rtvi::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    time::timeout,
};
const WAIT: Duration = Duration::from_secs(5);
fn base() -> Value {
    json!({"prompt_tokens":7,"completion_tokens":5,"total_tokens":12})
}
fn metric() -> Metric {
    Metric::LlmUsage {
        processor: "llm".into(),
        prompt_tokens: 7,
        completion_tokens: 5,
        total_tokens: 12,
    }
}
fn observer(enabled: bool) -> RtviObserver {
    RtviObserver::new(Default::default(), None)
        .unwrap()
        .with_llm_usage_details(enabled)
}
fn frame(usage: Value) -> Frame {
    let mut f = Frame::new(FrameData::Metrics(vec![metric()]));
    f.metadata.insert("llm_usage".into(), usage);
    f
}
fn wire(observer: &RtviObserver, frame: &Frame) -> Vec<Value> {
    observer
        .observe(frame, Observation::new("llm", Direction::Downstream))
        .unwrap()
        .into_iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
fn tokens(wire: &[Value]) -> &Value {
    &wire[0]["data"]["tokens"]
}
#[test]
fn disabled_wire_is_unchanged_and_optional_zero_is_preserved() {
    let mut usage = base();
    usage["cache_read_input_tokens"] = json!(0);
    usage["cache_creation_input_tokens"] = json!(3);
    usage["reasoning_tokens"] = json!(2);
    let f = frame(usage.clone());
    let original = f.clone();
    assert_eq!(tokens(&wire(&observer(false), &f)), &json!([base()]));
    let o = observer(true);
    assert_eq!(tokens(&wire(&o, &f)), &json!([usage]));
    assert_eq!(f, original);
    assert!(wire(&o, &f).is_empty()); // Same frame is not counted twice.
    o.reset();
    let reset = wire(&o, &f);
    assert_eq!(tokens(&reset)[0]["cache_read_input_tokens"], 0);
    assert_eq!(tokens(&reset)[0]["reasoning_tokens"], 2); // Config survives reset.
}
#[test]
fn missing_and_null_details_omit_fields_and_unknown_metadata_never_leaks() {
    let mut usage = base();
    usage["cache_read_input_tokens"] = Value::Null;
    usage["reasoning_tokens"] = Value::Null;
    usage["model"] = json!("private-model");
    usage["prompt"] = json!("private text");
    usage["input_audio_tokens"] = json!(99);
    usage["unknown"] = json!("x".repeat(100_000));
    let f = frame(usage);
    let output = wire(&observer(true), &f);
    assert_eq!(tokens(&output), &json!([base()]));
    assert!(!output[0].to_string().contains("private"));
}
#[test]
fn any_known_invalid_field_or_mismatched_base_rejects_whole_extension() {
    for key in [
        "prompt_tokens",
        "completion_tokens",
        "total_tokens",
        "cache_read_input_tokens",
        "cache_creation_input_tokens",
        "reasoning_tokens",
    ] {
        for bad in [
            json!(-1),
            json!(1.5),
            json!("1"),
            json!(true),
            json!([]),
            json!({}),
            serde_json::from_str::<Value>("18446744073709551616").unwrap(),
        ] {
            let mut usage = base();
            usage["cache_read_input_tokens"] = json!(9);
            usage["reasoning_tokens"] = json!(3);
            usage[key] = bad;
            assert_eq!(
                tokens(&wire(&observer(true), &frame(usage))),
                &json!([base()]),
                "{key}"
            );
        }
    }
    for key in ["prompt_tokens", "completion_tokens", "total_tokens"] {
        let mut usage = base();
        usage["reasoning_tokens"] = json!(1);
        usage[key] = json!(999);
        assert_eq!(
            tokens(&wire(&observer(true), &frame(usage))),
            &json!([base()])
        );
        let mut usage = base();
        usage.as_object_mut().unwrap().remove(key);
        assert_eq!(
            tokens(&wire(&observer(true), &frame(usage))),
            &json!([base()])
        );
    }
    for usage in [Value::Null, json!([]), json!(42)] {
        assert_eq!(
            tokens(&wire(&observer(true), &frame(usage))),
            &json!([base()])
        );
    }
}
#[test]
fn multiple_llm_metrics_do_not_guess_ownership_but_other_metrics_are_allowed() {
    let mut usage = base();
    usage["reasoning_tokens"] = json!(u64::MAX);
    let mut f = frame(usage.clone());
    if let FrameData::Metrics(m) = &mut f.data {
        m.push(metric());
    }
    assert_eq!(tokens(&wire(&observer(true), &f)), &json!([base(), base()]));
    let mut f = frame(usage.clone());
    if let FrameData::Metrics(m) = &mut f.data {
        m.push(Metric::TtsUsage {
            processor: "tts".into(),
            characters: 4,
        });
    }
    let output = wire(&observer(true), &f);
    assert_eq!(tokens(&output), &json!([usage]));
    assert_eq!(output[0]["data"]["characters"][0]["value"], 4);
}
#[test]
fn metrics_disabled_and_nonfirst_push_still_suppress_projection() {
    let o = RtviObserver::new(
        RtviObserverParams {
            metrics_enabled: false,
            ..Default::default()
        },
        None,
    )
    .unwrap()
    .with_llm_usage_details(true);
    assert!(wire(&o, &frame(base())).is_empty());
    let o = observer(true);
    let f = frame(base());
    let mut observation = Observation::new("llm", Direction::Downstream);
    observation.first_push = false;
    assert!(o.observe(&f, observation).unwrap().is_empty());
    assert_eq!(tokens(&wire(&o, &f)), &json!([base()]));
}
async fn request(socket: &mut tokio::net::TcpStream) -> Result<Value, String> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 2048];
    let max = 32768;
    let end = loop {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed headers".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > max {
            return Err("request budget".into());
        }
        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let length = String::from_utf8_lossy(&bytes[..end])
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|n| n.trim().parse::<usize>())
        })
        .ok_or("missing length")?
        .map_err(|e| e.to_string())?;
    if length > max - end {
        return Err("body budget".into());
    }
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed body".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > max {
            return Err("request budget".into());
        }
    }
    serde_json::from_slice(&bytes[end..end + length]).map_err(|e| e.to_string())
}
async fn pipeline_case(
    provider: Provider,
    enabled: bool,
    usage_enabled: bool,
) -> (Vec<Value>, Vec<Frame>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(provider, "fixture-key");
    config.endpoint = format!("http://{}/completion", listener.local_addr().unwrap());
    let client = LlmClient::new(config).unwrap();
    let request_config = CompletionRequest::new(
        "usage-fixture",
        Context::new(vec![Message::text(Role::User, "hello")]),
    );
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let mut peer = tokio::spawn(async move {
        timeout(WAIT,async {
            let accepted=tokio::select! {v=listener.accept()=>v.map_err(|e|e.to_string())?,_= &mut stop_rx=>return Ok::<_,String>(None)};
            let mut socket=accepted.0;let body=request(&mut socket).await?;
            if body["model"]!="usage-fixture" || body["stream"]!=true{return Err("wrong request".into());}
            let data=if provider==Provider::Anthropic {
                concat!("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3,\"output_tokens\":0,\"cache_read_input_tokens\":4,\"cache_creation_input_tokens\":0}}}\n\n",
                "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":2}}\n\n",
                "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
            } else {concat!("data: {\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":5,\"total_tokens\":12,\"prompt_tokens_details\":{\"cached_tokens\":0},\"completion_tokens_details\":{\"reasoning_tokens\":2}}}\n\n",
                "data: [DONE]\n\n")};
            let response=format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",data.len(),data);
            socket.write_all(response.as_bytes()).await.map_err(|e|e.to_string())?;socket.shutdown().await.map_err(|e|e.to_string())?;Ok(Some(body))
        }).await.map_err(|_|"HTTP peer deadline".to_owned()).and_then(|r|r)
    });
    let o = Arc::new(observer(enabled));
    let created = Pipeline::new(vec![
        Arc::new(LlmProcessor::new(client, request_config)),
        Arc::new(o.tap("usage-wire")),
    ])
    .start(PipelineConfig {
        shutdown_timeout: Duration::from_secs(2),
        start: StartFrame {
            enable_usage_metrics: usage_enabled,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    let mut worker = match created {
        Ok(w) => w,
        Err(e) => {
            let _ = stop_tx.send(());
            peer.abort();
            let _ = peer.await;
            panic!("start: {e}");
        }
    };
    let mut frames = Vec::new();
    let observed = timeout(WAIT, async {
        loop {
            if worker
                .recv()
                .await
                .ok_or("closed before Start")?
                .frame
                .data
                .is_start()
            {
                break;
            }
        }
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .map_err(|e| e.to_string())?;
        loop {
            let f = worker.recv().await.ok_or("closed before end")?.frame;
            if matches!(f.data, FrameData::Error(_) | FrameData::ErrorDetailed(_)) {
                return Err(format!("service error: {:?}", f.data));
            }
            let done = matches!(f.data, FrameData::LlmResponseEnd);
            frames.push(f);
            if done {
                break;
            }
        }
        Ok::<_, String>(())
    })
    .await;
    let normal = matches!(observed, Ok(Ok(())));
    let admission = timeout(WAIT, async {
        if normal {
            worker.end().await
        } else {
            worker.cancel("observation failure").await
        }
    })
    .await;
    if !matches!(admission, Ok(Ok(()))) {
        let _ = timeout(WAIT, worker.cancel("admission deadline")).await;
    }
    let mut timed_out = false;
    let joined = match timeout(WAIT, async {
        while let Some(f) = worker.recv().await {
            frames.push(f.frame);
        }
        worker.join().await
    })
    .await
    {
        Ok(r) => r,
        Err(_) => {
            timed_out = true;
            let _ = timeout(WAIT, worker.cancel("join deadline")).await;
            worker.join().await
        }
    };
    let _ = stop_tx.send(());
    let remote = match timeout(WAIT, &mut peer).await {
        Ok(r) => r,
        Err(e) => {
            peer.abort();
            let _ = peer.await;
            panic!("peer aborted and joined: {e}");
        }
    };
    assert!(normal, "{observed:?}");
    assert!(matches!(admission, Ok(Ok(()))));
    assert!(!timed_out);
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0);
    assert!(remote.unwrap().unwrap().is_some());
    let wires = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::OutputTransportMessageUrgent(v) if v["type"] == "metrics" => Some(v.clone()),
            _ => None,
        })
        .collect();
    (wires, frames)
}
#[tokio::test]
async fn openai_http_sse_typed_usage_reaches_opt_in_wire_once() {
    for enabled in [false, true] {
        let (wire, frames) = pipeline_case(Provider::OpenAi, enabled, true).await;
        let mut expected = base();
        if enabled {
            expected["cache_read_input_tokens"] = json!(0);
            expected["reasoning_tokens"] = json!(2);
        }
        assert_eq!(wire.len(), 1);
        assert_eq!(tokens(&wire), &json!([expected]));
        let metrics = frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::Metrics(_)))
            .collect::<Vec<_>>();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].metadata["llm_usage"]["reasoning_tokens"], 2);
    }
}
#[tokio::test]
async fn anthropic_cumulative_stream_preserves_existing_gross_counts_without_double_count() {
    let (wire, _) = pipeline_case(Provider::Anthropic, true, true).await;
    assert_eq!(wire.len(), 1);
    assert_eq!(
        tokens(&wire),
        &json!([{"prompt_tokens":7,"completion_tokens":2,"total_tokens":9,"cache_read_input_tokens":4,"cache_creation_input_tokens":0}])
    );
}
#[tokio::test]
async fn disabling_source_usage_metrics_emits_no_projected_usage() {
    let (wire, frames) = pipeline_case(Provider::OpenAi, true, false).await;
    assert!(wire.is_empty());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.data, FrameData::Metrics(_))));
}

#[test]
fn pinned_optional_wire_fields_preserve_zero_and_nonadditive_totals() {
    let source: Value =
        serde_json::from_str(include_str!("fixtures/python_usage_details_source.json")).unwrap();
    for id in [
        "optional_absent",
        "optional_explicit_none",
        "all_optional_zero",
        "all_optional_nonzero",
    ] {
        let row = source["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["id"] == id)
            .unwrap();
        let input = &row["input"];
        let mut f = Frame::new(FrameData::Metrics(vec![Metric::LlmUsage {
            processor: "source".into(),
            prompt_tokens: input["prompt_tokens"].as_u64().unwrap(),
            completion_tokens: input["completion_tokens"].as_u64().unwrap(),
            total_tokens: input["total_tokens"].as_u64().unwrap(),
        }]));
        f.metadata.insert("llm_usage".into(), input.clone());
        let mut expected = row["message"]["data"]["tokens"][0].clone();
        // This native carrier deliberately does not implement the source audio buckets.
        for key in [
            "input_audio_tokens",
            "output_audio_tokens",
            "cache_read_input_audio_tokens",
        ] {
            expected.as_object_mut().unwrap().remove(key);
        }
        assert_eq!(
            tokens(&wire(&observer(true), &f)),
            &json!([expected]),
            "{id}"
        );
    }
}

#[test]
fn untouched_constructor_and_absent_metadata_keep_exact_legacy_envelope() {
    let o = RtviObserver::new(Default::default(), None).unwrap();
    let f = Frame::new(FrameData::Metrics(vec![metric()]));
    let output = wire(&o, &f);
    assert_eq!(
        output,
        vec![
            json!({"label":"rtvi-ai","type":"metrics","data":{"tokens":[{"prompt_tokens":7,"completion_tokens":5,"total_tokens":12}]}})
        ]
    );
    assert_eq!(tokens(&wire(&observer(true), &f)), &json!([base()]));
    let mut details = base();
    details["reasoning_tokens"] = json!(2);
    assert_eq!(tokens(&wire(&o, &frame(details))), &json!([base()]));
    assert_eq!(
        serde_json::to_value(metric()).unwrap(),
        json!({"kind":"llm_usage","processor":"llm","prompt_tokens":7,"completion_tokens":5,"total_tokens":12})
    );
}
