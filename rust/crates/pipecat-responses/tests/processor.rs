//! Pipeline ownership, immutable admissions, inference and failure contracts.
use pipecat_core::{self as core, Direction, Frame, FrameData, Pipeline, PipelineConfig, Setting};
use pipecat_responses::*;
use pipecat_services::{InferenceOptions, LlmInference};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
mod common;
use common::*;
async fn send(worker: &core::PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}

#[tokio::test]
async fn pipeline_streams_boundaries_thoughts_usage_and_reasoning_roundtrip() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        let events = vec![
            created("r1"),
            json!({"type":"response.reasoning_summary_text.delta","delta":"think"}),
            json!({"type":"response.output_item.done","item":{"type":"reasoning","id":"rs","summary":[{"type":"summary_text","text":"think"}],"encrypted_content":"opaque"}}),
            delta("hello"),
            completed("r1", "hello"),
        ];
        reply(
            &mut socket,
            "200 OK",
            "text/event-stream",
            &events.into_iter().flat_map(record).collect::<Vec<_>>(),
        )
        .await;
    });
    let processor = Arc::new(ResponsesProcessor::http(cfg, request()).unwrap());
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig {
            start: core::StartFrame {
                enable_metrics: true,
                enable_usage_metrics: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    send(&worker, FrameData::LlmRun).await;
    worker.end().await.unwrap();
    let mut outputs = vec![];
    tokio::time::timeout(Duration::from_secs(4), async {
        while let Some(output) = worker.recv().await {
            outputs.push(output);
        }
    })
    .await
    .unwrap();
    worker.join().await.unwrap();
    assert_eq!(
        outputs
            .iter()
            .filter(|o| matches!(o.frame.data, FrameData::LlmResponseStart))
            .count(),
        1
    );
    assert_eq!(
        outputs
            .iter()
            .filter(|o| matches!(o.frame.data, FrameData::LlmResponseEnd))
            .count(),
        1
    );
    assert!(outputs
        .iter()
        .any(|o| matches!(&o.frame.data,FrameData::LlmText(t) if t.text=="hello")));
    assert!(outputs.iter().any(|o|matches!(&o.frame.data,FrameData::LlmThoughtText(t) if t.text=="think" && t.skip_tts==Some(true) && !t.append_to_context)));
    let reasoning = outputs
        .iter()
        .find_map(|o| {
            if let FrameData::LlmMessagesAppendDetailed(m) = &o.frame.data {
                Some(m)
            } else {
                None
            }
        })
        .unwrap();
    let converted = ResponsesContext::from_core(&core::LlmContextData {
        messages: reasoning.messages.clone(),
        tools: core::ToolSet::NotGiven,
        tool_choice: None,
    })
    .unwrap();
    assert!(
        matches!(&converted.items[0],InputItem::Reasoning(item) if item.encrypted_content.as_deref()==Some("opaque"))
    );
    assert!(outputs.iter().any(|o| o
        .frame
        .metadata
        .get("llm_usage")
        .is_some_and(|u| u["cache_creation_input_tokens"] == 2)));
    assert!(!outputs
        .iter()
        .any(|o| matches!(o.frame.data, FrameData::Error(_))));
    server.await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_admissions_preserve_order_and_effective_settings_snapshots() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let (capture, mut captured) = mpsc::channel(3);
    let (release, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        capture.send(read_http(&mut first).await.1).await.unwrap();
        wait.await.unwrap();
        reply(
            &mut first,
            "200 OK",
            "text/event-stream",
            &[record(delta("first")), record(completed("r1", "first"))].concat(),
        )
        .await;
        for id in ["second", "third"] {
            let (mut socket, _) = listener.accept().await.unwrap();
            capture.send(read_http(&mut socket).await.1).await.unwrap();
            reply(
                &mut socket,
                "200 OK",
                "text/event-stream",
                &[record(delta(id)), record(completed(id, id))].concat(),
            )
            .await;
        }
    });
    let processor = Arc::new(ResponsesProcessor::http(cfg, request()).unwrap());
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::LlmRun).await;
    let first = captured.recv().await.unwrap();
    for model in ["second", "third"] {
        let mut delta = core::LlmSettings::default();
        delta.base.model = Setting::Value(model.into());
        send(
            &worker,
            FrameData::LlmUpdateSettings(Box::new(core::ServiceUpdateSettingsFrame {
                delta: Some(delta),
                ..Default::default()
            })),
        )
        .await;
        send(&worker, FrameData::LlmRun).await;
    }
    send(&worker, FrameData::Text(core::TextFrame::new("fence"))).await;
    while let Some(output) = worker.recv().await {
        if matches!(output.frame.data, FrameData::Text(_)) {
            break;
        }
    }
    release.send(()).unwrap();
    worker.end().await.unwrap();
    while worker.recv().await.is_some() {}
    worker.join().await.unwrap();
    let second = captured.recv().await.unwrap();
    let third = captured.recv().await.unwrap();
    assert_eq!(first["model"], "gpt-4.1");
    assert_eq!(second["model"], "second");
    assert_eq!(third["model"], "third");
    assert_eq!(
        processor
            .request_snapshot()
            .unwrap()
            .effective_model()
            .unwrap(),
        "third"
    );
    server.await.unwrap();
}
#[tokio::test]
async fn invalid_settings_are_atomic_and_do_not_poison_usable_service() {
    let processor = Arc::new(ResponsesProcessor::http(config(), request()).unwrap());
    let original = processor.request_snapshot().unwrap();
    let mut bad = ResponsesSettings::default();
    bad.llm.temperature = Setting::Value(9.0);
    bad.llm.base.model = Setting::Value("other".into());
    assert!(processor.update_settings(&bad).is_err());
    assert_eq!(processor.request_snapshot().unwrap(), original);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(
        &worker,
        FrameData::LlmUpdateSettings(Box::new(core::ServiceUpdateSettingsFrame {
            delta: Some(bad.llm),
            ..Default::default()
        })),
    )
    .await;
    worker.end().await.unwrap();
    let mut errors = 0;
    while let Some(out) = worker.recv().await {
        if matches!(out.frame.data, FrameData::Error(_)) {
            errors += 1;
        }
    }
    worker.join().await.unwrap();
    assert!(errors > 0);
    assert!(core::FrameProcessor::health(processor.as_ref())
        .unwrap()
        .is_usable());
    assert_eq!(processor.request_snapshot().unwrap(), original);
}
#[tokio::test]
async fn websocket_processor_out_of_band_inference_uses_http_supplied_context() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let (_, body) = read_http(&mut socket).await;
        assert_eq!(body["stream"], false);
        assert_eq!(body["input"][0]["content"], "one shot");
        assert_eq!(body["max_output_tokens"], 7);
        reply(&mut socket,"200 OK","application/json",json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"answer"}]}]}).to_string().as_bytes()).await;
    });
    let processor = ResponsesProcessor::websocket(cfg, request()).unwrap();
    let context = core::LlmContextData {
        messages: vec![core::ChatMessage::new(core::ChatRole::User, "one shot").into()],
        tools: core::ToolSet::NotGiven,
        tool_choice: None,
    };
    let mut options = InferenceOptions::default();
    options.settings.max_tokens = Setting::Value(7);
    assert_eq!(
        LlmInference::run_inference(&processor, context, options)
            .await
            .unwrap()
            .as_deref(),
        Some("answer")
    );
    assert_eq!(processor.request_snapshot().unwrap(), request());
    server.await.unwrap();
}
#[tokio::test]
async fn pipeline_interruption_cancels_http_work_without_stale_output() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/", listener.local_addr().unwrap());
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        use tokio::io::AsyncReadExt;
        let (mut socket, _) = listener.accept().await.unwrap();
        read_http(&mut socket).await;
        tx.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let mut worker = Pipeline::new(vec![Arc::new(
        ResponsesProcessor::http(cfg, request()).unwrap(),
    )])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, FrameData::LlmRun).await;
    rx.await.unwrap();
    send(&worker, FrameData::Interruption).await;
    worker.end().await.unwrap();
    while let Some(out) = worker.recv().await {
        assert!(!matches!(out.frame.data, FrameData::LlmText(_)));
    }
    worker.join().await.unwrap();
    server.await.unwrap();
}
