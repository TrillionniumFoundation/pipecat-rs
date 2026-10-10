//! Synthetic backend receipts through the actual native output transport.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_grok_realtime::{
    GrokRealtimeProcessor, PlaybackPosition, RealtimeClient, RealtimeConfig,
};
use pipecat_media_transport::{
    OutputBackend, OutputTransport, OutputTransportConfig, PlaybackReceipt,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, time::timeout};
use tokio_tungstenite::tungstenite::Message;
const WAIT: Duration = Duration::from_secs(5);
struct ReceiptBackend {
    processor: Arc<GrokRealtimeProcessor>,
    acknowledge: bool,
    calls: AtomicUsize,
    changed: Notify,
}
#[async_trait]
impl OutputBackend for ReceiptBackend {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let audio = match &frame.data {
            FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) => audio,
            other => panic!("unexpected backend frame {other:?}"),
        };
        assert_eq!(audio.sample_rate, 24000);
        assert_eq!(audio.num_channels, 1);
        assert_eq!(audio.audio, [1_u8, 0].repeat(240));
        let count = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.acknowledge && count == 0 {
            let item_id = frame
                .metadata
                .get("item_id")
                .and_then(Value::as_str)
                .expect("output lost provider item identity");
            let content_index = frame
                .metadata
                .get("content_index")
                .and_then(Value::as_u64)
                .unwrap();
            self.processor.acknowledge_playback(PlaybackPosition {
                item_id: item_id.into(),
                content_index: content_index as u32,
                audio_end_ms: 10,
            })?;
            self.changed.notify_one();
            return Ok(PlaybackReceipt::Played);
        }
        self.changed.notify_one();
        Ok(PlaybackReceipt::Discarded)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
async fn receipt_case(acknowledge: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("review-only-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    config.limits.connect_timeout = WAIT;
    config.limits.operation_timeout = WAIT;
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        for event in [
            json!({"type":"session.created","session":{"id":"s"}}),
            json!({"type":"conversation.created","conversation":{"id":"c"}}),
        ] {
            let mut event = event;
            event["event_id"] = json!("receipt");
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        let update = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(update.to_text().unwrap()).unwrap()["type"],
            "session.update"
        );
        ws.send(Message::Text(
            json!({"event_id":"ready","type":"session.updated","session":{"id":"s"}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        let request = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(request.to_text().unwrap()).unwrap()["type"],
            "response.create"
        );
        for event in [
            json!({"event_id":"created","type":"response.created","response":{"id":"r","status":"in_progress","output":[]}}),
            json!({"event_id":"audio","type":"response.output_audio.delta","response_id":"r","item_id":"spoken-item","output_index":0,"content_index":1,"delta":STANDARD.encode([1_u8,0].repeat(480))}),
        ] {
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        let cancel = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(cancel.to_text().unwrap()).unwrap()["type"],
            "response.cancel"
        );
        if acknowledge {
            let truncate = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
            let truncate: Value = serde_json::from_str(truncate.to_text().unwrap()).unwrap();
            assert_eq!(truncate["type"], "conversation.item.truncate");
            assert_eq!(truncate["item_id"], "spoken-item");
            assert_eq!(truncate["content_index"], 1);
            assert_eq!(truncate["audio_end_ms"], 10);
        }
        assert!(
            timeout(Duration::from_millis(80), ws.next()).await.is_err(),
            "fabricated truncation or duplicate cancellation"
        );
        ws.send(Message::Text(json!({"event_id":"done","type":"response.done","response":{"id":"r","status":"cancelled","output":[]}}).to_string().into())).await.unwrap();
        ws.close(None).await.unwrap();
    });
    let backend = Arc::new(ReceiptBackend {
        processor: processor.clone(),
        acknowledge,
        calls: AtomicUsize::new(0),
        changed: Notify::new(),
    });
    let output = OutputTransport::new(
        "review-output",
        OutputTransportConfig {
            sample_rate: Some(24000),
            chunk_duration: Duration::from_millis(10),
            auto_silence: false,
            end_silence: Duration::ZERO,
            write_timeout: WAIT,
            ..Default::default()
        },
        backend.clone(),
        None,
        BTreeMap::new(),
    )
    .unwrap();
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = 24000;
    pipeline_config.start.audio_out_sample_rate = 24000;
    let worker = Pipeline::new(vec![processor, Arc::new(output)])
        .start(pipeline_config)
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    timeout(WAIT, backend.changed.notified()).await.unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::Interruption), Direction::Downstream)
        .await
        .unwrap();
    timeout(WAIT, server).await.unwrap().unwrap();
    worker.end().await.unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(backend.calls.load(Ordering::SeqCst) > 0);
}
#[tokio::test]
async fn review_real_output_receipt_truncates_exact_item_content_and_played_position() {
    receipt_case(true).await;
}
#[tokio::test]
async fn review_discarded_output_never_invents_a_playback_position() {
    receipt_case(false).await;
}
