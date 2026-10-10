//! Real Cartesia loopback events cross the timed output boundary and RTVI wire.
//! Backend acknowledgements are synthetic; no live provider or device is used.
use futures_util::{SinkExt, StreamExt};
use pipecat::{audio::PcmFormat, core::*, media_transport::*, rtvi::*, streaming_speech::*, wire};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    time::Instant,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

struct Clock(Instant);
impl ObserverClock for Clock {
    fn now_ns(&self) -> u64 {
        9_000_000_000 + self.0.elapsed().as_nanos() as u64
    }
}
struct Backend {
    entered: Semaphore,
    frames: Mutex<Vec<Frame>>,
}
#[async_trait]
impl OutputBackend for Backend {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.frames.lock().unwrap().push(frame.clone());
        self.entered.add_permits(1);
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
struct Probe {
    after_output: bool,
    frames: Mutex<Vec<Frame>>,
    observer: RtviObserver,
    wire_events: Mutex<Vec<Value>>,
}
impl Probe {
    fn new(after_output: bool) -> Self {
        Self {
            after_output,
            frames: Mutex::new(vec![]),
            observer: RtviObserver::new(RtviObserverParams::default(), None).unwrap(),
            wire_events: Mutex::new(vec![]),
        }
    }
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        if self.after_output {
            "after-output"
        } else {
            "before-output"
        }
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.frames.lock().unwrap().push(frame.clone());
            if frame.has_pipeline_pts() {
                assert!(!self.after_output, "output leaked pipeline-clock timing");
                assert!(serde_json::to_value(&frame).is_err());
                assert!(wire::encode(&frame).is_err());
            } else if self.after_output {
                serde_json::to_value(&frame).unwrap();
            }
            for event in self.observer.observe(
                &frame,
                Observation {
                    source: self.name(),
                    direction,
                    first_push: true,
                    after_output_transport: self.after_output,
                },
            )? {
                let event = serde_json::to_value(event).unwrap();
                let outgoing = Frame::new(FrameData::OutputTransportMessage(event.clone()));
                let encoded = wire::encode(&outgoing).unwrap().unwrap();
                let decoded = wire::decode(&encoded).unwrap();
                assert_eq!(
                    decoded.data,
                    FrameData::InputTransportMessage(event.clone())
                );
                self.wire_events.lock().unwrap().push(event);
            }
        }
        ctx.push(frame, direction).await
    }
}
type Socket = WebSocketStream<TcpStream>;
async fn read(socket: &mut Socket) -> Value {
    let next = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(next.to_text().unwrap()).unwrap()
}
async fn write(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
struct Completed {
    before: Arc<Probe>,
    after: Arc<Probe>,
    backend: Arc<Backend>,
}
async fn loopback(timestamps: bool) -> Completed {
    loopback_routed(timestamps, None).await
}
async fn loopback_routed(timestamps: bool, destination: Option<&str>) -> Completed {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cartesia = CartesiaConfig::new("loopback", "voice");
    cartesia.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    cartesia.sample_rate = 8000;
    cartesia.add_timestamps = timestamps;
    let backend = Arc::new(Backend {
        entered: Semaphore::new(0),
        frames: Mutex::new(vec![]),
    });
    let before = Arc::new(Probe::new(false));
    let after = Arc::new(Probe::new(true));
    let output = Arc::new(
        OutputTransport::new(
            "output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                auto_silence: false,
                end_silence: Duration::ZERO,
                destinations: destination.into_iter().map(str::to_owned).collect(),
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_out_sample_rate = 8000;
    let worker = Pipeline::new(vec![
        Arc::new(CartesiaTtsProcessor::new(
            CartesiaClient::new(cartesia).unwrap(),
        )),
        before.clone(),
        output,
        after.clone(),
    ])
    .with_observer_clock(Arc::new(Clock(Instant::now())))
    .start(pipeline_config)
    .await
    .unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request["transcript"], "Hello world");
        assert_eq!(read(&mut socket).await["continue"], false);
        if timestamps {
            write(&mut socket, json!({"type":"timestamps", "context_id":request["context_id"], "word_timestamps":{"words":["Hello", "world"], "start":[0.0, 0.35]}})).await;
        }
        // A full native output chunk. Both timestamps deliberately precede PCM.
        write(&mut socket, json!({"type":"chunk", "context_id":request["context_id"], "data":"6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6AM="})).await;
        sent.send(()).unwrap();
        write(
            &mut socket,
            json!({"type":"done", "context_id":request["context_id"]}),
        )
        .await;
        while let Some(message) = socket.next().await {
            if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                break;
            }
        }
    });
    send(&worker, Frame::new(FrameData::LlmResponseStart)).await;
    let mut input = TtsTextInputFrame::new("Hello world");
    input.includes_inter_frame_spaces = true;
    let mut input = Frame::new(FrameData::TtsTextInput(input));
    input.transport_destination = destination.map(str::to_owned);
    send(&worker, input).await;
    send(&worker, Frame::new(FrameData::LlmResponseEnd)).await;
    received.await.unwrap();
    // A future word must not stall the PCM queued behind it.
    tokio::time::timeout(Duration::from_millis(200), backend.entered.acquire())
        .await
        .expect("future word blocked following PCM")
        .unwrap()
        .forget();
    worker.sender().end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
    Completed {
        before,
        after,
        backend,
    }
}
#[tokio::test]
async fn future_words_allow_pcm_and_cross_output_with_rebased_timing() {
    let completed = loopback(true).await;
    let before = completed.before.frames.lock().unwrap();
    let after = completed.after.frames.lock().unwrap();
    let original: Vec<_> = before
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsText(_)))
        .collect();
    let rebased: Vec<_> = after
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsText(_)))
        .collect();
    assert_eq!(original.len(), 2);
    assert_eq!(rebased.len(), 2);
    assert!(original
        .iter()
        .all(|f| f.has_pipeline_pts() && f.pts.unwrap() >= 9_000_000_000));
    assert!(rebased
        .iter()
        .all(|f| !f.has_pipeline_pts() && f.pts.unwrap() < 1_000_000_000));
    assert_eq!(
        original.iter().map(|f| f.id).collect::<Vec<_>>(),
        rebased.iter().map(|f| f.id).collect::<Vec<_>>()
    );
    assert_eq!(
        rebased[1].pts.unwrap() - rebased[0].pts.unwrap(),
        350_000_000
    );
    assert!(!completed.backend.frames.lock().unwrap().is_empty());
    let end = after
        .iter()
        .position(|f| matches!(f.data, FrameData::LlmResponseEnd))
        .unwrap();
    let last = after
        .iter()
        .rposition(|f| matches!(f.data, FrameData::AggregatedTextProgress(_)))
        .unwrap();
    assert!(last < end);
}
#[tokio::test]
async fn rtvi_wire_announces_anchor_before_matching_progress() {
    let completed = loopback(true).await;
    let after = completed.after.frames.lock().unwrap();
    let anchor = after
        .iter()
        .find(|f| matches!(f.data, FrameData::AggregatedText(_)))
        .unwrap();
    let wire = completed.after.wire_events.lock().unwrap();
    let statuses: Vec<_> = wire
        .iter()
        .filter(|event| {
            event["type"] == "bot-output" && event["data"]["segment_id"] == json!(anchor.id)
        })
        .map(|event| event["data"]["spoken_status"].clone())
        .collect();
    assert_eq!(
        statuses,
        [json!("new"), json!("in-progress"), json!("completed")],
        "{wire:?}"
    );
}
#[tokio::test]
async fn no_timestamp_success_stays_diagnostic_through_rtvi_wire() {
    let completed = loopback(false).await;
    let after = completed.after.frames.lock().unwrap();
    assert!(!after.iter().any(|f| matches!(
        f.data,
        FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_)
    )));
    assert!(after.iter().any(|f| matches!(&f.data, FrameData::TtsAlignmentDiagnostic(d) if d.evidence == TtsAlignmentEvidence::Unreported && d.synthesis_outcome == TtsSynthesisOutcome::Succeeded)));
    let wire = completed.after.wire_events.lock().unwrap();
    assert!(!wire
        .iter()
        .any(|event| event["type"] == "bot-tts-text"
            || event["data"]["spoken_status"] == "completed"));
}

#[tokio::test]
async fn native_prepared_input_preserves_destination_and_spacing() {
    let completed = loopback_routed(true, Some("secondary")).await;
    for probe in [&completed.before, &completed.after] {
        let frames = probe.frames.lock().unwrap();
        let synthesis: Vec<_> = frames
            .iter()
            .filter(|frame| {
                matches!(
                    frame.data,
                    FrameData::TtsStarted(_)
                        | FrameData::TtsStopped(_)
                        | FrameData::TtsAudio(_)
                        | FrameData::AggregatedText(_)
                        | FrameData::TtsText(_)
                        | FrameData::AggregatedTextProgress(_)
                )
            })
            .collect();
        assert!(synthesis.len() >= 8);
        assert!(
            synthesis
                .iter()
                .all(|frame| frame.transport_destination.as_deref() == Some("secondary")),
            "{synthesis:?}"
        );
        assert!(synthesis
            .iter()
            .filter_map(|frame| match &frame.data {
                FrameData::TtsText(text) => Some(text),
                _ => None,
            })
            .all(|text| text.text.includes_inter_frame_spaces));
    }
    assert!(completed
        .backend
        .frames
        .lock()
        .unwrap()
        .iter()
        .all(|frame| frame.transport_destination.as_deref() == Some("secondary")));
}
