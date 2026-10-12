//! A public network-bus decoder produces the assistant image sent to real shared output.
use pipecat::{
    audio::PcmFormat,
    core::*,
    media_transport::{OutputBackend, OutputTransport, OutputTransportConfig, PlaybackReceipt},
    network_bus::PythonJsonCodec,
    workers::MessageData,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(5);
type TestResult<T> = Result<T, String>;
async fn bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label}: deadline"))?
        .map_err(|error| format!("{label}: {error}"))
}
#[derive(Default)]
struct Backend {
    images: Mutex<Vec<Frame>>,
    started: AtomicUsize,
    stopped: AtomicUsize,
    audio: AtomicUsize,
}
#[async_trait]
impl OutputBackend for Backend {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.audio.fetch_add(1, Ordering::SeqCst);
        Ok(PlaybackReceipt::Played)
    }
    async fn write_image(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.images.lock().unwrap().push(frame.clone());
        Ok(())
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn decoded_assistant() -> (Frame, Direction) {
    let wire = json!({
        "__type__": "pipecat.bus.messages.BusFrameMessage",
        "__data__": {
            "source": "image-worker",
            "target": "display-worker",
            "frame": {
                "__type__": "pipecat.frames.frames.AssistantImageRawFrame",
                "__data__": {
                    "id": 87201,
                    "name": "AssistantImageRawFrame#wire",
                    "pts": 0,
                    "broadcast_sibling_id": 87202,
                    "metadata": {"image-source": ["generated", null]},
                    "transport_source": "native-bus",
                    "transport_destination": "display",
                    "interruptible": false,
                    "image": {"__type__": "bytes", "__data__": "AQID"},
                    "size": [1, 1],
                    "format": "RGB",
                    "sync_with_audio": true,
                    "original_data": {"__type__": "bytes", "__data__": "iVBORw=="},
                    "original_mime_type": "image/png"
                }
            },
            "direction": {
                "__type__": "pipecat.processors.frame_processor.FrameDirection",
                "__data__": "DOWNSTREAM"
            },
            "bridge": "media"
        }
    });
    let envelope = PythonJsonCodec::default()
        .decode(&serde_json::to_vec(&wire).unwrap())
        .unwrap();
    let MessageData::Frame {
        frame, direction, ..
    } = &envelope.as_bus_message().unwrap().data
    else {
        panic!("bus did not decode a typed frame")
    };
    assert!(matches!(frame.data, FrameData::AssistantImage(_)));
    ((**frame).clone(), *direction)
}
async fn finish(mut worker: PipelineWorker) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
    let end = bounded("end", worker.end()).await;
    if end.is_err() {
        let _ = bounded("fallback cancel", worker.cancel("end failed")).await;
    }
    let joined = bounded("join", worker.join()).await;
    if joined.is_err() {
        let _ = bounded("fallback cancel", worker.cancel("join failed")).await;
        let _ = bounded("fallback join", worker.join()).await;
    }
    let report = joined?;
    end?;
    let mut frames = vec![];
    while let Ok(frame) = worker.try_recv() {
        frames.push(frame);
    }
    Ok((frames, report))
}

#[tokio::test]
async fn decoded_assistant_image_reaches_shared_output_through_facade() {
    let (expected, direction) = decoded_assistant();
    let backend = Arc::new(Backend::default());
    let output = Arc::new(
        OutputTransport::new(
            "wire-assistant-output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                destinations: vec!["display".into()],
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: Duration::from_secs(1),
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let control = output.control();
    let worker = bounded(
        "start",
        Pipeline::new(vec![output]).start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let operations = async {
        timeout(WAIT, async {
            while !control.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "output ready: deadline".to_string())?;
        bounded("send", worker.sender().send(expected.clone(), direction)).await
    }
    .await;
    let finished = finish(worker).await;
    assert_eq!(backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.audio.load(Ordering::SeqCst), 0);
    operations.unwrap();
    let (frames, report) = finished.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.state, WorkerState::Ended);
    let images = backend.images.lock().unwrap();
    assert_eq!(
        images.len(),
        1,
        "decoded assistant image was silently skipped"
    );
    assert_eq!(images[0], expected);
    assert_eq!(images[0].observation_token(), expected.observation_token());
    let returned: Vec<_> = frames
        .iter()
        .filter(|item| item.direction == direction && item.frame.id == expected.id)
        .collect();
    assert_eq!(returned.len(), 1);
    assert_eq!(returned[0].frame, expected);
    assert_eq!(
        returned[0].frame.observation_token(),
        expected.observation_token()
    );
}
