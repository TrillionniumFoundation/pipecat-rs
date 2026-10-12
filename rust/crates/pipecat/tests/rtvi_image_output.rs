//! Public codec/tap/shared-output composition with an application image backend.
use pipecat::{
    audio::PcmFormat,
    core::*,
    media_transport::{
        OutputAdmissionPolicy, OutputBackend, OutputTransport, OutputTransportConfig,
        PlaybackReceipt,
    },
    network_bus::PythonJsonCodec,
    rtvi::{RtviObserver, RtviObserverParams},
    workers::MessageData,
};
use serde_json::{json, Value};
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

const MESSAGE_LIMIT: usize = 1024;
const WAIT: Duration = Duration::from_secs(10);
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
    messages: Mutex<Vec<Frame>>,
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
    async fn send_message(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.messages.lock().unwrap().push(frame.clone());
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
#[derive(Default)]
struct Recorder {
    frames: Mutex<Vec<DirectedFrame>>,
}
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "after-image-tap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}
fn wire_image(class: &str, id: u64, large_pixels: bool, large_original: bool) -> Value {
    let pixels = if large_pixels { 768 } else { 3 };
    let original = if large_original { 768 } else { 3 };
    let mut data = json!({
        "id": id,
        "name": format!("{class}#wire-{id}"),
        "pts": 0,
        "metadata": {"image-source": ["generated", null]},
        "transport_source": "native-bus",
        "transport_destination": "display",
        "interruptible": false,
        "image": {"__type__": "bytes", "__data__": "AAAA".repeat(pixels / 3)},
        "size": if large_pixels { [16, 16] } else { [1, 1] },
        "format": "RGB",
        "sync_with_audio": false
    });
    match class {
        "AssistantImageRawFrame" => {
            data["original_data"] =
                json!({"__type__": "bytes", "__data__": "AAAA".repeat(original / 3)});
            data["original_mime_type"] = json!("image/png");
        }
        "URLImageRawFrame" => data["url"] = json!("https://example.invalid/opaque.png"),
        "UserImageRawFrame" => {
            data["user_id"] = json!("alice");
            data["text"] = json!("image context");
            data["append_to_context"] = json!(true);
            data["run_llm"] = json!(false);
            data["request"] = Value::Null;
        }
        _ => {}
    }
    json!({
        "__type__": "pipecat.bus.messages.BusFrameMessage",
        "__data__": {
            "source": "image-worker",
            "target": "display-worker",
            "frame": {"__type__": format!("pipecat.frames.frames.{class}"), "__data__": data},
            "direction": {
                "__type__": "pipecat.processors.frame_processor.FrameDirection",
                "__data__": "DOWNSTREAM"
            },
            "bridge": "media"
        }
    })
}
fn decode(wire: &Value) -> Frame {
    let bytes = serde_json::to_vec(wire).unwrap();
    assert!(bytes.len() < 8 * 1024 * 1024);
    let envelope = PythonJsonCodec::default().decode(&bytes).unwrap();
    let MessageData::Frame {
        frame, direction, ..
    } = &envelope.as_bus_message().unwrap().data
    else {
        panic!("bus did not decode a typed frame")
    };
    assert_eq!(*direction, Direction::Downstream);
    eprintln!(
        "{}: wire bytes={}, FrameData bytes={}",
        frame.name,
        bytes.len(),
        serde_json::to_vec(&frame.data).unwrap().len()
    );
    (**frame).clone()
}
fn native_output(large: bool) -> Frame {
    let mut frame = Frame::new(FrameData::OutputImage(ImageRawFrame {
        image: vec![0; if large { 768 } else { 3 }],
        size: if large { (16, 16) } else { (1, 1) },
        format: Some("RGB".into()),
    }));
    frame.name = format!("native-output-large-{large}");
    frame.transport_destination = Some("display".into());
    frame.interruptible = false;
    frame
}
struct Completed {
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
    backend: Arc<Backend>,
    after_tap: Arc<Recorder>,
}
async fn finish(worker: PipelineWorker) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
    let sender = worker.sender();
    let terminal = bounded("End", sender.end()).await;
    if terminal.is_err() {
        let _ = bounded("Cancel failed End", sender.cancel("image End failed")).await;
    }
    let mut collection = tokio::spawn(worker.collect());
    let collected = match timeout(WAIT, &mut collection).await {
        Ok(result) => result
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string()),
        Err(_) => {
            let _ = bounded(
                "Cancel collection",
                sender.cancel("image collection deadline"),
            )
            .await;
            match timeout(WAIT, &mut collection).await {
                Ok(result) => Err(format!(
                    "collection deadline; cancelled and joined: {result:?}"
                )),
                Err(_) => {
                    collection.abort();
                    let _ = collection.await;
                    Err("collection deadline after cancellation; aborted and joined".into())
                }
            }
        }
    };
    terminal.and(collected)
}
async fn route(expected: &[Frame], queue_bytes: usize) -> TestResult<Completed> {
    let backend = Arc::new(Backend::default());
    let output = Arc::new(
        OutputTransport::new(
            "image-output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                destinations: vec!["display".into()],
                queue_capacity: 64,
                queue_byte_capacity: queue_bytes,
                admission_policy: OutputAdmissionPolicy::FailFast,
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: Duration::from_secs(1),
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .map_err(|error| error.to_string())?,
    );
    let control = output.control();
    let observer = Arc::new(
        RtviObserver::new(
            RtviObserverParams {
                max_message_bytes: MESSAGE_LIMIT,
                ..Default::default()
            },
            None,
        )
        .map_err(|error| error.to_string())?,
    );
    let after_tap = Arc::new(Recorder::default());
    let mut worker = bounded(
        "start",
        Pipeline::new(vec![
            Arc::new(observer.tap("image-tap")),
            after_tap.clone(),
            output,
        ])
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        }),
    )
    .await?;
    let system_marker = Frame::new(FrameData::System);
    let mut normal_marker = Frame::new(FrameData::Data);
    normal_marker.transport_destination = Some("display".into());
    let mut observed = Vec::new();
    let operations: TestResult<()> = async {
        timeout(WAIT, async {
            while !control.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .map_err(|_| "output ready: deadline".to_owned())?;
        for frame in expected {
            bounded(
                "send image",
                worker.sender().send(frame.clone(), Direction::Downstream),
            )
            .await?;
        }
        for marker in [&system_marker, &normal_marker] {
            bounded(
                "send marker",
                worker.sender().send(marker.clone(), Direction::Downstream),
            )
            .await?;
        }
        timeout(WAIT, async {
            let mut system_seen = false;
            let mut normal_seen = false;
            while !(system_seen && normal_seen) {
                let item = worker.recv().await.ok_or("closed before lane markers")?;
                system_seen |= item.frame.id == system_marker.id;
                normal_seen |= item.frame.id == normal_marker.id;
                observed.push(item);
            }
            Ok::<(), String>(())
        })
        .await
        .map_err(|_| "lane markers: deadline".to_owned())?
    }
    .await;
    let finished = finish(worker).await;
    let (remaining, report) = finished?;
    observed.extend(remaining);
    operations?;
    Ok(Completed {
        frames: observed,
        report,
        backend,
        after_tap,
    })
}
fn exact_originals(actual: &[DirectedFrame], expected: &[Frame], tag: &LocalFrameTag) {
    for expected in expected {
        let matching: Vec<_> = actual
            .iter()
            .filter(|item| item.frame.id == expected.id)
            .collect();
        assert_eq!(matching.len(), 1, "{}", expected.name);
        assert_eq!(matching[0].direction, Direction::Downstream);
        assert_eq!(matching[0].frame, *expected);
        assert_eq!(
            matching[0].frame.observation_token(),
            expected.observation_token()
        );
        assert_eq!(
            matching[0].frame.has_pipeline_pts(),
            expected.has_pipeline_pts()
        );
        assert!(tag.contains(&matching[0].frame));
    }
}
fn assert_completed(case: &Completed, expected: &[Frame], writes: bool, tag: &LocalFrameTag) {
    let images = case.backend.images.lock().unwrap();
    let after_tap = case.after_tap.frames.lock().unwrap();
    for frame in expected {
        eprintln!(
            "{}: after-tap={}, backend={}, sink={}",
            frame.name,
            after_tap
                .iter()
                .filter(|item| item.frame.id == frame.id)
                .count(),
            images.iter().filter(|item| item.id == frame.id).count(),
            case.frames
                .iter()
                .filter(|item| item.frame.id == frame.id)
                .count()
        );
    }
    eprintln!("route errors={:?}", case.report.errors);
    assert_eq!(case.backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(case.backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(case.backend.audio.load(Ordering::SeqCst), 0);
    assert_eq!(case.report.state, WorkerState::Ended);
    assert!(case.report.errors.is_empty(), "{:?}", case.report.errors);
    assert!(case.backend.messages.lock().unwrap().is_empty());
    exact_originals(&after_tap, expected, tag);
    exact_originals(&case.frames, expected, tag);
    assert_eq!(images.len(), if writes { expected.len() } else { 0 });
    if writes {
        for (actual, expected) in images.iter().zip(expected) {
            assert_eq!(actual, expected);
            assert_eq!(actual.observation_token(), expected.observation_token());
            assert!(tag.contains(actual));
        }
    }
}

#[tokio::test]
async fn decoded_large_output_images_reach_the_application_backend_through_rtvi() {
    let mut expected = vec![native_output(false), native_output(true)];
    for (index, class) in [
        "OutputImageRawFrame",
        "URLImageRawFrame",
        "AssistantImageRawFrame",
    ]
    .into_iter()
    .enumerate()
    {
        let small = decode(&wire_image(class, 900_000 + 2 * index as u64, false, false));
        let large = decode(&wire_image(class, 900_001 + 2 * index as u64, true, false));
        assert!(serde_json::to_vec(&small.data).unwrap().len() < MESSAGE_LIMIT);
        assert!(serde_json::to_vec(&large.data).unwrap().len() > MESSAGE_LIMIT);
        expected.extend([small, large]);
    }
    let original_only = decode(&wire_image("AssistantImageRawFrame", 900_010, false, true));
    let FrameData::AssistantImage(assistant) = &original_only.data else {
        panic!("wrong assistant variant")
    };
    assert_eq!(assistant.image.image.image.len(), 3);
    assert_eq!(assistant.original_data.as_ref().unwrap().len(), 768);
    assert!(serde_json::to_vec(&original_only.data).unwrap().len() > MESSAGE_LIMIT);
    expected.push(original_only);
    let tag = LocalFrameTag::new();
    for frame in &mut expected {
        tag.mark(frame).unwrap();
    }
    let completed = route(&expected, 64 * 1024).await.unwrap();
    assert_completed(&completed, &expected, true, &tag);
}

#[tokio::test]
async fn decoded_input_images_pass_shared_output_without_image_backend_calls() {
    let mut expected = Vec::new();
    for (index, class) in ["InputImageRawFrame", "UserImageRawFrame"]
        .into_iter()
        .enumerate()
    {
        for large in [false, true] {
            let frame = decode(&wire_image(
                class,
                910_000 + 2 * index as u64 + u64::from(large),
                large,
                false,
            ));
            assert!(frame.is_system());
            assert_eq!(
                serde_json::to_vec(&frame.data).unwrap().len() > MESSAGE_LIMIT,
                large
            );
            expected.push(frame);
        }
    }
    let tag = LocalFrameTag::new();
    for frame in &mut expected {
        tag.mark(frame).unwrap();
    }
    let completed = route(&expected, 64 * 1024).await.unwrap();
    assert_completed(&completed, &expected, false, &tag);
}

#[test]
fn repeat_wire_ids_keep_numeric_fifo_identity_despite_fresh_local_tokens() {
    for large in [false, true] {
        for distinct_id in [false, true] {
            let observer = RtviObserver::new(
                RtviObserverParams {
                    max_message_bytes: MESSAGE_LIMIT,
                    dedup_capacity: 2,
                    ..Default::default()
                },
                None,
            )
            .unwrap();
            let observation = pipecat::rtvi::Observation::new("wire", Direction::Downstream);
            let original = decode(&wire_image("AssistantImageRawFrame", 920_000, large, false));
            let repeated = decode(&wire_image(
                "AssistantImageRawFrame",
                920_000 + u64::from(distinct_id),
                large,
                false,
            ));
            assert_ne!(original.observation_token(), repeated.observation_token());
            assert_eq!(original.id == repeated.id, !distinct_id);
            let first = Frame::new(FrameData::UserStartedSpeaking);
            let second = Frame::new(FrameData::UserStoppedSpeaking);
            assert!(observer.observe(&original, observation).unwrap().is_empty());
            assert_eq!(observer.observe(&first, observation).unwrap().len(), 1);
            assert!(observer.observe(&repeated, observation).unwrap().is_empty());
            assert_eq!(observer.observe(&second, observation).unwrap().len(), 1);
            assert_eq!(
                observer.observe(&first, observation).unwrap().len(),
                usize::from(distinct_id)
            );
        }
    }
}

#[tokio::test]
async fn seen_image_ids_still_deliver_every_backend_traversal() {
    let wire = wire_image("AssistantImageRawFrame", 930_000, true, true);
    let original = decode(&wire);
    let redecode = decode(&wire);
    assert_eq!(original.id, redecode.id);
    assert_ne!(original.observation_token(), redecode.observation_token());
    let duplicate = original.duplicate();
    assert_ne!(original.id, duplicate.id);
    let expected = vec![original.clone(), original, redecode, duplicate];
    let completed = route(&expected, 64 * 1024).await.unwrap();
    assert_eq!(completed.report.state, WorkerState::Ended);
    assert!(
        completed.report.errors.is_empty(),
        "{:?}",
        completed.report.errors
    );
    assert_eq!(completed.backend.started.load(Ordering::SeqCst), 1);
    assert_eq!(completed.backend.stopped.load(Ordering::SeqCst), 1);
    assert!(completed.backend.messages.lock().unwrap().is_empty());
    let images = completed.backend.images.lock().unwrap();
    assert_eq!(*images, expected);
    let returned: Vec<_> = completed
        .frames
        .iter()
        .filter(|item| expected.iter().any(|frame| frame.id == item.frame.id))
        .collect();
    assert_eq!(returned.len(), expected.len());
    for ((actual, returned), expected) in images.iter().zip(returned).zip(&expected) {
        assert_eq!(actual.observation_token(), expected.observation_token());
        assert_eq!(returned.direction, Direction::Downstream);
        assert_eq!(returned.frame, *expected);
        assert_eq!(
            returned.frame.observation_token(),
            expected.observation_token()
        );
    }
}

#[tokio::test]
async fn shared_output_retains_its_separate_image_allocation_budget() {
    for original_bytes in [false, true] {
        let small = decode(&wire_image("AssistantImageRawFrame", 940_000, false, false));
        let mut wire = wire_image("AssistantImageRawFrame", 940_001, false, false);
        let data = &mut wire["__data__"]["frame"]["__data__"];
        let large_bytes = json!({"__type__": "bytes", "__data__": "AAAA".repeat(4096)});
        if original_bytes {
            data["original_data"] = large_bytes;
        } else {
            data["image"] = large_bytes;
            data["size"] = json!([64, 64]);
        }
        let rejected = decode(&wire);
        let FrameData::AssistantImage(assistant) = &rejected.data else {
            panic!("wrong image variant")
        };
        let retained = if original_bytes {
            assistant.original_data.as_ref().unwrap().len()
        } else {
            assistant.image.image.image.len()
        };
        assert_eq!(retained, 12_288);
        let tag = LocalFrameTag::new();
        let mut expected = vec![small, rejected];
        for frame in &mut expected {
            tag.mark(frame).unwrap();
        }
        let completed = route(&expected, 4096).await.unwrap();
        assert_eq!(completed.report.state, WorkerState::Ended);
        assert_eq!(completed.backend.started.load(Ordering::SeqCst), 1);
        assert_eq!(completed.backend.stopped.load(Ordering::SeqCst), 1);
        assert_eq!(completed.backend.audio.load(Ordering::SeqCst), 0);
        let after_tap = completed.after_tap.frames.lock().unwrap();
        exact_originals(&after_tap, &expected, &tag);
        exact_originals(&completed.frames, &expected[..1], &tag);
        assert!(!completed
            .frames
            .iter()
            .any(|item| item.frame.id == expected[1].id));
        let images = completed.backend.images.lock().unwrap();
        assert_eq!(*images, expected[..1]);
        assert_eq!(
            images[0].observation_token(),
            expected[0].observation_token()
        );
        assert_eq!(
            completed.report.errors.len(),
            1,
            "{:?}",
            completed.report.errors
        );
        assert_eq!(
            completed.report.errors[0].error,
            "bounded media queue is full"
        );
        assert!(!completed
            .report
            .errors
            .iter()
            .any(|error| error.error.contains("RTVI payload")));
    }
}
