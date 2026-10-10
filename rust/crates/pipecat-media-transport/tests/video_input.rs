use async_trait::async_trait;
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::{video::*, *};
use serde_json::Value;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct Backend {
    stopped: AtomicUsize,
    connected: Notify,
}
#[async_trait]
impl InputBackend for Backend {
    fn supports_video_source(&self, source: &VideoSourceId) -> bool {
        source == &VideoSourceId::camera() || source == &VideoSourceId::screen()
    }
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError> {
        // Synthetic backend confirms configured captures before connected event.
        let available: Vec<_> = ingress.configured_video_sources().keys().cloned().collect();
        ingress
            .video_participant_connected(VideoParticipantId::new("peer").unwrap(), &available)?;
        self.connected.notify_one();
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn key(participant: &str, source: &str) -> VideoSourceKey {
    VideoSourceKey::new(
        VideoParticipantId::new(participant).unwrap(),
        VideoSourceId::new(source).unwrap(),
    )
}
fn config() -> InputTransportConfig {
    let mut video = VideoInputConfig::default();
    video.sources.insert(
        VideoSourceId::camera(),
        VideoInSourceParams::on_request_only(),
    );
    video.sources.insert(
        VideoSourceId::screen(),
        VideoInSourceParams::on_request_only(),
    );
    InputTransportConfig {
        video_enabled: true,
        video,
        operation_timeout: Duration::from_millis(100),
        ..Default::default()
    }
}
fn image(value: u8) -> Frame {
    Frame::new(FrameData::InputImage(ImageRawFrame {
        image: vec![value; 3],
        size: (1, 1),
        format: Some("RGB".into()),
    }))
}
fn request(key: &VideoSourceKey, tool: &str, log: &Arc<Mutex<Vec<(String, Value)>>>) -> Frame {
    let log = log.clone();
    let id = tool.to_owned();
    Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
        user_id: key.participant.as_str().into(),
        video_source: Some(key.source.as_str().into()),
        text: Some("what is this?".into()),
        append_to_context: Some(true),
        function_name: Some("look".into()),
        tool_call_id: Some(tool.into()),
        result_callback: Some(LocalResultCallback::new(move |value| {
            log.lock().unwrap().push((id.clone(), value));
            async { Ok(()) }
        })),
    }))
}
async fn start(config: InputTransportConfig) -> (PipelineWorker, InputIngress, Arc<Backend>) {
    let backend = Arc::new(Backend::default());
    let input = Arc::new(InputTransport::new("video", config, backend.clone(), None).unwrap());
    let ingress = input.ingress();
    let worker = Pipeline::new(vec![input])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), backend.connected.notified())
        .await
        .unwrap();
    (worker, ingress, backend)
}
async fn wait_results(log: &Arc<Mutex<Vec<(String, Value)>>>, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while log.lock().unwrap().len() < count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn finish(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap()
}
#[tokio::test]
async fn next_frame_fifo_source_isolation_correlation_and_graceful_drain() {
    let (worker, ingress, backend) = start(config()).await;
    let log = Arc::new(Mutex::new(vec![]));
    let camera = key("peer", "camera");
    let screen = key("peer", "screenVideo");
    ingress.push_video_frame(&camera, image(0)).unwrap();
    // Exercise the real processor request path as well as backend ingress.
    worker
        .sender()
        .send(request(&camera, "first", &log), Direction::Upstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while ingress.pending_image_requests() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    ingress
        .request_image(request(&camera, "second", &log))
        .unwrap();
    ingress
        .request_image(request(&screen, "screen", &log))
        .unwrap();
    ingress
        .capture_video_source(
            camera.clone(),
            VideoInSourceParams { framerate: Some(1) },
            true,
        )
        .unwrap();
    ingress.push_video_frame(&camera, image(1)).unwrap();
    ingress.push_video_frame(&camera, image(2)).unwrap();
    ingress
        .push_video_frame(&key("other", "camera"), image(9))
        .unwrap();
    ingress.push_video_frame(&screen, image(3)).unwrap();
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let images: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::UserImage(image) => Some((&f.frame, image)),
            _ => None,
        })
        .collect();
    assert_eq!(images.len(), 3);
    for (index, (frame, image)) in images.iter().enumerate() {
        assert_eq!(image.image.image, [index as u8 + 1; 3]);
        assert_eq!(image.text.as_deref(), Some("what is this?"));
        assert_eq!(image.append_to_context, Some(true));
        assert!(image.request.as_ref().unwrap().result_callback.is_none());
        assert_eq!(
            frame.transport_source.as_deref(),
            Some(if index == 2 { "screenVideo" } else { "camera" })
        );
    }
    assert_eq!(
        log.lock()
            .unwrap()
            .iter()
            .map(|(id, value)| (id.as_str(), value.is_null()))
            .collect::<Vec<_>>(),
        [("first", true), ("second", true), ("screen", true)]
    );
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert!(ingress.push_video_frame(&camera, image(4)).is_err());
}
#[tokio::test]
async fn unavailable_source_off_and_disconnect_complete_once_and_keep_other_sources() {
    let (worker, ingress, _) = start(config()).await;
    let log = Arc::new(Mutex::new(vec![]));
    let camera = key("peer", "camera");
    let screen = key("peer", "screenVideo");
    ingress
        .request_image(request(&key("unknown", "camera"), "missing", &log))
        .unwrap();
    ingress.set_video_source_available(&camera, false).unwrap();
    ingress
        .request_image(request(&camera, "off", &log))
        .unwrap();
    ingress.set_video_source_available(&camera, true).unwrap();
    ingress
        .request_image(request(&camera, "stopped", &log))
        .unwrap();
    ingress.set_video_source_available(&camera, false).unwrap();
    ingress
        .request_image(request(&screen, "left", &log))
        .unwrap();
    ingress.remove_video_participant(&camera.participant);
    ingress.disconnect_video();
    wait_results(&log, 4).await;
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    let results = log.lock().unwrap();
    assert_eq!(results.len(), 4);
    assert!(results
        .iter()
        .all(|(_, value)| value.get("error").is_some()));
    assert_eq!(results[0].1["video_source"], "camera");
}
#[tokio::test]
async fn waiting_requests_fail_once_on_end_and_cancel() {
    for cancel in [false, true] {
        let (worker, ingress, backend) = start(config()).await;
        let log = Arc::new(Mutex::new(vec![]));
        let camera = key("peer", "camera");
        ingress
            .request_image(request(&camera, "one", &log))
            .unwrap();
        ingress
            .request_image(request(&camera, "two", &log))
            .unwrap();
        if cancel {
            worker.cancel("video test").await.unwrap();
        } else {
            worker.end().await.unwrap();
        }
        let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty());
        assert_eq!(log.lock().unwrap().len(), 2);
        assert!(log
            .lock()
            .unwrap()
            .iter()
            .all(|(_, value)| value.get("error").is_some()));
        assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn duplicate_ids_lifetime_tombstones_bounded_requests_and_route_isolation() {
    let mut conf = config();
    conf.video.max_request_ids = 2;
    conf.video.max_pending_requests = 1;
    conf.video.request_destination = Some("video-route".into());
    let (worker, ingress, _) = start(conf).await;
    let log = Arc::new(Mutex::new(vec![]));
    let camera = key("peer", "camera");
    let mut first = request(&camera, "one", &log);
    assert!(ingress.request_image(first.clone()).is_err());
    first.transport_destination = Some("video-route".into());
    ingress.request_image(first.clone()).unwrap();
    assert!(ingress
        .request_image(first.clone())
        .unwrap_err()
        .message
        .contains("duplicate"));
    let mut second = request(&camera, "two", &log);
    second.transport_destination = first.transport_destination.clone();
    assert!(ingress.request_image(second.clone()).is_err());
    ingress.push_video_frame(&camera, image(1)).unwrap();
    wait_results(&log, 1).await;
    assert!(ingress
        .request_image(first.duplicate())
        .unwrap_err()
        .message
        .contains("duplicate"));
    ingress.request_image(second).unwrap();
    ingress.push_video_frame(&camera, image(2)).unwrap();
    wait_results(&log, 2).await;
    let mut third = request(&camera, "three", &log);
    third.transport_destination = first.transport_destination.clone();
    assert!(ingress
        .request_image(third)
        .unwrap_err()
        .message
        .contains("bounds"));
    let foreign = request(&camera, "foreign", &log);
    let foreign_id = foreign.id;
    worker
        .sender()
        .send(foreign, Direction::Upstream)
        .await
        .unwrap();
    let (frames, _) = finish(worker).await;
    assert!(frames.iter().any(
        |f| f.frame.id == foreign_id && matches!(f.frame.data, FrameData::UserImageRequest(_))
    ));
    assert_eq!(log.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn image_and_context_byte_bounds_do_not_consume_the_waiting_request() {
    let mut conf = config();
    conf.queue_byte_capacity = 256;
    conf.video.max_request_bytes = 64;
    let (worker, ingress, _) = start(conf).await;
    let camera = key("peer", "camera");
    let log = Arc::new(Mutex::new(vec![]));
    let mut oversized = request(&camera, "large", &log);
    if let FrameData::UserImageRequest(r) = &mut oversized.data {
        r.text = Some("x".repeat(100));
    }
    assert!(ingress.request_image(oversized).is_err());
    ingress.request_image(request(&camera, "ok", &log)).unwrap();
    let mut frame = image(1);
    frame
        .metadata
        .insert("large".into(), Value::Array(vec![Value::Null; 300]));
    assert!(ingress.push_video_frame(&camera, frame).is_err());
    ingress.push_video_frame(&camera, image(2)).unwrap();
    let (frames, _) = finish(worker).await;
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::UserImage(_)))
            .count(),
        1
    );
    assert_eq!(log.lock().unwrap().as_slice(), [("ok".into(), Value::Null)]);
}
#[tokio::test]
async fn disabled_or_unsupported_capture_is_not_silently_enabled() {
    let backend = Arc::new(Backend::default());
    let mut conf = config();
    conf.video_enabled = false;
    assert!(InputTransport::new("video", conf, backend.clone(), None).is_err());
    let mut conf = config();
    conf.video.sources.insert(
        VideoSourceId::new("unknown").unwrap(),
        VideoInSourceParams::default(),
    );
    let input = Arc::new(InputTransport::new("video", conf, backend.clone(), None).unwrap());
    assert!(Pipeline::new(vec![input])
        .start(PipelineConfig::default())
        .await
        .is_err());
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_racing_image_admission_has_one_consistent_completion() {
    for _ in 0..32 {
        let (worker, ingress, _) = start(config()).await;
        let log = Arc::new(Mutex::new(vec![]));
        let camera = key("peer", "camera");
        ingress
            .request_image(request(&camera, "race", &log))
            .unwrap();
        let gate = Arc::new(std::sync::Barrier::new(2));
        let capture = ingress.clone();
        let signal = gate.clone();
        let thread = std::thread::spawn(move || {
            signal.wait();
            capture.push_video_frame(&camera, image(1))
        });
        gate.wait();
        worker.cancel("race").await.unwrap();
        let _ = thread.join().unwrap();
        let (frames, _) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        let results = log.lock().unwrap();
        assert_eq!(results.len(), 1);
        let images = frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::UserImage(_)))
            .count();
        assert_eq!(images, usize::from(results[0].1.is_null()));
        assert_eq!(ingress.pending_image_requests(), 0);
    }
}
#[tokio::test(start_paused = true)]
async fn monotonic_backend_sampling_and_muted_frames_do_not_consume_requests() {
    let (worker, ingress, _) = start(config()).await;
    let camera = key("peer", "camera");
    let log = Arc::new(Mutex::new(vec![]));
    ingress
        .capture_video_source(
            camera.clone(),
            VideoInSourceParams { framerate: Some(1) },
            true,
        )
        .unwrap();
    ingress.push_video_frame(&camera, image(1)).unwrap();
    tokio::time::advance(Duration::from_millis(100)).await;
    ingress.push_video_frame(&camera, image(9)).unwrap();
    ingress
        .request_image(request(&camera, "request", &log))
        .unwrap();
    ingress.pause();
    ingress.push_video_frame(&camera, image(9)).unwrap();
    ingress.resume();
    ingress.push_video_frame(&camera, image(2)).unwrap();
    tokio::time::advance(Duration::from_millis(800)).await;
    ingress.push_video_frame(&camera, image(9)).unwrap();
    tokio::time::advance(Duration::from_millis(200)).await;
    ingress.push_video_frame(&camera, image(3)).unwrap();
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    let values: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::UserImage(i) => Some(i.image.image[0]),
            _ => None,
        })
        .collect();
    assert_eq!(values, [1, 2, 3]);
    assert_eq!(log.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn stalled_callback_cannot_delay_backend_release_past_cleanup_deadline() {
    let backend = Arc::new(Backend::default());
    let mut conf = config();
    conf.operation_timeout = Duration::from_secs(1);
    let input = Arc::new(InputTransport::new("video", conf, backend.clone(), None).unwrap());
    let ingress = input.ingress();
    let worker = Pipeline::new(vec![input])
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_millis(20),
            ..Default::default()
        })
        .await
        .unwrap();
    backend.connected.notified().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let mut frame = request(
        &key("peer", "camera"),
        "stall",
        &Arc::new(Mutex::new(vec![])),
    );
    if let FrameData::UserImageRequest(request) = &mut frame.data {
        let count = calls.clone();
        request.result_callback = Some(LocalResultCallback::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            std::future::pending()
        }));
    }
    ingress.request_image(frame).unwrap();
    worker.cancel("stop").await.unwrap();
    assert!(worker.collect().await.is_err());
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(ingress.pending_image_requests(), 0);
}

#[tokio::test]
async fn distinct_sources_can_admit_same_public_image_id_without_cross_correlation() {
    let (worker, ingress, _) = start(config()).await;
    let log = Arc::new(Mutex::new(vec![]));
    let camera = key("peer", "camera");
    let screen = key("peer", "screenVideo");
    ingress
        .request_image(request(&camera, "camera", &log))
        .unwrap();
    ingress
        .request_image(request(&screen, "screen", &log))
        .unwrap();
    let first = image(1);
    let mut second = image(2);
    second.id = first.id;
    ingress.push_video_frame(&camera, first).unwrap();
    ingress.push_video_frame(&screen, second).unwrap();
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    let images: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::UserImage(image) => Some((
                image.image.image[0],
                image
                    .request
                    .as_ref()
                    .unwrap()
                    .tool_call_id
                    .as_deref()
                    .unwrap(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(images, [(1, "camera"), (2, "screen")]);
    assert_eq!(
        log.lock().unwrap().as_slice(),
        [
            ("camera".into(), Value::Null),
            ("screen".into(), Value::Null)
        ]
    );
}
