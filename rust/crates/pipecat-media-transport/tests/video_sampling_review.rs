use async_trait::async_trait;
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::{video::*, *};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
struct Backend {
    connected: Notify,
}
#[async_trait]
impl InputBackend for Backend {
    fn supports_video_source(&self, _: &VideoSourceId) -> bool {
        true
    }
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError> {
        ingress.video_participant_connected(
            VideoParticipantId::new("peer").unwrap(),
            &[VideoSourceId::camera()],
        )?;
        self.connected.notify_one();
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
fn camera() -> VideoSourceKey {
    VideoSourceKey::new(
        VideoParticipantId::new("peer").unwrap(),
        VideoSourceId::camera(),
    )
}
fn config() -> InputTransportConfig {
    let mut config = InputTransportConfig {
        video_enabled: true,
        operation_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    config.video.sources.insert(
        VideoSourceId::camera(),
        VideoInSourceParams::on_request_only(),
    );
    config
}
fn image(value: u8) -> Frame {
    Frame::new(FrameData::InputImage(ImageRawFrame {
        image: vec![value; 3],
        size: (1, 1),
        format: Some("RGB".into()),
    }))
}
type Results = Arc<Mutex<Vec<(String, Value)>>>;
fn request(tool: &str, results: &Results) -> Frame {
    let results = results.clone();
    let id = tool.to_owned();
    Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
        user_id: "peer".into(),
        video_source: Some("camera".into()),
        text: Some(tool.into()),
        append_to_context: Some(true),
        function_name: Some("look".into()),
        tool_call_id: Some(tool.into()),
        result_callback: Some(LocalResultCallback::new(move |value| {
            results.lock().unwrap().push((id.clone(), value));
            async { Ok(()) }
        })),
    }))
}
async fn start(
    config: InputTransportConfig,
    filter: Option<Arc<dyn AudioFilter>>,
) -> (PipelineWorker, InputIngress) {
    let backend = Arc::new(Backend::default());
    let input =
        Arc::new(InputTransport::new("video-review", config, backend.clone(), filter).unwrap());
    let ingress = input.ingress();
    let worker = Pipeline::new(vec![input])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), backend.connected.notified())
        .await
        .unwrap();
    (worker, ingress)
}
async fn finish(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn source_off_queued_image_cannot_complete_a_new_request_with_reused_image_id() {
    let (worker, ingress) = start(config(), None).await;
    let results = Results::default();
    let frame = image(1);
    ingress.request_image(request("old", &results)).unwrap();
    ingress.push_video_frame(&camera(), frame.clone()).unwrap();
    ingress
        .set_video_source_available(&camera(), false)
        .unwrap();
    ingress.set_video_source_available(&camera(), true).unwrap();
    ingress.request_image(request("new", &results)).unwrap();
    // Reusing an image identity must either be rejected or retain exact request
    // correlation, even while a source-off image remains in the media queue.
    let accepted = ingress.push_video_frame(&camera(), frame).is_ok();
    if !accepted {
        ingress.push_video_frame(&camera(), image(2)).unwrap();
    }
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let tools: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::UserImage(image) => image
                .request
                .as_ref()
                .and_then(|request| request.tool_call_id.as_deref()),
            _ => None,
        })
        .collect();
    assert_eq!(tools, ["new"]);
    let results = results.lock().unwrap();
    assert_eq!(results.len(), 2);
    assert!(results
        .iter()
        .find(|(id, _)| id == "old")
        .unwrap()
        .1
        .get("error")
        .is_some());
    assert!(results
        .iter()
        .find(|(id, _)| id == "new")
        .unwrap()
        .1
        .is_null());
}

#[derive(Default)]
struct FailingAudioFilter {
    called: Notify,
}
#[async_trait]
impl AudioFilter for FailingAudioFilter {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn filter(&self, _: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        self.called.notify_one();
        Err(ProcessorError::new("review filter failure"))
    }
    async fn control(&self, _: &FrameData) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}

#[tokio::test]
async fn audio_tail_failure_does_not_orphan_the_next_correlated_image() {
    let filter = Arc::new(FailingAudioFilter::default());
    let mut config = config();
    config.sample_rate = Some(8_000);
    let (worker, ingress) = start(config, Some(filter.clone())).await;
    let results = Results::default();
    ingress
        .push(Frame::new(FrameData::InputAudio(
            AudioRawFrame::new(vec![0; 2], 16_000, 1).unwrap(),
        )))
        .unwrap();
    ingress.request_image(request("image", &results)).unwrap();
    ingress.push_video_frame(&camera(), image(1)).unwrap();
    tokio::time::timeout(Duration::from_secs(2), filter.called.notified())
        .await
        .unwrap();
    // Completion may either report the flush failure or forward the independent
    // image, but it cannot wait for a future session shutdown.
    let completed = tokio::time::timeout(Duration::from_millis(100), async {
        while results.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    let _ = finish(worker).await;
    assert!(
        completed,
        "a popped image remained queued in the request ledger after audio flush failed"
    );
}

#[tokio::test]
async fn completion_releases_request_capacity_and_allows_synchronous_reentry() {
    let mut config = config();
    config.video.max_pending_requests = 1;
    config.video.max_request_bytes = 36;
    let (worker, ingress) = start(config, None).await;
    let results = Results::default();
    let mut first = request("first", &results);
    let reentrant = ingress.clone();
    let recorded = results.clone();
    if let FrameData::UserImageRequest(payload) = &mut first.data {
        payload.result_callback = Some(LocalResultCallback::new(move |value| {
            let (sent, received) = std::sync::mpsc::channel();
            let reentrant = reentrant.clone();
            let recorded = recorded.clone();
            std::thread::spawn(move || {
                let pending = reentrant.pending_image_requests();
                let result = reentrant.request_image(request("second", &recorded));
                if result.is_ok() {
                    reentrant
                        .set_video_source_available(&camera(), false)
                        .unwrap();
                }
                recorded.lock().unwrap().push(("first".into(), value));
                let _ = sent.send((pending, result));
            });
            // A callback invoked under the lifecycle mutex cannot complete this
            // reentrant operation. Bound the probe so a regression fails cleanly.
            let result = received.recv_timeout(Duration::from_millis(200));
            async move {
                let (pending, admission) =
                    result.map_err(|_| ProcessorError::new("callback ran under lifecycle lock"))?;
                assert_eq!(pending, 0);
                admission
            }
        }));
    }
    ingress.request_image(first).unwrap();
    ingress.push_video_frame(&camera(), image(1)).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while results.lock().unwrap().len() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(ingress.pending_image_requests(), 0);
    let results = results.lock().unwrap();
    assert!(results
        .iter()
        .find(|(id, _)| id == "first")
        .unwrap()
        .1
        .is_null());
    assert!(results
        .iter()
        .find(|(id, _)| id == "second")
        .unwrap()
        .1
        .get("error")
        .is_some());
}

#[tokio::test]
async fn correlation_bytes_are_charged_before_sampler_consumption() {
    let mut config = config();
    config.queue_byte_capacity = 100;
    let (worker, ingress) = start(config, None).await;
    let results = Results::default();
    let mut first = request("large", &results);
    if let FrameData::UserImageRequest(payload) = &mut first.data {
        payload.text = Some("x".repeat(80));
    }
    ingress.request_image(first).unwrap();
    // The raw image fits, but retaining two copies of request text does not.
    assert!(ingress.push_video_frame(&camera(), image(1)).is_err());
    assert_eq!(ingress.pending_image_requests(), 1);
    ingress
        .set_video_source_available(&camera(), false)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while results.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    ingress.set_video_source_available(&camera(), true).unwrap();
    ingress.request_image(request("small", &results)).unwrap();
    ingress.push_video_frame(&camera(), image(2)).unwrap();
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let images: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::UserImage(image) => Some(image.image.image[0]),
            _ => None,
        })
        .collect();
    assert_eq!(images, [2]);
    let results = results.lock().unwrap();
    assert_eq!(results.len(), 2);
    assert!(results[0].1.get("error").is_some());
    assert!(results[1].1.is_null());
}

#[test]
fn sampling_matches_pinned_python_schedule_away_from_nanosecond_rounding_edges() {
    // Direct translation of af9f9da's arithmetic oracle, with a deterministic
    // jittered stream, stalls, requests and rate changes. No Python runtime.
    for initial_rate in [None, Some(0), Some(1), Some(5), Some(30), Some(90)] {
        let mut sampler = VideoInSampler::new(
            VideoInSourceParams {
                framerate: initial_rate,
            },
            8,
        )
        .unwrap();
        let mut rate = initial_rate;
        let mut next = 0.0_f64;
        let mut now = 10.0_f64;
        let mut requests = std::collections::VecDeque::new();
        let mut seed = 91_u64;
        for index in 0..2_000_u64 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            now += 0.01 + (seed % 2_000) as f64 / 100_003.0;
            if index % 107 == 0 {
                now += 1.1;
            }
            if index % 73 == 0 {
                sampler.add_request(index).unwrap();
                requests.push_back(index);
            }
            if index == 501 {
                rate = Some(3);
                sampler
                    .set_params(VideoInSourceParams { framerate: rate })
                    .unwrap();
            }
            let requested = requests.pop_front();
            let due = match rate {
                None => true,
                Some(0) => requested.is_some(),
                Some(rate) => {
                    let interval = 1.0 / f64::from(rate);
                    let tolerance = 0.05_f64.min(interval / 2.0);
                    if requested.is_none() && now < next - tolerance {
                        false
                    } else {
                        next = if requested.is_some() || now - next > interval {
                            now + interval
                        } else {
                            next.max(now - tolerance) + interval
                        };
                        true
                    }
                }
            };
            assert_eq!(
                sampler.sample(Duration::from_secs_f64(now)),
                (due, requested),
                "rate={initial_rate:?}, frame={index}"
            );
        }
    }
}

#[tokio::test]
async fn lifecycle_removal_suppresses_queued_images_and_preserves_other_participants() {
    for removal in ["source-off", "participant", "disconnect"] {
        let (worker, ingress) = start(config(), None).await;
        let results = Results::default();
        let other = VideoSourceKey::new(
            VideoParticipantId::new("other").unwrap(),
            VideoSourceId::camera(),
        );
        ingress
            .capture_video_source(other.clone(), VideoInSourceParams::on_request_only(), true)
            .unwrap();
        ingress.request_image(request("queued", &results)).unwrap();
        ingress.push_video_frame(&camera(), image(1)).unwrap();
        ingress.request_image(request("waiting", &results)).unwrap();
        let mut other_request = request("other", &results);
        if let FrameData::UserImageRequest(payload) = &mut other_request.data {
            payload.user_id = "other".into();
        }
        ingress.request_image(other_request).unwrap();
        ingress.push_video_frame(&other, image(2)).unwrap();
        match removal {
            "source-off" => ingress
                .set_video_source_available(&camera(), false)
                .unwrap(),
            "participant" => ingress.remove_video_participant(&camera().participant),
            "disconnect" => ingress.disconnect_video(),
            _ => unreachable!(),
        }
        let (frames, report) = finish(worker).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let participants: Vec<_> = frames
            .iter()
            .filter_map(|frame| match &frame.frame.data {
                FrameData::UserImage(image) => Some(image.user_id.as_str()),
                _ => None,
            })
            .collect();
        if removal == "disconnect" {
            assert!(participants.is_empty());
        } else {
            assert_eq!(participants, ["other"]);
        }
        let results = results.lock().unwrap();
        assert_eq!(results.len(), 3);
        for (id, value) in results.iter() {
            assert_eq!(value.is_null(), removal != "disconnect" && id == "other");
        }
        assert_eq!(ingress.pending_image_requests(), 0);
    }
}

#[tokio::test]
async fn full_queue_and_provenance_limits_preserve_the_waiting_request() {
    let mut config = config();
    config.queue_capacity = 1;
    let (mut worker, ingress) = start(config, None).await;
    let results = Results::default();
    ingress.push(image(0)).unwrap();
    ingress.request_image(request("next", &results)).unwrap();
    assert!(ingress
        .push_video_frame(&camera(), image(1))
        .unwrap_err()
        .message
        .contains("full"));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker.recv().await.unwrap();
            if matches!(frame.frame.data, FrameData::InputImage(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    let mut tagged = image(2);
    for _ in 0..LocalFrameTag::MAX_TAGS_PER_FRAME {
        LocalFrameTag::new().mark(&mut tagged).unwrap();
    }
    assert!(ingress
        .push_video_frame(&camera(), tagged)
        .unwrap_err()
        .message
        .contains("tag"));
    assert_eq!(ingress.pending_image_requests(), 1);
    ingress.push_video_frame(&camera(), image(3)).unwrap();
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let values: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::UserImage(image) => Some(image.image.image[0]),
            _ => None,
        })
        .collect();
    assert_eq!(values, [3]);
    assert_eq!(*results.lock().unwrap(), [("next".into(), Value::Null)]);
}
