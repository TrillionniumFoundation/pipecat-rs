use async_trait::async_trait;
use pipecat_audio::{encode_pcm16_le, resample_pcm16, scale_audio, PcmFormat};
use pipecat_core::*;
use pipecat_media_transport::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

struct Backend {
    format: Mutex<Option<PcmFormat>>,
    stopped: AtomicUsize,
    processed: Arc<Semaphore>,
}
impl Default for Backend {
    fn default() -> Self {
        Self {
            format: Mutex::new(None),
            stopped: AtomicUsize::new(0),
            processed: Arc::new(Semaphore::new(0)),
        }
    }
}
struct ObservedInput {
    input: Arc<InputTransport>,
    processed: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for ObservedInput {
    fn name(&self) -> &str {
        self.input.name()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.input.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let is_audio = raw(&frame).is_some();
        let result = self.input.process(frame, direction, ctx).await;
        if is_audio {
            self.processed.add_permits(1);
        }
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.input.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.input.cleanup().await
    }
}
async fn inject(worker: &PipelineWorker, backend: &Backend, frame: Frame, direction: Direction) {
    worker.sender().send(frame, direction).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), backend.processed.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
#[async_trait]
impl InputBackend for Backend {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        *self.format.lock().unwrap() = Some(format);
        Ok(())
    }
    async fn capture(&self, _: InputIngress) -> Result<(), ProcessorError> {
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[derive(Clone, Copy)]
enum Transform {
    Gain,
    Empty,
    LargeLength,
    LargeCapacity,
    EmptyCapacity,
    InvalidPcm,
}
struct Filter {
    transform: Transform,
    calls: AtomicUsize,
    active: AtomicUsize,
    completed: AtomicUsize,
    starts: AtomicUsize,
    stops: AtomicUsize,
    entered: Notify,
    gate: Option<Semaphore>,
}
struct ActiveFilter<'a>(&'a AtomicUsize);
impl Drop for ActiveFilter<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl Filter {
    fn new(transform: Transform, block: bool) -> Arc<Self> {
        Arc::new(Self {
            transform,
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            completed: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            entered: Notify::new(),
            gate: block.then(|| Semaphore::new(0)),
        })
    }
    async fn wait_calls(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let entered = self.entered.notified();
                if self.calls.load(Ordering::SeqCst) >= count {
                    break;
                }
                entered.await;
            }
        })
        .await
        .unwrap();
    }
}
#[async_trait]
impl AudioFilter for Filter {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn filter(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveFilter(&self.active);
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        self.completed.fetch_add(1, Ordering::SeqCst);
        match self.transform {
            Transform::Gain => {
                scale_audio(&audio, 2.0).map_err(|error| ProcessorError::new(error.to_string()))
            }
            Transform::Empty => Ok(Vec::new()),
            Transform::LargeLength => Ok(vec![0; 4096]),
            Transform::LargeCapacity | Transform::EmptyCapacity => {
                let mut result = Vec::with_capacity(4096);
                if matches!(self.transform, Transform::LargeCapacity) {
                    result.extend_from_slice(&[0, 0]);
                }
                Ok(result)
            }
            Transform::InvalidPcm => Ok(vec![1]),
        }
    }
    async fn control(&self, _: &FrameData) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn config() -> InputTransportConfig {
    InputTransportConfig {
        sample_rate: Some(8000),
        stream_on_start: false,
        operation_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}
fn audio(user: Option<&str>, rate: u32, samples: &[i16]) -> Frame {
    let audio = AudioRawFrame::new(encode_pcm16_le(samples), rate, 1).unwrap();
    Frame::new(match user {
        None => FrameData::InputAudio(audio),
        Some(user) => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: user.into(),
        }),
    })
}
fn raw(frame: &Frame) -> Option<&AudioRawFrame> {
    match &frame.data {
        FrameData::InputAudio(audio) => Some(audio),
        FrameData::UserAudio(audio) => Some(&audio.audio),
        _ => None,
    }
}
fn raw_mut(frame: &mut Frame) -> &mut AudioRawFrame {
    match &mut frame.data {
        FrameData::InputAudio(audio) => audio,
        FrameData::UserAudio(audio) => &mut audio.audio,
        _ => panic!("expected input audio"),
    }
}
async fn start(
    config: InputTransportConfig,
    filter: Option<Arc<Filter>>,
) -> (PipelineWorker, InputIngress, Arc<Backend>) {
    let backend = Arc::new(Backend::default());
    let input = Arc::new(
        InputTransport::new(
            "input-audio-test",
            config,
            backend.clone(),
            filter.map(|filter| filter as Arc<dyn AudioFilter>),
        )
        .unwrap(),
    );
    let ingress = input.ingress();
    let worker = Pipeline::new(vec![Arc::new(ObservedInput {
        input,
        processed: backend.processed.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !ingress.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    (worker, ingress, backend)
}
async fn finish(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap()
}
fn audio_frames(frames: &[DirectedFrame]) -> Vec<&DirectedFrame> {
    frames
        .iter()
        .filter(|frame| raw(&frame.frame).is_some())
        .collect()
}

#[tokio::test]
async fn capture_and_injection_filter_both_audio_classes_and_preserve_identity() {
    for user in [None, Some("speaker")] {
        for direction in [Direction::Downstream, Direction::Upstream] {
            for capture in [false, true] {
                let filter = Filter::new(Transform::Gain, false);
                let (worker, ingress, backend) = start(config(), Some(filter.clone())).await;
                let mut frame = audio(user, 8000, &[1200, -2400]);
                raw_mut(&mut frame).context_id = Some("context".into());
                frame.name = "application-input".into();
                frame.set_pipeline_pts(12345);
                frame.transport_source = Some("microphone".into());
                frame.transport_destination = Some("target".into());
                frame
                    .metadata
                    .insert("app".into(), serde_json::json!({"n": 7}));
                frame.interruptible = false;
                frame.broadcast_sibling_id = Some(123);
                let tag = LocalFrameTag::new();
                tag.mark(&mut frame).unwrap();
                let expected = frame.clone();
                let admitted = if capture {
                    ingress.push(frame)
                } else {
                    inject(&worker, &backend, frame, direction).await;
                    Ok(())
                };
                let (frames, report) = finish(worker).await;
                assert!(admitted.is_ok(), "{admitted:?}");
                assert!(report.errors.is_empty(), "{:?}", report.errors);
                let received = audio_frames(&frames);
                assert_eq!(received.len(), 1);
                let actual = &received[0].frame;
                assert_eq!(received[0].direction, Direction::Downstream);
                assert_eq!(raw(actual).unwrap().audio, encode_pcm16_le(&[2400, -4800]));
                assert_eq!(raw(actual).unwrap().sample_rate, 8000);
                assert_eq!(raw(actual).unwrap().context_id, Some("context".into()));
                assert_eq!(
                    actual.data.source_class_name(),
                    expected.data.source_class_name()
                );
                if let FrameData::UserAudio(user_audio) = &actual.data {
                    assert_eq!(user_audio.user_id, "speaker");
                }
                assert_eq!(actual.id, expected.id);
                assert_eq!(actual.name, expected.name);
                assert_eq!(actual.pts, expected.pts);
                assert_eq!(actual.has_pipeline_pts(), expected.has_pipeline_pts());
                assert_eq!(actual.metadata, expected.metadata);
                assert_eq!(actual.transport_source, expected.transport_source);
                assert_eq!(actual.transport_destination, expected.transport_destination);
                assert_eq!(actual.interruptible, expected.interruptible);
                assert_eq!(actual.broadcast_sibling_id, expected.broadcast_sibling_id);
                assert_eq!(actual.observation_token(), expected.observation_token());
                assert!(tag.contains(actual));
                assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
                assert_eq!(filter.starts.load(Ordering::SeqCst), 1);
                assert_eq!(filter.stops.load(Ordering::SeqCst), 1);
                assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
                assert_eq!(backend.format.lock().unwrap().unwrap().sample_rate(), 8000);
            }
        }
    }
}

#[tokio::test]
async fn audio_disabled_pause_and_passthrough_apply_to_both_entry_paths() {
    for gate in ["disabled", "paused", "passthrough"] {
        for user in [None, Some("speaker")] {
            for capture in [false, true] {
                let mut config = config();
                config.audio_enabled = gate != "disabled";
                config.passthrough = gate != "passthrough";
                let filter = Filter::new(Transform::Gain, false);
                let (worker, ingress, backend) = start(config, Some(filter.clone())).await;
                if gate == "paused" {
                    ingress.pause();
                }
                let frame = audio(user, 8000, &[100]);
                let admitted = if capture {
                    ingress.push(frame)
                } else {
                    inject(&worker, &backend, frame, Direction::Upstream).await;
                    Ok(())
                };
                let (frames, report) = finish(worker).await;
                assert!(admitted.is_ok(), "{admitted:?}");
                assert!(report.errors.is_empty(), "{:?}", report.errors);
                assert!(audio_frames(&frames).is_empty(), "gate={gate}");
                assert_eq!(
                    filter.calls.load(Ordering::SeqCst),
                    usize::from(gate == "passthrough")
                );
                assert_eq!(filter.starts.load(Ordering::SeqCst), 1);
                assert_eq!(filter.stops.load(Ordering::SeqCst), 1);
            }
        }
    }
}

#[tokio::test]
async fn native_empty_input_and_filtered_empty_output_are_dropped() {
    for user in [None, Some("speaker")] {
        for empty_input in [false, true] {
            let filter = Filter::new(Transform::Empty, false);
            let (worker, ingress, _) = start(config(), Some(filter.clone())).await;
            let admitted = ingress.push(audio(user, 8000, if empty_input { &[] } else { &[100] }));
            let (frames, report) = finish(worker).await;
            assert!(admitted.is_ok(), "{admitted:?}");
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert!(audio_frames(&frames).is_empty());
            assert_eq!(
                filter.calls.load(Ordering::SeqCst),
                usize::from(!empty_input)
            );
        }
    }
}

#[tokio::test]
async fn malformed_audio_rejects_without_replacing_the_pending_tail() {
    for user in [None, Some("speaker")] {
        let (worker, ingress, _) = start(config(), None).await;
        let good = audio(user, 16000, &[500; 24]);
        let id = good.id;
        let admitted = ingress.push(good);
        let mut rejected = Vec::new();
        for bad_kind in ["bytes", "rate", "channels"] {
            let mut bad = audio(user, 16000, &[900]);
            match bad_kind {
                "bytes" => raw_mut(&mut bad).audio.push(1),
                "rate" => raw_mut(&mut bad).sample_rate = 0,
                _ => raw_mut(&mut bad).num_channels = 0,
            }
            rejected.push((bad_kind, ingress.push(bad)));
        }
        let wrong_families = [
            FrameData::OutputAudio(AudioRawFrame::new(vec![0, 0], 8000, 1).unwrap()),
            FrameData::TtsAudio(AudioRawFrame::new(vec![0, 0], 8000, 1).unwrap()),
        ]
        .map(|data| ingress.push(Frame::new(data)));
        let (frames, report) = finish(worker).await;
        assert!(admitted.is_ok(), "{admitted:?}");
        for (kind, result) in rejected {
            assert!(result.is_err(), "{kind}");
        }
        assert!(wrong_families.iter().all(Result::is_err));
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let received = audio_frames(&frames);
        assert_eq!(received.len(), 1);
        assert_ne!(received[0].frame.id, id);
        assert_eq!(
            raw(&received[0].frame).unwrap().audio,
            encode_pcm16_le(&[500; 12])
        );
    }
}

#[tokio::test]
async fn nonempty_input_owns_the_tail_after_an_empty_frame_with_new_envelope() {
    for user in [None, Some("speaker")] {
        let (worker, ingress, _) = start(config(), None).await;
        let mut original = audio(user, 16000, &[700; 96]);
        original
            .metadata
            .insert("owner".into(), serde_json::json!("nonempty"));
        original.set_pipeline_pts(100);
        original.interruptible = false;
        original.broadcast_sibling_id = Some(55);
        let tag = LocalFrameTag::new();
        tag.mark(&mut original).unwrap();
        let expected = original.clone();
        let mut empty = audio(user, 16000, &[]);
        empty
            .metadata
            .insert("owner".into(), serde_json::json!("empty"));
        empty.set_pipeline_pts(900);
        let admissions = [ingress.push(original), ingress.push(empty)];
        let (frames, report) = finish(worker).await;
        assert!(admissions.iter().all(Result::is_ok), "{admissions:?}");
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let received = audio_frames(&frames);
        assert!(received.len() >= 2);
        let mut bytes = Vec::new();
        for item in &received {
            let frame = &item.frame;
            assert_eq!(frame.metadata, expected.metadata);
            assert_eq!(frame.pts, expected.pts);
            assert!(frame.has_pipeline_pts());
            assert!(!frame.interruptible);
            assert!(tag.contains(frame));
            assert_eq!(item.direction, Direction::Downstream);
            if frame.id == expected.id {
                assert_eq!(frame.observation_token(), expected.observation_token());
                assert_eq!(frame.broadcast_sibling_id, Some(55));
            } else {
                assert_ne!(frame.observation_token(), expected.observation_token());
                assert_eq!(frame.broadcast_sibling_id, None);
                assert!(frame.name.starts_with(frame.data.source_class_name()));
            }
            bytes.extend_from_slice(&raw(frame).unwrap().audio);
        }
        assert_eq!(bytes, encode_pcm16_le(&[700; 48]));
    }
}

#[tokio::test]
async fn resampling_keeps_stream_boundaries_and_tail_payload_identity() {
    for boundary in [
        "context",
        "destination",
        "source",
        "rate",
        "user",
        "plain-to-user",
        "user-to-plain",
    ] {
        let (worker, ingress, _) = start(config(), None).await;
        let mut first = audio(
            if matches!(boundary, "user" | "user-to-plain") {
                Some("alice")
            } else {
                None
            },
            16000,
            &[1000; 24],
        );
        let mut second = audio(
            if matches!(boundary, "user" | "plain-to-user") {
                Some("alice")
            } else {
                None
            },
            16000,
            &[-1000; 24],
        );
        first.transport_source = Some("mic".into());
        second.transport_source = Some("mic".into());
        first.transport_destination = Some("left".into());
        second.transport_destination = Some("left".into());
        raw_mut(&mut first).context_id = Some("ctx-a".into());
        raw_mut(&mut second).context_id = Some("ctx-a".into());
        match boundary {
            "context" => raw_mut(&mut second).context_id = Some("ctx-b".into()),
            "destination" => second.transport_destination = Some("right".into()),
            "source" => second.transport_source = Some("other-mic".into()),
            "rate" => raw_mut(&mut second).sample_rate = 24000,
            "user" => {
                if let FrameData::UserAudio(audio) = &mut second.data {
                    audio.user_id = "bob".into();
                }
            }
            "plain-to-user" | "user-to-plain" => {}
            _ => unreachable!(),
        }
        first
            .metadata
            .insert("segment".into(), serde_json::json!(1));
        second
            .metadata
            .insert("segment".into(), serde_json::json!(2));
        let expected = [first.clone(), second.clone()];
        let admissions = [ingress.push(first), ingress.push(second)];
        let (frames, report) = finish(worker).await;
        assert!(admissions.iter().all(Result::is_ok), "{admissions:?}");
        assert!(report.errors.is_empty(), "{boundary}: {:?}", report.errors);
        let received = audio_frames(&frames);
        assert_eq!(received.len(), 2, "{boundary}");
        for (actual, expected) in received.iter().zip(expected.iter()) {
            assert_eq!(actual.direction, Direction::Downstream);
            assert_eq!(actual.frame.metadata, expected.metadata, "{boundary}");
            assert_eq!(actual.frame.transport_source, expected.transport_source);
            assert_eq!(
                actual.frame.transport_destination,
                expected.transport_destination
            );
            assert_eq!(
                actual.frame.data.source_class_name(),
                expected.data.source_class_name()
            );
            assert_eq!(
                raw(&actual.frame).unwrap().context_id,
                raw(expected).unwrap().context_id
            );
            if let (FrameData::UserAudio(actual), FrameData::UserAudio(expected)) =
                (&actual.frame.data, &expected.data)
            {
                assert_eq!(actual.user_id, expected.user_id);
            }
            let source = raw(expected).unwrap();
            assert_eq!(
                raw(&actual.frame).unwrap().audio,
                resample_pcm16(&source.audio, source.sample_rate, 8000, 1).unwrap(),
                "{boundary}"
            );
            assert_ne!(
                actual.frame.observation_token(),
                expected.observation_token()
            );
        }
    }
}

#[tokio::test]
async fn split_user_audio_resampling_matches_whole_pcm_with_tail() {
    let (worker, ingress, _) = start(config(), None).await;
    let samples: Vec<i16> = (0..501).map(|sample| sample * 40 - 10000).collect();
    let admissions: Vec<_> = samples
        .chunks(33)
        .map(|chunk| ingress.push(audio(Some("speaker"), 16000, chunk)))
        .collect();
    let (frames, report) = finish(worker).await;
    assert!(admissions.iter().all(Result::is_ok), "{admissions:?}");
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let actual: Vec<u8> = audio_frames(&frames)
        .iter()
        .flat_map(|frame| raw(&frame.frame).unwrap().audio.iter().copied())
        .collect();
    assert_eq!(
        actual,
        resample_pcm16(&encode_pcm16_le(&samples), 16000, 8000, 1).unwrap()
    );
    assert!(audio_frames(&frames).iter().all(|frame| matches!(&frame.frame.data, FrameData::UserAudio(audio) if audio.user_id == "speaker")));
}

#[tokio::test]
async fn retained_input_allocations_cannot_bypass_admission_budget() {
    for user in [None, Some("speaker")] {
        let mut config = config();
        config.queue_byte_capacity = 1024;
        let (worker, ingress, _) = start(config, None).await;
        let mut rejected = Vec::new();
        for kind in [
            "pcm",
            "name",
            "source",
            "destination",
            "context",
            "empty-context",
            "metadata",
            "metadata-key",
            "nested",
            "empty-array",
            "deep",
        ] {
            let mut frame = audio(user, 8000, &[1]);
            let reserve = || {
                let mut text = String::with_capacity(4096);
                text.push('x');
                text
            };
            match kind {
                "pcm" => raw_mut(&mut frame).audio.reserve(4096),
                "name" => frame.name = reserve(),
                "source" => frame.transport_source = Some(reserve()),
                "destination" => frame.transport_destination = Some(reserve()),
                "context" => raw_mut(&mut frame).context_id = Some(reserve()),
                "empty-context" => {
                    raw_mut(&mut frame).context_id = Some(String::with_capacity(4096))
                }
                "metadata-key" => {
                    frame.metadata.insert(reserve(), serde_json::Value::Null);
                }
                "empty-array" => {
                    frame.metadata.insert(
                        "empty".into(),
                        serde_json::Value::Array(Vec::with_capacity(128)),
                    );
                }
                "metadata" => {
                    frame
                        .metadata
                        .insert("large".into(), serde_json::Value::String(reserve()));
                }
                "nested" => {
                    frame.metadata.insert(
                        "array".into(),
                        serde_json::Value::Array(vec![serde_json::Value::Null; 128]),
                    );
                }
                "deep" => {
                    let mut value = serde_json::Value::Null;
                    for _ in 0..=NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
                        value = serde_json::json!([value]);
                    }
                    frame.metadata.insert("deep".into(), value);
                }
                _ => unreachable!(),
            }
            let large_capacity = match kind {
                "pcm" => raw(&frame).unwrap().audio.capacity(),
                "name" => frame.name.capacity(),
                "source" => frame.transport_source.as_ref().unwrap().capacity(),
                "destination" => frame.transport_destination.as_ref().unwrap().capacity(),
                "context" => raw(&frame).unwrap().context_id.as_ref().unwrap().capacity(),
                "metadata" => match &frame.metadata["large"] {
                    serde_json::Value::String(text) => text.capacity(),
                    _ => unreachable!(),
                },
                "empty-context" => raw(&frame).unwrap().context_id.as_ref().unwrap().capacity(),
                "metadata-key" => frame.metadata.keys().next().unwrap().capacity(),
                _ => 0,
            };
            rejected.push((kind, large_capacity, ingress.push(frame)));
        }
        let mut frame = audio(Some("speaker"), 8000, &[1]);
        if let FrameData::UserAudio(audio) = &mut frame.data {
            audio.user_id.reserve(4096);
        }
        let user_rejected = ingress.push(frame);
        let recovered = ingress.push(audio(user, 8000, &[2]));
        let (frames, report) = finish(worker).await;
        for (kind, capacity, result) in rejected {
            if matches!(
                kind,
                "pcm"
                    | "name"
                    | "source"
                    | "destination"
                    | "context"
                    | "empty-context"
                    | "metadata"
                    | "metadata-key"
            ) {
                assert!(capacity > 1024, "{kind} fixture capacity");
            }
            assert!(result.is_err(), "{kind}, user={user:?}");
        }
        assert!(user_rejected.is_err());
        assert!(recovered.is_ok(), "{recovered:?}");
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(audio_frames(&frames).len(), 1);
    }
}

#[tokio::test]
async fn queue_count_and_user_bytes_release_after_processing() {
    for count_limited in [false, true] {
        let mut config = config();
        config.queue_capacity = if count_limited { 1 } else { 8 };
        config.queue_byte_capacity = 512;
        let filter = Filter::new(Transform::Gain, true);
        let (worker, ingress, _) = start(config, Some(filter.clone())).await;
        ingress.push(audio(None, 8000, &[100])).unwrap();
        filter.wait_calls(1).await;
        let user = "u".repeat(300);
        let admitted = ingress.push(audio(Some(&user), 8000, &[200]));
        if admitted.is_err() {
            filter.gate.as_ref().unwrap().add_permits(8);
            finish(worker).await;
            assert!(admitted.is_ok(), "{admitted:?}");
            unreachable!();
        }
        let overfull = ingress.push(audio(Some(&user), 8000, &[300]));
        filter.gate.as_ref().unwrap().add_permits(1);
        filter.wait_calls(2).await;
        let recovered = ingress.push(audio(Some(&user), 8000, &[400]));
        filter.gate.as_ref().unwrap().add_permits(8);
        let (frames, report) = finish(worker).await;
        assert!(overfull.is_err(), "{overfull:?}");
        assert!(recovered.is_ok(), "{recovered:?}");
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let values: Vec<Vec<u8>> = audio_frames(&frames)
            .iter()
            .map(|frame| raw(&frame.frame).unwrap().audio.clone())
            .collect();
        assert_eq!(
            values,
            [
                encode_pcm16_le(&[200]),
                encode_pcm16_le(&[400]),
                encode_pcm16_le(&[800])
            ]
        );
    }
}

#[tokio::test]
async fn filtered_output_is_validated_and_retained_budget_is_enforced() {
    for user in [None, Some("speaker")] {
        for transform in [
            Transform::LargeLength,
            Transform::LargeCapacity,
            Transform::EmptyCapacity,
            Transform::InvalidPcm,
        ] {
            let mut config = config();
            config.queue_byte_capacity = 1024;
            let filter = Filter::new(transform, false);
            let (worker, ingress, _) = start(config, Some(filter.clone())).await;
            let admitted = ingress.push(audio(user, 8000, &[100]));
            let (frames, report) = finish(worker).await;
            assert!(admitted.is_ok(), "{admitted:?}");
            assert!(audio_frames(&frames).is_empty());
            assert_eq!(report.errors.len(), 1);
            assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
            assert_eq!(filter.stops.load(Ordering::SeqCst), 1);
        }
    }
}

#[tokio::test]
async fn cancellation_discards_queued_user_audio_and_rejects_late_ingress() {
    let filter = Filter::new(Transform::Gain, true);
    let (worker, ingress, backend) = start(config(), Some(filter.clone())).await;
    ingress.push(audio(None, 8000, &[100])).unwrap();
    filter.wait_calls(1).await;
    let admitted = ingress.push(audio(Some("speaker"), 8000, &[200]));
    let entered_active = filter.active.load(Ordering::SeqCst);
    worker.cancel("test shutdown").await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(admitted.is_ok(), "{admitted:?}");
    assert_eq!(entered_active, 1);
    assert_eq!(filter.active.load(Ordering::SeqCst), 0);
    assert_eq!(filter.completed.load(Ordering::SeqCst), 0);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(audio_frames(&frames).is_empty());
    assert_eq!(filter.calls.load(Ordering::SeqCst), 1);
    assert_eq!(filter.stops.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert!(!ingress.is_ready());
    assert!(ingress.push(audio(Some("speaker"), 8000, &[300])).is_err());
}

#[tokio::test]
async fn compact_plain_pcm_keeps_the_exact_byte_boundary() {
    let mut bytes = Vec::with_capacity(128);
    for _ in 0..64 {
        bytes.extend_from_slice(&1_i16.to_le_bytes());
    }
    assert_eq!(bytes.capacity(), 128);
    let frame = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(bytes, 8000, 1).unwrap(),
    ));
    let mut config = config();
    config.queue_byte_capacity = 128;
    let (worker, ingress, _) = start(config, None).await;
    let admitted = ingress.push(frame);
    let (frames, report) = finish(worker).await;
    assert!(admitted.is_ok(), "{admitted:?}");
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(audio_frames(&frames).len(), 1);
}
