//! Public wire settings -> paired transport ownership with recording app filters.
use pipecat::{
    audio::{decode_pcm16_le, encode_pcm16_le, scale_audio, PcmFormat},
    core::*,
    media_transport::*,
    network_bus::PythonJsonCodec,
    workers::MessageData,
};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(3);

struct CaptureCommand {
    frame: Frame,
    admitted: oneshot::Sender<Result<(), ProcessorError>>,
}
struct CaptureBackend {
    commands: tokio::sync::Mutex<mpsc::Receiver<CaptureCommand>>,
    starts: AtomicUsize,
    stops: AtomicUsize,
    captures: AtomicUsize,
    active: AtomicUsize,
}
struct ActiveCapture<'a>(&'a AtomicUsize);
impl Drop for ActiveCapture<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl InputBackend for CaptureBackend {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveCapture(&self.active);
        let mut commands = self.commands.lock().await;
        while let Some(command) = commands.recv().await {
            let _ = command.admitted.send(ingress.push(command.frame));
        }
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct RecordingOutput {
    starts: AtomicUsize,
    stops: AtomicUsize,
    written: Mutex<Vec<Frame>>,
}
#[async_trait]
impl OutputBackend for RecordingOutput {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        // This synthetic backend's acknowledgement boundary is the recorded write.
        self.written.lock().unwrap().push(frame.clone());
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// RNNoise currently handles enable only. This is a settings-aware application filter.
struct SettingsFilter {
    factor: Mutex<f32>,
    starts: AtomicUsize,
    stops: AtomicUsize,
    settings: AtomicUsize,
    controls: Mutex<Vec<FrameData>>,
    pcm: Mutex<Vec<(f32, Vec<i16>)>>,
}
impl SettingsFilter {
    fn new(factor: f32) -> Arc<Self> {
        Arc::new(Self {
            factor: Mutex::new(factor),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            settings: AtomicUsize::new(0),
            controls: Mutex::new(Vec::new()),
            pcm: Mutex::new(Vec::new()),
        })
    }
}
#[async_trait]
impl AudioFilter for SettingsFilter {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn filter(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        let factor = *self.factor.lock().unwrap();
        let samples = decode_pcm16_le(&audio).map_err(|e| ProcessorError::new(e.to_string()))?;
        self.pcm.lock().unwrap().push((factor, samples));
        scale_audio(&audio, factor).map_err(|e| ProcessorError::new(e.to_string()))
    }
    async fn control(&self, frame: &FrameData) -> Result<(), ProcessorError> {
        self.controls.lock().unwrap().push(frame.clone());
        if let FrameData::FilterUpdateSettings(settings) = frame {
            self.settings.fetch_add(1, Ordering::SeqCst);
            let factor = settings
                .get("factor")
                .and_then(serde_json::Value::as_f64)
                .ok_or_else(|| ProcessorError::new("factor missing from settings"))?;
            *self.factor.lock().unwrap() = factor as f32;
        }
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct Fixture {
    ingress: InputIngress,
    output_control: OutputControl,
    capture_tx: mpsc::Sender<CaptureCommand>,
    capture: Arc<CaptureBackend>,
    output: Arc<RecordingOutput>,
    input_filter: Option<Arc<SettingsFilter>>,
    output_filter: Arc<SettingsFilter>,
}
struct Session {
    worker: PipelineWorker,
    fixture: Fixture,
    observed: Vec<DirectedFrame>,
}
struct Finished {
    fixture: Fixture,
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
}
impl Session {
    async fn start(input_filter: Option<Arc<SettingsFilter>>) -> Self {
        let (capture_tx, commands) = mpsc::channel(4);
        let capture = Arc::new(CaptureBackend {
            commands: tokio::sync::Mutex::new(commands),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            captures: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        });
        let input = Arc::new(
            InputTransport::new(
                "settings-input",
                InputTransportConfig {
                    sample_rate: Some(8000),
                    operation_timeout: Duration::from_secs(1),
                    ..Default::default()
                },
                capture.clone(),
                input_filter
                    .clone()
                    .map(|filter| filter as Arc<dyn AudioFilter>),
            )
            .unwrap(),
        );
        let ingress = input.ingress();
        let output = Arc::new(RecordingOutput::default());
        let output_filter = SettingsFilter::new(3.0);
        let output_transport = Arc::new(
            OutputTransport::new(
                "settings-output",
                OutputTransportConfig {
                    sample_rate: Some(8000),
                    chunk_duration: Duration::from_millis(10),
                    auto_silence: false,
                    end_silence: Duration::ZERO,
                    write_timeout: Duration::from_secs(1),
                    ..Default::default()
                },
                output.clone(),
                Some(output_filter.clone()),
                BTreeMap::new(),
            )
            .unwrap(),
        );
        let output_control = output_transport.control();
        let worker = Pipeline::new(vec![input, output_transport])
            .start(PipelineConfig {
                shutdown_timeout: WAIT,
                ..Default::default()
            })
            .await
            .unwrap();
        timeout(WAIT, async {
            while !ingress.is_ready()
                || !output_control.is_ready()
                || capture.active.load(Ordering::SeqCst) != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("paired transports and owned capture must become ready");
        Self {
            worker,
            fixture: Fixture {
                ingress,
                output_control,
                capture_tx,
                capture,
                output,
                input_filter,
                output_filter,
            },
            observed: Vec::new(),
        }
    }
    async fn capture(&self, frame: Frame) {
        let (admitted, result) = oneshot::channel();
        timeout(WAIT, async {
            self.fixture
                .capture_tx
                .send(CaptureCommand { frame, admitted })
                .await
                .unwrap();
            result.await.unwrap().unwrap();
        })
        .await
        .expect("real capture task must acknowledge bounded ingress admission");
    }
    async fn send(&self, frame: Frame, direction: Direction) {
        self.worker.sender().send(frame, direction).await.unwrap();
    }
    async fn observe(&mut self, wanted: impl Fn(&[DirectedFrame]) -> bool) {
        timeout(WAIT, async {
            while !wanted(&self.observed) {
                self.observed.push(
                    self.worker
                        .recv()
                        .await
                        .expect("pipeline closed before expected observation"),
                );
            }
        })
        .await
        .expect("expected actual frame before deadline; timeouts are not baseline failures");
    }
    async fn observe_frame(&mut self, expected: &Frame) {
        let token = expected.observation_token();
        self.observe(|frames| frames.iter().any(|f| f.frame.observation_token() == token))
            .await;
    }
    async fn finish(self) -> Finished {
        self.worker.end().await.unwrap();
        let (remaining, report) = timeout(WAIT + WAIT, self.worker.collect())
            .await
            .expect("normal End must drain and join")
            .unwrap();
        let mut frames = self.observed;
        frames.extend(remaining);
        Finished {
            fixture: self.fixture,
            frames,
            report,
        }
    }
}
impl Finished {
    fn assert_clean(&self) {
        assert_eq!(self.report.state, WorkerState::Ended);
        assert!(self.report.errors.is_empty(), "{:?}", self.report.errors);
        assert_eq!(self.report.total_errors, 0);
        assert_eq!(self.fixture.capture.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.fixture.capture.stops.load(Ordering::SeqCst), 1);
        assert_eq!(self.fixture.capture.captures.load(Ordering::SeqCst), 1);
        assert_eq!(self.fixture.capture.active.load(Ordering::SeqCst), 0);
        assert_eq!(self.fixture.output.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.fixture.output.stops.load(Ordering::SeqCst), 1);
        assert!(!self.fixture.ingress.is_ready());
        assert!(!self.fixture.output_control.is_ready());
        for filter in self
            .fixture
            .input_filter
            .iter()
            .chain(std::iter::once(&self.fixture.output_filter))
        {
            assert_eq!(filter.starts.load(Ordering::SeqCst), 1);
            assert_eq!(filter.stops.load(Ordering::SeqCst), 1);
        }
        assert_eq!(
            self.frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::End(_) | FrameData::EndDetailed(_)))
                .count(),
            1
        );
    }
}

fn settings_from_wire(direction: Direction) -> (Frame, Direction) {
    let bytes = serde_json::to_vec(&json!({
        "__type__": "pipecat.bus.messages.BusFrameMessage",
        "__data__": {
            "source": "settings-controller",
            "target": "paired-transports",
            "frame": {
                "__type__": "pipecat.frames.frames.FilterUpdateSettingsFrame",
                "__data__": {
                    "id": 38001,
                    "name": "application-filter-settings",
                    "pts": null,
                    "broadcast_sibling_id": 38002,
                    "metadata": {"owner": "microphone", "nested": {"revision": 1}},
                    "transport_source": "microphone-controller",
                    "transport_destination": null,
                    "interruptible": false,
                    "settings": {"factor": 4}
                }
            },
            "direction": {
                "__type__": "pipecat.processors.frame_processor.FrameDirection",
                "__data__": match direction {
                    Direction::Downstream => "DOWNSTREAM",
                    Direction::Upstream => "UPSTREAM"
                }
            }
        }
    }))
    .unwrap();
    let envelope = PythonJsonCodec::default().decode(&bytes).unwrap();
    match &envelope.as_bus_message().unwrap().data {
        MessageData::Frame {
            frame, direction, ..
        } => ((**frame).clone(), *direction),
        other => panic!("expected typed settings frame, received {other:?}"),
    }
}
fn input_pcm(context: &str) -> Frame {
    let mut audio = AudioRawFrame::new(encode_pcm16_le(&[1000; 80]), 8000, 1).unwrap();
    audio.context_id = Some(context.into());
    Frame::new(FrameData::InputAudio(audio))
}
fn playback_pcm() -> Frame {
    Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(encode_pcm16_le(&[1000; 80]), 8000, 1).unwrap(),
    ))
}
fn assert_same_envelope(actual: &Frame, expected: &Frame) {
    assert_eq!(actual.observation_token(), expected.observation_token());
    assert_eq!(actual.id, expected.id);
    assert_eq!(actual.name, expected.name);
    assert_eq!(actual.pts, expected.pts);
    assert_eq!(actual.has_pipeline_pts(), expected.has_pipeline_pts());
    assert_eq!(actual.metadata, expected.metadata);
    assert_eq!(actual.transport_source, expected.transport_source);
    assert_eq!(actual.transport_destination, expected.transport_destination);
    assert_eq!(actual.broadcast_sibling_id, expected.broadcast_sibling_id);
    assert_eq!(actual.interruptible, expected.interruptible);
    assert_eq!(actual.data, expected.data);
}

#[tokio::test]
async fn decoded_input_settings_do_not_reconfigure_output() {
    let input_filter = SettingsFilter::new(2.0);
    let mut session = Session::start(Some(input_filter.clone())).await;
    let before = input_pcm("before-settings");
    session.capture(before.clone()).await;
    session.observe_frame(&before).await;

    let (settings, direction) = settings_from_wire(Direction::Downstream);
    session.send(settings.clone(), direction).await;
    let marker = Frame::new(FrameData::FilterControl);
    session.send(marker.clone(), Direction::Downstream).await;
    session.observe_frame(&marker).await;

    // Input PCM and normal controls use different core lanes. The control marker
    // witnesses serialized callbacks, not arrival of unrelated PCM at this edge.
    let after = input_pcm("after-settings");
    session.capture(after.clone()).await;
    session.observe_frame(&after).await;
    session.send(playback_pcm(), Direction::Downstream).await;
    session
        .observe(|frames| {
            frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::OutputAudio(_)))
        })
        .await;
    let finished = session.finish().await;
    finished.assert_clean();

    // Old-source red is this ownership assertion after successful End and join.
    assert_eq!(
        finished
            .fixture
            .output_filter
            .settings
            .load(Ordering::SeqCst),
        0,
        "input-owned settings reached the playback filter"
    );
    assert_eq!(input_filter.settings.load(Ordering::SeqCst), 1);
    assert!(!finished
        .frames
        .iter()
        .any(|f| f.frame.observation_token() == settings.observation_token()));
    assert_eq!(
        *input_filter.pcm.lock().unwrap(),
        [(2.0, vec![1000; 80]), (4.0, vec![1000; 80])]
    );
    assert_eq!(
        *finished.fixture.output_filter.pcm.lock().unwrap(),
        [(3.0, vec![1000; 80])]
    );
    for (expected, value) in [(&before, 2000), (&after, 4000)] {
        let frames: Vec<_> = finished
            .frames
            .iter()
            .filter(|f| f.frame.observation_token() == expected.observation_token())
            .collect();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].direction, Direction::Downstream);
        let FrameData::InputAudio(audio) = &frames[0].frame.data else {
            panic!("expected captured PCM")
        };
        assert_eq!(decode_pcm16_le(&audio.audio).unwrap(), vec![value; 80]);
    }
    let written = finished.fixture.output.written.lock().unwrap();
    assert_eq!(written.len(), 1);
    let FrameData::OutputAudio(audio) = &written[0].data else {
        panic!("expected playback PCM")
    };
    assert_eq!(decode_pcm16_le(&audio.audio).unwrap(), vec![3000; 80]);
}

#[tokio::test]
async fn decoded_unfiltered_upstream_settings_keep_direction() {
    let mut session = Session::start(None).await;
    let (settings, direction) = settings_from_wire(Direction::Upstream);
    // Public upstream send enters the last processor (the actual OutputTransport).
    session.send(settings.clone(), direction).await;
    // Both old and fixed code reach an edge. Do not wait only for the wanted edge.
    session.observe_frame(&settings).await;
    let finished = session.finish().await;
    finished.assert_clean();
    let frames: Vec<_> = finished
        .frames
        .iter()
        .filter(|f| f.frame.observation_token() == settings.observation_token())
        .collect();
    assert_eq!(frames.len(), 1);
    // Old-source red is wrong direction, after normal lifecycle completion.
    assert_eq!(frames[0].direction, Direction::Upstream);
    assert_same_envelope(&frames[0].frame, &settings);
    assert_eq!(
        finished
            .fixture
            .output_filter
            .settings
            .load(Ordering::SeqCst),
        0
    );
    assert!(finished.fixture.output.written.lock().unwrap().is_empty());
}

use pipecat::audio::SincResampler;
use tokio::sync::Semaphore;

enum SettingsAction {
    Fail,
    Pending,
    Gated(Arc<Semaphore>),
}
struct ControlledSettingsFilter {
    inner: Arc<SettingsFilter>,
    action: SettingsAction,
    entered: Semaphore,
    calls: AtomicUsize,
    active: AtomicUsize,
}
impl ControlledSettingsFilter {
    fn new(inner: Arc<SettingsFilter>, action: SettingsAction) -> Arc<Self> {
        Arc::new(Self {
            inner,
            action,
            entered: Semaphore::new(0),
            calls: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        })
    }
}
#[async_trait]
impl AudioFilter for ControlledSettingsFilter {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        self.inner.start(format).await
    }
    async fn filter(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        self.inner.filter(audio).await
    }
    async fn control(&self, frame: &FrameData) -> Result<(), ProcessorError> {
        if matches!(frame, FrameData::FilterUpdateSettings(_)) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.active.fetch_add(1, Ordering::SeqCst);
            let _active = ActiveCapture(&self.active);
            self.entered.add_permits(1);
            match &self.action {
                SettingsAction::Fail => {
                    return Err(ProcessorError::new("application settings rejected"))
                }
                SettingsAction::Pending => return std::future::pending().await,
                SettingsAction::Gated(gate) => gate.acquire().await.unwrap().forget(),
            }
            self.inner.control(frame).await
        } else {
            self.inner.control(frame).await
        }
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.inner.stop().await
    }
}

impl Session {
    async fn start_with(
        input_filter: Option<Arc<SettingsFilter>>,
        filter_override: Option<Arc<dyn AudioFilter>>,
        destinations: Vec<String>,
    ) -> Self {
        let (capture_tx, commands) = mpsc::channel(4);
        let capture = Arc::new(CaptureBackend {
            commands: tokio::sync::Mutex::new(commands),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            captures: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
        });
        let input = Arc::new(
            InputTransport::new(
                "settings-input",
                InputTransportConfig {
                    sample_rate: Some(8000),
                    operation_timeout: Duration::from_secs(1),
                    ..Default::default()
                },
                capture.clone(),
                filter_override.or_else(|| {
                    input_filter
                        .clone()
                        .map(|filter| filter as Arc<dyn AudioFilter>)
                }),
            )
            .unwrap(),
        );
        let ingress = input.ingress();
        let output = Arc::new(RecordingOutput::default());
        let output_filter = SettingsFilter::new(3.0);
        let output_transport = Arc::new(
            OutputTransport::new(
                "settings-output",
                OutputTransportConfig {
                    sample_rate: Some(8000),
                    destinations,
                    chunk_duration: Duration::from_millis(10),
                    auto_silence: false,
                    end_silence: Duration::ZERO,
                    write_timeout: Duration::from_secs(1),
                    ..Default::default()
                },
                output.clone(),
                Some(output_filter.clone()),
                BTreeMap::new(),
            )
            .unwrap(),
        );
        let output_control = output_transport.control();
        let worker = Pipeline::new(vec![input, output_transport])
            .start(PipelineConfig {
                shutdown_timeout: WAIT,
                ..Default::default()
            })
            .await
            .unwrap();
        timeout(WAIT, async {
            while !ingress.is_ready()
                || !output_control.is_ready()
                || capture.active.load(Ordering::SeqCst) != 1
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("paired transports and owned capture must become ready");
        Self {
            worker,
            fixture: Fixture {
                ingress,
                output_control,
                capture_tx,
                capture,
                output,
                input_filter,
                output_filter,
            },
            observed: Vec::new(),
        }
    }
    async fn collect_after_end(self) -> Finished {
        let (remaining, report) = timeout(WAIT + WAIT, self.worker.collect())
            .await
            .expect("normal End must drain and join")
            .unwrap();
        let mut frames = self.observed;
        frames.extend(remaining);
        Finished {
            fixture: self.fixture,
            frames,
            report,
        }
    }
}
impl Finished {
    fn assert_lifecycle(&self) {
        assert_eq!(self.report.state, WorkerState::Ended);
        assert_eq!(self.fixture.capture.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.fixture.capture.stops.load(Ordering::SeqCst), 1);
        assert_eq!(self.fixture.capture.captures.load(Ordering::SeqCst), 1);
        assert_eq!(self.fixture.capture.active.load(Ordering::SeqCst), 0);
        assert_eq!(self.fixture.output.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.fixture.output.stops.load(Ordering::SeqCst), 1);
        assert!(!self.fixture.ingress.is_ready());
        assert!(!self.fixture.output_control.is_ready());
        for filter in self
            .fixture
            .input_filter
            .iter()
            .chain(std::iter::once(&self.fixture.output_filter))
        {
            assert_eq!(filter.starts.load(Ordering::SeqCst), 1);
            assert_eq!(filter.stops.load(Ordering::SeqCst), 1);
        }
        assert_eq!(
            self.frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::End(_) | FrameData::EndDetailed(_)))
                .count(),
            1
        );
    }
}

fn routed_settings_from_wire(
    direction: Direction,
    destination: Option<&str>,
    pts: Option<u64>,
) -> (Frame, Direction) {
    let bytes = serde_json::to_vec(&json!({
        "__type__": "pipecat.bus.messages.BusFrameMessage",
        "__data__": {
            "source": "settings-controller",
            "target": "paired-transports",
            "frame": {
                "__type__": "pipecat.frames.frames.FilterUpdateSettingsFrame",
                "__data__": {
                    "id": 38001,
                    "name": "application-filter-settings",
                    "pts": pts,
                    "broadcast_sibling_id": 38002,
                    "metadata": {"owner": "microphone", "nested": {"revision": 1}},
                    "transport_source": "microphone-controller",
                    "transport_destination": destination,
                    "interruptible": false,
                    "settings": {"factor": 4}
                }
            },
            "direction": {
                "__type__": "pipecat.processors.frame_processor.FrameDirection",
                "__data__": match direction {
                    Direction::Downstream => "DOWNSTREAM",
                    Direction::Upstream => "UPSTREAM"
                }
            }
        }
    }))
    .unwrap();
    let envelope = PythonJsonCodec::default().decode(&bytes).unwrap();
    match &envelope.as_bus_message().unwrap().data {
        MessageData::Frame {
            frame, direction, ..
        } => ((**frame).clone(), *direction),
        other => panic!("expected typed settings frame, received {other:?}"),
    }
}

#[tokio::test]
async fn decoded_unfiltered_downstream_settings_reach_output_once() {
    let mut session = Session::start(None).await;
    let (settings, direction) = settings_from_wire(Direction::Downstream);
    session.send(settings.clone(), direction).await;
    session.observe_frame(&settings).await;
    session.send(playback_pcm(), Direction::Downstream).await;
    session
        .observe(|frames| {
            frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::OutputAudio(_)))
        })
        .await;
    let finished = session.finish().await;
    finished.assert_clean();
    let frames: Vec<_> = finished
        .frames
        .iter()
        .filter(|f| f.frame.observation_token() == settings.observation_token())
        .collect();
    assert_eq!(frames.len(), 1);
    assert_eq!(frames[0].direction, Direction::Downstream);
    assert_same_envelope(&frames[0].frame, &settings);
    assert_eq!(
        finished
            .fixture
            .output_filter
            .settings
            .load(Ordering::SeqCst),
        1
    );
    let written = finished.fixture.output.written.lock().unwrap();
    assert_eq!(written.len(), 1);
    let FrameData::OutputAudio(audio) = &written[0].data else {
        panic!("expected playback PCM")
    };
    assert_eq!(decode_pcm16_le(&audio.audio).unwrap(), vec![4000; 80]);
}

#[tokio::test]
async fn unfiltered_settings_preserve_routed_wire_envelope_in_both_directions() {
    for direction in [Direction::Upstream, Direction::Downstream] {
        let mut session = Session::start_with(None, None, vec!["speaker".into()]).await;
        let (settings, decoded_direction) =
            routed_settings_from_wire(direction, Some("speaker"), Some(9001));
        session.send(settings.clone(), decoded_direction).await;
        session.observe_frame(&settings).await;
        let finished = session.finish().await;
        finished.assert_clean();
        let frames: Vec<_> = finished
            .frames
            .iter()
            .filter(|f| f.frame.observation_token() == settings.observation_token())
            .collect();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].direction, direction);
        assert_same_envelope(&frames[0].frame, &settings);
        // Output filters belong to the default endpoint, not this named route.
        assert_eq!(
            finished
                .fixture
                .output_filter
                .settings
                .load(Ordering::SeqCst),
            0
        );
        assert!(finished.fixture.output.written.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn configured_input_consumes_upstream_settings_while_capture_paused() {
    let input_filter = SettingsFilter::new(2.0);
    let mut session = Session::start(Some(input_filter.clone())).await;
    session.fixture.ingress.pause();
    let (settings, direction) = settings_from_wire(Direction::Upstream);
    session.send(settings.clone(), direction).await;
    timeout(WAIT, async {
        while input_filter.settings.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let marker = Frame::new(FrameData::FilterControl);
    session.send(marker.clone(), Direction::Downstream).await;
    session.observe_frame(&marker).await;
    session.fixture.ingress.resume();
    let after = input_pcm("after-paused-settings");
    session.capture(after.clone()).await;
    session.observe_frame(&after).await;
    let finished = session.finish().await;
    finished.assert_clean();
    assert_eq!(input_filter.settings.load(Ordering::SeqCst), 1);
    assert_eq!(*input_filter.pcm.lock().unwrap(), [(4.0, vec![1000; 80])]);
    assert_eq!(
        finished
            .fixture
            .output_filter
            .settings
            .load(Ordering::SeqCst),
        0
    );
    assert!(!finished
        .frames
        .iter()
        .any(|f| f.frame.observation_token() == settings.observation_token()));
    let actual = finished
        .frames
        .iter()
        .find(|f| f.frame.observation_token() == after.observation_token())
        .unwrap();
    let FrameData::InputAudio(audio) = &actual.frame.data else {
        panic!("expected captured PCM")
    };
    assert_eq!(decode_pcm16_le(&audio.audio).unwrap(), vec![4000; 80]);
}

fn resampled_input(context: &str) -> Frame {
    let mut audio = AudioRawFrame::new(encode_pcm16_le(&[1000; 160]), 16000, 1).unwrap();
    audio.context_id = Some(context.into());
    Frame::new(FrameData::InputAudio(audio))
}
fn captured_context<'a>(frames: &'a [DirectedFrame], context: &str) -> Vec<&'a AudioRawFrame> {
    frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::InputAudio(audio) if audio.context_id.as_deref() == Some(context) => {
                Some(audio)
            }
            _ => None,
        })
        .collect()
}
fn resampled_parts() -> (Vec<u8>, Vec<u8>) {
    let mut stream = SincResampler::new(16000, 8000, 1).unwrap();
    let prefix = stream.process(&encode_pcm16_le(&[1000; 160])).unwrap();
    let tail = stream.flush().unwrap();
    assert!(!prefix.is_empty());
    assert!(!tail.is_empty());
    (prefix, tail)
}

#[tokio::test]
async fn settings_flush_resampler_tail_with_old_gain_before_new_pcm() {
    let (prefix, tail) = resampled_parts();
    let input_filter = SettingsFilter::new(2.0);
    let mut session = Session::start(Some(input_filter.clone())).await;
    let before = resampled_input("old-gain");
    session.capture(before.clone()).await;
    session.observe_frame(&before).await;
    let (settings, direction) = settings_from_wire(Direction::Downstream);
    session.send(settings.clone(), direction).await;
    let marker = Frame::new(FrameData::FilterControl);
    session.send(marker.clone(), Direction::Downstream).await;
    session.observe_frame(&marker).await;
    // The normal marker may reach this edge before the separate System PCM tail.
    session
        .observe(|frames| captured_context(frames, "old-gain").len() == 2)
        .await;
    let after = resampled_input("new-gain");
    session.capture(after.clone()).await;
    session.observe_frame(&after).await;
    let finished = session.finish().await;
    finished.assert_clean();
    assert_eq!(input_filter.settings.load(Ordering::SeqCst), 1);
    assert_eq!(
        finished
            .fixture
            .output_filter
            .settings
            .load(Ordering::SeqCst),
        0
    );
    assert!(!finished
        .frames
        .iter()
        .any(|f| f.frame.observation_token() == settings.observation_token()));
    for (context, gain) in [("old-gain", 2.0), ("new-gain", 4.0)] {
        let captured = captured_context(&finished.frames, context);
        assert_eq!(captured.len(), 2);
        assert_eq!(captured[0].audio, scale_audio(&prefix, gain).unwrap());
        assert_eq!(captured[1].audio, scale_audio(&tail, gain).unwrap());
    }
    assert_eq!(
        *input_filter.pcm.lock().unwrap(),
        [
            (2.0, decode_pcm16_le(&prefix).unwrap()),
            (2.0, decode_pcm16_le(&tail).unwrap()),
            (4.0, decode_pcm16_le(&prefix).unwrap()),
            (4.0, decode_pcm16_le(&tail).unwrap()),
        ]
    );
    assert!(finished
        .fixture
        .output_filter
        .pcm
        .lock()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn generic_control_and_enable_keep_forwarding_and_tail_flush() {
    let (prefix, tail) = resampled_parts();
    let input_filter = SettingsFilter::new(2.0);
    let mut session = Session::start(Some(input_filter.clone())).await;
    let before = resampled_input("before-generic-control");
    session.capture(before.clone()).await;
    session.observe_frame(&before).await;
    let marker = Frame::new(FrameData::FilterControl);
    session.send(marker.clone(), Direction::Downstream).await;
    session.observe_frame(&marker).await;
    session
        .observe(|frames| captured_context(frames, "before-generic-control").len() == 2)
        .await;
    let enable = Frame::new(FrameData::FilterEnable(false));
    session.send(enable.clone(), Direction::Downstream).await;
    session.observe_frame(&enable).await;
    let after = input_pcm("after-disable");
    session.capture(after.clone()).await;
    session.observe_frame(&after).await;
    session.send(playback_pcm(), Direction::Downstream).await;
    session
        .observe(|frames| {
            frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::OutputAudio(_)))
        })
        .await;
    let finished = session.finish().await;
    finished.assert_clean();
    for expected in [&marker, &enable] {
        let frames: Vec<_> = finished
            .frames
            .iter()
            .filter(|f| f.frame.observation_token() == expected.observation_token())
            .collect();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].direction, Direction::Downstream);
        assert_same_envelope(&frames[0].frame, expected);
    }
    for filter in [&input_filter, &finished.fixture.output_filter] {
        assert_eq!(
            *filter.controls.lock().unwrap(),
            [FrameData::FilterControl, FrameData::FilterEnable(false)]
        );
        assert_eq!(filter.settings.load(Ordering::SeqCst), 0);
    }
    let before = captured_context(&finished.frames, "before-generic-control");
    assert_eq!(before.len(), 2);
    assert_eq!(before[0].audio, scale_audio(&prefix, 2.0).unwrap());
    assert_eq!(before[1].audio, scale_audio(&tail, 2.0).unwrap());
    let after = captured_context(&finished.frames, "after-disable");
    assert_eq!(after.len(), 1);
    assert_eq!(decode_pcm16_le(&after[0].audio).unwrap(), vec![2000; 80]);
    let written = finished.fixture.output.written.lock().unwrap();
    assert_eq!(written.len(), 1);
    let FrameData::OutputAudio(audio) = &written[0].data else {
        panic!("expected playback PCM")
    };
    assert_eq!(decode_pcm16_le(&audio.audio).unwrap(), vec![3000; 80]);
}

#[tokio::test]
async fn failed_or_timed_out_settings_do_not_leak_to_output() {
    for (action, expected_error) in [
        (SettingsAction::Fail, "application settings rejected"),
        (SettingsAction::Pending, "media backend operation timed out"),
    ] {
        let input_filter = SettingsFilter::new(2.0);
        let callback = ControlledSettingsFilter::new(input_filter.clone(), action);
        let mut session = Session::start_with(
            Some(input_filter.clone()),
            Some(callback.clone()),
            Vec::new(),
        )
        .await;
        let (settings, direction) = settings_from_wire(Direction::Downstream);
        session.send(settings.clone(), direction).await;
        session
            .observe(|frames| {
                frames
                    .iter()
                    .any(|f| matches!(f.frame.data, FrameData::Error(_)))
            })
            .await;
        let marker = Frame::new(FrameData::FilterControl);
        session.send(marker.clone(), Direction::Downstream).await;
        session.observe_frame(&marker).await;
        let finished = session.finish().await;
        finished.assert_lifecycle();
        assert_eq!(finished.report.total_errors, 1);
        assert_eq!(finished.report.errors.len(), 1);
        assert_eq!(finished.report.errors[0].error, expected_error);
        assert!(!finished.report.errors[0].fatal);
        let errors: Vec<_> = finished
            .frames
            .iter()
            .filter_map(|f| match &f.frame.data {
                FrameData::Error(error) => Some((f.direction, error)),
                _ => None,
            })
            .collect();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].0, Direction::Upstream);
        assert_eq!(errors[0].1.error, expected_error);
        assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
        assert_eq!(callback.active.load(Ordering::SeqCst), 0);
        assert_eq!(input_filter.settings.load(Ordering::SeqCst), 0);
        assert_eq!(*input_filter.factor.lock().unwrap(), 2.0);
        assert_eq!(
            finished
                .fixture
                .output_filter
                .settings
                .load(Ordering::SeqCst),
            0
        );
        assert!(!finished
            .frames
            .iter()
            .any(|f| f.frame.observation_token() == settings.observation_token()));
    }
}

#[tokio::test]
async fn normal_end_drains_admitted_settings_callback_before_cleanup() {
    let gate = Arc::new(Semaphore::new(0));
    let input_filter = SettingsFilter::new(2.0);
    let callback =
        ControlledSettingsFilter::new(input_filter.clone(), SettingsAction::Gated(gate.clone()));
    let session = Session::start_with(
        Some(input_filter.clone()),
        Some(callback.clone()),
        Vec::new(),
    )
    .await;
    let (settings, direction) = settings_from_wire(Direction::Downstream);
    session.send(settings.clone(), direction).await;
    timeout(WAIT, callback.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    session.worker.end().await.unwrap();
    timeout(WAIT, async {
        while session.fixture.ingress.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("End must close capture admission while settings are pending");
    gate.add_permits(1);
    let finished = session.collect_after_end().await;
    finished.assert_clean();
    assert_eq!(callback.active.load(Ordering::SeqCst), 0);
    assert_eq!(callback.calls.load(Ordering::SeqCst), 1);
    assert_eq!(input_filter.settings.load(Ordering::SeqCst), 1);
    assert_eq!(*input_filter.factor.lock().unwrap(), 4.0);
    assert_eq!(
        finished
            .fixture
            .output_filter
            .settings
            .load(Ordering::SeqCst),
        0
    );
    assert!(!finished
        .frames
        .iter()
        .any(|f| f.frame.observation_token() == settings.observation_token()));
}
