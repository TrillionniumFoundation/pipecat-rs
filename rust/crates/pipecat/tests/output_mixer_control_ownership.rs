//! Wire controls are owned by the selected output, including outputs without mixers.
//! WAV mixing is real; Played acknowledges this test backend's recorded PCM only.
use base64::Engine;
use pipecat::{
    audio::{decode_pcm16_le, encode_pcm16_le, pcm_to_wav, PcmFormat},
    core::*,
    media_transport::{mixer::*, *},
    network_bus::PythonJsonCodec,
    workers::MessageData,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::time::timeout;

const WAIT: Duration = Duration::from_secs(3);
const RATE: u32 = 8000;
const CHUNK: usize = 80;
type Mixers = BTreeMap<Option<String>, Arc<dyn AudioMixer>>;

struct WavFixture {
    directory: PathBuf,
    originals: Vec<(PathBuf, Vec<u8>)>,
}
impl WavFixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "pipecat-output-mixer-ownership-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&directory).unwrap();
        Self {
            directory,
            originals: Vec::new(),
        }
    }
    fn mixer(&mut self, prefix: &str, normal: i16, alternate: i16) -> Arc<SoundfileMixer> {
        let mut sound_files = BTreeMap::new();
        for (sound, value) in [("normal", normal), ("alternate", alternate)] {
            let path = self.directory.join(format!("{prefix}-{sound}.wav"));
            let bytes = pcm_to_wav(&encode_pcm16_le(&[value; CHUNK]), RATE, 1).unwrap();
            std::fs::write(&path, &bytes).unwrap();
            self.originals.push((path.clone(), bytes));
            sound_files.insert(sound.to_owned(), path);
        }
        Arc::new(
            SoundfileMixer::new(SoundfileMixerConfig {
                sound_files,
                default_sound: "normal".into(),
                volume: 1.0,
                ..Default::default()
            })
            .unwrap(),
        )
    }
    fn assert_unchanged(&self) {
        for (path, original) in &self.originals {
            assert_eq!(&std::fs::read(path).unwrap(), original);
        }
    }
}
impl Drop for WavFixture {
    fn drop(&mut self) {
        // Only these test-created files and their exclusive directory are owned.
        for (path, _) in &self.originals {
            let _ = std::fs::remove_file(path);
        }
        let _ = std::fs::remove_dir(&self.directory);
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

struct Session {
    worker: PipelineWorker,
    outputs: [Arc<RecordingOutput>; 2],
    controls: [OutputControl; 2],
    observed: Vec<DirectedFrame>,
}
struct Finished {
    outputs: [Arc<RecordingOutput>; 2],
    controls: [OutputControl; 2],
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
}
impl Session {
    async fn start(
        mixers: [Mixers; 2],
        destinations: Vec<String>,
        write_timeout: Duration,
    ) -> Self {
        let outputs = [
            Arc::new(RecordingOutput::default()),
            Arc::new(RecordingOutput::default()),
        ];
        let mut transports = Vec::new();
        let mut controls = Vec::new();
        for (index, mixers) in mixers.into_iter().enumerate() {
            let transport = Arc::new(
                OutputTransport::new(
                    format!("mixer-output-{index}"),
                    OutputTransportConfig {
                        sample_rate: Some(RATE),
                        chunk_duration: Duration::from_millis(10),
                        destinations: destinations.clone(),
                        auto_silence: false,
                        end_silence: Duration::ZERO,
                        write_timeout,
                        ..Default::default()
                    },
                    outputs[index].clone(),
                    None,
                    mixers,
                )
                .unwrap(),
            );
            controls.push(transport.control());
            transports.push(transport as Arc<dyn FrameProcessor>);
        }
        let worker = Pipeline::new(transports)
            .start(PipelineConfig {
                shutdown_timeout: WAIT,
                ..Default::default()
            })
            .await
            .unwrap();
        timeout(WAIT, async {
            while controls.iter().any(|control| !control.is_ready()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both real outputs must become ready");
        Self {
            worker,
            outputs,
            controls: controls.try_into().ok().unwrap(),
            observed: Vec::new(),
        }
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
        .expect("expected actual boundary; a timeout is not a baseline behavior failure");
    }
    async fn play(&mut self, context: &str, destination: Option<&str>) {
        let (frame, direction) = wire_audio(context, &[0; CHUNK], RATE, destination);
        self.send(frame, direction).await;
        // OutputAudio and mixer controls share both normal processor lanes and
        // serial media queues. B's forwarded PCM acknowledges the entire prefix.
        self.observe(|frames| {
            frames.iter().any(|frame| {
                matches!(&frame.frame.data, FrameData::OutputAudio(audio)
                    if audio.context_id.as_deref() == Some(context))
            })
        })
        .await;
    }
    async fn finish(self) -> Finished {
        self.worker.end().await.unwrap();
        let (remaining, report) = timeout(WAIT + WAIT, self.worker.collect())
            .await
            .expect("normal End must drain and join both output workers")
            .unwrap();
        let mut frames = self.observed;
        frames.extend(remaining);
        Finished {
            outputs: self.outputs,
            controls: self.controls,
            frames,
            report,
        }
    }
}
impl Finished {
    fn assert_lifecycle(&self, state: WorkerState) {
        assert_eq!(self.report.state, state);
        for index in 0..2 {
            assert_eq!(self.outputs[index].starts.load(Ordering::SeqCst), 1);
            assert_eq!(self.outputs[index].stops.load(Ordering::SeqCst), 1);
            assert!(!self.controls[index].is_ready());
        }
    }
    fn assert_clean(&self) {
        self.assert_lifecycle(WorkerState::Ended);
        assert!(self.report.errors.is_empty(), "{:?}", self.report.errors);
        assert_eq!(self.report.total_errors, 0);
        assert_eq!(
            self.frames
                .iter()
                .filter(|frame| matches!(
                    frame.frame.data,
                    FrameData::End(_) | FrameData::EndDetailed(_)
                ))
                .count(),
            1
        );
    }
    fn assert_consumed(&self, controls: &[Frame]) {
        for control in controls {
            assert!(!self
                .frames
                .iter()
                .any(|frame| frame.frame.observation_token() == control.observation_token()));
        }
    }
    fn samples(&self, output: usize) -> Vec<Vec<i16>> {
        self.outputs[output]
            .written
            .lock()
            .unwrap()
            .iter()
            .map(|frame| {
                let FrameData::OutputAudio(audio) = &frame.data else {
                    panic!("expected output PCM")
                };
                decode_pcm16_le(&audio.audio).unwrap()
            })
            .collect()
    }
}

fn wire(
    class: &str,
    fields: Value,
    direction: Direction,
    destination: Option<&str>,
) -> (Frame, Direction) {
    static NEXT: AtomicU64 = AtomicU64::new(39000);
    let id = NEXT.fetch_add(2, Ordering::Relaxed);
    let mut data = json!({
        "id": id, "name": format!("ownership-{class}-{id}"), "pts": null,
        "broadcast_sibling_id": id + 1,
        "metadata": {"owner": "selected-output", "nested": {"revision": 1}},
        "transport_source": "mixer-controller", "transport_destination": destination,
        "interruptible": false
    });
    data.as_object_mut()
        .unwrap()
        .extend(fields.as_object().unwrap().clone());
    let bytes = serde_json::to_vec(&json!({
        "__type__": "pipecat.bus.messages.BusFrameMessage",
        "__data__": {
            "source": "mixer-controller", "target": "chained-outputs",
            "frame": {"__type__": format!("pipecat.frames.frames.{class}"), "__data__": data},
            "direction": {
                "__type__": "pipecat.processors.frame_processor.FrameDirection",
                "__data__": match direction { Direction::Downstream => "DOWNSTREAM", Direction::Upstream => "UPSTREAM" }
            }
        }
    })).unwrap();
    let decoded = PythonJsonCodec::default().decode(&bytes).unwrap();
    match &decoded.as_bus_message().unwrap().data {
        MessageData::Frame {
            frame, direction, ..
        } => ((**frame).clone(), *direction),
        other => panic!("expected typed frame, received {other:?}"),
    }
}
fn wire_audio(
    context: &str,
    samples: &[i16],
    rate: u32,
    destination: Option<&str>,
) -> (Frame, Direction) {
    wire(
        "OutputAudioRawFrame",
        json!({
            "audio": {"__type__":"bytes", "__data__":base64::engine::general_purpose::STANDARD.encode(encode_pcm16_le(samples))},
            "sample_rate":rate, "num_channels":1, "num_frames":samples.len(), "context_id":context
        }),
        Direction::Downstream,
        destination,
    )
}
fn default_mixer(mixer: Arc<dyn AudioMixer>) -> Mixers {
    BTreeMap::from([(None, mixer)])
}

#[tokio::test]
async fn decoded_controls_change_only_first_real_wav_mixer() {
    let mut fixture = WavFixture::new();
    let first = fixture.mixer("first", 10, 20);
    let second = fixture.mixer("second", 100, 1000);
    let mut session = Session::start(
        [default_mixer(first.clone()), default_mixer(second.clone())],
        vec![],
        Duration::from_secs(1),
    )
    .await;
    let loaded = [
        first.load_statuses().unwrap(),
        second.load_statuses().unwrap(),
    ];
    session.play("before", None).await;
    let (disable, direction) = wire(
        "MixerEnableFrame",
        json!({"enable":false}),
        Direction::Downstream,
        None,
    );
    session.send(disable.clone(), direction).await;
    session.play("disabled", None).await;
    let (settings, direction) = wire(
        "MixerUpdateSettingsFrame",
        json!({"settings":{"sound":"alternate", "volume":2.0}}),
        Direction::Downstream,
        None,
    );
    session.send(settings.clone(), direction).await;
    let (enable, direction) = wire(
        "MixerEnableFrame",
        json!({"enable":true}),
        Direction::Downstream,
        None,
    );
    session.send(enable.clone(), direction).await;
    session.play("updated", None).await;
    let finished = session.finish().await;
    finished.assert_clean();
    fixture.assert_unchanged();
    for statuses in loaded {
        assert!(statuses
            .values()
            .all(|status| *status == SoundLoadStatus::Loaded { samples: CHUNK }));
    }
    assert_eq!(
        finished.samples(0),
        [vec![10; CHUNK], vec![0; CHUNK], vec![40; CHUNK]]
    );
    // Each second-stage sample includes A's already-mixed PCM plus B's own sound.
    assert_eq!(
        finished.samples(1),
        [vec![110; CHUNK], vec![100; CHUNK], vec![140; CHUNK]],
        "first-output controls changed the later real WAV mixer's played PCM"
    );
    finished.assert_consumed(&[disable, settings, enable]);
}

#[tokio::test]
async fn decoded_disable_is_consumed_by_known_output_without_mixer() {
    let mut fixture = WavFixture::new();
    let second = fixture.mixer("second", 100, 1000);
    let mut session = Session::start(
        [BTreeMap::new(), default_mixer(second.clone())],
        vec![],
        Duration::from_secs(1),
    )
    .await;
    let loaded = second.load_statuses().unwrap();
    let (disable, direction) = wire(
        "MixerEnableFrame",
        json!({"enable":false}),
        Direction::Downstream,
        None,
    );
    session.send(disable.clone(), direction).await;
    session.play("unmixed-first", None).await;
    let finished = session.finish().await;
    finished.assert_clean();
    fixture.assert_unchanged();
    assert!(loaded
        .values()
        .all(|status| *status == SoundLoadStatus::Loaded { samples: CHUNK }));
    assert_eq!(finished.samples(0), [vec![0; CHUNK]]);
    assert_eq!(
        finished.samples(1),
        [vec![100; CHUNK]],
        "a known output without a mixer leaked disable into the later real WAV mixer"
    );
    finished.assert_consumed(&[disable]);
}

use pipecat::audio::SincResampler;
use tokio::sync::Semaphore;

enum CallbackAction {
    Pass,
    Fail,
    Pending,
    Gated(Arc<Semaphore>),
}
struct RecordingMixer {
    inner: Arc<SoundfileMixer>,
    action: CallbackAction,
    controls: Mutex<Vec<FrameData>>,
    entered: Semaphore,
    active: AtomicUsize,
    starts: AtomicUsize,
    stops: AtomicUsize,
}
struct ActiveControl<'a>(&'a AtomicUsize);
impl Drop for ActiveControl<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl RecordingMixer {
    fn new(inner: Arc<SoundfileMixer>, action: CallbackAction) -> Arc<Self> {
        Arc::new(Self {
            inner,
            action,
            controls: Mutex::new(Vec::new()),
            entered: Semaphore::new(0),
            active: AtomicUsize::new(0),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
        })
    }
    async fn wait_entered(&self) {
        timeout(WAIT, self.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    fn assert_stopped(&self) {
        assert_eq!(self.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.stops.load(Ordering::SeqCst), 1);
        assert_eq!(self.active.load(Ordering::SeqCst), 0);
    }
}
#[async_trait]
impl AudioMixer for RecordingMixer {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        self.inner.start(format).await
    }
    async fn mix(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        self.inner.mix(audio).await
    }
    async fn control(&self, frame: &FrameData) -> Result<(), ProcessorError> {
        self.controls.lock().unwrap().push(frame.clone());
        self.active.fetch_add(1, Ordering::SeqCst);
        let _active = ActiveControl(&self.active);
        self.entered.add_permits(1);
        match &self.action {
            CallbackAction::Pass => {}
            CallbackAction::Fail => return Err(ProcessorError::new("mixer callback rejected")),
            CallbackAction::Pending => return std::future::pending().await,
            CallbackAction::Gated(gate) => gate.acquire().await.unwrap().forget(),
        }
        self.inner.control(frame).await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        self.inner.stop().await
    }
}
impl Session {
    async fn observe_frame(&mut self, frame: &Frame) {
        let token = frame.observation_token();
        self.observe(|frames| {
            frames
                .iter()
                .any(|frame| frame.frame.observation_token() == token)
        })
        .await;
    }
    async fn collect_terminal(self) -> Finished {
        let (remaining, report) = timeout(WAIT + WAIT, self.worker.collect())
            .await
            .expect("terminal must join all owned workers")
            .unwrap();
        let mut frames = self.observed;
        frames.extend(remaining);
        Finished {
            outputs: self.outputs,
            controls: self.controls,
            frames,
            report,
        }
    }
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
fn family() -> [(&'static str, Value); 3] {
    [
        ("MixerControlFrame", json!({})),
        ("MixerEnableFrame", json!({"enable":false})),
        (
            "MixerUpdateSettingsFrame",
            json!({"settings":{"volume":2.0}}),
        ),
    ]
}

#[tokio::test]
async fn named_family_is_owned_once_and_upstream_envelopes_bypass_mixers() {
    let mut fixture = WavFixture::new();
    let first = RecordingMixer::new(fixture.mixer("first", 10, 20), CallbackAction::Pass);
    let named = RecordingMixer::new(fixture.mixer("named", 20, 40), CallbackAction::Pass);
    let second = RecordingMixer::new(fixture.mixer("second", 100, 1000), CallbackAction::Pass);
    let second_named = RecordingMixer::new(
        fixture.mixer("second-named", 200, 2000),
        CallbackAction::Pass,
    );
    let no_mixer_target =
        RecordingMixer::new(fixture.mixer("later-only", 500, 5000), CallbackAction::Pass);
    let mut a = default_mixer(first.clone());
    a.insert(Some("speaker".into()), named.clone());
    let mut b = default_mixer(second.clone());
    b.insert(Some("speaker".into()), second_named.clone());
    b.insert(Some("no-mixer".into()), no_mixer_target.clone());
    let mut session = Session::start(
        [a, b],
        vec!["speaker".into(), "no-mixer".into()],
        Duration::from_secs(1),
    )
    .await;
    let mut consumed = Vec::new();
    for (class, fields) in family().into_iter().chain(std::iter::once((
        "MixerEnableFrame",
        json!({"enable":true}),
    ))) {
        let (frame, direction) = wire(class, fields, Direction::Downstream, Some("speaker"));
        session.send(frame.clone(), direction).await;
        consumed.push(frame);
    }
    session.play("named", Some("speaker")).await;
    session.play("default-unaffected", None).await;
    for (class, fields) in family() {
        let (frame, direction) = wire(class, fields, Direction::Downstream, Some("no-mixer"));
        session.send(frame.clone(), direction).await;
        consumed.push(frame);
    }
    session.play("known-without-mixer", Some("no-mixer")).await;
    let mut upstream = Vec::new();
    for destination in [None, Some("speaker"), Some("unknown")] {
        for (class, mut fields) in family() {
            fields
                .as_object_mut()
                .unwrap()
                .insert("pts".into(), json!(9001));
            let (frame, direction) = wire(class, fields, Direction::Upstream, destination);
            session.send(frame.clone(), direction).await;
            session.observe_frame(&frame).await;
            upstream.push(frame);
        }
    }
    session.play("after-upstream", Some("speaker")).await;
    let finished = session.finish().await;
    finished.assert_clean();
    fixture.assert_unchanged();
    assert_eq!(
        finished.samples(0),
        [
            vec![40; CHUNK],
            vec![10; CHUNK],
            vec![0; CHUNK],
            vec![40; CHUNK]
        ]
    );
    assert_eq!(
        finished.samples(1),
        [
            vec![240; CHUNK],
            vec![110; CHUNK],
            vec![500; CHUNK],
            vec![240; CHUNK]
        ]
    );
    assert_eq!(
        *named.controls.lock().unwrap(),
        consumed[..4]
            .iter()
            .map(|frame| frame.data.clone())
            .collect::<Vec<_>>()
    );
    for mixer in [&first, &second, &second_named, &no_mixer_target] {
        assert!(mixer.controls.lock().unwrap().is_empty());
    }
    for mixer in [&first, &named, &second, &second_named, &no_mixer_target] {
        mixer.assert_stopped();
    }
    finished.assert_consumed(&consumed);
    for expected in upstream {
        let actual: Vec<_> = finished
            .frames
            .iter()
            .filter(|frame| frame.frame.observation_token() == expected.observation_token())
            .collect();
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].direction, Direction::Upstream);
        assert_same_envelope(&actual[0].frame, &expected);
    }
}

#[tokio::test]
async fn unknown_downstream_routes_keep_explicit_errors_for_the_whole_family() {
    let mut fixture = WavFixture::new();
    let first = RecordingMixer::new(fixture.mixer("first", 10, 20), CallbackAction::Pass);
    let second = RecordingMixer::new(fixture.mixer("second", 100, 1000), CallbackAction::Pass);
    let mut session = Session::start(
        [default_mixer(first.clone()), default_mixer(second.clone())],
        vec![],
        Duration::from_secs(1),
    )
    .await;
    let mut rejected = Vec::new();
    for (class, fields) in family() {
        let (frame, direction) = wire(class, fields, Direction::Downstream, Some("unknown"));
        session.send(frame.clone(), direction).await;
        rejected.push(frame);
    }
    session.play("after-unknown", None).await;
    let finished = session.finish().await;
    finished.assert_lifecycle(WorkerState::Ended);
    fixture.assert_unchanged();
    assert_eq!(finished.report.total_errors, 3);
    assert_eq!(finished.report.errors.len(), 3);
    assert!(finished
        .report
        .errors
        .iter()
        .all(|error| !error.fatal && error.error == "unknown output destination"));
    assert_eq!(finished.samples(0), [vec![10; CHUNK]]);
    assert_eq!(finished.samples(1), [vec![110; CHUNK]]);
    finished.assert_consumed(&rejected);
    for mixer in [&first, &second] {
        assert!(mixer.controls.lock().unwrap().is_empty());
        mixer.assert_stopped();
    }
}

#[tokio::test]
async fn paused_end_drains_resampler_and_partial_pcm_before_owned_control() {
    let mut fixture = WavFixture::new();
    let first = RecordingMixer::new(fixture.mixer("first", 10, 20), CallbackAction::Pass);
    let second = RecordingMixer::new(fixture.mixer("second", 100, 1000), CallbackAction::Pass);
    let session = Session::start(
        [default_mixer(first.clone()), default_mixer(second.clone())],
        vec![],
        Duration::from_secs(1),
    )
    .await;
    session.controls[0].pause(None).unwrap();
    let input: Vec<i16> = (0..97).map(|index| (index * 17) as i16).collect();
    let mut reference = SincResampler::new(16000, RATE, 1).unwrap();
    let emitted = reference.process(&encode_pcm16_le(&input)).unwrap();
    let tail = reference.flush().unwrap();
    let mut expected = decode_pcm16_le(&[emitted.as_slice(), tail.as_slice()].concat()).unwrap();
    let unpadded = expected.len();
    expected.resize(CHUNK, 0);
    let (prefix, direction) = wire_audio("partial-resampled-prefix", &input, 16000, None);
    session.send(prefix, direction).await;
    let (settings, direction) = wire(
        "MixerUpdateSettingsFrame",
        json!({"settings":{"volume":2.0}}),
        Direction::Downstream,
        None,
    );
    session.send(settings.clone(), direction).await;
    let (after, direction) = wire_audio("after-control", &[0; CHUNK], RATE, None);
    session.send(after, direction).await;
    // End owns unpausing and draining; no resume, scheduler sleep or discarded tail.
    let finished = session.finish().await;
    finished.assert_clean();
    fixture.assert_unchanged();
    assert!(!emitted.is_empty() && !tail.is_empty());
    assert!(unpadded < CHUNK);
    assert_eq!(
        finished.samples(0),
        [
            expected
                .iter()
                .map(|sample| sample + 10)
                .collect::<Vec<_>>(),
            vec![20; CHUNK]
        ]
    );
    assert_eq!(
        finished.samples(1),
        [
            expected
                .iter()
                .map(|sample| sample + 110)
                .collect::<Vec<_>>(),
            vec![120; CHUNK]
        ]
    );
    assert_eq!(
        first.controls.lock().unwrap().as_slice(),
        std::slice::from_ref(&settings.data)
    );
    assert!(second.controls.lock().unwrap().is_empty());
    finished.assert_consumed(&[settings]);
    first.assert_stopped();
    second.assert_stopped();
}

#[tokio::test]
async fn failed_and_timed_out_callbacks_never_become_downstream_controls() {
    for (action, expected_error) in [
        (CallbackAction::Fail, "mixer callback rejected"),
        (CallbackAction::Pending, "media backend operation timed out"),
    ] {
        let mut fixture = WavFixture::new();
        let first = RecordingMixer::new(fixture.mixer("first", 10, 20), action);
        let second = RecordingMixer::new(fixture.mixer("second", 100, 1000), CallbackAction::Pass);
        let mut session = Session::start(
            [default_mixer(first.clone()), default_mixer(second.clone())],
            vec![],
            Duration::from_secs(1),
        )
        .await;
        let (settings, direction) = wire(
            "MixerUpdateSettingsFrame",
            json!({"settings":{"volume":2.0}}),
            Direction::Downstream,
            None,
        );
        session.send(settings.clone(), direction).await;
        first.wait_entered().await;
        session
            .observe(|frames| {
                frames
                    .iter()
                    .any(|frame| matches!(frame.frame.data, FrameData::Error(_)))
            })
            .await;
        session.play("after-failed-control", None).await;
        let finished = session.finish().await;
        finished.assert_lifecycle(WorkerState::Ended);
        fixture.assert_unchanged();
        assert_eq!(finished.report.total_errors, 1);
        assert_eq!(finished.report.errors.len(), 1);
        assert_eq!(finished.report.errors[0].error, expected_error);
        assert!(!finished.report.errors[0].fatal);
        assert_eq!(finished.samples(0), [vec![10; CHUNK]]);
        assert_eq!(finished.samples(1), [vec![110; CHUNK]]);
        assert_eq!(
            first.controls.lock().unwrap().as_slice(),
            std::slice::from_ref(&settings.data)
        );
        assert!(second.controls.lock().unwrap().is_empty());
        finished.assert_consumed(&[settings]);
        first.assert_stopped();
        second.assert_stopped();
    }
}

#[tokio::test]
async fn cancel_drops_in_flight_uninterruptible_mixer_callback_and_joins() {
    let mut fixture = WavFixture::new();
    let first = RecordingMixer::new(fixture.mixer("first", 10, 20), CallbackAction::Pending);
    let second = RecordingMixer::new(fixture.mixer("second", 100, 1000), CallbackAction::Pass);
    let session = Session::start(
        [default_mixer(first.clone()), default_mixer(second.clone())],
        vec![],
        Duration::from_secs(30),
    )
    .await;
    let (settings, direction) = wire(
        "MixerUpdateSettingsFrame",
        json!({"settings":{"volume":2.0}}),
        Direction::Downstream,
        None,
    );
    session.send(settings.clone(), direction).await;
    first.wait_entered().await;
    session
        .worker
        .cancel("cancel owned mixer callback")
        .await
        .unwrap();
    let finished = session.collect_terminal().await;
    finished.assert_lifecycle(WorkerState::Cancelled);
    fixture.assert_unchanged();
    assert_eq!(finished.report.total_errors, 0);
    assert!(finished.samples(0).is_empty());
    assert!(finished.samples(1).is_empty());
    assert_eq!(
        first.controls.lock().unwrap().as_slice(),
        std::slice::from_ref(&settings.data)
    );
    assert!(second.controls.lock().unwrap().is_empty());
    finished.assert_consumed(&[settings]);
    first.assert_stopped();
    second.assert_stopped();
}

#[tokio::test]
async fn interruption_drops_interruptible_control_and_later_pcm_still_plays() {
    let mut fixture = WavFixture::new();
    let first = RecordingMixer::new(fixture.mixer("first", 10, 20), CallbackAction::Pending);
    let second = RecordingMixer::new(fixture.mixer("second", 100, 1000), CallbackAction::Pass);
    let mut session = Session::start(
        [default_mixer(first.clone()), default_mixer(second.clone())],
        vec![],
        Duration::from_secs(30),
    )
    .await;
    let (settings, direction) = wire(
        "MixerUpdateSettingsFrame",
        json!({"settings":{"volume":2.0}, "interruptible":true}),
        Direction::Downstream,
        None,
    );
    session.send(settings.clone(), direction).await;
    first.wait_entered().await;
    let interruption = Frame::new(FrameData::Interruption);
    session
        .send(interruption.clone(), Direction::Downstream)
        .await;
    session.observe_frame(&interruption).await;
    session.play("after-interruption", None).await;
    let finished = session.finish().await;
    finished.assert_clean();
    fixture.assert_unchanged();
    assert_eq!(finished.samples(0), [vec![10; CHUNK]]);
    assert_eq!(finished.samples(1), [vec![110; CHUNK]]);
    assert_eq!(
        first.controls.lock().unwrap().as_slice(),
        std::slice::from_ref(&settings.data)
    );
    assert!(second.controls.lock().unwrap().is_empty());
    finished.assert_consumed(&[settings]);
    first.assert_stopped();
    second.assert_stopped();
}

#[tokio::test]
async fn normal_end_waits_for_admitted_callback_and_consumes_it_once() {
    let mut fixture = WavFixture::new();
    let release = Arc::new(Semaphore::new(0));
    let first = RecordingMixer::new(
        fixture.mixer("first", 10, 20),
        CallbackAction::Gated(release.clone()),
    );
    let second = RecordingMixer::new(fixture.mixer("second", 100, 1000), CallbackAction::Pass);
    let mut session = Session::start(
        [default_mixer(first.clone()), default_mixer(second.clone())],
        vec![],
        Duration::from_secs(1),
    )
    .await;
    let (settings, direction) = wire(
        "MixerUpdateSettingsFrame",
        json!({"settings":{"volume":2.0}}),
        Direction::Downstream,
        None,
    );
    session.send(settings.clone(), direction).await;
    first.wait_entered().await;
    let (audio, direction) = wire_audio("after-gated-control", &[0; CHUNK], RATE, None);
    session.send(audio, direction).await;
    session.worker.end().await.unwrap();
    // Normal End is behind the control in the media worker. Releasing the owned
    // callback permits its queued PCM to drain through both public outputs.
    release.add_permits(1);
    session.observe(|frames| frames.iter().any(|frame| matches!(&frame.frame.data, FrameData::OutputAudio(audio) if audio.context_id.as_deref() == Some("after-gated-control")))).await;
    let finished = session.collect_terminal().await;
    finished.assert_clean();
    fixture.assert_unchanged();
    assert_eq!(finished.samples(0), [vec![20; CHUNK]]);
    assert_eq!(finished.samples(1), [vec![120; CHUNK]]);
    assert_eq!(
        first.controls.lock().unwrap().as_slice(),
        std::slice::from_ref(&settings.data)
    );
    assert!(second.controls.lock().unwrap().is_empty());
    finished.assert_consumed(&[settings]);
    first.assert_stopped();
    second.assert_stopped();
}
