use async_trait::async_trait;
use pipecat_audio::{decode_pcm16_le, encode_pcm16_le, scale_audio, PcmFormat};
use pipecat_core::*;
use pipecat_media_transport::*;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

struct Sink {
    written: Mutex<Vec<Frame>>,
    calls: AtomicUsize,
    interrupts: AtomicUsize,
    stopped: AtomicUsize,
    entered: Notify,
    gate: Option<Semaphore>,
    active: Arc<AtomicUsize>,
    discard: bool,
}
impl Sink {
    fn new(block: bool) -> Self {
        Self {
            written: Mutex::new(vec![]),
            calls: AtomicUsize::new(0),
            interrupts: AtomicUsize::new(0),
            stopped: AtomicUsize::new(0),
            entered: Notify::new(),
            gate: block.then(|| Semaphore::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            discard: false,
        }
    }
    async fn wait_calls(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.calls.load(Ordering::SeqCst) < count {
                let wait = self.entered.notified();
                if self.calls.load(Ordering::SeqCst) < count {
                    wait.await;
                }
            }
        })
        .await
        .unwrap();
    }
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl OutputBackend for Sink {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.active.fetch_add(1, Ordering::SeqCst);
        let _guard = Active(self.active.clone());
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_waiters();
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        if self.discard {
            return Ok(PlaybackReceipt::Discarded);
        }
        self.written.lock().unwrap().push(frame.clone());
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        self.interrupts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn send_message(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.play_audio(frame).await.map(|_| ())
    }
    async fn write_image(&self, frame: &Frame) -> Result<(), ProcessorError> {
        self.play_audio(frame).await.map(|_| ())
    }
}
struct Filter {
    factor: Mutex<f32>,
    empty: bool,
    log: Arc<Mutex<Vec<&'static str>>>,
    stopped: AtomicUsize,
}
#[async_trait]
impl AudioFilter for Filter {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn filter(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        self.log.lock().unwrap().push("filter");
        if self.empty {
            Ok(vec![])
        } else {
            scale_audio(&audio, *self.factor.lock().unwrap())
                .map_err(|e| ProcessorError::new(e.to_string()))
        }
    }
    async fn control(&self, frame: &FrameData) -> Result<(), ProcessorError> {
        self.log.lock().unwrap().push("control");
        if matches!(frame, FrameData::FilterEnable(false)) {
            *self.factor.lock().unwrap() = 1.0;
        }
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Mixer {
    log: Arc<Mutex<Vec<&'static str>>>,
}
#[async_trait]
impl AudioMixer for Mixer {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn mix(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        self.log.lock().unwrap().push("mixer");
        Ok(encode_pcm16_le(
            &decode_pcm16_le(&audio)
                .unwrap()
                .into_iter()
                .map(|sample| sample.saturating_add(100))
                .collect::<Vec<_>>(),
        ))
    }
    async fn control(&self, _: &FrameData) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
fn config() -> OutputTransportConfig {
    OutputTransportConfig {
        sample_rate: Some(8000),
        chunk_duration: Duration::from_millis(10),
        auto_silence: false,
        end_silence: Duration::ZERO,
        write_timeout: Duration::from_millis(100),
        ..Default::default()
    }
}
fn pcm(value: i16, samples: usize, interruptible: bool) -> Frame {
    Frame::new(FrameData::TtsAudio(
        AudioRawFrame::new(encode_pcm16_le(&vec![value; samples]), 8000, 1).unwrap(),
    ))
    .with_interruptible(interruptible)
}
fn stop() -> Frame {
    Frame::new(FrameData::TtsStopped(TtsStoppedFrame { context_id: None }))
}
async fn start(output: Arc<OutputTransport>) -> PipelineWorker {
    let control = output.control();
    let worker = Pipeline::new(vec![output])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !control.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    worker
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn finish(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap()
}
fn written_samples(sink: &Sink) -> Vec<i16> {
    sink.written
        .lock()
        .unwrap()
        .iter()
        .flat_map(|frame| match &frame.data {
            FrameData::OutputAudio(a)
            | FrameData::TtsAudio(a)
            | FrameData::SpeechOutputAudio(a) => decode_pcm16_le(&a.audio).unwrap(),
            _ => panic!(),
        })
        .collect()
}

#[tokio::test]
async fn filter_gain_mixer_and_controls_are_ordered_before_real_playback() {
    let sink = Arc::new(Sink::new(false));
    let log = Arc::new(Mutex::new(vec![]));
    let filter = Arc::new(Filter {
        factor: Mutex::new(2.0),
        empty: false,
        log: log.clone(),
        stopped: AtomicUsize::new(0),
    });
    let mixer: Arc<dyn AudioMixer> = Arc::new(Mixer { log: log.clone() });
    let output = Arc::new(
        OutputTransport::new(
            "out",
            config(),
            sink.clone(),
            Some(filter.clone()),
            BTreeMap::from([(None, mixer)]),
        )
        .unwrap(),
    );
    let worker = start(output).await;
    send(&worker, Frame::new(FrameData::Volume(0.5))).await;
    send(&worker, pcm(1000, 80, true)).await;
    send(&worker, Frame::new(FrameData::FilterEnable(false))).await;
    send(&worker, Frame::new(FrameData::VolumeGain(2.0))).await;
    send(&worker, pcm(1000, 80, true)).await;
    send(&worker, stop()).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(written_samples(&sink), vec![1100; 160]);
    assert_eq!(
        *log.lock().unwrap(),
        ["filter", "mixer", "control", "filter", "mixer"]
    );
    assert_eq!(filter.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(
        frames
            .iter()
            .filter(|f| f.direction == Direction::Downstream
                && matches!(f.frame.data, FrameData::BotStartedSpeaking))
            .count(),
        1
    );
}
#[tokio::test]
async fn short_tts_tail_pads_and_end_drains_before_terminal() {
    let sink = Arc::new(Sink::new(false));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(&worker, pcm(1234, 17, true)).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    let samples = written_samples(&sink);
    assert_eq!(&samples[..17], &[1234; 17]);
    assert_eq!(&samples[17..], &[0; 63]);
    let down: Vec<_> = frames
        .iter()
        .filter(|f| f.direction == Direction::Downstream)
        .map(|f| &f.frame.data)
        .collect();
    assert!(
        down.iter()
            .position(|d| matches!(d, FrameData::TtsAudio(_)))
            .unwrap()
            < down
                .iter()
                .position(|d| matches!(d, FrameData::End(_)))
                .unwrap()
    );
    assert!(down
        .iter()
        .any(|d| matches!(d, FrameData::BotStartedSpeaking)));
    assert!(down
        .iter()
        .any(|d| matches!(d, FrameData::BotStoppedSpeaking)));
}
#[tokio::test]
async fn partial_tail_keeps_uninterruptibility() {
    let sink = Arc::new(Sink::new(false));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(&worker, pcm(1, 40, true)).await;
    send(&worker, pcm(2, 40, false)).await;
    send(&worker, stop()).await;
    finish(worker).await;
    let frames = sink.written.lock().unwrap();
    assert_eq!(frames.len(), 1);
    assert!(!frames[0].interruptible);
}
#[tokio::test]
async fn paused_output_preserves_every_sample_and_resampler_tail() {
    let sink = Arc::new(Sink::new(false));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let control = output.control();
    let worker = start(output).await;
    control.pause(None).unwrap();
    let frame = Frame::new(FrameData::TtsAudio(
        AudioRawFrame::new(encode_pcm16_le(&vec![3000; 1000]), 16000, 1).unwrap(),
    ));
    send(&worker, frame).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(sink.calls.load(Ordering::SeqCst), 0);
    control.resume(None).unwrap();
    send(&worker, stop()).await;
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    let samples = written_samples(&sink);
    assert_eq!(samples.len(), 560);
    assert!(samples[..500].iter().all(|v| *v == 3000));
    assert!(samples[500..].iter().all(|v| *v == 0));
}
#[tokio::test]
async fn delivery_gap_does_not_reset_resampler() {
    let sink = Arc::new(Sink::new(false));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    for _ in 0..2 {
        send(
            &worker,
            Frame::new(FrameData::TtsAudio(
                AudioRawFrame::new(encode_pcm16_le(&vec![3000; 1000]), 16000, 1).unwrap(),
            )),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    send(&worker, stop()).await;
    finish(worker).await;
    let samples = written_samples(&sink);
    assert_eq!(samples.len(), 1040);
    assert!(samples[..1000].iter().all(|v| *v == 3000));
}
#[tokio::test]
async fn interrupted_inflight_and_queued_audio_never_replays() {
    let sink = Arc::new(Sink::new(true));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(&worker, pcm(8000, 800, true)).await;
    sink.wait_calls(1).await;
    send(&worker, Frame::interruption()).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.interrupts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    sink.gate.as_ref().unwrap().add_permits(20);
    send(&worker, pcm(-8000, 80, true)).await;
    send(&worker, stop()).await;
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(written_samples(&sink), vec![-8000; 80]);
    assert_eq!(sink.active.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn interruption_retains_uninterruptible_inflight_and_gain() {
    let sink = Arc::new(Sink::new(true));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(&worker, pcm(1000, 80, false)).await;
    sink.wait_calls(1).await;
    send(&worker, Frame::new(FrameData::VolumeGain(2.0))).await;
    send(&worker, pcm(9000, 80, true)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    send(&worker, Frame::interruption()).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    sink.gate.as_ref().unwrap().add_permits(20);
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.interrupts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send(&worker, pcm(1000, 80, true)).await;
    send(&worker, stop()).await;
    finish(worker).await;
    let samples = written_samples(&sink);
    assert_eq!(&samples[..80], &[1000; 80]);
    assert_eq!(&samples[80..], &[2000; 80]);
}
#[tokio::test]
async fn timeout_writes_off_peer_once_and_end_still_arrives() {
    let sink = Arc::new(Sink::new(true));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(&worker, pcm(1000, 800, true)).await;
    let (frames, report) = finish(worker).await;
    assert_eq!(report.errors.len(), 1);
    assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
    assert_eq!(sink.active.load(Ordering::SeqCst), 0);
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::End(_))));
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::BotStartedSpeaking)));
}
#[tokio::test]
async fn discarded_backend_audio_never_announces_speaking() {
    let sink = Arc::new(Sink {
        discard: true,
        ..Sink::new(false)
    });
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(&worker, pcm(1, 80, true)).await;
    send(&worker, stop()).await;
    let (frames, _) = finish(worker).await;
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::BotStartedSpeaking | FrameData::TtsAudio(_)
    )));
}
#[tokio::test]
async fn cancellation_drops_owned_stalled_write_and_stops_backend() {
    let sink = Arc::new(Sink::new(true));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(&worker, pcm(1000, 80, true)).await;
    sink.wait_calls(1).await;
    worker.cancel("test").await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(sink.active.load(Ordering::SeqCst), 0);
    assert_eq!(sink.stopped.load(Ordering::SeqCst), 1);
    assert!(sink.written.lock().unwrap().is_empty());
}
#[tokio::test]
async fn filter_empty_skips_write_and_mixer() {
    let sink = Arc::new(Sink::new(false));
    let log = Arc::new(Mutex::new(vec![]));
    let filter = Arc::new(Filter {
        factor: Mutex::new(1.0),
        empty: true,
        log: log.clone(),
        stopped: AtomicUsize::new(0),
    });
    let mixer: Arc<dyn AudioMixer> = Arc::new(Mixer { log: log.clone() });
    let output = Arc::new(
        OutputTransport::new(
            "out",
            config(),
            sink.clone(),
            Some(filter),
            BTreeMap::from([(None, mixer)]),
        )
        .unwrap(),
    );
    let worker = start(output).await;
    send(&worker, pcm(1, 80, true)).await;
    finish(worker).await;
    assert_eq!(*log.lock().unwrap(), ["filter"]);
    assert_eq!(sink.calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn destination_isolation_routes_audio_and_reports_unknown_destination() {
    let sink = Arc::new(Sink::new(false));
    let mut cfg = config();
    cfg.destinations = vec!["hold".into()];
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let control = output.control();
    let worker = start(output).await;
    control.pause(Some("hold")).unwrap();
    let mut held = pcm(10, 80, true);
    held.transport_destination = Some("hold".into());
    send(&worker, held).await;
    send(&worker, pcm(20, 80, true)).await;
    sink.wait_calls(1).await;
    assert!(sink.written.lock().unwrap()[0]
        .transport_destination
        .is_none());
    let mut unknown = pcm(30, 80, true);
    unknown.transport_destination = Some("missing".into());
    send(&worker, unknown).await;
    control.resume(Some("hold")).unwrap();
    let (_, report) = finish(worker).await;
    assert_eq!(sink.written.lock().unwrap().len(), 2);
    assert_eq!(report.errors.len(), 1);
}

struct Source {
    captures: AtomicUsize,
    stopped: AtomicUsize,
    active: Arc<AtomicUsize>,
}
#[async_trait]
impl InputBackend for Source {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn capture(&self, _: InputIngress) -> Result<(), ProcessorError> {
        self.captures.fetch_add(1, Ordering::SeqCst);
        self.active.fetch_add(1, Ordering::SeqCst);
        let _guard = Active(self.active.clone());
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn input_injection_and_capture_share_filter_and_owned_shutdown() {
    let source = Arc::new(Source {
        captures: AtomicUsize::new(0),
        stopped: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
    });
    let filter = Arc::new(Filter {
        factor: Mutex::new(2.0),
        empty: false,
        log: Arc::new(Mutex::new(vec![])),
        stopped: AtomicUsize::new(0),
    });
    let input = Arc::new(
        InputTransport::new(
            "in",
            InputTransportConfig {
                sample_rate: Some(8000),
                stream_on_start: false,
                ..Default::default()
            },
            source.clone(),
            Some(filter),
        )
        .unwrap(),
    );
    let ingress = input.ingress();
    let worker = Pipeline::new(vec![input])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    while !ingress.is_ready() {
        tokio::task::yield_now().await;
    }
    send(
        &worker,
        Frame::new(FrameData::InputTransportStartAudioStreaming),
    )
    .await;
    let frame = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(encode_pcm16_le(&[1000; 80]), 8000, 1).unwrap(),
    ));
    ingress.push(frame.clone()).unwrap();
    send(&worker, frame).await;
    while source.captures.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::InputAudio(a) => Some(a),
            _ => None,
        })
        .collect();
    assert_eq!(audio.len(), 2);
    for a in audio {
        assert_eq!(decode_pcm16_le(&a.audio).unwrap(), vec![2000; 80]);
    }
    assert_eq!(source.active.load(Ordering::SeqCst), 0);
    assert_eq!(source.stopped.load(Ordering::SeqCst), 1);
    assert!(!ingress.is_ready());
}

#[tokio::test]
async fn idle_mixer_continues_through_interruption_without_speech_or_filtering() {
    let sink = Arc::new(Sink::new(false));
    let log = Arc::new(Mutex::new(vec![]));
    let filter = Arc::new(Filter {
        factor: Mutex::new(2.0),
        empty: false,
        log: log.clone(),
        stopped: AtomicUsize::new(0),
    });
    let mixer: Arc<dyn AudioMixer> = Arc::new(Mixer { log: log.clone() });
    let mut cfg = config();
    cfg.auto_silence = true;
    let output = Arc::new(
        OutputTransport::new(
            "out",
            cfg,
            sink.clone(),
            Some(filter),
            BTreeMap::from([(None, mixer)]),
        )
        .unwrap(),
    );
    let worker = start(output).await;
    sink.wait_calls(1).await;
    send(&worker, Frame::interruption()).await;
    sink.wait_calls(3).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert!(log.lock().unwrap().iter().all(|event| *event == "mixer"));
    assert!(written_samples(&sink).iter().all(|sample| *sample == 100));
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::BotStartedSpeaking)));
    assert!(sink.interrupts.load(Ordering::SeqCst) >= 1);
}
#[tokio::test]
async fn bounded_paused_queue_rejects_overload_and_end_drains_admitted_frames() {
    let sink = Arc::new(Sink::new(false));
    let mut cfg = config();
    cfg.queue_capacity = 2;
    cfg.admission_policy = OutputAdmissionPolicy::FailFast;
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let control = output.control();
    let worker = start(output).await;
    control.pause(None).unwrap();
    for _ in 0..5 {
        send(&worker, pcm(5, 80, true)).await;
    }
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (_, report) = finish(worker).await;
    assert_eq!(sink.written.lock().unwrap().len(), 2);
    assert_eq!(report.errors.len(), 3);
}
#[tokio::test]
async fn no_speaking_before_playback_ack_and_slow_valid_write_is_not_cut_short() {
    let sink = Arc::new(Sink::new(true));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let mut worker = start(output).await;
    send(&worker, pcm(20, 80, true)).await;
    sink.wait_calls(1).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    while let Ok(frame) = worker.try_recv() {
        assert!(!matches!(
            frame.frame.data,
            FrameData::BotStartedSpeaking | FrameData::TtsAudio(_)
        ));
    }
    sink.gate.as_ref().unwrap().add_permits(1);
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::BotStartedSpeaking)));
    assert_eq!(sink.written.lock().unwrap().len(), 1);
}
#[tokio::test]
async fn timed_frames_are_not_emitted_early_or_after_interruption() {
    let sink = Arc::new(Sink::new(false));
    let output =
        Arc::new(OutputTransport::new("out", config(), sink, None, BTreeMap::new()).unwrap());
    let mut worker = start(output).await;
    let mut timed = Frame::text("obsolete");
    timed.pts = Some(1_000_000_000);
    send(&worker, timed).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    while let Ok(frame) = worker.try_recv() {
        assert!(!matches!(frame.frame.data, FrameData::Text(_)));
    }
    send(&worker, Frame::interruption()).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::Text(_))));
}
#[tokio::test]
async fn bundled_dtmf_bypasses_speech_filter_gain_and_speaking_boundaries() {
    let sink = Arc::new(Sink::new(false));
    let log = Arc::new(Mutex::new(vec![]));
    let filter = Arc::new(Filter {
        factor: Mutex::new(0.0),
        empty: false,
        log: log.clone(),
        stopped: AtomicUsize::new(0),
    });
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), Some(filter), BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(&worker, Frame::new(FrameData::Volume(0.0))).await;
    send(&worker, Frame::new(FrameData::OutputDtmf(KeypadEntry::One))).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert_eq!(
        written_samples(&sink),
        decode_pcm16_le(
            &pipecat_audio::load_dtmf_audio(pipecat_audio::KeypadEntry::One, 8000).unwrap()
        )
        .unwrap()
    );
    assert!(log.lock().unwrap().is_empty());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::BotStartedSpeaking)));
}
#[tokio::test]
async fn failed_start_never_reports_ready_and_cleanup_stops_backend() {
    struct Failed(Arc<AtomicUsize>);
    #[async_trait]
    impl OutputBackend for Failed {
        async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
            Err(ProcessorError::new("no device"))
        }
        async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
            panic!("unopened device")
        }
        async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), ProcessorError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let stopped = Arc::new(AtomicUsize::new(0));
    let output = Arc::new(
        OutputTransport::new(
            "out",
            config(),
            Arc::new(Failed(stopped.clone())),
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let control = output.control();
    assert!(Pipeline::new(vec![output])
        .start(PipelineConfig::default())
        .await
        .is_err());
    tokio::time::timeout(Duration::from_secs(1), async {
        while stopped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!control.is_ready());
}

#[tokio::test]
async fn chunking_keeps_speech_class_and_context_boundaries() {
    let sink = Arc::new(Sink::new(false));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(
        &worker,
        Frame::new(FrameData::OutputAudio(
            AudioRawFrame::new(encode_pcm16_le(&[50; 40]), 8000, 1).unwrap(),
        )),
    )
    .await;
    let mut tts = pcm(100, 40, true);
    if let FrameData::TtsAudio(a) = &mut tts.data {
        a.context_id = Some("first".into());
    }
    send(&worker, tts).await;
    let mut tts = pcm(200, 40, true);
    if let FrameData::TtsAudio(a) = &mut tts.data {
        a.context_id = Some("second".into());
    }
    send(&worker, tts).await;
    send(&worker, stop()).await;
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    let written = sink.written.lock().unwrap();
    assert_eq!(written.len(), 3);
    assert!(matches!(written[0].data, FrameData::OutputAudio(_)));
    assert!(
        matches!(&written[1].data, FrameData::TtsAudio(a) if a.context_id.as_deref() == Some("first"))
    );
    assert!(
        matches!(&written[2].data, FrameData::TtsAudio(a) if a.context_id.as_deref() == Some("second"))
    );
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::BotStartedSpeaking)));
}

#[tokio::test]
async fn input_streaming_resample_matches_unsplit_audio_and_flushes_tail() {
    let source = Arc::new(Source {
        captures: AtomicUsize::new(0),
        stopped: AtomicUsize::new(0),
        active: Arc::new(AtomicUsize::new(0)),
    });
    let input = Arc::new(
        InputTransport::new(
            "in",
            InputTransportConfig {
                sample_rate: Some(8000),
                stream_on_start: false,
                ..Default::default()
            },
            source,
            None,
        )
        .unwrap(),
    );
    let ingress = input.ingress();
    let worker = Pipeline::new(vec![input])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    while !ingress.is_ready() {
        tokio::task::yield_now().await;
    }
    let bytes = encode_pcm16_le(&(0..501).map(|x| x * 40 - 10000).collect::<Vec<i16>>());
    for data in bytes.chunks(66) {
        ingress
            .push(Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(data.to_vec(), 16000, 1).unwrap(),
            )))
            .unwrap();
    }
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    let actual: Vec<u8> = frames
        .into_iter()
        .flat_map(|f| {
            if let FrameData::InputAudio(a) = f.frame.data {
                a.audio
            } else {
                vec![]
            }
        })
        .collect();
    assert_eq!(
        actual,
        pipecat_audio::resample_pcm16(&bytes, 16000, 8000, 1).unwrap()
    );
}

#[tokio::test]
async fn interruption_cancels_stalled_image_and_message_without_stale_forwarding() {
    for frame in [
        Frame::new(FrameData::OutputImage(ImageRawFrame {
            image: vec![0; 3],
            size: (1, 1),
            format: Some("RGB".into()),
        })),
        Frame::new(FrameData::OutputTransportMessage(
            serde_json::json!({"obsolete": true}),
        )),
    ] {
        let sink = Arc::new(Sink::new(true));
        let output = Arc::new(
            OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
        );
        let worker = start(output).await;
        send(&worker, frame).await;
        sink.wait_calls(1).await;
        send(&worker, Frame::interruption()).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.interrupts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        sink.gate.as_ref().unwrap().add_permits(1);
        send(&worker, pcm(1, 80, true)).await;
        let (frames, report) = finish(worker).await;
        assert!(report.errors.is_empty());
        assert_eq!(sink.written.lock().unwrap().len(), 1);
        assert!(!frames.iter().any(|f| matches!(
            f.frame.data,
            FrameData::OutputImage(_) | FrameData::OutputTransportMessage(_)
        )));
    }
}

#[tokio::test]
async fn interruption_during_class_boundary_cannot_seed_new_resampler_with_old_audio() {
    let sink = Arc::new(Sink::new(true));
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let worker = start(output).await;
    send(
        &worker,
        Frame::new(FrameData::OutputAudio(
            AudioRawFrame::new(encode_pcm16_le(&[6000; 40]), 16000, 1).unwrap(),
        )),
    )
    .await;
    send(
        &worker,
        Frame::new(FrameData::TtsAudio(
            AudioRawFrame::new(encode_pcm16_le(&[8000; 100]), 16000, 1).unwrap(),
        )),
    )
    .await;
    sink.wait_calls(1).await;
    send(&worker, Frame::interruption()).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.interrupts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    sink.gate.as_ref().unwrap().add_permits(20);
    send(
        &worker,
        Frame::new(FrameData::TtsAudio(
            AudioRawFrame::new(encode_pcm16_le(&[-8000; 320]), 16000, 1).unwrap(),
        )),
    )
    .await;
    send(&worker, stop()).await;
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert_eq!(written_samples(&sink), vec![-8000; 160]);
}

fn flow_flush(number: u64) -> Frame {
    let mut frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        origin: Some("pipecat.flows".into()),
        returning: false,
        event: Some(LocalFlushEvent::default()),
    }));
    frame
        .metadata
        .insert("pipecat.flows.barrier".into(), number.into());
    frame
        .metadata
        .insert("retained".into(), serde_json::json!({"nested": [1, 2, 3]}));
    frame
}
fn flow_flush_event(frame: &Frame) -> LocalFlushEvent {
    let FrameData::FlushDetailed(flush) = &frame.data else {
        panic!("expected a flow flush marker")
    };
    flush.event.clone().unwrap()
}
async fn receive_flow_flush(worker: &mut PipelineWorker) -> Frame {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker.recv().await.expect("pipeline closed before flush receipt").frame;
            if matches!(&frame.data, FrameData::FlushDetailed(flush) if flush.origin.as_deref() == Some("pipecat.flows") && flush.returning) {
                assert!(flow_flush_event(&frame).is_set());
                return frame;
            }
        }
    }).await.unwrap()
}
fn assert_no_flush(worker: &mut PipelineWorker) {
    while let Ok(frame) = worker.try_recv() {
        assert!(
            !matches!(frame.frame.data, FrameData::FlushDetailed(_)),
            "premature flush receipt"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flow_flush_waits_for_all_destinations_and_padded_tails_preserving_marker() {
    let sink = Arc::new(Sink::new(true));
    let mut cfg = config();
    cfg.destinations = vec!["other".into()];
    cfg.write_timeout = Duration::from_secs(2);
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let mut worker = start(output).await;
    send(&worker, pcm(10, 13, true)).await;
    let mut other = pcm(20, 17, true);
    other.transport_destination = Some("other".into());
    send(&worker, other).await;
    let marker = flow_flush(101);
    let event = flow_flush_event(&marker);
    send(&worker, marker.clone()).await;
    sink.wait_calls(2).await;
    assert_no_flush(&mut worker);
    assert!(!event.is_set());
    sink.gate.as_ref().unwrap().add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.written.lock().unwrap().len() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_no_flush(&mut worker);
    assert!(!event.is_set());
    sink.gate.as_ref().unwrap().add_permits(1);
    let returned = receive_flow_flush(&mut worker).await;
    assert_eq!(returned.id, marker.id);
    assert_eq!(returned.metadata, marker.metadata);
    assert_eq!(returned.interruptible, marker.interruptible);
    if let FrameData::FlushDetailed(flush) = &returned.data {
        assert!(flush.event.as_ref().unwrap().is_set());
    }
    assert_eq!(sink.written.lock().unwrap().len(), 2);
    for frame in sink.written.lock().unwrap().iter() {
        let FrameData::TtsAudio(audio) = &frame.data else {
            panic!()
        };
        let samples = decode_pcm16_le(&audio.audio).unwrap();
        assert_eq!(samples.len(), 80);
        let n = if frame.transport_destination.is_none() {
            13
        } else {
            17
        };
        assert!(samples[n..].iter().all(|v| *v == 0));
    }
    let (frames, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert!(
        !frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::FlushDetailed(_))),
        "duplicate flush receipt"
    );
}

#[tokio::test]
async fn flow_flush_excludes_later_audio_even_when_its_backend_write_stalls() {
    let sink = Arc::new(Sink::new(true));
    let mut cfg = config();
    cfg.write_timeout = Duration::from_secs(2);
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let control = output.control();
    let mut worker = Pipeline::new(vec![output])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    while !control.is_ready() {
        tokio::task::yield_now().await;
    }
    send(&worker, pcm(10, 80, true)).await;
    sink.wait_calls(1).await;
    let marker = flow_flush(102);
    let event = flow_flush_event(&marker);
    send(&worker, marker).await;
    send(&worker, pcm(20, 80, true)).await;
    assert_no_flush(&mut worker);
    assert!(!event.is_set());
    sink.gate.as_ref().unwrap().add_permits(1);
    sink.wait_calls(2).await;
    let returned = receive_flow_flush(&mut worker).await;
    assert_eq!(returned.metadata["pipecat.flows.barrier"], 102);
    assert_eq!(sink.written.lock().unwrap().len(), 1);
    sink.gate.as_ref().unwrap().add_permits(1);
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flow_flush_cancel_releases_rendezvous_with_a_paused_destination() {
    let sink = Arc::new(Sink::new(false));
    let mut cfg = config();
    cfg.destinations = vec!["paused".into()];
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let control = output.control();
    let mut worker = start(output).await;
    control.pause(Some("paused")).unwrap();
    send(&worker, pcm(10, 80, true)).await;
    sink.wait_calls(1).await;
    let marker = flow_flush(103);
    let event = flow_flush_event(&marker);
    send(&worker, marker).await;
    send(
        &worker,
        Frame::new(FrameData::ProcessorPauseUrgent {
            processor: "unrelated".into(),
        }),
    )
    .await;
    // A system frame must traverse while the destination workers rendezvous.
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let frame = worker.recv().await.unwrap();
            if matches!(frame.frame.data, FrameData::ProcessorPauseUrgent { .. }) {
                break;
            }
            assert!(!matches!(frame.frame.data, FrameData::FlushDetailed(_)));
        }
    })
    .await
    .unwrap();
    assert!(!event.is_set());
    worker.cancel("cancel pending flush").await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(1), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(sink.active.load(Ordering::SeqCst), 0);
    assert!(!event.is_set());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::FlushDetailed(_))));
}

#[tokio::test]
async fn flow_flush_paused_output_retains_fence_through_interruption_then_resumes() {
    let sink = Arc::new(Sink::new(false));
    let mut cfg = config();
    cfg.destinations = vec!["paused".into()];
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let control = output.control();
    let mut worker = start(output).await;
    control.pause(Some("paused")).unwrap();
    let mut audio = pcm(50, 80, true);
    audio.transport_destination = Some("paused".into());
    send(&worker, audio).await;
    let marker = flow_flush(104);
    let event = flow_flush_event(&marker);
    send(&worker, marker).await;
    send(&worker, Frame::interruption()).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let frame = worker.recv().await.unwrap();
            if matches!(frame.frame.data, FrameData::Interruption) {
                break;
            }
            assert!(!matches!(frame.frame.data, FrameData::FlushDetailed(_)));
        }
    })
    .await
    .unwrap();
    assert_no_flush(&mut worker);
    assert!(!event.is_set());
    control.resume(Some("paused")).unwrap();
    receive_flow_flush(&mut worker).await;
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert!(sink.written.lock().unwrap().is_empty());
}

#[tokio::test]
async fn flow_flush_stalled_backend_fails_without_success_receipt() {
    let sink = Arc::new(Sink::new(true));
    let mut cfg = config();
    cfg.write_timeout = Duration::from_millis(20);
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let mut worker = start(output).await;
    send(&worker, pcm(1, 80, true)).await;
    let marker = flow_flush(105);
    let event = flow_flush_event(&marker);
    send(&worker, marker).await;
    let mut seen = vec![];
    tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(frame) = worker.recv().await {
            seen.push(frame);
        }
    })
    .await
    .unwrap();
    assert!(worker.join().await.is_err());
    assert!(!event.is_set());
    assert!(!seen
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::FlushDetailed(_))));
    assert!(worker
        .errors()
        .iter()
        .any(|e| e.fatal && e.error.contains("flow barrier Some(105)")));
    assert_eq!(sink.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn flow_flush_accepts_explicit_discard_receipts() {
    let sink = Arc::new(Sink {
        discard: true,
        ..Sink::new(false)
    });
    let output = Arc::new(
        OutputTransport::new("out", config(), sink.clone(), None, BTreeMap::new()).unwrap(),
    );
    let mut worker = start(output).await;
    send(&worker, pcm(1, 17, true)).await;
    send(&worker, flow_flush(106)).await;
    receive_flow_flush(&mut worker).await;
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty());
    assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
    assert!(sink.written.lock().unwrap().is_empty());
}

#[tokio::test]
async fn flow_flush_full_destination_fails_boundedly_without_partial_completion() {
    let sink = Arc::new(Sink::new(false));
    let mut cfg = config();
    cfg.destinations = vec!["full".into()];
    cfg.queue_capacity = 1;
    cfg.admission_policy = OutputAdmissionPolicy::Wait {
        timeout: Duration::from_millis(20),
    };
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let control = output.control();
    let mut worker = start(output).await;
    control.pause(Some("full")).unwrap();
    let mut audio = pcm(1, 80, true);
    audio.transport_destination = Some("full".into());
    send(&worker, audio).await;
    let marker = flow_flush(107);
    let event = flow_flush_event(&marker);
    send(&worker, marker).await;
    let mut frames = vec![];
    tokio::time::timeout(Duration::from_secs(1), async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
    })
    .await
    .unwrap();
    assert!(worker.join().await.is_err());
    assert!(!event.is_set());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::FlushDetailed(_))));
    assert!(worker
        .errors()
        .iter()
        .any(|e| e.fatal && e.error.contains("flow barrier Some(107)")));
    assert_eq!(sink.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn bounded_wait_preserves_pcm_and_flush_receipt_order() {
    for detailed in [false, true] {
        let sink = Arc::new(Sink::new(true));
        let mut cfg = config();
        cfg.queue_capacity = 1;
        cfg.queue_byte_capacity = 160;
        cfg.write_timeout = Duration::from_secs(2);
        let output = Arc::new(
            OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap(),
        );
        let mut worker = start(output).await;
        send(&worker, pcm(1, 80, true)).await;
        sink.wait_calls(1).await;
        for value in 2..=4 {
            send(&worker, pcm(value, 80, true)).await;
        }
        let marker = if detailed {
            Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                origin: Some("pipecat.flows".into()),
                returning: false,
                event: Some(LocalFlushEvent::default()),
            }))
        } else {
            Frame::new(FrameData::Flush(FlushFrame {
                origin: Some("pipecat.flows".into()),
                returning: false,
            }))
        };
        let marker_id = marker.id;
        send(&worker, marker).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(worker.errors().is_empty());
        assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
        assert_no_flush(&mut worker);
        sink.gate.as_ref().unwrap().add_permits(4);
        let received = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let frame = worker.recv().await.unwrap().frame;
                if frame.id == marker_id {
                    return frame;
                }
            }
        })
        .await
        .unwrap();
        assert!(match &received.data {
            FrameData::Flush(flush) => flush.returning,
            FrameData::FlushDetailed(flush) => {
                assert!(flush.event.as_ref().unwrap().is_set());
                flush.returning
            }
            _ => false,
        });
        assert_eq!(
            written_samples(&sink),
            (1..=4)
                .flat_map(|value| vec![value; 80])
                .collect::<Vec<_>>()
        );
        let (_, report) = finish(worker).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }
}

#[tokio::test]
async fn bounded_wait_timeout_releases_end_and_stop_after_paused_admission() {
    for stop in [false, true] {
        let sink = Arc::new(Sink::new(false));
        let mut cfg = config();
        cfg.queue_capacity = 1;
        cfg.admission_policy = OutputAdmissionPolicy::Wait {
            timeout: Duration::from_millis(20),
        };
        let output = Arc::new(
            OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap(),
        );
        let control = output.control();
        let worker = start(output).await;
        control.pause(None).unwrap();
        send(&worker, pcm(1, 80, true)).await;
        send(&worker, pcm(2, 80, true)).await;
        if stop {
            send(&worker, Frame::new(FrameData::Stop)).await;
        } else {
            worker.end().await.unwrap();
        }
        let (frames, report) = tokio::time::timeout(Duration::from_secs(1), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        assert!(report.errors[0].error.contains("admission timed out"));
        assert_eq!(written_samples(&sink), vec![1; 80]);
        assert!(frames.iter().any(|frame| if stop {
            matches!(frame.frame.data, FrameData::Stop)
        } else {
            matches!(frame.frame.data, FrameData::End(_))
        }));
    }
}

#[tokio::test]
async fn bounded_wait_cancel_preempts_admission_and_owned_backend_write() {
    let sink = Arc::new(Sink::new(true));
    let mut cfg = config();
    cfg.queue_capacity = 1;
    cfg.write_timeout = Duration::from_secs(5);
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let worker = start(output).await;
    send(&worker, pcm(1, 80, true)).await;
    sink.wait_calls(1).await;
    send(&worker, pcm(2, 80, true)).await;
    send(&worker, pcm(3, 80, true)).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    worker.cancel("bounded admission test").await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(1), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(sink.active.load(Ordering::SeqCst), 0);
    assert!(sink.written.lock().unwrap().is_empty());
}

#[tokio::test]
async fn bounded_wait_interruption_discards_only_interruptible_pending_pcm() {
    for keep in [false, true] {
        let sink = Arc::new(Sink::new(true));
        let mut cfg = config();
        cfg.queue_capacity = 1;
        cfg.write_timeout = Duration::from_secs(2);
        let output = Arc::new(
            OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap(),
        );
        let worker = start(output).await;
        send(&worker, pcm(1, 80, true)).await;
        sink.wait_calls(1).await;
        send(&worker, pcm(2, 80, !keep)).await;
        send(&worker, pcm(3, 80, !keep)).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        send(&worker, Frame::new(FrameData::Interruption)).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.interrupts.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if !keep {
            send(&worker, pcm(9, 80, true)).await;
        }
        sink.gate.as_ref().unwrap().add_permits(2);
        let (_, report) = finish(worker).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let expected = if keep {
            [vec![2; 80], vec![3; 80]].concat()
        } else {
            vec![9; 80]
        };
        assert_eq!(written_samples(&sink), expected);
        assert_eq!(sink.active.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn bounded_wait_pause_resume_preserves_all_samples() {
    let sink = Arc::new(Sink::new(false));
    let mut cfg = config();
    cfg.queue_capacity = 1;
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let control = output.control();
    let worker = start(output).await;
    control.pause(None).unwrap();
    send(&worker, pcm(1, 80, true)).await;
    send(&worker, pcm(2, 80, true)).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(sink.calls.load(Ordering::SeqCst), 0);
    assert!(worker.errors().is_empty());
    control.resume(None).unwrap();
    let (_, report) = finish(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(written_samples(&sink), [vec![1; 80], vec![2; 80]].concat());
}

#[tokio::test]
async fn bounded_wait_backend_failure_drains_without_fabricating_receipts() {
    let sink = Arc::new(Sink::new(true));
    let mut cfg = config();
    cfg.queue_capacity = 1;
    cfg.write_timeout = Duration::from_millis(30);
    let output =
        Arc::new(OutputTransport::new("out", cfg, sink.clone(), None, BTreeMap::new()).unwrap());
    let worker = start(output).await;
    send(&worker, pcm(1, 80, true)).await;
    sink.wait_calls(1).await;
    send(&worker, pcm(2, 80, true)).await;
    send(&worker, pcm(3, 80, true)).await;
    let (frames, report) = finish(worker).await;
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(report.errors[0].error.contains("operation timed out"));
    assert_eq!(sink.calls.load(Ordering::SeqCst), 1);
    assert_eq!(sink.active.load(Ordering::SeqCst), 0);
    assert!(sink.written.lock().unwrap().is_empty());
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::TtsAudio(_) | FrameData::BotStartedSpeaking
    )));
}
