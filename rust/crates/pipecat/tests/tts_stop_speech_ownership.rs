//! Original HeyGen-method offline wire -> public codec -> real Pipeline/OutputTransport.
//! Played acknowledges this test backend's recorded PCM, never a physical speaker.
//! The fixture is not an SDK, hosted avatar, or source pipeline/output-runtime run.
use pipecat::{
    audio::{decode_pcm16_le, encode_pcm16_le, PcmFormat},
    core::*,
    media_transport::*,
    network_bus::PythonJsonCodec,
    workers::MessageData,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::Semaphore, time::timeout};

const WAIT: Duration = Duration::from_secs(3);
const RATE: u32 = 16000;
const SAMPLES: usize = 640;
const OUTPUT_NAME: &str = "tts-stop-ownership-output";

struct RecordingOutput {
    entered: Semaphore,
    playback: Semaphore,
    starts: AtomicUsize,
    stops: AtomicUsize,
    written: Mutex<Vec<Frame>>,
}
impl Default for RecordingOutput {
    fn default() -> Self {
        Self {
            entered: Semaphore::new(0),
            playback: Semaphore::new(0),
            starts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            written: Mutex::new(Vec::new()),
        }
    }
}
#[async_trait]
impl OutputBackend for RecordingOutput {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.entered.add_permits(1);
        self.playback.acquire().await.unwrap().forget();
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

struct AdmissionProbe {
    sentinel: FrameObservationToken,
    entered: Semaphore,
}
#[async_trait]
impl PipelineObserver for AdmissionProbe {
    async fn on_event(&self, event: ObservationEvent) {
        if let PipelineObservation::Process {
            processor,
            frame,
            direction: Direction::Downstream,
            ..
        } = event.observation.as_ref()
        {
            if processor.name.as_ref() == OUTPUT_NAME
                && frame.token.as_ref() == Some(&self.sentinel)
            {
                // Normal-lane invocations are serial: entering the last sentinel
                // proves all preceding sequence frames reached this real output.
                self.entered.add_permits(1);
            }
        }
    }
}

struct Finished {
    backend: Arc<RecordingOutput>,
    control: OutputControl,
    observer: ObserverHandle,
    frames: Vec<DirectedFrame>,
    report: WorkerReport,
}
async fn play_complete_sequence(sequence: Vec<Frame>, destinations: Vec<String>) -> Finished {
    let backend = Arc::new(RecordingOutput::default());
    let config = OutputTransportConfig {
        sample_rate: Some(RATE),
        auto_silence: false,
        end_silence: Duration::ZERO,
        destinations,
        ..Default::default()
    };
    assert_eq!(config.speech_idle_timeout, Duration::from_secs(1));
    assert_eq!(config.chunk_duration, Duration::from_millis(40));
    let output = Arc::new(
        OutputTransport::new(OUTPUT_NAME, config, backend.clone(), None, BTreeMap::new()).unwrap(),
    );
    let control = output.control();
    let sentinel = Frame::text("ownership-admission-sentinel");
    let probe = Arc::new(AdmissionProbe {
        sentinel: sentinel.observation_token(),
        entered: Semaphore::new(0),
    });
    let mut pipeline = Pipeline::new(vec![output]);
    let observer = pipeline.add_observer(
        probe.clone(),
        ObserverOptions {
            capacity: 1024,
            drain_timeout: WAIT,
            ..Default::default()
        },
    );
    let worker = pipeline
        .start(PipelineConfig {
            shutdown_timeout: WAIT + WAIT,
            ..Default::default()
        })
        .await
        .unwrap();
    let sender = worker.sender();
    let mut sequence = sequence.into_iter();
    sender
        .send(sequence.next().unwrap(), Direction::Downstream)
        .await
        .unwrap();
    timeout(WAIT, backend.entered.acquire())
        .await
        .expect("first real backend write must reach the playback barrier")
        .unwrap()
        .forget();
    for frame in sequence {
        sender.send(frame, Direction::Downstream).await.unwrap();
    }
    sender.send(sentinel, Direction::Downstream).await.unwrap();
    timeout(WAIT, probe.entered.acquire())
        .await
        .expect("normal-lane sentinel must prove actual output admission")
        .unwrap()
        .forget();
    // The first write still has no Played receipt. All following controls and
    // audio are already in the destination queues. Release only after ordinary
    // End admission; no idle-clock window separates the TTS-stop/audio prefix.
    worker.end().await.unwrap();
    backend.playback.add_permits(128);
    let (frames, report) = timeout(WAIT + WAIT, worker.collect())
        .await
        .expect("normal End must drain and join the actual media worker")
        .unwrap();
    Finished {
        backend,
        control,
        observer,
        frames,
        report,
    }
}
impl Finished {
    fn assert_clean(&self) {
        assert_eq!(self.report.state, WorkerState::Ended);
        assert!(self.report.errors.is_empty(), "{:?}", self.report.errors);
        assert_eq!(self.report.total_errors, 0);
        assert_eq!(self.backend.starts.load(Ordering::SeqCst), 1);
        assert_eq!(self.backend.stops.load(Ordering::SeqCst), 1);
        assert!(!self.control.is_ready());
        let stats = self.observer.stats();
        assert_eq!((stats.dropped, stats.abandoned, stats.failures), (0, 0, 0));
        assert_eq!(
            self.frames
                .iter()
                .filter(|event| event.direction == Direction::Downstream
                    && matches!(
                        event.frame.data,
                        FrameData::End(_) | FrameData::EndDetailed(_)
                    ))
                .count(),
            1
        );
    }
    fn trace(&self, destination: Option<&str>, direction: Direction) -> Vec<String> {
        self.frames
            .iter()
            .filter(|event| {
                event.direction == direction
                    && event.frame.transport_destination.as_deref() == destination
            })
            .filter_map(|event| match &event.frame.data {
                FrameData::BotStartedSpeaking => Some("started".into()),
                FrameData::BotStoppedSpeaking => Some("stopped".into()),
                FrameData::TtsAudio(audio) => Some(format!("tts:{}", samples(audio)[0])),
                FrameData::SpeechOutputAudio(audio) => {
                    Some(format!("speech:{}", samples(audio)[0]))
                }
                FrameData::OutputAudio(audio) => Some(format!("output:{}", samples(audio)[0])),
                FrameData::TtsStopped(stop) => Some(format!(
                    "tts-stop:{}",
                    stop.context_id.as_deref().unwrap_or("none")
                )),
                _ => None,
            })
            .collect()
    }
    fn assert_pcm(&self, destination: Option<&str>, expected: &[(&str, i16)]) {
        let written = self.backend.written.lock().unwrap();
        let actual: Vec<_> = written
            .iter()
            .filter(|frame| frame.transport_destination.as_deref() == destination)
            .map(|frame| {
                let (kind, audio) = match &frame.data {
                    FrameData::TtsAudio(audio) => ("tts", audio),
                    FrameData::SpeechOutputAudio(audio) => ("speech", audio),
                    FrameData::OutputAudio(audio) => ("output", audio),
                    _ => panic!("backend must receive real PCM only"),
                };
                assert_eq!((audio.sample_rate, audio.num_channels), (RATE, 1));
                let decoded = samples(audio);
                assert_eq!(decoded.len(), SAMPLES);
                assert!(decoded.iter().all(|sample| *sample == decoded[0]));
                (kind, decoded[0])
            })
            .collect();
        assert_eq!(
            actual, expected,
            "every source PCM must obtain an explicit recorded Played receipt"
        );
    }
    fn assert_forwarded(&self, frame: &Frame) {
        let matching: Vec<_> = self
            .frames
            .iter()
            .filter(|event| {
                event.direction == Direction::Downstream
                    && event.frame.observation_token() == frame.observation_token()
            })
            .collect();
        assert_eq!(
            matching.len(),
            1,
            "TTS stop must forward once even without TTS ownership"
        );
        assert_eq!(&matching[0].frame, frame);
    }
}
fn samples(audio: &AudioRawFrame) -> Vec<i16> {
    decode_pcm16_le(&audio.audio).unwrap()
}
fn original_fixture_frames(key: &str) -> Vec<Frame> {
    let fixture: Value = serde_json::from_str(HEYGEN_ORIGINAL_METHOD_FIXTURE).unwrap();
    assert_eq!(
        fixture["source_revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    fixture[key]
        .as_array()
        .unwrap()
        .iter()
        .map(|wire| {
            let decoded = PythonJsonCodec::default()
                .decode(&serde_json::to_vec(wire).unwrap())
                .unwrap();
            match &decoded.as_bus_message().unwrap().data {
                MessageData::Frame {
                    frame,
                    direction: Direction::Downstream,
                    ..
                } => (**frame).clone(),
                other => panic!("expected original downstream frame, got {other:?}"),
            }
        })
        .collect()
}
fn pcm(kind: &str, value: i16, destination: Option<&str>) -> Frame {
    let audio = AudioRawFrame::new(encode_pcm16_le(&[value; SAMPLES]), RATE, 1).unwrap();
    let mut frame = Frame::new(match kind {
        "tts" => FrameData::TtsAudio(audio),
        "speech" => FrameData::SpeechOutputAudio(audio),
        "output" => FrameData::OutputAudio(audio),
        _ => panic!("unknown PCM kind"),
    });
    frame.transport_destination = destination.map(str::to_owned);
    frame
}
fn stop(context: &str, destination: Option<&str>) -> Frame {
    let mut frame = Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
        context_id: Some(context.into()),
    }));
    frame.transport_destination = destination.map(str::to_owned);
    frame
}

#[tokio::test]
async fn official_heygen_forwarded_stop_preserves_avatar_after_played_tts_control() {
    let direct = original_fixture_frames("direct_tts_positive_control");
    let avatar = original_fixture_frames("emitted_sequence");
    let direct_stop = direct[1].clone();
    let avatar_stop = avatar[1].clone();
    let finished = play_complete_sequence(direct.into_iter().chain(avatar).collect(), vec![]).await;
    // Behavior assertions intentionally follow normal End, complete drain, and join.
    finished.assert_clean();
    finished.assert_pcm(None, &[("tts", 1000), ("speech", 1000), ("speech", 2000)]);
    finished.assert_forwarded(&direct_stop);
    finished.assert_forwarded(&avatar_stop);
    let down = finished.trace(None, Direction::Downstream);
    assert_eq!(
        &down[..4],
        [
            "started",
            "tts:1000",
            "stopped",
            "tts-stop:direct-tts-positive"
        ],
        "positive control: actual Played TTS must stop at TTSStopped"
    );
    assert_eq!(
        down,
        [
            "started",
            "tts:1000",
            "stopped",
            "tts-stop:direct-tts-positive",
            "started",
            "speech:1000",
            "tts-stop:heygen-original-tts",
            "speech:2000",
            "stopped"
        ],
        "forwarded HeyGen TTSStopped must not stop/restart the continuing avatar speech stream"
    );
    assert_eq!(
        finished.trace(None, Direction::Upstream),
        ["started", "stopped", "started", "stopped"]
    );
}

#[tokio::test]
async fn played_tts_latch_survives_both_mixed_orders_then_resets_per_destination() {
    let first_stop = stop("tts-then-speech", None);
    let second_stop = stop("speech-then-tts", None);
    let after_reset = stop("speech-only-after-reset", None);
    let other_stop = stop("separate-speech-destination", Some("avatar"));
    let background_stop = stop("output-only", Some("background"));
    let sequence = vec![
        pcm("tts", 1100, None),
        pcm("speech", 1200, None),
        first_stop.clone(),
        pcm("speech", 2100, None),
        pcm("tts", 2200, None),
        second_stop.clone(),
        pcm("speech", 3100, None),
        Frame::new(FrameData::TtsStarted(TtsStartedFrame::default())),
        after_reset.clone(),
        pcm("speech", 3200, None),
        pcm("speech", 4100, Some("avatar")),
        other_stop.clone(),
        pcm("speech", 4200, Some("avatar")),
        pcm("output", 5100, Some("background")),
        background_stop.clone(),
    ];
    let finished =
        play_complete_sequence(sequence, vec!["avatar".into(), "background".into()]).await;
    finished.assert_clean();
    finished.assert_pcm(
        None,
        &[
            ("tts", 1100),
            ("speech", 1200),
            ("speech", 2100),
            ("tts", 2200),
            ("speech", 3100),
            ("speech", 3200),
        ],
    );
    finished.assert_pcm(Some("avatar"), &[("speech", 4100), ("speech", 4200)]);
    finished.assert_pcm(Some("background"), &[("output", 5100)]);
    for frame in [
        &first_stop,
        &second_stop,
        &after_reset,
        &other_stop,
        &background_stop,
    ] {
        finished.assert_forwarded(frame);
    }
    let down = finished.trace(None, Direction::Downstream);
    assert_eq!(
        &down[..10],
        [
            "started",
            "tts:1100",
            "speech:1200",
            "stopped",
            "tts-stop:tts-then-speech",
            "started",
            "speech:2100",
            "tts:2200",
            "stopped",
            "tts-stop:speech-then-tts"
        ],
        "positive controls: TTS ownership is latched, not overwritten by the last audio kind"
    );
    assert_eq!(
        finished.trace(Some("background"), Direction::Downstream),
        ["output:5100", "tts-stop:output-only"]
    );
    assert!(finished
        .trace(Some("background"), Direction::Upstream)
        .is_empty());
    assert_eq!(
        down,
        [
            "started",
            "tts:1100",
            "speech:1200",
            "stopped",
            "tts-stop:tts-then-speech",
            "started",
            "speech:2100",
            "tts:2200",
            "stopped",
            "tts-stop:speech-then-tts",
            "started",
            "speech:3100",
            "tts-stop:speech-only-after-reset",
            "speech:3200",
            "stopped"
        ],
        "a completed TTS turn must not retain ownership of later ordinary speech"
    );
    assert_eq!(
        finished.trace(Some("avatar"), Direction::Downstream),
        [
            "started",
            "speech:4100",
            "tts-stop:separate-speech-destination",
            "speech:4200",
            "stopped"
        ],
        "another destination's TTS must not own this avatar stream"
    );
    assert_eq!(
        finished.trace(None, Direction::Upstream),
        ["started", "stopped", "started", "stopped", "started", "stopped"]
    );
    assert_eq!(
        finished.trace(Some("avatar"), Direction::Upstream),
        ["started", "stopped"]
    );
}

// Verbatim bytes of the generated artifact bound in the external provenance record.
// Python source af9f9da9d7afddaa8811c0e0251b32566c980bdf.
// Artifact SHA-256 0c679747afbc2d2e722bc2efcd894069d839992fda778febffe78caa24e37c27.
const HEYGEN_ORIGINAL_METHOD_FIXTURE: &str = r#"{
  "schema": "pipecat-heygen-original-method-offline-v1",
  "source_revision": "af9f9da9d7afddaa8811c0e0251b32566c980bdf",
  "qualification": "Pinned original HeyGen methods and original frame/bus serialization executed offline with explicit collaborators. Not full SDK, source pipeline/output runtime, hosted session, network, inference, device playback, or avatar timing qualification.",
  "configuration": {
    "sample_rate": 16000,
    "num_channels": 1,
    "samples_per_chunk": 640,
    "pcm_a_amplitude": 1000,
    "pcm_b_amplitude": 2000
  },
  "method_pins": {
    "_on_participant_audio_data": {
      "lineno": 203,
      "end_lineno": 210,
      "ast_sha256": "74eb8d8dab2d7aa7be763d12427668c7480f7240482f1819f28667d1364925cf",
      "source_segment_sha256": "2d69917abb7539576a521bb0a6f709fe08cf96a18eab06030399e1c3a09b0a39"
    },
    "process_frame": {
      "lineno": 251,
      "end_lineno": 288,
      "ast_sha256": "f2708b40c47c0a94e1940c211625adf786f6fcd0d4a6a0a7562f6773faf56cc0",
      "source_segment_sha256": "ea2a7a5c4e679ffc6836b73549a3bdb2575f0f6f7d75df3940c3f919a5fbd3c0"
    },
    "_handle_audio_frame": {
      "lineno": 348,
      "end_lineno": 357,
      "ast_sha256": "4a6bc8ee122f527ffe585fbc22577abfb4f183a075fe90077e5bbe6d310772d7",
      "source_segment_sha256": "7d77f7f9bc0654f0313d7471cd3bcf8641b130e5685adfdfedf434eced64dc54"
    }
  },
  "source_pins": {
    "src/pipecat/services/heygen/video.py": "cb9a9a9b6c10e3caa9488aaf6530cd500f1e5c560f5f8e5e28ed83fc952f41fa",
    "src/pipecat/frames/frames.py": "765266f414354ec753cda52b40f0517fffa056c868fbb767f3b41179d4f86449",
    "src/pipecat/processors/frame_processor.py": "9d40141eef4badb211d7318c989f3ca17e3bd41909704cd6811c1c982672556f",
    "src/pipecat/bus/messages.py": "06e6e986b4adb8f4de5dbe1e20b36c52c09e9091f4d336b89e7989cd42e6dd89",
    "src/pipecat/bus/serializers/json.py": "1a10ca82d548cf941630b56d5d401b7e3bd07fdc17f76f77b55575d5e530be50",
    "src/pipecat/utils/utils.py": "4b5127d6833a86e4834b86a7193fe2467560319dc9b1c0603b95e08b220b3330"
  },
  "emitted_sequence": [
    {
      "__type__": "pipecat.bus.messages.BusFrameMessage",
      "__data__": {
        "id": 5,
        "name": "BusFrameMessage#0",
        "source": "heygen-original-method-offline",
        "target": "native-output",
        "frame": {
          "__type__": "pipecat.frames.frames.SpeechOutputAudioRawFrame",
          "__data__": {
            "audio": {
              "__type__": "bytes",
              "__data__": "6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6AM="
            },
            "sample_rate": 16000,
            "num_channels": 1,
            "num_frames": 640,
            "id": 3,
            "name": "SpeechOutputAudioRawFrame#0",
            "metadata": {},
            "interruptible": true
          }
        },
        "direction": {
          "__type__": "pipecat.processors.frame_processor.FrameDirection",
          "__data__": "DOWNSTREAM"
        },
        "bridge": "media"
      }
    },
    {
      "__type__": "pipecat.bus.messages.BusFrameMessage",
      "__data__": {
        "id": 8,
        "name": "BusFrameMessage#2",
        "source": "heygen-original-method-offline",
        "target": "native-output",
        "frame": {
          "__type__": "pipecat.frames.frames.TTSStoppedFrame",
          "__data__": {
            "id": 2,
            "name": "TTSStoppedFrame#0",
            "metadata": {},
            "interruptible": true,
            "context_id": "heygen-original-tts"
          }
        },
        "direction": {
          "__type__": "pipecat.processors.frame_processor.FrameDirection",
          "__data__": "DOWNSTREAM"
        },
        "bridge": "media"
      }
    },
    {
      "__type__": "pipecat.bus.messages.BusFrameMessage",
      "__data__": {
        "id": 11,
        "name": "BusFrameMessage#4",
        "source": "heygen-original-method-offline",
        "target": "native-output",
        "frame": {
          "__type__": "pipecat.frames.frames.SpeechOutputAudioRawFrame",
          "__data__": {
            "audio": {
              "__type__": "bytes",
              "__data__": "0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0AfQB9AH0Ac="
            },
            "sample_rate": 16000,
            "num_channels": 1,
            "num_frames": 640,
            "id": 4,
            "name": "SpeechOutputAudioRawFrame#1",
            "metadata": {},
            "interruptible": true
          }
        },
        "direction": {
          "__type__": "pipecat.processors.frame_processor.FrameDirection",
          "__data__": "DOWNSTREAM"
        },
        "bridge": "media"
      }
    }
  ],
  "withheld_tts_input": {
    "__type__": "pipecat.bus.messages.BusFrameMessage",
    "__data__": {
      "id": 16,
      "name": "BusFrameMessage#6",
      "source": "heygen-original-method-offline",
      "target": "native-output",
      "frame": {
        "__type__": "pipecat.frames.frames.TTSAudioRawFrame",
        "__data__": {
          "audio": {
            "__type__": "bytes",
            "__data__": "6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6AM="
          },
          "sample_rate": 16000,
          "num_channels": 1,
          "num_frames": 640,
          "id": 0,
          "name": "TTSAudioRawFrame#0",
          "metadata": {},
          "interruptible": true,
          "context_id": "heygen-original-tts"
        }
      },
      "direction": {
        "__type__": "pipecat.processors.frame_processor.FrameDirection",
        "__data__": "DOWNSTREAM"
      },
      "bridge": "media"
    }
  },
  "direct_tts_positive_control": [
    {
      "__type__": "pipecat.bus.messages.BusFrameMessage",
      "__data__": {
        "id": 19,
        "name": "BusFrameMessage#8",
        "source": "heygen-original-method-offline",
        "target": "native-output",
        "frame": {
          "__type__": "pipecat.frames.frames.TTSAudioRawFrame",
          "__data__": {
            "audio": {
              "__type__": "bytes",
              "__data__": "6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6APoA+gD6AM="
            },
            "sample_rate": 16000,
            "num_channels": 1,
            "num_frames": 640,
            "id": 14,
            "name": "TTSAudioRawFrame#1",
            "metadata": {},
            "interruptible": true,
            "context_id": "direct-tts-positive"
          }
        },
        "direction": {
          "__type__": "pipecat.processors.frame_processor.FrameDirection",
          "__data__": "DOWNSTREAM"
        },
        "bridge": "media"
      }
    },
    {
      "__type__": "pipecat.bus.messages.BusFrameMessage",
      "__data__": {
        "id": 22,
        "name": "BusFrameMessage#10",
        "source": "heygen-original-method-offline",
        "target": "native-output",
        "frame": {
          "__type__": "pipecat.frames.frames.TTSStoppedFrame",
          "__data__": {
            "id": 15,
            "name": "TTSStoppedFrame#2",
            "metadata": {},
            "interruptible": true,
            "context_id": "direct-tts-positive"
          }
        },
        "direction": {
          "__type__": "pipecat.processors.frame_processor.FrameDirection",
          "__data__": "DOWNSTREAM"
        },
        "bridge": "media"
      }
    }
  ],
  "observations": {
    "queued_tts_is_original_object": true,
    "forwarded_stop_is_original_object": true,
    "no_tts_audio_forwarded": true,
    "base_process_calls": [
      [
        "TTSStartedFrame",
        1,
        "DOWNSTREAM"
      ],
      [
        "TTSAudioRawFrame",
        0,
        "DOWNSTREAM"
      ],
      [
        "TTSStoppedFrame",
        2,
        "DOWNSTREAM"
      ]
    ],
    "metrics_started": 1,
    "metrics_stopped": 0
  },
  "collaborators": {
    "base_service": "explicit recording stub, source AIService lifecycle not run",
    "provider_client": null,
    "audio_send_queue": "real asyncio.Queue; original _handle_audio_frame enqueues the original nonempty TTS",
    "metrics": "test counters only",
    "class_initialization": "__new__; credentials, SDK dependencies and __init__ bypassed",
    "identities": "complete original stdlib-only utils/utils.py functions",
    "serializer": "original JSONMessageSerializer class with empty adapter registry; only original dataclasses/enums/bytes exercised",
    "deprecation_decorator": "identity shim for legacy UninterruptibleFrame marker; original class body untouched",
    "schedule": "test invokes first avatar callback, original TTSStopped dispatch, second callback; remote timing not measured"
  }
}
"#;

// Additional ownership controls use a backend rendezvous per real audio write.
// The backend never treats an entered or discarded write as Played.
use tokio::sync::{mpsc, oneshot};

struct PlaybackAttempt {
    frame: Frame,
    receipt: oneshot::Sender<PlaybackReceipt>,
}
struct ControlledOutput {
    recording: Arc<RecordingOutput>,
    attempts: mpsc::UnboundedSender<PlaybackAttempt>,
}
#[async_trait]
impl OutputBackend for ControlledOutput {
    async fn start(
        &self,
        format: PcmFormat,
        destinations: &[Option<String>],
    ) -> Result<(), ProcessorError> {
        self.recording.start(format, destinations).await
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let (receipt, answer) = oneshot::channel();
        self.attempts
            .send(PlaybackAttempt {
                frame: frame.clone(),
                receipt,
            })
            .map_err(|_| ProcessorError::new("test playback receiver closed"))?;
        let receipt = answer
            .await
            .map_err(|_| ProcessorError::new("test receipt sender closed"))?;
        if receipt == PlaybackReceipt::Played {
            self.recording.written.lock().unwrap().push(frame.clone());
        }
        Ok(receipt)
    }
    async fn interrupt(&self, destination: Option<&str>) -> Result<(), ProcessorError> {
        self.recording.interrupt(destination).await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.recording.stop().await
    }
}
struct ControlledSession {
    worker: PipelineWorker,
    recording: Arc<RecordingOutput>,
    control: OutputControl,
    observer: ObserverHandle,
    probe: Arc<AdmissionProbe>,
    sentinel: Frame,
    attempts: mpsc::UnboundedReceiver<PlaybackAttempt>,
    observed: Vec<DirectedFrame>,
    ending: bool,
}
impl ControlledSession {
    async fn start(destinations: Vec<String>) -> Self {
        let recording = Arc::new(RecordingOutput::default());
        let (attempts, receiver) = mpsc::unbounded_channel();
        let backend = Arc::new(ControlledOutput {
            recording: recording.clone(),
            attempts,
        });
        let config = OutputTransportConfig {
            sample_rate: Some(RATE),
            auto_silence: false,
            end_silence: Duration::ZERO,
            destinations,
            ..Default::default()
        };
        assert_eq!(config.speech_idle_timeout, Duration::from_secs(1));
        let output = Arc::new(
            OutputTransport::new(OUTPUT_NAME, config, backend, None, BTreeMap::new()).unwrap(),
        );
        let control = output.control();
        let sentinel = Frame::text("controlled-ownership-admission");
        let probe = Arc::new(AdmissionProbe {
            sentinel: sentinel.observation_token(),
            entered: Semaphore::new(0),
        });
        let mut pipeline = Pipeline::new(vec![output]);
        let observer = pipeline.add_observer(
            probe.clone(),
            ObserverOptions {
                capacity: 1024,
                drain_timeout: WAIT,
                ..Default::default()
            },
        );
        let worker = pipeline
            .start(PipelineConfig {
                shutdown_timeout: WAIT + WAIT,
                ..Default::default()
            })
            .await
            .unwrap();
        Self {
            worker,
            recording,
            control,
            observer,
            probe,
            sentinel,
            attempts: receiver,
            observed: Vec::new(),
            ending: false,
        }
    }
    async fn admit(&self, frames: Vec<Frame>) {
        for frame in frames {
            self.worker
                .sender()
                .send(frame, Direction::Downstream)
                .await
                .unwrap();
        }
        self.worker
            .sender()
            .send(self.sentinel.clone(), Direction::Downstream)
            .await
            .unwrap();
        timeout(WAIT, self.probe.entered.acquire())
            .await
            .expect("actual normal output admission barrier")
            .unwrap()
            .forget();
    }
    async fn next_attempt(&mut self) -> PlaybackAttempt {
        timeout(WAIT, self.attempts.recv())
            .await
            .expect("actual backend write must reach its receipt rendezvous")
            .expect("backend closed before the expected write")
    }
    async fn observe(&mut self, wanted: impl Fn(&[DirectedFrame]) -> bool) {
        timeout(WAIT, async {
            while !wanted(&self.observed) {
                self.observed.push(
                    self.worker
                        .recv()
                        .await
                        .expect("pipeline closed before an actual boundary"),
                );
            }
        })
        .await
        .expect("expected actual output boundary; a timeout is not an ownership result");
    }
    async fn acknowledge_played(&mut self, attempt: PlaybackAttempt) -> Frame {
        let frame = attempt.frame;
        let token = frame.observation_token();
        attempt
            .receipt
            .send(PlaybackReceipt::Played)
            .expect("backend must still own the receipt");
        // Audio reaches this boundary only after successful receipt handling and
        // track_speech. Merely entering play_audio cannot satisfy this wait.
        self.observe(|events| {
            events.iter().any(|event| {
                event.direction == Direction::Downstream && event.frame.observation_token() == token
            })
        })
        .await;
        frame
    }
    async fn queue_end(&mut self) {
        if !self.ending {
            self.worker.end().await.unwrap();
            self.ending = true;
        }
    }
    async fn finish(mut self) -> Finished {
        self.queue_end().await;
        let (remaining, report) = timeout(WAIT + WAIT, self.worker.collect())
            .await
            .expect("normal End must drain and join controlled output workers")
            .unwrap();
        self.observed.extend(remaining);
        Finished {
            backend: self.recording,
            control: self.control,
            observer: self.observer,
            frames: self.observed,
            report,
        }
    }
}

#[tokio::test]
async fn tts_started_empty_and_discarded_pcm_cannot_own_played_speech() {
    let mut session = ControlledSession::start(vec![]).await;
    let unowned_stop = stop("unplayed-tts", None);
    let tail_stop = stop("partial-tts-tail", None);
    let tail = Frame::new(FrameData::TtsAudio(
        AudioRawFrame::new(encode_pcm16_le(&[6400; 17]), RATE, 1).unwrap(),
    ));
    let empty = Frame::new(FrameData::TtsAudio(
        AudioRawFrame::new(vec![], RATE, 1).unwrap(),
    ));
    session
        .admit(vec![
            pcm("speech", 6100, None),
            Frame::new(FrameData::TtsStarted(TtsStartedFrame::default())),
            empty,
            pcm("tts", 6200, None),
            unowned_stop.clone(),
            pcm("speech", 6300, None),
            tail,
            tail_stop.clone(),
        ])
        .await;
    session.queue_end().await;
    let speech = session.next_attempt().await;
    let first = session.acknowledge_played(speech).await;
    let discarded = session.next_attempt().await;
    let discarded_frame = discarded.frame;
    discarded.receipt.send(PlaybackReceipt::Discarded).unwrap();
    let continuation = session.next_attempt().await;
    let last = session.acknowledge_played(continuation).await;
    let flushed_tail = session.next_attempt().await;
    let tail = session.acknowledge_played(flushed_tail).await;
    let finished = session.finish().await;
    finished.assert_clean();
    assert!(matches!(first.data, FrameData::SpeechOutputAudio(_)));
    assert!(matches!(discarded_frame.data, FrameData::TtsAudio(_)));
    assert!(matches!(last.data, FrameData::SpeechOutputAudio(_)));
    let written = finished.backend.written.lock().unwrap();
    assert_eq!(written.len(), 3);
    for (frame, value) in written[..2].iter().zip([6100, 6300]) {
        let FrameData::SpeechOutputAudio(audio) = &frame.data else {
            panic!("ordinary speech must survive the unplayed TTS stop")
        };
        assert_eq!(samples(audio), vec![value; SAMPLES]);
    }
    let FrameData::TtsAudio(audio) = &tail.data else {
        panic!("stop must flush the partial TTS tail through actual playback")
    };
    assert_eq!(
        samples(audio),
        [vec![6400; 17], vec![0; SAMPLES - 17]].concat()
    );
    assert_eq!(written[2], tail);
    drop(written);
    finished.assert_forwarded(&unowned_stop);
    finished.assert_forwarded(&tail_stop);
    assert_eq!(
        finished.trace(None, Direction::Downstream),
        [
            "started",
            "speech:6100",
            "tts-stop:unplayed-tts",
            "speech:6300",
            "tts:6400",
            "stopped",
            "tts-stop:partial-tts-tail"
        ]
    );
    assert_eq!(
        finished.trace(None, Direction::Upstream),
        ["started", "stopped"]
    );
}

#[tokio::test]
async fn actual_interruption_clears_tts_ownership_before_new_speech() {
    let mut session = ControlledSession::start(vec![]).await;
    session
        .admit(vec![pcm("tts", 7100, None), pcm("output", 7200, None)])
        .await;
    let initial = session.next_attempt().await;
    let tts = session.acknowledge_played(initial).await;
    let interrupted_guard = session.next_attempt().await;
    // This in-flight plain PCM keeps the actor out of its idle branch until the
    // actual system interruption cancels it. It must never obtain Played.
    session
        .worker
        .sender()
        .send(Frame::new(FrameData::Interruption), Direction::Downstream)
        .await
        .unwrap();
    session
        .observe(|events| {
            events.iter().any(|event| {
                event.direction == Direction::Downstream
                    && matches!(event.frame.data, FrameData::BotStoppedSpeaking)
            })
        })
        .await;
    let cancelled_receipt = interrupted_guard
        .receipt
        .send(PlaybackReceipt::Played)
        .is_err();
    let new_stop = stop("after-interruption", None);
    session
        .admit(vec![
            pcm("speech", 7300, None),
            new_stop.clone(),
            pcm("speech", 7400, None),
        ])
        .await;
    session.queue_end().await;
    let next = session.next_attempt().await;
    session.acknowledge_played(next).await;
    let next = session.next_attempt().await;
    session.acknowledge_played(next).await;
    let finished = session.finish().await;
    finished.assert_clean();
    assert!(matches!(tts.data, FrameData::TtsAudio(_)));
    assert!(
        cancelled_receipt,
        "interrupted write must not retain a receipt receiver"
    );
    finished.assert_pcm(None, &[("tts", 7100), ("speech", 7300), ("speech", 7400)]);
    finished.assert_forwarded(&new_stop);
    assert_eq!(
        finished.trace(None, Direction::Downstream),
        [
            "started",
            "tts:7100",
            "stopped",
            "started",
            "speech:7300",
            "tts-stop:after-interruption",
            "speech:7400",
            "stopped"
        ]
    );
    assert_eq!(
        finished.trace(None, Direction::Upstream),
        ["started", "stopped", "started", "stopped"]
    );
}

#[tokio::test]
async fn existing_one_second_idle_stop_clears_tts_ownership_before_new_speech() {
    let mut session = ControlledSession::start(vec![]).await;
    session.admit(vec![pcm("tts", 8100, None)]).await;
    let initial = session.next_attempt().await;
    session.acknowledge_played(initial).await;
    // Observe the existing event, without asserting an exact wall-clock delay
    // or changing the transport's default one-second silence policy.
    session
        .observe(|events| {
            events.iter().any(|event| {
                event.direction == Direction::Downstream
                    && matches!(event.frame.data, FrameData::BotStoppedSpeaking)
            })
        })
        .await;
    let new_stop = stop("after-idle", None);
    session
        .admit(vec![
            pcm("speech", 8200, None),
            new_stop.clone(),
            pcm("speech", 8300, None),
        ])
        .await;
    session.queue_end().await;
    let next = session.next_attempt().await;
    session.acknowledge_played(next).await;
    let next = session.next_attempt().await;
    session.acknowledge_played(next).await;
    let finished = session.finish().await;
    finished.assert_clean();
    finished.assert_pcm(None, &[("tts", 8100), ("speech", 8200), ("speech", 8300)]);
    finished.assert_forwarded(&new_stop);
    assert_eq!(
        finished.trace(None, Direction::Downstream),
        [
            "started",
            "tts:8100",
            "stopped",
            "started",
            "speech:8200",
            "tts-stop:after-idle",
            "speech:8300",
            "stopped"
        ]
    );
    assert_eq!(
        finished.trace(None, Direction::Upstream),
        ["started", "stopped", "started", "stopped"]
    );
}

#[tokio::test]
async fn played_tts_on_one_destination_cannot_own_another_avatar_stream() {
    let mut session = ControlledSession::start(vec!["avatar".into()]).await;
    session
        .admit(vec![pcm("tts", 9100, None), pcm("output", 9200, None)])
        .await;
    let initial = session.next_attempt().await;
    let actual_tts = session.acknowledge_played(initial).await;
    let default_guard = session.next_attempt().await;
    // Default TTS is now definitely Played and its actor is held in the next
    // write, so it cannot idle-stop or release its latch during the avatar test.
    let avatar_stop = stop("other-destination", Some("avatar"));
    session
        .admit(vec![
            pcm("speech", 9300, Some("avatar")),
            avatar_stop.clone(),
            pcm("speech", 9400, Some("avatar")),
        ])
        .await;
    let next = session.next_attempt().await;
    session.acknowledge_played(next).await;
    let next = session.next_attempt().await;
    session.acknowledge_played(next).await;
    session.queue_end().await;
    let guard = session.acknowledge_played(default_guard).await;
    let finished = session.finish().await;
    finished.assert_clean();
    assert!(matches!(actual_tts.data, FrameData::TtsAudio(_)));
    assert_eq!(actual_tts.transport_destination, None);
    assert!(matches!(guard.data, FrameData::OutputAudio(_)));
    finished.assert_pcm(None, &[("tts", 9100), ("output", 9200)]);
    finished.assert_pcm(Some("avatar"), &[("speech", 9300), ("speech", 9400)]);
    finished.assert_forwarded(&avatar_stop);
    assert_eq!(
        finished.trace(None, Direction::Downstream),
        ["started", "tts:9100", "output:9200", "stopped"]
    );
    assert_eq!(
        finished.trace(Some("avatar"), Direction::Downstream),
        [
            "started",
            "speech:9300",
            "tts-stop:other-destination",
            "speech:9400",
            "stopped"
        ]
    );
    assert_eq!(
        finished.trace(Some("avatar"), Direction::Upstream),
        ["started", "stopped"]
    );
}
