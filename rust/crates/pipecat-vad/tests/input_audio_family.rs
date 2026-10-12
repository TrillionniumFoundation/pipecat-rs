use pipecat_core::{
    async_trait, AudioRawFrame, DirectedFrame, Direction, Frame, FrameData, FrameProcessor,
    ImageRawFrame, LocalFrameTag, Pipeline, PipelineConfig, PipelineWorker, ProcessorContext,
    ProcessorError, StartFrame, UserAudioRawFrame, VadParams, WorkerReport, WorkerState,
};
use pipecat_vad::{
    ProcessorOptions, RuntimeOptions, SileroModel, SileroVadAnalyzer, SileroVadProcessor,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc;

const DEADLINE: Duration = Duration::from_secs(15);
const SPEECH: &[u8] = include_bytes!("fixtures/smart_turn_speech.pcm16");

type Receipt = (u64, Result<(), ProcessorError>);

// A receipt is sent only after the real processor call returns. Input queue
// admission alone cannot establish that audio was accepted before termination.
struct ProcessedVad {
    vad: Arc<SileroVadProcessor>,
    receipts: mpsc::UnboundedSender<Receipt>,
}

#[async_trait]
impl FrameProcessor for ProcessedVad {
    fn name(&self) -> &str {
        "ProcessedVad"
    }

    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.vad.setup(config, ctx).await
    }

    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let id = frame.id;
        let result = self.vad.process(frame, direction, ctx).await;
        let _ = self.receipts.send((id, result.clone()));
        result
    }

    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.vad.prepare_end(ctx).await
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.vad.cleanup().await
    }
}

struct Harness {
    worker: PipelineWorker,
    vad: Arc<SileroVadProcessor>,
    receipts: mpsc::UnboundedReceiver<Receipt>,
}

impl Harness {
    async fn start() -> Self {
        let runtime = RuntimeOptions {
            library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
                .map(PathBuf::from)
                .unwrap_or_else(|| {
                    PathBuf::from("/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0")
                }),
        };
        let model_path = std::env::var_os("PIPECAT_SILERO_MODEL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
            });
        let model = SileroModel::load(&runtime, model_path)
            .expect("actual Silero model and native ONNX Runtime are required");
        let analyzer = SileroVadAnalyzer::new(model, 16_000, VadParams::default()).unwrap();
        let vad = Arc::new(
            SileroVadProcessor::new(
                analyzer,
                ProcessorOptions {
                    queue_capacity: 256,
                    audio_idle_timeout: Duration::ZERO,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let (tx, receipts) = mpsc::unbounded_channel();
        let worker = Pipeline::new(vec![Arc::new(ProcessedVad {
            vad: vad.clone(),
            receipts: tx,
        })])
        .start(PipelineConfig {
            output_capacity: 2048,
            ..Default::default()
        })
        .await
        .unwrap();
        Self {
            worker,
            vad,
            receipts,
        }
    }

    async fn send(&mut self, frame: Frame, direction: Direction) -> Result<(), ProcessorError> {
        let id = frame.id;
        self.worker.sender().send(frame, direction).await.unwrap();
        tokio::time::timeout(DEADLINE, async {
            loop {
                let (processed, result) = self.receipts.recv().await.expect("processor closed");
                if processed == id {
                    return result;
                }
            }
        })
        .await
        .expect("real processor call did not finish")
    }

    async fn finish(self, cancel: bool) -> (Vec<DirectedFrame>, WorkerReport) {
        if cancel {
            self.worker.cancel("input audio family test").await.unwrap();
        } else {
            self.worker.end().await.unwrap();
        }
        let result = tokio::time::timeout(DEADLINE, self.worker.collect())
            .await
            .expect("native inference and cleanup did not finish")
            .unwrap();
        assert_eq!(
            result.1.state,
            if cancel {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        assert!(
            !self.vad.is_running(),
            "cleanup must join the native thread"
        );
        result
    }
}

#[derive(Clone, Copy, Debug)]
enum Family {
    Input,
    User,
}

fn audio_frame(family: Family, audio: AudioRawFrame) -> Frame {
    Frame::new(match family {
        Family::Input => FrameData::InputAudio(audio),
        Family::User => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "speaker-7".into(),
        }),
    })
}

fn marked(mut frame: Frame, index: usize, tag: &LocalFrameTag) -> Frame {
    frame.name = format!("unchanged-input-{index}");
    frame.pts = Some(17_000 + index as u64);
    frame.metadata.insert(
        "application-data".into(),
        serde_json::json!({"index": index, "nested": [true, "preserve"]}),
    );
    frame.transport_source = Some("microphone-track".into());
    frame.transport_destination = Some("monitor-track".into());
    frame.broadcast_sibling_id = Some(90_000 + index as u64);
    frame.interruptible = false;
    tag.mark(&mut frame).unwrap();
    frame
}

fn assert_passthrough(
    output: &[DirectedFrame],
    sent: &[Frame],
    direction: Direction,
    tag: &LocalFrameTag,
) {
    for expected in sent {
        let matches: Vec<_> = output
            .iter()
            .filter(|f| f.frame.id == expected.id)
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "each original frame must be forwarded once"
        );
        let actual = matches[0];
        assert_eq!(actual.direction, direction);
        // Frame equality includes PCM, user/context IDs and all public envelope
        // fields. Local capabilities need separate checks, outside wire equality.
        assert_eq!(actual.frame, *expected);
        assert_eq!(
            actual.frame.observation_token(),
            expected.observation_token()
        );
        assert!(tag.contains(&actual.frame));
    }
}

fn is_vad(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::VadUserStartedSpeaking(_)
            | FrameData::VadUserStoppedSpeaking(_)
            | FrameData::UserSpeaking
    )
}

async fn speech_case(family: Family, direction: Direction) {
    let mut harness = Harness::start().await;
    let tag = LocalFrameTag::new();
    let mut sent = Vec::new();
    for (index, bytes) in SPEECH
        .chunks(1024)
        .chain(std::iter::repeat_n(&[0_u8; 1024][..], 60))
        .enumerate()
    {
        let mut audio = AudioRawFrame::new(bytes.to_vec(), 16_000, 1).unwrap();
        audio.context_id = Some(format!("audio-context-{index}"));
        let frame = marked(audio_frame(family, audio), index, &tag);
        harness.send(frame.clone(), direction).await.unwrap();
        sent.push(frame);
    }
    let (output, report) = harness.finish(false).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_passthrough(&output, &sent, direction, &tag);
    for event_direction in [Direction::Downstream, Direction::Upstream] {
        assert!(
            output.iter().any(|f| f.direction == event_direction
                && matches!(f.frame.data, FrameData::VadUserStartedSpeaking(_))),
            "{family:?} {direction:?} must run the actual model and broadcast speech"
        );
        assert!(output.iter().any(|f| f.direction == event_direction
            && matches!(f.frame.data, FrameData::VadUserStoppedSpeaking(_))));
        assert!(output
            .iter()
            .any(|f| f.direction == event_direction
                && matches!(f.frame.data, FrameData::UserSpeaking)));
    }
    let end = output
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::End(_)))
        .unwrap();
    assert!(output.iter().rposition(|f| is_vad(&f.frame.data)).unwrap() < end);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_input_downstream_runs_real_vad_and_preserves_frame() {
    speech_case(Family::Input, Direction::Downstream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_input_downstream_runs_real_vad_and_preserves_frame() {
    speech_case(Family::User, Direction::Downstream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordinary_input_upstream_runs_real_vad_and_preserves_frame() {
    speech_case(Family::Input, Direction::Upstream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn user_input_upstream_runs_real_vad_and_preserves_frame() {
    speech_case(Family::User, Direction::Upstream).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn output_audio_and_other_raw_frames_are_only_forwarded() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        let mut harness = Harness::start().await;
        let tag = LocalFrameTag::new();
        let mut sent = Vec::new();
        // A complete speech fixture per output family detects accidentally
        // widening the input match to every frame carrying PCM.
        for family in 0..3 {
            for bytes in SPEECH.chunks(1024) {
                let audio = AudioRawFrame::new(bytes.to_vec(), 16_000, 1).unwrap();
                let data = match family {
                    0 => FrameData::OutputAudio(audio),
                    1 => FrameData::TtsAudio(audio),
                    _ => FrameData::SpeechOutputAudio(audio),
                };
                let frame = marked(Frame::new(data), sent.len(), &tag);
                harness.send(frame.clone(), direction).await.unwrap();
                sent.push(frame);
            }
        }
        let frame = marked(
            Frame::new(FrameData::InputImage(ImageRawFrame {
                image: vec![1, 2, 3],
                size: (1, 1),
                format: Some("RGB".into()),
            })),
            sent.len(),
            &tag,
        );
        harness.send(frame.clone(), direction).await.unwrap();
        sent.push(frame);
        if direction == Direction::Upstream {
            let frame = marked(
                Frame::new(FrameData::VadParamsUpdate(VadParams {
                    confidence: -1.0,
                    ..Default::default()
                })),
                sent.len(),
                &tag,
            );
            harness.send(frame.clone(), direction).await.unwrap();
            sent.push(frame);
        }
        let (output, report) = harness.finish(false).await;
        assert!(report.errors.is_empty());
        assert_passthrough(&output, &sent, direction, &tag);
        assert!(!output.iter().any(|f| is_vad(&f.frame.data)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn both_input_families_share_validation_in_both_directions() {
    for family in [Family::Input, Family::User] {
        for direction in [Direction::Downstream, Direction::Upstream] {
            let mut harness = Harness::start().await;
            let invalid = [
                (vec![0; 3], 16_000, 1, "whole signed 16-bit"),
                (vec![0; 1024], 0, 1, "must be nonzero"),
                (vec![0; 1024], 16_000, 0, "must be nonzero"),
                (vec![0; 1024], 8_000, 1, "configured sample rate"),
                (vec![0; 1024], 16_000, 2, "requires mono PCM"),
                (vec![0; 32_002], 16_000, 1, "duration limit"),
            ];
            let mut rejected = Vec::new();
            let mut outcomes = Vec::new();
            for (audio, sample_rate, num_channels, message) in invalid {
                let frame = audio_frame(
                    family,
                    AudioRawFrame {
                        audio,
                        sample_rate,
                        num_channels,
                        context_id: Some("invalid-context".into()),
                    },
                );
                rejected.push(frame.id);
                outcomes.push((message, harness.send(frame, direction).await));
            }
            // Nonfatal validation failures must not disable subsequent analysis
            // of valid input in this same session.
            for bytes in SPEECH.chunks(1024) {
                let frame = audio_frame(
                    family,
                    AudioRawFrame::new(bytes.to_vec(), 16_000, 1).unwrap(),
                );
                harness.send(frame, direction).await.unwrap();
            }
            let (output, report) = harness.finish(false).await;
            for (message, outcome) in outcomes {
                let error = outcome.expect_err("invalid input accepted");
                assert!(!error.fatal);
                assert!(error.message.contains(message), "{}", error.message);
            }
            assert_eq!(report.errors.len(), rejected.len());
            // Native validation deliberately precedes forwarding; Python forwards first.
            assert!(!output.iter().any(|f| rejected.contains(&f.frame.id)));
            assert!(output
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::VadUserStartedSpeaking(_))));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_input_is_accepted_and_preserved_in_both_families_and_directions() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        let mut harness = Harness::start().await;
        let tag = LocalFrameTag::new();
        let mut sent = Vec::new();
        for family in [Family::Input, Family::User] {
            let frame = marked(
                audio_frame(family, AudioRawFrame::new(Vec::new(), 16_000, 1).unwrap()),
                sent.len(),
                &tag,
            );
            harness.send(frame.clone(), direction).await.unwrap();
            sent.push(frame);
        }
        let (output, report) = harness.finish(false).await;
        assert!(report.errors.is_empty());
        assert_passthrough(&output, &sent, direction, &tag);
        assert!(!output.iter().any(|f| is_vad(&f.frame.data)));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_joins_native_worker_after_each_input_family_and_direction() {
    for family in [Family::Input, Family::User] {
        for direction in [Direction::Downstream, Direction::Upstream] {
            let mut harness = Harness::start().await;
            let frame = audio_frame(
                family,
                AudioRawFrame::new(SPEECH[..32_000].to_vec(), 16_000, 1).unwrap(),
            );
            harness.send(frame, direction).await.unwrap();
            let (_, report) = harness.finish(true).await;
            assert!(report.errors.is_empty());
        }
    }
}
