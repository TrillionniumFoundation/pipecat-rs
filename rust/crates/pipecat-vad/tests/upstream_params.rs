use pipecat_core::{
    async_trait, DirectedFrame, Direction, Frame, FrameData, FrameProcessor, LocalFrameTag,
    Pipeline, PipelineConfig, PipelineWorker, ProcessorContext, ProcessorError, StartFrame,
    VadParams, WorkerReport, WorkerState,
};
use pipecat_vad::{
    ProcessorOptions, RuntimeOptions, SileroModel, SileroVadAnalyzer, SileroVadProcessor,
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::mpsc;

const DEADLINE: Duration = Duration::from_secs(15);
type Receipt = (u64, Result<(), ProcessorError>);

// A receipt acknowledges the real processor call, not just PipelineSender
// admission. It does not claim that the native thread has applied a command.
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
    async fn start(params: VadParams) -> Self {
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
        let analyzer = SileroVadAnalyzer::new(model, 16_000, params.clone()).unwrap();
        let vad = Arc::new(
            SileroVadProcessor::new(
                analyzer,
                ProcessorOptions {
                    queue_capacity: 32,
                    audio_idle_timeout: Duration::ZERO,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let (tx, receipts) = mpsc::unbounded_channel();
        let mut worker = Pipeline::new(vec![Arc::new(ProcessedVad {
            vad: vad.clone(),
            receipts: tx,
        })])
        .start(PipelineConfig {
            output_capacity: 256,
            ..Default::default()
        })
        .await
        .unwrap();
        // Fully consume the real Start and its initial settings in both
        // directions, so later observations cannot confuse startup with update.
        tokio::time::timeout(DEADLINE, async {
            let mut started = false;
            let mut directions = Vec::new();
            while !started || directions.len() < 2 {
                let item = worker.recv().await.expect("pipeline closed during startup");
                match item.frame.data {
                    FrameData::Start(_) => {
                        assert_eq!(item.direction, Direction::Downstream);
                        assert!(!started);
                        started = true;
                    }
                    FrameData::SpeechControlParams(control) => {
                        assert!(started, "Start must precede initial settings");
                        assert_eq!(control.vad_params, Some(params.clone()));
                        assert!(!directions.contains(&item.direction));
                        directions.push(item.direction);
                    }
                    other => panic!("unexpected startup frame: {other:?}"),
                }
            }
        })
        .await
        .expect("real pipeline startup did not complete");
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

    async fn collect(self, state: WorkerState) -> (Vec<DirectedFrame>, WorkerReport) {
        let result = tokio::time::timeout(DEADLINE, self.worker.collect())
            .await
            .expect("native parameter work and cleanup did not finish")
            .unwrap();
        assert_eq!(result.1.state, state);
        assert!(
            !self.vad.is_running(),
            "cleanup must join the native thread"
        );
        result
    }

    async fn finish(self) -> (Vec<DirectedFrame>, WorkerReport) {
        self.worker.end().await.unwrap();
        self.collect(WorkerState::Ended).await
    }
}

fn changed_params() -> VadParams {
    VadParams {
        confidence: 0.61,
        min_volume: 0.31,
        start_secs: 0.064,
        stop_secs: 0.192,
    }
}

fn marked(mut frame: Frame, index: usize, tag: &LocalFrameTag) -> Frame {
    frame.name = format!("upstream-params-{index}");
    frame.pts = Some(17_000 + index as u64);
    frame.metadata.insert(
        "application-data".into(),
        serde_json::json!({"index": index, "nested": [true, "preserve"]}),
    );
    frame.transport_source = Some("ivr-control".into());
    frame.transport_destination = Some("vad-control".into());
    frame.broadcast_sibling_id = Some(90_000 + index as u64);
    frame.interruptible = false;
    tag.mark(&mut frame).unwrap();
    frame
}

fn assert_original(
    output: &[DirectedFrame],
    expected: &Frame,
    direction: Direction,
    tag: &LocalFrameTag,
) -> usize {
    let matches: Vec<_> = output
        .iter()
        .enumerate()
        .filter(|(_, f)| f.frame.id == expected.id)
        .collect();
    assert_eq!(matches.len(), 1, "each original must be forwarded once");
    let (position, actual) = matches[0];
    assert_eq!(actual.direction, direction);
    assert_eq!(actual.frame, *expected);
    assert_eq!(
        actual.frame.observation_token(),
        expected.observation_token()
    );
    assert!(tag.contains(&actual.frame));
    position
}

fn assert_applied(
    output: &[DirectedFrame],
    params: &VadParams,
    original: &Frame,
    original_position: usize,
    terminal_position: usize,
    tag: &LocalFrameTag,
) {
    for direction in [Direction::Downstream, Direction::Upstream] {
        let matches: Vec<_> = output
            .iter()
            .enumerate()
            .filter(|(_, f)| {
                f.direction == direction
                    && matches!(&f.frame.data, FrameData::SpeechControlParams(control)
                        if control.vad_params.as_ref() == Some(params))
            })
            .collect();
        assert_eq!(
            matches.len(),
            1,
            "applied settings must be broadcast once in {direction:?}"
        );
        let (position, applied) = matches[0];
        assert!(original_position < position);
        assert!(position < terminal_position);
        assert_ne!(applied.frame.id, original.id);
        assert_ne!(
            applied.frame.observation_token(),
            original.observation_token()
        );
        assert!(
            !tag.contains(&applied.frame),
            "derived settings must not inherit local tags"
        );
        if let FrameData::SpeechControlParams(control) = &applied.frame.data {
            assert!(control.turn_params.is_none());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_update_preserves_identity_and_broadcasts_applied_params_before_end() {
    let mut harness = Harness::start(VadParams::default()).await;
    let tag = LocalFrameTag::new();
    let params = changed_params();
    let update = marked(
        Frame::new(FrameData::VadParamsUpdate(params.clone())),
        0,
        &tag,
    );
    let accepted = harness.send(update.clone(), Direction::Upstream).await;
    // Always finish and join before the expected old-source missing-broadcast
    // assertion. No timeout is treated as evidence of absent settings.
    let (output, report) = harness.finish().await;
    accepted.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let original = assert_original(&output, &update, Direction::Upstream, &tag);
    let end = output
        .iter()
        .position(|f| {
            f.direction == Direction::Downstream && matches!(f.frame.data, FrameData::End(_))
        })
        .expect("normal End must be forwarded");
    assert_applied(&output, &params, &update, original, end, &tag);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_upstream_update_is_rejected_before_forwarding_and_repair_remains_usable() {
    let mut harness = Harness::start(VadParams::default()).await;
    let tag = LocalFrameTag::new();
    let invalid = marked(
        Frame::new(FrameData::VadParamsUpdate(VadParams {
            confidence: -1.0,
            ..Default::default()
        })),
        0,
        &tag,
    );
    let rejected = harness.send(invalid.clone(), Direction::Upstream).await;
    let params = changed_params();
    let repair = marked(
        Frame::new(FrameData::VadParamsUpdate(params.clone())),
        1,
        &tag,
    );
    let repaired = harness.send(repair.clone(), Direction::Downstream).await;
    let (output, report) = harness.finish().await;
    let error = rejected.expect_err("invalid upstream parameter update must be rejected");
    assert!(!error.fatal);
    assert!(
        error.message.contains("finite values in [0, 1]"),
        "{}",
        error.message
    );
    repaired.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert!(!output.iter().any(|f| f.frame.id == invalid.id));
    assert!(output.iter().all(|f| !matches!(&f.frame.data,
        FrameData::SpeechControlParams(control) if control.vad_params.as_ref() != Some(&params))));
    let original = assert_original(&output, &repair, Direction::Downstream, &tag);
    let end = output
        .iter()
        .position(|f| {
            f.direction == Direction::Downstream && matches!(f.frame.data, FrameData::End(_))
        })
        .expect("normal End must be forwarded after repair");
    assert_applied(&output, &params, &repair, original, end, &tag);
}

// Controls below extend the frozen two-witness baseline without changing it.
impl Harness {
    async fn receive_until(
        &mut self,
        predicate: impl Fn(&[DirectedFrame]) -> bool,
    ) -> Vec<DirectedFrame> {
        tokio::time::timeout(DEADLINE, async {
            let mut output = Vec::new();
            loop {
                output.push(
                    self.worker
                        .recv()
                        .await
                        .expect("pipeline closed before event"),
                );
                if predicate(&output) {
                    return output;
                }
            }
        })
        .await
        .expect("required real processor event did not arrive")
    }
}

fn applied_count(output: &[DirectedFrame], params: &VadParams) -> usize {
    output
        .iter()
        .filter(|f| {
            matches!(&f.frame.data, FrameData::SpeechControlParams(control)
            if control.vad_params.as_ref() == Some(params))
        })
        .count()
}

fn terminal_position(output: &[DirectedFrame], terminal: &Frame, tag: &LocalFrameTag) -> usize {
    let position = assert_original(output, terminal, Direction::Downstream, tag);
    assert_eq!(
        position,
        output.len() - 1,
        "terminal must follow drained output"
    );
    position
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn downstream_update_keeps_the_same_identity_and_applied_broadcast_contract() {
    let mut harness = Harness::start(VadParams::default()).await;
    let tag = LocalFrameTag::new();
    let params = changed_params();
    let update = marked(
        Frame::new(FrameData::VadParamsUpdate(params.clone())),
        0,
        &tag,
    );
    let accepted = harness.send(update.clone(), Direction::Downstream).await;
    let (output, report) = harness.finish().await;
    accepted.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let original = assert_original(&output, &update, Direction::Downstream, &tag);
    let end = output
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::End(_)))
        .unwrap();
    assert_applied(&output, &params, &update, original, end, &tag);
    assert_eq!(applied_count(&output, &params), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inclusive_parameter_bounds_are_accepted_in_both_directions() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        let mut harness = Harness::start(VadParams::default()).await;
        let tag = LocalFrameTag::new();
        let params = [
            VadParams {
                confidence: 0.0,
                min_volume: 0.0,
                start_secs: 0.0,
                stop_secs: 0.0,
            },
            VadParams {
                confidence: 1.0,
                min_volume: 1.0,
                start_secs: 3600.0,
                stop_secs: 3600.0,
            },
        ];
        let mut updates = Vec::new();
        let mut outcomes = Vec::new();
        for (index, params) in params.iter().enumerate() {
            let update = marked(
                Frame::new(FrameData::VadParamsUpdate(params.clone())),
                index,
                &tag,
            );
            outcomes.push(harness.send(update.clone(), direction).await);
            updates.push(update);
        }
        let (output, report) = harness.finish().await;
        for outcome in outcomes {
            outcome.unwrap();
        }
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let end = output
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::End(_)))
            .unwrap();
        for (params, update) in params.iter().zip(&updates) {
            let original = assert_original(&output, update, direction, &tag);
            assert_applied(&output, params, update, original, end, &tag);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_parameter_rejects_nonfinite_and_out_of_range_values_then_repairs_in_both_directions()
{
    for direction in [Direction::Downstream, Direction::Upstream] {
        let mut harness = Harness::start(VadParams::default()).await;
        let tag = LocalFrameTag::new();
        let mut invalid_ids = Vec::new();
        let mut outcomes = Vec::new();
        for field in 0..4 {
            let maximum = if field < 2 { 1.0 } else { 3600.0 };
            for value in [
                -0.01,
                maximum + 0.01,
                f64::NAN,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ] {
                let mut params = changed_params();
                match field {
                    0 => params.confidence = value,
                    1 => params.min_volume = value,
                    2 => params.start_secs = value,
                    _ => params.stop_secs = value,
                }
                let update = marked(
                    Frame::new(FrameData::VadParamsUpdate(params)),
                    invalid_ids.len(),
                    &tag,
                );
                invalid_ids.push(update.id);
                outcomes.push((field, harness.send(update, direction).await));
            }
        }
        let params = changed_params();
        let repair = marked(
            Frame::new(FrameData::VadParamsUpdate(params.clone())),
            invalid_ids.len(),
            &tag,
        );
        let repaired = harness.send(repair.clone(), direction).await;
        let (output, report) = harness.finish().await;
        for (field, result) in outcomes {
            let error = result.expect_err("invalid parameter update must be rejected");
            assert!(!error.fatal);
            let message = if field < 2 {
                "finite values in [0, 1]"
            } else {
                "finite values in [0, 3600]"
            };
            assert!(error.message.contains(message), "{}", error.message);
        }
        repaired.unwrap();
        assert_eq!(report.errors.len(), invalid_ids.len());
        assert!(!output.iter().any(|f| invalid_ids.contains(&f.frame.id)));
        // Do not compare NaN payloads for equality; only the valid repair may
        // have an applied-settings completion after startup was drained.
        assert!(output.iter().all(|f| !matches!(&f.frame.data,
            FrameData::SpeechControlParams(control) if control.vad_params.as_ref() != Some(&params))));
        let original = assert_original(&output, &repair, direction, &tag);
        let end = output
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::End(_)))
            .unwrap();
        assert_applied(&output, &params, &repair, original, end, &tag);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_updates_from_both_directions_drain_in_order_before_detailed_end_and_stop() {
    for first_direction in [Direction::Upstream, Direction::Downstream] {
        for data in [
            FrameData::EndDetailed(pipecat_core::ReasonFrame {
                reason: Some(serde_json::json!({"kind": "completed", "turn": 7})),
            }),
            FrameData::Stop,
        ] {
            let mut harness = Harness::start(VadParams::default()).await;
            let tag = LocalFrameTag::new();
            let second_direction = if first_direction == Direction::Upstream {
                Direction::Downstream
            } else {
                Direction::Upstream
            };
            let params = [
                changed_params(),
                VadParams {
                    stop_secs: 0.384,
                    ..changed_params()
                },
            ];
            let mut updates = Vec::new();
            let mut outcomes = Vec::new();
            for (index, direction) in [first_direction, second_direction].into_iter().enumerate() {
                let update = marked(
                    Frame::new(FrameData::VadParamsUpdate(params[index].clone())),
                    index,
                    &tag,
                );
                outcomes.push(harness.send(update.clone(), direction).await);
                updates.push((update, direction));
            }
            // Both process-return receipts precede terminal admission. Merely
            // placing updates in the input queue would not prove this ordering.
            let state = if matches!(data, FrameData::Stop) {
                WorkerState::Stopped
            } else {
                WorkerState::Ended
            };
            let terminal = marked(Frame::new(data), 2, &tag);
            harness
                .worker
                .sender()
                .send(terminal.clone(), Direction::Downstream)
                .await
                .unwrap();
            let (output, report) = harness.collect(state).await;
            for outcome in outcomes {
                outcome.unwrap();
            }
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            let end = terminal_position(&output, &terminal, &tag);
            for ((update, direction), params) in updates.iter().zip(&params) {
                let original = assert_original(&output, update, *direction, &tag);
                assert_applied(&output, params, update, original, end, &tag);
            }
            for direction in [Direction::Downstream, Direction::Upstream] {
                let applied: Vec<_> = output
                    .iter()
                    .filter_map(|f| {
                        if f.direction != direction {
                            return None;
                        }
                        match &f.frame.data {
                            FrameData::SpeechControlParams(control) => control.vad_params.clone(),
                            _ => None,
                        }
                    })
                    .collect();
                assert_eq!(applied, params, "native completion order must be preserved");
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancel_variants_join_after_acknowledged_updates_without_claiming_unapplied_work_is_lost() {
    for direction in [Direction::Upstream, Direction::Downstream] {
        for detailed in [false, true] {
            let mut harness = Harness::start(VadParams::default()).await;
            let tag = LocalFrameTag::new();
            let first_params = changed_params();
            let first = marked(
                Frame::new(FrameData::VadParamsUpdate(first_params.clone())),
                0,
                &tag,
            );
            harness.send(first.clone(), direction).await.unwrap();
            // Establish that the first update really applied before cancel;
            // the second is only acknowledged, so its completion is racy.
            let mut output = harness
                .receive_until(|frames| applied_count(frames, &first_params) == 2)
                .await;
            let second_params = VadParams {
                start_secs: 0.128,
                ..changed_params()
            };
            let second = marked(
                Frame::new(FrameData::VadParamsUpdate(second_params.clone())),
                1,
                &tag,
            );
            harness.send(second.clone(), direction).await.unwrap();
            let data = if detailed {
                FrameData::CancelDetailed(pipecat_core::ReasonFrame {
                    reason: Some(serde_json::json!({"kind": "caller_disconnected"})),
                })
            } else {
                FrameData::Cancel(pipecat_core::CancelFrame {
                    reason: Some("parameter control test".into()),
                })
            };
            let terminal = marked(Frame::new(data), 2, &tag);
            harness
                .worker
                .sender()
                .send(terminal.clone(), Direction::Downstream)
                .await
                .unwrap();
            let (tail, report) = harness.collect(WorkerState::Cancelled).await;
            output.extend(tail);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            let cancel = terminal_position(&output, &terminal, &tag);
            let first_position = assert_original(&output, &first, direction, &tag);
            assert_applied(&output, &first_params, &first, first_position, cancel, &tag);
            let second_position = assert_original(&output, &second, direction, &tag);
            for event_direction in [Direction::Downstream, Direction::Upstream] {
                let completed: Vec<_> = output.iter().enumerate().filter(|(_, f)| {
                    f.direction == event_direction && matches!(&f.frame.data,
                        FrameData::SpeechControlParams(control) if control.vad_params.as_ref() == Some(&second_params))
                }).collect();
                assert!(completed.len() <= 1);
                if let Some((position, frame)) = completed.first() {
                    assert!(second_position < *position && *position < cancel);
                    assert!(!tag.contains(&frame.frame));
                }
            }
            assert!(output.iter().all(|f| !matches!(&f.frame.data,
                FrameData::SpeechControlParams(control)
                    if control.vad_params.as_ref() != Some(&first_params)
                        && control.vad_params.as_ref() != Some(&second_params))));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrelated_upstream_controls_remain_unchanged_passthrough() {
    let mut harness = Harness::start(VadParams::default()).await;
    let tag = LocalFrameTag::new();
    let mut sent = Vec::new();
    for data in [
        FrameData::SpeechControlParams(pipecat_core::SpeechControlParamsFrame {
            vad_params: Some(VadParams {
                confidence: -1.0,
                ..Default::default()
            }),
            turn_params: None,
        }),
        FrameData::UserIdleTimeoutUpdate { timeout: 12.0 },
        FrameData::BotStartedSpeaking,
        FrameData::BotStoppedSpeaking,
        FrameData::SttMute(true),
        FrameData::UserMuteStarted,
        FrameData::Control,
    ] {
        let frame = marked(Frame::new(data), sent.len(), &tag);
        harness
            .send(frame.clone(), Direction::Upstream)
            .await
            .unwrap();
        sent.push(frame);
    }
    let (output, report) = harness.finish().await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    for frame in &sent {
        assert_original(&output, frame, Direction::Upstream, &tag);
    }
    assert_eq!(
        output.len(),
        sent.len() + 1,
        "no derived settings or VAD activity may be emitted"
    );
    assert!(matches!(
        output.last().unwrap().frame.data,
        FrameData::End(_)
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_applied_params_change_real_inference_from_forced_speaking_to_quiet() {
    let forced = VadParams {
        confidence: 0.0,
        min_volume: 0.0,
        start_secs: 0.0,
        stop_secs: 0.0,
    };
    let mut harness = Harness::start(forced).await;
    let silence_window = || {
        Frame::new(FrameData::InputAudio(
            pipecat_core::AudioRawFrame::new(vec![0; 1024], 16_000, 1).unwrap(),
        ))
    };
    harness
        .send(silence_window(), Direction::Downstream)
        .await
        .unwrap();
    let mut output = harness
        .receive_until(|frames| {
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::VadUserStartedSpeaking(_)))
                .count()
                == 2
        })
        .await;
    let tag = LocalFrameTag::new();
    let quiet = VadParams {
        confidence: 1.0,
        min_volume: 1.0,
        start_secs: 0.0,
        stop_secs: 0.0,
    };
    let update = marked(
        Frame::new(FrameData::VadParamsUpdate(quiet.clone())),
        0,
        &tag,
    );
    harness
        .send(update.clone(), Direction::Upstream)
        .await
        .unwrap();
    output.extend(
        harness
            .receive_until(|frames| applied_count(frames, &quiet) == 2)
            .await,
    );
    // A complete real 512-sample inference window follows confirmed application.
    // Idle stopping is disabled, so stopped events must come from analysis.
    harness
        .send(silence_window(), Direction::Downstream)
        .await
        .unwrap();
    let (tail, report) = harness.finish().await;
    output.extend(tail);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let original = assert_original(&output, &update, Direction::Upstream, &tag);
    let end = output
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::End(_)))
        .unwrap();
    assert_applied(&output, &quiet, &update, original, end, &tag);
    for direction in [Direction::Downstream, Direction::Upstream] {
        let events: Vec<_> = output
            .iter()
            .enumerate()
            .filter(|(_, f)| f.direction == direction)
            .collect();
        let starts: Vec<_> = events
            .iter()
            .filter(|(_, f)| matches!(f.frame.data, FrameData::VadUserStartedSpeaking(_)))
            .collect();
        let stops: Vec<_> = events
            .iter()
            .filter(|(_, f)| matches!(f.frame.data, FrameData::VadUserStoppedSpeaking(_)))
            .collect();
        assert_eq!(starts.len(), 1);
        assert_eq!(stops.len(), 1);
        assert!(starts[0].0 < original && original < stops[0].0 && stops[0].0 < end);
        if let FrameData::VadUserStoppedSpeaking(speech) = &stops[0].1.frame.data {
            assert_eq!(speech.duration_secs, quiet.stop_secs);
        }
    }
}
