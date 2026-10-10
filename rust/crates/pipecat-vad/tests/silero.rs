use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, Pipeline, PipelineConfig, VadParams, WorkerState,
};
use pipecat_vad::{
    AudioVolumeTracker, ProcessorOptions, RuntimeOptions, SileroModel, SileroVadAnalyzer,
    SileroVadProcessor, VadState, VadStateMachine,
};
use std::{path::PathBuf, sync::Arc, time::Duration};

fn runtime() -> RuntimeOptions {
    RuntimeOptions {
        library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0")),
    }
}
fn model() -> SileroModel {
    let path = std::env::var_os("PIPECAT_SILERO_MODEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
        });
    SileroModel::load(&runtime(), path)
        .expect("real ONNX runtime/model required; see pipecat-vad README")
}
fn decode(bytes: &[u8]) -> Vec<i16> {
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|&b| i16::from_le_bytes(b))
        .collect()
}
fn oracle() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/silero_oracle.json")).unwrap()
}
fn analyzer(params: VadParams) -> SileroVadAnalyzer {
    SileroVadAnalyzer::new(model(), 16000, params).unwrap()
}

#[test]
fn model_outputs_match_python_reference_speech_tone_silence_both_rates() {
    let speech = decode(include_bytes!("fixtures/smart_turn_speech.pcm16"));
    let tone = decode(include_bytes!("fixtures/smart_turn_tone.pcm16"));
    let speech8: Vec<_> = speech.iter().step_by(2).copied().collect();
    let cases = [
        ("speech16", speech, 16000),
        ("tone16", tone, 16000),
        ("silence16", vec![0; 512 * 8], 16000),
        ("silence8", vec![0; 256 * 8], 8000),
        ("speech8", speech8, 8000),
    ];
    let mut model = model();
    let expected = oracle();
    for (name, samples, rate) in cases {
        model.reset();
        let size = SileroModel::required_samples(rate).unwrap();
        let chunks = samples.chunks_exact(size);
        let references = expected["cases"][name].as_array().unwrap();
        assert_eq!(chunks.len(), references.len());
        for (i, (chunk, value)) in chunks.zip(references).enumerate() {
            let actual = model.predict_pcm16(chunk, rate).unwrap();
            let expected = value.as_f64().unwrap() as f32;
            assert!(
                (actual - expected).abs() < 2e-6,
                "{name} window{i}: {actual} != {expected}"
            );
        }
    }
}
#[test]
fn sample_rate_change_and_explicit_reset_clear_recurrent_and_context_state() {
    let mut model = model();
    let first = model.predict_pcm16(&vec![0; 512], 16000).unwrap();
    let second = model.predict_pcm16(&vec![0; 512], 16000).unwrap();
    assert_ne!(
        first, second,
        "recurrent state should affect silence outputs"
    );
    model.reset();
    assert_eq!(first, model.predict_pcm16(&vec![0; 512], 16000).unwrap());
    model.predict_pcm16(&vec![8000; 256], 8000).unwrap();
    assert_eq!(first, model.predict_pcm16(&vec![0; 512], 16000).unwrap());
}
#[test]
fn model_validates_rate_size_finite_values_and_runtime_path() {
    let mut model = model();
    for rate in [0, 11025, 24000, 48000] {
        assert!(model.predict_pcm16(&vec![0; 512], rate).is_err());
    }
    for count in [0, 255, 256, 511, 513, 1024] {
        assert!(model.predict_pcm16(&vec![0; count], 16000).is_err());
    }
    assert!(model.predict(&vec![f32::NAN; 512], 16000).is_err());
    assert!(SileroModel::load(
        &RuntimeOptions {
            library_path: PathBuf::from("/missing-onnx-runtime.so")
        },
        "missing-model.onnx"
    )
    .is_err());
    assert!(SileroModel::load(&runtime(), "missing-model.onnx").is_err());
}
#[test]
fn volume_matches_loudness_python_histogram_reference() {
    for case in oracle()["volume"].as_array().unwrap() {
        let rate = case["rate"].as_u64().unwrap() as u32;
        let amplitude = case["amplitude"].as_f64().unwrap();
        let samples: Vec<_> = (0..rate * 2 / 5)
            .map(|i| {
                ((2.0 * std::f64::consts::PI * f64::from(i) * 440.0 / f64::from(rate)).sin()
                    * amplitude) as i16
            })
            .collect();
        let mut tracker = AudioVolumeTracker::new(rate).unwrap();
        tracker.update(&samples[..samples.len() - 1]);
        assert_eq!(tracker.volume(), 0.0);
        tracker.update(&samples[samples.len() - 1..]);
        let expected = case["volume"].as_f64().unwrap();
        assert!(
            (tracker.volume() - expected).abs() < 1e-10,
            "rate{rate}, amplitude{amplitude}: {} != {expected}",
            tracker.volume()
        );
        tracker.update(&vec![0; rate as usize]);
        assert_eq!(tracker.buffered_samples(), (rate * 2 / 5) as usize);
        assert_eq!(tracker.volume(), 0.0);
    }
}
#[test]
fn hysteresis_debounces_and_recovers_from_partial_transitions() {
    let mut state = VadStateMachine::new(VadParams {
        start_secs: 0.064,
        stop_secs: 0.064,
        ..Default::default()
    })
    .unwrap();
    state.update(0.8, 0.8);
    assert_eq!(state.finish_batch(), VadState::Starting);
    state.update(0.1, 0.8);
    assert_eq!(state.finish_batch(), VadState::Quiet);
    state.update(0.8, 0.8);
    state.update(0.8, 0.8);
    assert_eq!(state.finish_batch(), VadState::Speaking);
    state.update(0.1, 0.8);
    assert_eq!(state.finish_batch(), VadState::Stopping);
    state.update(0.8, 0.8);
    assert_eq!(state.finish_batch(), VadState::Speaking);
    state.update(0.1, 0.8);
    state.update(0.1, 0.8);
    assert_eq!(state.finish_batch(), VadState::Quiet);
    state.update(1.0, 0.5);
    assert_eq!(state.finish_batch(), VadState::Quiet);
}
#[test]
fn rounding_matches_python_ties_even_and_batch_confirmation() {
    let mut state = VadStateMachine::new(VadParams {
        start_secs: 0.080,
        stop_secs: 0.016,
        ..Default::default()
    })
    .unwrap();
    state.update(1.0, 1.0);
    state.update(1.0, 1.0);
    assert_eq!(state.finish_batch(), VadState::Speaking);
    state.update(0.0, 0.0);
    assert_eq!(state.finish_batch(), VadState::Quiet);
    // Promotions happen after a complete input batch, not between its windows.
    state.update(1.0, 1.0);
    state.update(1.0, 1.0);
    state.update(0.0, 0.0);
    assert_eq!(state.finish_batch(), VadState::Quiet);
}
#[test]
fn analyzer_preserves_byte_alignment_and_resets_after_five_seconds() {
    let mut analyzer = analyzer(VadParams::default());
    assert_eq!(
        analyzer
            .analyze_at(&vec![0; 1023], Duration::ZERO)
            .unwrap()
            .windows,
        0
    );
    assert_eq!(analyzer.pending_bytes(), 1023);
    assert_eq!(
        analyzer.analyze_at(&[0], Duration::ZERO).unwrap().windows,
        1
    );
    assert_eq!(analyzer.model_reset_count(), 1);
    let first = analyzer
        .analyze_at(&vec![0; 1024], Duration::from_millis(4999))
        .unwrap()
        .confidence;
    assert_eq!(analyzer.model_reset_count(), 1);
    analyzer
        .analyze_at(&vec![0; 1024], Duration::from_secs(5))
        .unwrap();
    assert_eq!(analyzer.model_reset_count(), 2);
    let after_reset = analyzer
        .analyze_at(&vec![0; 1024], Duration::from_millis(5001))
        .unwrap()
        .confidence;
    assert_eq!(first, after_reset);
    let result = analyzer
        .analyze_at(&vec![0; 1024 * 3 + 7], Duration::from_secs(6))
        .unwrap();
    assert_eq!(result.windows, 3);
    assert_eq!(analyzer.pending_bytes(), 7);
    assert_eq!(result.state, VadState::Quiet);
    analyzer.reset();
    assert_eq!(analyzer.pending_bytes(), 0);
}
#[test]
fn analyzer_parameter_update_preserves_volume_and_partial_audio() {
    let mut analyzer = analyzer(VadParams::default());
    let tone = include_bytes!("fixtures/smart_turn_tone.pcm16");
    let result = analyzer.analyze_at(tone, Duration::ZERO).unwrap();
    assert!(result.volume > 0.6);
    let pending = analyzer.pending_bytes();
    analyzer
        .set_params(VadParams {
            confidence: 0.4,
            ..Default::default()
        })
        .unwrap();
    assert_eq!(analyzer.pending_bytes(), pending);
    assert_eq!(
        analyzer.analyze_at(&[], Duration::ZERO).unwrap().volume,
        result.volume
    );
}
#[test]
fn params_and_processor_options_reject_invalid_values() {
    for value in [f64::NAN, f64::INFINITY, -0.1, 1.1] {
        assert!(VadStateMachine::new(VadParams {
            confidence: value,
            ..Default::default()
        })
        .is_err());
        assert!(VadStateMachine::new(VadParams {
            min_volume: value,
            ..Default::default()
        })
        .is_err());
    }
    assert!(VadStateMachine::new(VadParams {
        start_secs: -1.0,
        ..Default::default()
    })
    .is_err());
    assert!(SileroVadProcessor::new(
        analyzer(VadParams::default()),
        ProcessorOptions {
            queue_capacity: 0,
            ..Default::default()
        }
    )
    .is_err());
}
fn forced_params() -> VadParams {
    VadParams {
        confidence: 0.0,
        min_volume: 0.0,
        start_secs: 0.0,
        stop_secs: 0.0,
    }
}
fn audio() -> Frame {
    Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![0; 1024], 16000, 1).unwrap(),
    ))
}
async fn wait_for(
    worker: &mut pipecat_core::PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> Vec<FrameData> {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut output = Vec::new();
        loop {
            let item = worker.recv().await.expect("pipeline closed");
            let matches = item.direction == Direction::Downstream && predicate(&item.frame.data);
            output.push(item.frame.data);
            if matches {
                return output;
            }
        }
    })
    .await
    .expect("VAD result timed out")
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processor_forwards_audio_then_emits_speech_activity_and_idle_stop() {
    let processor = Arc::new(
        SileroVadProcessor::new(
            analyzer(forced_params()),
            ProcessorOptions {
                audio_idle_timeout: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    wait_for(&mut worker, |f| matches!(f, FrameData::Start(_))).await;
    worker
        .sender()
        .send(audio(), Direction::Downstream)
        .await
        .unwrap();
    let output = wait_for(&mut worker, |f| {
        matches!(f, FrameData::VadUserStartedSpeaking(_))
    })
    .await;
    assert!(output.iter().any(|f| matches!(f, FrameData::InputAudio(_))));
    let output = wait_for(&mut worker, |f| {
        matches!(f, FrameData::VadUserStoppedSpeaking(_))
    })
    .await;
    assert!(output.iter().any(|f| matches!(f, FrameData::UserSpeaking)));
    worker
        .sender()
        .send(audio(), Direction::Downstream)
        .await
        .unwrap();
    wait_for(&mut worker, |f| {
        matches!(f, FrameData::VadUserStartedSpeaking(_))
    })
    .await;
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(10), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(!processor.is_running());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_drains_accepted_audio_and_cancel_joins_owned_thread() {
    for cancel in [false, true] {
        let processor = Arc::new(
            SileroVadProcessor::new(
                analyzer(forced_params()),
                ProcessorOptions {
                    queue_capacity: 64,
                    ..Default::default()
                },
            )
            .unwrap(),
        );
        let mut worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        wait_for(&mut worker, |f| matches!(f, FrameData::Start(_))).await;
        for _ in 0..10 {
            worker
                .sender()
                .send(audio(), Direction::Downstream)
                .await
                .unwrap();
        }
        if cancel {
            worker.sender().cancel("test").await.unwrap();
        } else {
            worker.end().await.unwrap();
        }
        let (frames, report) = tokio::time::timeout(Duration::from_secs(10), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            report.state,
            if cancel {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        assert!(!processor.is_running());
        if !cancel {
            assert!(frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::VadUserStartedSpeaking(_))));
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_pipeline_rate_fails_setup_without_leaking_thread() {
    let processor = Arc::new(
        SileroVadProcessor::new(analyzer(VadParams::default()), ProcessorOptions::default())
            .unwrap(),
    );
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 48000;
    let worker = Pipeline::new(vec![processor.clone()])
        .start(config)
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(10), worker.collect())
        .await
        .unwrap();
    assert!(result.is_err());
    assert!(!processor.is_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn processor_rejects_invalid_audio_and_applies_parameter_updates() {
    let processor = Arc::new(
        SileroVadProcessor::new(analyzer(forced_params()), ProcessorOptions::default()).unwrap(),
    );
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    wait_for(&mut worker, |f| matches!(f, FrameData::Start(_))).await;
    let sender = worker.sender();
    let invalid = [
        AudioRawFrame {
            audio: vec![0; 3],
            sample_rate: 16000,
            num_channels: 1,
            context_id: None,
        },
        AudioRawFrame::new(vec![0; 1024], 16000, 2).unwrap(),
        AudioRawFrame::new(vec![0; 1024], 8000, 1).unwrap(),
        AudioRawFrame::new(vec![0; 32002], 16000, 1).unwrap(),
    ];
    for frame in invalid {
        sender
            .send(
                Frame::new(FrameData::InputAudio(frame)),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    sender.send(audio(), Direction::Downstream).await.unwrap();
    let before = wait_for(&mut worker, |f| {
        matches!(f, FrameData::VadUserStartedSpeaking(_))
    })
    .await;
    assert_eq!(
        before
            .iter()
            .filter(|f| matches!(f, FrameData::Error(_)))
            .count(),
        4
    );
    sender
        .send(
            Frame::new(FrameData::VadParamsUpdate(VadParams {
                confidence: 1.0,
                min_volume: 1.0,
                start_secs: 0.0,
                stop_secs: 0.0,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    sender.send(audio(), Direction::Downstream).await.unwrap();
    let updated = wait_for(&mut worker, |f| {
        matches!(f, FrameData::VadUserStoppedSpeaking(_))
    })
    .await;
    assert!(updated.iter().any(|f| matches!(f, FrameData::SpeechControlParams(p) if p.vad_params.as_ref().is_some_and(|p| p.confidence == 1.0))));
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.errors.len(), 4);
    assert!(!processor.is_running());
}

#[test]
fn real_speech_with_default_gates_enters_and_leaves_speaking() {
    let mut analyzer = analyzer(VadParams::default());
    let mut observed_speech = false;
    for (index, chunk) in include_bytes!("fixtures/smart_turn_speech.pcm16")
        .chunks(1024)
        .enumerate()
    {
        let result = analyzer
            .analyze_at(chunk, Duration::from_millis(index as u64 * 32))
            .unwrap();
        observed_speech |= result.state == VadState::Speaking;
    }
    assert!(
        observed_speech,
        "real speech must pass both the actual model and loudness gate"
    );
    for index in 0..60 {
        analyzer
            .analyze_at(&[0; 1024], Duration::from_millis(6000 + index * 32))
            .unwrap();
    }
    assert_eq!(analyzer.state(), VadState::Quiet);
}

#[test]
fn enormous_queue_capacity_is_an_error_instead_of_a_setup_panic() {
    assert!(SileroVadProcessor::new(
        analyzer(VadParams::default()),
        ProcessorOptions {
            queue_capacity: usize::MAX,
            ..Default::default()
        }
    )
    .is_err());
}

#[test]
fn volume_rejects_invalid_or_unsupported_rates() {
    for rate in [0, 1, 2, 7999, 16001, 48000, u32::MAX] {
        assert!(AudioVolumeTracker::new(rate).is_err());
    }
}

struct DetailedStart;
#[pipecat_core::async_trait]
impl pipecat_core::FrameProcessor for DetailedStart {
    fn name(&self) -> &str {
        "DetailedStart"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> Result<(), pipecat_core::ProcessorError> {
        if let FrameData::Start(configuration) = &frame.data {
            frame.data = FrameData::StartDetailed(pipecat_core::DetailedStartFrame {
                configuration: configuration.clone(),
                enable_tracing: true,
                tracing_context: Some(pipecat_core::TracingContext {
                    conversation_id: Some("vad-conversation".into()),
                    ..Default::default()
                }),
            });
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detailed_start_broadcasts_configured_params_and_keeps_start_payload() {
    let params = forced_params();
    let processor = Arc::new(
        SileroVadProcessor::new(
            SileroVadAnalyzer::new(model(), 8000, params.clone()).unwrap(),
            ProcessorOptions::default(),
        )
        .unwrap(),
    );
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 8000;
    config.start_metadata.insert("session".into(), "vad".into());
    let mut worker = Pipeline::new(vec![Arc::new(DetailedStart), processor.clone()])
        .start(config)
        .await
        .unwrap();
    let mut directions = vec![];
    let mut started = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        while directions.len() < 2 {
            let directed = worker.recv().await.unwrap();
            match directed.frame.data {
                FrameData::StartDetailed(start) => {
                    assert_eq!(directed.direction, Direction::Downstream);
                    assert_eq!(start.configuration.audio_in_sample_rate, 8000);
                    assert!(start.enable_tracing);
                    assert_eq!(
                        start.tracing_context.unwrap().conversation_id.as_deref(),
                        Some("vad-conversation")
                    );
                    assert_eq!(directed.frame.metadata["session"], "vad");
                    started = true;
                }
                FrameData::SpeechControlParams(control) => {
                    assert!(started, "Start must precede settings at the output");
                    assert_eq!(control.vad_params, Some(params.clone()));
                    directions.push(directed.direction);
                }
                other => panic!("unexpected frame {other:?}"),
            }
        }
    })
    .await
    .expect("detailed Start must broadcast VAD settings in both directions");
    assert!(directions.contains(&Direction::Downstream));
    assert!(directions.contains(&Direction::Upstream));
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![0; 512], 8000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::VadUserStartedSpeaking(_))));
    assert!(!processor.is_running());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detailed_end_and_stop_drain_audio_and_parameter_updates_before_terminal() {
    for data in [
        FrameData::EndDetailed(pipecat_core::ReasonFrame {
            reason: Some(serde_json::json!({"kind": "completed", "turn": 3})),
        }),
        FrameData::Stop,
    ] {
        let processor = Arc::new(
            SileroVadProcessor::new(analyzer(forced_params()), ProcessorOptions::default())
                .unwrap(),
        );
        let mut worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        wait_for(&mut worker, |f| matches!(f, FrameData::Start(_))).await;
        let updated = VadParams {
            start_secs: 0.032,
            ..forced_params()
        };
        worker
            .sender()
            .send(
                Frame::new(FrameData::VadParamsUpdate(updated.clone())),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let applied = wait_for(&mut worker, |frame| {
            matches!(frame, FrameData::SpeechControlParams(control)
                if control.vad_params == Some(updated.clone()))
        })
        .await;
        worker
            .sender()
            .send(audio(), Direction::Downstream)
            .await
            .unwrap();
        let final_params = VadParams {
            stop_secs: 0.12,
            ..updated.clone()
        };
        worker
            .sender()
            .send(
                Frame::new(FrameData::VadParamsUpdate(final_params.clone())),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let mut terminal = Frame::new(data.clone());
        terminal.metadata.insert("session".into(), "vad".into());
        worker
            .sender()
            .send(terminal.clone(), Direction::Downstream)
            .await
            .unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            report.state,
            if matches!(data, FrameData::Stop) {
                WorkerState::Stopped
            } else {
                WorkerState::Ended
            }
        );
        assert!(!processor.is_running());
        assert!(applied.iter().any(|f| matches!(f,
            FrameData::SpeechControlParams(control) if control.vad_params == Some(updated.clone()))));
        assert!(frames.iter().any(|f| matches!(&f.frame.data,
            FrameData::VadUserStartedSpeaking(speech) if speech.duration_secs == updated.start_secs)), "{frames:?}");
        assert!(frames.iter().any(|f| matches!(&f.frame.data,
            FrameData::SpeechControlParams(control) if control.vad_params == Some(final_params.clone()))));
        assert_eq!(frames.last().unwrap().frame, terminal);
        pipecat_core::FrameProcessor::cleanup(&*processor)
            .await
            .unwrap();
    }
}

struct HoldCancel(Arc<tokio::sync::Notify>);
#[pipecat_core::async_trait]
impl pipecat_core::FrameProcessor for HoldCancel {
    fn name(&self) -> &str {
        "HoldCancel"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> Result<(), pipecat_core::ProcessorError> {
        if frame.data.is_cancel() {
            self.0.notified().await;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detailed_cancel_stops_idle_native_thread_before_downstream_cleanup() {
    let processor = Arc::new(
        SileroVadProcessor::new(analyzer(forced_params()), ProcessorOptions::default()).unwrap(),
    );
    let release = Arc::new(tokio::sync::Notify::new());
    let mut worker = Pipeline::new(vec![
        processor.clone(),
        Arc::new(HoldCancel(release.clone())),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    worker
        .sender()
        .send(audio(), Direction::Downstream)
        .await
        .unwrap();
    wait_for(&mut worker, |f| matches!(f, FrameData::UserSpeaking)).await;
    assert!(processor.is_running());
    let cancel = Frame::new(FrameData::CancelDetailed(pipecat_core::ReasonFrame {
        reason: Some(serde_json::json!({"kind": "caller_disconnected"})),
    }));
    worker
        .sender()
        .send(cancel.clone(), Direction::Downstream)
        .await
        .unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(2), async {
        while processor.is_running() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .is_ok();
    assert!(!worker.state().is_finished());
    release.notify_one();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        stopped,
        "CancelDetailed must release the native worker immediately"
    );
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(frames.last().unwrap().frame, cancel);
    assert!(!processor.is_running());
}
