use pipecat_vad::{
    EndOfTurnState, MonotonicSmartTurnClock, SmartTurnAnalyzer, SmartTurnBuffer,
    SmartTurnCancellation, SmartTurnClock, SmartTurnCompletionReason, SmartTurnParams,
    SmartTurnPrediction, SmartTurnPredictor, VadError,
};
use serde_json::Value;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct ManualClock(AtomicU64);
impl ManualClock {
    fn set(&self, seconds: f64) {
        self.0.store(
            Duration::from_secs_f64(seconds).as_nanos() as u64,
            Ordering::SeqCst,
        );
    }
    fn advance(&self, duration: Duration) {
        self.0
            .fetch_add(duration.as_nanos() as u64, Ordering::SeqCst);
    }
}
impl SmartTurnClock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_nanos(self.0.load(Ordering::SeqCst))
    }
}

struct Scripted {
    clock: Arc<ManualClock>,
    outcome: Arc<Mutex<Value>>,
    captured: Arc<Mutex<Option<Vec<i16>>>>,
}
impl SmartTurnPredictor for Scripted {
    fn predict(
        &mut self,
        samples: &[i16],
        _: u32,
        _: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        *self.captured.lock().unwrap() = Some(samples.to_vec());
        if self.outcome.lock().unwrap().as_str() == Some("timeout") {
            return Err(VadError::SmartTurnTimeout);
        }
        self.clock.advance(Duration::from_millis(125));
        let probability = self.outcome.lock().unwrap().as_f64().unwrap() as f32;
        Ok(SmartTurnPrediction {
            probability,
            complete: probability > 0.5,
        })
    }
}
fn params(value: &Value) -> SmartTurnParams {
    SmartTurnParams {
        stop_secs: value["stop_secs"].as_f64().unwrap(),
        pre_speech_ms: value["pre_speech_ms"].as_f64().unwrap(),
        max_duration_secs: value["max_duration_secs"].as_f64().unwrap(),
    }
}
fn state_name(state: EndOfTurnState) -> &'static str {
    match state {
        EndOfTurnState::Complete => "COMPLETE",
        EndOfTurnState::Incomplete => "INCOMPLETE",
    }
}

#[tokio::test]
async fn smart_turn_matches_source_generated_state_and_model_input_traces() {
    let oracle: Value =
        serde_json::from_str(include_str!("fixtures/smart_turn_policy_oracle.json")).unwrap();
    for case in oracle["cases"].as_array().unwrap() {
        let clock = Arc::new(ManualClock::default());
        let outcome = Arc::new(Mutex::new(Value::Null));
        let captured = Arc::new(Mutex::new(None));
        let predictor = Scripted {
            clock: clock.clone(),
            outcome: outcome.clone(),
            captured: captured.clone(),
        };
        let mut analyzer = SmartTurnAnalyzer::with_predictor(
            predictor,
            case["rate"].as_u64().unwrap() as u32,
            params(&case["params"]),
            clock.clone(),
            None,
        )
        .unwrap();
        for (index, step) in case["steps"].as_array().unwrap().iter().enumerate() {
            let label = format!("{} step {index}", case["name"]);
            let event = &step["event"];
            match event["op"].as_str().unwrap() {
                "append" => {
                    clock.set(event["at"].as_f64().unwrap());
                    let samples = vec![
                        event["sample"].as_i64().unwrap() as i16;
                        event["count"].as_u64().unwrap() as usize
                    ];
                    let state = analyzer
                        .append_pcm16(&samples, event["speech"].as_bool().unwrap())
                        .unwrap();
                    assert_eq!(
                        state_name(state),
                        step["state"].as_str().unwrap(),
                        "{label}"
                    );
                }
                "vad_start" => analyzer
                    .update_vad_start_secs(event["seconds"].as_f64().unwrap())
                    .unwrap(),
                "clear" => analyzer.clear(),
                "analyze" => {
                    *outcome.lock().unwrap() = event["probability"].clone();
                    *captured.lock().unwrap() = None;
                    let result = analyzer.analyze_end_of_turn().await.unwrap();
                    assert_eq!(
                        state_name(result.state),
                        step["state"].as_str().unwrap(),
                        "{label}"
                    );
                    let expected = step["model_input"].as_array().map(|runs| {
                        runs.iter()
                            .flat_map(|run| {
                                std::iter::repeat_n(
                                    run[0].as_i64().unwrap() as i16,
                                    run[1].as_u64().unwrap() as usize,
                                )
                            })
                            .collect::<Vec<_>>()
                    });
                    assert_eq!(
                        *captured.lock().unwrap(),
                        expected,
                        "{label}: exact model input"
                    );
                    if step["metrics"].is_null() {
                        assert!(result.metrics.is_none(), "{label}");
                    } else {
                        let metrics = result.metrics.unwrap();
                        assert_eq!(
                            metrics.processor,
                            step["metrics"]["processor"].as_str().unwrap()
                        );
                        assert_eq!(
                            metrics.is_complete,
                            step["metrics"]["is_complete"].as_bool().unwrap()
                        );
                        assert!(
                            (f64::from(metrics.probability)
                                - step["metrics"]["probability"].as_f64().unwrap())
                            .abs()
                                < 1e-6
                        );
                        assert_eq!(
                            metrics.e2e_processing_time_ms,
                            step["metrics"]["e2e_processing_time_ms"].as_f64().unwrap()
                        );
                    }
                }
                other => panic!("unknown trace event {other}"),
            }
            assert_eq!(
                analyzer.buffer().speech_triggered(),
                step["speech_triggered"].as_bool().unwrap(),
                "{label}"
            );
            assert!(
                (analyzer.buffer().silence_ms() - step["silence_ms"].as_f64().unwrap()).abs()
                    < 1e-9,
                "{label}"
            );
            assert_eq!(
                analyzer
                    .buffer()
                    .speech_start()
                    .unwrap_or_default()
                    .as_secs_f64(),
                step["speech_start"].as_f64().unwrap(),
                "{label}"
            );
        }
        analyzer.cleanup().await.unwrap();
    }
}

#[test]
fn smart_turn_bounded_continuous_speech_and_unpaced_prespeech_keep_exact_tail() {
    for is_speech in [false, true] {
        let mut buffer = SmartTurnBuffer::new(16_000, SmartTurnParams::default()).unwrap();
        for index in 0..10_000 {
            let sample = (index % i16::MAX as usize) as i16;
            assert_eq!(
                buffer
                    .append_pcm16_at(&[sample; 320], is_speech, Duration::ZERO)
                    .unwrap(),
                EndOfTurnState::Incomplete
            );
            assert!(buffer.buffered_samples() <= 128_000);
            assert!(buffer.buffered_chunks() <= 400);
        }
        let expected: Vec<_> = (9600..10_000)
            .flat_map(|index| std::iter::repeat_n(index as i16, 320))
            .collect();
        assert_eq!(buffer.analysis_samples(), expected);
    }
}

#[test]
fn smart_turn_large_frame_counts_all_silence_but_retains_only_bounded_tail() {
    let mut buffer = SmartTurnBuffer::new(16_000, SmartTurnParams::default()).unwrap();
    let mut frame = vec![123; 400_000];
    let tail = frame.len() - 128_000;
    frame[tail..].fill(456);
    buffer
        .append_pcm16_at(&frame, true, Duration::ZERO)
        .unwrap();
    assert_eq!(buffer.buffered_samples(), 128_000);
    assert_eq!(buffer.analysis_samples(), vec![456; 128_000]);
    assert_eq!(
        buffer
            .append_pcm16_at(&frame, false, Duration::ZERO)
            .unwrap(),
        EndOfTurnState::Complete
    );
    assert_eq!(buffer.buffered_samples(), 0);
}

#[test]
fn smart_turn_rejects_invalid_parameters_without_mutating_audio() {
    for value in [f64::NAN, f64::INFINITY, -1.0] {
        assert!(SmartTurnBuffer::new(
            16000,
            SmartTurnParams {
                stop_secs: value,
                ..Default::default()
            }
        )
        .is_err());
        assert!(SmartTurnBuffer::new(
            16000,
            SmartTurnParams {
                pre_speech_ms: value,
                ..Default::default()
            }
        )
        .is_err());
        assert!(SmartTurnBuffer::new(
            16000,
            SmartTurnParams {
                max_duration_secs: value,
                ..Default::default()
            }
        )
        .is_err());
    }
    assert!(SmartTurnBuffer::new(0, SmartTurnParams::default()).is_err());
    assert!(SmartTurnBuffer::new(
        16000,
        SmartTurnParams {
            max_duration_secs: 0.0,
            ..Default::default()
        }
    )
    .is_err());
    let mut buffer = SmartTurnBuffer::new(16000, SmartTurnParams::default()).unwrap();
    buffer
        .append_pcm16_at(&[123], true, Duration::from_secs(2))
        .unwrap();
    assert!(buffer
        .append_audio_at(&[1], false, Duration::from_secs(2))
        .is_err());
    assert!(buffer
        .append_pcm16_at(&[2], false, Duration::from_secs(1))
        .is_err());
    assert!(buffer.update_vad_start_secs(f64::NAN).is_err());
    assert_eq!(buffer.analysis_samples(), vec![123]);
    assert_eq!(buffer.silence_ms(), 0.0);
}

#[test]
fn smart_turn_fragmentation_limit_is_explicit_and_atomic() {
    let mut buffer =
        SmartTurnBuffer::with_chunk_limit(16000, SmartTurnParams::default(), 2).unwrap();
    buffer.append_pcm16_at(&[], true, Duration::ZERO).unwrap();
    buffer.append_pcm16_at(&[1], true, Duration::ZERO).unwrap();
    assert!(buffer.append_pcm16_at(&[], false, Duration::ZERO).is_err());
    assert_eq!(buffer.buffered_chunks(), 2);
    assert_eq!(buffer.analysis_samples(), vec![1]);
    buffer
        .append_pcm16_at(&vec![2; 128_000], true, Duration::ZERO)
        .unwrap();
    assert_eq!(buffer.buffered_chunks(), 1);
}

#[test]
fn smart_turn_pcm_bytes_are_little_endian_and_clock_zero_is_valid() {
    let mut buffer = SmartTurnBuffer::new(16000, SmartTurnParams::default()).unwrap();
    buffer
        .append_audio_at(&[0, 128, 255, 127], true, Duration::ZERO)
        .unwrap();
    buffer
        .append_pcm16_at(&[123], true, Duration::from_millis(10))
        .unwrap();
    assert_eq!(buffer.speech_start(), Some(Duration::ZERO));
    assert_eq!(buffer.analysis_samples(), vec![i16::MIN, i16::MAX, 123]);
}

struct Gate {
    calls: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    release: Arc<AtomicUsize>,
    entered: tokio::sync::mpsc::UnboundedSender<usize>,
    drop_flag: Arc<AtomicBool>,
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.drop_flag.store(true, Ordering::SeqCst);
    }
}
impl SmartTurnPredictor for Gate {
    fn predict(
        &mut self,
        _: &[i16],
        _: u32,
        cancellation: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let _ = self.entered.send(call);
        while self.release.load(Ordering::SeqCst) < call && !cancellation.is_cancelled() {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.active.fetch_sub(1, Ordering::SeqCst);
        if cancellation.is_cancelled() {
            Err(VadError::Cancelled)
        } else {
            Ok(SmartTurnPrediction {
                probability: 0.8,
                complete: true,
            })
        }
    }
}
struct Harness {
    analyzer: SmartTurnAnalyzer,
    entered: tokio::sync::mpsc::UnboundedReceiver<usize>,
    calls: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
    release: Arc<AtomicUsize>,
    drop_flag: Arc<AtomicBool>,
}
fn harness() -> Harness {
    let (sender, entered) = tokio::sync::mpsc::unbounded_channel();
    let calls = Arc::new(AtomicUsize::new(0));
    let active = Arc::new(AtomicUsize::new(0));
    let maximum = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(AtomicUsize::new(0));
    let drop_flag = Arc::new(AtomicBool::new(false));
    let predictor = Gate {
        calls: calls.clone(),
        active: active.clone(),
        maximum: maximum.clone(),
        release: release.clone(),
        entered: sender,
        drop_flag: drop_flag.clone(),
    };
    let mut analyzer = SmartTurnAnalyzer::with_predictor(
        predictor,
        16000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap();
    analyzer.append_pcm16(&[1; 160], true).unwrap();
    Harness {
        analyzer,
        entered,
        calls,
        active,
        maximum,
        release,
        drop_flag,
    }
}
async fn entered(harness: &mut Harness) -> usize {
    tokio::time::timeout(Duration::from_secs(5), harness.entered.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn smart_turn_serial_native_owner_has_one_bounded_queued_snapshot() {
    let mut h = harness();
    let first = h.analyzer.begin_analysis().unwrap();
    assert_eq!(entered(&mut h).await, 1);
    let second = h.analyzer.begin_analysis().unwrap();
    assert!(h.analyzer.begin_analysis().is_err());
    second.cancellation().cancel();
    h.release.store(1, Ordering::SeqCst);
    assert_eq!(first.wait().await.unwrap().state, EndOfTurnState::Complete);
    assert!(matches!(second.wait().await, Err(VadError::Cancelled)));
    assert_eq!(h.calls.load(Ordering::SeqCst), 1);
    h.analyzer.cleanup().await.unwrap();
    assert_eq!(h.maximum.load(Ordering::SeqCst), 1);
    assert_eq!(h.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn smart_turn_timeout_completes_without_probability_and_joins_cancelled_run() {
    let mut h = harness();
    h.analyzer
        .set_inference_timeout(Some(Duration::from_millis(20)))
        .unwrap();
    let pending = h.analyzer.begin_analysis().unwrap();
    entered(&mut h).await;
    let result = pending.wait().await.unwrap();
    assert_eq!(result.state, EndOfTurnState::Complete);
    assert_eq!(result.reason, SmartTurnCompletionReason::Timeout);
    assert!(result.metrics.is_none());
    assert!(h.analyzer.apply_analysis(&result));
    assert!(!h.analyzer.buffer().speech_triggered());
    h.analyzer.cleanup().await.unwrap();
    assert_eq!(h.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn smart_turn_cancellation_and_dropped_future_preserve_buffer() {
    let mut h = harness();
    let pending = h.analyzer.begin_analysis().unwrap();
    entered(&mut h).await;
    pending.cancellation().cancel();
    assert!(matches!(pending.wait().await, Err(VadError::Cancelled)));
    assert_eq!(h.analyzer.buffer().buffered_samples(), 160);
    let pending = h.analyzer.begin_analysis().unwrap();
    entered(&mut h).await;
    drop(pending);
    h.analyzer.cleanup().await.unwrap();
    assert_eq!(h.active.load(Ordering::SeqCst), 0);
    assert_eq!(h.analyzer.buffer().buffered_samples(), 160);
}

#[tokio::test]
async fn smart_turn_cleanup_preserves_state_and_reuses_owned_model() {
    let mut h = harness();
    let pending = h.analyzer.begin_analysis().unwrap();
    entered(&mut h).await;
    h.analyzer.cleanup().await.unwrap();
    assert!(matches!(pending.wait().await, Err(VadError::Cancelled)));
    assert_eq!(h.analyzer.buffer().buffered_samples(), 160);
    h.release.store(2, Ordering::SeqCst);
    let result = h.analyzer.analyze_end_of_turn().await.unwrap();
    assert_eq!(result.state, EndOfTurnState::Complete);
    assert_eq!(h.calls.load(Ordering::SeqCst), 2);
    assert_eq!(h.maximum.load(Ordering::SeqCst), 1);
    assert!(!h.drop_flag.load(Ordering::SeqCst));
    h.analyzer.cleanup().await.unwrap();
    drop(h.analyzer);
    assert!(h.drop_flag.load(Ordering::SeqCst));
}

#[tokio::test]
async fn smart_turn_resumed_speech_and_external_clear_reject_stale_completions() {
    for clear in [false, true] {
        let mut h = harness();
        h.release.store(1, Ordering::SeqCst);
        let result = h.analyzer.begin_analysis().unwrap().wait().await.unwrap();
        if clear {
            h.analyzer.clear();
        }
        h.analyzer.append_pcm16(&[2; 160], true).unwrap();
        assert!(!h.analyzer.apply_analysis(&result));
        assert!(h.analyzer.buffer().speech_triggered());
        assert_eq!(
            h.analyzer.buffer().buffered_samples(),
            if clear { 160 } else { 320 }
        );
        h.analyzer.cleanup().await.unwrap();
    }
}

#[tokio::test]
async fn smart_turn_resumed_speech_cancels_running_inference() {
    let mut h = harness();
    let pending = h.analyzer.begin_analysis().unwrap();
    entered(&mut h).await;
    h.analyzer.append_pcm16(&[2; 160], true).unwrap();
    assert!(matches!(pending.wait().await, Err(VadError::Cancelled)));
    assert!(h.analyzer.buffer().speech_triggered());
    h.analyzer.cleanup().await.unwrap();
}

#[tokio::test]
async fn smart_turn_new_silence_does_not_invalidate_model_result() {
    let mut h = harness();
    let pending = h.analyzer.begin_analysis().unwrap();
    entered(&mut h).await;
    h.analyzer.append_pcm16(&[0; 160], false).unwrap();
    h.release.store(1, Ordering::SeqCst);
    let result = pending.wait().await.unwrap();
    assert!(h.analyzer.apply_analysis(&result));
    assert_eq!(h.analyzer.buffer().buffered_samples(), 0);
    h.analyzer.cleanup().await.unwrap();
}

struct Failing(bool);
impl SmartTurnPredictor for Failing {
    fn predict(
        &mut self,
        _: &[i16],
        _: u32,
        _: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        if std::mem::replace(&mut self.0, false) {
            Err(VadError::Model("bad graph".into()))
        } else {
            Ok(SmartTurnPrediction {
                probability: 0.1,
                complete: false,
            })
        }
    }
}
#[tokio::test]
async fn smart_turn_model_error_is_not_a_completion_or_synthetic_prediction() {
    let mut analyzer = SmartTurnAnalyzer::with_predictor(
        Failing(true),
        16000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap();
    analyzer.append_pcm16(&[1; 160], true).unwrap();
    assert!(matches!(
        analyzer.analyze_end_of_turn().await,
        Err(VadError::Model(_))
    ));
    assert!(analyzer.buffer().speech_triggered());
    let result = analyzer.analyze_end_of_turn().await.unwrap();
    assert_eq!(result.state, EndOfTurnState::Incomplete);
    assert_eq!(analyzer.buffer().buffered_samples(), 160);
    analyzer.cleanup().await.unwrap();
}

struct DelayedCancellation {
    finish: Arc<AtomicBool>,
    entered: tokio::sync::mpsc::UnboundedSender<()>,
}
impl SmartTurnPredictor for DelayedCancellation {
    fn predict(
        &mut self,
        _: &[i16],
        _: u32,
        _: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        let _ = self.entered.send(());
        // Simulate a bounded native operation that acknowledges cancellation
        // only after returning, as preprocessing does before an ONNX run.
        while !self.finish.load(Ordering::SeqCst) {
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(SmartTurnPrediction {
            probability: 0.1,
            complete: false,
        })
    }
}
#[tokio::test]
async fn smart_turn_cancelled_cleanup_still_joins_owner_before_reuse() {
    let finish = Arc::new(AtomicBool::new(false));
    let (entered, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut analyzer = SmartTurnAnalyzer::with_predictor(
        DelayedCancellation {
            finish: finish.clone(),
            entered,
        },
        16000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap();
    analyzer.append_pcm16(&[1; 160], true).unwrap();
    let pending = analyzer.begin_analysis().unwrap();
    receiver.recv().await.unwrap();
    let timed_out = tokio::time::timeout(Duration::from_millis(10), analyzer.cleanup())
        .await
        .is_err();
    let overlap_rejected = analyzer.begin_analysis().is_err();
    finish.store(true, Ordering::SeqCst);
    assert!(timed_out);
    assert!(overlap_rejected);
    assert!(matches!(pending.wait().await, Err(VadError::Cancelled)));
    let next = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(pending) = analyzer.begin_analysis() {
                break pending;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(next.wait().await.unwrap().state, EndOfTurnState::Incomplete);
    analyzer.cleanup().await.unwrap();
}

#[tokio::test]
async fn smart_turn_explicit_cancellation_wins_over_timeout() {
    let finish = Arc::new(AtomicBool::new(false));
    let (entered, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let mut analyzer = SmartTurnAnalyzer::with_predictor(
        DelayedCancellation {
            finish: finish.clone(),
            entered,
        },
        16000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        Some(Duration::from_millis(20)),
    )
    .unwrap();
    analyzer.append_pcm16(&[1; 160], true).unwrap();
    let pending = analyzer.begin_analysis().unwrap();
    receiver.recv().await.unwrap();
    pending.cancellation().cancel();
    let result = pending.wait().await;
    finish.store(true, Ordering::SeqCst);
    analyzer.cleanup().await.unwrap();
    assert!(matches!(result, Err(VadError::Cancelled)));
    assert!(analyzer.buffer().speech_triggered());
}

#[tokio::test(start_paused = true)]
async fn smart_turn_empty_analysis_cannot_time_out_into_phantom_completion() {
    let mut h = harness();
    h.analyzer.clear();
    h.analyzer
        .set_inference_timeout(Some(Duration::from_nanos(1)))
        .unwrap();
    let pending = h.analyzer.begin_analysis().unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    let result = pending.wait().await.unwrap();
    assert_eq!(result.state, EndOfTurnState::Incomplete);
    assert_eq!(result.reason, SmartTurnCompletionReason::Empty);
    assert_eq!(h.calls.load(Ordering::SeqCst), 0);
    h.analyzer.cleanup().await.unwrap();
}
