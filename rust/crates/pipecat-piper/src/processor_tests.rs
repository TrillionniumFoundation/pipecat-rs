//! Reconstructed regression cases; these require fresh qualification after recovery.
use crate::service::tests::{admission_probe, fake};
use crate::{PiperError, PiperProcessorOptions, PiperService, PiperSettings, PiperTtsProcessor};
use pipecat_core::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

fn config() -> PipelineConfig {
    let mut config = PipelineConfig::default();
    config.start.audio_out_sample_rate = 16000;
    config.shutdown_timeout = Duration::from_secs(3);
    config
}
fn processor(service: PiperService) -> Arc<PiperTtsProcessor> {
    Arc::new(
        PiperTtsProcessor::new(
            service,
            PiperSettings::default(),
            PiperProcessorOptions::default(),
        )
        .unwrap(),
    )
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn collect(worker: PipelineWorker) -> Vec<DirectedFrame> {
    tokio::time::timeout(Duration::from_secs(4), worker.collect())
        .await
        .unwrap()
        .unwrap()
        .0
}
#[tokio::test]
async fn source_channels_contexts_and_unreported_completion_are_preserved() {
    let service = fake(4, |text, _, _, _| {
        assert_eq!(text, "one dollar");
        Ok([1, 0].repeat(5))
    })
    .await;
    let p = processor(service);
    let worker = Pipeline::new(vec![p]).start(config()).await.unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    let mut frame = Frame::new(FrameData::TtsTextInput(TtsTextInputFrame {
        synthesis_text: "one dollar".into(),
        display_text: "$1".into(),
        original_text: "<b>$1</b>".into(),
        append_to_context: true,
        includes_inter_frame_spaces: false,
        aggregated_by: "sentence".into(),
        skip_tts: false,
    }));
    frame.transport_destination = Some("left".into());
    let source_id = frame.id;
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    let frames = collect(worker).await;
    let mut context = None;
    let mut diagnostics = 0;
    for output in frames {
        match output.frame.data {
            FrameData::TtsStarted(start) => context = start.context_id,
            FrameData::TtsAudio(audio) => {
                assert_eq!(audio.context_id, context);
                assert_eq!(audio.audio, [1, 0].repeat(5));
                assert_eq!(output.frame.transport_destination.as_deref(), Some("left"));
                assert_eq!(output.frame.metadata["source_frame_id"], source_id);
            }
            FrameData::TtsAlignmentDiagnostic(diagnostic) => {
                assert_eq!(diagnostic.evidence, TtsAlignmentEvidence::Unreported);
                assert_eq!(diagnostic.synthesis_outcome, TtsSynthesisOutcome::Succeeded);
                assert_eq!(diagnostic.synthesis_text, "one dollar");
                assert_eq!(diagnostic.original_text.as_deref(), Some("<b>$1</b>"));
                diagnostics += 1;
            }
            FrameData::TtsText(_) => panic!("synthesis is not spoken text"),
            FrameData::Error(error) => panic!("{}", error.error),
            _ => {}
        }
    }
    assert_eq!(diagnostics, 1);
}
#[tokio::test]
async fn overlapping_response_starts_cannot_overtake_previous_audio_or_end() {
    let service = fake(4, |text, _, _, _| {
        if text == "first" {
            std::thread::sleep(Duration::from_millis(20));
        }
        Ok([1, 0].repeat(5))
    })
    .await;
    let worker = Pipeline::new(vec![processor(service)])
        .start(config())
        .await
        .unwrap();
    for text in ["first", "second"] {
        send(&worker, FrameData::LlmResponseStart).await;
        send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new(text))).await;
        send(&worker, FrameData::LlmResponseEnd).await;
    }
    worker.end().await.unwrap();
    let order: Vec<_> = collect(worker)
        .await
        .into_iter()
        .filter_map(|out| match out.frame.data {
            FrameData::LlmResponseStart => Some("start"),
            FrameData::TtsAudio(_) => Some("audio"),
            FrameData::LlmResponseEnd => Some("end"),
            _ => None,
        })
        .collect();
    assert_eq!(order, ["start", "audio", "end", "start", "audio", "end"]);
}
#[tokio::test]
async fn invalid_local_requests_do_not_consume_zero_audio_budget() {
    let called = Arc::new(AtomicUsize::new(0));
    let backend_called = called.clone();
    let service = fake(4, move |_, _, _, _| {
        backend_called.fetch_add(1, Ordering::SeqCst);
        Ok(vec![1, 0])
    })
    .await;
    let p = processor(service);
    let health = p.health().unwrap();
    let worker = Pipeline::new(vec![p]).start(config()).await.unwrap();
    for _ in 0..3 {
        send(
            &worker,
            FrameData::TtsSpeak(TtsSpeakFrame::new("bad\0input")),
        )
        .await;
    }
    worker.end().await.unwrap();
    let frames = collect(worker).await;
    assert_eq!(called.load(Ordering::SeqCst), 0);
    assert!(health.is_usable());
    assert_eq!(
        frames
            .iter()
            .filter(|out| matches!(out.frame.data, FrameData::Error(_)))
            .count(),
        3
    );
}
#[tokio::test]
async fn settings_are_snapshotted_and_model_changes_fail_without_restoring_health() {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let captured = calls.clone();
    let service = fake(4, move |_, settings, _, _| {
        captured.lock().unwrap().push(settings.clone());
        Ok(vec![1, 0])
    })
    .await;
    let p = processor(service);
    let health = p.health().unwrap();
    health.set_usable(false);
    assert!(p
        .update_settings(&TtsSettings {
            voice: Setting::Value("other".into()),
            ..Default::default()
        })
        .is_err());
    assert!(!health.is_usable());
    assert!(!p.update_settings(&TtsSettings::default()).unwrap());
    assert!(!health.is_usable());
    let mut delta = TtsSettings::default();
    delta.base.extra.insert("noise_scale".into(), 0.0.into());
    assert!(p.update_settings(&delta).unwrap());
    assert!(health.is_usable());
    let worker = Pipeline::new(vec![p]).start(config()).await.unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("hello"))).await;
    worker.end().await.unwrap();
    collect(worker).await;
    assert_eq!(calls.lock().unwrap()[0].noise_scale, Some(0.0));
}
#[tokio::test]
async fn cancel_joins_active_native_request() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered_tx = Some(entered_tx);
    let service = fake(4, move |_, _, cancel, stop| {
        if let Some(tx) = entered_tx.take() {
            let _ = tx.send(());
        }
        while !cancel.is_cancelled() && !stop.is_cancelled() {
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(PiperError::Cancelled)
    })
    .await;
    let probe = admission_probe(&service);
    let worker = Pipeline::new(vec![processor(service)])
        .start(config())
        .await
        .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("long"))).await;
    entered_rx.await.unwrap();
    worker.cancel("test").await.unwrap();
    let frames = collect(worker).await;
    assert!(!frames
        .iter()
        .any(|out| matches!(out.frame.data, FrameData::TtsAudio(_))));
    assert_eq!(probe.available(), 4);
}

struct NoopObserver;
#[async_trait]
impl PipelineObserver for NoopObserver {
    async fn on_event(&self, _: ObservationEvent) {}
}

struct RecoveryClock {
    health: ProcessorHealth,
    armed: AtomicBool,
    calls: AtomicUsize,
    flipped: AtomicBool,
    entered: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}
impl ObserverClock for RecoveryClock {
    fn now_ns(&self) -> u64 {
        let count = if self.armed.load(Ordering::SeqCst) {
            self.calls.fetch_add(1, Ordering::SeqCst) + 1
        } else {
            0
        };
        if count == 2 && self.armed.swap(false, Ordering::SeqCst) {
            if let Some(entered) = self.entered.lock().unwrap().take() {
                // Test-only scheduling barrier; the native OS thread can enter
                // while this deliberately adversarial observer clock is blocked.
                entered.recv_timeout(Duration::from_secs(2)).unwrap();
            }
            self.flipped.store(true, Ordering::SeqCst);
            self.health.set_usable(false);
            self.health.set_usable(true);
        }
        count as u64
    }
}
fn clock(
    health: ProcessorHealth,
    entered: Option<std::sync::mpsc::Receiver<()>>,
) -> Arc<RecoveryClock> {
    Arc::new(RecoveryClock {
        health,
        entered: Mutex::new(entered),
        armed: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
        flipped: AtomicBool::new(false),
    })
}
#[tokio::test]
async fn recovery_during_started_emission_cancels_active_native_and_reclaims_admission() {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let cancelled = Arc::new(AtomicBool::new(false));
    let observed = cancelled.clone();
    let service = fake(4, move |_, _, cancel, stop| {
        entered_tx.send(()).unwrap();
        while !cancel.is_cancelled() && !stop.is_cancelled() {
            std::thread::sleep(Duration::from_millis(1));
        }
        observed.store(true, Ordering::SeqCst);
        Err(PiperError::Cancelled)
    })
    .await;
    let probe = admission_probe(&service);
    let p = processor(service);
    let clock = clock(p.health().unwrap(), Some(entered_rx));
    let worker = Pipeline::new(vec![p])
        .with_observer_clock(clock.clone())
        .with_observer(Arc::new(NoopObserver), ObserverOptions::default())
        .start(config())
        .await
        .unwrap();
    clock.armed.store(true, Ordering::SeqCst);
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("active"))).await;
    let completed = tokio::time::timeout(Duration::from_millis(200), async {
        while !cancelled.load(Ordering::SeqCst) || probe.available() != 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    worker.cancel("cleanup").await.unwrap();
    collect(worker).await;
    assert!(clock.flipped.load(Ordering::SeqCst));
    assert!(
        completed.is_ok(),
        "active native request retained cancellation/permit ownership before cleanup"
    );
}
#[tokio::test]
async fn recovery_during_started_emission_drops_queued_request_without_backend_entry() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered_tx = Some(entered_tx);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let stale_calls = Arc::new(AtomicUsize::new(0));
    let called = stale_calls.clone();
    let service = fake(4, move |text, _, _, _| {
        if text == "blocker" {
            if let Some(tx) = entered_tx.take() {
                let _ = tx.send(());
            }
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        } else {
            called.fetch_add(1, Ordering::SeqCst);
        }
        Ok(vec![1, 0])
    })
    .await;
    let blocker = service
        .submit("blocker", "direct", PiperSettings::default())
        .unwrap();
    entered_rx.await.unwrap();
    let probe = admission_probe(&service);
    let p = processor(service);
    let clock = clock(p.health().unwrap(), None);
    let mut worker = Pipeline::new(vec![p])
        .with_observer_clock(clock.clone())
        .with_observer(Arc::new(NoopObserver), ObserverOptions::default())
        .start(config())
        .await
        .unwrap();
    // Do not add response-start here: the two clock calls are process + Started.
    clock.armed.store(true, Ordering::SeqCst);
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("stale"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let fence = tokio::time::timeout(Duration::from_millis(200), async {
        while let Some(out) = worker.recv().await {
            if matches!(out.frame.data, FrameData::LlmResponseEnd) {
                return out.frame.metadata.get("failed").and_then(|v| v.as_bool()) == Some(true);
            }
        }
        false
    })
    .await;
    release_tx.send(()).unwrap();
    blocker.result().await.unwrap();
    let reclaimed = tokio::time::timeout(Duration::from_millis(200), async {
        while probe.available() != 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    worker.cancel("cleanup").await.unwrap();
    collect(worker).await;
    assert!(clock.flipped.load(Ordering::SeqCst));
    assert_eq!(
        fence,
        Ok(true),
        "queued stale relay must release its failed response fence while blocker is still active"
    );
    assert!(reclaimed.is_ok());
    assert_eq!(stale_calls.load(Ordering::SeqCst), 0);
}

struct BlockingSink {
    count: AtomicUsize,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl FrameProcessor for BlockingSink {
    fn name(&self) -> &str {
        "BlockingSink"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::TtsAudio(_))
            && self.count.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn cancelling_capacity_wait_reclaims_native_ownership_without_releasing_sink() {
    let service = fake(4, |_, _, _, _| Ok([1, 0].repeat(8192))).await;
    let probe = admission_probe(&service);
    let p = processor(service);
    let sink = Arc::new(BlockingSink {
        count: AtomicUsize::new(0),
        entered: Default::default(),
        release: Default::default(),
    });
    let mut cfg = config();
    cfg.queue_capacity = 1;
    let worker = Pipeline::new(vec![p, sink.clone()])
        .start(cfg)
        .await
        .unwrap();
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("many chunks")),
    )
    .await;
    sink.entered.notified().await;
    worker.cancel("blocked admission").await.unwrap();
    collect(worker).await;
    assert_eq!(probe.available(), 4);
}
#[tokio::test]
async fn health_recovery_discards_pcm_waiting_for_downstream_capacity() {
    let service = fake(4, |_, _, _, _| Ok([1, 0].repeat(8192))).await;
    let p = processor(service);
    let health = p.health().unwrap();
    let sink = Arc::new(BlockingSink {
        count: AtomicUsize::new(0),
        entered: Default::default(),
        release: Default::default(),
    });
    let mut cfg = config();
    cfg.queue_capacity = 1;
    let worker = Pipeline::new(vec![p, sink.clone()])
        .start(cfg)
        .await
        .unwrap();
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("many chunks")),
    )
    .await;
    sink.entered.notified().await;
    health.set_usable(false);
    health.set_usable(true);
    sink.release.notify_one();
    worker.end().await.unwrap();
    let frames = collect(worker).await;
    let audio = frames
        .iter()
        .filter(|out| matches!(out.frame.data, FrameData::TtsAudio(_)))
        .count();
    assert!(
        audio <= 2,
        "pending capacity-waited third frame crossed restoration"
    );
}
#[tokio::test]
async fn pcm_relay_preserves_exact_chunks_across_tight_adjacent_lane() {
    let pcm = [11, 0].repeat(8192);
    let generated = pcm.clone();
    let service = fake(4, move |_, _, _, _| Ok(generated.clone())).await;
    let p = Arc::new(
        PiperTtsProcessor::new(
            service,
            PiperSettings::default(),
            PiperProcessorOptions {
                max_pending: 4,
                chunk_samples: 1,
            },
        )
        .unwrap(),
    );
    let mut cfg = config();
    cfg.queue_capacity = 1;
    cfg.output_capacity = 16384;
    let worker = Pipeline::new(vec![p, Arc::new(IdentityProcessor)])
        .start(cfg)
        .await
        .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("burst"))).await;
    worker.end().await.unwrap();
    let frames = collect(worker).await;
    let mut actual = Vec::new();
    for out in frames {
        match out.frame.data {
            FrameData::TtsAudio(audio) => {
                assert_eq!(audio.audio.len(), 2);
                actual.extend(audio.audio);
            }
            FrameData::Error(error) => panic!("{}", error.error),
            _ => {}
        }
    }
    assert_eq!(actual, pcm);
}
#[tokio::test]
async fn bounds_and_wrong_pipeline_rate_are_explicit_errors() {
    let service = fake(4, |_, _, _, _| Ok(vec![1, 0])).await;
    assert!(PiperTtsProcessor::new(
        service,
        PiperSettings::default(),
        PiperProcessorOptions {
            max_pending: 5,
            chunk_samples: 1
        }
    )
    .is_err());
    let service = fake(4, |_, _, _, _| Ok(vec![1, 0])).await;
    match Pipeline::new(vec![processor(service)])
        .start(PipelineConfig::default())
        .await
    {
        Err(_) => {}
        Ok(worker) => {
            // Core publishes setup errors through the started worker. A fatal
            // rate mismatch must still prevent successful pipeline completion.
            assert!(
                tokio::time::timeout(Duration::from_secs(3), worker.collect())
                    .await
                    .unwrap()
                    .is_err()
            );
        }
    }
}

struct CompletionSink {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait]
impl FrameProcessor for CompletionSink {
    fn name(&self) -> &str {
        "CompletionSink"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::TtsAlignmentDiagnostic(_)) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn graceful_end_and_stop_wait_for_space_after_final_pcm() {
    for terminal in [FrameData::End(EndFrame::default()), FrameData::Stop] {
        let service = fake(4, |_, _, _, _| Ok(vec![1, 0])).await;
        let sink = Arc::new(CompletionSink {
            entered: Default::default(),
            release: Default::default(),
        });
        let mut cfg = config();
        cfg.queue_capacity = 1;
        let worker = Pipeline::new(vec![processor(service), sink.clone()])
            .start(cfg)
            .await
            .unwrap();
        send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("hello"))).await;
        sink.entered.notified().await;
        send(&worker, terminal).await;
        // Completion is processing and TtsStopped can occupy the only queue
        // slot. Terminal admission must remain pending without runtime failure.
        tokio::task::yield_now().await;
        sink.release.notify_one();
        let frames = collect(worker).await;
        let stop = frames
            .iter()
            .position(|out| matches!(out.frame.data, FrameData::TtsStopped(_)))
            .unwrap();
        let end = frames
            .iter()
            .position(|out| matches!(out.frame.data, FrameData::End(_) | FrameData::Stop))
            .unwrap();
        assert!(stop < end);
        assert!(!frames
            .iter()
            .any(|out| matches!(out.frame.data, FrameData::Error(_))));
    }
}
#[tokio::test]
async fn compact_and_detailed_flush_cannot_finish_before_prior_native_audio() {
    for detailed in [false, true] {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let mut entered_tx = Some(entered_tx);
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let service = fake(4, move |_, _, _, _| {
            if let Some(tx) = entered_tx.take() {
                let _ = tx.send(());
            }
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            Ok(vec![1, 0])
        })
        .await;
        let mut worker = Pipeline::new(vec![processor(service)])
            .start(config())
            .await
            .unwrap();
        send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("blocked"))).await;
        entered_rx.await.unwrap();
        let flush = if detailed {
            FrameData::FlushDetailed(PipelineFlushFrame::default())
        } else {
            FrameData::Flush(FlushFrame {
                returning: false,
                origin: None,
            })
        };
        send(&worker, flush).await;
        let early = tokio::time::timeout(Duration::from_millis(20), async {
            while let Some(out) = worker.recv().await {
                if matches!(
                    out.frame.data,
                    FrameData::Flush(_) | FrameData::FlushDetailed(_)
                ) {
                    return true;
                }
            }
            false
        })
        .await;
        release_tx.send(()).unwrap();
        worker.end().await.unwrap();
        let frames = collect(worker).await;
        assert!(
            early.is_err(),
            "Flush completed while native work remained blocked"
        );
        let audio = frames
            .iter()
            .position(|out| matches!(out.frame.data, FrameData::TtsAudio(_)))
            .unwrap();
        let flush = frames
            .iter()
            .position(|out| {
                matches!(
                    out.frame.data,
                    FrameData::Flush(_) | FrameData::FlushDetailed(_)
                )
            })
            .unwrap();
        assert!(audio < flush);
        assert!(!frames
            .iter()
            .any(|out| matches!(out.frame.data, FrameData::Error(_))));
    }
}

#[tokio::test]
async fn cancelled_middle_relay_cannot_release_surviving_fence_before_earlier_audio() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered_tx = Some(entered_tx);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let service = fake(4, move |text, _, _, _| {
        if text == "first" {
            if let Some(tx) = entered_tx.take() {
                let _ = tx.send(());
            }
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        }
        Ok(vec![1, 0])
    })
    .await;
    let probe = admission_probe(&service);
    let mut worker = Pipeline::new(vec![processor(service)])
        .start(config())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("first"))).with_interruptible(false),
            Direction::Downstream,
        )
        .await
        .unwrap();
    entered_rx.await.unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("middle"))).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmResponseEnd).with_interruptible(false),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_millis(200), async {
        while probe.available() != 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    send(&worker, FrameData::Interruption).await;
    let early = tokio::time::timeout(Duration::from_millis(20), async {
        while let Some(out) = worker.recv().await {
            if matches!(out.frame.data, FrameData::LlmResponseEnd) {
                return true;
            }
        }
        false
    })
    .await;
    release_tx.send(()).unwrap();
    worker.end().await.unwrap();
    let frames = collect(worker).await;
    assert!(
        early.is_err(),
        "surviving response fence overtook earlier uninterruptible native work"
    );
    let audio = frames
        .iter()
        .position(|out| matches!(out.frame.data, FrameData::TtsAudio(_)))
        .unwrap();
    let fence = frames
        .iter()
        .position(|out| matches!(out.frame.data, FrameData::LlmResponseEnd))
        .unwrap();
    assert!(audio < fence);
    assert_eq!(
        frames
            .iter()
            .filter(|out| matches!(out.frame.data, FrameData::TtsAudio(_)))
            .count(),
        1
    );
}

#[tokio::test]
async fn skipped_text_preserves_spacing_and_source_even_when_native_health_is_unusable() {
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let service = fake(4, move |_, _, _, _| {
        called.fetch_add(1, Ordering::SeqCst);
        Ok(vec![1, 0])
    })
    .await;
    let p = processor(service);
    p.health().unwrap().set_usable(false);
    let worker = Pipeline::new(vec![p]).start(config()).await.unwrap();
    send(
        &worker,
        FrameData::TtsTextInput(TtsTextInputFrame {
            synthesis_text: "not spoken".into(),
            display_text: "display".into(),
            original_text: "<i>display</i>".into(),
            append_to_context: false,
            includes_inter_frame_spaces: false,
            aggregated_by: "skipped".into(),
            skip_tts: true,
        }),
    )
    .await;
    worker.end().await.unwrap();
    let frames = collect(worker).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(frames.iter().any(|out| matches!(&out.frame.data, FrameData::AggregatedText(text) if text.text.text == "display" && text.raw_text.as_deref() == Some("<i>display</i>") && !text.will_be_spoken && text.text.skip_tts == Some(true) && !text.text.includes_inter_frame_spaces && !text.text.append_to_context)));
    assert!(!frames.iter().any(|out| matches!(
        out.frame.data,
        FrameData::Error(_) | FrameData::TtsStarted(_)
    )));
}
#[tokio::test]
async fn selecting_existing_inference_defaults_is_not_effective_health_recovery() {
    let service = fake(4, |_, _, _, _| Ok(vec![1, 0])).await;
    let p = processor(service);
    let health = p.health().unwrap();
    health.set_usable(false);
    let mut delta = TtsSettings::default();
    delta
        .base
        .extra
        .insert("noise_scale".into(), serde_json::json!(0.667));
    delta
        .base
        .extra
        .insert("length_scale".into(), serde_json::json!(1.0));
    delta
        .base
        .extra
        .insert("noise_w".into(), serde_json::json!(0.8));
    delta
        .base
        .extra
        .insert("speaker_id".into(), serde_json::json!(0));
    assert!(!p.update_settings(&delta).unwrap());
    assert!(!health.is_usable());
    delta
        .base
        .extra
        .insert("noise_scale".into(), serde_json::Value::Null);
    assert!(!p.update_settings(&delta).unwrap());
    assert!(!health.is_usable());
}

struct SlowRecorder {
    pcm: Mutex<Vec<u8>>,
    played: bool,
}
#[async_trait]
impl pipecat_media_transport::OutputBackend for SlowRecorder {
    async fn start(
        &self,
        _: pipecat_audio::PcmFormat,
        _: &[Option<String>],
    ) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(
        &self,
        frame: &Frame,
    ) -> Result<pipecat_media_transport::PlaybackReceipt, ProcessorError> {
        tokio::time::sleep(Duration::from_millis(1)).await;
        match &frame.data {
            FrameData::TtsAudio(audio)
            | FrameData::OutputAudio(audio)
            | FrameData::SpeechOutputAudio(audio) => {
                self.pcm.lock().unwrap().extend_from_slice(&audio.audio)
            }
            _ => return Err(ProcessorError::new("unexpected recorder payload")),
        }
        Ok(if self.played {
            pipecat_media_transport::PlaybackReceipt::Played
        } else {
            pipecat_media_transport::PlaybackReceipt::Discarded
        })
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
struct HistorySink(AtomicUsize);
impl pipecat_media_transport::SpokenHistorySink for HistorySink {
    fn commit(
        &self,
        _: pipecat_media_transport::SpokenContext,
    ) -> Result<Vec<DirectedFrame>, ProcessorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Vec::new())
    }
}
#[tokio::test]
async fn complete_pcm_crosses_default_slow_media_capacity_without_implying_spoken_history() {
    use pipecat_media_transport::*;
    for played in [false, true] {
        let expected = [17, 0].repeat(256 * 1024);
        let pcm = expected.clone();
        let service = fake(4, move |_, _, _, _| Ok(pcm.clone())).await;
        let recorder = Arc::new(SlowRecorder {
            pcm: Mutex::new(Vec::new()),
            played,
        });
        let sink = Arc::new(HistorySink(AtomicUsize::new(0)));
        let history = SpokenHistory::new(SpokenHistoryConfig::default(), sink.clone()).unwrap();
        let output = OutputTransport::new(
            "recorder",
            OutputTransportConfig {
                sample_rate: Some(16000),
                auto_silence: false,
                end_silence: Duration::ZERO,
                ..Default::default()
            },
            recorder.clone(),
            None,
            Default::default(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap();
        let mut cfg = config();
        cfg.shutdown_timeout = Duration::from_secs(10);
        let worker = Pipeline::new(vec![
            Arc::new(history.source_boundary("source").unwrap()),
            processor(service),
            Arc::new(output),
        ])
        .start(cfg)
        .await
        .unwrap();
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            FrameData::TtsSpeak(TtsSpeakFrame::new("complete native result")),
        )
        .await;
        send(&worker, FrameData::LlmResponseEnd).await;
        worker.end().await.unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(12), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.dropped_frames, 0);
        let actual = recorder.pcm.lock().unwrap();
        assert_eq!(
            &actual[..expected.len().min(actual.len())],
            &expected[..expected.len().min(actual.len())]
        );
        assert_eq!(actual.len(), expected.len().next_multiple_of(1280));
        assert!(actual[expected.len()..].iter().all(|byte| *byte == 0));
        assert_eq!(history.status().committed_contexts, 0);
        assert_eq!(sink.0.load(Ordering::SeqCst), 0);
        assert!(!frames
            .iter()
            .any(|out| matches!(out.frame.data, FrameData::TtsText(_))));
        assert!(frames.iter().any(|out| matches!(&out.frame.data, FrameData::TtsAlignmentDiagnostic(d) if d.evidence == TtsAlignmentEvidence::Unreported && d.synthesis_outcome == TtsSynthesisOutcome::Succeeded)));
    }
}
#[tokio::test]
async fn unrelated_original_and_display_provenance_rejects_before_native_admission() {
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let service = fake(4, move |_, _, _, _| {
        called.fetch_add(1, Ordering::SeqCst);
        Ok(vec![1, 0])
    })
    .await;
    let worker = Pipeline::new(vec![processor(service)])
        .start(config())
        .await
        .unwrap();
    send(
        &worker,
        FrameData::TtsTextInput(TtsTextInputFrame {
            synthesis_text: "one dollar".into(),
            display_text: "$1".into(),
            original_text: "USD 1".into(),
            ..TtsTextInputFrame::new("")
        }),
    )
    .await;
    worker.end().await.unwrap();
    let frames = collect(worker).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(frames
        .iter()
        .any(|out| matches!(out.frame.data, FrameData::Error(_))));
}

// Independent recovery review: native ownership starts before an ordered fence
// is released, so health cancellation must cover the predecessor wait itself.
#[tokio::test]
async fn independent_health_reset_behind_full_response_start_cancels_active_native() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered_tx = Some(entered_tx);
    let cancelled = Arc::new(AtomicBool::new(false));
    let observed = cancelled.clone();
    let service = fake(4, move |_, _, cancel, stop| {
        if let Some(tx) = entered_tx.take() {
            let _ = tx.send(());
        }
        while !cancel.is_cancelled() && !stop.is_cancelled() {
            std::thread::sleep(Duration::from_millis(1));
        }
        observed.store(true, Ordering::SeqCst);
        Err(PiperError::Cancelled)
    })
    .await;
    let probe = admission_probe(&service);
    let p = processor(service);
    let health = p.health().unwrap();
    let mut cfg = config();
    cfg.output_capacity = 1;
    let worker = Pipeline::new(vec![p]).start(cfg).await.unwrap();
    // Start occupies the sole output slot. ResponseStart cannot be admitted,
    // and speech therefore remains behind an owned predecessor fence.
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("active behind fence")),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(1), entered_rx)
        .await
        .unwrap()
        .unwrap();
    health.set_usable(false);
    health.set_usable(true);
    let released_before_output_drain = tokio::time::timeout(Duration::from_millis(250), async {
        while !cancelled.load(Ordering::SeqCst) || probe.available() != 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .is_ok();
    worker.cancel("independent cleanup").await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap();
    assert!(released_before_output_drain, "health reset retained active native ownership behind a full response-start fence until cleanup");
}

#[tokio::test]
async fn independent_health_reset_behind_full_response_start_cancels_queued_native() {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered_tx = Some(entered_tx);
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let stale_calls = Arc::new(AtomicUsize::new(0));
    let observed = stale_calls.clone();
    let service = fake(4, move |text, _, _, _| {
        if text == "blocker" {
            if let Some(tx) = entered_tx.take() {
                let _ = tx.send(());
            }
            release_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        } else {
            observed.fetch_add(1, Ordering::SeqCst);
        }
        Ok(vec![1, 0])
    })
    .await;
    let blocker = service
        .submit("blocker", "independent-direct", PiperSettings::default())
        .unwrap();
    entered_rx.await.unwrap();
    let probe = admission_probe(&service);
    let p = processor(service);
    let health = p.health().unwrap();
    let mut cfg = config();
    cfg.output_capacity = 1;
    let worker = Pipeline::new(vec![p.clone()]).start(cfg).await.unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("queued behind fence")),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while probe.available() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    health.set_usable(false);
    health.set_usable(true);
    // Observe actual relay ownership completion before the native blocker is
    // released. Its PendingSynthesis has therefore been dropped, rather than
    // relying on a delay to guess whether cancellation reached the queue.
    let cancelled_before_native_dequeue = tokio::time::timeout(Duration::from_millis(250), async {
        while p.unfinished_speech_relays() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok();
    release_tx.send(()).unwrap();
    blocker.result().await.unwrap();
    let released_before_output_drain = tokio::time::timeout(Duration::from_millis(250), async {
        while probe.available() != 4 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .is_ok();
    let stale_calls_before_cleanup = stale_calls.load(Ordering::SeqCst);
    worker.cancel("independent cleanup").await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap();
    assert!(cancelled_before_native_dequeue, "queued native ownership was not dropped while the preceding response-start fence remained blocked");
    assert_eq!(stale_calls_before_cleanup, 0, "health-stale queued work entered the backend while its response-start fence was still blocked");
    assert!(
        released_before_output_drain,
        "health-stale queued result retained admission until output drain or cleanup"
    );
}
