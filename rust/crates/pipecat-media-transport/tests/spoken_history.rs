//! Synthetic explicit playback receipts through the real native output and
//! assistant Spoken aggregator. No hardware or provider acceptance is claimed.
use async_trait::async_trait;
use pipecat_audio::{encode_pcm16_le, PcmFormat};
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_services::{Content, Context};
use pipecat_turns::{
    AggregationAction, AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator,
    SharedContext,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

struct Receipts {
    calls: AtomicUsize,
    played: AtomicUsize,
    changed: Notify,
    gate: Option<Semaphore>,
    discard: Option<usize>,
    fail: Option<usize>,
    frames: Mutex<Vec<Frame>>,
}
impl Receipts {
    fn new(block: bool, discard: Option<usize>, fail: Option<usize>) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            played: AtomicUsize::new(0),
            changed: Notify::new(),
            gate: block.then(|| Semaphore::new(0)),
            discard,
            fail,
            frames: Mutex::new(vec![]),
        }
    }
    async fn wait_calls(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let changed = self.changed.notified();
                if self.calls.load(Ordering::SeqCst) >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}
#[async_trait]
impl OutputBackend for Receipts {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        self.changed.notify_waiters();
        if let Some(gate) = &self.gate {
            gate.acquire().await.unwrap().forget();
        }
        if self.fail == Some(call) {
            return Err(ProcessorError::new("synthetic playback failure"));
        }
        if self.discard == Some(call) {
            return Ok(PlaybackReceipt::Discarded);
        }
        self.frames.lock().unwrap().push(frame.clone());
        self.played.fetch_add(1, Ordering::SeqCst);
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
struct AssistantSink(Arc<LLMAssistantAggregator>);
impl SpokenHistorySink for AssistantSink {
    fn commit(&self, context: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        // Mutation occurs here, under the ledger's interruption lock. Context
        // notifications are ordinary follow-on frames, never the mutation.
        self.0
            .confirm_playback(&context.context_id, &context.texts)?
            .actions
            .into_iter()
            .map(|action| match action {
                AggregationAction::Push(frame) => Ok(frame),
                _ => Err(ProcessorError::new(
                    "unexpected assistant confirmation effect",
                )),
            })
            .collect()
    }
}
fn config() -> OutputTransportConfig {
    OutputTransportConfig {
        sample_rate: Some(8000),
        chunk_duration: Duration::from_millis(10),
        auto_silence: false,
        end_silence: Duration::ZERO,
        write_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}
struct Fixture {
    #[cfg(feature = "assistant-history")]
    assistant: Arc<LLMAssistantAggregator>,
    worker: PipelineWorker,
    history: SpokenHistory,
    shared: SharedContext,
    control: OutputControl,
}
async fn fixture(backend: Arc<Receipts>, bounds: SpokenHistoryConfig) -> Fixture {
    fixture_with_output_config(backend, bounds, config()).await
}
async fn fixture_with_output_config(
    backend: Arc<Receipts>,
    bounds: SpokenHistoryConfig,
    output_config: OutputTransportConfig,
) -> Fixture {
    fixture_with_adapter(backend, bounds, output_config, false).await
}
async fn fixture_with_adapter(
    backend: Arc<Receipts>,
    bounds: SpokenHistoryConfig,
    output_config: OutputTransportConfig,
    use_assistant_adapter: bool,
) -> Fixture {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let sink: Arc<dyn SpokenHistorySink> = Arc::new(AssistantSink(assistant.clone()));
    #[cfg(feature = "assistant-history")]
    let sink: Arc<dyn SpokenHistorySink> = if use_assistant_adapter {
        Arc::new(AssistantSpokenHistorySink::new(assistant.clone()))
    } else {
        sink
    };
    #[cfg(not(feature = "assistant-history"))]
    let _ = use_assistant_adapter;
    let history = SpokenHistory::new(bounds, sink).unwrap();
    let source = Arc::new(history.source_boundary("source").unwrap());
    let output = Arc::new(
        OutputTransport::new("output", output_config, backend, None, BTreeMap::new())
            .unwrap()
            .with_spoken_history(history.clone())
            .unwrap(),
    );
    let control = output.control();
    let worker = Pipeline::new(vec![source, output, assistant.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !control.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    Fixture {
        #[cfg(feature = "assistant-history")]
        assistant,
        worker,
        history,
        shared,
        control,
    }
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
fn start(id: &str) -> Frame {
    Frame::new(FrameData::TtsStarted(TtsStartedFrame {
        context_id: Some(id.into()),
        append_to_context: true,
    }))
}
fn text(id: &str, content: &str) -> Frame {
    Frame::new(FrameData::TtsText(AggregatedTextFrame {
        text: TextFrame::new(content),
        context_id: Some(id.into()),
        aggregated_by: "word".into(),
        raw_text: None,
        will_be_spoken: true,
    }))
}
fn audio(id: Option<&str>, samples: usize, rate: u32) -> Frame {
    let mut pcm = AudioRawFrame::new(encode_pcm16_le(&vec![1000; samples]), rate, 1).unwrap();
    pcm.context_id = id.map(str::to_owned);
    Frame::new(FrameData::TtsAudio(pcm))
}
fn stop(id: &str) -> Frame {
    Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
        context_id: Some(id.into()),
    }))
}
async fn begin(worker: &PipelineWorker, id: &str) {
    send(worker, Frame::new(FrameData::LlmResponseStart)).await;
    send(worker, start(id)).await;
    send(worker, text(id, "Heard")).await;
}
async fn complete(worker: &PipelineWorker, id: &str) {
    send(worker, audio(Some(id), 80, 8000)).await;
    send(worker, stop(id)).await;
    send(worker, Frame::new(FrameData::LlmResponseEnd)).await;
}
async fn fence(worker: &mut PipelineWorker) {
    let marker = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("test-fence".into()),
        returning: false,
    }));
    let id = marker.id;
    send(worker, marker).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if frame.frame.id == id {
                return;
            }
        }
        panic!("pipeline ended before fence");
    })
    .await
    .unwrap();
}
fn contents(shared: &SharedContext) -> Vec<String> {
    shared
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter_map(|message| match message.content {
            Some(Content::Text(text)) => Some(text),
            _ => None,
        })
        .collect()
}
async fn finish(f: Fixture) -> WorkerReport {
    f.worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), f.worker.collect())
        .await
        .unwrap()
        .unwrap()
        .1
}

#[tokio::test]
async fn resampled_split_pcm_requires_last_explicit_receipt_and_final_response() {
    let backend = Arc::new(Receipts::new(true, None, None));
    let mut f = fixture(backend.clone(), SpokenHistoryConfig::default()).await;
    begin(&f.worker, "one").await;
    send(&f.worker, text("one", "completely")).await;
    // One provider frame becomes five backend writes after resampling/chunking.
    send(&f.worker, audio(Some("one"), 800, 16000)).await;
    send(&f.worker, stop("one")).await;
    send(&f.worker, Frame::new(FrameData::LlmResponseEnd)).await;
    backend.wait_calls(1).await;
    assert!(contents(&f.shared).is_empty());
    backend.gate.as_ref().unwrap().add_permits(4);
    backend.wait_calls(5).await;
    assert_eq!(backend.played.load(Ordering::SeqCst), 4);
    assert!(
        contents(&f.shared).is_empty(),
        "partial playback must not invent a word boundary"
    );
    backend.gate.as_ref().unwrap().add_permits(1);
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard completely"]);
    assert_eq!(f.history.status().committed_contexts, 1);
    assert_eq!(backend.frames.lock().unwrap().len(), 5);
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn padded_tail_and_coalesced_provider_frames_are_attested_without_count_equality() {
    let backend = Arc::new(Receipts::new(false, None, None));
    let mut f = fixture(backend.clone(), SpokenHistoryConfig::default()).await;
    begin(&f.worker, "tail").await;
    for size in [3, 7, 11] {
        send(&f.worker, audio(Some("tail"), size, 8000)).await;
    }
    send(&f.worker, stop("tail")).await;
    fence(&mut f.worker).await;
    assert!(
        contents(&f.shared).is_empty(),
        "provider stop alone is not source success"
    );
    send(&f.worker, Frame::new(FrameData::LlmResponseEnd)).await;
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"], "{:?}", f.history.status());
    assert_eq!(backend.played.load(Ordering::SeqCst), 1);
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn discarded_partial_pcm_and_backend_failure_never_commit() {
    for fail in [false, true] {
        let backend = Arc::new(Receipts::new(
            false,
            (!fail).then_some(2),
            fail.then_some(2),
        ));
        let mut f = fixture(backend.clone(), SpokenHistoryConfig::default()).await;
        begin(&f.worker, "partial").await;
        send(&f.worker, audio(Some("partial"), 240, 8000)).await;
        send(&f.worker, stop("partial")).await;
        send(&f.worker, Frame::new(FrameData::LlmResponseEnd)).await;
        fence(&mut f.worker).await;
        assert!(backend.played.load(Ordering::SeqCst) > 0);
        assert!(contents(&f.shared).is_empty());
        assert_eq!(f.history.status().committed_contexts, 0);
        let report = finish(f).await;
        assert_eq!(!report.errors.is_empty(), fail);
    }
}

#[tokio::test]
async fn failed_source_end_cannot_certify_an_otherwise_played_context() {
    let mut f = fixture(
        Arc::new(Receipts::new(false, None, None)),
        SpokenHistoryConfig::default(),
    )
    .await;
    begin(&f.worker, "failed").await;
    send(&f.worker, audio(Some("failed"), 80, 8000)).await;
    send(&f.worker, stop("failed")).await;
    let mut end = Frame::new(FrameData::LlmResponseEnd);
    end.metadata.insert("failed".into(), true.into());
    send(&f.worker, end).await;
    fence(&mut f.worker).await;
    assert!(contents(&f.shared).is_empty());
    assert!(f
        .history
        .status()
        .last_rejection
        .unwrap()
        .contains("source"));
    begin(&f.worker, "fresh").await;
    complete(&f.worker, "fresh").await;
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"]);
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn unknown_mismatched_missing_stop_and_interleaved_contexts_withhold_history() {
    for case in 0..6 {
        let mut f = fixture(
            Arc::new(Receipts::new(false, None, None)),
            SpokenHistoryConfig::default(),
        )
        .await;
        begin(&f.worker, "one").await;
        match case {
            0 => send(&f.worker, audio(None, 80, 8000)).await,
            1 => send(&f.worker, text("other", "unknown")).await,
            2 => send(&f.worker, start("other")).await,
            3 => send(&f.worker, stop("other")).await,
            4 => {}
            5 => send(&f.worker, Frame::new(FrameData::LlmResponseStart)).await,
            _ => unreachable!(),
        }
        send(&f.worker, audio(Some("one"), 80, 8000)).await;
        if case != 4 {
            send(&f.worker, stop("one")).await;
        }
        send(&f.worker, Frame::new(FrameData::LlmResponseEnd)).await;
        fence(&mut f.worker).await;
        assert!(contents(&f.shared).is_empty(), "case {case}");
        assert!(finish(f).await.errors.is_empty());
    }
}

#[tokio::test]
async fn standalone_context_cannot_borrow_a_later_response_success() {
    let mut f = fixture(
        Arc::new(Receipts::new(false, None, None)),
        SpokenHistoryConfig::default(),
    )
    .await;
    send(&f.worker, start("standalone")).await;
    send(&f.worker, text("standalone", "Uncertified")).await;
    send(&f.worker, audio(Some("standalone"), 80, 8000)).await;
    send(&f.worker, stop("standalone")).await;
    send(&f.worker, Frame::new(FrameData::LlmResponseStart)).await;
    send(&f.worker, Frame::new(FrameData::LlmResponseEnd)).await;
    fence(&mut f.worker).await;
    assert!(contents(&f.shared).is_empty());
    assert!(f
        .history
        .status()
        .last_rejection
        .unwrap()
        .contains("standalone"));
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn immediate_invalidation_between_receipt_and_commit_rejects_late_context() {
    let backend = Arc::new(Receipts::new(false, None, None));
    let mut f = fixture(backend.clone(), SpokenHistoryConfig::default()).await;
    begin(&f.worker, "old").await;
    send(&f.worker, audio(Some("old"), 80, 8000)).await;
    send(&f.worker, stop("old")).await;
    fence(&mut f.worker).await;
    assert_eq!(backend.played.load(Ordering::SeqCst), 1);
    assert!(contents(&f.shared).is_empty());
    // Invalidate before interruption admission, not after a queue eventually
    // invokes OutputTransport::process. A late finalization cannot win this race.
    f.history.invalidate();
    send(&f.worker, Frame::interruption()).await;
    fence(&mut f.worker).await;
    send(&f.worker, Frame::new(FrameData::LlmResponseEnd)).await;
    send(&f.worker, text("old", "late")).await;
    send(&f.worker, stop("old")).await;
    fence(&mut f.worker).await;
    assert!(contents(&f.shared).is_empty());
    begin(&f.worker, "new").await;
    complete(&f.worker, "new").await;
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"]);
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn output_interruption_fallback_and_cancel_withhold_inflight_pcm() {
    for cancel in [false, true] {
        let backend = Arc::new(Receipts::new(true, None, None));
        let mut f = fixture(backend.clone(), SpokenHistoryConfig::default()).await;
        begin(&f.worker, "blocked").await;
        complete(&f.worker, "blocked").await;
        backend.wait_calls(1).await;
        if cancel {
            f.history.invalidate();
            f.worker.cancel("cancelled playback").await.unwrap();
            let (_, report) = f.worker.collect().await.unwrap();
            assert!(report.errors.is_empty());
            assert!(contents(&f.shared).is_empty());
            assert_eq!(f.history.status().buffered_text_bytes, 0);
        } else {
            send(&f.worker, Frame::interruption()).await;
            fence(&mut f.worker).await;
            backend.gate.as_ref().unwrap().add_permits(10);
            assert!(contents(&f.shared).is_empty());
            assert!(finish(f).await.errors.is_empty());
        }
    }
}

#[tokio::test]
async fn duplicate_and_reused_contexts_do_not_commit_twice() {
    let mut f = fixture(
        Arc::new(Receipts::new(false, None, None)),
        SpokenHistoryConfig::default(),
    )
    .await;
    begin(&f.worker, "one").await;
    complete(&f.worker, "one").await;
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"]);
    begin(&f.worker, "one").await;
    complete(&f.worker, "one").await;
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"]);
    begin(&f.worker, "two").await;
    let duplicated = text("two", "duplicate");
    send(&f.worker, duplicated.clone()).await;
    send(&f.worker, duplicated).await;
    complete(&f.worker, "two").await;
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"]);
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn replay_capacity_exhaustion_is_latched_without_tombstone_eviction() {
    let mut f = fixture(
        Arc::new(Receipts::new(false, None, None)),
        SpokenHistoryConfig {
            max_context_ids: 1,
            ..Default::default()
        },
    )
    .await;
    for id in ["one", "two", "one", "three"] {
        begin(&f.worker, id).await;
        complete(&f.worker, id).await;
        fence(&mut f.worker).await;
    }
    assert_eq!(contents(&f.shared), ["Heard"]);
    let status = f.history.status();
    assert!(status.exhausted);
    assert_eq!(status.retained_context_ids, 1);
    assert_eq!(status.pending_responses, 0);
    assert_eq!(status.buffered_text_bytes, 0);
    assert!(status.last_rejection.unwrap().contains("capacity"));
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn text_and_frame_budgets_release_buffers_without_later_false_proof() {
    for frame_limit in [false, true] {
        let bounds = SpokenHistoryConfig {
            max_text_bytes: if frame_limit { 4096 } else { 1 },
            max_frames_per_response: if frame_limit { 3 } else { 100 },
            ..Default::default()
        };
        let mut f = fixture(Arc::new(Receipts::new(false, None, None)), bounds).await;
        begin(&f.worker, "bounded").await;
        for _ in 0..20 {
            send(&f.worker, text("bounded", "x")).await;
        }
        complete(&f.worker, "bounded").await;
        fence(&mut f.worker).await;
        assert!(contents(&f.shared).is_empty());
        assert_eq!(f.history.status().buffered_text_bytes, 0);
        assert_eq!(f.history.status().pending_responses, 0);
        assert!(finish(f).await.errors.is_empty());
    }
}

#[tokio::test]
async fn opt_in_strips_untrusted_spoken_metadata_and_withholds_muted_pcm() {
    let mut f = fixture(
        Arc::new(Receipts::new(false, None, None)),
        SpokenHistoryConfig::default(),
    )
    .await;
    begin(&f.worker, "mute").await;
    let mut forged = text("mute", "Never heard");
    forged.metadata.insert("spoken".into(), true.into());
    send(&f.worker, forged).await;
    send(&f.worker, Frame::new(FrameData::VolumeGain(0.0))).await;
    complete(&f.worker, "mute").await;
    fence(&mut f.worker).await;
    assert!(contents(&f.shared).is_empty());
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn default_text_aggregation_remains_unchanged_without_opt_in() {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig::default(),
    ));
    let output = Arc::new(
        OutputTransport::new(
            "out",
            config(),
            Arc::new(Receipts::new(false, None, None)),
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let control = output.control();
    let mut worker = Pipeline::new(vec![output, assistant])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    while !control.is_ready() {
        tokio::task::yield_now().await;
    }
    begin(&worker, "text").await;
    send(&worker, Frame::new(FrameData::LlmResponseEnd)).await;
    fence(&mut worker).await;
    assert_eq!(contents(&shared), ["Heard"]);
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}

struct Noop;
impl SpokenHistorySink for Noop {
    fn commit(&self, _: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        Ok(vec![])
    }
}
struct EmptyFilter;
#[async_trait]
impl AudioFilter for EmptyFilter {
    async fn start(&self, _: PcmFormat) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn filter(&self, _: Vec<u8>) -> Result<Vec<u8>, ProcessorError> {
        Ok(vec![])
    }
    async fn control(&self, _: &FrameData) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
#[test]
fn unsupported_destinations_and_filters_are_rejected_before_playback() {
    for multi in [false, true] {
        let history = SpokenHistory::new(SpokenHistoryConfig::default(), Arc::new(Noop)).unwrap();
        let mut cfg = config();
        if multi {
            cfg.destinations.push("other".into());
        }
        let filter = (!multi).then(|| Arc::new(EmptyFilter) as Arc<dyn AudioFilter>);
        assert!(OutputTransport::new(
            "out",
            cfg,
            Arc::new(Receipts::new(false, None, None)),
            filter,
            BTreeMap::new()
        )
        .unwrap()
        .with_spoken_history(history)
        .is_err());
    }
}

#[tokio::test]
async fn late_source_pcm_after_queued_response_end_invalidates_the_pending_proof() {
    let backend = Arc::new(Receipts::new(true, None, None));
    let mut f = fixture(backend.clone(), SpokenHistoryConfig::default()).await;
    begin(&f.worker, "late").await;
    complete(&f.worker, "late").await;
    backend.wait_calls(1).await;
    send(&f.worker, audio(Some("late"), 80, 8000)).await;
    // The late PCM is admitted while the earlier receipt is still blocked.
    tokio::time::timeout(Duration::from_secs(3), async {
        while !f
            .history
            .status()
            .last_rejection
            .as_deref()
            .is_some_and(|reason| reason.contains("late"))
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    backend.gate.as_ref().unwrap().add_permits(10);
    fence(&mut f.worker).await;
    assert!(contents(&f.shared).is_empty());
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn pending_response_capacity_exhaustion_stays_disabled_after_drain() {
    let mut f = fixture(
        Arc::new(Receipts::new(false, None, None)),
        SpokenHistoryConfig {
            max_pending_responses: 1,
            ..Default::default()
        },
    )
    .await;
    f.control.pause(None).unwrap();
    for id in ["one", "two"] {
        begin(&f.worker, id).await;
        complete(&f.worker, id).await;
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while !f.history.status().exhausted {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.control.resume(None).unwrap();
    fence(&mut f.worker).await;
    assert!(contents(&f.shared).is_empty());
    assert_eq!(f.history.status().buffered_text_bytes, 0);
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn missing_backend_receipt_times_out_without_history() {
    let backend = Arc::new(Receipts::new(true, None, None));
    let mut f = fixture(backend.clone(), SpokenHistoryConfig::default()).await;
    begin(&f.worker, "timeout").await;
    complete(&f.worker, "timeout").await;
    backend.wait_calls(1).await;
    fence(&mut f.worker).await;
    assert!(contents(&f.shared).is_empty());
    assert!(!f.control.is_usable(None).unwrap());
    assert!(!finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn admission_timeout_withholds_history_even_when_admitted_pcm_is_played() {
    let backend = Arc::new(Receipts::new(false, None, None));
    let mut output_config = config();
    output_config.queue_capacity = 1;
    output_config.admission_policy = OutputAdmissionPolicy::Wait {
        timeout: Duration::from_millis(30),
    };
    let mut f = fixture_with_output_config(
        backend.clone(),
        SpokenHistoryConfig::default(),
        output_config,
    )
    .await;
    begin(&f.worker, "admission-timeout").await;
    fence(&mut f.worker).await;
    f.control.pause(None).unwrap();
    send(&f.worker, audio(Some("admission-timeout"), 80, 8000)).await;
    send(&f.worker, audio(Some("admission-timeout"), 80, 8000)).await;
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.worker.errors().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(f.worker.errors()[0].error.contains("admission timed out"));
    f.control.resume(None).unwrap();
    send(&f.worker, stop("admission-timeout")).await;
    send(&f.worker, Frame::new(FrameData::LlmResponseEnd)).await;
    fence(&mut f.worker).await;
    assert_eq!(backend.played.load(Ordering::SeqCst), 1);
    assert_eq!(f.history.status().committed_contexts, 0);
    assert!(contents(&f.shared).is_empty());
    assert_eq!(finish(f).await.errors.len(), 1);
}

struct LockedCommit {
    inner: AssistantSink,
    entered: Notify,
    release: (Mutex<bool>, std::sync::Condvar),
}
impl SpokenHistorySink for LockedCommit {
    fn commit(&self, context: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        self.entered.notify_one();
        let (lock, changed) = &self.release;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        self.inner.commit(context)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_invalidation_cannot_split_validation_from_atomic_context_commit() {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let sink = Arc::new(LockedCommit {
        inner: AssistantSink(assistant.clone()),
        entered: Notify::new(),
        release: (Mutex::new(false), std::sync::Condvar::new()),
    });
    let history = SpokenHistory::new(SpokenHistoryConfig::default(), sink.clone()).unwrap();
    let source = Arc::new(history.source_boundary("source").unwrap());
    let output = Arc::new(
        OutputTransport::new(
            "out",
            config(),
            Arc::new(Receipts::new(false, None, None)),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let control = output.control();
    let mut worker = Pipeline::new(vec![source, output, assistant.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    while !control.is_ready() {
        tokio::task::yield_now().await;
    }
    begin(&worker, "linearized").await;
    complete(&worker, "linearized").await;
    tokio::time::timeout(Duration::from_secs(3), sink.entered.notified())
        .await
        .unwrap();
    assert!(
        contents(&shared).is_empty(),
        "test gate precedes the synchronous context mutation"
    );
    let (attempting, attempted) = tokio::sync::oneshot::channel();
    let invalidated = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let changed = invalidated.clone();
    let invalidate = tokio::task::spawn_blocking(move || {
        attempting.send(()).unwrap();
        history.invalidate();
        changed.store(true, Ordering::SeqCst);
    });
    attempted.await.unwrap();
    assert!(
        !invalidated.load(Ordering::SeqCst),
        "invalidation cannot enter while the callback holds the proof lock"
    );
    *sink.release.0.lock().unwrap() = true;
    sink.release.1.notify_one();
    invalidate.await.unwrap();
    assert_eq!(
        contents(&shared),
        ["Heard"],
        "the commit linearized before invalidation"
    );
    send(&worker, Frame::interruption()).await;
    fence(&mut worker).await;
    assert_eq!(
        contents(&shared),
        ["Heard"],
        "a later interruption preserves committed speech"
    );
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}

#[derive(Default)]
struct ReplayOldSourceEnd(Mutex<Option<Frame>>);
#[async_trait]
impl FrameProcessor for ReplayOldSourceEnd {
    fn name(&self) -> &str {
        "replay-old-source-end"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream && matches!(frame.data, FrameData::LlmResponseEnd) {
            let mut saved = self.0.lock().unwrap();
            if saved.is_none() {
                *saved = Some(frame.clone());
            }
        }
        let replay = if matches!(&frame.data, FrameData::TtsStopped(stop) if stop.context_id.as_deref() == Some("new"))
        {
            self.0.lock().unwrap().clone()
        } else {
            None
        };
        ctx.push(frame, direction).await?;
        if let Some(replay) = replay {
            ctx.push(replay, Direction::Downstream).await?;
        }
        Ok(())
    }
}
#[tokio::test]
async fn stale_source_end_cannot_certify_a_new_failed_response() {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let history = SpokenHistory::new(
        SpokenHistoryConfig::default(),
        Arc::new(AssistantSink(assistant.clone())),
    )
    .unwrap();
    let source = Arc::new(history.source_boundary("source").unwrap());
    let output = Arc::new(
        OutputTransport::new(
            "out",
            config(),
            Arc::new(Receipts::new(false, None, None)),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let control = output.control();
    let mut worker = Pipeline::new(vec![
        source,
        Arc::new(ReplayOldSourceEnd::default()),
        output,
        assistant,
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    while !control.is_ready() {
        tokio::task::yield_now().await;
    }
    begin(&worker, "old").await;
    complete(&worker, "old").await;
    fence(&mut worker).await;
    assert_eq!(contents(&shared), ["Heard"]);
    begin(&worker, "new").await;
    send(&worker, audio(Some("new"), 80, 8000)).await;
    send(&worker, stop("new")).await;
    let mut failed = Frame::new(FrameData::LlmResponseEnd);
    failed.metadata.insert("failed".into(), true.into());
    send(&worker, failed).await;
    fence(&mut worker).await;
    assert_eq!(contents(&shared), ["Heard"]);
    assert_eq!(history.status().committed_contexts, 1);
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}

#[tokio::test]
async fn unstamped_source_is_never_treated_as_a_successful_response_proof() {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let history = SpokenHistory::new(
        SpokenHistoryConfig::default(),
        Arc::new(AssistantSink(assistant.clone())),
    )
    .unwrap();
    let output = Arc::new(
        OutputTransport::new(
            "out",
            config(),
            Arc::new(Receipts::new(false, None, None)),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let control = output.control();
    let mut worker = Pipeline::new(vec![output, assistant])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    while !control.is_ready() {
        tokio::task::yield_now().await;
    }
    begin(&worker, "unstamped").await;
    complete(&worker, "unstamped").await;
    fence(&mut worker).await;
    assert!(contents(&shared).is_empty());
    assert_eq!(history.status().committed_contexts, 0);
    assert!(history
        .status()
        .last_rejection
        .unwrap()
        .contains("source response boundary"));
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}

#[tokio::test]
async fn source_frame_identity_replay_cannot_create_a_fresh_attestation() {
    let mut f = fixture(
        Arc::new(Receipts::new(false, None, None)),
        SpokenHistoryConfig::default(),
    )
    .await;
    let original_start = Frame::new(FrameData::LlmResponseStart);
    send(&f.worker, original_start.clone()).await;
    send(&f.worker, start("one")).await;
    send(&f.worker, text("one", "Heard")).await;
    complete(&f.worker, "one").await;
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"]);
    send(&f.worker, original_start).await;
    send(&f.worker, start("two")).await;
    send(&f.worker, text("two", "Replayed")).await;
    complete(&f.worker, "two").await;
    fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"]);
    assert!(finish(f).await.errors.is_empty());
}

#[tokio::test]
async fn source_identity_budget_disables_attestation_without_eviction() {
    let mut f = fixture(
        Arc::new(Receipts::new(false, None, None)),
        SpokenHistoryConfig {
            max_source_frame_ids: 2,
            ..Default::default()
        },
    )
    .await;
    for id in ["one", "two", "three"] {
        begin(&f.worker, id).await;
        complete(&f.worker, id).await;
        fence(&mut f.worker).await;
    }
    assert_eq!(contents(&f.shared), ["Heard"]);
    assert!(f.history.status().exhausted);
    assert_eq!(f.history.status().retained_source_frame_ids, 2);
    assert_eq!(f.history.status().pending_source_responses, 0);
    assert!(finish(f).await.errors.is_empty());
}

struct HoldFirstSourceStart {
    entered: Notify,
    gate: Semaphore,
    first: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl FrameProcessor for HoldFirstSourceStart {
    fn name(&self) -> &str {
        "hold-first-source-start"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::LlmResponseStart)
            && self.first.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.gate.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn interruption_between_source_stamping_and_output_admission_invalidates_the_marker() {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let history = SpokenHistory::new(
        SpokenHistoryConfig::default(),
        Arc::new(AssistantSink(assistant.clone())),
    )
    .unwrap();
    let source = Arc::new(history.source_boundary("source").unwrap());
    let hold = Arc::new(HoldFirstSourceStart {
        entered: Notify::new(),
        gate: Semaphore::new(0),
        first: std::sync::atomic::AtomicBool::new(true),
    });
    let output = Arc::new(
        OutputTransport::new(
            "out",
            config(),
            Arc::new(Receipts::new(false, None, None)),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let control = output.control();
    let mut worker = Pipeline::new(vec![source, hold.clone(), output, assistant])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    while !control.is_ready() {
        tokio::task::yield_now().await;
    }
    // Keep the old marker alive deliberately to exercise ledger invalidation,
    // rather than relying on ordinary interruptible-frame queue removal.
    send(
        &worker,
        Frame::new(FrameData::LlmResponseStart).with_interruptible(false),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), hold.entered.notified())
        .await
        .unwrap();
    assert_eq!(history.status().pending_source_responses, 1);
    assert_eq!(history.status().pending_responses, 0);
    history.invalidate();
    send(&worker, Frame::interruption()).await;
    hold.gate.add_permits(1);
    fence(&mut worker).await;
    send(&worker, start("old")).await;
    send(&worker, text("old", "Stale")).await;
    complete(&worker, "old").await;
    fence(&mut worker).await;
    assert!(contents(&shared).is_empty());
    begin(&worker, "fresh").await;
    complete(&worker, "fresh").await;
    fence(&mut worker).await;
    assert_eq!(contents(&shared), ["Heard"]);
    worker.end().await.unwrap();
    assert!(worker.collect().await.unwrap().1.errors.is_empty());
}

async fn feedback_fence(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut feedback = vec![];
        for direction in [Direction::Downstream, Direction::Upstream] {
            let marker = Frame::new(FrameData::Flush(FlushFrame {
                origin: Some("feedback-fence".into()),
                returning: false,
            }));
            let id = marker.id;
            worker.sender().send(marker, direction).await.unwrap();
            loop {
                let frame = worker.recv().await.expect("pipeline feedback fence");
                if frame.frame.id == id {
                    break;
                }
                if matches!(
                    frame.frame.data,
                    FrameData::LlmContextAssistantTurn(_)
                        | FrameData::LlmContextAssistantTimestamp { .. }
                ) {
                    feedback.push(frame);
                }
            }
        }
        feedback
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn receipt_sink_projects_linked_feedback_after_commit_in_each_generation() {
    let backend = Arc::new(Receipts::new(false, None, None));
    let mut f = fixture(backend, SpokenHistoryConfig::default()).await;
    for id in ["first", "second"] {
        begin(&f.worker, id).await;
        assert!(feedback_fence(&mut f.worker).await.is_empty());
        assert_eq!(
            f.history.status().committed_contexts,
            if id == "first" { 0 } else { 1 }
        );
        complete(&f.worker, id).await;
        let frames = feedback_fence(&mut f.worker).await;
        assert_eq!(frames.len(), 3);
        assert_eq!(contents(&f.shared).last().unwrap(), "Heard");
        let turns: Vec<_> = frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_)))
            .collect();
        assert_eq!(turns.len(), 2);
        assert_ne!(turns[0].direction, turns[1].direction);
        assert_eq!(turns[0].frame.broadcast_sibling_id, Some(turns[1].frame.id));
        assert_eq!(turns[1].frame.broadcast_sibling_id, Some(turns[0].frame.id));
        assert!(turns.iter().all(
            |f| matches!(&f.frame.data, FrameData::LlmContextAssistantTurn(t) if t.text == "Heard")
        ));
        assert!(feedback_fence(&mut f.worker).await.is_empty());
        send(&f.worker, Frame::interruption()).await;
        assert!(feedback_fence(&mut f.worker).await.is_empty());
    }
    assert_eq!(f.history.status().committed_contexts, 2);
    assert!(finish(f).await.errors.is_empty());
}

#[path = "support/review_spoken_feedback.rs"]
mod review_spoken_feedback;

#[cfg(feature = "assistant-history")]
#[tokio::test]
async fn stopped_record_uses_real_media_receipt_and_committed_context() {
    let backend = Arc::new(Receipts::new(true, None, None));
    let mut f = fixture_with_adapter(backend.clone(), Default::default(), config(), true).await;
    let mut records = f.assistant.subscribe_stopped(Default::default()).unwrap();
    for id in ["first", "second"] {
        begin(&f.worker, id).await;
        complete(&f.worker, id).await;
        backend.wait_calls(if id == "first" { 1 } else { 2 }).await;
        assert!(records.try_recv().is_none(), "submission is not playback");
        backend.gate.as_ref().unwrap().add_permits(1);
        fence(&mut f.worker).await;
        let record = records.try_recv().expect("ordered media completion");
        assert_eq!(record.completion().text(), "Heard");
        assert!(!record.completion().interrupted());
        assert_eq!(contents(&f.shared).last().unwrap(), "Heard");
        assert!(records.try_recv().is_none());
    }
    f.worker.end().await.unwrap();
    assert!(f.worker.collect().await.unwrap().1.errors.is_empty());
    assert!(records.recv().await.is_none());
    assert_eq!(
        records.closed_reason(),
        Some(pipecat_turns::AssistantStoppedClose::End)
    );
}

#[cfg(feature = "assistant-history")]
#[tokio::test]
async fn discarded_media_receipt_never_delivers_successful_stopped_record() {
    let mut f = fixture_with_adapter(
        Arc::new(Receipts::new(false, Some(1), None)),
        Default::default(),
        config(),
        true,
    )
    .await;
    let mut records = f.assistant.subscribe_stopped(Default::default()).unwrap();
    begin(&f.worker, "discarded").await;
    complete(&f.worker, "discarded").await;
    fence(&mut f.worker).await;
    assert!(records.try_recv().is_none());
    assert!(contents(&f.shared).is_empty());
    f.worker.cancel("finish discarded receipt").await.unwrap();
    let _ = f.worker.collect().await.unwrap();
    while let Some(record) = records.recv().await {
        assert!(record.completion().interrupted());
    }
}

#[cfg(feature = "assistant-history")]
#[path = "support/assistant_stopped_start_probe.rs"]
mod assistant_stopped_start_probe;
