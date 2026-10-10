//! Independent baseline-correction acceptance probes. These run the real
//! assistant processor and synthetic Played backend; no provider or hardware.
//! Enable assistant-history. Original failing held-Start probes stay unchanged.
#![cfg(feature = "assistant-history")]

use async_trait::async_trait;
use pipecat_audio::{encode_pcm16_le, PcmFormat};
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_services::{adapters::request_body, CompletionRequest, Context, Provider};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator, SharedContext,
};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

#[derive(Default)]
struct ImmediatePlayed {
    calls: AtomicUsize,
    changed: Notify,
}
#[async_trait]
impl OutputBackend for ImmediatePlayed {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
impl ImmediatePlayed {
    async fn calls_at_least(&self, count: usize) {
        loop {
            let changed = self.changed.notified();
            if self.calls.load(Ordering::SeqCst) >= count {
                return;
            }
            changed.await;
        }
    }
    async fn assert_blocked_at(&self, count: usize) {
        // An explicit negative observation window, bounded well below the
        // configured readiness timeout; the real downstream gate is entered.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), self.calls_at_least(count + 1))
                .await
                .is_err(),
            "backend audio submission overtook the real assistant boundary"
        );
        assert_eq!(self.calls.load(Ordering::SeqCst), count);
    }
}
#[derive(Clone, Copy)]
enum HoldKind {
    SourceStart,
    TtsStart,
    SourceEnd,
}
struct HoldBoundary {
    kind: HoldKind,
    first: AtomicBool,
    entered: Notify,
    release: Semaphore,
}
impl HoldBoundary {
    fn new(kind: HoldKind) -> Arc<Self> {
        Arc::new(Self {
            kind,
            first: AtomicBool::new(true),
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
    async fn wait_entered(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.entered.notified())
            .await
            .unwrap();
    }
}
#[async_trait]
impl FrameProcessor for HoldBoundary {
    fn name(&self) -> &str {
        "independent-downstream-boundary-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let matches = match self.kind {
            HoldKind::SourceStart => matches!(frame.data, FrameData::LlmResponseStart),
            HoldKind::TtsStart => matches!(frame.data, FrameData::TtsStarted(_)),
            HoldKind::SourceEnd => matches!(frame.data, FrameData::LlmResponseEnd),
        };
        if direction == Direction::Downstream && matches && self.first.swap(false, Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}
#[derive(Default)]
struct SourceAdmissions {
    starts: AtomicUsize,
    changed: Notify,
}
#[async_trait]
impl FrameProcessor for SourceAdmissions {
    fn name(&self) -> &str {
        "independent-source-admission-observer"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let start =
            direction == Direction::Downstream && matches!(frame.data, FrameData::LlmResponseStart);
        ctx.push(frame, direction).await?;
        if start {
            self.starts.fetch_add(1, Ordering::SeqCst);
            self.changed.notify_waiters();
        }
        Ok(())
    }
}
impl SourceAdmissions {
    async fn wait_starts(&self, count: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = self.changed.notified();
                if self.starts.load(Ordering::SeqCst) >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}
struct Fixture {
    worker: PipelineWorker,
    history: SpokenHistory,
    shared: SharedContext,
    backend: Arc<ImmediatePlayed>,
    hold: Arc<HoldBoundary>,
    admissions: Arc<SourceAdmissions>,
}
async fn fixture(kind: HoldKind) -> Fixture {
    fixture_with_timeout(kind, Duration::from_secs(5), true).await
}
async fn fixture_with_timeout(
    kind: HoldKind,
    timeout: Duration,
    install_assistant: bool,
) -> Fixture {
    let shared = SharedContext::new(Context::default());
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    let history = SpokenHistory::new(
        SpokenHistoryConfig::default(),
        Arc::new(AssistantSpokenHistorySink::new(assistant.clone())),
    )
    .unwrap();
    let source = Arc::new(
        history
            .source_boundary("independent-spoken-source")
            .unwrap(),
    );
    let backend = Arc::new(ImmediatePlayed::default());
    let output = Arc::new(
        OutputTransport::new(
            "independent-output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: timeout,
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history.clone())
        .unwrap(),
    );
    let control = output.control();
    let hold = HoldBoundary::new(kind);
    let admissions = Arc::new(SourceAdmissions::default());
    let mut processors: Vec<Arc<dyn FrameProcessor>> =
        vec![source, admissions.clone(), output, hold.clone()];
    if install_assistant {
        processors.push(assistant);
    }
    let worker = Pipeline::new(processors)
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while !control.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    Fixture {
        worker,
        history,
        shared,
        backend,
        hold,
        admissions,
    }
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
fn signed(data: FrameData, response: u64, signature: Option<&str>) -> Frame {
    let continuation = match signature {
        None => NativeLlmContinuation::start(response, LlmContinuationProvider::Anthropic),
        Some(signature) => NativeLlmContinuation::new(
            response,
            LlmContinuationProvider::Anthropic,
            vec![
                LlmContinuationBlock::Thinking {
                    text: format!("thought-{signature}"),
                    signature: signature.into(),
                },
                LlmContinuationBlock::Text("Heard".into()),
            ],
        ),
    }
    .unwrap();
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(Arc::new(continuation))
        .unwrap();
    frame
}
async fn response(worker: &PipelineWorker, id: &str, signature: Option<&str>) {
    let response = NativeLlmContinuation::next_response_id().unwrap();
    send(
        worker,
        if signature.is_some() {
            signed(FrameData::LlmResponseStart, response, None)
        } else {
            Frame::new(FrameData::LlmResponseStart)
        },
    )
    .await;
    send(
        worker,
        Frame::new(FrameData::TtsStarted(TtsStartedFrame {
            context_id: Some(id.into()),
            append_to_context: true,
        })),
    )
    .await;
    send(
        worker,
        Frame::new(FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new("Heard"),
            context_id: Some(id.into()),
            raw_text: None,
            aggregated_by: "independent-fixture".into(),
            will_be_spoken: true,
        })),
    )
    .await;
    let mut pcm = AudioRawFrame::new(encode_pcm16_le(&[1000; 80]), 8000, 1).unwrap();
    pcm.context_id = Some(id.into());
    send(worker, Frame::new(FrameData::TtsAudio(pcm))).await;
    send(
        worker,
        Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
            context_id: Some(id.into()),
        })),
    )
    .await;
    send(
        worker,
        if let Some(signature) = signature {
            signed(FrameData::LlmResponseEnd, response, Some(signature))
        } else {
            Frame::new(FrameData::LlmResponseEnd)
        },
    )
    .await;
}
async fn drain(worker: &mut PipelineWorker) {
    let frame = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("independent-readiness-drain".into()),
        returning: false,
    }));
    let id = frame.id;
    send(worker, frame).await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = worker.recv().await {
            if frame.frame.id == id {
                return;
            }
        }
        panic!("pipeline ended before readiness fence");
    })
    .await
    .unwrap();
}
fn signed_blocks(shared: &SharedContext) -> Vec<Value> {
    let request = request_body(
        Provider::Anthropic,
        &CompletionRequest::new("synthetic", shared.snapshot().unwrap()),
    )
    .unwrap();
    request["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap().iter().cloned())
        .collect()
}
async fn finish(f: Fixture) {
    f.worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(5), f.worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        report.errors.is_empty(),
        "unexpected pipeline errors: {:?}",
        report.errors
    );
}

#[tokio::test]
async fn original_start_is_reduced_before_any_backend_submission() {
    let mut f = fixture(HoldKind::SourceStart).await;
    response(&f.worker, "first", None).await;
    f.hold.wait_entered().await;
    f.backend.assert_blocked_at(0).await;
    assert_eq!(f.history.status().committed_contexts, 0);
    assert!(f.shared.snapshot().unwrap().messages.is_empty());
    f.hold.release.add_permits(1);
    drain(&mut f.worker).await;
    assert_eq!(f.backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.history.status().committed_contexts, 1);
    assert_eq!(f.shared.snapshot().unwrap().messages.len(), 1);
    finish(f).await;
}

#[tokio::test]
async fn signed_tts_registration_is_reduced_before_any_backend_submission() {
    let mut f = fixture(HoldKind::TtsStart).await;
    response(&f.worker, "signed", Some("signature-one+/==")).await;
    f.hold.wait_entered().await;
    f.backend.assert_blocked_at(0).await;
    assert_eq!(f.history.status().committed_contexts, 0);
    f.hold.release.add_permits(1);
    drain(&mut f.worker).await;
    assert_eq!(f.backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.history.status().committed_contexts, 1);
    assert_eq!(
        signed_blocks(&f.shared),
        vec![
            json!({"type":"thinking","thinking":"thought-signature-one+/==","signature":"signature-one+/=="}),
            json!({"type":"text","text":"Heard"}),
        ]
    );
    finish(f).await;
}

#[tokio::test]
async fn queued_next_source_cannot_cross_prior_signed_end_or_merge_identical_text() {
    let mut f = fixture(HoldKind::SourceEnd).await;
    response(&f.worker, "a", Some("signature-a+/==")).await;
    f.hold.wait_entered().await;
    assert_eq!(f.backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(f.history.status().committed_contexts, 1);
    response(&f.worker, "b", Some("signature-b+/==")).await;
    f.admissions.wait_starts(2).await;
    f.backend.assert_blocked_at(1).await;
    assert_eq!(f.history.status().committed_contexts, 1);
    f.hold.release.add_permits(1);
    drain(&mut f.worker).await;
    assert_eq!(f.backend.calls.load(Ordering::SeqCst), 2);
    assert_eq!(f.history.status().committed_contexts, 2);
    assert_eq!(
        signed_blocks(&f.shared),
        vec![
            json!({"type":"thinking","thinking":"thought-signature-a+/==","signature":"signature-a+/=="}),
            json!({"type":"text","text":"Heard"}),
            json!({"type":"thinking","thinking":"thought-signature-b+/==","signature":"signature-b+/=="}),
            json!({"type":"text","text":"Heard"}),
        ]
    );
    finish(f).await;
}

// Unlike the historical reproduction, these scenarios require zero backend
// submission until readiness. They do not discard an accepted Played receipt.
#[tokio::test]
async fn timed_out_start_never_releases_its_later_pcm_after_the_gate_opens() {
    let f = fixture_with_timeout(HoldKind::SourceStart, Duration::from_millis(200), true).await;
    response(&f.worker, "times-out", None).await;
    f.hold.wait_entered().await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.history.status().last_rejection.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(f.backend.calls.load(Ordering::SeqCst), 0);
    f.hold.release.add_permits(1);
    // A media flush intentionally reports the preceding rejected playback.
    // End still drains and must not submit the already-denied response's PCM.
    f.worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(5), f.worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !report.errors.is_empty(),
        "readiness timeout must be reported"
    );
    assert_eq!(f.backend.calls.load(Ordering::SeqCst), 0);
    assert!(f.shared.snapshot().unwrap().messages.is_empty());
    assert_eq!(f.history.status().committed_contexts, 0);
}

#[tokio::test]
async fn cancellation_settles_pending_readiness_without_playback_or_error_conversion() {
    let f = fixture(HoldKind::SourceStart).await;
    response(&f.worker, "cancelled", None).await;
    f.hold.wait_entered().await;
    f.worker
        .cancel("independent cancel while waiting for original Start")
        .await
        .unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(5), f.worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        report.errors.is_empty(),
        "ordinary Cancel became a readiness error: {:?}",
        report.errors
    );
    assert_eq!(f.backend.calls.load(Ordering::SeqCst), 0);
    assert!(f.shared.snapshot().unwrap().messages.is_empty());
}

#[tokio::test]
async fn receipt_only_assistant_adapter_cannot_claim_readiness_without_actual_processor() {
    let f = fixture_with_timeout(HoldKind::SourceStart, Duration::from_millis(200), false).await;
    response(&f.worker, "no-assistant-node", None).await;
    f.hold.wait_entered().await;
    f.hold.release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(3), async {
        while f.history.status().last_rejection.is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    f.worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(5), f.worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(!report.errors.is_empty());
    assert_eq!(f.backend.calls.load(Ordering::SeqCst), 0);
    assert!(f.shared.snapshot().unwrap().messages.is_empty());
    assert_eq!(f.history.status().committed_contexts, 0);
}

#[path = "support/readiness_failures.rs"]
mod readiness_failures;
