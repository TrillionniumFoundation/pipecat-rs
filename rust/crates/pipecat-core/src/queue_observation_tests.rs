use super::*;
use crate::{
    DetailedFunctionCallResultFrame, Direction, EagerSourceSessionOwner, Frame, FrameProcessor,
    FunctionCallResultProperties, LlmContextData, LocalAsyncCallback, NativeEagerConsumer,
    Pipeline, PipelineConfig, PipelineWorker, ProcessorContext, ProcessorError, SpeculationOwner,
    SpeculationStatus, ToolSet, TranscriptionFrame,
};
use std::sync::{atomic::AtomicBool, mpsc, Weak};
use std::time::Duration;

const WAIT: Duration = Duration::from_secs(3);

struct Capture(Mutex<Option<ProcessorContext>>);
#[crate::async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "queue-observation-capture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        context: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.0.lock().unwrap() = Some(context.clone());
        }
        context.push(frame, direction).await
    }
}

async fn contexts() -> (PipelineWorker, ProcessorContext, ProcessorContext) {
    let first = Arc::new(Capture(Mutex::new(None)));
    let second = Arc::new(Capture(Mutex::new(None)));
    let mut worker = Pipeline::new(vec![first.clone(), second.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("capture invocation"), Direction::Downstream)
        .await
        .unwrap();
    loop {
        let frame = tokio::time::timeout(WAIT, worker.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(frame.frame.data, FrameData::Text(_)) {
            break;
        }
    }
    let first = first.0.lock().unwrap().take().unwrap();
    let second = second.0.lock().unwrap().take().unwrap();
    assert!(first.current_generation().is_some());
    assert!(second.current_generation().is_some());
    (worker, first, second)
}

async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(WAIT, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

fn payload(eager: bool) -> FrameData {
    let text = TranscriptionFrame {
        text: "bound".into(),
        finalized: !eager,
        ..Default::default()
    };
    if eager {
        FrameData::EagerTranscription(text)
    } else {
        FrameData::Transcription(text)
    }
}
fn empty_context() -> LlmContextData {
    LlmContextData {
        messages: vec![],
        tools: ToolSet::Functions(vec![]),
        tool_choice: None,
    }
}

#[derive(Clone, Copy, Debug)]
enum Removal {
    QueuedInterruption,
    DirectInterruption,
    Discard,
}
enum DropAction {
    Here,
    Transfer,
}

struct DropProbe {
    owner: Option<EagerSourceSessionOwner>,
    queue: Weak<FrameQueue>,
    entered: mpsc::Sender<bool>,
    action: Mutex<mpsc::Receiver<DropAction>>,
    cleanup: mpsc::Sender<EagerSourceSessionOwner>,
    dropped_here: Arc<AtomicBool>,
    invoked: Arc<AtomicBool>,
}
impl DropProbe {
    fn note_invoked(&self) {
        self.invoked.store(true, Ordering::SeqCst);
    }
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        let under_q = self.queue.upgrade().is_some_and(|queue| {
            let locked = matches!(
                queue.queues.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            );
            locked
        });
        self.entered.send(under_q).unwrap();
        match self.action.get_mut().unwrap().recv().unwrap() {
            DropAction::Here => {
                drop(self.owner.take());
                self.dropped_here.store(true, Ordering::SeqCst);
            }
            DropAction::Transfer => self.cleanup.send(self.owner.take().unwrap()).unwrap(),
        }
    }
}

struct ProbeResult {
    entered_under_q: Option<bool>,
    before_release: Option<bool>,
    observed: Option<bool>,
    remover_ok: bool,
    dropped_here: bool,
    transferred: bool,
    callback_invoked: bool,
}
impl ProbeResult {
    fn assert_observation(self, expected: bool) {
        assert_eq!(
            self.entered_under_q,
            Some(true),
            "real callback destruction must hold Q"
        );
        assert!(self.remover_ok, "queue removal failed");
        assert!(
            self.before_release.is_some(),
            "observation must finish before callback releases Q"
        );
        assert_eq!(
            self.before_release,
            Some(expected),
            "published cutoff must decide admission while Q is held"
        );
        assert_eq!(self.observed, Some(expected));
        assert!(
            self.dropped_here,
            "positive path must destroy the actual source owner under Q"
        );
        assert!(!self.transferred);
        assert!(
            !self.callback_invoked,
            "destructor witness must not execute the callback"
        );
    }
}

// Timeout cleanup transfers the owner instead of destroying it under Q, then
// joins both threads before any assertion. A locked-read control cannot strand
// an observer holding L/C against a callback destructor waiting for that lock.
fn observe_during_callback_drop(
    queue: Arc<FrameQueue>,
    owner: EagerSourceSessionOwner,
    removal: Removal,
    observe: impl FnOnce() -> bool + Send + 'static,
) -> ProbeResult {
    queue.set_normal_paused(true);
    let (entered_tx, entered_rx) = mpsc::channel();
    let (action_tx, action_rx) = mpsc::channel();
    let (cleanup_tx, cleanup_rx) = mpsc::channel();
    let dropped_here = Arc::new(AtomicBool::new(false));
    let invoked = Arc::new(AtomicBool::new(false));
    let probe = DropProbe {
        owner: Some(owner),
        queue: Arc::downgrade(&queue),
        entered: entered_tx,
        action: Mutex::new(action_rx),
        cleanup: cleanup_tx,
        dropped_here: dropped_here.clone(),
        invoked: invoked.clone(),
    };
    let callback = LocalAsyncCallback::new(move || {
        probe.note_invoked();
        std::future::ready(Ok(()))
    });
    let frame = Frame::new(FrameData::FunctionCallResultDetailed(
        DetailedFunctionCallResultFrame {
            function_name: "drop-only".into(),
            tool_call_id: "drop-only".into(),
            arguments: serde_json::Value::Null,
            result: serde_json::Value::Null,
            run_llm: None,
            properties: Some(FunctionCallResultProperties {
                on_context_updated: Some(callback),
                ..Default::default()
            }),
            error: None,
        },
    ))
    .with_interruptible(!matches!(removal, Removal::Discard));
    queue
        .try_send(DirectedFrame::new(frame, Direction::Downstream))
        .unwrap();
    let remover_queue = queue.clone();
    let remover = std::thread::spawn(move || match removal {
        Removal::QueuedInterruption => remover_queue
            .try_send(DirectedFrame::new(
                Frame::interruption(),
                Direction::Downstream,
            ))
            .is_ok(),
        Removal::DirectInterruption => remover_queue.reset_interruptible() == 1,
        Removal::Discard => remover_queue.discard_normal() == 1,
    });
    let entered_under_q = entered_rx.recv_timeout(WAIT).ok();
    let (result_tx, result_rx) = mpsc::channel();
    let observer = entered_under_q.map(|_| {
        std::thread::spawn(move || {
            let result = observe();
            result_tx.send(result).unwrap();
            result
        })
    });
    let before_release = observer
        .as_ref()
        .and_then(|_| result_rx.recv_timeout(WAIT).ok());
    action_tx
        .send(if before_release.is_some() {
            DropAction::Here
        } else {
            DropAction::Transfer
        })
        .unwrap();
    let remover_ok = remover.join().unwrap_or(false);
    // If a setup regression did not remove the frame, drain it only after the
    // transfer instruction is already available to its destructor.
    queue.discard_normal();
    let observed = observer.and_then(|thread| thread.join().ok());
    let transferred_owner = cleanup_rx.try_recv().ok();
    let transferred = transferred_owner.is_some();
    drop(transferred_owner);
    queue.set_normal_paused(false);
    ProbeResult {
        entered_under_q,
        before_release,
        observed,
        remover_ok,
        dropped_here: dropped_here.load(Ordering::SeqCst),
        transferred,
        callback_invoked: invoked.load(Ordering::SeqCst),
    }
}

async fn source_callback_schedule(removal: Removal) {
    for inherited in [false, true] {
        let (worker, first, second) = contexts().await;
        let context = if inherited {
            second.forwarding_context(&first)
        } else {
            first.clone()
        };
        let source = EagerSourceSessionOwner::from_context(4096, &context).unwrap();
        let turn = source.begin_turn().unwrap();
        let prediction = source.predict(&turn, payload(true)).unwrap();
        assert!(
            prediction.is_live(),
            "pre-publication observation must remain current"
        );
        let observed_prediction = prediction.clone();
        let result = observe_during_callback_drop(
            first.runtime.queues[first.index].clone(),
            source,
            removal,
            move || observed_prediction.is_live(),
        );
        assert!(
            !prediction.is_live(),
            "real last-owner Drop must revoke the source"
        );
        finish(worker).await;
        result.assert_observation(matches!(removal, Removal::Discard));
    }
}

#[tokio::test]
async fn source_observation_returns_before_queued_interruption_callback_drop() {
    source_callback_schedule(Removal::QueuedInterruption).await;
}
#[tokio::test]
async fn source_observation_returns_before_direct_interruption_callback_drop() {
    source_callback_schedule(Removal::DirectInterruption).await;
}
#[tokio::test]
async fn source_observation_returns_before_uninterruptible_callback_discard() {
    source_callback_schedule(Removal::Discard).await;
}

async fn bound_callback_schedule(inherited: bool, effect: bool) {
    for removal in [Removal::QueuedInterruption, Removal::DirectInterruption] {
        let (worker, first, second) = contexts().await;
        let context = if inherited {
            second.forwarding_context(&first)
        } else {
            first.clone()
        };
        let health = context.health().generation();
        assert!(!context.source_is_stale_observed(context.current_generation()));
        // Standalone STT has no queue fence that could mask the retained LLM
        // admission read. An inherited test keeps the current LLM queue live.
        let source = EagerSourceSessionOwner::new(4096).unwrap();
        let turn = source.begin_turn().unwrap();
        let consumer = NativeEagerConsumer::new();
        assert!(turn.claim_start(&consumer));
        let prediction = source.predict(&turn, payload(true)).unwrap();
        let owner = Arc::new(
            SpeculationOwner::try_adopt_source_prediction(&prediction, &consumer).unwrap(),
        );
        let capability = owner.capability();
        assert!(capability.claim_inference());
        assert!(capability.bind_admission(context, health));
        let receipt = source.offer_final(&turn, payload(false)).unwrap();
        assert!(receipt.claim_final(&consumer));
        assert!(receipt.claim_stop(&consumer));
        if effect {
            assert!(owner.confirm_source_with_context(&receipt, &consumer, empty_context()));
            assert!(capability.release_after_boundary());
            assert!(
                capability.admits_effect(),
                "live effect control must be admitted"
            );
        }
        let observing_owner = owner.clone();
        let observing_capability = capability.clone();
        let result = observe_during_callback_drop(
            first.runtime.queues[first.index].clone(),
            source,
            removal,
            move || {
                if effect {
                    observing_capability.admits_effect()
                } else {
                    observing_owner.confirm_source_with_context(
                        &receipt,
                        &consumer,
                        empty_context(),
                    )
                }
            },
        );
        if effect {
            assert_eq!(
                capability.status(),
                SpeculationStatus::Released,
                "STT owner Drop cannot reclaim a transferred effect owner"
            );
        }
        finish(worker).await;
        result.assert_observation(false);
    }
}

#[tokio::test]
async fn bound_confirmation_observes_current_queue_before_callback_drop() {
    bound_callback_schedule(false, false).await;
}
#[tokio::test]
async fn bound_confirmation_observes_inherited_queue_before_callback_drop() {
    bound_callback_schedule(true, false).await;
}
#[tokio::test]
async fn released_bound_effect_observes_current_queue_before_callback_drop() {
    bound_callback_schedule(false, true).await;
}
#[tokio::test]
async fn released_bound_effect_observes_inherited_queue_before_callback_drop() {
    bound_callback_schedule(true, true).await;
}

#[test]
fn published_queue_cutoff_matches_both_writers_without_regression() {
    let queue = FrameQueue::new(8);
    assert_eq!(queue.observed_generation(), 0);
    assert_eq!(queue.generation(), 0);
    let (first, _) = queue.interrupt_now(None, None);
    assert_eq!(queue.observed_generation(), first);
    assert_eq!(queue.observed_generation(), queue.generation());
    let interruption = Frame::interruption();
    queue
        .try_send(DirectedFrame::new(
            interruption.clone(),
            Direction::Downstream,
        ))
        .unwrap();
    let second = queue.generation();
    assert!(second > first);
    assert_eq!(queue.observed_generation(), second);
    queue
        .try_send(DirectedFrame::new(interruption, Direction::Downstream))
        .unwrap();
    assert_eq!(
        queue.observed_generation(),
        second,
        "duplicate cannot advance or regress cutoff"
    );
    let older = AtomicU64::new(first);
    let (reserved, _) = queue.interrupt_now(None, Some(&older));
    assert_eq!(reserved, first);
    assert_eq!(queue.generation(), second);
    assert_eq!(
        queue.observed_generation(),
        second,
        "publish the locked max, not reserved epoch"
    );
    let stale_frame = Frame::interruption();
    stale_frame
        .interruption_epoch
        .as_ref()
        .unwrap()
        .store(first, Ordering::SeqCst);
    queue
        .try_send(DirectedFrame::new(stale_frame, Direction::Downstream))
        .unwrap();
    assert_eq!(queue.observed_generation(), second);
    queue
        .try_send(DirectedFrame::new(
            Frame::text("remove"),
            Direction::Downstream,
        ))
        .unwrap();
    assert_eq!(queue.reset_before_generation(second + 1), 1);
    assert_eq!(
        queue.generation(),
        second,
        "existing reset-only operation does not change cutoff"
    );
    assert_eq!(queue.observed_generation(), second);
    queue.close();
    assert_eq!(queue.observed_generation(), second);
}

#[tokio::test]
async fn observed_context_staleness_preserves_none_current_and_inherited_semantics() {
    let (worker, first, second) = contexts().await;
    let nested = second.forwarding_context(&first);
    let epoch = first.current_generation().unwrap();
    assert!(!first.source_is_stale_observed(Some(epoch)));
    assert!(!nested.source_is_stale_observed(Some(epoch)));
    first.runtime.queues[first.index].reset_interruptible();
    let current = first.runtime.queues[first.index].generation();
    assert!(current > epoch);
    assert!(first.source_is_stale_observed(Some(epoch)));
    assert!(
        !first.source_is_stale_observed(Some(current)),
        "comparison is strictly greater"
    );
    assert!(
        nested.source_is_stale_observed(Some(epoch)),
        "inherited cutoff must be checked"
    );
    assert!(
        !nested.source_is_stale_observed(None),
        "None skips current and inherited checks"
    );
    for generation in [None, Some(epoch), Some(current)] {
        assert_eq!(
            first.source_is_stale_observed(generation),
            first.source_is_stale(generation)
        );
        assert_eq!(
            nested.source_is_stale_observed(generation),
            nested.source_is_stale(generation)
        );
    }
    let mut raw_none = nested.clone();
    raw_none.generation = None;
    assert!(!raw_none.source_is_stale_observed(raw_none.current_generation()));
    assert!(
        !raw_none
            .invocation_fence()
            .unwrap()
            .observe()
            .unwrap()
            .is_current(),
        "invocation-fence origins remain independently checked even with no direct epoch"
    );
    finish(worker).await;
}
