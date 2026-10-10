use pipecat_core::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

const LIMIT: Duration = Duration::from_secs(3);

async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(LIMIT, worker.recv())
        .await
        .expect("frame timed out")
        .expect("output closed")
}

async fn started(processors: Vec<Arc<dyn FrameProcessor>>) -> PipelineWorker {
    let mut worker = Pipeline::new(processors)
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    worker
}

async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}

async fn finish(worker: PipelineWorker) -> (Vec<String>, WorkerReport) {
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(LIMIT, worker.collect())
        .await
        .expect("worker did not end")
        .unwrap();
    let texts = frames
        .into_iter()
        .filter_map(|item| match item.frame.data {
            FrameData::Text(text) => Some(text.text),
            _ => None,
        })
        .collect();
    (texts, report)
}

struct SystemGate {
    entered: Semaphore,
    release: Semaphore,
}
impl SystemGate {
    fn new() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        }
    }
}
#[async_trait]
impl FrameProcessor for SystemGate {
    fn name(&self) -> &str {
        "review-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::UserStartedSpeaking) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}

async fn pause_and_hold(worker: &mut PipelineWorker, gate: &SystemGate) {
    send(
        worker,
        Frame::new(FrameData::ProcessorPauseUrgent {
            processor: "review-gate".into(),
        }),
    )
    .await;
    assert!(matches!(
        next(worker).await.frame.data,
        FrameData::ProcessorPauseUrgent { .. }
    ));
    send(worker, Frame::new(FrameData::UserStartedSpeaking)).await;
    tokio::time::timeout(LIMIT, gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

async fn resume(worker: &PipelineWorker) {
    send(
        worker,
        Frame::new(FrameData::ProcessorResumeUrgent {
            processor: "review-gate".into(),
        }),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_admission_after_interruption_survives_delayed_handler() {
    let gate = Arc::new(SystemGate::new());
    let mut worker = started(vec![gate.clone()]).await;
    pause_and_hold(&mut worker, &gate).await;
    send(&worker, Frame::text("obsolete")).await;
    send(&worker, Frame::interruption()).await;
    send(&worker, Frame::text("fresh")).await;
    resume(&worker).await;
    gate.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert_eq!(texts, ["fresh"]);
    assert_eq!(report.dropped_frames, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_interruption_identity_does_not_invalidate_fresh_admission() {
    let gate = Arc::new(SystemGate::new());
    let mut worker = started(vec![gate.clone()]).await;
    pause_and_hold(&mut worker, &gate).await;
    let interruption = Frame::interruption();
    send(&worker, interruption.clone()).await;
    send(&worker, Frame::text("fresh")).await;
    send(&worker, interruption).await;
    resume(&worker).await;
    gate.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert_eq!(texts, ["fresh"]);
    assert_eq!(report.dropped_frames, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distinct_second_interruption_invalidates_only_earlier_generations() {
    let gate = Arc::new(SystemGate::new());
    let mut worker = started(vec![gate.clone()]).await;
    pause_and_hold(&mut worker, &gate).await;
    send(&worker, Frame::text("generation-zero")).await;
    send(&worker, Frame::interruption()).await;
    send(&worker, Frame::text("generation-one")).await;
    send(&worker, Frame::text("retained").with_interruptible(false)).await;
    send(&worker, Frame::interruption()).await;
    send(&worker, Frame::text("generation-two")).await;
    resume(&worker).await;
    gate.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert_eq!(texts, ["retained", "generation-two"]);
    assert_eq!(report.dropped_frames, 2);
}

struct BroadcastFresh;
#[async_trait]
impl FrameProcessor for BroadcastFresh {
    fn name(&self) -> &str {
        "review-broadcast"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::InputText(_)) {
            ctx.broadcast_interruption().await?;
            ctx.push(Frame::text("fresh-down"), Direction::Downstream)
                .await?;
            return ctx.push(Frame::text("fresh-up"), Direction::Upstream).await;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_preserves_same_invocation_fresh_frames_across_both_branches() {
    let mut worker = started(vec![
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
        Arc::new(BroadcastFresh),
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
    ])
    .await;
    send(
        &worker,
        Frame::new(FrameData::InputText(TextFrame::new("new turn"))),
    )
    .await;
    let mut observed = Vec::new();
    while observed.len() < 2 {
        if let DirectedFrame {
            frame:
                Frame {
                    data: FrameData::Text(text),
                    ..
                },
            direction,
        } = next(&mut worker).await
        {
            observed.push((text.text, direction));
        }
    }
    assert!(observed.contains(&("fresh-down".into(), Direction::Downstream)));
    assert!(observed.contains(&("fresh-up".into(), Direction::Upstream)));
    finish(worker).await;
}

/// A synchronous section models a task already executing when cancellation is
/// requested: aborting its handle cannot preempt code before its next yield.
struct BlockingEmitter {
    entered: Arc<Semaphore>,
    released: Arc<(Mutex<bool>, Condvar)>,
    attempted: Arc<Semaphore>,
    interruptible_task: bool,
    enqueue_self: bool,
    cloned_before_broadcast: bool,
    broadcast_ready: Semaphore,
}
impl BlockingEmitter {
    fn new(interruptible_task: bool, enqueue_self: bool) -> Self {
        Self {
            entered: Arc::new(Semaphore::new(0)),
            released: Arc::new((Mutex::new(false), Condvar::new())),
            attempted: Arc::new(Semaphore::new(0)),
            interruptible_task,
            enqueue_self,
            cloned_before_broadcast: false,
            broadcast_ready: Semaphore::new(0),
        }
    }
    fn release(&self) {
        let (lock, changed) = &*self.released;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
}
#[async_trait]
impl FrameProcessor for BlockingEmitter {
    fn name(&self) -> &str {
        "review-emitter"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !matches!(frame.data, FrameData::LlmRun) {
            return ctx.push(frame, direction).await;
        }
        let old_ctx = ctx.clone();
        let entered = self.entered.clone();
        let released = self.released.clone();
        let attempted = self.attempted.clone();
        let enqueue_self = self.enqueue_self;
        ctx.spawn(
            "review-blocking-emitter",
            self.interruptible_task,
            async move {
                entered.add_permits(1);
                {
                    let (lock, changed) = &*released;
                    let released = lock.lock().unwrap();
                    let (released, _) = changed
                        .wait_timeout_while(released, LIMIT, |released| !*released)
                        .unwrap();
                    assert!(*released, "test did not release synchronous emitter");
                }
                let result = if enqueue_self {
                    old_ctx
                        .queue(Frame::text("late"), Direction::Downstream)
                        .await
                } else {
                    old_ctx
                        .push(Frame::text("late"), Direction::Downstream)
                        .await
                };
                attempted.add_permits(1);
                result
            },
        )?;
        if self.cloned_before_broadcast {
            tokio::time::timeout(LIMIT, self.broadcast_ready.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            ctx.broadcast_interruption().await?;
            ctx.push(Frame::text("fresh"), direction).await?;
        }
        Ok(())
    }
}

async fn exercise_late_emitter(
    interruptible: bool,
    enqueue_self: bool,
) -> (Vec<String>, WorkerReport) {
    let emitter = Arc::new(BlockingEmitter::new(interruptible, enqueue_self));
    let mut worker = started(vec![emitter.clone(), Arc::new(IdentityProcessor)]).await;
    send(&worker, Frame::new(FrameData::LlmRun)).await;
    tokio::time::timeout(LIMIT, emitter.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    send(&worker, Frame::interruption()).await;
    let interruption = next(&mut worker).await;
    emitter.release();
    assert!(matches!(interruption.frame.data, FrameData::Interruption));
    tokio::time::timeout(LIMIT, emitter.attempted.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    finish(worker).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruptible_background_cannot_emit_after_interruption_reaches_output() {
    let (texts, report) = exercise_late_emitter(true, false).await;
    assert!(texts.is_empty(), "stale task leaked output: {texts:?}");
    assert!(report.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruptible_background_cannot_requeue_after_interruption() {
    let (texts, report) = exercise_late_emitter(true, true).await;
    assert!(
        texts.is_empty(),
        "stale task re-entered normal processing: {texts:?}"
    );
    assert!(report.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opted_out_background_can_emit_after_interruption() {
    let (texts, report) = exercise_late_emitter(false, false).await;
    assert_eq!(texts, ["late"]);
    assert!(report.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcast_does_not_refresh_previously_cloned_background_context() {
    let mut emitter = BlockingEmitter::new(true, false);
    emitter.cloned_before_broadcast = true;
    let emitter = Arc::new(emitter);
    let mut worker = started(vec![emitter.clone(), Arc::new(IdentityProcessor)]).await;
    send(&worker, Frame::new(FrameData::LlmRun)).await;
    tokio::time::timeout(LIMIT, emitter.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    emitter.broadcast_ready.add_permits(1);
    let mut interruptions = 0;
    let mut texts = Vec::new();
    while interruptions < 2 {
        match next(&mut worker).await.frame.data {
            FrameData::Interruption => interruptions += 1,
            FrameData::Text(text) => texts.push(text.text),
            _ => {}
        }
    }
    emitter.release();
    tokio::time::timeout(LIMIT, emitter.attempted.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let (remaining, report) = finish(worker).await;
    texts.extend(remaining);
    assert_eq!(texts, ["fresh"]);
    assert!(report.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_normal_lane_keeps_system_admission_available_and_fresh_sender_progresses() {
    let gate = Arc::new(SystemGate::new());
    let mut worker = Pipeline::new(vec![gate.clone()])
        .start(PipelineConfig {
            queue_capacity: 1,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut worker).await;
    pause_and_hold(&mut worker, &gate).await;
    send(&worker, Frame::text("obsolete")).await;
    send(&worker, Frame::interruption()).await;
    let sender = worker.sender();
    let admitted = Arc::new(AtomicBool::new(false));
    let completed = admitted.clone();
    let pending = tokio::spawn(async move {
        sender
            .send(Frame::text("fresh"), Direction::Downstream)
            .await
            .unwrap();
        completed.store(true, Ordering::SeqCst);
    });
    resume(&worker).await;
    gate.release.add_permits(1);
    tokio::time::timeout(LIMIT, pending).await.unwrap().unwrap();
    assert!(admitted.load(Ordering::SeqCst));
    let (texts, report) = finish(worker).await;
    assert_eq!(texts, ["fresh"]);
    assert_eq!(report.dropped_frames, 1);
}

struct UpstreamFenceGate {
    entered: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl FrameProcessor for UpstreamFenceGate {
    fn name(&self) -> &str {
        "review-upstream-fence"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Interruption) && direction == Direction::Upstream {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_upstream_interruption_does_not_promote_old_remote_output() {
    let emitter = Arc::new(BlockingEmitter::new(true, false));
    let fence = Arc::new(UpstreamFenceGate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut worker = started(vec![
        emitter.clone(),
        fence.clone(),
        Arc::new(BroadcastFresh),
        Arc::new(IdentityProcessor),
    ])
    .await;
    send(&worker, Frame::new(FrameData::LlmRun)).await;
    tokio::time::timeout(LIMIT, emitter.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    send(
        &worker,
        Frame::new(FrameData::InputText(TextFrame::new("new turn"))),
    )
    .await;
    tokio::time::timeout(LIMIT, fence.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    emitter.release();
    tokio::time::timeout(LIMIT, emitter.attempted.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    fence.release.add_permits(1);
    let mut texts = Vec::new();
    while !texts.contains(&"fresh-down".to_owned()) || !texts.contains(&"fresh-up".to_owned()) {
        if let FrameData::Text(text) = next(&mut worker).await.frame.data {
            texts.push(text.text);
        }
    }
    let (rest, report) = finish(worker).await;
    texts.extend(rest);
    assert!(
        !texts.iter().any(|text| text == "late"),
        "pre-interruption output was promoted by a later-node admission: {texts:?}"
    );
    assert!(report.errors.is_empty());
}

struct DirectNormalEmitter(Arc<BlockingEmitter>);
#[async_trait]
impl FrameProcessor for DirectNormalEmitter {
    fn name(&self) -> &str {
        "review-direct-emitter"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmRun) {
            self.0.entered.add_permits(1);
            {
                let (lock, changed) = &*self.0.released;
                let released = lock.lock().unwrap();
                let (released, _) = changed
                    .wait_timeout_while(released, LIMIT, |released| !*released)
                    .unwrap();
                assert!(
                    *released,
                    "test did not release synchronous normal processor"
                );
            }
            return ctx.push(Frame::text("late"), direction).await;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn normal_emission_is_fenced_at_interruption_admission_before_handler_runs() {
    let control = Arc::new(BlockingEmitter::new(true, false));
    let worker = started(vec![
        Arc::new(DirectNormalEmitter(control.clone())),
        Arc::new(IdentityProcessor),
    ])
    .await;
    send(&worker, Frame::new(FrameData::LlmRun)).await;
    tokio::time::timeout(LIMIT, control.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    send(&worker, Frame::interruption()).await;
    control.release();
    let (texts, report) = finish(worker).await;
    assert!(
        texts.is_empty(),
        "stale normal invocation emitted before its system loop could run: {texts:?}"
    );
    assert!(report.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_siblings_share_one_fence_when_their_paths_rejoin() {
    let gate = Arc::new(SystemGate::new());
    let mut worker = started(vec![gate.clone()]).await;
    pause_and_hold(&mut worker, &gate).await;
    let (down, up) = Frame::interruption().broadcast_pair();
    send(&worker, down).await;
    send(&worker, Frame::text("fresh")).await;
    worker.sender().send(up, Direction::Upstream).await.unwrap();
    resume(&worker).await;
    gate.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert_eq!(texts, ["fresh"]);
    assert_eq!(report.dropped_frames, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_external_admission_survives_while_upstream_interruption_is_delayed() {
    let fence = Arc::new(UpstreamFenceGate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut worker = started(vec![
        Arc::new(IdentityProcessor),
        fence.clone(),
        Arc::new(BroadcastFresh),
        Arc::new(IdentityProcessor),
    ])
    .await;
    send(
        &worker,
        Frame::new(FrameData::InputText(TextFrame::new("new turn"))),
    )
    .await;
    tokio::time::timeout(LIMIT, fence.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    send(&worker, Frame::text("fresh-external")).await;
    fence.release.add_permits(1);
    let mut texts = Vec::new();
    while !texts.contains(&"fresh-down".to_owned()) || !texts.contains(&"fresh-up".to_owned()) {
        if let FrameData::Text(text) = next(&mut worker).await.frame.data {
            texts.push(text.text);
        }
    }
    let (rest, report) = finish(worker).await;
    texts.extend(rest);
    assert!(
        texts.iter().any(|text| text == "fresh-external"),
        "post-interruption input lost its causal generation: {texts:?}"
    );
    assert!(report.errors.is_empty());
}

struct AwaitAfterBroadcast {
    entered: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl FrameProcessor for AwaitAfterBroadcast {
    fn name(&self) -> &str {
        "review-await-after-broadcast"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmRun) {
            ctx.broadcast_interruption().await?;
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            return ctx.push(Frame::text("fresh-after-await"), direction).await;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deliberate_broadcast_refreshes_active_invocation_across_await() {
    let source = Arc::new(AwaitAfterBroadcast {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut worker = started(vec![source.clone(), Arc::new(IdentityProcessor)]).await;
    send(&worker, Frame::new(FrameData::LlmRun)).await;
    tokio::time::timeout(LIMIT, source.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let mut interruptions = 0;
    while interruptions < 2 {
        if matches!(next(&mut worker).await.frame.data, FrameData::Interruption) {
            interruptions += 1;
        }
    }
    source.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert_eq!(texts, ["fresh-after-await"]);
    assert!(report.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn later_distinct_interruption_cancels_refreshed_active_invocation() {
    let source = Arc::new(AwaitAfterBroadcast {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let mut worker = started(vec![source.clone(), Arc::new(IdentityProcessor)]).await;
    send(&worker, Frame::new(FrameData::LlmRun)).await;
    tokio::time::timeout(LIMIT, source.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let mut interruptions = 0;
    while interruptions < 2 {
        if matches!(next(&mut worker).await.frame.data, FrameData::Interruption) {
            interruptions += 1;
        }
    }
    send(&worker, Frame::interruption()).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    source.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert!(
        texts.is_empty(),
        "new distinct interruption did not cancel refreshed invocation: {texts:?}"
    );
    assert!(report.errors.is_empty());
}

struct InterruptionObserver {
    calls: AtomicUsize,
    observed: Semaphore,
}
#[async_trait]
impl FrameProcessor for InterruptionObserver {
    fn name(&self) -> &str {
        "review-interruption-observer"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Interruption) {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.observed.add_permits(1);
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn older_interruption_arriving_after_newer_fence_does_not_reinvoke_processor() {
    let source = Arc::new(InterruptionObserver {
        calls: AtomicUsize::new(0),
        observed: Semaphore::new(0),
    });
    let fence = Arc::new(UpstreamFenceGate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let worker = started(vec![
        source.clone(),
        fence.clone(),
        Arc::new(BroadcastFresh),
        Arc::new(IdentityProcessor),
    ])
    .await;
    send(
        &worker,
        Frame::new(FrameData::InputText(TextFrame::new("first interruption"))),
    )
    .await;
    tokio::time::timeout(LIMIT, fence.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    send(&worker, Frame::interruption()).await;
    tokio::time::timeout(LIMIT, source.observed.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    send(&worker, Frame::text("fresh-after-second")).await;
    fence.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        1,
        "older delayed interruption re-entered processor state after newer fence"
    );
    assert!(texts.iter().any(|text| text == "fresh-after-second"));
    assert!(report.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replayed_old_interruption_does_not_become_fresh_after_many_newer_events() {
    let gate = Arc::new(SystemGate::new());
    let mut worker = started(vec![gate.clone()]).await;
    let original = Frame::interruption();
    send(&worker, original.clone()).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    for _ in 0..512 {
        send(&worker, Frame::interruption()).await;
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::Interruption
        ));
    }
    pause_and_hold(&mut worker, &gate).await;
    send(&worker, Frame::text("fresh-after-many-turns")).await;
    send(&worker, original).await;
    resume(&worker).await;
    gate.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert_eq!(texts, ["fresh-after-many-turns"]);
    assert_eq!(report.dropped_frames, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicated_interruption_has_a_new_logical_identity() {
    let gate = Arc::new(SystemGate::new());
    let mut worker = started(vec![gate.clone()]).await;
    pause_and_hold(&mut worker, &gate).await;
    let original = Frame::interruption();
    send(&worker, original.clone()).await;
    send(&worker, Frame::text("first-generation")).await;
    send(&worker, original.duplicate()).await;
    send(&worker, Frame::text("second-generation")).await;
    resume(&worker).await;
    gate.release.add_permits(1);
    let (texts, report) = finish(worker).await;
    assert_eq!(texts, ["second-generation"]);
    assert_eq!(report.dropped_frames, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rebroadcast_of_admitted_event_does_not_repeat_its_callback() {
    let source = Arc::new(InterruptionObserver {
        calls: AtomicUsize::new(0),
        observed: Semaphore::new(0),
    });
    let mut worker = started(vec![source.clone()]).await;
    let original = Frame::interruption();
    send(&worker, original.clone()).await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    let (down, up) = original.broadcast_pair();
    send(&worker, down).await;
    worker.sender().send(up, Direction::Upstream).await.unwrap();
    send(&worker, Frame::text("fresh")).await;
    let (texts, report) = finish(worker).await;
    assert_eq!(
        source.calls.load(Ordering::SeqCst),
        1,
        "one shared interruption token invoked its handler more than once"
    );
    assert_eq!(texts, ["fresh"]);
    assert_eq!(report.dropped_frames, 0);
}

struct BoundedBroadcast {
    old_admitted: Semaphore,
    fresh_attempted: Semaphore,
}
#[async_trait]
impl FrameProcessor for BoundedBroadcast {
    fn name(&self) -> &str {
        "review-bounded-broadcast"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::InputText(_)) {
            ctx.broadcast_interruption().await?;
            let result = ctx.push(Frame::text("fresh"), Direction::Downstream).await;
            self.fresh_attempted.add_permits(1);
            return result;
        }
        let old = matches!(&frame.data, FrameData::Text(text) if text.text == "obsolete");
        ctx.push(frame, direction).await?;
        if old {
            self.old_admitted.add_permits(1);
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_reclaims_full_normal_lane_before_fresh_internal_emission() {
    let source = Arc::new(BoundedBroadcast {
        old_admitted: Semaphore::new(0),
        fresh_attempted: Semaphore::new(0),
    });
    let gate = Arc::new(SystemGate::new());
    let mut worker = Pipeline::new(vec![source.clone(), gate.clone()])
        .start(PipelineConfig {
            queue_capacity: 1,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut worker).await;
    pause_and_hold(&mut worker, &gate).await;
    send(&worker, Frame::text("obsolete")).await;
    tokio::time::timeout(LIMIT, source.old_admitted.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    send(
        &worker,
        Frame::new(FrameData::InputText(TextFrame::new("new turn"))),
    )
    .await;
    tokio::time::timeout(LIMIT, source.fresh_attempted.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    gate.release.add_permits(1);
    assert!(
        !worker.has_finished(),
        "post-interruption internal emission failed: {:?}",
        worker.join().await
    );
    resume(&worker).await;
    loop {
        if let FrameData::Text(text) = next(&mut worker).await.frame.data {
            assert_eq!(text.text, "fresh");
            break;
        }
    }
    let (texts, report) = finish(worker).await;
    assert!(texts.is_empty());
    assert_eq!(report.dropped_frames, 1);
    assert!(report.errors.is_empty());
}
