//! Offline witnesses for conditional wake ordering and fixed-session admission.
use futures_util::FutureExt;
use pipecat_core::*;
use pipecat_processors::*;
use std::{
    future::pending,
    panic::{resume_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Semaphore;

#[derive(Default)]
struct Pipelines {
    workers: Vec<Option<PipelineWorker>>,
}
impl Pipelines {
    fn worker(&mut self, index: usize) -> &mut PipelineWorker {
        self.workers[index].as_mut().unwrap()
    }
    async fn start_nodes(
        &mut self,
        nodes: Vec<Arc<dyn FrameProcessor>>,
        config: PipelineConfig,
    ) -> usize {
        let worker = Pipeline::new(nodes).start(config).await.unwrap();
        let index = self.workers.len();
        self.workers.push(Some(worker));
        assert!(self.recv(index).await.frame.data.is_start());
        index
    }
    async fn start(&mut self, processor: Arc<dyn FrameProcessor>) -> usize {
        self.start_nodes(vec![processor], Default::default()).await
    }
    async fn recv(&mut self, index: usize) -> DirectedFrame {
        tokio::time::timeout(Duration::from_secs(5), self.worker(index).recv())
            .await
            .expect("frame receive deadline")
            .expect("output closed before expected frame")
    }
    async fn until(&mut self, mut condition: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("condition deadline");
    }
    async fn send(&mut self, index: usize, frame: Frame, direction: Direction) {
        self.worker(index)
            .sender()
            .send(frame, direction)
            .await
            .unwrap();
    }
    async fn barrier(&mut self, index: usize) {
        let frame = Frame::new(FrameData::LlmRun);
        let id = frame.id;
        self.send(index, frame, Direction::Downstream).await;
        assert_eq!(self.recv(index).await.frame.id, id);
    }
    async fn cache(&mut self, index: usize, frame: Frame) {
        self.send(index, frame, Direction::Downstream).await;
        self.barrier(index).await;
    }
    async fn interrupt(&mut self, index: usize) {
        self.send(
            index,
            Frame::new(FrameData::Interruption),
            Direction::Downstream,
        )
        .await;
        loop {
            if matches!(self.recv(index).await.frame.data, FrameData::Interruption) {
                break;
            }
        }
    }
    async fn complete(
        &mut self,
        index: usize,
    ) -> (Vec<DirectedFrame>, Result<WorkerReport, PipelineError>) {
        // Keep each owner outside the deadline future; failed observations never
        // implicitly drop some other independent pipeline's only owner.
        tokio::time::timeout(Duration::from_secs(5), async {
            let worker = self.worker(index);
            let mut output = Vec::new();
            while let Some(frame) = worker.recv().await {
                output.push(frame);
            }
            (output, worker.join().await)
        })
        .await
        .expect("drain/join deadline")
    }
    async fn finish_after_error(&mut self, index: usize) -> (Vec<DirectedFrame>, String) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.worker(index).total_errors() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("processor error observation deadline");
        let error = self
            .worker(index)
            .errors()
            .iter()
            .map(|error| error.error.clone())
            .collect::<Vec<_>>()
            .join("; ");
        // ProcessorError::new is recoverable in core. Observe it, then explicitly
        // cancel before drain/join rather than pretending it terminates a run.
        self.worker(index)
            .cancel("observed expected processor error")
            .await
            .unwrap();
        let (frames, result) = self.complete(index).await;
        assert!(result.is_ok(), "expected recoverable error, got {result:?}");
        (frames, error)
    }
    async fn stop(&mut self, index: usize) {
        self.send(index, Frame::new(FrameData::Stop), Direction::Downstream)
            .await;
        assert!(self.complete(index).await.1.is_ok());
    }
    async fn shutdown(&mut self) {
        let mut failed = Vec::new();
        // Cancel every still-live owner before joining any of them.
        for worker in self.workers.iter().flatten() {
            if !worker.has_finished() {
                match tokio::time::timeout(Duration::from_secs(5), worker.cancel("test cleanup"))
                    .await
                {
                    Ok(Ok(())) | Ok(Err(PipelineError::Closed)) => {}
                    other => failed.push(format!("cancel: {other:?}")),
                }
            }
        }
        for worker in self.workers.iter_mut().flatten() {
            let result = tokio::time::timeout(Duration::from_secs(5), async {
                while worker.recv().await.is_some() {}
                worker.join().await
            })
            .await;
            match result {
                Ok(Ok(_))
                | Ok(Err(PipelineError::Execution(_)))
                | Ok(Err(PipelineError::AlreadyJoined)) => {}
                other => failed.push(format!("drain/join: {other:?}")),
            }
        }
        assert!(failed.is_empty(), "cleanup failures: {failed:?}");
    }
}
// Assertions and observation failures also run cancel-all, then drain/join-all.
macro_rules! pipeline_test {
    ($name:ident, |$pipes:ident| $body:block) => {
        #[tokio::test]
        async fn $name() {
            let mut owners = Pipelines::default();
            let result = AssertUnwindSafe(async {
                let $pipes = &mut owners;
                $body
            })
            .catch_unwind()
            .await;
            owners.shutdown().await;
            if let Err(payload) = result {
                resume_unwind(payload);
            }
        }
    };
}
fn texts() -> FramePredicate {
    Arc::new(|frame| matches!(frame.data, FrameData::Text(_)))
}
fn yes() -> AsyncFramePredicate {
    Arc::new(|_| Box::pin(async { Ok(true) }))
}
fn callback(calls: Arc<AtomicUsize>) -> AsyncWakeNotification {
    Arc::new(move || {
        let calls = calls.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
    })
}
fn gate() -> Arc<GatedLlmContextAggregator> {
    Arc::new(GatedLlmContextAggregator::new(Default::default()).unwrap())
}
fn typed(
    notifier: GatedLlmContextNotifier,
    filter: AsyncFramePredicate,
) -> Arc<WakeNotifierFilter> {
    Arc::new(
        WakeNotifierFilter::for_gated_context(Default::default(), texts(), filter, notifier)
            .unwrap(),
    )
}
fn context(text: &str, detailed: bool) -> Frame {
    if detailed {
        Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![ChatMessage::new(ChatRole::User, text).into()],
                tools: ToolSet::NotGiven,
                tool_choice: None,
            },
            speculation: false,
        }))
    } else {
        Frame::new(FrameData::LlmContext(LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, text)],
            ..Default::default()
        }))
    }
}

struct CheckNotifyBeforeForward {
    notifier: GatedLlmContextNotifier,
    checked: Arc<AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for CheckNotifyBeforeForward {
    fn name(&self) -> &str {
        "CheckNotifyBeforeForward"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmResponseStart) {
            let snapshot = self.notifier.snapshot();
            if !snapshot.notification_pending && snapshot.notifications_consumed == 0 {
                return Err(ProcessorError::new(
                    "source forwarded before target notify admission",
                ));
            }
            self.checked.fetch_add(1, Ordering::SeqCst);
        }
        ctx.push(frame, direction).await
    }
}
pipeline_test!(
    independent_pipelines_short_circuit_latest_context_and_original_authority,
    |pipes| {
        for detailed in [false, true] {
            let target = gate();
            let b = pipes.start(target.clone()).await;
            let notifier = target.current_notifier().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let hook_calls = calls.clone();
            let predicate: AsyncFramePredicate = Arc::new(move |frame| {
                hook_calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(matches!(frame.data, FrameData::LlmResponseStart)) })
            });
            let filter = Arc::new(
                WakeNotifierFilter::for_gated_context(
                    Default::default(),
                    Arc::new(|frame| {
                        matches!(frame.data, FrameData::Text(_) | FrameData::LlmResponseStart)
                    }),
                    predicate,
                    notifier.clone(),
                )
                .unwrap(),
            );
            let checked = Arc::new(AtomicUsize::new(0));
            let a = pipes
                .start_nodes(
                    vec![
                        filter,
                        Arc::new(CheckNotifyBeforeForward {
                            notifier: notifier.clone(),
                            checked: checked.clone(),
                        }),
                    ],
                    Default::default(),
                )
                .await;
            let first = context("A", !detailed);
            let first_identity = first.observation_token();
            pipes.cache(b, first).await;
            let mut latest = context("B", detailed);
            let tag = LocalFrameTag::new();
            tag.mark(&mut latest).unwrap();
            latest.pts = Some(42);
            latest.transport_source = Some("original-context-source".into());
            latest
                .metadata
                .insert("opaque".into(), serde_json::json!([7, "B"]));
            let cause = if detailed {
                NativeRealtimeContextProducer::default()
                    .attach(&mut latest, RealtimeContextIntent::Run, true)
                    .unwrap();
                Some(
                    latest
                        .native_realtime_context_cause()
                        .unwrap()
                        .unwrap()
                        .clone(),
                )
            } else {
                None
            };
            let expected = latest.clone();
            pipes.cache(b, latest).await;
            assert!(!first_identity.is_alive());
            pipes.barrier(a).await; // Selector nonmatch: no predicate or notification.
            pipes
                .send(a, Frame::text("false"), Direction::Upstream)
                .await;
            let false_output = pipes.recv(a).await;
            assert_eq!(false_output.direction, Direction::Upstream);
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(notifier.snapshot().notifications_consumed, 0);
            assert!(notifier.snapshot().context_pending);
            let carrier = Arc::new(
                NativeLlmContinuation::start(42, LlmContinuationProvider::Anthropic).unwrap(),
            );
            let mut trigger = Frame::new(FrameData::LlmResponseStart);
            trigger
                .set_native_llm_continuation(carrier.clone())
                .unwrap();
            let source_tag = LocalFrameTag::new();
            source_tag.mark(&mut trigger).unwrap();
            trigger
                .metadata
                .insert("source".into(), serde_json::json!("untouched"));
            let source_identity = trigger.observation_token();
            let expected_source = trigger.clone();
            pipes.send(a, trigger, Direction::Downstream).await;
            let original = pipes.recv(a).await;
            let released = pipes.recv(b).await;
            assert_eq!(checked.load(Ordering::SeqCst), 1);
            // Only notify-before-source-forward is asserted, never an ordering of
            // the two independent worker output streams.
            assert_eq!(original.frame, expected_source);
            assert_eq!(original.frame.observation_token(), source_identity);
            assert!(source_tag.contains(&original.frame));
            assert!(Arc::ptr_eq(
                original.frame.native_llm_continuation().unwrap().unwrap(),
                &carrier
            ));
            assert_eq!(released.frame, expected);
            assert_eq!(
                released.frame.observation_token(),
                expected.observation_token()
            );
            assert!(tag.contains(&released.frame));
            assert_eq!(released.direction, Direction::Downstream);
            if let Some(cause) = cause {
                assert!(Arc::ptr_eq(
                    released
                        .frame
                        .native_realtime_context_cause()
                        .unwrap()
                        .unwrap(),
                    &cause
                ));
            }
            assert_eq!(notifier.snapshot().notifications_consumed, 1);
            assert!(!notifier.snapshot().context_pending);
            assert!(pipes.worker(b).try_recv().is_err());
        }
    }
);

pipeline_test!(
    generic_await_order_both_directions_and_lifecycle_no_bypass,
    |pipes| {
        let events = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let predicate_events = events.clone();
        let predicate: AsyncFramePredicate = Arc::new(move |_| {
            predicate_events.lock().unwrap().push("predicate-construct");
            let events = predicate_events.clone();
            Box::pin(async move {
                tokio::task::yield_now().await;
                events.lock().unwrap().push("predicate-ready");
                Ok(true)
            })
        });
        let notify_events = events.clone();
        let notify: AsyncWakeNotification = Arc::new(move || {
            notify_events.lock().unwrap().push("notify-construct");
            let events = notify_events.clone();
            Box::pin(async move {
                tokio::task::yield_now().await;
                events.lock().unwrap().push("notify-ready");
                Ok(())
            })
        });
        let filter = Arc::new(
            WakeNotifierFilter::new(Default::default(), Arc::new(|_| true), predicate, notify)
                .unwrap(),
        );
        let a = pipes.start(filter).await;
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                "predicate-construct",
                "predicate-ready",
                "notify-construct",
                "notify-ready"
            ]
        );
        for direction in [Direction::Upstream, Direction::Downstream] {
            events.lock().unwrap().clear();
            let frame = Frame::text("same frame");
            let expected = frame.clone();
            pipes.send(a, frame, direction).await;
            let actual = pipes.recv(a).await;
            assert_eq!(actual.frame, expected);
            assert_eq!(actual.direction, direction);
            assert_eq!(
                *events.lock().unwrap(),
                vec![
                    "predicate-construct",
                    "predicate-ready",
                    "notify-construct",
                    "notify-ready"
                ]
            );
        }
        events.lock().unwrap().clear();
        pipes.interrupt(a).await;
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                "predicate-construct",
                "predicate-ready",
                "notify-construct",
                "notify-ready"
            ]
        );
        events.lock().unwrap().clear();
        pipes.send(a, Frame::end(), Direction::Downstream).await;
        let (frames, result) = pipes.complete(a).await;
        assert!(result.is_ok());
        assert!(frames.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::End(_) | FrameData::EndDetailed(_)
        )));
        assert_eq!(
            *events.lock().unwrap(),
            vec![
                "predicate-construct",
                "predicate-ready",
                "notify-construct",
                "notify-ready"
            ]
        );
    }
);

pipeline_test!(
    predicate_and_notify_errors_panics_and_deadlines_never_forward,
    |pipes| {
        for notify_side in [false, true] {
            for failure in 0..4 {
                let calls = Arc::new(AtomicUsize::new(0));
                let predicate: AsyncFramePredicate = if notify_side {
                    yes()
                } else {
                    Arc::new(move |_| {
                        if failure == 1 {
                            panic!("predicate construction sentinel");
                        }
                        Box::pin(async move {
                            match failure {
                                0 => Err(ProcessorError::new("predicate returned error")),
                                2 => panic!("predicate polling sentinel"),
                                _ => pending().await,
                            }
                        })
                    })
                };
                let notify: AsyncWakeNotification = if !notify_side {
                    callback(calls.clone())
                } else {
                    let calls = calls.clone();
                    Arc::new(move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        if failure == 1 {
                            panic!("notify construction sentinel");
                        }
                        Box::pin(async move {
                            match failure {
                                0 => Err(ProcessorError::new("notification returned error")),
                                2 => panic!("notify polling sentinel"),
                                _ => pending().await,
                            }
                        })
                    })
                };
                let filter = Arc::new(
                    WakeNotifierFilter::new(
                        WakeNotifierFilterConfig {
                            callback_timeout: Duration::from_millis(20),
                        },
                        texts(),
                        predicate,
                        notify,
                    )
                    .unwrap(),
                );
                let a = pipes.start(filter).await;
                let trigger = Frame::text("must not forward");
                let id = trigger.id;
                pipes.send(a, trigger, Direction::Downstream).await;
                let (frames, error) = pipes.finish_after_error(a).await;
                assert!(!frames.iter().any(|frame| frame.frame.id == id));
                let expected = match (notify_side, failure) {
                    (false, 0) => "predicate returned error",
                    (true, 0) => "notification returned error",
                    (false, 1 | 2) => "predicate panicked",
                    (true, 1 | 2) => "notification panicked",
                    (false, _) => "predicate timed out",
                    (true, _) => "notification timed out",
                };
                assert!(error.contains(expected), "{error}");
                assert_eq!(calls.load(Ordering::SeqCst), usize::from(notify_side));
            }
        }
    }
);

pipeline_test!(
    selector_panic_is_contained_and_skips_async_callbacks,
    |pipes| {
        let calls = Arc::new(AtomicUsize::new(0));
        let filter = Arc::new(
            WakeNotifierFilter::new(
                Default::default(),
                Arc::new(|frame| {
                    if matches!(frame.data, FrameData::Text(_)) {
                        panic!("selector sentinel");
                    }
                    false
                }),
                Arc::new(|_| panic!("nonmatch must not construct predicate")),
                callback(calls.clone()),
            )
            .unwrap(),
        );
        let a = pipes.start(filter).await;
        pipes.barrier(a).await;
        let frame = Frame::text("selector panic");
        let id = frame.id;
        pipes.send(a, frame, Direction::Downstream).await;
        let (frames, error) = pipes.finish_after_error(a).await;
        assert!(!frames.iter().any(|frame| frame.frame.id == id));
        assert!(error.contains("selector panicked"));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
);

fn waiting_predicate(calls: Arc<AtomicUsize>, release: Arc<Semaphore>) -> AsyncFramePredicate {
    Arc::new(move |_| {
        let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
        let release = release.clone();
        Box::pin(async move {
            if first {
                release.acquire().await.unwrap().forget();
            }
            Ok(true)
        })
    })
}
pipeline_test!(
    predicate_await_cannot_refresh_either_original_scope_and_next_message_recovers,
    |pipes| {
        for interrupt_source in [false, true] {
            let target = gate();
            let b = pipes.start(target.clone()).await;
            let notifier = target.current_notifier().unwrap();
            let calls = Arc::new(AtomicUsize::new(0));
            let release = Arc::new(Semaphore::new(0));
            let a = pipes
                .start(typed(
                    notifier.clone(),
                    waiting_predicate(calls.clone(), release.clone()),
                ))
                .await;
            let initial = context("cached before await", false);
            let mut expected = initial.clone();
            pipes.cache(b, initial).await;
            let mut old = Frame::text("old source invocation");
            old.interruptible = false;
            let old_id = old.id;
            pipes.send(a, old, Direction::Downstream).await;
            pipes.until(|| calls.load(Ordering::SeqCst) == 1).await;
            if interrupt_source {
                pipes.interrupt(a).await;
            } else {
                pipes.interrupt(b).await;
                expected = context("fresh target epoch while predicate waits", true);
                pipes.cache(b, expected.clone()).await;
            }
            release.add_permits(1);
            assert_eq!(
                pipes.recv(a).await.frame.id,
                old_id,
                "uninterruptible original uses normal core survival"
            );
            assert_eq!(notifier.snapshot().notifications_consumed, 0);
            assert!(!notifier.snapshot().notification_pending);
            assert!(notifier.snapshot().context_pending);
            assert!(pipes.worker(b).try_recv().is_err());
            let fresh = Frame::text("fresh source invocation");
            let id = fresh.id;
            pipes.send(a, fresh, Direction::Downstream).await;
            assert_eq!(pipes.recv(a).await.frame.id, id);
            assert_eq!(pipes.recv(b).await.frame, expected);
            assert_eq!(notifier.snapshot().notifications_consumed, 1);
        }
    }
);

pipeline_test!(
    empty_typed_signal_is_consumed_and_never_becomes_future_credit,
    |pipes| {
        let target = gate();
        let b = pipes.start(target.clone()).await;
        let notifier = target.current_notifier().unwrap();
        let a = pipes.start(typed(notifier.clone(), yes())).await;
        pipes
            .send(a, Frame::text("empty signal"), Direction::Downstream)
            .await;
        pipes.recv(a).await;
        pipes
            .until(|| notifier.snapshot().notifications_consumed == 1)
            .await;
        let next = context("arrives after empty consumption", false);
        let expected = next.clone();
        pipes.cache(b, next).await;
        assert!(notifier.snapshot().context_pending);
        assert!(!notifier.snapshot().notification_pending);
        assert!(pipes.worker(b).try_recv().is_err());
        pipes
            .send(a, Frame::text("new signal"), Direction::Downstream)
            .await;
        pipes.recv(a).await;
        assert_eq!(pipes.recv(b).await.frame, expected);
        assert_eq!(notifier.snapshot().notifications_consumed, 2);
    }
);

pipeline_test!(
    closed_target_false_predicate_still_forwards_and_fixed_old_handle_never_retargets,
    |pipes| {
        let target = gate();
        let b = pipes.start(target.clone()).await;
        let old_notifier = target.current_notifier().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let a = pipes
            .start(typed(
                old_notifier.clone(),
                waiting_predicate(calls.clone(), release.clone()),
            ))
            .await;
        pipes.cache(b, context("old session", false)).await;
        pipes
            .send(a, Frame::text("captured old ticket"), Direction::Downstream)
            .await;
        pipes.until(|| calls.load(Ordering::SeqCst) == 1).await;
        pipes.stop(b).await;
        let new_b = pipes.start(target.clone()).await;
        let fresh_notifier = target.current_notifier().unwrap();
        let new_context = context("new session", true);
        pipes.cache(new_b, new_context.clone()).await;
        release.add_permits(1);
        pipes.recv(a).await;
        assert_eq!(fresh_notifier.snapshot().notifications_consumed, 0);
        assert!(fresh_notifier.snapshot().context_pending);
        assert!(old_notifier.notify().is_err());
        // Capturing a closed fixed target produces an error, but false predicates
        // do not notify and must not surface that deferred error.
        let false_filter: AsyncFramePredicate = Arc::new(|_| Box::pin(async { Ok(false) }));
        let false_source = pipes.start(typed(old_notifier.clone(), false_filter)).await;
        let pass = Frame::text("false on a closed target");
        let expected = pass.clone();
        pipes.send(false_source, pass, Direction::Upstream).await;
        assert_eq!(pipes.recv(false_source).await.frame, expected);
        // A new true invocation bound to the old handle fails, never targets new_b.
        let bad = Frame::text("must not retarget");
        let bad_id = bad.id;
        pipes.send(a, bad, Direction::Downstream).await;
        let (output, error) = pipes.finish_after_error(a).await;
        assert!(!output.iter().any(|frame| frame.frame.id == bad_id));
        assert!(error.contains("session is closed"));
        assert_eq!(fresh_notifier.snapshot().notifications_consumed, 0);
        let fresh_source = pipes.start(typed(fresh_notifier, yes())).await;
        pipes
            .send(
                fresh_source,
                Frame::text("fresh handle"),
                Direction::Downstream,
            )
            .await;
        pipes.recv(fresh_source).await;
        assert_eq!(pipes.recv(new_b).await.frame, new_context);
    }
);

struct ActiveGuard(Arc<AtomicUsize>);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct CleanupMarker {
    inner: Arc<dyn FrameProcessor>,
    cleaned: Arc<AtomicBool>,
}
#[async_trait]
impl FrameProcessor for CleanupMarker {
    fn name(&self) -> &str {
        "WakeOwnerCleanupMarker"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await?;
        self.cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}
pipeline_test!(
    owner_drop_cancels_active_predicate_or_notification_without_detached_work,
    |pipes| {
        for notify_side in [false, true] {
            let active = Arc::new(AtomicUsize::new(0));
            let started = Arc::new(AtomicUsize::new(0));
            let cleaned = Arc::new(AtomicBool::new(false));
            let target = gate();
            let b = pipes.start(target.clone()).await;
            let notifier = target.current_notifier().unwrap();
            pipes
                .cache(b, context("target survives source owner drop", false))
                .await;
            let filter: Arc<dyn FrameProcessor> = if notify_side {
                let active = active.clone();
                let started = started.clone();
                let notify: AsyncWakeNotification = Arc::new(move || {
                    let active = active.clone();
                    let started = started.clone();
                    Box::pin(async move {
                        active.fetch_add(1, Ordering::SeqCst);
                        let _guard = ActiveGuard(active);
                        started.fetch_add(1, Ordering::SeqCst); // An already-started effect cannot be undone.
                        pending::<Result<(), ProcessorError>>().await
                    })
                });
                Arc::new(
                    WakeNotifierFilter::new(Default::default(), texts(), yes(), notify).unwrap(),
                )
            } else {
                let active = active.clone();
                let started = started.clone();
                let predicate: AsyncFramePredicate = Arc::new(move |_| {
                    let active = active.clone();
                    let started = started.clone();
                    Box::pin(async move {
                        active.fetch_add(1, Ordering::SeqCst);
                        let _guard = ActiveGuard(active);
                        started.fetch_add(1, Ordering::SeqCst);
                        pending::<Result<bool, ProcessorError>>().await
                    })
                });
                typed(notifier.clone(), predicate)
            };
            let a = pipes
                .start(Arc::new(CleanupMarker {
                    inner: filter,
                    cleaned: cleaned.clone(),
                }))
                .await;
            pipes
                .send(
                    a,
                    Frame::text("active at owner drop"),
                    Direction::Downstream,
                )
                .await;
            pipes.until(|| active.load(Ordering::SeqCst) == 1).await;
            drop(pipes.workers[a].take()); // Explicit owner-drop scenario, never a timeout side effect.
            pipes
                .until(|| active.load(Ordering::SeqCst) == 0 && cleaned.load(Ordering::SeqCst))
                .await;
            assert_eq!(started.load(Ordering::SeqCst), 1);
            assert_eq!(notifier.snapshot().notifications_consumed, 0);
            assert!(notifier.snapshot().context_pending);
            assert!(!notifier.snapshot().closed);
            notifier.notify().unwrap();
            pipes.recv(b).await;
        }
    }
);

pipeline_test!(
    generic_callback_started_before_interruption_is_not_rolled_back_or_retried,
    |pipes| {
        let entered = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let notify: AsyncWakeNotification = {
            let entered = entered.clone();
            let completed = completed.clone();
            let release = release.clone();
            Arc::new(move || {
                let entered = entered.clone();
                let completed = completed.clone();
                let release = release.clone();
                Box::pin(async move {
                    entered.fetch_add(1, Ordering::SeqCst);
                    release.acquire().await.unwrap().forget();
                    completed.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
            })
        };
        let a = pipes
            .start(Arc::new(
                WakeNotifierFilter::new(Default::default(), texts(), yes(), notify).unwrap(),
            ))
            .await;
        let mut old = Frame::text("already-started notification");
        old.interruptible = false;
        let expected = old.clone();
        pipes.send(a, old, Direction::Downstream).await;
        pipes.until(|| entered.load(Ordering::SeqCst) == 1).await;
        pipes.interrupt(a).await;
        release.add_permits(1);
        assert_eq!(pipes.recv(a).await.frame, expected);
        assert_eq!(entered.load(Ordering::SeqCst), 1);
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }
);

pipeline_test!(
    notification_success_then_original_push_failure_is_visible_without_retry,
    |pipes| {
        let calls = Arc::new(AtomicUsize::new(0));
        let filter = Arc::new(
            WakeNotifierFilter::new(Default::default(), texts(), yes(), callback(calls.clone()))
                .unwrap(),
        );
        let a = pipes
            .start_nodes(
                vec![filter],
                PipelineConfig {
                    output_capacity: 1,
                    ..Default::default()
                },
            )
            .await;
        let first = Frame::text("occupy output queue");
        let first_id = first.id;
        pipes.send(a, first, Direction::Downstream).await;
        pipes.until(|| calls.load(Ordering::SeqCst) == 1).await;
        let failing = Frame::text("notify succeeds; original push fails");
        let failing_id = failing.id;
        pipes.send(a, failing, Direction::Downstream).await;
        let sender = pipes.worker(a).sender();
        pipes.until(|| sender.state().is_finished()).await;
        let (frames, result) = pipes.complete(a).await;
        assert!(frames.iter().any(|frame| frame.frame.id == first_id));
        assert!(!frames.iter().any(|frame| frame.frame.id == failing_id));
        assert!(result.unwrap_err().to_string().contains("full"));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "one notification per input, no push retry"
        );
    }
);

struct SkipTargetStart {
    inner: Arc<GatedLlmContextAggregator>,
}
#[async_trait]
impl FrameProcessor for SkipTargetStart {
    fn name(&self) -> &str {
        "SkipTargetStart"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // Deliberately model a wrapper that set up the gate but has not given
        // it a real lifecycle invocation. Setup must not invent a target epoch.
        if frame.data.is_start() {
            return ctx.push(frame, direction).await;
        }
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
pipeline_test!(
    setup_context_is_not_a_target_epoch_and_missing_scope_error_is_deferred,
    |pipes| {
        let target = gate();
        let b = pipes
            .start(Arc::new(SkipTargetStart {
                inner: target.clone(),
            }))
            .await;
        let notifier = target.current_notifier().unwrap();
        let false_predicate: AsyncFramePredicate = Arc::new(|_| Box::pin(async { Ok(false) }));
        let a = pipes.start(typed(notifier.clone(), false_predicate)).await;
        let allowed = Frame::text("false without target Start");
        let expected = allowed.clone();
        pipes.send(a, allowed, Direction::Downstream).await;
        assert_eq!(pipes.recv(a).await.frame, expected);
        let failing = pipes.start(typed(notifier.clone(), yes())).await;
        let rejected = Frame::text("true without target Start");
        let id = rejected.id;
        pipes.send(failing, rejected, Direction::Downstream).await;
        let (output, error) = pipes.finish_after_error(failing).await;
        assert!(!output.iter().any(|frame| frame.frame.id == id));
        assert!(error.contains("has not processed Start"));
        assert_eq!(notifier.snapshot().notifications_consumed, 0);
        // A real Interruption also establishes the gate's current raw scope.
        pipes.interrupt(b).await;
        pipes
            .cache(b, context("after real target lifecycle invocation", false))
            .await;
        let fresh = pipes.start(typed(notifier.clone(), yes())).await;
        pipes
            .send(fresh, Frame::text("fresh ticket"), Direction::Downstream)
            .await;
        pipes.recv(fresh).await;
        pipes.recv(b).await;
        assert_eq!(notifier.snapshot().notifications_consumed, 1);
    }
);

struct InterruptAfterAdmission {
    inner: Arc<WakeNotifierFilter>,
    notifier: GatedLlmContextNotifier,
    observed: Arc<AtomicBool>,
}
#[async_trait]
impl FrameProcessor for InterruptAfterAdmission {
    fn name(&self) -> &str {
        "InterruptAfterWakeAdmission"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let selected = matches!(frame.data, FrameData::Text(_));
        self.inner.process(frame, direction, ctx).await?;
        if selected {
            // Current-thread execution stays in the same poll: the synchronous
            // wake has queued the supervisor, but it cannot consume yet.
            if !self.notifier.snapshot().notification_pending {
                return Err(ProcessorError::new(
                    "signal not pending before source interruption",
                ));
            }
            ctx.broadcast_interruption().await?;
            if !self.notifier.snapshot().notification_pending {
                return Err(ProcessorError::new(
                    "source interruption retracted admitted signal",
                ));
            }
            self.observed.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}
pipeline_test!(
    admitted_coalescing_signal_does_not_inherit_source_revocation,
    |pipes| {
        let target = gate();
        let b = pipes.start(target.clone()).await;
        let notifier = target.current_notifier().unwrap();
        let expected = context("admitted before source cutoff", true);
        pipes.cache(b, expected.clone()).await;
        let observed = Arc::new(AtomicBool::new(false));
        let source = Arc::new(InterruptAfterAdmission {
            inner: typed(notifier.clone(), yes()),
            notifier: notifier.clone(),
            observed: observed.clone(),
        });
        let a = pipes.start(source).await;
        pipes
            .send(a, Frame::text("wake then interrupt"), Direction::Downstream)
            .await;
        assert_eq!(pipes.recv(b).await.frame, expected);
        assert!(observed.load(Ordering::SeqCst));
        assert_eq!(notifier.snapshot().notifications_consumed, 1);
    }
);

pipeline_test!(
    matched_stop_and_cancel_run_predicate_without_lifecycle_bypass,
    |pipes| {
        for terminal in [FrameData::Stop, FrameData::Cancel(Default::default())] {
            let predicate_calls = Arc::new(AtomicUsize::new(0));
            let notify_calls = Arc::new(AtomicUsize::new(0));
            let predicate: AsyncFramePredicate = {
                let calls = predicate_calls.clone();
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(true) })
                })
            };
            let filter = Arc::new(
                WakeNotifierFilter::new(
                    Default::default(),
                    Arc::new(|frame| matches!(frame.data, FrameData::Stop | FrameData::Cancel(_))),
                    predicate,
                    callback(notify_calls.clone()),
                )
                .unwrap(),
            );
            let a = pipes.start(filter).await;
            let is_cancel = matches!(terminal, FrameData::Cancel(_));
            pipes
                .send(a, Frame::new(terminal), Direction::Downstream)
                .await;
            let (frames, result) = pipes.complete(a).await;
            assert!(result.is_ok());
            assert_eq!(predicate_calls.load(Ordering::SeqCst), 1);
            assert!(frames.iter().any(|frame| if is_cancel {
                frame.frame.data.is_cancel()
            } else {
                matches!(frame.frame.data, FrameData::Stop)
            }));
            // With no intervening interruption, both lifecycle invocations
            // have a current raw source fence and notify exactly once.
            assert_eq!(notify_calls.load(Ordering::SeqCst), 1);
        }
    }
);

#[test]
fn callback_deadline_configuration_is_bounded() {
    for timeout in [
        Duration::ZERO,
        Duration::from_secs(60) + Duration::from_nanos(1),
    ] {
        assert!(WakeNotifierFilter::new(
            WakeNotifierFilterConfig {
                callback_timeout: timeout
            },
            texts(),
            yes(),
            callback(Arc::new(AtomicUsize::new(0)))
        )
        .is_err());
    }
    for timeout in [Duration::from_nanos(1), Duration::from_secs(60)] {
        assert!(WakeNotifierFilter::new(
            WakeNotifierFilterConfig {
                callback_timeout: timeout
            },
            texts(),
            yes(),
            callback(Arc::new(AtomicUsize::new(0)))
        )
        .is_ok());
    }
}

pipeline_test!(
    generic_notification_checks_original_source_after_predicate_await,
    |pipes| {
        let predicate_calls = Arc::new(AtomicUsize::new(0));
        let notify_calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let filter = Arc::new(
            WakeNotifierFilter::new(
                Default::default(),
                texts(),
                waiting_predicate(predicate_calls.clone(), release.clone()),
                callback(notify_calls.clone()),
            )
            .unwrap(),
        );
        let a = pipes.start(filter).await;
        let mut old = Frame::text("old generic callback source");
        old.interruptible = false;
        let expected = old.clone();
        pipes.send(a, old, Direction::Downstream).await;
        pipes
            .until(|| predicate_calls.load(Ordering::SeqCst) == 1)
            .await;
        pipes.interrupt(a).await;
        release.add_permits(1);
        assert_eq!(pipes.recv(a).await.frame, expected);
        assert_eq!(notify_calls.load(Ordering::SeqCst), 0);
        pipes
            .send(
                a,
                Frame::text("current generic callback source"),
                Direction::Downstream,
            )
            .await;
        pipes.recv(a).await;
        assert_eq!(notify_calls.load(Ordering::SeqCst), 1);
    }
);
