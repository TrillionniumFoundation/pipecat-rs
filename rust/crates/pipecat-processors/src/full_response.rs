//! Bounded generated-response collection, independent of conversation history.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    StartFrame,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, watch, Notify};

/// An immutable snapshot of generated text at a response boundary.
///
/// `completed` means an LLM/Vision response-end marker was observed. It does not
/// attest playback, tool execution, provider success, or a committed history turn.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FullResponseCompletion {
    pub text: String,
    pub completed: bool,
    /// Monotonic within this aggregator; a new pipeline setup starts a new session.
    pub session_id: u64,
    /// Monotonic boundary number, including boundaries rejected by resource limits.
    pub sequence: u64,
    pub trigger_frame_id: u64,
    pub direction: Direction,
}

/// One fixed callback registration. No pipeline-emission authority is supplied.
///
/// Futures must yield cooperatively and must not detach work. A callback may run
/// concurrently with later callbacks and frames. The caller owns any copies or
/// external effects it retains; cancellation cannot undo those effects.
/// Application destructors must not panic; destructor and panic-payload Drop
/// panics can reach the core task-panic path instead of sanitized callback errors.
pub type FullResponseCallback = Arc<
    dyn Fn(
            FullResponseCompletion,
        ) -> Pin<Box<dyn Future<Output = Result<(), ProcessorError>> + Send>>
        + Send
        + Sync,
>;

#[derive(Clone, Debug)]
pub struct FullResponseAggregatorConfig {
    /// UTF-8 bytes in the current response. Valid range: 1..=16 MiB.
    pub max_response_bytes: usize,
    /// Admitted callback futures, including those not yet polled. Range: 1..=1024.
    pub max_in_flight_callbacks: usize,
    /// Total retained text allocation capacity of admitted callbacks. Range: 1..=64 MiB.
    pub max_in_flight_bytes: usize,
    /// Deadline from callback admission. Range: 1 ms..=60 seconds.
    pub callback_timeout: Duration,
}
impl Default for FullResponseAggregatorConfig {
    fn default() -> Self {
        Self {
            max_response_bytes: 1024 * 1024,
            max_in_flight_callbacks: 16,
            max_in_flight_bytes: 8 * 1024 * 1024,
            callback_timeout: Duration::from_secs(10),
        }
    }
}
impl FullResponseAggregatorConfig {
    fn validate(&self) -> Result<(), ProcessorError> {
        if !(1..=16 * 1024 * 1024).contains(&self.max_response_bytes)
            || !(1..=1024).contains(&self.max_in_flight_callbacks)
            || !(1..=64 * 1024 * 1024).contains(&self.max_in_flight_bytes)
            || self.callback_timeout < Duration::from_millis(1)
            || self.callback_timeout > Duration::from_secs(60)
        {
            return Err(ProcessorError::new(
                "invalid full-response aggregation configuration",
            ));
        }
        Ok(())
    }
}

/// Payload-free accounting, retained across sequential pipeline sessions.
/// Counters saturate; no completion text, callback errors, or panic payloads are retained.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FullResponseStats {
    pub buffered_bytes: usize,
    pub buffered_capacity: usize,
    pub in_flight_callbacks: usize,
    pub in_flight_bytes: usize,
    /// Charged deliveries not yet transferred to the core task owner.
    pub pending_registrations: usize,
    pub boundaries: u64,
    pub admitted: u64,
    pub succeeded: u64,
    pub callback_errors: u64,
    pub callback_panics: u64,
    pub timed_out: u64,
    pub cancelled: u64,
    pub rejected: u64,
    pub response_overflows: u64,
    pub discarded_responses: u64,
}

struct State {
    active: bool,
    accepting: bool,
    started: bool,
    poisoned_response: bool,
    text: String,
    session_id: u64,
    cancel: watch::Sender<bool>,
    stats: FullResponseStats,
}
impl State {
    fn clear_response(&mut self) {
        self.started = false;
        self.poisoned_response = false;
        self.text = String::new();
        self.stats.buffered_bytes = 0;
        self.stats.buffered_capacity = 0;
    }
}
struct Shared {
    state: Mutex<State>,
    idle: Notify,
}
impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // User code never runs under this lock. Recovering also lets leases
        // release their bounded reservations during unwinding and teardown.
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
}
enum Outcome {
    Success,
    Error,
    Panic,
    Timeout,
    Cancelled,
    Rejected,
}
struct Lease {
    shared: Arc<Shared>,
    bytes: usize,
    settled: bool,
}
impl Lease {
    fn settle(&mut self, outcome: Outcome) {
        if self.settled {
            return;
        }
        self.settled = true;
        {
            let mut state = self.shared.lock();
            let stats = &mut state.stats;
            stats.in_flight_callbacks -= 1;
            stats.in_flight_bytes -= self.bytes;
            let counter = match outcome {
                Outcome::Success => &mut stats.succeeded,
                Outcome::Error => &mut stats.callback_errors,
                Outcome::Panic => &mut stats.callback_panics,
                Outcome::Timeout => &mut stats.timed_out,
                Outcome::Cancelled => &mut stats.cancelled,
                Outcome::Rejected => &mut stats.rejected,
            };
            *counter = counter.saturating_add(1);
        }
        self.shared.idle.notify_waiters();
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.settle(Outcome::Cancelled);
    }
}
struct Delivery {
    completion: FullResponseCompletion,
    lease: Lease,
    cancel: watch::Receiver<bool>,
    deadline: tokio::time::Instant,
    registration: Option<Registration>,
}
struct Registration(Arc<Shared>);
impl Drop for Registration {
    fn drop(&mut self) {
        self.0.lock().stats.pending_registrations -= 1;
        self.0.idle.notify_waiters();
    }
}

/// Collects exact LLM/Vision text between response starts and boundaries.
///
/// Repeated starts do not clear text. Every end and interruption, including an
/// inactive/empty one, admits a completion unless a documented bound or shutdown
/// prevents delivery. Original frames are forwarded unchanged in their direction.
/// Callback admission precedes forwarding the boundary; execution is asynchronous.
///
/// There is no pending event queue. A full callback budget rejects that boundary
/// explicitly. An oversized response is discarded until its next end/interruption;
/// it never becomes a truncated successful completion. Errors are payload-free.
/// End/Stop discard unfinished text and drain admitted work. Cancel, cleanup and
/// owner drop cancel work; no lifecycle terminal manufactures a completion.
/// Join the previous worker before sequential reuse. Sharing an instance across
/// concurrent pipelines is unsupported; setup diagnostics are not an isolation
/// boundary because the core cleanup hook has no invocation identity.
pub struct LlmFullResponseAggregator {
    config: FullResponseAggregatorConfig,
    callback: FullResponseCallback,
    shared: Arc<Shared>,
}
impl LlmFullResponseAggregator {
    pub fn new(
        config: FullResponseAggregatorConfig,
        callback: FullResponseCallback,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        let (cancel, _) = watch::channel(false);
        Ok(Self {
            config,
            callback,
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    active: false,
                    accepting: false,
                    started: false,
                    poisoned_response: false,
                    text: String::new(),
                    session_id: 0,
                    cancel,
                    stats: FullResponseStats::default(),
                }),
                idle: Notify::new(),
            }),
        })
    }
    pub fn stats(&self) -> FullResponseStats {
        self.shared.lock().stats.clone()
    }
    fn reduce(
        &self,
        frame: &Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<Option<Delivery>, ProcessorError> {
        let mut state = self.shared.lock();
        if !state.accepting {
            return Ok(None);
        }
        // A normal invocation can race the urgent interruption lane. Never let
        // an old token seed the response which follows that interruption.
        if matches!(
            frame.data,
            FrameData::LlmResponseStart
                | FrameData::LlmFullResponseStart(_)
                | FrameData::VisionFullResponseStart(_)
                | FrameData::LlmText(_)
                | FrameData::VisionText(_)
        ) && !ctx.is_current()
        {
            return Ok(None);
        }
        let completed = match &frame.data {
            FrameData::LlmResponseStart
            | FrameData::LlmFullResponseStart(_)
            | FrameData::VisionFullResponseStart(_) => {
                state.started = true;
                return Ok(None);
            }
            FrameData::LlmText(text) | FrameData::VisionText(text) => {
                if state.started && !state.poisoned_response {
                    let fits = text.text.len()
                        <= self
                            .config
                            .max_response_bytes
                            .saturating_sub(state.text.len());
                    let target = state
                        .text
                        .capacity()
                        .saturating_mul(2)
                        .max(state.text.len().saturating_add(text.text.len()))
                        .min(self.config.max_response_bytes);
                    let needs_allocation =
                        text.text.len() > state.text.capacity() - state.text.len();
                    let reserve = target.saturating_sub(state.text.len());
                    if !fits
                        || (needs_allocation && state.text.try_reserve_exact(reserve).is_err())
                        || state.text.capacity() > self.config.max_response_bytes
                    {
                        state.text = String::new();
                        state.stats.buffered_bytes = 0;
                        state.stats.buffered_capacity = 0;
                        state.poisoned_response = true;
                        state.stats.response_overflows =
                            state.stats.response_overflows.saturating_add(1);
                        return Err(ProcessorError::new("full-response text capacity exceeded"));
                    }
                    state.text.push_str(&text.text);
                    state.stats.buffered_bytes = state.text.len();
                    state.stats.buffered_capacity = state.text.capacity();
                }
                return Ok(None);
            }
            FrameData::LlmResponseEnd
            | FrameData::LlmFullResponseEnd(_)
            | FrameData::VisionFullResponseEnd(_) => true,
            FrameData::Interruption => false,
            FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_) => {
                state.accepting = false;
                state.clear_response();
                return Ok(None);
            }
            _ => return Ok(None),
        };
        state.stats.boundaries = state.stats.boundaries.saturating_add(1);
        if state.poisoned_response {
            state.stats.discarded_responses = state.stats.discarded_responses.saturating_add(1);
            state.clear_response();
            return Err(ProcessorError::new(
                "full-response completion discarded after text overflow",
            ));
        }
        let text = std::mem::take(&mut state.text);
        state.clear_response();
        let bytes = text.capacity();
        if state.stats.in_flight_callbacks >= self.config.max_in_flight_callbacks
            || bytes
                > self
                    .config
                    .max_in_flight_bytes
                    .saturating_sub(state.stats.in_flight_bytes)
        {
            state.stats.rejected = state.stats.rejected.saturating_add(1);
            return Err(ProcessorError::new(
                "full-response callback capacity exceeded",
            ));
        }
        state.stats.in_flight_callbacks += 1;
        state.stats.in_flight_bytes += bytes;
        state.stats.pending_registrations += 1;
        state.stats.admitted = state.stats.admitted.saturating_add(1);
        Ok(Some(Delivery {
            completion: FullResponseCompletion {
                text,
                completed,
                session_id: state.session_id,
                sequence: state.stats.boundaries,
                trigger_frame_id: frame.id,
                direction,
            },
            lease: Lease {
                shared: self.shared.clone(),
                bytes,
                settled: false,
            },
            cancel: state.cancel.subscribe(),
            deadline: tokio::time::Instant::now() + self.config.callback_timeout,
            registration: Some(Registration(self.shared.clone())),
        }))
    }
    fn dispatch(
        &self,
        mut delivery: Delivery,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let callback = self.callback.clone();
        // Graceful close waits for this guard before the core drains tasks.
        // Never hold the state lock while acquiring runtime admission locks.
        let _registration = delivery.registration.take();
        // Classify spawn rejection before handing over the owned reservation.
        // The one-shot is not a pending-event queue: every slot is already charged.
        let (send, receive) = oneshot::channel::<Delivery>();
        let future = async move {
            let Ok(mut delivery) = receive.await else {
                return Ok(());
            };
            if *delivery.cancel.borrow() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= delivery.deadline {
                delivery.lease.settle(Outcome::Timeout);
                return Err(ProcessorError::new("full-response callback timed out"));
            }
            let guarded =
                std::panic::AssertUnwindSafe(async { (callback)(delivery.completion).await })
                    .catch_unwind();
            let result = tokio::select! {
                biased;
                _ = delivery.cancel.changed() => return Ok(()),
                result = tokio::time::timeout_at(delivery.deadline, guarded) => result,
            };
            let (outcome, error) = match result {
                Ok(Ok(Ok(()))) => (Outcome::Success, None),
                Ok(Ok(Err(_))) => (Outcome::Error, Some("full-response callback failed")),
                Ok(Err(_)) => (Outcome::Panic, Some("full-response callback panicked")),
                Err(_) => (Outcome::Timeout, Some("full-response callback timed out")),
            };
            delivery.lease.settle(outcome);
            error.map_or(Ok(()), |error| Err(ProcessorError::new(error)))
        };
        // Survive ordinary interruption so interrupted partial completions can
        // finish. The runtime still owns cancellation, joining and error delivery.
        match ctx.spawn("full-response-callback", false, future) {
            Ok(_) => {
                // If cancellation won before the first poll, dropping the
                // returned delivery settles its reservation as cancelled.
                let _ = send.send(delivery);
                Ok(())
            }
            Err(error) => {
                delivery.lease.settle(Outcome::Rejected);
                Err(error)
            }
        }
    }
    fn close(&self, cancel_callbacks: bool) {
        let mut state = self.shared.lock();
        state.accepting = false;
        state.clear_response();
        if cancel_callbacks {
            state.cancel.send_replace(true);
        }
    }
}
#[async_trait]
impl FrameProcessor for LlmFullResponseAggregator {
    fn name(&self) -> &str {
        "LlmFullResponseAggregator"
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut state = self.shared.lock();
        if state.active
            || state.stats.in_flight_callbacks != 0
            || state.stats.pending_registrations != 0
        {
            return Err(ProcessorError::new(
                "full-response aggregator is already in use",
            ));
        }
        state.session_id = state
            .session_id
            .checked_add(1)
            .ok_or_else(|| ProcessorError::new("full-response session identity exhausted"))?;
        state.active = true;
        state.accepting = true;
        state.clear_response();
        state.cancel.send_replace(false);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let result = self
            .reduce(&frame, direction, ctx)
            .and_then(|delivery| delivery.map_or(Ok(()), |delivery| self.dispatch(delivery, ctx)));
        if matches!(frame.data, FrameData::Stop) {
            // Stop does not invoke cleanup in the core runtime. Its owned work
            // has already drained before this terminal invocation.
            self.shared.lock().active = false;
        }
        // Resource and callback-admission failures do not swallow input frames.
        ctx.push(frame, direction).await?;
        result
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.close(false);
        loop {
            let idle = self.shared.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.shared.lock().stats.pending_registrations == 0 {
                break;
            }
            idle.await;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.close(true);
        loop {
            let idle = self.shared.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            let done = {
                let state = self.shared.lock();
                state.stats.in_flight_callbacks == 0 && state.stats.pending_registrations == 0
            };
            if done {
                break;
            }
            idle.await;
        }
        self.shared.lock().active = false;
        Ok(())
    }
}

#[cfg(test)]
mod registration_tests {
    use super::*;
    use pipecat_core::{Pipeline, PipelineConfig, WorkerState};
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Widen the real reservation-to-registration window without changing the
    /// production reducer, registration handoff, or graceful-close implementation.
    struct RegistrationWindow {
        inner: Arc<LlmFullResponseAggregator>,
        reserved: Arc<Notify>,
        register: Arc<Notify>,
        closing: Arc<Notify>,
        begin_close: Arc<Notify>,
        closed: Arc<AtomicBool>,
    }
    #[async_trait]
    impl FrameProcessor for RegistrationWindow {
        fn name(&self) -> &str {
            "RegistrationWindow"
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.inner.setup(config, ctx).await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Interruption) {
                let delivery = self.inner.reduce(&frame, direction, ctx)?.unwrap();
                self.reserved.notify_one();
                self.register.notified().await;
                self.inner.dispatch(delivery, ctx)?;
                ctx.push(frame, direction).await
            } else {
                self.inner.process(frame, direction, ctx).await
            }
        }
        async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
            self.closing.notify_one();
            self.begin_close.notified().await;
            self.inner.prepare_end(ctx).await?;
            self.closed.store(true, Ordering::SeqCst);
            Ok(())
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.inner.cleanup().await
        }
    }
    #[tokio::test]
    async fn graceful_close_waits_for_reserved_system_callback_to_register_before_drain() {
        for stop in [false, true] {
            let callback_entered = Arc::new(Notify::new());
            let callback_release = Arc::new(Notify::new());
            let callback: FullResponseCallback = {
                let entered = callback_entered.clone();
                let release = callback_release.clone();
                Arc::new(move |_| {
                    let entered = entered.clone();
                    let release = release.clone();
                    Box::pin(async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(())
                    })
                })
            };
            let inner =
                Arc::new(LlmFullResponseAggregator::new(Default::default(), callback).unwrap());
            let window = Arc::new(RegistrationWindow {
                inner: inner.clone(),
                reserved: Arc::new(Notify::new()),
                register: Arc::new(Notify::new()),
                closing: Arc::new(Notify::new()),
                begin_close: Arc::new(Notify::new()),
                closed: Arc::new(AtomicBool::new(false)),
            });
            let mut worker = Pipeline::new(vec![window.clone()])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            worker.recv().await.unwrap();
            let terminal = if stop {
                FrameData::Stop
            } else {
                FrameData::End(Default::default())
            };
            worker
                .sender()
                .send(Frame::new(terminal), Direction::Downstream)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), window.closing.notified())
                .await
                .unwrap();
            worker
                .sender()
                .send(Frame::interruption(), Direction::Downstream)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), window.reserved.notified())
                .await
                .unwrap();
            window.begin_close.notify_one();
            tokio::task::yield_now().await;
            assert!(!window.closed.load(Ordering::SeqCst));
            assert_eq!(inner.stats().pending_registrations, 1);
            window.register.notify_one();
            tokio::time::timeout(Duration::from_secs(2), callback_entered.notified())
                .await
                .unwrap();
            assert_eq!(inner.stats().pending_registrations, 0);
            while let Ok(frame) = worker.try_recv() {
                assert!(!frame.frame.is_terminal());
            }
            callback_release.notify_one();
            let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                report.state,
                if stop {
                    WorkerState::Stopped
                } else {
                    WorkerState::Ended
                }
            );
            assert!(report.errors.is_empty());
            assert!(window.closed.load(Ordering::SeqCst));
            assert_eq!(inner.stats().succeeded, 1);
        }
    }
}

#[cfg(test)]
mod registration_drop_tests {
    use super::*;
    use pipecat_core::{Pipeline, PipelineConfig};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct HoldRegistration {
        inner: Arc<LlmFullResponseAggregator>,
        held: Arc<Notify>,
    }
    #[async_trait]
    impl FrameProcessor for HoldRegistration {
        fn name(&self) -> &str {
            "HoldRegistration"
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.inner.setup(config, ctx).await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Interruption) {
                let _delivery = self.inner.reduce(&frame, direction, ctx)?.unwrap();
                self.held.notify_one();
                std::future::pending::<()>().await;
                unreachable!()
            }
            self.inner.process(frame, direction, ctx).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.inner.cleanup().await
        }
    }
    #[tokio::test]
    async fn owner_drop_releases_reservation_before_core_registration() {
        let calls = Arc::new(AtomicUsize::new(0));
        let callback: FullResponseCallback = {
            let calls = calls.clone();
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(()) })
            })
        };
        let inner = Arc::new(LlmFullResponseAggregator::new(Default::default(), callback).unwrap());
        let held = Arc::new(Notify::new());
        let mut worker = Pipeline::new(vec![Arc::new(HoldRegistration {
            inner: inner.clone(),
            held: held.clone(),
        })])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        worker.recv().await.unwrap();
        worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), held.notified())
            .await
            .unwrap();
        assert_eq!(inner.stats().pending_registrations, 1);
        drop(worker);
        tokio::time::timeout(Duration::from_secs(2), async {
            while inner.stats().pending_registrations != 0 || inner.stats().in_flight_callbacks != 0
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(inner.stats().cancelled, 1);
    }
}
