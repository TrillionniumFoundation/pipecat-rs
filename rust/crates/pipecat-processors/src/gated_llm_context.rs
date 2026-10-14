//! Single-slot, session-scoped release of the latest original LLM context.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, InvocationFence, ProcessorContext,
    ProcessorError, StartFrame, ToolSet,
};
use std::{
    io::Write,
    sync::{Arc, Mutex},
};
use tokio::sync::{watch, Notify};

/// Native payload bounds for the latest-context gate.
#[derive(Clone, Debug)]
pub struct GatedLlmContextAggregatorConfig {
    /// Pass the first context in each setup through in its original direction.
    pub start_open: bool,
    /// Positive counted payload budget, at most 64 MiB. This counts serialized
    /// data, metadata and envelope strings, plus shallow retained state and
    /// known continuation bytes. Opaque callback captures and runtime ownership
    /// are not a whole-heap memory guarantee.
    pub max_context_bytes: usize,
}
impl Default for GatedLlmContextAggregatorConfig {
    fn default() -> Self {
        Self {
            start_open: false,
            max_context_bytes: 4 * 1024 * 1024,
        }
    }
}

/// Content-free diagnostics for one notification session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatedLlmContextSnapshot {
    pub closed: bool,
    pub context_pending: bool,
    pub notification_pending: bool,
    /// Saturating count of consumed events, including empty events.
    pub notifications_consumed: u64,
    pub supervisor_running: bool,
}

struct Retained {
    frame: Frame,
    ctx: ProcessorContext,
    fence: InvocationFence,
    sequence: u64,
}
struct State {
    closed: bool,
    owned: bool,
    pending: bool,
    start_open: bool,
    consumed: u64,
    sequence: u64,
    slot: Option<Retained>,
    // Latest real Start/Interruption invocation, never a setup context. Weak
    // origins do not extend a runtime owner's lifetime.
    notification_scope: Option<InvocationFence>,
}
struct Session {
    state: Mutex<State>,
    changed: Notify,
    drained: Notify,
    cancel: watch::Sender<bool>,
}
impl Session {
    fn new(start_open: bool) -> Self {
        Self {
            state: Mutex::new(State {
                closed: false,
                owned: false,
                pending: false,
                start_open,
                consumed: 0,
                sequence: 0,
                slot: None,
                notification_scope: None,
            }),
            changed: Notify::new(),
            drained: Notify::new(),
            cancel: watch::channel(false).0,
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    fn close(&self) {
        let removed = {
            let mut state = self.lock();
            state.closed = true;
            state.pending = false;
            state.slot.take()
        };
        // Dropping a context can release runtime/callback ownership. Never do it
        // under a state lock or runtime admission guard.
        drop(removed);
        self.cancel.send_replace(true);
    }
    async fn join(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.lock().owned {
                return;
            }
            notified.await;
        }
    }
    fn consume(&self) -> Option<Retained> {
        let mut state = self.lock();
        if state.closed || !state.pending {
            return None;
        }
        state.pending = false;
        state.consumed = state.consumed.saturating_add(1);
        // Taking before publication is essential: its completion must never
        // clear a newer context that arrived while this context was in flight.
        state.slot.take()
    }
    fn discard_stale(&self) {
        let candidate = self
            .lock()
            .slot
            .as_ref()
            .map(|held| (held.sequence, held.fence.clone()));
        if let Some((sequence, fence)) = candidate {
            // The fence acquires runtime admission: do not hold state while
            // checking, and recheck slot identity after reacquiring state.
            if !fence.is_current() {
                let removed = {
                    let mut state = self.lock();
                    if state
                        .slot
                        .as_ref()
                        .is_some_and(|held| held.sequence == sequence)
                    {
                        state.slot.take()
                    } else {
                        None
                    }
                };
                drop(removed);
            }
        }
    }
}

/// A cloneable notification handle bound to exactly one setup/run. Obtaining a
/// new run's handle is explicit; old handles never target the new session.
#[derive(Clone)]
pub struct GatedLlmContextNotifier {
    session: Arc<Session>,
}
impl GatedLlmContextNotifier {
    /// Coalesce a signal for the single supervisor. A signal is consumed even
    /// when its slot is empty; it does not become a ticket for a future context.
    /// A context arriving before that consumption can still be released.
    pub fn notify(&self) -> Result<(), ProcessorError> {
        {
            let mut state = self.session.lock();
            if state.closed || !state.owned {
                return Err(ProcessorError::new("gated LLM context session is closed"));
            }
            state.pending = true;
        }
        self.session.changed.notify_waiters();
        Ok(())
    }
    // Capture before an asynchronous predicate. A later target interruption
    // changes only future tickets; it cannot refresh an old callback's scope.
    pub(crate) fn notification_ticket(
        &self,
        source: &InvocationFence,
    ) -> Result<GatedNotificationTicket, ProcessorError> {
        let target = {
            let state = self.session.lock();
            if state.closed || !state.owned {
                return Err(ProcessorError::new("gated LLM context session is closed"));
            }
            state
                .notification_scope
                .clone()
                .ok_or_else(|| ProcessorError::new("gated LLM context has not processed Start"))?
        };
        Ok(GatedNotificationTicket {
            session: self.session.clone(),
            combined: source.intersection(&target)?,
        })
    }
    pub fn snapshot(&self) -> GatedLlmContextSnapshot {
        let state = self.session.lock();
        GatedLlmContextSnapshot {
            closed: state.closed,
            context_pending: state.slot.is_some(),
            notification_pending: state.pending,
            notifications_consumed: state.consumed,
            supervisor_running: state.owned,
        }
    }
}

// One admission attempt, tied to both original invocations and this fixed
// session. The signal itself retains no source fence: once admitted it has the
// existing coalescing semantics, including consumption while the slot is empty.
pub(crate) struct GatedNotificationTicket {
    session: Arc<Session>,
    combined: InvocationFence,
}
impl GatedNotificationTicket {
    pub(crate) fn notify(self) -> Result<(), ProcessorError> {
        let admitted = self.combined.with_current(|| {
            let mut state = self.session.lock();
            if state.closed || !state.owned {
                return Err(ProcessorError::new("gated LLM context session is closed"));
            }
            state.pending = true;
            Ok(())
        });
        // A synchronous waker may reenter routing. Release both runtime
        // admission and session locks before waking the supervisor.
        match admitted {
            Some(Ok(())) => {
                self.session.changed.notify_waiters();
                Ok(())
            }
            Some(Err(error)) => Err(error),
            None => Ok(()), // A stale source/target never signals another epoch.
        }
    }
}

// Created before spawn admission. Even rejection or an unpolled future's drop
// releases the slot and ownership, and retires this session's notifier.
struct Owned(Arc<Session>);
impl Drop for Owned {
    fn drop(&mut self) {
        self.0.close();
        self.0.lock().owned = false;
        self.0.drained.notify_waiters();
    }
}
struct Lifecycle {
    active: bool,
    session: Arc<Session>,
}

/// Gates `LlmContext` and `LlmContextDetailed`, retaining only the latest original
/// frame. Notifications release downstream; `start_open` passes only the first
/// context in its original direction. All other frames preserve their identity
/// and direction, including Start (exactly once).
///
/// Each setup owns one interruption-surviving supervisor. Each delayed context
/// retains its original invocation and fence: notification cannot renew its
/// causal epoch. Interruption discards stale buffered work. End/Stop/Cancel,
/// prepare_end, cleanup, worker-owner drop, and supervisor failure retire the
/// session and release its slot. Core publication is nonblocking queue admission;
/// a full destination fails rather than waiting for capacity. A failed release
/// closes the session, clears the slot and reports the core error without retry.
/// Selection takes the slot under its lock: a context already selected by a
/// notification may publish while close is joining it, before close returns.
/// Termination does not flush an unselected context or undo admitted effects.
///
/// Sequential reuse requires prior work to have joined. Concurrent pipeline use
/// is unsupported: setup rejects it, but core cleanup has no invocation identity
/// and that diagnostic is not an isolation boundary. Contexts are owned native
/// snapshots; Python's separately shared mutable context object is not emulated.
pub struct GatedLlmContextAggregator {
    config: GatedLlmContextAggregatorConfig,
    lifecycle: Mutex<Lifecycle>,
}
impl GatedLlmContextAggregator {
    pub fn new(config: GatedLlmContextAggregatorConfig) -> Result<Self, ProcessorError> {
        if config.max_context_bytes == 0 || config.max_context_bytes > 64 * 1024 * 1024 {
            return Err(ProcessorError::new(
                "invalid gated LLM context payload budget",
            ));
        }
        let session = Arc::new(Session::new(config.start_open));
        session.close();
        Ok(Self {
            config,
            lifecycle: Mutex::new(Lifecycle {
                active: false,
                session,
            }),
        })
    }
    fn session(&self) -> Arc<Session> {
        self.lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .session
            .clone()
    }
    /// Fails before setup and after the run closes. Keep the processor in an Arc
    /// to obtain the handle after Pipeline::start has completed.
    pub fn current_notifier(&self) -> Result<GatedLlmContextNotifier, ProcessorError> {
        let lifecycle = self
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let state = lifecycle.session.lock();
        if !lifecycle.active || state.closed || !state.owned {
            return Err(ProcessorError::new(
                "gated LLM context has no active session",
            ));
        }
        Ok(GatedLlmContextNotifier {
            session: lifecycle.session.clone(),
        })
    }
    async fn close(&self, retire: bool) {
        let session = self.session();
        session.close();
        session.join().await;
        if retire {
            self.lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active = false;
        }
    }
}
#[async_trait]
impl FrameProcessor for GatedLlmContextAggregator {
    fn name(&self) -> &str {
        "GatedLlmContextAggregator"
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let session = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if lifecycle.active || lifecycle.session.lock().owned {
                return Err(ProcessorError::new(
                    "gated LLM context processor is already in use",
                ));
            }
            let session = Arc::new(Session::new(self.config.start_open));
            session.lock().owned = true;
            lifecycle.session = session.clone();
            lifecycle.active = true;
            session
        };
        let owned = Owned(session.clone());
        let mut cancel = session.cancel.subscribe();
        ctx.spawn("gated-llm-context", false, async move {
            let _owned = owned;
            let session = &_owned.0;
            loop {
                if *cancel.borrow() {
                    return Ok(());
                }
                let notified = session.changed.notified();
                tokio::pin!(notified);
                // Registration precedes every state inspection, so a signal
                // between inspection and await cannot be lost.
                notified.as_mut().enable();
                if let Some(held) = session.consume() {
                    held.ctx.try_push_with_invocation_fence(
                        held.frame,
                        Direction::Downstream,
                        &held.fence,
                    )?;
                    continue;
                }
                tokio::select! {
                    biased;
                    _ = cancel.changed() => return Ok(()),
                    _ = &mut notified => {},
                }
            }
        })
        .map(|_| ())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
                | FrameData::Stop
        ) {
            self.close(matches!(frame.data, FrameData::Stop)).await;
            return ctx.push(frame, direction).await;
        }
        let session = self.session();
        if frame.data.is_start() || matches!(frame.data, FrameData::Interruption) {
            let scope = ctx.invocation_fence()?;
            scope.with_current(|| {
                let mut state = session.lock();
                if !state.closed {
                    state.notification_scope = Some(scope.clone());
                }
            });
        }
        if matches!(frame.data, FrameData::Interruption) {
            session.discard_stale();
            return ctx.push(frame, direction).await;
        }
        if !matches!(
            frame.data,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
        ) {
            return ctx.push(frame, direction).await;
        }
        // Validate before replacing the previous valid context or consuming the
        // first-frame bypass. No payload or error content is formatted via Debug.
        context_bytes(&frame, self.config.max_context_bytes).ok_or_else(|| {
            ProcessorError::new(
                "gated LLM context exceeds its payload budget or cannot be accounted for",
            )
        })?;
        let fence = ctx.invocation_fence()?;
        let mut incoming = Some(frame);
        let mut removed = None;
        let mut open = false;
        let admitted = fence.with_current(|| {
            let mut state = session.lock();
            if state.closed {
                return false;
            }
            if state.start_open {
                state.start_open = false;
                open = true;
            } else {
                let Some(sequence) = state.sequence.checked_add(1) else {
                    return false;
                };
                state.sequence = sequence;
                removed = state.slot.replace(Retained {
                    frame: incoming.take().expect("context is admitted once"),
                    ctx: ctx.clone(),
                    fence: fence.clone(),
                    sequence,
                });
            }
            true
        });
        drop(removed);
        if admitted == Some(false) {
            return Err(ProcessorError::new(
                "gated LLM context session is closed or exhausted",
            ));
        }
        if open {
            if let Err(error) = ctx.try_push_with_invocation_fence(
                incoming.take().expect("open gate retains input"),
                direction,
                &fence,
            ) {
                let mut state = session.lock();
                if !state.closed {
                    state.start_open = true;
                }
                return Err(error);
            }
        }
        Ok(())
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.close(false).await;
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.close(true).await;
        Ok(())
    }
}

/// Allocation-free counting of the inspectable payload. Native frame envelopes
/// are never serialized, reconstructed, or stripped of their local authority.
fn context_bytes(frame: &Frame, limit: usize) -> Option<usize> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl Counter {
        fn add(&mut self, bytes: usize) -> Option<()> {
            self.bytes = self
                .bytes
                .checked_add(bytes)
                .filter(|bytes| *bytes <= self.limit)?;
            Some(())
        }
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.add(bytes.len())
                .ok_or_else(|| std::io::Error::other("context payload budget"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    counter.add(std::mem::size_of::<Retained>())?;
    counter.add(frame.native_llm_continuation_retained_bytes())?;
    counter.add(frame.name.len())?;
    counter.add(frame.transport_source.as_ref().map_or(0, String::len))?;
    counter.add(frame.transport_destination.as_ref().map_or(0, String::len))?;
    serde_json::to_writer(&mut counter, &frame.metadata).ok()?;
    if let FrameData::LlmContextDetailed(detailed) = &frame.data {
        serde_json::to_writer(&mut counter, &detailed.context.messages).ok()?;
        serde_json::to_writer(&mut counter, &detailed.context.tool_choice).ok()?;
        // These local callbacks are not wire serializable. Count their public
        // schema and shallow handles while preserving the original functions.
        match &detailed.context.tools {
            ToolSet::Direct(tools) => {
                counter.add(
                    tools
                        .len()
                        .checked_mul(std::mem::size_of::<pipecat_core::DirectFunction>())?,
                )?;
                for tool in tools {
                    serde_json::to_writer(&mut counter, &tool.schema).ok()?;
                }
            }
            ToolSet::Schema(schema) => {
                counter.add(
                    schema
                        .standard_tools
                        .len()
                        .checked_mul(std::mem::size_of::<pipecat_core::StandardTool>())?,
                )?;
                for tool in &schema.standard_tools {
                    match tool {
                        pipecat_core::StandardTool::Function(definition) => {
                            serde_json::to_writer(&mut counter, definition).ok()?;
                        }
                        pipecat_core::StandardTool::Direct(tool) => {
                            serde_json::to_writer(&mut counter, &tool.schema).ok()?;
                        }
                    }
                }
                serde_json::to_writer(&mut counter, &schema.custom_tools).ok()?;
            }
            tools => {
                serde_json::to_writer(&mut counter, tools).ok()?;
            }
        }
    } else {
        serde_json::to_writer(&mut counter, &frame.data).ok()?;
    }
    Some(counter.bytes)
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use pipecat_core::{
        ChatMessage, ChatRole, LlmContextFrame, Pipeline, PipelineConfig, PipelineWorker,
    };
    use std::time::Duration;

    fn context(text: &str) -> Frame {
        Frame::new(FrameData::LlmContext(LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, text)],
            ..Default::default()
        }))
    }
    async fn recv(worker: &mut PipelineWorker) -> pipecat_core::DirectedFrame {
        tokio::time::timeout(Duration::from_secs(5), worker.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn barrier(worker: &mut PipelineWorker) {
        let frame = Frame::text("barrier");
        let id = frame.id;
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        assert_eq!(recv(worker).await.frame.id, id);
    }
    async fn start(processor: Arc<GatedLlmContextAggregator>) -> PipelineWorker {
        let mut worker = Pipeline::new(vec![processor])
            .start(Default::default())
            .await
            .unwrap();
        assert!(recv(&mut worker).await.frame.data.is_start());
        worker
    }
    async fn end(worker: PipelineWorker) {
        worker.sender().end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), worker.collect())
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn unpolled_owned_future_drop_retires_only_its_session() {
        let old = Arc::new(Session::new(false));
        let new = Arc::new(Session::new(false));
        old.lock().owned = true;
        new.lock().owned = true;
        let owned = Owned(old.clone());
        let future = async move {
            let _owned = owned;
            std::future::pending::<()>().await;
        };
        drop(future);
        old.join().await;
        assert!(old.lock().closed);
        assert!(!old.lock().owned);
        assert!(!new.lock().closed);
        assert!(new.lock().owned);
    }

    #[tokio::test]
    async fn taken_release_never_clears_a_context_arriving_before_publication() {
        let processor = Arc::new(GatedLlmContextAggregator::new(Default::default()).unwrap());
        let mut worker = start(processor.clone()).await;
        let notifier = processor.current_notifier().unwrap();
        let first = context("selected first");
        let first_id = first.id;
        worker
            .sender()
            .send(first, Direction::Downstream)
            .await
            .unwrap();
        barrier(&mut worker).await;
        notifier.notify().unwrap();
        // Same current-thread poll: select exactly as the supervisor does, then
        // leave publication pending while a newer invocation reaches the gate.
        let selected = processor.session().consume().unwrap();
        let next = context("arrived while the old release was in flight");
        let next_id = next.id;
        worker
            .sender()
            .send(next, Direction::Downstream)
            .await
            .unwrap();
        barrier(&mut worker).await;
        assert!(notifier.snapshot().context_pending);
        selected
            .ctx
            .try_push_with_invocation_fence(selected.frame, Direction::Downstream, &selected.fence)
            .unwrap();
        assert_eq!(recv(&mut worker).await.frame.id, first_id);
        assert!(
            notifier.snapshot().context_pending,
            "old publication cleared the replacement slot"
        );
        notifier.notify().unwrap();
        assert_eq!(recv(&mut worker).await.frame.id, next_id);
        end(worker).await;
    }

    #[tokio::test]
    async fn notification_while_empty_can_select_a_context_arriving_before_consumption() {
        let processor = Arc::new(GatedLlmContextAggregator::new(Default::default()).unwrap());
        let mut worker = start(processor.clone()).await;
        let notifier = processor.current_notifier().unwrap();
        worker
            .sender()
            .send(context("context"), Direction::Downstream)
            .await
            .unwrap();
        barrier(&mut worker).await;
        let session = processor.session();
        let held = session.lock().slot.take();
        assert!(!notifier.snapshot().context_pending);
        notifier.notify().unwrap();
        assert!(notifier.snapshot().notification_pending);
        session.lock().slot = held;
        let selected = session.consume();
        assert!(
            selected.is_some(),
            "notify must not synchronously drop empty signals"
        );
        assert_eq!(notifier.snapshot().notifications_consumed, 1);
        drop(selected);
        end(worker).await;
    }

    struct Capture {
        gate: Arc<GatedLlmContextAggregator>,
        ctx: Mutex<Option<ProcessorContext>>,
    }
    #[async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "CaptureGateInvocation"
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.gate.setup(config, ctx).await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.ctx.lock().unwrap() = Some(ctx.clone());
            self.gate.process(frame, direction, ctx).await
        }
        async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
            self.gate.prepare_end(ctx).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.ctx.lock().unwrap().take();
            self.gate.cleanup().await
        }
    }
    #[tokio::test]
    async fn failed_first_context_publication_restores_start_open_until_session_closes() {
        let processor = Arc::new(
            GatedLlmContextAggregator::new(GatedLlmContextAggregatorConfig {
                start_open: true,
                ..Default::default()
            })
            .unwrap(),
        );
        let capture = Arc::new(Capture {
            gate: processor.clone(),
            ctx: Mutex::new(None),
        });
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(PipelineConfig {
                output_capacity: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(recv(&mut worker).await.frame.data.is_start());
        let ctx = capture.ctx.lock().unwrap().as_ref().unwrap().clone();
        processor
            .process(Frame::text("fill output"), Direction::Downstream, &ctx)
            .await
            .unwrap();
        let failure = processor
            .process(context("first"), Direction::Downstream, &ctx)
            .await
            .unwrap_err();
        assert!(failure.message.contains("output queue is full"));
        assert!(processor.session().lock().start_open);
        // Failed queue admission closes the core runtime; no same-runtime retry
        // is promised. The rollback records that no first context was admitted.
        assert!(
            tokio::time::timeout(Duration::from_secs(5), worker.collect())
                .await
                .unwrap()
                .is_err()
        );
        let mut worker = start(processor).await;
        let first = context("fresh run first");
        let id = first.id;
        worker
            .sender()
            .send(first, Direction::Upstream)
            .await
            .unwrap();
        let actual = recv(&mut worker).await;
        assert_eq!(actual.frame.id, id);
        assert_eq!(actual.direction, Direction::Upstream);
        end(worker).await;
    }
}
