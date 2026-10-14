//! Bounded, session-local adaptation of Python's AsyncGeneratorProcessor.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    StartFrame,
};
use pipecat_protocol::{adapters::FrameSerializer, WireMessage};
use std::{
    collections::VecDeque,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
};
use tokio::sync::{Mutex as AsyncMutex, Notify};

/// Called once per setup, outside all lifecycle/queue locks. Return a fresh
/// serializer: reusing external mutable codec state defeats run isolation.
/// The bridge does not call serializer setup, deserialize, or flush methods.
pub type AsyncGeneratorSerializerFactory =
    Arc<dyn Fn() -> Result<Box<dyn FrameSerializer + Send>, ProcessorError> + Send + Sync>;

/// Limits on queued results, not allocations made inside a user serializer.
#[derive(Clone, Debug)]
pub struct AsyncGeneratorProcessorConfig {
    /// Positive, at most 65,536 messages. A full queue fails closed immediately.
    pub max_queued_messages: usize,
    /// Positive, at most 16 MiB. Counts String/Vec capacity, including spare bytes.
    pub max_message_bytes: usize,
    /// Positive, at most 64 MiB. Sum of the capacities of queued payloads.
    /// Dequeued messages and serializer-owned/temporary allocations are excluded;
    /// bounded queue bookkeeping is covered separately by the message count.
    pub max_queued_bytes: usize,
}
impl Default for AsyncGeneratorProcessorConfig {
    fn default() -> Self {
        Self {
            max_queued_messages: 1024,
            max_message_bytes: 1024 * 1024,
            max_queued_bytes: 4 * 1024 * 1024,
        }
    }
}
impl AsyncGeneratorProcessorConfig {
    fn validate(&self) -> Result<(), ProcessorError> {
        if self.max_queued_messages == 0
            || self.max_queued_messages > 65_536
            || self.max_message_bytes == 0
            || self.max_message_bytes > 16 * 1024 * 1024
            || self.max_queued_bytes == 0
            || self.max_queued_bytes > 64 * 1024 * 1024
        {
            return Err(ProcessorError::new(
                "invalid async-generator processor configuration",
            ));
        }
        Ok(())
    }
}

/// Content-free reason when failure closes the stream; first closure wins.
/// Codec error strings are not copied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AsyncGeneratorStreamError {
    ForwardingFailed,
    InvocationFenceUnavailable,
    NativeAuthority,
    SerializerFailed,
    SerializerPanicked,
    MessageTooLarge,
    QueueFull,
    QueueByteLimit,
    ReceiverDropped,
}
impl AsyncGeneratorStreamError {
    fn after_forward(self) -> ProcessorError {
        let reason = match self {
            Self::ForwardingFailed => "forwarding failed",
            Self::InvocationFenceUnavailable => "invocation fence unavailable",
            Self::NativeAuthority => "native semantic authority cannot be serialized",
            Self::SerializerFailed => "serializer failed",
            Self::SerializerPanicked => "serializer panicked",
            Self::MessageTooLarge => "message capacity limit exceeded",
            Self::QueueFull => "message count limit exceeded",
            Self::QueueByteLimit => "queued capacity limit exceeded",
            Self::ReceiverDropped => "receiver was dropped",
        };
        ProcessorError::new(format!(
            "async-generator stream failed after original-frame forwarding admission: {reason}"
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AsyncGeneratorSnapshot {
    pub closed: bool,
    pub receiver_taken: bool,
    pub queued_messages: usize,
    pub queued_bytes: usize,
    pub error_reason: Option<AsyncGeneratorStreamError>,
}

struct State {
    queue: VecDeque<WireMessage>,
    queued_bytes: usize,
    closed: bool,
    receiver_taken: bool,
    error_reason: Option<AsyncGeneratorStreamError>,
    serializer: Option<Box<dyn FrameSerializer + Send>>,
}
struct Session {
    config: AsyncGeneratorProcessorConfig,
    state: Mutex<State>,
    // Serializes mutable codec access without making close/recv wait for codec
    // completion. The codec itself is taken out of State before calling it.
    serialization: AsyncMutex<()>,
    changed: Notify,
}
fn capacity(message: &WireMessage) -> usize {
    match message {
        WireMessage::Text(text) => text.capacity(),
        WireMessage::Binary(bytes) => bytes.capacity(),
    }
}
fn is_empty(message: &WireMessage) -> bool {
    match message {
        WireMessage::Text(text) => text.is_empty(),
        WireMessage::Binary(bytes) => bytes.is_empty(),
    }
}
impl Session {
    fn new(
        config: AsyncGeneratorProcessorConfig,
        serializer: Box<dyn FrameSerializer + Send>,
    ) -> Self {
        Self {
            config,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                queued_bytes: 0,
                closed: false,
                receiver_taken: false,
                error_reason: None,
                serializer: Some(serializer),
            }),
            serialization: AsyncMutex::new(()),
            changed: Notify::new(),
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    fn snapshot(&self) -> AsyncGeneratorSnapshot {
        let state = self.lock();
        AsyncGeneratorSnapshot {
            closed: state.closed,
            receiver_taken: state.receiver_taken,
            queued_messages: state.queue.len(),
            queued_bytes: state.queued_bytes,
            error_reason: state.error_reason,
        }
    }
    fn close(&self, reason: Option<AsyncGeneratorStreamError>, discard: bool) {
        let (serializer, discarded) = {
            let mut state = self.lock();
            if !state.closed {
                state.closed = true;
                state.error_reason = reason;
            }
            let discarded = if discard {
                state.queued_bytes = 0;
                Some(std::mem::take(&mut state.queue))
            } else {
                None
            };
            (state.serializer.take(), discarded)
        };
        // Never run arbitrary codec destructors or wake callers under a queue,
        // lifecycle, or runtime-admission lock. An in-flight codec is owned by
        // its invocation and is dropped when that synchronous call returns.
        drop(serializer);
        drop(discarded);
        self.changed.notify_waiters();
    }
    fn admit(&self, message: WireMessage) -> Result<(), AsyncGeneratorStreamError> {
        let mut state = self.lock();
        if state.closed {
            // A terminal close may race a codec return. Its result is discarded
            // without converting a clean EOF into an error.
            return Ok(());
        }
        let bytes = capacity(&message);
        let reason = if bytes > self.config.max_message_bytes {
            Some(AsyncGeneratorStreamError::MessageTooLarge)
        } else if state.queue.len() >= self.config.max_queued_messages {
            Some(AsyncGeneratorStreamError::QueueFull)
        } else if bytes
            > self
                .config
                .max_queued_bytes
                .saturating_sub(state.queued_bytes)
        {
            Some(AsyncGeneratorStreamError::QueueByteLimit)
        } else {
            None
        };
        if let Some(reason) = reason {
            // Linearize failure against all other admissions here. Codec
            // destruction and notification are performed after the fence exits.
            state.closed = true;
            state.error_reason = Some(reason);
            return Err(reason);
        }
        state.queued_bytes += bytes;
        state.queue.push_back(message);
        Ok(())
    }
}

/// Single consumer for exactly one setup/run; it is deliberately not Clone.
/// Closure drains already queued FIFO data, then recv returns None forever.
/// Dropping this receiver closes the stream and releases its queued payloads.
pub struct AsyncGeneratorReceiver {
    session: Arc<Session>,
}
impl AsyncGeneratorReceiver {
    pub async fn recv(&mut self) -> Option<WireMessage> {
        loop {
            let notified = self.session.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.session.lock();
                if let Some(message) = state.queue.pop_front() {
                    state.queued_bytes -= capacity(&message);
                    return Some(message);
                }
                if state.closed {
                    return None;
                }
            }
            notified.await;
        }
    }
    /// True once closed, including while previously queued messages remain.
    pub fn is_closed(&self) -> bool {
        self.session.snapshot().closed
    }
    pub fn error_reason(&self) -> Option<AsyncGeneratorStreamError> {
        self.session.snapshot().error_reason
    }
    pub fn snapshot(&self) -> AsyncGeneratorSnapshot {
        self.session.snapshot()
    }
}
impl Drop for AsyncGeneratorReceiver {
    fn drop(&mut self) {
        self.session
            .close(Some(AsyncGeneratorStreamError::ReceiverDropped), true);
    }
}

#[derive(Default)]
struct Lifecycle {
    active: bool,
    session: Option<Arc<Session>>,
}

/// Forwards each original frame in its original direction, then serializes it.
/// Forwarding success is local admission, not downstream processing or delivery.
/// Later codec/budget failures cannot retract that forwarded frame. After a
/// stream failure, later originals still forward, but the codec stays closed;
/// the first error remains available through the snapshot/receiver diagnostics.
///
/// End/Cancel (including Detailed variants) close without calling the codec.
/// Start and Interruption do call it; interruption preserves queued messages.
/// Native Stop is serialized before closing. Cleanup and owner drop also close.
/// No flush, transport, network I/O, background task, or general event bus exists.
///
/// The synchronous serializer must be fast and nonblocking. Cancellation cannot
/// preempt it and no timeout is promised. Runtime cancellation handling can
/// itself wait for this call; results completed before terminal processing may
/// already be queued. Its returned allocation is checked only after return. An original invocation fence rejects unpublished stale
/// results, including surviving system/uninterruptible invocations; it never
/// renews their epoch. Already queued data is not retroactively invalidated,
/// and rejecting a result does not roll back codec state or external effects.
///
/// Sequential reuse requires joining the previous worker. Every setup creates
/// a fresh serializer and session, and old receivers never observe a new run.
/// Concurrent sharing between pipelines is unsupported; setup rejects it, but
/// identity-free core cleanup is not a concurrent-use isolation boundary.
pub struct AsyncGeneratorProcessor {
    config: AsyncGeneratorProcessorConfig,
    factory: AsyncGeneratorSerializerFactory,
    lifecycle: Mutex<Lifecycle>,
}
impl AsyncGeneratorProcessor {
    pub fn new(
        config: AsyncGeneratorProcessorConfig,
        factory: AsyncGeneratorSerializerFactory,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        Ok(Self {
            config,
            factory,
            lifecycle: Mutex::new(Lifecycle::default()),
        })
    }
    fn session(&self) -> Option<Arc<Session>> {
        self.lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .session
            .clone()
    }
    /// Obtain the current run's receiver once. Before setup or after it has
    /// already been taken, returns None. A closed run may still be drained.
    pub fn take_receiver(&self) -> Option<AsyncGeneratorReceiver> {
        let session = self.session()?;
        {
            let mut state = session.lock();
            if state.receiver_taken {
                return None;
            }
            state.receiver_taken = true;
        }
        Some(AsyncGeneratorReceiver { session })
    }
    pub fn current_snapshot(&self) -> Option<AsyncGeneratorSnapshot> {
        self.session().map(|session| session.snapshot())
    }
}
// Native Stop must close on every exit, including forwarding/codec failures or
// cooperative cancellation while waiting for the serial codec permit.
struct CloseOnExit<'a> {
    session: Option<Arc<Session>>,
    stop_lifecycle: Option<&'a Mutex<Lifecycle>>,
}
impl Drop for CloseOnExit<'_> {
    fn drop(&mut self) {
        if let Some(session) = &self.session {
            session.close(None, false);
        }
        // Native Stop does not necessarily run core cleanup. Joined reuse is
        // still required; this merely retires the stopped session for setup.
        if let Some(lifecycle) = self.stop_lifecycle {
            lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active = false;
        }
    }
}
#[async_trait]
impl FrameProcessor for AsyncGeneratorProcessor {
    fn name(&self) -> &str {
        "AsyncGeneratorProcessor"
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if lifecycle.active {
                return Err(ProcessorError::new(
                    "async-generator processor is already in use",
                ));
            }
            lifecycle.active = true;
        }
        let serializer = catch_unwind(AssertUnwindSafe(|| (self.factory)()));
        let serializer = match serializer {
            Ok(Ok(serializer)) => serializer,
            failure => {
                self.lifecycle
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .active = false;
                // Do not expose factory errors, which can contain private data.
                drop(failure);
                return Err(ProcessorError::new(
                    "async-generator serializer factory failed",
                ));
            }
        };
        let fresh = Arc::new(Session::new(self.config.clone(), serializer));
        let old = self
            .lifecycle
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .session
            .replace(fresh);
        if let Some(old) = old {
            old.close(None, false);
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let session = self
            .session()
            .ok_or_else(|| ProcessorError::new("async-generator processor has not been set up"))?;
        let source_terminal = matches!(
            frame.data,
            FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
        );
        let terminal = source_terminal || matches!(frame.data, FrameData::Stop);
        let _close = CloseOnExit {
            session: terminal.then(|| session.clone()),
            stop_lifecycle: matches!(frame.data, FrameData::Stop).then_some(&self.lifecycle),
        };
        // Capture before forwarding; no later observation renews this scope.
        let fence = ctx.invocation_fence();
        if let Err(error) = ctx.push(frame.clone(), direction).await {
            session.close(Some(AsyncGeneratorStreamError::ForwardingFailed), false);
            return Err(error);
        }
        if source_terminal {
            return Ok(());
        }
        if let Err(_error) = frame.require_no_native_llm_continuation() {
            session.close(Some(AsyncGeneratorStreamError::NativeAuthority), false);
            return Err(AsyncGeneratorStreamError::NativeAuthority.after_forward());
        }
        let fence = match fence {
            Ok(fence) => fence,
            Err(_) => {
                session.close(
                    Some(AsyncGeneratorStreamError::InvocationFenceUnavailable),
                    false,
                );
                return Err(AsyncGeneratorStreamError::InvocationFenceUnavailable.after_forward());
            }
        };
        let _serial = session.serialization.lock().await;
        if !fence.is_current() {
            return Ok(());
        }
        let mut serializer = {
            let mut state = session.lock();
            if state.closed {
                return Ok(());
            }
            state.serializer.take().expect("serial codec ownership")
        };
        // No queue/lifecycle/runtime lock is held while calling arbitrary code.
        let result = catch_unwind(AssertUnwindSafe(|| serializer.serialize(&frame)));
        let result = match result {
            Ok(Ok(result)) => result,
            failure => {
                let reason = if failure.is_err() {
                    AsyncGeneratorStreamError::SerializerPanicked
                } else {
                    AsyncGeneratorStreamError::SerializerFailed
                };
                session.close(Some(reason), false);
                drop(serializer);
                drop(failure);
                return Err(reason.after_forward());
            }
        };
        {
            let mut state = session.lock();
            if !state.closed {
                state.serializer = Some(serializer);
            } else {
                drop(state);
                drop(serializer);
                return Ok(());
            }
        }
        let Some(message) = result.filter(|message| !is_empty(message)) else {
            return Ok(());
        };
        let admitted = fence.with_current(|| session.admit(message));
        // Notify only after releasing runtime admission; registered wakers are
        // caller code. Never invoke the serializer or routing inside the fence.
        session.changed.notify_waiters();
        if let Some(Err(reason)) = admitted {
            session.close(Some(reason), false);
            return Err(reason.after_forward());
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let session = {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            lifecycle.active = false;
            lifecycle.session.clone()
        };
        if let Some(session) = session {
            session.close(None, false);
        }
        Ok(())
    }
}
impl Drop for AsyncGeneratorProcessor {
    fn drop(&mut self) {
        if let Some(session) = self.session() {
            session.close(None, false);
        }
    }
}
