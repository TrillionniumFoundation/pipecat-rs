//! Owned, bounded idle monitoring for a stream of original frames.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use crate::FramePredicate;
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
use tokio::sync::{watch, Notify};

/// A serial, cooperative callback with no implicit pipeline-emission authority.
/// Do not block the executor or detach work. Cancellation drops the owned future;
/// it cannot undo external effects. Destructors and panic payloads must not panic.
pub type IdleCallback =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), ProcessorError>> + Send>> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct IdleFrameProcessorConfig {
    /// Positive duration, at most 24 hours.
    pub idle_timeout: Duration,
    /// Positive cooperative deadline, at most 60 seconds.
    pub callback_timeout: Duration,
}
impl Default for IdleFrameProcessorConfig {
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(5),
            callback_timeout: Duration::from_secs(10),
        }
    }
}
#[derive(Default)]
struct State {
    started: bool,
    closed: bool,
    pending: bool,
    owned: bool,
}
struct Session {
    state: Mutex<State>,
    activity: Notify,
    drained: Notify,
    cancel: watch::Sender<bool>,
}
impl Session {
    fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            activity: Notify::new(),
            drained: Notify::new(),
            cancel: watch::channel(false).0,
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn signal(&self) {
        let mut state = self.lock();
        if !state.closed {
            state.pending = true;
            self.activity.notify_waiters();
        }
    }
    fn close(&self) {
        self.lock().closed = true;
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
}
// Constructed before admission and captured by the future, so rejection or
// cancellation before its first poll releases ownership just as completion does.
struct Owned(Arc<Session>);
impl Drop for Owned {
    fn drop(&mut self) {
        self.0.lock().owned = false;
        self.0.drained.notify_waiters();
    }
}
struct Lifecycle {
    active: bool,
    session: Arc<Session>,
}

/// Forwards every original frame in its original direction, then records matching
/// activity. `None` monitors all frames; a predicate is an explicit native filter,
/// not Python class-list equivalence. Predicates must be fast, pure and nonblocking;
/// a predicate panic follows the core panic policy, not a pass-through guarantee.
///
/// One runtime-owned supervisor survives interruption. Callbacks are serial and a
/// full idle interval starts after each completion. Activity during a callback is
/// coalesced and retained. Failure stops monitoring until a new setup; repeated
/// starts never restart it. End/Stop/Cancel and cleanup cancel owned work.
/// Sequential reuse requires joining the prior worker. Concurrent pipeline use is
/// unsupported: setup rejects it, but core cleanup has no invocation identity and
/// that diagnostic is not an isolation boundary.
pub struct IdleFrameProcessor {
    config: IdleFrameProcessorConfig,
    filter: Option<FramePredicate>,
    callback: IdleCallback,
    lifecycle: Mutex<Lifecycle>,
}
impl IdleFrameProcessor {
    pub fn new(
        config: IdleFrameProcessorConfig,
        filter: Option<FramePredicate>,
        callback: IdleCallback,
    ) -> Result<Self, ProcessorError> {
        if config.idle_timeout.is_zero()
            || config.idle_timeout > Duration::from_secs(24 * 60 * 60)
            || config.callback_timeout.is_zero()
            || config.callback_timeout > Duration::from_secs(60)
        {
            return Err(ProcessorError::new(
                "invalid idle-frame processor configuration",
            ));
        }
        Ok(Self {
            config,
            filter,
            callback,
            lifecycle: Mutex::new(Lifecycle {
                active: false,
                session: Arc::new(Session::new()),
            }),
        })
    }
    fn session(&self) -> Arc<Session> {
        self.lifecycle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .session
            .clone()
    }
    fn start(&self, session: &Arc<Session>, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        {
            let mut state = session.lock();
            if state.started || state.closed {
                return Ok(());
            }
            state.started = true;
            state.owned = true;
        }
        let owned = Owned(session.clone());
        let callback = self.callback.clone();
        let config = self.config.clone();
        let mut cancel = session.cancel.subscribe();
        ctx.spawn("idle-frame-monitor", false, async move {
            let _owned = owned;
            let session = &_owned.0;
            loop {
                if *cancel.borrow() {
                    return Ok(());
                }
                // Clear/coalesce the event before beginning a fresh full interval.
                session.lock().pending = false;
                let activity = session.activity.notified();
                tokio::pin!(activity);
                activity.as_mut().enable();
                if session.lock().pending {
                    continue;
                }
                tokio::select! {
                    biased;
                    _ = cancel.changed() => return Ok(()),
                    _ = &mut activity => continue,
                    _ = tokio::time::sleep(config.idle_timeout) => {}
                }
                let guarded =
                    std::panic::AssertUnwindSafe(async { callback().await }).catch_unwind();
                let result = tokio::select! {
                    biased;
                    _ = cancel.changed() => return Ok(()),
                    result = tokio::time::timeout(config.callback_timeout, guarded) => result,
                };
                match result {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(_))) => {
                        return Err(ProcessorError::new("idle-frame callback failed"))
                    }
                    Ok(Err(_)) => return Err(ProcessorError::new("idle-frame callback panicked")),
                    Err(_) => return Err(ProcessorError::new("idle-frame callback timed out")),
                }
            }
        })
        .map(|_| ())
    }
}
#[async_trait]
impl FrameProcessor for IdleFrameProcessor {
    fn name(&self) -> &str {
        "IdleFrameProcessor"
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut lifecycle = self.lifecycle.lock().unwrap_or_else(|e| e.into_inner());
        if lifecycle.active || lifecycle.session.lock().owned {
            return Err(ProcessorError::new(
                "idle-frame processor is already in use",
            ));
        }
        lifecycle.session = Arc::new(Session::new());
        lifecycle.active = true;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let session = self.session();
        let result = if frame.data.is_start() {
            self.start(&session, ctx)
        } else {
            Ok(())
        };
        let stop = matches!(frame.data, FrameData::Stop);
        if matches!(
            frame.data,
            FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
                | FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
        ) {
            session.close();
        }
        let matches = self.filter.as_ref().is_none_or(|filter| filter(&frame));
        ctx.push(frame, direction).await?;
        if matches {
            session.signal();
        }
        if stop {
            self.lifecycle
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .active = false;
        }
        result
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let session = self.session();
        session.close();
        session.join().await;
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let session = self.session();
        session.close();
        session.join().await;
        self.lifecycle
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .active = false;
        Ok(())
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    #[tokio::test]
    async fn unpolled_future_drop_releases_only_its_own_session() {
        let old = Arc::new(Session::new());
        let new = Arc::new(Session::new());
        old.lock().owned = true;
        new.lock().owned = true;
        let owned = Owned(old.clone());
        let future = async move {
            let _owned = owned;
            std::future::pending::<()>().await;
        };
        old.close();
        drop(future);
        old.join().await;
        assert!(!old.lock().owned);
        assert!(new.lock().owned);
        assert!(!new.lock().closed);
    }

    // Exercise the private monitor start hook without sending a duplicate Start
    // through Pipeline ingress, which correctly rejects duplicate starts.
    struct RepeatStart(Arc<IdleFrameProcessor>);
    #[async_trait]
    impl FrameProcessor for RepeatStart {
        fn name(&self) -> &str {
            "RepeatStart"
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.0.setup(config, ctx).await
        }
        async fn process(
            &self,
            mut frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            let session = self.0.session();
            if frame.data.is_start() {
                frame.data = FrameData::StartDetailed(pipecat_core::DetailedStartFrame {
                    configuration: Default::default(),
                    enable_tracing: false,
                    tracing_context: None,
                });
            }
            self.0.process(frame, direction, ctx).await?;
            self.0.start(&session, ctx)?;
            self.0.start(&session, ctx)
        }
        async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
            self.0.prepare_end(ctx).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.0.cleanup().await
        }
    }
    #[tokio::test(start_paused = true)]
    async fn detailed_start_and_repeated_private_start_use_one_monitor_even_after_failure() {
        use pipecat_core::{Pipeline, PipelineConfig};
        use std::sync::atomic::{AtomicUsize, Ordering};
        for fail in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let callback: IdleCallback = {
                let calls = calls.clone();
                Arc::new(move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async move {
                        if fail {
                            Err(ProcessorError::new("private"))
                        } else {
                            Ok(())
                        }
                    })
                })
            };
            let inner =
                Arc::new(IdleFrameProcessor::new(Default::default(), None, callback).unwrap());
            let mut worker = Pipeline::new(vec![Arc::new(RepeatStart(inner))])
                .start(PipelineConfig {
                    max_background_tasks: 1,
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(matches!(
                tokio::time::timeout(Duration::from_secs(100), worker.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .frame
                    .data,
                FrameData::StartDetailed(_)
            ));
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(5)).await;
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            worker
                .sender()
                .send(Frame::text("retry start hook"), Direction::Downstream)
                .await
                .unwrap();
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            tokio::time::advance(Duration::from_secs(5)).await;
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert_eq!(calls.load(Ordering::SeqCst), if fail { 1 } else { 2 });
            worker.end().await.unwrap();
            let (_, report) = tokio::time::timeout(Duration::from_secs(100), worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(report.errors.len(), usize::from(fail));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn worker_owner_drop_joins_monitor_during_wait_or_callback() {
        use pipecat_core::Pipeline;
        use std::sync::atomic::{AtomicUsize, Ordering};
        struct DropCount(Arc<AtomicUsize>);
        impl Drop for DropCount {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        for active in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let callback: IdleCallback = {
                let calls = calls.clone();
                let drops = drops.clone();
                Arc::new(move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let guard = DropCount(drops.clone());
                    Box::pin(async move {
                        let _guard = guard;
                        std::future::pending::<()>().await;
                        Ok(())
                    })
                })
            };
            let inner =
                Arc::new(IdleFrameProcessor::new(Default::default(), None, callback).unwrap());
            let mut worker = Pipeline::new(vec![inner.clone()])
                .start(Default::default())
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(100), worker.recv())
                .await
                .unwrap()
                .unwrap();
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            if active {
                tokio::time::advance(Duration::from_secs(5)).await;
                for _ in 0..32 {
                    tokio::task::yield_now().await;
                }
            }
            let session = inner.session();
            drop(worker);
            tokio::time::timeout(Duration::from_secs(100), async {
                loop {
                    let owned = session.lock().owned;
                    let running = inner.lifecycle.lock().unwrap().active;
                    if !owned && !running {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            assert_eq!(drops.load(Ordering::SeqCst), usize::from(active));
            tokio::time::advance(Duration::from_secs(30)).await;
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(active));
        }
    }
}
