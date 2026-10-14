//! Await a conditional notification, then forward the untouched original frame.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use crate::{AsyncFramePredicate, FramePredicate, GatedLlmContextNotifier};
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, Frame, FrameProcessor, ProcessorContext, ProcessorError,
};
use std::{future::Future, panic::AssertUnwindSafe, pin::Pin, sync::Arc, time::Duration};

/// An asynchronous, content-free notification. Callbacks must cooperate with
/// cancellation and must not block the executor or detach work. Construction
/// and polling are panic-contained; dropping a future cannot undo side effects
/// that it already started or committed.
pub type AsyncWakeNotification =
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<(), ProcessorError>> + Send>> + Send + Sync>;

/// Independent cooperative deadline for each predicate and notification call.
#[derive(Clone, Debug)]
pub struct WakeNotifierFilterConfig {
    /// Positive and at most sixty seconds. This cannot preempt blocking code.
    pub callback_timeout: Duration,
}
impl Default for WakeNotifierFilterConfig {
    fn default() -> Self {
        Self {
            callback_timeout: Duration::from_secs(10),
        }
    }
}
impl WakeNotifierFilterConfig {
    fn validate(&self) -> Result<(), ProcessorError> {
        if self.callback_timeout.is_zero() || self.callback_timeout > Duration::from_secs(60) {
            return Err(error("invalid callback timeout"));
        }
        Ok(())
    }
}

enum Notification {
    Callback(AsyncWakeNotification),
    GatedContext(GatedLlmContextNotifier),
}

/// Native adapter for Python's WakeNotifierFilter ordering:
/// selector -> awaited predicate -> (if true) awaited notify -> original push.
/// The synchronous selector expresses Rust frame matching; it does not emulate
/// Python's complete isinstance inheritance table. Every direction and lifecycle
/// frame follows this same path, with no special bypass or background task.
///
/// The predicate receives only a borrowed frame. Identity, metadata, local tags
/// and native authority are neither cloned for callbacks nor rewritten. A false
/// predicate or nonmatch forwards normally; errors and callback panics/timeouts
/// stop before forwarding. A successful notification is not retried or rolled
/// back if the subsequent original push fails.
///
/// Generic notifications only observe the original source scope before calling
/// the notifier. They cannot atomically cancel external effects. The typed gate
/// adapter instead admits one local coalescing signal under the original source
/// and target fences, frozen before the predicate awaits. Target interruptions
/// do not refresh an old ticket. A validly captured ticket that later expires
/// skips notification; the original frame still uses the core's existing
/// push/survival rules. Ticket-capture errors are deferred until a true predicate
/// and then stop before forwarding; a false predicate ignores them. A signal
/// already admitted is not retracted when its source later expires.
pub struct WakeNotifierFilter {
    config: WakeNotifierFilterConfig,
    selector: FramePredicate,
    filter: AsyncFramePredicate,
    notification: Notification,
}
impl WakeNotifierFilter {
    pub fn new(
        config: WakeNotifierFilterConfig,
        selector: FramePredicate,
        filter: AsyncFramePredicate,
        notify: AsyncWakeNotification,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        Ok(Self {
            config,
            selector,
            filter,
            notification: Notification::Callback(notify),
        })
    }
    /// Bind exactly this gate session. Obtain its notifier after observing the
    /// target pipeline's Start; this adapter never looks up a newer run's handle.
    pub fn for_gated_context(
        config: WakeNotifierFilterConfig,
        selector: FramePredicate,
        filter: AsyncFramePredicate,
        notifier: GatedLlmContextNotifier,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        Ok(Self {
            config,
            selector,
            filter,
            notification: Notification::GatedContext(notifier),
        })
    }
}
#[async_trait]
impl FrameProcessor for WakeNotifierFilter {
    fn name(&self) -> &str {
        "WakeNotifierFilter"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let selected = std::panic::catch_unwind(AssertUnwindSafe(|| (self.selector)(&frame)))
            .map_err(|_| error("selector panicked"))?;
        if selected {
            // Freeze both scopes before any asynchronous predicate. Errors are
            // intentionally deferred: false predicates never invoke notify.
            let source = ctx.invocation_fence();
            let ticket = match &self.notification {
                Notification::GatedContext(notifier) => Some(match &source {
                    Ok(source) => notifier.notification_ticket(source),
                    Err(_) => Err(error("could not capture source scope")),
                }),
                Notification::Callback(_) => None,
            };
            let matched = callback(
                || (self.filter)(&frame),
                self.config.callback_timeout,
                "predicate panicked",
                "predicate timed out",
            )
            .await?;
            if matched {
                match &self.notification {
                    Notification::GatedContext(_) => {
                        ticket
                            .expect("typed notification has a captured ticket")?
                            .notify()?;
                    }
                    Notification::Callback(notify) => {
                        let source = source?;
                        // Observations only: never run user code while holding
                        // runtime admission or state locks.
                        if source.is_current() {
                            callback(
                                || notify(),
                                self.config.callback_timeout,
                                "notification panicked",
                                "notification timed out",
                            )
                            .await?;
                            // A stale completion cannot undo an effect.
                            // Forward below through the original core context.
                        }
                    }
                }
            }
        }
        ctx.push(frame, direction).await
    }
}

async fn callback<T, F, Fut>(
    make: F,
    timeout: Duration,
    panic_message: &'static str,
    timeout_message: &'static str,
) -> Result<T, ProcessorError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, ProcessorError>>,
{
    // Both construction and every poll occur inside the unwind boundary.
    tokio::time::timeout(
        timeout,
        AssertUnwindSafe(async { make().await }).catch_unwind(),
    )
    .await
    .map_err(|_| error(timeout_message))?
    .map_err(|_| error(panic_message))?
}
fn error(message: &str) -> ProcessorError {
    ProcessorError::new(format!("wake notifier: {message}"))
}
