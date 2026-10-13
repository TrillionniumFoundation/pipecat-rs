use super::{
    LocalToolCompletion, LocalToolFunction, LocalToolFunctionIdentity, LocalToolOptions,
    LocalToolReporter, LocalToolResult, ToolFunctionState, ToolFuture, MISSING_REPORTER,
};
use crate::{tool_outcome::encoded_bytes, NativeToolCallBatch, ProcessorError};
use serde_json::Value;
use std::{
    fmt,
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, MutexGuard, TryLockError, Weak,
    },
};
use tokio::sync::oneshot;

type FinalCompletion = Option<Result<LocalToolCompletion, ProcessorError>>;
struct State {
    reporter: Option<LocalToolReporter>,
    sender: Option<oneshot::Sender<FinalCompletion>>,
    final_claimed: bool,
}
struct Shared {
    closed: AtomicBool,
    state: Mutex<State>,
}
impl Shared {
    fn pending(&self) -> Result<MutexGuard<'_, State>, ProcessorError> {
        let state = self.state.try_lock().map_err(|error| match error {
            TryLockError::WouldBlock => ProcessorError::new("deferred tool responder is busy"),
            TryLockError::Poisoned(_) => {
                ProcessorError::fatal("deferred tool responder state poisoned")
            }
        })?;
        if self.closed.load(Ordering::Acquire) || state.final_claimed {
            return Err(closed());
        }
        Ok(state)
    }
}
fn closed() -> ProcessorError {
    ProcessorError::new("deferred tool responder is no longer active")
}

/// A weak endpoint for one admitted deferred call. Retaining or cloning this
/// endpoint cannot keep the invocation, its deadline, or its runtime alive.
#[derive(Clone)]
pub struct LocalToolResponder(Weak<Shared>);
impl fmt::Debug for LocalToolResponder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LocalToolResponder")
    }
}
impl LocalToolResponder {
    /// Report bounded progress. Success acknowledges the reporting sink, not
    /// context application. Contention and synchronous reentry return a busy error.
    pub async fn report(&self, result: LocalToolResult) -> Result<(), ProcessorError> {
        self.report_with_context_updated(LocalToolCompletion::new(result))
            .await
    }
    pub async fn report_with_context_updated(
        &self,
        completion: LocalToolCompletion,
    ) -> Result<(), ProcessorError> {
        let shared = self.0.upgrade().ok_or_else(closed)?;
        // Keep the callable's last resource-owning reference outside the state
        // lock even if the reporting sink rejects its owned completion.
        let retained_hook = completion.on_context_updated.clone();
        let result = {
            let state = shared.pending()?;
            let reporter = state.reporter.as_ref().ok_or_else(closed)?;
            (reporter.0)(completion)
        };
        drop(retained_hook);
        result
    }
    /// Hand off one bounded final result. Success is neither native frame
    /// admission nor context application. An early final waits for dispatch ack;
    /// a dispatch error or panic discards it. The first final claim closes reports.
    pub fn finish(
        &self,
        result: Result<LocalToolCompletion, ProcessorError>,
    ) -> Result<(), ProcessorError> {
        match &result {
            Ok(completion) => {
                encoded_bytes(
                    &completion.result.value,
                    NativeToolCallBatch::MAX_RETAINED_BYTES,
                )?;
            }
            Err(error) => {
                encoded_bytes(&error.message, NativeToolCallBatch::MAX_RETAINED_BYTES / 2)?;
            }
        }
        self.handoff(Some(result))
    }
    /// Acknowledge cancellation using typed `None`. This does not prove that
    /// external work or remote side effects were cancelled or rolled back.
    pub fn cancel(&self) -> Result<(), ProcessorError> {
        self.handoff(None)
    }
    fn handoff(&self, result: FinalCompletion) -> Result<(), ProcessorError> {
        let shared = self.0.upgrade().ok_or_else(closed)?;
        let (sender, reporter) = {
            let mut state = shared.pending()?;
            let sender = state.sender.take().ok_or_else(closed)?;
            state.final_claimed = true;
            (sender, state.reporter.take())
        };
        // Sending can synchronously wake the receiver. Neither wakeups nor
        // callback/sink capture destruction may reenter a held state lock.
        drop(reporter);
        if shared.closed.load(Ordering::Acquire) {
            drop(sender);
            return Err(closed());
        }
        sender.send(result).map_err(|_| closed())
    }
}

struct Owner(Arc<Shared>);
impl Owner {
    fn new(
        reporter: LocalToolReporter,
    ) -> (Self, LocalToolResponder, oneshot::Receiver<FinalCompletion>) {
        let (sender, receiver) = oneshot::channel();
        let shared = Arc::new(Shared {
            closed: AtomicBool::new(false),
            state: Mutex::new(State {
                reporter: Some(reporter),
                sender: Some(sender),
                final_claimed: false,
            }),
        });
        let responder = LocalToolResponder(Arc::downgrade(&shared));
        (Self(shared), responder, receiver)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.0.closed.store(true, Ordering::Release);
        let retired = match self.0.state.try_lock() {
            Ok(mut state) => Some((state.sender.take(), state.reporter.take())),
            Err(TryLockError::Poisoned(error)) => {
                let mut state = error.into_inner();
                Some((state.sender.take(), state.reporter.take()))
            }
            Err(TryLockError::WouldBlock) => None,
        };
        // An in-flight operation owns the last strong Shared when this lock is
        // busy, and releases its state naturally after observing the closed bit.
        drop(retired);
    }
}

impl LocalToolFunction {
    /// Declare a deferred dispatcher using the owner's weak result endpoint.
    /// Deferred native registration requires a finite effective deadline and
    /// cancel_on_interruption=false. Standalone calls still need a reporting owner.
    pub fn new_deferred<F, Fut>(dispatcher: F) -> Self
    where
        F: Fn(Value, LocalToolResponder) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ProcessorError>> + Send + 'static,
    {
        let dispatcher = Arc::new(dispatcher);
        Self(
            Arc::new(ToolFunctionState {
                handler: Arc::new(move |arguments, reporter| -> ToolFuture {
                    let Some(reporter) = reporter else {
                        return Box::pin(async {
                            Some(Err(ProcessorError::new(MISSING_REPORTER)))
                        });
                    };
                    let dispatcher = dispatcher.clone();
                    Box::pin(async move {
                        let (owner, responder, receiver) = Owner::new(reporter);
                        let dispatched = dispatcher(arguments, responder).await;
                        let completion = match dispatched {
                            Err(error) => Some(Err(error)),
                            Ok(()) => receiver.await.unwrap_or_else(|_| {
                                Some(Err(ProcessorError::new(
                                    "deferred tool final handoff disconnected",
                                )))
                            }),
                        };
                        drop(owner);
                        completion
                    })
                }),
                requires_reporter: true,
                requires_deferred: true,
                options: LocalToolOptions {
                    cancel_on_interruption: false,
                    ..LocalToolOptions::default()
                },
                cleanup: None,
            }),
            LocalToolFunctionIdentity(Arc::new(())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalAsyncCallback;
    use serde_json::json;
    use std::{
        pin::Pin,
        sync::atomic::AtomicUsize,
        task::{Context, Waker},
        time::Duration,
    };

    fn assert_pending(future: Pin<&mut impl Future>) {
        assert!(future
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
    }
    fn reporter() -> LocalToolReporter {
        LocalToolReporter::new_sink(|_| Ok(()))
    }

    #[tokio::test]
    async fn deferred_contract_preserves_identity_and_fails_before_dispatch_without_owner() {
        let effects = Arc::new(AtomicUsize::new(0));
        let captured = effects.clone();
        let handler = LocalToolFunction::new_deferred(move |_, _| {
            captured.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        });
        assert!(handler.requires_deferred());
        assert!(handler.requires_reporter());
        assert!(!handler.options().cancel_on_interruption);
        assert_eq!(handler.options().timeout, None);
        assert_eq!(handler, handler.clone());
        assert_eq!(
            handler.call_completion_cancellable(Value::Null).await,
            Some(Err(ProcessorError::new(MISSING_REPORTER)))
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        assert!(handler.with_options(LocalToolOptions::default()).is_err());
        let cleanup = LocalAsyncCallback::new(|| async { Ok(()) });
        let configured = handler
            .with_options(LocalToolOptions {
                cancel_on_interruption: false,
                timeout: Some(Duration::from_secs(1)),
            })
            .unwrap()
            .with_cleanup(cleanup.clone());
        assert_ne!(handler, configured);
        assert_eq!(configured, configured.clone());
        assert!(configured.requires_deferred());
        assert_eq!(configured.cleanup_callback(), Some(cleanup));
        for ordinary in [
            LocalToolFunction::new(|value| async move { Ok(value) }),
            LocalToolFunction::new_cancellable(|value| async move { Some(Ok(value)) }),
            LocalToolFunction::new_with_completion(|value| async move {
                Ok(LocalToolCompletion::new(LocalToolResult::new(value)))
            }),
            LocalToolFunction::new_with_reporter(|value, _| async move {
                Ok(LocalToolResult::new(value))
            }),
        ] {
            assert!(!ordinary.requires_deferred());
            assert!(!ordinary
                .with_cleanup(LocalAsyncCallback::new(|| async { Ok(()) }))
                .requires_deferred());
        }
    }

    #[tokio::test]
    async fn early_final_waits_for_ack_and_dispatch_error_or_panic_discards_its_hook() {
        // Exercise both a dispatch-future panic and a synchronous dispatcher panic.
        for mode in 0..4 {
            let resource = Arc::new(());
            let weak = Arc::downgrade(&resource);
            let invoked = Arc::new(AtomicUsize::new(0));
            let count = invoked.clone();
            let hook = LocalAsyncCallback::new(move || {
                let resource = resource.clone();
                let count = count.clone();
                async move {
                    drop(resource);
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            });
            let hooks = Arc::new(Mutex::new(Some(hook)));
            let (acknowledge, ack) = oneshot::channel();
            let acks = Arc::new(Mutex::new(Some(ack)));
            let handler = LocalToolFunction::new_deferred(move |_, responder| {
                let hook = hooks.lock().unwrap().take().unwrap();
                responder
                    .finish(Ok(LocalToolCompletion::new(
                        LocalToolResult::new(json!("done")).with_run_llm(false),
                    )
                    .with_context_updated(hook)))
                    .unwrap();
                if mode == 3 {
                    panic!("synchronous dispatcher panic after early final");
                }
                let ack = acks.lock().unwrap().take().unwrap();
                async move {
                    ack.await.unwrap();
                    match mode {
                        0 => Ok(()),
                        1 => Err(ProcessorError::new("dispatch failed")),
                        _ => panic!("dispatch future panic after early final"),
                    }
                }
            });
            let mut future = Box::pin(async move {
                handler
                    .call_completion_with_reporter(Value::Null, Some(reporter()))
                    .await
            });
            if mode == 3 {
                assert!(tokio::spawn(future).await.unwrap_err().is_panic());
            } else {
                assert_pending(future.as_mut());
                assert!(weak.upgrade().is_some(), "queued final lost its hook");
                acknowledge.send(()).unwrap();
                if mode == 2 {
                    assert!(tokio::spawn(future).await.unwrap_err().is_panic());
                } else {
                    let completion = future.await.unwrap();
                    if mode == 0 {
                        let completion = completion.unwrap();
                        assert_eq!(
                            completion.result,
                            LocalToolResult::new(json!("done")).with_run_llm(false)
                        );
                        assert!(completion.on_context_updated.is_some());
                        assert!(weak.upgrade().is_some());
                        drop(completion);
                    } else {
                        assert_eq!(completion, Err(ProcessorError::new("dispatch failed")));
                    }
                }
            }
            assert!(weak.upgrade().is_none(), "queued hook capture leaked");
            assert_eq!(invoked.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn owner_drop_closes_weak_endpoint_and_cancel_error_and_null_stay_distinct() {
        let endpoints = Arc::new(Mutex::new(None));
        let captured = endpoints.clone();
        let handler = LocalToolFunction::new_deferred(move |_, responder| {
            *captured.lock().unwrap() = Some(responder);
            async { Ok(()) }
        });
        let resource = Arc::new(());
        let weak_resource = Arc::downgrade(&resource);
        let sink = LocalToolReporter::new_sink(move |_| {
            let _retained = &resource;
            Ok(())
        });
        let mut future = Box::pin(handler.call_completion_with_reporter(Value::Null, Some(sink)));
        assert_pending(future.as_mut());
        let responder = endpoints.lock().unwrap().take().unwrap();
        let weak_owner = responder.0.clone();
        assert!(weak_owner.upgrade().is_some());
        assert!(weak_resource.upgrade().is_some());
        drop(future);
        assert!(weak_owner.upgrade().is_none());
        assert!(weak_resource.upgrade().is_none());
        assert_eq!(responder.cancel(), Err(closed()));
        assert_eq!(
            responder.finish(Ok(LocalToolCompletion::new(LocalToolResult::new(
                Value::Null
            )))),
            Err(closed())
        );
        assert_eq!(
            responder.report(LocalToolResult::new(Value::Null)).await,
            Err(closed())
        );
        for mode in 0..3 {
            let handler = LocalToolFunction::new_deferred(move |_, responder| {
                match mode {
                    0 => responder.cancel().unwrap(),
                    1 => responder
                        .finish(Err(ProcessorError::new(
                            "direct tool invocation was cancelled",
                        )))
                        .unwrap(),
                    _ => responder
                        .finish(Ok(LocalToolCompletion::new(LocalToolResult::new(
                            Value::Null,
                        ))))
                        .unwrap(),
                }
                async { Ok(()) }
            });
            let actual = handler
                .call_completion_with_reporter(Value::Null, Some(reporter()))
                .await;
            let expected = match mode {
                0 => None,
                1 => Some(Err(ProcessorError::new(
                    "direct tool invocation was cancelled",
                ))),
                _ => Some(Ok(LocalToolCompletion::new(LocalToolResult::new(
                    Value::Null,
                )))),
            };
            assert_eq!(actual, expected);
        }
    }

    #[tokio::test]
    async fn reentrant_report_is_busy_and_invalid_final_budget_preserves_one_valid_handoff() {
        let endpoints = Arc::new(Mutex::new(None::<LocalToolResponder>));
        let captured = endpoints.clone();
        let handler = LocalToolFunction::new_deferred(move |_, responder| {
            *captured.lock().unwrap() = Some(responder);
            async { Ok(()) }
        });
        let sink_endpoints = endpoints.clone();
        let reports = Arc::new(AtomicUsize::new(0));
        let count = reports.clone();
        let sink = LocalToolReporter::new_sink(move |_| {
            let responder = sink_endpoints.lock().unwrap().as_ref().unwrap().clone();
            assert_eq!(
                responder.cancel().unwrap_err().message,
                "deferred tool responder is busy"
            );
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let mut future = Box::pin(handler.call_completion_with_reporter(Value::Null, Some(sink)));
        assert_pending(future.as_mut());
        let responder = endpoints.lock().unwrap().as_ref().unwrap().clone();
        responder
            .report(LocalToolResult::new(json!("working")))
            .await
            .unwrap();
        assert_eq!(reports.load(Ordering::SeqCst), 1);
        assert!(responder
            .finish(Ok(LocalToolCompletion::new(LocalToolResult::new(
                Value::String("x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES),)
            ))))
            .is_err());
        assert!(responder
            .finish(Err(ProcessorError::new(
                "x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES / 2),
            )))
            .is_err());
        assert_pending(future.as_mut());
        let completion =
            LocalToolCompletion::new(LocalToolResult::new(json!("done")).with_run_llm(false));
        responder.finish(Ok(completion.clone())).unwrap();
        assert_eq!(responder.finish(Ok(completion.clone())), Err(closed()));
        assert_eq!(
            responder.report(LocalToolResult::new(json!("late"))).await,
            Err(closed())
        );
        assert_eq!(future.await, Some(Ok(completion)));
        assert_eq!(responder.cancel(), Err(closed()));
    }
}
