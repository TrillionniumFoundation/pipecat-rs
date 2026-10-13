use super::{LocalAsyncCallback, LocalToolFunction, LocalToolFunctionIdentity};
use crate::ProcessorError;
use serde_json::Value;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

/// Options carried by a local handler. An absent deadline inherits the owning
/// binding's default; when both are absent, the call has no execution deadline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalToolOptions {
    pub cancel_on_interruption: bool,
    pub timeout: Option<Duration>,
}
impl Default for LocalToolOptions {
    fn default() -> Self {
        Self {
            cancel_on_interruption: true,
            timeout: None,
        }
    }
}
type ToolFuture = Pin<Box<dyn Future<Output = Option<Result<Value, ProcessorError>>> + Send>>;
pub(super) struct ToolFunctionState {
    handler: Arc<dyn Fn(Value) -> ToolFuture + Send + Sync>,
    options: LocalToolOptions,
    cleanup: Option<LocalAsyncCallback>,
}
impl LocalToolFunction {
    /// Construct an ordinary result handler with default call options.
    pub fn new<F, Fut>(handler: F) -> Self
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value, ProcessorError>> + Send + 'static,
    {
        Self::new_cancellable(move |arguments| {
            let future = handler(arguments);
            async move { Some(future.await) }
        })
    }
    /// Construct a handler whose `None` acknowledges cancellation. `Some(Ok(null))`
    /// is a successful null result, and `Some(Err(error))` remains a failure.
    pub fn new_cancellable<F, Fut>(handler: F) -> Self
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<Result<Value, ProcessorError>>> + Send + 'static,
    {
        Self(
            Arc::new(ToolFunctionState {
                handler: Arc::new(move |arguments| Box::pin(handler(arguments))),
                options: LocalToolOptions::default(),
                cleanup: None,
            }),
            LocalToolFunctionIdentity(Arc::new(())),
        )
    }
    /// Invoke with typed cancellation. Owning executors must use this entrypoint
    /// instead of interpreting an error message as a cancellation acknowledgment.
    pub async fn call_cancellable(
        &self,
        arguments: Value,
    ) -> Option<Result<Value, ProcessorError>> {
        (self.0.handler)(arguments).await
    }
    /// Compatibility invocation returning an ordinary result. Typed cancellation
    /// is represented as an error here because this return type cannot carry it.
    pub async fn call(&self, arguments: Value) -> Result<Value, ProcessorError> {
        self.call_cancellable(arguments)
            .await
            .unwrap_or_else(|| Err(ProcessorError::new("direct tool invocation was cancelled")))
    }
    pub fn options(&self) -> LocalToolOptions {
        self.0.options
    }
    /// Clone the teardown hook without executing it. The pipeline owner records
    /// its identity and awaits it even when this handler is later unregistered.
    pub fn cleanup_callback(&self) -> Option<LocalAsyncCallback> {
        self.0.cleanup.clone()
    }
    /// Return a new handler contract. Clones preserve identity; changing options
    /// creates a fresh contract identity, including the tiny realtime seal token.
    pub fn with_options(&self, options: LocalToolOptions) -> Result<Self, ProcessorError> {
        if options.timeout.is_some_and(|timeout| timeout.is_zero()) {
            return Err(ProcessorError::new("direct tool timeout must be positive"));
        }
        Ok(self.configured(options, self.0.cleanup.clone()))
    }
    /// Return a new contract carrying an owned teardown hook. Resource sharing
    /// should use clones of the same callback so an owner can deduplicate it.
    pub fn with_cleanup(&self, cleanup: LocalAsyncCallback) -> Self {
        self.configured(self.0.options, Some(cleanup))
    }
    fn configured(&self, options: LocalToolOptions, cleanup: Option<LocalAsyncCallback>) -> Self {
        Self(
            Arc::new(ToolFunctionState {
                handler: self.0.handler.clone(),
                options,
                cleanup,
            }),
            LocalToolFunctionIdentity(Arc::new(())),
        )
    }
}
