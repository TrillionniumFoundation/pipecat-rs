use super::{LocalAsyncCallback, LocalToolFunction, LocalToolFunctionIdentity};
use crate::ProcessorError;
use serde_json::Value;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

/// A successful local result and its optional model-continuation policy.
/// An absent policy leaves continuation to the context consumer.
#[derive(Debug, Clone, PartialEq)]
pub struct LocalToolResult {
    pub value: Value,
    pub run_llm: Option<bool>,
}
impl LocalToolResult {
    pub fn new(value: Value) -> Self {
        Self {
            value,
            run_llm: None,
        }
    }
    pub fn with_run_llm(mut self, run_llm: bool) -> Self {
        self.run_llm = Some(run_llm);
        self
    }
}

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
type ToolFuture =
    Pin<Box<dyn Future<Output = Option<Result<LocalToolResult, ProcessorError>>> + Send>>;
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
        Self::new_cancellable_with_result(move |arguments| {
            let future = handler(arguments);
            async move { future.await.map(|result| result.map(LocalToolResult::new)) }
        })
    }
    /// Construct a handler preserving its successful result's continuation policy.
    pub fn new_with_result<F, Fut>(handler: F) -> Self
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<LocalToolResult, ProcessorError>> + Send + 'static,
    {
        Self::new_cancellable_with_result(move |arguments| {
            let future = handler(arguments);
            async move { Some(future.await) }
        })
    }
    /// Construct a result-policy handler whose `None` acknowledges cancellation.
    pub fn new_cancellable_with_result<F, Fut>(handler: F) -> Self
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Option<Result<LocalToolResult, ProcessorError>>> + Send + 'static,
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
    /// Invoke with typed cancellation and preserve successful result metadata.
    pub async fn call_result_cancellable(
        &self,
        arguments: Value,
    ) -> Option<Result<LocalToolResult, ProcessorError>> {
        (self.0.handler)(arguments).await
    }
    /// Compatibility invocation with typed cancellation and an ordinary value.
    /// Result metadata is discarded; owners that honor it use `call_result_cancellable`.
    pub async fn call_cancellable(
        &self,
        arguments: Value,
    ) -> Option<Result<Value, ProcessorError>> {
        self.call_result_cancellable(arguments)
            .await
            .map(|result| result.map(|result| result.value))
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
