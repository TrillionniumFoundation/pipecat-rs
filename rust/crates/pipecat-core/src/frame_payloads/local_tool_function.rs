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

/// A local reporting endpoint supplied by an admitted tool owner.
/// Clones share the same synchronous sink; this type carries no wire capability.
#[derive(Clone)]
pub struct LocalToolReporter(
    Arc<dyn Fn(LocalToolResult) -> Result<(), ProcessorError> + Send + Sync>,
);
impl LocalToolReporter {
    /// Construct a trusted local sink. The callback must perform bounded admission
    /// without awaiting, spawning work, or retaining the owning runtime strongly.
    /// The runtime owns queue limits and the endpoint's active/closed lifecycle.
    pub fn new_sink<F>(sink: F) -> Self
    where
        F: Fn(LocalToolResult) -> Result<(), ProcessorError> + Send + Sync + 'static,
    {
        Self(Arc::new(sink))
    }
    /// Acknowledge admission by the sink. Success does not mean that a context
    /// consumer has applied the report. This method adds no queue or background task.
    pub async fn report(&self, result: LocalToolResult) -> Result<(), ProcessorError> {
        (self.0)(result)
    }
}

const MISSING_REPORTER: &str = "reporting direct tool requires an admitted reporter endpoint";

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
    handler: Arc<dyn Fn(Value, Option<LocalToolReporter>) -> ToolFuture + Send + Sync>,
    requires_reporter: bool,
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
                handler: Arc::new(move |arguments, _reporter| Box::pin(handler(arguments))),
                requires_reporter: false,
                options: LocalToolOptions::default(),
                cleanup: None,
            }),
            LocalToolFunctionIdentity(Arc::new(())),
        )
    }
    /// Construct a reporting handler whose ordinary return is its final result.
    /// Reporting calls require an admitted endpoint. Native reporting keeps the
    /// call alive across interruption; opting into interruption cancellation is rejected.
    pub fn new_with_reporter<F, Fut>(handler: F) -> Self
    where
        F: Fn(Value, LocalToolReporter) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<LocalToolResult, ProcessorError>> + Send + 'static,
    {
        Self(
            Arc::new(ToolFunctionState {
                handler: Arc::new(move |arguments, reporter| -> ToolFuture {
                    let Some(reporter) = reporter else {
                        return Box::pin(async {
                            Some(Err(ProcessorError::new(MISSING_REPORTER)))
                        });
                    };
                    let future = handler(arguments, reporter);
                    Box::pin(async move { Some(future.await) })
                }),
                requires_reporter: true,
                options: LocalToolOptions {
                    cancel_on_interruption: false,
                    ..LocalToolOptions::default()
                },
                cleanup: None,
            }),
            LocalToolFunctionIdentity(Arc::new(())),
        )
    }
    /// Invoke with the owner's reporting endpoint while preserving final metadata
    /// and typed cancellation. Missing required endpoints fail before constructing
    /// the user's handler future. Ordinary handlers ignore a supplied endpoint.
    pub async fn call_result_with_reporter(
        &self,
        arguments: Value,
        reporter: Option<LocalToolReporter>,
    ) -> Option<Result<LocalToolResult, ProcessorError>> {
        if self.requires_reporter() && reporter.is_none() {
            return Some(Err(ProcessorError::new(MISSING_REPORTER)));
        }
        (self.0.handler)(arguments, reporter).await
    }
    /// Invoke with typed cancellation and preserve successful result metadata.
    /// Reporting handlers require `call_result_with_reporter` and an admitted endpoint.
    pub async fn call_result_cancellable(
        &self,
        arguments: Value,
    ) -> Option<Result<LocalToolResult, ProcessorError>> {
        self.call_result_with_reporter(arguments, None).await
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
    /// Whether invocation requires an endpoint from an admitted reporting owner.
    pub fn requires_reporter(&self) -> bool {
        self.0.requires_reporter
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
        if self.requires_reporter() && options.cancel_on_interruption {
            return Err(ProcessorError::new(
                "reporting direct tools require cancel_on_interruption=false",
            ));
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
                requires_reporter: self.0.requires_reporter,
                options,
                cleanup,
            }),
            LocalToolFunctionIdentity(Arc::new(())),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    #[tokio::test]
    async fn reporting_requires_endpoint_before_constructing_handler() {
        let effects = Arc::new(AtomicUsize::new(0));
        let captured = effects.clone();
        let handler = LocalToolFunction::new_with_reporter(move |arguments, _reporter| {
            captured.fetch_add(1, Ordering::SeqCst);
            async move { Ok(LocalToolResult::new(arguments)) }
        });
        assert!(handler.requires_reporter());
        assert!(!handler.options().cancel_on_interruption);
        assert_eq!(handler.options().timeout, None);
        assert_eq!(
            handler.call(json!(1)).await.unwrap_err().message,
            MISSING_REPORTER
        );
        assert_eq!(
            handler
                .call_cancellable(json!(2))
                .await
                .unwrap()
                .unwrap_err()
                .message,
            MISSING_REPORTER
        );
        assert_eq!(
            handler
                .call_result_cancellable(json!(3))
                .await
                .unwrap()
                .unwrap_err()
                .message,
            MISSING_REPORTER
        );
        assert_eq!(
            handler
                .call_result_with_reporter(json!(4), None)
                .await
                .unwrap()
                .unwrap_err()
                .message,
            MISSING_REPORTER
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn reporting_retains_policies_and_contract_configuration() {
        let reports = Arc::new(Mutex::new(Vec::new()));
        let captured = reports.clone();
        let reporter = LocalToolReporter::new_sink(move |result| {
            captured.lock().unwrap().push(result);
            Ok(())
        });
        let handler = LocalToolFunction::new_with_reporter(|arguments, reporter| async move {
            reporter
                .report(LocalToolResult::new(arguments).with_run_llm(true))
                .await?;
            Ok(LocalToolResult::new(json!("final")).with_run_llm(false))
        });
        assert_eq!(handler, handler.clone());
        let configured = handler
            .with_options(LocalToolOptions {
                cancel_on_interruption: false,
                timeout: Some(Duration::from_secs(1)),
            })
            .unwrap()
            .with_cleanup(LocalAsyncCallback::new(|| async { Ok(()) }));
        assert_ne!(handler, configured);
        assert!(configured.requires_reporter());
        assert!(configured.cleanup_callback().is_some());
        assert_eq!(configured.options().timeout, Some(Duration::from_secs(1)));
        let result = configured
            .call_result_with_reporter(json!("progress"), Some(reporter.clone()))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result,
            LocalToolResult::new(json!("final")).with_run_llm(false)
        );
        assert_eq!(
            *reports.lock().unwrap(),
            vec![LocalToolResult::new(json!("progress")).with_run_llm(true)]
        );
        assert_eq!(
            handler
                .with_options(LocalToolOptions::default())
                .unwrap_err()
                .message,
            "reporting direct tools require cancel_on_interruption=false"
        );
        assert_eq!(
            handler
                .with_options(LocalToolOptions {
                    cancel_on_interruption: false,
                    timeout: Some(Duration::ZERO),
                })
                .unwrap_err()
                .message,
            "direct tool timeout must be positive"
        );
    }

    #[tokio::test]
    async fn ordinary_handlers_ignore_supplied_reporting_endpoint() {
        let reports = Arc::new(AtomicUsize::new(0));
        let captured = reports.clone();
        let reporter = LocalToolReporter::new_sink(move |_| {
            captured.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let handler = LocalToolFunction::new_with_result(|arguments| async move {
            Ok(LocalToolResult::new(arguments).with_run_llm(false))
        });
        assert!(!handler.requires_reporter());
        assert_eq!(handler.options(), LocalToolOptions::default());
        assert_eq!(
            handler
                .call_result_with_reporter(json!(7), Some(reporter))
                .await
                .unwrap()
                .unwrap(),
            LocalToolResult::new(json!(7)).with_run_llm(false)
        );
        assert_eq!(reports.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn reporting_preserves_sink_admission_error() {
        let reporter =
            LocalToolReporter::new_sink(|_| Err(ProcessorError::new("sink admission closed")));
        assert_eq!(
            reporter
                .clone()
                .report(LocalToolResult::new(Value::Null))
                .await
                .unwrap_err()
                .message,
            "sink admission closed"
        );
    }
}
