use super::{LocalAsyncCallback, LocalToolFunction, LocalToolFunctionIdentity};
use crate::ProcessorError;
use serde_json::Value;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

#[path = "local_tool_function/deferred.rs"]
mod deferred;
pub use deferred::LocalToolResponder;

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

/// A successful result and an optional callback for its context consumer.
/// Invoking a local handler alone never runs this callback.
#[derive(Debug, Clone, PartialEq)]
pub struct LocalToolCompletion {
    pub result: LocalToolResult,
    pub on_context_updated: Option<LocalAsyncCallback>,
}
impl LocalToolCompletion {
    pub fn new(result: LocalToolResult) -> Self {
        Self {
            result,
            on_context_updated: None,
        }
    }
    pub fn with_context_updated(mut self, callback: LocalAsyncCallback) -> Self {
        self.on_context_updated = Some(callback);
        self
    }
}

/// A local reporting endpoint supplied by an admitted tool owner.
/// Clones share the same synchronous sink; this type carries no wire capability.
#[derive(Clone)]
pub struct LocalToolReporter(
    Arc<dyn Fn(LocalToolCompletion) -> Result<(), ProcessorError> + Send + Sync>,
);
impl LocalToolReporter {
    /// Construct a trusted local sink. The callback must perform bounded admission
    /// without awaiting, spawning work, or retaining the owning runtime strongly.
    /// The runtime owns queue limits and the endpoint's active/closed lifecycle.
    pub fn new_sink<F>(sink: F) -> Self
    where
        F: Fn(LocalToolResult) -> Result<(), ProcessorError> + Send + Sync + 'static,
    {
        Self::new_completion_sink(move |completion| sink(completion.result))
    }
    /// Construct a trusted local sink retaining a report's context-update hook.
    /// As with `new_sink`, admission is synchronous and bounded. Compatibility
    /// result sinks discard hooks without invoking them.
    pub fn new_completion_sink<F>(sink: F) -> Self
    where
        F: Fn(LocalToolCompletion) -> Result<(), ProcessorError> + Send + Sync + 'static,
    {
        Self(Arc::new(sink))
    }
    /// Acknowledge admission by the sink. Success does not mean that a context
    /// consumer has applied the report. This method adds no queue or background task.
    pub async fn report(&self, result: LocalToolResult) -> Result<(), ProcessorError> {
        self.report_with_context_updated(LocalToolCompletion::new(result))
            .await
    }
    /// Acknowledge admission of a result and hook without executing that hook.
    /// Only the successful context consumer owns context-update notification.
    pub async fn report_with_context_updated(
        &self,
        completion: LocalToolCompletion,
    ) -> Result<(), ProcessorError> {
        (self.0)(completion)
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
    Pin<Box<dyn Future<Output = Option<Result<LocalToolCompletion, ProcessorError>>> + Send>>;
pub(super) struct ToolFunctionState {
    handler: Arc<dyn Fn(Value, Option<LocalToolReporter>) -> ToolFuture + Send + Sync>,
    requires_reporter: bool,
    requires_deferred: bool,
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
                handler: Arc::new(move |arguments, _reporter| {
                    let future = handler(arguments);
                    Box::pin(async move {
                        future
                            .await
                            .map(|result| result.map(LocalToolCompletion::new))
                    })
                }),
                requires_reporter: false,
                requires_deferred: false,
                options: LocalToolOptions::default(),
                cleanup: None,
            }),
            LocalToolFunctionIdentity(Arc::new(())),
        )
    }
    /// Construct a final handler retaining a callback for the owning context consumer.
    /// Neither standalone invocation nor a compatibility result call runs the hook.
    pub fn new_with_completion<F, Fut>(handler: F) -> Self
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<LocalToolCompletion, ProcessorError>> + Send + 'static,
    {
        Self(
            Arc::new(ToolFunctionState {
                handler: Arc::new(move |arguments, _reporter| {
                    let future = handler(arguments);
                    Box::pin(async move { Some(future.await) })
                }),
                requires_reporter: false,
                requires_deferred: false,
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
                    Box::pin(async move { Some(future.await.map(LocalToolCompletion::new)) })
                }),
                requires_reporter: true,
                requires_deferred: false,
                options: LocalToolOptions {
                    cancel_on_interruption: false,
                    ..LocalToolOptions::default()
                },
                cleanup: None,
            }),
            LocalToolFunctionIdentity(Arc::new(())),
        )
    }
    /// Invoke with the owner's reporting endpoint, retaining final result and hook.
    /// Missing required endpoints fail before constructing the user's handler future.
    /// Ordinary handlers ignore a supplied endpoint. This method never runs the hook.
    pub async fn call_completion_with_reporter(
        &self,
        arguments: Value,
        reporter: Option<LocalToolReporter>,
    ) -> Option<Result<LocalToolCompletion, ProcessorError>> {
        if self.requires_reporter() && reporter.is_none() {
            return Some(Err(ProcessorError::new(MISSING_REPORTER)));
        }
        (self.0.handler)(arguments, reporter).await
    }
    /// Invoke without a reporter while retaining completion metadata and cancellation.
    /// Reporting handlers require `call_completion_with_reporter` and an endpoint.
    pub async fn call_completion_cancellable(
        &self,
        arguments: Value,
    ) -> Option<Result<LocalToolCompletion, ProcessorError>> {
        self.call_completion_with_reporter(arguments, None).await
    }
    /// Compatibility invocation retaining result policy but discarding its hook.
    /// This method never runs a discarded context-update callback.
    pub async fn call_result_with_reporter(
        &self,
        arguments: Value,
        reporter: Option<LocalToolReporter>,
    ) -> Option<Result<LocalToolResult, ProcessorError>> {
        self.call_completion_with_reporter(arguments, reporter)
            .await
            .map(|result| result.map(|completion| completion.result))
    }
    /// Invoke with typed cancellation and preserve successful result policy.
    /// Context-update hooks are discarded and never invoked by this method.
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
    /// Whether a native owner must retain the dispatch and final-result wait
    /// under one finite execution deadline.
    pub fn requires_deferred(&self) -> bool {
        self.0.requires_deferred
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
                requires_deferred: self.0.requires_deferred,
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
    async fn completion_sink_retains_report_hook_and_legacy_report_defaults_to_no_hook() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let captured = received.clone();
        let reporter = LocalToolReporter::new_completion_sink(move |completion| {
            captured.lock().unwrap().push(completion);
            Ok(())
        });
        let invoked = Arc::new(AtomicUsize::new(0));
        let count = invoked.clone();
        let callback = LocalAsyncCallback::new(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        let result = LocalToolResult::new(Value::Null).with_run_llm(false);
        reporter.report(result.clone()).await.unwrap();
        reporter
            .clone()
            .report_with_context_updated(
                LocalToolCompletion::new(result.clone()).with_context_updated(callback.clone()),
            )
            .await
            .unwrap();
        assert_eq!(
            *received.lock().unwrap(),
            vec![
                LocalToolCompletion::new(result.clone()),
                LocalToolCompletion::new(result).with_context_updated(callback),
            ]
        );
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn result_sink_discards_report_hook_without_invoking_or_retaining_it() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let captured = received.clone();
        let reporter = LocalToolReporter::new_sink(move |result| {
            captured.lock().unwrap().push(result);
            Ok(())
        });
        let resource = Arc::new(());
        let weak = Arc::downgrade(&resource);
        let callback = LocalAsyncCallback::new(move || {
            let resource = resource.clone();
            async move {
                drop(resource);
                panic!("compatibility sink must not execute the hook");
            }
        });
        let result = LocalToolResult::new(json!("progress")).with_run_llm(false);
        reporter
            .report_with_context_updated(
                LocalToolCompletion::new(result.clone()).with_context_updated(callback),
            )
            .await
            .unwrap();
        assert_eq!(*received.lock().unwrap(), vec![result]);
        assert!(
            weak.upgrade().is_none(),
            "legacy sink retained a discarded hook"
        );
    }

    #[tokio::test]
    async fn completion_retains_hook_but_compatibility_invocations_never_run_it() {
        let hooks = Arc::new(AtomicUsize::new(0));
        let captured = hooks.clone();
        let hook = LocalAsyncCallback::new(move || {
            let captured = captured.clone();
            async move {
                captured.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        let callback = hook.clone();
        let handler = LocalToolFunction::new_with_completion(move |arguments| {
            let callback = callback.clone();
            async move {
                Ok(
                    LocalToolCompletion::new(LocalToolResult::new(arguments).with_run_llm(false))
                        .with_context_updated(callback),
                )
            }
        });
        let configured = handler
            .with_options(LocalToolOptions::default())
            .unwrap()
            .with_cleanup(LocalAsyncCallback::new(|| async { Ok(()) }));
        let completion = configured
            .call_completion_cancellable(Value::Null)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            completion.result,
            LocalToolResult::new(Value::Null).with_run_llm(false)
        );
        assert_eq!(completion.on_context_updated, Some(hook));
        assert_eq!(
            configured.call_result_cancellable(Value::Null).await,
            Some(Ok(LocalToolResult::new(Value::Null).with_run_llm(false)))
        );
        assert_eq!(
            configured.call_cancellable(Value::Null).await,
            Some(Ok(Value::Null))
        );
        assert_eq!(configured.call(Value::Null).await.unwrap(), Value::Null);
        assert_eq!(hooks.load(Ordering::SeqCst), 0);
        completion.on_context_updated.unwrap().call().await.unwrap();
        assert_eq!(hooks.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn completion_normalizes_legacy_results_and_preserves_reporter_preconditions() {
        for handler in [
            LocalToolFunction::new(|value| async move { Ok(value) }),
            LocalToolFunction::new_with_result(
                |value| async move { Ok(LocalToolResult::new(value)) },
            ),
        ] {
            assert_eq!(
                handler.call_completion_cancellable(Value::Null).await,
                Some(Ok(LocalToolCompletion::new(LocalToolResult::new(
                    Value::Null
                ))))
            );
        }
        let cancelled = LocalToolFunction::new_cancellable(|_| async { None });
        assert_eq!(
            cancelled.call_completion_cancellable(Value::Null).await,
            None
        );
        let failed = LocalToolFunction::new_with_completion(|_| async {
            Err(ProcessorError::fatal("callback failure"))
        });
        assert_eq!(
            failed.call_completion_cancellable(Value::Null).await,
            Some(Err(ProcessorError::fatal("callback failure")))
        );
        let effects = Arc::new(AtomicUsize::new(0));
        let captured = effects.clone();
        let reporting = LocalToolFunction::new_with_reporter(move |arguments, _reporter| {
            captured.fetch_add(1, Ordering::SeqCst);
            async move { Ok(LocalToolResult::new(arguments).with_run_llm(false)) }
        });
        assert_eq!(
            reporting
                .call_completion_with_reporter(Value::Null, None)
                .await,
            Some(Err(ProcessorError::new(MISSING_REPORTER)))
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        let reporter = LocalToolReporter::new_sink(|_| Ok(()));
        assert_eq!(
            reporting
                .call_completion_with_reporter(json!(7), Some(reporter))
                .await,
            Some(Ok(LocalToolCompletion::new(
                LocalToolResult::new(json!(7)).with_run_llm(false)
            )))
        );
        assert_eq!(effects.load(Ordering::SeqCst), 1);
    }

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
