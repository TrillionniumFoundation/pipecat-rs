use pipecat_core::{LocalToolFunction, LocalToolResult, ProcessorError};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[tokio::test]
async fn result_policy_survives_typed_invocation_and_legacy_calls_return_only_the_value() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let handler = LocalToolFunction::new_with_result(move |arguments| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(LocalToolResult::new(arguments["value"].clone())
                .with_run_llm(arguments["run_llm"].as_bool().unwrap()))
        }
    });
    for (value, run_llm) in [(Value::Null, false), (json!({"answer": 42}), true)] {
        let arguments = json!({"value": value, "run_llm": run_llm});
        assert_eq!(
            handler.call_result_cancellable(arguments.clone()).await,
            Some(Ok(LocalToolResult {
                value: value.clone(),
                run_llm: Some(run_llm),
            }))
        );
        assert_eq!(
            handler.call_cancellable(arguments.clone()).await,
            Some(Ok(value.clone()))
        );
        assert_eq!(handler.call(arguments).await.unwrap(), value);
    }
    assert_eq!(calls.load(Ordering::SeqCst), 6);
}

#[tokio::test]
async fn legacy_constructors_normalize_success_without_a_continuation_override() {
    let ordinary = LocalToolFunction::new(|value| async { Ok(value) });
    let cancellable = LocalToolFunction::new_cancellable(|value| async {
        if value == json!("cancel") {
            None
        } else {
            Some(Ok(value))
        }
    });
    for handler in [ordinary, cancellable.clone()] {
        for value in [Value::Null, json!({"nested": [1, true, null]})] {
            assert_eq!(
                handler.call_result_cancellable(value.clone()).await,
                Some(Ok(LocalToolResult {
                    value,
                    run_llm: None,
                }))
            );
        }
    }
    assert_eq!(
        cancellable.call_result_cancellable(json!("cancel")).await,
        None
    );
}

#[tokio::test]
async fn typed_cancellation_error_and_successful_null_remain_distinct() {
    let handler = LocalToolFunction::new_cancellable_with_result(|argument| async move {
        match argument.as_str() {
            Some("cancel") => None,
            Some("error") => Some(Err(ProcessorError::fatal(
                "direct tool invocation was cancelled",
            )
            .with_permanence_override(false))),
            _ => Some(Ok(LocalToolResult::new(Value::Null).with_run_llm(false))),
        }
    });
    assert_eq!(handler.call_result_cancellable(json!("cancel")).await, None);
    assert_eq!(handler.call_cancellable(json!("cancel")).await, None);
    assert_eq!(
        handler.call(json!("cancel")).await.unwrap_err(),
        ProcessorError::new("direct tool invocation was cancelled")
    );
    let error = ProcessorError::fatal("direct tool invocation was cancelled")
        .with_permanence_override(false);
    assert_eq!(
        handler.call_result_cancellable(json!("error")).await,
        Some(Err(error.clone()))
    );
    assert_eq!(
        handler.call_cancellable(json!("error")).await,
        Some(Err(error.clone()))
    );
    assert_eq!(handler.call(json!("error")).await.unwrap_err(), error);
    assert_eq!(
        handler.call_result_cancellable(Value::Null).await,
        Some(Ok(LocalToolResult {
            value: Value::Null,
            run_llm: Some(false),
        }))
    );
    assert_eq!(
        handler.call_cancellable(Value::Null).await,
        Some(Ok(Value::Null))
    );
}
