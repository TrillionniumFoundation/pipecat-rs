//! Public receipt/slot integrity; callback execution here is explicit local invocation.
use pipecat_core::*;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Capture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "callback-publication"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.0.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn callback_binding_remains_sealed_after_outcome_take_and_is_once_per_publication() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let capture = Arc::new(Capture::default());
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(Default::default())
            .await
            .unwrap();
        let ctx = capture.0.lock().unwrap().take().unwrap();
        let invoked = Arc::new(AtomicUsize::new(0));
        let count = invoked.clone();
        let callback = LocalAsyncCallback::new(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        let mut published = vec![];
        for id in ["first", "second"] {
            let call = Arc::new(FunctionCall {
                function_name: "echo".into(),
                tool_call_id: id.into(),
                arguments: json!({"value":id}),
            });
            let batch = NativeToolCallBatch::new(id.into(), vec![(call.clone(), true)]).unwrap();
            let receipt = ctx
                .tool_outcome_publisher(call, true)
                .unwrap()
                .with_batch(batch)
                .unwrap()
                .complete_with_context_updated(
                    Ok(json!({"saved":id})),
                    Some(false),
                    Some(callback.clone()),
                    ToolCallEffectStatus::ExternalEffectsUnknown,
                    ToolCallEndReason::CallbackReturned,
                );
            assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
            published.push(receipt.outcome);
        }
        let mut frames = vec![];
        while frames.len() < 2 {
            let frame = worker.recv().await.unwrap().frame;
            if matches!(frame.data, FrameData::FunctionCallResult(_)) {
                frames.push(frame);
            }
        }
        let mut first = frames.remove(0);
        let mut second = frames.remove(0);
        let first_outcome = first.take_native_tool_outcome().unwrap().unwrap();
        let second_outcome = second.take_native_tool_outcome().unwrap().unwrap();
        assert_ne!(first_outcome.id(), second_outcome.id());
        assert_eq!(
            first.native_tool_context_updated().unwrap(),
            Some(first_outcome.id())
        );
        assert!(
            serde_json::to_value(&first).is_err(),
            "hook carrier remains local after outcome take"
        );
        assert!(first
            .take_native_tool_context_updated(&second_outcome)
            .is_err());
        let mut duplicate = first.duplicate();
        assert_ne!(first.id, duplicate.id);
        for field in 0..4 {
            let mut changed = first.clone();
            let FrameData::FunctionCallResult(result) = &mut changed.data else {
                unreachable!()
            };
            match field {
                0 => result.result = json!({"saved":"forged"}),
                1 => result.run_llm = Some(true),
                2 => result.is_final = false,
                _ => result.error = Some("forged".into()),
            }
            assert!(
                changed.native_tool_context_updated().is_err(),
                "metadata must still validate field {field}"
            );
            assert!(changed
                .take_native_tool_context_updated(&first_outcome)
                .is_err());
        }
        assert_eq!(
            invoked.load(Ordering::SeqCst),
            0,
            "publication and validation must not run the callback"
        );
        let taken = first
            .take_native_tool_context_updated(&first_outcome)
            .unwrap()
            .unwrap();
        assert!(duplicate
            .take_native_tool_context_updated(&first_outcome)
            .unwrap()
            .is_none());
        assert!(first.native_tool_context_updated().unwrap().is_none());
        let wire = serde_json::to_value(&duplicate).unwrap();
        let decoded: Frame = serde_json::from_value(wire).unwrap();
        assert!(decoded.native_tool_context_updated().unwrap().is_none());
        taken.call().await.unwrap();
        second
            .take_native_tool_context_updated(&second_outcome)
            .unwrap()
            .unwrap()
            .call()
            .await
            .unwrap();
        assert_eq!(
            invoked.load(Ordering::SeqCst),
            2,
            "one callback instance must still run separately for two publications"
        );
        assert_eq!(published.len(), 2);
        worker.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    })
    .await
    .unwrap();
}
