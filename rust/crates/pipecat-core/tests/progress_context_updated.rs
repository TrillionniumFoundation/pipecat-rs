//! Local progress callback seals and ownership; explicit invocation is not a context acknowledgment.
use pipecat_core::*;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};

#[derive(Default)]
struct Capture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "progress-hook-publication"
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
async fn receive(worker: &mut PipelineWorker) -> Frame {
    loop {
        let frame = worker.recv().await.unwrap().frame;
        if matches!(frame.data, FrameData::FunctionCallResult(_)) {
            return frame;
        }
    }
}
fn owned_callback() -> (LocalAsyncCallback, Weak<()>) {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let callback = LocalAsyncCallback::new(move || {
        let resource = resource.clone();
        async move {
            drop(resource);
            panic!("publication or validation must not execute a hook");
        }
    });
    (callback, weak)
}

#[tokio::test]
async fn progress_hooks_remain_sealed_and_once_per_publication_without_receipt_retention() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let capture = Arc::new(Capture::default());
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(Default::default())
            .await
            .unwrap();
        let ctx = capture.0.lock().unwrap().take().unwrap();
        let call = Arc::new(FunctionCall {
            function_name: "echo".into(),
            tool_call_id: "progress".into(),
            arguments: json!({"value":1}),
        });
        let batch = NativeToolCallBatch::new("group".into(), vec![(call.clone(), false)]).unwrap();
        let publisher = ctx
            .tool_outcome_publisher(call, false)
            .unwrap()
            .with_batch(batch)
            .unwrap();
        let progress = publisher.progress_publisher().unwrap();
        for (value, sequence) in [
            (json!({}), 0),
            (
                json!("x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES)),
                1,
            ),
        ] {
            let (callback, weak) = owned_callback();
            assert!(progress
                .publish_progress_with_context_updated(
                    LocalToolResult::new(value),
                    sequence,
                    Some(callback),
                )
                .is_err());
            assert!(weak.upgrade().is_none(), "invalid report retained its hook");
        }
        let invoked = Arc::new(AtomicUsize::new(0));
        let count = invoked.clone();
        let callback = LocalAsyncCallback::new(move || {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        });
        for sequence in [1, 2] {
            assert_eq!(
                progress
                    .publish_progress_with_context_updated(
                        LocalToolResult::new(json!({"working":true})).with_run_llm(false),
                        sequence,
                        Some(callback.clone()),
                    )
                    .unwrap(),
                ToolOutcomeAdmission::Admitted
            );
        }
        let mut first = receive(&mut worker).await;
        let mut second = receive(&mut worker).await;
        assert!(first.native_tool_outcome().unwrap().is_none());
        assert!(first.native_tool_context_updated().unwrap().is_none());
        let first_progress = first.take_native_tool_progress().unwrap().unwrap();
        let second_progress = second.take_native_tool_progress().unwrap().unwrap();
        assert_eq!(first_progress.sequence(), 1);
        assert_eq!(second_progress.sequence(), 2);
        assert!(first
            .take_native_tool_progress_context_updated(&second_progress)
            .is_err());
        let mut duplicate = first.duplicate();
        assert_ne!(first.id, duplicate.id);
        assert_eq!(
            first.native_tool_progress_context_updated().unwrap(),
            Some(first_progress.id())
        );
        for field in 0..3 {
            let mut changed = first.clone();
            let FrameData::FunctionCallResult(result) = &mut changed.data else {
                unreachable!()
            };
            match field {
                0 => result.result = json!("forged"),
                1 => result.run_llm = Some(true),
                _ => result.is_final = true,
            }
            assert!(changed.native_tool_progress_context_updated().is_err());
            assert!(changed
                .take_native_tool_progress_context_updated(&first_progress)
                .is_err());
        }
        assert!(serde_json::to_value(&first).is_err());
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        let taken = first
            .take_native_tool_progress_context_updated(&first_progress)
            .unwrap()
            .unwrap();
        assert!(
            duplicate.require_no_native_llm_continuation().is_err(),
            "empty shared hook carrier remains local"
        );
        assert!(duplicate
            .take_native_tool_progress_context_updated(&first_progress)
            .unwrap()
            .is_none());
        let decoded: Frame =
            serde_json::from_value(serde_json::to_value(&duplicate).unwrap()).unwrap();
        assert!(decoded
            .native_tool_progress_context_updated()
            .unwrap()
            .is_none());
        taken.call().await.unwrap();
        second
            .take_native_tool_progress_context_updated(&second_progress)
            .unwrap()
            .unwrap()
            .call()
            .await
            .unwrap();
        assert_eq!(
            invoked.load(Ordering::SeqCst),
            2,
            "same callback must remain separate for two progress publications"
        );
        let (callback, weak) = owned_callback();
        progress
            .publish_progress_with_context_updated(
                LocalToolResult::new(json!("held")),
                3,
                Some(callback),
            )
            .unwrap();
        let mut held = receive(&mut worker).await;
        let retained_progress = held.take_native_tool_progress().unwrap().unwrap();
        assert!(
            weak.upgrade().is_some(),
            "unconsumed frame owns its callback"
        );
        drop(held);
        assert!(
            weak.upgrade().is_none(),
            "retained progress receipt must not retain callback captures"
        );
        assert_eq!(retained_progress.sequence(), 3);
        publisher.cancel(
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::ExplicitCancel,
            false,
        );
        worker.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let (callback, weak) = owned_callback();
        assert_eq!(
            progress
                .publish_progress_with_context_updated(
                    LocalToolResult::new(json!({})),
                    4,
                    Some(callback)
                )
                .unwrap(),
            ToolOutcomeAdmission::Closed
        );
        assert!(
            weak.upgrade().is_none(),
            "closed publication retained its hook"
        );
    })
    .await
    .unwrap();
}
