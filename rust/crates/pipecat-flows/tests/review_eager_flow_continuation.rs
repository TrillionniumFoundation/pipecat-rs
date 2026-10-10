//! Cancelling after a tool result is ready must also fence later flow actions.
use pipecat_core::*;
use pipecat_flows::*;
use pipecat_turns::SharedContext;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};
struct ResultSink {
    seen: Notify,
    resume: Semaphore,
}
#[async_trait]
impl FlowSink for ResultSink {
    async fn queue_frame(&self, frame: Frame) -> Result<(), FlowError> {
        if matches!(frame.data, FrameData::FunctionCallResultDetailed(_)) {
            self.seen.notify_one();
            self.resume.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn drain(&self) -> Result<(), FlowError> {
        Ok(())
    }
}
struct ArmFlow {
    owner: Arc<SpeculationOwner>,
    flow: FlowProcessor,
}
#[async_trait]
impl FrameProcessor for ArmFlow {
    fn name(&self) -> &str {
        "ReviewArmFlow"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let Some(capability) = frame.native_speculation() {
            assert!(capability.claim_inference());
            assert!(capability.bind_admission(ctx.clone(), ctx.health().generation()));
            assert!(self.owner.confirm());
            assert!(capability.release_after_boundary());
        }
        self.flow.process(frame, direction, ctx).await
    }
}
#[tokio::test]
async fn review_cancel_between_result_and_transition_prevents_later_action_constructor() {
    for cancel in [false, true] {
        let effects = Arc::new(AtomicUsize::new(0));
        let counted = effects.clone();
        let mut registry = HandlerRegistry::new();
        registry
            .register_action(
                "effect",
                ActionHandler::new(move |_, _| {
                    counted.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                }),
            )
            .unwrap();
        let config = FlowConfig::from_json(r#"{"initial_node":"a","nodes":{"a":{"task_messages":[{"role":"developer","content":"first"}],"functions":[{"name":"go","description":"continue","transition_only":true,"transition_to":"b"}],"respond_immediately":false},"b":{"task_messages":[{"role":"developer","content":"second"}],"pre_actions":[{"type":"function","handler":"effect"}],"respond_immediately":false}}}"#).unwrap();
        let sink = Arc::new(ResultSink {
            seen: Notify::new(),
            resume: Semaphore::new(0),
        });
        let manager = FlowManager::new(
            Flow::new(config, registry).unwrap(),
            sink.clone(),
            SharedContext::default(),
        );
        manager.initialize().await.unwrap();
        let owner = Arc::new(SpeculationOwner::new());
        let worker = Pipeline::new(vec![Arc::new(ArmFlow {
            owner: owner.clone(),
            flow: FlowProcessor::new(manager.clone()),
        })])
        .start(Default::default())
        .await
        .unwrap();
        let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
            function_name: "go".into(),
            tool_call_id: "review-go".into(),
            arguments: json!({}),
        }]));
        frame.set_native_speculation(owner.capability()).unwrap();
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), sink.seen.notified())
            .await
            .unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        if cancel {
            owner.cancel();
        }
        sink.resume.add_permits(1);
        worker.end().await.unwrap();
        let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            effects.load(Ordering::SeqCst),
            usize::from(!cancel),
            "tool permission must extend through its queued transition continuation"
        );
        assert_eq!(
            manager.current_node().as_deref(),
            Some(if cancel { "a" } else { "b" })
        );
        manager.cancel();
    }
}
