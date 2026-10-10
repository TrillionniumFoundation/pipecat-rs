//! Health invalidation must wake an idle, already-admitted speculative callback.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
struct Arm {
    owner: Arc<SpeculationOwner>,
    tools: ToolExecutor,
    health: ProcessorHealth,
}
#[async_trait]
impl FrameProcessor for Arm {
    fn name(&self) -> &str {
        "ReviewHealthArm"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
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
        self.tools.process(frame, direction, ctx).await
    }
}
#[tokio::test]
async fn review_source_health_aba_wakes_and_drops_idle_effect_without_owner_cancel() {
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let signal = entered.clone();
    let count = dropped.clone();
    let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        let lease = DropCount(count.clone());
        signal.notify_one();
        Box::pin(async move {
            let _lease = lease;
            std::future::pending().await
        })
    }));
    tool.cancel_on_interruption = false;
    tools.register("idle", tool).unwrap();
    let owner = Arc::new(SpeculationOwner::new());
    let health = ProcessorHealth::default();
    let worker = Pipeline::new(vec![Arc::new(Arm {
        owner: owner.clone(),
        tools,
        health: health.clone(),
    })])
    .start(Default::default())
    .await
    .unwrap();
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
        function_name: "idle".into(),
        tool_call_id: "review-idle".into(),
        arguments: json!({}),
    }]));
    frame.set_native_speculation(owner.capability()).unwrap();
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    health.set_usable(false);
    health.set_usable(true);
    tokio::time::timeout(Duration::from_secs(1), async {
        while dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("health generation change must wake a stalled speculative effect");
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
