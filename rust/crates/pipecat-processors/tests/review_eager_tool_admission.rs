//! Independent eager-effect admission probes. Callback construction is an effect.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

fn declaration(owner: &SpeculationOwner) -> Frame {
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
        function_name: "effect".into(),
        tool_call_id: "review-effect".into(),
        arguments: json!({}),
    }]));
    frame.set_native_speculation(owner.capability()).unwrap();
    frame
}
fn executor(effects: Arc<AtomicUsize>) -> ToolExecutor {
    let mut executor = ToolExecutor::new(1, 8, 4096).unwrap();
    let mut registration = RegisteredTool::new(Arc::new(move |_| {
        // This increments before a future exists; checking only the future's
        // result, output frames or first await does not protect this effect.
        effects.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(json!({"done": true})) })
    }));
    registration.cancel_on_interruption = false;
    executor.register("effect", registration).unwrap();
    executor
}
async fn run(processor: Arc<dyn FrameProcessor>, frame: Frame) -> Vec<DirectedFrame> {
    let worker = Pipeline::new(vec![processor])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    frames
}
#[tokio::test]
async fn review_unadmitted_capabilities_never_construct_tool_callbacks() {
    for state in [
        SpeculationStatus::Pending,
        SpeculationStatus::Confirmed,
        SpeculationStatus::Released,
        SpeculationStatus::Cancelled,
    ] {
        let effects = Arc::new(AtomicUsize::new(0));
        let owner = SpeculationOwner::new();
        match state {
            SpeculationStatus::Pending => {}
            SpeculationStatus::Confirmed => {
                assert!(owner.confirm());
            }
            SpeculationStatus::Released => {
                assert!(owner.confirm());
                assert!(owner.capability().release_after_boundary());
            }
            SpeculationStatus::Cancelled => {
                owner.cancel();
            }
        }
        run(Arc::new(executor(effects.clone())), declaration(&owner)).await;
        assert_eq!(
            effects.load(Ordering::SeqCst),
            0,
            "unadmitted {state:?} capability constructed callback"
        );
    }
}
#[derive(Clone, Copy)]
enum Revocation {
    None,
    Cancel,
    Health,
    Interruption,
}
struct DispatchThenRevoke {
    executor: ToolExecutor,
    owner: Arc<SpeculationOwner>,
    revocation: Revocation,
    health: ProcessorHealth,
}
#[async_trait]
impl FrameProcessor for DispatchThenRevoke {
    fn name(&self) -> &str {
        "ReviewDispatchThenRevoke"
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
        if matches!(frame.data, FrameData::FunctionCallsStarted(_)) {
            let capability = frame.native_speculation().unwrap();
            assert!(capability.claim_inference());
            assert!(capability.bind_admission(ctx.clone(), ctx.health().generation()));
            assert!(self.owner.confirm());
            assert!(capability.release_after_boundary());
            assert!(capability.admits_effect());
            self.executor.process(frame, direction, ctx).await?;
            // A current-thread runtime cannot first poll the spawned application
            // callback before this immediately-ready process call returns.
            match self.revocation {
                Revocation::None => {}
                Revocation::Cancel => {
                    self.owner.cancel();
                }
                Revocation::Health => {
                    ctx.set_usable(false);
                    ctx.set_usable(true);
                }
                Revocation::Interruption => {
                    ctx.broadcast_interruption().await?;
                }
            }
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
async fn dispatch_revocation(revocation: Revocation) -> (usize, Vec<DirectedFrame>) {
    let effects = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(SpeculationOwner::new());
    let frame = declaration(&owner);
    let wrapper = DispatchThenRevoke {
        executor: executor(effects.clone()),
        owner,
        revocation,
        health: ProcessorHealth::default(),
    };
    let frames = run(Arc::new(wrapper), frame).await;
    (effects.load(Ordering::SeqCst), frames)
}
#[tokio::test]
async fn review_cancel_after_dispatch_before_constructor_has_no_effect() {
    let (effects, _) = dispatch_revocation(Revocation::Cancel).await;
    assert_eq!(
        effects, 0,
        "output admission is too early to protect the callback constructor"
    );
}
#[tokio::test]
async fn review_health_aba_after_dispatch_before_constructor_has_no_effect() {
    let (effects, _) = dispatch_revocation(Revocation::Health).await;
    assert_eq!(
        effects, 0,
        "health recovery cannot revive an old effect admission"
    );
}
#[tokio::test]
async fn review_live_admission_executes_once_and_consumes_wire_capability() {
    let (effects, frames) = dispatch_revocation(Revocation::None).await;
    assert_eq!(
        effects, 1,
        "positive control must actually construct the callback"
    );
    let started = frames
        .iter()
        .find(|f| matches!(f.frame.data, FrameData::FunctionCallsStarted(_)))
        .expect("admitted tool declaration");
    assert!(
        started.frame.native_speculation().is_none(),
        "executor must retain capability privately, not leak it to the wire declaration"
    );
    assert!(started.frame.native_tool_batch().unwrap().is_some());
    assert!(serde_json::to_value(&started.frame).is_err());
    let mut consumed = started.frame.clone();
    assert!(consumed.take_native_tool_batch().unwrap().is_some());
    assert!(serde_json::to_value(&consumed).is_ok());
}
#[tokio::test]
async fn review_causal_interruption_after_dispatch_blocks_noninterruptible_tool_constructor() {
    let (effects, _) = dispatch_revocation(Revocation::Interruption).await;
    assert_eq!(
        effects, 0,
        "a noninterruptible tool still needs its speculative causal admission"
    );
}
struct DispatchThenInterruptOrdinary(ToolExecutor);
#[async_trait]
impl FrameProcessor for DispatchThenInterruptOrdinary {
    fn name(&self) -> &str {
        "ReviewOrdinarySurvival"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let tools = matches!(frame.data, FrameData::FunctionCallsStarted(_));
        self.0.process(frame, direction, ctx).await?;
        if tools {
            ctx.broadcast_interruption().await?;
        }
        Ok(())
    }
}
#[tokio::test]
async fn review_ordinary_noninterruptible_callback_survives_same_interruption() {
    let effects = Arc::new(AtomicUsize::new(0));
    let owner = SpeculationOwner::new();
    let mut frame = declaration(&owner);
    frame.take_native_speculation();
    let frames = run(
        Arc::new(DispatchThenInterruptOrdinary(executor(effects.clone()))),
        frame,
    )
    .await;
    assert_eq!(
        effects.load(Ordering::SeqCst),
        1,
        "fixing speculative epochs must not remove ordinary noninterruptible work"
    );
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_))));
}
