//! Native authoritative history must preserve eager effect-admission decisions.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
struct AdmitThenDecide {
    executor: ToolExecutor,
    owner: Arc<SpeculationOwner>,
    revoke: bool,
}
#[async_trait]
impl FrameProcessor for AdmitThenDecide {
    fn name(&self) -> &str {
        "IndependentEagerNativeHistory"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::FunctionCallsStarted(_))
        {
            let capability = frame.native_speculation().unwrap();
            assert!(capability.claim_inference());
            assert!(capability.bind_admission(ctx.clone(), ctx.health().generation()));
            assert!(self.owner.confirm());
            assert!(capability.release_after_boundary());
            self.executor.process(frame, direction, ctx).await?;
            // All queue admissions are ready on this current-thread runtime;
            // callback construction cannot happen before this final decision.
            if self.revoke {
                self.owner.cancel();
            }
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn independent_eager_effect_decision_and_native_history_agree() {
    for revoke in [false, true] {
        let effects = Arc::new(AtomicUsize::new(0));
        let counter = effects.clone();
        let mut executor = ToolExecutor::new(1, 8, 4096).unwrap();
        executor
            .register(
                "effect",
                RegisteredTool::new(Arc::new(move |_| {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Box::pin(async { Ok(json!("committed")) })
                })),
            )
            .unwrap();
        let journal = executor.outcomes();
        let owner = Arc::new(SpeculationOwner::new());
        let context = SharedContext::default();
        let assistant = Arc::new(LLMAssistantAggregator::new(
            context.clone(),
            Default::default(),
        ));
        let worker = Pipeline::new(vec![
            Arc::new(AdmitThenDecide {
                executor,
                owner: owner.clone(),
                revoke,
            }),
            assistant.clone(),
        ])
        .start(Default::default())
        .await
        .unwrap();
        let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
            function_name: "effect".into(),
            tool_call_id: "eager-native".into(),
            arguments: json!({}),
        }]));
        frame.set_native_speculation(owner.capability()).unwrap();
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        let expected = if revoke {
            "CANCELLED".to_owned()
        } else {
            json!("committed").to_string()
        };
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if journal.snapshot().total_terminal == 1
                    && context
                        .snapshot_rich()
                        .unwrap()
                        .messages
                        .iter()
                        .any(|message| {
                            message.role == ChatRole::Tool
                                && message.tool_call_id.as_deref() == Some("eager-native")
                                && message.content == MessageContent::Text(expected.clone())
                        })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(effects.load(Ordering::SeqCst), usize::from(!revoke));
        let snapshot = journal.snapshot();
        assert!(snapshot.active.is_empty());
        assert_eq!(snapshot.total_terminal, 1);
        assert_eq!(
            snapshot.terminal[0].outcome.effects(),
            if revoke {
                ToolCallEffectStatus::NotStarted
            } else {
                ToolCallEffectStatus::ExternalEffectsUnknown
            }
        );
        assert_eq!(
            snapshot.terminal[0].outcome.state(),
            if revoke {
                ToolCallTerminalState::Cancelled
            } else {
                ToolCallTerminalState::Completed
            }
        );
        assert!(!assistant.has_function_calls_in_progress().unwrap());
        worker
            .sender()
            .send(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
            .await
            .unwrap();
        worker.end().await.unwrap();
        let (frames, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            frames
                .iter()
                .filter(|frame| frame.direction == Direction::Upstream
                    && matches!(frame.frame.data, FrameData::LlmContext(_)))
                .count(),
            usize::from(!revoke)
        );
        for frame in frames {
            assert!(frame.frame.native_tool_outcome().unwrap().is_none());
            serde_json::to_vec(&frame.frame).unwrap();
        }
    }
}

#[derive(Default)]
struct BatchWitness(std::sync::Mutex<Vec<(u64, String)>>);
#[async_trait]
impl FrameProcessor for BatchWitness {
    fn name(&self) -> &str {
        "IndependentBatchIdentityWitness"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let Some(outcome) = frame.native_tool_outcome()? {
            let batch = outcome.batch().expect("executor outcome carries batch");
            self.0
                .lock()
                .unwrap()
                .push((batch.id(), batch.group_id().to_owned()));
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn independent_executor_repeated_public_frame_ids_cannot_alias_batches() {
    let effects = Arc::new(AtomicUsize::new(0));
    let count = effects.clone();
    let mut executor = ToolExecutor::new(2, 16, 4096).unwrap();
    executor
        .register(
            "effect",
            RegisteredTool::new(Arc::new(move |call| {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(json!(call.tool_call_id)) })
            })),
        )
        .unwrap();
    let witness = Arc::new(BatchWitness::default());
    let context = SharedContext::default();
    let worker = Pipeline::new(vec![
        Arc::new(executor),
        witness.clone(),
        Arc::new(LLMAssistantAggregator::new(
            context.clone(),
            Default::default(),
        )),
    ])
    .start(Default::default())
    .await
    .unwrap();
    for id in ["one", "two"] {
        let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
            function_name: "effect".into(),
            tool_call_id: id.into(),
            arguments: json!({}),
        }]));
        frame.id = u64::MAX;
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3),async{loop{
        if context.snapshot_rich().unwrap().messages.iter().filter(|message|message.role==ChatRole::Tool && matches!(&message.content,MessageContent::Text(value) if value=="\"one\"" || value=="\"two\"")).count()==2{break}
        tokio::task::yield_now().await;
    }}).await.unwrap();
    let groups = witness.0.lock().unwrap().clone();
    assert_eq!(groups.len(), 2);
    assert_ne!(groups[0].0, groups[1].0);
    assert_ne!(groups[0].1, groups[1].1);
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
