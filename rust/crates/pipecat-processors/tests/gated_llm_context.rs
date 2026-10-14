//! Real-pipeline witnesses for the pinned Event/latest-context contract.
use pipecat_core::*;
use pipecat_processors::*;
use std::{
    future::pending,
    sync::{Arc, Mutex},
    time::Duration,
};

fn context(text: &str) -> Frame {
    Frame::new(FrameData::LlmContext(LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, text)],
        ..Default::default()
    }))
}
fn detailed(text: &str) -> Frame {
    Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, text).into()],
            tools: ToolSet::NotGiven,
            tool_choice: None,
        },
        speculation: false,
    }))
}
async fn wait_for(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition did not become true");
}
async fn recv(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(5), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn start(processor: Arc<GatedLlmContextAggregator>) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![processor])
        .start(Default::default())
        .await
        .unwrap();
    assert!(recv(&mut worker).await.frame.data.is_start());
    assert!(worker.try_recv().is_err(), "Start must pass exactly once");
    worker
}
async fn barrier(worker: &mut PipelineWorker, direction: Direction) {
    let frame = Frame::text("processing barrier");
    let id = frame.id;
    worker.sender().send(frame, direction).await.unwrap();
    let actual = recv(worker).await;
    assert_eq!(actual.frame.id, id);
    assert_eq!(actual.direction, direction);
}
async fn send_cached(worker: &mut PipelineWorker, frame: Frame, direction: Direction) {
    worker.sender().send(frame, direction).await.unwrap();
    barrier(worker, direction).await;
}
async fn finish(worker: PipelineWorker, terminal: FrameData) -> (Vec<DirectedFrame>, WorkerReport) {
    worker
        .sender()
        .send(Frame::new(terminal), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap()
}
fn gate() -> Arc<GatedLlmContextAggregator> {
    Arc::new(GatedLlmContextAggregator::new(Default::default()).unwrap())
}

#[tokio::test]
async fn retains_only_latest_and_preserves_both_context_forms() {
    for latest_detailed in [false, true] {
        let processor = gate();
        assert!(processor.current_notifier().is_err());
        let mut worker = start(processor.clone()).await;
        let notifier = processor.current_notifier().unwrap();
        let a = context("A");
        let a_identity = a.observation_token();
        send_cached(&mut worker, a, Direction::Downstream).await;
        let mut b = if latest_detailed {
            detailed("B")
        } else {
            context("B")
        };
        let tag = LocalFrameTag::new();
        tag.mark(&mut b).unwrap();
        b.pts = Some(123);
        b.metadata
            .insert("sentinel".into(), serde_json::json!(["original", 7]));
        b.transport_source = Some("source".into());
        b.transport_destination = Some("destination".into());
        let expected = b.clone();
        send_cached(&mut worker, b, Direction::Upstream).await;
        assert!(
            !a_identity.is_alive(),
            "replaced context must release ownership"
        );
        assert!(notifier.snapshot().context_pending);
        assert!(worker.try_recv().is_err());
        // Same synchronous turn: Event notifications coalesce before the worker
        // gets polled, rather than granting eight future release tickets.
        for _ in 0..8 {
            notifier.notify().unwrap();
        }
        let actual = recv(&mut worker).await;
        assert_eq!(actual.frame, expected);
        assert!(tag.contains(&actual.frame));
        assert_eq!(actual.direction, Direction::Downstream);
        assert_eq!(notifier.snapshot().notifications_consumed, 1);
        assert!(!notifier.snapshot().context_pending);
        assert!(worker.try_recv().is_err());
        let (_, report) = finish(worker, FrameData::End(Default::default())).await;
        assert!(report.errors.is_empty());
        assert!(notifier.notify().is_err());
        assert!(processor.current_notifier().is_err());
    }
}

#[tokio::test]
async fn empty_consumed_event_does_not_release_a_future_context() {
    let processor = gate();
    let mut worker = start(processor.clone()).await;
    let notifier = processor.current_notifier().unwrap();
    notifier.notify().unwrap();
    wait_for(|| notifier.snapshot().notifications_consumed == 1).await;
    assert!(!notifier.snapshot().notification_pending);
    let next = context("arrived after empty consumption");
    let id = next.id;
    send_cached(&mut worker, next, Direction::Downstream).await;
    assert!(notifier.snapshot().context_pending);
    assert!(worker.try_recv().is_err());
    notifier.notify().unwrap();
    assert_eq!(recv(&mut worker).await.frame.id, id);
    assert_eq!(notifier.snapshot().notifications_consumed, 2);
    finish(worker, FrameData::End(Default::default())).await;
}

#[tokio::test]
async fn start_open_is_one_context_per_setup_and_preserves_original_direction() {
    let processor = Arc::new(
        GatedLlmContextAggregator::new(GatedLlmContextAggregatorConfig {
            start_open: true,
            ..Default::default()
        })
        .unwrap(),
    );
    for _ in 0..2 {
        let mut worker = start(processor.clone()).await;
        let notifier = processor.current_notifier().unwrap();
        // Ordinary frames do not consume the first-context bypass.
        barrier(&mut worker, Direction::Downstream).await;
        let first = detailed("first");
        let expected = first.clone();
        worker
            .sender()
            .send(first, Direction::Upstream)
            .await
            .unwrap();
        let actual = recv(&mut worker).await;
        assert_eq!(actual.frame, expected);
        assert_eq!(actual.direction, Direction::Upstream);
        let next = context("second");
        let id = next.id;
        send_cached(&mut worker, next, Direction::Upstream).await;
        notifier.notify().unwrap();
        let actual = recv(&mut worker).await;
        assert_eq!(actual.frame.id, id);
        assert_eq!(actual.direction, Direction::Downstream);
        finish(worker, FrameData::Stop).await;
    }
}

#[tokio::test]
async fn noncontext_frames_and_native_continuation_pass_unchanged_in_both_directions() {
    let processor = gate();
    let mut worker = start(processor).await;
    for direction in [Direction::Upstream, Direction::Downstream] {
        let carrier =
            Arc::new(NativeLlmContinuation::start(42, LlmContinuationProvider::Anthropic).unwrap());
        let mut frame = Frame::new(FrameData::LlmResponseStart);
        frame.set_native_llm_continuation(carrier.clone()).unwrap();
        let tag = LocalFrameTag::new();
        tag.mark(&mut frame).unwrap();
        frame
            .metadata
            .insert("sentinel".into(), serde_json::json!("opaque envelope"));
        let expected = frame.clone();
        worker.sender().send(frame, direction).await.unwrap();
        let actual = recv(&mut worker).await;
        assert_eq!(actual.direction, direction);
        assert_eq!(actual.frame, expected);
        assert!(tag.contains(&actual.frame));
        assert!(Arc::ptr_eq(
            actual.frame.native_llm_continuation().unwrap().unwrap(),
            &carrier
        ));
    }
    finish(worker, FrameData::End(Default::default())).await;
}

#[tokio::test]
async fn delayed_native_context_preserves_realtime_cause_direct_handler_and_speculation() {
    let processor = gate();
    let mut worker = start(processor.clone()).await;
    let notifier = processor.current_notifier().unwrap();
    let mut realtime = detailed("authorized native realtime context");
    NativeRealtimeContextProducer::default()
        .attach(&mut realtime, RealtimeContextIntent::Run, true)
        .unwrap();
    let cause = realtime
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .clone();
    let expected = realtime.clone();
    send_cached(&mut worker, realtime, Direction::Downstream).await;
    notifier.notify().unwrap();
    let actual = recv(&mut worker).await.frame;
    assert_eq!(actual, expected);
    assert!(Arc::ptr_eq(
        actual.native_realtime_context_cause().unwrap().unwrap(),
        &cause
    ));
    for schema_form in [false, true] {
        let handler = LocalToolFunction::new(|value| async move { Ok(value) });
        let tool = DirectFunction {
            schema: ToolDefinition {
                name: "echo".into(),
                description: "echo input".into(),
                parameters: serde_json::json!({"type":"object"}),
            },
            handler: handler.clone(),
        };
        let tools = if schema_form {
            ToolSet::Schema(ToolsSchema {
                standard_tools: vec![StandardTool::Direct(tool)],
                custom_tools: None,
            })
        } else {
            ToolSet::Direct(vec![tool])
        };
        let frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![ChatMessage::new(ChatRole::User, "authorized context").into()],
                tools,
                tool_choice: None,
            },
            speculation: false,
        }));
        let expected = frame.clone();
        send_cached(&mut worker, frame, Direction::Downstream).await;
        notifier.notify().unwrap();
        let actual = recv(&mut worker).await.frame;
        assert_eq!(actual, expected);
        let FrameData::LlmContextDetailed(context) = actual.data else {
            panic!("detailed context");
        };
        let actual_handler = match &context.context.tools {
            ToolSet::Direct(tools) => &tools[0].handler,
            ToolSet::Schema(schema) => match &schema.standard_tools[0] {
                StandardTool::Direct(tool) => &tool.handler,
                _ => panic!("direct tool"),
            },
            _ => panic!("original tools"),
        };
        assert_eq!(actual_handler, &handler);
        assert_eq!(
            actual_handler
                .call(serde_json::json!({"ok":true}))
                .await
                .unwrap(),
            serde_json::json!({"ok":true})
        );
    }
    let owner = SpeculationOwner::new();
    let capability = owner.capability();
    let mut frame = detailed("speculative");
    if let FrameData::LlmContextDetailed(context) = &mut frame.data {
        context.speculation = true;
    }
    frame.set_native_speculation(capability.clone()).unwrap();
    let expected = frame.clone();
    send_cached(&mut worker, frame, Direction::Upstream).await;
    notifier.notify().unwrap();
    let actual = recv(&mut worker).await.frame;
    assert_eq!(actual, expected);
    assert_eq!(actual.native_speculation(), Some(&capability));
    finish(worker, FrameData::End(Default::default())).await;
}

#[tokio::test]
async fn interruption_releases_old_slot_without_killing_supervisor_or_refreshing_it() {
    let processor = gate();
    let mut worker = start(processor.clone()).await;
    let notifier = processor.current_notifier().unwrap();
    let old = context("old epoch");
    let old_identity = old.observation_token();
    send_cached(&mut worker, old, Direction::Downstream).await;
    let interruption = Frame::interruption();
    let expected = interruption.clone();
    worker
        .sender()
        .send(interruption, Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(recv(&mut worker).await.frame, expected);
    assert!(!old_identity.is_alive());
    assert!(!notifier.snapshot().context_pending);
    assert!(notifier.snapshot().supervisor_running);
    notifier.notify().unwrap();
    wait_for(|| notifier.snapshot().notifications_consumed == 1).await;
    assert!(worker.try_recv().is_err());
    let fresh = detailed("new epoch");
    let id = fresh.id;
    send_cached(&mut worker, fresh, Direction::Downstream).await;
    notifier.notify().unwrap();
    assert_eq!(recv(&mut worker).await.frame.id, id);
    finish(worker, FrameData::End(Default::default())).await;
}

// Withhold the local clear only, so this real runtime interruption tests the
// retained invocation fence independently of the gate's slot-clearing logic.
struct GateWithoutLocalClear(Arc<GatedLlmContextAggregator>);
#[async_trait]
impl FrameProcessor for GateWithoutLocalClear {
    fn name(&self) -> &str {
        "GateWithoutLocalClear"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Interruption) {
            ctx.push(frame, direction).await
        } else {
            self.0.process(frame, direction, ctx).await
        }
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.0.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cleanup().await
    }
}
#[tokio::test]
async fn original_fence_survives_noninterruptible_background_scope_even_without_local_clear() {
    for explicitly_uninterruptible in [false, true] {
        let processor = gate();
        let mut worker = Pipeline::new(vec![Arc::new(GateWithoutLocalClear(processor.clone()))])
            .start(Default::default())
            .await
            .unwrap();
        assert!(recv(&mut worker).await.frame.data.is_start());
        let notifier = processor.current_notifier().unwrap();
        let mut old = context("must not renew invocation at notification");
        old.interruptible = !explicitly_uninterruptible;
        send_cached(&mut worker, old, Direction::Downstream).await;
        worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        assert!(matches!(
            recv(&mut worker).await.frame.data,
            FrameData::Interruption
        ));
        assert!(notifier.snapshot().context_pending);
        notifier.notify().unwrap();
        wait_for(|| notifier.snapshot().notifications_consumed == 1).await;
        barrier(&mut worker, Direction::Downstream).await;
        assert!(
            worker.try_recv().is_err(),
            "old invocation escaped through surviving task"
        );
        let fresh = context("new generation");
        let id = fresh.id;
        send_cached(&mut worker, fresh, Direction::Downstream).await;
        notifier.notify().unwrap();
        assert_eq!(recv(&mut worker).await.frame.id, id);
        let (_, report) = finish(worker, FrameData::End(Default::default())).await;
        assert!(report.dropped_frames >= 1);
    }
}

#[tokio::test]
async fn every_terminal_releases_waiting_slot_joins_and_retires_old_notifiers() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::EndDetailed(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
        FrameData::CancelDetailed(Default::default()),
    ] {
        for has_context in [false, true] {
            let processor = gate();
            let mut worker = start(processor.clone()).await;
            let old = processor.current_notifier().unwrap();
            let held = context("never released at termination");
            let identity = held.observation_token();
            if has_context {
                send_cached(&mut worker, held, Direction::Downstream).await;
            } else {
                drop(held);
            }
            let (outputs, report) = finish(worker, terminal.clone()).await;
            assert!(report.errors.is_empty());
            assert!(!outputs.iter().any(|frame| matches!(
                frame.frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )));
            assert!(!identity.is_alive());
            assert!(old.snapshot().closed);
            assert!(!old.snapshot().supervisor_running);
            assert!(old.notify().is_err());
            assert!(processor.current_notifier().is_err());
            let mut worker = start(processor.clone()).await;
            let new = processor.current_notifier().unwrap();
            let fresh = context("next session");
            let id = fresh.id;
            send_cached(&mut worker, fresh, Direction::Downstream).await;
            assert!(old.notify().is_err());
            assert_eq!(new.snapshot().notifications_consumed, 0);
            assert!(worker.try_recv().is_err());
            new.notify().unwrap();
            assert_eq!(recv(&mut worker).await.frame.id, id);
            finish(worker, FrameData::End(Default::default())).await;
        }
    }
}

#[tokio::test]
async fn worker_owner_drop_releases_buffered_and_already_published_ownership() {
    for release in [false, true] {
        let processor = gate();
        let mut worker = start(processor.clone()).await;
        let old = processor.current_notifier().unwrap();
        let frame = context("owned by the gate or worker output");
        let identity = frame.observation_token();
        send_cached(&mut worker, frame, Direction::Downstream).await;
        if release {
            old.notify().unwrap();
            wait_for(|| old.snapshot().notifications_consumed == 1).await;
        }
        drop(worker);
        wait_for(|| {
            old.snapshot().closed && !old.snapshot().supervisor_running && !identity.is_alive()
        })
        .await;
        assert!(old.notify().is_err());
        assert!(processor.current_notifier().is_err());
        // Dropping the owner offers no public coordinator join handle. Reuse is
        // covered only by the explicitly joined terminal matrix above.
    }
}

#[tokio::test]
async fn oversized_replacement_keeps_valid_slot_and_does_not_consume_start_open() {
    for start_open in [false, true] {
        let processor = Arc::new(
            GatedLlmContextAggregator::new(GatedLlmContextAggregatorConfig {
                start_open,
                max_context_bytes: 4096,
            })
            .unwrap(),
        );
        let mut worker = start(processor.clone()).await;
        let notifier = processor.current_notifier().unwrap();
        let valid = context("valid");
        let id = valid.id;
        if !start_open {
            send_cached(&mut worker, valid.clone(), Direction::Downstream).await;
        }
        worker
            .sender()
            .send(
                context(&"oversized secret".repeat(1000)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let error = recv(&mut worker).await;
        let FrameData::Error(error) = error.frame.data else {
            panic!("budget rejection is visible");
        };
        assert!(error.error.contains("payload budget"));
        assert!(!error.error.contains("oversized secret"));
        if start_open {
            worker
                .sender()
                .send(valid, Direction::Upstream)
                .await
                .unwrap();
        } else {
            assert!(notifier.snapshot().context_pending);
            notifier.notify().unwrap();
        }
        let actual = recv(&mut worker).await;
        assert_eq!(actual.frame.id, id);
        assert_eq!(
            actual.direction,
            if start_open {
                Direction::Upstream
            } else {
                Direction::Downstream
            }
        );
        let (_, report) = finish(worker, FrameData::End(Default::default())).await;
        assert_eq!(report.errors.len(), 1);
    }
}

struct OccupyTaskSlot(Arc<GatedLlmContextAggregator>);
#[async_trait]
impl FrameProcessor for OccupyTaskSlot {
    fn name(&self) -> &str {
        "OccupyGateTaskSlot"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.spawn("occupied", false, async {
            pending::<()>().await;
            Ok(())
        })?;
        self.0.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.0.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cleanup().await
    }
}
#[tokio::test]
async fn rejected_spawn_drops_its_unpolled_guard_and_allows_joined_reuse() {
    let processor = gate();
    let result = Pipeline::new(vec![Arc::new(OccupyTaskSlot(processor.clone()))])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await;
    let error = match result {
        Err(error) => error,
        Ok(worker) => tokio::time::timeout(Duration::from_secs(5), worker.collect())
            .await
            .unwrap()
            .unwrap_err(),
    };
    assert!(error.to_string().contains("background task limit"));
    assert!(processor.current_notifier().is_err());
    let worker = start(processor).await;
    finish(worker, FrameData::End(Default::default())).await;
}

// A downstream processor owns one normal invocation, keeping a one-element
// normal queue full. Gate publication must fail visibly instead of retrying or
// silently buffering unbounded work. The system lane remains available.
struct BlockText {
    entered: Arc<Mutex<bool>>,
}
#[async_trait]
impl FrameProcessor for BlockText {
    fn name(&self) -> &str {
        "BlockText"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.entered.lock().unwrap() = true;
            pending::<()>().await;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn full_destination_causes_visible_error_and_closes_notification_session() {
    let processor = gate();
    let entered = Arc::new(Mutex::new(false));
    let mut worker = Pipeline::new(vec![
        processor.clone(),
        Arc::new(BlockText {
            entered: entered.clone(),
        }),
    ])
    .start(PipelineConfig {
        queue_capacity: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(recv(&mut worker).await.frame.data.is_start());
    let notifier = processor.current_notifier().unwrap();
    worker
        .sender()
        .send(
            Frame::text("block destination invocation"),
            Direction::Downstream,
        )
        .await
        .unwrap();
    wait_for(|| *entered.lock().unwrap()).await;
    worker
        .sender()
        .send(Frame::text("fill destination lane"), Direction::Downstream)
        .await
        .unwrap();
    // FIFO at the gate means this context is cached only after the preceding
    // fill frame was admitted to the destination's already-blocked lane.
    worker
        .sender()
        .send(
            context("release into full destination"),
            Direction::Downstream,
        )
        .await
        .unwrap();
    wait_for(|| notifier.snapshot().context_pending).await;
    notifier.notify().unwrap();
    wait_for(|| notifier.snapshot().closed).await;
    assert!(!notifier.snapshot().context_pending);
    assert!(!notifier.snapshot().supervisor_running);
    assert!(notifier.notify().is_err());
    let result = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap();
    assert!(
        result.unwrap_err().to_string().contains("lane is full"),
        "queue admission error is fatal and visible"
    );
}

#[test]
fn validates_payload_budget_bounds() {
    for limit in [0, 64 * 1024 * 1024 + 1] {
        assert!(
            GatedLlmContextAggregator::new(GatedLlmContextAggregatorConfig {
                max_context_bytes: limit,
                ..Default::default()
            })
            .is_err()
        );
    }
    for limit in [1, 64 * 1024 * 1024] {
        assert!(
            GatedLlmContextAggregator::new(GatedLlmContextAggregatorConfig {
                max_context_bytes: limit,
                ..Default::default()
            })
            .is_ok()
        );
    }
}
