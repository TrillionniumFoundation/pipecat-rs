//! Known-open-call permission must keep the original assistant authority.
use super::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::Context;
use pipecat_turns::{LLMContextAggregatorPair, RealtimeServiceMode};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Semaphore;

struct Boundary {
    late_declarations: Option<mpsc::UnboundedSender<(Frame, ProcessorContext)>>,
    hold_continuation: AtomicBool,
    stop_interruption: AtomicBool,
    held: mpsc::UnboundedSender<(Frame, ProcessorContext)>,
    forwarded: mpsc::UnboundedSender<u64>,
}
#[async_trait]
impl FrameProcessor for Boundary {
    fn name(&self) -> &str {
        "known-open-continuation-boundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if let Some(late) = &self.late_declarations {
                if matches!(&frame.data, FrameData::FunctionCallsStarted(calls) if calls.iter().any(|call| call.tool_call_id == "second"))
                    || matches!(&frame.data, FrameData::FunctionCallInProgress(call) if call.tool_call_id == "second")
                {
                    late.send((frame, ctx.clone())).unwrap();
                    return Ok(());
                }
            }
        }
        let continuation = direction == Direction::Upstream
            && frame.native_realtime_context_cause()?.is_some_and(|cause| {
                cause.intent() == RealtimeContextIntent::AssistantContinuation
            });
        if continuation && self.hold_continuation.swap(false, Ordering::SeqCst) {
            self.held.send((frame, ctx.clone())).unwrap();
            return Ok(());
        }
        if direction == Direction::Upstream
            && matches!(frame.data, FrameData::Interruption)
            && self.stop_interruption.load(Ordering::SeqCst)
        {
            return Ok(());
        }
        let id = frame.id;
        ctx.push(frame, direction).await?;
        if continuation {
            self.forwarded.send(id).unwrap();
        }
        Ok(())
    }
}
async fn barrier(worker: &mut PipelineWorker) {
    inject(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("owed-barrier".into()),
        }),
    )
    .await;
    seen(worker, |frame| matches!(frame, FrameData::Flush(flush) if flush.origin.as_deref() == Some("owed-barrier"))).await;
}
async fn command(peer: &mut Peer, worker: &mut PipelineWorker, stage: &str) -> Value {
    timeout(Duration::from_secs(2), async {
        loop {
            tokio::select! {
                command = peer.recv.recv() => return command.expect("peer closed"),
                frame = worker.recv() => {
                    let frame = frame.expect("pipeline closed before provider command");
                    if let FrameData::Error(error) = frame.frame.data {
                        panic!("{stage}: pipeline error: {error:?}");
                    }
                }
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("provider command timeout during {stage}"))
}
#[derive(Clone, Copy)]
enum Last {
    Null,
    Cancel,
    Value,
}
async fn known_open(expire: bool, last: Last) {
    let (mut peer, client) = Peer::new().await;
    let processor = Arc::new(OpenAiRealtimeProcessor::new(client).unwrap());
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Auto,
    )
    .unwrap();
    let first_release = Arc::new(Semaphore::new(0));
    let last_release = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let release = first_release.clone();
    let called = entered.clone();
    let second_release = last_release.clone();
    let mut executor = ToolExecutor::new(2, 32, 4096).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |call| {
        let release = release.clone();
        let called = called.clone();
        let second_release = second_release.clone();
        Box::pin(async move {
            if call.tool_call_id == "first" {
                release.acquire().await.unwrap().forget();
                return Ok(json!("first-result"));
            }
            called.add_permits(1);
            if !matches!(last, Last::Cancel) {
                second_release.acquire().await.unwrap().forget();
            }
            match last {
                Last::Null => Ok(Value::Null),
                Last::Value => Ok(json!("new-intent")),
                Last::Cancel => std::future::pending().await,
            }
        })
    }));
    // This branch deliberately permits native completion after interruption;
    // only the shared assistant may select and bind its renewed local intent.
    if matches!(last, Last::Value) {
        tool.cancel_on_interruption = false;
    }
    executor.register("work", tool).unwrap();
    let (held_tx, mut held_rx) = mpsc::unbounded_channel();
    let (forward_tx, mut forward_rx) = mpsc::unbounded_channel();
    let (late_tx, mut late_rx) = mpsc::unbounded_channel();
    let before = Arc::new(Boundary {
        late_declarations: None,
        hold_continuation: false.into(),
        stop_interruption: false.into(),
        held: held_tx.clone(),
        forwarded: forward_tx.clone(),
    });
    let after = Arc::new(Boundary {
        late_declarations: matches!(last, Last::Value).then_some(late_tx),
        hold_continuation: true.into(),
        stop_interruption: false.into(),
        held: held_tx,
        forwarded: forward_tx,
    });
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24_000;
    config.start.audio_out_sample_rate = 24_000;
    let mut worker = Pipeline::new(vec![
        pair.user(),
        processor.clone(),
        before,
        Arc::new(executor),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        after.clone(),
        pair.assistant(),
    ])
    .start(config)
    .await
    .unwrap();
    barrier(&mut worker).await;
    peer.event(json!({"type":"response.created","response":{"id":"tools","status":"in_progress","output":[]}}));
    for (index, id) in ["first", "second"].into_iter().enumerate() {
        peer.event(json!({"type":"response.output_item.added","response_id":"tools","output_index":index,"item":{"type":"function_call","id":format!("item-{id}"),"call_id":id,"name":"work","arguments":""}}));
        peer.event(json!({"type":"response.function_call_arguments.done","response_id":"tools","item_id":format!("item-{id}"),"output_index":index,"call_id":id,"arguments":"{}"}));
    }
    timeout(Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let mut late_declarations = Vec::new();
    if matches!(last, Last::Value) {
        // Exercise the supported receipt-before-declaration path, without ever
        // importing a pending asynchronous context envelope into the provider.
        for _ in 0..2 {
            late_declarations.push(
                timeout(Duration::from_secs(2), late_rx.recv())
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
    } else {
        timeout(Duration::from_secs(2), async {
            loop {
                if pair
                    .context()
                    .snapshot()
                    .unwrap()
                    .messages
                    .iter()
                    .any(|message| message.tool_calls.iter().any(|call| call.id == "second"))
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second declaration precedes the genuine response end");
    }
    peer.event(
        json!({"type":"response.done","response":{"id":"tools","status":"completed","output":[]}}),
    );
    first_release.add_permits(1);
    assert_eq!(
        command(&mut peer, &mut worker, "first native output").await["item"]["call_id"],
        "first"
    );
    let (continuation, origin) = timeout(Duration::from_secs(2), held_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let original = continuation
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .inference_fence()
        .unwrap()
        .clone();
    let id = continuation.id;
    origin
        .push(continuation, Direction::Upstream)
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        while forward_rx.recv().await.unwrap() != id {}
    })
    .await
    .unwrap();
    barrier(&mut worker).await;
    let sender = processor.sender().unwrap();
    sender
        .send(Action::UpdateSession(SessionProperties::default()))
        .await
        .unwrap();
    assert_eq!(
        command(&mut peer, &mut worker, "admission FIFO marker").await["type"],
        "session.update"
    );
    peer.quiet().await;
    let generation = sender.generation();
    if expire {
        after.stop_interruption.store(true, Ordering::SeqCst);
        worker
            .sender()
            .send(Frame::interruption(), Direction::Upstream)
            .await
            .unwrap();
        barrier(&mut worker).await;
        assert!(!original.is_current());
        assert_eq!(generation, sender.generation());
    } else {
        assert!(original.is_current());
    }
    last_release.add_permits(1);
    if matches!(last, Last::Cancel) {
        inject(
            &worker,
            FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "work".into(),
                tool_call_id: "second".into(),
                run_llm: false,
            }),
        )
        .await;
    }
    let output = command(&mut peer, &mut worker, "last native output").await;
    assert_eq!(output["type"], "conversation.item.create");
    assert_eq!(output["item"]["call_id"], "second");
    assert_eq!(
        output["item"]["output"],
        match last {
            Last::Null => "COMPLETED",
            Last::Cancel => "CANCELLED",
            Last::Value => "\"new-intent\"",
        }
    );
    if !expire || matches!(last, Last::Value) {
        assert_eq!(
            command(&mut peer, &mut worker, "permitted continuation").await["type"],
            "response.create"
        );
        peer.event(json!({"type":"response.created","response":{"id":"continued","status":"in_progress","output":[]}}));
        peer.event(json!({"type":"response.done","response":{"id":"continued","status":"completed","output":[]}}));
    }
    barrier(&mut worker).await;
    peer.quiet().await;
    let settled_history = pair.context().snapshot().unwrap();
    for (declaration, origin) in late_declarations {
        origin
            .push(declaration, Direction::Downstream)
            .await
            .unwrap();
    }
    barrier(&mut worker).await;
    peer.quiet().await;
    assert_eq!(
        pair.context().snapshot().unwrap(),
        settled_history,
        "late declarations must not resurrect settled history or inference"
    );
    // All outcomes survive, regardless of whether their scheduling authority did.
    let history = pair.context().snapshot().unwrap();
    assert_eq!(
        history
            .messages
            .iter()
            .filter(|message| message.role == pipecat_services::Role::Tool)
            .count(),
        2
    );
    end(worker, peer).await;
}
#[tokio::test]
async fn held_request_with_null_last_result_keeps_original_authority() {
    for expire in [false, true] {
        known_open(expire, Last::Null).await;
    }
}
#[tokio::test]
async fn held_request_with_no_run_cancellation_keeps_original_authority() {
    for expire in [false, true] {
        known_open(expire, Last::Cancel).await;
    }
}
#[tokio::test]
async fn genuinely_new_native_intent_can_supersede_expired_held_request() {
    known_open(true, Last::Value).await;
}
