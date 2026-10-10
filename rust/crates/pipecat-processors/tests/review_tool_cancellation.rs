//! Independent cancellation probes; construction itself may have external effects.
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
use tokio::sync::Notify;

fn call(id: &str, name: &str) -> FunctionCall {
    FunctionCall {
        function_name: name.into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    }
}
fn declaration(calls: Vec<FunctionCall>) -> Frame {
    Frame::new(FrameData::FunctionCallsStarted(calls))
}
fn cancels(frames: &[DirectedFrame], id: &str) -> usize {
    frames.iter().filter(|item| matches!(&item.frame.data, FrameData::FunctionCallCancel(c) if c.tool_call_id == id)).count()
}
fn results(frames: &[DirectedFrame], id: &str) -> usize {
    frames.iter().filter(|item| matches!(&item.frame.data, FrameData::FunctionCallResult(c) if c.tool_call_id == id)).count()
}
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("review operation deadlocked or failed to settle")
}

struct BeforeFirstPoll {
    executor: ToolExecutor,
    reject_spawn: bool,
}
#[async_trait]
impl FrameProcessor for BeforeFirstPoll {
    fn name(&self) -> &str {
        "ReviewBeforeFirstPoll"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !matches!(frame.data, FrameData::FunctionCallsStarted(_)) {
            return ctx.push(frame, direction).await;
        }
        if self.reject_spawn {
            let occupied = ctx.spawn("occupied", false, std::future::pending())?;
            let rejected = self.executor.process(frame, direction, ctx).await;
            assert!(rejected.is_err(), "test must exercise real spawn rejection");
            assert!(ctx.cancel_task(occupied));
            Ok(())
        } else {
            self.executor.process(frame, direction, ctx).await?;
            // Current-thread execution and ready pushes ensure the callback has
            // not been polled before this synchronous interruption.
            ctx.broadcast_interruption().await
        }
    }
}
async fn pre_poll(reject_spawn: bool) -> Vec<DirectedFrame> {
    let constructions = Arc::new(AtomicUsize::new(0));
    let count = constructions.clone();
    let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
    executor
        .register(
            "effect",
            RegisteredTool::new(Arc::new(move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!("external effect returned")) })
            })),
        )
        .unwrap();
    let worker = Pipeline::new(vec![Arc::new(BeforeFirstPoll {
        executor,
        reject_spawn,
    })])
    .start(PipelineConfig {
        max_background_tasks: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    worker
        .sender()
        .send(
            declaration(vec![call("never-polled", "effect")]),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(constructions.load(Ordering::SeqCst), 0);
    assert_eq!(results(&frames, "never-polled"), 0);
    assert_eq!(
        cancels(&frames, "never-polled"),
        1,
        "a declared call needs exactly one cancellation outcome even when its future never starts"
    );
    frames
}
#[tokio::test]
async fn review_interruption_before_first_poll_settles_declared_call() {
    pre_poll(false).await;
}
#[tokio::test]
async fn review_spawn_rejection_settles_without_admission_lock_deadlock() {
    pre_poll(true).await;
}

#[tokio::test]
async fn review_running_interruptible_call_cancels_and_async_call_survives() {
    let entered = Arc::new(Notify::new());
    let running = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let mut executor = ToolExecutor::new(2, 8, 1024).unwrap();
    for (name, interruptible) in [("sync", true), ("async", false)] {
        let entered = entered.clone();
        let running = running.clone();
        let release = release.clone();
        let mut tool = RegisteredTool::new(Arc::new(move |_| {
            let (entered, running, release) = (entered.clone(), running.clone(), release.clone());
            running.fetch_add(1, Ordering::SeqCst);
            entered.notify_one();
            Box::pin(async move {
                release.notified().await;
                Ok(json!("finished"))
            })
        }));
        tool.cancel_on_interruption = interruptible;
        executor.register(name, tool).unwrap();
    }
    let mut worker = Pipeline::new(vec![Arc::new(executor)])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            declaration(vec![call("cancel-me", "sync"), call("keep-me", "async")]),
            Direction::Downstream,
        )
        .await
        .unwrap();
    bounded(async {
        while running.load(Ordering::SeqCst) != 2 {
            entered.notified().await;
        }
    })
    .await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    let mut frames = vec![];
    bounded(async {
        loop {
            let item = worker.recv().await.expect("closed before cancellation outcome");
            let done = matches!(&item.frame.data, FrameData::FunctionCallCancel(c) if c.tool_call_id == "cancel-me");
            frames.push(item);
            if done { break; }
        }
    }).await;
    release.notify_waiters();
    worker.end().await.unwrap();
    let (tail, report) = bounded(worker.collect()).await.unwrap();
    frames.extend(tail);
    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(cancels(&frames, "cancel-me"), 1);
    assert_eq!(results(&frames, "cancel-me"), 0);
    assert_eq!(cancels(&frames, "keep-me"), 0);
    assert_eq!(results(&frames, "keep-me"), 1);
}

#[tokio::test]
async fn review_explicit_cancel_is_one_terminal_outcome_without_effect_replay() {
    let entered = Arc::new(Notify::new());
    let effect_count = Arc::new(AtomicUsize::new(0));
    let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
    let (notify, count) = (entered.clone(), effect_count.clone());
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        notify.notify_one();
        Box::pin(std::future::pending())
    }));
    tool.cancel_on_interruption = false;
    executor.register("external", tool).unwrap();
    let worker = Pipeline::new(vec![Arc::new(executor)])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            declaration(vec![call("explicit", "external")]),
            Direction::Downstream,
        )
        .await
        .unwrap();
    bounded(entered.notified()).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "external".into(),
                tool_call_id: "explicit".into(),
                run_llm: true,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            declaration(vec![call("explicit", "external")]),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(effect_count.load(Ordering::SeqCst), 1);
    assert_eq!(cancels(&frames, "explicit"), 1);
    assert_eq!(results(&frames, "explicit"), 0);
    let cancelled = frames
        .iter()
        .find_map(|item| match &item.frame.data {
            FrameData::FunctionCallCancel(c) => Some(c),
            _ => None,
        })
        .unwrap();
    assert!(
        cancelled.run_llm,
        "an explicitly requested inference continuation must not be silently discarded"
    );
}
