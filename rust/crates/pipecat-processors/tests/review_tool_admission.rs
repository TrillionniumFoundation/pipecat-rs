//! Adversarial admission and cancellation across separately queued tool batches.
use pipecat_core::*;
use pipecat_processors::tools::*;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Semaphore;

fn call(id: &str, name: &str) -> Frame {
    Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
        function_name: name.into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    }]))
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn result(worker: &mut PipelineWorker) -> FunctionCallResultFrame {
    loop {
        let frame = worker.recv().await.expect("pipeline ended before result");
        if let FrameData::FunctionCallResult(result) = frame.frame.data {
            return result;
        }
    }
}
#[tokio::test]
async fn capacity_rejection_is_terminal_and_does_not_execute_the_second_callback() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let release = Arc::new(Semaphore::new(0));
        let started = Arc::new(Semaphore::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let mut executor = ToolExecutor::new(1, 8, 1024).unwrap();
        executor
            .register(
                "hold",
                RegisteredTool::new(Arc::new({
                    let release = release.clone();
                    let started = started.clone();
                    let count = count.clone();
                    move |_| {
                        let release = release.clone();
                        let started = started.clone();
                        let count = count.clone();
                        Box::pin(async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            started.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            Ok(json!(true))
                        })
                    }
                })),
            )
            .unwrap();
        let mut worker = Pipeline::new(vec![Arc::new(executor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        send(&worker, call("first", "hold")).await;
        started.acquire().await.unwrap().forget();
        send(&worker, call("second", "hold")).await;
        let rejected = result(&mut worker).await;
        assert_eq!(rejected.tool_call_id, "second");
        assert_eq!(
            rejected.error.as_deref(),
            Some("tool concurrency capacity exceeded")
        );
        assert!(rejected.is_final);
        release.add_permits(1);
        assert_eq!(result(&mut worker).await.tool_call_id, "first");
        assert_eq!(count.load(Ordering::SeqCst), 1);
        worker.end().await.unwrap();
        assert!(worker.collect().await.unwrap().1.errors.is_empty());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn active_call_is_not_reexecuted_after_recent_id_window_is_evicted() {
    tokio::time::timeout(Duration::from_secs(3), async {
        let release = Arc::new(Semaphore::new(0));
        let started = Arc::new(Semaphore::new(0));
        let count = Arc::new(AtomicUsize::new(0));
        let mut executor = ToolExecutor::new(1, 1, 1024).unwrap();
        executor
            .register(
                "hold",
                RegisteredTool::new(Arc::new({
                    let release = release.clone();
                    let started = started.clone();
                    let count = count.clone();
                    move |_| {
                        let release = release.clone();
                        let started = started.clone();
                        let count = count.clone();
                        Box::pin(async move {
                            count.fetch_add(1, Ordering::SeqCst);
                            started.add_permits(1);
                            release.acquire().await.unwrap().forget();
                            Ok(json!(true))
                        })
                    }
                })),
            )
            .unwrap();
        let mut worker = Pipeline::new(vec![Arc::new(executor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        send(&worker, call("active", "hold")).await;
        started.acquire().await.unwrap().forget();
        for i in 0..10 {
            send(&worker, call(&format!("unknown-{i}"), "unknown")).await;
            assert_eq!(
                result(&mut worker).await.error.as_deref(),
                Some("tool is not registered")
            );
            send(&worker, call("active", "hold")).await;
        }
        let fence = Frame::new(FrameData::Flush(FlushFrame {
            origin: Some("admission-fence".into()),
            returning: false,
        }));
        let id = fence.id;
        send(&worker, fence).await;
        loop {
            let frame = worker.recv().await.unwrap();
            assert!(
                !matches!(frame.frame.data, FrameData::FunctionCallResult(_)),
                "an active duplicate must not yield a second result"
            );
            if frame.frame.id == id {
                break;
            }
        }
        release.add_permits(1);
        assert_eq!(result(&mut worker).await.tool_call_id, "active");
        worker.end().await.unwrap();
        let (frames, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
        assert!(!frames
            .iter()
            .any(|frame| matches!(frame.frame.data, FrameData::FunctionCallResult(_))));
        assert_eq!(count.load(Ordering::SeqCst), 1);
    })
    .await
    .unwrap();
}
