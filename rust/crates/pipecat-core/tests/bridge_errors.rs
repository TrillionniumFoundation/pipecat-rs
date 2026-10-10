use pipecat_core::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

struct ErrorRecorder {
    consume: bool,
    setup: Mutex<Option<ProcessorContext>>,
    seen: Mutex<Vec<(Frame, ProcessorContext)>>,
    received: Semaphore,
}
impl ErrorRecorder {
    fn new(consume: bool) -> Self {
        Self {
            consume,
            setup: Mutex::new(None),
            seen: Mutex::new(vec![]),
            received: Semaphore::new(0),
        }
    }
    fn context(&self) -> ProcessorContext {
        self.setup.lock().unwrap().as_ref().unwrap().clone()
    }
    async fn recorded(&self) -> (Frame, ProcessorContext) {
        tokio::time::timeout(Duration::from_secs(2), self.received.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        self.seen.lock().unwrap().last().unwrap().clone()
    }
}
#[async_trait]
impl FrameProcessor for ErrorRecorder {
    fn name(&self) -> &str {
        "error-recorder"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.setup.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.as_error_frame().is_some() {
            self.seen.lock().unwrap().push((frame.clone(), ctx.clone()));
            self.received.add_permits(1);
            if self.consume {
                return Ok(());
            }
        }
        ctx.push(frame, direction).await
    }
}
struct ProduceError;
#[async_trait]
impl FrameProcessor for ProduceError {
    fn name(&self) -> &str {
        "produce-error"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            return ctx.push_error(ErrorFrame::new("branch failure")).await;
        }
        ctx.push(frame, direction).await
    }
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn finish(worker: PipelineWorker) -> WorkerReport {
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap()
        .1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_bridged_errors_record_once_per_runtime_despite_consumption_bounce_and_eviction() {
    for capacity in [0, 1] {
        let child_recorder = Arc::new(ErrorRecorder::new(false));
        let parent_recorder = Arc::new(ErrorRecorder::new(true));
        let parent_anchor = Arc::new(ErrorRecorder::new(false));
        let config = PipelineConfig {
            error_history_capacity: capacity,
            ..PipelineConfig::default()
        };
        let mut child = Pipeline::new(vec![child_recorder.clone(), Arc::new(ProduceError)])
            .start(config.clone())
            .await
            .unwrap();
        let mut parent = Pipeline::new(vec![parent_recorder.clone(), parent_anchor.clone()])
            .start(config)
            .await
            .unwrap();
        next(&mut child).await;
        next(&mut parent).await;
        child
            .sender()
            .send(Frame::text("fail"), Direction::Downstream)
            .await
            .unwrap();
        let emitted = next(&mut child).await.frame;
        let (original, origin) = child_recorder.recorded().await;
        assert_eq!(emitted, original);
        assert_eq!(child.total_errors(), 1);
        assert_eq!(parent.total_errors(), 0);

        let relay = parent_anchor.context().forwarding_context(&origin);
        relay
            .push(original.clone(), Direction::Upstream)
            .await
            .unwrap();
        let (consumed, parent_origin) = parent_recorder.recorded().await;
        assert_eq!(consumed, original);
        assert_eq!(parent.total_errors(), 1);
        assert!(
            parent.try_recv().is_err(),
            "parent handler consumed the error"
        );

        let bounce = child_recorder.context().forwarding_context(&parent_origin);
        bounce.push(consumed, Direction::Upstream).await.unwrap();
        assert_eq!(next(&mut child).await.frame, original);
        assert_eq!(child.total_errors(), 1);

        child_recorder
            .context()
            .push_error(ErrorFrame::new("second child error"))
            .await
            .unwrap();
        next(&mut child).await;
        parent_anchor
            .context()
            .push_error(ErrorFrame::new("second parent error"))
            .await
            .unwrap();
        parent_recorder.recorded().await;
        assert_eq!(child.total_errors(), 2);
        assert_eq!(parent.total_errors(), 2);
        assert_eq!(child.errors().len(), capacity);
        assert_eq!(parent.errors().len(), capacity);
        assert_eq!(child.evicted_errors(), 2 - capacity as u64);
        assert_eq!(parent.evicted_errors(), 2 - capacity as u64);

        // Replays of the retained native clone must not resurrect an evicted
        // history entry or increase counters in either already-visited runtime.
        relay
            .push(original.clone(), Direction::Upstream)
            .await
            .unwrap();
        parent_recorder.recorded().await;
        child
            .sender()
            .try_send_from(original.clone(), Direction::Upstream, &parent_origin)
            .unwrap();
        child_recorder.recorded().await;
        assert_eq!(next(&mut child).await.frame, original);
        assert_eq!(child.total_errors(), 2);
        assert_eq!(parent.total_errors(), 2);
        assert_eq!(child.evicted_errors(), 2 - capacity as u64);
        assert_eq!(parent.evicted_errors(), 2 - capacity as u64);

        child_recorder
            .context()
            .push(original.clone(), Direction::Upstream)
            .await
            .unwrap();
        assert_eq!(next(&mut child).await.frame, original);
        assert_eq!(child.total_errors(), 2);
        let serialized = serde_json::to_value(&original).unwrap();
        assert!(serialized.get("reported_runtimes").is_none());

        let duplicate = original.duplicate();
        assert_ne!(duplicate.id, original.id);
        assert_eq!(duplicate.data, original.data);
        relay
            .push(duplicate.clone(), Direction::Upstream)
            .await
            .unwrap();
        let (new_error, _) = parent_recorder.recorded().await;
        assert_eq!(new_error, duplicate);
        bounce
            .push(duplicate.clone(), Direction::Upstream)
            .await
            .unwrap();
        assert_eq!(next(&mut child).await.frame, duplicate);
        assert_eq!(child.total_errors(), 3);
        assert_eq!(parent.total_errors(), 3);
        assert_eq!(child.evicted_errors(), 3 - capacity as u64);
        assert_eq!(parent.evicted_errors(), 3 - capacity as u64);

        let child_report = finish(child).await;
        let parent_report = finish(parent).await;
        assert_eq!(child_report.total_errors, 3);
        assert_eq!(parent_report.total_errors, 3);
        assert_eq!(child_report.errors.len(), capacity);
        assert_eq!(parent_report.errors.len(), capacity);
    }
}
