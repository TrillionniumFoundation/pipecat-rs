//! Independent deterministic transport-admission controls. Included as a child of client.rs.
use super::send_fenced;
use futures_util::Sink;
use pipecat_core::*;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

#[derive(Default)]
struct State {
    ready: AtomicBool,
    flush_ready: AtomicBool,
    ready_polls: AtomicUsize,
    flush_polls: AtomicUsize,
    sends: AtomicUsize,
    items: Mutex<Vec<String>>,
    waker: Mutex<Option<Waker>>,
}
impl State {
    fn wake(&self) {
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
    async fn wait_for(&self, count: &AtomicUsize) {
        tokio::time::timeout(Duration::from_secs(3), async {
            while count.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("controlled sink was never polled");
    }
}
struct ControlledSink(Arc<State>);
impl Sink<String> for ControlledSink {
    type Error = &'static str;
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.ready_polls.fetch_add(1, Ordering::SeqCst);
        *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.0.ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn start_send(self: Pin<&mut Self>, item: String) -> Result<(), Self::Error> {
        self.0.sends.fetch_add(1, Ordering::SeqCst);
        self.0.items.lock().unwrap().push(item);
        Ok(())
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.0.flush_polls.fetch_add(1, Ordering::SeqCst);
        *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
        if self.0.flush_ready.load(Ordering::SeqCst) {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.poll_flush(cx)
    }
}
#[derive(Default)]
struct Capture(Mutex<Option<InvocationFence>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "independent-sink-fence"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "capture") {
            *self.0.lock().unwrap() = Some(ctx.invocation_fence()?);
        }
        ctx.push(frame, direction).await
    }
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn source() -> (PipelineWorker, InvocationFence) {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    worker
        .sender()
        .send(Frame::text("capture"), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Text(_)
    ));
    let fence = capture.0.lock().unwrap().take().unwrap();
    (worker, fence)
}
async fn interrupt(worker: &mut PipelineWorker) {
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(worker).await.frame.data,
        FrameData::Interruption
    ));
}
async fn close(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test]
async fn review_pending_sink_readiness_is_not_admission_and_stale_inference_never_starts() {
    let (mut worker, fence) = source().await;
    let state = Arc::new(State::default());
    let operation_state = state.clone();
    let operation = tokio::spawn(async move {
        let mut sink = ControlledSink(operation_state);
        send_fenced(&mut sink, "response.create".to_owned(), Some(&fence)).await
    });
    state.wait_for(&state.ready_polls).await;
    assert_eq!(state.sends.load(Ordering::SeqCst), 0);
    interrupt(&mut worker).await;
    state.ready.store(true, Ordering::SeqCst);
    state.flush_ready.store(true, Ordering::SeqCst);
    state.wake();
    assert!(!tokio::time::timeout(Duration::from_secs(3), operation)
        .await
        .unwrap()
        .unwrap()
        .unwrap());
    assert_eq!(
        state.sends.load(Ordering::SeqCst),
        0,
        "first Pending poll was incorrectly treated as socket admission"
    );
    assert_eq!(state.flush_polls.load(Ordering::SeqCst), 0);
    close(worker).await;
}
#[tokio::test]
async fn review_admitted_write_finishes_pending_flush_once_after_interruption() {
    let (mut worker, fence) = source().await;
    let state = Arc::new(State::default());
    state.ready.store(true, Ordering::SeqCst);
    let operation_state = state.clone();
    let operation = tokio::spawn(async move {
        let mut sink = ControlledSink(operation_state);
        send_fenced(&mut sink, "response.create".to_owned(), Some(&fence)).await
    });
    state.wait_for(&state.flush_polls).await;
    assert_eq!(state.sends.load(Ordering::SeqCst), 1);
    interrupt(&mut worker).await;
    state.flush_ready.store(true, Ordering::SeqCst);
    state.wake();
    assert!(tokio::time::timeout(Duration::from_secs(3), operation)
        .await
        .unwrap()
        .unwrap()
        .unwrap());
    assert_eq!(&*state.items.lock().unwrap(), &["response.create"]);
    assert_eq!(
        state.ready_polls.load(Ordering::SeqCst),
        1,
        "admitted frame retried readiness/start_send"
    );
    close(worker).await;
}
#[tokio::test]
async fn review_pending_flush_is_timeout_bounded_without_resending_ambiguous_frame() {
    let (worker, fence) = source().await;
    let state = Arc::new(State::default());
    state.ready.store(true, Ordering::SeqCst);
    let mut sink = ControlledSink(state.clone());
    let result = tokio::time::timeout(
        Duration::from_millis(25),
        send_fenced(&mut sink, "response.create".to_owned(), Some(&fence)),
    )
    .await;
    assert!(result.is_err());
    assert_eq!(state.sends.load(Ordering::SeqCst), 1);
    assert!(state.flush_polls.load(Ordering::SeqCst) > 0);
    assert_eq!(&*state.items.lock().unwrap(), &["response.create"]);
    close(worker).await;
}
