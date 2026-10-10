//! Capacity-wait contracts for owned downstream work. No model or device needed.
use pipecat_core::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::Semaphore;
async fn wait(gate: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(2), gate.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn collect(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap()
}
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
struct Burst {
    admitted: Arc<Semaphore>,
    ended: Arc<AtomicBool>,
    interruptible: bool,
    count: usize,
}
#[async_trait]
impl FrameProcessor for Burst {
    fn name(&self) -> &str {
        "Burst"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            let output = ctx.clone();
            let admitted = self.admitted.clone();
            let guard = Dropped(self.ended.clone());
            let count = self.count;
            ctx.spawn("bounded-burst", self.interruptible, async move {
                let _guard = guard;
                for n in 0..count {
                    output
                        .push_downstream_wait(Frame::text(n.to_string()))
                        .await?;
                    admitted.add_permits(1);
                }
                Ok(())
            })?;
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
struct Gate {
    entered: Semaphore,
    release: Semaphore,
    once: AtomicBool,
}
#[async_trait]
impl FrameProcessor for Gate {
    fn name(&self) -> &str {
        "Gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) && !self.once.swap(true, Ordering::SeqCst) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}
fn burst(interruptible: bool, count: usize) -> Arc<Burst> {
    Arc::new(Burst {
        admitted: Arc::new(Semaphore::new(0)),
        ended: Arc::new(AtomicBool::new(false)),
        interruptible,
        count,
    })
}
fn gate() -> Arc<Gate> {
    Arc::new(Gate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        once: AtomicBool::new(false),
    })
}
fn config() -> PipelineConfig {
    PipelineConfig {
        queue_capacity: 1,
        output_capacity: 32,
        ..Default::default()
    }
}
#[tokio::test]
async fn adjacent_capacity_wait_preserves_frames_and_owned_cancel_wakes_it() {
    for cancel in [false, true] {
        let burst = burst(true, 3);
        let sink = gate();
        let worker = Pipeline::new(vec![burst.clone(), sink.clone()])
            .start(config())
            .await
            .unwrap();
        send(&worker, Frame::text("go")).await;
        wait(&sink.entered).await;
        wait(&burst.admitted).await;
        wait(&burst.admitted).await;
        assert!(!burst.ended.load(Ordering::SeqCst));
        if cancel {
            worker.cancel("full lane").await.unwrap();
        } else {
            sink.release.add_permits(1);
            wait(&burst.admitted).await;
            worker.end().await.unwrap();
        }
        let (frames, report) = collect(worker).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(burst.ended.load(Ordering::SeqCst));
        if !cancel {
            let text: Vec<_> = frames
                .into_iter()
                .filter_map(|f| {
                    if let FrameData::Text(text) = f.frame.data {
                        Some(text.text)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(text, ["0", "1", "2"]);
        }
    }
}
#[tokio::test]
async fn worker_output_capacity_is_reserved_without_holding_admission_lock() {
    let burst = burst(true, 3);
    let mut cfg = config();
    cfg.output_capacity = 1;
    let mut worker = Pipeline::new(vec![burst.clone()]).start(cfg).await.unwrap();
    worker.recv().await.unwrap(); // Start occupies the only output slot.
    send(&worker, Frame::text("go")).await;
    wait(&burst.admitted).await;
    assert!(!burst.ended.load(Ordering::SeqCst));
    let first = worker.recv().await.unwrap();
    assert!(matches!(&first.frame.data, FrameData::Text(text) if text.text == "0"));
    worker.end().await.unwrap();
    let (frames, report) = collect(worker).await;
    assert!(report.errors.is_empty());
    let text: Vec<_> = frames
        .into_iter()
        .filter_map(|f| {
            if let FrameData::Text(text) = f.frame.data {
                Some(text.text)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(text, ["1", "2"]);
}
#[tokio::test]
async fn uninterruptible_owned_task_survives_interruption_while_capacity_waiting() {
    let burst = burst(false, 3);
    let sink = gate();
    let worker = Pipeline::new(vec![burst.clone(), sink.clone()])
        .start(config())
        .await
        .unwrap();
    send(&worker, Frame::text("go")).await;
    wait(&sink.entered).await;
    wait(&burst.admitted).await;
    wait(&burst.admitted).await;
    send(&worker, Frame::new(FrameData::Interruption)).await;
    sink.release.add_permits(1);
    wait(&burst.admitted).await;
    worker.end().await.unwrap();
    let (_, report) = collect(worker).await;
    assert!(report.errors.is_empty());
    assert!(burst.ended.load(Ordering::SeqCst));
}
#[derive(Default)]
struct Observer(Mutex<Vec<ObservationEvent>>);
#[async_trait]
impl PipelineObserver for Observer {
    async fn on_event(&self, event: ObservationEvent) {
        self.0.lock().unwrap().push(event);
    }
}
#[tokio::test]
async fn full_attempts_do_not_duplicate_push_observations() {
    let burst = burst(true, 3);
    let sink = gate();
    let observer = Arc::new(Observer::default());
    let worker = Pipeline::new(vec![burst.clone(), sink.clone()])
        .with_observer(observer.clone(), ObserverOptions::default())
        .start(config())
        .await
        .unwrap();
    send(&worker, Frame::text("go")).await;
    wait(&sink.entered).await;
    wait(&burst.admitted).await;
    wait(&burst.admitted).await;
    sink.release.add_permits(1);
    wait(&burst.admitted).await;
    worker.end().await.unwrap();
    collect(worker).await;
    let events = observer.0.lock().unwrap();
    let ids: Vec<_> = events
        .iter()
        .filter_map(|event| match event.observation.as_ref() {
            PipelineObservation::Push { source, frame, .. }
                if source.name.as_ref() == "Burst" && frame.kind == "TextFrame" =>
            {
                Some(frame.id)
            }
            _ => None,
        })
        .collect();
    assert_eq!(ids.len(), 3);
    assert_eq!(
        ids.iter().collect::<std::collections::BTreeSet<_>>().len(),
        3
    );
}
struct Rejections(Arc<AtomicUsize>);
#[async_trait]
impl FrameProcessor for Rejections {
    fn name(&self) -> &str {
        "Rejections"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            assert!(ctx.push_downstream_wait(Frame::text("sync")).await.is_err());
            assert!(ctx
                .push_downstream_flush_wait(Frame::new(FrameData::Flush(FlushFrame {
                    returning: true,
                    origin: None
                })))
                .await
                .is_err());
            assert!(ctx
                .push_downstream_terminal_wait(Frame::end())
                .await
                .is_err());
            let output = ctx.clone();
            let count = self.0.clone();
            ctx.spawn("reject-controls", false, async move {
                for data in [
                    FrameData::End(EndFrame::default()),
                    FrameData::Interruption,
                    FrameData::Start(StartFrame::default()),
                    FrameData::Cancel(CancelFrame::default()),
                    FrameData::Error(ErrorFrame::new("x")),
                    FrameData::StopWorker,
                ] {
                    assert!(output.push_downstream_wait(Frame::new(data)).await.is_err());
                    count.fetch_add(1, Ordering::SeqCst);
                }
                assert!(!output.has_downstream_processor());
                assert!(output
                    .push_downstream_flush_wait(Frame::new(FrameData::Flush(FlushFrame {
                        returning: true,
                        origin: None
                    })))
                    .await
                    .is_err());
                assert!(output
                    .push_downstream_terminal_wait(Frame::end())
                    .await
                    .is_err());
                Ok(())
            })?;
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn synchronous_special_and_final_control_routes_are_rejected() {
    let rejected = Arc::new(AtomicUsize::new(0));
    let worker = Pipeline::new(vec![Arc::new(Rejections(rejected.clone()))])
        .start(config())
        .await
        .unwrap();
    send(&worker, Frame::text("go")).await;
    worker.end().await.unwrap();
    let (_, report) = collect(worker).await;
    assert!(report.errors.is_empty());
    assert_eq!(rejected.load(Ordering::SeqCst), 6);
}

struct Capture {
    context: Mutex<Option<ProcessorContext>>,
    seen: Semaphore,
}
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "Capture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.context.lock().unwrap() = Some(ctx.clone());
            self.seen.add_permits(1);
        }
        ctx.push(frame, direction).await
    }
}
async fn capture() -> (PipelineWorker, ProcessorContext) {
    let capture = Arc::new(Capture {
        context: Mutex::new(None),
        seen: Semaphore::new(0),
    });
    let mut cfg = config();
    cfg.output_capacity = 1;
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(cfg)
        .await
        .unwrap();
    worker.recv().await.unwrap();
    send(&worker, Frame::text("capture")).await;
    wait(&capture.seen).await;
    worker.recv().await.unwrap();
    let ctx = capture.context.lock().unwrap().clone().unwrap();
    (worker, ctx)
}
#[tokio::test]
async fn pending_output_wait_wakes_for_each_inherited_origin_and_keeps_uninterruptible_frame() {
    for interrupted in [0, 1] {
        for uninterruptible in [false, true] {
            let (mut source, source_ctx) = capture().await;
            let (mut middle, middle_ctx) = capture().await;
            let (mut destination, destination_ctx) = capture().await;
            destination_ctx
                .push(Frame::text("occupy"), Direction::Downstream)
                .await
                .unwrap();
            let forwarded =
                destination_ctx.forwarding_context(&middle_ctx.forwarding_context(&source_ctx));
            let started = Arc::new(Semaphore::new(0));
            let done = Arc::new(Semaphore::new(0));
            let begun = started.clone();
            let completed = done.clone();
            let output = forwarded.clone();
            forwarded
                .spawn("inherited-full-output", true, async move {
                    use std::future::{poll_fn, Future};
                    use std::task::Poll;
                    let mut admission = Box::pin(output.push_downstream_wait(
                        Frame::text("late").with_interruptible(!uninterruptible),
                    ));
                    poll_fn(|cx| match admission.as_mut().poll(cx) {
                        Poll::Pending => Poll::Ready(()),
                        Poll::Ready(result) => panic!("output was not full: {result:?}"),
                    })
                    .await;
                    begun.add_permits(1);
                    admission.await?;
                    completed.add_permits(1);
                    Ok(())
                })
                .unwrap();
            wait(&started).await;
            let origin = if interrupted == 0 {
                &mut source
            } else {
                &mut middle
            };
            send(origin, Frame::interruption()).await;
            origin.recv().await.unwrap();
            if !uninterruptible {
                wait(&done).await;
            }
            let occupied = destination.recv().await.unwrap();
            assert!(matches!(&occupied.frame.data, FrameData::Text(text) if text.text == "occupy"));
            if uninterruptible {
                wait(&done).await;
            }
            destination.end().await.unwrap();
            let (frames, report) = collect(destination).await;
            assert!(report.errors.is_empty());
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::Text(_)))
                    .count(),
                usize::from(uninterruptible)
            );
            source.end().await.unwrap();
            collect(source).await;
            middle.end().await.unwrap();
            collect(middle).await;
        }
    }
}
struct FlushSource {
    done: Arc<Semaphore>,
    waiting: Arc<Semaphore>,
    detailed: bool,
    returning: bool,
}
#[async_trait]
impl FrameProcessor for FlushSource {
    fn name(&self) -> &str {
        "FlushSource"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            let output = ctx.clone();
            let done = self.done.clone();
            let waiting = self.waiting.clone();
            let (detailed, returning) = (self.detailed, self.returning);
            ctx.spawn("flush-stream", false, async move {
                output.push_downstream_wait(Frame::text("hold")).await?;
                output.push_downstream_wait(Frame::text("queued")).await?;
                let data = if detailed {
                    FrameData::FlushDetailed(PipelineFlushFrame {
                        returning,
                        ..Default::default()
                    })
                } else {
                    FrameData::Flush(FlushFrame {
                        returning,
                        origin: None,
                    })
                };
                use std::future::{poll_fn, Future};
                use std::task::Poll;
                let mut flush = Box::pin(output.push_downstream_flush_wait(Frame::new(data)));
                poll_fn(|cx| match flush.as_mut().poll(cx) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(result) => panic!("adjacent lane not full: {result:?}"),
                })
                .await;
                waiting.add_permits(1);
                flush.await?;
                done.add_permits(1);
                Ok(())
            })?;
            Ok(())
        } else if direction == Direction::Downstream
            && matches!(
                frame.data,
                FrameData::Flush(_) | FrameData::FlushDetailed(_)
            )
        {
            let output = ctx.clone();
            ctx.spawn("returning-flush", false, async move {
                output.push_downstream_flush_wait(frame).await
            })?;
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn adjacent_compact_and_detailed_flush_waits_for_capacity_on_both_return_states() {
    for detailed in [false, true] {
        for returning in [false, true] {
            let done = Arc::new(Semaphore::new(0));
            let waiting = Arc::new(Semaphore::new(0));
            let sink = gate();
            let mut worker = Pipeline::new(vec![
                Arc::new(FlushSource {
                    done: done.clone(),
                    waiting: waiting.clone(),
                    detailed,
                    returning,
                }),
                sink.clone(),
            ])
            .start(config())
            .await
            .unwrap();
            send(&worker, Frame::text("go")).await;
            wait(&sink.entered).await;
            wait(&waiting).await;
            assert_eq!(done.available_permits(), 0);
            sink.release.add_permits(1);
            wait(&done).await;
            let mut text = Vec::new();
            tokio::time::timeout(Duration::from_secs(2), async {
                while let Some(out) = worker.recv().await {
                    match out.frame.data {
                        FrameData::Text(t) => text.push(t.text),
                        FrameData::Flush(_) | FrameData::FlushDetailed(_) => break,
                        _ => {}
                    }
                }
            })
            .await
            .unwrap();
            assert_eq!(text, ["hold", "queued"]);
            worker.end().await.unwrap();
            let (_, report) = collect(worker).await;
            assert!(report.errors.is_empty(), "{:?}", report.errors);
        }
    }
}

#[tokio::test]
async fn later_legacy_hop_keeps_explicit_fail_fast_policy() {
    let burst = burst(true, 16);
    let sink = gate();
    let worker = Pipeline::new(vec![burst, Arc::new(IdentityProcessor), sink])
        .start(config())
        .await
        .unwrap();
    send(&worker, Frame::text("go")).await;
    let result = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap();
    assert!(
        matches!(result, Err(PipelineError::Backpressure(_))),
        "bounded first hop must not pretend to retrofit legacy forwarding: {result:?}"
    );
}
