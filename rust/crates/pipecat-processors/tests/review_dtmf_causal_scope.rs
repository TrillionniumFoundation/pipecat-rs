//! Independent regression fixtures for retained keypad invocation authority.
use pipecat_core::*;
use pipecat_processors::{DtmfAggregator, DtmfAggregatorConfig};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Capture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "ReviewCapture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.0.lock().unwrap() = Some(ctx.clone());
        ctx.push(frame, direction).await
    }
}
async fn origin() -> (PipelineWorker, ProcessorContext) {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Start(_)
    ));
    let context = capture.0.lock().unwrap().as_ref().unwrap().clone();
    (worker, context)
}
type InputGate = (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>);
struct RetainedInput {
    aggregator: DtmfAggregator,
    context: Mutex<Option<ProcessorContext>>,
    retained: Mutex<Option<ProcessorContext>>,
    completed: AtomicUsize,
    gate: Mutex<Option<InputGate>>,
}
impl RetainedInput {
    fn new(config: DtmfAggregatorConfig) -> Self {
        Self {
            aggregator: DtmfAggregator::new(config).unwrap(),
            context: Mutex::new(None),
            retained: Mutex::new(None),
            completed: AtomicUsize::new(0),
            gate: Mutex::new(None),
        }
    }
    fn destination(&self) -> ProcessorContext {
        self.context.lock().unwrap().as_ref().unwrap().clone()
    }
    fn retain(&self, ctx: &ProcessorContext) {
        *self.retained.lock().unwrap() = Some(ctx.clone());
    }
}
#[async_trait]
impl FrameProcessor for RetainedInput {
    fn name(&self) -> &str {
        "ReviewRetainedDtmfInput"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.aggregator.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        let gate = if matches!(frame.data, FrameData::InputDtmf(_)) {
            self.gate.lock().unwrap().take()
        } else {
            None
        };
        if let Some((entered, release)) = gate {
            entered.notify_one();
            release.notified().await;
        }
        let selected = if matches!(frame.data, FrameData::InputDtmf(_)) {
            self.retained.lock().unwrap().clone()
        } else {
            None
        };
        let result = self
            .aggregator
            .process(frame, direction, selected.as_ref().unwrap_or(ctx))
            .await;
        self.completed.fetch_add(1, Ordering::SeqCst);
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.aggregator.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.aggregator.cleanup().await
    }
}
async fn start(config: DtmfAggregatorConfig) -> (PipelineWorker, Arc<RetainedInput>) {
    let processor = Arc::new(RetainedInput::new(config));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(Default::default())
        .await
        .unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Start(_)
    ));
    (worker, processor)
}
async fn digit(worker: &PipelineWorker, processor: &RetainedInput, value: KeypadEntry) {
    let before = processor.completed.load(Ordering::SeqCst);
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputDtmf(value)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while processor.completed.load(Ordering::SeqCst) == before {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("digit processing should complete");
}
fn drain(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let mut frames = vec![];
    while let Ok(frame) = worker.try_recv() {
        frames.push(frame);
    }
    frames
}
fn transcripts(frames: &[DirectedFrame]) -> Vec<String> {
    frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::TranscriptionDetailed(t) => Some(t.text.text.clone()),
            _ => None,
        })
        .collect()
}
async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

#[tokio::test]
async fn review_old_origin_cannot_borrow_unrelated_current_bridge() {
    let (origin_a_worker, origin_a) = origin().await;
    let (mut origin_b_worker, origin_b) = origin().await;
    let (mut worker, processor) = start(Default::default()).await;
    let a = processor.destination().forwarding_context(&origin_a);
    processor.retain(&a);
    digit(&worker, &processor, KeypadEntry::One).await;
    let first = drain(&mut worker);
    let interruption = first
        .iter()
        .find(|item| {
            item.direction == Direction::Downstream
                && matches!(item.frame.data, FrameData::Interruption)
        })
        .unwrap()
        .frame
        .clone();
    assert!(origin_b
        .broadcast_interruption_frame(&interruption)
        .await
        .unwrap());
    drain(&mut origin_b_worker);
    let b = processor.destination().forwarding_context(&origin_b);
    assert!(b.invocation_fence().unwrap().is_current());
    processor.retain(&b);
    digit(&worker, &processor, KeypadEntry::Two).await;
    assert_eq!(processor.aggregator.buffered_len().await, 1);
    // A has now also lost its inherited origin; B stays current and unrelated.
    origin_a.broadcast_interruption().await.unwrap();
    assert!(!a.invocation_fence().unwrap().is_current());
    processor.retain(&a);
    digit(&worker, &processor, KeypadEntry::Nine).await;
    processor.retain(&b);
    digit(&worker, &processor, KeypadEntry::Pound).await;
    let output = drain(&mut worker);
    assert_eq!(
        transcripts(&output),
        ["DTMF: 2#"],
        "stale A must not append to B's admitted entry"
    );
    finish(worker).await;
    finish(origin_a_worker).await;
    finish(origin_b_worker).await;
}

#[tokio::test(start_paused = true)]
async fn review_completed_entry_does_not_grant_permanent_scope_renewal() {
    let config = DtmfAggregatorConfig {
        idle_timeout: Duration::from_secs(1),
        max_sequence_duration: Duration::from_secs(2),
        ..Default::default()
    };
    let (mut worker, processor) = start(config).await;
    let old = processor.destination();
    processor.retain(&old);
    digit(&worker, &processor, KeypadEntry::Pound).await;
    assert_eq!(transcripts(&drain(&mut worker)), ["DTMF: #"]);
    tokio::time::advance(Duration::from_secs(3)).await;
    // The same pre-interruption authority is older than the hard lifetime.
    digit(&worker, &processor, KeypadEntry::Pound).await;
    assert!(
        transcripts(&drain(&mut worker)).is_empty(),
        "completed entry renewed an indefinitely retained stale scope"
    );
    finish(worker).await;
}

#[tokio::test]
async fn review_invalid_stale_key_cannot_clear_current_entry() {
    let (mut worker, processor) = start(Default::default()).await;
    let old = processor.destination();
    processor.retain(&old);
    digit(&worker, &processor, KeypadEntry::One).await;
    drain(&mut worker);
    // Invalidate the saved pre-entry context with an independent interruption.
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    loop {
        if matches!(
            worker.recv().await.unwrap().frame.data,
            FrameData::Interruption
        ) {
            break;
        }
    }
    let fresh = processor.destination();
    processor.retain(&fresh);
    digit(&worker, &processor, KeypadEntry::Two).await;
    processor.retain(&old);
    digit(&worker, &processor, KeypadEntry::A).await;
    assert_eq!(
        processor.aggregator.buffered_len().await,
        1,
        "unadmitted stale input must not mutate a current entry"
    );
    processor.retain(&fresh);
    digit(&worker, &processor, KeypadEntry::Pound).await;
    assert_eq!(transcripts(&drain(&mut worker)), ["DTMF: 2#"]);
    finish(worker).await;
}

#[tokio::test]
async fn review_external_interruption_fences_input_before_its_callback() {
    let (mut worker, processor) = start(Default::default()).await;
    processor.retain(&processor.destination());
    digit(&worker, &processor, KeypadEntry::One).await;
    drain(&mut worker);
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    *processor.gate.lock().unwrap() = Some((entered.clone(), release.clone()));
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputDtmf(KeypadEntry::Pound)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    // The DTMF callback is held, so the following interruption cannot yet run.
    // Queue admission has nevertheless already invalidated the local epoch.
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    release.notify_one();
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        transcripts(&frames).is_empty(),
        "pending callback revived input invalidated at queue admission"
    );
}

#[tokio::test(start_paused = true)]
async fn review_inherited_session_close_fences_owned_timer() {
    let (origin_worker, origin) = origin().await;
    let (mut worker, processor) = start(DtmfAggregatorConfig {
        idle_timeout: Duration::from_millis(50),
        ..Default::default()
    })
    .await;
    let inherited = processor.destination().forwarding_context(&origin);
    processor.retain(&inherited);
    digit(&worker, &processor, KeypadEntry::One).await;
    drain(&mut worker);
    finish(origin_worker).await;
    assert!(!inherited.invocation_fence().unwrap().is_current());
    tokio::time::advance(Duration::from_millis(100)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(
        transcripts(&drain(&mut worker)).is_empty(),
        "timer emitted after the inherited owner closed"
    );
    assert_eq!(processor.aggregator.buffered_len().await, 0);
    finish(worker).await;
}
