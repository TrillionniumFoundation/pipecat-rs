use pipecat_classifiers::*;
use pipecat_core::*;
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

pub const LIMIT: Duration = Duration::from_secs(5);
pub async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("independent harness deadline")
}

#[derive(Debug)]
pub struct Requested {
    pub state: Value,
    pub questions: Questions,
    pub options: CallOptions,
    pub answer: oneshot::Sender<Result<ClassificationBatch>>,
}
pub struct Scripted {
    pub requested: mpsc::Sender<Requested>,
    pub active: Arc<AtomicUsize>,
    pub maximum: Arc<AtomicUsize>,
    pub dropped: Arc<AtomicUsize>,
}
struct Active {
    active: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl Drop for Active {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::SeqCst);
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Classifier for Scripted {
    async fn ask(
        &self,
        state: &Value,
        questions: &Questions,
        options: CallOptions,
    ) -> Result<ClassificationBatch> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let _active = Active {
            active: self.active.clone(),
            dropped: self.dropped.clone(),
        };
        let (send, recv) = oneshot::channel();
        self.requested
            .try_send(Requested {
                state: state.clone(),
                questions: questions.clone(),
                options,
                answer: send,
            })
            .expect("bounded classifier request log");
        // Deliberately ignore CallOptions: the detector must enforce its own bound.
        recv.await.map_err(|_| ClassifierError::Cancelled)?
    }
}
pub fn scripted() -> (Arc<Scripted>, mpsc::Receiver<Requested>) {
    let (requested, requests) = mpsc::channel(8);
    (
        Arc::new(Scripted {
            requested,
            active: Arc::new(AtomicUsize::new(0)),
            maximum: Arc::new(AtomicUsize::new(0)),
            dropped: Arc::new(AtomicUsize::new(0)),
        }),
        requests,
    )
}
pub fn batch(label: &str, confidence: f64) -> ClassificationBatch {
    ClassificationBatch {
        answers: BTreeMap::from([(
            "voicemail".into(),
            ClassifierResult::Choice(ChoiceResult {
                choice: label.into(),
                probabilities: BTreeMap::from([(label.into(), confidence)]),
                confidence,
            }),
        )]),
        metrics: ClassificationMetrics {
            classifier: "independent-scripted".into(),
            model: None,
            elapsed: Duration::from_millis(1),
            usage: None,
            provenance: ProbabilityProvenance::LlmEstimate,
        },
    }
}
pub fn transcript(text: &str) -> Frame {
    Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        ..Default::default()
    }))
}
pub fn tts(text: &str) -> Frame {
    Frame::new(FrameData::TtsText(AggregatedTextFrame {
        text: TextFrame::new(text),
        aggregated_by: "sentence".into(),
        context_id: None,
        raw_text: None,
        will_be_spoken: true,
    }))
}

/// Acknowledges completion of the actual processor call, rather than guessing
/// from task yields or send-queue admission. Journal overflow fails the test.
pub struct Acknowledged {
    pub inner: Arc<dyn FrameProcessor>,
    pub acknowledgments: mpsc::Sender<u64>,
    pub contexts: Mutex<VecDeque<(u64, ProcessorContext)>>,
}
#[async_trait]
impl FrameProcessor for Acknowledged {
    fn name(&self) -> &str {
        self.inner.name()
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        let id = frame.id;
        let result = self.inner.process(frame, direction, ctx).await;
        let mut contexts = self.contexts.lock().unwrap();
        if contexts.len() == 4 {
            contexts.pop_front();
        }
        contexts.push_back((id, ctx.clone()));
        self.acknowledgments
            .try_send(id)
            .expect("bounded admission journal");
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> std::result::Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> std::result::Result<(), ProcessorError> {
        self.contexts.lock().unwrap().clear();
        self.inner.cleanup().await
    }
}
pub fn acknowledged(inner: Arc<dyn FrameProcessor>) -> (Arc<Acknowledged>, mpsc::Receiver<u64>) {
    let (acknowledgments, recv) = mpsc::channel(64);
    (
        Arc::new(Acknowledged {
            inner,
            acknowledgments,
            contexts: Mutex::new(VecDeque::new()),
        }),
        recv,
    )
}
pub async fn ack(recv: &mut mpsc::Receiver<u64>, id: u64) {
    loop {
        if bounded(recv.recv()).await.expect("ack channel closed") == id {
            break;
        }
    }
}
pub async fn receive(worker: &mut PipelineWorker) -> DirectedFrame {
    bounded(worker.recv()).await.expect("worker output closed")
}

/// Finite output retention even if a faulty processor produces a runaway stream.
pub async fn collect_bounded(
    mut worker: PipelineWorker,
) -> std::result::Result<(Vec<DirectedFrame>, WorkerReport), PipelineError> {
    bounded(async {
        let mut frames = Vec::new();
        while let Some(frame) = worker.recv().await {
            assert!(frames.len() < 128, "independent output journal capacity");
            frames.push(frame);
        }
        let report = worker.join().await?;
        Ok((frames, report))
    })
    .await
}
