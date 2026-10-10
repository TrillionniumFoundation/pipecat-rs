//! Independent generated-speech admission observations. No Spoken-history or
//! device-audibility attestation is inferred from these local observations.
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::{OutputBackend, PlaybackReceipt};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{mpsc, oneshot};

pub const JOURNAL_CAPACITY: usize = 64;

/// Opaque native identity and wire-visible fields are separate assertions:
/// serializing and rebuilding an otherwise equal frame must not pass this probe.
pub struct Original {
    pub frame: Frame,
    wire: Value,
    token: FrameObservationToken,
    tag: LocalFrameTag,
}
impl Original {
    pub fn new(data: FrameData, ordinal: usize) -> Self {
        let mut frame = Frame::new(data);
        frame.pts = Some(40_000 + ordinal as u64);
        frame.metadata.insert(
            "review-original".into(),
            json!({"ordinal": ordinal, "nested": ["preserve", {"value": 17}]}),
        );
        frame.transport_source = Some("synthetic-tts".into());
        // This helper is also used before an OutputTransport. A declared
        // destination is checked in the gate-only probe, and cleared explicitly
        // for the separate default-output receipt case.
        frame.transport_destination = Some("review-destination".into());
        frame.broadcast_sibling_id = Some(80_000 + ordinal as u64);
        let tag = LocalFrameTag::new();
        tag.mark(&mut frame).unwrap();
        let token = frame.observation_token();
        let wire = serde_json::to_value(&frame).unwrap();
        Self {
            frame,
            wire,
            token,
            tag,
        }
    }
    pub fn check(&self, actual: &DirectedFrame) {
        assert_eq!(actual.direction, Direction::Downstream);
        assert_eq!(
            actual.frame.observation_token(),
            self.token,
            "native logical frame replaced"
        );
        assert!(self.tag.contains(&actual.frame), "opaque provenance lost");
        assert_eq!(
            serde_json::to_value(&actual.frame).unwrap(),
            self.wire,
            "original frame fields changed"
        );
    }
}

pub fn generated_response(context: &str, detailed: bool) -> Vec<Original> {
    let start = if detailed {
        FrameData::LlmFullResponseStart(LlmFullResponseFrame::default())
    } else {
        FrameData::LlmResponseStart
    };
    let end = if detailed {
        FrameData::LlmFullResponseEnd(LlmFullResponseFrame::default())
    } else {
        FrameData::LlmResponseEnd
    };
    let mut pcm = AudioRawFrame::new([1_u8, 0].repeat(80), 8000, 1).unwrap();
    pcm.context_id = Some(context.into());
    let text = Original::new(
        FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new("Prepared speech"),
            aggregated_by: "word".into(),
            context_id: Some(context.into()),
            raw_text: Some("<s>Prepared speech</s>".into()),
            will_be_spoken: true,
        }),
        2,
    );
    let segment_id = text.frame.id;
    let mut result = vec![
        Original::new(start, 0),
        Original::new(
            FrameData::TtsStarted(TtsStartedFrame {
                context_id: Some(context.into()),
                append_to_context: true,
            }),
            1,
        ),
    ];
    // Retain the aggregate's exact identity as the diagnostic's anchor.
    result.push(text);
    result.push(Original::new(
        FrameData::TtsAlignmentDiagnostic(TtsAlignmentDiagnosticFrame {
            context_id: context.into(),
            segment_id: Some(segment_id),
            evidence: TtsAlignmentEvidence::Resynchronized,
            synthesis_text: "Prepared speech".into(),
            original_text: Some("<s>Prepared speech</s>".into()),
            source_span: Some(TtsSourceSpan { start: 3, end: 18 }),
            display_position: Some(0),
            synthesis_outcome: TtsSynthesisOutcome::Succeeded,
        }),
        3,
    ));
    result.push(Original::new(
        FrameData::AggregatedTextProgress(AggregatedTextProgressFrame {
            segment_id,
            context_id: Some(context.into()),
            text: "Prepared".into(),
            aggregated_by: "word".into(),
            accumulated_text: "Prepared".into(),
            remaining_text: " speech".into(),
        }),
        4,
    ));
    result.push(Original::new(FrameData::TtsAudio(pcm), 5));
    result.push(Original::new(
        FrameData::TtsStopped(TtsStoppedFrame {
            context_id: Some(context.into()),
        }),
        6,
    ));
    result.push(Original::new(end, 7));
    result
}

pub struct AdmissionJournal {
    name: &'static str,
    sent: mpsc::Sender<DirectedFrame>,
    pub audio: AtomicUsize,
}

/// One explicit downstream capacity barrier. The test holds the first released
/// original frame while newer generated frames are admitted by the gate.
pub struct BlockFirst {
    pub target: u64,
    pub entered: mpsc::Sender<u64>,
    pub release: std::sync::Mutex<Option<oneshot::Receiver<()>>>,
    pub sent: mpsc::Sender<DirectedFrame>,
}
#[async_trait]
impl FrameProcessor for BlockFirst {
    fn name(&self) -> &str {
        "review-release-barrier"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.id == self.target {
            let release = self
                .release
                .lock()
                .unwrap()
                .take()
                .expect("first frame repeated");
            self.entered
                .try_send(frame.id)
                .expect("bounded first-release barrier");
            release
                .await
                .map_err(|_| ProcessorError::new("review release barrier dropped"))?;
        }
        self.sent
            .try_send(DirectedFrame::new(frame.clone(), direction))
            .expect("bounded release journal");
        ctx.push(frame, direction).await
    }
}
impl AdmissionJournal {
    pub fn new(name: &'static str) -> (Arc<Self>, mpsc::Receiver<DirectedFrame>) {
        let (sent, received) = mpsc::channel(JOURNAL_CAPACITY);
        (
            Arc::new(Self {
                name,
                sent,
                audio: AtomicUsize::new(0),
            }),
            received,
        )
    }
}
#[async_trait]
impl FrameProcessor for AdmissionJournal {
    fn name(&self) -> &str {
        self.name
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::TtsAudio(_)) {
            self.audio.fetch_add(1, Ordering::SeqCst);
        }
        self.sent
            .try_send(DirectedFrame::new(frame.clone(), direction))
            .expect("bounded admission journal");
        ctx.push(frame, direction).await
    }
}

#[derive(Debug)]
pub struct Submission {
    pub frame: Frame,
    pub receipt: oneshot::Sender<PlaybackReceipt>,
}
pub struct Receipts {
    submitted: mpsc::Sender<Submission>,
    pub calls: AtomicUsize,
    pub played: AtomicUsize,
    pub completed: AtomicUsize,
    pub interrupted: AtomicUsize,
    pub stopped: AtomicUsize,
}
impl Receipts {
    pub fn new() -> (Arc<Self>, mpsc::Receiver<Submission>) {
        let (submitted, received) = mpsc::channel(8);
        (
            Arc::new(Self {
                submitted,
                calls: AtomicUsize::new(0),
                played: AtomicUsize::new(0),
                completed: AtomicUsize::new(0),
                interrupted: AtomicUsize::new(0),
                stopped: AtomicUsize::new(0),
            }),
            received,
        )
    }
}
#[async_trait]
impl OutputBackend for Receipts {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let (receipt, received) = oneshot::channel();
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.submitted
            .try_send(Submission {
                frame: frame.clone(),
                receipt,
            })
            .expect("bounded backend submission journal");
        let receipt = received
            .await
            .map_err(|_| ProcessorError::new("synthetic receipt released without result"))?;
        if receipt == PlaybackReceipt::Played {
            self.played.fetch_add(1, Ordering::SeqCst);
        }
        self.completed.fetch_add(1, Ordering::SeqCst);
        Ok(receipt)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        self.interrupted.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
