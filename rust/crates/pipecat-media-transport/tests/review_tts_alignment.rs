//! Clock-lane regressions use a shared pipeline timeline and deterministic time.
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

struct Clock(Instant);
impl ObserverClock for Clock {
    fn now_ns(&self) -> u64 {
        9_000_000_000 + self.0.elapsed().as_nanos() as u64
    }
}
#[derive(Default)]
struct Backend(Mutex<Vec<Frame>>);
#[async_trait]
impl OutputBackend for Backend {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.0.lock().unwrap().push(frame.clone());
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
#[derive(Default)]
struct Probe(Mutex<Vec<Frame>>);
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "clock-lane-probe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}
struct Harness {
    worker: PipelineWorker,
    clock: Arc<Clock>,
    control: OutputControl,
    probe: Arc<Probe>,
    backend: Arc<Backend>,
}
fn config() -> OutputTransportConfig {
    OutputTransportConfig {
        sample_rate: Some(8000),
        chunk_duration: Duration::from_millis(10),
        auto_silence: false,
        end_silence: Duration::ZERO,
        ..Default::default()
    }
}
async fn settle() {
    for _ in 0..80 {
        tokio::task::yield_now().await;
    }
}
impl Harness {
    async fn new(config: OutputTransportConfig) -> Self {
        let clock = Arc::new(Clock(Instant::now()));
        let backend = Arc::new(Backend::default());
        let probe = Arc::new(Probe::default());
        let output = Arc::new(
            OutputTransport::new("output", config, backend.clone(), None, BTreeMap::new()).unwrap(),
        );
        let control = output.control();
        let worker = Pipeline::new(vec![output, probe.clone()])
            .with_observer_clock(clock.clone())
            .start(PipelineConfig::default())
            .await
            .unwrap();
        settle().await;
        Self {
            worker,
            clock,
            control,
            probe,
            backend,
        }
    }
    async fn send(&self, frame: Frame) {
        self.worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        settle().await;
    }
    fn word(&self, text: &str, delay: Duration, destination: Option<&str>) -> Frame {
        let mut frame = Frame::new(FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new(text),
            aggregated_by: "word".into(),
            context_id: Some("ctx".into()),
            raw_text: Some(text.into()),
            will_be_spoken: true,
        }));
        frame.set_pipeline_pts(self.clock.now_ns() + delay.as_nanos() as u64);
        frame.transport_destination = destination.map(str::to_owned);
        frame
    }
    fn contains(&self, id: u64) -> bool {
        self.probe.0.lock().unwrap().iter().any(|f| f.id == id)
    }
    async fn finish(self, stop: bool) -> (Vec<Frame>, WorkerReport) {
        if stop {
            self.worker
                .sender()
                .send(Frame::new(FrameData::Stop), Direction::Downstream)
                .await
                .unwrap();
        } else {
            self.worker.sender().end().await.unwrap();
        }
        let (_, report) = self.worker.collect().await.unwrap();
        let frames = self.probe.0.lock().unwrap().clone();
        (frames, report)
    }
}
fn pcm(destination: Option<&str>) -> Frame {
    let mut frame = Frame::new(FrameData::TtsAudio(
        AudioRawFrame::new(vec![1; 160], 8000, 1).unwrap(),
    ));
    frame.transport_destination = destination.map(str::to_owned);
    frame
}
#[tokio::test(start_paused = true)]
async fn pause_holds_due_metadata_then_resume_releases_once() {
    let h = Harness::new(config()).await;
    h.control.pause(None).unwrap();
    let frame = h.word("held", Duration::from_millis(100), None);
    let id = frame.id;
    h.send(frame).await;
    tokio::time::advance(Duration::from_millis(200)).await;
    settle().await;
    assert!(!h.contains(id));
    h.control.resume(None).unwrap();
    settle().await;
    assert!(h.contains(id));
    let (frames, report) = h.finish(false).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(frames.iter().filter(|f| f.id == id).count(), 1);
}
#[tokio::test(start_paused = true)]
async fn interruption_discards_interruptible_timed_metadata_only() {
    let h = Harness::new(config()).await;
    let interrupted = h.word("drop", Duration::from_millis(100), None);
    let drop_id = interrupted.id;
    let kept = h
        .word("keep", Duration::from_millis(100), None)
        .with_interruptible(false);
    let keep_id = kept.id;
    h.send(interrupted).await;
    h.send(kept).await;
    h.send(Frame::new(FrameData::Interruption)).await;
    tokio::time::advance(Duration::from_millis(150)).await;
    settle().await;
    assert!(!h.contains(drop_id));
    assert!(h.contains(keep_id));
    let (_, report) = h.finish(false).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test(start_paused = true)]
async fn future_metadata_retains_queue_count_budget() {
    let mut cfg = config();
    cfg.admission_policy = OutputAdmissionPolicy::FailFast;
    cfg.queue_capacity = 2;
    let h = Harness::new(cfg).await;
    h.send(h.word("one", Duration::from_millis(100), None))
        .await;
    h.send(h.word("two", Duration::from_millis(100), None))
        .await;
    h.send(h.word("three", Duration::from_millis(100), None))
        .await;
    let (_, report) = h.finish(false).await;
    assert!(
        report
            .errors
            .iter()
            .any(|e| e.error.contains("bounded media queue is full")),
        "{:?}",
        report.errors
    );
}
#[tokio::test(start_paused = true)]
async fn future_metadata_retains_queue_text_byte_budget() {
    let mut cfg = config();
    cfg.admission_policy = OutputAdmissionPolicy::FailFast;
    cfg.queue_byte_capacity = 200;
    let h = Harness::new(cfg).await;
    h.send(h.word(&"x".repeat(60), Duration::from_millis(100), None))
        .await;
    h.send(h.word(&"y".repeat(60), Duration::from_millis(100), None))
        .await;
    let (_, report) = h.finish(false).await;
    assert!(
        report
            .errors
            .iter()
            .any(|e| e.error.contains("bounded media queue is full")),
        "{:?}",
        report.errors
    );
}
#[tokio::test(start_paused = true)]
async fn all_destination_end_and_stop_wait_for_future_metadata() {
    for stop in [false, true] {
        let mut cfg = config();
        cfg.destinations = vec!["late".into()];
        let h = Harness::new(cfg).await;
        let a = h.word("default", Duration::from_millis(100), None);
        let a_id = a.id;
        let b = h.word("late", Duration::from_millis(300), Some("late"));
        let b_id = b.id;
        h.send(a).await;
        h.send(b).await;
        let (frames, report) = h.finish(stop).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let terminal = frames.iter().position(Frame::is_terminal).unwrap();
        assert!(frames.iter().position(|f| f.id == a_id).unwrap() < terminal);
        assert!(frames.iter().position(|f| f.id == b_id).unwrap() < terminal);
    }
}
#[tokio::test(start_paused = true)]
async fn all_destination_flush_waits_for_words_and_blocks_later_pcm() {
    let mut cfg = config();
    cfg.destinations = vec!["late".into()];
    let h = Harness::new(cfg).await;
    let a = h.word("default", Duration::from_millis(100), None);
    let a_id = a.id;
    let b = h.word("late", Duration::from_millis(300), Some("late"));
    let b_id = b.id;
    h.send(a).await;
    h.send(b).await;
    let marker = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        origin: Some("pipecat.flows".into()),
        ..Default::default()
    }));
    let marker_id = marker.id;
    h.send(marker).await;
    h.send(pcm(None)).await;
    tokio::time::advance(Duration::from_millis(150)).await;
    settle().await;
    assert!(h.contains(a_id));
    assert!(!h.contains(b_id));
    assert!(!h.contains(marker_id));
    assert!(
        h.backend.0.lock().unwrap().is_empty(),
        "PCM crossed an all-destination flush fence"
    );
    tokio::time::advance(Duration::from_millis(200)).await;
    settle().await;
    assert!(h.contains(b_id));
    assert!(h.contains(marker_id));
    assert!(!h.backend.0.lock().unwrap().is_empty());
    let (frames, report) = h.finish(false).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    for returning in [false, true] {
        assert_eq!(frames.iter().filter(|f| f.id == marker_id && matches!(&f.data, FrameData::FlushDetailed(flush) if flush.returning == returning)).count(), 1);
    }
}
#[tokio::test(start_paused = true)]
async fn overly_future_and_overdue_pts_have_bounded_explicit_handling() {
    let mut cfg = config();
    cfg.max_future_pts = Duration::from_secs(1);
    let h = Harness::new(cfg).await;
    let mut overdue = h.word("overdue", Duration::ZERO, None);
    overdue.set_pipeline_pts(0);
    let id = overdue.id;
    h.send(overdue).await;
    assert_eq!(
        h.probe
            .0
            .lock()
            .unwrap()
            .iter()
            .find(|f| f.id == id)
            .unwrap()
            .pts,
        Some(0)
    );
    h.send(h.word("impossible", Duration::from_secs(2), None))
        .await;
    let (_, report) = h.finish(false).await;
    assert!(
        report
            .errors
            .iter()
            .any(|e| e.error.contains("future horizon")),
        "{:?}",
        report.errors
    );
}

#[tokio::test(start_paused = true)]
async fn timed_routing_and_generated_provenance_remain_inside_byte_budget() {
    for routing in [false, true] {
        let destination = "d".repeat(201);
        let mut cfg = config();
        cfg.queue_byte_capacity = 200;
        if routing {
            cfg.destinations = vec![destination.clone()];
        }
        let h = Harness::new(cfg).await;
        let mut frame = h.word(
            if routing {
                "short"
            } else {
                "a reasonably sized text whose retained copies already use most of this budget"
            },
            Duration::from_millis(100),
            routing.then_some(destination.as_str()),
        );
        if !routing {
            frame.metadata.insert("segment_id".into(), 123.into());
            frame.metadata.insert("source_frame_id".into(), 456.into());
            frame
                .metadata
                .insert("alignment_evidence".into(), "provider_report".into());
            frame
                .metadata
                .insert("audio_offset_seconds".into(), 0.1.into());
        }
        let id = frame.id;
        h.send(frame).await;
        let (frames, report) = h.finish(false).await;
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.error.contains("bounded media queue is full")),
            "uncharged routing={routing}: {:?}",
            report.errors
        );
        assert!(!frames.iter().any(|frame| frame.id == id));
    }
}

#[tokio::test(start_paused = true)]
async fn each_timed_payload_dynamic_field_is_charged() {
    let long = "x".repeat(201);
    let mut payloads = vec![];
    for field in 0..4 {
        let mut value = AggregatedTextFrame {
            text: TextFrame::new(""),
            aggregated_by: String::new(),
            context_id: None,
            raw_text: None,
            will_be_spoken: true,
        };
        match field {
            0 => value.text.text = long.clone(),
            1 => value.aggregated_by = long.clone(),
            2 => value.context_id = Some(long.clone()),
            _ => value.raw_text = Some(long.clone()),
        }
        payloads.push(FrameData::AggregatedText(value.clone()));
        payloads.push(FrameData::TtsText(value));
    }
    for field in 0..5 {
        let mut value = AggregatedTextProgressFrame {
            segment_id: 1,
            context_id: None,
            text: String::new(),
            aggregated_by: String::new(),
            accumulated_text: String::new(),
            remaining_text: String::new(),
        };
        match field {
            0 => value.context_id = Some(long.clone()),
            1 => value.text = long.clone(),
            2 => value.aggregated_by = long.clone(),
            3 => value.accumulated_text = long.clone(),
            _ => value.remaining_text = long.clone(),
        }
        payloads.push(FrameData::AggregatedTextProgress(value));
    }
    for field in 0..3 {
        let mut value = TtsAlignmentDiagnosticFrame {
            context_id: String::new(),
            segment_id: None,
            evidence: TtsAlignmentEvidence::Unreported,
            synthesis_text: String::new(),
            original_text: None,
            source_span: None,
            display_position: None,
            synthesis_outcome: TtsSynthesisOutcome::Succeeded,
        };
        match field {
            0 => value.context_id = long.clone(),
            1 => value.synthesis_text = long.clone(),
            _ => value.original_text = Some(long.clone()),
        }
        payloads.push(FrameData::TtsAlignmentDiagnostic(value));
    }
    for payload in payloads {
        let mut cfg = config();
        cfg.queue_byte_capacity = 200;
        let h = Harness::new(cfg).await;
        let mut frame = Frame::new(payload);
        let kind = frame.data.source_class_name();
        frame.set_pipeline_pts(h.clock.now_ns() + 100_000_000);
        h.send(frame).await;
        let (_, report) = h.finish(false).await;
        assert!(
            report
                .errors
                .iter()
                .any(|error| error.error.contains("bounded media queue is full")),
            "{kind}: {:?}",
            report.errors
        );
    }
}

#[tokio::test(start_paused = true)]
async fn bounded_wait_preserves_timed_deadline_and_all_destination_flush() {
    let mut cfg = config();
    cfg.queue_capacity = 1;
    cfg.destinations = vec!["later".into()];
    let h = Harness::new(cfg).await;
    let first = h.word("first", Duration::from_millis(100), None);
    let first_id = first.id;
    let later = h.word("later", Duration::from_millis(200), Some("later"));
    let later_id = later.id;
    h.send(first).await;
    h.send(later).await;
    let event = LocalFlushEvent::default();
    let marker = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        event: Some(event.clone()),
        returning: false,
        origin: Some("pipecat.flows".into()),
    }));
    let marker_id = marker.id;
    h.send(marker).await;
    assert!(!event.is_set());
    assert!(!h.contains(marker_id));
    tokio::time::advance(Duration::from_millis(100)).await;
    settle().await;
    assert!(h.contains(first_id));
    assert!(!h.contains(later_id));
    assert!(!event.is_set());
    assert!(!h.contains(marker_id));
    tokio::time::advance(Duration::from_millis(100)).await;
    settle().await;
    assert!(h.contains(later_id));
    assert!(h.contains(marker_id));
    assert!(event.is_set());
    let (_, report) = h.finish(false).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

#[tokio::test(start_paused = true)]
async fn bounded_wait_reports_future_metadata_head_of_line_timeouts_without_fake_pcm() {
    let mut cfg = config();
    cfg.queue_capacity = 1;
    cfg.admission_policy = OutputAdmissionPolicy::Wait {
        timeout: Duration::from_millis(50),
    };
    let h = Harness::new(cfg).await;
    let first = h.word("first", Duration::from_millis(500), None);
    let first_id = first.id;
    let blocked = h.word("blocked", Duration::from_millis(500), None);
    let blocked_id = blocked.id;
    h.send(first).await;
    h.send(blocked).await;
    h.send(Frame::new(FrameData::TtsAudio(
        AudioRawFrame::new(vec![1; 160], 8000, 1).unwrap(),
    )))
    .await;
    tokio::time::advance(Duration::from_millis(49)).await;
    settle().await;
    assert!(h.worker.errors().is_empty());
    assert!(h.backend.0.lock().unwrap().is_empty());
    tokio::time::advance(Duration::from_millis(1)).await;
    settle().await;
    assert_eq!(h.worker.errors().len(), 1);
    tokio::time::advance(Duration::from_millis(50)).await;
    settle().await;
    assert_eq!(h.worker.errors().len(), 2);
    assert!(h
        .worker
        .errors()
        .iter()
        .all(|error| error.error.contains("admission timed out")));
    assert!(h.backend.0.lock().unwrap().is_empty());
    assert!(!h.contains(blocked_id));
    tokio::time::advance(Duration::from_millis(400)).await;
    settle().await;
    assert!(h.contains(first_id));
    let (_, report) = h.finish(false).await;
    assert_eq!(report.errors.len(), 2);
}
