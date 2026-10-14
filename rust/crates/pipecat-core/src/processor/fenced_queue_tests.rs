use super::*;
use crate::{
    AggregatedTextFrame, AudioRawFrame, DetailedErrorFrame, DetailedTranscriptionFrame,
    DirectedFrame, LocalFrameTag, LocalObject, ObservedProcessor, Pipeline, PipelineConfig,
    PipelineWorker, SpeculationOwner, TextFrame, TranscriptionFrame, UserAudioRawFrame,
};
use std::sync::{mpsc, Barrier, Mutex};
use std::time::Duration;

type Seen = (u64, Direction, ObservedProcessor, Option<u64>);
#[derive(Default)]
struct Capture {
    context: Mutex<Option<ProcessorContext>>,
    seen: Mutex<Vec<Seen>>,
}
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "fenced-self-queue"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "capture") {
            *self.context.lock().unwrap() = Some(ctx.clone());
        }
        self.seen.lock().unwrap().push((
            frame.id,
            direction,
            ctx.processor_identity(),
            ctx.current_generation(),
        ));
        ctx.push(frame, direction).await
    }
}
async fn fixture(capacity: usize) -> (PipelineWorker, ProcessorContext, Arc<Capture>) {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(PipelineConfig {
            queue_capacity: capacity,
            system_queue_capacity: capacity,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    receive(&mut worker).await;
    worker
        .sender()
        .send(Frame::text("capture"), Direction::Downstream)
        .await
        .unwrap();
    receive(&mut worker).await;
    let context = capture.context.lock().unwrap().take().unwrap();
    (worker, context, capture)
}
async fn receive(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .expect("processor did not forward the admitted frame")
        .expect("pipeline unexpectedly closed")
}
async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .expect("pipeline owners did not join")
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
async fn interrupt(worker: &mut PipelineWorker, context: &ProcessorContext) {
    context.broadcast_interruption().await.unwrap();
    for _ in 0..2 {
        assert!(matches!(
            receive(worker).await.frame.data,
            FrameData::Interruption
        ));
    }
}

#[tokio::test]
async fn different_runtime_epochs_admit_to_self_and_preserve_identity_and_direction() {
    let (source_worker, source, _) = fixture(32).await;
    let (mut destination_worker, destination, capture) = fixture(32).await;
    assert!(!Arc::ptr_eq(&source.runtime, &destination.runtime));
    assert_eq!(source.current_generation(), Some(0));
    interrupt(&mut destination_worker, &destination).await;
    assert!(destination.current_generation().unwrap() > 0);
    let fence = source.invocation_fence().unwrap();
    for direction in [Direction::Downstream, Direction::Upstream] {
        let mut frame = Frame::text("independent epochs");
        frame.pts = Some(42);
        frame
            .metadata
            .insert("relay".into(), serde_json::json!([1, 2]));
        frame.transport_source = Some("source".into());
        frame.transport_destination = Some("destination".into());
        let tag = LocalFrameTag::new();
        tag.mark(&mut frame).unwrap();
        let expected = frame.clone();
        assert_eq!(
            destination.try_queue_with_invocation_fence(frame, direction, &fence),
            Ok(FencedQueueOutcome::Queued)
        );
        let output = receive(&mut destination_worker).await;
        assert_eq!(output.direction, direction);
        assert_eq!(output.frame, expected);
        assert!(tag.contains(&output.frame));
        assert!(capture.seen.lock().unwrap().contains(&(
            expected.id,
            direction,
            destination.processor_identity(),
            destination.current_generation(),
        )));
    }
    finish(source_worker).await;
    finish(destination_worker).await;
}

#[tokio::test]
async fn raw_source_and_destination_fences_override_system_and_survival_exemptions() {
    for stale_source in [true, false] {
        let (mut source_worker, source, _) = fixture(32).await;
        let (mut destination_worker, destination, capture) = fixture(32).await;
        let fence = source.invocation_fence().unwrap();
        let old_destination = destination.clone();
        if stale_source {
            interrupt(&mut source_worker, &source).await;
        } else {
            interrupt(&mut destination_worker, &destination).await;
        }
        for frame in [
            Frame::text("ordinary"),
            Frame::text("surviving").with_interruptible(false),
            Frame::new(FrameData::InputText(TextFrame::new("system"))),
        ] {
            let id = frame.id;
            let mut retained = old_destination.clone();
            retained.system_frame_id = Some(id);
            let outcome = BACKGROUND_INTERRUPTIBLE
                .scope(false, async {
                    retained.try_queue_with_invocation_fence(frame, Direction::Downstream, &fence)
                })
                .await;
            assert_eq!(outcome, Ok(FencedQueueOutcome::Stale));
            assert!(!capture.seen.lock().unwrap().iter().any(|seen| seen.0 == id));
        }
        assert!(destination_worker.try_recv().is_err());
        finish(source_worker).await;
        finish(destination_worker).await;
    }
}

#[tokio::test]
async fn inherited_raw_origins_remain_restrictions_when_local_generation_is_none() {
    for inherited_source in [true, false] {
        let (source_worker, mut source, _) = fixture(16).await;
        let (destination_worker, mut destination, _) = fixture(16).await;
        let (mut origin_worker, origin, _) = fixture(16).await;
        let inherited = ForwardingOrigin {
            runtime: origin.runtime.clone(),
            index: origin.index,
            generation: origin.current_generation().unwrap(),
        };
        if inherited_source {
            source.generation = None;
            source.origins.push(inherited);
        } else {
            destination.generation = None;
            destination.origins.push(inherited);
        }
        let fence = source.invocation_fence().unwrap();
        interrupt(&mut origin_worker, &origin).await;
        assert_eq!(
            destination.try_queue_with_invocation_fence(
                Frame::new(FrameData::InputText(TextFrame::new("raw inherited")))
                    .with_interruptible(false),
                Direction::Downstream,
                &fence,
            ),
            Ok(FencedQueueOutcome::Stale)
        );
        finish(source_worker).await;
        finish(destination_worker).await;
        finish(origin_worker).await;
    }
}

#[tokio::test]
async fn later_source_interruption_does_not_retract_an_already_queued_snapshot() {
    let (mut source_worker, source, _) = fixture(16).await;
    let (mut destination_worker, destination, capture) = fixture(16).await;
    let fence = source.invocation_fence().unwrap();
    destination.runtime.queues[destination.index].set_normal_paused(true);
    let frame = Frame::text("admission already committed");
    let id = frame.id;
    assert_eq!(
        destination.try_queue_with_invocation_fence(frame, Direction::Upstream, &fence),
        Ok(FencedQueueOutcome::Queued)
    );
    assert_eq!(destination.runtime.queues[destination.index].len(), 1);
    interrupt(&mut source_worker, &source).await;
    assert!(!fence.is_current());
    destination.runtime.queues[destination.index].set_normal_paused(false);
    let output = receive(&mut destination_worker).await;
    assert_eq!(
        (output.frame.id, output.direction),
        (id, Direction::Upstream)
    );
    assert!(capture.seen.lock().unwrap().iter().any(|seen| seen.0 == id));
    finish(source_worker).await;
    finish(destination_worker).await;
}

#[tokio::test]
async fn full_queue_is_an_error_and_does_not_claim_admission() {
    let (source_worker, source, _) = fixture(1).await;
    let (mut destination_worker, destination, capture) = fixture(1).await;
    let fence = source.invocation_fence().unwrap();
    destination.runtime.queues[destination.index].set_normal_paused(true);
    let first = Frame::text("first");
    let first_id = first.id;
    assert_eq!(
        destination.try_queue_with_invocation_fence(first, Direction::Downstream, &fence),
        Ok(FencedQueueOutcome::Queued)
    );
    let rejected = Frame::text("full");
    let rejected_id = rejected.id;
    let error = destination
        .try_queue_with_invocation_fence(rejected, Direction::Downstream, &fence)
        .unwrap_err();
    assert_eq!(error.message, QueueError::Full.to_string());
    assert_eq!(destination.runtime.queues[destination.index].len(), 1);
    destination.runtime.queues[destination.index].set_normal_paused(false);
    assert_eq!(receive(&mut destination_worker).await.frame.id, first_id);
    assert!(!capture
        .seen
        .lock()
        .unwrap()
        .iter()
        .any(|seen| seen.0 == rejected_id));
    finish(source_worker).await;
    finish(destination_worker).await;
}

#[tokio::test]
async fn closed_source_and_expired_weak_are_stale_but_closed_destination_is_an_error() {
    let (source_worker, source, _) = fixture(16).await;
    let (destination_worker, destination, _) = fixture(16).await;
    let fence = source.invocation_fence().unwrap();
    let weak = Arc::downgrade(&source.runtime);
    finish(source_worker).await;
    assert_eq!(
        destination.try_queue_with_invocation_fence(
            Frame::text("closed source"),
            Direction::Downstream,
            &fence,
        ),
        Ok(FencedQueueOutcome::Stale)
    );
    drop(source);
    assert!(
        weak.upgrade().is_none(),
        "a fence must not retain its runtime"
    );
    assert_eq!(
        destination.try_queue_with_invocation_fence(
            Frame::text("retired source"),
            Direction::Downstream,
            &fence,
        ),
        Ok(FencedQueueOutcome::Stale)
    );
    finish(destination_worker).await;
    let error = destination
        .try_queue_with_invocation_fence(Frame::text("both closed"), Direction::Downstream, &fence)
        .unwrap_err();
    assert_eq!(error.message, "pipeline is closed");
    // Existing push intentionally preserves its silent stale/closed behavior.
    assert_eq!(
        destination.try_push_with_invocation_fence(
            Frame::text("legacy silent drop"),
            Direction::Downstream,
            &fence,
        ),
        Ok(())
    );
}

fn relay_payloads() -> Vec<FrameData> {
    let text = TextFrame::new("relay");
    let aggregated = AggregatedTextFrame {
        text: text.clone(),
        aggregated_by: "sentence".into(),
        context_id: None,
        raw_text: None,
        will_be_spoken: true,
    };
    let transcript = TranscriptionFrame {
        text: "relay".into(),
        ..Default::default()
    };
    let detailed = DetailedTranscriptionFrame {
        text: text.clone(),
        user_id: "user".into(),
        timestamp: "time".into(),
        language: None,
        result: Some(serde_json::json!({"result": 1})),
        finalized: true,
    };
    let audio = AudioRawFrame::new(vec![0, 0, 1, 0], 16_000, 1).unwrap();
    vec![
        FrameData::Text(text.clone()),
        FrameData::LlmText(text.clone()),
        FrameData::InputText(text.clone()),
        FrameData::VisionText(text),
        FrameData::AggregatedText(aggregated.clone()),
        FrameData::TtsText(aggregated),
        FrameData::Transcription(transcript.clone()),
        FrameData::InterimTranscription(transcript.clone()),
        FrameData::EagerTranscription(transcript.clone()),
        FrameData::Translation(transcript),
        FrameData::TranscriptionDetailed(detailed.clone()),
        FrameData::InterimTranscriptionDetailed(detailed.clone()),
        FrameData::EagerTranscriptionDetailed(detailed.clone()),
        FrameData::TranslationDetailed(detailed),
        FrameData::InputAudio(audio.clone()),
        FrameData::OutputAudio(audio.clone()),
        FrameData::TtsAudio(audio.clone()),
        FrameData::SpeechOutputAudio(audio.clone()),
        FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "user".into(),
        }),
    ]
}

#[tokio::test]
async fn all_nineteen_snapshot_payloads_are_admitted_unchanged() {
    let (source_worker, source, _) = fixture(32).await;
    let (mut destination_worker, destination, _) = fixture(32).await;
    let fence = source.invocation_fence().unwrap();
    let payloads = relay_payloads();
    assert_eq!(payloads.len(), 19);
    for payload in payloads {
        let frame = Frame::new(payload);
        let expected = frame.clone();
        assert_eq!(
            destination.try_queue_with_invocation_fence(frame, Direction::Downstream, &fence),
            Ok(FencedQueueOutcome::Queued)
        );
        assert_eq!(receive(&mut destination_worker).await.frame, expected);
    }
    finish(source_worker).await;
    finish(destination_worker).await;
}

#[tokio::test]
async fn forbidden_payloads_and_authority_are_rejected_before_admission_locks() {
    let (source_worker, source, _) = fixture(16).await;
    let (destination_worker, destination, _) = fixture(16).await;
    let fence = source.invocation_fence().unwrap();
    let mut clock_frame = Frame::text("foreign clock");
    clock_frame.set_pipeline_pts(42);
    let owner = SpeculationOwner::new();
    let mut native_frame = Frame::text("native authority");
    native_frame
        .set_native_speculation(owner.capability())
        .unwrap();
    let mut error_frame = Frame::text("mutated error payload");
    error_frame.error_origin = Some(Arc::new(ProcessorErrorOrigin {
        processor: source.processor_identity(),
        health: source.health(),
        reported_health: source.health().snapshot(),
    }));
    let malformed_pcm = Frame::new(FrameData::InputAudio(AudioRawFrame {
        audio: vec![0],
        sample_rate: 0,
        num_channels: 0,
        context_id: None,
    }));
    let rejected = [
        Frame::start(),
        Frame::end(),
        Frame::cancel("control"),
        Frame::interruption(),
        Frame::new(FrameData::LlmThoughtText(TextFrame::new("thought"))),
        clock_frame,
        native_frame,
        error_frame,
        malformed_pcm,
    ];
    {
        // Calling while both gates are held proves every rejection precedes
        // lock acquisition; none may block trying to reacquire either gate.
        let _source_gate = source.runtime.admission.lock().unwrap();
        let _destination_gate = destination.runtime.admission.lock().unwrap();
        for frame in rejected {
            assert!(destination
                .try_queue_with_invocation_fence(frame, Direction::Downstream, &fence)
                .is_err());
        }
    }
    drop(owner);
    finish(source_worker).await;
    finish(destination_worker).await;
}

struct DropProbe {
    source: Arc<Runtime>,
    destination: Arc<Runtime>,
    observed: mpsc::Sender<bool>,
}
impl Drop for DropProbe {
    fn drop(&mut self) {
        let source_unlocked = self.source.admission.try_lock().is_ok();
        let destination_unlocked = self.destination.admission.try_lock().is_ok();
        self.observed
            .send(source_unlocked && destination_unlocked)
            .unwrap();
    }
}
fn dropping_frame(
    source: &ProcessorContext,
    destination: &ProcessorContext,
) -> (Frame, mpsc::Receiver<bool>) {
    let (observed, receiver) = mpsc::channel();
    let frame = Frame::new(FrameData::ErrorDetailed(DetailedErrorFrame {
        error: "local payload".into(),
        fatal: false,
        processor: None,
        exception: Some(LocalObject::new(DropProbe {
            source: source.runtime.clone(),
            destination: destination.runtime.clone(),
            observed,
        })),
        category: None,
    }));
    (frame, receiver)
}

#[tokio::test]
async fn rejected_owned_payloads_drop_outside_both_admission_gates() {
    let (mut source_worker, source, _) = fixture(16).await;
    let (destination_worker, destination, _) = fixture(16).await;
    let fence = source.invocation_fence().unwrap();
    let (frame, dropped) = dropping_frame(&source, &destination);
    assert!(destination
        .try_queue_with_invocation_fence(frame, Direction::Downstream, &fence)
        .is_err());
    assert!(dropped.recv_timeout(Duration::from_secs(1)).unwrap());
    interrupt(&mut source_worker, &source).await;
    let (frame, dropped) = dropping_frame(&source, &destination);
    assert_eq!(
        destination.try_push_with_invocation_fence(frame, Direction::Downstream, &fence),
        Ok(())
    );
    assert!(dropped.recv_timeout(Duration::from_secs(1)).unwrap());
    finish(source_worker).await;
    finish(destination_worker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opposite_runtime_admissions_use_one_lock_order_and_join_all_owners() {
    const COUNT: usize = 128;
    let (left_worker, left, _) = fixture(COUNT + 1).await;
    let (right_worker, right, _) = fixture(COUNT + 1).await;
    left.runtime.queues[left.index].set_normal_paused(true);
    right.runtime.queues[right.index].set_normal_paused(true);
    let left_fence = left.invocation_fence().unwrap();
    let right_fence = right.invocation_fence().unwrap();
    let barrier = Arc::new(Barrier::new(3));
    let (done, completed) = mpsc::channel();
    let mut threads = Vec::new();
    for (destination, fence) in [(right.clone(), left_fence), (left.clone(), right_fence)] {
        let barrier = barrier.clone();
        let done = done.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            for _ in 0..COUNT {
                assert_eq!(
                    destination.try_queue_with_invocation_fence(
                        Frame::text("opposite direction"),
                        Direction::Downstream,
                        &fence,
                    ),
                    Ok(FencedQueueOutcome::Queued)
                );
            }
            done.send(()).unwrap();
        }));
    }
    barrier.wait();
    for _ in 0..2 {
        completed
            .recv_timeout(Duration::from_secs(3))
            .expect("opposite admissions deadlocked");
    }
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(left.runtime.queues[left.index].len(), COUNT);
    assert_eq!(right.runtime.queues[right.index].len(), COUNT);
    left.runtime.queues[left.index].set_normal_paused(false);
    right.runtime.queues[right.index].set_normal_paused(false);
    finish(left_worker).await;
    finish(right_worker).await;
}
