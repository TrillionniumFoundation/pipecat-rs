#![cfg(feature = "voicemail")]
#[allow(dead_code)]
#[path = "support/review_voicemail_http.rs"]
mod http;
#[allow(dead_code)]
#[path = "support/review_voicemail_support.rs"]
mod support;
use pipecat_classifiers::*;
use pipecat_core::*;
use pipecat_processors::voicemail::*;
use serde_json::{json, Value};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use support::*;
use tokio::sync::mpsc;

fn config() -> VoicemailConfig {
    VoicemailConfig {
        decision_silence: Duration::from_millis(100),
        message_silence: Duration::from_millis(200),
        classifier_timeout: Duration::from_millis(300),
        max_unresolved_hold: Duration::from_secs(2),
        drain_timeout: Duration::from_millis(500),
        ..Default::default()
    }
}
async fn status(
    handle: &mut VoicemailStatusHandle,
    condition: impl Fn(&VoicemailStatus) -> bool,
) -> VoicemailStatus {
    bounded(async {
        loop {
            let value = handle.snapshot();
            if condition(&value) {
                return value;
            }
            handle.changed().await.expect("status producer closed");
        }
    })
    .await
}
struct Fixture {
    worker: PipelineWorker,
    detector: Arc<VoicemailDetector>,
    _gate: Arc<TtsGate>,
    detector_ack: Arc<Acknowledged>,
    admitted: mpsc::Receiver<u64>,
    gated: mpsc::Receiver<u64>,
    status: VoicemailStatusHandle,
}
impl Fixture {
    async fn new(classifier: Arc<dyn Classifier>, config: VoicemailConfig) -> Self {
        Self::with_pipeline(
            classifier,
            config,
            PipelineConfig {
                shutdown_timeout: Duration::from_secs(1),
                ..Default::default()
            },
        )
        .await
    }
    async fn with_pipeline(
        classifier: Arc<dyn Classifier>,
        config: VoicemailConfig,
        pipeline: PipelineConfig,
    ) -> Self {
        let (detector, gate, status) = voicemail_pair(classifier, config).unwrap();
        let detector = Arc::new(detector);
        let gate = Arc::new(gate);
        let (detector_ack, admitted) = acknowledged(detector.clone());
        let (gate_ack, gated) = acknowledged(gate.clone());
        let mut worker = Pipeline::new(vec![detector_ack.clone(), gate_ack])
            .start(pipeline)
            .await
            .unwrap();
        assert!(receive(&mut worker).await.frame.data.is_start());
        Self {
            worker,
            detector,
            _gate: gate,
            detector_ack,
            admitted,
            gated,
            status,
        }
    }
    async fn input(&mut self, frame: Frame) -> u64 {
        let id = frame.id;
        bounded(self.worker.sender().send(frame, Direction::Downstream))
            .await
            .unwrap();
        ack(&mut self.admitted, id).await;
        id
    }
    async fn both(&mut self, frame: Frame) -> u64 {
        let id = self.input(frame).await;
        ack(&mut self.gated, id).await;
        id
    }
    async fn finish(mut self) {
        self.worker.end().await.unwrap();
        let (_, report) = collect_bounded(self.worker).await.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        let status = status(&mut self.status, |s| s.supervisors == 0).await;
        assert_eq!(status.retained_frames, 0);
        assert_eq!(status.retained_frame_bytes, 0);
        assert_eq!(status.retained_pcm_bytes, 0);
        assert_eq!(status.evidence_bytes, 0);
        assert!(!status.in_flight_classifier);
        assert!(!status.drain_in_flight);
    }
}
fn check_question(request: &Requested) {
    assert_eq!(request.questions.len(), 1);
    let ClassifierQuestion::Choice(question) = &request.questions["voicemail"] else {
        panic!("wrong source question");
    };
    assert_eq!(question.instructions, json!("A bot has placed an outbound phone call. This is what was heard after the call connected. Decide whether a person answered or the call went to voicemail."));
    assert_eq!(question.options["conversation"], Some(json!("a person answered: a greeting such as 'hello?', 'hi', 'yeah?' or 'John speaking'; a question to the caller such as 'who is this?' or 'can I help you?'; spontaneous speech that expects a reply")));
    assert_eq!(question.options["voicemail"], Some(json!("an automated greeting or carrier message: 'you've reached', 'leave a message', 'I'm not available right now', 'call me back', 'mailbox is full', 'not in service', 'all circuits are busy', 'our office is currently closed'")));
    assert_eq!(question.options.len(), 2);
    assert!(request.options.deadline.is_some());
}

#[tokio::test(start_paused = true)]
async fn independent_no_startup_timer_stop_without_text_has_explicit_fallback() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(classifier, config()).await;
    tokio::time::advance(Duration::from_secs(20)).await;
    assert_eq!(f.status.snapshot().phase, DetectionPhase::Pending);
    assert!(requests.try_recv().is_err());
    f.both(Frame::new(FrameData::UserStoppedSpeaking)).await;
    tokio::time::advance(Duration::from_millis(99)).await;
    assert!(f.status.snapshot().event.is_none());
    tokio::time::advance(Duration::from_millis(1)).await;
    let value = status(&mut f.status, |s| s.event.is_some()).await;
    assert_eq!(
        value.event,
        Some(VoicemailEvent::ConversationDetected {
            session_id: value.session_id,
            reason: EvidenceReason::NoAnswerFallback
        })
    );
    assert_eq!(value.attempts, 0);
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_cumulative_coalescing_latest_success_not_confidence() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(classifier.clone(), config()).await;
    f.both(Frame::new(FrameData::UserStartedSpeaking)).await;
    f.both(transcript(" \tfirst  words\n")).await;
    let first = bounded(requests.recv()).await.unwrap();
    check_question(&first);
    assert_eq!(first.state, json!("first  words"));
    f.both(transcript(" second ")).await;
    f.both(transcript(" second ")).await;
    assert!(requests.try_recv().is_err());
    first.answer.send(Ok(batch("voicemail", 0.99))).unwrap();
    let second = bounded(requests.recv()).await.unwrap();
    assert_eq!(second.state, json!("first  words second second"));
    second.answer.send(Ok(batch("conversation", 0.01))).unwrap();
    status(&mut f.status, |s| s.attempted_revision == 3).await;
    f.both(Frame::new(FrameData::UserStoppedSpeaking)).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    let value = status(&mut f.status, |s| s.phase == DetectionPhase::Conversation).await;
    assert_eq!(value.attempts, 2);
    assert_eq!(value.missed_answers, 0);
    assert_eq!(classifier.maximum.load(Ordering::SeqCst), 1);
    assert_eq!(classifier.active.load(Ordering::SeqCst), 0);
    assert_eq!(classifier.dropped.load(Ordering::SeqCst), 2);
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_final_variants_preserve_source_input_policy() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(classifier, config()).await;
    f.both(Frame::new(FrameData::UserStartedSpeaking)).await;
    for data in [
        FrameData::InterimTranscription(TranscriptionFrame {
            text: "interim".into(),
            ..Default::default()
        }),
        FrameData::EagerTranscription(TranscriptionFrame {
            text: "eager".into(),
            ..Default::default()
        }),
        FrameData::Translation(TranscriptionFrame {
            text: "translation".into(),
            ..Default::default()
        }),
    ] {
        f.both(Frame::new(data)).await;
    }
    f.both(transcript(" \t\n ")).await;
    assert_eq!(f.status.snapshot().revision, 0);
    assert!(requests.try_recv().is_err());
    f.both(Frame::new(FrameData::TranscriptionDetailed(
        DetailedTranscriptionFrame {
            text: TextFrame::new("  hello  "),
            user_id: "first-user".into(),
            timestamp: "ignored".into(),
            language: None,
            result: None,
            finalized: false,
        },
    )))
    .await;
    let first = bounded(requests.recv()).await.unwrap();
    assert_eq!(first.state, json!("hello"));
    f.both(Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: " there ".into(),
        user_id: "other-user".into(),
        finalized: false,
        ..Default::default()
    })))
    .await;
    first.answer.send(Ok(batch("conversation", 0.5))).unwrap();
    let second = bounded(requests.recv()).await.unwrap();
    assert_eq!(second.state, json!("hello there"));
    second.answer.send(Ok(batch("conversation", 0.2))).unwrap();
    status(&mut f.status, |s| s.attempted_revision == 2).await;
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_late_final_and_resumed_speech_restart_full_decision_wait() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(classifier, config()).await;
    f.both(transcript("hello")).await;
    let first = bounded(requests.recv()).await.unwrap();
    first.answer.send(Ok(batch("conversation", 0.99))).unwrap();
    status(&mut f.status, |s| s.attempted_revision == 1).await;
    tokio::time::advance(Duration::from_millis(99)).await;
    f.both(transcript("anybody there")).await;
    let second = bounded(requests.recv()).await.unwrap();
    second.answer.send(Ok(batch("conversation", 0.5))).unwrap();
    status(&mut f.status, |s| s.attempted_revision == 2).await;
    tokio::time::advance(Duration::from_millis(99)).await;
    assert!(f.status.snapshot().event.is_none());
    f.both(Frame::new(FrameData::UserStartedSpeaking)).await;
    tokio::time::advance(Duration::from_millis(200)).await;
    assert!(f.status.snapshot().event.is_none());
    f.both(Frame::new(FrameData::UserStoppedSpeaking)).await;
    tokio::time::advance(Duration::from_millis(99)).await;
    assert!(f.status.snapshot().event.is_none());
    tokio::time::advance(Duration::from_millis(1)).await;
    status(&mut f.status, |s| s.phase == DetectionPhase::Conversation).await;
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_timeout_of_uncooperative_classifier_settles_once() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(classifier.clone(), config()).await;
    f.both(transcript("no answer")).await;
    let request = bounded(requests.recv()).await.unwrap();
    tokio::time::advance(Duration::from_millis(100)).await;
    assert_eq!(f.status.snapshot().phase, DetectionPhase::Pending);
    tokio::time::advance(Duration::from_millis(200)).await;
    let value = status(&mut f.status, |s| s.phase == DetectionPhase::Conversation).await;
    assert_eq!(value.missed_answers, 1);
    assert_eq!(value.attempted_revision, 1);
    assert_eq!(classifier.active.load(Ordering::SeqCst), 0);
    assert!(request.answer.send(Ok(batch("voicemail", 1.0))).is_err());
    assert_eq!(
        value.event,
        Some(VoicemailEvent::ConversationDetected {
            session_id: value.session_id,
            reason: EvidenceReason::NoAnswerFallback
        })
    );
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_malformed_new_answer_preserves_old_success_and_two_timers() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(classifier, config()).await;
    f.both(Frame::new(FrameData::UserStartedSpeaking)).await;
    f.both(transcript("leave a message")).await;
    bounded(requests.recv())
        .await
        .unwrap()
        .answer
        .send(Ok(batch("voicemail", 0.01)))
        .unwrap();
    status(&mut f.status, |s| s.attempted_revision == 1).await;
    f.both(transcript("more greeting")).await;
    let mut malformed = batch("conversation", 1.0);
    malformed.answers.clear();
    bounded(requests.recv())
        .await
        .unwrap()
        .answer
        .send(Ok(malformed))
        .unwrap();
    status(&mut f.status, |s| s.attempted_revision == 2).await;
    f.both(Frame::new(FrameData::UserStoppedSpeaking)).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    // Observe the actual gate interruption before measuring post-verdict delay.
    loop {
        let frame = receive(&mut f.worker).await;
        if matches!(frame.frame.data, FrameData::Interruption)
            && frame.direction == Direction::Downstream
        {
            break;
        }
    }
    assert_eq!(f.status.snapshot().phase, DetectionPhase::VoicemailWaiting);
    assert!(f.status.snapshot().event.is_none());
    tokio::time::advance(Duration::from_millis(100)).await;
    f.input(transcript("late transcript does not extend message wait"))
        .await;
    tokio::time::advance(Duration::from_millis(99)).await;
    assert!(f.status.snapshot().event.is_none());
    tokio::time::advance(Duration::from_millis(1)).await;
    let ready = status(&mut f.status, |s| s.phase == DetectionPhase::VoicemailReady).await;
    assert_eq!(ready.missed_answers, 1);
    assert_eq!(
        ready.event,
        Some(VoicemailEvent::VoicemailReady {
            session_id: ready.session_id,
            reason: EvidenceReason::Classifier
        })
    );
    f.input(Frame::new(FrameData::UserStartedSpeaking)).await;
    f.input(Frame::new(FrameData::UserStoppedSpeaking)).await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(f.status.snapshot().event, ready.event);
    assert_eq!(f.status.snapshot().revision, 2);
    assert!(requests.try_recv().is_err());
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_pending_end_and_stop_discard_without_fallback_and_join() {
    for terminal in [
        Frame::end(),
        Frame::new(FrameData::EndDetailed(Default::default())),
        Frame::new(FrameData::Stop),
    ] {
        let expected = if matches!(terminal.data, FrameData::Stop) {
            WorkerState::Stopped
        } else {
            WorkerState::Ended
        };
        let (classifier, mut requests) = scripted();
        let mut f = Fixture::new(classifier.clone(), config()).await;
        f.both(Frame::new(FrameData::TtsStarted(Default::default())))
            .await;
        f.both(transcript("blocked classifier")).await;
        let call = bounded(requests.recv()).await.unwrap();
        f.worker
            .sender()
            .send(terminal, Direction::Downstream)
            .await
            .unwrap();
        let (outputs, report) = collect_bounded(f.worker).await.unwrap();
        assert_eq!(report.state, expected);
        assert!(!outputs
            .iter()
            .any(|o| matches!(o.frame.data, FrameData::TtsStarted(_))));
        let value = status(&mut f.status, |s| s.supervisors == 0).await;
        assert_eq!(value.phase, DetectionPhase::Closed);
        assert!(value.event.is_none());
        assert_eq!((value.retained_frames, value.evidence_bytes), (0, 0));
        assert_eq!(classifier.active.load(Ordering::SeqCst), 0);
        assert!(call.answer.send(Ok(batch("conversation", 1.0))).is_err());
    }
}

#[tokio::test(start_paused = true)]
async fn independent_hold_deadline_fails_closed_without_silence_fallback() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(classifier, config()).await;
    f.both(Frame::new(FrameData::TtsStarted(Default::default())))
        .await;
    tokio::time::advance(Duration::from_millis(1999)).await;
    assert_eq!(f.status.snapshot().phase, DetectionPhase::Pending);
    tokio::time::advance(Duration::from_millis(1)).await;
    let value = status(&mut f.status, |s| s.phase == DetectionPhase::Failed).await;
    assert_eq!(value.failure, Some(Failure::HoldTimeout));
    assert!(value.event.is_none());
    assert_eq!(value.retained_frames, 0);
    assert!(requests.try_recv().is_err());
    f.worker.cancel("independent completion").await.unwrap();
    let _ = collect_bounded(f.worker).await;
}

#[tokio::test(start_paused = true)]
async fn independent_segment_and_byte_limits_fail_without_partial_inference() {
    for (limit, text) in [
        (
            VoicemailConfig {
                max_transcript_segments: 1,
                ..config()
            },
            "b",
        ),
        (
            VoicemailConfig {
                max_transcript_bytes: 3,
                ..config()
            },
            "longer",
        ),
    ] {
        let (classifier, mut requests) = scripted();
        let mut f = Fixture::new(classifier.clone(), limit).await;
        f.both(Frame::new(FrameData::UserStartedSpeaking)).await;
        f.both(transcript("a")).await;
        let first = bounded(requests.recv()).await.unwrap();
        f.input(transcript(text)).await;
        let value = status(&mut f.status, |s| s.phase == DetectionPhase::Failed).await;
        assert_eq!(value.failure, Some(Failure::TranscriptLimit));
        assert!(value.event.is_none());
        f.worker.cancel("independent completion").await.unwrap();
        let _ = collect_bounded(f.worker).await;
        assert!(requests.try_recv().is_err());
        assert_eq!(classifier.active.load(Ordering::SeqCst), 0);
        assert!(first.answer.send(Ok(batch("conversation", 1.0))).is_err());
        assert_eq!(f.status.snapshot().evidence_bytes, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn independent_draining_frame_remains_charged_under_full_output() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::with_pipeline(
        classifier,
        VoicemailConfig {
            max_frames: 2,
            ..config()
        },
        PipelineConfig {
            output_capacity: 1,
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .await;
    f.both(Frame::new(FrameData::TtsStarted(Default::default())))
        .await;
    f.both(tts("held")).await;
    f.both(transcript("hello")).await; // fills the actual output lane
    bounded(requests.recv())
        .await
        .unwrap()
        .answer
        .send(Ok(batch("conversation", 1.0)))
        .unwrap();
    status(&mut f.status, |s| s.attempted_revision == 1).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    let blocked = status(&mut f.status, |s| s.drain_in_flight).await;
    assert_eq!(
        blocked.retained_frames, 2,
        "one FIFO plus the externally held drain frame"
    );
    assert_eq!(blocked.completed_release_attempts, 0);
    f.input(Frame::new(FrameData::TtsStopped(Default::default())))
        .await;
    let failed = status(&mut f.status, |s| s.phase == DetectionPhase::Failed).await;
    assert_eq!(failed.failure, Some(Failure::FrameLimit));
    // Reporting the expected limit failure into this deliberately full output
    // can already terminate the runtime. Closed is therefore a settled outcome.
    let cancelled = f.worker.cancel("independent full-lane cleanup").await;
    assert!(cancelled.is_ok() || matches!(cancelled, Err(PipelineError::Closed)));
    let mut outputs = Vec::new();
    bounded(async {
        while let Some(frame) = f.worker.recv().await {
            outputs.push(frame);
            assert!(outputs.len() <= 16);
        }
    })
    .await;
    let joined = bounded(f.worker.join()).await;
    assert!(matches!(
        f.worker.state(),
        WorkerState::Cancelled | WorkerState::Failed
    ));
    if let Err(error) = joined {
        assert!(
            matches!(
                error,
                PipelineError::Execution(_) | PipelineError::Backpressure(_)
            ),
            "{error:?}"
        );
    }
    assert!(!outputs.iter().any(|o| matches!(
        o.frame.data,
        FrameData::TtsStarted(_) | FrameData::TtsText(_) | FrameData::TtsStopped(_)
    )));
    let zero = status(&mut f.status, |s| s.supervisors == 0).await;
    assert_eq!(zero.retained_frames, 0);
    assert_eq!(zero.retained_frame_bytes, 0);
}

#[tokio::test(start_paused = true)]
async fn independent_release_keeps_original_ids_metadata_and_new_tail_order() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::with_pipeline(
        classifier,
        config(),
        PipelineConfig {
            output_capacity: 1,
            shutdown_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    )
    .await;
    let mut originals = vec![
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::TtsStarted(Default::default())),
        tts("first"),
        Frame::new(FrameData::TtsStopped(Default::default())),
        Frame::new(FrameData::LlmResponseEnd),
    ];
    for (index, frame) in originals.iter_mut().enumerate() {
        frame.pts = Some(index as u64 + 100);
        frame.metadata.insert(
            "independent".into(),
            json!({"order":index,"non_ascii":"语音"}),
        );
        frame.transport_destination = Some("unchanged".into());
    }
    let expected: Vec<_> = originals
        .iter()
        .map(|f| (f.id, serde_json::to_value(f).unwrap()))
        .collect();
    for frame in originals {
        f.both(frame).await;
    }
    f.both(transcript("hello")).await;
    bounded(requests.recv())
        .await
        .unwrap()
        .answer
        .send(Ok(batch("conversation", 1.0)))
        .unwrap();
    status(&mut f.status, |s| s.attempted_revision == 1).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    status(&mut f.status, |s| s.drain_in_flight).await;
    let later = Frame::new(FrameData::TtsStarted(Default::default()));
    let tail_id = later.id;
    f.both(later).await;
    assert_eq!(f.status.snapshot().retained_frames, 6);
    // Only now release downstream capacity; a newer start cannot overtake the old end.
    assert!(matches!(
        receive(&mut f.worker).await.frame.data,
        FrameData::Transcription(_)
    ));
    for (id, expected) in expected {
        let actual = receive(&mut f.worker).await;
        assert_eq!(actual.frame.id, id);
        assert_eq!(serde_json::to_value(&actual.frame).unwrap(), expected);
    }
    assert_eq!(receive(&mut f.worker).await.frame.id, tail_id);
    f.both(Frame::new(FrameData::TtsStopped(Default::default())))
        .await;
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_stale_detector_invocation_cannot_mutate_later_silence_or_input() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(classifier, config()).await;
    let id = f
        .both(Frame::new(FrameData::Text(TextFrame::new(
            "capture invocation",
        ))))
        .await;
    let old = f
        .detector_ack
        .contexts
        .lock()
        .unwrap()
        .iter()
        .find(|(frame_id, _)| *frame_id == id)
        .unwrap()
        .1
        .clone();
    f.both(Frame::interruption()).await;
    let before = f.status.snapshot();
    f.detector
        .process(
            transcript("stale secret transcript"),
            Direction::Downstream,
            &old,
        )
        .await
        .unwrap();
    f.detector
        .process(
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Downstream,
            &old,
        )
        .await
        .unwrap();
    tokio::time::advance(Duration::from_millis(101)).await;
    let after = f.status.snapshot();
    assert_eq!(after.revision, before.revision);
    assert_eq!(after.phase, DetectionPhase::Pending);
    assert_eq!(after.attempts, 0);
    assert!(after.event.is_none());
    assert!(requests.try_recv().is_err());
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_evidence_reservation_and_overallocated_frame_fail_closed() {
    let (classifier, mut requests) = scripted();
    let mut f = Fixture::new(
        classifier,
        VoicemailConfig {
            max_evidence_bytes: 7,
            ..config()
        },
    )
    .await;
    f.input(transcript("four")).await;
    let failed = status(&mut f.status, |s| s.phase == DetectionPhase::Failed).await;
    assert_eq!(failed.failure, Some(Failure::EvidenceLimit));
    assert!(requests.try_recv().is_err());
    assert!(failed.event.is_none());
    f.worker.cancel("evidence cleanup").await.unwrap();
    let _ = collect_bounded(f.worker).await;
    for metadata in [false, true] {
        let (classifier, _) = scripted();
        let mut f = Fixture::new(
            classifier,
            VoicemailConfig {
                max_frame_bytes: 32 * 1024,
                ..config()
            },
        )
        .await;
        let mut frame = Frame::new(FrameData::TtsStarted(Default::default()));
        let mut allocation = String::with_capacity(64 * 1024);
        allocation.push('x');
        if metadata {
            frame
                .metadata
                .insert("oversized capacity".into(), Value::String(allocation));
        } else {
            frame.name = allocation;
        }
        f.input(frame).await;
        let failed = status(&mut f.status, |s| s.phase == DetectionPhase::Failed).await;
        assert_eq!(failed.failure, Some(Failure::FrameLimit));
        assert_eq!(failed.retained_frames, 0);
        assert!(failed.event.is_none());
        f.worker.cancel("allocation cleanup").await.unwrap();
        let _ = collect_bounded(f.worker).await;
    }
}

#[tokio::test(start_paused = true)]
async fn independent_pcm_capacity_and_duration_are_distinct_budgets() {
    for duration_case in [false, true] {
        let (classifier, _) = scripted();
        let limits = if duration_case {
            VoicemailConfig {
                max_pcm_duration: Duration::from_millis(1),
                ..config()
            }
        } else {
            VoicemailConfig {
                max_pcm_bytes: 32,
                ..config()
            }
        };
        let mut f = Fixture::new(classifier, limits).await;
        f.both(Frame::new(FrameData::TtsStarted(Default::default())))
            .await;
        let audio = || {
            Frame::new(FrameData::TtsAudio(
                AudioRawFrame::new(vec![0; 16], 8000, 1).unwrap(),
            ))
        };
        f.both(audio()).await;
        let at_limit = f.status.snapshot();
        assert_eq!(at_limit.retained_pcm_duration, Duration::from_millis(1));
        assert_eq!(
            at_limit.retained_pcm_bytes, 32,
            "original and reserved drain copy"
        );
        f.input(audio()).await;
        let failed = status(&mut f.status, |s| s.phase == DetectionPhase::Failed).await;
        assert_eq!(failed.failure, Some(Failure::PcmLimit));
        assert_eq!(failed.retained_pcm_bytes, 0);
        f.worker.cancel("PCM cleanup").await.unwrap();
        let _ = collect_bounded(f.worker).await;
    }
}

fn jev_response(label: &str) -> Value {
    json!({"answers":{"voicemail":{"type":"choice","choice":label,"probabilities":{"voicemail":0.5,"conversation":0.5},"confidence":0.5}},"usage":{"input_tokens":2,"output_tokens":1}})
}
fn jev(server: &http::Peer, response_limit: usize) -> (JevClient, Arc<dyn Classifier>) {
    let mut config = JevConfig::new("independent-synthetic-key");
    config.base_url = server.url.clone();
    config.max_retries = 0;
    config.timeout = Duration::from_secs(1);
    config.limits.max_response_bytes = response_limit;
    let client = JevClient::new(config).unwrap();
    (client.clone(), Arc::new(JevClassifier::shared(client)))
}

#[tokio::test]
async fn independent_real_http_classifier_exact_protocol_and_cumulative_snapshots() {
    let mut server = http::Peer::start().await;
    let (client, classifier) = jev(&server, 4096);
    let mut f = Fixture::new(classifier, config()).await;
    f.both(Frame::new(FrameData::UserStartedSpeaking)).await;
    f.both(transcript(" hello ")).await;
    let first = server.request().await;
    assert!(first.headers.starts_with("POST /v1/systemone HTTP/1.1\r\n"));
    assert!(first
        .headers
        .to_ascii_lowercase()
        .contains("authorization: bearer independent-synthetic-key"));
    assert_eq!(first.body["state"], json!("hello"));
    assert_eq!(first.body["questions"].as_object().unwrap().len(), 1);
    assert_eq!(
        first.body["questions"]["voicemail"]["type"],
        json!("choice")
    );
    assert_eq!(
        first.body["questions"]["voicemail"]["criteria"]
            .as_object()
            .unwrap()
            .len(),
        2
    );
    f.both(transcript("leave")).await;
    f.both(transcript("a message")).await;
    first
        .respond
        .send(http::Response::Json(jev_response("conversation")))
        .unwrap();
    let second = server.request().await;
    assert_eq!(second.body["state"], json!("hello leave a message"));
    second
        .respond
        .send(http::Response::Json(jev_response("voicemail")))
        .unwrap();
    status(&mut f.status, |s| s.attempted_revision == 3).await;
    f.both(Frame::new(FrameData::UserStoppedSpeaking)).await;
    let ready = status(&mut f.status, |s| s.phase == DetectionPhase::VoicemailReady).await;
    assert_eq!(ready.attempts, 2);
    assert_eq!(client.usage().unwrap().input_tokens, 4);
    assert_eq!(server.count.load(Ordering::SeqCst), 2);
    f.finish().await;
    assert!(!client.is_closed());
    server.finish().await;
}

#[tokio::test]
async fn independent_real_http_faults_settle_as_missed_answer_without_retry() {
    let faults = vec![
        http::Response::Status(400),
        http::Response::Json(json!({"answers":{},"usage":{"input_tokens":0,"output_tokens":0}})),
        http::Response::Raw {
            declared: 1,
            body: b"{".to_vec(),
        },
        http::Response::Raw {
            declared: 100,
            body: b"{\"answers\":".to_vec(),
        },
        http::Response::Raw {
            declared: 5000,
            body: vec![b' '; 5000],
        },
        http::Response::Hang,
    ];
    for fault in faults {
        let mut server = http::Peer::start().await;
        let (client, classifier) = jev(&server, 4096);
        let mut f = Fixture::new(classifier, config()).await;
        f.both(transcript("greeting")).await;
        server.request().await.respond.send(fault).unwrap();
        let value = status(&mut f.status, |s| s.phase == DetectionPhase::Conversation).await;
        assert_eq!(value.attempts, 1);
        assert_eq!(value.missed_answers, 1);
        assert_eq!(
            value.event,
            Some(VoicemailEvent::ConversationDetected {
                session_id: value.session_id,
                reason: EvidenceReason::NoAnswerFallback
            })
        );
        assert_eq!(server.count.load(Ordering::SeqCst), 1);
        assert!(!client.is_closed());
        f.finish().await;
        server.finish().await;
    }
}

#[tokio::test]
async fn independent_cancel_one_pair_preserves_other_real_shared_client() {
    let mut server = http::Peer::start().await;
    let (client, classifier) = jev(&server, 4096);
    let mut first = Fixture::new(classifier.clone(), config()).await;
    let mut second = Fixture::new(classifier, config()).await;
    assert_ne!(
        first.status.snapshot().session_id,
        second.status.snapshot().session_id
    );
    first.both(transcript("cancel this request")).await;
    server
        .request()
        .await
        .respond
        .send(http::Response::Hang)
        .unwrap();
    first.worker.cancel("pair cancelled").await.unwrap();
    let (_, report) = collect_bounded(first.worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(!client.is_closed());
    assert!(first.status.snapshot().event.is_none());
    second.both(transcript("hello")).await;
    server
        .request()
        .await
        .respond
        .send(http::Response::Json(jev_response("conversation")))
        .unwrap();
    status(&mut second.status, |s| {
        s.phase == DetectionPhase::Conversation
    })
    .await;
    second.finish().await;
    assert!(!client.is_closed());
    server.finish().await;
}

struct ExactDeadlineAnswer(mpsc::Sender<()>);
#[async_trait]
impl Classifier for ExactDeadlineAnswer {
    async fn ask(
        &self,
        _: &Value,
        _: &Questions,
        options: CallOptions,
    ) -> pipecat_classifiers::Result<ClassificationBatch> {
        self.0.try_send(()).unwrap();
        tokio::time::sleep_until(options.deadline.unwrap()).await;
        Ok(batch("voicemail", 1.0))
    }
}
#[tokio::test(start_paused = true)]
async fn independent_answer_ready_exactly_at_deadline_is_not_a_success() {
    let (send, mut started) = mpsc::channel(1);
    let mut f = Fixture::new(Arc::new(ExactDeadlineAnswer(send)), config()).await;
    f.both(transcript("at deadline")).await;
    bounded(started.recv()).await.unwrap();
    tokio::time::advance(Duration::from_millis(300)).await;
    let settled = status(&mut f.status, |s| s.attempted_revision == 1).await;
    assert_eq!(settled.missed_answers, 1);
    let value = status(&mut f.status, |s| s.event.is_some()).await;
    assert_eq!(
        value.event,
        Some(VoicemailEvent::ConversationDetected {
            session_id: value.session_id,
            reason: EvidenceReason::NoAnswerFallback
        })
    );
    f.finish().await;
}

#[tokio::test(start_paused = true)]
async fn independent_missing_reversed_duplicate_and_reused_halves_fail_closed() {
    for shape in 0..5 {
        let (classifier, _) = scripted();
        let (detector, gate, mut watch) = voicemail_pair(classifier, config()).unwrap();
        let detector = Arc::new(detector);
        let gate = Arc::new(gate);
        let nodes: Vec<Arc<dyn FrameProcessor>> = match shape {
            0 => vec![detector],
            1 => vec![gate],
            2 => vec![gate, detector],
            3 => vec![detector.clone(), detector, gate],
            _ => vec![detector, gate.clone(), gate],
        };
        let start = Pipeline::new(nodes)
            .start(PipelineConfig {
                startup_timeout: Duration::from_millis(100),
                shutdown_timeout: Duration::from_millis(100),
                ..Default::default()
            })
            .await;
        if let Ok(worker) = start {
            // A runtime that reports failed setup after entering Running must
            // still never have a usable shared pair or publish a verdict.
            let state = status(&mut watch, |s| {
                matches!(s.phase, DetectionPhase::Failed | DetectionPhase::Closed)
            })
            .await;
            assert_eq!(
                state.failure,
                Some(Failure::PairAttachment),
                "shape {shape}"
            );
            worker.cancel("invalid pairing cleanup").await.unwrap();
            let _ = collect_bounded(worker).await;
        }
        let state = status(&mut watch, |s| s.supervisors == 0).await;
        assert!(matches!(
            state.phase,
            DetectionPhase::Failed | DetectionPhase::Closed
        ));
        assert!(state.event.is_none());
        assert_eq!(state.retained_frames, 0);
    }
    let (classifier, _) = scripted();
    let (detector, gate, watch) = voicemail_pair(classifier, config()).unwrap();
    let detector = Arc::new(detector);
    let gate = Arc::new(gate);
    let mut first = Pipeline::new(vec![detector.clone(), gate.clone()])
        .start(Default::default())
        .await
        .unwrap();
    receive(&mut first).await;
    first.end().await.unwrap();
    collect_bounded(first).await.unwrap();
    let second = Pipeline::new(vec![detector, gate])
        .start(PipelineConfig {
            startup_timeout: Duration::from_millis(100),
            shutdown_timeout: Duration::from_millis(100),
            ..Default::default()
        })
        .await;
    assert!(
        second.is_err(),
        "closed pair cannot attach to a new runtime"
    );
    assert!(watch.snapshot().event.is_none());
}

#[tokio::test(start_paused = true)]
async fn independent_worker_drop_with_only_status_retained_releases_classifier_and_media() {
    let (classifier, mut requests) = scripted();
    let weak = Arc::downgrade(&classifier);
    let (detector, gate, mut watch) = voicemail_pair(classifier.clone(), config()).unwrap();
    drop(classifier);
    let mut worker = Pipeline::new(vec![Arc::new(detector), Arc::new(gate)])
        .start(Default::default())
        .await
        .unwrap();
    receive(&mut worker).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::TtsStarted(Default::default())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(transcript("never complete"), Direction::Downstream)
        .await
        .unwrap();
    let request = bounded(requests.recv()).await.unwrap();
    status(&mut watch, |s| s.retained_frames == 1).await;
    drop(worker);
    let zero = status(&mut watch, |s| s.supervisors == 0).await;
    assert_eq!(zero.phase, DetectionPhase::Closed);
    assert!(zero.event.is_none());
    assert_eq!((zero.retained_frames, zero.evidence_bytes), (0, 0));
    assert!(request.answer.send(Ok(batch("voicemail", 1.0))).is_err());
    // Core supervisor cleanup may perform its final drop after its last watch
    // publication; an explicit bounded task rendezvous establishes completion.
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while weak.upgrade().is_some() {
        assert!(
            std::time::Instant::now() < deadline,
            "classifier owner leaked after worker drop"
        );
        tokio::task::yield_now().await;
    }
}
