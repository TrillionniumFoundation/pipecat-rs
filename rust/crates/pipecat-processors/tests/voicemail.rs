#![cfg(feature = "voicemail")]
//! Native local orchestration only; no calls, provider credentials or playback claims.
use pipecat_classifiers::*;
use pipecat_core::*;
use pipecat_processors::voicemail::*;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

struct Attempt {
    state: Value,
    questions: Questions,
    complete: oneshot::Sender<Option<&'static str>>,
}
struct Script {
    calls: mpsc::Sender<Attempt>,
    active: Arc<AtomicUsize>,
    maximum: Arc<AtomicUsize>,
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl Classifier for Script {
    async fn ask(
        &self,
        state: &Value,
        questions: &Questions,
        _: CallOptions,
    ) -> pipecat_classifiers::Result<ClassificationBatch> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.maximum.fetch_max(active, Ordering::SeqCst);
        let _active = Active(self.active.clone());
        let (complete, wait) = oneshot::channel();
        self.calls
            .send(Attempt {
                state: state.clone(),
                questions: questions.clone(),
                complete,
            })
            .await
            .unwrap();
        let choice = wait
            .await
            .map_err(|_| ClassifierError::Inference)?
            .ok_or(ClassifierError::Inference)?;
        Ok(ClassificationBatch {
            answers: BTreeMap::from([(
                "voicemail".into(),
                ClassifierResult::Choice(ChoiceResult {
                    choice: choice.into(),
                    probabilities: BTreeMap::new(),
                    confidence: 0.0001,
                }),
            )]),
            metrics: ClassificationMetrics {
                classifier: "synthetic".into(),
                model: None,
                elapsed: Duration::ZERO,
                usage: None,
                provenance: ProbabilityProvenance::LlmEstimate,
            },
        })
    }
}
fn script() -> (Arc<Script>, mpsc::Receiver<Attempt>) {
    let (calls, rx) = mpsc::channel(8);
    (
        Arc::new(Script {
            calls,
            active: Arc::new(AtomicUsize::new(0)),
            maximum: Arc::new(AtomicUsize::new(0)),
        }),
        rx,
    )
}
async fn status_until(
    handle: &mut VoicemailStatusHandle,
    predicate: impl Fn(&VoicemailStatus) -> bool,
) -> VoicemailStatus {
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let snapshot = handle.snapshot();
            if predicate(&snapshot) {
                return snapshot;
            }
            handle.changed().await.unwrap();
        }
    })
    .await
    .expect("status barrier")
}
async fn start(
    config: VoicemailConfig,
    classifier: Arc<dyn Classifier>,
) -> (PipelineWorker, VoicemailStatusHandle) {
    let (detector, gate, status) = voicemail_pair(classifier, config).unwrap();
    let mut worker = Pipeline::new(vec![Arc::new(detector), Arc::new(gate)])
        .start(Default::default())
        .await
        .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    (worker, status)
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn transcript(worker: &PipelineWorker, text: &str) {
    send(
        worker,
        FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            ..Default::default()
        }),
    )
    .await;
}
async fn finish(worker: PipelineWorker, status: &mut VoicemailStatusHandle) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(60), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    let snapshot = status.snapshot();
    assert_eq!(snapshot.retained_frames, 0);
    assert_eq!(snapshot.evidence_bytes, 0);
    assert_eq!(snapshot.supervisors, 0);
}
fn tts(text: &str) -> FrameData {
    FrameData::TtsText(AggregatedTextFrame {
        text: TextFrame::new(text),
        aggregated_by: "sentence".into(),
        context_id: None,
        raw_text: None,
        will_be_spoken: true,
    })
}

#[test]
fn unsupported_topologies_and_zero_limits_reject_before_setup() {
    let (classifier, _) = script();
    for topology in [VoicemailTopology::SpokenHistory, VoicemailTopology::Other] {
        assert!(matches!(
            voicemail_pair(
                classifier.clone(),
                VoicemailConfig {
                    topology,
                    ..Default::default()
                }
            ),
            Err(VoicemailConfigError::UnsupportedTopology)
        ));
    }
    assert!(matches!(
        voicemail_pair(
            classifier,
            VoicemailConfig {
                max_frames: 0,
                ..Default::default()
            }
        ),
        Err(VoicemailConfigError::RetentionLimit)
    ));
}
#[tokio::test(start_paused = true)]
async fn no_startup_fallback_stop_without_transcript_waits_full_silence() {
    let (classifier, mut calls) = script();
    let (worker, mut status) = start(Default::default(), classifier).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    assert_eq!(status.snapshot().phase, DetectionPhase::Pending);
    send(&worker, FrameData::UserStoppedSpeaking).await;
    status_until(&mut status, |s| s.phase == DetectionPhase::Conversation).await;
    assert_eq!(
        status.snapshot().event,
        Some(VoicemailEvent::ConversationDetected {
            session_id: status.snapshot().session_id,
            reason: EvidenceReason::NoAnswerFallback
        })
    );
    assert!(calls.try_recv().is_err());
    finish(worker, &mut status).await;
}
#[tokio::test(start_paused = true)]
async fn cumulative_sequential_coalescing_and_latest_success() {
    let (classifier, mut calls) = script();
    let (worker, mut status) = start(Default::default(), classifier.clone()).await;
    send(&worker, FrameData::UserStartedSpeaking).await;
    status_until(&mut status, |s| s.speaking).await;
    transcript(&worker, " first ").await;
    let first = calls.recv().await.unwrap();
    assert_eq!(first.state, Value::String("first".into()));
    assert_eq!(first.questions.len(), 1);
    let ClassifierQuestion::Choice(question) = &first.questions["voicemail"] else {
        panic!()
    };
    assert!(question.options["voicemail"]
        .as_ref()
        .unwrap()
        .as_str()
        .unwrap()
        .contains("all circuits are busy"));
    transcript(&worker, "second").await;
    transcript(&worker, " third ").await;
    status_until(&mut status, |s| s.revision == 3).await;
    first.complete.send(Some("voicemail")).unwrap();
    let second = calls.recv().await.unwrap();
    assert_eq!(second.state, Value::String("first second third".into()));
    second.complete.send(Some("conversation")).unwrap();
    status_until(&mut status, |s| s.attempted_revision == 3).await;
    assert_eq!(classifier.maximum.load(Ordering::SeqCst), 1);
    assert_eq!(status.snapshot().phase, DetectionPhase::Pending);
    send(&worker, FrameData::UserStoppedSpeaking).await;
    status_until(&mut status, |s| s.phase == DetectionPhase::Conversation).await;
    assert_eq!(status.snapshot().attempts, 2);
    finish(worker, &mut status).await;
}
#[tokio::test(start_paused = true)]
async fn prior_success_survives_failed_new_attempt_and_readiness_is_one_shot() {
    let (classifier, mut calls) = script();
    let (worker, mut status) = start(Default::default(), classifier).await;
    send(&worker, FrameData::UserStartedSpeaking).await;
    status_until(&mut status, |s| s.speaking).await;
    transcript(&worker, "leave a message").await;
    calls
        .recv()
        .await
        .unwrap()
        .complete
        .send(Some("voicemail"))
        .unwrap();
    status_until(&mut status, |s| s.attempted_revision == 1).await;
    transcript(&worker, "after beep").await;
    calls.recv().await.unwrap().complete.send(None).unwrap();
    status_until(&mut status, |s| s.attempted_revision == 2).await;
    send(&worker, FrameData::UserStoppedSpeaking).await;
    status_until(&mut status, |s| s.phase == DetectionPhase::VoicemailWaiting).await;
    assert!(status.snapshot().event.is_none());
    send(&worker, FrameData::UserStartedSpeaking).await;
    status_until(&mut status, |s| s.speaking).await;
    tokio::time::advance(Duration::from_secs(3)).await;
    assert_eq!(status.snapshot().phase, DetectionPhase::VoicemailWaiting);
    send(&worker, FrameData::UserStoppedSpeaking).await;
    status_until(&mut status, |s| s.phase == DetectionPhase::VoicemailReady).await;
    let event = status.snapshot().event;
    send(&worker, FrameData::UserStartedSpeaking).await;
    send(&worker, FrameData::UserStoppedSpeaking).await;
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(status.snapshot().event, event);
    assert_eq!(status.snapshot().missed_answers, 1);
    finish(worker, &mut status).await;
}
#[tokio::test(start_paused = true)]
async fn custom_classifier_outer_timeout_falls_back_without_hanging() {
    let (classifier, mut calls) = script();
    let (worker, mut status) = start(Default::default(), classifier.clone()).await;
    transcript(&worker, "hello").await;
    let held = calls.recv().await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(status.snapshot().phase, DetectionPhase::Pending);
    tokio::time::advance(Duration::from_secs(4)).await;
    status_until(&mut status, |s| s.phase == DetectionPhase::Conversation).await;
    assert_eq!(status.snapshot().missed_answers, 1);
    assert_eq!(classifier.active.load(Ordering::SeqCst), 0);
    assert!(held.complete.send(Some("voicemail")).is_err());
    finish(worker, &mut status).await;
}
#[tokio::test(start_paused = true)]
async fn held_frames_release_original_ids_in_order_and_end_drains() {
    let (classifier, _) = script();
    let (mut worker, mut status) = start(Default::default(), classifier).await;
    let frames = [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(tts("a")),
        Frame::new(tts("b")),
        Frame::new(FrameData::LlmResponseEnd),
    ];
    let ids: Vec<_> = frames.iter().map(|f| f.id).collect();
    for frame in frames {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    status_until(&mut status, |s| s.retained_frames == 4).await;
    assert!(worker.try_recv().is_err());
    send(&worker, FrameData::UserStoppedSpeaking).await;
    status_until(&mut status, |s| s.phase == DetectionPhase::Conversation).await;
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    let actual: Vec<_> = frames
        .iter()
        .filter(|f| ids.contains(&f.frame.id))
        .map(|f| f.frame.id)
        .collect();
    assert_eq!(actual, ids);
    assert_eq!(status.snapshot().completed_release_attempts, 4);
    assert_eq!(status.snapshot().retained_frames, 0);
}
#[tokio::test(start_paused = true)]
async fn unresolved_hold_fails_closed_and_drops_speech() {
    let (classifier, _) = script();
    let (worker, mut status) = start(
        VoicemailConfig {
            max_unresolved_hold: Duration::from_secs(2),
            ..Default::default()
        },
        classifier,
    )
    .await;
    send(&worker, tts("never release")).await;
    status_until(&mut status, |s| s.retained_frames == 1).await;
    status_until(&mut status, |s| s.phase == DetectionPhase::Failed).await;
    assert_eq!(status.snapshot().failure, Some(Failure::HoldTimeout));
    assert!(status.snapshot().event.is_none());
    assert_eq!(status.snapshot().retained_frames, 0);
    finish(worker, &mut status).await;
}
#[tokio::test(start_paused = true)]
async fn pending_stop_closes_without_verdict_or_flush() {
    let (classifier, _) = script();
    let (worker, mut status) = start(Default::default(), classifier).await;
    send(&worker, tts("discard on stop")).await;
    status_until(&mut status, |s| s.retained_frames == 1).await;
    send(&worker, FrameData::Stop).await;
    let (output, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Stopped);
    assert!(!output
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    assert_eq!(status.snapshot().retained_frames, 0);
    assert_eq!(status.snapshot().supervisors, 0);
    assert!(status.snapshot().event.is_none());
}
#[tokio::test(start_paused = true)]
async fn owner_drop_releases_buffers_while_status_survives() {
    let (classifier, mut calls) = script();
    let (worker, mut status) = start(Default::default(), classifier.clone()).await;
    transcript(&worker, "held classifier").await;
    let _call = calls.recv().await.unwrap();
    send(&worker, tts("held speech")).await;
    status_until(&mut status, |s| s.retained_frames == 1).await;
    drop(worker);
    let snapshot = status_until(&mut status, |s| s.supervisors == 0).await;
    assert_eq!(snapshot.retained_frames, 0);
    assert_eq!(snapshot.evidence_bytes, 0);
    assert_eq!(classifier.active.load(Ordering::SeqCst), 0);
}
#[tokio::test(start_paused = true)]
async fn frame_and_transcript_overflows_are_payload_free_failures() {
    let (classifier, _) = script();
    let (worker, mut status) = start(
        VoicemailConfig {
            max_frames: 1,
            ..Default::default()
        },
        classifier,
    )
    .await;
    send(&worker, tts("secret-one")).await;
    status_until(&mut status, |s| s.retained_frames == 1).await;
    send(&worker, tts("secret-two")).await;
    status_until(&mut status, |s| s.phase == DetectionPhase::Failed).await;
    assert_eq!(status.snapshot().failure, Some(Failure::FrameLimit));
    assert_eq!(status.snapshot().retained_frame_bytes, 0);
    assert!(!format!("{:?}", status.snapshot()).contains("secret"));
    finish(worker, &mut status).await;
    let (classifier, _) = script();
    let (worker, mut status) = start(
        VoicemailConfig {
            max_transcript_bytes: 3,
            ..Default::default()
        },
        classifier,
    )
    .await;
    transcript(&worker, "secret").await;
    status_until(&mut status, |s| s.phase == DetectionPhase::Failed).await;
    assert_eq!(status.snapshot().failure, Some(Failure::TranscriptLimit));
    finish(worker, &mut status).await;
}

struct PanickingClassifier;
#[async_trait]
impl Classifier for PanickingClassifier {
    async fn ask(
        &self,
        _: &Value,
        _: &Questions,
        _: CallOptions,
    ) -> pipecat_classifiers::Result<ClassificationBatch> {
        panic!("synthetic private classifier panic");
    }
}
#[tokio::test(start_paused = true)]
async fn custom_inference_panic_is_a_payload_free_missed_answer() {
    let (worker, mut status) = start(Default::default(), Arc::new(PanickingClassifier)).await;
    transcript(&worker, "hello").await;
    status_until(&mut status, |s| s.phase == DetectionPhase::Conversation).await;
    assert_eq!(status.snapshot().missed_answers, 1);
    assert!(!format!("{:?}", status.snapshot()).contains("private"));
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty());
    assert_eq!(status.snapshot().supervisors, 0);
}

/// An upstream TTS task deliberately finishes only after its prepare_end hook.
/// Core must join this task before forwarding the original terminal to the gate.
struct DeferredTtsTail {
    entered: mpsc::Sender<()>,
    ending: mpsc::Sender<()>,
    release: std::sync::Mutex<Option<oneshot::Receiver<()>>>,
    output: std::sync::Mutex<Option<Vec<Frame>>>,
}
#[async_trait]
impl FrameProcessor for DeferredTtsTail {
    fn name(&self) -> &str {
        "DeferredTtsTail"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::TtsSpeak(_)) {
            let release = self.release.lock().unwrap().take().unwrap();
            let output = self.output.lock().unwrap().take().unwrap();
            let original = ctx.clone();
            ctx.spawn("pre-terminal-tts-tail", true, async move {
                if release.await.is_err() {
                    return Ok(());
                }
                for frame in output {
                    original.push_downstream_wait(frame).await?;
                }
                Ok(())
            })?;
            self.entered.try_send(()).unwrap();
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> std::result::Result<(), ProcessorError> {
        self.ending.try_send(()).unwrap();
        Ok(())
    }
}
#[tokio::test(start_paused = true)]
async fn conversation_end_and_stop_keep_gate_open_for_preterminal_upstream_tts_tail() {
    for stop in [false, true] {
        for cancel in [false, true] {
            let (classifier, _) = script();
            let (detector, gate, mut status) =
                voicemail_pair(classifier, Default::default()).unwrap();
            let (entered, mut started) = mpsc::channel(1);
            let (ending, mut stopping) = mpsc::channel(1);
            let (release, wait) = oneshot::channel();
            let output = vec![
                Frame::new(FrameData::LlmResponseStart),
                Frame::new(FrameData::TtsStarted(Default::default())),
                Frame::new(tts("finish already admitted synthesis")),
                Frame::new(FrameData::TtsStopped(Default::default())),
                Frame::new(FrameData::LlmResponseEnd),
            ];
            let expected: Vec<_> = output.iter().map(|frame| frame.id).collect();
            let tts = Arc::new(DeferredTtsTail {
                entered,
                ending,
                release: std::sync::Mutex::new(Some(wait)),
                output: std::sync::Mutex::new(Some(output)),
            });
            let mut worker = Pipeline::new(vec![Arc::new(detector), tts, Arc::new(gate)])
                .start(PipelineConfig {
                    shutdown_timeout: Duration::from_secs(2),
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(worker.recv().await.unwrap().frame.data.is_start());
            send(&worker, FrameData::UserStoppedSpeaking).await;
            status_until(&mut status, |s| s.phase == DetectionPhase::Conversation).await;
            send(
                &worker,
                FrameData::TtsSpeak(TtsSpeakFrame::new("already admitted")),
            )
            .await;
            tokio::time::timeout(Duration::from_secs(1), started.recv())
                .await
                .unwrap()
                .unwrap();
            send(
                &worker,
                if stop {
                    FrameData::Stop
                } else {
                    FrameData::End(Default::default())
                },
            )
            .await;
            // This is the real upstream prepare_end barrier, reached only after
            // the detector's own coordinator has completed its terminal drain.
            tokio::time::timeout(Duration::from_secs(1), stopping.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(status.snapshot().retained_frames, 0);
            if cancel {
                worker
                    .cancel("cancel during graceful synthesis drain")
                    .await
                    .unwrap();
                drop(release);
            } else {
                release.send(()).unwrap();
            }
            let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
                .await
                .unwrap()
                .unwrap();
            let actual: Vec<_> = frames
                .iter()
                .filter(|frame| expected.contains(&frame.frame.id))
                .map(|frame| frame.frame.id)
                .collect();
            if cancel {
                assert_eq!(report.state, WorkerState::Cancelled);
                assert!(actual.is_empty());
            } else {
                assert_eq!(
                    report.state,
                    if stop {
                        WorkerState::Stopped
                    } else {
                        WorkerState::Ended
                    }
                );
                assert_eq!(actual, expected);
            }
            assert!(report.errors.is_empty());
            assert_eq!(status.snapshot().retained_frames, 0);
            assert_eq!(status.snapshot().supervisors, 0);
            assert!(!status.snapshot().drain_owner_running);
        }
    }
}
