//! Independent voicemail gate review: generated speech admission only.
//! No live provider, device, SpokenHistory integration, or outbound call/message.
#![cfg(feature = "voicemail")]
#[path = "support/review_voicemail_media_support.rs"]
mod media;
#[allow(dead_code)]
#[path = "support/review_voicemail_support.rs"]
mod support;

use media::*;
use pipecat_core::*;
use pipecat_media_transport::{
    OutputTransport, OutputTransportConfig, PlaybackReceipt, SpokenContext, SpokenHistory,
    SpokenHistoryConfig, SpokenHistorySink,
};
use pipecat_processors::voicemail::*;
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};
use support::{ack, acknowledged, batch, bounded, scripted, transcript};
use tokio::sync::{mpsc, oneshot};

fn config() -> VoicemailConfig {
    VoicemailConfig {
        decision_silence: Duration::from_millis(10),
        classifier_timeout: Duration::from_secs(3),
        max_unresolved_hold: Duration::from_secs(4),
        drain_timeout: Duration::from_secs(3),
        ..Default::default()
    }
}
fn pipeline_config() -> PipelineConfig {
    PipelineConfig {
        output_capacity: 64,
        max_background_tasks: 16,
        error_history_capacity: 16,
        startup_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}
async fn until(
    status: &mut VoicemailStatusHandle,
    predicate: impl Fn(&VoicemailStatus) -> bool,
) -> VoicemailStatus {
    bounded(async {
        loop {
            let snapshot = status.snapshot();
            if predicate(&snapshot) {
                return snapshot;
            }
            status
                .changed()
                .await
                .expect("gate status closed before expected state");
        }
    })
    .await
}
async fn submit(worker: &PipelineWorker, acknowledged: &mut mpsc::Receiver<u64>, frame: Frame) {
    let id = frame.id;
    bounded(worker.sender().send(frame, Direction::Downstream))
        .await
        .unwrap();
    ack(acknowledged, id).await;
}
async fn finish(mut worker: PipelineWorker) -> WorkerReport {
    bounded(worker.end()).await.unwrap();
    let mut count = 0;
    bounded(async {
        while worker.recv().await.is_some() {
            count += 1;
            assert!(count <= 128, "bounded completion frame journal");
        }
        worker.join().await.unwrap()
    })
    .await
}
async fn expect_originals(
    received: &mut mpsc::Receiver<DirectedFrame>,
    originals: &[Original],
    worker: &PipelineWorker,
) {
    let expected_ids: std::collections::BTreeSet<_> =
        originals.iter().map(|item| item.frame.id).collect();
    for original in originals {
        loop {
            let frame = bounded(received.recv()).await.unwrap_or_else(|| {
                panic!(
                    "original frame journal closed: worker={:?}, errors={:?}",
                    worker.state(),
                    worker.errors()
                )
            });
            if expected_ids.contains(&frame.frame.id) {
                original.check(&frame);
                break;
            }
            assert!(
                !matches!(
                    frame.frame.data,
                    FrameData::LlmResponseStart
                        | FrameData::LlmResponseEnd
                        | FrameData::TtsStarted(_)
                        | FrameData::TtsText(_)
                        | FrameData::TtsAudio(_)
                        | FrameData::TtsStopped(_)
                        | FrameData::TtsAlignmentDiagnostic(_)
                        | FrameData::AggregatedTextProgress(_)
                ),
                "unexpected or reconstructed response frame {:?}",
                frame.frame.data
            );
        }
    }
}

#[test]
fn explicit_spoken_history_and_other_topologies_are_rejected() {
    for topology in [VoicemailTopology::SpokenHistory, VoicemailTopology::Other] {
        let (classifier, _requests) = scripted();
        let result = voicemail_pair(
            classifier,
            VoicemailConfig {
                topology,
                ..config()
            },
        );
        match result {
            Err(error) => assert_eq!(error, VoicemailConfigError::UnsupportedTopology),
            Ok(_) => panic!("unsupported topology was accepted: {topology:?}"),
        }
    }
}

#[tokio::test]
async fn mismatched_active_tts_contexts_fail_closed() {
    bounded(async {
        let mut audio = AudioRawFrame::new([1_u8, 0].repeat(80), 8000, 1).unwrap();
        audio.context_id = Some("other-context".into());
        let invalid = [
            FrameData::TtsAudio(audio),
            FrameData::TtsText(AggregatedTextFrame {
                text: TextFrame::new("wrong context"),
                aggregated_by: "sentence".into(),
                context_id: Some("other-context".into()),
                raw_text: None,
                will_be_spoken: true,
            }),
            FrameData::TtsAlignmentDiagnostic(TtsAlignmentDiagnosticFrame {
                context_id: "other-context".into(),
                segment_id: None,
                evidence: TtsAlignmentEvidence::Unmatched,
                synthesis_text: "wrong context".into(),
                original_text: None,
                source_span: None,
                display_position: None,
                synthesis_outcome: TtsSynthesisOutcome::Pending,
            }),
            FrameData::AggregatedTextProgress(AggregatedTextProgressFrame {
                segment_id: 1,
                context_id: Some("other-context".into()),
                text: "wrong context".into(),
                aggregated_by: "word".into(),
                accumulated_text: "wrong context".into(),
                remaining_text: String::new(),
            }),
        ];
        for invalid in invalid {
            let (classifier, _requests) = scripted();
            let (detector, gate, mut status) = voicemail_pair(classifier, config()).unwrap();
            let (gate, mut admissions) = acknowledged(Arc::new(gate));
            let worker =
                bounded(Pipeline::new(vec![Arc::new(detector), gate]).start(pipeline_config()))
                    .await
                    .unwrap();
            submit(
                &worker,
                &mut admissions,
                Frame::new(FrameData::LlmResponseStart),
            )
            .await;
            submit(
                &worker,
                &mut admissions,
                Frame::new(FrameData::TtsStarted(TtsStartedFrame {
                    context_id: Some("active-context".into()),
                    append_to_context: true,
                })),
            )
            .await;
            submit(&worker, &mut admissions, Frame::new(invalid)).await;
            let failed = until(&mut status, |s| s.phase == DetectionPhase::Failed).await;
            assert_eq!(failed.failure, Some(Failure::UnsupportedTopology));
            assert_eq!(failed.completed_release_attempts, 0);
            assert_eq!(failed.retained_frames, 0);
            assert!(failed.event.is_none());
            let report = finish(worker).await;
            assert_eq!(report.state, WorkerState::Ended);
            assert!(report
                .errors
                .iter()
                .any(|e| e.error.contains("UnsupportedTopology")));
            assert_eq!(status.snapshot().supervisors, 0);
        }
    })
    .await;
}

struct NoHistoryCommit(std::sync::atomic::AtomicUsize);
impl SpokenHistorySink for NoHistoryCommit {
    fn commit(&self, _: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(vec![])
    }
}

#[tokio::test]
async fn actual_spoken_source_topology_cannot_silently_use_compact_mode() {
    bounded(async {
        let (classifier, _requests) = scripted();
        let (detector, gate, mut status) = voicemail_pair(classifier, config()).unwrap();
        let (gate, mut admissions) = acknowledged(Arc::new(gate));
        let sink = Arc::new(NoHistoryCommit(std::sync::atomic::AtomicUsize::new(0)));
        let history = SpokenHistory::new(SpokenHistoryConfig::default(), sink.clone()).unwrap();
        let source = Arc::new(
            history
                .source_boundary("review-actual-spoken-source")
                .unwrap(),
        );
        let (stamped, mut stamped_frames) = AdmissionJournal::new("review-source-stamp");
        let (backend, _submitted) = Receipts::new();
        let output = OutputTransport::new(
            "review-spoken-output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: Duration::from_secs(3),
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap()
        .with_spoken_history(history)
        .unwrap();
        let worker = bounded(
            Pipeline::new(vec![
                Arc::new(detector),
                source,
                stamped,
                gate,
                Arc::new(output),
            ])
            .start(pipeline_config()),
        )
        .await
        .unwrap();
        let start = Frame::new(FrameData::LlmResponseStart);
        let id = start.id;
        let token = start.observation_token();
        assert!(!start.metadata.contains_key("pipecat.media.spoken.source"));
        submit(&worker, &mut admissions, start).await;
        loop {
            let stamped = bounded(stamped_frames.recv()).await.unwrap();
            if stamped.frame.id == id {
                assert_eq!(stamped.frame.observation_token(), token);
                assert!(
                    stamped
                        .frame
                        .metadata
                        .contains_key("pipecat.media.spoken.source"),
                    "actual source boundary did not stamp original frame"
                );
                break;
            }
        }
        let failed = until(&mut status, |s| s.phase == DetectionPhase::Failed).await;
        assert_eq!(failed.failure, Some(Failure::UnsupportedTopology));
        assert_eq!(failed.retained_frames, 0);
        assert_eq!(failed.completed_release_attempts, 0);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
        assert_eq!(sink.0.load(Ordering::SeqCst), 0);
        let report = finish(worker).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report
            .errors
            .iter()
            .any(|e| e.error.contains("UnsupportedTopology")));
        assert_eq!(status.snapshot().supervisors, 0);
    })
    .await;
}

#[tokio::test]
async fn compact_originals_survive_ordered_release_with_new_arrivals_under_pressure() {
    bounded(async {
        let (classifier, mut requests) = scripted();
        let (detector, gate, mut status) = voicemail_pair(classifier, config()).unwrap();
        let (gate, mut admissions) = acknowledged(Arc::new(gate));
        let first = generated_response("review-first", false);
        let second = generated_response("review-second", false);
        let (entered, mut enter) = mpsc::channel(1);
        let (release, receive) = oneshot::channel();
        let (sent, mut released) = mpsc::channel(JOURNAL_CAPACITY);
        let barrier = Arc::new(BlockFirst {
            target: first[0].frame.id,
            entered,
            release: Mutex::new(Some(receive)),
            sent,
        });
        let mut settings = pipeline_config();
        settings.queue_capacity = 1;
        let worker =
            bounded(Pipeline::new(vec![Arc::new(detector), gate, barrier]).start(settings))
                .await
                .unwrap();
        submit(&worker, &mut admissions, transcript("Are you available?")).await;
        let request = bounded(requests.recv()).await.unwrap();
        for original in &first {
            submit(&worker, &mut admissions, original.frame.clone()).await;
        }
        assert_eq!(status.snapshot().retained_frames, first.len());
        assert_eq!(status.snapshot().completed_release_attempts, 0);
        request.answer.send(Ok(batch("conversation", 0.6))).unwrap();
        assert_eq!(bounded(enter.recv()).await.unwrap(), first[0].frame.id);
        // The receiver has acknowledged that the original first source boundary
        // is inside its blocked process call. New arrivals must join its tail.
        for original in &second {
            submit(&worker, &mut admissions, original.frame.clone()).await;
        }
        release.send(()).unwrap();
        let mut originals = first;
        originals.extend(second);
        expect_originals(&mut released, &originals, &worker).await;
        let drained = until(&mut status, |s| {
            s.retained_frames == 0 && !s.drain_in_flight
        })
        .await;
        assert_eq!(drained.completed_release_attempts, originals.len() as u64);
        assert_eq!(drained.discarded_frames, 0);
        assert_eq!(drained.retained_frame_bytes, 0);
        assert_eq!(drained.retained_pcm_bytes, 0);
        let report = finish(worker).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report.errors.is_empty(), "{report:?}");
        assert_eq!(status.snapshot().supervisors, 0);
    })
    .await;
}

async fn receipt_case(receipt: PlaybackReceipt) {
    let (classifier, mut requests) = scripted();
    let (detector, gate, mut status) = voicemail_pair(classifier, config()).unwrap();
    let (gate, mut admissions) = acknowledged(Arc::new(gate));
    let (journal, mut released) = AdmissionJournal::new("review-before-output");
    let (backend, mut submitted) = Receipts::new();
    let output = OutputTransport::new(
        "review-real-output",
        OutputTransportConfig {
            sample_rate: Some(8000),
            chunk_duration: Duration::from_millis(10),
            destinations: vec!["review-destination".into()],
            auto_silence: false,
            end_silence: Duration::ZERO,
            write_timeout: Duration::from_secs(3),
            ..Default::default()
        },
        backend.clone(),
        None,
        BTreeMap::new(),
    )
    .unwrap();
    let worker = bounded(
        Pipeline::new(vec![
            Arc::new(detector),
            gate,
            journal.clone(),
            Arc::new(output),
        ])
        .start(pipeline_config()),
    )
    .await
    .unwrap();
    submit(&worker, &mut admissions, transcript("Yes, hello")).await;
    let request = bounded(requests.recv()).await.unwrap();
    let originals = generated_response("review-output-context", false);
    for original in &originals {
        submit(&worker, &mut admissions, original.frame.clone()).await;
    }
    let held = status.snapshot();
    assert_eq!(held.phase, DetectionPhase::Pending);
    assert_eq!(held.retained_frames, originals.len());
    assert_eq!(held.completed_release_attempts, 0);
    assert_eq!(journal.audio.load(Ordering::SeqCst), 0);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 0);
    assert_eq!(backend.played.load(Ordering::SeqCst), 0);
    request.answer.send(Ok(batch("conversation", 0.6))).unwrap();
    expect_originals(&mut released, &originals, &worker).await;
    let submission = bounded(submitted.recv()).await.unwrap();
    assert_eq!(
        submission.frame.transport_destination.as_deref(),
        Some("review-destination")
    );
    let pcm = match &submission.frame.data {
        FrameData::TtsAudio(pcm) => pcm,
        other => panic!("unexpected submitted media {other:?}"),
    };
    assert_eq!(pcm.context_id.as_deref(), Some("review-output-context"));
    assert_eq!(pcm.audio, [1_u8, 0].repeat(80));
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(journal.audio.load(Ordering::SeqCst), 1);
    assert_eq!(backend.played.load(Ordering::SeqCst), 0);
    assert_eq!(backend.completed.load(Ordering::SeqCst), 0);
    submission.receipt.send(receipt).unwrap();
    let drained = until(&mut status, |s| {
        s.retained_frames == 0 && !s.drain_in_flight
    })
    .await;
    assert_eq!(drained.completed_release_attempts, originals.len() as u64);
    let report = finish(worker).await;
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.completed.load(Ordering::SeqCst), 1);
    assert_eq!(
        backend.played.load(Ordering::SeqCst),
        usize::from(receipt == PlaybackReceipt::Played)
    );
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(status.snapshot().supervisors, 0);
}

#[tokio::test]
async fn held_generated_speech_is_not_submitted_and_release_is_not_played() {
    bounded(receipt_case(PlaybackReceipt::Played)).await;
}

#[tokio::test]
async fn submitted_but_discarded_audio_is_never_counted_as_played() {
    bounded(receipt_case(PlaybackReceipt::Discarded)).await;
}

#[tokio::test]
async fn detailed_response_boundaries_fail_closed() {
    bounded(async {
        let (classifier, _requests) = scripted();
        let (detector, gate, mut status) = voicemail_pair(classifier, config()).unwrap();
        let (gate, mut admissions) = acknowledged(Arc::new(gate));
        let worker =
            bounded(Pipeline::new(vec![Arc::new(detector), gate]).start(pipeline_config()))
                .await
                .unwrap();
        submit(
            &worker,
            &mut admissions,
            Frame::new(FrameData::LlmFullResponseStart(
                LlmFullResponseFrame::default(),
            )),
        )
        .await;
        let failed = until(&mut status, |s| s.phase == DetectionPhase::Failed).await;
        assert_eq!(failed.failure, Some(Failure::UnsupportedTopology));
        assert_eq!(failed.completed_release_attempts, 0);
        assert_eq!(failed.retained_frames, 0);
        assert!(failed.event.is_none());
        let report = finish(worker).await;
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report
            .errors
            .iter()
            .any(|e| e.error.contains("UnsupportedTopology")));
        assert_eq!(status.snapshot().supervisors, 0);
    })
    .await;
}
