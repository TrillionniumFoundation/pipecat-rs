#![cfg(feature = "voicemail")]
#[allow(dead_code)]
#[path = "support/review_voicemail_support.rs"]
mod support;
use pipecat_core::*;
use pipecat_processors::voicemail::*;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use support::*;
use tokio::sync::mpsc;

async fn state(
    status: &mut VoicemailStatusHandle,
    predicate: impl Fn(&VoicemailStatus) -> bool,
) -> VoicemailStatus {
    bounded(async {
        loop {
            let value = status.snapshot();
            if predicate(&value) {
                return value;
            }
            status.changed().await.unwrap();
        }
    })
    .await
}
async fn send(worker: &PipelineWorker, acked: &mut mpsc::Receiver<u64>, frame: Frame) -> u64 {
    let id = frame.id;
    bounded(worker.sender().send(frame, Direction::Downstream))
        .await
        .unwrap();
    ack(acked, id).await;
    id
}
struct BlockNormal {
    target: u64,
    entered: mpsc::Sender<()>,
}
#[async_trait]
impl FrameProcessor for BlockNormal {
    fn name(&self) -> &str {
        "independent-interruption-pressure"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.id == self.target {
            self.entered.try_send(()).unwrap();
            std::future::pending::<()>().await;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(start_paused = true)]
async fn independent_interrupt_during_blocked_release_discards_old_interruptible_and_restarts_uninterruptible(
) {
    for survives in [false, true] {
        let (classifier, mut requests) = scripted();
        let (detector, gate, mut status) = voicemail_pair(
            classifier,
            VoicemailConfig {
                decision_silence: Duration::from_millis(100),
                max_unresolved_hold: Duration::from_secs(3),
                ..Default::default()
            },
        )
        .unwrap();
        let (gate, mut admitted) = acknowledged(Arc::new(gate));
        let blocker = Frame::new(FrameData::Text(TextFrame::new(
            "block ordinary destination",
        )));
        let (entered, mut blocked) = mpsc::channel(1);
        let sink = Arc::new(BlockNormal {
            target: blocker.id,
            entered,
        });
        let mut worker = Pipeline::new(vec![Arc::new(detector), gate, sink])
            .start(PipelineConfig {
                queue_capacity: 1,
                output_capacity: 64,
                shutdown_timeout: Duration::from_secs(1),
                ..Default::default()
            })
            .await
            .unwrap();
        receive(&mut worker).await;
        send(&worker, &mut admitted, blocker).await;
        bounded(blocked.recv()).await.unwrap();
        let frames = vec![
            Frame::new(FrameData::TtsStarted(Default::default())),
            tts("old speech"),
            Frame::new(FrameData::TtsStopped(Default::default())),
        ];
        let ids: BTreeSet<_> = frames.iter().map(|frame| frame.id).collect();
        for frame in frames {
            send(&worker, &mut admitted, frame.with_interruptible(!survives)).await;
        }
        send(&worker, &mut admitted, transcript("hello")).await; // occupies the one normal destination slot
        bounded(requests.recv())
            .await
            .unwrap()
            .answer
            .send(Ok(batch("conversation", 1.0)))
            .unwrap();
        state(&mut status, |s| s.attempted_revision == 1).await;
        tokio::time::advance(Duration::from_millis(100)).await;
        let waiting = state(&mut status, |s| s.drain_in_flight).await;
        assert_eq!(waiting.retained_frames, 3);
        send(&worker, &mut admitted, Frame::interruption()).await;
        // The real interruption cancels the sink's occupied normal call and the
        // old gate drain. Only explicitly uninterruptible frames may restart.
        state(&mut status, |s| {
            s.retained_frames == 0 && !s.drain_in_flight
        })
        .await;
        worker.end().await.unwrap();
        let (outputs, report) = collect_bounded(worker).await.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 0);
        let observed: Vec<_> = outputs
            .iter()
            .filter(|frame| ids.contains(&frame.frame.id))
            .map(|frame| frame.frame.id)
            .collect();
        if survives {
            assert_eq!(observed, ids.into_iter().collect::<Vec<_>>());
        } else {
            assert!(observed.is_empty());
            assert_eq!(status.snapshot().discarded_frames, 3);
        }
        assert_eq!(status.snapshot().supervisors, 0);
        assert_eq!(status.snapshot().retained_frame_bytes, 0);
        assert!(matches!(
            status.snapshot().event,
            Some(VoicemailEvent::ConversationDetected { .. })
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn independent_voicemail_discards_uninterruptible_held_speech_and_rejects_stale_tail() {
    let (classifier, mut requests) = scripted();
    let (detector, gate, mut status) = voicemail_pair(
        classifier,
        VoicemailConfig {
            decision_silence: Duration::from_millis(100),
            message_silence: Duration::from_millis(200),
            ..Default::default()
        },
    )
    .unwrap();
    let gate = Arc::new(gate);
    let (gate_wrapper, mut admitted) = acknowledged(gate.clone());
    let mut worker = Pipeline::new(vec![Arc::new(detector), gate_wrapper.clone()])
        .start(Default::default())
        .await
        .unwrap();
    receive(&mut worker).await;
    let held = Frame::new(FrameData::TtsStarted(Default::default())).with_interruptible(false);
    let held_id = send(&worker, &mut admitted, held).await;
    let old = gate_wrapper
        .contexts
        .lock()
        .unwrap()
        .iter()
        .find(|(id, _)| *id == held_id)
        .unwrap()
        .1
        .clone();
    send(&worker, &mut admitted, transcript("leave a message")).await;
    bounded(requests.recv())
        .await
        .unwrap()
        .answer
        .send(Ok(batch("voicemail", 1.0)))
        .unwrap();
    state(&mut status, |s| s.attempted_revision == 1).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    loop {
        let output = receive(&mut worker).await;
        assert_ne!(output.frame.id, held_id);
        if matches!(output.frame.data, FrameData::Interruption)
            && output.direction == Direction::Downstream
        {
            break;
        }
    }
    assert_eq!(status.snapshot().retained_frames, 0);
    assert_eq!(status.snapshot().discarded_frames, 1);
    let stale = tts("stale provider tail").with_interruptible(false);
    let stale_id = stale.id;
    gate.process(stale, Direction::Downstream, &old)
        .await
        .unwrap();
    // A fresh application-controlled gate invocation can admit new speech.
    let heartbeat = Frame::new(FrameData::System);
    let heartbeat_id = send(&worker, &mut admitted, heartbeat).await;
    let fresh = gate_wrapper
        .contexts
        .lock()
        .unwrap()
        .iter()
        .find(|(id, _)| *id == heartbeat_id)
        .unwrap()
        .1
        .clone();
    let new = Frame::new(FrameData::TtsStarted(Default::default()));
    let new_id = new.id;
    gate.process(new, Direction::Downstream, &fresh)
        .await
        .unwrap();
    let new_stop = Frame::new(FrameData::TtsStopped(Default::default()));
    gate.process(new_stop, Direction::Downstream, &fresh)
        .await
        .unwrap();
    tokio::time::advance(Duration::from_millis(200)).await;
    state(&mut status, |s| s.phase == DetectionPhase::VoicemailReady).await;
    worker.end().await.unwrap();
    let (outputs, report) = collect_bounded(worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(!outputs
        .iter()
        .any(|f| f.frame.id == held_id || f.frame.id == stale_id));
    assert!(outputs.iter().any(|f| f.frame.id == new_id));
    assert_eq!(status.snapshot().supervisors, 0);
}

#[tokio::test(start_paused = true)]
async fn independent_voicemail_closed_input_preserves_system_worker_routes_and_blocks_normal_input()
{
    let (classifier, mut requests) = scripted();
    let (detector, gate, mut status) = voicemail_pair(
        classifier,
        VoicemailConfig {
            decision_silence: Duration::from_millis(10),
            message_silence: Duration::from_millis(10),
            ..Default::default()
        },
    )
    .unwrap();
    let (detector, mut admitted) = acknowledged(Arc::new(detector));
    let mut worker = Pipeline::new(vec![detector, Arc::new(gate)])
        .start(Default::default())
        .await
        .unwrap();
    receive(&mut worker).await;
    send(&worker, &mut admitted, transcript("voicemail greeting")).await;
    bounded(requests.recv())
        .await
        .unwrap()
        .answer
        .send(Ok(batch("voicemail", 1.0)))
        .unwrap();
    state(&mut status, |s| s.attempted_revision == 1).await;
    tokio::time::advance(Duration::from_millis(10)).await;
    state(&mut status, |s| s.phase == DetectionPhase::VoicemailWaiting).await;
    tokio::time::advance(Duration::from_millis(10)).await;
    state(&mut status, |s| s.phase == DetectionPhase::VoicemailReady).await;
    let event = status.snapshot().event;
    let mut allowed = BTreeSet::new();
    let mut denied = BTreeSet::new();
    for direction in [Direction::Downstream, Direction::Upstream] {
        for data in [
            FrameData::System,
            FrameData::Worker,
            FrameData::Task,
            FrameData::WorkerSystem,
            FrameData::TaskSystem,
            FrameData::InputText(TextFrame::new("system data stays allowed")),
            FrameData::Error(ErrorFrame::new("synthetic ordinary error")),
        ] {
            let frame = Frame::new(data);
            let id = frame.id;
            worker.sender().send(frame, direction).await.unwrap();
            ack(&mut admitted, id).await;
            allowed.insert(id);
        }
        for data in [
            FrameData::Heartbeat { timestamp: 7 },
            FrameData::Text(TextFrame::new("blocked normal")),
            FrameData::LlmText(TextFrame::llm("blocked LLM")),
            FrameData::Transcription(TranscriptionFrame {
                text: "blocked late final".into(),
                ..Default::default()
            }),
        ] {
            let frame = Frame::new(data);
            let id = frame.id;
            worker.sender().send(frame, direction).await.unwrap();
            ack(&mut admitted, id).await;
            denied.insert(id);
        }
    }
    worker.end().await.unwrap();
    let (outputs, report) = collect_bounded(worker).await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    let observed: BTreeSet<_> = outputs.iter().map(|f| f.frame.id).collect();
    assert!(
        allowed.is_subset(&observed),
        "missing allowed IDs: {:?}; worker errors: {:?}",
        allowed.difference(&observed).collect::<Vec<_>>(),
        report.errors
    );
    assert!(denied.is_disjoint(&observed));
    assert_eq!(status.snapshot().event, event);
    assert_eq!(status.snapshot().revision, 1);
    assert_eq!(status.snapshot().supervisors, 0);
}

#[tokio::test(start_paused = true)]
async fn independent_worker_terminal_requests_complete_in_both_directions_before_and_after_verdict()
{
    for decided in [false, true] {
        for direction in [Direction::Downstream, Direction::Upstream] {
            for terminal in 0..3 {
                let (classifier, mut requests) = scripted();
                let (detector, gate, mut status) = voicemail_pair(
                    classifier,
                    VoicemailConfig {
                        decision_silence: Duration::from_millis(10),
                        message_silence: Duration::from_millis(10),
                        ..Default::default()
                    },
                )
                .unwrap();
                let (detector, mut admitted) = acknowledged(Arc::new(detector));
                let mut worker = Pipeline::new(vec![detector, Arc::new(gate)])
                    .start(PipelineConfig {
                        shutdown_timeout: Duration::from_secs(1),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                receive(&mut worker).await;
                send(
                    &worker,
                    &mut admitted,
                    Frame::new(FrameData::TtsStarted(Default::default())),
                )
                .await;
                state(&mut status, |s| s.retained_frames == 1).await;
                if decided {
                    send(&worker, &mut admitted, transcript("leave a message")).await;
                    bounded(requests.recv())
                        .await
                        .unwrap()
                        .answer
                        .send(Ok(batch("voicemail", 1.0)))
                        .unwrap();
                    state(&mut status, |s| s.attempted_revision == 1).await;
                    tokio::time::advance(Duration::from_millis(10)).await;
                    state(&mut status, |s| s.phase == DetectionPhase::VoicemailWaiting).await;
                }
                let (request, expected) = match terminal {
                    0 => (FrameData::EndWorker(Default::default()), WorkerState::Ended),
                    1 => (FrameData::StopWorker, WorkerState::Stopped),
                    _ => (
                        FrameData::CancelWorker(Default::default()),
                        WorkerState::Cancelled,
                    ),
                };
                worker
                    .sender()
                    .send(Frame::new(request), direction)
                    .await
                    .unwrap();
                let (outputs, report) = collect_bounded(worker).await.unwrap();
                assert_eq!(
                    report.state, expected,
                    "decided={decided} direction={direction:?} terminal={terminal}"
                );
                assert!(!outputs
                    .iter()
                    .any(|f| matches!(f.frame.data, FrameData::TtsStarted(_))));
                let zero = state(&mut status, |s| s.supervisors == 0).await;
                assert_eq!(zero.retained_frames, 0);
                assert!(zero.event.is_none());
                assert_eq!(zero.evidence_bytes, 0);
            }
        }
    }
}
