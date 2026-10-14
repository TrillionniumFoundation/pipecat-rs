//! Real-pipeline parity and lifecycle checks for StatelessTextTransformer.
use pipecat_core::*;
use pipecat_processors::{
    AsyncTextTransform, StatelessTextTransformer, StatelessTextTransformerConfig,
};
use std::{
    future::pending,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

async fn start(transformer: StatelessTextTransformer) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![Arc::new(transformer)])
        .start(Default::default())
        .await
        .unwrap();
    assert!(recv(&mut worker).await.frame.data.is_start());
    worker
}
async fn recv(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .expect("bounded output wait")
        .expect("pipeline output")
}
async fn end(worker: PipelineWorker) -> WorkerReport {
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap()
        .1
}
async fn barrier(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    worker
        .sender()
        .send(
            Frame::new(FrameData::Flush(FlushFrame {
                returning: false,
                origin: Some("transform-barrier".into()),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let mut frames = vec![];
    loop {
        let next = recv(worker).await;
        if matches!(next.frame.data, FrameData::Flush(_)) {
            return frames;
        }
        frames.push(next);
    }
}
fn variants(value: &str) -> Vec<FrameData> {
    let text = TextFrame {
        text: value.into(),
        skip_tts: Some(true),
        includes_inter_frame_spaces: true,
        append_to_context: false,
    };
    let aggregate = AggregatedTextFrame {
        text: text.clone(),
        aggregated_by: "sentence".into(),
        context_id: Some("context".into()),
        raw_text: Some("raw".into()),
        will_be_spoken: false,
    };
    let transcript = TranscriptionFrame {
        text: value.into(),
        user_id: "speaker".into(),
        timestamp: "timestamp".into(),
        language: Some("en".into()),
        result: Some(serde_json::json!({"private":true})),
        finalized: true,
    };
    let detailed = DetailedTranscriptionFrame {
        text: text.clone(),
        user_id: transcript.user_id.clone(),
        timestamp: transcript.timestamp.clone(),
        language: transcript.language.clone(),
        result: transcript.result.clone(),
        finalized: true,
    };
    vec![
        FrameData::Text(text.clone()),
        FrameData::LlmText(text.clone()),
        FrameData::InputText(text.clone()),
        FrameData::VisionText(text),
        FrameData::AggregatedText(aggregate.clone()),
        FrameData::TtsText(aggregate),
        FrameData::Transcription(transcript.clone()),
        FrameData::InterimTranscription(transcript.clone()),
        FrameData::EagerTranscription(transcript.clone()),
        FrameData::Translation(transcript),
        FrameData::TranscriptionDetailed(detailed.clone()),
        FrameData::InterimTranscriptionDetailed(detailed.clone()),
        FrameData::EagerTranscriptionDetailed(detailed.clone()),
        FrameData::TranslationDetailed(detailed),
    ]
}
#[tokio::test]
async fn all_python_text_subclasses_get_fresh_plain_downstream_frames() {
    let source: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/text_transformer_source.json")).unwrap();
    let expected: std::collections::BTreeSet<&str> = source["source_classes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|name| name.as_str().unwrap())
        .collect();
    let cases = variants("source coverage");
    let actual: std::collections::BTreeSet<&str> =
        cases.iter().map(FrameData::source_class_name).collect();
    assert_eq!(actual, expected);
    let mut worker = start(
        StatelessTextTransformer::new_sync(Default::default(), |s| Ok(format!("[{s}]"))).unwrap(),
    )
    .await;
    for direction in [Direction::Upstream, Direction::Downstream] {
        for data in variants("你好 é") {
            let mut input = Frame::new(data);
            input.pts = Some(123);
            input
                .metadata
                .insert("private".into(), serde_json::json!(true));
            input.transport_source = Some("source".into());
            input.transport_destination = Some("destination".into());
            input.broadcast_sibling_id = Some(999);
            input.interruptible = false;
            let id = input.id;
            worker.sender().send(input, direction).await.unwrap();
            let out = recv(&mut worker).await;
            assert_eq!(out.direction, Direction::Downstream);
            assert_ne!(out.frame.id, id);
            assert!(out.frame.metadata.is_empty());
            assert!(out.frame.pts.is_none());
            assert!(out.frame.transport_source.is_none());
            assert!(out.frame.transport_destination.is_none());
            assert!(out.frame.broadcast_sibling_id.is_none());
            assert!(out.frame.interruptible);
            assert_eq!(out.frame.data, FrameData::Text(TextFrame::new("[你好 é]")));
        }
    }
    assert_eq!(end(worker).await.total_errors, 0);
}
#[tokio::test]
async fn empty_input_and_output_are_not_suppressed() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut worker = start(
        StatelessTextTransformer::new_sync(Default::default(), move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(String::new())
        })
        .unwrap(),
    )
    .await;
    for input in ["", "nonempty"] {
        worker
            .sender()
            .send(Frame::text(input), Direction::Upstream)
            .await
            .unwrap();
        let output = recv(&mut worker).await;
        assert_eq!(output.direction, Direction::Downstream);
        assert_eq!(output.frame.data, FrameData::Text(TextFrame::new("")));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(end(worker).await.total_errors, 0);
}
#[tokio::test]
async fn excluded_text_and_nontext_keep_original_frame_and_direction() {
    let mut worker = start(
        StatelessTextTransformer::new_sync(Default::default(), |_| {
            panic!("not a TextFrame subclass")
        })
        .unwrap(),
    )
    .await;
    for direction in [Direction::Upstream, Direction::Downstream] {
        for data in [
            FrameData::LlmThoughtText(TextFrame::new("thought")),
            FrameData::TtsSpeak(TtsSpeakFrame::new("speak")),
            FrameData::TtsTextInput(TtsTextInputFrame::new("native")),
            FrameData::OpenClawText(OpenClawTextFrame {
                text: "remote".into(),
                run_id: "run".into(),
            }),
            FrameData::AggregatedTextProgress(AggregatedTextProgressFrame {
                segment_id: 1,
                context_id: None,
                text: "source".into(),
                aggregated_by: "sentence".into(),
                accumulated_text: "so far".into(),
                remaining_text: "remaining".into(),
            }),
            FrameData::LlmResponseStart,
            FrameData::LlmResponseEnd,
            FrameData::InputAudio(AudioRawFrame::new(vec![0, 0], 16000, 1).unwrap()),
            FrameData::Control,
        ] {
            let mut frame = Frame::new(data);
            frame.metadata.insert("keep".into(), serde_json::json!(1));
            let original = frame.clone();
            worker.sender().send(frame, direction).await.unwrap();
            let output = recv(&mut worker).await;
            assert_eq!(output.direction, direction);
            assert_eq!(output.frame.id, original.id);
            assert_eq!(output.frame.data, original.data);
            assert_eq!(output.frame.metadata, original.metadata);
        }
    }
    assert_eq!(end(worker).await.total_errors, 0);
}
#[test]
fn config_limits_are_positive_and_bounded() {
    let good = StatelessTextTransformerConfig::default();
    assert_eq!(good.max_input_bytes, 1024 * 1024);
    assert_eq!(good.max_output_bytes, 1024 * 1024);
    assert_eq!(good.callback_timeout, Duration::from_secs(10));
    for config in [
        StatelessTextTransformerConfig {
            max_input_bytes: 0,
            ..good.clone()
        },
        StatelessTextTransformerConfig {
            max_input_bytes: 16 * 1024 * 1024 + 1,
            ..good.clone()
        },
        StatelessTextTransformerConfig {
            max_output_bytes: 0,
            ..good.clone()
        },
        StatelessTextTransformerConfig {
            max_output_bytes: 16 * 1024 * 1024 + 1,
            ..good.clone()
        },
        StatelessTextTransformerConfig {
            callback_timeout: Duration::ZERO,
            ..good.clone()
        },
        StatelessTextTransformerConfig {
            callback_timeout: Duration::from_secs(60) + Duration::from_nanos(1),
            ..good.clone()
        },
    ] {
        assert!(StatelessTextTransformer::new_sync(config, Ok).is_err());
    }
    assert!(StatelessTextTransformer::new_sync(
        StatelessTextTransformerConfig {
            max_input_bytes: 16 * 1024 * 1024,
            max_output_bytes: 16 * 1024 * 1024,
            callback_timeout: Duration::from_secs(60)
        },
        Ok
    )
    .is_ok());
}
#[tokio::test]
async fn errors_bounds_and_panics_emit_no_text_and_allow_recovery() {
    for failure in 0..5 {
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let callback: AsyncTextTransform = Arc::new(move |s| {
            count.fetch_add(1, Ordering::SeqCst);
            if s == "bad" && failure == 3 {
                panic!("factory panic");
            }
            Box::pin(async move {
                if s == "bad" {
                    match failure {
                        0 => return Err(ProcessorError::new("hook failed")),
                        1 => return Ok("éé".into()),
                        3 => unreachable!(),
                        4 => panic!("poll panic"),
                        _ => {}
                    }
                }
                Ok(s)
            })
        });
        let config = StatelessTextTransformerConfig {
            max_input_bytes: 3,
            max_output_bytes: 3,
            ..Default::default()
        };
        let mut worker =
            start(StatelessTextTransformer::new_async(config, callback).unwrap()).await;
        worker
            .sender()
            .send(
                Frame::text(if failure == 2 { "éé" } else { "bad" }),
                Direction::Downstream,
            )
            .await
            .unwrap();
        assert!(barrier(&mut worker)
            .await
            .iter()
            .all(|f| !matches!(f.frame.data, FrameData::Text(_))));
        assert_eq!(worker.total_errors(), 1);
        assert_eq!(calls.load(Ordering::SeqCst), usize::from(failure != 2));
        worker
            .sender()
            .send(Frame::text("é!"), Direction::Downstream)
            .await
            .unwrap();
        assert_eq!(
            recv(&mut worker).await.frame.data,
            FrameData::Text(TextFrame::new("é!"))
        );
        assert_eq!(end(worker).await.total_errors, 1);
    }
}
struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn pending_hook(
    entered: Arc<Notify>,
    release: Arc<Notify>,
    drops: Arc<AtomicUsize>,
) -> AsyncTextTransform {
    Arc::new(move |text| {
        let entered = entered.clone();
        let release = release.clone();
        let probe = DropProbe(drops.clone());
        Box::pin(async move {
            let _probe = probe;
            entered.notify_one();
            release.notified().await;
            Ok(text)
        })
    })
}
#[tokio::test]
async fn pending_async_callback_completes_in_original_invocation() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut worker = start(
        StatelessTextTransformer::new_async(
            Default::default(),
            pending_hook(entered.clone(), release.clone(), drops.clone()),
        )
        .unwrap(),
    )
    .await;
    worker
        .sender()
        .send(Frame::text("later"), Direction::Upstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    assert!(worker.try_recv().is_err());
    release.notify_one();
    let out = recv(&mut worker).await;
    assert_eq!(out.direction, Direction::Downstream);
    assert_eq!(out.frame.data, FrameData::Text(TextFrame::new("later")));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(end(worker).await.total_errors, 0);
}
#[tokio::test]
async fn timeout_drops_owned_future_and_recovers() {
    let drops = Arc::new(AtomicUsize::new(0));
    let count = drops.clone();
    let callback: AsyncTextTransform = Arc::new(move |s| {
        let probe = DropProbe(count.clone());
        Box::pin(async move {
            let _probe = probe;
            if s == "pending" {
                pending::<()>().await;
            }
            Ok(s)
        })
    });
    let mut worker = start(
        StatelessTextTransformer::new_async(
            StatelessTextTransformerConfig {
                callback_timeout: Duration::from_millis(20),
                ..Default::default()
            },
            callback,
        )
        .unwrap(),
    )
    .await;
    worker
        .sender()
        .send(Frame::text("pending"), Direction::Downstream)
        .await
        .unwrap();
    assert!(barrier(&mut worker)
        .await
        .iter()
        .all(|f| !matches!(f.frame.data, FrameData::Text(_))));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    worker
        .sender()
        .send(Frame::text("recovered"), Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(
        recv(&mut worker).await.frame.data,
        FrameData::Text(TextFrame::new("recovered"))
    );
    assert_eq!(end(worker).await.total_errors, 1);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn interruption_cancel_and_owner_drop_discard_pending_future() {
    for action in 0..3 {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let mut worker = start(
            StatelessTextTransformer::new_async(
                Default::default(),
                pending_hook(entered.clone(), release.clone(), drops.clone()),
            )
            .unwrap(),
        )
        .await;
        worker
            .sender()
            .send(Frame::text("stale"), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        match action {
            0 => {
                worker
                    .sender()
                    .send(Frame::interruption(), Direction::Downstream)
                    .await
                    .unwrap();
                let frames = barrier(&mut worker).await;
                assert!(frames
                    .iter()
                    .any(|f| matches!(f.frame.data, FrameData::Interruption)));
                assert!(!frames
                    .iter()
                    .any(|f| matches!(f.frame.data, FrameData::Text(_))));
                release.notify_one();
                assert!(barrier(&mut worker).await.is_empty());
                assert_eq!(end(worker).await.total_errors, 0);
            }
            1 => {
                worker.cancel("test cancellation").await.unwrap();
                let (frames, report) =
                    tokio::time::timeout(Duration::from_secs(3), worker.collect())
                        .await
                        .unwrap()
                        .unwrap();
                assert!(!frames
                    .iter()
                    .any(|f| matches!(f.frame.data, FrameData::Text(_))));
                assert_eq!(report.total_errors, 0);
            }
            _ => drop(worker),
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while drops.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned future released");
        release.notify_one();
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn end_and_stop_drain_pending_callback_before_terminal() {
    for stop in [false, true] {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let drops = Arc::new(AtomicUsize::new(0));
        let mut worker = start(
            StatelessTextTransformer::new_async(
                Default::default(),
                pending_hook(entered.clone(), release.clone(), drops.clone()),
            )
            .unwrap(),
        )
        .await;
        worker
            .sender()
            .send(Frame::text("drained"), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), entered.notified())
            .await
            .unwrap();
        if stop {
            worker
                .sender()
                .send(Frame::new(FrameData::Stop), Direction::Downstream)
                .await
                .unwrap();
        } else {
            worker.end().await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), worker.recv())
                .await
                .is_err()
        );
        release.notify_one();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(
            frames[0].frame.data,
            FrameData::Text(TextFrame::new("drained"))
        );
        assert!(frames[1].frame.is_terminal());
        assert_eq!(
            report.state,
            if stop {
                WorkerState::Stopped
            } else {
                WorkerState::Ended
            }
        );
        assert_eq!(report.total_errors, 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
#[tokio::test]
async fn native_semantic_authority_is_rejected_before_hook_but_passthrough_is_preserved() {
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let mut worker = start(
        StatelessTextTransformer::new_sync(Default::default(), move |s| {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(s)
        })
        .unwrap(),
    )
    .await;
    let owner = SpeculationOwner::new();
    for data in variants("authority") {
        let mut frame = Frame::new(data);
        frame.set_native_speculation(owner.capability()).unwrap();
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        assert!(!barrier(&mut worker)
            .await
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::Text(_))));
    }
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
    frame.set_native_speculation(owner.capability()).unwrap();
    let original = frame.clone();
    worker
        .sender()
        .send(frame, Direction::Upstream)
        .await
        .unwrap();
    let out = recv(&mut worker).await;
    assert_eq!(out.direction, Direction::Upstream);
    assert_eq!(out.frame.id, original.id);
    assert_eq!(
        out.frame.native_speculation(),
        original.native_speculation()
    );
    assert_eq!(end(worker).await.total_errors, 14);
}
#[tokio::test]
async fn pending_system_input_cannot_publish_after_interruption() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut worker = start(
        StatelessTextTransformer::new_async(
            Default::default(),
            pending_hook(entered.clone(), release.clone(), drops.clone()),
        )
        .unwrap(),
    )
    .await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputText(TextFrame::new("stale system text"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    release.notify_one();
    let frames = barrier(&mut worker).await;
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::Text(_))));
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::Interruption)));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(end(worker).await.total_errors, 0);
}

#[tokio::test]
async fn synchronous_errors_and_panics_use_the_same_recoverable_error_path() {
    for panic in [false, true] {
        let transform = StatelessTextTransformer::new_sync(Default::default(), move |text| {
            if text == "bad" {
                assert!(!panic, "synthetic synchronous panic");
                return Err(ProcessorError::new("synthetic synchronous error"));
            }
            Ok(text)
        })
        .unwrap();
        let mut worker = start(transform).await;
        worker
            .sender()
            .send(Frame::text("bad"), Direction::Downstream)
            .await
            .unwrap();
        assert!(barrier(&mut worker)
            .await
            .iter()
            .all(|f| !matches!(f.frame.data, FrameData::Text(_))));
        worker
            .sender()
            .send(Frame::text("recovered"), Direction::Downstream)
            .await
            .unwrap();
        assert_eq!(
            recv(&mut worker).await.frame.data,
            FrameData::Text(TextFrame::new("recovered"))
        );
        assert_eq!(end(worker).await.total_errors, 1);
    }
}
