//! Native subscription witnesses use independent pipeline owners, not branches.
use pipecat_core::*;
use pipecat_processors::frame_subscription::*;
use std::{
    future::pending,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Semaphore;

#[derive(Default)]
struct Pipelines {
    workers: Vec<PipelineWorker>,
}
impl Pipelines {
    async fn start(&mut self, processor: Arc<dyn FrameProcessor>) -> usize {
        let worker = Pipeline::new(vec![processor])
            .start(Default::default())
            .await
            .unwrap();
        let index = self.workers.len();
        self.workers.push(worker);
        let frame = self.recv(index).await;
        if !frame.frame.data.is_start() {
            self.shutdown().await;
            panic!("expected startup frame");
        }
        index
    }
    async fn recv(&mut self, index: usize) -> DirectedFrame {
        match tokio::time::timeout(Duration::from_secs(5), self.workers[index].recv()).await {
            Ok(Some(frame)) => frame,
            _ => {
                self.shutdown().await;
                panic!("frame receive timed out or closed");
            }
        }
    }
    async fn until(&mut self, mut condition: impl FnMut() -> bool) {
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            while !condition() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if result.is_err() {
            self.shutdown().await;
            panic!("condition timed out");
        }
    }
    async fn send(&self, index: usize, frame: Frame, direction: Direction) {
        self.workers[index]
            .sender()
            .send(frame, direction)
            .await
            .unwrap();
    }
    async fn interrupt(&mut self, index: usize) {
        self.send(
            index,
            Frame::new(FrameData::Interruption),
            Direction::Downstream,
        )
        .await;
        loop {
            if matches!(self.recv(index).await.frame.data, FrameData::Interruption) {
                break;
            }
        }
    }
    async fn stop(&mut self, index: usize, terminal: FrameData) {
        self.send(index, Frame::new(terminal), Direction::Downstream)
            .await;
        let result = tokio::time::timeout(Duration::from_secs(5), async {
            while self.workers[index].recv().await.is_some() {}
            self.workers[index].join().await
        })
        .await;
        if !matches!(result, Ok(Ok(_))) {
            self.shutdown().await;
            panic!("terminal failed to join successfully");
        }
    }
    async fn shutdown(&mut self) {
        let mut timed_out = false;
        // First request cancellation from every unfinished owner. Even one
        // failed deadline must not skip cleanup attempts for the other owners.
        for worker in &self.workers {
            if !worker.has_finished() {
                timed_out |=
                    tokio::time::timeout(Duration::from_secs(5), worker.cancel("test cleanup"))
                        .await
                        .is_err();
            }
        }
        for worker in &mut self.workers {
            // Owners stay outside deadline futures. Execution errors (including
            // intentionally tested queue-full/startup failures) and AlreadyJoined
            // are allowed here; an unfinished join deadline is never swallowed.
            let result = tokio::time::timeout(Duration::from_secs(5), async {
                while worker.recv().await.is_some() {}
                worker.join().await
            })
            .await;
            timed_out |= result.is_err();
        }
        assert!(
            !timed_out,
            "cleanup failed to cancel/drain/join before its deadline"
        );
    }
}
fn texts() -> AsyncFramePredicate {
    Arc::new(|frame| Box::pin(async move { Ok(matches!(frame.data, FrameData::Text(_))) }))
}
fn all() -> AsyncFramePredicate {
    Arc::new(|_| Box::pin(async { Ok(true) }))
}
fn producer(
    config: FrameSubscriptionConfig,
    filter: AsyncFramePredicate,
    transform: Option<FrameTransformer>,
    passthrough: bool,
) -> Arc<ProducerProcessor> {
    Arc::new(ProducerProcessor::new(config, filter, transform, passthrough).unwrap())
}
fn consumer(
    producer: &ProducerProcessor,
    transform: Option<FrameTransformer>,
    direction: Direction,
) -> Arc<ConsumerProcessor> {
    Arc::new(ConsumerProcessor::new(
        producer.subscribe_handle(),
        transform,
        direction,
    ))
}
fn audio() -> Frame {
    Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![0, 1, 2, 3], 16000, 1).unwrap(),
    ))
}
fn audio_transform() -> FrameTransformer {
    Arc::new(|_| Box::pin(async { Ok(audio()) }))
}
fn gated(calls: Arc<AtomicUsize>, release: Arc<Semaphore>) -> FrameTransformer {
    Arc::new(move |frame| {
        let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
        let release = release.clone();
        Box::pin(async move {
            if first {
                release.acquire().await.unwrap().forget();
            }
            Ok(frame)
        })
    })
}

#[tokio::test]
async fn source_shapes_passthrough_nonmatch_fanout_and_two_audio_transform_sides() {
    for mode in 0..6 {
        let mut pipes = Pipelines::default();
        let p = producer(
            Default::default(),
            texts(),
            (mode == 4).then(audio_transform),
            mode != 1 && mode != 2,
        );
        let a = pipes.start(p.clone()).await;
        let b_processor = consumer(&p, (mode == 5).then(audio_transform), Direction::Downstream);
        let b = pipes.start(b_processor.clone()).await;
        let second = if mode == 2 || mode == 3 {
            Some(pipes.start(consumer(&p, None, Direction::Downstream)).await)
        } else {
            None
        };
        let text = Frame::text("hello");
        let identity = text.observation_token();
        pipes.send(a, text.clone(), Direction::Downstream).await;
        let copy = pipes.recv(b).await;
        let other = if let Some(c) = second {
            Some(pipes.recv(c).await)
        } else {
            None
        };
        let original = if mode != 1 && mode != 2 {
            Some(pipes.recv(a).await)
        } else {
            None
        };
        let nonmatch = Frame::new(FrameData::LlmRun);
        pipes.send(a, nonmatch.clone(), Direction::Upstream).await;
        let passed = pipes.recv(a).await;
        let snapshot = p.snapshot();
        pipes.shutdown().await;
        assert_eq!(passed.frame, nonmatch);
        assert_eq!(passed.direction, Direction::Upstream);
        assert_eq!(
            snapshot.buffer_admitted,
            if second.is_some() { 2 } else { 1 }
        );
        if mode >= 4 {
            assert!(matches!(copy.frame.data, FrameData::InputAudio(_)));
        } else {
            assert_eq!(copy.frame.observation_token(), identity);
            assert_eq!(copy.frame, text);
        }
        if let Some(copy) = other {
            assert_eq!(copy.frame.observation_token(), identity);
        }
        if let Some(original) = original {
            assert_eq!(original.frame, text);
        }
    }
}

#[tokio::test]
async fn identity_provenance_metadata_direction_and_local_consumer_input() {
    let mut pipes = Pipelines::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let hook_calls = calls.clone();
    let transform: FrameTransformer = Arc::new(move |frame| {
        hook_calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(frame) })
    });
    let p = producer(Default::default(), texts(), None, true);
    let a = pipes.start(p.clone()).await;
    let b_processor = consumer(&p, Some(transform), Direction::Upstream);
    let b = pipes.start(b_processor.clone()).await;
    let mut frame = Frame::text("native");
    let tag = LocalFrameTag::new();
    tag.mark(&mut frame).unwrap();
    frame.pts = Some(123);
    frame
        .metadata
        .insert("id".into(), serde_json::json!({"items": [1, "yes"]}));
    frame.transport_source = Some("A".into());
    frame.transport_destination = Some("B".into());
    pipes.send(a, frame.clone(), Direction::Downstream).await;
    let received = pipes.recv(b).await;
    let original = pipes.recv(a).await;
    let local = Frame::text("local B input");
    pipes.send(b, local.clone(), Direction::Downstream).await;
    let local_received = pipes.recv(b).await;
    pipes.shutdown().await;
    assert_eq!(received.direction, Direction::Upstream);
    assert_eq!(received.frame, frame);
    assert_eq!(
        received.frame.observation_token(),
        frame.observation_token()
    );
    assert!(tag.contains(&received.frame));
    assert_eq!(original.frame, frame);
    assert_eq!(local_received.frame, local);
    assert_eq!(local_received.direction, Direction::Downstream);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn independent_owned_mutation_registration_order_and_partial_prefix_failure() {
    for fail_second in [false, true] {
        let mut pipes = Pipelines::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let transform: FrameTransformer = Arc::new(move |mut frame| {
            let n = count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if fail_second && n == 1 {
                    return Err(ProcessorError::new("second hook failed"));
                }
                if let FrameData::Text(text) = &mut frame.data {
                    text.text.push_str(&format!("-{n}"));
                }
                frame.metadata.insert("copy".into(), serde_json::json!(n));
                Ok(frame)
            })
        });
        let p = producer(Default::default(), texts(), Some(transform), true);
        let a = pipes.start(p.clone()).await;
        let bp = consumer(&p, None, Direction::Downstream);
        let b = pipes.start(bp.clone()).await;
        let cp = consumer(&p, None, Direction::Downstream);
        let c = pipes.start(cp.clone()).await;
        let original = Frame::text("original");
        pipes.send(a, original.clone(), Direction::Downstream).await;
        let first = pipes.recv(b).await;
        let second = if fail_second {
            None
        } else {
            Some(pipes.recv(c).await)
        };
        let source = pipes.recv(a).await;
        let snapshot = p.snapshot();
        let c_snapshot = cp.snapshot();
        pipes.shutdown().await;
        assert!(matches!(&first.frame.data, FrameData::Text(t) if t.text == "original-0"));
        assert_eq!(first.frame.id, original.id);
        if let Some(second) = second {
            assert!(matches!(&second.frame.data, FrameData::Text(t) if t.text == "original-1"));
            assert_eq!(source.frame, original);
        } else {
            assert!(matches!(source.frame.data, FrameData::Error(_)));
            assert_eq!(snapshot.failed, 1);
            assert_eq!(snapshot.buffer_admitted, 1);
            assert_eq!(c_snapshot.buffer_admitted, 0);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}

fn profile_frames() -> Vec<Frame> {
    let t = TextFrame::new("t");
    let aggregated = AggregatedTextFrame {
        text: t.clone(),
        aggregated_by: "sentence".into(),
        context_id: Some("context".into()),
        raw_text: Some("raw".into()),
        will_be_spoken: true,
    };
    let transcription = TranscriptionFrame {
        text: "t".into(),
        user_id: "u".into(),
        timestamp: "now".into(),
        language: Some("en".into()),
        result: Some(serde_json::json!({"words": ["t"]})),
        finalized: true,
    };
    let detailed = DetailedTranscriptionFrame {
        text: t.clone(),
        user_id: "u".into(),
        timestamp: "now".into(),
        language: Some("en".into()),
        result: Some(serde_json::json!({"words": ["t"]})),
        finalized: true,
    };
    let pcm = AudioRawFrame::new(vec![0, 0], 16000, 1).unwrap();
    vec![
        FrameData::Text(t.clone()),
        FrameData::LlmText(t.clone()),
        FrameData::InputText(t.clone()),
        FrameData::VisionText(t),
        FrameData::AggregatedText(aggregated.clone()),
        FrameData::TtsText(aggregated),
        FrameData::Transcription(transcription.clone()),
        FrameData::InterimTranscription(transcription.clone()),
        FrameData::EagerTranscription(transcription.clone()),
        FrameData::Translation(transcription),
        FrameData::TranscriptionDetailed(detailed.clone()),
        FrameData::InterimTranscriptionDetailed(detailed.clone()),
        FrameData::EagerTranscriptionDetailed(detailed.clone()),
        FrameData::TranslationDetailed(detailed),
        FrameData::InputAudio(pcm.clone()),
        FrameData::OutputAudio(pcm.clone()),
        FrameData::TtsAudio(pcm.clone()),
        FrameData::SpeechOutputAudio(pcm.clone()),
        FrameData::UserAudio(UserAudioRawFrame {
            audio: pcm,
            user_id: "u".into(),
        }),
    ]
    .into_iter()
    .map(Frame::new)
    .collect()
}
#[tokio::test]
async fn all_nineteen_payloads_survive_independently_unequal_epochs() {
    let mut pipes = Pipelines::default();
    let p = producer(Default::default(), all(), None, false);
    let a = pipes.start(p.clone()).await;
    let bp = consumer(&p, None, Direction::Downstream);
    let b = pipes.start(bp.clone()).await;
    // B has a new epoch while A still has zero. Later A advances independently.
    pipes.interrupt(b).await;
    let mut comparisons = Vec::new();
    for (n, frame) in profile_frames().into_iter().enumerate() {
        if n == 10 {
            pipes.interrupt(a).await;
        }
        pipes.send(a, frame.clone(), Direction::Downstream).await;
        comparisons.push((frame, pipes.recv(b).await.frame));
    }
    let snapshot = bp.snapshot();
    pipes.shutdown().await;
    for (expected, actual) in comparisons {
        assert_eq!(expected, actual);
        assert_eq!(expected.observation_token(), actual.observation_token());
    }
    assert_eq!(snapshot.target_queued, 19);
    assert_eq!(snapshot.stale, 0);
}

#[tokio::test]
async fn producer_and_consumer_await_never_refresh_either_original_scope() {
    for hook_side in ["producer", "consumer"] {
        for interrupt_source in [false, true] {
            for frame_kind in 0..4 {
                let mut pipes = Pipelines::default();
                let calls = Arc::new(AtomicUsize::new(0));
                let release = Arc::new(Semaphore::new(0));
                let hook = gated(calls.clone(), release.clone());
                let p = producer(
                    Default::default(),
                    all(),
                    (hook_side == "producer").then(|| hook.clone()),
                    false,
                );
                let a = pipes.start(p.clone()).await;
                let bp = consumer(
                    &p,
                    (hook_side == "consumer").then(|| hook.clone()),
                    Direction::Downstream,
                );
                let b = pipes.start(bp.clone()).await;
                let mut old = match frame_kind {
                    0 => Frame::text("old"),
                    1 => Frame::new(FrameData::InputText(TextFrame::new("old"))),
                    2 => audio(),
                    _ => Frame::new(FrameData::UserAudio(UserAudioRawFrame {
                        audio: AudioRawFrame::new(vec![], 16000, 1).unwrap(),
                        user_id: "u".into(),
                    })),
                };
                old.interruptible = false;
                pipes.send(a, old, Direction::Downstream).await;
                pipes.until(|| calls.load(Ordering::SeqCst) > 0).await;
                let interrupted = if interrupt_source { a } else { b };
                // Sending Interruption advances the queue cutoff even if a
                // system-frame callback occupies that node's system loop.
                pipes
                    .send(
                        interrupted,
                        Frame::new(FrameData::Interruption),
                        Direction::Downstream,
                    )
                    .await;
                release.add_permits(1);
                loop {
                    if matches!(
                        pipes.recv(interrupted).await.frame.data,
                        FrameData::Interruption
                    ) {
                        break;
                    }
                }
                pipes
                    .until(|| p.snapshot().stale + bp.snapshot().stale > 0)
                    .await;
                let fresh = Frame::text("fresh");
                pipes.send(a, fresh.clone(), Direction::Downstream).await;
                let actual = pipes.recv(b).await;
                let before_close = bp.snapshot();
                pipes.shutdown().await;
                assert_eq!(
                    actual.frame, fresh,
                    "{hook_side}, source={interrupt_source}, kind={frame_kind}"
                );
                assert_eq!(before_close.target_queued, 1);
            }
        }
    }
}

#[tokio::test]
async fn snapshot_precedes_predicate_and_late_registration_waits_for_next_frame() {
    let mut pipes = Pipelines::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let predicate: AsyncFramePredicate = {
        let calls = calls.clone();
        let release = release.clone();
        Arc::new(move |_| {
            let first = calls.fetch_add(1, Ordering::SeqCst) == 0;
            let release = release.clone();
            Box::pin(async move {
                if first {
                    release.acquire().await.unwrap().forget();
                }
                Ok(true)
            })
        })
    };
    let p = producer(Default::default(), predicate, None, false);
    let a = pipes.start(p.clone()).await;
    let bp = consumer(&p, None, Direction::Downstream);
    let b = pipes.start(bp.clone()).await;
    let mut old = Frame::text("old");
    old.interruptible = false;
    pipes.send(a, old, Direction::Downstream).await;
    pipes.until(|| calls.load(Ordering::SeqCst) == 1).await;
    pipes.interrupt(b).await;
    let cp = consumer(&p, None, Direction::Downstream);
    let c = pipes.start(cp.clone()).await;
    release.add_permits(1);
    pipes.until(|| p.snapshot().stale == 1).await;
    let first_c = cp.snapshot();
    let fresh = Frame::text("fresh");
    pipes.send(a, fresh.clone(), Direction::Downstream).await;
    let fb = pipes.recv(b).await;
    let fc = pipes.recv(c).await;
    pipes.shutdown().await;
    assert_eq!(first_c.buffer_admitted, 0);
    assert_eq!(fb.frame, fresh);
    assert_eq!(fc.frame, fresh);
}

#[tokio::test]
async fn count_and_bytes_include_inflight_and_spare_capacity_cannot_hide() {
    for byte_limit in [false, true] {
        let mut pipes = Pipelines::default();
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let config = FrameSubscriptionConfig {
            max_pending_frames: if byte_limit { 4 } else { 1 },
            max_pending_bytes: if byte_limit { 42000 } else { 1024 * 1024 },
            ..Default::default()
        };
        let p = producer(config, texts(), None, false);
        let a = pipes.start(p.clone()).await;
        let bp = consumer(
            &p,
            Some(gated(calls.clone(), release.clone())),
            Direction::Downstream,
        );
        let b = pipes.start(bp.clone()).await;
        pipes
            .send(a, Frame::text("in flight"), Direction::Downstream)
            .await;
        pipes.until(|| calls.load(Ordering::SeqCst) == 1).await;
        pipes
            .send(a, Frame::text("full"), Direction::Downstream)
            .await;
        let rejected = pipes.recv(a).await;
        let held = bp.snapshot();
        release.add_permits(1);
        let received = pipes.recv(b).await;
        pipes.until(|| bp.snapshot().pending_frames == 0).await;
        pipes
            .send(a, Frame::text("after release"), Direction::Downstream)
            .await;
        let next = pipes.recv(b).await;
        pipes.shutdown().await;
        assert!(matches!(rejected.frame.data, FrameData::Error(_)));
        assert_eq!(held.pending_frames, 1);
        assert!(matches!(&received.frame.data, FrameData::Text(t) if t.text == "in flight"));
        assert!(matches!(&next.frame.data, FrameData::Text(t) if t.text == "after release"));
    }
}

#[tokio::test]
async fn invalid_payload_capacity_geometry_json_authority_pts_and_controls_fail_before_transform() {
    let owner = SpeculationOwner::new();
    let mut authority = Frame::text("authority");
    authority
        .set_native_speculation(owner.capability())
        .unwrap();
    let mut pts = Frame::text("clock");
    pts.set_pipeline_pts(123);
    let mut spare = String::with_capacity(128 * 1024);
    spare.push('x');
    let mut json = serde_json::json!(0);
    for _ in 0..10 {
        json = serde_json::json!([json]);
    }
    let mut deep = Frame::text("deep");
    deep.metadata.insert("nested".into(), json);
    let invalid = Frame::new(FrameData::InputAudio(AudioRawFrame {
        audio: vec![1],
        sample_rate: 16000,
        num_channels: 1,
        context_id: None,
    }));
    let cases = vec![
        authority,
        pts,
        Frame::text(spare),
        deep,
        invalid,
        Frame::new(FrameData::LlmRun),
        Frame::new(FrameData::ProcessorPause {
            processor: "other".into(),
        }),
    ];
    let mut pipes = Pipelines::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let transform: FrameTransformer = Arc::new(move |frame| {
        count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(frame) })
    });
    let config = FrameSubscriptionConfig {
        max_frame_bytes: 64000,
        max_json_depth: 4,
        ..Default::default()
    };
    let p = producer(config, all(), Some(transform), false);
    let a = pipes.start(p.clone()).await;
    let bp = consumer(&p, None, Direction::Downstream);
    let _b = pipes.start(bp.clone()).await;
    let mut errors = Vec::new();
    for frame in cases {
        pipes.send(a, frame, Direction::Downstream).await;
        errors.push(pipes.recv(a).await);
    }
    let snapshot = bp.snapshot();
    pipes.shutdown().await;
    assert_eq!(errors.len(), 7);
    assert!(errors
        .into_iter()
        .all(|f| matches!(f.frame.data, FrameData::Error(_))));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(snapshot.buffer_admitted, 0);
}

#[tokio::test]
async fn both_transform_outputs_are_revalidated_and_callback_failures_are_local() {
    for side in ["predicate", "producer", "consumer"] {
        for mode in 0..5 {
            let mut pipes = Pipelines::default();
            let filter: AsyncFramePredicate = if side == "predicate" {
                Arc::new(move |_| {
                    Box::pin(async move { Err(ProcessorError::new("predicate failed")) })
                })
            } else {
                texts()
            };
            let hook: FrameTransformer = Arc::new(move |_| {
                if mode == 3 {
                    panic!("construction");
                }
                Box::pin(async move {
                    match mode {
                        0 => Ok(Frame::new(FrameData::LlmRun)),
                        1 => Err(ProcessorError::new("hook failed")),
                        2 => panic!("poll"),
                        _ => pending().await,
                    }
                })
            });
            let config = FrameSubscriptionConfig {
                callback_timeout: Duration::from_millis(20),
                ..Default::default()
            };
            let p = producer(
                config,
                filter,
                (side == "producer").then(|| hook.clone()),
                false,
            );
            let a = pipes.start(p.clone()).await;
            let bp = consumer(
                &p,
                (side == "consumer").then(|| hook.clone()),
                Direction::Downstream,
            );
            let b = pipes.start(bp.clone()).await;
            pipes
                .send(a, Frame::text("fail"), Direction::Downstream)
                .await;
            let error_frame = pipes.recv(if side == "consumer" { b } else { a }).await;
            if side == "consumer" {
                pipes.until(|| !bp.snapshot().worker_running).await;
            }
            let source = p.snapshot();
            let target = bp.snapshot();
            pipes.shutdown().await;
            assert!(matches!(error_frame.frame.data, FrameData::Error(_)));
            if side == "consumer" {
                assert_eq!(target.failed, 1);
                assert!(target.closed);
                assert_eq!(source.consumers, 0);
                assert_eq!(target.pending_frames, 0);
            } else {
                assert_eq!(source.failed, 1);
                assert_eq!(target.buffer_admitted, 0);
            }
        }
    }
}

#[tokio::test]
async fn each_terminal_retires_only_its_side_and_objects_reject_reuse() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
    ] {
        for close_source in [false, true] {
            let mut pipes = Pipelines::default();
            let p = producer(Default::default(), texts(), None, false);
            let a = pipes.start(p.clone()).await;
            let bp = consumer(&p, None, Direction::Downstream);
            let b = pipes.start(bp.clone()).await;
            let cp = consumer(&p, None, Direction::Downstream);
            let c = pipes.start(cp.clone()).await;
            pipes
                .stop(if close_source { a } else { b }, terminal.clone())
                .await;
            pipes.until(|| !bp.snapshot().worker_running).await;
            let snapshot = p.snapshot();
            let b_closed = bp.snapshot();
            let still_active = if close_source {
                let local = Frame::text("B remains independent");
                pipes.send(b, local.clone(), Direction::Downstream).await;
                pipes.recv(b).await.frame == local
            } else {
                let shared = Frame::text("C remains subscribed");
                pipes.send(a, shared.clone(), Direction::Downstream).await;
                pipes.recv(c).await.frame == shared
            };
            pipes.shutdown().await;
            let p_reuse = Pipeline::new(vec![p.clone()])
                .start(Default::default())
                .await;
            let b_reuse = Pipeline::new(vec![bp.clone()])
                .start(Default::default())
                .await;
            let p_failed = p_reuse.is_err();
            let b_failed = b_reuse.is_err();
            if let Ok(mut worker) = p_reuse {
                let _ = worker.cancel("cleanup").await;
                let _ = worker.join().await;
            }
            if let Ok(mut worker) = b_reuse {
                let _ = worker.cancel("cleanup").await;
                let _ = worker.join().await;
            }
            assert!(still_active);
            assert!(b_closed.closed);
            assert_eq!(snapshot.consumers, if close_source { 0 } else { 1 });
            assert!(p_failed && b_failed);
        }
    }
}

#[tokio::test]
async fn terminal_cancels_full_queue_and_callback_without_waiting_for_deadline() {
    let mut pipes = Pipelines::default();
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let p = producer(
        FrameSubscriptionConfig {
            max_pending_frames: 2,
            ..Default::default()
        },
        texts(),
        None,
        false,
    );
    let a = pipes.start(p.clone()).await;
    let bp = consumer(
        &p,
        Some(gated(calls.clone(), release)),
        Direction::Downstream,
    );
    let b = pipes.start(bp.clone()).await;
    pipes
        .send(a, Frame::text("active"), Direction::Downstream)
        .await;
    pipes.until(|| calls.load(Ordering::SeqCst) == 1).await;
    pipes
        .send(a, Frame::text("queued"), Direction::Downstream)
        .await;
    pipes.until(|| bp.snapshot().pending_frames == 2).await;
    pipes.stop(b, FrameData::End(Default::default())).await;
    let snapshot = bp.snapshot();
    pipes.shutdown().await;
    assert!(snapshot.closed);
    assert!(!snapshot.worker_running);
    assert_eq!(snapshot.pending_frames, 0);
    assert_eq!(snapshot.discarded_on_close, 2);
    assert_eq!(p.snapshot().consumers, 0);
}

struct BlockLocal {
    inner: Arc<ConsumerProcessor>,
    entered: Arc<AtomicUsize>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for BlockLocal {
    fn name(&self) -> &str {
        "BlockLocalConsumer"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "block local") {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
#[tokio::test]
async fn target_queue_receipt_is_precise_full_is_error_and_admitted_effects_are_not_retracted() {
    for fill_queue in [false, true] {
        let mut pipes = Pipelines::default();
        let p = producer(Default::default(), texts(), None, false);
        let a = pipes.start(p.clone()).await;
        let bp = consumer(&p, None, Direction::Downstream);
        let entered = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let wrapper = Arc::new(BlockLocal {
            inner: bp.clone(),
            entered: entered.clone(),
            release: release.clone(),
        });
        let worker = Pipeline::new(vec![wrapper])
            .start(PipelineConfig {
                queue_capacity: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        let b = pipes.workers.len();
        pipes.workers.push(worker);
        let start = pipes.recv(b).await;
        pipes
            .send(b, Frame::text("block local"), Direction::Downstream)
            .await;
        pipes.until(|| entered.load(Ordering::SeqCst) == 1).await;
        let admitted = Frame::text("already admitted");
        pipes.send(a, admitted.clone(), Direction::Downstream).await;
        pipes.until(|| bp.snapshot().target_queued == 1).await;
        if fill_queue {
            pipes
                .send(a, Frame::text("queue full"), Direction::Downstream)
                .await;
            pipes.until(|| bp.snapshot().failed == 1).await;
        } else {
            pipes.stop(a, FrameData::Cancel(Default::default())).await;
        }
        let snapshot = bp.snapshot();
        release.add_permits(1);
        let mut seen = Vec::new();
        if !fill_queue {
            loop {
                let frame = pipes.recv(b).await;
                let is_admitted = frame.frame.id == admitted.id;
                seen.push(frame);
                if is_admitted {
                    break;
                }
            }
        }
        pipes.shutdown().await;
        assert!(start.frame.data.is_start());
        assert_eq!(snapshot.target_queued, 1);
        assert_eq!(snapshot.failed, u64::from(fill_queue));
        assert_eq!(snapshot.pending_frames, 0);
        if !fill_queue {
            assert!(seen.iter().any(|frame| frame.frame == admitted));
        }
        if fill_queue {
            assert_eq!(snapshot.buffer_admitted, 2);
        }
    }
}

struct StartupControl {
    inner: Arc<ConsumerProcessor>,
    mode: u8,
}
#[async_trait]
impl FrameProcessor for StartupControl {
    fn name(&self) -> &str {
        "SubscriptionStartupControl"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await?;
        if self.mode == 0 {
            ctx.spawn("occupied", false, async {
                pending::<()>().await;
                Ok(())
            })?;
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let start = frame.data.is_start();
        self.inner.process(frame.clone(), direction, ctx).await?;
        if start && self.mode == 1 {
            // This fresh node has no setup tasks: core allocates its first owned
            // task ID as 1. No await/yield occurs between spawn and this abort,
            // so the current-thread test exercises future-drop before first poll.
            if !ctx.cancel_task(1) {
                return Err(ProcessorError::new("first task missing"));
            }
        }
        if start && self.mode == 2 {
            self.inner.process(frame, direction, ctx).await?;
        }
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
#[tokio::test]
async fn rejected_spawn_and_unpolled_abort_unregister_but_duplicate_start_is_idempotent() {
    for mode in 0..3 {
        let mut pipes = Pipelines::default();
        let p = producer(Default::default(), texts(), None, false);
        let _a = pipes.start(p.clone()).await;
        let bp = consumer(&p, None, Direction::Downstream);
        let wrapper = Arc::new(StartupControl {
            inner: bp.clone(),
            mode,
        });
        let started = Pipeline::new(vec![wrapper])
            .start(PipelineConfig {
                max_background_tasks: 1,
                ..Default::default()
            })
            .await;
        let startup_rejected = started.is_err();
        let (first, second) = if let Ok(worker) = started {
            let b = pipes.workers.len();
            pipes.workers.push(worker);
            let first = Some(pipes.recv(b).await);
            let second = if mode == 2 {
                Some(pipes.recv(b).await)
            } else {
                None
            };
            (first, second)
        } else {
            (None, None)
        };
        if mode < 2 {
            pipes
                .until(|| !bp.snapshot().worker_running && bp.snapshot().closed)
                .await;
        }
        let p_snapshot = p.snapshot();
        let b_snapshot = bp.snapshot();
        pipes.shutdown().await;
        if mode == 0 {
            assert!(
                startup_rejected
                    || first.is_some_and(|frame| matches!(frame.frame.data, FrameData::Error(_)))
            );
        } else {
            assert!(first.unwrap().frame.data.is_start());
        }
        if mode < 2 {
            assert_eq!(p_snapshot.consumers, 0);
            assert_eq!(b_snapshot.pending_frames, 0);
            assert_eq!(b_snapshot.pending_bytes, 0);
        } else {
            assert_eq!(p_snapshot.consumers, 1);
            assert!(b_snapshot.worker_running);
            assert!(second.unwrap().frame.data.is_start());
        }
    }
}

#[tokio::test]
async fn dropping_either_pipeline_owner_releases_subscription_and_preserves_other_runtime() {
    for (drop_source, active) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut pipes = Pipelines::default();
        let p = producer(Default::default(), texts(), None, false);
        let a = pipes.start(p.clone()).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(Semaphore::new(0));
        let bp = consumer(
            &p,
            active.then(|| gated(calls.clone(), release)),
            Direction::Downstream,
        );
        let b = pipes.start(bp.clone()).await;
        if active {
            pipes
                .send(
                    a,
                    Frame::text("active callback at owner loss"),
                    Direction::Downstream,
                )
                .await;
            pipes.until(|| calls.load(Ordering::SeqCst) == 1).await;
        }
        let dropped = pipes.workers.remove(if drop_source { a } else { b });
        drop(dropped);
        pipes
            .until(|| {
                !bp.snapshot().worker_running && bp.snapshot().closed && p.snapshot().consumers == 0
            })
            .await;
        let independent = if drop_source {
            let frame = Frame::text("B still local");
            pipes.send(0, frame.clone(), Direction::Downstream).await;
            pipes.recv(0).await.frame == frame
        } else {
            let frame = Frame::new(FrameData::LlmRun);
            pipes.send(0, frame.clone(), Direction::Downstream).await;
            pipes.recv(0).await.frame == frame
        };
        let snapshot = bp.snapshot();
        pipes.shutdown().await;
        assert!(independent);
        assert_eq!(snapshot.pending_frames, 0);
        assert_eq!(snapshot.pending_bytes, 0);
    }
}

#[tokio::test]
async fn consumer_limit_rejects_new_registration_without_disturbing_existing_binding() {
    let mut pipes = Pipelines::default();
    let p = producer(
        FrameSubscriptionConfig {
            max_consumers: 1,
            ..Default::default()
        },
        texts(),
        None,
        false,
    );
    let a = pipes.start(p.clone()).await;
    let bp = consumer(&p, None, Direction::Downstream);
    let b = pipes.start(bp.clone()).await;
    let cp = consumer(&p, None, Direction::Downstream);
    let started = Pipeline::new(vec![cp.clone()])
        .start(Default::default())
        .await;
    let rejected = if let Ok(worker) = started {
        let c = pipes.workers.len();
        pipes.workers.push(worker);
        matches!(pipes.recv(c).await.frame.data, FrameData::Error(_))
    } else {
        true
    };
    let frame = Frame::text("old binding remains live");
    pipes.send(a, frame.clone(), Direction::Downstream).await;
    let actual = pipes.recv(b).await;
    let snapshot = p.snapshot();
    pipes.shutdown().await;
    assert!(rejected);
    assert_eq!(snapshot.consumers, 1);
    assert_eq!(actual.frame, frame);
}

#[test]
fn configuration_limits_reject_zero_unbounded_or_invalid_deadlines() {
    let base = FrameSubscriptionConfig::default();
    let invalid = [
        FrameSubscriptionConfig {
            max_consumers: 0,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_consumers: 257,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_pending_frames: 0,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_pending_frames: 4097,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_pending_bytes: 1,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_pending_bytes: 256 * 1024 * 1024 + 1,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_frame_bytes: 0,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_frame_bytes: 64 * 1024 * 1024 + 1,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_json_depth: 0,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_json_depth: 65,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_json_nodes: 0,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            max_json_nodes: 65537,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            callback_timeout: Duration::ZERO,
            ..base.clone()
        },
        FrameSubscriptionConfig {
            callback_timeout: Duration::from_secs(61),
            ..base
        },
    ];
    for config in invalid {
        assert!(ProducerProcessor::new(config, texts(), None, true).is_err());
    }
}

#[tokio::test]
async fn producer_must_be_ready_and_failed_consumer_cannot_rebind_in_new_run() {
    let mut pipes = Pipelines::default();
    let p = producer(Default::default(), texts(), None, false);
    let early = consumer(&p, None, Direction::Downstream);
    let result = Pipeline::new(vec![early.clone()])
        .start(Default::default())
        .await;
    let early_rejected = match result {
        Err(_) => true,
        Ok(worker) => {
            let index = pipes.workers.len();
            pipes.workers.push(worker);
            matches!(pipes.recv(index).await.frame.data, FrameData::Error(_))
        }
    };
    let a = pipes.start(p.clone()).await;
    let fresh = consumer(&p, None, Direction::Downstream);
    let b = pipes.start(fresh.clone()).await;
    let original = Frame::text("new explicit binding");
    pipes.send(a, original.clone(), Direction::Downstream).await;
    let actual = pipes.recv(b).await;
    pipes.shutdown().await;
    let reused = Pipeline::new(vec![early.clone()])
        .start(Default::default())
        .await;
    let reuse_rejected = reused.is_err();
    if let Ok(mut worker) = reused {
        let _ = worker.cancel("test cleanup").await;
        let _ = worker.join().await;
    }
    assert!(early_rejected && reuse_rejected);
    assert_eq!(actual.frame, original);
}

#[tokio::test]
async fn zero_consumers_skip_transform_and_lifecycle_bypasses_both_callbacks() {
    let mut pipes = Pipelines::default();
    let filters = Arc::new(AtomicUsize::new(0));
    let transforms = Arc::new(AtomicUsize::new(0));
    let filter_count = filters.clone();
    let filter: AsyncFramePredicate = Arc::new(move |frame| {
        filter_count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(matches!(frame.data, FrameData::Text(_))) })
    });
    let transform_count = transforms.clone();
    let transform: FrameTransformer = Arc::new(move |frame| {
        transform_count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(frame) })
    });
    let p = producer(Default::default(), filter, Some(transform), false);
    let a = pipes.start(p.clone()).await;
    pipes
        .send(
            a,
            Frame::text("matched without subscribers"),
            Direction::Downstream,
        )
        .await;
    pipes.until(|| p.snapshot().matched == 1).await;
    pipes.interrupt(a).await;
    let nonmatched = Frame::new(FrameData::LlmRun);
    pipes.send(a, nonmatched.clone(), Direction::Upstream).await;
    let actual = pipes.recv(a).await;
    pipes.stop(a, FrameData::End(Default::default())).await;
    let snapshot = p.snapshot();
    pipes.shutdown().await;
    assert_eq!(actual.frame, nonmatched);
    assert_eq!(actual.direction, Direction::Upstream);
    assert_eq!(filters.load(Ordering::SeqCst), 2);
    assert_eq!(transforms.load(Ordering::SeqCst), 0);
    assert_eq!(snapshot.matched, 1);
    assert_eq!(snapshot.buffer_admitted, 0);
    assert!(snapshot.closed);
}

struct EmitLocalError;
#[async_trait]
impl FrameProcessor for EmitLocalError {
    fn name(&self) -> &str {
        "EmitLocalError"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            Err(ProcessorError::new("local source error"))
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn error_origin_and_every_transform_output_boundary_are_checked() {
    let mut pipes = Pipelines::default();
    let fixture = pipes.start(Arc::new(EmitLocalError)).await;
    pipes
        .send(
            fixture,
            Frame::text("make attributed error"),
            Direction::Downstream,
        )
        .await;
    let mut attributed = pipes.recv(fixture).await.frame;
    let has_origin = attributed.error_origin().is_some();
    attributed.data = FrameData::Text(TextFrame::new("payload changed, origin retained"));
    let owner = Arc::new(SpeculationOwner::new());
    let mut checks = Vec::new();
    for side in ["input", "producer", "consumer"] {
        for mode in 0..6 {
            if side == "input" && mode != 0 {
                continue;
            }
            let attributed = attributed.clone();
            let owner = owner.clone();
            let hook: FrameTransformer = Arc::new(move |_| {
                let frame = match mode {
                    0 => attributed.clone(),
                    1 => {
                        let mut f = Frame::text("authority");
                        f.set_native_speculation(owner.capability()).unwrap();
                        f
                    }
                    2 => {
                        let mut f = Frame::text("clock");
                        f.set_pipeline_pts(1);
                        f
                    }
                    3 => Frame::new(FrameData::InputAudio(AudioRawFrame {
                        audio: vec![0],
                        sample_rate: 16000,
                        num_channels: 1,
                        context_id: None,
                    })),
                    4 => {
                        let mut text = String::with_capacity(128 * 1024);
                        text.push('x');
                        Frame::text(text)
                    }
                    _ => {
                        let mut f = Frame::text("deep output");
                        let mut json = serde_json::Value::Null;
                        for _ in 0..10 {
                            json = serde_json::json!([json]);
                        }
                        f.metadata.insert("deep".into(), json);
                        f
                    }
                };
                Box::pin(async move { Ok(frame) })
            });
            let p = producer(
                FrameSubscriptionConfig {
                    max_frame_bytes: 64000,
                    max_json_depth: 4,
                    ..Default::default()
                },
                texts(),
                (side == "producer").then(|| hook.clone()),
                false,
            );
            let a = pipes.start(p.clone()).await;
            let bp = consumer(
                &p,
                (side == "consumer").then(|| hook.clone()),
                Direction::Downstream,
            );
            let b = pipes.start(bp.clone()).await;
            let input = if side == "input" {
                attributed_for_input(&hook).await
            } else {
                Frame::text("valid input")
            };
            pipes.send(a, input, Direction::Downstream).await;
            let error_frame = pipes.recv(if side == "consumer" { b } else { a }).await;
            checks.push((side, error_frame, p.snapshot(), bp.snapshot()));
        }
    }
    pipes.shutdown().await;
    assert!(has_origin);
    for (side, error_frame, source, target) in checks {
        assert!(matches!(error_frame.frame.data, FrameData::Error(_)));
        assert_eq!(target.target_queued, 0);
        if side == "consumer" {
            assert_eq!(target.failed, 1);
        } else {
            assert_eq!(source.failed, 1);
            assert_eq!(target.buffer_admitted, 0);
        }
    }
}
async fn attributed_for_input(hook: &FrameTransformer) -> Frame {
    hook(Frame::text("ignored")).await.unwrap()
}
