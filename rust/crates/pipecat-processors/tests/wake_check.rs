#![cfg(feature = "wake-check")]
//! Real-pipeline witnesses for the bounded deprecated-filter migration profile.
use futures_util::FutureExt;
use pipecat_core::*;
use pipecat_processors::{WakeCheckFilter, WakeCheckFilterConfig};
use pipecat_text::wake_check::WakeCheckLimits;
use std::{
    panic::{resume_unwind, AssertUnwindSafe},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Semaphore;

#[derive(Default)]
struct Pipelines(Vec<Option<PipelineWorker>>);
impl Pipelines {
    fn worker(&mut self, i: usize) -> &mut PipelineWorker {
        self.0[i].as_mut().unwrap()
    }
    async fn start(&mut self, filter: Arc<dyn FrameProcessor>) -> usize {
        self.nodes(vec![filter], PipelineConfig::default()).await
    }
    async fn nodes(
        &mut self,
        nodes: Vec<Arc<dyn FrameProcessor>>,
        config: PipelineConfig,
    ) -> usize {
        let worker = Pipeline::new(nodes).start(config).await.unwrap();
        let index = self.0.len();
        self.0.push(Some(worker));
        assert!(self.recv(index).await.frame.data.is_start());
        index
    }
    async fn recv(&mut self, i: usize) -> DirectedFrame {
        tokio::time::timeout(Duration::from_secs(5), self.worker(i).recv())
            .await
            .expect("receive deadline")
            .expect("output closed")
    }
    async fn send(&mut self, i: usize, frame: Frame, direction: Direction) {
        self.worker(i)
            .sender()
            .send(frame, direction)
            .await
            .unwrap();
    }
    async fn barrier(&mut self, i: usize) -> Vec<DirectedFrame> {
        let marker = Frame::new(FrameData::Data);
        let id = marker.id;
        self.send(i, marker, Direction::Downstream).await;
        let mut before = Vec::new();
        loop {
            let output = self.recv(i).await;
            if output.frame.id == id {
                return before;
            }
            before.push(output);
        }
    }
    async fn filtered(&mut self, i: usize, frame: Frame) {
        self.send(i, frame, Direction::Downstream).await;
        assert!(self
            .barrier(i)
            .await
            .iter()
            .all(|f| transcription_text(&f.frame).is_none()));
    }
    async fn accepted(&mut self, i: usize, frame: Frame, direction: Direction) -> Frame {
        self.send(i, frame, direction).await;
        let output = self.recv(i).await;
        assert_eq!(output.direction, Direction::Downstream);
        output.frame
    }
    async fn finish(&mut self, i: usize, stop: bool) -> WorkerReport {
        if stop {
            self.send(i, Frame::new(FrameData::Stop), Direction::Downstream)
                .await;
        } else {
            self.worker(i).end().await.unwrap();
        }
        self.join(i).await
    }
    async fn join(&mut self, i: usize) -> WorkerReport {
        tokio::time::timeout(Duration::from_secs(5), async {
            let worker = self.worker(i);
            while worker.recv().await.is_some() {}
            worker.join().await.unwrap()
        })
        .await
        .expect("join deadline")
    }
    async fn cleanup(&mut self) {
        for worker in self.0.iter().flatten() {
            if !worker.has_finished() {
                let result =
                    tokio::time::timeout(Duration::from_secs(5), worker.cancel("test cleanup"))
                        .await;
                assert!(matches!(
                    result,
                    Ok(Ok(())) | Ok(Err(PipelineError::Closed))
                ));
            }
        }
        for worker in self.0.iter_mut().flatten() {
            let result = tokio::time::timeout(Duration::from_secs(5), async {
                while worker.recv().await.is_some() {}
                worker.join().await
            })
            .await;
            assert!(matches!(
                result,
                Ok(Ok(_))
                    | Ok(Err(PipelineError::AlreadyJoined))
                    | Ok(Err(PipelineError::Execution(_)))
            ));
        }
    }
}
macro_rules! pipeline_test {
    ($name:ident, |$pipes:ident| $body:block) => {
        #[tokio::test(start_paused = true)]
        async fn $name() {
            let mut owners = Pipelines::default();
            let result = AssertUnwindSafe(async {
                let $pipes = &mut owners;
                $body
            })
            .catch_unwind()
            .await;
            owners.cleanup().await;
            if let Err(payload) = result {
                resume_unwind(payload);
            }
        }
    };
}
fn filter() -> Arc<WakeCheckFilter> {
    Arc::new(WakeCheckFilter::new(Default::default(), ["hey bot"]).unwrap())
}
fn transcription(user_id: &str, text: &str) -> Frame {
    Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        user_id: user_id.into(),
        timestamp: "source timestamp".into(),
        language: Some("en".into()),
        result: Some(serde_json::json!({"preserve": 1})),
        finalized: false,
    }))
}
fn transcription_text(frame: &Frame) -> Option<&str> {
    match &frame.data {
        FrameData::Transcription(f) => Some(&f.text),
        FrameData::TranscriptionDetailed(f) => Some(&f.text.text),
        _ => None,
    }
}
fn assert_text(frame: &Frame, expected: &str) {
    assert_eq!(transcription_text(frame), Some(expected));
}
fn with_text(mut frame: Frame, text: &str) -> Frame {
    match &mut frame.data {
        FrameData::Transcription(f) => f.text = text.into(),
        FrameData::TranscriptionDetailed(f) => f.text.text = text.into(),
        _ => unreachable!(),
    }
    frame
}

#[test]
fn configuration_has_native_finite_hard_bounds() {
    let base = WakeCheckFilterConfig::default();
    assert_eq!(base.keepalive_timeout, Duration::from_secs(3));
    let mut bad = Vec::new();
    for timeout in [
        Duration::ZERO,
        Duration::from_secs(86400) + Duration::from_nanos(1),
        Duration::MAX,
    ] {
        bad.push(WakeCheckFilterConfig {
            keepalive_timeout: timeout,
            ..base.clone()
        });
    }
    for (field, above) in [(0, 65), (1, 513), (2, 16385), (3, 131073)] {
        for value in [0, above] {
            let mut config = base.clone();
            match field {
                0 => config.max_participants = value,
                1 => config.max_user_id_bytes = value,
                2 => config.max_participant_bytes = value,
                _ => config.max_total_retained_bytes = value,
            }
            bad.push(config);
        }
    }
    for config in bad {
        assert!(WakeCheckFilter::new(config, ["wake"]).is_err());
    }
    assert!(WakeCheckFilter::new(base.clone(), [""]).is_err());
    assert!(WakeCheckFilter::new(base.clone(), ["唤醒"]).is_err());
    assert!(WakeCheckFilter::new(base, std::iter::empty::<&str>()).is_err());
}

pipeline_test!(
    participant_isolation_plain_concatenation_and_current_identity,
    |pipes| {
        let i = pipes.start(filter()).await;
        pipes
            .filtered(i, transcription("alice", "前缀。 hey"))
            .await;
        pipes
            .filtered(i, transcription("bob", "bot wrong participant"))
            .await;
        let mut frame = transcription("alice", "bot, 你好");
        frame.name = "current-source-frame".into();
        frame.set_pipeline_pts(19);
        frame
            .metadata
            .insert("preserve".into(), serde_json::json!({"x": [1, 2]}));
        frame.transport_source = Some("microphone".into());
        frame.transport_destination = Some("destination".into());
        frame.broadcast_sibling_id = Some(41);
        let tag = LocalFrameTag::new();
        tag.mark(&mut frame).unwrap();
        let token = frame.observation_token();
        let expected = with_text(frame.clone(), "heybot, 你好");
        let output = pipes.accepted(i, frame, Direction::Upstream).await;
        assert_eq!(output, expected);
        assert_eq!(output.observation_token(), token);
        assert!(output.has_pipeline_pts());
        assert!(tag.contains(&output));
        pipes.filtered(i, transcription("bob", " still idle")).await;
        assert_text(
            &pipes
                .accepted(i, transcription("alice", "awake"), Direction::Upstream)
                .await,
            "awake",
        );
        pipes.filtered(i, transcription("", "hey")).await;
        assert_text(
            &pipes
                .accepted(i, transcription("", "bot"), Direction::Downstream)
                .await,
            "heybot",
        );
        assert_eq!(pipes.finish(i, false).await.total_errors, 0);
    }
);

pipeline_test!(
    detailed_frame_and_configuration_order_preserve_all_fields,
    |pipes| {
        let gate = Arc::new(WakeCheckFilter::new(Default::default(), ["later", "early"]).unwrap());
        let i = pipes.start(gate).await;
        let mut text = TextFrame::new("前。 early content LATER suffix");
        text.skip_tts = Some(true);
        text.append_to_context = false;
        text.includes_inter_frame_spaces = true;
        let frame = Frame::new(FrameData::TranscriptionDetailed(
            DetailedTranscriptionFrame {
                text,
                user_id: "u".into(),
                timestamp: "yesterday".into(),
                language: Some("zh".into()),
                result: Some(serde_json::json!([1, "provider"])),
                finalized: false,
            },
        ));
        let expected = with_text(frame.clone(), "LATER suffix");
        let output = pipes.accepted(i, frame, Direction::Upstream).await;
        assert_eq!(output, expected);
        let mut finalized = transcription("u", "final remains unchanged");
        if let FrameData::Transcription(f) = &mut finalized.data {
            f.finalized = true;
        }
        assert_eq!(
            pipes
                .accepted(i, finalized.clone(), Direction::Downstream)
                .await,
            finalized
        );
    }
);

pipeline_test!(
    rolling_keepalive_strict_deadline_and_empty_refresh,
    |pipes| {
        let i = pipes.start(filter()).await;
        pipes
            .accepted(i, transcription("a", "hey bot"), Direction::Downstream)
            .await;
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_text(
            &pipes
                .accepted(i, transcription("a", ""), Direction::Downstream)
                .await,
            "",
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_text(
            &pipes
                .accepted(i, transcription("a", "roll"), Direction::Downstream)
                .await,
            "roll",
        );
        tokio::time::advance(Duration::from_secs(3)).await;
        pipes
            .filtered(i, transcription("a", "deadline expired. "))
            .await;
        assert_text(
            &pipes
                .accepted(i, transcription("a", "heybot retry"), Direction::Downstream)
                .await,
            "heybot retry",
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        let bypass = Frame::text("hey bot does not refresh");
        pipes.send(i, bypass.clone(), Direction::Upstream).await;
        assert_eq!(pipes.recv(i).await.frame, bypass);
        tokio::time::advance(Duration::from_secs(1)).await;
        pipes.filtered(i, transcription("a", "expired again")).await;
    }
);

pipeline_test!(
    all_excluded_variants_keep_original_direction_and_identity,
    |pipes| {
        let i = pipes.start(filter()).await;
        for direction in [Direction::Upstream, Direction::Downstream] {
            let compact = TranscriptionFrame {
                text: "hey bot".into(),
                ..Default::default()
            };
            let detailed = DetailedTranscriptionFrame {
                text: TextFrame::new("hey bot"),
                user_id: "u".into(),
                timestamp: "t".into(),
                language: None,
                result: None,
                finalized: false,
            };
            for data in [
                FrameData::InterimTranscription(compact.clone()),
                FrameData::EagerTranscription(compact.clone()),
                FrameData::Translation(compact),
                FrameData::InterimTranscriptionDetailed(detailed.clone()),
                FrameData::EagerTranscriptionDetailed(detailed.clone()),
                FrameData::TranslationDetailed(detailed),
                FrameData::Text(TextFrame::new("hey bot")),
                FrameData::InputText(TextFrame::new("hey bot")),
                FrameData::Control,
                FrameData::Data,
            ] {
                let mut frame = Frame::new(data);
                frame.metadata.insert("keep".into(), true.into());
                let original = frame.clone();
                pipes.send(i, frame, direction).await;
                let output = pipes.recv(i).await;
                assert_eq!(output.direction, direction);
                assert_eq!(output.frame, original);
            }
        }
        pipes.filtered(i, transcription("u", "still idle")).await;
    }
);

pipeline_test!(
    interruption_preserves_input_accumulator_and_awake_window,
    |pipes| {
        let i = pipes.start(filter()).await;
        pipes.filtered(i, transcription("a", "prefix hey")).await;
        pipes
            .send(i, Frame::interruption(), Direction::Downstream)
            .await;
        assert!(matches!(
            pipes.recv(i).await.frame.data,
            FrameData::Interruption
        ));
        assert_text(
            &pipes
                .accepted(i, transcription("a", "bot"), Direction::Downstream)
                .await,
            "heybot",
        );
        pipes
            .send(i, Frame::interruption(), Direction::Downstream)
            .await;
        assert!(matches!(
            pipes.recv(i).await.frame.data,
            FrameData::Interruption
        ));
        assert_text(
            &pipes
                .accepted(
                    i,
                    transcription("a", "same input window"),
                    Direction::Downstream,
                )
                .await,
            "same input window",
        );
    }
);

pipeline_test!(
    idle_authority_rejection_does_not_cache_or_refresh_awake_preserves_it,
    |pipes| {
        let i = pipes.start(filter()).await;
        let owner = SpeculationOwner::new();
        let mut rejected = transcription("u", "hey");
        rejected.set_native_speculation(owner.capability()).unwrap();
        pipes.filtered(i, rejected).await;
        assert_eq!(pipes.worker(i).total_errors(), 1);
        pipes
            .filtered(i, transcription("u", "bot not cached. "))
            .await;
        pipes
            .accepted(i, transcription("u", "hey bot"), Direction::Downstream)
            .await;
        let mut awake = transcription("u", "authority unchanged");
        awake.set_native_speculation(owner.capability()).unwrap();
        let expected = awake.clone();
        let output = pipes.accepted(i, awake, Direction::Upstream).await;
        assert_eq!(output, expected);
        assert_eq!(output.native_speculation(), expected.native_speculation());
        tokio::time::advance(Duration::from_secs(3)).await;
        let mut expired = transcription("u", "hey bot forbidden");
        expired.set_native_speculation(owner.capability()).unwrap();
        pipes.filtered(i, expired).await;
        pipes.filtered(i, transcription("u", "not awake")).await;
        assert_eq!(pipes.worker(i).total_errors(), 2);
    }
);

pipeline_test!(
    native_eager_final_idle_rejected_awake_original_receipt_survives,
    |pipes| {
        let i = pipes.start(filter()).await;
        let source = EagerSourceSessionOwner::new(4096).unwrap();
        let turn = source.begin_turn().unwrap();
        let mut idle = transcription("u", "hey bot source");
        let receipt = source.offer_final(&turn, idle.data.clone()).unwrap();
        idle.set_native_eager_source(NativeEagerSource::Final(receipt))
            .unwrap();
        pipes.filtered(i, idle).await;
        assert_eq!(pipes.worker(i).total_errors(), 1);
        pipes
            .accepted(i, transcription("u", "hey bot"), Direction::Downstream)
            .await;
        let turn = source.begin_turn().unwrap();
        let mut awake = transcription("u", "source final");
        let receipt = source.offer_final(&turn, awake.data.clone()).unwrap();
        awake
            .set_native_eager_source(NativeEagerSource::Final(receipt))
            .unwrap();
        let expected = awake.clone();
        let output = pipes.accepted(i, awake, Direction::Downstream).await;
        assert_eq!(output, expected);
        assert!(output.native_eager_source().is_some());
        assert!(output.require_no_native_llm_continuation().is_err());
    }
);

pipeline_test!(
    capacity_rejections_leave_existing_and_new_participants_retryable,
    |pipes| {
        let config = WakeCheckFilterConfig {
            max_participants: 1,
            max_user_id_bytes: 2,
            max_participant_bytes: 8,
            max_total_retained_bytes: 9,
            ..Default::default()
        };
        let i = pipes
            .start(Arc::new(WakeCheckFilter::new(config, ["hey bot"]).unwrap()))
            .await;
        pipes.filtered(i, transcription("long-id", "hey bot")).await;
        pipes.filtered(i, transcription("u", "hey")).await;
        pipes.filtered(i, transcription("v", "hey bot")).await;
        pipes.filtered(i, transcription("u", "xxxxxxxx")).await;
        assert_text(
            &pipes
                .accepted(i, transcription("u", "bot"), Direction::Downstream)
                .await,
            "heybot",
        );
        // Awake payloads are not copied into retained/matching storage.
        let large = "large passthrough".repeat(1024);
        assert_text(
            &pipes
                .accepted(i, transcription("u", &large), Direction::Downstream)
                .await,
            &large,
        );
        assert_eq!(pipes.worker(i).total_errors(), 3);
    }
);

pipeline_test!(
    total_budget_counts_ids_and_failed_new_entry_is_not_retained,
    |pipes| {
        let config = WakeCheckFilterConfig {
            max_total_retained_bytes: 8,
            ..Default::default()
        };
        let i = pipes
            .start(Arc::new(WakeCheckFilter::new(config, ["go"]).unwrap()))
            .await;
        pipes.filtered(i, transcription("abc", "x ")).await; // 3 + 2 bytes retained.
        pipes.filtered(i, transcription("def", "z")).await; // Would retain 9.
        assert_eq!(pipes.worker(i).total_errors(), 1);
        assert_text(
            &pipes
                .accepted(i, transcription("abc", "go"), Direction::Downstream)
                .await,
            "go",
        );
        assert_text(
            &pipes
                .accepted(i, transcription("def", "go"), Direction::Downstream)
                .await,
            "go",
        );
        assert_eq!(pipes.worker(i).total_errors(), 1);
    }
);

pipeline_test!(
    work_rejection_preserves_pending_prefix_and_retries,
    |pipes| {
        let limits = WakeCheckLimits {
            max_match_work: 30,
            ..Default::default()
        };
        let config = WakeCheckFilterConfig {
            matcher_limits: limits,
            ..Default::default()
        };
        let i = pipes
            .start(Arc::new(WakeCheckFilter::new(config, ["hey bot"]).unwrap()))
            .await;
        pipes.filtered(i, transcription("u", "hey")).await;
        pipes
            .filtered(i, transcription("u", &" ".repeat(100)))
            .await;
        assert_eq!(pipes.worker(i).total_errors(), 1);
        assert_text(
            &pipes
                .accepted(i, transcription("u", "bot"), Direction::Downstream)
                .await,
            "heybot",
        );
    }
);

pipeline_test!(
    concurrent_setup_failure_and_cleanup_do_not_damage_first_owner,
    |pipes| {
        let gate = filter();
        let i = pipes.start(gate.clone()).await;
        pipes.filtered(i, transcription("u", "hey")).await;
        let failed = Pipeline::new(vec![gate.clone()])
            .start(Default::default())
            .await;
        assert!(failed.is_err(), "second live runtime was admitted");
        gate.cleanup().await.unwrap(); // Contextless cleanup cannot identify failed setup.
        assert_text(
            &pipes
                .accepted(i, transcription("u", "bot"), Direction::Downstream)
                .await,
            "heybot",
        );
        assert_eq!(pipes.finish(i, false).await.total_errors, 0);
        let next = pipes.start(gate).await;
        pipes
            .filtered(next, transcription("u", "no old awake window"))
            .await;
        assert_text(
            &pipes
                .accepted(next, transcription("u", " hey bot"), Direction::Downstream)
                .await,
            "hey bot",
        );
    }
);

pipeline_test!(end_stop_cancel_and_owner_drop_allow_fresh_reuse, |pipes| {
    for mode in 0..4 {
        let gate = filter();
        let i = pipes.start(gate.clone()).await;
        pipes.filtered(i, transcription("u", "hey")).await;
        match mode {
            0 => {
                assert_eq!(pipes.finish(i, false).await.state, WorkerState::Ended);
            }
            1 => {
                assert_eq!(pipes.finish(i, true).await.state, WorkerState::Stopped);
            }
            2 => {
                pipes.worker(i).cancel("cancel reuse").await.unwrap();
                assert_eq!(pipes.join(i).await.state, WorkerState::Cancelled);
            }
            _ => {
                let sender = pipes.worker(i).sender();
                drop(pipes.0[i].take());
                tokio::time::timeout(Duration::from_secs(5), async {
                    while !sender.state().is_finished() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            }
        }
        let next = pipes.start(gate).await;
        pipes
            .filtered(next, transcription("u", "bot must not combine. "))
            .await;
        assert_text(
            &pipes
                .accepted(
                    next,
                    transcription("u", "heybot new run"),
                    Direction::Downstream,
                )
                .await,
            "heybot new run",
        );
        assert_eq!(pipes.finish(next, false).await.total_errors, 0);
    }
});

struct HoldStop {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for HoldStop {
    fn name(&self) -> &str {
        "HoldStop"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Stop) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}
pipeline_test!(
    stop_processing_does_not_release_owner_before_runtime_finishes,
    |pipes| {
        let gate = filter();
        let entered = Arc::new(Semaphore::new(0));
        let release = Arc::new(Semaphore::new(0));
        let i = pipes
            .nodes(
                vec![
                    gate.clone(),
                    Arc::new(HoldStop {
                        entered: entered.clone(),
                        release: release.clone(),
                    }),
                ],
                Default::default(),
            )
            .await;
        pipes
            .send(i, Frame::new(FrameData::Stop), Direction::Downstream)
            .await;
        entered.acquire().await.unwrap().forget();
        assert!(Pipeline::new(vec![gate.clone()])
            .start(Default::default())
            .await
            .is_err());
        release.add_permits(1);
        assert_eq!(pipes.join(i).await.state, WorkerState::Stopped);
        let next = pipes.start(gate).await;
        pipes
            .filtered(next, transcription("u", "idle new run"))
            .await;
    }
);

// Core output overflow terminates the runtime even when a wrapper catches the
// push error. Before returning to core cleanup, probe the filter directly with
// the same context: a committed awake state attempts another push (and errors),
// whereas rolled-back idle state would suppress this text and return Ok.
struct CapturePushFailure {
    gate: Arc<WakeCheckFilter>,
    results: Arc<Mutex<Vec<Result<(), ProcessorError>>>>,
}
#[async_trait]
impl FrameProcessor for CapturePushFailure {
    fn name(&self) -> &str {
        "CapturePushFailure"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.gate.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let selected = transcription_text(&frame).is_some();
        let result = self.gate.process(frame, direction, ctx).await;
        if selected {
            let failed = result.is_err();
            self.results.lock().unwrap().push(result);
            if failed {
                let probe = self
                    .gate
                    .process(
                        transcription("u", "already awake"),
                        Direction::Downstream,
                        ctx,
                    )
                    .await;
                self.results.lock().unwrap().push(probe);
            }
            Ok(())
        } else {
            result
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.gate.cleanup().await
    }
}
pipeline_test!(
    full_output_reports_failure_but_does_not_rollback_committed_awake_state,
    |pipes| {
        let results = Arc::new(Mutex::new(Vec::new()));
        let gate = filter();
        let wrapper = Arc::new(CapturePushFailure {
            gate: gate.clone(),
            results: results.clone(),
        });
        let i = pipes
            .nodes(
                vec![wrapper],
                PipelineConfig {
                    output_capacity: 1,
                    ..Default::default()
                },
            )
            .await;
        // Start was drained. Fill the only output slot and wait until it is occupied.
        let fill = Frame::new(FrameData::Data);
        let fill_id = fill.id;
        pipes.send(i, fill, Direction::Downstream).await;
        pipes
            .send(i, transcription("u", "hey bot"), Direction::Downstream)
            .await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while results.lock().unwrap().len() < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(results.lock().unwrap()[0].is_err());
        assert!(
            results.lock().unwrap()[1].is_err(),
            "committed awake state must try forwarding the direct probe"
        );
        assert_eq!(pipes.recv(i).await.frame.id, fill_id);
        assert!(pipes.worker(i).recv().await.is_none());
        assert!(matches!(
            pipes.worker(i).join().await,
            Err(PipelineError::Backpressure(_))
        ));
        // The failed runtime is finished, so normal cleanup allows fresh reuse.
        let next = pipes.start(gate).await;
        pipes
            .filtered(next, transcription("u", "idle new run"))
            .await;
    }
);
