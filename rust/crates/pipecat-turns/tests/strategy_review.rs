//! Independent start/mute integration regressions.
use pipecat_core::*;
use pipecat_services::{Content, Role};
use pipecat_turns::*;
use serde_json::json;
use std::time::Duration;

fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}
fn config(
    starts: Vec<UserTurnStartStrategy>,
    mutes: Vec<UserMuteStrategy>,
) -> UserAggregatorConfig {
    UserAggregatorConfig {
        turn: UserTurnConfig {
            start_strategies: Some(UserTurnStartStrategies::new(starts).unwrap()),
            speech_timeout: ms(10),
            enable_interruptions: false,
            ..Default::default()
        },
        mute_strategies: mutes,
        user_idle_timeout: ms(1000),
        ..Default::default()
    }
}
fn transcript(text: &str) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    })
}
fn vad(start: bool) -> FrameData {
    let timing = VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    };
    if start {
        FrameData::VadUserStartedSpeaking(timing)
    } else {
        FrameData::VadUserStoppedSpeaking(timing)
    }
}
fn process(user: &LLMUserAggregator, frame: FrameData, at: u64) -> AggregationOutput {
    user.process_at(Frame::new(frame), Direction::Downstream, ms(at))
        .unwrap()
}
fn call(id: &str) -> FunctionCall {
    FunctionCall {
        function_name: "lookup".into(),
        tool_call_id: id.into(),
        arguments: json!({"id": id}),
    }
}
fn user_texts(user: &LLMUserAggregator) -> Vec<String> {
    user.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter_map(|message| match (message.role, message.content) {
            (Role::User, Some(Content::Text(text))) => Some(text),
            _ => None,
        })
        .collect()
}
fn context_count(output: &AggregationOutput) -> usize {
    output.actions.iter().filter(|action| matches!(action, AggregationAction::Push(frame) if matches!(frame.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)))).count()
}

#[test]
fn invalid_policy_tool_batch_does_not_advance_the_owner_clock() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        config(
            vec![UserTurnStartStrategy::vad()],
            vec![UserMuteStrategy::FunctionCall],
        ),
    );
    let mut invalid = call("bad");
    invalid.arguments = json!(null);
    assert!(user
        .process_at(
            Frame::new(FrameData::FunctionCallsStarted(vec![call("good"), invalid])),
            Direction::Downstream,
            ms(100_000)
        )
        .is_err());
    assert!(!user.is_muted().unwrap());
    assert!(!user.function_calls_in_progress().unwrap());
    process(&user, FrameData::BotStoppedSpeaking, 1000);
    assert_eq!(user.idle_deadline().unwrap(), Some(ms(2000)));
}

#[test]
fn rejected_metadata_cannot_silently_latch_an_opening_mute() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        config(
            vec![UserTurnStartStrategy::vad()],
            vec![UserMuteStrategy::UntilFirstBotComplete],
        ),
    );
    let invalid = FrameData::SttMetadata(SttMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "test".into(),
            user_turn_strategies: None,
        },
        ttfs_p99_latency: f64::NAN,
    });
    assert!(user
        .process_at(Frame::new(invalid), Direction::Downstream, ms(100_000))
        .is_err());
    assert!(
        !user.is_muted().unwrap(),
        "a rejected frame must not consume the first mute transition without delivering it"
    );
    let output = process(&user, FrameData::BotStartedSpeaking, 1000);
    assert_eq!(
        output.events,
        [AggregationEvent::UserMuteChanged { muted: true }]
    );
}

#[test]
fn transcript_start_after_vad_and_model_completion_uses_the_transcript_fallback() {
    let mut turn = config(vec![UserTurnStartStrategy::min_words(2, false)], vec![]).turn;
    turn.require_model_completion = true;
    turn.stt_timeout = ms(500);
    turn.stop_timeout = ms(30_000);
    let mut controller = UserTurnController::new(turn);
    assert!(controller.process(&vad(true), ms(0)).is_empty());
    assert!(controller.process(&vad(false), ms(100)).is_empty());
    assert!(controller.mark_model_complete(ms(200)).is_empty());
    let started = controller.process(&transcript("hello there"), ms(300));
    assert!(started
        .iter()
        .any(|event| matches!(event, UserTurnEvent::Started { .. })));
    let stopped = controller.advance(ms(1000));
    assert!(stopped.iter().any(|event| matches!(event, UserTurnEvent::Stopped { .. })), "a transcript-driven turn after prior VAD/model completion must not wait for the watchdog: {stopped:?}");
}

#[test]
fn mute_discards_partial_preview_and_preserves_only_accepted_finals() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        config(
            vec![UserTurnStartStrategy::vad()],
            vec![UserMuteStrategy::Always],
        ),
    );
    process(&user, vad(true), 0);
    process(&user, transcript("accepted"), 1);
    process(
        &user,
        FrameData::InterimTranscription(TranscriptionFrame {
            text: "stale preview".into(),
            ..Default::default()
        }),
        2,
    );
    let muted = process(&user, FrameData::BotStartedSpeaking, 3);
    assert_eq!(user_texts(&user), ["accepted"]);
    assert_eq!(context_count(&muted), 0);
    assert_eq!(user.interim_text().unwrap(), "");
    process(&user, transcript("suppressed"), 4);
    process(&user, vad(false), 5);
    process(&user, FrameData::BotStoppedSpeaking, 6);
    process(&user, vad(true), 7);
    process(&user, transcript("fresh"), 8);
    process(&user, vad(false), 9);
    assert_eq!(context_count(&user.advance(ms(19)).unwrap()), 1);
    assert_eq!(user_texts(&user), ["accepted", "fresh"]);
}

#[test]
fn typed_tool_results_require_full_identity_and_finality_in_the_aggregator() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        config(
            vec![UserTurnStartStrategy::external()],
            vec![UserMuteStrategy::FunctionCall],
        ),
    );
    process(
        &user,
        FrameData::FunctionCallsStartedDetailed(vec![FunctionCallFromLlm {
            function_name: "lookup".into(),
            tool_call_id: "a".into(),
            arguments: [("id".into(), json!("a"))].into(),
            context: None,
        }]),
        0,
    );
    for (name, arguments, is_final) in [
        ("lookup", json!({"id":"a"}), false),
        ("wrong", json!({"id":"a"}), true),
        ("lookup", json!({"id":"wrong"}), true),
    ] {
        let out = process(
            &user,
            FrameData::FunctionCallResultDetailed(DetailedFunctionCallResultFrame {
                function_name: name.into(),
                tool_call_id: "a".into(),
                arguments,
                result: json!("ok"),
                run_llm: Some(true),
                error: None,
                properties: Some(FunctionCallResultProperties {
                    is_final,
                    ..Default::default()
                }),
            }),
            1,
        );
        assert!(!out
            .events
            .iter()
            .any(|event| matches!(event, AggregationEvent::UserMuteChanged { muted: false })));
        assert!(user.is_muted().unwrap());
        assert!(user.function_calls_in_progress().unwrap());
    }
    process(
        &user,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "lookup".into(),
            tool_call_id: "a".into(),
            run_llm: true,
        }),
        2,
    );
    assert!(!user.is_muted().unwrap());
    assert!(!user.function_calls_in_progress().unwrap());
    process(&user, FrameData::FunctionCallsStarted(vec![call("a")]), 3);
    assert!(!user.is_muted().unwrap());
    process(&user, FrameData::BotStoppedSpeaking, 4);
    assert_eq!(user.idle_deadline().unwrap(), Some(ms(1004)));
}

#[test]
fn terminal_boundaries_clear_wake_deadlines_and_never_commit_interim() {
    for terminal in [
        FrameData::EndDetailed(ReasonFrame::default()),
        FrameData::Stop,
        FrameData::CancelDetailed(ReasonFrame::default()),
    ] {
        let cancelling = matches!(terminal, FrameData::CancelDetailed(_));
        let wake =
            UserTurnStartStrategy::wake_phrase(vec!["hey cat".into()], ms(100), true).unwrap();
        let user = LLMUserAggregator::new(
            SharedContext::default(),
            config(
                vec![wake, UserTurnStartStrategy::transcription(true)],
                vec![],
            ),
        );
        process(&user, transcript("hey cat"), 0);
        process(
            &user,
            FrameData::InterimTranscription(TranscriptionFrame {
                text: "unfinished".into(),
                ..Default::default()
            }),
            1,
        );
        let out = process(&user, terminal, 2);
        assert_eq!(context_count(&out), usize::from(!cancelling));
        assert_eq!(user_texts(&user), ["hey cat"]);
        assert_eq!(user.deadline().unwrap(), None);
        assert!(user.advance(ms(1000)).unwrap().events.is_empty());
    }
}

struct CompletePredictor;
impl pipecat_vad::SmartTurnPredictor for CompletePredictor {
    fn predict(
        &mut self,
        _: &[i16],
        _: u32,
        _: &pipecat_vad::SmartTurnCancellation,
    ) -> pipecat_vad::Result<pipecat_vad::SmartTurnPrediction> {
        Ok(pipecat_vad::SmartTurnPrediction {
            probability: 1.0,
            complete: true,
        })
    }
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn receive(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let output = worker.recv().await.expect("pipeline output");
            assert!(
                !matches!(output.frame.data, FrameData::Error(_)),
                "{:?}",
                output.frame.data
            );
            if wanted(&output.frame.data) {
                return output;
            }
        }
    })
    .await
    .expect("frame deadline")
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("independent-strategy-review".into()),
        }),
    )
    .await;
    receive(worker, |data| matches!(data, FrameData::Flush(_))).await;
}

#[tokio::test]
async fn smart_turn_late_transcript_start_finishes_after_its_prior_model_verdict() {
    use pipecat_vad::{MonotonicSmartTurnClock, SmartTurnAnalyzer, SmartTurnParams};
    use std::sync::Arc;
    let mut config = config(vec![UserTurnStartStrategy::min_words(2, false)], vec![]);
    config.turn.stt_timeout = Duration::ZERO;
    config.turn.stop_timeout = Duration::from_secs(30);
    let analyzer = SmartTurnAnalyzer::with_predictor(
        CompletePredictor,
        16_000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap();
    let user = Arc::new(SmartTurnUserAggregator::new(
        SharedContext::default(),
        config,
        analyzer,
    ));
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig {
            max_background_tasks: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, FrameData::is_start).await;
    send(&worker, vad(true)).await;
    send(
        &worker,
        FrameData::InputAudio(
            AudioRawFrame::new([1_i16.to_le_bytes(); 320].concat(), 16_000, 1).unwrap(),
        ),
    )
    .await;
    barrier(&mut worker).await;
    send(&worker, vad(false)).await;
    receive(&mut worker, |data| matches!(data, FrameData::Metrics(_))).await;
    barrier(&mut worker).await;
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(&worker, transcript("late but final")).await;
    let completed = tokio::time::timeout(
        Duration::from_secs(1),
        receive(&mut worker, |data| {
            matches!(
                data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        }),
    )
    .await
    .is_ok();
    worker.end().await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), worker.join())
            .await
            .unwrap()
            .unwrap()
            .state,
        WorkerState::Ended
    );
    assert!(completed, "the completed VAD/model utterance must not strand a later transcript-driven turn until the 30-second watchdog");
    assert_eq!(user_texts(&user.user()), ["late but final"]);
}

#[test]
fn minimum_words_counts_python_unicode_whitespace_separators() {
    for separator in [
        '\u{1c}', '\u{1d}', '\u{1e}', '\u{1f}', '\u{2003}', '\u{3000}',
    ] {
        let user = LLMUserAggregator::new(
            SharedContext::default(),
            config(vec![UserTurnStartStrategy::min_words(2, false)], vec![]),
        );
        process(&user, FrameData::BotStartedSpeaking, 0);
        let output = process(&user, transcript(&format!("hello{separator}there")), 1);
        assert!(
            output.events.contains(&AggregationEvent::UserTurnStarted),
            "separator U+{:04X} must count as source str.split whitespace",
            separator as u32
        );
    }
}

#[test]
fn a_summary_requested_before_policy_mute_preserves_its_accepted_user_tail() {
    use pipecat_services::{Context, Message};
    let pair = LLMContextAggregatorPair::new(
        Context::new(vec![
            Message::text(Role::User, "old question"),
            Message::text(Role::Assistant, "old answer"),
            Message::text(Role::User, "recent question"),
        ]),
        config(
            vec![UserTurnStartStrategy::vad()],
            vec![UserMuteStrategy::Always],
        ),
        AssistantAggregatorConfig::default(),
    );
    let assistant = pair.assistant();
    let request = assistant
        .summarizer()
        .request(
            Some(&LlmContextSummaryConfig {
                min_messages_after_summary: 1,
                ..Default::default()
            }),
            true,
        )
        .unwrap()
        .unwrap();
    let user = pair.user();
    process(&user, vad(true), 0);
    process(&user, transcript("new accepted final"), 1);
    let muted = process(&user, FrameData::BotStartedSpeaking, 2);
    assert_eq!(context_count(&muted), 0);
    let range =
        get_messages_to_summarize(&request.context.messages, request.min_messages_to_keep).unwrap();
    assert!(assistant
        .summarizer()
        .apply(&LlmContextSummaryResultFrame {
            request_id: request.request_id,
            summary: "earlier exchange".into(),
            last_summarized_index: range.end - 1,
            error: None
        })
        .unwrap()
        .is_some());
    let texts = user_texts(&user);
    assert_eq!(
        &texts[texts.len() - 2..],
        ["recent question", "new accepted final"]
    );
    process(&user, transcript("muted rejection"), 3);
    assert_eq!(user_texts(&user), texts);
}

#[test]
fn rejected_tool_declaration_without_tool_mute_cannot_latch_an_opening_mute() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        config(
            vec![UserTurnStartStrategy::vad()],
            vec![UserMuteStrategy::UntilFirstBotComplete],
        ),
    );
    let mut invalid = call("invalid");
    invalid.arguments = json!(null);
    assert!(user
        .process_at(
            Frame::new(FrameData::FunctionCallsStarted(vec![invalid])),
            Direction::Downstream,
            ms(100_000)
        )
        .is_err());
    assert!(
        !user.is_muted().unwrap(),
        "idle validation must run before a mute transition is irreversibly accepted"
    );
    let output = process(&user, FrameData::BotStartedSpeaking, 1000);
    assert_eq!(
        output.events,
        [AggregationEvent::UserMuteChanged { muted: true }]
    );
}

#[derive(Default)]
struct BufferClock(std::sync::atomic::AtomicU64);
impl pipecat_vad::SmartTurnClock for BufferClock {
    fn now(&self) -> Duration {
        ms(self.0.load(std::sync::atomic::Ordering::SeqCst))
    }
}
struct SamplePredictor(tokio::sync::mpsc::UnboundedSender<Vec<i16>>);
impl pipecat_vad::SmartTurnPredictor for SamplePredictor {
    fn predict(
        &mut self,
        samples: &[i16],
        _: u32,
        _: &pipecat_vad::SmartTurnCancellation,
    ) -> pipecat_vad::Result<pipecat_vad::SmartTurnPrediction> {
        self.0.send(samples.to_vec()).unwrap();
        Ok(pipecat_vad::SmartTurnPrediction {
            probability: 1.0,
            complete: true,
        })
    }
}

#[tokio::test]
async fn manual_and_policy_mute_reset_smart_turn_speech_labels() {
    use pipecat_vad::{SmartTurnAnalyzer, SmartTurnParams};
    use std::sync::{atomic::Ordering, Arc};
    for manual in [false, true] {
        let clock = Arc::new(BufferClock::default());
        let (send_samples, mut samples) = tokio::sync::mpsc::unbounded_channel();
        let analyzer = SmartTurnAnalyzer::with_predictor(
            SamplePredictor(send_samples),
            16_000,
            SmartTurnParams {
                pre_speech_ms: 0.0,
                ..Default::default()
            },
            clock.clone(),
            None,
        )
        .unwrap();
        let mut settings = config(
            vec![UserTurnStartStrategy::vad()],
            vec![UserMuteStrategy::Always],
        );
        settings.user_idle_timeout = Duration::ZERO;
        let user = Arc::new(SmartTurnUserAggregator::new(
            SharedContext::default(),
            settings,
            analyzer,
        ));
        let mut worker = Pipeline::new(vec![user])
            .start(PipelineConfig {
                max_background_tasks: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        receive(&mut worker, FrameData::is_start).await;
        let audio = |sample: i16| {
            FrameData::InputAudio(
                AudioRawFrame::new([sample.to_le_bytes(); 320].concat(), 16_000, 1).unwrap(),
            )
        };
        send(&worker, vad(true)).await;
        send(&worker, audio(1)).await;
        barrier(&mut worker).await;
        send(
            &worker,
            if manual {
                FrameData::SttMute(true)
            } else {
                FrameData::BotStartedSpeaking
            },
        )
        .await;
        barrier(&mut worker).await;
        send(&worker, vad(false)).await;
        send(
            &worker,
            if manual {
                FrameData::SttMute(false)
            } else {
                FrameData::BotStoppedSpeaking
            },
        )
        .await;
        barrier(&mut worker).await;
        clock.0.store(1000, Ordering::SeqCst);
        send(&worker, audio(2)).await;
        barrier(&mut worker).await;
        clock.0.store(2000, Ordering::SeqCst);
        send(&worker, vad(true)).await;
        send(&worker, audio(3)).await;
        barrier(&mut worker).await;
        send(&worker, vad(false)).await;
        let actual = tokio::time::timeout(Duration::from_secs(3), samples.recv())
            .await
            .unwrap()
            .unwrap();
        worker.end().await.unwrap();
        worker.join().await.unwrap();
        assert_eq!(
            actual.len(),
            320,
            "manual={manual}: post-unmute silence must not become the next turn's speech onset"
        );
        assert_eq!(actual, vec![3; 320]);
    }
}
