//! Independent regressions for composable stop-policy lifecycle and admission.
use pipecat_core::*;
use pipecat_services::{Content, Context, Message, Role};
use pipecat_turns::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn config(p: Vec<UserTurnStopStrategy>) -> UserAggregatorConfig {
    UserAggregatorConfig {
        turn: UserTurnConfig {
            enable_interruptions: false,
            stop_timeout: ms(5000),
            stop_strategies: Some(UserTurnStopStrategies::new(p).unwrap()),
            ..Default::default()
        },
        ..Default::default()
    }
}
fn deferred() -> Vec<UserTurnStopStrategy> {
    vec![
        UserTurnStopStrategy::speech_timeout(ms(100), true).deferred(),
        UserTurnStopStrategy::external_completion(),
    ]
}
fn transcript(text: &str) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    })
}
fn at(user: &LLMUserAggregator, data: FrameData, n: u64) -> AggregationOutput {
    user.process_at(Frame::new(data), Direction::Downstream, ms(n))
        .unwrap()
}
fn requests(out: &AggregationOutput) -> Vec<UserTurnInferenceRequest> {
    out.events
        .iter()
        .filter_map(|event| match event {
            AggregationEvent::UserTurnInferenceRequested(request) => Some(request.clone()),
            _ => None,
        })
        .collect()
}
fn contexts(out: &AggregationOutput) -> usize {
    out.actions.iter().filter(|action| matches!(action, AggregationAction::Push(frame) if matches!(frame.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)))).count()
}
fn vad(start: bool) -> FrameData {
    let data = VadSpeakingFrame {
        timestamp: 0.0,
        duration_secs: 0.2,
    };
    if start {
        FrameData::VadUserStartedSpeaking(data)
    } else {
        FrameData::VadUserStoppedSpeaking(data)
    }
}

#[test]
fn repeated_deferred_detectors_share_one_request_until_new_final_evidence() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        config(vec![
            UserTurnStopStrategy::speech_timeout(ms(100), true).deferred(),
            UserTurnStopStrategy::speech_timeout(ms(200), true).deferred(),
            UserTurnStopStrategy::external_completion(),
        ]),
    );
    at(&user, transcript("one"), 0);
    let first = requests(&user.advance(ms(100)).unwrap()).remove(0);
    assert_eq!(first.strategy_index, 0);
    assert!(requests(&user.advance(ms(200)).unwrap()).is_empty());
    assert_eq!(
        user.pending_inference_request().unwrap(),
        Some(first.request_id)
    );
    at(&user, transcript("two"), 250);
    let second = requests(&user.advance(ms(350)).unwrap()).remove(0);
    assert_ne!(second.request_id, first.request_id);
    assert_eq!(second.text, "one two");
    assert_eq!(
        contexts(
            &user
                .complete_inference_at(first.request_id, ms(351))
                .unwrap()
        ),
        0
    );
    assert_eq!(
        contexts(
            &user
                .complete_inference_at(second.request_id, ms(352))
                .unwrap()
        ),
        1
    );
    assert!(requests(&user.advance(ms(450)).unwrap()).is_empty());
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
}

#[test]
fn invalid_completion_and_last_invalid_tool_declaration_preserve_live_request() {
    let user = LLMUserAggregator::new(SharedContext::default(), config(deferred()));
    at(&user, transcript("accepted"), 0);
    let request = requests(&user.advance(ms(100)).unwrap()).remove(0);
    let before = user.deadline().unwrap();
    let mut invalid = request.completion_frame();
    invalid
        .metadata
        .insert("user_turn_inference_id".into(), json!("invalid"));
    assert!(user
        .process_at(invalid, Direction::Downstream, ms(100_000))
        .is_err());
    let bad_calls = FrameData::FunctionCallsStarted(vec![
        FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "valid".into(),
            arguments: json!({}),
        },
        FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "invalid".into(),
            arguments: json!(null),
        },
    ]);
    assert!(user
        .process_at(Frame::new(bad_calls), Direction::Downstream, ms(100_000))
        .is_err());
    assert_eq!(
        user.pending_inference_request().unwrap(),
        Some(request.request_id)
    );
    assert_eq!(user.deadline().unwrap(), before);
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    assert_eq!(
        contexts(
            &user
                .complete_inference_at(request.request_id, ms(110))
                .unwrap()
        ),
        1
    );
}

#[test]
fn duplicate_final_frame_and_transcript_id_do_not_invalidate_pending_request() {
    let user = LLMUserAggregator::new(SharedContext::default(), config(deferred()));
    let mut original = Frame::new(transcript("accepted"));
    original
        .metadata
        .insert("transcript_id".into(), json!("stable"));
    user.process_at(original.clone(), Direction::Downstream, ms(0))
        .unwrap();
    let request = requests(&user.advance(ms(100)).unwrap()).remove(0);
    user.process_at(original, Direction::Downstream, ms(110))
        .unwrap();
    let mut duplicate = Frame::new(transcript("accepted"));
    duplicate
        .metadata
        .insert("transcript_id".into(), json!("stable"));
    user.process_at(duplicate, Direction::Downstream, ms(120))
        .unwrap();
    assert_eq!(
        user.pending_inference_request().unwrap(),
        Some(request.request_id)
    );
    assert_eq!(
        contexts(
            &user
                .complete_inference_at(request.request_id, ms(130))
                .unwrap()
        ),
        1
    );
    assert_eq!(
        user.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("accepted".into()))
    );
}

#[test]
fn deferred_stop_proposal_is_consumed_before_text_and_finalizes_only_once() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        config(vec![
            UserTurnStopStrategy::external(ms(100), true).deferred(),
            UserTurnStopStrategy::external_completion(),
        ]),
    );
    at(&user, FrameData::UserStartedSpeaking, 0);
    let proposal = at(&user, FrameData::ProposedUserStoppedSpeaking, 10);
    assert!(proposal.actions.is_empty());
    assert!(proposal.events.is_empty());
    at(&user, transcript("late"), 20);
    let request = requests(&user.advance(ms(120)).unwrap()).remove(0);
    assert_eq!(request.text, "late");
    assert_eq!(
        contexts(
            &user
                .complete_inference_at(request.request_id, ms(130))
                .unwrap()
        ),
        1
    );
    assert_eq!(
        contexts(&at(&user, FrameData::UserTurnInferenceCompleted, 140)),
        0
    );
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
}

#[test]
fn summary_applied_while_deferred_retains_one_complete_accepted_turn() {
    let pair = LLMContextAggregatorPair::new(
        Context::new(vec![
            Message::text(Role::User, "old question"),
            Message::text(Role::Assistant, "old answer"),
            Message::text(Role::User, "recent question"),
        ]),
        config(deferred()),
        AssistantAggregatorConfig::default(),
    );
    let assistant = pair.assistant();
    let summary = assistant
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
    at(&user, transcript("new final"), 0);
    let request = requests(&user.advance(ms(100)).unwrap()).remove(0);
    let range =
        get_messages_to_summarize(&summary.context.messages, summary.min_messages_to_keep).unwrap();
    assert!(assistant
        .summarizer()
        .apply(&LlmContextSummaryResultFrame {
            request_id: summary.request_id,
            summary: "earlier exchange".into(),
            last_summarized_index: range.end - 1,
            error: None,
        })
        .unwrap()
        .is_some());
    assert_eq!(
        contexts(
            &user
                .complete_inference_at(request.request_id, ms(110))
                .unwrap()
        ),
        1
    );
    assert_eq!(
        contexts(
            &user
                .complete_inference_at(request.request_id, ms(120))
                .unwrap()
        ),
        0
    );
    let history = user.context().snapshot().unwrap();
    let finals: Vec<_> = history
        .messages
        .iter()
        .filter(|m| m.content == Some(Content::Text("new final".into())))
        .collect();
    assert_eq!(finals.len(), 1);
    assert_eq!(
        history.messages.last().unwrap().content,
        Some(Content::Text("new final".into()))
    );
}

#[test]
fn stale_completion_cannot_close_the_next_semantic_turn() {
    let user = LLMUserAggregator::new(SharedContext::default(), config(deferred()));
    at(&user, transcript("old"), 0);
    let first = requests(&user.advance(ms(100)).unwrap()).remove(0);
    user.complete_inference_at(first.request_id, ms(110))
        .unwrap();
    at(&user, transcript("new"), 120);
    let second = requests(&user.advance(ms(220)).unwrap()).remove(0);
    assert!(user
        .process_at(first.completion_frame(), Direction::Downstream, ms(230))
        .unwrap()
        .actions
        .is_empty());
    assert_eq!(
        user.pending_inference_request().unwrap(),
        Some(second.request_id)
    );
    assert_eq!(
        contexts(
            &user
                .complete_inference_at(second.request_id, ms(240))
                .unwrap()
        ),
        1
    );
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 2);
}

struct Incomplete;
impl pipecat_vad::SmartTurnPredictor for Incomplete {
    fn predict(
        &mut self,
        _: &[i16],
        _: u32,
        _: &pipecat_vad::SmartTurnCancellation,
    ) -> pipecat_vad::Result<pipecat_vad::SmartTurnPrediction> {
        Ok(pipecat_vad::SmartTurnPrediction {
            complete: false,
            probability: 0.0,
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
async fn receive(worker: &mut PipelineWorker, predicate: impl Fn(&FrameData) -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let frame = worker.recv().await.unwrap();
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("{error:?}");
            }
            if predicate(&frame.frame.data) {
                break;
            }
        }
    })
    .await
    .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("review".into()),
        }),
    )
    .await;
    receive(worker, |data| matches!(data, FrameData::Flush(_))).await;
}

#[tokio::test]
async fn deferred_adopted_external_stop_retains_smart_turn_audio_until_semantic_completion() {
    use pipecat_vad::{MonotonicSmartTurnClock, SmartTurnAnalyzer, SmartTurnParams};
    let analyzer = SmartTurnAnalyzer::with_predictor(
        Incomplete,
        16_000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap();
    let user = Arc::new(SmartTurnUserAggregator::new(
        SharedContext::default(),
        config(vec![
            UserTurnStopStrategy::external(ms(100), true).deferred(),
            UserTurnStopStrategy::turn_analyzer(true).deferred(),
            UserTurnStopStrategy::external_completion(),
        ]),
        analyzer,
    ));
    let mut worker = Pipeline::new(vec![user.clone()])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, FrameData::is_start).await;
    send(&worker, vad(true)).await;
    send(
        &worker,
        FrameData::InputAudio(
            AudioRawFrame::new([7i16.to_le_bytes(); 320].concat(), 16_000, 1).unwrap(),
        ),
    )
    .await;
    send(&worker, transcript("accepted")).await;
    barrier(&mut worker).await;
    assert_eq!(user.buffered_samples().await, 320);
    send(&worker, FrameData::UserStoppedSpeaking).await;
    barrier(&mut worker).await;
    let buffered = user.buffered_samples().await;
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    let request = user.user().pending_inference_request().unwrap().unwrap();
    let mut completion = Frame::new(FrameData::UserTurnInferenceCompleted);
    completion
        .metadata
        .insert("user_turn_inference_id".into(), request.into());
    worker
        .sender()
        .send(completion, Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    assert_eq!(user.buffered_samples().await, 0);
    worker.end().await.unwrap();
    worker.join().await.unwrap();
    assert_eq!(
        buffered, 320,
        "a held physical stop must not clear an open semantic turn's acoustic evidence"
    );
}

#[test]
fn an_external_start_latches_until_its_matching_stop_despite_vad_silence() {
    for proposed in [false, true] {
        let mut settings = config(vec![
            UserTurnStopStrategy::external(ms(100), true).deferred(),
            UserTurnStopStrategy::external_completion(),
        ]);
        settings.turn.resolve_proposals = true;
        let user = LLMUserAggregator::new(SharedContext::default(), settings);
        at(
            &user,
            if proposed {
                FrameData::ProposedUserStartedSpeaking
            } else {
                FrameData::UserStartedSpeaking
            },
            0,
        );
        at(&user, transcript("accepted"), 10);
        at(&user, vad(false), 20);
        assert!(
            requests(&user.advance(ms(110)).unwrap()).is_empty(),
            "external start is still open, proposed={proposed}"
        );
        assert!(user.context().snapshot().unwrap().messages.is_empty());
        let stop = at(
            &user,
            if proposed {
                FrameData::ProposedUserStoppedSpeaking
            } else {
                FrameData::UserStoppedSpeaking
            },
            120,
        );
        let request = requests(&stop).remove(0);
        assert_eq!(request.text, "accepted");
        assert_eq!(
            contexts(
                &user
                    .complete_inference_at(request.request_id, ms(130))
                    .unwrap()
            ),
            1
        );
    }
}

#[test]
fn rejected_direct_completion_cannot_advance_the_monotonic_clock() {
    let user = LLMUserAggregator::new(SharedContext::default(), config(deferred()));
    at(&user, transcript("one"), 0);
    let first = requests(&user.advance(ms(100)).unwrap()).remove(0);
    at(&user, transcript("two"), 110);
    assert!(user
        .complete_inference_at(first.request_id, ms(100_000))
        .unwrap()
        .events
        .is_empty());
    at(&user, transcript("three"), 120);
    let next = requests(&user.advance(ms(220)).unwrap());
    assert_eq!(
        next.len(),
        1,
        "a stale verdict must not move future deadlines"
    );
    assert_eq!(next[0].text, "one two three");
}
