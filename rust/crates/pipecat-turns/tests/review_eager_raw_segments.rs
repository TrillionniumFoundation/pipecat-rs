//! Pinned source: af9f9da9d7afddaa8811c0e0251b32566c980bdf,
//! ExternalUserTurnStopStrategy._handle_transcription appends frame.text verbatim.
//! These are source-mapped native assertions, not a Python execution oracle.
use pipecat_core::{
    Direction, Frame, FrameData, MessageContent, SpeculationStatus, TranscriptionFrame,
};
use pipecat_turns::{
    AggregationAction, AggregationOutput, EagerMatchPolicy, EagerUserTurnConfig, LLMUserAggregator,
    SharedContext, UserAggregatorConfig,
};
use std::time::Duration;

fn feed(user: &LLMUserAggregator, frame: Frame, millis: u64) -> AggregationOutput {
    user.process_at(frame, Direction::Downstream, Duration::from_millis(millis))
        .unwrap()
}

fn transcript(kind: fn(TranscriptionFrame) -> FrameData, text: &str) -> Frame {
    Frame::new(kind(TranscriptionFrame {
        text: text.into(),
        user_id: "review-user".into(),
        timestamp: "0".into(),
        language: None,
        result: None,
        finalized: true,
    }))
}

fn contexts(output: &AggregationOutput) -> Vec<&Frame> {
    output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame)
                if matches!(
                    frame.frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                ) =>
            {
                Some(&frame.frame)
            }
            _ => None,
        })
        .collect()
}

fn assert_whitespace_segment_contract(policy: EagerMatchPolicy, prediction: &str) {
    let confirms = policy.matches(prediction, "a b");
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(EagerUserTurnConfig::new(policy, Duration::from_secs(5), 1024).unwrap()),
            empty_user_turn: None,
            ..Default::default()
        },
    );
    feed(&user, Frame::new(FrameData::ProposedUserStartedSpeaking), 0);
    let provisional = feed(
        &user,
        transcript(FrameData::EagerTranscription, prediction),
        1,
    );
    let capability = contexts(&provisional)[0]
        .native_speculation()
        .unwrap()
        .clone();
    for (index, segment) in ["a", " ", "b"].into_iter().enumerate() {
        feed(
            &user,
            transcript(FrameData::Transcription, segment),
            2 + index as u64,
        );
    }
    let stopped = feed(&user, Frame::new(FrameData::ProposedUserStoppedSpeaking), 5);

    // Native committed history has always ignored a standalone whitespace
    // segment and inserted its normal inter-frame space between a and b.
    let history = user.context().snapshot_rich().unwrap();
    assert_eq!(history.messages.len(), 1);
    assert_eq!(
        history.messages[0].content,
        MessageContent::Text("a b".into())
    );
    assert_eq!(
        capability.status(),
        if confirms {
            SpeculationStatus::Confirmed
        } else {
            SpeculationStatus::Cancelled
        },
        "source comparison must preserve the standalone whitespace segment"
    );
    assert_eq!(contexts(&stopped).len(), usize::from(!confirms));
}

#[test]
fn review_exact_whitespace_segment_prevents_false_confirmation() {
    assert_whitespace_segment_contract(EagerMatchPolicy::Exact, "ab");
}

#[test]
fn review_normalized_whitespace_segment_prevents_false_confirmation() {
    assert_whitespace_segment_contract(EagerMatchPolicy::Normalized, "ab");
}

#[test]
fn review_exact_whitespace_segment_accepts_matching_prediction() {
    assert_whitespace_segment_contract(EagerMatchPolicy::Exact, "a b");
}

#[test]
fn review_normalized_whitespace_segment_accepts_matching_prediction() {
    assert_whitespace_segment_contract(EagerMatchPolicy::Normalized, "a b");
}

fn assert_empty_final_clears_external_interim(final_segment: &str) {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(
                EagerUserTurnConfig::new(EagerMatchPolicy::Exact, Duration::from_secs(5), 1024)
                    .unwrap(),
            ),
            empty_user_turn: None,
            ..Default::default()
        },
    );
    feed(&user, Frame::new(FrameData::ProposedUserStartedSpeaking), 0);
    let provisional = feed(
        &user,
        transcript(FrameData::EagerTranscription, &format!("a{final_segment}")),
        1,
    );
    let capability = contexts(&provisional)[0]
        .native_speculation()
        .unwrap()
        .clone();
    feed(&user, transcript(FrameData::Transcription, "a"), 2);
    feed(
        &user,
        transcript(FrameData::InterimTranscription, "decoding"),
        3,
    );
    feed(
        &user,
        transcript(FrameData::Transcription, final_segment),
        4,
    );
    let stopped = feed(&user, Frame::new(FrameData::ProposedUserStoppedSpeaking), 5);

    // The pinned external strategy clears its interim flag on every final,
    // including an empty one. A timer advance is deliberately not supplied.
    assert_eq!(
        capability.status(),
        SpeculationStatus::Confirmed,
        "a whitespace/empty final must clear interim state before external stop"
    );
    assert!(contexts(&stopped).is_empty());
    let history = user.context().snapshot_rich().unwrap();
    assert_eq!(history.messages.len(), 1);
    assert_eq!(
        history.messages[0].content,
        MessageContent::Text("a".into())
    );
}

#[test]
fn review_whitespace_final_clears_interim_before_external_stop() {
    assert_empty_final_clears_external_interim(" ");
}

#[test]
fn review_empty_final_clears_interim_before_external_stop() {
    assert_empty_final_clears_external_interim("");
}
