//! Opt-in terminal audio duration; legacy providers keep their old boundary.
use pipecat_core::FrameData;
use pipecat_text::*;

fn alignment(reporting: TokenReporting) -> TtsAlignment {
    let mut alignment =
        TtsAlignment::new(7, true, reporting, None, TtsAlignmentLimits::default()).unwrap();
    alignment
        .register(&TtsTextChannels::identical("one two"), true)
        .unwrap();
    alignment
}
fn word_pts(events: &[TtsAlignmentEvent]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|event| match event.event {
            TokenSequencerEvent::Alignment(SequencerEvent::Word { .. }) => Some(event.pts_ns),
            _ => None,
        })
        .collect()
}
#[test]
fn none_keeps_legacy_word_and_synthesis_completion_boundaries() {
    for reporting in [
        TokenReporting::WordTimestamps,
        TokenReporting::SynthesisCompletion,
    ] {
        for optional in [false, true] {
            let mut alignment = alignment(reporting);
            alignment.start_audio(100, 0).unwrap();
            if reporting == TokenReporting::WordTimestamps {
                alignment.word("one", 0.25, false).unwrap();
            }
            if optional {
                alignment
                    .finish_success_with_audio_offset(999_000_000, 800_000_000, None)
                    .unwrap();
            } else {
                alignment.finish_success(999_000_000, 800_000_000).unwrap();
            }
            assert_eq!(
                alignment.boundary_pts().unwrap(),
                if reporting == TokenReporting::WordTimestamps {
                    250_000_101
                } else {
                    101
                }
            );
        }
    }
}
#[test]
fn zero_word_timestamp_survives_and_trailing_silence_advances_only_boundary() {
    let mut alignment = alignment(TokenReporting::WordTimestamps);
    alignment.start_audio(0, 0).unwrap();
    assert_eq!(word_pts(&alignment.word("one", 0.0, false).unwrap()), [0]);
    assert_eq!(
        word_pts(&alignment.word("two", 0.25, false).unwrap()),
        [250_000_000]
    );
    let done = alignment
        .finish_success_with_audio_offset(10, 0, Some(1.5))
        .unwrap();
    assert!(word_pts(&done).is_empty());
    assert_eq!(alignment.boundary_pts().unwrap(), 1_500_000_001);
}
#[test]
fn terminal_offset_never_regresses_last_word() {
    let mut alignment = alignment(TokenReporting::WordTimestamps);
    alignment.start_audio(42, 0).unwrap();
    alignment.word("one", 1.0, false).unwrap();
    alignment
        .finish_success_with_audio_offset(0, 0, Some(0.25))
        .unwrap();
    assert_eq!(alignment.boundary_pts().unwrap(), 1_000_000_043);
}
#[test]
fn no_audio_discards_words_ignores_duration_and_never_creates_playback_evidence() {
    let mut alignment = alignment(TokenReporting::WordTimestamps);
    let mut mapper =
        TtsAlignmentFrames::new("silent".into(), alignment.context(), true, 4).unwrap();
    alignment.word("one", 0.0, false).unwrap();
    let events = alignment
        .finish_success_with_audio_offset(10, 20, Some(5.0))
        .unwrap();
    assert!(word_pts(&events).is_empty());
    assert_eq!(alignment.boundary_pts().unwrap(), 21);
    let frames = mapper.convert(events).unwrap();
    assert!(frames
        .iter()
        .any(|f| matches!(f.data, FrameData::TtsAlignmentDiagnostic(_))));
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsText(_) | FrameData::TtsAudio(_) | FrameData::BotStartedSpeaking
    )));
}
#[test]
fn invalid_duration_and_checked_terminal_addition_poison_context() {
    for duration in [-1.0, f64::NAN, f64::INFINITY, u64::MAX as f64 / 1e9] {
        for audio in [false, true] {
            let mut alignment = alignment(TokenReporting::WordTimestamps);
            if audio {
                alignment.start_audio(0, 0).unwrap();
            }
            assert_eq!(
                alignment.finish_success_with_audio_offset(0, 0, Some(duration)),
                Err(TtsAlignmentError::InvalidTimestamp)
            );
            assert!(alignment.is_closed());
        }
    }
    for baseline in [u64::MAX - 1_000_000_000, u64::MAX - 999_999_999] {
        let mut alignment = alignment(TokenReporting::WordTimestamps);
        alignment.start_audio(baseline, 0).unwrap();
        assert_eq!(
            alignment.finish_success_with_audio_offset(0, 0, Some(1.0)),
            Err(TtsAlignmentError::TimestampOverflow)
        );
        assert!(alignment.is_closed());
    }
}
#[test]
fn successive_utterances_add_request_relative_offset_once() {
    let mut first = alignment(TokenReporting::WordTimestamps);
    first.start_audio(100, 0).unwrap();
    first.word("one", 0.0, false).unwrap();
    first
        .finish_success_with_audio_offset(0, 0, Some(2.0))
        .unwrap();
    let boundary = first.boundary_pts().unwrap();
    let mut second = alignment(TokenReporting::WordTimestamps);
    second.word("one", 0.0, false).unwrap();
    assert_eq!(
        word_pts(&second.start_audio(200, boundary).unwrap()),
        [boundary]
    );
    assert_eq!(
        word_pts(&second.word("two", 0.25, false).unwrap()),
        [boundary + 250_000_000]
    );
    second
        .finish_success_with_audio_offset(0, boundary, Some(0.5))
        .unwrap();
    assert_eq!(second.boundary_pts().unwrap(), boundary + 500_000_001);
    assert_ne!(first.context(), second.context());
}
