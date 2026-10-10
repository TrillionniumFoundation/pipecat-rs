//! Run with: cargo run -p pipecat-text --example token_alignment --offline
//! Entirely native; no provider, Python process, audio, or playback claims.
use pipecat_text::*;
fn show(label: &str, events: &[TokenSequencerEvent]) {
    println!("{label}");
    for event in events {
        match event {
            TokenSequencerEvent::Promoted {
                segment, sentence, ..
            } => println!(
                "  anchor {}: {:?}",
                segment.serial(),
                sentence.user_facing_text
            ),
            TokenSequencerEvent::Alignment(SequencerEvent::Word { completion, .. }) => println!(
                "  provider reconstruction: {:?}, source {:?}",
                completion.word, completion.source_span
            ),
            TokenSequencerEvent::Alignment(SequencerEvent::Unreported { completion, .. }) => {
                println!("  UNREPORTED reconstruction: {:?}", completion.source_text)
            }
            TokenSequencerEvent::Alignment(SequencerEvent::Skipped { text, .. }) => {
                println!("  ordered skipped text: {text:?}")
            }
            TokenSequencerEvent::Alignment(SequencerEvent::UnmatchedWord { word, .. }) => {
                println!("  unmatched provider diagnostic: {word:?}")
            }
            TokenSequencerEvent::Progress {
                accumulated_text,
                remaining_text,
                ..
            } => println!("  display progress: {accumulated_text:?} | {remaining_text:?}"),
        }
    }
}
fn main() -> Result<(), AlignmentError> {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            max_buffered_words: 1,
            ..Default::default()
        },
    )?;
    let c = s.open_context(true, TokenReporting::WordTimestamps, Some("en"))?;
    assert!(s
        .register_token(c, "Total is ", "Total is ", "Total is ")?
        .is_empty());
    assert!(s
        .register_token(c, "five dollars.", "$5.", "$5.")?
        .is_empty());
    assert!(s.process_word(c, "Total", 0, false)?.is_empty());
    assert_eq!(
        s.process_word(c, "is", 10, false),
        Err(AlignmentError::LimitExceeded("buffered words"))
    );
    println!("Early word queue is full; rejected 'is' without consuming it");
    let out = s.register_token(c, " Thanks!", " Thanks!", " Thanks!")?;
    show(
        "Lookahead promotes transformed sentence, then replays the accepted early word:",
        &out,
    );
    show(
        "Retry after promotion releases backpressure:",
        &s.process_word(c, "is", 10, false)?,
    );
    let held = s.process_word(c, "five", 20, false)?;
    assert!(!held
        .iter()
        .any(|e| matches!(e, TokenSequencerEvent::Progress { .. })));
    show(
        "Inside atomic $5 expansion: no speculative display progress:",
        &held,
    );
    show(
        "Expansion completes:",
        &s.process_word(c, "dollars", 30, false)?,
    );
    let (_, out) = s.register_skipped(c, "code sample", "<code>code sample</code>", None)?;
    show(
        "Skipped text first finalizes pending 'Thanks!', which keeps code blocked:",
        &out,
    );
    show(
        "Provider completion releases skipped text in order:",
        &s.process_word(c, "Thanks", 40, false)?,
    );
    s.interrupt()?;
    assert_eq!(
        s.process_word(c, "late", 50, false),
        Err(AlignmentError::StaleGeneration)
    );
    println!("Interruption rejects delayed words from the old generation");
    let c = s.open_context(true, TokenReporting::SynthesisCompletion, None)?;
    let out = s.register_token(
        c,
        "No timestamps. Still streaming",
        "No timestamps. Still streaming",
        "No timestamps. Still streaming",
    )?;
    let segment = out
        .iter()
        .find_map(|e| match e {
            TokenSequencerEvent::Promoted { segment, .. } => Some(*segment),
            _ => None,
        })
        .unwrap();
    show("No-timestamp promotion announces text only:", &out);
    show(
        "Explicit sentence synthesis completion:",
        &s.complete_synthesis_segment(c, segment, 0)?,
    );
    show(
        "Explicit context synthesis completion:",
        &s.complete_synthesis(c, 0)?,
    );
    assert_eq!(s.pending_text_bytes(), 0);
    println!("No event above is a Played receipt or an audible-history update");
    Ok(())
}
