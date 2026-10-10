//! Run with `cargo run -p pipecat-text --example align_speech --offline`.
//! Simulated provider reports; no synthesis, hardware, or playout claims.
use pipecat_text::{AggregatedFrameSequencer, AlignmentError, SequencerEvent, SequencerLimits};

fn show(events: Vec<SequencerEvent>) {
    for event in events {
        match event {
            SequencerEvent::Word { completion, .. } => println!(
                "provider reconstruction {:?}: word={:?}, original={:?}, complete={}",
                completion.evidence, completion.word, completion.source_text, completion.complete
            ),
            SequencerEvent::Unreported { completion, .. } => println!(
                "unreported original remainder={:?}; provider cursor remains {}",
                completion.source_text, completion.raw_pos
            ),
            SequencerEvent::Skipped { raw_text, .. } => {
                println!("excluded from synthesis: {raw_text:?}")
            }
            SequencerEvent::UnmatchedWord { word, .. } => {
                println!("unmatched provider token: {word:?}")
            }
        }
    }
}
fn main() -> Result<(), AlignmentError> {
    let mut sequencer = AggregatedFrameSequencer::new(0, SequencerLimits::default())?;
    let context = sequencer.open_context(true)?;
    sequencer.register_spoken(
        context,
        "Total is five dollars. Thank you.",
        "Total is $5. Thank you.",
        "Total is <b>$5</b>. Thank you.",
        false,
    )?;
    show(
        sequencer
            .register_skipped(context, "code", "<code>code</code>", None)?
            .1,
    );
    for (index, word) in ["Total", "is", "five", "dollars."].iter().enumerate() {
        show(sequencer.process_word(context, word, index as u64 * 100_000_000, false)?);
    }
    show(sequencer.close_context(context, 400_000_000)?);
    assert_eq!(sequencer.pending_slots(), 0);
    assert_eq!(
        sequencer.process_word(context, "Thank", 500_000_000, false),
        Err(AlignmentError::ContextClosed)
    );
    Ok(())
}
