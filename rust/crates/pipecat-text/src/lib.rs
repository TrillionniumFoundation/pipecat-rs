//! Streaming text aggregation, Markdown filtering, and Unicode text utilities.
//!
//! All public cursor offsets count Unicode scalar values, matching Python's
//! character-indexed API. No Python interpreter is needed at build or runtime.
pub mod aggregation;
pub mod alnum_utils;
pub mod base_text_filter;
pub mod markdown_text_filter;
pub mod markup_utils;
pub mod pattern_pair_aggregator;
pub mod processors;
pub mod simple_text_aggregator;
pub mod skip_tags_aggregator;
pub mod string;
pub mod word_timestamp_utils;

pub use aggregation::{Aggregation, AggregationType, TextAggregator};
pub use markdown_text_filter::{MarkdownSettings, MarkdownTextFilter};
pub use pattern_pair_aggregator::{MatchAction, PatternMatch, PatternPairAggregator};
pub use simple_text_aggregator::SimpleTextAggregator;
pub use skip_tags_aggregator::SkipTagsAggregator;

pub use processors::{LlmTextProcessor, SentenceAggregator};

mod unicode_categories;

pub use base_text_filter::TextFilter;

pub mod text_segment_map;
pub mod word_completion_tracker;

pub use text_segment_map::{
    AlignmentError, AlignmentLimits, CharacterSpan, TextSegment, TextSegmentMap,
};
pub use word_completion_tracker::{AlignmentEvidence, WordCompletion, WordCompletionTracker};

pub mod aggregated_frame_sequencer;
pub use aggregated_frame_sequencer::{
    AggregatedFrameSequencer, ContextId, SegmentId, SequencerEvent, SequencerLimits,
};

pub mod token_frame_sequencer;
pub use token_frame_sequencer::{
    ParallelSentence, ParallelSentenceAggregator, TokenFrameSequencer, TokenReporting,
    TokenSequencerEvent, TokenSequencerLimits,
};

pub mod tts_alignment;
pub use tts_alignment::{
    SynthesisOutcome, TtsAlignment, TtsAlignmentError, TtsAlignmentEvent, TtsAlignmentLimits,
    TtsTextChannels,
};

pub mod tts_alignment_frames;
pub use tts_alignment_frames::TtsAlignmentFrames;
