# pipecat-text

Native Rust text aggregation and filtering with typed `pipecat-core` processors.
No Python interpreter, subprocess, provider account, network connection, or model
weights are required at build time or runtime.

## API

- `CartesiaPronunciationTransform`: bounded, explicit IPA preparation for
  `TtsTextInputFrame`; only synthesis text changes. Display/original text and
  options remain intact. ASCII dictionary keys, Unicode word boundaries and
  source-specific inline phonemes are detailed in [PRONUNCIATIONS.md](PRONUNCIATIONS.md).
- `SimpleTextAggregator`: streaming sentence boundaries with the same native
  SentenceX 0.1.32 engine used by the source's Python SentenceX 1.0.32 binding.
  Abbreviations, decimals, URLs, regional language selection, quotations,
  character-level lookahead, and word-end retries use the source state machine.
- `PatternPairAggregator`: literal REMOVE, KEEP, and AGGREGATE pairs; split
  delimiters; match callbacks; unclosed-pattern and partial-delimiter flush
  handling. KEEP preserves delimiters, matching the source implementation.
- `SkipTagsAggregator`: buffer tagged fragments and keep protected spans intact
  during sentence matching, including checks after a tag has closed. Token mode
  holds partial start delimiters across input chunks.
- `MarkdownTextFilter`: Markdown formatting removal, source-compatible whitespace
  and numbered-list handling, HTML entities, links, repeated-sequence filtering,
  stateful code/table removal, settings updates, and interruption reset.
- `TextSegmentMap`, `WordCompletionTracker`, `AggregatedFrameSequencer`: bounded,
  generation-scoped provider-word alignment and completed-aggregate sequencing;
  original source spans, atomic transforms and explicit unreported remainders.
  See [speech/text alignment](ALIGNMENT.md) for evidence semantics, provenance,
  deliberate source corrections, and the remaining service/playout integration.
- `ParallelSentenceAggregator`, `TokenFrameSequencer`: bounded three-channel
  TOKEN promotion using the existing sentence engine, transform-safe cuts,
  language changes, early-word replay, stable progress anchors, and transactional
  backpressure. Timestamp-free completion requires an explicit synthesis signal
  and remains unreported reconstruction. Run the `token_alignment` example.
- `markup_utils`, `alnum_utils`, `word_timestamp_utils`: static versus truncated
  markup handling, character-offset translation, Unicode folding, tagged cursor
  advancement, markup runs, and punctuation/space timestamp merging.
- `string`: sentence matching, language normalization, tag scanning, proper-prefix
  matching, and aggregation-aware concatenation.
- `SentenceAggregator` and `LlmTextProcessor`: real `FrameProcessor`
  implementations with typed aggregated frames and downstream emission. The LLM
  processor preserves raw pattern text and `skip_tts`, flushes before response/end
  markers, and clears pending text on interruption.

```rust
use pipecat_text::{SimpleTextAggregator, TextAggregator};

let mut aggregator = SimpleTextAggregator::default();
assert!(aggregator.aggregate("Dr. Smith is here.").is_empty());
assert_eq!(aggregator.aggregate(" Next")[0].text, "Dr. Smith is here.");
assert_eq!(aggregator.flush().unwrap().text, "Next");
```

Public cursor offsets count Unicode scalar values, matching Python character
indices rather than UTF-8 byte offsets. Category tables use Unicode 15.0.0,
matching the fixture interpreter; accent folding uses canonical decomposition.
The basic text aggregators retain unfinished text without a size limit, as the
source does. Applications accepting untrusted streams should impose their own
input budgets. The parallel sentence/TOKEN sequencer and speech-alignment APIs
have explicit byte, token, slot, context, and early-word bounds.

## Verification and provenance

The implementation is based on these files at source revision
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- `src/pipecat/utils/string.py`
- `src/pipecat/utils/text/{base_text_aggregator,simple_text_aggregator,pattern_pair_aggregator,skip_tags_aggregator}.py`
- `src/pipecat/utils/text/{base_text_filter,markdown_text_filter,markup_utils,alnum_utils,word_timestamp_utils}.py`
- `src/pipecat/processors/aggregators/{sentence,llm_text_processor}.py`

`tests/fixtures/source_traces.json` records outputs from the unchanged Python
implementation while running all 110 tests in the original
`test_simple_text_aggregator.py`, `test_pattern_pair_aggregator.py`,
`test_skip_tags_aggregator.py`, `test_markdown_text_filter.py`,
`test_markup_utils.py`, and `test_word_timestamp_utils.py` suites, plus targeted
streaming/Unicode/Markdown cases. It contains dependency versions and source
SHA-256 hashes. Its source oracle used SentenceX 1.0.32 and Markdown 3.11;
Markdown 3.11 satisfies the source requirement `>=3.7,<4` but differs from the
repository lockfile's Markdown 3.10.3.

Rust tests replay the recorded public operations and utility calls. The original
private-buffer injection test has an equivalent native module test. Additional
native tests port `test_sentencex_tokenizer.py`'s multilingual/chunk-size,
quoted-abbreviation, pronunciation-initial, bounded-tokenizer-work, and
interruption cases. They also verify callbacks, typed frame behavior, and a real
pipeline's end-marker ordering.

```sh
cargo test -p pipecat-text --offline
cargo fmt -p pipecat-text --check
cargo clippy -p pipecat-text --all-targets --no-deps --offline -- -D warnings
```

The scripts under `tools/` are optional developer-only fixture/table generators.
Cargo does not invoke them. The fixture generator imports production modules and
original tests directly, bypassing only top-level package initialization that
otherwise requires the whole Python distribution.

## Compatibility boundaries

- The CPU-only aggregator API is synchronous and returns an ordered vector rather
  than an asynchronous generator. Match callbacks are synchronous, return
  `Result<(), String>`, and their errors are available through
  `take_handler_errors`; `take_matches` allows an async application to dispatch
  match events itself. Empty pattern delimiters are rejected explicitly.
- Markdown uses native pulldown-cmark with source-specific compatibility handling.
  All recorded source cases pass, but it is not the Python-Markdown engine and
  arbitrary extension/plugin grammars are not claimed to be byte-identical.
  Only source code/table state persists across calls; partial Markdown emphasis
  and backtick delimiters are not buffered beyond what the source buffers.
- WORD mode intentionally follows the source's sentence-mode behavior. The
  separate `SentenceAggregator` emits the whole buffered chunk upon any detected
  sentence boundary and does not discard it on interruption; it is intentionally
  distinct from `SimpleTextAggregator` and `LlmTextProcessor`.
- Tag and pattern parsing is literal and non-recursive. Ordered tag-scanning and
  unmatched-pair behavior follows the source rather than a general XML parser.
- Intentional protected-span correction: the unchanged Python
  `SkipTagsAggregator` can split inside a closed `<spell>` span when a later
  sentence-boundary check tokenizes its full buffer. Native sentence checks
  mask protected spans with equal-character-width text and rebase tag offsets
  after emitting a sentence. `tests/fixtures/protected_span_source_bug.json`
  records the reproduced Python output and corrected native output; the
  original source traces remain unchanged and continue to pass. Unicode and
  repeated-span regressions cover different chunk sizes.
- The TTS-service integration cases in `test_sentence_tokenizer_language.py`,
  parallel-channel TOKEN-mode sentence promotion, phoneme utilities, and text
  transform producers are not implemented in this crate. The completed-aggregate
  sequencer is implemented as a utility; provider and playout adapters are still
  required. Language normalization,
  independent aggregators, and buffer-preserving language updates are tested.
