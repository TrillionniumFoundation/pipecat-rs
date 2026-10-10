# Speech/text alignment

`TextSegmentMap`, `WordCompletionTracker`, `AggregatedFrameSequencer`, and
`TokenFrameSequencer` are native, synchronous utilities for aligning provider word reports with synthesis
text, display text, and original LLM text. They do not perform synthesis, schedule
audio, acknowledge transport writes, or mutate spoken history.

## API and meaning

```rust
use pipecat_text::{AlignmentLimits, AlignmentEvidence, WordCompletionTracker};

let mut tracker = WordCompletionTracker::new(
    4, "Total is five dollars", Some("Total is <b>$5</b>"),
    Some("Total is $5"), AlignmentLimits::default(),
)?;
for word in ["Total", "is", "five", "dollars"] {
    let event = tracker.add_word(4, word)?;
    assert_eq!(event.evidence, AlignmentEvidence::ProviderReport);
    // event.source_span/source_text refer to the original LLM text.
    // During "five", the atomic "$5" span is held and context is suppressed.
}
# Ok::<(), pipecat_text::AlignmentError>(())
```

All offsets are Unicode scalar counts, never UTF-8 byte offsets or grapheme
indices. `CharacterSpan` is half-open. Source ranges preserve whitespace and form
a monotonic, nonoverlapping partition of text actually attributed by the tracker.
The trimmed `word`/`overflow` fields preserve the source frame API's behavior.
Accumulated/remaining text views partition their corresponding strings without
implicit trimming. Reconstruction views are not audible positions.

The map uses the source's whitespace-token SequenceMatcher alignment, including
its earliest-source/earliest-target tie break and no automatic junk tokens. A
transform producer can instead supply validated contiguous `TextSegment` values
through `from_segments`, avoiding inferred diff boundaries. Original and LLM text
must contain identical folded alphanumeric content in order; unrelated source
text is rejected. Markup does not count as letters for this validation.

Unchanged content progresses word by word. A rewrite such as `$5` to `five
dollars`, a word-count change, or a synthesis-tagged span is atomic: the original
cursor waits until the expansion completes. Case/accent and typography folding,
markup fragments, word overflow, repeated punctuation, symbols, leading spaces,
French separated punctuation, CJK groups and trailing partial text follow the
source matching rules. Missing events can resynchronize at the next three word
starts by default; this is configurable. Reconstruction may attach trailing
punctuation/markup without a separate provider event. Textual completion uses the
source's alphanumeric rule and is not proof that every trailing symbol received
an event. A standalone tracker can still consume a trailing emoji after textual
completion; a completed sequencer slot is retired.

### Evidence is not playout

- `ProviderReport`: a provider token was textually matched, including permitted
  normalization or symbol substitution; original transforms can still be atomic
- `Resynchronized`: a matching provider token advanced past missing text events
- `UnreportedRemainder`: source-compatible fallback reconstruction with no matching
  provider report; its raw synthesis cursor stays where it was
- `AlreadyComplete`: the late token was ignored

The sequencer emits missing text through a distinct `SequencerEvent::Unreported`
variant. `SequencerEvent::Word` never contains `UnreportedRemainder`. An
application may consciously use unreported text in a source-reconstruction
context; it must not treat that event as a spoken word. `append_to_context` is
application intent and `suppress_in_context` is reconstruction metadata, not an
audio attestation. A resynchronized word includes the skipped source span and
must likewise not be converted directly to a word-level audible boundary.

### Sequencing and interruption

Open a context, register complete aggregates, then route its provider tokens with
that exact opaque `ContextId`. Completed spoken slots unblock skipped slots at
the head of the global queue. Word routing is per context: a later context can
report words while an earlier context still blocks skipped-frame release. As in
the source, this is not a global cross-context audio scheduler. A zero-nanosecond
PTS is preserved, including on released skipped text.

`close_context` releases explicitly unreported suffixes and retires the context;
late events then fail closed. Unlike source force completion, skipped slots
between incomplete aggregates are released in registration order. `interrupt`
discards all pending state and advances the checked generation. IDs include an
opaque owner token, so they also cannot be accidentally used in a different
sequencer instance. Context/segment serials are never recycled in an instance.
Standalone trackers require a strictly newer generation for `reset`.

Generation fencing is not provider-event deduplication. Repeating a valid token
in a live context can consume a later repeated word. An adapter must retain the
provider's event sequence/identity when available and reject duplicates before
calling this utility. PTS alone is not a unique event identity.

## Bounds

`AlignmentLimits` defaults: 65,536 UTF-8 bytes per text, 4,096 whitespace tokens
per text, 4,096 segments, 4,096 bytes per provider word, and 4,000,000 diff token
comparisons. Text lengths are admitted before fallback markup allocation;
token count is checked while tokenizing. Diffing uses an iterative work list,
not recursive stack depth. Its temporary storage is bounded by admitted text
and token counts. Markup splitting's temporary runs are bounded by admitted text
bytes; segment count limits the retained result. Word matching has bounded
lookahead and no retained event log.

`SequencerLimits` defaults: 32 live contexts, 256 pending slots, and 1 MiB of
pending input text. The byte budget counts synthesis/display/LLM strings (plus
skipped routing text), not allocator overhead or an exact resident-memory total.
Segments, immutable map copies, and the last result add storage bounded by those
admitted strings and slot counts. Returned event vectors belong to the caller;
callers must bound downstream queues. Finished slots release their budget;
contexts remain live until explicitly closed or interrupted. The completed-aggregate API does not buffer early words; use the explicitly
bounded TOKEN facade below when provider reports can precede promotion.

Oversized registration and word events fail before mutation. Identity exhaustion
fails closed. `from_segments` validates the caller's existing vector; callers
remain responsible for their own allocations before passing it in.

## TOKEN-mode promotion

`TokenFrameSequencer` composes the completed-aggregate core with a per-context
`ParallelSentenceAggregator`. The latter uses the existing native
`SimpleTextAggregator` and sentence matcher. It requires no additional sentence
engine, Python process, network service, or provider SDK.

```rust
use pipecat_text::{TokenFrameSequencer, TokenReporting, TokenSequencerLimits};

let mut seq = TokenFrameSequencer::new(0, TokenSequencerLimits::default())?;
let ctx = seq.open_context(true, TokenReporting::WordTimestamps, Some("en"))?;
assert!(seq.register_token(ctx, "Hello!", "Hello!", "Hello!")?.is_empty());
assert!(seq.process_word(ctx, "Hello", 0, false)?.is_empty());
let events = seq.register_token(ctx, " Next", " Next", " Next")?;
// Promoted anchor, replayed Word, then display Progress, all with the same
// context/segment identity. The original zero PTS is preserved.
assert_eq!(events.len(), 3);
seq.interrupt()?; // discards tokens, early words, slots, and progress anchors
assert!(seq.process_word(ctx, "late", 0, false).is_err());
# Ok::<(), pipecat_text::AlignmentError>(())
```

`register_token` takes synthesis, display, and original LLM text, in that order.
`ParallelSentenceAggregator::aggregate` uses synthesis, LLM, display order,
matching the source helper. Input spacing is retained exactly. A terminal mark
requires lookahead. Identical channels can split a coarse token at several
confirmed sentence boundaries. Divergent/transformed channels cut only at token
edges; alignment is restored only when all channels also agree with the sentence
driver's pending buffer. `set_language` changes subsequent boundary decisions
without clearing pending text. `finalize` promotes a trailing partial sentence
while keeping the context live for later words. Skipped registration finalizes
its own pending context first, so the skipped slot cannot jump that sentence.

`Promoted` establishes a stable segment anchor and is an announcement, not an
instruction to append text to context. `Progress` references that exact anchor;
it partitions display text and carries its reconstruction evidence. Provider-backed progress is
suppressed while an atomic transformed span is incomplete. An explicit Unreported
completion can reconstruct the full display text with Unreported progress; it
does not move the raw provider cursor or claim the transform was played. `Alignment` wraps the
existing explicit Word, Unreported, Skipped, and UnmatchedWord variants.

Early provider words and overflow suffixes are retained with their exact context,
generation, PTS, and spacing flag. Promotion replays one snapshot for that context;
still-unmatched reports wait for later promotion rather than looping. Closing
finalizes/replays the final partial, emits unmatched reports as diagnostics, then
reconstructs remaining text as Unreported. No speculative provider passthrough
exists. The owner still has to deduplicate provider events by genuine event
identity, not token text or PTS.

Choose `TokenReporting::SynthesisCompletion` for providers without timestamps.
Dispatch and promotion do not complete a slot. After the service establishes a
promoted sentence's synthesis completion, `complete_synthesis_segment` targets
its exact context and earliest incomplete segment and releases that slot without
closing the context. Out-of-order same-context completions fail without mutation;
retry them after their predecessors complete. Other contexts remain independent.
`complete_synthesis` completes and closes the whole context. These paths emit
Unreported reconstruction and progress, with an unchanged raw synthesis-report
cursor. They cannot establish played audio. Failed or cancelled requests should
not use a successful-synthesis completion signal. Word events are rejected in
this mode; an arbitrary or another context's segment cannot be acknowledged.

This deliberately strengthens the source's no-timestamp TOKEN behavior, which
completes a sentence immediately on promotion based on prior dispatch. Explicit
completion is needed here because dispatch is not synthesis success or playout.

### TOKEN bounds and backpressure

`TokenSequencerLimits` adds defaults of 1,024 early words and 65,536 early-word
bytes. Its embedded `SequencerLimits::max_pending_text_bytes` covers retained
slot input strings, all three pending text channels, the sentence driver's
additional synthesis buffer, display anchor copies, and early-word bytes. Pending
channels reserve any additional display-anchor storage required at promotion, so
admitted text can finalize without a larger byte budget. Locale identifiers have
a separate 64-byte pre-normalization cap per context.
Per-channel byte/token limits are checked before feeding the sentence engine.
Admission conservatively includes the whole incoming token plus existing pending
text, even when processing that token would promote and release some text.

Each mutating multi-step operation runs against a private bounded checkpoint.
Rejected input, a partially matching word with an unbufferable overflow suffix,
multi-sentence promotion, and finalize-before-skipped registration all roll back
as a unit, including segment IDs. The caller may drain other work and retry the
same event. Splitting an oversized coarse token is safe only at an upstream-known
three-channel boundary; arbitrary splitting can corrupt transform provenance.

The checkpoint duplicates admitted state temporarily; memory is bounded by the
configured retained data, map/driver working storage, and slot/context counts,
not exactly the byte counter. One additional internal working slot lets close
promote a final partial when the public queue is full. Closing retires its complete
tracked slots even behind another live context; skipped slots keep their positions
and remain blocked by earlier unfinished contexts. Every committed transaction
still obeys the configured public slot cap. Returned vectors and downstream queues remain the
caller's responsibility. Progress text is released when its slot completes, even
if another context still blocks global slot retirement.

Run `cargo run -p pipecat-text --example token_alignment --offline` for a native
example of promotion, early-word backpressure/retry, transformed progress,
skipped ordering, interruption, and explicit no-timestamp completion.

## Source provenance and validation

Baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

- `src/pipecat/utils/context/text_segment_map.py`
- `src/pipecat/utils/context/word_completion_tracker.py`
- `src/pipecat/utils/context/aggregated_frame_sequencer.py`
- `src/pipecat/services/tts_service.py` (integration contract inspection only)

`tools/generate_alignment_fixtures.py` is developer-only. It verifies source/test
bytes against that Git revision, imports the production modules unchanged while
bypassing package initialization, and runs all 302 original map/tracker tests.
`tests/fixtures/alignment_source_traces.json` contains 304 object traces, Python
and dependency versions, and SHA-256 hashes for both original test files and all
four source modules involved. Cargo tests load this static JSON directly. Cargo
and the shipped library never invoke Python, subprocesses, a network service, or
provider SDKs.

`alignment_source_traces.rs` compares segment construction, public operations,
and all recorded cursor/reconstruction states. There is one explicit corrected
oracle expectation, with the original JSON unchanged:

- `TestWordCompletionTrackerTokenChangingReplacements.test_hyphenated_single_token_replacement_is_not_discarded`,
  operation 1 (`body-pump`): source accumulation reports `Try BODYPUMP ` with an
  unreported trailing space. Native accumulation stops at `Try BODYPUMP` because
  extra synthesis punctuation cannot spend future original characters. The same
  correction prevents `a------` from attributing the later `b` in original `a b`.

Other intentional corrections are asserted independently in `review_alignment`:
forced completion cannot emit an already attributed comma twice; source ranges
retain whitespace across held transforms; punctuation-expanded display offsets
stay within their original segment. Python's U+001C–U+001F whitespace semantics
are covered as well as standard Unicode whitespace.

`alignment_contract.rs` maps the source build/advance, transformed spans, tagged
LLM, reset, token-changing replacements, SSML, stray-angle, provider-token-shape,
case/accent/typography, duplicate punctuation, separated punctuation, CJK,
overflow, missing-event, symbol, emoji, and remaining/accumulated test groups to
native assertions, with additional explicit-budget/generation tests.

`sequencer_contract.rs` maps the source's completed-aggregate cases for skipped
slots, flush ordering, word frames, context flags, raw attribution, overflow,
missing events, force completion, concurrent contexts, clear/interruption and
CJK spacing. Native tests add transactional limits, independent-owner IDs,
identity exhaustion and the absence of speculative streaming passthrough. This
is selected behavioral coverage, not a replay or full port of the entire source
sequencer test suite.


`tools/generate_token_fixtures.py` records TOKEN behavior from the same baseline.
The fixture contains 55 traces and 218 operations from 47 unchanged original tests
plus eight supplemental language/Unicode/interruption probes. All 154 tests in
the original sequencer module also pass under the oracle harness. Provenance
records 18 source/test SHA-256 values, interpreter/dependency versions, and exact
baseline frame AST definitions used to avoid unrelated provider imports. The
sequencer, sentence utilities, and original tests execute unchanged. Regeneration
is deterministic and fails before replacing fixtures if source validation or a
selected source test fails. The source's no-timestamp auto-completion behavior is
recorded unchanged, even though native completion deliberately requires a signal.

`token_sequencer_contract.rs` adds 31 native tests for transactional slot/text/word
budgets, replay overflow, identity fencing, concurrent contexts, transformed
progress, explicit segment/context synthesis completion, and duplicate-token
non-deduplication. `token_source_traces.rs` replays 23 parallel traces (87 operations) and 28
tracked TOKEN traces (121 operations), comparing emitted frame projections,
progress-anchor identity, queue counts and retained early-word bytes. The replay
explicitly excludes three non-streaming traces and the source's implicit
no-timestamp completion trace. Its adapter maps implicit source context names to
issued native IDs, checks four native stale-generation errors before projecting
them to the source's empty output, and trims native lossless source text only
for comparison with the source frame accessor. The fixture is unchanged. The
original completed-aggregate map/tracker oracle also remains unchanged.

## Integration work still required

No streaming, media, or core crate is changed by this utility layer.

1. In `pipecat-streaming-speech/src/processor.rs` and the ElevenLabs TTS processor,
   register the synthesis/display/LLM aggregate before dispatch. Map each provider
   context ID and cancellation generation to an issued `ContextId`. Feed
   validated `WordTimestamp` tokens at the existing word-timestamp event branch,
   preserving context-local start/end timing, source frame identity, and CJK
   spacing metadata. Reject stale and duplicate provider events before mapping.
2. Translate reconstruction results into typed TTS text/progress frames with
   explicit original spans. Keep `Unreported` separate through the adapter;
   discard it or opt into source-compatible context reconstruction explicitly.
   Do not set a spoken-history flag from provider timestamps or text completion.
3. For TOKEN mode, use `TokenFrameSequencer` with one issued context per synthesis
   stream/turn. Register each three-channel token before dispatch. Queue promoted
   anchors, replayed words and progress into the same bounded single-consumer
   audio-context stream as provider events; direct concurrent output can reorder
   them. Call `finalize` at end of input and before relevant stop/interrupt handling
   when partial reconstruction is wanted. For timestamp-free providers, feed
   explicit successful synthesis completion to the exact segment or context;
   dispatch/promotion alone must not complete tracking. Adapters are not wired to
   this facade yet.
4. Keep `pipecat-media-transport::SpokenHistory`'s existing whole-context contract:
   successful source response, final TTS stop, and explicit `Played` receipts for
   every transformed PCM write are separate requirements. Its existing
   whole-context acknowledgment does not identify a partially audible word.
   Word-level playback would require a new verified mapping from provider word
   intervals to transformed/resampled PCM ranges and output receipts, plus
   cancellation and partial-write coverage. These utilities do not supply it.
5. Add composed service/transport/assistant tests for reset races, stale provider
   responses, mixed skipped/spoken aggregates, normalized/transformed text,
   resampling, interruption inside an atomic transform, and partial output.
   Credentialed provider calls, physical hardware and acoustic audibility remain
   outside the validation performed here.

## Native service bridge

`TtsAlignment` adds first-audio timestamp baselines and bounded pre-audio events
around the sequencer. `TtsTextChannels` keeps synthesis, display and original
source separate. Register before provider submission; use `register_named` with
`complete_aggregate=true` for an exact completed unit, and false for token
promotion. `register_aggregate` on `TokenFrameSequencer` admits an already-complete
unit without guessing additional sentence boundaries. Skipped units preserve
source delimiters and release after preceding tracked text.

Call `start_audio(now_ns, previous_terminal_pts)` immediately before the first
nonempty PCM frame. Early words are released after their anchor, with checked
seconds-to-nanoseconds conversion and checked baseline addition. A zero timestamp
is valid. The previous terminal PTS prevents a newer serialized context from
moving behind earlier words when the clock lags provider offsets. After explicit
successful provider completion, call `finish_success`; never call it after an
error, cancellation, stale processor invocation or health-revision change.
`cancel` discards state. Any bridge error poisons the context, so adapters retire
it rather than replaying potentially dispatched text.

`TtsAlignmentFrames` maps opaque segment IDs to actual emitted aggregate frame
IDs. Provider-report words become `TtsText`; progress references that exact
anchor. Resynchronized, unmatched and unreported text becomes the distinct native
`TtsAlignmentDiagnostic` frame. No ordinary progress is fabricated for these
diagnostics. Their Pending/Succeeded outcome describes synthesis, never transport
submission, Played receipts or spoken history. A timestamp-disabled provider
therefore reports successful reconstruction diagnostically; applications may
implement an explicit reconstruction policy without treating it as audible text.

Timing-marked frames use the shared pipeline observer timeline. The marker is
local, retained by clone/duplicate, and cannot cross Frame serde or the native
manual wire codecs. `Frame::rebase_pipeline_pts(origin_ns)` is a checked conversion
to legacy destination-relative PTS. `set_relative_pts` explicitly replaces the
PTS and removes the marker. Native OutputTransport performs the rebase at its
scheduling boundary, including explicit zero for already-overdue pre-origin work.

The bridge additionally bounds pre-audio event count/bytes and provider-word
count/bytes. Aggregate names are bounded to 128 bytes. The frame mapper retains
only live anchor identities. Existing alignment limits still apply; these limits
are admission contracts rather than exact allocator resident-memory accounting.

The timestamp-free history path is intentionally incomplete: successful
Unreported reconstruction is not automatically added to audible assistant
history even when later backend receipts prove whole-context playback. Combining
that input with proven playback requires a separately reviewed receipt-bound
policy. The provider bridge never upgrades the diagnostic itself to Spoken.
