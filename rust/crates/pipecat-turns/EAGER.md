# Eager turn speculation

## Pinned source

Behavior is mapped from Python commit `af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

| Source | Native implementation |
| --- | --- |
| `turns/eager_end_of_turn_mixin.py` | `EagerEndOfTurn`: opt-in prediction, idempotent withdrawal, commit clearing, recommended configuration |
| `turns/user_stop/eager_match_policy.py` | `EagerMatchPolicy::{Exact, Normalized}` |
| `turns/user_stop/eager_user_turn_stop_strategy.py` | `EagerUserTurnConfig`, the user aggregator's provisional state and owned deadline |
| `processors/aggregators/llm_response_universal.py::_run_speculative_inference` | copied rich context plus provisional user message, without committing it |
| `turns/speculation_gate.py` | `pipecat-services::speculation::OutputGate` inside `LlmProcessor` |
| `services/llm_service.py::run_function_calls` | withdrawal at complete tool intent before any declaration or callback |
| `tests/test_speculation_gate.py`, `test_eager_user_turn_strategies.py`, `test_eager_end_of_turn_mixin.py` | native `eager_turns`, `eager_runtime`, capability, wire, and projection regressions |

No Python runtime, subprocess or Python test is used by this implementation.

## Installation

Set `UserAggregatorConfig.eager` to `Some(EagerUserTurnConfig::default())` in an
`LLMContextAggregatorPair`. The configuration replaces local detector policies
with external start/stop policies, preserving the interruption setting. Put a
native `LlmProcessor` after the user half, and the assistant half after delivery.
The gate is built into the LLM; no downstream buffering processor is required.

A provider adapter can drive `EagerEndOfTurn` with detailed STT transcripts. The
helper preserves user identity, language and provider payload; it does not
manufacture a prediction from ordinary interim transcripts. The separately
opted-in [Cartesia Turns adapter](../pipecat-streaming-speech/CARTESIA_TURNS.md#opt-in-source-bound-eager-processing)
drives source-bound eager signals through
`CartesiaTurnsProcessor::with_eager_turns`, behind the non-default
`cartesia-turns-eager` feature. Its exact local source ownership, suppressed
interim/UI previews, same-socket provider ambiguity and qualification limits
are documented there. Other adapters require their own explicit integration.
`EagerEndOfTurn::with_match_policy` carries an explicit matching policy into the
helper's recommended eager configuration.

## Turn and context contract

An eager transcript starts a real provider request while the user turn remains
open. The request uses a provisional rich snapshot. Neither its user message nor
its generated reply enters live conversation history before the committed turn.
The provisional request does not overwrite the LLM's persistent request context.
Provisional text and transcript matching use the external strategy's verbatim
segment concatenation; committed history retains the native aggregator's existing
inter-frame spacing and trim rules.
Whitespace-only final segments and finals with `append_to_context=false` still
participate in source matching. The latter remain excluded from committed history;
confirmation never inserts an empty replacement user message.

A matching final transcript commits the exact final text once, confirms the
native owner, and suppresses a second inference. The LLM synchronizes its
persistent context from that committed snapshot. A stop boundary must then pass
the LLM before held output is released. This also handles a system stop boundary
that overtakes the normal-priority provisional context.

A mismatch, unconfirmed source withdrawal, new turn, mute, context revision,
deadline, interruption, owner drop, or terminal lifecycle invalidates the
prediction. Matching text cannot revive a withdrawn or expired owner. Tool intent
before confirmation withdraws the entire prediction; the eventual final transcript
runs an ordinary fresh request. Confirmed tool intent remains ordered behind
the stop boundary.

For source-bound input, confirmation transfers authority to `SpeculationOwner`;
later source-only revocation or source-health changes do not cancel transferred
output. The owner's lifecycle and downstream output/effect admission still apply.
The Cartesia adapter documents its separate normal-reader-EOF preservation of
already-offered final rights.

Normalized matching performs NFKC, full default case folding, apostrophe
removal (`'`, U+2019, U+02BC), Unicode letter/number/underscore preservation and
whitespace collapse. Case-folding tables are Unicode 16 (`caseless`); NFKC and
category tables follow the pinned native dependencies. Unicode versions depend
on the Python runtime in the source implementation. Exact matching is available
when normalization is unwanted. Custom Python match-policy subclasses are not
executed or imported.

## Owned output and effects

The default service bounds are 512 frames and 4 MiB of retained payload,
routing metadata and exact signed continuation carriers. Exceeding either bound
withdraws instead of releasing partial output. Default unresolved speculation
expires after five seconds; the user aggregator owns one tracked deadline
supervisor. Transcript size is bounded separately. End/Stop preparation cancels
speculation before draining owned provider work, including a stalled stream.

Holding begins at the speculative response start, not when a provisional
context joins the serialized inference queue. A preceding ordinary response is
therefore not captured by a later prediction. Unrelated system controls bypass
the hold; generated tool declarations are response-semantic and remain held.
Uninterruptible unrelated normal frames retain order and survive withdrawal.
Retained output uses its original causal context and health generation rather
than the releasing system frame's newer admission.

`SpeculationOwner` is the only confirmation authority. Its `NativeSpeculation`
capability is process-local, cannot be forged from metadata, and cannot cross a
wire. Context clones and duplicate frames share single-use inference admission.
All existing manual native-frame wire guards reject unconsumed speculation,
including codecs that would otherwise ignore the frame type. RTVI suppresses
provisional prompt projections and unconsumed speculative tool declarations;
normal post-admission tool progress/results remain observable. Provisional
context necessarily travels to the LLM and remains visible to trusted, opt-in
context/diagnostic observers. Default structural observations carry no message
content. Generated `FunctionCallsStarted` only enters the pipeline after release.

Output buffering is not the tool safety boundary. Both `ToolExecutor` (and MCP
through it) and `FlowProcessor`/`FlowManager` recheck the bound source invocation,
health generation and released owner at the callback boundary. The check wraps
the callback constructor and each future poll. Owner cancellation or a source
health transition wakes and drops pending callback work. Same-value health
restorations are checked on the next callback poll even when they do not emit a
transition notification. Existing duplicate/signed-history admission remains
intact. A cancellation cannot undo an external effect from an already admitted
poll; callbacks must remain cancellation-safe and must not detach untracked work.

## Qualified scope

The integrated inference path is `pipecat-services::LlmProcessor`: native Chat
Completions-compatible/OpenAI/Azure, Anthropic, and Gemini streaming adapters.
Synthetic native loopback tests qualify the local orchestration and exact carrier
handling; no provider credentials or live-account behavior are qualified.
`pipecat-responses` still explicitly rejects speculative contexts. Realtime
speech-to-speech services and separate provider-specific inference processors
are not enabled by this configuration. Custom tool executors must honor native
capability admission before effects; downstream buffering alone is insufficient.
The separate unsigned completion-marker processor is not part of this feature,
and signed marker-looking text and signatures are never rewritten by this gate.
