# Unsigned LLM completion-marker strategy

`LLMTurnCompletionStrategy` is the deterministic, provider-independent owner of
completion-marker parsing and deadlines. `LLMTurnCompletionProcessor` adapts it
to the native pipeline. Place the processor after an explicitly unsigned LLM
and before its assistant aggregator. `LLMTurnCompletionConfig` includes bounded
resource controls and monotonic `Duration` timeouts; `TryFrom` accepts the core
wire `UserTurnCompletionConfig` and validates its floating-point seconds.

The application composes `config.completion_instructions()` into the LLM's
system instruction. The processor does not inject an unsupported service setting.
Its `stop_strategy()` selects the existing external-completion finalizer. Deferred
user detectors still require an application-owned inference-admission path:
this module does not start speculative conversational inference.

## Source contract

Source revision: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

| Source | Native behavior |
| --- | --- |
| `turns/user_turn_completion_mixin.py` constants/configuration | Configurable `●`, `◐`, `○`; exact source instruction and developer re-prompt templates |
| `_push_turn_text` complete path | Completion signal precedes marker and speech; one leading ASCII space is removed from currently buffered text |
| `_push_turn_text` incomplete path | Marker is appended to context immediately; all response text is suppressed |
| `_report_response` and `_turn_reset` | Bounded raw/marker report; unmarked fallback text is drained on response end |
| `process_frame` VAD/latch handling | Speech cancels waits and resets the voiced latch; stale complete while speaking becomes short/incomplete |
| `_start_incomplete_timeout` | Defaults of five seconds for short and ten seconds for long |
| `user_stop/llm_turn_completion_user_turn_stop_strategy.py` | Explicit strategy API exposes marker configuration and external-completion stop policy; service setup remains application-owned |
| `processors/aggregators/llm_response_universal.py` marker aggregation | Existing native assistant aggregator stores an incomplete marker alone and prepends a complete marker to delivered text |

The three `.txt` prompt files are static source text templates. No Rust runtime,
test, or build step invokes Python.

## Lifecycle and context ownership

A processor owns at most one tracked deadline supervisor, started lazily. It
rechecks the current deadline under the same state lock that handles speech and
response boundaries. End/Stop preparation cancels it before draining tasks;
Cancel, cleanup, and owner drop close the strategy.

A retry is a single append-and-run operation. The runtime represents it with a
process-local `LocalMessageTransform`: the assistant validates a weak owner,
epoch, closed state, and one-shot consumption flag before changing context.
Already queued retries are rejected after a new response/context request, speech,
interruption, terminal preparation, or owner destruction. Rejection is a
nonfatal error and cannot cause an LLM run. This guard is not wire-serializable.

A response that emits an incomplete marker but does not end before its deadline
fails explicitly and closes the strategy. The processor never fabricates a
provider response-end frame to make room for a retry. Late text after response
end or interruption is discarded until a fresh response-start. End drains
unmarked buffered text; Stop/Cancel/interruption discard it.

All native continuation carriers are rejected, including empty response-start
carriers: future chunks can require signed replay even when the initial carrier
has no signed content. Unexpected checkpoints or invalid carriers fail closed.
This module must not be used to rewrite signed Anthropic/Gemini response history.

## Deliberate bounded differences

- The source scans accumulated text for markers anywhere and prioritizes an
  incomplete marker found within a chunk. Native parsing requires a marker at
  the beginning of the response. Missing, partial, or nonleading markers fall
  back to ordinary text without generating a completion signal
- Prefix text is limited to 16 KiB by default, raw reports to 64 KiB. Both are
  configurable up to 1 MiB. Buffered input is also capped at 256 frames and
  64 KiB of serialized routing metadata plus frame-envelope storage. Reaching
  a prefix/envelope/frame bound streams original frames as unmarked fallback
  instead of retaining unbounded data. Prefix and buffered text duplicate at
  most twice the configured prefix bytes; raw reports and scan tails are bounded
- Buffered text frames retain their own text flags, routing metadata, identity,
  timestamps, and local provenance. Marker removal changes only their text
- Raw report metadata explicitly records truncation, prefix-limit fallback,
  malformed output, extra markers, and suppressed duplicate completions. A raw
  report is not evidence that omitted text contained no additional markers
- After a complete marker, later text is passed through unchanged, as in source.
  Repeated marker-looking text is reported as malformed; no additional completion
  is broadcast. Streaming output already delivered is not retroactively revoked
- Automatic re-prompts default to one per speaking segment, configurable from
  zero to 64. New response starts alone do not replenish this budget. Speech or
  an explicit requested run replenishes it
- A stale complete while VAD reports active speech is recorded as short, but its
  timer starts only on the transition to silence. Repeated silence frames do not
  extend it. No check-in is generated over continuing speech
- Interruption discards pending unmarked text rather than flushing it. A stalled
  incomplete response produces a fatal error instead of creating another run
- Source tool-start completion/latch behavior is excluded. Function-call frames
  pass through; this downstream filter neither dispatches nor prevents tool
  effects. Tool-effect admission and eager speculative inference need separate
  integration stages

These changes bound retention and retries and make stale context mutations
explicit. They are not a claim of complete Python turn-management parity.

Unsigned response-start frames are the caller's admission boundary. Old chunks
arriving after an admitted new start have no response identity and cannot be
distinguished locally from that new response. The caller must cancel/fence
superseded provider work before admitting the new stream. This text filter does
not cancel provider work or create a tool-admission gate.

## Assistant transcript events

`AssistantTurnStopped.text` removes all three default markers and any custom
markers observed on this assistant turn. This is an event-only projection:
stored context, provider text, signatures, and continuation carriers are not
rewritten. The projection trims surrounding whitespace only when a marker was
removed; existing aggregation/context normalization is unchanged.

Custom observations are limited to 64 distinct markers, 256 bytes per marker,
and 4096 bytes in total per assistant turn. Empty/oversized/excess markers fail
before their context mutation. Default and duplicate markers do not consume the
custom-marker budget. Limits reset on turn completion, interruption, terminal
cleanup, and the next new assistant turn. Unlike Python's lifetime-grown set,
native custom markers do not leak into unrelated future transcript events.
Overlapping markers are removed longest-first, then lexically, rather than
depending on set iteration order. Spoken `LlmText` has its leading marker removed
by the strategy; context retains that marker for the next model request.
