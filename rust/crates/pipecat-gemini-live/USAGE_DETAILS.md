# Raw usage detail metadata

The runtime frame adapter preserves the provider's optional cache, reasoning,
audio and tool-use counts on an emitted usage metric frame. This is raw reported
accounting, not normalized billing data or a claim that the counts are disjoint.

## Scope and compatibility

`Usage` retains its three optional public fields: `prompt_token_count`,
`response_token_count` and `total_token_count`. `GeminiEvent` and
`GeminiSession::next_event` are unchanged, including construction by complete
struct literals. Public events still expose only the original three totals.
Standalone `GeminiFrameMapper::frames` also keeps its previous output, without
`llm_usage` metadata. Details are private to the live runtime receive path.

The runtime retains the existing gate: a `Metric::LlmUsage` is emitted only when
all three provider totals are present and pipeline usage metrics are enabled.
Missing totals are not replaced by zero. The metric's prompt, completion and
total fields remain the provider's exact prompt, response and total counts,
even when they are mutually inconsistent or an optional count exceeds a total.
No addition, subtraction, summation, saturation or reconciliation is performed.

If at least one optional scalar below is reported, the same metric frame gets
`frame.metadata["llm_usage"]`. Its `prompt_tokens`, `completion_tokens` and
`total_tokens` exactly match the actual metric, followed by only reported
optional fields:

| Provider field | Frame metadata field |
| --- | --- |
| `cachedContentTokenCount` | `cache_read_input_tokens` |
| `thoughtsTokenCount` | `reasoning_tokens` |
| First AUDIO in `promptTokensDetails` | `input_audio_tokens` |
| First AUDIO in `responseTokensDetails` | `output_audio_tokens` |
| First AUDIO in `cacheTokensDetails` | `cache_read_input_audio_tokens` |
| `toolUsePromptTokenCount` | `tool_use_prompt_tokens` |

Absent or null optional counts stay unreported. An explicit zero is retained as
zero. If all selected optional values are absent, no `llm_usage` object is added.
Incomplete totals produce no metric or detail-only frame; their details cannot
carry over to a later complete packet. Disabling usage metrics discards both the
usage event and its details from runtime frame emission.

The existing `RtviObserver::with_llm_usage_details(true)` can expose the cache and
reasoning counts through its existing numeric whitelist. Default RTVI output is
unchanged. Audio counts remain metadata-only unless the separate audio opt-in
below is enabled. Tool-use counts remain metadata-only. Neither option changes
the core metric shape or invents cache-creation data.

## Decoding and ownership

Known usage counts must be unsigned 64-bit integers when non-null. The three
modality detail fields must be arrays when non-null, and every entry must be an
object with an optional/null string `modality` and optional/null unsigned
`tokenCount`. Wrong numeric types, negative or out-of-range counts, wrong array
or entry types and non-string modalities reject the whole packet through the
existing typed protocol error path, before any content or terminal event from
that packet is emitted. This includes malformed entries after a selected AUDIO
entry and malformed counts for an otherwise ignored modality. Unrecognized
extra fields in known usage objects remain informational and may be ignored.

Audio selection uses the first exact uppercase `"AUDIO"` entry. That entry ends
selection even when its count is missing, null or zero. Later AUDIO entries are
neither substituted nor summed. Empty/missing/null arrays and arrays without
AUDIO yield no selected count. Unknown well-typed modality strings, including
lowercase `"audio"`, are ignored rather than assigned a guessed meaning.

One private envelope carries each event and its own details in the existing
bounded queue. A packet's details are taken only by that packet's Usage event;
text, transcripts, generation/turn completion and tool events carry none. Queue
capacity and one-event-per-slot accounting are unchanged. There is no secondary
queue, last-usage state, cross-packet map or background task. The public receive
method and runtime receiver share the original generation filtering,
non-blocking tool claim/cancel ordering and cancellation-safe terminal task join.
Interruption, shutdown/drop and resumption ownership are unchanged.

## Source differences and evidence

Pinned Python reference: `ProfHepta/pipecat`
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- [`_audio_token_count`, lines 134–152](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/google/gemini_live/llm.py#L134-L152)
  supplies the first-AUDIO selection and absent-versus-zero behavior.
- [`_handle_msg_usage_metadata`, lines 2525–2551](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/google/gemini_live/llm.py#L2525-L2551)
  projects cache, reasoning and audio details. It also replaces missing base
  counts by zero, adds tool-use tokens to prompt tokens and thought tokens to
  response tokens, and recomputes total tokens. Those arithmetic and missing-data
  behaviors are deliberately **not** copied here.
- The pinned Python dispatcher handles usage with turn completion; native keeps
  its existing behavior of emitting Usage for each packet that reports it,
  including packets without `turnComplete`.

Google's [Live UsageMetadata reference](https://ai.google.dev/api/live#UsageMetadata)
and [API proto](https://github.com/googleapis/googleapis/blob/master/google/ai/generativelanguage/v1beta/generative_service.proto#L1681-L1717)
describe prompt counts as including cached content and describe total as prompt
plus response. The rolling [Python SDK Live UsageMetadata definition](https://github.com/googleapis/python-genai/blob/main/google/genai/types.py#L19169-L19217)
has a different total description that includes tool and thought counts. These
rolling sources were consulted on 2026-10-16 and do not establish one universal
normalization across models. Preserving all three provider totals avoids
unproven additions or double-counting; cache is not added to prompt again.
`tool_use_prompt_tokens` is an explicit raw metadata projection, not a claim of
Python `LLMTokenUsage` field parity.

Synthetic protocol and real loopback tests cover strict whole-packet rejection,
first-match and zero behavior, incomplete and consecutive usage, terminal-plus-
usage ordering, disabled metrics, public API compatibility, private event
ownership and the existing RTVI whitelist. They do not qualify live-provider
accounting or billing accuracy.

RTVI may now explicitly expose the three existing audio buckets with
`RtviObserver::with_llm_audio_usage_details(true)`. This switch is independent of
the legacy cache/reasoning opt-in and is false by default. Both selected groups
share atomic validation and exact base-count/unique-metric ownership checks.
Tool-use buckets stay private; base totals and audio bucket values are not summed.
