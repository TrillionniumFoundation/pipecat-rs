# Realtime token usage metadata

The pipeline adapter carries four existing provider breakdowns on its original
`Metric::LlmUsage` frame under `metadata["llm_usage"]`:

- `cache_read_input_tokens`: input token details' cached count
- `input_audio_tokens`: input token details' audio count
- `output_audio_tokens`: output token details' audio count
- `cache_read_input_audio_tokens`: nested input cached-token details' audio count

The three base counts are copied unchanged. These are breakdowns, not additional
tokens: no normalization, summation, inferred zero for an absent object, delta
accumulation, pricing, or billing guarantee is provided. Unknown fields and raw
provider payloads are not forwarded. Metadata appears only when at least one
selected detail exists, on the same usage frame, never on response-end or audio
bracket frames.

## Compatibility and validation

Public `Usage`, `TokenDetails`, `RealtimeEvent`, `RealtimeSession::next_event`
and `RealtimeFrameMapper::frames` retain their shapes and behavior. A private
received-event wrapper binds details to the exact original event; it shares the
existing queue, response/generation filters, admission fences and cancellation.
There is no separate mutable last-usage cache and no response lookup by guess.
The raw public mapper continues to produce its legacy base-only usage metric.
The existing typed decoder also tolerates positional arrays for some structures.
This historical Serde behavior is preserved, but is not recommended protocol
input: a non-object response, usage, or input-details ancestor contributes no
new nested cached-audio projection. Existing cache and audio counts still come
from the original typed event. The newly recognized cached-details value itself
must be an object (or null); an array is never treated as an empty object.

Input/output detail objects absent or null omit their respective fields. This
preserves the native Option contract; fixed Python Usage requires those two
detail objects, so this is not a claim of identical schema admission. In an
existing detail object, omitted scalar counts still default to zero, exactly as
the existing native typed decoder does. Explicit null for those existing u64
fields is still rejected; this remains stricter than fixed Python's optional
integer types. The nested `input_token_details.cached_tokens_details` object is
newly recognized: absent/null omits cached audio, an empty object defaults audio
to zero, explicit `audio_tokens: null` omits it, and an explicit unsigned count
including zero or u64 maximum is preserved. Its other keys, including text
counts, are ignored rather than projected or validated by this extension.

Nested cached-audio fields are parsed only for `response.done`, after the
original typed event decoder but before actor response state changes. Invalid
object shapes or signed, fractional, string, boolean, array, or out-of-range
audio counts reject the event. No success continuation is emitted for that
malformed packet. Existing status handling is retained: usage on cancelled or
failed responses is not silently reclassified as successful completion.

## Pipeline flag and RTVI

The production adapter now honors `StartFrame.enable_usage_metrics`. Its default
is false. Earlier native adapter versions emitted usage even when this flag was
false; correcting that bug changes default pipeline output. Enable the flag to
retain usage emission. This does not alter the public low-level mapper or govern
other kinds of metrics.

RTVI remains independently opt-in. `with_llm_usage_details(true)` selects the
cache-read bucket; `with_llm_audio_usage_details(true)` selects the three audio
buckets. With neither, the original three-field token record is unchanged.
Both selected groups share exact base-count and unique-metric ownership checks;
any invalid selected field falls back to the legacy base-only record.

## Reference and qualification

Fixed source: ProfHepta/pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`services/openai/realtime/llm.py::_handle_evt_response_done`,
`services/openai/realtime/events.py` token models, and `metrics/metrics.py`.
Local WebSocket protocol, pipeline, and serialized RTVI tests are distinct from
live OpenAI service acceptance, browser display, audible playback, and billing
qualification. No API credentials or live provider calls are required.
