# Grok usage admission

The production pipeline emits `Metric::LlmUsage` only when
`StartFrame.enable_usage_metrics` is true and the selected provider usage has a
nonzero total. The flag defaults to false. Earlier native versions ignored this
flag and emitted explicit zero totals; this correction changes that pipeline
output. `enable_metrics` is independent and does not enable token usage.

Usage selection is unchanged: a present top-level `response.done.usage` wins over
`response.usage`, even if its total is absent, null, or zero. Such a selected value
does not fall back to a nested positive total. An absent or null top-level usage
allows the nested usage. Missing or null input/output counts default to zero;
positive totals, including `u64::MAX`, are copied exactly. Counts are never
normalized, summed, or inferred from one another.

Only the original three base counts are emitted. Provider cache/audio detail
fields do not create extended usage metadata, even when RTVI detail options are
enabled. This matches the fixed Python Grok handler, which does not project
those breakdowns. These metrics are not a billing guarantee.

The filter applies only to the mapper's singleton LLM usage metric. Response
end, audio brackets, failed/cancelled status handling, invocation fences, and
other metrics retain their existing native behavior. A failed or cancelled
response can still emit positive usage when enabled. The public `Usage`,
`TokenDetails`, `RealtimeSession::next_event`, and `RealtimeFrameMapper` are
unchanged: the standalone mapper still emits an explicit zero total and does
not observe pipeline configuration.

Reference: ProfHepta/pipecat at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`services/xai/realtime/llm.py::_handle_evt_response_done` and
`processors/frame_processor.py::start_llm_usage_metrics`.
The native unsigned decoder remains stricter than Python integer coercion and
negative-number admission. Local WebSocket/pipeline/RTVI checks do not qualify
live xAI service acceptance, audible playback, or provider billing.
