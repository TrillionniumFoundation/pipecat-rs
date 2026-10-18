# Gemini streaming usage snapshots

This describes ordinary Gemini `generateContent` streaming, not Gemini Live.
The decoder keeps the latest reported usage snapshot and emits it once when the
client stream terminates while its consumer remains able to receive. An omitted or JSON-null `usageMetadata` does not erase
that snapshot and does not create one when none was reported. An empty object
is a reported zero snapshot. A partial object replaces the entire previous
snapshot; missing counters do not inherit values from earlier packets.
Explicit zeros, decreases, and repeated snapshots are preserved, not summed.

A missing or null `candidates` field means there are no candidate updates in that
packet. This permits usage-only packets, including a final usage report after a
candidate finish marker. Other non-array candidates, invalid candidate elements,
multiple candidates, and existing finish/content ordering violations retain
their existing errors. This change does not add new permissive candidate forms.

## Counting and compatibility

Native prompt counts are `promptTokenCount + toolUsePromptTokenCount`; completion
counts are `candidatesTokenCount + thoughtsTokenCount`. Each addition saturates
at `u64::MAX`. A present unsigned `totalTokenCount`, including zero, is preserved;
otherwise total falls back to the saturated sum of prompt and completion.
`cachedContentTokenCount` is an optional prompt subset and is not added again.
`thoughtsTokenCount` is an optional reasoning detail already included in
completion. Missing optional cache/reasoning fields remain `None`.

The fixed Python implementation always calculates total from the four component
counts. Native provider-total priority is deliberately retained here because a
sparse provider snapshot can contain a total without all of its components.
Inconsistent provider totals are not silently normalized. Saturation and native
optional fields also remain distinct from Python arbitrary-precision integers
and its initialized zero counters. No public `Usage` field or mapper shape has
changed, and no billing equivalence is claimed.

The official ordinary GenerateContent UsageMetadata defines total across prompt,
tool-use prompt, thoughts and candidates, with cached content included in prompt:
https://ai.google.dev/api/generate-content#UsageMetadata
This definition does not justify discarding a provider total from sparse input.

## Stream termination

Existing client behavior is retained: while the consumer remains available, a
final snapshot is emitted once on normal
completion, cancellation, timeout, or stream/protocol failure, before the error
and final termination event where applicable. Usage is observed before candidate
processing; this is not a new whole-packet rollback guarantee. Existing handling
of malformed usage counter values and non-object usage is not broadened or
hardened in this slice. Pipeline usage emission still requires the existing
`StartFrame.enable_usage_metrics` flag, and RTVI detail fields remain opt-in.

Reference: ProfHepta/pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`services/google/llm.py` streaming loop and its usage branch. Tests use
local HTTP/SSE and native pipeline boundaries; live provider acceptance, account
billing, and SDK validation are separate qualifications.
