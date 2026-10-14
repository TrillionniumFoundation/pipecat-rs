# Opt-in LLM usage breakdown

`RtviObserver::with_llm_usage_details(true)` projects three already-generated
numeric fields from `frame.metadata["llm_usage"]` into the existing RTVI metrics
`data.tokens` item: `cache_read_input_tokens`, `cache_creation_input_tokens` and
`reasoning_tokens`. Default is false, preserving the old exact wire shape.
Neither `RtviObserverParams` nor core `Metric::LlmUsage` gains fields or variants;
existing Rust constructors and serialized base metrics remain compatible.
`reset()` clears observation state but retains this explicit configuration.

Admission is deliberately unambiguous: the frame must contain exactly one
LlmUsage metric, and metadata prompt/completion/total must be unsigned 64-bit
integers exactly matching that metric. Other metric kinds may share the frame.
Missing/null optional fields are omitted; explicit zero is retained. If any known
field has an invalid type/range, any base count is absent/mismatched, or multiple
LLM metrics exist, the entire extension is ignored and the original projection
continues. This is not a metadata authentication boundary.

Only six named fields are borrowed/read. Unknown keys, prompts, model names,
provider payloads and audio-token buckets are never cloned or forwarded. The
projection is subject to the existing final message budget. Metrics-disabled,
first-push and frame deduplication behavior is unchanged. It enriches one existing
item; it does not emit an additional metric, accumulate snapshots, infer deltas,
recalculate totals or convert provider-specific accounting conventions.

## Accounting boundary

The fixed Python `LLMTokenUsage` and RTVI observer use optional fields and
`exclude_none=True`. Python distinguishes prompt net/gross cache conventions.
Native service metadata already reflects its current decoder normalization:
Anthropic in `pipecat-services` currently reports gross prompt (uncached plus
read/write cache), unlike the fixed Python net prompt; Bedrock and OpenAI-family
paths have their own existing conventions. This change preserves all three base
counts exactly. Cache and reasoning values are breakdowns, not extra tokens to
add to prompt/completion/total. No cost/billing calculation is supplied.

The source has three additional audio-related optional buckets; this slice only
exposes the three fields already represented by the native services `Usage` type.
Python integer coercion/range permissiveness is not duplicated: malformed numeric
metadata cannot alter the legacy metric. Missing data is not invented as zero.

## Evidence

Fixed reference: ProfHepta/pipecat
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, `metrics/metrics.py` and
`processors/frameworks/rtvi/observer.py::_handle_metrics`. The original-AST oracle
and corruption controls are distinct from native protocol tests.

Native tests exercise real local HTTP/SSE decoding through `LlmProcessor`, the
original typed Usage metadata, `RtviObserverTap`, and serialized RTVI output.
They verify opt-in/off, explicit zero, source usage disabled, final Anthropic
cumulative counts, exactly one token record, malformed/ambiguous metadata and
retained frame identity. Existing observer/pipeline/WebSocket/frame tests retain
applicable transport and default-shape coverage. No live provider or browser
billing display is qualified by these deterministic tests.
