# NVIDIA NIM reasoning streams

`ClientConfig::openai_compatible(OpenAiPresetConfig::new(OpenAiPreset::Nvidia), key)`
selects NVIDIA's source-pinned request defaults and native response adapter.
The default model is `nvidia/nemotron-3-super-120b-a12b` and the exact default
endpoint is `https://integrate.api.nvidia.com/v1/chat/completions`. Applications
supply credentials explicitly. Set `endpoint` for a local NIM deployment and
set `api_key = None` when that deployment requires no authentication. Other
OpenAI-compatible presets and the generic OpenAI decoder keep their existing
content/reasoning order and literal markup handling.

## Reasoning contract

Only an exact lowercase `<think>` at the beginning of content starts inline
reasoning. A partial opener is held until resolved; whitespace, another case,
a preamble, or a mismatch permanently selects ordinary content. The first
`</think>` ends the thought; later tags pass through literally. Open and close
tags may cross content deltas and arbitrary UTF-8 network boundaries. Between
deltas, the adapter retains at most seven Unicode scalar values (28 UTF-8
bytes), independently of thought length. Empty inline thoughts emit both real
boundaries.

Truthy `reasoning_content` takes precedence over `reasoning`. Null, false,
zero, empty strings, and empty lists/objects fall back to the alternate field,
matching Python's JSON-value truthiness. A selected truthy non-string is a
protocol error instead of being silently discarded or converted to text.
Structured reasoning is emitted before the same record's content. A later
nonempty content delta without structured reasoning ends the structured
channel. Inline and structured channels have independent boundaries and may
overlap, as in the source. A visible delta in a record containing structured
reasoning does not synthesize an extra thought-end boundary. This adapter
accepts reasoning text strings; arbitrary truthy non-string Python extension
payloads are not a supported thought-text contract.

`ResponseProgress` marks the first choice-bearing record, including role-only
records and buffered tag prefixes. Pipeline TTFB is emitted before any thought
boundary even for empty thoughts. Usage-only records do not mark first output.
`ThoughtStart` and `ThoughtEnd` are explicit stream boundaries; consumers should
not infer replacements from visible-text events. The processor forwards their
order exactly. Thought text carries `skip_tts = true` and
`append_to_context = false`; ordinary output uses the existing LLM text path.

A valid `[DONE]` flushes an incomplete opening prefix as visible text, or flushes
an unterminated thought's held tail as thought text, then closes active thought
channels. Failure, timeout, and cancellation discard held tails and close active
channels for a retained direct stream consumer. Failure cleanup tracks boundaries
actually delivered to the bounded channel, including cancellation halfway through
a decoded record. Explicit terminal validation is
unchanged: a clean TCP EOF without `[DONE]` is an error, unlike normal exhaustion
of the source Python iterator. Dropping a direct stream or interrupting/cancelling
a pipeline aborts network work; aborted pipeline tasks do not emit stale final
frames. Each request has independent reasoning state.

## Shared transport and remaining scope

The implementation reuses native SSE, request validation, bounded queues,
backpressure, timeout/cancellation, tool assembly, usage accounting, and pipeline
processing. A response error still prevents tool execution/admission. All wire
records continue through shared metadata/tool processing even when their
content is entirely hidden. Native tool deltas and visible content can both be
processed from a single record; the source inherited loop instead prioritizes
tool calls. Inherited audio transcript fallback uses visible content after the
reasoning filter: hidden-only thoughts and held opening prefixes do not suppress
the same choice's transcript. Transcripts bypass the reasoning parser; this wire
behavior does not establish NVIDIA audio-model support. See the shared
[audio transcript scope](README.md#client-api).
Existing generic limitations remain, including server-reported model
updates, multi-candidate responses, legacy
`function_call`, and provider SDK/session features.

The source baseline is Pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`:
`src/pipecat/services/nvidia/llm.py` (reasoning state, wrapper, finalization),
`src/pipecat/services/openai/base_llm.py` (first-choice TTFB and inherited loop),
`tests/test_nvidia_llm.py` (metric-before-thought regression), and
`src/pipecat/frames/frames.py` (thought visibility flags).
Tests use static source-derived expectations, synthetic credentials, and actual
localhost HTTP/SSE. They neither execute Python nor contact a live NVIDIA
service, and do not establish hosted account/model acceptance or real NIM
compatibility beyond the measured wire contract.
