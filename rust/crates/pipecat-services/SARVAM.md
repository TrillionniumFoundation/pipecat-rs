# Sarvam Chat Completions

`ClientConfig::openai_compatible(OpenAiPresetConfig::new(OpenAiPreset::Sarvam), key)`
selects native Sarvam request semantics over the existing bounded OpenAI HTTP/SSE
client. No Python process, SDK, decoder replacement or automatic tool execution is
involved. `preset.request(context)` defaults to `sarvam-105b`.

## Source contract

The static reference is Python revision
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, particularly
`src/pipecat/services/sarvam/llm.py` (model sets, constructor, headers, builder and
validation), `sarvam/_sdk.py` (identification), and
`openai/base_llm.py` (builder and extra-body merge). The source remains a readable
reference only; these native tests do not execute or import it.

| Model | Default route | Images | Reasoning effort | Wiki grounding |
| --- | --- | --- | --- | --- |
| deepseekv4-flash | v2 | no | low / medium / high | no |
| gemma4 | v2 | yes | low / medium / high | yes |
| glm5.2 | v2 | no | low / medium / high | no |
| sarvam-105b | v2 | no | low / medium / high | yes |
| sarvam-105b-conversations | v1 | no | no | no |

All models accept native function declarations and validated tool history. Any
configured tool choice, including `none` and `auto`, requires nonempty tools.
Developer roles become user roles without mutating the supplied context. The
image check operates on adapted messages and therefore includes image URL,
detailed image URL and inline-image inputs. This table pins source behavior,
not current model availability or live-provider compatibility.

## Routing and headers

The default native endpoint is
`https://api.sarvam.ai/{version}/chat/completions`. Only a single `{version}` path
segment immediately before the final `/chat/completions` is supported; malformed,
repeated, authority/query/fragment and other placeholders are rejected before
I/O. The same template works on HTTP loopback for protocol tests. Assigning an
exact `ClientConfig.endpoint` without a placeholder preserves that URL and its
path/query when the model changes. Shared HTTPS, credentials-in-URL and redirect
rules still apply.

The model is resolved from the immutable request snapshot, including
`request.settings.base.model`. Each request and prospective runtime update
validates the model and capabilities and resolves the route together. This is
an intentional native difference: Python validates the initial model and chooses
a default base URL at construction, then retains that route across settings
updates. Rust does not preserve the source's runtime unknown-model bypass.

The supplied nonempty API key sets both sensitive `Authorization: Bearer ...`
and `api-subscription-key` headers. `ClientConfig.sarvam_default_headers` accepts
additional headers only for the Sarvam preset. Required bearer/subscription and
User-Agent headers override caller values case-insensitively. The User-Agent is
`Pipecat/<native crate version> Rust/native`, rather than claiming a Python SDK
or interpreter version. Additional case-insensitive duplicate headers and
transport framing/Accept/Content-Type headers are rejected. These transport and
bearer protections are stricter than Python's free-form header map. Header and
credential errors occur before I/O and do not include their values in errors or
configuration Debug. A custom HTTP client retains the shared caller-owned
proxy/redirect/TLS responsibility.

## Options, precedence and runtime settings

`OpenAiPresetSettings` offers typed `wiki_grounding` and `reasoning_effort` defaults.
For Sarvam, the runtime-updatable counterparts live at
`LlmSettings.base.extra["wiki_grounding"]` and
`LlmSettings.base.extra["reasoning_effort"]`. Mapping-form settings are accepted
through the existing settings parser. A supplied runtime value overrides the
typed default; JSON null clears it. Absent values retain defaults. Boolean false
is configured wiki grounding and is rejected on models without that capability.

The final precedence for these two controls is nested `extra_body`, then explicit
runtime controls, then typed preset defaults, then raw `CompletionRequest.extra`
controls. This represents runtime typed options through the native common
settings type without adding fields to core. Nested extras are flattened into
the final body and validated there; null reasoning/wiki options are omitted.
The source likewise lets typed reasoning replace top-level extras and lets
explicit nested extras replace provider options, but its preflight checks do
not inspect all extra-body overrides. Native resolved validation intentionally
prevents those capability/type bypasses. Atomic rejected updates leave the last
valid state and already admitted request snapshots unchanged.

`stream_options`, `max_completion_tokens`, and `service_tier` are removed.
The source-compatible budget is `request.settings.max_tokens`, emitted as
`max_tokens`. The portable convenience `request.max_tokens` maps to OpenAI
`max_completion_tokens` and is therefore removed, never remapped. A given sparse
source budget supersedes that portable budget. Native direct inference uses the
same rule for `InferenceOptions.settings.max_tokens`.

Final nested extras cannot reintroduce removed token/tier fields. Attempts to
provide reserved identity/context/streaming fields, including `model`, `stream`,
`messages`, `tools`, `tool_choice`, `stream_options` and `n`, are rejected by the
existing native reserved-field rules instead of overriding them. This is stricter
than Python SDK flattening. Generic native sampling constraints remain; final
Sarvam `max_tokens` is additionally checked after flattening. Other ordinary
OpenAI sampling fields retain their native serialization, including nulls.

## Response and lifecycle scope

Ordinary OpenAI SSE supports fragmented UTF-8 text, indexed JSON tool fragments,
usage snapshots, terminal validation and bounded cancellation/timeouts. No
NVIDIA leading-think-tag filter is selected: literal `<think>` text stays text.
Generic OpenAI structured thought deltas remain a native extension; this does
not claim Python Sarvam thought-decoder parity. Tools are emitted only as
validated intentions and are not executed here.

`LlmProcessor` retains the existing atomic state update, nonfatal upstream error,
interruption/cancel, immutable per-request snapshot and restoration behavior.
Direct `LlmInference` consumes bounded SSE and aggregates text; Python's true
nonstreaming direct-inference transport is outside this adapter. Failed partial
inference is an error, and tool-only inference returns None. There are no new
SDK/session/realtime, audio-generation or third-party network capabilities.

`cargo test -p pipecat-services --locked --offline --no-fail-fast` exercises
synthetic keys and actual localhost HTTP/SSE. It verifies source-derived
capability matrices, headers, default and fixed routes, runtime updates,
preflight zero-I/O rejection, tools/usage, cancellation and protocol failure.
This qualification does not contact Sarvam or establish live-service acceptance.
