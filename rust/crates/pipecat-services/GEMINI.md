# Native Gemini signed continuation

The HTTP/SSE decoder preserves opaque `thoughtSignature` bytes on exact text
Parts and ID-bound function-call Parts. Empty trailing text Parts, repeated text,
thought flags and native unsigned parallel-call provenance remain distinct.
Completed signed prefixes can survive interruption; checkpoints never authorize
function execution. Tool calls are admitted only after a clean completed stream.
Tool responses require Gemini's literal `STOP` finish reason and clean SSE EOF.
Length, safety, recitation, malformed-call and unknown finishes cannot authorize
tools; missing finish markers or partial SSE tails fail closed. Duplicate markers
and other providers' stop aliases cannot upgrade failure into successful history.
This is stricter than the pinned Python service, which runs collected calls after
its stream loop. Valid signed checkpoints can remain after failure, while their
tool references never become calls or completed tool history.

Native context uses Gemini-specific typed Parts rather than Anthropic thinking
blocks. The process-local carrier is immutable, bounded and nonserializable;
its Debug output hides text, arguments and signatures. Rich context snapshots
preserve the typed Parts, while the Python-compatible bus codec rejects them.
OpenAI, Anthropic and Responses adapters reject Gemini-specific history rather
than disclose or reinterpret provider-private content.

Public text in conversation history remains receipt-backed. A signed Part that
was only partly delivered retains the heard text with an explicit incomplete
marker and without a signature. Gemini adaptation rejects that marker before
HTTP, including when an empty trailing signature follows unheard text. Deciding
how to remove or replace incomplete provider history requires an explicit
application transformation; no signature is attached to altered text.

Signed parallel calls and matching results are grouped by exact call IDs. A new
signed call starts a distinct sequential step. Foreign entirely unsigned tool
history receives Google's documented REST `skip_thought_signature_validator`
sentinel. Native metadata, even with no signature, prevents that fallback.
Gemini 3 native tool groups without a signed first-call anchor fail closed.
Unsigned native Gemini 2.5 groups remain unsigned because signatures are optional.

## Contracts and deliberate differences

Pinned reference: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/services/google/llm.py`,
`src/pipecat/adapters/services/gemini_adapter.py`, and
`tests/test_get_llm_invocation_params.py`.

Source SHA-256 values, verified against that Git revision without executing Python:
- `google/llm.py`: `c23a4915f5f0a9f63aac31b15c4047f57c3fdd06520c8b2cf322a638f509769d`
- `gemini_adapter.py`: `6716fc0eb0188c92b40cbadb271d16f33832fe342b95a589fc9aeb49f68b6dc1`
- `test_get_llm_invocation_params.py`: `afd10be7defd055c4abd55e9d0ff40197887bef2b9d4e83b1c47780b0e9a7ef5`

The source adapter finds text bookmarks using normalized whitespace and prefix
matching, including interrupted text. Native replay instead binds original Parts
by position and tool bookmarks by ID. It fails closed on truncated signed Parts
because Google's documented contract requires exact signed-Part replay. The
source SDK accepts the migration sentinel as bytes; the native REST adapter uses
the documented literal sentinel, not a fabricated cryptographic signature.

Primary protocol references:
- https://ai.google.dev/gemini-api/docs/generate-content/thought-signatures
- https://docs.cloud.google.com/gemini-enterprise-agent-platform/models/thinking/thought-signatures

Tests exercise native HTTP/SSE and real ToolExecutor execution against loopback
fixtures. They do not establish live Gemini/Vertex acceptance. Live provider
qualification, signed image Parts, code execution and Vertex authentication remain
separate gaps. No Python runtime or test execution is used by these components.

## System-only startup conversion

At the pinned reference above, `gemini_adapter.py:417–433` appends a distinct
user text Content when an effective system instruction exists but the converted,
signed-tool-grouped history has no regular message. A regular message has exactly
one nonempty text Part and no function call or function response. Whitespace is
nonempty; text plus media or multiple text Parts do not qualify. This decision
precedes generic adjacent-role merging and trailing-user enforcement, including
for the pinned model prefixes that permit assistant prefill.

The startup seed prefers the extracted initial context system text over service
settings, while the final `systemInstruction` prefers service settings over the
extracted context system text. A sole context system message is instead retained
as a user turn and does not receive a duplicate seed when it has ordinary text.
The seed exists only in the request body; repeated conversions do not change or
accumulate messages in the universal Context.

Gemini no longer fabricates `"(empty)"` text for absent content or an empty Parts
list: empty converted Contents are removed after the seed decision, matching the
pinned source. With an effective system instruction they cannot suppress the
seed; without one they do not invent a user turn. An explicit empty text Part
remains distinct from an empty Parts list. OpenAI and Anthropic normalization,
including Anthropic cache placement, is unchanged. Adapter fixtures and a
synthetic HTTP streaming regression cover this behavior; no live model acceptance
or behavior is claimed.
