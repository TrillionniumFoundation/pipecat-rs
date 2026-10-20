# Explicit Anthropic prompt-cache policy

`AnthropicPromptCachePolicy` is an opt-in native implementation of the fixed
Python Anthropic adapter's explicit cache breakpoints, not a cache-hit guarantee.
The cache policy does not change ordinary unmarked request behavior. No SDK or live-provider call is required to
prepare a request.

```rust
use pipecat_services::{
    AnthropicCacheTtl, AnthropicPromptCachePolicy, CompletionRequest, Context,
};
let request = CompletionRequest::new("your-anthropic-model", Context::default())
    .with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
        enabled: true,
        system_ttl: Some(AnthropicCacheTtl::FiveMinutes),
    });
```

The policy is validated with the actual client/provider when building a request.
Only Anthropic Messages accepts it. An explicit policy, including `enabled=false`,
is rejected for OpenAI/Gemini rather than silently ignored.

## Source and wire contract

Baseline: ProfHepta/pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
`anthropic_adapter.py` blob `b9d154dea65cb88b332eae706192df728e853227` and
`anthropic/llm.py` blob `fc776b26925758ccd2c901acf80b17900bcd690c` define the policy.

After ordinary typed context conversion, consecutive-role merging and the
model-aware trailing-user normalization described below, enabled policy marks the last content block of the **two most recent user-role wire
messages** with `cache_control: {type: "ephemeral"}`. Tool-result messages become
user-role content and participate in this selection. Earlier user messages and
all assistant content, including signed thinking and tool calls, stay unchanged.
Tools schemas are not marked. The selected system instruction becomes a single
text block with its own ephemeral marker; absent system remains absent.

`system_ttl=None` omits the TTL field. `FiveMinutes` and `OneHour` serialize as
`5m` and `1h`. Only the system marker receives that TTL; message markers omit it.
An enabled request has at most three generated markers. The provider determines
minimum cacheable prefix length, eligibility and reuse. A one-hour cache write
can cost more than a default-lifetime write and is never selected implicitly.
Disabling the policy returns to ordinary string-system/unmarked-message output.

Existing native conversion conventions remain intact: content strings are typed
text-block arrays, empty user content becomes `(empty)`, and empty explicit
system strings follow existing `split_system` fallback rules. This is not a port
of every source SDK passthrough.

## Model-aware trailing-user normalization

The same pinned `anthropic/llm.py:479–514` and
`anthropic_adapter.py:254–269` define a frozen, case-sensitive prefix test.
Models starting with `claude-2`, `claude-instant`, `claude-3`,
`claude-opus-4-0`, `claude-opus-4-1`, `claude-sonnet-4-0`,
`claude-sonnet-4-5`, or `claude-haiku-4-5` keep assistant-ending histories.
All other model identifiers append exactly a user text block containing `.`
when the converted, merged history ends in assistant. Empty histories and
user/tool-result endings are unchanged. This applies independently of caching,
only to fresh wire data; repeated serialization never mutates stored context.

When caching is enabled, that injected turn becomes the newest user breakpoint,
displacing the formerly second-newest user breakpoint. System TTL is unchanged.
These are pinned Python runtime/body semantics, not claims about current model
availability or live-provider acceptance. OpenAI and Gemini are unaffected.

## Configuration and runtime updates

The typed builder writes the canonical local settings keys
`enable_prompt_caching` (boolean) and `system_prompt_cache_ttl` (null, `5m`, `1h`).
Those keys can also be supplied through existing `LlmSettings.base.extra` or
mapping-based `LlmUpdateSettings`/`ServiceUpdateSettings` deltas. They are consumed
locally after sparse settings resolution and never emitted as unknown provider
fields. Settings keys override raw `CompletionRequest.extra` keys. The builder
writes both keys, including null for an omitted TTL, so old raw extras cannot
override the typed choice.

An omitted delta retains the stored value. Null TTL explicitly clears it to the
provider default; null enable is invalid. A disable delta keeps the TTL, allowing
a later enable to reuse it. Invalid TTL or types are rejected even while disabled.
The entire prospective request validates before a settings update commits.

`LlmEnablePromptCaching { enable }` follows the source's consuming control path:
it updates only enable, preserves TTL, forwards no control and launches no
inference, including when received upstream. It uses the same atomic settings
path. Rejected controls are non-permanent proposal errors and do not mark service
health unusable. Active and queued requests keep immutable admission snapshots;
per-call inference overrides affect only that call. Policy updates do not create
new workers, retries, timers or replay of cancelled requests. Existing processor
session/cleanup semantics are unchanged. Completed sequential ordinary sessions
retain the stored policy until another update; this is not a new guarantee for
speculative-session reuse.

Top-level automatic `cache_control` in generic extras conflicts with any explicit
policy, even disabled policy, and is rejected. Without explicit policy keys the
existing generic-extra behavior is unchanged; that escape hatch is not newly
qualified automatic caching support. Reserved system/messages overrides remain
rejected. This policy does not manufacture signed history, tool authority or
physical-playback receipts.

## Native validation and qualification

The original source helper trusts TTL/control annotations and can return partial
markers after malformed passthrough input. Native policy instead validates types
and provider, relies on typed context validation, and fails the request rather
than returning a partial marked body. Errors do not include policy values or
prompt content. Building a request does not mutate the caller's context.

`tests/fixtures/anthropic_cache_source.json` preserves 31 original-method probes,
with complete source blob gates in the separate research oracle. Rust checks 12
supported message cases and three accepted TTL cases, and explicitly rejects the
source's invalid-TTL negative. Only string-content-to-text-block representation
is normalized for those comparisons. The remaining source probes cover dispatch,
invocation and malformed-source boundaries; their presence is not a native test
pass or execution of the full Python SDK.

Native tests additionally cover typed and mapping configuration, merged user
blocks, tool-result placement, signed assistant preservation, disabled defaults,
validation and settings atomicity. Real Pipeline plus bounded loopback HTTP/SSE
fixtures observe outgoing requests, active/queued snapshots, no-request controls,
recoverable errors, cancellation peer EOF and joined worker/server cleanup.
No live Anthropic cache hit, price, token savings, account/model eligibility,
Bedrock/Vertex implementation, browser or physical audio is established.

## Single-media ordering before caching

At pinned Python `anthropic_adapter.py:475–489`, an ordinary message containing
exactly one image/document and at least one text block moves that media block
immediately before its first text block. Native conversion now applies this
stable move before same-role merging, trailing-user normalization and cache
placement. The last text block, not the moved media, therefore receives a user
cache marker. Multiple-media, media-only and text-only messages retain order;
content never moves across original message boundaries and stored context is
unchanged. URL/base64 payloads and PDF titles remain intact.

The source tool-result and tool-call branches return before normalization
(`:362–392`), so nested tool content and existing native tool-call content policy
are unchanged. Source provider-specific history bypasses standard conversion
(`:272–318`). Native signed/redacted thinking identifies that history and keeps
its block order. Native context has no arbitrary raw provider-message envelope;
this does not claim broader provider-passthrough parity. Body fixtures, loopback
HTTP, and the real RTVI uploaded-PDF-to-context fixture cover ordering; no live
provider execution or quality improvement is asserted.
