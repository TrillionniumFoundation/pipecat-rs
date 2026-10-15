# Anthropic opaque redacted-thinking history

Anthropic can return `{"type":"redacted_thinking","data":"..."}` when thinking
has been encrypted by the provider. A normal thinking/tool exchange can therefore
contain this block. Previously the native streaming decoder rejected it, even
though ordinary signed thinking and tool continuations were supported.

The implementation retains the nonempty data string as `ContentPart::RedactedThinking`,
core `MessagePart::RedactedThinking`, and an Anthropic-only
`LlmContinuationBlock::RedactedThinking`. It does not decode, normalize, inspect,
or invent this opaque value. Structural validation is not cryptographic
validation. Replay preserves the parsed string, not the original JSON escaping,
whitespace or raw HTTP bytes.

## Ordered streaming and ownership

The data arrives in `content_block_start`. The block occupies its original index
alongside thinking, text and tools. A redacted block has no accepted text,
thinking, signature or tool-argument delta. Closing it makes it eligible for the
existing contiguous-prefix checkpoint, while `message_stop` publishes complete
ordered history only when every block is closed and indexes are contiguous.
Duplicate indexes, malformed data and oversized history fail closed.

No `ThoughtText`, `LlmText` or synthetic signature is emitted for opaque data.
The existing LLM processor carries this native history to the assistant
aggregator, which must consume it before the next provider request. Adjacent
assistant blocks are serialized in the original order. Only successfully
completed responses admit tool execution. Existing checkpoint ownership remains:
both interruption and cancellation can reconcile an already-admitted closed
prefix and delivered text. An unclosed redacted block or an unadmitted carrier
does not enter history merely because bytes arrived; neither cancellation path
authorizes pending tools. The native carrier's response identity and mirror reconciliation
remain authoritative.

## Bounds and exposure

Streaming history remains limited to 1024 indexed blocks and 8 MiB of retained
text/signature/data/tool content. The native continuation separately charges
owned string capacities and vector/block overhead within its existing 8 MiB
retained-memory budget, so it can reject before the stream's content-byte limit.
Context validation requires assistant role, nonempty data and the shared 8 MiB
provider-history byte limit. Existing frame, HTTP/SSE and context limits also
apply. No additional unbounded side storage is introduced.

The three typed Debug implementations show byte counts rather than opaque data;
`request_body_for_logging` masks the `data` field. Explicit serialization,
history and request APIs expose the data to permit replay. Explicit context
summarization retains serialized provider parts under its existing transcript
limit; its token estimate charges opaque data by byte length. It is not a
plaintext interpretation of encrypted thinking.

OpenAI Chat Completions, Gemini, Responses, Bedrock and the network-bus context
encoder do not reinterpret Anthropic opaque history as their own reasoning or
text. Their existing unsupported-provider policy rejects it. Bedrock's distinct
binary `BedrockRedacted` variant and encoding remain separate.

## Source and verification boundary

The fixed Python source at `af9f9da9d7afddaa8811c0e0251b32566c980bdf`
handles ordinary thinking and signature stream events but omits redacted content
blocks. Its Anthropic adapter can deep-copy caller-supplied native messages,
including such blocks; that does not recover blocks omitted by its stream.
This change is a provider-protocol compatibility repair, rather than parity with
an existing Python redacted-stream implementation.

Official protocol reference: https://platform.claude.com/docs/en/build-with-claude/thinking
The local tests use synthetic opaque strings and real loopback HTTP/SSE with
native tool execution and a subsequent request. They establish transport,
ordering, retention and rejection behavior; they do not decrypt provider data or
claim real-provider/model acceptance.
