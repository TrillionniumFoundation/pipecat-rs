# Native OpenAI Responses

`pipecat-responses` implements the Responses wire protocol with native Rust
HTTP/SSE and persistent WebSocket clients. It does not route requests through
Chat Completions, Realtime, Python, or an SDK subprocess.

## Use

```rust,no_run
use pipecat_responses::{InputItem, ResponsesConfig, ResponsesContext,
    ResponsesHttpClient, ResponsesRequest, ResponseEvent};
use pipecat_services::Role;

# async fn example() -> Result<(), pipecat_responses::ServiceError> {
let client = ResponsesHttpClient::new(ResponsesConfig::new("caller-supplied-key"))?;
let request = ResponsesRequest::new("gpt-4.1", ResponsesContext::new(vec![
    InputItem::text(Role::User, "Hello"),
]));
let mut stream = client.stream(request)?;
while let Some(event) = stream.recv().await {
    if let ResponseEvent::TextDelta(text) = event? {
        print!("{text}");
    }
}
# Ok(()) }
```

`ResponsesWebSocketClient` exposes the same streaming interface. Clones share
one serialized connection; create separate clients for independent conversations.
`ResponsesProcessor::http` and `::websocket` provide pipeline processors and the
shared `LlmInference` capability. Out-of-band inference always uses HTTP with
`stream: false`, an explicit supplied context, and immutable settings overrides.
The processor supports ordinary/detailed context and message update frames,
tool/tool-choice updates, skip-TTS, metadata, health, ordered admission, flow
fences, thought frames, token usage and interruptible background inference.

## Context and settings

- System messages map to developer messages. A configured instruction uses
  `instructions`, or an initial developer message when input is empty
- Messages, function calls/results, images, base64 file data and provider-fetched
  HTTP(S) file URLs use Responses input shapes. Files are never read from disk
- Model refusals are visible text results, including final-only output. Delta,
  done and completed-response forms are deduplicated; ordinary refusals do not
  mark the service unusable. Aggregated assistant text preserves refusal history
- Completed function calls require valid object arguments. Delta events never
  authorize tool execution. This crate never invokes returned tools
- Reasoning summaries emit thought events. Opaque encrypted reasoning emits a
  separate `ReasoningItem`, whose `to_core_message()` representation round-trips
  through `ResponsesContext::from_core`. Pipeline output appends that item in
  order for an external context aggregator
- Responses settings use the common sparse `LlmSettings` plus
  `max_completion_tokens`, `reasoning` and `service_tier`. Omission preserves a
  stored value; null clears it. Runtime mapping frames recognize these fields
- Model, sampling, instructions, schema and tools are serialized from an immutable
  request snapshot. Invalid updates do not change committed processor state
- Reasoning defaults follow the pinned source's model classification. Explicit
  reasoning on a known non-reasoning model fails validation. Unknown effort,
  summary and mode strings are allowed for provider forward compatibility
- `response_schema` uses `text.format` with `type: json_schema` and `strict: true`
- Only `parallel_tool_calls` and `truncation` are accepted as arbitrary common
  extras. Request ownership, `store`, `stream`, model, tools, input and
  continuation cannot be overridden by extras

## Ownership, retries and bounds

Every request uses `store: false`. HTTP never supplies `previous_response_id`.
WebSocket continuation requires unchanged effective options and full input
prefix, followed by matching prior response output. Modified history, tools,
settings, missing output, disconnects and cancellations invalidate continuation.
Function and reasoning comparisons are deliberately stricter than the source's
ID-only matching, so edited history always falls back to a complete request.

Only a structured nested `error.code` of `previous_response_not_found` (when a
previous ID was actually sent) or `websocket_connection_limit_reached` can retry.
That error must arrive before `response.created` or output. The retry happens
at most once with the same immutable full request. Connection-limit rejection
opens a new socket. HTTP failures, timeouts, ambiguous sends/disconnects and
errors after response creation never replay work automatically. These stricter
rules replace the source's optional timeout/reconnection replay.

Dropping a stream signals cancellation; `close().await` also joins its owned
work. Pipeline cleanup closes the WebSocket session even when application code
retains the processor; stopping for later resume follows the core lifecycle.
HTTP cancellation drops the in-flight request. WebSocket cancellation sends
`response.cancel`, waiting for `response.created` if needed, then drains only the
known response through a matching terminal ID. Any ambiguous event, timeout or
read failure discards the connection. Remote cancellation is best effort and is
not a promise that generation or external hosted effects were undone.

Responses deltas do not reliably include response IDs. Serialization and terminal
boundaries provide ownership; malicious or protocol-violating events injected
after an otherwise valid terminal boundary cannot always be correlated. Known
reused IDs are rejected. A connection rotates at the configured age or 1,024
observed response IDs, so identity history is bounded and never silently forgotten
while keeping the same socket.

Channels, request size, message/event size, cumulative response size, connect,
application-idle, total-request and drain time are bounded. Control/ping traffic
does not extend idle deadlines. HTTPS/WSS is required outside loopback fixtures;
redirects are not followed. Config Debug, errors, provider messages, endpoints and
headers do not expose credentials. Provider error details are deliberately redacted.
`for_logging()` returns a copy with encoded media and encrypted reasoning removed.

## Explicit unsupported features

Input audio, typed refusal history, hosted tools/output (web search, computer
use, image generation, etc.), arbitrary raw provider items, background responses,
server conversation storage, externally managed continuation IDs, automatic
ambiguous retries, speculative context, runtime prompt-cache policy and context
transform frames return explicit unsupported errors. Custom tool schemas remain
unsupported. Standalone client streams do not execute application handlers. Ordinary Direct
handlers can use `pipecat-pipeline` with its `responses` feature and
`DirectToolBinding::new_responses`; the facade's `responses` feature enables both.
This typed binding owns selection, execution, cancellation, terminal receipts and
cleanup through the existing native executor. Provider preflight runs before
handler changes for detailed contexts, legacy contexts and tool setters; the
processor's standalone invocation remains inference only. There is no live
provider validation, credential discovery, file downloading or claim of
whole-provider parity.

## Validation provenance

The behavioral contracts are derived from pinned source commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- `src/pipecat/services/openai/responses/llm.py`
- `src/pipecat/adapters/services/open_ai_responses_adapter.py`
- `tests/test_openai_responses_http.py`
- `tests/test_openai_responses_websocket.py`
- `tests/test_openai_responses_logging.py`

Refusal output supplements the source's omitted refusal-event handling using the
[OpenAI Responses streaming event reference](https://developers.openai.com/api/reference/resources/responses/streaming-events)
and [output schema](https://developers.openai.com/api/reference/typescript/resources/responses).

Rust tests use actual loopback HTTP/TCP and WebSocket fixtures with synthetic
credentials. They do not call OpenAI. Run from the repository root after sourcing
the workspace toolchain environment:

```sh
cargo test --manifest-path rust/Cargo.toml --locked --offline -p pipecat-responses -j2
cargo clippy --manifest-path rust/Cargo.toml --locked --offline -p pipecat-responses --all-targets -j2 -- -D warnings
```
