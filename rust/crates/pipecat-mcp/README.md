# pipecat-mcp

Native MCP tools for Pipecat's Rust contexts, processors, and agent workers. Uses
the official [`rmcp` 3.3.0 SDK](https://docs.rs/rmcp/3.3.0/rmcp/) for negotiation,
JSON-RPC correlation, sessions, and Streamable HTTP semantics. There is no Python
or Node runtime, shell adapter, credential discovery, or external MCP test server.

## Pipeline tool executor

```rust,no_run
use pipecat_core::{Pipeline, PipelineConfig};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, ServerConfig};
use std::sync::Arc;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let client = McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
    HttpConfig::new("http://127.0.0.1:8000/mcp"),
)))?;
let tools = client.tools().await?;
let definitions = tools.definitions(); // Native pipecat_core::ToolDefinition
let executor = tools.executor()?;
let worker = Pipeline::new(vec![Arc::new(executor)])
    .start(PipelineConfig::default()).await?;
// FunctionCallsStarted frames invoke the selected tools. Discovery itself does not.
worker.end().await?;
let (_, report) = worker.collect().await?;
assert!(report.errors.is_empty()); // The executor closes its MCP connection.
# let _ = definitions;
# Ok(())
# }
```

`tools.schema()` returns a native `ToolsSchema` whose `StandardTool::Direct`
entries carry `LocalToolFunction` handlers. Add `tools.lifecycle()` to the owning
pipeline when using these handlers. Native core's schemas do not have Python's
`_pipecat_cleanup` callback attribute; retaining only schemas cannot arrange an
awaited pipeline teardown automatically. Dropping every client, tool snapshot,
and handler signals asynchronous SDK cleanup, but explicit teardown is preferred.

## Agent-worker registry

```rust,no_run
use pipecat_agent_workers::{LlmWorker, LlmWorkerOptions, ToolRegistry};
use pipecat_mcp::McpClient;
use pipecat_services::{CompletionRequest, LlmClient};
use std::sync::Arc;

# async fn example(mcp: McpClient, llm: LlmClient, request: CompletionRequest)
# -> Result<(), Box<dyn std::error::Error>> {
let tools = mcp.tools().await?;
let mut registry = ToolRegistry::new();
let lifecycle = tools.register_into(&mut registry)?;
let mut worker = LlmWorker::start_with_output(
    llm, request, registry, LlmWorkerOptions::default(),
    vec![Arc::new(lifecycle)],
).await?;
// Use the native worker's normal message, tool, interruption, and output APIs.
worker.handle().end().await?;
worker.join().await?;
# Ok(())
# }
```

Registration checks all definitions in a cloned registry and publishes them
atomically. A duplicate does not partially modify the original registry. The
native worker supplies tool call IDs, result frames, interruption behavior, and
model reruns; MCP calls go through the same bounded client as direct calls.

## Native stdio configuration

`StdioConfig::new` takes an absolute path to a caller-selected native executable.
Set `arguments`, `environment`, and an optional absolute `current_directory`
explicitly. No shell, `PATH` search, inherited environment, environment-file read,
or package-runner download occurs. Configuration construction does not execute
anything; only `start()` or `tools()` opens a connection or spawns the executable.
The application remains responsible for authorizing and trusting that executable.

The client owns the direct child with Tokio `kill_on_drop`, bounded JSON lines,
null stderr, bounded writes, and kill/wait cleanup. Tests launch only the compiled
`pipecat-mcp-fixture` binary, enabled only by the `native-fixtures` test feature.
Normal builds do not produce or install that executable. No process-group or arbitrary descendant management
is promised; native MCP executables must own their own descendants.

## Session and cancellation contract

- `start()` is idempotent and waits for the MCP initialize/initialized handshake.
  `tools()` starts lazily and discovers all bounded pages. Only servers advertising
  tools and negotiating a known classic protocol version are accepted. Lifecycle
  transitions are serialized across tasks
- `close()` is idempotent, cancels outstanding requests, waits for SDK cleanup,
  and leaves the client closed. Tool calls never reopen an explicitly closed or
  never-started client; only `start()`/`tools()` can do that
- Transport errors invalidate only the exact session generation used by the
  failed request. Another caller's replacement session is never closed by an old
  failure. The next separate invocation may reconnect; the failed invocation is
  not replayed
- There is at most one explicit `tools/call` dispatch per invocation, not a claim
  of end-to-end exactly-once side effects. A timeout, cancellation, disconnect, or
  server error may arrive after remote execution. Callers must not blindly retry
- SDK automatic 404 reinitialization/replay, SSE resumption retries, and reqwest
  automatic retries are disabled. The classic initialize lifecycle with protocol
  `2025-11-25` is requested. Discovery-mode negotiation and automatic MRTR rounds
  are not used
- Explicit cancellation and dropping a call future send a bounded
  `notifications/cancelled` message when possible. Local request tasks retain
  their capacity permit until cleanup completes. Cancellation is advisory; it
  cannot undo remote side effects
- Dropping a pending initializer releases its transport. A failed initializer can
  be retried explicitly. Awaited close errors are redacted `McpError::Cleanup`
- Dropping a close future retains its cleanup task. The next close or explicit
  reopen awaits that task and observes any cleanup error before proceeding
- SDK shutdown logs some transport failures instead of returning them. An
  independent cleanup outcome records failed or interrupted HTTP DELETE/native
  child cleanup, so successful task shutdown is not misreported as successful
  transport cleanup. If the close deadline expires, background SDK cleanup can
  briefly continue; this is reported as a cleanup error, never as completion

## Schemas and results

`tools_filter` is an allowlist; an absent or empty set exposes all tools, matching
the source. Calls are restricted to successfully discovered, allowed names.
Fixed `tools_arguments` override model-supplied values, including when arguments
are null, and their top-level properties/required names are hidden from advertised
schemas. Fixed values are never embedded into schemas. Output filters receive the
concatenated text, with bounded output and contained callback errors/panics.

Conversion preserves JSON Schema metadata, nested definitions/references, and
constraints rather than flattening them. Simple same-object combinator branches
also hide fixed properties. This is syntactic projection, not a general JSON Schema
partial evaluator: schemas whose fixed names are defined only via root `$ref`,
pattern properties, dependent schemas, or conditional logic may need an explicitly
adapted server schema. Argument JSON is shape/size checked, not validated against
the full advertised JSON Schema; the server remains responsible for validation.
Discovery failures are atomic and fail closed instead of silently skipping malformed
tools. Call `tools()` explicitly to refresh schemas; list-change notifications do
not trigger unsolicited discovery.

`call()` returns `ToolResponse` with joined text, original typed content blocks,
`structured_content`, and `is_error`. Image/audio/resource-link blocks are
preserved without fetching linked resources or passing them off as text.
`call_text()` and native registry handlers match the source's model-facing text
behavior: empty/non-text results use its fallback string; transport/server failures
become bounded class/code descriptions. On a failure, model-facing output filters
receive empty text and may supply a custom fallback. `NotConnected` remains a wiring error.
Local request cancellation propagates as `McpError::Cancelled` without invoking
output filters. Agent-worker handlers preserve it as a cancellation frame with
`run_llm: false`, rather than a successful result or a model rerun. Generic pipeline
executor callbacks currently settle that error as a callback failure; explicit
pipeline interruption/cancel commands still produce native cancellation receipts.
Server-provided error messages/data are deliberately not copied into errors.

## Bounds and privacy

Default limits: 8 in-flight requests, 1 MiB per JSON message/raw SSE event,
64 KiB merged arguments, 256 KiB tool result, 1 MiB total schemas, 128 discovered
tools, and 32 pages. Deadlines are 10 seconds to connect, 30 seconds per request,
and 3 seconds for close. All are explicit configuration. Schema/tool limits include
filtered-out entries. JSON and SSE are bounded before deserialization; SSE limits
apply across fragmented lines and empty-data accumulation. An HTTP stack can hold
its own network chunk before the application applies its byte limit.

HTTPS uses rustls and bundled public certificate roots with verification enabled.
Plaintext HTTP is limited to loopback. Redirects and environment proxies are
disabled. URL userinfo/fragments and overridden MCP/framing headers are rejected.
Explicit caller-provided auth headers are sent only to the configured endpoint.
No OAuth, refresh, persistent grant, credential store, root discovery, resource
read, sampling, elicitation, or tool auto-execution is enabled.

Public configuration/client/result `Debug` and errors redact URLs, headers,
arguments, environment values, content, and remote error details. Successful tool
text is intentionally returned to its authorized caller. The upstream SDK emits
protocol contents at debug/trace, server metadata/notifications at info, and some
error details at warn;
production applications handling private data must filter the `rmcp` tracing
target (`rmcp=off`). Application-provided filters and application logging remain
the application's responsibility. This crate does not install a global logger.

In the pinned SDK source, relevant sites are `src/service.rs:1345` (peer info),
`:1443` (full event), `:1540` (request), `:1586` (result), `:1590` (error),
`:1602` (notification), and `:1707` (id-less error).
`src/transport/streamable_http_client.rs:1788-1810` logs session IDs during cleanup.
The SDK's `tracing` dependency is unconditional and it has no feature that disables
only MCP protocol logs. A direct `tracing/max_level_off` dependency feature would
disable tracing for the unified workspace dependency, including unrelated crates;
this library deliberately does not impose that application-wide setting.
`rmcp=off` is guidance for the application's subscriber, not an enforced default.

## Source coverage and deliberate gaps

Reference: `src/pipecat/services/mcp_service.py` and `tests/test_mcp_service.py` at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

Implemented: owned session initialization/readiness, tools discovery, native schema
and executable handler conversion, name filtering, fixed arguments, text output
filters, typed and model-facing results/errors, task-independent close/reopen,
generation-safe dropped-session recovery, interruption/cancellation, bounded
requests, and native pipeline/agent registry integration.

Not implemented: deprecated standalone HTTP+SSE (`SseServerParameters`), Python
SDK session injection/types/context managers, deprecated Python registration method
aliases, and implicit `LLMSwitcher` handler propagation. The official SDK has no
legacy standalone SSE transport; Streamable HTTP SSE responses are supported and
tested. The source has no tool alias feature. Unknown configured fixed-argument
tool names are unused; Python's warning log for them is not reproduced. String
filters replace Python's dynamically typed callback contract. HTTP redirects,
inherited stdio environment, and unbounded/error-detail logging are intentionally
stricter. No live remote service or credentials have been qualified.

## Validation

From `rust/`, using the workspace toolchain:

```sh
source /workspace/shared/pipecat-tools/env.sh
cargo test -p pipecat-mcp --features native-fixtures -j 2 --offline
cargo clippy -p pipecat-mcp --all-targets --features native-fixtures -j 2 --offline -- -D warnings
cargo fmt -p pipecat-mcp -- --check
```

Fixtures cover real loopback HTTP JSON, Streamable HTTP SSE, stateful/stateless
sessions, the native LLM-worker tool round, native stdio child ownership/reaping,
side-effect-before404/disconnect no-replay behavior, teardown/reopen races,
request/initializer cancellation, future-drop cancellation, pagination, schema
errors, argument/output/wire limits, header/redirect boundaries, redaction, and
pipeline cleanup. Tests do not contact public MCP services or use credentials.

Official SDK sources: [repository](https://github.com/modelcontextprotocol/rust-sdk),
[versioned API](https://docs.rs/rmcp/3.3.0/rmcp/),
[registry release](https://crates.io/crates/rmcp/3.3.0).
