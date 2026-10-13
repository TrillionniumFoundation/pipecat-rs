# pipecat-mcp

Native MCP tools for Pipecat's Rust contexts, processors, and agent workers. Uses
the official [`rmcp` 3.3.0 SDK](https://docs.rs/rmcp/3.3.0/rmcp/) for negotiation,
JSON-RPC correlation, sessions, and Streamable HTTP semantics. There is no Python
or Node runtime, shell adapter, credential discovery, or external MCP test server.
The native standalone SSE adapter uses the same SDK negotiation and correlation.

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
entries carry `LocalToolFunction` handlers with typed cancellation, interruption
cancellation enabled, and a guard deadline covering one connection, request and
close budget. Each handler carries the same cleanup callback for its client, including
handlers from later discoveries; distinct clients have distinct cleanup identities.

`tools.executor()` and all MCP native LLM factories reuse these cached handlers,
including their identities, typed outcomes and deadlines. Returned wrapper errors,
such as invoking a cached handler after explicit client close, produce one admitted
failed tool result and a bounded nonfatal `Application` diagnostic. Ordinary remote
failures remain successful model-facing error text; typed cancellation and executor
timeouts retain their existing outcome behavior. The outer MCP lifecycle still
owns cleanup, and the existing model-facing wrapper error text is preserved.

The ordinary `pipecat_pipeline::DirectToolBinding` consumes these executable
schemas, honors their options, and records the shared cleanup hook. It retains
cleanup after refresh or pruning and awaits MCP close at pipeline teardown. The
hook keeps the resource alive without an ownership cycle. Its deadline overrides
the general binding's default so a retained callback can reconnect after an
invalidated session, then let the MCP transport settle its own request timeout.
The failed call itself is never replayed. Lifecycle-lock contention remains
subject to the finite guard.
The MCP-specific bindings below also check discovered schema/handler identity.

Manual callback owners that do not record cleanup metadata still need
`tools.lifecycle()` or an explicit awaited close. Merely retaining a schema does
not run its cleanup. Dropping every client, tool snapshot, handler and cleanup
owner signals asynchronous SDK cleanup.

## Ordinary native LLM pipeline binding

`tools.llm_stages(llm)` binds this immutable MCP snapshot to a concrete native
`Arc<pipecat_services::LlmProcessor>`. Insert the returned stages together and in
their original order between the user and assistant aggregators:

```rust,no_run
# use pipecat_core::FrameProcessor;
# use pipecat_mcp::McpTools;
# use pipecat_services::LlmProcessor;
# use std::sync::Arc;
# fn bind(tools: McpTools, llm: Arc<LlmProcessor>, user: Arc<dyn FrameProcessor>, assistant: Arc<dyn FrameProcessor>) -> Result<(), pipecat_core::ProcessorError> {
let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![user];
processors.extend(tools.llm_stages(llm)?);
processors.push(assistant);
let pipeline = pipecat_core::Pipeline::new(processors);
# let _ = pipeline;
# Ok(())
# }
```

The two projection stages accept ordinary detailed contexts and `LlmSetTools` in
both directions. They project executable definitions only when their schema and
callback identity match the bound MCP snapshot; same-name foreign callbacks,
modified definitions, duplicates and excessive tool counts fail before inference.
`tools.schema()` and snapshot clones preserve callback identity. A new `tools()`
discovery creates a distinct snapshot; this fixed convenience needs a new binding.

The original native LLM performs provider inference, the existing MCP executor
owns tool execution and cancellation, and the assistant aggregator schedules the
upstream continuation with real results. Shared conversation schemas retain their
handlers; only the forwarded ordinary frame is projected. Pipeline teardown awaits
the executor's MCP cleanup. Ordinary pipelines without this explicit binding still
reject executable schemas rather than discarding their handlers.

This is a fixed MCP inventory, not general schema auto-registration or runtime
handler replacement. Ordinary `Function` definitions may pass through for
advertisement, but this executor executes only the bound MCP names; it does not
register arbitrary application functions. Realtime sealed and speculative
executable contexts are explicitly rejected when projection is needed: their capabilities are never
stripped or resealed. Fixed native LLM bindings preflight projected ordinary
contexts and tool setters with the actual LLM's current settings and client,
before changing handler authority. Supported owning OpenAI declarations and
empty Gemini Google Search declarations remain
advertisement-only alongside the bound standard callbacks; foreign declarations
follow native adapter selection. Client restrictions, reserved fields and grouped
schemas retain their existing validation. Switcher bindings
keep provider-agnostic strict validation. These checks do not establish live-provider qualification.

For explicit runtime refresh, retain the handle returned by `tools.bind_llm(llm)`:

```rust,no_run
# use pipecat_mcp::{McpClient, McpTools};
# use pipecat_services::LlmProcessor;
# use std::sync::Arc;
# async fn refresh(tools: &McpTools, client: &McpClient, llm: Arc<LlmProcessor>) -> Result<(), Box<dyn std::error::Error>> {
let binding = tools.bind_llm(llm)?;
let pipeline = pipecat_core::Pipeline::new(binding.stages());
let next = client.tools().await?;
binding.update(&next)?;
let next_schema = next.schema(); // Supply this in subsequent context/tool frames.
# let _ = (pipeline, next_schema);
# Ok(())
# }
```

`update` accepts only snapshots from the same client and atomically publishes the
schema identities and executable registrations together. The caller updates its
own shared context; old executable context identities fail closed after the
update. Both projection bookends select the current snapshot when processing a
frame. The executor independently selects its current snapshot at batch admission
and retains that batch's callbacks, timeouts and interruption policy. Provider
requests spanning an update therefore use the current inventory when their calls
are admitted; they are not pinned to an earlier request's schemas. Removed names
produce native `NotRegistered` outcomes without MCP dispatch. Duplicate history,
active leases, admission limits and the cleanup owner stay in the same executor.
No publication lock is held across an await.

Cleanup permanently closes this binding before awaiting MCP teardown. An explicit
client reopen cannot reactivate it. This API provides caller-driven refresh of one
MCP inventory; it does not implement automatic server notification handling,
general advertised-handler synchronization or arbitrary resources.
Refresh preserves the fixed provider and the same sealed/speculative and
unsupported-schema restrictions.

Use `tools.bind_llm_auto(llm)?` when the advertised schema should select the
executable handlers. This opt-in binding starts with no selected handlers and
synchronizes on each ordinary detailed or legacy context, or `LlmSetTools` frame. Only exact
bound `Direct` handlers are selected; omitted names, empty/`NotGiven` tool sets
and handler-free declarations prune automatic selections. A handler-free function
with the same name stays advertisement-only. Preconfigured native function
declarations provide no executable handler fallback.
Legacy contexts carry handler-free definitions and retire automatic handlers
after successful native validation; their original payload is forwarded unchanged.

The complete cached inventory remains available, so advertising the original
handlers again restores them without discovery. A same-client `update` replaces
the inventory and clears the selection until a new valid schema frame arrives.
Complete native context conversion, including messages and tool choice, precedes
selection changes. Validation and publication are atomic; a late foreign handler,
unsupported custom/grouped schema or sealed/speculative context cannot partially change authority. Automatic
sync rejects sealed/speculative frames even when their tool set is empty, since
pruning itself changes execution authority. The incoming projection bookend alone
selects handlers; the opposite bookend forwards the plain projected declaration
without clearing the selection.

Already admitted callbacks retain their options and duplicate history. The binding
keeps its MCP cleanup owner when every automatic handler has been pruned and closes
at pipeline teardown. Explicit `bind_llm`, fixed `llm_stages` and `register_into`
remain inventory-based and do not prune omitted names. This mode is limited to
ordinary schema handlers from one bound MCP inventory, without arbitrary callback
registration or server notification handling.

For the existing native `pipecat_pipeline::LlmSwitcher`, use
`tools.bind_llm_switcher(switcher)?` or
`tools.bind_llm_switcher_auto(switcher)?`, then insert `binding.stages()` together
in the same order. The switcher's existing routing propagates projected tool
declarations to active and inactive members. All members share this binding's
single MCP executor, selected callback inventory, duplicate history and cleanup
owner. A model switch keeps the MCP session open; pipeline teardown awaits one
close. Its normal root interruption cancels admitted interruptible tool callbacks.
Wait for the switcher's actual metadata boundary and submit a new context or
`LlmRun` for subsequent inference; switching does not replay earlier requests.
The same inventory/auto-selection and sealed/speculative restrictions apply.
Out-of-band `switcher.run_inference` retains its inference-only behavior.

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

## Standalone SSE configuration

For the source's `SseServerParameters` transport, use the explicit factory:

```rust,no_run
use pipecat_mcp::{LegacySseConfig, McpClient};
# async fn example() -> Result<(), pipecat_mcp::McpError> {
let mut config = LegacySseConfig::new("http://127.0.0.1:8000/sse");
config.sse_read_timeout = std::time::Duration::from_secs(300);
let client = McpClient::new_legacy_sse(config)?;
let tools = client.tools().await?;
# let _ = tools;
client.close().await?;
# Ok(())
# }
```

`LegacySseConfig` provides the same limits, name filters, fixed arguments and
output filters. The GET event stream has its own positive finite idle timeout;
POST writes use the request deadline. A bounded `endpoint` event selects the
separate message URL only after same-origin validation, including effective port.
Caller headers are sent to both authorized endpoints. Responses arrive through
the owned GET stream, never from a substitute POST JSON body. Stream errors/EOF
invalidate the session, and the failed call is never replayed. Close cancels and
awaits owned reads/writes; standalone SSE has no HTTP DELETE. The existing HTTP
factory remains Streamable HTTP with no automatic downgrade, stream resumption,
or endpoint change. Existing `ServerConfig` variants and `McpConfig` literals are
unchanged.

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

Public `call()` / `call_text()` use the latest discovered names. Retained schema,
pipeline-executor and worker-registry handlers keep private authorization for
their successfully discovered snapshot names, matching the source callback
lifetime across a later discovery refresh. They may still attempt that name on
the same configured resource; the server can reject a retired tool. Refresh does
not implicitly unregister previously installed handlers. Immutable name filters,
request bounds and cancellation still apply, and explicit close revokes calls
until an owner explicitly reopens the client. No public API can mint snapshot
authorization for an arbitrary name or another client.

`call()` returns `ToolResponse` with joined text, original typed content blocks,
`structured_content`, and `is_error`. Image/audio/resource-link blocks are
preserved without fetching linked resources or passing them off as text.
`call_text()` and native registry handlers match the source's model-facing text
behavior: empty/non-text results use its fallback string; transport/server failures
become bounded class/code descriptions. On a failure, model-facing output filters
receive empty text and may supply a custom fallback. `NotConnected` remains a wiring error.
Local request cancellation propagates as `McpError::Cancelled` without invoking
output filters. Agent-worker handlers preserve it as a cancellation frame with
`run_llm: false`, rather than a successful result or a model rerun. The MCP pipeline
executor also preserves that cancellation as one native Cancelled / ExplicitCancel
receipt with unknown external effects and no LLM rerun request. Other callback
errors remain failures; explicit pipeline interruption/cancel commands retain
their native cancellation receipts.
Server-provided error messages/data are deliberately not copied into errors.

Tool-call JSON-RPC error `-32000` also invalidates the exact failed session,
matching the source's locked `mcp-types` 2.1.1 `CONNECTION_CLOSED` constant. This
is Python SDK compatibility, not a universal MCP transport-error interpretation.
The public error remains `Remote(-32000)`, and the failed invocation is never
replayed. A separate invocation may reconnect; other remote codes and discovery
errors retain their existing behavior.

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
Explicit caller-provided auth headers are sent only to the configured endpoint
and the validated same-origin standalone SSE message endpoint.
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
requests, native pipeline/agent registry integration, and schema-carried options
and shared cleanup for ordinary Direct bindings.
Native stdio, Streamable HTTP and standalone GET/POST SSE are supported.

Not implemented: Python SDK session injection/types/context managers, deprecated Python registration method
aliases, and implicit `LLMSwitcher` handler propagation. The official SDK has no
public standalone SSE transport; this crate supplies its native owned adapter.
The source has no tool alias feature. Unknown configured fixed-argument
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

Fixtures cover real loopback HTTP JSON, Streamable HTTP SSE, standalone SSE GET/POST, stateful/stateless
sessions, the native LLM-worker tool round, native stdio child ownership/reaping,
side-effect-before404/disconnect no-replay behavior, teardown/reopen races,
request/initializer cancellation, future-drop cancellation, pagination, schema
errors, argument/output/wire limits, header/redirect boundaries, redaction, and
pipeline cleanup. Tests do not contact public MCP services or use credentials.

Official SDK sources: [repository](https://github.com/modelcontextprotocol/rust-sdk),
[versioned API](https://docs.rs/rmcp/3.3.0/rmcp/),
[registry release](https://crates.io/crates/rmcp/3.3.0).
