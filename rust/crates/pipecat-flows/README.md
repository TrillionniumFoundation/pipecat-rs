# pipecat-flows

Native Rust conversation flows, using `pipecat-core` frames,
`pipecat-turns::SharedContext`, and native LLM clients. No Python subprocess,
embedding, handler invocation, or provider simulation is part of the runtime.

## Use

1. Load `FlowConfig::from_yaml`, `from_json`, or `from_file` (or construct typed Rust configuration)
2. Register Rust `ToolHandler` / `ActionHandler` values in `HandlerRegistry`
3. Join configuration and handlers with `Flow::new`
4. Construct a `PipelineFlowSink`, aggregator pair, and `FlowManager`
5. Place `FlowProcessor` **after the LLM and before assistant aggregation**,
   and `FlowActionBoundary` **after assistant aggregation and output transport**
6. Start the pipeline, bind its sender to the sink, and await `initialize()`

Continue consuming pipeline output while the pipeline runs. `FlowProcessor`
executes complete tool declarations in tracked background tasks. Applications
can also await `execute_tools()` for explicitly supplied calls; these are marked
to prevent double execution in the processor.

Run the offline, app-supplied-tool example:

```sh
cargo run --offline -p pipecat-flows --example configured_flow
```

The YAML example uses the same native engine and a Rust `save_order` handler,
with opt-in confined includes and a boolean-result branch:

```sh
cargo run --offline --locked -p pipecat-flows --example yaml_flow
```

It loads `examples/yaml/flow.yaml`, includes node/prompt files, supplies a tool
call, and prints the resulting `confirm` node and rendered conversation context.
It requires no Python runtime, provider credentials, network, or audio device.

Add a real `pipecat_services::LlmProcessor` between the user aggregator and
`FlowProcessor` for live inference. Tool schemas are advertised to the LLM;
execution remains local Rust. Handler closures may capture application clients
or transport handles. `ToolHandler::typed<T>` deserializes arguments into a
`serde::Deserialize` type before calling application code. JSON-schema metadata
is advertised, not treated as a complete local JSON-schema validator; use typed
arguments and application validation for constraints.

## Semantics

- Config validation rejects unknown fields, missing messages, invalid graph
  edges, duplicate/global-conflicting tools, malformed branch/action shapes,
  and missing handler references. Missing references are reported together
- Configured tools return `ToolResult::configured(result)` or `no_response`.
  Configured graph edges own transitions. `Flow::native` instead accepts
  `stay` / `transition(result, node_name)` from Rust handlers
- Branches select a top-level result field, normalize boolean/string cases,
  and support default or stay. Prompt templates resolve nested state mappings
  on each node entry; `\{{ key }}` escapes a placeholder
- Node transitions render first, execute pre-actions, update persistent role
  settings and context/tools, commit the current node, optionally run the LLM,
  and execute or defer post-actions. Context/tool updates survive interruption
- `append` preserves earlier context, including TTS pre-actions. `reset`
  replaces messages but preserves manager state and inherited role settings
- Node-local plus global tools retain declaration order. Tool calls within a
  batch execute concurrently; transitions wait for every result to reach the
  context. The last completed transition wins, matching the source manager
- Null consolidated results become `{"status":"acknowledged"}`. Handler
  failures, argument decoding errors, timeouts, and task panics become tool
  error results. `no_response` suppresses inference for that call
- Tool interruption cancellation defaults to false, matching Flows. Individual
  tools can opt in and set timeouts; manager-wide timeout is optional
- Manual transitions cancel old-generation work. Cancel/end stop tools and
  waiting actions. State writes through stale handles fail. Dropped tool-batch
  futures cancel their tasks; pending context calls are reconciled before the
  next manager operation. Prefer awaiting operations or explicitly cancelling
  the manager and its pipeline when abandoning a conversation
- `tts_say` queues real `TtsSpeak` frames; `end_conversation` optionally speaks
  before `End` and terminates the action list. Inline `function` actions run
  after the downstream fence; custom actions execute immediately when reached
- With `respond_immediately=false`, post-actions run once after a subsequent
  bot-stop event. Action-generated bot-stop events do not discharge them;
  changing nodes clears old deferred actions

## YAML, JSON, and include policy

Both formats use the same `FlowConfig` validation, graph, registry, and manager.
The six source YAML examples are load-tested from baseline
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`; their tool names must be joined to
application-supplied Rust handlers. YAML supports block/flow collections, quoted
and folded/literal prompts, ordinary bounded anchors/aliases, transition-only
functions, global tools, actions, templates, and boolean/numeric/null branch-case
keys. Boolean case strings are canonicalized; collisions such as `true` and
`"True"`, or `3` and `"3"`, are errors. Other mapping keys must be strings.

`from_yaml`, `from_json`, and `from_file` disable includes. Grant a directory
explicitly only for application-owned configuration:

```rust
use pipecat_flows::{FlowConfig, FlowIncludePolicy, FlowLoadOptions};

let root = std::path::PathBuf::from("/app/flows");
let config = FlowConfig::from_file_with_options(
    root.join("flow.yaml"),
    &FlowLoadOptions {
        includes: FlowIncludePolicy::Local { root },
        ..Default::default()
    },
)?;
# Ok::<(), pipecat_flows::FlowError>(())
```

Do not take the root from a request or other untrusted configuration. Every
`!include` uses this fixed root, including nested includes, matching the source
loader's fixed `base_dir`. Paths must be nonempty relative paths without dot or
parent components, empty components, backslashes, colons, or NULs. No URLs are
fetched. The entry file must also be inside the granted root. A deliberately
chosen root symlink is resolved once. Unix root acquisition then walks every
canonical absolute component from a pinned `/` descriptor with `DIRECTORY` and
`NOFOLLOW`, rejecting ancestor or final-component symlink replacements. Includes
walk descriptors relative to that pinned root and likewise reject intermediate
and final symlinks, without a canonicalize-then-open race. Only regular files are
read, with nonblocking opens to reject FIFOs. Confined includes return
`Unsupported` on non-Unix platforms; in-memory JSON/YAML remain portable.

The default hard ceilings, shared across a load and all includes, are:

- 1 MiB per source document and 4 MiB total source bytes
- 65,536 expanded value/key/tag nodes and nesting depth 64 (root depth zero)
- 4 MiB expanded scalar bytes, including repeated alias strings and keys
- Eight include edges per chain, 64 include reads, and 4,096-byte include paths

`FlowLoadOptions::limits` may lower these ceilings, never raise them. Parsing
charges nodes/depth/scalar bytes through `DeserializeSeed`/`Visitor` before
constructing tree values, including alias expansion. Duplicate keys fail before
their values are decoded. Includes share the same construction budget, count
repeated reads, detect cycles, and propagate the replacement's tree depth.
`FlowError::Limit` distinguishes resource rejection from malformed configuration.
Bounded source bytes also bound the YAML parser's source-event buffering; a
streaming maintained YAML scanner checks tag tokens with token/depth ceilings
before construction. It aligns YAML 1.1 Unicode line boundaries with libyaml.

Executable tags, global/verbatim tags (including explicit `!!str`), tag
directives, YAML merge keys, multiple documents, nonfinite numbers, Python
handler/module import expressions, and executable handler objects are rejected.
Tag-like text in comments, quotes, and block scalars is ordinary data. YAML
scalar resolution follows Serde YAML's YAML 1.2 rules: quote text such as `yes`,
`no`, `on`, `off`, legacy octal/date forms when migrating YAML 1.1 applications;
use `true`/`false` for booleans. The token scanner requires strict YAML 1.2
indentation and rejects permissive PyYAML/libyaml extensions such as
underindented multiline quoted strings or flow-collection closers. Indent
continuations, including those after raw NEL/LS/PS characters, or use escaped
Unicode in double-quoted strings. The loader does not reproduce Python coercions,
imports, evaluation, or the source loader's unrestricted filesystem reads.

## Action completion and integration

`FlowSink::drain` is an acknowledgement contract, not a timer. The supplied
pipeline sink uses an uninterruptible `FlushDetailed` frame with
`origin="pipecat.flows"` and a unique `pipecat.flows.barrier` metadata ID. The
core routes it down/up/down; the final returning marker is acknowledged and
consumed by `FlowActionBoundary`. Aborted waits remove their receipt; surviving
markers cannot run cancelled actions.

The native LLM and streaming-speech processors serialize nonreturning flow
fences behind accepted inference/synthesis. Output transports must preserve
that ordering and wait for their backend's explicit playback acknowledgement.
A network send alone is not evidence that audio played. Custom async processors
must implement the same fence contract, or supply a `FlowSink` with equivalent
acknowledgements. Failures are returned, not reported as completed speech.

## Summary adapter

`set_node_with_summary(name, prompt)` calls a configured `SummaryProvider`.
`LlmAdapter` implements this using real out-of-band native LLM inference, without
conversation tools. Successful summaries use the source developer-message
prefix. Missing, empty, failed, or timed-out summaries fall back to append;
the default timeout is five seconds. No locally fabricated summary is used.

## Explicit boundaries

- Filesystem includes require an explicit confined-root policy as above
- The graph and handler registry are fixed after construction. Runtime Python
  `NodeConfig` factories, Python signature/docstring extraction, decorators,
  legacy zero/one-argument call conventions, deprecated `role_messages`, and
  inline Python callable objects are not interpreted
- Rich conversation history is preserved through the shared context, but node
  prompt configuration follows the source declarative text-message format
- Only one tool batch runs per manager. Concurrent overlapping batches return
  `Busy`; the batch size defaults to 32 and replay IDs are bounded to 4096
- Provider-native realtime flows, arbitrary provider-specific actions, and
  dynamic LLM/service switching require explicit application integration
- External side effects already performed by a handler cannot be rolled back
  on cancellation. Handlers should honor their cancellation token

## Tests and source authority

`cargo test --offline -p pipecat-flows` covers graph/config/template parity,
context and action ordering, deferred work, native/configured return contracts,
parallel tool transitions, errors/panics, timeouts, interruption, stale writes,
dropped operations, sink shutdown, and native pipeline integration. Loopback HTTP/SSE and Cartesia WebSocket tests compose native inference,
synthesis, shared output, assistant aggregation, and flow observation. They
verify post-actions wait for a gated synthetic backend `Played` receipt and
committed assistant context, including cancellation at each asynchronous stage.
These protocol tests do not exercise a physical speaker or audio device.
The YAML suite also checks JSON/native-execution equivalence, the six baseline
source examples, unknown/duplicate keys, malformed/executable tags, alias bombs,
shared include budgets, cycles, traversal, symlinks, and nonregular files.

Behavioral authority is `src/pipecat/flows/{config,flow,manager,actions,types,
adapters}.py` and `tests/test_flows_*.py`. These tests port the supported
semantics; they do not claim to execute the Python test suite or cover every
provider/transport combination.
