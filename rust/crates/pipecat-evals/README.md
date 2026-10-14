# Native scripted evaluations

`pipecat-evals` drives **running RTVI eval WebSocket endpoints** and returns
source-shaped assertion, expectation, turn, and scenario results. It uses
`pipecat-protocol` envelopes and `pipecat-transports` bounded WebSocket sessions.
It can also own explicitly configured native bot processes and run a bounded
native release manifest. There is no Python execution, Python bot launch,
shell-command template, runtime fallback, or fabricated judge verdict.

## Run

From `rust/`, start the deterministic native protocol test target:

```sh
cargo run -p pipecat-evals --example echo_bot -- 7860
```

In another terminal:

```sh
cargo run -p pipecat-evals --bin pipecat-eval -- run \
  crates/pipecat-evals/tests/fixtures/echo.yaml ws://127.0.0.1:7860 --stop-bot
```

Replace the URL with an actual bot's eval endpoint to evaluate that bot. The
example is an echo target, not an LLM. `ws://` and `wss://` use the existing native
transport. Use TLS for non-loopback endpoints carrying private conversations.
The CLI prints one JSON result per scenario, exits 0 when all pass, 1 for failed
runs (including cancellation), and 2 for invalid input. Ctrl-C cancels the active
session and leaves later scenarios unrun. All scenarios are validated before any
network connection. Multiple scenarios run sequentially with separate connections;
the bot remains responsible for isolating their conversation state. `--stop-bot`
is limited to a single scenario because cancelling a shared endpoint would stop
later scenarios from connecting.

## Supported source contract

The implementation follows `src/pipecat/evals/{scenario,script,serializer,events,
matcher,script_driver,results}.py` and `tests/test_evals_{scenario,serializer,
session,suite}.py`:

- YAML/JSON scenario files: named `scenarios`, `<file>/<scenario>` names, whole-value
  default replacement, context seeding, `stop_on_failure`, `trigger_disconnect`,
  text `user`/`judge` modality, and the legacy single-scenario shape
- Bounded relative `!include`; no Python constructors or executable YAML tags
- Text, DTMF, observation-only turns, `send_after` event anchors and pure delays
- `client-ready` / `bot-ready`, `eval-configure`, `eval-context`, optional
  `eval-cancel`, text-mode `skip_tts` and image-capture connection flags
- RTVI user/VAD/bot-speaking events, final user transcriptions, LLM token assembly,
  first-token time, sideband markers, function start/stop, and image metadata
- Turn/interruption boundaries suppress stale LLM tails until the next
  `bot-llm-started`; old pending bot output is discarded while user transcriptions
  are retained
- Ordered assertions with unrelated events skipped; whitespace-normalized text
  containment/exclusion and cross-response text aggregation
- Function calls in any order, shallow argument subset checks, corrected-call
  retries, distinct missing-call versus mismatched-arguments failures
- Marker meanings (`complete`, `short`, `long`, `incomplete`), marker count,
  first-marker placement, and text following the first marker
- Absence windows, shared per-turn deadlines, stop/continue-on-failure, explicit
  `not_run` later turns, matched expectation summaries, and timestamped event history

`serializer::decode_control` is a typed **eval-only** bot-side control decoder.
`Context` means replace context without running the LLM. `Configure` exposes
function report levels, VAD, and marker flags. `Cancel` requests worker teardown.
Its bounded base64 image decoder is available to native bot hosts, although image
file upload is not implemented in the scripted client. An embedding host must
apply these controls to its native pipeline; the decoder itself does not host a
pipeline or elevate a production RTVI endpoint's privileges.

## Library and suite ownership

```rust,no_run
use pipecat_evals::{Cancellation, EvalSession, ScenarioFile, SessionOptions};
# async fn example() -> Result<(), pipecat_evals::EvalError> {
let scenario = ScenarioFile::load("scenario.yaml")?.scenarios.remove(0);
let cancel = Cancellation::new();
let session = EvalSession::new(scenario, "ws://127.0.0.1:7860", SessionOptions::default())?;
let result = session.run(cancel).await;
assert!(result.passed, "{:?}", result.failures);
# Ok(()) }
```

`suite::run_suite(Vec<SuiteJob>, concurrency, options, cancellation)` prevalidates
all jobs, runs bounded concurrent native sessions, and returns results in input
order. Supply independently hosted endpoints for stateful bots. This API does
not launch processes itself; `release::run_release` consumes the explicit native
manifest subset described below. Neither API executes Python bots. Cancellation is
sticky and race-safe: active sessions tear down, queued jobs return cancelled
results without connecting, and every owned task is joined. Dropping a suite
aborts its `JoinSet`; dropping a session future drops/aborts its WebSocket owner.

`stop_bot` sends `eval-cancel` on success, failure, or requested cancellation. It
is best effort if the connection is already lost. Outgoing controls are drained
before closing. Cleanup is bounded by transport write and close deadlines and
its errors are included in `debug_log`. The whole-session deadline excludes this
bounded cleanup. A disconnected bot never satisfies an absence assertion.

## Native process hosting and release execution

**Ownership is for the direct child only.** The runner does not create a Unix
process group or Windows Job Object. Bots must not leave independently owned
children running. It is not a sandbox: a trusted executable can access the same
files and network as the harness, read its own configuration, and launch other
programs. No security settings or capabilities are changed.

`process::run_hosted` accepts a `NativeCommand`, one scenario, a literal loopback
`ws://` URL, session options, process options and cancellation. The command has an
absolute executable path, a literal argv vector, an absolute working directory
and a complete explicit environment map. Relative executable search, shell
interpolation and implicit script execution are not supported. Common script
extensions are rejected; an existing readable regular executable must have a
supported ELF, PE/MZ or Mach-O header. This compatibility check rejects accidental
scripts, including extensionless text without a shebang. It cannot authenticate
contents, detect an adversarial renamed interpreter, or prevent a concurrent
executable replacement. The executable and its directory must be trusted.
Unreadable executable files return file-I/O errors; missing paths retain a
`spawn_failed` result identifying the OS error kind. Neither includes argv/env.

The harness retries WebSocket connection attempts within `startup_timeout`
(default 60 seconds), using the actual connection for the run. It does not open
and discard a separate TCP readiness probe. An established WebSocket and a still
running child are separate checks. The RTVI `bot-ready` handshake has its own
session deadline; no turn is replayed and an exited bot is never restarted. The
harness first binds the requested loopback address/port and rejects an occupied
port before launching or connecting to anything. It releases that reservation
immediately before spawn so ordinary native bots can bind. A process-local lock
uses async, cancellable admission under the startup deadline, then covers only
reservation and synchronous spawn without an await, preventing concurrent hosted
forks from inheriting each other's temporary listening FDs before exec. Waiting
callers do not block Tokio workers; sessions and teardown still run concurrently. The caller must still ensure exclusive
loopback ports and coordinate unrelated fork/exec activity in the embedding
process: a release-to-bind race remains possible, and loopback connectivity does
not authenticate the server as the launched process.

Every hosted session requests `eval-cancel`. The harness waits `shutdown_grace`
(default 2 seconds), then kills and reaps an uncooperative direct child. Explicit
cancellation shortens this grace and still completes teardown. Normal reaping
polls asynchronously, so unrelated Tokio tasks remain runnable. Both bounded
pipe readers are joined; pipe drain has a separate 1-second default deadline,
after which readers are aborted **and joined**, and incomplete output fails the
run. On Unix the pipe adapters are nonblocking, so an unowned descendant cannot
hold these reader tasks beyond the drain deadline. Tokio's Windows pipe adapters
use its blocking pool: an unowned descendant retaining a pipe handle can keep an
underlying read alive even after the controller task is cancelled, and can delay
runtime shutdown. Windows descendant-held pipe cleanup is not guaranteed; the
process/pipe tests here were run on Linux.

**Dropping/aborting a hosted future synchronously kills and waits for its direct
child.** This avoids a detached reaper or unreaped child on caller cancellation,
but can block the dropping thread. No OS provides an absolute completion bound
for killing/reaping a kernel-stuck process; this mandatory reap is outside the
normal readiness, graceful shutdown and pipe-drain deadlines. Synchronous OS
spawn and kill operations likewise cannot promise a hard execution deadline;
the startup budget bounds admission/readiness once the OS calls return. Unexpected OS
errors are reported on explicit shutdown; Drop cannot report errors. Pipe and
WebSocket tasks are abort-owned on Drop and cease when the runtime next polls
them. Prefer cooperative `Cancellation` and await the result for joined cleanup.

### Environment, configuration and retained output

- The child starts with `env_clear()`, null stdin and piped stdout/stderr. Nothing
  from the parent environment is allowlisted by default, including `PATH`,
  `HOME`, proxy variables, credential variables and loader variables. There is no
  dotenv loading or environment-value expansion
- Only `NativeCommand.env` / an entry's `env:` mapping is supplied. This map can
  carry explicitly chosen nonsecret settings or credentials already authorized
  for that bot. The executable may still read files or other external sources;
  environment isolation alone cannot prevent that
- `NativeCommand` Debug does not print argv, paths or environment values. Complete
  nonempty explicit environment values are exact-match masked in stdout/stderr,
  including values split across pipe reads. Longest matches win and replacements
  are not recursively searched. This is not semantic secret detection; encoded,
  transformed or partially printed secrets and secrets from config files cannot
  be recognized. Bot protocol responses/results are not secret-redacted
- Keep credentials out of argv, labels and runner-body data, and configure bots
  not to log secrets. Body mappings are ordinary bot configuration, never Python
  factories. The harness never interprets or calls a provider from a body
- Each raw output stream is capped before appending a fixed 4-KiB read chunk
  (default 64 KiB, maximum 1 MiB). Oversized lines cannot bypass this cap. Output
  is read as bytes and never buffered by line. Masked/UTF-8-lossy output has the
  same retained-byte cap; expansion beyond it fails the run. Any overflow or
  incomplete pipe output omits both streams, rather than returning a truncated
  secret prefix or treating missing evidence as a pass
- A complete `HostedResult` is capped at 16 MiB of serialized JSON by default,
  including process metadata, logs and every session field. Oversized results are
  replaced by a small failed `limit_exceeded` summary. Process PID, exit code,
  forced shutdown and reap status remain available. These are logical retained
  output bounds, not exact peak allocator or parser-memory guarantees

### Native manifest

Build and run the deterministic native example, with no provider calls:

```sh
cargo build -p pipecat-evals --example echo_bot --offline -j2
cargo run -p pipecat-evals --bin pipecat-eval --offline -- \
  suite crates/pipecat-evals/examples/native-manifest.yaml
```

`native_version: 1` is required. An unchanged `evals/release/manifest.yaml` is
explicitly rejected. Supported top-level fields are `concurrency` (1..32),
`repeat` (1..128), `base_port` (default 7900), `bots_dir`, `scenarios_dir` (default
`scenarios`), `record: false`, `cache_dir: null`, and `suite`. Unknown fields are
errors. At most 128 expanded runs are admitted.

Each `suite` entry has a compiled `bot` path, `scenarios` references, optional
`name`, `concurrency`, literal `args`, `env`, `cwd`, and `runner_body`. There are no
implicit bot flags. Only whole argv elements `{port}` and `{runner_body}` are
substituted. A body requires an explicit `{runner_body}` argument, normally
preceded by the bot's own `--runner-body` option. Embedded template strings and
source `{python}` / `{bot}` spawn templates are rejected.

Paths resolve relative to the manifest. Bot paths additionally use `bots_dir`;
scenario references ending in `.yaml`, `.yml` or `.json` resolve directly, while
other names resolve below `scenarios_dir` with `.yaml` added. scenario files use
the existing bounded loader; unsupported judge providers, audio and simulated scenarios fail before
any process launches. All scenario/config inputs are validated before execution.

`runner_body` accepts exactly one `path` or inline mapping `data`. A path must
contain a bounded YAML/JSON mapping and defaults the child's cwd to the body
file's directory, matching source relative-asset behavior. An explicit `cwd`
overrides it. Inline data is written to an owned per-run JSON file; its cwd is
still the manifest directory unless overridden. The temporary body directory is
mode 0700 on Unix and uses inherited temporary-directory permissions on Windows.
It is removed after child teardown, including cancellation/owner drop. No body
file is published or retained as a result artifact.

`release::run_release` keeps jobs in source attempt-major/manifest order, starts
at most the global and each entry-label concurrency cap, and returns results in
input order. Labels default to the bot path; duplicate label/scenario pairs are
rejected and entries sharing a label share its lowest cap. Every run has its own
assigned port, child, scenario, attempt and process report. Pending cancelled
jobs produce explicit cancellation records without spawning. The suite's 64-MiB
serialized result cap reserves a failure summary for each job before launch;
overflow replaces the full run with its provenance-bearing summary and cancels
remaining work. `run_release_with_budget` allows a smaller cap for embeddings.
A budget too small for summaries fails before any launch. Temporary active runs
still consume their separately bounded session/process memory.

The CLI prints one JSON suite result and exits 0 for pass, 1 for failed runs or
cancellation, and 2 for invalid input. Ctrl-C cancels and awaits all active jobs.
The `process-fixtures` Cargo feature only enables the dedicated test child binary;
it is not needed by the native hosting library, example, or CLI.

## Limits

Session defaults are configurable through `SessionOptions`; source-file and suite
caps are fixed:

- Transport queue: 64 messages; individual wire message: 1 MiB
- Connect/write: 10 s each; close: 2 s; handshake: 10 s
- Expectation: 60 s; send-after/bot-quiet wait: 30 s; session: 600 s
- Session wire traffic: 65,536 messages and 64 MiB, including ignored traffic
- Event history: 16,384 events and 16 MiB; LLM/aggregate text: 1 MiB; marker
  alphabet: 64 markers of at most 256 bytes; function calls per expectation: 256
- Source file: 1 MiB; all source/include bytes: 4 MiB; include depth: 8;
  include path strings: 4,096 bytes; scenario: 1,024 turns / 4,096 expectations
- Parsed construction: 65,536 expanded nodes, composed depth 64 and 4 MiB expanded
  scalar budget across the entire load, including all aliases and included files
- Suite: at most 128 jobs / 32 concurrent sessions / 64 MiB for the returned
  `SuiteResult` serialized as JSON, including its envelope and all result fields

The suite reserves a small, precisely sized failure summary for every job before
networking. A full result that cannot fit alongside the remaining reservations
is replaced entirely by an explicit `limit_exceeded` summary; all turn records,
matched strings, event history and diagnostics are omitted, and the scenario name
is abbreviated with its input job index. The summary is charged to the same
budget and remaining work is cancelled. Every stored record is counted, including
later cancellation/limit summaries. A budget too small for the summaries is
rejected before opening sockets. This cap bounds serialized returned output,
not the Rust allocator's overhead, transient result construction, or memory held
by concurrently active sessions.

YAML and JSON entry points construct values through a budgeted Serde seed/visitor.
Nodes are charged before recursive value construction; strings and tags are
charged before being copied into the expanded tree. Mapping keys count as nodes
and scalar bytes, duplicate keys are rejected before their values are parsed,
and vectors do not reserve an untrusted parser size hint. Scalar accounting uses
UTF-8 bytes for strings/tags/keys, one byte for booleans and eight for numbers.
Includes share this accounting and inherit their parent's structural depth;
include placeholders remain conservatively charged after replacement.

These are logical construction limits, not exact peak-memory guarantees. Serde
YAML first buffers source events and decoded source scalars before the visitor
runs. Each such source read is capped at 1 MiB, but parser event storage has its
own allocation overhead. JSON conversion, schema normalization/default copying,
allocator capacity and error formatting also add transient storage. The library
`from_value` entry point receives an already constructed caller-owned value;
construction limits cannot retroactively constrain its allocation. Include paths
resolve relative to their including file and are canonicalized for cycle checks;
ordinary absolute/parent path semantics and filesystem permissions still apply,
so this loader is not a filesystem sandbox.

Limits terminate runs rather than silently discard evidence and return a pass.
Failure categories preserve source names where applicable and add `cancelled`,
`disconnected`, and `limit_exceeded`. Empty scripted scenarios are permitted by
the source schema, but still require a successful connection and handshake.

## Explicit gaps and intentional strictness

Unsupported capabilities fail validation rather than being approximated:

- Judge/provider Python factory configuration, explainers, simulated personas,
  goals/metrics, provider services, Python bot subprocesses, Python suite manifests,
  runner HTTP requests, recording and cache orchestration
- YAML/CLI audio modalities, automatic file loading, TTS/STT/VAD inference,
  transcript-audio alignment, `tts_response`, recording, and scripted image-file
  input. The bounded recorded-input library API described below is available
  separately for compatible audio-capable endpoints
- No report dashboard, persistent runs/logs directory, progress callbacks,
  process respawn/retry, distributed scheduling, or descendant process containment
- The native manifest retains source labels, scenario expansion, per-label
  concurrency, repeat attempts and runner-body files. It does not translate
  source `spawn:` strings, Python interpreters, CLI glob filters, or source
  recording/cache/report-directory settings

The YAML parser is Serde YAML 1.2; quote DTMF strings (especially nondecimal or
underscore-containing scalar spellings) for exact source spelling. `012` is
preserved and tested. The Python loader's custom decimal-only integer resolver is
not reimplemented. Unknown keys/events, conflicting call shorthands, invalid
payload types, zero/negative budgets, and irrelevant call/text checks are rejected
more strictly than the Python loader. Non-finite YAML numbers are rejected.
Response absence follows resolved text-mode
`llm_response` semantics; audio-only reply-continuation handling is unsupported.
The native session pumps its bounded reader during waits and drains queued input
at turn boundaries; event timestamps represent harness ingestion, not packet
capture timestamps. No retry can eliminate network-in-flight ambiguity at a turn
boundary without a bot-provided turn ID.

## Verification

```sh
cargo test -p pipecat-evals --all-targets --offline -j2
cargo test -p pipecat-evals --features process-fixtures --all-targets --offline -j2
cargo clippy -p pipecat-evals --features process-fixtures --all-targets --offline -j2 -- -D warnings
cargo fmt -p pipecat-evals --check
```

Contract fixtures identify the original Python source. The untouched
`source_short_complete.yaml` and its `judge_text.yaml` are copied from
`evals/turn-completion/scenarios/` and assert explicit rejection of its unavailable
judge factory. The native fixture exercises the supported source schema. Real
TCP/WebSocket loopback tests cover pass/fail, tools, markers, DTMF, context,
handshake/connect failures, deadlines, absence, interruptions, event limits,
`eval-cancel`, dropped owners, and concurrent suite cancellation. They do not
claim live-provider, Python-host, TLS-infrastructure, audio, or full Python
release-suite parity. The opt-in native child tests cover startup errors,
readiness timeout, abnormal exits, cancellation and owner drop during readiness,
run and cleanup, blocked pipe writers on a single-thread runtime, already-exited
children, bounded logs/results, clean environment, literal argv, body file cleanup,
release repeats/cancellation, 32-way distinct-port launches, cancelled/timed
launcher admission, Linux `/proc` absence after reap and owned-task quiescence. No test calls a provider or executes an original Python bot.

## Native scripted classifier judging

Scripted text `response`/`llm_response` and `function_call` expectations accept
`eval: <criterion>` (1..8192 UTF-8 bytes). Deterministic text/name/argument checks
still apply before classifier inference. `function_call_stopped`, image, audio,
simulation, metrics, and other event types do not support native `eval`.

The CLI, independent endpoint suites, and native hosted release sessions use:

```yaml
name: geography
judge:
  modality: text
  eval:
    service: jev
    api_key_env: TYPESAFE_API_KEY
    model: jev-1.13.0
    timeout_ms: 10000
    allow_continue: true
    explainer: false
turns:
  - user: Where is Paris?
    expect:
      - event: response
        text_contains: France
        eval: Answers that Paris is in France
        within_ms: 10000
```

Only the explicitly named environment variable supplies the credential. There is
no default judge, implicit model download, Python factory, dotenv import, warmup,
or automatic outer retry. The native Jev builder also disables Jev's provider
retries, so one uncached judgment makes exactly one HTTP request. `base_url` may
specify an HTTPS origin or HTTP loopback for local fixtures; redirects, proxy
inheritance, userinfo, query strings and path prefixes are disallowed by the
classifier. Non-Jev provider configuration and explainers are rejected.

For another native provider, construct `EvalJudge::new(Arc<dyn Classifier>,
JudgeOptions)` and pass it to `EvalSession::with_judge`. This accepts the existing
`LlmClassifier` over a native `LlmInference` implementation. Injection takes
precedence over the scenario's provider settings and does not read its credential
environment variable. The classifier owns wire response limits and any explicitly
chosen retries; the judge enforces its own deadline/cancellation and validates the
typed answer. Provider probability provenance is retained; these numbers are not
claims of measured calibration. Direct `evaluate_reply` and `evaluate_call` are
also available with absolute `CallOptions` deadlines and cancellation tokens.

Each consumed reply segment is recorded once. Consecutive bot segments are merged
into one numbered bot turn; each new user/DTMF input delimits the next turn.
Responses consumed by deterministic checks and pending responses discarded at a
new input boundary remain conversation context. Calls are judged by their actual
matched name/arguments, independently of later bot promises. Tool entries are not
inserted into scripted reply/call conversation state. Scenario initial `context`
is sent to the bot; it is not silently copied to the judge.

Reply verdicts are `yes`, `no`, or `continue` (the latter is optional). A `no`
waits for another segment for up to two seconds when the bot is quiet, or until
the shared turn deadline when speaking. `continue` waits within that same turn
budget. New segments rejudge the changed snapshot, without appending the entire
aggregate a second time. Function judgments use a yes/no probability threshold
of 0.5. Per-expectation `judgments` retain decisions, numeric probabilities,
provenance and transcript revision; no classifier prompt or provider response
body is placed in results. The original bot `events_seen` and matched text remain
ordinary, potentially private run evidence.

During inference the session keeps ingesting bounded RTVI traffic. Interruptions
invalidate in-flight verdicts; late results never satisfy a replacement turn.
Cancellation, owner drop, provider deadline, malformed or duplicate-key replies,
and transport failures cannot pass an assertion. `judge_no` and `judge_continue`
are behavioral failures; `judge_error` and `judge_timeout` are infrastructure
failures. An in-flight interruption is `judge_interrupted`. No canceled or failed
provider response is cached. Duplicate empty LLM stop events are not new evidence.
Without wire-level bot turn identifiers, network-in-flight old output arriving
with a fresh start after the next input cannot be perfectly attributed.

Defaults per judge: one serial inference, 256 requests, 512 evaluations (including
cache hits), 2048 transcript entries / 256 KiB transcript text, 1 MiB rendered
request, 64 KiB native Jev response, 256 cache entries / 4 MiB cache keys plus reserved verdict bytes, 10-second
request deadline, and two-second no grace. All limits have finite hard ceilings.
Cache keys include judgment kind, trimmed criterion and exact structured state;
the cache belongs to one owned judge/run. Exhaustion fails rather than silently
truncating evidence. Native Jev retains its bounded JSON depth/node decoder; the
judge never executes content or gives the provider tools or external-agent access.

Intentional source differences: no failure retry, no optional LLM explanations,
no implicit Ollama default, only reply/function-call judging, explicit native
provider schema, bounded session-local cache, and infrastructure failures kept
separate from classifier `no`. Existing non-judge scenarios keep their matching
semantics. All tests use native localhost HTTP/RTVI peers and synthetic credentials.

## Real native text bot host

`bot_host::EvalBotHost` is an explicitly selected eval-only transport. It runs
an application-supplied `EvalBotFactory`, using the existing RTVI processor,
observer, native pipeline and `EvalControl` codec. It does not alter the browser
runner endpoint or add eval privileges to a production session.

The factory returns a fresh `EvalBotPipeline` containing its shared context,
application processors and final assistant aggregator. The host observes the
assistant **after** each frame is processed. Thus a response completion follows
its context commit, and locally executed tool calls/results use the ordinary
native tool/aggregator continuation path. Providers remain application-owned;
the loopback host does not constrain a custom factory's outbound provider URLs.

The compiled example uses `LlmProcessor`, an `LLMContextAggregatorPair` in
`AssistantTextMode::Text`, and a registered Rust `add` tool. Its model endpoint
must be explicit literal-loopback HTTP. Its HTTP client disables proxy discovery,
redirects and retries. It never launches Python or loads a provider SDK.

```sh
cargo run -p pipecat-evals --features native-bot-example --example native_text_bot -- \
  --port 7860 --llm-url http://127.0.0.1:11434/v1/chat/completions --model local-model
# Build a standalone executable for the existing native release manifest:
cargo build -p pipecat-evals --features native-bot-example --bin native-text-bot
```

Use the actual installed model name. The example has no greeting unless
`--introduce` is explicitly selected. A native-version-1 release manifest can
point `bot` at the compiled `native-text-bot` and supply:

```yaml
args: [--port, "{port}", --llm-url, http://127.0.0.1:11434/v1/chat/completions, --model, local-model]
```

### Capabilities and session boundaries

- The bind API accepts a `SocketAddr` with a literal loopback IP only. The host
  checks the exact local Host header, rejects all browser Origin headers,
  absolute-form request targets and paths other than `/`
- `skip_tts=true` is accepted; omitting it still selects mandatory text-only
  behavior. `false`, unknown/duplicate query keys, audio/image capture and binary
  WebSocket frames are rejected. `trigger_disconnect=true|false` is supported
- Only `client-ready`, `send-text`, `disconnect-bot`, `eval-context`,
  `eval-configure` and `eval-cancel` are admitted. IDs are required and bounded
- The host passes `skip_tts=true` to the factory before construction, then waits
  for `LlmConfigureOutput(true)` followed by a native normal-lane flush round trip
  before readiness. The flush is necessary because LlmProcessor consumes configuration. Optional
  introductions start only after the correlated `bot-ready` is forwarded
- Seed context replaces only messages, preserving application tool definitions.
  This first slice accepts only role/text-content messages, excluding tool
  history and multimodal content. Seeding never runs inference
- Context and observer configuration must precede the first admitted text input
  or introduction. This restriction prevents queued RTVI commands from racing
  direct control updates. Observer controls update the actual observer's
  function report level, VAD and LLM marker options
- Output is generated text. No speech, synthesis, playback receipt or physical
  audibility is invented. Factories must honor text-only settings and may not
  start inference in setup/Start; use `introduction` for a greeting
- An ordinary disconnect cancels and cleans up its pipeline, then permits a
  fresh factory call. There is no implicit context/tool reuse. A second client
  arriving while busy is closed, not admitted as another pipeline
- `trigger_disconnect=true` stops the host when that connection disconnects.
  `eval-cancel` stops both the session and host, including during startup.
  `disconnect-bot` sends native End and keeps the host available after cleanup.
  Applications request whole-pipeline termination with native EndWorker/StopWorker;
  a raw mid-pipeline End does not notify earlier processors

### Ownership and finite limits

Defaults bound socket, RTVI, observer and pipeline queues; application processor
count; tasks; frame size; cumulative processed bytes/frames and wire input;
context messages/bytes; connections; startup, session, host and shutdown times.
The host checks context growth before processor work and after assistant commits.
It fails visibly on overflow rather than truncating conversation or tool history.
The bounded frame meter supports the native ToolExecutor text path, preserving
local RTVI flush handles and result callbacks through nonexecuting size
projections. Other nonserializable application payloads, including local message
transforms or direct-function context schemas, are not part of this first slice
and fail admission rather than being silently stripped.
The core per-hook shutdown timeout is capped by the host shutdown budget divided
by the actual installed processor count plus two, accounting for serial cleanup.

The serve future owns the listener, one socket and one pipeline; it creates no
host background task. Cooperative cancellation drains/joins native owners and
waits for host-created processor wrappers to be released. Cancelling an incomplete
Pipeline::start triggers core asynchronous cleanup; the host tracks wrapper
release before admitting another session. Timeouts are reported, never claimed
as successful graceful cleanup. Ordinary peer I/O resets during cancellation
are treated as disconnects; protocol, deadline and task errors remain visible. Dropping the serve future closes listener/socket
and invokes native pipeline cancellation guards; core asynchronous cleanup settles
afterward. Blocking, non-cooperative application code cannot be forcibly stopped
by an async deadline and is outside this host's contract.

End/Stop waits for a bounded acknowledgment that admitted observer events have
entered the bounded transport before invalidating their original invocation
authority, then finishes the WebSocket writer and reports drain failures. Cancel
bypasses both drains. Queued events retain their original invocation fence,
so stale interrupted work cannot acquire a new session's output authority.

`tests/bot_host.rs` exercises actual localhost WebSocket/HTTP, context seeding,
real Rust tool continuation, two EvalSession turns, negative assertions,
configuration and cancellation. Hosted-release coverage starts and reaps the
compiled real bot. Test code is synthetic provider evidence, not live-provider,
audio, browser-device, simulated-persona or source-release-suite parity.

## Recorded input library extension

For a compatible audio-capable RTVI endpoint, `RecordedAudio` and
`EvalSession::with_recorded_audio_inputs` replay bounded 16 kHz mono PCM16/WAV
recordings through paced `raw-audio` messages and existing transcription/text
assertions. See [RECORDED_AUDIO.md](RECORDED_AUDIO.md) for budgets, ownership,
source differences and the real native pipeline fixture. This opt-in library
path does not enable audio in the bundled text host or YAML/CLI parser.
