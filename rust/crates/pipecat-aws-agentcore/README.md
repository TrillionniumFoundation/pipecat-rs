# Native AgentCore default-schema text application

This crate implements one bounded, finite, native context-to-text slice of the
pinned Pipecat `AWSAgentCoreProcessor`. It does not implement the entire AgentCore
API or arbitrary agent schemas. No Python/SDK sidecar is involved.

## Real workflow

`AgentCoreConfig` contains explicit `AwsCredentials`, a commercial AWS region,
and a full ordinary-partition runtime ARN. `AgentCoreClient` and
`AgentCoreProcessor` map an ordinary context to a signed InvokeAgentRuntime
request. `AgentCoreTextApp::new(config, contexts, output)` validates the complete
finite context batch, normalizes retained allocations, and constructs the actual
lifecycle-ingress → provider → caller-output `Pipeline`. `start()` returns the
owned running app; `finish()` joins it and returns `AgentCoreReport`.

The `agentcore_text` executable uses this same constructor and run path, not an
alternate HTTP demo. Run `--help` for its strict JSON input format and explicit
`PIPECAT_AGENTCORE_*` environment names. `--check-config` has no network/file
side effects. `--run INPUT.json OUTPUT.jsonl` creates a new bounded output file
only after validating all input. It never overwrites an existing file. Linux
file runs pin a validated directory descriptor to prevent symlink-parent
replacement from redirecting output; input descriptor/name identities are checked
before and after a bounded read. Help/configuration checks are portable.

Output JSONL contains `start`, `text`, `end`, then a terminal `report` record.
A failed run exits nonzero and retains any partial output. A failed final write
can leave a file without a terminal record: absence of that record is not success.
File writes are bounded synchronous regular-file writes, not playback or delivery
receipts. A same-identity concurrent in-place rewrite that evades filesystem
metadata is not claimed detectable.

## Deliberately narrow route and identity

- POST `/runtimes/{percent-encoded-full-ARN}/invocations`, signing service
  `bedrock-agentcore`, actual JSON body hash, double-encoded canonical URI.
- Fresh UUID-v4 `X-Amzn-Bedrock-AgentCore-Runtime-Session-Id` on every invocation,
  included in the signed headers. No remote-session reuse or implicit memory.
- Returned session metadata is optional, bounded to the distinct response shape
  (1–100 ASCII alphanumeric/underscore/hyphen, alphanumeric first). It need not
  equal the request ID and does not confer authority.
- Full-match ARN subset:
  `arn:aws:bedrock-agentcore:<region>:<12 digits>:runtime/<letter><0..47 alnum or underscore>-<10 alnum>`.
  Region must match exactly. Other partitions and partition-specific regions,
  qualifiers, endpoint versions, account-ID/bare-runtime mode, query strings,
  fragments, userinfo and caller-selected paths are unsupported.
- Hosted HTTPS requires **one caller-resolved numeric IP:443**. The regional
  hostname remains the HTTP Host, SigV4 host, TLS SNI and certificate name.
  No runtime DNS, proxy discovery, fallback, retry, redirect or connection pool.
- Tests use the explicit literal-loopback HTTP(S)-origin builder. It rejects a
  simultaneous address override. This is not a general production endpoint
  override and does not prove any hosted ARN exists or is authorized.

## Default mapping and stricter native differences

All contexts are bounded and validated, including older history that is not sent.
Only the **last** user/system message becomes `prompt`. Last assistant, empty
context, empty scalar content and empty parts are no-ops; older user messages are
not searched. Text parts join with one literal space including empty parts.
A nonempty array containing one empty text part still invokes with an empty
prompt. Whitespace and Unicode text are preserved. Wire JSON uses compact UTF-8
encoding rather than Python `json.dumps`' default spaces/ASCII escapes; decoded
prompt content is unchanged and SigV4 binds the exact emitted bytes.

Unlike Python's loose dictionaries, this profile rejects developer/tool roles,
message names/metadata, tools/tool choice, non-text/rich/provider-specific parts,
settings/context mutation controls, speculation, continuation, realtime receipts,
eager-source and native tool/image authority carriers. It does not silently strip
those fields or execute callbacks or tools. Ordinary unrelated/upstream frames
retain direction. There is no model setting, generated assistant-history append,
inference API, arbitrary transformer, runtime command or control-plane operation.

Successful HTTP200 responses must have exactly normalized `application/json` or
`text/event-stream`, optionally one `charset=utf-8` parameter. The default JSON
object's `response` may be missing, null, empty or text; duplicate response keys,
non-string values, malformed UTF-8, trailing documents and unsupported MIME/status
fail without logging peer bodies. Unknown ordinary JSON members are validated
within body/string/nesting bounds and ignored.

JSON accumulates to actual HTTP body EOF, independent of transport chunks.
SSE interprets **each data line independently**, not standard multiline SSE
records. LF/CRLF, one optional ASCII space after `data:`, and one initial SSE BOM
are supported. Bounded valid-UTF-8 blank/comment/non-data lines are ignored.
A final unterminated data line is parsed at EOF. `[DONE]` has no special meaning;
multiline JSON, generic event dispatch, trailers and arbitrary schemas fail.
These boundary repairs/strictness are explicit native differences from Python's
per-transport-part decoding and literal `data: ` prefix handling.

## Completion and ownership

Actual nonempty text opens a response segment. A fixed **one-second quiet gap**
closes that segment once. Later text in the same HTTP invocation opens another.
Empty invocations emit no fictional bookends. Body EOF closes any remaining
segment immediately. A segment End **does not complete an invocation**, release
its FIFO position, start queued requests or allow application End to overtake it.

Each invocation is a core-owned, interruption-aware background task. Its directly
polled Hyper HTTP/1 connection, request, response decoder and timer are in that
same future. There is no detached driver, reader, writer, timer or resolver task.
Success requires actual body EOF and natural connection-future completion.
Failure/cancellation destroys the request/connection future and socket together;
that synchronous destruction is not described as joining a separate driver task.
The core registry joins actual invocation tasks, and the retained app manager
joins the pipeline supervisor. Owner-release counters corroborate but do not
replace these joins. Cancelling/concurrently polling completion observers never
removes an unawaited JoinHandle. Dropping the running app cancels local work;
retain its completion handle if you need the terminal joined report.

Admission uses count and retained-payload byte permits and reserves FIFO order
before scheduling. The app's finite feeder pauses at that limit. A core
interruption aborts old local work/output; fresh contexts get fresh identities.
No uncertain write is replayed. End drains all admitted work. Its finite drain
budget covers the actual context count times the per-invocation absolute limit,
plus shutdown reserve. Connect, request+response-head, idle-response, output,
absolute invocation and output-cleanup deadlines are separate from the gap timer.
Generated frames use one bounded, caller-acknowledged in-flight credit. Core queue
admission alone is not completion: coalesced SSE cannot overflow the output lane
while a caller is held. Cancellation drops pending credit, and late acknowledgements
cannot release a fresh frame. Caller output setup/process/cleanup are bounded, panics/errors are retained as
redacted diagnostic categories, and primary/pipeline/cleanup results remain
separate. Forced core cancellation is never reported as successful output.

Core calls `prepare_end` and drains before delivering either End or Stop.
Therefore the provided `ingress_guard()` must be immediately upstream of a custom
standalone processor topology to signal Stop before that drain. The public app
always installs it and its typed Stop/Cancel handles signal cancellation directly.
Custom topology must also place `provider.output_guard(caller_output)` immediately
downstream. Its successful setup activates output credit; merely constructing an
unused guard is insufficient, and absent active credit fails before invocation.
A lone processor without the ingress guard inherits core's graceful Stop drain; it does
not claim pre-drain Stop cancellation. Cancel/Stop only end local observation;
a hosted agent might already be acting and is not proven remotely stopped.

## Evidence and nonclaims

Pinned behavioral source: `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/services/aws/agent_core.py`, blob
`b1a1dacadbfe539978ea0bcc198dd329402fcef6`.

Independent official wire reference: `boto/botocore` 1.43.109, immutable commit
`22be6841647761fb4ae0f8bef6b1a55e882039f5`; runtime model, endpoint rules, REST
serializer, SigV4 and idempotency handler. This is a selected wire reference,
not a claim that the pinned Python dependency lock resolved that version.

Local tests cover synthetic loopback protocol/application/compiled-process
behavior and independent signing vectors. They do not certify live AWS account
access, hosted TLS success, ARN existence, remote reasoning/tools, AWS permission,
remote cancellation, persistent memory, account/session creation, MCP/OAuth,
credentials discovery/refresh, speech/audio, browser/PSTN/WAN, deployment,
project generation, full Python parity or full-workspace/Direct/ONNX/cloud parity.
In-flight kernel TCP connect cancellation requires a deterministic fixture and
is not implied by pre-connect, withheld TLS/header/body cancellation tests. A
response-head timeout alone is not evidence of a stalled request-body write. A
separate real-app fixture constrains only its loopback peer receive window,
observes actual body arrival, then verifies incomplete delivery versus declared
Content-Length after both cancellation and request deadline, with no replay.

A hosted-identity local fixture could not bind privileged loopback port443 (OS
PermissionDenied) and remains a failed observation, without retry or escalation.
The separate unprivileged HTTPS literal-loopback fixture withholds the handshake
and proves local TLS-future/socket cancellation only. It neither establishes
regional SNI nor replaces hosted connectivity/certificate-verification success.
Normal certificate verification is unchanged.
