# Autonomous native text simulation

Build `pipecat-evals` with the opt-in `simulation` feature. Defaults, scripted
`run`/`suite` schemas and credential selection are unchanged. Run exactly one
scenario once:

```
pipecat-eval simulate scenario.yaml ws://127.0.0.1:8765/eval runtime.yaml
pipecat-eval simulate --help
```

The bot must implement existing RTVI text events, client-ready/bot-ready,
send-text and eval-configure full function reporting. The URL forces skip_tts
and strips audio/image/disconnect profile flags. No Python, audio, model pull,
provider fallback/retry, suite/repeats or images are implemented. Per-turn quality
criteria use the separate native judged_metrics list below. Unknown fields and
criteria/min_score inside measured metrics are rejected
before credentials, sockets or output. Files are bounded to 1 MiB. YAML expansion
uses the existing bounded loader. Typed `SimulationSession::new` does the same
preflight and `run(Cancellation)` owns the one-shot execution.

## Explicit configuration

```yaml
name: order mug
persona: An impatient customer who wants one blue mug.
goal: Order a blue mug.
success: The bot confirms the blue mug order.
max_turns: 20
max_duration_ms: 300000
max_silence_ms: 30000
closing_timeout_ms: 15000
handshake_timeout_ms: 10000
metrics:
  - {name: concise, measure: longest_bot_reply_words, max: 80}
  - {name: responsive, measure: slowest_text_response_ms, max: 2000}
  - {name: turn budget, measure: turns, max: 10}
  - {name: time budget, measure: duration_ms, max: 120000}
  - name: correct order
    measure: function_call
    function: place_order
    arguments: {color: blue}
```

Runtime endpoints and models are always explicit. `api_key_env` reads only the
specified variable when the selected run starts. Omit it or set null only for
an endpoint intentionally requiring no Authorization header. No keys enter
configuration Debug or result diagnostics. HTTP is loopback-only; otherwise
HTTPS is required. Credentials in URLs are rejected. Endpoint/provider bodies
are not included in execution errors.

```yaml
caller:
  endpoint: https://your-provider.example/v1/chat/completions
  model: your-caller-model
  api_key_env: MY_CALLER_KEY
  timeout_ms: 30000
# Optional, independently configured whole-conversation judge:
judge:
  endpoint: https://your-judge.example/v1/chat/completions
  model: your-judge-model
  api_key_env: MY_JUDGE_KEY
  timeout_ms: 30000
```

No inference begins before bot readiness and an actual greeting. Bot text is
user context to the caller; caller replies are assistant context. Evaluated
transcript roles are reversed: bot=assistant, caller=user. The caller receives
only the end_call tool and may make exactly one validated call with boolean
success and bounded reason. A goodbye alone does not hang up. Complete text is
sent once, never retried; an end_call with goodbye freezes future caller requests
and waits up to the closing deadline for one final bot reply. End_call without
text ends immediately. Tools are never executed by the harness.

Bot tool records are ordered by start and correlated by unique ID, not name.
Preamble text is retained while tools run and delivered together with the next
completed bot response after every tool stops, including an empty final response.
A stop alone does not fabricate completion. Duplicate/unknown IDs, malformed
arguments, tool mismatch, interruption and unexpected audio/image input fail.
The function_call metric requires an occurrence with recursively matching object
argument subset; cancelled and incomplete calls do not count. Unlike Python's
exact expected-call-set metric, unrelated extra completed calls are allowed.

## Truthful results and limits

`end_call.success` is only the persona's claim. A separate judge receives the
actual complete transcript and ordered tool records, not the persona's opinion.
It returns strict JSON `{ "verdict": "yes|no|none", "reason": "..." }`.
No judge is `unjudged`, never passed. Negative judgment and measured metric
misses are separate failure kinds; no-verdict/malformed judgment is an error.
Metrics with no relevant reply/latency evidence are null/unscored, not zero.
Latency measures first incoming text after a flushed caller send, not acoustic
latency. Turns count transmitted caller text, not an end_call-only invocation.
Duration starts before connection and ends at conversation cutoff; total elapsed
also includes cleanup and judging. Longest reply counts the aggregated bot turn.

Exit 0 means independently judged pass with no metric or execution failure;
exit 1 includes unjudged, failed and execution-error results; exit 2 means invalid
CLI/configuration. The result is JSON on stdout; no output artifact is created.

Turn, wall-clock and inactivity limits are distinct. Token/tool progress on both
sides refreshes inactivity, never the absolute duration. Cancellation is checked
through connection/readiness, streaming, sending and closing; Ctrl-C uses the
same owned cleanup. The separate judge deadline starts after bot/provider
cleanup and is not added to conversation duration. Cancellation does not launch
a judge. Python judges cap endings: this native route also retains independent
judge evidence on turn/duration/silence caps, but deliberately forbids overall
success for those endings. It never interprets an abrupt WebSocket close as a
successful bot hangup: this profile has no deliberate-bot-end wire evidence.

All history, input/output, tool counts and invocations have finite caps. The
provider request owner is cancelled and joined on every exit; a cleanup deadline
forces abort and then join and is reported as cleanup failure, not completion.
Socket graceful cleanup has a 2-second budget followed by the existing internally
bounded cancellation-and-join path (250-ms socket close timeout). Primary and
cleanup failures are both retained. Fresh sessions isolate context and results.

## Qualification

Tests use the actual production typed constructor and compiled CLI with real
loopback HTTP/SSE and WebSocket peers. Synthetic responders inspect live caller
context and actual transmitted turns; they are not a production canned persona.
No real credentials, paid requests, devices, SFU or model intelligence are qualified.
This slice does not claim full Python evaluation parity or full Pipecat parity.


## Native per-bot-turn quality criteria

Optional `judged_metrics` is separate from the unchanged measured `metrics`
list; this is deliberately NOT Python YAML drop-in compatibility:

```yaml
max_judged_turns: 128
judged_metrics:
  - name: courtesy
    criterion: Every reply addresses the caller politely.
    min_score: 0.8
  - name: disclosure
    criterion: A reply that offers an order discloses that payment is required.
```

Names must be nonblank, at most 128 bytes and unique across both lists. Criteria
must be nonblank and at most 32768 bytes. `min_score` is optional, finite, and in
[0,1]. Unknown/mixed measured fields are rejected. At most 64 criteria are allowed;
`max_judged_turns` defaults to 128, ranges 1..512, and criteria count multiplied by
that limit must not exceed 1024. This bounds at most 1024 criterion calls plus
one whole-goal call. These checks happen before credential reads or networking.
Exhausting the runtime turn bound is an execution error, never silent truncation.

A quality turn is completed bot text with no intervening caller or tool event.
Token chunks concatenate without added spaces; adjacent completed segments join
with one space. Tool boundaries split preamble and final replies even though the
caller and existing measured transcript still receive their original aggregation.
A preamble before tool start is retained at its actual arrival position, including
when its bot-llm-stopped comes after tool start. Only bot-llm-stopped admits complete
targets; token fragments or tool stop alone do not. Empty completion after a held
preamble does not invent an extra turn. An abandoned partial response is never a
target. Prefix bot entries are explicitly observed text, not completion claims.

The session keeps a byte/count-bounded ordered ledger and completed-target text
snapshots. No timestamp sorting is used, even when all event milliseconds are
equal. Tool start/stop observations are immutable, correlated by ID and include
only state known then. Each request carries one named criterion, 1-based turn,
prior ledger entries, and the complete target text. No later caller/bot text,
future tool completion/cancellation or caller end_call claim is supplied. Earlier
observed partial text may remain in later prefixes, labeled as observation rather
than completion. A fresh session has a fresh ledger. Native explicit tool-stop
boundaries and arrival ordering intentionally differ from Python simulation
caller's merged timeline; no exact Python end-to-end turn-count parity is claimed.

The independently configured judge first evaluates the whole goal using the
unchanged response contract. Criteria then run serially, metric-major/turn-minor,
under that SAME aggregate `judge.timeout_ms` deadline, beginning only after caller
and bot cleanup. Every admission rechecks cancellation and deadline synchronously.
There are no retries, fallback providers, concurrency or extra explainer calls.
Every stream is cancelled/joined. Transport, stream, deadline or cleanup failure
stops later calls, retains earlier results and leaves every expected remaining
turn as `none`, with diagnostics. Invalid completed JSON is recorded as `none`
and the next distinct question may proceed within the same budget; it is not a
retry. The goal-only route and exact goal parser are unchanged.

Turn answers are strict JSON with exactly `metric`, `turn`, `outcome`, and
nonblank `reason` (at most 4096 bytes). Outcomes are `meets`, `fails`,
`not_applicable`, or `none`. The first and third count yes. Score is yes divided
by ALL expected complete turns, never just returned answers. No turns means null
score/passed. No judge means unjudged outcomes and null score/passed, not a
fabricated pass. An omitted threshold is report-only (`passed: null`); malformed
or missing answers are visible diagnostics, not an independent report-only gate.
An actual infrastructure/cancellation failure still makes the run an error.
Threshold comparison is inclusive. A miss with a real `fails` is
`criterion_failure`; a miss caused only by missing judgments is
`criterion_no_verdict` and an error. Goal failures remain independent. No model
confidence/probability is fabricated from this text-response route.

`judged_metrics` results expose name, score, min_score, passed, failure_kind,
and every ordered turn's outcome, reason and diagnostic. This result field is
omitted for legacy scenarios without criteria. Existing measured fields,
serialized scenario defaults, CLI route, stdout JSON and exit rules are preserved.
This is still a single-run text simulation, not suite/repetition, audio/video,
real-model judgment quality, or full Python/Pipecat parity.
