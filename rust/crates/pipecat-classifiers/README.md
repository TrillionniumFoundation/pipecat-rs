# Native typed classifiers

`pipecat-classifiers` is an independent optional runtime component. It contains
no Python runtime, pipeline processor, UI-worker integration, voicemail timer,
or evaluation runner. Import the crate directly, or use the facade feature when
available in the containing workspace.

## API and ownership

`Classifier::ask` takes structured state plus a `BTreeMap` of mixed named
`YesNoQuestion`, `ChoiceQuestion`, and `ScoreQuestion` values. `yes_no`, `choice`,
and `score` return typed batches. Every successful batch contains elapsed-time
metrics and explicit probability provenance. Metrics are returned inline: there
is no unbounded metric/event queue, callback, or background task.

`LlmClassifier::new(Arc<dyn LlmInference>, config)` performs one isolated native
inference, supplying a fresh user message, empty tools, and `ToolChoice::None`.
System instructions and max tokens use the existing per-call settings override.
The provider processor's conversation snapshot is not modified. The classifier
does not own or close the injected inference service. No tool callbacks execute.

The generated named JSON schema is available via `response_schema`. Its complete
derived node/depth count is admitted before Value-tree construction, and its
encoded bytes share the remaining prompt/instructions request budget. The current
`LlmInference` interface has no response-schema parameter, so the schema is also
included in the prompt. This is **prompt-level guidance plus local parsing**,
not a claim of provider-enforced structured output. LLM probabilities have
`LlmEstimate` provenance and are not calibrated. Token usage is `None` because
the existing inference interface does not return it; no token estimates are
invented.

`JevClient` uses `POST /v1/systemone` with model/state/named questions and bearer
authentication. Its clones share an HTTP/2-capable reqwest pool (240-second idle
expiry), bounded admission, and checked cumulative usage. `connect` optionally
warms the pool with `GET /v1/models`, serialized to one successful warmup. A
failed warmup is returned explicitly; it does not prevent a later ask/connect.
`close` is permanent, cancels active requests, rejects queued/new calls, and
drops the stored pool handle. It is idempotent. In-flight pool clones are released
as cancelled futures are polled or dropped. No detached request task survives a
dropped call.

`JevClassifier::owned(config)` closes its client on cleanup and Drop.
`JevClassifier::shared(client)` never closes the shared client on cleanup/Drop;
the client's owner must close it. Any explicit `JevClient::close` affects all its
clones. Successful responses preserve provider score/confidence/probability
fields and receive `JevProvider` provenance; this does not certify a hosted
model's calibration or availability.

## Bounds and security

Default budgets: 1 MiB for a Jev encoded request or LLM prompt plus instructions,
1 MiB response, depth 32, 32,768 JSON nodes,
64 questions, 255 choices/score levels, 256-byte names, 16 admitted concurrent
calls. Configurations are validated against finite hard ceilings. Choice counts,
structured input shape, and aggregate text/node/depth budgets are checked before
encoding or network activity. The final Jev JSON envelope is checked again against
depth and node budgets, including null option descriptions and protocol fields.
Final Jev encoded-request and full response byte limits
are checked separately. Streamed responses are capped even without Content-Length.
A serde DeserializeSeed visitor admits depth/nodes before constructing each JSON
value; serde retains full grammar validation. Duplicate object keys are rejected
before their values are constructed, including conflicting answer/verdict fields.

One total deadline includes admission, transport, response reading and retry
sleep; an earlier absolute caller deadline can only shorten the configured
(default ten-second) bound. Cancellation is biased ahead of response delivery.
Only explicit HTTP 429/529 responses are retried, up to configured max retries
(default three, hard maximum eight), at 250, 500, 1000, then 2000 ms. Transport
errors, timeouts and other statuses are never replayed. Reqwest's implicit retry
policy is disabled. No retry response body is buffered.

Endpoints must be HTTPS origins without userinfo/path/query/fragment; HTTP is
allowed only for explicit loopback peers. Proxies and redirects are disabled;
authorization is sensitive-header-marked. Debug implementations omit keys,
endpoints, model labels, and instructions. Errors retain static categories/status
codes rather than provider text, caller text, URLs, or authentication values.

## Source behavior and deliberate differences

Pinned source: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

- Fenced, surrounding-prose, and single-unwrapped LLM JSON answers are accepted
- LLM yes/no values are clamped to [0, 1]. Choice probabilities default missing or
  unusable entries to zero, retain unnormalized source values, and derive
  confidence from the chosen entry. Score probabilities are clamped then
  normalized; score is their weighted position and confidence the largest share.
  The derived LLM score is clamped to the closed level range to prevent floating-
  point endpoint overshoot; normalized probabilities remain unchanged
- Jev uses `noul`, `choice`, and `score` wire types. Absent yes/no criteria are
  omitted; a present empty criterion maps to an empty string. Choice `None`
  descriptions remain JSON null. Score probabilities follow level positions,
  even when levels are structured JSON objects
- Jev usage absent or non-object is zero, as in the source. Invalid counters inside
  an object are errors. Counters and their sums are unsigned and checked; overflow
  fails without corrupting cumulative totals. Billable valid usage is counted
  even if named answers are subsequently malformed, as in the source
- Numeric strings are accepted if finite. Boolean numeric coercion is rejected.
  NaN/infinity and malformed/nonfinite JSON fail. Jev probabilities/confidences
  outside [0, 1] and scores outside the level range fail rather than being passed
  through. Non-object Jev probability maps and mismatched explicit answer types
  fail. Score levels require 2..=255; choice requires 1..=255. Empty batches and
  names are rejected. These are intentional tightenings of Python permissiveness
- Rust named maps have deterministic lexical ordering, rather than Python's
  insertion ordering. Scale order remains exactly the caller's vector order
- Setup returns warmup errors rather than silently logging and swallowing them
- Native JevClient takes the same typed question batch as Classifier; raw
  arbitrary protocol question dictionaries are not a second public input API

Synthetic local sockets exercise protocol/arithmetic/lifecycle behavior. They do
not qualify live Jev credentials, model availability/calibration, production
accuracy/latency, or HTTP/2 negotiation against the hosted service. The native
inference test uses a real `LlmProcessor` against a synthetic OpenAI-style SSE peer.

The injected LLM service retains its own transport/retry configuration; the LLM
classifier itself never retries. The Jev transport's 429/529-only rule is enforced
inside this crate. A caller-supplied implementation of `LlmInference` must uphold
that trait's no-tool-execution and owned-future cancellation contract.

The LLM byte budget bounds the prompt/instructions passed to `LlmInference`.
Provider-specific JSON escaping, headers and service settings are owned by the
injected native service and are not counted as classifier prompt bytes.
