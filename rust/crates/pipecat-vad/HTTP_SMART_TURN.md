# HTTP Smart Turn: bounded native adapter

Enable `pipecat-vad/http-smart-turn` (or facade `pipecat/http-smart-turn`).
The feature is off by default. `HttpSmartTurnConfig::new(endpoint, sample_rate,
params)` and `HttpSmartTurnPredictor::new(config)?.into_analyzer()` construct the
existing `SmartTurnAnalyzer`, usable by the existing `SmartTurnUserAggregator`.
No local ONNX model or runtime is loaded by this adapter. Local ONNX inference,
its probability threshold, buffers, and turn-controller rules are unchanged.

## Fixed source and a reproduced scheduling defect

The reference is `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- `src/pipecat/audio/turn/smart_turn/http_smart_turn.py`, blob
  `0d4b717b3ade5b3f7b024aee8236897a73f6f4aa`
- `src/pipecat/audio/turn/smart_turn/base_smart_turn.py`, blob
  `9d9debbac7c93e55dec03cbfc84b5365b18df4ef`

Running these unchanged classes against a synthetic loopback server reproduces
an upstream scheduling defect: BaseSmartTurn dispatches `_predict_endpoint` to
`run_in_executor`, but the HTTP implementation calls `asyncio.get_running_loop()`
inside that worker. The call reports `no running event loop`, sends zero HTTP
requests, and falls back to INCOMPLETE/probability zero. The retained probe stubs
only logging/deprecation imports. A separate diagnostic bridge to the actual
owner loop exercises the source's intended request and fallback behavior.

This native adapter fixes that scheduling defect and implements the intended
HTTP/NPY contract. It is not equivalent to the source's broken default execution.
The probe also establishes that source `stop_secs=0` disables the HTTP timeout,
and that some non-200 responses, including 201 and a 302 without Location, can
still parse successfully. The stricter native profile below is deliberate.

## Payload and result

The existing buffer selects pre-speech history and the maximum-duration tail.
The adapter encodes that selected mono PCM16 snapshot as a NumPy `.npy` 1.0 array:
rank one, C order, little-endian float32, each sample divided by 32768. There is
no eight-second padding, log-mel transform, resampling, compression, or Python
runtime dependency. Rate/channel metadata is not sent. The server must expect
the explicitly configured mono rate; this adapter does not hard-code 16 kHz.

A POST carries `Content-Type: application/octet-stream`. Successful responses
must have status 200 and a JSON or `+json` media type. A valid JSON object must
contain integer `prediction` equal to 0 or 1 and finite numeric `probability`
in [0,1]. `prediction == 1` decides COMPLETE independently of probability;
probability is used as a metric. Server-provided timing/metrics are ignored.
Missing or invalid schema is an explicit, content-free error, consistent with
the source BaseSmartTurn reading required fields outside its transport fallback.
The integer/domain restrictions are a stricter native profile.

Network, status, MIME, truncated-body, size-limit, and invalid-JSON failures,
and an elapsed HTTP deadline, return INCOMPLETE with probability zero. These
are inference results with local elapsed metrics, not `SmartTurnTimeout`:
the latter means COMPLETE in the existing local analyzer. The factory therefore
sets the outer analyzer inference timeout to None. Callers who explicitly
change that timeout change this policy. An empty buffer remains INCOMPLETE
without metrics or a request. Cancellation returns `VadError::Cancelled`;
it does not fabricate an incomplete result or a completed turn.

## Ownership, limits, and security profile

The initial profile accepts only explicitly configured IPv4/IPv6 literal HTTP(S)
endpoints. Hostnames are rejected. This avoids system DNS blocking jobs escaping
request cancellation or extending runtime destruction indefinitely. HTTPS keeps
normal certificate validation and requires a matching IP subject alternative
name; no insecure TLS option is added. Typical hostname-based hosted endpoints
are not supported by this initial profile.

URLs with embedded credentials or fragments are rejected. Limits are 2 KiB for
the URL, 64 custom headers totaling 8 KiB of names and values, 8 MiB for the
encoded request, and 64 KiB for the response body. The configured maximum audio
window must fit the request limit. Routing/framing headers cannot be overridden;
Content-Type may be explicitly overridden as in the source. Proxy discovery, redirects and
HTTP retries are disabled. Config debug output and fixed failures omit endpoint,
header values, response data and server diagnostics. Custom headers are explicit
caller configuration; the tests use only a synthetic value. No credentials are
created, discovered or saved. Endpoint selection itself is trusted caller
configuration, not a URL accepted from a frame or server response.

`stop_secs` must be finite and strictly positive. Each synchronous prediction
creates its own current-thread Tokio runtime and HTTP client on the existing
owned inference thread. The request/body read is bounded by the total timeout;
cooperative cancellation is checked at most once per 10 ms timer interval while
waiting (subject to scheduling). NPY serialization also checks each 4096 samples. The client/runtime are
released before returning to the serial inference worker. Nothing captures the
outer executor or stores a runtime inside the reusable predictor. Direct calls
to synchronous `predict` from inside a Tokio runtime are rejected rather than
nesting runtimes. Use the analyzer from async application code.

The existing cleanup cancels outstanding snapshots and joins the actual worker;
normal reuse happens after cleanup completes. Dropping the analyzer also cancels
and joins. Cancellation is cooperative, not arbitrary synchronous-code
preemption. Finite NPY construction and runtime setup precede HTTP polling;
no wall-clock real-time deadline is promised under executor/OS starvation.
Per-request runtime/client construction means no connection-pool reuse across
predictions. Local elapsed metrics include native normalization/runtime setup;
the Python timing starts after normalization. Both exclude inference-queue wait.

## Verification scope

Synthetic loopback tests exercise actual POST bytes and response parsing,
completion/probability independence, malformed and failed replies, header/body
timeouts, cancellation and socket closure, joined cleanup/reuse, redirect refusal,
and the existing pipeline/context integration. Fixed-source Python probes retain
the scheduling-defect reproduction and intended raw request observations.
These tests do not authenticate a real provider, test public networking/TLS,
qualify a model, change local ONNX thresholds, or resolve unrelated cloud CI.
