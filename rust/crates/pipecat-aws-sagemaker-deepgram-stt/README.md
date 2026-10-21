# Finite Deepgram SageMaker STT

A standalone, single-session native PCM16 processor, retained-owner application,
and Linux production CLI. This is a bounded deployment profile, **not full
Python service, SDK, hosted AWS, or whole-project replacement parity**.

## Contract

- Explicit immutable credentials and commercial regional SageMaker HTTP/2 TLS
  identity on port8443, with one caller-resolved numeric socket address. Literal
  HTTP loopback is for local fixtures only. No runtime DNS, proxy, credential
  discovery/refresh, retry, reconnect or replay.
- `POST /endpoints/{endpoint}/invocations-bidirectional-stream`; no URI query.
  The new common helper signs `x-amzn-sagemaker-model-invocation-path=v1/listen`
  and `x-amzn-sagemaker-model-query-string`. Both headers seed the one owned
  chained EventStream signer. Routing header debug values are sensitive.
- No startup event. Real first audio can upload before response headers. Each
  chunk is one raw `BINARY` PayloadPart, never base64 or NVIDIA append/commit.
- `KeepAlive`, `Finalize` and `CloseStream` are UTF8 JSON PayloadParts. The first
  five-second timer starts at accepted response head, independently of the next
  audio. Its single retained writer prioritizes due timers over queued audio.
  The next timer is five seconds after a successful write, without catch-up.
  A physically blocked H2 write cannot be preempted by another signed message.
- End fences public input and timer starts. A KeepAlive already linearized under
  the short shared fence may finish before CloseStream. No timer/control follows
  CloseStream or the single signed empty terminal. Cancel/Stop/drop retain and
  join the actual writer, reader, H2 driver, pipeline and output cleanup.
- Each audio/control command has a bounded one-shot **local DATA admission**
  receipt. This is not a peer acknowledgement. App chunk markers wait for the
  receipt, then pause in real time, then optionally send VAD-stopped. Cancellation
  interrupts this pause through the independently retained app manager.
- A nonblank result containing `channel` is eligible regardless of `type`.
  `is_final` selects final/interim frame and accounting. `finalized` is true only
  for a requested, confirmed, nonblank final and consumes the pending boolean.
  Ordinary finals and interim/blank results do not consume confirmation.
  Multiple requests coalesce, not a FIFO of expected transcripts.
- Response language is the first alternative's first language, checked against
  the exact pinned Python Language set. Missing/empty list means None, never a
  fallback to configured language. Unknown or malformed language, invalid UTF8,
  AWS exceptions and malformed wire fail with sanitized categories. Invalid
  JSON/unknown informational messages may be ignored under event bounds.

## Immutable settings

Defaults: `model=nova-3`, `language=en`, `sample_rate_hz=16000`, empty user_id;
false detect_entities/diarize/dictation/numerals/smart_format; true
interim_results/punctuate. Optional mip_opt_out is omitted unless a boolean is
specified; explicit null fails. Raw query order is:

```
detect_entities=false&diarize=false&dictation=false&interim_results=true&numerals=false&punctuate=true&smart_format=false&model=nova-3&language=en&encoding=linear16&channels=1&multichannel=false&sample_rate=16000
```

Models are 1..256 ASCII alnum/dot/underscore/hyphen; languages 1..63 ASCII
alnum/hyphen, preserving region/case; input rates8000..48000; user_id<=256 bytes
without controls. No percent transform is required for this alphabet. Richer
Python arrays, extra/NOT_GIVEN/None, LiveOptions merge, endpointing, keywords,
redaction, diarized frame expansion, multichannel and settings reconnect are
outside this slice. Updates to a running processor fail atomically.

## App

`SageMakerSttApp::new(config, pcm, output)?.with_timeline(Timeline(markers))?`
validates all PCM and timeline before starting the pipeline/network. Empty
Timeline is fast finite-file mode and sends no implicit final VAD. A marker has
`after_chunk`, `pause_ms`, `vad_stopped`. Boundaries are strictly increasing and
unique, start at1 and cannot exceed the actual chunk count. Up to64 markers,
0..15000ms each pause and60000ms total. Combine pause/VAD in one marker.

Defaults: PCM<=8MiB,120s,3200-byte chunks,2400 chunks; <=64 explicit Finalize
commands;8192 response events;64KiB result,32 JSON nesting,1MiB transcript,
8MiB serialized frame output. Owned PCM/timeline/settings allocations are
normalized to actual length. KeepAlive count is additionally bounded by the
absolute session deadline (<=420s and a fixed five-second period).

Default deadlines: connect/head10s, write10s, close10s, response-idle90s,
absolute session300s, output5s. Outgoing KeepAlive does not refresh response
idle. Timeline admission requires idle>=total pause+15s and
session>=total pause+connect+write+close+15s, with checked duration arithmetic.
Persistent flow-control blockage can still exhaust those finite deadlines.

Reports distinguish captured/admitted/written PCM, admitted/written chunks,
Finalize admission/writes, KeepAlive attempts/writes, observed and delivered
final/interim frames, partial failure, silence and actual join status. Dropping
one completion wait does not discard the retained handle; concurrent waiters
observe the cached report. Local cancellation cannot establish remote billing
or processing status. Forced task abort is reported, never counted as success.

## Production CLI

Exactly:

```
sagemaker_deepgram_stt [--help | --check-config]
sagemaker_deepgram_stt --run INPUT.pcm OUTPUT.jsonl [--settings SETTINGS.json]
```

Environment prefix is `PIPECAT_SAGEMAKER_DEEPGRAM_`: ENDPOINT_NAME; REGION
(default us-west-2); exactly one CONNECT_ADDRESS or LOOPBACK_ORIGIN; run-only
ACCESS_KEY_ID, SECRET_ACCESS_KEY and optional SESSION_TOKEN. Help/check perform
no file/network access and do not read real credentials. There is no ambient
AWS chain or alternative environment settings JSON path.

Optional <=16384-byte settings manifest:

```json
{"settings":{"model":"nova-3","language":"en"},"timeline":[{"after_chunk":1,"pause_ms":12000},{"after_chunk":2,"vad_stopped":true}]}
```

Object shapes, unknown/duplicate fields, nulls, arrays in place of objects and
out-of-range controls fail before output creation or network. Input is bounded
nonempty whole little-endian mono PCM16, not WAV. Rate/byte alignment are labels
and validation, not audio-quality evidence. Input/settings descriptors and each
path ancestor are retained and verified. Symlinks, traversal, settings/input
hardlink aliases, existing output or output aliases fail. New output is0600,
no overwrite. Replaced paths are not written as the original target.

JSONL records mark final/interim by variant, with finalized as a separate field.
Records and the application summary are buffered <=8MiB and only persisted after
real network/pipeline joins. Input/settings identity failure cannot report
success. Disk I/O can outlive network deadlines; no fsync/crash durability claim.
Exit0: successful transcript or silence (distinguished by summary); exit1:
partial/session/recording failure; exit2: preflight/startup/summary failure.

## Validation status and required gates

This source slice was prepared under **source-only admission**. No Cargo,
rustc, rustfmt, test, Clippy, normal-binary build or runtime gate was run as part
of that admission. Test source is not a passing result.

Targets: lib; signature_vectors; runtime_acceptance; cli. Real12s pause tests
exercise app and production CLI, and paced audio exceeds5s. The CLI test binary
can be overridden with `PIPECAT_SAGEMAKER_DEEPGRAM_STT_BIN` for an independently
built NORMAL executable; harness binary alone is not production qualification.
Independent request/HMAC/CRC parsing is in tests/common, with no production
encoder used for expected incoming wire. Private transport negative probes
also use synthetic startup frames and short limits; they do not prove the
public zero-startup or real-period contract.

Pending funded gates include formatting, production/test Clippy, all targets,
normal isolated build and exact-SHA replay, common lib/event_stream_signing/
agentcore_signing/new sagemaker_event_stream_signing, existing152 signature and
runtime/normal CLI smoke, and Nova Sonic shared-signer lifecycle consumers.
Do not claim full service/workspace parity from these selected gates.
