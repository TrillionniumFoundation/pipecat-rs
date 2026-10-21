# Finite NVIDIA SageMaker HTTP TTS

Standalone native Rust application of Python's `NvidiaSageMakerHTTPTTSService`:
complete utterances → SigV4 `InvokeEndpoint` → incremental mono PCM16 → a bounded
caller processor or a new recorded PCM file. The production `sagemaker_tts` binary
uses the same public `SageMakerTtsApp` as the integration tests.

This does **not** migrate the source example's bidirectional TTS voice bot. There
is no ASR, LLM, transport session, sentence/token aggregation, word alignment,
assistant history, playback, resampling, or remote-cancellation claim.

## Library

Construct explicit `AwsCredentials`, then
`SageMakerConfig::new(region, endpoint_name, credentials)`. Use exactly one:

- `with_connect_address(numeric_ip_port_443)`: regional HTTPS. The connection uses
  this already-resolved address, retaining `runtime.sagemaker.REGION.amazonaws.com`
  for Host, SNI, certificate verification and signing. There is no runtime DNS.
- `with_loopback_endpoint("http://127.0.0.1:PORT")`: literal-loopback fixtures only.
  HTTPS loopback is also allowed with normal certificate verification, never an
  insecure verifier. Hostnames, custom remote endpoints and arbitrary paths fail.

Commercial regional profile only; no partition/FIPS/dual-stack/VPC resolver. The
credentials are immutable and explicit, with no SDK, profiles, metadata, SSO,
refresh, environment fallback, retries, redirects, proxy, or shared client pool.

`with_settings(SynthesisSettings)` selects voice, language and rate. Defaults are
`Magpie-Multilingual.EN-US.Aria`, `en-US`, 24000 Hz. Fixed model metadata is Magpie;
`model` is not sent as a JSON field. Voice is bounded ASCII alphanumeric/`._-`,
language bounded ASCII alphanumeric/hyphen, and rate 8000..96000 Hz. These are
native validation limits, not proof a hosted model supports every accepted value.

`SageMakerTtsApp::new(config, Vec<Utterance>, output)` validates the entire finite
input before output setup or networking. It checks text, serialized request and
aggregate retained/pending bytes, and clones only live lengths, not spare caller
capacities. `start().await` returns a running handle without awaiting startup;
a retained manager owns startup itself. `completion()` is a cloneable read-only
observer. `finish().await`/`completion.wait().await` await the actual manager and
cache the result without surrendering the sole JoinHandle to canceled waiters.
Dropping the running handle cancels local work. Keep a completion observer when a
terminal join receipt is needed. Observers do not keep the public running handle
alive. Cancel/Stop/drop cannot prove a model stopped remotely.

For embedding the processor directly, install
`[processor.ingress_guard(), processor, processor.output_guard(caller)]` in this
order. It accepts complete `TtsSpeak` frames, not incremental LLM text. Typed
`TtsUpdateSettings` updates voice/language only for future admissions; accepted
requests keep immutable snapshots. Legacy keys are `voice` and `language`, or a
typed `TtsSettings` delta, never both. Unknown/null/model/rate/endpoint/credential
or targeted settings fail atomically. Other ordinary frames pass through.

## Wire and output contract

- POST `/endpoints/{EndpointName}/invocations`, signing service **sagemaker**, not
  `sagemaker-runtime`. Ordinary signed `Accept: application/octet-stream`, not
  the response-stream header or AWS EventStream protocol.
- JSON has exactly `text`, `voice_name`, `language_code`, `sample_rate_hz`.
- Source strip/alphanumeric gating uses a generated CPython 3.12.14 Unicode15.0
  table, not Rust's different Unicode classification. No Python executes at
  runtime. Empty, punctuation, emoji and combining-only input skip without HTTP
  or TTS frames. U+001C..U+001F whitespace behavior is included.
- HTTP200 must include one `application/octet-stream` Content-Type and a nonempty
  body. Duplicate/ambiguous/encoded headers, response trailers, truncation,
  oversized bodies and final odd sample bytes fail. This MIME hardening and
  empty-body rejection are deliberate native differences from Python's SDK loop.
- Read arbitrary HTTP fragments incrementally, retain at most one odd sample
  byte plus a 12-byte signature probe, and emit only whole PCM16 mono samples.
  Each emitted allocation is bounded by `max_frame_bytes`. Source recommends
  half-second chunks; transport/scheduling boundaries are not promised identical.
- WAV/FLAC/Ogg signatures are rejected when detectable, possibly after an actual
  PCM prefix has already been delivered. Arbitrary even bytes cannot be proven
  to be audio rather than another format. The rate labels remote bytes; it is
  not measured remote sample-rate or audio-quality evidence.
- `TtsStarted`, `TtsAudio` and `TtsStopped` share a local context ID. The direct
  processor preserves the caller's `append_to_context` flag on Start. The finite
  app explicitly sets false and emits no synthesis-text/history frames. A Stop
  closes a failed started context when output still accepts it; it is not a
  successful-synthesis receipt. Cancellation/interruption discards stale work.
- Each generated frame waits for the actual bounded caller `process()` result.
  Queue admission and acknowledged bytes are separate. Output credit prevents
  a slow sink from causing a core queue-overflow strategy. One invocation runs
  at a time; no following invocation starts before real EOF and final output ack.
- The HTTP driver is polled inline alongside the response/output future. No
  detached driver, DNS, connection pool or receive-owner task exists. Successful
  return requires the connection driver's natural return. Error/cancel destroys
  that locally owned socket future; no remote rollback is asserted.

## Limits and terminal reports

Defaults:64 utterances,64KiB raw text per utterance,256KiB serialized request,
8 pending invocations,1MiB aggregate pending payload and separately1MiB retained
text,8MiB per-response bytes,16MiB session PCM,24,000-byte PCM frames,16KiB HTTP
parser/header budget. All configurable limits have fixed ceilings. Data-frame
admission is capped at262,144 generated frames; bounded Start/Stop controls have
reserved allowance (at most two per bounded utterance). Hyper's bounded HTTP1
read buffer is separate from the sample remainder and output queue. A caller
output is trusted code: its own allocations or external effects are not sandboxed.

Independent deadlines cover connection/TLS, request/head, peer body idle, output,
absolute invocation and cleanup. Local output waits do not consume peer-idle
time. Pipeline drain has a finite whole-session boundary. Stop is fenced before
provider prepare-end; Cancel interrupts, End drains. Caller setup/process/cleanup
failures and panics are sanitized. Output cleanup is attempted once, including
Stop (where core otherwise intentionally skips processor cleanup).

`SageMakerReport::is_success()` requires End, actual pipeline join, no primary,
pipeline, secondary-cleanup, forced or unproven outcome, and exact successful
invocation-owner accounting. All-skipped input is a successful **no-audio** result,
not synthesis success; the CLI calls it `all_skipped`. `admitted`/`owners_released`
count real invocation owners only, not skipped entries. Each admitted owner has
one completed/failed/canceled terminal classification. Partial means a failed or
canceled invocation delivered some actual PCM. A manager panic conservatively
reports `join_unproven`, never an invented clean join.

`admitted_text_characters` counts locally admitted stripped text, including later
canceled queued work. `usage_text_characters` counts request attempts that returned,
including failed attempts, not provider/billed consumption. TTFB measures the
request path to the first nonempty body byte, excluding initial caller Start wait.
Counts and timings are diagnostics, not AWS account receipts.

## Production executable

`cargo build -p pipecat-aws-sagemaker --bin sagemaker_tts --offline --locked`

Default/help connect nowhere and read no credentials/files. `--check-config`
checks nonsecret route values with synthetic credentials without reading secret
environment variables or creating files. Active execution is explicit:

`target/debug/sagemaker_tts --run INPUT.json OUTPUT.pcm [--timeout-ms N] [--max-output-bytes N]`

JSON is strict UTF8, at most1MiB, with top-level `utterances:[{"text":"Hello"}]`
and optional `voice_name`, `language_code`, `sample_rate_hz`. Unknown/duplicate
fields, null settings, excessive depth/count/bytes and any invalid utterance fail
before output creation or network effects.

Explicit namespaced inputs: `PIPECAT_SAGEMAKER_REGION`, `ENDPOINT_NAME`,
`ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, optional `SESSION_TOKEN`, and exactly one
`CONNECT_ADDRESS` or `LOOPBACK_ORIGIN`, all with the same `PIPECAT_SAGEMAKER_` prefix.
Never put real credentials in source, fixtures, shell history or example files.

Linux file handling retains input/output/parent descriptors and validates every
ancestor identity; it rejects symlinks, aliases, traversal, existing output and
path substitutions. The file is new mode0600. The sink records PCM into a bounded
preallocated memory buffer while networking; only after real app owners join is
it persisted through the original retained file FD. A failed path identity may
still leave a correctly identified original partial file, but the run fails.
Memory-delivered versus file-recorded counts are distinct in sanitized stderr JSON.
An unsuccessful run is `partial` only when real PCM reached memory or the file;
zero-audio failures are `failed`, while cancellation-only terminals are `canceled`.
Files never contain status JSON, credentials, text or provider error bodies.
Normal regular-file I/O/finalization is outside the async network deadline and
may block; no physical playback or crash-durability guarantee is made.

Failures/nonzero exit retain identified partial files. Zero requires successful
terminal app state, owner proof, unchanged input and successful original-file
recording/finalization. No overwrite, stdout PCM, live AWS qualification, full
workspace qualification, deployment, permission change or facade export is implied.
