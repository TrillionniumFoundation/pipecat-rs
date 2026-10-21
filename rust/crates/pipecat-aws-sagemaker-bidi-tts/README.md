# Finite NVIDIA SageMaker BiDi TTS

Native Rust Magpie text-to-PCM synthesis using SageMaker HTTP/2 bidirectional
EventStream on port 8443. This is **not** the ordinary HTTP InvokeEndpoint TTS
profile in `pipecat-aws-sagemaker`, nor the NVIDIA STT protocol.

## Product path

`SageMakerBidiTtsApp::new(config, Vec<String>, Arc<dyn FrameProcessor>)` validates
all utterances and startup envelopes before output setup or network effects.
`start()` returns a `RunningApp`; `completion().wait()` and `finish()` retain the
same real manager handle across cancelled or concurrent waiters. `interrupt()`
fences the active output immediately and sends a native `Interruption` frame.
`cancel()`, `stop()` and dropping the running owner terminate local work. The
completion handle remains available to obtain actual join evidence after drop.

Each admitted utterance has a fresh single-use `SageMakerBidiTtsProcessor`, native
pipeline, monotonically numbered context within the app, and H2 session. All
pipeline and transport owners are joined before the next utterance. The output
processor is setup and cleaned per utterance; it must support that lifecycle.
There is no retry, replay, reconnect loop or cross-utterance hidden owner.

The signed startup contains exactly `synthesize_session.update`,
`input_text.append`, and `input_text.commit`, as raw UTF8 `PayloadPart` events.
Defaults: `Magpie-Multilingual.EN-US.Aria`, `en-US`, 24 kHz mono PCM16. The initial
buffer waits for half a second of PCM (sample-rate bytes), then releases complete
samples as they arrive. Odd sample fragments are retained until the next event;
an odd final byte fails. `speech.completed` is required for normal success. A
valid short initial tail is flushed at completion, deliberately improving on
Python's unflushed short initial tail. Empty completion is separately reported.

Interruption drops buffered and queued old-context audio, sends `input_text.done`
and observes at most one response during bounded control grace (default 500 ms,
maximum 5 seconds), then sends `session.end` and closes the signed upload with
exactly one empty signed terminal when the connection permits. A response is
only an observation, **never proof that remote synthesis stopped**. Missing
completion can still yield local interrupted termination; forced aborts, cleanup
errors, malformed frames and provider errors never count as successful recovery.
Late old-context PCM is discarded. The next session is for new text only.

The app deadline bounds active media/network work; connect, write, read, output,
and control operation budgets are clamped to remaining time at session admission.
Cancellation at the absolute app deadline stops further work. Real owner joins
may consume an additional finite cleanup grace (transport close budget and core
shutdown budget); this is not represented as an extension of successful media
work. Startup is awaited through its own bounded completion, never dropped by an
outer timeout. A user output implementation can perform arbitrary side effects
before its future is cancelled; cancellation cannot undo already accepted audio.
Delivered bytes mean its `process()` returned successfully, not physical playback.

## Routing and limits

Credentials are explicit immutable `AwsCredentials`; no AWS SDK, ambient credential
chain, model download, DNS resolver, proxy or arbitrary URL. Hosted connections
require a caller-resolved numeric IP:8443 and verify the regional TLS identity
and h2 ALPN. Literal-loopback HTTP exists only for deterministic fixtures.
Commercial AWS regions only. No optional model-routing headers are sent.

Text uses Python-compatible strip/isalnum from the retained Unicode 15 table.
Empty/punctuation-only text is skipped without a socket. Defaults: 16 utterances,
16 KiB per text, 64 KiB per response payload, 8192 response events per session,
8 MiB delivered audio across the app, and 300-second app deadline. Invalid UTF8 or
JSON payloads fall back to raw PCM as in the source; even bytes do not prove audio
format or quality. Valid JSON scalars/wrong field types, invalid base64, oversized
or deeply nested payloads, CRC/framing failures and modeled AWS errors fail with
static redacted error categories. The preallocation depth bound is intentionally
stricter than Python even for a binary payload that happens to contain brackets.

## Normal executable

`cargo build --offline --locked -p pipecat-aws-sagemaker-bidi-tts --bin sagemaker_bidi_tts`

`sagemaker_bidi_tts --run INPUT.json OUTPUT.pcm --timeout-ms 30000 --max-output-bytes 8388608`

Input is a nonempty JSON array of strings, for example `["First utterance", "Second
utterance"]`. Help and `--check-config` do not access credentials, files or network.
Use the `PIPECAT_SAGEMAKER_BIDI_TTS_` prefix for `ENDPOINT_NAME`, `REGION`, exactly one
of `CONNECT_ADDRESS` or fixture-only `LOOPBACK_ORIGIN`, optional `SETTINGS_JSON`
(`voice_name`, `language_code`, `sample_rate_hz`), and run-only `ACCESS_KEY_ID`,
`SECRET_ACCESS_KEY`, optional `SESSION_TOKEN`. No real credentials are needed for
loopback tests.

Linux file mode rejects existing outputs, symlinks, ancestor symlinks, traversal
and input/output aliases before sockets. It pins/verifies input and destination
identity and uses create-new mode0600 output. PCM is bounded in memory until real
joins, then written/flushed; stderr contains a redacted JSON terminal report with
per-context progress. Disk operations can outlive network budgets; no fsync,
crash durability or physical playback is claimed. The library interruption API
is not a fabricated CLI playback/control capability.

## Qualification scope

The source includes 30 library tests (27 retained private H2 transport tests and
3 new text/wire/PCM protocol tests), 1 independent HMAC/CRC signature oracle test,
11 runtime acceptance tests and 4 ordinary executable tests. The runtime fixtures
cover two sessions, streaming before completion, threshold/tail/fragmentation,
raw and base64 PCM, interruption before audio/during buffering/during blocked
output, recovery without stale bytes, missing remote completion, late errors,
cancel/stop/drop, concurrent and cancelled completion waiters, output panic and
cleanup error, malformed responses and absolute deadline. CLI tests exercise the
real normal binary and actual filesystem output, not a test-only replacement.
`PIPECAT_SAGEMAKER_BIDI_TTS_TEST_BINARY` optionally supplies the exact normal binary
for qualification; otherwise Cargo's binary artifact is used. No tests are ignored.

Source inclusion is not a passed-test claim. See the externally retained batch154
validation report for exact qualified source hashes, commands and results. No live
AWS/model, microphone/audio hardware, full voice example, ASR/LLM wiring, provider
quality, word alignment, heard history or full persistent Python class parity is
claimed. The ordinary HTTP and STT crates remain unchanged.
