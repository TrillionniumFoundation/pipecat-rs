# Native browser runner verification

Verified on 2026-10-09 with the workspace's Rust 1.99.0 toolchain and offline
cached dependencies:

- `cargo test --offline -p pipecat-runner -p pipecat-transports`: 34 tests passed
  (10 runner, 24 packet transport)
- `cargo clippy --offline -p pipecat-runner -p pipecat-transports --all-targets -- -D warnings`: passed
- `cargo test --offline -p pipecat-cli`: 23 tests passed, including native
  playback acknowledgement, stale playback invalidation, and model-backed
  Silero regressions
- `cargo run --offline -p pipecat-runner --bin pipecat-browser-agent -- --help`:
  passed with all three provider API-key variables explicitly removed

The cross-component test performs real HTTP `/api/start` requests, upgrades the
returned bound listener with the exact browser Origin, sends the browser crate's
RTVI/protobuf frames, streams through synthetic provider wire servers, and
projects the returned transcript with the browser crate's parser. Two sessions
verify fresh contexts. Input rate conversion is checked at the provider socket;
output audio bytes and 24 kHz format are checked at the browser-codec boundary.
No provider endpoint outside loopback is called.

Fixed source fixtures pin original protobuf tags and RTVI envelope fields.
Lifecycle regressions cover invalid Origin/path, admission limits, isolated stop,
unused-start expiry, handshaken-pipeline cancellation, socket closure, and host
shutdown. Input regressions cover sample-rate changes, stereo rejection,
transcript/control injection, and bounded cumulative textual context admission.

The browser executable is not part of this verification. AF_UNIX sandbox
restrictions and the cloud browser's separate loopback network prevent a
meaningful browser run in this environment. Microphone capture permissions,
physical speaker playback, live service credentials, and model quality are not
verified. Generated text and synthesis completion are never treated as playback
acknowledgement.

## Optional native WebRTC integration

Verified on 2026-10-09 with the same offline toolchain:

- `cargo test --offline --locked -p pipecat-runner --features webrtc`:
  17 runner tests passed, including all 10 WebSocket/default regressions
- `cargo test --offline --locked -p pipecat-webrtc`:
  30 native transport tests and one doctest passed
- `cargo clippy --offline --locked -p pipecat-runner -p pipecat-webrtc --features pipecat-runner/webrtc --all-targets -- -D warnings`: passed
- Default runner tests and all-target clippy with `--no-default-features`: passed
- Native WebRTC tests with `--no-default-features` (G.711, no Opus): passed
- Feature-enabled `pipecat-browser-agent --help` with all provider API keys
  removed from the environment: passed

The new end-to-end test sends the actual browser crate's SmallWebRTC start body
to HTTP, negotiates a real native peer using ICE/DTLS/SRTP/SCTP and Opus, then
sends source-compatible raw `ping` heartbeats and RTVI `client-ready`. Microphone
Opus is decoded at 48 kHz and the synthetic STT socket verifies the expected
16 kHz PCM size and nonzero audio. Synthetic streaming LLM text feeds TTS;
chunked 24 kHz synthesis returns as three nonzero 20 ms 48 kHz Opus packets.
Readiness correlation, final transcripts, generated-text context, and the absence
of invented playback confirmations are checked on that same connection.

Independent tests cover simultaneous peer/session isolation, wrong capability
and Origin, scoped PATCH and stop, duplicate/restart refusal, shared admission,
unused and malformed offers, shutdown before ICE completion, and a hard session
deadline under continuous RTP. PCM tests compare arbitrarily chunked output
against a continuous reference, including filter-tail drain, final packet padding,
interruption discard, and no duplicate flush.

The raw heartbeat regression uses actual native data-channel frames: `ping` and
source-compatible `ping` prefixes are consumed with no pong or application event.
Other malformed text, binary ping, and oversized ping remain rejected. The test
checks that heartbeats do not inflate application-message counters. The source
Python connection’s three-second ping-freshness `is_connected()` heuristic is
not implemented; native connection state and explicit runner deadlines govern
liveness.

This verifies native paired peers and synthetic wire services only. Browser
execution, microphone permission, physical playback, NAT traversal, TURN,
external provider credentials, and service quality remain unverified.

## Native Whisper onset audit

The runner's explicit native Whisper path uses three seconds of bounded VAD
pre-roll by default. This was checked with the verified upstream tiny.en weights
and public JFK fixture, native Silero, and paced 48 kHz browser-protocol audio.
It is a fixture-specific onset regression, not a universal speech-quality result.

The test normalizes exactly the browser encoder's PCM with the same stateful
48→16 kHz resampler and packet boundaries as the host. Temporary fixture-only
instrumentation captured bytes supplied to native Whisper. Removing the 8,000
samples of appended trailing silence from each segment yielded exact byte matches
at these half-open normalized-source sample intervals:

- One-second pre-roll: `[0, 37854)`, `[74590, 124382)`, `[125790, 164830)`
- Three-second pre-roll: `[0, 37854)`, `[42590, 124382)`, `[124382, 164830)`

At 16 kHz, the one-second configuration omitted the interval 2.365875–4.661875 s.
Native decoding of that omitted audio produced “Ask not!”; decoding the second
segment with two seconds of lead-in still omitted “Ask”. Three seconds moved the
second segment's onset to 2.661875 s and preserved “Ask not what your country can
do for you”. The final segment starts at the previous segment's exact endpoint,
so previously submitted speech is not replayed. Both full JFK clauses were then
asserted in actual browser transcription events and captured loopback LLM requests.

The temporary PCM-recording hooks were removed after the audit. No diagnostic PCM
or runtime recording hook is included in this crate. Three seconds retain at most
96,000 bytes of quiet PCM instead of 32,000 and may increase lead-in supplied to
inference. `PIPECAT_WHISPER_PRE_ROLL_MS` exposes the bounded tradeoff explicitly;
values above 5,000 ms and incompatible native segment limits are rejected.

### Runner feature matrix and real recognition

Verified offline on 2026-10-09 with the pinned Rust 1.99.0 toolchain:

- Default runner, `--no-default-features`: 15 tests passed
- `--features whisper`: 22 model-free tests passed; 11 explicit asset tests ignored
- `--features whisper,webrtc`: 29 model-free tests passed; 11 explicit asset tests ignored
- Strict all-target clippy for both default and combined features: passed
- Runner formatting and whitespace checks: passed
- Feature-enabled `pipecat-browser-agent --help` with all three hosted provider
  API-key variables removed: passed
- Explicit ignored library, `whisper_flow` and `whisper_lifecycle` targets: all 11 passed

The model-backed command uses the four explicit file variables documented in the
README and `-- --ignored --test-threads=1`. The real tiny.en model and JFK fixture
were checked against SHA-256 `921e4cf8686fdd993dcd081a5da5b6c365bfde1162e72b08d75ac75289920b1f`
and `59dfb9a4acb36fe2a2affc14bacbee2920ff435cb13cc314a08c13f66ba7860e`, respectively.
No credentials, paid providers, model downloads, or external network calls were used.

Two complete fixed windows and one paced VAD conversation go through actual HTTP
start, the browser crate's protobuf/RTVI encoders, 48→16 kHz normalization, native
Whisper, captured synthetic OpenAI requests, synthetic Cartesia synthesis, and
24 kHz output decoded by the browser crate. Both complete JFK country clauses are
required in recognized text and LLM input; returned audio bytes are checked exactly.
The Deepgram key is empty and its endpoint unusable throughout these checks.

Lifecycle checks cover denied transport admission, unused-start expiry, cancellation
after observed native window submission, and idle expiry with a minimum elapsed-time
check. Session reports and reusable admission slots appear only after owned native
workers have joined. The pre-roll regression transcribes the JFK fixture twice with
exactly two seconds of quiet before the second turn. Native input durations are
11.5 s and 13.5 s, respectively, proving that the three-second pre-roll does not
replay old utterance audio; both transcripts still contain both full clauses.

These results establish runner wiring and ownership for the tested assets. They do
not establish multilingual recognition quality, arbitrary VAD onset accuracy,
model-backed WebRTC recognition, physical browser/audio-device behavior, or live
OpenAI/Cartesia service behavior. Local STT still sends resulting conversation text
to the configured hosted LLM and TTS outside synthetic tests. Model loading and
cooperative native cancellation can outlast the nominal session deadline while
cleanup waits for the native owner to exit.

## Independent terminal and ownership regressions

Graceful End/Stop drains the shared input resampler before either STT provider
finishes. Both the source Python `BaseAudioResampler.flush` contract and native
`SincResampler.flush` require collection of the held stream tail. A deterministic
48→16 kHz test with 3,072 input samples formerly emitted 990 samples at End instead
of 1,024. End and Stop now emit the exact continuous-reference PCM, once, before
the terminal frame. A separate Cancel regression requires no tail emission.

The synthetic hosted-STT checks require the final tail before CloseStream:
WebSocket capture yields 956 + 68 PCM bytes (512 samples), and one 20 ms decoded
Opus packet yields 572 + 68 bytes (320 samples). Provider selection, credentials,
request settings and transcript behavior otherwise retain their existing defaults.
The explicit native partial-window test sends the complete 11-second JFK fixture
at 48 kHz, ends before its configured 30-second window fills, and requires exactly
one submission/transcript with 11.0 seconds of audio and both full JFK clauses.

Concurrent shutdown callers wait for one retained cleanup owner. Deterministically
polling two shutdown futures before scheduling session cleanup must leave both
pending. Dropping the first waiter also leaves cleanup owned, and a later caller
joins the same accepted session through its completion report and listener closure.

Additional real-model checks require separate owners for two ready sessions, release
only the stopped session's owner, and reuse its admission slot while the other model
stays active. Failed hosted LLM/TTS construction after native loading, authorized
clients that never send client-ready, and dropping the last runner handle all join
their native owners. Invalid GGML content has a model-free rollback regression that
reuses the session slot twice without contacting a provider. These checks neither
assume native loading is abortable nor impose the network deadline on its join.

## Slice 52: RTC registry and negotiation cleanup, 2026-10-14 UTC

Scope is the existing `serve_session` cleanup boundary after slice 51
`35f7f5d89f958ed590dc3c8f5f1215f102d8f993`. Registry close errors retain their
native error value and preserve any primary session error. Both immediate and
final negotiation joins now classify real Tokio panic/cancellation failures;
the final drain joins all owned tasks before returning the combined outcome.
The negotiation-join diagnostic includes failure counts instead of its panic
payload. Native registry error details are retained unchanged. This does not suppress the runtime's ordinary panic hook or change public session-summary
fields. Normal inner offer errors remain distinct from JoinError.

The focused tests use actual spawned panicking/aborted tasks and verify every
owner is dropped, all joins are consumed, primary/close/join errors coexist,
and expected inner `Closed` outcomes are not counted as task failures. Typed
registry-error combinations are explicitly injected outcomes through the same
private production cleanup helper; they are not a reproduced native registry
failure. A separate real SmallWebRTC registry/peer case checks gathered sockets
are held before close, released afterward, the registry is empty, repeated close
succeeds and final negotiation ownership is drained. Failure paths explicitly
close native owners and drain remaining tasks before assertions; combination
fixtures have a deadline with abort-and-join fallback.

The production change neither adds public fault-injection APIs nor modifies the
underlying handler's separate invalid-offer cleanup behavior. Browser execution,
public-provider compatibility and physical media remain outside this slice.

Fresh slice 52 checks passed:

- `cargo test --locked -p pipecat-runner --features webrtc -- --test-threads=1`:
  68 passed, none failed or ignored, including the six focused cleanup tests,
  actual HTTP/WS/RTC session lifecycle and the text/image provider witnesses.
- `cargo test --locked -p pipecat-runner --lib -- --test-threads=1` without
  optional features: 21 passed, none failed or ignored.
- Both default-feature and `--features webrtc` all-target Clippy with
  `-D warnings` passed. Native workspace formatting and `git diff --check` passed.

An initial focused test incorrectly assumed the existing boxed primary source
would downcast directly to `RunnerError`. The assertion was corrected to check
`Box<RunnerError>` and the preserved primary diagnostic; the production error
source representation was not changed. The final full replay covers the corrected assertion. Initial failure logs are distinguished from final runs.
