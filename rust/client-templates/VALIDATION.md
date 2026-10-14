# Browser-client validation record

Environment: Rust 1.99.0, wasm32-unknown-unknown, wasm-bindgen CLI 0.2.129.

## Replacement-host checks, 2026-10-10 UTC

The isolated recovered browser workspace again passed its 11 native tests,
native all-target strict Clippy, WASM strict Clippy, and formatting. The Rust
WASM release build succeeded, but the original packaging script then failed
because it hardcoded `target/` while Cargo used an exclusive external target.
`build.sh` now passes its selected target directory explicitly to Cargo and uses
the same directory for wasm-bindgen. The corrected script produced all five
static assets with `CARGO_TARGET_DIR` set to an external absolute path.

The official wasm-bindgen 0.2.129 Linux-musl archive was retrieved from
https://github.com/wasm-bindgen/wasm-bindgen/releases/download/0.2.129/wasm-bindgen-0.2.129-x86_64-unknown-linux-musl.tar.gz
and hashed as `82d12bb940e2d4e72e0d5605387fc1b8ca179044e012b620f0ce4e7440e8320e`.
No native tool binary is bundled in this source tree.

A fresh ordinary headless Chromium launch on the replacement host again exited
134 before loading `about:blank`, reporting the process-singleton local socket
failure `Operation not permitted`. The probe did not disable the browser sandbox
or change network/security settings. Therefore the build checks above still do
not constitute browser execution, microphone, visual, or playback acceptance.
The earlier detailed fixture record below is historical; the new probe verifies
the launch blocker, not those unexecuted fixture assertions.

## Passed

- `cargo test --offline --locked`: 11 native tests
- `cargo clippy --offline --locked --all-targets -- -D warnings`
- `cargo clippy --offline --locked --target wasm32-unknown-unknown -- -D warnings`
- `cargo fmt -- --check`
- `./build.sh`: optimized wasm32 build and generated browser interop bundle
- `python3 -m py_compile tests/browser_acceptance.py`

The native lifecycle tests use the same generation and queue-limit functions as
the browser implementation. They cover repeated Connect, disconnect/reconnect,
late results from microphone/start/offer/socket/peer stages, muted or disconnected
capture, 256 KiB socket backpressure, and playback time/node limits. They do not
exercise browser-owned resource cleanup.

## Blocked before browser assertions

`python3 tests/browser_acceptance.py` cannot launch Chromium in this execution
environment. Initial launch and the permitted elevated retry both terminate
before any app page loads. Relevant Chromium output:

```text
FATAL:chrome/browser/process_singleton_posix.cc:297
Check failed: . socket() failed: Operation not permitted (1)
```

The first launch also reports:

```text
chrome_crashpad_handler: --database is required
mkdir : No such file or directory (2)
```

The elevated retry's decisive error remains the denied local socket; changing
application code cannot fix that browser-launch restriction.

The supported cloud browser was separately asked to open a loopback static
server at `http://127.0.0.1:8765/` and returned `net::ERR_CONNECTION_REFUSED`.
It cannot access this execution namespace's loopback listener. No public tunnel,
security-setting change, alternate user computer, or physical-device access was
used. The temporary static listener was stopped.

Therefore the browser fixture is **unexecuted beyond launch**, no acceptance
screenshot exists, and no UI, browser microphone, speaker, WebRTC or WebSocket
end-to-end acceptance is claimed. Run the included fixture in an environment
that permits Chromium's local sockets. The fixture covers synthetic media,
actual loopback WebSocket/protobuf exchange, paired browser WebRTC SDP/data/media,
cancellation during pending permission/start/offer, reconnection, microphone
mute, queue limits, transcript safety, and browser resource cleanup.

Live providers, physical devices, TURN/WAN traversal, autoplay restrictions and
other browser engines remain separate acceptance gates even after that fixture
passes.

## Slice 45 host boundary, 2026-10-14 UTC

A fresh ordinary headless Chromium launch and one approved ordinary escalated
launch both failed before `about:blank` at the process-singleton socket with
`Operation not permitted`. Neither disabled the browser sandbox or changed
security settings. No browser application assertions, DOM interaction, screenshots,
physical media or browser end-to-end execution are claimed for this slice.
The updated acceptance fixture explicitly enables the Chromium sandbox and adds
real-form text cases for both transports; it remains unexecuted on this host.
Native shared-controller and real native transport/provider tests are separate
from browser acceptance. The complete image/receipt browser bridge is not present.

## Slice 46 microphone-free input boundary

At slice 46, the explicit text input configuration was limited to WebSocket. Its actual WASM
connection branch gates both getUserMedia and capture-node creation using the
immutable session policy, while keeping bot audio output. Default voice mode and
explicit unsupported WebRTC text-mode rejection are separate compatibility paths.

The old-entry witness uses the actual native Config parser/validator: legacy
voice JSON is accepted, while old45 rejects input_mode as an unknown field. This
is a configuration-entry boundary, not an executed microphone permission failure.
Selected native tests cover the shared input policy, real typed WebSocket replies,
and independent WS/Opus WebRTC voice/provider controls. They do not run browser
permission/capture APIs. The updated real Chromium fixture includes a throwing
getUserMedia trap for text mode, capture/audio/resource checks and mode changes
through disconnect/reload, but remains unexecuted on this host. The previously
recorded launch blocker is reused; no new launch or sandbox workaround is attempted.

## Slice 48 receive-only WebRTC boundary

Text-mode Config now accepts SmallWebRTC and mixed supported transport lists.
The browser owns its peer/data channel before awaited negotiation and adds an
explicit recvonly audio transceiver without a local microphone track. Existing
voice-mode capture and audio reply paths remain separate controls. Native tests
exercise actual peer direction, rejected audio send admission, zero audio uplink
observations with positive text/downlink traffic, and owned cancellation/close.
The standard runner child serves the exact opt-in config consumed by a separate
native loopback-provider witness. These boundaries do not execute browser APIs.

The Chromium fixture has corresponding throwing-permission, capture, local-track,
negotiated direction, RTP statistics, text/reply, reconnect and pending-offer
cleanup assertions. They remain unexecuted under the previously recorded ordinary
launch blocker. There is no new launch retry or sandbox workaround. Fresh WASM
and static packaging is a build/identity gate only; browser permissions/autoplay,
physical devices, WAN/TURN and the image/receipt bridge remain unqualified.

## Slice 51 explicit image input recovery, 2026-10-14 UTC

This candidate was rebuilt from published slice 50 (`b626d482bd19d1e69ac294983d584404a266c3d4`)
after replacement of the earlier execution environment. No uncommitted slice 51
source or old build result was assumed recovered. Tooling was restored from
Rust's official 1.99.0 distribution with wasm32 target and wasm-bindgen 0.2.129
(the upstream archive hash recorded above matched again). CMake 3.31.6 came from
the standard PyPI package to build the native Opus dependency. System ONNX Runtime
1.21.0 and tracked models support the native runner witnesses.

New native policy tests exercise exact readiness, cancelled/stale tickets,
MIME/signature and raw-size limits, original PNG/baseline/progressive/subsampled
JPEG bytes, dimensions and literal captions, exact WebRTC wire budget and one
over, base64/escaping overhead, checked transport backpressure, malformed CRC,
Adler, compressed tails, extra decoded scanlines, truncation and PNG animation.
The shared golden contract is checked against both browser encoders and the
native RTVI UserImage projection, without reencoding. Default-off vendor JPEG
strictness separately tests real entropy-bit consumption, padding, restart,
complete progressive scans, coefficient/refinement bounds and valid controls.

The extended native runner `text_flow` fixture executes both actual protobuf
WebSocket and native paired SmallWebRTC send-file paths, two fresh sessions each.
The exact browser-generated JPEG data URL and caption reach a loopback synthetic
OpenAI HTTP request; generated reply text and native TTS audio return, and the
final session history retains the original image Parts. The second session uses
explicit peer disconnect. The original two text tests remain controls. All four
passed, with joined provider/server/peer owners and no cleanup errors. These are
real local transport/provider-protocol observations, not calls to public provider
accounts and not browser FileReader/DOM execution.

A fresh normally sandboxed Chromium launch in the replacement environment still
failed before `about:blank`: process-singleton `socket() failed: Operation not
permitted`. No sandbox flag, permission change or alternate host was used to
work around that failure. Thus the expanded acceptance fixture is syntax checked
only; image controls, actual FileReader timing, layout/screenshots and browser
permissions/autoplay remain unexecuted. It includes both transport sends,
selection-only non-send, repeated clicks, raw and malformed-file rejection,
selection/caption/clear invalidation, instrumented delayed reads, busy/send
failure retention and disconnect/reconnect no replay. Its assertions are a
retained future qualification gate, not a current passing result.

Fresh selected checks for this rebuilt candidate (not a full native workspace
qualification):

- Browser `cargo test --locked`: 31 passed, none ignored or failed.
- Native `cargo test --locked -p pipecat-rtvi -p pipecat-images`: 117 passed,
  none ignored or failed.
- Native `cargo test --locked -p pipecat-runner --features webrtc --test text_flow
  -- --test-threads=1`: four passed, each exercising two sessions.
- Vendored JPEG library's direct Rust 2021 `--test` build with
  `--cfg 'feature="platform_independent"'`: 17 passed.
- Native selected packages (`pipecat-runner`, `pipecat-rtvi`, `pipecat-images`),
  all targets, runner `webrtc` feature: strict Clippy passed.
- Browser native all-target and wasm32 strict Clippy passed; release WASM and
  wasm-bindgen static packaging passed; browser format check passed.
- Expanded Python browser fixture passed `py_compile`; Chromium runtime gate
  remains blocked as stated above.

## Slice 53: bounded job activity, 2026-10-14 UTC

The existing four-kind `ui-job-group` contract was checked against Python commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, models.py Git blob
`cbd081ccd8e3ff7167c9456d438d16c537ddc7e3` (SHA-256
`2689d3cf72ea8348a20c869e01c868a8838e6068f3a7b5366b225dfc28205dd2`).
The current native typed envelope agrees with its fields/defaults except the
already-existing unsigned timestamp profile. This work implements the bounded
read-only rendering policy documented in README; it does not claim parity with
an unavailable external React reducer, image/video display, or cancellation.

The browser-independent reducer covers all four events, interleaved groups,
literal/arbitrary status and JSON, optional/null fields, delivery order (not
`at` order), stale epochs/closed sessions, terminal workers/groups, repeated IDs,
completed-group eviction, active-group retention, full ID ledger, exact UTF-8
and serialized JSON budgets, transactional payload rejection and persistent
incomplete warnings. Valid completion facts survive result-budget rejection.
Bounded omitted-completion tombstones prevent previously unseen workers from
reviving after capacity frees; ledger saturation fails closed for new workers.

The runner's `job_group_flow` fixture serializes native typed RTVI events,
transports them over actual localhost protobuf WebSocket and paired native
SmallWebRTC ICE/DTLS/SCTP, decodes the received values and applies the same browser
model. It checks exact wire values, all lifecycle transitions, isolation and
late-event rejection. Both endpoints remain owned outside a deadline-bounded
observation and are explicitly closed/joined before assertions, including setup
or observation failure. No provider service or external STUN/TURN is used.

Selected checks for this slice:
- Browser locked tests: 44 passed (including 13 new reducer tests).
- Runner `--test job_group_flow --features webrtc`: two real transport tests;
  default-feature execution: one WebSocket control.
- Browser native all-target and wasm32 strict Clippy; runner WebRTC all-target
  strict Clippy; browser/runner selected formatting; optimized wasm32 build and
  wasm-bindgen packaging.
- Extended browser fixture: Python syntax check only.

A fresh ordinary sandboxed Chromium launch again failed before `about:blank`
with process-singleton `socket() failed: Operation not permitted`. Therefore
DOM lifecycle, XSS-as-literal rendering, disconnect clearing and 720/480-pixel
viewport control reachability are retained fixture assertions, not executed
passes. Main/conversation scrolling and minimum panel sizes are a conservative
source-level accessibility mitigation; no screenshot or visual acceptance is
claimed. Native transport/model results do not establish browser DOM behavior.
