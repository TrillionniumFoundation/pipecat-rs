# SCTP and DTLS shutdown lifecycle patches

Source: the published crates.io `webrtc` 0.14.0 archive, SHA-256
`08fd686c0920ac08f3a57eacc48e31f0e4ca1ffefba4478784606f78c14e83ad`.
The archive records upstream Git commit
`a1f8f1919235d8452835852e018efd654f2f8366` and source directory `webrtc`.
`UPSTREAM_PROVENANCE.json` records the original published file hashes.

The MIT OR Apache-2.0 license declaration is unchanged. This published archive
omits license-text files; LICENSE-MIT and LICENSE-APACHE are copied unchanged
from the published `webrtc-ice` 0.14.0 archive, which records the identical
upstream commit. Copyright: 2021 WebRTC.rs.

The upstream code deltas are restricted to two transport modules.

`src/sctp_transport/mod.rs`:

- A shared, durable closed flag is set before shutdown awaits
- Association setup and the data-channel accept loop register their notification
  before checking the flag, preventing a lost shutdown wake
- Association publication checks the flag under its admission lock
- Incoming channels cannot be registered or opened after shutdown, including
  when shutdown happens inside an application callback

`src/dtls_transport/mod.rs`:

- A durable stopped flag is set before cleanup awaits
- A stop notification is registered before rechecking that flag and entering
  the native DTLS constructor; stop cancels a handshake that has not yet
  published its connection, allowing the owning peer operation and native
  reader/writer tasks to release their ICE socket references
- Native DTLS connections and SRTP/SRTCP sessions check that flag under their
  publication locks, so a late handshake completion cannot recreate closed media
- Closed state cannot regress when an earlier asynchronous state callback returns

The shutdown repairs do not change dependency versions, cryptographic algorithms,
wire formats, ICE routing or public APIs. Shutdown is terminal, matching the
existing peer connection lifecycle. The later read-only diagnostic API addition
is documented separately below.

Local regression coverage is in `pipecat-webrtc`: a deterministic incoming
channel callback barrier reproduces the missed shutdown notification, verifies
late channel admission is rejected, and checks release of real UDP sockets.
A second deterministic DTLS Connected-callback barrier verifies that shutdown
cannot be followed by late SRTP publication. A Connecting-callback barrier now
holds both peers before their native constructors, stops them, and checks that
resuming both starts cannot retain either real UDP candidate. A separate native
ICE pair leaves one DTLS handshake pending and checks that stop terminates the
start future with an error and releases the UDP sockets. Repeated sequential/concurrent
close, Drop and handshake-close tests exercise both peers. The separate wrapper error classifier only accepts the exact SCTP
reset-after-close aggregate after all transports report Closed; mixed errors
remain failures.

## Read-only SCTP startup observation

`src/sctp_transport/mod.rs` additionally exposes
`RTCSctpTransport::has_started() -> bool`. It reads the existing `is_started`
atomic with its existing sequentially consistent ordering. It does not write
state, add a task, retain transport ownership, change start admission or retry
association setup. This is a public diagnostic API addition; the earlier
no-public-API-change statement applies to the shutdown repairs only.

The flag is set when start is entered, before association completion. It must
not be interpreted as a Connected state, successful DCEP negotiation, application
channel publication or remote receipt. `pipecat-webrtc::ConnectionDiagnostics`
uses it alongside independent ICE, DTLS, SCTP and channel observations.

The unresolved historical data-channel setup timeout is not attributed to the
SCTP flag or repaired by this getter. A controlled real-UDP wrapper-publication
barrier test checks the distinction between native Open and application-ready
publication; it is not a reproduction of that stochastic failure. See
`pipecat-webrtc/SHUTDOWN_REGRESSION.md` for qualification limits and retained
failures.
