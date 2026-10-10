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
- Native DTLS connections and SRTP/SRTCP sessions check that flag under their
  publication locks, so a late handshake completion cannot recreate closed media
- Closed state cannot regress when an earlier asynchronous state callback returns

No dependency versions, cryptographic algorithms, wire formats, ICE routing,
or public APIs are changed. Shutdown is terminal, matching the existing peer
connection lifecycle.

Local regression coverage is in `pipecat-webrtc`: a deterministic incoming
channel callback barrier reproduces the missed shutdown notification, verifies
late channel admission is rejected, and checks release of real UDP sockets.
A second deterministic DTLS Connected-callback barrier verifies that shutdown
cannot be followed by late SRTP publication. Repeated sequential/concurrent
close, Drop and handshake-close tests exercise both peers. The separate wrapper error classifier only accepts the exact SCTP
reset-after-close aggregate after all transports report Closed; mixed errors
remain failures.
