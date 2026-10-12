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

## Pending ICE shutdown publication fence

The existing local WebRTC stress regression
`pipecat-webrtc/tests/review_lifecycle.rs::repeated_peer_shutdown_schedules_release_both_udp_ports`
failed at schedule 5, iteration 24 during the historical draft workspace
gate. This is a **real failed gate**; neither the earlier SCTP/DTLS patch nor a
successful `pc.close().await` alone proves UDP candidate release.

This **candidate**, not yet source-qualified, extends the patch to:

- `src/ice_transport/mod.rs`: async ICE agent callbacks use a monotonic
  compare/exchange so a late Connected/Checking report cannot overwrite Closed.
  Start checks terminal ownership before installing its dial/accept cancellation
  lease and again under the publication lock after dial/accept. A connection
  completed after Stop is closed and is not installed into the mux.
- `src/ice_transport/ice_gatherer.rs`: terminal Gatherer Closed cannot
  regress to Gathering/Complete. Candidate startup shares the ownership mutex
  with agent removal by Close; a delayed completion callback can no longer
  resurrect a terminal gatherer. Gather attempted after close fails explicitly.
- `src/ice_transport/ice_transport_test.rs`: terminal callback-state regression
  verifies that queued state reports never clear the close verdict.
  Additional Gatherer internal tests exercise post-close state and concurrent
  gather/close admission.

The original upstream published-file SHA-256 entries remain the original
upstream identities; `UPSTREAM_PROVENANCE.json`'s list of *locally modified*
files is extended instead of rewriting upstream hashes. This patch does not
change ICE cryptography, candidate wire schemas, credentials or the vendored
library's version. Neither port closure nor stress flake correction is claimed
until the exact new source runs native tests.

The mandatory acceptance includes a dedicated immutable-checkout target:

```sh
cd rust
export CARGO_TARGET_DIR="$(mktemp -d)/pipecat-rust-target"
cargo test -p pipecat-webrtc --test review_lifecycle repeated_peer_shutdown_schedules_release_both_udp_ports --locked
cargo test -p pipecat-webrtc --test review_lifecycle --locked
cargo test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo fmt -- --check
```

An independent vendor unit-test invocation is also necessary because vendored
upstream tests are outside the root workspace test membership. Preserve
unmodified source hashes, true exit codes, captured output, skipped tests and
any remaining failure. Live browser/WebRTC and production readiness remain
unqualified.
