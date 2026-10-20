
## SmallWebRTC selection

Normal `cargo run` enables SmallWebRTC without `--webrtc`. The included client
selects SmallWebRTC first; the host also retains the WebSocket endpoint. There is
no silent fallback if RTC negotiation fails. `/api/start` accepts `smallwebrtc`
(or the protocol alias `webrtc`), returning an isolated session offer endpoint.
ICE is host-only: no public STUN/TURN service is configured. This local scaffold
makes no WAN, deployment, browser automation, or physical-playback guarantee.
The native Opus dependency requires a working native C/CMake toolchain. Its
audited WebRTC implementation remains
a path dependency in the local checkout, including outside that workspace.

`src/lib.rs::create_runner` is shared with production startup and accepts explicit
provider and RTC settings for local native integration tests. Production startup
reads normal provider credentials; tests with injected loopback peers do not
validate real provider accounts or the production environment-to-provider path.
