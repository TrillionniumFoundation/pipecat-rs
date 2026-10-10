# Local SFU qualification

## Executed evidence and blocker

On 2026-10-09, the official Linux amd64 LiveKit server 1.13.9 binary was
workspace-locally downloaded, checksum-verified, extracted and queried with
`--version` / `--help`. Its dev-mode startup was attempted once with
[explicit loopback signaling and interface/IP filters](server-startup-attempt.yaml).
The process exited before listening:

```text
failed to get create network: route ip+net: netlinkrib: operation not permitted
```

The [complete startup log](server-startup-denied.log) records the attempt.
The initial configuration included an RTC TCP port. A later
[pinned upstream source inspection](https://github.com/livekit/mediatransportutil/blob/f234b534b095/pkg/rtcconfig/webrtc_config.go)
established that this TCP mux would bind wildcard independently of the main
signal-server bind address; startup failed earlier in UDP network setup and
never reached that listener. The unexecuted harness therefore disables RTC TCP
entirely (`tcp_port: 0`). No revised server startup was attempted.
Network-interface inspection with `ss` independently returned `Cannot open
netlink socket: Operation not permitted`. No bypass, security-setting change,
server source patch, external server, public listener, port forwarding, hosted
account, paid endpoint or real credential was used. No participant JWT was
minted because server startup never succeeded. The server process was reaped.

The `local_sfu` integration test is **compiled, not executed** on this host.
It is ignored by ordinary `cargo test`, so a green default test run must not
be reported as SFU qualification. No successful room join, ICE/DTLS session,
SFU PCM/data transfer, leave/rejoin, or network reconnection is claimed.

The independent `network_signaling` test did execute successfully. All six
cancel/abort/timeout cases complete real loopback TCP + WebSocket HTTP 101
handshakes and observe socket closure, both before a JoinResponse and after a
synthetic JoinResponse. They do not emulate an SFU. The client is injected via
the official SDK `livekit-net::set_ws_client` API, is limited to literal
`127.0.0.1`, and refuses to bypass a configured proxy unless that literal address
is already excluded by `NO_PROXY`. The environment remains unchanged. This
qualifies that test transport and real SDK task ownership, not the SDK's default
WebSocket proxy implementation.

## Official server provenance

- [Official dev-mode instructions](https://docs.livekit.io/transport/self-hosting/local/)
- [Release v1.13.9](https://github.com/livekit/livekit/releases/tag/v1.13.9),
  published 2026-10-07; source commit `6fbe7f7`
- [Official release API metadata](https://api.github.com/repos/livekit/livekit/releases/tags/v1.13.9)
- [Linux amd64 archive](https://github.com/livekit/livekit/releases/download/v1.13.9/livekit_1.13.9_linux_amd64.tar.gz),
  19,290,784 bytes
- Archive SHA256:
  `0b7fa208b662d09cfdeae8c06cf4c481aead0556086558b48250501e2e2d6e20`
- [Official checksum manifest](https://github.com/livekit/livekit/releases/download/v1.13.9/checksums.txt),
  498 bytes, SHA256
  `05fb9b428d6a25528545abf8545832bf97a5a5af7cf82e25755646ad6caf13e8`
- The archive matched both the release API's asset digest and `checksums.txt`
- Extracted executable: 57,295,010 bytes; locally calculated SHA256
  `0fbb1246f555fc3d27ae92530273f53d9463bbccd93c7edb398e0c6ccfe1536b`
- License: [Apache-2.0](https://github.com/livekit/livekit/blob/v1.13.9/LICENSE),
  also preserved as `LICENSE` in the downloaded archive

Downloads remain under the workspace tools prefix, outside the repository.
No system-wide install was performed. The executable hash used by the fixture
is derived from the verified official archive; it is not a separately published
vendor executable checksum or signature.

## Opt-in harness contract

`tests/local_sfu.rs` only accepts the pinned executable hash above. It starts
its own process with random available loopback ports, `bind_addresses:
[127.0.0.1]`, an explicit loopback node IP and interface/IP filters, disabled
external-IP discovery, disabled RTC TCP, and a loopback-only STUN address. No public STUN/TURN
server is configured. It inherits existing proxy and host security settings;
unrelated LiveKit account/configuration environment values are excluded from
the fixture child. It is Linux amd64-specific and requires ordinary local
network-interface enumeration to be permitted by the host.

Only after startup succeeds does it mint room-scoped, 120-second participant
tokens from the documented public dev-mode `devkey` / `secret` fixtures. Tokens
remain in memory and are never logged or persisted. It attempts to verify:

1. Two native adapter participants join one real SFU room and publish audio
2. Reliable and lossy identity-addressed data arrive with payload, identity,
   topic and reliability preserved
3. A generated 440Hz tone traverses native Opus encoding, SFU forwarding,
   decoding and 24kHz-to-16kHz input resampling in both directions; received
   nonzero PCM and pipeline identity/track metadata are checked, without a
   byte-exact fidelity or remote playback claim
4. Cancellation and awaited close produce a peer-observed departure; the same
   identity rejoins and sends fresh data; repeated close remains safe
5. Owned signaling references are released, the server process is killed and
   reaped, temporary fixture files are removed, and its signaling port closes

Startup and individual operations are bounded, with a 60-second scenario
limit. The process has a cleanup guard for assertion failure and cancellation.
No automatic reconnection, network-loss recovery, server-failure rollback or
playback/audibility result is covered by this harness.

On a host where local network enumeration is already permitted, after applying
the native prerequisites in [VALIDATION.md](../VALIDATION.md):

```sh
export PIPECAT_LIVEKIT_SERVER=/absolute/path/to/verified/livekit-server
cargo test -p pipecat-livekit --test local_sfu -- --ignored --nocapture
```

Do not re-run startup on a host that has explicitly denied the required access,
or change security/proxy settings just to make the fixture pass. To reproduce
the already permitted socket-only checks without running an SFU:

```sh
cargo test -p pipecat-livekit --test network_signaling -- --nocapture
```
