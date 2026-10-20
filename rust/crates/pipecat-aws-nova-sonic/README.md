# Native finite Nova Sonic PCM application

This crate is a **single-session, tool-free** Nova 2 speech application. It owns a
real standard `pipecat-turns` user/assistant pair, native HTTP/2 duplex transport,
explicit SigV4 authentication and signed nested AWS EventStream input. It is not
Bedrock Converse, a WebSocket adapter or a Python/SDK subprocess.

## Supported profile

- Explicit region, model, voice and `AwsCredentials`. No ambient credential
  provider, profile/IMDS/SSO lookup, token refresh or credential persistence.
- Models: `amazon.nova-2-sonic-v1:0`, `amazon.nova-2-5-sonic`. These identifiers
  describe the selected wire profile, not current regional/account entitlement.
- Raw signed PCM16LE, mono, 16 kHz input and 24 kHz output. No implicit conversion,
  WAV decoding, microphone/device capture or speaker playback qualification.
- Immutable bounded inference defaults: temperature .7, topP .9, maxTokens 1024.
  Optional endpointing sensitivity LOW/MEDIUM/HIGH. Construction validates settings.
- One consistent system instruction and bounded plain user/assistant seed. Seed is
  sent exactly once as **noninteractive** history, including a last user message.
  PCM starts live interaction; there is no fabricated client commit message.
- A finite absolute operation budget, at most 420 seconds, with an earlier local
  input deadline leaving a drain reserve. Expiry fails closed; it never renews.
- Exact regional HTTPS identity with normal certificate/hostname verification and
  ALPN h2, requiring one explicit caller-resolved `connect_address: SocketAddr`
  at port 443. The caller owns hostname resolution. TCP connects to that address
  once; the original regional URL remains the HTTP authority, TLS ServerName and
  SigV4 identity. No automatic DNS, address fallback or retry is performed.
  Literal-loopback HTTP/HTTPS connects directly and rejects an address override.
  No custom remote host, proxy discovery, redirects or HTTP/1 fallback.
  `NovaSonicConfig::new` validates syntax; `validate`, app construction and CLI
  configuration checking require a complete connection plan before any I/O.

`NovaSonicApp::new(config, context, output)` constructs the actual pair and the
immutable graph. HTTP/2 initial settings are processed within the bounded connect
phase before opening the request, so startup cannot spend an unnegotiated default
window. This does not wait for business response headers.
`start()` accepts startup only after initial writes and HTTP 200;
that is **not** a provider readiness event. The running app exposes a pipeline
worker, read-only history snapshots and restricted completion/join observations.
It does not expose raw wire senders, mutable shared context or tool callbacks.
Public input wrappers validate exact PCM, control/native-carrier policy, bounded
metadata depth/nodes and encoded frame size before enqueue. They reconstruct
ordinary input in fresh bounded allocations, so spare caller Vec/String capacity
is not retained by queues. The generic pipeline sender is not exposed. Transport
queues separately enforce their configured byte/count budgets; pipeline lanes
have fixed count bounds plus the admitted per-frame bound, not a universal shared
queue-byte counter.

## History and interruption contract

Completed USER FINAL text blocks are combined with explicit spaces until the
next assistant production boundary. The provider registers actual noninterruptible
upstream finals, and holds dependent output until the real standard user consumer
has appended that prefix and admitted its authenticated local snapshot. Receipt
waits do not block HTTP/2 reads, control, usage or cancellation. Unseen later user
text is never promised to precede already released output.

Speculative assistant text is visible but has `append_to_context=false`. Exactly
one speculative block is associated with its next audio segment; generated
`TtsText` is admitted at that audio segment's end. Provider FINAL text is not
appended a second time. History describes generated text traversing the configured
output path, **not** actual audible speech. Recorded bytes are not playback receipts.

A session-wide completion envelope may contain multiple speech turns. Sequential
content blocks are supported, with bounded explicit content identities and FIFO
speculative/final association. Ambiguous overlapping content or unresolved final
association across a new response fails closed instead of assigning text to the
wrong audio. AUDIO interruption and canonical late FINAL TEXT interruption both
broadcast through the actual causal interruption API, even after AUDIO END_TURN,
to flush queued old output. Bytes already written or played cannot be recalled.

End fences the real input lane, closes audio/prompt/session in order, sends one
signed zero-payload terminal and half-closes upload. Remaining output and user
receipts drain to a validated EOF. Cancel/Stop reset/cancel instead, never claiming
a graceful terminal. Retained task handles survive cancelled/concurrent join
waiters; primary failure, cleanup failure and forced-abort truth remain distinct.

## Recorded-PCM executable

Build `cargo build --offline --locked -p pipecat-aws-nova-sonic --example pcm_session`.
`pcm_session --help` reads no credentials and creates no files. `--check-config`
constructs the same app without network or output creation. `--session INPUT OUTPUT`
uses the same public constructor, validates bounded regular raw input before
`create_new` output, paces 1024-byte chunks, drains output continuously, waits for a
response and performs the app's real End and joins. Include sufficient trailing
end-of-speech silence in the input file. Session file validation is Unix-only and
fails closed elsewhere; help/config check remain available.

Supply `--region`, `--model`, `--voice`; optional `--endpoint` is subject to the
same endpoint policy. Regional HTTPS additionally requires `--connect-address IP:443`
with an address resolved by the caller; it never substitutes the IP into the TLS
or request authentication identity. `--credentials-env PREFIX` explicitly selects only
`PREFIX_ACCESS_KEY_ID`, `PREFIX_SECRET_ACCESS_KEY`, and optional
`PREFIX_SESSION_TOKEN` (default prefix AWS). Local acceptance uses a synthetic
namespace and a cleared environment; it does not discover real AWS credentials.
Output is raw 24 kHz PCM16LE mono. Existing files are never overwritten. A failed
session may leave a partial output file and is reported as such.

## Explicitly unsupported

Tools, function results, historical tool authority, tool rebinding, live text,
LlmRun, live context/settings changes, continuation, eight-minute renewal,
overlapping sessions, replay, retry, reconnect, resumption, old-model ready.wav,
arbitrary ARNs/partitions/voices, alternative codecs/rates, browser/WebRTC/PSTN,
deployment and account/permission changes. Rejecting a tool union does not mean
native tools are implemented. This slice is not full Python provider parity.

## Reference and local evidence boundary

Source behavior is referenced to ProfHepta/pipecat commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, especially
`src/pipecat/services/aws/nova_sonic/llm.py`. Python's dependency range is not a
resolved SDK pin. Wire reference source versions are aws-sdk-bedrockruntime
1.148.0, aws-runtime 1.10.0, aws-sigv4 1.6.0, aws-smithy-http 0.64.2 and
aws-smithy-eventstream 0.61.5, not runtime SDK dependencies or live entitlement.
Session-wide completion evidence also includes AWS's Nova evaluation harness
README at `008f72fc78dfcc0877b9b37950ac3a1e921b0338` and official Nova 2 console
sample at `0585cf443c998284ac6b9fdbfc0b41a1bc0619ea`.

Local TCP HTTP/2 fixtures exercise the actual production app and compiled binary,
with independent synthetic signing verification. They do not qualify DNS resolution
(which is outside this profile) and cannot certify successful hosted TLS,
AWS authorization, voice/model availability, provider VAD, speech quality, actual
speaker playback, WAN behavior or full source parity. See the batch acceptance
report for exact executed commands, counts, failures and nonclaims; merely having
these test sources is not evidence that they passed.
