# Deepgram WebSocket TTS: bounded complete utterances

The optional `deepgram-ws` feature exposes `deepgram_ws::DeepgramWsClient` and
`HttpTtsBackend::DeepgramWebSocket`. The facade uses the same feature name.
`HttpTtsProcessor` keeps its historical public type name; this explicitly named
variant uses WebSocket transport and reports `DeepgramWebSocketTtsService`.
Existing HTTP variants retain their original names and behavior.

## Fixed reference and intentional scope

The reference is [DeepgramTTSService at the pinned Python baseline](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/deepgram/tts.py),
commit `af9f9da9d7afddaa8811c0e0251b32566c980bdf`, source blob
`cb43af2e0f8ef86e8d0a6e20f6029915e7524f4a`. Its actual base is
`WebsocketTTSService`. It opens a service-lived socket during setup, sends Speak
without a context ID, flushes at the turn boundary, and attributes all binary
PCM and Flushed events to its current active audio context. Clear is sent on
interruption. Cleared is logged, and the source ignores sequence_id.

This first native profile deliberately uses one fresh socket for each complete
utterance: one Speak, one Flush, binary audio, Flushed, then protocol Close and
the WebSocket closing handshake. It does not reuse a socket for a successor.
This costs an additional connection/handshake per utterance and does not offer
persistent low-latency token appends or source scheduling equivalence. Untagged
PCM from a retired socket cannot be reassigned to another utterance.
The shared processor accepts already prepared complete utterances; it does not
reproduce the Python base service's sentence aggregation, pause scheduling or
automatic sentence-mode trailing space. Synthesis whitespace is preserved as
supplied, and any such preparation belongs upstream.

A locally completed Flush write and the subsequent observed Flushed form the
native completion rule. They are not a cryptographic proof of the peer's internal
processing order. Optional sequence metadata is not an utterance identity or a
license to reuse the old socket. Success requires socket closure and actor join;
Close alone is not synthesis proof. Extra application data after Flushed is an
explicit error, not more speech for a future context.

The source's permissive logging of malformed/unknown controls is tightened to
bounded, fail-closed parsing. These safety adaptations are not presented as
byte-for-byte behavior parity. Tests retain original-source negative controls.

## Configuration and audio

`DeepgramWsConfig` holds immutable endpoint, key and resource limits. Synthesis
uses the existing `DeepgramSynthesisRequest` with voice default
`aura-2-helena-en`, default native rate 24000 Hz, optional speed and MIP opt-out.
The source itself derives rate from setup when no explicit rate is provided.
This profile accepts only raw mono Linear16 at 8/16/24/32/48 kHz. Mulaw and Alaw
remain available through the existing HTTP client, not this WS variant.

The endpoint must be the canonical Deepgram WSS Speak endpoint or loopback for
tests. Query parameters are encoded, credentials use a sensitive Token header,
and redirects are not followed. No HTTP-only container query is added. TLS
certificate validation remains normal. Canonical-host DNS uses the existing
runtime/system resolver; socket/actor join evidence does not qualify cancellation
of every underlying resolver operation.

Odd binary fragments may carry one byte into the next fragment. A residual byte
at Flushed is an error. Empty synthesis does not invent audio or word timing.
Wire/message/audio/text/queue/actor limits and absolute deadlines bound retention
and waiting, including backpressure. Limits describe content and actor counts,
not an exact allocator/JSON/WebSocket overhead bound.

## Ownership, cancellation and pipeline integration

Client clones share one bounded actor owner. `fork_owner()` creates an independent
owner with the same configuration. A registered actor retains its socket until
normal completion or bounded cleanup; cancelled actors still consume capacity
until they are actually finished and reaped. Stream Drop requests cancellation.
It does not claim synchronous joined shutdown.

Cancellation attempts Clear and Close within a finite cleanup budget. A locally
written Clear does not prove that the remote server cleared anything. Retirement
of the old socket is the isolation boundary. `cleanup()` permanently seals its
owner against new work and joins registered actors. Its borrowed join handles
remain owned if the cleanup future is cancelled, so cleanup can be retried.
Reopening requires an explicit fresh owner.

The processor gives each setup an independent owner, preserving isolation when
several processors are constructed from the same external client. Normal reuse
requires the preceding run's shutdown to complete. The existing complete-utterance
scheduler, original/display/synthesis channels, destination routing and native
authority guards remain in effect. Only synthesis text reaches Speak. Supported
typed settings target the actual processor instance; no wrapper substitutes a
different Local identity. Live speed/MIP mutation and persistent socket reconnect
are outside this profile.

There are no provider word timestamps. The shared synthesis-completion projection
therefore stays Unreported for word playback. PCM output, synthesis completion,
a backend write receipt and a physical heard-history claim are distinct. Even a
controlled Played receipt cannot fabricate missing word timing.

## Qualification boundary

Protocol and pipeline fixtures use synthetic loopback servers and dummy keys.
They can establish framing, bounded ownership, cancellation, PCM routing and
local history behavior. They do not establish real provider authentication,
voice/rate eligibility, pronunciation quality, account quotas, physical playback
or deployment. The offline Python oracle uses unchanged extracted methods with
explicit boundary stubs, not a full Python service installation.
