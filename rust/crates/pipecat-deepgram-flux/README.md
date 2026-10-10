# Native Deepgram Flux Listen v2

A Rust WebSocket client and pipeline processor for Deepgram's Flux STT protocol.
This crate is separate from the Listen v1 client in `pipecat-streaming-speech`.
No Python interpreter, SDK, sidecar, or subprocess runs in the implementation.

## Composition

```rust,no_run
use std::sync::Arc;
use pipecat_core::Pipeline;
use pipecat_deepgram_flux::{FluxClient, FluxConfig, FluxSttProcessor};
use pipecat_services::Context;
use pipecat_turns::LLMContextAggregatorPair;

# fn example() -> Result<(), Box<dyn std::error::Error>> {
let mut config = FluxConfig::new(std::env::var("DEEPGRAM_API_KEY")?);
config.eager_end_of_turn = true; // Defaults the provider threshold to 0.5.
let stt = Arc::new(FluxSttProcessor::new(FluxClient::new(config)?));
let pair = LLMContextAggregatorPair::new(
    Context::default(), stt.recommended_user_config(), Default::default(),
);
// Place a native pipecat_services::LlmProcessor after pair.user(); its built-in
// speculation gate holds generated output until the committed turn matches.
let pipeline = Pipeline::new(vec![stt, pair.user(), pair.assistant()]);
# let _ = pipeline;
# Ok(())
# }
```

Explicit aggregator configuration is required. The native user aggregator does
not accept automatic strategy replacement from service metadata. This adapter
emits STT metadata without pretending it configured that downstream policy.
The recommended configuration uses external start/stop strategies and preserves
`should_interrupt`; only opt-in eager mode enables `EagerUserTurnConfig`.

## Wire and turn contracts

- TLS is required except for loopback fixtures. Authorization is a sensitive
  `Token` header. Endpoint query strings, embedded credentials and fragments are
  rejected; settings are URL-encoded, including repeated keyterms/tags/hints.
- HTTP upgrade alone is not readiness. `Connected` must arrive within a separate
  bounded deadline before audio admission or returning a session.
- Mono signed little-endian 16-bit PCM must match the configured/pipeline rate.
  Empty PCM does not start the silence watchdog. Incoming messages, queued audio,
  settings, transcript text and words have bounded sizes/counts.
- `StartOfTurn` proposes a user turn; it does not manufacture an interruption.
  `Update` remains a client event and does not become a final or interim frame.
  `EagerEndOfTurn` drives the existing native `EagerEndOfTurn` helper.
  `TurnResumed` cancels the prediction. Final transcription precedes the proposed
  stop; its `finalized` flag, provider payload, language and user identity survive.
- Enabled confidence filtering uses strict greater-than for the arithmetic mean
  of available word confidences. Missing confidence rejects the final. Zero or
  an absent threshold accepts it. A filtered final still ends the turn and
  explicitly revokes speculation. The first detected supported language wins;
  only the English model supplies an English fallback when languages are absent.
- Duplicate/completed turn indexes cannot duplicate a committed transcript.
  Unknown message/event names are ignored. Malformed known messages, orphan or
  overlapping turns, binary provider responses and invalid confidence fail closed.
- Watchdog silence is 500 ms of real PCM, chunked to the configured wire limit,
  sent only during an active turn after real audio stops for the greater of two
  audio-chunk durations or the watchdog minimum. Actual successful audio writes,
  including watchdog silence, drive optional usage metrics.

## Settings and connection ownership

`FluxSettings::merge` validates an entire delta before mutation. Queue admission
is atomic with the public desired-settings snapshot; a full queue cannot update
that snapshot. A snapshot expresses requested settings, not a claim of provider
acceptance. Both the session API and targeted STT/service settings frames use
this typed contract; unknown fields are rejected rather than silently ignored.

Configure-able fields are keyterms, EOT/eager-EOT thresholds, EOT timeout,
multilingual language hints and profanity filtering. Only one Configure may be
in flight. Further updates coalesce into the latest desired state. Success and
failure are explicit client events; a failure does not claim acceptance or
silently change the provider state. `min_confidence` is local. Model, numerals
and redaction changes reopen the connection after the current final turn.

Configure acknowledgements have no correlation identifier. On an acknowledgement
timeout this implementation revokes an unresolved eager turn and opens a new
connection carrying the latest settings, instead of assigning a delayed old
acknowledgement to a later request. This is an intentional safety difference
from the source's stale-ack supersession. Audio already written is never replayed.
Live coalescing assumes the provider sends one ordered acknowledgement for each
Configure. A duplicated old acknowledgement arriving after a newer Configure is
indistinguishable from its genuine acknowledgement on this protocol; it is not
claimed to authenticate acceptance. Stray acknowledgements with no request in
flight are ignored. Readiness and transport failures terminate the session; automatic transport
retry, ambiguous audio replay and transparent health restoration are not claimed.
An application may explicitly construct a new session after addressing an error.

The pipeline processor admits audio without awaiting network I/O and drains the
session in a tracked task. Input admission, output backpressure, writes, readiness,
configuration acknowledgements, close/drain and protocol cleanup are bounded.
Cancel and session drop interrupt queue waits and close the socket. End sends
CloseStream and drains until server close or the drain deadline. Health-generation
changes invalidate the receive task and withdraw pending predictions. Prediction
emission, mute, End and cancellation are serialized; End disables new eager
predictions while final results drain. Old session IDs cannot affect a replacement.
The existing aggregator retains ownership of native speculation confirmation,
context equality, capability identity, expiry, effect fencing and output release.

## Provenance

Behavior is mapped to `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

| Source | Coverage |
| --- | --- |
| `services/deepgram/flux/stt.py` | v2/listen token transport, readiness, CloseStream, lifecycle |
| `services/deepgram/flux/stt_base.py` | typed settings, Configure, turn events, language/confidence and silence watchdog |
| `turns/eager_end_of_turn_mixin.py` | existing native helper, opt-in eager lifecycle |
| `tests/test_deepgram_flux_stt.py` | settings classes, readiness, confidence and lifecycle source cases |

Source SHA-256:

```
e47a38dce3fea4f93522bb23d5667922800476c84da3f3213c4c04e9529f91ec  stt.py
50af1e23c69a8dc1dde9686f927649b0ab3e9946c20820e5c1b5acd77d37cc41  stt_base.py
e706555f9ded27c0ca466297431f70287ab14157f31fa1dcb003ff0d42defbd4  eager_end_of_turn_mixin.py
3556e9404ba7dae3e2db18f7803f35a7b02020e41c19468db457af524f4e0077  test_deepgram_flux_stt.py
```

The bounded socket/queue primitives in `common.rs` follow the native
`pipecat-assemblyai` implementation, with its license preserved. Flux parsing,
configuration, actor lifecycle and pipeline signal adapter are owned here.

## Qualification and explicit gaps

Run `cargo test -p pipecat-deepgram-flux`. Fixtures exercise real loopback
WebSocket traffic and real loopback HTTP inference with the native user/assistant
aggregators and LLM speculation gate. They do not impersonate a live Deepgram
acceptance result, transcription quality benchmark, latency measurement or billing
verification. Live paid-provider/TLS endpoint acceptance is not run by these tests.

SageMaker transport, Flux TTS, arbitrary future model names/encodings, Python
callbacks/subclasses, source tracing spans and automatic recommended-strategy
installation are outside this crate. Typed language hints use the supported base
codes rather than accepting and silently reducing arbitrary locale strings.
Malformed known protocol responses fail closed instead of being logged and skipped.
These deliberate limits are not full Deepgram-family or whole-project parity.
