# Native pipeline integration

`MoqTransport` owns a connected `MoqConnection` and exposes single-use
`MoqInputTransport` and `MoqOutputTransport` processors. Use both endpoints in
one pipeline, with input before output, or use either endpoint alone. Connection
and pipeline settings are separate typed structs. No Python process, synthetic
transport, browser connection, or provider account is used by the adapter.

```rust,no_run
use std::sync::Arc;
use pipecat_core::{FrameProcessor, Pipeline, PipelineConfig};
use pipecat_moq::{MoqConfig, MoqTransport, MoqTransportConfig};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let transport = MoqTransport::connect(
    MoqConfig::new("https://relay.example/moq"),
    MoqTransportConfig::default(),
).await?;
let processors: Vec<Arc<dyn FrameProcessor>> = vec![
    transport.input(),
    // Native STT, agent, and TTS processors belong here.
    transport.output(),
];
let worker = Pipeline::new(processors).start(PipelineConfig::default()).await?;
// Consume worker output concurrently for long-running applications.
worker.end().await?;
let (_frames, _report) = worker.collect().await?;
# Ok(())
# }
```

## Audio and frame mapping

- Incoming Opus is genuinely decoded by the native client to 48 kHz mono PCM.
  A persistent windowed-sinc resampler converts this to the configured input
  rate, or `StartFrame.audio_in_sample_rate` (normally 16 kHz). Rates are integer
  values from 8,000 through 192,000 Hz, including 22,050 and 44,100 Hz
- Packet timestamps remain `moq.packet_timestamp_us` metadata, not pipeline-clock
  PTS. Gaps or timestamp discontinuities flush the previous filter tail without
  inventing missing audio. Graceful input End also flushes the filter tail
- Incoming transcript records become `InputTransportMessage` frames upstream,
  matching the source transport's RTVI placement. Peer announcement emits the
  native `ClientConnected` frame. Output emits `OutputTransportReady` at Start
- `OutputAudio`, `TtsAudio`, and `SpeechOutputAudio` accept aligned PCM16LE at
  8–192 kHz with one to eight channels. Channels are explicitly averaged using
  a wide accumulator. Conversion is stateful across compatible frame splits
- Wire audio remains 48 kHz mono Opus in 20 ms packets. A rate/channel/context
  change, a causal-scope change, an ordered non-audio frame, or graceful End
  flushes the filter tail.
  Only the final incomplete 20 ms packet is zero-padded. Each input frame is
  capped at one second and must fit the serialized-frame byte limit
- Normal transport JSON is ordered after preceding normal audio. Urgent JSON
  has an independent bounded lane and can overtake normal output. The native
  client still serializes actual track publication
- No network submission is called `Played` or `Discarded`. The adapter emits
  no speaking receipt and makes no heard-history commitment. Playback-associated
  `TtsText` and `AggregatedTextProgress` frames are consumed without attesting
  that their text was heard

Resampling uses the existing native `pipecat-audio::SincResampler`, rather than
claiming sample-identical SOXR/resampy output. Settings are fixed for the lifetime
of the endpoints; conversion follows each output audio frame's explicit rate.
The low-level `MoqSender` continues requiring exact 48 kHz/20 ms audio.

## Ownership, pressure, and errors

Each endpoint registers a holder during setup. Input End stops its event pump
and releases its holder after that pump's resampler tail drains. Output End
closes its bounded queues, drains the publisher tasks, and then releases its
holder. Only the last holder ends the native connection. This prevents upstream
input End from closing a connection still carrying final output audio.

Event consumption is transferred once from the client; no connection mutex is
held over network reads or awaited close. The connection remains owned until
explicit shutdown. Event and publication tasks use core processor ownership.
Each publication is bound to its original core context, while the connection's
own generation fence rejects stale audio and interruptible JSON at admission
and at write. The final generation/Cancel check and synchronous local track
commit share the same mutex as admission and interruption; no pacing wait holds
that mutex. Cancelled publication futures invalidate only their own current
native generation; late cancellation cannot erase a later generation.

A converter retains its original causal context as well as the latest chunk's
context. Before appending a chunk, the adapter compares destination runtime/index,
stored invocation generation, and every inherited origin runtime/index/generation.
A different causal scope flushes or discards the old converter under its saved
guard before starting the new stream. This check is independent of background
task survival overrides and does not rely on the optional audio context ID.
Thus unrelated origins cannot hide stale middle contributors in one packet,
while same-scope tiny chunks continue batching without an unbounded guard list.

Interruption discards interruptible queued frames, partial PCM, and filter
history. Uninterruptible queued frames retain their explicit survival behavior.
Already transmitted audio cannot be recalled or claimed stopped in a browser.
Cancel and cleanup terminate the connection and join native tasks. Cancellation
can upgrade an in-progress graceful close. A dropped close future retains RAII
shutdown and publishes a terminal failure for concurrent close waiters.

Queues bound both item count and serialized frame bytes, including metadata.
Normal admission waits within `admission_timeout`; urgent admission fails fast.
One active item and one normal admission waiter may sit outside each queue's
stored budget. Network event/command and transcript limits remain independently
bounded by `MoqConfig`. Core background-task and worker-output limits also apply.
Set `PipelineConfig.shutdown_timeout` to cover the admitted audio duration;
it is an additional overall End deadline, not an unlimited drain guarantee.

`MoqTransportState`, a read-only shared processor health handle, and typed core
error categories expose failures. The caller cannot mark the closed transport
usable again through its health view. Permanent connection/pump failure makes
both endpoints unusable; core worker policy decides whether to end or cancel.
Bad individual output frames report invalid request without pretending that the
underlying connection permanently failed.

## Explicit boundaries

Unsupported output destinations, scheduled PTS, video/images, DTMF, gain/volume,
filter/mixer controls, and MoQ-specific pause/resume fail explicitly. This adapter
is not the generic media output implementation and has no playback-ack backend.
Core applies targeted processor queue pause/resume independently of a processor's
returned error. Such controls must not be used as transport playback controls:
the unsupported-control error does not undo their core scheduling effect.
No runner/CLI or browser integration is supplied here. No callback parity for
source track-subscription or reconnect lifecycle is claimed.

Serve mode, reconnect/replay across new sessions, browser buffer reset, browser
execution, mutual TLS, and non-Lite-06 interoperability remain unsupported by the
native client. TLS verification remains enabled. IPv4 loopback is the qualified
network scope; IPv6, actual browser, and hosted relay acceptance remain separate.

## Source mapping

Pinned original revision: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
Reference: `src/pipecat/transports/moq/transport.py` and
`tests/test_moq_transport.py`. Input/output setup, shared holder release,
upstream messages, 48 kHz Opus wire rate, interruption pacing reset, and
pipeline error routing map to the native `pipeline` module. Original serving,
redial, callback, and runner tests are not represented as completed parity.
