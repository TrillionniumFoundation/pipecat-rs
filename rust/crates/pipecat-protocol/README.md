# pipecat-protocol

Native, bounded wire codecs and frame adapters. No Python runtime, network client,
carrier credentials, REST call-control requests, or automatic hangup are involved.

## Implemented boundaries

- Twilio, Plivo, Telnyx, Vonage and Exotel audio/DTMF messages
- Genesys AudioHook v2 PCMU handshake, sequence echo, control replies, pause/resume,
  mono/stereo input, session metadata and Architect output variables
- Exact provider-specific media, clear, mark/checkpoint/notify JSON output; binary
  PCM16LE for Vonage, base64 raw PCM16LE for Exotel, G.711 for the other providers
- Stream start metadata, numeric/string sequence and chunk fields, timestamps,
  stop, mark acknowledgements, cleared and notify events
- Stateless strict parsing with structured errors; compatibility receive methods
  discard malformed JSON/base64/field packets without terminating a receive loop
- Optional session validation for lifecycle, stream identity and sequence order
- Shared `pipecat-audio` G.711 conversion and streaming Sinc frame adapters
- RTVI 2.2.0 client/server envelopes, version negotiation (including legacy 1.x),
  text, raw audio/batches, multi-key DTMF, function calls, transcriptions, errors,
  metrics, file-source DTOs, bot output progress and UI message payloads
- RTVI client-to-core frame mapping mirrors `RTVIClientSerializer`, including
  function cancellation, omitted report-level fields, markers, and interim text
- Five protobuf oneof variants using the original field tags, with optional PTS
  preserved, including a present zero

## Usage

```rust
use pipecat_protocol::{WireMessage, telephony::TelephonyCodec};
let codec = TelephonyCodec::twilio("stream123")?;
let event = codec.parse(&WireMessage::Text(
    r#"{"event":"dtmf","dtmf":{"digit":"#"}}"#.into()
))?;
let clear = codec.clear()?;
# Ok::<(), pipecat_protocol::ProtocolError>(())
```

`adapters::FrameSerializer` supplies synchronous `serialize`/`deserialize` methods
for asynchronous transports. `TelephonyFrameSerializer::new(codec, input_rate)`
keeps separate input/output resamplers. Flush audio tails at graceful stream end;
interruption discards the output tail. Changing output sample rate requires first
flushing buffered samples. Resampling is native windowed sinc, not SOXR parity.

A plain `TelephonyCodec` accepts only explicitly matching encoded format/rate.
It never relabels sample rates. The frame adapter resamples PCM automatically.
The default incoming/outgoing message bound is 1 MiB, configurable for telephony
and parse operations. JSON nesting is also bounded by serde_json.

`TelephonySession` enforces start before media and rejects packets after stop.
Twilio/Plivo sequence values must increase when present; gaps are allowed. Telnyx
sequence order is unchecked by default because its contract explicitly permits
media/DTMF reordering. Applications may select another `SequencePolicy`.
A mark acknowledgement is not proof of playback: providers also acknowledge
marks removed by a clear operation.

## Compatibility scope

These codecs do not establish WebSockets, authenticate request signatures,
originate calls, run a full RTVI processor/observer, fetch file URLs, or make REST
hangup requests. Genesys AudioHook v2 supports PCMU binary audio, open/opened, close/closed,
ping/pong, pause/resume, participant updates, DTMF, barge-in and disconnect.
L16 is explicitly rejected because the upstream implementation does not
implement its media path. Unknown telephony events
and unknown RTVI envelopes are retained for applications, while typed dispatch
rejects unsupported RTVI types. This is wire/codec coverage, not live-carrier or
real-time transport acceptance.

The wire parser deliberately rejects invalid base64, zero audio rates/channels,
misaligned PCM and incorrect configured stream IDs. Python accepted or dropped
some of these cases more loosely. Pipecat's twelve-key DTMF set is retained;
Plivo's additional A–D keys are not converted into Pipecat input events.

## Contract sources

- `src/pipecat/serializers/{twilio,plivo,telnyx,vonage,exotel,genesys,rtvi_client,protobuf}.py`
- `src/pipecat/processors/frameworks/rtvi/{models,processor}.py`
- `src/pipecat/frames/frames.proto`
- `tests/genesys/{conftest,test_genesys_serializer}.py`
- `tests/test_{twilio_serializer,telnyx_serializer,rtvi_client_serializer,protobuf_serializer,runner_utils}.py`
- [Twilio WebSocket messages](https://www.twilio.com/docs/voice/media-streams/websocket-messages)
- [Plivo protocol reference](https://docs.plivo.com/docs/voice-agents/audio-streaming/concepts/audio-streaming-reference)
- [Telnyx media streaming](https://developers.telnyx.com/docs/voice/programmable-voice/media-streaming)
- [Vonage WebSocket concepts](https://developer.vonage.com/en/voice/voice-api/concepts/websockets)

Golden fixture field names follow the sources above, with synthetic identifiers
and small explicit payload bytes. Protobuf golden bytes are derived directly
from the checked-in `.proto` tags; no Python test harness is used.

Run `cargo test -p pipecat-protocol` and
`cargo clippy -p pipecat-protocol --all-targets --no-deps -- -D warnings`.
