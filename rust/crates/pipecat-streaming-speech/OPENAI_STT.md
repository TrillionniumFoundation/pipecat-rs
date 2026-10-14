# OpenAI transcription-only Realtime STT

The optional `openai-stt` feature provides a dedicated streaming recognition
protocol. The facade enables it through the same feature name. It is separate
from the conversational `pipecat-realtime` client: no response creation, output
audio, tool dispatch or arbitrary JSON command interface is exposed.

## Fixed source and first native profile

The reference is [`OpenAIRealtimeSTTService`](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/openai/stt.py)
at baseline `af9f9da9d7afddaa8811c0e0251b32566c980bdf`, Git blob
`81cce1b3e03030409b040f61e88e53a8d2e56810`. This is a fixed-source adapter, not a
claim about current account/model eligibility or live API acceptance.

Connections use bearer authentication and `intent=transcription`. After
`session.created`, the client sends `session.update` with type `transcription`
and only `audio.input`. Local VAD is selected explicitly with
`turn_detection:null`. The initial native profile requires mono signed PCM16LE
at 24000 Hz. Resampling belongs upstream, where the existing InputTransport can
perform it; the source's internal SOXR resampler is not reproduced.

Configuration is immutable for a session. Server/semantic VAD, automatic
reconnection or audio replay, runtime settings updates and general conversation
operations are outside this first profile. The source accepts additional modes
and reconnects through its WebSocket base class. Native readiness waits for
`session.updated` before writing queued audio, with a deadline; the original
service merely records readiness and does not gate audio on it. This is an
explicit safety adaptation rather than an assertion of identical scheduling.

Readiness additionally requires a matching session ID, transcription type,
24 kHz PCM input, the requested model, explicit null turn detection, and no
output-audio configuration. Known transcription events require a nonempty item
ID, an unsigned content index, and the corresponding string payload (or an
error object for failure). These fail-closed checks are stricter than the
source's permissive field defaults and readiness flag.

The source default model is `gpt-realtime-whisper`, with language `en`. Language
tags use their lowercased base code. A nonempty prompt is omitted for that
specific model, as in the source. Optional input noise reduction and transcription
delay are typed configuration, not provider capability certification.

## Input, transcript and interruption semantics

Downstream input/user PCM is copied to `input_audio_buffer.append`; an upstream
audio frame is not sent. The original audio envelope can pass through unchanged.
Local VAD stopped frames, including their detailed form, enqueue a commit in
either direction, matching the source subclass. A generic UserStoppedSpeaking
frame does not create a commit or a model response.

Each delta is emitted independently as an interim transcription. A completed
event emits its entire transcript with `finalized=false`, preserving the source
frame default. Neither delta accumulation nor sorting by `item_id` or
`previous_item_id` is performed. The bounded provider event is retained as result
metadata, not interpreted as native authority. The configured user ID and the
local observation timestamp accompany the text. Provider completion does not
grant a native turn-finalization, tool, context or heard-history capability.

This is a user-input service. Bot interruption leaves its user audio and socket
alive, as in the source and the existing native Deepgram STT adapter. It does not
adopt the conversational output processor's bot-generation cancellation promise.
Run/session identity and processor-health generation reject newly selected
events from retired input sessions. Selection and queue admission are separate
points: an already selected publication can race a subsequent health setter.
There is no claim that a setter retracts an already selected/queued frame.
After the retired reader and cleanup have joined, no future publication remains.

Mute suppresses new audio submission. It does not clear already submitted user
audio, suppress its transcripts, or prevent a VAD stop from committing that
buffer. This follows the source base-class/subclass interaction.

## Commit accounting and shutdown

Only successfully written local commits create unresolved admission slots.
A new committed item ID associates one slot; duplicate acknowledgments cannot
consume another. A terminal event settles an acknowledged item once. A terminal
received before that item's acknowledgment still projects its text, but grants
no completion credit. A new terminal after the acknowledgment is required;
otherwise graceful drain times out. This deliberate stricter profile avoids
guessing which future commit an early/unknown item belongs to. Transcript
delivery still follows wire arrival order, independently of drain bookkeeping.

Seen item IDs are retained for the session rather than evicted and accidentally
reused. Reaching the configured bound fails explicitly. An absolute unresolved
batch deadline is not refreshed by duplicate or unknown events. Graceful Finish
is ordered behind preceding commands and waits a finite time for submitted
items. It does not implicitly commit unsubmitted residual audio.

The session retains its join handle through borrowed asynchronous shutdown.
Canceling that shutdown future does not lose the owner. The processor retains
the session separately from its tracked reader so cleanup can close and join
even after that reader is aborted. Standalone Drop requests cancellation;
it is not described as synchronously awaiting task completion.

Defaults bound each command/event queue to 32 entries, a wire message to 1 MiB,
raw PCM per append to 64 KiB and tracked item IDs to 1024 for the session. IDs
are at most 512 UTF-8 bytes. The configured queue-count × message-size product
must fit within 64 MiB for each queue; this is a content budget, not an exact
heap bound for JSON/container overhead. Connection/readiness/write/response/
close deadlines are independently positive and no greater than 24 hours.
Applications should keep the core shutdown deadline long enough for the
configured close/drain behavior; a forced core timeout is not graceful joining.

Endpoint configuration accepts the canonical OpenAI WSS endpoint and loopback
test endpoints. Userinfo, preexisting query strings and fragments are rejected.
The client adds its own intent query and marks the bearer header sensitive;
diagnostic formatting does not expose credentials, text, provider error bodies
or configured user IDs.
Canonical-host DNS resolution uses the existing runtime/system resolver. The
owned socket/reader join evidence does not qualify cancellation of every
underlying DNS resolver operation; loopback tests avoid that external boundary.

## Qualification

All protocol and pipeline tests use synthetic loopback peers and dummy keys.
They establish native framing, real PCM transport/resampling, event projection,
bounded queues and lifecycle behavior. They do not establish real authentication,
recognition quality, server model support, a physical microphone or deployment.
Source TTFS/usage metrics, STT metadata projection, runtime settings mutation and
server-VAD turn-strategy integration are not implemented by this first adapter.
