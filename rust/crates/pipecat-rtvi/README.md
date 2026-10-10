# Native RTVI processor and observer

RTVI 2.2.0 pipeline behavior implemented in Rust, using `pipecat-protocol`'s
existing envelopes and client codec. There is no Python execution, subprocess,
remote code evaluation, URL download, or UI command execution in this crate.

## Processor

Place `RtviProcessor` before the input transport. Feed client JSON as an
`InputTransportMessage` frame, or use `RtviHandle::handle_bytes` for a direct
transport integration. Outbound messages are `OutputTransportMessageUrgent`
frames containing the RTVI envelope. `RtviHandle::subscribe` provides bounded
application events; a slow receiver observes Tokio's `Lagged` error.

`client-ready` requests streaming with `InputTransportStartAudioStreaming` and
records the correlation ID. The application explicitly calls `set_bot_ready`.
The acknowledgement waits until both sides are ready; this avoids an empty
request ID when the application becomes ready first. Repeated readiness requests
receive their own correlated response. The 1.x protocol version is echoed in
normalized numeric form; invalid/unknown/incompatible versions emit the source
warning and still connect. A malformed repeat retains the previous valid version.
Default bot identification truthfully identifies `pipecat-rs`; applications may
supply their own `about` and capabilities.
Readiness validates the complete response, including correlation ID, capabilities,
and bot identification, before replacing session state. Oversized configuration
or a queued readiness race is rejected without stopping the command worker.

Supported client commands:

- `send-text`: interruption in both directions, asynchronous round-trip pipeline
  flush, user-message append, run/no-run, and temporary audio-response setting
- `send-file`: embedded image bytes become `UserImage`; other embedded or URL
  files become `UserFileDetailed`, preserving content, filename, MIME type,
  custom options, and run/audio options. Full base64 data URLs are accepted.
  URL sources remain unresolved, as in the reference processor.
- `raw-audio`, `raw-audio-batch`: validated PCM16 frames with sample rate/channels
- `dtmf`: ordered individual native keypad frames
- `llm-function-call-result`: typed tool-result frames, with no tool execution
- `client-message`: typed request frame and application event; responses preserve
  both the request ID and the custom message type
- `ui-event`, `ui-snapshot`, `ui-cancel-job-group`: validated, typed frames and
  application events. A cancellation request does not itself cancel a job.
- `disconnect-bot`: native `EndWorker` request ending the running pipeline

Pipeline errors also emit RTVI error events without consuming the original error
frame. Malformed payloads receive correlated errors when a valid envelope exists;
invalid transport envelopes receive uncorrelated error events. Foreign labels are
ignored. Validation diagnostics are concise Rust messages, not Pydantic strings.

The command queue, optional event stream, message bytes, audio-batch size, and
DTMF sequence size are bounded. IDs are limited to 512 bytes and types to 128.
Escaped IDs must also leave room for a bounded correlated error response.
Malformed PCM and base64 are rejected strictly, rather than silently accepted or
partially decoded. Saturation is explicit (`RtviError::Full`); no unbounded task
or queue is created per request. End and Cancel stop the message task and discard
pending commands, matching the source shutdown behavior. Send-text flushes have
a configured timeout; cancellation cannot append the pending text afterward.
A processor instance is single-session and cannot be restarted.

## Observer integration

`RtviObserver::observe(frame, Observation)` is an explicit push-hook API returning
native RTVI envelopes. `Arc<RtviObserver>::tap(source)` creates a frame-preserving
`FrameProcessor` for real pipelines. Multiple taps share state and a bounded
recent-frame identity cache to prevent repeated reports. Names in
`ignored_sources` must match the source names used by the host.

Ordinary taps report speaking/mute/interruption states, transcription, LLM/TTS
lifecycle, raw LLM text and sentence transcriptions, last-user context text,
markers, internal/external function-call lifecycle, metrics, custom server
messages/responses, UI commands, and UI job groups. Tool reporting defaults to
ID-only; per-function `disabled`, `none`, `name`, `arguments`, and `full` levels
control exposure. Runtime configuration is accepted only through a typed
server-side `RtviConfigureObserver` frame. No client wire command raises access.

Only call `observe` with `after_output_transport: true` for an actual push from
an output transport after its timing/playback handling. It enables aggregated
`bot-output`, `bot-tts-text`, and segment progress reports. Ordinary pipeline taps
never infer playback completion from synthesized text or received audio. Queued
segments wait for bot-started-speaking, preserve original frame IDs, and are
cleared on interruption/cancellation. The 1.x branch suppresses progress and
allows word/token bot-output; the pinned source populates both `spoken` and
`will_be_spoken` fields in initial aggregate events, which this crate preserves.

Pending segments, their text, the sentence buffer, message size, and recent frame
identity history are bounded. Identity deduplication is limited to that recent
window; a global hook should pass accurate `first_push` information. Output
progress is separately gated by actual output-transport provenance.

## Explicit compatibility gaps

This is not complete Python observer parity:

- The core runtime has no automatic global on-push observer registration. Hosts
  must install taps or call `observe`; frames never crossing those points are not
  observed. There is no fake global observer or automatic playback acknowledgement.
- BS.1770 rolling audio-level reporting and process-wide system-log forwarding
  are not implemented. Raw PCM audio is not duplicated onto the RTVI event stream.
- User-defined asynchronous bot-output transforms and the deprecated two-argument
  transform signature are not implemented.
- Metrics map the five current native metric variants. TTFA, TTFAT, optional model
  names, and extended cache/audio/reasoning token fields require richer core
  metric payloads and are not fabricated.
- Legacy deprecated `handle_function_call`/`llm-function-call` compatibility is not
  provided; supported tool phases use the current event contracts.
- Python dynamic classes, event decorators, and ignored processor object identity
  are represented by typed Rust events/configuration and explicit source names.

## Verification

`cargo test --locked --offline -p pipecat-rtvi`

Tests use native `Pipeline` execution, a real loopback WebSocket connection,
source-inspected golden wire fixtures and an AST-derived fixture for all nine
RTVI frame classes. They cover ready correlation/version compatibility, text
interruption/flush/order, embedded and URL files, PCM/DTMF/tools, UI, unknown and
oversized messages, bounded-queue pressure, concurrent requests, cancellation
during a blocked flush, observer filters, metadata privacy, and playback evidence.
The reference source commit and file hashes are stored with the fixtures.
