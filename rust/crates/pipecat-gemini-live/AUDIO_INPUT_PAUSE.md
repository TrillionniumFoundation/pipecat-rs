# Conversation audio pause

`GeminiLiveProcessor` defaults to unpaused conversation audio.
`with_audio_input_paused(bool)` selects its initial state, and
`set_audio_input_paused(&self, bool)` changes the state without sending a provider
control. The builder state is restored on every native setup, including
sequential reuse. Keep a typed `Arc` alongside the pipeline's shared processor to
control a running service:

```rust,no_run
use pipecat_core::{Pipeline, PipelineConfig, StartFrame};
use pipecat_gemini_live::{GeminiClient, GeminiConfig, GeminiLiveProcessor};
use std::sync::Arc;

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let config = GeminiConfig::new(
    std::env::var("GEMINI_API_KEY")?,
    std::env::var("GEMINI_LIVE_MODEL")?,
);
let service = Arc::new(
    GeminiLiveProcessor::new(GeminiClient::new(config)?)
        .with_audio_input_paused(true),
);
let mut worker = Pipeline::new(vec![service.clone()])
    .start(PipelineConfig {
        start: StartFrame {
            audio_out_sample_rate: 24_000,
            ..Default::default()
        },
        ..Default::default()
    })
    .await?;
service.set_audio_input_paused(false);
// Consume worker.recv() continuously and connect audio to a real transport.
// Call service.set_audio_input_paused(true) to suppress later conversation input.
# worker.cancel("example cleanup").await?;
# worker.join().await?;
# Ok(())
# }
```

The switch applies only to conversation sessions. Dedicated transcription ignores
both this builder setting and setter; its existing `SttMute` frame remains the
recognition-input control. Conversation mode still forwards `SttMute` unchanged
without applying it. Neither control changes capture or downstream playback.

For downstream `InputAudio` and `UserAudio`, the atomic pause load precedes PCM
validation, provider admission and new manual idle-pre-roll retention. Paused
frames, including otherwise invalid or oversized PCM, preserve their original
outer frame, metadata, payload and direction. Upstream audio remains ordinary
passthrough. Unpausing admits subsequent input under the existing validation and
size limits; paused audio is never saved for later replay.

The load is the concurrency boundary: a frame that already observed an unpaused
state may finish validation and admission after the setter returns. Setting pause
does not retract such a frame, remove queued commands or withdraw provider work.
It does not clear earlier pre-roll, end activity, interrupt a turn, or suppress
provider output. Manual `UserStartedSpeaking` continues to send `activityStart`
and flush existing pre-roll while paused. `UserStoppedSpeaking` still ends
activity and clears the buffer, and pre-roll sizing controls remain effective.
Unpausing creates no synthetic activity or audio-stream-end message. The video
pause switch and video throttle remain independent.

## Pinned-source boundary

Reference: ProfHepta/pipecat
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/services/google/gemini_live/llm.py`:

- The constructor defaults `start_audio_paused` to false and stores it once.
  `set_audio_input_paused` only updates that flag.
- `_send_user_audio` returns before sending or appending idle pre-roll when
  paused. The caller still forwards the original audio frame.
- Activity-start and pre-roll-flush handlers do not check pause, so old buffered
  audio can be sent while paused; activity end still clears the buffer.
- Source setup/stop/cleanup and reconnection do not restore the initial pause
  value. Native setup deliberately restores the builder state, as with video
  pause. Native receive-side session recovery is not a new processor setup and
  does not reset this flag.

Native provider input remains downstream-only. Its PCM validation, bounded
pre-roll, zero-window behavior and original-frame forwarding follow the existing
native contracts; this addition does not claim full Python scheduler or service
parity. Independent Python `GeminiSTTService` inherits the STT base rather than
this conversation pause API; see [STT mute](STT_MUTE.md).
