# Manual-turn interruption audio pre-roll

The paired `OpenAiRealtimeProcessor` uses a bounded rolling window to preserve
speech onset when local turn detection interrupts a previous response. In manual
turn mode, interruption clears the provider's uncommitted input buffer, re-appends
this window, then performs the existing response cancellation and acknowledged
playback truncation. `UserStartedSpeaking` itself does not trigger this operation.
`UserStoppedSpeaking` retains ordered audio commit and response creation.

## Configuration and ownership

The default window is 0.5 seconds of mono PCM16 at 24,000 Hz (24,000 bytes).
`with_user_audio_preroll_secs(seconds)` sets an explicit duration and takes
precedence over automatic sizing. Otherwise a `SpeechControlParams` frame with
VAD parameters sets the window to `start_secs + 0.3` seconds. Use an explicit
window for a non-VAD turn-start strategy whose detection delay is not described
by `start_secs`.

Durations must be finite, positive, no greater than five seconds, and represent
at least one complete PCM sample. Window sizes are aligned to two-byte samples;
the maximum retained audio is 240,000 bytes. Shrinking keeps only the newest
suffix. Expanding cannot recover samples already discarded. Configuration and
input validation reject unsupported sample formats and limits that cannot carry
one PCM sample.

The socket actor is the sole window owner. An ordinary audio append enters the
window only after its local write and flush succeeds. This is not a provider
acceptance or processing acknowledgement. Previously admitted but unwritten
commands from an interrupted generation retain their existing discard policy;
they are not promoted into the replay window. Audio that has not reached this
actor is not retrospectively classified as successfully sent.

Replay does not consume or reinsert the rolling window. A later interruption can
replay the same recent samples again. Committing a user turn does not itself
clear the local rolling history. The window is released with its session owner.

## Ordering, failure and cancellation

All clear/replay/cancel/truncate work runs in the existing socket owner. Replay
is split into sample-aligned chunks respecting both audio-chunk and JSON-message
limits. The entire transaction shares one operation deadline; a new chunk does
not restart that deadline. A write, serialization or timeout failure terminates
the session instead of allowing a later commit of an uncertain mixed buffer.

No interruption transaction is sent before session readiness. Control updates
retain the latest generation, playback position, resize and close state. A resize
alone is not an interruption. When control updates coalesce, the latest window
size is applied before the pending interruption. Close and generation changes are
checked between transaction stages. A superseding interruption starts another
clear and replay from the retained window, rather than treating partially replayed
audio as new input. Bytes already written cannot be remotely rolled back.

Cancellation and playback acknowledgement retain their existing contracts. The
native actor only sends response cancellation when an active or pending response
exists, and truncates only a supplied acknowledged position. No generated-byte
playback position is invented. This audio-buffer feature does not add reconnection,
provider retries or background tasks.

## Scope and fixed-source differences

This behavior is enabled for the paired manual-turn processor. The standalone
client and server-VAD processor keep their existing behavior. Server-side turn
detection does not use this local clear/replay window. Pause/mute and resampling
remain upstream responsibilities.

The fixed Python service uses the same default duration, VAD margin, explicit
precedence and clear/replay-before-cancel order. Its local append bookkeeping may
run after a no-socket no-op or an error-reporting send, so its window is not a
provider receipt either. Native retention deliberately requires a successful
local flush. Python's unvalidated zero duration can retain the entire buffer via
`[-0:]`; native rejects zero, non-finite, sub-sample and excessive durations.
Python reads its replay snapshot after awaiting the clear send, so another
coroutine can alter its window in that interval. The native single-owner window,
watch coalescing and stage generation checks are explicit ownership semantics,
not a claim that every Python scheduling interleaving is reproduced.

Local WebSocket tests establish wire order, byte suffixes and native ownership
behavior. They do not establish speech recognition quality or acceptance by a
live paid provider.
