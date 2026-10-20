# Optional STT idle-silence keepalive

`GeminiClient::with_stt_keepalive(SttKeepalivePolicy::new(timeout))` explicitly
selects silence injection for `SessionMode::Transcription` only. Existing clients
remain disabled. This additive builder does not add fields to `GeminiConfig` or
change existing public config literals. `new(timeout)` uses a five-second check
interval; the public `check_interval` may be changed explicitly. Both durations
must be positive and at most 24 hours. These are native resource/clock bounds,
not advertised Google service limits or an idle-expiry guarantee.

Before connection, selection validates the exact encoded audio message against
`max_audio_bytes` and `max_message_bytes`. The actor retains one encoded silence
message: `floor(input_sample_rate / 10)` mono PCM16 samples, all zero, with MIME
`audio/pcm;rate=<input_sample_rate>` (100 ms rounded down to whole samples).
Supported rates remain 8–96 kHz. No new task, input queue or frame is introduced.

The existing owned socket actor starts the idle clock after `setupComplete`.
Successful real audio writes reset it. Rejected or muted audio never resets it;
keepalive can continue while the dedicated STT processor is muted. Existing
passthrough, speaker attribution and unmute behavior are unchanged. Every
successful silence write resets the idle clock. Checks delayed by other work
are rescheduled from now, with no catch-up burst. Control, socket input and
explicit queued commands take priority over an idle timer.

Silence is written directly through the existing finite, cancel-aware socket
writer. It does not mark the session busy, set `has_audio`, allocate user origins,
change generation, produce input/output frames or invent usage. Actual server
transcription and usage messages retain their normal parsing and attribution.
An otherwise idle session finishes without a synthetic stream-end or a wait for
an imaginary transcript; real admitted audio retains its normal flush and final
transcript deadline. Finish admission disables checks immediately. The final
finish/cancel check is the silence write admission boundary; a write already
admitted may finish, and bytes already submitted cannot be retracted.

Receive-side resumption keeps its existing explicit policy. There is no timer
worker during a replacement handshake. Successful replacement setup resets the
idle/check clocks and replays neither old silence nor real audio. Finish/cancel
during setup retains its existing terminal behavior.

A silence write error or timeout terminates the actor through its normal error
and cleanup path. It is not retried, even with resumption selected, because a
partial write has uncertain delivery. This deliberately differs from Python's
keepalive task, which logs an exception and continues. No live-provider test,
provider acceptance, session lifetime extension, lossless delivery or automatic
write recovery is claimed.

## Source boundary

Pinned upstream `ProfHepta/pipecat` revision
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:
- `src/pipecat/services/stt_service.py`, blob
  `1198fb92413fcde1db1a24a93271bb0d47b133ed`: optional timeout, five-second
  interval, readiness, idle timestamp and 100 ms silence task.
- `src/pipecat/services/google/gemini_live/stt.py`, blob
  `dcce83e04ba07f8024d9965d2bf08330429655b6`: connect/disconnect lifecycle and
  audio/pcm silence submission without base usage accounting.

Native timestamps advance after successful admitted wire writes rather than
Python's pre-`run_stt` timestamp. The timer remains part of the single actor,
not a Python-compatible service/task abstraction. Tests use real credential-free
loopback WebSockets; the private write-failure fixture constrains the actual
WebSocket write buffer to force a terminal write error before bytes are sent.
