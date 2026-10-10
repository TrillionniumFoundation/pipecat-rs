# Assistant playback readiness

With `pipecat-media-transport/assistant-history`, use
`AssistantSpokenHistorySink::new(assistant.clone())` and place that same
`LLMAssistantAggregator` downstream of the output. The existing media sender
forwards each original source Start and matching TtsStarted, then waits for the
assistant to accept the exact native frame before submitting subsequent PCM.
The assistant acknowledges after successful reduction and bounded outward effect
admission. Signed continuation carriers remain on their original frames.

This prevents an immediate Played receipt from reaching an assistant before its
source timestamp and signed playback context exist. A successful trusted receipt
still commits history synchronously under the existing receipt fence. Nothing
replays signed state, advances the assistant from an upstream observer, delays an
accepted receipt's history write, or rolls committed history back. Adjacent
responses retain the sender's original FIFO order: A's receipt and End precede
B's accepted Start.

The readiness registration stores no frame or processor context. It is bounded
to 16 pending registrations and 256 bytes per TTS context ID. Native frame
identity and a private nonreused reservation ID prevent public-ID spoofing,
clone replay, and an old ticket retiring a replacement registration. Registration
alone cannot start an assistant turn or authorize playback. Direct reducer calls
cannot acknowledge a downstream processor invocation.

The media sender owns each wait in its existing task, outside ledger and assistant
locks, with the existing write timeout and queue interruption/cancellation race.
No dispatcher or detached task is created. Ticket drop, ignored or failed
reduction, expired original scope, purge, interruption, terminal cleanup, and
owner drop retire reservations. A queued success rechecks its original and
assistant scopes before delivery. Failed readiness suppresses that response's
later PCM even after its history ledger entry is cleared. A missing downstream
assistant therefore fails visibly before backend submission. End/Stop drain the
existing sender subject to its bounded deadline; Cancel aborts its wait.

The CLI playback output uses the same source Start handshake before its existing
synchronous `begin_playback` registration and remote audio admission. Its source
readiness deadline is ten seconds.

The media feature is additive. Standalone media defaults do not depend on turns.
Existing generic `SpokenHistorySink` implementations have a no-op readiness hook
and keep their frame-only behavior; they do not acquire this assistant ordering
guarantee. The explicit adapter supports the ledger's single destination,
noninterleaved whole-response context contract. Neither readiness nor a synthetic
Played backend proves physical-device audibility.
