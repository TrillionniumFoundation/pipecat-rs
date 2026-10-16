# Dedicated transcription metadata

`GeminiConfig::transcription` makes `GeminiLiveProcessor` advertise the existing
core `SttMetadataFrame`. Its service name is the native processor's
`GeminiLiveProcessor`, and `user_turn_strategies` is `None`: no start/stop strategy
is replaced or recommended. Conversation mode retains its existing behavior and
does not emit this STT metadata. This does not add conversation LLM metadata.

## Configured latency

The dedicated STT default is `Some(0.60)` seconds, the pinned Python
`GEMINI_TTFS_P99` value. It is a configured P99 speech-end-to-final-transcript
budget, not a runtime measurement or a promise about a live provider.

`GeminiLiveProcessor::with_ttfs_p99_latency(Option<f64>)` returns
`Result<Self, GeminiError>`. An explicit `Some(seconds)` overrides the default;
`None` selects the pinned STT base-service fallback of 1.0 second when metadata
is generated. `None` does not suppress metadata and does not restore 0.60.
Finite overrides in `0.0..=300.0` seconds are accepted, including zero. Negative,
non-finite, or larger values are rejected. This bound follows the existing native
Gradium/Gladia latency builders; it is not a provider protocol limit or the core
controller's maximum. The builder affects only transcription metadata, does not
change WebSocket setup, and keeps its value across setup and sequential reuse.
Conversation mode ignores it.

The existing turn controller subtracts already-observed VAD silence from the
STT budget while retaining its independent speech-timeout floor. For a turn
still waiting on STT, a 100 ms speech floor and 200 ms VAD stop duration expose
the default 600 - 200 = 400 ms wait after VAD stop. The `None` fallback gives
1000 - 200 = 800 ms. The normal 600 ms speech floor masks the shorter 400 ms
budget; observing a 600 ms stop with defaults alone cannot prove that metadata
was applied. A final transcript can release the STT wait, so it is also not an
appropriate observation of an outstanding P99 wait. This adapter does not change
controller logic or either floor.

## Frame lifecycle

- Both `Start` and `StartDetailed`, in either direction, are forwarded unchanged
  before the processor calls `ctx.broadcast` with a fresh metadata frame.
- `ServiceSwitcherRequestMetadata` in either direction is answered when its
  `Name` matches the native processor name or its `Local` reference points to
  this exact processor. A different local processor with the same name does not
  match. An unmatched request is forwarded without a metadata response.
- A matching request is answered before forwarding the original request in its
  original direction. Every request gets a new response, including repeated
  requests; there is no lifetime-wide deduplication.
- `ctx.broadcast` creates fresh downstream/upstream frame IDs and reciprocal
  `broadcast_sibling_id` references. It enqueues downstream then upstream. The
  two neighboring queues are independent: ordering described here is enqueue
  order and same-neighbor FIFO, not a global cross-direction delivery order.
- Forwarded Start and request frames retain their original IDs, metadata,
  payloads, and direction. No provider command or extra task is created.

## Pinned source and native boundaries

Reference commit: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

- [`services/stt_latency.py`](https://github.com/pipecat-ai/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/stt_latency.py),
  Git blob `583df6bdc1abaa0e9b2a3bd17669ed286bec3b09`, defines
  `GEMINI_TTFS_P99 = 0.60` and `DEFAULT_TTFS_P99 = 1.0`.
- `services/google/gemini_live/stt.py` takes the Gemini constant as its default
  and forwards an explicit `None` unchanged to the STT base constructor.
- `services/stt_service.py::service_metadata_frame` uses the configured value or
  the 1.0-second fallback. Gemini retains `supports_ttfs = True`, so server VAD
  does not imply a zero budget.
- `services/ai_service.py::push_frame` forwards Start before broadcasting;
  `process_frame` broadcasts on metadata requests, and the STT base forwards the
  request. The pinned Python request handler does not filter its target. Native
  intentionally keeps the Name/exact-Local safety gate used by other native
  service adapters, and uses its native processor name for switcher matching.
- `processors/frame_processor.py::broadcast_frame_instance` creates fresh
  reciprocal siblings on every broadcast. Native uses the shared core helper
  rather than inventing IDs or cloning the request as its response.

This is a local metadata and turn-budget contract. Source fixtures and scripted
loopback tests do not establish live Gemini latency, provider compatibility, or
full Python scheduler/service parity.
