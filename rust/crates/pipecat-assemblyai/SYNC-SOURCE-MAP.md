# AssemblyAI Sync STT native source map

## Pinned inputs

- `source/assemblyai/stt.py`, Sync portion beginning at line 1274: SHA-256 `c9cc690628cd53cfaefb20e2398b570334f1db04913ef798123443457f06cd23`
- `source/tests/test_assemblyai_sync_stt.py`: SHA-256 `ae1d3ed30633ca8dcd07da1fc45707edbd5c714ac43c307e20f6433c8c55506e`
- `source/stt_latency.py`: SHA-256 `cd1b1969ceedb228965d88ecc2a9e10bb9c615e3e7c8b332fe6b89d7c2e12227`; `ASSEMBLYAI_SYNC_TTFS_P99 = 0.65`
- `source/stt_service.py`: SHA-256 `9496155ae6a33b5a59229b44fdfe26df9e4479e9993cbe222ffb9533a2ca8589`; native segmentation maps the base service
- `source/language.py`: SHA-256 `50add254e24320691db12091b145cc932897b00afafd308ffc99f41fd137487a`; declared language fallback

## Implemented mapping

| Python surface | Native surface | Evidence |
|---|---|---|
| `AssemblyAISyncSTTSettings` | `AssemblyAiSyncSettings`, `ConversationContext` | Defaults, language precedence/deduplication/base normalization, unlisted base fallback, prompt/keyterms/timestamps, string/list manual context |
| `_transcribe` / `run_stt` | `AssemblyAiSyncStt::transcribe_wav` and `transcribe_pcm` | Real reqwest `POST /v1/transcribe`, multipart `audio` named `audio.wav`, MIME `audio/wav`, optional JSON `config`, raw `Authorization` key and `X-AAI-Model` |
| `_build_config` | `build_config` | Exact default/optional omission, timestamps false preserved, no streaming ten-language cap |
| `_append_context_turn` / `_process_assistant_turn` | Automatic bounded context and `append_assistant_turn` | First request excludes its own result, next request carries prior user and assistant turns in order, blank omission, explicit reset |
| `warm` / speech-start scheduling / cleanup | Shared reqwest pool, tracked prewarm task, separate cancellation token | Real `GET /v1/warm`, model header, no Authorization, swallowed failures, keepalive connection reused by subsequent transcription |
| `SegmentedSTTService` integration | `AssemblyAiSyncProcessor` | PCM16 VAD pre-roll/utterance buffering or complete-frame mode, WAV wrapping, bounded serial segment queue, upstream/downstream VAD and assistant turns, user identity, STT mute and settings updates |
| `TranscriptionFrame` | Native finalized `TranscriptionFrame` | Trimmed nonblank text, source language semantics, RFC3339 UTC timestamp, full structured result |
| HTTP error health | Typed `AssemblyAiSyncError` and native health generations | 401 authentication unusable; 413/429/503 usable; explicit pipeline recovery fences stale output/context; no automatic retry |
| Lifecycle | Native FrameProcessor / tracked worker | Bot interruption preserves user STT; Cancel aborts HTTP and warm; End/Stop flush active speech and drain accepted segments (active-speech flush extends the source base, which drains only already queued segments); exactly one terminal owner |
| Metadata/metrics | `SttMetadataFrame`, processing/STT usage | Exact source service name `AssemblyAISyncSTTService`, default TTFS 0.65; metrics respect pipeline flags |

## Intentional safety bounds and differences

- Explicit credentials only; no environment/credential-store lookup, no provider SDK, Python interpreter, subprocess inference, model download, or live-provider tests
- Endpoint is an HTTPS origin, or literal loopback HTTP for testing; URL userinfo/query/fragment/path prefixes rejected; proxies and redirects disabled to avoid credential redirection; warm has no default or inherited authentication
- PCM16 input is strictly validated: 8–48 kHz, one/two channels, complete channel frames, nonempty PCM, at most 120 seconds; WAV must be complete ordinary PCM16 RIFF/WAVE with no trailing bytes and a finite container bound. This intentionally rejects Python test placeholder bytes `RIFF....WAVEfmt `; tests use valid native WAV
- Defaults: response 1 MiB, settings/config 64 KiB, request 180 s, warm 5 s, four queued work items, 16 MiB aggregate accepted PCM, 1 s pre-roll, and 500 ms trailing silence for VAD segments. Trailing silence is configurable up to 5 s, included in the 120 s bound and usage, and can be disabled; complete-frame mode assumes the caller supplied a complete segment. Configuration has fixed ceilings, plus 120 s segment duration. No unbounded queues or per-request detached tasks
- Automatic context defaults to five turns/1500 Unicode scalar values. An over-budget single turn is clipped to the configured character budget instead of retaining the Python source's unbounded last turn. Explicit manual context remains byte-for-byte identical when within budget and is rejected otherwise. Zero character budget disables automatic context
- Remote error body details are deliberately not exposed, parsed into messages, or logged because providers may echo credentials, prompt text, or transcript content. HTTP status and typed classification are retained; Python problem-detail/error-code display strings are not reproduced
- Settings updates preserve buffered conversation context. Reset occurs for an explicit new conversation or invalidated health generation. A processor instance is single-run; construct another for a new pipeline
- No resampling or channel conversion is implicit. Pipeline rate/channels must match the configured processor. VAD overflow or mixed-speaker utterance discards the affected segment. Caller supplies VAD or complete segments; this module does not claim a neural VAD
- Successful raw response data is intentionally returned on the transcription frame for caller use; there is no transcript/audio/credential logging, and custom Debug implementations redact configuration content
- Trace decorators/event hooks and the Python metrics-manager API are not emulated; native metrics and typed frame contracts are used

## Test surface and qualification

`tests/sync_protocol.rs` uses only in-process literal loopback HTTP peers with explicit fake keys. It parses the emitted multipart envelope, compares audio bytes and MIME/disposition headers, and checks both success and failure wire behavior. Pipeline tests exercise upstream VAD/context, pre-roll, user identity, End flush/drain, interruption survival, cancellation, health changes, byte backpressure, and prewarming cleanup.

This map records implementation scope, not a claim that all tests passed. The parent-owned exclusive Cargo gate must qualify the final synchronized crate and record command/results in the shared provider evidence. No live credentials were supplied or used.
