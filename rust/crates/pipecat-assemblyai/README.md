# Native AssemblyAI recognition

Copyright (c) 2024–2026, Daily. BSD-2-Clause; repository LICENSE applies.

This crate implements AssemblyAI streaming `/v3/ws` and Sync multipart
`/v1/transcribe` directly in Rust. It does not invoke Python, a provider SDK,
a remote compatibility server, or a credentials-discovery helper.

## Streaming

Construct `AssemblyAiConfig::new(explicit_api_key)`, adjust its typed
`StreamingSettings`, and validate with `AssemblyAiClient::new`. Direct clients
use 16 kHz when no sample rate is given. `AssemblyAiSttProcessor` inherits the
pipeline input rate when no rate is specified; an explicit service rate wins.
Audio frames must actually match that rate and have one PCM16 channel. No
resampling or channel reinterpretation is implied. The processor can encode
PCM16 to configured G.711 μ-law with the existing native audio codec.

`client.connect()` owns a bounded WebSocket actor. `session.sender()` accepts
bounded audio, ForceEndpoint, settings updates, assistant context, and Finish.
Consume `session.next()` concurrently with producer work. An unconsumed full
event queue fails after the configured write timeout rather than retaining
unbounded audio or transcripts. The default is 32 input/output entries, 1 MiB
per audio command and provider message, 10-second connection/write timeout,
5-second termination deadline, and 2-second close bound. Pending audio is less
than one 50 ms chunk. Configurable ceilings are 128 queue entries, 4 MiB per audio/provider message,
and five minutes per timeout. Settings JSON is bounded to 64 KiB; carryover to 1,500
Unicode scalar values; model and credentials also have explicit limits.

- Models default to `universal-3-6-pro`; `u3-rt-pro*`,
  `universal-3-5-pro*`, and `universal-3-6-pro*` have U3 features
- Pipecat turn mode is default: U3 silence minimum defaults to 100 ms and
  maximum equals minimum; legacy streaming uses actual source threshold 1.0
  and minimum silence 160 ms. VAD stop sends ForceEndpoint
- Provider turn mode requires U3. SpeechStarted and final turns produce proposed
  speaking frames in both directions. The service does not inject interruptions
  or resolved UserStarted/UserStopped frames. External strategy metadata carries
  the configured `should_interrupt` recommendation
- Nonempty provider turns emit interim/final frames, raw word/timing metadata,
  speaker labels and optional `{speaker}` / `{text}` formatting. Language
  confidence below 0.7 or unknown language falls back to English. Finals require
  end-of-turn plus the configured formatting gate
- In Pipecat mode a formatted final confirms an outstanding ForceEndpoint;
  provider mode finals are finalized. An empty final never invents transcript
  content, but can complete an already proposed speaking turn
- Settings deltas use JSON objects. Missing fields retain values, null clears
  optional values, and empty language lists clear steering. Only agent_context
  and language_codes update a live session; every other effective change drains
  the old socket and reconnects. A mixed delta reconnects without redundant hot
  messages. Invalid settings leave active settings unchanged. Over-ten-language
  updates drop that field while applying valid other changes, matching source
- Regional language variants resolve to base codes, preserve declaration order
  and deduplicate before the ten-code streaming limit. Sync has no ten-code cap
- Automatic assistant carryover is disabled at previous_context_n_turns=0;
  explicit context updates remain available. Empty explicit context is a no-op;
  latest nonempty carryover is retained as the next connection seed

Graceful Finish flushes the sub-chunk audio tail and sends exactly one Terminate.
The actor drains final results through the provider Termination acknowledgement.
The pipeline owns its End/Stop: the provider acknowledgement does not inject a
second terminal frame. Cancellation and session drop bypass full queues, discard
queued results, close with a bounded wait, and **do not** flush audio or send
Terminate. Connection failure is surfaced; ambiguous audio is never replayed and
there is no automatic recovery loop. Settings-requested reconnects are explicit.

## Security and privacy

Keys are supplied by the application. Authorization is a header, never a query
parameter. Debug configuration omits keys, URLs, prompts, context and settings.
Provider bodies and transport errors are not copied into error text or logs.
No HTTP redirects are followed in Sync. Plaintext endpoints are restricted to
loopback; other endpoints require TLS. Endpoints reject embedded credentials,
query strings and fragments. Live audio/context transmission remains the
application's responsibility. Test keys and transcript strings are synthetic.

## Deliberate differences and open qualification

- Native strict bounds, unknown-setting rejection, required wire types and
  numeric checks replace Python's more permissive validation/coercion. Native
  model configuration is a nonempty string rather than Python's nullable model
- Duplicate/older finalized turns are dropped. Duplicate SpeechStarted is
  idempotent. Empty final/termination closes an active proposed turn. Source
  does not consistently protect these cases
- Malformed, binary or oversized provider messages end the session with a
  redacted error. Source logs/skips malformed JSON and may include private
  provider text in logs/errors
- μ-law uses 1 byte/sample for the 50 ms chunk size; source unconditionally
  multiplies by 2. PCM16 retains the original 2-byte chunk contract
- Runtime reconnect settings are revalidated and turn-mode silence invariants
  re-applied; Python configures the mode only at construction
- Deprecated Python `ConnectionParams` and keyword aliases are not exposed as
  artificial Rust compatibility wrappers. Canonical settings and native APIs
  implement the actual behaviors. Python callbacks/decorators/tracing spans are
  not recreated. `EndOfTurn` is a typed stream event; the frame processor does
  not expose a Python callback registry
- Direct streaming events retain raw provider result metadata; callers control
  whether to record it. Existing pipeline usage frames report accepted audio
  duration, not provider billing or latency certification. Streaming source-base
  TTFB timeout accounting and opt-in idle-silence keepalive are not implemented;
  TTFS recommendation defaults to 0.42 seconds and is configurable
- The integration User-Agent uses this Rust crate's package version. It does not
  impersonate the Python package version
- No live AssemblyAI credential, paid request, provider accuracy, latency,
  production TLS endpoint, hardware microphone or playback was tested. Loopback
  protocol and pipeline evidence does not establish live-provider acceptance

See `SOURCE-MAP.md` and `SYNC-SOURCE-MAP.md` for the pinned source/test mapping,
Sync segmentation details, and remaining differences. Run the focused crate
suite and strict lint against an immutable source snapshot with an exclusive
Cargo target directory. The workspace lockfile must be retained.
