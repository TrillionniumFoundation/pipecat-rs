# Native Azure Voice Live PCM application

This crate implements the pinned Python Azure Voice Live service's **distinct**
flat-session WebSocket dialect, not Azure OpenAI Realtime GA. It owns one socket
actor and composes the actual native paired user/assistant aggregators and final
`ToolExecutor` in `VoiceLiveApp::new(config, context, local_tools, output)`.
`start()` returns the real worker, read-only paired history, native tool journal,
and a restricted provider handle with response-completion, playback and actor-join
observations. No raw action sender is exposed by that application handle.

## Supported bounded profile

- Explicit resource endpoint and model; API version `2026-07-15` from pinned source.
- Explicit API key **or already supplied** Entra bearer token, never both.
- Mono PCM16 little-endian at 24,000 Hz in/out; no resampling or device access.
- `AzureStandardVoice` name, `azure-speech` input transcription, optional
  `azure_deep_noise_suppression`.
- Explicit manual turn detection (`turn_detection: null`) or explicit
  `azure_semantic_vad` with server response/interrupt enabled and auto-truncate
  disabled. Semantic speech events propose turns through the native turn strategy;
  they never also cause a client audio commit/response create.
- Bounded ordinary text history, explicit text append/run, final-only local tools
  with identical immutable offered schemas and registered interruptible callbacks.

Endpoint input must be query-free `wss://HOST/voice-live/realtime` or a query-free
HTTPS resource root. Only literal `ws://127.0.0.1:PORT/voice-live/realtime` and IPv6
`[::1]` are accepted for synthetic local acceptance. Credentials, fragments, wrong
paths, normalized/dot paths, arbitrary queries, remote plaintext, blank/control
credentials and overflow-prone limits fail before connection. The actor encodes
exactly one API-version and model query and sends exactly one sensitive auth
header. No redirects, retries, token callbacks, minting, refresh, ambient identity,
reconnection or automatic credential fallback are implemented.

## Conversation and ownership

Startup waits for `session.created`, sends the one flat typed `session.update`,
and waits for `session.updated`. Audio/history cannot reach the wire before ready.
The initial supported text snapshot is seeded once without inference. Send native
`LlmRun` to explicitly request a greeting. Leading system text becomes instructions;
explicit configured instructions take precedence. Remaining system/developer
roles are represented as user text in the source adapter's one-user-item packed
history. No opaque content, history tool authority or lossy media projection is
accepted. Live history replacement, settings/tool mutation and direct upstream
caller injection are rejected. Read-only history exposes snapshots, not mutation.

Final input transcripts retain bounded item identity and append durably through the
pair without echoing the audio turn as text or generating another response. Their
input authority is not invalidated by an unrelated output interruption; interims
remain droppable. Empty
repeat completions do not consume an outstanding claim. Separate generated-text
notification and TTS-aggregation views append assistant transcript once. One
response with multiple audio items has one balanced speech lifecycle. A validated
function-only response opens and closes a real LLM lifecycle for native paired
continuation, without inventing an assistant message or speech lifecycle.

Only the real executor's validated native terminal receipt can settle a local
call. Public results, retagged frames, cancellation requests, origin strings and
journal observations cannot. String successes are preserved verbatim; other JSON
successes are serialized. Failure/timeout/cancel wire projections explicitly say
failed/timed_out/cancelled and retain the actual native terminal journal state.
Wire settlement alone does **not** authorize inference: the paired assistant's
sealed continuation preserves native activity/playback policy. The actor defers
that request until the active response and all announced calls settle. This is a
deliberate safer native composition difference from Python's direct callback run.

A request error frees only its exact pending `response.create`, using nested
`error.event_id`. Unrelated/missing IDs do not free it. An active-response collision
waits for the actual server response; expected cancel/truncate races do not create
new work. Failed responses close only opened lifecycle frames, report failure,
and release already owed continuation without becoming successful completions.

Initial response/item/call announcements must establish their real identities.
Optional subsequent correlation IDs are accepted only when one announced binding
is unambiguous. After interruption, missing output/completion correlation fails
closed because stale omitted-ID traffic cannot be distinguished safely. This is
stricter than permissive Python behavior, not a claim to all optional-ID cases.
Session item/call/response/transcript budgets retain deduplication only in that
single session; no reconnect deduplication is promised.

A proposed tool is not yet an executed tool. Private native frame identity tracks
its queue handoff; a stale proposal can be discarded before mapping, after the
last original unentered frame expires, or at the exact executor-entry guard
before the real executor is called. The app guards entry to the actual
executor against the original response scope. Entered work still requires its
real native receipt, even if interruption wins afterward. No synthetic cancelled
result is invented for a call that never entered the executor. Standalone
`VoiceLiveProcessor::new` accepts only manual profiles without offered local tools
before side effects; the owned `VoiceLiveApp` topology supplies local tool execution
and native semantic-turn admission. The public low-level `VoiceLiveClient` remains
an explicit wire owner, not a replacement for that native turn topology.

In semantic mode, each server start/stop pauses later socket-event and inference
admission until an exact private, noninterruptible flush traverses the real native
user processing and returns to the provider. The native system work precedes this
normal marker at every queue, including muted/duplicate no-op proposals. Thus an
adjacent server speech-start/stop/response burst cannot bind a fresh response before
the prior native interruption is processed. This is bounded by the operation timeout;
close/cancel and real final tool settlement remain independently serviceable.

Interruption fences queued/future stale output immediately, cancels only actual
pending/active response work, and drops stale inference debt. In manual mode a
private admission barrier orders previously admitted PCM before clearing the
input buffer and replaying only its bounded recent suffix (500 ms by default,
subject to the configured cap and validated speech-control changes). Semantic
mode does not use manual clear/replay. Actual playback acknowledgments must name
known item/content, be monotonic, and not exceed generated audio. Without a real
receipt the app cancels/fences but invents no truncation. File bytes and elapsed
wall clock are not playback evidence. Server automatic truncation is not admitted.

Graceful End uses a private identity barrier, drains earlier input, active response
and already-announced native final writes, suppresses new inference and rejects
new function work. End explicitly abandons any incomplete semantic admission,
without acknowledging its marker or inventing native work/settlement, so its normal
lane cannot deadlock the final-drain gate. Only existing response/tool work drains;
new semantic controls/inference and unsolicited responses are not admitted. An
exact late abandoned marker is swallowed without turning abandonment into success.
Cancel is independent and can supersede End. Setup, receive,
output, drop and cancelled waiters retain a real actor owner; terminal observations
are published only after that actor joins. Available primary/cleanup errors are
retained, without claiming core diagnostics it does not surface.

## Recorded PCM executable

Build/run from `rust/`:

```sh
cargo run --locked -p pipecat-azure-voice-live --example pcm_session -- --help
```

Set `AZURE_VOICE_LIVE_ENDPOINT`, `AZURE_VOICE_LIVE_MODEL`,
`AZURE_VOICE_LIVE_VOICE`, and exactly one of `AZURE_VOICE_LIVE_API_KEY` or
`AZURE_VOICE_LIVE_BEARER_TOKEN`. `--check-config [--manual|--semantic-vad]` validates
that same public application without network/file creation. Then use
`--manual INPUT.pcm OUTPUT.pcm` or `--semantic-vad INPUT.pcm OUTPUT.pcm`.
Input is finite, aligned raw PCM (not decoded WAV); semantic input must include
end-of-speech silence. Output must be a new file. Input is paced while output is
drained concurrently. Completion requires real response observation, complete
clean pipeline report, native settlement checks and actual actor join, not just a
nonempty recording. A failed session can leave a partial newly-created output.

The facade feature is `pipecat/azure-voice-live`, exported as
`pipecat::azure_voice_live`.

## Source and nonclaims

Source parity baseline:
`ProfHepta/pipecat@af9f9da9d7afddaa8811c0e0251b32566c980bdf`, Voice Live
`llm.py`, `events.py`, adapter, example and six original test modules. The API date
is this source's contract, not a claim of current Azure subscription availability.

No live credentials/providers, model/voice entitlements, WAN/TLS proxy quality,
physical playback or speech/VAD/noise quality are qualified by synthetic tests.
No alternate API versions/output rates, resampling, G.711, custom/native/personal
voices, echo cancellation, multilingual/other VAD, custom speech/lexicons, live
settings/tools, built-in/remote tools, progressive or detached async tools,
noninterruptible callbacks, camera/avatar/video, browser/PSTN/WebRTC,
reset/reconnect/resumption, generated projects, hosting or deployment. This is a
bounded application, not full Voice Live family parity.
