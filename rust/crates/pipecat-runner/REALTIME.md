# OpenAI Realtime browser voice

Build the existing `pipecat-browser-agent` with the optional `realtime` feature,
then select `--realtime openai`. The default remains the original cascade.
Only `OPENAI_API_KEY` is read as a credential in the selected mode. Optional
`OPENAI_REALTIME_MODEL`, `OPENAI_REALTIME_VOICE` and `PIPECAT_SYSTEM_PROMPT` select
the model, voice and instructions. No STT/TTS client or credentials are created.
The default model comes from the native realtime adapter, not a claim of hosted
account/model availability. Help is credential-free.

```
cargo run -p pipecat-runner --features realtime --bin pipecat-browser-agent -- \
  --realtime openai --assets /existing/browser-assets --bind 127.0.0.1:7860
```

`NativeRunner::new_openai_realtime(RunnerConfig, OpenAiRealtimeVoiceConfig)` is the
same production constructor used by the compiled CLI and loopback flow tests.
Its trusted configuration can select an endpoint; plaintext endpoints require a
literal loopback IP under the native adapter policy. Browser `/api/start` bodies
cannot choose endpoints, credentials or modes. The existing one-use capability,
exact path/Origin checks, slot bound and loopback host are retained. `--text-input`
can select microphone-free browser input when assets are supplied.

## Composition and bounds

The actual pipeline is RTVI -> stateful 24 kHz normalization -> retained-text
budget -> paired user -> retained-text budget/upstream event observation ->
OpenAI Realtime -> retained-text budget -> tool rejection -> generated events/text
-> browser PCM output -> paired assistant. Initial shared context is empty;
instructions live in the initial session update. Optional introduction requests
one inference through the paired user, not an independent response command.

Input is mono PCM16, normalized from a stable capture rate in 8–96 kHz. Provider
input/output is always 24 kHz PCM16. Server VAD owns speech proposals, which travel
upstream to the native paired turn policy; transcriptions also stay upstream.
Generic RTVI text appends use the original paired inference cause. No ad hoc
second response is created. Per-frame input and cumulative retained text remain
bounded, including upstream transcripts before the paired user consumes them.
Audio transcript deltas remain native non-appending LLM notifications plus
appending TTS text; the real assistant after output owns context.

RTVI bot-ready is withheld until the native processor receives its first actual
`session.updated`. `session.created` or merely writing `session.update` is not
readiness. The native protocol does not correlate this first update with a
request ID. Readiness timeout, failure and receive closure end the session;
there is no reconnect or automatic paid-work retry.

Generated text and PCM retain the original pipeline invocation fence through the
bounded WebSocket queue, sink readiness and `start_send`. Stale queued output is
discarded on interruption/cancellation. Pending fenced socket admission rechecks
retirement at bounded 20 ms intervals even when the sink does not wake. Data already admitted to the WebSocket
sink is flushed once and cannot be retracted from an OS/network/client buffer.
This is not acoustic playback evidence. `spoken:false` and
`playback_acknowledgements:false` remain explicit. There is no invented playback
receipt or guessed truncation offset; the native provider owns response.cancel.

Cancellation during pipeline startup does not drop the startup future. The
session owner awaits its bounded setup and then cancels/collects the worker.
The five-second pipeline setup budget applies per processor and separately to
Running readiness, not a claimed global five-second deadline. Only the realtime
provider in this composition performs connecting setup; native connection,
operation and close limits are capped at five, two and one seconds. Provider
readiness cancellation uses the already-owned worker. Graceful disconnect uses
the native End path and bounded pipeline shutdown. Before forwarding End, the
output processor awaits an ordered socket-flush barrier (including queue admission,
within five seconds) while output fences are still live; only then may the pipeline
retire. This drains accepted output, not playback. Explicit API stop cancels.
Processing and browser cleanup errors remain failures, and all ordinary exits
await worker collection and transport cleanup before releasing the session slot.

## Deliberate exclusions

Local STT/VAD, cascade selection/settings, custom provider sample rates,
SmallWebRTC/Twilio realtime, tools, image/file input and output-mode changes are
rejected rather than silently falling back. Other vendors, project generation,
simulation and video are separate work. No full Python replacement is claimed.

Acceptance tests use literal loopback peers and dummy credentials, actual HTTP
start/capability/browser encoders, the production typed constructor and compiled
CLI. They do not qualify hosted accounts/models, browser microphone permission,
autoplay, physical sound, public networks or packaging. Test source is not a
passing result; executed results are recorded by the validation owner.
