# Scripted audio-to-audio evaluation

The joint `user-speech,bot-audio-transcription` feature selection adds one explicit
direct workflow: source-shaped user audio → Cartesia HTTP/private cache or WAV
override → paced raw audio → bot pipeline → received bot PCM → local Silero →
configured HTTP STT → spoken `response` assertions. The ordinary text,
user-speech-only, recorded-input and bot-transcription entries keep their defaults.

## Direct command and immutable library plan

```sh
pipecat-eval run scenarios.yaml ws://127.0.0.1:8765 \
  --user-speech tts-runtime.json --bot-audio-response stt-runtime.json --stop-bot
```

Both features must be compiled in. The command selects one combined branch before
either standalone loader. A missing feature, duplicate/missing/unknown flag or
incompatible selection fails before runtime-file/model/network preparation.
`--stop-bot` requires exactly one scenario. `suite`, `--recorded-audio`,
`--bot-audio-wav`, `--user-images`, DTMF and optional Smart Turn are unsupported
in this selected route. Per-turn `audio` already supplies WAV overrides.

```yaml
name: spoken-conversation
user:
  modality: audio
  speech:
    service: cartesia
    voice: YOUR_CARTESIA_VOICE_ID
    model: sonic-2
    language: en
    sample_rate: 16000
    speed: 1.0
turns:
  - user: "What is two plus two?"
    expect:
      - event: response
        text_contains: "four"
        within_ms: 15000
  - user: "Thank you."
    send_after:
      event: response
    expect:
      - event: response
        text_contains: "welcome"
        within_ms: 15000
```

```rust,ignore
use pipecat_evals::{
    AudioToAudioScenarioFile, BotTranscriptionSettings, Cancellation,
    CartesiaUserSpeechSettings, SessionOptions,
};

let cancellation = Cancellation::new();
let sessions = AudioToAudioScenarioFile::load("scenarios.yaml")?
    .into_sessions_with_cancellation(
        "ws://127.0.0.1:8765",
        SessionOptions::default(),
        CartesiaUserSpeechSettings::load("tts-runtime.json")?,
        BotTranscriptionSettings::load("stt-runtime.json")?,
        cancellation.clone(),
    )
    .await?;
for session in sessions {
    let result = session.run(cancellation.clone()).await;
    // Inspect result.passed and retain the structured failure details.
}
```

`AudioToAudioScenarioFile` parses the bounded include tree exactly once in
audio-response mode. It preserves `expect.response` and `send_after.response`;
explicit `llm_response` remains reported LLM text. Its scenarios, resolved voices,
recordings and exact PCM precharge are private. All expanded scenarios are
validated before any recording payload is read. Complete option, URL, judge and
settings validation precedes native model entry. Every model is prepared before
the first session can connect; an invalid later scenario/model starts no bot or
TTS/STT request. Configuration validation alone does not issue a judge request.

Preparation uses sequential owned model loaders with real Silero preflight
inference and independent recurrent state. At most sixteen model configurations
are retained. Only the session currently being driven starts a VAD PCM worker;
the direct CLI runs sessions sequentially. Native initialization/inference has no
hard cancellation deadline, and ONNX runtime selection remains process-global.

The bot endpoint must accept paced raw-audio input and expose actual downstream
bot PCM, such as an appropriately configured `EvalBotHost`. The evaluator selects
`capture_bot_audio=true` and removes `skip_tts`. It does not provision bot STT,
LLM or TTS services. Reference text is never sent through the bot's text command.

## Runtime settings and disclosure

Use the unchanged strict runtime JSON formats documented in
[USER_SPEECH.md](USER_SPEECH.md#direct-command) and
[BOT_AUDIO_RESPONSE.md](BOT_AUDIO_RESPONSE.md#explicit-cli-run-configuration).
Each file is at most 32 KiB and names its own explicit existing credential
environment variable. No ambient credential fallback, dotenv loading, key
creation, model download or credential persistence is performed. Do not include
credentials, endpoints or headers in source YAML.

Cartesia settings use the canonical HTTPS origin or literal-loopback HTTP
fixtures, with direct no-proxy, no-redirect, no-retry transport. STT keeps its
existing explicit HTTPS/loopback endpoint policy and disables redirects; its
ordinary HTTP client may use environment proxies. The Cartesia transport policy
does not change STT behavior. Optional `smart_turn_model` must be omitted or null
for this joint profile; standalone Smart Turn remains available.

Running against real providers sends user reference text to configured Cartesia
and received bot audio to configured STT, and may incur provider charges. Cache
entries contain private speech and identity hashes, and require an explicitly
selected private local directory. Settings and their cache policy are validated
by their existing constructors before use.

Semantic judging remains separately configured opt-in. Exact/text predicates do
not enable a judge or send judge HTTP requests. With explicit `judge.eval` and
`response.eval`, the judge receives user reference text and completed STT
responses. Parallel LLM/TTS observer text is never substituted as spoken truth.
Existing Yes/No/Continue behavior and epoch fences remain in force.

## Recording precedence, timing and success

Add `audio: recordings/turn.wav` beside a turn's nonempty `user` reference to use
that recording instead of synthesis. Files use the trusted-local contained-path
rules of [RECORDED_SCENARIOS.md](RECORDED_SCENARIOS.md), not an adversarial
filesystem sandbox. For an all-WAV scenario, omit `user.speech` and provide an
`audio` override on every user turn. It still selects combined mode, has frozen
recordings and fixed deadlines, and starts no synthesis owner. Both runtime
settings arguments remain required and validated. Returned sessions reject later
recording replacement, so callers cannot bypass the invocation precharge.

Only the admitted current turn can start cache lookup or synthesis. Audio remains
mono 16 kHz PCM16, paced in 1,280-byte/40 ms chunks with idle silence, tail padding
and no catch-up burst. A recording wins over synthesis. Every cache-hit playback
is logically charged just like fresh audio; no speculative future-turn producer
is started. Bot interruptions invalidate bot-response work without cancelling an
independently admitted user utterance.

Assertion/judge deadlines start at user admission and include cache/TTS latency.
Each playback has one absolute 61-second deadline from queue admission, capped by
the session deadline; it does not slide when more socket events arrive. This
applies to all-WAV combined sessions as well. Cache publication has its existing
five-second settlement budget. TTS timeout is at most 60 seconds; STT timeout is
at most 30 seconds. Receive fences are bounded to 256 ingested packets in rounds
of at most 32, with one additional accounted saturation probe allowed only to
fail the fence. A scheduling yield is not evidence of an empty socket queue.

Passing an early response assertion is insufficient to end the run successfully.
Success also requires user production/playback and requested cache publication to
settle, then a fixed bot-quiet/session-bounded final check of locally admitted
PCM, VAD, segments, HTTP work and retained recognition, followed by a receive
fence. Continued bot output cannot extend that final deadline indefinitely.
Remote `bot-stopped-speaking` does not close a local audio segment.

These receipts establish completion of the local admitted prefix. They do not
prove remote consumption, remote TTS EOF, lack of future network audio or physical
playback. Untagged audio first arriving after a fresh remote start can be
indistinguishable from that new reply. A complete-route test must independently
validate all expected user PCM at its real host pipeline before releasing the
fixture bot response; a queue admission receipt alone is insufficient.

## Bounds and owned cleanup

- One invocation: 1–16 scenarios, at most 128 spoken turns, reference text at most
  16 KiB each, and 15,360,000 logical user PCM bytes across recordings plus every
  synthesized/cache-hit use. An utterance is at most 1,920,000 bytes.
- Original bot capture: at most 8 MiB/4,096 chunks per session, shared across
  epochs. STT additionally retains at most 256 KiB queued PCM, a 30-second segment,
  four pending serial segments plus one HTTP request, and 64 KiB transcript text
  per epoch under existing event/ready queue bounds.
- Combined session options may tighten, but not raise, the existing default wire
  and event ceilings: 64 transport queue slots, 1 MiB messages, 65,536 received
  messages, 64 MiB received wire bytes, and 16,384 events/16 MiB history. Input
  sender traffic has its separate existing ledger; these are not a combined
  duplex traffic budget.
- Private cache: at most 256 files/64 MiB, including quota-charged old/temp data.
  Existing cache/Cartesia constructors enforce these ceilings before preparation.

These are logical payload allowances, not RSS limits. Retained capture, queued
PCM, segment copies, multipart WAV, HTTP bodies, decoded wire objects and
transcripts can coexist. Transport queues, host pipeline buffers, model/runtime
allocations and native thread stacks are additional. Up to sixteen model
configurations does not mean sixteen active VAD PCM workers or a runtime memory
guarantee.

Cooperative `run(cancellation).await` first revokes new media work and signals TTS,
cache, playback and recognizer owners. It then actually settles the owned async
playback/socket/HTTP work and joins native owners. Socket graceful-close timeout
escalates to cancellation on the same retained owner and awaits its retirement;
it does not detach a closer. Primary failures remain primary, with cleanup errors
appended. Native calls or filesystem I/O can delay cleanup without a hard bound.

Arbitrarily dropping or aborting the outer future requests async cancellation or
abort and can synchronously join native owners. It does not establish that all
async owners were awaited. Borrowed cleanup waits retain handles so cancellation
of a wait does not silently lose the ownership needed to resume it.

CLI exit status remains 0 for pass, 1 for run failure/cancellation, and 2 for
invalid/preparation input. Cancellation during owned model preparation returns
`EvalError::Cancelled` after loader cleanup, emits a safe stderr error and exits
2 with no fabricated run JSON. Cancellation after a session starts produces its
ordinary failed/cancelled JSON and exits 1. Both paths retire the signal owner.

## Fixed-source scope and evidence boundary

The reference is `ProfHepta/pipecat` at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- [script_session.py, lines 227–245](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/evals/script_session.py#L227-L245)
  configures user TTS and bot STT together.
- [script.py, lines 742–756](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/evals/script.py#L742-L756)
  preserves spoken `response` in audio mode.
- [base_driver.py, lines 98–124](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/evals/base_driver.py#L98-L124)
  gives recordings precedence over synthesis and text.

This is a finite direct scripted profile. Hosted/repeated user-speech suites,
two-sided timeline recording/mixing, images, DTMF, new synthesis providers,
Smart Turn composition, new codecs/rates, production-provider acceptance and
full Python evaluation/framework parity remain outside it. Existing WAV+STT
support is not new to this entry.

Local qualification uses synthetic loopback HTTP peers, actual `EvalBotHost`
pipelines and real installed Silero inference. A fixture STT transcript proves
protocol/model integration, not production recognizer word accuracy. Host PCM
validation proves pipeline delivery, not microphone/speaker operation. See test
reports for the exact targets and source revision actually run; documentation
alone is not execution evidence.

### Cancellation test boundary

Preparation cancellation is tested through a deterministic owned-loader library
barrier, including the actual native join. The CLI preserves its preparation-error
exit-2 mapping; CLI error negative controls and in-run Ctrl-C/exit-1 are exercised
separately. A CLI SIGINT specifically consumed during model preparation is not
deterministically witnessed by this suite. Thread snapshots cannot establish that
phase, and no production timing hook is installed to simulate it.
