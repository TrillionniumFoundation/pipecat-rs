# Scripted Cartesia user speech

The optional `user-speech` feature connects scripted input to native Cartesia HTTP
TTS, optional private persistence, and the existing paced RTVI PCM input sender.
It does not initialize VAD or ONNX inference. The eval crate already has an
indirect normal compile dependency through pipecat-turns to pipecat-vad and ORT;
this feature does not introduce that dependency or require model execution.
Ordinary scenario loaders and text-only runs keep their previous behavior.

Local compiled-child manifests with repeats and shared invocation accounting
are also available through `suite MANIFEST --user-speech RUNTIME.json`; see
[USER_SPEECH_SUITE.md](USER_SPEECH_SUITE.md). That selected hosted profile rejects
DTMF and bot-audio/duplex composition; the direct API below is unchanged.

## Direct command

```sh
cargo run -p pipecat-evals --features user-speech --bin pipecat-eval -- \
  run scenarios.yaml ws://127.0.0.1:8765 --user-speech runtime.json
```

Example scenario input:

```yaml
name: spoken-input
user:
  modality: audio
  speech:
    service: cartesia
    voice: YOUR_EXISTING_VOICE_ID
    model: sonic-2
    language: en
turns:
  - user: Hello from the scripted user.
    expect:
      - event: llm_response
        text_contains: Hello
  - user: This turn uses a recording instead.
    audio: recordings/second.wav
    expect:
      - event: llm_response
```

The runtime file contains no credential value:

```json
{
  "provider": "cartesia",
  "base_url": "https://api.cartesia.ai",
  "api_key_env": "EXPLICIT_EXISTING_CARTESIA_KEY",
  "request_timeout_ms": 30000,
  "cache_dir": null
}
```

The user supplies the explicitly named existing environment variable. There is
no ambient credential search, dotenv load, credential creation, or credential
persistence. Do not put secrets in scenario files, runtime files, command-line
arguments, or logs. Running an actual provider request transmits the scripted
text and selected voice/configuration to Cartesia and may incur provider charges;
only run it with the necessary authorization. Qualification uses synthetic keys
and local HTTP fixtures, not a live paid provider.

Both library and CLI validate the exact canonical Cartesia HTTPS origin/root;
literal-loopback HTTP is available for local fixtures. The new direct HTTP entry
uses no system proxy, no redirect, HTTP/1, and no automatic request retry. The
existing general Cartesia client's constructor retains its transport policy.
Cartesia success is now strictly HTTP 200 for both the existing public client
and the new direct entry; this is a provider-wide correction matching the pinned
Python HTTP adapter, not only an eval filter. A 201 or 206 body cannot become audio or
a successful cache entry, even if it contains otherwise valid PCM.

## Scenario and API contract

`UserSpeechScenarioFile::load` is a bounded, network-free preflight. It validates
all expanded scenarios before reading any recording payload. `into_sessions`
constructs all sessions without connecting. Recordings use the existing
trusted-local contained-path loader, not an adversarial filesystem sandbox.

Use `CartesiaUserSpeechSettings`, `UserSpeechCachePolicy`,
`CartesiaUserSpeechVoice`, and `EvalSession::with_cartesia_user_speech` for a
programmatically constructed scenario. Existing public scenario/options struct
literals stay source-compatible. A recording override wins regardless of the
order of the two ordinary session builders. Sessions returned by the new file
loader have a frozen recording plan: subsequently replacing their recordings
is rejected, so the shared precharged invocation budget cannot be bypassed.

- Every spoken user turn needs a recording or speech configuration; there is no
  text fallback. Reference text goes to TTS and the local judge, never into the
  bot as fabricated recognized speech.
- Only Cartesia is supported. Scenario credential/endpoint/factory/header fields
  are rejected. DTMF and observation turns do not synthesize audio.
- The eval default is `sonic-2`, 16,000 Hz, mono PCM16, English. The general
  Cartesia synthesis request retains its existing defaults.
- Language strings are trimmed, checked against the fixed Python Language enum,
  then resolved by the fixed Cartesia base-language mapping. Case is significant:
  `zh-TW` resolves to `zh`, while an unrecognized spelling is rejected.
- `sample_rate`, if present, must be integer 16000. `speed`, if present, must be
  1.0. Nondefault speed is rejected rather than silently ignored as in the pinned
  eval factory. Missing/null/empty model selects the eval default.
- At most 16 scenarios and 128 spoken turns; each reference is at most 16 KiB.
  Each utterance is at most 1,920,000 PCM bytes (60 seconds). The HTTP producer is
  bounded before client construction, not only after collection. Actual recorded
  and synthesized bytes share the 15,360,000-byte invocation budget.

The direct command rejects combinations with `--recorded-audio` or
`--bot-audio-wav`. With both audio features, adding `--bot-audio-response` selects
the separate immutable [audio-to-audio plan](AUDIO_TO_AUDIO.md), preserving
spoken `response` and using joint completion/cleanup. This does not relax the
ordinary `with_cartesia_user_speech` builder's transcription refusal or change
this standalone loader's text-response alias. `suite`, release manifests,
Kokoro, arbitrary Python factories, other providers/codecs and physical playback
remain outside this feature. DTMF requires a compatible endpoint; the current
native EvalBotHost does not implement it, and the joint audio profile rejects it.

## Timing, completion, and cancellation

Synthesis starts on turn admission, after existing send-after/bot-quiet handling.
The assertion deadline is anchored before cache lookup or HTTP completion.
Provider/cache progress is multiplexed with incoming endpoint events and fixed
absolute deadlines. A production/cache failure is terminal and cannot be
mistaken for an ordinary assertion timeout or advance into the next user turn.

One utterance is fully received, validated and its HTTP task joined before PCM
playback starts. The sender uses 1,280-byte frames every 40 ms, pads only the last
partial frame, and retains existing idle silence and no-catch-up-burst behavior.
A bot interruption does not itself cancel the scripted outgoing user utterance.

An early assertion success still waits for local playback admission and requested
cache settlement. Local playback completion means the bounded sender accepted
the last frame; it is not remote consumption, recognition, or physical playback.
The real-route tests use a fixture pipeline which validates the complete
multi-packet utterance before producing the terminal endpoint event. This does
not add or advertise a new production playback-ACK protocol.

The session owns cancellation and cleanup of HTTP, cache worker, playback, and
socket. `SpeechStream::cancel_and_join` waits for actual task termination and is
resumable if its first wait is cancelled. Its `next()` cancellation path also
aborts and awaits the retained task before returning cancellation. Stream Drop
requests abort; it cannot claim an asynchronous join.

## Explicit private cache

Cache is disabled by default. Disabled means no cache worker, lock, directory
lookup or disk write. An enabled cache requires an explicitly supplied existing
private Unix directory; there is no HOME/XDG/global default. Never point it at a
shared or untrusted directory. Existing directory permissions are not changed.

Persistence contains WAV audio and fixed version/lock information, not plaintext
transcripts, credentials or headers. Filenames derive from versioned SHA-256 of
exact-case text and effective voice/model/language/format/endpoint/API-version
configuration. Hashes do not anonymize predictable text, and speech itself is
private data. Protect and retain the cache accordingly. This key format is
intentionally incompatible with Python's lowercase-text cache identity.

A cooperative exclusive namespace lock spans lookup, cold HTTP synthesis and
publication. This serializes cold requests within that namespace to avoid
simultaneous duplicate synthesis. Waiting does not block the session's receive
loop or reset its deadlines. A cache hit makes no TTS HTTP request.

The implementation enforces bounded entries/bytes, counts temporary and old
replacement data, and does not automatically evict files. Before cold synthesis,
a miss reserves one extra filename and 1,985,536 bytes of logical space (including
a conservative WAV-header allowance). A corrupt old entry still counts. A quota
smaller than that allowance can serve compliant existing hits but cannot start
a cold synthesis. Unsafe file types,
symlinks, ownership/permission violations, unexpected links or oversized entries
are errors. Ordinary corrupt WAV entries can be regenerated; unrelated files
are never silently deleted. Trusted-root checks do not protect against malicious
same-UID processes, root, hostile mounts, or arbitrary filesystem-owner races.

Only complete, valid, successfully joined synthesis can be published. Publication
uses an owned private temporary file, identity checks, file sync, atomic commit,
own-temp removal and directory sync. Cancellation before worker-side commit
admission prevents publication. Cancellation after admission may leave valid
cached audio, including when a later playback/assertion fails. An uncertain
persistence result is an error, not a promise of rollback.

The cache worker owns its filesystem handles and lock; the session retains and
actually joins its native thread. Cancellation wakes command waits. A fixed five-second
publication deadline (also capped by the run deadline) is not refreshed by progress polling. Kernel filesystem
operations can be uninterruptible: cleanup may outlast the logical deadline,
and no hard kernel cancellation or bounded Drop duration is promised.

## Verification boundary

The pinned behavior reference is ProfHepta/pipecat
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`. The language fixture executes the
original enum/resolver/eval-language/Cartesia-mapping AST only; it is not SDK or
full Python lifecycle execution. Live provider/model availability and the
remaining framework replacement work require separate qualification.
