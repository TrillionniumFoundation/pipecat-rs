# Local hosted user-speech suites

The optional `user-speech` feature adds a complete local release-runner route:
a native manifest expands scenarios and repeats, launches one trusted compiled
child per attempt, sends synthesized or overridden user PCM, applies existing
generated-text/function assertions, settles owned work and returns ordered
`ReleaseResult` JSON. “Hosted” means ownership of these local direct children.
It does not mean cloud deployment or certification of a production provider.

## Selection and input

From `rust/`, with a previously configured runtime and compiled bot:

```sh
cargo run -p pipecat-evals --features user-speech --bin pipecat-eval -- \
  suite native-manifest.yaml --user-speech runtime.json
```

Library callers use `ReleaseManifest::load_user_speech(path, settings)` followed
by `release::run_release` or `release::run_release_with_budget`. `settings` is a
validated `CartesiaUserSpeechSettings`. See [USER_SPEECH.md](USER_SPEECH.md) for
the explicit runtime file, existing credential variable, endpoint policy, voice
options, PCM production and private cache configuration.

The existing native manifest schema is unchanged. For example:

```yaml
native_version: 1
concurrency: 2
repeat: 2
base_port: 7900
suite:
  - name: spoken-smoke
    bot: /absolute/path/to/compiled-eval-bot
    args: ["--port", "{port}"]
    scenarios: [spoken.yaml]
```

The bot's arguments must match that executable's interface; no arguments,
interpreter, provider configuration or environment variables are inferred.
`spoken.yaml` can contain:

```yaml
name: spoken
user:
  modality: audio
  speech:
    service: cartesia
    voice: YOUR_EXISTING_VOICE_ID
    model: sonic-2
    language: en
scenarios:
  - name: greeting
    context:
      - role: system
        content: Reply briefly to the spoken user.
    turns:
      - user: Hello there.
        expect:
          - event: llm_response
            text_contains: hello
      - user: This input has a recording override.
        audio: recordings/second.wav
        expect:
          - event: llm_response
```

Named scenarios and bounded contained `!include` use the existing source-style
whole-value replacement rules. A per-scenario `user` block replaces the whole
default block. Each spoken reference needs an effective Cartesia voice or a
WAV override. WAV wins for its turn and produces no synthesis/cache lookup.
All-WAV scenarios can omit `user.speech`. Reference text is never sent to the
bot as user text or fabricated recognized speech.

Prefer explicit `llm_response` and function expectations. The standalone text
parser's `response` alias still means generated LLM text; this route does not
transcribe or evaluate spoken bot output. Judge configuration is unchanged.
The current flat queue, minimum per-label concurrency caps, separate per-attempt
context and attempt-major result order are retained; this is not Python's
coalesced repeated-label queue implementation.

This standalone profile rejects DTMF, images and recording.
`--user-speech` cannot combine with suite `--recorded-audio`; per-turn WAV
overrides remain supported. With both audio features, adding
`--bot-audio-response` instead selects the separate explicit
[joint hosted profile](HOSTED_AUDIO_TO_AUDIO.md), preserving spoken responses.
It does not relax the standalone session builders' combination refusal. Missing,
unknown and duplicate flags are invalid input. A binary without `user-speech`
rejects the flag before reading the runtime/credential or launching a child.
Manifest `record: true` and `cache_dir` remain unsupported: cache configuration
comes only from the explicit speech runtime.

## Frozen provenance and invocation accounting

The public `ReleaseManifest { concurrency, jobs }` shape is unchanged. Each
selected job has a private immutable snapshot of its entire normalized scenario,
effective voice and settings, plus already-read WAV bytes. At execution every
public scenario must equal the bounded canonical snapshot exactly, including
name, references, order, context and expectations. Size/structural checks precede
allocating a comparison copy. Changing it returns an error before child/provider
work. Public job label, attempt, port and concurrency may still be reassigned
when valid; duplicate identities/ports and bot-command mismatches are rejected.
The trusted executable and an existing runner-body path remain mutable local
files, not authenticated or snapshot-frozen assets.

Jobs may be reordered, cloned with valid new identities, or combined from
independently loaded plans. Selected speech may mix with ordinary text/recorded
jobs. A standalone speech job mixed with any audio-response job is rejected before
model preparation. Each by-value invocation revalidates and charges every logical
use, then creates exactly one shared execution budget:

- At most 128 expanded jobs and 1..32 invocation/label concurrency
- At most 128 logical spoken references across selected and ordinary recorded
  jobs, including repeats; a WAV-covered selected turn counts once
- Exactly 15,360,000 bytes maximum admitted logical user PCM. Before any spawn,
  all WAV uses across the complete invocation are precharged by actual length,
  including repeated/cloned Arc-backed recordings and ordinary recorded jobs
- Every successful synthesized or warm-cache clip charges its actual PCM length
  once against that same atomic budget before playback/cache publication
  admission. There is no refund or deduplication by clip/cache identity. WAV
  overrides are not charged a second time when played
- At most 1,920,000 PCM bytes per utterance, 16 KiB per reference, 16 MiB actual
  WAV-file reads during a manifest load and 4 MiB serialized audio scenarios
  across logical expanded jobs. Remaining load allowances are supplied before
  the next bundle's media is read. Existing file/include construction limits
  still apply; private snapshots share immutable bytes across repeats
- Existing cache entry/byte quotas and the 64 MiB release-result retention limit
  remain separate

The PCM allowance bounds admitted logical use, not total traffic or allocator
memory. Up to 32 concurrent producers may each buffer their bounded utterance
before shared admission, a raw one-buffer reference of 61,440,000 bytes plus
existing queue/copy/cache buffers. Refusal prevents playback and new publication
of that clip; it cannot promise provider traffic/cost stops exactly at the cap.
Precharged WAVs remain charged even if cancellation prevents their use. A new
explicit `run_release` invocation receives a fresh quota; there is no global
cross-invocation quota or hidden reset within one invocation.

## Completion and cancellation

A selected attempt keeps its original session/future, direct child and pipe
readers through one ordinary cleanup tail. On success, failures and cooperative
cancellation, admission closes, producers and playback are cancelled/settled,
the final eval-cancel control is attempted at most once, and owned socket,
HTTP/cache and playback work is joined. Borrowed socket close timeout records
failure, cancels and joins that same socket owner. Child kill/wait and both pipe
reader joins complete before the scheduling slot is reused or a result appears.
Cleanup errors cannot turn into a passing result. If the OS cannot reap a child,
that failure is reported; no absolute kernel-stuck completion bound is promised.
Result retention overflow cancels active selected work cooperatively and drains
it; queued jobs are summarized without starting children or producers.

For a bounded wait, retain the original owning release future/task. If
`timeout(&mut future)` expires, cancel its token and await that same future.
Reconstructing a manifest/run is not continuation or proof of the original
owners' cleanup.

Actual destruction/abort of the owning future and Rust panic have a weaker
contract. JoinSet requests task abort; destruction can require later runtime
polls. Real native cache-owner destruction synchronously joins its existing
thread; direct-child Drop attempts kill/wait but cannot report OS errors.
HTTP/socket owners are abort-owned. Neither outer Drop returning nor observing
a panicked task establishes awaited HTTP/socket joins or successful process
reaping. Prefer cooperative cancellation and await the original owner. No new
global owner registry, strong panic guarantee or process-tree sandbox is added.

## Privacy and results

Only the runtime's explicitly named existing credential variable is read.
Cartesia settings are not forwarded to the child: its environment is cleared,
and only the manifest's explicit `env` values are supplied. A caller can choose
to supply the same value there independently; avoid secrets in scenario labels,
argv, runner-body data and bot protocol output. Settings/debug failures do not
print the speech credential. Existing child-env output masking remains in place.
See [README.md](README.md#environment-configuration-and-retained-output) for its
limits.

CLI exit status remains 0 only when every returned run passes, 1 for failed
attempts/cancellation and 2 for invalid input. The release JSON schema is
unchanged. Local deterministic acceptance uses synthetic credentials, local HTTP
and compiled `EvalBotHost` fixtures that validate complete multi-packet PCM;
it is not a live-provider, human-recognition-quality or production deployment
claim. Direct speech, direct duplex and ordinary suites retain their separate
selection and ownership contracts.
