# Recorded scenario bundles and direct CLI runs (Unix opt-in)

`RecordedScenarioFile::load(path)` preloads a bounded local YAML/JSON bundle and
its recordings. `into_sessions(url, options)` builds every session without
connecting. The existing public `ScriptTurn`, `ScriptScenario`, `ScenarioFile`
and `SessionOptions` fields are unchanged. Ordinary `ScenarioFile::load` and
CLI runs without the flag continue to reject audio scenarios.

Example bundle:

```yaml
name: recorded greeting
user:
  modality: audio
turns:
  - user: hello
    audio: hello.wav
    expect:
      - event: user_transcription
        text_contains: hello
        within_ms: 3000
      - event: llm_response
        text_contains: welcome
        within_ms: 5000
```

Place `hello.wav` beside the scenario file. It must contain ordinary PCM16
RIFF/WAVE at 16 kHz, mono, at most 60 seconds. Start an application endpoint that
accepts this audio, then run:

```sh
cargo run --locked -p pipecat-evals --bin pipecat-eval -- \
  run bundle/greeting.yaml ws://127.0.0.1:7860/ --recorded-audio --stop-bot
```

The optional flags can appear in either order. Unknown or repeated flags are
rejected before connecting. Exit codes remain 0 for passing assertions, 1 for a
failed evaluation, and 2 for invalid configuration/input. All scenarios and WAVs
are loaded and every session is constructed before the first network connection.
`--stop-bot` still cannot share one endpoint across multiple scenarios.

For the native host, select `.with_raw_audio_input(true)` when constructing
`EvalBotHost`, and supply an application STT/VAD/realtime pipeline. The host still
configures text output (`skip_tts: true`). The built-in `native-text-bot` example
does not acquire an STT provider from this flag. No credentials or models are
chosen automatically. The `user` string is a reference for judging; it is not
sent as `send-text` for a recording. Existing paced playback, tail padding,
continuous idle silence and ownership rules are described in
[RECORDED_AUDIO.md](RECORDED_AUDIO.md).

## Strict recording-only input schema

- Every expanded scenario must explicitly select `user.modality: audio`.
- Every spoken `user` turn must have an `audio` path and a nonempty reference.
  Missing recordings never silently become text or synthesized speech.
- `user.speech` and other user-provider configuration are unsupported in this
  mode. Observe-only and ordinary DTMF turns retain their existing meanings;
  a recording cannot share a turn with DTMF.
- Group headers and entries use the existing shallow override rules. Entries
  replace entire values; user-provider dictionaries are not deep-merged.
- Default `suite`/release manifest loading still rejects audio; explicit recorded
  suites are described in [RECORDED_SUITE.md](RECORDED_SUITE.md). Ordinary loaders
  still reject audio; explicit spoken-response modes are documented separately
  in [BOT_AUDIO_RESPONSE.md](BOT_AUDIO_RESPONSE.md). No route silently drops
  recording mappings and evaluates reference text instead.

## Paths, reads and memory limits

This is a **trusted local bundle** reader, not a filesystem security sandbox.
Media paths are based on the parent of the supplied outer scenario path,
including when that entry is a symlink, rather than the current working directory
or the directory of a fragment containing the turn. Canonical files must be
within that outer directory tree at validation time. Absolute paths inside the
bundle are allowed; escapes and symlinks resolving outside it are rejected.
Symlinks resolving inside it are allowed. Intermediate-directory replacement
races are not isolated by this static containment check.

Native `!include` resolution remains relative to each including file. The pinned
Python loader instead retains the outer base for nested includes; this preexisting
native difference is not changed. Its media paths also use the outer scenario
entry. Native bundle containment and nonempty reference requirements are stricter
than Python's broader file decoder/path support.

The new loader checks regular-file metadata before opening and again on the
opened descriptor, using Unix `O_NONBLOCK | O_NOFOLLOW`. A swapped-in FIFO cannot
block the opening step before descriptor validation rejects it. Directories,
FIFOs and devices are not accepted. Reads take at most the limit plus one byte,
then reject oversize data. This mode returns `Unsupported` on non-Unix platforms;
it does not fall back to potentially blocking special-file reads. The ordinary
text loader's behavior is unchanged.

Limits apply across the **entire expanded file**, counting repeated references
and inherited recordings in every scenario:

- Existing scenario/include parsing: 1 MiB per file, 4 MiB total, depth/cycle and
  YAML construction limits unchanged
- At most 4 MiB of cumulatively expanded scenario JSON, counting inherited
  header/context copies even in observe-only scenarios
- At most 128 recording references; each path at most 4096 UTF-8 bytes
- WAV container at most 1,985,536 bytes per clip; 16 MiB cumulative WAV reads
- At most 15,360,000 retained PCM bytes across all scenarios, including duplicates

All schema checks and all recording metadata/path checks precede recording
payload decoding. Actual read totals are checked again, so files growing after
metadata checks do not bypass budgets. Each decoded clip is copied into owned
memory before connecting; changing a source file later does not change playback.
These bounds do not provide deadlines for a faulty network filesystem. Use local,
trusted storage rather than remote mounts or files concurrently modified by an
adversary.

## Credential-free end-to-end reproduction

```sh
cargo test --locked -p pipecat-evals --test recorded_scenarios \
  actual_cli_recording_reaches_native_host_and_assertions_pass -- --exact
```

This fixture creates a WAV and scenario, launches the actual `pipecat-eval` CLI
as an owned child, and connects it to the actual opt-in native host. A deterministic
PCM-byte recognizer emits native transcription/text frames for assertions. It
proves the CLI-to-audio pipeline route without provider credentials; it is not a
human-speech recognition test. Additional tests cover assertion failure exit 1,
default text runs, preconnection rejection, include/media paths, regular-file
rules, aggregate budgets, and actual child/host cleanup.

Preserved test-harness replay can set `PIPECAT_EVAL_REPLAY_CLI` to the matching
preserved CLI executable. This is a test-helper override only. Replay manifests
pin both binaries; an old harness alone does not qualify a later CLI build.
