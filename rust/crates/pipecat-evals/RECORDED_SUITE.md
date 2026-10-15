# Recorded input in native hosted suites

On Unix, use `pipecat-eval suite MANIFEST.yaml --recorded-audio` or
`ReleaseManifest::load_recorded(path)` followed by the existing `run_release`.
The ordinary `suite MANIFEST.yaml` and `ReleaseManifest::load` remain text-only.
This mode uses the same `native_version: 1` manifest and compiled bot commands,
per-run child ownership, output retention, cancellation and result format.

Every referenced scenario must satisfy [the recorded scenario contract](RECORDED_SCENARIOS.md):
explicit `user.modality: audio`, a local 16 kHz mono PCM16 WAV for every spoken
turn, and nonempty reference text. Reference text is never sent as a substitute
for a missing recording. Observe-only and DTMF turns can remain without clips.
The bot must accept RTVI raw audio, for example an `EvalBotHost` built with
`.with_raw_audio_input(true)`, plus application STT/VAD or a realtime service.
The native host's assistant output remains text. This does not add a speech
recognizer or certify any external provider.

```yaml
native_version: 1
concurrency: 2
repeat: 2
base_port: 7900
suite:
  - name: voice-bot
    bot: /absolute/path/to/compiled-audio-bot
    args: ["--port", "{port}"]
    concurrency: 1
    scenarios: [conversation.yaml]
```

Bot argv is application-specific. The command above is an example contract, not
an assertion that every bot accepts `--port`. Existing empty inherited child
environment, literal argv placeholders, loopback validation and direct-child-only
cleanup boundaries remain applicable. See [README](README.md).

## Preflight, limits and lifetime

All files and every expanded job are validated before any child is launched.
Manifest and runner-body reads in this mode also use bounded Unix regular-file
reads; FIFOs/devices are rejected. Existing manifest path semantics remain:
explicit `.yaml`, `.yml` or `.json` references are manifest-relative, while bare
names resolve through `scenarios_dir`. Each scenario's recordings use that
supplied scenario path's parent, including its symlink spelling, as in the
recorded loader. Its static containment checks are for trusted local bundles,
not a race-proof filesystem sandbox. No shell, remote download or audio cache
is introduced.

In addition to existing 128-run, concurrency and file construction limits:

- Actual recording WAV reads across the manifest are bounded to 16 MiB. The
  loader receives the remaining allowance before inspecting/reading the next
  file and checks actual read sizes again.
- Across expanded jobs, counting every repeat, at most 128 clip references and
  15,360,000 logical PCM bytes are admitted. PCM length is checked against the
  remaining allowance before copying into an owned recording.
- Expanded recorded `ScriptScenario` representations across jobs are bounded to
  4 MiB, counting repeat copies. This is distinct from WAV/PCM storage.
- Repeated attempts share immutable `Arc` recording bytes, but each attempt still
  sends its own recording. Logical bytes are deliberately charged for every
  repeat, not deduplicated by filename or pointer identity. This also bounds
  retained clip bytes; it is not a bound on allocator overhead.

`ReleaseManifest.jobs` and each job's scenario are public. Immediately before
launch, `run_release` revalidates all clip indices, complete coverage of spoken
turns, DTMF conflicts and aggregate expanded-job limits. A caller changing a plan
cannot silently turn an uncovered audio turn into a text send. Recordings stay
attached to their job when jobs are reordered or cloned.

The existing attempt-major output order, label concurrency caps and 64 MiB
result retention remain. Invalid input fails before any spawn; failed assertions
produce failed runs and CLI exit 1. Cancellation stops playback and follows the
same session, child-reap and pipe-reader cleanup path. Existing external cancellation can immediately force-kill a still-running child
(the report marks `forced`), so it does not guarantee application cleanup inside
that terminated process. Client playback and pipe owners are still joined and the
child is reaped. A sticky pre-cancellation creates no child. The suite does not add a second task owner or scheduler.

## Source and explicit native boundaries

Fixed Python `af9f9da9d7afddaa8811c0e0251b32566c980bdf` chooses audio input from
scenario modality and `turn.audio`. Its suite `record` / runner `--audio` concern
conversation output capture. Those options remain unsupported here and are not
repurposed for input recordings.

Native behavior intentionally remains stricter: all-input preflight rejects
invalid files rather than making failed spawned runs; all repeated runs must
pass for CLI exit 0. The existing flat native scheduling policy is preserved,
including its difference from Python's coalesced repeated-label queues. Python
suite scenario resolution canonicalizes symlinks before loading, whereas this
mode preserves the recorded loader's supplied-reference media base. Python
spawn templates, synthesized user speech, output recording, judgement-provider
credentials and directory traversal on hostile mutable filesystems are outside
this subset.

## Deterministic local verification

`cargo test --locked -p pipecat-evals --test recorded_suite` uses a compiled
harness child running the actual native host and a deterministic PCM fixture.
The harness contains six parent verification cases and one helper entry which
returns without child mode; the helper is not independent business coverage.
The CLI launches a fresh child for each repeat/scenario, asserts exact PCM and
absence of text fallback, checks pass/fail exit codes, and verifies reaping and
cleanup. This validates transport and ownership, not natural-language STT.
Preserved replays must pin both the harness and its matching CLI binary, using
`PIPECAT_EVAL_REPLAY_CLI` for the latter; it is only a test-helper override.
