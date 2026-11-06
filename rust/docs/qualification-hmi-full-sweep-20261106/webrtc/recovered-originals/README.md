# WebRTC local preparatory evidence — 2026-11-06

This directory preserves the local diagnostic executions and source-bound build
attempts that preceded the separate complete native/workspace gate. It does not
contain that later gate's result, LiveKit SFU results, production media acceptance,
or a fix for the retained data-channel setup timeout.

## Result index

| Evidence | Recorded result | Runtime and source boundary |
| --- | --- | --- |
| [Two-job test build](runs/test-source-bound.json), [raw log](runs/test-source-bound.log) | Cargo exit **101**, 195.439 seconds; rustc compiling `time 0.3.55` received signal 9 / SIGKILL | **0 tests executed**; earlier diagnostic candidate |
| [One-job test build](runs/test-j1-source-bound.json), [raw log](runs/test-j1-source-bound.log) | Cargo exit **101**, 160.392 seconds; rustc compiling `pipecat-core` received signal 9 / SIGKILL | **0 tests executed**; the same earlier candidate |
| [Strict all-target Clippy](runs/clippy-j1-source-bound.json), [raw log](runs/clippy-j1-source-bound.log) | Cargo exit **0**, 100.553 seconds, `-- -D warnings` | Static checking of the later diagnostic checkpoint; **0 runtime tests** |
| [Inherited-binary repetitions](inherited-binary/baseline-repeat.json) | **40/40 invocations passed** | One selected historical stress case, not 40 distinct tests and **not runtime qualification of the changed source** |

The two compilation failures remain failures. Their parsed test counts are zero
because neither invocation reached a test runner; zero failed test cases is not
a successful build. The signal's cause was not established. In particular,
SIGKILL does not by itself establish OOM or explain the earlier setup timeout.

[Machine-readable index](index.json), [source comparison](source-comparison.json)
and [copy provenance](copy-manifest.json) retain the classifications separately.
Each original JSON and log is copied byte-for-byte, including unsuccessful
invocations. No raw result is amended to point at a later source revision.

## Three distinct provenance scopes

### Earlier candidate: two interrupted builds

Both builds captured exactly 238 selected files before and after execution.
Each invocation's before/after maps are identical, and the two builds used the
same selected-file map. Their `connection.rs` SHA256 was:

`0fe80723a54708c5a3b8eb021aeeedb2c1ef494caafa13b5768e1df221b2a454`

That is an earlier diagnostic candidate, before the final controlled test's
publication-timestamp observation refinement. Compared with the later Clippy
snapshot, `connection.rs` and two documentation files differ. Stable source
*during* an invocation does not mean it is the final frozen source. These failed
builds must not be described as runtime attempts on the final native gate's tree.

### Later candidate: strict Clippy

Clippy captured 238 unchanged files before and after its invocation. Its
`connection.rs` SHA256 was:

`5050a881213a22bf5ebce5b65bcac01194f2ac1c6a336bdea4cfe6f7d936d021`

At evidence assembly, the selected 238 paths match this Clippy snapshot except
for `SHUTDOWN_REGRESSION.md` and vendor `PATCHES.md`, which were subsequently
rewritten as a new documentation checkpoint. The precise recorded and current
hashes are listed in [source-comparison.json](source-comparison.json). This is a
selected-file comparison, not a whole-workspace or cross-environment result.

[The original frozen manifest](diagnostics/source-freeze.json) and
[the original diagnostic patch](diagnostics/webrtc-diagnostics.patch) preserve
that earlier handoff, including its original documentation. The patch is archived
provenance, not an instruction to overwrite the later documentation. Its base is
`e526aabaf98522b2294e744ac48d75d9ed89a254`.

All three local commands used the checkout's exclusive target
`/dev/shm/pipecat-webrtc-f9f576cc6c66`, Rust 1.99.0, locked/offline dependencies and
zero debug/incremental settings recorded in their JSON files. The
[original runner](runs/run_gate.py) records the exact commands and snapshot scope.
Its historical absolute paths are preserved rather than silently retargeted.

### Inherited executable: diagnostic repetitions only

The [original repetition runner](inherited-binary/baseline_repeat.py) invoked:

```text
/tmp/pipecat-hmi-native-27653505a519/debug/deps/review_lifecycle-a62df5bca02abea8
  repeated_peer_shutdown_schedules_release_both_udp_ports --exact --nocapture
```

The recorded executable SHA256 was:

`69d8b93d5b1e57db638946609bf883e8ea054417e4a024a8bd8d6fc9133f82d5`

This was an inherited artifact, not freshly rebuilt for the diagnostic changes.
The old target was used only to invoke that executable by this subtask, not as a
build target. The executable was no longer present when this evidence was
assembled; the original recorded hash is retained and is not claimed as a new
binary verification. The executable itself is not included here.

All forty original logs, `baseline-repeat-001.log` through `-040.log`, are retained
next to their manifest. Each records one passing selected test with five filtered
cases. Their bytes match every hash in the original manifest. Repeated executions
of the same case are not additional distinct requirements. The logs do not prove
a count of fully established data channels or qualify the modified-source test.

## Historical failures remain OPEN

### Invocation 25: data-channel setup timeout

[The complete prior repetition log](historical/webrtc-fixed-repeat.log) retains
the original failure, and [the original qualification record](historical/webrtc-shutdown-evidence.json)
retains its context. The historical stress series passed 24 invocations, then
failed at invocation 25 with `Timeout("data channel")` in the offerer's setup wait,
before that pair's shutdown. The existing wait bound was five seconds. The log's
23.83-second duration covers the entire test invocation, not that individual wait.

The subsequent 30 passes in the prior series and the 40 inherited-binary passes
here do not erase that failure. The new diagnostics distinguish wrapper readiness
from native ICE/DTLS/SCTP/channel state, but no protocol-race or resource-pressure
cause has been established. **No setup-timeout closure or protocol fix is claimed.**

### Workspace schedule 5, iteration 24: UDP retention

[The historical qualification document](../../../../docs/DRAFT_QUALIFICATION.md)
records canonical commit `167994707ab2b84a7ea3e9de423488752b5dbbbe`, tested
projection `2cbbb058e1092f5113b27ff522795211c421c6d6`, tree
`3e91f1f6c62671fe8e6504859de34f0bbc9ef339`: **3,477 passed, 1 failed, 55 ignored;
exit 101**. The recorded diagnostic is `UDP leak schedule=5, iteration=24` in the
same stress case.

[A clearly labelled document excerpt](historical/workspace-udp-record-excerpt.txt)
preserves that record with the complete document's hash and original line range.
It is **not a raw workspace test log**. No such raw historical workspace log was
found or manufactured for this collection. This exact stochastic failure was not
reproduced by the local repetitions, and local package checks or later passing
invocations do not close the historical full-workspace failure.

## Integrity and use

`SHA256SUMS.json` hashes every evidence file except the two checksum files.
`SHA256SUMS` additionally hashes `SHA256SUMS.json`; it excludes only itself.
Verify the complete collection from this directory with:

```sh
sha256sum -c SHA256SUMS
```

The collection has zero executed tests on the newly modified source. That scope
statement is local to these archived commands; results from a separate fresh
native/workspace/SFU gate must be recorded and evaluated separately. No code,
existing environment evidence or main qualification report is changed here.
