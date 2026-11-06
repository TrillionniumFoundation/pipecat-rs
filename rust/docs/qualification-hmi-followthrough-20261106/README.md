# HMI follow-through evidence — 2026-11-06

Start with [results.json](results.json) and the [decision report](../HMI_FOLLOWTHROUGH_20261106.md). These are selected engineering gates, **not** full-workspace release, physical-media acceptance or production Hepta composition.

## Final source and executable

- Tested local code: `772e3ca152c53a33f70b2f47bab8610efd6e5ce8`.
- Published code: `6367d62babf97cab96a756b58427c8ee5fac5f15`.
- Equal complete tree: `652aceec8a2dbf70acc6d58dcfc39af12e710f82`.
- Final native executable SHA-256: `212bf44ae88bee0c0640fa109ea308404a284f3b9157f8388737a1cc6858d973`.
- [Publication record](publication-code.json): API blob/tree checks, expected-head lease and independent public git fetch with an empty all-path diff.

The final gate's source snapshots are identical and clean. The subsequent evidence commit changes documentation/evidence only. [`SHA256SUMS.json`](SHA256SUMS.json) covers every file in this evidence directory except itself. Raw paths in logs identify the recorded engineering executor; they do not identify a production deployment.

Raw Cargo logs retain their original trailing blank lines and exact digests. The delivery whitespace check applies to authored files; it does not rewrite raw logs to satisfy an end-of-file whitespace rule.

## Final acceptance records

| Record | Meaning |
| --- | --- |
| [final-gates.json](final-gates.json) | Exact argv, build environment, toolchain, source/file hashes, exit codes, log hashes, before/after source snapshots |
| [final-core-pipeline.log](final-core-pipeline.log) | 480 passed, 0 failed, 0 ignored, including 13 new payload cases and 2 doc tests |
| [final-hepta-native.log](final-hepta-native.log) | 65 passed, 0 failed, 3 ignored; includes 24 supervisor and 3 top-level parent-death tests |
| [final-webrtc.log](final-webrtc.log) | 46 passed, 0 failed, 0 ignored, including the diagnostic-only lifecycle-test edit |
| [final-clippy.log](final-clippy.log) | Core/pipeline/Hepta/WebRTC, native feature, all targets, `-D warnings`, exit 0 |
| [final-native-build.log](final-native-build.log) | Actual native example build, exit 0 |
| [final-actual-entry.json](final-actual-entry.json), [log](final-actual-entry.log) | Eight executable fault cases, final binary SHA unchanged, no model/device stage |
| [final parent-death entry](parent-guard/final-native-entry-fifo.json) | Same final executable and frozen source; independently observed original native exit after wrapper SIGKILL; no emergency cleanup or manufactured terminal |
| [format](final-format.log), [diff check](final-diff-check.log) | Exit 0; empty successful logs are intentional |

Two Hepta ignored cases need real model/device inputs. The third is a subprocess-only test helper explicitly invoked by the three top-level parent-death tests. Do not add helper output, feature variants, subset reruns or repeated stress invocations to unique coverage.

[`run_final_gates.py`](run_final_gates.py) and the [final FIFO probe](parent-guard/native_entry_fifo_smoke_final.py) preserve the actual orchestration with recorded paths and exact source expectations. They are evidence snapshots, not production launchers. The reusable no-device entry gate is shipped at `rust/scripts/qualify_hmi_supervisor_entry.py`; the physical script at `rust/scripts/qualify_hmi_media_process.py` was not run against physical media this turn.

## Earlier stages and retained failures

Files without the `final-` prefix, and `parent-guard/evidence*.json`, are stage records. A stage's source/binary hashes, pending validation or worker-local publication flags describe that stage, not the later final tree. They remain unedited; publication is recorded separately above.

| Record | Why retained |
| --- | --- |
| [webrtc-baseline-both-regressions.log](webrtc-baseline-both-regressions.log) | Exact original vendor with the two added regressions: 0 passed, 2 lifecycle assertion failures |
| [webrtc-fixed-all-targets.log](webrtc-fixed-all-targets.log) | Earlier fixed package: 46 passes before the failure-diagnostic edit; final gate separately covers that edit |
| [webrtc-fixed-repeat.log](webrtc-fixed-repeat.log) | 60 deterministic executions passed; unchanged stress then passed 24 invocations and failed invocation 25 at data-channel setup, before shutdown |
| [webrtc-fixed-repeat-retry.log](webrtc-fixed-repeat-retry.log) | Subsequent 30 passing observations; they do not erase or explain the setup timeout |
| [webrtc-baseline-repeated.log](webrtc-baseline-repeated.log) | Fifty original-baseline stress passes; the exact historical random UDP-leak schedule was not reproduced this turn |
| [webrtc-shutdown-evidence.json](webrtc-shutdown-evidence.json) | Stage hashes/counts, final diagnostic source hash, unresolved attribution and required future phase evidence |
| [actual-supervisor-entry.log](actual-supervisor-entry.log) | First real executable probe failed with a full inherited stderr; preserves timeout and cleanup timeout |
| [actual-supervisor-entry-r2.json](actual-supervisor-entry-r2.json), [native build before freeze](native-build-before-freeze.log) | Intermediate fixed executable `f24e908…`: 8 passing cases, not silently relabelled as the final executable |
| [frozen-gates-r1/final-gates.json](frozen-gates-r1/final-gates.json), [failed lint](frozen-gates-r1/final-clippy.log) | Prior frozen tree `66e6a2f…` passed 480/65/46 but strict Clippy failed `collapsible_if`; equivalent condition merge, new source freeze and full final gate followed |
| [parent-guard/evidence.json](parent-guard/evidence.json) | Parent binding, initial missing locked cache, direct-child waitpid and listener release, intermediate binary probes |
| [initial FIFO inventory failure](parent-guard/native-entry-fifo-smoke-r1.json), [probe](parent-guard/native-entry-probe.json) | This kernel lacks task/TID/children; bounded PPid inventory fallback retained the exact process-identity checks |
| [FIFO R2](parent-guard/native-entry-fifo-smoke-r2.json), [FIFO R3](parent-guard/native-entry-fifo-smoke-r3.json) | Earlier binary identities remain separate from the final probe; no substitution across artifact hashes |
| [pidfd cleanup](parent-guard/harness-pidfd-cleanup.json) | Wrong incarnation refused; exact task-owned child signalled and reaped through pidfd; no numeric-PID fallback |
| [independent integration review](pipeline-supervision-review.md) | Turn-deadline and stale-timer findings, source re-review after fixes, minimal DTLS cancellation review |
| [pipeline-core-full.log](pipeline-core-full.log), [payload subset](pipeline-final-payload-tests.log), [supervisor subset](supervisor-tests-final.log) | Earlier full/subset results; not additive to final gate counts |

Other baseline/initial logs preserve ENOSPC, missing prerequisite/cache, fixture construction/compile corrections and coordinated interruption. These attempts are not passing tests or qualifying negative controls. `pipeline-clippy.log` was interrupted for shared-target coordination (exit 130); strict Clippy acceptance comes only from the completed final log. The initial import-order formatting failure is in `format-before-freeze.log`. The executable metadata-stage and private-method fixture compilation failures are retained in their explicitly named development logs. A controlled worker panic in an otherwise passing shutdown test is expected fault injection; the separate real data-channel setup failure above remains open.

## Real-environment boundary

[`environment-followthrough.json`](environment-followthrough.json) preserves three succeeded configured-host job identities and selected current inventory output, plus two unreconciled UNKNOWN jobs. The original capture digest is recorded. Unrelated historical deployment narrative and credential-path listings are excluded; that historical narrative is not reused as current Rust acceptance. No credentials were read, device grants changed, physical capture/playback performed or production services modified.

Pocket4's current codec is ALC245; ROG's is ALC294. The historical ALC897 HMI host and original callable executor were not recovered. The visible Pocket4 Python/SenseVoice/Melo deployment and its container with no devices do not establish current Rust HMI or the canonical Hepta owner. Current production media composition, adopted model manifests, signed observation producer/replay witness, current grants/generations and user-heard Chinese E2E remain open.

The final native FIFO probe reports original-incarnation **exit observation**, not ownership of native `waitpid`. The three separate parent-death tests supply direct-child waitpid evidence. Single millisecond observations do not establish a latency guarantee. Queue payload charges do not establish RSS, and kill does not undo submitted audio or remote effects.
