# Q strict Clippy after the MoQ diagnostic window

This is a prepared execution plan. The helper has not been run while authoring
or statically checking this package. It does not update a checkout, retry a
phase, run a test matrix, or invoke model inference. Gate owns native execution.

## Actual source identity

The helper is restricted to the published Q checkpoint:

- Head: `c1c3addb07dde0a143462c10961ff2494eeba90a`.
- Tree: `4df0d197540551ed9b74d4e11c0936e5bedee5f5`.
- Complete 2,422-file canonical manifest:
  `23a585582dba548ece61948fcb87ec32a0689ff46bef20ace1739edd3986e20d`.
- Root's 11,511-byte source contract:
  `c255d772305af493508754d78797cad6a8abb827aa40669b2b392b5c0b3256a3`.

The contract contains all 102 paths changed since original `0cc16e8...`:
100 documentation paths and exactly two fixture source files. The full
canonical map includes documentation. No compilation-only digest or two-file
replacement is substituted for that map.

The two exact reviewed source hashes are:

| File | SHA256 |
| --- | --- |
| `rust/crates/pipecat-moq/src/tests.rs` | `8695fd709059794132f88133a5d17bb6f3e82e8cd8b84dcdf9bce62176633219` |
| `rust/crates/pipecat-evals/src/simulation/quality.rs` | `4f5b66f3ba68317ab834ef8883ee0ed149477a2d032cae9ab243da513da02780` |

The first adds actual-result diagnostics while retaining the MoQ oracle,
150 ms configuration and original ordering. The second changes only the
reviewed test initializer that triggered the original strict lint.

## Admission and history

The launcher must first observe authoritative terminal managed-job statuses
for all original 0cc qualification stages, C3 and the one Q MoQ window. Holding
the same exclusive owner lock, this helper checks the actual MoQ receipt,
same Q head/tree, before/after full canonical identity, exact recorded source
contract, original MoQ helper identity, and completed process containment.
A nonzero MoQ result remains nonzero; it is not retried or treated as a
successful Q workspace gate.

The helper then rechecks the unchanged C3 receipt and its six original
preceding receipts: Grok, default, expanded, models, WebRTC and AISHELL.
C3's native result and comparison result remain separate; a comparison
failure is preserved and does not become a successful model qualification.

Original 0cc default and strict receipts are also copied into this window.
They are checked against their original head/tree and canonical
`1e07f773...`, not Q. The default supplies the recorded native environment
and history. The original strict receipt supplies the actual command and
environment overrides. Neither is misrepresented as a full default run on Q.

The old `desktop_additional_gate_pin_v4.py` requires the preceding default
to have the same head as its new gate, so it is not used for this window.
The new helper owns the lock itself and does not call the lock-taking v3
inner runner. This avoids a nested-lock failure and preserves the distinction
between old 0cc qualification and the limited new Q checks.

## Exactly three independent checks

The pinned Rust 1.99.0 tool prefix is
`/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66/tools/rust-1.99.0-r1/bin`.
The same exclusive `source-pin/target-pin` pair is used.

Each changed leaf file is checked once with pinned rustfmt:

```text
rustfmt --check --edition 2021 --config skip_children=true <absolute file>
```

The workspace and both relevant packages declare edition 2021. These are
two leaf-file checks, with independent argv, raw logs and exits; they do not
replace or relabel the failed historical `cargo fmt --all -- --check`.

The strict command runs at most once, with the original argument order:

```text
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
```

It is byte-for-byte equal as an argv array to the original 0cc strict
receipt. There is no added `--offline`, target exclusion, lint allowance,
feature reduction or whole-matrix rerun. An ordinary nonzero rustfmt result
is retained while the independently requested Clippy command still runs.
An admission, timeout, termination, containment or integrity failure stops
further phase admission without retry.

A successful Clippy result would qualify only this command on Q. It would
not make the four earlier complete 0cc commands successful, clear the
original MoQ failure, or establish Q runtime qualification.

## Time, exits and cleanup

The outer managed job must enforce **900 seconds, CPU 800%, 32 GiB, nice 10**.
The helper's complete internal budget is 870 seconds, including admission,
source and tool checks, two format checks, Clippy, cleanup, postchecks and
final receipt writes. Fifteen seconds are reserved during phase admission
for cleanup and postflight; the external 30-second margin remains necessary
for final containment/reporting. Resource values in the helper are requested
limits; the launcher must preserve the actual managed-job limit receipt.

Every process starts in a new owned process group. Launch attempts are
recorded before Popen; PID/PGID is recorded immediately after a successful
Popen. The helper preserves original process exits, runner phase status,
timeout, log integrity, containment and final aggregate status separately.
The first failing phase remains available if a later postcheck also fails.

TERM has at most two seconds of leader wait, followed by KILL with at most
one second. The actual leader reap and final non-atomic group probe are
recorded; a signal request or final group absence is not called an
individual join of all descendants. A surviving group after ordinary leader
exit is a containment failure even if later signalling removes it.

All raw command bytes are retained. Format/Clippy stderr is merged into
their corresponding `.stdout.log`, as in the original combined command
log. Git identity commands use separate stdout and stderr files. A phase
never admitted has a recorded missing-before-admission log state, not an
invented process log or a false source-integrity success.

The final before/after source maps and head/tree/clean checks are separate
from individual command execution intervals. Script, contract, original
receipts, environment, target marker and actual tool file identities are
also compared after the checks. Failure to finish a postcheck is explicit
and nonzero. A time interval that includes postflight must not be labelled
pure Cargo child time or inference time.

The native overrides are exactly equal to the original strict receipt.
The effective variables are also compared with the original default's
recorded build-environment whitelist. This is not a claim to reconstruct
unrecorded historical environment variables.

## Launch and deliverables

At ROOT `/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66`, stage the
reviewed helper as:

```text
ROOT/desktop_strict_clippy_after_moq_pin_r1.py
```

It consumes the already-staged actual contract at:

```text
ROOT/requests/moq-diagnostic-pin-r1.source-contract.json
```

Invoke it with the exact three positional values:

```text
python3 ROOT/desktop_strict_clippy_after_moq_pin_r1.py c1c3addb07dde0a143462c10961ff2494eeba90a 4df0d197540551ed9b74d4e11c0936e5bedee5f5 c255d772305af493508754d78797cad6a8abb827aa40669b2b392b5c0b3256a3
```

Do not pre-create `ROOT/evidence/strict-clippy-after-moq-pin-r1`; the helper
creates that directory exclusively. Its main receipt will be
`verification.json`. Keep the exact script bytes, argv, outer resource
limits, managed-job identity and final authoritative status in the launcher
receipt. Preparation of these files is not authorization to duplicate a
pending launch.

This package preserves the author audit, exact source contract, old original
receipts and the helper itself. Independent review is recorded separately;
no native result is prefilled here.
