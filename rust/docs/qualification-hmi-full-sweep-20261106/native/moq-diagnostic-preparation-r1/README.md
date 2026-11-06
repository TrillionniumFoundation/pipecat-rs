# MoQ original-suite diagnostic window: prepared, not executed

The new helper prepares a finite repeat window for the original 81-case MoQ
library harness. Its purpose is to capture the actual error if the absent-peer
deadline assertion fails again. The original r5 failure remains a failure with
an unknown returned error. A later passing run, including 20 passing repeats,
does not erase it or create 20 sets of distinct test coverage.

The only permitted change inside `rust/crates/pipecat-moq/` is the reviewed
diagnostic-only `src/tests.rs` candidate, SHA256
`8695fd709059794132f88133a5d17bb6f3e82e8cd8b84dcdf9bce62176633219`.
It retains the original 150 ms configuration, exact expected error, all
await/spawn ordering, and original cleanup. Other source changes, such as the
separately reviewed quality fixture initializer, must appear in the exact
root-approved full diff list and source identity contract.

No Cargo, native harness, model, or remote command was executed while preparing
or statically reviewing this helper. The source checkout remains under root
and gate ownership. This helper does not fetch, checkout, patch, format, or
otherwise update it.

## Frozen inputs

- `reviewed-runner.py`: the helper copied byte for byte from
  `task-helpers/desktop_moq_diagnostic_pin_r1.py`; 33,499 bytes, SHA256
  `bfbba41f328657e6db1bc3498a0636d2209e84ae163ff51c0912bc277d94e8af`.
- `original-case-names.json`: 81 unique sorted names, 6,882 bytes, SHA256
  `e87d053861a5d96597d28371305e85676864b27477334abcf218a843539f2a41`.
- `original-moq-target.log`: the original complete Running-to-failure section,
  8,165 bytes, SHA256
  `8a1fc305fac84714d2124c04c1af1aaeba6261e09c7214822d20e58a3e843c65`.
- `original-selection-binding.json`: exact offsets and SHA of the original
  16,384-byte runtime tail. The section has 80 passes and the one original
  absent-peer failure, not a newly executed selection check.
- `draft-before-review.py` and `review-corrections.diff`: the preserved initial
  helper and the subsequent evidence-accounting corrections.

## Launch sequence

Gate must first observe authoritative terminal managed-job statuses for every
original `0cc16e8123cb05aaad480d5ab7860a267d998bac` qualification stage and C3.
The helper also checks the completed C3 receipt and its six preceding stage
receipts, their unchanged bytes and source, and any C3 process containment.
Functional failures remain recorded failures; unresolved integrity or cleanup
failures block this window.

Root then publishes the new reviewed source. Gate updates the existing
exclusive `source-pin` checkout and retains its exclusively owned `target-pin`.
The new helper accepts only the published head/tree and the exact contract SHA.
The same `qualification-owner-pin.lock` remains held across all preparation,
compilation, tests, cleanup, and final source/artifact checks.

Under `/home/qian-qi/workspace/pipecat-workspace-f9f576cc6c66`, stage:

1. The helper as `desktop_moq_diagnostic_pin_r1.py`.
2. The original name list as
   `requests/moq-diagnostic-pin-r1.case-names.json`.
3. Root's newly generated contract as
   `requests/moq-diagnostic-pin-r1.source-contract.json`.

The contract is not a placeholder execution input. It must contain real values
after publication, with schema `pipecat-moq-diagnostic-source-contract-v1` and
these fields:

- `base`: original full `0cc16...` commit.
- `base_tree`: `aec037b9cf2be53fcf6f85fdc149382942277702`.
- `head` and `tree`: exact newly published source identities.
- `source_manifest_sha256`: the complete new source manifest, using the same
  sorted file entries and canonical JSON definition as the preserved
  `run_full_gate.py` helper. The contract stays outside the source tree.
- `changed_paths`: the complete list from
  `git diff --no-renames --name-only` between the original and new freeze.
- `source_sha256`: the reviewed changed source-file hashes, including the exact
  MoQ diagnostic candidate and the independently approved quality initializer.

The canonical source manifest covers every Git tracked or unignored other file,
including documentation; it is not a compilation-input-only manifest. Root must
derive the new complete map from the original `0cc16...` map and apply the actual
Git contents of every published `changed_paths` entry, including the intervening
documentation checkpoint and both reviewed source edits. Replacing only the two
Rust entries would produce an incorrect contract. The original `0cc16...` phases
and C3 continue to require their original `1e07f773...` manifest; this future
contract does not retroactively change those receipts.

Invoke the helper with positional arguments `HEAD TREE CONTRACT_SHA256`.
Use an external 600-second job, CPU 800%, 32 GiB and nice 10. Save the exact
helper/contract/name-list bytes and launch argv in the launch receipt. Do not
pre-create its create-only `evidence/moq-diagnostic-pin-r1` directory. This is
an execution plan, not a request to launch before the required stages finish.

## Compilation and actual harness binding

The entire internal 570-second budget includes source/tool checks, metadata,
compilation, test selection, up to 20 rounds, process cleanup and postchecks.
The remaining 30 seconds of the external job are a final reporting/termination
margin. Time is measured inside the job, not from control RPC latency.

The helper uses the pinned Rust 1.99.0 tools and original native environment.
It rejects an inherited `RUST_TEST_THREADS`. Compilation is exactly the
workspace/default feature selection, with `--locked --offline --no-run` and
Cargo JSON output. It does not run a workspace test matrix or strict Clippy
inside this window. Those are separate gate operations.

Metadata must identify the one owned MoQ package. Cargo must report one
successful libtest artifact for that package, library target, test profile and
exact `src/lib.rs` path. Its executable must be inside the exclusive target.
No binary is found by guessed filename, hashed-file glob or stale-artifact
fallback. The actual executable's `--list` must match all 81 original names.

Each round then runs the bare executable with no selector, no ignored flag,
and no test-thread override. It retains libtest's default parallelism and
captures the normal full failure report, including the added actual result.
A successful round requires all 81 original names exactly once, every result
`ok`, and the exact 81-pass summary. Any preparation, compilation, selection,
process, timeout, containment or summary failure stops the window. There are
no retries or replacement rounds.

## Exit and cleanup evidence

Every invoked command has its own process group. The launcher records a launch
attempt before Popen and records PID/PGID immediately after Popen succeeds.
Raw phase/round logs are retained with bytes and SHA. A phase that was never
admitted may truthfully have no raw process log; that is distinguished from a
missing log after a process launch.

Original process exits, timeout status, containment status, log integrity and
final wrapper status are separate. In particular, a test can originally exit
101 while a later cleanup failure sets the wrapper to 125; this does not turn
the Rust test's original exit into 125. A phase denied a time budget records
124 without pretending a test ran. The stopped phase preserves those fields.

Cleanup requests TERM with at most two seconds of leader wait, then KILL with
at most one second. Actual leader reap and final non-atomic group presence are
recorded. Group absence does not assert that every descendant was individually
joined. Any unresolved cleanup makes the overall result nonzero.

Complete source manifests and Git identities, the reviewed source files,
tool/contract/receipt inputs and compiler-reported harness are checked again.
If a time limit prevents a postcheck, the missing verification is an explicit
integrity failure; it is never converted to an unchanged-source claim. The
original execution status remains available alongside that wrapper result.

The final record is `evidence/moq-diagnostic-pin-r1/repetitions.json`, with
attempted, started, completed and successful repetitions distinguished.
Preparation of this plan supplies no new runtime qualification.
