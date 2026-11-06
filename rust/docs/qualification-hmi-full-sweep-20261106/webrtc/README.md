# WebRTC evidence availability after the second entrance change

**Only 4 of the original 60 archive files are currently recovered.** All four match their previously recorded SHA256 values. The other 56 original files remain unavailable from the accessible stores; none is replaced by a reconstruction of a log or by a new run.

The recovered files are under `recovered-originals/`: the original README, original index, historical invocation-25 failure log, and original qualification metadata. Their old statements describe the collection as it existed when assembled. This new receipt describes what can actually be read now.

## Missing evidence

The complete per-path inventory is in [availability.json](availability.json), including every known original hash and byte length. Missing files include both SIGKILL build JSON/log pairs, the strict Clippy JSON/log, all 40 inherited-binary repetition logs and their manifest, the 238-path source comparison, the original frozen manifest/patch, both runners, copy provenance, and the original checksum lists.

The original index preserves the observed outcomes and hash values: two exit-101 build failures before any runtime tests, a later static Clippy pass, and 40/40 diagnostic executions of one inherited-binary test. These are retained historical observations with missing supporting raw records. They are not relabelled as fresh runtime qualification or as newly recovered complete gate evidence.

## Historical failures

The exact invocation-25 setup failure log and metadata remain published at commit `e526aabaf98522b2294e744ac48d75d9ed89a254` and were recovered byte-for-byte. That failure remains open; later successes do not erase it.

The workspace `schedule=5, iteration=24` UDP-retention failure remains recorded in the published qualification document. Its complete source document is available, but it is a document record, not the original workspace execution log. The old generated archive excerpt itself was not recovered.

## Integrity and boundaries

`recovery-attempts.json` records the original SHA comparisons. `RECOVERY-SHA256SUMS` covers this new availability package only and must not be presented as the original missing `SHA256SUMS`. The original README/index contain historical links to missing files; their successful recovery does not imply those targets are available.

No historical raw log was generated from summary text, no executable was rerun to impersonate an earlier run, and no runtime test was performed by this recovery task. Independent current gates must retain their own commands, source bindings, and raw output.
