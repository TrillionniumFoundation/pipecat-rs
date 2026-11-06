# Current WebRTC evidence availability — read this first

**This recovery contains 4 of the original 60 archive files. The other 56 original files are not recovered.** The full per-path inventory and all retained original hashes are in [availability.json](availability.json). [README.md](README.md) explains the recovery procedure and its limits.

This file takes precedence when interpreting availability. The original README and index in `recovered-originals/` are preserved byte-for-byte and match the hashes recorded before the entrance change. They describe the complete archive as it existed then; they must not be read as a claim that its missing raw files are delivered now. Their original relative links are preserved, including links whose current targets are absent.

The recovered originals are the prior README, prior index, [invocation-25 failure log](recovered-originals/historical/webrtc-fixed-repeat.log), and [original qualification metadata](recovered-originals/historical/webrtc-shutdown-evidence.json). The separately included [historical workspace qualification document](historical-source-documents/DRAFT_QUALIFICATION.md) is available and hash verified, but is not one of those four archive files and is not a raw workspace execution log.

Missing files include the two SIGKILL JSON/log pairs, later Clippy JSON/log, all forty inherited-binary logs and their manifest, complete source comparison/freeze records, runners, patch, copy provenance and the original checksum lists. The original index retains historical result observations; it does not substitute for those missing raw files. Neither historical WebRTC failure is closed by this recovery or by later passing runs.

The receipt's path checks describe the read surface observed before this source checkout was restored. This current checkout now holds the recovery package; that does not recover the 56 missing originals. No log was generated from a summary and no new execution was assigned to an old record.

`RECOVERY-SHA256SUMS` covers the supplied nine-file availability package and is distinct from the unavailable original `SHA256SUMS`. This newly added entry point is tracked separately by the repository. The preserved original README's links without available current targets are listed below for clarity; its bytes have not been edited.

- `../../../../docs/DRAFT_QUALIFICATION.md`
- `copy-manifest.json`
- `diagnostics/source-freeze.json`
- `diagnostics/webrtc-diagnostics.patch`
- `historical/workspace-udp-record-excerpt.txt`
- `inherited-binary/baseline-repeat.json`
- `inherited-binary/baseline_repeat.py`
- `runs/clippy-j1-source-bound.json`
- `runs/clippy-j1-source-bound.log`
- `runs/run_gate.py`
- `runs/test-j1-source-bound.json`
- `runs/test-j1-source-bound.log`
- `runs/test-source-bound.json`
- `runs/test-source-bound.log`
- `source-comparison.json`
