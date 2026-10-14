# jpeg-decoder 0.3.2 local qualification

Upstream registry archive: `jpeg-decoder-0.3.2.crate`, SHA-256
`00810f1d8b74be64b13dbf3db89ac67740615d6c891f0e7b6179326533011a07`.
Registry: https://crates.io/crates/jpeg-decoder/0.3.2
Source commit: see the unchanged `.cargo_vcs_info.json`.
All archive files, manifests, licenses, and package version are retained.
Implementation changes are recorded in `LOCAL_PATCH.diff`: synchronous worker
selection and an opt-in strict DCT validation profile, with local unit tests.

The upstream public Decoder API does not expose worker selection. Disabling
`rayon` still allows its width-based worker-selection heuristic to choose
`MpscWorker`. That implementation creates per-component `std::thread` workers,
uses unbounded message channels, and drops their JoinHandles. A decode error or
cancellation can therefore return while component CPU work remains active.

This local patch unconditionally selects `ImmediateWorker` and synchronous
final color conversion. Neither `multithreaded` nor `rayon` worker modules are
compiled; enabling the upstream `rayon` feature cannot change the synchronous
execution contract. The existing immediate worker, IDCT, upsampling, and color-conversion code
remain unchanged. A unit regression pins
the Multithreaded preference to Immediate dispatch.

`pipecat-images` additionally enables upstream `platform_independent`, so its
codec configuration forbids architecture-specific unsafe implementations. Its
one owned outer decoder thread retains the job permit through complete decode
and conversion, and joins on last-owner drop. This patch does not promise
instantaneous cancellation inside a synchronous codec CPU operation.

The dependency is a version-pinned direct path, and this vendor directory is
excluded from the parent workspace. `pipecat-images` is `publish = false`:
registry publication is not qualified because stripping the path could restore
the unpatched detached-worker behavior. Source-tree consumers retain the patch.

License: MIT OR Apache-2.0. Both complete upstream license files are included.
`UPSTREAM_SHA256.json` records the original archive members' SHA-256 values;
`LOCAL_PATCH.diff` is the exact implementation delta against the official archive.

## Opt-in strict DCT validation

`Decoder::set_strict_mode(true)` enables the browser import validation profile;
the default remains false, retaining upstream tolerant/native decoding behavior.
This profile intentionally rejects valid-but-incomplete progressive encodings
that omit coefficient bands or stop refinement above bit zero, and rejects
lossless JPEGs. It is not a claim of exhaustive JPEG conformance validation.

The Huffman lookahead buffer tracks real entropy bits separately from upstream's
synthetic zero lookahead. Strict decoding cannot consume synthetic bits. Scan
and restart boundaries must contain only zero to seven all-one alignment bits,
followed by a marker; extra entropy bytes, malformed padding, missing markers,
overlong EOB runs and out-of-range AC runs are rejected. All-one Huffman codes
are rejected to prevent padding being interpreted as symbols. Unexpected final
restart markers and extraneous bytes between marker segments are rejected.

A fixed 4-by-64 coefficient-state array checks initial scan uniqueness, DC before
AC, ordered refinement, and complete coverage through bit zero before EOI. This
bounds accepted scans by 4 * 64 * 14 = 3584, independent of input. Entropy decoding
retains the existing dimension-derived MCU bounds. Input size, dimensions and
allocation budgets must be checked by the caller; this patch does not move the
upstream late decoded-buffer-size check ahead of progressive allocations.

The reader stops immediately after EOI. The caller MUST separately reject
trailing bytes, e.g. decode through `&mut Cursor`, drop the decoder, and require
`cursor.position() == input.len() as u64`. `read_info()` is only metadata parsing;
it does not validate entropy or scan completion.

`src/strict_tests.rs` includes structural tiny-image cases and inline RGB
baseline/progressive/restart fixtures generated with Pillow 12.3. It covers
metadata-before-decode, valid exact consumption, synthetic/truncated entropy,
extra entropy, malformed padding, missing EOI, marker garbage, incomplete and
duplicate/out-of-order progressive scans, restart numbering and padding,
all-one codes, exact AC/refinement run boundaries, textured progressive
refinement, and the caller's trailing-data check. Fixtures
need no runtime generator or extra dependency.
