# jpeg-decoder 0.3.2 synchronous worker qualification

Upstream registry archive: `jpeg-decoder-0.3.2.crate`, SHA-256
`00810f1d8b74be64b13dbf3db89ac67740615d6c891f0e7b6179326533011a07`.
Registry: https://crates.io/crates/jpeg-decoder/0.3.2
Source commit: see the unchanged `.cargo_vcs_info.json`.
All archive files, manifests, licenses, and package version are retained.
The only implementation modification is `src/worker/mod.rs`, recorded below.

The upstream public Decoder API does not expose worker selection. Disabling
`rayon` still allows its width-based worker-selection heuristic to choose
`MpscWorker`. That implementation creates per-component `std::thread` workers,
uses unbounded message channels, and drops their JoinHandles. A decode error or
cancellation can therefore return while component CPU work remains active.

This local patch unconditionally selects `ImmediateWorker` and synchronous
final color conversion. Neither `multithreaded` nor `rayon` worker modules are
compiled; enabling the upstream `rayon` feature cannot change the synchronous
execution contract. The existing immediate worker and all parsing, IDCT,
upsampling, and color-conversion code remain unchanged. A unit regression pins
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
