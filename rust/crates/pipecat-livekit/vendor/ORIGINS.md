# Vendored LiveKit sources

The two directories are the published crates from the official LiveKit Rust SDK
at commit `a6f93a16fa71fe811d040de175e070e233d9b1fe`.

| Published crate | Registry archive SHA-256 |
| --- | --- |
| livekit 0.9.4 | `4dc818741891a8179eafd4665156482761d5084fb15238c1de9c211fdb974e3e` |
| livekit-signaling 0.1.5 | `f2fd4fb8ade2b175576955b5927adf2ab4375bbe46f2bfcd3edc658e272f0eef` |

Sources: https://crates.io/crates/livekit/0.9.4 and
https://crates.io/crates/livekit-signaling/0.1.5. The archive digests match the
registry checksums. Both packages are Apache-2.0 licensed. All published source
notices and `.cargo_vcs_info.json` files are preserved. `LICENSE-APACHE-2.0` is
from the upstream repository at the same commit:
https://raw.githubusercontent.com/livekit/rust-sdks/a6f93a16fa71fe811d040de175e070e233d9b1fe/LICENSE.

The dependency path in `livekit-0.9.4/Cargo.toml` selects the sibling signaling
crate, so downstream applications inherit the lifecycle fix without a
workspace-level Cargo patch. The published manifests and original manifests
are otherwise retained. See `PATCHES.md` and `lifecycle.patch` for the limited
local changes.
