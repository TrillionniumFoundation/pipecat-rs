# Image codec dependency delta

The locked package set retains every pre-existing package version and checksum.
PNG/JPEG decoding adds three registry packages and one vendored package from
the exact official jpeg-decoder archive:

| Package | Version | SPDX license | crates.io archive SHA-256 |
| --- | --- | --- | --- |
| png | 0.18.1 | MIT OR Apache-2.0 | `60769b8b31b2a9f263dae2776c37b1b28ae246943cf719eb6946a1db05128a61` |
| jpeg-decoder (vendored, synchronous-worker patch) | 0.3.2 | MIT OR Apache-2.0 | `00810f1d8b74be64b13dbf3db89ac67740615d6c891f0e7b6179326533011a07` |
| fdeflate | 0.3.7 | MIT OR Apache-2.0 | `1e6853b52649d4ac5c0bd02320cddc5ba956bdb407c4b75a2c6b75bf51500f8c` |
| miniz_oxide | 0.8.9 | MIT OR Zlib OR Apache-2.0 | `1fa76a2c86f704bdb222d66965fb3d63269ce38518b83cb0575fca855ebb6316` |

`png` and `jpeg-decoder` are exact version requirements. PNG adds fdeflate and
miniz_oxide 0.8.9; existing flate2 still uses the original miniz_oxide 0.9.1.
Existing bitflags, crc32fast, flate2, adler2, and simd-adler32 versions remain locked.
`jpeg-decoder` is a pinned direct path to `rust/vendor/jpeg-decoder-0.3.2`.
Disabling upstream Rayon alone still permits detached std::thread component
workers. The vendor patch compiles only ImmediateWorker and performs final color
conversion synchronously, even if Rayon is enabled by feature unification.
`platform_independent` is enabled to forbid architecture-specific unsafe code.
The official version, manifests, and licenses remain unchanged. See its PATCHES.md,
LOCAL_PATCH.diff, and UPSTREAM_SHA256.json for the exact delta and archive members.
No C codec or subprocess is introduced. Existing reqwest rustls verification is unchanged.

Licenses above were checked against downloaded registry package manifests.
The root crate remains BSD-2-Clause. Codec copyright/license files are supplied
by their registry archives; redistributors must retain the selected notices.
This delta is dependency provenance, not a claim of a complete vulnerability audit.

Primary implementation references:

* https://docs.rs/png/0.18.1/png/struct.Decoder.html
* https://docs.rs/png/0.18.1/png/struct.Reader.html
* https://docs.rs/jpeg-decoder/0.3.2/jpeg_decoder/struct.Decoder.html
* https://docs.rs/jpeg-decoder/0.3.2/src/jpeg_decoder/decoder.rs.html

PNG's own limits are documented as best effort, so this adapter additionally
bounds encoded storage, chunk count, pre-decode dimensions, pixels, transformed
output bytes, and conservative scratch admission. Compressed text/ICC processing
is disabled. JPEG's output buffer limit alone does not limit progressive
coefficients or work; the adapter additionally bounds dimensions, sampling,
markers, and scans before instantiating the decoder.

The crate has `publish = false`, since registry publishing can strip a path and
restore upstream's unqualified detached-worker behavior. The patched JPEG package
has a path identity in Cargo.lock, not a crates.io checksum entry; the table above
records its verified original archive checksum. Every pre-slice locked registry
package identity/version/checksum is unchanged.
