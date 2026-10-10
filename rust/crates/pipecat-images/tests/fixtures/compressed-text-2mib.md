# Deterministic compressed metadata fixture

`compressed-text-2mib.png` is a locally generated 1×1 RGB PNG, pixel `[1, 2, 3]`,
with one `comment` zTXt chunk containing exactly 2,097,152 `x` bytes after
inflation. It contains no user or third-party media.

- File bytes: 9712
- SHA-256: `b669a5d26adb7007fab69a0c0e5f49cd9e317d6bb8d2f5d53cd65e4b2188e5b6`
- Compression: pinned flate2 fast rust_backend/miniz path, from the native
  diagnostic of the unchanged 1099816 public projection; PNG chunks/CRCs and
  complete text inflation independently verified
- Image/header chunks: pinned png 0.18.1 native encoder

The bytes are checked in deliberately. Under full workspace feature unification,
the same `Compression::fast()` text becomes 20,278 compressed bytes with zlib-rs,
versus 9,622 bytes with miniz. Both are valid but the former violates the test's
16 KiB compact-fixture premise. The fixture removes that generator dependency;
the test still checks full 2 MiB text inflation, compact encoded length, actual
RGB output under a 128 KiB decoding budget, and the separate JPEG scan bound.
No production decoder setting, resource limit, or dependency changes.
