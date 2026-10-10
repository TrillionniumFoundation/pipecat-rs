# moq-tokio 0.19.19 manifest correction

Upstream: https://github.com/moq-dev/moq/tree/b00be0ffc3734addcdd5e6ad4b348ae055e70957/rs/moq-tokio

Registry archive: `moq-tokio-0.19.19.crate`, 296115 bytes,
SHA-256 `122023d49844bba6e2010fe02040695db2995d59b4bef13e520696d04a99b05b`.
The original normalized manifest and all Rust implementation files come from
that verified official archive. `Cargo.toml.orig` is retained unchanged.

Only the normalized `Cargo.toml` dependency declaration is modified:
`dependencies.rustls.default-features = false`, retaining the non-provider
features `std`, `tls12`, and `logging` explicitly. The upstream TLS helper
references `TLS12` unconditionally. No Rust implementation,
transport adapter, wire codec, or upstream feature definition is modified.

Upstream's unconditional Rustls defaults activate AWS-LC even when
moq-tokio's own default features are disabled. That conflicts with the
workspace's ring-only implicit WSS/WebPki configuration. Pipecat explicitly
enables ring through its direct native TLS dependencies; no global provider
is installed by this patch. Default-disabled adapter use is the qualified
configuration. Other moq-tokio backends and provider features remain unqualified.

This directory is explicitly excluded from the parent Cargo workspace, so
`--workspace --all-features` does not enable its optional providers/backends.
Pipecat uses a pinned direct path dependency rather than a root-only patch,
so source consumers retain this correction in downstream workspaces.
Registry publishing is not qualified: Cargo can strip the path and select
the unpatched registry version. `pipecat-moq` has publishing disabled until
a suitable upstream release or qualified packaging route is available.

License: MIT OR Apache-2.0, Copyright (c) 2023 Luke Curley. The complete
LICENSE-MIT and LICENSE-APACHE files are copied from the exact upstream
commit above; the registry archive does not include those root-level files.
