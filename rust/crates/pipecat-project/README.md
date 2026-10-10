# pipecat-project

Native Rust project generation for the implemented browser voice runner. The
`pipecat-init` binary and reusable `ProjectPlan`/`generate_cancellable` library
write a Cargo application with Rust source, `.env.example`, `.gitignore`, README,
and reproducible generation choices in `pipecat-project.json`.

## Use

Run from the repository's `rust/` directory, with an existing output parent:

```sh
cargo run -p pipecat-project --bin pipecat-init -- \
  my-voice-bot --source-rust "$PWD" --parent /absolute/output/parent \
  --client rust-wasm
```

The dependency is a canonical local `pipecat-runner` path. No published crate,
unpushed Git branch, Python runtime, subprocess installer, or deployment service
is assumed. Keep the local checkout at that path; update generated `Cargo.toml`
after moving it. The generated application has its own Cargo workspace so it can
also be created inside another workspace without implicit membership.

`--dry-run` validates and prints paths/config without writing; `--list-options`
prints the supported integrations. `--config FILE` accepts the same strict JSON
shape as generated `pipecat-project.json`; explicit CLI flags win regardless of
argument order. Config is capped at 16 KiB, must be an object, and rejects duplicate
or unknown fields. Credentials are never accepted as generation fields or read
from the environment.

The optional `rust-wasm` client embeds an explicit source allowlist from
`rust/client-templates/`: Cargo manifest/lock, build script, Rust source, and
public HTML/CSS. Its generated browser config advertises only WebSocket and the
same-origin `/api/start` endpoint. It never copies `.env`, output bundles, caches,
private files, Python fixtures, or an arbitrary source directory. Run its build
script using `bash client/build.sh` as described in the generated README.

## Filesystem and cancellation contract

- Project names are portable lowercase Cargo identifiers, 1–64 characters;
  traversal, separators, shell/TOML injection, reserved names, and controls fail
- The explicitly selected parent must exist and cannot contain `..` or symlink
  components; the final destination must not exist, including dangling symlinks
- Exclusive directory/file creation refuses overwrites, including a destination
  created between planning and generation; no `--force` mode exists
- Planning renders all files in memory in deterministic order before writing
- Ctrl-C cooperatively cancels a write. Library callers can cancel at each file
  boundary, including before the first write or before committing the result
- Errors, cancellation, and unwinding panics trigger rollback of files/directories
  created by that invocation. Rollback never recursively deletes unrelated files
- The parent directory is trusted local storage, not an adversarial filesystem
  sandbox. SIGKILL, power loss, or cleanup permission errors can leave partial
  files; directory publication is not an atomic transaction

## Coverage of Python source surfaces

| Source | Native coverage | Explicit limit |
| --- | --- | --- |
| `cli/config_validator.py`, `scaffold.py` | Strict config, CLI precedence, defaults, dry-run, options | Native schema is smaller; unknown Python-only options error |
| `cli/generators/project.py` | Validated deterministic file plan, safe creation, rollback | No in-place merge or overwrite |
| `templates/server/bot_cascade.py.jinja2` | Native `pipecat_runner::run_from_env` entrypoint | Implemented WebSocket + Deepgram/OpenAI/Cartesia cascade only |
| `templates/server/pyproject.toml.jinja2` | Cargo manifest with real local dependency | Rust 1.99+ and local source checkout required |
| `templates/server/env.example.jinja2` | Empty provider secrets and supported non-secret defaults | No automatic dotenv load; no secret discovery |
| `templates/README.md.jinja2`, `gitignore.jinja2` | Native build/run/setup/limitations docs and private-file ignores | No false live-provider or physical-device acceptance claims |
| Client templates | Optional existing native Rust/WASM source scaffold | Not a React, Next.js, or JS SDK compatibility layer |
| Cloud/Daily/LiveKit/telephony/realtime templates | Explicit unsupported configuration error | Never silently replaced or emitted as a broken project |

The Python CLI/templates remain available as reference and are not modified.
Generation is not a claim of full CLI feature parity, native provider coverage,
production auth, or cloud deployment readiness.

## Verify

From `rust/`, with the pinned Rust toolchain and cached dependencies available:

```sh
cargo test -p pipecat-project --offline
cargo clippy -p pipecat-project --offline --all-targets -- -D warnings
cargo fmt -p pipecat-project --check
cargo test -p pipecat-project --offline --test cli \
  generated_application_builds_and_runs_without_credentials -- --ignored
```

The last test creates a fresh external application, builds the real local runner,
and executes its credential-free help command. It reuses the workspace target
cache and requires its native dependencies to be available offline. It does not
contact live AI providers or exercise physical microphone/speaker hardware.
