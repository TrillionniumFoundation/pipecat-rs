#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
target_dir="${CARGO_TARGET_DIR:-target}"
cargo build --locked --release --target wasm32-unknown-unknown --target-dir "$target_dir"
mkdir -p dist/pkg
wasm-bindgen --target web --no-typescript --out-dir dist/pkg "$target_dir/wasm32-unknown-unknown/release/pipecat_browser_client.wasm"
cp web/index.html web/style.css web/config.json dist/
printf 'Built static browser client in %s/dist\n' "$PWD"
