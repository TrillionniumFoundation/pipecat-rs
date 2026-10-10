#!/usr/bin/env python3
"""Verify the pinned upstream source and presence of the native protocol gates."""
import hashlib
import json
from pathlib import Path

crate = Path(__file__).resolve().parents[1]
root = crate.parents[2]
manifest = json.loads((crate / "source-manifest.json").read_text())
for name, expected in manifest["source_files"].items():
    actual = hashlib.sha256((root / name).read_bytes()).hexdigest()
    if actual != expected:
        raise SystemExit(f"source changed: {name}")
for name, path in manifest["native_gates"].items():
    if not (crate / path).is_file():
        raise SystemExit(f"missing gate: {name}")
print(f"Verified {len(manifest['source_files'])} pinned sources and {len(manifest['native_gates'])} native gate files")
