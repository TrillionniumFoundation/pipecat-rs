#!/usr/bin/env python3
"""Regenerate exhaustive Rust contracts from the reviewed native/source mapping."""
from pathlib import Path
import json
import re
ROOT = Path(__file__).resolve().parents[1]
source = (ROOT / "src/frames.rs").read_text()
start = source.index("pub enum FrameData {") + len("pub enum FrameData {")
depth, end = 1, start
while depth:
    if source[end] == "{": depth += 1
    if source[end] == "}": depth -= 1
    end += 1
body = source[start:end - 1]
patterns = {}
start = depth = 0
for i, char in enumerate(body):
    if char in "({<[": depth += 1
    if char in ")}>]": depth -= 1
    if char == "," and depth == 0:
        item = body[start:i].strip(); start = i + 1
        if item:
            name = re.match(r"\w+", item)[0]
            patterns[name] = name + ("(..)" if "(" in item else " { .. }" if "{" in item else "")
contracts = {row["name"]: row for row in json.loads((ROOT / "tests/fixtures/python_frame_contract.json").read_text())["classes"]}
mapping = json.loads((ROOT / "tests/fixtures/rust_frame_mapping.json").read_text())
assert set(patterns) == set(mapping), (set(patterns) - set(mapping), set(mapping) - set(patterns))
lines = ["//! Exhaustive native frame contracts checked against the source AST fixture.", "use crate::{FrameData, FramePriority};", "", "#[derive(Debug, Clone, Copy, PartialEq, Eq)]", "pub struct FrameContract { pub rust_variant: &'static str, pub source_class: &'static str, pub priority: FramePriority, pub interruptible: bool }", "", "pub const FRAME_CONTRACTS: &[FrameContract] = &["]
for variant, name in mapping.items():
    contract = contracts[name]
    lines.append(f'    FrameContract {{ rust_variant: "{variant}", source_class: "{name}", priority: FramePriority::{contract["priority"].title()}, interruptible: {str(contract["interruptible"]).lower()} }},')
lines += ["];", "", "impl FrameData {"]
for method, result, value in [
    ("source_class_name", "&'static str", lambda variant, contract: f'"{mapping[variant]}"'),
    ("priority", "FramePriority", lambda variant, contract: "FramePriority::" + contract["priority"].title()),
    ("default_interruptible", "bool", lambda variant, contract: str(contract["interruptible"]).lower()),
]:
    lines += [f"    pub fn {method}(&self) -> {result} {{", "        match self {"]
    for variant, name in mapping.items(): lines.append(f"            Self::{patterns[variant]} => {value(variant, contracts[name])},")
    lines += ["        }", "    }"]
lines += ["}"]
(ROOT / "src/frame_contract.rs").write_text("\n".join(lines) + "\n")
missing = [name for name, contract in contracts.items() if contract["frame"] and name not in mapping.values()]
assert not missing, missing
print(f"Generated {len(mapping)} variants covering {sum(contract['frame'] for contract in contracts.values())} source frame classes")
