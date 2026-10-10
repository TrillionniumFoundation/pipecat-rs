#!/usr/bin/env python3
"""Extract static dataclass frame contracts without importing Python Pipecat.

Run from the repository with Python 3.10+ after deliberately choosing a new
upstream reference. Runtime tests use the checked-in fixture, not Python.
"""
import ast
import json
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / "src/pipecat/frames/frames.py"
TARGET = Path(__file__).resolve().parents[1] / "tests/fixtures/python_frame_contract.json"
module = ast.parse(SOURCE.read_text())
classes = {node.name: node for node in module.body if isinstance(node, ast.ClassDef)}

def bases(name):
    result = []
    for base in classes[name].bases:
        while isinstance(base, ast.Subscript): base = base.value
        if isinstance(base, ast.Name): result.append(base.id)
    return result

def inherits(name, target):
    return name == target or any(base in classes and inherits(base, target) for base in bases(name))

def interruptible(name):
    for field in classes[name].body:
        if isinstance(field, ast.AnnAssign) and isinstance(field.target, ast.Name) and field.target.id == "interruptible":
            if isinstance(field.value, ast.Call):
                for keyword in field.value.keywords:
                    if keyword.arg == "default":
                        return ast.literal_eval(keyword.value)
    for base in bases(name):
        if base in classes and inherits(base, "Frame"):
            return interruptible(base)
    return True

def fields(name):
    result = {}
    for base in bases(name):
        if base in classes:
            result.update(fields(base))
    for field in classes[name].body:
        if isinstance(field, ast.AnnAssign) and isinstance(field.target, ast.Name):
            result[field.target.id] = ast.unparse(field.annotation)
    return result

payload = {
    "source_commit": "af9f9da9d7afddaa8811c0e0251b32566c980bdf",
    "source_file": "src/pipecat/frames/frames.py",
    "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
    "classes": [{"name": name, "frame": inherits(name, "Frame"),
                 "priority": "start" if name == "StartFrame" else "system" if inherits(name, "SystemFrame") else "normal",
                 "interruptible": interruptible(name), "fields": fields(name)} for name in classes],
}
TARGET.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
print(f"Exported {len(classes)} source classes to {TARGET}")
