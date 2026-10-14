"""Offline source-AST fixture generator; never imports or executes Pipecat.

Pass the pinned frames.py; output the fixture to stdout. Rust runtime/tests use
only the checked-in JSON. The transformer behavior fields come from reviewed
text_transformer.py lines 42–50, not execution of arbitrary Python code.
"""
import ast
import hashlib
import json
import sys
from pathlib import Path

raw = Path(sys.argv[1]).read_bytes()
blob = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
assert blob == '79dea3e32f5097a048d03b49f48031811fa1eca6', 'wrong pinned frames.py'
classes = {node.name: [ast.unparse(base) for base in node.bases]
           for node in ast.parse(raw).body if isinstance(node, ast.ClassDef)}
def text_child(name):
    return name == 'TextFrame' or any(text_child(base) for base in classes.get(name, []))
print(json.dumps({
    'source_repository': 'ProfHepta/pipecat',
    'source_commit': 'af9f9da9d7afddaa8811c0e0251b32566c980bdf',
    'source_path': 'src/pipecat/frames/frames.py',
    'source_blob': blob,
    'transformer_path': 'src/pipecat/processors/text_transformer.py',
    'transformer_blob': 'dffee7ac4bc3155f8f8997aa66e3057869ee1949',
    'source_classes': sorted(name for name in classes if text_child(name)),
    'source_semantics': {
        'text_direction': 'Downstream', 'new_plain_text_frame': True,
        'empty_output_is_forwarded': True,
        'non_text_identity_and_direction_preserved': True,
    },
}, indent=2))
