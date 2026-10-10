"""Generate wire fixtures with original Python classes/serializer using stdlib only.

AST-selects source definitions to avoid importing optional AI-provider packages.
LLMContext is represented by a data holder; its original adapter does the encoding.
Run from any directory: python3 path/to/this/file.py.
"""
import ast
import base64
import dataclasses
import enum
import itertools
import json
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
OUT = Path(__file__).parent / "fixtures" / "python_messages.json"
ids = itertools.count(1)
counts = {}

def count(obj):
    key = type(obj)
    counts[key] = counts.get(key, -1) + 1
    return counts[key]

def load(relative, module_name, selected, **extra):
    module = types.ModuleType(module_name)
    sys.modules[module_name] = module
    module.__dict__.update(dataclass=dataclasses.dataclass, field=dataclasses.field,
        Enum=enum.Enum, StrEnum=enum.StrEnum, Any=object,
        obj_id=lambda: next(ids), obj_count=count, **extra)
    tree = ast.parse((ROOT / relative).read_text())
    body = [ast.ImportFrom(module="__future__", names=[ast.alias(name="annotations")], level=0)]
    body.extend(node for node in tree.body if isinstance(node, ast.ClassDef) and (selected is None or node.name in selected))
    ast.fix_missing_locations(tree := ast.Module(body=body, type_ignores=[]))
    exec(compile(tree, str(ROOT / relative), "exec"), module.__dict__)
    return module

frames = load("src/pipecat/frames/frames.py", "pipecat.frames.frames", {
    "Frame", "DataFrame", "SystemFrame", "ControlFrame", "AudioRawFrame",
    "OutputAudioRawFrame", "TTSAudioRawFrame", "InputAudioRawFrame", "TextFrame",
    "LLMTextFrame", "LLMContextFrame"}, UninterruptibleFrame=type("UninterruptibleFrame", (), {}))
registry = load("src/pipecat/registry/types.py", "pipecat.registry.types", {"WorkerRegistryEntry"})
status = load("src/pipecat/pipeline/job_context.py", "pipecat.pipeline.job_context", {"JobStatus"}).JobStatus
direction = load("src/pipecat/processors/frame_processor.py", "pipecat.processors.frame_processor", {"FrameDirection"}).FrameDirection
bus = load("src/pipecat/bus/messages.py", "pipecat.bus.messages", None)
function = load("src/pipecat/adapters/schemas/function_schema.py", "pipecat.adapters.schemas.function_schema", {"FunctionSchema"}).FunctionSchema
tools = load("src/pipecat/adapters/schemas/tools_schema.py", "pipecat.adapters.schemas.tools_schema", {"AdapterType", "ToolsSchema"}, FunctionSchema=function)
tools_adapter = load("src/pipecat/bus/adapters/tools_schema_adapter.py", "pipecat.bus.adapters.tools_schema_adapter", {"ToolsSchemaAdapter"}, TypeAdapter=object, FunctionSchema=function, ToolsSchema=tools.ToolsSchema, AdapterType=tools.AdapterType).ToolsSchemaAdapter
class Context:
    def __init__(self, messages, tools):
        self.messages, self.tools, self.tool_choice = messages, tools, None
Context.__name__ = "LLMContext"
Context.__module__ = "pipecat.processors.aggregators.llm_context"
context_adapter = load("src/pipecat/bus/adapters/llm_context_adapter.py", "pipecat.bus.adapters.llm_context_adapter", {"LLMContextAdapter"}, TypeAdapter=object, ToolsSchemaAdapter=tools_adapter, is_given=lambda x: x is not None, LLMSpecificMessage=type("Specific", (), {})).LLMContextAdapter
serializer_class = load("src/pipecat/bus/serializers/json.py", "pipecat.bus.serializers.json", {"JSONMessageSerializer"}, MessageSerializer=object, dataclasses=dataclasses, base64=base64, json=json, BaseModel=type("BaseModel", (), {}), _JSON_NATIVE=(str, int, float, bool, type(None))).JSONMessageSerializer
serializer = serializer_class.__new__(serializer_class)
serializer._adapters = {Context: context_adapter(), tools.ToolsSchema: tools_adapter()}
fixtures = []
def add(name, **fields):
    message = getattr(bus, name)(source="worker-a", target="worker-b", **fields)
    fixtures.append(json.loads(serializer.serialize(message)))

for name in ["BusMessage", "BusDataMessage", "BusSystemMessage", "BusDeactivateWorkerMessage"]:
    add(name)
add("BusActivateWorkerMessage", args={"mode": "test"})
for name in ["BusEndMessage", "BusEndWorkerMessage", "BusCancelMessage", "BusCancelWorkerMessage"]:
    add(name, reason="done")
add("BusTTSSpeakMessage", text="hello 世界", append_to_context=False)
add("BusFlushProgressMessage", flush_id=17)
add("BusWorkerReadyMessage", runner="runner-a", parent="parent", active=True, bridged=True, started_at=1720000000.5)
add("BusWorkerRegistryMessage", runner="runner-a", workers=[registry.WorkerRegistryEntry("worker-a", active=True, started_at=1720000000.5)])
for name in ["BusWorkerErrorMessage", "BusWorkerLocalErrorMessage"]:
    add(name, error="example error")
add("BusJobRequestMessage", job_id="job-1", job_name="research", payload={"query":"weather", "bytes":b"\x00\xff"})
for name in ["BusJobResponseMessage", "BusJobResponseUrgentMessage"]:
    for s in status:
        add(name, job_id="job-1", status=s, response={"ok":True})
for name in ["BusJobUpdateMessage", "BusJobUpdateUrgentMessage"]:
    add(name, job_id="job-1", update={"progress":0.5})
add("BusJobUpdateRequestMessage", job_id="job-1")
add("BusJobCancelMessage", job_id="job-1", reason="cancel")
for name in ["BusJobStreamStartMessage", "BusJobStreamDataMessage", "BusJobStreamEndMessage"]:
    add(name, job_id="job-1", data={"metadata":"preserved"})
for frame in [frames.TextFrame("hello"), frames.LLMTextFrame("hello "), frames.TTSAudioRawFrame(b"\x01\x00\xff\x7f", 16000, 1, context_id="tts-1")]:
    frame.metadata["trace"] = "fixture"
    add("BusFrameMessage", frame=frame, direction=direction.DOWNSTREAM, bridge="voice")
standard = function("get_weather", "Get the weather", {"location":{"type":"string"}}, ["location"])
context = Context([{"role":"user", "content":"hi"}], tools.ToolsSchema([standard], {tools.AdapterType.GEMINI:[{"google_search":{}}]}))
add("BusFrameMessage", frame=frames.LLMContextFrame(context), direction=direction.UPSTREAM)
OUT.write_text(json.dumps(fixtures, ensure_ascii=False, indent=2) + "\n")
print(f"Wrote {len(fixtures)} original-source serializer fixtures to {OUT}")
