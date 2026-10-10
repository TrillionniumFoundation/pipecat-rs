"""Record offline TOKEN-mode source oracles; never called by Cargo or the runtime.

Run with the existing source-validation Python environment: sentencex==1.0.32,
loguru, and typing_extensions. No packages, model data, or network are fetched.
The original test module and sequencer are executed unchanged with observational
wrappers. Required frame definitions are compiled unchanged from verified AST
nodes, avoiding unrelated provider dependencies imported by the full frame module.
Every repository module is checked against BASELINE before its code executes.
"""

import ast
import asyncio
import dataclasses
import hashlib
import importlib.abc
import importlib.machinery
import importlib.metadata
import inspect
import json
from pathlib import Path
import platform
import subprocess
import sys
import types
from typing import Any
import unicodedata
import unittest

ROOT = Path(__file__).resolve().parents[4]
BASELINE = "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
TEST_MODULE = "test_aggregated_frame_sequencer"
FRAME_PATH = "src/pipecat/frames/frames.py"
FRAME_DEFINITIONS = (
    "format_pts",
    "Frame",
    "DataFrame",
    "UninterruptibleFrame",
    "TextFrame",
    "AggregatedTextFrame",
    "TTSTextFrame",
    "AggregatedTextProgressFrame",
)
SELECTED_CLASSES = (
    "TestParallelSentenceAggregator",
    "TestRegisterSpokenStreaming",
    "TestRegisterSpokenBufferedWords",
    "TestRegisterSkippedForcesFinalize",
    "TestFinalizeEndOfTurn",
    "TestFinalizeRescuesMidSentencePrefix",
    "TestConcurrentContexts",
    "TestClearResetsStreamingState",
)
VERIFIED = {}
sys.dont_write_bytecode = True


def verified_source(path):
    """Return baseline-identical bytes, recording their digest before execution."""
    path = Path(path).resolve()
    relative = str(path.relative_to(ROOT))
    if relative not in VERIFIED:
        source = path.read_bytes()
        baseline = subprocess.check_output(["git", "show", f"{BASELINE}:{relative}"], cwd=ROOT)
        if source != baseline:
            raise SystemExit(f"Source differs from baseline: {relative}")
        VERIFIED[relative] = (source, hashlib.sha256(source).hexdigest())
    return VERIFIED[relative][0]


class VerifiedLoader(importlib.machinery.SourceFileLoader):
    def get_code(self, fullname):
        return compile(verified_source(self.path), self.path, "exec")


class BaselineFinder(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        spec = importlib.machinery.PathFinder.find_spec(fullname, path)
        if spec is not None and spec.origin and spec.origin.endswith(".py"):
            source = Path(spec.origin).resolve()
            if source.is_relative_to(ROOT):
                verified_source(source)
                spec.loader = VerifiedLoader(fullname, str(source))
                return spec
        return None


# Check the primary source and test bytes even before package initialization.
for relative in (
    FRAME_PATH,
    "src/pipecat/__init__.py",
    "src/pipecat/utils/context/aggregated_frame_sequencer.py",
    "src/pipecat/utils/text/simple_text_aggregator.py",
    "src/pipecat/utils/string.py",
    f"tests/{TEST_MODULE}.py",
):
    verified_source(ROOT / relative)
if importlib.metadata.version("sentencex") != "1.0.32":
    raise SystemExit("This baseline requires sentencex==1.0.32")
sys.meta_path.insert(0, BaselineFinder())
package = types.ModuleType("pipecat")
package.__path__ = [str(ROOT / "src/pipecat")]
sys.modules["pipecat"] = package
sys.path.insert(0, str(ROOT / "tests"))

from pipecat.utils.deprecation import deprecated
from pipecat.utils.text.base_text_aggregator import AggregationType
from pipecat.utils.time import nanoseconds_to_str
from pipecat.utils.utils import obj_count, obj_id

# Preserve decorators, bases, annotations, and bodies of the selected definitions.
frame_tree = ast.parse(verified_source(ROOT / FRAME_PATH), filename=FRAME_PATH)
frame_nodes = [
    node
    for node in frame_tree.body
    if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in FRAME_DEFINITIONS
]
if {node.name for node in frame_nodes} != set(FRAME_DEFINITIONS):
    raise SystemExit("Required source frame definitions changed")
frame_module = types.ModuleType("pipecat.frames.frames")
frame_module.__file__ = str(ROOT / FRAME_PATH)
frame_module.__dict__.update(
    dataclass=dataclasses.dataclass,
    field=dataclasses.field,
    Any=Any,
    AggregationType=AggregationType,
    deprecated=deprecated,
    nanoseconds_to_str=nanoseconds_to_str,
    obj_count=obj_count,
    obj_id=obj_id,
)
sys.modules[frame_module.__name__] = frame_module
future = ast.parse("from __future__ import annotations").body
exec(
    compile(ast.Module(body=future + frame_nodes, type_ignores=[]), str(ROOT / FRAME_PATH), "exec"),
    frame_module.__dict__,
)

from loguru import logger
from pipecat.utils.context.aggregated_frame_sequencer import (
    AggregatedFrameSequencer,
    _ParallelSentenceAggregator,
)

logger.remove()
TRACES = []
CURRENT_TEST = ""
DEPTH = 0
FRAME_IDS = {}


def frame_id(raw):
    if raw not in FRAME_IDS:
        FRAME_IDS[raw] = len(FRAME_IDS)
    return FRAME_IDS[raw]


def serial(value):
    if isinstance(value, frame_module.Frame):
        result = {"class": type(value).__name__, "id": frame_id(value.id)}
        for name in (
            "pts", "transport_destination", "text", "aggregated_by", "context_id",
            "raw_text", "will_be_spoken", "append_to_context", "includes_inter_frame_spaces",
            "segment_id", "accumulated_text", "remaining_text",
        ):
            if hasattr(value, name):
                item = getattr(value, name)
                result[name] = frame_id(item) if name == "segment_id" else serial(item)
        return result
    if dataclasses.is_dataclass(value):
        return {
            field.name: serial(getattr(value, field.name)) for field in dataclasses.fields(value)
        }
    if isinstance(value, (tuple, list)):
        return [serial(item) for item in value]
    if isinstance(value, dict):
        return {key: serial(item) for key, item in value.items()}
    return value


def parallel_state(obj):
    return {
        "tts": obj._tts,
        "llm": obj._llm,
        "user": obj._user,
        "aligned": obj._aligned,
        "inner_text": obj._aggregator._text,
        "lookahead_state": obj._aggregator._lookahead_state.name,
        "language": obj._aggregator.language,
    }


def tracker_state(obj):
    if obj is None:
        return None
    return {
        "is_complete": obj.is_complete,
        "word": obj.get_word_for_frame(),
        "overflow": obj.get_overflow_word(),
        "llm_consumed": obj.get_llm_consumed(),
        "suppress": obj.suppress_in_context(),
        "accumulated_user": obj.get_accumulated_user_facing_text(),
        "remaining_user": obj.get_remaining_user_facing_text(strip=False),
        "accumulated_tts": obj.get_accumulated_tts_text(),
        "remaining_tts": obj.get_remaining_tts_text(strip=False),
        "accumulated_llm": obj.get_accumulated_llm_text(),
        "remaining_llm": obj.get_remaining_llm_text(),
    }


def sequencer_state(obj):
    return {
        "slots": [
            {
                "frame": serial(slot.frame),
                "context_id": slot.context_id,
                "spoken": slot.spoken,
                "complete": slot.complete,
                "includes_inter_frame_spaces": slot.includes_inter_frame_spaces,
                "transport_destination": slot.transport_destination,
                "tracker": tracker_state(slot.tracker),
            }
            for slot in obj._slots
        ],
        "context_append_to_context": dict(obj._context_append_to_context),
        "streaming_contexts": {
            context_id: {
                "append_to_context": context.append_to_context,
                "build_tracker": context.build_tracker,
                "aggregator": parallel_state(context.aggregator),
            }
            for context_id, context in obj._streaming_contexts.items()
        },
        "buffered_words": serial(obj._buffered_words),
    }


def instrument(cls, methods, state):
    original_init = cls.__init__

    def init(self, *args, **kwargs):
        global DEPTH
        outer = DEPTH == 0
        DEPTH += 1
        try:
            original_init(self, *args, **kwargs)
        finally:
            DEPTH -= 1
        if outer:
            config = inspect.signature(original_init).bind(self, *args, **kwargs)
            config.apply_defaults()
            self._oracle_trace = {
                "test": CURRENT_TEST,
                "class": cls.__name__,
                "config": serial(
                    {key: value for key, value in config.arguments.items() if key != "self"}
                ),
                "initial_state": state(self),
                "operations": [],
            }
            TRACES.append(self._oracle_trace)

    cls.__init__ = init

    def begin(self, method, args, kwargs):
        return {"method": method, "args": serial(args), "kwargs": serial(kwargs)}

    def finish(self, operation, result):
        operation.update(result=serial(result), state=state(self))
        self._oracle_trace["operations"].append(operation)

    for method in methods:
        original = getattr(cls, method)
        if inspect.isasyncgenfunction(original):
            async def wrapped(self, *args, _original=original, _method=method, **kwargs):
                global DEPTH
                outer = DEPTH == 0
                operation = begin(self, _method, args, kwargs) if outer else None
                DEPTH += 1
                result = []
                try:
                    async for value in _original(self, *args, **kwargs):
                        result.append(value)
                        yield value
                finally:
                    DEPTH -= 1
                if outer:
                    finish(self, operation, result)
        elif inspect.iscoroutinefunction(original):
            async def wrapped(self, *args, _original=original, _method=method, **kwargs):
                global DEPTH
                outer = DEPTH == 0
                operation = begin(self, _method, args, kwargs) if outer else None
                DEPTH += 1
                try:
                    result = await _original(self, *args, **kwargs)
                finally:
                    DEPTH -= 1
                if outer:
                    finish(self, operation, result)
                return result
        else:
            def wrapped(self, *args, _original=original, _method=method, **kwargs):
                global DEPTH
                outer = DEPTH == 0
                operation = begin(self, _method, args, kwargs) if outer else None
                DEPTH += 1
                try:
                    result = _original(self, *args, **kwargs)
                finally:
                    DEPTH -= 1
                if outer:
                    finish(self, operation, result)
                return result
        setattr(cls, method, wrapped)


instrument(
    _ParallelSentenceAggregator,
    ("aggregate", "flush", "handle_interruption", "set_language"),
    parallel_state,
)
instrument(
    AggregatedFrameSequencer,
    ("register_spoken", "register_skipped", "finalize", "process_word", "complete_spoken_slot",
     "flush", "force_complete", "clear"),
    sequencer_state,
)


class Result(unittest.TextTestResult):
    def startTest(self, test):
        global CURRENT_TEST
        CURRENT_TEST = test.id()
        FRAME_IDS.clear()
        super().startTest(test)


suite = unittest.TestSuite(
    unittest.defaultTestLoader.loadTestsFromName(f"{TEST_MODULE}.{name}")
    for name in SELECTED_CLASSES
)
result = unittest.TextTestRunner(resultclass=Result).run(suite)
if not result.wasSuccessful():
    raise SystemExit("Original source tests failed; refusing fixture output")


async def extra():
    """Exercise language selection and Unicode on the same verified source."""
    global CURRENT_TEST
    for name, language, chunks in (
        ("cjk", "ja", ["こんにちは。", "次です！", "終わり"]),
        ("german_abbreviation", "de-DE", ["Das ist z.B. wichtig.", " Weiter geht es."]),
        ("unicode_paragraphs", "en", ["👋 Hello.", " 😀 Next", "\n\nLast."]),
        ("quoted_abbreviation", "en", ['She said, "Dr.', ' Smith is here."', " Then left."]),
    ):
        CURRENT_TEST = f"additional.parallel_language.{name}"
        obj = _ParallelSentenceAggregator(language=language)
        for chunk in chunks:
            async for _ in obj.aggregate(chunk, chunk, chunk):
                pass
        await obj.flush()
    CURRENT_TEST = "additional.parallel_set_language_preserves_buffers"
    obj = _ParallelSentenceAggregator()
    async for _ in obj.aggregate("Hallo.", "Hallo.", "Hallo."):
        pass
    obj.set_language("de-DE")
    async for _ in obj.aggregate(" Weiter", " Weiter", " Weiter"):
        pass
    await obj.flush()
    CURRENT_TEST = "additional.parallel_set_language_cjk_pending"
    obj = _ParallelSentenceAggregator(language="en")
    async for _ in obj.aggregate("こんにちは。", "こんにちは。", "こんにちは。"):
        pass
    obj.set_language("ja-JP")
    async for _ in obj.aggregate("次です", "次です", "次です"):
        pass
    await obj.flush()
    CURRENT_TEST = "additional.parallel_whitespace_flush_then_input"
    obj = _ParallelSentenceAggregator()
    async for _ in obj.aggregate(" \t\n", " \t\n", " \t\n"):
        pass
    await obj.flush()
    await obj.flush()
    for chunk in ("Hi", "!", " Next"):
        async for _ in obj.aggregate(chunk, chunk, chunk):
            pass
    await obj.flush()
    CURRENT_TEST = "additional.parallel_interrupt_divergent_buffers"
    obj = _ParallelSentenceAggregator(language="de-DE")
    async for _ in obj.aggregate("fünf Euro.", "<price>5 €.</price>", "5 €."):
        pass
    await obj.handle_interruption()
    await obj.flush()
    for chunk in ("Das ist z.B. wichtig.", " Weiter"):
        async for _ in obj.aggregate(chunk, chunk, chunk):
            pass
    await obj.flush()


asyncio.run(extra())
output = ROOT / "rust/crates/pipecat-text/tests/fixtures/token_source_traces.json"
provenance = {
    "schema_version": 1,
    "baseline": BASELINE,
    "source_tests_run": result.testsRun,
    "source_test_classes": list(SELECTED_CLASSES),
    "python_version": sys.version,
    "python_implementation": platform.python_implementation(),
    "unicode_version": unicodedata.unidata_version,
    "dependencies": {
        name: importlib.metadata.version(name)
        for name in ("sentencex", "loguru", "typing_extensions")
    },
    "source_sha256": {path: value[1] for path, value in sorted(VERIFIED.items())},
    "oracle_adaptations": {
        "package_initialization": "pipecat root initialization bypassed",
        "frames": "Required definitions compiled unchanged from baseline-verified frames.py AST",
        "frame_definitions": list(FRAME_DEFINITIONS),
        "instrumentation": "Observational wrappers record top-level operations and post-call state",
        "frame_identity": (
            "Frame id and progress segment_id normalized by first encounter per source test"
        ),
        "original_tests": (
            "Selected unittest classes execute unchanged with all their original assertions"
        ),
    },
    "traces": TRACES,
}
output.write_text(json.dumps(provenance, ensure_ascii=False, indent=2) + "\n")
print(f"Recorded {len(TRACES)} traces from {result.testsRun} passing original tests: {output}")
