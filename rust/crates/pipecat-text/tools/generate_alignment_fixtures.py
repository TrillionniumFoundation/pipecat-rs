"""Developer-only oracle recorder; never called by Cargo or the Rust runtime.

Run with the existing source-validation Python environment (loguru installed).
Only package initialization is bypassed. Production modules and original tests
are unchanged, and their bytes are checked against the pinned baseline first.
"""
import dataclasses
import hashlib
import importlib.metadata
import inspect
import json
from pathlib import Path
import subprocess
import sys
import types
import unittest

ROOT = Path(__file__).resolve().parents[4]
BASELINE = "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
MODULES = ["test_text_segment_map", "test_word_completion_tracker"]
FILES = ["src/pipecat/utils/context/text_segment_map.py", "src/pipecat/utils/context/word_completion_tracker.py", "src/pipecat/utils/text/alnum_utils.py", "src/pipecat/utils/text/markup_utils.py"] + [f"tests/{m}.py" for m in MODULES]
for path in FILES:
    baseline = subprocess.check_output(["git", "show", f"{BASELINE}:{path}"], cwd=ROOT)
    if baseline != (ROOT / path).read_bytes():
        raise SystemExit(f"Source differs from baseline: {path}")
pkg = types.ModuleType("pipecat")
pkg.__path__ = [str(ROOT / "src/pipecat")]
sys.modules["pipecat"] = pkg
sys.path.insert(0, str(ROOT / "tests"))
from pipecat.utils.context.text_segment_map import TextSegmentMap
from pipecat.utils.context.word_completion_tracker import WordCompletionTracker
from loguru import logger
logger.remove()
traces = []
current_test = ""
depth = 0

def map_state(obj):
    return {k: getattr(obj,k) for k in ["raw_pos", "user_facing_pos", "llm_pos", "llm_spoken_pos", "last_overflow", "last_leading_duplicate", "is_complete", "in_transformed_segment"]} | {"last_completed_segment": dataclasses.asdict(obj.last_completed_segment) if obj.last_completed_segment else None}

def tracker_state(obj):
    return {"is_complete":obj.is_complete, "word":obj.get_word_for_frame(), "overflow":obj.get_overflow_word(), "llm_consumed":obj.get_llm_consumed(), "suppress":obj.suppress_in_context(), "accumulated_user":obj.get_accumulated_user_facing_text(), "remaining_user":obj.get_remaining_user_facing_text(strip=False), "accumulated_tts":obj.get_accumulated_tts_text(), "remaining_tts":obj.get_remaining_tts_text(strip=False), "accumulated_llm":obj.get_accumulated_llm_text(), "remaining_llm":obj.get_remaining_llm_text()}

for cls in [TextSegmentMap, WordCompletionTracker]:
    original_init=cls.__init__
    def init(self,*args,_original=original_init,_cls=cls,**kwargs):
        global depth
        outer=depth==0
        depth+=1
        try: _original(self,*args,**kwargs)
        finally: depth-=1
        if not outer: return
        bound=inspect.signature(_original).bind(self,*args,**kwargs)
        bound.apply_defaults()
        config={k:v for k,v in bound.arguments.items() if k!="self"}
        self.__trace={"test":current_test,"class":_cls.__name__,"config":config,"operations":[]}
        if _cls is TextSegmentMap: self.__trace["segments"]=[dataclasses.asdict(s) for s in self._segments]
        traces.append(self.__trace)
    cls.__init__=init
    methods = ["advance_word", "reset", "word_belongs_current_segment"] if cls is TextSegmentMap else ["add_word_and_check_complete", "reset", "word_belongs_here", "take_remaining_as_spoken"]
    for method in methods:
        original=getattr(cls,method)
        def wrapped(self,*args,_original=original,_method=method,_cls=cls,**kwargs):
            global depth
            outer=depth==0
            depth+=1
            try: result=_original(self,*args,**kwargs)
            finally: depth-=1
            if outer:
                state=map_state(self) if _cls is TextSegmentMap else tracker_state(self)
                self.__trace["operations"].append({"method":_method,"args":args,"kwargs":kwargs,"result":result,"state":state})
            return result
        setattr(cls,method,wrapped)

class Result(unittest.TextTestResult):
    def startTest(self,test):
        global current_test
        current_test=test.id()
        super().startTest(test)

suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromName(m) for m in MODULES)
result=unittest.TextTestRunner(resultclass=Result).run(suite)
if not result.wasSuccessful(): raise SystemExit("Source tests failed; refusing fixture output")
output=ROOT / "rust/crates/pipecat-text/tests/fixtures/alignment_source_traces.json"
output.write_text(json.dumps({"baseline":BASELINE,"source_tests_run":result.testsRun,"python_version":sys.version,"dependencies":{"loguru":importlib.metadata.version("loguru")},"source_sha256":{p:hashlib.sha256((ROOT/p).read_bytes()).hexdigest() for p in FILES},"traces":traces},ensure_ascii=False,indent=2)+"\n")
print(f"Recorded {len(traces)} traces from {result.testsRun} passing original tests")
