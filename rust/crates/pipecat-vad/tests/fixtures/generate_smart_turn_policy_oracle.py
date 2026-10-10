"""Generate deterministic policy traces from the actual tracked Python classes.

Only generation requires Python, NumPy and Pydantic. Imports are replaced with
an explicit namespace so provider/plugin dependencies are not needed. Class
bodies and their policy methods are compiled unchanged from the source AST.
"""
import ast
import asyncio
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
from pydantic import BaseModel

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[4]
BASE = ROOT / "src/pipecat/audio/turn/base_turn_analyzer.py"
SMART = ROOT / "src/pipecat/audio/turn/smart_turn/base_smart_turn.py"
METRICS = ROOT / "src/pipecat/metrics/metrics.py"
clock = SimpleNamespace(value=100.0, ticks=0)
def perf_counter():
    clock.ticks += 1
    return clock.ticks * 0.125
namespace = dict(ABC=ABC, abstractmethod=abstractmethod, Enum=Enum, BaseModel=BaseModel,
                 asyncio=asyncio, np=np, Any=Any, ThreadPoolExecutor=ThreadPoolExecutor,
                 time=SimpleNamespace(monotonic=lambda: clock.value, perf_counter=perf_counter),
                 logger=SimpleNamespace(debug=lambda *a: None, trace=lambda *a: None))

def source_classes(path, names):
    tree = ast.parse(path.read_text())
    nodes = [node for node in tree.body if isinstance(node, ast.ClassDef) and node.name in names]
    assert len(nodes) == len(names)
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), "exec"), namespace)

source_classes(METRICS, {"MetricsData", "TurnMetricsData"})
source_classes(BASE, {"BaseTurnParams", "BaseTurnAnalyzer", "EndOfTurnState"})
namespace.update(STOP_SECS=3, PRE_SPEECH_MS=500, MAX_DURATION_SECONDS=8)
source_classes(SMART, {"SmartTurnParams", "SmartTurnTimeoutException", "BaseSmartTurn"})

class Oracle(namespace["BaseSmartTurn"]):
    def _predict_endpoint(self, audio):
        self.captured = np.rint(audio * 32768).astype(np.int16).tolist()
        if self.outcome == "timeout":
            raise namespace["SmartTurnTimeoutException"]()
        return dict(prediction=int(self.outcome > 0.5), probability=self.outcome)

def append(at, sample, count, speech=False):
    return dict(op="append", at=at, sample=sample, count=count, speech=speech)
def analyze(probability):
    return dict(op="analyze", probability=probability)
def run_length(values):
    runs=[]
    for value in values:
        if runs and runs[-1][0] == value: runs[-1][1] += 1
        else: runs.append([value,1])
    return runs

CASES = [
    dict(name="pre_roll_vad_delay_and_reanalysis", rate=16000,
         params=dict(stop_secs=.3, pre_speech_ms=50, max_duration_secs=.2),
         events=[append(100,1,160),append(100.1,2,160),append(100.2,3,160),
                 dict(op="vad_start", seconds=.1),append(100.3,4,160,True),
                 append(100.31,5,160),analyze(.2),analyze(.8),analyze(.2)]),
    dict(name="maximum_duration_is_tail_not_completion", rate=16000,
         params=dict(stop_secs=.3, pre_speech_ms=500, max_duration_secs=.005),
         events=[append(100,1,40,True),append(100.01,2,60,True),
                 analyze(.5),append(100.02,3,10),analyze(.1),
                 append(100.03,4,120,True),analyze(.9)]),
    dict(name="sample_timed_silence_resume_and_forced_clear", rate=16000,
         params=dict(stop_secs=.025, pre_speech_ms=50, max_duration_secs=.2),
         events=[append(100,1,160,True),append(110,2,160),append(120,3,80),
                 append(121,4,160,True),append(122,5,160),append(122.01,6,160),
                 append(122.02,7,80),analyze(.8),append(123,8,160),
                 append(124,9,160,True),dict(op="clear"),append(130,10,800),analyze(.2)]),
    dict(name="pre_speech_time_eviction", rate=1000,
         params=dict(stop_secs=.2, pre_speech_ms=100, max_duration_secs=.1),
         events=[append(100,1,10),append(100.4,2,10),analyze(.1),
                 append(100.401,3,10),analyze(.1),append(101,4,10),analyze(.1)]),
    dict(name="empty_chunks_and_predictor_timeout", rate=16000,
         params=dict(stop_secs=.3, pre_speech_ms=0, max_duration_secs=.2),
         events=[analyze(.1),append(100,1,0,True),analyze(.8),
                 append(100.01,2,10,True),analyze("timeout"),analyze(.8)]),
]

async def main():
    traces=[]
    for case in CASES:
        oracle=Oracle(sample_rate=case["rate"], params=namespace["SmartTurnParams"](**case["params"]))
        oracle.set_sample_rate(48000)  # initial rate overrides the setup rate
        steps=[]
        for event in case["events"]:
            state=None
            metrics=None
            captured=None
            if event["op"] == "append":
                clock.value=event["at"]
                pcm=np.full(event["count"],event["sample"],dtype="<i2")
                state=oracle.append_audio(pcm.tobytes(),event["speech"]).name
            elif event["op"] == "vad_start":
                oracle.update_vad_start_secs(event["seconds"])
            elif event["op"] == "clear":
                oracle.clear()
            else:
                oracle.outcome=event["probability"]
                oracle.captured=None
                state,result=await oracle.analyze_end_of_turn()
                state=state.name
                captured=None if oracle.captured is None else run_length(oracle.captured)
                metrics=None if result is None else result.model_dump(exclude_none=True)
            steps.append(dict(event=event, state=state, speech_triggered=oracle.speech_triggered,
                              silence_ms=oracle._silence_ms, speech_start=oracle._speech_start_time,
                              model_input=captured, metrics=metrics))
        await oracle.cleanup()
        traces.append(dict(name=case["name"], rate=case["rate"], params=case["params"], steps=steps))
    data=dict(sources={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
                       for p in [BASE,SMART,METRICS]}, numpy=np.__version__, cases=traces)
    (HERE / "smart_turn_policy_oracle.json").write_text(json.dumps(data,indent=2)+"\n")

asyncio.run(main())
