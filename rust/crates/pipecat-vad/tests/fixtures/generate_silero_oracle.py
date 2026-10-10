"""Regenerate development-only model/loudness oracle vectors; not a runtime dependency.

Requires numpy, onnxruntime==1.21.0, loudness==0.2.0. Run from any directory.
The actual SileroOnnxModel class is extracted from the tracked Python source.
"""
import ast
import hashlib
import json
from pathlib import Path
from typing import cast
import numpy as np
import onnxruntime
import loudness

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[4]
SOURCE = ROOT / "src/pipecat/audio/vad/silero.py"
MODEL = ROOT / "src/pipecat/audio/vad/data/silero_vad.onnx"
module = ast.parse(SOURCE.read_text())
model_class = next(node for node in module.body if isinstance(node, ast.ClassDef) and node.name == "SileroOnnxModel")
namespace = {"np": np, "onnxruntime": onnxruntime, "cast": cast}
exec(compile(ast.Module(body=[model_class], type_ignores=[]), str(SOURCE), "exec"), namespace)
Model = namespace["SileroOnnxModel"]
model = Model(str(MODEL))
speech = np.frombuffer((HERE / "smart_turn_speech.pcm16").read_bytes(), dtype="<i2")
tone = np.frombuffer((HERE / "smart_turn_tone.pcm16").read_bytes(), dtype="<i2")
results = {"model_sha256": hashlib.sha256(MODEL.read_bytes()).hexdigest(), "source_sha256": hashlib.sha256(SOURCE.read_bytes()).hexdigest(), "onnxruntime": onnxruntime.__version__, "cases": {}}
for name, samples, rate in [("speech16",speech,16000),("tone16",tone,16000),("silence16",np.zeros(512*8,dtype=np.int16),16000),("silence8",np.zeros(256*8,dtype=np.int16),8000),("speech8",speech[::2],8000)]:
    # 8k fixture is exact decimation for model API coverage, not a resampling implementation.
    model.reset_states()
    size = 512 if rate == 16000 else 256
    values = [float(model(samples[i:i+size].astype(np.float32)/32768.0,rate).ravel()[0]) for i in range(0,len(samples)-size+1,size)]
    results['cases'][name] = values
volume=[]
for rate in [8000,16000]:
    for amp in [0,1,30,50,300,3000,12000,30000]:
        samples = (np.sin(2*np.pi*np.arange(rate*2//5)*440/rate)*amp).astype(np.int16)
        lufs = loudness.integrated_loudness(samples.astype(np.float32)/32768.0, rate)
        volume.append({"rate":rate,"amplitude":amp,"volume":float(np.clip((lufs+110)/100,0,1))})
results['volume']=volume
(HERE / 'silero_oracle.json').write_text(json.dumps(results,indent=2)+'\n')
print('wrote',HERE/'silero_oracle.json')
