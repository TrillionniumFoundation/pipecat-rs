"""Regenerate native Smart Turn test vectors from the repository's Python oracle.

Run with NumPy and onnxruntime 1.21.0 installed. These packages are needed only
for fixture generation, never for the Rust implementation or its tests.
"""

import importlib.util
import hashlib
import json
from pathlib import Path
import sys
import wave

import numpy as np
import onnxruntime as ort

HERE = Path(__file__).resolve().parent
ROOT = Path(__file__).resolve().parents[5]
sys.dont_write_bytecode = True
source = ROOT / "src/pipecat/audio/turn/smart_turn/_whisper_features.py"
spec = importlib.util.spec_from_file_location("whisper_features_oracle", source)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

options = ort.SessionOptions()
options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
options.inter_op_num_threads = 1
options.intra_op_num_threads = 1
options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
model = ROOT / "src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx"
session = ort.InferenceSession(str(model), sess_options=options, providers=["CPUExecutionProvider"])

with wave.open(str(ROOT / "scripts/provider-watch/assets/speech-16k.wav")) as wav:
    assert (wav.getnchannels(), wav.getframerate(), wav.getsampwidth()) == (1, 16000, 2)
    speech = np.frombuffer(wav.readframes(wav.getnframes()), dtype="<i2")
cases = {
    "speech": speech,
    "tone": (np.sin(2 * np.pi * 440 * np.arange(16000) / 16000) * 16384).astype("<i2"),
    "silence": np.zeros(16000, dtype="<i2"),
}
for name, pcm in cases.items():
    if name != "silence":
        pcm.tofile(HERE / f"smart_turn_{name}.pcm16")
    audio = pcm.astype(np.float32) / 32768.0
    audio = audio[-128000:]
    audio = np.pad(audio, (128000 - len(audio), 0))
    features = module.compute_whisper_log_mel_features(audio)
    features.astype("<f4").tofile(HERE / f"smart_turn_{name}.f32")
    probability = session.run(None, {"input_features": features[np.newaxis]})[0][0].item()
    (HERE / f"smart_turn_{name}.probability.txt").write_text(f"{probability:.17g}\n")
    print(name, "samples", len(pcm), "features", features.shape, "probability", probability)

metadata = {
    "source": str(source.relative_to(ROOT)),
    "model": str(model.relative_to(ROOT)),
    "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
    "model_sha256": hashlib.sha256(model.read_bytes()).hexdigest(),
    "speech_sha256": hashlib.sha256((ROOT / "scripts/provider-watch/assets/speech-16k.wav").read_bytes()).hexdigest(),
    "numpy": np.__version__,
    "onnxruntime": ort.__version__,
    "features": "little-endian float32; row-major 80 x 800; front-pad/take final 128000 samples; normalize=True",
    "pcm": "little-endian signed int16; mono 16000 Hz",
}
(HERE / "smart_turn_oracle.json").write_text(json.dumps(metadata, indent=2) + "\n")
