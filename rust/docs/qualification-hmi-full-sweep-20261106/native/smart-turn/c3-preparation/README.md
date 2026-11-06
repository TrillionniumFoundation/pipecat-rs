# ORT 1.21.0 wheel provenance and C3 preparation

This directory records preparation, static inspection and independent review. **C3 has not run at this checkpoint.** Its future result must be recorded separately. The earlier C1 and C2 probability failures remain failures against the unchanged tracked reference and `1e-5` tolerance.

## Identified artifacts

The complete [PyPI 1.21.0 metadata](https://pypi.org/pypi/onnxruntime/1.21.0/json) identifies one selected `cp312` Linux x86-64 wheel: 16,019,285 bytes, SHA-256 `9a04aafb802c1e5573ba4552f8babcb5021b041eb4cfa802c9b7644ca3510eca`. Its exact public download URL, metadata bytes, retrieval result and all 337 unique archive names are retained in this directory. The complete local archive passed that public digest before member inspection.

| Artifact | Bytes | SHA-256 | Role |
| --- | ---: | --- | --- |
| Restored C1 `libonnxruntime.so.1.21.0` | 20,558,800 | `3c8181b339025dd18899916357e74290029722e1d5a5778b4071a733f813420d` | Original runtime; retained unchanged |
| Wheel `capi/libonnxruntime.so.1.21.0` | 20,571,088 | `a0df84fae0f09d9eca3d6919f685ba435f1ae6cb628e128b69bcf2a2b965504c` | Separate standalone C API binary selected for C3 |
| Wheel `onnxruntime_pybind11_state.cpython-312-x86_64-linux-gnu.so` | 21,406,896 | `0ca6ae827d5f5b2cc6abf3c0cf286063dee0f6c09947d6d34e2d199335b189f6` | Separate Python extension; C3 does not import or execute it |

The selected native member is an ordinary, unencrypted ZIP entry. Its exact bytes and the unique corresponding wheel `RECORD` digest and size agree. Static ELF inspection identifies x86-64 ELF64, `libonnxruntime.so.1` SONAME, `$ORIGIN` RUNPATH, the expected C API exports and eight system-library dependencies. This inspection reads file data; it does not establish runtime ABI success or execute a model.

Pinned upstream [setup.py](https://github.com/microsoft/onnxruntime/blob/v1.21.0/setup.py) and [onnxruntime_python.cmake](https://github.com/microsoft/onnxruntime/blob/v1.21.0/cmake/onnxruntime_python.cmake) distinguish the standalone shared library from the separately linked Python extension. Their original Git blob identities and bytes were independently checked. The extension's `NEEDED` entries do not name the standalone `libonnxruntime.so`. Therefore the C API member must not be described as the Python oracle's runtime.

The official annotated tag `v1.21.0` resolves to commit `e0b66cad282043d4377cea5269083f17771b6dfc`. C1 reports this commit prefix. That prefix and its compiler hardening options do not prove that C1 is a custom or unofficial build. The historical probability oracle does not retain the exact runtime binary, dependency build or CPU identity; C3 does not recover those missing facts.

## Frozen single comparison

[c3-preparation.json](c3-preparation.json) binds the complete scheme to current native source `0cc16e8123cb05aaad480d5ab7860a267d998bac`, tree `aec037b9cf2be53fcf6f85fdc149382942277702`, and source manifest `1e07f773ca2827e8db5b06175f96137b5d85bb6dcb68229d29601efe29658535`.

The reviewed runner is 36,363 bytes, SHA-256 `7d23169a11c4790a4cb7294932fb43143d6933c08e1f041ef34daee5b7b22e98`; no-ref backup blob `fe3ac2987eb69b0cfda7a6c2d1b5e666ccb2c587` was fetched back and compared in full. It reuses the original 25,576-byte C1 executable, SHA-256 `e20adb1f67f6bd5f42ce30b0ad13a0ae568dcb80d4fcd7b2a0237366892fffbc`, without recompiling it. API 20, sequential execution, one inter-op and intra-op thread, `ORT_ENABLE_ALL`, CPU input memory, model and three frozen feature files remain unchanged. One session performs one `Run` for each of speech, tone and silence. C1's ALL outputs are reused as historical comparison data and are not rerun.

The runner is admitted only after the original current-source Grok, default workspace, expanded, model opt-in, WebRTC and AISHELL stages have authoritative terminal states and intact source/material/containment evidence. Ordinary failures remain recorded and need not become passes before C3. In particular, an AISHELL stage that failed before inference may report zero attempts, provided its explicit terminal failure and integrity/cleanup evidence are complete. Missing or active stages cannot be bypassed.

The same nonblocking exclusive owner lock covers C3 preparation, execution, containment and postchecks. C3 uses fresh create-only request and evidence directories. It downloads the exact public archive once and extracts only the selected C API library into a private runtime directory. It does not install a package, import ONNX Runtime Python, replace the restored runtime, invoke Cargo, alter native environment flags or change the source/model/reference.

The proposed outer job allows 210 seconds, 200% CPU and 4 GiB memory. The helper observes a 180-second whole budget, a maximum 60-second native wait and reserved cleanup time. The 45-second download budget is checked between reads and after completion; the 10-second socket timeout measures inactivity and is not a universal wall-time interrupt. The outer owned job remains the final containment boundary. Cleanup distinguishes requested PGID signals, actual leader reap and non-atomic group absence; it does not claim every descendant was individually joined.

Original native exit, output-protocol validation, unchanged-reference comparison and integrity/cleanup failures remain separate. Three completed calls may be claimed only after the exact eight-record protocol and native release record validate. Each output records float32 bits, delta from the original reference and bitwise equality with retained C1 ALL. Equal or different results concern these two identified standalone C API artifacts at these inputs and settings. Neither result alone identifies a CPU cause, restores the historical Python oracle, qualifies microphone/speaker quality or authorizes a production runtime change.

## Verification scope

Preparation used a public archive download, `RECORD` verification, static ELF reads, Python AST checks and an independent agent's source review. It did not load either runtime library or run a model. Exact input, primary-source and runner hashes are in [c3-preparation.json](c3-preparation.json); the eventual execution must supply its own raw stdout/stderr, terminal runtime receipt and before/after identities.
