# Source and ABI provenance

Behavioral contract: `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- `src/pipecat/services/moonshine/stt.py`
- `src/pipecat/services/stt_service.py` (SegmentedSTTService)
- `tests/test_moonshine_stt.py`
- `uv.lock`: `moonshine-voice` 0.1.5

The Rust worker/segmentation lifecycle follows this workspace's BSD-2-Clause
`pipecat-whisper` implementation. No Whisper dependency or inference is used.

Native ABI: official Moonshine 0.1.5 Linux x86_64 wheel from PyPI, matching the
original Python dependency lock. Its `moonshine_api.py` is inspected as ABI
reference text only. Header version 30000, transcript size 16, line size 88.
The memory-file entry point matches `core/moonshine-c-api.h`; the official native
binary's digest pins that ABI rather than trusting future header-version reuse.
`src/catalog.json` is the unmodified output of its `moonshine_get_stt_catalog`.

Sources:

- https://pypi.org/project/moonshine-voice/0.1.5/
- https://github.com/moonshine-ai/moonshine/blob/main/core/moonshine-c-api.h
- https://github.com/moonshine-ai/moonshine/blob/main/LICENSE

The source-code MIT notice extracted from the wheel is reproduced in
`licenses/moonshine-MIT.txt`. Moonshine's upstream license separately states all
streaming STT models and English tiny/base are MIT; legacy non-English
non-streaming models are excepted under Community License terms. No model-weight
redistribution rights are implied by this crate's source license. No native
binary, model weight or audio fixture is redistributed in this crate.

`libmoonshine.so` depends on `libonnxruntime-13ab8084.so.1`, with import symbol
`OrtGetApiBase@VERS_1.23.2`; the workspace's separate ORT 1.21.0 exports
`OrtGetApiBase@VERS_1.21.0`. Native libraries use RTLD_LOCAL and immutable memfd
snapshots, with no global loader or environment changes. Runtime dependency
handles and their sealed descriptors remain owned throughout inference.
