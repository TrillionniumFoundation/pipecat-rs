# Native image source map

Baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
All source/test files below are byte-identical to this baseline. This map covers
OpenAI Images, Gemini generate-content images, Azure REST images, and their shared output path.

| Pinned source | SHA-256 | Native implementation / tests |
| --- | --- | --- |
| `src/pipecat/services/openai/image.py` | `0dbade7698ac75defa4eeb7eaa8ecaef1c37c7a22676faf728161614ade76e4b` | `settings.rs`, `client.rs`, `decode.rs`; exact route/auth/body/default, source size null, URL precedence, native RGB conversion |
| `src/pipecat/services/image_service.py` | `267504b4bef1f32fdf8b9d389c0c7b50a0a35940c70d6ad1bf5a37f049b379ab` | `processor.rs`; ordered text passthrough, actual image output, processing metrics, bounded tracked work |
| `tests/test_openai_image_gen.py` | `9336114ec562823d774994b8418d84ed95669fc75bff4698c3e7c08b11d28d52` | `tests/protocol.rs`: inline RGB, RGBA to RGB, hosted image, missing image, exact default/no response_format |
| `examples/assets/cat.jpg` | `b6ef883a01b92b768692cff3d9369d9b6d7283c5552335c53eb851471dbc2023` | Byte-identical `tests/fixtures/source-cat.jpg`; native JPEG decode and adversarial scan fixture source |

Additional acceptance cases in `tests/protocol.rs`:

* Advertised/chunked encoded-body limits; base64 allocation bound; pixel limits,
  huge dimensions, malformed/truncated PNG, unsupported format, JPEG native decoding
* Compressed PNG text ignored under a small scratch budget; JPEG scan bomb rejected
* Explicit trusted origins, private and metadata hosts denied even when named in
  the trusted-origin list, literal loopback fixture isolation, URL credentials and
  fragments denied, no authorization/cookies on hosted fetch, no API/download redirects
* Request deadline, cancellation, dropped request closes its hosted connection,
  direct-client and processor queue backpressure
* Real pipeline → shared OutputTransport → OutputBackend RGB pixels, dimensions,
  generated URL carrier, passthrough/image ordering, source frame identity, End/Stop
* Future-only atomic settings changes, no partial update, source Text inheritance,
  interruption, Cancel, health false/true fencing, later-epoch recovery
* Shared output rejects a decoded image larger than its byte budget

`media-transport/src/queue.rs::image_queue_tests` also checks URL/pixel/metadata
admission, spare allocation capacities, deep metadata rejection, interruption,
and capacity release. Existing media output lifecycle tests remain applicable
because URL images use the existing cancellable `write_image` operation path.

## Deliberate safety tightening and boundaries

* Network/codec limits and exact hosted-origin allowlists are explicit native
  configuration. The baseline aiohttp fetch followed arbitrary provider URLs
  without these SSRF/redirect/DNS-pinning protections
* Native JPEG/PNG support is narrower than all Pillow codecs and variants;
  unsupported forms fail explicitly as documented in README
* JSON has bounded bytes/depth and at most 16 response entries; strict base64,
  complete PNG IEND, and complete JPEG EOI reject malformed/trailing content
* Source model/size accept null or bounded strings. Unknown settings, invalid
  setting types, or oversized settings fail borrowed validation before cloning or any atomic update
* Ordinary pipeline frames must be serializable for bounded queue admission;
  local executable payloads are rejected rather than given unbounded storage
* Processing moves into pipeline-owned tasks, while ordinary frame order and
  original passthrough identities are retained. No background work is detached
* Generated images are new frames; original transport routing and the source
  frame ID are carried explicitly. Provider URL metadata stays in UrlImage
* PNG high-bit-depth samples are reduced to 8-bit; color profiles are ignored.
  RGB conversion is not a color-management or image-quality equivalence claim

Azure polling, Fal, live hosted-provider acceptance, physical rendering,
and encoding/camera/video adapters are separate work.

JPEG ownership additionally depends on the exact vendored synchronous-dispatch
patch documented in `rust/vendor/jpeg-decoder-0.3.2/PATCHES.md`. Upstream's
Rayon-disabled implementation alone does not satisfy the owned-work contract.

## Gemini native protocol slice

Pinned Python files are inspected as static references only:

| Pinned source | SHA-256 | Native implementation |
| --- | --- | --- |
| `src/pipecat/services/google/image.py` | `fb44eb4683700caa39e2b4a53fe257e5a5080f7b3442e1d69ed0e695adceb3a8` | `gemini/settings.rs`: defaults/ignored negative prompt; `gemini/client.rs`: sequential generate-content; `gemini/wire.rs`: first inline part; shared `decode.rs`: native RGB |
| `tests/test_google_image_gen.py` | `b4419ea6459267ce64c076651b969cd90360740d13b6ad10419da79c67c63db3` | `tests/gemini.rs`: RGB/alpha, count/config, no image, API error, negative-prompt omission |
| `src/pipecat/services/image_service.py` | `267504b4bef1f32fdf8b9d389c0c7b50a0a35940c70d6ad1bf5a37f049b379ab` | `gemini/processor.rs`: ordered source text, incremental images, processing metric and lifecycle |

Primary REST references inspected 2026-10-10:

- [GenerateContent API](https://ai.google.dev/api/generate-content?hl=en):
  nonstreaming endpoint, `contents`, `generationConfig`, `responseModalities`,
  `imageConfig.aspectRatio`, candidate content and inline Blob MIME/base64
- [Google image-generation guide](https://ai.google.dev/gemini-api/docs/generate-content/image-generation?hl=en):
  REST credential header and IMAGE-only response configuration

The wire is frozen to those documented fields and the pinned source behavior.
SDK automatic function calling is local orchestration, not a REST property;
no tool declaration or automatic execution is implemented. Current documentation
may advertise different models or additional request formats; no such capability
or model migration is inferred from it.

Native loopback cases in `tests/gemini.rs` cover exact route/header/JSON, ordered
candidate/part selection, real PNG/JPEG RGB, sequential lazy pulls, immutable
multi-image snapshots, partial delivery, safety/no-image, strict MIME/base64,
HTTP errors, candidate/part/body/codec/aggregate limits, dropped-future ambiguity,
no retries/redirects, cancellation and deadlines, processor backpressure,
settings frames, health fencing, End/Stop/Cancel/drop, text inheritance, and
actual shared OutputBackend RGB delivery.

Intentional safety tightening: count 1–8; bounded model segment and enumerated
aspect ratios; MIME/signature agreement; response-wide safety/recitation veto
even when another candidate has valid bytes (both candidate orders are tested),
which differs from source first-inline selection; no arbitrary Pillow codecs; whole-prompt and
per-image deadlines; explicit aggregate byte caps. `negative_prompt` is ignored
without replicating Python's deprecation-warning machinery. Processing metrics
are emitted; source SDK TTFB instrumentation is not separately modeled. A lazy
pull API replaces the Python async generator; a cancelled pull cannot restart a
billable request. No live provider call, original Python execution, hosted model
availability/quality/billing, physical rendering or portable packaging is
qualified by these local tests. Offline gate evidence belongs to the source-hashed
qualification report for the exact candidate, not to this behavior map.

Independent cases in `tests/gemini_review/mod.rs` additionally exercise exact
aggregate-budget exhaustion before another billable HTTP request and admission
release after a fused HTTP failure while the failed sequence remains alive.

## Azure submit/poll/download

Pinned `src/pipecat/services/azure/image.py` has Git blob
`593089d7c97165f611dbee1570df5c7a2c7f052f` and SHA-256
`e6e176eb101467ce31dc5b4a34648f0629c39876ebede8f8cab1b15c17b8f420`.
`azure.rs` implements its request defaults, first-result selection and 119-poll
state machine. `processor.rs` reuses the existing ordered image path under a
separate backend/name. `tests/azure_http.rs` and `tests/azure_pipeline.rs` cover
local wire, safety, actual RGB and native output delivery.

See [AZURE.md](AZURE.md) for deliberate same-origin credential policy, strict
HTTP/field handling, whole-operation cancellation/deadline and RGB normalization.
The source returns original Pillow modes; this adapter does not claim mode
preservation. Its historical API version is a fixed reference, not a current
Azure availability guarantee. No Python runtime, provider SDK or live service
is used by the native implementation or these deterministic tests.
