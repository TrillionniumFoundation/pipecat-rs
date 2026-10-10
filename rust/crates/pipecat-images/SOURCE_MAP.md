# Native image source map

Baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
All source/test files below are byte-identical to this baseline. This map covers
only OpenAI Images and its shared output path; it does not certify other image providers.

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

Gemini, Azure polling, Fal, live hosted-provider acceptance, physical rendering,
and encoding/camera/video adapters are separate work.

JPEG ownership additionally depends on the exact vendored synchronous-dispatch
patch documented in `rust/vendor/jpeg-decoder-0.3.2/PATCHES.md`. Upstream's
Rayon-disabled implementation alone does not satisfy the owned-work contract.
