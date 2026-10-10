# Native image generation

`pipecat-images` implements the pinned OpenAI image-generation service using Rust
HTTP, PNG/JPEG decoders, and the native pipeline. It does not use Python, Pillow,
provider SDKs, a browser, subprocesses, or fabricated image pixels.

```rust,no_run
use pipecat_images::{ImageGenerationConfig, OpenAiImageClient, CancellationToken};
# async fn example(api_key: String) -> Result<(), Box<dyn std::error::Error>> {
let mut config = ImageGenerationConfig::new(api_key);
// Select a model your endpoint actually supports.
config.settings.model = Some("your-image-model".into());
config.settings.image_size = Some("1024x1024".into());
let client = OpenAiImageClient::new(config)?;
let image = client.generate("a red square", &CancellationToken::new()).await?;
assert_eq!(image.image.format.as_deref(), Some("RGB"));
# Ok(()) }
```

Use `ImageGenProcessor::new(client)` as a native `FrameProcessor` and a shared
`OutputTransport` with an `OutputBackend::write_image` implementation. Generated
`FrameData::UrlImage` contains decoded RGB bytes, actual dimensions, and the
provider URL if present. The shared output worker dispatches this frame to the
backend and charges pixels, URL, format, source/destination, metadata, and spare
allocation capacity against its media queue budget. Configure the output byte
budget for your admitted image sizes; its audio-oriented default is only 2 MiB.

## Wire and settings contract

* POST `{base_url}/images/generations`, with Bearer authentication, `prompt`,
  `model`, `n: 1`, and `size`; never `response_format`
* Defaults are exactly source-compatible: model `gpt-image-2.5-flare`, size null.
  The model string is a baseline identifier, not a hosted availability guarantee.
  The baseline docstring's claimed 1024x1024 default differs from its actual code
* Explicit model/size strings, including unfamiliar future sizes, are preserved;
  null and not-given settings are distinct. Strings have bounded lengths
* `ImageGenerationSettingsDelta` and targeted `ServiceUpdateSettings` updates
  validate atomically and affect future admissions. Healthy updates do not cancel
  already accepted images; changed settings can restore an unusable processor
* The first response image is used. A nonempty URL takes precedence over base64,
  even when fetching that URL fails. No fallback can conceal a fetch-policy error
* Empty data/missing image fields, malformed JSON/base64, errors, or unsupported
  images return explicit errors. No retry occurs, including reqwest protocol retries

## Hosted-image security

Hosted results are denied by default. Configure `HostedImagePolicy::https_origins`
with exact trusted HTTPS origins, e.g. `https://your-image-storage.example`.
The complete host, scheme, and effective port must match; suffix/wildcard matching
is not used. Signed query parameters may be retained, but errors and Debug output
never include image URLs, credentials, prompts, provider bodies, or decoded pixels.

Each download resolves once, validates every returned address, then pins those
addresses into a fresh credential-free reqwest client and verifies the connected
peer address. It disables proxies, redirects, cookies, and automatic retries.
Public-unicast policy rejects loopback, private, link-local, shared/CGNAT,
metadata, reserved/documentation ranges, and IPv6 mapping/translation/transition
addresses. TLS certificate and hostname verification remain enabled.

`HostedImagePolicy::loopback_fixtures` explicitly permits only exact literal
loopback origins (HTTP or HTTPS). It never permits `localhost` DNS aliases or
private LAN addresses. `allow_loopback_api` is a separate opt-in for a literal
loopback HTTP generation endpoint. API base URLs otherwise require HTTPS and
cannot contain credentials, fragments, or queries. The configured API endpoint
is a trusted caller-provided destination, not an untrusted response URL.

## Resources and ownership

The client bounds prompts, JSON responses (both advertised and streamed lengths),
encoded images, image count, dimensions, pixels, decoder scratch, request count,
and one total network/decode deadline. HTTP content encodings other than identity
are rejected, so response decompression cannot bypass the encoded-byte bound.
Base64 size is checked before its output buffer is allocated. JSON's standard
recursion bound is retained; only the first of at most 16 image entries is stored.

One owned native decoder thread serves each shared client. The pinned vendored
JPEG dispatcher and final color conversion run synchronously inside that thread;
upstream's detached component-thread and feature-dependent Rayon paths are not
compiled. Disabling upstream Rayon alone would not establish this contract. Its bounded queue and
request permits include active decoding. Dropping/cancelling a request signals
its decoder, but does not prematurely release that job's permit. Decoder reads,
PNG rows converted to RGB, and JPEG preflight loops check cancellation/deadline.
The last client owner closes the decoder lane and joins the thread; there are no
detached `spawn_blocking` jobs. A codec CPU operation already in progress cannot
be forcibly preempted; shutdown may wait for its bounded operation to reach a
check. The pixel/scan limits bound that residual work, not a hard real-time latency.

PNG supports static grayscale, grayscale-alpha, RGB, RGBA, indexed expansion,
8/16-bit input (16-bit is reduced to 8-bit), and the decoder's interlaced paths.
Alpha is discarded, matching source RGB conversion; no background is composited.
Dimensions are checked before decoder creation. Text and ICC chunks are ignored
without decompression; the native PNG decoder also has a scratch limit. APNG is
explicitly rejected. There is a 65,536-chunk ceiling and strict complete IEND.

JPEG supports 8-bit Huffman sequential/progressive DCT, grayscale, RGB, and CMYK,
with component sampling factors 1–2, at most 64 scans, and 4,096 markers. The entire
marker/entropy framing is checked before decoder allocation. Scratch admission
conservatively reserves 64 bytes per pixel plus 64 KiB (PNG uses 16 plus 64 KiB),
in addition to bounded encoded input/HTTP buffers. The decoder's own output bound
is enabled. The upstream platform_independent feature also forbids architecture-specific
unsafe implementations. Lossless/arithmetic/hierarchical/exotic sampling and truncated/trailing
JPEG variants are rejected. Decoder limits are not a process-wide RSS guarantee;
encoded buffers, task/HTTP state, metadata, and application-owned outputs are
separate bounded resources. Codec details and licenses are in `DEPENDENCIES.md`.

The processor admits a bounded count of tracked jobs. Every ordinary frame is
byte-counted using a bounded no-allocation serialization sink before retention;
nonserializable local executable payloads fail explicitly. Tasks use ordered
completion dependencies, so passthrough text precedes its image, and later normal
frames cannot overtake it. Text subclasses follow baseline inheritance, including
system-priority InputText but excluding LLMThoughtText. Generated output preserves
transport source/destination and records `image_generation.source_frame_id`.
System controls remain responsive; accepted settings snapshots are immutable.
End and Stop drain accepted tasks under the pipeline's shutdown deadline; Cancel
and interruptible-frame interruption abort them. Health changes fence old results
and prevent replay. Provider permanence disables health; malformed individual
images remain recoverable. Optional Processing metrics measure the actual work.

## Scope and acceptance

See `SOURCE_MAP.md` for pinned source/test hashes and native acceptance coverage.
Loopback tests demonstrate exact wire requests, native decoding, cancellation,
bounded errors, and real pipeline-to-output-backend delivery. They do not certify
live model availability, image quality, camera/video encoding, a physical display,
provider billing, or public production-network access.

Gemini image generation, Azure's operation-location polling API, and Fal are
explicitly unimplemented sibling adapters. They are not generic OpenAI aliases.

Source-tree distribution retains the patched JPEG path dependency. Crates.io
publishing of `pipecat-images` is disabled until a packaging route can preserve
its synchronous codec contract; silently substituting upstream is not qualified.
