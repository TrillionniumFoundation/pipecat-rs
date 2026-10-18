# Native image generation

`pipecat-images` implements the pinned OpenAI, Gemini and Azure REST image-generation services
using Rust HTTP, PNG/JPEG decoders, and the native pipeline. It does not use Python, Pillow,
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

## Gemini generate-content images

```rust,no_run
use pipecat_images::{GeminiImageConfig, GeminiImageClient, CancellationToken};
# async fn example(api_key: String) -> Result<(), Box<dyn std::error::Error>> {
let mut config = GeminiImageConfig::new(api_key);
config.settings.number_of_images = 3;
let client = GeminiImageClient::new(config)?;
let mut generation = client.generate("three variations of a red square", &CancellationToken::new())?;
while let Some(image) = generation.next_image().await? {
    assert_eq!(image.url, None);
    // Deliver each image here before requesting the next one.
}
# Ok(()) }
```

`GeminiImageGenProcessor::new(client)` provides the corresponding native pipeline
route, named `GoogleImageGenService`. It shares the owned PNG/JPEG decoder, RGB
carrier, text inheritance and output-backend path with OpenAI. The OpenAI public
client/settings/processor API is unchanged.

The REST request is POST `{base_url}/models/{model}:generateContent` with an
`x-goog-api-key` header and one user-text content. `generationConfig` contains
`responseModalities: ["IMAGE"]` and `imageConfig.aspectRatio`; null aspect omits
that property. No `tools`, `candidateCount`, SDK `automatic_function_calling`,
negative prompt, streaming flag or retry is sent. The source default model is
`gemini-3.1-flash-image`, count 1, aspect `1:1`. Model availability is unqualified.
`negative_prompt` is retained as a bounded, ignored compatibility setting.

Count is 1–8 sequential, independent nonstreaming requests. A sequence admits one
client slot and makes no HTTP request until `next_image` is polled. No next
request starts until the caller asks after the preceding image has returned.
The first nonempty `inlineData.data` across ordered candidates/parts is selected.
Only exact `image/png` and `image/jpeg` MIME types with matching signatures are
accepted; native codec restrictions above apply. A recognized safety/recitation
block on any candidate vetoes the entire response, even when an earlier or later
candidate has valid pixels; prompt-level blocks also veto the response. This is a
conservative difference from the source's unconditional first-inline selection.
Safety-blocked responses, no image, malformed JSON/base64, unsupported MIME, HTTP failures and resource limits
are explicit errors. Response text, function calls, file URLs, and executable
code are never executed or fetched. No hosted-image URL exists in Gemini output.

A sequence returns each successful RGB image immediately. The first error is
returned once; all later calls return `None`, preserving already delivered images.
Dropping a pending `next_image` future cancels its owned decode and poisons that
sequence, since submission may already have incurred billing. Polling it again
returns `Cancelled` once, never a replay. Cancelling, dropping, or timing out
cannot undo a remote generation or prove that the provider stopped or did not bill.

`GeminiImageLimits.image` supplies the same per-image limits documented above.
Additional defaults bound a prompt to 8 images, 16 candidates and 128 total parts
per response, 64 MiB cumulative JSON, 32 MiB cumulative encoded image bytes,
128 MiB cumulative RGB output, and 600 seconds total lifetime. Configurable hard
ceilings are 8/16/128, 512 MiB per aggregate byte budget, and 3600 seconds. Each
HTTP/decode also has its per-image deadline. Time between pulls counts against the
whole prompt deadline; an idle sequence owns no task, but retains its bounded
prompt/settings and admission slot until next polled or dropped. JSON parsing is
byte/depth bounded and retains only the chosen inline image. Remaining output
budget becomes a pixel cap before decoder allocation. These budgets bound work
for one prompt, not process-wide RSS or application retention of returned images.

Gemini settings deltas validate atomically and apply to future processor
admissions, including all requests of one immutable multi-image snapshot. Null
model/count are rejected; null aspect and negative prompt are accepted. Model
identifiers must be a single bounded ASCII resource segment; supported aspect
ratios are enumerated in `gemini/settings.rs`. Unknown keys and invalid values
are rejected. End/Stop drain; interruption/Cancel/drop cancel tracked work. Health
changes fence both in-flight requests and blocked image delivery. Successful
images already sent downstream are not retracted when a later image fails.

## Scope and acceptance

See `SOURCE_MAP.md` for pinned source/test hashes and native acceptance coverage.
Loopback tests demonstrate exact wire requests, native decoding, cancellation,
bounded errors, and real pipeline-to-output-backend delivery. They do not certify
live model availability, image quality, camera/video encoding, a physical display,
provider billing, or public production-network access.

Azure's operation-location polling API is a distinct adapter: use
`AzureImageConfig::new(key, endpoint)`, `AzureImageClient`, and
`ImageGenProcessor::azure(client)`. See [AZURE.md](AZURE.md) for the bounded
submit/poll/download contract and RGB-normalized differences. Fal remains an
unimplemented sibling adapter; neither service is a generic OpenAI alias.

Source-tree distribution retains the patched JPEG path dependency. Crates.io
publishing of `pipecat-images` is disabled until a packaging route can preserve
its synchronous codec contract; silently substituting upstream is not qualified.

## Explicit offline RGB owner

`OfflineRgbDecoder` exposes the existing bounded PNG/JPEG decoder without a
provider client, credentials, HTTP, or a new codec. It starts exactly one native
worker only when explicitly constructed. Existing provider constructors, public
settings, and decoding behavior remain unchanged. Merely constructing
`OfflineRgbDecodeLimits`, or using its synchronous preflight methods, starts no
thread.

```rust,no_run
use pipecat_images::{OfflineRgbDecodeLimits, OfflineRgbDecoder};
# async fn example(encoded_png: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
let limits = OfflineRgbDecodeLimits::default();
let dimensions = limits.inspect_encoded_image(&encoded_png, "image/png")?;
let decoder = OfflineRgbDecoder::new(limits)?;
let job = decoder.submit(encoded_png)?;
let rgb = job.await?;
assert_eq!(rgb.size, dimensions);
assert_eq!(rgb.format.as_deref(), Some("RGB"));
decoder.shutdown().await?;
assert!(decoder.status().joined);
# Ok(()) }
```

The opt-in defaults are also hard ceilings: 64 KiB encoded length **and Vec
capacity**, 1,024 per dimension, 262,144 pixels / 786,432 RGB bytes, 32 MiB
conservative codec scratch, two total native jobs, one pending lane slot, and a
five-second decode deadline including queue wait. Limits can only be lowered.
`submit` is immediate bounded admission; a full pending lane returns
`Backpressure`, even if its worker has not yet taken the first queued item.
`inspect_encoded_image(bytes, mime)` returns dimensions and
`validate_encoded_image(bytes, mime)` returns unit. Both require exact
`image/png` or `image/jpeg` matching the magic and perform the existing bounded
container/dimension preflight without pixel decoding. Success does not certify
CRC, compressed payload, or complete codec validity. Actual decoding still
performs those checks. Neither method reads files or fetches URLs.

A submitted `OfflineRgbDecodeJob` is an owned Tokio future, already admitted even
before its first poll. `job.cancel()` or dropping it signals cancellation;
request-future cancellation/timeout does **not** release native admission while
the worker still owns input/scratch. The native job drops its buffers before its
private permit. `submit_retained(bytes, Arc<dyn Send + Sync>)` additionally retains
a caller-owned accounting/snapshot anchor in both native work and the returned
future. Thus cancelling a host request cannot free a host storage slot while
native work still uses it. Callers must retain that same anchor in downstream
image carriers when they need queued/forwarded-image storage to remain charged.
The caller bounds its anchor allocations. An anchor must not retain the decoder
owner itself or have a destructor that waits for this worker; such caller-created
ownership cycles/blocking destructors are outside the supported contract.
Native `max_jobs` does not bound
arbitrarily many completed results retained by an application; the eval host's
separate carrier lease is responsible for that lifetime bound.

`close()` atomically closes admission and signals current native jobs; it makes
no claim of completed cleanup. `status().outstanding_jobs` counts actual native
ownership, not futures still awaited. `shutdown().await` closes/cancels and
actually joins once; repeated calls are idempotent. Its five-second configurable
cooperative diagnostic returns `Timeout` if the native thread has not exited,
while retaining the handle and cancellation registry in the same owner.
Dropping a shutdown future also retains that ownership, so another call resumes
joining. `status().joined` becomes true only after the actual join. There is no
`spawn_blocking`, detached join helper, fake hard deadline, or native-thread kill.
Final owner Drop cancels then synchronously joins, and can block until an in-flight
bounded codec operation reaches a cancellation check. Callers should explicitly
await shutdown rather than using Drop as an asynchronous timeout guarantee.

Static PNG palette/grayscale/alpha expansion and JPEG RGB normalization have the
same supported subset and color behavior as provider decoding. Alpha is discarded
without background compositing; high-bit-depth conversion, color profiles, CMYK,
and lossy JPEG are not universal Pillow pixel-equivalence claims. The new offline
API does not itself establish an eval transport, image-request/history ownership,
or function-calling video support.

`tests/offline_rgb.rs` covers public API RGB output, the existing synthetic gray
JPEG, PNG palette/grayscale/alpha, strict MIME, malformed/truncated/trailing/APNG,
header dimensions, and allocation-capacity rejection. Deterministic owner tests
hold native work across unpolled-future drop, queued/active cancellation, deadline,
shutdown-wait cancellation, diagnostic timeout, and last-owner Drop, then verify
actual permit release and joined cleanup.
