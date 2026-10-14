# Azure asynchronous REST images

`AzureImageConfig::new(api_key, endpoint)` and `AzureImageClient` implement the
pinned submit/poll/download service. `ImageGenProcessor::azure(client)` uses the
existing ordered native image processor, under the distinct name
`AzureImageGenServiceREST`. `ImageGenProcessor::new(OpenAiImageClient)` keeps its
original API and name. The facade's existing `images` feature exposes these types;
no new dependency or feature is required.

## Source and qualification

The fixed reference is [ProfHepta/pipecat, af9f9da9](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/azure/image.py),
Git blob `593089d7c97165f611dbee1570df5c7a2c7f052f`. The shared image service is
blob `28ab8d98be669775c28ffe7d28b9754f127810e4`. Tests use local synthetic peers,
virtual keys and real native image codecs. Current availability of the legacy
Azure endpoint/API version, authentication, hosted models, quality and billing
are unknown. These tests do not certify live Azure or physical display output.

## Submit, poll and select

The submit path is `openai/images/generations:submit`, under the configured base
path, with `api-version` defaulting to `2023-06-01-preview`. Native construction
normalizes the separating slash and encodes the query value; the source directly
concatenates its endpoint string. Authentication is `api-key`, not Bearer.
The JSON body contains `prompt` and `n: 1`; `size` is included only for a non-null
setting. Both settings default to null. A model setting is retained as metadata
and can be updated, but is never sent to this API, matching the source.

A submission is made once. Its single nonempty `operation-location` header must
be an absolute URL with exactly the API endpoint's scheme, host and effective
port, without credentials or fragment. Relative URLs and cross-origin operations
are rejected before any request can send the API key there. Redirects are never
followed, including same-origin redirects. Each credentialed poll checks the
operation policy. Operation and image query strings are omitted from the
adapter configuration/client/result Debug implementations and error output.
The successful result and downstream image carrier intentionally retain the
hosted URL, so applications must treat that field as potentially signed data.

Every poll is preceded by a sleep, including the first. Source-compatible defaults
are one second and at most 119 GETs: the Python loop starts with 120 and decrements
before checking for zero. A bounded configuration may reduce the poll count or
change the positive interval. Only exact `status: "succeeded"` stops polling;
`failed`, `canceled` and other status strings continue until the budget expires.
This intentionally preserves the pinned source rather than assuming a different
provider state machine. A poll HTTP error is terminal, not an automatic retry.

Successful JSON selects only `result.data[0].url`. No second-result or base64
fallback occurs if that first URL is absent, empty, denied or fails downloading.
Malformed JSON or missing/invalid fields are explicit errors. Poll JSON is parsed
from the bounded body regardless of its Content-Type; unlike aiohttp
`response.json()`, this native path does not enforce the response JSON MIME type.
Native submit and
poll require successful HTTP status codes, unlike source accesses that can throw
before clearly reporting a non-success response. Poll exhaustion is a timeout.
One total deadline additionally covers submit, sleeps, polls, hosted download and
decoding; a count budget alone would not bound stalled network requests.

## Credential and image boundaries

The source forwards its API key to an unchecked operation URL. Native same-origin
validation is a deliberate security restriction. The configured API endpoint is
trusted caller input; untrusted returned URLs are never treated as configuration.
No real credentials are needed for deterministic tests.

Hosted downloads reuse the same deny-by-default `HostedImagePolicy` as OpenAI:
explicit exact trusted origins, public-unicast address validation, pinned DNS
results and connected-peer verification. They use a separate credential-free
client, without `api-key`, Authorization, cookies, proxy, redirects or retries.
The API loopback opt-in and hosted literal-loopback fixture policy are separate.
An untrusted image URL cannot opt itself into loopback/private network access.

Existing image limits bound prompts, response bodies, encoded bytes, dimensions,
pixels, decoder scratch, accepted requests and deadlines. Operation URLs and
header/JSON handling are bounded. Submit/poll checks allow at most 64 headers
and 16 KiB of header names/values after reqwest has parsed them; operation URLs
are at most 8192 bytes. Admission remains owned through decoding;
cancelling a request does not prematurely free the active decoder's permit.
The shared owned decoder thread joins on last-client drop. A synchronous codec
operation already underway remains cooperatively cancellable, not preemptible.
Limits bound admitted content and work; they are not an exact process RSS claim.

Azure's Python adapter returns Pillow's original mode and bytes without an RGB
conversion. This native adapter deliberately uses the existing RGB-normalized
PNG/JPEG profile: grayscale expands, alpha is discarded, and 16-bit PNG reduces
to 8-bit. Tests make RGBA/grayscale differences explicit. This is not arbitrary
Pillow-format or source-mode preservation. Existing decode format/resource
restrictions remain in force.

## Pipeline behavior

Azure shares the established image processor's text-family mapping, frame and
opaque-authority guards, byte/count admission, ordered completion dependencies,
settings snapshots and health-generation fences. Text is forwarded before its
image, with identity and direction preserved; generated images use the native
downstream output path and carry the source frame ID and transport provenance.
No new text variant is silently admitted for old providers. Runtime settings
updates remain atomic and target the Azure name/local processor independently of
OpenAI. Endpoint, credentials, API version and poll limits are configuration,
not runtime settings fields.

End/Stop drain accepted work. Cancel and interruptible-frame interruption revoke
stale work without replaying a submission. As with the existing image processor,
a closed instance is not reusable after cleanup; create a new processor. Output
is a real decoded `UrlImage` through `OutputTransport::write_image`, not a browser
wire event or proof that a human saw it.

## Local acceptance

`tests/azure_http.rs` exercises the actual submit/poll/download protocol,
credential isolation, safe URL/redirect handling, status/count/first-result
rules, native RGB decoding, resource limits and cancellation. `tests/azure_pipeline.rs`
exercises actual TCP services through the shared processor and native output
backend, settings/provenance/ordering and lifecycle. The facade's
`azure_image_feature` test checks the exported API/defaults/name only.
Existing OpenAI and Gemini tests remain the shared-path regression gate.
