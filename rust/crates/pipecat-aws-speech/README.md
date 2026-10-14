# AWS speech

Native implementation of the AWS speech family from Pipecat Python revision
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`:

- `src/pipecat/services/aws/tts.py`: Polly language/settings, escaped SSML,
  signed `/v1/speech`, PCM16 response and output-rate conversion
- `src/pipecat/services/aws/stt.py`: Transcribe language/settings, fresh
  presigned WebSocket connection, interim/final transcripts and lifecycle
- `src/pipecat/services/aws/utils.py`: explicit credential value, SigV4 and
  AWS binary eventstream protocol
- `tests/test_aws_stt_language.py`: provider-specific language regressions

Credentials must be injected by the application. This crate does not resolve
ambient AWS environment variables, credential files, IAM metadata, SSO, or roles.

Protocol references:

- <https://docs.aws.amazon.com/transcribe/latest/dg/streaming-setting-up.html>
- <https://docs.aws.amazon.com/polly/latest/APIReference/API_SynthesizeSpeech.html>
- <https://smithy.io/2.0/aws/amazon-eventstream.html>
- <https://github.com/aws-samples/amazon-transcribe-streaming-python-websockets>

Implementation and verification status are documented below as each API lands.

## Runtime API

`AwsCredentials::new(access_key, secret_key, session_token)` validates and holds
application-injected credentials. All credential fields and signed URLs redact
`Debug`; HTTP/WebSocket failures omit provider bodies and request URLs. Each
connection/request receives a fresh SigV4 signature. Credentials are never
persisted and there is no account setup, automatic IAM lookup, or live call in
the test suite. Refresh temporary credentials in application code by constructing
a new client.

- `PollyConfig::new(credentials, region)` defaults to Joanna, en-US, 24 kHz
  output. `settings` exposes engine, pitch, rate, volume and lexicon names.
  `PollyClient::synthesize(text)` returns an owned bounded `PollyStream` of
  PCM16LE mono chunks. Polly is always asked for 16 kHz PCM; the native sinc
  resampler performs output-rate conversion, including arbitrary HTTP chunk
  splits. Dropping or cancelling a stream closes its work immediately.
- `PollyTtsProcessor::new(client, max_pending)` includes bounded sentence
  aggregation for Text/LLM/Vision text, preserving append policy, skip flags,
  destination and source identity. Complete `TtsSpeak`, `AggregatedText` and
  three-channel `TtsTextInput` are also accepted. Accepted utterances run in
  order with per-utterance immutable settings. End drains, Cancel and
  interruption cancel stale speech, and response-end/flush fences follow
  synthesis. Lifecycle, usage/TTFB, alignment source anchors and zero-audio
  watchdogs use the native shared frame contracts. Synthesis completion never
  claims audio was played or text spoken.
- `TranscribeConfig::new(credentials, region)` defaults to en-US, 16 kHz input,
  high partial-results stability. Input is mono PCM16LE; 8 kHz remains 8 kHz,
  other allowed 8–48 kHz rates are actually resampled to 16 kHz. The signed
  rate always matches wire PCM. `TranscribeClient::connect()` returns an owned
  session with a cloneable bounded sender and a transcript stream. `finish()`
  queues an empty binary AudioEvent after accepted audio and resampler tail,
  then drains server events to normal close with a deadline.
- `TranscribeSttProcessor` supports muted/passthrough audio, language/stability
  deltas, provider-defined transcript segmentation and bounded reconnection
  after an established socket fails. During VAD speech, settings wait until
  speech stops; the old session drains before the replacement connects. Audio
  admitted to a failed session is never replayed. Exhausted recovery reports
  unsent buffered audio and makes the provider unusable. An effective settings
  update can restore usability. Initial setup failure is returned to the caller.
  `SttMute` applies in either direction before the generic upstream bypass.
  Native control forwarding remains unchanged; see [mute control routing](STT_MUTE.md).
  Both `InputAudio` and `UserAudio` use this path. By default, the latest consumed
  user-audio command supplies the current speaker ID for interim/final results;
  plain input clears it. `with_user_id(...)` remains an explicit fixed override,
  including an empty string. Muted/unusable and upstream audio do not adopt
  the frame's speaker; upstream forwarding retains the existing native behavior.
  Health retirement can still clear the previous dynamic speaker generation.
  Audio passthrough preserves the original frame and applies to both input kinds.

`TtsSettings.base.extra` carries `engine`, `pitch`, `rate`, `volume`, and
`lexicon_names`; optional values can explicitly be null. Required voice and
language reject null. `SttSettings.base.extra` carries
`partial_results_stability` (`high`, `medium`, `low`). Credentials, transport
limits, region and PCM format are construction-time settings. Unknown deltas
fail atomically. Canonical processor names are `AWSPollyTTSService` and
`AWSTranscribeSTTService` for name-targeted settings frames.

## Bounds and protocol behavior

HTTP redirects and ambient proxies are disabled. Production endpoints must be
the exact regional AWS routes; only literal loopback overrides are accepted for
synthetic peers. Commercial/GovCloud-style `amazonaws.com` and China
`amazonaws.com.cn` routes are supported; isolated AWS partitions and custom
remote/VPC proxy routes are not implemented.

Polly validates a maximum 3,000 input characters and 6,000 escaped SSML
characters; it does not split overlong requests automatically. Text/attribute
XML escaping prevents malformed SSML injection. Pitch is emitted only with an
explicit standard engine, matching the source. Audio byte limits apply to both
received PCM and resampled output. Truncated samples, empty audio, unsupported
content types, provider errors, timeout and cancellation remain distinct.

Transcribe uses RFC 3986 canonical query encoding, with double-encoded equals
as required by its signing guide. WebSocket data and terminal AudioEvents use
CRC32-protected AWS eventstream, not JSON audio messages or an OpenAI-style
endpoint. Decoding verifies exact lengths, both CRCs, all ten header types,
unique names and limits before accessing JSON. All results in a TranscriptEvent
are processed; first alternatives and original result objects are retained.
Transcript finality comes from `IsPartial`, not a locally guessed VAD boundary.
Provider exception bodies and unknown error details are intentionally redacted.

Dynamic speaker UTF-8 bytes and PCM share the processor's existing
`max_audio_chunk_bytes` admission budget, checked before retaining an owned ID.
The constructor's fixed override and ignored frame IDs do not reduce the PCM
budget; the fixed override has no new length restriction. This is a byte-length
admission bound, not an allocator-capacity or process-memory ceiling. It excludes
the original frame envelope and pre-existing audio context. Valid large PCM still uses the low-level sender's EventStream chunking;
`max_message_bytes` is a separate encoded-wire limit. In default dynamic mode,
empty usable input updates identity through the bounded command FIFO but sends no
PCM and records no usage; it retains the prior bypass of empty-input geometry
validation. With an explicit fixed override, empty input bypasses the command queue
(including when full) and retains the configured audio passthrough behavior.
In dynamic mode, without an active input generation it only seeds the next
session, without a connection. Health retirement and cleanup discard that idle seed.

## Scope and verification limits

This crate implements the pinned Polly and Transcribe speech adapters. It does
not implement AWS Bedrock, HTTP/2 Transcribe, multichannel recognition,
automatic language identification, vocabulary management, speech marks,
automatic credential refresh, AWS SDK role/SSO/metadata chains, or keepalive
silence injection. Most of those are not exposed by the pinned speech service
constructors. Voice/engine/region combinations remain subject to AWS validation.
The sinc converter is native and deterministic, not sample-identical to SOXR.

The package uses existing workspace-cached Rust HTTP, WebSocket, crypto and CRC
libraries. Its lockfile adds only this crate's workspace package entry. Tests
use synthetic fake keys and real loopback HTTP/WebSocket peers; no production
AWS credentials, billable requests or provider quality claims are involved.

Python service callbacks/tracing wrappers and its precise STT latency/usage
emission timing are not fully mirrored: native STT usage records successfully
queued audio rather than emitting at final-transcript delivery. The utility-level
vocabulary/speaker/channel signing options are not exposed by `TranscribeConfig`;
the pinned service itself hardcodes those off. The eventstream decoder's 128
header/128 KiB header-region caps intentionally bound the AWS speech use case
rather than claiming an unrestricted general Smithy codec.

Current-speaker labeling follows native supervisor consumption, including output
while submitting audio, old-settings drain, and socket recovery within that
running supervisor. A new supervisor starts from its idle seed, fixed override,
or empty default. It is not a per-PCM identity association, diarization, or user-isolated resampling: the same
streaming sinc converter and its Finish tail continue across speaker changes.
The native ordering, upstream audio bypass, health-generation reset and retry/drain
behavior remain distinct from Python's callback scheduling. In particular, this
slice does not reproduce Python's final-callback identity read after awaiting
usage emission. Identity tests use independently CRC-framed loopback events and
verify the upgrade request's SigV4 MAC; EventStream payloads are not chunk-MACed.
No live-provider latency, recognition quality, or full asynchronous equivalence
is claimed.
