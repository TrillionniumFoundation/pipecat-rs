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
