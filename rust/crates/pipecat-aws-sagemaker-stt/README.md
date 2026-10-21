# Finite NVIDIA SageMaker STT

Native mono PCM16 → signed SageMaker HTTP/2 EventStream → NVIDIA NIM transcription → native detailed frames → bounded public application and `sagemaker_stt` executable. This crate does not import Nova's private APIs, run Python, or use the AWS SDK. It uses unchanged public core and common AWS signing primitives.

## Exact profile

- Commercial regional `https://runtime.sagemaker.{region}.amazonaws.com:8443/endpoints/{EndpointName}/invocations-bidirectional-stream`, service signing name `sagemaker`, caller-supplied numeric IP:8443, verified regional SNI and ALPN `h2`. Runtime DNS, proxies, endpoint redirects, credential discovery, insecure TLS and hosted fallback are absent. Literal-loopback HTTP is a separate explicit fixture route.
- Immutable settings default to `cache-aware-parakeet-rnnt-en-US-asr-streaming-sortformer`, `en-US`, 16000 Hz, one channel. The short model name in the Python docstring is not the emitted model default.
- One `transcription_session.update`, then **append followed by commit for each PCM chunk**, then NIM `session.end`, then exactly one signed outer message with zero payload. VAD does not defer commits. No `CompletionState`, `P`, model query/path/target-variant header is sent.
- Each request inner event is `PayloadPart`, `application/octet-stream`, `DataType=UTF8`, with raw NIM JSON bytes. Only the PCM within the append JSON is base64. There is no Nova `chunk` event or base64-of-JSON wrapper.
- A delta produces `InterimTranscriptionDetailed`; a non-Python-whitespace-only completed transcript produces `TranscriptionDetailed`, unchanged text, full result JSON, configured language/user, ISO8601 receive timestamp, and `finalized=true`. Python whitespace includes U+001C through U+001F. Empty or invalid UTF8/JSON and unknown informational events are counted/ignored. Corrupt framing/CRC, malformed AWS envelope, exception/error envelope and NIM error fail the finite session without reconnect/replay.

## Public app and ownership

Construct explicit `AwsCredentials`, `SageMakerSttConfig`, a route, optional `TranscriptionSettings`/`SttLimits`, and a caller `Arc<dyn FrameProcessor>`. `SageMakerSttApp::new(config, pcm, output)` validates the entire finite PCM admission before starting a pipeline. `start()` returns `RunningApp`; `completion()` can be cloned, `finish()` waits, and `cancel()`/`stop()` cancel locally. Dropping `RunningApp` cancels, not remote processing. Retain `Completion` to obtain cleanup evidence. Cancelling a completion waiter leaves its real manager handle retained for subsequent/concurrent waiters.

The app feeds actual Start/InputAudio/End through `SageMakerSttProcessor`. Output may arrive while upload remains open and before EOF. A downstream output guard applies deadlines and catches output panics. End fences input, closes NIM/signed input, drains responses, then joins the owned HTTP/2 writer, reader and connection. Cancel/Stop/drop cancel independent of data queue admission. Join flags report awaited handles, not merely EOF, channel closure or abort requests. Forced-abort/cleanup errors are not success; setup failure may have no created transport child tasks. A manager panic leaves join proof unknown and fails closed.

The input is unpaced finite-file audio, not microphone scheduling. Defaults: at most 8 MiB, 120 seconds, 2400 chunks of at most 3200 bytes; max 8192 response events, 64 KiB result JSON, JSON depth 32, 1 MiB aggregate transcript text and 8 MiB aggregate serialized output. Transport queues independently bound event count and retained bytes, with bounded frames, H2 windows/headers and connect/write/idle/consumer/close/absolute deadlines. Configuration cannot exceed hard caps. Each public output callback is bounded; no unbounded sender escapes the application.

Captured PCM, admitted PCM and locally uploaded append bytes are separate counters. Uploaded bytes are not proof of recognition, server processing or billing. First interim/final timings are local receive observations from processor setup, not speech recognition latency. Observed transcript frames and successfully returned caller-output callbacks are separately counted. Terminal outcomes distinguish completed with transcript, completed without transcript, partial delivered output plus failure, local cancellation and failure.

## Real executable

`cargo run --offline --locked -p pipecat-aws-sagemaker-stt --bin sagemaker_stt -- --help`

Help/no arguments do not read config/credentials/files or connect. `--check-config` validates explicit nonsecret settings and a route using synthetic credentials, without reading credential variables. Active invocation:

`sagemaker_stt --run INPUT.pcm OUTPUT.jsonl [--timeout-ms N] [--max-output-bytes N]`

Explicit environment prefix: `PIPECAT_SAGEMAKER_STT_`. Fields: `ENDPOINT_NAME`, optional `REGION` (default `us-west-2`), exactly one `CONNECT_ADDRESS` or `LOOPBACK_ORIGIN`, optional `SETTINGS_JSON`, and active-only `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`, optional `SESSION_TOKEN`. `SETTINGS_JSON` supports `model`, `language`, `sample_rate_hz`, `user_id`; unknown/duplicate fields and nulls reject. Settings JSON is limited to 4096 bytes and nesting depth four.

Linux file execution accepts a bounded nonempty regular non-symlink raw PCM file. Even-length bytes cannot prove PCM encoding. WAV headers are not parsed. Every input/config check precedes file creation/socket use. Output must be new; existing objects, aliases, symlinks, parent traversal and changed directory/file identities fail closed. Directory/file descriptors are retained and O_NOFOLLOW/O_EXCL used; files are mode0600. Output records are buffered in bounded memory. After network/pipeline joins, interim/final JSONL plus an application summary are persisted through the original descriptor, and recording success is separately reported on stderr. An application summary is not a crash-durability receipt. Blocking writes/flushes may extend beyond network deadlines; no fsync/crash durability is claimed. Write/path/identity failure is nonzero exit, never successful recording. Failed preflight produces a sanitized diagnostic and no new output; later failure can leave an empty/partial file.

## Qualification scope

Source-level test corpus includes independently implemented captured-wire HMAC/CRC, commercial :8443 signature/date/token/terminal vectors, real tiny-window H2 peers (including withheld headers), malformed/fragmented/coalesced frames, cancellation/deadline/retained-owner cases, native app/detailed metadata/output cases, and real subprocess JSONL/file cases. The CLI corpus accepts `PIPECAT_SAGEMAKER_STT_TEST_BINARY` **in tests only** to replay against a separately built normal production executable; production has no test switch.

Local finite qualification is recorded in per-release evidence bound to the exact source revision or digest, commands, executable hashes and observed results. Checks, builds, tests, Clippy, formatting and replay against a separately built normal executable have distinct evidence; source presence or one successful gate does not establish the others. Qualification is limited to the cases and environment actually recorded, not live AWS/NIM behavior or full service parity.

Not claimed: full Python service lifecycle/settings parity, SDK credential refresh, automatic reconnect/replay, remote cancellation/exactly-once processing, live AWS/NIM qualification, microphone timing, accuracy/quality/billing equivalence, full voice-bot/LLM/TTS parity, FIPS/dual-stack/China/Gov/isolated partition support, full-workspace CI, merge or deployment.
