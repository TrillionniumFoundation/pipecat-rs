# Core compatibility inventory

Reference: `src/pipecat/frames/frames.py` and the processor/pipeline runtime at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`. The static source fixture also records the exact
frame source SHA-256: `765266f414354ec753cda52b40f0517fffa056c868fbb767f3b41179d4f86449`.

## Frame surface coverage

All 134 Frame-derived source classes have dedicated native representations.
The eight non-frame helper/mixin classes have the mappings below. The 161 native
variants include convenience payloads and richer source-aligned variants. This
is a representation and queue-contract inventory, not a claim that every
provider, observer, strategy, or downstream consumer implements every frame.

Every native variant is explicitly instantiated by
`tests/native_contract_examples.rs`; its source class, priority and default
interruptibility are compared against the checked-in Python AST fixture.

| Python source class | Native representation(s) |
| --- | --- |
| `Frame` | `Base` |
| `SystemFrame` | `System` |
| `DataFrame` | `Data` |
| `ControlFrame` | `Control` |
| `UninterruptibleFrame` | `Frame::with_interruptible(false)` |
| `AudioRawFrame` | `AudioRawFrame` |
| `ImageRawFrame` | `ImageRawFrame` |
| `FileRawFrame` | `FileRawFrame` |
| `OutputAudioRawFrame` | `OutputAudio` |
| `OutputImageRawFrame` | `OutputImage`, `OutputImageDetailed` |
| `TTSAudioRawFrame` | `TtsAudio` |
| `SpeechOutputAudioRawFrame` | `SpeechOutputAudio` |
| `URLImageRawFrame` | `UrlImage` |
| `SpriteFrame` | `Sprite` |
| `TextFrame` | `Text` |
| `LLMTextFrame` | `LlmText` |
| `LLMMarkerFrame` | `LlmMarker` |
| `LLMMarkerResponseFrame` | `LlmMarkerResponse` |
| `AggregatedTextFrame` | `AggregatedText` |
| `VisionTextFrame` | `VisionText` |
| `TTSTextFrame` | `TtsText` |
| `AggregatedTextProgressFrame` | `AggregatedTextProgress` |
| `TranscriptionFrame` | `Transcription`, `TranscriptionDetailed` |
| `InterimTranscriptionFrame` | `InterimTranscription`, `InterimTranscriptionDetailed` |
| `EagerTranscriptionFrame` | `EagerTranscription`, `EagerTranscriptionDetailed` |
| `TranslationFrame` | `Translation`, `TranslationDetailed` |
| `LLMContextAssistantTimestampFrame` | `LlmContextAssistantTimestamp` |
| `LLMContextAssistantTurnFrame` | `LlmContextAssistantTurn` |
| `LLMContextFrame` | `LlmContext`, `LlmContextDetailed` |
| `LLMThoughtStartFrame` | `LlmThoughtStart`, `LlmThoughtStartDetailed` |
| `LLMThoughtTextFrame` | `LlmThoughtText` |
| `LLMThoughtEndFrame` | `LlmThoughtEnd`, `LlmThoughtEndDetailed` |
| `LLMRunFrame` | `LlmRun` |
| `LLMMessagesAppendFrame` | `LlmMessagesAppend`, `LlmMessagesAppendDetailed` |
| `LLMMessagesUpdateFrame` | `LlmMessagesUpdate`, `LlmMessagesUpdateDetailed` |
| `LLMMessagesTransformFrame` | `LlmMessagesTransform` |
| `LLMSetToolsFrame` | `LlmSetTools` |
| `LLMSetToolChoiceFrame` | `LlmSetToolChoice` |
| `LLMEnablePromptCachingFrame` | `LlmEnablePromptCaching` |
| `LLMConfigureOutputFrame` | `LlmConfigureOutput` |
| `FunctionCallResultProperties` | `FunctionCallResultProperties` |
| `FunctionCallResultFrame` | `FunctionCallResult`, `FunctionCallResultDetailed` |
| `TTSSpeakFrame` | `TtsSpeak` |
| `OutputTransportMessageFrame` | `OutputTransportMessage` |
| `DTMFFrame` | `FrameData::is_dtmf / KeypadEntry / DtmfSequence` |
| `OutputDTMFFrame` | `OutputDtmf`, `OutputDtmfSequence` |
| `StartFrame` | `Start`, `StartDetailed` |
| `CancelFrame` | `Cancel`, `CancelDetailed` |
| `ErrorFrame` | `Error`, `ErrorDetailed` |
| `FatalErrorFrame` | `FatalError` |
| `FrameProcessorPauseUrgentFrame` | `ProcessorPauseUrgent`, `ProcessorPauseUrgentTarget` |
| `FrameProcessorResumeUrgentFrame` | `ProcessorResumeUrgent` |
| `InterruptionFrame` | `Interruption` |
| `UserStartedSpeakingFrame` | `UserStartedSpeaking` |
| `UserStoppedSpeakingFrame` | `UserStoppedSpeaking` |
| `EagerEndOfTurnCancelFrame` | `EagerEndOfTurnCancel` |
| `UserMuteStartedFrame` | `UserMuteStarted` |
| `UserMuteStoppedFrame` | `UserMuteStopped` |
| `UserSpeakingFrame` | `UserSpeaking` |
| `UserTurnInferenceCompletedFrame` | `UserTurnInferenceCompleted` |
| `VADUserStartedSpeakingFrame` | `VadUserStartedSpeaking`, `VadUserStartedSpeakingDetailed` |
| `VADUserStoppedSpeakingFrame` | `VadUserStoppedSpeaking`, `VadUserStoppedSpeakingDetailed` |
| `ProposedUserStartedSpeakingFrame` | `ProposedUserStartedSpeaking` |
| `ProposedUserStoppedSpeakingFrame` | `ProposedUserStoppedSpeaking` |
| `BotStartedSpeakingFrame` | `BotStartedSpeaking` |
| `BotStoppedSpeakingFrame` | `BotStoppedSpeaking` |
| `BotSpeakingFrame` | `BotSpeaking` |
| `MetricsFrame` | `Metrics` |
| `FunctionCallFromLLM` | `FunctionCallFromLlm` |
| `FunctionCallsStartedFrame` | `FunctionCallsStarted`, `FunctionCallsStartedDetailed` |
| `ExternalFunctionCall` | `ExternalFunctionCall` |
| `ExternalFunctionCallsStartedFrame` | `ExternalFunctionCallsStarted` |
| `ExternalFunctionCallInProgressFrame` | `ExternalFunctionCallInProgress` |
| `ExternalFunctionCallResultFrame` | `ExternalFunctionCallResult` |
| `ExternalFunctionCallCancelFrame` | `ExternalFunctionCallCancel` |
| `FunctionCallCancelFrame` | `FunctionCallCancel` |
| `STTMuteFrame` | `SttMute` |
| `InputTransportMessageFrame` | `InputTransportMessage` |
| `OutputTransportMessageUrgentFrame` | `OutputTransportMessageUrgent` |
| `UserImageRequestFrame` | `UserImageRequest` |
| `InputAudioRawFrame` | `InputAudio` |
| `InputImageRawFrame` | `InputImage` |
| `InputFileRawFrame` | `InputFile` |
| `InputTextRawFrame` | `InputText` |
| `UserAudioRawFrame` | `UserAudio` |
| `UserImageRawFrame` | `UserImage` |
| `UserFileRawFrame` | `UserFile`, `UserFileDetailed` |
| `AssistantImageRawFrame` | `AssistantImage` |
| `InputDTMFFrame` | `InputDtmf` |
| `OutputDTMFUrgentFrame` | `OutputDtmfUrgent`, `OutputDtmfUrgentSequence` |
| `SpeechControlParamsFrame` | `SpeechControlParams` |
| `ServiceMetadataFrame` | `ServiceMetadata` |
| `STTMetadataFrame` | `SttMetadata` |
| `LLMServiceMetadataFrame` | `LlmServiceMetadata` |
| `ServiceSwitcherRequestMetadataFrame` | `ServiceSwitcherRequestMetadata` |
| `WorkerFrame` | `Worker` |
| `WorkerSystemFrame` | `WorkerSystem` |
| `EndWorkerFrame` | `EndWorker` |
| `StopWorkerFrame` | `StopWorker` |
| `CancelWorkerFrame` | `CancelWorker` |
| `InterruptionWorkerFrame` | `InterruptionWorker` |
| `TaskFrame` | `Task` |
| `TaskSystemFrame` | `TaskSystem` |
| `EndFrame` | `End`, `EndDetailed` |
| `StopFrame` | `Stop` |
| `PipelineFlushFrame` | `Flush`, `FlushDetailed` |
| `BotConnectedFrame` | `BotConnected` |
| `ClientConnectedFrame` | `ClientConnected` |
| `OutputTransportReadyFrame` | `OutputTransportReady` |
| `InputTransportStartAudioStreamingFrame` | `InputTransportStartAudioStreaming` |
| `HeartbeatFrame` | `Heartbeat` |
| `FrameProcessorPauseFrame` | `ProcessorPause`, `ProcessorPauseTarget` |
| `FrameProcessorResumeFrame` | `ProcessorResume`, `ProcessorResumeTarget` |
| `LLMFullResponseStartFrame` | `LlmResponseStart`, `LlmFullResponseStart` |
| `LLMFullResponseEndFrame` | `LlmResponseEnd`, `LlmFullResponseEnd` |
| `LLMAssistantPushAggregationFrame` | `LlmAssistantPushAggregation` |
| `LLMSummarizeContextFrame` | `LlmSummarizeContext` |
| `LLMContextSummaryRequestFrame` | `LlmContextSummaryRequest` |
| `LLMContextSummaryResultFrame` | `LlmContextSummaryResult` |
| `FunctionCallInProgressFrame` | `FunctionCallInProgress` |
| `VisionFullResponseStartFrame` | `VisionFullResponseStart` |
| `VisionFullResponseEndFrame` | `VisionFullResponseEnd` |
| `TTSStartedFrame` | `TtsStarted` |
| `TTSStoppedFrame` | `TtsStopped` |
| `ServiceUpdateSettingsFrame` | `ServiceUpdateSettings` |
| `LLMUpdateSettingsFrame` | `LlmUpdateSettings` |
| `TTSUpdateSettingsFrame` | `TtsUpdateSettings` |
| `STTUpdateSettingsFrame` | `SttUpdateSettings` |
| `UserIdleTimeoutUpdateFrame` | `UserIdleTimeoutUpdate` |
| `VADParamsUpdateFrame` | `VadParamsUpdate` |
| `FilterControlFrame` | `FilterControl` |
| `FilterUpdateSettingsFrame` | `FilterUpdateSettings` |
| `FilterEnableFrame` | `FilterEnable` |
| `AudioBufferStartRecordingFrame` | `AudioBufferStartRecording` |
| `AudioBufferStopRecordingFrame` | `AudioBufferStopRecording` |
| `MixerControlFrame` | `MixerControl` |
| `MixerUpdateSettingsFrame` | `MixerUpdateSettings` |
| `MixerEnableFrame` | `MixerEnable` |
| `VolumeFrame` | `Volume` |
| `VolumeGainFrame` | `VolumeGain` |
| `ServiceSwitcherFrame` | `ServiceSwitcher` |
| `ManuallySwitchServiceFrame` | `ManuallySwitchService` |

## Runtime behavior implemented here

- Stable Start > system > normal queue priorities, FIFO within each lane
- Explicit frame IDs, class-local names, metadata, timestamps, transport routing labels, paired broadcast IDs and per-instance interruptibility
- Separate system and normal processing lanes, generation-stamped interruption, pause/resume, bounded admission and explicit internal backpressure errors
- Start confirmation, End drain, Cancel preemption, Stop without cleanup, cancellation-safe join, setup/cleanup panic handling and cleanup after cancelled startup
- Tracked background futures, interruption opt-in, task limits, End preparation hooks and targeted task cancellation
- Upstream error propagation with configurable bounded history and cumulative/eviction counters
- EndWorker, StopWorker, CancelWorker and InterruptionWorker requests turn around at the sink and apply at the source
- Both ordinary and Detailed terminal/error/pause controls have runtime handling
- Flush and FlushDetailed make the downstream/upstream/downstream trip; an optional LocalFlushEvent is set when the detailed probe completes

## Detailed payloads and consumer boundaries

The richer variants preserve fields absent from the convenience payloads. They
are forwarded unchanged by this runtime. Existing service, transport or
aggregation crates may intentionally match only convenience variants; verify
that consumer before supplying a richer variant. Representation is not
automatic conversion or execution of every control command.

- OutputImageDetailed preserves sync_with_audio; UrlImage, Sprite, UserAudio/UserImage, UserFileDetailed and AssistantImage retain their source-specific fields. Video travels as timestamped image frames; this source version has no standalone VideoRawFrame class
- TranscriptionDetailed and its interim/eager/translation counterparts embed TextFrame properties. Convenience transcription variants retain direct text and transcription fields
- LlmContextDetailed carries rich message parts and speculation. LlmMessagesAppendDetailed/UpdateDetailed preserve optional run_llm. Existing simple context/message variants remain available
- LlmFullResponseStart/End preserve skip_tts; LlmThoughtStartDetailed/EndDetailed preserve append-to-context, model and signature. Convenience response/thought boundary variants are units
- FunctionCallsStartedDetailed includes context; FunctionCallResultDetailed includes result properties and an executable context-updated callback. Source arbitrary context objects are represented with typed native context data, not Python objects
- ServiceUpdateSettings and LLM/TTS/STT variants have typed boxed deltas, explicit omitted/null/value settings, legacy extension maps, optional processor targets and reach_inactive_services. Applying these deltas to a live provider is that provider’s responsibility
- LlmSetTools supports declared functions, executable native functions and provider-specific custom tools. This crate does not perform Python signature/docstring introspection or automatically register handlers with a provider
- Source DTMF sequences normalize button/buttons precedence and reject empty/invalid strings. The convenience KeypadEntry enum also retains A–D as a native extension; source-compatible DtmfSequence accepts only 0–9, * and #
- StartDetailed carries tracing metadata using native W3C traceparent strings. This does not install OpenTelemetry tracing or reproduce Python SpanContext objects
- EndDetailed/CancelDetailed and worker-request reasons preserve JSON values; convenience End/Cancel reasons remain optional strings
- ErrorDetailed can carry a native processor reference and local exception object. Fatal errors fail the worker immediately; retained error records remain available if shutdown preempts upstream delivery
- Named references are serializable. Native executable callbacks, arbitrary local objects, local processor references and local flush events explicitly reject serialization instead of losing behavior
- Audio sample count is a derived method; image/PCM payloads are owned bytes. Python shared-object mutation and pickling are not reproduced

## Remaining runtime gaps

Nested, parallel and synchronized parallel composition are implemented in
[`pipecat-pipeline`](../pipecat-pipeline/README.md), with bounded branch ownership
and documented synchronization limits. Its
[service/LLM switching](../pipecat-pipeline/README.md#service-and-llm-switching)
supports explicit manual/failover selection without automatic request replay.
Core [processor health and worker policy](README.md#processor-usability-and-worker-policy)
provide usability tracking, permanent-error handling and revisioned watches;
[compound support](README.md#compound-processor-support) supplies derived health
and causal routing contracts. Service-specific gating still requires each adapter's
implementation; these contracts do not establish full runtime parity.

- Python event-handler aliases and before/after callbacks; external tracing adapters and automatic rich metrics instrumentation of every service. Native global observation and explicit measured metric streams are provided by `pipecat-observers`.
- Python-style idle-worker policy and readiness callback/decorator parity (native opt-in heartbeat traversal diagnostics are documented below). Native registry watches and readiness waits are provided separately by [`pipecat-workers`](../pipecat-workers/README.md)
- Applying summarization, tools, prompt-caching, service-setting, mixer/filter and other domain-specific frames is not done by the generic core runtime
- Stop preserves processor-owned resources but does not provide a resume/re-attach API; owners must eventually clean up retained resources
- Flush does not establish quiescence of arbitrary detached provider work or coordinate distributed workers. Tracked work is drained by End/Stop
- Current pipeline startup runs processor setup sequentially, with a timeout per setup hook and another timeout for Start traversal. Timeouts require nonblocking cooperative futures
- Internal queue pressure is fail-fast instead of Python’s unbounded buffering; capacities bound counts rather than payload bytes
- The native API is not Python import, subclassing, decorator, exception-object, pickle or Python wire-schema compatibility

Multi-worker bus/RPC and registry facilities are in `pipecat-workers`. Service,
speech, turn, protocol and transport behavior must be assessed in their own
crates rather than inferred from this frame inventory.

## Fixture maintenance

The source fixture is exported using AST parsing only; importing Pipecat or
running Python at runtime is unnecessary. To deliberately refresh a reviewed
source reference:

```sh
python scripts/export_frame_contract.py
python scripts/generate_frame_contract.py
python scripts/generate_contract_examples.py
cargo fmt -p pipecat-core
cargo test -p pipecat-core
```

The reviewed native-to-source mapping lives beside the fixture. A source class
or native variant addition fails the exhaustive mapping/constructor checks
until its representation and queue contract are accounted for. The Rust runtime
tests are derived from the Python source semantics; the Python test suite is
not claimed to run against this native crate.

Opt-in bounded native root traversal monitoring is available through
`Pipeline::with_heartbeats`; see [HEARTBEATS.md](HEARTBEATS.md) for the pinned
source contract, native identity/pressure/lifecycle differences and limits.
