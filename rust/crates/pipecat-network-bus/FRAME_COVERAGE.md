# Frame wire coverage

The strict codec has source-generated encode/decode fixtures for 125 of the 134
unique frame classes in `src/pipecat/frames/frames.py`. This is **class coverage,
not unrestricted payload compatibility**. The table below is checked against
`pipecat_core::FRAME_CONTRACTS`; all populated fields in each fixture survive
native projection, and immutable envelopes preserve the complete original JSON
value, identities, omitted fields, and unknown ordinary fields across replay.
Unknown tagged classes are always rejected. No runtime Python, dynamic imports,
or opaque frame fallback is used by the Rust codec.

The source oracle selects actual class definitions, actual serializer methods,
and actual LLMContext/ToolsSchema adapters using AST loading. Optional provider
imports are excluded; LLMContext itself uses a minimal data holder. Parameter
fixtures use the actual Pydantic VAD/turn classes. Run the development-only
`tests/generate_frame_fixtures.py` with Python 3.11+ and Pydantic 2 to regenerate.
It also verifies original Python deserialization and reserialization, tuple-size
omission, unadapted tracing/callback omission, and explicit-null settings.

## Payload boundaries

- Images require a two-element, nonnegative u32 dimension list. Python's stock
  serializer omits tuple-valued `size`, so its normal tuple output cannot be
  recovered and is rejected. Rust emits a JSON list accepted by the original
  Python decoder and by Pillow; this is repaired interoperability, not a claim
  that Python's lossy tuple encoding is usable. Format strings/encoded data are
  transported; this codec does not validate a particular image file format
- Start preserves `enable_tracing`. Nonempty tracing contexts have no source bus
  adapter and are rejected on native export or explicit wire input
- Error frames reject processor and exception references. Error categories use
  the exact source `ErrorCategory` enum tag and uppercase member names.
  FatalError uses the core's Unknown category when the source category is absent
- Callback-bearing function results and user-image requests are transportable
  only without callbacks. Function-call contexts must be typed LLMContext values
- Common contexts support text, image_url, input_audio, base64 data-URL files,
  signed thinking, refusal, standard function calls, message extension fields,
  standard/custom tools, tool choice, and speculation. Provider-specific message
  wrappers, null/missing content, and unrepresentable content-part extensions are
  rejected. Function-call argument JSON text is decoded without integer rounding
  and re-emitted canonically; retain the envelope to preserve its exact original
  whitespace. Codec acceptance does not promise that every service adapter can
  execute every context or vendor extension
- ToolsSchema values and callback-free standard functions are supported. Direct
  function handlers and grouped tool sets have no executable network equivalent
- Base service/LLM/TTS/STT settings preserve settings dictionaries, sparse deltas,
  model/voice/language/sampling/turn-completion configuration, and inactive-service
  routing. Live service targets and provider-subclass tagged settings are
  rejected. Missing fields become NotGiven; explicit null becomes Null. Python's
  stock encoder omits None values, so a native sender emits explicit null when
  clearing a setting; the original Python decoder accepts this representation
- VAD fields and base turn parameters are typed. Service metadata is supported
  only without local user-turn-strategy objects. Strategy constructors and
  analyzer instances do not cross the network
- Flush frames are supported without a local synchronization event. Summary
  configs are supported without an in-memory LLM reference
- User images with a nested request and sprites require complete nested frame
  identities/metadata that the current native payloads cannot represent; these
  payloads are rejected rather than discarding nested identity
- Volume/gain use native f32. Incoming f64 values that cannot be represented
  exactly are rejected. Nonfinite native floats are rejected rather than changed
  to null. Integer payloads retain the existing signed/unsigned 64-bit guard
- Python itself silently omits many unadapted objects and callbacks. A receiver
  cannot detect state already erased by the sender. These limitations apply to
  fields actually present on the wire and to native export

## Unsupported classes

- `LLMMessagesTransformFrame`: executable message-transform callback
- `FrameProcessorPauseFrame`, `FrameProcessorResumeFrame`,
  `FrameProcessorPauseUrgentFrame`, `FrameProcessorResumeUrgentFrame`: required
  live processor reference; a string name is not its Python wire equivalent
- `ServiceSwitcherRequestMetadataFrame`, `ManuallySwitchServiceFrame`: required
  live service reference
- `SpriteFrame`: native sprite entries lack complete source nested-frame identity
- `MetricsFrame`: the native metric enum lacks model metadata, TTFA/TTFAT/turn
  metrics, and full cache/reasoning/audio token counters; dropping those fields
  would produce misleading telemetry

Framework RTVI frames and provider-specific frames are outside this core-frame
allowlist. Their local core payloads are not claims of Python bus-wire support.

## Per-class matrix

Every Supported entry has its own source-oracle fixture. Conditional restrictions
above apply even where a class has a supported fixture.

| Source class | Codec |
| --- | --- |
| `AggregatedTextFrame` | Supported |
| `AggregatedTextProgressFrame` | Supported |
| `AssistantImageRawFrame` | Supported |
| `AudioBufferStartRecordingFrame` | Supported |
| `AudioBufferStopRecordingFrame` | Supported |
| `BotConnectedFrame` | Supported |
| `BotSpeakingFrame` | Supported |
| `BotStartedSpeakingFrame` | Supported |
| `BotStoppedSpeakingFrame` | Supported |
| `CancelFrame` | Supported |
| `CancelWorkerFrame` | Supported |
| `ClientConnectedFrame` | Supported |
| `ControlFrame` | Supported |
| `DataFrame` | Supported |
| `EagerEndOfTurnCancelFrame` | Supported |
| `EagerTranscriptionFrame` | Supported |
| `EndFrame` | Supported |
| `EndWorkerFrame` | Supported |
| `ErrorFrame` | Supported |
| `ExternalFunctionCallCancelFrame` | Supported |
| `ExternalFunctionCallInProgressFrame` | Supported |
| `ExternalFunctionCallResultFrame` | Supported |
| `ExternalFunctionCallsStartedFrame` | Supported |
| `FatalErrorFrame` | Supported |
| `FilterControlFrame` | Supported |
| `FilterEnableFrame` | Supported |
| `FilterUpdateSettingsFrame` | Supported |
| `Frame` | Supported |
| `FrameProcessorPauseFrame` | Rejected |
| `FrameProcessorPauseUrgentFrame` | Rejected |
| `FrameProcessorResumeFrame` | Rejected |
| `FrameProcessorResumeUrgentFrame` | Rejected |
| `FunctionCallCancelFrame` | Supported |
| `FunctionCallInProgressFrame` | Supported |
| `FunctionCallResultFrame` | Supported |
| `FunctionCallsStartedFrame` | Supported |
| `HeartbeatFrame` | Supported |
| `InputAudioRawFrame` | Supported |
| `InputDTMFFrame` | Supported |
| `InputFileRawFrame` | Supported |
| `InputImageRawFrame` | Supported |
| `InputTextRawFrame` | Supported |
| `InputTransportMessageFrame` | Supported |
| `InputTransportStartAudioStreamingFrame` | Supported |
| `InterimTranscriptionFrame` | Supported |
| `InterruptionFrame` | Supported |
| `InterruptionWorkerFrame` | Supported |
| `LLMAssistantPushAggregationFrame` | Supported |
| `LLMConfigureOutputFrame` | Supported |
| `LLMContextAssistantTimestampFrame` | Supported |
| `LLMContextAssistantTurnFrame` | Supported |
| `LLMContextFrame` | Supported |
| `LLMContextSummaryRequestFrame` | Supported |
| `LLMContextSummaryResultFrame` | Supported |
| `LLMEnablePromptCachingFrame` | Supported |
| `LLMFullResponseEndFrame` | Supported |
| `LLMFullResponseStartFrame` | Supported |
| `LLMMarkerFrame` | Supported |
| `LLMMarkerResponseFrame` | Supported |
| `LLMMessagesAppendFrame` | Supported |
| `LLMMessagesTransformFrame` | Rejected |
| `LLMMessagesUpdateFrame` | Supported |
| `LLMRunFrame` | Supported |
| `LLMServiceMetadataFrame` | Supported |
| `LLMSetToolChoiceFrame` | Supported |
| `LLMSetToolsFrame` | Supported |
| `LLMSummarizeContextFrame` | Supported |
| `LLMTextFrame` | Supported |
| `LLMThoughtEndFrame` | Supported |
| `LLMThoughtStartFrame` | Supported |
| `LLMThoughtTextFrame` | Supported |
| `LLMUpdateSettingsFrame` | Supported |
| `ManuallySwitchServiceFrame` | Rejected |
| `MetricsFrame` | Rejected |
| `MixerControlFrame` | Supported |
| `MixerEnableFrame` | Supported |
| `MixerUpdateSettingsFrame` | Supported |
| `OutputAudioRawFrame` | Supported |
| `OutputDTMFFrame` | Supported |
| `OutputDTMFUrgentFrame` | Supported |
| `OutputImageRawFrame` | Supported |
| `OutputTransportMessageFrame` | Supported |
| `OutputTransportMessageUrgentFrame` | Supported |
| `OutputTransportReadyFrame` | Supported |
| `PipelineFlushFrame` | Supported |
| `ProposedUserStartedSpeakingFrame` | Supported |
| `ProposedUserStoppedSpeakingFrame` | Supported |
| `STTMetadataFrame` | Supported |
| `STTMuteFrame` | Supported |
| `STTUpdateSettingsFrame` | Supported |
| `ServiceMetadataFrame` | Supported |
| `ServiceSwitcherFrame` | Supported |
| `ServiceSwitcherRequestMetadataFrame` | Rejected |
| `ServiceUpdateSettingsFrame` | Supported |
| `SpeechControlParamsFrame` | Supported |
| `SpeechOutputAudioRawFrame` | Supported |
| `SpriteFrame` | Rejected |
| `StartFrame` | Supported |
| `StopFrame` | Supported |
| `StopWorkerFrame` | Supported |
| `SystemFrame` | Supported |
| `TTSAudioRawFrame` | Supported |
| `TTSSpeakFrame` | Supported |
| `TTSStartedFrame` | Supported |
| `TTSStoppedFrame` | Supported |
| `TTSTextFrame` | Supported |
| `TTSUpdateSettingsFrame` | Supported |
| `TaskFrame` | Supported |
| `TaskSystemFrame` | Supported |
| `TextFrame` | Supported |
| `TranscriptionFrame` | Supported |
| `TranslationFrame` | Supported |
| `URLImageRawFrame` | Supported |
| `UserAudioRawFrame` | Supported |
| `UserFileRawFrame` | Supported |
| `UserIdleTimeoutUpdateFrame` | Supported |
| `UserImageRawFrame` | Supported |
| `UserImageRequestFrame` | Supported |
| `UserMuteStartedFrame` | Supported |
| `UserMuteStoppedFrame` | Supported |
| `UserSpeakingFrame` | Supported |
| `UserStartedSpeakingFrame` | Supported |
| `UserStoppedSpeakingFrame` | Supported |
| `UserTurnInferenceCompletedFrame` | Supported |
| `VADParamsUpdateFrame` | Supported |
| `VADUserStartedSpeakingFrame` | Supported |
| `VADUserStoppedSpeakingFrame` | Supported |
| `VisionFullResponseEndFrame` | Supported |
| `VisionFullResponseStartFrame` | Supported |
| `VisionTextFrame` | Supported |
| `VolumeFrame` | Supported |
| `VolumeGainFrame` | Supported |
| `WorkerFrame` | Supported |
| `WorkerSystemFrame` | Supported |
