# Batch151 source map and intentional boundary

Python reference: ProfHepta/pipecat at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

- [HTTP service](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/nvidia/sagemaker/tts.py#L48-L223), Git blob`a6f6059be717bddcfb381420d6a2f0510431e55f`: defaults/settings → config.rs; trimmed text and exact four-field payload → protocol.rs/unicode15.rs; signed HTTP raw PCM invocation → transport.rs; source client lifecycle replaced by explicit credentials and locally owned HTTP futures.
- [Base TTS chunking](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/tts_service.py#L496-L510), blob`428b875796d0cf7ed07b89537be8a440ccc104f6`: half-second source body recommendation, but native preserves bounded samples, not scheduling identity.
- [Base TTSSpeak](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/tts_service.py#L956-L990) and [context close](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/src/pipecat/services/tts_service.py#L1886-L1962): processor.rs and app.rs admit complete utterances only; Start/Audio/Stop context lifecycle and interruption/drain. Source token aggregation, transformations, sequencer/history and timing are not imported.
- [Voice example](https://github.com/ProfHepta/pipecat/blob/af9f9da9d7afddaa8811c0e0251b32566c980bdf/examples/voice/voice-nvidia-sagemaker.py#L65-L103) uses bidirectional service, ASR/LLM and transport. This finite HTTP executable is expressly not a migrated version of that full voice bot.

Independent AWS wire reference: boto/botocore at
`22be6841647761fb4ae0f8bef6b1a55e882039f5`, not a claim the pinned Python environment
resolved that SDK version.

- [Service and InvokeEndpoint](https://github.com/boto/botocore/blob/22be6841647761fb4ae0f8bef6b1a55e882039f5/botocore/data/sagemaker-runtime/2017-05-13/service-2.json#L1-L35), blob`e1f5199fb439404e7fca2f59c780a0038e0a70c6`: signing name`sagemaker`, regional runtime endpoint prefix, POST endpoint-name invocation route.
- [Input/output](https://github.com/boto/botocore/blob/22be6841647761fb4ae0f8bef6b1a55e882039f5/botocore/data/sagemaker-runtime/2017-05-13/service-2.json#L256-L382): ordinary Accept, JSON body, raw response BodyBlob, bounded endpoint name and6,291,456-byte request ceiling; no EventStream envelope.
- [Endpoint rules](https://github.com/boto/botocore/blob/22be6841647761fb4ae0f8bef6b1a55e882039f5/botocore/data/sagemaker-runtime/2017-05-13/endpoint-rule-set-1.json#L334-L353), blob`7455b869c586be75d13e6ccf1206a3b85595e7d3`: commercial regional HTTPS profile.
- [Blob streaming treatment](https://github.com/boto/botocore/blob/22be6841647761fb4ae0f8bef6b1a55e882039f5/botocore/model.py#L735-L745): raw payload may be consumed incrementally without the response-stream API.

Existing unchanged native seams: pipecat-aws-common `signed_body_post_headers`;
core `TtsSpeak`/`TtsStarted`/`TtsAudio`/`TtsStopped`, guarded processor contexts,
interruptible task registry, pipeline admission/drain and retained worker joins.
AgentCore's owned HTTP1 driver, output acknowledgment and retained completion
patterns informed this implementation, without modifying or importing its crate.
No facade, shared core/turn/media/processors, Direct, ONNX, VAD or other provider
source was changed. Dependencies match the isolated AgentCore-shaped graph minus
UUID; no SDK or dependency upgrade was added.

Unicode generation reference is CPython3.12.14 Unicode15.0.0:747 sorted alphanumeric
ranges and29 whitespace scalars. This is an explicit source-compatible reference,
not an assumption that all future Python/Rust versions share the same tables.

Evidence is local synthetic-wire/file/ownership acceptance. No live model, AWS
account, real key, hosted TLS success, kernel in-progress public TCP connect,
voice quality, browser, physical playback or full Python-family parity is claimed.
