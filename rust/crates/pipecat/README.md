# Pipecat facade

The `pipecat` crate reexports the native workspace components. Optional providers,
transports, and local backends must be selected with Cargo features; none of the
features below is enabled by default.

| Feature | Public module | Scope |
| --- | --- | --- |
| `aws-bedrock` | `pipecat::aws_bedrock` | Bedrock Converse/ConverseStream and pipeline inference |
| `aws-speech` | `pipecat::aws_speech` | Polly synthesis and Transcribe streaming recognition |
| `classifiers` | `pipecat::classifiers` | Typed question batches through native LLM inference or Jev HTTP |
| `deepgram-flux` | `pipecat::deepgram_flux` | Flux Listen v2 WebSocket STT and pipeline processor |
| `grok-realtime` | `pipecat::grok_realtime` | Bounded xAI voice WebSocket client, frame processor, and explicit tool-result relay |
| `images` | `pipecat::images` | OpenAI image generation, bounded native PNG/JPEG decoding, and image frame delivery |
| `moq` | `pipecat::moq` | Native MoQ Lite 06 client, Opus audio, and ordered JSON |
| `piper` | `pipecat::piper` | Local synthesis configuration, service, and pipeline processor |
| `rnnoise` | `pipecat::rnnoise` | Local noise-filter configuration and media-transport `AudioFilter` |
| `soniox` | `pipecat::soniox` | Streaming STT, multiplexed TTS, and native pipeline processors |

For example, enable `aws-bedrock` and import
`pipecat::aws_bedrock::BedrockProcessor`. AWS credentials remain explicit inputs;
selecting a feature never connects to a provider or reads credentials.

The `classifiers` feature exposes standalone typed classification. LLM schemas
provide prompt guidance plus local parsing, not provider-enforced structured
output or calibrated probabilities. Pipeline/UI workers, voicemail timers, and
evaluation runners are outside this crate.

The `deepgram-flux` feature exposes Listen v2 separately from the Listen v1 client
in `pipecat::streaming_speech`. Applications must explicitly apply the processor's
recommended user-aggregator configuration. Eager turn prediction is opt-in;
selecting the Cargo feature does not enable it.

The `images` feature retains the source-tree-only `pipecat-images` package
(`publish = false`) and its patched direct-path JPEG decoder. Hosted image
downloads require an explicit exact-origin policy. Output backend support and
queue budgets remain application responsibilities. Gemini, Azure, and Fal image
adapters are not included.

The `soniox` feature exposes bounded streaming speech and per-stream TTS
cancellation. Provider-endpoint STT turns require explicit user-aggregator
configuration. Generated PCM and timestamps do not establish playback; spoken
history uses real output receipts. There is no automatic paid-work replay.

The `grok-realtime` feature exposes the native provider adapter and tool-result
relay. Its protocol, tool, and output fixtures use synthetic provider messages
and explicitly seeded context. Generic realtime aggregation, reliable executor
cancellation outcomes, and arbitrary upstream context reconciliation remain
separate prerequisites; selecting this feature does not supply them.

The `piper` facade feature does not enable its `onnx` or `espeak-gpl` backends,
and `rnnoise` does not enable its `native` backend. Add the corresponding direct
crate dependency with the desired backend feature when loading local native
artifacts. The GPL eSpeak phonemizer requires explicit opt-in. Models and native
libraries are caller-supplied inputs and are never downloaded by these services.

See the [Bedrock](../pipecat-aws-bedrock/README.md),
[AWS speech](../pipecat-aws-speech/README.md),
[Classifiers](../pipecat-classifiers/README.md),
[Deepgram Flux](../pipecat-deepgram-flux/README.md),
[Grok Realtime](../pipecat-grok-realtime/README.md),
[Images](../pipecat-images/README.md), [Soniox](../pipecat-soniox/README.md),
[MoQ](../pipecat-moq/README.md),
[Piper](../pipecat-piper/README.md), and [RNNoise](../pipecat-rnnoise/README.md)
crate documentation for backend requirements, supported protocols, and validation
limits. Facade availability does not establish live-provider or physical-output
acceptance, or full workspace parity.

From the Rust workspace, the API smoke tests run without credentials, network
access, or model/native-library loading:

```sh
cargo test -p pipecat --offline --locked --no-fail-fast --features aws-bedrock,aws-speech,classifiers,deepgram-flux,moq,piper,rnnoise,images,soniox,grok-realtime --test optional_features
```
