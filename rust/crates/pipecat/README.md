# Pipecat facade

The `pipecat` crate reexports the native workspace components. Optional providers,
transports, and local backends must be selected with Cargo features; none of the
features below is enabled by default.

| Feature | Public module | Scope |
| --- | --- | --- |
| `aws-bedrock` | `pipecat::aws_bedrock` | Bedrock Converse/ConverseStream and pipeline inference |
| `aws-speech` | `pipecat::aws_speech` | Polly synthesis and Transcribe streaming recognition |
| `moq` | `pipecat::moq` | Native MoQ Lite 06 client, Opus audio, and ordered JSON |
| `piper` | `pipecat::piper` | Local synthesis configuration, service, and pipeline processor |
| `rnnoise` | `pipecat::rnnoise` | Local noise-filter configuration and media-transport `AudioFilter` |

For example, enable `aws-bedrock` and import
`pipecat::aws_bedrock::BedrockProcessor`. AWS credentials remain explicit inputs;
selecting a feature never connects to a provider or reads credentials.

The `piper` facade feature does not enable its `onnx` or `espeak-gpl` backends,
and `rnnoise` does not enable its `native` backend. Add the corresponding direct
crate dependency with the desired backend feature when loading local native
artifacts. The GPL eSpeak phonemizer requires explicit opt-in. Models and native
libraries are caller-supplied inputs and are never downloaded by these services.

See the [Bedrock](../pipecat-aws-bedrock/README.md),
[AWS speech](../pipecat-aws-speech/README.md), [MoQ](../pipecat-moq/README.md),
[Piper](../pipecat-piper/README.md), and [RNNoise](../pipecat-rnnoise/README.md)
crate documentation for backend requirements, supported protocols, and validation
limits. Facade availability does not establish live-provider or device acceptance.

From the Rust workspace, the API smoke tests run without credentials, network
access, or model/native-library loading:

```sh
cargo test -p pipecat --locked --features aws-bedrock,aws-speech,moq,piper,rnnoise --test optional_features
```
