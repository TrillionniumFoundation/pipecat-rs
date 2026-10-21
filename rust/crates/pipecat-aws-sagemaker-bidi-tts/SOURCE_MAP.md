# Source map: finite Magpie SageMaker BiDi TTS

Python repository: https://github.com/ProfHepta/pipecat
Pinned commit: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

| Pinned source | Git blob | Native boundary |
|---|---|---|
| `src/pipecat/services/nvidia/sagemaker/tts.py` | `a6f6059be717bddcfb381420d6a2f0510431e55f` | `NvidiaSageMakerTTSService` lines226 onward: session config, stripped text gate, append/commit, JSON/base64 and raw PCM receive, jitter, interruption and session.end |
| `src/pipecat/services/tts_service.py` | `428b875796d0cf7ed07b89537be8a440ccc104f6` | Half-second chunk threshold, context/interruption semantics; no claim that TtsStarted is heard audio |
| `src/pipecat/services/aws/sagemaker/bidi_client.py` | `d23138d8a6fa0ef09feca0097166683da376aa34` | Regional port8443, raw UTF8 PayloadPart; SDK discovery/retry excluded |
| `examples/voice/voice-nvidia-sagemaker.py` | retained batch151 provenance | BiDi class identification only, not complete voice-bot migration |

Independent AWS service model: `aws/aws-sdk-js-v3` commit
`e37bf8c2898e5e85201648f81dd07974e7d7e51c`,
`sagemaker-runtime-http2.json` blob `35bf61f726c0576f18ac0bb918a0b4c302f3836d`.
It establishes H2 POST `/endpoints/{EndpointName}/invocations-bidirectional-stream`,
`sagemaker` signing scope, raw PayloadPart eventPayload, DataType header and modeled
exceptions. It does not identify Python's resolved SDK version.

## Reuse and deliberate distinctions

- `transport.rs` is a private copy of the already-owned native
  `pipecat-aws-sagemaker-stt` H2 transport at Rust baseline
  `679466e1736b7a92b02c8958a6f5c89c95115d2e`, with labels and control-send exposure
  adapted, unused STT audio-admission methods removed. Retained H2 lifecycle tests
  exercise the transport; they are not misrepresented as new provider TTS tests.
- `unicode15.rs` is byte-identical to `pipecat-aws-sagemaker/src/unicode15.rs` at that
  baseline, including its original provenance comments. No broad text refactor.
- Linux descriptor/identity-aware file helper and independent test CRC/HMAC oracle
  derive privately from that baseline's STT crate. The fixture now asserts TTS
  config/text/commit, no routing headers, separate sessions and real terminal chain.
- Existing `pipecat-aws-common` signer and `pipecat-core` public frames/pipeline are
  consumed unchanged; no private module include or new shared frame is added.
- The source's interruption event can be set by any response. Native report names
  it `interruption_response_observed`; it does not claim remote-stop verification.
- A finite new session per utterance deliberately replaces persistent class-level
  connection state and reconnect. There is no interrupted-text replay.
- Valid nonempty initial short PCM tail is flushed at speech completion, unlike
  Python's receive loop. Interruption discards it. Whole PCM16 samples, invalid
  JSON shape rejection and explicit bounds are a stricter finite profile.
- Model `magpie` is descriptive source default, not an invented JSON model field.
  Neither STT audio append/commit nor ordinary HTTP request JSON is sent.

Local provenance was rechecked by Git blob hashing before source freeze. The
batch154 implementation source manifest identifies every changed file and preserves
this baseline, Python pin and independent service model pin. No live-provider test
or full Python persistent lifecycle equivalence is implied by source mapping.
