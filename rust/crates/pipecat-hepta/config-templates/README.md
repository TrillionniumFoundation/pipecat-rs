# Local speech configuration template

`local-speech.template.json` mirrors the current `LocalSpeechConfig` serde shape.
It is an incomplete, passive preparation file. Invalid digests, zero generations
and placeholder paths/IDs prevent loading. It grants no device authority, creates
no key and enables no model download. No device or runtime is opened to validate
this template. `language: "zh"` is a proposed Chinese acceptance setting; repeat
with the actual target language and a trusted multilingual model. An English-only
Whisper model cannot establish Chinese quality.

An authorized existing owner must provide its real stream/turn/output epoch,
monotonic capture incarnation, existing private media root and matching signed
audio policy, native devices and trusted immutable local artifacts. Set Piper's
phonemizer inputs for the adopted voice; do not replace them with mock speech.
The listed queue, retention and thread budgets are preparation defaults, not a
claim that a target device meets its resource or latency acceptance limits.

Integrate the library in the existing supervised media owner. This template is
not a standalone launcher or a new conversation/signing service. The corresponding
Hepta materials and original owner flags are in
`codex-rs/hepta-agentd/config-templates/hmi/README.md` in the Hepta checkout.
Signature/file binding tests validate software claims; physical capture,
language quality, noise/echo, hotplug, revoked permissions, model failure,
playback observations and microphone-to-speaker p50/p95/p99 still require the
adopted platform, artifacts and actual devices.
