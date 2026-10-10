# Soniox source contract map

Python baseline: `ProfHepta/pipecat` commit `af9f9da9d7afddaa8811c0e0251b32566c980bdf`. Native baseline: `de78ee2ab9117614cddbca975f373044ffe1bb7d`.

| Pinned Python source | Native implementation | Offline fixtures |
| --- | --- | --- |
| `src/pipecat/services/soniox/stt.py:49–77,121–263` context, language hints and settings | `settings.rs`, `stt.rs` configuration | `typed_settings_validate_atomically_and_redact_secrets`, STT wire/region-language fixtures |
| `stt.py:418–476` turn proposals and settings reconnect | `stt.rs` actor/reducer; `stt_processor.rs` explicit recommended strategies | provider-turn/VAD fixture; independent STT native aggregator → HTTP LLM test |
| `stt.py:478–557,582–644` PCM/configuration, VAD finalize, empty-text End versus Cancel | `stt.rs` session/sender, `common.rs` | wire PCM/End, cancellation, multichannel, keepalive, settings reconnect, malformed/bounds and HTTP tests |
| `stt.py:659–777` final-token buffering, endpoint tokens, raw metadata, language tie and usage ordering | `stt.rs` transcript state; `stt_processor.rs` detailed frames | finalized/interim/error/finished/fragmentation fixtures and independent STT metadata/usage ordering |
| `src/pipecat/transcriptions/language.py` Language enum | `settings.rs` pinned result-language membership | fragmented STT `en-US`, unknown skip and first-seen tie fixtures |
| `src/pipecat/services/stt_latency.py:64` 0.35-second TTFS metadata | `stt_processor.rs` | source metadata only; no live latency claim |
| `src/pipecat/services/soniox/tts.py:116–241,405–517` settings, config-once, shared socket, keepalive | `settings.rs`, `tts.rs` shared hub, registry, stream actor | same-TCP interleaving/language snapshots, keepalive, HTTP handshake, stream admission/budget fixtures |
| `tts.py:294–403` eager stream, empty turn, flush/cancel, settings prefix rotation | `tts_processor.rs`; `tts.rs` per-stream finish/cancel | independent lifecycle and receipt-backed settings-rotation tests |
| `tts.py:519–635` partial words, CJK, audio/error/terminal routing | `tts.rs` live stream reducer | split/interleaved PCM/words, CJK language snapshot, sibling error/cancel, stale packets, terminal precedence, malformed data |
| `tts.py:637–671` incremental text and failure | `tts.rs` sender and shared actor | concurrent terminal admission, dropped finish/read, no replay, timeout and backpressure |
| `tests/test_soniox_stt.py` source regression intentions | public low-level and processor interfaces | `websocket.rs`, independent `review_stt_pipeline.rs` |
| `tests/test_soniox_tts.py` split words, settings-prefix rescue | same native text alignment and source-frame identities | `websocket.rs`, independent `review_spoken_receipts.rs`, `review_tts_lifecycle.rs` |

Native machinery is adapted from the reviewed `pipecat-streaming-speech/src/processor.rs` (aggregation, alignment, turn barriers, zero-audio watchdog), `pipecat-deepgram-flux/src/common.rs` (bounded socket/queue/cancel patterns) and its processor turn-strategy interface. No Cartesia/Flux wire protocol is reused. The Soniox TTS socket is shared across contexts rather than copied from the per-context connection model.

SHA-256 of the principal pinned sources:

```text
0efcc4de2a52df3fe0e71aaba123e2baeb4f3c5668b691268ef1c55b93fd25ed  src/pipecat/services/soniox/stt.py
3d2406c830ff7be26752ab90376b5c586aa405483e41f99a200a15848015c419  src/pipecat/services/soniox/tts.py
4db9087592e82c88933a7568b3c24897eb22337273fef5dd4147f6f02faba878  tests/test_soniox_stt.py
3eb2530c522220c2ef1eb5735608b6a6a5d7bc26888271eb0102ab82a404d11d  tests/test_soniox_tts.py
```

Supported scope and deliberate differences are in `README.md`. Source presets, synthetic fixture replies and successful socket configuration do not establish live inference parity.
