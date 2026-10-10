# AssemblyAI streaming source map

Reference files are the pinned repository's
`src/pipecat/services/assemblyai/{stt.py,models.py,__init__.py}` and
`tests/{test_assemblyai_stt.py,test_assemblyai_sync_stt.py}`. A source-frozen SHA256
manifest accompanies qualification. The Python init module is empty. No source
is executed by the native runtime.

| Source behavior | Native implementation | Evidence |
|---|---|---|
| stt.py 61–92 U3 prefix families | settings::is_u3_pro_model | variant loopback query tests; construction validation |
| stt.py 115–201 language resolution/dedupe/cap | settings::prepare_language_codes | regional dedupe, ten-code update tests; Sync uncapped test |
| stt.py 204–327 settings | StreamingSettings typed fields; serde unknown-field rejection | typed range/unit and exact request tests |
| stt.py 385–614 constructor defaults/validation | AssemblyAiConfig / normalized settings | default handshake, model gating, invalid limits; sample-rate pipeline test |
| stt.py 644–685 turn-mode configuration | StreamingSettings::normalized | U3 min=max and legacy threshold 1.0 query assertions |
| stt.py 696–712 external-strategy metadata | AssemblyAiSttProcessor metadata | provider-mode pipeline assertions, should_interrupt=false |
| stt.py 714–783 update settings | actor Command::Update / settings::patch | hot-only, mixed reconnect, over-limit drop, atomic invalid update |
| stt.py 797–862 chunking/ForceEndpoint | streaming actor and processor upstream VAD routing | exact binary 50 ms chunks, remainder ordering and ForceEndpoint |
| stt.py 873–943 query/context clipping | typed query builder, Unicode tail clipping | real handshake headers/query; U3/legacy omission matrices |
| stt.py 946–1003 carryover | actor Context and sender assistant_turn | explicit/automatic carryover and zero-context tests |
| stt.py 1005–1135 connect/disconnect | owned session actor, bounded drain/cancel | termination/late-message/cancel/timeout loopback tests |
| stt.py 1137–1243 messages/transcription | strict Wire/Turn/Word, TurnState, frame emission | malformed, oversize, format gate, language, speaker, ordering tests |
| models.py 17–128 message schemas | Wire / Turn / Word + raw result Value | required field and confidence/timing protocol validation |
| models.py 131–215 deprecated connection model | canonical Rust settings only | compatibility wrapper deliberately not claimed |
| stt.py 1274–1734 Sync | sync module | separate SYNC-SOURCE-MAP.md |

## Original streaming test groups

- `test_sample_rate_*`: native direct default and pipeline inherited/explicit rate
- Default/U3/continuous_partials/interruption_delay/previous_context/voice_focus/
  mode/language_code/language_codes/prompt test groups: typed field validation,
  source-gated URL construction and actual WebSocket query assertions
- `test_update_agent_context_*`, `test_update_settings_*`: live wire commands,
  clipping, idempotent empty/non-U3 no-op, field-drop behavior and real reconnect
- `_process_assistant_turn_*`: source upstream assistant-turn frame drives native
  context command; previous_context_n_turns=0 disables automatic carryover
- `test_speech_started_proposes_turn_without_interrupting` and
  `test_should_interrupt_rides_on_recommended_strategies`: native pipeline tests
  assert proposed frames, final-before-stop, both directions and metadata

These are native behavioral translations, not a claim that the Python pytest
suite was run against Rust. Additional adversarial tests strengthen ownership,
queue bounds, stale turns, malformed input, privacy and terminal handling.
