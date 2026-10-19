# Synthetic source-contract fixtures

All fixtures are constructed locally. None is a provider recording or live
conformance claim. Baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

- `setup_audio.json`: connection-time voice, language, temperature, max tokens,
  input/output transcriptions and function declarations from
  `GeminiLiveLLMService._connect`. This default-off fixture omits opt-in
  resumption/history settings and unsupported compression. `source-model` is a
  test identifier.
- `manual_activity.json`: PCM from manual activity-window tests in
  `tests/test_gemini_live_user_audio.py`; activity start must precede buffered
  onset audio, followed by active audio and activity end.
- `bundled_turn.json`: `_handle_server_message` and
  `tests/test_gemini_live_interaction_status.py` content-before-completion
  contract, with multiple model parts, thought/text/audio, independent input
  transcription, output transcription, generation/turn completion and usage.
  This Rust client processes all model parts. It accepts `IDLE` but explicitly
  rejects background-working statuses instead of claiming source parity.
- `stt_interim.json` and `stt_final.json`: utterance examples from
  `tests/test_gemini_stt.py`, translated from SDK snake_case into wire camelCase.
  They pin dedicated STT interim-versus-final semantics, not conversational
  transcript timeout aggregation.
- `initial_history_source.json`: static source provenance and bounded startup
  conclusions for the four exact pinned Python files. It is not an executed
  Python oracle, an SDK fixture, or evidence of hosted model availability. The
  native tests construct synthetic expected wire values and exercise actual
  processors/actors. In particular, the empty-context/system conclusion follows
  the two distinct adapter calls, not the stale automatic-greeting comment.

Wire casing and the setupComplete barrier were checked against Google's
https://ai.google.dev/api/live reference. JSON payloads remain small and contain
only synthetic text/audio/function definitions.
