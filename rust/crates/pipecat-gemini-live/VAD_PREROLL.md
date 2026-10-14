# VAD pre-roll controls

The existing core `SpeechControlParams` frame connects VAD/turn-analysis timing
to Gemini Live's manual-activity audio window. This adds no VAD model or provider
configuration. The initial window remains 500 ms, bounded by the configured raw
audio limit. A downstream control with `vad_params` sets its duration to
`start_secs + 0.1` seconds at the configured mono input sample rate. A control
without `vad_params` is a no-op. Accepted controls retain their original frame
envelope and continue downstream; they send no WebSocket message.

`with_preroll_ms` always wins, including zero. Under that explicit override all
controls, even invalid values, are ignored before payload validation. Without an
override, negative or non-finite start durations and windows exceeding either
the raw-audio or encoded JSON message limits are rejected atomically as
nonpermanent proposal errors. The previous capacity/audio remain usable.

The buffer retains complete PCM16 samples. Shrinking immediately keeps the
newest samples; growing cannot recover discarded history. Controls during an
active manual turn change the next idle window without replaying live audio or
synthesizing activity events. Server VAD still accepts capacity changes but
streams continuously and retains no idle audio. Sequential setup of the same
processor resets both buffer and capacity to its constructor/explicit setting.

## Fixed-source comparison

The reference is ProfHepta/pipecat
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/services/google/gemini_live/llm.py`. The captured fixture
`tests/fixtures/python_preroll_source.json` executes selected original AST
methods with immediate stubs, plus nine unchanged upstream test functions.
It does not run a Python scheduler, VAD model, or live provider.

Intentional native boundaries:

- Only downstream controls affect provider input. Python's selected handler
  does not filter direction; upstream native controls pass through unchanged.
- Python shrinks on the next idle audio append. Native shrinks immediately,
  including when activity starts before another audio frame arrives.
- Python can retain an odd byte count for fractional durations. Native rounds
  down to complete PCM16 samples.
- Python's zero-override `buffer[-0:]` retains all bytes. Native's existing zero
  override disables retention.
- Native validates finite/nonnegative duration and both configured size limits
  before mutation. Direct source-method calls bypass provider/model validation.

The fixture records these differences; it is not a blanket native-equality
oracle. Loopback WebSocket tests exercise the real native processor, frame
routing, wire order, error recovery, and sequential reuse without credentials.
