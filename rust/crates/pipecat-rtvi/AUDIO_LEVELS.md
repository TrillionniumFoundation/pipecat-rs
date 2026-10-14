# Optional rolling audio levels

Construct an observer with `with_audio_levels(AudioLevelConfig { user_enabled:
true, bot_enabled: true, ..Default::default() })?`. Both flags default false;
`RtviObserverParams`, core frames and existing wire schemas are unchanged.

This closes the PCM -> explicit observer/tap -> RTVI `user-audio-level` /
`bot-audio-level` path. It uses the existing BS.1770 implementation previously
owned by VAD, now shared through the lightweight `pipecat-audio` crate. It is not
RMS, speech probability, a microphone gain control or an output-playback receipt.

## Bounds and timing

- Only whole little-endian mono PCM16 at 8, 16, 24 or 48 kHz is admitted. Each
  stream retains the newest 400 ms, at most 19,200 samples. The default input
  limit is 192,000 bytes; configurations may lower it, with a hard ceiling of
  192,000 bytes. Validation precedes window, clock and dedup mutation. Rejected
  selected inputs return a nonpermanent error; disabled streams are not validated.
- User InputAudio and UserAudio share one window; TtsAudio has an independent bot
  window. OutputAudio and SpeechOutputAudio do not substitute for TtsAudio.
  Ordinary upstream observations are supported. Ignored-source, first-push,
  broadcast-sibling and duplicate filtering precede window updates.
- Every admitted frame feeds its window, including frames between reports. A
  level is calculated only when a report is due. Before 400 ms has accumulated,
  the level is zero. The fixed period defaults to 150 ms and must be 1 ms..60 s;
  comparison is strictly greater than the period. There are no background timers.
- The first eligible frame reports immediately. Later reports use a monotonic
  clock. Changing sample rate clears that stream's window, not its report clock.
  `reset()` clears windows, report clocks and observer dedup state, retaining the
  explicit configuration. Hosts reusing an observer choose when to call reset;
  tap setup does not silently reset a shared observer.
- PCM is fed without a temporary full-frame sample vector; oversized allowed
  chunks only visit the retained tail. Measurement visits at most 19,200 samples
  synchronously. Existing final RTVI message-size limits continue to apply.

## Fixed-source boundary

Reference: ProfHepta/pipecat `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`audio/volume.py`, `audio/utils.py`, and RTVI observer/models. Source uses a
wall-clock timestamp initialized to zero and normal epoch time gives an immediate
first report; native makes that starting policy explicit and uses monotonic time.
Source accepts general rates and treats interleaved channels as consecutive mono
samples. Native deliberately rejects unsupported rates/channels and malformed or
oversized PCM atomically. This slice does not add runtime configure-frame flags.

The standalone shared meter retains the previous VAD `volume(&self)` recompute-on-
read behavior. Source's standalone tracker lazily caches a measurement. RTVI only
calls the meter when reporting is due, so suppressed reports incur no measurement.
VAD keeps its existing public wrapper, return/error types and 8/16 kHz restriction.
The coefficients/quantization and MIT attribution are preserved. The new audio
meter does not claim arbitrary-duration integrated loudness or channel weighting.

## Qualification

Original-source AST state/routing cases and full-file corruption rejection are
separate from real `loudness==0.2.0` numerical vectors. Those vectors cover all four
rates, constant/sine/alternating/silent signals, partial and rolling windows and
rate changes. Native tests compare the shared meter, retain VAD compatibility
checks, and exercise the real pipeline tap, preserved audio identity, serialized
RTVI levels, bounded End/Cancel and explicit reset/reuse. These are local fixtures,
not a live microphone, browser display or physical output qualification.
