# Audio references

The G.711 binary vectors are independent CPython 3.12.14 `audioop` outputs,
matching the reference implementation used by `src/pipecat/audio/utils.py`.
They are test data only; the Rust library and tests do not invoke Python.

- `ulaw-encode.bin`, `alaw-encode.bin`: one byte for each signed PCM16 sample
  in ascending order from -32768 through 32767
- `ulaw-decode.pcm`, `alaw-decode.pcm`: little-endian PCM16 for codewords 0..255
- `stereo.wav`: Python `wave` output, PCM16LE stereo, 24000Hz, samples 1,-2,3,-4

Reproducible generation (Python 3.12 or the compatible `audioop` module):

```python
import audioop, struct, wave
from pathlib import Path
directory = Path(__file__).parent
pcm = struct.pack('<65536h', *range(-32768, 32768))
for name, encode, decode in [
    ('ulaw', audioop.lin2ulaw, audioop.ulaw2lin),
    ('alaw', audioop.lin2alaw, audioop.alaw2lin),
]:
    (directory / f'{name}-encode.bin').write_bytes(encode(pcm, 2))
    (directory / f'{name}-decode.pcm').write_bytes(decode(bytes(range(256)), 2))
with wave.open(str(directory / 'stereo.wav'), 'wb') as wav:
    wav.setnchannels(2)
    wav.setsampwidth(2)
    wav.setframerate(24000)
    wav.writeframes(struct.pack('<4h', 1, -2, 3, -4))
```

`assets/dtmf-*.wav` are exact copies of the corresponding Pipecat BSD-2-Clause
resources in `src/pipecat/audio/dtmf/`. They are 8000Hz, mono, PCM16, 500ms.
Resampler tests compare analytic sinusoids, DC, alias rejection, exact rational
duration, channel separation, and whole-buffer versus arbitrary stream chunks.
They do not assert SOXR or resampy equivalence.

## Onset corpus

`onset-reference.bin` was produced by executing the `detect_speech_onset`
function extracted directly from the repository's `src/pipecat/audio/utils.py`
with CPython 3.12.14 and NumPy 2.3.5, without altering the function. Each record
contains little-endian `u32 sample_rate`, `u16 channels`, `u32 pcm_byte_length`,
`i64 expected_onset` (-1 for None), followed by PCM bytes.

All cases are at 16000Hz. Tones use `sin(2*pi*220*arange(n)/rate)*5000`, cast
to int16 (truncation). Cases are:

1. 500ms silence + 500ms tone: onset 7936
2. 200ms tone: onset 0
3. 300ms silence: none
4. 5ms tone: none
5. 200ms silence + one sample of 2370 + 200ms silence: none
6. 200ms silence + 10ms tone at amplitude 8000 + 200ms silence: none
7. 100ms silence + one sample of 2370 + 150ms silence + 300ms tone: onset 3936
8. 100ms silence + 300ms tone, duplicated to stereo: onset 1536
9. One second of constant sample value 6000: none
10. 100ms silence + 300ms tone + one trailing byte 0x01: onset 1536

SHA256: `4dc67c96713088373f875dbb2c155911ed3e61b3e14f36e2ee4d3358ae3dbd27`
