# Fixed source protocol fixtures

These checked-in bytes were constructed independently of either Rust protobuf
implementation from the original `frames.proto` field numbers. The ready JSON
pins the original RTVI label, request correlation, and 2.2.0 version; the audio
fixture pins PCM16 bytes and actual 48 kHz browser capture rate.

Authority snapshots (SHA-256):

- `src/pipecat/frames/frames.proto`: `adcd346af21673d3385d075c5a662a8407584b4b04f177320609596f4dea5c37`
- `src/pipecat/processors/frameworks/rtvi/models.py`: `2689d3cf72ea8348a20c869e01c868a8838e6068f3a7b5366b225dfc28205dd2`
- `src/pipecat/runner/run.py`: `90984c48f45bc66fb2f3b1149e0daa4068a44ef961414a03c25fc70b518d48a6`

The original runner `/start` WebSocket branch returns `wsUrl`, `sessionId`,
and `token`; the native runner preserves those response fields and implements
its own bounded, one-shot per-session listener.
