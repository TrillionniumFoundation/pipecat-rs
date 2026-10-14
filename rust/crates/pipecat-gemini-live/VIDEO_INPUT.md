# Bounded realtime video input

Conversation-mode `GeminiLiveProcessor` accepts downstream `InputImage` and
`UserImage` raw pixels. It encodes JPEG and admits `realtimeInput.video` with
`mimeType: image/jpeg` to the existing bounded, generation-fenced session queue.
All successful, paused and throttled inputs retain their original frame,
direction and envelope. User/request/text metadata is forwarded, not sent as
provider context. Transcription mode and files remain unsupported; upstream
images retain the native bypass. This closes sampled-image input to the Live
service; it does not implement capture, resize, browser video or session resume.

Only exact packed, top-down RGB is accepted: dimensions 1..=2048 per axis,
at most 262144 pixels, and at most 786432 input bytes. A short buffer is rejected;
bounded surplus is ignored, following `Image.frombytes`. The cached native
jpeg-encoder 0.6.1 uses quality 75, 2x2 chroma sampling, baseline/nonoptimized
Huffman output, as in the existing turn image encoder. JPEG bytes are not claimed
to match Pillow byte-for-byte. Other pixel formats must be converted explicitly.

The output writer caps JPEG bytes before append, checks cancellation, and reports
allocation failures. Its limit reserves the exact base64 and JSON overhead from
`Limits::max_message_bytes`. The sender checks the complete serialized message
again before queue admission. These are logical byte/work bounds, not allocator
RSS guarantees. Public `Action::VideoJpeg` accepts pre-encoded bytes with bounded
size and JPEG boundary markers; it is not a JPEG decoder or authenticity check.

Encoding runs on the blocking pool, with at most one owned job per processor.
The task handle remains in a processor slot while the caller awaits it by
reference. Cancelling the caller cannot detach that work. Cleanup signals the
writer and actually joins the retained handle; a new invocation joins any prior
job before starting another. Input dimensions bound CPU work between writer
checks. Invocation and provider-generation checks reject late encoded results
before local queue admission. Commands retain the captured generation rather than
resampling after encoding; the actual video write shares the sender interruption
gate, so an interrupt cannot reclassify old pixels as new work. Interrupt may
wait for an already admitted bounded write; Cancel bypasses that gate. Already
written provider work cannot be retracted.

The default is unpaused. `with_video_input_paused` selects the initial state;
`set_video_input_paused` changes new admissions, without withdrawing existing
work. Each setup restores the initial pause state and resets the throttle.
One service-wide monotonic one-second window applies across users, camera sources
and both input image variants. The timestamp advances before validation/encoding,
so malformed images and failed sends consume the window. Paused, not-ready and
finishing sessions do not consume it. Pause/resume does not reset the window.
Validation/encoding failures are recoverable input errors and do not forward the
failed image. No synthetic activityStart, audioStreamEnd or turn-complete is sent.
Video alone does not mark the session busy: End drains admitted writes and closes,
without promising that visual inference or provider playback has completed.

## Reference and evidence boundary

Pinned Python: ProfHepta/pipecat
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`services/google/gemini_live/llm.py::_send_user_video` and `process_frame`.
`UserImageRawFrame` inherits `InputImageRawFrame`. The source uses wall-clock time,
Pillow and both directions; native uses monotonic time, the stated RGB subset and
its established downstream-only input policy. Source retains its clock/pause
state on reuse; native resets them at setup. Source catches send failures but
lets codec failures escape. Native reports bounded input/transport errors through
its existing error channel, without claiming identical exception propagation.

The wire shape is also documented in Google's [WebSocket video guide](https://ai.google.dev/gemini-api/docs/live-api/get-started-websocket).

The original-method oracle records gates, shared clock, failure timing and
passthrough, separately from real Pillow probes. Native tests use an actual local
WebSocket, exact video JSON/base64 and JPEG geometry, pause/invalid input/budgets,
normal and cancelled reuse, and a controlled blocked encoder with actual join and
no late wire output. They do not qualify live provider credentials, a camera,
browser, visual model quality or a full Python scheduler.
