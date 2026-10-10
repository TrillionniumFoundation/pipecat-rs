# Voicemail detection and generated-speech admission

`pipecat-processors/voicemail` (facade `pipecat/voicemail`) is an opt-in native
single-session classifier and ordered TTS gate. Defaults add no dependency or
behavior to the processors crate. The classifier is caller-owned: setup, warmup,
and shared-client closure remain with its owner.

Construct `voicemail_pair(Arc<dyn Classifier>, VoicemailConfig)` and install the
returned detector before conversation processing and the returned TTS gate after
TTS, in the same native pipeline, in that order. Keep the third return value only
for read-only `snapshot()` / `changed()` observations. It retains one payload-free
watch value and at most one typed event; no callback, action, event queue, dial,
send, tool registration, or application speech is created.

Each pair is for one session. Both halves must attach once. Missing, reversed,
reused, or cross-runtime attachments are unsupported. Core cleanup has no runtime
argument, so reusing an Arc half in another runtime can fail-close its original
pair too; it never closes the caller's classifier. Destroy the pair and construct
a new one for a new call. A retained status handle does not retain a pair/runtime.

## Source policy

Final compact and detailed transcripts are trimmed, appended with one space,
and classified as one cumulative string. Their finalized bit, speaker ID and
confidence do not change this policy. Empty finals and interim/eager/translation
frames do not classify. Only one call is active; finals during it coalesce into
one later full-transcript snapshot. The existing `Classifier::choice` receives
the exact source `voicemail` question/options and no conversation history.

Decision silence defaults to one second. Nonempty finals while silent and speech
stops restart it. Speech starts cancel it. There is no startup-silence decision.
Silence expiry waits for every admitted transcript revision to be attempted.
The latest successful literal `conversation` or `voicemail` answer wins without
a confidence threshold; later failed attempts retain the prior success. No
successful answer falls back to conversation with `NoAnswerFallback` evidence.
Invalid/missing named choices count as missed answers, without provider strings
or transcript text in diagnostics.

A voicemail decision discards held human-directed output and broadcasts an
interruption. A second, independent two-second quiet interval begins after the
broadcast. Speech cancels that interval and a later stop restarts it. A late final
by itself does not restart it. Exactly one `ConversationDetected` or
`VoicemailReady` event is retained. `VoicemailReady` does not mean a message was
left, generated, submitted, Played, or physically audible. A dropped subscriber
cannot cause the event or an external action to be repeated.

## Supported and rejected topology

`CompactTts` is generated-speech admission only. It holds original compact
`LlmResponseStart/End`, TTS start/text/audio/stop, `SpeechOutputAudio`, prepared
TTS text, alignment diagnostics and aggregate-progress frames in arrival order.
Fresh, single-runtime TTS frames are a caller precondition; CompactTts cannot
fully enforce that provenance. Original frame identity, public metadata, native local tags, directions,
destinations, PTS, and original invocation contexts survive admission. New output
always joins the same ordered FIFO, including while an older frame waits for
capacity. Unrelated source-allowed frames still flow; generic `OutputAudio` and
ordinary LLM/tool effects are not this gate's authority.

The gate requires noninterleaved compact responses and noninterleaved matching
TTS contexts. Detailed/Vision response boundaries, native private tool/eager/LLM
continuation carriers, and reserved SpokenHistory source/response stamps fail
closed as unsupported shapes. Carriers are never stripped or rewritten.

`SpokenHistory` and `Other` configuration selectors fail before attachment with
`VoicemailConfigError::UnsupportedTopology`. The current public ledger API cannot
verify detector/source-boundary/gate/output/assistant attachment identities from
an arbitrary shared history handle. No invalidation hook is installed or implied.
Therefore this slice does not qualify Spoken-assistant history, receipt-to-history
commit races, standalone voicemail history, or rollback behavior. A future narrow
binding must verify the complete topology and use `SpokenHistory::invalidate`
before interruption, preserving already committed Played history. It must not
infer playback from this gate's counters. `completed_release_attempts` counts completed drain
futures, including stale frames suppressed by core routing; it is not an admission
count. Actual output admission must be observed separately.

Pending transcripts still reach the main conversation. LLM requests and tools
may run before a verdict. This module does not mutate the shared conversation,
rewind it, undo a tool, or prevent effects that already occurred.

## Bounds and lifecycle

All durations are positive and at most one hour. Default limits are 16 KiB
cumulative transcript, 256 segments, 32 KiB combined transcript allocations,
256 retained logical frames, 2 MiB conservative retained frame reservations,
1 MiB retained PCM allocations, and ten seconds of logical PCM duration. An
outer five-second classifier deadline also applies to custom classifiers that
ignore `CallOptions`. Yielding inference panics count as missed answers without
copying their payload into status or worker errors. Custom classifiers must yield
cooperatively and must not detach work; process-abort/OOM and user-defined panic
hooks are outside this module's cancellation/diagnostic contract. A separate thirty-second unresolved-hold safety deadline
starts at the first detection input or held frame. Capacity waits have a
five-second per-frame deadline. These are safety limits, not source silence timers.

These limits bound accounted gate-owned payload/FIFO/in-flight allocation, not
universal heap retention. Core's opaque, shared observed-runtime/error bookkeeping
can have been grown by external runtime circulation or concurrently retained
frame clones. Its size and future growth are not publicly inspectable here.
Admitting an externally circulated/reused frame can therefore retain more shared
core bookkeeping than the fixed overhead reservation. Do not feed such frames
from an untrusted/native extension and claim a hard total-heap bound. Caller-owned
runtime state, observer state, and classifier-retained copies remain outside the
pair's payload accounting.

Frame reservations include original allocation capacities, a possible drain
clone, compact topology bookkeeping, recursive JSON metadata, and bounded
context/tag/queue overhead. The frame moved out of the FIFO stays charged until
its owned drain future is dropped. PCM allocations count both possible copies;
PCM duration counts logical audio once. An active TTS context-ID allocation is
charged separately. Limit exhaustion fails closed and frees retained evidence;
there is no truncation, oldest-frame eviction, or synthetic conversation verdict.
External classifiers/callers own copies they retain beyond the borrowed call.

The decision supervisor survives ordinary interruption; one continuing media drain is
separately interruption-cancellable and uses the original invocation context.
Ordinary interruption discards interruptible held media; queued uninterruptible
media remains governed by the decision. Voicemail audience rejection drops all
held speech. After its causal interruption, only fresh post-interruption output
may pass. Controls are never awaited behind an application callback or full
normal output lane. Upstream release is nonblocking and overload is explicit.

End/Stop while unresolved discard speech and cancel classification/timers with
no fallback/event. After conversation, the detector stops its timers at its own terminal while the
gate keeps accepting original-scope TTS output. Core drains each upstream TTS
processor's owned work before forwarding End/Stop to the gate. At that ordered
boundary, the gate closes admission and drains its held output before terminal
forwarding under finite per-frame and core shutdown limits. Cancel, cleanup and
owner drop release buffers and tracked work. Stop is explicitly closed even
though the native runtime does not invoke ordinary cleanup for it.

## Source map and native deviations

Static reference: Python baseline `af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/extensions/voicemail/voicemail_detector.py`. It is source evidence
only and must not be imported or executed for qualification.

- Lines 53, 346–350: closed detector system/worker allowlist → `detector_allows`
- Lines 58–77: exact question/options → `question`
- Lines 80–152: paired TTS holding/release/drop → `TtsGate`, `supervise_gate`
- Lines 200–205, 329–344: two source delays/input → `VoicemailConfig`, `input`
- Lines 352–387: cumulative sequential inference → `next_step`, `supervise_detector`
- Lines 389–425: silence/latest-success/fallback → `next_step`
- Lines 427–459: verdict/interruption/readiness → `supervise_detector`, `next_step`
- Source lifecycle gaps → `prepare_end`, `cleanup`, tracked task guards

Deliberate safety deviations: bounded retention/deadlines; stop accumulating after
verdict; reject malformed choice labels rather than silently calling them a
conversation; outer inference deadline; original response markers join ordered
release; ordinary interruption cleanup; pending End/Stop discards rather than
flushes; typed one-shot readiness rather than arbitrary application callbacks;
explicit unsupported spoken/detailed/native-carrier topology rejection. Neither
confidence calibration nor telephony greeting/beep accuracy is qualified.
Successful source classifier metric frames are not forwarded in this first slice;
only bounded attempt/missed-answer counters are available. Native metric projection
is an explicit remaining source gap, rather than a full metrics-parity claim.

Native qualification results, exact immutable source hashes and failed attempts
are recorded separately. Local synthetic receipt tests can distinguish held,
gate-admitted, backend-submitted and Played observations; they do not establish
real telephony delivery, device audibility or production readiness.
