# Native context aggregation and user turns

`pipecat-turns` provides deterministic user/assistant context state machines and
`FrameProcessor` adapters. The default controller consumes VAD decisions and uses
speech/STT deadlines. `SmartTurnUserAggregator` adds the actual native
`pipecat-vad::SmartTurnAnalyzer`, with its explicit ONNX model/runtime paths;
no Python process or substitute energy threshold participates in inference.

## API

The pair also supports explicit/auto/off [realtime trailing aggregation](REALTIME.md).
Auto is the pair default; cascade behavior remains until realtime metadata arrives.
Provider adapters must separately implement its state-only context contract.

- `LLMContextAggregatorPair::new(Context, UserAggregatorConfig, AssistantAggregatorConfig)`
  creates a shared history. `user()` and `assistant()` return `Arc` processors;
  `context().snapshot()` returns the provider-neutral services `Context`
- `LLMUserAggregator::process_at(frame, direction, monotonic_duration)` and
  `advance(monotonic_duration)` return `AggregationOutput` without doing I/O
- `UserTurnController::process(&FrameData, monotonic_duration)`, `advance`, and
  `deadline` expose the independent turn state machine
- `UserIdleController` exposes an independent one-shot waiting-for-user timer;
  `LLMUserAggregator::on_user_turn_idle` registers native asynchronous handlers
- `LLMAssistantAggregator::process_frame(frame, direction)` returns deterministic
  effects. `AggregationOutput.events` contains typed turn and ignored-result
  events; `actions` contains directed/broadcast frames and interruptions
- `AggregationOutput::emit` sends the actions through a processor context. The
  deterministic interface returns events without invoking registered handlers

Place the user half before the LLM and the assistant half after the delivery
boundary. In text mode, incoming text is considered delivered. In speech mode,
generating text, generating PCM, sending PCM, and provider synthesis completion
are not playback acknowledgements.

## Playback acknowledgement

Use `AssistantTextMode::Spoken` for a voice pipeline. A transport that knows a
complete context has actually been played should:

1. Call `begin_playback(context_id)` when registering pending audio. This blocks
   a tool-result inference while spoken filler still awaits acknowledgement
2. Validate that an acknowledgement refers to a ready, current-generation
   context, and that its order satisfies the transport's playback contract
3. While holding the same transport lock used for interruption invalidation,
   call `confirm_playback(context_id, &[AggregatedTextFrame])`. It synchronously
   commits that complete context to shared history before a concurrent user
   interruption or new user context snapshot can overtake it
4. Emit the returned actions and notify the client. Do not also submit those
   text parts through the frame interface

Playback confirmation validates all part IDs before writing, is idempotent for
recently closed IDs, and preserves future pending playback contexts. The
transport owns the truth of its playback acknowledgement; this crate cannot
prove that a physical speaker emitted audio.

The lower-level framed interface accepts only `TtsText` whose metadata contains
`"spoken": true`. A normal-lane `Flush` with `origin="assistant_turn_end"`
commits those parts. `BotStoppedSpeaking` is a system frame and can overtake
normal-lane words, so it cannot serve as a commit fence. A framed delivery
adapter must serialize acknowledgement delivery against interruption and mark
confirmed data uninterruptible; an ordinary enqueue is insufficient for an
atomic remote acknowledgement. Prefer `confirm_playback` for complete-context
network acknowledgements. Word-level partial acknowledgements require a
transport with a corresponding timestamp/generation contract.

A pair takes each user-request snapshot under the assistant state lock. Tool
results already included in that snapshot require no redundant inference;
results arriving afterward remain owed until the admitted response finishes.
Same-batch speaking boundaries and automatic interruption preserve this admission,
while an explicit interruption cancels it. Muted and unrecovered empty turns
release a pending tool continuation. Empty-turn recovery instead includes
already-completed tools in its own single inference snapshot.

A pair seals already-delivered assistant text synchronously when a new user
turn interrupts it, before constructing the user context snapshot. Standalone
halves have no such coordination: their caller must settle the assistant
interruption before submitting an immediate next-turn context.

## Implemented behavior

- VAD/transcription starts; adopted speaking frames without duplicate broadcasts;
  optional resolution of proposed start/stop frames
- Separate policy-silence and STT-latency waits. A finalized transcript can
  shorten the STT wait but never the silence floor. A subsequent interim
  hypothesis invalidates prior finalization. Resumed speech invalidates stale
  timers; the watchdog never closes audibly active speech
- No-VAD transcript inactivity fallback; final segment accumulation with correct
  spacing; interim preview replacement without committing interim hypotheses
- Bounded replay detection by frame identity and optional stable
  `metadata["transcript_id"]`; identical text with a fresh identity remains valid
  in a later turn. Repeated provider events need a stable provider segment ID
- Explicit mute/unmute suppression; empty/whitespace transcripts do not append
  context text. A qualified finalized control may release already-buffered text
  as described below. A mute boundary preserves previously accepted final text
  without running inference and discards interim previews
- Session-end flush, cancellation cleanup, and one managed per-session
  turn/wake supervisor that remains dormant between deadlines. `prepare_end`
  cancels the supervisor before the runtime drains tasks
- Optional user-idle detection after bot speech, runtime timeout updates,
  interruption/function-call guards, and empty-turn re-arming. Its owned idle
  supervisor and callbacks are cancelled before End/Stop draining or on Cancel
- Bounded empty-turn recovery, enabled by default for an interrupted response;
  the exact source developer prompt requests clarification without inventing
  a user transcript. Idle empty turns remain unanswered by default
- Typed `InputText` runs immediately and interrupts pending output. Context
  append/update/run frames preserve the shared history
- Assistant streaming text/word spacing, append-to-context filtering, thought
  exclusion, terminal flush, partial delivered-context commit on interruption,
  and suppression of old acknowledged playback contexts
- Tool declarations, in-progress placeholders, ordered result replacement,
  groups that run inference once when complete, sticky deferred inference while
  a user/model/bot/playback context is active, and result-before-start rejection
- Async-tool started/intermediate/final/cancelled JSON protocol. Fast results
  settle the original placeholder; a result after a user/developer message is
  recorded as a deferred developer message. Interruptible calls become cancelled
  on interruption; the execution layer remains responsible for stopping them
- System-priority tool-start declarations cannot cause a fast result to rerun
  the LLM before normal-priority response-start/end markers arrive

### Empty finalized speech transcripts

An admitted empty or whitespace-only finalized transcript can complete the STT
wait for an active turn that already accepted non-whitespace text and observed
VAD. A VAD start is enough to observe VAD: finalization while still speaking
cannot stop the turn, and finalization after VAD stop cannot cross the speech
floor. With a 100 ms floor, 800 ms STT P99, and a VAD stop at 20 ms carrying
200 ms of observed silence, an empty final at 200 ms releases the original
620 ms safety wait and commits the existing text once.

This bounded path supports the scalar SpeechTimeout configuration or exactly
one non-deferred explicit SpeechTimeout detector, with no custom start list
and with `require_model_completion=false` in both cases. The explicit detector
normally has its own priority over the scalar model gate; this new control
path does not broaden that configuration and requires the global flag to be
false. It neither starts a turn nor invents transcript text. In the user
aggregator it additionally requires nonempty buffered context text, eager
configuration disabled and cascade aggregation. Legacy and Detailed finals
are supported; `append_to_context=false` does not suppress this completion.

The control still passes private/source, mute, terminal and frame/transcript-ID
admission. It bypasses text append, raw empty-frame retention, custom start
policies and eager matching, then uses the existing locked stop/context commit
route. An accepted completion also clears the public interim preview immediately,
even before the speech floor; rejected duplicates and excluded configurations
keep their prior preview handling. Repeated finals and later advances do not
emit duplicate boundaries or context. Existing nonempty transcript handling is unchanged.

No-text or inactive turns, pending adopted/proposed external stops,
custom-start combinations (including MinWords), eager/realtime aggregation,
mixed or deferred stop lists, model-gated
configurations and no-VAD fallback retain their prior paths. This is not a
blank-transcript parity claim: Python's whitespace-only first-turn behavior
remains a separate decision; blank-interim support has its own boundary below.
The deterministic `speech_timeout_empty_final` target covers the bounded path
and its guards; Python af9f9da9 behavior is source-derived, not a runtime oracle.

### Blank interim before VAD stop

An admitted empty or whitespace-only interim revokes earlier finalization only
for an active scalar SpeechTimeout turn with existing accepted text, while VAD
is still speaking and no VAD/external stop is pending. This path requires
`wait_for_transcript=true`, no model completion requirement, no custom start or
stop lists, and no pending inference request. Legacy and Detailed interim frame
types take precedence over their incidental `finalized` field. With final
`hello` at 10 ms, a blank interim at 15 ms and VAD stop at 20 ms, the same
100 ms floor/800 ms P99/200 ms silence example waits until 620 ms rather than
committing at 120 ms. A later final may still release the STT wait.

Only the finality flag changes. This control does not start a turn, add text,
refresh the watchdog, set interim-pending state, or write a deadline. Adopted
external stops keep their existing contract. The user aggregator keeps its
existing locked admission, interim preview and original-frame retention;
Frame.id replay, mute, terminal and private/source checks still precede this
effect. Interim frames do not use the final-transcript `transcript_id` deque.

Inactive/no-text/no-VAD, transcript-optional/realtime, custom/model and eager
configurations retain their existing paths. Blank interims after VAD stop are
unchanged. The existing nonempty post-stop interim path still reopens the old
STT safety deadline; Python does not necessarily recreate that canceled wait,
so this adjacent native behavior is not a Python-parity claim. Native explicit
SpeechTimeout already invalidates blank interims and needs no change. The
`speech_timeout_empty_interim` target exercises the public controller and real
user aggregator with caller-owned clocks; Python af9f9da9 remains a source
comparison, not an executed oracle.

## Rich frame and context contracts

Both legacy and detailed transcription, VAD, and LLM-response families drive the
same state machines. On VAD stop frames, legacy `VadSpeakingFrame.duration_secs` and
detailed `VadUserStoppedSpeakingFrame.stop_secs` supply the actual VAD stop delay when
computing the remaining STT wait; the speech policy floor still applies.
Detailed transcripts retain whitespace/append flags, and
`transcript_frames()` returns the latest 512 accepted final/interim originals,
including identities, metadata, user IDs, timestamps, language, and provider
results. Rich transcription frames are consumed just like source transcripts;
provider metadata is not appended to the spoken text. Eager transcripts and
translations never become final user messages. Muting also suppresses `UserAudio`.

`SharedContext::new_rich`, `snapshot_rich`, `append_rich`, and `replace_rich`
maintain core `LlmContextData`. `LLMContextAggregatorPair::new_rich` and
`with_shared_context` construct coordinated halves from rich/shared histories.
`frame_data()` selects a legacy frame for compatible native text histories and
`LlmContextDetailed` for rich messages, extensions, explicit empty tool lists,
and richer tool schemas. Local tool-handler identities are retained in memory;
network serialization must reject them rather than pretend they are serializable.

The existing `new`, `snapshot`, `replace`, `append`, and `frame` APIs remain.
Legacy snapshots and text-only frames explicitly reject unrepresentable content.
Supported image/audio/file/thinking/refusal parts survive native-to-rich framing.
Native `InlineData` does not identify whether bytes are an image or file in the
core schema: legacy snapshots retain it exactly, but rich snapshots and framing
require an explicit image/file conversion. Message transforms preserve every
retained native occurrence, and reject ambiguous partial selection when distinct
native messages have the same core projection. A failed transform is atomic.


`AssistantImage` has a narrow encoded-original context path for ordinary
application frames, including frames decoded by the public `PythonJsonCodec`.
When both `original_data` and `original_mime_type` are nonempty, originals win.
Exact `image/png` or `image/jpeg` MIME with a matching
PNG/JPEG signature is appended immediately as one assistant-role rich image
data URL. Original encoded bytes are preserved; display RGB, size and format are
not used. Signature checks are not complete image validation: a provider may
still reject malformed data. Existing snapshots/framing and the Gemini request
adapter preserve these bytes as model-role `inlineData`; this does not establish
live-provider acceptance or a native-provider visual round trip. Native image
processors still produce `UrlImage`, and native Gemini multimodal output remains
unsupported.

This path adds a fixed native admission cap of 8,388,608 original bytes per image,
without changing `AssistantAggregatorConfig` or its existing struct literals.
Checked base64 expansion and prefix addition limit a data URL to 11,184,835 bytes
before fallible allocation. These are per-image logical-length limits, not a
SharedContext total, input allocation-capacity, decoded-pixel or RSS budget.
Callers still manage cumulative history. The codec's JSON-envelope limit remains
separate. No image decoder or new background task is involved.

An accepted `AssistantImage` is consumed in either direction, produces no action/event,
does not flush pending assistant text, and does not run inference or invoke a
tool callback. Consequently an image received during buffered text appears in
context before that text's normal end-of-response commit. Successful admission
shares the existing last-512-frame-ID replay cache; failed validation leaves an
ID retryable. This native bounded replay policy is deliberately stricter than
Python's repeated-image append behavior. Ordinary images after End/Cancel/Stop
are ignored. Display fallback is restricted below. `UserImage` and PDF
`UserFileDetailed` support have separate admission policies.

Image admission rejects enabled realtime mode and any signed-continuation
tracker retained by this assistant instance, including a completed response
still waiting for playback/reconciliation. A live image cannot carry native
continuation or tool authority, including a retyped private-carrier clone.
Completed signed prefixes are preserved when no local tracker remains. This
capability assumes one serialized context writer: it cannot detect another
aggregator's active signed continuation or protect against concurrent external
context edits. Signed-image bookmark production and continuation redesign are
outside this scope.

`UserFileDetailed` admits an ordinary idle PDF upload or the narrow false-run
mid-turn case below: `source_type=Bytes`,
exact `application/pdf`, `append_to_context=Some(true)`, and `custom_options=None`.
Explicit `run_llm=Some(false)` appends only; an idle `Some(true)` or `None`
upload also prepares one upstream context for inference. The file must start with exact
`data:application/pdf;base64,` followed by nonempty canonical STANDARD base64.
Decoded logical bytes must start with `%PDF-` and total at most 8,388,608 bytes.
The encoded body is bounded to 11,184,812 bytes and the entire data URL to
11,184,840 bytes before decoding or copying. Checked arithmetic and fallible
reservation precede decode/copy, and the actual decoded length is checked too
(the cap and cap + 1 can have equal encoded lengths). This is a per-file logical
byte cap, not an RSS, aggregate history, input String-capacity, or entire-frame
budget; accompanying text and filename are separate fields and allocations.
The existing RTVI envelope limit is separate. These checks validate encoding
and a header only, not complete PDF structure, safety, sanitization, or provider
acceptance. No PDF decoder, URL resolver, file read, or provider I/O is performed.

Nonempty text is preserved verbatim before one rich `MessagePart::File` whose
`data` is raw base64, not a data URL. Empty text is omitted; whitespace stays.
Missing or empty filename normalizes to `Some("")` for Python/OpenAI parity.
User ID, filename, and metadata are data, never message names or control authority.
The real RTVI `send-file` producer supports raw base64 and equivalent full data
URLs, including omitted options (which request inference by default). The consumer
rejects a mismatched data-URL MIME header even
when the producer preserves it. The existing OpenAI request adapter reconstructs
one exact PDF data URL and filename; this is wire parity, not a live-provider run.

PDF admission consumes the upload in either direction with no events. Explicit
false appends with zero actions and no history projection, preserving completed
signed prefixes, native originals, rich extensions, tools, and their existing
projection restrictions. During an ordinary unsigned active LLM response in
`AssistantTextMode::Text`, it first commits the held assistant prefix and then
the file under one context lock, clearing only the text buffer and its spacing
flag. Empty buffers add no prefix; nonempty whitespace buffers add assistant
`""` after trimming. No context, timestamp, stopped receipt or turn frame is
emitted at upload. The real end retains the original turn ID, job generation,
start timestamp, invocation cause and markers, and reports only the remaining
suffix. No suffix still closes once with an empty stopped receipt and no
context/timestamp/turn frames. True/None converts only the new message, then uses
`append_with_frame` to append and prepare exactly one non-speculative upstream
context under one context lock. Construction or frame-projection failure leaves
history unchanged and the same ID retryable. Native `InlineData` can prevent
frame projection; rich extensions and direct tools can survive detailed framing
but remain subject to later service-conversion rejection.

Successful preparation commits history and records replay identity before emit;
emit or provider failure does not roll back the PDF or create a retry debt. A
same-ID replay, including false changed to true/None, never resends a context.
A fatal emit error can still latch the existing session-failure state.
This is bounded frame deduplication, not transport-command exactly-once delivery
or proof of provider acceptance. Python OpenAI's later `remove_invalid_file_message`
error cleanup is a separate, unimplemented provider-specific behavior.

Busy true/None PDF uploads still reject active/LLM-active turns and buffered text.
The explicit-false exception requires both an active turn and an active LLM
response in Text mode; Spoken and TTS-only turns remain excluded. Every PDF run
mode rejects buffered thought, thought-append state, user/bot speech, pending
playback/tools, owed/awaited inference, enabled realtime, and any local signed-
history tracker (including retained completed ownership). Gemini and Anthropic
native responses acquire that tracker even before any signature arrives.
Private continuation/eager/speculation/realtime/tool carriers remain rejected;
metadata never substitutes for that authority. The serialized-writer assumption
for image admission applies here too. Validation precedes the shared 512-ID
replay lookup; only a successful append (and frame preparation when requested)
records the ID. File construction finishes before any prefix mutation, so a
malformed upload leaves held text and its ID retryable. A successful same-ID
replay is revalidated but cannot flush a later suffix. This failure atomicity is
Rust's ingress contract, not a claim about Python's validation behavior.
Ordinary frames after End/Cancel/Stop are ignored, with the same
private-carrier validation order as image ingress. Legacy UserFile remains
unsupported. Appending URLs, other MIME types, custom options and busy uploads
outside this explicit-false Text case remain unsupported.

A passive `UserFileDetailed` with `append_to_context=None` or `Some(false)` is
silently consumed in clean idle state or the same ordinary unsigned active
Text+LLM response envelope. All independent thought, speech, playback, tools,
inference-debt, realtime, signed-owner and private-carrier guards remain in force.
The pinned Python file handler returns before flushing held assistant text, so
prefix + passive file + suffix stays one complete text at the normal turn end.
No context mutation, projection, encoding, inference, events or actions occur.
File source type, bytes/URL, MIME, filename, text, custom options and all run
values are ignored; this does not authorize retrieval or appending those payloads.
Passive IDs neither enter nor evict the successful 512-ID replay cache; repeated
passives and later same-ID valid active uploads retain normal admission semantics.
The public PythonJsonCodec decodes UserFileRawFrame into this Detailed variant;
the concrete passive bus fixture arrives UPSTREAM. RTVI send-file always sets
append true, including when run-immediately is false, and is not a passive
producer. Raw FrameData::UserFile remains unsupported. No image behavior changes
are implied.

`UserImage` has a separate encoded upload path: `append_to_context=Some(true)`
is required. `request` may be absent or a plain association whose
`function_name`, `tool_call_id` and `result_callback` are all None. Its
`image.image` bytes and `image.format` MIME use the same PNG/JPEG signature,
8 MiB encoded-byte and checked
data-URL limits above; dimensions are ignored. Nonempty accompanying text comes
first (whitespace is retained), followed by one image part in a user-role message.
User IDs and frame metadata do not become message names, tool results or provider
authority. The public RTVI `send-file` producer supplies this shape both with
`run_immediately=false` and with options omitted (the default is true). Encoded
PNG/JPEG pass-through still performs no pixel decoding or re-encoding.

The ordinary upload path requires an idle non-realtime assistant. A narrow
mid-turn alternative accepts explicit `run_llm=Some(false)` during an active
unsigned LLM response in `AssistantTextMode::Text`. It commits held assistant
text immediately before the user image under one context lock, then clears only
the text aggregation. Empty buffers add no assistant message; nonempty
whitespace-only buffers add an empty assistant message after the existing trim.
This emits no context, timestamp, stopped receipt or turn frame. The real end
keeps the original turn identity/start timestamp and emits only the remaining
suffix as its stopped/turn text; no suffix means one empty stopped event and no
context/timestamp/turn frames. Transcript marker bookkeeping is retained.

Both paths reject a local signed owner, buffered thought, user/bot speech,
pending playback/tool calls, and owed/awaited inference. Busy true/None uploads,
busy Spoken mode and TTS-only turns remain excluded. Realtime is unsupported. Gemini and Anthropic
service responses acquire a private continuation owner even before a signature
arrives, so this does not admit their in-progress responses. Requests with any
function/tool/callback field present remain unsupported. The bounded RGB/RGBX/YCbCr/L/CMYK/1
conversion below admits only those exact raw-pixel modes. User files use the separate
PDF path described above.
The assistant invokes no capture callback or tool settlement. Arbitrary
`image/*`, other raw-pixel modes and tool-associated image handling remain
outside this bounded Python parity slice.

### Bounded assistant-image display fallback

For ordinary application/public-codec `AssistantImage`, either original field
being absent or empty selects display bytes; the other original field is ignored.
Exact display `format=Some("RGB")`, `Some("RGBX")`, `Some("YCbCr")`, `Some("L")`, `Some("CMYK")` or `Some("1")`
uses bounded JPEG conversion.
Exact `image/png` or `image/jpeg` uses the existing encoded-byte validator: a
matching PNG/JPEG signature and 1..=8,388,608 selected bytes are required. Encoded
display bytes pass through unchanged; display dimensions are ignored and no
pixels are decoded. Signature checks are not complete image validation.
Both nonempty originals keep the encoded-original path even if the display is
invalid; invalid originals are never rescued by a valid display. Whitespace MIME
is nonempty and fails original validation. Unselected original storage is ignored;
conversely, truthy originals ignore display storage.

Pinned Python treats every case-sensitive `image/*` format as encoded pass-through
at this layer, including formats, empty data and signature mismatches the native
path rejects. Native exact PNG/JPEG MIME, signature and selected-byte caps are an
intentionally narrower admission policy. MIME aliases, case changes, parameters,
RGBA, LA and other raw modes are not added. There is no new provider image producer
or live-provider capability claim.

The shared RGB/RGBX/YCbCr/L/CMYK/1 encoder below preserves the same dimensions, selected
raw-buffer, required-prefix, output and checked data-URL limits. Existing modes
retain their JPEG settings; CMYK uses the distinct sampling documented below.
Ignored originals are not prevalidated or included in those selected-pixel/output caps;
this is not a total-frame, retained-storage or RSS budget. Encoding remains
synchronous under the assistant state lock, with the same blocking/cancellation
residuals described below, not a realtime or camera-rate guarantee.

Assistant guards remain before replay lookup, which remains before conversion.
A malformed replay of a successfully admitted ID is silent after those guards;
an unseen malformed frame fails without appending, flushing buffered text or
reserving its ID. Same-ID repair remains possible. This differs deliberately
from UserImage's validation-before-replay ordering. An accepted image appends
before previously buffered text and creates no action, event or inference.
Terminal/private-tool ordering, retained local signed/realtime rejection and
completed historical signed-prefix preservation remain unchanged, under the
same single serialized context-writer assumption.

The `raw_rgb_assistant_image_context` facade target uses direct application and
actual `PythonJsonCodec` frames, independent JPEG decoding/orientation samples,
and exact Gemini model-role `inlineData` request construction. The
`encoded_display_assistant_image_context` target adds complete PNG/JPEG fixtures,
public-codec falsey-original selection, exact encoded-byte preservation, selected
caps, atomic repair/replay and existing guard-order checks. Both retain silent
image admission and ordinary buffered-text completion. These are context/request
body tests, not Python/native JPEG byte equivalence, native provider image
production or live-provider acceptance. Existing UserImage/helper tests retain
common encoder bounds and capped-writer coverage; no new dependency or worker
lifecycle is introduced.

### Bounded RGB/RGBX/YCbCr/L/CMYK/1 user-image conversion

Only ordinary `UserImage` with exact `image.format=Some("RGB")`, `Some("RGBX")`,
`Some("YCbCr")`, `Some("L")`, `Some("CMYK")` or `Some("1")` and `append_to_context=Some(true)` adds packed,
top-down raw encoding. RGB and YCbCr use three bytes per pixel; RGBX uses four
and ignores the fourth byte. CMYK uses four ordered C, M, Y, K bytes, with K
retained as a real channel. Each L pixel is one grayscale byte. Exact `1` uses
MSB-first packed rows of `ceil(width/8)` bytes: set bits become 255, clear bits 0.
Unused low bits in each final row byte and bounded trailing bytes are ignored.
Rows are rounded independently, never continuously packed across row boundaries.
Exact YCbCr means interleaved Y, Cb, Cr in full-range JPEG color space, with no RGB conversion,
chroma swapping, limited-range expansion or full-frame repack. There is no
palette or alpha compositing. Configurable stride, extra row-padding bytes, BGR,
RGBA, LA, other packed-bit layouts, 16-bit pixels, NV12/I420/YUV422 and aliases
including `1;I`, `1;R`, `1;IR`, `1;8` and `01` are outside this contract.
The bytes do not carry a layout discriminator: a wrong layout with enough bytes
cannot be detected reliably, and bounded trailing bytes remain accepted below.
Width and height must each be positive and at most 65,500. Checked
`width*height*3` for RGB/YCbCr, `width*height*4` for RGBX/CMYK or `width*height` for L
and the full input Vec logical length are independently capped at 8,388,608 bytes.
For `1`, checked expanded `width*height` has its own 8,388,608-byte limit, even
when packed input is much smaller. Checked row rounding and packed required-prefix
arithmetic are bounded separately; full supplied input retains the same cap.
All geometry, expansion, packed-prefix, full-buffer and short-buffer checks occur
before the single fallible expanded-L allocation. Only valid row bits are filled;
no second full-image staging buffer is introduced. Expansion is released before
base64 construction. These are logical limits, not allocator-capacity/RSS bounds.
Short input rejects; bounded surplus after the required pixel prefix is ignored,
matching the pinned Python/Pillow raw-decoder shape. These are native resource
policies, not a claim that Pillow enforces the same byte caps.

The pinned `jpeg-encoder` 0.6.1 uses only its `std` feature and quality 75.
RGB, RGBX and YCbCr retain 4:2:0 (`F_2_2`); L uses its existing single-component
`Luma` encoder with the same configured sampling setting. Exact `1` expands once
to L8 and uses this same quality-75, configured `F_2_2` Luma path (one actual 1x1
component). Exact YCbCr selects `ColorType::Ycbcr`, which consumes the packed Y/Cb/Cr values directly. RGBX uses the encoder's
internal `ColorType::Rgba` four-byte layout, which reads only RGB and ignores X;
that enum name does not admit public RGBA or implement alpha compositing.
Exact CMYK selects `ColorType::Cmyk` without pre-inversion: the encoder inverts
all four channels for Adobe APP14 transform 0. It uses `F_1_1` for CMYK only,
so C/M/Y/K all have 1x1 sampling; reusing `F_2_2` would give K a different
sampling factor. `CmykAsYcck`, transform 2, alternate layouts and raw-mode aliases
such as `CMYK;I` are not admitted. Existing byte-per-pixel modes still avoid
full-frame repacking; only packed mode `1` introduces an expanded-L buffer.
[Pinned Pillow 12.3.0 JPEG dispatch](https://github.com/python-pillow/Pillow/blob/12.3.0/src/PIL/JpegImagePlugin.py#L620-L627)
accepts exact RGBX, YCbCr and CMYK and rejects RGBA/LA. Pillow passes YCbCr to
libjpeg as `JCS_YCbCr`, and CMYK uses `CMYK;I`/`JCS_CMYK` for Adobe conventions.
Native CMYK uses the encoder's existing default quantization tables (table 1 for
C/M/Y, table 0 for K); matching Pillow/libjpeg quantization or JPEG bytes is not
promised.
Progressive and optimized-Huffman modes remain explicitly disabled. No dependency or license change is required.
No caller metadata, ICC/EXIF, resizing, user-selected quality or provider I/O is
added.
A fallible writer checks the independent 8 MiB encoded-output logical limit
before every append and discards partial output on failure. Successful bytes
use the existing fixed `image/jpeg` data-URL helper and checked base64 bound.
[Third-party notices](../../third-party/README.md) include the required IJG
acknowledgement: this software is based in part on the work of the Independent
JPEG Group. The upstream encoder is unmodified.

Encoding and mode-1 expansion are synchronous under the assistant state mutex,
before taking the SharedContext lock. Input and output
caps do not provide a deadline, realtime/camera-rate throughput, cancellation
preemption, or Python `asyncio.to_thread` responsiveness. The encoder has internal
infallible allocations (including width-dependent MCU row buffers); the limits
are not caller Vec capacity, allocator behavior, queue/history retention,
projection-clone or process-RSS bounds. Large inputs can stall the reducer and
same-assistant operations. Packed mode `1` adds an expansion loop and a transient
full-image allocation; packed input, expanded L, JPEG output and encoder internals
can coexist. Fallible reservation does not bound allocator capacity or process RSS.
No off-thread worker lifecycle or cancellation preemption is introduced.
The same 8 MiB input cap permits up to 8,388,608 L pixels, roughly three times
the RGB/YCbCr pixel count; RGBX/CMYK are limited to 2,097,152 pixels.
Mode `1` has the same maximum 8,388,608 expanded pixels: 4096x2048 is admitted
from 1,048,576 packed bytes; 4096x2049 rejects by expansion size even though its
1,049,088-byte packed input fits. The expansion cap prevents an eightfold pixel
work increase from accepting 8 MiB of packed bits, but does not bound CPU time.
For CMYK, 2048x1024 requires exactly 8,388,608 bytes, while 2048x1025 exceeds
the required-prefix cap. Four-component encoding adds work without a CPU bound.
Three-byte modes
permit at most 2,796,202 pixels by byte arithmetic, so an exactly 8 MiB required
pixel prefix is impossible. For example, 2048x1365 requires 8,386,560 bytes and
fits; 2048x1366 requires 8,392,704 and rejects. An exactly capped supplied buffer
with bounded surplus is valid. Equal byte limits do not imply equal CPU work,
latency or temporary allocation behavior; no realtime qualification is claimed.

Authority, private-carrier, terminal, passive, realtime/signed-owner and state
admission still run before conversion. Passive append=None/false skips conversion
and consumes no replay ID. Explicit false-run remains context-only, including
only the existing ordinary Text+LLM busy-prefix exception. True/None remains
idle-only and uses the existing append-and-frame transaction. Conversion precedes
prefix/history mutation and replay lookup; failure leaves held text, history and
the ID retryable. Replayed RGB/RGBX/YCbCr/L/CMYK/1 still pays validation/encoding
(and for `1`, expansion) cost before the successful-ID no-op. This native failure atomicity is stronger than Python's
prefix-before-encoding order and is not claimed as exact failure-order parity.
Encoded PNG/JPEG pass-through retains its existing admission behavior.
`AssistantImage` uses the same encoder only under the distinct selection and
replay rules above.

The `raw_rgb_user_image_context` facade target exercises actual application RGB
capture through InputBackend/InputTransport, the produced UserImage, assistant,
prepared shared context and a Gemini request body. Callback Null proves queue
admission only; explicit End/drain/join and consumer output are checked separately.
A direct test-only vendored `jpeg-decoder` 0.3.2 independently checks SOF dimensions,
RGB24 output length, odd-size padding and orientation/color samples with a
fixture-specific 12/255 channel tolerance. It enables `platform_independent`,
not rayon, and does not expose the image-generation decoder or enable `images`.
Neither JPEG byte-equivalence to Pillow, universal lossy-pixel tolerance, native
LiveKit pixel decoding, hardware capture nor live provider acceptance is claimed.
The `raw_luma_image_context` target additionally covers direct AssistantImage
fallback and the real owned InputTransport L-to-UserImage path, with L8 JPEG
dimensions, asymmetric grayscale samples, bounded tails, geometry, selection,
replay and unchanged authority/state guards. Its 12/255 interior tolerance is
fixture-specific. The `raw_rgbx_image_context` target uses an odd-width,
non-square asymmetric 49x33 RGBX fixture through both public consumers, retaining
normal End/drain/join before owned-input behavior assertions. Independent RGB24
JPEG decoding checks dimensions, sequential coding and color interiors. Varying
ignored X bytes and repacking the same pixels as native RGB must preserve exact
native JPEG bytes; no equality with Pillow JPEG bytes is claimed. Controls retain
required-prefix/full-buffer caps, bounded tails, geometry, original selection,
atomic retry/replay and authority/state ordering. The original RGB/L tests,
encoded PNG/JPEG paths and capped-writer unit tests are unchanged.

The `raw_ycbcr_image_context` target adds exact YCbCr through direct AssistantImage
and the owned InputTransport request/capture/consumer lifecycle. A 49x33 fixture
has packed 147-byte rows and six asymmetric tiles. Independent RGB24 decoding
checks the full-range color-domain contract at tile interiors, using predeclared
per-channel tolerances of 12 for colored tiles and 4 for neutral 16/235 tiles.
These fixture-specific tolerances do not bound all images. The tests distinguish
RGB interpretation, swapped chroma, limited-range expansion and row-order/stride
errors; same-native bounded tails preserve the JPEG bytes. Native/rich projections
and local Gemini request bodies retain roles, text and data. The old RGBX negative
matrix changes only its now-supported YCbCr token to unsupported YCbCrX; it does
not attempt to reject a different byte layout relabeled with a supported token.
There is no new provider producer, hardware capture or live-provider claim.

The `raw_cmyk_image_context` target uses direct AssistantImage and the owned
InputTransport request/capture/consumer lifecycle with a 65x33 odd-width fixture.
Eight asymmetric tiles include white, K-black, K-gray and equal C/M/Y with
independent K changes. Vendored independent decoding requires CMYK32, sequential
DCT, dimensions, byte length and predeclared per-channel interior tolerance 8.
Header checks additionally require Adobe APP14 transform 0 and four 1x1 SOF
sampling factors. These fixture-specific limits are not universal pixel or
color-managed/ICC equivalence. Bounded native tails preserve exact JPEG bytes;
no Pillow/native JPEG equality or live-provider acceptance is claimed.
Controls retain exact/near input caps, independent prefix/full-buffer rejection,
short buffers, geometry, original truthiness/no-rescue, distinct replay order,
passive/authority/state guards and transaction retry. Only the RGBX negative
matrix's complete four-byte CMYK case becomes unsupported CMYKX. The YCbCr
matrix's CMYK case stays unchanged because its three-byte buffer is too short.

The `raw_mono_image_context` target adds exact packed `1` through both public
consumers with unchanged End/drain/join cleanup. Literal independent L rows at
widths 1/7/8/9/17 establish same-native JPEG equality, MSB order and per-row rounding;
this is not Pillow JPEG equality. Independent decoding checks sequential L8,
dimensions and pixel count. A separate 49x33 asymmetric block fixture uses a
predeclared interior tolerance of 8, without demanding lossless sharp patterns.
Controls check unused low bits, bounded tails, distinct expansion/full-input caps,
short rows, selection, replay/retry, passive/authority/state and transaction rules.
Only the four old L/RGBX/YCbCr/CMYK matrices' complete exact-`1` rejection tokens
become unsupported `1;8`; every other row remains unchanged, including YCbCr's
short CMYK case. No allocator-failure or naturally oversized JPEG claim is made;
the existing capped-writer units retain exact/over-limit coverage.

For a plain association, content and policy come exclusively from the UserImage
frame: `image.image`, `image.format`, `text`, `append_to_context` and `run_llm`.
A differing request text or append flag does not override them. The aggregator
has no InputTransport ledger token and does not verify capture provenance.
Directly constructed plain request-bearing frames prove only this content
contract; public fields or IDs do not confer capture ownership.

The public `InputIngress::request_image` plus keyed `push_video_frame(InputImage)`
path can produce this shape from application-supplied encoded PNG/JPEG bytes
or packed RGB24 labeled exactly `RGB`, RGB plus an ignored byte labeled exactly
`RGBX`, packed full-range Y/Cb/Cr triplets labeled exactly `YCbCr`, or packed
grayscale L8 labeled exactly `L`, ordinary C/M/Y/K quads labeled exactly `CMYK`,
or MSB-first packed monochrome rows labeled exactly `1`.
The source must already be registered and ready; the existing sampler and
private-tag/Queued ledger checks remain in force. Input preserves the request
association, strips its callback from the derived UserImage, and sets
`run_llm=None`, so an append=true owned image still requires the ordinary idle
inference path. Explicit false-run direct content tests do not imply Input emits false.
Generic capture `push` still rejects request-bearing UserImage, keyed capture
still accepts only InputImage, and the Python JSON codec still rejects nested
request identity. None of these checks is relaxed by content admission here.

A passive `UserImage` with `append_to_context=None` or `Some(false)` consumes no
image content and never requests inference, regardless of `run_llm`. Its request
must be absent or plain as above. A clean idle assistant leaves history unchanged;
an ordinary unsigned active LLM response in Text mode first commits only its held
assistant prefix and clears that text buffer and spacing flag. Empty buffers add
nothing; nonempty whitespace buffers commit an empty assistant message after
trimming. The turn remains open with the same identity, timestamp, job correlation
and transcript markers, and its real end reports the remaining suffix. No action,
event, context, timestamp, stopped receipt or turn frame is emitted by the passive
image itself. The same thought, speech, playback, tool, owed/awaited inference,
realtime, signed-owner and private-carrier gates remain in force; Spoken and
TTS-only busy turns are excluded. This selected state envelope does not reproduce
Python's reset of thought or deferred-inference flags in other states.

Passive handling skips image bytes, MIME and dimensions without encoding or
projecting existing history. It neither checks nor inserts the content replay
cache: repeated passive IDs can commit newly held prefixes, an earlier successful
content ID does not suppress that commit, and later append=true content with an
otherwise unused ID remains admissible. The keyed InputTransport all_frames
sampler actually produces request=None/append=None/run=None UserImage when no
request is queued. Producer dimension, media budget, source-readiness and queue
validation remain unchanged. Consumer acceptance is not permission to bypass
those ingress requirements. Ordinary terminal-image behavior and its existing
private-tool validation order remain unchanged.

An original request callback may be owned and completed once by InputTransport.
Its Null result acknowledges downstream queue admission only; it may precede an
assistant rejection and does not attest context consumption or provider receipt.
The assistant rejects a residual received callback without invoking it. Context
commit, upstream context emission and provider I/O remain distinct stages;
request-body conversion alone is not a provider-receipt test.

Accepted uploads are consumed in either direction without aggregation events.
Explicit `run_llm=Some(false)` is append-only and produces no actions. True or
None commits the same message and prepares exactly one non-speculative context
action upstream, using the existing append-and-frame transaction. This image-
specific None behavior does not change detailed message append/update semantics.
The assistant adds no interruption or output-configuration action; RTVI owns its
existing interruption/audio envelope. Nondefault audio-option timing through a
provider chain is not qualified by this slice.

Encoded payload validation precedes context mutation and replay lookup; only a
committed append records its ID in the same 512-ID cache. Failed validation or
context admission leaves both the held prefix and ID retryable. Replay lookup
precedes a mid-turn flush, so a duplicate cannot flush later suffix text.
Append-only admission does not require unrelated existing history to project;
normal turn-end projection retains its existing boundary. An accepted false-run upload retyped as true with the
same ID emits no inference action. Equal bytes with different IDs remain distinct.
This is bounded native frame replay suppression, not lifetime or RTVI-command
exactly-once delivery. Ordinary uploads after End/Cancel/Stop are ignored,
retaining existing private-carrier validation order. Completed signed prefixes
are preserved; the same one-serialized-writer limitation stated above applies.

Explicit run=false does not project existing history: a native `InlineData`
prefix may independently make `snapshot_rich()`/`frame_data()` unsupported while
an append succeeds and `snapshot()` preserves that prefix. True/None requires a
lossless projection before admitting the message; a projection failure leaves
context unchanged, consumes no replay ID, and creates no inference scheduling
debt. The same input ID may be retried after the caller repairs history, or in
false-run append-only mode. No new context helper or deferred scheduling path is
introduced.

A successful reducer return establishes a committed append and a prepared
upstream action. Actual pipeline emission happens afterward. A later push or
provider failure does not roll back the append or replay identity; replaying an
accepted ID is not delivery recovery. The real default RTVI pipeline fixture
observes one upstream context and projects that emitted context through the
Gemini request builder, preserving exact user text/MIME/base64 order. These
checks establish routing and request formation, not provider receipt, completed
inference, live-provider acceptance or a native visual-output round trip.

Detailed message append/update and local transforms honor optional `run_llm`;
`None` does not request a run. Tool-set and tool-choice changes update the actual
shared history. Source-specific rich thoughts retain the LLM identity and JSON
signature as context extensions, separately from delivered assistant text;
provider adapters that do not support those extensions report an explicit error.
Markers and `LlmAssistantPushAggregation` perform context commits. Vision response
boundaries and text use the same aggregation and replay handling as LLM responses.

Detailed function-call results honor property-level `run_llm` before frame-level
`run_llm`, final/intermediate status, group completion, and cancellation. Non-null
intermediate results default to inference when no other sibling in their group
is running; explicit `run_llm=true` overrides that group wait. Completed groups
and cancellation continuations are independent of unrelated background groups.
Response, user-turn, and playback fences still defer these continuations. Accepted
`on_context_updated` callbacks run in owned asynchronous tasks after the result
is written, never under the conversation lock. Duplicate/ignored results do not
call them. Completed task registrations are removed; End/Cancel cancels pending
callbacks. Synchronous callers receive `AggregationAction::ContextUpdated` and
must emit or otherwise execute the returned effect.

In spoken mode, response/text `skip_tts=true` is an explicit text-only delivery
path and does not await a nonexistent playback acknowledgement. Ordinary speech
still requires acknowledgement. Rich and legacy flush fences behave identically.

`SttMetadata.ttfs_p99_latency` updates the actual STT budget. Detailed VAD stop
frames supply their typed `stop_secs`; legacy VAD frames use configured
`vad_stop_duration`. A positive UNIX stop timestamp discounts transport delay
from the STT budget, while zero denotes an unspecified timestamp for deterministic
traces. Non-finite/negative VAD timings and invalid STT latencies are rejected.
The fallback default STT latency is zero and makes no provider-latency claim.

## Native Smart Turn integration

Construct a `SmartTurnAnalyzer` with an explicitly loaded native model, then use
`SmartTurnUserAggregator::new(context, user_config, analyzer)`. To retain the
assistant coordination of an existing pair, wrap `pair.user()` with
`SmartTurnUserAggregator::with_aggregator`. Configure it before pipeline setup,
and place only the wrapper in the user-aggregator position.

The wrapper has one managed async supervisor and the analyzer's bounded native
inference worker. Audio continues through the pipeline while a model result is
pending. VAD start/stop drives real audio buffering and model inference. A
COMPLETE result opens the turn gate; finalized text or the physical-stop P99
budget releases context. `wait_for_transcript=false` releases immediately on
COMPLETE. Model latency never starts a fresh STT wait, and an interim hypothesis
revokes prior finalization without extending the physical-stop deadline.

For model-gated no-VAD transcript fallback, the first accepted nonempty segment
starts the remaining STT inactivity wait, even when that segment is finalized.
A later nonempty finalized segment releases an already-established fallback;
a later nonfinalized segment refreshes the wait. With an 800 ms STT budget and
explicitly zero VAD stop duration, final segments at 0 and 100 ms commit together
once at 100 ms, rather than restarting the wait until 900 ms. This matches the
public finalized-segment path of Python `TurnAnalyzerUserTurnStopStrategy` at
`af9f9da9` and native explicit `turn_analyzer(true)`. The Smart Turn wrapper sets
the scalar speech floor to zero; standalone scalar configurations still honor
their nonzero speech floor. Resumed VAD requires a current acoustic verdict, and
each later user turn establishes its own initial fallback. The focused
`final_transcript_fallback` tests use no audio or model execution; this bounded
timing compatibility makes no claim about deployed call impact or Python's
background-task lifecycle.

Resumed speech cancels and invalidates stale inference while preserving the
utterance buffer for reanalysis. Silence completion, no-VAD transcript fallback,
external stops, mute, watchdog, End, and Cancel all have explicit lifecycle
handling. End cancels incomplete model work and flushes accepted final text;
Cancel does not request a new response. Cleanup joins the native model worker.
Model timing/probability metadata is emitted only for actual model predictions;
silence and inference timeouts do not fabricate probabilities. Model parameters
travel in `SpeechControlParams` metadata because core `BaseTurnParams` is empty.

## User-idle detection

Set `UserAggregatorConfig::user_idle_timeout` to a nonzero `Duration` and
register `on_user_turn_idle` before starting the pipeline. The default is zero.
The timer starts when the bot stops speaking, never simply because a user turn
ended. User or bot speech cancels the timer. Function-call declarations suppress
it, including when they arrive before the bot-stop frame. A matching final
result or cancellation releases that call without assuming the bot has responded.
Accepted typed user input also closes the waiting window until the bot responds.

The native guard intentionally corrects the source controller's unkeyed counter:
it matches call IDs, names, and result arguments, ignores intermediate and
unmatched results, and deduplicates declarations. Empty declaration batches do
not imply activity. Declarations require nonempty IDs/names and object arguments,
matching the native assistant contract; conflicting duplicates are rejected.
At most 512 calls may remain pending; exceeding that limit rejects the whole
batch without evicting active guards. The last 512 completed IDs suppress replay.
Older completed IDs fall outside this bounded window, so callers must keep IDs
unique and must not replay old declarations. Well-formed source sequences retain
their original waiting-window behavior.

```rust
use pipecat_core::{Direction, Frame, FrameData, TtsSpeakFrame};
use pipecat_turns::{LLMUserAggregator, SharedContext, UserAggregatorConfig};
use std::time::Duration;

# fn example() -> Result<(), pipecat_core::ProcessorError> {
let user = LLMUserAggregator::new(SharedContext::default(), UserAggregatorConfig {
    user_idle_timeout: Duration::from_secs(5),
    ..Default::default()
});
user.on_user_turn_idle(|ctx| async move {
    ctx.push(
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("Are you still there?"))),
        Direction::Downstream,
    ).await
})?;
# Ok(())
# }
```

Each waiting window emits once. A new bot-stop frame or explicit
`wait_for_user(&ProcessorContext)` re-arms it. The aggregator also re-arms after
an empty turn closes without inference; it does not fabricate a transcript or
recovery prompt. Re-arming is suppressed while either speaker or a function
call is active. `waiting_for_user`, `function_calls_in_progress`, and
`idle_deadline` expose this state independently of the turn controller.

`UserIdleTimeoutUpdate { timeout }` uses seconds and restarts the timer from
the update time while already waiting, including when detection was disabled.
Zero or negative values disable detection. Non-finite or unrepresentable values
are rejected. Updating while awaiting a bot response never opens a new waiting
window. `wait_for_user_at` and `advance` support deterministic integration;
expiration returns `AggregationEvent::UserTurnIdle` without running handlers.

The runtime owns one idle supervisor after the first armed deadline. It sleeps
without polling when no timer is armed, reuses its task across updates, and runs
handlers outside conversation locks. Handlers may push frames or re-arm idle
detection; keep them short because handlers run in order. Nonfatal handler errors
emit an upstream error and allow later handlers and re-armed timers to continue;
fatal errors fail the pipeline. End, Stop, Cancel,
and cleanup cancel the timer and any pending handler. Include this task in
`max_background_tasks` alongside the independent turn/Smart Turn supervisor.
`SmartTurnUserAggregator::user()` exposes the same configuration and handlers.

## Cascade context summarization

Manual `LlmSummarizeContext(None)` frames work on every assistant aggregator.
Configure defaults and opt into response-start thresholds with
`LLMAssistantAggregator::with_summarization(ContextSummarizationConfig)`.
A manual frame may carry a per-request `LlmContextSummaryConfig`, including the
prompt, token budget, retained-message count, timeout, and `{summary}` template.
Successful summaries are user messages (`Conversation summary: {summary}` by
default). Flows' developer-message summary format is a separate contract.

Provide actual inference in either of two ways:

- Attach `SummaryLlm::new(LlmClient, CompletionRequest)` with
  `with_summary_llm` for a dedicated out-of-band native client
- Place `LLMContextSummaryProcessor::new(SummaryLlm)` before the assistant.
  It consumes upstream `LlmContextSummaryRequest` frames and returns correlated
  `LlmContextSummaryResult` frames downstream. The client may use the same model
  and configuration as the conversation LLM. Normal conversation frames pass
  unchanged. Other native responders can implement the same typed contract

The two assistant builders may be used in either order. A wire-level processor
reference in `LlmContextSummaryConfig::llm` is not executable and is rejected;
use the native client builder instead. Unconfigured request/result inference
reports a nonfatal timeout rather than inventing a summary.

Selection preserves all leading system messages and the requested recent suffix.
Mid-conversation system injections can be summarized. Tool groups are indivisible:
missing/pending results, asynchronous running markers, nonadjacent results,
orphan results, duplicate IDs, and overlapping groups constrain the boundary.
A reverse interval sweep is O(n log n). All leading system messages are preserved
intentionally; Python currently preserves only the first one.

Each admitted request owns an exact prefix snapshot and context lineage. Results
must match its identity and selected range. Replacing/transforming context, even
with equal values, invalidates old results. Appended messages survive compression;
apply-time boundary validation also protects new references to older tool calls.
Interruption, End, Stop, Cancel and cleanup invalidate pending results. A summary
failure never discards an unrelated live assistant response. Replacement/transform
frames also cancel the responder's obsolete HTTP calls immediately. Direct
`SharedContext::replace` calls invalidate result application and a subsequent
controller request, but cannot notify an independent responder; its old call may
occupy an admission slot until completion or timeout. Use replacement frames when
immediate cancellation across the request/result route is required.

Retained rich messages, extensions, native message distinctions, tools and tool
choice remain intact. Original native messages are remapped by index, including
forms with identical universal projections. Summary-only snapshots preserve such
native distinctions in a `pipecat_native_message` extension. The JSON transcript
includes all message fields and multimodal descriptors; this does not claim visual
or acoustic interpretation of embedded media. Executable tools are omitted from
summary requests. Provider settings that override the final tool-free request or
token budget are rejected.

The assistant reuses one owned supervisor for dedicated inference or the external
result deadline. It runs no inference inline in frame processing. The separate
responder also reuses one owned supervisor and defaults to one in-flight call;
`with_max_in_flight` sets its admission bound. In addition to the native client's bounded streaming transport, full
snapshot transcripts/prompts are limited to 16 MiB, summaries/rendered templates
to 1 MiB, request identities to 512 bytes, and timeouts to one day. Invalid input,
empty/truncated/filtered/tool-producing responses, transport failures and timeouts
leave history unchanged and permit a later request. No automatic retry contacts
an LLM after an error.

`AggregationEvent::SummaryApplied` reports original, new, summarized and preserved
message counts through the deterministic interface. `process_frame` only returns
request/result effects; callers using that interface own dispatch and deadlines.
Use the pipeline adapter for automatic inference/task lifecycle ownership.

The runnable native example requires an explicitly configured model and `--run`:

```sh
cargo run -p pipecat-turns --example context_summary -- --help
PIPECAT_SUMMARY_MODEL=your-local-model cargo run -p pipecat-turns \
  --example context_summary -- --run --dedicated --auto
```

The default provider is local Ollama. `PIPECAT_SUMMARY_PROVIDER` also accepts
`openai`, `anthropic` and `gemini`; remote providers require
`PIPECAT_SUMMARY_API_KEY`. `PIPECAT_SUMMARY_ENDPOINT` overrides the exact endpoint.
Without `--run`, the example prints usage and sends no request. Omit `--dedicated`
to exercise the responder route; omit `--auto` to use a manual trigger.

`tests/context_summary.rs` follows the selection and controller assertions in
`test_context_summarization.py` and `test_llm_context_summarizer.py`.
`tests/context_summary_runtime.rs` uses real native HTTP/SSE clients against
OpenAI, Anthropic and Gemini loopback fixtures, both routing modes, live-turn
concurrency, cancellation, teardown, provider failures and owned-task limits. `tests/review_context_summary.rs` adds
independent rich-identity, race and admission regressions.
`tests/review_summary_supervisors.rs` covers multi-request capacity, out-of-order
bidirectional correlation, replacement cancellation, repeated interruption/reuse,
and all terminal paths for concurrent HTTP calls. These tests use no
credentials, external providers or Python runtime. The four source examples under
`examples/context-summarization/` map to manual/automatic, provider selection and
dedicated-client modes; this native example does not reproduce their audio
transports, STT, TTS or live-call setup.

## Source and validation mapping

Golden assertions are traced in `tests/golden.rs` to:

- [`llm_response_universal.py`](../../../src/pipecat/processors/aggregators/llm_response_universal.py)
  and [`test_context_aggregators_universal.py`](../../../tests/test_context_aggregators_universal.py)
  for user/assistant context, spacing, mute, session-end, interruptions, tools,
  deferred results, and multiple turns
- [`user_turn_controller.py`](../../../src/pipecat/turns/user_turn_controller.py)
  and [`test_user_turn_controller.py`](../../../tests/test_user_turn_controller.py)
  for no duplicate starts/stops, stale completion guards, external signals, and
  late-transcript isolation
- [`speech_timeout_user_turn_stop_strategy.py`](../../../src/pipecat/turns/user_stop/speech_timeout_user_turn_stop_strategy.py)
  for the two-timer policy and STT finalization behavior
- [`async_tool_messages.py`](../../../src/pipecat/processors/aggregators/async_tool_messages.py)
  for the structured asynchronous tool messages
- [`test_empty_user_turn.py`](../../../tests/test_empty_user_turn.py) for the
  default interrupted recovery, optional idle prompt, retry bound, pending-tool
  exclusion, no-invented-transcript boundary and idle re-arming
- [`user_idle_controller.py`](../../../src/pipecat/turns/user_idle_controller.py)
  and [`test_user_idle_controller.py`](../../../tests/test_user_idle_controller.py)
  for waiting windows, interruption/function-call suppression, timeout updates,
  and explicit re-arming (`tests/user_idle.rs`, `tests/user_idle_runtime.rs`)

`tests/runtime.rs` uses a paused Tokio clock, exercises owned timer emission and
End/Cancel cleanup, and streams 200 transcript segments with a one-task limit.
These deterministic tests are based on source assertions. `tests/rich_frames.rs`
checks rich-frame semantics and callback lifecycle. `tests/smart_turn_runtime.rs`
uses controlled native predictors for result ordering, cancellation, and deadlines.
`tests/smart_turn_onnx_pipeline.rs` runs the actual tracked ONNX model and an audio
fixture through a native pipeline with synthetic STT/LLM processors. These tests
do not contact a live provider or claim microphone/speaker validation.

Run from `rust/`:

```sh
cargo test -p pipecat-turns --locked --offline
cargo clippy -p pipecat-turns --all-targets --locked --offline -- -D warnings
```

## Explicit compatibility scope and remaining work

This is a core cascade-turn implementation, not certification of the complete
Python modules or test suites. The migration inventory remains open. Create a fresh pair for each session;
instances closed by End, Stop, or Cancel are not restartable.

- The default remains the documented speech-timeout strategy; select the Smart
  Turn wrapper explicitly for native neural turn detection. Silero VAD is a
  separate `pipecat-vad` processor. LLM verdict strategies, speculative/eager
  inference and arbitrary strategy plugins have separate explicit native contracts.
  Supported service recommendations and paired realtime behavior are documented in
  [REALTIME.md](REALTIME.md); unsupported descriptors fail explicitly
- Explicit mute and the configurable native mute policies below are supported.
  Empty-turn recovery applies only to cascade mode; paired realtime activation
  disables it
- Raw image modes other than bounded RGB/RGBX/YCbCr/L/CMYK/1 and non-PDF files need an explicit
  encoder; encoded rich messages are supported. Full Python callback/event parity and
  provider-specific realtime speech-to-speech qualification
  remain outside this implementation
- Grouped tool results and active-response deferrals are coalesced. Arbitrary
  adjacent ungrouped append/result frames are not coalesced by queue inspection
- An actual executor must emit terminal results/cancellation and must honour
  `cancel_on_interruption`; recording cancellation does not stop external effects
- False, zero, empty string, and empty array tool results retain their JSON value;
  only null maps to `COMPLETED`. This deliberately avoids the Python truthiness
  rule that can erase legitimate falsy results
- Replay caches retain 512 identities. Providers/transports must keep stable,
  unique identifiers and enforce their own generation/lifetime checks
- Client playback acknowledgements and partial-word framing are transport
  contracts, not proof of acoustic playback. No live microphone/speaker claim
  follows from these tests

## Composable start and mute policies

`UserTurnConfig::start_strategies` is opt-in. `None` retains the native VAD,
transcription, and already-announced external-start defaults. An explicit
`UserTurnStartStrategies` list runs in order. Its default list is VAD followed
by transcription; an explicitly empty list disables starts. Stop policy remains
in the existing controller, including silence/STT budgets, watchdog and optional
native SmartTurn gating.

```rust
use pipecat_turns::{UserAggregatorConfig, UserMuteStrategy, UserTurnConfig,
    UserTurnStartStrategies, UserTurnStartStrategy};
use std::time::Duration;

let config = UserAggregatorConfig {
    turn: UserTurnConfig {
        start_strategies: Some(UserTurnStartStrategies::new(vec![
            UserTurnStartStrategy::wake_phrase(
                vec!["hey pipecat".into()], Duration::from_secs(10), false,
            )?,
            UserTurnStartStrategy::vad(),
            UserTurnStartStrategy::transcription(true),
        ])?),
        ..Default::default()
    },
    mute_strategies: vec![UserMuteStrategy::FirstSpeech, UserMuteStrategy::FunctionCall],
    ..Default::default()
};
# Ok::<(), pipecat_core::ProcessorError>(())
```

- `vad()`, `transcription(use_interim)`, `external()`, and
  `min_words(min_words, use_interim)` implement the corresponding source starts
- Minimum words counts each hypothesis independently while the bot speaks; when
  the bot is silent one word suffices. Rejected hypotheses reset accumulated
  text. A later fallback may still decide to start on that same frame
- A wake gate blocks later detectors until a final transcription matches. It
  accumulates at most 250 Unicode characters across final frames. Rejected final
  text is removed from aggregation, and only the matching frame is committed
- Wake timeout mode refreshes on transcripts, VAD starts, speaking activity, and
  a new turn. `single_activation=true` preserves the source's fixed keepalive
  timeout: turn stop does not immediately relock the gate
- Source-style `re.IGNORECASE` matching uses bounded native regexes with explicit
  letters/numbers/underscore word boundaries. Combining marks, connector
  punctuation other than underscore, and join controls are stripped; raw Rust
  regex `\w`/`\b` would differ. Whitespace includes Python's U+001C..U+001F,
  and dotted/dotless I are normalized for Python-style case equivalence. Unicode
  character tables come from the pinned Rust toolchain/regex versions, so newly
  assigned characters can differ from a Python runtime with another Unicode version
- Configuration bounds are 64 strategies, 64 total wake phrases, 250 characters
  per phrase and 64,000 total phrase bytes. Each compiled regex and DFA cache is
  limited to 64 KiB. These reject pathological configurations rather than
  creating an unbounded detector
- An external resolver consumes proposals even when an earlier wake gate blocks
  the decision. Announced speaking frames retain their original identity and
  direction; adopting one emits no duplicate boundary or interruption. Stop
  proposal resolution remains controlled by `resolve_proposals`
- `Always` mutes during every bot speech. `FirstSpeech` protects only the first
  speech. `UntilFirstBotComplete` also protects the opening wait and releases
  after an opening error before any bot audio. `FunctionCall` protects pending
  declared tools. Every policy sees every non-lifecycle frame, and results combine
  with OR, so one policy cannot hide lifecycle changes from another
- Policy admission uses the mute state at frame arrival, matching the source:
  transitions are broadcast before the triggering frame, and affect subsequent
  user frames. `Start` is forwarded before policy evaluation. To mute the first
  possible input frame itself, also set `initially_muted` and explicitly clear
  that manual mute when ready. Manual mute and policy mute are independent OR
  layers; an explicit unmute cannot disable a still-active policy
- Function-call mute deliberately preserves the stricter native tool contract:
  512 pending IDs, 512 recent completed IDs, atomic declarations, matching
  ID/name/arguments, and final-result or matching-cancel release. Intermediate,
  unknown, mismatched, or replayed results cannot silently unmute pending tools
- `AggregationEvent::UserStartStrategy` exposes wake detection/expiry, and
  `UserMuteChanged` exposes policy transitions. Deterministic callers drive
  `process_at`/`advance`; both ordinary and SmartTurn pipeline processors own
  their wake deadlines without extra per-strategy tasks. The ordinary supervisor
  remains dormant between turns to avoid task churn at low runtime task limits

Start-chain lifecycle methods and standalone controller `reset` permit fresh
sessions; aggregator cleanup cancels deadlines and clears strategy state.
Policy mute closes a current turn using the existing native mute rule: accepted
final text is preserved without immediate inference. SmartTurn invalidates any
in-flight model result and clears buffered audio at the same boundary.

`tests/fixtures/user_strategies.json` pins source provenance and manually mapped
assertions, not a Python execution baseline. Deterministic, paused-time runtime,
and independent review tests cover composition, proposals, context rejection,
tool finality, timer ownership, and SmartTurn integration. Proprietary Krisp
classification and arbitrary strategy plugins are not implemented here.
Service recommendations are limited to the native descriptors in [REALTIME.md](REALTIME.md). The bounded
unsigned LLM marker strategy and its external-completion finalizer are described
in [COMPLETION.md](COMPLETION.md); conversational inference and tool admission
remain caller-owned.

## Composable stop policies

`UserTurnConfig::stop_strategies` accepts an ordered `UserTurnStopStrategies`
list. `None` selects the native scalar controller. An empty list leaves the
inactivity watchdog and terminal/mute boundaries enabled.

For scalar non-model speech timeout, `wait_for_transcript=false` removes the
nonempty-text prerequisite, while the speech-silence floor and remaining STT
safety budget both apply. This is a deliberate timing alignment with native
`speech_timeout(duration, false)` and the executable Python explicit
`SpeechTimeoutUserTurnStopStrategy` at `af9f9da9`. For example, an 800 ms STT
budget minus 200 ms of observed VAD silence requires 600 ms after VAD stop,
even with a 100 ms speech policy and no transcript. Zero or exhausted STT budget
keeps the speech-policy fast path; finalized text short-circuits only STT.

The pinned Python default selects a turn analyzer, not SpeechTimeout; this does
not equate the two libraries' default strategy selection. Python's
transcript-optional prose omits the nonzero-STT distinction; this compatibility
choice follows its explicit speech-timeout implementation. Native model-gated
`wait_for_transcript=false` still releases immediately on COMPLETE, and external
stop, watchdog, resumed-speech, mute, and terminal boundaries retain their
existing behavior.
Explicit lists support:

- `speech_timeout(duration, wait_for_transcript)`: independent speech-silence
  and STT waits, finalized-text short circuit, interim invalidation, and
  transcript-only inactivity fallback
- `turn_analyzer(wait_for_transcript)`: an actual Smart Turn verdict plus the
  physical-stop STT budget; transcript-only input has a documented non-model
  fallback. Configure an analyzer policy when wrapping an explicit list with
  `SmartTurnUserAggregator`; unrelated policies may coexist in the same list
- `external(duration, wait_for_transcript)`: adopted external stops and consumed
  stop proposals, with late/consecutive final-transcript inactivity handling
- `external_completion()`: finalization on `UserTurnInferenceCompleted`, ignoring
  completions while speech is active

Each policy owns deterministic one-shot deadlines. The ordinary aggregator's
single supervised task drives the earliest deadline; the Smart Turn wrapper
uses its existing supervisor. A turn boundary clears every policy, even those
that did not end the turn. End/Stop draining, Cancel, mute and resumed speech
invalidate obsolete work. Timers never poll while dormant or finalize audibly
active speech. The watchdog is rearmed when an external verdict is requested.

With explicit stop policies, an adopted external stop is held while its semantic
turn remains open. It is forwarded once when that policy finalizes; another
completion policy emits its own final boundary instead. This prevents a physical
external stop from releasing a paired assistant's tool continuation prematurely.

### Deferred external-verdict notifications

`strategy.deferred()` requests an out-of-band decision instead of finalizing.
This native API deliberately has a narrower contract than Python's `deferred`:
accepted final text remains uncommitted, and **no conversational LLM inference,
output, or tool execution is started by the notification**. It is not speculative
execution or Python's automatic marker-filtered conversational inference route.

`AggregationEvent::UserTurnInferenceRequested` carries the accepted final text,
request ID, detector index and reason. Deterministic callers inspect this event
and use `complete_inference_at(id, now)` to apply a verdict. Runtime callers may
register `on_user_turn_inference_requested` to enqueue a notification to their
own external classifier. These synchronous handlers run outside conversation
locks; they must be fast and must not run model inference or block. They do not
create additional tasks. Return the request's `completion_frame()` through the
same pipeline input as speech after an actual external COMPLETE verdict. An
incomplete verdict needs no frame: the open turn awaits more evidence or its
watchdog. Keep the classifier's own calls bounded and cancel obsolete requests
in its owner; notification IDs prevent stale results from changing this turn.

Completion admits the usual final user context once, with the existing paired
assistant, tools, summary and playback invariants. New final/interim evidence,
resumed speech, reset, mute or shutdown invalidates previous IDs. Repeated
completions do nothing. Callers using bare `UserTurnInferenceCompleted` retain
source-compatible uncorrelated semantics: only the current speaking guard is
available, so delayed verdict producers should use correlated frames. Runtime
Smart Turn completions should use the pipeline frame route, which also clears
or cancels native analyzer work; the deterministic direct-completion method
has no access to a wrapper's audio worker.

```rust
use pipecat_turns::{UserTurnConfig, UserTurnStopStrategies, UserTurnStopStrategy};
use std::time::Duration;
# fn example() -> Result<(), pipecat_core::ProcessorError> {
let turn = UserTurnConfig {
    stop_strategies: Some(UserTurnStopStrategies::new(vec![
        UserTurnStopStrategy::speech_timeout(Duration::from_millis(600), true)
            .deferred(),
        UserTurnStopStrategy::external_completion(),
    ])?),
    ..Default::default()
};
# let _ = turn;
# Ok(())
# }
```

## Empty-turn recovery

`UserAggregatorConfig::empty_user_turn` defaults to
`Some(EmptyUserTurnConfig::default())`: one consecutive recovery with
`DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT`, no idle prompt. Set it to `None`
to leave all empty turns unanswered. Each optional prompt is a developer message;
`None` or an empty string disables that case. Set `max_consecutive_recoveries`
to customize the bound, or zero to disable recovery.

A turn counts as interrupted when its accepted start enables interruption and
the bot was not waiting for the user. This includes a pending response and the
opening wait before the first bot response. The classification is latched at
start: an output-side bot stop during user speech does not turn it into an idle
case. Already-announced external starts retain their native non-interrupting
contract, so they select the idle case.

Only nonempty final transcript content committed by the aggregator resets the
budget. Interim hypotheses, whitespace, rejected start-gate text, replayed
transcripts, context append/update frames, typed input and assistant responses
do not reset it. A skipped recovery consumes no budget. Muted turns, pending
declared tools, terminal End/Stop/Cancel boundaries and runtime shutdown never
start recovery. Matching final tool results or cancellation release the pending
guard; intermediate, mismatched and replayed tool events retain the existing
strict native contract.

Recovery uses the ordinary paired context-snapshot path: delivered assistant
text is sealed at interruption before the developer prompt, already-completed
tool results share the recovery's single request, and a later tool result still
owes its own continuation. No user message is invented. Without recovery, an
empty turn re-arms eligible idle detection; with recovery, the bot's response is
pending and idle remains unarmed. The existing ordinary or Smart Turn supervisor
owns timing, so recovery adds no tasks and does not bypass a model gate.

`tests/fixtures/empty_user_turn.json` pins source revision, Git blob IDs and
SHA-256 hashes, and manually maps the twelve cascade cases in
`tests/test_empty_user_turn.py`. Native deterministic, paused-time runtime and
controlled Smart Turn tests cover context ordering, budget reset, muted and
terminal turns, tool lifecycle/coalescing, idle windows and single-task reuse.
No Cargo test starts Python. Paired realtime aggregation disables recovery as
documented in [REALTIME.md](REALTIME.md); it is not provider qualification.

### Remaining strategy scope

The bounded unsigned LLM-completion marker parser and short/long automatic
re-prompts are documented in [COMPLETION.md](COMPLETION.md). Eager STT speculation,
exact/normalized matching, provisional-context output gates and native tool-effect
admission are implemented for the supported `pipecat-services::LlmProcessor`
topology in [EAGER.md](EAGER.md). Signed completion-marker filtering and eager
integration with Responses, realtime or separate provider-specific inference
processors remain unsupported. No synthetic classifier stands in for a real
verdict. Proprietary Krisp start/analyzer implementations and opaque
service-recommended strategy descriptors remain unsupported.


### Native signed history

The assistant consumes typed native provider continuation carriers. Signed
thinking is independent of delivered text: Spoken mode includes only receipt-backed
heard text, never generated but unheard text. Delivery must match an exact
provider prefix, allowing only existing trimming at committed chunk boundaries.
Interior whitespace transformations, ambiguous context changes and mismatched
provider identities fail explicitly. Rich clones preserve signatures.

Closed-prefix checkpoints preserve completed signed thoughts after a later
signature is interrupted, without admitting pending tools. Completed tool
continuation waits for pending playback before requesting the next response.
Receipt validation precedes identity consumption, permitting corrected retries.
Replacement validates the owned context suffix before mutation. Cancellation of
invalid pending delivery keeps the last valid committed history, discards the
invalid buffer, closes the scope and permits subsequent responses.

Signed scopes bound delivered chunks to 4096 and 8 MiB, playback IDs to 4096,
and closed response IDs to 512. Signatures are opaque provider data validated
for structure and bounds, not locally authenticated cryptography.

Gemini-specific Part metadata, receipt-backed truncation, strict terminal policy,
and foreign-only unsigned-history migration are described in
[the Gemini continuation contract](../pipecat-services/GEMINI.md).

## Native terminal ownership

`LLMAssistantAggregator` consumes validated `NativeToolCallOutcome` receipts at
its history boundary. The receipt seals call identity, interruption policy,
result/error/inference policy, original outcome identity, and optional complete
batch membership. Executor Started/InProgress declarations carry that same sealed batch descriptor,
establishing native active ownership before any terminal. Both declarations and
terminal frames retain their process-local proof through any provider relay; ordinary frame serialization rejects it until consumption.
A provider can use `Frame::project_native_tool_timeout_as_cancel()` only for a
validated Failed/TimedOut receipt. The projection preserves the original receipt
and permits exactly the original name/ID plus cancellation with `run_llm=true`.

A native terminal can reconstruct missing declarations and all admitted siblings.
The last sibling releases grouped inference. Results admitted before interruption
can replace only this aggregator's own provisional legacy cancellation; committed native
terminals never change. Publication identity makes duplicate clones idempotent,
including concurrent calls and changes to the public frame ID. Full batch
membership and policy must agree across all siblings. Signed declaration identity
survives response end so reconstruction preserves the original signed reasoning.
Spoken mode still waits for actual playback confirmation before inference.

Interruption keeps native-owned calls pending until an actual terminal receipt
arrives. Those active identities never evict to admit other work. Either native
declaration establishes ownership if the other is filtered; when both are lost,
the self-contained terminal reconstructs the same batch.

Raw upstream cancellation is a request and is forwarded to the executor. Raw
cancellation cannot settle a call already owned by a native batch. Legacy result,
detailed-result callbacks, and downstream legacy cancellation keep their existing
contracts. An application that validates and explicitly consumes a native receipt
and declaration batch before modifying an ordinary result opts into that legacy contract and forfeits
native restoration, immutability, and terminal-ownership guarantees downstream.

Native bookkeeping holds at most 1,024 calls and an 8 MiB conservative encoded
budget. Closed-group terminal markers retain the latest 512 entries; a live
batch's completed siblings remain until that group closes, within the same hard
call/byte bounds. Large payload Arcs can evict independently of compact sealed
publication IDs. Provisional legacy cancellations have a separate 1,024-entry/8 MiB window.
Native restoration over a legacy provisional cancellation requires that marker
to remain in this window; expired authority fails closed. Ordinary and
signed active declarations each allow 1,024 entries/8 MiB and reject new work
rather than evict unsettled authority. No bookkeeping retains a
processor or runtime. End, Stop, Cancel, and cleanup clear it. Conversation messages
are not removed by metadata eviction. Outside the identity window, a receipt
cannot adopt or overwrite an existing conversation call. Explicit context
replacement can admit a new call identity after old bookkeeping expires.

Executor admission accounts for the default consumer's policy-plus-batch storage
before callback effects. Independently saturated consumers still fail closed.
These are encoded-retention budgets, not process RSS bounds. A terminal delivered
after the assistant closes is ignored; first-hop executor admission does not
promise durable or end-to-end consumption. Cancellation never proves rollback of
external effects.

Tool metadata uses separate component limits, including copied ID keys/queues:
active pending policies and both legacy tombstone caches have explicit byte/count
bounds. Closed cancelled groups coalesce to one inference bit instead of retaining
labels during a long speech wait. Signed history's validation mirror is separately
bounded to 4,096 entries/32 MiB, measured before history mutation, with no eviction
of signed authority. Whole native batches preflight both pending ownership and
combined signed reconstruction/result additions. See [the complete state ownership
and budget table](TOOL_HISTORY.md); there is no global 8 MiB or RSS-bound claim.

## Bounded assistant completion receiver

Register `assistant.subscribe_stopped(AssistantStoppedConfig::default())` before
the first turn. `receiver.recv()` returns a non-cloneable record lease containing
immutable committed text, original start timestamp, interruption status, session
and turn identity, and an optional immutable job binding. Admission is ordered
after context/timestamp and before turn feedback, including paired interruption
and explicitly adapted playback receipts. No callback dispatcher task is created.

`handle_next` runs one cooperative, deadline-bounded handler in the caller's owned
future. Inspect `stopped_stats()` for payload-free resource and outcome counters.
End/Stop/Cancel admit their final record then close the stream; queued records
remain caller-drainable. Cancel never spawns a new async callback. A replacement
job requires a fresh assistant session. Full ordering, budgets and limits are in
[assistant completion ownership](../../docs/assistant-stopped.md).
