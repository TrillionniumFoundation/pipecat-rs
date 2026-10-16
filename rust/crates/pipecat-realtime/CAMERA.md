# Raw camera input

The camera path accepts downstream `InputImage` and passive `UserImage` frames
containing bounded packed RGB pixels. An `InputImage` retains its original frame
envelope when forwarded. A passive `UserImage` is consumed at the service after
camera handling, including pause or sampling suppression, so the default paired
assistant does not interpret it as a rich image-context operation. Upstream image
frames are forwarded without uploading; output and assistant image families are
not camera input. Capture ingress must have video enabled; this feature does not
provide camera capture, decoding, resizing, or playback.

Passive user images may carry ordinary capture association, but user text,
speaker identity, and that association are not serialized as image prompt
context. Images requesting `append_to_context=true` or `run_llm=true`, or carrying
function/tool/callback request authority, are rejected before pause, sampling,
encoding, or socket admission. This path does not complete an image request,
produce a tool receipt, or implement rich multimodal shared-context reconciliation.

Each accepted sample becomes one user `conversation.item.create` with one
`input_image`, a JPEG data URI, and detail `auto`. It never schedules
`response.create`; a separate normal turn or explicit response trigger remains
responsible for inference. Low/high detail controls are outside this bounded
adapter scope. Local command acceptance and socket flush are not server
acknowledgement. If an application needs a camera item to precede a response, it
must use ordering actually preserved by the production command path rather than
assuming two independent processor queues are consumed in enqueue order.

Video starts unpaused. `with_video_input_paused` sets the initial state and
`set_video_input_paused` controls subsequent sample admission. Pause does not
cancel already-admitted work. Sampling accepts at most one new attempt per second
using a monotonic clock; the exact one-second boundary is eligible. The timestamp
is recorded before raw-image validation and encoding, so a failure after sample
admission also occupies that window. Rich-image authority rejection occurs
earlier and does not count as an admitted camera attempt. Skipped frames are not
buffered for later replay. Pause/resume does not reset the sampling clock, and
unpausing cannot reopen terminally closed camera input. The processor remains
single-session; setup restores its configured initial pause state.

### Image and memory bounds

Only packed RGB is admitted, with dimensions from 1 through 2048, at most 262,144
pixels, and at most 786,432 raw bytes. Geometry and raw-byte requirements are
checked before cloning for the encoder. JPEG output uses quality 75 and 4:2:0
subsampling, with a capped writer. Before encoding, the available message budget
reserves the complete worst-case event envelope, including generated event/item
IDs, detail, the data-URI prefix, and padded base64 expansion. The full wire size
is checked again at queue admission and using the actual event ID before socket
submission. Generated camera identifiers are bounded and checked for overflow.

The encoder builds the data URI without a separate intermediate base64 string
and releases the JPEG buffer before command admission. Live memory still includes
bounded raw-frame and encoder-owned copies, the capped JPEG while encoding, the
data URI and serialized JSON where needed, and the existing bounded command
queue. `max_message_bytes` limits an individual complete message; it is not a
promise that the adapter, queued frames, or entire pipeline use only that much
memory.

### Interruption and shutdown

Camera writes retain the provider epoch and invocation fence captured when the
raw frame was admitted. Encoding never reacquires a newer fence to make an old
frame current. These original guards are checked after encoding, at private
command admission, during actor dispatch, and through the actual socket
readiness/`start_send` boundary. Camera commands are ephemeral across interruption,
including both interruption pre-drain paths and normal actor dequeue; public
durable `CreateItem` commands retain their existing semantics.

Once `start_send` accepts an image, the writer completes that one flush even if an
interruption arrives meanwhile. It does not pretend to retract an accepted write
or retry an ambiguous timeout. A frame waiting only for socket readiness must
still be discarded if its original epoch or fence becomes stale before
`start_send`.

The bounded blocking encoder has one owned join handle. Cancelling or dropping
the awaiting frame operation does not detach its worker. End and Stop close new
camera admission, cancel/join any retained encoder owner, and then request ordered
finish. Valid camera commands already queued before Finish may drain; stale ones
remain discarded. Stop must perform this join itself rather than relying on core
cleanup. Raw image processing awaits the bounded encoder on the service System lane.
This is not preemptible: queued Interruption and Cancel handling, and End/Stop
preparation, can wait for that encoding operation to return. The invocation
fence retired when Interruption is admitted to the service queue prevents a
completed old encoding result from being newly submitted. Cancel admission does
not retire that fence: while Cancel waits behind encoding, that camera may still
be sent once. Once Cancel is actually consumed by the service it
signals encoder cancellation and closes the sender; cleanup joins the retained
owner. No immediate cancellation or hard wall-clock deadline is promised for
blocking JPEG work.

### Python correspondence and qualification

The fixed Python Realtime source at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf` supplies the one-second sampling,
pause-before-admission, JPEG user-item insertion, default `auto` detail, and
no-automatic-response behavior. It updates the sampling timestamp before encoding
and event-model validation, so those failures also consume its sample window.

The native contract is deliberately narrower. Python passes the original Pillow
mode directly to JPEG encoding, does not impose these dimension/byte caps, and
uses wall-clock time. It accepts inherited `UserImageRawFrame` values while
ignoring their rich fields, checks no frame direction, and forwards successfully
handled images unchanged. Python also exposes low/high detail setters and only
detects an invalid constructor detail later when constructing the event model.
The native path instead bounds RGB, fixes detail to `auto`, excludes upstream
uploads, consumes passive user images, and rejects unsupported rich authority.
Its owned encoder, memory accounting, stale-write fences, and shutdown joins are
additional native guarantees, not results established by the synchronous Python
camera implementation.

Selected original-AST replay has verified the source behavior with real Pillow
and Pydantic, including encoding failures, sampling-window consumption, detail
validation, and the distinction between an internally handled socket error and an
exception escaping the event-send method. Its clock, WebSocket, and pipeline
base are explicit stand-ins; constructor execution is limited to the original
camera-relevant assignments. This establishes a source contract, not full Python
service, real WebSocket, production queue, or live-provider compatibility.

The native test scope uses a real loopback WebSocket and the ordinary
user/service/assistant chain. It covers decodable wire JPEGs, original-frame
forwarding versus passive-user consumption, no unsolicited response, sampling and
pause, rejection/budget boundaries, both stale-command pre-drains and ordinary
dispatch, readiness versus accepted-flush interruption, and encoder ownership
across dropped waiters, End, Stop, and Cancel. Execution results are recorded separately against the frozen source and
programs. A local peer does not establish live-provider compatibility.

## Separate deferred item: Gemini conversation transcription

Retain as a migration/design TODO rather than folding it into the camera feature:
Gemini conversation-mode input transcription still needs the existing Python
sentence/idle-timeout finalization behavior, upstream final delivery, and a
provider-aware paired-context policy that records user and assistant history
without launching a duplicate turn or attempting to reseed an already-running
session. Direction changes alone are insufficient: when user transcription and
assistant completion share one server packet, independent processor queues can
consume them in a different order. The completion path must causally observe the
intended accepted user final before committing the shared assistant turn. A
complete implementation also needs the fixed Python buffer/timer lifecycle across
interruption, turn completion, disconnect, and resumption, plus real paired-chain
validation of one final per intended chunk, ordered shared history, and no extra
provider turn. That broader context/ordering work is deferred and is not claimed
by Slice 102 camera qualification.
