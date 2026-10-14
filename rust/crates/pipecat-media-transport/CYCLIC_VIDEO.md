# Opt-in non-live raw-image cycling

`OutputTransport::with_cyclic_video(CyclicVideoOutputConfig { ... })` adds one
owned cyclic-image worker per destination. It is mutually exclusive with
`with_live_video`; the legacy constructor and live-image scheduling retain their
existing one-shot behavior. The backend must explicitly support independent
video: image callbacks may overlap PCM/control calls, but image calls for one
destination are serialized. Dropping an image callback must cancel its unfinished
work without a detached late write. No device, codec, URL fetch or default sink
is provided.

## Pinned source contract and bounded profile

Reference: ProfHepta/pipecat at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`,
`src/pipecat/transports/base_output.py`, blob
`2797744ed05fce9c089358e7860b3ae74a486abe`.
The source non-live path repeats both a singleton image and a `SpriteFrame`
using `itertools.cycle`. Each completed draw is followed by one frame period;
this is fixed delay, not catch-up scheduling. A normal image's
`sync_with_audio=True` routes activation through the audio queue. Sprite child
sync flags do not schedule the whole sprite. Each destination has its own set.
Repeated drawing calls the backend, not `push_frame`.

The native profile accepts `OutputImage`, `OutputImageDetailed`, and nonempty
`Sprite` with raw pixels at their existing dimensions. It does not resize,
convert formats, decode URLs, or implement source subclass polymorphism.
`UrlImage` and `AssistantImage` are rejected in this mode. Native semantic
carriers, error origins, PTS (relative or pipeline-clocked), and nonempty metadata
are also rejected; immutable raw playback cannot acquire or replay their effects.
The original activation retains its own identity for its one downstream forward;
backend callback copies have fresh native frame identities, preserve raw data and
public source/destination/name fields, and carry no original semantic authority.
Sprite children inherit the outer destination. A backend remains responsible
for accepting its supported raw formats; this adapter does not prove pixel
validity or physical display.

An empty sprite is rejected without replacing an existing valid cycle. The
source instead constructs an empty iterator and fails when taking its next item.
Source replacement lets an already-running old draw and current sleep finish.
The native mode cancels a repeat when finite queued work arrives so replacement
and flush can progress without waiting for the old repeat timeout. A cancelled
callback is a discard, not completed display. A successful new activation never
permits an old completion to reinstall its previous set.

## Activation, ordering and finite flush

Independent images and sprites use the bounded cyclic queue. Synchronous image
activation remains in the normal media queue behind preceding PCM. Both lanes
perform their own first draw under shared activation/image gates. The normal
lane does not enqueue work into the cyclic lane and wait for it: that would form
a deadlock when an all-lane flush waits for the normal lane.

The first successful backend draw forwards the original activation frame once
only while its original causal scope and queue cutoff remain current.
A retained set then cycles only through backend `write_image` calls. Repeats do
not forward more pipeline frames, add AssistantImage/context messages, emit
speech-history confirmations, or fabricate audio/playback receipts. A sprite
activation's first draw confirms only its first image; it does not promise that
every sprite image has already been shown.

The existing flow-flush rendezvous includes the normal and cyclic queues.
It is finite: preceding activations must finish their first draw/forward or be
causally discarded. It does not wait for an infinite animation to end or certify
a full cycle, and it may cancel a currently selected repeat to reach the fence.
Once an activation is selected, activation-gate/image-gate waits, a newly
observed pause and the first backend call share one absolute write-timeout
budget. Queue admission has its separately configured deadline.
A failed first draw or current-generation repeat is not converted into successful
rendering. Backend failures and timeout errors remain explicit, with conservative
flush failure handling. Original causal scopes and destination queue epochs are
kept separate. The acknowledgement receiver is the generation-free setup
context, saved only after setup succeeds and released during cleanup. It supplies
the destination identity; every actual admission still checks the complete
original input invocation fence. This avoids freezing acknowledgement at the
Start epoch while never renewing a retired activation's authority.

## Lifecycle and cancellation differences

Pause gates callback starts without discarding the retained set. It does not
undo a callback that already started. Resume permits one next draw and resets
fixed-delay progression without a burst of missed frames. Readiness is only
published after backend setup and owned-worker startup.

Source interruption restarts its video task but preserves the current iterator.
This native mode clears retired cycles. All activations retain the original
runtime invocation fence; any retirement of that scope stops the cycle, including
frames marked uninterruptible. A destination selects its transport queue cutoff,
but a pipeline `Interruption` advances the core invocation epoch globally, even
when its destination is named. Therefore a named interruption can retire other
destinations' existing cyclic sets too. Independent per-destination replacement
and pause do not imply independence from the core global causal epoch. Fresh
frames after interruption can establish fresh sets; old scopes are never renewed.
The one-shot/live paths keep their prior interruption contracts.

End/Stop stop periodic work while allowing finite queued activations and flush
participants to drain; cyclic completion cannot keep terminal drain alive
forever. Cancel/owner teardown cancel owned work. Normal media-worker graceful
exit must not be mistaken for cancellation of the other finite lane. Cleanup
joins the pipeline-owned workers before backend shutdown. These are cooperative
future-cancellation guarantees under the backend contract, not preemption of a
blocking callback or proof that a physical device retracted already displayed
pixels.

## Resource budgets

The configuration has no implicit defaults. Choose positive queue count/byte
budgets, retained-image/byte budgets, and framerate in 1..=1,000,000,000. Budgets
are per destination and additive with the existing media queue. A frame's
original invocation reserves a conservative bounded weak-origin allocation in
its queue entry. Sprite queue charging includes spare container capacity, every
child pixel/format allocation and the outer envelope instead of treating a sprite
as zero bytes.

Retained reservations cover active, pending and old in-flight sets until their
last owner releases them. The first draw temporarily retains both the original
activation and callback copies, counting 2N image slots for N images; after the
original forward only N callback slots remain. Replacement also needs capacity
for the still-retained old set. A caller should budget for this overlap rather
than assume a limit sufficient for first activation guarantees replacement.
Retained-byte reservations conservatively include callback/frame envelopes and
causal scope storage. Exceeding retained capacity fails immediately before
replacing the valid current set; queue admission follows the separately chosen
existing Wait/FailFast policy. These are logical retained-allocation budgets,
not a process RSS, allocator-overhead, backend-allocation or physical-device
buffer limit.

## Evidence limits

Tests use real native pipelines, controlled backend futures and tiny synthetic
RGB payloads. They observe callback starts/completion/drop, scheduling, first
forward, scope retirement, queue bounds, finite flush and terminal ownership.
The source oracle executes unchanged extracted cycle methods with deterministic
draw/sleep stubs; it is not a full Python transport or a physical display.
Camera capture, codecs, resizing, hosted avatars, real video devices and visible
rendering remain separate qualification work.
