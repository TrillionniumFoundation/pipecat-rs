# Scripted plain-request user images

The optional `user-images` feature adds a bounded, native image-input route for
an application that explicitly requests an image. It does not make an ordinary
text bot automatically vision-enabled or implement tool-driven video.

## Explicit entry points

- `UserImageScenarioFile::load(path)` reads a trusted local YAML/JSON bundle and
  its explicitly named PNG/JPEG assets before any connection. Image paths are
  relative to the outer bundle directory, including references in expanded
  includes. Files must remain inside that directory. This assumes a stable,
  trusted filesystem; canonical containment is not a hostile-race sandbox.
- `into_sessions(url, options)` creates all sessions without connecting. The
  preflighted image plan cannot subsequently be replaced. Ordinary
  `ScenarioFile` still rejects image inputs.
- `EncodedUserImage::new(bytes, mime)` and
  `EvalSession::with_user_images([(turn_index, image)])` support owned native
  inputs. Accepted MIME values are exactly `image/png` and `image/jpeg`.
- `pipecat-eval run SCENARIOS.yaml ws://127.0.0.1:PORT --user-images` selects the
  direct CLI route. `--stop-bot` remains available for a single scenario.
- The bot must opt in separately with
  `EvalBotHost::with_user_images(ImageInputLimits::default())` and provide an
  application pipeline that emits a real, plain `UserImageRequest`.

Existing public scenario/options/host configuration struct literals remain
valid. Default builds and disabled hosts create no image decoder worker. This
slice qualifies text+image and image-only turns. Recorded/synthesized user
speech, DTMF, bot-audio capture/transcription and hosted suite combinations are
explicitly unsupported in this route.

A turn can contain `image: relative/path.png` alongside its ordinary `user` and
`expect` fields. Registration happens after `send_after`/quiet waiting and before
that turn's user input. Failed waiting or registration prevents its following
user send. An image-only turn registers passively and then observes its
expectations. Neither registration nor its socket admission proves visual
consumption or triggers image inference. Ordinary text can still cause ordinary
text inference in an application that does not request the image.

## Request and cache contract

Each connection starts with an empty, bounded, in-memory latest-image cache.
The latest image persists across ordinary turns until replaced; omission or
`image: null` does not clear it. A request captures the image current when that
request is admitted. Replacing A with B during A's decode does not change A's
accepted request; the next request sees B. No image is persisted to disk by the
host.

Only a plain request is accepted: `function_name`, `tool_call_id` and
`result_callback` must all be absent. Authority-bearing requests are rejected
before ownership, without stripping their fields, invoking a callback or
manufacturing a tool result. Destination/source/identifier restrictions and
bounded lifetime request identities apply. Missing images fail promptly instead
of waiting for a later registration.

The actual result is tightly packed RGB `UserImage`, with the decoded dimensions,
original user/text/append fields and associated plain request. Encoded bytes are
never mislabeled as RGB. `run_llm` remains unset; the standard assistant's
existing append/run policy and idle-state restrictions still apply.

A private, non-wire causal constraint retains the scope captured at actual
image-input request admission and its memory lease through downstream queues.
This is not a socket-receive acknowledgement: requests not yet admitted are
subject to their actual queue epoch. An arbitrary application relay that forwards
a System request through another processor before image-input admission must
preserve its own causal contract; this feature does not globally change System
frame scheduling. The standard assistant validates it at
the actual synchronous history/replay commit, atomically with interruption
admission. Derived context is enqueued under the retained scope after unlocking.
An already-queued System image cannot bypass this check merely because System
frames survive normal-queue reset. Clone/duplicate preserves the restriction and
consume-once identity; mutation cannot convert it into fresh authority. A commit
that wins before interruption remains in history; cancellation does not roll it
back. The derived stale inference is suppressed if interruption wins before
its admission/consumption.

Image-derived provider work also retains this private constraint through the
native LLM completion FIFO. Its final synchronous inference admission occurs
under the original/current runtime gates and source-open lock, after the FIFO
and before any response-start boundary. A second freshness check after that
boundary prevents provider stream construction if closing won during its wait. Closing the
source before that admission prevents the provider request; an admission that
wins first is already admitted work and is canceled through the normal owned
completion lifecycle. Closing cannot recall an external side effect that has
already occurred. This image-specific gate does not change the global meaning
of `PipelineSender::cancel`: accepting Cancel into a queue is not proof that
all processors have consumed it or that their invocation epochs retired.

## Resource and shutdown bounds

The first route permits only stricter settings than the selected maxima:

- 64 asset references, 16 scenarios, 4 MiB total owned encoded bundle assets
- 64 KiB encoded bytes/image; PNG/JPEG signature and MIME must agree
- 1,024 pixels per dimension, 262,144 pixels/image, 786,432 RGB bytes
- 32 MiB decoder scratch; one native decoder worker, at most two jobs
- Two request slots across pending, decoding, ready and forwarded states;
  1 MiB reserved storage per slot, retained until actual consumption/final drop
- 16 KiB request/header fields, 256-byte identifiers, 8 KiB text
- 4,096 lifetime request identities, 64 registrations, 256 KiB identity storage
- 32 MiB cumulative decoded RGB; canceled/failed admitted decode work is also
  conservatively charged from validated dimensions
- 1 MiB native image frame allowance, with the existing cumulative processed
  frame/byte and context budgets still enforced
- At most 512 KiB sender image temporaries; exact base64/envelope size is checked
  before allocation and outgoing image bytes/messages have private accounting
- Five-second decode and cooperative shutdown diagnostic deadlines

The image-owned logical-buffer plan is at most 38.8125 MiB: 4 MiB assets,
64 KiB latest image, 2 MiB request slots, 32 MiB decoder scratch, 256 KiB
identity bookkeeping and 512 KiB sender temporaries. Existing finite
transport/pipeline/context allocations and object bookkeeping are separate.
This is not an allocator or process RSS hard bound, nor a bound on arbitrary
copies created by application code. The host avoids redundant RGB observer
clones. Its image-specific measurement does not JSON-expand pixel bytes and
does not increase the global wire/JSON limit. The existing JPEG/context/provider
request bounds still apply after conversion.

Decode futures cancel their own jobs when dropped; admission remains reserved
until the native job actually exits. Cancel, disconnect and timeout close
admission, revoke old work and join owned tasks/decoder handles. A shutdown
Timeout is a diagnostic, not proof of joining or permission to detach. An owned
handle is retained for resumed joining, and Drop joins as a fallback. Cooperative
codec checks cannot promise an OS-hard wall-clock termination bound. A deliberately
noncooperative application System-frame handler can block a later Cancel on the
same lane; the existing pipeline shutdown deadline then reports
`ShutdownTimeout` and aborts/joins its actors. Completed cleanup does not turn
that diagnostic failure into a successful cooperative cancellation.

Provider cancellation qualification distinguishes ownership: the pipeline joins
its completion task and drops/cancels the provider stream; local HTTP tests
observe the peer connection closing and join the fixture server. The existing
LLM client does not expose its internally spawned provider task handle, so this
route does not claim an explicit join of that private handle.

## Reference and intentional differences

The pinned reference is `ProfHepta/pipecat` commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`: `evals/script.py`,
`script_driver.py`, `client.py`, `serializer.py`, `transport.py`, and
`processors/aggregators/llm_response_universal.py`.

The reference registers encoded images before user input, retains the latest
across turns, snapshots it before asynchronous decoding, and emits actual raw
RGB with the original request. Native PNG/JPEG/type/path/size bounds are stricter
than Python's MIME guessing and Pillow-supported formats. Empty/malformed
registrations fail rather than silently retaining an old image. Missing-image
handling is an explicit error rather than a warning with no response. A new
native session is isolated; the reference serializer has no explicit image reset
on reconnect. Tool-image callback/history reconciliation remains unsupported.

Qualification uses synthetic local assets, real loopback WebSocket and HTTP,
actual RGB consumption by the standard assistant and its resulting native
provider request. A deliberately written application router converts the actual
RTVI user append into a plain request, without first starting another inference.
This tests the supported plain-request application composition, not a provider
function call or a fabricated visual result. No personal images, live provider,
camera, SFU, new credentials or paid request is required.
