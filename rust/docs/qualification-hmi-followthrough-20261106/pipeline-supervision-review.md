# Independent read-only integration review

Reviewed the current shared source without edits to code or Cargo execution. The
stderr/ExitCode issue is already being fixed by the owner and is not repeated as
a new finding. Findings below follow concrete control/data paths; none assumes
an RSS bound or rollback of device effects.

## P1 — owner Turn can block in native retirement without a supervisor deadline

`process_supervision.rs::commands` arms `Controls` only for `Interrupt` and
`Retire` (current match around lines 199–203). In the actual native endpoint,
`Command::Turn` calls `HmiSession::attach_turn`, which calls
`MediaControl::interrupt` whenever `owner_epoch > self.epoch`.
`LocalPresentation::interrupt` in turn calls `LocalAudioOutput::interrupt`, which
synchronously drops/joins the old CPAL stream and reopens it.

Concrete sequence: startup readiness succeeds; the owner sends a valid Turn for
a newer epoch while keeping stdin open and awaits its ACK; CPAL teardown/open
blocks. There is no pending `Controls` entry for this Turn, no shutdown/EOF
observed, startup is already disarmed, and the child cannot emit the ACK. All
supervisor futures can then remain pending indefinitely. This is the same native
control operation protected on explicit Interrupt/Retire, reached via an omitted
command path.

Fix location: include Turn in the parent's control arming set, and emit the
matching private `media_process_control_done` after the child's successful Turn
handling/ACK, including the valid same-turn case. Preserve the child's existing
scope gate as the authority owner. A regression fixture should leave owner input
open, send a valid Turn, and simulate a child stuck after ready/read; expect a
control timeout, kill request and observed reap. Existing command-sequence fixture
currently treats Turn as a non-control and will need adjustment.

## P2 — a completed control can race a stale cached timer and be killed anyway

The supervisor snapshots `controls.deadline()` before its `select!`. Its biased
timer branch is ordered before `controls.changed.notified()` and fails without
re-reading the current pending set (current lines 431–468).

`events` can consume a private control ACK before its deadline, successfully
remove the pending sequence in `Controls::acknowledge`, notify `changed`, then
continue into the next `bounded_line` read and return Pending. If the task is
rescheduled after the old deadline, the already-created sleep wins the biased
selection before the `changed` notification and reports a control timeout even
though that exact ACK was accepted before expiry. The same race occurs when the
old earliest control was removed but a later one remains pending.

Fix location: on timer wake, re-read the current pending deadline under the monitor
lock and fail only if the current earliest pending deadline is actually expired;
otherwise continue/rebuild the selection. Reordering notifications alone is less
robust than validating state at the timeout decision. A deterministic regression
can make ACK processing remove the entry before the deadline, then advance the
clock before the outer selection is repolled.

## Confirmed integration boundaries

- Local speech input's strict profile is compatible with the current
  CPAL -> Silero -> Whisper frame path: 20-ms PCM, VAD/speech-control parameter
  DTOs, metrics and transcription JSON. No opaque native LLM/tool capability is
  attached by these processors. Oversized transcription JSON is explicitly
  rejected. Native model allocations and processor state remain outside queue
  payload accounting.
- The reserved strict control slot covers Start separately and Cancel,
  Interruption and Resume independently of system-media capacity. Graceful
  Stop/End still keep their normal drain order.
- Invalid input closes the sole child stdin owner; startup/explicit stop,
  monitored control and pipe-write deadlines lead to terminate + bounded wait.
  `status` records actual wait evidence; `kill_requested` alone does not produce
  a reaped/successful report.
- Dropping the supervisor future requests kill-on-drop but supplies no observed
  reap report. Current documentation correctly limits that claim. The Linux
  executable's parent-death guard runs before runtime/model/device creation and
  its current-thread parent preserves the creating thread's lifetime.
- Presentation admission/epoch checks and command scope checks remain in the
  native child / existing HMI owner path. The parent validates framing and
  correlates private liveness receipts; it creates no second model/tool owner.
- Already callback-submitted audio remains an unknown device effect after stop
  or process death. The implementation and report do not claim it was retracted.

No code was modified and no Cargo command was run during this review.

## Follow-up: both supervision findings resolved in source

The owner added Turn to the monitored control set and added its matching private
completion event after successful native Turn admission/ACK. The command counter
still counts every command; the non-control fixture now uses Speak. The new
Turn-block test keeps owner input open and therefore targets the missing deadline
rather than the EOF/shutdown fallback.

The timer branch now refreshes `Controls::deadline()` and fails only when the
current earliest pending deadline is expired. An empty/replaced deadline rebuilds
the select loop. The new timely-ACK test deliberately stalls scheduling for
180 ms after consuming the ACK, with a 120-ms control deadline, matching the stale
snapshot race above. These two findings are resolved by source inspection. Tests
were not rerun by this reviewer; the root owns the combined execution gate.

## Independent review of the minimal DTLS stop-notification patch

Reviewed `rust/vendor/webrtc-0.14.0/src/dtls_transport/mod.rs`, its existing
publication/state guards, peer close ordering, the new real-UDP tests, and the
locked `dtls` 0.13.0 constructor implementation in the local Cargo source cache.
No new blocking correctness issue was found in the added notification/select.

- `stop` stores the durable closed flag before notifying and before cleanup
  awaits. Start enables its notification before checking that flag. This covers
  stop before registration and the check/select window without relying on a
  stored notify_waiters permit.
- Stop is the first biased branch, so a ready stop wins when handshake completion
  is concurrently ready. A stop racing later still reaches the existing closed
  check under the connection-publication mutex. SRTP/SRTCP publication guards and
  the terminal Closed state rules remain intact.
- The locked constructor owns the in-progress DTLSConn locally while awaiting
  handshake. Dropping it drops its sole reader_close_tx; the reader selects that
  channel and exits, releasing its packet sender, allowing the writer to finish.
  Peer close subsequently stops ICE/mux ownership. No task is detached by the
  new wrapper select, and the preexisting constructor tasks retain their stop
  path. This supports asynchronous cleanup, not preemption of blocking code.
- The change keeps the original client/server role selection and DTLS settings;
  it adds neither a second transport owner nor a restart capability.
- The Connecting-callback regression intentionally releases the application
  callbacks. The patch cancels a pending constructor and rejects constructor
  entry after stop; it does not claim to preempt an arbitrary callback that never
  returns. The other regression exercises a constructor already Pending with
  actual ICE connectivity and no peer DTLS negotiation.

The reported deterministic baseline failures and fixed 46/46 package result are
consistent with these code paths. This review does not rerun or independently
recount those executions. The retained five-second data-channel setup timeout in
an unchanged stress invocation remains a separate unresolved stability
observation; it must not be relabelled as a passing stress run or proof that this
cleanup patch caused/fixed setup flakiness. Whitespace check of the reviewed
modified source files passed. No code changes or Cargo commands were made.
