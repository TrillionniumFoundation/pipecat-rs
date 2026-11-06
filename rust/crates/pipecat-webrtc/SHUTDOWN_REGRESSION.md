# DTLS start/stop UDP retention regression

## Scope

This change fixes local shutdown ownership in the vendored WebRTC 0.14.0 DTLS
transport. The reference source is repository commit
`5e889453a6956b03c6d0745663a87b0e604adebd`; the new regressions are executed against
that transport source before applying the fix. Validation uses Rust 1.99.0,
locked dependencies, the default Opus feature and real localhost UDP sockets.

This is scoped evidence for local lifecycle cleanup. It does not qualify remote
ICE/TURN, SFU or LiveKit operation, browser permissions/recovery, camera/video,
screen capture, media quality or a production Hepta end-to-end loop. Cancellation
of this asynchronous handshake does not establish preemption of blocking native
calls or reversal of remote effects.

## Failure and cause

The historical qualification record names
`repeated_peer_shutdown_schedules_release_both_udp_ports` at schedule 5,
iteration 24. In this run the original unmodified stress test passed its initial
invocation and another 50 invocations. That historical stochastic schedule was
not observed failing again and is not presented as a newly reproduced failure.

Two deterministic tests reproduce the underlying shutdown ownership gap against
the unmodified vendored transport:

- `close_during_dtls_connecting_callback_releases_both_udp_ports` holds both DTLS
  Connecting callbacks, starts closing both peers, waits until both transports
  report Closed, and then releases the callbacks. Both peer close calls succeed,
  yet real UDP candidate ports remain bound and the two-second rebind assertion
  fails. The public connection handles stay alive during the resource check.
- `stop_cancels_an_already_pending_native_dtls_handshake` establishes a real ICE
  pair and polls one DTLS start into Connecting while the other side withholds
  DTLS negotiation. Stopping both transports does not complete the pending start
  future; the two-second completion assertion fails.

`RTCDtlsTransport::start` previously awaited `DTLSConn::new` before publishing
its connection in `self.conn`. `stop` could close only a published connection.
A start paused in a Connecting callback could enter that constructor after stop,
and a constructor already awaiting its handshake had no stop notification.
The peer operation awaiting this start retained the native peer/ICE ownership,
including candidate socket references, after the public close path returned.

## Repair

`src/dtls_transport/mod.rs` in the vendored crate adds one stop notification.
Before entering the constructor, start registers that notification and then
rechecks the existing durable closed flag. This ordering covers a stop before
registration as well as one after the check. A biased `select!` ends a pending
constructor when stop is observed. Dropping the native constructor drops its
reader-stop sender and allows its reader/writer tasks and the awaiting peer
operation to release their references.

`stop` sets the closed flag and signals the notification before its first
cleanup await. The existing publication guards and terminal Closed-state rules
remain in force. No dependency version, wire protocol, crypto algorithm or
public transport API changes.

## Verification

The controlled baseline run contains **0 passed, 2 failed** tests; both failures
are the expected lifecycle assertions described above, rather than build or
fixture failures. After the fix, `cargo test -p pipecat-webrtc --locked --offline
--all-targets` passes **46 tests, 0 failed, 0 ignored**:

| Target | Passed |
| --- | ---: |
| Library unit tests, including the two new regressions | 20 |
| Localhost integration | 16 |
| Receive-only integration | 4 |
| Shutdown lifecycle integration, including the unchanged historical test | 6 |

The test log also contains deliberate worker panic injection from existing
shutdown tests; those tests verify the error report and resource cleanup and
pass. Those injected panics are not hidden failures.

The new regression module is `src/shutdown_start_tests.rs`. The historical test
in `tests/review_lifecycle.rs` subsequently gains failure diagnostics only; its
acceptance criteria, A-then-B wait order and five-second per-peer setup deadlines
are unchanged. The 46-test package result above precedes that diagnostic-only
edit. The later frozen-source gate also passed **46 tests, 0 failed, 0 ignored**
including that edit, with strict selected-package Clippy and unchanged source
before/after. Its published code is
`6367d62babf97cab96a756b58427c8ee5fac5f15`, full tree
`652aceec8a2dbf70acc6d58dcfc39af12e710f82`; see the
[source-bound final evidence](../../docs/qualification-hmi-followthrough-20261106/final-gates.json).
Repository formatting and whitespace checks cover only the modified files.
Full-workspace qualification remains a separate gate and cannot be inferred from
these package results.

### Extended concurrency check

The two deterministic regressions also passed 30 successive invocations each
(60 test executions). A subsequent unchanged historical-stress loop passed
24 invocations and failed on invocation 25 while waiting for a data channel to
open, before that pair's shutdown. The error is `Timeout("data channel")` at the
existing five-second setup bound, rather than a UDP rebind failure. The failed
invocation is retained as a separate setup stability observation; it is not
counted as a passing stress run and no test deadline or assertion is changed.
The subsequent follow-up passed all 30 invocations of the unchanged test and
its original deadlines; it is recorded separately and does not erase the
retained setup timeout. Executor command delays were observed
at the same time, but no resource-pressure cause was established.

### Follow-up review of the setup timeout

The retained log identifies the offerer's first application-channel wait, but
does not identify its schedule/iteration or capture either peer's state at the
deadline. It cannot establish whether ICE, DTLS, SCTP, channel opening or runtime
scheduling prevented readiness. The reported 23.83-second duration covers the
whole test invocation, not the individual five-second wait.

Source review found no direct lost-wakeup window in the wrapper's
`watch::Receiver::borrow_and_update` / `changed` readiness loop. The native
`on_open` registration also rechecks the Open state and takes its one-shot
handler under a lock. The wrapper waits for that asynchronously scheduled
handler to publish the channel; native Open alone is not a published wrapper
notification. Cancellation is observed separately and returns Closed rather
than the observed Timeout. Neither inference identifies the original failure.

The peer's public Connected state summarizes ICE/DTLS rather than SCTP or DCEP.
Native SCTP startup/channel-open errors are logged internally, and a terminal
SCTP startup failure is therefore not established by the original log's generic
Timeout. There is no evidence that this branch caused the retained failure, so
no transport behavior is changed on that hypothesis.

That initial diagnostic checkpoint records schedule, iteration, A/B side,
monotonic elapsed wait time and both peers' public state, cancellation, retained
fatal error and transport counters if either existing wait fails. It takes those
observations before panic/drop can change ownership or state. It neither retries
the failed wait nor changes its result or deadline. That checkpoint did not expand
production APIs to inspect native state.

Attribution still requires observations of both peers during the actual failed
wait. Deadline overshoot alone does not prove CPU contention. The layered
observations added below narrow the missing phase information, without assigning
a cause to the retained timeout. Successful subsequent runs do not close this
setup stability observation.

### Layered setup observations, 2026-11-06

`SmallWebRtcConnection::diagnostics()` now returns `ConnectionDiagnostics`: the
wrapper state, native peer/ICE/DTLS/SCTP/channel states, whether SCTP start was
entered, whether the wrapper channel watch contains a channel, cancellation, and
bounded first-occurrence `SetupMilestones`. Milestones distinguish channel
registration, peer/ICE connected callbacks, the open callback, wrapper publication
and the close callback. They use elapsed monotonic time and fixed-size storage;
SDP, payloads and unbounded event histories are not collected.

These fields are sampled independently, not one atomic transport snapshot.
The channel observer holds a Weak reference. A diagnostic call may briefly
clone the native peer while reading its states; observations retain no persistent
peer or channel owner. Native-state fields become absent when wrapper close
takes its peer handle, which can precede completion of asynchronous transport
cleanup. Absence therefore does not attest that native shutdown has completed.
Likewise, the watch waiter can wake before the following publication timestamp
is stored. Callbacks and their milestones do not provide a global protocol clock.

The vendor adds only `RTCSctpTransport::has_started()`, a read of its existing
atomic flag. True means start was entered, not that SCTP association or DCEP
completed. Native peer Connected still describes ICE/DTLS. Native channel Open
and wrapper publication remain separate observations. No SCTP start admission,
DTLS algorithm, transport retry or timeout is changed by these additions.

The original stress test still waits for A and then B, with five seconds per
side, and keeps all 32 batches, six shutdown schedules and the original UDP
rebind criteria. During each wait it samples both peers every five milliseconds
and retains at most 32 changed observations. Failure records the affected pair,
elapsed times, sample/eviction counts and maximum sampling gap before panic/drop.
Sampling can miss short transitions and can affect scheduling. A sampling gap
is an observation, not proof of a resource-pressure cause.

`diagnostics_distinguish_native_open_from_delayed_publication` is a controlled
real-UDP regression. Its test barrier holds only the offerer's wrapper open
callback before ordinary publication. It checks native DTLS/SCTP/channel
readiness while the wrapper is unpublished, the existing data-channel timeout,
an already-pending watch waiter waking after release, readiness for a later
waiter, and terminal wrapper cleanup with retained milestone observations.
It waits separately for the diagnostic publication timestamp because readiness
can be observed first. This fixture verifies the exposed states and readiness
semantics; it does not reproduce or explain the historical invocation-25 failure.

Source review also inspected the native SCTP start flag's separate load/store.
The original initial-SDP path queues transport and RTP startup on the same
serialized Operations worker. No concurrent SCTP-start execution was established
for the retained stress failure, so no speculative admission repair was applied.

The local current-source all-target Clippy check completed successfully with
`-D warnings`. Two attempts to build runtime tests were interrupted by rustc
SIGKILL in dependencies, with no new-source test executed; the cause of those
signals was not established. Forty additional invocations of an inherited
stress executable passed, but that executable predates these diagnostics and
does not qualify the modified source. Current-source package and full-workspace
runtime gates remain required. The retained setup timeout and the unreproduced
historical workspace UDP-retention schedule remain open.
