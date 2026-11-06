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
edit. Repository formatting and whitespace checks cover only the modified files.
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

The stress test now records schedule, iteration, A/B side, monotonic elapsed
wait time and both peers' public state, cancellation, retained fatal error and
transport counters if either existing wait fails. It takes those observations
before panic/drop can change ownership or state. It neither retries the failed
wait nor changes its result or deadline. No production API is expanded to obtain
private native state.

If the setup timeout occurs again, attribution still requires a phase timeline
for both peers (ICE selection, DTLS completion, SCTP association completion,
channel Open and publication) plus scheduling-delay evidence during that exact
wait. The new failure context can identify the affected pair and show deadline
overshoot; overshoot alone does not prove CPU contention. Capturing native
library logs or a separately scoped native-transport diagnostic harness would
be necessary to resolve the remaining phase ambiguity. Successful subsequent
runs do not close this setup stability observation.
