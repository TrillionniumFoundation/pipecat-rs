# Opt-in Live thinking completion

`GeminiClient::with_thinking(ThinkingPolicy::default())` enables a narrow,
explicit conversation-mode subset. `ThinkingPolicy` selects `ThinkingLevel::Low`,
`Medium`, or `High` and a positive `silence_timeout` of at most one day (default
30 seconds). Setup adds `generationConfig.thinkingConfig.thinkingLevel`.
Choose a compatible model explicitly; there is no model-name inference or
availability probe. The ordinary client sends exactly its existing setup and
continues to reject background reasoning statuses.

Thinking with any tool declarations, or transcription-only mode, is rejected
before a connection. Extended-thinking tools require NON_BLOCKING declarations;
this crate's existing tool path does not yet provide their complete semantics.
Unexpected tool calls still fail closed. Thinking budgets and thought-summary
configuration are not exposed by this subset. Existing `thought` text events
remain observable.

## Completion and ownership

- `IN_PROGRESS` plus `turnComplete` stores one bounded pending completion. It
  does not end the logical response or allow graceful finish to return early.
- Further output remains in the same response. `generationComplete` remains an
  observable generation-chunk event, not the logical response boundary.
- `IDLE` or the historical `REQUIRES_ACTION` status releases a stored completion
  once, after the packet's other events have been validated and dispatched.
  A status alone cannot invent a completion when none was stored.
- A new `turnComplete` with missing/UNSPECIFIED/idle status completes normally
  and supersedes a previous stored completion. Unknown status strings fail
  closed. Repeated idle messages cannot repeat the stored completion.
- Stored completion retains its original origin and local interruption
  generation. Local interruption or a provider interruption discards it. A later
  idle-only packet cannot use it to finish a newer generation. Input remains
  available: real audio or manual activityStart may be necessary to interrupt
  the provider. There is no provider turn ID, so a previously unseen late wire
  packet cannot be authenticated as belonging to an old or new turn.

The same owned worker holds the state across reads and reconnects. Resumption
re-sends the thinking setup and preserves a valid same-generation completion;
its handshake does not restart the silence timer. An interruption during the
handshake invalidates the old completion and timer immediately when its control
notification is handled. No extra timer task, reader or reconnect worker is
spawned. Normal shutdown awaits the existing worker; strong Drop retains the
crate's existing abort semantics and is not a synchronous join guarantee.

## Deadlines

While a completion is pending, each successfully parsed application JSON packet
resets its silence deadline. WebSocket Ping/Pong and setup acknowledgements do
not. Continuous valid application traffic can therefore extend this silence
window; it is not a total response-time limit. Graceful finish retains its
separate absolute deadline. Cancel remains prioritized. Expiry returns
`Timeout("thinking silence")`, including during a reconnect handshake, without
manufacturing a successful `TurnComplete`. The worker observes this timer in
its receive/reconnect select. An already active write (including Pong) is
bounded by the existing write timeout first; this is not a hard wall-clock
termination guarantee while a write is in progress.

The fixed Python source resets a 30-second watchdog on SDK server messages and
synthetically releases a deferred completion on silence. Native expiry is a
conservative, explicit difference. The source also treats unknown status as
unspecified; native continues to reject it. Python ignores `generation_complete`,
whereas the native API preserves its existing extra event. This implementation
retains native packet ordering; it does not reproduce Python's different tool
ordering for an immediate, non-deferred completion. Thinking tool integration is
outside the supported subset.

## Evidence boundary

The tests use real local WebSockets and the actual session worker, including
setup, continuation, interruption, finish/cancel and resumption. They do not
establish paid-provider availability, acoustic quality or server-side exactly-once
restoration. Fixed Python evidence and standalone state vectors are separate
from these native integration checks.

Provider protocol reference, checked 2026-10-15:
[Thinking in the Live API](https://ai.google.dev/gemini-api/docs/live-api/thinking/).
The reference distinguishes regular Live models (no configurable thinking level)
from extended-thinking models and requires NON_BLOCKING tools for the latter.
