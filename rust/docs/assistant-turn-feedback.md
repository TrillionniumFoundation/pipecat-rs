# Committed assistant-turn feedback

`LLMAssistantAggregator` produces the existing assistant context feedback frames
at the same synchronous completion boundary that commits assistant history.
The context update precedes a downstream `LlmContextAssistantTimestamp`; a
nonempty marker-filtered transcript also produces downstream and upstream
`LlmContextAssistantTurn` siblings. The reducer exposes both siblings as `Push`
actions, constructed with `Frame::broadcast_pair`, so frame-only receipt sinks
retain their existing interface and genuine reciprocal frame identity.

The turn timestamp is an RFC3339 wall-clock sample captured once on the first
real active-turn start. Repeated response starts within that turn retain it.
The commit timestamp is sampled separately after history mutation. The paired
monotonic `process_at` clock continues to govern realtime handoff; it is not
misrepresented as a wall-clock timestamp.

Compact, detailed and Vision response boundaries, explicit aggregation flush,
interruption and lifecycle completion share this reducer boundary. Empty active
turns retain their existing stopped event but produce no feedback frames. A
marker-only committed message produces a commit timestamp and no empty turn.
Two genuine identical turns produce different sibling identities. Existing
frame replay and playback-context tombstones suppress repeated completions.
Thought text, tool results and unconfirmed generated speech never become the
outward turn transcript. Spoken mode continues to require the existing trusted
receipt or spoken-text/flush interface; these notifications add no playback
attestation of their own.

## Receipt and interruption routes

- Paired user interruption retains the committed timestamp and turn effects
  after the actual interruption action, so that interruption does not purge
  its own partial-turn feedback. Shared history is already committed. The
  assistant's ordinary downstream context snapshot is intentionally still
  omitted from this user-owned route: emitting it there would pass it through
  the LLM and could schedule an extra inference. Typed input subsequently
  carries the merged context; VAD keeps the existing later user-context
  timing. This is not the source's separate assistant-origin context admission
  ordering
- CLI playback commits under the existing pending-receipt lock, releases it,
  and then admits notifications. It retains the acknowledged start invocation
  and intersects the text/audio/stop invocation fences for the new timestamp
  and turn frames. Existing context/tool effects still use the original
  `AggregationOutput::emit` path with the acknowledged start invocation;
  sealed realtime causes keep their existing binding. When only a later
  material origin becomes stale, new feedback is suppressed while the existing
  context effect can still be emitted. Receipt history remains committed
- Common-media receipt sinks still return `Vec<DirectedFrame>`. Source response
  boundaries and every contributing receipt frame retain their original
  invocation fences, and the output-start invocation supplies the exact
  destination. All returned frame effects are admitted under that intersection
  after the ledger lock is released. This deliberately tightens the old
  setup-scope forwarding of context effects as well as fencing new feedback.
  A stale source does not roll back truthful receipt history. No generation is
  resampled at receipt time

No background notification task, additional unbounded queue, callback registry
or core frame is introduced. A completion adds at most three feedback frames.
Media pending and scope retention use the existing bounded response ledger and
core's bounded 64-origin intersection. Pipeline admission remains nonblocking:
full output lanes report existing backpressure errors, with committed history
retained. Direct reducer callers own emission of their returned effects, just
as they own existing context actions; emitting a cloned output twice is not a
second playback confirmation and is not a supported exactly-once host pattern.

Terminal completion is reduced before forwarding the owned terminal. Existing
pipeline cancellation, closed providers and stale invocation fences may
suppress terminal-time outward delivery. The code does not restart a terminated
STT service or force a final network update through a closed provider. No async
Cancel callback delivery is promised.

AssemblyAI's streaming WS and Sync HTTP consumers require no wire changes.
Native loopback fixtures drive actual assistant completion rather than
injecting assistant-turn frames, verify automatic `agent_context` and next
multipart chronological context, and retain disabled/manual-context controls.
This slice does not provide the public stopped-callback bridge, full-response
utility or latest-context notifier gate.
