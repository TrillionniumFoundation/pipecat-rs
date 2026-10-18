# Optional paired audio history

Enable `pipecat-gemini-live/paired-history` for an explicit, single-use,
tool-free audio conversation connected to the standard turns pair. The default
feature set and raw `GeminiSession::next_event` event ordering are unchanged.
This is a bounded subset, not complete Gemini Live/Python service replacement.

Construct an unused `LLMContextAggregatorPair` with
`RealtimeServiceMode::Enabled`, then
`GeminiHistoryRoute::new(client, pair, GeminiHistoryOptions::default())`.
`into_processors(delivery)` returns the mandatory ingress gate, that exact user
aggregator, Gemini service, delivery processors, and that exact assistant
aggregator. Gate/user/service must remain contiguous and in this order. Caller
input/VAD processors may precede the gate; sinks may follow the assistant.
Setup validates the selected components before the service connects on Start.
This does not promise that arbitrary application delivery processors cannot fail
later. Use conversation mode with input transcription enabled and matching PCM
sample rates. No new credentials, external service, or model runtime is involved
in constructing this route.

## What the receipt proves

The private receive path looks ahead within each accepted provider packet while
preserving public raw event order. Actual input transcription contributions are
concatenated exactly, segmented with the existing sentence matcher, and otherwise
finalized after 500 ms of inactivity. Interim hypotheses never enter history.
Punctuation results retain normalized text/language provenance; timeout finals
have no result. This does not reconstruct the Python SDK's complete message.

A registered final must be consumed by the actual standard user aggregator. A
receipt is issued only after the real shared user-message append and admission
of its required authentic local snapshot to the adjacent service queue. Merely
writing the socket, reversing a frame direction, enqueueing a final, or seeing a
public context callback is insufficient. The service consumes this local snapshot
without replaying it to the provider. Dependent text, audio, output transcription
and completion remain FIFO-held until the corresponding receipt is validated.
Usage/control processing remains live while the consumer is paused.

A completed sentence is staged promptly, but it does not force a separate history
message. With no dependent output, ordinary user aggregation remains in control.
An output barrier requests the required received prefix; final End commits remaining
speech only after actual provider EOF and suffix finalization. Standard spacing
between independent final frames is retained, including a separator beside a
source-retained leading space. Waiting for future output alone does not start the
consumer deadline.

The guarantee is a received-input-prefix guarantee, including same-packet input.
An unknown later transcription cannot retroactively precede output already
released. A receipt does not mean a remote provider consumed a snapshot. Normal
history is shared with the exact standard assistant; no shadow transcript is used.
Applications must not externally replace or edit the shared context while active.

## Deliberate exclusions

The selected route rejects all configured tools, nonblocking tool policies,
resumption, runtime text/run requests, context editing/replacement, summaries,
image/file input, foreign transcription and unsupported live settings before
history mutation. Unexpected provider tool packets fail before any of that
packet's model output is dispatched. Raw unbound tools retain their existing
behavior. This mode does not implement tool-result reconciliation or Gemini 3
initial-history nudges. Eager/SmartTurn and automatic summary combinations are
not part of this initial mode.

A nonempty initial context requires `initial_seed` explicitly matching the
pair's plain legacy text context. Seed size and shape are validated before
connection and revalidated at Start. It is sent once; subsequent local snapshots
are never provider seeds. System/developer text must match configured setup.
`user_id` supplies the final transcript label.

## Lifetime and limits

Default maxima are 64 KiB per fragment and retained suffix, 256 KiB queued final
payload, 128 held events/4 MiB held payload, 256 outstanding exact identities,
one pending commit, and 1 MiB retained local snapshot payload. Options can lower
storage bounds. Checked overflows, bounds, consumer failures and deadlines fail
closed instead of truncating speech or releasing uncommitted output. These are
logical buffering limits, not a hard process RSS cap. Ordinary provider/core
queue limits still apply independently.

End first sends one exact registered, noninterruptible Normal Flush marker through
the actual user and service. Its service consumption acknowledges completion of
prior input handlers, not a socket write or a history commit. Only then does
provider finish begin. Source timeout finalization and real history commits
remain active during drain. Late application audio is rejected; already extracted
late VAD observations cannot send fresh provider activity after the marker.

Cancel/Stop revoke releases, discard unfinalized source suffix and held output,
and preserve already accepted standard user speech for its actual terminal flush.
Shared completion is cancellation-safe: dropping the End waiter cannot lose the
only shutdown owner. Normal cooperative completion awaits the inner provider
worker, then the pipeline joins its tracked receive task. A forced supervisor or
explicit cooperative-join deadline abort has a narrower guarantee: the existing
session destructor requests inner abort, but does not prove an awaited inner
join. Actual outer pipeline and loopback peer joins are reported separately.
The default consumer deadline is 5 s; drain and cooperative settlement each have
a finite 30 s default. Configure pipeline shutdown accommodation accordingly.

## Evidence boundary

Reference behavior is pinned to ProfHepta/pipecat commit
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, including Gemini source finalization
and its model-before-input public dispatcher order. Tests use synthetic local
WebSocket packets and the actual standard pair. No live Google account, remote
provider quality, billing, reconnection/resumption, or production deployment is
certified by these loopback checks. Enabling the feature adds optional turns/text
normal dependency paths. In the locked normal dependency graph, the default
Gemini feature set has no ORT dependency, while this feature reaches the existing
`pipecat-vad` ORT/ORT-sys 2.0.0-rc.9 dependencies through turns. This dependency
change does not load a model or execute inference; the paired-history tests do
neither. Dependency-tree evidence is separate from runtime/model execution claims.
