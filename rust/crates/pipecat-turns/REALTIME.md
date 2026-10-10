# Paired realtime aggregation

`LLMContextAggregatorPair` shares one realtime configuration, pending user
transcript, and handoff clock. Existing pair constructors select
`RealtimeServiceMode::Auto`: ordinary cascade behavior continues until a
`LlmServiceMetadata` frame declares a realtime service. Activation is sticky;
non-realtime metadata does not turn it off. Use `with_realtime_mode` or
`with_shared_context_and_realtime_mode` for `Enabled` or `Disabled`. The existing
user/assistant config structs and cascade constructors retain their fields.
Standalone halves reject realtime activation: sharing a `SharedContext` alone
does not install the paired handoff or its output route.

## Strategy and context rules

Metadata recommendations are independent of mode. Native descriptors support
VAD/external starts and speech-timeout/external stops. An explicit native start
or stop strategy list, or an explicitly selected native model-completion gate,
takes precedence over the entire recommendation. Unsupported
recommendation kinds, invalid times and excessive lists fail before activation;
opaque local plugins, eager, and LLM-completion descriptors are not dynamically
instantiated. External recommendations use the source's 500 ms transcript wait.
An explicit scalar `enable_interruptions=false` also remains a veto.

Enabling realtime removes transcription-only starts and clears transcript waits
on the active default/custom stop policies. Every realtime metadata broadcast
reapplies the mutation after recommendation adoption. Non-realtime metadata may
adopt a new recommendation without this mutation, matching the source dispatch.
Empty-turn recovery is disabled: a missing transcript is not missing model audio.
A realtime turn-stop emits `RealtimeUserTurnStopped`, whose missing text is
intentional; the later `UserTurnMessageAdded` event contains committed final text.
Interims and eager predictions never become final user history.

Assistant response start arms a deferred flush for STT metadata's TTFS P99, with
a one-second default. A newer start replaces that deadline. The user's existing
owned supervisor handles it even if no further user frames arrive. Response end
flushes remaining text first. Native assistant text commits, interruptions and
playback receipts also flush pending user text before the assistant text commit,
so delayed scheduling cannot reverse those messages. Multiple transcript pieces
within the window concatenate; a piece after the timed flush and before response
end becomes a second user message, as in the source. Tool declarations and their
placeholders remain immediate, so they can precede a late transcript; this does
not promise provider conversation-item ordering or alter tool-result ownership.

Only realtime trailing-write snapshots carry the bounded semantic marker
`metadata["realtime_context_update"] = true`, using core
`REALTIME_CONTEXT_UPDATE_METADATA` and `Frame::is_realtime_context_update()`.
The helper recognizes only non-speculative context payloads. A provider must treat these as
state-only updates, **never** a request to seed provider conversation history or
to run inference, even for the first snapshot in an audio-only session. Explicit
`InputText`, `LlmRun`, and message updates requesting inference keep their normal
unmarked context semantics. This marker communicates intent, not authentication
or permission. The core frame payload and network schemas are unchanged.

An assistant-triggered flush is represented by `RealtimeUserContext` in an
`AggregationOutput`. Its frame is inspectable by deterministic callers;
`AggregationOutput::emit` routes it from the paired user's downstream boundary.
Sending it upstream from the assistant would erroneously request inference.
Tool-result continuations remain upstream context requests, with existing group,
user/model activity, bot-speaking and playback deferral rules unchanged.

## Ownership and failure behavior

Put the user before the provider and the assistant after delivery in the same
runtime. Runtime processing binds the user route and rejects a missing/misordered
realtime pair. A single session owns these processors. End/Stop/Cancel cancel the
handoff deadline and preserve already accepted final transcript text; cleanup
releases the route and pending causal contexts. `prepare_end` stops the supervisor
before task draining. No task is spawned per response and no lock crosses an await.

Core Stop intentionally skips processor cleanup. Each half therefore releases
shared runtime route/cause ownership after attempting its terminal effects,
including when admission fails (the original error is preserved), while
retaining only a non-owning processor identity for the
remaining terminal topology checks. History stays inspectable with the pair
alive. These aggregators are single-session: Stop already ends their controller
and timer state, so a stopped pair cannot be restarted or rebound. A new runtime
is rejected immediately rather than entering a partially closed session. Build a
new pair over the retained SharedContext to continue a conversation.

Lock order is user state → assistant state → shared handoff → conversation; no
assistant operation enters the user's state lock. A routed effect's generation
check and nonblocking enqueue are serialized. A newer nonempty flush supersedes
older retained snapshots; an empty failsafe does not revoke an otherwise valid
snapshot. Generation increment is checked,
not wrapping. Timed effects retain the assistant start invocation's raw causal
scope, including inherited origins, in one shared revocable slot keyed to the
effect generation. Inspectable or cloned outputs own no ProcessorContext; a
terminal or cleanup revokes that slot even if the caller retains an output after
successful or failed emission. A revoked captured cause never falls back to the
current caller's scope. `try_push_from_invocation` bypasses the
ambient interruption-survival override at both construction and admission. A
stale effect cannot become current merely because the supervisor survives.
Explicitly uninterruptible frames retain core survival semantics.

The pending realtime transcript is limited to 1 MiB, checked before concatenation.
This is a native resource bound absent from Python. Existing total conversation
history remains application-owned and may require the context summarizer. Frame
projection and user-history append are atomic: an unrepresentable history leaves
pending transcript intact. Playback confirmation validates this projection before
consuming its receipt or adding acknowledged words, so a failed projection is
retryable without duplicating those words. Once committed, a routing/backpressure error is
reported rather than rolling back truthful user history or replaying it as another
message. Interruption can suppress stale network effects while preserving that
history. Providers must support idempotent state updates and their own session
lifecycle; these aggregators do not execute tools or certify provider delivery.

## Explicit limits

- `SmartTurnUserAggregator` owns a separate model supervisor and does not yet
  integrate realtime route binding and handoff wakeups. Wrapping an Enabled pair
  is rejected before configuration changes. An Auto pair already wrapped by
  Smart Turn rejects realtime metadata at either half before activation or
  strategy/history mutation. Use Disabled for cascade Smart Turn: its explicit
  model gate remains authoritative over service recommendations. Supporting the
  wrapper in realtime requires a separately qualified supervisor integration
- Eager speculative inference and realtime mode cannot share a pair. Explicit
  Enabled rejects that configuration; Auto reports the conflict at activation
- Signed cascade continuation carriers are rejected in active realtime mode;
  this is not a signed realtime-history adapter
- `AssistantTextMode::Spoken` continues to require actual delivery receipts.
  Response completion, PCM generation and transcript generation prove no playback
- `OpenAiRealtimeProcessor` supports the paired topology, auto-enabling metadata
  and state-only contexts documented in
  [AGGREGATION.md](../pipecat-realtime/AGGREGATION.md). Its bounded text/history and
  native tool-receipt contracts have synthetic local protocol qualification;
  unsupported rich history and arbitrary replacement/transforms remain excluded
- This common layer does not qualify Grok, Gemini Live, OpenAI or any live provider.
  Provider topology, context seeding, proposal routing, tool-result transport and
  audio-only startup require separate combined acceptance tests
- Python callback timestamp payloads and queue-inspection coalescing are not added
  by this slice. Native events and deterministic outputs retain their documented
  API; no Python code runs in the runtime or tests

## Source map

Baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.

- `src/pipecat/processors/aggregators/llm_response_universal.py`
  blob `7c18aa616c9e8e5b00e7518d2af1df57c647d631`, SHA-256
  `b44680dd1804a7a7c23de029f1e89c65a6045842b2cc4e24c32ce7475ae46642`
  - 935–1013: terminal flush and realtime policy mutations
  - 1015–1191: service recommendations, sticky auto mode, TTFS and failsafe
  - 1370–1464: event-only inference/stop with deferred user content
  - 1828–1883: assistant auto mode, paired topology and proposal ownership
  - 2103–2137: upstream context requests for tool continuations in both modes
  - 2279–2309, 2594–2649: paired handoff and pair construction
- `tests/test_context_aggregators_universal.py` 2899–3238,
  SHA-256 `cfbc1b1a22bc8f4b3940fea41b554bd2798e77de11445a5bc26183064bc1ca0c`:
  explicit/auto/off, recommendation precedence/rebroadcast and trailing writes
- `src/pipecat/services/stt_latency.py`, SHA-256
  `cd1b1969ceedb228965d88ecc2a9e10bb9c615e3e7c8b332fe6b89d7c2e12227`:
  one-second default TTFS

Native deterministic cases are in `tests/realtime_aggregation.rs`, paused-time
pipeline cases in `tests/realtime_aggregation_runtime.rs`, and raw causal-origin
regressions in the core processor's `causal_scope_tests` module. These are manually
mapped source contracts and adversarial lifecycle tests, not execution of Python
or live-provider qualification.

## Explicit native context intent

Active realtime pairs attach a process-local `NativeRealtimeContextCause` to
explicit typed input, run requests, appends, replacements and transforms. The
cause identifies one producer and checked monotonic sequence, records the exact
append range (rather than inferring changes from equal text), and seals the
bounded snapshot with streaming SHA-256. Direct function tools retain their
local callback identity without serializing or retaining callbacks in the seal.
The cause and provider admission cursor retain constant-size intent state, not
an operation log or another conversation-history copy. Cascade/Disabled behavior
is unchanged.

An explicit `run_llm=false` mutation emits its own uninterruptible context cause
without requesting inference. A provider may admit its history immediately, so
this is an intentional native synchronization timing difference from Python's
user-first local-only update. A later state-only trailing snapshot cannot swallow
or duplicate that explicit edit. Repeated identical typed text remains distinct
intent. An unconsumed cause cannot cross any native frame wire encoder.

Original emission binds a non-owning invocation fence after the explicit turn's
own interruption. It captures the raw generation and every inherited origin;
background-task survival cannot erase those origins. Providers keep durable
history admission separate from inference, preserve the original fence through
queued/deferred inference and output, and recheck it at actual socket admission.
Local enqueue or socket admission is not provider confirmation. Completed or
expired runtimes invalidate the fence, and retained cause clones own no runtime.

Provider admission validates the entire operation before changing remote state.
Unsupported replacement/deletion/transform may be rejected after the application
has already edited local history, which is the source-compatible local mutation
boundary. An error does not roll back unrelated assistant/tool edits. If snapshot
projection or sealing fails, the pair preserves its local edit, poisons further
explicit admission before another mutation, and raises a fatal session error.
Repair retained history and construct a new pair and provider session to resume;
retrying the old edit must not silently duplicate or discard it.

Explicit positional edits also capture the shared context's lineage while holding
its mutation lock. Snapshot projection checks that lineage under the same context
lock; a replacement, transform or summary between mutation and projection fails
fatally before sealing, sequence advancement or assistant inference acknowledgment.
Current local history is retained as-is, including a concurrent replacement; it is
not rolled back or described as still containing an edit that another writer removed.
Ordinary concurrent appends preserve earlier indices and remain supported. The
assistant→context lock order is unchanged, and results arriving after the actual
inference snapshot still owe continuation. Signed cascade contexts remain unsupported
in active realtime aggregation; their suffix reconciliation is not a realtime edit API.

### Assistant inference scheduling

Realtime assistant continuations use a separate bounded producer and the sealed
`AssistantContinuation` intent. The assistant creates it only after existing
user-speech, grouped-tool and delivered-playback gates allow scheduling. It is
bound at original assistant emission, never refreshed when a provider becomes
ready or an active response ends. Providers validate direction before effects:
assistant continuations are upstream and request inference only; user edits and
runs are downstream. Separate constant-size admission cursors preserve each
producer's replay/sequence contract. Retained tool history and settlement remain
durable even when the original inference fence expires. Failure to seal or emit
an assistant request is fatal, preserves local history, and requires a fresh
pair/provider session; sealing failure does not clear owed inference. Explicit
Disabled cascade contexts retain their previous unsealed behavior.

The assistant also seals a tiny optional native-terminal scheduling reason: the
immutable outcome ID and its native interruption policy, without retaining the
receipt or result. It selects this reason only within the existing eligible
result/cancellation branch after native validation and deduplication. One pending
reason follows the coalesced owed/group lifetime and is consumed by `clear_runs`.
A provider's gate-reevaluation `LlmRun` creates no new native reason. Native false
cancellation creates neither a new owed flag nor a reason, but reevaluates an
already-owed request after clearing its group membership. Exact terminal replay
cannot schedule a second cause. Raw callback cancellation behavior is unchanged.

A newly eligible terminal whose immutable native policy is
`cancel_on_interruption=false` may arrive through an old durable delivery
invocation. At actual assistant admission, `try_push_realtime_context` binds this
never-bound new request to the current local generation (always `Some`) and
queues it within the same ordered admission lock. Inherited origin restrictions
remain exact. Cancel-enabled terminal requests retain their old raw invocation
scope. A previously bound cause never refreshes, and later interruption still
invalidates the admitted request before provider inference/output. Finished
runtimes reject admission. This is deliberately limited to new shared-assistant
scheduling after validated native reconciliation, not a revival of old requests.
