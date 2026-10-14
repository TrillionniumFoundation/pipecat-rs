# Explicit root-wide idle policy

`Pipeline::with_idle_timeout(WorkerIdleConfig)` adds a root-owned inactivity
policy. Existing pipelines remain disabled. Once explicitly enabled, the config
default is 300 seconds and `IdleTimeoutAction::CancelRoot`; choose `NotifyOnly`
to observe repeated timeouts without cancelling. `worker.idle_monitor()` exposes
a bounded latest-value watch of activities, timeout count, cancellation decision,
coverage loss and shutdown status. Children cannot configure or expose a separate
idle monitor. No event-handler callback, background monitor task or runner-wide
cancellation is installed.

Run `cargo run -p pipecat-core --example worker_idle` for a bounded,
credential-free notify-only example.

## Activity and identity

The initial native selector is exactly the source's default activity family:
BotSpeaking, InterimTranscription, Transcription, UserSpeaking and
UserStartedSpeaking; Start always counts. The detailed native transcription
representations also count. This does not include output audio, generic text,
Heartbeat, proposed/VAD boundaries, or arbitrary application frames. Custom
Python activity tuples are outside this initial native API.

Activity is the first successful native push admission of a logical frame at
any processor in the root composition, in either direction. Lifecycle-owned
children contribute even without inheriting observers. It is not a sink receipt,
client-input timestamp, user-turn deadline, proof of model health, or playback
confirmation. A single-position IdleFrameProcessor, conversation UserIdleTracker
and runner's incoming-client timeout remain separate facilities.

Every frame, including non-activity frames, is marked at its first push. Mutating
the same logical frame from Text to Transcription later cannot create activity.
Clones share native identity; `duplicate()` and wire reconstruction have fresh
identity, even if public IDs are reused. A new root has a new idle domain.
The private idle domain uses the same unique allocator as observer domains but
is never an actual ObserverHub's ID. It cannot steal an independent child/root
observer's first actual Push event. Ordinary observation remains unchanged.

There is no root-owned frame-history queue. The existing per-frame observation
ledger receives at most one extra idle-domain key per root. Before adding a new
idle key, its total current key count is checked against
`max_frame_observation_domains` (1..=4096, default 64). An existing idle key is
still a duplicate even above the cap. This limit controls idle insertion only,
not existing external observer bookkeeping. If insertion is impossible, the
snapshot permanently reports `coverage_lost=true` and this run stops idle
monitoring/automatic cancellation. Business traffic and ordinary observer
insertion continue. Fresh frames do not silently restore coverage. This is a
conservative visible loss of coverage, not a healthy result.

## Timer, cancellation and shutdown

After setup, the supervisor activates idle monitoring before processor actors
start, without waiting for Start to reach the sink. Successful activity sets one
coalesced pending bit. On supervisor consumption, a full interval begins anew.
At the interval boundary, pending activity wins over expiry. This is monotonic
consumption-time accounting, not precision wall-clock scheduling. The existing
10ms supervisor tick and executor scheduling affect notification latency; timeout
validation accepts 10ms through 24h. A delayed tick emits one timeout rather than
an unbounded catch-up burst. Watch updates coalesce, and counters saturate.

NotifyOnly repeats after each interval. CancelRoot makes one irreversible decision
at its first timeout, stops counting further timeouts, closes root input and
requests Cancel with reason `idle timeout`. It uses the existing bounded pending
terminal slot and retries if the system lane is full. New activity cannot retract
the decision. The existing finite shutdown deadline still applies. A processor
holding Start also holds its system lane: Cancel cannot overtake that handler,
so an indefinitely blocked Start produces `ShutdownTimeout` and cleanup rather
than pretending Cancel traversed successfully. No startup priority is changed.

Native idle monitoring stops as soon as the root closes input, including End,
Stop, Cancel or an existing processor-unusable terminal policy. It does not
replace a terminal already chosen by that policy, compete with graceful drain,
or restart afterward. Root heartbeat monitoring separately retains its documented
drain behavior. Child-local closure does not stop the root idle monitor. Final
cleanup freezes idle state and retains no frame payloads or extra monitor tasks.

## Fixed-source comparison

Reference: ProfHepta/pipecat revision
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, `src/pipecat/pipeline/worker.py`, blob
`fea182ed33490caad81d1dd821597cdfc49d99ae`.

The checked-in `tests/fixtures/python_worker_idle_source.json` is the unchanged
bounded original-AST replay. Its evidence includes the complete hash-gated source,
runner, provenance, deterministic dependency stubs and corruption-refusal checks.
The original observer class and eight worker methods execute; the observer's
first-push dispatcher, real scheduler and BaseWorker bus do not. Native tests
separately use real workers, observer dispatchers, queues and owned children.

Source enables 300s idle cancellation by default and may cancel its whole runner.
Native requires opt-in and only cancels its own root composition. Source allows
custom activity classes and keeps its idle task alive through End drain; native
uses the fixed initial selector and stops at input closure. Source's start helper
has no duplicate guard and cleanup clears the task handle, not its Event; native
has one lifecycle-bound state and permanently closes it at shutdown. Callback
aliases and arbitrary Python exception/observer behavior are not reproduced.
