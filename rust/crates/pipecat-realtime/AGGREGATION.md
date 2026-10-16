# Shared realtime aggregation adapter

Use this topology in one pipeline and session:

```text
paired user -> OpenAiRealtimeProcessor -> ToolExecutor
            -> OpenAiRealtimeToolResultRelay -> output -> paired assistant
```

Use the pair's Auto or Enabled realtime mode for this topology. Disabled-mode
legacy upstream context requests are outside this adapter contract and fail
explicitly; direct service controls and the low-level client remain available.
Omit executor and relay for an agent without tools. The user must precede the
provider and the assistant must follow delivery. Startup broadcasts realtime
service metadata to both halves. Server turn detection recommends external
start/stop strategies; manual mode makes no such recommendation. Server speech
activity becomes upstream proposals, which the user resolves using its actual
strategies and interruption policy. Server transcriptions travel upstream to the
user. Response, text, audio and tool frames travel downstream. Neither service
metadata nor transcriptions request inference by themselves.

## Context ownership

`with_initial_context(LlmContextData)` explicitly seeds the provider without
inference during setup. Initialize the pair with that same history. Alternatively,
a first unmarked plain/detailed context explicitly imports its history and asks
for a response. An empty audio-only session needs neither operation.

A context whose `is_realtime_context_update()` helper returns true is a trailing
local write. It never imports items, changes settings or creates a response,
including when it is the first context received. Duplicate snapshots are
idempotent; a stale shorter snapshot does not rewind the accepted local prefix.
A conflicting trailing rewrite fails explicitly. These frames are consumed in
either direction rather than allowed to circulate through the pair.

Unmarked downstream contexts reconcile an append-only local history and request
inference. Upstream inference requires the paired assistant's sealed
`AssistantContinuation`, with an independent producer admission cursor. Missing,
stripped or wrong-direction carriers fail before provider effects; direct public
InputText/LlmRun controls retain their own invocation authority. Already observed server user transcripts, assistant text and
tool declarations are not recreated on the server. Tool placeholders can become
final outcomes; unrelated edits or history truncation require a new session.
Reconciliation validates the entire projected context and reserves the whole
command batch before updating local bookkeeping. Saturated queues therefore
cannot partially enqueue a seed, tool update or inference request.

The local ledger records admitted commands, not provider acceptance. Conversation
item/session commands survive an interruption while their associated inference
request does not. They still wait for session readiness. There are no implicit
retries or reconnects. Any provider `error` event closes the frame adapter session
with an explicit error, preventing reuse of possibly rejected history. The
standalone client retains its separate exact-request response-create recovery.
Provider item confirmations and loopback wire observations do not establish that
a live model used a context correctly.

Plain contexts and detailed text/string-part contexts are supported without
lossy projection. The first system message becomes session instructions, with
explicit initialization instructions taking precedence. Seed history follows the
pinned adapter: remaining system/developer roles become user, one user message
stays a normal item, and other histories are packed into one user item using the
source introduction, Unicode pretty JSON and trailing instruction. Later appends
remain separate items. This reproduces conversion without qualifying live model
or audio behavior. Function
schemas and direct-function schemas are accepted; direct callbacks still execute
in the tool executor. `NotGiven` tools preserve configured tools; an explicit
empty function list clears them. Auto/none/required tool choices are supported.
Named choices, grouped/provider-specific tool schemas, named messages, opaque
extensions, multimodal/signed context and speculative inference fail explicitly.
Image input and output-mode/prompt-cache/summarization controls also fail
explicitly. Manual mode commits on resolved UserStoppedSpeaking. Local interruption
uses the bounded, VAD-autosized audio window described in [manual-turn pre-roll](PREROLL.md):
clear, replay the successfully flushed suffix, then the existing cancellation and
acknowledged truncation. An explicit duration overrides automatic sizing.
Audio/video pause controls remain upstream; server-VAD input does not use this
pre-roll window.

The paired user emits sealed, process-local causes for InputText, LlmRun and
message edits. Each binds the exact bounded snapshot, producer sequence, append
range and original invocation fence. The adapter checks the seal before the
state-only marker, silently consumes an exact admitted replay, and rejects
forged/gapped/foreign/unbound causes before provider mutation. Explicit appends
retain ownership even when a later trailing snapshot overtakes them or contains
identical audio text. A no-run append synchronizes its durable item immediately
without inference. Stale inference is suppressed while its durable history and
sequence remain admitted. This is an explicit native synchronization timing
difference. Replacement and arbitrary transforms require a new seeded session.

Original user and assistant invocation fences survive actor readiness/deferred-response waits.
Socket readiness is rechecked under that fence through start_send; after local
sink admission the bounded flush completes without retry. This does not establish
remote acceptance. Response output is admitted under the same original fence.
Direct service inputs also support InputText, LlmRun, plain/detailed
append and append-only update frames; they are consumed to avoid a second request
from the assistant. A direct transform is unsupported. Live cause snapshots do
not restore old tool settings; explicit tools/choice controls take precedence
over delayed initial snapshots. Arbitrary settings reorder parity is not claimed.

## Tool and interruption ownership

Validated calls dispatch immediately at arguments.done, including before
response.done, as in the pinned Python service. The native executor admits each
actual batch's placeholders and performs callbacks once. A provider response
may have more than one native executor batch. The adapter holds an upstream tool
continuation while any known provider call remains unresolved. The held request
retains its original weak invocation fence. When the last outcome arrives and
that authority remains current, it requests a downstream LlmRun re-evaluation by
the assistant, which still owns current user/model/playback gates. Permission is
intersected with the held original authority through actual socket admission;
merely enqueuing a gate recheck does not transfer or renew that authority. A
genuinely new eligible native terminal can establish a new assistant request:
the assistant selects its receipt reason, and the provider matches that ID to a
previously validated outcome revision. It does not copy the assistant's
null/error/group eligibility policy. Durable outcomes remain independent of
stale inference. Result revisions coalesce
duplicate tool continuations; explicit downstream LlmRun/InputText requests keep
their own inference intent. The client separately serializes active responses.

The relay sends each downstream final outcome upstream before forwarding it to
the assistant. The provider validates identity/arguments, sends the outcome with
`run_response=false`, and consumes that upstream copy. It never independently
infers from a tool result. Duplicate identical final outcomes are idempotent;
conflicting or unknown outcomes fail. Null means COMPLETED and structured errors
match the assistant's stored error envelope. Raw cancel frames are requests for
the executor and never provider settlement. Native terminal receipts are borrowed
and validated, retained for the assistant, and verified timeouts project to
CANCELLED while retaining their Failed/TimedOut provenance. Interruption cancels
inference but waits for actual executor terminal receipts, preserving success
admitted before interruption even when its relay delivery is delayed. There is
no invented rollback. Announced calls retain bounded ownership until settlement;
missing/dropped terminal delivery fails closed after response_timeout rather
than guessing an outcome. Unannounced interrupted calls are discarded.

All live tool settlements require a native receipt. Unreceipted plain/detailed
result frames and new tool outcomes in submitted contexts are rejected before
provider effects; an invalid receipt never falls back to legacy behavior.
Explicit initial saved-history seeds may contain past tool messages. Use the
standalone Action API for separately owned low-level tool output. Existing
confirmed tool history remains reconcilable without duplicating its wire result.
To match native shared history, only null maps to COMPLETED; false, zero and
empty JSON values remain literal. The pinned Python implementation treats all
falsy values as COMPLETED. This is a documented native semantic difference.

Intermediate results and deferred/non-cancel-on-interruption asynchronous tool
context envelopes are explicitly unsupported in this frame adapter. Use ordinary
final-result callbacks with cancellation enabled. A narrow scheduling regression
also covers a non-cancel-on-interruption callback's receipt-first ordinary final
projection before any pending async envelope snapshot is imported, without an
intervening user turn; it does not
qualify general asynchronous context conversion. The standalone client retains
its typed intermediate-result API, which does not by itself qualify shared-pair
continuation. This integration requires the typed native terminal receipt and
assistant history dependencies described in its joint validation manifest.

## Playback and lifecycle

Audio transcripts emit a non-context-appending LlmText notification plus TtsText
carrying the response context. Generated PCM, transcript text and response.done
are never played receipts. Spoken assistant mode requires the output's actual
validated confirmation API. The output also calls `acknowledge_playback` with its
provider item/content offset. That offset must belong to this session/generation,
not exceed generated audio, and be monotonic for the item. An interruption cancels
generation and truncates only an acknowledged item at that exact offset. Without
a receipt no played offset is guessed. BotStoppedSpeaking clears stored playback.

Each processor is single-session, including after Stop. End/EndDetailed use ordered
finish; Stop/Cancel/CancelDetailed close the socket and release local state.
Cleanup is idempotent and mapping/route errors close the owned session. Build a
new processor and pair to restart. The receiver is gated until startup metadata
is enqueued. It is one owned task, not a task per response. No context lock crosses
an await. The adapter's history/ownership ledger shares a 1 MiB
(default max_message_bytes) encoded budget and 1024-entry limits, including the
native receipt-ID/revision ledger. One held continuation retains at most 64 weak
raw scopes through checked intersection. Session settings
and individual commands have separate max_message_bytes limits. Emitted audio
item receipts and recent response correlations are each limited to 64. The
session's max_outstanding_tools bounds live provider calls; each call argument
buffer is also bounded by max_message_bytes. Input/output command queues are
bounded by queue_capacity, so these are component bounds, not a single RSS cap.
Long histories need a new explicitly seeded session; arbitrary in-place summary
replacement is not implemented.

## Source and evidence boundaries

Static reference only: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`. No Python runs.

- `src/pipecat/services/openai/realtime/llm.py`, blob
  `5e859077a8ee44057adb6d1902709e44e04885cd`, SHA-256
  `0f8659fa2f33e81649e1698f2d3394e338970d21854162567ebb1f2220804b89`
  - 544–578: manual mode, external strategy recommendations and metadata
  - 666–775: context direction, tool-result transport and append ownership
  - 1043–1070, 1137–1226: upstream transcripts, audio transcript frames and proposals
  - 1273–1378, 1494–1531: initial seeding, deferred response and tool outcomes
- `src/pipecat/services/openai/realtime/events.py`, SHA-256
  `5d5c71e9d7b55db93b2d1eaa4c5bde9e36edac6e8a5444dcde74963bb005ff98`:
  native GA wire shapes and typed events
- `src/pipecat/adapters/services/open_ai_realtime_adapter.py`, lines 104–222:
  first-system instruction and packed history source behavior; blob
  `037046e1531b927c2cc7b0fd4610e91bfca3e3b0`, SHA-256
  `feaac42e26194917e617168ba09b902a461edae4be00e03aa1cb973831e417d9`
- Shared aggregation contract: `pipecat-turns/REALTIME.md`, source baseline
  `3e0a4dd64f993d0a7de5c52d64b5dddd7ad5a820`, SHA-256
  `5ca7f62e2dfcfa3b45a57660190e3e86001a9c0ae96124b3da2681b816d2d244`

`aggregation_contract.rs` and independent `review_aggregation.rs` use actual
loopback TCP/WebSocket peers and actual paired aggregators/native tool executor.
They cover wire-silent first snapshots, automatic metadata, audio-only routing,
explicit seed/run, incremental history, tool groups, bot/playback deferral,
interruption/readiness, rejected items, receipt truncation and terminal cleanup.
`context.rs` has deterministic projection, bounds and mutation tests. Existing
session/protocol tests remain separate. These tests are synthetic local protocol
qualification; live OpenAI acceptance and physical audio playback are unverified.
