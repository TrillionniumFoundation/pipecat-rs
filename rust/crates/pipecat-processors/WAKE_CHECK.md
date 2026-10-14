# Legacy participant wake-check adapter

The opt-in `wake-check` feature exports `WakeCheckFilter` and
`WakeCheckFilterConfig`. It supports migration of an old pipeline that used the
Python `WakeCheckFilter`; it is not a new authentication or user-mute boundary.
For new pipelines, the existing `pipecat-turns` WakePhrase user-turn start
strategy remains the recommended path. Existing user mute policies are separate.
The facade enables this adapter with its same-named feature.

## Fixed source contract

Reference: `src/pipecat/processors/filters/wake_check_filter.py` at
`af9f9da9d7afddaa8811c0e0251b32566c980bdf`, Git blob
`70a233000b5498c98b1747e15d94ab58bae7c844`. The source marks this class deprecated
since 0.0.106, with removal planned for 2.0.0. The accompanying source fixture
executes the original class using frame, processor and clock stubs, rather than
claiming to run the complete Python pipeline.

Only `Transcription` and `TranscriptionDetailed` participate, including a
Detailed frame with `finalized = false`. Interim, eager and translation variants
are distinct source classes and bypass this filter, as do other frame kinds.
Bypassed frames retain their direction. Accepted transcriptions travel
**downstream**, even when received upstream, matching the source's default push.

Each exact participant ID has independent idle text and an awake window. Idle
chunks concatenate verbatim, with no inserted separator or normalization. Phrase
configuration order wins over the globally earliest occurrence; within a phrase,
the first match wins. On a match, the current frame's text becomes the accumulated
suffix starting at the wake phrase, including the phrase itself. All other
current-frame fields, identity, local tags and metadata are preserved. Previous
chunks do not contribute their metadata to the current frame.

While awake, the original frame passes unchanged and refreshes a sliding timeout,
including when its text is empty. The default interval is three seconds and an
elapsed interval equal to the timeout is expired. Interruption preserves the
input accumulator and window. This is user-input state, not a bot-output epoch.

State commits before forwarding, just as in the source. A subsequent pipeline
push failure is returned and does not roll back the state; queue admission is not
a delivery receipt. State locks are released before calling the pipeline.

## Bounded native profile

The matcher accepts nonempty phrases with ASCII literal tokens; token separators
use Python's Unicode whitespace definition. Matching input remains Unicode.
Tokens are joined with zero-or-more whitespace, including zero characters.
Literal punctuation is escaped semantically and both ends use the actual Python
word-boundary XOR rule, not a blanket prohibition on adjacent word characters.
Unicode 15 letters, numbers and underscore are word characters; combining marks
are not. ASCII case folding includes Python's special input characters
`İ`, `ı`, `ſ` and `K`. There is no normalization or `ß`/`ss` expansion.
Non-ASCII literal tokens and empty source patterns are explicitly unsupported.
Matcher ranges are UTF-8 byte offsets, unlike the text crate's scalar cursors.

Default matcher bounds are 32 phrases, 256 bytes per raw phrase, 8 KiB total
phrase bytes, 16 KiB input and 1,048,576 search work units. Independently enforced
hard ceilings are 128 phrases, 1 KiB each, 64 KiB total, 64 KiB input and 4,194,304
work units. Every bound must be positive. Work exhaustion is an error, never a
false no-match result. Synchronous matching is bounded but not preemptible.

Processor defaults and hard ceilings are 64 participants, 512 bytes per ID,
16 KiB idle text per participant and 128 KiB total retained ID/text content.
Smaller bounds can be configured. Empty IDs are valid. Participants are never
silently evicted, so reaching the participant cap requires a fresh session or a
configuration/pipeline change. A prospective accumulated candidate must fit all
bounds before matching, even if a match would immediately clear it. The content
budget excludes allocator/container overhead and untouched awake/bypass frame
payloads. The bounded candidate allocation exists transiently alongside retained
state. Phrase storage has its separate matcher budget.

Capacity, work and authority errors leave participant state unchanged, including
expired windows and prospective new participants. Idle native-authority carriers
are rejected before text is copied, accumulated or rewritten; awake forwarding
preserves the complete original carrier. This does not turn the wake filter into
a security boundary, especially because nonselected variants bypass it.

The timeout must be positive and at most 24 hours. Native time uses one monotonic
Tokio Instant per transcription rather than the source's separate wall-clock
check and refresh calls. Concurrent out-of-order samples cannot move a stored
refresh backward. No timer task or background worker is created.

Each successful native setup starts a fresh map. Actual End, Stop and Cancel
clear and close it; the source's object-lifetime map has no equivalent reset.
A weak, generation-free setup owner rejects concurrent reuse until the old
runtime finishes. Failed concurrent setup cleanup cannot erase the live run, and
old runtime cleanup cannot erase a replacement run. Completed sequential reuse
starts fresh, including after Stop. Owner destruction relies on normal core
cleanup; there is no detached task or retained strong runtime owner.

## Verification scope

The fixed oracle contains 48 frame traces and 72 matcher cases. The supported
matcher profile accepts 69 cases and explicitly rejects three unsupported
configurations. Native tests cover Unicode boundaries and slicing, ordered and
cross-frame matches, rolling windows, per-participant isolation, resource-error
atomicity, native authority, push failure and session lifecycle. Real facade
fixtures route accepted frames through `LLMUserAggregator` and a loopback
HTTP/SSE provider while proving closed transcripts create no context or request.
These use synthetic local data and do not qualify a live provider, browser,
physical playback, speech recognition or the complete Python runtime.
