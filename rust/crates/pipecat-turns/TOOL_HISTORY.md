# Assistant tool bookkeeping ownership and bounds

These are separate component limits, not one global 8 MiB budget and not RSS limits. Fixed-size tree/vector/node overhead is bounded by counts. Encoded strings/values are charged before retained copies; transient caller frames and application-owned conversation messages are outside these metadata ledgers. Saturated active authority rejects additional admission; it never evicts live calls. Full native-batch preflights include all missing siblings and the terminal presentation before any history change.

| Assistant field/component | Count bound | Payload/identity byte bound and ownership | Release/eviction rule |
|---|---:|---|---|
| native_tools.calls; native_tools.completed; retained terminal Arc | 1,024 calls; closed-group target512 | Shared8MiB component ledger: encoded policy, complete reachable batch per member conservatively, two additional ID copies for map key and eventual completed queue, optional complete receipt bytes | Active identities and completed siblings of any active batch never evict. Optional strong outcome payload retention evicts first; compact immutable publication IDs stay. Closed groups may evict. Cleanup/terminal clear |
| pending presentation map | 1,024 | Separate8MiB: map-key ID plus encoded optional policy including its second ID/arguments/group | Reject new admission or None→Some growth before context mutation; remove only actual terminal/provisional legacy cancellation; clear on lifecycle |
| declared_tools | 1,024 | Separate8MiB: full call plus map-key ID; bounded measurement before clone | Active ordinary declarations never evict. Remove on policy/terminal or legacy interruption; clear on lifecycle |
| signed_declarations | 1,024 | Separate8MiB: full call plus map-key ID | Preserve through response End until terminal; no active eviction; clear on lifecycle |
| provisional_tools.calls and order | 1,024 | Separate8MiB: full policy plus map-key ID and order-queue ID | Legacy-only restoration window, oldest may evict. Native declared ownership bypasses this window. Restore/remove or clear on lifecycle |
| completed legacy hints | 512 | Separate8MiB raw owned ID strings, measured before copy; duplicate insert does not add ownership | Oldest evicts by either bound. Oversized hint skipped. Native late declarations additionally consult protected compact native terminal markers, so hint eviction cannot reopen them. Clear on lifecycle |
| interrupted_declarations legacy hints | 512 | Separate8MiB raw owned ID strings, measured before copy | Oldest evicts by either bound; remove on policy or native reconstruction; clear on lifecycle |
| cancelled_run_groups | ≤1,024 live group labels | ≤8MiB implied by live pending policies (each stored distinct label corresponds to a live policy charged there); this is a separate duplicate copy bound | Every maybe_run folds closed groups into one owed boolean even when speech blocks inference; only live groups retain labels. Clear after inference/user-context handoff/interruption/lifecycle |
| run_owed; tool_run_owed; related activity gates | Fixed booleans | No retained strings | Cleared on inference/user-context handoff/lifecycle |
| signed_history.mirror.owned/results | 4,096 total entries | Separate32MiB; full encoded ContextMessage for owned entries, raw tool ID for mutable ordinary Tool results. Streaming measured before cloning. Map keys are fixed usize | No eviction. Combined missing-call+terminal additions and replacement reconciliation reject before context mutation. Text commit also preflights replacement. Dropped on signed-scope closure/cleanup |
| signed_history.snapshot | ≤1 native continuation, ≤1,024 blocks | Separate8MiB validated native continuation, individual signature≤1MiB | Exact-prefix/identity checks preserve signatures; drop at scope closure/cleanup |
| signed_history.delivered | 4,096 chunks | Separate8MiB delivered text | Reject before admission; drop at scope closure/cleanup |
| signed_history.playback | 4,096 IDs | Each ID≤1,024 bytes, therefore≤4MiB string data | Exact ordered confirmation removes; drop at scope closure/cleanup |
| completed_responses | 512 | Fixed u64 IDs, 4KiB value data | Oldest evicts; never a tool identity or runtime owner |
| seen frame IDs | 512 (existing RecentFrames default) | Fixed u64 IDs in bounded set and queue | Oldest evicts; native terminal duplicate authority bypasses mutable public frame IDs |

Executor pre-effect reservation equals the default native ledger's base cost: for each member, full encoded policy + full batch bytes + twice ID length. This conservatively dominates the default pending map's same-batch policy+key cost. Optional receipt payload retention cannot force terminal rejection; it is skipped/evicted independently. Independent prior consumer saturation and custom publisher configurations outside the domain still fail closed.

## Other assistant storage, outside tool metadata

- SharedContext, native originals, committed tool results and conversation messages are application-owned conversation history. Tool metadata eviction does not delete them.
- Existing text/thought/transcript and generic audio context/playback buffers are media state, not copies of native tool IDs. Signed playback and delivered state have the separate bounds above; generic unsigned media retention is not claimed globally bounded by this change.
- Existing callback_tasks tracks explicitly requested legacy ContextUpdated callbacks and their ProcessorContexts; native outcomes cannot carry those callbacks. Its lifecycle cancellation contract is unchanged. Immutable batches/outcomes/tool bookkeeping have no ProcessorContext, runtime or processor references.
- The realtime handoff and summarizer state are separate existing components, unchanged by this terminal patch.

The signed mirror cap is measured on actual encoded ContextMessages. Async JSON/string wrapping may amplify callback output, so the executor's 8 MiB native result-body cap is not a promise that every such output fits the separate 32 MiB signed mirror representation. Rejection is explicit and precedes context/mirror mutation; it never converts a successful effect into a claimed cancellation or retries effects.
