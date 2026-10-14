# Cartesia pronunciation preparation

`CartesiaPronunciationTransform` is an explicit, bounded prepared-input adapter.
Build it from `(word, IPA)` entries, then call `apply_to_input` on an existing
`TtsTextInputFrame` before submitting that frame to a Cartesia TTS processor.
Only `synthesis_text` is replaced after the entire transformation succeeds.
`display_text`, `original_text`, `append_to_context`, `skip_tts`,
`includes_inter_frame_spaces` and `aggregated_by` are preserved. A skipped input
is left unchanged. Failure leaves the input unchanged; the caller must handle
the error rather than submit an unintended untransformed fallback.

The existing HTTP and WebSocket processors already consume these three channels.
No automatic provider hook, network call, frame-schema change, new provider
capability or change to default synthesis is introduced. This does not implement
the source's arbitrary `TTSService.text_transforms` callback list or its dynamic
pronunciation-capability gating for other providers. Apply any desired markup
cleanup before pronunciation preparation so that later transforms do not destroy
the generated `<<...>>` blocks. The server-side `pronunciation_dict_id` option
remains a separate existing capability.

## Fixed source contract

Source revision: [`af9f9da9d7afddaa8811c0e0251b32566c980bdf`](https://github.com/ProfHepta/pipecat/tree/af9f9da9d7afddaa8811c0e0251b32566c980bdf).

| Source path | Git blob |
| --- | --- |
| `src/pipecat/utils/text/phonemes.py` | `a71d06854e74a2069b4ba7e550aa4126b17fbf6d` |
| `src/pipecat/utils/text/transforms/pronunciations.py` | `47591a62188c7f873bcca62f76267c2a27a60d0b` |
| `src/pipecat/services/cartesia/tts.py` | `70c492f5bb63402b6ba7d7c3a3f049fc8d84b86c` |
| `src/pipecat/services/tts_service.py` | `428b875796d0cf7ed07b89537be8a440ccc104f6` |
| `tests/test_tts_pronunciations.py` | `29e6f25449c236994a5c6e0cac870fb2c07b1fb6` |

Normalization composes NFC, strips one matching outer `/.../` or `[...]` pair,
unifies notation and collapses whitespace. Tokenization preserves the source's
tie-bar, common affricate/diphthong, length and nonspacing-diacritic behavior.
Stress is moved to the following vowel, or dropped when no vowel follows.
Each whitespace-separated IPA word becomes a `<<phone|phone>>` block.

This is notation processing, not an IPA validator. Unknown characters remain in
the source-defined output, including markup characters. A normalized-empty IPA
is unusable and its dictionary entry is omitted. Pure stress is nonempty input
and produces `<<>>`, as in the source. The adapter does not promise that a real
Cartesia service accepts or correctly pronounces arbitrary generated markup.

Dictionary keys are stripped, empty keys ignored, and usable keys matched
longest first. Duplicate case-folded keys replace the earlier pronunciation
without changing its insertion position; an unusable duplicate does not remove
an earlier usable entry. Equal-length entries retain insertion order. Matches
cannot be adjacent to a word character or ASCII hyphen. Replacements scan the
original input once and are not recursively transformed.

## Explicit native profile

- Dictionary keys must be ASCII after trimming. Matching uses ASCII case folding;
  Python's non-ASCII `IGNORECASE` equivalences such as `İ`, `ı`, `ſ` and `K` are
  outside this profile.
- Neighboring word characters use this crate's fixed Unicode 15.0 letter/number
  tables plus underscore, with ASCII hyphen separately excluded. Combining marks
  and other connector punctuation are not automatically word characters. This
  deliberately avoids Rust regex's broader `\w`; Unicode database differences
  from a particular Python interpreter are not claimed equivalent.
- Pronunciation matching is not an HTML, SSML or Cartesia-markup parser. A key
  inside pre-existing `<<word>>` or an attribute can match, just as with the
  source's regular expression. Original markup remains in display/original
  channels, and inserted replacements are not rescanned.
- `PronunciationLimits` bounds visited entry count, raw key/IPA bytes, cumulative
  raw-key + raw-IPA + formatted bytes (including overwritten/ignored entries),
  input bytes and output bytes. Defaults are respectively 256 entries, 256-byte
  keys, 2 KiB IPA, 256 KiB cumulative content, 64 KiB input and 256 KiB output.
  Hard ceilings are 1,024 entries, 1 KiB keys, 16 KiB IPA, 4 MiB cumulative
  content, 1 MiB input and 4 MiB output. Limits are positive; output expansion
  is checked before publication. Normalization/phone text also has a separate
  128 KiB working-string ceiling. A skipped input bypasses transformation and
  its text limits because it is never sent for synthesis by this adapter.
  These content budgets do not claim to include allocator bookkeeping, the
  caller's input allocation or all temporary normalization allocations.
- The transform is synchronous and finite under those limits. It does not offer
  a preemptive timeout or rollback of a provider request already submitted by
  the caller. Prepared frame submission retains the existing pipeline authority,
  cancellation and generation checks.
- Construction's duplicate lookup and each application also have an 8 MiB
  conservative key-comparison work budget. A key comparison is charged before
  execution, including unsuccessful candidates, and exhaustion returns
  `WorkLimitExceeded`. This prevents the largest byte/count configurations from
  implying an unrestricted product of input length, dictionary size and key
  length. The work units are not elapsed-time guarantees or allocator accounting.

## Evidence boundaries

The committed source golden is produced by executing unchanged selected Python
AST definitions with minimal import stubs, after verifying fixed Git blobs. It
is not a complete Python TTS service run. Native tests additionally cover the
restricted profile, bounds, atomic mutation and provenance preservation.

Loopback provider tests use the production Cartesia clients/processors with
synthetic PCM, timestamps and playback receipts. They establish request text and
native display/source/history mapping, not actual pronunciation quality, audible
playback, live provider authentication or real service timestamp behavior for
IPA markup. Timestamp-free reconstruction remains subject to existing alignment
evidence rules and must not be promoted to heard history without valid receipts.
