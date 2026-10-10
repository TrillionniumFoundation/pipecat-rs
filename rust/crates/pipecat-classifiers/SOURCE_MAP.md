# Classifier source map

Source baseline: `af9f9da9d7afddaa8811c0e0251b32566c980bdf`.
Native integration baseline: `8682f6e154a3f5241b0e20df83660fe000571faa`.
The referenced Python classifier files and tests are unchanged between these
commits. All execution paths in this crate are Rust; original Python tests are
contract inputs, not executed runtime dependencies.

| Pinned source | Native implementation | Synthetic acceptance tests |
| --- | --- | --- |
| `classifiers/base_classifier.py:35–166` questions/results | `src/types.rs`, `src/limits.rs` | mixed batches; typed helper variant rejection; finite/range/scale checks |
| `base_classifier.py:168–312` reusable typed calls and metrics | `Classifier`, `ClassificationBatch`, `ClassificationMetrics` | typed helpers, elapsed/usage/provenance, native LLM inference |
| `classifiers/llm/classifier.py:35–50` instructions | `DEFAULT_INSTRUCTIONS` | `llm_mixed_named_schema_isolated_context_and_estimated_metrics` |
| `llm/classifier.py:111–159` isolated inference | `LlmClassifier::ask` using existing `LlmInference` | `real_native_llm_inference_uses_fresh_request_without_mutating_service`, cancellation/drop/error tests |
| `llm/classifier.py:160–225` render and named schema | bounded `render`, `response_schema` | mixed/structured prompt/schema and bounded request tests |
| `llm/classifier.py:227–280` tolerated parsing and arithmetic | `parse_answers`, `result`, finite numeric conversion | fences/prose/unwrapped, missing/wrong/unknown choices, clamp, normalized score, NaN/inf |
| `classifiers/jev/client.py:50–57` usage | `JevUsage`, checked cumulative accounting | concurrent usage, malformed counters, per-call and cumulative overflow |
| `jev/client.py:60–143` pooled client and warmup | `JevClient::new`, `connect`, `close` | shared connect-once, failed warmup retry, owned/shared cleanup, close active request |
| `jev/client.py:145–208` exact route/retries/body/usage | `JevClient::ask`, bounded body reading, 429/529-only loop | exact headers/route/body; 429 then 529 then success; exhaustion; all non-busy statuses; timeout/disconnect/no replay |
| `classifiers/jev/classifier.py:43–121` ownership and typed ask | `JevClassifier::owned/shared/setup/cleanup`, Drop | ownership, typed provider metrics, active close |
| `jev/classifier.py:123–148` noul/choice/score mapping | `to_jev`, typed input validation | omitted/empty criteria, structured descriptions, ordered levels, exact 255 allowed / 256 rejected |
| `jev/classifier.py:150–193` results | `from_jev`, `probabilities`, `unit` | provider score/confidence preserved, missing option zero, invalid result/probability/type/range rejected |
| `tests/test_llm_classifier.py` | `tests/classifiers.rs` | all major source contracts above; no Python dependency |
| `tests/test_classifiers_jev.py` | `tests/classifiers.rs` | all major non-live source contracts above; no hosted provider calls |

Security additions beyond the pinned implementation are explicit deadline and
cancellation tokens, bounded admission/JSON/HTTP retention, redacted error
categories, redirect/proxy disabling, implicit retry disabling, strict finite
arithmetic, and owned request cancellation on future drop. README.md enumerates
behavioral tightening and unsupported integration claims.

The UI-worker, voicemail and evaluation consumers are deliberately outside this
crate's scope. No claim is made that they have adopted this API.

`tests/classifiers_review.rs` preserves eleven additional independently authored
regressions in the normal crate suite: duplicate-key and Unicode JSON grammar,
pre-allocation node/depth admission, transactional token-usage overflow,
subnormal score normalization, cancelled/expired admission, complete reply-byte
bounds, native unsolicited-tool isolation, queued/active future-drop ownership,
and exact derived schema budgets. Three separately retained old-source controls
must fail their corresponding regressions; intentionally broken source is not
part of this crate.
