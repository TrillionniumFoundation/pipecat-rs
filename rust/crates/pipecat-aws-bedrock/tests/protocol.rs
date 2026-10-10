mod support;
use pipecat_aws_bedrock::protocol::*;
use pipecat_services::{FinishReason, ServiceError};
use serde_json::json;
fn decoder() -> ConverseDecoder {
    ConverseDecoder::new(1024 * 1024, 1024 * 1024)
}
fn start(d: &mut ConverseDecoder) {
    assert_eq!(
        d.event(&support::record(
            "messageStart",
            json!({"role":"assistant"})
        ))
        .unwrap(),
        [BedrockEvent::Started]
    );
}
#[test]
fn indexed_parallel_tool_calls_are_finalized_independently_and_in_index_order() {
    let mut d = decoder();
    start(&mut d);
    for (i, id, name) in [(0, "a", "first"), (1, "b", "second")] {
        d.event(&support::record(
            "contentBlockStart",
            json!({"contentBlockIndex":i,"start":{"toolUse":{"toolUseId":id,"name":name}}}),
        ))
        .unwrap();
    }
    for (i, input) in [(1, "{\"b\":"), (0, "{\"a\":1}"), (1, "2}")] {
        assert!(d
            .event(&support::record(
                "contentBlockDelta",
                json!({"contentBlockIndex":i,"delta":{"toolUse":{"input":input}}})
            ))
            .unwrap()
            .is_empty());
    }
    for i in [1, 0] {
        d.event(&support::record(
            "contentBlockStop",
            json!({"contentBlockIndex":i}),
        ))
        .unwrap();
    }
    d.event(&support::record(
        "messageStop",
        json!({"stopReason":"tool_use"}),
    ))
    .unwrap();
    match d.finish().unwrap() {
        BedrockEvent::Completed { blocks, reason } => {
            assert_eq!(reason, FinishReason::ToolCalls);
            assert!(
                matches!(&blocks[0],BedrockBlock::Tool(t) if t.id=="a"&&t.arguments==json!({"a":1}))
            );
            assert!(
                matches!(&blocks[1],BedrockBlock::Tool(t) if t.id=="b"&&t.arguments==json!({"b":2}))
            );
        }
        _ => panic!(),
    }
}
#[test]
fn reasoning_signature_and_redacted_chunks_have_closed_checkpoints() {
    let mut d = decoder();
    start(&mut d);
    for delta in [
        json!({"text":"private"}),
        json!({"signature":"sig+/"}),
        json!({"signature":"=="}),
    ] {
        d.event(&support::record(
            "contentBlockDelta",
            json!({"contentBlockIndex":0,"delta":{"reasoningContent":delta}}),
        ))
        .unwrap();
    }
    let checkpoint = d
        .event(&support::record(
            "contentBlockStop",
            json!({"contentBlockIndex":0}),
        ))
        .unwrap();
    assert!(
        matches!(&checkpoint[..],[BedrockEvent::Checkpoint(b)] if b==&vec![BedrockBlock::Reasoning{text:"private".into(),signature:Some("sig+/==".into())}])
    );
    for data in ["AAE=", "AgM="] {
        d.event(&support::record(
            "contentBlockDelta",
            json!({"contentBlockIndex":1,"delta":{"reasoningContent":{"redactedContent":data}}}),
        ))
        .unwrap();
    }
    d.event(&support::record(
        "contentBlockStop",
        json!({"contentBlockIndex":1}),
    ))
    .unwrap();
    d.event(&support::record(
        "messageStop",
        json!({"stopReason":"end_turn"}),
    ))
    .unwrap();
    assert!(
        matches!(d.finish().unwrap(),BedrockEvent::Completed{blocks,..} if blocks[1]==BedrockBlock::Redacted(vec![0,1,2,3]))
    );
}
#[test]
fn unsigned_reasoning_is_preserved_but_cannot_create_a_signed_checkpoint() {
    let mut d = decoder();
    start(&mut d);
    d.event(&support::record(
        "contentBlockDelta",
        json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"text":"thinking"}}}),
    ))
    .unwrap();
    assert!(d
        .event(&support::record(
            "contentBlockStop",
            json!({"contentBlockIndex":0})
        ))
        .unwrap()
        .is_empty());
    d.event(&support::record(
        "messageStop",
        json!({"stopReason":"end_turn"}),
    ))
    .unwrap();
    assert!(
        matches!(d.finish().unwrap(),BedrockEvent::Completed{blocks,..} if matches!(&blocks[0],BedrockBlock::Reasoning{signature:None,..}))
    );
}
#[test]
fn cumulative_usage_counts_source_cache_components() {
    let mut d = decoder();
    start(&mut d);
    d.event(&support::record(
        "messageStop",
        json!({"stopReason":"end_turn"}),
    ))
    .unwrap();
    let events=d.event(&support::record("metadata",json!({"usage":{"inputTokens":2,"outputTokens":3,"cacheReadInputTokens":4,"cacheWriteInputTokens":5,"totalTokens":5}}))).unwrap();
    assert!(
        matches!(&events[..],[BedrockEvent::Usage(u)] if u.total_tokens==14&&u.prompt_tokens==2&&u.cache_creation_input_tokens==Some(5))
    );
}
#[test]
fn malformed_crcs_lengths_headers_and_poisoned_state_fail_closed() {
    let good = support::record("messageStart", json!({"role":"assistant"}));
    for index in [0, 5, 8, 15, good.len() - 1] {
        let mut bad = good.clone();
        bad[index] ^= 1;
        let mut d = decoder();
        assert!(d.event(&bad).is_err());
        assert!(d.event(&good).is_err());
    }
    let mut d = ConverseDecoder::new(16, 100);
    assert!(d.event(&good).is_err());
}
#[test]
fn unfinished_empty_unknown_and_mixed_blocks_do_not_complete() {
    for records in [
        vec![],
        vec![support::record("messageStart", json!({"role":"assistant"}))],
        vec![
            support::record("messageStart", json!({"role":"assistant"})),
            support::record(
                "contentBlockDelta",
                json!({"contentBlockIndex":0,"delta":{"text":"hi"}}),
            ),
        ],
    ] {
        let mut d = decoder();
        for r in records {
            d.event(&r).unwrap();
        }
        assert!(d.finish().is_err());
    }
    let mut d = decoder();
    start(&mut d);
    assert!(d
        .event(&support::record(
            "contentBlockDelta",
            json!({"contentBlockIndex":0,"delta":{"text":"hi","toolUse":{"input":"{}"}}})
        ))
        .is_err());
}
#[test]
fn malformed_tool_arguments_and_duplicate_ids_never_admit_tools() {
    let mut d = decoder();
    start(&mut d);
    d.event(&support::record(
        "contentBlockStart",
        json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"a","name":"f"}}}),
    ))
    .unwrap();
    d.event(&support::record(
        "contentBlockDelta",
        json!({"contentBlockIndex":0,"delta":{"toolUse":{"input":"[1]"}}}),
    ))
    .unwrap();
    assert!(d
        .event(&support::record(
            "contentBlockStop",
            json!({"contentBlockIndex":0})
        ))
        .is_err());
    let mut d = decoder();
    start(&mut d);
    for i in [0, 1] {
        let result = d.event(&support::record(
            "contentBlockStart",
            json!({"contentBlockIndex":i,"start":{"toolUse":{"toolUseId":"same","name":"f"}}}),
        ));
        assert_eq!(result.is_err(), i == 1);
    }
}
#[test]
fn exception_bodies_and_unknown_error_kinds_are_redacted() {
    let mut d = decoder();
    let record = support::raw_record(
        "exception",
        ":exception-type",
        "validationException",
        br#"{"message":"PRIVATE TOKEN"}"#.to_vec(),
    );
    let e = d.event(&record).unwrap_err();
    assert!(matches!(e,ServiceError::Provider{ref kind,..} if kind=="validationException"));
    assert!(!format!("{e:?}").contains("PRIVATE"));
}
#[test]
fn output_limit_applies_to_thoughts_and_tool_fragments() {
    let mut d = ConverseDecoder::new(4096, 3);
    start(&mut d);
    assert!(d
        .event(&support::record(
            "contentBlockDelta",
            json!({"contentBlockIndex":0,"delta":{"reasoningContent":{"text":"long"}}})
        ))
        .is_err());
}
#[test]
fn tools_cannot_complete_after_filtered_truncated_malformed_or_unknown_stops() {
    for stop in [
        "guardrail_intervened",
        "content_filtered",
        "max_tokens",
        "model_context_window_exceeded",
        "end_turn",
        "stop_sequence",
        "malformed_tool_use",
        "malformed_model_output",
        "future_unknown",
    ] {
        let mut decoder = decoder();
        start(&mut decoder);
        decoder.event(&support::record("contentBlockStart",json!({"contentBlockIndex":0,"start":{"toolUse":{"toolUseId":"call","name":"effect"}}}))).unwrap();
        decoder
            .event(&support::record(
                "contentBlockDelta",
                json!({"contentBlockIndex":0,"delta":{"toolUse":{"input":"{}"}}}),
            ))
            .unwrap();
        decoder
            .event(&support::record(
                "contentBlockStop",
                json!({"contentBlockIndex":0}),
            ))
            .unwrap();
        let stopped = decoder.event(&support::record("messageStop", json!({"stopReason":stop})));
        assert!(
            stopped.is_err() || decoder.finish().is_err(),
            "tools admitted after {stop}"
        );
    }
    let mut decoder = decoder();
    start(&mut decoder);
    decoder
        .event(&support::record(
            "messageStop",
            json!({"stopReason":"tool_use"}),
        ))
        .unwrap();
    assert!(decoder.finish().is_err());
}
