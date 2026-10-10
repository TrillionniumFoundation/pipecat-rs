use pipecat_core::*;
#[test]
fn gemini_carriers_reject_untyped_text_before_empty_signed_tail() {
    assert!(NativeLlmContinuation::new(
        1,
        LlmContinuationProvider::Gemini,
        vec![
            LlmContinuationBlock::Text("unheard".into()),
            LlmContinuationBlock::GeminiText {
                text: "".into(),
                thought: false,
                signature: Some("secret".into())
            }
        ]
    )
    .is_err());
}
#[test]
fn generic_text_remains_supported_for_anthropic_carriers() {
    assert!(NativeLlmContinuation::new(
        1,
        LlmContinuationProvider::Anthropic,
        vec![LlmContinuationBlock::Text("text".into())]
    )
    .is_ok());
}
