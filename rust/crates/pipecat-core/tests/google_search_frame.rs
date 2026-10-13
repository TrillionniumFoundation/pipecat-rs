use pipecat_core::{
    Frame, FrameData, FramePriority, LlmSearchOrigin, LlmSearchResponseFrame, LlmSearchResult,
};
use serde_json::json;

#[test]
fn google_search_frame_preserves_nullable_payload_and_normal_contract() {
    let payload = LlmSearchResponseFrame {
        search_result: Some("Answer".into()),
        rendered_content: Some("<div>Literal source HTML</div>".into()),
        origins: vec![LlmSearchOrigin {
            site_uri: None,
            site_title: None,
            results: vec![
                LlmSearchResult {
                    text: None,
                    confidence: None,
                },
                LlmSearchResult {
                    text: Some("".into()),
                    confidence: Some(vec![]),
                },
            ],
        }],
    };
    let frame = Frame::new(FrameData::LlmSearchResponse(payload.clone()));
    assert_eq!(frame.data.source_class_name(), "LLMSearchResponseFrame");
    assert!(frame.name.starts_with("LLMSearchResponseFrame#"));
    assert_eq!(frame.priority(), FramePriority::Normal);
    assert!(frame.interruptible);
    assert_eq!(
        serde_json::to_value(&frame.data).unwrap(),
        json!({"type":"llm_search_response","payload":{"search_result":"Answer","rendered_content":"<div>Literal source HTML</div>","origins":[{"site_uri":null,"site_title":null,"results":[{"text":null,"confidence":null},{"text":"","confidence":[]}]}]}})
    );
    let encoded = serde_json::to_vec(&frame).unwrap();
    assert_eq!(serde_json::from_slice::<Frame>(&encoded).unwrap(), frame);
    assert_eq!(LlmSearchResponseFrame::default().origins, vec![]);
    assert_eq!(LlmSearchOrigin::default().results, vec![]);
}
