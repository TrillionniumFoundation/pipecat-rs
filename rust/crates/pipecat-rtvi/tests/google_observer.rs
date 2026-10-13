use pipecat_core::{
    Direction, Frame, FrameData, LlmSearchOrigin, LlmSearchResponseFrame, LlmSearchResult,
    Pipeline, PipelineConfig, TextFrame,
};
use pipecat_rtvi::{Observation, RtviObserver, RtviObserverParams};
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn response() -> LlmSearchResponseFrame {
    LlmSearchResponseFrame {
        search_result: Some("Rust answer".into()),
        rendered_content: Some("<div>literal source</div>".into()),
        origins: vec![LlmSearchOrigin {
            site_uri: Some("https://example.invalid/rust".into()),
            site_title: None,
            results: vec![LlmSearchResult {
                text: Some("Rust answer".into()),
                confidence: Some(vec![0.8, 0.6]),
            }],
        }],
    }
}

#[test]
fn google_observer_projects_valid_search_with_source_wire_shape() {
    let observer = observer(RtviObserverParams::default());
    let events = observer
        .observe(
            &Frame::new(FrameData::LlmSearchResponse(response())),
            Observation::new("gemini", Direction::Downstream),
        )
        .unwrap();
    assert_eq!(
        events.len(),
        1,
        "valid native Google search must have an RTVI projection"
    );
    assert_eq!(
        serde_json::to_value(&events[0]).unwrap(),
        json!({
            "label":"rtvi-ai", "type":"bot-llm-search-response", "data":{
                "search_result":"Rust answer", "rendered_content":"<div>literal source</div>",
                "origins":[{"site_uri":"https://example.invalid/rust", "results":[{
                    "text":"Rust answer", "confidence":[0.8,0.6]
                }]}]
            }
        })
    );
}

fn observer(params: RtviObserverParams) -> pipecat_rtvi::GoogleRtviObserver {
    pipecat_rtvi::GoogleRtviObserver::new(params, None).unwrap()
}
fn observe(observer: &pipecat_rtvi::GoogleRtviObserver, frame: &Frame) -> usize {
    observer
        .observe(frame, Observation::new("gemini", Direction::Downstream))
        .unwrap()
        .len()
}

#[test]
fn native_filters_do_not_consume_search_identity_and_fifo_cache_is_bounded() {
    let observer = observer(RtviObserverParams {
        ignored_sources: ["ignored".into()].into(),
        dedup_capacity: 2,
        ..Default::default()
    });
    let mut frame = Frame::new(FrameData::LlmSearchResponse(response()));
    let mut observation = Observation::new("ignored", Direction::Downstream);
    assert!(observer.observe(&frame, observation).unwrap().is_empty());
    observation.source = "gemini";
    observation.first_push = false;
    assert!(observer.observe(&frame, observation).unwrap().is_empty());
    observation.first_push = true;
    observation.direction = Direction::Upstream;
    frame.broadcast_sibling_id = Some(42);
    assert!(observer.observe(&frame, observation).unwrap().is_empty());
    observation.direction = Direction::Downstream;
    assert_eq!(observer.observe(&frame, observation).unwrap().len(), 1);
    assert_eq!(observe(&observer, &frame), 0);
    let second = Frame::new(FrameData::LlmSearchResponse(response()));
    let third = Frame::new(FrameData::LlmSearchResponse(response()));
    assert_eq!(observe(&observer, &second), 1);
    assert_eq!(observe(&observer, &frame), 0); // Cache hits do not refresh FIFO order.
    assert_eq!(observe(&observer, &third), 1);
    assert_eq!(observe(&observer, &frame), 1);
    observer.reset();
    assert_eq!(observe(&observer, &frame), 1);
}

#[test]
fn malformed_nested_results_and_oversize_do_not_poison_identity() {
    let observer = observer(RtviObserverParams {
        max_message_bytes: 1024,
        ..Default::default()
    });
    for (text, confidence) in [(None, Some(vec![])), (Some("answer".into()), None)] {
        let mut invalid = response();
        invalid.origins[0].results[0] = LlmSearchResult { text, confidence };
        let mut frame = Frame::new(FrameData::LlmSearchResponse(invalid));
        assert!(observer
            .observe(&frame, Observation::new("gemini", Direction::Downstream))
            .is_err());
        frame.data = FrameData::LlmSearchResponse(response());
        assert_eq!(observe(&observer, &frame), 1);
    }
    let mut large = response();
    large.rendered_content = Some("x".repeat(1024));
    let mut frame = Frame::new(FrameData::LlmSearchResponse(large));
    assert!(observer
        .observe(&frame, Observation::new("gemini", Direction::Downstream))
        .is_err());
    frame.data = FrameData::LlmSearchResponse(response());
    assert_eq!(observe(&observer, &frame), 1);

    // Source data fits, but the service-specific complete wire envelope does not.
    let mut boundary = response();
    boundary.origins[0].site_title = Some("source".into());
    boundary.rendered_content = Some(String::new());
    let size = serde_json::to_vec(&FrameData::LlmSearchResponse(boundary.clone()))
        .unwrap()
        .len();
    boundary.rendered_content = Some("x".repeat(1024 - size));
    let mut frame = Frame::new(FrameData::LlmSearchResponse(boundary));
    assert_eq!(serde_json::to_vec(&frame.data).unwrap().len(), 1024);
    assert!(observer
        .observe(&frame, Observation::new("gemini", Direction::Downstream))
        .is_err());
    frame.data = FrameData::LlmSearchResponse(response());
    assert_eq!(observe(&observer, &frame), 1);
}

#[test]
fn optional_none_is_omitted_and_ordinary_events_keep_base_behavior() {
    let observer = observer(RtviObserverParams {
        bot_llm_enabled: false,
        ..Default::default()
    });
    let frame = Frame::new(FrameData::LlmSearchResponse(LlmSearchResponseFrame {
        origins: vec![LlmSearchOrigin::default()],
        ..Default::default()
    }));
    let events = observer
        .observe(&frame, Observation::new("gemini", Direction::Downstream))
        .unwrap();
    assert_eq!(
        serde_json::to_value(&events[0]).unwrap(),
        json!({
            "label":"rtvi-ai","type":"bot-llm-search-response","data":{"origins":[{"results":[]}]}
        })
    );
    let ordinary = Frame::new(FrameData::UserStartedSpeaking);
    let base = RtviObserver::new(RtviObserverParams::default(), None).unwrap();
    assert_eq!(
        observer
            .observe(&ordinary, Observation::new("user", Direction::Downstream))
            .unwrap(),
        base.observe(&ordinary, Observation::new("user", Direction::Downstream))
            .unwrap()
    );
    assert!(observer
        .observe(
            &Frame::new(FrameData::LlmText(TextFrame::llm("hidden"))),
            Observation::new("gemini", Direction::Downstream)
        )
        .unwrap()
        .iter()
        .all(|e| e.message_type != "bot-llm-text"));
}

#[tokio::test]
async fn tap_forwards_repeated_and_invalid_search_frames_without_duplicate_wire() {
    let observer = Arc::new(observer(RtviObserverParams::default()));
    let worker = Pipeline::new(vec![Arc::new(observer.tap("gemini"))])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    let valid = Frame::new(FrameData::LlmSearchResponse(response()));
    let mut invalid = response();
    invalid.origins[0].results[0].confidence = None;
    let invalid = Frame::new(FrameData::LlmSearchResponse(invalid));
    for frame in [valid.clone(), valid.clone(), invalid.clone()] {
        tokio::time::timeout(
            Duration::from_secs(5),
            sender.send(frame, Direction::Downstream),
        )
        .await
        .unwrap()
        .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(5), sender.end())
        .await
        .unwrap()
        .unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmSearchResponse(_))
                && f.frame.id == valid.id)
            .count(),
        2
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmSearchResponse(_))
                && f.frame.id == invalid.id)
            .count(),
        1
    );
    assert_eq!(frames.iter().filter(|f| matches!(&f.frame.data, FrameData::OutputTransportMessageUrgent(v) if v["type"] == "bot-llm-search-response")).count(), 1);
    assert_eq!(report.errors.len(), 1);
}
