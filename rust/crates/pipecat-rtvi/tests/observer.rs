use pipecat_core::*;
use pipecat_rtvi::*;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
fn observed(observer: &RtviObserver, data: FrameData) -> Vec<Value> {
    observer
        .observe(
            &Frame::new(data),
            Observation::new("llm", Direction::Downstream),
        )
        .unwrap()
        .into_iter()
        .map(|e| serde_json::to_value(e).unwrap())
        .collect()
}
fn observer(params: RtviObserverParams) -> RtviObserver {
    RtviObserver::new(params, None).unwrap()
}
fn agg(text: &str) -> AggregatedTextFrame {
    AggregatedTextFrame {
        text: TextFrame::new(text),
        aggregated_by: "sentence".into(),
        context_id: Some("ctx".into()),
        raw_text: None,
        will_be_spoken: true,
    }
}
#[test]
fn state_lifecycle_transcripts_and_markers_follow_source_contract() {
    let observer = observer(RtviObserverParams::default());
    let fixture: Value = serde_json::from_str(include_str!("fixtures/wire.json")).unwrap();
    let inputs = vec![
        FrameData::UserStartedSpeaking,
        FrameData::UserStoppedSpeaking,
        FrameData::BotStartedSpeaking,
        FrameData::BotStoppedSpeaking,
        FrameData::UserMuteStarted,
        FrameData::UserMuteStopped,
        FrameData::LlmResponseStart,
        FrameData::LlmResponseEnd,
        FrameData::TtsStarted(TtsStartedFrame::default()),
        FrameData::TtsStopped(TtsStoppedFrame::default()),
        FrameData::Interruption,
    ];
    for (input, expected) in inputs
        .into_iter()
        .zip(fixture["lifecycle"].as_array().unwrap())
    {
        assert_eq!(&observed(&observer, input)[0], expected);
    }
    assert!(observed(
        &observer,
        FrameData::LlmMarkerResponse(LlmMarkerResponseFrame {
            raw: "raw".into(),
            marker: Some("●".into()),
            kind: Some("complete".into()),
            markers: vec!["●".into()]
        })
    )
    .is_empty());
    observed(
        &observer,
        FrameData::RtviConfigureObserver(RtviConfigureObserverFrame {
            bot_llm_marker_enabled: Some(true),
            ..Default::default()
        }),
    );
    assert_eq!(
        observed(
            &observer,
            FrameData::LlmMarkerResponse(LlmMarkerResponseFrame {
                raw: "●done".into(),
                marker: Some("●".into()),
                kind: Some("complete".into()),
                markers: vec!["●".into()]
            })
        )[0],
        fixture["marker"]
    );
    for is_final in [false, true] {
        let t = TranscriptionFrame {
            text: "hello".into(),
            user_id: "u".into(),
            timestamp: "2026-01-01T00:00:00Z".into(),
            ..Default::default()
        };
        let event = observed(
            &observer,
            if is_final {
                FrameData::Transcription(t)
            } else {
                FrameData::InterimTranscription(t)
            },
        );
        assert_eq!(event[0]["data"]["final"], is_final);
    }
    let output = observed(
        &observer,
        FrameData::LlmText(TextFrame::llm("Hello world.")),
    );
    assert_eq!(output[0]["type"], "bot-llm-text");
    assert_eq!(output[1]["type"], "bot-transcription");
    assert_eq!(output[1]["data"]["text"], "Hello world.");
}
#[test]
fn tool_metadata_is_private_by_default_and_explicit_levels_control_each_field() {
    let observer = observer(RtviObserverParams::default());
    let progress = || {
        FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
            function_name: "secret".into(),
            tool_call_id: "call".into(),
            arguments: json!({"password":"test-only"}),
            cancel_on_interruption: true,
            group_id: None,
        })
    };
    assert_eq!(
        observed(&observer, progress())[0],
        json!({"label":"rtvi-ai","type":"llm-function-call-in-progress","data":{"tool_call_id":"call"}})
    );
    for (level, name, args, result) in [
        (RtviFunctionCallReportLevel::Disabled, false, false, false),
        (RtviFunctionCallReportLevel::None, false, false, false),
        (RtviFunctionCallReportLevel::Name, true, false, false),
        (RtviFunctionCallReportLevel::Arguments, true, true, false),
        (RtviFunctionCallReportLevel::Full, true, true, true),
    ] {
        observed(
            &observer,
            FrameData::RtviConfigureObserver(RtviConfigureObserverFrame {
                function_call_report_level: Some(BTreeMap::from([("secret".into(), level)])),
                ..Default::default()
            }),
        );
        let p = observed(&observer, progress());
        let r = observed(
            &observer,
            FrameData::ExternalFunctionCallResult(ExternalFunctionCallResultFrame {
                function_name: "secret".into(),
                tool_call_id: "call".into(),
                arguments: json!({}),
                result: json!({"value":42}),
                is_final: true,
            }),
        );
        if level == RtviFunctionCallReportLevel::Disabled {
            assert!(p.is_empty());
            assert!(r.is_empty());
            continue;
        }
        assert_eq!(p[0]["data"].get("function_name").is_some(), name);
        assert_eq!(p[0]["data"].get("arguments").is_some(), args);
        assert_eq!(r[0]["data"].get("result").is_some(), result);
    }
    assert!(observed(
        &observer,
        FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: "secret".into(),
            tool_call_id: "call".into(),
            arguments: json!({}),
            result: json!({"value":42}),
            run_llm: None,
            is_final: false,
            error: None
        })
    )
    .is_empty());
    let cancelled = observed(
        &observer,
        FrameData::ExternalFunctionCallCancel(ExternalFunctionCall {
            function_name: "secret".into(),
            tool_call_id: "call".into(),
        }),
    );
    assert_eq!(cancelled[0]["data"]["cancelled"], true);
    assert!(cancelled[0]["data"].get("result").is_none());
}
#[test]
fn exact_metrics_shapes_are_source_compatible() {
    let observer = observer(RtviObserverParams::default());
    let metrics = observed(
        &observer,
        FrameData::Metrics(vec![
            Metric::Ttfb {
                processor: "llm".into(),
                seconds: 0.1,
            },
            Metric::Processing {
                processor: "tts".into(),
                seconds: 0.2,
            },
            Metric::LlmUsage {
                processor: "llm".into(),
                prompt_tokens: 1,
                completion_tokens: 2,
                total_tokens: 3,
            },
            Metric::TtsUsage {
                processor: "tts".into(),
                characters: 5,
            },
            Metric::SttUsage {
                processor: "stt".into(),
                audio_seconds: 1.25,
            },
        ]),
    );
    let fixture: Value = serde_json::from_str(include_str!("fixtures/wire.json")).unwrap();
    assert_eq!(metrics[0], fixture["metrics"]);
}
#[test]
fn output_requires_real_transport_evidence_and_retains_segment_identity() {
    let observer = observer(RtviObserverParams::default());
    let frame = Frame::new(FrameData::TtsText(agg("hello")));
    assert!(observer
        .observe(&frame, Observation::new("tts", Direction::Downstream))
        .unwrap()
        .is_empty());
    let delivered = Observation {
        source: "output",
        direction: Direction::Downstream,
        first_push: false,
        after_output_transport: true,
    };
    assert!(observer.observe(&frame, delivered).unwrap().is_empty());
    let events = observed(&observer, FrameData::BotStartedSpeaking);
    assert_eq!(events.len(), 3);
    assert_eq!(events[1]["type"], "bot-output");
    assert_eq!(events[1]["data"]["segment_id"], frame.id);
    assert_eq!(events[1]["data"]["spoken_status"], "completed");
    assert_eq!(events[2]["type"], "bot-tts-text");
    assert!(observer.observe(&frame, delivered).unwrap().is_empty());
    let progress = Frame::new(FrameData::AggregatedTextProgress(
        AggregatedTextProgressFrame {
            segment_id: frame.id,
            context_id: None,
            text: "hello there".into(),
            aggregated_by: "sentence".into(),
            accumulated_text: "hello".into(),
            remaining_text: " there".into(),
        },
    ));
    assert!(observer
        .observe(&progress, Observation::new("synth", Direction::Downstream))
        .unwrap()
        .is_empty());
    let events = observer.observe(&progress, delivered).unwrap();
    let data = events[0].data.as_ref().unwrap();
    assert_eq!(data["spoken_status"], "in-progress");
    assert_eq!(data["spoken_progress"]["remaining_text"], " there");
}
#[test]
fn interruption_discards_queued_output_and_buffers_are_bounded() {
    let observer = observer(RtviObserverParams {
        max_pending_segments: 1,
        max_pending_text_bytes: 16,
        ..Default::default()
    });
    let delivered = Observation {
        source: "out",
        direction: Direction::Downstream,
        first_push: false,
        after_output_transport: true,
    };
    observer
        .observe(
            &Frame::new(FrameData::AggregatedText(agg("first"))),
            delivered,
        )
        .unwrap();
    assert!(observer
        .observe(
            &Frame::new(FrameData::AggregatedText(agg("second"))),
            delivered
        )
        .is_err());
    observed(&observer, FrameData::Interruption);
    assert_eq!(observed(&observer, FrameData::BotStartedSpeaking).len(), 1);
    assert!(observer
        .observe(
            &Frame::new(FrameData::LlmText(TextFrame::llm("x".repeat(17)))),
            Observation::new("llm", Direction::Downstream)
        )
        .is_err());
}
#[test]
fn source_filters_broadcast_siblings_and_frame_identity_prevent_duplicates() {
    let observer = observer(RtviObserverParams {
        ignored_sources: ["private".into()].into_iter().collect(),
        ..Default::default()
    });
    let frame = Frame::new(FrameData::BotStartedSpeaking);
    assert!(observer
        .observe(&frame, Observation::new("private", Direction::Downstream))
        .unwrap()
        .is_empty());
    assert_eq!(
        observer
            .observe(&frame, Observation::new("out", Direction::Downstream))
            .unwrap()
            .len(),
        1
    );
    assert!(observer
        .observe(&frame, Observation::new("out2", Direction::Downstream))
        .unwrap()
        .is_empty());
    let (down, up) = Frame::interruption().broadcast_pair();
    assert!(observer
        .observe(&up, Observation::new("up", Direction::Upstream))
        .unwrap()
        .is_empty());
    assert_eq!(
        observer
            .observe(&down, Observation::new("down", Direction::Downstream))
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn correlated_server_responses_and_ui_job_envelopes() {
    let observer = observer(RtviObserverParams::default());
    let request = RtviClientMessageFrame {
        msg_id: "request".into(),
        message_type: "custom".into(),
        data: None,
    };
    assert_eq!(
        observed(
            &observer,
            FrameData::RtviServerResponse(RtviServerResponseFrame {
                client_msg: request.clone(),
                data: Some(json!({"yes":true})),
                error: None
            })
        )[0],
        json!({"label":"rtvi-ai","type":"server-response","id":"request","data":{"t":"custom","d":{"yes":true}}})
    );
    assert_eq!(
        observed(
            &observer,
            FrameData::RtviServerResponse(RtviServerResponseFrame {
                client_msg: request,
                data: None,
                error: Some("failed".into())
            })
        )[0]["type"],
        "error-response"
    );
    assert_eq!(
        observed(
            &observer,
            FrameData::RtviUiCommand(RtviUiCommandFrame {
                command: "toast".into(),
                payload: Some(json!({"message":"hello"}))
            })
        )[0]["data"]["command"],
        "toast"
    );
    assert_eq!(
        observed(
            &observer,
            FrameData::RtviUiJobGroup(Some(json!({"kind":"group_completed","job_id":"j","at":3})))
        )[0]["data"]["kind"],
        "group_completed"
    );
}
#[tokio::test]
async fn observer_tap_emits_real_pipeline_wire_events_without_consuming_frames() {
    let observer = Arc::new(observer(RtviObserverParams::default()));
    let worker = Pipeline::new(vec![
        Arc::new(observer.tap("llm-output")),
        Arc::new(observer.tap("transport-input")),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmText(TextFrame::llm("Hello."))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmText(_)))
            .count(),
        1
    );
    assert_eq!(frames.iter().filter(|f| matches!(&f.frame.data,FrameData::OutputTransportMessageUrgent(v) if v["type"] == "bot-llm-text")).count(),1);
}
