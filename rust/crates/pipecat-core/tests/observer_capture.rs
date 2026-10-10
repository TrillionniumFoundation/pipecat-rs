//! Privacy isolation and capture bounds are tested at live admission boundaries.
use pipecat_core::*;
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Recorder {
    policy: ObserverCapturePolicy,
    events: Mutex<Vec<(ObservationEvent, Option<CapturedPush>)>>,
}
#[async_trait]
impl PipelineObserver for Recorder {
    fn capture_policy(&self) -> ObserverCapturePolicy {
        self.policy
    }
    async fn on_event(&self, event: ObservationEvent) {
        self.events.lock().unwrap().push((event, None));
    }
    async fn on_captured_event(&self, event: ObservationEvent, captured: CapturedPush) {
        self.events.lock().unwrap().push((event, Some(captured)));
    }
}
fn all() -> ObserverCapturePolicy {
    ObserverCapturePolicy {
        errors: Some(ErrorCaptureOptions { message: true }),
        functions: Some(FunctionCaptureOptions {
            identity: true,
            arguments: true,
            results: true,
            errors: true,
        }),
        speech: true,
        ..Default::default()
    }
}
fn started(id: &str, arguments: serde_json::Value) -> Frame {
    Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
        function_name: "f".into(),
        tool_call_id: id.into(),
        arguments,
    }]))
}
async fn run(policies: Vec<ObserverCapturePolicy>, frames: Vec<Frame>) -> Vec<Arc<Recorder>> {
    let observers: Vec<_> = policies
        .into_iter()
        .map(|policy| {
            Arc::new(Recorder {
                policy,
                ..Default::default()
            })
        })
        .collect();
    let mut pipeline = Pipeline::new(vec![
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
    ]);
    for observer in &observers {
        pipeline.add_observer(observer.clone(), ObserverOptions::default());
    }
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    for frame in frames {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
    }
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    observers
}
fn captures(observer: &Recorder) -> Vec<CapturedPush> {
    observer
        .events
        .lock()
        .unwrap()
        .iter()
        .filter_map(|(event, capture)| match &*event.observation {
            PipelineObservation::Push {
                first_push: true, ..
            } => capture.clone(),
            _ => None,
        })
        .collect()
}
fn calls(observer: &Recorder) -> Vec<CapturedFunctionCalls> {
    captures(observer)
        .into_iter()
        .filter_map(|capture| match capture.payload {
            CapturedPayload::Functions(calls) => Some(calls),
            _ => None,
        })
        .collect()
}
#[tokio::test]
async fn shared_hub_never_promotes_one_subscribers_sensitive_capture() {
    let args_only = ObserverCapturePolicy {
        functions: Some(FunctionCaptureOptions {
            arguments: true,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut frame = started("private-id", json!({"secret": "private-argument"}));
    frame
        .metadata
        .insert("private-meta".into(), json!("never-capture"));
    let observers = run(
        vec![ObserverCapturePolicy::default(), all(), args_only],
        vec![
            frame,
            Frame::text("private-text"),
            Frame::new(FrameData::Error(ErrorFrame::new("private-error"))),
        ],
    )
    .await;
    let ordinary = observers[0].events.lock().unwrap();
    assert!(ordinary.iter().all(|(_, capture)| capture.is_none()));
    let formatted = format!("{ordinary:?}");
    for secret in [
        "private-id",
        "private-argument",
        "private-text",
        "private-error",
        "private-meta",
        "never-capture",
    ] {
        assert!(!formatted.contains(secret));
    }
    let full = calls(&observers[1]);
    assert_eq!(full[0].calls[0].tool_call_id.value().unwrap(), "private-id");
    let partial = calls(&observers[2]);
    assert_eq!(partial[0].calls[0].tool_call_id, Captured::NotRequested);
    assert!(partial[0].calls[0].arguments.value().is_some());
    // The safe Arc itself is shared; captured content lives in disjoint records.
    let complete = observers[1].events.lock().unwrap();
    assert!(ordinary.iter().any(|(a, _)| complete
        .iter()
        .any(|(b, capture)| capture.is_some() && Arc::ptr_eq(&a.observation, &b.observation))));
}
#[tokio::test]
async fn identities_are_omitted_whole_and_call_and_aggregate_limits_are_enforced() {
    let policy = ObserverCapturePolicy {
        limits: CaptureLimits::new(4, 7, 1, 10, 3).unwrap(),
        ..all()
    };
    let frames = vec![
        started("🔥🔥", json!(null)),
        Frame::new(FrameData::FunctionCallsStarted(vec![
            FunctionCall {
                function_name: "func".into(),
                tool_call_id: "four".into(),
                arguments: json!({}),
            },
            FunctionCall {
                function_name: "other".into(),
                tool_call_id: "two".into(),
                arguments: json!({}),
            },
        ])),
    ];
    let observers = run(vec![policy], frames).await;
    let events = calls(&observers[0]);
    assert_eq!(
        events[0].calls[0].tool_call_id,
        Captured::Omitted(CaptureOmission::FieldBytes)
    );
    assert_eq!(events[1].omitted_calls, 1);
    assert_eq!(
        events[1].calls[0].tool_call_id,
        Captured::Omitted(CaptureOmission::TotalBytes)
    );
}
#[tokio::test]
async fn json_depth_nodes_and_field_bytes_are_checked_before_retention() {
    let policies = vec![
        ObserverCapturePolicy {
            limits: CaptureLimits::new(4096, 8192, 4, 3, 2).unwrap(),
            ..all()
        },
        ObserverCapturePolicy {
            limits: CaptureLimits::new(4, 8192, 4, 100, 20).unwrap(),
            ..all()
        },
    ];
    let observers = run(
        policies,
        vec![
            started("i", json!([[[[null]]]])),
            started("i", json!([null, null, null, null])),
            started("i", json!("🔥🔥")),
        ],
    )
    .await;
    let node = calls(&observers[0]);
    assert_eq!(
        node[0].calls[0].arguments,
        Captured::Omitted(CaptureOmission::JsonDepth)
    );
    assert_eq!(
        node[1].calls[0].arguments,
        Captured::Omitted(CaptureOmission::JsonNodes)
    );
    assert_eq!(
        calls(&observers[1])[2].calls[0].arguments,
        Captured::Omitted(CaptureOmission::FieldBytes)
    );
}
#[tokio::test]
async fn contexts_exceptions_signatures_and_payload_wall_clocks_are_never_captured() {
    let frames = vec![
        Frame::new(FrameData::LlmThoughtEndDetailed(LlmThoughtEndFrame {
            signature: Some(json!("private-provider-signature")),
        })),
        Frame::new(FrameData::FunctionCallsStartedDetailed(vec![
            FunctionCallFromLlm {
                function_name: "f".into(),
                tool_call_id: "i".into(),
                arguments: Default::default(),
                context: Some(LlmContextData {
                    messages: vec![],
                    tools: ToolSet::Functions(vec![]),
                    tool_choice: None,
                }),
            },
        ])),
        Frame::new(FrameData::ErrorDetailed(DetailedErrorFrame {
            error: "visible-message".into(),
            fatal: false,
            processor: None,
            exception: Some(LocalObject::new("private-exception".to_owned())),
            category: None,
        })),
        Frame::new(FrameData::VadUserStartedSpeakingDetailed(
            VadUserStartedSpeakingFrame {
                start_secs: 0.2,
                timestamp: 912_345.0,
            },
        )),
    ];
    let observers = run(vec![all()], frames).await;
    let captured = captures(&observers[0]);
    let formatted = format!("{captured:?}");
    assert!(!formatted.contains("private-exception"));
    assert!(!formatted.contains("private-provider-signature"));
    assert!(!formatted.contains("912345"));
    assert!(matches!(
        captured.last().unwrap().payload,
        CapturedPayload::Speech(CapturedSpeech {
            confirmation_secs: 0.2,
            ..
        })
    ));
}
struct Recovered {
    health: ProcessorHealth,
}
#[async_trait]
impl FrameProcessor for Recovered {
    fn name(&self) -> &str {
        "actual-origin"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            ctx.push_error(ErrorFrame {
                error: "recoverable-message".into(),
                fatal: false,
                processor: Some("spoofed-label".into()),
                category: ErrorCategory::Authentication,
            })
            .await?;
            self.health.set_usable(true);
        }
        ctx.push(frame, direction).await
    }
}
struct Recover;
#[async_trait]
impl FrameProcessor for Recover {
    fn name(&self) -> &str {
        "recover"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.as_error_frame().is_some() {
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn recovered_error_keeps_native_origin_and_reporting_verdict_after_health_restoration() {
    let recorder = Arc::new(Recorder {
        policy: all(),
        ..Default::default()
    });
    let mut pipeline = Pipeline::new(vec![
        Arc::new(Recover),
        Arc::new(Recovered {
            health: ProcessorHealth::default(),
        }),
    ]);
    pipeline.add_observer(recorder.clone(), ObserverOptions::default());
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    worker
        .sender()
        .send(Frame::text("trigger"), Direction::Downstream)
        .await
        .unwrap();
    worker.recv().await.unwrap();
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    let errors: Vec<_> = captures(&recorder)
        .into_iter()
        .filter_map(|capture| {
            if let CapturedPayload::Error(error) = capture.payload {
                Some(error)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(errors.len(), 1);
    assert_eq!(&*errors[0].processor.name, "actual-origin");
    assert!(!errors[0].processor_usable);
    assert_eq!(errors[0].category, ErrorCategory::Authentication);
}
struct StreamIdentity;
#[async_trait]
impl FrameProcessor for StreamIdentity {
    fn name(&self) -> &str {
        "stream-identity"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            let output = ctx.clone();
            ctx.spawn("stream", true, async move {
                output.push_downstream_wait(frame).await
            })?;
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn adjacent_and_boundary_stream_paths_preserve_safe_observation_and_first_push() {
    let recorder = Arc::new(Recorder {
        policy: all(),
        ..Default::default()
    });
    let mut pipeline = Pipeline::new(vec![Arc::new(StreamIdentity), Arc::new(StreamIdentity)]);
    pipeline.add_observer(recorder.clone(), ObserverOptions::default());
    let mut worker = pipeline.start(PipelineConfig::default()).await.unwrap();
    worker.recv().await.unwrap();
    let text = Frame::text("secret-stream-text");
    let id = text.id;
    worker
        .sender()
        .send(text, Direction::Downstream)
        .await
        .unwrap();
    worker.recv().await.unwrap();
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    let events = recorder.events.lock().unwrap();
    let pushes: Vec<_> = events
        .iter()
        .filter_map(|(event, capture)| match &*event.observation {
            PipelineObservation::Push {
                frame, first_push, ..
            } if frame.id == id => {
                assert!(capture.is_none());
                Some(*first_push)
            }
            _ => None,
        })
        .collect();
    assert_eq!(pushes, vec![true, false]);
    assert!(!format!("{events:?}").contains("secret-stream-text"));
}
#[test]
fn capture_limits_reject_unbounded_or_zero_configuration() {
    for args in [
        (0, 1, 1, 1, 1),
        (1, 262_145, 1, 1, 1),
        (1, 1, 257, 1, 1),
        (1, 1, 1, 8193, 1),
        (1, 1, 1, 1, 33),
    ] {
        assert!(CaptureLimits::new(args.0, args.1, args.2, args.3, args.4).is_err());
    }
}

#[tokio::test]
async fn bare_error_falls_back_to_actual_pusher_and_message_opt_in_is_independent() {
    let observers = run(
        vec![
            ObserverCapturePolicy {
                errors: Some(ErrorCaptureOptions::default()),
                ..Default::default()
            },
            ObserverCapturePolicy {
                limits: CaptureLimits::new(4, 10, 1, 10, 2).unwrap(),
                ..all()
            },
        ],
        vec![Frame::new(FrameData::Error(ErrorFrame {
            error: "large-private-error".into(),
            fatal: false,
            processor: Some("untrusted-processor-label".into()),
            category: ErrorCategory::Unknown,
        }))],
    )
    .await;
    let safe = captures(&observers[0]);
    let private = captures(&observers[1]);
    let CapturedPayload::Error(safe) = &safe[0].payload else {
        panic!()
    };
    let CapturedPayload::Error(private) = &private[0].payload else {
        panic!()
    };
    assert_eq!(safe.message, Captured::NotRequested);
    assert_eq!(safe.category, ErrorCategory::Unknown);
    assert_ne!(&*safe.processor.name, "untrusted-processor-label");
    assert!(safe.processor_usable);
    assert_eq!(
        private.message,
        Captured::Omitted(CaptureOmission::FieldBytes)
    );
}

#[tokio::test]
async fn detailed_results_preserve_finality_empty_error_truthiness_and_requested_fields() {
    let frame = |is_final, error: Option<&str>| {
        Frame::new(FrameData::FunctionCallResultDetailed(
            DetailedFunctionCallResultFrame {
                function_name: "f".into(),
                tool_call_id: "i".into(),
                arguments: json!("not-result-arguments"),
                result: json!("private-result"),
                run_llm: None,
                properties: Some(FunctionCallResultProperties {
                    is_final,
                    ..Default::default()
                }),
                error: error.map(str::to_owned),
            },
        ))
    };
    let observers = run(
        vec![
            all(),
            ObserverCapturePolicy {
                functions: Some(FunctionCaptureOptions {
                    results: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        ],
        vec![
            frame(false, None),
            frame(true, Some("")),
            frame(true, Some("failed")),
        ],
    )
    .await;
    let full = calls(&observers[0]);
    assert!(full[0].calls.is_empty());
    assert_eq!(full[1].calls[0].phase, FunctionCallPhase::Completed);
    assert_eq!(full[2].calls[0].phase, FunctionCallPhase::Failed);
    assert_eq!(full[1].calls[0].arguments, Captured::NotRequested);
    assert_eq!(full[2].calls[0].result, Captured::NotRequested);
    let results_only = calls(&observers[1]);
    assert_eq!(
        results_only[1].calls[0].result.value(),
        Some(&json!("private-result"))
    );
    assert_eq!(
        results_only[1].calls[0].tool_call_id,
        Captured::NotRequested
    );
    assert_eq!(results_only[2].calls[0].error, Captured::NotRequested);
}

#[tokio::test]
async fn json_node_budget_is_shared_across_the_whole_call_batch() {
    let policy = ObserverCapturePolicy {
        functions: Some(FunctionCaptureOptions {
            arguments: true,
            ..Default::default()
        }),
        limits: CaptureLimits::new(100, 100, 2, 3, 3).unwrap(),
        ..Default::default()
    };
    let frame = Frame::new(FrameData::FunctionCallsStarted(vec![
        FunctionCall {
            function_name: "a".into(),
            tool_call_id: "a".into(),
            arguments: json!([null]),
        },
        FunctionCall {
            function_name: "b".into(),
            tool_call_id: "b".into(),
            arguments: json!([null]),
        },
    ]));
    let observers = run(vec![policy], vec![frame]).await;
    let calls = calls(&observers[0]);
    assert!(calls[0].calls[0].arguments.value().is_some());
    assert_eq!(
        calls[0].calls[1].arguments,
        Captured::Omitted(CaptureOmission::JsonNodes)
    );
}
