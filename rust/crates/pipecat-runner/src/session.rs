use crate::{
    processors::*, transport::BrowserSession, RunnerConfig, RunnerError, SessionFailure,
    SessionFailureStage, SttProvider,
};
#[cfg(feature = "webrtc")]
use pipecat_cli::config::ProviderConfig;
use pipecat_core::*;
use pipecat_rtvi::{RtviConfig, RtviEvent, RtviObserver, RtviObserverParams, RtviProcessor};
use pipecat_services::{CompletionRequest, Context, LlmClient, LlmProcessor, Message, Role};
use pipecat_transports::{SessionConfig, WebSocketSession};
use pipecat_turns::{AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch, time::Instant};

/// Retains observations independently from success/failure. Initialization can
/// fail before either observation exists; that state must remain explicit.
#[derive(Debug)]
pub(crate) struct SessionOutcome {
    pub generated_context: Option<Context>,
    pub worker_report: Option<WorkerReport>,
    pub failure: Option<SessionFailure>,
}
impl SessionOutcome {
    pub(crate) fn from_result(
        result: Result<Self, RunnerError>,
        stage: SessionFailureStage,
    ) -> Self {
        match result {
            Ok(outcome) => outcome,
            Err(error) => Self {
                generated_context: None,
                worker_report: None,
                failure: Some(SessionFailure {
                    stage,
                    error: Arc::new(error),
                    cleanup_errors: Vec::new(),
                }),
            },
        }
    }
    fn record_failure(&mut self, stage: SessionFailureStage, error: RunnerError) {
        if let Some(primary) = &mut self.failure {
            primary.cleanup_errors.push(Arc::new(error));
        } else {
            self.failure = Some(SessionFailure {
                stage,
                error: Arc::new(error),
                cleanup_errors: Vec::new(),
            });
        }
    }
}

/// Unlike a generic Result merge, successful session observations survive a
/// later transport/negotiation cleanup failure.
pub(crate) fn with_session_cleanup(
    primary: Result<SessionOutcome, RunnerError>,
    cleanup: Result<(), RunnerError>,
) -> Result<SessionOutcome, RunnerError> {
    let mut outcome = SessionOutcome::from_result(primary, SessionFailureStage::Initialization);
    if let Err(error) = cleanup {
        outcome.record_failure(SessionFailureStage::Cleanup, error);
    }
    Ok(outcome)
}

#[derive(Clone, Default)]
pub(crate) struct SessionResources {
    #[cfg(feature = "whisper")]
    pub local_stt_workers: Arc<crate::whisper::NativeModelOwners>,
}

impl SessionResources {
    pub fn for_session(&self) -> Self {
        Self {
            #[cfg(feature = "whisper")]
            local_stt_workers: Arc::new(crate::whisper::NativeModelOwners::for_session(
                self.local_stt_workers.clone(),
            )),
        }
    }
    pub async fn join_native(&self) {
        #[cfg(feature = "whisper")]
        self.local_stt_workers.wait_empty().await;
    }
}

pub async fn accept_session(
    listener: TcpListener,
    token: &str,
    config: &RunnerConfig,
    providers: &crate::cascade::SessionProviders,
    cancel: &mut watch::Receiver<bool>,
    resources: &SessionResources,
) -> Result<SessionOutcome, RunnerError> {
    let session_deadline = Instant::now() + config.max_session_duration;
    let deadline = (Instant::now() + config.connect_timeout).min(session_deadline);
    let session = loop {
        let (stream, _) = tokio::select! {
            biased;
            _=cancel.changed()=>return Err(RunnerError::Protocol("session canceled")),
            accepted=tokio::time::timeout_at(deadline,listener.accept())=>accepted.map_err(|_|RunnerError::Deadline)??,
        };
        let expected_path = format!("/ws-client/{token}");
        let origins = config.allowed_origins.clone();
        let allow_missing = config.allow_missing_origin;
        let accept = WebSocketSession::accept_with_request_policy(
            stream,
            SessionConfig {
                queue_capacity: 8,
                max_message_bytes: 128 * 1024,
                connect_timeout: Duration::from_secs(5),
                write_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            move |request| {
                let origin: Vec<_> = request.headers().get_all("origin").iter().collect();
                let origin_ok = match origin.as_slice() {
                    [] => allow_missing,
                    [value] => value.to_str().is_ok_and(|s| origins.iter().any(|x| x == s)),
                    _ => false,
                };
                origin_ok
                    && request.uri().path() == expected_path
                    && request.uri().query().is_none()
            },
        );
        let result = tokio::select! {biased;_=cancel.changed()=>return Err(RunnerError::Protocol("session canceled")),result=tokio::time::timeout_at(deadline,accept)=>result.map_err(|_|RunnerError::Deadline)?};
        if let Ok(session) = result {
            break session;
        }
    };
    drop(listener);
    let mut session = BrowserSession::WebSocket(session);
    let result = run_cascade(
        &mut session,
        config,
        providers,
        cancel,
        session_deadline,
        resources,
    )
    .await;
    let cleanup = session.finish().await;
    with_session_cleanup(result, cleanup)
}

// Cleanup has already been awaited. Preserve a lone original error unchanged;
// only simultaneous errors need an aggregate, with the original as its source.
#[cfg(test)]
pub(crate) fn with_transport_cleanup<T>(
    primary: Result<T, RunnerError>,
    cleanup: Result<(), RunnerError>,
) -> Result<T, RunnerError> {
    match (primary, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(primary), Ok(())) => Err(primary),
        (Ok(_), Err(cleanup)) => Err(cleanup),
        (Err(primary), Err(cleanup)) => Err(RunnerError::SessionCleanup {
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

#[cfg(feature = "webrtc")]
pub(crate) async fn run(
    session: &mut BrowserSession,
    config: &RunnerConfig,
    providers: &ProviderConfig,
    cancel: &mut watch::Receiver<bool>,
    session_deadline: Instant,
    resources: &SessionResources,
) -> Result<SessionOutcome, RunnerError> {
    let result = run_cascade(
        session,
        config,
        &crate::cascade::SessionProviders::Default(Box::new(providers.clone())),
        cancel,
        session_deadline,
        resources,
    )
    .await;
    Ok(SessionOutcome::from_result(
        result,
        SessionFailureStage::Initialization,
    ))
}

async fn run_cascade(
    session: &mut BrowserSession,
    config: &RunnerConfig,
    providers: &crate::cascade::SessionProviders,
    cancel: &mut watch::Receiver<bool>,
    session_deadline: Instant,
    _resources: &SessionResources,
) -> Result<SessionOutcome, RunnerError> {
    let rtvi=Arc::new(RtviProcessor::new(RtviConfig{max_message_bytes:session.message_limit(),capabilities:Some(json!({"voice_mode":providers.mode(),"context_mode":"generated-text","playback_acknowledgements":false,"input_sample_rate":providers.input_rate(),"output_sample_rate":session.output_sample_rate(providers.output_rate()),"synthesis_sample_rate":providers.output_rate()}).as_object().unwrap().clone()),..Default::default()}).map_err(|_|RunnerError::Rtvi)?);
    let handle = rtvi.handle();
    let mut events = handle.subscribe();
    let observer = Arc::new(
        RtviObserver::new(
            RtviObserverParams {
                bot_output_enabled: false,
                bot_speaking_enabled: false,
                ..Default::default()
            },
            Some(handle.clone()),
        )
        .map_err(|_| RunnerError::Rtvi)?,
    );
    let output = session.sender();
    let tap = |name| {
        Arc::new(EventTap {
            observer: observer.clone(),
            sender: output.clone(),
            name,
        }) as Arc<dyn FrameProcessor>
    };
    let mut processors: Vec<Arc<dyn FrameProcessor>> =
        vec![rtvi, Arc::new(NormalizeAudio::new(providers.input_rate()))];
    #[cfg(feature = "realtime")]
    let mut realtime_state = None;
    let context = match providers {
        #[cfg(feature = "realtime")]
        crate::cascade::SessionProviders::Realtime(realtime) => {
            let (context, state) = crate::realtime::compose(
                realtime,
                &mut processors,
                output.clone(),
                observer.clone(),
            )?;
            realtime_state = Some(state);
            context
        }
        _ => {
            if let Some(paths) = config.options.vad.clone() {
                let rate = providers.input_rate();
                let processor = tokio::task::spawn_blocking(move || {
                    let model = pipecat_vad::SileroModel::load(
                        &pipecat_vad::RuntimeOptions {
                            library_path: paths.runtime,
                        },
                        paths.model,
                    )?;
                    pipecat_vad::SileroVadProcessor::new(
                        pipecat_vad::SileroVadAnalyzer::new(model, rate, Default::default())?,
                        Default::default(),
                    )
                })
                .await
                .map_err(|_| RunnerError::Provider)?
                .map_err(|_| RunnerError::Provider)?;
                processors.push(Arc::new(processor));
            }
            let crate::cascade::SessionParts {
                stt,
                tts,
                llm,
                user_config,
                initial_context,
                tools,
                model,
                system_prompt,
            } = match providers {
                crate::cascade::SessionProviders::Default(providers) => match &config.stt {
                    SttProvider::Deepgram => {
                        crate::cascade::hosted_parts(providers, config.options.turn_timeout)?
                    }
                    #[cfg(feature = "whisper")]
                    SttProvider::Whisper(local) => {
                        let processor = tokio::select! {
                            biased;
                            _ = cancel.changed() => return Err(RunnerError::Protocol("session canceled during model loading")),
                            result = tokio::time::timeout_at(session_deadline, crate::whisper::OwnedWhisper::load(local, _resources.local_stt_workers.clone())) => result.map_err(|_| RunnerError::Deadline)??,
                        };
                        crate::cascade::hosted_parts_with_stt(
                            providers,
                            config.options.turn_timeout,
                            Arc::new(processor),
                        )?
                    }
                },
                #[cfg(feature = "realtime")]
                crate::cascade::SessionProviders::Realtime(_) => {
                    unreachable!("separate realtime composition")
                }
                #[cfg(feature = "soniox")]
                crate::cascade::SessionProviders::Soniox(providers) => providers.build_session()?,
            };
            let pair = LLMContextAggregatorPair::new(
                initial_context,
                user_config,
                AssistantAggregatorConfig {
                    text_mode: AssistantTextMode::Text,
                },
            );
            let context = pair.context();
            let mut request = CompletionRequest::new(model, Context::default());
            request.system_instruction = Some(system_prompt);
            let retention_budget = Arc::new(RetentionBudget::default());
            processors.extend([
                stt,
                retention_budget.clone(),
                tap("BrowserInputEvents"),
                pair.user(),
                tap("BrowserUserEvents"),
                Arc::new(LlmProcessor::new(
                    LlmClient::new(llm).map_err(|_| RunnerError::Provider)?,
                    request,
                )),
            ]);
            processors.extend(tools);
            processors.extend([
                retention_budget as Arc<dyn FrameProcessor>,
                tap("BrowserLlmEvents"),
                Arc::new(GeneratedText::new(output.clone())),
                Arc::new(GeneratedContext {
                    assistant: pair.assistant(),
                }),
                tts,
                tap("BrowserSynthesisEvents"),
                Arc::new(BrowserOutput {
                    sender: output.clone(),
                }),
            ]);
            context
        }
    };
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = providers.input_rate();
    pipeline_config.start.audio_out_sample_rate = providers.output_rate();
    pipeline_config.shutdown_timeout = Duration::from_secs(5);
    let pipeline = Pipeline::new(processors);
    if providers.is_realtime() {
        // Do not drop Pipeline::start on cancellation: its drop fallback owns
        // asynchronous cleanup. Await this bounded setup, then cancel+collect
        // the returned worker before releasing the browser/session owner.
        pipeline_config.startup_timeout = config.connect_timeout.min(Duration::from_secs(5));
    }
    let mut worker = if providers.is_realtime() {
        pipeline.start(pipeline_config).await?
    } else {
        tokio::select! {
            biased;
            _ = cancel.changed() => return Err(RunnerError::Protocol("session canceled during initialization")),
            result = tokio::time::timeout_at(session_deadline, pipeline.start(pipeline_config)) => result.map_err(|_| RunnerError::Deadline)??,
        }
    };
    let sender = worker.sender();
    #[allow(unused_mut)]
    let mut provider_ready = !providers.is_realtime();
    if provider_ready {
        handle
            .set_bot_ready(Some(
                json!({"implementation":"pipecat-rust-runner","context_mode":"generated-text"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ))
            .map_err(|_| RunnerError::Rtvi)?;
    }
    let began = Instant::now();
    let mut last_activity = began;
    let mut ready = false;
    let mut introduced = false;
    #[cfg(feature = "realtime")]
    let mut draining = false;
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    let result = loop {
        if *cancel.borrow() {
            break Ok(());
        }
        #[cfg(feature = "realtime")]
        if let Some(state) = &mut realtime_state {
            match *state.borrow_and_update() {
                pipecat_realtime::RealtimeConnectionState::Closed if !draining => {
                    break Err(RunnerError::Protocol(
                        "realtime provider closed before browser session ended",
                    ));
                }
                pipecat_realtime::RealtimeConnectionState::Ready if !provider_ready => {
                    provider_ready = true;
                    if handle.set_bot_ready(Some(json!({"implementation":"pipecat-rust-runner","voice_mode":"openai-realtime","context_mode":"generated-text"}).as_object().unwrap().clone())).is_err() { break Err(RunnerError::Rtvi); }
                }
                _ => {}
            }
        }
        if ready && provider_ready && !introduced && config.options.introduce {
            introduced = true;
            if context
                .append(Message::text(
                    Role::Developer,
                    "Please introduce yourself to the user.",
                ))
                .is_err()
            {
                break Err(RunnerError::Provider);
            }
            if sender
                .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
                .await
                .is_err()
            {
                break Err(RunnerError::Provider);
            }
        }
        // Enforce deadlines even when continuous RTP keeps the ready branches busy.
        if Instant::now() >= session_deadline
            || (!(ready && provider_ready) && began.elapsed() > config.connect_timeout)
            || last_activity.elapsed() > config.options.idle_timeout
        {
            break Err(RunnerError::Deadline);
        }
        tokio::select! {
            biased;
            _=cancel.changed()=>break Ok(()),
            event=events.recv()=>match event {
                Ok(RtviEvent::ClientReady(_))=>{
                    ready=true;
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_))=>break Err(RunnerError::Protocol("RTVI event overflow")),
                _=>{}
            },
            packet=session.recv_frame()=>{
                let Some(packet)=packet else {break Ok(());};last_activity=Instant::now();
                let frame=match packet {Ok(frame)=>frame,Err(error)=>break Err(error)};
                #[cfg(feature = "realtime")]
                if providers.is_realtime() {
                    if let Err(error) = crate::realtime::admit_browser(&frame) { break Err(error); }
                    if matches!(&frame.data, FrameData::InputTransportMessage(value) if value["type"] == "disconnect-bot") { draining = true; }
                    if !provider_ready && !matches!(&frame.data, FrameData::InputTransportMessage(value) if value["type"] == "client-ready") { break Err(RunnerError::Protocol("provider readiness must precede input")); }
                }
                // RTP capture can precede SCTP readiness. Do not retain early microphone audio.
                if !ready && session.is_realtime_audio() && matches!(frame.data, FrameData::InputAudio(_)) { continue; }
                if !ready && !matches!(&frame.data,FrameData::InputTransportMessage(value) if value["type"]=="client-ready") {break Err(RunnerError::Protocol("client-ready must precede input"));}
                if sender.send(frame,Direction::Downstream).await.is_err(){break Err(RunnerError::Provider);}
            },
            frame=worker.recv()=>match frame {
                None=>break Ok(()),
                Some(frame) if frame.frame.data.as_error_frame().is_some()=>break Err(RunnerError::Provider),
                _=>{}
            },
            _=timer.tick()=>{
                if Instant::now() >= session_deadline || (!(ready&&provider_ready)&&began.elapsed()>config.connect_timeout)||last_activity.elapsed()>config.options.idle_timeout {break Err(RunnerError::Deadline);}
            }
        }
    };
    let cancellation = if worker.has_finished() {
        Ok(())
    } else {
        match worker.cancel("browser session closing").await {
            // A terminal worker can win the check/send race. Collection still
            // supplies its actual final report; Closed alone adds no failure.
            Err(PipelineError::Closed) if worker.has_finished() => Ok(()),
            result => result,
        }
    };
    // Collect and snapshot separately: failure of either must not erase the
    // observations obtained from the other, or the primary runtime failure.
    let collected = worker.collect().await;
    let snapshot = context.snapshot();
    let mut outcome = SessionOutcome {
        generated_context: None,
        worker_report: None,
        failure: None,
    };
    if let Err(error) = result {
        outcome.record_failure(SessionFailureStage::Runtime, error);
    }
    if let Err(error) = cancellation {
        outcome.record_failure(SessionFailureStage::Cleanup, error.into());
    }
    match collected {
        Ok((_, report)) => {
            if report.total_errors > 0 && outcome.failure.is_none() {
                outcome.record_failure(SessionFailureStage::Runtime, RunnerError::Provider);
            }
            outcome.worker_report = Some(report);
        }
        Err(error) => outcome.record_failure(SessionFailureStage::Cleanup, error.into()),
    }
    match snapshot {
        Ok(snapshot) => outcome.generated_context = Some(snapshot),
        Err(_) => outcome.record_failure(SessionFailureStage::Cleanup, RunnerError::Provider),
    }
    Ok(outcome)
}

#[cfg(test)]
mod cleanup_tests {
    use super::*;
    use std::error::Error as _;
    #[test]
    fn transport_cleanup_preserves_primary_priority_and_both_typed_diagnostics() {
        assert_eq!(with_transport_cleanup(Ok(17), Ok(())).unwrap(), 17);
        assert!(matches!(
            with_transport_cleanup::<()>(Err(RunnerError::Protocol("primary")), Ok(())),
            Err(RunnerError::Protocol("primary"))
        ));
        assert!(matches!(
            with_transport_cleanup(
                Ok(17),
                Err(RunnerError::Transport(
                    pipecat_transports::TransportError::Timeout("close")
                ))
            ),
            Err(RunnerError::Transport(
                pipecat_transports::TransportError::Timeout("close")
            ))
        ));
        let both = with_transport_cleanup::<()>(
            Err(RunnerError::Protocol("primary")),
            Err(RunnerError::Transport(
                pipecat_transports::TransportError::Timeout("close"),
            )),
        )
        .unwrap_err();
        assert_eq!(
            both.source().unwrap().to_string(),
            "browser protocol: primary"
        );
        assert_eq!(
            both.to_string(),
            "browser protocol: primary; transport cleanup also failed: WebSocket failed"
        );
        assert!(
            matches!(&both, RunnerError::SessionCleanup {primary,cleanup}
            if matches!(**primary,RunnerError::Protocol("primary"))
            && matches!(**cleanup,RunnerError::Transport(pipecat_transports::TransportError::Timeout("close"))))
        );
    }
    #[test]
    fn session_cleanup_retains_observed_context_counts_and_original_failure() {
        let context = Context {
            messages: vec![Message::text(Role::Assistant, "completed before failure")],
            ..Default::default()
        };
        let mut primary = SessionOutcome {
            generated_context: Some(context.clone()),
            worker_report: Some(WorkerReport {
                state: WorkerState::Failed,
                errors: vec![ErrorFrame::new("original processor diagnostic")],
                dropped_frames: 0,
                total_errors: 3,
                evicted_errors: 2,
            }),
            failure: None,
        };
        primary.record_failure(
            SessionFailureStage::Runtime,
            RunnerError::Protocol("original runtime failure"),
        );
        let outcome = with_session_cleanup(
            Ok(primary),
            Err(RunnerError::Transport(
                pipecat_transports::TransportError::Timeout("close"),
            )),
        )
        .unwrap();
        let summary = crate::SessionSummary::from_outcome("session".into(), outcome);
        assert!(!summary.successful);
        assert!(summary.generated_context_observed);
        assert_eq!(summary.generated_context, context);
        assert_eq!(summary.pipeline_errors, 3);
        assert_eq!(summary.observed_pipeline_errors, Some(3));
        let report = summary.worker_report.unwrap();
        assert_eq!(report.evicted_errors, 2);
        assert_eq!(report.errors[0].error, "original processor diagnostic");
        let failure = summary.failure.unwrap();
        assert_eq!(failure.stage, SessionFailureStage::Runtime);
        assert!(matches!(
            &*failure.error,
            RunnerError::Protocol("original runtime failure")
        ));
        assert_eq!(failure.cleanup_errors.len(), 1);
        assert!(matches!(
            &*failure.cleanup_errors[0],
            RunnerError::Transport(pipecat_transports::TransportError::Timeout("close"))
        ));
    }
    #[test]
    fn cleanup_failure_after_success_preserves_observations_and_marks_cleanup_stage() {
        let outcome = with_session_cleanup(
            Ok(SessionOutcome {
                generated_context: Some(Context::default()),
                worker_report: Some(WorkerReport {
                    state: WorkerState::Ended,
                    errors: Vec::new(),
                    dropped_frames: 0,
                    total_errors: 0,
                    evicted_errors: 0,
                }),
                failure: None,
            }),
            Err(RunnerError::Deadline),
        )
        .unwrap();
        let summary = crate::SessionSummary::from_outcome("session".into(), outcome);
        assert!(!summary.successful);
        assert!(summary.generated_context_observed);
        assert_eq!(summary.observed_pipeline_errors, Some(0));
        let failure = summary.failure.unwrap();
        assert_eq!(failure.stage, SessionFailureStage::Cleanup);
        assert!(matches!(&*failure.error, RunnerError::Deadline));
        assert!(failure.cleanup_errors.is_empty());
    }
    #[cfg(feature = "webrtc")]
    #[test]
    fn native_cleanup_conversion_retains_shutdown_details() {
        // Type-conversion coverage, not an injected native worker experiment.
        let detail = "owned transport worker failures: panicked=1, cancelled=0, timed_out=0";
        let error: RunnerError = pipecat_webrtc::Error::Shutdown(detail.into()).into();
        assert!(
            matches!(&error, RunnerError::WebRtcCleanup(pipecat_webrtc::Error::Shutdown(text)) if text == detail)
        );
        assert!(error.to_string().contains(detail));
        assert!(error.source().unwrap().to_string().contains(detail));
    }
}
