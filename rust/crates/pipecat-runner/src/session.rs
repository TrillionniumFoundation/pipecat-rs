use crate::{processors::*, transport::BrowserSession, RunnerConfig, RunnerError, SttProvider};
use pipecat_cli::config::ProviderConfig;
use pipecat_core::*;
use pipecat_rtvi::{RtviConfig, RtviEvent, RtviObserver, RtviObserverParams, RtviProcessor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use pipecat_streaming_speech::{
    CartesiaClient, CartesiaConfig, CartesiaTtsProcessor, DeepgramClient, DeepgramConfig,
    DeepgramSttProcessor,
};
use pipecat_transports::{SessionConfig, WebSocketSession};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, UserAggregatorConfig,
    UserTurnConfig,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::watch, time::Instant};

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
    providers: &ProviderConfig,
    cancel: &mut watch::Receiver<bool>,
    resources: &SessionResources,
) -> Result<(Context, u64), RunnerError> {
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
    let result = run(
        &mut session,
        config,
        providers,
        cancel,
        session_deadline,
        resources,
    )
    .await;
    session.finish().await;
    result
}

pub(crate) async fn run(
    session: &mut BrowserSession,
    config: &RunnerConfig,
    providers: &ProviderConfig,
    cancel: &mut watch::Receiver<bool>,
    session_deadline: Instant,
    _resources: &SessionResources,
) -> Result<(Context, u64), RunnerError> {
    let rtvi=Arc::new(RtviProcessor::new(RtviConfig{max_message_bytes:session.message_limit(),capabilities:Some(json!({"context_mode":"generated-text","playback_acknowledgements":false,"input_sample_rate":providers.input_rate,"output_sample_rate":session.output_sample_rate(providers.output_rate),"synthesis_sample_rate":providers.output_rate}).as_object().unwrap().clone()),..Default::default()}).map_err(|_|RunnerError::Rtvi)?);
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
        vec![rtvi, Arc::new(NormalizeAudio::new(providers.input_rate))];
    if let Some(paths) = config.options.vad.clone() {
        let rate = providers.input_rate;
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
    let stt: Arc<dyn FrameProcessor> = match &config.stt {
        SttProvider::Deepgram => {
            let mut stt = DeepgramConfig::new(providers.deepgram_key.clone());
            stt.endpoint = providers.deepgram_endpoint.clone();
            stt.sample_rate = providers.input_rate;
            stt.channels = 1;
            Arc::new(DeepgramSttProcessor::new(
                DeepgramClient::new(stt).map_err(|_| RunnerError::Provider)?,
            ))
        }
        #[cfg(feature = "whisper")]
        SttProvider::Whisper(local) => {
            let processor = tokio::select! {
                biased;
                _ = cancel.changed() => return Err(RunnerError::Protocol("session canceled during model loading")),
                result = tokio::time::timeout_at(session_deadline, crate::whisper::OwnedWhisper::load(local, _resources.local_stt_workers.clone())) => result.map_err(|_| RunnerError::Deadline)??,
            };
            Arc::new(processor)
        }
    };
    let mut tts = CartesiaConfig::new(providers.cartesia_key.clone(), providers.voice_id.clone());
    tts.endpoint = providers.cartesia_endpoint.clone();
    tts.sample_rate = providers.output_rate;
    let mut llm = ClientConfig::new(Provider::OpenAi, providers.openai_key.clone());
    llm.endpoint = providers.openai_endpoint.clone();
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                speech_timeout: config.options.turn_timeout,
                ..Default::default()
            },
            ..Default::default()
        },
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Text,
        },
    );
    let context = pair.context();
    let mut request = CompletionRequest::new(providers.model.clone(), Context::default());
    request.system_instruction = Some(providers.system_prompt.clone());
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
        retention_budget,
        tap("BrowserLlmEvents"),
        Arc::new(GeneratedText::new(output.clone())),
        Arc::new(GeneratedContext {
            assistant: pair.assistant(),
        }),
        Arc::new(CartesiaTtsProcessor::new(
            CartesiaClient::new(tts).map_err(|_| RunnerError::Provider)?,
        )),
        tap("BrowserSynthesisEvents"),
        Arc::new(BrowserOutput {
            sender: output.clone(),
        }),
    ]);
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = providers.input_rate;
    pipeline_config.start.audio_out_sample_rate = providers.output_rate;
    pipeline_config.shutdown_timeout = Duration::from_secs(5);
    let pipeline = Pipeline::new(processors);
    let mut worker = tokio::select! {
        biased;
        _ = cancel.changed() => return Err(RunnerError::Protocol("session canceled during initialization")),
        result = tokio::time::timeout_at(session_deadline, pipeline.start(pipeline_config)) => result.map_err(|_| RunnerError::Deadline)??,
    };
    let sender = worker.sender();
    handle
        .set_bot_ready(Some(
            json!({"implementation":"pipecat-rust-runner","context_mode":"generated-text"})
                .as_object()
                .unwrap()
                .clone(),
        ))
        .map_err(|_| RunnerError::Rtvi)?;
    let began = Instant::now();
    let mut last_activity = began;
    let mut ready = false;
    let mut introduced = false;
    let mut timer = tokio::time::interval(Duration::from_millis(100));
    let result = loop {
        // Enforce deadlines even when continuous RTP keeps the ready branches busy.
        if Instant::now() >= session_deadline
            || (!ready && began.elapsed() > config.connect_timeout)
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
                    if !introduced&&config.options.introduce{
                        introduced=true;
                        context.append(Message::text(Role::Developer,"Please introduce yourself to the user.")).map_err(|_|RunnerError::Provider)?;
                        if sender.send(Frame::new(FrameData::LlmRun),Direction::Downstream).await.is_err(){break Err(RunnerError::Provider);}
                    }
                },
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_))=>break Err(RunnerError::Protocol("RTVI event overflow")),
                _=>{}
            },
            packet=session.recv_frame()=>{
                let Some(packet)=packet else {break Ok(());};last_activity=Instant::now();
                let frame=match packet {Ok(frame)=>frame,Err(error)=>break Err(error)};
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
                if Instant::now() >= session_deadline || (!ready&&began.elapsed()>config.connect_timeout)||last_activity.elapsed()>config.options.idle_timeout {break Err(RunnerError::Deadline);}
            }
        }
    };
    if !worker.has_finished() {
        let _ = worker.cancel("browser session closing").await;
    }
    let (_, report) = worker.collect().await?;
    let snapshot = context.snapshot().map_err(|_| RunnerError::Provider)?;
    result?;
    if report.total_errors > 0 {
        return Err(RunnerError::Provider);
    }
    Ok((snapshot, report.total_errors))
}
