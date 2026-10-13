//! Runnable native voice agent and its playback-confirmed transport boundary.
pub mod config;
pub mod playback;
pub mod receipt_output;
pub mod smoke;
pub mod wire;
use config::{Options, ProviderConfig};
use pipecat_core::{
    Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig, WorkerReport,
};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use pipecat_streaming_speech::{
    CartesiaClient, CartesiaConfig, CartesiaTtsProcessor, DeepgramClient, DeepgramConfig,
    DeepgramSttProcessor,
};
use pipecat_transports::{OriginPolicy, SessionConfig, WebSocketSession};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, UserAggregatorConfig,
    UserTurnConfig,
};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    time::Instant,
};

#[derive(Debug, thiserror::Error)]
pub enum AgentError {
    #[error("shutdown failed: {0}")]
    ShutdownFailed(Box<AgentError>),
    #[error("configuration: {0}")]
    Config(&'static str),
    #[error("protocol: {0}")]
    Protocol(&'static str),
    #[error("native Silero initialization failed; check model/runtime paths and PCM rate")]
    Vad,
    #[error("provider initialization failed; check configuration")]
    Provider,
    #[error("provider request failed; check account access and connectivity")]
    ProviderRequest,
    #[error("I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("WebSocket transport failed")]
    Transport(#[from] pipecat_transports::TransportError),
    #[error("protobuf frame is invalid")]
    Protobuf(#[from] pipecat_protocol::ProtocolError),
    #[error("pipeline failed")]
    Pipeline(#[from] pipecat_core::PipelineError),
    #[error("pipeline processor failed")]
    Processor(#[from] pipecat_core::ProcessorError),
    #[error("background task failed")]
    Task(#[from] tokio::task::JoinError),
    #[error("smoke verification failed: {0}")]
    Smoke(&'static str),
}
pub struct SessionReport {
    pub context: Context,
    pub pipeline: WorkerReport,
    pub shutdown_requested: bool,
}
struct AbortTask<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Serve one session at a time, with a fresh provider connection and isolated
/// conversation context per client. The default loopback binding is deliberate.
pub async fn serve(options: Options, providers: ProviderConfig) -> Result<(), AgentError> {
    if !options.bind.ip().is_loopback() {
        return Err(AgentError::Config("listener must use a loopback address"));
    }
    let listener = TcpListener::bind(options.bind).await?;
    println!(
        "Native voice agent listening on ws://{}",
        listener.local_addr()?
    );
    loop {
        let stream = tokio::select! {
            accepted=listener.accept()=>accepted?.0,
            result=tokio::signal::ctrl_c()=>{result?;return Ok(());}
        };
        let result = run_session(stream, &options, &providers).await;
        match &result {
            Ok(report) => println!(
                "Session ended: {:?}, {} provider/runtime errors",
                report.pipeline.state, report.pipeline.total_errors
            ),
            Err(error) => eprintln!("Session ended: {error}"),
        }
        if options.once
            || matches!(&result, Err(AgentError::ShutdownFailed(_)))
            || result
                .as_ref()
                .is_ok_and(|report| report.shutdown_requested)
        {
            return result.map(|_| ());
        }
    }
}

/// Runs the complete native provider pipeline against an accepted WebSocket.
/// The returned context is useful for application persistence or assertions;
/// this example does not persist or log private conversation contents.
pub async fn run_session(
    stream: TcpStream,
    options: &Options,
    providers: &ProviderConfig,
) -> Result<SessionReport, AgentError> {
    run_session_with_input_processors(stream, options, providers, vec![]).await
}

/// Adds native input processors, such as a model-backed VAD, before STT.
/// Processors must preserve the configured PCM format and forward frames.
/// Each session requires its own processor instances and state.
pub async fn run_session_with_input_processors(
    stream: TcpStream,
    options: &Options,
    providers: &ProviderConfig,
    mut input_processors: Vec<Arc<dyn FrameProcessor>>,
) -> Result<SessionReport, AgentError> {
    let session_config = SessionConfig {
        queue_capacity: 8,
        max_message_bytes: if options.playback_protocol == receipt_output::PlaybackProtocol::ChunkV2
        {
            receipt_output::MAX_PACKET_BYTES
        } else {
            SessionConfig::default().max_message_bytes
        },
        write_timeout: Duration::from_secs(5),
        ..Default::default()
    };
    let mut session = WebSocketSession::accept_with_origin_policy(
        stream,
        session_config,
        OriginPolicy::LoopbackOnlyAllowMissing,
    )
    .await?;
    if options.playback_protocol == receipt_output::PlaybackProtocol::ChunkV2 {
        return receipt_output::run_provider_session(session, options, providers, input_processors)
            .await;
    }
    if let Some(paths) = options.vad.clone() {
        let rate = providers.input_rate;
        let processor = tokio::task::spawn_blocking(move || {
            let runtime = pipecat_vad::RuntimeOptions {
                library_path: paths.runtime,
            };
            let model = pipecat_vad::SileroModel::load(&runtime, paths.model)?;
            let analyzer = pipecat_vad::SileroVadAnalyzer::new(model, rate, Default::default())?;
            pipecat_vad::SileroVadProcessor::new(analyzer, Default::default())
        })
        .await?
        .map_err(|_| AgentError::Vad)?;
        input_processors.insert(0, Arc::new(processor));
    }
    let mut stt_config = DeepgramConfig::new(providers.deepgram_key.clone());
    stt_config.endpoint = providers.deepgram_endpoint.clone();
    stt_config.sample_rate = providers.input_rate;
    stt_config.channels = 1;
    let stt = DeepgramSttProcessor::new(
        DeepgramClient::new(stt_config).map_err(|_| AgentError::Provider)?,
    );
    let mut tts_config =
        CartesiaConfig::new(providers.cartesia_key.clone(), providers.voice_id.clone());
    tts_config.endpoint = providers.cartesia_endpoint.clone();
    tts_config.sample_rate = providers.output_rate;
    let tts = CartesiaTtsProcessor::new(
        CartesiaClient::new(tts_config).map_err(|_| AgentError::Provider)?,
    );
    let mut llm_config = ClientConfig::new(Provider::OpenAi, providers.openai_key.clone());
    llm_config.endpoint = providers.openai_endpoint.clone();
    let llm_client = LlmClient::new(llm_config).map_err(|_| AgentError::Provider)?;
    let context = Context::default();
    let pair = LLMContextAggregatorPair::new(
        context.clone(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                speech_timeout: options.turn_timeout,
                ..Default::default()
            },
            ..Default::default()
        },
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    let (output, channels) = playback::PlaybackOutput::new(pair.assistant());
    let mut writer = AbortTask(tokio::spawn(channels.run(session.sender())));
    let shared_context = pair.context();
    let mut request = CompletionRequest::new(providers.model.clone(), context);
    request.system_instruction = Some(providers.system_prompt.clone());
    input_processors.extend([
        Arc::new(stt) as Arc<dyn FrameProcessor>,
        pair.user(),
        Arc::new(LlmProcessor::new(llm_client, request)),
        Arc::new(tts),
        output.clone(),
        pair.assistant(),
    ]);
    let pipeline = Pipeline::new(input_processors);
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = providers.input_rate;
    pipeline_config.start.audio_out_sample_rate = providers.output_rate;
    pipeline_config.start.enable_metrics = true;
    pipeline_config.start.enable_usage_metrics = true;
    pipeline_config.shutdown_timeout = Duration::from_secs(10);
    let mut worker = pipeline.start(pipeline_config).await?;
    let sender = worker.sender();
    if options.introduce {
        shared_context.append(Message::text(
            Role::Developer,
            "Please introduce yourself to the user.",
        ))?;
        sender
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await?;
    }
    let mut last_activity = Instant::now();
    let mut timer = tokio::time::interval(Duration::from_millis(250));
    let mut end_requested = false;
    let mut shutdown_requested = false;
    let result: Result<(), AgentError> = loop {
        tokio::select! {
            packet=session.recv(),if !end_requested=> {
                let Some(packet)=packet else {let _=sender.cancel("client disconnected").await;break Ok(());};
                last_activity=Instant::now();
                match wire::decode(packet,providers.input_rate) {
                    Ok(wire::ClientInput::Frame(frame))=> {
                        if matches!(frame.data,FrameData::Interruption|FrameData::VadUserStartedSpeaking(_))
                            || matches!(&frame.data,FrameData::InputText(text) if !text.text.trim().is_empty()) {
                            if let Err(e)=output.invalidate() {break Err(e.into());}
                        }
                        if let Err(e)=sender.send(*frame,Direction::Downstream).await {break Err(e.into());}
                    },
                    Ok(wire::ClientInput::PlaybackComplete(id))=>if let Err(e)=output.acknowledge(id).await {break Err(e);},
                    Ok(wire::ClientInput::V2(_)) | Ok(wire::ClientInput::Cancel)=>break Err(AgentError::Protocol("v2 control requires explicit chunk-v2 mode")),
                    Ok(wire::ClientInput::End)=>{end_requested=true;if let Err(e)=sender.end().await {break Err(e.into());}},
                    Err(e)=>break Err(e),
                }
            }
            frame=worker.recv()=> {
                match frame {
                    None=>break Ok(()),
                    Some(frame) if matches!(frame.frame.data,FrameData::Error(_))=>break Err(AgentError::ProviderRequest),
                    Some(_)=>{},
                }
            }
            result=&mut writer.0=>break result.map_err(AgentError::from).and_then(|r|r).and(Err(AgentError::Protocol("output writer stopped"))),
            _=timer.tick()=> {
                if last_activity.elapsed()>options.idle_timeout {break Err(AgentError::Protocol("client idle timeout"));}
                if output.playback_timed_out(Duration::from_secs(120)) {break Err(AgentError::Protocol("remote playback acknowledgement timeout"));}
            }
            result=tokio::signal::ctrl_c()=> {
                if let Err(e)=result {break Err(e.into());}
                shutdown_requested=true;
                let _=sender.cancel("shutdown requested").await;
                break Ok(());
            }
        }
    };
    if !worker.has_finished() {
        let _ = worker.cancel("session closing").await;
    }
    let (_, report) = worker.collect().await?;
    let context = shared_context.snapshot()?;
    writer.0.abort();
    let _ = session.finish().await;
    result?;
    if report.total_errors > 0 {
        return Err(AgentError::ProviderRequest);
    }
    Ok(SessionReport {
        context,
        pipeline: report,
        shutdown_requested,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[tokio::test]
    async fn external_browser_origin_is_rejected_before_connecting_any_provider() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let provider = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let provider_address = provider.local_addr().unwrap();
        let mut providers = ProviderConfig::from_lookup(|key| {
            key.ends_with("KEY").then(|| "synthetic-only".into())
        })
        .unwrap();
        providers.deepgram_endpoint = format!("ws://{provider_address}/v1/listen");
        providers.openai_endpoint = format!("http://{provider_address}/v1/chat/completions");
        providers.cartesia_endpoint = format!("ws://{provider_address}/tts/websocket");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            run_session(
                stream,
                &Options {
                    introduce: false,
                    ..Default::default()
                },
                &providers,
            )
            .await
        });
        let mut socket = TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("GET / HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\nOrigin: https://untrusted.example\r\n\r\n").as_bytes()).await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(String::from_utf8_lossy(&response).starts_with("HTTP/1.1 403"));
        assert!(matches!(
            server.await.unwrap(),
            Err(AgentError::Transport(_))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), provider.accept())
                .await
                .is_err()
        );
    }
}
