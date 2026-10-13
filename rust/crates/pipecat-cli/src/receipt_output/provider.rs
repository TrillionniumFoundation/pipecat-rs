use super::{ReceiptOutput, ReceiptRunConfig, ReceiptSession, INPUT_BYTE_CAPACITY, INPUT_CAPACITY};
use crate::{
    config::{Options, ProviderConfig},
    AgentError, SessionReport,
};
use pipecat_core::{Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig};
use pipecat_media_transport::{
    AssistantSpokenHistorySink, OutputAdmissionPolicy, OutputTransportConfig, SpokenHistory,
    SpokenHistoryConfig,
};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
};
use pipecat_streaming_speech::{
    CartesiaClient, CartesiaConfig, CartesiaTtsProcessor, DeepgramClient, DeepgramConfig,
    DeepgramSttProcessor,
};
use pipecat_transports::WebSocketSession;
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, UserAggregatorConfig,
    UserTurnConfig,
};
use std::{sync::Arc, time::Duration};

pub(crate) async fn run_provider_session(
    session: WebSocketSession,
    options: &Options,
    providers: &ProviderConfig,
    mut input_processors: Vec<Arc<dyn FrameProcessor>>,
) -> Result<SessionReport, AgentError> {
    let owner =
        ReceiptSession::negotiate(session, options.receipt.clone(), providers.output_rate).await?;
    let prepared = async {
        if let Some(paths) = options.vad.clone() {
            let rate = providers.input_rate;
            let processor = tokio::task::spawn_blocking(move || {
                let runtime = pipecat_vad::RuntimeOptions {
                    library_path: paths.runtime,
                };
                let model = pipecat_vad::SileroModel::load(&runtime, paths.model)?;
                let analyzer =
                    pipecat_vad::SileroVadAnalyzer::new(model, rate, Default::default())?;
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
        let history = SpokenHistory::new(
            SpokenHistoryConfig::default(),
            Arc::new(AssistantSpokenHistorySink::new(pair.assistant())),
        )?;
        let boundary = Arc::new(history.source_boundary("v2-spoken-source")?);
        let output = Arc::new(ReceiptOutput::new(
            "v2-shared-output",
            OutputTransportConfig {
                sample_rate: Some(providers.output_rate),
                channels: 1,
                chunk_duration: Duration::from_millis(40),
                queue_capacity: INPUT_CAPACITY,
                queue_byte_capacity: INPUT_BYTE_CAPACITY,
                admission_policy: OutputAdmissionPolicy::Wait {
                    timeout: options.receipt.drain_timeout,
                },
                write_timeout: options
                    .receipt
                    .chunk_timeout
                    .max(options.receipt.cutoff_timeout)
                    + Duration::from_millis(20),
                auto_silence: false,
                end_silence: Duration::ZERO,
                ..Default::default()
            },
            owner.backend(),
            Some(history.clone()),
        )?);
        let shared_context = pair.context();
        let mut request = CompletionRequest::new(providers.model.clone(), context);
        request.system_instruction = Some(providers.system_prompt.clone());
        input_processors.extend([
            Arc::new(stt) as Arc<dyn FrameProcessor>,
            pair.user(),
            Arc::new(LlmProcessor::new(llm_client, request)),
            boundary,
            Arc::new(tts),
            output,
            pair.assistant(),
        ]);
        let mut initial_frames = vec![];
        if options.introduce {
            shared_context.append(Message::text(
                Role::Developer,
                "Please introduce yourself to the user.",
            ))?;
            initial_frames.push(Frame::new(FrameData::LlmRun));
        }
        let mut pipeline_config = PipelineConfig::default();
        pipeline_config.start.audio_in_sample_rate = providers.input_rate;
        pipeline_config.start.audio_out_sample_rate = providers.output_rate;
        pipeline_config.start.enable_metrics = true;
        pipeline_config.start.enable_usage_metrics = true;
        pipeline_config.shutdown_timeout = options.receipt.drain_timeout;
        Ok::<_, AgentError>((
            Pipeline::new(input_processors),
            pipeline_config,
            history,
            shared_context,
            initial_frames,
        ))
    }
    .await;
    let (pipeline, pipeline_config, history, context, initial_frames) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            let _ = owner.close().await;
            return Err(error);
        }
    };
    let report = owner
        .run(
            pipeline,
            pipeline_config,
            ReceiptRunConfig {
                input_rate: providers.input_rate,
                idle_timeout: options.idle_timeout,
                history: Some(history),
                initial_frames,
            },
        )
        .await?;
    Ok(SessionReport {
        context: context.snapshot()?,
        pipeline: report.pipeline,
        shutdown_requested: report.shutdown_requested,
    })
}
