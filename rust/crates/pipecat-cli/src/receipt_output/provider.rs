use super::{ReceiptOutput, ReceiptRunConfig, ReceiptSession, INPUT_BYTE_CAPACITY, INPUT_CAPACITY};
use crate::{
    config::{Options, ProviderConfig},
    AgentError, SessionReport,
};
use pipecat_core::{Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig};
use pipecat_media_transport::{
    AssistantSpokenHistorySink, LiveVideoOutputConfig, OutputAdmissionPolicy,
    OutputTransportConfig, SpokenHistory, SpokenHistoryConfig,
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
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::net::TcpListener;

pub(crate) async fn run_provider_session(
    session: WebSocketSession,
    options: &Options,
    providers: &ProviderConfig,
    mut input_processors: Vec<Arc<dyn FrameProcessor>>,
) -> Result<SessionReport, AgentError> {
    let owner = if let Some(images) = &options.image_channel {
        let listener = match TcpListener::bind(SocketAddr::new(options.bind.ip(), 0)).await {
            Ok(listener) => listener,
            Err(error) => {
                session.cancel().await?;
                return Err(error.into());
            }
        };
        ReceiptSession::negotiate_with_images(
            session,
            options.receipt.clone(),
            providers.output_rate,
            listener,
            images.clone(),
        )
        .await?
    } else {
        ReceiptSession::negotiate(session, options.receipt.clone(), providers.output_rate).await?
    };
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
        // Image retention is explicitly additive; default audio budgets stay unchanged.
        let image_queue_bytes = options
            .image_channel
            .as_ref()
            .map(|images| {
                images
                    .max_pixel_bytes
                    .checked_add(4096)
                    .ok_or(AgentError::Config("image queue budget overflow"))
            })
            .transpose()?;
        let write_timeout = options
            .receipt
            .chunk_timeout
            .max(options.receipt.cutoff_timeout)
            .max(
                options
                    .image_channel
                    .as_ref()
                    .map_or(Duration::ZERO, |images| images.operation_timeout),
            )
            .checked_add(Duration::from_millis(20))
            .ok_or(AgentError::Config("output write deadline overflow"))?;
        let output_config = OutputTransportConfig {
            sample_rate: Some(providers.output_rate),
            channels: 1,
            chunk_duration: Duration::from_millis(40),
            queue_capacity: INPUT_CAPACITY,
            queue_byte_capacity: image_queue_bytes
                .map_or(INPUT_BYTE_CAPACITY, |bytes| bytes.max(INPUT_BYTE_CAPACITY)),
            admission_policy: OutputAdmissionPolicy::Wait {
                timeout: options.receipt.drain_timeout,
            },
            write_timeout,
            auto_silence: false,
            end_silence: Duration::ZERO,
            ..Default::default()
        };
        let output = Arc::new(if let Some(queue_byte_capacity) = image_queue_bytes {
            ReceiptOutput::new_with_images(
                "v2-shared-output",
                output_config,
                owner.backend(),
                Some(history.clone()),
                LiveVideoOutputConfig {
                    queue_capacity: 2,
                    queue_byte_capacity,
                    framerate: None,
                },
            )?
        } else {
            ReceiptOutput::new(
                "v2-shared-output",
                output_config,
                owner.backend(),
                Some(history.clone()),
            )?
        });
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
        // One trusted bounded startup clone, separately budgeted from remote input.
        let mut initial_frames = vec![];
        if let Some(image) = &options.initial_image {
            initial_frames.push(Frame::new(FrameData::OutputImage(image.frame())));
        }
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
            owner.close().await?;
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
