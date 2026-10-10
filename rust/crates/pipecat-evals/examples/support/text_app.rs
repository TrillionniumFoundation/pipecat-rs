//! Native application composition shared by the executable and its wire tests.
use pipecat_core::{async_trait, FrameProcessor};
use pipecat_evals::bot_host::{BotHostError, EvalBotFactory, EvalBotPipeline, EvalBotSettings};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Message, Provider, Role,
    ToolDefinition,
};
use pipecat_turns::{AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair};
use serde_json::json;
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

pub struct TextBot {
    pub endpoint: String,
    pub model: String,
    pub introduce: bool,
    pub tool_calls: Arc<AtomicUsize>,
}
impl TextBot {
    pub fn new(endpoint: String, model: String) -> Result<Self, BotHostError> {
        let url = url::Url::parse(&endpoint).map_err(|e| BotHostError::Invalid(e.to_string()))?;
        if url.scheme() != "http"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.host_str().is_some_and(|host| {
                host.trim_matches(['[', ']'])
                    .parse::<IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
            })
        {
            return Err(BotHostError::Invalid("this example requires an explicit literal-loopback HTTP LLM endpoint without credentials/query/fragment".into()));
        }
        Ok(Self {
            endpoint,
            model,
            introduce: false,
            tool_calls: Arc::new(AtomicUsize::new(0)),
        })
    }
}
#[async_trait]
impl EvalBotFactory for TextBot {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if !settings.skip_tts {
            return Err(BotHostError::Invalid("text bot requires skip_tts".into()));
        }
        let mut config = ClientConfig::new(Provider::OpenAi, "native-loopback-eval");
        config.endpoint = self.endpoint.clone();
        config.request_timeout = Duration::from_secs(10);
        config.connect_timeout = Duration::from_secs(2);
        config.idle_timeout = Duration::from_secs(5);
        config.max_event_bytes = 64 * 1024;
        config.channel_capacity = 8;
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(config.connect_timeout)
            .build()
            .map_err(|e| BotHostError::Invalid(e.to_string()))?;
        let client = LlmClient::with_http_client(config, http)
            .map_err(|e| BotHostError::Invalid(e.to_string()))?;
        let mut context = Context::new(vec![Message::text(
            Role::System,
            "You are a concise text assistant. Use the add tool for arithmetic.",
        )]);
        context.tools = Some(vec![ToolDefinition {
            name: "add".into(),
            description: "Add two integers using native Rust".into(),
            parameters: json!({"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"],"additionalProperties":false}),
        }]);
        let pair = LLMContextAggregatorPair::new(
            context,
            Default::default(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Text,
            },
        );
        let mut executor = ToolExecutor::new(2, 16, 4096)?;
        let total = self.tool_calls.clone();
        // This counter is intentionally session-local. Identical tool IDs on a
        // later connection are independent native executions.
        let session_calls = Arc::new(AtomicUsize::new(0));
        executor.register(
            "add",
            RegisteredTool::new(Arc::new(move |call| {
                let total = total.clone();
                let session_calls = session_calls.clone();
                Box::pin(async move {
                    let a = call
                        .arguments
                        .get("a")
                        .and_then(|v| v.as_i64())
                        .ok_or("a must be an integer")?;
                    let b = call
                        .arguments
                        .get("b")
                        .and_then(|v| v.as_i64())
                        .ok_or("b must be an integer")?;
                    let sum = a.checked_add(b).ok_or("sum overflow")?;
                    total.fetch_add(1, Ordering::SeqCst);
                    let invocation = session_calls.fetch_add(1, Ordering::SeqCst) + 1;
                    Ok(json!({"sum":sum,"session_invocation":invocation}))
                })
            })),
        )?;
        let processors: Vec<Arc<dyn FrameProcessor>> = vec![
            pair.user(),
            Arc::new(LlmProcessor::new(
                client,
                CompletionRequest::new(&self.model, Context::default()),
            )),
            Arc::new(executor),
        ];
        Ok(EvalBotPipeline {
            context: pair.context(),
            processors,
            assistant: pair.assistant(),
            introduction: self.introduce.then(|| "Please introduce yourself.".into()),
        })
    }
}
