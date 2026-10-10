//! Editable eval-only prompt, native tool and fresh text-pipeline factory.
//! Share application behavior with the voice runner only after qualifying that composition.
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
    endpoint: String,
    model: String,
}
impl TextBot {
    pub fn new(endpoint: String, model: String) -> Result<Self, &'static str> {
        if endpoint.is_empty()
            || endpoint.len() > 4096
            || endpoint
                .chars()
                .any(|c| c.is_control() || c.is_whitespace() || c == '\\')
        {
            return Err("model endpoint must be an explicit bounded loopback HTTP URL");
        }
        if model.trim().is_empty() || model.len() > 256 || model.chars().any(char::is_control) {
            return Err("model must be nonempty, at most 256 bytes, without control characters");
        }
        let url = url::Url::parse(&endpoint).map_err(|_| "invalid model endpoint")?;
        let authority = endpoint
            .strip_prefix("http://")
            .and_then(|value| value.split_once('/'))
            .map(|(authority, _)| authority)
            .ok_or("model endpoint requires literal-loopback HTTP and an explicit path")?;
        let (host, port) = if let Some(ipv6) = authority.strip_prefix('[') {
            let (host, suffix) = ipv6.split_once(']').ok_or("invalid model endpoint")?;
            (
                host,
                if suffix.is_empty() {
                    None
                } else {
                    Some(suffix.strip_prefix(':').ok_or("invalid model endpoint")?)
                },
            )
        } else {
            authority
                .split_once(':')
                .map_or((authority, None), |(host, port)| (host, Some(port)))
        };
        if !host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
            || port.is_some_and(|port| {
                port.is_empty()
                    || !port.bytes().all(|byte| byte.is_ascii_digit())
                    || !port.parse::<u16>().is_ok_and(|value| value != 0)
            })
            || url.scheme() != "http"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() == "/"
        {
            return Err("model endpoint requires a literal-loopback HTTP address and full path, without credentials, query, or fragment");
        }
        Ok(Self { endpoint, model })
    }
}

#[async_trait]
impl EvalBotFactory for TextBot {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if !settings.skip_tts {
            return Err(BotHostError::Invalid("text bot requires skip_tts".into()));
        }
        // This fixed nonsecret marker is required by the client API, not a user credential.
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
            .timeout(config.request_timeout)
            .build()
            .map_err(|_| BotHostError::Invalid("cannot construct bounded HTTP client".into()))?;
        let client = LlmClient::with_http_client(config, http).map_err(|_| {
            BotHostError::Invalid("invalid local model client configuration".into())
        })?;
        let mut context = Context::new(vec![Message::text(
            Role::System,
            "You are a concise text assistant. Use the add tool for arithmetic, include the result as a number, and remember it for the next turn.",
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
        let session_calls = Arc::new(AtomicUsize::new(0));
        executor.register(
            "add",
            RegisteredTool::new(Arc::new(move |call| {
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
            introduction: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::TextBot;

    #[test]
    fn endpoints_require_explicit_literal_loopback_and_full_path() {
        for endpoint in [
            "http://127.0.0.1:1234/v1/chat/completions",
            "http://127.1.2.3/v1/chat/completions",
            "http://[::1]:1234/v1/chat/completions",
        ] {
            assert!(TextBot::new(endpoint.into(), "synthetic".into()).is_ok());
        }
        for endpoint in [
            "",
            "http://localhost/v1/chat/completions",
            "http://example.com/v1/chat/completions",
            "http://192.0.2.1/v1/chat/completions",
            "http://0.0.0.0/v1/chat/completions",
            "https://127.0.0.1/v1/chat/completions",
            "http://[::]/v1/chat/completions",
            "http://user:SECRET@127.0.0.1/v1/chat/completions",
            "http://127.0.0.1/v1/chat/completions?SECRET",
            "http://127.0.0.1/v1/chat/completions#SECRET",
            "http://127.1/v1/chat/completions",
            "http://2130706433/v1/chat/completions",
            "http://0x7f000001/v1/chat/completions",
            "http://127.0.0.1:0/v1/chat/completions",
            "http://127.0.0.1",
            "http://127.0.0.1/",
            "http://127.0.0.1/\nSECRET",
            "http://127.0.0.1\\evil/v1/chat/completions",
        ] {
            let error = TextBot::new(endpoint.into(), "synthetic".into())
                .err()
                .expect("invalid endpoint accepted");
            assert!(!error.contains("SECRET"));
        }
        assert!(
            TextBot::new(format!("http://127.0.0.1/{}", "a".repeat(4096)), "x".into()).is_err()
        );
        for model in ["".into(), "  ".into(), "a\nb".into(), "x".repeat(257)] {
            assert!(TextBot::new("http://127.0.0.1/v1/chat/completions".into(), model).is_err());
        }
    }
}
