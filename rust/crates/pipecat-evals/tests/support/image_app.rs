//! Synthetic application policy: consume real RTVI user input, request its image,
//! then let the ordinary standard assistant and native LLM own all inference.
use pipecat_core::*;
use pipecat_evals::bot_host::{BotHostError, EvalBotFactory, EvalBotPipeline, EvalBotSettings};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, SharedContext,
};
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore};

#[derive(Debug, Clone)]
pub struct ObservedImage {
    pub id: u64,
    pub request_id: u64,
    pub rgb: Vec<u8>,
    pub size: (u32, u32),
    pub format: Option<String>,
    pub user_id: String,
    pub text: Option<String>,
    pub append_to_context: Option<bool>,
    pub request: Option<UserImageRequestFrame>,
    pub source: Option<String>,
    pub destination: Option<String>,
}
pub struct Trace {
    pub requests: Mutex<Vec<(u64, String)>>,
    pub images: Mutex<Vec<ObservedImage>>,
    pub contexts: Mutex<Vec<SharedContext>>,
    pub interruptions: AtomicUsize,
    pub cleaned: AtomicUsize,
    pub dropped: AtomicUsize,
    pub image_arrived: Notify,
    pub request_arrived: Notify,
    pub hold_next_image: AtomicBool,
    pub image_gate: Semaphore,
    pub held_image_fence: Mutex<Option<InvocationFence>>,
    pub observe_held_image_close: AtomicBool,
    pub held_image_constraint: Mutex<Option<NativeUserImageConstraint>>,
    pub hold_next_context: AtomicBool,
    pub context_arrived: Notify,
    pub context_gate: Semaphore,
    pub held_images: AtomicUsize,
    pub cleanup_arrived: Notify,
    pub end_arrived: Notify,
    pub assistant_image_calls: AtomicUsize,
    pub assistant_image_arrived: Notify,
    pub probe_after_response: Mutex<Option<Duration>>,
    probe_fence: Mutex<Option<InvocationFence>>,
}
impl Default for Trace {
    fn default() -> Self {
        Self {
            requests: Mutex::new(vec![]),
            images: Mutex::new(vec![]),
            contexts: Mutex::new(vec![]),
            interruptions: AtomicUsize::new(0),
            cleaned: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            image_arrived: Notify::new(),
            request_arrived: Notify::new(),
            hold_next_image: AtomicBool::new(false),
            image_gate: Semaphore::new(0),
            held_image_fence: Mutex::new(None),
            observe_held_image_close: AtomicBool::new(false),
            held_image_constraint: Mutex::new(None),
            hold_next_context: AtomicBool::new(false),
            context_arrived: Notify::new(),
            context_gate: Semaphore::new(0),
            held_images: AtomicUsize::new(0),
            cleanup_arrived: Notify::new(),
            end_arrived: Notify::new(),
            assistant_image_calls: AtomicUsize::new(0),
            assistant_image_arrived: Notify::new(),
            probe_after_response: Mutex::new(None),
            probe_fence: Mutex::new(None),
        }
    }
}
#[derive(Clone, Copy)]
pub enum InputPolicy {
    RequestImage,
    OrdinaryText,
    PassiveImage,
}
impl InputPolicy {
    fn requests_image(self) -> bool {
        match self {
            Self::RequestImage => true,
            Self::PassiveImage => true,
            Self::OrdinaryText => false,
        }
    }
}
pub struct ImageBot {
    pub endpoint: String,
    pub trace: Arc<Trace>,
    pub policy: InputPolicy,
}
impl ImageBot {
    pub fn new(endpoint: String) -> Result<Self, BotHostError> {
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
            return Err(BotHostError::Invalid(
                "fixture needs literal loopback HTTP without credentials/query/fragment".into(),
            ));
        }
        Ok(Self {
            endpoint,
            trace: Arc::new(Trace::default()),
            policy: InputPolicy::RequestImage,
        })
    }
}
struct HeldImageCall<'a>(&'a AtomicUsize);
impl Drop for HeldImageCall<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct RequestImageForUserInput {
    trace: Arc<Trace>,
    policy: InputPolicy,
}
impl Drop for RequestImageForUserInput {
    fn drop(&mut self) {
        self.trace.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for RequestImageForUserInput {
    fn name(&self) -> &str {
        "request-image-for-real-rtvi-user-input"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Interruption) {
            self.trace.interruptions.fetch_add(1, Ordering::SeqCst);
        }
        if direction == Direction::Downstream {
            if let FrameData::LlmMessagesAppend(messages) = &frame.data {
                if self.policy.requests_image() {
                    if messages.messages.len() != 1
                        || messages.messages[0].role != ChatRole::User
                        || !messages.run_llm
                    {
                        return Err(ProcessorError::fatal(
                            "fixture expected one real immediate RTVI user append",
                        ));
                    }
                    let text = messages.messages[0].content.clone();
                    let request = Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
                        user_id: "synthetic-eval-user".into(),
                        text: Some(text.clone()),
                        append_to_context: Some(matches!(self.policy, InputPolicy::RequestImage)),
                        video_source: Some("camera".into()),
                        function_name: None,
                        tool_call_id: None,
                        result_callback: None,
                    }));
                    self.trace.requests.lock().unwrap().push((request.id, text));
                    self.trace.request_arrived.notify_one();
                    // Do not forward this append or inject context: the returned
                    // image is the sole user message and inference cause.
                    return ctx.push(request, Direction::Upstream).await;
                }
            }
            if let FrameData::UserImage(image) = &frame.data {
                let constraint = frame
                    .native_user_image_constraint(direction)?
                    .ok_or_else(|| ProcessorError::new("expected real eval-owned image carrier"))?;
                let request_id = constraint.request_id();
                if self.trace.probe_after_response.lock().unwrap().is_some()
                    && self.trace.probe_fence.lock().unwrap().is_none()
                {
                    *self.trace.probe_fence.lock().unwrap() =
                        Some(constraint.intersect_invocation(&ctx.invocation_fence()?)?);
                }
                self.trace.images.lock().unwrap().push(ObservedImage {
                    id: frame.id,
                    request_id,
                    rgb: image.image.image.clone(),
                    size: image.image.size,
                    format: image.image.format.clone(),
                    user_id: image.user_id.clone(),
                    text: image.text.clone(),
                    append_to_context: image.append_to_context,
                    request: image.request.as_deref().cloned(),
                    source: frame.transport_source.clone(),
                    destination: frame.transport_destination.clone(),
                });
                let held = self.trace.hold_next_image.swap(false, Ordering::SeqCst);
                if held {
                    *self.trace.held_image_fence.lock().unwrap() =
                        Some(constraint.intersect_invocation(&ctx.invocation_fence()?)?);
                    if self
                        .trace
                        .observe_held_image_close
                        .swap(false, Ordering::SeqCst)
                    {
                        *self.trace.held_image_constraint.lock().unwrap() =
                            Some(constraint.clone());
                    }
                }
                let _held = held.then(|| {
                    self.trace.held_images.fetch_add(1, Ordering::SeqCst);
                    HeldImageCall(&self.trace.held_images)
                });
                self.trace.image_arrived.notify_one();
                if held {
                    // A real already-emitted System image is retained at an
                    // application processor, not fabricated after interruption.
                    self.trace
                        .image_gate
                        .acquire()
                        .await
                        .map_err(|e| ProcessorError::new(e.to_string()))?
                        .forget();
                }
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.trace.end_arrived.notify_one();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.trace.cleaned.fetch_add(1, Ordering::SeqCst);
        self.trace.cleanup_arrived.notify_one();
        Ok(())
    }
}
// Diagnostic wrapper only: all configuration, input handling, history admission
// and lifecycle calls delegate to the unchanged standard assistant implementation.
// A witness means process() returned, never a synthetic visual-success event.
struct StandardAssistantWitness {
    inner: Arc<dyn FrameProcessor>,
    trace: Arc<Trace>,
}
#[async_trait]
impl FrameProcessor for StandardAssistantWitness {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let image = matches!(frame.data, FrameData::UserImage(_));
        let response_ended = matches!(frame.data, FrameData::LlmResponseEnd);
        self.inner.process(frame, direction, ctx).await?;
        if image {
            self.trace
                .assistant_image_calls
                .fetch_add(1, Ordering::SeqCst);
            self.trace.assistant_image_arrived.notify_one();
        }
        if response_ended {
            let delay = self.trace.probe_after_response.lock().unwrap().take();
            if let Some(delay) = delay {
                let fence = self
                    .trace
                    .probe_fence
                    .lock()
                    .unwrap()
                    .take()
                    .ok_or_else(|| {
                        ProcessorError::new("timing probe has no genuine image cause")
                    })?;
                let owned = ctx.clone();
                let trace = self.trace.clone();
                ctx.spawn(
                    "passive-image-probe-during-source-send-after",
                    true,
                    async move {
                        tokio::time::sleep(delay).await;
                        let request =
                            Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
                                user_id: "synthetic-eval-user".into(),
                                text: Some("passive timing probe".into()),
                                append_to_context: Some(false),
                                video_source: Some("camera".into()),
                                function_name: None,
                                tool_call_id: None,
                                result_callback: None,
                            }));
                        trace
                            .requests
                            .lock()
                            .unwrap()
                            .push((request.id, "passive timing probe".into()));
                        owned.try_push_with_invocation_fence(request, Direction::Upstream, &fence)
                    },
                )?;
            }
        }
        Ok(())
    }
}

// The host has already performed its boundary admission when this normal
// invocation is held. After release the exact native LlmProcessor receives the
// unchanged frame, so a host boundary recheck cannot masquerade as its own
// close/launch guard. All lifecycle behavior delegates unchanged.
struct BeforeNativeHttp {
    inner: Arc<dyn FrameProcessor>,
    trace: Arc<Trace>,
}
#[async_trait]
impl FrameProcessor for BeforeNativeHttp {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Upstream
            && matches!(frame.data, FrameData::LlmContextDetailed(_))
        {
            frame.native_user_image_context(direction)?;
            if self.trace.hold_next_context.swap(false, Ordering::SeqCst) {
                self.trace.context_arrived.notify_one();
                self.trace
                    .context_gate
                    .acquire()
                    .await
                    .map_err(|e| ProcessorError::new(e.to_string()))?
                    .forget();
            }
        }
        self.inner.process(frame, direction, ctx).await
    }
}
#[async_trait]
impl EvalBotFactory for ImageBot {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if !settings.skip_tts {
            return Err(BotHostError::Invalid(
                "image fixture is text output only".into(),
            ));
        }
        let mut config = ClientConfig::new(Provider::OpenAi, "synthetic-local-image-key");
        config.endpoint = self.endpoint.clone();
        config.request_timeout = Duration::from_secs(4);
        config.connect_timeout = Duration::from_secs(1);
        config.idle_timeout = Duration::from_secs(3);
        let http = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .connect_timeout(config.connect_timeout)
            .build()
            .map_err(|e| BotHostError::Invalid(e.to_string()))?;
        let client = LlmClient::with_http_client(config, http)
            .map_err(|e| BotHostError::Invalid(e.to_string()))?;
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            Default::default(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Text,
            },
        );
        self.trace.contexts.lock().unwrap().push(pair.context());
        Ok(EvalBotPipeline {
            context: pair.context(),
            processors: vec![
                Arc::new(RequestImageForUserInput {
                    trace: self.trace.clone(),
                    policy: self.policy,
                }),
                pair.user(),
                Arc::new(BeforeNativeHttp {
                    inner: Arc::new(LlmProcessor::new(
                        client,
                        CompletionRequest::new("synthetic-vision", Context::default()),
                    )),
                    trace: self.trace.clone(),
                }),
            ],
            assistant: Arc::new(StandardAssistantWitness {
                inner: pair.assistant(),
                trace: self.trace.clone(),
            }),
            introduction: None,
        })
    }
}
