use crate::budget::Budget;
use crate::{
    AzureImageClient, CancellationToken, GeneratedImage, ImageError, ImageGenerationLimits,
    ImageGenerationSettings, ImageGenerationSettingsDelta, OpenAiImageClient, Result,
};
use pipecat_core::{
    async_trait, Direction, ErrorFrame, Frame, FrameData, FrameProcessor, Metric,
    OutputImageRawFrame, ProcessorContext, ProcessorError, ProcessorHealth, ProcessorReference,
    Setting, StartFrame, UrlImageRawFrame,
};
use serde::Serialize;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::sync::{oneshot, Semaphore};

#[derive(Clone)]
enum ImageBackend {
    OpenAi(OpenAiImageClient),
    Azure(AzureImageClient),
}
impl ImageBackend {
    fn settings(&self) -> &ImageGenerationSettings {
        match self {
            Self::OpenAi(client) => &client.config().settings,
            Self::Azure(client) => &client.config().settings,
        }
    }
    fn limits(&self) -> &ImageGenerationLimits {
        match self {
            Self::OpenAi(client) => &client.config().limits,
            Self::Azure(client) => &client.config().limits,
        }
    }
    fn name(&self) -> &'static str {
        match self {
            Self::OpenAi(_) => "OpenAIImageGenService",
            Self::Azure(_) => "AzureImageGenServiceREST",
        }
    }
    fn task_name(&self) -> &'static str {
        match self {
            Self::OpenAi(_) => "openai-image-generation",
            Self::Azure(_) => "azure-image-generation",
        }
    }
    async fn generate(
        &self,
        prompt: &str,
        settings: &ImageGenerationSettings,
        cancellation: &CancellationToken,
    ) -> Result<GeneratedImage> {
        match self {
            Self::OpenAi(client) => {
                client
                    .generate_with_settings(prompt, settings, cancellation)
                    .await
            }
            Self::Azure(client) => {
                client
                    .generate_with_settings(prompt, settings, cancellation)
                    .await
            }
        }
    }
}

struct State {
    settings: ImageGenerationSettings,
    tail: Option<oneshot::Receiver<()>>,
    started: bool,
    ending: bool,
}
/// Ordered, bounded native image service. Normal frames retain their source
/// identity and direction; Text is forwarded before its generated image. System
/// controls remain responsive while accepted normal work runs in tracked tasks.
/// End/Stop drain work, Cancel aborts it, and interruption cancels interruptible work.
pub struct ImageGenProcessor {
    client: ImageBackend,
    state: Mutex<State>,
    permits: Arc<Semaphore>,
    health: ProcessorHealth,
}
impl ImageGenProcessor {
    pub fn new(client: OpenAiImageClient) -> Self {
        Self::with_backend(ImageBackend::OpenAi(client))
    }
    /// Use the Azure submit/poll/download protocol with the same ordered native
    /// frame and lifecycle handling as the OpenAI image service.
    pub fn azure(client: AzureImageClient) -> Self {
        Self::with_backend(ImageBackend::Azure(client))
    }
    fn with_backend(client: ImageBackend) -> Self {
        Self {
            state: Mutex::new(State {
                settings: client.settings().clone(),
                tail: None,
                started: false,
                ending: false,
            }),
            permits: Arc::new(Semaphore::new(client.limits().max_pending)),
            client,
            health: Default::default(),
        }
    }
    pub fn settings(&self) -> Result<ImageGenerationSettings> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ImageError::Closed)?
            .settings
            .clone())
    }
    pub fn update_settings(&self, delta: &ImageGenerationSettingsDelta) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ImageError::Closed)?;
        if state.ending {
            return Err(ImageError::Closed);
        }
        let settings = state.settings.apply(delta)?;
        if settings != state.settings {
            state.settings = settings;
            if !self.health.is_usable() {
                self.health.set_usable(true);
            }
        }
        Ok(())
    }
    fn targets(&self, target: &Option<ProcessorReference>) -> bool {
        match target {
            None => true,
            Some(ProcessorReference::Name(name)) => name == self.name(),
            Some(ProcessorReference::Local(target)) => {
                std::ptr::addr_eq(target.0.as_ref(), self as &dyn FrameProcessor)
            }
        }
    }
}
#[async_trait]
impl FrameProcessor for ImageGenProcessor {
    fn name(&self) -> &str {
        self.client.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(
        &self,
        _: &StartFrame,
        _: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageError::Closed.processor())?;
        if state.started || state.ending {
            return Err(ImageError::Closed.processor());
        }
        state.started = true;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if let FrameData::ServiceUpdateSettings(update) = &frame.data {
            if self.targets(&update.service) {
                let values = update
                    .delta
                    .as_ref()
                    .map_or(&update.settings, |typed| &typed.extra);
                if let Some(typed) = &update.delta {
                    crate::settings::validate_setting(&typed.model, 1024)
                        .map_err(ImageError::processor)?;
                }
                // Validate borrowed values completely before cloning any update.
                // Unknown/deep structured values never enter a recursive clone.
                for (key, value) in values {
                    let maximum = match key.as_str() {
                        "model" => 1024,
                        "image_size" => 128,
                        _ => {
                            return Err(ImageError::Invalid("unsupported image setting").processor())
                        }
                    };
                    match value {
                        serde_json::Value::Null => (),
                        serde_json::Value::String(s) if s.len() <= maximum => (),
                        _ => {
                            return Err(ImageError::Invalid(
                                "settings require bounded strings or null",
                            )
                            .processor())
                        }
                    }
                }
                let mut delta = ImageGenerationSettingsDelta::default();
                if let Some(typed) = &update.delta {
                    delta.model = typed.model.clone();
                }
                for (key, value) in values {
                    let setting = match value {
                        serde_json::Value::Null => Setting::Null,
                        serde_json::Value::String(s) => Setting::Value(s.clone()),
                        _ => unreachable!("borrowed setting validation succeeded"),
                    };
                    match key.as_str() {
                        "model" => delta.model = setting,
                        "image_size" => delta.image_size = setting,
                        _ => unreachable!("borrowed setting key validation succeeded"),
                    }
                }
                return self.update_settings(&delta).map_err(ImageError::processor);
            }
        }
        if (frame.is_system() && prompt(&frame.data).is_none()) || frame.is_terminal() {
            return ctx.push(frame, direction).await;
        }
        let incoming_prompt = prompt(&frame.data);
        if incoming_prompt.is_some_and(|s| s.len() > self.client.limits().max_prompt_bytes) {
            return Err(ImageError::TooLarge.processor());
        }
        // Byte-count serialization writes to a bounded sink, never a JSON buffer.
        // Local executable payloads cannot be safely budgeted and are rejected.
        let mut budget = Budget::new(self.client.limits().max_response_bytes);
        frame
            .data
            .serialize(&mut budget)
            .map_err(|_| ImageError::TooLarge.processor())?;
        frame
            .metadata
            .serialize(&mut budget)
            .map_err(|_| ImageError::TooLarge.processor())?;
        let envelope = frame
            .name
            .len()
            .saturating_add(frame.transport_source.as_ref().map_or(0, String::len))
            .saturating_add(frame.transport_destination.as_ref().map_or(0, String::len))
            .saturating_add(frame.native_llm_continuation_retained_bytes());
        if envelope > budget.remaining {
            return Err(ImageError::TooLarge.processor());
        }
        // Clone after bounded-depth validation to shed attacker-supplied spare
        // String/Vec capacity while preserving the frame's logical identity.
        let compact = frame.clone();
        drop(frame);
        let frame = compact;
        let permit = self
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| ImageError::Backpressure.processor())?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageError::Closed.processor())?;
        if state.ending || !state.started {
            return Err(ImageError::Closed.processor());
        }
        let previous = state.tail.take();
        let (done, next) = oneshot::channel();
        state.tail = Some(next);
        let settings = state.settings.clone();
        let client = self.client.clone();
        let output = ctx.clone();
        let health = self.health.clone();
        let generation = health.generation();
        let mut changed = health.watch();
        let interruptible = frame.interruptible;
        ctx.spawn(client.task_name(), interruptible, async move {
            let _permit = permit;
            // Sender drop releases the next job, including cancellation/panic paths.
            let _done = done;
            if let Some(previous) = previous {
                let _ = previous.await;
            }
            let text = prompt(&frame.data).map(str::to_owned);
            let source_id = frame.id;
            let source = frame.transport_source.clone();
            let destination = frame.transport_destination.clone();
            output.push(frame, direction).await?;
            let Some(prompt) = text else {
                return Ok(());
            };
            changed.borrow_and_update();
            if !output.is_current() || !health.is_usable() || health.generation() != generation {
                return Ok(());
            }
            let cancellation = CancellationToken::new();
            let started = Instant::now();
            let result = tokio::select! { biased;
                _ = changed.changed() => return Ok(()),
                result = client.generate(&prompt, &settings, &cancellation) => result,
            };
            if !output.is_current() || !health.is_usable() || health.generation() != generation {
                return Ok(());
            }
            match result {
                Ok(image) => {
                    let mut frame = Frame::new(FrameData::UrlImage(UrlImageRawFrame {
                        image: OutputImageRawFrame {
                            image: image.image,
                            sync_with_audio: false,
                        },
                        url: image.url,
                    }));
                    frame.interruptible = interruptible;
                    frame.transport_source = source;
                    frame.transport_destination = destination;
                    frame
                        .metadata
                        .insert("image_generation.source_frame_id".into(), source_id.into());
                    output.push(frame, Direction::Downstream).await?;
                }
                Err(ImageError::Cancelled) => return Ok(()),
                Err(error) => {
                    // Provider HTTP errors retain their standard permanent categories.
                    // Malformed content and local limits remain recoverable.
                    let category = if error.is_permanent() || matches!(error, ImageError::Http(_)) {
                        error.category()
                    } else {
                        pipecat_core::ErrorCategory::Application
                    };
                    output
                        .push_error_if_health_generation(
                            ErrorFrame {
                                category,
                                ..ErrorFrame::new(error.to_string())
                            },
                            error.is_permanent(),
                            generation,
                        )
                        .await?;
                }
            }
            if output.configuration().enable_metrics && health.generation() == generation {
                output
                    .push(
                        Frame::new(FrameData::Metrics(vec![Metric::Processing {
                            processor: client.name().into(),
                            seconds: started.elapsed().as_secs_f64(),
                        }])),
                        Direction::Downstream,
                    )
                    .await?;
            }
            Ok(())
        })?;
        Ok(())
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> std::result::Result<(), ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ImageError::Closed.processor())?
            .ending = true;
        Ok(())
    }
    async fn cleanup(&self) -> std::result::Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ImageError::Closed.processor())?;
        state.ending = true;
        state.tail.take();
        Ok(())
    }
}
pub(crate) fn prompt(data: &FrameData) -> Option<&str> {
    match data {
        FrameData::Text(t)
        | FrameData::LlmText(t)
        | FrameData::InputText(t)
        | FrameData::VisionText(t) => Some(&t.text),
        FrameData::AggregatedText(t) | FrameData::TtsText(t) => Some(&t.text.text),
        FrameData::Transcription(t)
        | FrameData::InterimTranscription(t)
        | FrameData::EagerTranscription(t)
        | FrameData::Translation(t) => Some(&t.text),
        FrameData::TranscriptionDetailed(t)
        | FrameData::InterimTranscriptionDetailed(t)
        | FrameData::EagerTranscriptionDetailed(t)
        | FrameData::TranslationDetailed(t) => Some(&t.text.text),
        _ => None,
    }
}
