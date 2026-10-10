use crate::budget::Budget;
use crate::{
    CancellationToken, GeminiImageClient, GeminiImageSettings, GeminiImageSettingsDelta,
    ImageError, Result,
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

struct State {
    settings: GeminiImageSettings,
    tail: Option<oneshot::Receiver<()>>,
    started: bool,
    ending: bool,
}
/// Ordered, bounded native image service. Normal frames retain their source
/// identity and direction; Text is forwarded before its generated image. System
/// controls remain responsive while accepted normal work runs in tracked tasks.
/// End/Stop drain work, Cancel aborts it, and interruption cancels interruptible work.
pub struct GeminiImageGenProcessor {
    client: GeminiImageClient,
    state: Mutex<State>,
    permits: Arc<Semaphore>,
    health: ProcessorHealth,
}
impl GeminiImageGenProcessor {
    pub fn new(client: GeminiImageClient) -> Self {
        Self {
            state: Mutex::new(State {
                settings: client.config().settings.clone(),
                tail: None,
                started: false,
                ending: false,
            }),
            permits: Arc::new(Semaphore::new(client.config().limits.image.max_pending)),
            client,
            health: Default::default(),
        }
    }
    pub fn settings(&self) -> Result<GeminiImageSettings> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ImageError::Closed)?
            .settings
            .clone())
    }
    pub fn update_settings(&self, delta: &GeminiImageSettingsDelta) -> Result<()> {
        let mut state = self.state.lock().map_err(|_| ImageError::Closed)?;
        if state.ending {
            return Err(ImageError::Closed);
        }
        let settings = state.settings.apply(delta)?;
        if settings.number_of_images > self.client.config().limits.max_images {
            return Err(ImageError::TooLarge);
        }
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
impl FrameProcessor for GeminiImageGenProcessor {
    fn name(&self) -> &str {
        "GoogleImageGenService"
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
                let delta = settings_delta(update).map_err(ImageError::processor)?;
                return self.update_settings(&delta).map_err(ImageError::processor);
            }
        }
        if (frame.is_system() && prompt(&frame.data).is_none()) || frame.is_terminal() {
            return ctx.push(frame, direction).await;
        }
        let incoming_prompt = prompt(&frame.data);
        if incoming_prompt
            .is_some_and(|s| s.len() > self.client.config().limits.image.max_prompt_bytes)
        {
            return Err(ImageError::TooLarge.processor());
        }
        // Byte-count serialization writes to a bounded sink, never a JSON buffer.
        // Local executable payloads cannot be safely budgeted and are rejected.
        let mut budget = Budget::new(self.client.config().limits.image.max_response_bytes);
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
        ctx.spawn("gemini-image-generation", interruptible, async move {
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
            let mut images = client
                .generate_with_settings(&prompt, &settings, &cancellation)
                .map_err(ImageError::processor)?;
            loop {
                if !output.is_current() || !health.is_usable() || health.generation() != generation
                {
                    return Ok(());
                }
                let result = tokio::select! { biased;
                    _ = changed.changed() => return Ok(()),
                    result = images.next_image() => result,
                };
                if !output.is_current() || !health.is_usable() || health.generation() != generation
                {
                    return Ok(());
                }
                match result {
                    Ok(Some(image)) => {
                        let mut frame = Frame::new(FrameData::UrlImage(UrlImageRawFrame {
                            image: OutputImageRawFrame {
                                image: image.image,
                                sync_with_audio: false,
                            },
                            url: image.url,
                        }));
                        frame.interruptible = interruptible;
                        frame.transport_source = source.clone();
                        frame.transport_destination = destination.clone();
                        frame
                            .metadata
                            .insert("image_generation.source_frame_id".into(), source_id.into());
                        tokio::select! { biased;
                            _ = changed.changed() => return Ok(()),
                            result = output.push(frame, Direction::Downstream) => result?,
                        }
                    }
                    Ok(None) => break,
                    Err(ImageError::Cancelled) => return Ok(()),
                    Err(error) => {
                        // Provider HTTP errors retain their standard permanent categories.
                        // Malformed content and local limits remain recoverable.
                        let category =
                            if error.is_permanent() || matches!(error, ImageError::Http(_)) {
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
                        break;
                    }
                }
            }
            if output.configuration().enable_metrics && health.generation() == generation {
                output
                    .push(
                        Frame::new(FrameData::Metrics(vec![Metric::Processing {
                            processor: "GoogleImageGenService".into(),
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
use crate::processor::prompt;

fn settings_delta(
    update: &pipecat_core::ServiceUpdateSettingsFrame<pipecat_core::ServiceSettings>,
) -> Result<GeminiImageSettingsDelta> {
    let values = update
        .delta
        .as_ref()
        .map_or(&update.settings, |typed| &typed.extra);
    if let Some(typed) = &update.delta {
        crate::settings::validate_setting(&typed.model, 256)?;
    }
    // Validate every borrowed field before retaining or cloning any update.
    for (key, value) in values {
        match (key.as_str(), value) {
            ("model", serde_json::Value::String(s)) if s.len() <= 256 => (),
            ("aspect_ratio", serde_json::Value::String(s)) if s.len() <= 16 => (),
            ("negative_prompt", serde_json::Value::String(s)) if s.len() <= 65536 => (),
            ("number_of_images", serde_json::Value::Number(n))
                if n.as_u64().is_some_and(|n| (1..=8).contains(&n)) => {}
            (
                "model" | "aspect_ratio" | "negative_prompt" | "number_of_images",
                serde_json::Value::Null,
            ) => (),
            _ => {
                return Err(ImageError::Invalid(
                    "unsupported Gemini image setting or value",
                ))
            }
        }
    }
    let mut delta = GeminiImageSettingsDelta::default();
    if let Some(typed) = &update.delta {
        delta.model = typed.model.clone();
    }
    for (key, value) in values {
        if key == "number_of_images" {
            delta.number_of_images = if value.is_null() {
                Setting::Null
            } else {
                Setting::Value(value.as_u64().unwrap() as u8)
            };
            continue;
        }
        let setting = if let serde_json::Value::String(value) = value {
            Setting::Value(value.clone())
        } else {
            Setting::Null
        };
        match key.as_str() {
            "model" => delta.model = setting,
            "aspect_ratio" => delta.aspect_ratio = setting,
            "negative_prompt" => delta.negative_prompt = setting,
            _ => unreachable!("borrowed setting validation succeeded"),
        }
    }
    Ok(delta)
}
