use super::{
    settings::{GeminiImageConfig, GeminiImageSettings},
    wire,
};
use crate::{
    client::{completion_fence, read_body},
    decode::{CancelOnDrop, DecodeControl, DecoderWorker},
    security::{parse_url, valid_scheme},
    CancellationToken, GeneratedImage, ImageError, Result,
};
use base64::Engine;
use reqwest::{header::HeaderValue, Client};
use serde::Serialize;
use std::{sync::Arc, time::Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

struct Inner {
    config: GeminiImageConfig,
    base: Url,
    http: Client,
    decoder: DecoderWorker,
    permits: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct GeminiImageClient(Arc<Inner>);
impl std::fmt::Debug for GeminiImageClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("GeminiImageClient")
            .field(&self.0.config)
            .finish()
    }
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Request<'a> {
    contents: [Content<'a>; 1],
    generation_config: GenerationConfig<'a>,
}
#[derive(Serialize)]
struct Content<'a> {
    role: &'static str,
    parts: [Text<'a>; 1],
}
#[derive(Serialize)]
struct Text<'a> {
    text: &'a str,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GenerationConfig<'a> {
    response_modalities: [&'static str; 1],
    image_config: ImageConfig<'a>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ImageConfig<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    aspect_ratio: Option<&'a str>,
}
impl GeminiImageClient {
    pub fn new(config: GeminiImageConfig) -> Result<Self> {
        config.settings.validate()?;
        config.limits.validate()?;
        if config.settings.number_of_images > config.limits.max_images {
            return Err(ImageError::TooLarge);
        }
        if config.api_key.is_empty()
            || config.api_key.len() > 4096
            || HeaderValue::from_str(&config.api_key).is_err()
        {
            return Err(ImageError::Invalid("invalid API credential"));
        }
        let base = parse_url(&config.base_url)?;
        if !valid_scheme(&base, config.allow_loopback_api) || base.query().is_some() {
            return Err(ImageError::AddressDenied);
        }
        let http = Client::builder()
            .retry(reqwest::retry::never())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ImageError::Transport)?;
        let decoder = DecoderWorker::new(config.limits.image.clone())?;
        let permits = Arc::new(Semaphore::new(config.limits.image.max_pending));
        Ok(Self(Arc::new(Inner {
            config,
            base,
            http,
            decoder,
            permits,
        })))
    }
    pub fn config(&self) -> &GeminiImageConfig {
        &self.0.config
    }
    /// Admit a lazy sequence. Each next_image call makes at most one billable
    /// request, only after the previous result has been returned to the caller.
    pub fn generate(
        &self,
        prompt: &str,
        cancellation: &CancellationToken,
    ) -> Result<GeminiImageGeneration> {
        self.generate_with_settings(prompt, &self.0.config.settings, cancellation)
    }
    pub fn generate_with_settings(
        &self,
        prompt: &str,
        settings: &GeminiImageSettings,
        cancellation: &CancellationToken,
    ) -> Result<GeminiImageGeneration> {
        settings.validate()?;
        let limits = &self.0.config.limits;
        if prompt.len() > limits.image.max_prompt_bytes
            || settings.number_of_images > limits.max_images
        {
            return Err(ImageError::TooLarge);
        }
        if cancellation.is_cancelled() {
            return Err(ImageError::Cancelled);
        }
        let permit = self
            .0
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| ImageError::Backpressure)?;
        Ok(GeminiImageGeneration {
            client: self.clone(),
            prompt: prompt.to_owned(),
            settings: settings.clone(),
            cancellation: cancellation.clone(),
            permit: Some(Arc::new(permit)),
            deadline: Instant::now() + limits.generation_timeout,
            delivered: 0,
            response_bytes: 0,
            encoded_bytes: 0,
            output_bytes: 0,
            in_flight: false,
            finished: false,
        })
    }
}
/// Pull-based, sequential image generation with exact partial delivery. An error
/// is returned once and fuses the sequence; earlier returned images stay valid.
/// Dropping a pending next_image future permanently cancels this sequence: an
/// ambiguously submitted generation is never automatically replayed.
pub struct GeminiImageGeneration {
    client: GeminiImageClient,
    prompt: String,
    settings: GeminiImageSettings,
    cancellation: CancellationToken,
    permit: Option<Arc<OwnedSemaphorePermit>>,
    deadline: Instant,
    delivered: u8,
    response_bytes: usize,
    encoded_bytes: usize,
    output_bytes: usize,
    in_flight: bool,
    finished: bool,
}
impl std::fmt::Debug for GeminiImageGeneration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeminiImageGeneration")
            .field("delivered", &self.delivered)
            .field("finished", &self.finished)
            .finish()
    }
}
impl GeminiImageGeneration {
    pub fn delivered(&self) -> u8 {
        self.delivered
    }
    pub async fn next_image(&mut self) -> Result<Option<GeneratedImage>> {
        if self.finished {
            return Ok(None);
        }
        // An unfinished previous future means submission may already have happened.
        if self.in_flight {
            self.finish();
            return Err(ImageError::Cancelled);
        }
        self.in_flight = true;
        let deadline = self
            .deadline
            .min(Instant::now() + self.client.0.config.limits.image.request_timeout);
        let cancel = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancel.clone());
        let control = DecodeControl {
            cancel,
            caller: self.cancellation.clone(),
            deadline,
        };
        let caller = self.cancellation.clone();
        let result = if let Err(error) = completion_fence(Ok(()), &self.cancellation, deadline) {
            Err(error)
        } else {
            tokio::select! { biased;
                _ = caller.cancelled() => Err(ImageError::Cancelled),
                result = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), self.request(control)) => {
                    result.unwrap_or(Err(ImageError::Timeout))
                }
            }
        };
        let result = completion_fence(result, &self.cancellation, deadline);
        self.in_flight = false;
        match result {
            Ok(image) => {
                self.delivered += 1;
                if self.delivered == self.settings.number_of_images {
                    self.finish();
                }
                Ok(Some(image))
            }
            Err(error) => {
                self.finish();
                Err(error)
            }
        }
    }
    fn finish(&mut self) {
        self.finished = true;
        self.permit.take();
    }
    async fn request(&mut self, control: DecodeControl) -> Result<GeneratedImage> {
        let limits = &self.client.0.config.limits;
        let body_limit = limits
            .image
            .max_response_bytes
            .min(limits.max_total_response_bytes - self.response_bytes);
        let encoded_limit = limits
            .image
            .max_encoded_image_bytes
            .min(limits.max_total_encoded_bytes - self.encoded_bytes);
        let output_limit = limits.max_total_output_bytes - self.output_bytes;
        if body_limit == 0 || encoded_limit == 0 || output_limit < 3 {
            return Err(ImageError::TooLarge);
        }
        let endpoint = parse_url(&format!(
            "{}/models/{}:generateContent",
            self.client.0.base.as_str().trim_end_matches('/'),
            self.settings.model
        ))?;
        let request = Request {
            contents: [Content {
                role: "user",
                parts: [Text { text: &self.prompt }],
            }],
            generation_config: GenerationConfig {
                response_modalities: ["IMAGE"],
                image_config: ImageConfig {
                    aspect_ratio: self.settings.aspect_ratio.as_deref(),
                },
            },
        };
        let response = self
            .client
            .0
            .http
            .post(endpoint)
            .header("x-goog-api-key", &self.client.0.config.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|_| ImageError::Transport)?;
        let body = read_body(response, body_limit).await?;
        self.response_bytes += body.len();
        let inline = wire::first_image(&body, limits)?;
        let mime = inline.mime_type.ok_or(ImageError::Protocol)?;
        if !matches!(mime.as_str(), "image/png" | "image/jpeg") {
            return Err(ImageError::Decode);
        }
        let b64 = inline.data.ok_or(ImageError::NoImage)?;
        let padding = b64
            .as_bytes()
            .iter()
            .rev()
            .take_while(|&&b| b == b'=')
            .count();
        if b64.len() % 4 != 0 || padding > 2 {
            return Err(ImageError::Protocol);
        }
        let predicted = (b64.len() / 4)
            .checked_mul(3)
            .and_then(|n| n.checked_sub(padding))
            .ok_or(ImageError::Protocol)?;
        if predicted > encoded_limit {
            return Err(ImageError::TooLarge);
        }
        let mut bytes = vec![0; predicted];
        let count = base64::engine::general_purpose::STANDARD
            .decode_slice(&b64, &mut bytes)
            .map_err(|_| ImageError::Protocol)?;
        if count != predicted {
            return Err(ImageError::Protocol);
        }
        if (mime == "image/png" && !bytes.starts_with(b"\x89PNG\r\n\x1a\n"))
            || (mime == "image/jpeg" && !bytes.starts_with(&[0xff, 0xd8]))
        {
            return Err(ImageError::Decode);
        }
        self.encoded_bytes += bytes.len();
        drop(b64);
        drop(body);
        let image = self
            .client
            .0
            .decoder
            .decode_shared(
                bytes,
                control,
                self.permit.as_ref().ok_or(ImageError::Closed)?.clone(),
                output_limit,
            )
            .await?;
        self.output_bytes += image.image.len();
        Ok(GeneratedImage { image, url: None })
    }
}
