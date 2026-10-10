use crate::{
    decode::{CancelOnDrop, DecodeControl, DecoderWorker},
    security::{parse_url, valid_scheme},
    CancellationToken, ImageError, ImageGenerationConfig, ImageGenerationSettings, Result,
};
use base64::Engine;
use futures_util::StreamExt;
use pipecat_core::ImageRawFrame;
use reqwest::{
    header::{HeaderValue, CONTENT_ENCODING},
    Client, Response,
};
use serde::Deserialize;
use std::{sync::Arc, time::Instant};
use tokio::sync::Semaphore;
use url::Url;

#[derive(Clone, PartialEq, Eq)]
pub struct GeneratedImage {
    pub image: ImageRawFrame,
    pub url: Option<String>,
}
impl std::fmt::Debug for GeneratedImage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeneratedImage")
            .field("size", &self.image.size)
            .field("pixels_and_url", &"[redacted]")
            .finish()
    }
}
struct Inner {
    config: ImageGenerationConfig,
    endpoint: Url,
    http: Client,
    decoder: DecoderWorker,
    permits: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct OpenAiImageClient(Arc<Inner>);
impl std::fmt::Debug for OpenAiImageClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OpenAiImageClient")
            .field(&self.0.config)
            .finish()
    }
}
#[derive(Deserialize)]
struct Reply {
    data: Option<FirstImage>,
}
struct FirstImage(Option<Datum>);
impl<'de> Deserialize<'de> for FirstImage {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = FirstImage;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a bounded image array")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let first = seq.next_element::<Datum>()?;
                let mut count = 0;
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {
                    count += 1;
                    if count >= 16 {
                        return Err(serde::de::Error::custom("image count limit"));
                    }
                }
                Ok(FirstImage(first))
            }
        }
        deserializer.deserialize_seq(Visitor)
    }
}
#[derive(Deserialize)]
struct Datum {
    url: Option<String>,
    b64_json: Option<String>,
}
impl OpenAiImageClient {
    pub fn new(config: ImageGenerationConfig) -> Result<Self> {
        config.settings.validate()?;
        config.limits.validate()?;
        if config.api_key.is_empty()
            || config.api_key.len() > 4096
            || HeaderValue::from_str(&format!("Bearer {}", config.api_key)).is_err()
        {
            return Err(ImageError::Invalid("invalid API credential"));
        }
        let base = parse_url(&config.base_url)?;
        if !valid_scheme(&base, config.allow_loopback_api) || base.query().is_some() {
            return Err(ImageError::AddressDenied);
        }
        let endpoint = parse_url(&format!(
            "{}/images/generations",
            base.as_str().trim_end_matches('/')
        ))?;
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
        let decoder = DecoderWorker::new(config.limits.clone())?;
        let permits = Arc::new(Semaphore::new(config.limits.max_pending));
        Ok(Self(Arc::new(Inner {
            config,
            endpoint,
            http,
            decoder,
            permits,
        })))
    }
    pub fn config(&self) -> &ImageGenerationConfig {
        &self.0.config
    }
    pub async fn generate(
        &self,
        prompt: &str,
        cancellation: &CancellationToken,
    ) -> Result<GeneratedImage> {
        self.generate_with_settings(prompt, &self.0.config.settings, cancellation)
            .await
    }
    /// Settings are borrowed for one request; processor admission owns its snapshot.
    /// Retries are never automatic, since ambiguous requests may incur charges.
    pub async fn generate_with_settings(
        &self,
        prompt: &str,
        settings: &ImageGenerationSettings,
        cancellation: &CancellationToken,
    ) -> Result<GeneratedImage> {
        settings.validate()?;
        if prompt.len() > self.0.config.limits.max_prompt_bytes {
            return Err(ImageError::TooLarge);
        }
        let permit = self
            .0
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| ImageError::Backpressure)?;
        let cancel = CancellationToken::new();
        let _cancel_on_drop = CancelOnDrop(cancel.clone());
        let deadline = Instant::now() + self.0.config.limits.request_timeout;
        let control = DecodeControl {
            cancel,
            caller: cancellation.clone(),
            deadline,
        };
        let result = tokio::select! { biased;
            _ = cancellation.cancelled() => Err(ImageError::Cancelled),
            result = tokio::time::timeout(self.0.config.limits.request_timeout, self.generate_inner(prompt, settings, control, permit)) => result.map_err(|_| ImageError::Timeout)?,
        };
        completion_fence(result, cancellation, deadline)
    }
    async fn generate_inner(
        &self,
        prompt: &str,
        settings: &ImageGenerationSettings,
        control: DecodeControl,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<GeneratedImage> {
        let response = self.0.http.post(self.0.endpoint.clone())
            .bearer_auth(&self.0.config.api_key)
            .json(&serde_json::json!({ "prompt": prompt, "model": settings.model, "n": 1, "size": settings.image_size }))
            .send().await.map_err(|_| ImageError::Transport)?;
        let body = read_body(response, self.0.config.limits.max_response_bytes).await?;
        let reply: Reply = serde_json::from_slice(&body).map_err(|_| ImageError::Protocol)?;
        let first = reply
            .data
            .and_then(|values| values.0)
            .ok_or(ImageError::NoImage)?;
        let Datum { url, b64_json } = first;
        let url = url.filter(|s| !s.is_empty());
        let bytes = if let Some(url) = &url {
            drop(b64_json);
            self.download(url).await?
        } else if let Some(b64) = b64_json.filter(|s| !s.is_empty()) {
            // Bound decoded storage before decoding or allocating output.
            let padding = b64
                .as_bytes()
                .iter()
                .rev()
                .take_while(|&&b| b == b'=')
                .count();
            if b64.len() % 4 != 0 || padding > 2 {
                return Err(ImageError::Protocol);
            }
            let predicted = b64
                .len()
                .checked_div(4)
                .and_then(|n| n.checked_mul(3))
                .and_then(|n| n.checked_sub(padding))
                .ok_or(ImageError::TooLarge)?;
            if predicted > self.0.config.limits.max_encoded_image_bytes {
                return Err(ImageError::TooLarge);
            }
            let mut bytes = vec![0; predicted];
            let count = base64::engine::general_purpose::STANDARD
                .decode_slice(&b64, &mut bytes)
                .map_err(|_| ImageError::Protocol)?;
            if count != predicted {
                return Err(ImageError::Protocol);
            }
            bytes
        } else {
            return Err(ImageError::NoImage);
        };
        // Drop response strings and JSON buffers before decoder scratch admission.
        drop(body);
        let image = self.0.decoder.decode(bytes, control, permit).await?;
        Ok(GeneratedImage { image, url })
    }
    async fn download(&self, value: &str) -> Result<Vec<u8>> {
        let url = parse_url(value)?;
        let addresses = self.0.config.hosted_images.addresses(&url).await?;
        // A fresh credential-free client pins the exact validated lookup result.
        // Disabling proxies and redirects prevents a second authority/resolution path.
        let client = Client::builder()
            .retry(reqwest::retry::never())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .redirect(reqwest::redirect::Policy::none())
            .resolve_to_addrs(url.host_str().ok_or(ImageError::AddressDenied)?, &addresses)
            .build()
            .map_err(|_| ImageError::Transport)?;
        let response = client
            .get(url)
            .send()
            .await
            .map_err(|_| ImageError::Transport)?;
        if !response
            .remote_addr()
            .is_some_and(|remote| addresses.contains(&remote))
        {
            return Err(ImageError::AddressDenied);
        }
        read_body(response, self.0.config.limits.max_encoded_image_bytes).await
    }
}
pub(crate) async fn read_body(response: Response, maximum: usize) -> Result<Vec<u8>> {
    if !response.status().is_success() {
        return Err(ImageError::Http(response.status().as_u16()));
    }
    if response
        .headers()
        .get(CONTENT_ENCODING)
        .is_some_and(|value| value.as_bytes() != b"identity")
    {
        return Err(ImageError::Protocol);
    }
    if response
        .content_length()
        .is_some_and(|size| size > maximum as u64)
    {
        return Err(ImageError::TooLarge);
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| ImageError::Transport)?;
        if chunk.len() > maximum.saturating_sub(bytes.len()) {
            return Err(ImageError::TooLarge);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(crate) fn completion_fence<T>(
    result: Result<T>,
    cancellation: &CancellationToken,
    deadline: Instant,
) -> Result<T> {
    if cancellation.is_cancelled() {
        Err(ImageError::Cancelled)
    } else if Instant::now() >= deadline {
        Err(ImageError::Timeout)
    } else {
        result
    }
}
#[cfg(test)]
mod completion_tests {
    use super::*;
    #[test]
    fn ready_result_cannot_escape_cancellation_or_deadline() {
        let token = CancellationToken::new();
        let result: Result<()> = {
            token.cancel();
            Ok(())
        };
        assert_eq!(
            completion_fence(
                result,
                &token,
                Instant::now() + std::time::Duration::from_secs(1)
            ),
            Err(ImageError::Cancelled)
        );
        assert_eq!(
            completion_fence(Ok(()), &CancellationToken::new(), Instant::now()),
            Err(ImageError::Timeout)
        );
    }
}
