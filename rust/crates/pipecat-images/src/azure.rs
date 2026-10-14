// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Azure's pinned submit/poll REST image protocol, with a bounded native safety profile.
use crate::{
    client::{completion_fence, download_hosted_image, read_body},
    decode::{CancelOnDrop, DecodeControl, DecoderWorker},
    security::{parse_url, valid_scheme},
    CancellationToken, GeneratedImage, HostedImagePolicy, ImageError, ImageGenerationLimits,
    ImageGenerationSettings, Result,
};
use reqwest::{
    header::{HeaderMap, HeaderValue, CONTENT_TYPE},
    Client, Response,
};
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use url::Url;

const MAX_POLLS: usize = 119;
const MAX_HEADER_BYTES: usize = 16 * 1024;
const MAX_HEADERS: usize = 64;

#[derive(Clone)]
pub struct AzureImageConfig {
    pub(crate) api_key: String,
    pub base_url: String,
    pub api_version: String,
    /// Model remains local settings metadata and is never sent in this protocol.
    pub settings: ImageGenerationSettings,
    pub limits: ImageGenerationLimits,
    pub hosted_images: HostedImagePolicy,
    /// Allows only literal loopback HTTP API endpoints for local fixtures.
    pub allow_loopback_api: bool,
    /// A sleep precedes every poll, including the first one. Nonzero, at most 60s.
    pub poll_interval: Duration,
    /// At most 119 polls, matching the pinned source's decrement-before-poll loop.
    pub max_polls: usize,
}
impl AzureImageConfig {
    pub fn new(api_key: impl Into<String>, endpoint: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: endpoint.into(),
            api_version: "2023-06-01-preview".into(),
            settings: ImageGenerationSettings {
                model: None,
                image_size: None,
            },
            limits: Default::default(),
            hosted_images: Default::default(),
            allow_loopback_api: false,
            poll_interval: Duration::from_secs(1),
            max_polls: MAX_POLLS,
        }
    }
}
impl std::fmt::Debug for AzureImageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureImageConfig")
            .field("credentials_endpoint_and_settings", &"[redacted]")
            .field("limits", &self.limits)
            .field("poll_interval", &self.poll_interval)
            .field("max_polls", &self.max_polls)
            .finish()
    }
}
struct Inner {
    config: AzureImageConfig,
    endpoint: Url,
    api_key: HeaderValue,
    http: Client,
    decoder: DecoderWorker,
    permits: Arc<Semaphore>,
}
#[derive(Clone)]
pub struct AzureImageClient(Arc<Inner>);
impl std::fmt::Debug for AzureImageClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("AzureImageClient")
            .field(&self.0.config)
            .finish()
    }
}
#[derive(Serialize)]
struct Submission<'a> {
    prompt: &'a str,
    n: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<&'a str>,
}
#[derive(Deserialize)]
struct PollStatus {
    status: String,
}
#[derive(Deserialize)]
struct PollSuccess {
    result: Option<ImageResult>,
}
#[derive(Deserialize)]
struct ImageResult {
    data: Option<FirstImage>,
}
#[derive(Deserialize)]
struct Datum {
    url: Option<String>,
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
                f.write_str("an image array")
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut seq: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let first = seq.next_element::<Datum>()?;
                // Only the first result is interpreted. Remaining values are scanned
                // without materializing them, inside the response byte/JSON depth bounds.
                while seq.next_element::<serde::de::IgnoredAny>()?.is_some() {}
                Ok(FirstImage(first))
            }
        }
        deserializer.deserialize_seq(Visitor)
    }
}
impl AzureImageClient {
    pub fn new(config: AzureImageConfig) -> Result<Self> {
        config.settings.validate()?;
        config.limits.validate()?;
        if config.api_key.is_empty() || config.api_key.len() > 4096 {
            return Err(ImageError::Invalid("invalid API credential"));
        }
        let mut api_key = HeaderValue::from_str(&config.api_key)
            .map_err(|_| ImageError::Invalid("invalid API credential"))?;
        api_key.set_sensitive(true);
        if config.api_version.is_empty() || config.api_version.len() > 128 {
            return Err(ImageError::Invalid("invalid API version"));
        }
        if config.max_polls == 0
            || config.max_polls > MAX_POLLS
            || config.poll_interval.is_zero()
            || config.poll_interval > Duration::from_secs(60)
        {
            return Err(ImageError::Invalid("invalid polling limit"));
        }
        let base = strict_url(&config.base_url)?;
        if !valid_scheme(&base, config.allow_loopback_api) || base.query().is_some() {
            return Err(ImageError::AddressDenied);
        }
        // Native profile: normalize the joining slash and encode the API-version
        // query value, rather than the source's raw string concatenation.
        let mut endpoint = strict_url(&format!(
            "{}/openai/images/generations:submit",
            base.as_str().trim_end_matches('/')
        ))?;
        endpoint
            .query_pairs_mut()
            .append_pair("api-version", &config.api_version);
        if endpoint.as_str().len() > 8192 {
            return Err(ImageError::TooLarge);
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
        let decoder = DecoderWorker::new(config.limits.clone())?;
        let permits = Arc::new(Semaphore::new(config.limits.max_pending));
        Ok(Self(Arc::new(Inner {
            config,
            endpoint,
            api_key,
            http,
            decoder,
            permits,
        })))
    }
    pub fn config(&self) -> &AzureImageConfig {
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
    /// No submit or poll request is automatically retried. Each call owns one
    /// total deadline, including admission, sleep, network and RGB decoding.
    pub async fn generate_with_settings(
        &self,
        prompt: &str,
        settings: &ImageGenerationSettings,
        cancellation: &CancellationToken,
    ) -> Result<GeneratedImage> {
        let deadline = Instant::now() + self.0.config.limits.request_timeout;
        completion_fence(Ok(()), cancellation, deadline)?;
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
        let control = DecodeControl {
            cancel,
            caller: cancellation.clone(),
            deadline,
        };
        let result = tokio::select! { biased;
            _ = cancellation.cancelled() => Err(ImageError::Cancelled),
            result = tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                self.generate_inner(prompt, settings, control, permit),
            ) => result.unwrap_or(Err(ImageError::Timeout)),
        };
        completion_fence(result, cancellation, deadline)
    }
    async fn generate_inner(
        &self,
        prompt: &str,
        settings: &ImageGenerationSettings,
        control: DecodeControl,
        permit: OwnedSemaphorePermit,
    ) -> Result<GeneratedImage> {
        let response = self
            .0
            .http
            .post(self.0.endpoint.clone())
            .header("api-key", self.0.api_key.clone())
            .json(&Submission {
                prompt,
                n: 1,
                size: settings.image_size.as_deref(),
            })
            .send()
            .await
            .map_err(|_| ImageError::Transport)?;
        check_response(&response)?;
        let operation = operation_location(response.headers(), &self.0.endpoint)?;
        // Submission bodies are deliberately ignored, but fully bounded and read.
        drop(read_body(response, self.0.config.limits.max_response_bytes).await?);
        for _ in 0..self.0.config.max_polls {
            tokio::time::sleep(self.0.config.poll_interval).await;
            // Recheck immediately before attaching credentials to any poll request.
            check_operation(&operation, &self.0.endpoint)?;
            let response = self
                .0
                .http
                .get(operation.clone())
                .header("api-key", self.0.api_key.clone())
                .header(CONTENT_TYPE, "application/json")
                .send()
                .await
                .map_err(|_| ImageError::Transport)?;
            check_response(&response)?;
            let body = read_body(response, self.0.config.limits.max_response_bytes).await?;
            let status: PollStatus =
                serde_json::from_slice(&body).map_err(|_| ImageError::Protocol)?;
            if status.status != "succeeded" {
                // Source also keeps polling for failed, canceled and unknown strings.
                continue;
            }
            let reply: PollSuccess =
                serde_json::from_slice(&body).map_err(|_| ImageError::Protocol)?;
            let url = reply
                .result
                .and_then(|result| result.data)
                .and_then(|data| data.0)
                .and_then(|datum| datum.url)
                .filter(|url| !url.is_empty())
                .ok_or(ImageError::NoImage)?;
            drop(body);
            let bytes = download_hosted_image(
                &url,
                &self.0.config.hosted_images,
                self.0.config.limits.max_encoded_image_bytes,
            )
            .await?;
            // Decoder jobs retain admission until native work exits, even on drop.
            let image = self.0.decoder.decode(bytes, control, permit).await?;
            return Ok(GeneratedImage {
                image,
                url: Some(url),
            });
        }
        Err(ImageError::Timeout)
    }
}
fn strict_url(value: &str) -> Result<Url> {
    let url = parse_url(value)?;
    // Require an explicit, nonempty authority and reject even empty userinfo.
    // URL's permissive special-scheme parser otherwise canonicalizes it away.
    let (_, rest) = value.split_once("://").ok_or(ImageError::AddressDenied)?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty()
        || authority.contains(['@', '\\'])
        || value
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
    {
        return Err(ImageError::AddressDenied);
    }
    Ok(url)
}

fn check_operation(operation: &Url, endpoint: &Url) -> Result<()> {
    if operation.origin() != endpoint.origin()
        || !operation.username().is_empty()
        || operation.password().is_some()
        || operation.fragment().is_some()
    {
        return Err(ImageError::AddressDenied);
    }
    Ok(())
}
fn operation_location(headers: &HeaderMap, endpoint: &Url) -> Result<Url> {
    let mut values = headers.get_all("operation-location").iter();
    let value = values.next().ok_or(ImageError::Protocol)?;
    if values.next().is_some() || value.is_empty() {
        return Err(ImageError::Protocol);
    }
    let value = value.to_str().map_err(|_| ImageError::Protocol)?;
    let operation = strict_url(value)?;
    check_operation(&operation, endpoint)?;
    Ok(operation)
}
fn check_response(response: &Response) -> Result<()> {
    if !response.status().is_success() {
        return Err(ImageError::Http(response.status().as_u16()));
    }
    let mut bytes = 0usize;
    if response.headers().len() > MAX_HEADERS {
        return Err(ImageError::TooLarge);
    }
    for (name, value) in response.headers() {
        bytes = bytes.saturating_add(name.as_str().len() + value.as_bytes().len() + 4);
        if bytes > MAX_HEADER_BYTES {
            return Err(ImageError::TooLarge);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_origin_is_canonical_and_every_credential_header_is_sensitive() {
        let client = AzureImageClient::new(AzureImageConfig::new(
            "synthetic-secret",
            "https://API.example:443/resource/",
        ))
        .unwrap();
        assert!(client.0.api_key.is_sensitive());
        let endpoint = &client.0.endpoint;
        assert!(check_operation(
            &strict_url("https://api.example/operation?signature=secret").unwrap(),
            endpoint,
        )
        .is_ok());
        for value in [
            "https://api.example:444/operation",
            "http://api.example/operation",
            "https://api.example.evil/operation",
            "https://evilapi.example/operation",
            "https://api.example./operation",
        ] {
            assert_eq!(
                check_operation(&strict_url(value).unwrap(), endpoint),
                Err(ImageError::AddressDenied),
            );
        }
        for value in [
            "https://@api.example/operation",
            "https:////@api.example/operation",
            "https:api.example/operation",
            "https://user@api.example/operation",
            "https://api.example/operation#secret",
            " https://api.example/operation",
            "https://api.example/operation\n",
        ] {
            assert_eq!(strict_url(value), Err(ImageError::AddressDenied));
        }
    }

    #[tokio::test]
    async fn azure_last_owner_joins_decoder_and_releases_its_admission() {
        let mut config = AzureImageConfig::new("synthetic-key", "https://fixture.example");
        config.limits.max_pending = 1;
        let client = AzureImageClient::new(config).unwrap();
        let permits = client.0.permits.clone();
        let permit = permits.clone().try_acquire_owned().unwrap();
        let caller = CancellationToken::new();
        caller.cancel();
        let result = client
            .0
            .decoder
            .decode(
                vec![0; 100],
                DecodeControl {
                    cancel: CancellationToken::new(),
                    caller,
                    deadline: Instant::now() + Duration::from_secs(1),
                },
                permit,
            )
            .await;
        drop(client); // DecoderWorker::drop synchronously joins before returning.
        assert_eq!(result, Err(ImageError::Cancelled));
        assert_eq!(permits.available_permits(), 1);
    }
}
