//! Shared bounded HTTP transport and owned response streams.
use crate::{ResponseStream, SpeechChunk, SpeechConfig, SpeechError, SpeechStream};
use futures_util::StreamExt;
use pipecat_services::CancellationToken;
use reqwest::{header::HeaderValue, RequestBuilder, Response, Url};
use serde::de::DeserializeOwned;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;

#[derive(Clone)]
pub(crate) struct HttpClient {
    pub config: SpeechConfig,
    http: reqwest::Client,
    base: Url,
}
impl HttpClient {
    pub fn new(config: SpeechConfig) -> Result<Self, SpeechError> {
        Self::with_direct_policy(config, false)
    }
    #[cfg(any(feature = "speechmatics", feature = "speechify", feature = "minimax"))]
    pub fn direct(config: SpeechConfig) -> Result<Self, SpeechError> {
        Self::with_direct_policy(config, true)
    }
    fn with_direct_policy(config: SpeechConfig, direct: bool) -> Result<Self, SpeechError> {
        let base = Url::parse(&config.base_url).map_err(|_| SpeechError::Invalid("base URL"))?;
        let loopback = base.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if !(base.scheme() == "https" || (base.scheme() == "http" && loopback))
            || base.host_str().is_none()
            || !base.username().is_empty()
            || base.password().is_some()
            || base.query().is_some()
            || base.fragment().is_some()
            || config.timeout.is_zero()
            || config.queue_capacity == 0
            || config.max_audio_bytes == 0
            || config.max_json_bytes == 0
        {
            return Err(SpeechError::Invalid("configuration"));
        }
        secret(&config.api_key)?;
        let mut builder = reqwest::Client::builder().redirect(reqwest::redirect::Policy::none());
        if direct {
            builder = builder
                .no_proxy()
                .http1_only()
                .retry(reqwest::retry::never());
        }
        let http = builder.build().map_err(|_| SpeechError::Transport)?;
        Ok(Self { config, http, base })
    }
    pub fn post(&self, path: &[&str]) -> Result<RequestBuilder, SpeechError> {
        let mut url = self.base.clone();
        let mut segments = url
            .path_segments_mut()
            .map_err(|_| SpeechError::Invalid("base URL"))?;
        segments.pop_if_empty();
        for segment in path {
            if segment.is_empty() || matches!(*segment, "." | "..") {
                return Err(SpeechError::Invalid("URL path segment"));
            }
            segments.push(segment);
        }
        drop(segments);
        Ok(self.http.post(url))
    }
    pub fn wav_part(
        &self,
        pcm: &[u8],
        rate: u32,
        channels: u16,
        mime: &str,
    ) -> Result<reqwest::multipart::Part, SpeechError> {
        if pcm.len() > self.config.max_audio_bytes {
            return Err(SpeechError::TooLarge);
        }
        pipecat_audio::PcmFormat::new(rate, channels)?.validate(pcm)?;
        if pcm.is_empty() {
            return Err(SpeechError::Invalid("empty audio"));
        }
        let wav = pipecat_audio::pcm_to_wav(pcm, rate, channels)?;
        if wav.len() > self.config.max_audio_bytes {
            return Err(SpeechError::TooLarge);
        }
        reqwest::multipart::Part::bytes(wav)
            .file_name("audio.wav")
            .mime_str(mime)
            .map_err(|_| SpeechError::Invalid("MIME"))
    }
    pub async fn json<T: DeserializeOwned>(
        &self,
        request: RequestBuilder,
        cancel: CancellationToken,
    ) -> Result<T, SpeechError> {
        let operation = async {
            let mut response = send(request).await?;
            check_content_type(&response, &["application/json"], None)?;
            let mut bytes = Vec::new();
            let mut total = 0;
            while let Some(chunk) = response.chunk().await.map_err(|_| SpeechError::Transport)? {
                checked_total(&mut total, chunk.len(), self.config.max_json_bytes)?;
                bytes.extend_from_slice(&chunk);
            }
            Ok(serde_json::from_slice(&bytes)?)
        };
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(SpeechError::Cancelled),
            result = tokio::time::timeout(self.config.timeout, operation) => result.unwrap_or(Err(SpeechError::Timeout)),
        }
    }
    pub fn spawn<T, F, Fut>(&self, operation: F) -> ResponseStream<T>
    where
        T: Send + 'static,
        F: FnOnce(mpsc::Sender<T>, Arc<AtomicBool>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), SpeechError>> + Send + 'static,
    {
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let (tx, rx) = mpsc::channel(self.config.queue_capacity);
        let timeout = self.config.timeout;
        let source_audio = Arc::new(AtomicBool::new(false));
        let observed = source_audio.clone();
        let task = tokio::spawn(async move {
            tokio::select! { biased;
                _ = task_cancel.cancelled() => Err(SpeechError::Cancelled),
                result = tokio::time::timeout(timeout, operation(tx, observed)) => result.unwrap_or(Err(SpeechError::Timeout)),
            }
        });
        ResponseStream {
            rx,
            cancel,
            task,
            ended: false,
            source_audio,
        }
    }
    pub fn raw_stream(
        &self,
        request: RequestBuilder,
        rate: u32,
        encoding: RawEncoding,
        content_types: &'static [&'static str],
        empty_response_is_source_audio: bool,
    ) -> SpeechStream {
        let limit = self.config.max_audio_bytes;
        self.spawn(move |tx, observed| async move {
            let response = send(request).await?;
            check_content_type(&response, content_types, Some((rate, 1)))?;
            let source_success = response.status() == reqwest::StatusCode::OK;
            let mut stream = response.bytes_stream();
            let mut decoder = AudioDecoder::new(rate, limit, encoding);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| SpeechError::Transport)?;
                if let Some(audio) = decoder.push(&chunk)? {
                    observed.store(true, Ordering::Release);
                    tx.send(audio)
                        .await
                        .map_err(|_| SpeechError::ConsumerClosed)?;
                }
            }
            let result = decoder.finish();
            // Cartesia's source adapter emits its whole-response audio frame
            // only after an HTTP 200 body has been read in full.
            if source_success
                && empty_response_is_source_audio
                && matches!(result, Err(SpeechError::EmptyAudio))
            {
                observed.store(true, Ordering::Release);
            }
            result
        })
    }
}
pub(crate) fn secret(value: &str) -> Result<HeaderValue, SpeechError> {
    if value.is_empty() {
        return Err(SpeechError::Invalid("empty API key"));
    }
    let mut value =
        HeaderValue::from_str(value).map_err(|_| SpeechError::Invalid("API key header"))?;
    value.set_sensitive(true);
    Ok(value)
}
pub(crate) async fn send(request: RequestBuilder) -> Result<Response, SpeechError> {
    let response = request.send().await.map_err(|_| SpeechError::Transport)?;
    if !response.status().is_success() {
        return Err(SpeechError::Http(response.status().as_u16()));
    }
    Ok(response)
}
pub(crate) fn checked_total(
    total: &mut usize,
    amount: usize,
    limit: usize,
) -> Result<(), SpeechError> {
    *total = total.checked_add(amount).ok_or(SpeechError::TooLarge)?;
    if *total > limit {
        Err(SpeechError::TooLarge)
    } else {
        Ok(())
    }
}
pub(crate) fn check_content_type(
    response: &Response,
    allowed: &[&str],
    format: Option<(u32, u16)>,
) -> Result<(), SpeechError> {
    let Some(mime) = response.headers().get(reqwest::header::CONTENT_TYPE) else {
        return Ok(());
    };
    let mime = mime.to_str().map_err(|_| SpeechError::ContentType)?;
    let mut parts = mime.split(';');
    let media_type = parts.next().unwrap_or_default().trim().to_ascii_lowercase();
    if !allowed.contains(&media_type.as_str()) {
        return Err(SpeechError::ContentType);
    }
    if let Some((rate, channels)) = format {
        for part in parts {
            if let Some((key, value)) = part.split_once('=') {
                let value = value.trim().trim_matches('"');
                let expected = match key.trim().to_ascii_lowercase().as_str() {
                    "rate" => rate,
                    "channels" => u32::from(channels),
                    _ => continue,
                };
                if value.parse::<u32>().ok() != Some(expected) {
                    return Err(SpeechError::ContentType);
                }
            }
        }
    }
    Ok(())
}
#[derive(Clone, Copy)]
pub(crate) enum RawEncoding {
    Pcm16,
    Mulaw,
    Alaw,
}
pub(crate) struct AudioDecoder {
    rate: u32,
    limit: usize,
    total: usize,
    pending: Option<u8>,
    encoding: RawEncoding,
}
impl AudioDecoder {
    pub fn has_partial_sample(&self) -> bool {
        self.pending.is_some()
    }
    pub fn new(rate: u32, limit: usize, encoding: RawEncoding) -> Self {
        Self {
            rate,
            limit,
            total: 0,
            pending: None,
            encoding,
        }
    }
    pub fn push(&mut self, bytes: &[u8]) -> Result<Option<SpeechChunk>, SpeechError> {
        let size = match self.encoding {
            RawEncoding::Pcm16 => bytes.len(),
            _ => bytes.len().checked_mul(2).ok_or(SpeechError::TooLarge)?,
        };
        checked_total(&mut self.total, size, self.limit)?;
        let pcm = match self.encoding {
            RawEncoding::Pcm16 => {
                let mut pcm = Vec::with_capacity(bytes.len() + usize::from(self.pending.is_some()));
                if let Some(byte) = self.pending.take() {
                    pcm.push(byte);
                }
                pcm.extend_from_slice(bytes);
                if pcm.len() % 2 != 0 {
                    self.pending = pcm.pop();
                }
                pcm
            }
            RawEncoding::Mulaw => pipecat_audio::ulaw_to_pcm(bytes),
            RawEncoding::Alaw => pipecat_audio::alaw_to_pcm(bytes),
        };
        Ok((!pcm.is_empty()).then_some(SpeechChunk {
            pcm,
            sample_rate: self.rate,
            num_channels: 1,
        }))
    }
    pub fn finish(&self) -> Result<(), SpeechError> {
        if self.total == 0 {
            Err(SpeechError::EmptyAudio)
        } else if self.pending.is_some() {
            Err(SpeechError::PartialSample)
        } else {
            Ok(())
        }
    }
}
pub(crate) fn finite_range(value: Option<f64>, low: f64, high: f64) -> bool {
    value.is_none_or(|v| v.is_finite() && (low..=high).contains(&v))
}
pub(crate) fn optional_text(value: &Option<String>) -> bool {
    value.as_ref().is_none_or(|v| !v.is_empty())
}
