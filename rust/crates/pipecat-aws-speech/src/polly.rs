//! Polly's signed REST synthesis and PCM16 output stream.
use crate::{auth, AwsCredentials, SpeechError};
use futures_util::StreamExt;
use pipecat_audio::SincResampler;
use pipecat_services::CancellationToken;
use pipecat_speech::SpeechChunk;
use serde_json::{json, Value};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PollySettings {
    pub voice: String,
    pub language: String,
    pub engine: Option<String>,
    pub pitch: Option<String>,
    pub rate: Option<String>,
    pub volume: Option<String>,
    pub lexicon_names: Option<Vec<String>>,
}
impl Default for PollySettings {
    fn default() -> Self {
        Self {
            voice: "Joanna".into(),
            language: "en-US".into(),
            engine: None,
            pitch: None,
            rate: None,
            volume: None,
            lexicon_names: None,
        }
    }
}
fn xml(value: &str, attribute: bool) -> String {
    let mut result = String::new();
    for ch in value.chars() {
        match ch {
            '&' => result.push_str("&amp;"),
            '<' => result.push_str("&lt;"),
            '>' => result.push_str("&gt;"),
            '\'' if attribute => result.push_str("&apos;"),
            '"' if attribute => result.push_str("&quot;"),
            _ => result.push(ch),
        }
    }
    result
}
fn valid_xml(value: &str) -> bool {
    value
        .chars()
        .all(|c| matches!(c, '\t' | '\n' | '\r') || c >= ' ' && c != '\u{fffe}' && c != '\u{ffff}')
}
impl PollySettings {
    pub fn validate(&self) -> Result<(), SpeechError> {
        if self.voice.is_empty()
            || self.voice.len() > 128
            || !self.voice.bytes().all(|b| b.is_ascii_alphanumeric())
            || self.language.is_empty()
            || self.language.len() > 64
            || !self
                .language
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || self.engine.as_ref().is_some_and(|s| {
                !matches!(
                    s.as_str(),
                    "standard" | "neural" | "long-form" | "generative"
                )
            })
            || [&self.pitch, &self.rate, &self.volume].iter().any(|s| {
                s.as_ref()
                    .is_some_and(|v| v.is_empty() || v.len() > 128 || !valid_xml(v))
            })
            || self.lexicon_names.as_ref().is_some_and(|names| {
                names.len() > 5
                    || names.iter().any(|n| {
                        n.is_empty()
                            || n.len() > 20
                            || !n.bytes().all(|b| b.is_ascii_alphanumeric())
                    })
            })
        {
            return Err(SpeechError::Invalid("Polly settings"));
        }
        Ok(())
    }
    pub fn ssml(&self, text: &str) -> Result<String, SpeechError> {
        self.validate()?;
        if text.is_empty() || text.chars().count() > 3000 || !valid_xml(text) {
            return Err(SpeechError::Invalid(
                "Polly text must contain 1..=3000 XML-compatible characters",
            ));
        }
        let language = crate::polly_language(&self.language);
        let mut ssml = format!("<speak><lang xml:lang='{}'>", xml(language, true));
        let mut attrs = Vec::new();
        if self.engine.as_deref() == Some("standard") {
            if let Some(pitch) = &self.pitch {
                attrs.push(format!("pitch='{}'", xml(pitch, true)));
            }
        }
        if let Some(rate) = &self.rate {
            attrs.push(format!("rate='{}'", xml(rate, true)));
        }
        if let Some(volume) = &self.volume {
            attrs.push(format!("volume='{}'", xml(volume, true)));
        }
        if !attrs.is_empty() {
            ssml.push_str(&format!("<prosody {}>", attrs.join(" ")));
        }
        ssml.push_str(&xml(text, false));
        if !attrs.is_empty() {
            ssml.push_str("</prosody>");
        }
        ssml.push_str("</lang></speak>");
        if ssml.chars().count() > 6000 {
            return Err(SpeechError::Invalid("Polly SSML exceeds 6000 characters"));
        }
        Ok(ssml)
    }
    pub fn request_body(&self, text: &str) -> Result<Value, SpeechError> {
        let mut body = json!({"Text":self.ssml(text)?,"TextType":"ssml","OutputFormat":"pcm","VoiceId":self.voice,"SampleRate":"16000"});
        if let Some(engine) = &self.engine {
            body["Engine"] = engine.clone().into();
        }
        if let Some(names) = &self.lexicon_names {
            body["LexiconNames"] = json!(names);
        }
        Ok(body)
    }
}
#[derive(Clone)]
pub struct PollyConfig {
    pub credentials: AwsCredentials,
    pub region: String,
    /// Regional AWS route, or a literal loopback route for application tests.
    pub endpoint: String,
    pub settings: PollySettings,
    pub sample_rate: u32,
    pub timeout: Duration,
    pub queue_capacity: usize,
    pub max_audio_bytes: usize,
}
impl fmt::Debug for PollyConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollyConfig")
            .field("credentials", &self.credentials)
            .field("region", &self.region)
            .field("sample_rate", &self.sample_rate)
            .finish_non_exhaustive()
    }
}
impl PollyConfig {
    pub fn new(credentials: AwsCredentials, region: impl Into<String>) -> Self {
        let region = region.into();
        Self {
            endpoint: format!("https://polly.{region}.{}/v1/speech", auth::domain(&region)),
            region,
            credentials,
            settings: PollySettings::default(),
            sample_rate: 24000,
            timeout: Duration::from_secs(60),
            queue_capacity: 16,
            max_audio_bytes: 25 * 1024 * 1024,
        }
    }
    fn url(&self) -> Result<url::Url, SpeechError> {
        auth::region(&self.region).map_err(|_| SpeechError::Invalid("AWS region"))?;
        self.settings.validate()?;
        if !(8000..=192000).contains(&self.sample_rate)
            || self.timeout.is_zero()
            || self.queue_capacity == 0
            || self.queue_capacity > 65536
            || self.max_audio_bytes == 0
            || self.max_audio_bytes > 512 * 1024 * 1024
        {
            return Err(SpeechError::Invalid("Polly limits"));
        }
        auth::endpoint(
            &self.endpoint,
            &format!(
                "https://polly.{}.{}/v1/speech",
                self.region,
                auth::domain(&self.region)
            ),
            "/v1/speech",
            false,
        )
        .map_err(|_| SpeechError::Invalid("Polly endpoint"))
    }
}
#[derive(Clone)]
pub struct PollyClient {
    config: PollyConfig,
    http: reqwest::Client,
}
impl fmt::Debug for PollyClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PollyClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl PollyClient {
    pub fn new(config: PollyConfig) -> Result<Self, SpeechError> {
        config.url()?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(config.timeout)
            .build()
            .map_err(|_| SpeechError::Transport)?;
        Ok(Self { config, http })
    }
    pub fn config(&self) -> &PollyConfig {
        &self.config
    }
    pub fn synthesize(&self, text: impl Into<String>) -> Result<PollyStream, SpeechError> {
        let text = text.into();
        let body = serde_json::to_vec(&self.config.settings.request_body(&text)?)
            .map_err(|_| SpeechError::Invalid("Polly request JSON"))?;
        let config = self.config.clone();
        let http = self.http.clone();
        let url = config.url()?;
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let source_audio = Arc::new(AtomicBool::new(false));
        let observed = source_audio.clone();
        let task = tokio::spawn(async move {
            let operation = async {
                let headers = auth::polly_headers(
                    &url,
                    &config.region,
                    &config.credentials,
                    &body,
                    &auth::now(),
                )
                .map_err(|_| SpeechError::Invalid("Polly signing"))?;
                let response = http
                    .post(url)
                    .headers(headers)
                    .body(body)
                    .send()
                    .await
                    .map_err(|_| SpeechError::Transport)?;
                if !response.status().is_success() {
                    return Err(SpeechError::Http(response.status().as_u16()));
                }
                if response
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|h| h.to_str().ok())
                    .and_then(|h| h.split(';').next())
                    .map(str::trim)
                    != Some("audio/pcm")
                {
                    return Err(SpeechError::ContentType);
                }
                if response
                    .content_length()
                    .is_some_and(|n| n > config.max_audio_bytes as u64)
                {
                    return Err(SpeechError::TooLarge);
                }
                let mut resampler = SincResampler::new(16000, config.sample_rate, 1)?;
                let mut body = response.bytes_stream();
                let mut received = 0usize;
                let mut produced = 0usize;
                while let Some(bytes) = body.next().await {
                    let bytes = bytes.map_err(|_| SpeechError::Transport)?;
                    received = received
                        .checked_add(bytes.len())
                        .ok_or(SpeechError::TooLarge)?;
                    if received > config.max_audio_bytes {
                        return Err(SpeechError::TooLarge);
                    }
                    if !bytes.is_empty() {
                        observed.store(true, Ordering::Release);
                    }
                    for piece in bytes.chunks(16384) {
                        let pcm = resampler.process(piece)?;
                        emit(&tx, pcm, &config, &mut produced).await?;
                    }
                }
                if resampler.pending_bytes() != 0 {
                    return Err(SpeechError::PartialSample);
                }
                emit(&tx, resampler.flush()?, &config, &mut produced).await?;
                if received == 0 {
                    return Err(SpeechError::EmptyAudio);
                }
                Ok(())
            };
            tokio::select! {biased;_=worker_cancel.cancelled()=>Err(SpeechError::Cancelled),result=tokio::time::timeout(config.timeout,operation)=>result.map_err(|_|SpeechError::Timeout)?}
        });
        Ok(PollyStream {
            rx,
            cancel,
            task,
            ended: false,
            source_audio,
        })
    }
}
async fn emit(
    tx: &mpsc::Sender<SpeechChunk>,
    pcm: Vec<u8>,
    config: &PollyConfig,
    total: &mut usize,
) -> Result<(), SpeechError> {
    *total = total.checked_add(pcm.len()).ok_or(SpeechError::TooLarge)?;
    if *total > config.max_audio_bytes {
        return Err(SpeechError::TooLarge);
    }
    let size = (config.sample_rate as usize / 50) * 2;
    for chunk in pcm.chunks(size) {
        tx.send(SpeechChunk {
            pcm: chunk.to_vec(),
            sample_rate: config.sample_rate,
            num_channels: 1,
        })
        .await
        .map_err(|_| SpeechError::ConsumerClosed)?;
    }
    Ok(())
}
/// Bounded, cancel-on-drop response. Completion acknowledges synthesis, not playout.
pub struct PollyStream {
    rx: mpsc::Receiver<SpeechChunk>,
    cancel: CancellationToken,
    task: JoinHandle<Result<(), SpeechError>>,
    ended: bool,
    source_audio: Arc<AtomicBool>,
}
impl PollyStream {
    pub fn source_audio_observed(&self) -> bool {
        self.source_audio.load(Ordering::Acquire)
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub async fn next(&mut self) -> Option<Result<SpeechChunk, SpeechError>> {
        if self.ended {
            return None;
        }
        tokio::select! {biased;_=self.cancel.cancelled()=>{self.rx.close();while self.rx.try_recv().is_ok() {} self.task.abort();self.ended=true;return Some(Err(SpeechError::Cancelled));},chunk=self.rx.recv()=>if let Some(chunk)=chunk {return Some(Ok(chunk));}}
        let result = (&mut self.task).await;
        self.ended = true;
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(SpeechError::Transport)),
        }
    }
}
impl Drop for PollyStream {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}
