//! Browser-compatible RTVI runner with bounded WebSocket and optional WebRTC sessions.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
mod cascade;
mod command;
mod config;
pub use cascade::VoiceCascade;
#[cfg(feature = "soniox")]
mod soniox;
pub use config::SttProvider;
#[cfg(feature = "soniox")]
pub use soniox::{SonioxSessionTools, SonioxToolsFactory, SonioxVoiceConfig};
#[cfg(feature = "whisper")]
mod whisper;
pub use command::run_from_env;
#[cfg(feature = "whisper")]
pub use whisper::{LocalWhisperConfig, WhisperSegmentation};
mod processors;
mod session;
mod transport;
#[cfg(feature = "webrtc")]
mod webrtc;
#[cfg(feature = "webrtc")]
pub use webrtc::WebRtcRunnerConfig;
pub mod wire;

pub use pipecat_cli::config::{Options, ProviderConfig};
use pipecat_client_server::{server::BotApi, ProxyError, SessionMethod};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, watch},
    task::JoinSet,
};

#[derive(Debug, thiserror::Error)]
pub enum RunnerError {
    #[error("invalid runner configuration")]
    Configuration,
    #[error("invalid voice cascade configuration: {0}")]
    CascadeConfiguration(&'static str),
    #[error("invalid STT configuration: {0}")]
    SttConfiguration(&'static str),
    #[cfg(feature = "whisper")]
    #[error("local Whisper failed: {0}")]
    Whisper(#[from] pipecat_whisper::WhisperError),
    #[error("browser protocol: {0}")]
    Protocol(&'static str),
    #[error("I/O failed")]
    Io(#[from] std::io::Error),
    #[error("WebSocket failed")]
    Transport(#[from] pipecat_transports::TransportError),
    #[error("protobuf failed")]
    Protobuf(#[from] pipecat_protocol::ProtocolError),
    #[error("pipeline failed")]
    Pipeline(#[from] pipecat_core::PipelineError),
    #[error("provider initialization or request failed")]
    Provider,
    #[error("RTVI initialization failed")]
    Rtvi,
    #[error("session deadline exceeded")]
    Deadline,
}
#[derive(Clone, Debug)]
pub struct RunnerConfig {
    pub bind_ip: IpAddr,
    pub allowed_origins: Vec<String>,
    /// Intended only for native API clients and credential-free tests.
    pub allow_missing_origin: bool,
    pub max_sessions: usize,
    pub connect_timeout: Duration,
    pub max_session_duration: Duration,
    pub options: Options,
    pub stt: SttProvider,
}
impl Default for RunnerConfig {
    fn default() -> Self {
        Self {
            bind_ip: IpAddr::from([127, 0, 0, 1]),
            allowed_origins: vec![],
            allow_missing_origin: false,
            max_sessions: 8,
            connect_timeout: Duration::from_secs(30),
            max_session_duration: Duration::from_secs(3600),
            options: Options::default(),
            stt: SttProvider::default(),
        }
    }
}
#[derive(Debug, Clone)]
pub struct SessionSummary {
    pub session_id: String,
    /// Text-generation context, explicitly not a record of audible playback.
    pub generated_context: pipecat_services::Context,
    pub pipeline_errors: u64,
    pub successful: bool,
}
struct Entry {
    cancel: watch::Sender<bool>,
    #[cfg(feature = "webrtc")]
    signaling: Option<tokio::sync::mpsc::Sender<webrtc::SignalingCommand>>,
}
struct State {
    closed: bool,
    sessions: BTreeMap<String, Entry>,
    tasks: JoinSet<()>,
}
struct Inner {
    config: RunnerConfig,
    providers: cascade::SessionProviders,
    resources: session::SessionResources,
    state: Mutex<State>,
    reports: broadcast::Sender<SessionSummary>,
    shutdown: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    #[cfg(feature = "webrtc")]
    webrtc: Option<WebRtcRunnerConfig>,
}
/// Owns every session task. Dropping its last handle aborts the JoinSet; call
/// `shutdown` to request cancellation and join all session cleanup first.
#[derive(Clone)]
pub struct NativeRunner {
    inner: Arc<Inner>,
}
impl NativeRunner {
    pub fn new(config: RunnerConfig, providers: ProviderConfig) -> Result<Self, RunnerError> {
        Self::new_cascade(
            config,
            cascade::SessionProviders::Default(Box::new(providers)),
        )
    }
    /// Soniox recognition and synthesis with native OpenAI/tool continuation.
    /// This composition supports WebSocket sessions and generated-text history.
    #[cfg(feature = "soniox")]
    pub fn new_soniox(
        config: RunnerConfig,
        providers: SonioxVoiceConfig,
    ) -> Result<Self, RunnerError> {
        providers.validate()?;
        if !matches!(config.stt, SttProvider::Deepgram) || config.options.vad.is_some() {
            return Err(RunnerError::CascadeConfiguration("Soniox cascade uses provider endpoint detection; local STT and VAD are unsupported"));
        }
        Self::new_cascade(
            config,
            cascade::SessionProviders::Soniox(Box::new(providers)),
        )
    }
    fn new_cascade(
        config: RunnerConfig,
        providers: cascade::SessionProviders,
    ) -> Result<Self, RunnerError> {
        if !config.bind_ip.is_loopback()
            || config.max_sessions == 0
            || config.max_sessions > 1024
            || config.connect_timeout.is_zero()
            || config.max_session_duration.is_zero()
            || config.options.idle_timeout.is_zero()
            || config
                .allowed_origins
                .iter()
                .any(|origin| !valid_origin(origin))
            || !(8000..=48000).contains(&providers.input_rate())
            || !(8000..=48000).contains(&providers.output_rate())
        {
            return Err(RunnerError::Configuration);
        }
        #[cfg(feature = "whisper")]
        if let SttProvider::Whisper(local) = &config.stt {
            local.validate()?;
            if providers.input_rate() != 16000 {
                return Err(RunnerError::SttConfiguration(
                    "Whisper input rate must be 16000",
                ));
            }
            if matches!(local.segmentation, WhisperSegmentation::FixedWindow(_))
                && config.options.vad.is_some()
            {
                return Err(RunnerError::SttConfiguration(
                    "choose either Whisper VAD or fixed-window segmentation",
                ));
            }
            if matches!(local.segmentation, WhisperSegmentation::Vad)
                && config.options.vad.is_none()
            {
                return Err(RunnerError::SttConfiguration("Whisper VAD mode requires --vad-model and --onnx-runtime, or explicit fixed-window segmentation"));
            }
        }
        let (reports, _) = broadcast::channel(config.max_sessions.max(16));
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                providers,
                resources: session::SessionResources::default(),
                state: Mutex::new(State {
                    closed: false,
                    sessions: BTreeMap::new(),
                    tasks: JoinSet::new(),
                }),
                reports,
                shutdown: tokio::sync::Mutex::new(None),
                #[cfg(feature = "webrtc")]
                webrtc: None,
            }),
        })
    }
    /// Enable SmallWebRTC alongside the unchanged WebSocket API. Session IDs
    /// returned for WebRTC are secret bearer capabilities, not public peer IDs.
    #[cfg(feature = "webrtc")]
    pub fn new_with_webrtc(
        config: RunnerConfig,
        providers: ProviderConfig,
        webrtc: WebRtcRunnerConfig,
    ) -> Result<Self, RunnerError> {
        if webrtc.signaling_timeout.is_zero() || !webrtc.codec.available() {
            return Err(RunnerError::Configuration);
        }
        let mut runner = Self::new(config, providers)?;
        Arc::get_mut(&mut runner.inner).unwrap().webrtc = Some(webrtc);
        Ok(runner)
    }
    #[cfg(feature = "webrtc")]
    fn start_webrtc(&self) -> Result<Value, ProxyError> {
        let rtc = self
            .inner
            .webrtc
            .clone()
            .ok_or(ProxyError::InvalidRequest)?;
        let id = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let (cancel, cancelled) = watch::channel(false);
        let (signaling, commands) = tokio::sync::mpsc::channel(4);
        let config = self.inner.config.clone();
        let providers = self
            .inner
            .providers
            .legacy()
            .ok_or(ProxyError::InvalidRequest)?
            .clone();
        let resources = self.inner.resources.for_session();
        let reports = self.inner.reports.clone();
        let weak = Arc::downgrade(&self.inner);
        let session_id = id.clone();
        let mut state = self.inner.state.lock().map_err(|_| ProxyError::Closed)?;
        while state.tasks.try_join_next().is_some() {}
        if state.closed {
            return Err(ProxyError::Closed);
        }
        if state.sessions.len() >= config.max_sessions {
            return Err(ProxyError::Busy);
        }
        state.sessions.insert(
            id.clone(),
            Entry {
                cancel,
                signaling: Some(signaling),
            },
        );
        state.tasks.spawn(async move {
            let result = webrtc::serve_session(
                commands,
                config,
                providers,
                rtc,
                cancelled,
                resources.clone(),
            )
            .await;
            resources.join_native().await;
            let (generated_context, pipeline_errors, successful) = match result {
                Ok(report) => (report.0, report.1, true),
                Err(_) => (Default::default(), 0, false),
            };
            if let Some(inner) = weak.upgrade() {
                inner.state.lock().unwrap().sessions.remove(&session_id);
            }
            let _ = reports.send(SessionSummary {
                session_id,
                generated_context,
                pipeline_errors,
                successful,
            });
        });
        Ok(
            json!({"sessionId":id,"token":null,"transport":"webrtc","icePolicy":"host-only","iceConfig":{"iceServers":[]},"contextMode":"generated-text","playbackAcknowledgements":false}),
        )
    }
    pub fn subscribe(&self) -> broadcast::Receiver<SessionSummary> {
        self.inner.reports.subscribe()
    }
    pub fn active_sessions(&self) -> usize {
        self.inner.state.lock().unwrap().sessions.len()
    }
    /// Native model owners, including loading and teardown. Zero guarantees
    /// all owned Whisper workers/reapers have been joined.
    #[cfg(feature = "whisper")]
    pub fn active_local_stt_workers(&self) -> usize {
        self.inner
            .resources
            .local_stt_workers
            .count
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    /// Cumulative fixed windows accepted by native inference, including a
    /// graceful final partial window. VAD segment admission stays inside STT.
    #[cfg(feature = "whisper")]
    pub fn local_stt_fixed_windows_submitted(&self) -> usize {
        self.inner
            .resources
            .local_stt_workers
            .fixed_windows_submitted
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub async fn shutdown(&self) {
        // Retain one cleanup owner across concurrent or cancelled callers.
        let mut shutdown = self.inner.shutdown.lock().await;
        if shutdown.is_none() {
            let mut state = self.inner.state.lock().unwrap();
            if !state.closed {
                state.closed = true;
                for entry in state.sessions.values() {
                    let _ = entry.cancel.send(true);
                }
                let mut tasks = std::mem::take(&mut state.tasks);
                let resources = self.inner.resources.clone();
                let weak = Arc::downgrade(&self.inner);
                *shutdown = Some(tokio::spawn(async move {
                    if tokio::time::timeout(Duration::from_secs(15), async {
                        while tasks.join_next().await.is_some() {}
                    })
                    .await
                    .is_err()
                    {
                        tasks.abort_all();
                        while tasks.join_next().await.is_some() {}
                    }
                    // Pipeline cancellation can outlive a force-aborted session.
                    resources.join_native().await;
                    if let Some(inner) = weak.upgrade() {
                        inner.state.lock().unwrap().sessions.clear();
                    }
                }));
            }
        }
        if let Some(task) = shutdown.as_mut() {
            let _ = task.await;
            shutdown.take();
        }
    }
}
fn valid_origin(origin: &str) -> bool {
    url::Url::parse(origin).is_ok_and(|u| {
        matches!(u.scheme(), "http" | "https")
            && u.host_str().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && u.query().is_none()
            && u.fragment().is_none()
            && u.path() == "/"
            && u.origin().ascii_serialization() == origin
    })
}
#[async_trait::async_trait]
impl BotApi for NativeRunner {
    fn max_request_bytes(&self) -> usize {
        64 * 1024
    }
    async fn start(&self, body: &Value) -> Result<Value, ProxyError> {
        if body
            .get("body")
            .is_some_and(|v| !v.is_object() || !v.as_object().unwrap().is_empty())
        {
            return Err(ProxyError::InvalidRequest);
        }
        if !body.is_object() {
            return Err(ProxyError::InvalidRequest);
        }
        let transport = body.get("transport").and_then(Value::as_str);
        #[cfg(feature = "webrtc")]
        if matches!(transport, Some("webrtc" | "smallwebrtc")) {
            // Source browser hints never enable third-party services on this
            // loopback runner. The response explicitly supplies host-only ICE.
            if body.as_object().unwrap().keys().any(|k| {
                !matches!(
                    k.as_str(),
                    "transport" | "body" | "createDailyRoom" | "enableDefaultIceServers"
                )
            }) || body.get("createDailyRoom").is_some_and(|v| v != false)
                || body
                    .get("enableDefaultIceServers")
                    .is_some_and(|v| !v.is_boolean())
            {
                return Err(ProxyError::InvalidRequest);
            }
            return self.start_webrtc();
        }
        if transport != Some("websocket")
            || body
                .as_object()
                .unwrap()
                .keys()
                .any(|k| k != "transport" && k != "body")
        {
            return Err(ProxyError::InvalidRequest);
        }
        // Binding is the only await before registration; a canceled HTTP request
        // cannot leave an unregistered listener or detached session task.
        let listener = TcpListener::bind((self.inner.config.bind_ip, 0))
            .await
            .map_err(|_| ProxyError::Connection)?;
        let address = listener.local_addr().map_err(|_| ProxyError::Connection)?;
        let id = uuid::Uuid::new_v4().to_string();
        let token = uuid::Uuid::new_v4().simple().to_string();
        let ws_url = format!("ws://{address}/ws-client/{token}");
        let (cancel, mut cancelled) = watch::channel(false);
        let config = self.inner.config.clone();
        let providers = self.inner.providers.clone();
        let resources = self.inner.resources.for_session();
        let reports = self.inner.reports.clone();
        let weak = Arc::downgrade(&self.inner);
        let session_id = id.clone();
        let mut state = self.inner.state.lock().map_err(|_| ProxyError::Closed)?;
        while state.tasks.try_join_next().is_some() {}
        if state.closed {
            return Err(ProxyError::Closed);
        }
        if state.sessions.len() >= config.max_sessions {
            return Err(ProxyError::Busy);
        }
        state.sessions.insert(
            id.clone(),
            Entry {
                cancel,
                #[cfg(feature = "webrtc")]
                signaling: None,
            },
        );
        state.tasks.spawn(async move {
            let result = session::accept_session(
                listener,
                &token,
                &config,
                &providers,
                &mut cancelled,
                &resources,
            )
            .await;
            resources.join_native().await;
            let (generated_context, pipeline_errors, successful) = match result {
                Ok(report) => (report.0, report.1, true),
                Err(_) => (Default::default(), 0, false),
            };
            if let Some(inner) = weak.upgrade() {
                inner.state.lock().unwrap().sessions.remove(&session_id);
            }
            let _ = reports.send(SessionSummary {
                session_id,
                generated_context,
                pipeline_errors,
                successful,
            });
        });
        Ok(
            json!({"wsUrl":ws_url,"sessionId":id,"token":null,"transport":"websocket","contextMode":"generated-text","playbackAcknowledgements":false}),
        )
    }
    async fn session(
        &self,
        method: SessionMethod,
        id: &str,
        path: &[&str],
        body: &Value,
    ) -> Result<Value, ProxyError> {
        #[cfg(feature = "webrtc")]
        if path == ["api", "offer"] {
            let signaling = {
                let state = self.inner.state.lock().map_err(|_| ProxyError::Closed)?;
                state
                    .sessions
                    .get(id)
                    .and_then(|entry| entry.signaling.clone())
                    .ok_or(ProxyError::Path)?
            };
            let (reply, response) = tokio::sync::oneshot::channel();
            signaling
                .try_send(webrtc::SignalingCommand {
                    method,
                    body: body.clone(),
                    reply,
                })
                .map_err(|error| match error {
                    tokio::sync::mpsc::error::TrySendError::Full(_) => ProxyError::Busy,
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => ProxyError::Closed,
                })?;
            return response.await.map_err(|_| ProxyError::Closed)?;
        }
        if !matches!(method, SessionMethod::Post)
            || path != ["stop"]
            || !body.is_object()
            || !body.as_object().unwrap().is_empty()
        {
            return Err(ProxyError::InvalidRequest);
        }
        let state = self.inner.state.lock().map_err(|_| ProxyError::Closed)?;
        let entry = state.sessions.get(id).ok_or(ProxyError::Path)?;
        let _ = entry.cancel.send(true);
        Ok(json!({"sessionId":id,"stopping":true}))
    }
}
