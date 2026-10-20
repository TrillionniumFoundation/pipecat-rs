//! Explicit native Twilio inbound application. No browser RTVI readiness or wire frames.
mod config;
mod security;
mod session;
use crate::ProviderConfig;
use bytes::Bytes;
pub use config::{HangupPolicy, TwilioConfig};
use http_body_util::{BodyExt, Full, Limited};
use hyper::{body::Incoming, service::service_fn, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use pipecat_services::Context;
use std::{
    collections::BTreeMap,
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{broadcast, oneshot, watch},
    task::{JoinHandle, JoinSet},
    time::Instant,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TwilioError {
    #[error("invalid Twilio configuration: {0}")]
    Configuration(&'static str),
    #[error("Twilio protocol rejected: {0}")]
    Protocol(&'static str),
    #[error("Twilio deadline exceeded")]
    Deadline,
    #[error("Twilio transport failed")]
    Transport,
    #[error("Twilio pipeline failed")]
    Pipeline,
    #[error("Twilio hangup request failed")]
    Hangup,
    #[error("Twilio hangup timed out")]
    HangupTimeout,
    #[error("Twilio hangup returned status {0}")]
    HangupStatus(u16),
    #[error("Twilio owned task failed")]
    Task,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Termination {
    End,
    Cancel,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Control {
    Running,
    End,
    Cancel,
}
#[derive(Debug, Clone)]
pub struct CallSummary {
    pub call_sid: String,
    pub verified_start: bool,
    /// Generated text, not proof of audible playback.
    pub generated_context: Context,
    pub error: Option<TwilioError>,
    /// A failed final pipeline join, including cleanup errors/timeouts. This
    /// does not assert that every pipeline join failure is a cleanup failure.
    pub pipeline_join_error: Option<TwilioError>,
    pub media_cleanup_error: Option<TwilioError>,
    pub hangup_error: Option<TwilioError>,
}
impl CallSummary {
    pub fn successful(&self) -> bool {
        self.error.is_none()
            && self.pipeline_join_error.is_none()
            && self.media_cleanup_error.is_none()
            && self.hangup_error.is_none()
    }
    fn new(call_sid: String) -> Self {
        Self {
            call_sid,
            verified_start: false,
            generated_context: Context::default(),
            error: None,
            pipeline_join_error: None,
            media_cleanup_error: None,
            hangup_error: None,
        }
    }
}
#[derive(Default, Debug, Clone)]
pub struct ShutdownReport {
    pub joined_connections: usize,
    pub failed_calls: usize,
    pub pipeline_join_errors: usize,
    pub media_cleanup_errors: usize,
    pub hangup_errors: usize,
    pub task_errors: usize,
}
impl ShutdownReport {
    pub fn failed_cleanup(&self) -> bool {
        self.pipeline_join_errors
            + self.media_cleanup_errors
            + self.hangup_errors
            + self.task_errors
            > 0
    }
    fn account_summary(&mut self, summary: &CallSummary) {
        if !summary.successful() {
            self.failed_calls += 1;
        }
        if summary.pipeline_join_error.is_some() {
            self.pipeline_join_errors += 1;
        }
        if summary.media_cleanup_error.is_some() {
            self.media_cleanup_errors += 1;
        }
        if summary.hangup_error.is_some() {
            self.hangup_errors += 1;
        }
    }
}
struct Pending {
    call: String,
    expires: Instant,
}
struct State {
    closed: bool,
    pending: BTreeMap<String, Pending>,
    active: BTreeMap<String, watch::Sender<Control>>,
    recent: BTreeMap<String, Instant>,
}
impl State {
    fn prune(&mut self) {
        let now = Instant::now();
        self.pending.retain(|_, v| v.expires > now);
        self.recent.retain(|_, v| *v > now);
    }
}
struct Shared {
    config: TwilioConfig,
    providers: ProviderConfig,
    state: Mutex<State>,
    reports: broadcast::Sender<CallSummary>,
    hangup_client: reqwest::Client,
}
struct Owner {
    shared: Arc<Shared>,
    stop: watch::Sender<bool>,
    task: tokio::sync::Mutex<Option<JoinHandle<ShutdownReport>>>,
    result: Mutex<Option<Result<ShutdownReport, TwilioError>>>,
}
impl Drop for Owner {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
/// A bounded listener plus all its HTTP/upgrade/media/provider tasks. Explicit
/// shutdown joins the retained supervisor; canceling a waiter never drops it.
#[derive(Clone)]
pub struct TwilioApp {
    owner: Arc<Owner>,
}
impl TwilioApp {
    /// Production and synthetic tests use this exact constructor. It neither
    /// reads environment variables nor enables any browser/RTC routes.
    pub fn new(
        listener: TcpListener,
        config: TwilioConfig,
        providers: ProviderConfig,
    ) -> Result<Self, TwilioError> {
        config.validate()?;
        if !listener
            .local_addr()
            .map_err(|_| TwilioError::Transport)?
            .ip()
            .is_loopback()
        {
            return Err(TwilioError::Configuration(
                "listener must use literal loopback",
            ));
        }
        for (endpoint, kind) in [
            (&providers.deepgram_endpoint, "ws"),
            (&providers.cartesia_endpoint, "ws"),
            (&providers.openai_endpoint, "http"),
        ] {
            security::validate_url(endpoint, kind).map_err(TwilioError::Configuration)?;
        }
        if [
            &providers.deepgram_key,
            &providers.openai_key,
            &providers.cartesia_key,
        ]
        .iter()
        .any(|k| k.trim().is_empty())
            || !(8000..=48000).contains(&providers.input_rate)
            || !(8000..=48000).contains(&providers.output_rate)
        {
            return Err(TwilioError::Configuration(
                "invalid hosted credentials or rates",
            ));
        }
        // Validate without connecting. Provider secrets never enter Debug/errors.
        crate::cascade::hosted_parts(&providers, config.turn_timeout)
            .map_err(|_| TwilioError::Configuration("invalid hosted provider settings"))?;
        let client = reqwest::Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .pool_max_idle_per_host(0)
            .timeout(config.io_timeout)
            .build()
            .map_err(|_| TwilioError::Configuration("hangup HTTP client"))?;
        let (reports, _) = broadcast::channel(config.max_calls.max(16));
        let shared = Arc::new(Shared {
            config,
            providers,
            state: Mutex::new(State {
                closed: false,
                pending: BTreeMap::new(),
                active: BTreeMap::new(),
                recent: BTreeMap::new(),
            }),
            reports,
            hangup_client: client,
        });
        let (stop, stopped) = watch::channel(false);
        let task = tokio::spawn(supervise(listener, shared.clone(), stopped));
        Ok(Self {
            owner: Arc::new(Owner {
                shared,
                stop,
                task: tokio::sync::Mutex::new(Some(task)),
                result: Mutex::new(None),
            }),
        })
    }
    pub fn subscribe(&self) -> broadcast::Receiver<CallSummary> {
        self.owner.shared.reports.subscribe()
    }
    pub fn active_sessions(&self) -> usize {
        self.owner.shared.state.lock().unwrap().active.len()
    }
    pub fn pending_sessions(&self) -> usize {
        let mut state = self.owner.shared.state.lock().unwrap();
        state.prune();
        state.pending.len()
    }
    /// Trusted embedding control only. No remote HTTP control route is exposed.
    pub fn terminate(&self, call_sid: &str, termination: Termination) -> bool {
        self.owner
            .shared
            .state
            .lock()
            .unwrap()
            .active
            .get(call_sid)
            .is_some_and(|s| {
                if s.receiver_count() == 0 {
                    return false;
                }
                let requested = match termination {
                    Termination::End => Control::End,
                    Termination::Cancel => Control::Cancel,
                };
                s.send_if_modified(|current| {
                    if requested > *current {
                        *current = requested;
                        true
                    } else {
                        false
                    }
                });
                true
            })
    }
    pub async fn shutdown(&self) -> Result<ShutdownReport, TwilioError> {
        let _ = self.owner.stop.send(true);
        let mut task = self.owner.task.lock().await;
        if let Some(handle) = task.as_mut() {
            let report = handle.await.map_err(|_| TwilioError::Task);
            *self.owner.result.lock().unwrap() = Some(report);
            task.take();
        }
        self.owner
            .result
            .lock()
            .unwrap()
            .clone()
            .ok_or(TwilioError::Task)?
    }
}
async fn supervise(
    listener: TcpListener,
    shared: Arc<Shared>,
    mut stop: watch::Receiver<bool>,
) -> ShutdownReport {
    let mut tasks = JoinSet::new();
    let mut report = ShutdownReport::default();
    let mut prune = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! { biased;
            _=stop.changed()=>break,
            result=tasks.join_next(),if !tasks.is_empty()=>account(result.unwrap(),&shared,&mut report),
            _=prune.tick()=>shared.state.lock().unwrap().prune(),
            accepted=listener.accept()=>match accepted {
                Ok((stream,_)) if tasks.len()<shared.config.max_connections=>{tasks.spawn(connection(stream,shared.clone(),stop.clone()));},
                Ok(_)=>{},
                Err(_)=>{report.task_errors+=1;break;}
            }
        }
    }
    drop(listener);
    {
        let mut state = shared.state.lock().unwrap();
        state.closed = true;
        state.pending.clear();
        for signal in state.active.values() {
            let _ = signal.send(Control::Cancel);
        }
    }
    while let Some(result) = tasks.join_next().await {
        account(result, &shared, &mut report);
    }
    shared.state.lock().unwrap().active.clear();
    report
}
fn account(
    result: Result<Option<CallSummary>, tokio::task::JoinError>,
    shared: &Shared,
    report: &mut ShutdownReport,
) {
    report.joined_connections += 1;
    match result {
        Ok(Some(summary)) => {
            report.account_summary(&summary);
            let _ = shared.reports.send(summary);
        }
        Ok(None) => {}
        Err(_) => report.task_errors += 1,
    }
}
struct Upgrade {
    on_upgrade: hyper::upgrade::OnUpgrade,
    call: String,
    control: watch::Receiver<Control>,
}
type Body = Full<Bytes>;
fn response(status: StatusCode, text: &str) -> Response<Body> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("cache-control", "no-store")
        .header("connection", "close")
        .body(Full::new(Bytes::copy_from_slice(text.as_bytes())))
        .unwrap()
}
async fn connection(
    stream: TcpStream,
    shared: Arc<Shared>,
    mut stop: watch::Receiver<bool>,
) -> Option<CallSummary> {
    let (tx, mut rx) = oneshot::channel();
    let tx = Arc::new(Mutex::new(Some(tx)));
    let state = shared.clone();
    let service = service_fn(move |req| {
        let state = state.clone();
        let tx = tx.clone();
        async move { Ok::<_, Infallible>(handle(req, state, tx).await) }
    });
    let mut builder = hyper::server::conn::http1::Builder::new();
    builder
        .keep_alive(true)
        .max_buf_size(32 * 1024)
        .timer(TokioTimer::new())
        .header_read_timeout(shared.config.connect_timeout);
    let http = builder
        .serve_connection(TokioIo::new(stream), service)
        .with_upgrades();
    let result = tokio::select! { biased;_=stop.changed()=>Err(TwilioError::Deadline),r=tokio::time::timeout(shared.config.connect_timeout,http)=>match r {Ok(Ok(()))=>Ok(()),_=>Err(TwilioError::Transport)} };
    let Ok(upgrade) = rx.try_recv() else {
        return None;
    };
    let call = upgrade.call.clone();
    let mut summary = if result.is_ok() {
        session::run(upgrade, shared.clone()).await
    } else {
        let mut s = CallSummary::new(call.clone());
        s.error = result.err();
        s
    };
    {
        let mut state = shared.state.lock().unwrap();
        state.active.remove(&call);
        state
            .recent
            .insert(call, Instant::now() + shared.config.replay_retention);
    }
    // No inbound metadata or URLs are logged, including capability values.
    if !summary.verified_start {
        summary.generated_context = Context::default();
    }
    Some(summary)
}
async fn handle(
    mut req: Request<Incoming>,
    shared: Arc<Shared>,
    upgrade_tx: Arc<Mutex<Option<oneshot::Sender<Upgrade>>>>,
) -> Response<Body> {
    if req.uri().query().is_some() {
        return response(StatusCode::BAD_REQUEST, "query is unsupported");
    }
    let path = req.uri().path().to_owned();
    if req.method() == hyper::Method::POST && path == "/twilio/voice" {
        let content_types: Vec<_> = req.headers().get_all("content-type").iter().collect();
        if content_types.len() != 1
            || content_types[0].to_str().ok() != Some("application/x-www-form-urlencoded")
        {
            return response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "form content type required",
            );
        }
        let (parts, body) = req.into_parts();
        let bytes = match tokio::time::timeout(
            shared.config.io_timeout,
            Limited::new(body, shared.config.max_form_bytes).collect(),
        )
        .await
        {
            Ok(Ok(body)) => body.to_bytes(),
            _ => return response(StatusCode::PAYLOAD_TOO_LARGE, "invalid or oversized form"),
        };
        let fields = match security::parse_form(&bytes) {
            Ok(fields) => fields,
            Err(_) => return response(StatusCode::BAD_REQUEST, "invalid form"),
        };
        if !security::verify_signature(
            &shared.config.auth_token,
            &shared.config.webhook_url,
            &fields,
            &parts.headers,
        ) {
            return response(StatusCode::FORBIDDEN, "request rejected");
        }
        let Some(call) = fields
            .get("CallSid")
            .filter(|s| security::valid_sid(s, "CA"))
        else {
            return response(StatusCode::BAD_REQUEST, "invalid call identity");
        };
        if fields.get("AccountSid") != Some(&shared.config.account_sid) {
            return response(StatusCode::FORBIDDEN, "account rejected");
        }
        let token = {
            let mut state = shared.state.lock().unwrap();
            state.prune();
            if state.closed
                || state.active.len() + state.pending.len() >= shared.config.max_calls
                || state.recent.len() + state.active.len() + state.pending.len()
                    >= shared.config.max_calls * 4
            {
                return response(StatusCode::SERVICE_UNAVAILABLE, "admission closed");
            }
            if state.active.contains_key(call)
                || state.recent.contains_key(call)
                || state.pending.values().any(|p| &p.call == call)
            {
                return response(StatusCode::CONFLICT, "call already admitted");
            }
            let token = format!(
                "{}{}",
                uuid::Uuid::new_v4().simple(),
                uuid::Uuid::new_v4().simple()
            );
            state.pending.insert(
                token.clone(),
                Pending {
                    call: call.clone(),
                    expires: Instant::now() + shared.config.connect_timeout,
                },
            );
            token
        };
        // Configuration forbids query/fragment; escape every XML-sensitive character anyway.
        let url = format!("{}/{token}", shared.config.media_url)
            .replace('&', "&amp;")
            .replace('"', "&quot;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('\'', "&apos;");
        let xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Response><Connect><Stream url=\"{url}\" /></Connect></Response>"
        );
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/xml; charset=utf-8")
            .header("cache-control", "no-store")
            .header("connection", "close")
            .body(Full::new(Bytes::from(xml)))
            .unwrap();
    }
    if req.method() != hyper::Method::GET {
        return response(StatusCode::NOT_FOUND, "not found");
    }
    let Some(token) = path
        .strip_prefix("/twilio/media/")
        .filter(|t| t.len() == 64 && t.bytes().all(|c| c.is_ascii_hexdigit()))
    else {
        return response(StatusCode::NOT_FOUND, "not found");
    };
    let canonical = format!("{}/{token}", shared.config.media_url);
    if !security::verify_signature(
        &shared.config.auth_token,
        &canonical,
        &BTreeMap::new(),
        req.headers(),
    ) {
        return response(StatusCode::FORBIDDEN, "request rejected");
    }
    let headers_request = {
        let mut r = Request::new(());
        *r.method_mut() = req.method().clone();
        *r.uri_mut() = req.uri().clone();
        *r.version_mut() = req.version();
        *r.headers_mut() = req.headers().clone();
        r
    };
    let handshake = match tokio_tungstenite::tungstenite::handshake::server::create_response(
        &headers_request,
    ) {
        Ok(r) => r,
        Err(_) => return response(StatusCode::BAD_REQUEST, "invalid upgrade"),
    };
    let (call, control) = {
        let mut state = shared.state.lock().unwrap();
        state.prune();
        if state.closed {
            return response(StatusCode::SERVICE_UNAVAILABLE, "admission closed");
        }
        let Some(pending) = state.pending.remove(token) else {
            return response(StatusCode::FORBIDDEN, "capability rejected");
        };
        let (signal, control) = watch::channel(Control::Running);
        state.active.insert(pending.call.clone(), signal);
        (pending.call, control)
    };
    let on_upgrade = hyper::upgrade::on(&mut req);
    let sent = upgrade_tx.lock().unwrap().take().is_some_and(|tx| {
        tx.send(Upgrade {
            on_upgrade,
            call: call.clone(),
            control,
        })
        .is_ok()
    });
    if !sent {
        let mut state = shared.state.lock().unwrap();
        state.active.remove(&call);
        state
            .recent
            .insert(call, Instant::now() + shared.config.replay_retention);
        return response(StatusCode::SERVICE_UNAVAILABLE, "upgrade unavailable");
    }
    handshake.map(|_| Full::new(Bytes::new()))
}
async fn hangup(shared: &Shared, call: &str) -> Result<(), TwilioError> {
    let url = format!(
        "{}/Accounts/{}/Calls/{call}.json",
        shared.config.api_base, shared.config.account_sid
    );
    let mut response = shared
        .hangup_client
        .post(url)
        .basic_auth(&shared.config.account_sid, Some(&shared.config.auth_token))
        .form(&[("Status", "completed")])
        .send()
        .await
        .map_err(|e| {
            if e.is_timeout() {
                TwilioError::HangupTimeout
            } else {
                TwilioError::Hangup
            }
        })?;
    if response.status().as_u16() == 200 {
        return Ok(());
    }
    let status = response.status().as_u16();
    if status == 404 {
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| {
            if e.is_timeout() {
                TwilioError::HangupTimeout
            } else {
                TwilioError::Hangup
            }
        })? {
            if body.len() + chunk.len() > 8192 {
                return Err(TwilioError::HangupStatus(status));
            }
            body.extend_from_slice(&chunk);
        }
        if serde_json::from_slice::<serde_json::Value>(&body).is_ok_and(|v| v["code"] == 20404) {
            return Ok(());
        }
    }
    Err(TwilioError::HangupStatus(status))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    fn providers() -> ProviderConfig {
        ProviderConfig {
            deepgram_key: "fake-stt".into(),
            openai_key: "fake-llm".into(),
            cartesia_key: "fake-tts".into(),
            deepgram_endpoint: "ws://127.0.0.1:9/v1/listen".into(),
            openai_endpoint: "http://127.0.0.1:9/v1/chat/completions".into(),
            cartesia_endpoint: "ws://127.0.0.1:9/tts/websocket".into(),
            model: "fake-model".into(),
            voice_id: "fake-voice".into(),
            system_prompt: "Be brief".into(),
            input_rate: 16000,
            output_rate: 24000,
        }
    }
    async fn app(base: String) -> TwilioApp {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut config = TwilioConfig::new(
            format!("AC{}", "0".repeat(32)),
            "fake-secret".into(),
            format!("http://{addr}/twilio/voice"),
            format!("ws://{addr}/twilio/media"),
        );
        config.api_base = base;
        config.io_timeout = Duration::from_millis(100);
        TwilioApp::new(listener, config, providers()).unwrap()
    }
    #[tokio::test]
    async fn hangup_exact_request_bounded_status_and_no_redirect_contract() {
        use base64::Engine;
        for (status, body, expected) in [
            (200, "{}", None),
            (404, "{\"code\":20404}", None),
            (404, "{}", Some(TwilioError::HangupStatus(404))),
            (500, "{}", Some(TwilioError::HangupStatus(500))),
            (302, "{}", Some(TwilioError::HangupStatus(302))),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let redirect = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let redirect_addr = redirect.local_addr().unwrap();
            let app = app(format!("http://{addr}/2010-04-01")).await;
            let peer = tokio::spawn(async move {
                let (mut socket, peer) = listener.accept().await.unwrap();
                assert!(peer.ip().is_loopback());
                let mut bytes = vec![];
                let mut buffer = [0; 1024];
                loop {
                    let n = socket.read(&mut buffer).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&buffer[..n]);
                    if bytes
                        .windows(b"Status=completed".len())
                        .any(|w| w == b"Status=completed")
                    {
                        break;
                    }
                    assert!(bytes.len() < 8192);
                }
                let text = String::from_utf8(bytes).unwrap();
                assert!(text.starts_with(&format!(
                    "POST /2010-04-01/Accounts/AC{}/Calls/CA{}.json HTTP/1.1\r\n",
                    "0".repeat(32),
                    "1".repeat(32)
                )));
                let auth = base64::engine::general_purpose::STANDARD
                    .encode(format!("AC{}:fake-secret", "0".repeat(32)));
                assert!(text.to_ascii_lowercase().contains(&format!(
                    "authorization: basic {}",
                    auth.to_ascii_lowercase()
                )));
                assert!(text.ends_with("Status=completed"));
                socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: keep-alive\r\nLocation: http://{redirect_addr}/must-not-follow\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
                assert_eq!(
                    tokio::time::timeout(Duration::from_millis(500), socket.read(&mut [0; 1]))
                        .await
                        .unwrap()
                        .unwrap(),
                    0,
                    "one-shot hangup must not retain an idle HTTP socket"
                );
            });
            let result = hangup(&app.owner.shared, &format!("CA{}", "1".repeat(32))).await;
            assert_eq!(result.err(), expected);
            peer.await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(20), redirect.accept())
                    .await
                    .is_err()
            );
            assert!(!app.shutdown().await.unwrap().failed_cleanup());
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let app = app(format!(
            "http://{}/2010-04-01",
            listener.local_addr().unwrap()
        ))
        .await;
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut b = [0; 2048];
            let _ = socket.read(&mut b).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        });
        assert_eq!(
            hangup(&app.owner.shared, &format!("CA{}", "1".repeat(32))).await,
            Err(TwilioError::HangupTimeout)
        );
        peer.await.unwrap();
        app.shutdown().await.unwrap();
    }
    #[tokio::test]
    async fn simultaneous_and_repeated_shutdown_join_the_same_owner() {
        let app = app("http://127.0.0.1:9/2010-04-01".into()).await;
        let waiter = {
            let app = app.clone();
            tokio::spawn(async move { app.shutdown().await })
        };
        waiter.abort();
        let _ = waiter.await;
        let (one, two) = tokio::join!(app.shutdown(), app.shutdown());
        assert!(!one.unwrap().failed_cleanup());
        assert!(!two.unwrap().failed_cleanup());
        assert_eq!(app.active_sessions(), 0);
        assert_eq!(app.pending_sessions(), 0);
        assert!(!app.shutdown().await.unwrap().failed_cleanup());
    }
    #[tokio::test]
    async fn failed_supervisor_result_is_cached_across_shutdown_callers() {
        let app = app("http://127.0.0.1:9/2010-04-01".into()).await;
        app.owner.task.lock().await.as_ref().unwrap().abort();
        assert!(matches!(app.shutdown().await, Err(TwilioError::Task)));
        assert!(matches!(app.shutdown().await, Err(TwilioError::Task)));
    }
}
