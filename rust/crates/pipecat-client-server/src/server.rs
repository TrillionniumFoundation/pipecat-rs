//! Deliberately loopback-only HTTP hosting for native client template routes.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use crate::{ClientProxy, ProxyError, SessionMethod};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
    body::Incoming,
    header::{CONTENT_TYPE, ORIGIN},
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use std::{convert::Infallible, future::Future, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::Semaphore, task::JoinSet};

/// Native bot lifecycle handler or fixed upstream proxy. Implementations must
/// own spawned work and make dropped requests cancellation-safe. HTTP hosting
/// supplies body/origin/deadline bounds; handlers enforce session ownership.
#[async_trait::async_trait]
pub trait BotApi: Send + Sync {
    fn max_request_bytes(&self) -> usize;
    async fn start(&self, body: &serde_json::Value) -> Result<serde_json::Value, ProxyError>;
    async fn session(
        &self,
        method: SessionMethod,
        session_id: &str,
        path: &[&str],
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, ProxyError>;
}
#[async_trait::async_trait]
impl BotApi for ClientProxy {
    fn max_request_bytes(&self) -> usize {
        self.limits.request_bytes
    }
    async fn start(&self, body: &serde_json::Value) -> Result<serde_json::Value, ProxyError> {
        ClientProxy::start(self, body).await
    }
    async fn session(
        &self,
        method: SessionMethod,
        session_id: &str,
        path: &[&str],
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, ProxyError> {
        ClientProxy::session(self, method, session_id, path, body).await
    }
}

type Body = Full<Bytes>;
/// Only the native browser build's five public output files can be served.
#[derive(Clone)]
pub struct AssetBundle(std::collections::BTreeMap<&'static str, (&'static str, Bytes)>);
impl std::fmt::Debug for AssetBundle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssetBundle")
            .field("files", &self.0.len())
            .finish()
    }
}
impl AssetBundle {
    /// Replace only the public browser configuration, without mutating build
    /// files. Never put upstream credentials here: this asset is served to the
    /// browser. The serialized object is limited to 16 KiB.
    pub fn with_browser_config(mut self, value: serde_json::Value) -> Result<Self, ServerError> {
        if !value.is_object() {
            return Err(ServerError::Assets);
        }
        let mut writer = crate::LimitedWriter {
            bytes: Vec::new(),
            limit: 16 * 1024,
        };
        serde_json::to_writer(&mut writer, &value).map_err(|_| ServerError::Assets)?;
        self.0.insert(
            "/config.json",
            ("application/json", Bytes::from(writer.bytes)),
        );
        Ok(self)
    }
    /// Canonical paths must remain within the supplied build directory. Reads are
    /// bounded and happen only at startup, never from browser-supplied paths.
    pub fn from_directory(directory: &std::path::Path) -> Result<Self, ServerError> {
        use std::io::Read;
        let root = directory.canonicalize().map_err(|_| ServerError::Assets)?;
        let mut files = std::collections::BTreeMap::new();
        let mut total = 0usize;
        for (route, name, mime) in [
            ("/", "index.html", "text/html; charset=utf-8"),
            ("/style.css", "style.css", "text/css; charset=utf-8"),
            ("/config.json", "config.json", "application/json"),
            (
                "/pkg/pipecat_browser_client.js",
                "pkg/pipecat_browser_client.js",
                "text/javascript; charset=utf-8",
            ),
            (
                "/pkg/pipecat_browser_client_bg.wasm",
                "pkg/pipecat_browser_client_bg.wasm",
                "application/wasm",
            ),
        ] {
            let path = root
                .join(name)
                .canonicalize()
                .map_err(|_| ServerError::Assets)?;
            if !path.starts_with(&root) {
                return Err(ServerError::Assets);
            }
            let mut bytes = Vec::new();
            let remaining = (32usize << 20).saturating_sub(total);
            std::fs::File::open(path)
                .map_err(|_| ServerError::Assets)?
                .take(remaining as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| ServerError::Assets)?;
            if bytes.len() > remaining {
                return Err(ServerError::Assets);
            }
            total += bytes.len();
            files.insert(route, (mime, Bytes::from(bytes)));
        }
        Ok(Self(files))
    }
}
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Optional fixed, preloaded browser bundle. No request-time filesystem access.
    pub assets: Option<Arc<AssetBundle>>,
    /// Exact browser origins. This is CSRF protection, not user authentication.
    pub allowed_origins: Vec<String>,
    pub allow_missing_origin: bool,
    pub max_connections: usize,
    pub request_deadline: Duration,
}
impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            assets: None,
            allowed_origins: vec![],
            allow_missing_origin: false,
            max_connections: 32,
            request_deadline: Duration::from_secs(30),
        }
    }
}
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("invalid or oversized browser asset bundle")]
    Assets,
    #[error("server requires a loopback listener and bounded valid configuration")]
    Configuration,
    #[error("listener failed")]
    Listener,
}
/// Serve one request per connection; shutdown aborts owned connections and joins
/// their tasks before returning. Dropping this future schedules task aborts;
/// await shutdown when joined cleanup is required. Caller owns listener binding.
pub async fn serve(
    listener: TcpListener,
    proxy: ClientProxy,
    config: ServerConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ServerError> {
    serve_with_api(listener, Arc::new(proxy), config, shutdown).await
}
/// Serve the same bounded routes with a native bot implementation instead of an
/// HTTP proxy. This does not expose a listener outside the loopback boundary.
pub async fn serve_with_api(
    listener: TcpListener,
    proxy: Arc<dyn BotApi>,
    config: ServerConfig,
    shutdown: impl Future<Output = ()>,
) -> Result<(), ServerError> {
    if proxy.max_request_bytes() == 0 {
        return Err(ServerError::Configuration);
    }
    if !listener
        .local_addr()
        .map_err(|_| ServerError::Listener)?
        .ip()
        .is_loopback()
        || config.max_connections == 0
        || config.max_connections > 65536
        || config.request_deadline.is_zero()
        || config.allowed_origins.iter().any(|s| !valid_origin(s))
    {
        return Err(ServerError::Configuration);
    }
    let semaphore = Arc::new(Semaphore::new(config.max_connections));
    let mut tasks = JoinSet::new();
    tokio::pin!(shutdown);
    let result = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = listener.accept() => {
                let (stream, _) = match accepted { Ok(pair) => pair, Err(_) => break Err(ServerError::Listener) };
                let permit = match semaphore.clone().try_acquire_owned() { Ok(permit) => permit, Err(_) => { drop(stream); continue; } };
                let proxy = proxy.clone();
                let config = config.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    let deadline = config.request_deadline;
                    let service = service_fn(move |request| {
                        let proxy = proxy.clone(); let config = config.clone();
                        async move { Ok::<_, Infallible>(handle(request, proxy.as_ref(), &config).await) }
                    });
                    let mut builder = hyper::server::conn::http1::Builder::new();
                    builder.keep_alive(false).max_headers(64).max_buf_size(16384);
                    let _ = tokio::time::timeout(deadline, builder.serve_connection(TokioIo::new(stream), service)).await;
                });
            }
        }
    };
    tasks.abort_all();
    while tasks.join_next().await.is_some() {}
    result
}
fn valid_origin(text: &str) -> bool {
    reqwest::Url::parse(text).is_ok_and(|u| {
        matches!(u.scheme(), "http" | "https")
            && u.host_str().is_some()
            && u.username().is_empty()
            && u.password().is_none()
            && u.query().is_none()
            && u.fragment().is_none()
            && u.path() == "/"
            && u.origin().ascii_serialization() == text
    })
}
fn error(status: StatusCode, message: &str) -> Response<Body> {
    response(status, serde_json::json!({"error":message}))
}
fn response(status: StatusCode, value: serde_json::Value) -> Response<Body> {
    let mut response = Response::new(Full::new(Bytes::from(value.to_string())));
    *response.status_mut() = status;
    response.headers_mut().insert(
        CONTENT_TYPE,
        hyper::header::HeaderValue::from_static("application/json"),
    );
    response.headers_mut().insert(
        hyper::header::CACHE_CONTROL,
        hyper::header::HeaderValue::from_static("no-store"),
    );
    response.headers_mut().insert(
        hyper::header::X_CONTENT_TYPE_OPTIONS,
        hyper::header::HeaderValue::from_static("nosniff"),
    );
    response
}
async fn handle(
    mut request: Request<Incoming>,
    proxy: &dyn BotApi,
    config: &ServerConfig,
) -> Response<Body> {
    if request.method() == Method::GET && request.uri().query().is_none() {
        if let Some((mime, bytes)) = config
            .assets
            .as_ref()
            .and_then(|assets| assets.0.get(request.uri().path()))
        {
            let mut response = Response::new(Full::new(bytes.clone()));
            response
                .headers_mut()
                .insert(CONTENT_TYPE, hyper::header::HeaderValue::from_static(mime));
            response.headers_mut().insert(
                hyper::header::CACHE_CONTROL,
                hyper::header::HeaderValue::from_static("no-store"),
            );
            response.headers_mut().insert(
                hyper::header::X_CONTENT_TYPE_OPTIONS,
                hyper::header::HeaderValue::from_static("nosniff"),
            );
            return response;
        }
    }
    let origins: Vec<_> = request.headers().get_all(ORIGIN).iter().collect();
    let origin_ok = match origins.as_slice() {
        [] => config.allow_missing_origin,
        [value] => value
            .to_str()
            .is_ok_and(|value| config.allowed_origins.iter().any(|origin| origin == value)),
        _ => false,
    };
    if !origin_ok {
        return error(StatusCode::FORBIDDEN, "origin denied");
    }
    if request.uri().query().is_some() {
        return error(StatusCode::BAD_REQUEST, "query parameters are unsupported");
    }
    let path = request.uri().path().to_owned();
    let method = request.method().clone();
    if (path == "/api/start" && method != Method::POST)
        || (path.starts_with("/api/sessions/") && method != Method::POST && method != Method::PATCH)
    {
        return error(StatusCode::METHOD_NOT_ALLOWED, "method denied");
    }
    if path != "/api/start" && !path.starts_with("/api/sessions/") {
        return error(StatusCode::NOT_FOUND, "route not found");
    }
    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .unwrap_or("")
        .trim();
    if !content_type.eq_ignore_ascii_case("application/json") {
        return error(StatusCode::UNSUPPORTED_MEDIA_TYPE, "JSON body required");
    }
    let mut bytes = Vec::new();
    while let Some(frame) = request.body_mut().frame().await {
        let frame = match frame {
            Ok(frame) => frame,
            Err(_) => return error(StatusCode::BAD_REQUEST, "invalid request body"),
        };
        if let Some(data) = frame.data_ref() {
            if data.len() > proxy.max_request_bytes().saturating_sub(bytes.len()) {
                return error(StatusCode::PAYLOAD_TOO_LARGE, "request body too large");
            }
            bytes.extend_from_slice(data);
        }
    }
    let body = match serde_json::from_slice(&bytes) {
        Ok(body) => body,
        Err(_) => return error(StatusCode::BAD_REQUEST, "invalid JSON body"),
    };
    let result = if path == "/api/start" {
        proxy.start(&body).await
    } else {
        let mut segments = Vec::new();
        for part in path.strip_prefix("/api/sessions/").unwrap_or("").split('/') {
            let Ok(decoded) = percent_encoding::percent_decode_str(part).decode_utf8() else {
                return error(StatusCode::BAD_REQUEST, "invalid session path");
            };
            segments.push(decoded.into_owned());
        }
        if segments.len() < 2 {
            return error(StatusCode::BAD_REQUEST, "invalid session path");
        }
        let tail: Vec<_> = segments[1..].iter().map(String::as_str).collect();
        if crate::validate_session_path(&segments[0], &tail).is_err() {
            return error(StatusCode::BAD_REQUEST, "invalid session path");
        }
        proxy
            .session(
                if method == Method::POST {
                    SessionMethod::Post
                } else {
                    SessionMethod::Patch
                },
                &segments[0],
                &tail,
                &body,
            )
            .await
    };
    match result {
        Ok(value) => response(StatusCode::OK, value),
        Err(ProxyError::InvalidRequest) => error(StatusCode::BAD_REQUEST, "invalid bot request"),
        Err(ProxyError::Busy) => error(StatusCode::TOO_MANY_REQUESTS, "bot capacity is full"),
        Err(ProxyError::Closed) => error(StatusCode::SERVICE_UNAVAILABLE, "bot service is closed"),
        Err(ProxyError::Path) => error(StatusCode::BAD_REQUEST, "invalid session path"),
        Err(ProxyError::RequestTooLarge) => {
            error(StatusCode::PAYLOAD_TOO_LARGE, "request body too large")
        }
        Err(ProxyError::Deadline) => {
            error(StatusCode::GATEWAY_TIMEOUT, "upstream deadline exceeded")
        }
        Err(_) => error(StatusCode::BAD_GATEWAY, "upstream request failed"),
    }
}
