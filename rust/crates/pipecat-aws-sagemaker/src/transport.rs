//! The socket, TLS, HTTP1 driver, decoder and output wait share exactly one
//! pipeline-owned invocation future. No driver/resolver/pool task is detached.
use crate::{
    processor::{InvocationStats, Shared},
    protocol::Pcm,
    Error, SageMakerConfig,
};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{client::conn::http1, Request};
use hyper_util::rt::TokioIo;
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, ProcessorContext, TtsStartedFrame, TtsStoppedFrame,
};
use std::{
    future::Future,
    net::IpAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{timeout, Instant},
};
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}
async fn emit(
    ctx: &ProcessorContext,
    data: FrameData,
    deadline: Duration,
    shared: &Shared,
    stats: &Arc<Mutex<InvocationStats>>,
) -> Result<(), Error> {
    let bytes = if let FrameData::TtsAudio(a) = &data {
        a.audio.len()
    } else {
        0
    };
    let frame = Frame::new(data);
    let mut ticket = shared.output.begin(frame.id, bytes, stats.clone())?;
    timeout(deadline, async {
        ctx.push(frame, Direction::Downstream)
            .await
            .map_err(|_| Error::Output)?;
        ticket.admitted();
        ticket.wait().await
    })
    .await
    .map_err(|_| Error::Timeout("caller output"))?
}
pub(crate) struct Invocation {
    pub body: Vec<u8>,
    pub rate: u32,
    pub sequence: u64,
    pub append_to_context: bool,
    pub text_characters: u64,
}
pub(crate) async fn invoke(
    config: &SageMakerConfig,
    invocation: Invocation,
    ctx: &ProcessorContext,
    shared: &Shared,
    stats: &Arc<Mutex<InvocationStats>>,
) -> Result<(), Error> {
    let context_id = format!("sagemaker-{}", invocation.sequence);
    emit(
        ctx,
        FrameData::TtsStarted(TtsStartedFrame {
            context_id: Some(context_id.clone()),
            append_to_context: invocation.append_to_context,
        }),
        config.limits().output_timeout,
        shared,
        stats,
    )
    .await?;
    let text_characters = invocation.text_characters;
    let result = timeout(
        config.limits().total_timeout,
        invoke_inner(config, invocation, &context_id, ctx, shared, stats),
    )
    .await
    .unwrap_or(Err(Error::Timeout("absolute invocation")));
    shared.progress.send_modify(|p| {
        p.usage_text_characters += text_characters;
        if let Err(error) = &result {
            if p.primary.is_none() {
                p.primary = Some(error.clone());
            }
        }
    });
    // A stopped context closes its lifecycle even on provider/protocol failure.
    // It is not an audio-success receipt. Cancellation drops this entire future.
    let stopped = emit(
        ctx,
        FrameData::TtsStopped(TtsStoppedFrame {
            context_id: Some(context_id),
        }),
        config.limits().output_timeout,
        shared,
        stats,
    )
    .await;
    match (result, stopped) {
        (Err(primary), Err(secondary)) => {
            shared
                .cleanup
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push(secondary);
            Err(primary)
        }
        (Err(primary), Ok(())) => Err(primary),
        (Ok(()), result) => result,
    }
}
async fn invoke_inner(
    config: &SageMakerConfig,
    invocation: Invocation,
    context_id: &str,
    ctx: &ProcessorContext,
    shared: &Shared,
    stats: &Arc<Mutex<InvocationStats>>,
) -> Result<(), Error> {
    let started = Instant::now();
    let Invocation {
        body: payload,
        rate,
        ..
    } = invocation;
    let url = config.url()?;
    let limits = config.limits();
    let hostname = url.host_str().ok_or(Error::Invalid("hostname"))?;
    let address = if let Ok(ip) = hostname.trim_matches(['[', ']']).parse::<IpAddr>() {
        std::net::SocketAddr::new(
            ip,
            url.port_or_known_default().ok_or(Error::Invalid("port"))?,
        )
    } else {
        config
            .connect_address()
            .ok_or(Error::Invalid("explicit resolved address required"))?
    };
    let stream = timeout(limits.connect_timeout, TcpStream::connect(address))
        .await
        .map_err(|_| Error::Timeout("TCP connect"))?
        .map_err(|_| Error::Io)?;
    stream.set_nodelay(true).map_err(|_| Error::Io)?;
    let io: Box<dyn Io> = if url.scheme() == "https" {
        let roots =
            rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(|_| Error::Io)?
        .with_root_certificates(roots)
        .with_no_client_auth();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        let name = tls_server_name(hostname)?;
        let stream = timeout(
            limits.connect_timeout,
            tokio_rustls::TlsConnector::from(Arc::new(tls)).connect(name, stream),
        )
        .await
        .map_err(|_| Error::Timeout("TLS handshake"))?
        .map_err(|_| Error::Io)?;
        Box::new(stream)
    } else {
        Box::new(stream)
    };
    let (mut sender, connection) = http1::Builder::new()
        .max_headers(32)
        .max_buf_size(limits.max_header_bytes)
        .handshake(TokioIo::new(io))
        .await
        .map_err(|_| Error::Io)?;
    let headers = pipecat_aws_common::auth::signed_body_post_headers(
        &url,
        config.region(),
        config.credentials(),
        &payload,
        &pipecat_aws_common::auth::now(),
        "sagemaker",
        url.path(),
        &[("accept", "application/octet-stream")],
    )
    .map_err(|_| Error::Invalid("request signing"))?;
    let mut request = Request::post(url.path())
        .body(Full::new(Bytes::from(payload)))
        .map_err(|_| Error::Invalid("request"))?;
    *request.headers_mut() = headers;
    request
        .headers_mut()
        .insert("connection", http::HeaderValue::from_static("close"));
    let operation = async move {
        let response = timeout(limits.write_timeout, sender.send_request(request))
            .await
            .map_err(|_| Error::Timeout("request and response head"))?
            .map_err(|_| Error::Io)?;
        if response.status().as_u16() != 200 {
            return Err(Error::HttpStatus(response.status().as_u16()));
        }
        let headers = response.headers();
        let mut bytes = 0usize;
        for (name, value) in headers {
            bytes = bytes
                .saturating_add(name.as_str().len())
                .saturating_add(value.len());
            if bytes > limits.max_header_bytes {
                return Err(Error::TooLarge);
            }
        }
        for name in [
            "content-type",
            "content-length",
            "transfer-encoding",
            "content-encoding",
        ] {
            if headers.get_all(name).iter().count() > 1 {
                return Err(Error::Protocol("duplicate response header"));
            }
        }
        let mime = headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .ok_or(Error::Protocol("content type"))?;
        if !mime.trim().eq_ignore_ascii_case("application/octet-stream") {
            return Err(Error::Protocol("content type is not raw PCM"));
        }
        if let Some(encoding) = headers.get("content-encoding") {
            if encoding != "identity" {
                return Err(Error::Protocol("encoded response body"));
            }
        }
        if let Some(te) = headers.get("transfer-encoding") {
            if te != "chunked" || headers.contains_key("content-length") {
                return Err(Error::Protocol("response framing"));
            }
        }
        if let Some(length) = headers.get("content-length") {
            let length = length
                .to_str()
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or(Error::Protocol("content length"))?;
            if length > limits.max_response_bytes as u64 {
                return Err(Error::TooLarge);
            }
        }
        let mut body = response.into_body();
        let mut pcm = Pcm::new(limits);
        let mut first = true;
        loop {
            // The idle budget applies only while waiting on the peer. Caller
            // backpressure has its own deadline and does not spend network idle.
            let frame = timeout(limits.idle_timeout, body.frame())
                .await
                .map_err(|_| Error::Timeout("response idle"))?;
            match frame {
                Some(Ok(frame)) => {
                    let data = frame
                        .data_ref()
                        .ok_or(Error::Protocol("response trailers"))?;
                    if !data.is_empty() {
                        if first {
                            first = false;
                            let micros = started.elapsed().as_micros().min(u64::MAX as u128) as u64;
                            shared.progress.send_modify(|p| {
                                p.ttfb_observations += 1;
                                p.last_ttfb_micros = Some(micros);
                            });
                        }
                        shared.progress.send_modify(|p| {
                            p.wire_bytes = p.wire_bytes.saturating_add(data.len() as u64)
                        });
                    }
                    for bytes in pcm.feed(data)? {
                        let mut audio = AudioRawFrame::new(bytes, rate, 1)
                            .map_err(|_| Error::Protocol("PCM sample alignment"))?;
                        audio.context_id = Some(context_id.to_owned());
                        emit(
                            ctx,
                            FrameData::TtsAudio(audio),
                            limits.output_timeout,
                            shared,
                            stats,
                        )
                        .await?;
                    }
                }
                Some(Err(_)) => return Err(Error::Protocol("truncated HTTP response")),
                None => break,
            }
        }
        pcm.finish()?;
        drop(body);
        drop(sender);
        Ok(())
    };
    drive(connection, operation, limits.shutdown_timeout).await
}
async fn drive<C, O>(connection: C, operation: O, close: Duration) -> Result<(), Error>
where
    C: Future<Output = Result<(), hyper::Error>>,
    O: Future<Output = Result<(), Error>>,
{
    let mut connection = Box::pin(connection);
    let mut operation = Box::pin(operation);
    let mut driver_done = false;
    let result = loop {
        tokio::select! {
            result = &mut operation => break result,
            result = &mut connection, if !driver_done => {
                driver_done = true;
                result.map_err(|_| Error::Protocol("HTTP connection"))?;
            }
        }
    };
    // Destroy any request/response references before asking the driver to close.
    drop(operation);
    result?;
    if !driver_done {
        timeout(close, Pin::as_mut(&mut connection))
            .await
            .map_err(|_| Error::Timeout("connection close"))?
            .map_err(|_| Error::Protocol("HTTP connection close"))?;
    }
    Ok(())
}

fn tls_server_name(hostname: &str) -> Result<rustls::pki_types::ServerName<'static>, Error> {
    // URL host syntax brackets IPv6; TLS verifies its raw IP identity, while the
    // original bracketed URL authority remains unchanged for HTTP and signing.
    rustls::pki_types::ServerName::try_from(hostname.trim_matches(['[', ']']).to_owned())
        .map_err(|_| Error::Invalid("TLS hostname"))
}
