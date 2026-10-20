//! HTTP/1.1 connection and request futures share ONE pipeline-owned task.
//! The driver is polled directly, never spawned. Dropping that owner destroys
//! the request, decoder, timer and socket together; there is no resolver/pool.
use crate::{processor::OutputFlow, protocol::Decoder, AgentCoreConfig, Error};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{client::conn::http1, Request};
use hyper_util::rt::TokioIo;
use pipecat_core::{Direction, Frame, FrameData, ProcessorContext, TextFrame};
use std::{future::Future, net::IpAddr, pin::Pin, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::{sleep_until, timeout, Instant},
};
trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

async fn emit(
    ctx: &ProcessorContext,
    data: FrameData,
    deadline: Duration,
    flow: &OutputFlow,
) -> Result<(), Error> {
    // Core push is intentionally nonblocking. Allow just one generated frame in
    // flight and await the actual caller's processing acknowledgement, not queue
    // admission. The connection driver remains polled by `drive` during this wait.
    let frame = Frame::new(data);
    let id = frame.id;
    let mut ticket = flow.begin(id)?;
    timeout(deadline, async {
        ctx.push(frame, Direction::Downstream)
            .await
            .map_err(|_| Error::Output)?;
        ticket.wait().await
    })
    .await
    .map_err(|_| Error::Timeout("caller output"))?
}
async fn texts(
    ctx: &ProcessorContext,
    values: Vec<String>,
    open: &mut bool,
    gap: &mut Instant,
    limit: Duration,
    flow: &OutputFlow,
) -> Result<(), Error> {
    for text in values {
        if !*open {
            emit(ctx, FrameData::LlmResponseStart, limit, flow).await?;
            *open = true;
        }
        emit(ctx, FrameData::LlmText(TextFrame::llm(text)), limit, flow).await?;
        *gap = Instant::now() + Duration::from_secs(1);
    }
    Ok(())
}

pub(crate) async fn invoke(
    config: &AgentCoreConfig,
    payload: Vec<u8>,
    ctx: &ProcessorContext,
    flow: &OutputFlow,
) -> Result<(), Error> {
    timeout(
        config.limits().total_timeout,
        invoke_inner(config, payload, ctx, flow),
    )
    .await
    .map_err(|_| Error::Timeout("absolute invocation"))?
}
async fn invoke_inner(
    config: &AgentCoreConfig,
    payload: Vec<u8>,
    ctx: &ProcessorContext,
    flow: &OutputFlow,
) -> Result<(), Error> {
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
    let session = uuid::Uuid::new_v4().to_string();
    let headers = pipecat_aws_common::auth::signed_body_post_headers(
        &url,
        config.region(),
        config.credentials(),
        &payload,
        &pipecat_aws_common::auth::now(),
        "bedrock-agentcore",
        &config.canonical_uri()?,
        &[
            ("accept", "application/json, text/event-stream"),
            ("x-amzn-bedrock-agentcore-runtime-session-id", &session),
        ],
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
        let mut header_bytes = 0usize;
        for (name, value) in response.headers() {
            header_bytes = header_bytes
                .saturating_add(name.as_str().len())
                .saturating_add(value.len());
            if header_bytes > limits.max_header_bytes {
                return Err(Error::TooLarge);
            }
        }
        for name in [
            "content-type",
            "x-amzn-bedrock-agentcore-runtime-session-id",
        ] {
            if response.headers().get_all(name).iter().count() > 1 {
                return Err(Error::Protocol("duplicate response header"));
            }
        }
        if let Some(session) = response
            .headers()
            .get("x-amzn-bedrock-agentcore-runtime-session-id")
        {
            if session.is_empty()
                || session.len() > 100
                || !session.as_bytes()[0].is_ascii_alphanumeric()
                || !session
                    .as_bytes()
                    .iter()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_'))
            {
                return Err(Error::Protocol("response session shape"));
            }
        }
        let mime = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .ok_or(Error::Protocol("content type"))?;
        let mut decoder = Decoder::new(mime, limits)?;
        let mut body = response.into_body();
        let mut open = false;
        let mut gap = Instant::now() + Duration::from_secs(1);
        let mut idle = Instant::now() + limits.idle_timeout;
        loop {
            tokio::select! {
                biased;
                _ = sleep_until(idle) => return Err(Error::Timeout("response idle")),
                _ = sleep_until(gap), if open => {
                    let output_started = Instant::now();
                    emit(ctx, FrameData::LlmResponseEnd, limits.output_timeout, flow).await?;
                    // Peer-idle budget excludes locally blocked caller processing.
                    idle += Instant::now() - output_started;
                    open = false;
                }
                frame = body.frame() => match frame {
                    Some(Ok(frame)) => {
                        if let Some(data) = frame.data_ref() {
                            texts(ctx, decoder.feed(data)?, &mut open, &mut gap, limits.output_timeout, flow).await?;
                            idle = Instant::now() + limits.idle_timeout;
                            decoder.check_error()?;
                        } else { return Err(Error::Protocol("response trailers")); }
                    }
                    Some(Err(_)) => return Err(Error::Protocol("truncated HTTP response")),
                    None => break,
                }
            }
        }
        texts(
            ctx,
            decoder.finish()?,
            &mut open,
            &mut gap,
            limits.output_timeout,
            flow,
        )
        .await?;
        if open {
            emit(ctx, FrameData::LlmResponseEnd, limits.output_timeout, flow).await?;
        }
        drop(body);
        drop(sender);
        Ok(())
    };
    drive(connection, operation, limits.shutdown_timeout).await
}
/// Poll the exact connection future alongside I/O and output. Success requires
/// its natural return. Failure/cancellation synchronously destroys both futures.
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
#[cfg(test)]
mod tls_identity_tests {
    use super::*;
    #[test]
    fn literal_tls_identity_accepts_ipv4_ipv6_without_changing_regional_name() {
        assert!(matches!(
            tls_server_name("127.0.0.1").unwrap(),
            rustls::pki_types::ServerName::IpAddress(_)
        ));
        assert!(matches!(
            tls_server_name("[::1]").unwrap(),
            rustls::pki_types::ServerName::IpAddress(_)
        ));
        assert!(matches!(
            tls_server_name("bedrock-agentcore.us-east-1.amazonaws.com").unwrap(),
            rustls::pki_types::ServerName::DnsName(_)
        ));
    }
}
