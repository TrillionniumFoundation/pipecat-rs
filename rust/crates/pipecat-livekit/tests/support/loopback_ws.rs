//! A real TCP/WebSocket fixture transport, restricted to the existing loopback
//! proxy exclusion. It does not replace SDK signaling or fabricate server replies.
use std::sync::{Arc, Mutex as StdMutex, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use livekit_net::{Header, TransportError, WsClient, WsConnectResult, WsConnection};
use tokio::{net::TcpStream, sync::Mutex, time::timeout};
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, Message},
    WebSocketStream,
};

type Socket = WebSocketStream<TcpStream>;

#[derive(Default)]
pub struct LoopbackClient {
    connections: StdMutex<Vec<Weak<LoopbackConnection>>>,
}
impl LoopbackClient {
    pub fn live_connections(&self) -> usize {
        self.connections
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.strong_count() > 0)
            .count()
    }
    pub async fn assert_released(&self) {
        timeout(Duration::from_secs(3), async {
            while self.live_connections() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("all owned signaling socket references must be released");
    }
}
fn error(e: impl std::fmt::Display) -> TransportError {
    TransportError::Connection(e.to_string())
}

#[async_trait]
impl WsClient for LoopbackClient {
    async fn connect(
        &self,
        raw_url: String,
        headers: Vec<Header>,
        timeout_ms: u64,
    ) -> Result<WsConnectResult, TransportError> {
        let url = url::Url::parse(&raw_url).map_err(error)?;
        if url.scheme() != "ws"
            || url.host_str() != Some("127.0.0.1")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(TransportError::Other(
                "fixture accepts only plain loopback WebSockets".into(),
            ));
        }
        let proxy_set = [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ]
        .iter()
        .any(|key| std::env::var(key).is_ok_and(|s| !s.is_empty()));
        let excluded = ["NO_PROXY", "no_proxy"]
            .iter()
            .filter_map(|key| std::env::var(key).ok())
            .any(|s| s.split(',').any(|v| v.trim() == "127.0.0.1"));
        if proxy_set && !excluded {
            return Err(TransportError::Other(
                "existing NO_PROXY must explicitly exclude 127.0.0.1".into(),
            ));
        }
        let connect = async {
            let stream =
                TcpStream::connect(("127.0.0.1", url.port_or_known_default().unwrap_or(80)))
                    .await
                    .map_err(error)?;
            if !stream.peer_addr().map_err(error)?.ip().is_loopback() {
                return Err(TransportError::Other("non-loopback fixture peer".into()));
            }
            let mut request = raw_url.into_client_request().map_err(error)?;
            for header in headers {
                request.headers_mut().insert(
                    tokio_tungstenite::tungstenite::http::HeaderName::from_bytes(
                        header.name.as_bytes(),
                    )
                    .map_err(error)?,
                    header.value.parse().map_err(error)?,
                );
            }
            let (socket, response) = tokio_tungstenite::client_async(request, stream)
                .await
                .map_err(error)?;
            if response.status() != 101 {
                return Err(TransportError::Http {
                    status: response.status().as_u16(),
                });
            }
            let (writer, reader) = socket.split();
            let connection = Arc::new(LoopbackConnection {
                writer: Mutex::new(writer),
                reader: Mutex::new(reader),
            });
            self.connections
                .lock()
                .unwrap()
                .push(Arc::downgrade(&connection));
            Ok(WsConnectResult { connection })
        };
        timeout(Duration::from_millis(timeout_ms.max(1)), connect)
            .await
            .map_err(|_| TransportError::Timeout)?
    }
}
struct LoopbackConnection {
    writer: Mutex<SplitSink<Socket, Message>>,
    reader: Mutex<SplitStream<Socket>>,
}
#[async_trait]
impl WsConnection for LoopbackConnection {
    async fn send(&self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.writer
            .lock()
            .await
            .send(Message::Binary(frame.into()))
            .await
            .map_err(error)
    }
    async fn recv(&self) -> Result<Option<Vec<u8>>, TransportError> {
        let mut reader = self.reader.lock().await;
        loop {
            match reader.next().await {
                Some(Ok(Message::Binary(data))) => return Ok(Some(data.to_vec())),
                Some(Ok(Message::Ping(data))) => self
                    .writer
                    .lock()
                    .await
                    .send(Message::Pong(data))
                    .await
                    .map_err(error)?,
                Some(Ok(Message::Close(_))) | None => return Ok(None),
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(error(e)),
            }
        }
    }
    async fn close(&self) {
        let _ = timeout(Duration::from_secs(1), async {
            self.writer.lock().await.close().await
        })
        .await;
    }
}
