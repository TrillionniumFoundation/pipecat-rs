//! Direct private-wrapper error identity; separate from public runner summaries.
use super::*;
use std::{net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::tungstenite::{error::ProtocolError, Error as WsError};
type Outcome<T = ()> = Result<T, String>;
macro_rules! get {
    ($v:expr) => {
        $v.map_err(|error| format!("{error:?}"))?
    };
}
#[derive(Debug, Clone, Copy)]
enum Ending {
    Graceful,
    Unmasked,
    Abrupt,
}
struct Pair {
    client: TcpStream,
    session: Option<BrowserSession>,
    address: SocketAddr,
}
impl Pair {
    async fn new() -> Outcome<Self> {
        let listener = get!(TcpListener::bind("127.0.0.1:0").await);
        let address = get!(listener.local_addr());
        let mut client = get!(TcpStream::connect(address).await);
        get!(client.write_all(format!("GET / HTTP/1.1\r\nHost: {address}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").as_bytes()).await);
        let (stream, _) = get!(listener.accept().await);
        let session = get!(
            WebSocketSession::accept(stream, pipecat_transports::SessionConfig::default()).await
        );
        // No fallible/awaited step after the session starts its owned task.
        Ok(Self {
            client,
            session: Some(BrowserSession::WebSocket(session)),
            address,
        })
    }
    async fn observe(&mut self, ending: Ending) -> Outcome {
        let mut header = vec![];
        while !header.ends_with(b"\r\n\r\n") {
            if header.len() >= 16384 {
                return Err("upgrade budget".into());
            }
            let mut byte = [0];
            get!(self.client.read_exact(&mut byte).await);
            header.push(byte[0]);
        }
        if !header.starts_with(b"HTTP/1.1 101 ") {
            return Err("upgrade failed".into());
        }
        match ending {
            Ending::Graceful => get!(self.client.write_all(&[0x88, 0x80, 1, 2, 3, 4]).await),
            Ending::Unmasked => get!(self.client.write_all(&[0x82, 0]).await),
            Ending::Abrupt => get!(self.client.shutdown().await),
        }
        if self.session.as_mut().unwrap().recv_frame().await.is_some() {
            return Err("fault became an application frame".into());
        }
        Ok(())
    }
}
async fn run(ending: Ending) {
    let mut pair = tokio::time::timeout(Duration::from_secs(5), Pair::new())
        .await
        .expect("raw pair setup deadline; no owned task survives a cancelled setup")
        .expect("owned raw/native pair construction");
    let observation = tokio::time::timeout(Duration::from_secs(5), pair.observe(ending)).await;
    let finish = tokio::time::timeout(
        Duration::from_secs(5),
        pair.session.take().unwrap().finish(),
    )
    .await;
    let terminal = tokio::time::timeout(Duration::from_secs(2), async {
        let mut bytes = vec![];
        let mut chunk = [0u8; 16];
        loop {
            match pair.client.read(&mut chunk).await {
                Ok(0) => break Ok(bytes),
                Ok(count) => {
                    bytes.extend_from_slice(&chunk[..count]);
                    if bytes.len() > 16 {
                        break Err("terminal byte budget".to_owned());
                    }
                }
                Err(error)
                    if error.kind() == std::io::ErrorKind::ConnectionReset
                        && !matches!(ending, Ending::Graceful) =>
                {
                    break Ok(bytes)
                }
                Err(error) => break Err(format!("terminal {error:?}")),
            }
        }
    })
    .await;
    drop(pair.client);
    let released = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if let Ok(listener) = TcpListener::bind(pair.address).await {
                drop(listener);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    eprintln!("direct-finish ending={ending:?}; observation={observation:?}; finish={finish:?}; terminal={terminal:?}; listener_released={released:?}");
    assert!(matches!(observation, Ok(Ok(()))), "{observation:?}");
    assert!(released.is_ok());
    let bytes = terminal
        .expect("terminal deadline")
        .expect("terminal socket result");
    match ending {
        Ending::Graceful => {
            assert!(matches!(finish, Ok(Ok(()))), "{finish:?}");
            assert_eq!(bytes, vec![0x88, 0]);
        }
        Ending::Unmasked => {
            assert!(matches!(
                finish,
                Ok(Err(RunnerError::Transport(
                    pipecat_transports::TransportError::WebSocket(WsError::Protocol(
                        ProtocolError::UnmaskedFrameFromClient
                    ))
                )))
            ));
            assert!(bytes.is_empty());
        }
        Ending::Abrupt => {
            assert!(matches!(
                finish,
                Ok(Err(RunnerError::Transport(
                    pipecat_transports::TransportError::WebSocket(WsError::Protocol(
                        ProtocolError::ResetWithoutClosingHandshake
                    ))
                )))
            ));
            assert!(bytes.is_empty());
        }
    }
}
#[tokio::test]
async fn graceful_finish_reports_success_after_join() {
    run(Ending::Graceful).await;
}
#[tokio::test]
async fn malformed_frame_finish_returns_exact_protocol_error_after_join() {
    run(Ending::Unmasked).await;
}
#[tokio::test]
async fn abrupt_eof_finish_returns_exact_protocol_error_after_join() {
    run(Ending::Abrupt).await;
}

#[cfg(feature = "webrtc")]
#[tokio::test]
async fn native_finish_preserves_success_and_releases_gathered_sockets() {
    use pipecat_webrtc::{ConnectionState, SmallWebRtcConfig, SmallWebRtcConnection};
    use std::{net::UdpSocket, sync::Arc};
    let peer = Arc::new(
        SmallWebRtcConnection::new(SmallWebRtcConfig {
            audio_codec: None,
            localhost_only: true,
            gather_timeout: Duration::from_secs(2),
            ..Default::default()
        })
        .await
        .expect("native owner"),
    );
    let observation = tokio::time::timeout(Duration::from_secs(3), async {
        let offer = get!(peer.create_offer().await);
        let mut addresses = vec![];
        for line in offer
            .sdp
            .lines()
            .filter(|line| line.starts_with("a=candidate:"))
        {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() < 6 {
                return Err("invalid candidate".into());
            }
            addresses.push(get!(
                format!("{}:{}", fields[4], fields[5]).parse::<SocketAddr>()
            ));
        }
        if addresses.is_empty()
            || addresses
                .iter()
                .any(|address| UdpSocket::bind(address).is_ok())
        {
            return Err("candidate socket not held".into());
        }
        Ok::<_, String>(addresses)
    })
    .await;
    let finish = tokio::time::timeout(
        Duration::from_secs(6),
        BrowserSession::WebRtc(peer.clone()).finish(),
    )
    .await;
    let cached = tokio::time::timeout(Duration::from_secs(2), peer.close()).await;
    let released = if let Ok(Ok(addresses)) = &observation {
        tokio::time::timeout(Duration::from_secs(2), async {
            while addresses
                .iter()
                .any(|address| UdpSocket::bind(address).is_err())
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok()
    } else {
        false
    };
    eprintln!("native-wrapper observation={observation:?}; finish={finish:?}; cached={cached:?}; state={:?}; sockets_released={released}",peer.state());
    assert!(matches!(observation, Ok(Ok(_))));
    assert!(matches!(finish, Ok(Ok(()))));
    assert!(matches!(cached, Ok(Ok(()))));
    assert_eq!(peer.state(), ConnectionState::Closed);
    assert!(released);
}
