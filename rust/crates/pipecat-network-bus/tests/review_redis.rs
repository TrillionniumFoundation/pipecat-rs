//! Cancellation and redirect checks against deliberately scripted loopback peers.
use pipecat_network_bus::{RedisBus, RedisConfig, RedisError};
use pipecat_workers::{BusMessage, MessageData};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::timeout,
};

async fn command(socket: &mut BufReader<TcpStream>) -> Vec<Vec<u8>> {
    let mut line = String::new();
    socket.read_line(&mut line).await.unwrap();
    let count: usize = line.strip_prefix('*').unwrap().trim().parse().unwrap();
    let mut result = vec![];
    for _ in 0..count {
        line.clear();
        socket.read_line(&mut line).await.unwrap();
        let size: usize = line.strip_prefix('$').unwrap().trim().parse().unwrap();
        let mut bytes = vec![0; size];
        socket.read_exact(&mut bytes).await.unwrap();
        let mut end = [0; 2];
        socket.read_exact(&mut end).await.unwrap();
        assert_eq!(&end, b"\r\n");
        result.push(bytes);
    }
    result
}
fn message(text: &str) -> BusMessage {
    BusMessage {
        source: "review".into(),
        target: None,
        data: MessageData::Speak {
            text: text.into(),
            append_to_context: true,
        },
    }
}
#[tokio::test]
async fn review_aborting_publish_discards_connection_and_does_not_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = RedisConfig {
        address: listener.local_addr().unwrap().to_string(),
        channel: "review".into(),
        command_timeout: Duration::from_secs(1),
        ..RedisConfig::default()
    };
    let (published, seen) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut subscriber = BufReader::new(socket);
        assert_eq!(
            command(&mut subscriber).await,
            vec![b"SUBSCRIBE".to_vec(), b"review".to_vec()]
        );
        subscriber
            .get_mut()
            .write_all(b"*3\r\n$9\r\nsubscribe\r\n$6\r\nreview\r\n:1\r\n")
            .await
            .unwrap();
        let (socket, _) = listener.accept().await.unwrap();
        let mut first = BufReader::new(socket);
        let first_publish = command(&mut first).await;
        assert_eq!(first_publish[0], b"PUBLISH");
        published.send(()).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), first.read_u8())
                .await
                .unwrap()
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        let (socket, _) = listener.accept().await.unwrap();
        let mut second = BufReader::new(socket);
        let second_publish = command(&mut second).await;
        assert_eq!(second_publish[0], b"PUBLISH");
        assert_ne!(first_publish[2], second_publish[2]);
        second.get_mut().write_all(b":7\r\n").await.unwrap();
        assert_eq!(
            timeout(Duration::from_secs(1), subscriber.read_u8())
                .await
                .unwrap()
                .unwrap_err()
                .kind(),
            std::io::ErrorKind::UnexpectedEof
        );
        assert!(timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err());
    });
    let bus = Arc::new(RedisBus::connect(config).await.unwrap());
    let task = {
        let bus = bus.clone();
        tokio::spawn(async move { bus.send(&message("ambiguous first")).await })
    };
    timeout(Duration::from_secs(1), seen)
        .await
        .unwrap()
        .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(bus.send(&message("independent second")).await.unwrap(), 7);
    bus.shutdown().await;
    server.await.unwrap();
}
#[tokio::test]
async fn review_redis_cluster_redirect_is_not_followed_with_credentials() {
    let origin = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let redirected = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let destination = redirected.local_addr().unwrap();
    let config = RedisConfig {
        address: origin.local_addr().unwrap().to_string(),
        password: Some("synthetic-local-only".into()),
        ..RedisConfig::default()
    };
    let server = tokio::spawn(async move {
        let (socket, _) = origin.accept().await.unwrap();
        let mut socket = BufReader::new(socket);
        assert_eq!(
            command(&mut socket).await,
            vec![b"AUTH".to_vec(), b"synthetic-local-only".to_vec()]
        );
        socket.get_mut().write_all(b"+OK\r\n").await.unwrap();
        assert_eq!(command(&mut socket).await[0], b"SUBSCRIBE");
        socket
            .get_mut()
            .write_all(format!("-MOVED 42 {destination}\r\n").as_bytes())
            .await
            .unwrap();
    });
    let result = RedisBus::connect(config).await;
    assert!(matches!(result, Err(RedisError::Server(ref error)) if error.starts_with("MOVED")));
    assert!(timeout(Duration::from_millis(20), redirected.accept())
        .await
        .is_err());
    server.await.unwrap();
}
#[tokio::test]
async fn review_invalid_subscribe_ack_fails_closed() {
    for bytes in [
        b"*3\r\n$9\r\nsubscribe\r\n$5\r\nwrong\r\n:1\r\n".to_vec(),
        b"*3\r\n$9\r\nsubscribe\r\n$6\r\nreview\r\n:2\r\n".to_vec(),
        b"$999999999999999999\r\n".to_vec(),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = RedisConfig {
            address: listener.local_addr().unwrap().to_string(),
            channel: "review".into(),
            ..RedisConfig::default()
        };
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = BufReader::new(socket);
            command(&mut socket).await;
            socket.get_mut().write_all(&bytes).await.unwrap();
        });
        assert!(matches!(
            RedisBus::connect(config).await,
            Err(RedisError::Protocol(_))
        ));
        server.await.unwrap();
    }
}

#[tokio::test]
#[ignore = "requires a permitted non-loopback local route; not exercised by default"]
async fn review_auth_non_loopback_connected_peer_requires_local_route() {
    // UDP connect selects a route but sends no packets. Environments without a
    // non-loopback interface cannot exercise this connected-peer check.
    let route = tokio::net::UdpSocket::bind("0.0.0.0:0").await.unwrap();
    if route.connect("192.0.2.1:9").await.is_err() {
        eprintln!("No non-loopback route; connected-peer auth test not applicable");
        return;
    }
    let ip = route.local_addr().unwrap().ip();
    if ip.is_loopback() || ip.is_unspecified() {
        eprintln!("No non-loopback interface; connected-peer auth test not applicable");
        return;
    }
    let listener = TcpListener::bind(std::net::SocketAddr::new(ip, 0))
        .await
        .unwrap();
    let config = RedisConfig {
        address: listener.local_addr().unwrap().to_string(),
        password: Some("synthetic-never-transmitted".into()),
        ..RedisConfig::default()
    };
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut byte = [0];
        assert_eq!(
            timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0,
            "AUTH must not send any bytes on this connection"
        );
    });
    assert!(matches!(
        RedisBus::connect(config).await,
        Err(RedisError::Configuration(_))
    ));
    server.await.unwrap();
}
