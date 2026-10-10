//! Real loopback TCP against a small independent RESP2 server, not live Redis.
use pipecat_network_bus::*;
use pipecat_workers::{BusMessage, MessageData};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{broadcast, watch, Mutex},
    task::JoinHandle,
    time::{sleep, timeout},
};

struct MockRedis {
    address: String,
    messages: broadcast::Sender<Vec<u8>>,
    disconnect: watch::Sender<u64>,
    accepted: Arc<AtomicUsize>,
    published: Arc<Mutex<Vec<Vec<u8>>>>,
    response_delay_ms: Arc<AtomicU64>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl MockRedis {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (messages, _) = broadcast::channel(128);
        let (disconnect, _) = watch::channel(0);
        let (stop, mut stopped) = watch::channel(false);
        let active = Arc::new(AtomicUsize::new(0));
        let accepted = Arc::new(AtomicUsize::new(0));
        let published = Arc::new(Mutex::new(vec![]));
        let response_delay_ms = Arc::new(AtomicU64::new(0));
        let (m, d, a, c, p, delay) = (
            messages.clone(),
            disconnect.clone(),
            active.clone(),
            accepted.clone(),
            published.clone(),
            response_delay_ms.clone(),
        );
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _=stopped.changed()=>break,
                    socket=listener.accept()=>{let(socket,_)=socket.unwrap();c.fetch_add(1,Ordering::SeqCst);tasks.spawn(serve(socket,m.clone(),d.subscribe(),a.clone(),p.clone(),delay.clone()));},
                    _=tasks.join_next(),if !tasks.is_empty()=>{},
                }
            }
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        });
        Self {
            address,
            messages,
            disconnect,
            accepted,
            published,
            response_delay_ms,
            stop,
            task,
        }
    }
    fn config(&self) -> RedisConfig {
        RedisConfig {
            address: self.address.clone(),
            channel: "test:bus".into(),
            reconnect_min: Duration::from_millis(10),
            reconnect_max: Duration::from_millis(30),
            command_timeout: Duration::from_millis(300),
            ..RedisConfig::default()
        }
    }
    async fn shutdown(self) {
        self.stop.send_replace(true);
        self.task.await.unwrap();
    }
}
struct Active(Arc<AtomicUsize>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
async fn serve(
    socket: TcpStream,
    messages: broadcast::Sender<Vec<u8>>,
    mut disconnect: watch::Receiver<u64>,
    active: Arc<AtomicUsize>,
    published: Arc<Mutex<Vec<Vec<u8>>>>,
    delay: Arc<AtomicU64>,
) {
    let mut socket = BufReader::new(socket);
    let mut receive = messages.subscribe();
    let mut subscriber = false;
    let mut active_guard = None;
    loop {
        tokio::select! {
            _=disconnect.changed(),if subscriber=>break,
            payload=receive.recv(),if subscriber=>{
                let Ok(payload)=payload else{break;};
                let mut bytes=b"*3\r\n$7\r\nmessage\r\n$8\r\ntest:bus\r\n".to_vec();
                bytes.extend_from_slice(format!("${}\r\n",payload.len()).as_bytes());bytes.extend_from_slice(&payload);bytes.extend_from_slice(b"\r\n");
                if socket.get_mut().write_all(&bytes).await.is_err(){break;}
            },
            command=read_command(&mut socket)=>{
                let Ok(command)=command else{break;};
                match command.first().map(Vec::as_slice) {
                    Some(b"SUBSCRIBE")=>{
                        assert_eq!(command[1],b"test:bus");subscriber=true;
                        active.fetch_add(1,Ordering::SeqCst);active_guard=Some(Active(active.clone()));
                        if socket.get_mut().write_all(b"*3\r\n$9\r\nsubscribe\r\n$8\r\ntest:bus\r\n:1\r\n").await.is_err(){break;}
                    },
                    Some(b"PUBLISH")=>{
                        assert_eq!(command[1],b"test:bus");published.lock().await.push(command[2].clone());
                        let _=messages.send(command[2].clone());
                        sleep(Duration::from_millis(delay.load(Ordering::SeqCst))).await;
                        let count=active.load(Ordering::SeqCst);
                        if socket.get_mut().write_all(format!(":{count}\r\n").as_bytes()).await.is_err(){break;}
                    },
                    Some(b"AUTH")|Some(b"SELECT")=>{if socket.get_mut().write_all(b"+OK\r\n").await.is_err(){break;}},
                    _=>break,
                }
            }
        }
    }
    drop(active_guard);
}
async fn read_command(socket: &mut BufReader<TcpStream>) -> std::io::Result<Vec<Vec<u8>>> {
    let mut line = String::new();
    if socket.read_line(&mut line).await? == 0 {
        return Err(std::io::ErrorKind::UnexpectedEof.into());
    }
    if !line.starts_with('*') {
        return Err(std::io::ErrorKind::InvalidData.into());
    }
    let count: usize = line[1..]
        .trim()
        .parse()
        .map_err(|_| std::io::ErrorKind::InvalidData)?;
    let mut args = vec![];
    for _ in 0..count {
        line.clear();
        socket.read_line(&mut line).await?;
        if !line.starts_with('$') {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        let n: usize = line[1..]
            .trim()
            .parse()
            .map_err(|_| std::io::ErrorKind::InvalidData)?;
        let mut data = vec![0; n];
        socket.read_exact(&mut data).await?;
        let mut crlf = [0; 2];
        socket.read_exact(&mut crlf).await?;
        if crlf != *b"\r\n" {
            return Err(std::io::ErrorKind::InvalidData.into());
        }
        args.push(data);
    }
    Ok(args)
}
fn data(text: &str) -> BusMessage {
    BusMessage {
        source: "sender".into(),
        target: None,
        data: MessageData::Speak {
            text: text.into(),
            append_to_context: true,
        },
    }
}
async fn recv(subscription: &NetworkSubscription) -> MessageEnvelope {
    timeout(Duration::from_secs(2), subscription.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn actual_tcp_fanout_targets_self_echo_and_python_wire() {
    let server = MockRedis::start().await;
    let a = RedisBus::connect(server.config()).await.unwrap();
    let b = RedisBus::connect(server.config()).await.unwrap();
    let own = a.subscribe().unwrap();
    let all = b.subscribe().unwrap();
    let target = b.subscribe_target("chosen").unwrap();
    let other = b.subscribe_target("other").unwrap();
    let mut message = data("hello 世界");
    message.target = Some("chosen".into());
    assert_eq!(a.send(&message).await.unwrap(), 2);
    let envelope = recv(&own).await;
    assert_eq!(envelope.as_bus_message(), Some(&message));
    assert_eq!(recv(&all).await, envelope);
    assert_eq!(recv(&target).await, envelope);
    assert!(timeout(Duration::from_millis(30), other.recv())
        .await
        .is_err());
    let published = server.published.lock().await;
    let wire: serde_json::Value = serde_json::from_slice(&published[0]).unwrap();
    assert_eq!(wire["__type__"], "pipecat.bus.messages.BusTTSSpeakMessage");
    drop(published);
    a.shutdown().await;
    b.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn malformed_message_does_not_kill_reader_and_local_error_never_publishes() {
    let server = MockRedis::start().await;
    let bus = RedisBus::connect(server.config()).await.unwrap();
    let sub = bus.subscribe().unwrap();
    let mut events = bus.events();
    server.messages.send(b"not json".to_vec()).unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap(),
        RedisEvent::InvalidMessage(_)
    ));
    bus.send(&data("valid")).await.unwrap();
    recv(&sub).await;
    let codec = PythonJsonCodec::default();
    let local=codec.decode_value(json!({"__type__":"pipecat.bus.messages.BusWorkerLocalErrorMessage","__data__":{"source":"child","error":"local"}})).unwrap();
    assert_eq!(bus.send_envelope(&local).await.unwrap(), 1);
    assert_eq!(recv(&sub).await, local);
    assert_eq!(server.published.lock().await.len(), 1);
    server.messages.send(codec.encode(&local).unwrap()).unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap()
            .unwrap(),
        RedisEvent::InvalidMessage(_)
    ));
    bus.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn bounded_data_queue_reports_lag_and_preserves_system_priority() {
    let server = MockRedis::start().await;
    let mut config = server.config();
    config.capacity_per_priority = 1;
    let bus = RedisBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    bus.send(&data("one")).await.unwrap();
    bus.send(&data("two")).await.unwrap();
    let cancel = BusMessage {
        source: "sender".into(),
        target: None,
        data: MessageData::Cancel {
            reason: Some("stop".into()),
        },
    };
    bus.send(&cancel).await.unwrap();
    sleep(Duration::from_millis(20)).await;
    assert_eq!(sub.recv().await.unwrap_err(), ReceiveError::Lagged(1));
    assert_eq!(recv(&sub).await.as_bus_message(), Some(&cancel));
    assert_eq!(recv(&sub).await.as_bus_message(), Some(&data("one")));
    bus.shutdown().await;
    assert!(sub.recv().await.unwrap().is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn disconnect_reconnect_resubscribes_before_reading_again() {
    let server = MockRedis::start().await;
    let bus = RedisBus::connect(server.config()).await.unwrap();
    let sub = bus.subscribe().unwrap();
    let mut events = bus.events();
    server.disconnect.send_replace(1);
    assert_eq!(
        timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap(),
        RedisEvent::Disconnected
    );
    assert_eq!(
        timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap(),
        RedisEvent::Reconnected
    );
    assert_eq!(*bus.connection_state().borrow(), ConnectionState::Connected);
    bus.send(&data("after reconnect")).await.unwrap();
    assert_eq!(
        recv(&sub).await.as_bus_message(),
        Some(&data("after reconnect"))
    );
    bus.shutdown().await;
    assert_eq!(*bus.connection_state().borrow(), ConnectionState::Stopped);
    server.shutdown().await;
}

#[tokio::test]
async fn ambiguous_publish_timeout_is_not_retried_and_connection_is_discarded() {
    let server = MockRedis::start().await;
    let mut config = server.config();
    config.command_timeout = Duration::from_millis(30);
    let bus = RedisBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    server.response_delay_ms.store(200, Ordering::SeqCst);
    assert!(matches!(
        bus.send(&data("first")).await,
        Err(RedisError::Timeout)
    ));
    assert_eq!(recv(&sub).await.as_bus_message(), Some(&data("first")));
    server.response_delay_ms.store(0, Ordering::SeqCst);
    assert_eq!(bus.send(&data("second")).await.unwrap(), 1);
    assert_eq!(recv(&sub).await.as_bus_message(), Some(&data("second")));
    assert_eq!(server.published.lock().await.len(), 2);
    assert_eq!(server.accepted.load(Ordering::SeqCst), 3);
    bus.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn cancel_pending_send_and_shutdown_wake_idle_subscribers() {
    let server = MockRedis::start().await;
    let bus = Arc::new(RedisBus::connect(server.config()).await.unwrap());
    let sub = bus.subscribe().unwrap();
    server.response_delay_ms.store(1000, Ordering::SeqCst);
    let task = {
        let bus = bus.clone();
        tokio::spawn(async move { bus.send(&data("inflight")).await })
    };
    recv(&sub).await;
    timeout(Duration::from_secs(1), bus.shutdown())
        .await
        .unwrap();
    assert!(matches!(task.await.unwrap(), Err(RedisError::Closed)));
    assert!(sub.recv().await.unwrap().is_none());
    assert!(matches!(
        bus.send(&data("late")).await,
        Err(RedisError::Closed)
    ));
    assert!(matches!(bus.subscribe(), Err(RedisError::Closed)));
    server.shutdown().await;
}

#[tokio::test]
async fn validates_configuration_and_supports_auth_select_handshake() {
    let server = MockRedis::start().await;
    let mut invalid = server.config();
    invalid.capacity_per_priority = 0;
    assert!(matches!(
        RedisBus::connect(invalid).await,
        Err(RedisError::Configuration(_))
    ));
    let mut config = server.config();
    config.username = Some("test-user".into());
    config.password = Some("test-only-password".into());
    config.database = 2;
    assert!(!format!("{config:?}").contains("test-only-password"));
    let bus = RedisBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    bus.send(&data("auth")).await.unwrap();
    recv(&sub).await;
    bus.shutdown().await;
    server.shutdown().await;
}
