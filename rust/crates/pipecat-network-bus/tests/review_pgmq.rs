#![cfg(feature = "pgmq")]
//! Adversarial PostgreSQL protocol checks. No database server or credentials required.
use pipecat_network_bus::{PgmqBus, PgmqConfig, PgmqEvent, PythonJsonCodec};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::watch,
    time::timeout,
};

fn packet(kind: u8, bytes: &[u8]) -> Vec<u8> {
    let mut result = vec![kind];
    result.extend_from_slice(&((bytes.len() + 4) as u32).to_be_bytes());
    result.extend_from_slice(bytes);
    result
}
fn row_description(columns: &[(&str, u32)]) -> Vec<u8> {
    let mut out = (columns.len() as i16).to_be_bytes().to_vec();
    for (name, oid) in columns {
        out.extend_from_slice(name.as_bytes());
        out.push(0);
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend_from_slice(&0i16.to_be_bytes());
        out.extend_from_slice(&oid.to_be_bytes());
        out.extend_from_slice(&(-1i16).to_be_bytes());
        out.extend_from_slice(&(-1i32).to_be_bytes());
        out.extend_from_slice(&1i16.to_be_bytes());
    }
    packet(b'T', &out)
}
fn row(values: &[Vec<u8>]) -> Vec<u8> {
    let mut out = (values.len() as i16).to_be_bytes().to_vec();
    for value in values {
        out.extend_from_slice(&(value.len() as i32).to_be_bytes());
        out.extend_from_slice(value);
    }
    packet(b'D', &out)
}
fn take_string<'a>(bytes: &mut &'a [u8]) -> &'a str {
    let end = bytes.iter().position(|b| *b == 0).unwrap();
    let text = std::str::from_utf8(&bytes[..end]).unwrap();
    *bytes = &bytes[end + 1..];
    text
}
async fn serve(
    mut socket: TcpStream,
    payloads: Arc<Vec<Option<Vec<u8>>>>,
    reads: Arc<AtomicUsize>,
    acks: Arc<AtomicUsize>,
    message_oid: u32,
) -> std::io::Result<()> {
    let length = socket.read_u32().await? as usize;
    let mut startup = vec![0; length - 4];
    socket.read_exact(&mut startup).await?;
    if startup[..4] == 80877102u32.to_be_bytes() {
        return Ok(());
    }
    socket.write_all(&packet(b'R', &0i32.to_be_bytes())).await?;
    socket.write_all(&packet(b'K', &[0; 8])).await?;
    socket.write_all(&packet(b'Z', b"I")).await?;
    let mut sql = String::new();
    let mut rows = vec![];
    loop {
        let kind = socket.read_u8().await?;
        let length = socket.read_u32().await? as usize;
        let mut body = vec![0; length - 4];
        socket.read_exact(&mut body).await?;
        match kind {
            b'P' => {
                let mut data = body.as_slice();
                take_string(&mut data);
                sql = take_string(&mut data).into();
                socket.write_all(&packet(b'1', &[])).await?;
            }
            b'B' => socket.write_all(&packet(b'2', &[])).await?,
            b'D' => {
                rows.clear();
                // query_typed supplies the parameter types, so only the count is relevant.
                let count = sql.bytes().filter(|b| *b == b'$').count();
                let mut parameter_types = (count as i16).to_be_bytes().to_vec();
                for n in 0..count {
                    parameter_types
                        .extend_from_slice(&(if n == 0 { 25u32 } else { 23u32 }).to_be_bytes());
                }
                socket.write_all(&packet(b't', &parameter_types)).await?;
                if sql.contains("read_with_poll") {
                    socket
                        .write_all(&row_description(&[
                            ("msg_id", 20),
                            ("message", message_oid),
                        ]))
                        .await?;
                    if reads.fetch_add(1, Ordering::SeqCst) == 0 {
                        for (n, payload) in payloads.iter().enumerate() {
                            let id = (n as i64 + 1).to_be_bytes().to_vec();
                            if let Some(payload) = payload {
                                let mut jsonb = vec![1];
                                jsonb.extend_from_slice(payload);
                                rows.push(row(&[id, jsonb]));
                            } else {
                                let mut data = 2i16.to_be_bytes().to_vec();
                                data.extend_from_slice(&8i32.to_be_bytes());
                                data.extend_from_slice(&id);
                                data.extend_from_slice(&(-1i32).to_be_bytes());
                                rows.push(packet(b'D', &data));
                            }
                        }
                    }
                } else if sql.contains("pgmq.delete") {
                    acks.fetch_add(1, Ordering::SeqCst);
                    socket
                        .write_all(&row_description(&[("delete", 16)]))
                        .await?;
                    rows.push(row(&[vec![1]]));
                } else {
                    socket.write_all(&row_description(&[])).await?;
                }
            }
            b'E' => {
                for row in &rows {
                    socket.write_all(row).await?;
                }
                socket
                    .write_all(&packet(b'C', format!("SELECT {}\0", rows.len()).as_bytes()))
                    .await?;
            }
            b'S' => socket.write_all(&packet(b'Z', b"I")).await?,
            b'C' => socket.write_all(&packet(b'3', &[])).await?,
            b'H' => (),
            b'X' => return Ok(()),
            other => panic!("unexpected message {other}"),
        }
    }
}
async fn server(
    payloads: Vec<Option<Vec<u8>>>,
) -> (
    PgmqConfig,
    Arc<AtomicUsize>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    server_with_message_type(payloads, 3802).await
}
async fn server_with_message_type(
    payloads: Vec<Option<Vec<u8>>>,
    message_oid: u32,
) -> (
    PgmqConfig,
    Arc<AtomicUsize>,
    watch::Sender<bool>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let reads = Arc::new(AtomicUsize::new(0));
    let acks = Arc::new(AtomicUsize::new(0));
    let acknowledgements = acks.clone();
    let payloads = Arc::new(payloads);
    let (stop, mut stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = stopped.changed() => break,
                accept = listener.accept() => { let (socket, _) = accept.unwrap(); tasks.spawn(serve(socket, payloads.clone(), reads.clone(), acks.clone(), message_oid)); },
                _ = tasks.join_next(), if !tasks.is_empty() => (),
            }
        }
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    });
    let config = PgmqConfig {
        address,
        max_poll_seconds: 0,
        poll_interval_ms: 5,
        retry_delay: Duration::from_millis(5),
        command_timeout: Duration::from_secs(1),
        ..PgmqConfig::default()
    };
    (config, acknowledgements, stop, task)
}
fn valid_payload() -> Vec<u8> {
    serde_json::to_vec(&json!({"__type__":"pipecat.bus.messages.BusTTSSpeakMessage", "__data__":{"source":"review", "text":"valid sibling"}})).unwrap()
}

#[tokio::test]
async fn review_pgmq_beyond_f64_number_is_poison_not_batch_failure() {
    // PostgreSQL JSONB accepts finite arbitrary-precision numerics beyond f64.
    let (config, acks, stop, task) = server(vec![
        Some(b"{\"number\":1e1000}".to_vec()),
        Some(valid_payload()),
    ])
    .await;
    let bus = PgmqBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    let mut events = bus.events();
    let event = timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap();
    let valid = timeout(Duration::from_secs(1), sub.recv()).await;
    timeout(Duration::from_secs(1), async {
        while acks.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    bus.shutdown().await.unwrap();
    stop.send_replace(true);
    task.await.unwrap();
    assert!(
        matches!(event, PgmqEvent::InvalidMessage { msg_id: 1, .. }),
        "poison must be handled per message; got {event:?}"
    );
    assert!(
        valid.is_ok(),
        "valid sibling in leased batch must still be delivered"
    );
    assert_eq!(acks.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn review_pgmq_oversized_message_is_poison_without_blocking_valid_sibling() {
    let (mut config, acks, stop, task) = server(vec![
        Some(serde_json::to_vec(&json!({"text":"x".repeat(4096)})).unwrap()),
        Some(valid_payload()),
    ])
    .await;
    config.codec = PythonJsonCodec {
        max_message_bytes: 512,
    };
    let bus = PgmqBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    let mut events = bus.events();
    let event = timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap();
    let valid = timeout(Duration::from_secs(1), sub.recv()).await;
    // Waiting for an acknowledged valid row also orders the preceding poison ack.
    timeout(Duration::from_secs(1), async {
        while acks.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    bus.shutdown().await.unwrap();
    stop.send_replace(true);
    task.await.unwrap();
    assert!(matches!(event, PgmqEvent::InvalidMessage { msg_id: 1, .. }));
    assert!(valid.unwrap().unwrap().is_some());
}

#[tokio::test]
async fn review_pgmq_null_row_is_poison_not_batch_failure() {
    let (config, acks, stop, task) = server(vec![None, Some(valid_payload())]).await;
    let bus = PgmqBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    let mut events = bus.events();
    let event = timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap();
    let valid = timeout(Duration::from_secs(1), sub.recv()).await;
    timeout(Duration::from_secs(1), async {
        while acks.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    bus.shutdown().await.unwrap();
    stop.send_replace(true);
    task.await.unwrap();
    assert!(matches!(event, PgmqEvent::InvalidMessage { msg_id: 1, .. }));
    assert!(valid.unwrap().unwrap().is_some());
}

#[tokio::test]
async fn review_pgmq_wrong_result_schema_is_not_acknowledged_as_poison() {
    let (config, acks, stop, task) =
        server_with_message_type(vec![Some(valid_payload())], 25).await;
    let bus = PgmqBus::connect(config).await.unwrap();
    let mut events = bus.events();
    let event = timeout(Duration::from_secs(2), events.recv())
        .await
        .unwrap()
        .unwrap();
    bus.shutdown().await.unwrap();
    stop.send_replace(true);
    task.await.unwrap();
    assert_eq!(event, PgmqEvent::ReadFailed);
    assert_eq!(acks.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn review_pgmq_non_loopback_endpoint_is_rejected_before_connect() {
    let config = PgmqConfig {
        address: "192.0.2.1:5432".parse().unwrap(),
        password: Some("synthetic-never-transmitted".into()),
        ..PgmqConfig::default()
    };
    assert!(matches!(
        PgmqBus::connect(config).await,
        Err(pipecat_network_bus::PgmqError::Configuration(_))
    ));
}

#[tokio::test]
async fn review_pgmq_unsupported_integer_is_poison_without_losing_sibling() {
    let raw = br#"{"__type__":"pipecat.bus.messages.BusJobRequestMessage","__data__":{"source":"review","job_id":"number","payload":{"number":18446744073709551617}}}"#.to_vec();
    let (config, acks, stop, task) = server(vec![Some(raw), Some(valid_payload())]).await;
    let bus = PgmqBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    let mut events = bus.events();
    let event = timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap()
        .unwrap();
    let message = timeout(Duration::from_secs(1), sub.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(1), async {
        while acks.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    bus.shutdown().await.unwrap();
    stop.send_replace(true);
    task.await.unwrap();
    assert!(matches!(event, PgmqEvent::InvalidMessage { msg_id: 1, .. }));
    assert_eq!(message.fields()["text"], "valid sibling");
}
