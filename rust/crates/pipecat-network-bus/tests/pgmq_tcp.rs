#![cfg(feature = "pgmq")]
//! Actual tokio-postgres driver over loopback against a scripted PostgreSQL wire
//! server. This tests SQL/bind/connection behavior, not a live PGMQ extension.
use pipecat_network_bus::*;
use pipecat_workers::{BusMessage, MessageData};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Mutex},
    task::JoinHandle,
    time::{sleep, timeout},
};

type Pending = (i64, Value, bool);
#[derive(Default)]
struct State {
    queues: BTreeMap<String, VecDeque<Pending>>,
    queries: Vec<(String, Vec<Value>)>,
    next_id: i64,
    deleted: usize,
}
struct Server {
    address: std::net::SocketAddr,
    state: Arc<Mutex<State>>,
    read_delay: Arc<AtomicU64>,
    reads: Arc<AtomicUsize>,
    cancels: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}
impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let state = Arc::new(Mutex::new(State::default()));
        let read_delay = Arc::new(AtomicU64::new(0));
        let reads = Arc::new(AtomicUsize::new(0));
        let cancels = Arc::new(AtomicUsize::new(0));
        let connections = Arc::new(AtomicUsize::new(0));
        let (stop, mut stopped) = watch::channel(false);
        let (s, d, r, c, n) = (
            state.clone(),
            read_delay.clone(),
            reads.clone(),
            cancels.clone(),
            connections.clone(),
        );
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {_=stopped.changed()=>break,accepted=listener.accept()=>{let(socket,_)=accepted.unwrap();n.fetch_add(1,Ordering::SeqCst);tasks.spawn(serve(socket,s.clone(),d.clone(),r.clone(),c.clone()));},_=tasks.join_next(),if !tasks.is_empty()=>{}}
            }
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        });
        Self {
            address,
            state,
            read_delay,
            reads,
            cancels,
            connections,
            stop,
            task,
        }
    }
    fn config(&self) -> PgmqConfig {
        PgmqConfig {
            address: self.address,
            channel: "test:channel".into(),
            max_poll_seconds: 0,
            poll_interval_ms: 5,
            retry_delay: Duration::from_millis(10),
            command_timeout: Duration::from_millis(250),
            ..PgmqConfig::default()
        }
    }
    async fn close(self) {
        self.stop.send_replace(true);
        self.task.await.unwrap();
    }
}
fn msg() -> BusMessage {
    BusMessage {
        source: "source".into(),
        target: None,
        data: MessageData::JobRequest {
            job_id: "id".into(),
            name: Some("task".into()),
            payload: Some(json!({"literal":"'quoted'; DROP TABLE nope;"})),
        },
    }
}
fn packet(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![kind];
    out.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    out.extend_from_slice(body);
    out
}
fn text_at<'a>(data: &mut &'a [u8]) -> &'a str {
    let n = data.iter().position(|&b| b == 0).unwrap();
    let s = std::str::from_utf8(&data[..n]).unwrap();
    *data = &data[n + 1..];
    s
}
fn i16_at(data: &mut &[u8]) -> usize {
    let n = u16::from_be_bytes(data[..2].try_into().unwrap()) as usize;
    *data = &data[2..];
    n
}
fn i32_at(data: &mut &[u8]) -> usize {
    let n = u32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
    *data = &data[4..];
    n
}
fn bind_values(body: &[u8], types: &[u32]) -> Vec<Value> {
    let mut data = body;
    text_at(&mut data);
    text_at(&mut data);
    let formats = i16_at(&mut data);
    for _ in 0..formats {
        i16_at(&mut data);
    }
    let count = i16_at(&mut data);
    let mut values = vec![];
    for oid in &types[..count] {
        let n = i32_at(&mut data);
        let bytes = &data[..n];
        data = &data[n..];
        values.push(match oid {
            25 => String::from_utf8(bytes.to_vec()).unwrap().into(),
            23 => i32::from_be_bytes(bytes.try_into().unwrap()).into(),
            20 => i64::from_be_bytes(bytes.try_into().unwrap()).into(),
            3802 => {
                assert_eq!(bytes[0], 1);
                serde_json::from_slice(&bytes[1..]).unwrap()
            }
            _ => panic!("unknown bound type {oid}"),
        });
    }
    values
}
#[derive(Clone)]
struct Column {
    name: &'static str,
    oid: u32,
}
struct ResultRows {
    columns: Vec<Column>,
    rows: Vec<Vec<Vec<u8>>>,
}
fn result(sql: &str, params: &[Value], state: &mut State) -> ResultRows {
    let mut output = ResultRows {
        columns: vec![],
        rows: vec![],
    };
    if sql.contains("pgmq.create") {
        state
            .queues
            .insert(params[0].as_str().unwrap().into(), VecDeque::new());
    } else if sql.contains("bus_join") {
        let name = format!("isolated_{}", state.queues.len());
        state.queues.insert(name.clone(), VecDeque::new());
        output.columns = vec![Column {
            name: "bus_join",
            oid: 25,
        }];
        output.rows.push(vec![name.into_bytes()]);
    } else if sql.contains("list_queues") {
        output.columns = vec![Column {
            name: "queue_name",
            oid: 25,
        }];
        output.rows = state
            .queues
            .keys()
            .map(|name| vec![name.as_bytes().to_vec()])
            .collect();
    } else if sql.contains("pgmq.send") {
        state.next_id += 1;
        let id = state.next_id;
        state
            .queues
            .get_mut(params[0].as_str().unwrap())
            .unwrap()
            .push_back((id, params[1].clone(), false));
        output.columns = vec![Column {
            name: "send",
            oid: 20,
        }];
        output.rows.push(vec![id.to_be_bytes().to_vec()]);
    } else if sql.contains("bus_publish") {
        state.next_id += 1;
        let id = state.next_id;
        for queue in state.queues.values_mut() {
            queue.push_back((id, params[2].clone(), false));
        }
        let mut array = vec![];
        array.extend_from_slice(&1i32.to_be_bytes());
        array.extend_from_slice(&0i32.to_be_bytes());
        array.extend_from_slice(&20i32.to_be_bytes());
        array.extend_from_slice(&1i32.to_be_bytes());
        array.extend_from_slice(&1i32.to_be_bytes());
        array.extend_from_slice(&8i32.to_be_bytes());
        array.extend_from_slice(&id.to_be_bytes());
        output.columns = vec![Column {
            name: "bus_publish",
            oid: 1016,
        }];
        output.rows.push(vec![array]);
    } else if sql.contains("read_with_poll") || sql.contains("bus_subscribe") {
        output.columns = vec![
            Column {
                name: "msg_id",
                oid: 20,
            },
            Column {
                name: "message",
                oid: 3802,
            },
        ];
        let qty = if sql.contains("bus_subscribe") {
            params[3].as_u64().unwrap()
        } else {
            params[2].as_u64().unwrap()
        } as usize;
        if let Some(queue) = state.queues.get_mut(params[0].as_str().unwrap()) {
            for (id, payload, leased) in
                queue.iter_mut().filter(|(_, _, leased)| !*leased).take(qty)
            {
                *leased = true;
                let mut json = vec![1];
                json.extend_from_slice(&serde_json::to_vec(payload).unwrap());
                output.rows.push(vec![id.to_be_bytes().to_vec(), json]);
            }
        }
    } else if sql.contains("pgmq.delete") || sql.contains("bus_archive") {
        let id = params.last().unwrap().as_i64().unwrap();
        let queue = state.queues.get_mut(params[0].as_str().unwrap()).unwrap();
        let previous = queue.len();
        queue.retain(|(item, _, _)| *item != id);
        let deleted = queue.len() < previous;
        state.deleted += usize::from(deleted);
        output.columns = vec![Column {
            name: "deleted",
            oid: 16,
        }];
        output.rows.push(vec![vec![u8::from(deleted)]]);
    } else if sql.contains("drop_queue") || sql.contains("bus_leave") {
        state.queues.remove(params[0].as_str().unwrap());
    } else {
        panic!("unexpected SQL {sql}");
    }
    output
}
fn row_description(columns: &[Column]) -> Vec<u8> {
    let mut body = (columns.len() as i16).to_be_bytes().to_vec();
    for column in columns {
        body.extend_from_slice(column.name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&column.oid.to_be_bytes());
        body.extend_from_slice(&(-1i16).to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&1i16.to_be_bytes());
    }
    packet(b'T', &body)
}
async fn serve(
    mut socket: TcpStream,
    state: Arc<Mutex<State>>,
    read_delay: Arc<AtomicU64>,
    reads: Arc<AtomicUsize>,
    cancels: Arc<AtomicUsize>,
) -> std::io::Result<()> {
    socket.set_nodelay(true)?;
    let length = socket.read_u32().await? as usize;
    let mut startup = vec![0; length - 4];
    socket.read_exact(&mut startup).await?;
    let protocol = u32::from_be_bytes(startup[..4].try_into().unwrap());
    if protocol == 80877102 {
        cancels.fetch_add(1, Ordering::SeqCst);
        return Ok(());
    }
    assert_eq!(protocol, 196608);
    socket.write_all(&packet(b'R', &0i32.to_be_bytes())).await?;
    let mut key = 123i32.to_be_bytes().to_vec();
    key.extend_from_slice(&456i32.to_be_bytes());
    socket.write_all(&packet(b'K', &key)).await?;
    socket
        .write_all(&packet(b'S', b"server_version\x0016.0\x00"))
        .await?;
    socket
        .write_all(&packet(b'S', b"client_encoding\x00UTF8\x00"))
        .await?;
    socket.write_all(&packet(b'Z', b"I")).await?;
    let mut sql = String::new();
    let mut types = vec![];
    let mut params = vec![];
    let mut current = None;
    loop {
        let kind = socket.read_u8().await?;
        let length = socket.read_u32().await? as usize;
        let mut body = vec![0; length - 4];
        socket.read_exact(&mut body).await?;
        match kind {
            b'P' => {
                let mut data = body.as_slice();
                text_at(&mut data);
                sql = text_at(&mut data).into();
                let n = i16_at(&mut data);
                types = (0..n).map(|_| i32_at(&mut data) as u32).collect();
                socket.write_all(&packet(b'1', &[])).await?;
            }
            b'B' => {
                params = bind_values(&body, &types);
                socket.write_all(&packet(b'2', &[])).await?;
            }
            b'D' => {
                assert_eq!(body[0], b'S');
                let mut description = (types.len() as i16).to_be_bytes().to_vec();
                for oid in &types {
                    description.extend_from_slice(&oid.to_be_bytes());
                }
                socket.write_all(&packet(b't', &description)).await?;
                state
                    .lock()
                    .await
                    .queries
                    .push((sql.clone(), params.clone()));
                if sql.contains("read_with_poll") || sql.contains("bus_subscribe") {
                    reads.fetch_add(1, Ordering::SeqCst);
                    sleep(Duration::from_millis(read_delay.load(Ordering::SeqCst))).await;
                }
                let output = result(&sql, &params, &mut *state.lock().await);
                socket.write_all(&row_description(&output.columns)).await?;
                current = Some(output);
            }
            b'E' => {
                let output = current.take().unwrap();
                for row in &output.rows {
                    let mut body = (row.len() as i16).to_be_bytes().to_vec();
                    for value in row {
                        body.extend_from_slice(&(value.len() as i32).to_be_bytes());
                        body.extend_from_slice(value);
                    }
                    socket.write_all(&packet(b'D', &body)).await?;
                }
                socket
                    .write_all(&packet(
                        b'C',
                        format!("SELECT {}\0", output.rows.len()).as_bytes(),
                    ))
                    .await?;
            }
            b'S' => socket.write_all(&packet(b'Z', b"I")).await?,
            b'C' => socket.write_all(&packet(b'3', &[])).await?,
            b'H' => {}
            b'X' => return Ok(()),
            other => panic!("unexpected frontend message {other}"),
        }
    }
}
async fn recv(sub: &NetworkSubscription) -> MessageEnvelope {
    timeout(Duration::from_secs(3), sub.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
async fn until(mut condition: impl AsyncFnMut() -> bool) {
    timeout(Duration::from_secs(3), async {
        while !condition().await {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn direct_real_driver_binds_values_fans_out_and_deletes_own_queue() {
    let server = Server::start().await;
    let bus = PgmqBus::connect(server.config()).await.unwrap();
    assert!(bus.queue_name().starts_with("test_channel_"));
    let sub = bus.subscribe().unwrap();
    assert_eq!(
        bus.send(&msg()).await.unwrap(),
        PublishReport {
            delivered: 1,
            failed_peers: vec![]
        }
    );
    assert_eq!(recv(&sub).await.as_bus_message(), Some(&msg()));
    until(async || server.state.lock().await.deleted == 1).await;
    let state = server.state.lock().await;
    let publish = state
        .queries
        .iter()
        .find(|(sql, _)| sql.contains("pgmq.send"))
        .unwrap();
    assert_eq!(publish.0, "SELECT pgmq.send($1, $2::jsonb)");
    assert_eq!(
        publish.1[1]["__data__"]["payload"]["literal"],
        "'quoted'; DROP TABLE nope;"
    );
    drop(state);
    bus.shutdown().await.unwrap();
    assert!(server.state.lock().await.queues.is_empty());
    assert!(sub.recv().await.unwrap().is_none());
    server.close().await;
}

#[tokio::test]
async fn isolated_uses_only_wrappers_and_preserves_capability_as_bound_parameter() {
    let server = Server::start().await;
    let mut config = server.config();
    config.mode = PgmqMode::Isolated;
    config.channel = "secret'; SQL is data".into();
    let bus = PgmqBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    assert_eq!(bus.send(&msg()).await.unwrap().delivered, 1);
    recv(&sub).await;
    until(async || server.state.lock().await.deleted == 1).await;
    bus.shutdown().await.unwrap();
    let state = server.state.lock().await;
    assert!(state
        .queries
        .iter()
        .all(|(sql, _)| !sql.contains("pgmq.") && !sql.contains("secret")));
    assert!(state
        .queries
        .iter()
        .any(|(sql, params)| sql.contains("bus_subscribe") && params[1] == "secret'; SQL is data"));
    drop(state);
    server.close().await;
}

#[tokio::test]
async fn shutdown_cancels_inflight_postgres_query_without_reusing_connection() {
    let server = Server::start().await;
    server.read_delay.store(1000, Ordering::SeqCst);
    let bus = PgmqBus::connect(server.config()).await.unwrap();
    until(async || server.reads.load(Ordering::SeqCst) > 0).await;
    timeout(Duration::from_secs(1), bus.shutdown())
        .await
        .unwrap()
        .unwrap();
    until(async || server.cancels.load(Ordering::SeqCst) > 0).await;
    assert!(server.state.lock().await.queues.is_empty());
    server.close().await;
}

#[tokio::test]
async fn timed_out_reader_discards_session_and_reconnects_with_same_queue() {
    let server = Server::start().await;
    server.read_delay.store(300, Ordering::SeqCst);
    let mut config = server.config();
    config.command_timeout = Duration::from_millis(60);
    let bus = PgmqBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    let mut events = bus.events();
    assert_eq!(
        timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap(),
        PgmqEvent::ReadFailed
    );
    server.read_delay.store(0, Ordering::SeqCst);
    bus.send(&msg()).await.unwrap();
    assert_eq!(recv(&sub).await.as_bus_message(), Some(&msg()));
    assert!(server.connections.load(Ordering::SeqCst) >= 3);
    until(async || server.cancels.load(Ordering::SeqCst) > 0).await;
    bus.shutdown().await.unwrap();
    server.close().await;
}

#[tokio::test]
async fn bounded_admission_precedes_ack_and_poison_messages_are_acknowledged() {
    let server = Server::start().await;
    let mut config = server.config();
    config.capacity_per_priority = 1;
    let bus = PgmqBus::connect(config).await.unwrap();
    let sub = bus.subscribe().unwrap();
    let mut events = bus.events();
    bus.send(&msg()).await.unwrap();
    bus.send(&msg()).await.unwrap();
    until(async || server.state.lock().await.deleted == 1).await;
    sleep(Duration::from_millis(30)).await;
    assert_eq!(server.state.lock().await.deleted, 1);
    recv(&sub).await;
    recv(&sub).await;
    until(async || server.state.lock().await.deleted == 2).await;
    {
        let mut state = server.state.lock().await;
        state.queues.get_mut(bus.queue_name()).unwrap().push_back((
            999,
            json!({"bad":"message"}),
            false,
        ));
    }
    assert!(matches!(
        timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap(),
        PgmqEvent::InvalidMessage { msg_id: 999, .. }
    ));
    until(async || server.state.lock().await.deleted == 3).await;
    bus.shutdown().await.unwrap();
    server.close().await;
}

#[test]
fn channel_sanitization_and_debug_redaction() {
    assert_eq!(sanitize_pgmq_channel(""), "pipecat_bus");
    assert_eq!(sanitize_pgmq_channel("9:你好"), "q_9___");
    assert_eq!(sanitize_pgmq_channel(&"a".repeat(50)), "a".repeat(30));
    let config = PgmqConfig {
        password: Some("not-for-logs".into()),
        channel: "secret-capability".into(),
        ..PgmqConfig::default()
    };
    let debug = format!("{config:?}");
    assert!(!debug.contains("not-for-logs") && !debug.contains("secret-capability"));
}
