//! Real loopback TCP against a small independent RESP2 server, not live Redis.
use pipecat_network_bus::*;
use pipecat_workers::*;
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
    disconnect: watch::Sender<u64>,
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
            disconnect,
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
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    timeout(Duration::from_secs(5), future)
        .await
        .expect("operation stalled")
}
async fn connect(
    server: &MockRedis,
    runner: &WorkerRunner,
) -> (Arc<RedisBus>, WorkerNetworkAdapter) {
    let network = Arc::new(RedisBus::connect(server.config()).await.unwrap());
    let mut adapter = WorkerNetworkAdapter::attach(
        runner.bus(),
        runner.registry(),
        network.clone(),
        WorkerNetworkOptions {
            registry_interval: Duration::from_secs(60),
            ..Default::default()
        },
    )
    .unwrap();
    bounded(adapter.ready()).await.unwrap();
    (network, adapter)
}
async fn finish(
    server: MockRedis,
    mut adapters: Vec<WorkerNetworkAdapter>,
    networks: Vec<Arc<RedisBus>>,
) {
    for adapter in &mut adapters {
        let _ = bounded(adapter.shutdown()).await;
    }
    for network in networks {
        network.shutdown().await;
    }
    server.shutdown().await;
}
#[tokio::test]
async fn two_real_runners_discover_group_stream_and_shutdown_without_echoes() {
    let server = MockRedis::start().await;
    let a = WorkerRunner::new("runner-a", 32).unwrap();
    let b = WorkerRunner::new("runner-b", 32).unwrap();
    for name in ["first", "second"] {
        b.add(
            NativeWorker::new(name)
                .active(true)
                .on_job("", false, |context| async move {
                    context
                        .update(Some(json!({"progress":1})), true)
                        .await
                        .unwrap();
                    context.stream_start().await.unwrap();
                    context
                        .stream_data(json!({"worker": context.worker}))
                        .await
                        .unwrap();
                    context.stream_end().await.unwrap();
                    JobResponse::completed(json!({"id":context.job_id}))
                })
                .unwrap(),
        )
        .unwrap();
    }
    let (na, aa) = connect(&server, &a).await;
    let (nb, ab) = connect(&server, &b).await;
    bounded(a.registry().wait_ready("first")).await;
    let mut group = a
        .client("app")
        .request_group(["first", "second"], Default::default())
        .unwrap();
    let id = group.id().to_owned();
    let mut events = vec![];
    while let Some(event) = bounded(group.next_event()).await.unwrap() {
        events.push(event);
    }
    let result = bounded(group.result()).await.unwrap();
    assert_eq!(result.responses.len(), 2);
    assert!(result
        .responses
        .values()
        .all(|response| response.response == Some(json!({"id":id}))));
    assert_eq!(events.len(), 8);
    bounded(b.end()).await.unwrap();
    let mut ready = a.registry().watch("first");
    bounded(async {
        while ready.borrow_and_update().is_some() {
            ready.changed().await.unwrap();
        }
    })
    .await;
    let wire = server.published.lock().await.clone();
    let requests = wire
        .iter()
        .filter(|bytes| {
            std::str::from_utf8(bytes)
                .unwrap()
                .contains("BusJobRequestMessage")
        })
        .count();
    assert_eq!(
        requests, 2,
        "network echo or automatic replay created duplicate requests"
    );
    assert!(wire.len() < 35, "registry changes formed a feedback loop");
    finish(server, vec![aa, ab], vec![na, nb]).await;
}
#[tokio::test]
async fn distributed_cancellation_before_readiness_never_starts_late_worker() {
    let server = MockRedis::start().await;
    let a = WorkerRunner::new("a", 8).unwrap();
    let b = WorkerRunner::new("b", 8).unwrap();
    let (na, aa) = connect(&server, &a).await;
    let (nb, ab) = connect(&server, &b).await;
    let mut group = a
        .client("app")
        .request_group(["late"], Default::default())
        .unwrap();
    bounded(group.cancel(None)).await.unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    b.add(
        NativeWorker::new("late")
            .active(true)
            .on_job("", false, move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                async { JobResponse::completed(json!({"ok":true})) }
            })
            .unwrap(),
    )
    .unwrap();
    bounded(a.registry().wait_ready("late")).await;
    assert_eq!(
        bounded(group.result()).await.unwrap_err().partial.responses["late"].status,
        JobStatus::Cancelled
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    bounded(b.end()).await.unwrap();
    finish(server, vec![aa, ab], vec![na, nb]).await;
}
#[tokio::test]
async fn network_update_request_crosses_a_saturated_job_data_lane() {
    let server = MockRedis::start().await;
    let a = WorkerRunner::new("a", 16).unwrap();
    let b = WorkerRunner::new("b", 1).unwrap();
    let entered = Arc::new(tokio::sync::Notify::new());
    let seen = entered.clone();
    b.add(
        NativeWorker::new("worker")
            .active(true)
            .max_jobs(1)
            .on_job("", false, move |mut context| {
                let entered = seen.clone();
                async move {
                    entered.notify_one();
                    assert!(context.update_requested().await);
                    JobResponse::completed(json!({"updated":true}))
                }
            })
            .unwrap(),
    )
    .unwrap();
    let (na, aa) = connect(&server, &a).await;
    let (nb, ab) = connect(&server, &b).await;
    bounded(a.registry().wait_ready("worker")).await;
    let mut first = a
        .client("app")
        .request("worker", Default::default())
        .unwrap();
    bounded(entered.notified()).await;
    // The second job occupies worker's sole queued data slot while first owns execution.
    let second = a
        .client("app")
        .request("worker", Default::default())
        .unwrap();
    let third = a
        .client("app")
        .request("worker", Default::default())
        .unwrap();
    // Third ingress is blocked on the full local data lane; updates must use
    // the independent network control receiver, not merely spare data capacity.
    sleep(Duration::from_millis(20)).await;
    bounded(first.request_update()).await.unwrap();
    assert_eq!(
        bounded(first.result()).await.unwrap().response,
        Some(json!({"updated":true}))
    );
    second.cancel(None).await.unwrap();
    third.cancel(None).await.unwrap();
    bounded(b.cancel()).await.unwrap();
    finish(server, vec![aa, ab], vec![na, nb]).await;
}
#[tokio::test]
async fn ambiguous_publish_stops_forwarding_without_replaying_executed_job() {
    let server = MockRedis::start().await;
    let a = WorkerRunner::new("a", 16).unwrap();
    let b = WorkerRunner::new("b", 16).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    b.add(
        NativeWorker::new("worker")
            .active(true)
            .on_job("", false, move |_| {
                count.fetch_add(1, Ordering::SeqCst);
                async { JobResponse::completed(json!({"ok":true})) }
            })
            .unwrap(),
    )
    .unwrap();
    let (na, mut aa) = connect(&server, &a).await;
    let (nb, ab) = connect(&server, &b).await;
    bounded(a.registry().wait_ready("worker")).await;
    // Redis delivers PUBLISH but its acknowledgement exceeds the command deadline.
    server.response_delay_ms.store(450, Ordering::SeqCst);
    let mut job = a
        .client("app")
        .request("worker", Default::default())
        .unwrap();
    let outcome = bounded(job.result()).await;
    assert!(outcome.is_ok() || matches!(outcome, Err(JobError::Bus(BusError::Closed))));
    assert!(matches!(
        bounded(aa.wait()).await,
        Err(ForwardError::Transport(_))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        server
            .published
            .lock()
            .await
            .iter()
            .filter(|bytes| std::str::from_utf8(bytes)
                .unwrap()
                .contains("BusJobRequestMessage"))
            .count(),
        1
    );
    server.response_delay_ms.store(0, Ordering::SeqCst);
    finish(server, vec![aa, ab], vec![na, nb]).await;
}
#[tokio::test]
async fn subscription_loss_closes_readiness_waits_instead_of_replaying_jobs() {
    let server = MockRedis::start().await;
    let a = WorkerRunner::new("a", 8).unwrap();
    let (network, mut adapter) = connect(&server, &a).await;
    let mut job = a
        .client("app")
        .request("not-ready", Default::default())
        .unwrap();
    server.disconnect.send_modify(|generation| *generation += 1);
    assert!(matches!(
        bounded(adapter.wait()).await,
        Err(ForwardError::Transport(_))
    ));
    assert!(matches!(
        bounded(job.result()).await,
        Err(JobError::Bus(BusError::Closed))
    ));
    finish(server, vec![adapter], vec![network]).await;
}
#[tokio::test]
async fn split_network_lanes_do_not_duplicate_and_both_wake_on_shutdown() {
    let server = MockRedis::start().await;
    let network = Arc::new(RedisBus::connect(server.config()).await.unwrap());
    let sub = network.subscribe().unwrap();
    for index in 0..3 {
        network
            .send(&BusMessage {
                source: "sender".into(),
                target: None,
                data: MessageData::Speak {
                    text: index.to_string(),
                    append_to_context: false,
                },
            })
            .await
            .unwrap();
    }
    network
        .send(&BusMessage {
            source: "sender".into(),
            target: None,
            data: MessageData::Cancel { reason: None },
        })
        .await
        .unwrap();
    assert!(matches!(
        bounded(sub.recv_system())
            .await
            .unwrap()
            .unwrap()
            .as_bus_message()
            .unwrap()
            .data,
        MessageData::Cancel { .. }
    ));
    for index in 0..3 {
        let envelope = bounded(sub.recv_data()).await.unwrap().unwrap();
        assert!(
            matches!(&envelope.as_bus_message().unwrap().data, MessageData::Speak { text, .. } if text == &index.to_string())
        );
    }
    network.shutdown().await;
    assert!(bounded(sub.recv_data()).await.unwrap().is_none());
    assert!(bounded(sub.recv_system()).await.unwrap().is_none());
    assert!(bounded(sub.recv()).await.unwrap().is_none());
    server.shutdown().await;
}
#[tokio::test]
async fn distributed_group_cancellation_aborts_both_remote_handler_futures() {
    struct CountDrop(Arc<AtomicUsize>);
    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let server = MockRedis::start().await;
    let a = WorkerRunner::new("a", 32).unwrap();
    let b = WorkerRunner::new("b", 32).unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    for name in ["one", "two"] {
        let dropped = dropped.clone();
        b.add(
            NativeWorker::new(name)
                .active(true)
                .on_job("", false, move |context| {
                    let dropped = dropped.clone();
                    async move {
                        let _guard = CountDrop(dropped);
                        context
                            .update(Some(json!({"started":true})), true)
                            .await
                            .unwrap();
                        std::future::pending().await
                    }
                })
                .unwrap(),
        )
        .unwrap();
    }
    let (na, aa) = connect(&server, &a).await;
    let (nb, ab) = connect(&server, &b).await;
    bounded(a.registry().wait_ready("one")).await;
    let mut group = a
        .client("app")
        .request_group(["one", "two"], Default::default())
        .unwrap();
    bounded(group.next_event()).await.unwrap().unwrap();
    bounded(group.next_event()).await.unwrap().unwrap();
    bounded(group.cancel(Some("cancel group".into())))
        .await
        .unwrap();
    assert!(bounded(group.result()).await.is_err());
    bounded(async {
        while dropped.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    bounded(b.end()).await.unwrap();
    finish(server, vec![aa, ab], vec![na, nb]).await;
}
#[tokio::test]
async fn distributed_partial_group_retains_completed_and_failed_responses() {
    let server = MockRedis::start().await;
    let a = WorkerRunner::new("a", 32).unwrap();
    let b = WorkerRunner::new("b", 32).unwrap();
    b.add(
        NativeWorker::new("good")
            .active(true)
            .on_job("", false, |_| async {
                JobResponse::completed(json!({"value":42}))
            })
            .unwrap(),
    )
    .unwrap();
    b.add(
        NativeWorker::new("bad")
            .active(true)
            .on_job("", false, |_| async {
                JobResponse::failed(json!({"error":"rejected"}))
            })
            .unwrap(),
    )
    .unwrap();
    let (na, aa) = connect(&server, &a).await;
    let (nb, ab) = connect(&server, &b).await;
    bounded(a.registry().wait_ready("good")).await;
    let mut group = a
        .client("app")
        .request_group(
            ["good", "bad"],
            JobGroupParams {
                fail_fast: false,
                ..Default::default()
            },
        )
        .unwrap();
    let result = bounded(group.result()).await.unwrap_err().partial;
    assert_eq!(result.responses["good"].status, JobStatus::Completed);
    assert_eq!(result.responses["bad"].status, JobStatus::Failed);
    assert_eq!(result.responses["good"].response, Some(json!({"value":42})));
    bounded(b.end()).await.unwrap();
    finish(server, vec![aa, ab], vec![na, nb]).await;
}
#[tokio::test]
async fn graceful_adapter_shutdown_drains_all_admitted_outbound_messages() {
    let server = MockRedis::start().await;
    let a = WorkerRunner::new("a", 32).unwrap();
    let (network, mut adapter) = connect(&server, &a).await;
    for index in 0..12 {
        a.bus()
            .publish(BusMessage {
                source: "app".into(),
                target: Some("remote".into()),
                data: MessageData::Speak {
                    text: index.to_string(),
                    append_to_context: false,
                },
            })
            .unwrap();
    }
    bounded(adapter.shutdown()).await.unwrap();
    let published = server.published.lock().await.clone();
    let speaks: Vec<_> = published
        .iter()
        .filter_map(|bytes| PythonJsonCodec::default().decode(bytes).ok())
        .filter_map(|envelope| match envelope.as_bus_message() {
            Some(BusMessage {
                data: MessageData::Speak { text, .. },
                ..
            }) => Some(text.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        speaks,
        (0..12).map(|index| index.to_string()).collect::<Vec<_>>()
    );
    network.shutdown().await;
    server.shutdown().await;
}
#[tokio::test]
async fn native_priority_is_opt_in_and_neither_lane_duplicates_or_starves_data() {
    let server = MockRedis::start().await;
    let network = RedisBus::connect(server.config()).await.unwrap();
    let raw = network.subscribe().unwrap();
    let native = network.subscribe().unwrap();
    native.enable_native_control().unwrap();
    for index in 0..12 {
        network
            .send(&BusMessage {
                source: "caller".into(),
                target: None,
                data: MessageData::Speak {
                    text: index.to_string(),
                    append_to_context: false,
                },
            })
            .await
            .unwrap();
        network
            .send(&BusMessage {
                source: "caller".into(),
                target: None,
                data: MessageData::JobUpdateRequest {
                    job_id: index.to_string(),
                },
            })
            .await
            .unwrap();
    }
    for index in 0..12 {
        let envelope = bounded(native.recv_system()).await.unwrap().unwrap();
        assert!(
            !envelope.is_system(),
            "wire priority must retain source classification"
        );
        assert!(
            matches!(&envelope.as_bus_message().unwrap().data, MessageData::JobUpdateRequest { job_id } if job_id == &index.to_string())
        );
        assert!(
            matches!(&bounded(native.recv_data()).await.unwrap().unwrap().as_bus_message().unwrap().data, MessageData::Speak { text, .. } if text == &index.to_string())
        );
        assert!(
            matches!(&bounded(raw.recv()).await.unwrap().unwrap().as_bus_message().unwrap().data, MessageData::Speak { text, .. } if text == &index.to_string())
        );
        assert!(
            matches!(&bounded(raw.recv()).await.unwrap().unwrap().as_bus_message().unwrap().data, MessageData::JobUpdateRequest { job_id } if job_id == &index.to_string())
        );
    }
    network.shutdown().await;
    assert!(bounded(native.recv_data()).await.unwrap().is_none());
    assert!(bounded(native.recv_system()).await.unwrap().is_none());
    server.shutdown().await;
}
