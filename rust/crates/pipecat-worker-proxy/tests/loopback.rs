use pipecat_core::{Direction, Frame, FrameData, TextFrame};
use pipecat_network_bus::PythonJsonCodec;
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_worker_proxy::{
    ForwardKind, ProxyAttachment, ProxyError, ProxyEvent, ProxyOptions, ProxyState,
    WebSocketProxyClient, WebSocketProxyServer,
};
use pipecat_workers::{
    BusMessage, JobError, JobEvent, JobParams, JobResponse, JobStatus, LocalBus, MessageData,
    NativeWorker, WorkerReady, WorkerRegistry, WorkerRunner,
};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, Notify},
    time::timeout,
};
use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue};

const DEADLINE: Duration = Duration::from_secs(3);
fn params(name: &str) -> JobParams {
    JobParams {
        name: Some(name.into()),
        timeout: Some(DEADLINE),
        ..Default::default()
    }
}
fn options(name: &str, local: &str, remote: &str) -> ProxyOptions {
    let mut value = ProxyOptions::new(name, local, remote);
    value.admission_timeout = Duration::from_millis(300);
    value.cleanup_timeout = Duration::from_millis(300);
    value.session.close_timeout = Duration::from_millis(100);
    value
}
async fn pair(
    client_runner: &WorkerRunner,
    server_runner: &WorkerRunner,
) -> (WebSocketProxyClient, ProxyAttachment) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/worker", listener.local_addr().unwrap());
    let (bus, registry) = (server_runner.bus(), server_runner.registry());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        WebSocketProxyServer::accept_with_request_policy(
            stream,
            bus,
            registry,
            options("server-proxy", "worker", "voice"),
            |req| req.uri().path() == "/worker",
        )
        .await
        .unwrap()
    });
    let mut client = WebSocketProxyClient::new(
        url,
        client_runner.bus(),
        client_runner.registry(),
        options("client-proxy", "voice", "worker"),
    )
    .unwrap();
    assert!(client.attachment().is_none());
    client.activate().await.unwrap();
    (client, server.await.unwrap())
}
async fn raw_client(
    options: ProxyOptions,
    bus: LocalBus,
    registry: WorkerRegistry,
) -> (WebSocketSession, ProxyAttachment) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        WebSocketProxyServer::accept_with_request_policy(stream, bus, registry, options, |_| true)
            .await
            .unwrap()
    });
    let client = WebSocketSession::connect(&url, SessionConfig::default())
        .await
        .unwrap();
    (client, server.await.unwrap())
}
async fn send(session: &WebSocketSession, message: &BusMessage) {
    let codec = PythonJsonCodec::default();
    session
        .sender()
        .send(Packet::Binary(
            codec
                .encode(&codec.from_bus_message(message).unwrap())
                .unwrap()
                .into(),
        ))
        .await
        .unwrap();
}
fn speak(source: &str, target: Option<&str>) -> BusMessage {
    BusMessage {
        source: source.into(),
        target: target.map(str::to_owned),
        data: MessageData::Speak {
            text: "hello".into(),
            append_to_context: false,
        },
    }
}

#[tokio::test]
async fn registry_native_rpc_updates_stream_and_remote_errors() {
    let local = WorkerRunner::new("left", 32).unwrap();
    let remote = WorkerRunner::new("right", 32).unwrap();
    remote
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("echo", false, |context| async move {
                    context
                        .update(Some(json!({"state":"working"})), false)
                        .await
                        .unwrap();
                    context.stream_start().await.unwrap();
                    context.stream_data(json!({"chunk":1})).await.unwrap();
                    context.stream_end().await.unwrap();
                    JobResponse::completed(json!({"answer":42}))
                })
                .unwrap()
                .on_job("fail", false, |_| async {
                    JobResponse::error("expected failure")
                })
                .unwrap(),
        )
        .unwrap();
    let (mut client, mut server) = pair(&local, &remote).await;
    let ready = timeout(DEADLINE, local.registry().wait_ready("worker"))
        .await
        .unwrap();
    assert_eq!(ready.runner, "right");
    let mut job = local
        .client("voice")
        .request("worker", params("echo"))
        .unwrap();
    assert_eq!(
        job.next_event().await.unwrap(),
        Some(JobEvent::Update(Some(json!({"state":"working"}))))
    );
    assert_eq!(job.next_event().await.unwrap(), Some(JobEvent::StreamStart));
    assert_eq!(
        job.next_event().await.unwrap(),
        Some(JobEvent::StreamData(json!({"chunk":1})))
    );
    assert_eq!(job.next_event().await.unwrap(), Some(JobEvent::StreamEnd));
    assert_eq!(
        job.result().await.unwrap(),
        JobResponse::completed(json!({"answer":42}))
    );
    let mut failed = local
        .client("voice")
        .request("worker", params("fail"))
        .unwrap();
    assert_eq!(failed.result().await.unwrap().status, JobStatus::Error);
    client.stop().await.unwrap();
    assert!(timeout(DEADLINE, server.wait()).await.unwrap().is_err());
    assert!(local.registry().get("worker").is_none());
    remote.end().await.unwrap();
    local.end().await.unwrap();
}

#[tokio::test]
async fn update_request_and_cancellation_reach_running_remote_handler() {
    let local = WorkerRunner::new("left", 32).unwrap();
    let remote = WorkerRunner::new("right", 32).unwrap();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (notify, counter) = (started.clone(), dropped.clone());
    remote
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("wait", false, move |mut context| {
                    let (notify, counter) = (notify.clone(), counter.clone());
                    async move {
                        let _guard = Guard(counter);
                        notify.notify_one();
                        assert!(context.update_requested().await);
                        context
                            .update(Some(json!({"state":"requested"})), true)
                            .await
                            .unwrap();
                        std::future::pending::<JobResponse>().await
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let (mut client, mut server) = pair(&local, &remote).await;
    let mut job = local
        .client("voice")
        .request("worker", params("wait"))
        .unwrap();
    timeout(DEADLINE, started.notified()).await.unwrap();
    job.request_update().await.unwrap();
    assert_eq!(
        job.next_event().await.unwrap(),
        Some(JobEvent::Update(Some(json!({"state":"requested"}))))
    );
    job.cancel(Some("test".into())).await.unwrap();
    assert_eq!(job.result().await.unwrap().status, JobStatus::Cancelled);
    timeout(DEADLINE, async {
        while dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    client.stop().await.unwrap();
    let _ = server.wait().await;
    remote.end().await.unwrap();
}

#[tokio::test]
async fn strict_sources_targets_and_no_import_echo() {
    let bus = LocalBus::new(32).unwrap();
    let registry = WorkerRegistry::new("right");
    let local = bus.subscribe_target("worker").unwrap();
    let egress = bus.subscribe_outbound("other-adapter").unwrap();
    let (mut raw, mut server) =
        raw_client(options("proxy", "worker", "voice"), bus.clone(), registry).await;
    let _registry_packet = raw.recv().await.unwrap();
    let mut events = server.events();
    for message in [
        speak("intruder", Some("worker")),
        speak("voice", Some("other")),
        speak("voice", None),
    ] {
        send(&raw, &message).await;
        assert!(matches!(
            timeout(DEADLINE, events.recv()).await.unwrap().unwrap(),
            ProxyEvent::Rejected { .. }
        ));
    }
    let message = speak("voice", Some("worker"));
    send(&raw, &message).await;
    assert_eq!(
        timeout(DEADLINE, local.recv()).await.unwrap().unwrap(),
        Some(message)
    );
    assert!(egress.try_recv().unwrap().is_none());
    bus.publish(speak("intruder", Some("voice"))).unwrap();
    bus.publish(speak("worker", Some("other"))).unwrap();
    assert!(timeout(Duration::from_millis(40), raw.recv())
        .await
        .is_err());
    bus.publish(speak("worker", Some("voice"))).unwrap();
    let Packet::Binary(bytes) = timeout(DEADLINE, raw.recv()).await.unwrap().unwrap() else {
        panic!()
    };
    assert_eq!(
        PythonJsonCodec::default().decode(&bytes).unwrap().source(),
        "worker"
    );
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn frames_preserve_direction_and_allow_source_scoped_broadcast() {
    let bus = LocalBus::new(16).unwrap();
    let local = bus.subscribe_target("worker").unwrap();
    let mut config = options("proxy", "worker", "voice");
    config.forward_messages.insert(ForwardKind::Frame);
    let (mut raw, mut server) = raw_client(config, bus.clone(), WorkerRegistry::new("right")).await;
    raw.recv().await.unwrap();
    let message = BusMessage {
        source: "voice".into(),
        target: None,
        data: MessageData::Frame {
            frame: Box::new(Frame::new(FrameData::Text(TextFrame::new("hello")))),
            direction: Direction::Upstream,
            bridge: Some("edge".into()),
        },
    };
    send(&raw, &message).await;
    let received = timeout(DEADLINE, local.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(received, message);
    assert!(timeout(Duration::from_millis(40), raw.recv())
        .await
        .is_err());
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn malformed_local_only_and_oversized_packets_fail_closed() {
    for packet in [b"not json".to_vec(), serde_json::to_vec(&json!({"__type__":"pipecat.bus.messages.BusWorkerLocalErrorMessage","__data__":{"source":"voice","target":"worker","error":"oops"}})).unwrap(), vec![b'a';2048]] {
        let bus = LocalBus::new(16).unwrap();
        let mut config = options("proxy", "worker", "voice");
        config.codec.max_message_bytes = 1024;
        let (raw, mut server) = raw_client(config, bus, WorkerRegistry::new("right")).await;
        raw.sender().send(Packet::Binary(packet.into())).await.unwrap();
        assert!(matches!(timeout(DEADLINE, server.wait()).await.unwrap(), Err(ProxyError::Protocol(_))));
    }
}

#[tokio::test]
async fn auth_headers_origin_and_path_are_checked_before_upgrade() {
    for (token, origin, allowed) in [
        ("Bearer secret", "https://trusted.example", true),
        ("Bearer wrong", "https://trusted.example", false),
        ("Bearer secret", "https://evil.example", false),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/worker", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            WebSocketProxyServer::accept_with_request_policy(
                stream,
                LocalBus::new(8).unwrap(),
                WorkerRegistry::new("right"),
                options("server", "worker", "voice"),
                |request| {
                    request.uri().path() == "/worker"
                        && request
                            .headers()
                            .get("authorization")
                            .is_some_and(|value| value == "Bearer secret")
                        && request.headers().get_all("origin").iter().count() == 1
                        && request
                            .headers()
                            .get("origin")
                            .is_some_and(|value| value == "https://trusted.example")
                },
            )
            .await
        });
        let mut headers = HeaderMap::new();
        headers.insert("authorization", HeaderValue::from_str(token).unwrap());
        headers.insert("origin", HeaderValue::from_str(origin).unwrap());
        let mut client = WebSocketProxyClient::new(
            url,
            LocalBus::new(8).unwrap(),
            WorkerRegistry::new("left"),
            options("client", "voice", "worker"),
        )
        .unwrap()
        .with_headers(headers)
        .unwrap();
        assert_eq!(client.activate().await.is_ok(), allowed);
        let server = server.await.unwrap();
        assert_eq!(server.is_ok(), allowed);
        if allowed {
            client.stop().await.unwrap();
        }
    }
}

#[test]
fn custom_headers_require_tls_and_cannot_corrupt_handshake() {
    let headers = HeaderMap::from_iter([(
        "authorization".parse().unwrap(),
        HeaderValue::from_static("Bearer secret"),
    )]);
    let make = |url| {
        WebSocketProxyClient::new(
            url,
            LocalBus::new(8).unwrap(),
            WorkerRegistry::new("left"),
            options("client", "voice", "worker"),
        )
        .unwrap()
    };
    assert!(make("ws://remote.example")
        .with_headers(headers.clone())
        .is_err());
    assert!(make("wss://remote.example")
        .with_headers(headers.clone())
        .is_ok());
    assert!(make("ws://127.0.0.1").with_headers(headers.clone()).is_ok());
    assert!(make("ws://[::1]").with_headers(headers.clone()).is_ok());
    assert!(make("ws://localhost.evil.example")
        .with_headers(headers)
        .is_err());
    let headers = HeaderMap::from_iter([(
        "sec-websocket-key".parse().unwrap(),
        HeaderValue::from_static("override"),
    )]);
    assert!(make("ws://localhost").with_headers(headers).is_err());
}

#[tokio::test]
async fn dropout_fails_pending_cancels_execution_and_reconnect_never_replays() {
    let local = WorkerRunner::new("left", 32).unwrap();
    let remote = WorkerRunner::new("right", 32).unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (count, notification, cleanup) = (executions.clone(), started.clone(), dropped.clone());
    remote
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("wait", false, move |_| {
                    let (count, notification, cleanup) =
                        (count.clone(), notification.clone(), cleanup.clone());
                    async move {
                        let _guard = Guard(cleanup);
                        count.fetch_add(1, Ordering::SeqCst);
                        notification.notify_one();
                        std::future::pending::<JobResponse>().await
                    }
                })
                .unwrap()
                .on_job("echo", false, |_| async {
                    JobResponse::completed(json!({"value":"new request"}))
                })
                .unwrap(),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (tx, mut rx) = mpsc::channel(2);
    let (bus, registry) = (remote.bus(), remote.registry());
    let accepts = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let server = WebSocketProxyServer::accept_with_request_policy(
                stream,
                bus.clone(),
                registry.clone(),
                options("server-proxy", "worker", "voice"),
                |_| true,
            )
            .await
            .unwrap();
            tx.send(server).await.unwrap();
        }
    });
    let mut client = WebSocketProxyClient::new(
        url,
        local.bus(),
        local.registry(),
        options("client-proxy", "voice", "worker"),
    )
    .unwrap();
    client.activate().await.unwrap();
    let mut server = rx.recv().await.unwrap();
    let mut old_job = local
        .client("voice")
        .request("worker", params("wait"))
        .unwrap();
    timeout(DEADLINE, started.notified()).await.unwrap();
    server.shutdown().await.unwrap();
    assert!(client.attachment_mut().unwrap().wait().await.is_err());
    assert_eq!(old_job.result().await.unwrap().status, JobStatus::Error);
    timeout(DEADLINE, async {
        while dropped.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(local.registry().get("worker").is_none());
    // Disconnected publication is never retained for a future attachment.
    local.bus().publish(speak("voice", Some("worker"))).unwrap();
    client.activate().await.unwrap();
    let mut new_server = rx.recv().await.unwrap();
    let mut new_job = local
        .client("voice")
        .request("worker", params("echo"))
        .unwrap();
    assert_eq!(
        new_job.result().await.unwrap(),
        JobResponse::completed(json!({"value":"new request"}))
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    client.stop().await.unwrap();
    let _ = new_server.wait().await;
    accepts.await.unwrap();
    remote.end().await.unwrap();
}

#[tokio::test]
async fn bounded_diagnostic_ring_reports_lag_and_terminal_state_survives() {
    let mut config = options("proxy", "worker", "voice");
    config.event_capacity = 2;
    let (raw, mut server) = raw_client(
        config,
        LocalBus::new(16).unwrap(),
        WorkerRegistry::new("right"),
    )
    .await;
    let mut slow = server.events();
    let mut fast = server.events();
    for _ in 0..8 {
        send(&raw, &speak("intruder", Some("worker"))).await;
        assert!(matches!(
            timeout(DEADLINE, fast.recv()).await.unwrap().unwrap(),
            ProxyEvent::Rejected { .. }
        ));
    }
    assert!(matches!(
        slow.recv().await,
        Err(tokio::sync::broadcast::error::RecvError::Lagged(_))
    ));
    raw.cancel().await.unwrap();
    assert!(timeout(DEADLINE, server.wait()).await.unwrap().is_err());
    assert!(matches!(
        *server.watch().borrow(),
        ProxyState::Stopped { .. }
    ));
}

#[tokio::test]
async fn slow_native_bus_is_bounded_and_shutdown_interrupts_backpressure() {
    let bus = LocalBus::new(1).unwrap();
    let slow = bus.subscribe_target("worker").unwrap();
    let mut config = options("proxy", "worker", "voice");
    config.session.queue_capacity = 2;
    config.admission_timeout = Duration::from_secs(10);
    let (raw, mut server) = raw_client(config, bus.clone(), WorkerRegistry::new("right")).await;
    send(&raw, &speak("voice", Some("worker"))).await;
    // A system control message still crosses while normal admission is blocked.
    send(&raw, &speak("voice", Some("worker"))).await;
    let cancel = BusMessage {
        source: "voice".into(),
        target: Some("worker".into()),
        data: MessageData::CancelWorker { reason: None },
    };
    send(&raw, &cancel).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        timeout(DEADLINE, slow.recv_system())
            .await
            .unwrap()
            .unwrap(),
        Some(cancel)
    );
    timeout(Duration::from_secs(1), server.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert!(bus.publish(speak("worker", Some("unrelated"))).is_ok());
}

#[tokio::test]
async fn inbound_queue_overflow_is_explicit() {
    let bus = LocalBus::new(1).unwrap();
    let _slow = bus.subscribe_target("worker").unwrap();
    let mut config = options("proxy", "worker", "voice");
    config.session.queue_capacity = 1;
    config.admission_timeout = Duration::from_secs(10);
    let (raw, mut server) = raw_client(config, bus, WorkerRegistry::new("right")).await;
    for _ in 0..8 {
        send(&raw, &speak("voice", Some("worker"))).await;
    }
    assert!(matches!(
        timeout(DEADLINE, server.wait()).await.unwrap(),
        Err(ProxyError::Capacity("inbound priority lane"))
    ));
}

#[tokio::test]
async fn registry_arrival_removal_and_preservation_of_other_remote_workers() {
    let local = WorkerRunner::new("left", 32).unwrap();
    let remote = WorkerRunner::new("right", 32).unwrap();
    local.registry().register(WorkerReady {
        worker_name: "unrelated".into(),
        runner: "right".into(),
    });
    let (mut client, mut server) = pair(&local, &remote).await;
    remote
        .add(NativeWorker::new("worker").active(true))
        .unwrap();
    timeout(DEADLINE, local.registry().wait_ready("worker"))
        .await
        .unwrap();
    let mut changed = local.registry().watch("worker");
    remote.end().await.unwrap();
    timeout(DEADLINE, async {
        while changed.borrow_and_update().is_some() {
            changed.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    client.stop().await.unwrap();
    let _ = server.wait().await;
    assert!(local.registry().get("unrelated").is_some());
}

#[tokio::test]
async fn event_lag_does_not_destroy_job_terminal_result() {
    let local = WorkerRunner::new("left", 128).unwrap();
    let remote = WorkerRunner::new("right", 128).unwrap();
    remote
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("many", false, |context| async move {
                    for i in 0..20 {
                        context
                            .update(Some(json!({"index":i})), false)
                            .await
                            .unwrap();
                    }
                    JobResponse::completed(json!({"value":"done"}))
                })
                .unwrap(),
        )
        .unwrap();
    let (mut client, mut server) = pair(&local, &remote).await;
    let mut config = params("many");
    config.event_capacity = 2;
    let mut job = local.client("voice").request("worker", config).unwrap();
    assert_eq!(
        job.result().await.unwrap(),
        JobResponse::completed(json!({"value":"done"}))
    );
    assert!(matches!(
        job.next_event().await,
        Err(JobError::EventLagged(_))
    ));
    client.stop().await.unwrap();
    let _ = server.wait().await;
    remote.end().await.unwrap();
}

#[tokio::test]
async fn independent_source_json_fixture_drives_actual_worker() {
    let remote = WorkerRunner::new("right", 16).unwrap();
    remote
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("echo", false, |context| async move {
                    JobResponse::completed(context.payload.unwrap())
                })
                .unwrap(),
        )
        .unwrap();
    let (mut raw, mut server) = raw_client(
        options("proxy", "worker", "voice"),
        remote.bus(),
        remote.registry(),
    )
    .await;
    let registry = raw.recv().await.unwrap();
    let Packet::Binary(registry) = registry else {
        panic!()
    };
    let registry: serde_json::Value = serde_json::from_slice(&registry).unwrap();
    assert_eq!(
        registry["__type__"],
        "pipecat.bus.messages.BusWorkerRegistryMessage"
    );
    assert_eq!(
        registry["__data__"]["workers"][0]["__data__"]["name"],
        "worker"
    );
    raw.sender().send(Packet::Text(r#"{"__type__":"pipecat.bus.messages.BusJobRequestMessage","__data__":{"id":123,"name":"fixture","source":"voice","target":"worker","job_id":"python-fixture","job_name":"echo","payload":{"value":7}}}"#.into())).await.unwrap();
    let Packet::Binary(response) = timeout(DEADLINE, raw.recv()).await.unwrap().unwrap() else {
        panic!()
    };
    let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(
        response["__type__"],
        "pipecat.bus.messages.BusJobResponseMessage"
    );
    assert_eq!(response["__data__"]["source"], "worker");
    assert_eq!(response["__data__"]["target"], "voice");
    assert_eq!(response["__data__"]["job_id"], "python-fixture");
    assert_eq!(response["__data__"]["status"], "completed");
    assert_eq!(response["__data__"]["response"], json!({"value":7}));
    server.shutdown().await.unwrap();
    remote.end().await.unwrap();
}

#[tokio::test]
async fn duplicate_live_request_never_executes_twice() {
    let remote = WorkerRunner::new("right", 16).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let counter = count.clone();
    remote
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("wait", false, move |_| {
                    let counter = counter.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        std::future::pending::<JobResponse>().await
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let (raw, mut server) = raw_client(
        options("proxy", "worker", "voice"),
        remote.bus(),
        remote.registry(),
    )
    .await;
    let request = BusMessage {
        source: "voice".into(),
        target: Some("worker".into()),
        data: MessageData::JobRequest {
            job_id: "duplicate".into(),
            name: Some("wait".into()),
            payload: None,
        },
    };
    send(&raw, &request).await;
    timeout(DEADLINE, async {
        while count.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send(&raw, &request).await;
    assert!(matches!(
        timeout(DEADLINE, server.wait()).await.unwrap(),
        Err(ProxyError::Protocol(_))
    ));
    assert_eq!(count.load(Ordering::SeqCst), 1);
    remote.end().await.unwrap();
}

#[tokio::test]
async fn bounded_job_limit_fails_all_admitted_callers_including_overflow() {
    let local = WorkerRunner::new("left", 16).unwrap();
    let remote = WorkerRunner::new("right", 16).unwrap();
    remote
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("wait", false, |_| async {
                    std::future::pending::<JobResponse>().await
                })
                .unwrap(),
        )
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (bus, registry) = (remote.bus(), remote.registry());
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        WebSocketProxyServer::accept_with_request_policy(
            stream,
            bus,
            registry,
            options("server", "worker", "voice"),
            |_| true,
        )
        .await
        .unwrap()
    });
    let mut config = options("client", "voice", "worker");
    config.max_pending_jobs = 1;
    let mut client = WebSocketProxyClient::new(url, local.bus(), local.registry(), config).unwrap();
    client.activate().await.unwrap();
    let mut server = server.await.unwrap();
    let mut a = local
        .client("voice")
        .request("worker", params("wait"))
        .unwrap();
    let mut b = local
        .client("voice")
        .request("worker", params("wait"))
        .unwrap();
    assert_eq!(
        timeout(DEADLINE, a.result()).await.unwrap().unwrap().status,
        JobStatus::Error
    );
    assert_eq!(
        timeout(DEADLINE, b.result()).await.unwrap().unwrap().status,
        JobStatus::Error
    );
    assert!(matches!(
        client.attachment_mut().unwrap().wait().await,
        Err(ProxyError::Capacity("pending jobs"))
    ));
    let _ = server.wait().await;
    remote.end().await.unwrap();
}

#[test]
fn concurrent_remote_entry_updates_are_atomic_and_preserve_siblings() {
    let registry = WorkerRegistry::new("local");
    let revision = registry.local_changes();
    let threads = (0..8)
        .map(|index| {
            let registry = registry.clone();
            std::thread::spawn(move || {
                let name = format!("worker-{index}");
                let claim = registry.claim_remote_entry("remote", &name, None).unwrap();
                for generation in 0..200 {
                    let entry = pipecat_workers::WorkerRegistration {
                        ready: WorkerReady {
                            worker_name: name.clone(),
                            runner: "remote".into(),
                        },
                        parent: Some(generation.to_string()),
                        active: true,
                        bridged: false,
                    };
                    assert!(registry.update_remote_entry(&claim, Some(entry)));
                }
                assert!(registry.claim_remote_entry("remote", &name, None).is_none());
            })
        })
        .collect::<Vec<_>>();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(registry.entries("remote").len(), 8);
    assert_eq!(*revision.borrow(), 0);
    assert!(registry
        .claim_remote_entry("local", "forbidden", None)
        .is_none());
}

#[test]
fn opaque_claim_generation_prevents_identical_registration_aba_cleanup() {
    for mutation in 0..4 {
        let registry = WorkerRegistry::new("local");
        let entry = pipecat_workers::WorkerRegistration::from(WorkerReady {
            worker_name: "worker".into(),
            runner: "remote".into(),
        });
        let old = registry
            .claim_remote_entry("remote", "worker", Some(entry.clone()))
            .unwrap();
        match mutation {
            0 => {
                registry.unregister("worker", "remote");
            }
            1 => {
                registry.remove_runner("remote");
            }
            2 => {
                registry.replace_remote("remote", vec![]);
            }
            _ => {
                registry.replace_remote("remote", vec![entry.clone()]);
                registry.unregister("worker", "remote");
            }
        }
        let new = registry
            .claim_remote_entry("remote", "worker", Some(entry.clone()))
            .unwrap();
        assert!(!registry.update_remote_entry(&old, None));
        assert!(!registry.release_remote_entry(old));
        assert_eq!(registry.entries("remote"), vec![entry]);
        assert!(registry.release_remote_entry(new));
        assert!(registry.get("worker").is_none());
    }
    let registry = WorkerRegistry::new("local");
    let reserved = registry
        .claim_remote_entry("remote", "worker", None)
        .unwrap();
    assert!(registry
        .claim_remote_entry("remote", "worker", None)
        .is_none());
    let other_registry = WorkerRegistry::new("local");
    let _other = other_registry
        .claim_remote_entry("remote", "worker", None)
        .unwrap();
    assert!(!other_registry.update_remote_entry(&reserved, None));
    assert!(!other_registry.release_remote_entry(reserved.clone()));
    let entry = pipecat_workers::WorkerRegistration::from(WorkerReady {
        worker_name: "worker".into(),
        runner: "remote".into(),
    });
    assert!(registry.register_entry(entry.clone()));
    assert!(!registry.update_remote_entry(&reserved, Some(entry.clone())));
    assert!(!registry.release_remote_entry(reserved));
    assert_eq!(registry.entries("remote"), vec![entry]);
}

#[tokio::test]
async fn duplicate_discovery_cannot_remove_the_owning_connections_worker() {
    let local = WorkerRunner::new("left", 32).unwrap();
    let remote = WorkerRunner::new("right", 32).unwrap();
    remote
        .add(NativeWorker::new("worker").active(true))
        .unwrap();
    let (mut owner, mut owner_server) = pair(&local, &remote).await;
    timeout(DEADLINE, local.registry().wait_ready("worker"))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (bus, registry) = (remote.bus(), remote.registry());
    let second_server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        WebSocketProxyServer::accept_with_request_policy(
            stream,
            bus,
            registry,
            options("second-server", "worker", "voice"),
            |_| true,
        )
        .await
        .unwrap()
    });
    let mut duplicate = WebSocketProxyClient::new(
        url,
        local.bus(),
        local.registry(),
        options("second-client", "voice", "worker"),
    )
    .unwrap();
    duplicate.activate().await.unwrap();
    let mut second_server = second_server.await.unwrap();
    assert!(matches!(
        timeout(DEADLINE, duplicate.attachment_mut().unwrap().wait())
            .await
            .unwrap(),
        Err(ProxyError::Protocol(_))
    ));
    assert_eq!(local.registry().get("worker").unwrap().runner, "right");
    let _ = second_server.wait().await;
    owner.stop().await.unwrap();
    let _ = owner_server.wait().await;
    assert!(local.registry().get("worker").is_none());
    remote.end().await.unwrap();
}
