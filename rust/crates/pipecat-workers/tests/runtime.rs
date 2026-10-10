use pipecat_workers::*;
use serde_json::json;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("test operation stalled")
}
fn request(source: &str, id: &str) -> BusMessage {
    BusMessage {
        source: source.into(),
        target: Some("worker".into()),
        data: MessageData::JobRequest {
            job_id: id.into(),
            name: None,
            payload: None,
        },
    }
}
fn cancellation(source: &str, id: &str) -> BusMessage {
    BusMessage {
        source: source.into(),
        target: Some("worker".into()),
        data: MessageData::JobCancel {
            job_id: id.into(),
            reason: None,
        },
    }
}
struct Dropped(Arc<Notify>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn handler_stream_updates_and_response() {
    let runner = WorkerRunner::new("runner", 32).unwrap();
    runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, |context| async move {
                    context
                        .update(Some(json!({"progress":1})), false)
                        .await
                        .unwrap();
                    context.stream_start().await.unwrap();
                    context.stream_data(json!("chunk")).await.unwrap();
                    context.stream_end().await.unwrap();
                    JobResponse::completed(context.payload.unwrap())
                })
                .unwrap(),
        )
        .unwrap();
    let mut job = runner
        .client("caller")
        .request(
            "worker",
            JobParams {
                payload: Some(json!(42)),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        bounded(job.next_event()).await.unwrap(),
        Some(JobEvent::Update(Some(json!({"progress":1}))))
    );
    assert_eq!(
        bounded(job.next_event()).await.unwrap(),
        Some(JobEvent::StreamStart)
    );
    assert_eq!(
        bounded(job.next_event()).await.unwrap(),
        Some(JobEvent::StreamData(json!("chunk")))
    );
    assert_eq!(
        bounded(job.next_event()).await.unwrap(),
        Some(JobEvent::StreamEnd)
    );
    assert_eq!(
        bounded(job.result()).await.unwrap(),
        JobResponse::completed(42)
    );
    assert_eq!(job.next_event().await.unwrap(), None);
    bounded(runner.end()).await.unwrap();
}

#[tokio::test]
async fn cancellation_overtaking_queued_request_never_starts_handler() {
    let runner = WorkerRunner::new("runner", 4).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let shared = count.clone();
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, move |_| {
                    shared.fetch_add(1, Ordering::SeqCst);
                    async { JobResponse::completed(1) }
                })
                .unwrap(),
        )
        .unwrap();
    let sub = runner.bus().subscribe_target("caller").unwrap();
    runner.bus().publish(request("caller", "1")).unwrap();
    runner.bus().publish(cancellation("caller", "1")).unwrap();
    let result = bounded(sub.recv()).await.unwrap().unwrap();
    assert!(matches!(
        result.data,
        MessageData::JobResponse {
            status: JobStatus::Cancelled,
            ..
        }
    ));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    worker.cancel("done");
    bounded(worker.wait()).await.unwrap();
}

#[tokio::test]
async fn cancel_aborts_running_future_even_with_full_data_queue() {
    let runner = WorkerRunner::new("runner", 1).unwrap();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(Notify::new());
    let s = started.clone();
    let d = dropped.clone();
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .max_jobs(1)
                .on_job("", false, move |_| {
                    let s = s.clone();
                    let d = d.clone();
                    async move {
                        let _drop = Dropped(d);
                        s.notify_one();
                        std::future::pending::<JobResponse>().await
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let sub = runner.bus().subscribe_target("caller").unwrap();
    runner.bus().publish(request("caller", "1")).unwrap();
    bounded(started.notified()).await;
    runner.bus().publish(request("caller", "2")).unwrap();
    runner.bus().publish(cancellation("caller", "1")).unwrap();
    bounded(dropped.notified()).await;
    assert!(
        matches!(bounded(sub.recv()).await.unwrap().unwrap().data,MessageData::JobResponse{job_id,status:JobStatus::Cancelled,..} if job_id=="1")
    );
    worker.cancel("done");
    // Cancellation of queued work also produces a terminal response.
    assert!(
        matches!(bounded(sub.recv()).await.unwrap().unwrap().data,MessageData::JobResponse{job_id,status:JobStatus::Cancelled,..} if job_id=="2")
    );
    assert_eq!(
        bounded(worker.wait()).await.unwrap(),
        WorkerState::Cancelled
    );
}

#[tokio::test]
async fn graceful_end_waits_for_admitted_job_result() {
    let runner = WorkerRunner::new("runner", 8).unwrap();
    let started = Arc::new(Notify::new());
    let permit = Arc::new(Semaphore::new(0));
    let s = started.clone();
    let p = permit.clone();
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, move |_| {
                    let s = s.clone();
                    let p = p.clone();
                    async move {
                        s.notify_one();
                        let _permit = p.acquire().await.unwrap();
                        JobResponse::completed(7)
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let mut job = runner
        .client("caller")
        .request("worker", Default::default())
        .unwrap();
    bounded(started.notified()).await;
    worker.end(None).await.unwrap();
    tokio::task::yield_now().await;
    assert!(!worker.snapshot().state.is_terminal());
    permit.add_permits(1);
    assert_eq!(
        bounded(job.result()).await.unwrap(),
        JobResponse::completed(7)
    );
    assert_eq!(bounded(worker.wait()).await.unwrap(), WorkerState::Ended);
}

#[tokio::test]
async fn timeout_includes_readiness_and_runs_without_polling() {
    let runner = WorkerRunner::new("runner", 8).unwrap();
    let mut job = runner
        .client("caller")
        .request(
            "missing",
            JobParams {
                timeout: Some(Duration::from_millis(10)),
                ..Default::default()
            },
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(matches!(
        job.result().await,
        Err(JobError::Timeout {
            cancellation_error: None
        })
    ));
    bounded(runner.end()).await.unwrap();
}

#[tokio::test]
async fn timeout_and_handle_drop_cancel_remote_execution() {
    for drop_handle in [false, true] {
        let runner = WorkerRunner::new("runner", 8).unwrap();
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(Notify::new());
        let s = started.clone();
        let d = dropped.clone();
        runner
            .add(
                NativeWorker::new("worker")
                    .active(true)
                    .on_job("", false, move |_| {
                        let s = s.clone();
                        let d = d.clone();
                        async move {
                            let _drop = Dropped(d);
                            s.notify_one();
                            std::future::pending::<JobResponse>().await
                        }
                    })
                    .unwrap(),
            )
            .unwrap();
        let mut job = runner
            .client("caller")
            .request(
                "worker",
                JobParams {
                    timeout: Some(Duration::from_millis(40)),
                    ..Default::default()
                },
            )
            .unwrap();
        bounded(started.notified()).await;
        if drop_handle {
            drop(job);
        } else {
            assert!(matches!(
                bounded(job.result()).await,
                Err(JobError::Timeout {
                    cancellation_error: None
                })
            ));
        }
        bounded(dropped.notified()).await;
        bounded(runner.cancel()).await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequential_handler_fifo_and_cancelled_waiter() {
    let runner = WorkerRunner::new("runner", 64).unwrap();
    let order = Arc::new(Mutex::new(Vec::new()));
    let permits = Arc::new(Semaphore::new(0));
    let o = order.clone();
    let p = permits.clone();
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("serial", true, move |context| {
                    let o = o.clone();
                    let p = p.clone();
                    async move {
                        o.lock().unwrap().push(context.job_id.clone());
                        let permit = p.acquire().await.unwrap();
                        permit.forget();
                        JobResponse::completed(1)
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let sub = runner.bus().subscribe_target("caller").unwrap();
    for id in 0..8 {
        let mut message = request("caller", &id.to_string());
        if let MessageData::JobRequest { name, .. } = &mut message.data {
            *name = Some("serial".into());
        }
        runner.bus().publish(message).unwrap();
    }
    // Force the router to dispatch requests before cancelling a waiting job.
    bounded(async {
        loop {
            if !order.lock().unwrap().is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    runner.bus().publish(cancellation("caller", "3")).unwrap();
    assert!(
        matches!(bounded(sub.recv()).await.unwrap().unwrap().data,MessageData::JobResponse{job_id,status:JobStatus::Cancelled,..} if job_id=="3")
    );
    permits.add_permits(8);
    for _ in 0..7 {
        bounded(sub.recv()).await.unwrap().unwrap();
    }
    assert_eq!(*order.lock().unwrap(), ["0", "1", "2", "4", "5", "6", "7"]);
    worker.end(None).await.unwrap();
    bounded(worker.wait()).await.unwrap();
}

#[tokio::test]
async fn panic_is_reported_and_worker_accepts_later_jobs() {
    let runner = WorkerRunner::new("runner", 8).unwrap();
    runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("panic", false, |_| async { panic!("handler panic") })
                .unwrap()
                .on_job("ok", false, |_| async { JobResponse::completed(2) })
                .unwrap(),
        )
        .unwrap();
    let client = runner.client("caller");
    let mut bad = client
        .request(
            "worker",
            JobParams {
                name: Some("panic".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        bounded(bad.result()).await.unwrap().status,
        JobStatus::Error
    );
    let mut good = client
        .request(
            "worker",
            JobParams {
                name: Some("ok".into()),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        bounded(good.result()).await.unwrap(),
        JobResponse::completed(2)
    );
    bounded(runner.end()).await.unwrap();
}

#[tokio::test]
async fn registry_watch_is_immediate_deduplicated_and_tracks_removal() {
    let registry = WorkerRegistry::new("local");
    let mut watch = registry.watch("worker");
    assert!(watch.borrow().is_none());
    let remote = WorkerReady {
        worker_name: "worker".into(),
        runner: "remote".into(),
    };
    assert!(registry.register(remote.clone()));
    bounded(watch.changed()).await.unwrap();
    assert_eq!(*watch.borrow_and_update(), Some(remote.clone()));
    assert!(!registry.register(remote.clone()));
    assert!(!watch.has_changed().unwrap());
    assert!(registry.register(WorkerReady {
        worker_name: "worker".into(),
        runner: "local".into()
    }));
    bounded(watch.changed()).await.unwrap();
    assert_eq!(watch.borrow_and_update().as_ref().unwrap().runner, "local");
    registry.unregister("worker", "local");
    bounded(watch.changed()).await.unwrap();
    assert_eq!(*watch.borrow_and_update(), Some(remote));
    registry.unregister("worker", "remote");
    bounded(watch.changed()).await.unwrap();
    assert!(watch.borrow().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publishers_and_receivers_do_not_lose_wakeups() {
    let bus = LocalBus::new(1).unwrap();
    let a = bus.subscribe("a").unwrap();
    let b = bus.subscribe("b").unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for sub in [a, b] {
        tasks.spawn(async move {
            for _ in 0..400 {
                sub.recv().await.unwrap().unwrap();
            }
        });
    }
    for producer in 0..4 {
        let bus = bus.clone();
        tasks.spawn(async move {
            for n in 0..100 {
                bus.publish_wait(BusMessage {
                    source: producer.to_string(),
                    target: None,
                    data: MessageData::Speak {
                        text: n.to_string(),
                        append_to_context: false,
                    },
                })
                .await
                .unwrap();
            }
        });
    }
    bounded(async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn dropping_subscriber_releases_waiting_atomic_publish() {
    let bus = LocalBus::new(1).unwrap();
    let sub = bus.subscribe("blocked").unwrap();
    bus.publish(request("x", "0")).unwrap();
    let copy = bus.clone();
    let publish = tokio::spawn(async move { copy.publish_wait(request("x", "1")).await });
    tokio::task::yield_now().await;
    assert!(!publish.is_finished());
    drop(sub);
    assert_eq!(bounded(publish).await.unwrap().unwrap(), 0);
}

#[tokio::test]
async fn unrelated_target_does_not_consume_subscription_capacity() {
    let bus = LocalBus::new(1).unwrap();
    let sub = bus.subscribe_target("worker").unwrap();
    for _ in 0..100 {
        bus.publish(BusMessage {
            source: "x".into(),
            target: Some("other".into()),
            data: MessageData::Deactivate,
        })
        .unwrap();
    }
    assert!(sub.try_recv().unwrap().is_none());
    bus.close().unwrap();
    assert_eq!(sub.recv().await.unwrap(), None);
}

#[tokio::test]
async fn slow_event_reader_reports_lag_but_preserves_terminal_result() {
    let runner = WorkerRunner::new("runner", 32).unwrap();
    runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, |context| async move {
                    for n in 0..12 {
                        context.update(Some(json!(n)), false).await.unwrap();
                    }
                    JobResponse::completed(99)
                })
                .unwrap(),
        )
        .unwrap();
    let mut job = runner
        .client("caller")
        .request(
            "worker",
            JobParams {
                event_capacity: 2,
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        bounded(job.result()).await.unwrap(),
        JobResponse::completed(99)
    );
    assert!(matches!(
        job.next_event().await,
        Err(JobError::EventLagged(_))
    ));
    bounded(runner.end()).await.unwrap();
}

#[tokio::test]
async fn timeout_reports_failure_to_admit_remote_cancellation() {
    let runner = WorkerRunner::new("runner", 1).unwrap();
    let started = Arc::new(Notify::new());
    let s = started.clone();
    runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, move |_| {
                    let s = s.clone();
                    async move {
                        s.notify_one();
                        std::future::pending::<JobResponse>().await
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let mut job = runner
        .client("caller")
        .request(
            "worker",
            JobParams {
                timeout: Some(Duration::from_millis(30)),
                ..Default::default()
            },
        )
        .unwrap();
    bounded(started.notified()).await;
    let observer = runner.bus().subscribe("blocked_observer").unwrap();
    runner
        .bus()
        .publish(BusMessage {
            source: "x".into(),
            target: Some("unused".into()),
            data: MessageData::WorkerError {
                error: "fills only observer system lane".into(),
            },
        })
        .unwrap();
    assert!(matches!(
        bounded(job.result()).await,
        Err(JobError::Timeout {
            cancellation_error: Some(BusError::Full(_))
        })
    ));
    drop(observer);
    bounded(runner.cancel()).await.unwrap();
}

#[tokio::test]
async fn explicit_cancel_resolves_caller_and_requests_remote_stop() {
    let runner = WorkerRunner::new("runner", 4).unwrap();
    let started = Arc::new(Notify::new());
    let s = started.clone();
    runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, move |_| {
                    let s = s.clone();
                    async move {
                        s.notify_one();
                        std::future::pending::<JobResponse>().await
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let mut job = runner
        .client("caller")
        .request("worker", Default::default())
        .unwrap();
    bounded(started.notified()).await;
    bounded(job.cancel(Some("stop".into()))).await.unwrap();
    assert_eq!(
        bounded(job.result()).await.unwrap().status,
        JobStatus::Cancelled
    );
    bounded(runner.end()).await.unwrap();
}

#[tokio::test]
async fn bus_failure_wait_joins_handler_cleanup() {
    let runner = WorkerRunner::new("runner", 4).unwrap();
    let started = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let s = started.clone();
    let d = dropped.clone();
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, move |_| {
                    let s = s.clone();
                    let d = d.clone();
                    async move {
                        let _guard = Guard(d);
                        s.notify_one();
                        std::future::pending::<JobResponse>().await
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let _job = runner
        .client("caller")
        .request("worker", Default::default())
        .unwrap();
    bounded(started.notified()).await;
    runner.bus().close().unwrap();
    assert!(bounded(worker.wait()).await.is_err());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(runner.registry().get("worker").is_none());
}

#[tokio::test]
async fn inactive_gate_then_activation_args_and_update_requests() {
    let runner = WorkerRunner::new("runner", 8).unwrap();
    let entered = Arc::new(Notify::new());
    let e = entered.clone();
    let worker = runner
        .add(
            NativeWorker::new("worker")
                .on_job("", false, move |mut context| {
                    let e = e.clone();
                    async move {
                        e.notify_one();
                        assert!(context.update_requested().await);
                        JobResponse::completed(1)
                    }
                })
                .unwrap(),
        )
        .unwrap();
    assert_eq!(worker.snapshot().state, WorkerState::Inactive);
    worker.activate(Some(json!({"mode":"test"}))).await.unwrap();
    let mut job = runner
        .client("caller")
        .request("worker", Default::default())
        .unwrap();
    bounded(entered.notified()).await;
    assert_eq!(
        worker.snapshot().activation_args,
        Some(json!({"mode":"test"}))
    );
    job.request_update().await.unwrap();
    assert_eq!(
        bounded(job.result()).await.unwrap(),
        JobResponse::completed(1)
    );
    worker.deactivate().await.unwrap();
    worker.end(None).await.unwrap();
    bounded(worker.wait()).await.unwrap();
    assert_eq!(worker.snapshot().activation_args, None);
}
