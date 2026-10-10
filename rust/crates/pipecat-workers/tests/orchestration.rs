use pipecat_workers::*;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("operation stalled")
}
fn worker(name: &str) -> NativeWorker {
    NativeWorker::new(name)
        .active(true)
        .on_job("", false, |context| async move {
            context
                .update(Some(json!({"progress":1})), false)
                .await
                .unwrap();
            context.stream_start().await.unwrap();
            context
                .stream_data(json!({"chunk":context.worker}))
                .await
                .unwrap();
            context.stream_end().await.unwrap();
            JobResponse::completed(json!({"id":context.job_id}))
        })
        .unwrap()
}
#[tokio::test]
async fn group_has_shared_id_streams_and_independent_terminal_results() {
    let runner = WorkerRunner::new("local", 64).unwrap();
    runner.add(worker("a")).unwrap();
    runner.add(worker("b")).unwrap();
    let mut group = runner
        .client("caller")
        .request_group(["a", "b"], Default::default())
        .unwrap();
    let id = group.id().to_owned();
    let mut events = vec![];
    while let Some(event) = bounded(group.next_event()).await.unwrap() {
        events.push(event);
    }
    let result = bounded(group.result()).await.unwrap();
    assert_eq!(result.responses.len(), 2);
    for response in result.responses.values() {
        assert_eq!(response.response, Some(json!({"id":id})));
    }
    for worker in ["a", "b"] {
        let worker_events: Vec<_> = events
            .iter()
            .filter(|event| event.worker == worker)
            .collect();
        assert_eq!(worker_events.len(), 4);
        assert!(matches!(worker_events[0].event, JobEvent::Update(_)));
        assert!(matches!(worker_events[3].event, JobEvent::StreamEnd));
    }
    bounded(runner.end()).await.unwrap();
}
#[tokio::test]
async fn group_partial_failures_preserve_successes_and_errors() {
    let runner = WorkerRunner::new("local", 32).unwrap();
    runner.add(worker("good")).unwrap();
    runner
        .add(
            NativeWorker::new("bad")
                .active(true)
                .on_job("", false, |_| async { JobResponse::failed("rejected") })
                .unwrap(),
        )
        .unwrap();
    let mut group = runner
        .client("caller")
        .request_group(
            ["good", "bad", "missing"],
            JobGroupParams {
                fail_fast: false,
                job: JobParams {
                    timeout: Some(Duration::from_millis(30)),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
    let error = bounded(group.result()).await.unwrap_err();
    assert_eq!(error.partial.responses["good"].status, JobStatus::Completed);
    assert_eq!(error.partial.responses["bad"].status, JobStatus::Failed);
    assert!(matches!(
        error.partial.errors["missing"],
        JobError::Timeout { .. }
    ));
    bounded(runner.end()).await.unwrap();
}
#[tokio::test]
async fn group_cancel_before_readiness_never_runs_late_workers() {
    let runner = WorkerRunner::new("local", 16).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let mut group = runner
        .client("caller")
        .request_group(["a", "b"], Default::default())
        .unwrap();
    bounded(group.cancel(Some("cancel all".into())))
        .await
        .unwrap();
    for name in ["a", "b"] {
        let count = count.clone();
        runner
            .add(
                NativeWorker::new(name)
                    .active(true)
                    .on_job("", false, move |_| {
                        count.fetch_add(1, Ordering::SeqCst);
                        async { JobResponse::completed(1) }
                    })
                    .unwrap(),
            )
            .unwrap();
    }
    let error = bounded(group.result()).await.unwrap_err();
    assert_eq!(error.partial.responses.len(), 2);
    assert!(error
        .partial
        .responses
        .values()
        .all(|response| response.status == JobStatus::Cancelled));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    bounded(runner.end()).await.unwrap();
}
#[tokio::test]
async fn group_fail_fast_cancels_other_running_members() {
    let runner = WorkerRunner::new("local", 16).unwrap();
    let entered = Arc::new(Notify::new());
    let child_entered = entered.clone();
    runner
        .add(
            NativeWorker::new("slow")
                .active(true)
                .on_job("", false, move |_| {
                    let entered = child_entered.clone();
                    async move {
                        entered.notify_one();
                        std::future::pending().await
                    }
                })
                .unwrap(),
        )
        .unwrap();
    runner
        .add(
            NativeWorker::new("fail")
                .active(true)
                .on_job("", false, move |_| {
                    let entered = entered.clone();
                    async move {
                        entered.notified().await;
                        JobResponse::error("failed")
                    }
                })
                .unwrap(),
        )
        .unwrap();
    let mut group = runner
        .client("caller")
        .request_group(["slow", "fail"], Default::default())
        .unwrap();
    let error = bounded(group.result()).await.unwrap_err();
    assert_eq!(error.partial.responses["fail"].status, JobStatus::Error);
    assert_eq!(error.partial.responses["slow"].status, JobStatus::Cancelled);
    bounded(runner.cancel()).await.unwrap();
}
#[tokio::test]
async fn group_rejects_duplicates_and_bounds_target_iteration() {
    let runner = WorkerRunner::new("local", 4).unwrap();
    assert!(matches!(
        runner
            .client("caller")
            .request_group(["a", "a"], Default::default()),
        Err(JobError::InvalidGroup)
    ));
    assert!(matches!(
        runner.client("caller").request_group(
            std::iter::repeat("a"),
            JobGroupParams {
                max_workers: 2,
                ..Default::default()
            }
        ),
        Err(JobError::InvalidGroup)
    ));
}
#[tokio::test]
async fn tree_end_is_root_owned_and_waits_for_descendants() {
    let runner = WorkerRunner::new("tree", 32).unwrap();
    let root = runner
        .add(worker("parent").child(worker("child").child(worker("leaf"))))
        .unwrap();
    assert_eq!(root.parent(), None);
    assert_eq!(root.children()[0].parent(), Some("parent"));
    assert_eq!(root.children()[0].children()[0].parent(), Some("child"));
    let audit = runner.bus().subscribe("audit").unwrap();
    let mut job = runner
        .client("caller")
        .request("leaf", Default::default())
        .unwrap();
    assert_eq!(
        bounded(job.result()).await.unwrap().status,
        JobStatus::Completed
    );
    bounded(runner.end()).await.unwrap();
    let mut ends = vec![];
    while let Some(message) = audit.try_recv().unwrap() {
        if matches!(message.data, MessageData::EndWorker { .. }) {
            ends.push((message.source, message.target.unwrap()));
        }
    }
    assert!(ends.contains(&("worker_handle".into(), "parent".into())));
    assert!(ends.contains(&("parent".into(), "child".into())));
    assert!(ends.contains(&("child".into(), "leaf".into())));
    assert!(!ends.contains(&("worker_handle".into(), "child".into())));
    assert!(runner.registry().local_workers().is_empty());
    assert_eq!(root.children()[0].snapshot().state, WorkerState::Ended);
}
#[tokio::test]
async fn cancelling_parent_aborts_running_descendant_and_auto_end_finishes() {
    let runner = WorkerRunner::new("tree", 16).unwrap();
    let entered = Arc::new(Notify::new());
    let seen = entered.clone();
    let root = runner
        .add(
            NativeWorker::new("parent").child(
                NativeWorker::new("child")
                    .active(true)
                    .on_job("", false, move |_| {
                        let entered = seen.clone();
                        async move {
                            entered.notify_one();
                            std::future::pending().await
                        }
                    })
                    .unwrap(),
            ),
        )
        .unwrap();
    let mut job = runner
        .client("caller")
        .request("child", Default::default())
        .unwrap();
    bounded(entered.notified()).await;
    root.cancel("stop tree");
    assert_eq!(
        bounded(job.result()).await.unwrap().status,
        JobStatus::Cancelled
    );
    bounded(runner.run(true)).await.unwrap();
    assert_eq!(root.children()[0].snapshot().state, WorkerState::Cancelled);
    assert!(runner.registry().local_workers().is_empty());
}
#[tokio::test]
async fn long_lived_runner_waits_for_explicit_bus_end_after_roots_finish() {
    let runner = WorkerRunner::new("host", 8).unwrap();
    let root = runner.add(worker("root")).unwrap();
    bounded(root.end(None)).await.unwrap();
    bounded(root.wait()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), runner.run(false))
            .await
            .is_err()
    );
    let bus = runner.bus();
    let run = runner.run(false);
    tokio::pin!(run);
    tokio::select! { biased; _ = &mut run => panic!("exited early"), _ = tokio::task::yield_now() => {} }
    bus.publish(BusMessage {
        source: "app".into(),
        target: None,
        data: MessageData::End { reason: None },
    })
    .unwrap();
    bounded(run).await.unwrap();
}
#[tokio::test]
async fn tree_duplicate_names_are_rejected_before_starting_any_worker() {
    let runner = WorkerRunner::new("tree", 4).unwrap();
    assert!(runner.add(worker("same").child(worker("same"))).is_err());
    assert!(runner.registry().local_workers().is_empty());
}
#[tokio::test]
async fn remote_registry_snapshots_do_not_echo_and_preserve_local_precedence() {
    let registry = WorkerRegistry::new("local");
    let mut revision = registry.local_changes();
    let entry = |runner: &str, worker: &str| WorkerRegistration {
        ready: WorkerReady {
            worker_name: worker.into(),
            runner: runner.into(),
        },
        parent: None,
        active: true,
        bridged: false,
    };
    registry.replace_remote("remote", vec![entry("remote", "a")]);
    assert!(!revision.has_changed().unwrap());
    let mut ready = registry.watch("a");
    assert_eq!(ready.borrow().as_ref().unwrap().runner, "remote");
    registry.register_entry(entry("local", "a"));
    assert!(revision.has_changed().unwrap());
    revision.borrow_and_update();
    assert_eq!(ready.borrow_and_update().as_ref().unwrap().runner, "local");
    registry.replace_remote("remote", vec![]);
    assert!(!ready.has_changed().unwrap());
    assert!(!revision.has_changed().unwrap());
    assert!(!registry.replace_remote("local", vec![]));
    assert!(registry.get("a").is_some());
}
#[tokio::test]
async fn imported_messages_never_enter_outbound_lanes_and_close_wakes_both() {
    let bus = LocalBus::new(1).unwrap();
    let outbound = bus.subscribe_outbound("network").unwrap();
    let target = bus.subscribe_target("worker").unwrap();
    let data = BusMessage {
        source: "peer".into(),
        target: Some("worker".into()),
        data: MessageData::Deactivate,
    };
    bus.inject(data.clone()).unwrap();
    assert!(outbound.try_recv().unwrap().is_none());
    assert!(matches!(bus.publish(data.clone()), Err(BusError::Full(_))));
    assert!(outbound.try_recv().unwrap().is_none());
    let cancel = BusMessage {
        data: MessageData::CancelWorker { reason: None },
        ..data
    };
    bus.inject(cancel).unwrap();
    assert!(matches!(
        bounded(target.recv_system()).await.unwrap().unwrap().data,
        MessageData::CancelWorker { .. }
    ));
    target.try_recv().unwrap();
    bus.close().unwrap();
    assert!(bounded(target.recv_data()).await.unwrap().is_none());
    assert!(bounded(target.recv_system()).await.unwrap().is_none());
}
#[tokio::test]
async fn cancellation_before_remote_request_admission_has_a_bounded_tombstone() {
    let runner = WorkerRunner::new("local", 4).unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let called = count.clone();
    runner
        .add(
            NativeWorker::new("worker")
                .active(true)
                .on_job("", false, move |_| {
                    called.fetch_add(1, Ordering::SeqCst);
                    async { JobResponse::completed(1) }
                })
                .unwrap(),
        )
        .unwrap();
    let replies = runner.bus().subscribe_target("remote").unwrap();
    runner
        .bus()
        .inject(BusMessage {
            source: "remote".into(),
            target: Some("worker".into()),
            data: MessageData::JobCancel {
                job_id: "late".into(),
                reason: None,
            },
        })
        .unwrap();
    tokio::task::yield_now().await;
    runner
        .bus()
        .inject(BusMessage {
            source: "remote".into(),
            target: Some("worker".into()),
            data: MessageData::JobRequest {
                job_id: "late".into(),
                name: None,
                payload: None,
            },
        })
        .unwrap();
    assert!(matches!(
        bounded(replies.recv()).await.unwrap().unwrap().data,
        MessageData::JobResponse {
            status: JobStatus::Cancelled,
            ..
        }
    ));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    bounded(runner.end()).await.unwrap();
}
#[tokio::test]
async fn empty_long_lived_host_accepts_dynamic_roots_and_retains_finished_handles() {
    let runner = Arc::new(WorkerRunner::new("dynamic", 16).unwrap());
    let host = runner.clone();
    let task = tokio::spawn(async move { host.run(false).await });
    tokio::task::yield_now().await;
    assert!(!task.is_finished());
    let root = runner
        .add(worker("dynamic-root").child(worker("dynamic-child")))
        .unwrap();
    let child = runner.get("dynamic-child").unwrap();
    let mut job = runner
        .client("caller")
        .request("dynamic-child", Default::default())
        .unwrap();
    assert_eq!(
        bounded(job.result()).await.unwrap().status,
        JobStatus::Completed
    );
    bounded(runner.end()).await.unwrap();
    bounded(task).await.unwrap().unwrap();
    assert_eq!(root.snapshot().state, WorkerState::Ended);
    assert_eq!(child.snapshot().state, WorkerState::Ended);
    assert!(matches!(
        runner.add(worker("orphan")),
        Err(WorkerError::RunnerStopped)
    ));
    assert!(runner.registry().local_workers().is_empty());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_racing_shutdown_is_owned_or_rejected_never_orphaned() {
    for _ in 0..32 {
        let runner = Arc::new(WorkerRunner::new("race", 8).unwrap());
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let r = runner.clone();
        let b = barrier.clone();
        let add = tokio::spawn(async move {
            b.wait().await;
            r.add(worker("new").child(worker("child")))
        });
        let r = runner.clone();
        let stop = tokio::spawn(async move {
            barrier.wait().await;
            r.cancel().await
        });
        let added = bounded(add).await.unwrap();
        bounded(stop).await.unwrap().unwrap();
        match added {
            Ok(handle) => {
                assert!(handle.snapshot().state.is_terminal());
                assert!(handle.children()[0].snapshot().state.is_terminal());
            }
            Err(error) => assert_eq!(error, WorkerError::RunnerStopped),
        }
        assert!(runner.registry().local_workers().is_empty());
        assert!(runner.add(worker("late")).is_err());
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn add_racing_empty_auto_end_is_atomically_admitted_or_rejected() {
    for _ in 0..32 {
        let runner = Arc::new(WorkerRunner::new("auto", 8).unwrap());
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let r = runner.clone();
        let b = barrier.clone();
        let run = tokio::spawn(async move {
            b.wait().await;
            r.run(true).await
        });
        let r = runner.clone();
        let add = tokio::spawn(async move {
            barrier.wait().await;
            r.add(worker("new"))
        });
        match bounded(add).await.unwrap() {
            Ok(handle) => {
                bounded(handle.end(None)).await.unwrap();
                bounded(handle.wait()).await.unwrap();
            }
            Err(error) => assert_eq!(error, WorkerError::RunnerStopped),
        }
        bounded(run).await.unwrap().unwrap();
        assert!(runner.registry().local_workers().is_empty());
        assert!(runner.add(worker("late")).is_err());
    }
}
#[tokio::test]
async fn worker_tree_can_fan_out_nested_jobs_through_handler_context() {
    let runner = WorkerRunner::new("nested", 32).unwrap();
    let parent = NativeWorker::new("parent")
        .active(true)
        .on_job("", false, |context| async move {
            let mut group = context
                .client()
                .request_group(["left", "right"], Default::default())
                .unwrap();
            let result = group.result().await.unwrap();
            JobResponse::completed(json!({"count":result.responses.len()}))
        })
        .unwrap()
        .child(worker("left"))
        .child(worker("right"));
    runner.add(parent).unwrap();
    let mut job = runner
        .client("app")
        .request("parent", Default::default())
        .unwrap();
    assert_eq!(
        bounded(job.result()).await.unwrap().response,
        Some(json!({"count":2}))
    );
    bounded(runner.end()).await.unwrap();
}
#[tokio::test]
async fn remote_registry_removal_cannot_overtake_an_admitted_response() {
    let runner = WorkerRunner::new("caller-host", 8).unwrap();
    runner.registry().register(WorkerReady {
        worker_name: "remote".into(),
        runner: "peer".into(),
    });
    let outbound = runner.bus().subscribe_outbound("network").unwrap();
    let mut job = runner
        .client("caller")
        .request(
            "remote",
            JobParams {
                timeout: Some(Duration::from_secs(1)),
                ..Default::default()
            },
        )
        .unwrap();
    let request = bounded(outbound.recv()).await.unwrap().unwrap();
    runner.registry().remove_runner("peer");
    tokio::task::yield_now().await;
    runner
        .bus()
        .inject(BusMessage {
            source: "remote".into(),
            target: Some(request.source),
            data: MessageData::JobResponse {
                job_id: job.id().into(),
                status: JobStatus::Completed,
                response: Some(json!({"late":true})),
                urgent: false,
            },
        })
        .unwrap();
    assert_eq!(
        bounded(job.result()).await.unwrap().status,
        JobStatus::Completed
    );
}
#[tokio::test]
async fn lost_remote_peer_resolves_at_the_end_to_end_deadline() {
    let runner = WorkerRunner::new("caller-host", 8).unwrap();
    runner.registry().register(WorkerReady {
        worker_name: "remote".into(),
        runner: "peer".into(),
    });
    let outbound = runner.bus().subscribe_outbound("network").unwrap();
    let mut job = runner
        .client("caller")
        .request(
            "remote",
            JobParams {
                timeout: Some(Duration::from_millis(30)),
                ..Default::default()
            },
        )
        .unwrap();
    bounded(outbound.recv()).await.unwrap().unwrap();
    runner.registry().remove_runner("peer");
    assert!(matches!(
        bounded(job.result()).await,
        Err(JobError::Timeout { .. })
    ));
}
#[tokio::test]
async fn group_event_overflow_keeps_successful_terminal_responses() {
    let runner = WorkerRunner::new("events", 64).unwrap();
    runner
        .add(
            NativeWorker::new("verbose")
                .active(true)
                .on_job("", false, |context| async move {
                    for index in 0..32 {
                        context
                            .update(Some(json!({"index":index})), false)
                            .await
                            .unwrap();
                    }
                    JobResponse::completed(json!({"done":true}))
                })
                .unwrap(),
        )
        .unwrap();
    let mut group = runner
        .client("caller")
        .request_group(
            ["verbose"],
            JobGroupParams {
                job: JobParams {
                    event_capacity: 1,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(
        bounded(group.result()).await.unwrap().responses["verbose"].status,
        JobStatus::Completed
    );
    assert!(matches!(
        group.next_event().await,
        Err(JobError::EventLagged(_))
    ));
    bounded(runner.end()).await.unwrap();
}
