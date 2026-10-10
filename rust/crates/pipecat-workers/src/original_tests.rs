use super::*;
fn data(n: &str) -> BusMessage {
    BusMessage {
        source: "a".into(),
        target: None,
        data: MessageData::Speak {
            text: n.into(),
            append_to_context: true,
        },
    }
}
fn cancel() -> BusMessage {
    BusMessage {
        source: "a".into(),
        target: Some("b".into()),
        data: MessageData::CancelWorker { reason: None },
    }
}
#[tokio::test]
async fn priority_fifo_and_broadcast() {
    let bus = LocalBus::new(3).unwrap();
    let a = bus.subscribe("a").unwrap();
    let b = bus.subscribe("b").unwrap();
    bus.publish(data("one")).unwrap();
    bus.publish(cancel()).unwrap();
    bus.publish(data("two")).unwrap();
    for sub in [&a, &b] {
        assert_eq!(sub.recv().await.unwrap(), Some(cancel()));
        assert_eq!(sub.recv().await.unwrap(), Some(data("one")));
        assert_eq!(sub.recv().await.unwrap(), Some(data("two")));
    }
}
#[tokio::test]
async fn backpressure_is_atomic_and_does_not_block_cancel() {
    let bus = LocalBus::new(1).unwrap();
    let a = bus.subscribe("a").unwrap();
    let b = bus.subscribe("b").unwrap();
    bus.publish(data("one")).unwrap();
    a.recv().await.unwrap();
    assert_eq!(bus.publish(data("two")), Err(BusError::Full("b".into())));
    bus.publish(cancel()).unwrap();
    assert_eq!(a.recv().await.unwrap(), Some(cancel()));
    assert_eq!(b.recv().await.unwrap(), Some(cancel()));
    assert_eq!(b.recv().await.unwrap(), Some(data("one")));
    bus.close().unwrap();
    assert_eq!(a.recv().await.unwrap(), None);
}
#[tokio::test]
async fn close_wakes_waiter_and_drains() {
    let bus = LocalBus::new(2).unwrap();
    let sub = bus.subscribe("s").unwrap();
    let copy = bus.clone();
    let task = tokio::spawn(async move { sub.recv().await });
    tokio::task::yield_now().await;
    copy.close().unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        None
    );
    assert_eq!(bus.publish(data("late")), Err(BusError::Closed));
}
#[test]
fn duplicate_registration_and_drop() {
    let bus = LocalBus::new(1).unwrap();
    let sub = bus.subscribe("a").unwrap();
    assert!(matches!(bus.subscribe("a"), Err(BusError::Duplicate(_))));
    drop(sub);
    assert!(bus.subscribe("a").is_ok());
    let r = WorkerRegistry::new("local");
    assert!(r.register(WorkerReady {
        worker_name: "w".into(),
        runner: "remote".into()
    }));
    assert!(!r.register(WorkerReady {
        worker_name: "w".into(),
        runner: "remote".into()
    }));
    assert!(r.register(WorkerReady {
        worker_name: "w".into(),
        runner: "local".into()
    }));
    assert_eq!(r.get("w").unwrap().runner, "local");
}
#[test]
fn job_status_wire_values() {
    for (s, w) in [
        (JobStatus::Completed, "completed"),
        (JobStatus::Cancelled, "cancelled"),
        (JobStatus::Failed, "failed"),
        (JobStatus::Error, "error"),
    ] {
        assert_eq!(serde_json::to_value(s).unwrap(), w);
    }
}
