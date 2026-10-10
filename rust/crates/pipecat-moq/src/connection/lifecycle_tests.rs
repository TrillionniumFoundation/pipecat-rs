use super::*;

#[tokio::test]
async fn cancelled_join_keeps_the_supervisor_owned_for_drop() {
    let shared = Arc::new(Shared {
        accepting: Mutex::new(true),
        shutdown: watch::channel(Shutdown::Running).0,
        interrupt: watch::channel(0).0,
        tasks: AtomicUsize::new(0),
        idle: Notify::new(),
    });
    let (done, done_rx) = watch::channel(None);
    let monitor = MoqMonitor {
        shared: shared.clone(),
        done: done_rx,
    };
    let guard = TaskGuard::new(&shared);
    let done = DoneGuard(done);
    let task = tokio::spawn(async move {
        let (_guard, _done) = (guard, done);
        std::future::pending::<()>().await;
    });
    let (tx, _rx) = mpsc::channel(1);
    let (_events, events) = mpsc::channel(1);
    let endpoint = moq_noq::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    let mut connection = MoqConnection {
        sender: MoqSender {
            shared,
            tx,
            limits: Default::default(),
            audio_enabled: false,
        },
        events,
        monitor: monitor.clone(),
        endpoint,
        task: Some(task),
        end_timeout: Duration::from_secs(1),
    };
    let mut cancelling = Box::pin(connection.cancel());
    std::future::poll_fn(|cx| {
        assert!(cancelling.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    drop(cancelling);
    assert!(
        connection.task.is_some(),
        "cancelled join detached its supervisor"
    );
    drop(connection);
    tokio::time::timeout(Duration::from_secs(1), monitor.wait_idle())
        .await
        .unwrap();
    assert_eq!(monitor.active_tasks(), 0);
}
