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
        events: Some(events),
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

fn shared() -> Arc<Shared> {
    Arc::new(Shared {
        accepting: Mutex::new(true),
        shutdown: watch::channel(Shutdown::Running).0,
        interrupt: watch::channel(0).0,
        tasks: AtomicUsize::new(0),
        idle: Notify::new(),
    })
}

#[test]
fn final_commit_rechecks_generation_after_a_barrier() {
    let shared = shared();
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    let written = Arc::new(std::sync::atomic::AtomicBool::new(false));
    std::thread::scope(|scope| {
        let writer_shared = shared.clone();
        let writer_entered = entered.clone();
        let writer_release = release.clone();
        let writer_written = written.clone();
        let writer = scope.spawn(move || {
            // This is the earlier admission/writer check. Interruption occurs
            // deterministically between this check and the final track commit.
            assert_eq!(*writer_shared.interrupt.borrow(), 0);
            writer_entered.wait();
            writer_release.wait();
            writer_shared.commit(Some(0), || {
                writer_written.store(true, Ordering::Release);
                Ok(())
            })
        });
        entered.wait();
        let (tx, _rx) = mpsc::channel(1);
        MoqSender {
            shared: shared.clone(),
            tx,
            limits: Default::default(),
            audio_enabled: true,
        }
        .interrupt_now()
        .unwrap();
        release.wait();
        assert_eq!(writer.join().unwrap(), Err(Error::Interrupted));
    });
    assert!(!written.load(Ordering::Acquire));
}

#[test]
fn synchronous_commit_holds_the_same_lock_as_interrupt_and_cancel() {
    let shared = shared();
    let entered = Arc::new(std::sync::Barrier::new(2));
    let release = Arc::new(std::sync::Barrier::new(2));
    std::thread::scope(|scope| {
        let writer_shared = shared.clone();
        let writer_entered = entered.clone();
        let writer_release = release.clone();
        let writer = scope.spawn(move || {
            writer_shared.commit(Some(0), || {
                writer_entered.wait();
                writer_release.wait();
                Ok(())
            })
        });
        entered.wait();
        let locked = matches!(
            shared.accepting.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        release.wait();
        assert_eq!(writer.join().unwrap(), Ok(()));
        assert!(
            locked,
            "commit did not hold the admission/interruption lock"
        );
    });
}

#[test]
fn end_drains_admitted_commits_but_cancel_rejects_even_unfenced_json() {
    let shared = shared();
    shared.stop(Shutdown::End);
    assert_eq!(shared.commit(Some(0), || Ok(())), Ok(()));
    assert_eq!(shared.commit(None, || Ok(())), Ok(()));
    shared.interrupt.send_replace(1);
    assert_eq!(shared.commit(Some(0), || Ok(())), Err(Error::Interrupted));
    assert_eq!(shared.commit(None, || Ok(())), Ok(()));
    shared.stop(Shutdown::Cancel);
    assert_eq!(
        shared.commit(None, || panic!("cancelled commit ran")),
        Err(Error::Closed)
    );
}
