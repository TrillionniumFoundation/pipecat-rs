//! Real owned-task outcomes and native resources, with cleanup before assertions.
use super::*;
use std::{
    net::{SocketAddr, UdpSocket},
    sync::atomic::AtomicBool,
};
type Outcome<T = ()> = std::result::Result<T, String>;
macro_rules! get {
    ($value:expr) => {
        $value.map_err(|error| format!("{error:?}"))?
    };
}
macro_rules! check {
    ($value:expr,$message:expr) => {
        if !$value {
            return Err($message.into());
        }
    };
}
#[derive(Clone, Copy, Debug)]
enum Failure {
    NormalCancellation,
    ActualWriterAbort,
    InjectedOwnedPanic,
}
struct Dropped(Arc<AtomicBool>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
struct Pair {
    a: SmallWebRtcConnection,
    b: SmallWebRtcConnection,
    addresses: Vec<SocketAddr>,
}
impl Pair {
    async fn new() -> Outcome<Self> {
        Self::with_timeout(Duration::from_secs(5)).await
    }
    async fn with_timeout(close_timeout: Duration) -> Outcome<Self> {
        let config = SmallWebRtcConfig {
            close_timeout,
            localhost_only: true,
            audio_codec: None,
            ..Default::default()
        };
        let a = get!(SmallWebRtcConnection::new(config.clone()).await);
        let b = match SmallWebRtcConnection::new(config).await {
            Ok(peer) => peer,
            Err(error) => {
                let cleanup = tokio::time::timeout(Duration::from_secs(8), a.close()).await;
                return Err(format!(
                    "second constructor={error:?}; first cleanup={cleanup:?}"
                ));
            }
        };
        Ok(Self {
            a,
            b,
            addresses: vec![],
        })
    }
    fn candidates(&mut self, sdp: &str) -> Outcome {
        for line in sdp.lines().filter(|line| line.starts_with("a=candidate:")) {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            check!(fields.len() >= 6, "invalid candidate");
            self.addresses
                .push(get!(format!("{}:{}", fields[4], fields[5]).parse()));
        }
        Ok(())
    }
    async fn connect(&mut self) -> Outcome {
        let offer = get!(self.a.create_offer().await);
        self.candidates(&offer.sdp)?;
        let answer = get!(self.b.answer_offer(offer.sdp).await);
        self.candidates(&answer.sdp)?;
        get!(self.a.accept_answer(answer).await);
        let (a, b) = tokio::join!(
            self.a.wait_data_channel(Duration::from_secs(5)),
            self.b.wait_data_channel(Duration::from_secs(5))
        );
        get!(a);
        get!(b);
        let request = serde_json::json!({"close_worker_control":"真实消息 🦀"});
        get!(self.a.send_app_message(&request).await);
        get!(self.a.flush(Duration::from_secs(2)).await);
        loop {
            match self.b.recv_event().await {
                Some(TransportEvent::AppMessage(value)) => {
                    check!(value == request, "positive data request differs");
                    break;
                }
                Some(TransportEvent::Error(error)) => return Err(error),
                None => return Err("peer closed before positive request".into()),
                _ => {}
            }
        }
        check!(
            !self.addresses.is_empty(),
            "missing gathered candidate sockets"
        );
        check!(
            self.addresses
                .iter()
                .all(|address| UdpSocket::bind(address).is_err()),
            "candidate was not actually held"
        );
        Ok(())
    }
    async fn observe_failure(&self, mode: Failure, dropped: Arc<AtomicBool>) -> Outcome {
        let count = self.a.tasks.lock().unwrap().len();
        check!(
            count == 1,
            "data-only peer must own exactly its real message writer"
        );
        match mode {
            Failure::NormalCancellation => self.a.cancel(),
            Failure::ActualWriterAbort => self.a.tasks.lock().unwrap()[0].abort(),
            Failure::InjectedOwnedPanic => {
                let handle = tokio::spawn(async move {
                    let _owned = Dropped(dropped);
                    panic!("deliberately injected owned worker panic");
                });
                self.a.tasks.lock().unwrap().push(handle);
            }
        }
        get!(
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let finished = {
                        let tasks = self.a.tasks.lock().unwrap();
                        match mode {
                            Failure::InjectedOwnedPanic => tasks.last().unwrap().is_finished(),
                            _ => tasks[0].is_finished(),
                        }
                    };
                    if finished {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
        );
        Ok(())
    }
}
async fn exercise(mode: Failure) {
    let mut pair = Pair::new().await.expect("owned native peer construction");
    let dropped = Arc::new(AtomicBool::new(false));
    let observation = tokio::time::timeout(Duration::from_secs(12), async {
        pair.connect().await?;
        pair.observe_failure(mode, dropped.clone()).await
    })
    .await;
    let (first, concurrent) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(8), pair.a.close()),
        tokio::time::timeout(Duration::from_secs(8), pair.a.close())
    );
    let second = tokio::time::timeout(Duration::from_secs(8), pair.b.close()).await;
    let cached = tokio::time::timeout(Duration::from_secs(2), pair.a.close()).await;
    let states = (pair.a.state(), pair.b.state());
    let worker_count = pair.a.tasks.lock().unwrap().len();
    let pending = pair.a.ctx.pending.count.load(Ordering::Acquire);
    let events_empty = matches!(
        pair.a.events.lock().await.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    );
    let channel_released = pair.a.ctx.channel.borrow().is_none();
    let peer_released = pair.a.pc.lock().unwrap().is_none();
    let output_closed = pair.a.outgoing_messages.is_closed();
    let sockets = tokio::time::timeout(Duration::from_secs(2), async {
        while pair
            .addresses
            .iter()
            .any(|address| UdpSocket::bind(address).is_err())
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    eprintln!("worker-close mode={mode:?}; observation={observation:?}; first={first:?}; concurrent={concurrent:?}; cached={cached:?}; other={second:?}; states={states:?}; owned_tasks={worker_count}; pending={pending}; events_empty={events_empty}; channel_released={channel_released}; peer_released={peer_released}; output_closed={output_closed}; owned_drop={}; sockets={sockets:?}",dropped.load(Ordering::Acquire));
    // Every owner cleanup runs before any observation or expected-error assertion.
    assert!(
        matches!(second, Ok(Ok(()))),
        "other peer cleanup failed: {second:?}"
    );
    assert!(
        matches!(observation, Ok(Ok(()))),
        "observation failed: {observation:?}"
    );
    assert_eq!(states, (ConnectionState::Closed, ConnectionState::Closed));
    assert!(
        worker_count == 0
            && pending == 0
            && events_empty
            && channel_released
            && peer_released
            && output_closed
            && sockets.is_ok(),
        "resource release failed"
    );
    assert_eq!(
        format!("{first:?}"),
        format!("{concurrent:?}"),
        "concurrent close result changed"
    );
    assert_eq!(
        format!("{first:?}"),
        format!("{cached:?}"),
        "cached close result changed"
    );
    let expected = match mode {
        Failure::NormalCancellation => None,
        Failure::ActualWriterAbort => {
            Some("owned transport worker failures: panicked=0, cancelled=1, timed_out=0")
        }
        Failure::InjectedOwnedPanic => {
            assert!(
                dropped.load(Ordering::Acquire),
                "injected task did not unwind its owned guard"
            );
            Some("owned transport worker failures: panicked=1, cancelled=0, timed_out=0")
        }
    };
    match expected {
        None=>assert!(matches!(first,Ok(Ok(()))),"normal token exit failed: {first:?}"),
        Some(expected)=>assert!(matches!(&first,Ok(Err(Error::Shutdown(message))) if message==expected),"close falsely reported success or lost the actual owned-worker outcome: {first:?}; expected {expected}"),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn normal_token_cancellation_preserves_success_and_releases_native_resources() {
    exercise(Failure::NormalCancellation).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn actual_message_writer_abort_is_reported_after_resource_release() {
    exercise(Failure::ActualWriterAbort).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn injected_owned_worker_panic_is_reported_after_resource_release() {
    exercise(Failure::InjectedOwnedPanic).await;
}

struct AbortDrop {
    dropped: Arc<AtomicBool>,
    panic: bool,
}
impl Drop for AbortDrop {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
        assert!(!self.panic, "deliberate panic during owned task abort");
    }
}
async fn released(peer: &SmallWebRtcConnection) -> bool {
    peer.state() == ConnectionState::Closed
        && peer.tasks.lock().unwrap().is_empty()
        && peer.ctx.pending.count.load(Ordering::Acquire) == 0
        && peer.ctx.channel.borrow().is_none()
        && peer.pc.lock().unwrap().is_none()
        && peer.outgoing_messages.is_closed()
        && matches!(
            peer.events.lock().await.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        )
}
async fn sockets_released(addresses: &[SocketAddr]) -> bool {
    tokio::time::timeout(Duration::from_secs(2), async {
        while addresses
            .iter()
            .any(|address| UdpSocket::bind(address).is_err())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}
async fn forced_worker_deadline(panic_on_abort: bool) {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    let mut pair = Pair::with_timeout(Duration::from_millis(100))
        .await
        .expect("native pair");
    let mut drops = vec![];
    let observation = tokio::time::timeout(Duration::from_secs(12), async {
        pair.connect().await?;
        // Two consecutive held tasks force the public 100 ms close deadline to
        // expire before owned cleanup finishes. Both hold genuine Pending guards.
        for index in 0..2 {
            let dropped = Arc::new(AtomicBool::new(false));
            drops.push(dropped.clone());
            let queued = Queued::new(serde_json::json!({"held": index}), 0, &pair.a.ctx.pending);
            let (started, ready) = tokio::sync::oneshot::channel();
            let handle = tokio::spawn(async move {
                let _queued = queued;
                let _guard = AbortDrop {
                    dropped,
                    panic: panic_on_abort && index == 0,
                };
                let _ = started.send(());
                std::future::pending::<()>().await;
            });
            pair.a.tasks.lock().unwrap().push(handle);
            get!(ready.await);
        }
        check!(
            pair.a.ctx.pending.count.load(Ordering::Acquire) == 2,
            "held guards not observable"
        );
        {
            let mut caller = Box::pin(pair.a.close());
            get!(
                poll_fn(|cx| Poll::Ready(match caller.as_mut().poll(cx) {
                    Poll::Pending => Ok(()),
                    Poll::Ready(value) => Err(format!("close not genuinely pending: {value:?}")),
                }))
                .await
            );
            // Drop the actually-polled close caller; the shared cleanup stays owned.
        }
        Ok::<(), String>(())
    })
    .await;
    let (first, concurrent) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(3), pair.a.close()),
        tokio::time::timeout(Duration::from_secs(3), pair.a.close())
    );
    let settled = tokio::time::timeout(Duration::from_secs(3), async {
        let mut state = pair.a.subscribe_state();
        while *state.borrow_and_update() != ConnectionState::Closed {
            get!(state.changed().await);
        }
        Ok::<(), String>(())
    })
    .await;
    let cached = tokio::time::timeout(Duration::from_secs(2), pair.a.close()).await;
    let repeated = tokio::time::timeout(Duration::from_secs(2), pair.a.close()).await;
    let other = tokio::time::timeout(Duration::from_secs(3), pair.b.close()).await;
    let resources = released(&pair.a).await && released(&pair.b).await;
    let sockets = sockets_released(&pair.addresses).await;
    let dropped = drops.iter().all(|flag| flag.load(Ordering::Acquire));
    eprintln!("worker-deadline panic_on_abort={panic_on_abort}; observation={observation:?}; first={first:?}; concurrent={concurrent:?}; settled={settled:?}; cached={cached:?}; repeated={repeated:?}; other={other:?}; resources={resources}; sockets={sockets}; owned_drops={dropped}");
    assert!(matches!(observation, Ok(Ok(()))), "{observation:?}");
    assert!(matches!(other, Ok(Ok(()))), "other close: {other:?}");
    assert!(matches!(settled, Ok(Ok(()))) && resources && sockets && dropped);
    assert!(
        matches!(first, Ok(Err(Error::Timeout("peer shutdown")))),
        "first close must time out while owned workers remain: {first:?}"
    );
    let expected = format!(
        "owned transport worker failures: panicked={}, cancelled=0, timed_out=2",
        usize::from(panic_on_abort)
    );
    assert!(
        matches!(&cached,Ok(Err(Error::Shutdown(text))) if text==&expected),
        "{cached:?}"
    );
    assert_eq!(format!("{cached:?}"), format!("{repeated:?}"));
    assert!(
        matches!(&concurrent, Ok(Err(Error::Timeout("peer shutdown"))))
            || format!("{concurrent:?}") == format!("{cached:?}"),
        "concurrent close: {concurrent:?}"
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_worker_abort_survives_cancelled_and_concurrent_close_callers() {
    forced_worker_deadline(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panic_during_fallback_abort_is_preserved_after_all_joins() {
    forced_worker_deadline(true).await;
}
#[test]
fn native_and_worker_errors_are_both_preserved_without_panic_payloads() {
    let worker = "owned transport worker failures: panicked=1, cancelled=2, timed_out=3".to_owned();
    assert_eq!(combine_close_results(Ok(()), None), Ok(()));
    assert_eq!(
        combine_close_results(Err("native close failure".into()), None),
        Err("native close failure".into())
    );
    assert_eq!(
        combine_close_results(Ok(()), Some(worker.clone())),
        Err(worker.clone())
    );
    assert_eq!(
        combine_close_results(Err("native close failure".into()), Some(worker.clone())),
        Err(format!("native close failure\n{worker}"))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registry_reports_worker_failure_after_closing_every_admitted_peer() {
    use crate::{ConnectionMode, SmallWebRtcRequest, SmallWebRtcRequestHandler};
    let config = SmallWebRtcConfig {
        localhost_only: true,
        audio_codec: None,
        ..Default::default()
    };
    let registry =
        SmallWebRtcRequestHandler::new(config.clone(), ConnectionMode::Multiple, 2).unwrap();
    let mut clients = vec![];
    let mut servers = vec![];
    let mut addresses = vec![];
    let observation = tokio::time::timeout(Duration::from_secs(15), async {
        for index in 0..2 {
            clients.push(get!(SmallWebRtcConnection::new(config.clone()).await));
            let client = clients.last().unwrap();
            let offer = get!(client.create_offer().await);
            let (answer, server) = get!(
                registry
                    .handle_offer(SmallWebRtcRequest {
                        sdp: offer.sdp.clone(),
                        sdp_type: "offer".into(),
                        pc_id: None,
                        restart_pc: false,
                        request_data: None
                    })
                    .await
            );
            servers.push(server);
            for sdp in [&offer.sdp, &answer.sdp] {
                for line in sdp.lines().filter(|line| line.starts_with("a=candidate:")) {
                    let fields = line.split_whitespace().collect::<Vec<_>>();
                    check!(fields.len() >= 6, "invalid candidate");
                    addresses.push(get!(
                        format!("{}:{}", fields[4], fields[5]).parse::<SocketAddr>()
                    ));
                }
            }
            get!(
                client
                    .accept_answer(get!(RTCSessionDescription::answer(answer.sdp)))
                    .await
            );
            let server = servers.last().unwrap();
            get!(client.wait_data_channel(Duration::from_secs(5)).await);
            get!(server.wait_data_channel(Duration::from_secs(5)).await);
            let value = serde_json::json!({"registry_control":index});
            get!(client.send_app_message(&value).await);
            get!(client.flush(Duration::from_secs(2)).await);
            loop {
                match server.recv_event().await {
                    Some(TransportEvent::AppMessage(actual)) => {
                        check!(actual == value, "registry positive data mismatch");
                        break;
                    }
                    Some(TransportEvent::Error(error)) => return Err(error),
                    None => return Err("registry peer closed early".into()),
                    _ => {}
                }
            }
        }
        check!(
            registry.len().await == 2,
            "registry did not retain both peers"
        );
        check!(
            !addresses.is_empty()
                && addresses
                    .iter()
                    .all(|address| UdpSocket::bind(address).is_err()),
            "native sockets not held"
        );
        check!(
            servers[0].tasks.lock().unwrap().len() == 1,
            "expected actual message writer"
        );
        servers[0].tasks.lock().unwrap()[0].abort();
        while !servers[0].tasks.lock().unwrap()[0].is_finished() {
            tokio::task::yield_now().await;
        }
        Ok::<(), String>(())
    })
    .await;
    let first = tokio::time::timeout(Duration::from_secs(12), registry.close()).await;
    let cached = tokio::time::timeout(Duration::from_secs(2), registry.close()).await;
    let mut cleanup = vec![];
    for client in &clients {
        cleanup.push(tokio::time::timeout(Duration::from_secs(8), client.close()).await);
    }
    // Retained public server wrappers are explicitly closed again and inspected.
    let mut server_results = vec![];
    let mut resources = true;
    for server in &servers {
        server_results.push(tokio::time::timeout(Duration::from_secs(8), server.close()).await);
        resources &= released(server).await;
    }
    for client in &clients {
        resources &= released(client).await;
    }
    let sockets = sockets_released(&addresses).await;
    let empty = registry.is_empty().await;
    eprintln!("registry-worker observation={observation:?}; first={first:?}; cached={cached:?}; clients={cleanup:?}; servers={server_results:?}; registry_empty={empty}; resources={resources}; sockets={sockets}");
    assert!(matches!(observation, Ok(Ok(()))), "{observation:?}");
    assert!(cleanup.iter().all(|result| matches!(result, Ok(Ok(())))));
    assert!(empty && resources && sockets);
    let expected = "owned transport worker failures: panicked=0, cancelled=1, timed_out=0";
    assert!(
        matches!(&first,Ok(Err(Error::Shutdown(text))) if text==&format!("peer shutdown failed: {expected}")),
        "{first:?}"
    );
    assert_eq!(format!("{first:?}"), format!("{cached:?}"));
    assert!(matches!(&server_results[0],Ok(Err(Error::Shutdown(text))) if text==expected));
    assert!(matches!(&server_results[1], Ok(Ok(()))));
}
