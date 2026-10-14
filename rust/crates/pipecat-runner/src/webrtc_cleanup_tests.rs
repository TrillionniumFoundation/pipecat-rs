//! Actual Tokio task failures through the session's final cleanup path. Registry
//! error-value injection is explicit; no synthetic failure is called a peer fault.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::task::JoinSet;

async fn bounded_finish<T, N: Send + 'static>(
    primary: Result<T, RunnerError>,
    close: impl std::future::Future<Output = pipecat_webrtc::Result<()>>,
    negotiations: &mut JoinSet<N>,
) -> Result<T, RunnerError> {
    match tokio::time::timeout(
        Duration::from_secs(3),
        finish_signaling(primary, close, negotiations),
    )
    .await
    {
        Ok(result) => result,
        Err(_) => {
            negotiations.abort_all();
            while negotiations.join_next().await.is_some() {}
            panic!("cleanup fixture deadline after abort-and-join fallback");
        }
    }
}

#[tokio::test]
async fn preserves_primary_registry_failure_and_both_typed_errors_after_drain() {
    for (primary_fails, close_fails) in [(false, false), (true, false), (false, true), (true, true)]
    {
        let closed = Arc::new(AtomicBool::new(false));
        let observed = closed.clone();
        let (release, released) = oneshot::channel();
        let mut negotiations = JoinSet::new();
        negotiations.spawn(async move {
            released.await.unwrap();
            assert!(observed.load(Ordering::SeqCst));
        });
        let result = bounded_finish(
            if primary_fails {
                Err(RunnerError::Deadline)
            } else {
                Ok(17)
            },
            async {
                closed.store(true, Ordering::SeqCst);
                let _ = release.send(());
                if close_fails {
                    Err(pipecat_webrtc::Error::Shutdown(
                        "injected registry close failure".into(),
                    ))
                } else {
                    Ok(())
                }
            },
            &mut negotiations,
        )
        .await;
        assert!(negotiations.is_empty());
        assert!(closed.load(Ordering::SeqCst));
        match (primary_fails, close_fails, result) {
            (false, false, Ok(17)) | (true, false, Err(RunnerError::Deadline)) => {}
            (
                false,
                true,
                Err(RunnerError::WebRtcCleanup(pipecat_webrtc::Error::Shutdown(detail))),
            ) => {
                assert_eq!(detail, "injected registry close failure");
            }
            (true, true, Err(error @ RunnerError::SessionCleanup { .. })) => {
                let source = std::error::Error::source(&error).unwrap();
                assert_eq!(source.to_string(), RunnerError::Deadline.to_string());
                assert!(source
                    .downcast_ref::<Box<RunnerError>>()
                    .is_some_and(|e| matches!(**e, RunnerError::Deadline)));
                let RunnerError::SessionCleanup { primary, cleanup } = error else {
                    unreachable!()
                };
                assert!(matches!(*primary, RunnerError::Deadline));
                assert!(
                    matches!(*cleanup, RunnerError::WebRtcCleanup(pipecat_webrtc::Error::Shutdown(ref s)) if s == "injected registry close failure")
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn actual_panics_and_aborts_are_counted_after_every_task_is_joined() {
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut negotiations = JoinSet::<pipecat_webrtc::Result<()>>::new();
    let (release, released) = oneshot::channel();
    let late_drop = Dropped(dropped.clone());
    negotiations.spawn(async move {
        let _guard = late_drop;
        released.await.unwrap();
        Ok(())
    });
    let panic_drop = Dropped(dropped.clone());
    negotiations.spawn(async move {
        let _guard = panic_drop;
        panic!("synthetic-private-panic-payload");
    });
    let abort_drop = Dropped(dropped.clone());
    let (started, started_rx) = oneshot::channel();
    let aborted = negotiations.spawn(async move {
        let _guard = abort_drop;
        let _ = started.send(());
        std::future::pending::<()>().await;
        Ok(())
    });
    started_rx.await.unwrap();
    aborted.abort();
    let result = bounded_finish(
        Ok(17),
        async {
            let _ = release.send(());
            Ok(())
        },
        &mut negotiations,
    )
    .await
    .unwrap_err();
    assert!(negotiations.is_empty());
    assert_eq!(dropped.load(Ordering::SeqCst), 3);
    assert!(matches!(
        result,
        RunnerError::WebRtcNegotiationTasks {
            panicked: 1,
            cancelled: 1
        }
    ));
    assert!(!format!("{result:?} {result}").contains("synthetic-private-panic-payload"));
}

#[tokio::test]
async fn primary_and_close_failure_do_not_hide_later_real_join_failure() {
    let mut negotiations = JoinSet::<()>::new();
    negotiations.spawn(async { panic!("synthetic negotiation panic") });
    let error = bounded_finish::<(), _>(
        Err(RunnerError::Deadline),
        async { Err(pipecat_webrtc::Error::Shutdown("injected close".into())) },
        &mut negotiations,
    )
    .await
    .unwrap_err();
    assert!(negotiations.is_empty());
    let RunnerError::SessionCleanup { primary, cleanup } = error else {
        panic!("missing task aggregate")
    };
    assert!(matches!(
        *cleanup,
        RunnerError::WebRtcNegotiationTasks {
            panicked: 1,
            cancelled: 0
        }
    ));
    let RunnerError::SessionCleanup { primary, cleanup } = *primary else {
        panic!("missing close aggregate")
    };
    assert!(matches!(*primary, RunnerError::Deadline));
    assert!(
        matches!(*cleanup, RunnerError::WebRtcCleanup(pipecat_webrtc::Error::Shutdown(ref s)) if s == "injected close")
    );
}

#[tokio::test]
async fn ordinary_offer_closed_result_is_not_a_task_failure() {
    let mut negotiations = JoinSet::new();
    negotiations.spawn(async { Err::<(), _>(pipecat_webrtc::Error::Closed) });
    let result = bounded_finish(Ok(17), async { Ok(()) }, &mut negotiations).await;
    assert_eq!(result.unwrap(), 17);
    assert!(negotiations.is_empty());
}

#[tokio::test]
async fn initial_join_failure_retains_kind_without_exposing_payload() {
    for panic in [false, true] {
        let mut tasks = JoinSet::<()>::new();
        let abort = tasks.spawn(async move {
            if panic {
                panic!("private initial negotiation payload")
            }
            std::future::pending::<()>().await;
        });
        if !panic {
            abort.abort();
        }
        let error = tasks.join_next().await.unwrap().unwrap_err();
        let error = negotiation_task_error(error);
        assert!(
            matches!(error, RunnerError::WebRtcNegotiationTasks { panicked, cancelled } if panicked == usize::from(panic) && cancelled == usize::from(!panic))
        );
        assert!(!format!("{error:?} {error}").contains("private initial negotiation payload"));
        assert!(tasks.is_empty());
    }
}

#[tokio::test]
async fn real_registry_close_precedes_join_and_releases_retained_peer_sockets() {
    use pipecat_webrtc::ConnectionState;
    use std::net::{SocketAddr, UdpSocket};
    let config = SmallWebRtcConfig {
        audio_codec: None,
        localhost_only: true,
        gather_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let client = SmallWebRtcConnection::new(config.clone()).await.unwrap();
    let handler = SmallWebRtcRequestHandler::new(config, ConnectionMode::Single, 1).unwrap();
    let observation = tokio::time::timeout(Duration::from_secs(5), async {
        let offer = client.create_offer().await.map_err(|e| e.to_string())?;
        let (answer, peer) = handler
            .handle_offer(SmallWebRtcRequest {
                sdp: offer.sdp,
                sdp_type: "offer".into(),
                pc_id: None,
                restart_pc: false,
                request_data: None,
            })
            .await
            .map_err(|e| e.to_string())?;
        let addresses = answer
            .sdp
            .lines()
            .filter(|line| line.starts_with("a=candidate:"))
            .map(|line| {
                let fields = line.split_whitespace().collect::<Vec<_>>();
                if fields.len() < 6 {
                    return Err("candidate fields".to_string());
                }
                format!("{}:{}", fields[4], fields[5])
                    .parse::<SocketAddr>()
                    .map_err(|e| e.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        if addresses.is_empty()
            || addresses
                .iter()
                .any(|address| UdpSocket::bind(address).is_ok())
        {
            return Err("registry candidate socket was not held before close".to_string());
        }
        Ok::<_, String>((peer, addresses))
    })
    .await;
    let (released, release) = oneshot::channel();
    let mut negotiations = JoinSet::new();
    negotiations.spawn(async move {
        release.await.unwrap();
        Err::<(), _>(pipecat_webrtc::Error::Closed)
    });
    let closed = tokio::time::timeout(
        Duration::from_secs(6),
        finish_signaling(
            Ok(17),
            async {
                let result = handler.close().await;
                let _ = released.send(());
                result
            },
            &mut negotiations,
        ),
    )
    .await;
    // Always close both native endpoints before inspecting success/failure.
    let cached = handler.close().await;
    let client_closed = client.close().await;
    let mut fallback_joins = Vec::new();
    while let Some(outcome) = negotiations.join_next().await {
        fallback_joins.push(outcome);
    }
    let sockets_released = if let Ok(Ok((_, addresses))) = &observation {
        !addresses.is_empty()
            && tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    if addresses
                        .iter()
                        .all(|address| UdpSocket::bind(address).is_ok())
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok()
    } else {
        false
    };
    let registry_empty = handler.is_empty().await;
    eprintln!("registry cleanup result={closed:?}; cached={cached:?}; client={client_closed:?}; empty={registry_empty}; sockets_released={sockets_released}");
    assert!(matches!(closed, Ok(Ok(17))));
    assert!(
        fallback_joins.is_empty(),
        "unexpected fallback joins: {fallback_joins:?}"
    );
    assert!(cached.is_ok() && client_closed.is_ok());
    assert!(negotiations.is_empty());
    assert!(registry_empty && sockets_released);
    let (peer, _) = observation
        .expect("offer deadline")
        .expect("actual registry offer");
    assert_eq!(peer.state(), ConnectionState::Closed);
}
