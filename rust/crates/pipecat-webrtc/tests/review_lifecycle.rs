//! Regression coverage for cancellation-safe peer shutdown using real UDP sockets.
use pipecat_webrtc::{AudioCodec, SmallWebRtcConfig, SmallWebRtcConnection};
use std::{
    future::{poll_fn, Future},
    net::{SocketAddr, UdpSocket},
    task::Poll,
    time::Duration,
};

fn config() -> SmallWebRtcConfig {
    SmallWebRtcConfig {
        localhost_only: true,
        audio_codec: Some(AudioCodec::Pcmu),
        ..Default::default()
    }
}

fn candidate_addresses(sdp: &str) -> Vec<SocketAddr> {
    sdp.lines()
        .filter(|line| line.starts_with("a=candidate:"))
        .map(|line| {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            format!("{}:{}", fields[4], fields[5]).parse().unwrap()
        })
        .collect()
}

#[tokio::test]
async fn cancelling_then_retrying_close_releases_all_gathered_udp_sockets() {
    let a = SmallWebRtcConnection::new(config()).await.unwrap();
    let b = SmallWebRtcConnection::new(config()).await.unwrap();
    let offer = a.create_offer().await.unwrap();
    let addresses = candidate_addresses(&offer.sdp);
    assert!(!addresses.is_empty());
    let answer = b.answer_offer(offer.sdp).await.unwrap();
    a.accept_answer(answer).await.unwrap();
    a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
    for address in &addresses {
        assert!(
            UdpSocket::bind(address).is_err(),
            "candidate must be held before close"
        );
    }
    {
        let close = a.close();
        tokio::pin!(close);
        poll_fn(|cx| {
            assert!(
                close.as_mut().poll(cx).is_pending(),
                "close must yield for cancellation test"
            );
            Poll::Ready(())
        })
        .await;
    }
    a.close().await.unwrap();
    b.close().await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    for address in addresses {
        let bound = UdpSocket::bind(address);
        assert!(
            bound.is_ok(),
            "cancelled close leaked UDP candidate {address}: {bound:?}"
        );
    }
}

#[tokio::test]
async fn cancelling_registry_close_does_not_leave_retained_peers_running() {
    use pipecat_webrtc::{ConnectionMode, SmallWebRtcRequest, SmallWebRtcRequestHandler};
    let handler = SmallWebRtcRequestHandler::new(config(), ConnectionMode::Multiple, 2).unwrap();
    let mut callers = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..2 {
        let caller = SmallWebRtcConnection::new(config()).await.unwrap();
        let offer = caller.create_offer().await.unwrap();
        let (_, peer) = handler
            .handle_offer(SmallWebRtcRequest {
                sdp: offer.sdp,
                sdp_type: "offer".into(),
                pc_id: None,
                restart_pc: false,
                request_data: None,
            })
            .await
            .unwrap();
        peers.push(peer);
        callers.push(caller);
    }
    {
        let close = handler.close();
        tokio::pin!(close);
        poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    handler.close().await.unwrap();
    let all_closed = peers.iter().all(|peer| peer.is_cancelled());
    for peer in peers {
        peer.close().await.unwrap();
    }
    for caller in callers {
        caller.close().await.unwrap();
    }
    assert!(
        all_closed,
        "registry lost retained peers when its close future was cancelled"
    );
}

async fn connected_pair(
    close_timeout: Duration,
) -> (
    SmallWebRtcConnection,
    SmallWebRtcConnection,
    Vec<SocketAddr>,
) {
    let a = SmallWebRtcConnection::new(SmallWebRtcConfig {
        close_timeout,
        ..config()
    })
    .await
    .unwrap();
    let b = SmallWebRtcConnection::new(config()).await.unwrap();
    let offer = a.create_offer().await.unwrap();
    let addresses = candidate_addresses(&offer.sdp);
    let answer = b.answer_offer(offer.sdp).await.unwrap();
    a.accept_answer(answer).await.unwrap();
    a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
    (a, b, addresses)
}

async fn assert_sockets_released(addresses: &[SocketAddr]) {
    tokio::time::timeout(Duration::from_secs(2), async {
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
    .expect("all candidate UDP sockets must be released");
}

#[tokio::test]
async fn normal_close_releases_native_peer_while_public_wrapper_is_retained() {
    use pipecat_webrtc::{ConnectionState, Error, RTCIceCandidateInit};
    let (a, b, addresses) = connected_pair(Duration::from_secs(5)).await;
    a.close().await.unwrap();
    assert_sockets_released(&addresses).await;
    assert_eq!(a.state(), ConnectionState::Closed);
    assert!(matches!(a.create_offer().await, Err(Error::Closed)));
    assert!(matches!(a.create_offer_trickle().await, Err(Error::Closed)));
    assert!(matches!(
        a.answer_offer("invalid SDP").await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        a.add_ice_candidate(RTCIceCandidateInit::default()).await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        a.wait_connected(Duration::from_millis(10)).await,
        Err(Error::Closed)
    ));
    assert!(matches!(
        a.wait_data_channel(Duration::from_millis(10)).await,
        Err(Error::Closed)
    ));
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn cancelled_close_followed_by_drop_keeps_cleanup_owned() {
    let (a, b, addresses) = connected_pair(Duration::from_secs(5)).await;
    {
        let close = a.close();
        tokio::pin!(close);
        poll_fn(|cx| {
            assert!(close.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
    }
    drop(a);
    assert_sockets_released(&addresses).await;
    b.close().await.unwrap();
}

#[tokio::test]
async fn short_close_deadline_keeps_cleanup_owned_for_a_retry() {
    use pipecat_webrtc::{ConnectionState, Error};
    let (a, b, addresses) = connected_pair(Duration::from_nanos(1)).await;
    let mut state = a.subscribe_state();
    assert!(matches!(
        a.close().await,
        Ok(()) | Err(Error::Timeout("peer shutdown"))
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while *state.borrow_and_update() != ConnectionState::Closed {
            state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    a.close().await.unwrap();
    assert_sockets_released(&addresses).await;
    b.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_peer_shutdown_schedules_release_both_udp_ports() {
    use pipecat_webrtc::ConnectionState;
    // Verify all ports before creating the next batch: otherwise the OS can
    // legitimately reuse a closed peer's port for the next iteration's peer.
    for iteration in 0..32 {
        let mut jobs = tokio::task::JoinSet::new();
        for schedule in 0..6 {
            jobs.spawn(async move {
                let cfg = SmallWebRtcConfig {
                    audio_codec: None,
                    ..config()
                };
                let a = SmallWebRtcConnection::new(cfg.clone()).await.unwrap();
                let b = SmallWebRtcConnection::new(cfg).await.unwrap();
                let offer = a.create_offer().await.unwrap();
                let mut addresses = candidate_addresses(&offer.sdp);
                let answer = b.answer_offer(offer.sdp).await.unwrap();
                addresses.extend(candidate_addresses(&answer.sdp));
                a.accept_answer(answer).await.unwrap();
                if schedule != 5 {
                    a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
                    b.wait_data_channel(Duration::from_secs(5)).await.unwrap();
                }
                match schedule {
                    0 => {
                        a.close().await.unwrap();
                        b.close().await.unwrap();
                    }
                    1 => {
                        b.close().await.unwrap();
                        a.close().await.unwrap();
                    }
                    2 | 5 => {
                        let (left, right) = tokio::join!(a.close(), b.close());
                        left.unwrap_or_else(|e| {
                            panic!("left close schedule={schedule}, iteration={iteration}: {e}")
                        });
                        right.unwrap_or_else(|e| {
                            panic!("right close schedule={schedule}, iteration={iteration}: {e}")
                        });
                    }
                    3 => {
                        drop(a);
                        b.close().await.unwrap();
                        return (schedule, addresses, None, Some(b));
                    }
                    4 => {
                        drop(b);
                        a.close().await.unwrap();
                        return (schedule, addresses, Some(a), None);
                    }
                    _ => unreachable!(),
                }
                assert_eq!(a.state(), ConnectionState::Closed);
                assert_eq!(b.state(), ConnectionState::Closed);
                (schedule, addresses, Some(a), Some(b))
            });
        }
        let mut retained = Vec::new();
        while let Some(result) = jobs.join_next().await {
            retained.push(result.unwrap());
        }
        for (schedule, addresses, _, _) in &retained {
            tokio::time::timeout(Duration::from_secs(2), async {
                while addresses
                    .iter()
                    .any(|address| UdpSocket::bind(address).is_err())
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "UDP leak schedule={schedule}, iteration={iteration}, addresses={addresses:?}"
                )
            });
        }
    }
}
