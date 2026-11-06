//! Real loopback regressions for transport starts racing irreversible shutdown.
use super::*;
use std::net::{SocketAddr, UdpSocket};
use webrtc::dtls_transport::{dtls_transport_state::RTCDtlsTransportState, RTCDtlsTransport};

fn hold_connecting(
    transport: &Arc<RTCDtlsTransport>,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let mut entered = Some(entered_tx);
    let mut release = Some(release_rx);
    transport.on_state_change(Box::new(move |state| {
        let wait = if state == RTCDtlsTransportState::Connecting {
            Some((entered.take().unwrap(), release.take().unwrap()))
        } else {
            None
        };
        Box::pin(async move {
            if let Some((entered, release)) = wait {
                let _ = entered.send(());
                let _ = release.await;
            }
        })
    }));
    (entered_rx, release_tx)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_during_dtls_connecting_callback_releases_both_udp_ports() {
    let config = SmallWebRtcConfig {
        localhost_only: true,
        audio_codec: None,
        ..Default::default()
    };
    let a = Arc::new(SmallWebRtcConnection::new(config.clone()).await.unwrap());
    let b = Arc::new(SmallWebRtcConnection::new(config).await.unwrap());
    let a_dtls = a.peer().unwrap().dtls_transport();
    let b_dtls = b.peer().unwrap().dtls_transport();
    let (a_entered, a_release) = hold_connecting(&a_dtls);
    let (b_entered, b_release) = hold_connecting(&b_dtls);

    let offer = a.create_offer().await.unwrap();
    let mut addresses = candidate_addresses(&offer.sdp);
    assert!(
        !addresses.is_empty(),
        "offerer must gather a real localhost candidate"
    );
    let answer = b.answer_offer(offer.sdp).await.unwrap();
    let answer_addresses = candidate_addresses(&answer.sdp);
    assert!(
        !answer_addresses.is_empty(),
        "answerer must gather a real localhost candidate"
    );
    addresses.extend(answer_addresses);
    addresses.sort_unstable();
    addresses.dedup();
    assert!(
        addresses.len() >= 2,
        "both peers must bind distinct UDP candidates"
    );
    a.accept_answer(answer).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        a_entered.await.unwrap();
        b_entered.await.unwrap();
    })
    .await
    .expect("both peers must reach DTLS Connecting before shutdown");
    for address in &addresses {
        assert!(
            UdpSocket::bind(address).is_err(),
            "candidate must be held before close"
        );
    }

    let a_close = {
        let a = Arc::clone(&a);
        tokio::spawn(async move { a.close().await })
    };
    let b_close = {
        let b = Arc::clone(&b);
        tokio::spawn(async move { b.close().await })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while a_dtls.state() != RTCDtlsTransportState::Closed
            || b_dtls.state() != RTCDtlsTransportState::Closed
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("stop must fence starts before awaiting the Connecting callbacks");
    a_release.send(()).unwrap();
    b_release.send(()).unwrap();
    a_close.await.unwrap().unwrap();
    b_close.await.unwrap().unwrap();
    assert_eq!(a.state(), ConnectionState::Closed);
    assert_eq!(b.state(), ConnectionState::Closed);
    drop((a_dtls, b_dtls));

    // Keep the public connection handles alive. Close must release the native
    // peer and handshake workers; dropping an entire runtime is not cleanup.
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
    .expect("DTLS starts resumed after stop retained real UDP candidates");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_cancels_an_already_pending_native_dtls_handshake() {
    use std::{
        future::{poll_fn, Future},
        task::Poll,
    };
    use webrtc::{
        api::{setting_engine::SettingEngine, APIBuilder},
        ice::{mdns::MulticastDnsMode, network_type::NetworkType},
        ice_transport::{ice_gatherer::RTCIceGatherOptions, ice_role::RTCIceRole},
    };

    let mut setting = SettingEngine::default();
    setting.set_network_types(vec![NetworkType::Udp4]);
    setting.set_ice_multicast_dns_mode(MulticastDnsMode::Disabled);
    setting.set_include_loopback_candidate(true);
    setting.set_ip_filter(Box::new(|ip| ip.is_loopback()));
    let loopback = webrtc::util::vnet::interface::Interface::new(
        "lo".into(),
        vec!["127.0.0.1/8".parse().unwrap()],
    );
    setting.set_vnet(Some(Arc::new(webrtc::util::vnet::net::Net::Ifs(vec![
        loopback,
    ]))));
    let api = APIBuilder::new().with_setting_engine(setting).build();
    let a_gatherer = Arc::new(
        api.new_ice_gatherer(RTCIceGatherOptions::default())
            .unwrap(),
    );
    let b_gatherer = Arc::new(
        api.new_ice_gatherer(RTCIceGatherOptions::default())
            .unwrap(),
    );
    for gatherer in [&a_gatherer, &b_gatherer] {
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let mut done_tx = Some(done_tx);
        gatherer.on_local_candidate(Box::new(move |candidate| {
            let done = candidate.is_none().then(|| done_tx.take()).flatten();
            Box::pin(async move {
                if let Some(done) = done {
                    let _ = done.send(());
                }
            })
        }));
        gatherer.gather().await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), done_rx)
            .await
            .unwrap()
            .unwrap();
    }
    let a_candidates = a_gatherer.get_local_candidates().await.unwrap();
    let b_candidates = b_gatherer.get_local_candidates().await.unwrap();
    let addresses: Vec<SocketAddr> = a_candidates
        .iter()
        .chain(b_candidates.iter())
        .map(|candidate| {
            format!("{}:{}", candidate.address, candidate.port)
                .parse()
                .unwrap()
        })
        .collect();
    assert!(addresses.len() >= 2);
    let a_ice = Arc::new(api.new_ice_transport(Arc::clone(&a_gatherer)));
    let b_ice = Arc::new(api.new_ice_transport(Arc::clone(&b_gatherer)));
    a_ice.set_remote_candidates(&b_candidates).await.unwrap();
    b_ice.set_remote_candidates(&a_candidates).await.unwrap();
    let a_parameters = a_gatherer.get_local_parameters().await.unwrap();
    let b_parameters = b_gatherer.get_local_parameters().await.unwrap();
    let (a_started, b_started) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            a_ice.start(&b_parameters, Some(RTCIceRole::Controlling)),
            b_ice.start(&a_parameters, Some(RTCIceRole::Controlled)),
        )
    })
    .await
    .unwrap();
    a_started.unwrap();
    b_started.unwrap();
    let a_dtls = api.new_dtls_transport(Arc::clone(&a_ice), vec![]).unwrap();
    let b_dtls = api.new_dtls_transport(Arc::clone(&b_ice), vec![]).unwrap();

    let shutdown_result = {
        // The other peer provides real ICE connectivity but intentionally does
        // not start DTLS. The constructor must therefore remain in flight.
        let start = a_dtls.start(b_dtls.get_local_parameters().unwrap());
        tokio::pin!(start);
        poll_fn(|cx| {
            assert!(start.as_mut().poll(cx).is_pending());
            assert_eq!(a_dtls.state(), RTCDtlsTransportState::Connecting);
            Poll::Ready(())
        })
        .await;
        for address in &addresses {
            assert!(UdpSocket::bind(address).is_err());
        }
        a_dtls.stop().await.unwrap();
        b_dtls.stop().await.unwrap();
        a_ice.stop().await.unwrap();
        b_ice.stop().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), &mut start).await
    };
    drop((a_dtls, b_dtls, a_ice, b_ice, a_gatherer, b_gatherer));
    tokio::time::timeout(Duration::from_secs(2), async {
        while addresses
            .iter()
            .any(|address| UdpSocket::bind(address).is_err())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled native constructor must release its socket owners");
    assert!(
        matches!(shutdown_result, Ok(Err(_))),
        "stop must finish a pending DTLS start with an error: {shutdown_result:?}"
    );
}
