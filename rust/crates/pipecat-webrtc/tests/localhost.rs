//! These tests use real UDP/ICE/DTLS/SRTP/SCTP peers, never an external STUN server.
use pipecat_core::AudioRawFrame;
use pipecat_core::{Frame, FrameData, ReasonFrame};
use pipecat_webrtc::{
    AudioCodec, ConnectionMode, ConnectionState, Error, IceCandidate, SmallWebRtcConfig,
    SmallWebRtcConnection, SmallWebRtcPatchRequest, SmallWebRtcRequest, SmallWebRtcRequestHandler,
    TransportEvent,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

fn config(codec: Option<AudioCodec>) -> SmallWebRtcConfig {
    SmallWebRtcConfig {
        localhost_only: true,
        audio_codec: codec,
        ..Default::default()
    }
}
async fn pair(codec: Option<AudioCodec>) -> (SmallWebRtcConnection, SmallWebRtcConnection) {
    let a = SmallWebRtcConnection::new(config(codec)).await.unwrap();
    let b = SmallWebRtcConnection::new(config(codec)).await.unwrap();
    let offer = a.create_offer().await.unwrap();
    assert!(offer.sdp.contains("a=fingerprint:sha-256"));
    assert!(offer.sdp.contains("a=ice-ufrag:"));
    assert!(offer.sdp.contains("127.0.0.1"), "{}", offer.sdp);
    assert!(!offer.sdp.contains(" typ srflx"));
    assert!(!offer.sdp.contains(" typ relay"));
    if let Some(codec) = codec {
        assert!(offer.sdp.contains(&format!(
            "{}{}",
            codec.mime_type().split('/').nth(1).unwrap(),
            if codec == AudioCodec::Opus {
                "/48000/2"
            } else {
                "/8000"
            }
        )));
    }
    let answer = b.answer_offer(offer.sdp).await.unwrap();
    a.accept_answer(answer).await.unwrap();
    let deadline = Duration::from_secs(10);
    let (a_ready, b_ready) = tokio::join!(a.wait_connected(deadline), b.wait_connected(deadline));
    a_ready.unwrap();
    b_ready.unwrap();
    let (a_data, b_data) =
        tokio::join!(a.wait_data_channel(deadline), b.wait_data_channel(deadline));
    a_data.unwrap();
    b_data.unwrap();
    (a, b)
}
async fn app(peer: &SmallWebRtcConnection) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match peer.recv_event().await.expect("open event stream") {
                TransportEvent::AppMessage(value) => return value,
                TransportEvent::Error(error) => panic!("transport error: {error}"),
                _ => {}
            }
        }
    })
    .await
    .expect("application message")
}
async fn audio(peer: &SmallWebRtcConnection) -> pipecat_webrtc::IncomingAudio {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match peer.recv_event().await.expect("open event stream") {
                TransportEvent::Audio(audio) => return audio,
                TransportEvent::Error(error) => panic!("transport error: {error}"),
                _ => {}
            }
        }
    })
    .await
    .expect("RTP audio")
}
fn pcm(codec: AudioCodec) -> AudioRawFrame {
    let rate = codec.sample_rate();
    let samples = (0..rate / 50)
        .map(|i| {
            ((i as f32 * 2.0 * std::f32::consts::PI * 440.0 / rate as f32).sin() * 10_000.0) as i16
        })
        .collect::<Vec<_>>();
    AudioRawFrame::new(pipecat_audio::encode_pcm16_le(&samples), rate, 1).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_data_channel_preserves_rtvi_json_bidirectionally() {
    let a = SmallWebRtcConnection::new(config(None)).await.unwrap();
    let b = SmallWebRtcConnection::new(config(None)).await.unwrap();
    let ready = json!({"label":"rtvi-ai", "type":"client-ready", "id":"ready-1", "data":{"version":"1.0.0"}});
    // Queue on both sides before either side has a usable data channel.
    a.send_app_message(&ready).await.unwrap();
    b.send_app_message(&json!({"type":"bot-ready", "data":{"version":"1.0.0"}}))
        .await
        .unwrap();
    let offer = a.create_offer().await.unwrap();
    let answer = b.answer_offer(offer.sdp).await.unwrap();
    a.accept_answer(answer).await.unwrap();
    assert_eq!(app(&b).await, ready);
    assert_eq!(app(&a).await["type"], "bot-ready");
    for sequence in 0..8 {
        a.send_app_message(&json!({"sequence": sequence}))
            .await
            .unwrap();
    }
    for sequence in 0..8 {
        assert_eq!(app(&b).await, json!({"sequence": sequence}));
    }
    assert_eq!(b.stats().app_messages_received, 9);
    a.close().await.unwrap();
    b.close().await.unwrap();
    assert_eq!(a.state(), ConnectionState::Closed);
    assert!(matches!(
        a.send_app_message(&ready).await,
        Err(Error::Closed)
    ));
    assert!(a.recv_event().await.is_none());
    a.close().await.unwrap();
}

async fn verify_audio(codec: AudioCodec) {
    let (a, b) = pair(Some(codec)).await;
    let input = pcm(codec);
    for _ in 0..4 {
        a.send_audio(input.clone()).await.unwrap();
        b.send_audio(input.clone()).await.unwrap();
    }
    let first = audio(&b).await;
    let second = audio(&b).await;
    let reverse = audio(&a).await;
    assert_eq!(first.codec, codec);
    assert_eq!(first.frame.sample_rate, codec.sample_rate());
    assert_eq!(first.frame.num_channels, 1);
    assert_eq!(first.frame.num_frames(), input.num_frames());
    assert_eq!(
        second.sequence_number.wrapping_sub(first.sequence_number),
        1
    );
    assert_eq!(
        second.timestamp.wrapping_sub(first.timestamp),
        codec.sample_rate() / 50
    );
    assert_eq!(reverse.frame.num_frames(), input.num_frames());
    match codec {
        AudioCodec::Pcmu => assert_eq!(
            first.frame.audio,
            pipecat_audio::ulaw_to_pcm(&pipecat_audio::pcm_to_ulaw(&input.audio).unwrap())
        ),
        AudioCodec::Pcma => assert_eq!(
            first.frame.audio,
            pipecat_audio::alaw_to_pcm(&pipecat_audio::pcm_to_alaw(&input.audio).unwrap())
        ),
        AudioCodec::Opus => assert!(pipecat_audio::decode_pcm16_le(&second.frame.audio)
            .unwrap()
            .iter()
            .any(|sample| sample.abs() > 100)),
    }
    assert!(b.stats().audio_packets_received >= 2);
    a.close().await.unwrap();
    b.close().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pcmu_audio_uses_real_srtp_and_g711() {
    verify_audio(AudioCodec::Pcmu).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pcma_audio_uses_real_srtp_and_g711() {
    verify_audio(AudioCodec::Pcma).await;
}
#[cfg(feature = "opus")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opus_audio_is_encoded_decoded_and_clocked_at_48khz() {
    verify_audio(AudioCodec::Opus).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_preopen_queue_cancel_and_input_validation() {
    let peer = Arc::new(
        SmallWebRtcConnection::new(SmallWebRtcConfig {
            queue_capacity: 1,
            ..config(Some(AudioCodec::Pcmu))
        })
        .await
        .unwrap(),
    );
    assert!(matches!(
        peer.send_audio(AudioRawFrame::new(vec![0; 640], 16_000, 1).unwrap())
            .await,
        Err(Error::InvalidAudio(_))
    ));
    assert!(matches!(
        peer.send_app_message(&json!("x".repeat(16_384))).await,
        Err(Error::MessageTooLarge { .. })
    ));
    peer.send_app_message(&json!({"first":true})).await.unwrap();
    peer.send_app_message(&json!({"second":true}))
        .await
        .unwrap();
    let mut hit_full = false;
    for _ in 0..3 {
        if matches!(
            peer.try_send_app_message(&json!({"queued":true})),
            Err(Error::QueueFull)
        ) {
            hit_full = true;
            break;
        }
    }
    assert!(hit_full);
    let sending = {
        let peer = peer.clone();
        tokio::spawn(async move { peer.send_app_message(&json!({"blocked":true})).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!sending.is_finished());
    peer.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), sending)
            .await
            .unwrap()
            .unwrap(),
        Err(Error::Closed)
    ));
    peer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signaling_handler_aliases_reuse_limits_and_end_of_candidates() {
    let client = SmallWebRtcConnection::new(config(None)).await.unwrap();
    let offer = client.create_offer().await.unwrap();
    let request: SmallWebRtcRequest = serde_json::from_value(
        json!({"sdp": offer.sdp, "type":"offer", "requestData":{"session":"demo"}}),
    )
    .unwrap();
    assert_eq!(request.request_data.as_ref().unwrap()["session"], "demo");
    let handler = SmallWebRtcRequestHandler::new(config(None), ConnectionMode::Single, 1).unwrap();
    let (answer, peer) = handler.handle_offer(request.clone()).await.unwrap();
    assert_eq!(answer.sdp_type, "answer");
    assert!(matches!(
        handler.handle_offer(request).await,
        Err(Error::ConnectionLimit)
    ));
    client
        .accept_answer(pipecat_webrtc::RTCSessionDescription::answer(answer.sdp).unwrap())
        .await
        .unwrap();
    handler
        .handle_patch(SmallWebRtcPatchRequest {
            pc_id: answer.pc_id.clone(),
            candidates: vec![IceCandidate {
                candidate: String::new(),
                sdp_mid: Some("0".into()),
                sdp_mline_index: Some(0),
                username_fragment: None,
            }],
        })
        .await
        .unwrap();
    client
        .wait_data_channel(Duration::from_secs(10))
        .await
        .unwrap();
    client
        .send_app_message(&json!({"handler":"works"}))
        .await
        .unwrap();
    assert_eq!(app(&peer).await["handler"], "works");
    assert_eq!(handler.len().await, 1);
    handler.close().await.unwrap();
    assert!(handler.is_empty().await);
    client.close().await.unwrap();
}

#[test]
fn candidate_json_supports_browser_camel_case_and_source_snake_case() {
    let browser: IceCandidate = serde_json::from_value(
        json!({"candidate":"", "sdpMid":"0", "sdpMLineIndex":0, "usernameFragment":"demo"}),
    )
    .unwrap();
    assert_eq!(browser.sdp_mid.as_deref(), Some("0"));
    assert_eq!(browser.sdp_mline_index, Some(0));
    let source: IceCandidate =
        serde_json::from_value(json!({"candidate":"", "sdp_mid":"0", "sdp_mline_index":0}))
            .unwrap();
    assert_eq!(source.sdp_mid, browser.sdp_mid);
}

#[cfg(not(feature = "opus"))]
#[tokio::test]
async fn unavailable_opus_is_explicit_and_not_advertised() {
    assert!(matches!(
        SmallWebRtcConnection::new(config(Some(AudioCodec::Opus))).await,
        Err(Error::CodecUnavailable(AudioCodec::Opus))
    ));
    let peer = SmallWebRtcConnection::new(config(Some(AudioCodec::Pcmu)))
        .await
        .unwrap();
    assert!(!peer.create_offer().await.unwrap().sdp.contains("opus"));
    peer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trickle_candidates_negotiate_without_external_servers() {
    let a = SmallWebRtcConnection::new(config(None)).await.unwrap();
    let b = SmallWebRtcConnection::new(config(None)).await.unwrap();
    let offer = a.create_offer_trickle().await.unwrap();
    let answer = b.answer_offer_trickle(offer.sdp).await.unwrap();
    a.accept_answer(answer).await.unwrap();
    async fn relay(source: &SmallWebRtcConnection, destination: &SmallWebRtcConnection) {
        loop {
            if let TransportEvent::IceCandidate(candidate) = source.recv_event().await.unwrap() {
                let done = candidate.is_none();
                destination
                    .add_ice_candidate(candidate.unwrap_or_default())
                    .await
                    .unwrap();
                if done {
                    break;
                }
            }
        }
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(relay(&a, &b), relay(&b, &a));
    })
    .await
    .unwrap();
    a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
    a.send_app_message(&json!({"trickle":true})).await.unwrap();
    assert_eq!(app(&b).await, json!({"trickle":true}));
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interruption_discards_queued_audio_before_negotiation_and_flush_drains_new_audio() {
    let codec = AudioCodec::Pcmu;
    let a = SmallWebRtcConnection::new(config(Some(codec)))
        .await
        .unwrap();
    let b = SmallWebRtcConnection::new(config(Some(codec)))
        .await
        .unwrap();
    let obsolete = AudioRawFrame::new(vec![0; 320], 8000, 1).unwrap();
    a.send_audio(obsolete.clone()).await.unwrap();
    a.send_audio(obsolete).await.unwrap();
    a.interrupt_audio();
    let wanted = pcm(codec);
    a.send_audio(wanted.clone()).await.unwrap();
    let offer = a.create_offer().await.unwrap();
    let answer = b.answer_offer(offer.sdp).await.unwrap();
    a.accept_answer(answer).await.unwrap();
    a.flush(Duration::from_secs(5)).await.unwrap();
    let received = audio(&b).await;
    assert_eq!(
        received.frame.audio,
        pipecat_audio::ulaw_to_pcm(&pipecat_audio::pcm_to_ulaw(&wanted.audio).unwrap())
    );
    assert_eq!(a.stats().audio_packets_sent, 1);
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn audio_track_status_disables_and_resumes_incoming_pcm() {
    let (a, b) = pair(Some(AudioCodec::Pcmu)).await;
    async fn toggle(a: &SmallWebRtcConnection, b: &SmallWebRtcConnection, enabled: bool) {
        a.send_app_message(&json!({"type":"signalling", "message":{"type":"trackStatus", "receiver_index":0, "enabled":enabled}})).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(TransportEvent::TrackStatus {
                    receiver_index,
                    enabled: received,
                }) = b.recv_event().await
                {
                    assert_eq!(receiver_index, 0);
                    assert_eq!(received, enabled);
                    break;
                }
            }
        })
        .await
        .unwrap();
    }
    toggle(&a, &b, false).await;
    a.send_audio(pcm(AudioCodec::Pcmu)).await.unwrap();
    a.flush(Duration::from_secs(2)).await.unwrap();
    assert!(tokio::time::timeout(Duration::from_millis(40), async {
        loop {
            if let Some(TransportEvent::Audio(_)) = b.recv_event().await {
                break;
            }
        }
    })
    .await
    .is_err());
    toggle(&a, &b, true).await;
    a.send_audio(pcm(AudioCodec::Pcmu)).await.unwrap();
    assert_eq!(audio(&b).await.frame.num_frames(), 160);
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn invalid_configuration_sdp_and_bounded_flush_fail_explicitly() {
    assert!(matches!(
        SmallWebRtcConnection::new(SmallWebRtcConfig {
            queue_capacity: 0,
            ..config(None)
        })
        .await,
        Err(Error::InvalidConfig(_))
    ));
    let peer = SmallWebRtcConnection::new(config(None)).await.unwrap();
    assert!(peer.answer_offer("not SDP").await.is_err());
    assert!(matches!(
        peer.send_audio(pcm(AudioCodec::Pcmu)).await,
        Err(Error::AudioDisabled)
    ));
    peer.send_app_message(&json!({"pending":true}))
        .await
        .unwrap();
    assert!(matches!(
        peer.flush(Duration::from_millis(10)).await,
        Err(Error::Timeout(_))
    ));
    peer.cancel();
    assert!(matches!(
        peer.flush(Duration::from_millis(10)).await,
        Err(Error::Closed)
    ));
    peer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn registry_reuses_existing_peer_and_restart_closes_old_peer() {
    let handler = SmallWebRtcRequestHandler::new(config(None), ConnectionMode::Single, 1).unwrap();
    let a = SmallWebRtcConnection::new(config(None)).await.unwrap();
    let request = |sdp, pc_id, restart_pc| SmallWebRtcRequest {
        sdp,
        sdp_type: "offer".into(),
        pc_id,
        restart_pc,
        request_data: None,
    };
    let offer = a.create_offer().await.unwrap();
    let (answer, first) = handler
        .handle_offer(request(offer.sdp, None, false))
        .await
        .unwrap();
    a.accept_answer(pipecat_webrtc::RTCSessionDescription::answer(answer.sdp).unwrap())
        .await
        .unwrap();
    a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
    let offer = a.create_offer().await.unwrap();
    let (answer, reused) = handler
        .handle_offer(request(offer.sdp, Some(first.pc_id().into()), false))
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&first, &reused));
    a.accept_answer(pipecat_webrtc::RTCSessionDescription::answer(answer.sdp).unwrap())
        .await
        .unwrap();
    let fresh = SmallWebRtcConnection::new(config(None)).await.unwrap();
    let offer = fresh.create_offer().await.unwrap();
    let (answer, replacement) = handler
        .handle_offer(request(offer.sdp, Some(first.pc_id().into()), true))
        .await
        .unwrap();
    assert_ne!(replacement.pc_id(), first.pc_id());
    assert_eq!(first.state(), ConnectionState::Closed);
    assert!(handler.get(first.pc_id()).await.is_none());
    fresh
        .accept_answer(pipecat_webrtc::RTCSessionDescription::answer(answer.sdp).unwrap())
        .await
        .unwrap();
    fresh
        .wait_data_channel(Duration::from_secs(5))
        .await
        .unwrap();
    handler.close().await.unwrap();
    a.close().await.unwrap();
    fresh.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incompatible_audio_codecs_fail_negotiation_explicitly() {
    let a = SmallWebRtcConnection::new(config(Some(AudioCodec::Pcmu)))
        .await
        .unwrap();
    let b = SmallWebRtcConnection::new(config(Some(AudioCodec::Pcma)))
        .await
        .unwrap();
    let offer = a.create_offer().await.unwrap();
    assert!(b.answer_offer(offer.sdp).await.is_err());
    a.close().await.unwrap();
    b.close().await.unwrap();
}

#[tokio::test]
async fn connection_timeout_retains_fatal_reason_after_cancellation() {
    let peer = SmallWebRtcConnection::new(SmallWebRtcConfig {
        connect_timeout: Duration::from_millis(20),
        ..config(None)
    })
    .await
    .unwrap();
    peer.send_app_message(&json!({"pending":true}))
        .await
        .unwrap();
    let mut state = peer.subscribe_state();
    tokio::time::timeout(Duration::from_secs(1), async {
        while *state.borrow_and_update() != ConnectionState::Failed {
            state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(peer.is_cancelled());
    assert!(peer.last_error().unwrap().contains("timed out opening"));
    assert!(peer.recv_event().await.is_none());
    peer.close().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn detailed_end_drains_accepted_audio_before_closing_real_peer() {
    let (a, b) = pair(Some(AudioCodec::Pcmu)).await;
    for _ in 0..3 {
        a.send_audio(pcm(AudioCodec::Pcmu)).await.unwrap();
    }
    let end = Frame::new(FrameData::EndDetailed(ReasonFrame::default()));
    assert!(
        tokio::time::timeout(Duration::from_secs(5), a.send_frame(&end))
            .await
            .unwrap()
            .unwrap()
    );
    assert_eq!(a.stats().audio_packets_sent, 3);
    assert_eq!(a.state(), ConnectionState::Closed);
    assert!(a.is_cancelled());
    b.close().await.unwrap();
}

#[tokio::test]
async fn detailed_cancel_discards_preconnection_queue_without_waiting_for_drain() {
    let a = SmallWebRtcConnection::new(config(None)).await.unwrap();
    a.send_app_message(&json!({"pending": true})).await.unwrap();
    let cancel = Frame::new(FrameData::CancelDetailed(ReasonFrame::default()));
    assert!(
        tokio::time::timeout(Duration::from_secs(1), a.send_frame(&cancel))
            .await
            .unwrap()
            .unwrap()
    );
    assert_eq!(a.state(), ConnectionState::Closed);
    assert!(matches!(
        a.send_app_message(&json!({})).await,
        Err(Error::Closed)
    ));
}
