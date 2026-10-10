use std::{borrow::Cow, time::Duration};

use futures_util::StreamExt;
use livekit::{
    prelude::LocalAudioTrack,
    webrtc::{
        audio_frame::AudioFrame,
        audio_source::{native::NativeAudioSource, AudioSourceOptions, RtcAudioSource},
        audio_stream::native::NativeAudioStream,
    },
};
use pipecat_audio::{encode_pcm16_le, PcmFormat};
use pipecat_core::{AudioRawFrame, FrameData};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::{protocol::*, *};

#[test]
fn validated_configuration_rejects_native_panic_inputs() {
    let mut config = LiveKitConfig::default();
    assert!(config.validate().is_ok());
    config.source_queue_ms = 11;
    assert!(config.validate().is_err());
    config.source_queue_ms = 0;
    assert!(config.validate().is_ok());
    config.queue_capacity = 0;
    assert!(config.validate().is_err());
    config.queue_capacity = 1;
    config.input_format = PcmFormat::new(48000, 3).unwrap();
    assert!(config.validate().is_err());
}

#[test]
fn interleaved_stereo_uses_samples_per_channel() {
    let samples = [-32768, 32767, -1234, 1234, 0, 2];
    let pcm = AudioRawFrame::new(encode_pcm16_le(&samples), 48000, 2).unwrap();
    let native = to_sdk_audio(&pcm, PcmFormat::new(48000, 2).unwrap(), 1024).unwrap();
    assert_eq!(native.samples_per_channel, 3);
    assert_eq!(native.data.as_ref(), samples);
    assert_eq!(from_sdk_audio(native, 1024).unwrap(), pcm);
}
#[test]
fn malformed_or_mismatched_audio_is_rejected() {
    let pcm = AudioRawFrame::new(vec![0; 16], 24000, 2).unwrap();
    assert!(to_sdk_audio(&pcm, PcmFormat::new(24000, 1).unwrap(), 1024).is_err());
    assert!(to_sdk_audio(&pcm, PcmFormat::new(24000, 2).unwrap(), 8).is_err());
    let bad = AudioFrame {
        data: Cow::Owned(vec![1; 5]),
        sample_rate: 48000,
        num_channels: 2,
        samples_per_channel: 3,
    };
    assert!(from_sdk_audio(bad, 1024).is_err());
}
#[test]
fn user_audio_retains_identity_track_and_context() {
    let mut audio = AudioRawFrame::new(vec![0; 16], 16000, 1).unwrap();
    audio.context_id = Some("segment-a".into());
    let frame = LiveKitEvent::Audio {
        identity: "alice".into(),
        track_sid: "TR_audio".into(),
        audio: audio.clone(),
    }
    .into_frame()
    .unwrap()
    .unwrap();
    assert_eq!(frame.transport_source.as_deref(), Some("alice"));
    assert_eq!(frame.metadata["livekit.track_sid"], "TR_audio");
    assert!(
        matches!(frame.data, FrameData::UserAudio(user) if user.user_id == "alice" && user.audio == audio)
    );
}
#[test]
fn server_json_data_has_no_invented_participant() {
    let frame = LiveKitEvent::Data {
        payload: br#"{"message":"hello"}"#.to_vec(),
        identity: None,
        topic: Some("chat".into()),
        reliable: true,
    }
    .into_frame()
    .unwrap()
    .unwrap();
    assert!(frame.transport_source.is_none());
    assert_eq!(frame.metadata["livekit.topic"], "chat");
    assert_eq!(frame.metadata["livekit.reliable"], true);
    assert!(
        matches!(frame.data, FrameData::InputTransportMessage(value) if value["message"] == "hello")
    );
}
#[test]
fn binary_data_is_explicitly_non_json() {
    let result = LiveKitEvent::Data {
        payload: vec![0xff, 0],
        identity: Some("alice".into()),
        topic: None,
        reliable: false,
    }
    .into_frame();
    assert!(matches!(result, Err(Error::Json(_))));
}
#[test]
fn participant_sid_is_not_used_as_identity() {
    let event = LiveKitEvent::ParticipantConnected(ParticipantInfo {
        identity: "alice".into(),
        sid: "PA_xyz".into(),
        name: "Alice".into(),
        metadata: "{}".into(),
    });
    let frame = event.into_frame().unwrap().unwrap();
    assert_eq!(frame.transport_source.as_deref(), Some("alice"));
    assert_eq!(frame.metadata["livekit.participant_sid"], "PA_xyz");
}
#[test]
fn dtmf_covers_rfc4733_keypad_and_rejects_unknown_codes() {
    for (code, digit) in "0123456789*#ABCD".chars().enumerate() {
        let entry = pipecat_core::KeypadEntry::try_from(digit).unwrap();
        assert_eq!(dtmf_code(entry), code as u32);
        assert_eq!(dtmf_digit(code as u32), Some(entry));
    }
    assert_eq!(dtmf_digit(16), None);
    assert_eq!(dtmf_digit(u32::MAX), None);
}
#[test]
fn participant_filter_is_exact() {
    let mut config = LiveKitConfig::default();
    assert!(config.accepts("alice"));
    config.participant_filter.push("alice".into());
    assert!(config.accepts("alice"));
    assert!(!config.accepts("PA_alice"));
    assert!(!config.accepts("Alice"));
}
#[tokio::test]
async fn cancelled_connection_does_not_attempt_network() {
    let cancel = CancellationToken::new();
    cancel.cancel();
    let result = LiveKitConnection::connect(
        "ws://127.0.0.1:1",
        "invalid-test-token",
        LiveKitConfig::default(),
        cancel,
    )
    .await;
    assert!(matches!(result, Err(Error::Closed)));
}
#[tokio::test]
async fn bad_url_and_empty_token_fail_before_native_connect() {
    for (url, token) in [
        ("https://example.invalid", "test"),
        ("ws://127.0.0.1:1", ""),
        ("ws://user:pass@127.0.0.1:1", "test"),
    ] {
        assert!(matches!(
            LiveKitConnection::connect(
                url,
                token,
                LiveKitConfig::default(),
                CancellationToken::new()
            )
            .await,
            Err(Error::InvalidConfig(_))
        ));
    }
}

/// Exercises the actual linked WebRTC source/track/sink, with no server or network.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_source_track_sink_roundtrip_stereo_and_close() {
    let source = NativeAudioSource::new(AudioSourceOptions::default(), 48000, 2, 0);
    let track =
        LocalAudioTrack::create_audio_track("native-test", RtcAudioSource::Native(source.clone()));
    let mut stream = NativeAudioStream::new(track.rtc_track(), 48000, 2);
    let samples: Vec<i16> = (0..480).flat_map(|i| [i as i16, -(i as i16)]).collect();
    let pcm = AudioRawFrame::new(encode_pcm16_le(&samples), 48000, 2).unwrap();
    let native = to_sdk_audio(&pcm, PcmFormat::new(48000, 2).unwrap(), 8192).unwrap();
    timeout(Duration::from_secs(2), source.capture_frame(&native))
        .await
        .unwrap()
        .unwrap();
    let received = timeout(Duration::from_secs(2), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(from_sdk_audio(received, 8192).unwrap(), pcm);
    stream.close();
    assert!(timeout(Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .is_none());
    source.clear_buffer();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_direct_source_rejects_wrong_duration() {
    let source = NativeAudioSource::new(AudioSourceOptions::default(), 16000, 1, 0);
    let pcm = AudioRawFrame::new(vec![0; 20], 16000, 1).unwrap();
    let native = to_sdk_audio(&pcm, PcmFormat::new(16000, 1).unwrap(), 8192).unwrap();
    assert!(source.capture_frame(&native).await.is_err());
}

#[tokio::test]
async fn actual_sdk_rejects_unauthorized_local_signaling() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let responder = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 8192];
        let _ = socket.read(&mut buffer).await.unwrap();
        socket
            .write_all(
                b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
    });
    let config = LiveKitConfig {
        connect_timeout: Duration::from_secs(20),
        output_format: None,
        ..Default::default()
    };
    let result = timeout(
        Duration::from_secs(25),
        LiveKitConnection::connect(
            &format!("ws://{addr}"),
            "invalid-test-token",
            config,
            CancellationToken::new(),
        ),
    )
    .await
    .unwrap();
    let error = result.err().expect("unauthorized signaling must fail");
    let mut responder = responder;
    let observed = timeout(Duration::from_secs(3), &mut responder).await;
    if observed.is_err() {
        responder.abort();
        let _ = responder.await;
    }
    assert!(
        observed.is_ok(),
        "SDK never reached the rejection fixture: {error}"
    );
}

#[test]
fn excessive_event_metadata_is_rejected() {
    let event = LiveKitEvent::ParticipantConnected(ParticipantInfo {
        identity: "alice".into(),
        sid: "PA_xyz".into(),
        name: "Alice".into(),
        metadata: "x".repeat(65537),
    });
    assert!(matches!(validate_event(&event), Err(Error::Limit(_))));
}

#[tokio::test]
async fn cancellation_during_sdk_signaling_closes_socket() {
    use tokio::io::AsyncReadExt;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let cancel = CancellationToken::new();
    let trigger = cancel.clone();
    let observer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut buffer = [0; 8192];
        assert!(socket.read(&mut buffer).await.unwrap() > 0);
        trigger.cancel();
        assert_eq!(
            timeout(Duration::from_secs(2), socket.read(&mut buffer))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let result = LiveKitConnection::connect(
        &format!("ws://{addr}"),
        "invalid-test-token",
        LiveKitConfig::default(),
        cancel,
    )
    .await;
    assert!(matches!(result, Err(Error::Closed)));
    observer.await.unwrap();
}

#[test]
fn json_budget_counts_utf8_and_stops_before_oversized_allocation() {
    let value = serde_json::json!({"text": "你好"});
    let bytes = bounded_json(&value, 17).unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
        value
    );
    assert!(matches!(bounded_json(&value, 16), Err(Error::Limit(_))));
    let large = serde_json::json!({"text": "x".repeat(65536)});
    assert!(matches!(bounded_json(&large, 16), Err(Error::Limit(_))));
}
