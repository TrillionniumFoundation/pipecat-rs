//! Independent local-wire review. Fake credentials only; fixtures do not use the production encoder.
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use pipecat_aws_speech::*;
use pipecat_core::{AudioRawFrame, FrameProcessor};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

fn credentials() -> AwsCredentials {
    AwsCredentials::new(
        "REVIEWACCESS",
        "REVIEWSECRET",
        Some("REVIEWTOKEN/+==".into()),
    )
    .unwrap()
}
fn crc(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for b in bytes {
        c ^= u32::from(*b);
        for _ in 0..8 {
            c = (c >> 1) ^ if c & 1 == 1 { 0xedb88320 } else { 0 };
        }
    }
    !c
}
fn raw_event(raw_headers: &[u8], payload: &[u8]) -> Vec<u8> {
    let mut m = Vec::new();
    m.extend_from_slice(&((16 + raw_headers.len() + payload.len()) as u32).to_be_bytes());
    m.extend_from_slice(&(raw_headers.len() as u32).to_be_bytes());
    m.extend_from_slice(&crc(&m).to_be_bytes());
    m.extend_from_slice(raw_headers);
    m.extend_from_slice(payload);
    m.extend_from_slice(&crc(&m).to_be_bytes());
    m
}
fn header(name: &str, value: &str) -> Vec<u8> {
    let mut h = vec![name.len() as u8];
    h.extend_from_slice(name.as_bytes());
    h.push(7);
    h.extend_from_slice(&(value.len() as u16).to_be_bytes());
    h.extend_from_slice(value.as_bytes());
    h
}
fn transcript(value: Value) -> Vec<u8> {
    let h = [
        header(":message-type", "event"),
        header(":event-type", "TranscriptEvent"),
        header(":content-type", "application/json"),
    ]
    .concat();
    raw_event(&h, &serde_json::to_vec(&value).unwrap())
}
fn config(listener: &TcpListener) -> TranscribeConfig {
    let mut c = TranscribeConfig::new(credentials(), "us-east-1");
    c.endpoint = format!(
        "ws://{}/stream-transcription-websocket",
        listener.local_addr().unwrap()
    );
    c.limits.connect_timeout = Duration::from_secs(2);
    c.limits.write_timeout = Duration::from_millis(100);
    c.limits.response_timeout = Duration::from_millis(100);
    c.limits.close_timeout = Duration::from_millis(100);
    c
}
async fn next(s: &mut TranscribeSession) -> Option<Result<TranscribeEvent, StreamingError>> {
    tokio::time::timeout(Duration::from_secs(2), s.next())
        .await
        .expect("client stalled")
}

#[test]
fn independent_presign_vectors_cover_china_ipv6_and_punctuation() {
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("review_sigv4_vectors.json")).unwrap();
    for v in cases {
        let creds = AwsCredentials::new(
            "REVIEWACCESS",
            "REVIEWSECRET",
            v["token"].as_str().map(str::to_owned),
        )
        .unwrap();
        let mut c = TranscribeConfig::new(creds, v["region"].as_str().unwrap());
        c.endpoint = v["endpoint"].as_str().unwrap().into();
        c.language = v["language"].as_str().unwrap().into();
        c.sample_rate = v["rate"].as_u64().unwrap() as u32;
        let signed = c.presigned_url_at(v["date"].as_str().unwrap()).unwrap();
        assert_eq!(signed.as_str(), v["url"].as_str().unwrap());
        for secret in ["REVIEWACCESS", "REVIEWSECRET", "TOKEN", "Signature"] {
            assert!(!format!("{signed:?} {c:?}").contains(secret));
        }
    }
}

#[test]
fn independent_crc_and_length_negative_controls() {
    let valid = transcript(json!({"Transcript":{"Results":[]}}));
    assert!(eventstream::decode(&valid, 4096).is_ok());
    for n in 0..16 {
        let mut m = valid.clone();
        m[0..4].copy_from_slice(&(n as u32).to_be_bytes());
        let pre = crc(&m[..8]);
        m[8..12].copy_from_slice(&pre.to_be_bytes());
        let l = m.len();
        let crc = crc(&m[..l - 4]);
        m[l - 4..].copy_from_slice(&crc.to_be_bytes());
        assert!(eventstream::decode(&m, 4096).is_err());
    }
    let duplicate = [header("x", "one"), header("x", "two")].concat();
    assert!(eventstream::decode(&raw_event(&duplicate, b""), 4096).is_err());
    for h in [
        vec![0, 7, 0, 0],
        vec![1, b'x', 42],
        vec![1, 0xff, 7, 0, 0],
        vec![1, b'x', 7, 0, 3, b'a'],
        vec![1, b'x', 7, 0, 1, 0xff],
    ] {
        assert!(eventstream::decode(&raw_event(&h, b""), 4096).is_err());
    }
    let mut bad = valid.clone();
    bad[8] ^= 1;
    let n = bad.len();
    let c = crc(&bad[..n - 4]);
    bad[n - 4..].copy_from_slice(&c.to_be_bytes());
    assert!(
        eventstream::decode(&bad, 4096).is_err(),
        "prelude CRC must be checked independently"
    );
    let mut bad = valid.clone();
    let n = bad.len();
    bad[n - 1] ^= 1;
    assert!(
        eventstream::decode(&bad, 4096).is_err(),
        "message CRC must be checked"
    );
    let mut all = Vec::new();
    for (typ, payload) in [
        (0, vec![]),
        (1, vec![]),
        (2, vec![0xff]),
        (3, vec![0xff; 2]),
        (4, vec![0xff; 4]),
        (5, vec![0xff; 8]),
        (6, vec![0, 2, 0, 255]),
        (7, vec![0, 2, b'o', b'k']),
        (8, vec![0; 8]),
        (9, vec![0; 16]),
    ] {
        all.extend_from_slice(&[1, b'A' + typ, typ]);
        all.extend_from_slice(&payload);
    }
    assert_eq!(
        eventstream::decode(&raw_event(&all, b"payload"), 4096)
            .unwrap()
            .headers
            .len(),
        10
    );
}

#[tokio::test]
async fn independent_peer_drains_multiple_results_and_resampled_audio_before_empty_end() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config(&l);
    c.sample_rate = 48000;
    c.limits.response_timeout = Duration::from_secs(1);
    let peer = tokio::spawn(async move {
        let mut ws = accept_async(l.accept().await.unwrap().0).await.unwrap();
        let mut total = 0;
        loop {
            let Message::Binary(m) = ws.next().await.unwrap().unwrap() else {
                panic!("expected binary AudioEvent")
            };
            assert_eq!(
                u32::from_be_bytes(m[..4].try_into().unwrap()) as usize,
                m.len()
            );
            assert_eq!(
                crc(&m[..8]),
                u32::from_be_bytes(m[8..12].try_into().unwrap())
            );
            assert_eq!(
                crc(&m[..m.len() - 4]),
                u32::from_be_bytes(m[m.len() - 4..].try_into().unwrap())
            );
            let h = u32::from_be_bytes(m[4..8].try_into().unwrap()) as usize;
            assert!(m[12..12 + h].windows(10).any(|w| w == b"AudioEvent"));
            let pcm = &m[12 + h..m.len() - 4];
            if pcm.is_empty() {
                break;
            }
            assert!(pcm.len() <= 3200);
            assert_eq!(pcm.len() % 2, 0);
            total += pcm.len();
        }
        assert_eq!(total, 3200);
        ws.send(Message::Binary(transcript(json!({"Transcript":{"Results":[{"IsPartial":true,"Alternatives":[{"Transcript":"par"}]},{"IsPartial":false,"ResultId":"last","Alternatives":[{"Transcript":"final"},{"Transcript":"unused"}]}]}})).into())).await.unwrap();
        ws.close(None).await.unwrap();
    });
    let mut s = TranscribeClient::new(c).unwrap().connect().await.unwrap();
    let sender = s.sender();
    sender
        .send_audio(AudioRawFrame {
            audio: vec![0; 9600],
            sample_rate: 48000,
            num_channels: 1,
            context_id: None,
        })
        .await
        .unwrap();
    sender.finish().await.unwrap();
    assert!(matches!(sender.try_finish(), Err(StreamingError::Closed)));
    let mut out = Vec::new();
    while let Some(e) = next(&mut s).await {
        let TranscribeEvent::Transcript(t) = e.unwrap();
        out.push(t);
    }
    assert_eq!(out.len(), 2);
    assert!(!out[0].is_final);
    assert!(out[1].is_final);
    assert_eq!(out[1].result["ResultId"], "last");
    assert_eq!(out[1].text, "final");
    peer.await.unwrap();
}

#[tokio::test]
async fn independent_peer_rejects_crc_bad_frames_and_redacts_exception_payload() {
    for mode in 0..3 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let c = config(&l);
        let peer = tokio::spawn(async move {
            let mut ws = accept_async(l.accept().await.unwrap().0).await.unwrap();
            let mut message = transcript(json!({"Transcript":{"Results":[]}}));
            if mode == 0 {
                let n = message.len();
                message[n - 1] ^= 1;
            } else if mode == 1 {
                message = raw_event(
                    &[
                        header(":message-type", "exception"),
                        header(":exception-type", "UnrecognizedClientException"),
                    ]
                    .concat(),
                    b"REVIEWSECRET REVIEWTOKEN",
                );
            } else {
                message = transcript(
                    json!({"Transcript":{"Results":[{"IsPartial":"false","Alternatives":[]}]}}),
                );
            }
            ws.send(Message::Binary(message.into())).await.unwrap();
            let _ = ws.next().await;
        });
        let mut s = TranscribeClient::new(c).unwrap().connect().await.unwrap();
        let error = next(&mut s).await.unwrap().unwrap_err();
        if mode == 1 {
            assert!(matches!(
                error,
                StreamingError::Provider { status: Some(403) }
            ));
        } else {
            assert!(matches!(error, StreamingError::Protocol(_)));
        }
        let rendered = format!("{error:?} {error}");
        assert!(!rendered.contains("REVIEW"));
        assert!(next(&mut s).await.is_none());
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn independent_finish_deadline_and_cancel_under_output_backpressure() {
    for cancel in [false, true] {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut c = config(&l);
        c.limits.queue_capacity = 1;
        let peer = tokio::spawn(async move {
            let mut ws = accept_async(l.accept().await.unwrap().0).await.unwrap();
            if cancel {
                let payload = transcript(
                    json!({"Transcript":{"Results":[{"IsPartial":true,"Alternatives":[{"Transcript":"one"}]},{"IsPartial":true,"Alternatives":[{"Transcript":"two"}]},{"IsPartial":true,"Alternatives":[{"Transcript":"three"}]}]}}),
                );
                ws.send(Message::Binary(payload.into())).await.unwrap();
            }
            loop {
                match ws.next().await {
                    Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                    _ => {}
                }
            }
        });
        let mut s = TranscribeClient::new(c).unwrap().connect().await.unwrap();
        if cancel {
            tokio::time::sleep(Duration::from_millis(20)).await;
            s.cancel();
            assert!(matches!(
                next(&mut s).await,
                Some(Err(StreamingError::Cancelled))
            ));
        } else {
            s.sender().finish().await.unwrap();
            assert!(matches!(
                next(&mut s).await,
                Some(Err(StreamingError::Timeout("Transcribe final response")))
            ));
        }
        assert!(next(&mut s).await.is_none());
        peer.await.unwrap();
    }
}

fn digest(b: &[u8]) -> String {
    format!("{:x}", Sha256::digest(b))
}
fn sign(key: &[u8], b: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(b);
    mac.finalize().into_bytes().to_vec()
}
async fn request(s: &mut TcpStream) -> (BTreeMap<String, String>, Vec<u8>) {
    let mut all = Vec::new();
    let mut buf = [0; 4096];
    let end = loop {
        let n = s.read(&mut buf).await.unwrap();
        assert!(n > 0);
        all.extend_from_slice(&buf[..n]);
        if let Some(end) = all.windows(4).position(|b| b == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let head = std::str::from_utf8(&all[..end]).unwrap();
    assert!(head.starts_with("POST /v1/speech HTTP/1.1\r\n"));
    let headers: BTreeMap<_, _> = head
        .lines()
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string()))
        .collect();
    let len = headers["content-length"].parse::<usize>().unwrap();
    while all.len() < end + len {
        let n = s.read(&mut buf).await.unwrap();
        assert!(n > 0);
        all.extend_from_slice(&buf[..n]);
    }
    (headers, all[end..end + len].to_vec())
}
#[tokio::test]
async fn independent_polly_wire_signature_covers_exact_body_session_token_and_host() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = PollyConfig::new(credentials(), "us-east-1");
    c.endpoint = format!("http://{}/v1/speech", l.local_addr().unwrap());
    c.sample_rate = 16000;
    let peer = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let (h, b) = request(&mut s).await;
        let names = "content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token";
        assert_eq!(h["x-amz-content-sha256"], digest(&b));
        assert_eq!(h["x-amz-security-token"], "REVIEWTOKEN/+==");
        let headers = names
            .split(';')
            .map(|n| format!("{n}:{}\n", h[n]))
            .collect::<String>();
        let canonical = format!("POST\n/v1/speech\n\n{headers}\n{names}\n{}", digest(&b));
        let date = &h["x-amz-date"];
        let scope = format!("{}/us-east-1/polly/aws4_request", &date[..8]);
        let mut key = b"AWS4REVIEWSECRET".to_vec();
        for part in [&date[..8], "us-east-1", "polly", "aws4_request"] {
            key = sign(&key, part.as_bytes());
        }
        let signature = sign(
            &key,
            format!(
                "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
                digest(canonical.as_bytes())
            )
            .as_bytes(),
        )
        .iter()
        .map(|v| format!("{v:02x}"))
        .collect::<String>();
        assert_eq!(h["authorization"],format!("AWS4-HMAC-SHA256 Credential=REVIEWACCESS/{scope}, SignedHeaders={names}, Signature={signature}"));
        s.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: audio/pcm\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\0\0").await.unwrap();
    });
    let mut s = PollyClient::new(c)
        .unwrap()
        .synthesize("独立 & <test>")
        .unwrap();
    assert_eq!(s.next().await.unwrap().unwrap().pcm, vec![0, 0]);
    assert!(s.next().await.is_none());
    peer.await.unwrap();
}

#[tokio::test]
async fn redirect_location_is_never_followed() {
    let trap = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = PollyConfig::new(credentials(), "us-east-1");
    c.endpoint = format!("http://{}/v1/speech", l.local_addr().unwrap());
    let response=format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{}/steal\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",trap.local_addr().unwrap());
    let peer = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        request(&mut s).await;
        s.write_all(response.as_bytes()).await.unwrap();
    });
    let mut s = PollyClient::new(c).unwrap().synthesize("hi").unwrap();
    assert!(matches!(s.next().await, Some(Err(SpeechError::Http(307)))));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), trap.accept())
            .await
            .is_err()
    );
    peer.await.unwrap();
}

#[test]
fn provider_identity_and_rejected_credentials_do_not_require_network() {
    let s = TranscribeSttProcessor::new(
        TranscribeClient::new(TranscribeConfig::new(credentials(), "us-east-1")).unwrap(),
    );
    assert_eq!(s.name(), "AWSTranscribeSTTService");
    for token in ["", "bad\n", "bad value"] {
        assert!(AwsCredentials::new("ACCESS", "SECRET", Some(token.into())).is_err());
    }
    for key in ["", "ACCESS/evil", "ACCESS\n"] {
        assert!(AwsCredentials::new(key, "SECRET", None).is_err());
    }
}

#[tokio::test]
async fn settings_rotation_preserves_old_final_and_respects_local_identity_and_delta() {
    use pipecat_core::*;
    use std::sync::Arc;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = config(&listener);
    config.limits.response_timeout = Duration::from_secs(1);
    let (old_audio, received) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let Message::Binary(audio) = first.next().await.unwrap().unwrap() else {
            panic!("audio was not binary")
        };
        assert_eq!(eventstream::decode(&audio, 4096).unwrap().payload.len(), 2);
        old_audio.send(()).unwrap();
        let Message::Binary(end) = first.next().await.unwrap().unwrap() else {
            panic!("End was not binary")
        };
        assert!(eventstream::decode(&end, 4096).unwrap().payload.is_empty());
        first.send(Message::Binary(transcript(json!({"Transcript":{"Results":[{"IsPartial":false,"Alternatives":[{"Transcript":"old final"}]}]}})).into())).await.unwrap();
        first.close(None).await.unwrap();
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let Message::Binary(end) = second.next().await.unwrap().unwrap() else {
            panic!("End was not binary")
        };
        assert!(eventstream::decode(&end, 4096).unwrap().payload.is_empty());
        second.close(None).await.unwrap();
    });
    let processor = Arc::new(TranscribeSttProcessor::new(
        TranscribeClient::new(config.clone()).unwrap(),
    ));
    let unrelated = Arc::new(TranscribeSttProcessor::new(
        TranscribeClient::new(config).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let input = worker.sender();
    let send = |data| input.send(Frame::new(data), Direction::Downstream);
    send(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 1.0,
    }))
    .await
    .unwrap();
    let unrelated_update = Frame::new(FrameData::SttUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            delta: Some(SttSettings {
                language: Setting::Value("ja-JP".into()),
                ..Default::default()
            }),
            service: Some(ProcessorReference::Local(LocalProcessorReference(
                unrelated,
            ))),
            ..Default::default()
        },
    )));
    let unrelated_id = unrelated_update.id;
    worker
        .sender()
        .send(unrelated_update, Direction::Downstream)
        .await
        .unwrap();
    let update = Frame::new(FrameData::SttUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            settings: [("language".into(), json!("de-DE"))].into(),
            delta: Some(SttSettings {
                language: Setting::Value("fr-FR".into()),
                ..Default::default()
            }),
            service: Some(ProcessorReference::Local(LocalProcessorReference(
                processor.clone(),
            ))),
            ..Default::default()
        },
    )));
    let handled_id = update.id;
    worker
        .sender()
        .send(update, Direction::Downstream)
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![0, 0], 16000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.1,
                timestamp: 1.1,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut final_seen = false;
    let mut unrelated_seen = false;
    while let Some(out) = tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
    {
        assert_ne!(
            out.frame.id, handled_id,
            "handled settings must be consumed"
        );
        unrelated_seen |= out.frame.id == unrelated_id;
        match out.frame.data {
            FrameData::Transcription(t) => {
                assert_eq!(t.text, "old final");
                assert_eq!(t.language.as_deref(), Some("en-US"));
                final_seen = true;
            }
            FrameData::End(_) => assert!(final_seen),
            FrameData::Error(e) => panic!("{}", e.error),
            _ => {}
        }
    }
    assert!(unrelated_seen && final_seen);
    assert_eq!(processor.config_snapshot().unwrap().language, "fr-FR");
    assert!(worker.join().await.unwrap().errors.is_empty());
    peer.await.unwrap();
}

#[tokio::test]
async fn unsynchronized_system_audio_precedes_normal_settings_without_loss() {
    use pipecat_core::*;
    use std::sync::Arc;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut c = config(&listener);
    c.limits.response_timeout = Duration::from_secs(1);
    let peer = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        for value in [0, 1] {
            let Message::Binary(bytes) = first.next().await.unwrap().unwrap() else {
                panic!("not binary")
            };
            assert_eq!(
                eventstream::decode(&bytes, 4096).unwrap().payload,
                vec![value; 640]
            );
        }
        let Message::Binary(bytes) = first.next().await.unwrap().unwrap() else {
            panic!("not binary")
        };
        assert!(eventstream::decode(&bytes, 4096)
            .unwrap()
            .payload
            .is_empty());
        first.send(Message::Binary(transcript(json!({"Transcript":{"Results":[{"IsPartial":false,"Alternatives":[{"Transcript":"both chunks"}]}]}})).into())).await.unwrap();
        first.close(None).await.unwrap();
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let Message::Binary(bytes) = second.next().await.unwrap().unwrap() else {
            panic!("not binary")
        };
        assert!(eventstream::decode(&bytes, 4096)
            .unwrap()
            .payload
            .is_empty());
        second.close(None).await.unwrap();
    });
    let processor = Arc::new(TranscribeSttProcessor::new(
        TranscribeClient::new(c).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let input = worker.sender();
    for data in [
        FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        }),
        FrameData::InputAudio(AudioRawFrame::new(vec![0; 640], 16000, 1).unwrap()),
        FrameData::SttUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [("language".into(), json!("fr"))].into(),
            service: Some(ProcessorReference::Name("AWSTranscribeSTTService".into())),
            ..Default::default()
        })),
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.02,
            timestamp: 1.0,
        }),
        FrameData::InputAudio(AudioRawFrame::new(vec![1; 640], 16000, 1).unwrap()),
    ] {
        input
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let mut final_seen = false;
    while let Some(out) = tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
    {
        match out.frame.data {
            FrameData::Transcription(t) => {
                assert_eq!(t.text, "both chunks");
                assert_eq!(t.language.as_deref(), Some("en-US"));
                final_seen = true;
            }
            FrameData::End(_) => assert!(final_seen),
            FrameData::Error(e) => panic!("{}", e.error),
            _ => {}
        }
    }
    assert!(final_seen);
    assert_eq!(processor.config_snapshot().unwrap().language, "fr");
    assert!(worker.join().await.unwrap().errors.is_empty());
    peer.await.unwrap();
}
