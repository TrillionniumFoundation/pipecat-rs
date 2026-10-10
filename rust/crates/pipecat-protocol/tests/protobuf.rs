use pipecat_protocol::protobuf::{self, frame::Kind, Frame};
#[test]
fn text_wire_golden_uses_original_proto_tags() {
    let frame = Frame {
        frame: Some(Kind::Text(protobuf::TextFrame {
            id: 1,
            name: "x".into(),
            text: "hi".into(),
        })),
    };
    let golden = [
        0x0a, 0x09, 0x08, 0x01, 0x12, 0x01, b'x', 0x1a, 0x02, b'h', b'i',
    ];
    assert_eq!(frame.encode_checked().unwrap(), golden);
    assert_eq!(Frame::parse(&golden).unwrap(), frame);
}
#[test]
fn interruption_wire_golden() {
    let frame = Frame {
        frame: Some(Kind::Interruption(protobuf::InterruptionFrame {
            id: 7,
            name: "".into(),
        })),
    };
    assert_eq!(frame.encode_checked().unwrap(), [0x2a, 0x02, 0x08, 0x07]);
}
#[test]
fn audio_preserves_pts_presence_including_zero() {
    for pts in [None, Some(0), Some(u64::MAX)] {
        let frame = Frame {
            frame: Some(Kind::Audio(protobuf::AudioRawFrame {
                id: 2,
                name: "audio".into(),
                audio: vec![0, 1, 2, 3],
                sample_rate: 16000,
                num_channels: 1,
                pts,
            })),
        };
        assert_eq!(
            Frame::parse(&frame.encode_checked().unwrap()).unwrap(),
            frame
        );
    }
}
#[test]
fn transcription_and_json_message_roundtrip() {
    let frames = [
        Frame {
            frame: Some(Kind::Transcription(protobuf::TranscriptionFrame {
                id: 1,
                name: "t".into(),
                text: "你好".into(),
                user_id: "u".into(),
                timestamp: "t".into(),
            })),
        },
        Frame {
            frame: Some(Kind::Message(protobuf::MessageFrame {
                data: r#"{"label":"rtvi-ai","type":"bot-started-speaking"}"#.into(),
            })),
        },
    ];
    for frame in frames {
        assert_eq!(
            Frame::parse(&frame.encode_checked().unwrap()).unwrap(),
            frame
        );
    }
}
#[test]
fn malformed_unknown_empty_and_oversize_protobuf_rejected() {
    for bytes in [vec![], vec![0x0a, 0xff], vec![0x7a, 0], vec![0xff]] {
        assert!(Frame::parse(&bytes).is_err());
    }
    assert!(Frame::parse_with_limit(&[0x2a, 0x02, 0x08, 0x07], 3).is_err());
    assert!(Frame {
        frame: Some(Kind::Message(protobuf::MessageFrame {
            data: "bad JSON".into()
        }))
    }
    .encode_checked()
    .is_err());
}
#[test]
fn invalid_audio_fields_rejected() {
    for (sample_rate, num_channels, audio) in [
        (0, 1, vec![]),
        (16000, 0, vec![]),
        (16000, 1, vec![1]),
        (16000, 2, vec![1, 2]),
    ] {
        assert!(Frame {
            frame: Some(Kind::Audio(protobuf::AudioRawFrame {
                id: 0,
                name: String::new(),
                audio,
                sample_rate,
                num_channels,
                pts: None
            }))
        }
        .encode_checked()
        .is_err());
    }
}
