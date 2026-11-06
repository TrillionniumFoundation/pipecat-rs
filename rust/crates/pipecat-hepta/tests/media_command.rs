//! Protocol regression tests. No model, device, signer or conversation is created.
use pipecat_hepta::*;
use serde_json::json;

fn stream() -> StreamIdentity {
    StreamIdentity {
        principal_id: "issuer".into(),
        device_id: "device".into(),
        device_generation: 2,
        organ_id: "organ".into(),
        organ_generation: 3,
        logical_session_id: "thread:existing".into(),
    }
}

fn scope(connection_id: u64) -> HeptaOutputScope {
    let stream = stream();
    HeptaOutputScope {
        principal_id: stream.principal_id,
        device_id: stream.device_id,
        device_generation: stream.device_generation,
        organ_id: stream.organ_id,
        organ_generation: stream.organ_generation,
        logical_session_id: stream.logical_session_id,
        utterance_sequence: 7,
        connection_id,
        output_epoch: 9,
        turn_id: Some("turn:existing".into()),
        utterance_id: Some("observation:existing".into()),
    }
}

fn source() -> CapturedAudioRef {
    CapturedAudioRef {
        stream: stream(),
        capture_generation: 4,
        sequence: 7,
        asset_id: "capture-4-7.pcm16".into(),
        sha256: "a".repeat(64),
        byte_length: 320,
        sample_rate: 16_000,
        num_channels: 1,
        start_sample: 0,
        end_sample: 160,
    }
}

#[test]
fn connection_is_checked_before_the_lossy_output_identity_conversion() {
    let binding = MediaConnectionBinding::new(stream(), 18).unwrap();
    // The historic primitive erases the incarnation. The actual endpoint must
    // validate before calling that conversion or admitting either operation.
    assert_eq!(
        OutputIdentity::try_from(scope(17)).unwrap(),
        OutputIdentity::try_from(scope(18)).unwrap()
    );
    for connection_id in [0, 17, 19, u64::MAX] {
        for command in [
            MediaCommand::Turn {
                request_id: 1,
                scope: scope(connection_id),
            },
            MediaCommand::Speak {
                request_id: 2,
                scope: scope(connection_id),
                text: "第一段。第二段。".into(),
            },
        ] {
            assert_eq!(
                binding.validate(&command),
                Err(MediaCommandError::ForeignScope)
            );
        }
    }
    // A rejected first command cannot teach the binding a different connection.
    assert!(binding
        .validate(&MediaCommand::Turn {
            request_id: 1,
            scope: scope(18)
        })
        .is_ok());
}

#[test]
fn every_stream_dimension_is_bound_including_source_release() {
    let binding = MediaConnectionBinding::new(stream(), 18).unwrap();
    for dimension in 0..6 {
        let mut foreign = stream();
        match dimension {
            0 => foreign.principal_id.push('x'),
            1 => foreign.device_id.push('x'),
            2 => foreign.device_generation += 1,
            3 => foreign.organ_id.push('x'),
            4 => foreign.organ_generation += 1,
            5 => foreign.logical_session_id.push('x'),
            _ => unreachable!(),
        }
        let mut output = scope(18);
        output.principal_id = foreign.principal_id.clone();
        output.device_id = foreign.device_id.clone();
        output.device_generation = foreign.device_generation;
        output.organ_id = foreign.organ_id.clone();
        output.organ_generation = foreign.organ_generation;
        output.logical_session_id = foreign.logical_session_id.clone();
        assert_eq!(
            binding.validate(&MediaCommand::Turn {
                request_id: 1,
                scope: output
            }),
            Err(MediaCommandError::ForeignScope)
        );
        let mut audio = source();
        audio.stream = foreign;
        for command in [
            MediaCommand::VerifySource {
                request_id: 2,
                source: audio.clone(),
            },
            MediaCommand::ReleaseSource {
                request_id: 3,
                source: audio,
            },
        ] {
            assert_eq!(
                binding.validate(&command),
                Err(MediaCommandError::ForeignScope)
            );
        }
    }
}

#[test]
fn all_correlated_commands_reject_zero_before_effects() {
    let binding = MediaConnectionBinding::new(stream(), 18).unwrap();
    for command in [
        MediaCommand::Turn {
            request_id: 0,
            scope: scope(18),
        },
        MediaCommand::Speak {
            request_id: 0,
            scope: scope(18),
            text: "你好".into(),
        },
        MediaCommand::Retire {
            request_id: 0,
            output_epoch: 10,
        },
        MediaCommand::VerifySource {
            request_id: 0,
            source: source(),
        },
        MediaCommand::ReleaseSource {
            request_id: 0,
            source: source(),
        },
        MediaCommand::RevokeCapture { request_id: 0 },
    ] {
        assert_eq!(
            binding.validate(&command),
            Err(MediaCommandError::InvalidRequest)
        );
    }
    assert!(binding.validate(&MediaCommand::Interrupt).is_ok());
    assert!(binding.validate(&MediaCommand::Shutdown).is_ok());
}

#[test]
fn request_correlation_is_not_mistaken_for_owner_sequence_order() {
    let binding = MediaConnectionBinding::new(stream(), 18).unwrap();
    for request_id in [8, 2, 7, 1] {
        assert!(binding
            .validate(&MediaCommand::VerifySource {
                request_id,
                source: source()
            })
            .is_ok());
    }
    // Request validation never bypasses the existing session's sequence fence.
    let id = OutputIdentity::try_from(scope(18)).unwrap();
    let mut session = HmiSession::with_epoch(stream(), id.turn_id.clone(), 9, 2).unwrap();
    session.generated(id.clone()).unwrap();
    assert_eq!(session.generated(id), Err(HmiError::Replay));
}

#[test]
fn admitted_ids_allow_arbitrary_gaps_and_reordering_but_never_replay() {
    let binding = MediaConnectionBinding::new(stream(), 18).unwrap();
    let mut gate = MediaCommandGate::new(binding, 4).unwrap();
    for request_id in [1_000_000, 2, 999_999, 1] {
        gate.admit(&MediaCommand::VerifySource {
            request_id,
            source: source(),
        })
        .unwrap();
    }
    // Changing the operation or payload cannot reuse a consumed ID, even when
    // its first effect subsequently failed or its acknowledgement was lost.
    assert_eq!(
        gate.admit(&MediaCommand::ReleaseSource {
            request_id: 2,
            source: source()
        }),
        Err(MediaCommandError::Replay)
    );
    assert_eq!(
        gate.admit(&MediaCommand::Retire {
            request_id: 1_000_000,
            output_epoch: 10
        }),
        Err(MediaCommandError::Replay)
    );
    assert_eq!(
        gate.admit(&MediaCommand::RevokeCapture { request_id: 3 }),
        Err(MediaCommandError::Capacity)
    );
    assert!(gate.admit(&MediaCommand::Interrupt).is_ok());
    assert!(gate.admit(&MediaCommand::Shutdown).is_ok());
    assert_eq!(
        gate.admit(&MediaCommand::VerifySource {
            request_id: 1,
            source: source()
        }),
        Err(MediaCommandError::Replay)
    );
}

#[test]
fn foreign_scope_cannot_consume_a_valid_command_id_or_capacity() {
    let binding = MediaConnectionBinding::new(stream(), 18).unwrap();
    let mut gate = MediaCommandGate::new(binding, 1).unwrap();
    assert_eq!(
        gate.admit(&MediaCommand::Turn {
            request_id: 1,
            scope: scope(17)
        }),
        Err(MediaCommandError::ForeignScope)
    );
    gate.admit(&MediaCommand::Turn {
        request_id: 1,
        scope: scope(18),
    })
    .unwrap();
    assert_eq!(
        gate.admit(&MediaCommand::Turn {
            request_id: 2,
            scope: scope(18)
        }),
        Err(MediaCommandError::Capacity)
    );
}

#[test]
fn binding_and_output_require_complete_owner_identity() {
    assert!(matches!(
        MediaConnectionBinding::new(stream(), 0),
        Err(MediaCommandError::InvalidConnection)
    ));
    let mut invalid = stream();
    invalid.organ_generation = 0;
    assert!(matches!(
        MediaConnectionBinding::new(invalid, 18),
        Err(MediaCommandError::InvalidConnection)
    ));
    let binding = MediaConnectionBinding::new(stream(), u64::MAX).unwrap();
    let mut output = scope(u64::MAX);
    output.turn_id = None;
    assert_eq!(
        binding.validate(&MediaCommand::Turn {
            request_id: 1,
            scope: output
        }),
        Err(MediaCommandError::InvalidScope(HmiError::InvalidIdentity))
    );
}

#[test]
fn one_byte_reads_preserve_chinese_emoji_and_newlines_across_suspensions() {
    let expected = "中文第一段。\n第二段🙂，保留全部输出。";
    let mut wire = serde_json::to_vec(
        &json!({"type":"speak","request_id":2,"scope":scope(18),"text":expected}),
    )
    .unwrap();
    wire.push(b'\n');
    let mut decoder = MediaCommandDecoder::default();
    for (index, byte) in wire.iter().enumerate() {
        let (used, command) = decoder.push(&[*byte]).unwrap();
        assert_eq!(used, 1);
        if index + 1 == wire.len() {
            let MediaCommand::Speak {
                text,
                scope: output,
                ..
            } = command.unwrap()
            else {
                panic!("wrong command")
            };
            assert_eq!(text, expected);
            assert_eq!(output.connection_id, 18);
        } else {
            assert!(command.is_none());
            assert_eq!(decoder.buffered_bytes(), index + 1);
        }
        // No read future owns these bytes; the caller may select interruption
        // or suspend before the next byte without losing a partial UTF-8 scalar.
    }
    assert_eq!(decoder.buffered_bytes(), 0);
    decoder.finish().unwrap();
}

#[test]
fn zero_utterance_sequence_is_rejected_independently_of_request_id() {
    let binding = MediaConnectionBinding::new(stream(), 18).unwrap();
    let mut output = scope(18);
    output.utterance_sequence = 0;
    for command in [
        MediaCommand::Turn {
            request_id: 1,
            scope: output.clone(),
        },
        MediaCommand::Speak {
            request_id: 2,
            scope: output,
            text: "你好".into(),
        },
    ] {
        assert_eq!(
            binding.validate(&command),
            Err(MediaCommandError::InvalidScope(HmiError::InvalidIdentity))
        );
    }
}

#[test]
fn two_commands_in_one_read_keep_wire_order_without_swallowing_the_next_line() {
    let wire = b"{\"type\":\"interrupt\"}\n{\"type\":\"shutdown\"}\n";
    let mut decoder = MediaCommandDecoder::default();
    let (used, first) = decoder.push(wire).unwrap();
    assert!(matches!(first, Some(MediaCommand::Interrupt)));
    let (rest, second) = decoder.push(&wire[used..]).unwrap();
    assert!(matches!(second, Some(MediaCommand::Shutdown)));
    assert_eq!(used + rest, wire.len());
    decoder.finish().unwrap();
    assert!(matches!(
        decoder.push(wire),
        Err(MediaDecodeError::Terminal)
    ));
}

#[test]
fn exactly_one_bounded_line_is_admitted_and_overflow_never_resynchronizes() {
    let command = b"{\"type\":\"shutdown\"}";
    let mut exact = command.to_vec();
    exact.resize(MAX_MEDIA_COMMAND_BYTES - 1, b' ');
    exact.push(b'\n');
    let mut decoder = MediaCommandDecoder::default();
    let (used, result) = decoder.push(&exact).unwrap();
    assert_eq!(used, MAX_MEDIA_COMMAND_BYTES);
    assert!(matches!(result, Some(MediaCommand::Shutdown)));

    let mut decoder = MediaCommandDecoder::default();
    exact.pop();
    exact.push(b' ');
    assert!(decoder.push(&exact).unwrap().1.is_none());
    assert_eq!(decoder.buffered_bytes(), MAX_MEDIA_COMMAND_BYTES);
    assert!(matches!(
        decoder.push(b"\n"),
        Err(MediaDecodeError::LineTooLong)
    ));
    assert_eq!(decoder.buffered_bytes(), MAX_MEDIA_COMMAND_BYTES);
    assert!(matches!(
        decoder.push(b"{\"type\":\"shutdown\"}\n"),
        Err(MediaDecodeError::Terminal)
    ));
}

#[test]
fn malformed_unknown_or_incomplete_commands_are_terminal() {
    for wire in [
        b"{\"type\":\"shutdown\",\"unexpected\":true}\n".as_slice(),
        b"{\"type\":\"shutdown\",\"type\":\"interrupt\"}\n".as_slice(),
        b"{\"type\":\"retire\",\"request_id\":\"1\",\"output_epoch\":2}\n".as_slice(),
        b"{\"type\":\"unknown\"}\n".as_slice(),
        b"\xff\n".as_slice(),
    ] {
        let mut decoder = MediaCommandDecoder::default();
        assert!(matches!(
            decoder.push(wire),
            Err(MediaDecodeError::InvalidCommand)
        ));
        assert!(matches!(
            decoder.push(b"{\"type\":\"shutdown\"}\n"),
            Err(MediaDecodeError::Terminal)
        ));
    }
    let mut decoder = MediaCommandDecoder::default();
    decoder.push(b"{\"type\":\"shutdown\"}").unwrap();
    assert_eq!(decoder.finish(), Err(MediaDecodeError::IncompleteCommand));
    assert!(matches!(
        decoder.push(b"\n"),
        Err(MediaDecodeError::Terminal)
    ));
}
