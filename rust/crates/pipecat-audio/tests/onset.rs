use pipecat_audio::*;

#[test]
fn matches_original_numpy_onset_reference_corpus() {
    let mut data = &include_bytes!("fixtures/onset-reference.bin")[..];
    let mut count = 0;
    while !data.is_empty() {
        let rate = u32::from_le_bytes(data[..4].try_into().unwrap());
        let channels = u16::from_le_bytes(data[4..6].try_into().unwrap());
        let length = u32::from_le_bytes(data[6..10].try_into().unwrap()) as usize;
        let expected = i64::from_le_bytes(data[10..18].try_into().unwrap());
        let input = &data[18..18 + length];
        assert_eq!(
            detect_speech_onset(input, rate, channels).unwrap(),
            if expected < 0 {
                None
            } else {
                Some(expected as usize)
            },
            "reference case {count}"
        );
        count += 1;
        data = &data[18 + length..];
    }
    assert_eq!(count, 10);
}

#[test]
fn empty_invalid_and_too_short_inputs_are_handled() {
    assert_eq!(detect_speech_onset(&[], 16000, 1).unwrap(), None);
    assert_eq!(detect_speech_onset(&[1], 16000, 1).unwrap(), None);
    assert_eq!(detect_speech_onset(&[], 0, 1).unwrap(), None);
    assert!(detect_speech_onset(&[], 16000, 0).is_err());
    for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
        let config = SpeechOnsetConfig {
            hop_ms: bad,
            ..Default::default()
        };
        assert!(detect_speech_onset_with_config(&[], 16000, 1, config).is_err());
    }
    let config = SpeechOnsetConfig {
        threshold_db: f64::NAN,
        ..Default::default()
    };
    assert!(detect_speech_onset_with_config(&[], 16000, 1, config).is_err());
}

#[test]
fn signed_downmix_cancels_opposite_channels() {
    let samples: Vec<i16> = (0..8000)
        .flat_map(|i| {
            if i % 2 == 0 {
                [16000, -16000]
            } else {
                [-16000, 16000]
            }
        })
        .collect();
    assert_eq!(
        detect_speech_onset(&encode_pcm16_le(&samples), 16000, 2).unwrap(),
        None
    );
}
