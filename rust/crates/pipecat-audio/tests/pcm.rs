use pipecat_audio::*;

#[test]
fn pcm_is_explicitly_little_endian_and_checks_boundaries() {
    let samples = [i16::MIN, -2, 0, 1, 0x1234, i16::MAX];
    let bytes = [0, 128, 254, 255, 0, 0, 1, 0, 52, 18, 255, 127];
    assert_eq!(encode_pcm16_le(&samples), bytes);
    assert_eq!(decode_pcm16_le(&bytes).unwrap(), samples);
    assert_eq!(decode_pcm16_le(&[]).unwrap(), []);
    assert!(matches!(
        decode_pcm16_le(&[1]),
        Err(AudioError::IncompleteFrame { .. })
    ));
}

#[test]
fn formats_validate_channels_rate_and_frame_lengths() {
    assert!(PcmFormat::new(0, 1).is_err());
    assert!(PcmFormat::new(8000, 0).is_err());
    assert!(PcmFormat::new(8000, u16::MAX).is_err());
    let stereo = PcmFormat::new(24000, 2).unwrap();
    assert_eq!(stereo.frame_count(&[0; 12]).unwrap(), 3);
    assert!(stereo.validate(&[0; 10]).is_err());
}

#[test]
fn mixing_pads_and_saturates() {
    let a = encode_pcm16_le(&[30000, -30000, 10, i16::MIN, 100]);
    let b = encode_pcm16_le(&[10000, -10000, -5, -1]);
    assert_eq!(
        decode_pcm16_le(&mix_audio(&a, &b).unwrap()).unwrap(),
        [32767, -32768, 5, -32768, 100]
    );
    assert_eq!(mix_audio(&a, &[]).unwrap(), a);
    assert_eq!(mix_audio(&[], &b).unwrap(), b);
    assert!(mix_audio(&a, &[1]).is_err());
}

#[test]
fn scaling_truncates_float32_and_clips() {
    let pcm = encode_pcm16_le(&[i16::MIN, -3, -1, 0, 1, 3, i16::MAX]);
    assert_eq!(
        decode_pcm16_le(&scale_audio(&pcm, 0.5).unwrap()).unwrap(),
        [-16384, -1, 0, 0, 0, 1, 16383]
    );
    assert_eq!(
        decode_pcm16_le(&scale_audio(&pcm, -2.0).unwrap()).unwrap(),
        [32767, 6, 2, 0, -2, -6, -32768]
    );
    assert_eq!(scale_audio(&pcm, 1.0).unwrap(), pcm);
    assert!(scale_audio(&pcm, f32::NAN).is_err());
    assert!(scale_audio(&pcm, f32::INFINITY).is_err());
}

#[test]
fn interleave_truncates_to_shorter_mono_channel() {
    let left = encode_pcm16_le(&[1, 2, 3]);
    let right = encode_pcm16_le(&[-1, -2]);
    assert_eq!(
        decode_pcm16_le(&interleave_stereo_audio(&left, &right).unwrap()).unwrap(),
        [1, -1, 2, -2]
    );
    assert!(interleave_stereo_audio(&[], &right).unwrap().is_empty());
}

#[test]
fn silence_preserves_the_inclusive_threshold_and_negative_full_scale() {
    for (samples, expected) in [
        (&[-20, 20][..], true),
        (&[-21, 0], false),
        (&[0, 21], false),
        (&[i16::MIN], false),
        (&[i16::MAX], false),
    ] {
        assert_eq!(is_silence(&encode_pcm16_le(samples)).unwrap(), expected);
    }
    assert_eq!(is_silence(&[]), Err(AudioError::EmptyAudio));
}

#[test]
fn wav_header_matches_python_wave_golden() {
    let pcm = encode_pcm16_le(&[1, -2, 3, -4, 5]);
    let actual = pcm_to_wav(&pcm, 24000, 2).unwrap();
    assert_eq!(actual, include_bytes!("fixtures/stereo.wav"));
    let parsed = read_pcm16_wav(&actual).unwrap();
    assert_eq!(parsed.format, PcmFormat::new(24000, 2).unwrap());
    assert_eq!(parsed.pcm, &pcm[..8]);
    let mono = pcm_to_wav(&[1, 0, 9], 16000, 1).unwrap();
    assert_eq!(read_pcm16_wav(&mono).unwrap().pcm, [1, 0]);
    let empty = pcm_to_wav(&[], 16000, 1).unwrap();
    assert_eq!(empty.len(), 44);
    assert!(read_pcm16_wav(&empty).unwrap().pcm.is_empty());
    assert!(pcm_to_wav(&[], u32::MAX, 2).is_err());
}

#[test]
fn wav_reader_handles_unknown_odd_chunks_and_rejects_corruption() {
    let original = pcm_to_wav(&[1, 0], 8000, 1).unwrap();
    let mut extra = original[..12].to_vec();
    extra.extend_from_slice(b"JUNK\x01\x00\x00\x00\x42\x00");
    extra.extend_from_slice(&original[12..]);
    let size = (extra.len() - 8) as u32;
    extra[4..8].copy_from_slice(&size.to_le_bytes());
    assert_eq!(read_pcm16_wav(&extra).unwrap().pcm, [1, 0]);
    for length in 0..original.len() {
        assert!(read_pcm16_wav(&original[..length]).is_err());
    }
    let mut bad = original.clone();
    bad[20] = 3; // IEEE float rather than PCM
    assert!(read_pcm16_wav(&bad).is_err());
    let mut bad = original.clone();
    bad[32] = 4; // invalid block alignment
    assert!(read_pcm16_wav(&bad).is_err());
}

#[test]
fn smoothing_matches_formula() {
    assert_eq!(exp_smoothing(10.0, 2.0, 0.25), 4.0);
    assert_eq!(exp_smoothing(10.0, 2.0, 0.0), 2.0);
    assert_eq!(exp_smoothing(10.0, 2.0, 1.0), 10.0);
}
