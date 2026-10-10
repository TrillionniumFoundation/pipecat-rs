use pipecat_audio::*;

#[test]
fn g711_matches_audioop_for_every_pcm16_input() {
    let ulaw = include_bytes!("fixtures/ulaw-encode.bin");
    let alaw = include_bytes!("fixtures/alaw-encode.bin");
    for (index, sample) in (i16::MIN..=i16::MAX).enumerate() {
        assert_eq!(encode_ulaw(sample), ulaw[index], "mu-law sample {sample}");
        assert_eq!(encode_alaw(sample), alaw[index], "A-law sample {sample}");
    }
}

#[test]
fn g711_matches_audioop_for_all_codewords() {
    let codes: Vec<u8> = (u8::MIN..=u8::MAX).collect();
    assert_eq!(
        ulaw_to_pcm(&codes),
        include_bytes!("fixtures/ulaw-decode.pcm")
    );
    assert_eq!(
        alaw_to_pcm(&codes),
        include_bytes!("fixtures/alaw-decode.pcm")
    );
    let ulaw = decode_pcm16_le(include_bytes!("fixtures/ulaw-decode.pcm")).unwrap();
    let alaw = decode_pcm16_le(include_bytes!("fixtures/alaw-decode.pcm")).unwrap();
    for (index, &code) in codes.iter().enumerate() {
        assert_eq!(decode_ulaw(code), ulaw[index]);
        assert_eq!(decode_alaw(code), alaw[index]);
    }
}

#[test]
fn g711_buffer_encoders_match_scalar_and_validate_pcm() {
    let samples = [i16::MIN, -10000, -1, 0, 1, 10000, i16::MAX];
    let pcm = encode_pcm16_le(&samples);
    assert_eq!(pcm_to_ulaw(&pcm).unwrap(), samples.map(encode_ulaw));
    assert_eq!(pcm_to_alaw(&pcm).unwrap(), samples.map(encode_alaw));
    assert!(pcm_to_ulaw(&[0]).is_err());
    assert!(pcm_to_alaw(&[0]).is_err());
    assert!(pcm_to_ulaw(&[]).unwrap().is_empty());
    assert!(ulaw_to_pcm(&[]).is_empty());
    assert_eq!(encode_ulaw(0), 0xff);
    assert_eq!(decode_ulaw(0x7f), 0);
    assert_eq!(decode_ulaw(0xff), 0);
    assert_eq!(decode_alaw(0xd5), 8);
    assert_eq!(decode_alaw(0x55), -8);
}
