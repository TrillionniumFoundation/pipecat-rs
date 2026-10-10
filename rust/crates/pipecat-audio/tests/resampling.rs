use std::f64::consts::TAU;

use pipecat_audio::*;

fn tone(rate: u32, frequency: f64, frames: usize) -> Vec<u8> {
    let samples: Vec<i16> = (0..frames)
        .map(|i| (12000.0 * (TAU * frequency * i as f64 / f64::from(rate)).sin()).round() as i16)
        .collect();
    encode_pcm16_le(&samples)
}

#[test]
fn chunks_match_batch_bit_for_bit_including_odd_splits_and_stereo() {
    let mono = tone(16000, 500.0, 511);
    let other = tone(16000, 1700.0, 511);
    for (input, channels) in [
        (&mono, 1),
        (&interleave_stereo_audio(&mono, &other).unwrap(), 2),
    ] {
        for (in_rate, out_rate) in [
            (8000, 16000),
            (16000, 8000),
            (44100, 48000),
            (48000, 22050),
            (16000, 16000),
        ] {
            let expected = resample_pcm16(input, in_rate, out_rate, channels).unwrap();
            for size in [1, 3, 127, 320, 1023, 4096] {
                let mut converter = SincResampler::new(in_rate, out_rate, channels).unwrap();
                let mut actual = Vec::new();
                for chunk in input.chunks(size) {
                    actual.extend(converter.process(chunk).unwrap());
                }
                actual.extend(converter.flush().unwrap());
                assert_eq!(
                    actual, expected,
                    "rate {in_rate}->{out_rate}, channels {channels}, chunk {size}"
                );
                assert!(converter.flush().unwrap().is_empty());
                let mut reused = converter.process(input).unwrap();
                reused.extend(converter.flush().unwrap());
                assert_eq!(reused, expected);
            }
        }
    }
}

#[test]
fn identity_and_constant_signals_are_exact() {
    let original = encode_pcm16_le(&[i16::MIN, -1, 0, 1, i16::MAX]);
    assert_eq!(
        resample_pcm16(&original, 44100, 44100, 1).unwrap(),
        original
    );
    for value in [i16::MIN, -10000, 0, 10000, i16::MAX] {
        for (from, to) in [(8000, 48000), (48000, 8000), (44100, 16000)] {
            let pcm = encode_pcm16_le(&vec![value; 127]);
            let output = decode_pcm16_le(&resample_pcm16(&pcm, from, to, 1).unwrap()).unwrap();
            let count = (127_u64 * u64::from(to) + u64::from(from) / 2) / u64::from(from);
            assert_eq!(output, vec![value; count as usize]);
        }
    }
}

#[test]
fn short_streams_have_defined_duration_and_no_division_by_zero() {
    for frames in 0..20 {
        for (from, to) in [(8000, 16000), (16000, 8000), (48000, 8000), (8000, 48000)] {
            let result =
                resample_pcm16(&encode_pcm16_le(&vec![1000; frames]), from, to, 1).unwrap();
            let expected = (frames as u64 * u64::from(to) + u64::from(from) / 2) / u64::from(from);
            assert_eq!(result.len() / 2, expected as usize);
        }
    }
}

#[test]
fn anti_alias_filter_rejects_out_of_band_tone() {
    // Analytic reference: 10kHz is outside the target 8kHz sampling band.
    // Ignore the defined sample-hold edge transient before measuring attenuation.
    let input = tone(48000, 10000.0, 4800);
    let output = decode_pcm16_le(&resample_pcm16(&input, 48000, 8000, 1).unwrap()).unwrap();
    let interior = &output[80..output.len() - 80];
    let rms = (interior.iter().map(|&s| f64::from(s).powi(2)).sum::<f64>() / interior.len() as f64)
        .sqrt();
    assert!(
        rms < 2.0,
        "out-of-band RMS {rms}, expected >72dB suppression relative to source RMS"
    );
}

#[test]
fn passband_tone_matches_continuous_time_reference() {
    for (input_rate, output_rate) in [(48000, 8000), (8000, 48000), (44100, 16000)] {
        let input = tone(input_rate, 1000.0, input_rate as usize / 5);
        let result =
            decode_pcm16_le(&resample_pcm16(&input, input_rate, output_rate, 1).unwrap()).unwrap();
        let margin = output_rate as usize / 50;
        let errors: Vec<f64> = (margin..result.len() - margin)
            .map(|i| {
                let expected = 12000.0 * (TAU * 1000.0 * i as f64 / f64::from(output_rate)).sin();
                f64::from(result[i]) - expected
            })
            .collect();
        let rms_error = (errors.iter().map(|e| e * e).sum::<f64>() / errors.len() as f64).sqrt();
        assert!(
            rms_error < 1.0,
            "{input_rate}->{output_rate}: RMS error {rms_error}"
        );
    }
}

#[test]
fn channels_do_not_leak_and_history_remains_bounded() {
    let left = tone(16000, 1000.0, 16000);
    let stereo = interleave_stereo_audio(&left, &vec![0; left.len()]).unwrap();
    let mut converter = SincResampler::new(16000, 8000, 2).unwrap();
    let mut output = Vec::new();
    for chunk in stereo.chunks(643) {
        output.extend(converter.process(chunk).unwrap());
        assert!(converter.buffered_frames() <= 2 * converter.latency_frames() + 1);
        assert!(converter.pending_bytes() < 4);
    }
    output.extend(converter.flush().unwrap());
    let samples = decode_pcm16_le(&output).unwrap();
    assert!(samples.as_chunks::<2>().0.iter().all(|pair| pair[1] == 0));
    assert!(samples.as_chunks::<2>().0.iter().any(|pair| pair[0] != 0));
}

#[test]
fn invalid_inputs_reset_and_flush_are_explicit() {
    assert!(SincResampler::new(0, 8000, 1).is_err());
    assert!(SincResampler::new(8000, 0, 1).is_err());
    assert!(SincResampler::new(8000, 16000, 0).is_err());
    assert!(SincResampler::new(1, 10000, 1).is_err());
    assert!(SincResampler::new(10000, 1, 1).is_err());
    assert!(resample_pcm16(&[1, 0], 8000, 16000, 2).is_err());
    let mut converter = SincResampler::new(16000, 8000, 1).unwrap();
    assert!(converter.flush().unwrap().is_empty());
    converter.process(&[1]).unwrap();
    assert!(converter.flush().is_err());
    assert_eq!(converter.pending_bytes(), 1);
    converter.process(&[0]).unwrap();
    assert_eq!(decode_pcm16_le(&converter.flush().unwrap()).unwrap(), [1]);
    converter.process(&tone(16000, 1000.0, 640)).unwrap();
    converter.reset();
    assert!(converter.flush().unwrap().is_empty());
}
