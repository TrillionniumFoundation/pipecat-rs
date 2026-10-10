use std::f64::consts::TAU;

use pipecat_audio::*;

fn component(samples: &[i16], rate: u32, frequency: u32) -> f64 {
    let (sine, cosine) = samples
        .iter()
        .enumerate()
        .fold((0.0, 0.0), |(s, c), (i, &value)| {
            let phase = TAU * f64::from(frequency) * i as f64 / f64::from(rate);
            (
                s + f64::from(value) * phase.sin(),
                c + f64::from(value) * phase.cos(),
            )
        });
    2.0 * sine.hypot(cosine) / samples.len() as f64
}

#[test]
fn all_keypad_symbols_round_trip() {
    assert_eq!(KeypadEntry::ALL.len(), 12);
    for key in KeypadEntry::ALL {
        assert_eq!(
            key.as_char().to_string().parse::<KeypadEntry>().unwrap(),
            key
        );
        assert_eq!(KeypadEntry::try_from(key.as_char()).unwrap(), key);
    }
    for invalid in ["", "12", "A", "x", " ", "💬"] {
        assert!(invalid.parse::<KeypadEntry>().is_err());
    }
}

#[test]
fn original_assets_keep_native_pcm_and_rate_specific_duration() {
    let original = read_pcm16_wav(include_bytes!("../assets/dtmf-1.wav")).unwrap();
    assert_eq!(
        load_dtmf_audio(KeypadEntry::One, 8000).unwrap(),
        original.pcm
    );
    for key in KeypadEntry::ALL {
        let native = load_dtmf_audio(key, 8000).unwrap();
        assert_eq!(native.len(), 8000);
        assert_eq!(load_dtmf_audio(key, 16000).unwrap().len(), 16000);
        assert_eq!(load_dtmf_audio(key, 8000).unwrap(), native);
    }
    assert!(load_dtmf_audio(KeypadEntry::One, 0).is_err());
}

#[test]
fn generated_dtmf_contains_both_standard_frequencies() {
    // One second yields integral cycles for all DTMF frequencies, so the
    // discrete Fourier amplitudes have a direct analytic reference.
    for key in KeypadEntry::ALL {
        let pcm = generate_dtmf(key, 8000, 1000, 0.5).unwrap();
        let samples = decode_pcm16_le(&pcm).unwrap();
        assert_eq!(samples.len(), 8000);
        assert_eq!(samples[0], 0);
        let (low, high) = key.frequencies();
        assert!((component(&samples, 8000, low) - 32767.0 * 0.25).abs() < 1.0);
        assert!((component(&samples, 8000, high) - 32767.0 * 0.25).abs() < 1.0);
        assert!(component(&samples, 8000, 2000) < 1.0);
        assert!(samples.iter().all(|&s| i32::from(s).abs() <= 16384));
    }
}

#[test]
fn generation_validates_nyquist_and_amplitude() {
    assert!(generate_dtmf(KeypadEntry::One, 0, 50, 0.5).is_err());
    assert!(generate_dtmf(KeypadEntry::Pound, 2000, 50, 0.5).is_err());
    for amplitude in [-1.0, 1.1, f64::NAN, f64::INFINITY] {
        assert!(generate_dtmf(KeypadEntry::One, 8000, 50, amplitude).is_err());
    }
    assert!(generate_dtmf(KeypadEntry::One, 8000, 0, 0.5)
        .unwrap()
        .is_empty());
    assert_eq!(
        generate_dtmf(KeypadEntry::One, 8000, 50, 0.0).unwrap(),
        vec![0; 800]
    );
}
