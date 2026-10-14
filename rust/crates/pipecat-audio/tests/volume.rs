use pipecat_audio::AudioVolumeTracker;
include!("fixtures/volume_reference.rs");
fn samples(kind: &str, rate: u32, amplitude: i16, count: usize) -> Vec<i16> {
    (0..count)
        .map(|i| match kind {
            "silence" => 0,
            "constant" => amplitude,
            "alternating" => {
                if i % 2 == 0 {
                    -amplitude
                } else {
                    amplitude
                }
            }
            "sine" => {
                ((2.0 * std::f64::consts::PI * i as f64 * 440.0 / f64::from(rate)).sin()
                    * f64::from(amplitude)) as i16
            }
            _ => panic!("unknown fixture"),
        })
        .collect()
}
#[test]
fn four_rates_match_real_python_loudness_and_preserve_partial_warmup() {
    assert_eq!(CASES.len(), 48);
    for &(kind, rate, amp, expected) in CASES {
        let pcm = samples(kind, rate, amp, rate as usize * 2 / 5);
        let mut meter = AudioVolumeTracker::new(rate).unwrap();
        meter.update(&pcm[..pcm.len() - 1]);
        assert_eq!(meter.volume(), 0.0);
        meter.update(&pcm[pcm.len() - 1..]);
        assert!(
            (meter.volume() - expected).abs() < 1e-10,
            "{kind} {rate} {amp}: {} != {expected}",
            meter.volume()
        );
        assert_eq!(meter.volume(), meter.volume());
        meter.reset();
        assert_eq!(meter.buffered_samples(), 0);
        assert_eq!(meter.volume(), 0.0);
    }
}
#[test]
fn rolling_windows_match_source_for_short_chunks_and_oversized_replacement() {
    assert_eq!(ROLLING.len(), 28);
    let mut current_rate = 0;
    let mut meter = None;
    for &(rate, kind, amp, count, expected, retained) in ROLLING {
        if current_rate != rate {
            meter = Some(AudioVolumeTracker::new(rate).unwrap());
            current_rate = rate;
        }
        let m = meter.as_mut().unwrap();
        let bytes: Vec<_> = samples(kind, rate, amp, count)
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect();
        m.update_pcm16_le(&bytes).unwrap();
        assert_eq!(m.buffered_samples(), retained);
        assert!(
            (m.volume() - expected).abs() < 1e-10,
            "{rate} {kind}: {} != {expected}",
            m.volume()
        );
    }
}
#[test]
fn invalid_rate_or_odd_pcm_does_not_mutate_valid_window() {
    for rate in [0, 1, 7999, 16001, 44100, 48001, u32::MAX] {
        assert!(AudioVolumeTracker::new(rate).is_err());
    }
    let mut m = AudioVolumeTracker::new(8000).unwrap();
    m.update(&vec![3000; 3200]);
    let before = m.volume();
    assert!(m.update_pcm16_le(&[0, 1, 2]).is_err());
    assert_eq!(m.buffered_samples(), 3200);
    assert_eq!(m.volume(), before);
    m.update_pcm16_le(&[]).unwrap();
    assert_eq!(m.volume(), before);
}
