// BS.1770 coefficients and histogram quantization follow libloudness 0.2.0.
// Copyright (c) 2023 Nomono AS; (c) 2025 Iver Jordal. MIT; see LICENSE-loudness.
use std::collections::VecDeque;

/// Mono 400 ms BS.1770 rolling loudness, normalized from [-110, -10] LUFS.
/// The source `loudness` package quantizes its single block to 0.1 LUFS bins.
pub struct AudioVolumeTracker {
    samples: VecDeque<i16>,
    sample_rate: u32,
    a: [f64; 5],
    b: [f64; 5],
}
impl AudioVolumeTracker {
    pub fn new(sample_rate: u32) -> crate::Result<Self> {
        crate::SileroModel::required_samples(sample_rate)?;
        let (a, b) = coefficients(f64::from(sample_rate));
        Ok(Self {
            samples: VecDeque::new(),
            sample_rate,
            a,
            b,
        })
    }
    pub fn reset(&mut self) {
        self.samples.clear();
    }
    pub fn buffered_samples(&self) -> usize {
        self.samples.len()
    }
    pub fn update(&mut self, samples: &[i16]) {
        let limit = (self.sample_rate as usize * 2) / 5;
        for &sample in samples {
            if self.samples.len() == limit {
                self.samples.pop_front();
            }
            self.samples.push_back(sample);
        }
    }
    pub fn volume(&self) -> f64 {
        if self.samples.len() != (self.sample_rate as usize * 2) / 5 || self.samples.is_empty() {
            return 0.0;
        }
        // Each source measurement creates a fresh meter for the entire window.
        let mut v = [0.0; 5];
        let mut sum = 0.0;
        for &sample in &self.samples {
            v.copy_within(0..4, 1);
            v[0] = f64::from(sample) / 32768.0
                - self.a[1] * v[1]
                - self.a[2] * v[2]
                - self.a[3] * v[3]
                - self.a[4] * v[4];
            let filtered = self.b[0] * v[0]
                + self.b[1] * v[1]
                + self.b[2] * v[2]
                + self.b[3] * v[3]
                + self.b[4] * v[4];
            sum += filtered * filtered;
        }
        let lufs = 10.0 * (sum / self.samples.len() as f64).log10() - 0.691;
        if lufs < -70.0 {
            return 0.0;
        }
        let bin = ((lufs + 70.0) / 0.1).floor().clamp(0.0, 999.0);
        let quantized = bin * 0.1 - 70.0 + 0.05;
        ((quantized + 110.0) / 100.0).clamp(0.0, 1.0)
    }
}
fn coefficients(sample_rate: f64) -> ([f64; 5], [f64; 5]) {
    let vh = 10.0_f64.powf(3.999843853973347 / 20.0);
    let vb = vh.powf(0.4996667741545416);
    let q = 0.7071752369554196;
    let k = (std::f64::consts::PI * 1681.974450955533 / sample_rate).tan();
    let a0 = 1.0 + k / q + k * k;
    let pb = [
        (vh + vb * k / q + k * k) / a0,
        2.0 * (k * k - vh) / a0,
        (vh - vb * k / q + k * k) / a0,
    ];
    let pa = [1.0, 2.0 * (k * k - 1.0) / a0, (1.0 - k / q + k * k) / a0];
    let k = (std::f64::consts::PI * 38.13547087602444 / sample_rate).tan();
    let q = 0.5003270373238773;
    let ra = [
        1.0,
        2.0 * (k * k - 1.0) / (1.0 + k / q + k * k),
        (1.0 - k / q + k * k) / (1.0 + k / q + k * k),
    ];
    let mut a = [0.0; 5];
    let mut b = [0.0; 5];
    for i in 0..3 {
        for j in 0..3 {
            a[i + j] += pa[i] * ra[j];
            b[i + j] += pb[i] * [1.0, -2.0, 1.0][j];
        }
    }
    (a, b)
}
