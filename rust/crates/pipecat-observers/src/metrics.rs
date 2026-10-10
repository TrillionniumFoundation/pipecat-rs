//! Explicit service measurements on a monotonic clock.
//!
//! A service starts a request and reports actual output at the point the
//! provider produces it. Connection acknowledgements and keepalives are not
//! output. No metric is inferred from a frame's mere presence in the pipeline.
//! Timings are seconds; explicit timestamps are nanoseconds in the injected
//! clock's domain. One collector represents one non-overlapping service stream.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use pipecat_core::ObserverClock;
use serde::{Deserialize, Serialize};

const NANOS_PER_SECOND: f64 = 1_000_000_000.0;
const MAX_SAMPLE_RATE: u32 = 384_000;
const MAX_CHANNELS: u16 = 64;
const ONSET_SCAN_SECONDS: u64 = 3;

/// Identifies the service and model that produced a measurement.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricIdentity {
    pub processor: String,
    pub model: Option<String>,
}

/// Whether the provider's prompt count includes its cache buckets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PromptTokenAccounting {
    /// The provider has not specified its prompt accounting convention.
    #[default]
    Unspecified,
    /// Cache reads and writes are already included in prompt_tokens.
    IncludesCache,
    /// Cache reads and writes are reported alongside prompt_tokens.
    ExcludesCache,
}

/// Raw provider usage, never a price estimate.
///
/// `total_tokens` is the gross provider total, including cached input. Preserve
/// it rather than assuming prompt + completion: Anthropic-style net prompt
/// counts exclude cache buckets, while OpenAI-style gross counts include them.
/// Reasoning is part of completion, and audio fields are sub-buckets, not extra
/// tokens to add to the total. None means unreported, distinct from known zero.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmTokenUsage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
    pub input_audio_tokens: Option<u64>,
    pub output_audio_tokens: Option<u64>,
    pub cache_read_input_audio_tokens: Option<u64>,
    #[serde(default)]
    pub prompt_accounting: PromptTokenAccounting,
}

impl LlmTokenUsage {
    fn is_zero(&self) -> bool {
        self.prompt_tokens == 0
            && self.completion_tokens == 0
            && self.total_tokens == 0
            && self.cache_read_input_tokens.unwrap_or(0) == 0
            && self.cache_creation_input_tokens.unwrap_or(0) == 0
            && self.reasoning_tokens.unwrap_or(0) == 0
            && self.input_audio_tokens.unwrap_or(0) == 0
            && self.output_audio_tokens.unwrap_or(0) == 0
            && self.cache_read_input_audio_tokens.unwrap_or(0) == 0
    }

    fn delta_from(&self, previous: &Self) -> Result<Self, MetricsError> {
        fn delta(value: u64, previous: u64) -> Result<u64, MetricsError> {
            value
                .checked_sub(previous)
                .ok_or(MetricsError::UsageRegression)
        }
        fn optional(
            value: Option<u64>,
            previous: Option<u64>,
        ) -> Result<Option<u64>, MetricsError> {
            value.map(|v| delta(v, previous.unwrap_or(0))).transpose()
        }
        if self.prompt_accounting != previous.prompt_accounting {
            return Err(MetricsError::UsageAccountingChanged);
        }
        Ok(Self {
            prompt_tokens: delta(self.prompt_tokens, previous.prompt_tokens)?,
            completion_tokens: delta(self.completion_tokens, previous.completion_tokens)?,
            total_tokens: delta(self.total_tokens, previous.total_tokens)?,
            cache_read_input_tokens: optional(
                self.cache_read_input_tokens,
                previous.cache_read_input_tokens,
            )?,
            cache_creation_input_tokens: optional(
                self.cache_creation_input_tokens,
                previous.cache_creation_input_tokens,
            )?,
            reasoning_tokens: optional(self.reasoning_tokens, previous.reasoning_tokens)?,
            input_audio_tokens: optional(self.input_audio_tokens, previous.input_audio_tokens)?,
            output_audio_tokens: optional(self.output_audio_tokens, previous.output_audio_tokens)?,
            cache_read_input_audio_tokens: optional(
                self.cache_read_input_audio_tokens,
                previous.cache_read_input_audio_tokens,
            )?,
            prompt_accounting: self.prompt_accounting,
        })
    }

    fn retain_unreported_buckets(&mut self, previous: &Self) {
        self.cache_read_input_tokens = self
            .cache_read_input_tokens
            .or(previous.cache_read_input_tokens);
        self.cache_creation_input_tokens = self
            .cache_creation_input_tokens
            .or(previous.cache_creation_input_tokens);
        self.reasoning_tokens = self.reasoning_tokens.or(previous.reasoning_tokens);
        self.input_audio_tokens = self.input_audio_tokens.or(previous.input_audio_tokens);
        self.output_audio_tokens = self.output_audio_tokens.or(previous.output_audio_tokens);
        self.cache_read_input_audio_tokens = self
            .cache_read_input_audio_tokens
            .or(previous.cache_read_input_audio_tokens);
    }
}

/// Service measurements, separate from the core frame metric wire protocol.
///
/// TTFA and TTFAT repeat TTFB for a self-contained breakdown. Their TTFB field
/// must not be aggregated as another independent TTFB observation. Usage values
/// are deltas since the preceding report; consumers may sum them.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MeasureMetric {
    Ttfb {
        identity: MetricIdentity,
        value: f64,
    },
    Ttfa {
        identity: MetricIdentity,
        ttfa: f64,
        ttfb: f64,
        leading_silence: f64,
    },
    Ttfat {
        identity: MetricIdentity,
        ttfat: f64,
        ttfb: f64,
        thinking_time: f64,
    },
    Processing {
        identity: MetricIdentity,
        value: f64,
    },
    TextAggregation {
        identity: MetricIdentity,
        value: f64,
    },
    LlmUsage {
        identity: MetricIdentity,
        value: LlmTokenUsage,
    },
    SttUsage {
        identity: MetricIdentity,
        audio_seconds: f64,
    },
    TtsUsage {
        identity: MetricIdentity,
        characters: u64,
    },
}

/// What the provider actually produced, before downstream buffering/filtering.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServiceOutput {
    /// HTTP headers, stream-open, usage-only, and keepalive events.
    Acknowledgement,
    Reasoning,
    /// A nonempty answer token. Empty deltas are acknowledgements.
    AnswerToken,
    /// The start of a tool call; do not wait for its argument stream to finish.
    ToolCall,
    Audio,
    Other,
}

/// Per-collector TTFB reporting policy, also governing TTFA and TTFAT.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TtfbPolicy {
    #[default]
    EveryRequest,
    /// Measure only the first started request, even if that request is canceled.
    InitialOnly,
}

/// Invalid input is rejected without creating a latency or usage observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricsError {
    InvalidAudioFormat,
    AudioFormatChanged,
    IncompleteAudioFrame,
    UsageRegression,
    UsageAccountingChanged,
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidAudioFormat => "PCM requires 1..=384000 Hz and 1..=64 channels",
            Self::AudioFormatChanged => "PCM format changed within the TTFA measurement",
            Self::IncompleteAudioFrame => {
                "PCM usage must contain complete interleaved 16-bit frames"
            }
            Self::UsageRegression => "cumulative usage decreased within a request",
            Self::UsageAccountingChanged => "prompt accounting changed within a request",
        })
    }
}

impl std::error::Error for MetricsError {}

/// Explicit timers and incremental usage accounting for one service.
///
/// Start a request before feeding output. Use `observe_output` for LLM output
/// or `observe_audio` for PCM TTS output. Call `cancel_request` on interruption,
/// timeout, or failure, and `finish_request` on normal completion. Neither
/// creates a fabricated final latency when the expected output never arrived.
/// Processing and text aggregation have independent explicit start/stop pairs.
/// Usage reporting is enabled by default for this explicitly driven collector.
/// A pipeline may disable it with `set_usage_enabled`.
pub struct ServiceMetrics {
    identity: MetricIdentity,
    clock: Arc<dyn ObserverClock>,
    policy: TtfbPolicy,
    initial_started: bool,
    ttfb_start: Option<u64>,
    last_ttfb_ns: Option<u64>,
    answer_start: Option<(u64, u64)>,
    audio: Option<AudioMeasurement>,
    processing_start: Option<u64>,
    aggregation_start: Option<u64>,
    usage_enabled: bool,
    stt_pending_seconds: f64,
    llm_previous: Option<LlmTokenUsage>,
}

struct AudioMeasurement {
    ttfb_ns: u64,
    detector: Option<SpeechOnset>,
}

impl ServiceMetrics {
    pub fn new(
        processor: impl Into<String>,
        model: Option<String>,
        clock: Arc<dyn ObserverClock>,
    ) -> Self {
        Self::with_policy(processor, model, clock, TtfbPolicy::EveryRequest)
    }

    pub fn with_policy(
        processor: impl Into<String>,
        model: Option<String>,
        clock: Arc<dyn ObserverClock>,
        policy: TtfbPolicy,
    ) -> Self {
        Self {
            identity: MetricIdentity {
                processor: processor.into(),
                model,
            },
            clock,
            policy,
            initial_started: false,
            ttfb_start: None,
            last_ttfb_ns: None,
            answer_start: None,
            audio: None,
            processing_start: None,
            aggregation_start: None,
            usage_enabled: true,
            stt_pending_seconds: 0.0,
            llm_previous: None,
        }
    }

    pub fn identity(&self) -> &MetricIdentity {
        &self.identity
    }

    pub fn begin_request(&mut self) {
        self.begin_request_at(self.clock.now_ns());
    }

    /// Clears abandoned timing state even when initial-only suppresses this request.
    /// Pending STT usage survives request boundaries; cumulative LLM usage resets.
    pub fn begin_request_at(&mut self, start_ns: u64) {
        self.clear_timers();
        self.llm_previous = None;
        if self.policy == TtfbPolicy::EveryRequest || !self.initial_started {
            self.ttfb_start = Some(start_ns);
        }
        self.initial_started = true;
    }

    /// Abandons all open timers, but preserves submitted/billable usage.
    pub fn cancel_request(&mut self) {
        self.clear_timers();
    }

    /// Drops unfinished timings and flushes trailing incremental STT usage.
    pub fn finish_request(&mut self) -> Option<MeasureMetric> {
        self.clear_timers();
        self.flush_stt_usage()
    }

    /// Begins a new reporting session, including the initial-only policy.
    /// Flush usage first if the previous session's pending usage is needed.
    pub fn reset_session(&mut self) {
        self.clear_timers();
        self.initial_started = false;
        self.stt_pending_seconds = 0.0;
        self.llm_previous = None;
    }

    fn clear_timers(&mut self) {
        self.ttfb_start = None;
        self.last_ttfb_ns = None;
        self.answer_start = None;
        self.audio = None;
        self.processing_start = None;
        self.aggregation_start = None;
    }

    /// Last TTFB, or the current elapsed interval while waiting for first output.
    /// A zero-duration measurement is valid, as is a request starting at zero.
    pub fn ttfb(&self) -> Option<f64> {
        self.last_ttfb_ns
            .or_else(|| {
                self.ttfb_start
                    .and_then(|start| self.clock.now_ns().checked_sub(start))
            })
            .map(seconds)
    }

    pub fn observe_output(&mut self, output: ServiceOutput) -> Vec<MeasureMetric> {
        self.observe_output_at(output, self.clock.now_ns())
    }

    pub fn observe_output_at(&mut self, output: ServiceOutput, now_ns: u64) -> Vec<MeasureMetric> {
        if output == ServiceOutput::Acknowledgement {
            return Vec::new();
        }
        let mut metrics = Vec::with_capacity(2);
        if let Some(metric) = self.stop_ttfb_at(now_ns) {
            metrics.push(metric);
        }
        if matches!(output, ServiceOutput::AnswerToken | ServiceOutput::ToolCall) {
            if let Some(metric) = self.stop_ttfat_at(now_ns) {
                metrics.push(metric);
            }
        }
        metrics
    }

    /// Call only for actual provider output, never a connection acknowledgement.
    pub fn stop_ttfb(&mut self) -> Option<MeasureMetric> {
        self.stop_ttfb_at(self.clock.now_ns())
    }

    pub fn stop_ttfb_at(&mut self, end_ns: u64) -> Option<MeasureMetric> {
        let start = self.ttfb_start.take()?;
        // A stale timestamp closes the timer; it must not attach to later output.
        let elapsed = end_ns.checked_sub(start)?;
        self.last_ttfb_ns = Some(elapsed);
        self.answer_start = Some((start, end_ns));
        self.audio = Some(AudioMeasurement {
            ttfb_ns: elapsed,
            detector: None,
        });
        Some(MeasureMetric::Ttfb {
            identity: self.identity.clone(),
            value: seconds(elapsed),
        })
    }

    pub fn stop_ttfat(&mut self) -> Option<MeasureMetric> {
        self.stop_ttfat_at(self.clock.now_ns())
    }

    pub fn stop_ttfat_at(&mut self, end_ns: u64) -> Option<MeasureMetric> {
        let (start, first_output) = self.answer_start.take()?;
        let thinking = end_ns.checked_sub(first_output)?;
        Some(MeasureMetric::Ttfat {
            identity: self.identity.clone(),
            ttfat: seconds(end_ns - start),
            ttfb: seconds(first_output - start),
            thinking_time: seconds(thinking),
        })
    }

    /// Records first audio output and incrementally scans little-endian PCM16.
    /// Empty chunks do not end TTFB. Incomplete samples/interleaved frames are
    /// carried across chunks. Invalid or changed formats cancel the TTFA scan.
    pub fn observe_audio(
        &mut self,
        audio: &[u8],
        sample_rate: u32,
        channels: u16,
    ) -> Result<Vec<MeasureMetric>, MetricsError> {
        self.observe_audio_at(audio, sample_rate, channels, self.clock.now_ns())
    }

    pub fn observe_audio_at(
        &mut self,
        audio: &[u8],
        sample_rate: u32,
        channels: u16,
        now_ns: u64,
    ) -> Result<Vec<MeasureMetric>, MetricsError> {
        if let Err(error) = validate_format(sample_rate, channels) {
            self.audio = None;
            return Err(error);
        }
        if audio.is_empty() {
            return Ok(Vec::new());
        }
        let mut metrics = self.observe_output_at(ServiceOutput::Audio, now_ns);
        if let Some(metric) = self.process_ttfa(audio, sample_rate, channels)? {
            metrics.push(metric);
        }
        Ok(metrics)
    }

    /// Scans audio after TTFB has been explicitly stopped. Confirmation uses
    /// actual future samples rather than right-padding each chunk, so results
    /// are independent of chunk boundaries. The scan ends after three seconds
    /// without onset, retaining at most one 10ms analysis window at any time.
    pub fn process_ttfa(
        &mut self,
        audio: &[u8],
        sample_rate: u32,
        channels: u16,
    ) -> Result<Option<MeasureMetric>, MetricsError> {
        let Some(mut measurement) = self.audio.take() else {
            return Ok(None);
        };
        validate_format(sample_rate, channels)?;
        if let Some(detector) = &measurement.detector {
            if detector.sample_rate != sample_rate || detector.channels != channels {
                return Err(MetricsError::AudioFormatChanged);
            }
        }
        let detector = measurement
            .detector
            .get_or_insert_with(|| SpeechOnset::new(sample_rate, channels));
        if let Some(sample) = detector.feed(audio) {
            let leading_silence = sample as f64 / sample_rate as f64;
            let ttfb = seconds(measurement.ttfb_ns);
            return Ok(Some(MeasureMetric::Ttfa {
                identity: self.identity.clone(),
                ttfa: ttfb + leading_silence,
                ttfb,
                leading_silence,
            }));
        }
        if !detector.exhausted {
            self.audio = Some(measurement);
        }
        Ok(None)
    }

    /// Number of mono samples retained for onset analysis, not total audio seen.
    pub fn ttfa_buffered_samples(&self) -> usize {
        self.audio
            .as_ref()
            .and_then(|a| a.detector.as_ref())
            .map_or(0, |d| d.window.len())
    }

    pub fn ttfa_active(&self) -> bool {
        self.audio.is_some()
    }

    pub fn start_processing(&mut self) {
        self.start_processing_at(self.clock.now_ns());
    }
    pub fn start_processing_at(&mut self, start_ns: u64) {
        self.processing_start = Some(start_ns);
    }
    pub fn stop_processing(&mut self) -> Option<MeasureMetric> {
        self.stop_processing_at(self.clock.now_ns())
    }
    pub fn stop_processing_at(&mut self, end_ns: u64) -> Option<MeasureMetric> {
        let elapsed = end_ns.checked_sub(self.processing_start.take()?)?;
        Some(MeasureMetric::Processing {
            identity: self.identity.clone(),
            value: seconds(elapsed),
        })
    }

    pub fn start_text_aggregation(&mut self) {
        self.start_text_aggregation_at(self.clock.now_ns());
    }
    /// Later tokens do not restart first-token-to-first-sentence timing.
    pub fn start_text_aggregation_at(&mut self, start_ns: u64) {
        self.aggregation_start.get_or_insert(start_ns);
    }
    pub fn stop_text_aggregation(&mut self) -> Option<MeasureMetric> {
        self.stop_text_aggregation_at(self.clock.now_ns())
    }
    pub fn stop_text_aggregation_at(&mut self, end_ns: u64) -> Option<MeasureMetric> {
        let elapsed = end_ns.checked_sub(self.aggregation_start.take()?)?;
        Some(MeasureMetric::TextAggregation {
            identity: self.identity.clone(),
            value: seconds(elapsed),
        })
    }

    /// Disabling drops pending usage so it cannot leak into a later enabled period.
    pub fn set_usage_enabled(&mut self, enabled: bool) {
        self.usage_enabled = enabled;
        if !enabled {
            self.stt_pending_seconds = 0.0;
        }
    }

    /// Reports a provider delta or one final per-inference usage record.
    /// Do not pass repeated cumulative snapshots here; use `record_llm_cumulative`.
    pub fn record_llm_usage(&self, value: LlmTokenUsage) -> Option<MeasureMetric> {
        (self.usage_enabled && !value.is_zero()).then(|| MeasureMetric::LlmUsage {
            identity: self.identity.clone(),
            value,
        })
    }

    /// Converts monotonic per-request provider snapshots into incremental usage.
    /// Duplicate snapshots emit nothing; a regressing snapshot is rejected
    /// atomically. Unreported optional buckets retain their previous baseline.
    pub fn record_llm_cumulative(
        &mut self,
        mut value: LlmTokenUsage,
    ) -> Result<Option<MeasureMetric>, MetricsError> {
        let delta = if let Some(previous) = &self.llm_previous {
            let delta = value.delta_from(previous)?;
            value.retain_unreported_buckets(previous);
            delta
        } else {
            value.clone()
        };
        self.llm_previous = Some(value);
        Ok(self.record_llm_usage(delta))
    }

    /// Records PCM actually submitted to STT, including submitted silence and
    /// keepalives. Segmented services call this only for submitted segments.
    pub fn record_stt_audio(
        &mut self,
        audio: &[u8],
        sample_rate: u32,
        channels: u16,
    ) -> Result<(), MetricsError> {
        validate_format(sample_rate, channels)?;
        let bytes_per_frame = usize::from(channels) * 2;
        if !audio.len().is_multiple_of(bytes_per_frame) {
            return Err(MetricsError::IncompleteAudioFrame);
        }
        if self.usage_enabled {
            self.stt_pending_seconds += (audio.len() / bytes_per_frame) as f64 / sample_rate as f64;
        }
        Ok(())
    }

    /// Consumes pending usage. Repeated flushes without new audio emit nothing.
    pub fn flush_stt_usage(&mut self) -> Option<MeasureMetric> {
        let audio_seconds = std::mem::take(&mut self.stt_pending_seconds);
        (self.usage_enabled && audio_seconds > 0.0).then(|| MeasureMetric::SttUsage {
            identity: self.identity.clone(),
            audio_seconds,
        })
    }

    /// Counts Unicode scalar values, matching Python's len(text), not UTF-8 bytes.
    pub fn record_tts_text(&self, text: &str) -> Option<MeasureMetric> {
        (self.usage_enabled && !text.is_empty()).then(|| MeasureMetric::TtsUsage {
            identity: self.identity.clone(),
            characters: text.chars().count() as u64,
        })
    }
}

fn seconds(ns: u64) -> f64 {
    ns as f64 / NANOS_PER_SECOND
}

fn validate_format(sample_rate: u32, channels: u16) -> Result<(), MetricsError> {
    if sample_rate == 0 || sample_rate > MAX_SAMPLE_RATE || channels == 0 || channels > MAX_CHANNELS
    {
        return Err(MetricsError::InvalidAudioFormat);
    }
    Ok(())
}

/// Centered, mean-removed RMS over 10ms, sampled every 1ms. Fifty consecutive
/// windows above -40dBFS confirm onset. Keeping only one window bounds memory
/// independently of the caller's chunk size or the length of leading silence.
struct SpeechOnset {
    sample_rate: u32,
    channels: u16,
    frame: usize,
    hop: usize,
    window: VecDeque<f64>,
    sum: f64,
    sum_sq: f64,
    center: u64,
    active_windows: u64,
    samples: u64,
    partial_byte: Option<u8>,
    channel_sum: f64,
    channel_index: u16,
    exhausted: bool,
}

impl SpeechOnset {
    fn new(sample_rate: u32, channels: u16) -> Self {
        let frame = ((sample_rate as f64 / 100.0).round() as usize).max(1);
        let hop = ((sample_rate as f64 / 1000.0).round() as usize).max(1);
        Self {
            sample_rate,
            channels,
            frame,
            hop,
            window: VecDeque::with_capacity(frame),
            sum: 0.0,
            sum_sq: 0.0,
            center: 0,
            active_windows: 0,
            samples: 0,
            partial_byte: None,
            channel_sum: 0.0,
            channel_index: 0,
            exhausted: false,
        }
    }

    fn push_window(&mut self, sample: f64) {
        self.window.push_back(sample);
        self.sum += sample;
        self.sum_sq += sample * sample;
    }

    fn feed(&mut self, audio: &[u8]) -> Option<u64> {
        for &byte in audio {
            let Some(low) = self.partial_byte.take() else {
                self.partial_byte = Some(byte);
                continue;
            };
            self.channel_sum += f64::from(i16::from_le_bytes([low, byte]));
            self.channel_index += 1;
            if self.channel_index < self.channels {
                continue;
            }
            let mono = self.channel_sum / f64::from(self.channels) / 32768.0;
            self.channel_sum = 0.0;
            self.channel_index = 0;
            if self.samples == 0 {
                for _ in 0..self.frame / 2 {
                    self.push_window(mono);
                }
            }
            self.samples += 1;
            self.push_window(mono);
            if self.window.len() == self.frame {
                let mean = self.sum / self.frame as f64;
                let variance = (self.sum_sq / self.frame as f64 - mean * mean).max(0.0);
                if variance > 0.0001 {
                    self.active_windows += 1;
                } else {
                    self.active_windows = 0;
                }
                if self.active_windows >= 50 {
                    return Some(self.center - 49 * self.hop as u64);
                }
                for _ in 0..self.hop {
                    if let Some(old) = self.window.pop_front() {
                        self.sum -= old;
                        self.sum_sq -= old * old;
                    }
                }
                self.center += self.hop as u64;
            }
            if self.samples >= u64::from(self.sample_rate) * ONSET_SCAN_SECONDS {
                self.exhausted = true;
                return None;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Default)]
    struct TestClock(AtomicU64);
    impl ObserverClock for TestClock {
        fn now_ns(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }
    impl TestClock {
        fn set(&self, now: u64) {
            self.0.store(now, Ordering::SeqCst);
        }
    }

    fn collector() -> (ServiceMetrics, Arc<TestClock>) {
        let clock = Arc::new(TestClock::default());
        (
            ServiceMetrics::new("service", Some("model".into()), clock.clone()),
            clock,
        )
    }
    fn silence(samples: usize, channels: u16) -> Vec<u8> {
        vec![0; samples * usize::from(channels) * 2]
    }
    fn tone(samples: usize, sample_rate: u32, channels: u16) -> Vec<u8> {
        (0..samples)
            .flat_map(|i| {
                let sample = ((i as f64 * 220.0 * std::f64::consts::TAU / sample_rate as f64).sin()
                    * 5000.0) as i16;
                (0..channels).flat_map(move |_| sample.to_le_bytes())
            })
            .collect()
    }
    fn close(actual: f64, expected: f64) {
        assert!((actual - expected).abs() < 1e-9, "{actual} != {expected}");
    }
    fn arm_audio(metrics: &mut ServiceMetrics) {
        metrics.begin_request_at(0);
        metrics.stop_ttfb_at(200_000_000).unwrap();
    }

    #[test]
    fn zero_epoch_acknowledgements_and_real_first_output() {
        let (mut m, clock) = collector();
        m.begin_request();
        clock.set(100_000_000);
        assert!(m.observe_output(ServiceOutput::Acknowledgement).is_empty());
        close(m.ttfb().unwrap(), 0.1);
        clock.set(200_000_000);
        let first = m.observe_output(ServiceOutput::Reasoning);
        assert_eq!(
            first,
            vec![MeasureMetric::Ttfb {
                identity: m.identity.clone(),
                value: 0.2
            }]
        );
        clock.set(900_000_000);
        assert_eq!(
            m.observe_output(ServiceOutput::AnswerToken),
            vec![MeasureMetric::Ttfat {
                identity: m.identity.clone(),
                ttfat: 0.9,
                ttfb: 0.2,
                thinking_time: 0.7
            }]
        );
        assert!(m.observe_output(ServiceOutput::AnswerToken).is_empty());
        assert!(m.observe_output(ServiceOutput::ToolCall).is_empty());
    }

    #[test]
    fn tool_call_and_direct_answer_each_end_ttfat_immediately() {
        for output in [ServiceOutput::ToolCall, ServiceOutput::AnswerToken] {
            let (mut m, _) = collector();
            m.begin_request_at(10);
            let got = m.observe_output_at(output, 10);
            assert_eq!(got.len(), 2);
            assert!(matches!(
                got[1],
                MeasureMetric::Ttfat {
                    ttfat: 0.0,
                    ttfb: 0.0,
                    thinking_time: 0.0,
                    ..
                }
            ));
            assert_eq!(m.ttfb(), Some(0.0));
        }
    }

    #[test]
    fn stale_timestamps_close_measurements_without_negative_metrics() {
        let (mut m, _) = collector();
        m.begin_request_at(100);
        assert!(m
            .observe_output_at(ServiceOutput::AnswerToken, 99)
            .is_empty());
        assert!(m
            .observe_output_at(ServiceOutput::AnswerToken, 200)
            .is_empty());
        m.begin_request_at(100);
        m.stop_ttfb_at(200).unwrap();
        assert!(m.stop_ttfat_at(199).is_none());
        assert!(m.stop_ttfat_at(300).is_none());
        m.start_processing_at(100);
        assert!(m.stop_processing_at(99).is_none());
        assert!(m.stop_processing_at(200).is_none());
        m.start_text_aggregation_at(100);
        assert!(m.stop_text_aggregation_at(99).is_none());
    }

    #[test]
    fn cancellation_and_new_requests_drop_all_pending_timers() {
        let (mut m, _) = collector();
        arm_audio(&mut m);
        m.process_ttfa(&silence(1600, 1), 16000, 1).unwrap();
        m.start_processing_at(0);
        m.start_text_aggregation_at(0);
        m.cancel_request();
        assert!(m.stop_processing_at(10).is_none());
        assert!(m.stop_text_aggregation_at(10).is_none());
        assert!(m.stop_ttfat_at(900_000_000).is_none());
        assert!(m
            .process_ttfa(&tone(3200, 16000, 1), 16000, 1)
            .unwrap()
            .is_none());
        arm_audio(&mut m);
        m.process_ttfa(&silence(1600, 1), 16000, 1).unwrap();
        m.begin_request_at(2_000_000_000);
        assert!(!m.ttfa_active());
        assert!(m.stop_ttfat_at(2_100_000_000).is_none());
        let got = m.observe_output_at(ServiceOutput::AnswerToken, 2_300_000_000);
        assert!(matches!(
            got[1],
            MeasureMetric::Ttfat {
                ttfat: 0.3,
                thinking_time: 0.0,
                ..
            }
        ));
    }

    #[test]
    fn initial_only_clears_abandoned_audio_and_answer_then_reset_rearms() {
        let (_, clock) = collector();
        let mut m = ServiceMetrics::with_policy("svc", None, clock, TtfbPolicy::InitialOnly);
        arm_audio(&mut m);
        m.begin_request_at(2_000_000_000);
        assert!(m
            .observe_output_at(ServiceOutput::AnswerToken, 2_200_000_000)
            .is_empty());
        assert!(!m.ttfa_active());
        m.reset_session();
        m.begin_request_at(0);
        assert_eq!(m.observe_output_at(ServiceOutput::ToolCall, 10).len(), 2);
    }

    #[test]
    fn no_measurements_without_starts_and_processing_uses_first_aggregation_token() {
        let (mut m, clock) = collector();
        assert!(m.stop_ttfb().is_none());
        assert!(m.stop_ttfat().is_none());
        assert!(m.stop_processing().is_none());
        assert!(m.stop_text_aggregation().is_none());
        m.start_processing();
        m.start_text_aggregation();
        clock.set(100_000_000);
        m.start_text_aggregation();
        clock.set(250_000_000);
        assert!(matches!(
            m.stop_processing(),
            Some(MeasureMetric::Processing { value: 0.25, .. })
        ));
        assert!(matches!(
            m.stop_text_aggregation(),
            Some(MeasureMetric::TextAggregation { value: 0.25, .. })
        ));
        assert!(m.stop_processing().is_none());
        assert!(m.stop_text_aggregation().is_none());
    }

    #[test]
    fn streamed_ttfa_carries_ttfb_and_silence_and_emits_once() {
        let (mut m, _) = collector();
        arm_audio(&mut m);
        assert!(m
            .process_ttfa(&silence(4800, 1), 16000, 1)
            .unwrap()
            .is_none());
        let mut next = silence(1600, 1);
        next.extend(tone(4800, 16000, 1));
        let metric = m.process_ttfa(&next, 16000, 1).unwrap().unwrap();
        let MeasureMetric::Ttfa {
            ttfa,
            ttfb,
            leading_silence,
            ..
        } = metric
        else {
            panic!("wrong metric")
        };
        close(ttfb, 0.2);
        assert!((leading_silence - 0.4).abs() < 0.01);
        close(ttfa, ttfb + leading_silence);
        assert!(!m.ttfa_active());
        assert!(m.process_ttfa(&next, 16000, 1).unwrap().is_none());
    }

    #[test]
    fn onset_is_independent_of_odd_byte_and_interleaved_chunk_boundaries() {
        for channels in [1, 2, 3] {
            let mut pcm = silence(1600, channels);
            pcm.extend(tone(3200, 16000, channels));
            let (mut whole, _) = collector();
            arm_audio(&mut whole);
            let expected = whole.process_ttfa(&pcm, 16000, channels).unwrap();
            assert!(expected.is_some());
            for chunk_size in [1, 3, 127, 320, 1999] {
                let (mut streamed, _) = collector();
                arm_audio(&mut streamed);
                let mut got = None;
                for chunk in pcm.chunks(chunk_size) {
                    if let Some(metric) = streamed.process_ttfa(chunk, 16000, channels).unwrap() {
                        assert!(got.is_none());
                        got = Some(metric);
                    }
                    assert!(streamed.ttfa_buffered_samples() <= 160);
                }
                assert_eq!(got, expected);
            }
        }
    }

    #[test]
    fn silence_dc_blips_and_short_bursts_do_not_trigger_audio_onset() {
        let mut blip = silence(1600, 1);
        blip.extend(i16::MAX.to_le_bytes());
        blip.extend(silence(3200, 1));
        let mut burst = silence(1600, 1);
        burst.extend(tone(160, 16000, 1));
        burst.extend(silence(3200, 1));
        let dc: Vec<u8> = (0..8000).flat_map(|_| 10000i16.to_le_bytes()).collect();
        for pcm in [silence(8000, 1), dc, blip, burst] {
            let (mut m, _) = collector();
            arm_audio(&mut m);
            assert!(m.process_ttfa(&pcm, 16000, 1).unwrap().is_none());
            assert!(m.ttfa_buffered_samples() <= 160);
        }
    }

    #[test]
    fn onset_scanning_caps_work_and_memory_even_for_one_huge_chunk() {
        let (mut m, _) = collector();
        arm_audio(&mut m);
        let mut pcm = silence(16000 * 4, 1);
        pcm.extend(tone(3200, 16000, 1));
        assert!(m.process_ttfa(&pcm, 16000, 1).unwrap().is_none());
        assert!(!m.ttfa_active());
        assert_eq!(m.ttfa_buffered_samples(), 0);
        assert!(m
            .process_ttfa(&tone(3200, 16000, 1), 16000, 1)
            .unwrap()
            .is_none());
        arm_audio(&mut m);
        assert!(m
            .process_ttfa(&tone(3200, 16000, 1), 16000, 1)
            .unwrap()
            .is_some());
    }

    #[test]
    fn malformed_formats_cancel_audio_and_partial_pcm_cannot_fabricate_onset() {
        let (mut m, _) = collector();
        arm_audio(&mut m);
        for (rate, channels) in [(0, 1), (16000, 0), (u32::MAX, 1), (16000, u16::MAX)] {
            arm_audio(&mut m);
            assert_eq!(
                m.process_ttfa(&[1], rate, channels),
                Err(MetricsError::InvalidAudioFormat)
            );
            assert!(!m.ttfa_active());
        }
        arm_audio(&mut m);
        assert!(m.process_ttfa(&[255], 16000, 2).unwrap().is_none());
        assert_eq!(
            m.process_ttfa(&[1], 24000, 2),
            Err(MetricsError::AudioFormatChanged)
        );
        assert!(!m.ttfa_active());
        assert!(m.finish_request().is_none());
        m.begin_request_at(0);
        assert!(m.observe_audio_at(&[], 16000, 1, 100).unwrap().is_empty());
        assert!(m.stop_ttfb_at(200).is_some());
    }

    #[test]
    fn audio_path_reports_first_byte_then_sustained_audible_onset() {
        let (mut m, _) = collector();
        m.begin_request_at(0);
        let metrics = m
            .observe_audio_at(&tone(3200, 16000, 1), 16000, 1, 100_000_000)
            .unwrap();
        assert_eq!(metrics.len(), 2);
        assert!(matches!(metrics[0], MeasureMetric::Ttfb { value: 0.1, .. }));
        assert!(matches!(
            metrics[1],
            MeasureMetric::Ttfa {
                leading_silence: 0.0,
                ..
            }
        ));
    }

    #[test]
    fn stt_usage_is_incremental_counts_channels_and_flushes_trailing_audio() {
        let (mut m, _) = collector();
        m.record_stt_audio(&silence(16000, 2), 16000, 2).unwrap();
        m.record_stt_audio(&silence(8000, 1), 16000, 1).unwrap();
        assert!(matches!(
            m.flush_stt_usage(),
            Some(MeasureMetric::SttUsage {
                audio_seconds: 1.5,
                ..
            })
        ));
        assert!(m.flush_stt_usage().is_none());
        m.record_stt_audio(&silence(4000, 1), 16000, 1).unwrap();
        m.begin_request_at(0);
        m.cancel_request();
        assert!(matches!(
            m.finish_request(),
            Some(MeasureMetric::SttUsage {
                audio_seconds: 0.25,
                ..
            })
        ));
        assert_eq!(
            m.record_stt_audio(&[0], 16000, 1),
            Err(MetricsError::IncompleteAudioFrame)
        );
        assert!(m.flush_stt_usage().is_none());
    }

    #[test]
    fn disabled_usage_cannot_leak_to_later_reporting_and_unicode_counts_characters() {
        let (mut m, _) = collector();
        assert!(matches!(
            m.record_tts_text("aé中🦀"),
            Some(MeasureMetric::TtsUsage { characters: 4, .. })
        ));
        assert!(m.record_tts_text("").is_none());
        m.record_stt_audio(&silence(16000, 1), 16000, 1).unwrap();
        m.set_usage_enabled(false);
        m.record_stt_audio(&silence(16000, 1), 16000, 1).unwrap();
        assert!(m.record_tts_text("text").is_none());
        assert!(m
            .record_llm_usage(LlmTokenUsage {
                total_tokens: 1,
                ..Default::default()
            })
            .is_none());
        m.set_usage_enabled(true);
        assert!(m.flush_stt_usage().is_none());
    }

    #[test]
    fn provider_token_buckets_and_model_identity_are_preserved() {
        let (m, _) = collector();
        let usage = LlmTokenUsage {
            prompt_tokens: 100,
            completion_tokens: 50,
            total_tokens: 2450,
            cache_read_input_tokens: Some(2000),
            cache_creation_input_tokens: Some(300),
            reasoning_tokens: Some(20),
            input_audio_tokens: Some(10),
            output_audio_tokens: Some(15),
            cache_read_input_audio_tokens: Some(100),
            prompt_accounting: PromptTokenAccounting::ExcludesCache,
        };
        let metric = m.record_llm_usage(usage.clone()).unwrap();
        assert_eq!(
            metric,
            MeasureMetric::LlmUsage {
                identity: MetricIdentity {
                    processor: "service".into(),
                    model: Some("model".into())
                },
                value: usage
            }
        );
        let json = serde_json::to_string(&metric).unwrap();
        assert_eq!(
            serde_json::from_str::<MeasureMetric>(&json).unwrap(),
            metric
        );
        assert!(m.record_llm_usage(LlmTokenUsage::default()).is_none());
        assert!(m
            .record_llm_usage(LlmTokenUsage {
                cache_read_input_tokens: Some(1),
                ..Default::default()
            })
            .is_some());
    }

    #[test]
    fn cumulative_usage_emits_deltas_rejects_regression_and_resets_per_request() {
        let (mut m, _) = collector();
        m.begin_request_at(0);
        let first = LlmTokenUsage {
            prompt_tokens: 10,
            completion_tokens: 2,
            total_tokens: 12,
            reasoning_tokens: Some(1),
            ..Default::default()
        };
        assert!(m.record_llm_cumulative(first.clone()).unwrap().is_some());
        assert!(m.record_llm_cumulative(first.clone()).unwrap().is_none());
        let next = LlmTokenUsage {
            completion_tokens: 5,
            total_tokens: 15,
            reasoning_tokens: None,
            ..first.clone()
        };
        let Some(MeasureMetric::LlmUsage { value, .. }) =
            m.record_llm_cumulative(next.clone()).unwrap()
        else {
            panic!("missing usage")
        };
        assert_eq!(
            (
                value.prompt_tokens,
                value.completion_tokens,
                value.total_tokens
            ),
            (0, 3, 3)
        );
        assert_eq!(
            m.record_llm_cumulative(first.clone()),
            Err(MetricsError::UsageRegression)
        );
        assert!(m.record_llm_cumulative(next.clone()).unwrap().is_none());
        let next = LlmTokenUsage {
            reasoning_tokens: Some(2),
            ..next
        };
        let Some(MeasureMetric::LlmUsage { value, .. }) = m.record_llm_cumulative(next).unwrap()
        else {
            panic!("missing bucket delta")
        };
        assert_eq!(value.reasoning_tokens, Some(1));
        m.begin_request_at(100);
        assert!(m.record_llm_cumulative(first).unwrap().is_some());
    }
}
