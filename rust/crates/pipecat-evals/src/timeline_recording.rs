//! Bounded, two-sided diagnostic audio on one session-relative monotonic clock.
//!
//! User timestamps describe source-clip enqueue, but only successful local mpsc
//! admissions qualify source bytes. Bot timestamps describe application ingest;
//! its continuous virtual playout is a simulation, not a remote acknowledgement.
use crate::{matcher::ScriptResult, EvalError};
use std::{mem::size_of, sync::Arc, time::Duration};

const NS_PER_SECOND: u128 = 1_000_000_000;
const GAP_NS: u128 = 200_000_000;
const OUTPUT_RATE: u32 = 16_000;
const MAX_DURATION: Duration = Duration::from_secs(120);
const MAX_RAW_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS: usize = 4096;
const MAX_OUTPUT_BYTES: usize = 3_840_000;
const MAX_LAYOUT_BYTES: usize = 11_520_000;
const METADATA_BYTES: usize = 2 * 1024 * 1024;
const RENDER_BYTES: usize = 128 * 1024 * 1024;

/// Limits for the opt-in direct timeline recorder. Smaller positive limits are
/// supported; none can exceed the defaults. Existing capture limits are separate.
#[derive(Debug, Clone, Copy)]
pub struct TimelineRecordingLimits {
    pub max_duration: Duration,
    pub max_raw_bytes: usize,
    pub max_records: usize,
}

impl Default for TimelineRecordingLimits {
    fn default() -> Self {
        Self {
            max_duration: MAX_DURATION,
            max_raw_bytes: MAX_RAW_BYTES,
            max_records: MAX_RECORDS,
        }
    }
}

impl TimelineRecordingLimits {
    pub(crate) fn validate(self) -> Result<(), EvalError> {
        if self.max_duration.is_zero()
            || self.max_duration > MAX_DURATION
            || self.max_raw_bytes == 0
            || self.max_raw_bytes > MAX_RAW_BYTES
            || self.max_records == 0
            || self.max_records > MAX_RECORDS
        {
            return Err(EvalError::Scenario(
                "invalid timeline recording limits".into(),
            ));
        }
        Ok(())
    }
}

/// The original evaluation result and an immutable, bounded diagnostic prefix.
/// A failed result stays failed even when audio can be exported. No artifact is
/// promised if the run future itself is dropped instead of awaited to completion.
#[derive(Debug)]
pub struct TimelineRecordingRun {
    pub result: ScriptResult,
    capture: Capture,
}

impl TimelineRecordingRun {
    /// Render mono PCM16 at 16000 Hz, saturating overlap and zero-padding the
    /// shorter side. Empty or wholly cropped audio returns `None`. Rendering
    /// errors never produce an allegedly complete, silently truncated WAV.
    pub fn to_wav(&self) -> Result<Option<Vec<u8>>, EvalError> {
        self.capture.to_wav()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UserTicket(usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    User,
    Bot,
}

#[derive(Debug)]
enum Source {
    User(Arc<[u8]>),
    Bot(Box<[u8]>),
}

impl Source {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::User(audio) => audio,
            Self::Bot(audio) => audio,
        }
    }

    fn side(&self) -> Side {
        match self {
            Self::User(_) => Side::User,
            Self::Bot(_) => Side::Bot,
        }
    }
}

#[derive(Debug)]
struct Record {
    source: Source,
    arrival_ns: u64,
    /// User: successful source-byte prefix. Bot: surviving virtual-playout prefix.
    retained_bytes: usize,
    /// Bot's continuous virtual start, in nanoseconds multiplied by its rate.
    bot_start_scaled: u128,
}

/// Pure state: callers provide elapsed nanoseconds from their one run origin.
/// The owning session synchronizes mutations, closes before cleanup, permits
/// outstanding successful input receipts through Playback's actual join, then
/// consumes this value. There is no encoder task or independently ticking clock.
#[derive(Debug)]
pub(crate) struct Capture {
    limits: TimelineRecordingLimits,
    records: Vec<Record>,
    raw_bytes: usize,
    bot_rate: Option<u32>,
    user_last_arrival: Option<u64>,
    bot_last_arrival: Option<u64>,
    bot_position: Option<u128>,
    last_interruption: Option<u64>,
    closed_at: Option<u64>,
}

impl Capture {
    pub(crate) fn new(limits: TimelineRecordingLimits) -> Result<Self, EvalError> {
        limits.validate()?;
        let mut records = Vec::new();
        records
            .try_reserve_exact(limits.max_records)
            .map_err(|_| EvalError::Limit("timeline recording metadata allocation"))?;
        check_capacity(records.capacity(), size_of::<Record>(), METADATA_BYTES)?;
        // This upper envelope includes hidden pipecat-audio Vec/VecDeque growth;
        // it is a conservative capacity reservation, not a measured RSS claim.
        render_envelope(MAX_LAYOUT_BYTES, MAX_OUTPUT_BYTES)?;
        Ok(Self {
            limits,
            records,
            raw_bytes: 0,
            bot_rate: None,
            user_last_arrival: None,
            bot_last_arrival: None,
            bot_position: None,
            last_interruption: None,
            closed_at: None,
        })
    }

    fn admit(&self, bytes: usize) -> Result<usize, EvalError> {
        if self.closed_at.is_some() {
            return Err(EvalError::Cancelled);
        }
        if self.records.len() >= self.limits.max_records {
            return Err(EvalError::Limit("timeline recording records"));
        }
        self.raw_bytes
            .checked_add(bytes)
            .filter(|total| *total <= self.limits.max_raw_bytes)
            .ok_or(EvalError::Limit("timeline recording raw bytes"))
    }

    /// For a bounded encoded-payload precheck while the same capture lock is
    /// held through decode and admission. Cropping never replenishes this quota.
    pub(crate) fn remaining_raw_bytes(&self) -> usize {
        self.limits.max_raw_bytes.saturating_sub(self.raw_bytes)
    }

    pub(crate) fn remaining_records(&self) -> usize {
        self.limits.max_records.saturating_sub(self.records.len())
    }

    /// Call only while holding both the capture lock and a reserved queue slot;
    /// publish that slot without an intervening await. The shared source is not
    /// yet qualified for rendering and no wire padding is registered here.
    pub(crate) fn register_user(
        &mut self,
        at_ns: u64,
        pcm: Arc<[u8]>,
    ) -> Result<UserTicket, EvalError> {
        validate_pcm(&pcm, OUTPUT_RATE, 1)?;
        check_monotone(self.user_last_arrival, at_ns)?;
        let raw_bytes = self.admit(pcm.len())?;
        let ticket = UserTicket(self.records.len());
        self.records.push(Record {
            source: Source::User(pcm),
            arrival_ns: at_ns,
            retained_bytes: 0,
            bot_start_scaled: 0,
        });
        self.raw_bytes = raw_bytes;
        self.user_last_arrival = Some(at_ns);
        Ok(ticket)
    }

    /// Commit synchronously immediately after the local sender returns success,
    /// before any await or completion receipt. Closing new admission deliberately
    /// does not disable these already registered receipts. A failed send must
    /// never call this method; offsets are source offsets, excluding padding.
    pub(crate) fn commit_user(
        &mut self,
        ticket: UserTicket,
        offset: usize,
        len: usize,
    ) -> Result<(), EvalError> {
        let record = self
            .records
            .get_mut(ticket.0)
            .filter(|record| record.source.side() == Side::User)
            .ok_or_else(|| EvalError::Scenario("invalid timeline source ticket".into()))?;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= record.source.bytes().len())
            .ok_or(EvalError::Limit("timeline source receipt"))?;
        if offset != record.retained_bytes || len == 0 || !len.is_multiple_of(2) {
            return Err(EvalError::Scenario(
                "timeline source receipt must be a contiguous whole-sample prefix".into(),
            ));
        }
        record.retained_bytes = end;
        Ok(())
    }

    pub(crate) fn ingest_bot(
        &mut self,
        at_ns: u64,
        pcm: Vec<u8>,
        sample_rate: u32,
        channels: u16,
    ) -> Result<(), EvalError> {
        validate_pcm(&pcm, sample_rate, channels)?;
        if self.bot_rate.is_some_and(|rate| rate != sample_rate) {
            return Err(EvalError::Unsupported(
                "timeline bot sample rate changed within a recording".into(),
            ));
        }
        check_monotone(self.bot_last_arrival, at_ns)?;
        let raw_bytes = self.admit(pcm.len())?;
        // The session preflights and decodes into an exact-sized buffer. Do not
        // retain arbitrary caller-supplied spare capacity. Shrinking into a box
        // happens during admission, before any render buffer exists.
        check_capacity(pcm.capacity(), 1, MAX_RAW_BYTES)?;
        let start = scaled_time(at_ns, sample_rate)?.max(self.bot_position.unwrap_or(0));
        let end = advance(start, pcm.len() / 2)?;
        let retained_bytes = pcm.len();
        self.records.push(Record {
            source: Source::Bot(pcm.into_boxed_slice()),
            arrival_ns: at_ns,
            retained_bytes,
            bot_start_scaled: start,
        });
        self.raw_bytes = raw_bytes;
        self.bot_rate = Some(sample_rate);
        self.bot_position = Some(end);
        self.bot_last_arrival = Some(at_ns);
        Ok(())
    }

    /// Trim the future bot suffix across every queued chunk. Neither cumulative
    /// quota nor the fixed bot rate is reset. Untagged later PCM is a new local
    /// admission; it cannot be authenticated as an old/new remote generation.
    pub(crate) fn interrupt_bot(&mut self, at_ns: u64) -> Result<(), EvalError> {
        if self.closed_at.is_some() {
            return Ok(());
        }
        check_monotone(self.last_interruption, at_ns)?;
        self.crop_bot(at_ns)?;
        self.last_interruption = Some(at_ns);
        Ok(())
    }

    /// Latch the semantic cutoff before any asynchronous cleanup. Reentrant
    /// calls cannot advance it, including after a slow or cancelled join wait.
    pub(crate) fn close(&mut self, at_ns: u64) -> Result<(), EvalError> {
        if self.closed_at.is_some() {
            return Ok(());
        }
        self.closed_at = Some(at_ns);
        self.crop_bot(at_ns)
    }

    fn crop_bot(&mut self, at_ns: u64) -> Result<(), EvalError> {
        let Some(rate) = self.bot_rate else {
            return Ok(());
        };
        let cutoff = scaled_time(at_ns, rate)?;
        self.bot_position = None;
        for record in &mut self.records {
            if record.source.side() != Side::Bot || record.retained_bytes == 0 {
                continue;
            }
            let remaining_frames = cutoff.saturating_sub(record.bot_start_scaled) / NS_PER_SECOND;
            let kept = remaining_frames.min((record.retained_bytes / 2) as u128);
            record.retained_bytes = frame_bytes(kept)?;
            if record.retained_bytes != 0 {
                self.bot_position = Some(advance(record.bot_start_scaled, kept as usize)?);
            }
        }
        Ok(())
    }

    pub(crate) fn into_run(self, result: ScriptResult) -> TimelineRecordingRun {
        TimelineRecordingRun {
            result,
            capture: self,
        }
    }

    fn side_records(&self, side: Side) -> impl Iterator<Item = &Record> {
        self.records
            .iter()
            .filter(move |record| record.source.side() == side && record.retained_bytes != 0)
    }

    fn plan(&self, side: Side, origin_ns: u64) -> Result<Option<SidePlan>, EvalError> {
        let rate = match side {
            Side::User => OUTPUT_RATE,
            Side::Bot => self.bot_rate.unwrap_or(OUTPUT_RATE),
        };
        let mut records = self.side_records(side);
        let Some(first) = records.next() else {
            return Ok(None);
        };
        let mut cursor = LayoutCursor::new(first.arrival_ns, rate)?;
        cursor.append(first.arrival_ns, first.retained_bytes / 2, rate)?;
        for record in records {
            cursor.append(record.arrival_ns, record.retained_bytes / 2, rate)?;
        }
        let origin = scaled_time(origin_ns, rate)?;
        let span = cursor
            .logical_scaled
            .checked_sub(origin)
            .ok_or(EvalError::Limit("timeline recording time order"))?;
        let max_span = self
            .limits
            .max_duration
            .as_nanos()
            .checked_mul(u128::from(rate))
            .ok_or(EvalError::Limit("timeline recording duration overflow"))?;
        if span > max_span {
            return Err(EvalError::Limit("timeline recording duration"));
        }
        let layout_bytes = frame_bytes(cursor.rendered_frames)?;
        let rate_bound = frame_bytes(max_span / NS_PER_SECOND)?;
        if layout_bytes > rate_bound || layout_bytes > MAX_LAYOUT_BYTES {
            return Err(EvalError::Limit("timeline original-rate layout"));
        }
        let resampled_frames = resampled_count(cursor.rendered_frames, rate)?;
        let leading_frames = u128::from(
            first
                .arrival_ns
                .checked_sub(origin_ns)
                .ok_or(EvalError::Limit("timeline recording origin"))?,
        )
        .checked_mul(u128::from(OUTPUT_RATE))
        .ok_or(EvalError::Limit("timeline alignment overflow"))?
            / NS_PER_SECOND;
        let aligned_frames = leading_frames
            .checked_add(resampled_frames)
            .ok_or(EvalError::Limit("timeline output overflow"))?;
        let output_bound = self
            .limits
            .max_duration
            .as_nanos()
            .checked_mul(u128::from(OUTPUT_RATE))
            .ok_or(EvalError::Limit("timeline output duration overflow"))?
            / NS_PER_SECOND;
        if aligned_frames > output_bound || frame_bytes(aligned_frames)? > MAX_OUTPUT_BYTES {
            return Err(EvalError::Limit("timeline aligned output"));
        }
        Ok(Some(SidePlan {
            side,
            rate,
            first_ns: first.arrival_ns,
            layout_bytes,
            resampled_bytes: frame_bytes(resampled_frames)?,
            leading_bytes: frame_bytes(leading_frames)?,
            output_bytes: frame_bytes(aligned_frames)?,
        }))
    }

    fn render_side(&self, plan: Option<SidePlan>) -> Result<Vec<u8>, EvalError> {
        let Some(plan) = plan else {
            return Ok(Vec::new());
        };
        let mut layout = exact_buffer(plan.layout_bytes)?;
        let mut cursor = LayoutCursor::new(plan.first_ns, plan.rate)?;
        for record in self.side_records(plan.side) {
            let gap_frames =
                cursor.append(record.arrival_ns, record.retained_bytes / 2, plan.rate)?;
            let gap_bytes = frame_bytes(gap_frames)?;
            let after_gap = layout
                .len()
                .checked_add(gap_bytes)
                .filter(|size| *size <= plan.layout_bytes)
                .ok_or(EvalError::Limit("timeline render gap"))?;
            layout.resize(after_gap, 0);
            layout.extend_from_slice(&record.source.bytes()[..record.retained_bytes]);
        }
        if layout.len() != plan.layout_bytes {
            return Err(EvalError::Limit("timeline render length"));
        }
        let resampled = if plan.rate == OUTPUT_RATE {
            layout
        } else {
            // One state and one flush for the entire side, including its gaps.
            // This returns after dropping resampler decoder/deque allocations.
            let output = pipecat_audio::resample_pcm16(&layout, plan.rate, OUTPUT_RATE, 1)
                .map_err(audio_error)?;
            drop(layout);
            output
        };
        if resampled.len() != plan.resampled_bytes {
            return Err(EvalError::Limit("timeline resampler output length"));
        }
        check_capacity(
            resampled.capacity(),
            1,
            growth_capacity(plan.resampled_bytes)?,
        )?;
        if plan.leading_bytes == 0 {
            return Ok(resampled);
        }
        // The original layout and all resampler state have retired before this
        // alignment allocation. No clone of an already aligned track is made.
        let mut aligned = exact_buffer(plan.output_bytes)?;
        aligned.resize(plan.leading_bytes, 0);
        aligned.extend_from_slice(&resampled);
        drop(resampled);
        Ok(aligned)
    }

    fn to_wav(&self) -> Result<Option<Vec<u8>>, EvalError> {
        let Some(origin_ns) = self
            .records
            .iter()
            .filter(|record| record.retained_bytes != 0)
            .map(|record| record.arrival_ns)
            .min()
        else {
            return Ok(None);
        };
        // Calculate both original layouts, common alignment, exact sinc-flush
        // lengths and RIFF sizes before constructing any large render buffer.
        let user_plan = self.plan(Side::User, origin_ns)?;
        let bot_plan = self.plan(Side::Bot, origin_ns)?;
        let output_bytes = user_plan
            .map_or(0, |plan| plan.output_bytes)
            .max(bot_plan.map_or(0, |plan| plan.output_bytes));
        let wav_bytes = checked_wav_bytes(output_bytes)?;
        let largest_layout = user_plan
            .map_or(0, |plan| plan.layout_bytes)
            .max(bot_plan.map_or(0, |plan| plan.layout_bytes));
        render_envelope(largest_layout, output_bytes)?;
        let user = self.render_side(user_plan)?;
        let bot = self.render_side(bot_plan)?;
        let mixed = pipecat_audio::mix_audio(&user, &bot).map_err(audio_error)?;
        drop(user);
        drop(bot);
        if mixed.len() != output_bytes || !mixed.len().is_multiple_of(2) {
            return Err(EvalError::Limit("timeline mixed PCM length"));
        }
        check_capacity(mixed.capacity(), 1, growth_capacity(output_bytes)?)?;
        let wav = pipecat_audio::pcm_to_wav(&mixed, OUTPUT_RATE, 1).map_err(audio_error)?;
        drop(mixed);
        if wav.len() != wav_bytes {
            return Err(EvalError::Limit("timeline WAV length"));
        }
        check_capacity(wav.capacity(), 1, wav_bytes)?;
        Ok(Some(wav))
    }
}

#[derive(Debug, Clone, Copy)]
struct SidePlan {
    side: Side,
    rate: u32,
    first_ns: u64,
    layout_bytes: usize,
    resampled_bytes: usize,
    leading_bytes: usize,
    output_bytes: usize,
}

/// The logical cursor tracks time even when small jitter contributes no samples.
/// The rendered cursor only counts original-rate samples actually appended.
#[derive(Debug)]
struct LayoutCursor {
    logical_scaled: u128,
    rendered_frames: u128,
    last_arrival: u64,
}

impl LayoutCursor {
    fn new(at_ns: u64, rate: u32) -> Result<Self, EvalError> {
        Ok(Self {
            logical_scaled: scaled_time(at_ns, rate)?,
            rendered_frames: 0,
            last_arrival: at_ns,
        })
    }

    fn append(&mut self, at_ns: u64, frames: usize, rate: u32) -> Result<u128, EvalError> {
        check_monotone(Some(self.last_arrival), at_ns)?;
        let arrival = scaled_time(at_ns, rate)?;
        let late = arrival.saturating_sub(self.logical_scaled);
        let threshold = GAP_NS
            .checked_mul(u128::from(rate))
            .ok_or(EvalError::Limit("timeline gap threshold"))?;
        let gap_frames = if late > threshold {
            late / NS_PER_SECOND
        } else {
            0
        };
        let rendered = self
            .rendered_frames
            .checked_add(gap_frames)
            .and_then(|count| count.checked_add(frames as u128))
            .ok_or(EvalError::Limit("timeline sample count overflow"))?;
        let logical = advance(self.logical_scaled.max(arrival), frames)?;
        self.rendered_frames = rendered;
        self.logical_scaled = logical;
        self.last_arrival = at_ns;
        Ok(gap_frames)
    }
}

fn check_monotone(previous: Option<u64>, at_ns: u64) -> Result<(), EvalError> {
    if previous.is_some_and(|previous| at_ns < previous) {
        return Err(EvalError::Scenario(
            "timeline timestamps must be monotonic".into(),
        ));
    }
    Ok(())
}

fn validate_pcm(pcm: &[u8], rate: u32, channels: u16) -> Result<(), EvalError> {
    if channels != 1
        || !matches!(rate, 8_000 | 16_000 | 24_000 | 48_000)
        || pcm.is_empty()
        || !pcm.len().is_multiple_of(2)
    {
        return Err(EvalError::Scenario(
            "timeline audio requires nonempty mono PCM16 at 8/16/24/48 kHz".into(),
        ));
    }
    Ok(())
}

fn scaled_time(at_ns: u64, rate: u32) -> Result<u128, EvalError> {
    u128::from(at_ns)
        .checked_mul(u128::from(rate))
        .ok_or(EvalError::Limit("timeline timestamp overflow"))
}

fn advance(position: u128, frames: usize) -> Result<u128, EvalError> {
    (frames as u128)
        .checked_mul(NS_PER_SECOND)
        .and_then(|duration| position.checked_add(duration))
        .ok_or(EvalError::Limit("timeline position overflow"))
}

fn frame_bytes(frames: u128) -> Result<usize, EvalError> {
    frames
        .checked_mul(2)
        .and_then(|bytes| usize::try_from(bytes).ok())
        .ok_or(EvalError::Limit("timeline byte count overflow"))
}

fn resampled_count(frames: u128, rate: u32) -> Result<u128, EvalError> {
    frames
        .checked_mul(u128::from(OUTPUT_RATE))
        .and_then(|count| count.checked_add(u128::from(rate) / 2))
        .and_then(|count| count.checked_div(u128::from(rate)))
        .ok_or(EvalError::Limit("timeline resampled count overflow"))
}

fn checked_wav_bytes(bytes: usize) -> Result<usize, EvalError> {
    if !bytes.is_multiple_of(2) || bytes > MAX_OUTPUT_BYTES {
        return Err(EvalError::Limit("timeline WAV PCM bytes"));
    }
    u32::try_from(bytes)
        .ok()
        .and_then(|count| count.checked_add(36))
        .ok_or(EvalError::Limit("timeline RIFF length"))?;
    bytes
        .checked_add(44)
        .ok_or(EvalError::Limit("timeline WAV overflow"))
}

fn check_capacity(capacity: usize, item_bytes: usize, bound: usize) -> Result<(), EvalError> {
    if capacity
        .checked_mul(item_bytes)
        .is_none_or(|bytes| bytes > bound)
    {
        return Err(EvalError::Limit("timeline allocation capacity"));
    }
    Ok(())
}

fn exact_buffer(bytes: usize) -> Result<Vec<u8>, EvalError> {
    let mut buffer = Vec::new();
    buffer
        .try_reserve_exact(bytes)
        .map_err(|_| EvalError::Limit("timeline buffer allocation"))?;
    check_capacity(buffer.capacity(), 1, bytes)?;
    Ok(buffer)
}

fn growth_capacity(bytes: usize) -> Result<usize, EvalError> {
    bytes
        .checked_mul(2)
        .map(|bound| bound.max(8))
        .ok_or(EvalError::Limit("timeline capacity overflow"))
}

fn render_envelope(layout: usize, output: usize) -> Result<usize, EvalError> {
    // Raw 8MiB + metadata 2MiB + original layout L + decoder pending 2L
    // + decoded samples 2L + deque reallocation old/new 3L
    // + completed other-side capacity 2O + current output growth old/new 3O.
    // Layout/resampler retire before alignment; both aligned tracks retire before
    // WAV allocation. Mixer phases (tracks + decoded copies + mixed growth) and
    // WAV phases are smaller than this envelope at the fixed maxima. Tiny sinc
    // per-channel scratch/filter-tail allocations fit the >11MiB remaining margin.
    let bytes = layout
        .checked_mul(8)
        .and_then(|value| output.checked_mul(5).and_then(|out| value.checked_add(out)))
        .and_then(|value| value.checked_add(MAX_RAW_BYTES + METADATA_BYTES))
        .filter(|value| *value <= RENDER_BYTES)
        .ok_or(EvalError::Limit("timeline render memory envelope"))?;
    Ok(bytes)
}

fn audio_error(error: pipecat_audio::AudioError) -> EvalError {
    EvalError::Scenario(format!("timeline recording audio: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcm(samples: &[i16]) -> Vec<u8> {
        pipecat_audio::encode_pcm16_le(samples)
    }

    fn capture() -> Capture {
        Capture::new(TimelineRecordingLimits::default()).unwrap()
    }

    fn user(capture: &mut Capture, at_ns: u64, samples: &[i16]) -> UserTicket {
        let audio: Arc<[u8]> = Arc::from(pcm(samples));
        let len = audio.len();
        let ticket = capture.register_user(at_ns, audio).unwrap();
        capture.commit_user(ticket, 0, len).unwrap();
        ticket
    }

    fn bot(capture: &mut Capture, at_ns: u64, samples: &[i16]) {
        capture
            .ingest_bot(at_ns, pcm(samples), OUTPUT_RATE, 1)
            .unwrap();
    }

    fn output(capture: &Capture) -> Vec<i16> {
        let wav = capture.to_wav().unwrap().unwrap();
        let audio = pipecat_audio::read_pcm16_wav(&wav).unwrap();
        assert_eq!(audio.format.sample_rate(), OUTPUT_RATE);
        assert_eq!(audio.format.channels(), 1);
        assert_eq!(wav.len(), audio.pcm.len() + 44);
        pipecat_audio::decode_pcm16_le(audio.pcm).unwrap()
    }

    #[test]
    fn limits_are_positive_bounded_and_have_v2_defaults() {
        let default = TimelineRecordingLimits::default();
        assert_eq!(default.max_duration, Duration::from_secs(120));
        assert_eq!(default.max_raw_bytes, 8_388_608);
        assert_eq!(default.max_records, 4096);
        default.validate().unwrap();
        for max_duration in [Duration::ZERO, MAX_DURATION + Duration::from_nanos(1)] {
            assert!(Capture::new(TimelineRecordingLimits {
                max_duration,
                ..default
            })
            .is_err());
        }
        for max_raw_bytes in [0, MAX_RAW_BYTES + 1] {
            assert!(Capture::new(TimelineRecordingLimits {
                max_raw_bytes,
                ..default
            })
            .is_err());
        }
        for max_records in [0, MAX_RECORDS + 1] {
            assert!(Capture::new(TimelineRecordingLimits {
                max_records,
                ..default
            })
            .is_err());
        }
    }

    #[test]
    fn no_samples_and_uncommitted_sources_are_truly_empty() {
        let mut capture = capture();
        assert!(capture.to_wav().unwrap().is_none());
        capture.register_user(0, Arc::from(pcm(&[1, 2]))).unwrap();
        capture.close(99_000_000_000).unwrap();
        let run = capture.into_run(ScriptResult::new("empty", 0));
        assert!(!run.result.passed);
        assert!(run.to_wav().unwrap().is_none());
    }

    #[test]
    fn successful_source_prefix_excludes_unsent_suffix_and_wire_padding() {
        let mut capture = capture();
        let ticket = capture
            .register_user(17_000_000_000, Arc::from(pcm(&[10, 20, 30])))
            .unwrap();
        // A six-byte source could travel in a 1280-byte final wire packet. Only
        // the actual source prefix is committed; no packet bytes are accepted.
        capture.commit_user(ticket, 0, 4).unwrap();
        capture.close(17_000_000_001).unwrap();
        assert_eq!(output(&capture), [10, 20]);
        capture.commit_user(ticket, 4, 2).unwrap();
        assert_eq!(output(&capture), [10, 20, 30]);
    }

    #[test]
    fn source_receipts_are_atomic_contiguous_whole_samples() {
        let mut capture = capture();
        let ticket = capture
            .register_user(0, Arc::from(pcm(&[1, 2, 3])))
            .unwrap();
        for (offset, len) in [(2, 2), (0, 1), (0, 0), (0, 8), (usize::MAX, 2)] {
            assert!(capture.commit_user(ticket, offset, len).is_err());
            assert_eq!(capture.records[ticket.0].retained_bytes, 0);
        }
        assert!(capture.commit_user(UserTicket(100), 0, 2).is_err());
        bot(&mut capture, 0, &[9]);
        assert!(capture.commit_user(UserTicket(1), 0, 2).is_err());
        capture.commit_user(ticket, 0, 2).unwrap();
        assert!(capture.commit_user(ticket, 0, 2).is_err());
        assert_eq!(capture.records[ticket.0].retained_bytes, 2);
    }

    #[test]
    fn common_origin_omits_handshake_and_floors_later_side_alignment() {
        let mut capture = capture();
        user(&mut capture, 5_000_000_000, &[11]);
        bot(&mut capture, 5_100_062_499, &[22]);
        capture.close(6_000_000_000).unwrap();
        let samples = output(&capture);
        assert_eq!(samples.len(), 1601);
        assert_eq!(samples[0], 11);
        assert!(samples[1..1600].iter().all(|sample| *sample == 0));
        assert_eq!(samples[1600], 22);
    }

    #[test]
    fn ghost_origins_from_uncommitted_or_wholly_cropped_sources_disappear() {
        let mut capture = capture();
        capture.register_user(0, Arc::from(pcm(&[8]))).unwrap();
        bot(&mut capture, 1_000_000_000, &[9]);
        capture.interrupt_bot(999_999_999).unwrap();
        user(&mut capture, 2_000_000_000, &[7]);
        assert_eq!(output(&capture), [7]);
    }

    #[test]
    fn gap_threshold_is_exactly_greater_than_two_hundred_milliseconds() {
        for (extra_ns, silence) in [(0, 0), (1, 3200)] {
            let mut capture = capture();
            user(&mut capture, 0, &[1; 640]);
            user(&mut capture, 240_000_000 + extra_ns, &[2; 640]);
            let samples = output(&capture);
            assert_eq!(samples.len(), 1280 + silence);
            assert!(samples[..640].iter().all(|sample| *sample == 1));
            assert!(samples[640..640 + silence]
                .iter()
                .all(|sample| *sample == 0));
            assert!(samples[640 + silence..].iter().all(|sample| *sample == 2));
        }
    }

    #[test]
    fn small_jitter_advances_logical_cursor_without_adding_rendered_samples() {
        let mut capture = capture();
        user(&mut capture, 0, &[1]);
        user(&mut capture, 100_000_000, &[2]);
        // Relative to the second logical end this is exactly 200ms. Relative
        // to a wrongly reused rendered cursor it would incorrectly add a gap.
        user(&mut capture, 300_062_500, &[3]);
        assert_eq!(output(&capture), [1, 2, 3]);
        user(&mut capture, 600_125_000, &[4]);
        let samples = output(&capture);
        assert_eq!(samples.len(), 4804);
        assert_eq!(&samples[..3], &[1, 2, 3]);
        assert!(samples[3..4803].iter().all(|sample| *sample == 0));
        assert_eq!(samples[4803], 4);
    }

    #[test]
    fn arrival_covered_by_source_lead_adds_no_silence() {
        let mut capture = capture();
        user(&mut capture, 0, &[1; 16_000]);
        user(&mut capture, 900_000_000, &[2]);
        let samples = output(&capture);
        assert_eq!(samples.len(), 16_001);
        assert_eq!(samples[16_000], 2);
    }

    #[test]
    fn bot_virtual_playout_crops_across_fast_queued_chunks() {
        let mut capture = capture();
        bot(&mut capture, 0, &[1; 1600]);
        bot(&mut capture, 0, &[2; 1600]);
        bot(&mut capture, 0, &[3; 1600]);
        capture.close(150_000_000).unwrap();
        let samples = output(&capture);
        assert_eq!(samples.len(), 2400);
        assert!(samples[..1600].iter().all(|sample| *sample == 1));
        assert!(samples[1600..].iter().all(|sample| *sample == 2));
        assert_eq!(capture.records[2].retained_bytes, 0);
    }

    #[test]
    fn bot_crop_before_first_at_first_inside_sample_and_at_end() {
        for (cutoff, count) in [
            (999_999_999, 0),
            (1_000_000_000, 0),
            (1_000_062_499, 0),
            (1_000_062_500, 1),
            (1_000_125_000, 2),
            (2_000_000_000, 2),
        ] {
            let mut capture = capture();
            bot(&mut capture, 1_000_000_000, &[1, 2]);
            capture.close(cutoff).unwrap();
            if count == 0 {
                assert!(capture.to_wav().unwrap().is_none());
            } else {
                assert_eq!(output(&capture), [1, 2][..count]);
            }
        }
    }

    #[test]
    fn bot_crop_uses_rational_source_sample_time() {
        for (cutoff, bytes) in [(20_833, 0), (20_834, 2), (62_500, 6)] {
            let mut capture = capture();
            capture.ingest_bot(0, pcm(&[1, 2, 3]), 48_000, 1).unwrap();
            capture.close(cutoff).unwrap();
            assert_eq!(capture.records[0].retained_bytes, bytes);
        }
    }

    #[test]
    fn repeated_interruption_is_idempotent_and_new_bot_admission_uses_retained_end() {
        let mut capture = capture();
        bot(&mut capture, 0, &[1, 2, 3]);
        bot(&mut capture, 0, &[4, 5, 6]);
        capture.interrupt_bot(62_500).unwrap();
        capture.interrupt_bot(62_500).unwrap();
        assert_eq!(output(&capture), [1]);
        bot(&mut capture, 100_000, &[7, 8]);
        assert_eq!(capture.records[2].bot_start_scaled, 100_000 * 16_000);
        capture.close(1_000_000).unwrap();
        assert_eq!(output(&capture), [1, 7, 8]);
        assert_eq!(capture.records[0].retained_bytes, 2);
        assert_eq!(capture.records[1].retained_bytes, 0);
    }

    #[test]
    fn frozen_bot_cutoff_does_not_follow_join_time_or_trim_successful_user_prefix() {
        let mut capture = capture();
        let ticket = capture
            .register_user(0, Arc::from(pcm(&[10, 20, 30])))
            .unwrap();
        capture.commit_user(ticket, 0, 2).unwrap();
        bot(&mut capture, 0, &[1, 2, 3]);
        capture.close(62_500).unwrap();
        // Model an in-flight successful send receipt while an owner join waits.
        capture.commit_user(ticket, 2, 4).unwrap();
        capture.close(99_000_000_000).unwrap();
        capture.interrupt_bot(100_000_000_000).unwrap();
        assert_eq!(capture.closed_at, Some(62_500));
        assert_eq!(output(&capture), [11, 20, 30]);
        assert!(capture.register_user(70_000, Arc::from(pcm(&[4]))).is_err());
        assert!(capture
            .ingest_bot(70_000, pcm(&[4]), OUTPUT_RATE, 1)
            .is_err());
    }

    #[test]
    fn overlap_saturates_both_signs_and_keeps_longer_mono_side() {
        let mut capture = capture();
        user(&mut capture, 0, &[30_000, -30_000, 1, 2]);
        bot(&mut capture, 0, &[10_000, -10_000]);
        capture.close(1_000_000).unwrap();
        assert_eq!(output(&capture), [i16::MAX, i16::MIN, 1, 2]);
    }

    #[test]
    fn whole_side_resampling_matches_native_dsp_and_is_chunk_split_invariant() {
        let input: Vec<i16> = (0..96).map(|index| (index * 619 - 28_000) as i16).collect();
        let bytes = pcm(&input);
        for rate in [8_000, 16_000, 24_000, 48_000] {
            let expected = pipecat_audio::resample_pcm16(&bytes, rate, OUTPUT_RATE, 1).unwrap();
            let expected = pipecat_audio::decode_pcm16_le(&expected).unwrap();
            let mut whole = capture();
            whole.ingest_bot(0, bytes.clone(), rate, 1).unwrap();
            whole.close(1_000_000_000).unwrap();
            let mut split = capture();
            for chunk in bytes.chunks(14) {
                split.ingest_bot(0, chunk.to_vec(), rate, 1).unwrap();
            }
            split.close(1_000_000_000).unwrap();
            assert_eq!(output(&whole), expected, "whole rate={rate}");
            assert_eq!(output(&split), expected, "split rate={rate}");
            assert_eq!(expected.len() as u128, resampled_count(96, rate).unwrap());
        }
    }

    #[test]
    fn resampler_filter_tail_and_nearest_half_up_counts_are_included() {
        for (rate, frames, expected) in [(8_000, 1, 2), (24_000, 1, 1), (48_000, 2, 1)] {
            let mut capture = capture();
            capture
                .ingest_bot(0, pcm(&vec![321; frames]), rate, 1)
                .unwrap();
            capture.close(1_000_000_000).unwrap();
            assert_eq!(output(&capture), vec![321; expected]);
        }
        assert_eq!(resampled_count(1, 32_000).unwrap(), 1);
        assert_eq!(resampled_count(3, 32_000).unwrap(), 2);
        assert_eq!(resampled_count(1, 48_000).unwrap(), 0);
    }

    #[test]
    fn invalid_pcm_and_mid_side_rate_changes_are_rejected_before_mutation() {
        let mut capture = capture();
        for (audio, rate, channels) in [
            (vec![], 16_000, 1),
            (vec![1], 16_000, 1),
            (vec![1, 0], 0, 1),
            (vec![1, 0], 44_100, 1),
            (vec![1, 0], 16_000, 0),
            (vec![1, 0], 16_000, 2),
        ] {
            assert!(capture.ingest_bot(0, audio, rate, channels).is_err());
            assert!(capture.records.is_empty());
            assert_eq!(capture.bot_rate, None);
        }
        assert!(capture.register_user(0, Arc::from([1_u8])).is_err());
        capture.ingest_bot(0, pcm(&[1]), 8_000, 1).unwrap();
        capture.interrupt_bot(0).unwrap();
        assert!(capture.ingest_bot(1, pcm(&[2]), 16_000, 1).is_err());
        assert_eq!(capture.bot_rate, Some(8_000));
        assert_eq!(capture.raw_bytes, 2);
    }

    #[test]
    fn backwards_timestamps_reject_without_rewriting_admission_state() {
        let mut capture = capture();
        user(&mut capture, 10, &[1]);
        bot(&mut capture, 10, &[2]);
        assert!(capture.register_user(9, Arc::from(pcm(&[3]))).is_err());
        assert!(capture.ingest_bot(9, pcm(&[3]), OUTPUT_RATE, 1).is_err());
        capture.interrupt_bot(10).unwrap();
        assert!(capture.interrupt_bot(9).is_err());
        assert!(capture.ingest_bot(9, pcm(&[3]), OUTPUT_RATE, 1).is_err());
        assert_eq!(capture.records.len(), 2);
        assert_eq!(capture.raw_bytes, 4);
    }

    #[test]
    fn raw_quota_is_combined_cumulative_and_crop_does_not_refund_it() {
        let mut capture = Capture::new(TimelineRecordingLimits {
            max_raw_bytes: 6,
            ..TimelineRecordingLimits::default()
        })
        .unwrap();
        capture.register_user(0, Arc::from(pcm(&[1, 2]))).unwrap();
        bot(&mut capture, 0, &[3]);
        assert_eq!(capture.remaining_raw_bytes(), 0);
        capture.interrupt_bot(0).unwrap();
        assert!(capture.ingest_bot(0, pcm(&[4]), OUTPUT_RATE, 1).is_err());
        assert!(capture.register_user(0, Arc::from(pcm(&[4]))).is_err());
        assert_eq!(capture.raw_bytes, 6);
        assert_eq!(capture.records.len(), 2);
        assert!(capture.to_wav().unwrap().is_none());
    }

    #[test]
    fn full_raw_limit_accepts_exactly_maximum_without_qualifying_unsent_audio() {
        let mut capture = capture();
        let full: Arc<[u8]> = Arc::from(vec![0; MAX_RAW_BYTES]);
        capture.register_user(0, full).unwrap();
        assert_eq!(capture.remaining_raw_bytes(), 0);
        assert!(capture.ingest_bot(0, pcm(&[1]), OUTPUT_RATE, 1).is_err());
        assert!(capture.to_wav().unwrap().is_none());
    }

    #[test]
    fn record_quota_accepts_boundary_without_vector_growth_and_is_not_refunded() {
        let mut capture = capture();
        let capacity = capture.records.capacity();
        for _ in 0..MAX_RECORDS {
            bot(&mut capture, 0, &[1]);
        }
        assert_eq!(capture.remaining_records(), 0);
        assert_eq!(capture.records.capacity(), capacity);
        assert!(capacity * size_of::<Record>() <= METADATA_BYTES);
        capture.interrupt_bot(0).unwrap();
        assert!(capture.ingest_bot(0, pcm(&[1]), OUTPUT_RATE, 1).is_err());
        assert!(capture.to_wav().unwrap().is_none());
    }

    #[test]
    fn duration_includes_common_alignment_and_small_jitter_logical_span() {
        let limits = TimelineRecordingLimits {
            max_duration: Duration::from_millis(1),
            ..TimelineRecordingLimits::default()
        };
        let mut exact = Capture::new(limits).unwrap();
        user(&mut exact, 50_000_000_000, &[1; 16]);
        assert_eq!(output(&exact).len(), 16);
        let mut too_long = Capture::new(limits).unwrap();
        user(&mut too_long, 0, &[1; 17]);
        assert!(too_long.to_wav().is_err());
        let mut aligned = Capture::new(limits).unwrap();
        user(&mut aligned, 0, &[1]);
        bot(&mut aligned, 1_000_000, &[1]);
        assert!(aligned.to_wav().is_err());
        let mut jitter = Capture::new(limits).unwrap();
        user(&mut jitter, 0, &[1]);
        user(&mut jitter, 1_000_000, &[2]);
        assert!(jitter.to_wav().is_err());
    }

    #[test]
    fn full_duration_and_wav_bound_are_exact_and_next_frame_fails() {
        let mut capture = capture();
        let audio: Arc<[u8]> = Arc::from(vec![0; MAX_OUTPUT_BYTES]);
        let ticket = capture.register_user(0, audio).unwrap();
        capture.commit_user(ticket, 0, MAX_OUTPUT_BYTES).unwrap();
        let wav = capture.to_wav().unwrap().unwrap();
        assert_eq!(wav.len(), 3_840_044);
        assert_eq!(wav.capacity(), 3_840_044);
        drop(wav);
        bot(&mut capture, 120_000_000_000, &[1]);
        assert!(capture.to_wav().is_err());
    }

    #[test]
    fn high_rate_expansion_and_large_gap_are_preflighted_without_rendering() {
        let mut capture = capture();
        capture.ingest_bot(0, pcm(&[1; 48]), 48_000, 1).unwrap();
        capture
            .ingest_bot(119_999_000_000, pcm(&[2; 48]), 48_000, 1)
            .unwrap();
        let plan = capture.plan(Side::Bot, 0).unwrap().unwrap();
        assert_eq!(plan.layout_bytes, MAX_LAYOUT_BYTES);
        assert_eq!(plan.output_bytes, MAX_OUTPUT_BYTES);
        assert_eq!(capture.raw_bytes, 192);
        // One additional ns must reject the common logical duration before any
        // huge gap/layout allocation. This does not execute a 120s sinc filter.
        let mut beyond = Capture::new(TimelineRecordingLimits::default()).unwrap();
        beyond.ingest_bot(0, pcm(&[1; 48]), 48_000, 1).unwrap();
        beyond
            .ingest_bot(119_999_000_001, pcm(&[2; 48]), 48_000, 1)
            .unwrap();
        assert!(beyond.to_wav().is_err());
    }

    #[test]
    fn checked_arithmetic_rejects_overflow_without_wrap_or_partial_cursor_update() {
        assert!(advance(u128::MAX, 1).is_err());
        assert!(frame_bytes(u128::MAX).is_err());
        assert!(resampled_count(u128::MAX, 8_000).is_err());
        assert!(resampled_count(1, 0).is_err());
        assert!(checked_wav_bytes(usize::MAX).is_err());
        assert!(checked_wav_bytes(MAX_OUTPUT_BYTES + 2).is_err());
        assert!(checked_wav_bytes(1).is_err());
        assert!(growth_capacity(usize::MAX).is_err());
        assert!(render_envelope(usize::MAX, 1).is_err());
        assert!(check_capacity(usize::MAX, 2, usize::MAX).is_err());
        assert_eq!(
            scaled_time(u64::MAX, 48_000).unwrap(),
            u128::from(u64::MAX) * 48_000
        );
        let mut cursor = LayoutCursor {
            logical_scaled: u128::MAX,
            rendered_frames: 0,
            last_arrival: 0,
        };
        assert!(cursor.append(0, 1, OUTPUT_RATE).is_err());
        assert_eq!(cursor.rendered_frames, 0);
        let mut cursor = LayoutCursor {
            logical_scaled: 0,
            rendered_frames: u128::MAX,
            last_arrival: 0,
        };
        assert!(cursor.append(0, 1, OUTPUT_RATE).is_err());
        assert_eq!(cursor.logical_scaled, 0);
    }

    #[test]
    fn actual_exposed_capacities_and_conservative_peak_fit_the_reserved_envelope() {
        assert_eq!(
            render_envelope(MAX_LAYOUT_BYTES, MAX_OUTPUT_BYTES).unwrap(),
            121_845_760
        );
        assert_eq!(RENDER_BYTES - 121_845_760, 12_371_968);
        assert_eq!(exact_buffer(1234).unwrap().capacity(), 1234);
        assert!(check_capacity(9, 1, 8).is_err());
        for rate in [8_000, 16_000, 24_000, 48_000] {
            let mut capture = capture();
            user(&mut capture, 0, &[4]);
            capture
                .ingest_bot(10_000_000, pcm(&[3; 128]), rate, 1)
                .unwrap();
            let plan = capture.plan(Side::Bot, 0).unwrap().unwrap();
            let aligned = capture.render_side(Some(plan)).unwrap();
            assert_eq!(aligned.capacity(), plan.output_bytes);
            assert_eq!(aligned.len(), plan.output_bytes);
        }
    }

    #[test]
    fn oversized_inbound_spare_capacity_is_rejected_before_it_can_be_retained() {
        let mut capture = capture();
        let mut audio = Vec::with_capacity(MAX_RAW_BYTES + 1);
        audio.extend_from_slice(&[1, 0]);
        assert!(capture.ingest_bot(0, audio, OUTPUT_RATE, 1).is_err());
        assert_eq!(capture.raw_bytes, 0);
        assert!(capture.records.is_empty());
    }

    #[test]
    fn exact_bot_raw_limit_accepts_exact_decoder_buffer_and_stores_exact_box() {
        let mut capture = capture();
        let audio = vec![0; MAX_RAW_BYTES];
        capture.ingest_bot(0, audio, 48_000, 1).unwrap();
        assert_eq!(capture.remaining_raw_bytes(), 0);
        assert_eq!(capture.records[0].source.bytes().len(), MAX_RAW_BYTES);
        assert!(capture.ingest_bot(0, pcm(&[1]), 48_000, 1).is_err());
    }
}
