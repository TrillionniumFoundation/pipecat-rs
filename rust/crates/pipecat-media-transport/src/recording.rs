//! Synchronized, bounded user/bot PCM recording as a native frame processor.
//!
//! Events use a bounded broadcast channel: slow consumers receive an explicit
//! `Lagged` error instead of making the pipeline wait. Subscribe before recording.
//! The `turn-recording` feature supports explicit attachment to one native turn
//! tracker. Numbered events use sealed local-frame boundaries, never timestamps
//! or playback receipts. Manual integrations can use `emit_turn_audio` instead.

#[cfg(feature = "turn-recording")]
mod turns;
#[cfg(feature = "turn-recording")]
pub use turns::{TurnRecordingLoss, TurnRecordingStatus};

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use pipecat_audio::{interleave_stereo_audio, mix_audio, PcmFormat, SincResampler};
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    StartFrame,
};
use tokio::sync::broadcast;

pub trait RecordingClock: Send + Sync {
    fn now(&self) -> Duration;
}

pub struct SystemRecordingClock(Instant);
impl Default for SystemRecordingClock {
    fn default() -> Self {
        Self(Instant::now())
    }
}
impl RecordingClock for SystemRecordingClock {
    fn now(&self) -> Duration {
        self.0.elapsed()
    }
}

#[derive(Debug, Clone)]
pub struct AudioBufferConfig {
    pub sample_rate: Option<u32>,
    /// One mixes both speakers; two puts user on the left and bot on the right.
    pub num_channels: u16,
    /// Per-track byte threshold for emitting audio; zero waits for explicit flush.
    pub buffer_size: usize,
    pub enable_turn_audio: bool,
    pub auto_start_recording: bool,
    /// Hard limit for each primary track, including inserted silence.
    pub max_track_bytes: usize,
    /// Hard limit for each speaker's accumulated turn, including its current run.
    pub max_turn_bytes: usize,
    pub event_capacity: usize,
    pub gap_threshold: Duration,
}
impl Default for AudioBufferConfig {
    fn default() -> Self {
        Self {
            sample_rate: None,
            num_channels: 1,
            buffer_size: 0,
            enable_turn_audio: false,
            auto_start_recording: false,
            max_track_bytes: 8 * 1024 * 1024,
            max_turn_bytes: 8 * 1024 * 1024,
            event_capacity: 32,
            gap_threshold: Duration::from_millis(200),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnAudioData {
    pub turn_number: u64,
    /// Process-local numbered session identity; manual emissions use None.
    pub session_id: Option<u64>,
    pub audio: Arc<[u8]>,
    pub sample_rate: u32,
    pub num_channels: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingEvent {
    RecordingStarted,
    AudioData {
        audio: Arc<[u8]>,
        sample_rate: u32,
        num_channels: u16,
    },
    TrackAudioData {
        user: Arc<[u8]>,
        bot: Arc<[u8]>,
        sample_rate: u32,
        num_channels: u16,
    },
    UserRunAudio {
        audio: Arc<[u8]>,
        sample_rate: u32,
    },
    BotRunAudio {
        audio: Arc<[u8]>,
        sample_rate: u32,
    },
    UserTurnAudio(TurnAudioData),
    BotTurnAudio(TurnAudioData),
    #[cfg(feature = "turn-recording")]
    TurnAttributionLost(TurnRecordingLoss),
    RecordingStopped,
}

#[derive(Default)]
struct Track {
    primary: Vec<u8>,
    run: Vec<u8>,
    turn: Vec<u8>,
    speaking: bool,
    last_update: Option<Duration>,
    resampler: Option<SincResampler>,
    resampler_input_frames: u64,
    resampler_output_frames: u64,
    pending_tail_bytes: usize,
}
struct RecordingState {
    sample_rate: u32,
    recording: bool,
    user: Track,
    bot: Track,
    #[cfg(feature = "turn-recording")]
    automatic: Option<turns::AutomaticTurns>,
}

pub struct AudioBufferProcessor {
    config: AudioBufferConfig,
    state: Mutex<RecordingState>,
    clock: Arc<dyn RecordingClock>,
    events: broadcast::Sender<RecordingEvent>,
    #[cfg(feature = "turn-recording")]
    subscription: Mutex<Option<turns::OwnedSubscription>>,
}

fn audio_error(error: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::new(error.to_string())
}

impl AudioBufferProcessor {
    pub fn new(config: AudioBufferConfig) -> Result<Self, ProcessorError> {
        Self::with_clock(config, Arc::new(SystemRecordingClock::default()))
    }

    pub fn with_clock(
        config: AudioBufferConfig,
        clock: Arc<dyn RecordingClock>,
    ) -> Result<Self, ProcessorError> {
        if !matches!(config.num_channels, 1 | 2) {
            return Err(ProcessorError::new(
                "recording output must have one or two channels",
            ));
        }
        if config.sample_rate == Some(0) {
            return Err(ProcessorError::new(
                "recording sample rate must be positive",
            ));
        }
        if config.event_capacity == 0 || config.event_capacity > 65536 {
            return Err(ProcessorError::new(
                "recording event capacity must be within 1..=65536",
            ));
        }
        if config.max_track_bytes < 2
            || config.max_turn_bytes < 2
            || !config.max_track_bytes.is_multiple_of(2)
            || !config.max_turn_bytes.is_multiple_of(2)
            || !config.buffer_size.is_multiple_of(2)
            || config.buffer_size > config.max_track_bytes
        {
            return Err(ProcessorError::new("recording buffer limits must be PCM16 aligned and the flush threshold must fit the track limit"));
        }
        let (events, _) = broadcast::channel(config.event_capacity);
        Ok(Self {
            state: Mutex::new(RecordingState {
                sample_rate: config.sample_rate.unwrap_or(0),
                recording: false,
                user: Track::default(),
                bot: Track::default(),
                #[cfg(feature = "turn-recording")]
                automatic: None,
            }),
            config,
            clock,
            events,
            #[cfg(feature = "turn-recording")]
            subscription: Mutex::new(None),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, RecordingState>, ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ProcessorError::fatal("recording state lock poisoned"))
    }

    /// Events are ordered, nonblocking, and bounded. Receivers must handle lag.
    pub fn subscribe(&self) -> broadcast::Receiver<RecordingEvent> {
        self.events.subscribe()
    }

    pub fn sample_rate(&self) -> Result<u32, ProcessorError> {
        Ok(self.lock()?.sample_rate)
    }
    pub fn num_channels(&self) -> u16 {
        self.config.num_channels
    }
    pub fn is_recording(&self) -> Result<bool, ProcessorError> {
        Ok(self.lock()?.recording)
    }
    pub fn has_audio(&self) -> Result<bool, ProcessorError> {
        let state = self.lock()?;
        Ok(!state.user.primary.is_empty() || !state.bot.primary.is_empty())
    }
    pub fn buffered_bytes(&self) -> Result<(usize, usize), ProcessorError> {
        let state = self.lock()?;
        Ok((state.user.primary.len(), state.bot.primary.len()))
    }
    /// Whether this build supports explicit native tracker attachment.
    pub fn automatic_turn_tracking_supported(&self) -> bool {
        cfg!(feature = "turn-recording")
    }

    pub fn merge_audio_buffers(&self) -> Result<Vec<u8>, ProcessorError> {
        let state = self.lock()?;
        self.merge(&state.user.primary, &state.bot.primary)
    }

    fn emit(&self, event: RecordingEvent) {
        // An absent receiver is allowed: recording can be used without event listeners.
        let _ = self.events.send(event);
    }

    fn merge(&self, user: &[u8], bot: &[u8]) -> Result<Vec<u8>, ProcessorError> {
        if self.config.num_channels == 1 {
            mix_audio(user, bot).map_err(audio_error)
        } else {
            let length = user.len().max(bot.len());
            let mut left = user.to_vec();
            let mut right = bot.to_vec();
            left.resize(length, 0);
            right.resize(length, 0);
            interleave_stereo_audio(&left, &right).map_err(audio_error)
        }
    }

    pub fn start_recording(&self) -> Result<(), ProcessorError> {
        self.start_locked(&mut *self.lock()?)
    }
    fn start_locked(&self, state: &mut RecordingState) -> Result<(), ProcessorError> {
        if state.recording {
            return Ok(());
        }
        if state.sample_rate == 0 {
            return Err(ProcessorError::new(
                "recording requires setup or an explicit sample rate",
            ));
        }
        #[cfg(feature = "turn-recording")]
        self.recording_restarted(state);
        state.user = Track::default();
        state.bot = Track::default();
        state.recording = true;
        self.emit(RecordingEvent::RecordingStarted);
        Ok(())
    }

    /// Flush complete primary audio without stopping or discarding resampler history.
    pub fn flush(&self) -> Result<(), ProcessorError> {
        self.flush_locked(&mut *self.lock()?, self.clock.now())
    }
    fn flush_locked(
        &self,
        state: &mut RecordingState,
        now: Duration,
    ) -> Result<(), ProcessorError> {
        if !state.recording || (state.user.primary.is_empty() && state.bot.primary.is_empty()) {
            return Ok(());
        }
        let length = state.user.primary.len().max(state.bot.primary.len());
        state.user.primary.resize(length, 0);
        state.bot.primary.resize(length, 0);
        let merged = self.merge(&state.user.primary, &state.bot.primary)?;
        self.emit(RecordingEvent::AudioData {
            audio: merged.into(),
            sample_rate: state.sample_rate,
            num_channels: self.config.num_channels,
        });
        self.emit(RecordingEvent::TrackAudioData {
            user: std::mem::take(&mut state.user.primary).into(),
            bot: std::mem::take(&mut state.bot.primary).into(),
            sample_rate: state.sample_rate,
            num_channels: self.config.num_channels,
        });
        state.user.last_update = Some(now);
        state.bot.last_update = Some(now);
        Ok(())
    }

    pub fn stop_recording(&self) -> Result<(), ProcessorError> {
        self.stop_locked(&mut *self.lock()?, self.clock.now())
    }
    fn stop_locked(&self, state: &mut RecordingState, now: Duration) -> Result<(), ProcessorError> {
        if !state.recording {
            return Ok(());
        }
        self.drain_tail(state, true, now)?;
        self.drain_tail(state, false, now)?;
        self.flush_locked(state, now)?;
        if self.config.enable_turn_audio {
            Self::finish_run(&mut state.user);
            Self::finish_run(&mut state.bot);
        }
        state.recording = false;
        state.user.speaking = false;
        state.bot.speaking = false;
        state.user.last_update = None;
        state.bot.last_update = None;
        state.user.resampler = None;
        state.bot.resampler = None;
        self.emit(RecordingEvent::RecordingStopped);
        Ok(())
    }

    fn gap_bytes(
        &self,
        track: &Track,
        now: Duration,
        bytes: usize,
        rate: u32,
    ) -> Result<usize, ProcessorError> {
        let Some(last) = track.last_update else {
            return Ok(0);
        };
        let elapsed = now.saturating_sub(last).as_secs_f64();
        let gap = elapsed - bytes as f64 / (f64::from(rate) * 2.0);
        if gap <= self.config.gap_threshold.as_secs_f64() {
            return Ok(0);
        }
        let samples = (gap * f64::from(rate)).floor();
        if samples > (self.config.max_track_bytes / 2) as f64 {
            return Err(ProcessorError::new(
                "recording silence gap exceeds the track byte limit",
            ));
        }
        Ok(samples as usize * 2)
    }

    fn validate_append(
        &self,
        track: &Track,
        size: usize,
        gap: usize,
        rate: u32,
        user: bool,
        turn_audio: bool,
    ) -> Result<(), ProcessorError> {
        let length = track
            .primary
            .len()
            .checked_add(gap)
            .and_then(|length| length.checked_add(size));
        if length.is_none_or(|length| length > self.config.max_track_bytes) {
            return Err(ProcessorError::new(
                "recording track byte limit exceeded; flush more frequently",
            ));
        }
        if turn_audio && !self.turn_append_fits(track, size, rate, user) {
            return Err(ProcessorError::new(
                "recording turn byte limit exceeded; emit the completed turn",
            ));
        }
        Ok(())
    }

    fn turn_append_fits(&self, track: &Track, size: usize, rate: u32, user: bool) -> bool {
        if !user && !track.speaking {
            return true;
        }
        let run_length = track.run.len().saturating_add(size);
        let retained = if user && !track.speaking {
            run_length
                .min((rate as usize).saturating_mul(2))
                .min(self.config.max_turn_bytes.saturating_sub(track.turn.len()))
        } else {
            run_length
        };
        retained.saturating_add(track.turn.len()) <= self.config.max_turn_bytes
    }

    fn append(
        &self,
        state: &mut RecordingState,
        user: bool,
        audio: &[u8],
        now: Duration,
        gap: usize,
        sync_peer: bool,
    ) -> Result<(), ProcessorError> {
        if audio.is_empty() {
            return Ok(());
        }
        let turn_audio = self.capture_turn_audio(state);
        let (track, peer) = if user {
            (&mut state.user, &mut state.bot)
        } else {
            (&mut state.bot, &mut state.user)
        };
        self.validate_append(track, audio.len(), gap, state.sample_rate, user, turn_audio)?;
        if sync_peer
            && !peer.speaking
            && track
                .primary
                .len()
                .saturating_add(gap)
                .saturating_add(peer.pending_tail_bytes)
                > self.config.max_track_bytes
        {
            return Err(ProcessorError::new(
                "synchronizing recording tracks would exceed the pending audio limit",
            ));
        }
        track.primary.resize(track.primary.len() + gap, 0);
        if sync_peer && !peer.speaking {
            if peer.primary.len() < track.primary.len() {
                peer.primary.resize(track.primary.len(), 0);
            }
            peer.last_update = Some(now);
        }
        track.primary.extend_from_slice(audio);
        track.last_update = Some(now);
        if turn_audio && (user || track.speaking) {
            if user && !track.speaking {
                let keep = (state.sample_rate as usize)
                    .saturating_mul(2)
                    .min(self.config.max_turn_bytes.saturating_sub(track.turn.len()));
                if audio.len() >= keep {
                    track.run.clear();
                    track.run.extend_from_slice(&audio[audio.len() - keep..]);
                } else {
                    let discard = track
                        .run
                        .len()
                        .saturating_add(audio.len())
                        .saturating_sub(keep);
                    track.run.drain(..discard);
                    track.run.extend_from_slice(audio);
                }
            } else {
                track.run.extend_from_slice(audio);
            }
        }
        Ok(())
    }

    fn record_audio(
        &self,
        state: &mut RecordingState,
        user: bool,
        frame: &AudioRawFrame,
        now: Duration,
    ) -> Result<(), ProcessorError> {
        let format = PcmFormat::new(frame.sample_rate, frame.num_channels).map_err(audio_error)?;
        format.validate(&frame.audio).map_err(audio_error)?;
        if format.channels() != 1 {
            return Err(ProcessorError::new("recording input tracks must be mono"));
        }
        if frame.audio.is_empty() {
            return Ok(());
        }
        if frame.audio.len() > self.config.max_track_bytes {
            return Err(ProcessorError::new(
                "recording input frame exceeds the track byte limit",
            ));
        }
        let projected = ((frame.audio.len() / 2) as u128 * u128::from(state.sample_rate))
            .div_ceil(u128::from(frame.sample_rate))
            * 2;
        if projected > self.config.max_track_bytes as u128 {
            return Err(ProcessorError::new(
                "resampled frame exceeds the track byte limit",
            ));
        }
        let track = if user { &state.user } else { &state.bot };
        let mut resampler = track.resampler.clone();
        let mut input_frames = track.resampler_input_frames;
        let mut output_frames = track.resampler_output_frames;
        let mut audio = Vec::new();
        if resampler
            .as_ref()
            .is_some_and(|value| value.input_format() != format)
        {
            audio.extend(resampler.as_mut().unwrap().flush().map_err(audio_error)?);
            resampler = None;
            input_frames = 0;
            output_frames = 0;
        }
        let mut resampler = match resampler {
            Some(value) => value,
            None => {
                SincResampler::new(frame.sample_rate, state.sample_rate, 1).map_err(audio_error)?
            }
        };
        let produced = resampler.process(&frame.audio).map_err(audio_error)?;
        input_frames = input_frames
            .checked_add((frame.audio.len() / 2) as u64)
            .ok_or_else(|| ProcessorError::new("recording sample count overflow"))?;
        output_frames = output_frames
            .checked_add((produced.len() / 2) as u64)
            .ok_or_else(|| ProcessorError::new("recording sample count overflow"))?;
        let expected_frames = (u128::from(input_frames) * u128::from(state.sample_rate)
            + u128::from(frame.sample_rate) / 2)
            / u128::from(frame.sample_rate);
        let pending_tail_bytes =
            usize::try_from(expected_frames.saturating_sub(u128::from(output_frames)) * 2)
                .map_err(|_| ProcessorError::new("recording resampler tail overflow"))?;
        audio.extend(produced);
        let gap = if audio.is_empty() {
            0
        } else {
            self.gap_bytes(track, now, audio.len(), state.sample_rate)?
        };
        #[cfg(feature = "turn-recording")]
        self.enforce_turn_capacity(state, user, audio.len().saturating_add(pending_tail_bytes));
        let track = if user { &state.user } else { &state.bot };
        // Reserve the eventual filter tail so every accepted frame can be flushed.
        self.validate_append(
            track,
            audio.len().saturating_add(pending_tail_bytes),
            gap,
            state.sample_rate,
            user,
            self.capture_turn_audio(state),
        )?;
        self.append(state, user, &audio, now, gap, true)?;
        let track = if user {
            &mut state.user
        } else {
            &mut state.bot
        };
        track.resampler = Some(resampler);
        track.resampler_input_frames = input_frames;
        track.resampler_output_frames = output_frames;
        track.pending_tail_bytes = pending_tail_bytes;
        if self.config.buffer_size > 0
            && (state.user.primary.len() >= self.config.buffer_size
                || state.bot.primary.len() >= self.config.buffer_size)
        {
            self.flush_locked(state, now)?;
        }
        Ok(())
    }

    fn drain_tail(
        &self,
        state: &mut RecordingState,
        user: bool,
        now: Duration,
    ) -> Result<(), ProcessorError> {
        let track = if user { &state.user } else { &state.bot };
        let Some(mut resampler) = track.resampler.clone() else {
            return Ok(());
        };
        let tail = resampler.flush().map_err(audio_error)?;
        self.append(state, user, &tail, now, 0, false)?;
        let track = if user {
            &mut state.user
        } else {
            &mut state.bot
        };
        track.resampler = Some(resampler);
        track.resampler_input_frames = 0;
        track.resampler_output_frames = 0;
        track.pending_tail_bytes = 0;
        Ok(())
    }

    fn finish_run(track: &mut Track) {
        track.turn.append(&mut track.run);
    }

    fn capture_turn_audio(&self, state: &RecordingState) -> bool {
        #[cfg(feature = "turn-recording")]
        if state.automatic.as_ref().is_some_and(|a| !a.enabled()) {
            return false;
        }
        #[cfg(not(feature = "turn-recording"))]
        let _ = state;
        self.config.enable_turn_audio
    }

    /// Emit completed runs for a caller-supplied turn. An open user run is kept
    /// for the next turn so interruption speech is not assigned to the old turn.
    pub fn emit_turn_audio(&self, turn_number: u64) -> Result<(), ProcessorError> {
        if !self.config.enable_turn_audio {
            return Err(ProcessorError::new("turn audio is disabled"));
        }
        let mut state = self.lock()?;
        #[cfg(feature = "turn-recording")]
        if state.automatic.is_some() {
            return Err(ProcessorError::new(
                "manual turn emission cannot be combined with a tracker attachment",
            ));
        }
        self.drain_tail(&mut state, false, self.clock.now())?;
        Self::finish_run(&mut state.bot);
        let rate = state.sample_rate;
        if !state.user.turn.is_empty() {
            self.emit(RecordingEvent::UserTurnAudio(TurnAudioData {
                turn_number,
                session_id: None,
                audio: std::mem::take(&mut state.user.turn).into(),
                sample_rate: rate,
                num_channels: 1,
            }));
        }
        if !state.bot.turn.is_empty() {
            self.emit(RecordingEvent::BotTurnAudio(TurnAudioData {
                turn_number,
                session_id: None,
                audio: std::mem::take(&mut state.bot.turn).into(),
                sample_rate: rate,
                num_channels: 1,
            }));
        }
        Ok(())
    }

    /// Process recording controls/audio without routing. Pipeline integrations
    /// should install this object as a [`FrameProcessor`] to preserve forwarding.
    pub fn handle_frame(&self, frame: &FrameData) -> Result<(), ProcessorError> {
        let now = self.clock.now();
        let mut state = self.lock()?;
        #[cfg(feature = "turn-recording")]
        if state.automatic.is_some() {
            return Err(ProcessorError::new(
                "attached turn recording requires native Frame processing",
            ));
        }
        self.handle_locked(&mut state, frame, now)
    }

    fn handle_locked(
        &self,
        state: &mut RecordingState,
        frame: &FrameData,
        now: Duration,
    ) -> Result<(), ProcessorError> {
        match frame {
            FrameData::Start(start)
            | FrameData::StartDetailed(pipecat_core::DetailedStartFrame {
                configuration: start,
                ..
            }) => {
                let sample_rate = self
                    .config
                    .sample_rate
                    .unwrap_or(start.audio_out_sample_rate);
                if sample_rate == 0 {
                    return Err(ProcessorError::new(
                        "recording sample rate must be positive",
                    ));
                }
                if state.recording && state.sample_rate != sample_rate {
                    return Err(ProcessorError::new(
                        "cannot change the recording sample rate while active",
                    ));
                }
                state.sample_rate = sample_rate;
                if self.config.auto_start_recording {
                    self.start_locked(state)?;
                }
            }
            FrameData::AudioBufferStartRecording => self.start_locked(state)?,
            FrameData::AudioBufferStopRecording
            | FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_)
            | FrameData::Stop => self.stop_locked(state, now)?,
            _ if !state.recording => {}
            FrameData::InputAudio(audio) => self.record_audio(state, true, audio, now)?,
            FrameData::UserAudio(audio) => self.record_audio(state, true, &audio.audio, now)?,
            FrameData::OutputAudio(audio)
            | FrameData::TtsAudio(audio)
            | FrameData::SpeechOutputAudio(audio) => self.record_audio(state, false, audio, now)?,
            FrameData::UserStartedSpeaking => {
                if !state.user.speaking {
                    self.drain_tail(state, true, now)?;
                    state.user.speaking = true;
                }
            }
            FrameData::BotStartedSpeaking => {
                if !state.bot.speaking {
                    self.drain_tail(state, false, now)?;
                    state.bot.speaking = true;
                }
            }
            FrameData::UserStoppedSpeaking | FrameData::BotStoppedSpeaking => {
                let user = matches!(frame, FrameData::UserStoppedSpeaking);
                self.drain_tail(state, user, now)?;
                let rate = state.sample_rate;
                let track = if user {
                    &mut state.user
                } else {
                    &mut state.bot
                };
                track.speaking = false;
                if self.config.enable_turn_audio {
                    let audio: Arc<[u8]> = track.run.clone().into();
                    self.emit(if user {
                        RecordingEvent::UserRunAudio {
                            audio,
                            sample_rate: rate,
                        }
                    } else {
                        RecordingEvent::BotRunAudio {
                            audio,
                            sample_rate: rate,
                        }
                    });
                    Self::finish_run(track);
                }
            }
            _ => {}
        }
        #[cfg(feature = "turn-recording")]
        self.discard_disabled_turn_audio(state);
        Ok(())
    }
}

#[async_trait]
impl FrameProcessor for AudioBufferProcessor {
    fn name(&self) -> &str {
        "AudioBufferProcessor"
    }

    async fn setup(
        &self,
        config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let sample_rate = self
            .config
            .sample_rate
            .unwrap_or(config.audio_out_sample_rate);
        PcmFormat::new(sample_rate, 1).map_err(audio_error)?;
        let mut state = self.lock()?;
        if state.recording && state.sample_rate != sample_rate {
            return Err(ProcessorError::new(
                "cannot change the recording sample rate while active",
            ));
        }
        state.sample_rate = sample_rate;
        Ok(())
    }

    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        #[cfg(feature = "turn-recording")]
        let result = self.handle_native_frame(&frame);
        #[cfg(not(feature = "turn-recording"))]
        let result = self.handle_frame(&frame.data);
        ctx.push(frame, direction).await?;
        result
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let result = self.stop_recording();
        #[cfg(feature = "turn-recording")]
        self.close_turn_subscription().await;
        result
    }
}
