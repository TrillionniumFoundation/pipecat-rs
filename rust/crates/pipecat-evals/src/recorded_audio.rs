//! Opt-in bounded recording playback for compatible RTVI evaluation endpoints.
use crate::{Cancellation, EvalError, SessionOptions};
use pipecat_protocol::rtvi::{ClientCommand, Envelope, RawAudioData};
use pipecat_transports::{Packet, SessionSender};
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::Instant,
};

const MAX_CLIP_BYTES: usize = 1_920_000; // 60 seconds, 16 kHz mono PCM16.
pub(crate) const MAX_TOTAL_BYTES: usize = 8 * MAX_CLIP_BYTES;
const CHUNK_BYTES: usize = 1280;
const PERIOD: Duration = Duration::from_millis(40);

/// A prevalidated 16 kHz mono PCM16 recording, at most 60 seconds.
/// Loading file bytes is the caller's responsibility; no paths or URLs are opened.
#[derive(Clone, Debug)]
pub struct RecordedAudio {
    pcm: Arc<[u8]>,
}
impl RecordedAudio {
    pub fn from_pcm16(pcm: &[u8]) -> Result<Self, EvalError> {
        if pcm.is_empty() || pcm.len() > MAX_CLIP_BYTES || !pcm.len().is_multiple_of(2) {
            return Err(EvalError::Scenario(
                "recording requires 1..960000 complete mono PCM16 samples at 16000 Hz".into(),
            ));
        }
        Ok(Self {
            pcm: Arc::from(pcm),
        })
    }
    /// Strict ordinary PCM16 RIFF/WAVE subset. Stereo/compressed/other rates
    /// are rejected rather than silently resampled or interpreted as mono.
    pub fn from_wav(bytes: &[u8]) -> Result<Self, EvalError> {
        if bytes.len() > MAX_CLIP_BYTES + 65536 {
            return Err(EvalError::Limit("recording WAV bytes"));
        }
        let wav = pipecat_audio::read_pcm16_wav(bytes)
            .map_err(|e| EvalError::Scenario(format!("recording WAV: {e}")))?;
        if wav.format.sample_rate() != 16000 || wav.format.channels() != 1 {
            return Err(EvalError::Unsupported(
                "recording WAV must be 16000 Hz mono PCM16".into(),
            ));
        }
        Self::from_pcm16(wav.pcm)
    }
    pub fn pcm_bytes(&self) -> &[u8] {
        &self.pcm
    }
}

pub(crate) struct Playback {
    commands: mpsc::Sender<(RecordedAudio, oneshot::Sender<()>)>,
    stop: Cancellation,
    health: watch::Receiver<Option<String>>,
    task: Option<JoinHandle<Result<(), EvalError>>>,
}
impl Drop for Playback {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl Playback {
    pub fn start(sender: SessionSender, options: &SessionOptions) -> Self {
        let (commands, rx) = mpsc::channel(1);
        let (tx, health) = watch::channel(None);
        let stop = Cancellation::new();
        let task_stop = stop.clone();
        let options = options.clone();
        let task = tokio::spawn(async move {
            let result = tokio::select! {
                biased;
                _ = task_stop.cancelled() => Ok(()),
                result = stream(sender, rx, &options) => result,
            };
            if let Err(error) = &result {
                tx.send_replace(Some(error.to_string()));
            }
            result
        });
        Self {
            commands,
            stop,
            health,
            task: Some(task),
        }
    }
    pub fn queue(&self, clip: RecordedAudio) -> Result<oneshot::Receiver<()>, EvalError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .try_send((clip, tx))
            .map_err(|_| EvalError::Limit("recording playback queue"))?;
        Ok(rx)
    }
    pub fn health(&self) -> watch::Receiver<Option<String>> {
        self.health.clone()
    }
    pub async fn finish(&mut self) -> Result<(), EvalError> {
        self.stop.cancel();
        // Borrow the handle across await. Cancellation cannot detach this owner.
        let result = match self.task.as_mut() {
            Some(task) => task
                .await
                .map_err(|e| EvalError::Scenario(format!("recording task: {e}")))?,
            None => return Ok(()),
        };
        self.task.take();
        result
    }
}

async fn stream(
    sender: SessionSender,
    mut commands: mpsc::Receiver<(RecordedAudio, oneshot::Sender<()>)>,
    options: &SessionOptions,
) -> Result<(), EvalError> {
    let mut current: Option<(RecordedAudio, usize, oneshot::Sender<()>)> = None;
    let mut next = Instant::now();
    let mut count = 0usize;
    let mut bytes = 0usize;
    loop {
        tokio::time::sleep_until(next).await;
        let tick = Instant::now();
        // Missed ticks never burst-catch up. Preserve source 40 ms stream pacing.
        next = tick + PERIOD;
        if current.is_none() {
            current = commands.try_recv().ok().map(|(clip, done)| (clip, 0, done));
        }
        let mut chunk = [0u8; CHUNK_BYTES];
        if let Some((clip, offset, _)) = &mut current {
            let size = CHUNK_BYTES.min(clip.pcm.len() - *offset);
            chunk[..size].copy_from_slice(&clip.pcm[*offset..*offset + size]);
            *offset += size;
        }
        count = count
            .checked_add(1)
            .ok_or(EvalError::Limit("recording wire messages"))?;
        let wire = Envelope::client(
            format!("audio-{count}"),
            &ClientCommand::RawAudio(RawAudioData::from_pcm16(&chunk, 16000, 1)?),
        )?
        .encode()?;
        if count > options.max_wire_messages
            || wire.len() > options.max_wire_bytes.saturating_sub(bytes)
            || wire.len() > options.transport.max_message_bytes
        {
            return Err(EvalError::Limit("recording wire traffic"));
        }
        bytes += wire.len();
        tokio::time::timeout(
            options.transport.write_timeout,
            sender.send(Packet::Text(wire)),
        )
        .await
        .map_err(|_| EvalError::Timeout("recording send"))??;
        if current
            .as_ref()
            .is_some_and(|(clip, offset, _)| *offset == clip.pcm.len())
        {
            if let Some((_, _, done)) = current.take() {
                let _ = done.send(());
            }
        }
    }
}
