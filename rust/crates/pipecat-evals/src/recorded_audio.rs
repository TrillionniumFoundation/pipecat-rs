//! Opt-in bounded recording playback for compatible RTVI evaluation endpoints.
use crate::timeline_recording::{Capture as TimelineCapture, UserTicket};
use crate::{Cancellation, EvalError, SessionOptions};
use pipecat_protocol::rtvi::{ClientCommand, Envelope, RawAudioData};
use pipecat_transports::{Packet, SessionSender};
use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
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

/// The only clock and shared ledger used by the session and its existing player.
/// A lock is always short-lived and is never retained across an await.
#[derive(Clone)]
pub(crate) struct TimelineRecorder {
    pub origin: Instant,
    pub capture: Arc<Mutex<TimelineCapture>>,
}
impl TimelineRecorder {
    pub fn elapsed_ns(&self) -> Result<u64, EvalError> {
        let elapsed = Instant::now()
            .checked_duration_since(self.origin)
            .ok_or_else(|| EvalError::Scenario("timeline clock moved backwards".into()))?;
        u64::try_from(elapsed.as_nanos()).map_err(|_| EvalError::Limit("timeline clock"))
    }
    pub fn lock(&self) -> Result<MutexGuard<'_, TimelineCapture>, EvalError> {
        self.capture
            .lock()
            .map_err(|_| EvalError::Scenario("timeline recording ledger poisoned".into()))
    }
}

struct QueuedClip {
    clip: RecordedAudio,
    completion: Completion,
    ticket: Option<UserTicket>,
}

enum Completion {
    Ordinary(oneshot::Sender<()>),
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    Timed(oneshot::Sender<Instant>),
}
impl Completion {
    fn send(self) {
        match self {
            Self::Ordinary(done) => {
                let _ = done.send(());
            }
            #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
            Self::Timed(done) => {
                let _ = done.send(Instant::now());
            }
        }
    }
}
pub(crate) struct Playback {
    commands: mpsc::Sender<QueuedClip>,
    timeline: Option<TimelineRecorder>,
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
        Self::start_with_timeline(sender, options, None)
    }
    pub(crate) fn start_with_timeline(
        sender: SessionSender,
        options: &SessionOptions,
        timeline: Option<TimelineRecorder>,
    ) -> Self {
        let (commands, rx) = mpsc::channel(1);
        let (tx, health) = watch::channel(None);
        let stop = Cancellation::new();
        let task_stop = stop.clone();
        let options = options.clone();
        let task_timeline = timeline.clone();
        let task = tokio::spawn(async move {
            let result = tokio::select! {
                biased;
                _ = task_stop.cancelled() => Ok(()),
                result = stream(sender, rx, &options, task_timeline.as_ref()) => result,
            };
            if let Err(error) = &result {
                tx.send_replace(Some(error.to_string()));
            }
            result
        });
        Self {
            commands,
            timeline,
            stop,
            health,
            task: Some(task),
        }
    }
    pub fn queue(&self, clip: RecordedAudio) -> Result<oneshot::Receiver<()>, EvalError> {
        let (tx, rx) = oneshot::channel();
        self.queue_completion(clip, Completion::Ordinary(tx))?;
        Ok(rx)
    }
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    pub(crate) fn queue_timed(
        &self,
        clip: RecordedAudio,
    ) -> Result<oneshot::Receiver<Instant>, EvalError> {
        let (tx, rx) = oneshot::channel();
        self.queue_completion(clip, Completion::Timed(tx))?;
        Ok(rx)
    }
    fn queue_completion(
        &self,
        clip: RecordedAudio,
        completion: Completion,
    ) -> Result<(), EvalError> {
        if self.stop.is_cancelled() {
            return Err(EvalError::Cancelled);
        }
        // Reserve queue capacity first. Publication cannot fail after ledger
        // registration, and a consumer can never observe an unregistered ticket.
        let permit = self
            .commands
            .try_reserve()
            .map_err(|_| EvalError::Limit("recording playback queue"))?;
        let ticket = if let Some(timeline) = &self.timeline {
            let mut capture = timeline.lock()?;
            Some(capture.register_user(timeline.elapsed_ns()?, clip.pcm.clone())?)
        } else {
            None
        };
        permit.send(QueuedClip {
            clip,
            completion,
            ticket,
        });
        Ok(())
    }
    pub fn health(&self) -> watch::Receiver<Option<String>> {
        self.health.clone()
    }
    /// Revoke clip admission and signal pacing before any other owner's join.
    pub(crate) fn request_stop(&self) {
        self.stop.cancel();
    }
    pub async fn finish(&mut self) -> Result<(), EvalError> {
        self.request_stop();
        // Borrow the handle across await. Cancellation cannot detach this owner.
        let result = match self.task.as_mut() {
            Some(task) => task.await,
            None => return Ok(()),
        };
        self.task.take();
        result.map_err(|e| EvalError::Scenario(format!("recording task: {e}")))?
    }
}

async fn stream(
    sender: SessionSender,
    mut commands: mpsc::Receiver<QueuedClip>,
    options: &SessionOptions,
    timeline: Option<&TimelineRecorder>,
) -> Result<(), EvalError> {
    let mut current: Option<(QueuedClip, usize)> = None;
    let mut next = Instant::now();
    let mut count = 0usize;
    let mut bytes = 0usize;
    loop {
        tokio::time::sleep_until(next).await;
        let tick = Instant::now();
        // Missed ticks never burst-catch up. Preserve source 40 ms stream pacing.
        next = tick + PERIOD;
        if current.is_none() {
            current = commands.try_recv().ok().map(|queued| (queued, 0));
        }
        let mut chunk = [0u8; CHUNK_BYTES];
        let source = if let Some((queued, offset)) = &current {
            let size = CHUNK_BYTES.min(queued.clip.pcm.len() - *offset);
            chunk[..size].copy_from_slice(&queued.clip.pcm[*offset..*offset + size]);
            Some((queued.ticket, *offset, size))
        } else {
            None
        };
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
        // Successful local mpsc admission is the sole qualification point.
        // There is no await or completion receipt between send success and
        // this pre-reserved, synchronous cursor commit. Padding/idle is absent.
        if let Some((ticket, offset, size)) = source {
            if let (Some(timeline), Some(ticket)) = (timeline, ticket) {
                timeline.lock()?.commit_user(ticket, offset, size)?;
            }
            if let Some((_, cursor)) = &mut current {
                *cursor = offset + size;
            }
        }
        if current
            .as_ref()
            .is_some_and(|(queued, offset)| *offset == queued.clip.pcm.len())
        {
            if let Some((queued, _)) = current.take() {
                queued.completion.send();
            }
        }
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;

    #[tokio::test]
    async fn stop_revokes_admission_and_cancelled_finish_retains_actual_join() {
        let (commands, _input) = mpsc::channel(1);
        let (_health_tx, health) = watch::channel(None);
        let stop = Cancellation::new();
        let task_stop = stop.clone();
        let (entered, mut observed) = oneshot::channel();
        let (release, held) = oneshot::channel();
        let (retired, retired_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            task_stop.cancelled().await;
            let _ = entered.send(());
            let _ = held.await;
            let _ = retired.send(());
            Ok(())
        });
        let mut playback = Playback {
            commands,
            timeline: None,
            stop,
            health,
            task: Some(task),
        };
        playback.request_stop();
        playback.request_stop();
        let rejected = playback.queue(RecordedAudio::from_pcm16(&[0, 0]).unwrap());
        {
            let finish = playback.finish();
            tokio::pin!(finish);
            tokio::select! {
                biased;
                result = &mut finish => panic!("held playback retired early: {result:?}"),
                result = &mut observed => result.unwrap(),
            }
        }
        let retained = playback.task.is_some();
        release.send(()).unwrap();
        let joined = playback.finish().await;
        retired_rx.await.unwrap();
        let retired = playback.task.is_none();
        playback.finish().await.unwrap();
        assert!(matches!(rejected, Err(EvalError::Cancelled)));
        assert!(retained && retired);
        assert!(joined.is_ok());
    }

    #[tokio::test]
    async fn failed_task_is_retired_before_repeated_finish() {
        let (commands, _input) = mpsc::channel(1);
        let (_health_tx, health) = watch::channel(None);
        let mut playback = Playback {
            commands,
            timeline: None,
            stop: Cancellation::new(),
            health,
            task: Some(tokio::spawn(async {
                Err(EvalError::Timeout("recording send"))
            })),
        };
        let first = playback.finish().await;
        assert!(playback.task.is_none());
        assert!(matches!(first, Err(EvalError::Timeout("recording send"))));
        playback.finish().await.unwrap();
    }
}

#[cfg(test)]
mod timeline_tests {
    use super::*;
    use crate::{matcher::ScriptResult, TimelineRecordingLimits};
    use pipecat_transports::{SessionConfig, WebSocketSession};

    fn recorder(limits: TimelineRecordingLimits) -> TimelineRecorder {
        TimelineRecorder {
            origin: Instant::now(),
            capture: Arc::new(Mutex::new(TimelineCapture::new(limits).unwrap())),
        }
    }
    fn snapshot(recorder: TimelineRecorder) -> crate::TimelineRecordingRun {
        Arc::try_unwrap(recorder.capture)
            .unwrap()
            .into_inner()
            .unwrap()
            .into_run(ScriptResult::new("playback recording", 0))
    }
    async fn sockets() -> (WebSocketSession, WebSocketSession) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("ws://{}", listener.local_addr().unwrap());
        let accept = async {
            let (socket, _) = listener.accept().await.unwrap();
            WebSocketSession::accept(socket, SessionConfig::default())
                .await
                .unwrap()
        };
        let (client, server) = tokio::join!(
            WebSocketSession::connect(&address, SessionConfig::default()),
            accept
        );
        (client.unwrap(), server)
    }

    #[test]
    fn rejected_queue_never_registers_a_source_and_failed_registration_releases_permit() {
        let timeline = recorder(TimelineRecordingLimits {
            max_raw_bytes: 2,
            ..Default::default()
        });
        let (commands, mut input) = mpsc::channel(1);
        let (_health_tx, health) = watch::channel(None);
        let playback = Playback {
            commands,
            timeline: Some(timeline.clone()),
            stop: Cancellation::new(),
            health,
            task: None,
        };
        let too_large = playback.queue(RecordedAudio::from_pcm16(&[1, 0, 2, 0]).unwrap());
        assert!(matches!(
            too_large,
            Err(EvalError::Limit("timeline recording raw bytes"))
        ));
        assert_eq!(timeline.lock().unwrap().remaining_raw_bytes(), 2);
        assert!(matches!(
            input.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        let _receipt = playback
            .queue(RecordedAudio::from_pcm16(&[1, 0]).unwrap())
            .unwrap();
        let records = timeline.lock().unwrap().remaining_records();
        let full = playback.queue(RecordedAudio::from_pcm16(&[2, 0]).unwrap());
        assert!(matches!(
            full,
            Err(EvalError::Limit("recording playback queue"))
        ));
        assert_eq!(timeline.lock().unwrap().remaining_records(), records);
        let registered = input.try_recv().unwrap();
        assert!(registered.ticket.is_some());
        timeline.lock().unwrap().close(1_000_000_000).unwrap();
        let closed = playback.queue(RecordedAudio::from_pcm16(&[3, 0]).unwrap());
        assert!(matches!(closed, Err(EvalError::Cancelled)));
        assert!(matches!(
            input.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(playback);
        assert!(snapshot(timeline).to_wav().unwrap().is_none());
    }

    #[tokio::test]
    async fn actual_send_receipt_keeps_source_only_and_excludes_wire_idle_and_padding() {
        let (mut client, mut peer) = sockets().await;
        let timeline = recorder(TimelineRecordingLimits::default());
        let mut playback = Playback::start_with_timeline(
            client.sender(),
            &SessionOptions::default(),
            Some(timeline.clone()),
        );
        // Observe a real idle wire packet before a source is queued.
        tokio::time::timeout(Duration::from_secs(2), peer.recv())
            .await
            .unwrap()
            .unwrap();
        let pcm = [9, 0, 10, 0, 11, 0];
        let receipt = playback
            .queue(RecordedAudio::from_pcm16(&pcm).unwrap())
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), receipt)
            .await
            .unwrap()
            .unwrap();
        // A zero bot cutoff must not discard the actual successful user receipt.
        timeline.lock().unwrap().close(0).unwrap();
        playback.finish().await.unwrap();
        drop(playback);
        let (client_joined, peer_joined) =
            tokio::join!(client.cancel_and_join(), peer.cancel_and_join());
        assert!(client_joined.is_ok() && peer_joined.is_ok());
        let wav = snapshot(timeline).to_wav().unwrap().unwrap();
        assert_eq!(pipecat_audio::read_pcm16_wav(&wav).unwrap().pcm, pcm);
    }

    #[tokio::test]
    async fn rejected_actual_sender_does_not_commit_or_send_a_completion_receipt() {
        let (mut client, mut peer) = sockets().await;
        let sender = client.sender();
        let (client_joined, peer_joined) =
            tokio::join!(client.cancel_and_join(), peer.cancel_and_join());
        assert!(client_joined.is_ok() && peer_joined.is_ok());
        let timeline = recorder(TimelineRecordingLimits::default());
        let mut playback = Playback::start_with_timeline(
            sender,
            &SessionOptions::default(),
            Some(timeline.clone()),
        );
        let receipt = playback
            .queue(RecordedAudio::from_pcm16(&[9, 0]).unwrap())
            .unwrap();
        let mut health = playback.health();
        tokio::time::timeout(Duration::from_secs(2), health.changed())
            .await
            .unwrap()
            .unwrap();
        assert!(health.borrow().is_some());
        assert!(receipt.await.is_err());
        assert!(matches!(
            playback.finish().await,
            Err(EvalError::Transport(_))
        ));
        drop(playback);
        timeline.lock().unwrap().close(0).unwrap();
        assert!(snapshot(timeline).to_wav().unwrap().is_none());
    }
}
