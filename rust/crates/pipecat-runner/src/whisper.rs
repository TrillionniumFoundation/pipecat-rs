//! Session-owned native recognition and explicit continuous-audio segmentation.
use crate::RunnerError;
use pipecat_core::*;
use pipecat_whisper::{
    SegmentationMode, WhisperConfig, WhisperProcessorOptions, WhisperService, WhisperSettings,
    WhisperSttProcessor,
};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Clone, Debug)]
pub enum WhisperSegmentation {
    /// Requires the runner's configured native Silero VAD model and runtime.
    Vad,
    /// Nonoverlapping windows of normalized PCM. The final partial window is
    /// flushed on graceful End/Stop; cancellation discards it. This can cut words.
    FixedWindow(Duration),
}
#[derive(Clone, Debug)]
pub struct LocalWhisperConfig {
    pub model: WhisperConfig,
    pub settings: WhisperSettings,
    /// Bounded onset retention for delayed VAD starts; ignored for fixed windows.
    pub pre_roll_duration: Duration,
    pub segmentation: WhisperSegmentation,
}
impl LocalWhisperConfig {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            model: WhisperConfig::new(path),
            settings: WhisperSettings::default(),
            pre_roll_duration: Duration::from_secs(3),
            segmentation: WhisperSegmentation::Vad,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), RunnerError> {
        self.model.validate()?;
        self.settings.validate()?;
        match self.segmentation {
            WhisperSegmentation::Vad if self.pre_roll_duration > Duration::from_secs(5) || self.model.max_segment_duration <= self.pre_roll_duration + Duration::from_millis(500) => Err(RunnerError::SttConfiguration("Whisper VAD pre-roll must be at most 5000 ms and fit the native segment capacity plus 500 ms trailing silence")),
            WhisperSegmentation::FixedWindow(window) if !(Duration::from_secs(1)..=Duration::from_secs(30)).contains(&window) || window > self.model.max_segment_duration => Err(RunnerError::SttConfiguration("Whisper fixed windows must be 1000..30000 ms and fit the native segment capacity")),
            _ => Ok(()),
        }
    }
}

#[derive(Default)]
pub(crate) struct NativeModelOwners {
    pub count: AtomicUsize,
    pub fixed_windows_submitted: AtomicUsize,
    changed: tokio::sync::Notify,
    parent: Option<Arc<NativeModelOwners>>,
}
impl NativeModelOwners {
    pub fn for_session(parent: Arc<Self>) -> Self {
        Self {
            parent: Some(parent),
            ..Default::default()
        }
    }
    fn acquire(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
        if let Some(parent) = &self.parent {
            parent.acquire();
        }
    }
    fn release(&self) {
        self.count.fetch_sub(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        if let Some(parent) = &self.parent {
            parent.release();
        }
    }
    fn submitted(&self) {
        self.fixed_windows_submitted.fetch_add(1, Ordering::SeqCst);
        if let Some(parent) = &self.parent {
            parent.submitted();
        }
    }
    pub async fn wait_empty(&self) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.count.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    }
}
struct ModelLease(Arc<NativeModelOwners>);
impl ModelLease {
    fn new(active: Arc<NativeModelOwners>) -> Self {
        active.acquire();
        Self(active)
    }
}
impl Drop for ModelLease {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[derive(Default)]
struct WindowBuffer {
    bytes: Vec<u8>,
    muted: bool,
    health_revision: u64,
}
impl WindowBuffer {
    fn sync_health(&mut self, health: ProcessorHealthSnapshot) -> bool {
        if self.health_revision != health.revision || !health.is_usable {
            self.health_revision = health.revision;
            self.bytes.clear();
        }
        health.is_usable
    }
    fn append(&mut self, bytes: &[u8], capacity: usize) -> Vec<Vec<u8>> {
        if self.muted {
            return vec![];
        }
        let mut remaining = bytes;
        let mut complete = Vec::new();
        while !remaining.is_empty() {
            let take = remaining.len().min(capacity - self.bytes.len());
            self.bytes.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if self.bytes.len() == capacity {
                complete.push(std::mem::take(&mut self.bytes));
            }
        }
        complete
    }
}
/// The lease is released only after native Drop has joined its worker/reaper,
/// including failed setup, cancelled loading, and aborted async cleanup.
pub(crate) struct OwnedWhisper {
    processor: Option<WhisperSttProcessor>,
    window_bytes: Option<usize>,
    buffer: Mutex<WindowBuffer>,
    lease: ModelLease,
}
impl OwnedWhisper {
    pub async fn load(
        config: &LocalWhisperConfig,
        active: Arc<NativeModelOwners>,
    ) -> Result<Self, RunnerError> {
        config.validate()?;
        let lease = ModelLease::new(active);
        let service = WhisperService::load(config.model.clone()).await?;
        let (segmentation, window_bytes) = match config.segmentation {
            WhisperSegmentation::Vad => (SegmentationMode::Vad, None),
            WhisperSegmentation::FixedWindow(duration) => (
                SegmentationMode::AudioFrames,
                Some((duration.as_secs_f64() * 16000.).floor() as usize * 2),
            ),
        };
        let mut options = WhisperProcessorOptions {
            settings: config.settings.clone(),
            pre_roll_duration: config.pre_roll_duration,
            segmentation,
            audio_passthrough: false,
            ..Default::default()
        };
        if window_bytes.is_some() {
            options.pre_roll_duration = Duration::ZERO;
            options.trailing_silence = Duration::ZERO;
        }
        Ok(Self {
            processor: Some(WhisperSttProcessor::new(service, options)?),
            window_bytes,
            buffer: Mutex::default(),
            lease,
        })
    }
    fn native(&self) -> &WhisperSttProcessor {
        self.processor.as_ref().unwrap()
    }
    fn buffer(&self) -> Result<std::sync::MutexGuard<'_, WindowBuffer>, ProcessorError> {
        self.buffer.lock().map_err(|_| {
            ProcessorError::new("Whisper window poisoned").with_category(ErrorCategory::Application)
        })
    }
    async fn submit(
        &self,
        bytes: Vec<u8>,
        revision: u64,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let audio = AudioRawFrame::new(bytes, 16000, 1).map_err(|error| {
            ProcessorError::new(error.to_string()).with_category(ErrorCategory::InvalidRequest)
        })?;
        let health = ctx.health();
        if !revision_is_usable(&health, revision) {
            return Ok(());
        }
        self.native()
            .process(
                Frame::new(FrameData::InputAudio(audio)),
                Direction::Downstream,
                ctx,
            )
            .await?;
        if revision_is_usable(&health, revision) {
            self.lease.0.submitted();
        }
        Ok(())
    }
    async fn flush(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let (bytes, revision) = {
            let mut buffer = self.buffer()?;
            if !buffer.sync_health(ctx.health().snapshot()) {
                return Ok(());
            }
            (std::mem::take(&mut buffer.bytes), buffer.health_revision)
        };
        self.submit(bytes, revision, ctx).await
    }
}
fn revision_is_usable(health: &ProcessorHealth, revision: u64) -> bool {
    let current = health.snapshot();
    current.is_usable && current.revision == revision
}
impl Drop for OwnedWhisper {
    fn drop(&mut self) {
        drop(self.processor.take());
    }
}
#[async_trait]
impl FrameProcessor for OwnedWhisper {
    fn name(&self) -> &str {
        self.native().name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.native().health()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.native().setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let Some(capacity) = self.window_bytes {
            self.buffer()?.sync_health(ctx.health().snapshot());
            match &frame.data {
                FrameData::InputAudio(audio) if direction == Direction::Downstream => {
                    let (segments, revision) = {
                        let mut buffer = self.buffer()?;
                        if !buffer.sync_health(ctx.health().snapshot()) || buffer.muted {
                            return Ok(());
                        }
                        if audio.sample_rate != 16000
                            || audio.num_channels != 1
                            || audio.audio.is_empty()
                            || audio.audio.len() > 128 * 1024
                            || !audio.audio.len().is_multiple_of(2)
                        {
                            buffer.bytes.clear();
                            return Err(ProcessorError::new(
                                "Whisper segmentation requires bounded normalized 16 kHz mono PCM16",
                            ).with_category(ErrorCategory::InvalidRequest));
                        }
                        (
                            buffer.append(&audio.audio, capacity),
                            buffer.health_revision,
                        )
                    };
                    for bytes in segments {
                        self.submit(bytes, revision, ctx).await?;
                    }
                    return Ok(());
                }
                FrameData::SttMute(muted) => {
                    let mut buffer = self.buffer()?;
                    buffer.muted = *muted;
                    if *muted {
                        buffer.bytes.clear();
                    }
                }
                FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                    self.buffer()?.bytes.clear();
                }
                FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop => {
                    self.flush(ctx).await?
                }
                _ => {}
            }
        }
        self.native().process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if self.window_bytes.is_some() {
            self.flush(ctx).await?;
        }
        self.native().prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.native().cleanup().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn session_join_is_isolated_and_host_join_counts_every_owner() {
        let host = Arc::new(NativeModelOwners::default());
        let first = Arc::new(NativeModelOwners::for_session(host.clone()));
        let second = Arc::new(NativeModelOwners::for_session(host.clone()));
        let first_lease = ModelLease::new(first.clone());
        let second_lease = ModelLease::new(second.clone());
        first.submitted();
        assert_eq!(host.count.load(Ordering::SeqCst), 2);
        assert_eq!(host.fixed_windows_submitted.load(Ordering::SeqCst), 1);
        drop(first_lease);
        first.wait_empty().await;
        assert_eq!(host.count.load(Ordering::SeqCst), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), host.wait_empty())
                .await
                .is_err()
        );
        drop(second_lease);
        host.wait_empty().await;
        assert_eq!(second.count.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn invalid_native_model_releases_ownership_without_fallback() {
        let owners = Arc::new(NativeModelOwners::default());
        let config = LocalWhisperConfig::new(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
        assert!(OwnedWhisper::load(&config, owners.clone()).await.is_err());
        owners.wait_empty().await;
        assert_eq!(owners.count.load(Ordering::SeqCst), 0);
        assert_eq!(owners.fixed_windows_submitted.load(Ordering::SeqCst), 0);
    }
    #[test]
    fn local_configuration_rejects_unbounded_or_incomplete_segmentation() {
        let mut config =
            LocalWhisperConfig::new(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"));
        config.validate().unwrap(); // Contents are validated by the native loader.
        assert_eq!(config.pre_roll_duration, Duration::from_secs(3));
        config.pre_roll_duration = Duration::from_millis(5001);
        assert!(config.validate().is_err());
        config.pre_roll_duration = Duration::from_millis(5000);
        config.validate().unwrap();
        config.pre_roll_duration = Duration::from_secs(3);
        config.model.max_segment_duration = Duration::from_millis(3500);
        assert!(config.validate().is_err());
        config.model.max_segment_duration = Duration::from_secs(60);
        for duration in [
            Duration::ZERO,
            Duration::from_millis(999),
            Duration::from_secs(31),
        ] {
            config.segmentation = WhisperSegmentation::FixedWindow(duration);
            assert!(config.validate().is_err());
        }
        config.segmentation = WhisperSegmentation::FixedWindow(Duration::from_secs(12));
        config.model.max_segment_duration = Duration::from_secs(10);
        assert!(config.validate().is_err());
        config.segmentation = WhisperSegmentation::Vad;
        config.model.max_segment_duration = Duration::from_secs(1);
        assert!(config.validate().is_err());
    }
    #[test]
    fn continuous_packets_are_joined_and_split_at_explicit_windows() {
        let mut buffer = WindowBuffer::default();
        assert!(buffer.append(&[1, 2], 8).is_empty());
        assert_eq!(
            buffer.append(
                &[3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18],
                8
            ),
            vec![
                vec![1, 2, 3, 4, 5, 6, 7, 8],
                vec![9, 10, 11, 12, 13, 14, 15, 16]
            ]
        );
        assert_eq!(buffer.bytes, vec![17, 18]);
        buffer.muted = true;
        buffer.bytes.clear();
        assert!(buffer.append(&[19, 20], 8).is_empty());
        assert!(buffer.bytes.is_empty());
    }

    #[test]
    fn coalesced_health_revisions_discard_window_prefix_and_preserve_mute() {
        let health = ProcessorHealth::default();
        let mut buffer = WindowBuffer::default();
        assert!(buffer.sync_health(health.snapshot()));
        assert!(buffer.append(&[1, 2], 8).is_empty());
        health.set_usable(false);
        health.set_usable(true);
        assert!(buffer.sync_health(health.snapshot()));
        assert!(buffer.append(&[3, 4], 8).is_empty());
        assert_eq!(buffer.bytes, [3, 4]);
        buffer.muted = true;
        health.set_usable(false);
        health.set_usable(true);
        assert!(buffer.sync_health(health.snapshot()));
        assert!(buffer.muted);
        assert!(buffer.bytes.is_empty());
        assert!(buffer.append(&[5, 6], 8).is_empty());
        assert!(buffer.bytes.is_empty());
    }

    #[test]
    fn retired_multi_window_batches_never_admit_under_a_new_revision() {
        let health = ProcessorHealth::default();
        let mut buffer = WindowBuffer::default();
        buffer.sync_health(health.snapshot());
        let windows = buffer.append(&[0; 20], 8);
        let revision = buffer.health_revision;
        assert_eq!(windows.len(), 2);
        assert!(revision_is_usable(&health, revision));
        health.set_usable(false);
        health.set_usable(true);
        assert!(windows
            .iter()
            .all(|_| !revision_is_usable(&health, revision)));
        assert!(buffer.sync_health(health.snapshot()));
        assert!(
            buffer.bytes.is_empty(),
            "retired partial tail must not flush"
        );
        assert!(revision_is_usable(&health, buffer.health_revision));
        health.set_usable(false);
        assert!(!buffer.sync_health(health.snapshot()));
        assert!(!revision_is_usable(&health, buffer.health_revision));
    }

    async fn window_barrier(worker: &mut PipelineWorker) {
        let marker = Frame::text("window barrier");
        let id = marker.id;
        worker
            .sender()
            .send(marker, Direction::Downstream)
            .await
            .unwrap();
        loop {
            let frame = tokio::time::timeout(Duration::from_secs(5), worker.recv())
                .await
                .unwrap()
                .unwrap();
            if frame.frame.id == id {
                return;
            }
        }
    }

    async fn window_input(worker: &mut PipelineWorker, bytes: Vec<u8>, rate: u32) {
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputAudio(AudioRawFrame {
                    audio: bytes,
                    sample_rate: rate,
                    num_channels: 1,
                    context_id: None,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
        window_barrier(worker).await;
    }

    async fn window_temperature(worker: &mut PipelineWorker, temperature: f32) {
        worker
            .sender()
            .send(
                Frame::new(FrameData::SttUpdateSettings(Box::new(
                    ServiceUpdateSettingsFrame {
                        settings: [("temperature".into(), temperature.into())]
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    },
                ))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        window_barrier(worker).await;
    }

    #[tokio::test]
    #[ignore = "requires explicit PIPECAT_WHISPER_MODEL; no audio inference"]
    async fn owned_health_gates_fixed_windows_and_shares_native_settings_recovery() {
        tokio::time::timeout(Duration::from_secs(30), async {
            let path =
                std::env::var_os("PIPECAT_WHISPER_MODEL").expect("set PIPECAT_WHISPER_MODEL");
            let mut config = LocalWhisperConfig::new(path);
            config.segmentation = WhisperSegmentation::FixedWindow(Duration::from_secs(1));
            let owners = Arc::new(NativeModelOwners::default());
            let owned = Arc::new(OwnedWhisper::load(&config, owners.clone()).await.unwrap());
            let native_health = owned.native().health().unwrap();
            native_health.set_usable(false);
            assert!(!owned.health().unwrap().is_usable());
            let mut worker = Pipeline::new(vec![owned.clone()])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            let health = worker.processor_health(0).unwrap();
            assert!(!health.is_usable());
            owned
                .native()
                .update_settings(WhisperSettings {
                    temperature: 0.1,
                    ..owned.native().settings().unwrap()
                })
                .unwrap();
            assert!(
                health.is_usable(),
                "public native settings must recover the wrapper runtime"
            );
            assert_eq!(native_health.snapshot(), health.snapshot());

            window_input(&mut worker, vec![1; 100], 16000).await;
            assert_eq!(owned.buffer().unwrap().bytes, [1; 100]);
            health.set_usable(false);
            health.set_usable(true);
            window_input(&mut worker, vec![2; 200], 16000).await;
            assert_eq!(owned.buffer().unwrap().bytes, [2; 200]);
            assert_eq!(native_health.snapshot(), health.snapshot());

            health.set_usable(false);
            window_input(&mut worker, vec![0; 64_000], 16000).await;
            window_input(&mut worker, vec![1], 8000).await;
            assert!(owned.buffer().unwrap().bytes.is_empty());
            assert!(
                worker.errors().is_empty(),
                "gated input must not be validated or submitted"
            );
            assert_eq!(owners.fixed_windows_submitted.load(Ordering::SeqCst), 0);
            let unusable = health.snapshot();
            window_temperature(&mut worker, 0.1).await;
            assert_eq!(health.snapshot(), unusable);
            window_temperature(&mut worker, -1.0).await;
            assert_eq!(health.snapshot(), unusable);
            assert_eq!(worker.errors().len(), 1);
            assert_eq!(worker.errors()[0].category, ErrorCategory::InvalidRequest);
            window_temperature(&mut worker, 0.2).await;
            assert!(native_health.is_usable());
            assert!(health.is_usable());
            window_input(&mut worker, vec![1, 2], 8000).await;
            assert!(
                !health.is_usable(),
                "admitted malformed audio is a permanent request error"
            );
            assert_eq!(worker.errors().len(), 2);
            assert_eq!(worker.errors()[1].category, ErrorCategory::InvalidRequest);
            window_temperature(&mut worker, 0.3).await;
            assert!(health.is_usable());

            window_input(&mut worker, vec![3; 100], 16000).await;
            worker
                .sender()
                .send(Frame::new(FrameData::SttMute(true)), Direction::Downstream)
                .await
                .unwrap();
            window_barrier(&mut worker).await;
            health.set_usable(false);
            health.set_usable(true);
            window_input(&mut worker, vec![4; 100], 16000).await;
            assert!(owned.buffer().unwrap().muted);
            assert!(owned.buffer().unwrap().bytes.is_empty());
            worker
                .sender()
                .send(Frame::new(FrameData::SttMute(false)), Direction::Downstream)
                .await
                .unwrap();
            window_barrier(&mut worker).await;
            window_input(&mut worker, vec![5; 100], 16000).await;
            health.set_usable(false);
            worker.end().await.unwrap();
            let (frames, report) = worker.collect().await.unwrap();
            assert_eq!(report.errors.len(), 2);
            assert!(frames
                .iter()
                .any(|frame| matches!(frame.frame.data, FrameData::End(_))));
            assert!(!frames
                .iter()
                .any(|frame| matches!(frame.frame.data, FrameData::Transcription(_))));
            assert!(owned.buffer().unwrap().bytes.is_empty());
            assert_eq!(owners.fixed_windows_submitted.load(Ordering::SeqCst), 0);
            assert!(!owned.native().is_running());
            drop(owned);
            owners.wait_empty().await;
        })
        .await
        .expect("fixed-window health regression stalled");
    }

    #[tokio::test]
    #[ignore = "requires PIPECAT_WHISPER_MODEL and PIPECAT_WHISPER_FIXTURE; real native inference"]
    async fn final_partial_window_includes_the_normalizer_tail_exactly_once() {
        tokio::time::timeout(Duration::from_secs(90), async {
            let model =
                std::env::var_os("PIPECAT_WHISPER_MODEL").expect("set PIPECAT_WHISPER_MODEL");
            let fixture =
                std::env::var_os("PIPECAT_WHISPER_FIXTURE").expect("set PIPECAT_WHISPER_FIXTURE");
            let bytes = std::fs::read(fixture).unwrap();
            let wav = pipecat_audio::read_pcm16_wav(&bytes).unwrap();
            assert_eq!(wav.format.sample_rate(), 16000);
            assert_eq!(wav.format.channels(), 1);
            let pcm: Vec<u8> = wav
                .pcm
                .as_chunks::<2>()
                .0
                .iter()
                .flat_map(|sample| [*sample; 3])
                .flatten()
                .collect();
            let mut config = LocalWhisperConfig::new(model);
            config.model.inference_threads = 2;
            config.settings.strategy = pipecat_whisper::DecodingStrategy::Greedy { best_of: 1 };
            config.segmentation = WhisperSegmentation::FixedWindow(Duration::from_secs(30));
            let owners = Arc::new(NativeModelOwners::default());
            let processor = OwnedWhisper::load(&config, owners.clone()).await.unwrap();
            let worker = Pipeline::new(vec![
                Arc::new(crate::processors::NormalizeAudio::new(16000)),
                Arc::new(processor),
            ])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(60),
                ..Default::default()
            })
            .await
            .unwrap();
            for bytes in pcm.chunks(2880) {
                worker
                    .sender()
                    .send(
                        Frame::new(FrameData::InputAudio(
                            AudioRawFrame::new(bytes.to_vec(), 48000, 1).unwrap(),
                        )),
                        Direction::Downstream,
                    )
                    .await
                    .unwrap();
            }
            worker.end().await.unwrap();
            let (frames, report) = worker.collect().await.unwrap();
            assert_eq!(report.total_errors, 0, "{report:?}");
            let transcripts: Vec<_> = frames
                .iter()
                .filter_map(|frame| match &frame.frame.data {
                    FrameData::Transcription(transcript) => Some(transcript),
                    _ => None,
                })
                .collect();
            assert_eq!(transcripts.len(), 1);
            let text = transcripts[0].text.to_ascii_lowercase();
            assert!(
                text.contains("ask not what your country can do for you"),
                "{text}"
            );
            assert!(
                text.contains("ask what you can do for your country"),
                "{text}"
            );
            let duration = transcripts[0].result.as_ref().unwrap()["audio_duration_seconds"]
                .as_f64()
                .unwrap();
            assert!((duration - wav.pcm.len() as f64 / 32000.).abs() < 1. / 16000.);
            owners.wait_empty().await;
            assert_eq!(owners.fixed_windows_submitted.load(Ordering::SeqCst), 1);
        })
        .await
        .expect("partial-window graceful drain timed out");
    }

    #[tokio::test]
    #[ignore = "requires PIPECAT_WHISPER_MODEL; drop cleanup of a live native session"]
    async fn dropping_the_last_runner_handle_joins_the_session_native_owner() {
        use pipecat_client_server::server::BotApi;
        use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
        use serde_json::{json, Value};

        tokio::time::timeout(Duration::from_secs(45), async {
            let path =
                std::env::var_os("PIPECAT_WHISPER_MODEL").expect("set PIPECAT_WHISPER_MODEL");
            let mut local = LocalWhisperConfig::new(path);
            local.model.inference_threads = 2;
            local.segmentation = WhisperSegmentation::FixedWindow(Duration::from_secs(30));
            let runner = crate::NativeRunner::new(
                crate::RunnerConfig {
                    stt: crate::SttProvider::Whisper(local),
                    allow_missing_origin: true,
                    options: crate::Options {
                        introduce: false,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                crate::ProviderConfig {
                    deepgram_key: String::new(),
                    deepgram_endpoint: "ws://127.0.0.1:0/unused".into(),
                    openai_key: "synthetic-only".into(),
                    openai_endpoint: "http://127.0.0.1:0/unused".into(),
                    cartesia_key: "synthetic-only".into(),
                    cartesia_endpoint: "ws://127.0.0.1:0/unused".into(),
                    model: "synthetic-only".into(),
                    voice_id: "synthetic-only".into(),
                    system_prompt: "test".into(),
                    input_rate: 16000,
                    output_rate: 24000,
                },
            )
            .unwrap();
            let owners = runner.inner.resources.local_stt_workers.clone();
            let response = runner
                .start(&json!({"transport":"websocket"}))
                .await
                .unwrap();
            let mut client = WebSocketSession::connect(
                response["wsUrl"].as_str().unwrap(),
                SessionConfig::default(),
            )
            .await
            .unwrap();
            client
                .sender()
                .send(Packet::Binary(
                    pipecat_browser_client::protocol::encode_message(
                        &pipecat_browser_client::client_ready(1),
                    )
                    .into(),
                ))
                .await
                .unwrap();
            loop {
                let Packet::Binary(bytes) = client.recv().await.expect("bot-ready") else {
                    panic!("expected protobuf")
                };
                if let Some(pipecat_browser_client::protocol::frame::Body::Message(message)) =
                    pipecat_browser_client::protocol::decode(&bytes)
                        .unwrap()
                        .body
                {
                    let value: Value = serde_json::from_str(&message.data).unwrap();
                    if value["type"] == "bot-ready" {
                        break;
                    }
                }
            }
            assert_eq!(owners.count.load(Ordering::SeqCst), 1);
            drop(runner);
            owners.wait_empty().await;
            assert_eq!(owners.count.load(Ordering::SeqCst), 0);
            while client.recv().await.is_some() {}
            let _ = client.finish().await;
        })
        .await
        .expect("runner drop retained an owned native model");
    }
}
