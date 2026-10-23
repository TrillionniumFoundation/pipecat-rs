//! Actual local engines and CPAL devices. Loading/open/close can block in native
//! libraries: run this owner in a supervised media process, never the CNS loop.
use crate::{MediaControl, OutputIdentity, StreamIdentity};
use pipecat_audio::PcmFormat;
use pipecat_core::{
    AudioRawFrame, Direction, FrameData, Pipeline, PipelineConfig, PipelineWorker, WorkerReport,
};
use pipecat_local_audio::{
    AudioLimits, DeviceSelector, LocalAudioOutput, LocalAudioTransport, LocalAudioTransportParams,
    SubmissionReceipt,
};
use pipecat_piper::{Artifact, PiperConfig, PiperService, PiperSettings};
use pipecat_vad::{RuntimeOptions, SileroModel, SileroVadAnalyzer, SileroVadProcessor};
use pipecat_whisper::{
    WhisperConfig, WhisperProcessorOptions, WhisperService, WhisperSettings, WhisperSttProcessor,
};
use serde::{Deserialize, Serialize};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSpeechConfig {
    pub stream: StreamIdentity,
    pub turn_id: String,
    /// Current Hepta output epoch, not a separate media-local counter.
    pub output_epoch: u64,
    pub whisper_model: Artifact,
    pub whisper_settings: WhisperSettings,
    pub silero_model: Artifact,
    pub piper: PiperConfig,
    /// Native device IDs; None selects that endpoint's native default.
    pub input_device: Option<String>,
    pub output_device: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum LocalSpeechError {
    #[error("local media failure: {0}")]
    Native(String),
    #[error("stale output epoch")]
    Stale,
    #[error("local input is single-use")]
    InputUsed,
}
fn native(error: impl std::fmt::Display) -> LocalSpeechError {
    LocalSpeechError::Native(error.to_string().chars().take(1024).collect())
}
fn verify(artifact: &Artifact, max_bytes: u64) -> Result<(), LocalSpeechError> {
    let metadata = std::fs::metadata(&artifact.path).map_err(native)?;
    if !metadata.is_file()
        || metadata.len() > max_bytes
        || artifact.sha256.len() != 64
        || !artifact.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        || pipecat_piper::sha256_file(&artifact.path).map_err(native)?
            != artifact.sha256.to_ascii_lowercase()
    {
        return Err(native("artifact identity or size mismatch"));
    }
    Ok(())
}
fn selector(id: Option<String>) -> DeviceSelector {
    id.map(DeviceSelector::Id).unwrap_or_default()
}

pub struct LocalSpeech {
    whisper: Option<WhisperService>,
    vad: Option<SileroVadAnalyzer>,
    settings: WhisperSettings,
    input_device: Option<String>,
    presentation: LocalPresentation,
}
impl LocalSpeech {
    /// No downloads, alternate providers, credentials or synthesized transcripts.
    /// Artifacts must remain immutable between hash checking and native loading.
    pub async fn load(config: LocalSpeechConfig) -> Result<Self, LocalSpeechError> {
        crate::HmiSession::with_epoch(
            config.stream.clone(),
            config.turn_id.clone(),
            config.output_epoch,
            1,
        )
        .map_err(native)?;
        verify(&config.whisper_model, 1024 * 1024 * 1024)?;
        verify(&config.silero_model, 64 * 1024 * 1024)?;
        verify(&config.piper.runtime, 512 * 1024 * 1024)?;
        let model = SileroModel::load(
            &RuntimeOptions {
                library_path: config.piper.runtime.path.clone(),
            },
            &config.silero_model.path,
        )
        .map_err(native)?;
        let vad = SileroVadAnalyzer::new(model, 16_000, Default::default()).map_err(native)?;
        let whisper = WhisperService::load(WhisperConfig::new(&config.whisper_model.path))
            .await
            .map_err(native)?;
        whisper
            .validate_settings(&config.whisper_settings)
            .map_err(native)?;
        let piper = PiperService::load(config.piper).await.map_err(native)?;
        let output = LocalAudioOutput::new(selector(config.output_device), AudioLimits::default())
            .map_err(native)?;
        Ok(Self {
            whisper: Some(whisper),
            vad: Some(vad),
            settings: config.whisper_settings,
            input_device: config.input_device,
            presentation: LocalPresentation {
                piper,
                output,
                stream: config.stream,
                gate: Arc::new(Mutex::new(OutputGate {
                    epoch: config.output_epoch,
                    turn_id: config.turn_id,
                })),
            },
        })
    }

    pub fn presentation(&self) -> &LocalPresentation {
        &self.presentation
    }

    /// Opens actual capture through the existing bounded VAD/STT pipeline.
    /// Call recv continuously; overflow fails explicitly rather than buffering.
    pub async fn start_input(&mut self) -> Result<RecognitionStream, LocalSpeechError> {
        let whisper = self.whisper.take().ok_or(LocalSpeechError::InputUsed)?;
        let vad = self.vad.take().ok_or(LocalSpeechError::InputUsed)?;
        let transport = LocalAudioTransport::new(LocalAudioTransportParams {
            input_device: selector(self.input_device.take()),
            ..Default::default()
        })
        .map_err(native)?;
        let stt = WhisperSttProcessor::new(
            whisper,
            WhisperProcessorOptions {
                settings: self.settings.clone(),
                audio_passthrough: false,
                ..Default::default()
            },
        )
        .map_err(native)?;
        let vad = SileroVadProcessor::new(vad, Default::default()).map_err(native)?;
        let mut config = PipelineConfig::default();
        config.start.audio_in_sample_rate = 16_000;
        let worker = Pipeline::new(vec![transport.input(), Arc::new(vad), Arc::new(stt)])
            .start(config)
            .await
            .map_err(native)?;
        Ok(RecognitionStream { worker })
    }

    pub async fn shutdown(&self) -> Result<(), LocalSpeechError> {
        self.presentation.output.close();
        if let Some(whisper) = &self.whisper {
            whisper.shutdown().await.map_err(native)?;
        }
        self.presentation.piper.shutdown().await.map_err(native)
    }
}

#[derive(Debug)]
pub enum RecognitionEvent {
    SpeechStarted,
    FinalTranscript(pipecat_core::TranscriptionFrame),
    Failure(pipecat_core::ErrorFrame),
}
pub struct RecognitionStream {
    worker: PipelineWorker,
}
pub struct InputTermination {
    pub report: Option<WorkerReport>,
    pub error: Option<String>,
    pub total_errors: u64,
}
impl RecognitionStream {
    pub async fn recv(&mut self) -> Option<RecognitionEvent> {
        while let Some(frame) = self.worker.recv().await {
            match frame.frame.data {
                FrameData::Error(error) => return Some(RecognitionEvent::Failure(error)),
                FrameData::VadUserStartedSpeaking(_)
                | FrameData::VadUserStartedSpeakingDetailed(_)
                    if frame.direction == Direction::Downstream =>
                {
                    return Some(RecognitionEvent::SpeechStarted)
                }
                FrameData::Transcription(text) if frame.direction == Direction::Downstream => {
                    return Some(RecognitionEvent::FinalTranscript(text))
                }
                _ => {}
            }
        }
        None
    }
    /// Cancel and drain owned pipeline work; never converts failure into zero errors.
    pub async fn shutdown(mut self) -> InputTermination {
        let mut error = self
            .worker
            .cancel("Hepta input owner closed")
            .await
            .err()
            .map(|error| error.to_string());
        while self.worker.recv().await.is_some() {}
        let report = match self.worker.join().await {
            Ok(report) => Some(report),
            Err(failure) => {
                error = Some(failure.to_string());
                None
            }
        };
        InputTermination {
            report,
            error,
            total_errors: self.worker.total_errors(),
        }
    }
}

/// Prepared native PCM remains unsubmitted; its private epoch cannot be relabeled.
pub struct PreparedAudio {
    identity: OutputIdentity,
    owner: Arc<Mutex<OutputGate>>,
    audio: AudioRawFrame,
}
struct OutputGate {
    epoch: u64,
    turn_id: String,
}
pub struct LocalPresentation {
    piper: PiperService,
    output: LocalAudioOutput,
    stream: StreamIdentity,
    gate: Arc<Mutex<OutputGate>>,
}
impl LocalPresentation {
    fn check_scope(
        &self,
        gate: &OutputGate,
        identity: &OutputIdentity,
    ) -> Result<(), LocalSpeechError> {
        if identity.stream != self.stream
            || identity.output_epoch != gate.epoch
            || identity.turn_id != gate.turn_id
            || !crate::session::valid_id(&identity.utterance_id)
        {
            return Err(LocalSpeechError::Stale);
        }
        Ok(())
    }
    /// Only call after Hepta admits the turn. The shared epoch must already have
    /// been advanced through the HmiSession/MediaControl retirement boundary.
    pub fn admit_turn(&self, turn_id: &str, epoch: u64) -> Result<(), LocalSpeechError> {
        if !crate::session::valid_id(turn_id) {
            return Err(LocalSpeechError::Stale);
        }
        let mut gate = self.gate.lock().map_err(native)?;
        if gate.epoch != epoch {
            return Err(LocalSpeechError::Stale);
        }
        gate.turn_id = turn_id.to_owned();
        Ok(())
    }
    pub fn open_output(&self) -> Result<(), LocalSpeechError> {
        self.output
            .open(PcmFormat::new(self.piper.sample_rate(), 1).map_err(native)?)
            .map_err(native)
    }
    pub async fn synthesize(
        &self,
        identity: OutputIdentity,
        text: &str,
    ) -> Result<PreparedAudio, LocalSpeechError> {
        {
            let gate = self.gate.lock().map_err(native)?;
            self.check_scope(&gate, &identity)?;
        }
        let result = self
            .piper
            .submit(text, &identity.utterance_id, PiperSettings::default())
            .map_err(native)?
            .result()
            .await
            .map_err(native)?;
        {
            let gate = self.gate.lock().map_err(native)?;
            self.check_scope(&gate, &identity)?;
        }
        Ok(PreparedAudio {
            identity,
            owner: self.gate.clone(),
            audio: AudioRawFrame::new(result.pcm, result.sample_rate, result.num_channels)
                .map_err(native)?,
        })
    }
    /// The first poll admits PCM under the same fence as interrupt. Subsequent
    /// waits hold no mutex; interrupt cancels any already admitted old write.
    pub async fn submit(
        &self,
        prepared: PreparedAudio,
    ) -> Result<SubmissionReceipt, LocalSpeechError> {
        let mut write = Box::pin(self.output.write_audio(&prepared.audio));
        let first = {
            let gate = self.gate.lock().map_err(native)?;
            if !Arc::ptr_eq(&prepared.owner, &self.gate) {
                return Err(LocalSpeechError::Stale);
            }
            self.check_scope(&gate, &prepared.identity)?;
            write.as_mut().poll(&mut Context::from_waker(Waker::noop()))
        };
        match first {
            Poll::Ready(result) => result.map_err(native),
            Poll::Pending => write.await.map_err(native),
        }
    }
}
impl MediaControl for LocalPresentation {
    fn interrupt(&self, new_epoch: u64) -> Result<(), String> {
        let mut gate = self
            .gate
            .lock()
            .map_err(|_| "media epoch lock poisoned".to_owned())?;
        if new_epoch <= gate.epoch {
            return Err("media epoch cannot be reused".into());
        }
        gate.epoch = new_epoch;
        self.piper.interrupt();
        if self.output.is_open() {
            self.output.interrupt().map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}
