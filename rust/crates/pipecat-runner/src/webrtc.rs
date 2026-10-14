//! Session-scoped SmallWebRTC signaling and PCM packetization.
use crate::{session, transport::BrowserSession, RunnerConfig, RunnerError};
use pipecat_audio::SincResampler;
use pipecat_client_server::{ProxyError, SessionMethod};
use pipecat_core::{AudioRawFrame, Frame, FrameData, ProcessorError};
use pipecat_services::Context;
use pipecat_webrtc::{
    AudioCodec, ConnectionMode, SmallWebRtcConfig, SmallWebRtcConnection, SmallWebRtcPatchRequest,
    SmallWebRtcRequest, SmallWebRtcRequestHandler,
};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::Instant,
};

/// Native audio peer options. No external STUN/TURN server is used.
#[derive(Clone, Debug)]
pub struct WebRtcRunnerConfig {
    pub codec: AudioCodec,
    /// Restrict native ICE candidates to loopback; useful for offline peer tests.
    pub localhost_only: bool,
    pub signaling_timeout: Duration,
}
impl Default for WebRtcRunnerConfig {
    fn default() -> Self {
        Self {
            codec: AudioCodec::Opus,
            localhost_only: false,
            signaling_timeout: Duration::from_secs(10),
        }
    }
}
pub(crate) struct SignalingCommand {
    pub method: SessionMethod,
    pub body: Value,
    pub reply: oneshot::Sender<Result<Value, ProxyError>>,
}

pub(crate) async fn serve_session(
    mut commands: mpsc::Receiver<SignalingCommand>,
    config: RunnerConfig,
    providers: crate::ProviderConfig,
    rtc: WebRtcRunnerConfig,
    mut cancel: watch::Receiver<bool>,
    resources: session::SessionResources,
) -> Result<(Context, u64), RunnerError> {
    let session_deadline = Instant::now() + config.max_session_duration;
    let connect_deadline = (Instant::now() + config.connect_timeout).min(session_deadline);
    let handler = Arc::new(
        SmallWebRtcRequestHandler::new(
            SmallWebRtcConfig {
                audio_codec: Some(rtc.codec),
                localhost_only: rtc.localhost_only,
                connect_timeout: config.connect_timeout,
                gather_timeout: rtc
                    .signaling_timeout
                    .min(config.connect_timeout)
                    .min(config.max_session_duration),
                queue_capacity: 16,
                ..Default::default()
            },
            ConnectionMode::Single,
            1,
        )
        .map_err(|_| RunnerError::Configuration)?,
    );
    let mut negotiations = tokio::task::JoinSet::new();
    let result = async {
        let peer = loop {
            if Instant::now() >= connect_deadline { return Err(RunnerError::Deadline); }
            let command = tokio::select! {
                biased;
                _ = cancel.changed() => return Err(RunnerError::Protocol("session canceled")),
                result = tokio::time::timeout_at(connect_deadline, commands.recv()) =>
                    result.map_err(|_| RunnerError::Deadline)?.ok_or(RunnerError::Protocol("session closed"))?,
            };
            if command.reply.is_closed() { continue; }
            let request = if matches!(command.method, SessionMethod::Post) {
                serde_json::from_value::<SmallWebRtcRequest>(command.body).ok()
            } else { None };
            let Some(request) = request.filter(|r| r.pc_id.is_none() && !r.restart_pc) else {
                let _ = command.reply.send(Err(ProxyError::InvalidRequest));
                continue;
            };
            // The session retains negotiation ownership across an HTTP timeout
            // or stop. Cleanup first closes the registry, then joins this task.
            let offer_handler = handler.clone();
            negotiations.spawn(async move { offer_handler.handle_offer(request).await });
            let answer = tokio::select! {
                biased;
                _ = cancel.changed() => return Err(RunnerError::Protocol("session canceled")),
                result = tokio::time::timeout_at(
                    connect_deadline.min(Instant::now() + rtc.signaling_timeout),
                    negotiations.join_next()
                ) => result,
            };
            match answer {
                Ok(Some(Ok(Ok((answer, peer))))) => {
                    // If the HTTP caller disappeared, don't keep an orphan peer.
                    if command.reply.send(Ok(json!(answer))).is_err() {
                        return Err(RunnerError::Protocol("offer caller disconnected"));
                    }
                    break peer;
                }
                Ok(Some(Ok(Err(_)))) => { let _ = command.reply.send(Err(ProxyError::InvalidRequest)); }
                Ok(Some(Err(error))) => return Err(negotiation_task_error(error)),
                Ok(None) => return Err(RunnerError::Protocol("WebRTC negotiation task failed")),
                Err(_) => {
                    let _ = command.reply.send(Err(ProxyError::Deadline));
                    return Err(RunnerError::Deadline);
                }
            }
        };
        let mut pipeline_cancel = cancel.clone();
        let pipeline = async {
            tokio::select! {
                biased;
                _ = pipeline_cancel.changed() => return Err(RunnerError::Protocol("session canceled before connection")),
                result = tokio::time::timeout_at(connect_deadline, async {
                    peer.wait_connected(config.connect_timeout).await?;
                    peer.wait_data_channel(config.connect_timeout).await
                }) => result.map_err(|_| RunnerError::Deadline)?.map_err(|_| RunnerError::Protocol("WebRTC connection failed"))?,
            }
            let mut transport = BrowserSession::WebRtc(peer.clone());
            session::run(&mut transport, &config, &providers, &mut pipeline_cancel, session_deadline, &resources).await
        };
        tokio::pin!(pipeline);
        loop {
            tokio::select! {
                biased;
                result = &mut pipeline => break result,
                command = commands.recv() => {
                    let Some(command) = command else { break Err(RunnerError::Protocol("session closed")); };
                    if command.reply.is_closed() { continue; }
                    // Browser capture uses one gathered offer. Reconnect creates a
                    // fresh session rather than replacing an active pipeline's peer.
                    if !matches!(command.method, SessionMethod::Patch) {
                        let _ = command.reply.send(Err(ProxyError::InvalidRequest));
                        continue;
                    }
                    let request = serde_json::from_value::<SmallWebRtcPatchRequest>(command.body);
                    let reply = match request {
                        Ok(request) if request.pc_id == peer.pc_id() => {
                            tokio::select! {
                                biased;
                                _ = cancel.changed() => Err(ProxyError::Closed),
                                result = tokio::time::timeout_at(session_deadline.min(Instant::now() + rtc.signaling_timeout), handler.handle_patch(request)) =>
                                    match result {
                                        Ok(Ok(())) => Ok(json!({"status":"success"})),
                                        Ok(Err(_)) => Err(ProxyError::InvalidRequest),
                                        Err(_) => Err(ProxyError::Deadline),
                                    },
                            }
                        }
                        Ok(_) => Err(ProxyError::Path),
                        Err(_) => Err(ProxyError::InvalidRequest),
                    };
                    let _ = command.reply.send(reply);
                }
            }
        }
    }.await;
    // Registry cleanup runs even after failed negotiation, missed deadlines, or
    // a dropped HTTP response. Its close operation survives caller cancellation.
    finish_signaling(result, handler.close(), &mut negotiations).await
}

fn negotiation_task_error(error: tokio::task::JoinError) -> RunnerError {
    // Panic payloads can contain SDP or credentials; report only failure kind.
    RunnerError::WebRtcNegotiationTasks {
        panicked: usize::from(error.is_panic()),
        cancelled: usize::from(error.is_cancelled()),
    }
}

async fn finish_signaling<T, N: Send + 'static>(
    primary: Result<T, RunnerError>,
    close: impl std::future::Future<Output = pipecat_webrtc::Result<()>>,
    negotiations: &mut tokio::task::JoinSet<N>,
) -> Result<T, RunnerError> {
    // Closing the registry first cancels the owned peers. Even a close failure
    // must not skip joining pending negotiation owners or replace the primary.
    let closed = close.await.map_err(RunnerError::WebRtcCleanup);
    let result = session::with_transport_cleanup(primary, closed);
    let (mut panicked, mut cancelled) = (0usize, 0usize);
    while let Some(outcome) = negotiations.join_next().await {
        if let Err(error) = outcome {
            panicked += usize::from(error.is_panic());
            cancelled += usize::from(error.is_cancelled());
        }
        // An ordinary offer result such as Error::Closed after cancellation is
        // not a task ownership failure. The signaling path has already supplied
        // its invalid-request response or primary cancellation/deadline error.
    }
    let joined = if panicked == 0 && cancelled == 0 {
        Ok(())
    } else {
        Err(RunnerError::WebRtcNegotiationTasks {
            panicked,
            cancelled,
        })
    };
    session::with_transport_cleanup(result, joined)
}

struct AudioState {
    resampler: Option<SincResampler>,
    pending: Vec<u8>,
}
/// Converts arbitrary provider PCM chunks into exact, paced 20 ms codec packets.
/// A synthesis stop flushes the filter tail and pads only the final RTP packet.
pub(crate) struct AudioOutput {
    pub peer: Arc<SmallWebRtcConnection>,
    state: Mutex<AudioState>,
}
impl AudioOutput {
    pub fn new(peer: Arc<SmallWebRtcConnection>) -> Self {
        Self {
            peer,
            state: Mutex::new(AudioState {
                resampler: None,
                pending: Vec::new(),
            }),
        }
    }
    fn audio(&self, input: Option<&AudioRawFrame>) -> Result<Vec<AudioRawFrame>, ProcessorError> {
        let rate = self
            .peer
            .config()
            .audio_codec
            .expect("runner audio codec")
            .sample_rate();
        let mut state = self
            .state
            .lock()
            .map_err(|_| error("audio state poisoned"))?;
        if let Some(audio) = input {
            if audio.num_channels != 1 || audio.audio.is_empty() || audio.audio.len() > 128 * 1024 {
                return Err(error("WebRTC synthesis requires bounded mono PCM16"));
            }
            if state.resampler.is_none() {
                state.resampler =
                    Some(SincResampler::new(audio.sample_rate, rate, 1).map_err(error)?);
            }
            let filter = state.resampler.as_mut().unwrap();
            if filter.input_format().sample_rate() != audio.sample_rate {
                return Err(error("synthesis rate changed during turn"));
            }
            let bytes = filter.process(&audio.audio).map_err(error)?;
            state.pending.extend(bytes);
        } else if let Some(mut filter) = state.resampler.take() {
            state.pending.extend(filter.flush().map_err(error)?);
        }
        let packet_bytes = rate as usize / 50 * 2;
        let mut bytes = std::mem::take(&mut state.pending);
        if input.is_none() && !bytes.is_empty() {
            bytes.resize(bytes.len().div_ceil(packet_bytes) * packet_bytes, 0);
        }
        let remainder = bytes.len() % packet_bytes;
        state.pending = bytes.split_off(bytes.len() - remainder);
        bytes
            .chunks_exact(packet_bytes)
            .map(|chunk| AudioRawFrame::new(chunk.to_vec(), rate, 1).map_err(error))
            .collect()
    }
    fn clear(&self) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| error("audio state poisoned"))?;
        state.resampler = None;
        state.pending.clear();
        self.peer.interrupt_audio();
        Ok(())
    }
    pub async fn frame(&self, frame: &Frame) -> Result<(), ProcessorError> {
        let audio = match &frame.data {
            FrameData::OutputTransportMessage(value)
            | FrameData::OutputTransportMessageUrgent(value) => {
                self.peer.send_app_message(value).await.map_err(error)?;
                return Ok(());
            }
            FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) => {
                self.audio(Some(audio))?
            }
            FrameData::TtsStopped(_) => self.audio(None)?,
            FrameData::Interruption
            | FrameData::InterruptionWorker
            | FrameData::Cancel(_)
            | FrameData::End(_) => {
                self.clear()?;
                return Ok(());
            }
            _ => return Ok(()),
        };
        for packet in audio {
            self.peer.send_audio(packet).await.map_err(error)?;
        }
        Ok(())
    }
}
fn error(error: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::new(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn packetizer_preserves_chunk_independence_flushes_tail_and_clears_interruptions() {
        let peer = Arc::new(
            SmallWebRtcConnection::new(SmallWebRtcConfig {
                localhost_only: true,
                ..Default::default()
            })
            .await
            .unwrap(),
        );
        let output = AudioOutput::new(peer.clone());
        let bytes: Vec<u8> = (0..1441)
            .flat_map(|i| ((i % 100) as i16 * 100).to_le_bytes())
            .collect();
        let mut expected_filter = SincResampler::new(24000, 48000, 1).unwrap();
        let mut expected = expected_filter.process(&bytes).unwrap();
        expected.extend(expected_filter.flush().unwrap());
        expected.resize(expected.len().div_ceil(1920) * 1920, 0);
        let mut actual = Vec::new();
        for chunk in bytes.chunks(222) {
            actual.extend(
                output
                    .audio(Some(&AudioRawFrame::new(chunk.to_vec(), 24000, 1).unwrap()))
                    .unwrap(),
            );
        }
        actual.extend(output.audio(None).unwrap());
        assert!(actual
            .iter()
            .all(|a| a.num_frames() == 960 && a.sample_rate == 48000 && a.num_channels == 1));
        assert_eq!(
            actual.into_iter().flat_map(|a| a.audio).collect::<Vec<_>>(),
            expected
        );
        assert!(output.audio(None).unwrap().is_empty());
        assert!(output
            .audio(Some(&AudioRawFrame::new(vec![1; 100], 24000, 1).unwrap()))
            .unwrap()
            .is_empty());
        output.clear().unwrap();
        assert!(output.audio(None).unwrap().is_empty());
        let fresh = output
            .audio(Some(&AudioRawFrame::new(vec![0; 1920], 48000, 1).unwrap()))
            .unwrap();
        assert_eq!(fresh.len(), 1);
        assert_eq!(fresh[0].audio, vec![0; 1920]);
        assert!(output
            .audio(Some(&AudioRawFrame::new(vec![0; 1920], 48000, 2).unwrap()))
            .is_err());
        peer.close().await.unwrap();
    }
}

#[cfg(test)]
#[path = "webrtc_cleanup_tests.rs"]
mod cleanup_tests;
