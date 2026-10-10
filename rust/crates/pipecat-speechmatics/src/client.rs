// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    common::{self, EventStream, Socket},
    protocol::{self, WireEvent},
    *,
};
use futures_util::StreamExt;
use pipecat_core::AudioRawFrame;
use serde_json::{json, Map, Value};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, PartialEq)]
pub struct SpeechmaticsSttConfig {
    pub endpoint: String,
    pub api_key: String,
    pub settings: SpeechmaticsSttSettings,
    pub should_interrupt: bool,
    pub ttfs_p99_latency: f64,
    pub max_transcript_bytes: usize,
    pub limits: StreamingLimits,
}
impl std::fmt::Debug for SpeechmaticsSttConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpeechmaticsSttConfig")
            .field("api_key", &"[redacted]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl SpeechmaticsSttConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://eu2.rt.speechmatics.com/v2/agent".into(),
            api_key: api_key.into(),
            settings: SpeechmaticsSttSettings::default(),
            should_interrupt: true,
            ttfs_p99_latency: 0.74,
            max_transcript_bytes: 128 * 1024,
            limits: StreamingLimits::default(),
        }
    }
    pub fn validate(&self) -> Result<(), StreamingError> {
        self.limits.validate()?;
        let url = common::endpoint(&self.endpoint)?;
        self.settings.validate()?;
        if self.api_key.is_empty()
            || self.api_key.len() > 8192
            || self.api_key.chars().any(|c| c.is_control())
            || !(1..=1024 * 1024).contains(&self.max_transcript_bytes)
            || !self.ttfs_p99_latency.is_finite()
            || self.ttfs_p99_latency < 0.0
            || self.ttfs_p99_latency > 300.0
        {
            return Err(StreamingError::Invalid("Speechmatics configuration"));
        }
        common::request(
            url.as_str(),
            &[("authorization", &format!("Bearer {}", self.api_key))],
        )?;
        if self.settings.start_message()?.to_string().len() > self.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionLifecycle {
    Connecting,
    Active,
    Draining,
    Closed,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq)]
pub struct SessionSnapshot {
    pub lifecycle: SessionLifecycle,
    pub sent_seq: u64,
    pub acked_seq: u64,
    pub audio_bytes: u64,
    pub session_id: Option<String>,
    pub request_id: String,
}
#[derive(Debug, Clone)]
pub struct SpeechmaticsSttClient {
    config: SpeechmaticsSttConfig,
}
impl SpeechmaticsSttClient {
    pub fn new(config: SpeechmaticsSttConfig) -> Result<Self, StreamingError> {
        config.validate()?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &SpeechmaticsSttConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<SpeechmaticsSttSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<SpeechmaticsSttSession, StreamingError> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let url = common::endpoint(&self.config.endpoint)?;
        let request = common::request(
            url.as_str(),
            &[
                ("authorization", &format!("Bearer {}", self.config.api_key)),
                ("x-request-id", &request_id),
            ],
        )?;
        let mut socket =
            common::cancelled(&cancel, common::connect(request, &self.config.limits)).await?;
        let startup = common::cancelled(&cancel, async {
            common::write(
                &mut socket,
                Message::Text(self.config.settings.start_message()?.to_string().into()),
                &self.config.limits,
            )
            .await?;
            tokio::time::timeout(self.config.limits.ready_timeout, async {
                loop {
                    match socket.next().await {
                        Some(Ok(Message::Text(text))) => {
                            match protocol::parse(&text, self.config.max_transcript_bytes)? {
                                WireEvent::Ready(id) => return Ok(id),
                                WireEvent::Error(kind) => {
                                    return Err(StreamingError::Provider { kind })
                                }
                                WireEvent::End => {
                                    return Err(StreamingError::Protocol("end before recognition"))
                                }
                                WireEvent::Warning | WireEvent::Info | WireEvent::Unknown => (),
                                _ => {
                                    return Err(StreamingError::Protocol(
                                        "event before recognition",
                                    ))
                                }
                            }
                        }
                        Some(Ok(Message::Ping(data))) => {
                            common::write(&mut socket, Message::Pong(data), &self.config.limits)
                                .await?
                        }
                        Some(Ok(Message::Pong(_))) => (),
                        Some(Ok(Message::Close(_))) | None => return Err(StreamingError::Closed),
                        Some(Err(e)) => return Err(common::transport(e)),
                        _ => return Err(StreamingError::Protocol("binary provider message")),
                    }
                }
            })
            .await
            .map_err(|_| StreamingError::Timeout("recognition ready"))?
        })
        .await;
        let session_id = match startup {
            Ok(id) => id,
            Err(e) => {
                common::close(&mut socket, &self.config.limits).await;
                return Err(e);
            }
        };
        let (tx, commands) = mpsc::channel(self.config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let (status_tx, status) = watch::channel(SessionSnapshot {
            lifecycle: SessionLifecycle::Active,
            sent_seq: 0,
            acked_seq: 0,
            audio_bytes: 0,
            session_id: session_id.clone(),
            request_id: request_id.clone(),
        });
        let gate = Arc::new(Mutex::new(Gate {
            finished: false,
            settings: self.config.settings.clone(),
            admitted_bytes: 0,
            last_finalize_bytes: 0,
        }));
        let turn = Arc::new(AtomicU64::new(0));
        let sender = SpeechmaticsSttSender {
            tx,
            cancel: cancel.clone(),
            gate,
            bytes: Arc::new(AtomicUsize::new(0)),
            turn: turn.clone(),
            config: self.config.clone(),
            status,
        };
        let worker_cancel = cancel.clone();
        let config = self.config.clone();
        let limits = config.limits.clone();
        let task = tokio::spawn(async move {
            let result = common::cancelled(&worker_cancel, async {
                emit(
                    &events,
                    SpeechmaticsSttEvent::Ready {
                        session_id,
                        request_id,
                    },
                    &limits,
                )
                .await?;
                if config.settings.locale_falls_back() {
                    emit(&events, SpeechmaticsSttEvent::LocaleFallback, &limits).await?;
                }
                run(&mut socket, commands, &events, config, &status_tx, &turn).await
            })
            .await;
            status_tx.send_modify(|s| {
                s.lifecycle = match result {
                    Ok(()) => SessionLifecycle::Closed,
                    Err(StreamingError::Cancelled) => SessionLifecycle::Cancelled,
                    Err(_) => SessionLifecycle::Failed,
                }
            });
            common::close(&mut socket, &limits).await;
            result
        });
        Ok(SpeechmaticsSttSession {
            sender,
            events: EventStream {
                rx,
                cancel,
                task,
                ended: false,
            },
        })
    }
}
struct Gate {
    finished: bool,
    settings: SpeechmaticsSttSettings,
    admitted_bytes: u64,
    last_finalize_bytes: u64,
}
struct AudioBudget {
    bytes: Arc<AtomicUsize>,
    len: usize,
}
impl Drop for AudioBudget {
    fn drop(&mut self) {
        self.bytes.fetch_sub(self.len, Ordering::AcqRel);
    }
}
enum Command {
    Audio(AudioRawFrame, AudioBudget),
    Finalize { bytes: u64, turn: u64 },
    StartTurn,
    Format(String),
    GetSpeakers(bool),
    Finish,
}
#[derive(Clone)]
pub struct SpeechmaticsSttSender {
    tx: mpsc::Sender<Command>,
    cancel: CancellationToken,
    gate: Arc<Mutex<Gate>>,
    bytes: Arc<AtomicUsize>,
    turn: Arc<AtomicU64>,
    config: SpeechmaticsSttConfig,
    status: watch::Receiver<SessionSnapshot>,
}
impl SpeechmaticsSttSender {
    pub fn settings_snapshot(&self) -> Result<SpeechmaticsSttSettings, StreamingError> {
        self.gate
            .lock()
            .map(|s| s.settings.clone())
            .map_err(|_| StreamingError::Closed)
    }
    pub fn snapshot(&self) -> SessionSnapshot {
        self.status.borrow().clone()
    }
    pub fn turn_generation(&self) -> u64 {
        self.turn.load(Ordering::Acquire)
    }
    pub fn validate_audio(&self, audio: &AudioRawFrame) -> Result<(), StreamingError> {
        if audio.sample_rate != 16000 || audio.num_channels != 1 {
            return Err(StreamingError::Invalid(
                "only mono 16 kHz PCM16 is supported",
            ));
        }
        if !audio.audio.len().is_multiple_of(2) {
            return Err(StreamingError::PartialSample);
        }
        if audio.audio.is_empty() {
            return Err(StreamingError::Invalid("empty PCM audio"));
        }
        if audio.audio.len() > self.config.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    fn check(&self, gate: &Gate) -> Result<(), StreamingError> {
        if self.cancel.is_cancelled() {
            Err(StreamingError::Cancelled)
        } else if gate.finished || self.tx.is_closed() {
            Err(StreamingError::Closed)
        } else {
            Ok(())
        }
    }
    fn try_command(&self, command: Command) -> Result<(), StreamingError> {
        self.tx.try_send(command).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => StreamingError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => StreamingError::Closed,
        })
    }
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate_audio(&audio)?;
        let mut gate = self.gate.lock().map_err(|_| StreamingError::Closed)?;
        self.check(&gate)?;
        let len = audio.audio.len();
        let total = gate
            .admitted_bytes
            .checked_add(len as u64)
            .ok_or(StreamingError::TooLarge)?;
        if self.bytes.load(Ordering::Acquire).saturating_add(len)
            > self.config.limits.max_buffered_audio_bytes
        {
            return Err(StreamingError::Backpressure);
        }
        self.bytes.fetch_add(len, Ordering::AcqRel);
        let budget = AudioBudget {
            bytes: self.bytes.clone(),
            len,
        };
        self.try_command(Command::Audio(audio, budget))?;
        gate.admitted_bytes = total;
        Ok(())
    }
    /// Nonblocking admission is deliberate: audio pressure is surfaced, never silently dropped or replayed.
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.try_send_audio(audio)
    }
    pub fn try_finalize(&self) -> Result<(), StreamingError> {
        let mut gate = self.gate.lock().map_err(|_| StreamingError::Closed)?;
        self.check(&gate)?;
        if gate.settings.turn_detection_mode != TurnDetectionMode::External {
            return Err(StreamingError::Invalid("finalize requires external mode"));
        }
        if gate.admitted_bytes == 0 || gate.admitted_bytes == gate.last_finalize_bytes {
            return Err(StreamingError::Invalid("finalize requires new audio"));
        }
        self.try_command(Command::Finalize {
            bytes: gate.admitted_bytes,
            turn: self.turn_generation(),
        })?;
        gate.last_finalize_bytes = gate.admitted_bytes;
        Ok(())
    }
    pub async fn finalize(&self) -> Result<(), StreamingError> {
        self.try_finalize()
    }
    pub fn try_start_turn(&self) -> Result<(), StreamingError> {
        let gate = self.gate.lock().map_err(|_| StreamingError::Closed)?;
        self.check(&gate)?;
        let next = self
            .turn_generation()
            .checked_add(1)
            .ok_or(StreamingError::TooLarge)?;
        self.try_command(Command::StartTurn)?;
        self.turn.store(next, Ordering::Release);
        Ok(())
    }
    pub fn try_get_speakers(&self, final_result: bool) -> Result<(), StreamingError> {
        let gate = self.gate.lock().map_err(|_| StreamingError::Closed)?;
        self.check(&gate)?;
        if !gate.settings.enable_diarization {
            return Err(StreamingError::Invalid("speaker diarization is disabled"));
        }
        self.try_command(Command::GetSpeakers(final_result))
    }
    pub fn merge_settings(&self, delta: &Map<String, Value>) -> Result<(), StreamingError> {
        let mut gate = self.gate.lock().map_err(|_| StreamingError::Closed)?;
        self.check(&gate)?;
        let next = gate.settings.merge(delta)?;
        if next == gate.settings {
            return Ok(());
        }
        let mut wire = next.clone();
        wire.speaker_active_format = gate.settings.speaker_active_format.clone();
        if wire != gate.settings {
            return Err(StreamingError::RequiresRestart);
        }
        self.try_command(Command::Format(next.speaker_active_format.clone()))?;
        gate.settings = next;
        Ok(())
    }
    /// Atomically admits one EndOfStream after earlier commands. Wait on session.next() for terminal outcome.
    pub async fn finish(&self) -> Result<(), StreamingError> {
        {
            let gate = self.gate.lock().map_err(|_| StreamingError::Closed)?;
            self.check(&gate)?;
        }
        let permit = common::cancelled(&self.cancel, async {
            tokio::time::timeout(self.config.limits.write_timeout, self.tx.reserve())
                .await
                .map_err(|_| StreamingError::Timeout("finish admission"))?
                .map_err(|_| StreamingError::Closed)
        })
        .await?;
        let mut gate = self.gate.lock().map_err(|_| StreamingError::Closed)?;
        self.check(&gate)?;
        permit.send(Command::Finish);
        gate.finished = true;
        Ok(())
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub fn is_closed(&self) -> bool {
        self.cancel.is_cancelled()
            || self.tx.is_closed()
            || self.gate.lock().map_or(true, |s| s.finished)
    }
}
pub struct SpeechmaticsSttSession {
    sender: SpeechmaticsSttSender,
    events: EventStream<SpeechmaticsSttEvent>,
}
impl SpeechmaticsSttSession {
    pub fn sender(&self) -> SpeechmaticsSttSender {
        self.sender.clone()
    }
    pub async fn next(&mut self) -> Option<Result<SpeechmaticsSttEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.sender.cancel();
    }
}
async fn emit(
    events: &mpsc::Sender<SpeechmaticsSttEvent>,
    event: SpeechmaticsSttEvent,
    limits: &StreamingLimits,
) -> Result<(), StreamingError> {
    tokio::time::timeout(limits.output_timeout, events.send(event))
        .await
        .map_err(|_| StreamingError::Timeout("event output"))?
        .map_err(|_| StreamingError::Closed)
}
struct Reducer {
    draining: bool,
    sent: u64,
    acked: u64,
    bytes: u64,
    pending: Option<u64>,
    settings: SpeechmaticsSttSettings,
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<SpeechmaticsSttEvent>,
    config: SpeechmaticsSttConfig,
    status: &watch::Sender<SessionSnapshot>,
    turn: &AtomicU64,
) -> Result<(), StreamingError> {
    let mut state = Reducer {
        draining: false,
        sent: 0,
        acked: 0,
        bytes: 0,
        pending: None,
        settings: config.settings.clone(),
    };
    loop {
        tokio::select! {
            command=commands.recv()=>match command{
                Some(Command::Audio(audio,_budget))=>{let sent=state.sent.checked_add(1).ok_or(StreamingError::TooLarge)?;let bytes=state.bytes.checked_add(audio.audio.len() as u64).ok_or(StreamingError::TooLarge)?;let seconds=audio.audio.len() as f64/32000.0;common::write(socket,Message::Binary(audio.audio.into()),&config.limits).await?;state.sent=sent;state.bytes=bytes;status.send_modify(|s|{s.sent_seq=sent;s.audio_bytes=bytes;});emit(events,SpeechmaticsSttEvent::AudioSent{seconds,sent_seq:sent,audio_bytes:bytes},&config.limits).await?;}
                Some(Command::Finalize{bytes,turn:epoch})=>{if bytes!=state.bytes{return Err(StreamingError::Protocol("finalize audio boundary"));}common::write(socket,Message::Text(json!({"message":"ForceEndOfUtterance","timestamp":state.bytes as f64/32000.0}).to_string().into()),&config.limits).await?;state.pending=Some(epoch);}
                Some(Command::StartTurn)=>state.pending=None,
                Some(Command::Format(format))=>state.settings.speaker_active_format=format,
                Some(Command::GetSpeakers(final_result))=>common::write(socket,Message::Text(json!({"message":"GetSpeakers","final":final_result}).to_string().into()),&config.limits).await?,
                Some(Command::Finish)=>{
                    state.draining = true;
                    status.send_modify(|s|s.lifecycle=SessionLifecycle::Draining);
                    return tokio::time::timeout(config.limits.response_timeout,async{
                        common::write(socket,Message::Text(json!({"message":"EndOfStream","last_seq_no":state.sent}).to_string().into()),&config.limits).await?;
                        loop{let message=socket.next().await; if receive(message,socket,events,&config,&mut state,status,turn).await?{return Ok(());}}
                    }).await.map_err(|_|StreamingError::Timeout("response drain"))?;
                }
                None=>return Err(StreamingError::Closed),
            },
            message=socket.next()=>{if receive(message,socket,events,&config,&mut state,status,turn).await?{return Err(StreamingError::Protocol("end before finish"));}},
        }
    }
}
async fn receive(
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    socket: &mut Socket,
    events: &mpsc::Sender<SpeechmaticsSttEvent>,
    config: &SpeechmaticsSttConfig,
    state: &mut Reducer,
    status: &watch::Sender<SessionSnapshot>,
    turn: &AtomicU64,
) -> Result<bool, StreamingError> {
    let wire = match message {
        Some(Ok(Message::Text(text))) => protocol::parse(&text, config.max_transcript_bytes)?,
        Some(Ok(Message::Ping(data))) => {
            common::write(socket, Message::Pong(data), &config.limits).await?;
            return Ok(false);
        }
        Some(Ok(Message::Pong(_))) => return Ok(false),
        Some(Ok(Message::Close(_))) | None => return Err(StreamingError::Closed),
        Some(Err(e)) => return Err(common::transport(e)),
        _ => return Err(StreamingError::Protocol("binary provider message")),
    };
    let event = match wire {
        WireEvent::Ready(_) => {
            return Err(StreamingError::Protocol(
                "duplicate recognition acknowledgment",
            ))
        }
        WireEvent::Error(kind) => return Err(StreamingError::Provider { kind }),
        WireEvent::End => {
            if !state.draining {
                return Err(StreamingError::Protocol("end before finish"));
            }
            emit(
                events,
                SpeechmaticsSttEvent::EndOfTranscript,
                &config.limits,
            )
            .await?;
            return Ok(true);
        }
        WireEvent::AudioAdded(seq) => {
            if seq > state.sent {
                return Err(StreamingError::Protocol(
                    "acknowledgment exceeds sent audio",
                ));
            }
            state.acked = state.acked.max(seq);
            status.send_modify(|s| s.acked_seq = state.acked);
            SpeechmaticsSttEvent::AudioAdded {
                acked_seq: state.acked,
            }
        }
        WireEvent::Transcript {
            text,
            speaker,
            start,
            end,
            is_final,
        } => {
            if text.is_empty() {
                return Ok(false);
            }
            let epoch = turn.load(Ordering::Acquire);
            let finalized = is_final && state.pending == Some(epoch);
            if is_final {
                state.pending = None;
            }
            let text = state.settings.format_text(
                &text,
                speaker.as_deref(),
                &format!("{start:?}"),
                config.max_transcript_bytes,
            )?;
            SpeechmaticsSttEvent::Transcript(SpeechmaticsTranscript {
                text,
                speaker,
                language: state.settings.language.clone(),
                start_time: start,
                end_time: end,
                is_final,
                finalized,
                turn_generation: epoch,
            })
        }
        WireEvent::TurnStarted(time) => SpeechmaticsSttEvent::TurnStarted { time },
        WireEvent::TurnStopped(time) => SpeechmaticsSttEvent::TurnStopped { time },
        WireEvent::SpeechStarted(time) => SpeechmaticsSttEvent::SpeechStarted { time },
        WireEvent::SpeechEnded(time) => SpeechmaticsSttEvent::SpeechEnded { time },
        WireEvent::Speakers(v) => SpeechmaticsSttEvent::Speakers(v),
        WireEvent::Warning => SpeechmaticsSttEvent::Warning,
        WireEvent::Info => SpeechmaticsSttEvent::Info,
        WireEvent::Unknown => return Ok(false),
    };
    emit(events, event, &config.limits).await?;
    Ok(false)
}
