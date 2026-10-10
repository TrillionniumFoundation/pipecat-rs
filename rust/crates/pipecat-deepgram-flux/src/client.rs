// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    common::{self, EventStream, Input, Socket},
    protocol::{self, Incoming},
    CancellationToken, FluxConfig, FluxModel, FluxSettings, FluxTurn, StreamingError, TurnEvent,
};
use futures_util::StreamExt;
use pipecat_core::AudioRawFrame;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, PartialEq)]
pub enum FluxEvent {
    Turn {
        turn: FluxTurn,
        model: FluxModel,
        min_confidence: Option<f64>,
    },
    /// The old connection is closed; any outstanding eager response must be revoked.
    Reset {
        interrupted_turn: bool,
    },
    Configured,
    ConfigureRejected,
    /// Provider-consumed silence, included in usage accounting.
    WatchdogAudio {
        seconds: f64,
    },
    AudioSent {
        seconds: f64,
    },
}
#[derive(Debug, Clone)]
pub struct FluxClient {
    config: FluxConfig,
}
impl FluxClient {
    pub fn new(mut config: FluxConfig) -> Result<Self, StreamingError> {
        config.normalize();
        let url = config.url()?;
        common::request(
            &url,
            &[("authorization", &format!("Token {}", config.api_key))],
        )?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &FluxConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<FluxSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<FluxSession, StreamingError> {
        let mut socket = common::cancelled(&cancel, open(&self.config)).await?;
        let config = self.config.clone();
        let (tx, commands) = mpsc::channel(config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(config.limits.queue_capacity);
        let input = Input {
            tx,
            cancel: cancel.clone(),
            limits: config.limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        };
        let sender = FluxSender {
            input,
            sample_rate: config.sample_rate,
            settings: Arc::new(Mutex::new(config.settings.clone())),
        };
        let worker_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let result =
                common::cancelled(&worker_cancel, run(&mut socket, commands, &events, &config))
                    .await;
            common::close(&mut socket, &config.limits).await;
            result
        });
        Ok(FluxSession {
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
#[derive(Clone)]
pub struct FluxSender {
    input: Input<Command>,
    sample_rate: u32,
    settings: Arc<Mutex<FluxSettings>>,
}
impl FluxSender {
    pub(crate) fn validate_audio(&self, audio: &AudioRawFrame) -> Result<(), StreamingError> {
        audio
            .validate()
            .map_err(|_| StreamingError::PartialSample)?;
        if audio.sample_rate != self.sample_rate || audio.num_channels != 1 {
            return Err(StreamingError::Invalid(
                "Flux requires configured-rate mono linear16",
            ));
        }
        if audio.audio.len() > self.input.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate_audio(&audio)?;
        self.input.send(Command::Audio(audio), false).await
    }
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate_audio(&audio)?;
        self.input.try_send(Command::Audio(audio), false)
    }
    /// Queue one complete desired state atomically. Rejected queue admission does not
    /// mutate the settings snapshot. Provider rejection is reported as an event.
    pub fn update_settings(&self, settings: FluxSettings) -> Result<(), StreamingError> {
        settings.validate()?;
        let mut current = self.settings.lock().map_err(|_| StreamingError::Closed)?;
        self.input
            .try_send(Command::Settings(settings.clone()), false)?;
        *current = settings;
        Ok(())
    }
    pub fn merge_settings(
        &self,
        delta: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<(), StreamingError> {
        let mut current = self.settings.lock().map_err(|_| StreamingError::Closed)?;
        let updated = current.merge(delta)?;
        self.input
            .try_send(Command::Settings(updated.clone()), false)?;
        *current = updated;
        Ok(())
    }
    pub fn settings(&self) -> Result<FluxSettings, StreamingError> {
        self.settings
            .lock()
            .map(|v| v.clone())
            .map_err(|_| StreamingError::Closed)
    }
    pub async fn finish(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finish, true).await
    }
    pub fn cancel(&self) {
        self.input.cancel.cancel();
    }
    pub fn is_closed(&self) -> bool {
        self.input.tx.is_closed() || self.input.cancel.is_cancelled()
    }
}
pub struct FluxSession {
    sender: FluxSender,
    events: EventStream<FluxEvent>,
}
impl FluxSession {
    pub fn sender(&self) -> FluxSender {
        self.sender.clone()
    }
    pub fn cancel(&self) {
        self.sender.cancel();
    }
    pub async fn next(&mut self) -> Option<Result<FluxEvent, StreamingError>> {
        self.events.next().await
    }
}
enum Command {
    Audio(AudioRawFrame),
    Settings(FluxSettings),
    Finish,
}

async fn open(config: &FluxConfig) -> Result<Socket, StreamingError> {
    let request = common::request(
        &config.url()?,
        &[("authorization", &format!("Token {}", config.api_key))],
    )?;
    let mut socket = common::connect(request, &config.limits).await?;
    let ready = tokio::time::timeout(config.ready_timeout, async {
        loop {
            match receive(&mut socket, config).await? {
                Incoming::Connected => return Ok(()),
                Incoming::Unknown => (),
                _ => {
                    return Err(StreamingError::Protocol(
                        "expected Connected before provider events",
                    ))
                }
            }
        }
    })
    .await
    .map_err(|_| StreamingError::Timeout("Flux Connected"))?;
    if let Err(error) = ready {
        common::close(&mut socket, &config.limits).await;
        return Err(error);
    }
    Ok(socket)
}
async fn receive(socket: &mut Socket, config: &FluxConfig) -> Result<Incoming, StreamingError> {
    loop {
        match socket.next().await {
            Some(Ok(Message::Text(text))) => {
                return protocol::parse(&text, config.limits.max_message_bytes)
            }
            Some(Ok(Message::Ping(bytes))) => {
                common::write(socket, Message::Pong(bytes), &config.limits).await?
            }
            Some(Ok(Message::Pong(_))) => (),
            Some(Ok(Message::Close(_))) | None => return Err(StreamingError::Closed),
            Some(Ok(_)) => return Err(StreamingError::Protocol("expected text provider response")),
            Some(Err(error)) => return Err(common::transport(error)),
        }
    }
}
async fn publish(
    events: &mpsc::Sender<FluxEvent>,
    event: FluxEvent,
    config: &FluxConfig,
) -> Result<(), StreamingError> {
    tokio::time::timeout(config.limits.write_timeout, events.send(event))
        .await
        .map_err(|_| StreamingError::Timeout("event queue"))?
        .map_err(|_| StreamingError::Closed)
}
async fn json_write(
    socket: &mut Socket,
    value: serde_json::Value,
    config: &FluxConfig,
) -> Result<(), StreamingError> {
    let text = value.to_string();
    if text.len() > config.limits.max_message_bytes {
        return Err(StreamingError::TooLarge);
    }
    common::write(socket, Message::Text(text.into()), &config.limits).await
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<FluxEvent>,
    initial: &FluxConfig,
) -> Result<(), StreamingError> {
    let mut config = initial.clone();
    let mut desired = config.settings.clone();
    let mut sent = desired.clone();
    let mut in_flight: Option<Instant> = None;
    let mut pending = false;
    let mut reconnect = false;
    let mut active_turn: Option<u64> = None;
    let mut completed_turn: Option<u64> = None;
    let mut last_audio: Option<Instant> = None;
    let mut chunk_duration = Duration::ZERO;
    let mut closing: Option<Instant> = None;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        if reconnect && active_turn.is_none() && closing.is_none() {
            publish(
                events,
                FluxEvent::Reset {
                    interrupted_turn: false,
                },
                &config,
            )
            .await?;
            json_write(socket, json!({"type":"CloseStream"}), &config).await?;
            common::close(socket, &config.limits).await;
            config.settings = desired.clone();
            *socket = open(&config).await?;
            sent = desired.clone();
            in_flight = None;
            pending = false;
            reconnect = false;
            last_audio = None;
            completed_turn = None;
        }
        if pending && in_flight.is_none() && !reconnect && closing.is_none() {
            if let Some(message) = sent.configure_delta(&desired) {
                json_write(socket, message, &config).await?;
                in_flight = Some(Instant::now());
            }
            sent = desired.clone();
            pending = false;
        }
        tokio::select! {
            // Poll both input and output fairly: provider bursts cannot starve audio.
            incoming=receive(socket,&config)=>match incoming {
                Ok(Incoming::Turn(turn))=>{
                    if completed_turn.is_some_and(|last|turn.turn_index<=last) {continue;}
                    match turn.event {
                        TurnEvent::StartOfTurn=>{
                            if active_turn==Some(turn.turn_index){continue;}
                            if active_turn.is_some(){return Err(StreamingError::Protocol("overlapping Flux turns"));}
                            active_turn=Some(turn.turn_index);
                        },
                        _ if active_turn!=Some(turn.turn_index)=>return Err(StreamingError::Protocol("TurnInfo does not belong to the active turn")),
                        TurnEvent::EndOfTurn=>{active_turn=None;completed_turn=Some(turn.turn_index);},
                        _=>(),
                    }
                    publish(events,FluxEvent::Turn {turn,model:config.settings.model,min_confidence:desired.min_confidence},&config).await?;
                },
                Ok(Incoming::ConfigureSuccess)|Ok(Incoming::ConfigureFailure)=>{
                    let accepted=matches!(incoming,Ok(Incoming::ConfigureSuccess));
                    if in_flight.take().is_some() {publish(events,if accepted {FluxEvent::Configured}else{FluxEvent::ConfigureRejected},&config).await?;}
                },
                Ok(Incoming::Connected)=>return Err(StreamingError::Protocol("duplicate Connected")),
                Ok(Incoming::Unknown)=>(),
                Err(StreamingError::Closed) if closing.is_some()=>return Ok(()),
                Err(error)=>return Err(error),
            },
            command=commands.recv(),if closing.is_none()=>match command {
                Some(Command::Audio(audio))=>{
                    if audio.audio.is_empty(){continue;}
                    common::write(socket,Message::Binary(audio.audio.to_vec().into()),&config.limits).await?;
                    chunk_duration=Duration::from_secs_f64(audio.duration_seconds());last_audio=Some(Instant::now());
                    publish(events,FluxEvent::AudioSent{seconds:audio.duration_seconds()},&config).await?;
                },
                Some(Command::Settings(settings))=>{
                    reconnect=config.settings.reconnects(&settings);
                    desired=settings;pending=true;
                },
                Some(Command::Finish)|None=>{
                    json_write(socket,json!({"type":"CloseStream"}),&config).await?;
                    closing=Some(Instant::now());
                },
            },
            _=tick.tick()=>{
                if closing.is_some_and(|time|time.elapsed()>=config.limits.response_timeout){return Err(StreamingError::Timeout("Flux drain"));}
                if closing.is_some(){continue;}
                // Configure acknowledgements have no request id. Rotate the socket
                // after a missing ack instead of misattributing a delayed old ack.
                if in_flight.is_some_and(|time|time.elapsed()>=config.configure_ack_timeout){
                    publish(events,FluxEvent::Reset { interrupted_turn: active_turn.is_some() },&config).await?;
                    json_write(socket,json!({"type":"CloseStream"}),&config).await?;
                    common::close(socket,&config.limits).await;
                    config.settings=desired.clone();*socket=open(&config).await?;
                    sent=desired.clone();in_flight=None;pending=false;reconnect=false;last_audio=None;active_turn=None;completed_turn=None;
                    continue;
                }
                let threshold=config.watchdog_min_timeout.max(chunk_duration.saturating_mul(2));
                if active_turn.is_some() && last_audio.is_some_and(|time|time.elapsed()>threshold){
                    let bytes=(config.sample_rate as usize / 2) * 2; // 500 ms, mono, signed 16-bit PCM.
                    for chunk in vec![0u8;bytes].chunks(config.limits.max_message_bytes.max(2)&!1){
                        common::write(socket,Message::Binary(chunk.to_vec().into()),&config.limits).await?;
                    }
                    last_audio=Some(Instant::now());
                    publish(events,FluxEvent::WatchdogAudio{seconds:bytes as f64 / (config.sample_rate as f64 * 2.0)},&config).await?;
                }
            }
        }
    }
}
