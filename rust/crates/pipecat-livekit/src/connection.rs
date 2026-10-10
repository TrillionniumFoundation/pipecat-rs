use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use livekit::{
    options::TrackPublishOptions,
    prelude::*,
    webrtc::{
        audio_source::{native::NativeAudioSource, AudioSourceOptions, RtcAudioSource},
        audio_stream::native::NativeAudioStream,
    },
    DataPacket, DataPacketKind, SipDTMF,
};
use pipecat_core::{AudioRawFrame, KeypadEntry};
use tokio::{
    sync::{mpsc, oneshot, watch, Mutex},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

use crate::protocol::{dtmf_code, dtmf_digit, from_sdk_audio, to_sdk_audio};
use crate::{
    ConnectionState, Error, LiveKitConfig, LiveKitEvent, ParticipantInfo, Result, SourceAccepted,
};

/// A single-use session owning the room event loop and all audio-reader tasks.
/// Call `close().await` for verified teardown. Drop requests bounded best-effort close.
pub struct LiveKitConnection {
    room: Arc<Room>,
    config: LiveKitConfig,
    source: Option<NativeAudioSource>,
    events: Mutex<mpsc::Receiver<LiveKitEvent>>,
    state: watch::Receiver<ConnectionState>,
    cancel: CancellationToken,
    audio: Option<crate::publisher::AudioPublisher>,
    owner: Mutex<Option<JoinHandle<Result<()>>>>,
}
impl LiveKitConnection {
    /// Join using an already-authorized participant JWT. No token is generated or persisted.
    pub async fn connect(
        url: &str,
        token: &str,
        config: LiveKitConfig,
        cancel: CancellationToken,
    ) -> Result<Self> {
        config.validate()?;
        let parsed = url::Url::parse(url).map_err(|_| Error::InvalidConfig("invalid room URL"))?;
        if !matches!(parsed.scheme(), "ws" | "wss")
            || parsed.host_str().is_none()
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || token.trim().is_empty()
        {
            return Err(Error::InvalidConfig(
                "a ws/wss room URL and nonempty participant token are required",
            ));
        }
        let mut options = RoomOptions::default();
        options.auto_subscribe = false;
        options.join_retries = 0;
        options.connect_timeout = config.connect_timeout;
        let joined = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err(Error::Closed),
            result = timeout(config.connect_timeout, Room::connect(url, token, options)) => result.map_err(|_| Error::Timeout("room connect"))?.map_err(|error| Error::Sdk(error.to_string().replace(token, "[redacted]")))?,
        };
        let (room, room_events) = joined;
        let room = Arc::new(room);
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let (state_tx, state) = watch::channel(ConnectionState::Connecting);
        let (ready_tx, ready_rx) = oneshot::channel();
        let mut guard = SetupCancellation(Some(cancel.clone()));
        let mut owner = tokio::spawn(initialize_room(
            room.clone(),
            room_events,
            tx,
            state_tx,
            cancel.clone(),
            config.clone(),
            ready_tx,
        ));
        let readiness = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Error::Closed),
            ready = ready_rx => ready.map_err(|_| Error::Sdk("room initialization task stopped".into())).and_then(|ready| ready),
        };
        let ready = match readiness {
            Ok(ready) => ready,
            Err(error) => {
                cancel.cancel();
                // Await the existing owner; dropping this connect future leaves
                // the cancellation guard responsible for its bounded close.
                let cleanup = (&mut owner).await.map_err(sdk);
                return match cleanup {
                    Ok(Ok(())) => Err(error),
                    Ok(Err(cleanup)) => Err(cleanup),
                    Err(cleanup) => Err(cleanup),
                };
            }
        };
        guard.0 = None;
        Ok(Self {
            room,
            config,
            source: ready.source,
            events: Mutex::new(rx),
            state,
            cancel,
            audio: ready.publisher,
            owner: Mutex::new(Some(owner)),
        })
    }

    pub fn state(&self) -> ConnectionState {
        self.state.borrow().clone()
    }
    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.state.clone()
    }
    pub fn identity(&self) -> String {
        self.room.local_participant().identity().to_string()
    }
    pub fn room_name(&self) -> String {
        self.room.name()
    }
    pub fn participants(&self) -> Vec<ParticipantInfo> {
        let mut participants: Vec<_> = self
            .room
            .remote_participants()
            .values()
            .map(participant_info)
            .collect();
        participants.sort_by(|a, b| a.identity.cmp(&b.identity));
        participants
    }
    pub async fn recv(&self) -> Option<LiveKitEvent> {
        self.events.lock().await.recv().await
    }
    pub(crate) fn audio_generation(&self) -> u64 {
        self.audio
            .as_ref()
            .map_or(0, crate::publisher::AudioPublisher::generation)
    }
    pub fn config(&self) -> &LiveKitConfig {
        &self.config
    }
    fn ready(&self) -> Result<()> {
        if self.cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        match self.state() {
            ConnectionState::Connected => Ok(()),
            ConnectionState::Closed | ConnectionState::Failed(_) => Err(Error::Closed),
            _ => Err(Error::NotReady),
        }
    }
    /// Publishes exact-format PCM16-LE. The acknowledgement is native source acceptance.
    /// Cancellation or interruption clears pending source PCM, but cannot recall sent RTP.
    pub async fn publish_audio(&self, audio: &AudioRawFrame) -> Result<SourceAccepted> {
        let format = self.config.output_format.ok_or(Error::AudioDisabled)?;
        let frame = to_sdk_audio(audio, format, self.config.max_audio_frame_bytes)?;
        self.ready()?;
        let accepted = self
            .audio
            .as_ref()
            .ok_or(Error::AudioDisabled)?
            .publish(frame)
            .await?;
        self.ready()?;
        Ok(accepted)
    }
    pub fn interrupt_audio(&self) {
        if let Some(audio) = &self.audio {
            audio.interrupt();
        }
        if let Some(source) = &self.source {
            source.clear_buffer();
        }
    }

    pub async fn publish_data(
        &self,
        payload: Vec<u8>,
        destination: Option<&str>,
        topic: Option<String>,
        reliable: bool,
    ) -> Result<()> {
        self.ready()?;
        if payload.len() > self.config.max_data_bytes {
            return Err(Error::Limit("data payload bytes"));
        }
        if topic.as_ref().is_some_and(|s| s.len() > 256)
            || destination.is_some_and(|s| s.is_empty() || s.len() > 256)
        {
            return Err(Error::InvalidConfig(
                "topic and destination identities are limited to 256 bytes",
            ));
        }
        let packet = DataPacket {
            payload,
            topic,
            reliable,
            destination_identities: destination
                .map(|s| vec![s.to_owned().into()])
                .unwrap_or_default(),
        };
        let participant = self.room.local_participant();
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(Error::Closed),
            result = timeout(self.config.operation_timeout, participant.publish_data(packet)) => result.map_err(|_| Error::Timeout("data publication")).and_then(|r| r.map_err(sdk)),
        }
    }
    pub async fn publish_dtmf(&self, digit: KeypadEntry, destination: Option<&str>) -> Result<()> {
        self.ready()?;
        if destination.is_some_and(|s| s.is_empty() || s.len() > 256) {
            return Err(Error::InvalidConfig("invalid destination identity"));
        }
        let packet = SipDTMF {
            code: dtmf_code(digit),
            digit: digit.as_char().to_string(),
            destination_identities: destination
                .map(|s| vec![s.to_owned().into()])
                .unwrap_or_default(),
        };
        let participant = self.room.local_participant();
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(Error::Closed),
            result = timeout(self.config.operation_timeout, participant.publish_dtmf(packet)) => result.map_err(|_| Error::Timeout("DTMF publication")).and_then(|r| r.map_err(sdk)),
        }
    }
    pub async fn close(&self) -> Result<()> {
        self.interrupt_audio();
        self.cancel.cancel();
        let mut owned = self.owner.lock().await;
        if let Some(owner) = owned.as_mut() {
            let result = owner.await.map_err(sdk).and_then(|result| result);
            owned.take();
            result
        } else {
            let mut state = self.state.clone();
            loop {
                match state.borrow_and_update().clone() {
                    ConnectionState::Closed => return Ok(()),
                    ConnectionState::Failed(error) => return Err(Error::Sdk(error)),
                    _ => {}
                }
                state.changed().await.map_err(|_| Error::Closed)?;
            }
        }
    }
}
impl Drop for LiveKitConnection {
    fn drop(&mut self) {
        self.interrupt_audio();
        self.cancel.cancel();
    }
}
fn sdk(error: impl std::fmt::Display) -> Error {
    Error::Sdk(error.to_string())
}
fn participant_info(participant: &RemoteParticipant) -> ParticipantInfo {
    ParticipantInfo {
        identity: participant.identity().to_string(),
        sid: participant.sid().to_string(),
        name: participant.name(),
        metadata: participant.metadata(),
    }
}
fn emit(tx: &mpsc::Sender<LiveKitEvent>, event: LiveKitEvent) -> Result<()> {
    crate::protocol::validate_event(&event)?;
    tx.try_send(event).map_err(|e| match e {
        mpsc::error::TrySendError::Full(_) => Error::QueueFull,
        mpsc::error::TrySendError::Closed(_) => Error::Closed,
    })
}

struct AudioReader {
    identity: String,
    generation: u64,
    cancel: CancellationToken,
}
struct SetupCancellation(Option<CancellationToken>);
impl Drop for SetupCancellation {
    fn drop(&mut self) {
        if let Some(cancel) = &self.0 {
            cancel.cancel();
        }
    }
}
struct ReadyMedia {
    source: Option<NativeAudioSource>,
    publisher: Option<crate::publisher::AudioPublisher>,
}
async fn initialize_room(
    room: Arc<Room>,
    events: mpsc::UnboundedReceiver<RoomEvent>,
    tx: mpsc::Sender<LiveKitEvent>,
    state: watch::Sender<ConnectionState>,
    cancel: CancellationToken,
    config: LiveKitConfig,
    ready: oneshot::Sender<Result<ReadyMedia>>,
) -> Result<()> {
    let setup = async {
        let source = if let Some(format) = config.output_format {
            let source = NativeAudioSource::new(AudioSourceOptions::default(), format.sample_rate(), format.channels().into(), config.source_queue_ms);
            let track = LocalAudioTrack::create_audio_track("pipecat-audio", RtcAudioSource::Native(source.clone()));
            let participant = room.local_participant();
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Error::Closed),
                result = timeout(config.operation_timeout, participant.publish_track(LocalTrack::Audio(track), TrackPublishOptions { source: TrackSource::Microphone, ..Default::default() })) => { result.map_err(|_| Error::Timeout("audio track publication"))?.map_err(sdk)?; }
            }
            Some(source)
        } else { None };
        if cancel.is_cancelled() { return Err(Error::Closed); }
        emit(&tx, LiveKitEvent::Connected { identity: room.local_participant().identity().to_string(), room: room.name() })?;
        Ok(source)
    }.await;
    let source = match setup {
        Ok(source) => source,
        Err(error) => {
            let closed = close_room(&room, config.close_timeout).await;
            let message = match closed {
                Ok(()) => error.to_string(),
                Err(cleanup) => format!("{error}; rollback: {cleanup}"),
            };
            state.send_replace(ConnectionState::Failed(message.clone()));
            let _ = ready.send(Err(Error::Sdk(message.clone())));
            return Err(Error::Sdk(message));
        }
    };
    let audio = source.clone().map(|source| {
        let (publisher, receiver) = crate::publisher::AudioPublisher::channel(
            config.queue_capacity,
            cancel.clone(),
            config.operation_timeout,
        );
        let task = tokio::spawn(crate::publisher::run_publisher(
            source,
            receiver,
            publisher.subscribe(),
            cancel.clone(),
            config.source_queue_ms,
            config.operation_timeout,
        ));
        (publisher, task)
    });
    let (publisher, task) = match audio {
        Some((publisher, task)) => (Some(publisher), Some(task)),
        None => (None, None),
    };
    state.send_replace(ConnectionState::Connected);
    if ready
        .send(Ok(ReadyMedia {
            source: source.clone(),
            publisher: publisher.clone(),
        }))
        .is_err()
    {
        cancel.cancel();
    }
    room_loop(
        room,
        events,
        tx,
        state,
        cancel,
        RoomMedia {
            source,
            publisher,
            task,
        },
        config,
    )
    .await
}
async fn close_room(room: &Room, deadline: std::time::Duration) -> Result<()> {
    timeout(deadline, room.close())
        .await
        .map_err(|_| Error::Timeout("room close"))
        .and_then(|result| match result {
            Ok(()) | Err(livekit::RoomError::AlreadyClosed) => Ok(()),
            Err(error) => Err(sdk(error)),
        })
}
struct RoomMedia {
    source: Option<NativeAudioSource>,
    publisher: Option<crate::publisher::AudioPublisher>,
    task: Option<JoinHandle<Result<()>>>,
}
async fn room_loop(
    room: Arc<Room>,
    mut events: mpsc::UnboundedReceiver<RoomEvent>,
    tx: mpsc::Sender<LiveKitEvent>,
    state: watch::Sender<ConnectionState>,
    cancel: CancellationToken,
    mut media: RoomMedia,
    config: LiveKitConfig,
) -> Result<()> {
    let mut tracks = HashMap::<String, AudioReader>::new();
    let mut readers = JoinSet::<(String, u64, Result<()>)>::new();
    let mut next_generation = 0u64;
    let result = async {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                published = async { media.task.as_mut().unwrap().await }, if media.task.is_some() => {
                    media.task.take();
                    published.map_err(sdk)??;
                    return Err(Error::Sdk("native publisher stopped unexpectedly".into()));
                }
                ended = readers.join_next(), if !readers.is_empty() => {
                    let (sid, generation, result) = ended.expect("nonempty JoinSet").map_err(sdk)?;
                    if tracks.get(&sid).is_some_and(|reader| reader.generation == generation) { tracks.remove(&sid); }
                    result?;
                }
                event = events.recv() => {
                    let Some(event) = event else { return Err(Error::Sdk("SDK event channel closed unexpectedly".into())); };
                    match event {
                        RoomEvent::ParticipantConnected(p) => emit(&tx, LiveKitEvent::ParticipantConnected(participant_info(&p)))?,
                        RoomEvent::ParticipantActive(p) => emit(&tx, LiveKitEvent::ParticipantActive(participant_info(&p)))?,
                        RoomEvent::ParticipantDisconnected(p) => {
                            let identity = p.identity().to_string();
                            for reader in tracks.values().filter(|r| r.identity == identity) { reader.cancel.cancel(); }
                            emit(&tx, LiveKitEvent::ParticipantDisconnected(participant_info(&p)))?;
                        }
                        RoomEvent::Connected { participants_with_tracks } => {
                            for (p, publications) in participants_with_tracks {
                                emit(&tx, LiveKitEvent::ParticipantConnected(participant_info(&p)))?;
                                if config.accepts(&p.identity().to_string()) { for publication in publications { if publication.kind() == TrackKind::Audio { publication.set_subscribed(true); } } }
                            }
                        }
                        RoomEvent::TrackPublished { publication, participant } => {
                            if publication.kind() == TrackKind::Audio && config.accepts(&participant.identity().to_string()) { publication.set_subscribed(true); }
                        }
                        RoomEvent::TrackSubscribed { track: RemoteTrack::Audio(track), participant, .. } => {
                            let identity = participant.identity().to_string();
                            if !config.accepts(&identity) { continue; }
                            let sid = track.sid().to_string();
                            if tracks.contains_key(&sid) { continue; }
                            if readers.len() >= config.max_audio_tracks { return Err(Error::Limit("subscribed audio tracks")); }
                            emit(&tx, LiveKitEvent::AudioSubscribed { identity: identity.clone(), track_sid: sid.clone() })?;
                            let stream = NativeAudioStream::new(track.rtc_track(), config.input_format.sample_rate() as i32, config.input_format.channels().into());
                            let stop = cancel.child_token();
                            let generation = next_generation;
                            next_generation = next_generation.wrapping_add(1);
                            tracks.insert(sid.clone(), AudioReader { identity: identity.clone(), generation, cancel: stop.clone() });
                            let sender = tx.clone();
                            let limit = config.max_audio_frame_bytes;
                            readers.spawn(async move { let result = read_audio(stream, identity, sid.clone(), sender, stop, limit).await; (sid, generation, result) });
                        }
                        RoomEvent::TrackUnsubscribed { track: RemoteTrack::Audio(track), participant, .. } => {
                            let sid = track.sid().to_string();
                            if let Some(reader) = tracks.remove(&sid) { reader.cancel.cancel(); }
                            emit(&tx, LiveKitEvent::AudioUnsubscribed { identity: participant.identity().to_string(), track_sid: sid })?;
                        }
                        RoomEvent::TrackSubscriptionFailed { error, .. } => return Err(sdk(error)),
                        RoomEvent::DataReceived { payload, topic, kind, participant } => {
                            if payload.len() > config.max_data_bytes { return Err(Error::Limit("incoming data payload bytes")); }
                            emit(&tx, LiveKitEvent::Data { payload: payload.as_ref().clone(), identity: participant.map(|p| p.identity().to_string()), topic, reliable: kind == DataPacketKind::Reliable })?;
                        }
                        RoomEvent::SipDTMFReceived { code, participant, .. } => {
                            if let Some(digit) = dtmf_digit(code) { emit(&tx, LiveKitEvent::Dtmf { digit, identity: participant.map(|p| p.identity().to_string()) })?; }
                        }
                        RoomEvent::ActiveSpeakersChanged { speakers } => {
                            if speakers.len() > config.max_audio_tracks { return Err(Error::Limit("active speakers per event")); }
                            emit(&tx, LiveKitEvent::ActiveSpeakers(speakers.iter().map(|p| p.identity().to_string()).collect()))?;
                        }
                        RoomEvent::Reconnecting => { state.send_replace(ConnectionState::Reconnecting); if let Some(publisher) = &media.publisher { publisher.interrupt(); }
                            if let Some(source) = &media.source { source.clear_buffer(); } emit(&tx, LiveKitEvent::Reconnecting)?; }
                        RoomEvent::Reconnected => { state.send_replace(ConnectionState::Connected); emit(&tx, LiveKitEvent::Reconnected)?; }
                        RoomEvent::Disconnected { reason } => { emit(&tx, LiveKitEvent::Disconnected { reason: format!("{reason:?}") })?; break; }
                        _ => {}
                    }
                }
            }
        }
        Ok(())
    }.await;
    cancel.cancel();
    if let Some(source) = &media.source {
        source.clear_buffer();
    }
    readers.abort_all();
    while readers.join_next().await.is_some() {}
    let publisher_closed = if let Some(mut task) = media.task.take() {
        match timeout(config.close_timeout, &mut task).await {
            Ok(result) => result.map_err(sdk).and_then(|result| result),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err(Error::Timeout("audio publisher close"))
            }
        }
    } else {
        Ok(())
    };
    let closed = close_room(&room, config.close_timeout).await;
    let result = result.and(publisher_closed).and(closed);
    state.send_replace(match &result {
        Ok(()) => ConnectionState::Closed,
        Err(error) => ConnectionState::Failed(error.to_string()),
    });
    if let Err(error) = &result {
        let _ = tx.try_send(LiveKitEvent::Error(error.to_string()));
    }
    result
}
async fn read_audio(
    mut stream: NativeAudioStream,
    identity: String,
    sid: String,
    tx: mpsc::Sender<LiveKitEvent>,
    cancel: CancellationToken,
    limit: usize,
) -> Result<()> {
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => return Ok(()),
            frame = stream.next() => {
                let Some(frame) = frame else { return Ok(()); };
                emit(&tx, LiveKitEvent::Audio { identity: identity.clone(), track_sid: sid.clone(), audio: from_sdk_audio(frame, limit)? })?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn room_event_overflow_is_explicit_and_nonblocking() {
        let (tx, mut rx) = mpsc::channel(1);
        emit(&tx, LiveKitEvent::Reconnecting).unwrap();
        assert!(matches!(
            emit(&tx, LiveKitEvent::Reconnected),
            Err(Error::QueueFull)
        ));
        assert!(matches!(rx.try_recv(), Ok(LiveKitEvent::Reconnecting)));
        drop(rx);
        assert!(matches!(
            emit(&tx, LiveKitEvent::Reconnected),
            Err(Error::Closed)
        ));
    }
}
