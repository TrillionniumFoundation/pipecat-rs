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
use pipecat_core::{AudioRawFrame, KeypadEntry, OutputImageRawFrame};
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
    events: Mutex<EventReceiver>,
    state: watch::Receiver<ConnectionState>,
    video_input_stats: Arc<crate::video_input::Stats>,
    pub(crate) video_capture: crate::video_input::CaptureHandle,
    cancel: CancellationToken,
    audio: Option<crate::publisher::AudioPublisher>,
    audio_control: crate::participant_audio::AudioControl,
    video: Option<crate::video::VideoPublisher>,
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
        let (video_readers, video_rx) = crate::video_input::VideoReaders::new(
            config.video_input.clone().unwrap_or_default(),
            cancel.clone(),
        );
        let video_input_stats = video_readers.stats();
        let video_capture = video_readers.capture_handle();
        let (state_tx, state) = watch::channel(ConnectionState::Connecting);
        let (audio_control, audio_requests) = crate::participant_audio::AudioControl::channel(
            config.queue_capacity,
            state.clone(),
            cancel.clone(),
            config.operation_timeout,
        );
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
            video_readers,
            audio_requests,
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
                    Ok(Err(cleanup)) | Err(cleanup) => {
                        Err(crate::publication::with_cleanup(error, Err(cleanup)))
                    }
                };
            }
        };
        guard.0 = None;
        Ok(Self {
            room,
            config,
            source: ready.source,
            events: Mutex::new(EventReceiver {
                events: rx,
                video: video_rx,
                video_closed: false,
            }),
            state,
            video_input_stats,
            video_capture,
            cancel,
            audio: ready.publisher,
            audio_control,
            video: ready.video,
            owner: Mutex::new(Some(owner)),
        })
    }

    /// Install a per-source policy, including before subscription. Does not enable input,
    /// bypass participant filtering, or promise an available track. Existing cadence remains.
    /// Python's capture defaults correspond to Camera + FramesPerSecond(30).
    pub fn capture_participant_video(
        &self,
        identity: &str,
        source: crate::VideoSource,
        mode: crate::VideoCaptureMode,
    ) -> Result<()> {
        if self.config.video_input.is_none() {
            return Err(Error::InvalidConfig("video input is disabled"));
        }
        if !self.config.accepts(identity) {
            return Err(Error::InvalidConfig("video participant is filtered"));
        }
        self.video_capture.capture(identity, source, mode)
    }
    pub fn video_input_stats(&self) -> crate::VideoInputStats {
        self.video_input_stats.snapshot()
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
    pub(crate) async fn discard_input_video(&self) {
        self.events.lock().await.video.close();
    }
    pub(crate) async fn recv_input(&self) -> Option<crate::video_input::InputEvent> {
        self.events.lock().await.recv_input().await
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
        crate::participant_audio::ready(&self.state(), &self.cancel)
    }
    /// Request unsubscribe for the current remote participant's audio publications.
    /// Identity, not SID; unknown/filtered participants and absent audio are no-ops.
    /// Success means local SDK setter completion, not SFU confirmation or silence.
    /// Does not mute their microphone, recall queued PCM or affect future tracks.
    pub async fn mute_participant(&self, identity: &str) -> Result<()> {
        self.audio_control.set_subscribed(identity, false).await
    }
    /// Request subscribe for current remote audio publications, respecting the filter.
    /// No persistent policy or remote acknowledgement. Empty/>256-byte identities
    /// are invalid; closed sessions return Closed and reconnecting ones NotReady.
    /// Calls observed canceled/expired before application are skipped. Cancellation
    /// racing an issued setter cannot roll it back, including partial applications.
    pub async fn unmute_participant(&self, identity: &str) -> Result<()> {
        self.audio_control.set_subscribed(identity, true).await
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

    /// Capture-time video only: pipeline PTS is ignored, alpha discarded.
    /// The result describes native adaptation, not publication or remote receipt.
    pub fn publish_video(
        &self,
        output: &OutputImageRawFrame,
    ) -> Result<crate::VideoCaptureOutcome> {
        self.publish_video_generation(output, None)
    }
    pub(crate) fn video_generation(&self) -> u64 {
        self.video
            .as_ref()
            .map_or(0, crate::video::VideoPublisher::generation)
    }
    pub(crate) fn stop_video(&self) {
        if let Some(video) = &self.video {
            video.close();
        }
    }
    pub(crate) fn interrupt_video(&self) {
        if let Some(video) = &self.video {
            video.interrupt();
        }
    }
    pub(crate) fn publish_video_generation<'a>(
        &self,
        output: impl Into<crate::video::VideoInput<'a>>,
        generation: Option<u64>,
    ) -> Result<crate::VideoCaptureOutcome> {
        self.ready()?;
        let config = self
            .config
            .video_output
            .as_ref()
            .ok_or(Error::VideoDisabled)?;
        self.video.as_ref().ok_or(Error::VideoDisabled)?.capture(
            output,
            config,
            generation,
            &self.cancel,
        )
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
        if let Some(video) = &self.video {
            video.close();
        }
        self.interrupt_audio();
        self.cancel.cancel();
        let mut owned = self.owner.lock().await;
        if let Some(owner) = owned.as_mut() {
            let result = owner.await.map_err(sdk).and_then(|result| result);
            owned.take();
            self.events.lock().await.video.close();
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
        if let Some(video) = &self.video {
            video.close();
        }
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

/// Admit the complete raw payload before the first-join observation, without media filtering.
/// Non-atomic failure terminates the owner: no rollback, retry or resumed processing.
pub(crate) fn participant_connected(
    tx: &mpsc::Sender<LiveKitEvent>,
    other_participant_has_joined: &mut bool,
    participant: ParticipantInfo,
) -> Result<()> {
    let raw = LiveKitEvent::ParticipantConnected(participant);
    crate::protocol::validate_event(&raw)?;
    let LiveKitEvent::ParticipantConnected(participant) = &raw else {
        unreachable!()
    };
    let first_identity =
        (!*other_participant_has_joined).then(|| participant.identity.as_str().to_owned());
    emit(tx, raw)?;
    if let Some(identity) = first_identity {
        emit(tx, LiveKitEvent::FirstParticipantJoined { identity })?;
        *other_participant_has_joined = true;
    }
    Ok(())
}

/// Sample current unfiltered SDK membership only after successful raw admission.
/// The deferred fact also lets normalized tests exercise this ordering without a joined room.
pub(crate) fn participant_disconnected(
    tx: &mpsc::Sender<LiveKitEvent>,
    other_participant_has_joined: &mut bool,
    participant: ParticipantInfo,
    current_remotes_empty: impl FnOnce() -> bool,
) -> Result<()> {
    emit(tx, LiveKitEvent::ParticipantDisconnected(participant))?;
    if current_remotes_empty() {
        *other_participant_has_joined = false;
    }
    Ok(())
}

/// Raw-first, non-atomic admission. Failure terminates the room owner; never retry.
/// Only a compact copy of the validated last identity survives a successful change.
pub(crate) fn active_speakers_changed(
    config: &LiveKitConfig,
    tx: &mpsc::Sender<LiveKitEvent>,
    last_identity: &mut Option<String>,
    speakers: Vec<String>,
) -> Result<()> {
    if speakers.len() > config.max_audio_tracks {
        return Err(Error::Limit("active speakers per event"));
    }
    let raw = LiveKitEvent::ActiveSpeakers(speakers);
    crate::protocol::validate_event(&raw)?;
    let LiveKitEvent::ActiveSpeakers(speakers) = &raw else {
        unreachable!()
    };
    let changed = speakers
        .last()
        .filter(|identity| last_identity.as_deref() != Some(identity.as_str()))
        .map(|identity| identity.as_str().to_owned());
    emit(tx, raw)?;
    if let Some(identity) = changed {
        emit(
            tx,
            LiveKitEvent::ActiveSpeakerChanged {
                identity: identity.clone(),
            },
        )?;
        *last_identity = Some(identity);
    }
    Ok(())
}

struct AudioReader {
    identity: String,
    generation: u64,
    cancel: CancellationToken,
}
fn audio_reader_finished(tracks: &mut HashMap<String, AudioReader>, sid: &str, generation: u64) {
    if tracks
        .get(sid)
        .is_some_and(|reader| reader.generation == generation)
    {
        tracks.remove(sid);
    }
}
fn audio_unsubscribed(
    tx: &mpsc::Sender<LiveKitEvent>,
    tracks: &mut HashMap<String, AudioReader>,
    identity: String,
    sid: String,
) -> Result<()> {
    if let Some(reader) = tracks.remove(&sid) {
        reader.cancel.cancel();
    }
    emit(
        tx,
        LiveKitEvent::AudioUnsubscribed {
            identity,
            track_sid: sid,
        },
    )
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
    video: Option<crate::video::VideoPublisher>,
    source: Option<NativeAudioSource>,
    publisher: Option<crate::publisher::AudioPublisher>,
}
#[allow(clippy::too_many_arguments)] // One owner receives all session resources.
async fn initialize_room(
    room: Arc<Room>,
    events: mpsc::UnboundedReceiver<RoomEvent>,
    tx: mpsc::Sender<LiveKitEvent>,
    state: watch::Sender<ConnectionState>,
    cancel: CancellationToken,
    config: LiveKitConfig,
    ready: oneshot::Sender<Result<ReadyMedia>>,
    video_readers: crate::video_input::VideoReaders,
    audio_requests: mpsc::Receiver<crate::participant_audio::AudioRequest>,
) -> Result<()> {
    let mut setup = RoomSetup {
        room: &room,
        config: &config,
        source: None,
        video: None,
    };
    let initialized = crate::publication::initialize(&mut setup, &config, &cancel).await;
    if let Err(error) = initialized {
        let message = error.to_string();
        state.send_replace(ConnectionState::Failed(message.clone()));
        let _ = ready.send(Err(Error::Sdk(message.clone())));
        return Err(Error::Sdk(message));
    }
    let source = setup.source.take();
    let video = setup.video.take().map(crate::video::VideoPublisher::new);
    drop(setup);
    // No Connected event/readiness until every configured publication completed.
    let announced = if cancel.is_cancelled() {
        Err(Error::Closed)
    } else {
        emit(
            &tx,
            LiveKitEvent::Connected {
                identity: room.local_participant().identity().to_string(),
                room: room.name(),
            },
        )
    };
    if let Err(primary) = announced {
        if let Some(video) = &video {
            video.close();
        }
        let error = crate::publication::with_cleanup(
            primary,
            close_room(&room, config.close_timeout).await,
        );
        state.send_replace(ConnectionState::Failed(error.to_string()));
        let _ = ready.send(Err(Error::Sdk(error.to_string())));
        return Err(error);
    }
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
    crate::publication::transfer_ready(
        ready,
        ReadyMedia {
            source: source.clone(),
            publisher: publisher.clone(),
            video: video.clone(),
        },
        &cancel,
    );
    room_loop(
        room,
        events,
        tx,
        state,
        cancel,
        RoomMedia {
            video,
            source,
            publisher,
            task,
        },
        config,
        video_readers,
        audio_requests,
    )
    .await
}
struct RoomSetup<'a> {
    room: &'a Room,
    config: &'a LiveKitConfig,
    source: Option<NativeAudioSource>,
    video: Option<crate::video::NativeCamera>,
}
#[async_trait::async_trait]
impl crate::publication::PublicationOwner for RoomSetup<'_> {
    async fn publish_audio(&mut self) -> Result<()> {
        let format = self.config.output_format.ok_or(Error::AudioDisabled)?;
        let source = NativeAudioSource::new(
            AudioSourceOptions::default(),
            format.sample_rate(),
            format.channels().into(),
            self.config.source_queue_ms,
        );
        let track = LocalAudioTrack::create_audio_track(
            "pipecat-audio",
            RtcAudioSource::Native(source.clone()),
        );
        self.source = Some(source);
        self.room
            .local_participant()
            .publish_track(
                LocalTrack::Audio(track),
                TrackPublishOptions {
                    source: TrackSource::Microphone,
                    ..Default::default()
                },
            )
            .await
            .map_err(sdk)?;
        Ok(())
    }
    async fn publish_video(&mut self) -> Result<()> {
        let camera = crate::video::NativeCamera::new(
            self.config
                .video_output
                .as_ref()
                .ok_or(Error::VideoDisabled)?,
        )?;
        let track = camera.track.clone();
        self.video = Some(camera);
        self.room
            .local_participant()
            .publish_track(
                LocalTrack::Video(track),
                TrackPublishOptions {
                    source: TrackSource::Camera,
                    ..Default::default()
                },
            )
            .await
            .map_err(sdk)?;
        Ok(())
    }
    async fn rollback(&mut self) -> Result<()> {
        // Release our ownership even if SDK close times out. SDK room owns published tracks
        // until its own close/drop; no separate detached publication tasks are started here.
        self.video.take();
        self.source.take();
        close_room(self.room, self.config.close_timeout).await
    }
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
    video: Option<crate::video::VideoPublisher>,
    source: Option<NativeAudioSource>,
    publisher: Option<crate::publisher::AudioPublisher>,
    task: Option<JoinHandle<Result<()>>>,
}
#[allow(clippy::too_many_arguments)] // Session owner, not a detached video lifecycle.
async fn room_loop(
    room: Arc<Room>,
    mut events: mpsc::UnboundedReceiver<RoomEvent>,
    tx: mpsc::Sender<LiveKitEvent>,
    state: watch::Sender<ConnectionState>,
    cancel: CancellationToken,
    mut media: RoomMedia,
    config: LiveKitConfig,
    mut video_readers: crate::video_input::VideoReaders,
    mut audio_requests: mpsc::Receiver<crate::participant_audio::AudioRequest>,
) -> Result<()> {
    let mut tracks = HashMap::<String, AudioReader>::new();
    let mut readers = JoinSet::<(String, u64, Result<()>)>::new();
    let mut next_generation = 0u64;
    let mut audio_requests_open = true;
    // Session-owned: empty lists, departure and automatic reconnect do not reset it.
    let mut last_active_speaker = None;
    // Fresh owner only; reconnect and empty initial snapshots do not reset this bool.
    let mut other_participant_has_joined = false;
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
                ended = video_readers.next_finished() => { ended?; }
                ended = readers.join_next(), if !readers.is_empty() => {
                    let (sid, generation, result) = ended.expect("nonempty JoinSet").map_err(sdk)?;
                    audio_reader_finished(&mut tracks, &sid, generation);
                    result?;
                }
                event = events.recv() => {
                    let Some(event) = event else { return Err(Error::Sdk("SDK event channel closed unexpectedly".into())); };
                    match event {
                        RoomEvent::ParticipantConnected(p) => participant_connected(&tx, &mut other_participant_has_joined, participant_info(&p))?,
                        RoomEvent::ParticipantActive(p) => emit(&tx, LiveKitEvent::ParticipantActive(participant_info(&p)))?,
                        RoomEvent::ParticipantDisconnected(p) => {
                            let identity = p.identity().to_string();
                            video_readers.remove_participant(&identity).await?;
                            for reader in tracks.values().filter(|r| r.identity == identity) { reader.cancel.cancel(); }
                            participant_disconnected(&tx, &mut other_participant_has_joined, participant_info(&p), || room.remote_participants().is_empty())?;
                        }
                        RoomEvent::Connected { participants_with_tracks } => {
                            for (p, publications) in participants_with_tracks {
                                participant_connected(&tx, &mut other_participant_has_joined, participant_info(&p))?;
                                for publication in publications { if selects_publication(&config, &p.identity().to_string(), publication.kind()) { publication.set_subscribed(true); } }
                            }
                        }
                        RoomEvent::TrackPublished { publication, participant } => {
                            if selects_publication(&config, &participant.identity().to_string(), publication.kind()) { publication.set_subscribed(true); }
                        }
                        RoomEvent::TrackSubscribed { track: RemoteTrack::Video(track), publication, participant } => {
                            let identity = participant.identity().to_string();
                            let key = crate::video_input::Key { identity, source: crate::VideoSource::from_track(publication.source()) };
                            video_subscribed(&config, &tx, &mut video_readers, key, track.sid().to_string(), track.rtc_track(), publication.is_muted()).await?;
                        }
                        RoomEvent::TrackUnsubscribed { track: RemoteTrack::Video(track), publication, participant } => {
                            let key = crate::video_input::Key { identity: participant.identity().to_string(), source: crate::VideoSource::from_track(publication.source()) };
                            video_unsubscribed(&tx, &mut video_readers, key, track.sid().to_string()).await?;
                        }
                        RoomEvent::TrackUnpublished { publication, participant } => {
                            if publication.kind() == TrackKind::Video {
                                let key = crate::video_input::Key { identity: participant.identity().to_string(), source: crate::VideoSource::from_track(publication.source()) };
                                video_readers.stop(&key, Some(&publication.sid().to_string())).await?;
                            }
                        }
                        RoomEvent::TrackMuted { participant: Participant::Remote(participant), publication: TrackPublication::Remote(publication) } => {
                            if publication.kind() == TrackKind::Video {
                                let key = crate::video_input::Key { identity: participant.identity().to_string(), source: crate::VideoSource::from_track(publication.source()) };
                                video_readers.mute(&key, &publication.sid().to_string()).await?;
                            }
                        }
                        RoomEvent::TrackUnmuted { participant: Participant::Remote(participant), publication: TrackPublication::Remote(publication) } => {
                            if publication.kind() == TrackKind::Video {
                                let key = crate::video_input::Key { identity: participant.identity().to_string(), source: crate::VideoSource::from_track(publication.source()) };
                                // Only the currently owned SID can resume, with an entirely fresh sink.
                                video_readers.unmute(&key, &publication.sid().to_string())?;
                            }
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
                            audio_unsubscribed(&tx, &mut tracks, participant.identity().to_string(), sid)?;
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
                            active_speakers_changed(&config, &tx, &mut last_active_speaker, speakers.iter().map(|p| p.identity().to_string()).collect())?;
                        }
                        RoomEvent::Reconnecting => { video_readers.reset_readers().await?; state.send_replace(ConnectionState::Reconnecting); if let Some(video) = &media.video { video.interrupt(); }
                            if let Some(publisher) = &media.publisher { publisher.interrupt(); }
                            if let Some(source) = &media.source { source.clear_buffer(); } emit(&tx, LiveKitEvent::Reconnecting)?; }
                        RoomEvent::Reconnected => {
                            for participant in room.remote_participants().values() {
                                let identity = participant.identity().to_string();
                                if !selects_publication(&config, &identity, TrackKind::Video) { continue; }
                                for publication in participant.track_publications().values() {
                                    if let Some(RemoteTrack::Video(track)) = publication.track() {
                                        let key = crate::video_input::Key { identity: identity.clone(), source: crate::VideoSource::from_track(publication.source()) };
                                        video_readers.start(key, track.sid().to_string(), track.rtc_track(), publication.is_muted()).await?;
                                    }
                                }
                            }
                            state.send_replace(ConnectionState::Connected); emit(&tx, LiveKitEvent::Reconnected)?;
                        }
                        RoomEvent::Disconnected { reason } => { video_readers.close().await?; emit(&tx, LiveKitEvent::Disconnected { reason: format!("{reason:?}") })?; break; }
                        _ => {}
                    }
                }
                request = audio_requests.recv(), if audio_requests_open => {
                    if let Some(request) = request {
                        request.execute(&state.borrow(), &cancel, |identity, subscribed, check| {
                            crate::participant_audio::sdk_ready(room.connection_state())?;
                            crate::participant_audio::apply_current(
                                &config,
                                identity,
                                subscribed,
                                room.remote_participants().into_values().map(|participant| (participant.identity().to_string(), participant)),
                                |participant| participant.track_publications().into_values().collect(),
                                check,
                            )
                        });
                    } else {
                        // Disable a closed lane rather than spinning or creating another owner.
                        audio_requests_open = false;
                    }
                }
            }
        }
        Ok(())
    }.await;
    if let Some(video) = &media.video {
        video.close();
    }
    cancel.cancel();
    if let Some(source) = &media.source {
        source.clear_buffer();
    }
    let result = finish_video_input(result, &mut video_readers).await;
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
    let result = crate::publication::combine_cleanup(result, publisher_closed);
    let result = crate::publication::combine_cleanup(result, closed);
    state.send_replace(match &result {
        Ok(()) => ConnectionState::Closed,
        Err(error) => ConnectionState::Failed(error.to_string()),
    });
    if let Err(error) = &result {
        let _ = tx.try_send(LiveKitEvent::Error(error.to_string()));
    }
    result
}
/// The room owner's terminal video cleanup, also exercised without a hosted room.
pub(crate) async fn finish_video_input(
    result: Result<()>,
    readers: &mut crate::video_input::VideoReaders,
) -> Result<()> {
    crate::publication::combine_cleanup(result, readers.close().await)
}

// Normalized SDK handlers are shared with local no-SFU lifecycle tests. Reconnect
// reconstruction deliberately calls VideoReaders::start directly, without emission.
pub(crate) async fn video_subscribed(
    config: &LiveKitConfig,
    tx: &mpsc::Sender<LiveKitEvent>,
    readers: &mut crate::video_input::VideoReaders,
    key: crate::video_input::Key,
    sid: String,
    track: livekit::webrtc::video_track::RtcVideoTrack,
    muted: bool,
) -> Result<()> {
    if !selects_publication(config, &key.identity, TrackKind::Video) {
        return Ok(());
    }
    if readers
        .start(key.clone(), sid.clone(), track, muted)
        .await?
        == crate::video_input::OwnershipTransition::Changed
    {
        emit(
            tx,
            LiveKitEvent::VideoSubscribed {
                identity: key.identity,
                track_sid: sid,
                source: key.source,
            },
        )?;
    }
    Ok(())
}
pub(crate) async fn video_unsubscribed(
    tx: &mpsc::Sender<LiveKitEvent>,
    readers: &mut crate::video_input::VideoReaders,
    key: crate::video_input::Key,
    sid: String,
) -> Result<()> {
    if readers.stop(&key, Some(&sid)).await? == crate::video_input::OwnershipTransition::Changed {
        emit(
            tx,
            LiveKitEvent::VideoUnsubscribed {
                identity: key.identity,
                track_sid: sid,
                source: key.source,
            },
        )?;
    }
    Ok(())
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

#[cfg(test)]
mod setup_guard_tests {
    use super::*;
    #[tokio::test]
    async fn aborted_connect_caller_guard_signals_existing_owner() {
        let cancel = CancellationToken::new();
        let owned_cancel = cancel.clone();
        let owner = tokio::spawn(async move {
            owned_cancel.cancelled().await;
        });
        let caller_cancel = cancel.clone();
        let (started, waiting) = oneshot::channel();
        let caller = tokio::spawn(async move {
            let _guard = SetupCancellation(Some(caller_cancel));
            started.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        timeout(std::time::Duration::from_secs(1), waiting)
            .await
            .unwrap()
            .unwrap();
        caller.abort();
        assert!(timeout(std::time::Duration::from_secs(1), caller)
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled());
        timeout(std::time::Duration::from_secs(1), owner)
            .await
            .unwrap()
            .unwrap();
        assert!(cancel.is_cancelled());
    }
}

/// The same opt-in/filter predicate is used for initial and later publications.
fn selects_publication(config: &LiveKitConfig, identity: &str, kind: TrackKind) -> bool {
    config.accepts(identity)
        && (kind == TrackKind::Audio || (kind == TrackKind::Video && config.video_input.is_some()))
}
struct EventReceiver {
    events: mpsc::Receiver<LiveKitEvent>,
    video: crate::video_input::VideoReceiver,
    video_closed: bool,
}
impl EventReceiver {
    async fn recv_input(&mut self) -> Option<crate::video_input::InputEvent> {
        loop {
            tokio::select! {
                event = self.events.recv() => return event.map(crate::video_input::InputEvent::Event),
                event = self.video.recv_input(), if !self.video_closed => match event {
                    Some(event) => return Some(event),
                    None => self.video_closed = true,
                }
            }
        }
    }
    async fn recv(&mut self) -> Option<LiveKitEvent> {
        loop {
            tokio::select! {
                event = self.events.recv() => return event,
                event = self.video.recv(), if !self.video_closed => match event {
                    Some(event) => return Some(event),
                    None => self.video_closed = true,
                }
            }
        }
    }
}

#[cfg(test)]
mod video_selection_tests {
    use super::*;
    #[test]
    fn initial_and_later_publications_use_opt_in_and_identity_filter() {
        let mut config = LiveKitConfig::default();
        assert!(selects_publication(&config, "alice", TrackKind::Audio));
        assert!(!selects_publication(&config, "alice", TrackKind::Video));
        config.video_input = Some(crate::VideoInputConfig::default());
        assert!(selects_publication(&config, "alice", TrackKind::Video));
        config.participant_filter = vec!["alice".into()];
        for kind in [TrackKind::Audio, TrackKind::Video] {
            assert!(selects_publication(&config, "alice", kind));
            assert!(!selects_publication(&config, "bob", kind));
        }
    }
}

#[cfg(test)]
mod audio_unsubscribe_tests {
    use super::*;

    fn reader(generation: u64) -> AudioReader {
        AudioReader {
            identity: "alice".into(),
            generation,
            cancel: CancellationToken::new(),
        }
    }

    #[test]
    fn unsubscribe_uses_track_sid_not_identity_and_old_completion_preserves_replacement() {
        let (tx, mut rx) = mpsc::channel(2);
        let first = reader(1);
        let first_cancel = first.cancel.clone();
        let other = reader(2);
        let other_cancel = other.cancel.clone();
        let mut tracks = HashMap::from([("first".into(), first), ("other".into(), other)]);
        audio_unsubscribed(&tx, &mut tracks, "alice".into(), "first".into()).unwrap();
        assert!(first_cancel.is_cancelled());
        assert!(!other_cancel.is_cancelled());
        assert!(tracks.contains_key("other"));
        assert!(
            matches!(rx.try_recv(), Ok(LiveKitEvent::AudioUnsubscribed { identity, track_sid }) if identity == "alice" && track_sid == "first")
        );
        tracks.insert("first".into(), reader(3));
        audio_reader_finished(&mut tracks, "first", 1);
        assert_eq!(tracks.get("first").unwrap().generation, 3);
        audio_reader_finished(&mut tracks, "first", 3);
        assert!(!tracks.contains_key("first"));
        assert!(tracks.contains_key("other"));
    }

    #[test]
    fn unsubscribe_cancels_before_raw_admission_failure_and_unknown_sid_remains_raw() {
        let (tx, mut rx) = mpsc::channel(1);
        let owned = reader(1);
        let cancel = owned.cancel.clone();
        let mut tracks = HashMap::from([("audio".into(), owned)]);
        emit(&tx, LiveKitEvent::Reconnecting).unwrap();
        assert!(matches!(
            audio_unsubscribed(&tx, &mut tracks, "alice".into(), "audio".into()),
            Err(Error::QueueFull)
        ));
        assert!(cancel.is_cancelled());
        assert!(tracks.is_empty());
        assert!(matches!(rx.try_recv(), Ok(LiveKitEvent::Reconnecting)));
        audio_unsubscribed(&tx, &mut tracks, "alice".into(), "unknown".into()).unwrap();
        assert!(
            matches!(rx.try_recv(), Ok(LiveKitEvent::AudioUnsubscribed { track_sid, .. }) if track_sid == "unknown")
        );
    }
}
