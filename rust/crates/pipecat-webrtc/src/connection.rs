use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex as StdMutex,
};
use std::time::Duration;

use pipecat_core::{AudioRawFrame, Frame, FrameData};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{mpsc, watch, Mutex, Notify},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use webrtc::{
    api::{
        interceptor_registry::register_default_interceptors, media_engine::MediaEngine,
        setting_engine::SettingEngine, APIBuilder,
    },
    data_channel::{data_channel_state::RTCDataChannelState, RTCDataChannel},
    dtls_transport::dtls_transport_state::RTCDtlsTransportState,
    ice::network_type::NetworkType,
    ice_transport::ice_transport_state::RTCIceTransportState,
    interceptor::registry::Registry,
    media::Sample,
    peer_connection::{
        configuration::RTCConfiguration, peer_connection_state::RTCPeerConnectionState,
        sdp::sdp_type::RTCSdpType, RTCPeerConnection,
    },
    rtp_transceiver::{
        rtp_codec::RTPCodecType, rtp_transceiver_direction::RTCRtpTransceiverDirection,
        RTCRtpTransceiverInit,
    },
    sctp_transport::sctp_transport_state::RTCSctpTransportState,
    track::{
        track_local::{track_local_static_sample::TrackLocalStaticSample, TrackLocal},
        track_remote::TrackRemote,
    },
};

use crate::{
    codec::{Decoder, Encoder},
    AudioCodec, Error, RTCIceCandidateInit, RTCIceServer, RTCSessionDescription, Result,
};

/// Resource and network limits. No public STUN/TURN server is used by default.
#[derive(Debug, Clone)]
pub struct SmallWebRtcConfig {
    pub ice_servers: Vec<RTCIceServer>,
    /// None creates a data-only peer. Opus is available with the default feature.
    pub audio_codec: Option<AudioCodec>,
    pub queue_capacity: usize,
    pub max_message_bytes: usize,
    pub max_buffered_bytes: usize,
    pub connect_timeout: Duration,
    pub gather_timeout: Duration,
    pub close_timeout: Duration,
    /// Restrict gathered host candidates to IPv4 loopback for offline tests.
    pub localhost_only: bool,
    pub data_channel_label: String,
}
impl Default for SmallWebRtcConfig {
    fn default() -> Self {
        Self {
            ice_servers: Vec::new(),
            audio_codec: Some(AudioCodec::default()),
            queue_capacity: 50,
            max_message_bytes: 16_384,
            max_buffered_bytes: 65_536,
            connect_timeout: Duration::from_secs(15),
            gather_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(5),
            localhost_only: false,
            data_channel_label: "pipecat".into(),
        }
    }
}
impl SmallWebRtcConfig {
    fn validate(&self) -> Result<()> {
        if self.queue_capacity == 0 || self.queue_capacity > 65_536 {
            return Err(Error::InvalidConfig(
                "queue_capacity must be between 1 and 65536",
            ));
        }
        if self.max_message_bytes == 0 || self.max_message_bytes > 16_384 {
            return Err(Error::InvalidConfig(
                "max_message_bytes must be between 1 and 16384",
            ));
        }
        if self.max_buffered_bytes < self.max_message_bytes {
            return Err(Error::InvalidConfig(
                "max_buffered_bytes must cover one maximum-size message",
            ));
        }
        if self.connect_timeout.is_zero()
            || self.gather_timeout.is_zero()
            || self.close_timeout.is_zero()
        {
            return Err(Error::InvalidConfig("timeouts must be positive"));
        }
        if self.localhost_only && !self.ice_servers.is_empty() {
            return Err(Error::InvalidConfig(
                "localhost_only cannot use external ICE servers",
            ));
        }
        if self.data_channel_label.len() > 65_535 {
            return Err(Error::InvalidConfig(
                "data channel label exceeds 65535 bytes",
            ));
        }
        if let Some(codec) = self.audio_codec {
            if !codec.available() {
                return Err(Error::CodecUnavailable(codec));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    New,
    Connecting,
    Connected,
    Disconnected,
    Failed,
    Closing,
    Closed,
}
impl From<RTCPeerConnectionState> for ConnectionState {
    fn from(state: RTCPeerConnectionState) -> Self {
        match state {
            RTCPeerConnectionState::Connecting => Self::Connecting,
            RTCPeerConnectionState::Connected => Self::Connected,
            RTCPeerConnectionState::Disconnected => Self::Disconnected,
            RTCPeerConnectionState::Failed => Self::Failed,
            RTCPeerConnectionState::Closed => Self::Closed,
            _ => Self::New,
        }
    }
}

/// Decoded PCM plus original RTP timing for downstream jitter/playout policy.
#[derive(Debug, Clone)]
pub struct IncomingAudio {
    pub frame: AudioRawFrame,
    pub codec: AudioCodec,
    pub sequence_number: u16,
    pub timestamp: u32,
    pub ssrc: u32,
    pub track_id: String,
}
impl IncomingAudio {
    pub fn into_frame(self) -> Frame {
        let mut frame = Frame::new(FrameData::InputAudio(self.frame));
        frame.transport_source = Some(self.track_id);
        frame
            .metadata
            .insert("rtp_timestamp".into(), self.timestamp.into());
        frame
            .metadata
            .insert("rtp_sequence_number".into(), self.sequence_number.into());
        frame.metadata.insert("rtp_ssrc".into(), self.ssrc.into());
        frame
    }
}

#[derive(Debug, Clone)]
pub enum TransportEvent {
    AppMessage(Value),
    Signaling(Value),
    Audio(IncomingAudio),
    /// None denotes completion of local gathering (trickle ICE).
    IceCandidate(Option<RTCIceCandidateInit>),
    DataChannelOpen,
    DataChannelClosed,
    TrackStatus {
        receiver_index: u32,
        enabled: bool,
    },
    Error(String),
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportStats {
    pub audio_packets_sent: u64,
    pub audio_packets_received: u64,
    pub audio_packets_dropped: u64,
    pub audio_packets_out_of_order: u64,
    pub audio_packets_missing: u64,
    pub app_messages_sent: u64,
    pub app_messages_received: u64,
    pub malformed_messages: u64,
    pub events_dropped: u64,
}
#[derive(Default)]
struct Counters {
    audio_sent: AtomicU64,
    audio_received: AtomicU64,
    audio_dropped: AtomicU64,
    audio_out_of_order: AtomicU64,
    audio_missing: AtomicU64,
    messages_sent: AtomicU64,
    messages_received: AtomicU64,
    malformed: AtomicU64,
    events_dropped: AtomicU64,
}
struct Context {
    cancel: CancellationToken,
    events: mpsc::Sender<TransportEvent>,
    state: watch::Sender<ConnectionState>,
    channel: watch::Sender<Option<Arc<RTCDataChannel>>>,
    channel_claimed: AtomicBool,
    audio_enabled: AtomicBool,
    receive_only: bool,
    audio_track_claimed: AtomicBool,
    audio_generation: AtomicU64,
    trickle: AtomicBool,
    pending: Arc<Pending>,
    last_error: StdMutex<Option<String>>,
    counters: Counters,
    config: SmallWebRtcConfig,
}
impl Context {
    fn emit(&self, event: TransportEvent) {
        if self.events.try_send(event).is_err() {
            self.counters.events_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    fn fail(&self, message: String) {
        *self.last_error.lock().unwrap() = Some(message.clone());
        self.emit(TransportEvent::Error(message));
        self.state.send_replace(ConnectionState::Failed);
        self.cancel.cancel();
    }
}
#[derive(Default)]
struct Pending {
    count: AtomicU64,
    changed: Notify,
}
struct PendingGuard(Arc<Pending>);
impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
struct Queued<T> {
    value: T,
    generation: u64,
    _guard: PendingGuard,
}
impl<T> Queued<T> {
    fn new(value: T, generation: u64, pending: &Arc<Pending>) -> Self {
        pending.count.fetch_add(1, Ordering::AcqRel);
        Self {
            value,
            generation,
            _guard: PendingGuard(pending.clone()),
        }
    }
}
type Tasks = Arc<StdMutex<Vec<JoinHandle<()>>>>;

enum CloseState {
    Idle,
    Running(JoinHandle<std::result::Result<(), String>>),
    Finished(std::result::Result<(), String>),
}

/// Owns one peer and its bounded I/O queues. Call close().await for deterministic cleanup.
///
/// send_* accepts a queue entry, not a delivery receipt. Poll events and state for
/// asynchronous errors. Audio uses drop-newest when the receive queue is full;
/// JSON receive applies backpressure. Cancellation discards queued output.
pub struct SmallWebRtcConnection {
    id: String,
    pc: StdMutex<Option<Arc<RTCPeerConnection>>>,
    ctx: Arc<Context>,
    outgoing_messages: mpsc::Sender<Queued<String>>,
    outgoing_audio: mpsc::Sender<Queued<AudioRawFrame>>,
    events: Arc<Mutex<mpsc::Receiver<TransportEvent>>>,
    tasks: Tasks,
    negotiation: Mutex<()>,
    close_state: Mutex<CloseState>,
}
impl SmallWebRtcConnection {
    pub async fn new(config: SmallWebRtcConfig) -> Result<Self> {
        Self::build(config, false).await
    }

    /// Negotiate receive-only audio without a local track, encoder or audio writer.
    /// Application messages and normal ICE/DTLS/RTCP traffic remain bidirectional.
    /// Audio send APIs return AudioDisabled while open. The mode cannot be promoted.
    pub async fn new_receive_only(config: SmallWebRtcConfig) -> Result<Self> {
        if config.audio_codec.is_none() {
            return Err(Error::InvalidConfig(
                "receive-only audio requires an audio codec",
            ));
        }
        Self::build(config, true).await
    }

    async fn build(config: SmallWebRtcConfig, receive_only: bool) -> Result<Self> {
        config.validate()?;
        let mut media = MediaEngine::default();
        if let Some(codec) = config.audio_codec {
            media.register_codec(codec.parameters(), RTPCodecType::Audio)?;
        }
        let registry = register_default_interceptors(Registry::new(), &mut media)?;
        let mut settings = SettingEngine::default();
        if config.localhost_only {
            settings.set_network_types(vec![NetworkType::Udp4]);
            settings.set_include_loopback_candidate(true);
            settings.set_ip_filter(Box::new(|ip| ip.is_loopback()));
            settings.set_ice_multicast_dns_mode(webrtc::ice::mdns::MulticastDnsMode::Disabled);
            // Net::Ifs uses real OS UDP sockets with an explicit interface catalog.
            // This also works on hosts where getifaddrs/netlink enumeration is restricted.
            let loopback = webrtc::util::vnet::interface::Interface::new(
                "lo".into(),
                vec!["127.0.0.1/8"
                    .parse()
                    .map_err(|_| Error::InvalidConfig("invalid loopback interface"))?],
            );
            settings.set_vnet(Some(Arc::new(webrtc::util::vnet::net::Net::Ifs(vec![
                loopback,
            ]))));
        }
        let api = APIBuilder::new()
            .with_media_engine(media)
            .with_interceptor_registry(registry)
            .with_setting_engine(settings)
            .build();
        let pc = Arc::new(
            api.new_peer_connection(RTCConfiguration {
                ice_servers: config.ice_servers.clone(),
                ..Default::default()
            })
            .await?,
        );
        let (event_tx, event_rx) = mpsc::channel(config.queue_capacity);
        let (message_tx, message_rx) = mpsc::channel(config.queue_capacity);
        let (audio_tx, audio_rx) = mpsc::channel(config.queue_capacity);
        let (state, _) = watch::channel(ConnectionState::New);
        let (channel, channel_rx) = watch::channel(None);
        let ctx = Arc::new(Context {
            cancel: CancellationToken::new(),
            events: event_tx,
            state,
            channel,
            channel_claimed: AtomicBool::new(false),
            audio_enabled: AtomicBool::new(true),
            receive_only,
            audio_track_claimed: AtomicBool::new(false),
            audio_generation: AtomicU64::new(0),
            trickle: AtomicBool::new(false),
            pending: Arc::new(Pending::default()),
            last_error: StdMutex::new(None),
            counters: Counters::default(),
            config: config.clone(),
        });
        let tasks: Tasks = Arc::new(StdMutex::new(Vec::new()));
        static NEXT_ID: AtomicU64 = AtomicU64::new(1);
        let id = format!(
            "pc-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        );
        let connection = Self {
            id,
            pc: StdMutex::new(Some(pc.clone())),
            ctx: ctx.clone(),
            outgoing_messages: message_tx,
            outgoing_audio: audio_tx,
            events: Arc::new(Mutex::new(event_rx)),
            tasks: tasks.clone(),
            negotiation: Mutex::new(()),
            close_state: Mutex::new(CloseState::Idle),
        };
        // Own the peer before awaited setup. A cancelled constructor schedules
        // existing Drop cleanup; only explicit close provides an awaited result.
        install_callbacks(&pc, &ctx, &tasks);
        // Complete fallible codec/track setup before starting queue workers.
        let audio_setup = async {
            if receive_only {
                pc.add_transceiver_from_kind(
                    RTPCodecType::Audio,
                    Some(RTCRtpTransceiverInit {
                        direction: RTCRtpTransceiverDirection::Recvonly,
                        send_encodings: vec![],
                    }),
                )
                .await?;
                Ok::<_, Error>(None)
            } else if let Some(codec) = config.audio_codec {
                let encoder = Encoder::new(codec)?;
                let track = Arc::new(TrackLocalStaticSample::new(
                    codec.parameters().capability,
                    "audio".into(),
                    "pipecat".into(),
                ));
                let sender = pc
                    .add_track(track.clone() as Arc<dyn TrackLocal + Send + Sync>)
                    .await?;
                let rtcp_ctx = ctx.clone();
                tasks.lock().unwrap().push(tokio::spawn(async move {
                    loop {
                        tokio::select! { biased;
                            _ = rtcp_ctx.cancel.cancelled() => break,
                            result = sender.read_rtcp() => if result.is_err() { break; },
                        }
                    }
                }));
                Ok::<_, Error>(Some((track, encoder)))
            } else {
                Ok(None)
            }
        }
        .await;
        let audio_setup = match audio_setup {
            Ok(setup) => setup,
            Err(error) => {
                if let Err(cleanup) = connection.close().await {
                    return Err(Error::Shutdown(format!(
                        "setup failed: {error}; setup cleanup unverified: {cleanup}"
                    )));
                }
                return Err(error);
            }
        };
        if let Some((track, encoder)) = audio_setup {
            tasks.lock().unwrap().push(tokio::spawn(audio_writer(
                ctx.clone(),
                track,
                encoder,
                audio_rx,
            )));
        }
        tasks.lock().unwrap().push(tokio::spawn(message_writer(
            ctx.clone(),
            channel_rx,
            message_rx,
        )));
        Ok(connection)
    }

    pub fn pc_id(&self) -> &str {
        &self.id
    }
    pub fn state(&self) -> ConnectionState {
        *self.ctx.state.borrow()
    }
    pub fn subscribe_state(&self) -> watch::Receiver<ConnectionState> {
        self.ctx.state.subscribe()
    }
    pub fn config(&self) -> &SmallWebRtcConfig {
        &self.ctx.config
    }
    pub fn is_cancelled(&self) -> bool {
        self.ctx.cancel.is_cancelled()
    }
    /// Last fatal asynchronous error, retained after cancellation and queue closure.
    pub fn last_error(&self) -> Option<String> {
        self.ctx.last_error.lock().unwrap().clone()
    }
    pub fn stats(&self) -> TransportStats {
        let c = &self.ctx.counters;
        TransportStats {
            audio_packets_sent: c.audio_sent.load(Ordering::Relaxed),
            audio_packets_received: c.audio_received.load(Ordering::Relaxed),
            audio_packets_dropped: c.audio_dropped.load(Ordering::Relaxed),
            audio_packets_out_of_order: c.audio_out_of_order.load(Ordering::Relaxed),
            audio_packets_missing: c.audio_missing.load(Ordering::Relaxed),
            app_messages_sent: c.messages_sent.load(Ordering::Relaxed),
            app_messages_received: c.messages_received.load(Ordering::Relaxed),
            malformed_messages: c.malformed.load(Ordering::Relaxed),
            events_dropped: c.events_dropped.load(Ordering::Relaxed),
        }
    }
    fn peer(&self) -> Result<Arc<RTCPeerConnection>> {
        self.pc.lock().unwrap().clone().ok_or(Error::Closed)
    }

    fn ensure_open(&self) -> Result<()> {
        if self.is_cancelled() {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }

    /// Create a fully gathered offer, including a reliable ordered JSON channel.
    pub async fn create_offer(&self) -> Result<RTCSessionDescription> {
        self.offer(true).await
    }
    /// Return immediately after setting local SDP; forward IceCandidate events.
    pub async fn create_offer_trickle(&self) -> Result<RTCSessionDescription> {
        self.offer(false).await
    }
    async fn offer(&self, gather: bool) -> Result<RTCSessionDescription> {
        let _guard = self.negotiation.lock().await;
        self.ensure_open()?;
        if !self.ctx.channel_claimed.load(Ordering::Acquire) {
            let channel = self
                .peer()?
                .create_data_channel(&self.ctx.config.data_channel_label, None)
                .await?;
            install_channel(channel, self.ctx.clone()).await;
        }
        let offer = self.peer()?.create_offer(None).await?;
        self.set_local(offer, gather).await
    }
    /// Accept a remote offer and return the fully gathered answer.
    pub async fn answer_offer(&self, sdp: impl Into<String>) -> Result<RTCSessionDescription> {
        self.answer(sdp.into(), true).await
    }
    pub async fn answer_offer_trickle(
        &self,
        sdp: impl Into<String>,
    ) -> Result<RTCSessionDescription> {
        self.answer(sdp.into(), false).await
    }
    async fn answer(&self, sdp: String, gather: bool) -> Result<RTCSessionDescription> {
        if sdp.len() > 262_144 {
            return Err(Error::Signaling("SDP exceeds 256 KiB".into()));
        }
        let _guard = self.negotiation.lock().await;
        self.ensure_open()?;
        self.peer()?
            .set_remote_description(RTCSessionDescription::offer(sdp)?)
            .await?;
        let answer = self.peer()?.create_answer(None).await?;
        self.set_local(answer, gather).await
    }
    async fn set_local(
        &self,
        description: RTCSessionDescription,
        gather: bool,
    ) -> Result<RTCSessionDescription> {
        self.ctx.trickle.store(!gather, Ordering::Release);
        let mut complete = self.peer()?.gathering_complete_promise().await;
        self.peer()?.set_local_description(description).await?;
        if gather {
            tokio::select! { biased;
                _ = self.ctx.cancel.cancelled() => return Err(Error::Closed),
                result = tokio::time::timeout(self.ctx.config.gather_timeout, complete.recv()) => { result.map_err(|_| Error::Timeout("ICE gathering"))?; }
            }
        }
        let local = self
            .peer()?
            .local_description()
            .await
            .ok_or_else(|| Error::Signaling("local description missing".into()))?;
        if gather
            && !local
                .sdp
                .lines()
                .any(|line| line.starts_with("a=candidate:"))
        {
            return Err(Error::Signaling(
                "ICE gathering produced no candidates; check network interfaces and ICE servers"
                    .into(),
            ));
        }
        Ok(local)
    }
    pub async fn accept_answer(&self, answer: RTCSessionDescription) -> Result<()> {
        if answer.sdp.len() > 262_144 {
            return Err(Error::Signaling("SDP exceeds 256 KiB".into()));
        }
        let _guard = self.negotiation.lock().await;
        self.ensure_open()?;
        if answer.sdp_type != RTCSdpType::Answer {
            return Err(Error::Signaling("expected SDP answer".into()));
        }
        Ok(self.peer()?.set_remote_description(answer).await?)
    }
    /// Empty candidate string is the supported end-of-candidates marker.
    pub async fn add_ice_candidate(&self, candidate: RTCIceCandidateInit) -> Result<()> {
        if candidate.candidate.len() > 8192 {
            return Err(Error::Signaling("ICE candidate exceeds 8192 bytes".into()));
        }
        self.ensure_open()?;
        Ok(self.peer()?.add_ice_candidate(candidate).await?)
    }
    pub async fn wait_connected(&self, timeout: Duration) -> Result<()> {
        let mut state = self.subscribe_state();
        let wait = async {
            loop {
                self.ensure_open()?;
                match *state.borrow_and_update() {
                    ConnectionState::Connected => return Ok(()),
                    ConnectionState::Closed
                    | ConnectionState::Closing
                    | ConnectionState::Failed => return Err(Error::Closed),
                    _ => {}
                }
                tokio::select! { biased;
                    _ = self.ctx.cancel.cancelled() => return Err(Error::Closed),
                    result = state.changed() => result.map_err(|_| Error::Closed)?,
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| Error::Timeout("peer connection"))?
    }
    pub async fn wait_data_channel(&self, timeout: Duration) -> Result<()> {
        let mut channel = self.ctx.channel.subscribe();
        tokio::time::timeout(timeout, wait_channel(&self.ctx, &mut channel))
            .await
            .map_err(|_| Error::Timeout("data channel"))??;
        Ok(())
    }
    fn message_text(&self, message: &Value) -> Result<String> {
        self.ensure_open()?;
        let text = serde_json::to_string(message)?;
        if text.len() > self.ctx.config.max_message_bytes {
            return Err(Error::MessageTooLarge {
                size: text.len(),
                limit: self.ctx.config.max_message_bytes,
            });
        }
        Ok(text)
    }
    /// Bounded pre-open buffering; waits when the outgoing queue is full.
    pub async fn send_app_message(&self, message: &Value) -> Result<()> {
        let text = Queued::new(self.message_text(message)?, 0, &self.ctx.pending);
        tokio::select! { biased;
            _ = self.ctx.cancel.cancelled() => Err(Error::Closed),
            result = self.outgoing_messages.send(text) => result.map_err(|_| Error::Closed),
        }
    }
    /// Send the source SmallWebRTC client's raw `ping` heartbeat. It uses the
    /// same bounded queue as application messages; the receiver sends no pong.
    pub async fn send_keepalive(&self) -> Result<()> {
        self.ensure_open()?;
        if self.ctx.config.max_message_bytes < 4 {
            return Err(Error::MessageTooLarge {
                size: 4,
                limit: self.ctx.config.max_message_bytes,
            });
        }
        let text = Queued::new("ping".to_owned(), 0, &self.ctx.pending);
        tokio::select! { biased;
            _ = self.ctx.cancel.cancelled() => Err(Error::Closed),
            result = self.outgoing_messages.send(text) => result.map_err(|_| Error::Closed),
        }
    }
    pub fn try_send_app_message(&self, message: &Value) -> Result<()> {
        let text = Queued::new(self.message_text(message)?, 0, &self.ctx.pending);
        self.outgoing_messages
            .try_send(text)
            .map_err(map_queue_error)
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<()> {
        self.ensure_open()?;
        if self.ctx.receive_only {
            return Err(Error::AudioDisabled);
        }
        self.ctx
            .config
            .audio_codec
            .ok_or(Error::AudioDisabled)?
            .validate_audio(&audio)?;
        let audio = Queued::new(
            audio,
            self.ctx.audio_generation.load(Ordering::Acquire),
            &self.ctx.pending,
        );
        tokio::select! { biased;
            _ = self.ctx.cancel.cancelled() => Err(Error::Closed),
            result = self.outgoing_audio.send(audio) => result.map_err(|_| Error::Closed),
        }
    }
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<()> {
        self.ensure_open()?;
        if self.ctx.receive_only {
            return Err(Error::AudioDisabled);
        }
        self.ctx
            .config
            .audio_codec
            .ok_or(Error::AudioDisabled)?
            .validate_audio(&audio)?;
        let audio = Queued::new(
            audio,
            self.ctx.audio_generation.load(Ordering::Acquire),
            &self.ctx.pending,
        );
        self.outgoing_audio.try_send(audio).map_err(map_queue_error)
    }
    /// Discard queued and not-yet-written audio for a pipeline interruption.
    pub fn interrupt_audio(&self) {
        self.ctx.audio_generation.fetch_add(1, Ordering::AcqRel);
    }

    /// Wait for accepted output to reach the WebRTC stack, not a remote playback receipt.
    pub async fn flush(&self, timeout: Duration) -> Result<()> {
        let wait = async {
            loop {
                let changed = self.ctx.pending.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                self.ensure_open()?;
                if self.ctx.pending.count.load(Ordering::Acquire) == 0 {
                    return Ok(());
                }
                tokio::select! { biased; _ = self.ctx.cancel.cancelled() => return Err(Error::Closed), _ = &mut changed => {} }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| Error::Timeout("outgoing transport drain"))?
    }

    /// Map native pipeline output/control frames to this peer. Unhandled frames return false.
    pub async fn send_frame(&self, frame: &Frame) -> Result<bool> {
        match &frame.data {
            FrameData::OutputAudio(audio)
            | FrameData::TtsAudio(audio)
            | FrameData::SpeechOutputAudio(audio) => self.send_audio(audio.clone()).await?,
            FrameData::OutputTransportMessage(value)
            | FrameData::OutputTransportMessageUrgent(value) => {
                self.send_app_message(value).await?
            }
            FrameData::Interruption | FrameData::InterruptionWorker => self.interrupt_audio(),
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop => {
                self.flush(self.ctx.config.connect_timeout).await?;
                self.close().await?;
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self.close().await?,
            _ => return Ok(false),
        }
        Ok(true)
    }

    /// Single-consumer event stream. Closed connections return None immediately.
    pub async fn recv_event(&self) -> Option<TransportEvent> {
        tokio::select! { biased;
            _ = self.ctx.cancel.cancelled() => None,
            event = async { self.events.lock().await.recv().await } => event,
        }
    }
    /// Synchronous cancellation wakes all blocked producers/readers. Follow with close().
    pub fn cancel(&self) {
        if !self.ctx.cancel.is_cancelled() {
            self.ctx.state.send_replace(ConnectionState::Closing);
        }
        self.ctx.cancel.cancel();
    }
    /// Wait for the peer's one owned shutdown operation. Caller cancellation or
    /// timeout leaves cleanup running; another close call can await it again.
    pub async fn close(&self) -> Result<()> {
        let mut state = self.close_state.lock().await;
        if matches!(*state, CloseState::Idle) {
            *state = CloseState::Running(self.spawn_close(&tokio::runtime::Handle::current()));
        }
        if let CloseState::Running(handle) = &mut *state {
            let result = tokio::time::timeout(self.ctx.config.close_timeout, handle)
                .await
                .map_err(|_| Error::Timeout("peer shutdown"))?
                .unwrap_or_else(|error| Err(format!("shutdown task: {error}")));
            *state = CloseState::Finished(result);
        }
        match &*state {
            CloseState::Finished(result) => result.clone().map_err(Error::Shutdown),
            _ => unreachable!("shutdown either completed or returned a timeout"),
        }
    }

    fn spawn_close(
        &self,
        runtime: &tokio::runtime::Handle,
    ) -> JoinHandle<std::result::Result<(), String>> {
        self.ctx.state.send_replace(ConnectionState::Closing);
        self.ctx.cancel.cancel();
        let pc = self
            .pc
            .lock()
            .unwrap()
            .take()
            .expect("peer owned before shutdown");
        let ctx = self.ctx.clone();
        let tasks = self.tasks.clone();
        let events = self.events.clone();
        runtime.spawn(async move {
            // WebRTC-rs marks its peer closed before asynchronous cleanup. Its
            // close future must not be cancelled, even when our caller times out.
            let result = pc.close().await;
            let transports_closed = pc.sctp().state() == RTCSctpTransportState::Closed
                && pc.dtls_transport().state() == RTCDtlsTransportState::Closed
                && pc.dtls_transport().ice_transport().state() == RTCIceTransportState::Closed;
            let result = normalize_peer_close(result, transports_closed);
            let handles = std::mem::take(&mut *tasks.lock().unwrap());
            for mut handle in handles {
                if tokio::time::timeout(ctx.config.close_timeout, &mut handle)
                    .await
                    .is_err()
                {
                    handle.abort();
                    let _ = handle.await;
                }
            }
            let mut events = events.lock().await;
            events.close();
            while events.try_recv().is_ok() {}
            ctx.channel.send_replace(None);
            // ICE's selected candidate retains its OS socket until the peer is
            // released, even after WebRTC-rs reports successful shutdown.
            drop(pc);
            ctx.state.send_replace(ConnectionState::Closed);
            result
        })
    }
}
impl Drop for SmallWebRtcConnection {
    fn drop(&mut self) {
        self.ctx.cancel.cancel();
        if matches!(self.close_state.get_mut(), CloseState::Idle) {
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let task = self.spawn_close(&runtime);
                *self.close_state.get_mut() = CloseState::Running(task);
            }
        }
        // Dropping a running JoinHandle detaches the owned cleanup operation.
    }
}

const CLOSED_SCTP_RESET: &str = "data_channels: sending reset packet in non-Established state";

fn normalize_peer_close(
    result: std::result::Result<(), webrtc::Error>,
    transports_closed: bool,
) -> std::result::Result<(), String> {
    match result {
        // WebRTC-rs 0.14 flattens close errors into newline-separated strings.
        // A stream reset is redundant after association teardown, but only this
        // exact aggregate is benign after every transport has actually closed.
        Err(webrtc::Error::new(message))
            if transports_closed && message.split('\n').all(|entry| entry == CLOSED_SCTP_RESET) =>
        {
            Ok(())
        }
        result => result.map_err(|error| error.to_string()),
    }
}

fn map_queue_error<T>(error: mpsc::error::TrySendError<T>) -> Error {
    match error {
        mpsc::error::TrySendError::Full(_) => Error::QueueFull,
        mpsc::error::TrySendError::Closed(_) => Error::Closed,
    }
}

fn install_callbacks(pc: &Arc<RTCPeerConnection>, ctx: &Arc<Context>, tasks: &Tasks) {
    let state_ctx = ctx.clone();
    pc.on_peer_connection_state_change(Box::new(move |state| {
        let ctx = state_ctx.clone();
        Box::pin(async move {
            if !ctx.cancel.is_cancelled() {
                ctx.state.send_replace(state.into());
            }
            if state == RTCPeerConnectionState::Failed && !ctx.cancel.is_cancelled() {
                ctx.fail("WebRTC peer connection failed".into());
            } else if state == RTCPeerConnectionState::Closed {
                ctx.cancel.cancel();
            }
        })
    }));
    let ice_ctx = ctx.clone();
    pc.on_ice_candidate(Box::new(move |candidate| {
        let ctx = ice_ctx.clone();
        Box::pin(async move {
            if !ctx.trickle.load(Ordering::Acquire) {
                return;
            }
            let event = match candidate {
                Some(candidate) => match candidate.to_json() {
                    Ok(value) => TransportEvent::IceCandidate(Some(value)),
                    Err(e) => TransportEvent::Error(e.to_string()),
                },
                None => TransportEvent::IceCandidate(None),
            };
            tokio::select! { biased;
                _ = ctx.cancel.cancelled() => {},
                _ = ctx.events.send(event) => {},
            }
        })
    }));
    let channel_ctx = ctx.clone();
    pc.on_data_channel(Box::new(move |channel| {
        let ctx = channel_ctx.clone();
        Box::pin(install_channel(channel, ctx))
    }));
    let track_ctx = ctx.clone();
    let track_tasks = tasks.clone();
    pc.on_track(Box::new(move |track, receiver, _| {
        let ctx = track_ctx.clone();
        let tasks = track_tasks.clone();
        Box::pin(async move {
            if ctx.cancel.is_cancelled() {
                return;
            }
            if track.kind() != RTPCodecType::Audio {
                ctx.emit(TransportEvent::Error(
                    "video tracks are not implemented".into(),
                ));
                return;
            }
            // Static PT=0 may arrive before TrackRemote initializes its metadata.
            // Resolve against negotiated codecs, never infer the encoding from PCM.
            if track.codec().capability.mime_type.is_empty() {
                let parameters = receiver.get_parameters().await;
                if let Some(codec) = parameters
                    .codecs
                    .into_iter()
                    .find(|codec| codec.payload_type == track.payload_type())
                {
                    track.set_codec(codec);
                }
            }
            if ctx.audio_track_claimed.swap(true, Ordering::AcqRel) {
                ctx.emit(TransportEvent::Error(
                    "only one incoming audio track is supported".into(),
                ));
                return;
            }
            let mut tasks = tasks.lock().unwrap();
            if !ctx.cancel.is_cancelled() {
                tasks.push(tokio::spawn(audio_reader(ctx, track)));
            }
        })
    }));
}

async fn install_channel(channel: Arc<RTCDataChannel>, ctx: Arc<Context>) {
    if ctx.channel_claimed.swap(true, Ordering::AcqRel) {
        ctx.emit(TransportEvent::Error(
            "only one application data channel is supported".into(),
        ));
        let _ = channel.close().await;
        return;
    }
    let open_ctx = ctx.clone();
    let weak_channel = Arc::downgrade(&channel);
    channel.on_open(Box::new(move || {
        Box::pin(async move {
            if let Some(channel) = weak_channel.upgrade() {
                if !open_ctx.cancel.is_cancelled() {
                    open_ctx.channel.send_replace(Some(channel));
                    open_ctx.emit(TransportEvent::DataChannelOpen);
                }
            }
        })
    }));
    let close_ctx = ctx.clone();
    channel.on_close(Box::new(move || {
        let ctx = close_ctx.clone();
        Box::pin(async move {
            ctx.channel.send_replace(None);
            ctx.emit(TransportEvent::DataChannelClosed);
        })
    }));
    let message_ctx = ctx.clone();
    channel.on_message(Box::new(move |message| {
        let ctx = message_ctx.clone();
        Box::pin(async move {
            if !message.is_string || message.data.len() > ctx.config.max_message_bytes {
                ctx.counters.malformed.fetch_add(1, Ordering::Relaxed);
                ctx.emit(TransportEvent::Error(
                    "application channel expects bounded UTF-8 JSON text".into(),
                ));
                return;
            }
            // Match the source client's bounded text heartbeat before JSON
            // parsing. Binary, oversized, and invalid UTF-8 payloads still fail.
            if std::str::from_utf8(&message.data).is_ok_and(|text| text.starts_with("ping")) {
                return;
            }
            let value: Value = match serde_json::from_slice(&message.data) {
                Ok(value) => value,
                Err(error) => {
                    ctx.counters.malformed.fetch_add(1, Ordering::Relaxed);
                    ctx.emit(TransportEvent::Error(format!(
                        "invalid application JSON: {error}"
                    )));
                    return;
                }
            };
            let event = if value.get("type").and_then(Value::as_str) == Some("signalling") {
                let signal = value.get("message").cloned().unwrap_or(Value::Null);
                if signal.get("type").and_then(Value::as_str) == Some("trackStatus") {
                    match (
                        signal.get("receiver_index").and_then(Value::as_u64),
                        signal.get("enabled").and_then(Value::as_bool),
                    ) {
                        (Some(0), Some(enabled)) => {
                            ctx.audio_enabled.store(enabled, Ordering::Release);
                            TransportEvent::TrackStatus {
                                receiver_index: 0,
                                enabled,
                            }
                        }
                        _ => {
                            ctx.counters.malformed.fetch_add(1, Ordering::Relaxed);
                            TransportEvent::Error(
                                "trackStatus requires audio receiver_index 0 and boolean enabled"
                                    .into(),
                            )
                        }
                    }
                } else {
                    TransportEvent::Signaling(signal)
                }
            } else {
                TransportEvent::AppMessage(value)
            };
            deliver_message(&ctx, event).await;
        })
    }));
    let error_ctx = ctx;
    channel.on_error(Box::new(move |error| {
        let ctx = error_ctx.clone();
        Box::pin(async move {
            ctx.emit(TransportEvent::Error(format!("data channel: {error}")));
        })
    }));
}

async fn deliver_message(ctx: &Context, event: TransportEvent) {
    tokio::select! { biased;
        _ = ctx.cancel.cancelled() => {},
        permit = ctx.events.reserve() => {
            if let Ok(permit) = permit {
                ctx.counters.messages_received.fetch_add(1, Ordering::Relaxed);
                permit.send(event);
            }
        }
    }
}

async fn wait_channel(
    ctx: &Context,
    channel_rx: &mut watch::Receiver<Option<Arc<RTCDataChannel>>>,
) -> Result<Arc<RTCDataChannel>> {
    loop {
        if ctx.cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        if let Some(channel) = channel_rx.borrow_and_update().clone() {
            if channel.ready_state() == RTCDataChannelState::Open {
                return Ok(channel);
            }
        }
        tokio::select! { biased;
            _ = ctx.cancel.cancelled() => return Err(Error::Closed),
            result = channel_rx.changed() => result.map_err(|_| Error::Closed)?,
        }
    }
}
async fn message_writer(
    ctx: Arc<Context>,
    mut channel_rx: watch::Receiver<Option<Arc<RTCDataChannel>>>,
    mut messages: mpsc::Receiver<Queued<String>>,
) {
    loop {
        let text = tokio::select! { biased; _ = ctx.cancel.cancelled() => break, text = messages.recv() => match text { Some(text) => text, None => break } };
        let channel = match tokio::time::timeout(
            ctx.config.connect_timeout,
            wait_channel(&ctx, &mut channel_rx),
        )
        .await
        {
            Ok(Ok(channel)) => channel,
            Ok(Err(_)) => break,
            Err(_) => {
                ctx.fail("timed out opening application data channel".into());
                break;
            }
        };
        // Bound SCTP's buffered payload as well as the application queue.
        loop {
            if channel
                .buffered_amount()
                .await
                .saturating_add(text.value.len())
                <= ctx.config.max_buffered_bytes
            {
                break;
            }
            tokio::select! { biased; _ = ctx.cancel.cancelled() => return, _ = tokio::time::sleep(Duration::from_millis(5)) => {} }
        }
        tokio::select! { biased;
            _ = ctx.cancel.cancelled() => break,
            result = channel.send_text(text.value.as_str()) => match result {
                Ok(_) => { if !text.value.starts_with("ping") { ctx.counters.messages_sent.fetch_add(1, Ordering::Relaxed); } },
                Err(error) => { ctx.fail(format!("sending application message: {error}")); break; }
            }
        }
    }
}

async fn audio_writer(
    ctx: Arc<Context>,
    track: Arc<TrackLocalStaticSample>,
    mut encoder: Encoder,
    mut audio_rx: mpsc::Receiver<Queued<AudioRawFrame>>,
) {
    let mut state = ctx.state.subscribe();
    let ready = async {
        loop {
            if *state.borrow_and_update() == ConnectionState::Connected {
                return Ok::<_, Error>(());
            }
            tokio::select! { biased; _ = ctx.cancel.cancelled() => return Err(Error::Closed), result = state.changed() => result.map_err(|_| Error::Closed)?, }
        }
    };
    // Start the connection deadline when audio is actually requested.
    let first = tokio::select! { biased; _ = ctx.cancel.cancelled() => return, audio = audio_rx.recv() => match audio { Some(audio) => audio, None => return } };
    match tokio::time::timeout(ctx.config.connect_timeout, ready).await {
        Ok(Ok(())) => {}
        Ok(Err(_)) => return,
        Err(_) => {
            ctx.fail("timed out connecting audio transport".into());
            return;
        }
    }
    let mut pending = Some(first);
    let mut next = tokio::time::Instant::now();
    loop {
        let audio = match pending.take() {
            Some(audio) => audio,
            None => {
                tokio::select! { biased; _ = ctx.cancel.cancelled() => break, audio = audio_rx.recv() => match audio { Some(audio) => audio, None => break } }
            }
        };
        tokio::select! { biased; _ = ctx.cancel.cancelled() => break, _ = tokio::time::sleep_until(next) => {} }
        if audio.generation != ctx.audio_generation.load(Ordering::Acquire) {
            continue;
        }
        let data = match encoder.encode(&audio.value.audio) {
            Ok(data) => data,
            Err(error) => {
                ctx.fail(error.to_string());
                break;
            }
        };
        let sample = Sample {
            data,
            duration: Duration::from_millis(20),
            ..Default::default()
        };
        tokio::select! { biased;
            _ = ctx.cancel.cancelled() => break,
            result = track.write_sample(&sample) => match result {
                Ok(()) => { ctx.counters.audio_sent.fetch_add(1, Ordering::Relaxed); },
                Err(error) => { ctx.fail(format!("sending audio RTP: {error}")); break; }
            }
        }
        // Never burst old audio to catch up after scheduler delays.
        next = tokio::time::Instant::now() + Duration::from_millis(20);
    }
}
async fn audio_reader(ctx: Arc<Context>, track: Arc<TrackRemote>) {
    struct TrackGuard(Arc<Context>);
    impl Drop for TrackGuard {
        fn drop(&mut self) {
            self.0.audio_track_claimed.store(false, Ordering::Release);
        }
    }
    let _track_guard = TrackGuard(ctx.clone());
    let codec = match ctx.config.audio_codec {
        Some(codec)
            if track
                .codec()
                .capability
                .mime_type
                .eq_ignore_ascii_case(codec.mime_type()) =>
        {
            codec
        }
        _ => {
            ctx.emit(TransportEvent::Error(format!(
                "unsupported negotiated audio codec: {}",
                track.codec().capability.mime_type
            )));
            return;
        }
    };
    let mut decoder = match Decoder::new(codec) {
        Ok(decoder) => decoder,
        Err(error) => {
            ctx.fail(error.to_string());
            return;
        }
    };
    let mut previous: Option<u16> = None;
    loop {
        let packet = tokio::select! { biased;
            _ = ctx.cancel.cancelled() => break,
            result = track.read_rtp() => match result { Ok((packet, _)) => packet, Err(error) => { if !ctx.cancel.is_cancelled() { ctx.emit(TransportEvent::Error(format!("audio RTP receive ended: {error}"))); } break; } }
        };
        ctx.counters.audio_received.fetch_add(1, Ordering::Relaxed);
        if let Some(last) = previous {
            let distance = packet.header.sequence_number.wrapping_sub(last);
            if distance == 0 || distance >= 32_768 {
                ctx.counters
                    .audio_out_of_order
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            ctx.counters
                .audio_missing
                .fetch_add(u64::from(distance - 1), Ordering::Relaxed);
        }
        previous = Some(packet.header.sequence_number);
        let pcm = match decoder.decode(&packet.payload) {
            Ok(pcm) => pcm,
            Err(error) => {
                ctx.emit(TransportEvent::Error(error.to_string()));
                continue;
            }
        };
        if !ctx.audio_enabled.load(Ordering::Acquire) {
            continue;
        }
        let frame = AudioRawFrame {
            audio: pcm,
            sample_rate: codec.sample_rate(),
            num_channels: 1,
            context_id: None,
        };
        let event = TransportEvent::Audio(IncomingAudio {
            frame,
            codec,
            sequence_number: packet.header.sequence_number,
            timestamp: packet.header.timestamp,
            ssrc: packet.header.ssrc,
            track_id: track.id(),
        });
        if ctx.events.try_send(event).is_err() {
            ctx.counters.audio_dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn malformed_binary_and_json_messages_are_reported_without_losing_channel() {
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: None,
            ..Default::default()
        };
        let a = SmallWebRtcConnection::new(config.clone()).await.unwrap();
        let b = SmallWebRtcConnection::new(config).await.unwrap();
        let offer = a.create_offer().await.unwrap();
        let answer = b.answer_offer(offer.sdp).await.unwrap();
        a.accept_answer(answer).await.unwrap();
        a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
        let channel = a.ctx.channel.borrow().clone().unwrap();
        channel.send(&Bytes::from_static(b"binary")).await.unwrap();
        channel.send_text("invalid JSON").await.unwrap();
        channel.send_text(r#"{"valid":true}"#).await.unwrap();
        let mut errors = 0;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match b.recv_event().await.unwrap() {
                    TransportEvent::Error(_) => errors += 1,
                    TransportEvent::AppMessage(value) => {
                        assert_eq!(value["valid"], true);
                        break;
                    }
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(errors, 2);
        assert_eq!(b.stats().malformed_messages, 2);
        assert_eq!(b.state(), ConnectionState::Connected);
        a.close().await.unwrap();
        b.close().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn source_ping_heartbeat_is_bounded_text_and_not_an_application_message() {
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: None,
            max_message_bytes: 32,
            ..Default::default()
        };
        let a = SmallWebRtcConnection::new(config.clone()).await.unwrap();
        let b = SmallWebRtcConnection::new(config).await.unwrap();
        let offer = a.create_offer().await.unwrap();
        let answer = b.answer_offer(offer.sdp).await.unwrap();
        a.accept_answer(answer).await.unwrap();
        a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
        a.send_keepalive().await.unwrap();
        a.flush(Duration::from_secs(5)).await.unwrap();
        assert_eq!(a.stats().app_messages_sent, 0);
        let channel = a.ctx.channel.borrow().clone().unwrap();
        channel.send_text("ping123").await.unwrap();
        channel.send_text("pong").await.unwrap();
        channel
            .send_text(format!("ping{}", "x".repeat(40)))
            .await
            .unwrap();
        channel.send(&Bytes::from_static(b"ping")).await.unwrap();
        channel
            .send_text(r#"{"after_heartbeat":true}"#)
            .await
            .unwrap();
        let mut errors = 0;
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match b.recv_event().await.unwrap() {
                    TransportEvent::Error(_) => errors += 1,
                    TransportEvent::AppMessage(value) => {
                        assert_eq!(value, serde_json::json!({"after_heartbeat":true}));
                        break;
                    }
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(errors, 3);
        assert_eq!(b.stats().malformed_messages, 3);
        assert_eq!(b.stats().app_messages_received, 1);
        // The source protocol consumes heartbeats without replying with pong.
        assert!(tokio::time::timeout(Duration::from_millis(30), async {
            loop {
                if matches!(
                    a.recv_event().await,
                    Some(TransportEvent::AppMessage(_) | TransportEvent::Error(_))
                ) {
                    break;
                }
            }
        })
        .await
        .is_err());
        a.close().await.unwrap();
        b.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_after_stopped_sctp_association_releases_native_udp_socket() {
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: None,
            ..Default::default()
        };
        let a = SmallWebRtcConnection::new(config.clone()).await.unwrap();
        let b = SmallWebRtcConnection::new(config).await.unwrap();
        let offer = a.create_offer().await.unwrap();
        let address: std::net::SocketAddr = offer
            .sdp
            .lines()
            .find(|line| line.starts_with("a=candidate:"))
            .map(|line| {
                let fields = line.split_whitespace().collect::<Vec<_>>();
                format!("{}:{}", fields[4], fields[5]).parse().unwrap()
            })
            .unwrap();
        let answer = b.answer_offer(offer.sdp).await.unwrap();
        a.accept_answer(answer).await.unwrap();
        a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
        let channel = a.ctx.channel.borrow().clone().unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered_tx);
        let mut release = Some(release_rx);
        channel.on_message(Box::new(move |_| {
            let entered = entered.take().unwrap();
            let release = release.take().unwrap();
            Box::pin(async move {
                let _ = entered.send(());
                let _ = release.await;
            })
        }));
        b.send_app_message(&serde_json::json!({"hold_read_loop":true}))
            .await
            .unwrap();
        entered_rx.await.unwrap();
        // Hold the channel's read callback while its association is stopped, so
        // its Open state cannot race ahead to Closed before peer shutdown.
        a.peer().unwrap().sctp().stop().await.unwrap();
        assert_eq!(channel.ready_state(), RTCDataChannelState::Open);
        let close_result = a.close().await;
        let _ = release_tx.send(());
        drop(channel);
        let other_close = b.close().await;
        tokio::time::timeout(Duration::from_secs(2), async {
            while std::net::UdpSocket::bind(address).is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("native UDP socket must be released while wrapper is retained");
        close_result.unwrap();
        other_close.unwrap();
    }

    #[test]
    fn shutdown_normalization_preserves_mixed_real_and_unclosed_errors() {
        assert_eq!(
            CLOSED_SCTP_RESET,
            format!(
                "data_channels: {}",
                webrtc::sctp::Error::ErrResetPacketInStateNotExist
            )
        );
        assert!(normalize_peer_close(Ok(()), true).is_ok());
        for message in [
            CLOSED_SCTP_RESET.to_string(),
            format!("{CLOSED_SCTP_RESET}\n{CLOSED_SCTP_RESET}"),
        ] {
            assert!(normalize_peer_close(Err(webrtc::Error::new(message.clone())), true).is_ok());
            assert_eq!(
                normalize_peer_close(Err(webrtc::Error::new(message.clone())), false),
                Err(message)
            );
        }
        for message in [
            format!("{CLOSED_SCTP_RESET}\nice_transport: genuine socket failure"),
            format!("dtls_transport: genuine close failure\n{CLOSED_SCTP_RESET}"),
            format!("{CLOSED_SCTP_RESET}: extra detail"),
            "sending reset packet in non-Established state".into(),
            "data_channels: sending payload data in non-Established state".into(),
            String::new(),
        ] {
            assert_eq!(
                normalize_peer_close(Err(webrtc::Error::new(message.clone())), true),
                Err(message)
            );
        }
        let typed = webrtc::Error::Sctp(webrtc::sctp::Error::ErrResetPacketInStateNotExist);
        let expected = typed.to_string();
        assert_eq!(normalize_peer_close(Err(typed), true), Err(expected));
    }

    #[tokio::test]
    async fn close_during_data_channel_callback_cannot_lose_sctp_shutdown() {
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: None,
            ..Default::default()
        };
        let a = SmallWebRtcConnection::new(config.clone()).await.unwrap();
        let b = SmallWebRtcConnection::new(config).await.unwrap();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered_tx);
        let mut release = Some(release_rx);
        let ctx = b.ctx.clone();
        b.peer().unwrap().on_data_channel(Box::new(move |channel| {
            let entered = entered.take().unwrap();
            let release = release.take().unwrap();
            let ctx = ctx.clone();
            Box::pin(async move {
                install_channel(channel.clone(), ctx).await;
                let _ = entered.send(channel);
                let _ = release.await;
            })
        }));
        let offer = a.create_offer().await.unwrap();
        let answer = b.answer_offer(offer.sdp).await.unwrap();
        let address: std::net::SocketAddr = answer
            .sdp
            .lines()
            .find(|line| line.starts_with("a=candidate:"))
            .map(|line| {
                let fields = line.split_whitespace().collect::<Vec<_>>();
                format!("{}:{}", fields[4], fields[5]).parse().unwrap()
            })
            .unwrap();
        a.accept_answer(answer).await.unwrap();
        let late_channel = tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        b.close().await.unwrap();
        release_tx.send(()).unwrap();
        a.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while std::net::UdpSocket::bind(address).is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("SCTP accept loop must observe close even when it was outside its select");
        assert_ne!(late_channel.ready_state(), RTCDataChannelState::Open);
    }

    #[tokio::test]
    async fn stopped_sctp_transport_rejects_late_association_start() {
        use webrtc::sctp_transport::sctp_transport_capabilities::SCTPTransportCapabilities;
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: None,
            ..Default::default()
        };
        let a = SmallWebRtcConnection::new(config.clone()).await.unwrap();
        let b = SmallWebRtcConnection::new(config).await.unwrap();
        let offer = a.create_offer().await.unwrap();
        let answer = b.answer_offer(offer.sdp).await.unwrap();
        a.accept_answer(answer).await.unwrap();
        a.wait_data_channel(Duration::from_secs(5)).await.unwrap();
        let native = a.peer().unwrap().sctp();
        a.close().await.unwrap();
        let late = native
            .start(
                SCTPTransportCapabilities {
                    max_message_size: 16_384,
                },
                5000,
                5000,
            )
            .await;
        assert!(matches!(late, Err(webrtc::Error::ErrSCTPTransportDTLS)));
        assert_eq!(native.state(), RTCSctpTransportState::Closed);
        drop(native);
        b.close().await.unwrap();
    }

    #[tokio::test]
    async fn close_during_dtls_connected_callback_cannot_admit_late_srtp() {
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: None,
            ..Default::default()
        };
        let a = Arc::new(SmallWebRtcConnection::new(config.clone()).await.unwrap());
        let b = SmallWebRtcConnection::new(config).await.unwrap();
        let dtls = a.peer().unwrap().dtls_transport();
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered_tx);
        let mut release = Some(release_rx);
        dtls.on_state_change(Box::new(move |state| {
            let wait = if state == RTCDtlsTransportState::Connected {
                Some((entered.take().unwrap(), release.take().unwrap()))
            } else {
                None
            };
            Box::pin(async move {
                if let Some((entered, release)) = wait {
                    let _ = entered.send(());
                    let _ = release.await;
                }
            })
        }));
        let offer = a.create_offer().await.unwrap();
        let address: std::net::SocketAddr = offer
            .sdp
            .lines()
            .find(|line| line.starts_with("a=candidate:"))
            .map(|line| {
                let fields = line.split_whitespace().collect::<Vec<_>>();
                format!("{}:{}", fields[4], fields[5]).parse().unwrap()
            })
            .unwrap();
        let answer = b.answer_offer(offer.sdp).await.unwrap();
        a.accept_answer(answer).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), entered_rx)
            .await
            .unwrap()
            .unwrap();
        let close = {
            let a = a.clone();
            tokio::spawn(async move { a.close().await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            while dtls.state() != RTCDtlsTransportState::Closed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release_tx.send(()).unwrap();
        close.await.unwrap().unwrap();
        b.close().await.unwrap();
        drop(dtls);
        tokio::time::timeout(Duration::from_secs(2), async {
            while std::net::UdpSocket::bind(address).is_err() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a stopped DTLS transport cannot publish new SRTP sessions");
    }

    #[tokio::test]
    async fn rejected_or_cancelled_message_admission_is_not_counted() {
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: None,
            queue_capacity: 1,
            ..Default::default()
        };
        let closed = SmallWebRtcConnection::new(config.clone()).await.unwrap();
        closed.events.lock().await.close();
        deliver_message(
            &closed.ctx,
            TransportEvent::AppMessage(serde_json::json!({"rejected":true})),
        )
        .await;
        assert_eq!(closed.stats().app_messages_received, 0);
        closed.close().await.unwrap();
        let cancelled = SmallWebRtcConnection::new(config).await.unwrap();
        cancelled.ctx.emit(TransportEvent::DataChannelOpen);
        let queued = {
            let ctx = cancelled.ctx.clone();
            tokio::spawn(async move {
                deliver_message(
                    &ctx,
                    TransportEvent::AppMessage(serde_json::json!({"cancelled":true})),
                )
                .await;
            })
        };
        cancelled.cancel();
        queued.await.unwrap();
        assert_eq!(cancelled.stats().app_messages_received, 0);
        cancelled.close().await.unwrap();
    }

    #[tokio::test]
    async fn receive_only_has_no_local_track_or_audio_worker() {
        let peer = SmallWebRtcConnection::new_receive_only(SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: Some(AudioCodec::Pcmu),
            ..Default::default()
        })
        .await
        .unwrap();
        let observation = tokio::time::timeout(Duration::from_secs(3), async {
            let native = peer.peer().map_err(|e| format!("{e:?}"))?;
            let mut tracks = 0;
            for sender in native.get_senders().await {
                tracks += usize::from(sender.track().await.is_some());
            }
            // Only the data writer is owned before negotiation; the receive-only
            // branch drops audio_rx and never creates an encoder/track/RTCP reader.
            Ok::<_, String>((
                tracks,
                peer.tasks.lock().unwrap().len(),
                peer.outgoing_audio.is_closed(),
                peer.ctx.pending.count.load(Ordering::Acquire),
            ))
        })
        .await;
        let cleanup = tokio::time::timeout(Duration::from_secs(8), peer.close()).await;
        eprintln!("receive-only initial ownership={observation:?}; close={cleanup:?}");
        assert!(matches!(cleanup, Ok(Ok(()))));
        assert!(matches!(observation, Ok(Ok((0, 1, true, 0)))));
    }

    async fn cancelled_receive_only_gather(answering: bool) {
        use std::{
            future::{poll_fn, Future},
            task::Poll,
        };
        use webrtc::ice_transport::ice_gatherer_state::RTCIceGathererState;
        let config = SmallWebRtcConfig {
            localhost_only: true,
            audio_codec: Some(AudioCodec::Pcmu),
            ..Default::default()
        };
        let target = SmallWebRtcConnection::new_receive_only(config.clone())
            .await
            .unwrap();
        let other = match SmallWebRtcConnection::new(config).await {
            Ok(peer) => peer,
            Err(error) => {
                let cleanup = target.close().await;
                panic!("second constructor={error:?}; cleanup={cleanup:?}");
            }
        };
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let mut entered = Some(entered_tx);
        let mut release = Some(release_rx);
        target
            .peer()
            .unwrap()
            .on_ice_gathering_state_change(Box::new(move |state| {
                let held = if state == RTCIceGathererState::Complete {
                    entered.take().zip(release.take())
                } else {
                    None
                };
                Box::pin(async move {
                    if let Some((entered, release)) = held {
                        let _ = entered.send(());
                        let _ = release.await;
                    }
                })
            }));
        let observation=tokio::time::timeout(Duration::from_secs(6),async {
            let offer=if answering {Some(other.create_offer().await.map_err(|e|format!("{e:?}"))?)} else {None};
            let operation=async { match offer {Some(offer)=>target.answer_offer(offer.sdp).await,None=>target.create_offer().await} };
            tokio::pin!(operation);
            tokio::select! {
                result=&mut operation=>return Err(format!("negotiation completed before held gathering callback: {result:?}")),
                entered=entered_rx=>entered.map_err(|e|format!("gathering callback: {e:?}"))?,
            }
            let pending=poll_fn(|cx|Poll::Ready(operation.as_mut().poll(cx).is_pending())).await;
            if !pending {return Err("gathering future was not physically Pending".into());}
            target.cancel();
            let result=tokio::time::timeout(Duration::from_secs(1),&mut operation).await;
            eprintln!("receive-only answering={answering}; held actual gathering Pending; cancel outcome={result:?}");
            if !matches!(result,Ok(Err(Error::Closed))) {return Err(format!("cancel did not terminate held gathering: {result:?}"));}
            Ok::<_,String>(())
        }).await;
        // Always release the actual callback before close, even on any failed observation.
        let _ = release_tx.send(());
        let mut addresses = vec![];
        for peer in [&target, &other] {
            if let Ok(native) = peer.peer() {
                if let Some(description) = native.local_description().await {
                    for line in description
                        .sdp
                        .lines()
                        .filter(|line| line.starts_with("a=candidate:"))
                    {
                        let fields = line.split_whitespace().collect::<Vec<_>>();
                        if fields.len() >= 6 {
                            if let Ok(address) = format!("{}:{}", fields[4], fields[5])
                                .parse::<std::net::SocketAddr>()
                            {
                                addresses.push(address);
                            }
                        }
                    }
                }
            }
        }
        let (a, b) = tokio::join!(
            tokio::time::timeout(Duration::from_secs(8), target.close()),
            tokio::time::timeout(Duration::from_secs(8), other.close())
        );
        let sockets = tokio::time::timeout(Duration::from_secs(2), async {
            while addresses
                .iter()
                .any(|address| std::net::UdpSocket::bind(address).is_err())
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        eprintln!(
            "receive-only cancelled negotiation={observation:?}; joins={a:?}/{b:?}; candidate_count={}; release={sockets:?}",
            addresses.len()
        );
        assert!(
            matches!(a, Ok(Ok(()))) && matches!(b, Ok(Ok(()))),
            "close joins failed"
        );
        assert!(
            !addresses.is_empty() && sockets.is_ok(),
            "real gathered sockets must be released"
        );
        assert!(matches!(observation, Ok(Ok(()))), "{observation:?}");
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn receive_only_cancel_during_pending_offer_gathering_joins() {
        cancelled_receive_only_gather(false).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn receive_only_cancel_during_pending_answer_gathering_joins() {
        cancelled_receive_only_gather(true).await;
    }
}
