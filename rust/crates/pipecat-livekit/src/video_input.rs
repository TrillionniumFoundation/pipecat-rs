//! Bounded, opt-in post-decode video input. Native decoder allocations are not bounded here.
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Poll,
    time::Instant,
};

use futures_util::{Stream, StreamExt};
use livekit::{
    prelude::TrackSource,
    webrtc::{
        video_frame::{native::VideoFrameBufferExt, BoxVideoFrame, VideoFormatType},
        video_stream::native::{NativeVideoStream, NativeVideoStreamOptions},
        video_track::RtcVideoTrack,
    },
};
use pipecat_core::{Frame, ImageRawFrame, InvocationFence};
use pipecat_media_transport::video::{VideoInSampler, VideoInSourceParams};
#[cfg(test)]
use std::time::Duration;
use tokio::{
    sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{Error, LiveKitEvent, Result};
mod requests;
pub use requests::ImageRequestConfig;

#[derive(Debug, Clone)]
pub struct VideoInputConfig {
    /// Disabled by default. Plain pipeline requests observe post-drain SDK input,
    /// which can contain keepalive/replayed pixels, not proven new captures.
    pub requests: Option<ImageRequestConfig>,
    /// Empty captures every source. Nonempty maps allow only listed sources, unless overridden.
    pub sources: HashMap<VideoSource, VideoCaptureMode>,
    /// Includes retained schedules and pre-subscription overrides. No eviction/fallback.
    pub max_capture_policies: usize,
    pub max_policy_identity_bytes: usize,
    pub max_tracks: usize,
    /// Positive native sink capacity; zero (SDK unbounded mode) is rejected.
    pub native_queue_capacity: usize,
    pub queue_capacity: usize,
    pub max_dimension: u32,
    pub max_pixels: usize,
    /// Retained RGBA allocation, including capacity after RGB compaction.
    pub max_frame_bytes: usize,
    pub max_retained_bytes: usize,
}
impl Default for VideoInputConfig {
    fn default() -> Self {
        Self {
            requests: None,
            sources: HashMap::new(),
            max_capture_policies: 16,
            max_policy_identity_bytes: 65_536,
            max_tracks: 4,
            native_queue_capacity: 1,
            queue_capacity: 4,
            max_dimension: 4096,
            max_pixels: 2_073_600,
            max_frame_bytes: 8 * 1024 * 1024,
            max_retained_bytes: 32 * 1024 * 1024,
        }
    }
}
impl VideoInputConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(requests) = &self.requests {
            requests.validate()?;
        }
        for mode in self.sources.values() {
            if *mode == VideoCaptureMode::RequestOnly && self.requests.is_none() {
                return Err(Error::InvalidConfig(
                    "request-only capture requires image requests",
                ));
            }
            mode.params()?;
        }
        if !(1..=128).contains(&self.max_capture_policies)
            || !(1..=65_536).contains(&self.max_policy_identity_bytes)
            || !(1..=16).contains(&self.max_tracks)
            || !(1..=4).contains(&self.native_queue_capacity)
            || !(1..=16).contains(&self.queue_capacity)
            || !(1..=4096).contains(&self.max_dimension)
            || !(1..=2_073_600).contains(&self.max_pixels)
            || !(4..=8 * 1024 * 1024).contains(&self.max_frame_bytes)
            || self.max_retained_bytes < self.max_frame_bytes
            || self.max_retained_bytes > 64 * 1024 * 1024
        {
            return Err(Error::InvalidConfig("invalid incoming video limits"));
        }
        Ok(())
    }
    fn layout(&self, width: u32, height: u32) -> Result<(usize, u32)> {
        if width == 0 || height == 0 || width > self.max_dimension || height > self.max_dimension {
            return Err(Error::Limit("incoming video dimensions"));
        }
        let pixels = (width as usize)
            .checked_mul(height as usize)
            .ok_or(Error::Limit("incoming video pixels"))?;
        let bytes = pixels
            .checked_mul(4)
            .ok_or(Error::Limit("incoming video bytes"))?;
        let stride = width
            .checked_mul(4)
            .filter(|v| *v <= i32::MAX as u32)
            .ok_or(Error::Limit("incoming video stride"))?;
        if pixels > self.max_pixels
            || bytes > self.max_frame_bytes
            || i32::try_from(bytes).is_err()
            || i32::try_from(height).is_err()
        {
            return Err(Error::Limit("incoming video allocation"));
        }
        Ok((bytes, stride))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VideoSource {
    Camera,
    ScreenVideo,
}
impl VideoSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Camera => "camera",
            Self::ScreenVideo => "screenVideo",
        }
    }
    pub(crate) fn from_track(source: TrackSource) -> Self {
        if source == TrackSource::Screenshare {
            Self::ScreenVideo
        } else {
            Self::Camera
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct Key {
    pub identity: String,
    pub source: VideoSource,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Stamp {
    sid: String,
    generation: u64,
}
/// Supported capture policies. RequestOnly requires opt-in pipeline requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCaptureMode {
    AllFrames,
    RequestOnly,
    FramesPerSecond(u32),
}
impl VideoCaptureMode {
    fn params(self) -> Result<VideoInSourceParams> {
        match self {
            Self::AllFrames => Ok(VideoInSourceParams::all_frames()),
            Self::RequestOnly => Ok(VideoInSourceParams::on_request_only()),
            Self::FramesPerSecond(rate) if (1..=1_000_000_000).contains(&rate) => {
                Ok(VideoInSourceParams {
                    framerate: Some(rate),
                })
            }
            _ => Err(Error::InvalidConfig(
                "video sampling requires 1..=1000000000 fps or RequestOnly",
            )),
        }
    }
}
#[derive(Default)]
struct GateState {
    requests: requests::Requests,
    active: HashMap<Key, Stamp>,
    policies: HashMap<Key, VideoInSampler<()>>,
    identity_bytes: usize,
    closed: bool,
    #[cfg(test)]
    now: Option<Duration>,
}
type Gates = Arc<Mutex<GateState>>;

#[derive(Clone)]
pub(crate) struct CaptureHandle {
    gates: Gates,
    config: VideoInputConfig,
    cancel: CancellationToken,
    origin: Instant,
    request_resources: requests::RequestResources,
    request_changed: Arc<Notify>,
    stats: Arc<Stats>,
}
impl CaptureHandle {
    // Validation and mutation are atomic. No command queue or pending string retention.
    pub(crate) fn capture(
        &self,
        identity: &str,
        source: VideoSource,
        mode: VideoCaptureMode,
    ) -> Result<()> {
        let params = mode.params()?;
        if mode == VideoCaptureMode::RequestOnly && self.config.requests.is_none() {
            return Err(Error::InvalidConfig(
                "request-only capture requires image requests",
            ));
        }
        if identity.is_empty() || identity.len() > 65_536 {
            return Err(Error::Limit("video capture identity"));
        }
        let mut state = self.gates.lock().unwrap();
        if state.closed || self.cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        let key = Key {
            identity: identity.into(),
            source,
        };
        if let Some(sampler) = state.policies.get_mut(&key) {
            sampler
                .set_params(params)
                .map_err(|_| Error::InvalidConfig("video sampling rate"))?;
        } else {
            self.insert(&mut state, key, params)?;
        }
        Ok(())
    }
    fn insert(&self, state: &mut GateState, key: Key, params: VideoInSourceParams) -> Result<()> {
        let bytes = state.identity_bytes.saturating_add(key.identity.len());
        if state.policies.len() >= self.config.max_capture_policies
            || bytes > self.config.max_policy_identity_bytes
        {
            return Err(Error::Limit("video capture policies"));
        }
        let sampler = VideoInSampler::new(params, 1)
            .map_err(|_| Error::InvalidConfig("video sampling rate"))?;
        state.policies.insert(key, sampler);
        state.identity_bytes = bytes;
        Ok(())
    }
    fn sample(&self, key: &Key, stamp: &Stamp) -> Result<bool> {
        let mut state = self.gates.lock().unwrap();
        if state.closed || self.cancel.is_cancelled() || state.active.get(key) != Some(stamp) {
            return Ok(false);
        }
        let now = self.origin.elapsed();
        #[cfg(test)]
        let now = state.now.unwrap_or(now);
        if !state.policies.contains_key(key) {
            let mode = if self.config.sources.is_empty() {
                // Preserve all-frame behavior without accumulating unnecessary policy keys.
                return Ok(true);
            } else if let Some(mode) = self.config.sources.get(&key.source) {
                *mode
            } else {
                return Ok(false);
            };
            self.insert(&mut state, key.clone(), mode.params()?)?;
        }
        Ok(state
            .policies
            .get_mut(key)
            .expect("installed policy")
            .sample(now)
            .0)
    }
}

/// Never exposed in the cloneable public event. Permits survive every private transfer.
pub(crate) struct QueuedVideo {
    key: Key,
    stamp: Stamp,
    event: LiveKitEvent,
    requested: Option<RequestedImage>,
    _bytes: OwnedSemaphorePermit,
    _pending: OwnedSemaphorePermit,
}
pub(crate) struct RequestedImage {
    pub(crate) frame: Frame,
    pub(crate) fence: InvocationFence,
    pub(crate) token: u64,
    pub(crate) output: pipecat_core::ProcessorContext,
}
pub(crate) enum InputEvent {
    Event(LiveKitEvent),
    Requested(Box<RequestedImage>),
}
pub(crate) struct VideoReceiver {
    rx: mpsc::Receiver<QueuedVideo>,
    gates: Gates,
    cancel: CancellationToken,
}
impl VideoReceiver {
    pub(crate) fn close(&mut self) {
        self.rx.close();
        while self.rx.try_recv().is_ok() {}
    }
    pub(crate) async fn recv(&mut self) -> Option<LiveKitEvent> {
        while let Some(event) = self.recv_input().await {
            if let InputEvent::Event(event) = event {
                return Some(event);
            }
            // Public recv is intentionally not a request consumer. Mixed use
            // with the pipeline is unsupported; never strip private authority.
        }
        None
    }
    pub(crate) async fn recv_input(&mut self) -> Option<InputEvent> {
        while let Some(queued) = self.rx.recv().await {
            let valid = {
                let gates = self.gates.lock().unwrap();
                !gates.closed
                    && !self.cancel.is_cancelled()
                    && gates.active.get(&queued.key) == Some(&queued.stamp)
            };
            if valid {
                return Some(match queued.requested {
                    Some(request) => InputEvent::Requested(Box::new(request)),
                    None => InputEvent::Event(queued.event),
                });
            }
        }
        None
    }
}
#[derive(Clone)]
struct Lane {
    tx: mpsc::Sender<QueuedVideo>,
    bytes: Arc<Semaphore>,
    pending: Arc<Semaphore>,
    stats: Arc<Stats>,
    capture: CaptureHandle,
}
struct Reader {
    stamp: Stamp,
    stop: CancellationToken,
    task: JoinHandle<Result<()>>,
}
/// Effective ownership changes, not SDK callback counts or pixel delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnershipTransition {
    Changed,
    Unchanged,
}
pub(crate) struct VideoReaders {
    config: VideoInputConfig,
    lane: Lane,
    gates: Gates,
    readers: HashMap<Key, Reader>,
    next_generation: u64,
    cancel: CancellationToken,
    known: HashMap<Key, (String, RtcVideoTrack)>,
}
impl VideoReaders {
    pub(crate) fn new(
        config: VideoInputConfig,
        cancel: CancellationToken,
    ) -> (Self, VideoReceiver) {
        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let gates = Gates::default();
        let receiver = VideoReceiver {
            rx,
            gates: gates.clone(),
            cancel: cancel.clone(),
        };
        let stats = Arc::new(Stats::default());
        let lane = Lane {
            tx,
            bytes: Arc::new(Semaphore::new(config.max_retained_bytes)),
            pending: Arc::new(Semaphore::new(config.queue_capacity)),
            stats: stats.clone(),
            capture: CaptureHandle {
                gates: gates.clone(),
                config: config.clone(),
                cancel: cancel.clone(),
                origin: Instant::now(),
                request_resources: requests::RequestResources::new(config.requests.as_ref()),
                request_changed: Arc::new(Notify::new()),
                stats,
            },
        };
        (
            Self {
                config,
                lane,
                gates,
                readers: HashMap::new(),
                next_generation: 0,
                cancel,
                known: HashMap::new(),
            },
            receiver,
        )
    }
    pub(crate) async fn start(
        &mut self,
        key: Key,
        sid: String,
        track: RtcVideoTrack,
        muted: bool,
    ) -> Result<OwnershipTransition> {
        if key.identity.len().saturating_add(sid.len()) > 65_536 {
            return Err(Error::Limit("video track metadata"));
        }
        if self.cancel.is_cancelled() || self.gates.lock().unwrap().closed {
            return Err(Error::Closed);
        }
        if self
            .known
            .get(&key)
            .is_some_and(|(current, _)| current == &sid)
        {
            return Ok(OwnershipTransition::Unchanged);
        }
        // Cancel AND join before removing the slot or allocating the replacement sink.
        self.stop(&key, None).await?;
        if self.known.len() >= self.config.max_tracks {
            return Err(Error::Limit("subscribed video readers"));
        }
        self.known.insert(key.clone(), (sid.clone(), track.clone()));
        if !muted {
            self.start_reader(key, sid, track)?;
        }
        Ok(OwnershipTransition::Changed)
    }
    fn start_reader(&mut self, key: Key, sid: String, track: RtcVideoTrack) -> Result<()> {
        let generation = self
            .next_generation
            .checked_add(1)
            .ok_or(Error::Limit("video generation"))?;
        self.next_generation = generation;
        let stamp = Stamp { sid, generation };
        let stream = NativeVideoStream::with_options(
            track,
            NativeVideoStreamOptions {
                queue_size_frames: Some(self.config.native_queue_capacity),
            },
        );
        let stop = self.cancel.child_token();
        self.gates
            .lock()
            .unwrap()
            .active
            .insert(key.clone(), stamp.clone());
        let task = tokio::spawn(read_video(
            stream,
            key.clone(),
            stamp.clone(),
            self.lane.clone(),
            stop.clone(),
            self.config.clone(),
        ));
        self.readers.insert(key, Reader { stamp, stop, task });
        Ok(())
    }
    pub(crate) fn capture_handle(&self) -> CaptureHandle {
        self.lane.capture.clone()
    }
    pub(crate) fn stats(&self) -> Arc<Stats> {
        self.lane.stats.clone()
    }
    pub(crate) async fn mute(&mut self, key: &Key, sid: &str) -> Result<()> {
        if self
            .known
            .get(key)
            .is_some_and(|(current, _)| current == sid)
        {
            self.stop_reader(key, Some(sid)).await?;
        }
        Ok(())
    }
    pub(crate) fn unmute(&mut self, key: &Key, sid: &str) -> Result<()> {
        if self.readers.contains_key(key) {
            return Ok(());
        }
        if let Some((current, track)) = self.known.get(key).cloned() {
            if current == sid {
                self.start_reader(key.clone(), current, track)?;
            }
        }
        Ok(())
    }
    pub(crate) async fn stop(
        &mut self,
        key: &Key,
        sid: Option<&str>,
    ) -> Result<OwnershipTransition> {
        if self
            .known
            .get(key)
            .is_some_and(|(current, _)| sid.is_none_or(|sid| current == sid))
        {
            self.known.remove(key);
        } else {
            return Ok(OwnershipTransition::Unchanged);
        }
        self.stop_reader(key, sid).await?;
        Ok(OwnershipTransition::Changed)
    }
    async fn stop_reader(&mut self, key: &Key, sid: Option<&str>) -> Result<()> {
        let Some(reader) = self.readers.get_mut(key) else {
            return Ok(());
        };
        if sid.is_some_and(|sid| reader.stamp.sid != sid) {
            return Ok(());
        }
        self.gates.lock().unwrap().active.remove(key);
        self.lane.capture.maintain_requests();
        reader.stop.cancel();
        let result = (&mut reader.task)
            .await
            .map_err(|e| Error::Sdk(format!("video reader: {e}")))
            .and_then(|r| r);
        self.readers.remove(key);
        result
    }
    /// Poll actual owned join handles: even a reader panic wakes the room owner.
    /// No completion notifier can be lost or block a cancellation join.
    pub(crate) async fn next_finished(&mut self) -> Result<()> {
        let (key, stamp, result) = futures_util::future::poll_fn(|cx| {
            for (key, reader) in &mut self.readers {
                if let Poll::Ready(result) = Pin::new(&mut reader.task).poll(cx) {
                    return Poll::Ready((key.clone(), reader.stamp.clone(), result));
                }
            }
            Poll::Pending
        })
        .await;
        self.finish_joined(
            key,
            stamp,
            result
                .map_err(|e| Error::Sdk(format!("video reader: {e}")))
                .and_then(|r| r),
        )
    }
    fn finish_joined(&mut self, key: Key, stamp: Stamp, result: Result<()>) -> Result<()> {
        if self.readers.get(&key).is_some_and(|r| r.stamp == stamp) {
            self.gates.lock().unwrap().active.remove(&key);
            self.lane.capture.maintain_requests();
            self.readers.remove(&key);
            self.known.remove(&key);
            result?;
        }
        Ok(())
    }
    pub(crate) async fn remove_participant(&mut self, identity: &str) -> Result<()> {
        let keys: Vec<_> = self
            .known
            .keys()
            .filter(|k| k.identity == identity)
            .cloned()
            .collect();
        let mut result = Ok(());
        for key in keys {
            result = crate::publication::combine_cleanup(
                result,
                self.stop(&key, None).await.map(|_| ()),
            );
        }
        let mut state = self.gates.lock().unwrap();
        state.policies.retain(|key, _| key.identity != identity);
        state.identity_bytes = state.policies.keys().map(|key| key.identity.len()).sum();
        result
    }
    pub(crate) async fn close(&mut self) -> Result<()> {
        {
            let mut state = self.gates.lock().unwrap();
            state.closed = true;
            state.policies.clear();
            state.identity_bytes = 0;
        }
        self.lane.capture.close_requests();
        self.reset_readers().await
    }
    /// Reconnect teardown invalidates readers, not capture policy or cadence.
    pub(crate) async fn reset_readers(&mut self) -> Result<()> {
        self.gates.lock().unwrap().active.clear();
        self.lane.capture.maintain_requests();
        for reader in self.readers.values() {
            reader.stop.cancel();
        }
        let keys: Vec<_> = self.known.keys().cloned().collect();
        let mut result = Ok(());
        for key in keys {
            result = crate::publication::combine_cleanup(
                result,
                self.stop(&key, None).await.map(|_| ()),
            );
        }
        result
    }
}
impl Drop for VideoReaders {
    fn drop(&mut self) {
        {
            let mut state = self.gates.lock().unwrap();
            state.closed = true;
            state.active.clear();
            state.policies.clear();
            state.identity_bytes = 0;
        }
        self.lane.capture.close_requests();
        for reader in self.readers.values() {
            reader.stop.cancel();
        }
    }
}

fn convert(
    frame: BoxVideoFrame,
    key: Key,
    stamp: Stamp,
    lane: &Lane,
    config: &VideoInputConfig,
) -> Result<Option<QueuedVideo>> {
    let buffer = frame.buffer.as_ref();
    let (width, height) = (buffer.width(), buffer.height());
    let (bytes, stride) = config.layout(width, height)?;
    // Restrict to the concrete, inspected no-copy I420 conversion. Other SDK routes
    // may copy, unwrap a platform handle, or contain todo!/assertions.
    let i420 = buffer
        .as_i420()
        .ok_or(Error::Video("incoming buffer must be I420"))?;
    let (sy, su, sv) = i420.strides();
    let cw = width.div_ceil(2);
    let ch = height.div_ceil(2);
    for (stride, columns, rows) in [(sy, width, height), (su, cw, ch), (sv, cw, ch)] {
        if stride < columns
            || i32::try_from(stride).is_err()
            || stride
                .checked_mul(rows)
                .filter(|n| *n <= i32::MAX as u32)
                .is_none()
        {
            return Err(Error::Video("invalid I420 stride"));
        }
    }
    let (y, u, v) = i420.data();
    if y.len() < sy as usize * height as usize
        || u.len() < su as usize * ch as usize
        || v.len() < sv as usize * ch as usize
    {
        return Err(Error::Video("invalid I420 planes"));
    }
    let Ok(pending) = lane.pending.clone().try_acquire_owned() else {
        return Ok(None);
    };
    let Ok(permit) = lane.bytes.clone().try_acquire_many_owned(bytes as u32) else {
        return Ok(None);
    };
    let mut rgba = vec![0; bytes];
    // SDK ABGR uses libyuv I420ToABGR: bytes in memory are R,G,B,A.
    i420.to_argb(
        VideoFormatType::ABGR,
        &mut rgba,
        stride,
        width as i32,
        height as i32,
    );
    for pixel in 0..bytes / 4 {
        rgba.copy_within(pixel * 4..pixel * 4 + 3, pixel * 3);
    }
    rgba.truncate(bytes / 4 * 3); // Capacity remains RGBA-sized and stays fully charged.
    let event = LiveKitEvent::Video {
        identity: key.identity.clone(),
        track_sid: stamp.sid.clone(),
        source: key.source,
        image: ImageRawFrame {
            image: rgba,
            size: (width, height),
            format: Some("RGB".into()),
        },
    };
    crate::protocol::validate_event(&event)?;
    Ok(Some(QueuedVideo {
        key,
        stamp,
        event,
        requested: None,
        _bytes: permit,
        _pending: pending,
    }))
}
#[cfg(test)]
fn deliver_frame(
    frame: BoxVideoFrame,
    key: &Key,
    stamp: &Stamp,
    lane: &Lane,
    config: &VideoInputConfig,
) -> Result<()> {
    deliver_observation(frame, key, stamp, lane, config, 0)
}
fn deliver_observation(
    frame: BoxVideoFrame,
    key: &Key,
    stamp: &Stamp,
    lane: &Lane,
    config: &VideoInputConfig,
    cutoff: u64,
) -> Result<()> {
    let selected = match lane.capture.sample_requested(key, stamp, cutoff) {
        Ok((true, selected)) => selected,
        Ok((false, _)) => {
            lane.stats.sampled_out.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        Err(error) => {
            lane.stats.reject(error.to_string());
            return Ok(());
        }
    };
    let token = selected.as_ref().map(|request| request.token);
    // Reject oversize requested images before native conversion, without
    // narrowing the existing ordinary video path or selecting a second request.
    if selected.is_some() {
        let (w, h) = (frame.buffer.width(), frame.buffer.height());
        if w == 0 || h == 0 || w > 1024 || h > 1024 || u64::from(w) * u64::from(h) > 262_144 {
            lane.capture.fail_request(
                token.unwrap(),
                "requested RGB dimensions exceed core limits",
            );
            return Ok(());
        }
    }
    let mut queued = match convert(frame, key.clone(), stamp.clone(), lane, config) {
        Ok(Some(queued)) => queued,
        Ok(None) => {
            lane.stats.dropped.fetch_add(1, Ordering::Relaxed);
            if let Some(token) = token {
                lane.capture
                    .fail_request(token, "requested image queue or bytes full");
            }
            return Ok(());
        }
        Err(error) => {
            lane.stats.reject(error.to_string());
            if let Some(token) = token {
                lane.capture
                    .fail_request(token, "requested image conversion rejected");
            }
            return Ok(());
        }
    };
    if let Some(selected) = selected {
        // Compact only the requested route. The ordinary permit still charges
        // RGBA capacity during compaction; the request reserves another MAX_RGB.
        let event = std::mem::replace(&mut queued.event, LiveKitEvent::Error(String::new()));
        let mut response = match event.into_frame() {
            Ok(Some(response)) => response,
            _ => {
                lane.capture
                    .fail_request(token.unwrap(), "requested image projection rejected");
                return Ok(());
            }
        };
        let pipecat_core::FrameData::UserImage(image) = &mut response.data else {
            unreachable!()
        };
        image.image.image = std::mem::take(&mut image.image.image)
            .into_boxed_slice()
            .into_vec();
        let fence = selected.fence.clone();
        if !lane.capture.current_request(selected.token) {
            lane.capture
                .fail_request(token.unwrap(), "requested image invalidated before binding");
            return Ok(());
        }
        let output = match selected.bind(&mut response) {
            Ok(output) => output,
            Err(_) => {
                lane.capture
                    .fail_request(token.unwrap(), "requested image binding rejected");
                return Ok(());
            }
        };
        queued.requested = Some(RequestedImage {
            frame: response,
            fence,
            token: token.unwrap(),
            output,
        });
    }
    match lane.tx.try_send(queued) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => {
            lane.stats.dropped.fetch_add(1, Ordering::Relaxed);
            if let Some(token) = token {
                lane.capture
                    .fail_request(token, "requested image queue full");
            }
            Ok(())
        }
        Err(mpsc::error::TrySendError::Closed(_)) => {
            if let Some(token) = token {
                lane.capture
                    .fail_request(token, "requested image receiver closed");
            }
            Err(Error::Closed)
        }
    }
}

/// Establish a reader-owned empty-queue fence. Each poll does bounded work;
/// producers preventing Pending cause yielding/expiry, never a false fence.
async fn drain_to_pending<S: Stream<Item = BoxVideoFrame> + Unpin>(
    stream: &mut S,
    lane: &Lane,
) -> bool {
    futures_util::future::poll_fn(|cx| {
        for _ in 0..8 {
            match Pin::new(&mut *stream).poll_next(cx) {
                Poll::Ready(Some(_)) => {}
                Poll::Ready(None) => return Poll::Ready(false),
                Poll::Pending => return Poll::Ready(true),
            }
        }
        lane.capture.maintain_requests();
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await
}
async fn read_video(
    mut stream: NativeVideoStream,
    key: Key,
    stamp: Stamp,
    lane: Lane,
    cancel: CancellationToken,
    config: VideoInputConfig,
) -> Result<()> {
    let result = read_observations(&mut stream, key, stamp, lane, cancel, config).await;
    stream.close();
    result
}
async fn read_observations<S: Stream<Item = BoxVideoFrame> + Unpin>(
    stream: &mut S,
    key: Key,
    stamp: Stamp,
    lane: Lane,
    cancel: CancellationToken,
    config: VideoInputConfig,
) -> Result<()> {
    let mut armed = 0;
    loop {
        // Register before observing the cutoff to avoid a lost admission wake.
        let changed = lane.capture.request_changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let cutoff = lane.capture.request_cutoff(&key, &stamp);
        if cutoff > armed {
            let fenced = tokio::select! { biased;
                _ = cancel.cancelled() => return Ok(()),
                result = tokio::time::timeout(config.requests.as_ref().expect("request cutoff requires opt-in").timeout, drain_to_pending(stream, &lane)) => match result {
                    Ok(fenced) => fenced,
                    Err(_) => { lane.capture.maintain_requests(); continue; }
                },
            };
            if !fenced {
                return Ok(());
            }
            // Arm only this pre-drain cutoff. A later request needs its own fence.
            armed = cutoff;
            continue;
        }
        // This value belongs to the poll, not to selection later in the loop.
        let eligible = armed;
        tokio::select! { biased;
            _ = cancel.cancelled() => return Ok(()),
            _ = &mut changed => continue,
            frame = stream.next() => {
                let Some(frame) = frame else { return Ok(()); };
                match deliver_observation(frame, &key, &stamp, &lane, &config, eligible) {
                    Ok(()) => {}, Err(Error::Closed) => return Ok(()), Err(error) => return Err(error)
                }
            }
        }
    }
}

#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod request_pipeline_tests;
#[cfg(test)]
mod tests;

/// Adapter-only counters. Rejection does not terminate unrelated audio/data delivery.
#[derive(Debug, Clone, Default)]
pub struct VideoInputStats {
    pub rejected_frames: u64,
    pub dropped_frames: u64,
    /// Policy-excluded, not-due or invalidated before conversion; not overload.
    pub sampled_out_frames: u64,
    pub last_rejection: Option<String>,
    pub rejected_requests: u64,
    pub failed_requests: u64,
    /// Local routing attempts only; never a claim of enqueue, consume or delivery.
    pub request_dispatch_attempts: u64,
}
#[derive(Default)]
pub(crate) struct Stats {
    rejected_requests: AtomicU64,
    failed_requests: AtomicU64,
    dispatch_attempts: AtomicU64,
    rejected: AtomicU64,
    dropped: AtomicU64,
    sampled_out: AtomicU64,
    last: Mutex<Option<String>>,
}
impl Stats {
    pub(crate) fn reject_request(&self, reason: String) {
        self.rejected_requests.fetch_add(1, Ordering::Relaxed);
        *self.last.lock().unwrap() = Some(reason.chars().take(256).collect());
    }
    fn reject(&self, reason: String) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        *self.last.lock().unwrap() = Some(reason.chars().take(256).collect());
    }
    pub(crate) fn snapshot(&self) -> VideoInputStats {
        VideoInputStats {
            rejected_requests: self.rejected_requests.load(Ordering::Relaxed),
            failed_requests: self.failed_requests.load(Ordering::Relaxed),
            request_dispatch_attempts: self.dispatch_attempts.load(Ordering::Relaxed),
            rejected_frames: self.rejected.load(Ordering::Relaxed),
            dropped_frames: self.dropped.load(Ordering::Relaxed),
            sampled_out_frames: self.sampled_out.load(Ordering::Relaxed),
            last_rejection: self.last.lock().unwrap().clone(),
        }
    }
}
