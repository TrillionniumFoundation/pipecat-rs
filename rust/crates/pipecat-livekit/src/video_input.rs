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
};

use futures_util::StreamExt;
use livekit::{
    prelude::TrackSource,
    webrtc::{
        video_frame::{native::VideoFrameBufferExt, BoxVideoFrame, VideoFormatType},
        video_stream::native::{NativeVideoStream, NativeVideoStreamOptions},
        video_track::RtcVideoTrack,
    },
};
use pipecat_core::ImageRawFrame;
use tokio::{
    sync::{mpsc, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use crate::{Error, LiveKitEvent, Result};

#[derive(Debug, Clone)]
pub struct VideoInputConfig {
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
        if !(1..=16).contains(&self.max_tracks)
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
type Gates = Arc<Mutex<HashMap<Key, Stamp>>>;

/// Never exposed in the cloneable public event. Permits survive every private transfer.
pub(crate) struct QueuedVideo {
    key: Key,
    stamp: Stamp,
    event: LiveKitEvent,
    _bytes: OwnedSemaphorePermit,
    _pending: OwnedSemaphorePermit,
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
        while let Some(queued) = self.rx.recv().await {
            // Linearization boundary: validation and removal from adapter accounting occur
            // under the same gate as lifecycle invalidation. Already admitted events cannot
            // be recalled from callers or downstream processors.
            let gates = self.gates.lock().unwrap();
            if !self.cancel.is_cancelled() && gates.get(&queued.key) == Some(&queued.stamp) {
                let QueuedVideo {
                    event,
                    _bytes,
                    _pending,
                    ..
                } = queued;
                drop(_bytes);
                drop(_pending);
                return Some(event);
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
}
struct Reader {
    stamp: Stamp,
    stop: CancellationToken,
    task: JoinHandle<Result<()>>,
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
        let lane = Lane {
            tx,
            bytes: Arc::new(Semaphore::new(config.max_retained_bytes)),
            pending: Arc::new(Semaphore::new(config.queue_capacity)),
            stats: Arc::new(Stats::default()),
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
    ) -> Result<()> {
        if key.identity.len().saturating_add(sid.len()) > 65_536 {
            return Err(Error::Limit("video track metadata"));
        }
        if self.cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        if self
            .known
            .get(&key)
            .is_some_and(|(current, _)| current == &sid)
        {
            return Ok(());
        }
        // Cancel AND join before removing the slot or allocating the replacement sink.
        self.stop(&key, None).await?;
        if self.known.len() >= self.config.max_tracks {
            return Err(Error::Limit("subscribed video readers"));
        }
        self.known.insert(key.clone(), (sid.clone(), track.clone()));
        if muted {
            return Ok(());
        }
        self.start_reader(key, sid, track)
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
    pub(crate) async fn stop(&mut self, key: &Key, sid: Option<&str>) -> Result<()> {
        if self
            .known
            .get(key)
            .is_some_and(|(current, _)| sid.is_none_or(|sid| current == sid))
        {
            self.known.remove(key);
        } else {
            return Ok(());
        }
        self.stop_reader(key, sid).await
    }
    async fn stop_reader(&mut self, key: &Key, sid: Option<&str>) -> Result<()> {
        let Some(reader) = self.readers.get_mut(key) else {
            return Ok(());
        };
        if sid.is_some_and(|sid| reader.stamp.sid != sid) {
            return Ok(());
        }
        self.gates.lock().unwrap().remove(key);
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
            self.gates.lock().unwrap().remove(&key);
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
            result = crate::publication::combine_cleanup(result, self.stop(&key, None).await);
        }
        result
    }
    pub(crate) async fn close(&mut self) -> Result<()> {
        self.gates.lock().unwrap().clear();
        for reader in self.readers.values() {
            reader.stop.cancel();
        }
        let keys: Vec<_> = self.known.keys().cloned().collect();
        let mut result = Ok(());
        for key in keys {
            result = crate::publication::combine_cleanup(result, self.stop(&key, None).await);
        }
        result
    }
}
impl Drop for VideoReaders {
    fn drop(&mut self) {
        self.gates.lock().unwrap().clear();
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
        _bytes: permit,
        _pending: pending,
    }))
}
fn deliver_frame(
    frame: BoxVideoFrame,
    key: &Key,
    stamp: &Stamp,
    lane: &Lane,
    config: &VideoInputConfig,
) -> Result<()> {
    let converted = match convert(frame, key.clone(), stamp.clone(), lane, config) {
        Ok(frame) => frame,
        Err(error) => {
            lane.stats.reject(error.to_string());
            return Ok(());
        }
    };
    if let Some(queued) = converted {
        match lane.tx.try_send(queued) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                lane.stats.dropped.fetch_add(1, Ordering::Relaxed);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(Error::Closed),
        }
    } else {
        lane.stats.dropped.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}
async fn read_video(
    mut stream: NativeVideoStream,
    key: Key,
    stamp: Stamp,
    lane: Lane,
    cancel: CancellationToken,
    config: VideoInputConfig,
) -> Result<()> {
    let result = async {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Ok(()),
                frame = stream.next() => {
                    let Some(frame) = frame else { return Ok(()); };
                    match deliver_frame(frame, &key, &stamp, &lane, &config) { Ok(()) => {}, Err(Error::Closed) => return Ok(()), Err(error) => return Err(error) }
                }
            }
        }
    }.await;
    stream.close();
    result
}

#[cfg(test)]
mod tests;

/// Adapter-only counters. Rejection does not terminate unrelated audio/data delivery.
#[derive(Debug, Clone, Default)]
pub struct VideoInputStats {
    pub rejected_frames: u64,
    pub dropped_frames: u64,
    pub last_rejection: Option<String>,
}
#[derive(Default)]
pub(crate) struct Stats {
    rejected: AtomicU64,
    dropped: AtomicU64,
    last: Mutex<Option<String>>,
}
impl Stats {
    fn reject(&self, reason: String) {
        self.rejected.fetch_add(1, Ordering::Relaxed);
        *self.last.lock().unwrap() = Some(reason.chars().take(256).collect());
    }
    pub(crate) fn snapshot(&self) -> VideoInputStats {
        VideoInputStats {
            rejected_frames: self.rejected.load(Ordering::Relaxed),
            dropped_frames: self.dropped.load(Ordering::Relaxed),
            last_rejection: self.last.lock().unwrap().clone(),
        }
    }
}
