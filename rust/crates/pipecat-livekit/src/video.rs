//! Bounded raw camera output. No device capture, scheduling or remote receipt claim.
use std::sync::{Arc, Mutex};

use livekit::{
    prelude::LocalVideoTrack,
    webrtc::{
        native::yuv_helper::abgr_to_i420,
        video_frame::{I420Buffer, VideoFrame, VideoRotation},
        video_source::{native::NativeVideoSource, RtcVideoSource, VideoResolution},
    },
};
use pipecat_core::{FrameData, ImageRawFrame, OutputImageRawFrame};
use tokio_util::sync::CancellationToken;

use crate::{Error, Result};

/// Borrow-only view of both core image variants; never clones raw pixel storage.
#[derive(Clone, Copy)]
pub(crate) struct VideoInput<'a> {
    image: &'a ImageRawFrame,
    sync_with_audio: bool,
}
impl<'a> From<&'a ImageRawFrame> for VideoInput<'a> {
    fn from(image: &'a ImageRawFrame) -> Self {
        Self {
            image,
            sync_with_audio: false,
        }
    }
}
impl<'a> From<&'a OutputImageRawFrame> for VideoInput<'a> {
    fn from(output: &'a OutputImageRawFrame) -> Self {
        Self {
            image: &output.image,
            sync_with_audio: output.sync_with_audio,
        }
    }
}
pub(crate) fn video_input(data: &FrameData) -> Option<VideoInput<'_>> {
    match data {
        FrameData::OutputImage(image) => Some(image.into()),
        FrameData::OutputImageDetailed(output) => Some(output.into()),
        _ => None,
    }
}

/// One fixed-size, room-wide camera track. Enable with `LiveKitConfig::video_output`.
#[derive(Debug, Clone)]
pub struct VideoOutputConfig {
    pub width: u32,
    pub height: u32,
    /// Dedicated pipeline lane, independent of audio/data queue capacity; 1..=4.
    pub queue_capacity: usize,
    /// Exact raw payload length AND retained Vec capacity must fit; at most 8 MiB.
    pub max_frame_bytes: usize,
    /// Bounds queued/in-flight raw payload plus one RGBA scratch and I420 buffer.
    /// Excludes SDK encoder/sink storage and general frame metadata. At most 64 MiB.
    pub max_retained_bytes: usize,
}
impl Default for VideoOutputConfig {
    fn default() -> Self {
        Self {
            width: 640,
            height: 480,
            queue_capacity: 4,
            max_frame_bytes: 8 * 1024 * 1024,
            max_retained_bytes: 64 * 1024 * 1024,
        }
    }
}
#[derive(Clone, Copy)]
struct Layout {
    pixels: usize,
    chroma: usize,
    i420: usize,
    rgba: usize,
}
impl VideoOutputConfig {
    fn layout(&self) -> Result<Layout> {
        let fail = || Error::Limit("video dimensions or checked allocation");
        if self.width == 0 || self.height == 0 || self.width > 4096 || self.height > 4096 {
            return Err(fail());
        }
        let w = usize::try_from(self.width).map_err(|_| fail())?;
        let h = usize::try_from(self.height).map_err(|_| fail())?;
        let pixels = w.checked_mul(h).ok_or_else(fail)?;
        if pixels > 2_073_600 {
            return Err(fail());
        }
        let chroma = w.div_ceil(2).checked_mul(h.div_ceil(2)).ok_or_else(fail)?;
        let i420 = chroma
            .checked_mul(2)
            .and_then(|n| n.checked_add(pixels))
            .ok_or_else(fail)?;
        let rgba = pixels.checked_mul(4).ok_or_else(fail)?;
        Ok(Layout {
            pixels,
            chroma,
            i420,
            rgba,
        })
    }
    pub(crate) fn validate(&self) -> Result<()> {
        let layout = self.layout()?;
        if !(1..=4).contains(&self.queue_capacity)
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > 8 * 1024 * 1024
            || self.max_retained_bytes > 64 * 1024 * 1024
        {
            return Err(Error::InvalidConfig("invalid video queue or byte limit"));
        }
        let retained = self
            .queue_capacity
            .checked_add(1)
            .and_then(|n| n.checked_mul(self.max_frame_bytes))
            .and_then(|n| n.checked_add(layout.rgba))
            .and_then(|n| n.checked_add(layout.i420))
            .ok_or(Error::Limit("video aggregate allocation"))?;
        if retained > self.max_retained_bytes || layout.i420 > self.max_frame_bytes {
            return Err(Error::Limit(
                "video aggregate or minimum I420 payload budget",
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_image<'a>(
        &self,
        output: impl Into<VideoInput<'a>>,
        destination: Option<&str>,
    ) -> Result<()> {
        let output = output.into();
        self.validate()?;
        if destination.is_some() {
            return Err(Error::Video("per-destination video is unsupported"));
        }
        if output.sync_with_audio {
            return Err(Error::Video("audio-synchronized video is unsupported"));
        }
        let image = &output.image;
        if image.size != (self.width, self.height) {
            return Err(Error::Video(
                "video dimensions differ from configured fixed size",
            ));
        }
        let layout = self.layout()?;
        let expected = match image.format.as_deref() {
            Some("RGB") => layout
                .pixels
                .checked_mul(3)
                .ok_or(Error::Limit("RGB bytes"))?,
            Some("RGBA" | "BGRA" | "ARGB") => layout.rgba,
            Some("I420") => layout.i420,
            _ => {
                return Err(Error::Video(
                    "explicit RGB, RGBA, BGRA, ARGB or packed I420 required",
                ))
            }
        };
        if expected > self.max_frame_bytes || image.image.capacity() > self.max_frame_bytes {
            return Err(Error::Limit("video frame bytes or retained capacity"));
        }
        if image.image.len() != expected {
            return Err(Error::Video("video payload length must match exactly"));
        }
        Ok(())
    }
}

/// Native adapter result, NOT encoding, network publication, or remote acknowledgement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCaptureOutcome {
    ForwardedToSource,
    AdaptationDropped,
}

/// Owns initialized SDK planes. Pipeline PTS is intentionally not an SDK timestamp.
pub(crate) fn to_sdk_video<'a>(
    output: impl Into<VideoInput<'a>>,
    config: &VideoOutputConfig,
) -> Result<VideoFrame<I420Buffer>> {
    let output = output.into();
    config.validate_image(output, None)?;
    let layout = config.layout()?;
    let mut buffer = I420Buffer::new_black(config.width, config.height);
    let strides = buffer.strides();
    let (y, u, v) = buffer.data_mut();
    // Initialize full planes including any padding before writing/conversion.
    y.fill(0);
    u.fill(128);
    v.fill(128);
    let input = &output.image.image;
    if output.image.format.as_deref() == Some("I420") {
        let (src_y, chroma) = input.split_at(layout.pixels);
        let (src_u, src_v) = chroma.split_at(layout.chroma);
        for (src, dst, width, stride) in [
            (src_y, y, config.width as usize, strides.0 as usize),
            (
                src_u,
                u,
                config.width.div_ceil(2) as usize,
                strides.1 as usize,
            ),
            (
                src_v,
                v,
                config.width.div_ceil(2) as usize,
                strides.2 as usize,
            ),
        ] {
            for (row, out) in src.chunks_exact(width).zip(dst.chunks_exact_mut(stride)) {
                out[..width].copy_from_slice(row);
            }
        }
    } else {
        let mut rgba = vec![0; layout.rgba];
        let format = output.image.format.as_deref().expect("validated format");
        let step = if format == "RGB" { 3 } else { 4 };
        // Validated layout.rgba is checked pixels * 4, so the remainder is empty.
        let (rgba_pixels, _) = rgba.as_chunks_mut::<4>();
        for (src, dst) in input.chunks_exact(step).zip(rgba_pixels.iter_mut()) {
            let rgb = match format {
                "RGB" | "RGBA" => [src[0], src[1], src[2]],
                "BGRA" => [src[2], src[1], src[0]],
                "ARGB" => [src[1], src[2], src[3]],
                _ => unreachable!("validated format"),
            };
            dst.copy_from_slice(&[rgb[0], rgb[1], rgb[2], 255]); // discard alpha, no compositing
        }
        // libyuv's ABGR denotes register order: its bytes are R,G,B,A.
        // Fixed dimension/pixel limits prove all SDK u32 products/i32 casts safe.
        let stride = config
            .width
            .checked_mul(4)
            .ok_or(Error::Limit("RGBA stride"))?;
        abgr_to_i420(
            &rgba,
            stride,
            y,
            strides.0,
            u,
            strides.1,
            v,
            strides.2,
            config.width as i32,
            config.height as i32,
        );
    }
    Ok(VideoFrame::new(VideoRotation::VideoRotation0, buffer))
}

pub(crate) struct NativeCamera {
    source: NativeVideoSource,
    pub(crate) track: LocalVideoTrack,
}
impl NativeCamera {
    pub(crate) fn new(config: &VideoOutputConfig) -> Result<Self> {
        config.validate()?;
        let source = NativeVideoSource::new(
            VideoResolution {
                width: config.width,
                height: config.height,
            },
            false,
        );
        let track = LocalVideoTrack::create_video_track(
            "pipecat-video",
            RtcVideoSource::Native(source.clone()),
        );
        Ok(Self { source, track })
    }
}
trait VideoSink {
    fn capture(&self, frame: &VideoFrame<I420Buffer>) -> bool;
}
impl VideoSink for NativeCamera {
    fn capture(&self, frame: &VideoFrame<I420Buffer>) -> bool {
        self.source.capture_frame(frame)
    }
}
struct Gate<S> {
    sink: Option<S>,
    generation: u64,
}
impl<S: VideoSink> Gate<S> {
    fn capture<'a>(
        &self,
        output: impl Into<VideoInput<'a>>,
        config: &VideoOutputConfig,
        expected: Option<u64>,
        cancel: &CancellationToken,
    ) -> Result<VideoCaptureOutcome> {
        let sink = self.sink.as_ref().ok_or(Error::Closed)?;
        if cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        if expected.is_some_and(|generation| generation != self.generation) {
            return Err(Error::Interrupted);
        }
        let frame = to_sdk_video(output, config)?;
        if cancel.is_cancelled() {
            return Err(Error::Closed);
        }
        Ok(if sink.capture(&frame) {
            VideoCaptureOutcome::ForwardedToSource
        } else {
            VideoCaptureOutcome::AdaptationDropped
        })
    }
}
/// Clones share one capture/close lock; native source/track clones never escape it.
#[derive(Clone)]
pub(crate) struct VideoPublisher(Arc<Mutex<Gate<NativeCamera>>>);
impl VideoPublisher {
    pub(crate) fn new(camera: NativeCamera) -> Self {
        Self(Arc::new(Mutex::new(Gate {
            sink: Some(camera),
            generation: 0,
        })))
    }
    pub(crate) fn generation(&self) -> u64 {
        self.0.lock().unwrap().generation
    }
    pub(crate) fn interrupt(&self) {
        let mut gate = self.0.lock().unwrap();
        gate.generation = gate.generation.wrapping_add(1);
    }
    pub(crate) fn close(&self) {
        self.0.lock().unwrap().sink.take();
    }
    pub(crate) fn capture<'a>(
        &self,
        output: impl Into<VideoInput<'a>>,
        config: &VideoOutputConfig,
        expected: Option<u64>,
        cancel: &CancellationToken,
    ) -> Result<VideoCaptureOutcome> {
        self.0
            .lock()
            .unwrap()
            .capture(output, config, expected, cancel)
    }
}

#[cfg(test)]
mod tests;
