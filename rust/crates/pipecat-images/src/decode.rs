use crate::{CancellationToken, ImageError, ImageGenerationLimits, Result};
use pipecat_core::ImageRawFrame;
use std::{
    io::{self, BufRead, Cursor, Read, Seek, SeekFrom},
    sync::{mpsc, Arc},
    thread,
    time::Instant,
};

pub(crate) struct CancelOnDrop(pub CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
#[derive(Clone)]
pub(crate) struct DecodeControl {
    pub cancel: CancellationToken,
    pub caller: CancellationToken,
    pub deadline: Instant,
}
impl DecodeControl {
    fn check(&self) -> Result<()> {
        if self.cancel.is_cancelled() || self.caller.is_cancelled() {
            Err(ImageError::Cancelled)
        } else if Instant::now() >= self.deadline {
            Err(ImageError::Timeout)
        } else {
            Ok(())
        }
    }
}
struct Input<'a> {
    cursor: Cursor<&'a [u8]>,
    control: &'a DecodeControl,
}
impl Read for Input<'_> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.control.check().map_err(io::Error::other)?;
        // Decoder read-ahead remains cancellable at bounded intervals.
        let length = buffer.len().min(8192);
        self.cursor.read(&mut buffer[..length])
    }
}
impl BufRead for Input<'_> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.control.check().map_err(io::Error::other)?;
        let bytes = self.cursor.fill_buf()?;
        Ok(&bytes[..bytes.len().min(8192)])
    }
    fn consume(&mut self, n: usize) {
        self.cursor.consume(n);
    }
}
impl Seek for Input<'_> {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.control.check().map_err(io::Error::other)?;
        self.cursor.seek(position)
    }
}
struct Job {
    _permit: Arc<tokio::sync::OwnedSemaphorePermit>,
    max_output_bytes: usize,
    bytes: Vec<u8>,
    control: DecodeControl,
    result: tokio::sync::oneshot::Sender<Result<ImageRawFrame>>,
}
/// One owned native thread per client, bounded by the client's admitted jobs.
/// Last-owner drop closes the lane and joins it; no detached blocking tasks.
pub(crate) struct DecoderWorker {
    sender: Option<mpsc::SyncSender<Job>>,
    thread: Option<thread::JoinHandle<()>>,
}
impl DecoderWorker {
    pub fn new(limits: ImageGenerationLimits) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<Job>(limits.max_pending);
        let thread = thread::Builder::new()
            .name("pipecat-image-decoder".into())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    if job.result.is_closed() {
                        continue;
                    }
                    let mut job_limits = limits.clone();
                    job_limits.max_pixels = job_limits.max_pixels.min(job.max_output_bytes / 3);
                    let result = decode(&job.bytes, &job_limits, &job.control);
                    let _ = job.result.send(result);
                }
            })
            .map_err(|_| ImageError::Closed)?;
        Ok(Self {
            sender: Some(sender),
            thread: Some(thread),
        })
    }
    pub async fn decode(
        &self,
        bytes: Vec<u8>,
        control: DecodeControl,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) -> Result<ImageRawFrame> {
        self.decode_shared(bytes, control, Arc::new(permit), usize::MAX)
            .await
    }
    /// Shared sequence admission survives cancellation until owned decoder work exits.
    pub(crate) async fn decode_shared(
        &self,
        bytes: Vec<u8>,
        control: DecodeControl,
        permit: Arc<tokio::sync::OwnedSemaphorePermit>,
        max_output_bytes: usize,
    ) -> Result<ImageRawFrame> {
        let (result, receive) = tokio::sync::oneshot::channel();
        self.sender
            .as_ref()
            .ok_or(ImageError::Closed)?
            .try_send(Job {
                _permit: permit,
                max_output_bytes,
                bytes,
                control,
                result,
            })
            .map_err(|error| match error {
                mpsc::TrySendError::Full(_) => ImageError::Backpressure,
                mpsc::TrySendError::Disconnected(_) => ImageError::Closed,
            })?;
        receive.await.map_err(|_| ImageError::Closed)?
    }
}
impl Drop for DecoderWorker {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
fn decode(
    bytes: &[u8],
    limits: &ImageGenerationLimits,
    control: &DecodeControl,
) -> Result<ImageRawFrame> {
    control.check()?;
    if bytes.len() > limits.max_encoded_image_bytes {
        return Err(ImageError::TooLarge);
    }
    let input = Input {
        cursor: Cursor::new(bytes),
        control,
    };
    let result = if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        png_decode(bytes, input, limits, control)
    } else if bytes.starts_with(&[0xff, 0xd8]) {
        jpeg_decode(bytes, input, limits, control)
    } else {
        Err(ImageError::Decode)
    };
    control.check()?;
    result
}
fn png_decode(
    bytes: &[u8],
    input: Input<'_>,
    limits: &ImageGenerationLimits,
    control: &DecodeControl,
) -> Result<ImageRawFrame> {
    // Validate dimensions before the decoder can reserve image-dependent storage.
    if bytes.len() < 33
        || &bytes[12..16] != b"IHDR"
        || u32::from_be_bytes(bytes[8..12].try_into().unwrap()) != 13
    {
        return Err(ImageError::Decode);
    }
    let width = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
    let height = u32::from_be_bytes(bytes[20..24].try_into().unwrap());
    let pixels = limits.pixels(width, height, 16)?;
    let mut offset = 8;
    let mut chunks = 0;
    let mut ended = false;
    while offset < bytes.len() {
        control.check()?;
        chunks += 1;
        if chunks > 65536 {
            return Err(ImageError::TooLarge);
        }
        let header = bytes.get(offset..offset + 8).ok_or(ImageError::Decode)?;
        let length = u32::from_be_bytes(header[..4].try_into().unwrap()) as usize;
        let end = offset
            .checked_add(length)
            .and_then(|n| n.checked_add(12))
            .ok_or(ImageError::TooLarge)?;
        if end > bytes.len() {
            return Err(ImageError::Decode);
        }
        if &header[4..] == b"acTL" {
            return Err(ImageError::Decode);
        }
        if &header[4..] == b"IEND" {
            if length != 0 || end != bytes.len() {
                return Err(ImageError::Decode);
            }
            ended = true;
        }
        offset = end;
    }
    if !ended {
        return Err(ImageError::Decode);
    }
    let mut decoder = png::Decoder::new_with_limits(
        input,
        png::Limits {
            bytes: limits.max_decode_bytes,
        },
    );
    decoder.set_ignore_text_chunk(true);
    decoder.set_ignore_iccp_chunk(true);
    decoder.set_transformations(png::Transformations::EXPAND | png::Transformations::STRIP_16);
    let mut reader = decoder.read_info().map_err(|_| ImageError::Decode)?;
    let size = reader.output_buffer_size().ok_or(ImageError::TooLarge)?;
    if size > pixels.checked_mul(4).ok_or(ImageError::TooLarge)? {
        return Err(ImageError::TooLarge);
    }
    let mut decoded = vec![0; size];
    let info = reader
        .next_frame(&mut decoded)
        .map_err(|_| ImageError::Decode)?;
    reader.finish().map_err(|_| ImageError::Decode)?;
    if info.width != width || info.height != height || info.bit_depth != png::BitDepth::Eight {
        return Err(ImageError::Decode);
    }
    let mut rgb = Vec::with_capacity(pixels * 3);
    let stride = match info.color_type {
        png::ColorType::Rgb => 3,
        png::ColorType::Rgba => 4,
        png::ColorType::Grayscale => 1,
        png::ColorType::GrayscaleAlpha => 2,
        _ => return Err(ImageError::Decode),
    };
    if info.buffer_size() != pixels * stride {
        return Err(ImageError::Decode);
    }
    for row in decoded[..info.buffer_size()].chunks(width as usize * stride) {
        control.check()?;
        for pixel in row.chunks_exact(stride) {
            if stride < 3 {
                rgb.extend_from_slice(&[pixel[0]; 3]);
            } else {
                rgb.extend_from_slice(&pixel[..3]);
            }
        }
    }
    Ok(ImageRawFrame {
        image: rgb,
        size: (width, height),
        format: Some("RGB".into()),
    })
}
fn jpeg_decode(
    bytes: &[u8],
    input: Input<'_>,
    limits: &ImageGenerationLimits,
    control: &DecodeControl,
) -> Result<ImageRawFrame> {
    // Reject exotic/lossless coding before decoder allocation and bound sampling.
    let mut offset = 2;
    let mut dimensions = None;
    let mut count = 0;
    let mut scans = 0;
    let mut ended = false;
    while offset < bytes.len() {
        control.check()?;
        count += 1;
        if count > 4096 {
            return Err(ImageError::TooLarge);
        }
        if bytes[offset] != 0xff {
            return Err(ImageError::Decode);
        }
        while bytes.get(offset) == Some(&0xff) {
            offset += 1;
        }
        let marker = *bytes.get(offset).ok_or(ImageError::Decode)?;
        offset += 1;
        if marker == 0xd9 {
            ended = offset == bytes.len();
            break;
        }
        let size = bytes.get(offset..offset + 2).ok_or(ImageError::Decode)?;
        let size = u16::from_be_bytes(size.try_into().unwrap()) as usize;
        if size < 2 {
            return Err(ImageError::Decode);
        }
        let data = bytes
            .get(offset + 2..offset.checked_add(size).ok_or(ImageError::TooLarge)?)
            .ok_or(ImageError::Decode)?;
        if (0xc0..=0xcf).contains(&marker) && ![0xc4, 0xc8, 0xcc].contains(&marker) {
            if ![0xc0, 0xc1, 0xc2].contains(&marker)
                || data.len() < 6
                || data[0] != 8
                || dimensions.is_some()
            {
                return Err(ImageError::Decode);
            }
            let height = u16::from_be_bytes([data[1], data[2]]) as u32;
            let width = u16::from_be_bytes([data[3], data[4]]) as u32;
            let components = data[5] as usize;
            if ![1, 3, 4].contains(&components) || data.len() != 6 + components * 3 {
                return Err(ImageError::Decode);
            }
            for component in data[6..].as_chunks::<3>().0 {
                if !(1..=2).contains(&(component[1] >> 4))
                    || !(1..=2).contains(&(component[1] & 15))
                {
                    return Err(ImageError::Decode);
                }
            }
            limits.pixels(width, height, 64)?;
            dimensions = Some((width, height));
        }
        offset += size;
        if marker == 0xda {
            scans += 1;
            if scans > 64 {
                return Err(ImageError::TooLarge);
            }
            // Entropy data may contain escaped FF bytes and restart markers.
            loop {
                control.check()?;
                let end = offset.saturating_add(8192).min(bytes.len());
                let Some(relative) = bytes[offset..end].iter().position(|&b| b == 0xff) else {
                    offset = end;
                    if offset == bytes.len() {
                        return Err(ImageError::Decode);
                    }
                    continue;
                };
                offset += relative;
                let begin = offset;
                while bytes.get(offset) == Some(&0xff) {
                    offset += 1;
                }
                let next = *bytes.get(offset).ok_or(ImageError::Decode)?;
                if next == 0 || (0xd0..=0xd7).contains(&next) {
                    offset += 1;
                } else {
                    offset = begin;
                    break;
                }
            }
        }
    }
    if !ended || scans == 0 {
        return Err(ImageError::Decode);
    }
    let dimensions = dimensions.ok_or(ImageError::Decode)?;
    let pixels = limits.pixels(dimensions.0, dimensions.1, 64)?;
    let mut decoder = jpeg_decoder::Decoder::new(input);
    decoder.set_max_decoding_buffer_size(limits.max_decode_bytes);
    decoder.read_info().map_err(|_| ImageError::Decode)?;
    let info = decoder.info().ok_or(ImageError::Decode)?;
    if (info.width as u32, info.height as u32) != dimensions {
        return Err(ImageError::Decode);
    }
    let decoded = decoder.decode().map_err(|_| ImageError::Decode)?;
    control.check()?;
    let mut rgb = Vec::with_capacity(pixels * 3);
    use jpeg_decoder::PixelFormat;
    let stride = info.pixel_format.pixel_bytes();
    if decoded.len() != pixels * stride {
        return Err(ImageError::Decode);
    }
    for row in decoded.chunks(dimensions.0 as usize * stride) {
        control.check()?;
        for pixel in row.chunks_exact(stride) {
            match info.pixel_format {
                PixelFormat::RGB24 => rgb.extend_from_slice(pixel),
                PixelFormat::L8 => rgb.extend_from_slice(&[pixel[0]; 3]),
                PixelFormat::CMYK32 => {
                    let black = 255 - pixel[3] as u16;
                    for value in &pixel[..3] {
                        rgb.push(((255 - *value as u16) * black / 255) as u8);
                    }
                }
                _ => return Err(ImageError::Decode),
            }
        }
    }
    Ok(ImageRawFrame {
        image: rgb,
        size: dimensions,
        format: Some("RGB".into()),
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    fn control() -> DecodeControl {
        DecodeControl {
            cancel: CancellationToken::new(),
            caller: CancellationToken::new(),
            deadline: Instant::now() + std::time::Duration::from_secs(10),
        }
    }
    #[test]
    fn limits_before_allocation_and_cancel() {
        let mut bytes = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR".to_vec();
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
        bytes.resize(33, 0);
        assert_eq!(
            decode(&bytes, &Default::default(), &control()),
            Err(ImageError::TooLarge)
        );
        let token = control();
        token.cancel.cancel();
        assert_eq!(
            decode(b"bad", &Default::default(), &token),
            Err(ImageError::Cancelled)
        );
        assert_eq!(
            decode(b"GIF89a", &Default::default(), &control()),
            Err(ImageError::Decode)
        );
    }
}

#[cfg(test)]
mod independent_review {
    use super::*;
    use std::sync::Arc;
    #[tokio::test]
    async fn queued_cancelled_decode_releases_owned_permit_and_last_owner_joins() {
        let limits = ImageGenerationLimits {
            max_pending: 1,
            ..Default::default()
        };
        let worker = DecoderWorker::new(limits).unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let caller = CancellationToken::new();
        caller.cancel();
        let control = DecodeControl {
            cancel: CancellationToken::new(),
            caller,
            deadline: Instant::now() + std::time::Duration::from_secs(1),
        };
        let permit = permits.clone().try_acquire_owned().unwrap();
        assert_eq!(permits.available_permits(), 0);
        assert_eq!(
            worker.decode(vec![0; 100], control, permit).await,
            Err(ImageError::Cancelled)
        );
        drop(worker); // join is synchronous; all queued ownership must be released now.
        assert_eq!(permits.available_permits(), 1);
    }
    #[test]
    fn codec_entry_honors_expired_deadlines_before_any_parsing() {
        let control = DecodeControl {
            cancel: CancellationToken::new(),
            caller: CancellationToken::new(),
            deadline: Instant::now(),
        };
        assert_eq!(
            decode(b"not an image", &Default::default(), &control),
            Err(ImageError::Timeout)
        );
    }
}

#[cfg(test)]
mod shared_sequence_tests {
    use super::*;
    #[tokio::test]
    async fn shared_sequence_admission_survives_decode_then_releases_on_last_owner() {
        let worker = DecoderWorker::new(ImageGenerationLimits::default()).unwrap();
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(permits.clone().try_acquire_owned().unwrap());
        let control = DecodeControl {
            cancel: CancellationToken::new(),
            caller: CancellationToken::new(),
            deadline: Instant::now() + std::time::Duration::from_secs(1),
        };
        let caller = control.caller.clone();
        caller.cancel();
        assert_eq!(
            worker
                .decode_shared(vec![0; 100], control, permit.clone(), 100)
                .await,
            Err(ImageError::Cancelled)
        );
        assert_eq!(permits.available_permits(), 0);
        drop(worker);
        assert_eq!(permits.available_permits(), 0);
        drop(permit);
        assert_eq!(permits.available_permits(), 1);
    }
    #[test]
    fn cancelled_job_keeps_admission_after_request_owner_is_dropped() {
        let permits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::new(permits.clone().try_acquire_owned().unwrap());
        let (result, receive) = tokio::sync::oneshot::channel();
        let cancel = CancellationToken::new();
        let job = Job {
            _permit: permit.clone(),
            bytes: vec![0],
            max_output_bytes: 3,
            control: DecodeControl {
                cancel: cancel.clone(),
                caller: CancellationToken::new(),
                deadline: Instant::now() + std::time::Duration::from_secs(1),
            },
            result,
        };
        cancel.cancel();
        drop(receive);
        drop(permit);
        assert_eq!(permits.available_permits(), 0);
        assert_eq!(
            decode(&job.bytes, &ImageGenerationLimits::default(), &job.control),
            Err(ImageError::Cancelled)
        );
        drop(job);
        assert_eq!(permits.available_permits(), 1);
    }
    #[test]
    fn decoder_input_observes_cancellation_between_native_reads() {
        let control = DecodeControl {
            cancel: CancellationToken::new(),
            caller: CancellationToken::new(),
            deadline: Instant::now() + std::time::Duration::from_secs(1),
        };
        let mut input = Input {
            cursor: Cursor::new(&b"native bytes"[..]),
            control: &control,
        };
        assert_eq!(input.read(&mut [0; 1]).unwrap(), 1);
        control.caller.cancel();
        assert!(input.read(&mut [0; 1]).is_err());
        assert!(input.fill_buf().is_err());
        assert!(input.seek(SeekFrom::Start(0)).is_err());
    }
}
