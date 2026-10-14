//! Bounded RGB-to-JPEG work. No decoder, resize, capture or playback claim.
use crate::GeminiError;
use pipecat_core::ImageRawFrame;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{sync::Mutex, task::JoinHandle};

const MAX_PIXELS: usize = 262_144;
const MAX_RAW_BYTES: usize = MAX_PIXELS * 3;

type EncodeJob = JoinHandle<Result<Vec<u8>, GeminiError>>;

pub(crate) struct VideoEncoder {
    job: Mutex<Option<EncodeJob>>,
    cancelled: Arc<AtomicBool>,
    #[cfg(test)]
    gate: std::sync::Mutex<Option<Arc<TestGate>>>,
}
impl Default for VideoEncoder {
    fn default() -> Self {
        Self {
            job: Mutex::new(None),
            cancelled: Arc::new(AtomicBool::new(false)),
            #[cfg(test)]
            gate: Default::default(),
        }
    }
}
impl VideoEncoder {
    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }
    pub(crate) async fn reset(&self) -> Result<(), GeminiError> {
        self.finish().await?;
        self.cancelled.store(false, Ordering::SeqCst);
        Ok(())
    }
    pub(crate) async fn finish(&self) -> Result<(), GeminiError> {
        self.cancel();
        let mut slot = self.job.lock().await;
        if let Some(job) = slot.as_mut() {
            // Borrow the handle: cancellation of this waiter retains ownership.
            let joined = job.await;
            *slot = None;
            let _ = joined.map_err(|_| GeminiError::Protocol("video encoder task failed"))?;
        }
        Ok(())
    }
    pub(crate) async fn encode(
        &self,
        image: &ImageRawFrame,
        max: usize,
    ) -> Result<Vec<u8>, GeminiError> {
        validate(image)?;
        let mut slot = self.job.lock().await;
        if let Some(job) = slot.as_mut() {
            let joined = job.await;
            *slot = None;
            let _ = joined.map_err(|_| GeminiError::Protocol("video encoder task failed"))?;
        }
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(GeminiError::Closed);
        }
        let image = image.clone(); // Only after raw dimensions and bytes are bounded.
        let cancel = self.cancelled.clone();
        #[cfg(test)]
        let gate = self.gate.lock().unwrap().take();
        *slot = Some(tokio::task::spawn_blocking(move || {
            #[cfg(test)]
            if let Some(gate) = gate {
                gate.entered.store(true, Ordering::SeqCst);
                let mut released = gate.released.lock().unwrap();
                while !*released {
                    released = gate.wake.wait(released).unwrap();
                }
                gate.exited.store(true, Ordering::SeqCst);
            }
            encode_rgb(image, max, cancel)
        }));
        let result = slot.as_mut().unwrap().await;
        *slot = None;
        result.map_err(|_| GeminiError::Protocol("video encoder task failed"))?
    }
}
fn validate(image: &ImageRawFrame) -> Result<(), GeminiError> {
    let (w, h) = image.size;
    if image.format.as_deref() != Some("RGB") || w == 0 || h == 0 || w > 2048 || h > 2048 {
        return Err(GeminiError::Invalid(
            "video requires packed RGB, dimensions 1..=2048",
        ));
    }
    let required = (w as usize)
        .checked_mul(h as usize)
        .filter(|n| *n <= MAX_PIXELS)
        .and_then(|n| n.checked_mul(3))
        .ok_or(GeminiError::TooLarge)?;
    if image.image.len() > MAX_RAW_BYTES {
        return Err(GeminiError::TooLarge);
    }
    if image.image.len() < required {
        return Err(GeminiError::Invalid("short RGB image"));
    }
    Ok(())
}
struct Writer {
    bytes: Vec<u8>,
    max: usize,
    cancel: Arc<AtomicBool>,
}
impl std::io::Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self.cancel.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("video cancelled"));
        }
        let needed = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .filter(|n| *n <= self.max)
            .ok_or_else(|| std::io::Error::other("JPEG budget"))?;
        if needed > self.bytes.capacity() {
            let desired = needed
                .max(self.bytes.capacity().saturating_mul(2))
                .min(self.max);
            self.bytes
                .try_reserve_exact(desired - self.bytes.len())
                .map_err(std::io::Error::other)?;
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn encode_rgb(
    image: ImageRawFrame,
    max: usize,
    cancel: Arc<AtomicBool>,
) -> Result<Vec<u8>, GeminiError> {
    let mut output = Writer {
        bytes: Vec::new(),
        max,
        cancel,
    };
    let mut encoder = jpeg_encoder::Encoder::new(&mut output, 75);
    // Same bounded native encoder and Pillow-compatible settings as turns.
    encoder.set_sampling_factor(jpeg_encoder::SamplingFactor::F_2_2);
    encoder.set_progressive(false);
    encoder.set_optimized_huffman_tables(false);
    let required = image.size.0 as usize * image.size.1 as usize * 3;
    encoder
        .encode(
            &image.image[..required],
            image.size.0 as u16,
            image.size.1 as u16,
            jpeg_encoder::ColorType::Rgb,
        )
        .map_err(|_| {
            if output.cancel.load(Ordering::SeqCst) {
                GeminiError::Closed
            } else {
                GeminiError::TooLarge
            }
        })?;
    Ok(output.bytes)
}

#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestGate {
    entered: AtomicBool,
    exited: AtomicBool,
    released: std::sync::Mutex<bool>,
    wake: std::sync::Condvar,
}
#[cfg(test)]
impl TestGate {
    pub(crate) fn entered(&self) -> bool {
        self.entered.load(Ordering::SeqCst)
    }
    pub(crate) fn exited(&self) -> bool {
        self.exited.load(Ordering::SeqCst)
    }
    pub(crate) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}
#[cfg(test)]
impl VideoEncoder {
    pub(crate) fn test_gate(&self) -> Arc<TestGate> {
        let gate = Arc::new(TestGate::default());
        *self.gate.lock().unwrap() = Some(gate.clone());
        gate
    }
    pub(crate) async fn test_idle(&self) -> bool {
        self.job.lock().await.is_none()
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn image() -> ImageRawFrame {
        ImageRawFrame {
            image: vec![0; 12],
            size: (2, 2),
            format: Some("RGB".into()),
        }
    }
    #[tokio::test]
    async fn cancelled_encode_waiter_retains_blocking_owner_until_finish_join() {
        let encoder = Arc::new(VideoEncoder::default());
        let gate = Arc::new(TestGate::default());
        *encoder.gate.lock().unwrap() = Some(gate.clone());
        let e = encoder.clone();
        let waiter = tokio::spawn(async move { e.encode(&image(), 4096).await });
        let entered = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !gate.entered.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await;
        waiter.abort();
        let _ = waiter.await;
        encoder.cancel();
        let e = encoder.clone();
        let mut finish = tokio::spawn(async move { e.finish().await });
        let pending = tokio::time::timeout(std::time::Duration::from_millis(20), &mut finish)
            .await
            .is_err();
        *gate.released.lock().unwrap() = true;
        gate.wake.notify_all();
        let joined = finish.await.unwrap();
        assert!(entered.is_ok());
        assert!(pending);
        assert!(joined.is_ok());
        assert!(gate.exited.load(Ordering::SeqCst));
        assert!(encoder.job.lock().await.is_none());
        encoder.reset().await.unwrap();
        assert!(encoder.encode(&image(), 4096).await.is_ok());
    }
    #[test]
    fn raw_bounds_and_output_caps_are_checked() {
        let mut i = image();
        i.size = (2049, 1);
        assert!(validate(&i).is_err());
        i.size = (1024, 1024);
        assert!(matches!(validate(&i), Err(GeminiError::TooLarge)));
        i = image();
        i.format = Some("RGBA".into());
        assert!(validate(&i).is_err());
        i = image();
        i.image.resize(MAX_RAW_BYTES + 1, 0);
        assert!(matches!(validate(&i), Err(GeminiError::TooLarge)));
        assert!(encode_rgb(image(), 1, Arc::new(AtomicBool::new(false))).is_err());
        assert!(encode_rgb(image(), 4096, Arc::new(AtomicBool::new(true))).is_err());
    }
}
