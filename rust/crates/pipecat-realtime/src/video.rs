//! Bounded RGB-to-JPEG work. No decoder, resize, capture or playback claim.
use crate::{Action, ConversationItem, ItemContent, RealtimeError, Role};
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_core::ImageRawFrame;
use serde::Serialize;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{sync::Mutex, task::JoinHandle};

const MAX_PIXELS: usize = 262_144;
const MAX_RAW_BYTES: usize = MAX_PIXELS * 3;
const DATA_URI_PREFIX: &str = "data:image/jpeg;base64,";
const MAX_EVENT_ID: &str = "pc_evt_18446744073709551615";
const MAX_CAMERA_ID: &str = "pc_camera_18446744073709551615";

#[derive(Serialize)]
struct CameraEvent<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    event_id: &'a str,
    item: &'a ConversationItem,
}

fn event<'a>(action: &'a Action, event_id: &'a str) -> Result<CameraEvent<'a>, RealtimeError> {
    let Action::CreateItem {
        item:
            ConversationItem::Message {
                id,
                role: Role::User,
                content,
            },
        previous_item_id: None,
    } = action
    else {
        return Err(RealtimeError::Invalid("private camera item"));
    };
    if !id.starts_with("pc_camera_")
        || id.len() > MAX_CAMERA_ID.len()
        || !matches!(content.as_slice(), [ItemContent::InputImage { image_url, detail }]
            if image_url.starts_with(DATA_URI_PREFIX) && detail.as_deref() == Some("auto"))
    {
        return Err(RealtimeError::Invalid("private camera item"));
    }
    let Action::CreateItem { item, .. } = action else {
        unreachable!()
    };
    Ok(CameraEvent {
        kind: "conversation.item.create",
        event_id,
        item,
    })
}

/// Reserve the complete wire envelope, including maximum-width local IDs,
/// before the capped JPEG writer or base64 allocation is allowed to grow.
pub(crate) fn jpeg_budget(limit: usize) -> Result<usize, RealtimeError> {
    let action = camera_action(Vec::new(), MAX_CAMERA_ID.into())?;
    let overhead = serde_json::to_vec(&event(&action, MAX_EVENT_ID)?)
        .map_err(|_| RealtimeError::Invalid("camera serialization"))?
        .len();
    let base64 = limit.checked_sub(overhead).ok_or(RealtimeError::TooLarge)?;
    Ok(base64 / 4 * 3)
}

pub(crate) fn camera_action(jpeg: Vec<u8>, id: String) -> Result<Action, RealtimeError> {
    let encoded = jpeg
        .len()
        .checked_add(2)
        .and_then(|n| (n / 3).checked_mul(4))
        .and_then(|n| n.checked_add(DATA_URI_PREFIX.len()))
        .ok_or(RealtimeError::TooLarge)?;
    let mut image_url = String::new();
    image_url
        .try_reserve_exact(encoded)
        .map_err(|_| RealtimeError::TooLarge)?;
    image_url.push_str(DATA_URI_PREFIX);
    STANDARD.encode_string(&jpeg, &mut image_url);
    // The queue retains only the data URI, not both the JPEG and its base64 copy.
    drop(jpeg);
    Ok(Action::CreateItem {
        item: ConversationItem::Message {
            id,
            role: Role::User,
            content: vec![ItemContent::InputImage {
                image_url,
                detail: Some("auto".into()),
            }],
        },
        previous_item_id: None,
    })
}

pub(crate) fn check_camera(action: &Action, limit: usize) -> Result<(), RealtimeError> {
    crate::client::bounded_serialized(&event(action, MAX_EVENT_ID)?, limit)
}

pub(crate) fn camera_wire(
    action: &Action,
    event_id: &str,
    limit: usize,
) -> Result<String, RealtimeError> {
    let event = event(action, event_id)?;
    crate::client::bounded_serialized(&event, limit)?;
    let wire = serde_json::to_string(&event)
        .map_err(|_| RealtimeError::Invalid("camera serialization"))?;
    if wire.len() > limit {
        return Err(RealtimeError::TooLarge);
    }
    Ok(wire)
}

type EncodeJob = JoinHandle<Result<Vec<u8>, RealtimeError>>;

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
    pub(crate) async fn reset(&self) -> Result<(), RealtimeError> {
        self.finish().await?;
        self.cancelled.store(false, Ordering::SeqCst);
        Ok(())
    }
    pub(crate) async fn finish(&self) -> Result<(), RealtimeError> {
        self.cancel();
        let mut slot = self.job.lock().await;
        if let Some(job) = slot.as_mut() {
            // Borrow the handle: cancellation of this waiter retains ownership.
            let joined = job.await;
            *slot = None;
            let _ = joined.map_err(|_| RealtimeError::Protocol("video encoder task failed"))?;
        }
        Ok(())
    }
    pub(crate) async fn encode(
        &self,
        image: &ImageRawFrame,
        max: usize,
    ) -> Result<Vec<u8>, RealtimeError> {
        validate(image)?;
        let mut slot = self.job.lock().await;
        if let Some(job) = slot.as_mut() {
            let joined = job.await;
            *slot = None;
            let _ = joined.map_err(|_| RealtimeError::Protocol("video encoder task failed"))?;
        }
        if self.cancelled.load(Ordering::SeqCst) {
            return Err(RealtimeError::Closed);
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
        result.map_err(|_| RealtimeError::Protocol("video encoder task failed"))?
    }
}
fn validate(image: &ImageRawFrame) -> Result<(), RealtimeError> {
    let (w, h) = image.size;
    if image.format.as_deref() != Some("RGB") || w == 0 || h == 0 || w > 2048 || h > 2048 {
        return Err(RealtimeError::Invalid(
            "video requires packed RGB, dimensions 1..=2048",
        ));
    }
    let required = (w as usize)
        .checked_mul(h as usize)
        .filter(|n| *n <= MAX_PIXELS)
        .and_then(|n| n.checked_mul(3))
        .ok_or(RealtimeError::TooLarge)?;
    if image.image.len() > MAX_RAW_BYTES {
        return Err(RealtimeError::TooLarge);
    }
    if image.image.len() < required {
        return Err(RealtimeError::Invalid("short RGB image"));
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
) -> Result<Vec<u8>, RealtimeError> {
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
                RealtimeError::Closed
            } else {
                RealtimeError::TooLarge
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
    #[tokio::test]
    async fn failed_encode_releases_owned_slot_before_next_sample() {
        let encoder = VideoEncoder::default();
        assert_eq!(
            encoder.encode(&image(), 1).await,
            Err(RealtimeError::TooLarge)
        );
        assert!(encoder.test_idle().await);
        assert!(encoder.encode(&image(), 4096).await.is_ok());
        encoder.finish().await.unwrap();
        assert!(encoder.test_idle().await);
    }
    #[test]
    fn raw_bounds_and_output_caps_are_checked() {
        let mut i = image();
        i.size = (2049, 1);
        assert!(validate(&i).is_err());
        i.size = (1024, 1024);
        assert!(matches!(validate(&i), Err(RealtimeError::TooLarge)));
        i = image();
        i.format = Some("RGBA".into());
        assert!(validate(&i).is_err());
        i = image();
        i.image.resize(MAX_RAW_BYTES + 1, 0);
        assert!(matches!(validate(&i), Err(RealtimeError::TooLarge)));
        assert!(encode_rgb(image(), 1, Arc::new(AtomicBool::new(false))).is_err());
        assert!(encode_rgb(image(), 4096, Arc::new(AtomicBool::new(true))).is_err());
    }
    #[test]
    fn complete_camera_envelope_and_base64_padding_fit_reserved_budget() {
        for limit in [256, 1024, 4096, 1 << 20] {
            let budget = jpeg_budget(limit).unwrap();
            let action = camera_action(vec![255; budget], MAX_CAMERA_ID.into()).unwrap();
            check_camera(&action, limit).unwrap();
            let wire = camera_wire(&action, MAX_EVENT_ID, limit).unwrap();
            assert!(wire.len() <= limit);
            assert!(camera_wire(&action, MAX_EVENT_ID, wire.len() - 1).is_err());
            let oversized = camera_action(vec![255; budget + 1], MAX_CAMERA_ID.into()).unwrap();
            assert!(check_camera(&oversized, limit).is_err());
            let parsed: serde_json::Value = serde_json::from_str(&wire).unwrap();
            assert_eq!(parsed["type"], "conversation.item.create");
            assert_eq!(parsed["item"]["content"][0]["detail"], "auto");
        }
        assert!(jpeg_budget(1).is_err());
    }
}
