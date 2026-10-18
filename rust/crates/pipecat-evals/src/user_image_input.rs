//! Opt-in, session-local, plain requested-image input. Registration is passive.
//!
//! The latest encoded allocation, request snapshots and decode inputs have
//! independent ownership. A request slot follows the native job and then the
//! constrained System frame until the last retained carrier is dropped.
use crate::bot_host::BotHostError;
use pipecat_core::*;
use pipecat_images::{OfflineRgbDecodeLimits, OfflineRgbDecoder};
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const MAX_ENCODED: usize = 64 * 1024;
const MAX_PIXELS: usize = 262_144;
const MAX_HEADER: usize = 16 * 1024;
const SLOT_BYTES: usize = 1024 * 1024;
const IDENTIFIER_BYTES: usize = 256;
const TEXT_BYTES: usize = 8 * 1024;

/// Independent opt-in image limits. They do not increase any WebSocket,
/// context, or cumulative application-boundary limit. The selected first route
/// accepts lower limits only; changing a destination does not add a camera.
#[derive(Debug, Clone)]
pub struct ImageInputLimits {
    pub request_destination: Option<String>,
    pub max_encoded_image_bytes: usize,
    pub max_pixels: usize,
    pub max_dimension: u32,
    pub max_decode_bytes: usize,
    /// Across decoding, ready, queued, and forwarded frames, until final drop.
    pub max_pending_requests: usize,
    /// Aggregate retained request/header copies, not only the request payload.
    pub max_request_bytes: usize,
    pub max_request_ids: usize,
    pub max_registrations: usize,
    /// Conservatively charged at admission, including cancelled/failed work.
    pub max_decoded_bytes: usize,
    pub max_native_frame_bytes: usize,
    pub decode_timeout: Duration,
    pub shutdown_timeout: Duration,
}
impl Default for ImageInputLimits {
    fn default() -> Self {
        Self {
            request_destination: None,
            max_encoded_image_bytes: MAX_ENCODED,
            max_pixels: MAX_PIXELS,
            max_dimension: 1024,
            max_decode_bytes: 32 * 1024 * 1024,
            max_pending_requests: 2,
            max_request_bytes: MAX_HEADER,
            max_request_ids: 4096,
            max_registrations: 64,
            max_decoded_bytes: 32 * 1024 * 1024,
            max_native_frame_bytes: SLOT_BYTES,
            decode_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}
impl ImageInputLimits {
    pub(crate) fn validate(&self) -> Result<(), BotHostError> {
        let bounded = [
            (self.max_encoded_image_bytes, MAX_ENCODED),
            (self.max_pixels, MAX_PIXELS),
            (self.max_decode_bytes, 32 * 1024 * 1024),
            (self.max_pending_requests, 2),
            (self.max_request_bytes, MAX_HEADER),
            (self.max_request_ids, 4096),
            (self.max_registrations, 64),
            (self.max_decoded_bytes, 32 * 1024 * 1024),
            (self.max_native_frame_bytes, SLOT_BYTES),
        ];
        if bounded
            .iter()
            .any(|(value, max)| *value == 0 || value > max)
            || self.max_dimension == 0
            || self.max_dimension > 1024
            || [self.decode_timeout, self.shutdown_timeout]
                .iter()
                .any(|time| time.is_zero() || *time > Duration::from_secs(5))
            || self
                .request_destination
                .as_deref()
                .is_some_and(|value| !valid_id(value))
        {
            return Err(BotHostError::Invalid(
                "invalid bounded image input limits".into(),
            ));
        }
        let retained = self
            .max_encoded_image_bytes
            .checked_mul(2)
            .and_then(|bytes| self.max_pixels.checked_mul(3)?.checked_add(bytes))
            .and_then(|bytes| bytes.checked_add(self.max_request_bytes))
            .ok_or(BotHostError::Limit("image retained storage"))?;
        if retained > SLOT_BYTES {
            return Err(BotHostError::Limit("image retained storage"));
        }
        self.max_pending_requests
            .checked_mul(SLOT_BYTES)
            .ok_or(BotHostError::Limit("image retained slots"))?;
        Ok(())
    }
    fn decoder_limits(&self) -> OfflineRgbDecodeLimits {
        OfflineRgbDecodeLimits {
            max_encoded_image_bytes: self.max_encoded_image_bytes,
            max_pixels: self.max_pixels,
            max_dimension: self.max_dimension,
            max_decode_bytes: self.max_decode_bytes,
            max_jobs: self.max_pending_requests,
            decode_timeout: self.decode_timeout,
            shutdown_timeout: self.shutdown_timeout,
        }
    }
}

struct Asset {
    bytes: Box<[u8]>,
    revision: u64,
    rgb_bytes: usize,
}
struct State {
    accepting: bool,
    latest: Option<Arc<Asset>>,
    registrations: usize,
    // Compact count-bounded tombstones: at most 32 KiB of u64 storage. Never
    // evict a request ID to allow it to consume a newer registration.
    seen: Vec<u64>,
    decoded_bytes: usize,
}
struct RequestLease {
    asset: Arc<Asset>,
    _slot: OwnedSemaphorePermit,
}
struct Admission {
    lease: Arc<RequestLease>,
    fence: InvocationFence,
}

pub(crate) struct UserImageInput {
    limits: ImageInputLimits,
    decoder: Arc<OfflineRgbDecoder>,
    source: Arc<NativeUserImageSource>,
    slots: Arc<Semaphore>,
    state: Mutex<State>,
}
impl UserImageInput {
    pub(crate) fn new(limits: ImageInputLimits) -> Result<Self, BotHostError> {
        limits.validate()?;
        let decoder = OfflineRgbDecoder::new(limits.decoder_limits()).map_err(image_error)?;
        Ok(Self {
            slots: Arc::new(Semaphore::new(limits.max_pending_requests)),
            state: Mutex::new(State {
                accepting: true,
                latest: None,
                registrations: 0,
                seen: Vec::with_capacity(limits.max_request_ids),
                decoded_bytes: 0,
            }),
            limits,
            decoder: Arc::new(decoder),
            source: Arc::new(NativeUserImageSource::new()),
        })
    }
    /// The serializer's base64 decoder can reserve up to two padding bytes.
    /// Validate its bounded logical length, then compact this internally owned
    /// wire temporary before it becomes retained image storage. Public asset
    /// constructors and direct native registration still count spare capacity.
    pub(crate) fn register_wire(&self, bytes: Vec<u8>, mime: &str) -> Result<(), BotHostError> {
        if bytes.len() > self.limits.max_encoded_image_bytes {
            return Err(BotHostError::Limit("encoded image bytes"));
        }
        self.register(bytes.into_boxed_slice().into_vec(), mime)
    }
    /// The single socket reader calls this synchronously before accepting its
    /// next packet. Failed validation cannot partially replace the old asset.
    pub(crate) fn register(&self, bytes: Vec<u8>, mime: &str) -> Result<(), BotHostError> {
        if bytes.capacity() > self.limits.max_encoded_image_bytes {
            return Err(BotHostError::Limit("encoded image capacity"));
        }
        let size = self
            .limits
            .decoder_limits()
            .inspect_encoded_image(&bytes, mime)
            .map_err(image_error)?;
        let rgb_bytes = rgb_size(size).map_err(image_error)?;
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return Err(BotHostError::Invalid("eval image input is closed".into()));
        }
        if state.registrations == self.limits.max_registrations {
            return Err(BotHostError::Limit("image registrations"));
        }
        let revision = u64::try_from(state.registrations + 1)
            .map_err(|_| BotHostError::Limit("image revision"))?;
        let asset = Arc::new(Asset {
            bytes: bytes.into_boxed_slice(),
            revision,
            rgb_bytes,
        });
        state.latest = Some(asset);
        state.registrations += 1;
        Ok(())
    }
    pub(crate) fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.accepting = false;
        state.latest = None;
        self.slots.close();
        self.source.close();
        self.decoder.close();
    }
    /// A diagnostic timeout never permits abandoning the native join. A
    /// cancelled caller can invoke this again and continue the same join.
    pub(crate) async fn shutdown_joined(&self) -> Result<(), BotHostError> {
        self.close();
        let mut timed_out = false;
        loop {
            match self.decoder.shutdown().await {
                Ok(()) if timed_out => {
                    return Err(BotHostError::Timeout("image decoder cleanup (joined)"))
                }
                Ok(()) => return Ok(()),
                Err(pipecat_images::ImageError::Timeout) => timed_out = true,
                Err(error) => return Err(image_error(error)),
            }
        }
    }
    /// Validate before every dispatch, including frames whose public variant
    /// was changed. The core consumer performs the authoritative atomic commit.
    pub(crate) fn validate_frame(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<bool, ProcessorError> {
        if direction == Direction::Upstream
            && matches!(frame.data, FrameData::UserImageRequest(_))
            && frame.transport_destination == self.limits.request_destination
        {
            validate_request(frame, &self.limits)?;
        }
        if let Some(constraint) = frame.native_user_image_constraint(direction)? {
            if !self.source.matches(frame)? {
                return Err(fatal("foreign eval image source"));
            }
            return Ok(constraint.is_current());
        }
        if let Some(context) = frame.native_user_image_context(direction)? {
            if !self.source.matches(frame)? {
                return Err(fatal("foreign eval image context source"));
            }
            return Ok(context.is_current());
        }
        Ok(true)
    }

    pub(crate) fn media_bytes(&self, frame: &Frame) -> Result<Option<usize>, ProcessorError> {
        if !self.source.matches(frame)? {
            return Ok(None);
        }
        let FrameData::UserImage(payload) = &frame.data else {
            // A derived ordinary context keeps its own normal JSON budget.
            return Ok(None);
        };
        if payload.image.format.as_deref() != Some("RGB") {
            return Err(fatal("eval images require RGB"));
        }
        let expected = rgb_size(payload.image.size)?;
        if payload.image.size.0 > self.limits.max_dimension
            || payload.image.size.1 > self.limits.max_dimension
            || expected
                > self
                    .limits
                    .max_pixels
                    .checked_mul(3)
                    .ok_or_else(|| fatal("RGB size overflow"))?
            || payload.image.image.len() != expected
            || payload.image.image.capacity() > expected
        {
            return Err(fatal("eval RGB dimensions, length or capacity exceeded"));
        }
        let header = image_header_bytes(frame, payload, self.limits.max_request_bytes)?;
        let bytes = expected
            .checked_add(header)
            .ok_or_else(|| fatal("image byte overflow"))?;
        if bytes > self.limits.max_native_frame_bytes {
            return Err(fatal("eval native image byte limit exceeded"));
        }
        Ok(Some(bytes))
    }
    fn admit(
        &self,
        frame: &Frame,
        ctx: &ProcessorContext,
    ) -> Result<Option<Admission>, ProcessorError> {
        validate_request(frame, &self.limits)?;
        let fence = ctx.invocation_fence()?;
        // Runtime admission precedes local state in this synchronous gate.
        // Neither registration nor close acquires runtime admission while
        // holding local state, so there is no reverse lock order.
        fence
            .with_current(|| {
                let mut state = self.state.lock().unwrap();
                if !state.accepting {
                    return Err(fatal("eval image input is closed"));
                }
                if state.seen.contains(&frame.id) {
                    return Err(fatal("duplicate image request identity"));
                }
                if state.seen.len() == self.limits.max_request_ids {
                    return Err(fatal("image request identities exhausted"));
                }
                let asset = state
                    .latest
                    .clone()
                    .ok_or_else(|| fatal("no eval image is registered"))?;
                let decoded_bytes = state
                    .decoded_bytes
                    .checked_add(asset.rgb_bytes)
                    .filter(|bytes| *bytes <= self.limits.max_decoded_bytes)
                    .ok_or_else(|| fatal("decoded image lifetime budget exceeded"))?;
                let slot = self
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| fatal("retained image request slots exhausted"))?;
                state.seen.push(frame.id);
                state.decoded_bytes = decoded_bytes;
                Ok(Admission {
                    lease: Arc::new(RequestLease { _slot: slot, asset }),
                    fence: fence.clone(),
                })
            })
            .transpose()
    }
    fn request(&self, request: Frame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let Some(admission) = self.admit(&request, ctx)? else {
            return Ok(());
        };
        let source = self.source.clone();
        let retained: Arc<dyn Send + Sync> = admission.lease.clone();
        // Exactly one additional encoded allocation, bounded and reserved
        // before creation. Worker ownership outlives an abandoned async job.
        let input = admission.lease.asset.bytes.to_vec();
        let job = match self.decoder.submit_retained(input, retained) {
            Ok(job) => job,
            Err(_) if !admission.fence.is_current() || self.decoder.status().closed => {
                return Ok(())
            }
            Err(error) => return Err(image_processor_error(error)),
        };
        let decoder = self.decoder.clone();
        let worker_ctx = ctx.clone();
        let max_frame_bytes = self.limits.max_native_frame_bytes;
        let max_header = self.limits.max_request_bytes;
        let task_name = format!("eval-user-image-decode-r{}", admission.lease.asset.revision);
        ctx.spawn(task_name, true, async move {
            let result = job.await;
            if !admission.fence.is_current() || decoder.status().closed {
                return Ok(());
            }
            let image = result.map_err(image_processor_error)?;
            let FrameData::UserImageRequest(payload) = &request.data else {
                unreachable!()
            };
            if image.image.len() != admission.lease.asset.rgb_bytes
                || image.image.capacity() > image.image.len()
            {
                return Err(fatal("decoder returned unreserved RGB storage"));
            }
            let mut response = Frame::new(FrameData::UserImage(UserImageRawFrame {
                image,
                user_id: payload.user_id.clone(),
                text: payload.text.clone(),
                append_to_context: payload.append_to_context,
                request: Some(Box::new(payload.clone())),
                run_llm: None,
            }));
            response.metadata = request.metadata.clone();
            response.transport_source = request.transport_source.clone();
            response.transport_destination = request.transport_destination.clone();
            let FrameData::UserImage(payload) = &response.data else {
                unreachable!()
            };
            let bytes = image_header_bytes(&response, payload, max_header)?
                .checked_add(payload.image.image.capacity())
                .ok_or_else(|| fatal("image byte overflow"))?;
            if bytes > max_frame_bytes {
                return Err(fatal("eval native image byte limit exceeded"));
            }
            let retained: Arc<dyn Send + Sync> = admission.lease.clone();
            if let Err(error) =
                source.bind(&mut response, &request, admission.fence.clone(), retained)
            {
                drop(response);
                return Err(error);
            }
            worker_ctx.try_push_with_invocation_fence(
                response,
                Direction::Downstream,
                &admission.fence,
            )
        })?;
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for UserImageInput {
    fn name(&self) -> &str {
        "eval-user-image-input"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !self.validate_frame(&frame, direction)? {
            return Ok(());
        }
        if matches!(frame.data, FrameData::UserImageRequest(_))
            && direction == Direction::Upstream
            && frame.transport_destination == self.limits.request_destination
        {
            return self.request(frame, ctx);
        }
        if frame.is_terminal() {
            self.close();
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.close();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.close();
        Ok(())
    }
}

fn valid_id(value: &str) -> bool {
    !value.is_empty() && value.len() <= IDENTIFIER_BYTES && !value.chars().any(char::is_control)
}
fn rgb_size(size: (u32, u32)) -> Result<usize, ProcessorError> {
    if size.0 == 0 || size.1 == 0 {
        return Err(fatal("empty RGB dimensions"));
    }
    usize::try_from(size.0)
        .ok()
        .and_then(|width| usize::try_from(size.1).ok()?.checked_mul(width))
        .and_then(|pixels| pixels.checked_mul(3))
        .ok_or_else(|| fatal("RGB size overflow"))
}
fn validate_request(frame: &Frame, limits: &ImageInputLimits) -> Result<(), ProcessorError> {
    let FrameData::UserImageRequest(payload) = &frame.data else {
        return Err(fatal("expected image request"));
    };
    // Reject authority before cloning, serializing, taking a slot or decoding.
    if payload.function_name.is_some()
        || payload.tool_call_id.is_some()
        || payload.result_callback.is_some()
    {
        return Err(fatal("eval image input supports plain requests only"));
    }
    if !valid_id(&payload.user_id)
        || payload
            .video_source
            .as_deref()
            .is_some_and(|source| source != "camera")
        || payload
            .text
            .as_ref()
            .is_some_and(|text| text.len() > TEXT_BYTES || text.capacity() > TEXT_BYTES)
        || [&frame.transport_source, &frame.transport_destination]
            .into_iter()
            .flatten()
            .any(|id| !valid_id(id))
    {
        return Err(fatal(
            "invalid eval image request identifiers, source or text",
        ));
    }
    if frame.transport_destination != limits.request_destination {
        return Err(fatal("foreign image request destination"));
    }
    // During construction, the original request plus the response's plain
    // request and duplicated user/text fields coexist. Reserve all copies.
    let retained_header = frame_header_storage(frame)?;
    let payload_storage = request_storage(payload)?;
    let payload_bytes = measured(payload, limits.max_request_bytes)?.max(payload_storage);
    let header = measured(
        &(
            &frame.name,
            frame.pts,
            &frame.metadata,
            &frame.transport_source,
            &frame.transport_destination,
            frame.broadcast_sibling_id,
            frame.interruptible,
        ),
        limits.max_request_bytes,
    )?
    .max(retained_header);
    let retained = payload_bytes
        .checked_mul(3)
        .and_then(|bytes| header.checked_mul(2)?.checked_add(bytes))
        .and_then(|bytes| bytes.checked_add(512))
        .ok_or_else(|| fatal("request header overflow"))?;
    if retained > limits.max_request_bytes {
        return Err(fatal("eval image request retained header budget exceeded"));
    }
    Ok(())
}
fn image_header_bytes(
    frame: &Frame,
    payload: &UserImageRawFrame,
    max: usize,
) -> Result<usize, ProcessorError> {
    let mut owned = frame_header_storage(frame)?;
    for bytes in [
        payload.user_id.capacity(),
        payload.text.as_ref().map_or(0, String::capacity),
        payload.image.format.as_ref().map_or(0, String::capacity),
        payload
            .request
            .as_deref()
            .map(request_storage)
            .transpose()?
            .unwrap_or(0),
        512,
    ] {
        owned = owned
            .checked_add(bytes)
            .ok_or_else(|| fatal("image header storage overflow"))?;
    }
    if owned > max {
        return Err(fatal("image header retained storage exceeded"));
    }
    let encoded = measured(
        &(
            &frame.id,
            &frame.name,
            frame.pts,
            &frame.metadata,
            &frame.transport_source,
            &frame.transport_destination,
            frame.broadcast_sibling_id,
            frame.interruptible,
            payload.image.size,
            &payload.image.format,
            &payload.user_id,
            &payload.text,
            payload.append_to_context,
            &payload.request,
            payload.run_llm,
        ),
        max,
    )?;
    Ok(encoded.max(owned))
}
fn request_storage(payload: &UserImageRequestFrame) -> Result<usize, ProcessorError> {
    [
        Some(&payload.user_id),
        payload.text.as_ref(),
        payload.video_source.as_ref(),
    ]
    .into_iter()
    .try_fold(256usize, |bytes, value| {
        bytes
            .checked_add(value.map_or(0, |s| s.capacity()))
            .ok_or_else(|| fatal("request storage overflow"))
    })
}
fn frame_header_storage(frame: &Frame) -> Result<usize, ProcessorError> {
    let mut bytes = frame
        .name
        .capacity()
        .checked_add(256)
        .ok_or_else(|| fatal("image header storage overflow"))?;
    for value in [&frame.transport_source, &frame.transport_destination]
        .into_iter()
        .flatten()
    {
        bytes = bytes
            .checked_add(value.capacity())
            .ok_or_else(|| fatal("image header storage overflow"))?;
    }
    let mut nodes = 0usize;
    for (key, value) in &frame.metadata {
        bytes = bytes
            .checked_add(128)
            .and_then(|n| n.checked_add(key.capacity()))
            .and_then(|n| value_storage(value, 0, &mut nodes).ok()?.checked_add(n))
            .filter(|n| *n <= MAX_HEADER)
            .ok_or_else(|| fatal("image metadata retained storage exceeded"))?;
    }
    Ok(bytes)
}
fn value_storage(
    value: &serde_json::Value,
    depth: usize,
    nodes: &mut usize,
) -> Result<usize, ProcessorError> {
    *nodes += 1;
    if depth > 32 || *nodes > 4096 {
        return Err(fatal("image metadata depth/count exceeded"));
    }
    let bytes = match value {
        serde_json::Value::String(text) => text.capacity(),
        serde_json::Value::Array(values) => {
            let base = values
                .capacity()
                .checked_mul(std::mem::size_of::<serde_json::Value>())
                .filter(|n| *n <= MAX_HEADER)
                .ok_or_else(|| fatal("image metadata array storage exceeded"))?;
            values.iter().try_fold(base, |sum, value| {
                value_storage(value, depth + 1, nodes)?
                    .checked_add(sum)
                    .filter(|n| *n <= MAX_HEADER)
                    .ok_or_else(|| fatal("image metadata storage exceeded"))
            })?
        }
        serde_json::Value::Object(values) => {
            values.iter().try_fold(0usize, |sum, (key, value)| {
                value_storage(value, depth + 1, nodes)?
                    .checked_add(sum)
                    .and_then(|n| n.checked_add(key.capacity()))
                    .and_then(|n| n.checked_add(128))
                    .filter(|n| *n <= MAX_HEADER)
                    .ok_or_else(|| fatal("image metadata storage exceeded"))
            })?
        }
        _ => 0,
    };
    if bytes > MAX_HEADER {
        return Err(fatal("image metadata retained storage exceeded"));
    }
    Ok(bytes)
}
fn measured(value: &impl Serialize, max: usize) -> Result<usize, ProcessorError> {
    struct Counter {
        count: usize,
        max: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.count = self
                .count
                .checked_add(bytes.len())
                .filter(|count| *count <= self.max)
                .ok_or_else(|| std::io::Error::other("image header byte limit exceeded"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { count: 0, max };
    serde_json::to_writer(&mut counter, value).map_err(image_processor_error)?;
    Ok(counter.count)
}
fn fatal(message: &'static str) -> ProcessorError {
    ProcessorError::fatal(message)
}
fn image_error(error: impl std::fmt::Display) -> BotHostError {
    BotHostError::Invalid(format!("eval image input: {error}"))
}
fn image_processor_error(error: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::fatal(format!("eval image input: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    fn png(color: [u8; 3]) -> Vec<u8> {
        let mut bytes = Vec::new();
        {
            let mut encoder = png::Encoder::new(&mut bytes, 1, 1);
            encoder.set_color(png::ColorType::Rgb);
            encoder.set_depth(png::BitDepth::Eight);
            encoder
                .write_header()
                .unwrap()
                .write_image_data(&color)
                .unwrap();
        }
        bytes.into_boxed_slice().into_vec()
    }
    fn request() -> Frame {
        Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
            user_id: "user-1".into(),
            text: Some("describe this".into()),
            append_to_context: Some(true),
            video_source: None,
            function_name: None,
            tool_call_id: None,
            result_callback: None,
        }))
    }
    struct Probe {
        input: Arc<UserImageInput>,
        results: mpsc::UnboundedSender<Result<Admission, ProcessorError>>,
    }
    #[async_trait]
    impl FrameProcessor for Probe {
        fn name(&self) -> &str {
            "image-admission-probe"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::UserImageRequest(_)) {
                self.results
                    .send(self.input.admit(&frame, ctx).and_then(|admission| {
                        admission.ok_or_else(|| fatal("test admission was stale"))
                    }))
                    .map_err(|_| fatal("test probe closed"))?;
                Ok(())
            } else {
                ctx.push(frame, direction).await
            }
        }
    }
    async fn admitted(
        worker: &PipelineWorker,
        results: &mut mpsc::UnboundedReceiver<Result<Admission, ProcessorError>>,
        frame: Frame,
    ) -> Result<Admission, ProcessorError> {
        worker
            .sender()
            .send(frame, Direction::Upstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), results.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn stop(mut worker: PipelineWorker, input: &UserImageInput) {
        let _ = worker.cancel("test complete").await;
        while worker.recv().await.is_some() {}
        let _ = worker.join().await;
        input.shutdown_joined().await.unwrap();
        assert!(input.decoder.status().joined);
    }
    async fn next_image(worker: &mut PipelineWorker) -> Frame {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let output = worker.recv().await.expect("pipeline closed before image");
                if matches!(output.frame.data, FrameData::UserImage(_)) {
                    return output.frame;
                }
                assert!(
                    output.frame.data.as_error_frame().is_none(),
                    "unexpected pipeline error"
                );
            }
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn registration_is_passive_atomic_and_session_local() {
        let first = UserImageInput::new(ImageInputLimits::default()).unwrap();
        first.register(png([1, 2, 3]), "image/png").unwrap();
        let snapshot = first.state.lock().unwrap().latest.clone().unwrap();
        assert_eq!(snapshot.revision, 1);
        assert!(first.register(png([9, 8, 7]), "image/jpeg").is_err());
        assert!(first.register(Vec::new(), "image/png").is_err());
        assert!(Arc::ptr_eq(
            &snapshot,
            first.state.lock().unwrap().latest.as_ref().unwrap()
        ));
        assert_eq!(first.decoder.status().outstanding_jobs, 0);
        assert_eq!(first.slots.available_permits(), 2);
        let second = UserImageInput::new(ImageInputLimits::default()).unwrap();
        assert!(second.state.lock().unwrap().latest.is_none());
        first.shutdown_joined().await.unwrap();
        second.shutdown_joined().await.unwrap();
        assert!(first.state.lock().unwrap().latest.is_none());
        assert!(first.register(png([0, 0, 0]), "image/png").is_err());
    }

    #[tokio::test]
    async fn accepted_snapshot_survives_replacement_and_ids_never_replay() {
        let input = Arc::new(UserImageInput::new(ImageInputLimits::default()).unwrap());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let worker = Pipeline::new(vec![Arc::new(Probe {
            input: input.clone(),
            results: tx,
        })])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let missing = admitted(&worker, &mut rx, request()).await;
        assert!(missing.err().unwrap().message.contains("no eval image"));
        assert!(input.state.lock().unwrap().seen.is_empty());
        input.register(png([10, 20, 30]), "image/png").unwrap();
        let original = request();
        let first = admitted(&worker, &mut rx, original.clone()).await.unwrap();
        let mut changed = original.clone();
        let FrameData::UserImageRequest(payload) = &mut changed.data else {
            unreachable!()
        };
        payload.text = Some("different payload, same identity".into());
        assert!(admitted(&worker, &mut rx, changed)
            .await
            .err()
            .unwrap()
            .message
            .contains("duplicate"));
        // Hold the actual admitted snapshot before submitting decode. This
        // deterministic stage exercises the same admission used by the owner.
        input.register(png([40, 50, 60]), "image/png").unwrap();
        let second = admitted(&worker, &mut rx, request()).await.unwrap();
        assert_eq!(first.lease.asset.revision, 1);
        assert_eq!(second.lease.asset.revision, 2);
        assert!(admitted(&worker, &mut rx, request())
            .await
            .err()
            .unwrap()
            .message
            .contains("slots"));
        let first_image = input
            .decoder
            .submit_retained(first.lease.asset.bytes.to_vec(), first.lease.clone())
            .unwrap()
            .await
            .unwrap();
        let second_image = input
            .decoder
            .submit_retained(second.lease.asset.bytes.to_vec(), second.lease.clone())
            .unwrap()
            .await
            .unwrap();
        assert_eq!(first_image.image, [10, 20, 30]);
        assert_eq!(second_image.image, [40, 50, 60]);
        drop(first);
        drop(second);
        assert!(admitted(&worker, &mut rx, original)
            .await
            .err()
            .unwrap()
            .message
            .contains("duplicate"));
        let fresh = admitted(&worker, &mut rx, request()).await.unwrap();
        assert_eq!(fresh.lease.asset.revision, 2);
        worker
            .sender()
            .send(Frame::new(FrameData::Interruption), Direction::Downstream)
            .await
            .unwrap();
        // Sending interruption retires the source queue generation atomically.
        assert!(!fresh.fence.is_current());
        drop(fresh);
        stop(worker, &input).await;
    }

    #[tokio::test]
    async fn actual_forwarded_frames_hold_slots_through_last_carrier_drop() {
        let input = Arc::new(UserImageInput::new(ImageInputLimits::default()).unwrap());
        input.register(png([11, 22, 33]), "image/png").unwrap();
        let mut worker = Pipeline::new(vec![input.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(request(), Direction::Upstream)
            .await
            .unwrap();
        let first = next_image(&mut worker).await;
        worker
            .sender()
            .send(request(), Direction::Upstream)
            .await
            .unwrap();
        let second = next_image(&mut worker).await;
        assert_eq!(input.slots.available_permits(), 0);
        assert!(input.slots.clone().try_acquire_owned().is_err());
        assert!(first
            .native_user_image_constraint(Direction::Downstream)
            .unwrap()
            .is_some());
        // Native duplicate preserves the same consume-once/lease identity.
        let duplicate = first.duplicate();
        drop(first);
        assert_eq!(input.slots.available_permits(), 0);
        drop(duplicate);
        assert_eq!(input.slots.available_permits(), 1);
        worker
            .sender()
            .send(request(), Direction::Upstream)
            .await
            .unwrap();
        let third = next_image(&mut worker).await;
        assert_eq!(input.slots.available_permits(), 0);
        drop(second);
        drop(third);
        stop(worker, &input).await;
        assert_eq!(input.slots.available_permits(), 2);
    }

    #[test]
    fn authority_and_retained_capacity_reject_before_ownership() {
        let called = Arc::new(AtomicUsize::new(0));
        let mut frame = request();
        let FrameData::UserImageRequest(payload) = &mut frame.data else {
            unreachable!()
        };
        let count = called.clone();
        payload.result_callback = Some(LocalResultCallback::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        }));
        let before = frame.clone();
        assert!(validate_request(&frame, &ImageInputLimits::default())
            .unwrap_err()
            .message
            .contains("plain"));
        assert_eq!(frame.data, before.data);
        assert_eq!(called.load(Ordering::SeqCst), 0);
        for field in ["function", "tool"] {
            let mut frame = request();
            let FrameData::UserImageRequest(payload) = &mut frame.data else {
                unreachable!()
            };
            if field == "function" {
                payload.function_name = Some("look".into());
            } else {
                payload.tool_call_id = Some("call-1".into());
            }
            let before = frame.clone();
            assert!(validate_request(&frame, &ImageInputLimits::default())
                .unwrap_err()
                .message
                .contains("plain"));
            assert_eq!(frame.data, before.data);
        }
        let mut frame = request();
        let mut storage = String::with_capacity(64 * 1024);
        storage.push('a');
        frame
            .metadata
            .insert("large spare capacity".into(), storage.into());
        assert!(validate_request(&frame, &ImageInputLimits::default()).is_err());
        let limits = ImageInputLimits {
            max_pending_requests: 3,
            ..Default::default()
        };
        assert!(limits.validate().is_err());
    }

    #[tokio::test]
    async fn registration_request_and_lifetime_work_exhaustion_are_finite() {
        let limits = ImageInputLimits {
            max_registrations: 1,
            max_request_ids: 1,
            max_decoded_bytes: 3,
            ..Default::default()
        };
        let input = Arc::new(UserImageInput::new(limits).unwrap());
        input.register(png([1, 1, 1]), "image/png").unwrap();
        assert!(input.register(png([2, 2, 2]), "image/png").is_err());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let worker = Pipeline::new(vec![Arc::new(Probe {
            input: input.clone(),
            results: tx,
        })])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let held = admitted(&worker, &mut rx, request()).await.unwrap();
        drop(held);
        assert!(admitted(&worker, &mut rx, request())
            .await
            .err()
            .unwrap()
            .message
            .contains("identities"));
        assert_eq!(input.state.lock().unwrap().decoded_bytes, 3);
        stop(worker, &input).await;
    }
    #[tokio::test]
    async fn exact_encoded_cap_wire_base64_padding_is_compacted_before_registration() {
        use base64::{engine::general_purpose::STANDARD, Engine};
        fn crc32(bytes: &[u8]) -> u32 {
            let mut crc = !0u32;
            for byte in bytes {
                crc ^= u32::from(*byte);
                for _ in 0..8 {
                    crc = (crc >> 1) ^ (0xedb8_8320 & 0u32.wrapping_sub(crc & 1));
                }
            }
            !crc
        }
        let small = png([17, 34, 51]);
        let padding = MAX_ENCODED - small.len() - 12;
        let mut image = Vec::with_capacity(MAX_ENCODED);
        image.extend_from_slice(&small[..small.len() - 12]);
        image.extend_from_slice(&u32::try_from(padding).unwrap().to_be_bytes());
        let chunk_start = image.len();
        image.extend_from_slice(b"tEXt");
        image.extend_from_slice(b"k\0");
        image.resize(image.len() + padding - 2, b'x');
        image.extend_from_slice(&crc32(&image[chunk_start..]).to_be_bytes());
        image.extend_from_slice(&small[small.len() - 12..]);
        assert_eq!(image.len(), MAX_ENCODED);
        let envelope = crate::serializer::control(
            1,
            crate::serializer::EVAL_IMAGE,
            serde_json::json!({"image": STANDARD.encode(&image), "format":"image/png"}),
        );
        let wire = envelope.encode().unwrap();
        let crate::serializer::EvalControl::Image { bytes, format } =
            crate::serializer::decode_control(wire.as_bytes(), 128 * 1024).unwrap()
        else {
            panic!("expected image control")
        };
        assert_eq!(bytes.len(), MAX_ENCODED);
        assert!(bytes.capacity() > MAX_ENCODED);
        let input = UserImageInput::new(ImageInputLimits::default()).unwrap();
        input.register_wire(bytes, &format).unwrap();
        let asset = input.state.lock().unwrap().latest.clone().unwrap();
        assert_eq!(asset.bytes.len(), MAX_ENCODED);
        let decoded = input
            .decoder
            .submit(asset.bytes.to_vec())
            .unwrap()
            .await
            .unwrap();
        assert_eq!(decoded.image, [17, 34, 51]);
        let oversized = vec![0; MAX_ENCODED + 1];
        assert!(input.register_wire(oversized, "image/png").is_err());
        let mut padded = Vec::with_capacity(MAX_ENCODED + 1);
        padded.extend_from_slice(&small);
        assert!(input.register(padded, "image/png").is_err());
        input.shutdown_joined().await.unwrap();
    }
    struct QueuedRequestGate {
        input: Arc<UserImageInput>,
        entered: tokio::sync::Notify,
        release: Semaphore,
        stale_done: tokio::sync::Notify,
        retired_id: u64,
    }
    #[async_trait]
    impl FrameProcessor for QueuedRequestGate {
        fn name(&self) -> &str {
            "queued-image-request-gate"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if let FrameData::UserImageRequest(request) = &frame.data {
                if request.user_id == "gate" {
                    self.entered.notify_one();
                    self.release.acquire().await.unwrap().forget();
                    return Ok(());
                }
            }
            let old = frame.id == self.retired_id;
            self.input.process(frame, direction, ctx).await?;
            if old {
                self.stale_done.notify_one();
            }
            Ok(())
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.input.cleanup().await
        }
    }

    #[tokio::test]
    async fn retired_queued_system_request_is_silent_and_fresh_request_still_responds() {
        let input = Arc::new(UserImageInput::new(ImageInputLimits::default()).unwrap());
        input.register(png([41, 42, 43]), "image/png").unwrap();
        let retired = request();
        let gate = Arc::new(QueuedRequestGate {
            input: input.clone(),
            entered: tokio::sync::Notify::new(),
            release: Semaphore::new(0),
            stale_done: tokio::sync::Notify::new(),
            retired_id: retired.id,
        });
        let mut worker = Pipeline::new(vec![gate.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let mut first = request();
        let FrameData::UserImageRequest(payload) = &mut first.data else {
            unreachable!()
        };
        payload.user_id = "gate".into();
        worker
            .sender()
            .send(first, Direction::Upstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        // The old request is in the real System queue behind the blocked
        // System invocation; interception has not sampled a newer generation.
        worker
            .sender()
            .send(retired, Direction::Upstream)
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::Interruption), Direction::Downstream)
            .await
            .unwrap();
        gate.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), gate.stale_done.notified())
            .await
            .unwrap();
        assert!(worker.errors().is_empty());
        assert!(input.state.lock().unwrap().seen.is_empty());
        assert_eq!(input.state.lock().unwrap().decoded_bytes, 0);
        assert_eq!(input.decoder.status().outstanding_jobs, 0);
        assert_eq!(input.slots.available_permits(), 2);
        worker
            .sender()
            .send(request(), Direction::Upstream)
            .await
            .unwrap();
        let fresh = next_image(&mut worker).await;
        let FrameData::UserImage(image) = &fresh.data else {
            unreachable!()
        };
        assert_eq!(image.image.image, [41, 42, 43]);
        assert_eq!(input.state.lock().unwrap().seen.len(), 1);
        drop(fresh);
        drop(gate);
        stop(worker, &input).await;
    }

    #[tokio::test]
    async fn terminal_and_runtime_close_clear_cache_join_jobs_and_release_last_carriers() {
        for mode in ["cancel", "end", "drop-runtime"] {
            let input = Arc::new(UserImageInput::new(ImageInputLimits::default()).unwrap());
            input.register(png([21, 22, 23]), "image/png").unwrap();
            let mut worker = Pipeline::new(vec![input.clone()])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            worker
                .sender()
                .send(request(), Direction::Upstream)
                .await
                .unwrap();
            let first = next_image(&mut worker).await;
            worker
                .sender()
                .send(request(), Direction::Upstream)
                .await
                .unwrap();
            let second = next_image(&mut worker).await;
            assert_eq!(input.slots.available_permits(), 0, "{mode}");
            if mode == "drop-runtime" {
                drop(worker);
                tokio::time::timeout(Duration::from_secs(2), async {
                    loop {
                        if !input.state.lock().unwrap().accepting && Arc::strong_count(&input) == 1
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
            } else {
                if mode == "cancel" {
                    worker.cancel("teardown test").await.unwrap();
                } else {
                    worker.end().await.unwrap();
                }
                tokio::time::timeout(Duration::from_secs(2), async {
                    while worker.recv().await.is_some() {}
                    worker.join().await.unwrap();
                })
                .await
                .unwrap();
                drop(worker);
            }
            assert!(!input.state.lock().unwrap().accepting, "{mode}");
            assert!(input.state.lock().unwrap().latest.is_none(), "{mode}");
            assert!(
                !first
                    .native_user_image_constraint(Direction::Downstream)
                    .unwrap()
                    .unwrap()
                    .is_current(),
                "{mode}"
            );
            input.shutdown_joined().await.unwrap();
            assert_eq!(input.decoder.status().outstanding_jobs, 0, "{mode}");
            assert!(input.decoder.status().joined, "{mode}");
            // Session closure does not release still-retained System carriers.
            assert_eq!(input.slots.available_permits(), 0, "{mode}");
            drop(first);
            drop(second);
            assert_eq!(input.slots.available_permits(), 2, "{mode}");
        }
    }
}
