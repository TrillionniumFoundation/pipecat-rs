//! Restrictive, process-local freshness for plain requested RGB images.
//!
//! This is not capture, tool-result, or provider authority. Native application
//! code can still construct unmarked frames. Eval-owned paths avoid pixel clones;
//! sharing this marker shares a lease, not the public pixel Vec allocation.
use crate::{Direction, Frame, FrameData, InvocationFence, ProcessorError};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex, MutexGuard};

const MAX_HEADER: usize = 16 * 1024;
const MAX_RGB: usize = 786_432;

/// Private session provenance for a bounded plain-image responder.
#[derive(Debug, Clone, Default)]
pub struct NativeUserImageSource(Arc<SourceState>);

#[derive(Debug)]
struct SourceState {
    closed: Mutex<bool>,
    changed: tokio::sync::watch::Sender<bool>,
}
impl Default for SourceState {
    fn default() -> Self {
        Self {
            closed: Mutex::new(false),
            changed: tokio::sync::watch::channel(false).0,
        }
    }
}
impl NativeUserImageSource {
    pub fn new() -> Self {
        Self::default()
    }
    /// Close new history and provider-stream admission. Existing marked
    /// completion guards are notified to cancel their retained streams; the
    /// owning pipeline still performs ordinary terminal cleanup and joining.
    pub fn close(&self) {
        {
            *self
                .0
                .closed
                .lock()
                .unwrap_or_else(|error| error.into_inner()) = true;
        }
        // Watch wakers may immediately resume provider work. Never publish while
        // holding the source lock used by the synchronous admission transaction.
        self.0.changed.send_replace(true);
    }
    /// Bind exactly once, before enqueue, to the original accepted request's
    /// invocation. `lease` must retain the responder's request-slot/storage
    /// reservation and encoded snapshot until the final image carrier drops.
    pub fn bind(
        &self,
        response: &mut Frame,
        request: &Frame,
        cause: InvocationFence,
        lease: Arc<dyn Send + Sync>,
    ) -> Result<(), ProcessorError> {
        if *self
            .0
            .closed
            .lock()
            .map_err(|_| error("image source lock poisoned"))?
        {
            return Err(error("image source is closed"));
        }
        if response.name.len() > 256 {
            return Err(error("image frame name exceeds bound"));
        }
        validate_metadata(request)?;
        response
            .require_no_native_llm_continuation()
            .map_err(error)?;
        request
            .require_no_native_llm_continuation()
            .map_err(error)?;
        let FrameData::UserImageRequest(original) = &request.data else {
            return Err(error("image constraint requires a genuine image request"));
        };
        if original.user_id.len() > 256
            || original.text.as_ref().is_some_and(|text| text.len() > 8192)
            || original
                .video_source
                .as_ref()
                .is_some_and(|source| source.len() > 256)
        {
            return Err(error("plain image request fields exceed their bounds"));
        }
        if original.function_name.is_some()
            || original.tool_call_id.is_some()
            || original.result_callback.is_some()
        {
            return Err(error("image constraint rejects tool/callback authority"));
        }
        bounded_digest(
            &(
                request.id,
                &request.metadata,
                &request.transport_source,
                &request.transport_destination,
                original,
            ),
            MAX_HEADER,
        )?;
        let FrameData::UserImage(image) = &response.data else {
            return Err(error("image constraint requires a genuine UserImage"));
        };
        if image.request.as_deref() != Some(original)
            || image.user_id != original.user_id
            || image.text != original.text
            || image.append_to_context != original.append_to_context
        {
            return Err(error("image response does not match its plain request"));
        }
        let digest = image_digest(response)?;
        response.native_user_image = Some(NativeUserImageConstraint {
            request: Arc::new(RequestState {
                source: self.0.clone(),
                request_id: request.id,
                fence: cause,
                consumed: Mutex::new(RequestConsumption::default()),
            }),
            _lease: lease,
            frame_id: response.id,
            frame_name: response.name.clone(),
            digest,
        });
        Ok(())
    }
    /// Validate before dispatch, including a changed public FrameData variant.
    /// This is provenance validation, not an atomic context-admission check.
    pub fn matches(&self, frame: &Frame) -> Result<bool, ProcessorError> {
        if let Some(image) = frame.native_user_image_constraint(Direction::Downstream)? {
            return Ok(Arc::ptr_eq(&self.0, &image.request.source));
        }
        if let Some(context) = frame.native_user_image_context(Direction::Upstream)? {
            return Ok(Arc::ptr_eq(&self.0, &context.request.source));
        }
        Ok(false)
    }
}

#[derive(Debug)]
struct RequestState {
    source: Arc<SourceState>,
    request_id: u64,
    fence: InvocationFence,
    consumed: Mutex<RequestConsumption>,
}
#[derive(Debug, Default)]
struct RequestConsumption {
    history_committed: bool,
    inference_admitted: bool,
}

/// One sealed frame view. Clones and duplicates share the original request's
/// consume-once state and storage lease, never a newly sampled causal scope.
#[derive(Clone)]
pub struct NativeUserImageConstraint {
    request: Arc<RequestState>,
    _lease: Arc<dyn Send + Sync>,
    frame_id: u64,
    frame_name: String,
    digest: [u8; 32],
}
impl std::fmt::Debug for NativeUserImageConstraint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeUserImageConstraint")
            .field("request_id", &self.request.request_id)
            .finish_non_exhaustive()
    }
}
impl NativeUserImageConstraint {
    pub fn request_id(&self) -> u64 {
        self.request.request_id
    }
    pub fn is_current(&self) -> bool {
        self.request.fence.is_current()
            && self
                .request
                .source
                .closed
                .lock()
                .is_ok_and(|closed| !*closed)
    }
    /// Intersect only: neither this operation nor a duplicate renews the source.
    pub fn intersect_invocation(
        &self,
        current: &InvocationFence,
    ) -> Result<InvocationFence, ProcessorError> {
        self.request.fence.intersection(current)
    }
    /// Acquire only inside the synchronous intersected invocation gate, before
    /// assistant/history locks. No callbacks, routing, or await may occur while
    /// this token is held. Dropping it before complete leaves admission retryable.
    pub fn begin_commit(&self) -> Result<Option<NativeUserImageCommit<'_>>, ProcessorError> {
        let source = self
            .request
            .source
            .closed
            .lock()
            .map_err(|_| error("image source lock poisoned"))?;
        if *source {
            return Ok(None);
        }
        let consumed = self
            .request
            .consumed
            .lock()
            .map_err(|_| error("image consumption lock poisoned"))?;
        if consumed.history_committed {
            return Ok(None);
        }
        Ok(Some(NativeUserImageCommit {
            request: &self.request,
            _source: source,
            consumed,
        }))
    }
    pub(crate) fn duplicate_identity(&mut self, id: u64, name: &str) {
        self.frame_id = id;
        self.frame_name = name.to_owned();
    }
    fn validate(&self, frame: &Frame, direction: Direction) -> Result<(), ProcessorError> {
        frame.require_no_other_user_image_authority()?;
        if direction != Direction::Downstream
            || frame.id != self.frame_id
            || frame.name != self.frame_name
            || image_digest(frame)? != self.digest
        {
            return Err(error(
                "native user image payload, identity or direction changed",
            ));
        }
        Ok(())
    }
}

/// Consume-once transaction token. It retains no additional pixel allocation.
pub struct NativeUserImageCommit<'a> {
    request: &'a Arc<RequestState>,
    consumed: MutexGuard<'a, RequestConsumption>,
    _source: MutexGuard<'a, bool>,
}
impl NativeUserImageCommit<'_> {
    /// Prepare only an ordinary context produced by this image-history
    /// transaction. The caller must roll back history if sealing fails.
    /// The context keeps the original constraint but no image/snapshot lease.
    pub fn prepare_context(
        &self,
        frame: &mut Frame,
        invocation: &InvocationFence,
    ) -> Result<(), ProcessorError> {
        frame.require_no_native_llm_continuation().map_err(error)?;
        let fence = self.request.fence.intersection(invocation)?;
        let digest = context_digest(frame)?;
        frame.native_user_image_context = Some(NativeUserImageContext {
            request: self.request.clone(),
            fence,
            frame_id: frame.id,
            digest,
        });
        Ok(())
    }
    /// Called only after synchronous history and replay mutation succeeds.
    pub fn complete(mut self) {
        self.consumed.history_committed = true;
    }
}

/// Lightweight derived inference constraint, sealed to one exact context.
#[derive(Debug, Clone)]
pub struct NativeUserImageContext {
    request: Arc<RequestState>,
    fence: InvocationFence,
    frame_id: u64,
    digest: [u8; 32],
}
impl NativeUserImageContext {
    pub fn invocation_fence(&self) -> &InvocationFence {
        &self.fence
    }
    pub fn is_current(&self) -> bool {
        self.fence.is_current()
            && self
                .request
                .source
                .closed
                .lock()
                .is_ok_and(|closed| !*closed)
    }
    pub fn request_id(&self) -> u64 {
        self.request.request_id
    }
    /// Observe source closure without retaining image/snapshot storage. Combine
    /// notifications with an is_current check on every completion-future poll.
    pub fn watch_closed(&self) -> tokio::sync::watch::Receiver<bool> {
        self.request.source.changed.subscribe()
    }
    /// One local inference admission after FIFO and before response boundaries.
    /// The caller must recheck source freshness after response-start waits and
    /// guard subsequent future polls; no remote write is made atomic here.
    /// Lock order is original/current runtime admission, source-open, request
    /// consumption. No provider work, callbacks, routing, or await occurs here.
    /// Closure winning this transaction suppresses the request; successful
    /// admission cannot retroactively recall a request already sent.
    pub fn try_admit_inference(&self, current: &InvocationFence) -> Result<bool, ProcessorError> {
        let fence = self.fence.intersection(current)?;
        fence
            .with_current(|| {
                let closed = self
                    .request
                    .source
                    .closed
                    .lock()
                    .map_err(|_| error("image source lock poisoned"))?;
                if *closed {
                    return Ok(false);
                }
                let mut consumption = self
                    .request
                    .consumed
                    .lock()
                    .map_err(|_| error("image consumption lock poisoned"))?;
                if !consumption.history_committed || consumption.inference_admitted {
                    return Ok(false);
                }
                consumption.inference_admitted = true;
                Ok(true)
            })
            .unwrap_or(Ok(false))
    }
    fn validate(&self, frame: &Frame, direction: Direction) -> Result<(), ProcessorError> {
        frame.require_no_other_user_image_authority()?;
        if direction != Direction::Upstream
            || self.frame_id != frame.id
            || context_digest(frame)? != self.digest
        {
            return Err(error(
                "native image-derived context payload, identity or direction changed",
            ));
        }
        if !self
            .request
            .consumed
            .lock()
            .map_err(|_| error("image consumption lock poisoned"))?
            .history_committed
        {
            return Err(error("image-derived context preceded its history commit"));
        }
        Ok(())
    }
}

impl Frame {
    /// Always call before variant dispatch; a changed non-image cannot bypass
    /// its existing restriction. Unmarked legacy/native frames return None.
    pub fn native_user_image_constraint(
        &self,
        direction: Direction,
    ) -> Result<Option<&NativeUserImageConstraint>, ProcessorError> {
        if let Some(constraint) = &self.native_user_image {
            constraint.validate(self, direction)?;
        }
        Ok(self.native_user_image.as_ref())
    }
    pub fn native_user_image_context(
        &self,
        direction: Direction,
    ) -> Result<Option<&NativeUserImageContext>, ProcessorError> {
        if let Some(context) = &self.native_user_image_context {
            context.validate(self, direction)?;
        }
        Ok(self.native_user_image_context.as_ref())
    }
}

fn error(message: impl ToString) -> ProcessorError {
    ProcessorError::new(message.to_string())
}

struct BoundedDigest {
    hash: Sha256,
    remaining: usize,
}
impl std::io::Write for BoundedDigest {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.remaining = self
            .remaining
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("image fingerprint bound"))?;
        self.hash.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn bounded_digest(value: &impl Serialize, limit: usize) -> Result<[u8; 32], ProcessorError> {
    let mut digest = BoundedDigest {
        hash: Sha256::new(),
        remaining: limit,
    };
    serde_json::to_writer(&mut digest, value).map_err(|_| {
        error("native image header/context exceeds its bound or contains executable authority")
    })?;
    Ok(digest.hash.finalize().into())
}
fn validate_metadata(frame: &Frame) -> Result<(), ProcessorError> {
    if frame.metadata.len() > 4096
        || frame
            .transport_source
            .as_ref()
            .is_some_and(|source| source.len() > 256)
        || frame
            .transport_destination
            .as_ref()
            .is_some_and(|destination| destination.len() > 256)
    {
        return Err(error("native image routing/metadata exceeds bound"));
    }
    let mut pending: Vec<_> = frame
        .metadata
        .values()
        .map(|value| (value, 0usize))
        .collect();
    let mut visited = 0usize;
    while let Some((value, depth)) = pending.pop() {
        visited += 1;
        if visited > 4096 || depth > 32 || pending.len() > 4096 {
            return Err(error("native image metadata nesting/count exceeds bound"));
        }
        match value {
            serde_json::Value::Array(values) => {
                if pending
                    .len()
                    .checked_add(values.len())
                    .and_then(|count| count.checked_add(visited))
                    .is_none_or(|count| count > 4096)
                {
                    return Err(error("native image metadata count exceeds bound"));
                }
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            serde_json::Value::Object(values) => {
                if pending
                    .len()
                    .checked_add(values.len())
                    .and_then(|count| count.checked_add(visited))
                    .is_none_or(|count| count > 4096)
                {
                    return Err(error("native image metadata count exceeds bound"));
                }
                pending.extend(values.values().map(|value| (value, depth + 1)));
            }
            _ => {}
        }
    }
    Ok(())
}
fn envelope(frame: &Frame) -> impl Serialize + '_ {
    (
        &frame.pts,
        &frame.metadata,
        &frame.transport_source,
        &frame.transport_destination,
        frame.broadcast_sibling_id,
        frame.interruptible,
    )
}
fn image_digest(frame: &Frame) -> Result<[u8; 32], ProcessorError> {
    validate_metadata(frame)?;
    let FrameData::UserImage(image) = &frame.data else {
        return Err(error("native image carrier changed frame type"));
    };
    let Some(request) = image.request.as_deref() else {
        return Err(error("native image lost its request"));
    };
    if request.function_name.is_some()
        || request.tool_call_id.is_some()
        || request.result_callback.is_some()
    {
        return Err(error("native image contains tool/callback authority"));
    }
    let (width, height) = image.image.size;
    let length = (width as usize)
        .checked_mul(height as usize)
        .and_then(|pixels| pixels.checked_mul(3));
    if width == 0
        || height == 0
        || width > 1024
        || height > 1024
        || image.image.format.as_deref() != Some("RGB")
        || length != Some(image.image.image.len())
        || image.image.image.len() > MAX_RGB
        || image.image.image.capacity() > MAX_RGB
    {
        return Err(error("native image requires bounded tightly packed RGB"));
    }
    let header = bounded_digest(
        &(
            envelope(frame),
            image.image.size,
            &image.image.format,
            &image.user_id,
            &image.text,
            image.append_to_context,
            image.run_llm,
            request,
        ),
        MAX_HEADER,
    )?;
    let mut digest = Sha256::new();
    digest.update(header);
    digest.update(&image.image.image);
    Ok(digest.finalize().into())
}
fn context_digest(frame: &Frame) -> Result<[u8; 32], ProcessorError> {
    validate_metadata(frame)?;
    match &frame.data {
        FrameData::LlmContext(_) => {}
        FrameData::LlmContextDetailed(context) if !context.speculation => {}
        _ => {
            return Err(error(
                "image-derived action requires ordinary non-speculative context",
            ))
        }
    }
    // Reuse the bounded native context fingerprint, including direct-tool local
    // identities, without invoking executable handlers or cloning history.
    let payload = crate::realtime_context::fingerprint(&frame.data)?;
    bounded_digest(&(envelope(frame), &frame.name, payload), MAX_HEADER)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        async_trait, FrameProcessor, ImageRawFrame, Pipeline, PipelineWorker, ProcessorContext,
        UserImageRawFrame, UserImageRequestFrame,
    };
    use std::time::Duration;
    use tokio::sync::mpsc;

    fn request() -> Frame {
        Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
            user_id: "synthetic-user".into(),
            text: Some("what color?".into()),
            append_to_context: Some(true),
            video_source: None,
            function_name: None,
            tool_call_id: None,
            result_callback: None,
        }))
    }
    fn response(request: &Frame) -> Frame {
        let FrameData::UserImageRequest(request) = &request.data else {
            unreachable!()
        };
        Frame::new(FrameData::UserImage(UserImageRawFrame {
            image: ImageRawFrame {
                image: vec![255, 0, 0],
                size: (1, 1),
                format: Some("RGB".into()),
            },
            user_id: request.user_id.clone(),
            text: request.text.clone(),
            append_to_context: request.append_to_context,
            request: Some(Box::new(request.clone())),
            run_llm: None,
        }))
    }
    struct Bind {
        source: NativeUserImageSource,
        lease: Arc<()>,
        output: mpsc::UnboundedSender<Frame>,
    }
    #[async_trait]
    impl FrameProcessor for Bind {
        fn name(&self) -> &str {
            "BindPlainImage"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::UserImageRequest(_)) {
                let mut image = response(&frame);
                self.source.bind(
                    &mut image,
                    &frame,
                    ctx.invocation_fence()?,
                    self.lease.clone(),
                )?;
                self.output.send(image).map_err(error)?;
                Ok(())
            } else {
                ctx.push(frame, direction).await
            }
        }
    }
    async fn fixture() -> (PipelineWorker, NativeUserImageSource, Frame, Arc<()>) {
        let source = NativeUserImageSource::new();
        let lease = Arc::new(());
        let (output, mut receiver) = mpsc::unbounded_channel();
        let worker = Pipeline::new(vec![Arc::new(Bind {
            source: source.clone(),
            lease: lease.clone(),
            output,
        })])
        .start(Default::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(request(), Direction::Upstream)
            .await
            .unwrap();
        let image = tokio::time::timeout(Duration::from_secs(3), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        (worker, source, image, lease)
    }
    #[tokio::test]
    async fn sealed_image_rejects_payload_routing_identity_and_wire_laundering() {
        let (worker, source, image, _) = fixture().await;
        assert!(source.matches(&image).unwrap());
        assert!(serde_json::to_vec(&image).is_err());
        assert!(image.require_no_native_llm_continuation().is_err());
        assert!(image
            .native_user_image_constraint(Direction::Upstream)
            .is_err());
        for mutation in 0..13 {
            let mut changed = image.clone();
            match mutation {
                0 => changed.id += 99,
                1 => changed.name.push('!'),
                2 => changed.transport_destination = Some("elsewhere".into()),
                3 => changed.transport_source = Some("elsewhere".into()),
                4 => {
                    changed.metadata.insert("changed".into(), true.into());
                }
                5 => changed.pts = Some(19),
                6 => changed.interruptible = !changed.interruptible,
                7 => changed.data = FrameData::LlmRun,
                8 => {
                    let FrameData::UserImage(image) = &mut changed.data else {
                        unreachable!()
                    };
                    image.image.image[0] = 1;
                }
                9 => {
                    let FrameData::UserImage(image) = &mut changed.data else {
                        unreachable!()
                    };
                    image.text = Some("changed".into());
                }
                10 => {
                    let FrameData::UserImage(image) = &mut changed.data else {
                        unreachable!()
                    };
                    image.request.as_mut().unwrap().user_id.push('!');
                }
                11 => {
                    let FrameData::UserImage(image) = &mut changed.data else {
                        unreachable!()
                    };
                    image.run_llm = Some(false);
                }
                _ => changed.broadcast_sibling_id = Some(2),
            }
            assert!(source.matches(&changed).is_err(), "mutation {mutation}");
            assert!(
                source.matches(&changed.duplicate()).is_err(),
                "duplicate mutation {mutation}"
            );
        }
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
    }
    #[tokio::test]
    async fn clones_duplicates_retain_lease_and_one_commit_but_context_releases_rgb_lease() {
        let (worker, source, image, lease) = fixture().await;
        let baseline = Arc::strong_count(&lease);
        let duplicate = image.duplicate();
        let clone = image.clone();
        assert!(source.matches(&duplicate).unwrap());
        assert_eq!(
            duplicate
                .native_user_image_constraint(Direction::Downstream)
                .unwrap()
                .unwrap()
                .request_id(),
            image
                .native_user_image_constraint(Direction::Downstream)
                .unwrap()
                .unwrap()
                .request_id()
        );
        let constraint = image
            .native_user_image_constraint(Direction::Downstream)
            .unwrap()
            .unwrap();
        let scope = constraint.request.fence.clone();
        let context = scope
            .with_current(|| {
                let commit = constraint.begin_commit().unwrap().unwrap();
                let mut context = Frame::new(FrameData::LlmContext(Default::default()));
                commit.prepare_context(&mut context, &scope).unwrap();
                commit.complete();
                context
            })
            .unwrap();
        assert!(context
            .native_user_image_context(Direction::Upstream)
            .unwrap()
            .is_some());
        assert!(serde_json::to_vec(&context).is_err());
        let derived = context
            .native_user_image_context(Direction::Upstream)
            .unwrap()
            .unwrap();
        assert!(derived.try_admit_inference(&scope).unwrap());
        assert!(!context
            .clone()
            .native_user_image_context(Direction::Upstream)
            .unwrap()
            .unwrap()
            .try_admit_inference(&scope)
            .unwrap());
        assert!(context
            .duplicate()
            .native_user_image_context(Direction::Upstream)
            .is_err());
        assert!(scope
            .with_current(|| duplicate
                .native_user_image_constraint(Direction::Downstream)
                .unwrap()
                .unwrap()
                .begin_commit()
                .unwrap()
                .is_none())
            .unwrap());
        drop(image);
        drop(duplicate);
        assert!(Arc::strong_count(&lease) > baseline - 1);
        drop(clone);
        assert_eq!(Arc::strong_count(&lease), baseline - 1);
        // The derived context still exists and is valid without retaining lease.
        assert!(source.matches(&context).unwrap());
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
    }
    #[tokio::test]
    async fn close_excludes_commit_and_derived_payload_cannot_change_or_rebind() {
        let (worker, source, image, lease) = fixture().await;
        let constraint = image
            .native_user_image_constraint(Direction::Downstream)
            .unwrap()
            .unwrap();
        let scope = constraint.request.fence.clone();
        let mut context = scope
            .with_current(|| {
                let commit = constraint.begin_commit().unwrap().unwrap();
                let mut context = Frame::new(FrameData::LlmContext(Default::default()));
                commit.prepare_context(&mut context, &scope).unwrap();
                commit.complete();
                context
            })
            .unwrap();
        context.data = FrameData::LlmRun;
        assert!(context
            .native_user_image_context(Direction::Upstream)
            .is_err());
        let mut unchanged = image.clone();
        assert!(source
            .bind(&mut unchanged, &request(), scope.clone(), lease)
            .is_err());
        source.close();
        assert!(!constraint.is_current());
        assert!(scope
            .with_current(|| constraint.begin_commit().unwrap().is_none())
            .unwrap());
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
    }
}
