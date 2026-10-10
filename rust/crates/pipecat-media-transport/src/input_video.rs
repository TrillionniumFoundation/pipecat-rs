use crate::output::bounded;
use crate::queue::{frame_bytes, MediaQueue};
use crate::video::*;
use pipecat_core::*;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::time::Instant;

#[derive(Clone)]
enum RequestState {
    Waiting,
    Queued,
    Finished(Value),
}
struct Request {
    key: VideoSourceKey,
    image_tag: LocalFrameTag,
    payload: UserImageRequestFrame,
    bytes: usize,
    state: RequestState,
}
struct State {
    accepting: bool,
    samplers: VideoInSamplers<u64>,
    available: BTreeSet<VideoSourceKey>,
    pending: BTreeMap<u64, Request>,
    seen_frames: BTreeSet<u64>,
    seen_tools: BTreeSet<String>,
    bytes: usize,
}
pub(crate) struct VideoInput {
    pub config: VideoInputConfig,
    state: Mutex<State>,
    queue: Arc<MediaQueue>,
    origin: Instant,
    byte_limit: usize,
}
impl VideoInput {
    pub fn new(
        config: VideoInputConfig,
        queue: Arc<MediaQueue>,
        byte_limit: usize,
    ) -> Result<Arc<Self>, ProcessorError> {
        config.validate()?;
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                accepting: false,
                samplers: VideoInSamplers::new(config.max_sources, config.max_pending_requests)?,
                available: BTreeSet::new(),
                pending: BTreeMap::new(),
                seen_frames: BTreeSet::new(),
                seen_tools: BTreeSet::new(),
                bytes: 0,
            }),
            config,
            queue,
            origin: Instant::now(),
            byte_limit,
        }))
    }
    pub fn start(&self) {
        self.state.lock().unwrap().accepting = true;
    }
    pub fn capture(
        &self,
        key: VideoSourceKey,
        params: VideoInSourceParams,
        available: bool,
    ) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return Err(ProcessorError::new("video input is closed"));
        }
        state.samplers.capture(key.clone(), params)?;
        set_availability(&mut state, key, available);
        self.queue.available.notify_one();
        Ok(())
    }
    /// Atomically registers configured policies. The backend has already opened
    /// these captures and supplies the subset currently sending video.
    pub fn connected(
        &self,
        participant: VideoParticipantId,
        available: &[VideoSourceId],
    ) -> Result<(), ProcessorError> {
        if available.len() > self.config.max_sources {
            return Err(ProcessorError::new(
                "video availability list exceeds source limit",
            ));
        }
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return Err(ProcessorError::new("video input is closed"));
        }
        let mut samplers = state.samplers.clone();
        for (source, params) in &self.config.sources {
            samplers.capture(
                VideoSourceKey::new(participant.clone(), source.clone()),
                *params,
            )?;
        }
        state.samplers = samplers;
        for source in self.config.sources.keys() {
            set_availability(
                &mut state,
                VideoSourceKey::new(participant.clone(), source.clone()),
                available.contains(source),
            );
        }
        self.queue.available.notify_one();
        Ok(())
    }
    pub fn availability(
        &self,
        key: &VideoSourceKey,
        available: bool,
    ) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return Err(ProcessorError::new("video input is closed"));
        }
        if !state.samplers.is_capturing(key) {
            return Err(ProcessorError::new("video source is not captured"));
        }
        set_availability(&mut state, key.clone(), available);
        self.queue.available.notify_one();
        Ok(())
    }
    pub fn remove(&self, participant: Option<&VideoParticipantId>) {
        let mut state = self.state.lock().unwrap();
        if let Some(participant) = participant {
            state.samplers.remove_participant(participant);
            state
                .available
                .retain(|key| &key.participant != participant);
        } else {
            state.samplers.clear();
            state.available.clear();
        }
        for request in state
            .pending
            .values_mut()
            .filter(|r| participant.is_none_or(|p| p == &r.key.participant))
        {
            finish_error(request, "video participant disconnected");
        }
        self.queue.available.notify_one();
    }
    pub fn request(&self, frame: Frame) -> Result<(), ProcessorError> {
        let FrameData::UserImageRequest(payload) = frame.data else {
            return Err(ProcessorError::new("expected an image request"));
        };
        let key = VideoSourceKey::new(
            VideoParticipantId::new(payload.user_id.clone())?,
            VideoSourceId::new(
                payload
                    .video_source
                    .clone()
                    .unwrap_or_else(|| "camera".into()),
            )?,
        );
        for id in [&payload.function_name, &payload.tool_call_id]
            .into_iter()
            .flatten()
        {
            if id.is_empty() || id.len() > VIDEO_IDENTIFIER_BYTES {
                return Err(ProcessorError::new(
                    "invalid image request identifier length",
                ));
            }
        }
        let bytes = request_bytes(&payload)
            .saturating_add(key.participant.as_str().len())
            .saturating_add(key.source.as_str().len());
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return Err(ProcessorError::new("video input is closed"));
        }
        if state.seen_frames.contains(&frame.id)
            || payload
                .tool_call_id
                .as_ref()
                .is_some_and(|id| state.seen_tools.contains(id))
        {
            return Err(ProcessorError::new("duplicate image request identity"));
        }
        if state.pending.len() >= self.config.max_pending_requests
            || state.seen_frames.len() >= self.config.max_request_ids
            || bytes > self.config.max_request_bytes.saturating_sub(state.bytes)
        {
            return Err(ProcessorError::new("image request bounds exhausted"));
        }
        let mut request = Request {
            key: key.clone(),
            image_tag: LocalFrameTag::new(),
            payload,
            bytes,
            state: RequestState::Waiting,
        };
        if !state.samplers.is_capturing(&key) {
            finish_error(&mut request, "video source is not captured");
        } else if !state.available.contains(&key) {
            finish_error(&mut request, "video source is not sending");
        } else {
            state.samplers.add_request(&key, frame.id)?;
        }
        state.seen_frames.insert(frame.id);
        if let Some(id) = &request.payload.tool_call_id {
            state.seen_tools.insert(id.clone());
        }
        state.bytes += bytes;
        state.pending.insert(frame.id, request);
        self.queue.available.notify_one();
        Ok(())
    }
    /// Sampler mutation, request association and bounded media admission share
    /// one lock. A rejected image cannot consume the next waiting request.
    pub fn push(&self, key: &VideoSourceKey, mut frame: Frame) -> Result<(), ProcessorError> {
        let FrameData::InputImage(image) = &frame.data else {
            return Err(ProcessorError::new("video ingress requires an InputImage"));
        };
        if image.size.0 == 0
            || image.size.1 == 0
            || image
                .format
                .as_ref()
                .is_some_and(|s| s.len() > VIDEO_IDENTIFIER_BYTES)
        {
            return Err(ProcessorError::new("invalid image dimensions or format"));
        }
        if frame_bytes(&frame) > self.byte_limit {
            return Err(ProcessorError::new(
                "input image exceeds the media byte budget",
            ));
        }
        let mut state = self.state.lock().unwrap();
        if !state.accepting {
            return Err(ProcessorError::new("video input is closed"));
        }
        if !state.available.contains(key) {
            return Ok(());
        }
        let Some(mut sampler) = state.samplers.samplers.get(key).cloned() else {
            return Ok(());
        };
        let (due, request_id) = sampler.sample(self.origin.elapsed());
        if !due {
            return Ok(());
        }
        let request = request_id
            .and_then(|id| state.pending.get(&id))
            .map(|request| {
                let mut payload = request.payload.clone();
                // Completion belongs to this input, never to a second downstream caller.
                payload.result_callback = None;
                payload
            });
        let FrameData::InputImage(image) = frame.data else {
            unreachable!()
        };
        frame.data = FrameData::UserImage(UserImageRawFrame {
            image,
            user_id: key.participant.as_str().into(),
            text: request.as_ref().and_then(|r| r.text.clone()),
            append_to_context: request.as_ref().and_then(|r| r.append_to_context),
            request: request.map(Box::new),
            run_llm: None,
        });
        frame.transport_source = Some(key.source.as_str().into());
        if let Some(id) = request_id {
            state.pending[&id].image_tag.mark(&mut frame)?;
        }
        self.queue.push(frame)?;
        state.samplers.samplers.insert(key.clone(), sampler);
        if let Some(id) = request_id {
            state.pending.get_mut(&id).unwrap().state = RequestState::Queued;
        }
        Ok(())
    }
    pub fn shutdown(&self, cancel: bool) {
        let mut state = self.state.lock().unwrap();
        state.accepting = false;
        state.samplers.clear();
        state.available.clear();
        for request in state.pending.values_mut() {
            if cancel || matches!(request.state, RequestState::Waiting) {
                finish_error(
                    request,
                    if cancel {
                        "video input cancelled"
                    } else {
                        "video input ended before an image arrived"
                    },
                );
            }
        }
        if cancel {
            self.queue.cancel();
        } else {
            self.queue.end();
        }
    }
    pub fn request_for_image(&self, frame: &Frame) -> Option<u64> {
        if !matches!(&frame.data, FrameData::UserImage(image) if image.request.is_some()) {
            return None;
        }
        // A public image ID can be reused after source-off while its old image
        // is still queued. Only private provenance can identify its request.
        self.state
            .lock()
            .unwrap()
            .pending
            .iter()
            .find_map(|(id, request)| {
                (matches!(request.state, RequestState::Queued) && request.image_tag.contains(frame))
                    .then_some(*id)
            })
    }
    pub fn fail_image(&self, request_id: Option<u64>) {
        let mut state = self.state.lock().unwrap();
        if let Some(request) = request_id.and_then(|id| state.pending.get_mut(&id)) {
            finish_error(request, "image input processing failed");
        }
        self.queue.available.notify_one();
    }
    pub async fn forward(
        &self,
        frame: Frame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let correlated =
            matches!(&frame.data, FrameData::UserImage(image) if image.request.is_some());
        if !correlated {
            return ctx.push(frame, Direction::Downstream).await;
        }
        let Some(request_id) = self.request_for_image(&frame) else {
            return Ok(());
        };
        let mut push = Box::pin(ctx.push(frame, Direction::Downstream));
        poll_fn(|cx| {
            let mut state = self.state.lock().unwrap();
            let request = state
                .pending
                .get_mut(&request_id)
                .filter(|r| matches!(r.state, RequestState::Queued));
            let Some(request) = request else {
                return Poll::Ready(Ok(()));
            };
            // Runtime admission is nonblocking. Polling under the lifecycle lock
            // also covers a future implementation that yields before admission.
            match push.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    match &result {
                        Ok(()) => request.state = RequestState::Finished(Value::Null),
                        Err(_) => finish_error(request, "image downstream admission failed"),
                    }
                    Poll::Ready(result)
                }
                Poll::Pending => Poll::Pending,
            }
        })
        .await
    }
    /// Claims a callback only when its future is polled. Cancellation before
    /// that point leaves ownership in the ledger for cleanup; after invocation,
    /// arbitrary application callbacks are attempted once and timeout bounded.
    pub async fn complete(&self, timeout: Duration) -> Result<(), ProcessorError> {
        let ids: Vec<_> = self
            .state
            .lock()
            .unwrap()
            .pending
            .iter()
            .filter_map(|(id, r)| matches!(r.state, RequestState::Finished(_)).then_some(*id))
            .collect();
        let mut futures: Vec<_> = ids
            .into_iter()
            .map(|id| {
                Some(Box::pin(async move {
                    let request = {
                        let mut state = self.state.lock().unwrap();
                        let Some(request) = state.pending.remove(&id) else {
                            return Ok(());
                        };
                        state.bytes -= request.bytes;
                        request
                    };
                    let RequestState::Finished(value) = request.state else {
                        unreachable!()
                    };
                    if let Some(callback) = request.payload.result_callback {
                        callback.call(value).await
                    } else {
                        Ok(())
                    }
                }))
            })
            .collect();
        let mut first_error = None;
        bounded(
            timeout,
            poll_fn(|cx| {
                let mut remaining = false;
                for future in &mut futures {
                    if let Some(active) = future {
                        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            Pin::new(active).poll(cx)
                        }))
                        .unwrap_or_else(|_| {
                            Poll::Ready(Err(ProcessorError::new("image request callback panicked")))
                        });
                        match polled {
                            Poll::Ready(result) => {
                                if let Err(error) = result {
                                    first_error.get_or_insert(error);
                                }
                                *future = None;
                            }
                            Poll::Pending => remaining = true,
                        }
                    }
                }
                if remaining {
                    Poll::Pending
                } else {
                    Poll::Ready(first_error.take().map_or(Ok(()), Err))
                }
            }),
        )
        .await
    }
    pub fn pending(&self) -> usize {
        self.state.lock().unwrap().pending.len()
    }
}
fn set_availability(state: &mut State, key: VideoSourceKey, available: bool) {
    if available {
        state.available.insert(key);
    } else {
        state.available.remove(&key);
        state
            .samplers
            .take_requests(&key.participant, Some(&key.source));
        for request in state.pending.values_mut().filter(|r| r.key == key) {
            finish_error(request, "video source stopped sending");
        }
    }
}
fn finish_error(request: &mut Request, message: &str) {
    if !matches!(request.state, RequestState::Finished(_)) {
        request.state = RequestState::Finished(
            json!({"error": message, "participant_id": request.key.participant.as_str(), "video_source": request.key.source.as_str()}),
        );
    }
}
pub(crate) fn request_bytes(request: &UserImageRequestFrame) -> usize {
    [
        &request.text,
        &request.video_source,
        &request.function_name,
        &request.tool_call_id,
    ]
    .into_iter()
    .flatten()
    .fold(request.user_id.len(), |size, text| {
        size.saturating_add(text.len())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn key(source: &str) -> VideoSourceKey {
        VideoSourceKey::new(
            VideoParticipantId::new("peer").unwrap(),
            VideoSourceId::new(source).unwrap(),
        )
    }
    fn request(key: &VideoSourceKey, calls: &Arc<AtomicUsize>) -> Frame {
        let calls = calls.clone();
        Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
            user_id: key.participant.as_str().into(),
            video_source: Some(key.source.as_str().into()),
            text: None,
            append_to_context: None,
            function_name: None,
            tool_call_id: None,
            result_callback: Some(LocalResultCallback::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            })),
        }))
    }
    fn image() -> Frame {
        Frame::new(FrameData::InputImage(ImageRawFrame {
            image: vec![0; 3],
            size: (1, 1),
            format: Some("RGB".into()),
        }))
    }
    fn state(capacity: usize) -> Arc<VideoInput> {
        let video = VideoInput::new(
            VideoInputConfig::default(),
            MediaQueue::new(capacity, 4096),
            4096,
        )
        .unwrap();
        video.start();
        video
            .capture(key("camera"), VideoInSourceParams::on_request_only(), true)
            .unwrap();
        video
    }
    #[tokio::test]
    async fn full_media_queue_rolls_back_sampler_and_next_request() {
        let video = state(1);
        let calls = Arc::new(AtomicUsize::new(0));
        video.queue.push(Frame::text("full")).unwrap();
        video.request(request(&key("camera"), &calls)).unwrap();
        assert!(video.push(&key("camera"), image()).is_err());
        assert!(
            video.state.lock().unwrap().samplers.samplers[&key("camera")]
                .clone()
                .sample(Duration::ZERO)
                .1
                .is_some()
        );
        video.queue.pop().unwrap();
        video.push(&key("camera"), image()).unwrap();
        video.availability(&key("camera"), false).unwrap();
        video.shutdown(true);
        video.complete(Duration::from_secs(1)).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(video.pending(), 0);
    }
    #[tokio::test]
    async fn concurrent_request_shutdown_never_orphans_an_admitted_callback() {
        for _ in 0..64 {
            let video = state(1);
            let calls = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(std::sync::Barrier::new(2));
            let reader = video.clone();
            let signal = gate.clone();
            let frame = request(&key("camera"), &calls);
            let admission = std::thread::spawn(move || {
                signal.wait();
                reader.request(frame).is_ok()
            });
            gate.wait();
            video.shutdown(true);
            let admitted = admission.join().unwrap();
            video.complete(Duration::from_secs(1)).await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(admitted));
            assert_eq!(video.pending(), 0);
        }
    }
    #[tokio::test]
    async fn callback_error_or_stall_cannot_skip_another_owned_callback() {
        for stalled in [false, true] {
            let video = state(1);
            let calls = Arc::new(AtomicUsize::new(0));
            let mut first = request(&key("camera"), &calls);
            if let FrameData::UserImageRequest(request) = &mut first.data {
                let calls = calls.clone();
                request.result_callback = Some(LocalResultCallback::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        if stalled {
                            std::future::pending().await
                        } else {
                            Err(ProcessorError::new("callback failed"))
                        }
                    }
                }));
            }
            video.request(first).unwrap();
            video.request(request(&key("camera"), &calls)).unwrap();
            video.shutdown(true);
            assert!(video.complete(Duration::from_millis(1)).await.is_err());
            video.complete(Duration::from_secs(1)).await.unwrap();
            assert_eq!(calls.load(Ordering::SeqCst), 2);
            assert_eq!(video.pending(), 0);
        }
    }
    #[tokio::test]
    async fn callback_panics_do_not_skip_remaining_callbacks() {
        let video = state(1);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut first = request(&key("camera"), &calls);
        if let FrameData::UserImageRequest(request) = &mut first.data {
            request.result_callback = Some(LocalResultCallback::new(|_| async {
                panic!("deliberately panicking application callback")
            }));
        }
        video.request(first).unwrap();
        video.request(request(&key("camera"), &calls)).unwrap();
        video.shutdown(true);
        assert!(video
            .complete(Duration::from_secs(1))
            .await
            .unwrap_err()
            .message
            .contains("panicked"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(video.pending(), 0);
    }
    #[tokio::test]
    async fn configured_sources_register_atomically_when_participant_capacity_is_full() {
        let mut config = VideoInputConfig {
            max_sources: 3,
            ..Default::default()
        };
        config
            .sources
            .insert(VideoSourceId::camera(), VideoInSourceParams::default());
        config
            .sources
            .insert(VideoSourceId::screen(), VideoInSourceParams::default());
        let video = VideoInput::new(config, MediaQueue::new(1, 4096), 4096).unwrap();
        video.start();
        video
            .connected(
                VideoParticipantId::new("first").unwrap(),
                &[VideoSourceId::camera()],
            )
            .unwrap();
        assert!(video
            .connected(
                VideoParticipantId::new("second").unwrap(),
                &[VideoSourceId::camera()]
            )
            .is_err());
        assert_eq!(video.state.lock().unwrap().samplers.samplers.len(), 2);
    }
}
