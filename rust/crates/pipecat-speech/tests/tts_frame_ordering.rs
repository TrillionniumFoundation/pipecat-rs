//! Pinned source: services/tts_service.py process_frame, lines 871-875 and
//! 1012-1021. Generic normal downstream frames cannot overtake audio contexts.
//! Local HTTP peers hold real synthesis open; admission is acknowledged only
//! after the actual HTTP processor invocation returns, never by a bypass frame.
#[allow(dead_code)]
mod support;
use pipecat_core::*;
use pipecat_speech::{cartesia::*, *};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

const DEADLINE: Duration = Duration::from_secs(5);
type Admission = (u64, Result<(), ProcessorError>);

struct ObservedTts {
    inner: HttpTtsProcessor,
    admitted: mpsc::Sender<Admission>,
}
#[async_trait]
impl FrameProcessor for ObservedTts {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let id = frame.id;
        let result = self.inner.process(frame, direction, ctx).await;
        self.admitted.try_send((id, result.clone())).unwrap();
        result
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
async fn start(
    config: SpeechConfig,
    limit: usize,
    pipeline: PipelineConfig,
) -> (PipelineWorker, mpsc::Receiver<Admission>) {
    let (admitted, receiver) = mpsc::channel(64);
    let processor = ObservedTts {
        inner: HttpTtsProcessor::new(
            HttpTtsBackend::Cartesia {
                client: CartesiaHttpClient::new(config).unwrap(),
                request: CartesiaSynthesisRequest::new("", "voice"),
            },
            limit,
        )
        .unwrap(),
        admitted,
    };
    (
        Pipeline::new(vec![Arc::new(processor)])
            .start(pipeline)
            .await
            .unwrap(),
        receiver,
    )
}
async fn admit(
    worker: &PipelineWorker,
    accepted: &mut mpsc::Receiver<Admission>,
    frame: Frame,
    direction: Direction,
) -> Result<(), ProcessorError> {
    let id = frame.id;
    worker.sender().send(frame, direction).await.unwrap();
    tokio::time::timeout(DEADLINE, async {
        loop {
            let (received, result) = accepted.recv().await.unwrap();
            if received == id {
                return result;
            }
        }
    })
    .await
    .unwrap()
}
async fn listener() -> (TcpListener, SpeechConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::cartesia("synthetic-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = DEADLINE;
    (listener, config)
}
async fn reply(socket: &mut TcpStream, pcm: &[u8]) {
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", pcm.len()).as_bytes()).await.unwrap();
    socket.write_all(pcm).await.unwrap();
}
async fn finish(
    worker: PipelineWorker,
    mut prefix: Vec<DirectedFrame>,
) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
        .await
        .unwrap()
        .unwrap();
    prefix.extend(frames);
    (prefix, report)
}
fn available(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let mut frames = vec![];
    while let Ok(frame) = worker.try_recv() {
        frames.push(frame);
    }
    frames
}

struct IdleBurst {
    inner: HttpTtsProcessor,
    frames: Mutex<Option<Vec<Frame>>>,
    admitted: mpsc::Sender<Vec<Admission>>,
}
#[async_trait]
impl FrameProcessor for IdleBurst {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Data) {
            let frames = self.frames.lock().unwrap().take().unwrap();
            let mut admitted = vec![];
            // Every inner invocation is immediately ready. On this test's
            // current-thread runtime, spawned tasks cannot poll between frames.
            for frame in frames {
                let id = frame.id;
                let result = self.inner.process(frame, direction, ctx).await;
                admitted.push((id, result));
            }
            self.admitted.try_send(admitted).unwrap();
            Ok(())
        } else {
            self.inner.process(frame, direction, ctx).await
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

#[tokio::test]
async fn idle_thought_and_control_burst_does_not_consume_pending_fence_capacity() {
    let (listener, config) = listener().await;
    let tag = LocalFrameTag::new();
    let transform = LocalMessageTransform::new(Ok);
    let mut marker = Frame::new(FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
        transform: transform.clone(),
        run_llm: Some(false),
    }));
    marker.name = "idle-original-envelope".into();
    marker.set_pipeline_pts(987654);
    marker
        .metadata
        .insert("original".into(), json!({"values":[1,2]}));
    marker.transport_source = Some("source".into());
    marker.transport_destination = Some("destination".into());
    marker.broadcast_sibling_id = Some(42);
    marker.interruptible = false;
    tag.mark(&mut marker).unwrap();
    let expected = vec![
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::LlmThoughtStart),
        Frame::new(FrameData::LlmThoughtText(TextFrame::new("first thought"))),
        Frame::new(FrameData::LlmThoughtText(TextFrame::new("second thought"))),
        Frame::new(FrameData::LlmThoughtEnd),
        Frame::new(FrameData::Control),
        Frame::text("incremental text"),
        Frame::new(FrameData::LlmText(TextFrame::llm("incremental LLM text"))),
        marker.clone(),
    ];
    let (admitted, mut receiver) = mpsc::channel(1);
    let processor = IdleBurst {
        inner: HttpTtsProcessor::new(
            HttpTtsBackend::Cartesia {
                client: CartesiaHttpClient::new(config).unwrap(),
                request: CartesiaSynthesisRequest::new("", "voice"),
            },
            4,
        )
        .unwrap(),
        frames: Mutex::new(Some(expected.clone())),
        admitted,
    };
    let worker = Pipeline::new(vec![Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::Data), Direction::Downstream)
        .await
        .unwrap();
    let admissions = tokio::time::timeout(DEADLINE, receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let (frames, report) = finish(worker, vec![]).await;
    assert_eq!(admissions.len(), expected.len());
    assert!(
        admissions.iter().all(|(_, result)| result.is_ok()),
        "{admissions:?}"
    );
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let forwarded: Vec<_> = frames
        .iter()
        .filter(|frame| {
            expected
                .iter()
                .any(|expected| expected.id == frame.frame.id)
        })
        .collect();
    assert_eq!(forwarded.len(), expected.len());
    for (forwarded, expected) in forwarded.iter().zip(&expected) {
        assert_eq!(forwarded.direction, Direction::Downstream);
        assert_eq!(&forwarded.frame, expected);
        assert_eq!(
            forwarded.frame.observation_token(),
            expected.observation_token()
        );
    }
    let forwarded = &forwarded.last().unwrap().frame;
    assert!(forwarded.has_pipeline_pts() && tag.contains(forwarded));
    let FrameData::LlmMessagesTransform(payload) = &forwarded.data else {
        panic!("lost local capability")
    };
    assert_eq!(payload.transform, transform);
    assert!(payload.transform.apply(vec![]).unwrap().is_empty());
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::TtsStarted(_) | FrameData::TtsAudio(_) | FrameData::TtsStopped(_)
    )));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), listener.accept())
            .await
            .is_err(),
        "idle forwarding synthesized speech"
    );
}

#[tokio::test]
async fn generic_frames_follow_active_and_queued_synthesis_with_original_envelopes() {
    let (listener, config) = listener().await;
    let (captured, capture) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut first).await.json()["transcript"],
            "first"
        );
        captured.send(()).unwrap();
        released.await.unwrap();
        reply(&mut first, &[1, 0]).await;
        let (mut second, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut second).await.json()["transcript"],
            "second"
        );
        reply(&mut second, &[2, 0]).await;
    });
    let (mut worker, mut accepted) = start(config, 16, PipelineConfig::default()).await;
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("first"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, capture)
        .await
        .unwrap()
        .unwrap();
    let before_second = Frame::new(FrameData::OutputTransportMessage(
        json!({"marker":"between"}),
    ));
    admit(
        &worker,
        &mut accepted,
        before_second.clone(),
        Direction::Downstream,
    )
    .await
    .unwrap();
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("second"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    let tag = LocalFrameTag::new();
    let transform = LocalMessageTransform::new(Ok);
    let mut marker = Frame::new(FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
        transform: transform.clone(),
        run_llm: Some(false),
    }));
    marker.name = "original-marker-name".into();
    marker.set_pipeline_pts(123456);
    marker
        .metadata
        .insert("original".into(), json!({"nested":[1,2,3]}));
    marker.transport_source = Some("source".into());
    marker.transport_destination = Some("destination".into());
    marker.broadcast_sibling_id = Some(42);
    marker.interruptible = false;
    tag.mark(&mut marker).unwrap();
    let observation = marker.observation_token();
    let response_start = Frame::new(FrameData::LlmResponseStart);
    let control = Frame::new(FrameData::Control);
    let text = Frame::text("incremental display");
    for frame in [&marker, &response_start, &control, &text] {
        admit(&worker, &mut accepted, frame.clone(), Direction::Downstream)
            .await
            .unwrap();
    }
    let prefix = available(&mut worker);
    let marker_ids = [
        before_second.id,
        marker.id,
        response_start.id,
        control.id,
        text.id,
    ];
    let overtook = prefix.iter().any(|f| marker_ids.contains(&f.frame.id));
    release.send(()).unwrap();
    let (frames, report) = finish(worker, prefix).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
    assert!(
        !overtook,
        "generic downstream frame overtook held HTTP synthesis"
    );
    let index = |id| frames.iter().position(|f| f.frame.id == id).unwrap();
    let stops: Vec<_> = frames
        .iter()
        .enumerate()
        .filter_map(|(index, f)| matches!(f.frame.data, FrameData::TtsStopped(_)).then_some(index))
        .collect();
    assert_eq!(stops.len(), 2);
    assert!(stops[0] < index(before_second.id) && index(before_second.id) < stops[1]);
    assert!(stops[1] < index(marker.id));
    assert!(marker_ids[1..]
        .windows(2)
        .all(|pair| index(pair[0]) < index(pair[1])));
    let forwarded = &frames[index(marker.id)].frame;
    assert_eq!(forwarded, &marker);
    assert_eq!(forwarded.observation_token(), observation);
    assert!(forwarded.has_pipeline_pts() && tag.contains(forwarded));
    let FrameData::LlmMessagesTransform(payload) = &forwarded.data else {
        panic!("lost local capability")
    };
    assert_eq!(payload.transform, transform);
    assert!(payload.transform.apply(vec![]).unwrap().is_empty());
    assert!(matches!(
        frames.last().unwrap().frame.data,
        FrameData::End(_)
    ));
}

#[tokio::test]
async fn system_upstream_and_explicit_skip_text_bypass_held_synthesis() {
    let (listener, config) = listener().await;
    let (captured, capture) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        captured.send(()).unwrap();
        released.await.unwrap();
        reply(&mut socket, &[1, 0]).await;
    });
    let (mut worker, mut accepted) = start(config, 8, PipelineConfig::default()).await;
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("held"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, capture)
        .await
        .unwrap()
        .unwrap();
    let normal = Frame::text("must wait");
    admit(
        &worker,
        &mut accepted,
        normal.clone(),
        Direction::Downstream,
    )
    .await
    .unwrap();
    let mut skipped = TextFrame::new("explicit bypass");
    skipped.skip_tts = Some(true);
    let bypass = [
        (Frame::new(FrameData::System), Direction::Downstream),
        (
            Frame::new(FrameData::OutputTransportMessage(json!({"upstream":true}))),
            Direction::Upstream,
        ),
        (
            Frame::new(FrameData::Text(skipped.clone())),
            Direction::Downstream,
        ),
        (
            Frame::new(FrameData::LlmText(skipped)),
            Direction::Downstream,
        ),
    ];
    for (frame, direction) in &bypass {
        admit(&worker, &mut accepted, frame.clone(), *direction)
            .await
            .unwrap();
    }
    let prefix = available(&mut worker);
    for (expected, direction) in &bypass {
        assert!(prefix
            .iter()
            .any(|f| f.frame == *expected && f.direction == *direction));
    }
    let overtook = prefix.iter().any(|f| f.frame.id == normal.id);
    assert_eq!(
        prefix
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::Start(_)))
            .count(),
        1
    );
    release.send(()).unwrap();
    let (frames, report) = finish(worker, prefix).await;
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!overtook, "ordinary text overtook held synthesis");
    assert!(frames.iter().any(|f| f.frame.id == normal.id));
}

#[tokio::test]
async fn interruption_discards_queued_interruptible_markers_but_retains_survivors() {
    let (listener, config) = listener().await;
    let (captured, capture) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        captured.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(DEADLINE, socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        let (mut fresh, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut fresh).await.json()["transcript"],
            "fresh"
        );
        reply(&mut fresh, &[9, 0]).await;
    });
    let (mut worker, mut accepted) = start(config, 8, PipelineConfig::default()).await;
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("old"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, capture)
        .await
        .unwrap()
        .unwrap();
    let stale = Frame::new(FrameData::Data);
    let survivor = Frame::new(FrameData::Control).with_interruptible(false);
    for frame in [&stale, &survivor] {
        admit(&worker, &mut accepted, frame.clone(), Direction::Downstream)
            .await
            .unwrap();
    }
    let prefix = available(&mut worker);
    let overtook = prefix
        .iter()
        .any(|f| [stale.id, survivor.id].contains(&f.frame.id));
    admit(
        &worker,
        &mut accepted,
        Frame::interruption(),
        Direction::Downstream,
    )
    .await
    .unwrap();
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    let (frames, report) = finish(worker, prefix).await;
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        !overtook,
        "marker overtook held synthesis before interruption"
    );
    assert!(!frames.iter().any(|f| f.frame.id == stale.id));
    assert_eq!(
        frames.iter().filter(|f| f.frame.id == survivor.id).count(),
        1
    );
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAudio(audio) => Some(audio.audio.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(audio, [vec![9, 0]]);
}

#[tokio::test]
async fn normal_fence_limit_is_recoverable_and_response_completion_is_failed() {
    let (listener, config) = listener().await;
    let (captured, capture) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        captured.send(()).unwrap();
        released.await.unwrap();
        reply(&mut socket, &[1, 0]).await;
    });
    let (mut worker, mut accepted) = start(config, 1, PipelineConfig::default()).await;
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("held"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, capture)
        .await
        .unwrap()
        .unwrap();
    let marker = Frame::new(FrameData::Data);
    admit(
        &worker,
        &mut accepted,
        marker.clone(),
        Direction::Downstream,
    )
    .await
    .unwrap();
    let overflow = Frame::new(FrameData::Control);
    let admission = admit(
        &worker,
        &mut accepted,
        overflow.clone(),
        Direction::Downstream,
    )
    .await;
    release.send(()).unwrap();
    let mut prefix = vec![];
    tokio::time::timeout(DEADLINE, async {
        loop {
            let frame = worker.recv().await.unwrap();
            let complete = frame.frame.id == marker.id;
            prefix.push(frame);
            if complete {
                break;
            }
        }
    })
    .await
    .unwrap();
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::LlmResponseEnd),
        Direction::Downstream,
    )
    .await
    .unwrap();
    let (frames, report) = finish(worker, prefix).await;
    server.await.unwrap();
    let error = admission.unwrap_err();
    assert_eq!(error.message, "HTTP TTS pending utterance limit reached");
    assert_eq!(error.category, ErrorCategory::Application);
    assert!(!error.fatal);
    assert!(report
        .errors
        .iter()
        .any(|error| error.error == "HTTP TTS pending utterance limit reached"));
    assert!(!frames.iter().any(|f| f.frame.id == overflow.id));
    let end = frames
        .iter()
        .find(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
        .unwrap();
    assert_eq!(end.frame.metadata.get("failed"), Some(&json!(true)));
}

#[tokio::test]
async fn cancel_closes_http_and_discards_even_uninterruptible_pending_frames() {
    let (listener, config) = listener().await;
    let (captured, capture) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        captured.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(DEADLINE, socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let (mut worker, mut accepted) = start(config, 8, PipelineConfig::default()).await;
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("held"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, capture)
        .await
        .unwrap()
        .unwrap();
    let marker = Frame::new(FrameData::Data).with_interruptible(false);
    admit(
        &worker,
        &mut accepted,
        marker.clone(),
        Direction::Downstream,
    )
    .await
    .unwrap();
    worker.cancel("cancel held synthesis").await.unwrap();
    let mut frames = vec![];
    let report = tokio::time::timeout(DEADLINE, async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        worker.join().await.unwrap()
    })
    .await
    .unwrap();
    server.await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(!frames.iter().any(|f| f.frame.id == marker.id));
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsAudio(_) | FrameData::End(_))));
}

#[tokio::test]
async fn generic_fence_obeys_core_task_budget_without_emitting_false_completion() {
    let (listener, config) = listener().await;
    let (captured, capture) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        support::read_request(&mut socket).await;
        captured.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(DEADLINE, socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let pipeline = PipelineConfig {
        max_background_tasks: 1,
        ..Default::default()
    };
    let (mut worker, mut accepted) = start(config, 8, pipeline).await;
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("held"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, capture)
        .await
        .unwrap()
        .unwrap();
    let marker = Frame::new(FrameData::Data);
    let admission = admit(
        &worker,
        &mut accepted,
        marker.clone(),
        Direction::Downstream,
    )
    .await;
    if admission.is_ok() {
        worker
            .cancel("clean up missing fence regression")
            .await
            .unwrap();
    }
    let mut frames = vec![];
    let result = tokio::time::timeout(DEADLINE, async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        worker.join().await
    })
    .await
    .unwrap();
    server.await.unwrap();
    let error = admission.unwrap_err();
    assert!(error.fatal);
    assert_eq!(error.message, "background task limit reached");
    assert!(
        matches!(result, Err(PipelineError::Execution(ref message)) if message == "background task limit reached")
    );
    assert!(!frames.iter().any(|f| f.frame.id == marker.id
        || matches!(f.frame.data, FrameData::End(_) | FrameData::LlmResponseEnd)));
}

#[tokio::test]
async fn failure_of_earlier_response_does_not_poison_queued_new_response() {
    let (listener, config) = listener().await;
    let (captured, capture) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        support::read_request(&mut old).await;
        captured.send(()).unwrap();
        released.await.unwrap();
        old.write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
        drop(old);
        let (mut fresh, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut fresh).await.json()["transcript"],
            "fresh"
        );
        reply(&mut fresh, &[9, 0]).await;
    });
    let (worker, mut accepted) = start(config, 8, PipelineConfig::default()).await;
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::LlmResponseStart),
        Direction::Downstream,
    )
    .await
    .unwrap();
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("old"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, capture)
        .await
        .unwrap()
        .unwrap();
    let old_end = Frame::new(FrameData::LlmResponseEnd);
    let fresh_end = Frame::new(FrameData::LlmResponseEnd);
    for frame in [
        old_end.clone(),
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))),
        fresh_end.clone(),
    ] {
        admit(&worker, &mut accepted, frame, Direction::Downstream)
            .await
            .unwrap();
    }
    release.send(()).unwrap();
    let (frames, _) = finish(worker, vec![]).await;
    server.await.unwrap();
    let original = frames.iter().find(|f| f.frame.id == old_end.id).unwrap();
    let fresh = frames.iter().find(|f| f.frame.id == fresh_end.id).unwrap();
    assert_eq!(original.frame.metadata.get("failed"), Some(&json!(true)));
    assert!(!fresh.frame.metadata.contains_key("failed"));
}

#[tokio::test]
async fn full_output_lane_fails_explicitly_without_marker_or_terminal_success() {
    let (_listener, config) = listener().await;
    let pipeline = PipelineConfig {
        output_capacity: 1,
        ..Default::default()
    };
    let (mut worker, mut accepted) = start(config, 8, pipeline).await;
    // Start occupies the only output slot. Do not drain it until routing has
    // reached a terminal failure: a queued marker must retain nonblocking push.
    let marker = Frame::new(FrameData::Data);
    let mut state = worker.sender().watch();
    let _ = admit(
        &worker,
        &mut accepted,
        marker.clone(),
        Direction::Downstream,
    )
    .await;
    tokio::time::timeout(DEADLINE, async {
        while !state.borrow_and_update().is_finished() {
            state.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    let mut frames = vec![];
    while let Some(frame) = worker.recv().await {
        frames.push(frame);
    }
    let result = worker.join().await;
    assert!(
        matches!(result, Err(PipelineError::Backpressure(ref message)) if message == "worker output queue is full; consume output concurrently")
    );
    assert_eq!(frames.len(), 1);
    assert!(matches!(frames[0].frame.data, FrameData::Start(_)));
    assert!(!frames.iter().any(|f| f.frame.id == marker.id
        || matches!(f.frame.data, FrameData::End(_) | FrameData::LlmResponseEnd)));
}

#[tokio::test]
async fn interrupted_synthesis_marks_surviving_completion_failed_without_poisoning_new_turn() {
    let (listener, config) = listener().await;
    let (captured, capture) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut old, _) = listener.accept().await.unwrap();
        support::read_request(&mut old).await;
        captured.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(DEADLINE, old.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        let (mut fresh, _) = listener.accept().await.unwrap();
        assert_eq!(
            support::read_request(&mut fresh).await.json()["transcript"],
            "fresh"
        );
        reply(&mut fresh, &[9, 0]).await;
    });
    let (worker, mut accepted) = start(config, 8, PipelineConfig::default()).await;
    admit(
        &worker,
        &mut accepted,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("old"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    tokio::time::timeout(DEADLINE, capture)
        .await
        .unwrap()
        .unwrap();
    let old_end = Frame::new(FrameData::LlmResponseEnd).with_interruptible(false);
    admit(
        &worker,
        &mut accepted,
        old_end.clone(),
        Direction::Downstream,
    )
    .await
    .unwrap();
    admit(
        &worker,
        &mut accepted,
        Frame::interruption(),
        Direction::Downstream,
    )
    .await
    .unwrap();
    let fresh_end = Frame::new(FrameData::LlmResponseEnd);
    for frame in [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))),
        fresh_end.clone(),
    ] {
        admit(&worker, &mut accepted, frame, Direction::Downstream)
            .await
            .unwrap();
    }
    let (frames, report) = finish(worker, vec![]).await;
    server.await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        frames.iter().filter(|f| f.frame.id == old_end.id).count(),
        1
    );
    assert_eq!(
        frames.iter().filter(|f| f.frame.id == fresh_end.id).count(),
        1
    );
    let starts: Vec<_> = frames
        .iter()
        .enumerate()
        .filter_map(|(index, frame)| {
            matches!(frame.frame.data, FrameData::TtsStarted(_)).then_some(index)
        })
        .collect();
    assert_eq!(starts.len(), 2);
    let old_index = frames
        .iter()
        .position(|f| f.frame.id == old_end.id)
        .unwrap();
    assert!(old_index < starts[1]);
    let old = &frames[old_index];
    let fresh = frames.iter().find(|f| f.frame.id == fresh_end.id).unwrap();
    assert_eq!(old.frame.metadata.get("failed"), Some(&json!(true)));
    assert!(!fresh.frame.metadata.contains_key("failed"));
}
