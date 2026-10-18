//! Native service admission tests. The synthetic producer exercises the core
//! image/context transaction; turns separately tests the real assistant commit.
use super::*;
use crate::{ClientConfig, Context, Provider};
use pipecat_core::{
    ChatMessage, ChatRole, ImageRawFrame, LlmContextFrame, NativeUserImageSource, Pipeline,
    PipelineWorker, UserImageRawFrame, UserImageRequestFrame,
};
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{oneshot, Notify};
const WAIT: Duration = Duration::from_secs(5);

struct ImageContextSource {
    source: NativeUserImageSource,
    produced: Notify,
    clone_context: bool,
    starts: AtomicUsize,
    ends: AtomicUsize,
    ended: Notify,
}
impl ImageContextSource {
    fn new(clone_context: bool) -> Self {
        Self {
            source: NativeUserImageSource::new(),
            produced: Notify::new(),
            clone_context,
            starts: AtomicUsize::new(0),
            ends: AtomicUsize::new(0),
            ended: Notify::new(),
        }
    }
}
#[async_trait]
impl FrameProcessor for ImageContextSource {
    fn name(&self) -> &str {
        "ImageContextSource"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::UserImageRequest(request) = &frame.data {
            let mut image = Frame::new(FrameData::UserImage(UserImageRawFrame {
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
            }));
            let scope = ctx.invocation_fence()?;
            self.source
                .bind(&mut image, &frame, scope.clone(), Arc::new(()))?;
            let constraint = image
                .native_user_image_constraint(Direction::Downstream)?
                .unwrap();
            let context = scope
                .with_current(|| {
                    let commit = constraint.begin_commit()?.unwrap();
                    let mut context = Frame::new(FrameData::LlmContext(LlmContextFrame {
                        messages: vec![ChatMessage::new(ChatRole::User, "synthetic image context")],
                        ..Default::default()
                    }));
                    commit.prepare_context(&mut context, &scope)?;
                    commit.complete();
                    Ok::<_, ProcessorError>(context)
                })
                .unwrap()?;
            let cloned = self.clone_context.then(|| context.clone());
            ctx.try_push_with_invocation_fence(context, Direction::Upstream, &scope)?;
            if let Some(cloned) = cloned {
                ctx.try_push_with_invocation_fence(cloned, Direction::Upstream, &scope)?;
            }
            self.produced.notify_one();
            return Ok(());
        }
        if matches!(frame.data, FrameData::LlmResponseStart) {
            self.starts.fetch_add(1, Ordering::SeqCst);
        }
        if matches!(frame.data, FrameData::LlmResponseEnd) {
            self.ends.fetch_add(1, Ordering::SeqCst);
            self.ended.notify_one();
        }
        ctx.push(frame, direction).await
    }
}
fn request() -> Frame {
    Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
        user_id: "synthetic-user".into(),
        text: Some("synthetic image context".into()),
        append_to_context: Some(true),
        video_source: None,
        function_name: None,
        tool_call_id: None,
        result_callback: None,
    }))
}
fn llm(listener: &TcpListener) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    let http = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()
        .unwrap();
    Arc::new(LlmProcessor::new(
        LlmClient::with_http_client(config, http).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ))
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn notified(notify: &Notify) {
    tokio::time::timeout(WAIT, notify.notified()).await.unwrap();
}
async fn barrier(worker: &mut PipelineWorker) {
    let frame = Frame::new(FrameData::Flush(pipecat_core::FlushFrame {
        returning: false,
        origin: Some("image-service".into()),
    }));
    let id = frame.id;
    send(worker, frame).await;
    tokio::time::timeout(WAIT, async {
        while let Some(output) = worker.recv().await {
            if output.frame.id == id {
                return;
            }
        }
        panic!("pipeline ended before image-service barrier");
    })
    .await
    .unwrap();
}
async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(WAIT, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test]
async fn source_close_after_actual_llm_consume_while_fifo_waits_has_no_start_or_http() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm = llm(&listener);
    let fifo = llm.completion_lock.clone().lock_owned().await;
    let source = Arc::new(ImageContextSource::new(false));
    let mut worker = Pipeline::new(vec![llm.clone(), source.clone()])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, request()).await;
    notified(&source.produced).await;
    barrier(&mut worker).await;
    assert_eq!(
        llm.request_snapshot().unwrap().context.messages.len(),
        1,
        "real LlmProcessor has consumed context and registered its blocked completion"
    );
    source.source.close();
    drop(fifo);
    finish(worker).await;
    assert_eq!(source.starts.load(Ordering::SeqCst), 0);
    assert_eq!(source.ends.load(Ordering::SeqCst), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn source_close_during_response_start_wait_suppresses_http_without_spending_initial_metric() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm = llm(&listener);
    let pause = Arc::new(ImageStartPause {
        entered: Notify::new(),
        release: Notify::new(),
    });
    *llm.image_start_pause.lock().unwrap() = Some(pause.clone());
    let source = Arc::new(ImageContextSource::new(false));
    let worker = Pipeline::new(vec![llm.clone(), source.clone()])
        .start(pipecat_core::PipelineConfig {
            start: pipecat_core::StartFrame {
                enable_metrics: true,
                report_only_initial_ttfb: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    send(&worker, request()).await;
    notified(&pause.entered).await;
    source.source.close();
    // The close watch must wake and cancel this await without test release.
    finish(worker).await;
    assert!(llm.should_report_ttfb.load(Ordering::SeqCst));
    assert_eq!(source.starts.load(Ordering::SeqCst), 0);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err()
    );
}
async fn reply(socket: &mut TcpStream) {
    let mut bytes = Vec::new();
    let mut part = [0u8; 4096];
    let end = loop {
        let count = socket.read(&mut part).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&part[..count]);
        assert!(bytes.len() <= 1024 * 1024);
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let count = socket.read(&mut part).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&part[..count]);
    }
    let body = "data: {\"choices\":[{\"delta\":{\"content\":\"red\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
}
#[tokio::test]
async fn cloned_context_claims_once_without_extra_start_and_ordinary_run_stays_unmarked() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let llm = llm(&listener);
    let source = Arc::new(ImageContextSource::new(true));
    let count = Arc::new(AtomicUsize::new(0));
    let observed = count.clone();
    let (stop, mut stopped) = oneshot::channel();
    let peer = tokio::spawn(async move {
        loop {
            let mut accepted = tokio::select! { biased; _ = &mut stopped => break, accepted = listener.accept() => accepted.unwrap() };
            observed.fetch_add(1, Ordering::SeqCst);
            reply(&mut accepted.0).await;
        }
    });
    let mut worker = Pipeline::new(vec![llm.clone(), source.clone()])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, request()).await;
    notified(&source.ended).await;
    barrier(&mut worker).await;
    // Synchronize behind both genuine FIFO positions before observing counts.
    let fifo = tokio::time::timeout(WAIT, llm.completion_lock.clone().lock_owned())
        .await
        .unwrap();
    assert_eq!(source.starts.load(Ordering::SeqCst), 1);
    assert_eq!(source.ends.load(Ordering::SeqCst), 1);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    drop(fifo);
    source.source.close();
    // A new ordinary explicit run uses retained normal history without inheriting
    // the closed image invocation's one-shot inference authorization.
    send(&worker, Frame::new(FrameData::LlmRun)).await;
    notified(&source.ended).await;
    finish(worker).await;
    assert_eq!(source.starts.load(Ordering::SeqCst), 2);
    assert_eq!(source.ends.load(Ordering::SeqCst), 2);
    assert_eq!(count.load(Ordering::SeqCst), 2);
    stop.send(()).unwrap();
    tokio::time::timeout(WAIT, peer).await.unwrap().unwrap();
}
